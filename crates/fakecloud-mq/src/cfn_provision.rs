//! CloudFormation-driven container backing for Amazon MQ brokers.
//!
//! When `AWS::AmazonMQ::Broker` is provisioned through a CloudFormation stack,
//! the CFN provisioner inserts the broker record synchronously (so `Ref` /
//! `Fn::GetAtt` resolve during provisioning) with `brokerState`
//! `CREATION_IN_PROGRESS`, then asks MQ to back it with a REAL ActiveMQ /
//! RabbitMQ container -- the same container the direct `CreateBroker` path
//! spawns -- flipping it to `RUNNING` once reachable. This module is the
//! background task the CFN `CreateStack` drain runs, so a CFN-provisioned broker
//! is genuinely connectable, not phantom metadata (the data-plane bar).

use std::sync::Arc;

use crate::runtime::MqRuntime;
use crate::service::{gather_spec, settle_broker_up};
use crate::SharedMqState;

/// Back an already-inserted (status `CREATION_IN_PROGRESS`) CFN broker with a
/// real engine container and settle it to `RUNNING` (recording the real
/// host/port binding), mirroring the direct-API `CreateBroker`
/// background-spawn-then-settle. No-op if the record is gone. Intended to be
/// `tokio::spawn`ed by the CloudFormation drain so stack creation never blocks
/// on a container boot/pull (the #1539/#1730 timeout lesson).
pub async fn cfn_ensure_broker_container(
    state: SharedMqState,
    runtime: Arc<MqRuntime>,
    broker_id: String,
    account_id: String,
) {
    let spec = {
        let guard = state.read();
        guard
            .get(&account_id)
            .and_then(|d| gather_spec(d, &broker_id))
    };
    let Some(spec) = spec else {
        return;
    };
    let running = runtime
        .ensure_broker(
            &broker_id,
            spec.engine,
            &spec.users,
            spec.user_config.as_deref(),
        )
        .await
        .map_err(
            |error| tracing::error!(%error, broker_id = %broker_id, "CFN MQ broker container failed to start"),
        )
        .ok();
    settle_broker_up(&state, &account_id, &broker_id, running, &runtime).await;
}

/// Tear down a CFN-provisioned broker's backing container on stack delete, so a
/// stack delete never leaks a running broker container.
pub async fn cfn_teardown_broker_container(runtime: Arc<MqRuntime>, broker_id: String) {
    runtime.stop_broker(&broker_id).await;
}
