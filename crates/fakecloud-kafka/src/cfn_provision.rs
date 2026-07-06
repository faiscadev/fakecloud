//! CloudFormation-driven container backing for Amazon MSK clusters.
//!
//! When `AWS::MSK::Cluster` / `AWS::MSK::ServerlessCluster` is provisioned
//! through a CloudFormation stack, the CFN provisioner inserts the cluster
//! record synchronously (so `Ref` / `Fn::GetAtt` resolve during provisioning)
//! with `state` `CREATING`, then -- for a PROVISIONED cluster -- asks the Kafka
//! runtime to back it with a REAL Apache Kafka container, the same container the
//! direct `CreateCluster` path spawns, settling it to `ACTIVE` once it serves.
//! This module is the background task the CFN `CreateStack` drain runs, so a
//! CFN-provisioned cluster is genuinely connectable, not phantom metadata (the
//! data-plane bar).

use std::sync::Arc;

use crate::runtime::KafkaRuntime;
use crate::service::settle_cluster_up;
use crate::SharedKafkaState;

/// Back an already-inserted (state `CREATING`) CFN cluster with a real Kafka
/// broker container and settle it to `ACTIVE` (recording the real host/port
/// binding), mirroring the direct-API `CreateCluster` background-spawn-then-
/// settle. No-op if the record is gone. Intended to be `tokio::spawn`ed by the
/// CloudFormation drain so stack creation never blocks on a container boot/pull
/// (the #1539/#1730 timeout lesson).
pub async fn cfn_ensure_cluster_container(
    state: SharedKafkaState,
    runtime: Arc<KafkaRuntime>,
    cluster_arn: String,
    account_id: String,
) {
    // The record must still exist (not deleted mid-drain).
    let present = {
        let guard = state.read();
        guard
            .get(&account_id)
            .is_some_and(|d| d.clusters.contains_key(&cluster_arn))
    };
    if !present {
        return;
    }
    let running = runtime.ensure_broker(&cluster_arn).await.map_err(|error| {
        tracing::error!(%error, cluster_arn = %cluster_arn, "CFN MSK Kafka broker container failed to start");
        error.to_string()
    });
    settle_cluster_up(&state, &account_id, &cluster_arn, running, &runtime).await;
}

/// Tear down a CFN-provisioned cluster's backing container on stack delete, so a
/// stack delete never leaks a running Kafka broker container.
pub async fn cfn_teardown_cluster_container(runtime: Arc<KafkaRuntime>, cluster_arn: String) {
    runtime.stop_broker(&cluster_arn).await;
}
