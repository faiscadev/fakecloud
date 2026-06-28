//! CloudFormation-driven real task backing for ECS services.
//!
//! When `AWS::ECS::Service` is provisioned through a CloudFormation stack, the
//! CFN provisioner inserts the service record synchronously (so `Ref`/`GetAtt`
//! resolve during provisioning) with `running_count` 0 and no tasks, then asks
//! ECS to launch the REAL tasks needed to reach `desiredCount` -- the same
//! container-backed tasks the direct `CreateService` path spawns. This module
//! mirrors that background task launch for an already-inserted service, so a
//! CFN-provisioned ECS service runs genuine containers, not phantom metadata.

use std::sync::Arc;

use crate::runtime::EcsRuntime;
use crate::{EcsState, SharedEcsState};

/// Launch the REAL tasks for an already-inserted (running_count 0) ECS service
/// so it reaches its `desiredCount`. Replicates the direct `CreateService` task
/// spawn: build the `Task` rows via `spawn_service_tasks`, then hand each task id
/// to `EcsRuntime::run_task` to start a real container. No-op if the service is
/// gone (e.g. the stack was deleted before this ran) or its desired count is 0.
/// Intended to be `tokio::spawn`ed by the CloudFormation `CreateStack` drain so
/// stack creation never blocks on a container boot/pull (the #1539/#1730 timeout
/// lesson). With no runtime wired (CI / metadata-only) the drain never calls
/// this, so the service stays at running_count 0 with no tasks -- matching the
/// provisioner's metadata-only behavior.
pub async fn cfn_launch_service_tasks(
    state: SharedEcsState,
    runtime: Arc<EcsRuntime>,
    cluster_name: String,
    service_name: String,
    account_id: String,
) {
    let spawn_task_ids = {
        let mut accounts = state.write();
        let Some(st) = accounts.get_mut(&account_id) else {
            return;
        };
        let key = EcsState::service_key(&cluster_name, &service_name);
        let Some(service) = st.services.get(&key).cloned() else {
            return;
        };
        if service.desired_count <= 0 {
            return;
        }
        let principal_arn = service
            .created_by
            .clone()
            .unwrap_or_else(|| format!("arn:aws:iam::{account_id}:root"));
        let launch_type = service.launch_type.clone();
        let desired = service.desired_count;
        crate::service::spawn_service_tasks(
            st,
            &service,
            desired,
            &principal_arn,
            &launch_type,
            None,
        )
    };

    for id in spawn_task_ids {
        runtime
            .clone()
            .run_task(state.clone(), id, account_id.clone());
    }
}

/// Stop the REAL tasks (containers) backing a CFN-provisioned ECS service when
/// its stack is deleted (or the service is removed by a stack update). Mirrors
/// the direct `DeleteService` teardown (`stop_task` per running task) so a stack
/// delete does not leak the running task containers. The service record itself
/// has already been removed by the synchronous provisioner delete; this reaps
/// the orphaned task containers and drops their records. Intended to be
/// `tokio::spawn`ed by the CloudFormation delete drain.
pub async fn cfn_stop_service_tasks(
    state: SharedEcsState,
    runtime: Arc<EcsRuntime>,
    cluster_name: String,
    service_name: String,
    account_id: String,
) {
    let task_ids = {
        let mut accounts = state.write();
        let Some(st) = accounts.get_mut(&account_id) else {
            return;
        };
        let service_tag = format!("ecs-svc/{service_name}");
        let ids: Vec<String> = st
            .tasks
            .iter()
            .filter(|(_, t)| {
                t.started_by.as_deref() == Some(service_tag.as_str())
                    && t.cluster_name == cluster_name
                    && t.last_status != "STOPPED"
            })
            .map(|(id, _)| id.clone())
            .collect();
        ids
    };

    for id in &task_ids {
        runtime.stop_task(id, "CloudFormation stack deletion").await;
        let mut accounts = state.write();
        if let Some(st) = accounts.get_mut(&account_id) {
            st.tasks.remove(id);
        }
    }
}
