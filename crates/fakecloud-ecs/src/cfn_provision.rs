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
        // No request region on the CFN drain path; keep the new tasks in the
        // region the service's stored ARN already carries.
        let svc_region = service
            .cluster_arn
            .split(':')
            .nth(3)
            .map(str::to_string)
            .unwrap_or_else(|| st.region.clone());
        let principal_arn = service.created_by.clone().unwrap_or_else(|| {
            fakecloud_aws::arn::Arn::global_in(&svc_region, "iam", &account_id, "root").to_string()
        });
        let launch_type = service.launch_type.clone();
        // Launch only the shortfall: the scheduler ticker reconciles every
        // ACTIVE service toward its desired count too, and may already have
        // started some (or all) of these tasks before this drain ran.
        // Spawning the full desired count regardless would double the
        // service's tasks.
        let shortfall =
            service.desired_count - st.active_service_task_count(&cluster_name, &service_name);
        if shortfall <= 0 {
            return;
        }
        let ids = crate::service::spawn_service_tasks(
            st,
            &svc_region,
            &service,
            shortfall,
            &principal_arn,
            &launch_type,
            None,
        );
        // Reflect the new PENDING tasks in the stored counts, as the
        // scheduler ticker does.
        if let Some(svc) = st.services.get_mut(&key) {
            svc.pending_count = svc.pending_count.saturating_add(ids.len() as i32);
            for d in svc.deployments.iter_mut().filter(|d| d.status == "PRIMARY") {
                d.pending_count = d.pending_count.saturating_add(ids.len() as i32);
            }
        }
        ids
    };

    for id in spawn_task_ids {
        runtime
            .clone()
            .run_task(state.clone(), id, account_id.clone());
    }
}

/// The live tasks (not STOPPED) of `cluster_name`, of its service
/// `service_name` only when given.
pub fn cfn_live_tasks(
    state: &SharedEcsState,
    account_id: &str,
    cluster_name: &str,
    service_name: Option<&str>,
) -> Vec<String> {
    let accounts = state.read();
    let Some(st) = accounts.get(account_id) else {
        return Vec::new();
    };
    let service_tag = service_name.map(|s| format!("ecs-svc/{s}"));
    st.tasks
        .iter()
        .filter(|(_, t)| {
            t.cluster_name == cluster_name
                && t.last_status != "STOPPED"
                && service_tag
                    .as_deref()
                    .is_none_or(|tag| t.started_by.as_deref() == Some(tag))
        })
        .map(|(id, _)| id.clone())
        .collect()
}

/// Start stopping `task_ids` for a stack deletion, while the service and
/// cluster records they belong to still exist: their load balancer targets
/// (found through the service) are deregistered and their ENIs captured for
/// release, so removing those records next doesn't leak either. The
/// containers are then stopped by [`cfn_stop_tasks`].
pub fn cfn_begin_stopping_tasks(
    state: &SharedEcsState,
    runtime: &EcsRuntime,
    account_id: &str,
    task_ids: &[String],
) {
    for id in task_ids {
        runtime.begin_stopping(state, account_id, id);
    }
}

/// Stop the REAL tasks (containers) of a CFN-provisioned ECS service or
/// cluster when its stack is deleted (or the resource is removed by a stack
/// update), so a stack delete does not leak the running task containers.
/// `task_ids` were captured (and [`cfn_begin_stopping_tasks`] run) by the
/// synchronous provisioner delete before it removed the service / cluster
/// record; this stops the containers and drops the task records. Intended
/// to be `tokio::spawn`ed by the CloudFormation delete drain.
pub async fn cfn_stop_tasks(
    state: SharedEcsState,
    runtime: Arc<EcsRuntime>,
    task_ids: Vec<String>,
    account_id: String,
) {
    // Tasks stop together. Each record is dropped only once its execution
    // has finalized the task (load balancer targets and ENI released,
    // awslogs forwarded), which reads the record.
    let mut stops = tokio::task::JoinSet::new();
    for id in task_ids {
        let (state, runtime, account_id) = (state.clone(), runtime.clone(), account_id.clone());
        stops.spawn(async move {
            runtime
                .stop_task_and_wait(&state, &account_id, &id, "CloudFormation stack deletion")
                .await;
            let mut accounts = state.write();
            if let Some(st) = accounts.get_mut(&account_id) {
                st.tasks.remove(&id);
            }
        });
    }
    stops.join_all().await;
}
