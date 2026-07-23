use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    Attribute, AttributeRef, AwsLogsConfig, CapacityProvider, CircuitBreakerConfig, Cluster,
    Container, ContainerInstance, Deployment, EcsSnapshot, EcsState, Service, SharedEcsState,
    TagEntry, Task, TaskDefinition, TaskSet, ECS_SNAPSHOT_SCHEMA_VERSION,
};

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateCluster",
    "DescribeClusters",
    "DeleteCluster",
    "ListClusters",
    "UpdateCluster",
    "UpdateClusterSettings",
    "PutClusterCapacityProviders",
    "RegisterTaskDefinition",
    "DescribeTaskDefinition",
    "DeregisterTaskDefinition",
    "DeleteTaskDefinitions",
    "ListTaskDefinitions",
    "ListTaskDefinitionFamilies",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
    "PutAccountSetting",
    "PutAccountSettingDefault",
    "DeleteAccountSetting",
    "ListAccountSettings",
    "RunTask",
    "StartTask",
    "StopTask",
    "DescribeTasks",
    "ListTasks",
    "CreateService",
    "UpdateService",
    "DeleteService",
    "DescribeServices",
    "ListServices",
    "ListServicesByNamespace",
    "RegisterContainerInstance",
    "DeregisterContainerInstance",
    "DescribeContainerInstances",
    "ListContainerInstances",
    "UpdateContainerAgent",
    "UpdateContainerInstancesState",
    "PutAttributes",
    "DeleteAttributes",
    "ListAttributes",
    "CreateCapacityProvider",
    "DeleteCapacityProvider",
    "DescribeCapacityProviders",
    "UpdateCapacityProvider",
    "GetTaskProtection",
    "UpdateTaskProtection",
    "CreateTaskSet",
    "UpdateTaskSet",
    "DeleteTaskSet",
    "DescribeTaskSets",
    "UpdateServicePrimaryTaskSet",
    "ExecuteCommand",
    "SubmitContainerStateChange",
    "SubmitTaskStateChange",
    "SubmitAttachmentStateChanges",
    "DiscoverPollEndpoint",
    "StopServiceDeployment",
    "ContinueServiceDeployment",
    "ListServiceDeployments",
    "DescribeServiceDeployments",
    "DescribeServiceRevisions",
    "RegisterDaemonTaskDefinition",
    "DescribeDaemonTaskDefinition",
    "DeleteDaemonTaskDefinition",
    "ListDaemonTaskDefinitions",
    "CreateDaemon",
    "DescribeDaemon",
    "UpdateDaemon",
    "DeleteDaemon",
    "ListDaemons",
    "DescribeDaemonDeployments",
    "ListDaemonDeployments",
    "DescribeDaemonRevisions",
    "CreateExpressGatewayService",
    "DescribeExpressGatewayService",
    "UpdateExpressGatewayService",
    "DeleteExpressGatewayService",
];

pub struct EcsService {
    state: SharedEcsState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    runtime: Option<Arc<crate::runtime::EcsRuntime>>,
    role_trust_validator: Option<Arc<dyn fakecloud_core::auth::RoleTrustValidator>>,
}

impl EcsService {
    pub fn new(state: SharedEcsState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            runtime: None,
            role_trust_validator: None,
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_runtime(mut self, runtime: Arc<crate::runtime::EcsRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn with_role_trust_validator(
        mut self,
        validator: Arc<dyn fakecloud_core::auth::RoleTrustValidator>,
    ) -> Self {
        self.role_trust_validator = Some(validator);
        self
    }

    fn check_pass_role(&self, account_id: &str, role_arn: &str) -> Result<(), AwsServiceError> {
        let Some(ref validator) = self.role_trust_validator else {
            return Ok(());
        };
        if let Err(err) = validator.validate(account_id, role_arn, "ecs-tasks.amazonaws.com") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                err.to_string(),
            ));
        }
        Ok(())
    }

    pub fn state_handle(&self) -> &SharedEcsState {
        &self.state
    }

    async fn save_snapshot(&self) {
        save_ecs_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current state when invoked, or `None` in
    /// memory mode. The CloudFormation provisioner mutates `state` directly and
    /// uses this to write a CFN-provisioned resource through to disk.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_ecs_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Reconcile persisted task state with reality after a fakecloud
    /// restart. Same bug class as RDS #1338: tasks were persisted with
    /// `lastStatus = RUNNING` but their docker containers are gone, so
    /// DescribeTasks reported phantom-running tasks and `docker exec`
    /// (for ECS Exec) would fail.
    ///
    /// Mark every non-STOPPED task as STOPPED with a `stoppedReason`
    /// that explains the restart, and reset service `runningCount` /
    /// `pendingCount` to zero. The service scheduler ticker then
    /// reconciles desiredCount and launches fresh tasks. Standalone
    /// `RunTask` tasks aren't auto-respawned because we don't have
    /// enough context to replay them safely; callers re-invoke.
    pub async fn reconcile_persisted_tasks(&self) {
        let touched = {
            let mut accounts = self.state.write();
            let mut touched_tasks = 0usize;
            let mut touched_services = 0usize;
            let now = chrono::Utc::now();
            for (_, state) in accounts.iter_mut() {
                for task in state.tasks.values_mut() {
                    if task.last_status != "STOPPED" {
                        task.last_status = "STOPPED".to_string();
                        task.desired_status = "STOPPED".to_string();
                        task.stop_code = Some("EssentialContainerExited".to_string());
                        task.stopped_reason =
                            Some("fakecloud restart - backing container lost".to_string());
                        if task.stopping_at.is_none() {
                            task.stopping_at = Some(now);
                        }
                        if task.stopped_at.is_none() {
                            task.stopped_at = Some(now);
                        }
                        for container in task.containers.iter_mut() {
                            container.last_status = "STOPPED".to_string();
                        }
                        touched_tasks += 1;
                    }
                }
                for service in state.services.values_mut() {
                    if service.running_count != 0 || service.pending_count != 0 {
                        service.running_count = 0;
                        service.pending_count = 0;
                        for deployment in service.deployments.iter_mut() {
                            deployment.running_count = 0;
                            deployment.pending_count = 0;
                        }
                        touched_services += 1;
                    }
                }
            }
            (touched_tasks, touched_services)
        };
        if touched.0 + touched.1 > 0 {
            tracing::info!(
                tasks = touched.0,
                services = touched.1,
                "reconciled persisted ecs tasks / service counts after restart",
            );
            self.save_snapshot().await;
        }
    }

    /// Converge every ACTIVE service toward its `desiredCount`, launching
    /// replacement tasks for any shortfall. Real ECS maintains `desiredCount`
    /// autonomously; `reconcile_persisted_tasks` STOPs all tasks and zeroes
    /// counts on restart "trusting a scheduler ticker," but no such ticker
    /// existed, so services with `desiredCount=N` stayed at `runningCount=0`
    /// forever. This is that ticker. It also re-launches tasks lost to crashed
    /// containers during normal operation. bug-audit 2026-06-15, 4.7.
    ///
    /// One pass: for each service, count its non-STOPPED tasks (matched by the
    /// `ecs-svc/<name>` `started_by` tag, same as `recompute_service_counts`);
    /// if that count is below `desiredCount`, spawn the difference via the
    /// shared `spawn_service_tasks` path and hand the new task IDs to the
    /// runtime. Persist only when something was launched.
    pub async fn reconcile_service_desired_counts(&self) {
        // Phase 1 (write lock): find shortfalls, spawn PENDING task rows,
        // and refresh each service's running/pending counts so DescribeServices
        // is accurate immediately. Collect the new task IDs to launch after the
        // lock is released.
        let mut to_launch: Vec<(String, String)> = Vec::new(); // (account_id, task_id)
        {
            let mut accounts = self.state.write();
            for (account_id, state) in accounts.iter_mut() {
                // Snapshot the services we may need to scale so we don't hold
                // an aliasing borrow of `state` while mutating it.
                let scalable: Vec<Service> = state
                    .services
                    .values()
                    .filter(|s| {
                        s.status == "ACTIVE"
                            && s.scheduling_strategy != "DAEMON"
                            && s.desired_count > 0
                    })
                    .cloned()
                    .collect();
                for service in scalable {
                    let service_tag = format!("ecs-svc/{}", service.service_name);
                    let mut active = 0i32;
                    for t in state.tasks.values() {
                        if t.started_by.as_deref() == Some(service_tag.as_str())
                            && t.cluster_name == service.cluster_name
                            && matches!(
                                t.last_status.as_str(),
                                "RUNNING" | "PENDING" | "PROVISIONING"
                            )
                        {
                            active += 1;
                        }
                    }
                    let shortfall = service.desired_count - active;
                    if shortfall <= 0 {
                        continue;
                    }
                    let launch_type = if service.launch_type.is_empty() {
                        "FARGATE"
                    } else {
                        &service.launch_type
                    };
                    // Background re-spawn has no request region; keep the new
                    // tasks in the region the service's stored ARN already
                    // carries (stamped from req.region at CreateService time).
                    let svc_region = service
                        .cluster_arn
                        .split(':')
                        .nth(3)
                        .map(str::to_string)
                        .unwrap_or_else(|| state.region.clone());
                    let ids = spawn_service_tasks(
                        state,
                        &svc_region,
                        &service,
                        shortfall,
                        service.role_arn.as_deref().unwrap_or(""),
                        launch_type,
                        None,
                    );
                    if ids.is_empty() {
                        continue;
                    }
                    tracing::info!(
                        service = %service.service_name,
                        cluster = %service.cluster_name,
                        launched = ids.len(),
                        desired = service.desired_count,
                        "ecs scheduler: launching tasks to converge to desiredCount",
                    );
                    // Reflect the new PENDING tasks in the stored counts.
                    // Services are keyed by "cluster/name", not the bare name —
                    // looking up by service_name returned None, so these counts
                    // were never updated (stale snapshot until DescribeServices
                    // recomputed them).
                    let svc_key = crate::state::EcsState::service_key(
                        &service.cluster_name,
                        &service.service_name,
                    );
                    if let Some(svc) = state.services.get_mut(&svc_key) {
                        svc.pending_count = svc.pending_count.saturating_add(ids.len() as i32);
                        for d in svc.deployments.iter_mut() {
                            if d.status == "PRIMARY" {
                                d.pending_count = d.pending_count.saturating_add(ids.len() as i32);
                            }
                        }
                    }
                    for id in ids {
                        to_launch.push((account_id.to_string(), id));
                    }
                }
            }
        }

        if to_launch.is_empty() {
            return;
        }

        // Phase 2 (no lock): hand new tasks to the runtime, which advances them
        // PENDING -> RUNNING in the background. Without a runtime (docker/podman
        // absent) mark them STOPPED so they don't linger PENDING forever.
        if let Some(rt) = &self.runtime {
            for (account_id, task_id) in &to_launch {
                rt.clone()
                    .run_task(self.state.clone(), task_id.clone(), account_id.clone());
            }
        } else {
            let mut accounts = self.state.write();
            for (account_id, task_id) in &to_launch {
                if let Some(state) = accounts.get_mut(account_id) {
                    if let Some(t) = state.tasks.get_mut(task_id) {
                        t.last_status = "STOPPED".into();
                        t.desired_status = "STOPPED".into();
                        t.stop_code = Some("TaskFailedToStart".into());
                        t.stopped_reason = Some(format!(
                            "No container runtime available (docker/podman not installed). {}",
                            fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                        ));
                        t.stopped_at = Some(chrono::Utc::now());
                        for c in t.containers.iter_mut() {
                            c.last_status = "STOPPED".into();
                        }
                    }
                }
            }
        }

        self.save_snapshot().await;
    }
}

pub async fn save_ecs_snapshot(
    state: &SharedEcsState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = EcsSnapshot {
        schema_version: ECS_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write ecs snapshot"),
        Err(err) => tracing::error!(%err, "ecs snapshot task panicked"),
    }
}

/// Background scheduler ticker: periodically converges ECS services toward
/// `desiredCount`. Wired in `fakecloud-server` like the other tickers. Real
/// ECS does this continuously; we tick every few seconds. bug-audit 4.7.
pub async fn run_scheduler_ticker(service: Arc<EcsService>, interval: std::time::Duration) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick; restart reconciliation runs separately.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        service.reconcile_service_desired_counts().await;
    }
}

#[async_trait]
impl AwsService for EcsService {
    fn service_name(&self) -> &str {
        "ecs"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating(request.action.as_str());
        let result = match request.action.as_str() {
            "CreateCluster" => self.create_cluster(&request),
            "DescribeClusters" => self.describe_clusters(&request),
            "DeleteCluster" => self.delete_cluster(&request),
            "ListClusters" => self.list_clusters(&request),
            "UpdateCluster" => self.update_cluster(&request),
            "UpdateClusterSettings" => self.update_cluster_settings(&request),
            "PutClusterCapacityProviders" => self.put_cluster_capacity_providers(&request),
            "RegisterTaskDefinition" => self.register_task_definition(&request),
            "DescribeTaskDefinition" => self.describe_task_definition(&request),
            "DeregisterTaskDefinition" => self.deregister_task_definition(&request),
            "DeleteTaskDefinitions" => self.delete_task_definitions(&request),
            "ListTaskDefinitions" => self.list_task_definitions(&request),
            "ListTaskDefinitionFamilies" => self.list_task_definition_families(&request),
            "TagResource" => self.tag_resource(&request),
            "UntagResource" => self.untag_resource(&request),
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "PutAccountSetting" => self.put_account_setting(&request),
            "PutAccountSettingDefault" => self.put_account_setting_default(&request),
            "DeleteAccountSetting" => self.delete_account_setting(&request),
            "ListAccountSettings" => self.list_account_settings(&request),
            "RunTask" => self.run_task(&request),
            "StartTask" => self.start_task(&request),
            "StopTask" => self.stop_task(&request).await,
            "DescribeTasks" => self.describe_tasks(&request),
            "ListTasks" => self.list_tasks(&request),
            "CreateService" => self.create_service(&request),
            "UpdateService" => self.update_service(&request),
            "DeleteService" => self.delete_service(&request).await,
            "DescribeServices" => self.describe_services(&request),
            "ListServices" => self.list_services(&request),
            "ListServicesByNamespace" => self.list_services_by_namespace(&request),
            "RegisterContainerInstance" => self.register_container_instance(&request),
            "DeregisterContainerInstance" => self.deregister_container_instance(&request),
            "DescribeContainerInstances" => self.describe_container_instances(&request),
            "ListContainerInstances" => self.list_container_instances(&request),
            "UpdateContainerAgent" => self.update_container_agent(&request),
            "UpdateContainerInstancesState" => self.update_container_instances_state(&request),
            "PutAttributes" => self.put_attributes(&request),
            "DeleteAttributes" => self.delete_attributes(&request),
            "ListAttributes" => self.list_attributes(&request),
            "CreateCapacityProvider" => self.create_capacity_provider(&request),
            "DeleteCapacityProvider" => self.delete_capacity_provider(&request),
            "DescribeCapacityProviders" => self.describe_capacity_providers(&request),
            "UpdateCapacityProvider" => self.update_capacity_provider(&request),
            "GetTaskProtection" => self.get_task_protection(&request),
            "UpdateTaskProtection" => self.update_task_protection(&request),
            "CreateTaskSet" => self.create_task_set(&request),
            "UpdateTaskSet" => self.update_task_set(&request),
            "DeleteTaskSet" => self.delete_task_set(&request),
            "DescribeTaskSets" => self.describe_task_sets(&request),
            "UpdateServicePrimaryTaskSet" => self.update_service_primary_task_set(&request),
            "ExecuteCommand" => self.execute_command(&request).await,
            "SubmitContainerStateChange" => self.submit_container_state_change(&request),
            "SubmitTaskStateChange" => self.submit_task_state_change(&request),
            "SubmitAttachmentStateChanges" => self.submit_attachment_state_changes(&request),
            "DiscoverPollEndpoint" => self.discover_poll_endpoint(&request),
            "StopServiceDeployment" => self.stop_service_deployment(&request),
            "ContinueServiceDeployment" => self.continue_service_deployment(&request),
            "ListServiceDeployments" => self.list_service_deployments(&request),
            "DescribeServiceDeployments" => self.describe_service_deployments(&request),
            "DescribeServiceRevisions" => self.describe_service_revisions(&request),
            "RegisterDaemonTaskDefinition" => self.register_daemon_task_definition(&request),
            "DescribeDaemonTaskDefinition" => self.describe_daemon_task_definition(&request),
            "DeleteDaemonTaskDefinition" => self.delete_daemon_task_definition(&request),
            "ListDaemonTaskDefinitions" => self.list_daemon_task_definitions(&request),
            "CreateDaemon" => self.create_daemon(&request),
            "DescribeDaemon" => self.describe_daemon(&request),
            "UpdateDaemon" => self.update_daemon(&request),
            "DeleteDaemon" => self.delete_daemon(&request),
            "ListDaemons" => self.list_daemons(&request),
            "DescribeDaemonDeployments" => self.describe_daemon_deployments(&request),
            "ListDaemonDeployments" => self.list_daemon_deployments(&request),
            "DescribeDaemonRevisions" => self.describe_daemon_revisions(&request),
            "CreateExpressGatewayService" => self.create_express_gateway_service(&request),
            "DescribeExpressGatewayService" => self.describe_express_gateway_service(&request),
            "UpdateExpressGatewayService" => self.update_express_gateway_service(&request),
            "DeleteExpressGatewayService" => self.delete_express_gateway_service(&request),
            _ => Err(AwsServiceError::action_not_implemented(
                "ecs",
                &request.action,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

// -------- helpers --------

// -------- operations: clusters --------

// -------- operations: task definitions --------

// -------- operations: tagging --------

// -------- operations: account settings --------

// -------- operations: tasks --------

// -------- operations: services --------

// -------- operations: container instances, attributes, capacity providers, task sets, task protection, ExecuteCommand, agent surface --------

#[path = "service_clusters.rs"]
mod service_clusters;

#[path = "service_task_definitions.rs"]
mod service_task_definitions;

#[path = "service_tagging.rs"]
mod service_tagging;

#[path = "service_account_settings.rs"]
mod service_account_settings;

#[path = "service_tasks.rs"]
mod service_tasks;

#[path = "service_services_resource.rs"]
mod service_services_resource;

#[path = "service_container_instances_etc.rs"]
mod service_container_instances_etc;

#[path = "service_daemons.rs"]
mod service_daemons;

#[path = "service_express_gateway.rs"]
mod service_express_gateway;

#[path = "helpers.rs"]
mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
