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
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let snapshot = EcsSnapshot {
            schema_version: ECS_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(self.state.read().clone()),
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
