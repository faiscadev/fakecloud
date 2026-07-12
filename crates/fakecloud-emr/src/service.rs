//! Amazon EMR awsJson1_1 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: ElasticMapReduce.<Operation>`; dispatch keys
//! off `req.action`. Every operation runs model-driven input validation first
//! (required / length / range / enum), then real, account-partitioned,
//! persisted CRUD. Operations that dereference a cluster/studio/session that
//! does not exist return EMR's canonical `InvalidRequestException`, matching the
//! live service.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{EmrState, SharedEmrState};

pub const EMR_ACTIONS: &[&str] = &[
    "AddInstanceFleet",
    "AddInstanceGroups",
    "AddJobFlowSteps",
    "AddTags",
    "CancelSteps",
    "CreatePersistentAppUI",
    "CreateSecurityConfiguration",
    "CreateStudio",
    "CreateStudioSessionMapping",
    "DeleteSecurityConfiguration",
    "DeleteStudio",
    "DeleteStudioSessionMapping",
    "DescribeCluster",
    "DescribeJobFlows",
    "DescribeNotebookExecution",
    "DescribePersistentAppUI",
    "DescribeReleaseLabel",
    "DescribeSecurityConfiguration",
    "DescribeStep",
    "DescribeStudio",
    "GetAutoTerminationPolicy",
    "GetBlockPublicAccessConfiguration",
    "GetClusterSessionCredentials",
    "GetManagedScalingPolicy",
    "GetOnClusterAppUIPresignedURL",
    "GetPersistentAppUIPresignedURL",
    "GetSession",
    "GetSessionEndpoint",
    "GetStudioSessionMapping",
    "ListBootstrapActions",
    "ListClusters",
    "ListInstanceFleets",
    "ListInstanceGroups",
    "ListInstances",
    "ListNotebookExecutions",
    "ListReleaseLabels",
    "ListSecurityConfigurations",
    "ListSessions",
    "ListSteps",
    "ListStudioSessionMappings",
    "ListStudios",
    "ListSupportedInstanceTypes",
    "ModifyCluster",
    "ModifyInstanceFleet",
    "ModifyInstanceGroups",
    "PutAutoScalingPolicy",
    "PutAutoTerminationPolicy",
    "PutBlockPublicAccessConfiguration",
    "PutManagedScalingPolicy",
    "RemoveAutoScalingPolicy",
    "RemoveAutoTerminationPolicy",
    "RemoveManagedScalingPolicy",
    "RemoveTags",
    "RunJobFlow",
    "SetKeepJobFlowAliveWhenNoSteps",
    "SetTerminationProtection",
    "SetUnhealthyNodeReplacement",
    "SetVisibleToAllUsers",
    "StartNotebookExecution",
    "StartSession",
    "StopNotebookExecution",
    "TerminateJobFlows",
    "TerminateSession",
    "UpdateStudio",
    "UpdateStudioSessionMapping",
];

/// Read-only verbs; any other action mutates persisted state and triggers a
/// snapshot after success. The inverse formulation guarantees no mutation is
/// ever missed if a new op is added.
fn is_mutating_action(action: &str) -> bool {
    const READ_PREFIXES: &[&str] = &["Get", "List", "Describe"];
    !READ_PREFIXES.iter().any(|p| action.starts_with(p))
}

pub struct EmrService {
    state: SharedEmrState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl EmrService {
    pub fn new(state: SharedEmrState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

#[async_trait]
impl AwsService for EmrService {
    fn service_name(&self) -> &str {
        "emr"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        // Model-driven input validation (required/length/range/enum) runs
        // before any handler, mirroring AWS's request-validation phase.
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            return Err(invalid_request(msg));
        }
        let result = self.dispatch(&action, &req);
        if is_mutating_action(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        EMR_ACTIONS
    }
}

impl EmrService {
    #[allow(clippy::too_many_lines)]
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            // Clusters / job flows
            "RunJobFlow" => self.run_job_flow(req),
            "DescribeCluster" => self.describe_cluster(req),
            "ListClusters" => self.list_clusters(req),
            "TerminateJobFlows" => self.terminate_job_flows(req),
            "ModifyCluster" => self.modify_cluster(req),
            "SetTerminationProtection" => self.set_termination_protection(req),
            "SetKeepJobFlowAliveWhenNoSteps" => self.set_keep_alive(req),
            "SetVisibleToAllUsers" => self.set_visible_to_all_users(req),
            "SetUnhealthyNodeReplacement" => self.set_unhealthy_node_replacement(req),
            "DescribeJobFlows" => self.describe_job_flows(req),
            // Steps
            "AddJobFlowSteps" => self.add_job_flow_steps(req),
            "ListSteps" => self.list_steps(req),
            "DescribeStep" => self.describe_step(req),
            "CancelSteps" => self.cancel_steps(req),
            // Instance groups
            "AddInstanceGroups" => self.add_instance_groups(req),
            "ListInstanceGroups" => self.list_instance_groups(req),
            "ModifyInstanceGroups" => self.modify_instance_groups(req),
            "PutAutoScalingPolicy" => self.put_auto_scaling_policy(req),
            "RemoveAutoScalingPolicy" => self.remove_auto_scaling_policy(req),
            // Instance fleets
            "AddInstanceFleet" => self.add_instance_fleet(req),
            "ListInstanceFleets" => self.list_instance_fleets(req),
            "ModifyInstanceFleet" => self.modify_instance_fleet(req),
            // Instances / bootstrap actions
            "ListInstances" => self.list_instances(req),
            "ListBootstrapActions" => self.list_bootstrap_actions(req),
            // Security configurations
            "CreateSecurityConfiguration" => self.create_security_configuration(req),
            "DescribeSecurityConfiguration" => self.describe_security_configuration(req),
            "DeleteSecurityConfiguration" => self.delete_security_configuration(req),
            "ListSecurityConfigurations" => self.list_security_configurations(req),
            // Studios
            "CreateStudio" => self.create_studio(req),
            "DescribeStudio" => self.describe_studio(req),
            "UpdateStudio" => self.update_studio(req),
            "DeleteStudio" => self.delete_studio(req),
            "ListStudios" => self.list_studios(req),
            // Studio session mappings
            "CreateStudioSessionMapping" => self.create_studio_session_mapping(req),
            "GetStudioSessionMapping" => self.get_studio_session_mapping(req),
            "UpdateStudioSessionMapping" => self.update_studio_session_mapping(req),
            "DeleteStudioSessionMapping" => self.delete_studio_session_mapping(req),
            "ListStudioSessionMappings" => self.list_studio_session_mappings(req),
            // Tags
            "AddTags" => self.add_tags(req),
            "RemoveTags" => self.remove_tags(req),
            // Block public access
            "GetBlockPublicAccessConfiguration" => self.get_block_public_access(req),
            "PutBlockPublicAccessConfiguration" => self.put_block_public_access(req),
            // Auto-termination policy
            "PutAutoTerminationPolicy" => self.put_auto_termination_policy(req),
            "GetAutoTerminationPolicy" => self.get_auto_termination_policy(req),
            "RemoveAutoTerminationPolicy" => self.remove_auto_termination_policy(req),
            // Managed-scaling policy
            "PutManagedScalingPolicy" => self.put_managed_scaling_policy(req),
            "GetManagedScalingPolicy" => self.get_managed_scaling_policy(req),
            "RemoveManagedScalingPolicy" => self.remove_managed_scaling_policy(req),
            // Notebook executions
            "StartNotebookExecution" => self.start_notebook_execution(req),
            "DescribeNotebookExecution" => self.describe_notebook_execution(req),
            "ListNotebookExecutions" => self.list_notebook_executions(req),
            "StopNotebookExecution" => self.stop_notebook_execution(req),
            // Persistent app UIs
            "CreatePersistentAppUI" => self.create_persistent_app_ui(req),
            "DescribePersistentAppUI" => self.describe_persistent_app_ui(req),
            "GetPersistentAppUIPresignedURL" => self.get_persistent_app_ui_presigned_url(req),
            "GetOnClusterAppUIPresignedURL" => self.get_on_cluster_app_ui_presigned_url(req),
            // Interactive sessions
            "StartSession" => self.start_session(req),
            "GetSession" => self.get_session(req),
            "GetSessionEndpoint" => self.get_session_endpoint(req),
            "TerminateSession" => self.terminate_session(req),
            "ListSessions" => self.list_sessions(req),
            "GetClusterSessionCredentials" => self.get_cluster_session_credentials(req),
            // Release labels / instance types
            "ListReleaseLabels" => self.list_release_labels(req),
            "DescribeReleaseLabel" => self.describe_release_label(req),
            "ListSupportedInstanceTypes" => self.list_supported_instance_types(req),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }
}

// ---- shared helpers -------------------------------------------------------

/// EMR's canonical client-error shape. Returned (as HTTP 400) whenever a
/// request dereferences a resource that does not exist or otherwise fails
/// validation; the live service uses the same shape service-wide.
pub(crate) fn invalid_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidRequestException",
        msg.into(),
    )
}

pub(crate) fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(value))
}

/// Current time as epoch seconds (awsJson1_1 default timestamp wire form).
pub(crate) fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// A short, uppercase, AWS-style random suffix of `n` characters derived from a
/// v4 UUID (hex, so `0-9A-F` — the alphabet EMR uses for `j-`/`s-`/`ig-` ids).
pub(crate) fn rand_suffix(n: usize) -> String {
    let mut s = String::new();
    while s.len() < n {
        s.push_str(&uuid::Uuid::new_v4().simple().to_string());
    }
    s.truncate(n);
    s.to_ascii_uppercase()
}

pub(crate) fn cluster_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:elasticmapreduce:{region}:{account}:cluster/{id}")
}

pub(crate) fn studio_arn(region: &str, account: &str, id: &str) -> String {
    format!("arn:aws:elasticmapreduce:{region}:{account}:studio/{id}")
}

impl EmrService {
    /// Run `f` against this account's mutable state.
    pub(crate) fn with_account_mut<R>(
        &self,
        req: &AwsRequest,
        f: impl FnOnce(&mut EmrState) -> R,
    ) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }

    /// Run `f` against this account's read-only state (empty default if the
    /// account has never been touched).
    pub(crate) fn with_account<R>(&self, req: &AwsRequest, f: impl FnOnce(&EmrState) -> R) -> R {
        let guard = self.state.read();
        match guard.get(&req.account_id) {
            Some(acct) => f(acct),
            None => f(&EmrState::default()),
        }
    }
}

include!("handlers.rs");
