mod associations;
mod automation;
mod cloud_connectors;
mod commands;
mod compliance;
mod documents;
mod instances;
mod inventory;
mod maintenance;
mod misc;
mod ops;
mod parameters;
mod patches;
mod resource_sync;
mod sessions;
mod tags;

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{SharedSsmState, SsmSnapshot, SSM_SNAPSHOT_SCHEMA_VERSION};

use fakecloud_secretsmanager::SharedSecretsManagerState;

const PARAMETER_VERSION_LIMIT: i64 = 100;

pub struct SsmService {
    state: SharedSsmState,
    secretsmanager_state: Option<SharedSecretsManagerState>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    pub(crate) kms_hook: Option<Arc<dyn fakecloud_core::delivery::KmsHook>>,
}

impl SsmService {
    pub fn new(state: SharedSsmState) -> Self {
        Self {
            state,
            secretsmanager_state: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            kms_hook: None,
        }
    }

    pub fn with_secretsmanager(mut self, sm_state: SharedSecretsManagerState) -> Self {
        self.secretsmanager_state = Some(sm_state);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn fakecloud_core::delivery::KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    /// Admin: force a SendCommand result to a specific terminal status
    /// (e.g. "Failed", "Cancelled", "TimedOut"). Used by tests to
    /// simulate command failures without waiting on a real SSM agent.
    /// All invocations on the command move to the same status so
    /// `GetCommandInvocation` reflects the override on the next read.
    /// Returns `true` when the command was found and updated.
    pub fn set_command_status(&self, account_id: &str, command_id: &str, status: &str) -> bool {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        if let Some(c) = state
            .commands
            .iter_mut()
            .find(|c| c.command_id == command_id)
        {
            let now = chrono::Utc::now();
            let details = commands::friendly_status_details(status);
            for inv in c.invocations.iter_mut() {
                inv.status = status.to_string();
                inv.status_details = details.clone();
                inv.response_code = match status {
                    "Success" => 0,
                    "Pending" | "InProgress" | "Delayed" => -1,
                    _ => 1,
                };
                inv.last_update_at = now;
            }
            c.status = status.to_string();
            return true;
        }
        false
    }

    /// Admin: force a single invocation (or all if `instance_id` is
    /// `None`) to `Failed` with optional friendlier `status_details`
    /// and `standard_error_content` overrides. Mirrors what a real SSM
    /// agent would report when a runShellScript step exits non-zero.
    /// Returns the number of invocations that were updated.
    pub fn fail_command_invocation(
        &self,
        account_id: &str,
        command_id: &str,
        instance_id: Option<&str>,
        status_details: Option<&str>,
        standard_error_content: Option<&str>,
    ) -> usize {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let Some(cmd) = state
            .commands
            .iter_mut()
            .find(|c| c.command_id == command_id)
        else {
            return 0;
        };
        let now = chrono::Utc::now();
        let mut updated = 0;
        for inv in cmd.invocations.iter_mut() {
            if let Some(iid) = instance_id {
                if inv.instance_id != iid {
                    continue;
                }
            }
            inv.status = "Failed".to_string();
            inv.status_details = status_details
                .map(|s| s.to_string())
                .unwrap_or_else(|| commands::friendly_status_details("Failed"));
            if let Some(err) = standard_error_content {
                inv.standard_error_content = err.to_string();
            }
            inv.response_code = 1;
            inv.last_update_at = now;
            updated += 1;
        }
        if updated > 0 {
            cmd.status = commands::aggregate_command_status(&cmd.invocations);
        }
        updated
    }

    /// Admin: read out the parameter-policy event log for the given
    /// account. Used by tests to verify that Expiration deletions and
    /// notification windows actually fired without standing up an
    /// EventBridge target. Reads also tick the lazy policy evaluator so
    /// callers don't have to issue a Get* call first.
    pub fn parameter_policy_events(
        &self,
        account_id: &str,
    ) -> Vec<crate::state::ParameterPolicyEvent> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        parameters::purge_expired_params(state);
        parameters::tick_policy_notifications(state);
        state.parameter_policy_events.clone()
    }

    /// Admin: drop the recorded parameter-policy event log for the
    /// given account. Tests use this to reset between assertions.
    pub fn clear_parameter_policy_events(&self, account_id: &str) {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.parameter_policy_events.clear();
    }

    /// Admin: inject a fake SSM session record so DescribeSessions /
    /// TerminateSession round-trip without going through StartSession (which
    /// returns the Smithy-declared `TargetNotConnected` unless
    /// `FAKECLOUD_SSM_SESSION_ECHO=1`). The injected
    /// session is otherwise indistinguishable from one created by StartSession
    /// in echo mode. Returns the assigned `SessionId`.
    #[allow(clippy::too_many_arguments)]
    pub fn inject_session(
        &self,
        account_id: &str,
        target: &str,
        status: Option<&str>,
        owner: Option<&str>,
        reason: Option<&str>,
        session_id: Option<&str>,
    ) -> String {
        let now = chrono::Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let id = match session_id {
            Some(s) if !s.is_empty() => s.to_string(),
            _ => {
                state.session_counter += 1;
                format!("session-{:012x}", state.session_counter)
            }
        };
        let resolved_status = status.unwrap_or("Connected").to_string();
        let end_date = if resolved_status == "Terminated" {
            Some(now)
        } else {
            None
        };
        let resolved_owner = owner.map(|s| s.to_string()).unwrap_or_else(|| {
            fakecloud_aws::arn::Arn::global("iam", &state.account_id, "root").to_string()
        });
        let session = crate::state::SsmSession {
            session_id: id.clone(),
            target: target.to_string(),
            status: resolved_status,
            start_date: now,
            end_date,
            owner: resolved_owner,
            reason: reason.map(|s| s.to_string()),
        };
        state.sessions.insert(id.clone(), session);
        id
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes,
    /// with serde + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_ssm_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current SSM state when invoked, or `None`
    /// in memory mode (no snapshot store). The CloudFormation provisioner
    /// mutates `state` directly and uses this to write a CFN-provisioned
    /// resource through to disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_ssm_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current SSM state as a snapshot. Offloads the serde + blocking
/// file write to the Tokio blocking pool. Noop when `store` is `None` (memory
/// mode). Shared by `SsmService::save_snapshot` and the CloudFormation
/// provisioner's post-provision persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_ssm_snapshot(
    state: &SharedSsmState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = SsmSnapshot {
        schema_version: SSM_SNAPSHOT_SCHEMA_VERSION,
        state: None,
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
        Ok(Err(err)) => tracing::error!(%err, "failed to write ssm snapshot"),
        Err(err) => tracing::error!(%err, "ssm snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for SsmService {
    fn service_name(&self) -> &str {
        "ssm"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = !is_read_only_action(req.action.as_str());
        let result = match req.action.as_str() {
            "PutParameter" => self.put_parameter(&req),
            "GetParameter" => self.get_parameter(&req),
            "GetParameters" => self.get_parameters(&req),
            "GetParametersByPath" => self.get_parameters_by_path(&req),
            "DeleteParameter" => self.delete_parameter(&req),
            "DeleteParameters" => self.delete_parameters(&req),
            "DescribeParameters" => self.describe_parameters(&req),
            "GetParameterHistory" => self.get_parameter_history(&req),
            "AddTagsToResource" => self.add_tags_to_resource(&req),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "LabelParameterVersion" => self.label_parameter_version(&req),
            "UnlabelParameterVersion" => self.unlabel_parameter_version(&req),
            "CreateDocument" => self.create_document(&req),
            "GetDocument" => self.get_document(&req),
            "DeleteDocument" => self.delete_document(&req),
            "UpdateDocument" => self.update_document(&req),
            "DescribeDocument" => self.describe_document(&req),
            "UpdateDocumentDefaultVersion" => self.update_document_default_version(&req),
            "ListDocuments" => self.list_documents(&req),
            "DescribeDocumentPermission" => self.describe_document_permission(&req),
            "ModifyDocumentPermission" => self.modify_document_permission(&req),
            "SendCommand" => self.send_command(&req),
            "ListCommands" => self.list_commands(&req),
            "GetCommandInvocation" => self.get_command_invocation(&req),
            "ListCommandInvocations" => self.list_command_invocations(&req),
            "CancelCommand" => self.cancel_command(&req),
            "CreateMaintenanceWindow" => self.create_maintenance_window(&req),
            "DescribeMaintenanceWindows" => self.describe_maintenance_windows(&req),
            "GetMaintenanceWindow" => self.get_maintenance_window(&req),
            "DeleteMaintenanceWindow" => self.delete_maintenance_window(&req),
            "UpdateMaintenanceWindow" => self.update_maintenance_window(&req),
            "RegisterTargetWithMaintenanceWindow" => {
                self.register_target_with_maintenance_window(&req)
            }
            "DeregisterTargetFromMaintenanceWindow" => {
                self.deregister_target_from_maintenance_window(&req)
            }
            "DescribeMaintenanceWindowTargets" => self.describe_maintenance_window_targets(&req),
            "RegisterTaskWithMaintenanceWindow" => self.register_task_with_maintenance_window(&req),
            "DeregisterTaskFromMaintenanceWindow" => {
                self.deregister_task_from_maintenance_window(&req)
            }
            "DescribeMaintenanceWindowTasks" => self.describe_maintenance_window_tasks(&req),
            "CreatePatchBaseline" => self.create_patch_baseline(&req),
            "DeletePatchBaseline" => self.delete_patch_baseline(&req),
            "DescribePatchBaselines" => self.describe_patch_baselines(&req),
            "GetPatchBaseline" => self.get_patch_baseline(&req),
            "RegisterPatchBaselineForPatchGroup" => {
                self.register_patch_baseline_for_patch_group(&req)
            }
            "DeregisterPatchBaselineForPatchGroup" => {
                self.deregister_patch_baseline_for_patch_group(&req)
            }
            "GetPatchBaselineForPatchGroup" => self.get_patch_baseline_for_patch_group(&req),
            "DescribePatchGroups" => self.describe_patch_groups(&req),
            // Associations
            "CreateAssociation" => self.create_association(&req),
            "DescribeAssociation" => self.describe_association(&req),
            "DeleteAssociation" => self.delete_association(&req),
            "ListAssociations" => self.list_associations(&req),
            "UpdateAssociation" => self.update_association(&req),
            "ListAssociationVersions" => self.list_association_versions(&req),
            "UpdateAssociationStatus" => self.update_association_status(&req),
            "StartAssociationsOnce" => self.start_associations_once(&req),
            "CreateAssociationBatch" => self.create_association_batch(&req),
            "DescribeAssociationExecutions" => self.describe_association_executions(&req),
            "DescribeAssociationExecutionTargets" => {
                self.describe_association_execution_targets(&req)
            }
            // OpsItems
            "CreateOpsItem" => self.create_ops_item(&req),
            "GetOpsItem" => self.get_ops_item(&req),
            "UpdateOpsItem" => self.update_ops_item(&req),
            "DeleteOpsItem" => self.delete_ops_item(&req),
            "DescribeOpsItems" => self.describe_ops_items(&req),
            // Document extras
            "ListDocumentVersions" => self.list_document_versions(&req),
            "ListDocumentMetadataHistory" => self.list_document_metadata_history(&req),
            "UpdateDocumentMetadata" => self.update_document_metadata(&req),
            // Resource policies
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "GetResourcePolicies" => self.get_resource_policies(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            // Inventory
            "PutInventory" => self.put_inventory(&req),
            "GetInventory" => self.get_inventory(&req),
            "GetInventorySchema" => self.get_inventory_schema(&req),
            "ListInventoryEntries" => self.list_inventory_entries(&req),
            "DeleteInventory" => self.delete_inventory(&req),
            "DescribeInventoryDeletions" => self.describe_inventory_deletions(&req),
            // Compliance
            "PutComplianceItems" => self.put_compliance_items(&req),
            "ListComplianceItems" => self.list_compliance_items(&req),
            "ListComplianceSummaries" => self.list_compliance_summaries(&req),
            "ListResourceComplianceSummaries" => self.list_resource_compliance_summaries(&req),
            // Maintenance window details
            "UpdateMaintenanceWindowTarget" => self.update_maintenance_window_target(&req),
            "UpdateMaintenanceWindowTask" => self.update_maintenance_window_task(&req),
            "GetMaintenanceWindowTask" => self.get_maintenance_window_task(&req),
            "GetMaintenanceWindowExecution" => self.get_maintenance_window_execution(&req),
            "GetMaintenanceWindowExecutionTask" => self.get_maintenance_window_execution_task(&req),
            "GetMaintenanceWindowExecutionTaskInvocation" => {
                self.get_maintenance_window_execution_task_invocation(&req)
            }
            "DescribeMaintenanceWindowExecutions" => {
                self.describe_maintenance_window_executions(&req)
            }
            "DescribeMaintenanceWindowExecutionTasks" => {
                self.describe_maintenance_window_execution_tasks(&req)
            }
            "DescribeMaintenanceWindowExecutionTaskInvocations" => {
                self.describe_maintenance_window_execution_task_invocations(&req)
            }
            "DescribeMaintenanceWindowSchedule" => self.describe_maintenance_window_schedule(&req),
            "DescribeMaintenanceWindowsForTarget" => {
                self.describe_maintenance_windows_for_target(&req)
            }
            "CancelMaintenanceWindowExecution" => self.cancel_maintenance_window_execution(&req),
            // Patch management details
            "UpdatePatchBaseline" => self.update_patch_baseline(&req),
            "DescribeInstancePatchStates" => self.describe_instance_patch_states(&req),
            "DescribeInstancePatchStatesForPatchGroup" => {
                self.describe_instance_patch_states_for_patch_group(&req)
            }
            "DescribeInstancePatches" => self.describe_instance_patches(&req),
            "DescribeEffectivePatchesForPatchBaseline" => {
                self.describe_effective_patches_for_patch_baseline(&req)
            }
            "GetDeployablePatchSnapshotForInstance" => {
                self.get_deployable_patch_snapshot_for_instance(&req)
            }
            // Resource data sync
            "CreateResourceDataSync" => self.create_resource_data_sync(&req),
            "DeleteResourceDataSync" => self.delete_resource_data_sync(&req),
            "ListResourceDataSync" => self.list_resource_data_sync(&req),
            "UpdateResourceDataSync" => self.update_resource_data_sync(&req),
            // Cloud connectors
            "CreateCloudConnector" => self.create_cloud_connector(&req),
            "DeleteCloudConnector" => self.delete_cloud_connector(&req),
            "GetCloudConnector" => self.get_cloud_connector(&req),
            "ListCloudConnectors" => self.list_cloud_connectors(&req),
            "UpdateCloudConnector" => self.update_cloud_connector(&req),
            "ValidateCloudConnector" => self.validate_cloud_connector(&req),
            // OpsItem related items
            "AssociateOpsItemRelatedItem" => self.associate_ops_item_related_item(&req),
            "DisassociateOpsItemRelatedItem" => self.disassociate_ops_item_related_item(&req),
            "ListOpsItemRelatedItems" => self.list_ops_item_related_items(&req),
            "ListOpsItemEvents" => self.list_ops_item_events(&req),
            // OpsMetadata
            "CreateOpsMetadata" => self.create_ops_metadata(&req),
            "GetOpsMetadata" => self.get_ops_metadata(&req),
            "UpdateOpsMetadata" => self.update_ops_metadata(&req),
            "DeleteOpsMetadata" => self.delete_ops_metadata(&req),
            "ListOpsMetadata" => self.list_ops_metadata(&req),
            // OpsMetadata extras
            "GetOpsSummary" => self.get_ops_summary(&req),
            // Automation
            "StartAutomationExecution" => self.start_automation_execution(&req),
            "StopAutomationExecution" => self.stop_automation_execution(&req),
            "GetAutomationExecution" => self.get_automation_execution(&req),
            "DescribeAutomationExecutions" => self.describe_automation_executions(&req),
            "DescribeAutomationStepExecutions" => self.describe_automation_step_executions(&req),
            "SendAutomationSignal" => self.send_automation_signal(&req),
            "StartChangeRequestExecution" => self.start_change_request_execution(&req),
            "StartExecutionPreview" => self.start_execution_preview(&req),
            "GetExecutionPreview" => self.get_execution_preview(&req),
            // Sessions
            "StartSession" => self.start_session(&req),
            "ResumeSession" => self.resume_session(&req),
            "TerminateSession" => self.terminate_session(&req),
            "DescribeSessions" => self.describe_sessions(&req),
            "StartAccessRequest" => self.start_access_request(&req),
            "GetAccessToken" => self.get_access_token(&req),
            // Managed instances
            "CreateActivation" => self.create_activation(&req),
            "DeleteActivation" => self.delete_activation(&req),
            "DescribeActivations" => self.describe_activations(&req),
            "DeregisterManagedInstance" => self.deregister_managed_instance(&req),
            "DescribeInstanceInformation" => self.describe_instance_information(&req),
            "DescribeInstanceProperties" => self.describe_instance_properties(&req),
            "UpdateManagedInstanceRole" => self.update_managed_instance_role(&req),
            // Other
            "ListNodes" => self.list_nodes(&req),
            "ListNodesSummary" => self.list_nodes_summary(&req),
            "DescribeEffectiveInstanceAssociations" => {
                self.describe_effective_instance_associations(&req)
            }
            "DescribeInstanceAssociationsStatus" => {
                self.describe_instance_associations_status(&req)
            }
            // Synthetic defaults (no managed-instance fleet to track) and
            // service settings backed by real per-account state.
            "GetConnectionStatus" => self.get_connection_status(&req),
            "GetCalendarState" => self.get_calendar_state(&req),
            "DescribePatchGroupState" => self.describe_patch_group_state(&req),
            "DescribePatchProperties" => self.describe_patch_properties(&req),
            "GetDefaultPatchBaseline" => self.get_default_patch_baseline(&req),
            "RegisterDefaultPatchBaseline" => self.register_default_patch_baseline(&req),
            "DescribeAvailablePatches" => self.describe_available_patches(&req),
            "GetServiceSetting" => self.get_service_setting(&req),
            "ResetServiceSetting" => self.reset_service_setting(&req),
            "UpdateServiceSetting" => self.update_service_setting(&req),
            _ => Err(AwsServiceError::action_not_implemented("ssm", &req.action)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "PutParameter",
            "GetParameter",
            "GetParameters",
            "GetParametersByPath",
            "DeleteParameter",
            "DeleteParameters",
            "DescribeParameters",
            "GetParameterHistory",
            "AddTagsToResource",
            "RemoveTagsFromResource",
            "ListTagsForResource",
            "LabelParameterVersion",
            "UnlabelParameterVersion",
            "CreateDocument",
            "GetDocument",
            "DeleteDocument",
            "UpdateDocument",
            "DescribeDocument",
            "UpdateDocumentDefaultVersion",
            "ListDocuments",
            "DescribeDocumentPermission",
            "ModifyDocumentPermission",
            "SendCommand",
            "ListCommands",
            "GetCommandInvocation",
            "ListCommandInvocations",
            "CancelCommand",
            "CreateMaintenanceWindow",
            "DescribeMaintenanceWindows",
            "GetMaintenanceWindow",
            "DeleteMaintenanceWindow",
            "UpdateMaintenanceWindow",
            "RegisterTargetWithMaintenanceWindow",
            "DeregisterTargetFromMaintenanceWindow",
            "DescribeMaintenanceWindowTargets",
            "RegisterTaskWithMaintenanceWindow",
            "DeregisterTaskFromMaintenanceWindow",
            "DescribeMaintenanceWindowTasks",
            "CreatePatchBaseline",
            "DeletePatchBaseline",
            "DescribePatchBaselines",
            "GetPatchBaseline",
            "RegisterPatchBaselineForPatchGroup",
            "DeregisterPatchBaselineForPatchGroup",
            "GetPatchBaselineForPatchGroup",
            "DescribePatchGroups",
            // Associations
            "CreateAssociation",
            "DescribeAssociation",
            "DeleteAssociation",
            "ListAssociations",
            "UpdateAssociation",
            "ListAssociationVersions",
            "UpdateAssociationStatus",
            "StartAssociationsOnce",
            "CreateAssociationBatch",
            "DescribeAssociationExecutions",
            "DescribeAssociationExecutionTargets",
            // OpsItems
            "CreateOpsItem",
            "GetOpsItem",
            "UpdateOpsItem",
            "DeleteOpsItem",
            "DescribeOpsItems",
            // Document extras
            "ListDocumentVersions",
            "ListDocumentMetadataHistory",
            "UpdateDocumentMetadata",
            // Resource policies
            "PutResourcePolicy",
            "GetResourcePolicies",
            "DeleteResourcePolicy",
            // Inventory
            "PutInventory",
            "GetInventory",
            "GetInventorySchema",
            "ListInventoryEntries",
            "DeleteInventory",
            "DescribeInventoryDeletions",
            // Compliance
            "PutComplianceItems",
            "ListComplianceItems",
            "ListComplianceSummaries",
            "ListResourceComplianceSummaries",
            // Maintenance window details
            "UpdateMaintenanceWindowTarget",
            "UpdateMaintenanceWindowTask",
            "GetMaintenanceWindowTask",
            "GetMaintenanceWindowExecution",
            "GetMaintenanceWindowExecutionTask",
            "GetMaintenanceWindowExecutionTaskInvocation",
            "DescribeMaintenanceWindowExecutions",
            "DescribeMaintenanceWindowExecutionTasks",
            "DescribeMaintenanceWindowExecutionTaskInvocations",
            "DescribeMaintenanceWindowSchedule",
            "DescribeMaintenanceWindowsForTarget",
            "CancelMaintenanceWindowExecution",
            // Patch management details
            "UpdatePatchBaseline",
            "DescribeInstancePatchStates",
            "DescribeInstancePatchStatesForPatchGroup",
            "DescribeInstancePatches",
            "DescribeEffectivePatchesForPatchBaseline",
            "GetDeployablePatchSnapshotForInstance",
            // Resource data sync
            "CreateResourceDataSync",
            "DeleteResourceDataSync",
            "ListResourceDataSync",
            "UpdateResourceDataSync",
            // Cloud connectors
            "CreateCloudConnector",
            "DeleteCloudConnector",
            "GetCloudConnector",
            "ListCloudConnectors",
            "UpdateCloudConnector",
            "ValidateCloudConnector",
            // OpsItem related items
            "AssociateOpsItemRelatedItem",
            "DisassociateOpsItemRelatedItem",
            "ListOpsItemRelatedItems",
            "ListOpsItemEvents",
            // OpsMetadata
            "CreateOpsMetadata",
            "GetOpsMetadata",
            "UpdateOpsMetadata",
            "DeleteOpsMetadata",
            "ListOpsMetadata",
            // OpsMetadata extras
            "GetOpsSummary",
            // Automation
            "StartAutomationExecution",
            "StopAutomationExecution",
            "GetAutomationExecution",
            "DescribeAutomationExecutions",
            "DescribeAutomationStepExecutions",
            "SendAutomationSignal",
            "StartChangeRequestExecution",
            "StartExecutionPreview",
            "GetExecutionPreview",
            // Sessions
            "StartSession",
            "ResumeSession",
            "TerminateSession",
            "DescribeSessions",
            "StartAccessRequest",
            "GetAccessToken",
            // Managed instances
            "CreateActivation",
            "DeleteActivation",
            "DescribeActivations",
            "DeregisterManagedInstance",
            "DescribeInstanceInformation",
            "DescribeInstanceProperties",
            "UpdateManagedInstanceRole",
            // Other
            "ListNodes",
            "ListNodesSummary",
            "DescribeEffectiveInstanceAssociations",
            "DescribeInstanceAssociationsStatus",
            // Synthetic defaults + service settings
            "GetConnectionStatus",
            "GetCalendarState",
            "DescribePatchGroupState",
            "DescribePatchProperties",
            "GetDefaultPatchBaseline",
            "RegisterDefaultPatchBaseline",
            "DescribeAvailablePatches",
            "GetServiceSetting",
            "ResetServiceSetting",
            "UpdateServiceSetting",
        ]
    }
}

mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
mod tests;
