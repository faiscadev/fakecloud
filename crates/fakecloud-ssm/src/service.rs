use std::collections::HashMap;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{
    AutomationExecution, ComplianceItem, ExecutionPreview, InventoryDeletion, InventoryEntry,
    InventoryItem, MaintenanceWindow, MaintenanceWindowTarget, MaintenanceWindowTask,
    OpsItemRelatedItem, OpsMetadataEntry, PatchBaseline, PatchGroup, ResourceDataSync,
    SharedSsmState, SsmActivation, SsmAssociation, SsmAssociationVersion, SsmCommand, SsmDocument,
    SsmDocumentVersion, SsmOpsItem, SsmParameter, SsmParameterVersion, SsmResourcePolicy,
    SsmServiceSetting, SsmSession,
};

use fakecloud_secretsmanager::state::SharedSecretsManagerState;

const PARAMETER_VERSION_LIMIT: i64 = 100;

pub struct SsmService {
    state: SharedSsmState,
    secretsmanager_state: Option<SharedSecretsManagerState>,
}

impl SsmService {
    pub fn new(state: SharedSsmState) -> Self {
        Self {
            state,
            secretsmanager_state: None,
        }
    }

    pub fn with_secretsmanager(mut self, sm_state: SharedSecretsManagerState) -> Self {
        self.secretsmanager_state = Some(sm_state);
        self
    }
}

#[async_trait]
impl AwsService for SsmService {
    fn service_name(&self) -> &str {
        "ssm"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
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
            // Stubs
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
        }
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
            // Stubs
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

fn parse_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Object(Default::default()))
}

fn json_resp(body: Value) -> AwsResponse {
    AwsResponse::json(StatusCode::OK, serde_json::to_string(&body).unwrap())
}

/// Normalize a parameter name - try looking up with and without leading slash
fn lookup_param<'a>(
    parameters: &'a std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<&'a SsmParameter> {
    // Direct lookup
    if let Some(p) = parameters.get(name) {
        return Some(p);
    }
    // Try with leading slash added/removed
    if let Some(stripped) = name.strip_prefix('/') {
        parameters.get(stripped)
    } else {
        parameters.get(&format!("/{name}"))
    }
}

fn lookup_param_mut<'a>(
    parameters: &'a mut std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<&'a mut SsmParameter> {
    // Direct lookup first
    if parameters.contains_key(name) {
        return parameters.get_mut(name);
    }
    // Try alternate form
    let alt = if let Some(stripped) = name.strip_prefix('/') {
        stripped.to_string()
    } else {
        format!("/{name}")
    };
    parameters.get_mut(&alt)
}

fn remove_param(
    parameters: &mut std::collections::BTreeMap<String, SsmParameter>,
    name: &str,
) -> Option<SsmParameter> {
    if let Some(p) = parameters.remove(name) {
        return Some(p);
    }
    let alt = if let Some(stripped) = name.strip_prefix('/') {
        stripped.to_string()
    } else {
        format!("/{name}")
    };
    parameters.remove(&alt)
}

/// Convert document content between JSON and YAML formats.
/// Falls back to returning content as-is if conversion isn't possible.
fn convert_document_content(content: &str, from_format: &str, to_format: &str) -> String {
    if from_format == to_format {
        return content.to_string();
    }

    // Parse the content from its source format
    let parsed: Option<serde_json::Value> = match from_format {
        "YAML" => serde_yaml::from_str(content).ok(),
        _ => serde_json::from_str(content).ok(),
    };

    if let Some(val) = parsed {
        match to_format {
            "JSON" => serde_json::to_string(&val).unwrap_or_else(|_| content.to_string()),
            "YAML" => serde_yaml::to_string(&val).unwrap_or_else(|_| content.to_string()),
            _ => content.to_string(),
        }
    } else {
        content.to_string()
    }
}

fn param_arn(region: &str, account_id: &str, name: &str) -> String {
    if name.starts_with('/') {
        format!("arn:aws:ssm:{region}:{account_id}:parameter{name}")
    } else {
        format!("arn:aws:ssm:{region}:{account_id}:parameter/{name}")
    }
}

/// Rewrite the region component of a parameter ARN.
fn rewrite_arn_region(arn: &str, region: &str) -> String {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() == 6 {
        format!(
            "{}:{}:{}:{}:{}:{}",
            parts[0], parts[1], parts[2], region, parts[4], parts[5]
        )
    } else {
        arn.to_string()
    }
}

fn param_to_json(p: &SsmParameter, with_value: bool, with_decryption: bool, region: &str) -> Value {
    let arn = rewrite_arn_region(&p.arn, region);
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": arn,
        "LastModifiedDate": p.last_modified.timestamp_millis() as f64 / 1000.0,
        "DataType": p.data_type,
    });
    if with_value {
        if p.param_type == "SecureString" {
            let key_id = p.key_id.as_deref().unwrap_or("alias/aws/ssm");
            if with_decryption {
                // Decrypted: return plain value
                v["Value"] = json!(p.value);
            } else {
                // Not decrypted: return kms:KEY_ID:VALUE placeholder
                v["Value"] = json!(format!("kms:{}:{}", key_id, p.value));
            }
        } else {
            v["Value"] = json!(p.value);
        }
    }
    v
}

fn param_to_describe_json(p: &SsmParameter, region: &str) -> Value {
    let arn = rewrite_arn_region(&p.arn, region);
    let mut v = json!({
        "Name": p.name,
        "Type": p.param_type,
        "Version": p.version,
        "ARN": arn,
        "LastModifiedDate": p.last_modified.timestamp_millis() as f64 / 1000.0,
        "LastModifiedUser": "N/A",
        "DataType": p.data_type,
        "Tier": p.tier,
    });
    if let Some(desc) = &p.description {
        v["Description"] = json!(desc);
    }
    if let Some(pattern) = &p.allowed_pattern {
        v["AllowedPattern"] = json!(pattern);
    }
    if let Some(key_id) = &p.key_id {
        v["KeyId"] = json!(key_id);
    }
    // Add policies if present and valid JSON
    if let Some(policies_str) = &p.policies {
        if let Ok(parsed) = serde_json::from_str::<Value>(policies_str) {
            if let Some(arr) = parsed.as_array() {
                let policy_objects: Vec<Value> = arr
                    .iter()
                    .filter_map(|p| p.as_str())
                    .map(|p| {
                        json!({
                            "PolicyText": p,
                            "PolicyType": p,
                            "PolicyStatus": "Finished",
                        })
                    })
                    .collect();
                if !policy_objects.is_empty() {
                    v["Policies"] = json!(policy_objects);
                }
            }
        }
    }
    v
}

/// Validate parameter name restrictions. Returns an error message string on failure.
fn validate_param_name(name: &str) -> Option<AwsServiceError> {
    let lower = name.to_lowercase();

    if let Some(stripped) = name.strip_prefix('/') {
        // Path-style names
        let first_segment = stripped.split('/').next().unwrap_or("");
        let first_lower = first_segment.to_lowercase();
        if first_lower.starts_with("aws") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("No access to reserved parameter name: {name}."),
            ));
        }
        if first_lower.starts_with("ssm") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter name: can't be prefixed with \"ssm\" (case-insensitive). \
                 If formed as a path, it can consist of sub-paths divided by slash \
                 symbol; each sub-path can be formed as a mix of letters, numbers \
                 and the following 3 symbols .-_"
                    .to_string(),
            ));
        }
    } else {
        // Non-path names
        if lower.starts_with("aws") || lower.starts_with("ssm") {
            return Some(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter name: can't be prefixed with \"aws\" or \"ssm\" (case-insensitive)."
                    .to_string(),
            ));
        }
    }
    None
}

/// Parse a parameter name that may include version or label selector.
/// Returns (base_name, selector) where selector can be version number or label string.
enum ParamSelector {
    None,
    Version(i64),
    Label(String),
    Invalid(String), // name with too many colons
}

fn parse_param_selector(name: &str) -> (&str, ParamSelector) {
    // Check for `:` separator (version or label)
    if let Some(colon_pos) = name.rfind(':') {
        let base = &name[..colon_pos];
        let selector = &name[colon_pos + 1..];

        // Check if there's another colon (invalid)
        if base.contains(':') {
            return (name, ParamSelector::Invalid(name.to_string()));
        }

        if let Ok(version) = selector.parse::<i64>() {
            (base, ParamSelector::Version(version))
        } else {
            (base, ParamSelector::Label(selector.to_string()))
        }
    } else {
        (name, ParamSelector::None)
    }
}

impl SsmService {
    fn put_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let value = body["Value"]
            .as_str()
            .ok_or_else(|| missing("Value"))?
            .to_string();

        // Validate empty value
        if value.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: \
                 Value '' at 'value' failed to satisfy constraint: \
                 Member must have length greater than or equal to 1.",
            ));
        }

        let param_type = body["Type"].as_str().map(|s| s.to_string());
        let overwrite = body["Overwrite"].as_bool().unwrap_or(false);
        let description = body["Description"].as_str().map(|s| s.to_string());
        let key_id = body["KeyId"].as_str().map(|s| s.to_string());
        let allowed_pattern = body["AllowedPattern"].as_str().map(|s| s.to_string());
        let data_type = body["DataType"].as_str().unwrap_or("text").to_string();
        let tier = body["Tier"].as_str().unwrap_or("Standard").to_string();
        let policies = body["Policies"].as_str().map(|s| s.to_string());
        let tags: Option<Vec<(String, String)>> = body["Tags"].as_array().map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let k = t["Key"].as_str()?;
                    let v = t["Value"].as_str()?;
                    Some((k.to_string(), v.to_string()))
                })
                .collect()
        });

        // Validate data type
        if !["text", "aws:ec2:image"].contains(&data_type.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following data type is not supported: {data_type} \
                     (Data type names are all lowercase.)"
                ),
            ));
        }

        // Validate param type
        if let Some(ref pt) = param_type {
            if !["String", "StringList", "SecureString"].contains(&pt.as_str()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{pt}' at 'type' \
                         failed to satisfy constraint: Member must satisfy enum value set: \
                         [SecureString, StringList, String]"
                    ),
                ));
            }
        }

        // Validate name
        if let Some(err) = validate_param_name(&name) {
            return Err(err);
        }

        let mut state = self.state.write();

        if let Some(existing) = lookup_param_mut(&mut state.parameters, &name) {
            if !overwrite {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterAlreadyExists",
                    "The parameter already exists. To overwrite this value, set the \
                     overwrite option in the request to true.",
                ));
            }

            // Cannot have tags and overwrite at the same time
            if tags.is_some() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Invalid request: tags and overwrite can't be used together.",
                ));
            }

            // Check version limit
            if existing.version >= PARAMETER_VERSION_LIMIT {
                // Check if oldest version has a label
                let oldest_version = existing
                    .history
                    .first()
                    .map(|h| h.version)
                    .unwrap_or(existing.version);
                let oldest_has_label = existing
                    .labels
                    .get(&oldest_version)
                    .is_some_and(|l| !l.is_empty());

                if oldest_has_label {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ParameterMaxVersionLimitExceeded",
                        format!(
                            "You attempted to create a new version of {} by calling \
                             the PutParameter API with the overwrite flag. Version {}, \
                             the oldest version, can't be deleted because it has a label \
                             associated with it. Move the label to another version of the \
                             parameter, and try again.",
                            name, oldest_version
                        ),
                    ));
                }

                // Delete oldest version from history to make room
                if !existing.history.is_empty() {
                    let removed = existing.history.remove(0);
                    existing.labels.remove(&removed.version);
                }
            }

            let now = Utc::now();
            let current_labels = existing
                .labels
                .get(&existing.version)
                .cloned()
                .unwrap_or_default();
            existing.history.push(SsmParameterVersion {
                value: existing.value.clone(),
                version: existing.version,
                last_modified: existing.last_modified,
                param_type: existing.param_type.clone(),
                description: existing.description.clone(),
                key_id: existing.key_id.clone(),
                labels: current_labels,
            });
            existing.version += 1;
            existing.value = value;
            existing.last_modified = now;

            // Only update these if provided
            if let Some(pt) = param_type {
                existing.param_type = pt;
            }
            if description.is_some() {
                existing.description = description;
            }
            if key_id.is_some() {
                existing.key_id = key_id;
            }
            // Always update data_type if explicitly provided
            if body["DataType"].as_str().is_some() {
                existing.data_type = data_type;
            }

            let resp_tier = existing.tier.clone();
            return Ok(json_resp(json!({
                "Version": existing.version,
                "Tier": resp_tier,
            })));
        }

        // New parameter - type is required
        let param_type = match param_type {
            Some(pt) => pt,
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "A parameter type is required when you create a parameter.",
                ));
            }
        };

        let now = Utc::now();
        let arn = param_arn(&req.region, &state.account_id, &name);

        let mut tag_map = HashMap::new();
        if let Some(tag_list) = tags {
            for (k, v) in tag_list {
                tag_map.insert(k, v);
            }
        }

        let param = SsmParameter {
            name: name.clone(),
            value,
            param_type,
            version: 1,
            arn,
            last_modified: now,
            history: Vec::new(),
            labels: HashMap::new(),
            tags: tag_map,
            description,
            allowed_pattern,
            key_id,
            data_type,
            tier: tier.clone(),
            policies,
        };

        state.parameters.insert(name, param);
        Ok(json_resp(json!({
            "Version": 1,
            "Tier": tier,
        })))
    }

    /// Resolve a Secrets Manager reference parameter.
    /// Path format: /aws/reference/secretsmanager/{secret-name}
    fn resolve_secretsmanager_param(
        &self,
        raw_name: &str,
        secret_name: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sm_state = self.secretsmanager_state.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;

        let sm = sm_state.read();
        let secret = sm.secrets.get(secret_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;

        if secret.deleted {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            ));
        }

        // Get the current version's secret string
        let current_vid = secret.current_version_id.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;
        let version = secret.versions.get(current_vid).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterNotFound",
                format!(
                    "An error occurred (ParameterNotFound) when referencing \
                     Secrets Manager: Secret {raw_name} not found.",
                ),
            )
        })?;

        let value = version.secret_string.as_deref().unwrap_or("").to_string();

        let ssm_state = self.state.read();
        let arn = format!(
            "arn:aws:ssm:{region}:{}:parameter{}",
            ssm_state.account_id, raw_name
        );

        Ok(json_resp(json!({
            "Parameter": {
                "Name": raw_name,
                "Type": "SecureString",
                "Value": value,
                "Version": 0,
                "ARN": arn,
                "LastModifiedDate": version.created_at.timestamp_millis() as f64 / 1000.0,
                "DataType": "text",
                "SourceResult": secret.arn,
            }
        })))
    }

    fn get_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let raw_name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);

        // Check for Secrets Manager references - require WithDecryption=true
        if raw_name.starts_with("/aws/reference/secretsmanager/") && !with_decryption {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "WithDecryption flag must be True for retrieving a Secret Manager secret.",
            ));
        }

        // Resolve Secrets Manager references via cross-service lookup
        if let Some(secret_name) = raw_name.strip_prefix("/aws/reference/secretsmanager/") {
            return self.resolve_secretsmanager_param(raw_name, secret_name, &req.region);
        }

        let state = self.state.read();

        // Handle ARN-style names directly (they contain many colons)
        if raw_name.starts_with("arn:aws:ssm:") {
            let param = resolve_param_by_name_or_arn(&state, raw_name)?;
            return Ok(json_resp(json!({
                "Parameter": param_to_json(param, true, with_decryption, &req.region),
            })));
        }

        let (base_name, selector) = parse_param_selector(raw_name);

        // Check for invalid selectors (too many colons)
        if let ParamSelector::Invalid(n) = selector {
            return Err(param_not_found(&n));
        }

        // Try looking up by name or by ARN - use raw_name in error for full context
        let param = resolve_param_by_name_or_arn(&state, base_name)
            .map_err(|_| param_not_found(raw_name))?;

        match selector {
            ParamSelector::None => Ok(json_resp(json!({
                "Parameter": param_to_json(param, true, with_decryption, &req.region),
            }))),
            ParamSelector::Version(ver) => {
                if param.version == ver {
                    return Ok(json_resp(json!({
                        "Parameter": param_to_json(param, true, with_decryption, &req.region),
                    })));
                }
                // Look in history
                if let Some(hist) = param.history.iter().find(|h| h.version == ver) {
                    let mut v = json!({
                        "Name": param.name,
                        "Type": hist.param_type,
                        "Version": hist.version,
                        "ARN": rewrite_arn_region(&param.arn, &req.region),
                        "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
                        "DataType": param.data_type,
                    });
                    if param.param_type == "SecureString" && !with_decryption {
                        v["Value"] = json!("****");
                    } else {
                        v["Value"] = json!(hist.value);
                    }
                    return Ok(json_resp(json!({ "Parameter": v })));
                }
                Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterVersionNotFound",
                    format!(
                        "Systems Manager could not find version {} of {}. \
                         Verify the version and try again.",
                        ver, base_name
                    ),
                ))
            }
            ParamSelector::Label(label) => {
                // Find version with this label
                for (ver, labels) in &param.labels {
                    if labels.contains(&label) {
                        if *ver == param.version {
                            return Ok(json_resp(json!({
                                "Parameter": param_to_json(param, true, with_decryption, &req.region),
                            })));
                        }
                        if let Some(hist) = param.history.iter().find(|h| h.version == *ver) {
                            let mut v = json!({
                                "Name": param.name,
                                "Type": hist.param_type,
                                "Version": hist.version,
                                "ARN": rewrite_arn_region(&param.arn, &req.region),
                                "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
                                "DataType": param.data_type,
                            });
                            if param.param_type == "SecureString" && !with_decryption {
                                v["Value"] = json!("****");
                            } else {
                                v["Value"] = json!(hist.value);
                            }
                            return Ok(json_resp(json!({ "Parameter": v })));
                        }
                    }
                }
                Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ParameterVersionLabelNotFound",
                    format!(
                        "Systems Manager could not find label {} for parameter {}. \
                         Verify the label and try again.",
                        label, base_name
                    ),
                ))
            }
            ParamSelector::Invalid(_) => unreachable!(),
        }
    }

    fn get_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let names = body["Names"].as_array().ok_or_else(|| missing("Names"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);

        // Validate max 10 names
        if names.len() > 10 {
            let name_strs: Vec<&str> = names.iter().filter_map(|n| n.as_str()).collect();
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: \
                     Value '[{}]' at 'names' failed to satisfy constraint: \
                     Member must have length less than or equal to 10.",
                    name_strs.join(", ")
                ),
            ));
        }

        let state = self.state.read();
        let mut parameters = Vec::new();
        let mut invalid = Vec::new();
        let mut seen_names = std::collections::HashSet::new();

        for name_val in names {
            if let Some(raw_name) = name_val.as_str() {
                // Deduplicate
                if !seen_names.insert(raw_name.to_string()) {
                    continue;
                }

                let (base_name, selector) = parse_param_selector(raw_name);

                match selector {
                    ParamSelector::Invalid(_) => {
                        invalid.push(raw_name.to_string());
                    }
                    ParamSelector::None => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            parameters.push(param_to_json(
                                param,
                                true,
                                with_decryption,
                                &req.region,
                            ));
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                    ParamSelector::Version(ver) => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            if param.version == ver {
                                parameters.push(param_to_json(
                                    param,
                                    true,
                                    with_decryption,
                                    &req.region,
                                ));
                            } else if let Some(hist) =
                                param.history.iter().find(|h| h.version == ver)
                            {
                                let mut v = json!({
                                    "Name": param.name,
                                    "Type": hist.param_type,
                                    "Version": hist.version,
                                    "ARN": rewrite_arn_region(&param.arn, &req.region),
                                    "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
                                    "DataType": param.data_type,
                                });
                                if hist.param_type == "SecureString" && !with_decryption {
                                    v["Value"] = json!("****");
                                } else {
                                    v["Value"] = json!(hist.value);
                                }
                                parameters.push(v);
                            } else {
                                invalid.push(raw_name.to_string());
                            }
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                    ParamSelector::Label(ref label) => {
                        if let Some(param) = lookup_param(&state.parameters, base_name) {
                            let mut found = false;
                            for (ver, labels) in &param.labels {
                                if labels.contains(label) {
                                    if *ver == param.version {
                                        parameters.push(param_to_json(
                                            param,
                                            true,
                                            with_decryption,
                                            &req.region,
                                        ));
                                    } else if let Some(hist) =
                                        param.history.iter().find(|h| h.version == *ver)
                                    {
                                        let mut v = json!({
                                            "Name": param.name,
                                            "Type": hist.param_type,
                                            "Version": hist.version,
                                            "ARN": rewrite_arn_region(&param.arn, &req.region),
                                            "LastModifiedDate": hist.last_modified.timestamp_millis() as f64 / 1000.0,
                                            "DataType": param.data_type,
                                        });
                                        if hist.param_type == "SecureString" && !with_decryption {
                                            v["Value"] = json!("****");
                                        } else {
                                            v["Value"] = json!(hist.value);
                                        }
                                        parameters.push(v);
                                    }
                                    found = true;
                                    break;
                                }
                            }
                            if !found {
                                invalid.push(raw_name.to_string());
                            }
                        } else {
                            invalid.push(raw_name.to_string());
                        }
                    }
                }
            }
        }

        Ok(json_resp(json!({
            "Parameters": parameters,
            "InvalidParameters": invalid,
        })))
    }

    fn get_parameters_by_path(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let path = body["Path"].as_str().ok_or_else(|| missing("Path"))?;
        let recursive = body["Recursive"].as_bool().unwrap_or(false);
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);
        let filters = body["ParameterFilters"].as_array().cloned();
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Validate MaxResults
        if max_results > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: \
                     Value {} at 'maxResults' failed to satisfy constraint: \
                     Member must have value less than or equal to 10",
                    max_results
                ),
            ));
        }

        // Validate path
        if !is_valid_param_path(path) {
            return Err(invalid_path_error(path));
        }

        // Validate ParameterFilters for by-path (only Type, KeyId, Label, tag:* allowed)
        if let Some(ref f) = filters {
            validate_parameter_filters_by_path(f)?;
        }

        let state = self.state.read();
        let prefix = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };

        let is_root = path == "/";
        let targets_aws = path.starts_with("/aws/") || path.starts_with("/aws");

        let all_params: Vec<&SsmParameter> = state
            .parameters
            .values()
            .filter(|p| {
                // Exclude /aws/ prefix params unless path explicitly targets them
                if !targets_aws && p.name.starts_with("/aws/") {
                    return false;
                }
                true
            })
            .filter(|p| {
                if is_root {
                    if recursive {
                        true
                    } else {
                        // Root level: params without '/' or with exactly one leading '/'
                        // and no further '/' in the name
                        if p.name.starts_with('/') {
                            // e.g., "/foo" is root-level, "/foo/bar" is not
                            !p.name[1..].contains('/')
                        } else {
                            // Non-path params like "foo" are at root
                            !p.name.contains('/')
                        }
                    }
                } else {
                    if p.name.starts_with(&prefix) {
                        if recursive {
                            true
                        } else {
                            !p.name[prefix.len()..].contains('/')
                        }
                    } else {
                        false
                    }
                }
            })
            .filter(|p| apply_parameter_filters(p, filters.as_ref()))
            .collect();

        let page = if next_token_offset < all_params.len() {
            &all_params[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() >= max_results;
        let parameters: Vec<Value> = page
            .iter()
            .take(max_results)
            .map(|p| param_to_json(p, true, with_decryption, &req.region))
            .collect();

        let mut resp = json!({ "Parameters": parameters });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn delete_parameter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        if remove_param(&mut state.parameters, name).is_none() {
            return Err(param_not_found(name));
        }

        Ok(json_resp(json!({})))
    }

    fn delete_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let names = body["Names"].as_array().ok_or_else(|| missing("Names"))?;

        let mut state = self.state.write();
        let mut deleted = Vec::new();
        let mut invalid = Vec::new();

        for name_val in names {
            if let Some(name) = name_val.as_str() {
                if remove_param(&mut state.parameters, name).is_some() {
                    deleted.push(name.to_string());
                } else {
                    invalid.push(name.to_string());
                }
            }
        }

        Ok(json_resp(json!({
            "DeletedParameters": deleted,
            "InvalidParameters": invalid,
        })))
    }

    fn describe_parameters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let param_filters = body["ParameterFilters"].as_array().cloned();
        let old_filters = body["Filters"].as_array().cloned();
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Can't use both Filters and ParameterFilters
        if param_filters.is_some() && old_filters.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "You can use either Filters or ParameterFilters in a single request.",
            ));
        }

        // Validate ParameterFilters
        if let Some(ref filters) = param_filters {
            validate_parameter_filters(filters)?;
        }

        let state = self.state.read();

        // Check if any filter explicitly targets /aws/ prefix paths
        let targets_aws_prefix = param_filters.as_ref().is_some_and(|filters| {
            filters.iter().any(|f| {
                let key = f["Key"].as_str().unwrap_or("");
                if key == "Path" {
                    f["Values"].as_array().is_some_and(|vals| {
                        vals.iter()
                            .any(|v| v.as_str().is_some_and(|s| s.starts_with("/aws")))
                    })
                } else if key == "Name" {
                    f["Values"].as_array().is_some_and(|vals| {
                        vals.iter().any(|v| {
                            v.as_str().is_some_and(|s| {
                                let n = s.strip_prefix('/').unwrap_or(s);
                                n.starts_with("aws/") || n.starts_with("aws")
                            })
                        })
                    })
                } else {
                    false
                }
            })
        });

        let all_params: Vec<&SsmParameter> = state
            .parameters
            .values()
            .filter(|p| {
                // Exclude /aws/ prefix params from user queries unless explicitly targeted
                if !targets_aws_prefix && p.name.starts_with("/aws/") {
                    return false;
                }
                true
            })
            .filter(|p| apply_parameter_filters(p, param_filters.as_ref()))
            .filter(|p| apply_old_filters(p, old_filters.as_ref()))
            .collect();

        let page = if next_token_offset < all_params.len() {
            &all_params[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() >= max_results;
        let parameters: Vec<Value> = page
            .iter()
            .take(max_results)
            .map(|p| param_to_describe_json(p, &req.region))
            .collect();

        let mut resp = json!({ "Parameters": parameters });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn get_parameter_history(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let with_decryption = body["WithDecryption"].as_bool().unwrap_or(false);
        let max_results = body["MaxResults"].as_i64();
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        // Validate MaxResults
        if let Some(mr) = max_results {
            if mr > 50 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{mr}' at 'maxResults' \
                         failed to satisfy constraint: Member must have value less than \
                         or equal to 50."
                    ),
                ));
            }
        }
        let max_results = max_results.unwrap_or(50) as usize;

        let state = self.state.read();
        let param = state
            .parameters
            .get(name)
            .ok_or_else(|| param_not_found(name))?;

        let mut all_history: Vec<Value> = param
            .history
            .iter()
            .map(|h| {
                let value = if h.param_type == "SecureString" && !with_decryption {
                    let kid = h.key_id.as_deref().unwrap_or("alias/aws/ssm");
                    format!("kms:{}:{}", kid, h.value)
                } else {
                    h.value.clone()
                };
                let mut entry = json!({
                    "Name": param.name,
                    "Value": value,
                    "Version": h.version,
                    "LastModifiedDate": h.last_modified.timestamp_millis() as f64 / 1000.0,
                    "Type": h.param_type,
                });
                if let Some(desc) = &h.description {
                    entry["Description"] = json!(desc);
                }
                if let Some(kid) = &h.key_id {
                    entry["KeyId"] = json!(kid);
                }
                let labels = param.labels.get(&h.version).cloned().unwrap_or_default();
                entry["Labels"] = json!(labels);
                entry
            })
            .collect();

        // Include current version
        let current_value = if param.param_type == "SecureString" && !with_decryption {
            let kid = param.key_id.as_deref().unwrap_or("alias/aws/ssm");
            format!("kms:{}:{}", kid, param.value)
        } else {
            param.value.clone()
        };
        let mut current = json!({
            "Name": param.name,
            "Value": current_value,
            "Version": param.version,
            "LastModifiedDate": param.last_modified.timestamp_millis() as f64 / 1000.0,
            "Type": param.param_type,
        });
        if let Some(desc) = &param.description {
            current["Description"] = json!(desc);
        }
        if let Some(kid) = &param.key_id {
            current["KeyId"] = json!(kid);
        }
        let current_labels = param
            .labels
            .get(&param.version)
            .cloned()
            .unwrap_or_default();
        current["Labels"] = json!(current_labels);
        all_history.push(current);

        let page = if next_token_offset < all_history.len() {
            &all_history[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() >= max_results;
        let result: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "Parameters": result });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn add_tags_to_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;
        let tags = body["Tags"].as_array().ok_or_else(|| missing("Tags"))?;

        let mut state = self.state.write();

        match resource_type {
            "Parameter" => {
                let param = lookup_param_mut(&mut state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        param.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "Document" => {
                let doc = state
                    .documents
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        doc.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        mw.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for tag in tags {
                    if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                        pb.tags.insert(key.to_string(), val.to_string());
                    }
                }
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidResourceType",
                    format!(
                        "{resource_type} is not a valid resource type. \
                         Valid resource types are: ManagedInstance, MaintenanceWindow, \
                         Parameter, PatchBaseline, OpsItem, Document."
                    ),
                ));
            }
        }

        Ok(json_resp(json!({})))
    }

    fn remove_tags_from_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_required("ResourceType", &body["ResourceType"])?;
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;
        let tag_keys = body["TagKeys"]
            .as_array()
            .ok_or_else(|| missing("TagKeys"))?;

        let mut state = self.state.write();

        match resource_type {
            "Parameter" => {
                let param = lookup_param_mut(&mut state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        param.tags.remove(k);
                    }
                }
            }
            "Document" => {
                let doc = state
                    .documents
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        doc.tags.remove(k);
                    }
                }
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        mw.tags.remove(k);
                    }
                }
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get_mut(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;

                for key in tag_keys {
                    if let Some(k) = key.as_str() {
                        pb.tags.remove(k);
                    }
                }
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidResourceType",
                    format!(
                        "{resource_type} is not a valid resource type. \
                         Valid resource types are: ManagedInstance, MaintenanceWindow, \
                         Parameter, PatchBaseline, OpsItem, Document."
                    ),
                ));
            }
        }

        Ok(json_resp(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resource_type = body["ResourceType"].as_str().unwrap_or("Parameter");
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?;

        let state = self.state.read();

        let tags: Vec<Value> = match resource_type {
            "Parameter" => {
                let param = lookup_param(&state.parameters, resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                param
                    .tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "Document" => {
                let doc = state
                    .documents
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                doc.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "MaintenanceWindow" => {
                let mw = state
                    .maintenance_windows
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                mw.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            "PatchBaseline" => {
                let pb = state
                    .patch_baselines
                    .get(resource_id)
                    .ok_or_else(|| invalid_resource_id(resource_id))?;
                pb.tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect()
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidResourceType",
                    format!(
                        "{resource_type} is not a valid resource type. \
                         Valid resource types are: ManagedInstance, MaintenanceWindow, \
                         Parameter, PatchBaseline, OpsItem, Document."
                    ),
                ));
            }
        };

        let mut tags = tags;
        tags.sort_by(|a, b| {
            let ka = a["Key"].as_str().unwrap_or("");
            let kb = b["Key"].as_str().unwrap_or("");
            ka.cmp(kb)
        });

        Ok(json_resp(json!({ "TagList": tags })))
    }

    fn label_parameter_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let labels = body["Labels"].as_array().ok_or_else(|| missing("Labels"))?;
        let version = body["ParameterVersion"].as_i64();

        let mut state = self.state.write();
        let param =
            lookup_param_mut(&mut state.parameters, name).ok_or_else(|| param_not_found(name))?;

        let target_version = version.unwrap_or(param.version);

        // Validate version exists
        let version_exists = param.version == target_version
            || param.history.iter().any(|h| h.version == target_version);
        if !version_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionNotFound",
                format!(
                    "Systems Manager could not find version {target_version} of {name}. \
                     Verify the version and try again."
                ),
            ));
        }

        let label_strings: Vec<String> = labels
            .iter()
            .filter_map(|l| l.as_str().map(|s| s.to_string()))
            .collect();

        // Validate label length (max 100)
        for label in &label_strings {
            if label.len() > 100 {
                let labels_display: Vec<&str> = label_strings.iter().map(|s| s.as_str()).collect();
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: \
                         Value '[{}]' at 'labels' failed to satisfy constraint: \
                         Member must satisfy constraint: \
                         [Member must have length less than or equal to 100, Member must \
                         have length greater than or equal to 1]",
                        labels_display.join(", ")
                    ),
                ));
            }
        }

        // Validate invalid labels (aws/ssm prefix, starts with digit, contains / or :)
        let mut invalid_labels = Vec::new();
        for label in &label_strings {
            let lower = label.to_lowercase();
            let is_invalid = lower.starts_with("aws")
                || lower.starts_with("ssm")
                || label.starts_with(|c: char| c.is_ascii_digit())
                || label.contains('/')
                || label.contains(':');
            if is_invalid {
                invalid_labels.push(label.clone());
            }
        }
        if !invalid_labels.is_empty() {
            return Ok(json_resp(json!({
                "InvalidLabels": invalid_labels,
                "ParameterVersion": target_version,
            })));
        }

        // Count current labels for target version
        let current_count = param
            .labels
            .get(&target_version)
            .map(|l| l.len())
            .unwrap_or(0);
        let new_unique: Vec<&String> = label_strings
            .iter()
            .filter(|l| {
                !param
                    .labels
                    .get(&target_version)
                    .is_some_and(|existing| existing.contains(l))
            })
            .collect();

        if current_count + new_unique.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionLabelLimitExceeded",
                "An error occurred (ParameterVersionLabelLimitExceeded) when \
                 calling the LabelParameterVersion operation: \
                 A parameter version can have maximum 10 labels.\
                 Move one or more labels to another version and try again.",
            ));
        }

        // Remove these labels from any other version (labels are unique across versions)
        for existing_labels in param.labels.values_mut() {
            existing_labels.retain(|l| !label_strings.contains(l));
        }
        // Remove empty entries
        param.labels.retain(|_, v| !v.is_empty());

        // Add labels to target version
        let entry = param.labels.entry(target_version).or_default();
        for label in &label_strings {
            if !entry.contains(label) {
                entry.push(label.clone());
            }
        }

        Ok(json_resp(json!({
            "InvalidLabels": [],
            "ParameterVersion": target_version,
        })))
    }

    fn unlabel_parameter_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let labels = body["Labels"].as_array().ok_or_else(|| missing("Labels"))?;
        let version = body["ParameterVersion"]
            .as_i64()
            .ok_or_else(|| missing("ParameterVersion"))?;

        let mut state = self.state.write();
        let param =
            lookup_param_mut(&mut state.parameters, name).ok_or_else(|| param_not_found(name))?;

        // Validate version exists
        let version_exists =
            param.version == version || param.history.iter().any(|h| h.version == version);
        if !version_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ParameterVersionNotFound",
                format!(
                    "Systems Manager could not find version {version} of {name}. \
                     Verify the version and try again."
                ),
            ));
        }

        let label_strings: Vec<String> = labels
            .iter()
            .filter_map(|l| l.as_str().map(|s| s.to_string()))
            .collect();

        // Find which labels don't exist on this version
        let invalid: Vec<String> = if let Some(existing) = param.labels.get(&version) {
            label_strings
                .iter()
                .filter(|l| !existing.contains(l))
                .cloned()
                .collect()
        } else {
            label_strings.clone()
        };

        // Remove labels
        if let Some(existing) = param.labels.get_mut(&version) {
            existing.retain(|l| !label_strings.contains(l));
        }

        // Clean up empty entries
        param.labels.retain(|_, v| !v.is_empty());

        Ok(json_resp(json!({
            "InvalidLabels": invalid,
            "RemovedLabels": label_strings.iter().filter(|l| !invalid.contains(l)).collect::<Vec<_>>(),
        })))
    }

    // ===== Document operations =====

    fn create_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let content = body["Content"]
            .as_str()
            .ok_or_else(|| missing("Content"))?
            .to_string();
        let doc_type = body["DocumentType"]
            .as_str()
            .unwrap_or("Command")
            .to_string();
        let doc_format = body["DocumentFormat"]
            .as_str()
            .unwrap_or("JSON")
            .to_string();
        let target_type = body["TargetType"].as_str().map(|s| s.to_string());
        let version_name = body["VersionName"].as_str().map(|s| s.to_string());

        let tags: HashMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        if state.documents.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DocumentAlreadyExists",
                "The specified document already exists.".to_string(),
            ));
        }

        // Validate content matches declared format
        if doc_format == "JSON" && serde_json::from_str::<Value>(&content).is_err() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDocumentContent",
                "The content for the document is not valid.",
            ));
        }

        let now = Utc::now();
        let content_hash = format!("{:x}", Sha256::digest(content.as_bytes()));

        let version = SsmDocumentVersion {
            content: content.clone(),
            document_version: "1".to_string(),
            version_name: version_name.clone(),
            created_date: now,
            status: "Active".to_string(),
            document_format: doc_format.clone(),
            is_default_version: true,
        };

        let doc = SsmDocument {
            name: name.clone(),
            content: content.clone(),
            document_type: doc_type.clone(),
            document_format: doc_format.clone(),
            target_type: target_type.clone(),
            version_name,
            tags,
            versions: vec![version],
            default_version: "1".to_string(),
            latest_version: "1".to_string(),
            created_date: now,
            owner: state.account_id.clone(),
            status: "Active".to_string(),
            permissions: HashMap::new(),
        };

        state.documents.insert(name.clone(), doc);

        let (doc_description, schema_ver, doc_params) =
            extract_document_metadata(&content, &doc_format);

        let mut desc_json = json!({
            "Name": name,
            "DocumentType": doc_type,
            "DocumentFormat": doc_format,
            "DocumentVersion": "1",
            "LatestVersion": "1",
            "DefaultVersion": "1",
            "Status": "Active",
            "CreatedDate": now.timestamp_millis() as f64 / 1000.0,
            "Owner": state.account_id,
            "SchemaVersion": schema_ver.as_deref().unwrap_or("2.2"),
            "PlatformTypes": ["Linux", "MacOS", "Windows"],
            "Hash": content_hash,
            "HashType": "Sha256",
        });
        if let Some(tt) = &target_type {
            desc_json["TargetType"] = json!(tt);
        }
        if let Some(vn) = state
            .documents
            .get(&name)
            .and_then(|d| d.version_name.as_ref())
        {
            desc_json["VersionName"] = json!(vn);
        }
        if let Some(d) = doc_description {
            desc_json["Description"] = json!(d);
        }
        if !doc_params.is_empty() {
            desc_json["Parameters"] = json!(doc_params);
        }
        // Include tags if present
        if let Some(doc) = state.documents.get(&name) {
            if !doc.tags.is_empty() {
                let tags_list: Vec<Value> = doc
                    .tags
                    .iter()
                    .map(|(k, v)| json!({"Key": k, "Value": v}))
                    .collect();
                desc_json["Tags"] = json!(tags_list);
            }
        }

        Ok(json_resp(json!({ "DocumentDescription": desc_json })))
    }

    fn get_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let version = body["DocumentVersion"].as_str();
        let version_name = body["VersionName"].as_str();
        let requested_format = body["DocumentFormat"].as_str();

        let state = self.state.read();
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        // Find the target version
        let ver = if let Some(vn) = version_name {
            // Lookup by VersionName (and optionally DocumentVersion)
            let candidates: Vec<&_> = doc
                .versions
                .iter()
                .filter(|v| v.version_name.as_deref() == Some(vn))
                .collect();
            if let Some(doc_ver) = version {
                // Both VersionName and DocumentVersion specified - must match
                candidates
                    .into_iter()
                    .find(|v| v.document_version == doc_ver)
                    .ok_or_else(|| doc_not_found(name))?
            } else {
                candidates
                    .first()
                    .copied()
                    .ok_or_else(|| doc_not_found(name))?
            }
        } else if let Some(doc_ver) = version {
            let target = if doc_ver == "$LATEST" {
                &doc.latest_version
            } else {
                doc_ver
            };
            doc.versions
                .iter()
                .find(|v| v.document_version == target)
                .ok_or_else(|| doc_not_found(name))?
        } else {
            doc.versions
                .iter()
                .find(|v| v.document_version == doc.default_version)
                .ok_or_else(|| doc_not_found(name))?
        };

        // Convert content format if requested
        let (content, format) = if let Some(fmt) = requested_format {
            let converted = convert_document_content(&ver.content, &ver.document_format, fmt);
            (converted, fmt.to_string())
        } else {
            // If stored as YAML but no explicit format requested, return as JSON
            let converted = convert_document_content(&ver.content, &ver.document_format, "JSON");
            (converted, "JSON".to_string())
        };

        let mut resp = json!({
            "Name": doc.name,
            "Content": content,
            "DocumentType": doc.document_type,
            "DocumentFormat": format,
            "DocumentVersion": ver.document_version,
            "Status": ver.status,
            "CreatedDate": ver.created_date.timestamp_millis() as f64 / 1000.0,
        });

        if let Some(ref vn) = ver.version_name {
            resp["VersionName"] = json!(vn);
        }

        Ok(json_resp(resp))
    }

    fn delete_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let doc_version = body["DocumentVersion"].as_str();
        let version_name = body["VersionName"].as_str();

        let mut state = self.state.write();

        if doc_version.is_some() || version_name.is_some() {
            // Deleting a specific version
            let doc = state
                .documents
                .get_mut(name)
                .ok_or_else(|| doc_not_found(name))?;

            // Find the target version
            let target_ver = if let Some(vn) = version_name {
                doc.versions
                    .iter()
                    .find(|v| v.version_name.as_deref() == Some(vn))
                    .map(|v| v.document_version.clone())
            } else {
                doc_version.map(|v| v.to_string())
            };

            if let Some(ver) = target_ver {
                if ver == doc.default_version {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidDocumentOperation",
                        "Default version of the document can't be deleted.",
                    ));
                }
                doc.versions.retain(|v| v.document_version != ver);
                // Update latest_version if we deleted the latest
                if doc.latest_version == ver {
                    doc.latest_version = doc
                        .versions
                        .iter()
                        .filter_map(|v| v.document_version.parse::<u64>().ok())
                        .max()
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "1".to_string());
                }
            } else {
                return Err(doc_not_found(name));
            }
        } else {
            // Delete the entire document
            if state.documents.remove(name).is_none() {
                return Err(doc_not_found(name));
            }
        }

        Ok(json_resp(json!({})))
    }

    fn update_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let content = body["Content"]
            .as_str()
            .ok_or_else(|| missing("Content"))?
            .to_string();
        let version_name = body["VersionName"].as_str().map(|s| s.to_string());
        let doc_format = body["DocumentFormat"].as_str();
        let target_version = body["DocumentVersion"].as_str();

        let mut state = self.state.write();
        let doc = state
            .documents
            .get_mut(name)
            .ok_or_else(|| doc_not_found(name))?;

        // Validate target version exists (if specified and not $LATEST)
        if let Some(ver) = target_version {
            if ver != "$LATEST" && !doc.versions.iter().any(|v| v.document_version == ver) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidDocument",
                    "The document version is not valid or does not exist.",
                ));
            }
        }

        // Check for duplicate content
        let new_hash = format!("{:x}", Sha256::digest(content.as_bytes()));
        for v in &doc.versions {
            let existing_hash = format!("{:x}", Sha256::digest(v.content.as_bytes()));
            if new_hash == existing_hash {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DuplicateDocumentContent",
                    "The content of the association document matches another \
                     document. Change the content of the document and try again.",
                ));
            }
        }

        // Check for duplicate version name
        if let Some(ref vn) = version_name {
            if doc
                .versions
                .iter()
                .any(|v| v.version_name.as_deref() == Some(vn))
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DuplicateDocumentVersionName",
                    "The specified version name is a duplicate.",
                ));
            }
        }

        let new_version_num = (doc.versions.len() + 1).to_string();
        let format = doc_format.unwrap_or(&doc.document_format).to_string();

        let (doc_description, schema_ver, doc_params) =
            extract_document_metadata(&content, &format);

        let version = SsmDocumentVersion {
            content: content.clone(),
            document_version: new_version_num.clone(),
            version_name,
            created_date: Utc::now(),
            status: "Active".to_string(),
            document_format: format.clone(),
            is_default_version: false,
        };

        doc.versions.push(version);
        doc.latest_version = new_version_num.clone();
        doc.content = content;

        let mut desc_json = json!({
            "Name": doc.name,
            "DocumentType": doc.document_type,
            "DocumentFormat": format,
            "DocumentVersion": new_version_num,
            "LatestVersion": doc.latest_version,
            "DefaultVersion": doc.default_version,
            "Status": "Active",
            "CreatedDate": doc.created_date.timestamp_millis() as f64 / 1000.0,
            "Owner": doc.owner,
            "SchemaVersion": schema_ver.as_deref().unwrap_or("2.2"),
        });
        if let Some(d) = doc_description {
            desc_json["Description"] = json!(d);
        }
        if !doc_params.is_empty() {
            desc_json["Parameters"] = json!(doc_params);
        }
        // Include VersionName from the newly created version
        if let Some(ver) = doc.versions.last() {
            if let Some(ref vn) = ver.version_name {
                desc_json["VersionName"] = json!(vn);
            }
        }

        Ok(json_resp(json!({ "DocumentDescription": desc_json })))
    }

    fn describe_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        // Use the default version's content for the hash and metadata
        let default_ver = doc
            .versions
            .iter()
            .find(|v| v.document_version == doc.default_version);
        let content_for_hash = default_ver
            .map(|v| v.content.as_str())
            .unwrap_or(&doc.content);
        let format_for_hash = default_ver
            .map(|v| v.document_format.as_str())
            .unwrap_or(&doc.document_format);

        let (doc_description, schema_ver, doc_params) =
            extract_document_metadata(content_for_hash, format_for_hash);

        let mut desc_json = json!({
            "Name": doc.name,
            "DocumentType": doc.document_type,
            "DocumentFormat": format_for_hash,
            "DocumentVersion": doc.default_version,
            "LatestVersion": doc.latest_version,
            "DefaultVersion": doc.default_version,
            "Status": doc.status,
            "CreatedDate": doc.created_date.timestamp_millis() as f64 / 1000.0,
            "Owner": doc.owner,
            "SchemaVersion": schema_ver.as_deref().unwrap_or("2.2"),
            "PlatformTypes": ["Linux", "MacOS", "Windows"],
            "Hash": format!("{:x}", Sha256::digest(content_for_hash.as_bytes())),
            "HashType": "Sha256",
        });
        if let Some(tt) = &doc.target_type {
            desc_json["TargetType"] = json!(tt);
        }
        if let Some(d) = doc_description {
            desc_json["Description"] = json!(d);
        }
        if !doc_params.is_empty() {
            desc_json["Parameters"] = json!(doc_params);
        }
        if let Some(ref vn) = doc.version_name {
            desc_json["VersionName"] = json!(vn);
        }
        // Find default version name
        if let Some(ver) = doc
            .versions
            .iter()
            .find(|v| v.document_version == doc.default_version)
        {
            if let Some(ref vn) = ver.version_name {
                desc_json["DefaultVersionName"] = json!(vn);
            }
        }

        Ok(json_resp(json!({ "Document": desc_json })))
    }

    fn update_document_default_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let version = body["DocumentVersion"]
            .as_str()
            .ok_or_else(|| missing("DocumentVersion"))?;

        let mut state = self.state.write();
        let doc = state
            .documents
            .get_mut(name)
            .ok_or_else(|| doc_not_found(name))?;

        // Validate version exists
        if !doc.versions.iter().any(|v| v.document_version == version) {
            return Err(doc_not_found(name));
        }

        doc.default_version = version.to_string();

        // Update is_default_version flags
        for v in &mut doc.versions {
            v.is_default_version = v.document_version == version;
        }

        // Find version name for the new default version
        let version_name = doc
            .versions
            .iter()
            .find(|v| v.document_version == version)
            .and_then(|v| v.version_name.clone());

        let mut desc = json!({
            "Name": name,
            "DefaultVersion": version,
        });
        if let Some(vn) = version_name {
            desc["DefaultVersionName"] = json!(vn);
        }

        Ok(json_resp(json!({ "Description": desc })))
    }

    fn list_documents(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(10) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let all_docs: Vec<Value> = state
            .documents
            .values()
            .filter(|doc| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "Owner" => {
                                // "Self" means owned by current account
                                if values.contains(&"Self") && doc.owner != state.account_id {
                                    return false;
                                }
                            }
                            "TargetType" => {
                                if let Some(tt) = &doc.target_type {
                                    if !values.contains(&tt.as_str()) {
                                        return false;
                                    }
                                } else {
                                    return false;
                                }
                            }
                            "Name" => {
                                if !values.contains(&doc.name.as_str()) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|doc| {
                let mut v = json!({
                    "Name": doc.name,
                    "DocumentType": doc.document_type,
                    "DocumentFormat": doc.document_format,
                    "DocumentVersion": doc.default_version,
                    "Owner": doc.owner,
                    "SchemaVersion": "2.2",
                    "PlatformTypes": ["Linux", "MacOS", "Windows"],
                });
                if let Some(tt) = &doc.target_type {
                    v["TargetType"] = json!(tt);
                }
                v
            })
            .collect();

        let page = if next_token_offset < all_docs.len() {
            &all_docs[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let result: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "DocumentIdentifiers": result });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        } else {
            resp["NextToken"] = json!("");
        }

        Ok(json_resp(resp))
    }

    fn describe_document_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let doc = state.documents.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDocument",
                "The specified document does not exist.".to_string(),
            )
        })?;

        let account_ids = doc.permissions.get("Share").cloned().unwrap_or_default();

        Ok(json_resp(json!({
            "AccountIds": account_ids,
            "AccountSharingInfoList": account_ids.iter().map(|id| json!({
                "AccountId": id,
                "SharedDocumentVersion": "$DEFAULT"
            })).collect::<Vec<_>>(),
        })))
    }

    fn modify_document_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let permission_type = body["PermissionType"].as_str().unwrap_or("Share");
        let accounts_to_add = body["AccountIdsToAdd"].as_array();
        let accounts_to_remove = body["AccountIdsToRemove"].as_array();

        let shared_doc_version = body["SharedDocumentVersion"].as_str();

        if permission_type != "Share" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidPermissionType",
                format!(
                    "1 validation error detected: Value '{permission_type}' at 'permissionType' \
                     failed to satisfy constraint: Member must satisfy enum value set: [Share]"
                ),
            ));
        }

        // Validate SharedDocumentVersion if provided
        if let Some(ver) = shared_doc_version {
            if ver != "$DEFAULT" && ver != "$LATEST" && ver.parse::<i64>().is_err() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{ver}' at 'sharedDocumentVersion' \
                         failed to satisfy constraint: \
                         Member must satisfy regular expression pattern: \
                         ([$]LATEST|[$]DEFAULT|[$]ALL)"
                    ),
                ));
            }
        }

        // Validate account IDs
        fn validate_account_ids(ids: &[Value]) -> Result<Vec<String>, AwsServiceError> {
            let mut result = Vec::new();
            let mut has_all = false;
            for id_val in ids {
                if let Some(id) = id_val.as_str() {
                    if id.eq_ignore_ascii_case("all") {
                        has_all = true;
                        result.push(id.to_string());
                    } else if id.len() != 12 || !id.chars().all(|c| c.is_ascii_digit()) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ValidationException",
                            format!(
                                "1 validation error detected: Value '[{id}]' at \
                                 'accountIdsToAdd' failed to satisfy constraint: \
                                 Member must satisfy regular expression pattern: \
                                 (?i)all|[0-9]{{12}}"
                            ),
                        ));
                    } else {
                        result.push(id.to_string());
                    }
                }
            }
            if has_all && result.len() > 1 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DocumentPermissionLimit",
                    "Accounts can either be all or a group of AWS accounts",
                ));
            }
            Ok(result)
        }

        let mut state = self.state.write();
        let doc = state.documents.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDocument",
                "The specified document does not exist.".to_string(),
            )
        })?;

        if let Some(add_ids) = accounts_to_add {
            let ids = validate_account_ids(add_ids)?;
            let entry = doc.permissions.entry("Share".to_string()).or_default();
            for id in ids {
                if !entry.contains(&id) {
                    entry.push(id);
                }
            }
        }

        if let Some(remove_ids) = accounts_to_remove {
            let ids = validate_account_ids(remove_ids)?;
            if let Some(entry) = doc.permissions.get_mut("Share") {
                entry.retain(|id| !ids.contains(id));
            }
        }

        Ok(json_resp(json!({})))
    }

    // ===== Command operations =====

    fn send_command(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();

        // Validate optional fields
        validate_optional_string_length("DocumentHash", body["DocumentHash"].as_str(), 0, 256)?;
        validate_optional_enum(
            "DocumentHashType",
            body["DocumentHashType"].as_str(),
            &["Sha256", "Sha1"],
        )?;
        validate_optional_range_i64(
            "TimeoutSeconds",
            body["TimeoutSeconds"].as_i64(),
            30,
            2592000,
        )?;
        validate_optional_string_length("Comment", body["Comment"].as_str(), 0, 100)?;
        validate_optional_string_length("OutputS3Region", body["OutputS3Region"].as_str(), 3, 20)?;
        validate_optional_string_length(
            "OutputS3BucketName",
            body["OutputS3BucketName"].as_str(),
            3,
            63,
        )?;
        validate_optional_string_length(
            "OutputS3KeyPrefix",
            body["OutputS3KeyPrefix"].as_str(),
            0,
            500,
        )?;
        validate_optional_string_length("MaxConcurrency", body["MaxConcurrency"].as_str(), 1, 7)?;
        validate_optional_string_length("MaxErrors", body["MaxErrors"].as_str(), 1, 7)?;

        let instance_ids: Vec<String> = body["InstanceIds"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let targets: Vec<Value> = body["Targets"].as_array().cloned().unwrap_or_default();
        let parameters: HashMap<String, Vec<String>> = body["Parameters"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let vals = v
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        (k.clone(), vals)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let comment = body["Comment"].as_str().map(|s| s.to_string());
        let output_s3_bucket = body["OutputS3BucketName"].as_str().map(|s| s.to_string());
        let output_s3_prefix = body["OutputS3KeyPrefix"].as_str().map(|s| s.to_string());
        let output_s3_region = body["OutputS3Region"].as_str().map(|s| s.to_string());
        let timeout = body["TimeoutSeconds"].as_i64();
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());
        let service_role = body["ServiceRoleArn"].as_str().map(|s| s.to_string());
        let notification = body.get("NotificationConfig").cloned();
        let document_hash = body["DocumentHash"].as_str().map(|s| s.to_string());
        let document_hash_type = body["DocumentHashType"].as_str().map(|s| s.to_string());

        let command_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        // Resolve targets to instance IDs (for tag-based targets, just use the instance_ids)
        let effective_instance_ids = if instance_ids.is_empty() && !targets.is_empty() {
            // For tag-based targets, we'll simulate with some dummy instance IDs
            vec!["i-placeholder".to_string()]
        } else {
            instance_ids.clone()
        };

        let cmd = SsmCommand {
            command_id: command_id.clone(),
            document_name: document_name.clone(),
            instance_ids: effective_instance_ids.clone(),
            parameters: parameters.clone(),
            status: "Success".to_string(),
            requested_date_time: now,
            comment: comment.clone(),
            output_s3_bucket_name: output_s3_bucket.clone(),
            output_s3_key_prefix: output_s3_prefix.clone(),
            output_s3_region: output_s3_region.clone(),
            timeout_seconds: timeout,
            service_role_arn: service_role.clone(),
            notification_config: notification.clone(),
            targets: targets.clone(),
            document_hash: document_hash.clone(),
            document_hash_type: document_hash_type.clone(),
        };

        let mut state = self.state.write();
        state.commands.push(cmd);

        let expires = now + chrono::Duration::seconds(timeout.unwrap_or(3600));
        let mut cmd_obj = json!({
            "CommandId": command_id,
            "DocumentName": document_name,
            "InstanceIds": effective_instance_ids,
            "Targets": targets,
            "Parameters": parameters,
            "Status": "Success",
            "StatusDetails": "Details placeholder",
            "RequestedDateTime": now.timestamp_millis() as f64 / 1000.0,
            "ExpiresAfter": expires.timestamp_millis() as f64 / 1000.0,
            "MaxConcurrency": max_concurrency.unwrap_or_default(),
            "MaxErrors": max_errors.unwrap_or_default(),
            "DeliveryTimedOutCount": 0,
        });
        if let Some(ref c) = comment {
            cmd_obj["Comment"] = json!(c);
        }
        if let Some(ref r) = output_s3_region {
            cmd_obj["OutputS3Region"] = json!(r);
        }
        if let Some(ref b) = output_s3_bucket {
            cmd_obj["OutputS3BucketName"] = json!(b);
        }
        if let Some(ref p) = output_s3_prefix {
            cmd_obj["OutputS3KeyPrefix"] = json!(p);
        }
        if let Some(t) = timeout {
            cmd_obj["TimeoutSeconds"] = json!(t);
        }

        Ok(json_resp(json!({ "Command": cmd_obj })))
    }

    fn list_commands(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("CommandId", body["CommandId"].as_str(), 36, 36)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let command_id = body["CommandId"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let state = self.state.read();
        let all_commands: Vec<Value> = state
            .commands
            .iter()
            .filter(|c| {
                if let Some(cid) = command_id {
                    if c.command_id != cid {
                        return false;
                    }
                }
                if let Some(iid) = instance_id {
                    if !c.instance_ids.contains(&iid.to_string()) {
                        return false;
                    }
                }
                true
            })
            .map(|c| {
                let expires = c.requested_date_time
                    + chrono::Duration::seconds(c.timeout_seconds.unwrap_or(3600));
                let v = json!({
                    "CommandId": c.command_id,
                    "DocumentName": c.document_name,
                    "InstanceIds": c.instance_ids,
                    "Targets": c.targets,
                    "Parameters": c.parameters,
                    "Status": c.status,
                    "StatusDetails": "Details placeholder",
                    "RequestedDateTime": c.requested_date_time.timestamp_millis() as f64 / 1000.0,
                    "ExpiresAfter": expires.timestamp_millis() as f64 / 1000.0,
                    "Comment": c.comment,
                    "OutputS3Region": c.output_s3_region,
                    "OutputS3BucketName": c.output_s3_bucket_name,
                    "OutputS3KeyPrefix": c.output_s3_key_prefix,
                    "DeliveryTimedOutCount": 0,
                });
                v
            })
            .collect();

        // If a specific CommandId was requested and not found, return an error
        if let Some(cid) = command_id {
            if all_commands.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidCommandId",
                    format!("Command with id {cid} does not exist."),
                ));
            }
        }

        let page = if next_token_offset < all_commands.len() {
            &all_commands[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let commands: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "Commands": commands });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn get_command_invocation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let command_id = body["CommandId"]
            .as_str()
            .ok_or_else(|| missing("CommandId"))?;
        validate_string_length("CommandId", command_id, 36, 36)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let plugin_name = body["PluginName"].as_str();
        validate_optional_string_length("PluginName", plugin_name, 4, 500)?;

        let state = self.state.read();
        let cmd = state
            .commands
            .iter()
            .find(|c| c.command_id == command_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvocationDoesNotExist",
                    format!("Command {command_id} not found"),
                )
            })?;

        // Check instance is part of the command
        if !cmd.instance_ids.contains(&instance_id.to_string()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvocationDoesNotExist",
                "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
            ));
        }

        // Validate plugin name if provided
        if let Some(pn) = plugin_name {
            let known_plugins = ["aws:runShellScript", "aws:runPowerShellScript"];
            if !known_plugins.contains(&pn) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvocationDoesNotExist",
                    "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation",
                ));
            }
        }

        Ok(json_resp(json!({
            "CommandId": cmd.command_id,
            "InstanceId": instance_id,
            "DocumentName": cmd.document_name,
            "Status": "Success",
            "StatusDetails": "Success",
            "ResponseCode": 0,
            "StandardOutputContent": "",
            "StandardOutputUrl": "",
            "StandardErrorContent": "",
            "StandardErrorUrl": "",
        })))
    }

    fn list_command_invocations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("CommandId", body["CommandId"].as_str(), 36, 36)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let command_id = body["CommandId"].as_str();

        let state = self.state.read();
        let all_invocations: Vec<Value> = state
            .commands
            .iter()
            .filter(|c| {
                if let Some(cid) = command_id {
                    c.command_id == cid
                } else {
                    true
                }
            })
            .flat_map(|c| {
                c.instance_ids.iter().map(|iid| {
                    json!({
                        "CommandId": c.command_id,
                        "InstanceId": iid,
                        "DocumentName": c.document_name,
                        "Status": "Success",
                        "StatusDetails": "Success",
                        "RequestedDateTime": c.requested_date_time.timestamp_millis() as f64 / 1000.0,
                        "Comment": c.comment,
                    })
                })
            })
            .collect();

        let page = if next_token_offset < all_invocations.len() {
            &all_invocations[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let invocations: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "CommandInvocations": invocations });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn cancel_command(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let command_id = body["CommandId"]
            .as_str()
            .ok_or_else(|| missing("CommandId"))?;
        validate_string_length("CommandId", command_id, 36, 36)?;

        let mut state = self.state.write();
        if let Some(cmd) = state
            .commands
            .iter_mut()
            .find(|c| c.command_id == command_id)
        {
            cmd.status = "Cancelled".to_string();
        }

        Ok(json_resp(json!({})))
    }

    // ===== Maintenance Window operations =====

    fn create_maintenance_window(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("Name", &name, 3, 128)?;
        let schedule = body["Schedule"]
            .as_str()
            .ok_or_else(|| missing("Schedule"))?
            .to_string();
        validate_string_length("Schedule", &schedule, 1, 256)?;
        let duration = body["Duration"]
            .as_i64()
            .ok_or_else(|| missing("Duration"))?;
        validate_range_i64("Duration", duration, 1, 24)?;
        let cutoff = body["Cutoff"].as_i64().ok_or_else(|| missing("Cutoff"))?;
        validate_range_i64("Cutoff", cutoff, 0, 23)?;
        validate_required(
            "AllowUnassociatedTargets",
            &body["AllowUnassociatedTargets"],
        )?;
        let allow_unassociated_targets =
            body["AllowUnassociatedTargets"].as_bool().unwrap_or(false);
        validate_optional_string_length("Description", body["Description"].as_str(), 1, 128)?;
        validate_optional_string_length("ClientToken", body["ClientToken"].as_str(), 1, 64)?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let schedule_timezone = body["ScheduleTimezone"].as_str().map(|s| s.to_string());
        let schedule_offset = body["ScheduleOffset"].as_i64();
        validate_optional_range_i64("ScheduleOffset", schedule_offset, 1, 6)?;
        let start_date = body["StartDate"].as_str().map(|s| s.to_string());
        let end_date = body["EndDate"].as_str().map(|s| s.to_string());

        let client_token = body["ClientToken"].as_str().map(|s| s.to_string());
        let tags: HashMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        // Idempotency: if a window with the same ClientToken already exists, return it
        if let Some(ref token) = client_token {
            if let Some(existing) = state
                .maintenance_windows
                .values()
                .find(|mw| mw.client_token.as_deref() == Some(token))
            {
                return Ok(json_resp(json!({ "WindowId": existing.id })));
            }
        }

        let window_id = format!("mw-{}", &uuid::Uuid::new_v4().to_string()[..17]);

        let mw = MaintenanceWindow {
            id: window_id.clone(),
            name,
            schedule,
            duration,
            cutoff,
            allow_unassociated_targets,
            enabled: true,
            description,
            tags,
            targets: Vec::new(),
            tasks: Vec::new(),
            schedule_timezone,
            schedule_offset,
            start_date,
            end_date,
            client_token,
        };

        state.maintenance_windows.insert(window_id.clone(), mw);

        Ok(json_resp(json!({ "WindowId": window_id })))
    }

    fn describe_maintenance_windows(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let all_windows: Vec<Value> = state
            .maintenance_windows
            .values()
            .filter(|mw| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "Name" => {
                                if !values.iter().any(|v| *v == mw.name) {
                                    return false;
                                }
                            }
                            "Enabled" => {
                                let enabled_str = if mw.enabled { "true" } else { "false" };
                                if !values.contains(&enabled_str) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|mw| {
                let mut v = json!({
                    "WindowId": mw.id,
                    "Name": mw.name,
                    "Schedule": mw.schedule,
                    "Duration": mw.duration,
                    "Cutoff": mw.cutoff,
                    "Enabled": mw.enabled,
                });
                if let Some(ref desc) = mw.description {
                    v["Description"] = json!(desc);
                }
                if let Some(ref tz) = mw.schedule_timezone {
                    v["ScheduleTimezone"] = json!(tz);
                }
                if let Some(offset) = mw.schedule_offset {
                    v["ScheduleOffset"] = json!(offset);
                }
                if let Some(ref sd) = mw.start_date {
                    v["StartDate"] = json!(sd);
                }
                if let Some(ref ed) = mw.end_date {
                    v["EndDate"] = json!(ed);
                }
                v
            })
            .collect();

        let page = if next_token_offset < all_windows.len() {
            &all_windows[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let windows: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "WindowIdentities": windows });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn get_maintenance_window(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let mut resp = json!({
            "WindowId": mw.id,
            "Name": mw.name,
            "Schedule": mw.schedule,
            "Duration": mw.duration,
            "Cutoff": mw.cutoff,
            "AllowUnassociatedTargets": mw.allow_unassociated_targets,
            "Enabled": mw.enabled,
        });
        if let Some(ref desc) = mw.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref tz) = mw.schedule_timezone {
            resp["ScheduleTimezone"] = json!(tz);
        }
        if let Some(offset) = mw.schedule_offset {
            resp["ScheduleOffset"] = json!(offset);
        }
        if let Some(ref sd) = mw.start_date {
            resp["StartDate"] = json!(sd);
        }
        if let Some(ref ed) = mw.end_date {
            resp["EndDate"] = json!(ed);
        }

        Ok(json_resp(resp))
    }

    fn delete_maintenance_window(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        validate_string_length("WindowId", window_id, 20, 20)?;

        let mut state = self.state.write();
        if state.maintenance_windows.remove(window_id).is_none() {
            return Err(mw_not_found(window_id));
        }

        Ok(json_resp(json!({ "WindowId": window_id })))
    }

    fn update_maintenance_window(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        validate_string_length("WindowId", window_id, 20, 20)?;
        validate_optional_string_length("Name", body["Name"].as_str(), 3, 128)?;
        validate_optional_string_length("Description", body["Description"].as_str(), 1, 128)?;
        validate_optional_string_length("Schedule", body["Schedule"].as_str(), 1, 256)?;
        validate_optional_range_i64("ScheduleOffset", body["ScheduleOffset"].as_i64(), 1, 6)?;
        validate_optional_range_i64("Duration", body["Duration"].as_i64(), 1, 24)?;
        validate_optional_range_i64("Cutoff", body["Cutoff"].as_i64(), 0, 23)?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        if let Some(name) = body["Name"].as_str() {
            mw.name = name.to_string();
        }
        if let Some(schedule) = body["Schedule"].as_str() {
            mw.schedule = schedule.to_string();
        }
        if let Some(duration) = body["Duration"].as_i64() {
            mw.duration = duration;
        }
        if let Some(cutoff) = body["Cutoff"].as_i64() {
            mw.cutoff = cutoff;
        }
        if let Some(enabled) = body["Enabled"].as_bool() {
            mw.enabled = enabled;
        }
        if let Some(allow) = body["AllowUnassociatedTargets"].as_bool() {
            mw.allow_unassociated_targets = allow;
        }
        if body.get("Description").is_some() {
            mw.description = body["Description"].as_str().map(|s| s.to_string());
        }

        let mut resp = json!({
            "WindowId": mw.id,
            "Name": mw.name,
            "Schedule": mw.schedule,
            "Duration": mw.duration,
            "Cutoff": mw.cutoff,
            "AllowUnassociatedTargets": mw.allow_unassociated_targets,
            "Enabled": mw.enabled,
        });
        if let Some(ref desc) = mw.description {
            resp["Description"] = json!(desc);
        }

        Ok(json_resp(resp))
    }

    fn register_target_with_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?
            .to_string();
        let targets = body["Targets"]
            .as_array()
            .cloned()
            .ok_or_else(|| missing("Targets"))?;
        let name = body["Name"].as_str().map(|s| s.to_string());
        let description = body["Description"].as_str().map(|s| s.to_string());
        let owner_information = body["OwnerInformation"].as_str().map(|s| s.to_string());

        let target_id = format!(
            "{}-{}",
            window_id,
            &uuid::Uuid::new_v4().to_string().replace('-', "")
        );

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let target = MaintenanceWindowTarget {
            window_target_id: target_id.clone(),
            window_id: window_id.to_string(),
            resource_type,
            targets,
            name,
            description,
            owner_information,
        };
        mw.targets.push(target);

        Ok(json_resp(json!({ "WindowTargetId": target_id })))
    }

    fn deregister_target_from_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let target_id = body["WindowTargetId"]
            .as_str()
            .ok_or_else(|| missing("WindowTargetId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        mw.targets.retain(|t| t.window_target_id != target_id);

        Ok(json_resp(json!({
            "WindowId": window_id,
            "WindowTargetId": target_id,
        })))
    }

    fn describe_maintenance_window_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let targets: Vec<Value> = mw
            .targets
            .iter()
            .map(|t| {
                let mut v = json!({
                    "WindowId": t.window_id,
                    "WindowTargetId": t.window_target_id,
                    "ResourceType": t.resource_type,
                    "Targets": t.targets,
                });
                if let Some(ref name) = t.name {
                    v["Name"] = json!(name);
                }
                if let Some(ref desc) = t.description {
                    v["Description"] = json!(desc);
                }
                if let Some(ref oi) = t.owner_information {
                    v["OwnerInformation"] = json!(oi);
                }
                v
            })
            .collect();

        Ok(json_resp(json!({ "Targets": targets })))
    }

    fn register_task_with_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_arn = body["TaskArn"]
            .as_str()
            .ok_or_else(|| missing("TaskArn"))?
            .to_string();
        let task_type = body["TaskType"]
            .as_str()
            .ok_or_else(|| missing("TaskType"))?
            .to_string();
        let targets = body["Targets"].as_array().cloned().unwrap_or_default();
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());
        let priority = body["Priority"].as_i64().unwrap_or(1);
        let service_role_arn = body["ServiceRoleArn"].as_str().map(|s| s.to_string());
        let name = body["Name"].as_str().map(|s| s.to_string());
        let description = body["Description"].as_str().map(|s| s.to_string());

        let task_id = format!(
            "{}-{}",
            window_id,
            &uuid::Uuid::new_v4().to_string().replace('-', "")
        );

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let task = MaintenanceWindowTask {
            window_task_id: task_id.clone(),
            window_id: window_id.to_string(),
            task_arn,
            task_type,
            targets,
            max_concurrency,
            max_errors,
            priority,
            service_role_arn,
            name,
            description,
        };
        mw.tasks.push(task);

        Ok(json_resp(json!({ "WindowTaskId": task_id })))
    }

    fn deregister_task_from_maintenance_window(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        mw.tasks.retain(|t| t.window_task_id != task_id);

        Ok(json_resp(json!({
            "WindowId": window_id,
            "WindowTaskId": task_id,
        })))
    }

    fn describe_maintenance_window_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;

        let state = self.state.read();
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let tasks: Vec<Value> = mw
            .tasks
            .iter()
            .map(|t| {
                let mut v = json!({
                    "WindowId": t.window_id,
                    "WindowTaskId": t.window_task_id,
                    "TaskArn": t.task_arn,
                    "Type": t.task_type,
                    "Targets": t.targets,
                    "Priority": t.priority,
                });
                if let Some(ref mc) = t.max_concurrency {
                    v["MaxConcurrency"] = json!(mc);
                }
                if let Some(ref me) = t.max_errors {
                    v["MaxErrors"] = json!(me);
                }
                if let Some(ref sr) = t.service_role_arn {
                    v["ServiceRoleArn"] = json!(sr);
                }
                if let Some(ref name) = t.name {
                    v["Name"] = json!(name);
                }
                if let Some(ref desc) = t.description {
                    v["Description"] = json!(desc);
                }
                v
            })
            .collect();

        Ok(json_resp(json!({ "Tasks": tasks })))
    }

    // ===== Patch Baseline operations =====

    fn create_patch_baseline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("Name", &name, 3, 128)?;
        validate_optional_enum(
            "OperatingSystem",
            body["OperatingSystem"].as_str(),
            &[
                "WINDOWS",
                "AMAZON_LINUX",
                "AMAZON_LINUX_2",
                "AMAZON_LINUX_2022",
                "UBUNTU",
                "REDHAT_ENTERPRISE_LINUX",
                "SUSE",
                "CENTOS",
                "ORACLE_LINUX",
                "DEBIAN",
                "MACOS",
                "RASPBIAN",
                "ROCKY_LINUX",
                "ALMA_LINUX",
                "AMAZON_LINUX_2023",
            ],
        )?;
        let operating_system = body["OperatingSystem"]
            .as_str()
            .unwrap_or("WINDOWS")
            .to_string();
        validate_optional_string_length("Description", body["Description"].as_str(), 1, 1024)?;
        validate_optional_enum(
            "ApprovedPatchesComplianceLevel",
            body["ApprovedPatchesComplianceLevel"].as_str(),
            &[
                "CRITICAL",
                "HIGH",
                "MEDIUM",
                "LOW",
                "INFORMATIONAL",
                "UNSPECIFIED",
            ],
        )?;
        validate_optional_enum(
            "RejectedPatchesAction",
            body["RejectedPatchesAction"].as_str(),
            &["ALLOW_AS_DEPENDENCY", "BLOCK"],
        )?;
        validate_optional_enum(
            "AvailableSecurityUpdatesComplianceStatus",
            body["AvailableSecurityUpdatesComplianceStatus"].as_str(),
            &["COMPLIANT", "NON_COMPLIANT"],
        )?;
        validate_optional_string_length("ClientToken", body["ClientToken"].as_str(), 1, 64)?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let approval_rules = body.get("ApprovalRules").cloned();
        let approved_patches: Vec<String> = body["ApprovedPatches"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let rejected_patches: Vec<String> = body["RejectedPatches"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let approved_patches_compliance_level = body["ApprovedPatchesComplianceLevel"]
            .as_str()
            .unwrap_or("UNSPECIFIED")
            .to_string();
        let rejected_patches_action = body["RejectedPatchesAction"]
            .as_str()
            .unwrap_or("ALLOW_AS_DEPENDENCY")
            .to_string();
        let global_filters = body.get("GlobalFilters").cloned();
        let sources: Vec<Value> = body["Sources"].as_array().cloned().unwrap_or_default();
        let approved_patches_enable_non_security = body["ApprovedPatchesEnableNonSecurity"]
            .as_bool()
            .unwrap_or(false);
        let tags: HashMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let available_security_updates_compliance_status = body
            ["AvailableSecurityUpdatesComplianceStatus"]
            .as_str()
            .map(|s| s.to_string());
        let client_token = body["ClientToken"].as_str().map(|s| s.to_string());

        let mut state = self.state.write();

        // Idempotency: if a baseline with the same ClientToken already exists, return it
        if let Some(ref token) = client_token {
            if let Some(existing) = state
                .patch_baselines
                .values()
                .find(|pb| pb.client_token.as_deref() == Some(token))
            {
                return Ok(json_resp(json!({ "BaselineId": existing.id })));
            }
        }

        let baseline_id = format!(
            "pb-{}",
            &uuid::Uuid::new_v4().to_string().replace('-', "")[..17]
        );

        let pb = PatchBaseline {
            id: baseline_id.clone(),
            name,
            operating_system,
            description,
            approval_rules,
            approved_patches,
            rejected_patches,
            tags,
            approved_patches_compliance_level,
            rejected_patches_action,
            global_filters,
            sources,
            approved_patches_enable_non_security,
            available_security_updates_compliance_status,
            client_token,
        };

        state.patch_baselines.insert(baseline_id.clone(), pb);

        Ok(json_resp(json!({ "BaselineId": baseline_id })))
    }

    fn delete_patch_baseline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        validate_string_length("BaselineId", baseline_id, 20, 128)?;

        let mut state = self.state.write();
        state.patch_baselines.remove(baseline_id);
        // Also remove any patch group associations
        state
            .patch_groups
            .retain(|pg| pg.baseline_id != baseline_id);

        Ok(json_resp(json!({ "BaselineId": baseline_id })))
    }

    fn describe_patch_baselines(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 100)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let all_baselines: Vec<Value> = state
            .patch_baselines
            .values()
            .filter(|pb| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "NAME_PREFIX" => {
                                if !values.iter().any(|v| pb.name.starts_with(v)) {
                                    return false;
                                }
                            }
                            "OWNER" => {
                                // We don't track owner, but "Self" means user-created
                                if values.contains(&"AWS") {
                                    return false;
                                }
                            }
                            "OPERATING_SYSTEM" => {
                                if !values.contains(&pb.operating_system.as_str()) {
                                    return false;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|pb| {
                let mut v = json!({
                    "BaselineId": pb.id,
                    "BaselineName": pb.name,
                    "OperatingSystem": pb.operating_system,
                    "DefaultBaseline": false,
                });
                if let Some(ref desc) = pb.description {
                    v["BaselineDescription"] = json!(desc);
                }
                v
            })
            .collect();

        let page = if next_token_offset < all_baselines.len() {
            &all_baselines[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let baselines: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "BaselineIdentities": baselines });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    fn get_patch_baseline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        validate_string_length("BaselineId", baseline_id, 20, 128)?;

        let state = self.state.read();
        let pb = state.patch_baselines.get(baseline_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Baseline {baseline_id} does not exist."),
            )
        })?;

        let mut resp = json!({
            "BaselineId": pb.id,
            "Name": pb.name,
            "OperatingSystem": pb.operating_system,
            "ApprovedPatches": pb.approved_patches,
            "RejectedPatches": pb.rejected_patches,
            "ApprovedPatchesComplianceLevel": pb.approved_patches_compliance_level,
            "RejectedPatchesAction": pb.rejected_patches_action,
            "ApprovedPatchesEnableNonSecurity": pb.approved_patches_enable_non_security,
            "Sources": pb.sources,
            "PatchGroups": state.patch_groups.iter()
                .filter(|pg| pg.baseline_id == baseline_id)
                .map(|pg| pg.patch_group.clone())
                .collect::<Vec<_>>(),
        });
        if let Some(ref desc) = pb.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref rules) = pb.approval_rules {
            resp["ApprovalRules"] = rules.clone();
        }
        if let Some(ref gf) = pb.global_filters {
            resp["GlobalFilters"] = gf.clone();
        }
        if let Some(ref status) = pb.available_security_updates_compliance_status {
            resp["AvailableSecurityUpdatesComplianceStatus"] = json!(status);
        }

        Ok(json_resp(resp))
    }

    fn register_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?
            .to_string();
        validate_string_length("BaselineId", &baseline_id, 20, 128)?;
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?
            .to_string();
        validate_string_length("PatchGroup", &patch_group, 1, 256)?;

        let mut state = self.state.write();

        // Check baseline exists (AWS returns "Maintenance window" in this error, not "Patch baseline")
        if !state.patch_baselines.contains_key(&baseline_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Maintenance window {baseline_id} does not exist"),
            ));
        }

        // Check if this patch group is already registered to a baseline with same OS
        let os = state.patch_baselines[&baseline_id].operating_system.clone();
        if let Some(existing) = state
            .patch_groups
            .iter()
            .find(|pg| pg.patch_group == patch_group)
        {
            if let Some(existing_pb) = state.patch_baselines.get(&existing.baseline_id) {
                if existing_pb.operating_system == os {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "AlreadyExistsException",
                        format!(
                            "Patch Group baseline already has a baseline registered for OperatingSystem {os}."
                        ),
                    ));
                }
            }
        }

        state.patch_groups.push(PatchGroup {
            baseline_id: baseline_id.clone(),
            patch_group: patch_group.clone(),
        });

        Ok(json_resp(json!({
            "BaselineId": baseline_id,
            "PatchGroup": patch_group,
        })))
    }

    fn deregister_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        validate_string_length("BaselineId", baseline_id, 20, 128)?;
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        validate_string_length("PatchGroup", patch_group, 1, 256)?;

        let mut state = self.state.write();

        // Check if the association exists
        let exists = state
            .patch_groups
            .iter()
            .any(|pg| pg.baseline_id == baseline_id && pg.patch_group == patch_group);
        if exists {
            state
                .patch_groups
                .retain(|pg| !(pg.baseline_id == baseline_id && pg.patch_group == patch_group));
        } else {
            // Allow deregistering default baselines (they are implicitly registered)
            let is_default = is_default_patch_baseline(baseline_id);
            if !is_default {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    "Patch Baseline to be retrieved does not exist.",
                ));
            }
        }

        Ok(json_resp(json!({
            "BaselineId": baseline_id,
            "PatchGroup": patch_group,
        })))
    }

    fn get_patch_baseline_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        validate_string_length("PatchGroup", patch_group, 1, 256)?;
        validate_optional_enum(
            "OperatingSystem",
            body["OperatingSystem"].as_str(),
            &[
                "WINDOWS",
                "AMAZON_LINUX",
                "AMAZON_LINUX_2",
                "AMAZON_LINUX_2022",
                "AMAZON_LINUX_2023",
                "UBUNTU",
                "REDHAT_ENTERPRISE_LINUX",
                "SUSE",
                "CENTOS",
                "ORACLE_LINUX",
                "DEBIAN",
                "MACOS",
                "RASPBIAN",
                "ROCKY_LINUX",
                "ALMA_LINUX",
            ],
        )?;
        let operating_system = body["OperatingSystem"].as_str().unwrap_or("WINDOWS");

        let state = self.state.read();

        // Find a patch group association matching both patch group and OS
        let found = state.patch_groups.iter().find(|pg| {
            pg.patch_group == patch_group
                && state
                    .patch_baselines
                    .get(&pg.baseline_id)
                    .is_some_and(|pb| pb.operating_system == operating_system)
        });

        if let Some(pg) = found {
            Ok(json_resp(json!({
                "BaselineId": pg.baseline_id,
                "PatchGroup": pg.patch_group,
                "OperatingSystem": operating_system,
            })))
        } else {
            // Fall back to default baseline for the region/OS
            let mut resp = json!({
                "PatchGroup": patch_group,
                "OperatingSystem": operating_system,
            });
            if let Some(baseline_id) = default_patch_baseline(&req.region, operating_system) {
                resp["BaselineId"] = json!(baseline_id);
            }
            Ok(json_resp(resp))
        }
    }

    fn describe_patch_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 100)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let filters = body["Filters"].as_array();

        let state = self.state.read();
        let all_mappings: Vec<Value> = state
            .patch_groups
            .iter()
            .filter(|pg| {
                if let Some(filters) = filters {
                    for filter in filters {
                        let key = filter["Key"].as_str().unwrap_or("");
                        let values: Vec<&str> = filter["Values"]
                            .as_array()
                            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
                            .unwrap_or_default();
                        match key {
                            "NAME_PREFIX" => {
                                if !values.iter().any(|v| pg.patch_group.starts_with(v)) {
                                    return false;
                                }
                            }
                            "OPERATING_SYSTEM" => {
                                if let Some(pb) = state.patch_baselines.get(&pg.baseline_id) {
                                    if !values.contains(&pb.operating_system.as_str()) {
                                        return false;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                true
            })
            .map(|pg| {
                let mut baseline_identity = json!({
                    "BaselineId": pg.baseline_id,
                    "DefaultBaseline": false,
                });
                if let Some(pb) = state.patch_baselines.get(&pg.baseline_id) {
                    baseline_identity["BaselineName"] = json!(pb.name);
                    baseline_identity["OperatingSystem"] = json!(pb.operating_system);
                    if let Some(ref desc) = pb.description {
                        baseline_identity["BaselineDescription"] = json!(desc);
                    }
                }
                json!({
                    "PatchGroup": pg.patch_group,
                    "BaselineIdentity": baseline_identity,
                })
            })
            .collect();

        let page = if next_token_offset < all_mappings.len() {
            &all_mappings[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let mappings: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "Mappings": mappings });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }

        Ok(json_resp(resp))
    }

    // -----------------------------------------------------------------------
    // Associations
    // -----------------------------------------------------------------------

    fn create_association_inner(&self, body: &Value) -> Result<Value, AwsServiceError> {
        validate_optional_string_length(
            "AssociationDispatchAssumeRole",
            body["AssociationDispatchAssumeRole"].as_str(),
            1,
            512,
        )?;
        validate_optional_string_length(
            "AutomationTargetParameterName",
            body["AutomationTargetParameterName"].as_str(),
            1,
            50,
        )?;
        validate_optional_string_length(
            "ScheduleExpression",
            body["ScheduleExpression"].as_str(),
            1,
            256,
        )?;
        validate_optional_string_length("MaxConcurrency", body["MaxConcurrency"].as_str(), 1, 7)?;
        validate_optional_string_length("MaxErrors", body["MaxErrors"].as_str(), 1, 7)?;
        validate_optional_enum(
            "ComplianceSeverity",
            body["ComplianceSeverity"].as_str(),
            &["Critical", "High", "Medium", "Low", "Unspecified"],
        )?;
        validate_optional_enum(
            "SyncCompliance",
            body["SyncCompliance"].as_str(),
            &["Auto", "Manual"],
        )?;
        validate_optional_range_i64("Duration", body["Duration"].as_i64(), 1, 24)?;
        validate_optional_range_i64("ScheduleOffset", body["ScheduleOffset"].as_i64(), 1, 6)?;

        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let targets: Vec<serde_json::Value> =
            body["Targets"].as_array().cloned().unwrap_or_default();
        let instance_id = body["InstanceId"].as_str().map(|s| s.to_string());

        // Must have either Targets or InstanceId
        if targets.is_empty() && instance_id.is_none() {
            // Accept it anyway like AWS does for document-only associations
        }

        let schedule_expression = body["ScheduleExpression"].as_str().map(|s| s.to_string());
        let parameters: HashMap<String, Vec<String>> = body["Parameters"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let vals = v
                            .as_array()
                            .map(|a| {
                                a.iter()
                                    .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        (k.clone(), vals)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let association_name = body["AssociationName"].as_str().map(|s| s.to_string());
        let document_version = body["DocumentVersion"].as_str().map(|s| s.to_string());
        let output_location = body.get("OutputLocation").filter(|v| !v.is_null()).cloned();
        let automation_target_parameter_name = body["AutomationTargetParameterName"]
            .as_str()
            .map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let compliance_severity = body["ComplianceSeverity"].as_str().map(|s| s.to_string());
        let sync_compliance = body["SyncCompliance"].as_str().map(|s| s.to_string());
        let apply_only_at_cron_interval =
            body["ApplyOnlyAtCronInterval"].as_bool().unwrap_or(false);
        let calendar_names: Vec<String> = body["CalendarNames"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let target_locations: Vec<serde_json::Value> = body["TargetLocations"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let schedule_offset = body["ScheduleOffset"].as_i64();
        let target_maps: Vec<serde_json::Value> =
            body["TargetMaps"].as_array().cloned().unwrap_or_default();
        let tags: HashMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let now = Utc::now();
        let association_id = uuid::Uuid::new_v4().to_string();

        let version = SsmAssociationVersion {
            version: 1,
            name: name.clone(),
            targets: targets.clone(),
            schedule_expression: schedule_expression.clone(),
            parameters: parameters.clone(),
            document_version: document_version.clone(),
            created_date: now,
            association_name: association_name.clone(),
            max_errors: max_errors.clone(),
            max_concurrency: max_concurrency.clone(),
            compliance_severity: compliance_severity.clone(),
        };

        let assoc = SsmAssociation {
            association_id: association_id.clone(),
            name: name.clone(),
            targets: targets.clone(),
            schedule_expression,
            parameters,
            association_name: association_name.clone(),
            document_version,
            output_location,
            automation_target_parameter_name,
            max_errors,
            max_concurrency,
            compliance_severity,
            sync_compliance,
            apply_only_at_cron_interval,
            calendar_names,
            target_locations,
            schedule_offset,
            target_maps,
            tags,
            status: "Pending".to_string(),
            status_date: now,
            overview: json!({"Status": "Pending", "DetailedStatus": "Creating", "AssociationStatusAggregatedCount": {}}),
            created_date: now,
            last_update_association_date: now,
            last_execution_date: None,
            instance_id,
            versions: vec![version],
        };

        let resp = association_to_json(&assoc);

        let mut state = self.state.write();
        state.associations.insert(association_id, assoc);

        Ok(resp)
    }

    fn create_association(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resp = self.create_association_inner(&body)?;
        Ok(json_resp(json!({ "AssociationDescription": resp })))
    }

    fn describe_association(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let association_id = body["AssociationId"].as_str();
        let name = body["Name"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let state = self.state.read();

        let assoc = if let Some(id) = association_id {
            state.associations.get(id)
        } else if let Some(n) = name {
            state.associations.values().find(|a| {
                a.name == n && (instance_id.is_none() || a.instance_id.as_deref() == instance_id)
            })
        } else {
            return Err(missing("AssociationId"));
        };

        match assoc {
            Some(a) => Ok(json_resp(
                json!({ "AssociationDescription": association_to_json(a) }),
            )),
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AssociationDoesNotExist",
                "The specified association does not exist.".to_string(),
            )),
        }
    }

    fn delete_association(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let association_id = body["AssociationId"].as_str();
        let name = body["Name"].as_str();
        let instance_id = body["InstanceId"].as_str();

        let mut state = self.state.write();

        let key = if let Some(id) = association_id {
            if state.associations.contains_key(id) {
                Some(id.to_string())
            } else {
                None
            }
        } else if let Some(n) = name {
            state
                .associations
                .iter()
                .find(|(_, a)| {
                    a.name == n
                        && (instance_id.is_none() || a.instance_id.as_deref() == instance_id)
                })
                .map(|(k, _)| k.clone())
        } else {
            return Err(missing("AssociationId"));
        };

        match key {
            Some(k) => {
                state.associations.remove(&k);
                Ok(json_resp(json!({})))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AssociationDoesNotExist",
                "The specified association does not exist.".to_string(),
            )),
        }
    }

    fn list_associations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
        let all: Vec<Value> = state
            .associations
            .values()
            .map(|a| {
                let mut v = json!({
                    "AssociationId": a.association_id,
                    "Name": a.name,
                });
                if let Some(d) = a.last_execution_date {
                    v["LastExecutionDate"] = json!(d.timestamp_millis() as f64 / 1000.0);
                }
                if let Some(ref an) = a.association_name {
                    v["AssociationName"] = json!(an);
                }
                if let Some(ref s) = a.schedule_expression {
                    v["ScheduleExpression"] = json!(s);
                }
                if !a.targets.is_empty() {
                    v["Targets"] = json!(a.targets);
                }
                if let Some(ref iid) = a.instance_id {
                    v["InstanceId"] = json!(iid);
                }
                v["Overview"] = a.overview.clone();
                v
            })
            .collect();

        let page = if next_token_offset < all.len() {
            &all[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "Associations": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    fn update_association(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;

        let mut state = self.state.write();
        let assoc = state.associations.get_mut(association_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AssociationDoesNotExist",
                "The specified association does not exist.".to_string(),
            )
        })?;

        let now = Utc::now();

        if let Some(n) = body["Name"].as_str() {
            assoc.name = n.to_string();
        }
        if let Some(targets) = body["Targets"].as_array() {
            assoc.targets = targets.clone();
        }
        if let Some(s) = body["ScheduleExpression"].as_str() {
            assoc.schedule_expression = Some(s.to_string());
        }
        if let Some(obj) = body["Parameters"].as_object() {
            assoc.parameters = obj
                .iter()
                .map(|(k, v)| {
                    let vals = v
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    (k.clone(), vals)
                })
                .collect();
        }
        if let Some(an) = body["AssociationName"].as_str() {
            assoc.association_name = Some(an.to_string());
        }
        if let Some(dv) = body["DocumentVersion"].as_str() {
            assoc.document_version = Some(dv.to_string());
        }
        if let Some(me) = body["MaxErrors"].as_str() {
            assoc.max_errors = Some(me.to_string());
        }
        if let Some(mc) = body["MaxConcurrency"].as_str() {
            assoc.max_concurrency = Some(mc.to_string());
        }
        if let Some(cs) = body["ComplianceSeverity"].as_str() {
            assoc.compliance_severity = Some(cs.to_string());
        }

        assoc.last_update_association_date = now;

        let next_version = assoc.versions.len() as i64 + 1;
        assoc.versions.push(SsmAssociationVersion {
            version: next_version,
            name: assoc.name.clone(),
            targets: assoc.targets.clone(),
            schedule_expression: assoc.schedule_expression.clone(),
            parameters: assoc.parameters.clone(),
            document_version: assoc.document_version.clone(),
            created_date: now,
            association_name: assoc.association_name.clone(),
            max_errors: assoc.max_errors.clone(),
            max_concurrency: assoc.max_concurrency.clone(),
            compliance_severity: assoc.compliance_severity.clone(),
        });

        let resp = association_to_json(assoc);
        Ok(json_resp(json!({ "AssociationDescription": resp })))
    }

    fn list_association_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
        let assoc = state.associations.get(association_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AssociationDoesNotExist",
                "The specified association does not exist.".to_string(),
            )
        })?;

        let all: Vec<Value> = assoc
            .versions
            .iter()
            .map(|v| {
                let mut j = json!({
                    "AssociationId": association_id,
                    "AssociationVersion": v.version.to_string(),
                    "Name": v.name,
                    "CreatedDate": v.created_date.timestamp_millis() as f64 / 1000.0,
                });
                if !v.targets.is_empty() {
                    j["Targets"] = json!(v.targets);
                }
                if let Some(ref s) = v.schedule_expression {
                    j["ScheduleExpression"] = json!(s);
                }
                if let Some(ref an) = v.association_name {
                    j["AssociationName"] = json!(an);
                }
                if let Some(ref dv) = v.document_version {
                    j["DocumentVersion"] = json!(dv);
                }
                if let Some(ref me) = v.max_errors {
                    j["MaxErrors"] = json!(me);
                }
                if let Some(ref mc) = v.max_concurrency {
                    j["MaxConcurrency"] = json!(mc);
                }
                if let Some(ref cs) = v.compliance_severity {
                    j["ComplianceSeverity"] = json!(cs);
                }
                j
            })
            .collect();

        let page = if next_token_offset < all.len() {
            &all[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "AssociationVersions": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    fn update_association_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        validate_required("AssociationStatus", &body["AssociationStatus"])?;
        let association_status = &body["AssociationStatus"];
        let new_status = association_status["Name"]
            .as_str()
            .unwrap_or("Pending")
            .to_string();

        let mut state = self.state.write();
        let assoc = state
            .associations
            .values_mut()
            .find(|a| a.name == name && a.instance_id.as_deref() == Some(instance_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AssociationDoesNotExist",
                    "The specified association does not exist.".to_string(),
                )
            })?;

        assoc.status = new_status;
        assoc.status_date = Utc::now();

        let resp = association_to_json(assoc);
        Ok(json_resp(json!({ "AssociationDescription": resp })))
    }

    fn start_associations_once(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let _association_ids = body["AssociationIds"]
            .as_array()
            .ok_or_else(|| missing("AssociationIds"))?;
        // No-op: return success
        Ok(json_resp(json!({})))
    }

    fn create_association_batch(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length(
            "AssociationDispatchAssumeRole",
            body["AssociationDispatchAssumeRole"].as_str(),
            1,
            512,
        )?;
        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing("Entries"))?;

        let mut successful = Vec::new();
        let mut failed = Vec::new();

        for entry in entries {
            match self.create_association_inner(entry) {
                Ok(desc) => successful.push(desc),
                Err(e) => {
                    let entry_name = entry["Name"].as_str().unwrap_or("");
                    failed.push(json!({
                        "Entry": entry,
                        "Message": e.to_string(),
                        "Fault": "Client",
                    }));
                    let _ = entry_name; // suppress unused
                }
            }
        }

        Ok(json_resp(json!({
            "Successful": successful,
            "Failed": failed,
        })))
    }

    fn describe_association_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let _association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        // Return empty list — associations don't actually run
        Ok(json_resp(json!({ "AssociationExecutions": [] })))
    }

    fn describe_association_execution_targets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let _association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;
        let _execution_id = body["ExecutionId"]
            .as_str()
            .ok_or_else(|| missing("ExecutionId"))?;
        Ok(json_resp(json!({ "AssociationExecutionTargets": [] })))
    }

    // -----------------------------------------------------------------------
    // OpsItems
    // -----------------------------------------------------------------------

    fn create_ops_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_required("Description", &body["Description"])?;
        validate_optional_string_length("Title", body["Title"].as_str(), 1, 1024)?;
        validate_optional_string_length("Source", body["Source"].as_str(), 1, 128)?;
        validate_optional_string_length("Description", body["Description"].as_str(), 1, 2048)?;
        validate_optional_string_length("Category", body["Category"].as_str(), 1, 64)?;
        validate_optional_string_length("Severity", body["Severity"].as_str(), 1, 64)?;
        validate_optional_range_i64("Priority", body["Priority"].as_i64(), 1, 5)?;
        let title = body["Title"]
            .as_str()
            .ok_or_else(|| missing("Title"))?
            .to_string();
        let source = body["Source"]
            .as_str()
            .ok_or_else(|| missing("Source"))?
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let priority = body["Priority"].as_i64();
        let severity = body["Severity"].as_str().map(|s| s.to_string());
        let category = body["Category"].as_str().map(|s| s.to_string());
        let ops_item_type = body["OpsItemType"].as_str().map(|s| s.to_string());
        let operational_data: HashMap<String, serde_json::Value> = body["OperationalData"]
            .as_object()
            .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
        let notifications: Vec<serde_json::Value> = body["Notifications"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let related_ops_items: Vec<serde_json::Value> = body["RelatedOpsItems"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let tags: HashMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let now = Utc::now();
        let mut state = self.state.write();
        state.ops_item_counter += 1;
        let ops_item_id = format!("oi-{:012x}", state.ops_item_counter);

        let item = SsmOpsItem {
            ops_item_id: ops_item_id.clone(),
            title,
            description,
            source,
            status: "Open".to_string(),
            priority,
            severity,
            category,
            operational_data,
            notifications,
            related_ops_items,
            tags,
            created_time: now,
            last_modified_time: now,
            created_by: format!("arn:aws:iam::{}:root", state.account_id),
            last_modified_by: format!("arn:aws:iam::{}:root", state.account_id),
            ops_item_type,
            planned_start_time: None,
            planned_end_time: None,
            actual_start_time: None,
            actual_end_time: None,
        };

        state.ops_items.insert(ops_item_id.clone(), item);

        Ok(json_resp(json!({ "OpsItemId": ops_item_id })))
    }

    fn get_ops_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?;

        let state = self.state.read();
        let item = state.ops_items.get(ops_item_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsItemNotFoundException",
                format!("OpsItem ID {ops_item_id} not found"),
            )
        })?;

        Ok(json_resp(json!({ "OpsItem": ops_item_to_json(item) })))
    }

    fn update_ops_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?;

        let mut state = self.state.write();
        let account_id = state.account_id.clone();
        let item = state.ops_items.get_mut(ops_item_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsItemNotFoundException",
                format!("OpsItem ID {ops_item_id} not found"),
            )
        })?;

        if let Some(t) = body["Title"].as_str() {
            item.title = t.to_string();
        }
        if let Some(d) = body["Description"].as_str() {
            item.description = Some(d.to_string());
        }
        if let Some(s) = body["Status"].as_str() {
            item.status = s.to_string();
        }
        if let Some(p) = body["Priority"].as_i64() {
            item.priority = Some(p);
        }
        if let Some(s) = body["Severity"].as_str() {
            item.severity = Some(s.to_string());
        }
        if let Some(c) = body["Category"].as_str() {
            item.category = Some(c.to_string());
        }
        if let Some(obj) = body["OperationalData"].as_object() {
            for (k, v) in obj {
                item.operational_data.insert(k.clone(), v.clone());
            }
        }
        if let Some(arr) = body["Notifications"].as_array() {
            item.notifications = arr.clone();
        }
        if let Some(arr) = body["RelatedOpsItems"].as_array() {
            item.related_ops_items = arr.clone();
        }

        item.last_modified_time = Utc::now();
        item.last_modified_by = format!("arn:aws:iam::{}:root", account_id);

        Ok(json_resp(json!({})))
    }

    fn delete_ops_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?;

        let mut state = self.state.write();
        state.ops_items.remove(ops_item_id);
        Ok(json_resp(json!({})))
    }

    fn describe_ops_items(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
        let all: Vec<Value> = state
            .ops_items
            .values()
            .map(|item| {
                let mut v = json!({
                    "OpsItemId": item.ops_item_id,
                    "Title": item.title,
                    "Status": item.status,
                    "Source": item.source,
                    "CreatedTime": item.created_time.timestamp_millis() as f64 / 1000.0,
                    "LastModifiedTime": item.last_modified_time.timestamp_millis() as f64 / 1000.0,
                    "CreatedBy": item.created_by,
                    "LastModifiedBy": item.last_modified_by,
                });
                if let Some(p) = item.priority {
                    v["Priority"] = json!(p);
                }
                if let Some(ref s) = item.severity {
                    v["Severity"] = json!(s);
                }
                if let Some(ref c) = item.category {
                    v["Category"] = json!(c);
                }
                v
            })
            .collect();

        let page = if next_token_offset < all.len() {
            &all[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "OpsItemSummaries": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    // -----------------------------------------------------------------------
    // Document extras
    // -----------------------------------------------------------------------

    fn list_document_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
        let doc = state
            .documents
            .get(name)
            .ok_or_else(|| doc_not_found(name))?;

        let all: Vec<Value> = doc
            .versions
            .iter()
            .map(|v| {
                json!({
                    "Name": name,
                    "DocumentVersion": v.document_version,
                    "CreatedDate": v.created_date.timestamp_millis() as f64 / 1000.0,
                    "IsDefaultVersion": v.is_default_version,
                    "DocumentFormat": v.document_format,
                    "Status": v.status,
                    "VersionName": v.version_name,
                })
            })
            .collect();

        let page = if next_token_offset < all.len() {
            &all[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();

        let mut resp = json!({ "DocumentVersions": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    fn list_document_metadata_history(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let _name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_required("Metadata", &body["Metadata"])?;
        validate_optional_enum("Metadata", body["Metadata"].as_str(), &["DocumentReviews"])?;
        let _metadata = body["Metadata"]
            .as_str()
            .ok_or_else(|| missing("Metadata"))?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;

        // Stub: return empty metadata
        Ok(json_resp(json!({
            "Name": _name,
            "Author": "",
            "Metadata": {
                "ReviewerResponse": []
            }
        })))
    }

    fn update_document_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        if !state.documents.contains_key(name) {
            return Err(doc_not_found(name));
        }

        // Stub: accept but do nothing
        Ok(json_resp(json!({})))
    }

    // -----------------------------------------------------------------------
    // Resource policies
    // -----------------------------------------------------------------------

    fn put_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ResourceArn", body["ResourceArn"].as_str(), 20, 2048)?;
        let resource_arn = body["ResourceArn"]
            .as_str()
            .ok_or_else(|| missing("ResourceArn"))?
            .to_string();
        let policy = body["Policy"]
            .as_str()
            .ok_or_else(|| missing("Policy"))?
            .to_string();

        let policy_id = body["PolicyId"].as_str().map(|s| s.to_string());
        let policy_hash = body["PolicyHash"].as_str().map(|s| s.to_string());

        let mut state = self.state.write();

        // If PolicyId is provided, update existing
        if let Some(ref pid) = policy_id {
            if let Some(existing) = state
                .resource_policies
                .iter_mut()
                .find(|p| p.policy_id == *pid && p.resource_arn == resource_arn)
            {
                // Verify hash matches if provided
                if let Some(ref expected_hash) = policy_hash {
                    if existing.policy_hash != *expected_hash {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ResourcePolicyConflictException",
                            "The policy hash does not match.".to_string(),
                        ));
                    }
                }
                existing.policy = policy;
                let new_hash = format!("{:x}", md5::compute(existing.policy.as_bytes()));
                existing.policy_hash = new_hash.clone();
                return Ok(json_resp(json!({
                    "PolicyId": pid,
                    "PolicyHash": new_hash,
                })));
            }
        }

        // Create new
        let new_id = uuid::Uuid::new_v4().to_string();
        let new_hash = format!("{:x}", md5::compute(policy.as_bytes()));
        state.resource_policies.push(SsmResourcePolicy {
            policy_id: new_id.clone(),
            policy_hash: new_hash.clone(),
            policy,
            resource_arn,
        });

        Ok(json_resp(json!({
            "PolicyId": new_id,
            "PolicyHash": new_hash,
        })))
    }

    fn get_resource_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ResourceArn", body["ResourceArn"].as_str(), 20, 2048)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let resource_arn = body["ResourceArn"]
            .as_str()
            .ok_or_else(|| missing("ResourceArn"))?;

        let state = self.state.read();
        let policies: Vec<Value> = state
            .resource_policies
            .iter()
            .filter(|p| p.resource_arn == resource_arn)
            .map(|p| {
                json!({
                    "PolicyId": p.policy_id,
                    "PolicyHash": p.policy_hash,
                    "Policy": p.policy,
                })
            })
            .collect();

        Ok(json_resp(json!({ "Policies": policies })))
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let resource_arn = body["ResourceArn"]
            .as_str()
            .ok_or_else(|| missing("ResourceArn"))?;
        let policy_id = body["PolicyId"]
            .as_str()
            .ok_or_else(|| missing("PolicyId"))?;
        let policy_hash = body["PolicyHash"]
            .as_str()
            .ok_or_else(|| missing("PolicyHash"))?;

        let mut state = self.state.write();
        let idx = state
            .resource_policies
            .iter()
            .position(|p| p.resource_arn == resource_arn && p.policy_id == policy_id);

        match idx {
            Some(i) => {
                if state.resource_policies[i].policy_hash != policy_hash {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourcePolicyConflictException",
                        "The policy hash does not match.".to_string(),
                    ));
                }
                state.resource_policies.remove(i);
                Ok(json_resp(json!({})))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourcePolicyNotFoundException",
                "The resource policy was not found.".to_string(),
            )),
        }
    }

    // -----------------------------------------------------------------------
    // Stubs
    // -----------------------------------------------------------------------

    fn get_connection_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("Target", body["Target"].as_str(), 1, 400)?;
        let target = body["Target"].as_str().ok_or_else(|| missing("Target"))?;
        Ok(json_resp(json!({
            "Target": target,
            "Status": "connected",
        })))
    }

    fn get_calendar_state(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let _calendar_names = body["CalendarNames"]
            .as_array()
            .ok_or_else(|| missing("CalendarNames"))?;
        Ok(json_resp(json!({
            "State": "OPEN",
            "AtTime": Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        })))
    }

    fn describe_patch_group_state(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("PatchGroup", body["PatchGroup"].as_str(), 1, 256)?;
        let _patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        Ok(json_resp(json!({
            "Instances": 0,
            "InstancesWithInstalledPatches": 0,
            "InstancesWithInstalledOtherPatches": 0,
            "InstancesWithInstalledRejectedPatches": 0,
            "InstancesWithInstalledPendingRebootPatches": 0,
            "InstancesWithMissingPatches": 0,
            "InstancesWithFailedPatches": 0,
            "InstancesWithNotApplicablePatches": 0,
            "InstancesWithUnreportedNotApplicablePatches": 0,
            "InstancesWithCriticalNonCompliantPatches": 0,
            "InstancesWithSecurityNonCompliantPatches": 0,
            "InstancesWithOtherNonCompliantPatches": 0,
        })))
    }

    fn describe_patch_properties(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_required("OperatingSystem", &body["OperatingSystem"])?;
        validate_optional_enum(
            "OperatingSystem",
            body["OperatingSystem"].as_str(),
            &[
                "WINDOWS",
                "AMAZON_LINUX",
                "AMAZON_LINUX_2",
                "AMAZON_LINUX_2022",
                "UBUNTU",
                "REDHAT_ENTERPRISE_LINUX",
                "SUSE",
                "CENTOS",
                "ORACLE_LINUX",
                "DEBIAN",
                "MACOS",
                "RASPBIAN",
                "ROCKY_LINUX",
                "ALMA_LINUX",
                "AMAZON_LINUX_2023",
            ],
        )?;
        validate_required("Property", &body["Property"])?;
        validate_optional_enum(
            "Property",
            body["Property"].as_str(),
            &[
                "PRODUCT",
                "PRODUCT_FAMILY",
                "CLASSIFICATION",
                "MSRC_SEVERITY",
                "PRIORITY",
                "SEVERITY",
            ],
        )?;
        validate_optional_enum(
            "PatchSet",
            body["PatchSet"].as_str(),
            &["OS", "APPLICATION"],
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        Ok(json_resp(json!({ "Properties": [] })))
    }

    fn get_default_patch_baseline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_enum(
            "OperatingSystem",
            body["OperatingSystem"].as_str(),
            &[
                "WINDOWS",
                "AMAZON_LINUX",
                "AMAZON_LINUX_2",
                "AMAZON_LINUX_2022",
                "UBUNTU",
                "REDHAT_ENTERPRISE_LINUX",
                "SUSE",
                "CENTOS",
                "ORACLE_LINUX",
                "DEBIAN",
                "MACOS",
                "RASPBIAN",
                "ROCKY_LINUX",
                "ALMA_LINUX",
                "AMAZON_LINUX_2023",
            ],
        )?;
        let operating_system = body["OperatingSystem"].as_str().unwrap_or("WINDOWS");

        let state = self.state.read();

        // Check if a custom default has been registered
        if let Some(ref baseline_id) = state.default_patch_baseline_id {
            return Ok(json_resp(json!({
                "BaselineId": baseline_id,
                "OperatingSystem": operating_system,
            })));
        }

        // Otherwise look up from defaults
        let baseline_id =
            default_patch_baseline(&state.region, operating_system).unwrap_or_default();
        Ok(json_resp(json!({
            "BaselineId": baseline_id,
            "OperatingSystem": operating_system,
        })))
    }

    fn register_default_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?
            .to_string();

        let mut state = self.state.write();

        // Verify baseline exists (custom or default)
        if !state.patch_baselines.contains_key(&baseline_id)
            && !is_default_patch_baseline(&baseline_id)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Patch baseline {baseline_id} does not exist"),
            ));
        }

        state.default_patch_baseline_id = Some(baseline_id.clone());
        Ok(json_resp(json!({
            "BaselineId": baseline_id,
        })))
    }

    fn describe_available_patches(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 100)?;
        Ok(json_resp(json!({ "Patches": [] })))
    }

    fn get_service_setting(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SettingId", body["SettingId"].as_str(), 1, 1000)?;
        let setting_id = body["SettingId"]
            .as_str()
            .ok_or_else(|| missing("SettingId"))?;

        let state = self.state.read();
        if let Some(setting) = state.service_settings.get(setting_id) {
            Ok(json_resp(json!({
                "ServiceSetting": {
                    "SettingId": setting.setting_id,
                    "SettingValue": setting.setting_value,
                    "LastModifiedDate": setting.last_modified_date.timestamp_millis() as f64 / 1000.0,
                    "LastModifiedUser": setting.last_modified_user,
                    "ARN": format!("arn:aws:ssm:{}:{}:servicesetting/{}", state.region, state.account_id, setting.setting_id),
                    "Status": setting.status,
                }
            })))
        } else {
            // Return sensible default for known settings
            Ok(json_resp(json!({
                "ServiceSetting": {
                    "SettingId": setting_id,
                    "SettingValue": get_default_service_setting(setting_id),
                    "LastModifiedDate": Utc::now().timestamp_millis() as f64 / 1000.0,
                    "LastModifiedUser": "System",
                    "ARN": format!("arn:aws:ssm:{}:{}:servicesetting/{}", state.region, state.account_id, setting_id),
                    "Status": "Default",
                }
            })))
        }
    }

    fn reset_service_setting(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SettingId", body["SettingId"].as_str(), 1, 1000)?;
        let setting_id = body["SettingId"]
            .as_str()
            .ok_or_else(|| missing("SettingId"))?;

        let mut state = self.state.write();
        state.service_settings.remove(setting_id);

        let default_value = get_default_service_setting(setting_id);
        Ok(json_resp(json!({
            "ServiceSetting": {
                "SettingId": setting_id,
                "SettingValue": default_value,
                "LastModifiedDate": Utc::now().timestamp_millis() as f64 / 1000.0,
                "LastModifiedUser": "System",
                "ARN": format!("arn:aws:ssm:{}:{}:servicesetting/{}", state.region, state.account_id, setting_id),
                "Status": "Default",
            }
        })))
    }

    fn update_service_setting(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SettingId", body["SettingId"].as_str(), 1, 1000)?;
        validate_optional_string_length("SettingValue", body["SettingValue"].as_str(), 1, 4096)?;
        let setting_id = body["SettingId"]
            .as_str()
            .ok_or_else(|| missing("SettingId"))?
            .to_string();
        let setting_value = body["SettingValue"]
            .as_str()
            .ok_or_else(|| missing("SettingValue"))?
            .to_string();

        let mut state = self.state.write();
        let now = Utc::now();
        let account_id = state.account_id.clone();
        state.service_settings.insert(
            setting_id.clone(),
            SsmServiceSetting {
                setting_id,
                setting_value,
                last_modified_date: now,
                last_modified_user: format!("arn:aws:iam::{}:root", account_id),
                status: "Customized".to_string(),
            },
        );

        Ok(json_resp(json!({})))
    }

    // ── Inventory ─────────────────────────────────────────────────

    fn put_inventory(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?
            .to_string();
        let items = body["Items"].as_array().ok_or_else(|| missing("Items"))?;

        let mut inv_items = Vec::new();
        for item in items {
            let type_name = item["TypeName"]
                .as_str()
                .ok_or_else(|| missing("TypeName"))?
                .to_string();
            let schema_version = item["SchemaVersion"]
                .as_str()
                .ok_or_else(|| missing("SchemaVersion"))?
                .to_string();
            let capture_time = item["CaptureTime"]
                .as_str()
                .ok_or_else(|| missing("CaptureTime"))?
                .to_string();
            let content: Vec<HashMap<String, String>> = item["Content"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| {
                            v.as_object().map(|obj| {
                                obj.iter()
                                    .filter_map(|(k, v)| {
                                        v.as_str().map(|s| (k.clone(), s.to_string()))
                                    })
                                    .collect()
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            let content_hash = item["ContentHash"].as_str().map(|s| s.to_string());
            let context: Option<HashMap<String, String>> = item["Context"].as_object().map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            });

            inv_items.push(InventoryItem {
                type_name,
                schema_version,
                capture_time,
                content,
                content_hash,
                context,
            });
        }

        let mut state = self.state.write();
        let entry = state
            .inventory_entries
            .entry(instance_id.clone())
            .or_insert_with(|| InventoryEntry {
                instance_id: instance_id.clone(),
                items: Vec::new(),
            });

        // Merge: replace items by TypeName, add new ones
        for new_item in inv_items {
            if let Some(existing) = entry
                .items
                .iter_mut()
                .find(|i| i.type_name == new_item.type_name)
            {
                *existing = new_item;
            } else {
                entry.items.push(new_item);
            }
        }

        Ok(json_resp(
            json!({ "Message": "Inventory was saved successfully" }),
        ))
    }

    fn get_inventory(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let entities: Vec<Value> = state
            .inventory_entries
            .values()
            .map(|entry| {
                let data: HashMap<String, Value> = entry
                    .items
                    .iter()
                    .map(|item| {
                        (
                            item.type_name.clone(),
                            json!({
                                "TypeName": item.type_name,
                                "SchemaVersion": item.schema_version,
                                "CaptureTime": item.capture_time,
                                "Content": item.content,
                            }),
                        )
                    })
                    .collect();
                json!({
                    "Id": entry.instance_id,
                    "Data": data,
                })
            })
            .collect();
        Ok(json_resp(json!({ "Entities": entities })))
    }

    fn get_inventory_schema(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("TypeName", body["TypeName"].as_str(), 0, 100)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 50, 200)?;
        // Return standard inventory type schemas
        let schemas = vec![
            json!({
                "TypeName": "AWS:Application",
                "Version": "1.1",
                "Attributes": [
                    {"Name": "Name", "DataType": "STRING"},
                    {"Name": "ApplicationType", "DataType": "STRING"},
                    {"Name": "Publisher", "DataType": "STRING"},
                    {"Name": "Version", "DataType": "STRING"},
                    {"Name": "InstalledTime", "DataType": "STRING"},
                    {"Name": "Architecture", "DataType": "STRING"},
                    {"Name": "URL", "DataType": "STRING"},
                ]
            }),
            json!({
                "TypeName": "AWS:InstanceInformation",
                "Version": "1.0",
                "Attributes": [
                    {"Name": "AgentType", "DataType": "STRING"},
                    {"Name": "AgentVersion", "DataType": "STRING"},
                    {"Name": "ComputerName", "DataType": "STRING"},
                    {"Name": "InstanceId", "DataType": "STRING"},
                    {"Name": "IpAddress", "DataType": "STRING"},
                    {"Name": "PlatformName", "DataType": "STRING"},
                    {"Name": "PlatformType", "DataType": "STRING"},
                    {"Name": "PlatformVersion", "DataType": "STRING"},
                    {"Name": "ResourceType", "DataType": "STRING"},
                ]
            }),
            json!({
                "TypeName": "AWS:Network",
                "Version": "1.0",
                "Attributes": [
                    {"Name": "Name", "DataType": "STRING"},
                    {"Name": "SubnetMask", "DataType": "STRING"},
                    {"Name": "Gateway", "DataType": "STRING"},
                    {"Name": "DHCPServer", "DataType": "STRING"},
                    {"Name": "DNSServer", "DataType": "STRING"},
                    {"Name": "MacAddress", "DataType": "STRING"},
                    {"Name": "IPV4", "DataType": "STRING"},
                    {"Name": "IPV6", "DataType": "STRING"},
                ]
            }),
        ];
        Ok(json_resp(json!({ "Schemas": schemas })))
    }

    fn list_inventory_entries(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("TypeName", body["TypeName"].as_str(), 1, 100)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let type_name = body["TypeName"]
            .as_str()
            .ok_or_else(|| missing("TypeName"))?;

        let state = self.state.read();
        let entries: Vec<&HashMap<String, String>> = state
            .inventory_entries
            .get(instance_id)
            .map(|entry| {
                entry
                    .items
                    .iter()
                    .filter(|item| item.type_name == type_name)
                    .flat_map(|item| item.content.iter())
                    .collect()
            })
            .unwrap_or_default();

        let capture_time = state
            .inventory_entries
            .get(instance_id)
            .and_then(|e| e.items.iter().find(|i| i.type_name == type_name))
            .map(|i| i.capture_time.as_str())
            .unwrap_or("");
        let schema_version = state
            .inventory_entries
            .get(instance_id)
            .and_then(|e| e.items.iter().find(|i| i.type_name == type_name))
            .map(|i| i.schema_version.as_str())
            .unwrap_or("1.0");

        Ok(json_resp(json!({
            "TypeName": type_name,
            "InstanceId": instance_id,
            "SchemaVersion": schema_version,
            "CaptureTime": capture_time,
            "Entries": entries,
        })))
    }

    fn delete_inventory(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("TypeName", body["TypeName"].as_str(), 1, 100)?;
        validate_optional_enum(
            "SchemaDeleteOption",
            body["SchemaDeleteOption"].as_str(),
            &["DISABLE_SCHEMA", "DELETE_SCHEMA"],
        )?;
        let type_name = body["TypeName"]
            .as_str()
            .ok_or_else(|| missing("TypeName"))?
            .to_string();

        let mut state = self.state.write();

        // Remove matching inventory items
        for entry in state.inventory_entries.values_mut() {
            entry.items.retain(|i| i.type_name != type_name);
        }

        state.inventory_deletion_counter += 1;
        let deletion_id = format!("{}", uuid::Uuid::new_v4());
        let now = Utc::now();

        state.inventory_deletions.push(InventoryDeletion {
            deletion_id: deletion_id.clone(),
            type_name: type_name.clone(),
            deletion_start_time: now,
            last_status: "Complete".to_string(),
            last_status_message: "Deletion completed successfully.".to_string(),
            deletion_summary: json!({
                "TotalCount": 0,
                "RemainingCount": 0,
                "SummaryItems": [],
            }),
            last_status_update_time: now,
        });

        Ok(json_resp(json!({
            "DeletionId": deletion_id,
            "TypeName": type_name,
            "DeletionSummary": {
                "TotalCount": 0,
                "RemainingCount": 0,
                "SummaryItems": [],
            },
        })))
    }

    fn describe_inventory_deletions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let deletions: Vec<Value> = state
            .inventory_deletions
            .iter()
            .map(|d| {
                json!({
                    "DeletionId": d.deletion_id,
                    "TypeName": d.type_name,
                    "DeletionStartTime": d.deletion_start_time.timestamp_millis() as f64 / 1000.0,
                    "LastStatus": d.last_status,
                    "LastStatusMessage": d.last_status_message,
                    "DeletionSummary": d.deletion_summary,
                    "LastStatusUpdateTime": d.last_status_update_time.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();
        Ok(json_resp(json!({ "InventoryDeletions": deletions })))
    }

    // ── Compliance ────────────────────────────────────────────────

    fn put_compliance_items(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ResourceId", body["ResourceId"].as_str(), 1, 100)?;
        validate_optional_string_length("ResourceType", body["ResourceType"].as_str(), 1, 50)?;
        validate_optional_string_length("ComplianceType", body["ComplianceType"].as_str(), 1, 100)?;
        validate_optional_string_length(
            "ItemContentHash",
            body["ItemContentHash"].as_str(),
            0,
            256,
        )?;
        validate_optional_enum(
            "UploadType",
            body["UploadType"].as_str(),
            &["COMPLETE", "PARTIAL"],
        )?;
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?
            .to_string();
        let resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?
            .to_string();
        let compliance_type = body["ComplianceType"]
            .as_str()
            .ok_or_else(|| missing("ComplianceType"))?
            .to_string();
        let execution_summary = body
            .get("ExecutionSummary")
            .cloned()
            .ok_or_else(|| missing("ExecutionSummary"))?;
        let items = body["Items"].as_array().ok_or_else(|| missing("Items"))?;

        let mut state = self.state.write();

        // Remove existing compliance items for this resource/type
        state
            .compliance_items
            .retain(|c| !(c.resource_id == resource_id && c.compliance_type == compliance_type));

        for item in items {
            let severity = item["Severity"]
                .as_str()
                .unwrap_or("UNSPECIFIED")
                .to_string();
            let status = item["Status"].as_str().unwrap_or("COMPLIANT").to_string();
            let title = item["Title"].as_str().map(|s| s.to_string());
            let id = item["Id"].as_str().map(|s| s.to_string());
            let details: HashMap<String, String> = item["Details"]
                .as_object()
                .map(|obj| {
                    obj.iter()
                        .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                        .collect()
                })
                .unwrap_or_default();

            state.compliance_items.push(ComplianceItem {
                resource_id: resource_id.clone(),
                resource_type: resource_type.clone(),
                compliance_type: compliance_type.clone(),
                severity,
                status,
                title,
                id,
                details,
                execution_summary: execution_summary.clone(),
            });
        }

        Ok(json_resp(json!({})))
    }

    fn list_compliance_items(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let resource_ids: Vec<&str> = body["ResourceIds"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();
        let resource_types: Vec<&str> = body["ResourceTypes"]
            .as_array()
            .map(|a| a.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let state = self.state.read();
        let all_items: Vec<Value> = state
            .compliance_items
            .iter()
            .filter(|c| {
                if !resource_ids.is_empty() && !resource_ids.contains(&c.resource_id.as_str()) {
                    return false;
                }
                if !resource_types.is_empty() && !resource_types.contains(&c.resource_type.as_str())
                {
                    return false;
                }
                true
            })
            .map(|c| {
                let mut v = json!({
                    "ResourceId": c.resource_id,
                    "ResourceType": c.resource_type,
                    "ComplianceType": c.compliance_type,
                    "Severity": c.severity,
                    "Status": c.status,
                    "ExecutionSummary": c.execution_summary,
                });
                if let Some(ref title) = c.title {
                    v["Title"] = json!(title);
                }
                if let Some(ref id) = c.id {
                    v["Id"] = json!(id);
                }
                if !c.details.is_empty() {
                    v["Details"] = json!(c.details);
                }
                v
            })
            .collect();

        let page = if next_token_offset < all_items.len() {
            &all_items[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();
        let mut resp = json!({ "ComplianceItems": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    fn list_compliance_summaries(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();

        // Group by compliance_type
        let mut type_counts: HashMap<String, (i64, i64)> = HashMap::new(); // (compliant, non_compliant)
        for item in &state.compliance_items {
            let entry = type_counts
                .entry(item.compliance_type.clone())
                .or_insert((0, 0));
            if item.status == "COMPLIANT" {
                entry.0 += 1;
            } else {
                entry.1 += 1;
            }
        }

        let summaries: Vec<Value> = type_counts
            .iter()
            .map(|(ct, (compliant, non_compliant))| {
                json!({
                    "ComplianceType": ct,
                    "CompliantSummary": {
                        "CompliantCount": compliant,
                        "SeveritySummary": {"CriticalCount": 0, "HighCount": 0, "MediumCount": 0, "LowCount": 0, "InformationalCount": 0, "UnspecifiedCount": 0},
                    },
                    "NonCompliantSummary": {
                        "NonCompliantCount": non_compliant,
                        "SeveritySummary": {"CriticalCount": 0, "HighCount": 0, "MediumCount": 0, "LowCount": 0, "InformationalCount": 0, "UnspecifiedCount": 0},
                    },
                })
            })
            .collect();

        Ok(json_resp(json!({ "ComplianceSummaryItems": summaries })))
    }

    fn list_resource_compliance_summaries(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();

        // Group by resource_id
        let mut resource_status: HashMap<String, (String, String, i64, i64)> = HashMap::new();
        for item in &state.compliance_items {
            let entry = resource_status
                .entry(item.resource_id.clone())
                .or_insert_with(|| (item.resource_type.clone(), "COMPLIANT".to_string(), 0, 0));
            if item.status == "COMPLIANT" {
                entry.2 += 1;
            } else {
                entry.1 = "NON_COMPLIANT".to_string();
                entry.3 += 1;
            }
        }

        let summaries: Vec<Value> = resource_status
            .iter()
            .map(
                |(resource_id, (resource_type, status, compliant, non_compliant))| {
                    json!({
                        "ResourceId": resource_id,
                        "ResourceType": resource_type,
                        "Status": status,
                        "OverallSeverity": "UNSPECIFIED",
                        "CompliantSummary": {
                            "CompliantCount": compliant,
                            "SeveritySummary": {"CriticalCount": 0, "HighCount": 0, "MediumCount": 0, "LowCount": 0, "InformationalCount": 0, "UnspecifiedCount": 0},
                        },
                        "NonCompliantSummary": {
                            "NonCompliantCount": non_compliant,
                            "SeveritySummary": {"CriticalCount": 0, "HighCount": 0, "MediumCount": 0, "LowCount": 0, "InformationalCount": 0, "UnspecifiedCount": 0},
                        },
                    })
                },
            )
            .collect();

        Ok(json_resp(
            json!({ "ResourceComplianceSummaryItems": summaries }),
        ))
    }

    // ── Maintenance Window Details ────────────────────────────────

    fn update_maintenance_window_target(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let target_id = body["WindowTargetId"]
            .as_str()
            .ok_or_else(|| missing("WindowTargetId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let target = mw
            .targets
            .iter_mut()
            .find(|t| t.window_target_id == target_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Target {target_id} does not exist in window {window_id}"),
                )
            })?;

        if let Some(name) = body["Name"].as_str() {
            target.name = Some(name.to_string());
        }
        if body.get("Description").is_some() {
            target.description = body["Description"].as_str().map(|s| s.to_string());
        }
        if let Some(targets) = body["Targets"].as_array() {
            target.targets = targets.clone();
        }
        if body.get("OwnerInformation").is_some() {
            target.owner_information = body["OwnerInformation"].as_str().map(|s| s.to_string());
        }

        let mut resp = json!({
            "WindowId": window_id,
            "WindowTargetId": target_id,
            "Targets": target.targets,
        });
        if let Some(ref name) = target.name {
            resp["Name"] = json!(name);
        }
        if let Some(ref desc) = target.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref oi) = target.owner_information {
            resp["OwnerInformation"] = json!(oi);
        }

        Ok(json_resp(resp))
    }

    fn update_maintenance_window_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let mut state = self.state.write();
        let mw = state
            .maintenance_windows
            .get_mut(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let task = mw
            .tasks
            .iter_mut()
            .find(|t| t.window_task_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Task {task_id} does not exist in window {window_id}"),
                )
            })?;

        if let Some(name) = body["Name"].as_str() {
            task.name = Some(name.to_string());
        }
        if body.get("Description").is_some() {
            task.description = body["Description"].as_str().map(|s| s.to_string());
        }
        if let Some(targets) = body["Targets"].as_array() {
            task.targets = targets.clone();
        }
        if let Some(task_arn) = body["TaskArn"].as_str() {
            task.task_arn = task_arn.to_string();
        }
        if let Some(mc) = body["MaxConcurrency"].as_str() {
            task.max_concurrency = Some(mc.to_string());
        }
        if let Some(me) = body["MaxErrors"].as_str() {
            task.max_errors = Some(me.to_string());
        }
        if let Some(p) = body["Priority"].as_i64() {
            task.priority = p;
        }

        let mut resp = json!({
            "WindowId": window_id,
            "WindowTaskId": task_id,
            "TaskArn": task.task_arn,
            "TaskType": task.task_type,
            "Targets": task.targets,
            "Priority": task.priority,
        });
        if let Some(ref name) = task.name {
            resp["Name"] = json!(name);
        }
        if let Some(ref desc) = task.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref mc) = task.max_concurrency {
            resp["MaxConcurrency"] = json!(mc);
        }
        if let Some(ref me) = task.max_errors {
            resp["MaxErrors"] = json!(me);
        }

        Ok(json_resp(resp))
    }

    fn get_maintenance_window_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let task_id = body["WindowTaskId"]
            .as_str()
            .ok_or_else(|| missing("WindowTaskId"))?;

        let state = self.state.read();
        let mw = state
            .maintenance_windows
            .get(window_id)
            .ok_or_else(|| mw_not_found(window_id))?;

        let task = mw
            .tasks
            .iter()
            .find(|t| t.window_task_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Task {task_id} does not exist in window {window_id}"),
                )
            })?;

        let mut resp = json!({
            "WindowId": window_id,
            "WindowTaskId": task_id,
            "TaskArn": task.task_arn,
            "TaskType": task.task_type,
            "Targets": task.targets,
            "Priority": task.priority,
        });
        if let Some(ref name) = task.name {
            resp["Name"] = json!(name);
        }
        if let Some(ref desc) = task.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref mc) = task.max_concurrency {
            resp["MaxConcurrency"] = json!(mc);
        }
        if let Some(ref me) = task.max_errors {
            resp["MaxErrors"] = json!(me);
        }
        if let Some(ref sra) = task.service_role_arn {
            resp["ServiceRoleArn"] = json!(sra);
        }

        Ok(json_resp(resp))
    }

    fn get_maintenance_window_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;

        let state = self.state.read();
        let exec = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Execution {execution_id} does not exist"),
                )
            })?;

        let mut resp = json!({
            "WindowExecutionId": exec.window_execution_id,
            "WindowId": exec.window_id,
            "Status": exec.status,
            "StartTime": exec.start_time.timestamp_millis() as f64 / 1000.0,
            "TaskIds": exec.tasks.iter().map(|t| &t.task_execution_id).collect::<Vec<_>>(),
        });
        if let Some(ref end) = exec.end_time {
            resp["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
        }

        Ok(json_resp(resp))
    }

    fn get_maintenance_window_execution_task(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;
        let task_id = body["TaskId"].as_str().ok_or_else(|| missing("TaskId"))?;

        let state = self.state.read();
        let exec = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Execution {execution_id} does not exist"),
                )
            })?;

        let task = exec
            .tasks
            .iter()
            .find(|t| t.task_execution_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Task {task_id} does not exist in execution {execution_id}"),
                )
            })?;

        let mut resp = json!({
            "WindowExecutionId": execution_id,
            "TaskExecutionId": task.task_execution_id,
            "TaskArn": task.task_arn,
            "Type": task.task_type,
            "Status": task.status,
            "StartTime": task.start_time.timestamp_millis() as f64 / 1000.0,
        });
        if let Some(ref end) = task.end_time {
            resp["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
        }

        Ok(json_resp(resp))
    }

    fn get_maintenance_window_execution_task_invocation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;
        let task_id = body["TaskId"].as_str().ok_or_else(|| missing("TaskId"))?;
        let invocation_id = body["InvocationId"]
            .as_str()
            .ok_or_else(|| missing("InvocationId"))?;

        let state = self.state.read();
        let exec = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Execution {execution_id} does not exist"),
                )
            })?;

        let task = exec
            .tasks
            .iter()
            .find(|t| t.task_execution_id == task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Task {task_id} does not exist"),
                )
            })?;

        let inv = task
            .invocations
            .iter()
            .find(|i| i.invocation_id == invocation_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Invocation {invocation_id} does not exist"),
                )
            })?;

        let mut resp = json!({
            "WindowExecutionId": execution_id,
            "TaskExecutionId": task_id,
            "InvocationId": invocation_id,
            "Status": inv.status,
            "StartTime": inv.start_time.timestamp_millis() as f64 / 1000.0,
        });
        if let Some(ref end) = inv.end_time {
            resp["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
        }
        if let Some(ref eid) = inv.execution_id {
            resp["ExecutionId"] = json!(eid);
        }
        if let Some(ref p) = inv.parameters {
            resp["Parameters"] = json!(p);
        }
        if let Some(ref oi) = inv.owner_information {
            resp["OwnerInformation"] = json!(oi);
        }
        if let Some(ref wtid) = inv.window_target_id {
            resp["WindowTargetId"] = json!(wtid);
        }
        if let Some(ref sd) = inv.status_details {
            resp["StatusDetails"] = json!(sd);
        }

        Ok(json_resp(resp))
    }

    fn describe_maintenance_window_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("WindowId", body["WindowId"].as_str(), 20, 20)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let window_id = body["WindowId"]
            .as_str()
            .ok_or_else(|| missing("WindowId"))?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50) as usize;
        let next_token_offset: usize = body["NextToken"]
            .as_str()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);

        let state = self.state.read();
        let all: Vec<Value> = state
            .maintenance_window_executions
            .iter()
            .filter(|e| e.window_id == window_id)
            .map(|e| {
                let mut v = json!({
                    "WindowId": e.window_id,
                    "WindowExecutionId": e.window_execution_id,
                    "Status": e.status,
                    "StartTime": e.start_time.timestamp_millis() as f64 / 1000.0,
                });
                if let Some(ref end) = e.end_time {
                    v["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
                }
                v
            })
            .collect();

        let page = if next_token_offset < all.len() {
            &all[next_token_offset..]
        } else {
            &[]
        };
        let has_more = page.len() > max_results;
        let items: Vec<Value> = page.iter().take(max_results).cloned().collect();
        let mut resp = json!({ "WindowExecutions": items });
        if has_more {
            resp["NextToken"] = json!((next_token_offset + max_results).to_string());
        }
        Ok(json_resp(resp))
    }

    fn describe_maintenance_window_execution_tasks(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length(
            "WindowExecutionId",
            body["WindowExecutionId"].as_str(),
            36,
            36,
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;

        let state = self.state.read();
        let tasks: Vec<Value> = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .map(|e| {
                e.tasks
                    .iter()
                    .map(|t| {
                        let mut v = json!({
                            "WindowExecutionId": execution_id,
                            "TaskExecutionId": t.task_execution_id,
                            "TaskArn": t.task_arn,
                            "Type": t.task_type,
                            "Status": t.status,
                            "StartTime": t.start_time.timestamp_millis() as f64 / 1000.0,
                        });
                        if let Some(ref end) = t.end_time {
                            v["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
                        }
                        v
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(json_resp(json!({ "WindowExecutionTaskIdentities": tasks })))
    }

    fn describe_maintenance_window_execution_task_invocations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length(
            "WindowExecutionId",
            body["WindowExecutionId"].as_str(),
            36,
            36,
        )?;
        validate_optional_string_length("TaskId", body["TaskId"].as_str(), 36, 36)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;
        let task_id = body["TaskId"].as_str().ok_or_else(|| missing("TaskId"))?;

        let state = self.state.read();
        let invocations: Vec<Value> = state
            .maintenance_window_executions
            .iter()
            .find(|e| e.window_execution_id == execution_id)
            .and_then(|e| e.tasks.iter().find(|t| t.task_execution_id == task_id))
            .map(|t| {
                t.invocations
                    .iter()
                    .map(|i| {
                        let mut v = json!({
                            "WindowExecutionId": execution_id,
                            "TaskExecutionId": task_id,
                            "InvocationId": i.invocation_id,
                            "Status": i.status,
                            "StartTime": i.start_time.timestamp_millis() as f64 / 1000.0,
                        });
                        if let Some(ref end) = i.end_time {
                            v["EndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
                        }
                        if let Some(ref eid) = i.execution_id {
                            v["ExecutionId"] = json!(eid);
                        }
                        v
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(json_resp(
            json!({ "WindowExecutionTaskInvocationIdentities": invocations }),
        ))
    }

    fn describe_maintenance_window_schedule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("WindowId", body["WindowId"].as_str(), 20, 20)?;
        validate_optional_enum(
            "ResourceType",
            body["ResourceType"].as_str(),
            &["INSTANCE", "RESOURCE_GROUP"],
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, i64::MAX)?;
        Ok(json_resp(json!({ "ScheduledWindowExecutions": [] })))
    }

    fn describe_maintenance_windows_for_target(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_enum(
            "ResourceType",
            body["ResourceType"].as_str(),
            &["INSTANCE", "RESOURCE_GROUP"],
        )?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, i64::MAX)?;
        let _resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?;
        let targets = body["Targets"]
            .as_array()
            .ok_or_else(|| missing("Targets"))?;

        // Extract instance IDs from targets
        let target_instance_ids: Vec<&str> = targets
            .iter()
            .filter(|t| t["Key"].as_str() == Some("InstanceIds"))
            .flat_map(|t| {
                t["Values"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>())
                    .unwrap_or_default()
            })
            .collect();

        let state = self.state.read();
        let windows: Vec<Value> = state
            .maintenance_windows
            .values()
            .filter(|mw| {
                if target_instance_ids.is_empty() {
                    return true;
                }
                mw.targets.iter().any(|t| {
                    t.targets.iter().any(|tgt| {
                        tgt["Key"].as_str() == Some("InstanceIds")
                            && tgt["Values"]
                                .as_array()
                                .map(|a| {
                                    a.iter().any(|v| {
                                        target_instance_ids.contains(&v.as_str().unwrap_or(""))
                                    })
                                })
                                .unwrap_or(false)
                    })
                })
            })
            .map(|mw| {
                json!({
                    "WindowId": mw.id,
                    "Name": mw.name,
                })
            })
            .collect();

        Ok(json_resp(json!({ "WindowIdentities": windows })))
    }

    fn cancel_maintenance_window_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let execution_id = body["WindowExecutionId"]
            .as_str()
            .ok_or_else(|| missing("WindowExecutionId"))?;

        let mut state = self.state.write();
        let exec = state
            .maintenance_window_executions
            .iter_mut()
            .find(|e| e.window_execution_id == execution_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "DoesNotExistException",
                    format!("Execution {execution_id} does not exist"),
                )
            })?;

        exec.status = "CANCELLING".to_string();

        Ok(json_resp(json!({ "WindowExecutionId": execution_id })))
    }

    // ── Patch Management Details ──────────────────────────────────

    fn update_patch_baseline(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;

        let mut state = self.state.write();
        let pb = state.patch_baselines.get_mut(baseline_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Patch baseline {baseline_id} does not exist"),
            )
        })?;

        if let Some(name) = body["Name"].as_str() {
            pb.name = name.to_string();
        }
        if body.get("Description").is_some() {
            pb.description = body["Description"].as_str().map(|s| s.to_string());
        }
        if let Some(rules) = body.get("ApprovalRules") {
            pb.approval_rules = Some(rules.clone());
        }
        if let Some(arr) = body["ApprovedPatches"].as_array() {
            pb.approved_patches = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(arr) = body["RejectedPatches"].as_array() {
            pb.rejected_patches = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(level) = body["ApprovedPatchesComplianceLevel"].as_str() {
            pb.approved_patches_compliance_level = level.to_string();
        }
        if let Some(action) = body["RejectedPatchesAction"].as_str() {
            pb.rejected_patches_action = action.to_string();
        }
        if let Some(gf) = body.get("GlobalFilters") {
            pb.global_filters = Some(gf.clone());
        }
        if let Some(arr) = body["Sources"].as_array() {
            pb.sources = arr.clone();
        }
        if let Some(enable) = body["ApprovedPatchesEnableNonSecurity"].as_bool() {
            pb.approved_patches_enable_non_security = enable;
        }

        let mut resp = json!({
            "BaselineId": pb.id,
            "Name": pb.name,
            "OperatingSystem": pb.operating_system,
            "ApprovedPatches": pb.approved_patches,
            "RejectedPatches": pb.rejected_patches,
            "ApprovedPatchesComplianceLevel": pb.approved_patches_compliance_level,
            "RejectedPatchesAction": pb.rejected_patches_action,
            "ApprovedPatchesEnableNonSecurity": pb.approved_patches_enable_non_security,
            "Sources": pb.sources,
        });
        if let Some(ref desc) = pb.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref rules) = pb.approval_rules {
            resp["ApprovalRules"] = rules.clone();
        }
        if let Some(ref gf) = pb.global_filters {
            resp["GlobalFilters"] = gf.clone();
        }

        Ok(json_resp(resp))
    }

    fn describe_instance_patch_states(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let _instance_ids = body["InstanceIds"]
            .as_array()
            .ok_or_else(|| missing("InstanceIds"))?;
        // Return empty - no real instances in emulator
        Ok(json_resp(json!({ "InstancePatchStates": [] })))
    }

    fn describe_instance_patch_states_for_patch_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("PatchGroup", body["PatchGroup"].as_str(), 1, 256)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let _patch_group = body["PatchGroup"]
            .as_str()
            .ok_or_else(|| missing("PatchGroup"))?;
        Ok(json_resp(json!({ "InstancePatchStates": [] })))
    }

    fn describe_instance_patches(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 10, 100)?;
        let _instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        Ok(json_resp(json!({ "Patches": [] })))
    }

    fn describe_effective_patches_for_patch_baseline(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("BaselineId", body["BaselineId"].as_str(), 20, 128)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 100)?;
        let _baseline_id = body["BaselineId"]
            .as_str()
            .ok_or_else(|| missing("BaselineId"))?;
        Ok(json_resp(json!({ "EffectivePatches": [] })))
    }

    fn get_deployable_patch_snapshot_for_instance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SnapshotId", body["SnapshotId"].as_str(), 36, 36)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let snapshot_id = body["SnapshotId"]
            .as_str()
            .ok_or_else(|| missing("SnapshotId"))?;

        Ok(json_resp(json!({
            "InstanceId": instance_id,
            "SnapshotId": snapshot_id,
            "Product": "{}",
            "SnapshotDownloadUrl": "",
        })))
    }

    // ── Resource Data Sync ────────────────────────────────────────

    fn create_resource_data_sync(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        let sync_name = body["SyncName"]
            .as_str()
            .ok_or_else(|| missing("SyncName"))?
            .to_string();

        let mut state = self.state.write();
        if state.resource_data_syncs.contains_key(&sync_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceDataSyncAlreadyExistsException",
                format!("Sync {sync_name} already exists"),
            ));
        }

        let now = Utc::now();
        let sync = ResourceDataSync {
            sync_name: sync_name.clone(),
            sync_type: body["SyncType"].as_str().map(|s| s.to_string()),
            sync_source: body.get("SyncSource").cloned(),
            s3_destination: body.get("S3Destination").cloned(),
            created_date: now,
            last_sync_time: None,
            last_successful_sync_time: None,
            last_status: "Successful".to_string(),
            sync_last_modified_time: now,
        };
        state.resource_data_syncs.insert(sync_name, sync);

        Ok(json_resp(json!({})))
    }

    fn delete_resource_data_sync(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        let sync_name = body["SyncName"]
            .as_str()
            .ok_or_else(|| missing("SyncName"))?;

        let mut state = self.state.write();
        if state.resource_data_syncs.remove(sync_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceDataSyncNotFoundException",
                format!("Sync {sync_name} not found"),
            ));
        }

        Ok(json_resp(json!({})))
    }

    fn list_resource_data_sync(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SyncType", body["SyncType"].as_str(), 1, 64)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let syncs: Vec<Value> = state
            .resource_data_syncs
            .values()
            .map(|s| {
                let mut v = json!({
                    "SyncName": s.sync_name,
                    "LastStatus": s.last_status,
                    "SyncCreatedTime": s.created_date.timestamp_millis() as f64 / 1000.0,
                    "LastSyncStatusMessage": "",
                    "SyncLastModifiedTime": s.sync_last_modified_time.timestamp_millis() as f64 / 1000.0,
                });
                if let Some(ref st) = s.sync_type {
                    v["SyncType"] = json!(st);
                }
                if let Some(ref src) = s.sync_source {
                    v["SyncSource"] = src.clone();
                }
                if let Some(ref dst) = s.s3_destination {
                    v["S3Destination"] = dst.clone();
                }
                if let Some(ref lst) = s.last_sync_time {
                    v["LastSyncTime"] = json!(lst.timestamp_millis() as f64 / 1000.0);
                }
                if let Some(ref lsst) = s.last_successful_sync_time {
                    v["LastSuccessfulSyncTime"] =
                        json!(lsst.timestamp_millis() as f64 / 1000.0);
                }
                v
            })
            .collect();
        Ok(json_resp(json!({ "ResourceDataSyncItems": syncs })))
    }

    fn update_resource_data_sync(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let sync_name = body["SyncName"]
            .as_str()
            .ok_or_else(|| missing("SyncName"))?;
        let _sync_type = body["SyncType"]
            .as_str()
            .ok_or_else(|| missing("SyncType"))?;
        let sync_source = body
            .get("SyncSource")
            .cloned()
            .ok_or_else(|| missing("SyncSource"))?;

        let mut state = self.state.write();
        let sync = state
            .resource_data_syncs
            .get_mut(sync_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceDataSyncNotFoundException",
                    format!("Sync {sync_name} not found"),
                )
            })?;
        sync.sync_source = Some(sync_source);
        sync.sync_last_modified_time = Utc::now();

        Ok(json_resp(json!({})))
    }

    // ── GetOpsSummary ─────────────────────────────────────────────

    fn get_ops_summary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        Ok(json_resp(json!({ "Entities": [] })))
    }

    // ── OpsItem Related Items ─────────────────────────────────────

    fn associate_ops_item_related_item(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?
            .to_string();
        let association_type = body["AssociationType"]
            .as_str()
            .ok_or_else(|| missing("AssociationType"))?
            .to_string();
        let resource_type = body["ResourceType"]
            .as_str()
            .ok_or_else(|| missing("ResourceType"))?
            .to_string();
        let resource_uri = body["ResourceUri"]
            .as_str()
            .ok_or_else(|| missing("ResourceUri"))?
            .to_string();

        let now = Utc::now();
        let mut state = self.state.write();

        // Verify ops item exists
        if !state.ops_items.contains_key(&ops_item_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsItemNotFoundException",
                format!("OpsItem ID {ops_item_id} not found"),
            ));
        }

        state.ops_item_related_item_counter += 1;
        let association_id = format!("oiri-{:012x}", state.ops_item_related_item_counter);
        let account_id = state.account_id.clone();

        state.ops_item_related_items.push(OpsItemRelatedItem {
            association_id: association_id.clone(),
            ops_item_id,
            association_type,
            resource_type,
            resource_uri,
            created_time: now,
            created_by: format!("arn:aws:iam::{account_id}:root"),
            last_modified_time: now,
            last_modified_by: format!("arn:aws:iam::{account_id}:root"),
        });

        Ok(json_resp(json!({ "AssociationId": association_id })))
    }

    fn disassociate_ops_item_related_item(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let ops_item_id = body["OpsItemId"]
            .as_str()
            .ok_or_else(|| missing("OpsItemId"))?;
        let association_id = body["AssociationId"]
            .as_str()
            .ok_or_else(|| missing("AssociationId"))?;

        let mut state = self.state.write();
        let before = state.ops_item_related_items.len();
        state
            .ops_item_related_items
            .retain(|ri| !(ri.ops_item_id == ops_item_id && ri.association_id == association_id));
        if state.ops_item_related_items.len() == before {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsItemRelatedItemAssociationNotFoundException",
                format!("Association {association_id} not found for OpsItem {ops_item_id}"),
            ));
        }

        Ok(json_resp(json!({})))
    }

    fn list_ops_item_related_items(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let ops_item_id = body["OpsItemId"].as_str();

        let items: Vec<Value> = state
            .ops_item_related_items
            .iter()
            .filter(|ri| ops_item_id.is_none_or(|id| ri.ops_item_id == id))
            .map(|ri| {
                json!({
                    "OpsItemId": ri.ops_item_id,
                    "AssociationId": ri.association_id,
                    "AssociationType": ri.association_type,
                    "ResourceType": ri.resource_type,
                    "ResourceUri": ri.resource_uri,
                    "CreatedTime": ri.created_time.timestamp(),
                    "CreatedBy": ri.created_by,
                    "LastModifiedTime": ri.last_modified_time.timestamp(),
                    "LastModifiedBy": ri.last_modified_by,
                })
            })
            .collect();

        Ok(json_resp(json!({ "Summaries": items })))
    }

    fn list_ops_item_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();

        // Filter by OpsItemId if provided in Filters
        let filter_id = body["Filters"].as_array().and_then(|filters| {
            filters.iter().find_map(|f| {
                if f["Key"].as_str() == Some("OpsItemId") {
                    f["Values"]
                        .as_array()
                        .and_then(|v| v.first())
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                }
            })
        });

        let events: Vec<Value> = state
            .ops_item_events
            .iter()
            .filter(|e| filter_id.as_ref().is_none_or(|id| e.ops_item_id == *id))
            .map(|e| {
                json!({
                    "OpsItemId": e.ops_item_id,
                    "EventId": e.event_id,
                    "Source": e.source,
                    "DetailType": e.detail_type,
                    "CreatedTime": e.created_time.timestamp_millis() as f64 / 1000.0,
                    "CreatedBy": e.created_by,
                })
            })
            .collect();

        Ok(json_resp(json!({ "Summaries": events })))
    }

    // ── OpsMetadata ───────────────────────────────────────────────

    fn create_ops_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ResourceId", body["ResourceId"].as_str(), 1, 1024)?;
        let resource_id = body["ResourceId"]
            .as_str()
            .ok_or_else(|| missing("ResourceId"))?
            .to_string();
        let metadata: HashMap<String, serde_json::Value> = body["Metadata"]
            .as_object()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let mut state = self.state.write();
        let arn = format!(
            "arn:aws:ssm:{}:{}:opsmetadata/{}",
            state.region, state.account_id, resource_id
        );

        if state.ops_metadata.contains_key(&arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsMetadataAlreadyExistsException",
                format!("OpsMetadata for {resource_id} already exists"),
            ));
        }

        let entry = OpsMetadataEntry {
            ops_metadata_arn: arn.clone(),
            resource_id,
            metadata,
            creation_date: Utc::now(),
        };
        state.ops_metadata.insert(arn.clone(), entry);

        Ok(json_resp(json!({ "OpsMetadataArn": arn })))
    }

    fn get_ops_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["OpsMetadataArn"]
            .as_str()
            .ok_or_else(|| missing("OpsMetadataArn"))?;

        let state = self.state.read();
        let entry = state.ops_metadata.get(arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsMetadataNotFoundException",
                format!("OpsMetadata {arn} not found"),
            )
        })?;

        Ok(json_resp(json!({
            "ResourceId": entry.resource_id,
            "Metadata": entry.metadata,
        })))
    }

    fn update_ops_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["OpsMetadataArn"]
            .as_str()
            .ok_or_else(|| missing("OpsMetadataArn"))?;

        let mut state = self.state.write();
        let entry = state.ops_metadata.get_mut(arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsMetadataNotFoundException",
                format!("OpsMetadata {arn} not found"),
            )
        })?;

        if let Some(to_add) = body["MetadataToUpdate"].as_object() {
            for (k, v) in to_add {
                entry.metadata.insert(k.clone(), v.clone());
            }
        }
        if let Some(to_del) = body["KeysToDelete"].as_array() {
            for k in to_del {
                if let Some(key) = k.as_str() {
                    entry.metadata.remove(key);
                }
            }
        }

        Ok(json_resp(json!({ "OpsMetadataArn": arn })))
    }

    fn delete_ops_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["OpsMetadataArn"]
            .as_str()
            .ok_or_else(|| missing("OpsMetadataArn"))?;

        let mut state = self.state.write();
        if state.ops_metadata.remove(arn).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "OpsMetadataNotFoundException",
                format!("OpsMetadata {arn} not found"),
            ));
        }

        Ok(json_resp(json!({})))
    }

    fn list_ops_metadata(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let items: Vec<Value> = state
            .ops_metadata
            .values()
            .map(|e| {
                json!({
                    "OpsMetadataArn": e.ops_metadata_arn,
                    "ResourceId": e.resource_id,
                    "CreationDate": e.creation_date.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();

        Ok(json_resp(json!({ "OpsMetadataList": items })))
    }

    // ── Automation ────────────────────────────────────────────────

    fn start_automation_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ClientToken", body["ClientToken"].as_str(), 36, 36)?;
        validate_optional_string_length("MaxConcurrency", body["MaxConcurrency"].as_str(), 1, 7)?;
        validate_optional_string_length("MaxErrors", body["MaxErrors"].as_str(), 1, 7)?;
        validate_optional_enum("Mode", body["Mode"].as_str(), &["Auto", "Interactive"])?;
        validate_optional_string_length(
            "TargetParameterName",
            body["TargetParameterName"].as_str(),
            1,
            50,
        )?;
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();
        let document_version = body["DocumentVersion"].as_str().map(|s| s.to_string());
        let parameters: HashMap<String, Vec<String>> = body["Parameters"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let vals = v
                            .as_array()
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|i| i.as_str().map(|s| s.to_string()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        (k.clone(), vals)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mode = body["Mode"].as_str().unwrap_or("Auto").to_string();
        let target = body["TargetParameterName"].as_str().map(|s| s.to_string());
        let targets: Vec<serde_json::Value> =
            body["Targets"].as_array().cloned().unwrap_or_default();
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());

        let now = Utc::now();
        let mut state = self.state.write();
        state.automation_execution_counter += 1;
        let exec_id = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            state.automation_execution_counter, 0, 0, 0, state.automation_execution_counter
        );
        let account_id = state.account_id.clone();

        let execution = AutomationExecution {
            automation_execution_id: exec_id.clone(),
            document_name,
            document_version,
            automation_execution_status: "InProgress".to_string(),
            execution_start_time: now,
            execution_end_time: None,
            parameters,
            outputs: HashMap::new(),
            mode,
            target,
            targets,
            max_concurrency,
            max_errors,
            executed_by: format!("arn:aws:iam::{account_id}:root"),
            step_executions: Vec::new(),
            automation_subtype: None,
            runbooks: Vec::new(),
            change_request_name: None,
            scheduled_time: None,
        };

        state
            .automation_executions
            .insert(exec_id.clone(), execution);

        Ok(json_resp(json!({ "AutomationExecutionId": exec_id })))
    }

    fn stop_automation_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let exec_id = body["AutomationExecutionId"]
            .as_str()
            .ok_or_else(|| missing("AutomationExecutionId"))?;

        let mut state = self.state.write();
        let exec = state
            .automation_executions
            .get_mut(exec_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AutomationExecutionNotFoundException",
                    format!("Automation execution {exec_id} not found"),
                )
            })?;

        exec.automation_execution_status = "Cancelled".to_string();
        exec.execution_end_time = Some(Utc::now());

        Ok(json_resp(json!({})))
    }

    fn get_automation_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let exec_id = body["AutomationExecutionId"]
            .as_str()
            .ok_or_else(|| missing("AutomationExecutionId"))?;

        let state = self.state.read();
        let exec = state.automation_executions.get(exec_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AutomationExecutionNotFoundException",
                format!("Automation execution {exec_id} not found"),
            )
        })?;

        Ok(json_resp(
            json!({ "AutomationExecution": automation_execution_to_json(exec) }),
        ))
    }

    fn describe_automation_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let items: Vec<Value> = state
            .automation_executions
            .values()
            .map(|e| {
                json!({
                    "AutomationExecutionId": e.automation_execution_id,
                    "DocumentName": e.document_name,
                    "AutomationExecutionStatus": e.automation_execution_status,
                    "ExecutionStartTime": e.execution_start_time.timestamp_millis() as f64 / 1000.0,
                    "ExecutedBy": e.executed_by,
                    "Mode": e.mode,
                    "Targets": e.targets,
                })
            })
            .collect();

        Ok(json_resp(
            json!({ "AutomationExecutionMetadataList": items }),
        ))
    }

    fn describe_automation_step_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let exec_id = body["AutomationExecutionId"]
            .as_str()
            .ok_or_else(|| missing("AutomationExecutionId"))?;

        let state = self.state.read();
        let exec = state.automation_executions.get(exec_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AutomationExecutionNotFoundException",
                format!("Automation execution {exec_id} not found"),
            )
        })?;

        let steps: Vec<Value> = exec
            .step_executions
            .iter()
            .map(|s| {
                json!({
                    "StepName": s.step_name,
                    "Action": s.action,
                    "StepStatus": s.step_status,
                    "StepExecutionId": s.step_execution_id,
                    "Inputs": s.inputs,
                    "Outputs": s.outputs,
                })
            })
            .collect();

        Ok(json_resp(json!({ "StepExecutions": steps })))
    }

    fn send_automation_signal(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let exec_id = body["AutomationExecutionId"]
            .as_str()
            .ok_or_else(|| missing("AutomationExecutionId"))?;
        let _signal_type = body["SignalType"]
            .as_str()
            .ok_or_else(|| missing("SignalType"))?;

        let state = self.state.read();
        if !state.automation_executions.contains_key(exec_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AutomationExecutionNotFoundException",
                format!("Automation execution {exec_id} not found"),
            ));
        }

        Ok(json_resp(json!({})))
    }

    fn start_change_request_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ClientToken", body["ClientToken"].as_str(), 36, 36)?;
        validate_optional_string_length(
            "ChangeRequestName",
            body["ChangeRequestName"].as_str(),
            1,
            1024,
        )?;
        validate_optional_string_length("ChangeDetails", body["ChangeDetails"].as_str(), 1, 32768)?;
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();
        let _runbooks = body["Runbooks"]
            .as_array()
            .ok_or_else(|| missing("Runbooks"))?;
        let change_request_name = body["ChangeRequestName"].as_str().map(|s| s.to_string());
        let runbooks: Vec<serde_json::Value> =
            body["Runbooks"].as_array().cloned().unwrap_or_default();

        let now = Utc::now();
        let mut state = self.state.write();
        state.automation_execution_counter += 1;
        let exec_id = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            state.automation_execution_counter, 0, 0, 0, state.automation_execution_counter
        );
        let account_id = state.account_id.clone();

        let execution = AutomationExecution {
            automation_execution_id: exec_id.clone(),
            document_name,
            document_version: None,
            automation_execution_status: "Pending".to_string(),
            execution_start_time: now,
            execution_end_time: None,
            parameters: HashMap::new(),
            outputs: HashMap::new(),
            mode: "Auto".to_string(),
            target: None,
            targets: Vec::new(),
            max_concurrency: None,
            max_errors: None,
            executed_by: format!("arn:aws:iam::{account_id}:root"),
            step_executions: Vec::new(),
            automation_subtype: Some("ChangeRequest".to_string()),
            runbooks,
            change_request_name,
            scheduled_time: None,
        };

        state
            .automation_executions
            .insert(exec_id.clone(), execution);

        Ok(json_resp(json!({ "AutomationExecutionId": exec_id })))
    }

    fn start_execution_preview(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();

        let now = Utc::now();
        let mut state = self.state.write();
        state.execution_preview_counter += 1;
        let preview_id = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            state.execution_preview_counter, 0, 0, 0, state.execution_preview_counter
        );

        let preview = ExecutionPreview {
            execution_preview_id: preview_id.clone(),
            document_name,
            status: "Success".to_string(),
            created_time: now,
        };
        state.execution_previews.insert(preview_id.clone(), preview);

        Ok(json_resp(json!({ "ExecutionPreviewId": preview_id })))
    }

    fn get_execution_preview(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let preview_id = body["ExecutionPreviewId"]
            .as_str()
            .ok_or_else(|| missing("ExecutionPreviewId"))?;

        let state = self.state.read();
        let preview = state.execution_previews.get(preview_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Execution preview {preview_id} not found"),
            )
        })?;

        Ok(json_resp(json!({
            "ExecutionPreviewId": preview.execution_preview_id,
            "Status": preview.status,
            "EndedAt": preview.created_time.timestamp_millis() as f64 / 1000.0,
        })))
    }

    // ── Sessions ──────────────────────────────────────────────────

    fn start_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("Target", body["Target"].as_str(), 1, 400)?;
        validate_optional_string_length("Reason", body["Reason"].as_str(), 1, 256)?;
        let target = body["Target"]
            .as_str()
            .ok_or_else(|| missing("Target"))?
            .to_string();
        let reason = body["Reason"].as_str().map(|s| s.to_string());

        let now = Utc::now();
        let mut state = self.state.write();
        state.session_counter += 1;
        let session_id = format!("session-{:012x}", state.session_counter);
        let account_id = state.account_id.clone();

        let session = SsmSession {
            session_id: session_id.clone(),
            target: target.clone(),
            status: "Connected".to_string(),
            start_date: now,
            end_date: None,
            owner: format!("arn:aws:iam::{account_id}:root"),
            reason,
        };
        state.sessions.insert(session_id.clone(), session);

        Ok(json_resp(json!({
            "SessionId": session_id,
            "TokenValue": format!("token-{session_id}"),
            "StreamUrl": format!("wss://ssm.us-east-1.amazonaws.com/session/{session_id}"),
        })))
    }

    fn resume_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let session_id = body["SessionId"]
            .as_str()
            .ok_or_else(|| missing("SessionId"))?;

        let state = self.state.read();
        let session = state.sessions.get(session_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DoesNotExistException",
                format!("Session {session_id} not found"),
            )
        })?;

        Ok(json_resp(json!({
            "SessionId": session.session_id,
            "TokenValue": format!("token-{}", session.session_id),
            "StreamUrl": format!("wss://ssm.us-east-1.amazonaws.com/session/{}", session.session_id),
        })))
    }

    fn terminate_session(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SessionId", body["SessionId"].as_str(), 1, 96)?;
        let session_id = body["SessionId"]
            .as_str()
            .ok_or_else(|| missing("SessionId"))?;

        let mut state = self.state.write();
        if let Some(session) = state.sessions.get_mut(session_id) {
            session.status = "Terminated".to_string();
            session.end_date = Some(Utc::now());
        }
        // AWS TerminateSession doesn't error on non-existent sessions

        Ok(json_resp(json!({ "SessionId": session_id })))
    }

    fn describe_sessions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_enum("State", body["State"].as_str(), &["Active", "History"])?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 200)?;
        let state_filter = body["State"].as_str().ok_or_else(|| missing("State"))?;

        let state = self.state.read();
        let sessions: Vec<Value> = state
            .sessions
            .values()
            .filter(|s| match state_filter {
                "Active" => s.status == "Connected",
                "History" => s.status == "Terminated",
                _ => true,
            })
            .map(|s| {
                let mut v = json!({
                    "SessionId": s.session_id,
                    "Target": s.target,
                    "Status": s.status,
                    "StartDate": s.start_date.timestamp_millis() as f64 / 1000.0,
                    "Owner": s.owner,
                });
                if let Some(ref end) = s.end_date {
                    v["EndDate"] = json!(end.timestamp_millis() as f64 / 1000.0);
                }
                if let Some(ref reason) = s.reason {
                    v["Reason"] = json!(reason);
                }
                v
            })
            .collect();

        Ok(json_resp(json!({ "Sessions": sessions })))
    }

    fn start_access_request(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("Reason", body["Reason"].as_str(), 1, 256)?;
        let _reason = body["Reason"].as_str().ok_or_else(|| missing("Reason"))?;
        let _targets = body["Targets"]
            .as_array()
            .ok_or_else(|| missing("Targets"))?;

        let mut state = self.state.write();
        state.session_counter += 1;
        let access_request_id = format!("ar-{:012x}", state.session_counter);

        Ok(json_resp(json!({ "AccessRequestId": access_request_id })))
    }

    fn get_access_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let _access_request_id = body["AccessRequestId"]
            .as_str()
            .ok_or_else(|| missing("AccessRequestId"))?;

        Ok(json_resp(json!({
            "AccessRequestStatus": "Approved",
            "Credentials": {
                "AccessKeyId": "AKIAIOSFODNN7EXAMPLE",
                "SecretAccessKey": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                "SessionToken": "FwoGZXIvYXdzEA...",
                "ExpirationTime": Utc::now().timestamp_millis() as f64 / 1000.0 + 3600.0,
            },
        })))
    }

    // ── Managed Instances ─────────────────────────────────────────

    fn create_activation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("IamRole", body["IamRole"].as_str(), 0, 64)?;
        validate_optional_string_length("Description", body["Description"].as_str(), 0, 256)?;
        validate_optional_string_length(
            "DefaultInstanceName",
            body["DefaultInstanceName"].as_str(),
            0,
            256,
        )?;
        validate_optional_range_i64(
            "RegistrationLimit",
            body["RegistrationLimit"].as_i64(),
            1,
            1000,
        )?;
        let iam_role = body["IamRole"]
            .as_str()
            .ok_or_else(|| missing("IamRole"))?
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let default_instance_name = body["DefaultInstanceName"].as_str().map(|s| s.to_string());
        let registration_limit = body["RegistrationLimit"].as_i64().unwrap_or(1);
        let tags: HashMap<String, String> = body["Tags"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t["Key"].as_str()?;
                        let v = t["Value"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let now = Utc::now();
        let mut state = self.state.write();
        state.activation_counter += 1;
        let activation_id = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            state.activation_counter, 0, 0, 0, state.activation_counter
        );
        let activation_code = format!("code-{}", activation_id);

        let activation = SsmActivation {
            activation_id: activation_id.clone(),
            iam_role,
            registration_limit,
            registrations_count: 0,
            expiration_date: None,
            description,
            default_instance_name,
            created_date: now,
            expired: false,
            tags,
        };
        state.activations.insert(activation_id.clone(), activation);

        Ok(json_resp(json!({
            "ActivationId": activation_id,
            "ActivationCode": activation_code,
        })))
    }

    fn delete_activation(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let activation_id = body["ActivationId"]
            .as_str()
            .ok_or_else(|| missing("ActivationId"))?;

        let mut state = self.state.write();
        if state.activations.remove(activation_id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidActivationId",
                format!("Activation ID {activation_id} not found"),
            ));
        }

        Ok(json_resp(json!({})))
    }

    fn describe_activations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let activations: Vec<Value> = state
            .activations
            .values()
            .map(|a| {
                let mut v = json!({
                    "ActivationId": a.activation_id,
                    "IamRole": a.iam_role,
                    "RegistrationLimit": a.registration_limit,
                    "RegistrationsCount": a.registrations_count,
                    "CreatedDate": a.created_date.timestamp_millis() as f64 / 1000.0,
                    "Expired": a.expired,
                });
                if let Some(ref d) = a.description {
                    v["Description"] = json!(d);
                }
                if let Some(ref n) = a.default_instance_name {
                    v["DefaultInstanceName"] = json!(n);
                }
                if let Some(ref e) = a.expiration_date {
                    v["ExpirationDate"] = json!(e.timestamp_millis() as f64 / 1000.0);
                }
                if !a.tags.is_empty() {
                    v["Tags"] = json!(a
                        .tags
                        .iter()
                        .map(|(k, v)| json!({"Key": k, "Value": v}))
                        .collect::<Vec<_>>());
                }
                v
            })
            .collect();

        Ok(json_resp(json!({ "ActivationList": activations })))
    }

    fn deregister_managed_instance(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("InstanceId", body["InstanceId"].as_str(), 20, 124)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;

        let mut state = self.state.write();
        state.managed_instances.remove(instance_id);
        // AWS doesn't error on non-existent instances

        Ok(json_resp(json!({})))
    }

    fn describe_instance_information(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 5, 50)?;
        let state = self.state.read();
        let instances: Vec<Value> = state
            .managed_instances
            .values()
            .map(|i| {
                json!({
                    "InstanceId": i.instance_id,
                    "PingStatus": i.ping_status,
                    "LastPingDateTime": i.last_ping_date_time.timestamp_millis() as f64 / 1000.0,
                    "AgentVersion": i.agent_version,
                    "IsLatestVersion": i.is_latest_version,
                    "PlatformType": i.platform_type,
                    "PlatformName": i.platform_name,
                    "PlatformVersion": i.platform_version,
                    "ResourceType": i.resource_type,
                    "IPAddress": i.ip_address,
                    "ComputerName": i.computer_name,
                    "IamRole": i.iam_role,
                    "RegistrationDate": i.registration_date.timestamp_millis() as f64 / 1000.0,
                })
            })
            .collect();

        Ok(json_resp(json!({ "InstanceInformationList": instances })))
    }

    fn describe_instance_properties(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 5, 1000)?;
        let state = self.state.read();
        let instances: Vec<Value> = state
            .managed_instances
            .values()
            .map(|i| {
                json!({
                    "InstanceId": i.instance_id,
                    "PingStatus": i.ping_status,
                    "LastPingDateTime": i.last_ping_date_time.timestamp_millis() as f64 / 1000.0,
                    "AgentVersion": i.agent_version,
                    "PlatformType": i.platform_type,
                    "PlatformName": i.platform_name,
                    "PlatformVersion": i.platform_version,
                    "ResourceType": i.resource_type,
                    "IPAddress": i.ip_address,
                    "ComputerName": i.computer_name,
                })
            })
            .collect();

        Ok(json_resp(json!({ "InstanceProperties": instances })))
    }

    fn update_managed_instance_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;
        let iam_role = body["IamRole"]
            .as_str()
            .ok_or_else(|| missing("IamRole"))?
            .to_string();

        let mut state = self.state.write();
        let instance = state
            .managed_instances
            .get_mut(instance_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInstanceId",
                    format!("Instance {instance_id} not found"),
                )
            })?;
        instance.iam_role = iam_role;

        Ok(json_resp(json!({})))
    }

    // ── Other ─────────────────────────────────────────────────────

    fn list_nodes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        Ok(json_resp(json!({ "Nodes": [] })))
    }

    fn list_nodes_summary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let _aggregators = body["Aggregators"]
            .as_array()
            .ok_or_else(|| missing("Aggregators"))?;
        Ok(json_resp(json!({ "Summary": [] })))
    }

    fn describe_effective_instance_associations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 5)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;

        let state = self.state.read();
        let associations: Vec<Value> = state
            .associations
            .values()
            .filter(|a| {
                // Match by direct instance_id or by targets containing the instance
                a.instance_id.as_deref() == Some(instance_id)
                    || a.targets.iter().any(|t| {
                        t["Key"].as_str() == Some("InstanceIds")
                            && t["Values"].as_array().is_some_and(|vals| {
                                vals.iter().any(|v| v.as_str() == Some(instance_id))
                            })
                    })
            })
            .map(|a| {
                json!({
                    "AssociationId": a.association_id,
                    "InstanceId": instance_id,
                    "Content": a.name,
                    "AssociationVersion": a.versions.len().to_string(),
                })
            })
            .collect();

        Ok(json_resp(json!({ "Associations": associations })))
    }

    fn describe_instance_associations_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let instance_id = body["InstanceId"]
            .as_str()
            .ok_or_else(|| missing("InstanceId"))?;

        let state = self.state.read();
        let statuses: Vec<Value> = state
            .associations
            .values()
            .filter(|a| {
                a.instance_id.as_deref() == Some(instance_id)
                    || a.targets.iter().any(|t| {
                        t["Key"].as_str() == Some("InstanceIds")
                            && t["Values"].as_array().is_some_and(|vals| {
                                vals.iter().any(|v| v.as_str() == Some(instance_id))
                            })
                    })
            })
            .map(|a| {
                json!({
                    "AssociationId": a.association_id,
                    "Name": a.name,
                    "InstanceId": instance_id,
                    "AssociationVersion": a.versions.len().to_string(),
                    "ExecutionDate": a.status_date.timestamp_millis() as f64 / 1000.0,
                    "Status": a.status,
                    "DetailedStatus": a.status,
                    "ExecutionSummary": format!("1 out of 1 plugin processed, 1 success"),
                })
            })
            .collect();

        Ok(json_resp(
            json!({ "InstanceAssociationStatusInfos": statuses }),
        ))
    }
}

fn automation_execution_to_json(e: &AutomationExecution) -> Value {
    let mut v = json!({
        "AutomationExecutionId": e.automation_execution_id,
        "DocumentName": e.document_name,
        "AutomationExecutionStatus": e.automation_execution_status,
        "ExecutionStartTime": e.execution_start_time.timestamp_millis() as f64 / 1000.0,
        "ExecutedBy": e.executed_by,
        "Mode": e.mode,
        "Parameters": e.parameters,
        "Outputs": e.outputs,
        "Targets": e.targets,
        "StepExecutions": e.step_executions.iter().map(|s| json!({
            "StepName": s.step_name,
            "Action": s.action,
            "StepStatus": s.step_status,
            "StepExecutionId": s.step_execution_id,
            "Inputs": s.inputs,
            "Outputs": s.outputs,
        })).collect::<Vec<Value>>(),
    });
    if let Some(ref dv) = e.document_version {
        v["DocumentVersion"] = json!(dv);
    }
    if let Some(ref end) = e.execution_end_time {
        v["ExecutionEndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
    }
    if let Some(ref target) = e.target {
        v["TargetParameterName"] = json!(target);
    }
    if let Some(ref mc) = e.max_concurrency {
        v["MaxConcurrency"] = json!(mc);
    }
    if let Some(ref me) = e.max_errors {
        v["MaxErrors"] = json!(me);
    }
    if let Some(ref subtype) = e.automation_subtype {
        v["AutomationSubtype"] = json!(subtype);
    }
    if !e.runbooks.is_empty() {
        v["Runbooks"] = json!(e.runbooks);
    }
    if let Some(ref crn) = e.change_request_name {
        v["ChangeRequestName"] = json!(crn);
    }
    v
}

fn association_to_json(a: &SsmAssociation) -> Value {
    let mut v = json!({
        "AssociationId": a.association_id,
        "Name": a.name,
        "AssociationVersion": a.versions.len().to_string(),
        "Date": a.created_date.timestamp_millis() as f64 / 1000.0,
        "LastUpdateAssociationDate": a.last_update_association_date.timestamp_millis() as f64 / 1000.0,
        "Status": {
            "Date": a.status_date.timestamp_millis() as f64 / 1000.0,
            "Name": a.status,
            "Message": "",
            "AdditionalInfo": "",
        },
        "Overview": a.overview,
        "ApplyOnlyAtCronInterval": a.apply_only_at_cron_interval,
    });
    if !a.targets.is_empty() {
        v["Targets"] = json!(a.targets);
    }
    if let Some(ref s) = a.schedule_expression {
        v["ScheduleExpression"] = json!(s);
    }
    if !a.parameters.is_empty() {
        v["Parameters"] = json!(a.parameters);
    }
    if let Some(ref an) = a.association_name {
        v["AssociationName"] = json!(an);
    }
    if let Some(ref dv) = a.document_version {
        v["DocumentVersion"] = json!(dv);
    }
    if let Some(ref ol) = a.output_location {
        v["OutputLocation"] = ol.clone();
    }
    if let Some(ref me) = a.max_errors {
        v["MaxErrors"] = json!(me);
    }
    if let Some(ref mc) = a.max_concurrency {
        v["MaxConcurrency"] = json!(mc);
    }
    if let Some(ref cs) = a.compliance_severity {
        v["ComplianceSeverity"] = json!(cs);
    }
    if let Some(ref sc) = a.sync_compliance {
        v["SyncCompliance"] = json!(sc);
    }
    if let Some(ref iid) = a.instance_id {
        v["InstanceId"] = json!(iid);
    }
    if let Some(so) = a.schedule_offset {
        v["ScheduleOffset"] = json!(so);
    }
    if let Some(ref led) = a.last_execution_date {
        v["LastExecutionDate"] = json!(led.timestamp_millis() as f64 / 1000.0);
    }
    v
}

fn ops_item_to_json(item: &SsmOpsItem) -> Value {
    json!({
        "OpsItemId": item.ops_item_id,
        "Title": item.title,
        "Description": item.description,
        "Source": item.source,
        "Status": item.status,
        "Priority": item.priority,
        "Severity": item.severity,
        "Category": item.category,
        "OperationalData": item.operational_data,
        "Notifications": item.notifications,
        "RelatedOpsItems": item.related_ops_items,
        "CreatedTime": item.created_time.timestamp_millis() as f64 / 1000.0,
        "LastModifiedTime": item.last_modified_time.timestamp_millis() as f64 / 1000.0,
        "CreatedBy": item.created_by,
        "LastModifiedBy": item.last_modified_by,
        "OpsItemType": item.ops_item_type,
    })
}

fn get_default_service_setting(setting_id: &str) -> String {
    match setting_id {
        s if s.contains("parameter-store") && s.contains("throughput") => "standard".to_string(),
        s if s.contains("parameter-store") && s.contains("high-throughput") => "false".to_string(),
        s if s.contains("session-manager") => "".to_string(),
        s if s.contains("managed-instance") => "".to_string(),
        _ => "".to_string(),
    }
}

/// Validate a path value for parameter path filters.
fn is_valid_param_path(path: &str) -> bool {
    if !path.starts_with('/') {
        return false;
    }
    if path == "//" {
        return false;
    }
    // Each segment between slashes must contain only letters, numbers, . - _
    let segments: Vec<&str> = path.split('/').collect();
    for seg in &segments[1..] {
        if seg.is_empty() {
            continue;
        }
        if !seg
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
        {
            return false;
        }
    }
    true
}

/// Full invalid-path error message (matches AWS format).
fn invalid_path_error(value: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!(
            "The parameter doesn't meet the parameter name requirements. \
             The parameter name must begin with a forward slash \"/\". \
             It can't be prefixed with \"aws\" or \"ssm\" (case-insensitive). \
             It must use only letters, numbers, or the following symbols: . \
             (period), - (hyphen), _ (underscore). \
             Special characters are not allowed. All sub-paths, if specified, \
             must use the forward slash symbol \"/\". \
             Valid example: /get/parameters2-/by1./path0_. \
             Invalid parameter name: {value}"
        ),
    )
}

/// Validate ParameterFilters for DescribeParameters.
fn validate_parameter_filters(filters: &[Value]) -> Result<(), AwsServiceError> {
    let valid_keys = ["Path", "Name", "Type", "KeyId", "Tier"];
    let valid_key_pattern = "tag:.+|Name|Type|KeyId|Path|Label|Tier";

    // Collect structural validation errors first
    let mut errors: Vec<String> = Vec::new();

    for (i, filter) in filters.iter().enumerate() {
        let key = filter["Key"].as_str().unwrap_or("");
        let option = filter["Option"].as_str();
        let values = filter["Values"].as_array();

        // Key must match pattern
        let key_valid = valid_keys.contains(&key) || key.starts_with("tag:") || key == "Label";
        if !key_valid {
            errors.push(format!(
                "Value '{}' at 'parameterFilters.{}.key' failed to satisfy constraint: \
                 Member must satisfy regular expression pattern: {}",
                key,
                i + 1,
                valid_key_pattern
            ));
        }

        // Key length <= 132
        if key.len() > 132 {
            errors.push(format!(
                "Value '{}' at 'parameterFilters.{}.key' failed to satisfy constraint: \
                 Member must have length less than or equal to 132",
                key,
                i + 1
            ));
        }

        // Option length <= 10
        if let Some(opt) = option {
            if opt.len() > 10 {
                errors.push(format!(
                    "Value '{}' at 'parameterFilters.{}.option' failed to satisfy constraint: \
                     Member must have length less than or equal to 10",
                    opt,
                    i + 1
                ));
            }
        }

        // Values length <= 50
        if let Some(vals) = values {
            if vals.len() > 50 {
                let vals_str: Vec<&str> = vals.iter().filter_map(|v| v.as_str()).collect();
                errors.push(format!(
                    "Value '[{}]' at 'parameterFilters.{}.values' failed to satisfy constraint: \
                     Member must have length less than or equal to 50",
                    vals_str.join(", "),
                    i + 1
                ));
            }
            // Each value <= 1024
            for val in vals {
                if let Some(v) = val.as_str() {
                    if v.len() > 1024 {
                        errors.push(format!(
                            "Value '[{}]' at 'parameterFilters.{}.values' failed to satisfy constraint: \
                             Member must have length less than or equal to 1024, \
                             Member must have length greater than or equal to 1",
                            v,
                            i + 1
                        ));
                    }
                }
            }
        }
    }

    if !errors.is_empty() {
        let msg = if errors.len() == 1 {
            format!("1 validation error detected: {}", errors[0])
        } else {
            format!(
                "{} validation errors detected: {}",
                errors.len(),
                errors.join("; ")
            )
        };
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            msg,
        ));
    }

    // Semantic validation (after structural validation passes)

    // Label is not valid for DescribeParameters
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if key == "Label" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "The following filter key is not valid: Label. \
                 Valid filter keys include: [Path, Name, Type, KeyId, Tier]",
            ));
        }
    }

    // Check for missing values (tag: filters are allowed without values - means "tag exists")
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        let values = filter["Values"].as_array();
        if !key.starts_with("tag:") && (values.is_none() || values.is_some_and(|v| v.is_empty())) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("The following filter values are missing : null for filter key {key}"),
            ));
        }
    }

    // Check for duplicate keys
    let mut seen_keys = std::collections::HashSet::new();
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if !seen_keys.insert(key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following filter is duplicated in the request: {key}. \
                     A request can contain only one occurrence of a specific filter."
                ),
            ));
        }
    }

    // Validate per-key constraints
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        let option = filter["Option"].as_str();
        let values = filter["Values"].as_array();

        if key == "Path" {
            // Path option must be Recursive or OneLevel, not Equals
            if let Some(opt) = option {
                if opt != "Recursive" && opt != "OneLevel" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!(
                            "The following filter option is not valid: {opt}. \
                             Valid options include: [Recursive, OneLevel]"
                        ),
                    ));
                }
            }

            // Path values can't start with aws or ssm
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !is_valid_param_path(v) {
                            return Err(invalid_path_error(v));
                        }
                        let stripped = v.strip_prefix('/').unwrap_or(v);
                        let first_segment = stripped.split('/').next().unwrap_or("");
                        let lower = first_segment.to_lowercase();
                        if lower.starts_with("aws") || lower.starts_with("ssm") {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                "Filters for common parameters can't be prefixed with \
                                 \"aws\" or \"ssm\" (case-insensitive).",
                            ));
                        }
                    }
                }
            }
        }

        if key == "Tier" {
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !["Standard", "Advanced", "Intelligent-Tiering"].contains(&v) {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                format!(
                                    "The following filter value is not valid: {v}. Valid \
                                     values include: [Standard, Advanced, Intelligent-Tiering]"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        if key == "Type" {
            if let Some(vals) = values {
                for val in vals {
                    if let Some(v) = val.as_str() {
                        if !["String", "StringList", "SecureString"].contains(&v) {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                format!(
                                    "The following filter value is not valid: {v}. Valid \
                                     values include: [String, StringList, SecureString]"
                                ),
                            ));
                        }
                    }
                }
            }
        }

        if key == "Name" {
            if let Some(opt) = option {
                if !["BeginsWith", "Equals", "Contains"].contains(&opt) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!(
                            "The following filter option is not valid: {opt}. Valid \
                             options include: [BeginsWith, Equals]."
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}

/// Validate ParameterFilters for GetParametersByPath (only Type, KeyId, Label, tag:* allowed).
fn validate_parameter_filters_by_path(filters: &[Value]) -> Result<(), AwsServiceError> {
    for filter in filters {
        let key = filter["Key"].as_str().unwrap_or("");
        if !["Type", "KeyId", "Label"].contains(&key) && !key.starts_with("tag:") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "The following filter key is not valid: {key}. \
                     Valid filter keys include: [Type, KeyId]."
                ),
            ));
        }
    }
    Ok(())
}

/// Apply ParameterFilters to a parameter.
fn apply_parameter_filters(param: &SsmParameter, filters: Option<&Vec<Value>>) -> bool {
    let filters = match filters {
        Some(f) => f,
        None => return true,
    };

    for filter in filters {
        let key = match filter["Key"].as_str() {
            Some(k) => k,
            None => continue,
        };
        let option = filter["Option"].as_str().unwrap_or("Equals");
        let values: Vec<&str> = filter["Values"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let matches = match key {
            "Name" => match option {
                "BeginsWith" => values.iter().any(|v| {
                    param.name.starts_with(v) || {
                        // Normalize: /foo matches foo, foo matches /foo
                        let normalized_v = v.strip_prefix('/').unwrap_or(v);
                        let normalized_name = param.name.strip_prefix('/').unwrap_or(&param.name);
                        normalized_name.starts_with(normalized_v)
                    }
                }),
                "Contains" => {
                    // Normalize name to always have leading /
                    let what = if param.name.starts_with('/') {
                        param.name.clone()
                    } else {
                        format!("/{}", param.name)
                    };
                    // Values NOT normalized for Contains (unlike Equals/BeginsWith)
                    values.iter().any(|v| what.contains(v))
                }
                "Equals" => values.iter().any(|v| {
                    param.name == *v || {
                        // Normalize: /foo matches foo, foo matches /foo
                        let normalized_v = v.strip_prefix('/').unwrap_or(v);
                        let normalized_name = param.name.strip_prefix('/').unwrap_or(&param.name);
                        normalized_name == normalized_v
                    }
                }),
                _ => true,
            },
            "Path" => {
                // Default option for Path is OneLevel
                let path_option = if option == "Equals" {
                    "OneLevel"
                } else {
                    option
                };
                match path_option {
                    "Recursive" => values.iter().any(|v| {
                        if *v == "/" {
                            true // All params are under root
                        } else {
                            let prefix = if v.ends_with('/') {
                                v.to_string()
                            } else {
                                format!("{v}/")
                            };
                            param.name.starts_with(&prefix)
                        }
                    }),
                    _ => values.iter().any(|v| {
                        if *v == "/" {
                            // Root level: no-slash params or single-level /params
                            if param.name.starts_with('/') {
                                !param.name[1..].contains('/')
                            } else {
                                !param.name.contains('/')
                            }
                        } else {
                            let prefix = if v.ends_with('/') {
                                v.to_string()
                            } else {
                                format!("{v}/")
                            };
                            param.name.starts_with(&prefix)
                                && !param.name[prefix.len()..].contains('/')
                        }
                    }),
                }
            }
            "Type" => {
                if values.is_empty() {
                    true
                } else {
                    match option {
                        "BeginsWith" => values.iter().any(|v| param.param_type.starts_with(v)),
                        _ => values.iter().any(|v| param.param_type == *v),
                    }
                }
            }
            "KeyId" => {
                // For SecureString params without explicit KeyId, default is alias/aws/ssm
                let effective_key_id = if param.param_type == "SecureString" {
                    Some(
                        param
                            .key_id
                            .as_deref()
                            .unwrap_or("alias/aws/ssm")
                            .to_string(),
                    )
                } else {
                    param.key_id.clone()
                };
                if values.is_empty() {
                    effective_key_id.is_some()
                } else {
                    match option {
                        "BeginsWith" => effective_key_id
                            .as_ref()
                            .is_some_and(|kid| values.iter().any(|v| kid.starts_with(v))),
                        _ => effective_key_id
                            .as_ref()
                            .is_some_and(|kid| values.contains(&kid.as_str())),
                    }
                }
            }
            "Tier" => values.iter().any(|v| param.tier == *v),
            "Label" => {
                let all_labels: Vec<&String> =
                    param.labels.values().flat_map(|v| v.iter()).collect();
                if values.is_empty() {
                    !all_labels.is_empty()
                } else {
                    match option {
                        "BeginsWith" => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.starts_with(v))),
                        "Contains" => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.contains(v))),
                        _ => values
                            .iter()
                            .any(|v| all_labels.iter().any(|l| l.as_str() == *v)),
                    }
                }
            }
            _ if key.starts_with("tag:") => {
                let tag_key = &key[4..];
                if let Some(tag_val) = param.tags.get(tag_key) {
                    if values.is_empty() {
                        true
                    } else {
                        match option {
                            "BeginsWith" => values.iter().any(|v| tag_val.starts_with(v)),
                            "Contains" => values.iter().any(|v| tag_val.contains(v)),
                            _ => values.contains(&tag_val.as_str()),
                        }
                    }
                } else {
                    false
                }
            }
            _ => true,
        };

        if !matches {
            return false;
        }
    }

    true
}

/// Apply old-style Filters (Filters key, not ParameterFilters).
fn apply_old_filters(param: &SsmParameter, filters: Option<&Vec<Value>>) -> bool {
    let filters = match filters {
        Some(f) => f,
        None => return true,
    };

    for filter in filters {
        let key = match filter["Key"].as_str() {
            Some(k) => k,
            None => continue,
        };
        let values: Vec<&str> = filter["Values"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default();

        let matches = match key {
            "Name" => values.iter().any(|v| param.name.contains(v)),
            "Type" => values.iter().any(|v| param.param_type == *v),
            "KeyId" => param
                .key_id
                .as_ref()
                .is_some_and(|kid| values.contains(&kid.as_str())),
            _ => true,
        };

        if !matches {
            return false;
        }
    }

    true
}

fn resolve_param_by_name_or_arn<'a>(
    state: &'a crate::state::SsmState,
    name: &str,
) -> Result<&'a SsmParameter, AwsServiceError> {
    // Direct name lookup with normalization
    if let Some(p) = lookup_param(&state.parameters, name) {
        return Ok(p);
    }

    // ARN lookup: arn:aws:ssm:REGION:ACCOUNT:parameter/NAME
    if name.starts_with("arn:aws:ssm:") {
        if let Some(param_part) = name.split(":parameter").nth(1) {
            if let Some(p) = lookup_param(&state.parameters, param_part) {
                return Ok(p);
            }
        }
    }

    Err(param_not_found(name))
}

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("The request must contain the parameter {name}"),
    )
}

fn param_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ParameterNotFound",
        format!("Parameter {name} not found."),
    )
}

fn doc_not_found(_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidDocument",
        "The specified document does not exist.".to_string(),
    )
}

fn invalid_resource_id(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidResourceId",
        format!("The resource ID \"{id}\" is not valid. Verify the ID and try again."),
    )
}

fn mw_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "DoesNotExistException",
        format!("Maintenance window {id} does not exist"),
    )
}

/// Parse a document content string (JSON or YAML) and extract metadata fields.
/// Returns (description, schema_version, parameters_json).
fn extract_document_metadata(
    content: &str,
    format: &str,
) -> (Option<String>, Option<String>, Vec<Value>) {
    let parsed: Option<Value> = match format {
        "YAML" => serde_yaml::from_str(content).ok(),
        _ => serde_json::from_str(content).ok(),
    };

    let parsed = match parsed {
        Some(v) => v,
        None => return (None, None, Vec::new()),
    };

    let description = parsed
        .get("description")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let schema_version = parsed
        .get("schemaVersion")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let parameters = if let Some(params_obj) = parsed.get("parameters").and_then(|v| v.as_object())
    {
        params_obj
            .iter()
            .map(|(name, def)| {
                let mut param = json!({ "Name": name });
                if let Some(t) = def.get("type").and_then(|v| v.as_str()) {
                    param["Type"] = json!(t);
                }
                if let Some(d) = def.get("description").and_then(|v| v.as_str()) {
                    param["Description"] = json!(d);
                }
                if let Some(dv) = def.get("default") {
                    let default_str = match dv {
                        Value::String(s) => s.clone(),
                        Value::Number(n) => n.to_string(),
                        Value::Bool(b) => if *b { "True" } else { "False" }.to_string(),
                        other => json_dumps(other),
                    };
                    param["DefaultValue"] = json!(default_str);
                }
                param
            })
            .collect()
    } else {
        Vec::new()
    };

    (description, schema_version, parameters)
}

/// Look up the default patch baseline for a given region and OS.
fn default_patch_baseline(region: &str, operating_system: &str) -> Option<String> {
    static DEFAULT_BASELINES: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("default_baselines.json")).unwrap_or(json!({}))
    });
    DEFAULT_BASELINES
        .get(region)
        .and_then(|r| r.get(operating_system))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Check if a baseline ID is a known default baseline.
fn is_default_patch_baseline(baseline_id: &str) -> bool {
    static DEFAULT_BASELINES: std::sync::LazyLock<Value> = std::sync::LazyLock::new(|| {
        serde_json::from_str(include_str!("default_baselines.json")).unwrap_or(json!({}))
    });
    if let Some(obj) = DEFAULT_BASELINES.as_object() {
        for region_data in obj.values() {
            if let Some(region_obj) = region_data.as_object() {
                for val in region_obj.values() {
                    if val.as_str() == Some(baseline_id) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Serialize a JSON value with Python-style separators (`, ` and `: `).
fn json_dumps(val: &Value) -> String {
    match val {
        Value::Null => "null".to_string(),
        Value::Bool(b) => if *b { "true" } else { "false" }.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
        Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_dumps).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(obj) => {
            let items: Vec<String> = obj
                .iter()
                .map(|(k, v)| {
                    format!(
                        "\"{}\": {}",
                        k.replace('\\', "\\\\").replace('"', "\\\""),
                        json_dumps(v)
                    )
                })
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_service() -> SsmService {
        let state: SharedSsmState = Arc::new(RwLock::new(crate::state::SsmState::new(
            "123456789012",
            "us-east-1",
        )));
        SsmService::new(state)
    }

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "ssm".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-id".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    fn send_command(svc: &SsmService, doc_name: &str) -> String {
        let req = make_request(
            "SendCommand",
            json!({
                "DocumentName": doc_name,
                "InstanceIds": ["i-1234567890abcdef0"]
            }),
        );
        let resp = svc.send_command(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        body["Command"]["CommandId"].as_str().unwrap().to_string()
    }

    #[test]
    fn list_commands_pagination() {
        let svc = make_service();

        // Send 3 commands
        let mut command_ids = Vec::new();
        for i in 0..3 {
            command_ids.push(send_command(&svc, &format!("AWS-RunShellScript-{i}")));
        }

        // First page: MaxResults=1
        let req = make_request("ListCommands", json!({ "MaxResults": 1 }));
        let resp = svc.list_commands(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Commands"].as_array().unwrap().len(), 1);
        let token = body["NextToken"].as_str().unwrap();

        // Second page
        let req = make_request(
            "ListCommands",
            json!({ "MaxResults": 1, "NextToken": token }),
        );
        let resp = svc.list_commands(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Commands"].as_array().unwrap().len(), 1);
        let token = body["NextToken"].as_str().unwrap();

        // Third page (last)
        let req = make_request(
            "ListCommands",
            json!({ "MaxResults": 1, "NextToken": token }),
        );
        let resp = svc.list_commands(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Commands"].as_array().unwrap().len(), 1);
        assert!(body.get("NextToken").is_none() || body["NextToken"].is_null());
    }

    #[test]
    fn send_command_response_omits_non_shape_fields() {
        let svc = make_service();

        // Create a document first
        let req = make_request(
            "CreateDocument",
            json!({
                "Name": "TestDoc",
                "Content": "{\"schemaVersion\":\"2.2\",\"mainSteps\":[]}",
                "DocumentType": "Command"
            }),
        );
        svc.create_document(&req).unwrap();

        let req = make_request(
            "SendCommand",
            json!({
                "DocumentName": "TestDoc",
                "InstanceIds": ["i-1234567890abcdef0"],
                "DocumentHash": "abc123hash",
                "DocumentHashType": "Sha256",
                "ServiceRoleArn": "arn:aws:iam::123456789012:role/MyRole"
            }),
        );
        let resp = svc.send_command(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let cmd = &body["Command"];

        // These fields are not part of the Smithy Command output shape
        assert!(
            !cmd.as_object().unwrap().contains_key("DocumentHash"),
            "DocumentHash should not be in SendCommand response"
        );
        assert!(
            !cmd.as_object().unwrap().contains_key("DocumentHashType"),
            "DocumentHashType should not be in SendCommand response"
        );
        assert!(
            !cmd.as_object().unwrap().contains_key("ServiceRoleArn"),
            "ServiceRoleArn should not be in SendCommand response"
        );

        // Ensure expected fields are still present
        assert!(cmd["CommandId"].is_string());
        assert_eq!(cmd["DocumentName"].as_str().unwrap(), "TestDoc");
    }

    #[test]
    fn describe_maintenance_windows_pagination() {
        let svc = make_service();

        // Create 3 maintenance windows (min MaxResults for this API is 10,
        // so we create 11 to test pagination with the minimum page size)
        for i in 0..11 {
            let req = make_request(
                "CreateMaintenanceWindow",
                json!({
                    "Name": format!("test-window-{i:02}"),
                    "Schedule": "cron(0 2 ? * SUN *)",
                    "Duration": 3,
                    "Cutoff": 1,
                    "AllowUnassociatedTargets": true
                }),
            );
            svc.create_maintenance_window(&req).unwrap();
        }

        // First page: MaxResults=10 (minimum allowed)
        let req = make_request("DescribeMaintenanceWindows", json!({ "MaxResults": 10 }));
        let resp = svc.describe_maintenance_windows(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["WindowIdentities"].as_array().unwrap().len(), 10);
        let token = body["NextToken"].as_str().unwrap();

        // Second page (1 remaining)
        let req = make_request(
            "DescribeMaintenanceWindows",
            json!({ "MaxResults": 10, "NextToken": token }),
        );
        let resp = svc.describe_maintenance_windows(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["WindowIdentities"].as_array().unwrap().len(), 1);
        assert!(body.get("NextToken").is_none() || body["NextToken"].is_null());
    }

    // -- Associations --

    #[test]
    fn association_crud() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreateAssociation",
            json!({
                "Name": "AWS-RunShellScript",
                "Targets": [{"Key": "InstanceIds", "Values": ["i-1234567890abcdef0"]}],
                "ScheduleExpression": "rate(1 hour)",
                "AssociationName": "my-assoc",
            }),
        );
        let resp = svc.create_association(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let assoc_id = body["AssociationDescription"]["AssociationId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            body["AssociationDescription"]["Name"].as_str().unwrap(),
            "AWS-RunShellScript"
        );

        // Describe
        let req = make_request("DescribeAssociation", json!({ "AssociationId": assoc_id }));
        let resp = svc.describe_association(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["AssociationDescription"]["AssociationName"]
                .as_str()
                .unwrap(),
            "my-assoc"
        );

        // List
        let req = make_request("ListAssociations", json!({}));
        let resp = svc.list_associations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Associations"].as_array().unwrap().len(), 1);

        // Update
        let req = make_request(
            "UpdateAssociation",
            json!({
                "AssociationId": assoc_id,
                "AssociationName": "updated-assoc",
            }),
        );
        let resp = svc.update_association(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["AssociationDescription"]["AssociationName"]
                .as_str()
                .unwrap(),
            "updated-assoc"
        );

        // ListAssociationVersions
        let req = make_request(
            "ListAssociationVersions",
            json!({ "AssociationId": assoc_id }),
        );
        let resp = svc.list_association_versions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["AssociationVersions"].as_array().unwrap().len(), 2);

        // Delete
        let req = make_request("DeleteAssociation", json!({ "AssociationId": assoc_id }));
        svc.delete_association(&req).unwrap();

        // Verify deleted
        let req = make_request("DescribeAssociation", json!({ "AssociationId": assoc_id }));
        assert!(svc.describe_association(&req).is_err());
    }

    #[test]
    fn association_batch_create() {
        let svc = make_service();
        let req = make_request(
            "CreateAssociationBatch",
            json!({
                "Entries": [
                    {"Name": "AWS-RunShellScript", "Targets": [{"Key": "InstanceIds", "Values": ["i-001"]}]},
                    {"Name": "AWS-RunShellScript", "Targets": [{"Key": "InstanceIds", "Values": ["i-002"]}]},
                ]
            }),
        );
        let resp = svc.create_association_batch(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Successful"].as_array().unwrap().len(), 2);
        assert!(body["Failed"].as_array().unwrap().is_empty());
    }

    #[test]
    fn start_associations_once_noop() {
        let svc = make_service();
        let req = make_request(
            "StartAssociationsOnce",
            json!({ "AssociationIds": ["fake-id"] }),
        );
        svc.start_associations_once(&req).unwrap();
    }

    // -- OpsItems --

    #[test]
    fn ops_item_crud() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreateOpsItem",
            json!({
                "Title": "Test OpsItem",
                "Source": "test",
                "Description": "A test ops item",
            }),
        );
        let resp = svc.create_ops_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ops_item_id = body["OpsItemId"].as_str().unwrap().to_string();

        // Get
        let req = make_request("GetOpsItem", json!({ "OpsItemId": ops_item_id }));
        let resp = svc.get_ops_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["OpsItem"]["Title"].as_str().unwrap(), "Test OpsItem");
        assert_eq!(body["OpsItem"]["Status"].as_str().unwrap(), "Open");

        // Update
        let req = make_request(
            "UpdateOpsItem",
            json!({
                "OpsItemId": ops_item_id,
                "Title": "Updated OpsItem",
                "Status": "Resolved",
            }),
        );
        svc.update_ops_item(&req).unwrap();

        // Verify update
        let req = make_request("GetOpsItem", json!({ "OpsItemId": ops_item_id }));
        let resp = svc.get_ops_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["OpsItem"]["Title"].as_str().unwrap(),
            "Updated OpsItem"
        );
        assert_eq!(body["OpsItem"]["Status"].as_str().unwrap(), "Resolved");

        // Describe
        let req = make_request("DescribeOpsItems", json!({}));
        let resp = svc.describe_ops_items(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["OpsItemSummaries"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request("DeleteOpsItem", json!({ "OpsItemId": ops_item_id }));
        svc.delete_ops_item(&req).unwrap();

        // Verify deleted
        let req = make_request("GetOpsItem", json!({ "OpsItemId": ops_item_id }));
        assert!(svc.get_ops_item(&req).is_err());
    }

    // -- Resource policies --

    #[test]
    fn resource_policy_crud() {
        let svc = make_service();
        let resource_arn = "arn:aws:ssm:us-east-1:123456789012:parameter/test";

        // Put
        let req = make_request(
            "PutResourcePolicy",
            json!({
                "ResourceArn": resource_arn,
                "Policy": r#"{"Version":"2012-10-17","Statement":[]}"#,
            }),
        );
        let resp = svc.put_resource_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let policy_id = body["PolicyId"].as_str().unwrap().to_string();
        let policy_hash = body["PolicyHash"].as_str().unwrap().to_string();

        // Get
        let req = make_request(
            "GetResourcePolicies",
            json!({ "ResourceArn": resource_arn }),
        );
        let resp = svc.get_resource_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Policies"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request(
            "DeleteResourcePolicy",
            json!({
                "ResourceArn": resource_arn,
                "PolicyId": policy_id,
                "PolicyHash": policy_hash,
            }),
        );
        svc.delete_resource_policy(&req).unwrap();

        // Verify deleted
        let req = make_request(
            "GetResourcePolicies",
            json!({ "ResourceArn": resource_arn }),
        );
        let resp = svc.get_resource_policies(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Policies"].as_array().unwrap().is_empty());
    }

    // -- Stubs --

    #[test]
    fn get_connection_status_returns_connected() {
        let svc = make_service();
        let req = make_request(
            "GetConnectionStatus",
            json!({ "Target": "i-1234567890abcdef0" }),
        );
        let resp = svc.get_connection_status(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Status"].as_str().unwrap(), "connected");
    }

    #[test]
    fn get_calendar_state_returns_open() {
        let svc = make_service();
        let req = make_request(
            "GetCalendarState",
            json!({ "CalendarNames": ["arn:aws:ssm:us-east-1:123456789012:document/cal"] }),
        );
        let resp = svc.get_calendar_state(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["State"].as_str().unwrap(), "OPEN");
    }

    #[test]
    fn service_setting_crud() {
        let svc = make_service();

        // Get default
        let req = make_request(
            "GetServiceSetting",
            json!({ "SettingId": "/ssm/parameter-store/high-throughput-enabled" }),
        );
        let resp = svc.get_service_setting(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ServiceSetting"]["Status"].as_str().unwrap(),
            "Default"
        );

        // Update
        let req = make_request(
            "UpdateServiceSetting",
            json!({
                "SettingId": "/ssm/parameter-store/high-throughput-enabled",
                "SettingValue": "true",
            }),
        );
        svc.update_service_setting(&req).unwrap();

        // Verify
        let req = make_request(
            "GetServiceSetting",
            json!({ "SettingId": "/ssm/parameter-store/high-throughput-enabled" }),
        );
        let resp = svc.get_service_setting(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ServiceSetting"]["Status"].as_str().unwrap(),
            "Customized"
        );
        assert_eq!(
            body["ServiceSetting"]["SettingValue"].as_str().unwrap(),
            "true"
        );

        // Reset
        let req = make_request(
            "ResetServiceSetting",
            json!({ "SettingId": "/ssm/parameter-store/high-throughput-enabled" }),
        );
        svc.reset_service_setting(&req).unwrap();

        // Verify reset
        let req = make_request(
            "GetServiceSetting",
            json!({ "SettingId": "/ssm/parameter-store/high-throughput-enabled" }),
        );
        let resp = svc.get_service_setting(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ServiceSetting"]["Status"].as_str().unwrap(),
            "Default"
        );
    }

    #[test]
    fn list_document_versions_works() {
        let svc = make_service();

        // Create a document
        let req = make_request(
            "CreateDocument",
            json!({
                "Name": "TestDocVer",
                "Content": r#"{"schemaVersion":"2.2","mainSteps":[]}"#,
                "DocumentType": "Command",
            }),
        );
        svc.create_document(&req).unwrap();

        // List versions
        let req = make_request("ListDocumentVersions", json!({ "Name": "TestDocVer" }));
        let resp = svc.list_document_versions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(!body["DocumentVersions"].as_array().unwrap().is_empty());
    }

    #[test]
    fn describe_patch_group_state_returns_zeros() {
        let svc = make_service();
        let req = make_request(
            "DescribePatchGroupState",
            json!({ "PatchGroup": "test-group" }),
        );
        let resp = svc.describe_patch_group_state(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Instances"].as_i64().unwrap(), 0);
    }

    #[test]
    fn get_default_patch_baseline_works() {
        let svc = make_service();
        let req = make_request(
            "GetDefaultPatchBaseline",
            json!({ "OperatingSystem": "WINDOWS" }),
        );
        let resp = svc.get_default_patch_baseline(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["BaselineId"].is_string());
    }

    #[test]
    fn describe_available_patches_returns_empty() {
        let svc = make_service();
        let req = make_request("DescribeAvailablePatches", json!({}));
        let resp = svc.describe_available_patches(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Patches"].as_array().unwrap().is_empty());
    }

    #[test]
    fn describe_patch_properties_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "DescribePatchProperties",
            json!({ "OperatingSystem": "WINDOWS", "Property": "PRODUCT" }),
        );
        let resp = svc.describe_patch_properties(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Properties"].as_array().unwrap().is_empty());
    }

    // ── Inventory ─────────────────────────────────────────────────

    #[test]
    fn inventory_lifecycle() {
        let svc = make_service();

        // PutInventory
        let req = make_request(
            "PutInventory",
            json!({
                "InstanceId": "i-1234567890abcdef0",
                "Items": [{
                    "TypeName": "AWS:Application",
                    "SchemaVersion": "1.1",
                    "CaptureTime": "2024-01-01T00:00:00Z",
                    "Content": [
                        {"Name": "TestApp", "Version": "1.0"},
                        {"Name": "AnotherApp", "Version": "2.0"},
                    ]
                }]
            }),
        );
        svc.put_inventory(&req).unwrap();

        // GetInventory
        let req = make_request("GetInventory", json!({}));
        let resp = svc.get_inventory(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Entities"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["Entities"][0]["Id"].as_str().unwrap(),
            "i-1234567890abcdef0"
        );

        // ListInventoryEntries
        let req = make_request(
            "ListInventoryEntries",
            json!({
                "InstanceId": "i-1234567890abcdef0",
                "TypeName": "AWS:Application",
            }),
        );
        let resp = svc.list_inventory_entries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Entries"].as_array().unwrap().len(), 2);
        assert_eq!(body["TypeName"].as_str().unwrap(), "AWS:Application");

        // GetInventorySchema
        let req = make_request("GetInventorySchema", json!({}));
        let resp = svc.get_inventory_schema(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(!body["Schemas"].as_array().unwrap().is_empty());

        // DeleteInventory
        let req = make_request("DeleteInventory", json!({ "TypeName": "AWS:Application" }));
        let resp = svc.delete_inventory(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["DeletionId"].is_string());

        // DescribeInventoryDeletions
        let req = make_request("DescribeInventoryDeletions", json!({}));
        let resp = svc.describe_inventory_deletions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["InventoryDeletions"].as_array().unwrap().len(), 1);

        // Verify inventory deleted
        let req = make_request(
            "ListInventoryEntries",
            json!({
                "InstanceId": "i-1234567890abcdef0",
                "TypeName": "AWS:Application",
            }),
        );
        let resp = svc.list_inventory_entries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Entries"].as_array().unwrap().is_empty());
    }

    // ── Compliance ────────────────────────────────────────────────

    #[test]
    fn compliance_lifecycle() {
        let svc = make_service();

        // PutComplianceItems
        let req = make_request(
            "PutComplianceItems",
            json!({
                "ResourceId": "i-1234567890abcdef0",
                "ResourceType": "ManagedInstance",
                "ComplianceType": "Custom:PatchTest",
                "ExecutionSummary": {
                    "ExecutionTime": "2024-01-01T00:00:00Z",
                },
                "Items": [
                    {
                        "Id": "patch-1",
                        "Title": "Security patch 1",
                        "Severity": "CRITICAL",
                        "Status": "COMPLIANT",
                    },
                    {
                        "Id": "patch-2",
                        "Title": "Security patch 2",
                        "Severity": "HIGH",
                        "Status": "NON_COMPLIANT",
                    },
                ],
            }),
        );
        svc.put_compliance_items(&req).unwrap();

        // ListComplianceItems
        let req = make_request("ListComplianceItems", json!({}));
        let resp = svc.list_compliance_items(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ComplianceItems"].as_array().unwrap().len(), 2);

        // ListComplianceSummaries
        let req = make_request("ListComplianceSummaries", json!({}));
        let resp = svc.list_compliance_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ComplianceSummaryItems"].as_array().unwrap().len(), 1);

        // ListResourceComplianceSummaries
        let req = make_request("ListResourceComplianceSummaries", json!({}));
        let resp = svc.list_resource_compliance_summaries(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ResourceComplianceSummaryItems"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    // ── Maintenance Window Details ────────────────────────────────

    fn create_mw_with_target_and_task(svc: &SsmService) -> (String, String, String) {
        // Create a window
        let req = make_request(
            "CreateMaintenanceWindow",
            json!({
                "Name": "test-mw",
                "Schedule": "cron(0 2 ? * SUN *)",
                "Duration": 3,
                "Cutoff": 1,
                "AllowUnassociatedTargets": true,
            }),
        );
        let resp = svc.create_maintenance_window(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let window_id = body["WindowId"].as_str().unwrap().to_string();

        // Register target
        let req = make_request(
            "RegisterTargetWithMaintenanceWindow",
            json!({
                "WindowId": window_id,
                "ResourceType": "INSTANCE",
                "Targets": [{"Key": "InstanceIds", "Values": ["i-001"]}],
                "Name": "test-target",
            }),
        );
        let resp = svc.register_target_with_maintenance_window(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let target_id = body["WindowTargetId"].as_str().unwrap().to_string();

        // Register task
        let req = make_request(
            "RegisterTaskWithMaintenanceWindow",
            json!({
                "WindowId": window_id,
                "TaskArn": "AWS-RunShellScript",
                "TaskType": "RUN_COMMAND",
                "Targets": [{"Key": "WindowTargetIds", "Values": [target_id]}],
                "Name": "test-task",
            }),
        );
        let resp = svc.register_task_with_maintenance_window(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let task_id = body["WindowTaskId"].as_str().unwrap().to_string();

        (window_id, target_id, task_id)
    }

    #[test]
    fn maintenance_window_update_target_and_task() {
        let svc = make_service();
        let (window_id, target_id, task_id) = create_mw_with_target_and_task(&svc);

        // Update target
        let req = make_request(
            "UpdateMaintenanceWindowTarget",
            json!({
                "WindowId": window_id,
                "WindowTargetId": target_id,
                "Name": "updated-target",
            }),
        );
        let resp = svc.update_maintenance_window_target(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"].as_str().unwrap(), "updated-target");

        // Get task
        let req = make_request(
            "GetMaintenanceWindowTask",
            json!({
                "WindowId": window_id,
                "WindowTaskId": task_id,
            }),
        );
        let resp = svc.get_maintenance_window_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TaskArn"].as_str().unwrap(), "AWS-RunShellScript");
        assert_eq!(body["Name"].as_str().unwrap(), "test-task");

        // Update task
        let req = make_request(
            "UpdateMaintenanceWindowTask",
            json!({
                "WindowId": window_id,
                "WindowTaskId": task_id,
                "Name": "updated-task",
                "MaxConcurrency": "10",
            }),
        );
        let resp = svc.update_maintenance_window_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"].as_str().unwrap(), "updated-task");
        assert_eq!(body["MaxConcurrency"].as_str().unwrap(), "10");
    }

    #[test]
    fn maintenance_window_execution_lifecycle() {
        let svc = make_service();
        let (window_id, _, _) = create_mw_with_target_and_task(&svc);

        let exec_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
        let task_exec_id = "11111111-2222-3333-4444-555555555555";

        // Manually insert an execution for testing
        {
            let now = chrono::Utc::now();
            let mut state = svc.state.write();
            let exec = crate::state::MaintenanceWindowExecution {
                window_execution_id: exec_id.to_string(),
                window_id: window_id.clone(),
                status: "IN_PROGRESS".to_string(),
                start_time: now,
                end_time: None,
                tasks: vec![crate::state::MaintenanceWindowExecutionTask {
                    task_execution_id: task_exec_id.to_string(),
                    window_execution_id: exec_id.to_string(),
                    task_arn: "AWS-RunShellScript".to_string(),
                    task_type: "RUN_COMMAND".to_string(),
                    status: "IN_PROGRESS".to_string(),
                    start_time: now,
                    end_time: None,
                    invocations: vec![crate::state::MaintenanceWindowExecutionTaskInvocation {
                        invocation_id: "inv-001".to_string(),
                        task_execution_id: task_exec_id.to_string(),
                        window_execution_id: exec_id.to_string(),
                        execution_id: Some("cmd-001".to_string()),
                        status: "IN_PROGRESS".to_string(),
                        start_time: now,
                        end_time: None,
                        parameters: None,
                        owner_information: None,
                        window_target_id: None,
                        status_details: None,
                    }],
                }],
            };
            state.maintenance_window_executions.push(exec);
        }

        // DescribeMaintenanceWindowExecutions
        let req = make_request(
            "DescribeMaintenanceWindowExecutions",
            json!({ "WindowId": window_id }),
        );
        let resp = svc.describe_maintenance_window_executions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["WindowExecutions"].as_array().unwrap().len(), 1);

        // GetMaintenanceWindowExecution
        let req = make_request(
            "GetMaintenanceWindowExecution",
            json!({ "WindowExecutionId": exec_id }),
        );
        let resp = svc.get_maintenance_window_execution(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Status"].as_str().unwrap(), "IN_PROGRESS");

        // DescribeMaintenanceWindowExecutionTasks
        let req = make_request(
            "DescribeMaintenanceWindowExecutionTasks",
            json!({ "WindowExecutionId": exec_id }),
        );
        let resp = svc
            .describe_maintenance_window_execution_tasks(&req)
            .unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["WindowExecutionTaskIdentities"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // GetMaintenanceWindowExecutionTask
        let req = make_request(
            "GetMaintenanceWindowExecutionTask",
            json!({
                "WindowExecutionId": exec_id,
                "TaskId": task_exec_id,
            }),
        );
        let resp = svc.get_maintenance_window_execution_task(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TaskArn"].as_str().unwrap(), "AWS-RunShellScript");

        // DescribeMaintenanceWindowExecutionTaskInvocations
        let req = make_request(
            "DescribeMaintenanceWindowExecutionTaskInvocations",
            json!({
                "WindowExecutionId": exec_id,
                "TaskId": task_exec_id,
            }),
        );
        let resp = svc
            .describe_maintenance_window_execution_task_invocations(&req)
            .unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["WindowExecutionTaskInvocationIdentities"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // GetMaintenanceWindowExecutionTaskInvocation
        let req = make_request(
            "GetMaintenanceWindowExecutionTaskInvocation",
            json!({
                "WindowExecutionId": exec_id,
                "TaskId": task_exec_id,
                "InvocationId": "inv-001",
            }),
        );
        let resp = svc
            .get_maintenance_window_execution_task_invocation(&req)
            .unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ExecutionId"].as_str().unwrap(), "cmd-001");

        // CancelMaintenanceWindowExecution
        let req = make_request(
            "CancelMaintenanceWindowExecution",
            json!({ "WindowExecutionId": exec_id }),
        );
        let resp = svc.cancel_maintenance_window_execution(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["WindowExecutionId"].as_str().unwrap(), exec_id);

        // DescribeMaintenanceWindowSchedule
        let req = make_request("DescribeMaintenanceWindowSchedule", json!({}));
        let resp = svc.describe_maintenance_window_schedule(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["ScheduledWindowExecutions"]
            .as_array()
            .unwrap()
            .is_empty());

        // DescribeMaintenanceWindowsForTarget
        let req = make_request(
            "DescribeMaintenanceWindowsForTarget",
            json!({
                "ResourceType": "INSTANCE",
                "Targets": [{"Key": "InstanceIds", "Values": ["i-001"]}],
            }),
        );
        let resp = svc.describe_maintenance_windows_for_target(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["WindowIdentities"].as_array().unwrap().len(), 1);
    }

    // ── Patch baseline update ─────────────────────────────────────

    #[test]
    fn update_patch_baseline_works() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreatePatchBaseline",
            json!({
                "Name": "test-baseline",
                "OperatingSystem": "AMAZON_LINUX_2",
                "Description": "original description",
            }),
        );
        let resp = svc.create_patch_baseline(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let baseline_id = body["BaselineId"].as_str().unwrap().to_string();

        // Update
        let req = make_request(
            "UpdatePatchBaseline",
            json!({
                "BaselineId": baseline_id,
                "Name": "updated-baseline",
                "Description": "updated description",
                "ApprovedPatches": ["KB001", "KB002"],
            }),
        );
        let resp = svc.update_patch_baseline(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"].as_str().unwrap(), "updated-baseline");
        assert_eq!(body["Description"].as_str().unwrap(), "updated description");
        assert_eq!(body["ApprovedPatches"].as_array().unwrap().len(), 2);
    }

    // ── Resource data sync ────────────────────────────────────────

    #[test]
    fn resource_data_sync_lifecycle() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreateResourceDataSync",
            json!({
                "SyncName": "test-sync",
                "SyncType": "SyncFromSource",
                "SyncSource": {
                    "SourceType": "AWS",
                    "SourceRegions": ["us-east-1"],
                },
            }),
        );
        svc.create_resource_data_sync(&req).unwrap();

        // List
        let req = make_request("ListResourceDataSync", json!({}));
        let resp = svc.list_resource_data_sync(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ResourceDataSyncItems"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["ResourceDataSyncItems"][0]["SyncName"]
                .as_str()
                .unwrap(),
            "test-sync"
        );

        // Update
        let req = make_request(
            "UpdateResourceDataSync",
            json!({
                "SyncName": "test-sync",
                "SyncType": "SyncFromSource",
                "SyncSource": {
                    "SourceType": "AWS",
                    "SourceRegions": ["us-east-1", "us-west-2"],
                },
            }),
        );
        svc.update_resource_data_sync(&req).unwrap();

        // Delete
        let req = make_request("DeleteResourceDataSync", json!({ "SyncName": "test-sync" }));
        svc.delete_resource_data_sync(&req).unwrap();

        // Verify deleted
        let req = make_request("ListResourceDataSync", json!({}));
        let resp = svc.list_resource_data_sync(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["ResourceDataSyncItems"].as_array().unwrap().is_empty());
    }

    // ── Patch stubs ───────────────────────────────────────────────

    #[test]
    fn describe_instance_patch_states_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "DescribeInstancePatchStates",
            json!({ "InstanceIds": ["i-001"] }),
        );
        let resp = svc.describe_instance_patch_states(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["InstancePatchStates"].as_array().unwrap().is_empty());
    }

    #[test]
    fn describe_instance_patches_returns_empty() {
        let svc = make_service();
        let req = make_request("DescribeInstancePatches", json!({ "InstanceId": "i-001" }));
        let resp = svc.describe_instance_patches(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Patches"].as_array().unwrap().is_empty());
    }

    #[test]
    fn describe_effective_patches_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "DescribeEffectivePatchesForPatchBaseline",
            json!({ "BaselineId": "pb-12345678901234567" }),
        );
        let resp = svc
            .describe_effective_patches_for_patch_baseline(&req)
            .unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["EffectivePatches"].as_array().unwrap().is_empty());
    }

    #[test]
    fn get_ops_summary_returns_empty() {
        let svc = make_service();
        let req = make_request("GetOpsSummary", json!({}));
        let resp = svc.get_ops_summary(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Entities"].as_array().unwrap().is_empty());
    }

    // ── OpsItem Related Items ─────────────────────────────────────

    #[test]
    fn associate_and_disassociate_ops_item_related_item() {
        let svc = make_service();
        // Create an ops item first
        let req = make_request(
            "CreateOpsItem",
            json!({ "Title": "Test", "Source": "test", "Description": "test desc" }),
        );
        let resp = svc.create_ops_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ops_item_id = body["OpsItemId"].as_str().unwrap().to_string();

        // Associate
        let req = make_request(
            "AssociateOpsItemRelatedItem",
            json!({
                "OpsItemId": ops_item_id,
                "AssociationType": "IsParentOf",
                "ResourceType": "AWS::SSMIncidents::IncidentRecord",
                "ResourceUri": "arn:aws:ssm-incidents::123456789012:incident-record/test"
            }),
        );
        let resp = svc.associate_ops_item_related_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let assoc_id = body["AssociationId"].as_str().unwrap().to_string();

        // List
        let req = make_request(
            "ListOpsItemRelatedItems",
            json!({ "OpsItemId": ops_item_id }),
        );
        let resp = svc.list_ops_item_related_items(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Summaries"].as_array().unwrap().len(), 1);

        // Disassociate
        let req = make_request(
            "DisassociateOpsItemRelatedItem",
            json!({ "OpsItemId": ops_item_id, "AssociationId": assoc_id }),
        );
        svc.disassociate_ops_item_related_item(&req).unwrap();
    }

    #[test]
    fn list_ops_item_events_returns_empty() {
        let svc = make_service();
        let req = make_request("ListOpsItemEvents", json!({}));
        let resp = svc.list_ops_item_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Summaries"].as_array().unwrap().is_empty());
    }

    // ── OpsMetadata ───────────────────────────────────────────────

    #[test]
    fn ops_metadata_lifecycle() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreateOpsMetadata",
            json!({
                "ResourceId": "test-resource",
                "Metadata": { "key1": { "Value": "val1" } }
            }),
        );
        let resp = svc.create_ops_metadata(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let arn = body["OpsMetadataArn"].as_str().unwrap().to_string();

        // Get
        let req = make_request("GetOpsMetadata", json!({ "OpsMetadataArn": arn }));
        let resp = svc.get_ops_metadata(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ResourceId"].as_str().unwrap(), "test-resource");

        // Update
        let req = make_request(
            "UpdateOpsMetadata",
            json!({
                "OpsMetadataArn": arn,
                "MetadataToUpdate": { "key2": { "Value": "val2" } }
            }),
        );
        svc.update_ops_metadata(&req).unwrap();

        // List
        let req = make_request("ListOpsMetadata", json!({}));
        let resp = svc.list_ops_metadata(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["OpsMetadataList"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request("DeleteOpsMetadata", json!({ "OpsMetadataArn": arn }));
        svc.delete_ops_metadata(&req).unwrap();
    }

    // ── Automation ────────────────────────────────────────────────

    #[test]
    fn automation_execution_lifecycle() {
        let svc = make_service();

        // Start
        let req = make_request(
            "StartAutomationExecution",
            json!({ "DocumentName": "AWS-RunShellScript" }),
        );
        let resp = svc.start_automation_execution(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let exec_id = body["AutomationExecutionId"].as_str().unwrap().to_string();

        // Get
        let req = make_request(
            "GetAutomationExecution",
            json!({ "AutomationExecutionId": exec_id }),
        );
        let resp = svc.get_automation_execution(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["AutomationExecution"]["AutomationExecutionStatus"]
                .as_str()
                .unwrap(),
            "InProgress"
        );

        // Describe
        let req = make_request("DescribeAutomationExecutions", json!({}));
        let resp = svc.describe_automation_executions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["AutomationExecutionMetadataList"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // DescribeSteps
        let req = make_request(
            "DescribeAutomationStepExecutions",
            json!({ "AutomationExecutionId": exec_id }),
        );
        let resp = svc.describe_automation_step_executions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["StepExecutions"].as_array().unwrap().is_empty());

        // Signal
        let req = make_request(
            "SendAutomationSignal",
            json!({ "AutomationExecutionId": exec_id, "SignalType": "Approve" }),
        );
        svc.send_automation_signal(&req).unwrap();

        // Stop
        let req = make_request(
            "StopAutomationExecution",
            json!({ "AutomationExecutionId": exec_id }),
        );
        svc.stop_automation_execution(&req).unwrap();
    }

    #[test]
    fn start_change_request_execution_works() {
        let svc = make_service();
        let req = make_request(
            "StartChangeRequestExecution",
            json!({
                "DocumentName": "AWS-ChangeManager",
                "Runbooks": [{ "DocumentName": "AWS-RunShellScript" }]
            }),
        );
        let resp = svc.start_change_request_execution(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["AutomationExecutionId"].as_str().is_some());
    }

    #[test]
    fn execution_preview_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "StartExecutionPreview",
            json!({ "DocumentName": "AWS-RunShellScript" }),
        );
        let resp = svc.start_execution_preview(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let preview_id = body["ExecutionPreviewId"].as_str().unwrap().to_string();

        let req = make_request(
            "GetExecutionPreview",
            json!({ "ExecutionPreviewId": preview_id }),
        );
        let resp = svc.get_execution_preview(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Status"].as_str().unwrap(), "Success");
    }

    // ── Sessions ──────────────────────────────────────────────────

    #[test]
    fn session_lifecycle() {
        let svc = make_service();

        // Start
        let req = make_request("StartSession", json!({ "Target": "i-001" }));
        let resp = svc.start_session(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let session_id = body["SessionId"].as_str().unwrap().to_string();
        assert!(body["TokenValue"].as_str().is_some());

        // Resume
        let req = make_request("ResumeSession", json!({ "SessionId": session_id }));
        let resp = svc.resume_session(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["SessionId"].as_str().unwrap(), session_id);

        // Describe (Active)
        let req = make_request("DescribeSessions", json!({ "State": "Active" }));
        let resp = svc.describe_sessions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Sessions"].as_array().unwrap().len(), 1);

        // Terminate
        let req = make_request("TerminateSession", json!({ "SessionId": session_id }));
        svc.terminate_session(&req).unwrap();

        // Describe (History)
        let req = make_request("DescribeSessions", json!({ "State": "History" }));
        let resp = svc.describe_sessions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Sessions"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn start_access_request_and_get_token() {
        let svc = make_service();

        let req = make_request(
            "StartAccessRequest",
            json!({ "Reason": "test", "Targets": [{"Key": "InstanceIds", "Values": ["i-001"]}] }),
        );
        let resp = svc.start_access_request(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ar_id = body["AccessRequestId"].as_str().unwrap().to_string();

        let req = make_request("GetAccessToken", json!({ "AccessRequestId": ar_id }));
        let resp = svc.get_access_token(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Credentials"]["AccessKeyId"].as_str().is_some());
        assert_eq!(body["AccessRequestStatus"].as_str(), Some("Approved"));
    }

    // ── Managed Instances ─────────────────────────────────────────

    #[test]
    fn activation_lifecycle() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreateActivation",
            json!({ "IamRole": "SSMServiceRole", "Description": "test" }),
        );
        let resp = svc.create_activation(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let activation_id = body["ActivationId"].as_str().unwrap().to_string();
        assert!(body["ActivationCode"].as_str().is_some());

        // Describe
        let req = make_request("DescribeActivations", json!({}));
        let resp = svc.describe_activations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ActivationList"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request("DeleteActivation", json!({ "ActivationId": activation_id }));
        svc.delete_activation(&req).unwrap();
    }

    #[test]
    fn deregister_managed_instance_no_error() {
        let svc = make_service();
        let req = make_request(
            "DeregisterManagedInstance",
            json!({ "InstanceId": "mi-01234567890123456" }),
        );
        svc.deregister_managed_instance(&req).unwrap();
    }

    #[test]
    fn describe_instance_information_empty() {
        let svc = make_service();
        let req = make_request("DescribeInstanceInformation", json!({}));
        let resp = svc.describe_instance_information(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["InstanceInformationList"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn describe_instance_properties_empty() {
        let svc = make_service();
        let req = make_request("DescribeInstanceProperties", json!({}));
        let resp = svc.describe_instance_properties(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["InstanceProperties"].as_array().unwrap().is_empty());
    }

    // ── Other ─────────────────────────────────────────────────────

    #[test]
    fn list_nodes_returns_empty() {
        let svc = make_service();
        let req = make_request("ListNodes", json!({}));
        let resp = svc.list_nodes(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Nodes"].as_array().unwrap().is_empty());
    }

    #[test]
    fn list_nodes_summary_returns_empty() {
        let svc = make_service();
        let req = make_request(
            "ListNodesSummary",
            json!({ "Aggregators": [{"AggregatorType": "Count"}] }),
        );
        let resp = svc.list_nodes_summary(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Summary"].as_array().unwrap().is_empty());
    }

    #[test]
    fn describe_effective_instance_associations_empty() {
        let svc = make_service();
        let req = make_request(
            "DescribeEffectiveInstanceAssociations",
            json!({ "InstanceId": "i-001" }),
        );
        let resp = svc.describe_effective_instance_associations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Associations"].as_array().unwrap().is_empty());
    }

    #[test]
    fn describe_instance_associations_status_empty() {
        let svc = make_service();
        let req = make_request(
            "DescribeInstanceAssociationsStatus",
            json!({ "InstanceId": "i-001" }),
        );
        let resp = svc.describe_instance_associations_status(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["InstanceAssociationStatusInfos"]
            .as_array()
            .unwrap()
            .is_empty());
    }
}
