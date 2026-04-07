mod associations;
mod automation;
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

use async_trait::async_trait;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::SharedSsmState;

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

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("The request must contain the parameter {name}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use serde_json::{json, Value};
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
            raw_query: String::new(),
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

    // ── Parameter Labels ─────────────────────────────────────────

    fn put_param(svc: &SsmService, name: &str, value: &str) -> i64 {
        let req = make_request(
            "PutParameter",
            json!({
                "Name": name,
                "Value": value,
                "Type": "String",
                "Overwrite": true,
            }),
        );
        let resp = svc.put_parameter(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        body["Version"].as_i64().unwrap()
    }

    #[test]
    fn label_and_unlabel_parameter_version() {
        let svc = make_service();

        // Create parameter with two versions
        put_param(&svc, "/label/test", "v1");
        put_param(&svc, "/label/test", "v2");

        // Label version 1
        let req = make_request(
            "LabelParameterVersion",
            json!({
                "Name": "/label/test",
                "ParameterVersion": 1,
                "Labels": ["prod", "stable"],
            }),
        );
        let resp = svc.label_parameter_version(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["InvalidLabels"].as_array().unwrap().is_empty());
        assert_eq!(body["ParameterVersion"].as_i64().unwrap(), 1);

        // Get parameter history — version 1 should have labels
        let req = make_request("GetParameterHistory", json!({ "Name": "/label/test" }));
        let resp = svc.get_parameter_history(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let params = body["Parameters"].as_array().unwrap();
        let v1 = params
            .iter()
            .find(|p| p["Version"].as_i64() == Some(1))
            .unwrap();
        let labels = v1["Labels"].as_array().unwrap();
        assert!(labels.iter().any(|l| l.as_str() == Some("prod")));
        assert!(labels.iter().any(|l| l.as_str() == Some("stable")));

        // Unlabel version 1
        let req = make_request(
            "UnlabelParameterVersion",
            json!({
                "Name": "/label/test",
                "ParameterVersion": 1,
                "Labels": ["prod"],
            }),
        );
        let resp = svc.unlabel_parameter_version(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["InvalidLabels"].as_array().unwrap().is_empty());
        let removed = body["RemovedLabels"].as_array().unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].as_str().unwrap(), "prod");
    }

    #[test]
    fn label_parameter_version_defaults_to_latest() {
        let svc = make_service();
        put_param(&svc, "/label/default", "v1");
        put_param(&svc, "/label/default", "v2");

        // Label without specifying version — should target latest (2)
        let req = make_request(
            "LabelParameterVersion",
            json!({
                "Name": "/label/default",
                "Labels": ["latest-label"],
            }),
        );
        let resp = svc.label_parameter_version(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ParameterVersion"].as_i64().unwrap(), 2);
    }

    #[test]
    fn label_parameter_version_invalid_labels() {
        let svc = make_service();
        put_param(&svc, "/label/invalid", "v1");

        // Labels starting with aws/ssm or containing / are invalid
        let req = make_request(
            "LabelParameterVersion",
            json!({
                "Name": "/label/invalid",
                "Labels": ["aws-reserved", "valid-label"],
            }),
        );
        let resp = svc.label_parameter_version(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let invalid = body["InvalidLabels"].as_array().unwrap();
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].as_str().unwrap(), "aws-reserved");
    }

    #[test]
    fn label_parameter_version_not_found() {
        let svc = make_service();
        put_param(&svc, "/label/notfound", "v1");

        let req = make_request(
            "LabelParameterVersion",
            json!({
                "Name": "/label/notfound",
                "ParameterVersion": 999,
                "Labels": ["test"],
            }),
        );
        let result = svc.label_parameter_version(&req);
        assert!(result.is_err());
    }

    #[test]
    fn unlabel_parameter_version_returns_invalid_for_missing_labels() {
        let svc = make_service();
        put_param(&svc, "/label/missing", "v1");

        let req = make_request(
            "UnlabelParameterVersion",
            json!({
                "Name": "/label/missing",
                "ParameterVersion": 1,
                "Labels": ["nonexistent"],
            }),
        );
        let resp = svc.unlabel_parameter_version(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let invalid = body["InvalidLabels"].as_array().unwrap();
        assert_eq!(invalid.len(), 1);
        assert_eq!(invalid[0].as_str().unwrap(), "nonexistent");
    }

    // ── Document Operations ──────────────────────────────────────

    fn create_doc(svc: &SsmService, name: &str) {
        let req = make_request(
            "CreateDocument",
            json!({
                "Name": name,
                "Content": r#"{"schemaVersion":"2.2","mainSteps":[]}"#,
                "DocumentType": "Command",
            }),
        );
        svc.create_document(&req).unwrap();
    }

    #[test]
    fn update_document_and_default_version() {
        let svc = make_service();
        create_doc(&svc, "TestDoc");

        // Update document (creates version 2)
        let req = make_request(
            "UpdateDocument",
            json!({
                "Name": "TestDoc",
                "Content": r#"{"schemaVersion":"2.2","description":"v2","mainSteps":[]}"#,
                "VersionName": "release-2",
            }),
        );
        let resp = svc.update_document(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let desc = &body["DocumentDescription"];
        assert_eq!(desc["DocumentVersion"].as_str().unwrap(), "2");
        assert_eq!(desc["VersionName"].as_str().unwrap(), "release-2");

        // List document versions
        let req = make_request("ListDocumentVersions", json!({ "Name": "TestDoc" }));
        let resp = svc.list_document_versions(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DocumentVersions"].as_array().unwrap().len(), 2);

        // Update default version to 2
        let req = make_request(
            "UpdateDocumentDefaultVersion",
            json!({
                "Name": "TestDoc",
                "DocumentVersion": "2",
            }),
        );
        let resp = svc.update_document_default_version(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Description"]["DefaultVersion"].as_str().unwrap(), "2");

        // Verify describe_document now shows version 2 as default
        let req = make_request("DescribeDocument", json!({ "Name": "TestDoc" }));
        let resp = svc.describe_document(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Document"]["DefaultVersion"].as_str().unwrap(), "2");
    }

    #[test]
    fn update_document_duplicate_content_fails() {
        let svc = make_service();
        create_doc(&svc, "DupDoc");

        // Try to update with same content
        let req = make_request(
            "UpdateDocument",
            json!({
                "Name": "DupDoc",
                "Content": r#"{"schemaVersion":"2.2","mainSteps":[]}"#,
            }),
        );
        let result = svc.update_document(&req);
        assert!(result.is_err());
    }

    #[test]
    fn document_permissions_modify_and_describe() {
        let svc = make_service();
        create_doc(&svc, "PermDoc");

        // Add permission
        let req = make_request(
            "ModifyDocumentPermission",
            json!({
                "Name": "PermDoc",
                "PermissionType": "Share",
                "AccountIdsToAdd": ["111111111111", "222222222222"],
            }),
        );
        svc.modify_document_permission(&req).unwrap();

        // Describe permission
        let req = make_request(
            "DescribeDocumentPermission",
            json!({
                "Name": "PermDoc",
                "PermissionType": "Share",
            }),
        );
        let resp = svc.describe_document_permission(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ids = body["AccountIds"].as_array().unwrap();
        assert_eq!(ids.len(), 2);

        // Remove one permission
        let req = make_request(
            "ModifyDocumentPermission",
            json!({
                "Name": "PermDoc",
                "PermissionType": "Share",
                "AccountIdsToRemove": ["111111111111"],
            }),
        );
        svc.modify_document_permission(&req).unwrap();

        // Verify only one remains
        let req = make_request(
            "DescribeDocumentPermission",
            json!({
                "Name": "PermDoc",
                "PermissionType": "Share",
            }),
        );
        let resp = svc.describe_document_permission(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let ids = body["AccountIds"].as_array().unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0].as_str().unwrap(), "222222222222");
    }

    #[test]
    fn modify_document_permission_invalid_type() {
        let svc = make_service();
        create_doc(&svc, "PermDoc2");

        let req = make_request(
            "ModifyDocumentPermission",
            json!({
                "Name": "PermDoc2",
                "PermissionType": "Invalid",
                "AccountIdsToAdd": ["111111111111"],
            }),
        );
        let result = svc.modify_document_permission(&req);
        assert!(result.is_err());
    }

    // ── Maintenance Window Targets and Tasks ─────────────────────

    #[test]
    fn describe_maintenance_window_targets_and_tasks() {
        let svc = make_service();
        let (window_id, _target_id, _task_id) = create_mw_with_target_and_task(&svc);

        // Describe targets
        let req = make_request(
            "DescribeMaintenanceWindowTargets",
            json!({ "WindowId": window_id }),
        );
        let resp = svc.describe_maintenance_window_targets(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let targets = body["Targets"].as_array().unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0]["Name"].as_str().unwrap(), "test-target");

        // Describe tasks
        let req = make_request(
            "DescribeMaintenanceWindowTasks",
            json!({ "WindowId": window_id }),
        );
        let resp = svc.describe_maintenance_window_tasks(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let tasks = body["Tasks"].as_array().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0]["TaskArn"].as_str().unwrap(), "AWS-RunShellScript");
        assert_eq!(tasks[0]["Name"].as_str().unwrap(), "test-task");
    }

    // ── Patch Baselines ──────────────────────────────────────────

    fn create_baseline(svc: &SsmService, name: &str) -> String {
        let req = make_request(
            "CreatePatchBaseline",
            json!({
                "Name": name,
                "OperatingSystem": "AMAZON_LINUX_2",
                "Description": "test baseline",
            }),
        );
        let resp = svc.create_patch_baseline(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        body["BaselineId"].as_str().unwrap().to_string()
    }

    #[test]
    fn patch_baseline_get_and_delete() {
        let svc = make_service();
        let baseline_id = create_baseline(&svc, "get-del-baseline");

        // Get
        let req = make_request("GetPatchBaseline", json!({ "BaselineId": baseline_id }));
        let resp = svc.get_patch_baseline(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"].as_str().unwrap(), "get-del-baseline");
        assert_eq!(body["OperatingSystem"].as_str().unwrap(), "AMAZON_LINUX_2");
        assert_eq!(body["Description"].as_str().unwrap(), "test baseline");

        // Delete
        let req = make_request("DeletePatchBaseline", json!({ "BaselineId": baseline_id }));
        svc.delete_patch_baseline(&req).unwrap();

        // Get should fail
        let req = make_request("GetPatchBaseline", json!({ "BaselineId": baseline_id }));
        let result = svc.get_patch_baseline(&req);
        assert!(result.is_err());
    }

    #[test]
    fn describe_patch_baselines_with_filter() {
        let svc = make_service();
        create_baseline(&svc, "alpha-baseline");
        create_baseline(&svc, "beta-baseline");

        // Filter by NAME_PREFIX
        let req = make_request(
            "DescribePatchBaselines",
            json!({
                "Filters": [{"Key": "NAME_PREFIX", "Values": ["alpha"]}],
            }),
        );
        let resp = svc.describe_patch_baselines(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let baselines = body["BaselineIdentities"].as_array().unwrap();
        assert_eq!(baselines.len(), 1);
        assert_eq!(
            baselines[0]["BaselineName"].as_str().unwrap(),
            "alpha-baseline"
        );
    }

    #[test]
    fn patch_group_register_and_deregister() {
        let svc = make_service();
        let baseline_id = create_baseline(&svc, "pg-baseline");

        // Register patch group
        let req = make_request(
            "RegisterPatchBaselineForPatchGroup",
            json!({
                "BaselineId": baseline_id,
                "PatchGroup": "production",
            }),
        );
        let resp = svc.register_patch_baseline_for_patch_group(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["PatchGroup"].as_str().unwrap(), "production");

        // Get patch baseline for group
        let req = make_request(
            "GetPatchBaselineForPatchGroup",
            json!({
                "PatchGroup": "production",
                "OperatingSystem": "AMAZON_LINUX_2",
            }),
        );
        let resp = svc.get_patch_baseline_for_patch_group(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["BaselineId"].as_str().unwrap(), baseline_id);

        // Describe patch groups
        let req = make_request("DescribePatchGroups", json!({}));
        let resp = svc.describe_patch_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let mappings = body["Mappings"].as_array().unwrap();
        assert_eq!(mappings.len(), 1);
        assert_eq!(mappings[0]["PatchGroup"].as_str().unwrap(), "production");

        // Deregister
        let req = make_request(
            "DeregisterPatchBaselineForPatchGroup",
            json!({
                "BaselineId": baseline_id,
                "PatchGroup": "production",
            }),
        );
        svc.deregister_patch_baseline_for_patch_group(&req).unwrap();

        // Verify removed
        let req = make_request("DescribePatchGroups", json!({}));
        let resp = svc.describe_patch_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Mappings"].as_array().unwrap().is_empty());
    }

    #[test]
    fn delete_patch_baseline_removes_patch_groups() {
        let svc = make_service();
        let baseline_id = create_baseline(&svc, "del-pg-baseline");

        // Register a patch group
        let req = make_request(
            "RegisterPatchBaselineForPatchGroup",
            json!({
                "BaselineId": baseline_id,
                "PatchGroup": "staging",
            }),
        );
        svc.register_patch_baseline_for_patch_group(&req).unwrap();

        // Delete baseline
        let req = make_request("DeletePatchBaseline", json!({ "BaselineId": baseline_id }));
        svc.delete_patch_baseline(&req).unwrap();

        // Patch groups should be cleaned up
        let req = make_request("DescribePatchGroups", json!({}));
        let resp = svc.describe_patch_groups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Mappings"].as_array().unwrap().is_empty());
    }

    // ── Command Details ──────────────────────────────────────────

    #[test]
    fn get_command_invocation_success() {
        let svc = make_service();
        let cmd_id = send_command(&svc, "AWS-RunShellScript");

        let req = make_request(
            "GetCommandInvocation",
            json!({
                "CommandId": cmd_id,
                "InstanceId": "i-1234567890abcdef0",
            }),
        );
        let resp = svc.get_command_invocation(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["CommandId"].as_str().unwrap(), cmd_id);
        assert_eq!(body["InstanceId"].as_str().unwrap(), "i-1234567890abcdef0");
        assert_eq!(body["Status"].as_str().unwrap(), "Success");
    }

    #[test]
    fn get_command_invocation_wrong_instance_fails() {
        let svc = make_service();
        let cmd_id = send_command(&svc, "AWS-RunShellScript");

        let req = make_request(
            "GetCommandInvocation",
            json!({
                "CommandId": cmd_id,
                "InstanceId": "i-0000000000000000f",
            }),
        );
        let result = svc.get_command_invocation(&req);
        assert!(result.is_err());
    }

    #[test]
    fn list_command_invocations() {
        let svc = make_service();
        let cmd_id = send_command(&svc, "AWS-RunShellScript");

        // List all invocations
        let req = make_request("ListCommandInvocations", json!({}));
        let resp = svc.list_command_invocations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let invocations = body["CommandInvocations"].as_array().unwrap();
        assert!(!invocations.is_empty());
        assert_eq!(invocations[0]["CommandId"].as_str().unwrap(), cmd_id);
        assert_eq!(
            invocations[0]["InstanceId"].as_str().unwrap(),
            "i-1234567890abcdef0"
        );

        // Filter by CommandId
        let req = make_request("ListCommandInvocations", json!({ "CommandId": cmd_id }));
        let resp = svc.list_command_invocations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["CommandInvocations"].as_array().unwrap().len(), 1);
    }
}
