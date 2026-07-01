use super::*;
use parking_lot::RwLock;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

fn make_service() -> SsmService {
    let state: SharedSsmState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
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
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Commands"].as_array().unwrap().len(), 1);
    let token = body["NextToken"].as_str().unwrap();

    // Second page
    let req = make_request(
        "ListCommands",
        json!({ "MaxResults": 1, "NextToken": token }),
    );
    let resp = svc.list_commands(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Commands"].as_array().unwrap().len(), 1);
    let token = body["NextToken"].as_str().unwrap();

    // Third page (last)
    let req = make_request(
        "ListCommands",
        json!({ "MaxResults": 1, "NextToken": token }),
    );
    let resp = svc.list_commands(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Commands"].as_array().unwrap().len(), 1);
    assert!(body.get("NextToken").is_none() || body["NextToken"].is_null());
}

#[test]
fn list_commands_rejects_invalid_next_token() {
    // bug-audit 2026-05-28, 1.7: a NextToken that does not parse as a valid
    // offset must surface SSM's declared `InvalidNextToken` wire code rather
    // than being silently treated as the first page (which can drive an
    // infinite client pagination loop).
    let svc = make_service();
    send_command(&svc, "AWS-RunShellScript");

    let req = make_request(
        "ListCommands",
        json!({ "MaxResults": 1, "NextToken": "not-a-valid-offset" }),
    );
    match svc.list_commands(&req) {
        Ok(_) => panic!("malformed NextToken must be rejected"),
        Err(err) => assert_eq!(err.code(), "InvalidNextToken"),
    }
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["WindowIdentities"].as_array().unwrap().len(), 10);
    let token = body["NextToken"].as_str().unwrap();

    // Second page (1 remaining)
    let req = make_request(
        "DescribeMaintenanceWindows",
        json!({ "MaxResults": 10, "NextToken": token }),
    );
    let resp = svc.describe_maintenance_windows(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["AssociationDescription"]["AssociationName"]
            .as_str()
            .unwrap(),
        "my-assoc"
    );

    // List
    let req = make_request("ListAssociations", json!({}));
    let resp = svc.list_associations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Successful"].as_array().unwrap().len(), 2);
    assert!(body["Failed"].as_array().unwrap().is_empty());
}

#[test]
fn start_associations_once_unknown_id_errors() {
    let svc = make_service();
    let req = make_request(
        "StartAssociationsOnce",
        json!({ "AssociationIds": ["fake-id"] }),
    );
    assert!(svc.start_associations_once(&req).is_err());
}

#[test]
fn start_associations_once_marks_pending_and_records_execution() {
    let svc = make_service();

    // Create a document to associate against
    let req = make_request(
        "CreateDocument",
        json!({
            "Name": "doc-for-assoc",
            "Content": "{\"schemaVersion\": \"2.2\"}",
            "DocumentType": "Command",
        }),
    );
    svc.create_document(&req).unwrap();

    // Create association
    let req = make_request(
        "CreateAssociation",
        json!({
            "Name": "doc-for-assoc",
            "Targets": [{"Key": "InstanceIds", "Values": ["i-1234"]}],
        }),
    );
    let resp = svc.create_association(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let assoc_id = body["AssociationDescription"]["AssociationId"]
        .as_str()
        .unwrap()
        .to_string();

    // Trigger StartAssociationsOnce
    let req = make_request(
        "StartAssociationsOnce",
        json!({ "AssociationIds": [assoc_id] }),
    );
    svc.start_associations_once(&req).unwrap();

    // Verify status flipped to Pending and last_execution_date is set
    let req = make_request("DescribeAssociation", json!({ "AssociationId": assoc_id }));
    let resp = svc.describe_association(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let desc = &body["AssociationDescription"];
    assert_eq!(desc["Status"]["Name"], "Pending");
    assert!(desc["LastExecutionDate"].is_number());
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ops_item_id = body["OpsItemId"].as_str().unwrap().to_string();

    // Get
    let req = make_request("GetOpsItem", json!({ "OpsItemId": ops_item_id }));
    let resp = svc.get_ops_item(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["OpsItem"]["Title"].as_str().unwrap(),
        "Updated OpsItem"
    );
    assert_eq!(body["OpsItem"]["Status"].as_str().unwrap(), "Resolved");

    // Describe
    let req = make_request("DescribeOpsItems", json!({}));
    let resp = svc.describe_ops_items(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let policy_id = body["PolicyId"].as_str().unwrap().to_string();
    let policy_hash = body["PolicyHash"].as_str().unwrap().to_string();

    // Get
    let req = make_request(
        "GetResourcePolicies",
        json!({ "ResourceArn": resource_arn }),
    );
    let resp = svc.get_resource_policies(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Policies"].as_array().unwrap().is_empty());
}

// -- Synthetic defaults + service settings --

#[test]
fn get_connection_status_returns_connected() {
    let svc = make_service();
    let req = make_request(
        "GetConnectionStatus",
        json!({ "Target": "i-1234567890abcdef0" }),
    );
    let resp = svc.get_connection_status(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["BaselineId"].is_string());
}

#[test]
fn describe_available_patches_returns_empty() {
    let svc = make_service();
    let req = make_request("DescribeAvailablePatches", json!({}));
    let resp = svc.describe_available_patches(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Patches"].as_array().unwrap().is_empty());
}

#[test]
fn describe_patch_properties_windows_products() {
    let svc = make_service();
    let req = make_request(
        "DescribePatchProperties",
        json!({ "OperatingSystem": "WINDOWS", "Property": "PRODUCT" }),
    );
    let resp = svc.describe_patch_properties(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let props = body["Properties"].as_array().unwrap();
    assert!(!props.is_empty());
    assert!(props.iter().any(|p| p["PRODUCT"] == "Windows10"));
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Entries"].as_array().unwrap().len(), 2);
    assert_eq!(body["TypeName"].as_str().unwrap(), "AWS:Application");

    // GetInventorySchema
    let req = make_request("GetInventorySchema", json!({}));
    let resp = svc.get_inventory_schema(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(!body["Schemas"].as_array().unwrap().is_empty());

    // DeleteInventory
    let req = make_request("DeleteInventory", json!({ "TypeName": "AWS:Application" }));
    let resp = svc.delete_inventory(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["DeletionId"].is_string());

    // DescribeInventoryDeletions
    let req = make_request("DescribeInventoryDeletions", json!({}));
    let resp = svc.describe_inventory_deletions(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ComplianceItems"].as_array().unwrap().len(), 2);

    // ListComplianceSummaries
    let req = make_request("ListComplianceSummaries", json!({}));
    let resp = svc.list_compliance_summaries(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ComplianceSummaryItems"].as_array().unwrap().len(), 1);

    // ListResourceComplianceSummaries
    let req = make_request("ListResourceComplianceSummaries", json!({}));
    let resp = svc.list_resource_compliance_summaries(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("123456789012");
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["WindowExecutions"].as_array().unwrap().len(), 1);

    // GetMaintenanceWindowExecution
    let req = make_request(
        "GetMaintenanceWindowExecution",
        json!({ "WindowExecutionId": exec_id }),
    );
    let resp = svc.get_maintenance_window_execution(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Status"].as_str().unwrap(), "IN_PROGRESS");

    // DescribeMaintenanceWindowExecutionTasks
    let req = make_request(
        "DescribeMaintenanceWindowExecutionTasks",
        json!({ "WindowExecutionId": exec_id }),
    );
    let resp = svc
        .describe_maintenance_window_execution_tasks(&req)
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ExecutionId"].as_str().unwrap(), "cmd-001");

    // CancelMaintenanceWindowExecution
    let req = make_request(
        "CancelMaintenanceWindowExecution",
        json!({ "WindowExecutionId": exec_id }),
    );
    let resp = svc.cancel_maintenance_window_execution(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["WindowExecutionId"].as_str().unwrap(), exec_id);

    // DescribeMaintenanceWindowSchedule
    let req = make_request("DescribeMaintenanceWindowSchedule", json!({}));
    let resp = svc.describe_maintenance_window_schedule(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["ResourceDataSyncItems"].as_array().unwrap().is_empty());
}

// ── Patch describes ───────────────────────────────────────────

#[test]
fn describe_instance_patch_states_returns_empty_for_unknown_instance() {
    let svc = make_service();
    let req = make_request(
        "DescribeInstancePatchStates",
        json!({ "InstanceIds": ["i-001"] }),
    );
    let resp = svc.describe_instance_patch_states(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["InstancePatchStates"].as_array().unwrap().is_empty());
}

#[test]
fn describe_instance_patches_returns_empty_for_unknown_instance() {
    let svc = make_service();
    let req = make_request("DescribeInstancePatches", json!({ "InstanceId": "i-001" }));
    let resp = svc.describe_instance_patches(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Patches"].as_array().unwrap().is_empty());
}

#[test]
fn describe_effective_patches_for_unknown_baseline_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeEffectivePatchesForPatchBaseline",
        json!({ "BaselineId": "pb-12345678901234567" }),
    );
    assert!(svc
        .describe_effective_patches_for_patch_baseline(&req)
        .is_err());
}

#[test]
fn describe_effective_patches_returns_approved_for_real_baseline() {
    let svc = make_service();
    let req = make_request(
        "CreatePatchBaseline",
        json!({
            "Name": "real-baseline",
            "OperatingSystem": "WINDOWS",
            "ApprovedPatches": ["KB12345", "KB67890"],
            "ApprovedPatchesComplianceLevel": "CRITICAL",
        }),
    );
    let resp = svc.create_patch_baseline(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let baseline_id = body["BaselineId"].as_str().unwrap().to_string();

    let req = make_request(
        "DescribeEffectivePatchesForPatchBaseline",
        json!({ "BaselineId": baseline_id }),
    );
    let resp = svc
        .describe_effective_patches_for_patch_baseline(&req)
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let patches = body["EffectivePatches"].as_array().unwrap();
    assert_eq!(patches.len(), 2);
    assert_eq!(patches[0]["PatchStatus"]["ComplianceLevel"], "CRITICAL");
}

#[test]
fn describe_patch_properties_returns_known_classifications() {
    let svc = make_service();
    let req = make_request(
        "DescribePatchProperties",
        json!({ "OperatingSystem": "WINDOWS", "Property": "CLASSIFICATION" }),
    );
    let resp = svc.describe_patch_properties(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let props = body["Properties"].as_array().unwrap();
    assert!(!props.is_empty());
    assert!(props
        .iter()
        .any(|p| p["CLASSIFICATION"] == "SecurityUpdates"));
}

#[test]
fn describe_instance_patches_surfaces_inventory_compliance() {
    let svc = make_service();
    let req = make_request(
        "PutInventory",
        json!({
            "InstanceId": "i-002",
            "Items": [{
                "TypeName": "AWS:PatchCompliance",
                "SchemaVersion": "1.0",
                "CaptureTime": "2026-04-25T20:00:00Z",
                "Content": [
                    {
                        "Title": "KB987654",
                        "KBId": "KB987654",
                        "Classification": "SecurityUpdates",
                        "Severity": "Critical",
                        "State": "Installed",
                        "InstalledTime": "2026-04-25T19:00:00Z"
                    }
                ]
            }]
        }),
    );
    svc.put_inventory(&req).unwrap();

    let req = make_request("DescribeInstancePatches", json!({ "InstanceId": "i-002" }));
    let resp = svc.describe_instance_patches(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let patches = body["Patches"].as_array().unwrap();
    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0]["KBId"], "KB987654");
    assert_eq!(patches[0]["Classification"], "SecurityUpdates");
}

#[test]
fn get_ops_summary_returns_empty() {
    let svc = make_service();
    let req = make_request("GetOpsSummary", json!({}));
    let resp = svc.get_ops_summary(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let assoc_id = body["AssociationId"].as_str().unwrap().to_string();

    // List
    let req = make_request(
        "ListOpsItemRelatedItems",
        json!({ "OpsItemId": ops_item_id }),
    );
    let resp = svc.list_ops_item_related_items(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let arn = body["OpsMetadataArn"].as_str().unwrap().to_string();

    // Get
    let req = make_request("GetOpsMetadata", json!({ "OpsMetadataArn": arn }));
    let resp = svc.get_ops_metadata(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let exec_id = body["AutomationExecutionId"].as_str().unwrap().to_string();

    // Get
    let req = make_request(
        "GetAutomationExecution",
        json!({ "AutomationExecutionId": exec_id }),
    );
    let resp = svc.get_automation_execution(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["AutomationExecution"]["AutomationExecutionStatus"]
            .as_str()
            .unwrap(),
        "InProgress"
    );

    // Describe
    let req = make_request("DescribeAutomationExecutions", json!({}));
    let resp = svc.describe_automation_executions(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let preview_id = body["ExecutionPreviewId"].as_str().unwrap().to_string();

    let req = make_request(
        "GetExecutionPreview",
        json!({ "ExecutionPreviewId": preview_id }),
    );
    let resp = svc.get_execution_preview(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Status"].as_str().unwrap(), "Success");
}

// ── Sessions ──────────────────────────────────────────────────

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Ok(_) => panic!("expected an error, got Ok"),
        Err(e) => e,
    }
}

#[test]
fn start_session_returns_target_not_connected_without_echo_mode() {
    // Belt-and-braces in case a parallel test polluted the env. The
    // module guards `serial_test` aren't on the workspace dev-deps yet,
    // so we just remove and verify the default path.
    std::env::remove_var("FAKECLOUD_SSM_SESSION_ECHO");

    let svc = make_service();
    let req = make_request("StartSession", json!({ "Target": "i-001" }));
    let err = expect_err(svc.start_session(&req));
    // We use `TargetNotConnected` because it's declared on StartSession in
    // the SSM Smithy model and it's the most honest reading of fakecloud's
    // state: there is no SSM agent attached to the target.
    assert_eq!(err.status(), http::StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "TargetNotConnected");
    // The error message must point at the documented escape hatches.
    assert!(err.message().contains("FAKECLOUD_SSM_SESSION_ECHO"));
    assert!(err.message().contains("/_fakecloud/ssm/sessions/inject"));
}

#[test]
fn resume_session_returns_does_not_exist_without_echo_mode() {
    std::env::remove_var("FAKECLOUD_SSM_SESSION_ECHO");

    let svc = make_service();
    let req = make_request(
        "ResumeSession",
        json!({ "SessionId": "session-000000000001" }),
    );
    let err = expect_err(svc.resume_session(&req));
    // ResumeSession declares `DoesNotExistException`; outside echo mode the
    // session doesn't exist on any data plane, so this is the honest code.
    assert_eq!(err.status(), http::StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "DoesNotExistException");
}

#[test]
fn injected_session_round_trips_describe_and_terminate() {
    // Default (no echo mode): admin inject + DescribeSessions + Terminate
    // remains the supported flow.
    std::env::remove_var("FAKECLOUD_SSM_SESSION_ECHO");

    let svc = make_service();

    let session_id = svc.inject_session(
        "123456789012",
        "i-injected",
        None,
        None,
        Some("admin-test"),
        None,
    );
    assert!(session_id.starts_with("session-"));

    // Describe (Active) sees it.
    let req = make_request("DescribeSessions", json!({ "State": "Active" }));
    let resp = svc.describe_sessions(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Sessions"].as_array().unwrap().len(), 1);
    assert_eq!(body["Sessions"][0]["Reason"].as_str(), Some("admin-test"));

    // Terminate is allowed even without echo mode.
    let req = make_request("TerminateSession", json!({ "SessionId": session_id }));
    svc.terminate_session(&req).unwrap();

    let req = make_request("DescribeSessions", json!({ "State": "History" }));
    let resp = svc.describe_sessions(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Sessions"].as_array().unwrap().len(), 1);
}

#[test]
fn echo_mode_full_lifecycle() {
    // Drive the legacy lifecycle path with echo mode flipped on. We have
    // to set + unset the env var inline since this test mutates global
    // process state; the explicit `remove_var` calls in the default-mode
    // tests above keep them isolated regardless of run order.
    std::env::set_var("FAKECLOUD_SSM_SESSION_ECHO", "1");

    let svc = make_service();

    // Start
    let req = make_request("StartSession", json!({ "Target": "i-001" }));
    let resp = svc.start_session(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let session_id = body["SessionId"].as_str().unwrap().to_string();
    // Echo mode must mark the token with the sentinel so callers can't
    // mistake it for a real websocket handshake.
    assert_eq!(
        body["TokenValue"].as_str(),
        Some("fakecloud-echo-mode-not-real-websocket")
    );

    // Resume
    let req = make_request("ResumeSession", json!({ "SessionId": session_id.clone() }));
    let resp = svc.resume_session(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SessionId"].as_str().unwrap(), session_id);
    assert_eq!(
        body["TokenValue"].as_str(),
        Some("fakecloud-echo-mode-not-real-websocket")
    );

    // Describe (Active)
    let req = make_request("DescribeSessions", json!({ "State": "Active" }));
    let resp = svc.describe_sessions(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Sessions"].as_array().unwrap().len(), 1);

    // Terminate
    let req = make_request("TerminateSession", json!({ "SessionId": session_id }));
    svc.terminate_session(&req).unwrap();

    // Describe (History)
    let req = make_request("DescribeSessions", json!({ "State": "History" }));
    let resp = svc.describe_sessions(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Sessions"].as_array().unwrap().len(), 1);

    std::env::remove_var("FAKECLOUD_SSM_SESSION_ECHO");
}

#[test]
fn start_access_request_and_get_token() {
    let svc = make_service();

    let req = make_request(
        "StartAccessRequest",
        json!({ "Reason": "test", "Targets": [{"Key": "InstanceIds", "Values": ["i-001"]}] }),
    );
    let resp = svc.start_access_request(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ar_id = body["AccessRequestId"].as_str().unwrap().to_string();

    let req = make_request("GetAccessToken", json!({ "AccessRequestId": ar_id }));
    let resp = svc.get_access_token(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let activation_id = body["ActivationId"].as_str().unwrap().to_string();
    assert!(body["ActivationCode"].as_str().is_some());

    // Describe
    let req = make_request("DescribeActivations", json!({}));
    let resp = svc.describe_activations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["InstanceProperties"].as_array().unwrap().is_empty());
}

// ── Other ─────────────────────────────────────────────────────

#[test]
fn list_nodes_returns_empty() {
    let svc = make_service();
    let req = make_request("ListNodes", json!({}));
    let resp = svc.list_nodes(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["InvalidLabels"].as_array().unwrap().is_empty());
    assert_eq!(body["ParameterVersion"].as_i64().unwrap(), 1);

    // Get parameter history — version 1 should have labels
    let req = make_request("GetParameterHistory", json!({ "Name": "/label/test" }));
    let resp = svc.get_parameter_history(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let invalid = body["InvalidLabels"].as_array().unwrap();
    assert_eq!(invalid.len(), 1);
    assert_eq!(invalid[0].as_str().unwrap(), "nonexistent");
}

#[test]
fn unlabel_parameter_version_requires_parameter_version() {
    let svc = make_service();
    put_param(&svc, "/label/reqver", "v1");

    // Omit ParameterVersion — should fail with ValidationException
    let req = make_request(
        "UnlabelParameterVersion",
        json!({
            "Name": "/label/reqver",
            "Labels": ["some-label"],
        }),
    );
    let result = svc.unlabel_parameter_version(&req);
    assert!(result.is_err());
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let desc = &body["DocumentDescription"];
    assert_eq!(desc["DocumentVersion"].as_str().unwrap(), "2");
    assert_eq!(desc["VersionName"].as_str().unwrap(), "release-2");

    // List document versions
    let req = make_request("ListDocumentVersions", json!({ "Name": "TestDoc" }));
    let resp = svc.list_document_versions(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Description"]["DefaultVersion"].as_str().unwrap(), "2");

    // Verify describe_document now shows version 2 as default
    let req = make_request("DescribeDocument", json!({ "Name": "TestDoc" }));
    let resp = svc.describe_document(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let targets = body["Targets"].as_array().unwrap();
    assert_eq!(targets.len(), 1);
    assert_eq!(targets[0]["Name"].as_str().unwrap(), "test-target");

    // Describe tasks
    let req = make_request(
        "DescribeMaintenanceWindowTasks",
        json!({ "WindowId": window_id }),
    );
    let resp = svc.describe_maintenance_window_tasks(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["BaselineId"].as_str().unwrap().to_string()
}

#[test]
fn patch_baseline_get_and_delete() {
    let svc = make_service();
    let baseline_id = create_baseline(&svc, "get-del-baseline");

    // Get
    let req = make_request("GetPatchBaseline", json!({ "BaselineId": baseline_id }));
    let resp = svc.get_patch_baseline(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["BaselineId"].as_str().unwrap(), baseline_id);

    // Describe patch groups
    let req = make_request("DescribePatchGroups", json!({}));
    let resp = svc.describe_patch_groups(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Mappings"].as_array().unwrap().is_empty());
}

// ── Command Details ──────────────────────────────────────────

#[test]
fn get_command_invocation_success() {
    let svc = make_service();
    let cmd_id = send_command(&svc, "AWS-RunShellScript");

    // Without a tokio runtime the async transition task isn't
    // spawned, so the invocation stays at `Pending` and exposes the
    // friendly StatusDetails. The async transition is exercised in
    // the E2E suite where the runtime is real.
    let req = make_request(
        "GetCommandInvocation",
        json!({
            "CommandId": cmd_id,
            "InstanceId": "i-1234567890abcdef0",
        }),
    );
    let resp = svc.get_command_invocation(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["CommandId"].as_str().unwrap(), cmd_id);
    assert_eq!(body["InstanceId"].as_str().unwrap(), "i-1234567890abcdef0");
    assert_eq!(body["Status"].as_str().unwrap(), "Pending");
    assert_eq!(body["StatusDetails"].as_str().unwrap(), "Pending");
    assert_eq!(body["ResponseCode"].as_i64().unwrap(), -1);
}

#[test]
fn get_command_invocation_reflects_admin_force_success() {
    let svc = make_service();
    let cmd_id = send_command(&svc, "AWS-RunShellScript");

    // Admin override flips the lifecycle into Success without waiting
    // on the spawned transition task; per-invocation StatusDetails
    // matches AWS's friendlier human-facing string.
    assert!(svc.set_command_status("123456789012", &cmd_id, "Success"));

    let req = make_request(
        "GetCommandInvocation",
        json!({
            "CommandId": cmd_id,
            "InstanceId": "i-1234567890abcdef0",
        }),
    );
    let resp = svc.get_command_invocation(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Status"].as_str().unwrap(), "Success");
    assert_eq!(body["StatusDetails"].as_str().unwrap(), "Success");
    assert_eq!(body["ResponseCode"].as_i64().unwrap(), 0);
}

#[test]
fn fail_command_invocation_marks_single_instance() {
    let svc = make_service();
    // Two-instance command so we can verify the per-instance flag flow.
    let req = make_request(
        "SendCommand",
        json!({
            "DocumentName": "AWS-RunShellScript",
            "InstanceIds": ["i-1111111111111111a", "i-2222222222222222b"],
        }),
    );
    let resp = svc.send_command(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let cmd_id = body["Command"]["CommandId"].as_str().unwrap().to_string();

    let updated = svc.fail_command_invocation(
        "123456789012",
        &cmd_id,
        Some("i-1111111111111111a"),
        Some("Script exited with code 7"),
        Some("boom"),
    );
    assert_eq!(updated, 1);

    let req = make_request(
        "GetCommandInvocation",
        json!({
            "CommandId": cmd_id,
            "InstanceId": "i-1111111111111111a",
        }),
    );
    let resp = svc.get_command_invocation(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Status"].as_str().unwrap(), "Failed");
    assert_eq!(
        body["StatusDetails"].as_str().unwrap(),
        "Script exited with code 7"
    );
    assert_eq!(body["StandardErrorContent"].as_str().unwrap(), "boom");
    assert_eq!(body["ResponseCode"].as_i64().unwrap(), 1);

    // The other instance is untouched.
    let req = make_request(
        "GetCommandInvocation",
        json!({
            "CommandId": cmd_id,
            "InstanceId": "i-2222222222222222b",
        }),
    );
    let resp = svc.get_command_invocation(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Status"].as_str().unwrap(), "Pending");

    // Aggregate command status surfaces the failure.
    let req = make_request("ListCommands", json!({"CommandId": cmd_id}));
    let resp = svc.list_commands(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Commands"][0]["Status"].as_str().unwrap(), "Failed");
}

#[test]
fn fail_command_invocation_unknown_command_returns_zero() {
    let svc = make_service();
    let updated = svc.fail_command_invocation("123456789012", "no-such-id", None, None, None);
    assert_eq!(updated, 0);
}

#[test]
fn send_command_starts_pending() {
    let svc = make_service();
    let cmd_id = send_command(&svc, "AWS-RunShellScript");
    let req = make_request("ListCommands", json!({"CommandId": cmd_id}));
    let resp = svc.list_commands(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["Commands"][0]["Status"].as_str().unwrap(),
        "Pending",
        "SendCommand returns Pending until first poll advances it"
    );
}

#[test]
fn set_command_status_overrides_lifecycle() {
    let svc = make_service();
    let cmd_id = send_command(&svc, "AWS-RunShellScript");
    // Force a Failed status. ListCommands and GetCommandInvocation
    // both pick up the override since they read the live state.
    let updated = svc.set_command_status("123456789012", &cmd_id, "Failed");
    assert!(updated);
    let req = make_request(
        "GetCommandInvocation",
        json!({
            "CommandId": cmd_id,
            "InstanceId": "i-1234567890abcdef0",
        }),
    );
    let resp = svc.get_command_invocation(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Status"].as_str().unwrap(), "Failed");
    assert_eq!(body["StatusDetails"].as_str().unwrap(), "Failed");

    let req = make_request("ListCommands", json!({"CommandId": cmd_id}));
    let resp = svc.list_commands(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Commands"][0]["Status"].as_str().unwrap(), "Failed");
}

#[test]
fn set_command_status_unknown_id_returns_false() {
    let svc = make_service();
    assert!(!svc.set_command_status("123456789012", "no-such-id", "Failed"));
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
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
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["CommandInvocations"].as_array().unwrap().len(), 1);
}

// ── Parameter store handlers ────────────────────────────────────
//
// (`put_param` defined earlier in this module already uses
// `Overwrite: true`, so it's reused here. For tests that need the
// non-overwrite path, the `PutParameter` request is built inline.)

fn put_simple_string(svc: &SsmService, name: &str, value: &str) {
    let req = make_request(
        "PutParameter",
        json!({ "Name": name, "Value": value, "Type": "String" }),
    );
    svc.put_parameter(&req).unwrap();
}

fn json_body(resp: AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

#[test]
fn put_parameter_creates_with_version_1() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({ "Name": "/app/db/url", "Value": "postgres://...", "Type": "String" }),
    );
    let body = json_body(svc.put_parameter(&req).unwrap());
    assert_eq!(body["Version"], json!(1));
}

#[test]
fn put_parameter_duplicate_without_overwrite_errors() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v1");
    let req = make_request(
        "PutParameter",
        json!({ "Name": "/a", "Value": "v2", "Type": "String" }),
    );
    let err = svc.put_parameter(&req).err().expect("expected error");
    assert_eq!(err.code(), "ParameterAlreadyExists");
}

#[test]
fn put_parameter_overwrite_bumps_version() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v1");
    let req = make_request(
        "PutParameter",
        json!({ "Name": "/a", "Value": "v2", "Type": "String", "Overwrite": true }),
    );
    let body = json_body(svc.put_parameter(&req).unwrap());
    assert_eq!(body["Version"], json!(2));
}

#[test]
fn get_parameter_returns_latest_version() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v1");
    let req = make_request("GetParameter", json!({ "Name": "/a" }));
    let body = json_body(svc.get_parameter(&req).unwrap());
    assert_eq!(body["Parameter"]["Name"], json!("/a"));
    assert_eq!(body["Parameter"]["Value"], json!("v1"));
    assert_eq!(body["Parameter"]["Version"], json!(1));
}

#[test]
fn get_parameter_unknown_errors() {
    let svc = make_service();
    let req = make_request("GetParameter", json!({ "Name": "/ghost" }));
    let err = svc.get_parameter(&req).err().expect("expected error");
    assert_eq!(err.code(), "ParameterNotFound");
}

#[test]
fn get_parameter_missing_name_errors() {
    let svc = make_service();
    let req = make_request("GetParameter", json!({}));
    assert!(svc.get_parameter(&req).is_err());
}

#[test]
fn get_parameters_returns_found_and_invalid_lists() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v1");
    put_simple_string(&svc, "/b", "v2");
    let req = make_request(
        "GetParameters",
        json!({ "Names": ["/a", "/b", "/missing"] }),
    );
    let body = json_body(svc.get_parameters(&req).unwrap());
    let params = body["Parameters"].as_array().unwrap();
    let names: Vec<&str> = params.iter().map(|p| p["Name"].as_str().unwrap()).collect();
    assert!(names.contains(&"/a"));
    assert!(names.contains(&"/b"));
    let invalid = body["InvalidParameters"].as_array().unwrap();
    assert_eq!(invalid.len(), 1);
    assert_eq!(invalid[0], json!("/missing"));
}

#[test]
fn get_parameters_by_path_filters_prefix() {
    let svc = make_service();
    put_simple_string(&svc, "/app/db/url", "u");
    put_simple_string(&svc, "/app/db/user", "x");
    put_simple_string(&svc, "/app/cache/host", "h");
    let req = make_request(
        "GetParametersByPath",
        json!({ "Path": "/app/db/", "Recursive": false }),
    );
    let body = json_body(svc.get_parameters_by_path(&req).unwrap());
    let params = body["Parameters"].as_array().unwrap();
    assert_eq!(params.len(), 2);
    for p in params {
        assert!(p["Name"].as_str().unwrap().starts_with("/app/db/"));
    }
}

#[test]
fn get_parameters_by_path_recursive_finds_nested() {
    let svc = make_service();
    put_simple_string(&svc, "/app/a/x", "1");
    put_simple_string(&svc, "/app/b/c/y", "2");
    let req = make_request(
        "GetParametersByPath",
        json!({ "Path": "/app", "Recursive": true }),
    );
    let body = json_body(svc.get_parameters_by_path(&req).unwrap());
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 2);
}

#[test]
fn delete_parameter_removes_entry() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v1");
    let req = make_request("DeleteParameter", json!({ "Name": "/a" }));
    svc.delete_parameter(&req).unwrap();
    let get = svc.get_parameter(&make_request("GetParameter", json!({ "Name": "/a" })));
    assert!(get.is_err());
}

#[test]
fn delete_parameter_unknown_errors() {
    let svc = make_service();
    let req = make_request("DeleteParameter", json!({ "Name": "/ghost" }));
    let err = svc.delete_parameter(&req).err().expect("expected error");
    assert_eq!(err.code(), "ParameterNotFound");
}

#[test]
fn delete_parameters_partitions_into_deleted_and_invalid() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v");
    put_simple_string(&svc, "/b", "v");
    let req = make_request(
        "DeleteParameters",
        json!({ "Names": ["/a", "/b", "/ghost"] }),
    );
    let body = json_body(svc.delete_parameters(&req).unwrap());
    let deleted: Vec<&str> = body["DeletedParameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    let invalid: Vec<&str> = body["InvalidParameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(deleted.len(), 2);
    assert_eq!(invalid, vec!["/ghost"]);
}

#[test]
fn describe_parameters_lists_all() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v");
    put_simple_string(&svc, "/b", "v");
    let req = make_request("DescribeParameters", json!({}));
    let body = json_body(svc.describe_parameters(&req).unwrap());
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 2);
}

#[test]
fn get_parameter_history_lists_all_versions_after_overwrite() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v1");
    let ow = make_request(
        "PutParameter",
        json!({ "Name": "/a", "Value": "v2", "Type": "String", "Overwrite": true }),
    );
    svc.put_parameter(&ow).unwrap();
    let req = make_request("GetParameterHistory", json!({ "Name": "/a" }));
    let body = json_body(svc.get_parameter_history(&req).unwrap());
    let params = body["Parameters"].as_array().unwrap();
    assert_eq!(params.len(), 2);
    let versions: Vec<i64> = params
        .iter()
        .map(|p| p["Version"].as_i64().unwrap())
        .collect();
    assert!(versions.contains(&1));
    assert!(versions.contains(&2));
}

#[test]
fn get_parameter_history_unknown_errors() {
    let svc = make_service();
    let req = make_request("GetParameterHistory", json!({ "Name": "/ghost" }));
    assert!(svc.get_parameter_history(&req).is_err());
}

#[test]
fn label_parameter_version_attaches_label_to_version() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v1");
    let req = make_request(
        "LabelParameterVersion",
        json!({
            "Name": "/a",
            "ParameterVersion": 1,
            "Labels": ["prod"]
        }),
    );
    let body = json_body(svc.label_parameter_version(&req).unwrap());
    let invalid = body["InvalidLabels"].as_array().unwrap();
    assert!(invalid.is_empty());
}

#[test]
fn unlabel_parameter_version_removes_label() {
    let svc = make_service();
    put_simple_string(&svc, "/a", "v1");
    svc.label_parameter_version(&make_request(
        "LabelParameterVersion",
        json!({ "Name": "/a", "ParameterVersion": 1, "Labels": ["prod", "stable"] }),
    ))
    .unwrap();
    let req = make_request(
        "UnlabelParameterVersion",
        json!({ "Name": "/a", "ParameterVersion": 1, "Labels": ["prod"] }),
    );
    let body = json_body(svc.unlabel_parameter_version(&req).unwrap());
    let removed = body["RemovedLabels"].as_array().unwrap();
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0], json!("prod"));
}

#[test]
fn put_parameter_missing_name_errors() {
    let svc = make_service();
    let req = make_request("PutParameter", json!({ "Value": "v", "Type": "String" }));
    assert!(svc.put_parameter(&req).is_err());
}

#[test]
fn put_parameter_invalid_type_errors() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({ "Name": "/a", "Value": "v", "Type": "WatType" }),
    );
    assert!(svc.put_parameter(&req).is_err());
}

#[test]
fn param_arn_and_rewrite_region_helpers() {
    let arn = crate::service::parameters::param_arn("us-east-1", "123456789012", "/a/b");
    assert_eq!(arn, "arn:aws:ssm:us-east-1:123456789012:parameter/a/b");
    let rewritten = crate::service::parameters::rewrite_arn_region(&arn, "eu-west-1");
    assert_eq!(
        rewritten,
        "arn:aws:ssm:eu-west-1:123456789012:parameter/a/b"
    );
}

#[test]
fn parse_param_selector_extracts_version_and_label() {
    use crate::service::parameters::ParamSelector;
    let (name, sel) = crate::service::parameters::parse_param_selector("/a/b:42");
    assert_eq!(name, "/a/b");
    assert!(matches!(sel, ParamSelector::Version(42)));

    let (name, sel) = crate::service::parameters::parse_param_selector("/a/b:prod");
    assert_eq!(name, "/a/b");
    assert!(matches!(sel, ParamSelector::Label(ref l) if l == "prod"));

    let (name, sel) = crate::service::parameters::parse_param_selector("/a/b");
    assert_eq!(name, "/a/b");
    assert!(matches!(sel, ParamSelector::None));
}

fn expect_err_code(result: Result<AwsResponse, AwsServiceError>, code: &str) {
    match result {
        Err(e) => assert!(e.to_string().contains(code), "expected {code}, got: {e}"),
        Ok(_) => panic!("expected error {code}, got Ok"),
    }
}

// ── Parameter error branches ──

#[test]
fn put_parameter_missing_name() {
    let svc = make_service();
    let req = make_request("PutParameter", json!({"Value": "v", "Type": "String"}));
    expect_err_code(svc.put_parameter(&req), "InvalidAllowedPatternException");
}

#[test]
fn put_parameter_missing_value() {
    let svc = make_service();
    let req = make_request("PutParameter", json!({"Name": "/test", "Type": "String"}));
    expect_err_code(svc.put_parameter(&req), "InvalidAllowedPatternException");
}

#[test]
fn put_parameter_missing_type() {
    let svc = make_service();
    let req = make_request("PutParameter", json!({"Name": "/test", "Value": "v"}));
    expect_err_code(svc.put_parameter(&req), "InvalidAllowedPatternException");
}

#[test]
fn put_parameter_overwrite_increments_version() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({"Name": "/versioned", "Value": "v1", "Type": "String"}),
    );
    let resp = svc.put_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Version"], 1);

    let req = make_request(
        "PutParameter",
        json!({"Name": "/versioned", "Value": "v2", "Type": "String", "Overwrite": true}),
    );
    let resp = svc.put_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Version"], 2);
}

#[test]
fn put_parameter_already_exists_without_overwrite() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({"Name": "/exists", "Value": "v1", "Type": "String"}),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request(
        "PutParameter",
        json!({"Name": "/exists", "Value": "v2", "Type": "String"}),
    );
    expect_err_code(svc.put_parameter(&req), "ParameterAlreadyExists");
}

#[test]
fn get_parameter_not_found() {
    let svc = make_service();
    let req = make_request("GetParameter", json!({"Name": "/nonexistent"}));
    expect_err_code(svc.get_parameter(&req), "ParameterNotFound");
}

#[test]
fn get_parameters_returns_invalid_for_missing() {
    let svc = make_service();
    let req = make_request(
        "GetParameters",
        json!({"Names": ["/missing1", "/missing2"]}),
    );
    let resp = svc.get_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let invalid = body["InvalidParameters"].as_array().unwrap();
    assert_eq!(invalid.len(), 2);
}

#[test]
fn delete_parameter_not_found() {
    let svc = make_service();
    let req = make_request("DeleteParameter", json!({"Name": "/gone"}));
    expect_err_code(svc.delete_parameter(&req), "ParameterNotFound");
}

#[test]
fn delete_parameters_returns_invalid_for_missing() {
    let svc = make_service();
    let req = make_request("DeleteParameters", json!({"Names": ["/miss1", "/miss2"]}));
    let resp = svc.delete_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let invalid = body["InvalidParameters"].as_array().unwrap();
    assert_eq!(invalid.len(), 2);
}

#[test]
fn get_parameter_history_not_found() {
    let svc = make_service();
    let req = make_request("GetParameterHistory", json!({"Name": "/nope"}));
    expect_err_code(svc.get_parameter_history(&req), "ParameterNotFound");
}

#[test]
fn label_parameter_version_missing_param() {
    let svc = make_service();
    let req = make_request(
        "LabelParameterVersion",
        json!({"Name": "/nope", "Labels": ["prod"], "ParameterVersion": 1}),
    );
    expect_err_code(svc.label_parameter_version(&req), "ParameterNotFound");
}

#[test]
fn unlabel_parameter_version_missing_param() {
    let svc = make_service();
    let req = make_request(
        "UnlabelParameterVersion",
        json!({"Name": "/nope", "Labels": ["prod"], "ParameterVersion": 1}),
    );
    expect_err_code(svc.unlabel_parameter_version(&req), "ParameterNotFound");
}

#[test]
fn get_parameters_by_path_invalid_path() {
    let svc = make_service();
    let req = make_request("GetParametersByPath", json!({"Path": "no-leading-slash"}));
    assert!(svc.get_parameters_by_path(&req).is_err());
}

// ── Document error branches ──

#[test]
fn get_document_not_found() {
    let svc = make_service();
    let req = make_request("GetDocument", json!({"Name": "nonexistent-doc"}));
    expect_err_code(svc.get_document(&req), "InvalidDocument");
}

#[test]
fn update_document_not_found() {
    let svc = make_service();
    let req = make_request(
        "UpdateDocument",
        json!({"Name": "nope", "Content": "{}", "DocumentVersion": "$LATEST"}),
    );
    expect_err_code(svc.update_document(&req), "InvalidDocument");
}

#[test]
fn delete_document_not_found() {
    let svc = make_service();
    let req = make_request("DeleteDocument", json!({"Name": "nope"}));
    expect_err_code(svc.delete_document(&req), "InvalidDocument");
}

#[test]
fn describe_document_not_found() {
    let svc = make_service();
    let req = make_request("DescribeDocument", json!({"Name": "nope"}));
    expect_err_code(svc.describe_document(&req), "InvalidDocument");
}

// ── Maintenance window error branches ──

// ── Maintenance window — create and get lifecycle ──

#[test]
fn maintenance_window_create_and_get() {
    let svc = make_service();
    let req = make_request(
        "CreateMaintenanceWindow",
        json!({
            "Name": "test-window",
            "Schedule": "cron(0 2 * * ? *)",
            "Duration": 2,
            "Cutoff": 1,
            "AllowUnassociatedTargets": false,
        }),
    );
    let resp = svc.create_maintenance_window(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let window_id = body["WindowId"].as_str().unwrap().to_string();

    let req = make_request("GetMaintenanceWindow", json!({"WindowId": window_id}));
    let resp = svc.get_maintenance_window(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "test-window");
}

// ── Patch baseline — create and get lifecycle ──

#[test]
fn patch_baseline_create_and_get() {
    let svc = make_service();
    let req = make_request(
        "CreatePatchBaseline",
        json!({
            "Name": "test-baseline",
            "OperatingSystem": "AMAZON_LINUX_2",
        }),
    );
    let resp = svc.create_patch_baseline(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let baseline_id = body["BaselineId"].as_str().unwrap().to_string();

    let req = make_request("GetPatchBaseline", json!({"BaselineId": baseline_id}));
    let resp = svc.get_patch_baseline(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "test-baseline");
}

// ── Command — send and get invocation ──

#[test]
fn send_command_and_list() {
    let svc = make_service();
    // Create a document first
    let req = make_request(
        "CreateDocument",
        json!({
            "Name": "RunShellScript",
            "Content": "{\"schemaVersion\":\"2.2\",\"mainSteps\":[]}",
            "DocumentType": "Command",
        }),
    );
    svc.create_document(&req).unwrap();

    let req = make_request(
        "SendCommand",
        json!({
            "DocumentName": "RunShellScript",
            "InstanceIds": ["i-12345"],
        }),
    );
    let resp = svc.send_command(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Command"]["CommandId"].as_str().is_some());

    let req = make_request("ListCommands", json!({}));
    let resp = svc.list_commands(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(!body["Commands"].as_array().unwrap().is_empty());
}

// ── Automation error branches ──

#[test]
fn get_automation_execution_not_found() {
    let svc = make_service();
    let req = make_request(
        "GetAutomationExecution",
        json!({"AutomationExecutionId": "exec-nope"}),
    );
    expect_err_code(
        svc.get_automation_execution(&req),
        "AutomationExecutionNotFoundException",
    );
}

#[test]
fn stop_automation_execution_not_found() {
    let svc = make_service();
    let req = make_request(
        "StopAutomationExecution",
        json!({"AutomationExecutionId": "exec-nope"}),
    );
    expect_err_code(
        svc.stop_automation_execution(&req),
        "AutomationExecutionNotFoundException",
    );
}

// ── OpsItem error branches ──

#[test]
fn update_ops_item_not_found() {
    let svc = make_service();
    let req = make_request(
        "UpdateOpsItem",
        json!({"OpsItemId": "oi-nope", "Status": "Resolved"}),
    );
    expect_err_code(svc.update_ops_item(&req), "OpsItemNotFoundException");
}

// ── Tag operations (tags.rs - 0% coverage) ──

#[test]
fn add_and_list_tags_for_parameter() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({"Name": "/tagged/param", "Value": "v", "Type": "String"}),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request(
        "AddTagsToResource",
        json!({
            "ResourceType": "Parameter",
            "ResourceId": "/tagged/param",
            "Tags": [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "be"}],
        }),
    );
    svc.add_tags_to_resource(&req).unwrap();

    let req = make_request(
        "ListTagsForResource",
        json!({"ResourceType": "Parameter", "ResourceId": "/tagged/param"}),
    );
    let resp = svc.list_tags_for_resource(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TagList"].as_array().unwrap().len(), 2);
}

#[test]
fn remove_tags_from_parameter() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({"Name": "/rm-tag", "Value": "v", "Type": "String"}),
    );
    svc.put_parameter(&req).unwrap();

    svc.add_tags_to_resource(&make_request(
        "AddTagsToResource",
        json!({
            "ResourceType": "Parameter",
            "ResourceId": "/rm-tag",
            "Tags": [{"Key": "env", "Value": "prod"}],
        }),
    ))
    .unwrap();

    svc.remove_tags_from_resource(&make_request(
        "RemoveTagsFromResource",
        json!({"ResourceType": "Parameter", "ResourceId": "/rm-tag", "TagKeys": ["env"]}),
    ))
    .unwrap();

    let resp = svc
        .list_tags_for_resource(&make_request(
            "ListTagsForResource",
            json!({"ResourceType": "Parameter", "ResourceId": "/rm-tag"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["TagList"].as_array().unwrap().is_empty());
}

#[test]
fn tags_invalid_resource_type() {
    let svc = make_service();
    expect_err_code(
        svc.add_tags_to_resource(&make_request(
            "AddTagsToResource",
            json!({"ResourceType": "Bogus", "ResourceId": "x", "Tags": [{"Key": "k", "Value": "v"}]}),
        )),
        "InvalidResourceType",
    );
}

#[test]
fn tags_invalid_resource_id() {
    let svc = make_service();
    expect_err_code(
        svc.add_tags_to_resource(&make_request(
            "AddTagsToResource",
            json!({"ResourceType": "Parameter", "ResourceId": "/nonexistent", "Tags": [{"Key": "k", "Value": "v"}]}),
        )),
        "InvalidResourceId",
    );
}

#[test]
fn tags_on_document() {
    let svc = make_service();
    svc.create_document(&make_request(
        "CreateDocument",
        json!({"Name": "TagDoc", "Content": "{\"schemaVersion\":\"2.2\",\"mainSteps\":[]}", "DocumentType": "Command"}),
    ))
    .unwrap();

    svc.add_tags_to_resource(&make_request(
        "AddTagsToResource",
        json!({"ResourceType": "Document", "ResourceId": "TagDoc", "Tags": [{"Key": "p", "Value": "t"}]}),
    ))
    .unwrap();

    let resp = svc
        .list_tags_for_resource(&make_request(
            "ListTagsForResource",
            json!({"ResourceType": "Document", "ResourceId": "TagDoc"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TagList"].as_array().unwrap().len(), 1);
}

// ── Parameter deep coverage ──

#[test]
fn describe_parameters_with_filters() {
    let svc = make_service();
    for name in &["/app/db-host", "/app/db-port", "/other/key"] {
        svc.put_parameter(&make_request(
            "PutParameter",
            json!({"Name": name, "Value": "v", "Type": "String"}),
        ))
        .unwrap();
    }

    // Filter by path
    let req = make_request(
        "DescribeParameters",
        json!({"ParameterFilters": [{"Key": "Name", "Option": "BeginsWith", "Values": ["/app/"]}]}),
    );
    let resp = svc.describe_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 2);
}

#[test]
fn describe_parameters_empty() {
    let svc = make_service();
    let req = make_request("DescribeParameters", json!({}));
    let resp = svc.describe_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Parameters"].as_array().unwrap().is_empty());
}

#[test]
fn get_parameters_by_path_recursive() {
    let svc = make_service();
    for name in &["/a/b/c", "/a/b/d", "/a/e"] {
        svc.put_parameter(&make_request(
            "PutParameter",
            json!({"Name": name, "Value": "v", "Type": "String"}),
        ))
        .unwrap();
    }

    // Non-recursive: only direct children of /a
    let req = make_request(
        "GetParametersByPath",
        json!({"Path": "/a", "Recursive": false}),
    );
    let resp = svc.get_parameters_by_path(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 1); // only /a/e

    // Recursive: all under /a
    let req = make_request(
        "GetParametersByPath",
        json!({"Path": "/a", "Recursive": true}),
    );
    let resp = svc.get_parameters_by_path(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 3);
}

#[test]
fn label_unlabel_and_get_by_label() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/labeled", "Value": "v1", "Type": "String"}),
    ))
    .unwrap();

    // Label version 1
    let req = make_request(
        "LabelParameterVersion",
        json!({"Name": "/labeled", "Labels": ["prod"], "ParameterVersion": 1}),
    );
    svc.label_parameter_version(&req).unwrap();

    // Get by label
    let req = make_request("GetParameter", json!({"Name": "/labeled:prod"}));
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Value"], "v1");

    // Unlabel
    let req = make_request(
        "UnlabelParameterVersion",
        json!({"Name": "/labeled", "Labels": ["prod"], "ParameterVersion": 1}),
    );
    svc.unlabel_parameter_version(&req).unwrap();
}

#[test]
fn get_parameter_by_version() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/ver-param", "Value": "v1", "Type": "String"}),
    ))
    .unwrap();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/ver-param", "Value": "v2", "Type": "String", "Overwrite": true}),
    ))
    .unwrap();

    // Get version 1
    let req = make_request("GetParameter", json!({"Name": "/ver-param:1"}));
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Value"], "v1");

    // Get latest (version 2)
    let req = make_request("GetParameter", json!({"Name": "/ver-param"}));
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Value"], "v2");
}

#[test]
fn get_parameter_history_with_versions() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/hist", "Value": "v1", "Type": "String"}),
    ))
    .unwrap();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/hist", "Value": "v2", "Type": "String", "Overwrite": true}),
    ))
    .unwrap();

    let req = make_request("GetParameterHistory", json!({"Name": "/hist"}));
    let resp = svc.get_parameter_history(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let history = body["Parameters"].as_array().unwrap();
    assert_eq!(history.len(), 2);
}

#[test]
fn put_parameter_secure_string() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({"Name": "/secure", "Value": "secret", "Type": "SecureString"}),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request(
        "GetParameter",
        json!({"Name": "/secure", "WithDecryption": true}),
    );
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Type"], "SecureString");
}

#[test]
fn put_parameter_secure_string_hard_fails_when_kms_rejects() {
    use fakecloud_core::delivery::KmsHook;
    use std::collections::HashMap;

    struct AlwaysFailKms;
    impl KmsHook for AlwaysFailKms {
        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            _key_id: &str,
            _plaintext: &[u8],
            _service_principal: &str,
            _ec: HashMap<String, String>,
        ) -> Result<String, String> {
            Err("KMS unavailable".into())
        }
        fn decrypt(
            &self,
            _account_id: &str,
            _ciphertext_b64: &str,
            _service_principal: &str,
            _ec: HashMap<String, String>,
        ) -> Result<Vec<u8>, String> {
            Err("KMS unavailable".into())
        }
    }

    let svc = make_service().with_kms_hook(Arc::new(AlwaysFailKms));
    let req = make_request(
        "PutParameter",
        json!({"Name": "/secure-fail", "Value": "secret", "Type": "SecureString"}),
    );
    let err = match svc.put_parameter(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected InvalidKeyId (KMS encrypt failure)"),
    };
    // PutParameter's Smithy errors list doesn't include
    // KMSAccessDeniedException, so KMS encrypt failures surface as
    // InvalidKeyId (the declared shape covering key-related errors).
    assert_eq!(err.code(), "InvalidKeyId");
}

/// The tier/policy conflict must be rejected BEFORE any KMS encryption work,
/// so a SecureString + Standard-tier + Policies request surfaces the
/// validation error rather than the KMS/InvalidKeyId error.
#[test]
fn put_parameter_validates_tier_policy_before_kms() {
    use fakecloud_core::delivery::KmsHook;
    use std::collections::HashMap;

    struct AlwaysFailKms;
    impl KmsHook for AlwaysFailKms {
        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            _key_id: &str,
            _plaintext: &[u8],
            _service_principal: &str,
            _ec: HashMap<String, String>,
        ) -> Result<String, String> {
            Err("KMS unavailable".into())
        }
        fn decrypt(
            &self,
            _account_id: &str,
            _ciphertext_b64: &str,
            _service_principal: &str,
            _ec: HashMap<String, String>,
        ) -> Result<Vec<u8>, String> {
            Err("KMS unavailable".into())
        }
    }

    let svc = make_service().with_kms_hook(Arc::new(AlwaysFailKms));
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": "9999-01-01T00:00:00.000Z" }
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/secure-standard-policy",
            "Value": "secret",
            "Type": "SecureString",
            "Policies": policies
        }),
    );
    let err = match svc.put_parameter(&req) {
        Err(e) => e,
        Ok(_) => panic!("standard tier + policies must be rejected"),
    };
    // The validation error (remapped from ValidationException) must win over
    // the KMS InvalidKeyId error, proving validation runs first.
    assert_ne!(err.code(), "InvalidKeyId");
    assert!(
        err.message().contains("Advanced"),
        "expected the Advanced-tier validation message, got: {}",
        err.message()
    );
}

#[test]
fn put_parameter_string_list() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({"Name": "/list", "Value": "a,b,c", "Type": "StringList"}),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request("GetParameter", json!({"Name": "/list"}));
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Value"], "a,b,c");
}

#[test]
fn get_parameters_mixed_found_and_missing() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/exists", "Value": "v", "Type": "String"}),
    ))
    .unwrap();

    let req = make_request("GetParameters", json!({"Names": ["/exists", "/missing"]}));
    let resp = svc.get_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 1);
    assert_eq!(body["InvalidParameters"].as_array().unwrap().len(), 1);
}

#[test]
fn delete_parameters_mixed() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/del-mix", "Value": "v", "Type": "String"}),
    ))
    .unwrap();

    let req = make_request("DeleteParameters", json!({"Names": ["/del-mix", "/ghost"]}));
    let resp = svc.delete_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DeletedParameters"].as_array().unwrap().len(), 1);
    assert_eq!(body["InvalidParameters"].as_array().unwrap().len(), 1);
}

// ── parameters.rs filter validation coverage ──

#[test]
fn describe_parameters_invalid_filter_key_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [{"Key": "Bogus", "Values": ["x"]}]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_label_filter_rejected() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [{"Key": "Label", "Values": ["v1"]}]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_missing_values_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({"ParameterFilters": [{"Key": "Name"}]}),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_duplicate_filter_key_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Name", "Values": ["a"]},
                {"Key": "Name", "Values": ["b"]}
            ]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_path_invalid_option_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Path", "Option": "Equals", "Values": ["/foo"]}
            ]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_path_aws_prefix_rejected() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Path", "Values": ["/aws/secret"]}
            ]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_tier_invalid_value_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({"ParameterFilters": [{"Key": "Tier", "Values": ["Bogus"]}]}),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_type_invalid_value_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({"ParameterFilters": [{"Key": "Type", "Values": ["Bogus"]}]}),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_name_invalid_option_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Name", "Option": "EndsWith", "Values": ["x"]}
            ]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_key_length_exceeds() {
    let svc = make_service();
    let long_key = "k".repeat(140);
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": long_key, "Values": ["v"]}
            ]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_option_length_exceeds() {
    let svc = make_service();
    let long_opt = "x".repeat(20);
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Name", "Option": long_opt, "Values": ["v"]}
            ]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn describe_parameters_value_length_exceeds() {
    let svc = make_service();
    let long_val = "v".repeat(1100);
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Name", "Values": [long_val]}
            ]
        }),
    );
    expect_err_code(svc.describe_parameters(&req), "InvalidFilterKey");
}

#[test]
fn get_parameters_by_path_filters_disallow_name() {
    let svc = make_service();
    let req = make_request(
        "GetParametersByPath",
        json!({
            "Path": "/",
            "ParameterFilters": [
                {"Key": "Name", "Values": ["x"]}
            ]
        }),
    );
    expect_err_code(svc.get_parameters_by_path(&req), "InvalidFilterKey");
}

#[test]
fn get_parameters_by_path_missing_path_errors() {
    let svc = make_service();
    let req = make_request("GetParametersByPath", json!({}));
    expect_err_code(svc.get_parameters_by_path(&req), "InvalidFilterValue");
}

#[test]
fn put_parameter_securestring_encrypted_roundtrip() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/sec",
            "Value": "secret-value",
            "Type": "SecureString"
        }),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request(
        "GetParameter",
        json!({"Name": "/sec", "WithDecryption": true}),
    );
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Value"], "secret-value");
}

#[test]
fn put_parameter_stringlist_with_commas() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({"Name": "/list", "Value": "a,b,c", "Type": "StringList"}),
    );
    svc.put_parameter(&req).unwrap();
}

#[test]
fn get_parameter_by_arn_resolves() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/byarn", "Value": "v", "Type": "String"}),
    ))
    .unwrap();

    let arn = "arn:aws:ssm:us-east-1:123456789012:parameter/byarn";
    let req = make_request("GetParameter", json!({"Name": arn}));
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Name"], "/byarn");
}

#[test]
fn get_parameter_with_version_selector() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/verp", "Value": "v1", "Type": "String"}),
    ))
    .unwrap();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/verp", "Value": "v2", "Type": "String", "Overwrite": true}),
    ))
    .unwrap();

    let req = make_request("GetParameter", json!({"Name": "/verp:1"}));
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Value"], "v1");
}

#[test]
fn get_parameter_nonexistent_version_errors() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/gpnv", "Value": "v", "Type": "String"}),
    ))
    .unwrap();
    let req = make_request("GetParameter", json!({"Name": "/gpnv:99"}));
    expect_err_code(svc.get_parameter(&req), "ParameterVersionNotFound");
}

// ── documents.rs error branches ──

#[test]
fn create_document_missing_name_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateDocument",
        json!({"Content": r#"{"schemaVersion":"2.2","mainSteps":[]}"#}),
    );
    expect_err_code(svc.create_document(&req), "ValidationException");
}

#[test]
fn create_document_missing_content_errors() {
    let svc = make_service();
    let req = make_request("CreateDocument", json!({"Name": "doc1"}));
    expect_err_code(svc.create_document(&req), "ValidationException");
}

#[test]
fn describe_document_not_found_branch() {
    let svc = make_service();
    let req = make_request("DescribeDocument", json!({"Name": "ghost"}));
    assert!(svc.describe_document(&req).is_err());
}

#[test]
fn list_documents_pagination_round_trip_and_omits_empty_token() {
    // bug-audit 2026-05-28, 1.7 regression guard: list_documents must emit a
    // NextToken it will accept on the next request, and must OMIT NextToken on
    // the last page. Previously it emitted an empty-string token there, which
    // the stricter paginate_checked would reject if a client echoed it back.
    let svc = make_service();
    for i in 0..3 {
        let req = make_request(
            "CreateDocument",
            json!({
                "Name": format!("doc-{i}"),
                "Content": "{\"schemaVersion\":\"2.2\",\"mainSteps\":[]}",
                "DocumentType": "Command"
            }),
        );
        svc.create_document(&req).unwrap();
    }

    let mut next: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut params = json!({ "MaxResults": 2 });
        if let Some(t) = &next {
            params["NextToken"] = json!(t);
        }
        let req = make_request("ListDocuments", params);
        // The token emitted by the previous page must round-trip, not error.
        let resp = svc.list_documents(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        pages += 1;
        assert!(pages < 50, "pagination did not terminate");
        match body["NextToken"].as_str() {
            Some(t) if !t.is_empty() => next = Some(t.to_string()),
            _ => {
                // Last page: NextToken omitted or null, never an empty string.
                assert!(
                    body.get("NextToken").is_none_or(|v| v.is_null()),
                    "last page must omit NextToken, got {:?}",
                    body.get("NextToken")
                );
                break;
            }
        }
    }
    assert!(
        pages >= 2,
        "expected multiple pages for 3 docs at MaxResults=2"
    );
}

#[test]
fn update_document_default_version_not_found() {
    let svc = make_service();
    let req = make_request(
        "UpdateDocumentDefaultVersion",
        json!({"Name": "ghost", "DocumentVersion": "1"}),
    );
    assert!(svc.update_document_default_version(&req).is_err());
}

#[test]
fn list_document_versions_not_found() {
    let svc = make_service();
    let req = make_request("ListDocumentVersions", json!({"Name": "ghost"}));
    assert!(svc.list_document_versions(&req).is_err());
}

#[test]
fn describe_document_permission_not_found() {
    let svc = make_service();
    let req = make_request(
        "DescribeDocumentPermission",
        json!({"Name": "ghost", "PermissionType": "Share"}),
    );
    assert!(svc.describe_document_permission(&req).is_err());
}

// ── associations.rs error branches ──

#[test]
fn describe_association_not_found() {
    let svc = make_service();
    let req = make_request("DescribeAssociation", json!({"AssociationId": "ghost"}));
    assert!(svc.describe_association(&req).is_err());
}

#[test]
fn delete_association_not_found() {
    let svc = make_service();
    let req = make_request("DeleteAssociation", json!({"AssociationId": "ghost"}));
    assert!(svc.delete_association(&req).is_err());
}

#[test]
fn update_association_not_found() {
    let svc = make_service();
    let req = make_request(
        "UpdateAssociation",
        json!({"AssociationId": "ghost", "Name": "AWS-RunShellScript"}),
    );
    assert!(svc.update_association(&req).is_err());
}

// ── maintenance.rs error branches ──

#[test]
fn get_maintenance_window_not_found() {
    let svc = make_service();
    let req = make_request("GetMaintenanceWindow", json!({"WindowId": "mw-ghost"}));
    assert!(svc.get_maintenance_window(&req).is_err());
}

#[test]
fn delete_maintenance_window_is_idempotent_for_well_formed_id() {
    // DeleteMaintenanceWindow is idempotent in AWS for well-formed but
    // unknown WindowIds (its Smithy errors list is just
    // InternalServerError; AWS docs state the op is idempotent). A
    // malformed id — wrong @length, missing field — is rejected at the
    // wire layer instead. We mirror that: a 20-char "mw-..." that
    // happens to refer to no window returns 200, but a short stub like
    // "mw-ghost" gets a 400.
    let svc = make_service();
    let req = make_request(
        "DeleteMaintenanceWindow",
        json!({"WindowId": "mw-ffffffffffffffff0"}),
    );
    let resp = svc
        .delete_maintenance_window(&req)
        .expect("delete on missing well-formed id should succeed");
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["WindowId"], "mw-ffffffffffffffff0");
}

#[test]
fn delete_maintenance_window_rejects_malformed_id() {
    let svc = make_service();
    let req = make_request("DeleteMaintenanceWindow", json!({"WindowId": "mw-ghost"}));
    let err = expect_err(svc.delete_maintenance_window(&req));
    assert_eq!(err.code(), "ValidationException");
}

#[test]
fn update_maintenance_window_not_found() {
    let svc = make_service();
    let req = make_request("UpdateMaintenanceWindow", json!({"WindowId": "mw-ghost"}));
    assert!(svc.update_maintenance_window(&req).is_err());
}

#[test]
fn update_maintenance_window_applies_schedule_fields() {
    // ScheduleTimezone/ScheduleOffset/StartDate/EndDate were dropped on update
    // (ScheduleOffset even validated) -- bug-audit 2026-06-20, 1.24.
    let svc = make_service();
    let create = svc
        .create_maintenance_window(&make_request(
            "CreateMaintenanceWindow",
            json!({
                "Name": "win",
                "Schedule": "rate(7 days)",
                "Duration": 2,
                "Cutoff": 1,
                "AllowUnassociatedTargets": false
            }),
        ))
        .unwrap();
    let cb: Value = serde_json::from_slice(create.body.expect_bytes()).unwrap();
    let window_id = cb["WindowId"].as_str().unwrap().to_string();

    let resp = svc
        .update_maintenance_window(&make_request(
            "UpdateMaintenanceWindow",
            json!({
                "WindowId": window_id,
                "ScheduleTimezone": "America/New_York",
                "ScheduleOffset": 2,
                "StartDate": "2026-07-01T00:00:00Z",
                "EndDate": "2026-12-31T00:00:00Z"
            }),
        ))
        .unwrap();
    let b: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(b["ScheduleTimezone"], "America/New_York", "{b}");
    assert_eq!(b["ScheduleOffset"], 2);
    assert_eq!(b["StartDate"], "2026-07-01T00:00:00Z");
    assert_eq!(b["EndDate"], "2026-12-31T00:00:00Z");

    // Persisted: DescribeMaintenanceWindows reflects them too.
    let d = svc
        .describe_maintenance_windows(&make_request("DescribeMaintenanceWindows", json!({})))
        .unwrap();
    let db: Value = serde_json::from_slice(d.body.expect_bytes()).unwrap();
    let win = db["WindowIdentities"]
        .as_array()
        .unwrap()
        .iter()
        .find(|w| w["WindowId"] == window_id.as_str())
        .unwrap();
    assert_eq!(win["ScheduleTimezone"], "America/New_York", "{win}");
}

#[test]
fn register_target_unknown_window_errors() {
    let svc = make_service();
    let req = make_request(
        "RegisterTargetWithMaintenanceWindow",
        json!({
            "WindowId": "mw-ghost",
            "ResourceType": "INSTANCE",
            "Targets": [{"Key": "InstanceIds", "Values": ["i-1"]}]
        }),
    );
    assert!(svc.register_target_with_maintenance_window(&req).is_err());
}

#[test]
fn register_task_unknown_window_errors() {
    let svc = make_service();
    let req = make_request(
        "RegisterTaskWithMaintenanceWindow",
        json!({
            "WindowId": "mw-ghost",
            "TaskArn": "AWS-RunShellScript",
            "TaskType": "RUN_COMMAND",
            "Targets": []
        }),
    );
    assert!(svc.register_task_with_maintenance_window(&req).is_err());
}

// ── patches.rs error branches ──

#[test]
fn get_patch_baseline_not_found() {
    let svc = make_service();
    let req = make_request("GetPatchBaseline", json!({"BaselineId": "pb-ghost"}));
    assert!(svc.get_patch_baseline(&req).is_err());
}

#[test]
fn delete_patch_baseline_not_found() {
    let svc = make_service();
    let req = make_request("DeletePatchBaseline", json!({"BaselineId": "pb-ghost"}));
    assert!(svc.delete_patch_baseline(&req).is_err());
}

#[test]
fn register_patch_baseline_for_patch_group_with_unknown() {
    let svc = make_service();
    let req = make_request(
        "RegisterPatchBaselineForPatchGroup",
        json!({"BaselineId": "pb-ghost", "PatchGroup": "prod"}),
    );
    assert!(svc.register_patch_baseline_for_patch_group(&req).is_err());
}

// ── describe_parameters filter matching ──

#[test]
fn describe_parameters_name_begins_with_filter() {
    let svc = make_service();
    for name in &["/app/db/host", "/app/db/port", "/other/key"] {
        svc.put_parameter(&make_request(
            "PutParameter",
            json!({"Name": name, "Value": "v", "Type": "String"}),
        ))
        .unwrap();
    }
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Name", "Option": "BeginsWith", "Values": ["/app/"]}
            ]
        }),
    );
    let resp = svc.describe_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let params = body["Parameters"].as_array().unwrap();
    assert_eq!(params.len(), 2);
}

#[test]
fn describe_parameters_name_contains_filter() {
    let svc = make_service();
    for name in &["/app/db", "/system/app", "/misc/x"] {
        svc.put_parameter(&make_request(
            "PutParameter",
            json!({"Name": name, "Value": "v", "Type": "String"}),
        ))
        .unwrap();
    }
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Name", "Option": "Contains", "Values": ["/app"]}
            ]
        }),
    );
    let resp = svc.describe_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let params = body["Parameters"].as_array().unwrap();
    assert_eq!(params.len(), 2);
}

#[test]
fn describe_parameters_path_recursive_filter() {
    let svc = make_service();
    for name in &["/app/a", "/app/sub/b", "/other/x"] {
        svc.put_parameter(&make_request(
            "PutParameter",
            json!({"Name": name, "Value": "v", "Type": "String"}),
        ))
        .unwrap();
    }
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Path", "Option": "Recursive", "Values": ["/app"]}
            ]
        }),
    );
    let resp = svc.describe_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 2);
}

#[test]
fn describe_parameters_type_filter() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/p1", "Value": "v", "Type": "String"}),
    ))
    .unwrap();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/p2", "Value": "v", "Type": "SecureString"}),
    ))
    .unwrap();
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [
                {"Key": "Type", "Values": ["SecureString"]}
            ]
        }),
    );
    let resp = svc.describe_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 1);
}

#[test]
fn get_parameters_by_path_non_recursive_single_level() {
    let svc = make_service();
    for name in &["/g/a", "/g/sub/b"] {
        svc.put_parameter(&make_request(
            "PutParameter",
            json!({"Name": name, "Value": "v", "Type": "String"}),
        ))
        .unwrap();
    }
    let req = make_request(
        "GetParametersByPath",
        json!({"Path": "/g", "Recursive": false}),
    );
    let resp = svc.get_parameters_by_path(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameters"].as_array().unwrap().len(), 1);
}

// ---- Instances / Activations ----

#[test]
fn create_activation_requires_iam_role() {
    let svc = make_service();
    let req = make_request("CreateActivation", json!({}));
    assert!(svc.create_activation(&req).is_err());
}

#[test]
fn create_activation_stores_record() {
    let svc = make_service();
    let req = make_request(
        "CreateActivation",
        json!({"IamRole": "SSMRole", "Description": "d", "RegistrationLimit": 5}),
    );
    let resp = svc.create_activation(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["ActivationId"].is_string());
    assert!(body["ActivationCode"].is_string());
    assert_eq!(svc.state.read().default_ref().activations.len(), 1);
}

#[test]
fn delete_activation_missing_id_errors() {
    let svc = make_service();
    let req = make_request("DeleteActivation", json!({}));
    assert!(svc.delete_activation(&req).is_err());
}

#[test]
fn delete_activation_unknown_errors() {
    let svc = make_service();
    let req = make_request("DeleteActivation", json!({"ActivationId": "missing"}));
    assert!(svc.delete_activation(&req).is_err());
}

#[test]
fn delete_activation_removes_existing() {
    let svc = make_service();
    let create = make_request("CreateActivation", json!({"IamRole": "r"}));
    let resp = svc.create_activation(&create).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let id = body["ActivationId"].as_str().unwrap().to_string();
    svc.delete_activation(&make_request(
        "DeleteActivation",
        json!({"ActivationId": id}),
    ))
    .unwrap();
    assert!(svc.state.read().default_ref().activations.is_empty());
}

#[test]
fn describe_activations_returns_all() {
    let svc = make_service();
    for i in 0..3 {
        svc.create_activation(&make_request(
            "CreateActivation",
            json!({"IamRole": format!("r{i}")}),
        ))
        .unwrap();
    }
    let resp = svc
        .describe_activations(&make_request("DescribeActivations", json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ActivationList"].as_array().unwrap().len(), 3);
}

#[test]
fn deregister_managed_instance_missing_id_errors() {
    let svc = make_service();
    let req = make_request("DeregisterManagedInstance", json!({}));
    assert!(svc.deregister_managed_instance(&req).is_err());
}

#[test]
fn describe_instance_information_returns_list() {
    let svc = make_service();
    let req = make_request("DescribeInstanceInformation", json!({}));
    let resp = svc.describe_instance_information(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["InstanceInformationList"].is_array());
}

#[test]
fn describe_instance_properties_returns_list() {
    let svc = make_service();
    let req = make_request("DescribeInstanceProperties", json!({}));
    let resp = svc.describe_instance_properties(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["InstanceProperties"].is_array());
}

#[test]
fn update_managed_instance_role_missing_id_errors() {
    let svc = make_service();
    let req = make_request("UpdateManagedInstanceRole", json!({"IamRole": "r"}));
    assert!(svc.update_managed_instance_role(&req).is_err());
}

#[test]
fn list_nodes_returns_empty_list() {
    let svc = make_service();
    let req = make_request("ListNodes", json!({}));
    let resp = svc.list_nodes(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Nodes"].is_array());
}

#[test]
fn list_nodes_summary_missing_aggregators_errors() {
    let svc = make_service();
    let req = make_request("ListNodesSummary", json!({}));
    assert!(svc.list_nodes_summary(&req).is_err());
}

// ── associations.rs extra coverage ────────────────────────────────

fn create_assoc_with_instance(svc: &SsmService, name: &str, instance_id: &str) -> String {
    let req = make_request(
        "CreateAssociation",
        json!({
            "Name": name,
            "InstanceId": instance_id,
            "Targets": [{"Key": "InstanceIds", "Values": [instance_id]}],
        }),
    );
    let resp = svc.create_association(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    body["AssociationDescription"]["AssociationId"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn describe_association_by_name_and_instance() {
    let svc = make_service();
    create_assoc_with_instance(&svc, "AWS-RunShellScript", "i-123");
    let req = make_request(
        "DescribeAssociation",
        json!({"Name": "AWS-RunShellScript", "InstanceId": "i-123"}),
    );
    let resp = svc.describe_association(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["AssociationDescription"]["Name"].as_str().unwrap(),
        "AWS-RunShellScript"
    );
}

#[test]
fn describe_association_missing_identifier_errors() {
    let svc = make_service();
    let req = make_request("DescribeAssociation", json!({}));
    assert!(svc.describe_association(&req).is_err());
}

#[test]
fn delete_association_by_name_and_instance() {
    let svc = make_service();
    create_assoc_with_instance(&svc, "AWS-RunShellScript", "i-del");
    let req = make_request(
        "DeleteAssociation",
        json!({"Name": "AWS-RunShellScript", "InstanceId": "i-del"}),
    );
    svc.delete_association(&req).unwrap();
}

#[test]
fn delete_association_missing_identifier_errors() {
    let svc = make_service();
    let req = make_request("DeleteAssociation", json!({}));
    assert!(svc.delete_association(&req).is_err());
}

#[test]
fn update_association_changes_multiple_fields() {
    let svc = make_service();
    let assoc_id = create_assoc_with_instance(&svc, "AWS-RunShellScript", "i-upd");
    let req = make_request(
        "UpdateAssociation",
        json!({
            "AssociationId": assoc_id,
            "Name": "AWS-RunPowerShell",
            "Targets": [{"Key": "InstanceIds", "Values": ["i-upd2"]}],
            "ScheduleExpression": "rate(30 minutes)",
            "Parameters": {"p1": ["a", "b"]},
            "AssociationName": "new-name",
            "DocumentVersion": "2",
            "MaxErrors": "5",
            "MaxConcurrency": "10",
            "ComplianceSeverity": "HIGH"
        }),
    );
    let resp = svc.update_association(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["AssociationDescription"]["Name"], "AWS-RunPowerShell");
    assert_eq!(
        body["AssociationDescription"]["MaxErrors"]
            .as_str()
            .unwrap(),
        "5"
    );
    assert_eq!(
        body["AssociationDescription"]["ComplianceSeverity"]
            .as_str()
            .unwrap(),
        "HIGH"
    );
}

#[test]
fn update_association_status_updates_and_not_found() {
    let svc = make_service();
    create_assoc_with_instance(&svc, "AWS-RunShellScript", "i-aa");

    // Success path
    let req = make_request(
        "UpdateAssociationStatus",
        json!({
            "Name": "AWS-RunShellScript",
            "InstanceId": "i-aa",
            "AssociationStatus": {
                "Name": "Success",
                "Date": "2026-01-01T00:00:00Z",
                "Message": "ok"
            }
        }),
    );
    let resp = svc.update_association_status(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["AssociationDescription"]["Status"]["Name"], "Success");

    // Not found
    let req = make_request(
        "UpdateAssociationStatus",
        json!({
            "Name": "AWS-RunShellScript",
            "InstanceId": "i-ghost",
            "AssociationStatus": {"Name": "Success", "Date": "2026-01-01T00:00:00Z", "Message": "ok"}
        }),
    );
    assert!(svc.update_association_status(&req).is_err());
}

#[test]
fn list_association_versions_not_found() {
    let svc = make_service();
    let req = make_request(
        "ListAssociationVersions",
        json!({"AssociationId": "missing"}),
    );
    assert!(svc.list_association_versions(&req).is_err());
}

#[test]
fn describe_effective_instance_associations_returns_matching() {
    let svc = make_service();
    create_assoc_with_instance(&svc, "AWS-RunShellScript", "i-xy");
    let req = make_request(
        "DescribeEffectiveInstanceAssociations",
        json!({"InstanceId": "i-xy"}),
    );
    let resp = svc.describe_effective_instance_associations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Associations"].as_array().unwrap().len(), 1);
}

#[test]
fn describe_instance_associations_status_returns_matching() {
    let svc = make_service();
    create_assoc_with_instance(&svc, "AWS-RunShellScript", "i-ss");
    let req = make_request(
        "DescribeInstanceAssociationsStatus",
        json!({"InstanceId": "i-ss"}),
    );
    let resp = svc.describe_instance_associations_status(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["InstanceAssociationStatusInfos"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn describe_association_executions_empty() {
    let svc = make_service();
    let req = make_request(
        "DescribeAssociationExecutions",
        json!({"AssociationId": "any"}),
    );
    let resp = svc.describe_association_executions(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["AssociationExecutions"].as_array().unwrap().is_empty());
}

#[test]
fn describe_association_execution_targets_empty() {
    let svc = make_service();
    let req = make_request(
        "DescribeAssociationExecutionTargets",
        json!({"AssociationId": "any", "ExecutionId": "ex"}),
    );
    let resp = svc.describe_association_execution_targets(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["AssociationExecutionTargets"]
        .as_array()
        .unwrap()
        .is_empty());
}

// ── documents.rs extra coverage ───────────────────────────────────

fn create_doc_basic(svc: &SsmService, name: &str) {
    let req = make_request(
        "CreateDocument",
        json!({
            "Name": name,
            "Content": "{\"schemaVersion\":\"2.2\",\"description\":\"d\",\"mainSteps\":[]}",
            "DocumentFormat": "JSON",
            "DocumentType": "Command"
        }),
    );
    svc.create_document(&req).unwrap();
}

#[test]
fn list_document_metadata_history_returns_empty_without_reviews() {
    let svc = make_service();
    create_doc_basic(&svc, "DocA");
    let req = make_request(
        "ListDocumentMetadataHistory",
        json!({"Name": "DocA", "Metadata": "DocumentReviews"}),
    );
    let resp = svc.list_document_metadata_history(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "DocA");
    assert!(body["Metadata"]["ReviewerResponse"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn list_document_metadata_history_invalid_metadata_enum() {
    let svc = make_service();
    let req = make_request(
        "ListDocumentMetadataHistory",
        json!({"Name": "x", "Metadata": "Bogus"}),
    );
    assert!(svc.list_document_metadata_history(&req).is_err());
}

#[test]
fn update_document_metadata_not_found() {
    let svc = make_service();
    let req = make_request(
        "UpdateDocumentMetadata",
        json!({
            "Name": "missing",
            "DocumentReviews": {"Action": "SendForReview"},
        }),
    );
    assert!(svc.update_document_metadata(&req).is_err());
}

#[test]
fn update_document_metadata_persists_review_history() {
    let svc = make_service();
    create_doc_basic(&svc, "DocB");

    let req = make_request(
        "UpdateDocumentMetadata",
        json!({
            "Name": "DocB",
            "DocumentReviews": {
                "Action": "Approve",
                "Comment": [{"Type": "Comment", "Content": "lgtm"}]
            }
        }),
    );
    svc.update_document_metadata(&req).unwrap();

    let req = make_request(
        "ListDocumentMetadataHistory",
        json!({"Name": "DocB", "Metadata": "DocumentReviews"}),
    );
    let resp = svc.list_document_metadata_history(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let responses = body["Metadata"]["ReviewerResponse"].as_array().unwrap();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0]["ReviewStatus"], "APPROVED");
    assert_eq!(responses[0]["Comment"][0]["Content"], "lgtm");
}

#[test]
fn ssm_resource_policy_crud_full() {
    let svc = make_service();
    let arn = "arn:aws:ssm:us-east-1:123:parameter/foo";

    let req = make_request(
        "PutResourcePolicy",
        json!({"ResourceArn": arn, "Policy": "{\"Version\":\"2012-10-17\"}"}),
    );
    let resp = svc.put_resource_policy(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let policy_id = body["PolicyId"].as_str().unwrap().to_string();
    let policy_hash = body["PolicyHash"].as_str().unwrap().to_string();

    let req = make_request("GetResourcePolicies", json!({"ResourceArn": arn}));
    let resp = svc.get_resource_policies(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Policies"].as_array().unwrap().len(), 1);

    let req = make_request(
        "PutResourcePolicy",
        json!({
            "ResourceArn": arn,
            "Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[]}",
            "PolicyId": policy_id,
            "PolicyHash": policy_hash
        }),
    );
    let resp = svc.put_resource_policy(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let new_hash = body["PolicyHash"].as_str().unwrap().to_string();
    assert_ne!(new_hash, policy_hash);

    let req = make_request(
        "DeleteResourcePolicy",
        json!({
            "ResourceArn": arn,
            "PolicyId": policy_id,
            "PolicyHash": new_hash
        }),
    );
    svc.delete_resource_policy(&req).unwrap();

    let req = make_request("GetResourcePolicies", json!({"ResourceArn": arn}));
    let resp = svc.get_resource_policies(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Policies"].as_array().unwrap().is_empty());
}

#[test]
fn put_resource_policy_hash_mismatch_errors() {
    let svc = make_service();
    let arn = "arn:aws:ssm:us-east-1:123:parameter/hashtest";
    let req = make_request(
        "PutResourcePolicy",
        json!({"ResourceArn": arn, "Policy": "{}"}),
    );
    let resp = svc.put_resource_policy(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let policy_id = body["PolicyId"].as_str().unwrap().to_string();

    let req = make_request(
        "PutResourcePolicy",
        json!({
            "ResourceArn": arn,
            "Policy": "{\"v\":1}",
            "PolicyId": policy_id,
            "PolicyHash": "wrong-hash"
        }),
    );
    assert!(svc.put_resource_policy(&req).is_err());
}

#[test]
fn ssm_delete_resource_policy_not_found() {
    let svc = make_service();
    let req = make_request(
        "DeleteResourcePolicy",
        json!({
            "ResourceArn": "arn:aws:ssm:us-east-1:123:parameter/ghost",
            "PolicyId": "no-such-policy",
            "PolicyHash": "h"
        }),
    );
    assert!(svc.delete_resource_policy(&req).is_err());
}

#[test]
fn get_parameter_with_expired_policy_returns_not_found() {
    let svc = make_service();
    let past = "1990-01-01T00:00:00.000Z";
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": past }
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/expired",
            "Value": "v1",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies
        }),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request("GetParameter", json!({ "Name": "/expired" }));
    let err = match svc.get_parameter(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected expired param to be hidden"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn get_parameter_with_future_expiration_still_returned() {
    let svc = make_service();
    let future = "9999-01-01T00:00:00.000Z";
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": future }
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/notyet",
            "Value": "v1",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies
        }),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request("GetParameter", json!({ "Name": "/notyet" }));
    let resp = svc.get_parameter(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Parameter"]["Value"], "v1");
}

#[test]
fn get_parameters_drops_expired_into_invalid_list() {
    let svc = make_service();
    // Live param
    let req = make_request(
        "PutParameter",
        json!({ "Name": "/live", "Value": "ok", "Type": "String" }),
    );
    svc.put_parameter(&req).unwrap();

    // Expired param
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": "1990-01-01T00:00:00.000Z" }
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/dead",
            "Value": "no",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies
        }),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request("GetParameters", json!({ "Names": ["/live", "/dead"] }));
    let resp = svc.get_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let params = body["Parameters"].as_array().unwrap();
    let invalid = body["InvalidParameters"].as_array().unwrap();
    assert_eq!(params.len(), 1);
    assert_eq!(params[0]["Name"], "/live");
    assert!(invalid.iter().any(|v| v == "/dead"));
}

#[test]
fn get_parameters_by_path_skips_expired_entries() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({ "Name": "/app/live", "Value": "ok", "Type": "String" }),
    );
    svc.put_parameter(&req).unwrap();

    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": "1990-01-01T00:00:00.000Z" }
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/app/dead",
            "Value": "no",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies
        }),
    );
    svc.put_parameter(&req).unwrap();

    let req = make_request("GetParametersByPath", json!({ "Path": "/app" }));
    let resp = svc.get_parameters_by_path(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names: Vec<&str> = body["Parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["Name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["/app/live"]);
}

#[test]
fn put_parameter_rejects_policy_on_standard_tier() {
    let svc = make_service();
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": "9999-01-01T00:00:00.000Z" }
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/standard-policy",
            "Value": "v1",
            "Type": "String",
            "Policies": policies
        }),
    );
    let err = match svc.put_parameter(&req) {
        Err(e) => e,
        Ok(_) => panic!("standard tier with policies should fail"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert!(
        err.message().contains("Advanced"),
        "expected Advanced-tier message, got: {}",
        err.message()
    );
}

#[test]
fn put_parameter_rejects_malformed_policies() {
    let svc = make_service();
    // Missing Attributes.Timestamp on an Expiration policy.
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": {}
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/malformed",
            "Value": "v1",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies
        }),
    );
    let err = match svc.put_parameter(&req) {
        Err(e) => e,
        Ok(_) => panic!("malformed policy should fail"),
    };
    assert_eq!(err.code(), "InvalidPolicyAttributeException");
}

#[test]
fn put_parameter_rejects_unknown_policy_type() {
    let svc = make_service();
    let policies = serde_json::to_string(&json!([{
        "Type": "NotARealPolicy",
        "Version": "1.0",
        "Attributes": { "X": "Y" }
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/unknown-type",
            "Value": "v1",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies
        }),
    );
    let err = match svc.put_parameter(&req) {
        Err(e) => e,
        Ok(_) => panic!("unknown type should fail"),
    };
    assert_eq!(err.code(), "InvalidPolicyAttributeException");
}

#[test]
fn put_parameter_rejects_invalid_policies_json() {
    let svc = make_service();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/garbage",
            "Value": "v1",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": "{not-json"
        }),
    );
    let err = match svc.put_parameter(&req) {
        Err(e) => e,
        Ok(_) => panic!("non-JSON Policies should fail"),
    };
    assert_eq!(err.code(), "InvalidPolicyAttributeException");
}

#[test]
fn put_parameter_records_policy_registration_event() {
    let svc = make_service();
    let policies = serde_json::to_string(&json!([
        {
            "Type": "Expiration",
            "Version": "1.0",
            "Attributes": { "Timestamp": "9999-01-01T00:00:00.000Z" }
        },
        {
            "Type": "ExpirationNotification",
            "Version": "1.0",
            "Attributes": { "Before": "30", "Unit": "Days" }
        }
    ]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/with-policies",
            "Value": "v1",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies
        }),
    );
    svc.put_parameter(&req).unwrap();

    let events = svc.parameter_policy_events("123456789012");
    assert!(events
        .iter()
        .any(|e| e.event_type == "ExpirationRegistered" && e.parameter_name == "/with-policies"));
    assert!(events
        .iter()
        .any(|e| e.event_type == "ExpirationNotificationRegistered"
            && e.parameter_name == "/with-policies"));
}

#[test]
fn overwrite_keeps_existing_policies_when_omitted() {
    let svc = make_service();
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": "9999-01-01T00:00:00.000Z" }
    }]))
    .unwrap();
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/keep-policies",
            "Value": "v1",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies
        }),
    );
    svc.put_parameter(&req).unwrap();

    // Overwrite without Policies.
    let req = make_request(
        "PutParameter",
        json!({
            "Name": "/keep-policies",
            "Value": "v2",
            "Type": "String",
            "Overwrite": true
        }),
    );
    svc.put_parameter(&req).unwrap();

    // Read back via DescribeParameters and confirm Policies survived.
    let req = make_request(
        "DescribeParameters",
        json!({
            "ParameterFilters": [{
                "Key": "Name",
                "Values": ["/keep-policies"]
            }]
        }),
    );
    let resp = svc.describe_parameters(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let params = body["Parameters"].as_array().unwrap();
    assert_eq!(params.len(), 1);
    assert!(params[0]["Policies"].is_array());
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = make_service();
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating SSM state
/// directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = make_service().with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    // Must not panic; exercises the closure and the snapshot save path.
    hook().await;
}

/// UpdateAssociation must persist SyncCompliance, ApplyOnlyAtCronInterval,
/// ScheduleOffset and OutputLocation, all of which DescribeAssociation
/// echoes back (regression: previously silently dropped).
#[test]
fn update_association_persists_sync_compliance_and_friends() {
    let svc = make_service();
    let assoc_id = create_assoc_with_instance(&svc, "AWS-RunShellScript", "i-sync");
    let output_location = json!({
        "S3Location": {
            "OutputS3BucketName": "my-bucket",
            "OutputS3KeyPrefix": "ssm/",
        }
    });
    let req = make_request(
        "UpdateAssociation",
        json!({
            "AssociationId": assoc_id,
            "ScheduleExpression": "cron(0 2 ? * SUN *)",
            "SyncCompliance": "Manual",
            "ApplyOnlyAtCronInterval": true,
            "ScheduleOffset": 3,
            "OutputLocation": output_location,
        }),
    );
    let resp = svc.update_association(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ad = &body["AssociationDescription"];
    assert_eq!(ad["SyncCompliance"].as_str().unwrap(), "Manual");
    assert!(ad["ApplyOnlyAtCronInterval"].as_bool().unwrap());
    assert_eq!(ad["ScheduleOffset"].as_i64().unwrap(), 3);
    assert_eq!(ad["OutputLocation"], output_location);

    // Round-trips through DescribeAssociation too.
    let desc = svc
        .describe_association(&make_request(
            "DescribeAssociation",
            json!({"AssociationId": assoc_id}),
        ))
        .unwrap();
    let dbody: Value = serde_json::from_slice(desc.body.expect_bytes()).unwrap();
    let dad = &dbody["AssociationDescription"];
    assert_eq!(dad["SyncCompliance"].as_str().unwrap(), "Manual");
    assert!(dad["ApplyOnlyAtCronInterval"].as_bool().unwrap());
    assert_eq!(dad["ScheduleOffset"].as_i64().unwrap(), 3);
    assert_eq!(dad["OutputLocation"], output_location);
}

/// PutParameter(Overwrite=true) with Tier=Advanced must actually upgrade the
/// stored tier and echo the new tier (regression: tier was never updated so
/// the response echoed the old Standard tier).
#[test]
fn put_parameter_overwrite_upgrades_tier() {
    let svc = make_service();
    // Create a Standard-tier parameter.
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/tier-upgrade", "Value": "v1", "Type": "String"}),
    ))
    .unwrap();

    // Overwrite to Advanced with a valid Expiration policy.
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": "9999-01-01T00:00:00.000Z" }
    }]))
    .unwrap();
    let resp = svc
        .put_parameter(&make_request(
            "PutParameter",
            json!({
                "Name": "/tier-upgrade",
                "Value": "v2",
                "Overwrite": true,
                "Tier": "Advanced",
                "Policies": policies,
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Tier"].as_str().unwrap(), "Advanced");

    // GetParameter (describe path) reports Advanced too.
    let desc = svc
        .describe_parameters(&make_request(
            "DescribeParameters",
            json!({"ParameterFilters": [{"Key": "Name", "Values": ["/tier-upgrade"]}]}),
        ))
        .unwrap();
    let dbody: Value = serde_json::from_slice(desc.body.expect_bytes()).unwrap();
    assert_eq!(dbody["Parameters"][0]["Tier"].as_str().unwrap(), "Advanced");
}

/// PutParameter overwrite must preserve the existing tier when Tier is
/// omitted from the request (AWS never silently downgrades).
#[test]
fn put_parameter_overwrite_preserves_tier_when_omitted() {
    let svc = make_service();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({"Name": "/tier-keep", "Value": "v1", "Type": "String", "Tier": "Advanced"}),
    ))
    .unwrap();

    let resp = svc
        .put_parameter(&make_request(
            "PutParameter",
            json!({"Name": "/tier-keep", "Value": "v2", "Overwrite": true}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Tier"].as_str().unwrap(), "Advanced");
}

/// Overwriting an Advanced parameter (that carries policies) down to
/// Standard while keeping the policies must be rejected: advanced-only
/// policies cannot live on a Standard-tier parameter.
#[test]
fn put_parameter_overwrite_rejects_downgrade_with_policies() {
    let svc = make_service();
    let policies = serde_json::to_string(&json!([{
        "Type": "Expiration",
        "Version": "1.0",
        "Attributes": { "Timestamp": "9999-01-01T00:00:00.000Z" }
    }]))
    .unwrap();
    svc.put_parameter(&make_request(
        "PutParameter",
        json!({
            "Name": "/tier-downgrade",
            "Value": "v1",
            "Type": "String",
            "Tier": "Advanced",
            "Policies": policies,
        }),
    ))
    .unwrap();

    // Downgrade to Standard without clearing the policies -> rejected.
    let err = match svc.put_parameter(&make_request(
        "PutParameter",
        json!({
            "Name": "/tier-downgrade",
            "Value": "v2",
            "Overwrite": true,
            "Tier": "Standard",
        }),
    )) {
        Err(e) => e,
        Ok(_) => panic!("downgrade with policies should fail"),
    };
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert!(err.message().contains("Advanced"));
}
