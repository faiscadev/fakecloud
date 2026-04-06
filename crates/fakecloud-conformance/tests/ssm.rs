mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// -- Parameter CRUD --

#[test_action("ssm", "PutParameter", checksum = "3620c469")]
#[test_action("ssm", "GetParameter", checksum = "2ce7443c")]
#[test_action("ssm", "DeleteParameter", checksum = "5c66ec04")]
#[tokio::test]
async fn ssm_put_get_delete_parameter() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    client
        .put_parameter()
        .name("/conf/param1")
        .value("value1")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_parameter()
        .name("/conf/param1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameter().unwrap().value().unwrap(), "value1");

    client
        .delete_parameter()
        .name("/conf/param1")
        .send()
        .await
        .unwrap();
}

// -- GetParameters (batch) --

#[test_action("ssm", "GetParameters", checksum = "0bb4c5f2")]
#[tokio::test]
async fn ssm_get_parameters() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    client
        .put_parameter()
        .name("/conf/multi1")
        .value("a")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();
    client
        .put_parameter()
        .name("/conf/multi2")
        .value("b")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();
    let resp = client
        .get_parameters()
        .names("/conf/multi1")
        .names("/conf/multi2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameters().len(), 2);
}

// -- GetParametersByPath --

#[test_action("ssm", "GetParametersByPath", checksum = "1617e5a0")]
#[tokio::test]
async fn ssm_get_parameters_by_path() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    client
        .put_parameter()
        .name("/conf/path/a")
        .value("x")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();
    let resp = client
        .get_parameters_by_path()
        .path("/conf/path")
        .send()
        .await
        .unwrap();
    assert!(!resp.parameters().is_empty());
}

// -- DeleteParameters (batch) --

#[test_action("ssm", "DeleteParameters", checksum = "ee715760")]
#[tokio::test]
async fn ssm_delete_parameters() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    client
        .put_parameter()
        .name("/conf/del1")
        .value("x")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();
    let resp = client
        .delete_parameters()
        .names("/conf/del1")
        .send()
        .await
        .unwrap();
    assert!(!resp.deleted_parameters().is_empty());
}

// -- DescribeParameters --

#[test_action("ssm", "DescribeParameters", checksum = "bd157747")]
#[tokio::test]
async fn ssm_describe_parameters() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    client
        .put_parameter()
        .name("/conf/desc1")
        .value("x")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();
    let resp = client.describe_parameters().send().await.unwrap();
    assert!(!resp.parameters().is_empty());
}

// -- GetParameterHistory --

#[test_action("ssm", "GetParameterHistory", checksum = "a26dd5b9")]
#[tokio::test]
async fn ssm_get_parameter_history() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    client
        .put_parameter()
        .name("/conf/hist")
        .value("v1")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();
    client
        .put_parameter()
        .name("/conf/hist")
        .value("v2")
        .overwrite(true)
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();
    let resp = client
        .get_parameter_history()
        .name("/conf/hist")
        .send()
        .await
        .unwrap();
    assert!(resp.parameters().len() >= 2);
}

// -- Tags --

#[test_action("ssm", "AddTagsToResource", checksum = "0beb4b05")]
#[test_action("ssm", "ListTagsForResource", checksum = "9580cae3")]
#[test_action("ssm", "RemoveTagsFromResource", checksum = "6dd59ebb")]
#[tokio::test]
async fn ssm_tags() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    client
        .put_parameter()
        .name("/conf/tagged")
        .value("x")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();

    client
        .add_tags_to_resource()
        .resource_type(aws_sdk_ssm::types::ResourceTypeForTagging::Parameter)
        .resource_id("/conf/tagged")
        .tags(
            aws_sdk_ssm::types::Tag::builder()
                .key("env")
                .value("test")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_for_resource()
        .resource_type(aws_sdk_ssm::types::ResourceTypeForTagging::Parameter)
        .resource_id("/conf/tagged")
        .send()
        .await
        .unwrap();
    assert!(!resp.tag_list().is_empty());

    client
        .remove_tags_from_resource()
        .resource_type(aws_sdk_ssm::types::ResourceTypeForTagging::Parameter)
        .resource_id("/conf/tagged")
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

// -- Label / Unlabel parameter version --

#[test_action("ssm", "LabelParameterVersion", checksum = "50630187")]
#[test_action("ssm", "UnlabelParameterVersion", checksum = "16fdaeea")]
#[tokio::test]
async fn ssm_label_unlabel_parameter_version() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    client
        .put_parameter()
        .name("/conf/label")
        .value("x")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();

    client
        .label_parameter_version()
        .name("/conf/label")
        .labels("prod")
        .send()
        .await
        .unwrap();

    client
        .unlabel_parameter_version()
        .name("/conf/label")
        .labels("prod")
        .send()
        .await
        .unwrap();
}

// -- Document CRUD --

#[test_action("ssm", "CreateDocument", checksum = "4a0c1ee8")]
#[test_action("ssm", "GetDocument", checksum = "c9a5cfaf")]
#[test_action("ssm", "DescribeDocument", checksum = "124cfc8d")]
#[test_action("ssm", "DeleteDocument", checksum = "a7d809b4")]
#[tokio::test]
async fn ssm_document_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let doc_content = r#"{"schemaVersion":"2.2","description":"Test","mainSteps":[{"action":"aws:runShellScript","name":"test","inputs":{"runCommand":["echo hello"]}}]}"#;

    client
        .create_document()
        .name("conf-doc")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    let resp = client.get_document().name("conf-doc").send().await.unwrap();
    assert!(resp.content().is_some());

    let desc = client
        .describe_document()
        .name("conf-doc")
        .send()
        .await
        .unwrap();
    assert!(desc.document().is_some());

    client
        .delete_document()
        .name("conf-doc")
        .send()
        .await
        .unwrap();
}

// -- UpdateDocument + UpdateDocumentDefaultVersion --

#[test_action("ssm", "UpdateDocument", checksum = "7d752e4c")]
#[test_action("ssm", "UpdateDocumentDefaultVersion", checksum = "1cad9bdf")]
#[tokio::test]
async fn ssm_update_document() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let doc_v1 = r#"{"schemaVersion":"2.2","description":"v1","mainSteps":[{"action":"aws:runShellScript","name":"test","inputs":{"runCommand":["echo v1"]}}]}"#;
    let doc_v2 = r#"{"schemaVersion":"2.2","description":"v2","mainSteps":[{"action":"aws:runShellScript","name":"test","inputs":{"runCommand":["echo v2"]}}]}"#;

    client
        .create_document()
        .name("conf-doc-upd")
        .content(doc_v1)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    let upd = client
        .update_document()
        .name("conf-doc-upd")
        .content(doc_v2)
        .document_version("$LATEST")
        .send()
        .await
        .unwrap();
    let new_ver = upd
        .document_description()
        .unwrap()
        .document_version()
        .unwrap()
        .to_string();

    client
        .update_document_default_version()
        .name("conf-doc-upd")
        .document_version(&new_ver)
        .send()
        .await
        .unwrap();
}

// -- ListDocuments --

#[test_action("ssm", "ListDocuments", checksum = "c177e191")]
#[tokio::test]
async fn ssm_list_documents() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let _ = client.list_documents().send().await.unwrap();
}

// -- Document permissions --

#[test_action("ssm", "DescribeDocumentPermission", checksum = "5dc18586")]
#[test_action("ssm", "ModifyDocumentPermission", checksum = "679c9876")]
#[tokio::test]
async fn ssm_document_permissions() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let doc_content = r#"{"schemaVersion":"2.2","description":"perm","mainSteps":[{"action":"aws:runShellScript","name":"test","inputs":{"runCommand":["echo hi"]}}]}"#;
    client
        .create_document()
        .name("conf-doc-perm")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    client
        .modify_document_permission()
        .name("conf-doc-perm")
        .permission_type(aws_sdk_ssm::types::DocumentPermissionType::Share)
        .account_ids_to_add("123456789012")
        .send()
        .await
        .unwrap();

    let _ = client
        .describe_document_permission()
        .name("conf-doc-perm")
        .permission_type(aws_sdk_ssm::types::DocumentPermissionType::Share)
        .send()
        .await
        .unwrap();
}

// -- SendCommand + ListCommands + GetCommandInvocation + ListCommandInvocations + CancelCommand --

#[test_action("ssm", "SendCommand", checksum = "e3d9a465")]
#[test_action("ssm", "ListCommands", checksum = "6dd0b4fc")]
#[test_action("ssm", "ListCommandInvocations", checksum = "11365416")]
#[test_action("ssm", "CancelCommand", checksum = "3472b53a")]
#[tokio::test]
async fn ssm_commands() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let send = client
        .send_command()
        .document_name("AWS-RunShellScript")
        .instance_ids("i-00000000000000001")
        .parameters("commands", vec!["echo hello".to_string()])
        .send()
        .await
        .unwrap();
    let cmd_id = send.command().unwrap().command_id().unwrap().to_string();

    let _ = client.list_commands().send().await.unwrap();
    let _ = client.list_command_invocations().send().await.unwrap();
    let _ = client.cancel_command().command_id(&cmd_id).send().await;
}

// -- GetCommandInvocation --

#[test_action("ssm", "GetCommandInvocation", checksum = "0b0d098b")]
#[tokio::test]
async fn ssm_get_command_invocation() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let send = client
        .send_command()
        .document_name("AWS-RunShellScript")
        .instance_ids("i-00000000000000001")
        .parameters("commands", vec!["echo test".to_string()])
        .send()
        .await
        .unwrap();
    let cmd_id = send.command().unwrap().command_id().unwrap().to_string();

    let _ = client
        .get_command_invocation()
        .command_id(&cmd_id)
        .instance_id("i-00000000000000001")
        .send()
        .await;
}

// -- Maintenance windows --

#[test_action("ssm", "CreateMaintenanceWindow", checksum = "4225d446")]
#[test_action("ssm", "DescribeMaintenanceWindows", checksum = "9cbfac10")]
#[test_action("ssm", "GetMaintenanceWindow", checksum = "4078c60b")]
#[test_action("ssm", "UpdateMaintenanceWindow", checksum = "9e7748d9")]
#[test_action("ssm", "DeleteMaintenanceWindow", checksum = "0d510d38")]
#[tokio::test]
async fn ssm_maintenance_window_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_maintenance_window()
        .name("conf-mw")
        .schedule("rate(1 day)")
        .duration(2)
        .cutoff(1)
        .allow_unassociated_targets(true)
        .send()
        .await
        .unwrap();
    let mw_id = create.window_id().unwrap().to_string();

    let _ = client.describe_maintenance_windows().send().await.unwrap();

    let _ = client
        .get_maintenance_window()
        .window_id(&mw_id)
        .send()
        .await
        .unwrap();

    client
        .update_maintenance_window()
        .window_id(&mw_id)
        .name("conf-mw-updated")
        .send()
        .await
        .unwrap();

    client
        .delete_maintenance_window()
        .window_id(&mw_id)
        .send()
        .await
        .unwrap();
}

// -- Maintenance window targets --

#[test_action("ssm", "RegisterTargetWithMaintenanceWindow", checksum = "6cdf4aa5")]
#[test_action("ssm", "DescribeMaintenanceWindowTargets", checksum = "0c15c7b4")]
#[test_action("ssm", "DeregisterTargetFromMaintenanceWindow", checksum = "c6b419d2")]
#[tokio::test]
async fn ssm_maintenance_window_targets() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_maintenance_window()
        .name("conf-mw-tgt")
        .schedule("rate(1 day)")
        .duration(2)
        .cutoff(1)
        .allow_unassociated_targets(true)
        .send()
        .await
        .unwrap();
    let mw_id = create.window_id().unwrap().to_string();

    let reg = client
        .register_target_with_maintenance_window()
        .window_id(&mw_id)
        .resource_type(aws_sdk_ssm::types::MaintenanceWindowResourceType::Instance)
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-00000000000000001")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let target_id = reg.window_target_id().unwrap().to_string();

    let _ = client
        .describe_maintenance_window_targets()
        .window_id(&mw_id)
        .send()
        .await
        .unwrap();

    client
        .deregister_target_from_maintenance_window()
        .window_id(&mw_id)
        .window_target_id(&target_id)
        .send()
        .await
        .unwrap();
}

// -- Maintenance window tasks --

#[test_action("ssm", "RegisterTaskWithMaintenanceWindow", checksum = "b32d2007")]
#[test_action("ssm", "DescribeMaintenanceWindowTasks", checksum = "8289f62c")]
#[test_action("ssm", "DeregisterTaskFromMaintenanceWindow", checksum = "3b0d9f87")]
#[tokio::test]
async fn ssm_maintenance_window_tasks() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_maintenance_window()
        .name("conf-mw-task")
        .schedule("rate(1 day)")
        .duration(2)
        .cutoff(1)
        .allow_unassociated_targets(true)
        .send()
        .await
        .unwrap();
    let mw_id = create.window_id().unwrap().to_string();

    let reg = client
        .register_task_with_maintenance_window()
        .window_id(&mw_id)
        .task_arn("AWS-RunShellScript")
        .task_type(aws_sdk_ssm::types::MaintenanceWindowTaskType::RunCommand)
        .max_concurrency("1")
        .max_errors("0")
        .send()
        .await
        .unwrap();
    let task_id = reg.window_task_id().unwrap().to_string();

    let _ = client
        .describe_maintenance_window_tasks()
        .window_id(&mw_id)
        .send()
        .await
        .unwrap();

    client
        .deregister_task_from_maintenance_window()
        .window_id(&mw_id)
        .window_task_id(&task_id)
        .send()
        .await
        .unwrap();
}

// -- Patch baselines --

#[test_action("ssm", "CreatePatchBaseline", checksum = "9b5b1ac6")]
#[test_action("ssm", "DescribePatchBaselines", checksum = "babfc26d")]
#[test_action("ssm", "GetPatchBaseline", checksum = "bc756260")]
#[test_action("ssm", "DeletePatchBaseline", checksum = "eb0d4378")]
#[tokio::test]
async fn ssm_patch_baseline_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_patch_baseline()
        .name("conf-baseline")
        .send()
        .await
        .unwrap();
    let baseline_id = create.baseline_id().unwrap().to_string();

    let _ = client.describe_patch_baselines().send().await.unwrap();

    let _ = client
        .get_patch_baseline()
        .baseline_id(&baseline_id)
        .send()
        .await
        .unwrap();

    client
        .delete_patch_baseline()
        .baseline_id(&baseline_id)
        .send()
        .await
        .unwrap();
}

// -- Patch groups --

#[test_action("ssm", "RegisterPatchBaselineForPatchGroup", checksum = "6526be39")]
#[test_action("ssm", "GetPatchBaselineForPatchGroup", checksum = "23f4460d")]
#[test_action("ssm", "DescribePatchGroups", checksum = "dc683f88")]
#[test_action("ssm", "DeregisterPatchBaselineForPatchGroup", checksum = "ff76cb6e")]
#[tokio::test]
async fn ssm_patch_groups() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_patch_baseline()
        .name("conf-baseline-pg")
        .send()
        .await
        .unwrap();
    let baseline_id = create.baseline_id().unwrap().to_string();

    client
        .register_patch_baseline_for_patch_group()
        .baseline_id(&baseline_id)
        .patch_group("conf-group")
        .send()
        .await
        .unwrap();

    let _ = client
        .get_patch_baseline_for_patch_group()
        .patch_group("conf-group")
        .send()
        .await
        .unwrap();

    let _ = client.describe_patch_groups().send().await.unwrap();

    client
        .deregister_patch_baseline_for_patch_group()
        .baseline_id(&baseline_id)
        .patch_group("conf-group")
        .send()
        .await
        .unwrap();
}

// -- Association lifecycle --

#[test_action("ssm", "CreateAssociation", checksum = "507ad141")]
#[test_action("ssm", "DescribeAssociation", checksum = "2ffc2f3f")]
#[test_action("ssm", "UpdateAssociation", checksum = "2febcaea")]
#[test_action("ssm", "ListAssociations", checksum = "373868d2")]
#[test_action("ssm", "ListAssociationVersions", checksum = "6d4e7407")]
#[test_action("ssm", "DeleteAssociation", checksum = "89e9a7ab")]
#[tokio::test]
async fn ssm_association_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_association()
        .name("AWS-RunShellScript")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-00000000000000001")
                .build(),
        )
        .schedule_expression("rate(1 hour)")
        .association_name("conf-assoc")
        .send()
        .await
        .unwrap();
    let assoc_id = create
        .association_description()
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();

    let _ = client
        .describe_association()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();

    client
        .update_association()
        .association_id(&assoc_id)
        .association_name("conf-assoc-updated")
        .send()
        .await
        .unwrap();

    let _ = client.list_associations().send().await.unwrap();

    let _ = client
        .list_association_versions()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();

    client
        .delete_association()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();
}

// -- UpdateAssociationStatus --

#[test_action("ssm", "UpdateAssociationStatus", checksum = "1668b3f6")]
#[tokio::test]
async fn ssm_update_association_status() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_association()
        .name("AWS-RunShellScript")
        .instance_id("i-00000000000000001")
        .send()
        .await
        .unwrap();
    let _assoc_id = create
        .association_description()
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();

    client
        .update_association_status()
        .name("AWS-RunShellScript")
        .instance_id("i-00000000000000001")
        .association_status(
            aws_sdk_ssm::types::AssociationStatus::builder()
                .name(aws_sdk_ssm::types::AssociationStatusName::Success)
                .date(aws_sdk_ssm::primitives::DateTime::from_secs(0))
                .message("ok")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

// -- StartAssociationsOnce --

#[test_action("ssm", "StartAssociationsOnce", checksum = "7f3c858a")]
#[tokio::test]
async fn ssm_start_associations_once() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_association()
        .name("AWS-RunShellScript")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-00000000000000001")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let assoc_id = create
        .association_description()
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();

    client
        .start_associations_once()
        .association_ids(&assoc_id)
        .send()
        .await
        .unwrap();
}

// -- CreateAssociationBatch --

#[test_action("ssm", "CreateAssociationBatch", checksum = "d64a58de")]
#[tokio::test]
async fn ssm_create_association_batch() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let entry = aws_sdk_ssm::types::CreateAssociationBatchRequestEntry::builder()
        .name("AWS-RunShellScript")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-00000000000000001")
                .build(),
        )
        .build()
        .unwrap();

    let resp = client
        .create_association_batch()
        .entries(entry)
        .send()
        .await
        .unwrap();
    assert!(!resp.successful().is_empty());
}

// -- DescribeAssociationExecutions + Targets --

#[test_action("ssm", "DescribeAssociationExecutions", checksum = "6b36f591")]
#[tokio::test]
async fn ssm_describe_association_executions() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_association()
        .name("AWS-RunShellScript")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-00000000000000001")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let assoc_id = create
        .association_description()
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();

    let resp = client
        .describe_association_executions()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();
    assert!(resp.association_executions().is_empty());
}

#[test_action("ssm", "DescribeAssociationExecutionTargets", checksum = "10258e0d")]
#[tokio::test]
async fn ssm_describe_association_execution_targets() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_association()
        .name("AWS-RunShellScript")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-00000000000000001")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let assoc_id = create
        .association_description()
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();

    let resp = client
        .describe_association_execution_targets()
        .association_id(&assoc_id)
        .execution_id("fake-execution-id")
        .send()
        .await
        .unwrap();
    assert!(resp.association_execution_targets().is_empty());
}

// -- OpsItems --

#[test_action("ssm", "CreateOpsItem", checksum = "48b1d2b8")]
#[test_action("ssm", "GetOpsItem", checksum = "649a65f9")]
#[test_action("ssm", "UpdateOpsItem", checksum = "43879dd9")]
#[test_action("ssm", "DescribeOpsItems", checksum = "3a284cc4")]
#[test_action("ssm", "DeleteOpsItem", checksum = "f705d8f4")]
#[tokio::test]
async fn ssm_ops_item_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_ops_item()
        .title("Conf OpsItem")
        .source("conf-test")
        .send()
        .await
        .unwrap();
    let ops_item_id = create.ops_item_id().unwrap().to_string();

    let get = client
        .get_ops_item()
        .ops_item_id(&ops_item_id)
        .send()
        .await
        .unwrap();
    assert_eq!(get.ops_item().unwrap().title().unwrap(), "Conf OpsItem");

    client
        .update_ops_item()
        .ops_item_id(&ops_item_id)
        .title("Updated Conf OpsItem")
        .send()
        .await
        .unwrap();

    let _ = client.describe_ops_items().send().await.unwrap();

    client
        .delete_ops_item()
        .ops_item_id(&ops_item_id)
        .send()
        .await
        .unwrap();
}

// -- Document extras --

#[test_action("ssm", "ListDocumentVersions", checksum = "aadc01d4")]
#[tokio::test]
async fn ssm_list_document_versions() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let doc_content = r#"{"schemaVersion":"2.2","description":"ver","mainSteps":[{"action":"aws:runShellScript","name":"test","inputs":{"runCommand":["echo hi"]}}]}"#;

    client
        .create_document()
        .name("conf-doc-ver")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    let resp = client
        .list_document_versions()
        .name("conf-doc-ver")
        .send()
        .await
        .unwrap();
    assert!(!resp.document_versions().is_empty());
}

#[test_action("ssm", "ListDocumentMetadataHistory", checksum = "c6bdd053")]
#[tokio::test]
async fn ssm_list_document_metadata_history() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let doc_content = r#"{"schemaVersion":"2.2","description":"meta","mainSteps":[{"action":"aws:runShellScript","name":"test","inputs":{"runCommand":["echo hi"]}}]}"#;

    client
        .create_document()
        .name("conf-doc-meta")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    let _ = client
        .list_document_metadata_history()
        .name("conf-doc-meta")
        .metadata(aws_sdk_ssm::types::DocumentMetadataEnum::DocumentReviews)
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "UpdateDocumentMetadata", checksum = "9e4be7a5")]
#[tokio::test]
async fn ssm_update_document_metadata() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let doc_content = r#"{"schemaVersion":"2.2","description":"updmeta","mainSteps":[{"action":"aws:runShellScript","name":"test","inputs":{"runCommand":["echo hi"]}}]}"#;

    client
        .create_document()
        .name("conf-doc-updmeta")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    client
        .update_document_metadata()
        .name("conf-doc-updmeta")
        .document_reviews(
            aws_sdk_ssm::types::DocumentReviews::builder()
                .action(aws_sdk_ssm::types::DocumentReviewAction::Approve)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

// -- Resource policies --

#[test_action("ssm", "PutResourcePolicy", checksum = "7ee6b0bd")]
#[test_action("ssm", "GetResourcePolicies", checksum = "303e2bb5")]
#[test_action("ssm", "DeleteResourcePolicy", checksum = "df09409d")]
#[tokio::test]
async fn ssm_resource_policies() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // First create a parameter to use as resource
    client
        .put_parameter()
        .name("/conf/policy-test")
        .value("x")
        .r#type(aws_sdk_ssm::types::ParameterType::String)
        .send()
        .await
        .unwrap();

    let put = client
        .put_resource_policy()
        .resource_arn("arn:aws:ssm:us-east-1:123456789012:parameter/conf/policy-test")
        .policy(r#"{"Version":"2012-10-17","Statement":[]}"#)
        .send()
        .await
        .unwrap();
    let policy_id = put.policy_id().unwrap().to_string();
    let policy_hash = put.policy_hash().unwrap().to_string();

    let get = client
        .get_resource_policies()
        .resource_arn("arn:aws:ssm:us-east-1:123456789012:parameter/conf/policy-test")
        .send()
        .await
        .unwrap();
    assert!(!get.policies().is_empty());

    client
        .delete_resource_policy()
        .resource_arn("arn:aws:ssm:us-east-1:123456789012:parameter/conf/policy-test")
        .policy_id(&policy_id)
        .policy_hash(&policy_hash)
        .send()
        .await
        .unwrap();
}

// -- Stubs --

#[test_action("ssm", "GetConnectionStatus", checksum = "5ceb276b")]
#[tokio::test]
async fn ssm_get_connection_status() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client
        .get_connection_status()
        .target("i-00000000000000001")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        Some(&aws_sdk_ssm::types::ConnectionStatus::Connected)
    );
}

#[test_action("ssm", "GetCalendarState", checksum = "ead1c10e")]
#[tokio::test]
async fn ssm_get_calendar_state() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let _ = client
        .get_calendar_state()
        .calendar_names("arn:aws:ssm:us-east-1:123456789012:document/cal")
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "DescribePatchGroupState", checksum = "6ff4c75a")]
#[tokio::test]
async fn ssm_describe_patch_group_state() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let _ = client
        .describe_patch_group_state()
        .patch_group("conf-group")
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "DescribePatchProperties", checksum = "79e19b9c")]
#[tokio::test]
async fn ssm_describe_patch_properties() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let _ = client
        .describe_patch_properties()
        .operating_system(aws_sdk_ssm::types::OperatingSystem::Windows)
        .property(aws_sdk_ssm::types::PatchProperty::Product)
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "GetDefaultPatchBaseline", checksum = "e52823f7")]
#[tokio::test]
async fn ssm_get_default_patch_baseline() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client.get_default_patch_baseline().send().await.unwrap();
    assert!(resp.baseline_id().is_some());
}

#[test_action("ssm", "RegisterDefaultPatchBaseline", checksum = "5b5ac699")]
#[tokio::test]
async fn ssm_register_default_patch_baseline() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create a baseline to register
    let create = client
        .create_patch_baseline()
        .name("conf-default-bl")
        .send()
        .await
        .unwrap();
    let baseline_id = create.baseline_id().unwrap().to_string();

    client
        .register_default_patch_baseline()
        .baseline_id(&baseline_id)
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "DescribeAvailablePatches", checksum = "2d7c2b66")]
#[tokio::test]
async fn ssm_describe_available_patches() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client.describe_available_patches().send().await.unwrap();
    assert!(resp.patches().is_empty());
}

#[test_action("ssm", "GetServiceSetting", checksum = "3419aa2b")]
#[test_action("ssm", "UpdateServiceSetting", checksum = "ea87e0e0")]
#[test_action("ssm", "ResetServiceSetting", checksum = "cfad6810")]
#[tokio::test]
async fn ssm_service_settings() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let _ = client
        .get_service_setting()
        .setting_id("/ssm/parameter-store/high-throughput-enabled")
        .send()
        .await
        .unwrap();

    client
        .update_service_setting()
        .setting_id("/ssm/parameter-store/high-throughput-enabled")
        .setting_value("true")
        .send()
        .await
        .unwrap();

    client
        .reset_service_setting()
        .setting_id("/ssm/parameter-store/high-throughput-enabled")
        .send()
        .await
        .unwrap();
}

// -- Inventory --

#[test_action("ssm", "PutInventory", checksum = "786481b4")]
#[test_action("ssm", "GetInventory", checksum = "b835d42b")]
#[test_action("ssm", "GetInventorySchema", checksum = "3e900dba")]
#[test_action("ssm", "ListInventoryEntries", checksum = "8b96bfd7")]
#[test_action("ssm", "DeleteInventory", checksum = "85aaa21a")]
#[test_action("ssm", "DescribeInventoryDeletions", checksum = "14715864")]
#[tokio::test]
async fn ssm_inventory_lifecycle() {
    use aws_sdk_ssm::types::InventoryItem;

    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let item = InventoryItem::builder()
        .type_name("Custom:ConfApp")
        .schema_version("1.0")
        .capture_time("2024-01-01T00:00:00Z")
        .content({
            let mut map = std::collections::HashMap::new();
            map.insert("Name".to_string(), "ConfApp".to_string());
            map
        })
        .build()
        .unwrap();

    client
        .put_inventory()
        .instance_id("i-conf001")
        .items(item)
        .send()
        .await
        .unwrap();

    let resp = client.get_inventory().send().await.unwrap();
    assert!(!resp.entities().is_empty());

    let resp = client.get_inventory_schema().send().await.unwrap();
    assert!(!resp.schemas().is_empty());

    let resp = client
        .list_inventory_entries()
        .instance_id("i-conf001")
        .type_name("Custom:ConfApp")
        .send()
        .await
        .unwrap();
    assert!(!resp.entries().is_empty());

    let resp = client
        .delete_inventory()
        .type_name("Custom:ConfApp")
        .send()
        .await
        .unwrap();
    assert!(resp.deletion_id().is_some());

    let resp = client.describe_inventory_deletions().send().await.unwrap();
    assert!(!resp.inventory_deletions().is_empty());
}

// -- Compliance --

#[test_action("ssm", "PutComplianceItems", checksum = "0aa021e4")]
#[test_action("ssm", "ListComplianceItems", checksum = "2df340a8")]
#[test_action("ssm", "ListComplianceSummaries", checksum = "926a1629")]
#[test_action("ssm", "ListResourceComplianceSummaries", checksum = "ca169aac")]
#[tokio::test]
async fn ssm_compliance_lifecycle() {
    use aws_sdk_ssm::types::{
        ComplianceExecutionSummary, ComplianceItemEntry, ComplianceSeverity, ComplianceStatus,
    };

    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let exec_summary = ComplianceExecutionSummary::builder()
        .execution_time(aws_sdk_ssm::primitives::DateTime::from_secs(1704067200))
        .build()
        .unwrap();

    let item = ComplianceItemEntry::builder()
        .severity(ComplianceSeverity::Critical)
        .status(ComplianceStatus::Compliant)
        .title("Conf patch")
        .id("patch-conf-001")
        .build()
        .unwrap();

    client
        .put_compliance_items()
        .resource_id("i-conf001")
        .resource_type("ManagedInstance")
        .compliance_type("Custom:ConfPatch")
        .execution_summary(exec_summary)
        .items(item)
        .send()
        .await
        .unwrap();

    let resp = client.list_compliance_items().send().await.unwrap();
    assert!(!resp.compliance_items().is_empty());

    let resp = client.list_compliance_summaries().send().await.unwrap();
    assert!(!resp.compliance_summary_items().is_empty());

    let resp = client
        .list_resource_compliance_summaries()
        .send()
        .await
        .unwrap();
    assert!(!resp.resource_compliance_summary_items().is_empty());
}

// -- Maintenance Window Details --

#[test_action("ssm", "UpdateMaintenanceWindowTarget", checksum = "e4fc5bc1")]
#[test_action("ssm", "UpdateMaintenanceWindowTask", checksum = "986d0ee9")]
#[test_action("ssm", "GetMaintenanceWindowTask", checksum = "55896b29")]
#[test_action("ssm", "DescribeMaintenanceWindowExecutions", checksum = "e1b44cbf")]
#[test_action("ssm", "DescribeMaintenanceWindowSchedule", checksum = "e4fb18b6")]
#[test_action("ssm", "DescribeMaintenanceWindowsForTarget", checksum = "e509e63a")]
#[tokio::test]
async fn ssm_maintenance_window_details() {
    use aws_sdk_ssm::types::{MaintenanceWindowResourceType, MaintenanceWindowTaskType, Target};

    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let mw = client
        .create_maintenance_window()
        .name("conf-mw")
        .schedule("cron(0 2 ? * SUN *)")
        .duration(3)
        .cutoff(1)
        .allow_unassociated_targets(true)
        .send()
        .await
        .unwrap();
    let window_id = mw.window_id().unwrap().to_string();

    let target = Target::builder()
        .key("InstanceIds")
        .values("i-conf001")
        .build();
    let reg = client
        .register_target_with_maintenance_window()
        .window_id(&window_id)
        .resource_type(MaintenanceWindowResourceType::Instance)
        .targets(target)
        .name("conf-target")
        .send()
        .await
        .unwrap();
    let target_id = reg.window_target_id().unwrap().to_string();

    client
        .update_maintenance_window_target()
        .window_id(&window_id)
        .window_target_id(&target_id)
        .name("conf-target-updated")
        .send()
        .await
        .unwrap();

    let task = client
        .register_task_with_maintenance_window()
        .window_id(&window_id)
        .task_arn("AWS-RunShellScript")
        .task_type(MaintenanceWindowTaskType::RunCommand)
        .name("conf-task")
        .send()
        .await
        .unwrap();
    let task_id = task.window_task_id().unwrap().to_string();

    let resp = client
        .get_maintenance_window_task()
        .window_id(&window_id)
        .window_task_id(&task_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.task_arn().unwrap(), "AWS-RunShellScript");

    client
        .update_maintenance_window_task()
        .window_id(&window_id)
        .window_task_id(&task_id)
        .name("conf-task-updated")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_maintenance_window_executions()
        .window_id(&window_id)
        .send()
        .await
        .unwrap();
    assert!(resp.window_executions().is_empty());

    let resp = client
        .describe_maintenance_window_schedule()
        .send()
        .await
        .unwrap();
    assert!(resp.scheduled_window_executions().is_empty());

    let target = Target::builder()
        .key("InstanceIds")
        .values("i-conf001")
        .build();
    let resp = client
        .describe_maintenance_windows_for_target()
        .resource_type(MaintenanceWindowResourceType::Instance)
        .targets(target)
        .send()
        .await
        .unwrap();
    assert!(!resp.window_identities().is_empty());
}

#[test_action("ssm", "GetMaintenanceWindowExecution", checksum = "068e4bcf")]
#[test_action("ssm", "GetMaintenanceWindowExecutionTask", checksum = "09f909cd")]
#[test_action(
    "ssm",
    "GetMaintenanceWindowExecutionTaskInvocation",
    checksum = "e3b36580"
)]
#[test_action(
    "ssm",
    "DescribeMaintenanceWindowExecutionTasks",
    checksum = "65622cb3"
)]
#[test_action(
    "ssm",
    "DescribeMaintenanceWindowExecutionTaskInvocations",
    checksum = "842d26d2"
)]
#[test_action("ssm", "CancelMaintenanceWindowExecution", checksum = "f444e670")]
#[tokio::test]
async fn ssm_maintenance_window_execution_details() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let mw = client
        .create_maintenance_window()
        .name("conf-mw-exec")
        .schedule("cron(0 2 ? * SUN *)")
        .duration(3)
        .cutoff(1)
        .allow_unassociated_targets(true)
        .send()
        .await
        .unwrap();
    let window_id = mw.window_id().unwrap().to_string();

    // DescribeMaintenanceWindowExecutions (empty)
    let resp = client
        .describe_maintenance_window_executions()
        .window_id(&window_id)
        .send()
        .await
        .unwrap();
    assert!(resp.window_executions().is_empty());

    // Get operations return errors for non-existent IDs
    let result = client
        .get_maintenance_window_execution()
        .window_execution_id("nonexistent-exec")
        .send()
        .await;
    assert!(result.is_err());

    let result = client
        .get_maintenance_window_execution_task()
        .window_execution_id("nonexistent-exec")
        .task_id("nonexistent-task")
        .send()
        .await;
    assert!(result.is_err());

    let result = client
        .get_maintenance_window_execution_task_invocation()
        .window_execution_id("nonexistent-exec")
        .task_id("nonexistent-task")
        .invocation_id("nonexistent-inv")
        .send()
        .await;
    assert!(result.is_err());

    // Describe operations return empty for non-existent executions
    let resp = client
        .describe_maintenance_window_execution_tasks()
        .window_execution_id("nonexistent-exec")
        .send()
        .await
        .unwrap();
    assert!(resp.window_execution_task_identities().is_empty());

    let resp = client
        .describe_maintenance_window_execution_task_invocations()
        .window_execution_id("nonexistent-exec")
        .task_id("nonexistent-task")
        .send()
        .await
        .unwrap();
    assert!(resp
        .window_execution_task_invocation_identities()
        .is_empty());

    let result = client
        .cancel_maintenance_window_execution()
        .window_execution_id("nonexistent-exec")
        .send()
        .await;
    assert!(result.is_err());
}

// -- Patch Management Details --

#[test_action("ssm", "UpdatePatchBaseline", checksum = "fec284b2")]
#[tokio::test]
async fn ssm_update_patch_baseline() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_patch_baseline()
        .name("conf-update-bl")
        .send()
        .await
        .unwrap();
    let baseline_id = create.baseline_id().unwrap().to_string();

    let resp = client
        .update_patch_baseline()
        .baseline_id(&baseline_id)
        .name("conf-updated-bl")
        .approved_patches("KB001")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "conf-updated-bl");
    assert_eq!(resp.approved_patches().len(), 1);
}

#[test_action("ssm", "DescribeInstancePatchStates", checksum = "9e721313")]
#[tokio::test]
async fn ssm_describe_instance_patch_states() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client
        .describe_instance_patch_states()
        .instance_ids("i-001")
        .send()
        .await
        .unwrap();
    assert!(resp.instance_patch_states().is_empty());
}

#[test_action(
    "ssm",
    "DescribeInstancePatchStatesForPatchGroup",
    checksum = "6671823d"
)]
#[tokio::test]
async fn ssm_describe_instance_patch_states_for_patch_group() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client
        .describe_instance_patch_states_for_patch_group()
        .patch_group("conf-group")
        .send()
        .await
        .unwrap();
    assert!(resp.instance_patch_states().is_empty());
}

#[test_action("ssm", "DescribeInstancePatches", checksum = "8f6af635")]
#[tokio::test]
async fn ssm_describe_instance_patches() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client
        .describe_instance_patches()
        .instance_id("i-001")
        .send()
        .await
        .unwrap();
    assert!(resp.patches().is_empty());
}

#[test_action(
    "ssm",
    "DescribeEffectivePatchesForPatchBaseline",
    checksum = "cfe6c8da"
)]
#[tokio::test]
async fn ssm_describe_effective_patches() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_patch_baseline()
        .name("conf-eff-bl")
        .send()
        .await
        .unwrap();
    let baseline_id = create.baseline_id().unwrap().to_string();

    let resp = client
        .describe_effective_patches_for_patch_baseline()
        .baseline_id(&baseline_id)
        .send()
        .await
        .unwrap();
    assert!(resp.effective_patches().is_empty());
}

#[test_action("ssm", "GetDeployablePatchSnapshotForInstance", checksum = "b6b2fc7a")]
#[tokio::test]
async fn ssm_get_deployable_patch_snapshot() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client
        .get_deployable_patch_snapshot_for_instance()
        .instance_id("i-001")
        .snapshot_id("snap-conf-001")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.instance_id().unwrap(), "i-001");
    assert_eq!(resp.snapshot_id().unwrap(), "snap-conf-001");
}

// -- Resource Data Sync --

#[test_action("ssm", "CreateResourceDataSync", checksum = "e1d94684")]
#[test_action("ssm", "ListResourceDataSync", checksum = "52827e80")]
#[test_action("ssm", "UpdateResourceDataSync", checksum = "6685f0eb")]
#[test_action("ssm", "DeleteResourceDataSync", checksum = "6d74a15c")]
#[tokio::test]
async fn ssm_resource_data_sync() {
    use aws_sdk_ssm::types::ResourceDataSyncSource;

    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let source = ResourceDataSyncSource::builder()
        .source_type("AWS")
        .source_regions("us-east-1")
        .include_future_regions(false)
        .build()
        .unwrap();

    client
        .create_resource_data_sync()
        .sync_name("conf-sync")
        .sync_type("SyncFromSource")
        .sync_source(source)
        .send()
        .await
        .unwrap();

    let resp = client.list_resource_data_sync().send().await.unwrap();
    assert_eq!(resp.resource_data_sync_items().len(), 1);

    let updated_source = ResourceDataSyncSource::builder()
        .source_type("AWS")
        .source_regions("us-east-1")
        .source_regions("us-west-2")
        .include_future_regions(false)
        .build()
        .unwrap();

    client
        .update_resource_data_sync()
        .sync_name("conf-sync")
        .sync_type("SyncFromSource")
        .sync_source(updated_source)
        .send()
        .await
        .unwrap();

    client
        .delete_resource_data_sync()
        .sync_name("conf-sync")
        .send()
        .await
        .unwrap();
}

// -- GetOpsSummary --

#[test_action("ssm", "GetOpsSummary", checksum = "c5144b1c")]
#[tokio::test]
async fn ssm_get_ops_summary() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client.get_ops_summary().send().await.unwrap();
    assert!(resp.entities().is_empty());
}

// -- OpsItem Related Items --

#[test_action("ssm", "AssociateOpsItemRelatedItem", checksum = "a54779e7")]
#[test_action("ssm", "DisassociateOpsItemRelatedItem", checksum = "f99bed51")]
#[test_action("ssm", "ListOpsItemRelatedItems", checksum = "ebe48d0d")]
#[tokio::test]
async fn ssm_ops_item_related_items() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_ops_item()
        .title("Related")
        .source("conf-test")
        .send()
        .await
        .unwrap();
    let ops_item_id = create.ops_item_id().unwrap().to_string();

    let assoc = client
        .associate_ops_item_related_item()
        .ops_item_id(&ops_item_id)
        .association_type("IsParentOf")
        .resource_type("AWS::SSMIncidents::IncidentRecord")
        .resource_uri("arn:aws:ssm-incidents::123456789012:incident-record/conf")
        .send()
        .await
        .unwrap();
    let assoc_id = assoc.association_id().unwrap().to_string();

    let list = client
        .list_ops_item_related_items()
        .ops_item_id(&ops_item_id)
        .send()
        .await
        .unwrap();
    assert_eq!(list.summaries().len(), 1);

    client
        .disassociate_ops_item_related_item()
        .ops_item_id(&ops_item_id)
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "ListOpsItemEvents", checksum = "543a37ae")]
#[tokio::test]
async fn ssm_list_ops_item_events() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;
    let resp = client.list_ops_item_events().send().await.unwrap();
    assert!(resp.summaries().is_empty());
}

// -- OpsMetadata --

#[test_action("ssm", "CreateOpsMetadata", checksum = "78160ee3")]
#[test_action("ssm", "GetOpsMetadata", checksum = "ae27a60e")]
#[test_action("ssm", "UpdateOpsMetadata", checksum = "8f8c0f08")]
#[test_action("ssm", "ListOpsMetadata", checksum = "3d271490")]
#[test_action("ssm", "DeleteOpsMetadata", checksum = "25691922")]
#[tokio::test]
async fn ssm_ops_metadata_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_ops_metadata()
        .resource_id("conf-resource")
        .metadata(
            "confKey",
            aws_sdk_ssm::types::MetadataValue::builder()
                .value("confVal")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.ops_metadata_arn().unwrap().to_string();

    let get = client
        .get_ops_metadata()
        .ops_metadata_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get.resource_id().unwrap(), "conf-resource");

    client
        .update_ops_metadata()
        .ops_metadata_arn(&arn)
        .metadata_to_update(
            "key2",
            aws_sdk_ssm::types::MetadataValue::builder()
                .value("val2")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let list = client.list_ops_metadata().send().await.unwrap();
    assert_eq!(list.ops_metadata_list().len(), 1);

    client
        .delete_ops_metadata()
        .ops_metadata_arn(&arn)
        .send()
        .await
        .unwrap();
}

// -- Automation --

#[test_action("ssm", "StartAutomationExecution", checksum = "623d906e")]
#[test_action("ssm", "GetAutomationExecution", checksum = "77a91a2d")]
#[test_action("ssm", "DescribeAutomationExecutions", checksum = "630f0f37")]
#[test_action("ssm", "DescribeAutomationStepExecutions", checksum = "837ae952")]
#[test_action("ssm", "SendAutomationSignal", checksum = "d85c40bb")]
#[test_action("ssm", "StopAutomationExecution", checksum = "4200ac33")]
#[tokio::test]
async fn ssm_automation_execution_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let start = client
        .start_automation_execution()
        .document_name("AWS-RunShellScript")
        .send()
        .await
        .unwrap();
    let exec_id = start.automation_execution_id().unwrap().to_string();

    let get = client
        .get_automation_execution()
        .automation_execution_id(&exec_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.automation_execution().unwrap().document_name().unwrap(),
        "AWS-RunShellScript"
    );

    let desc = client
        .describe_automation_executions()
        .send()
        .await
        .unwrap();
    assert!(!desc.automation_execution_metadata_list().is_empty());

    let steps = client
        .describe_automation_step_executions()
        .automation_execution_id(&exec_id)
        .send()
        .await
        .unwrap();
    assert!(steps.step_executions().is_empty());

    client
        .send_automation_signal()
        .automation_execution_id(&exec_id)
        .signal_type(aws_sdk_ssm::types::SignalType::Approve)
        .send()
        .await
        .unwrap();

    client
        .stop_automation_execution()
        .automation_execution_id(&exec_id)
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "StartChangeRequestExecution", checksum = "c37a3f1c")]
#[tokio::test]
async fn ssm_start_change_request_execution() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let runbook = aws_sdk_ssm::types::Runbook::builder()
        .document_name("AWS-RunShellScript")
        .build()
        .unwrap();
    let resp = client
        .start_change_request_execution()
        .document_name("AWS-ChangeManager")
        .runbooks(runbook)
        .send()
        .await
        .unwrap();
    assert!(resp.automation_execution_id().is_some());
}

#[test_action("ssm", "StartExecutionPreview", checksum = "db7e07c5")]
#[test_action("ssm", "GetExecutionPreview", checksum = "91faf997")]
#[tokio::test]
async fn ssm_execution_preview() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let start = client
        .start_execution_preview()
        .document_name("AWS-RunShellScript")
        .send()
        .await
        .unwrap();
    let preview_id = start.execution_preview_id().unwrap().to_string();

    let get = client
        .get_execution_preview()
        .execution_preview_id(&preview_id)
        .send()
        .await
        .unwrap();
    assert!(get.execution_preview_id().is_some());
}

// -- Sessions --

#[test_action("ssm", "StartSession", checksum = "bbfb0d76")]
#[test_action("ssm", "ResumeSession", checksum = "da827500")]
#[test_action("ssm", "DescribeSessions", checksum = "6bc26ec4")]
#[test_action("ssm", "TerminateSession", checksum = "e8d1b586")]
#[tokio::test]
async fn ssm_session_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let start = client
        .start_session()
        .target("i-conf001")
        .send()
        .await
        .unwrap();
    let session_id = start.session_id().unwrap().to_string();
    assert!(start.token_value().is_some());

    let resume = client
        .resume_session()
        .session_id(&session_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resume.session_id().unwrap(), session_id);

    let desc = client
        .describe_sessions()
        .state(aws_sdk_ssm::types::SessionState::Active)
        .send()
        .await
        .unwrap();
    assert_eq!(desc.sessions().len(), 1);

    client
        .terminate_session()
        .session_id(&session_id)
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "StartAccessRequest", checksum = "1b32a067")]
#[test_action("ssm", "GetAccessToken", checksum = "cff0c4cc")]
#[tokio::test]
async fn ssm_access_request() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client
        .start_access_request()
        .reason("conf-test")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-conf001")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let ar_id = resp.access_request_id().unwrap().to_string();

    let token = client
        .get_access_token()
        .access_request_id(&ar_id)
        .send()
        .await
        .unwrap();
    assert!(token.access_request_status().is_some());
}

// -- Managed Instances --

#[test_action("ssm", "CreateActivation", checksum = "db31fde7")]
#[test_action("ssm", "DescribeActivations", checksum = "5481f5f0")]
#[test_action("ssm", "DeleteActivation", checksum = "3f2973d0")]
#[tokio::test]
async fn ssm_activation_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create = client
        .create_activation()
        .iam_role("SSMServiceRole")
        .description("conf activation")
        .send()
        .await
        .unwrap();
    let activation_id = create.activation_id().unwrap().to_string();
    assert!(create.activation_code().is_some());

    let desc = client.describe_activations().send().await.unwrap();
    assert_eq!(desc.activation_list().len(), 1);

    client
        .delete_activation()
        .activation_id(&activation_id)
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "DeregisterManagedInstance", checksum = "c3e05cb9")]
#[tokio::test]
async fn ssm_deregister_managed_instance() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    client
        .deregister_managed_instance()
        .instance_id("mi-conf001")
        .send()
        .await
        .unwrap();
}

#[test_action("ssm", "DescribeInstanceInformation", checksum = "2b439b42")]
#[tokio::test]
async fn ssm_describe_instance_information() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client.describe_instance_information().send().await.unwrap();
    assert!(resp.instance_information_list().is_empty());
}

#[test_action("ssm", "DescribeInstanceProperties", checksum = "0a3f70ed")]
#[tokio::test]
async fn ssm_describe_instance_properties() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client.describe_instance_properties().send().await.unwrap();
    assert!(resp.instance_properties().is_empty());
}

#[test_action("ssm", "UpdateManagedInstanceRole", checksum = "de3c4cf2")]
#[tokio::test]
async fn ssm_update_managed_instance_role() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // This should fail since instance doesn't exist, but we test the endpoint works
    let result = client
        .update_managed_instance_role()
        .instance_id("mi-conf001")
        .iam_role("NewRole")
        .send()
        .await;
    assert!(result.is_err()); // Expected: instance not found
}

// -- Other --

#[test_action("ssm", "ListNodes", checksum = "11c898a4")]
#[tokio::test]
async fn ssm_list_nodes() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client.list_nodes().send().await.unwrap();
    assert!(resp.nodes().is_empty());
}

#[test_action("ssm", "ListNodesSummary", checksum = "5f0509db")]
#[tokio::test]
async fn ssm_list_nodes_summary() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client
        .list_nodes_summary()
        .aggregators(
            aws_sdk_ssm::types::NodeAggregator::builder()
                .aggregator_type(aws_sdk_ssm::types::NodeAggregatorType::Count)
                .type_name(aws_sdk_ssm::types::NodeTypeName::Instance)
                .attribute_name(aws_sdk_ssm::types::NodeAttributeName::AgentVersion)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.summary().is_empty());
}

#[test_action("ssm", "DescribeEffectiveInstanceAssociations", checksum = "20fc257d")]
#[tokio::test]
async fn ssm_describe_effective_instance_associations() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client
        .describe_effective_instance_associations()
        .instance_id("i-conf001")
        .send()
        .await
        .unwrap();
    assert!(resp.associations().is_empty());
}

#[test_action("ssm", "DescribeInstanceAssociationsStatus", checksum = "309b1833")]
#[tokio::test]
async fn ssm_describe_instance_associations_status() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client
        .describe_instance_associations_status()
        .instance_id("i-conf001")
        .send()
        .await
        .unwrap();
    assert!(resp.instance_association_status_infos().is_empty());
}
