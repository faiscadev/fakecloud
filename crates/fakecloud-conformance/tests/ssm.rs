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
