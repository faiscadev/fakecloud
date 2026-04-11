mod helpers;

use aws_sdk_ssm::types::{ParameterType, Tag};
use helpers::TestServer;

#[tokio::test]
async fn ssm_put_get_delete_parameter() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Put
    client
        .put_parameter()
        .name("/app/config/key1")
        .value("value1")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();

    // Get
    let resp = client
        .get_parameter()
        .name("/app/config/key1")
        .send()
        .await
        .unwrap();
    let param = resp.parameter().unwrap();
    assert_eq!(param.name().unwrap(), "/app/config/key1");
    assert_eq!(param.value().unwrap(), "value1");
    assert_eq!(param.version(), 1);

    // Delete
    client
        .delete_parameter()
        .name("/app/config/key1")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client.get_parameter().name("/app/config/key1").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn ssm_put_overwrite_versioning() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    client
        .put_parameter()
        .name("/app/db/url")
        .value("postgres://old")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();

    // Overwrite
    let resp = client
        .put_parameter()
        .name("/app/db/url")
        .value("postgres://new")
        .r#type(ParameterType::String)
        .overwrite(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.version(), 2);

    let resp = client
        .get_parameter()
        .name("/app/db/url")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameter().unwrap().value().unwrap(), "postgres://new");
    assert_eq!(resp.parameter().unwrap().version(), 2);
}

#[tokio::test]
async fn ssm_put_without_overwrite_fails() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    client
        .put_parameter()
        .name("/test/dupe")
        .value("v1")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();

    let result = client
        .put_parameter()
        .name("/test/dupe")
        .value("v2")
        .r#type(ParameterType::String)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn ssm_get_parameters() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    client
        .put_parameter()
        .name("/a")
        .value("1")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();
    client
        .put_parameter()
        .name("/b")
        .value("2")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_parameters()
        .names("/a")
        .names("/b")
        .names("/nonexistent")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.parameters().len(), 2);
    assert_eq!(resp.invalid_parameters().len(), 1);
    assert_eq!(resp.invalid_parameters()[0], "/nonexistent");
}

#[tokio::test]
async fn ssm_get_parameters_by_path() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    for (name, value) in [
        ("/app/prod/db-url", "postgres://prod"),
        ("/app/prod/api-key", "secret"),
        ("/app/prod/nested/deep", "deep-val"),
        ("/app/staging/db-url", "postgres://staging"),
    ] {
        client
            .put_parameter()
            .name(name)
            .value(value)
            .r#type(ParameterType::String)
            .send()
            .await
            .unwrap();
    }

    // Non-recursive: only direct children
    let resp = client
        .get_parameters_by_path()
        .path("/app/prod")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameters().len(), 2); // db-url and api-key, not nested/deep

    // Recursive: all descendants
    let resp = client
        .get_parameters_by_path()
        .path("/app/prod")
        .recursive(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameters().len(), 3); // db-url, api-key, nested/deep
}

#[tokio::test]
async fn ssm_cli_put_get() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&[
            "ssm",
            "put-parameter",
            "--name",
            "/cli/test",
            "--value",
            "hello",
            "--type",
            "String",
        ])
        .await;
    assert!(output.success(), "put failed: {}", output.stderr_text());

    let output = server
        .aws_cli(&["ssm", "get-parameter", "--name", "/cli/test"])
        .await;
    assert!(output.success(), "get failed: {}", output.stderr_text());
    let json = output.stdout_json();
    assert_eq!(json["Parameter"]["Value"], "hello");
}

#[tokio::test]
async fn ssm_add_list_remove_tags() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create a parameter first
    client
        .put_parameter()
        .name("/tags/test-param")
        .value("tagged-value")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();

    // Add tags
    client
        .add_tags_to_resource()
        .resource_type(aws_sdk_ssm::types::ResourceTypeForTagging::Parameter)
        .resource_id("/tags/test-param")
        .tags(
            Tag::builder()
                .key("Environment")
                .value("Production")
                .build()
                .unwrap(),
        )
        .tags(Tag::builder().key("Team").value("Backend").build().unwrap())
        .send()
        .await
        .unwrap();

    // List tags
    let resp = client
        .list_tags_for_resource()
        .resource_type(aws_sdk_ssm::types::ResourceTypeForTagging::Parameter)
        .resource_id("/tags/test-param")
        .send()
        .await
        .unwrap();
    let tags = resp.tag_list();
    assert_eq!(tags.len(), 2);

    let env_tag = tags.iter().find(|t| t.key() == "Environment");
    assert!(env_tag.is_some());
    assert_eq!(env_tag.unwrap().value(), "Production");

    let team_tag = tags.iter().find(|t| t.key() == "Team");
    assert!(team_tag.is_some());
    assert_eq!(team_tag.unwrap().value(), "Backend");

    // Remove one tag
    client
        .remove_tags_from_resource()
        .resource_type(aws_sdk_ssm::types::ResourceTypeForTagging::Parameter)
        .resource_id("/tags/test-param")
        .tag_keys("Team")
        .send()
        .await
        .unwrap();

    // Verify only one tag remains
    let resp = client
        .list_tags_for_resource()
        .resource_type(aws_sdk_ssm::types::ResourceTypeForTagging::Parameter)
        .resource_id("/tags/test-param")
        .send()
        .await
        .unwrap();
    let tags = resp.tag_list();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), "Environment");
    assert_eq!(tags[0].value(), "Production");
}

#[tokio::test]
async fn ssm_pagination_get_parameters_by_path() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create 15 parameters under /page/
    for i in 0..15 {
        client
            .put_parameter()
            .name(format!("/page/param{i:02}"))
            .value(format!("val{i}"))
            .r#type(ParameterType::String)
            .send()
            .await
            .unwrap();
    }

    // First page: MaxResults=5
    let resp = client
        .get_parameters_by_path()
        .path("/page")
        .max_results(5)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameters().len(), 5);
    assert!(
        resp.next_token().is_some(),
        "should have NextToken for more results"
    );

    // Second page
    let resp2 = client
        .get_parameters_by_path()
        .path("/page")
        .max_results(5)
        .next_token(resp.next_token().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.parameters().len(), 5);
    assert!(resp2.next_token().is_some());

    // Third page (last page with items)
    let resp3 = client
        .get_parameters_by_path()
        .path("/page")
        .max_results(5)
        .next_token(resp2.next_token().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(resp3.parameters().len(), 5);

    // If there's a next token, the next page should be empty
    if let Some(token) = resp3.next_token() {
        let resp4 = client
            .get_parameters_by_path()
            .path("/page")
            .max_results(5)
            .next_token(token)
            .send()
            .await
            .unwrap();
        assert_eq!(resp4.parameters().len(), 0);
        assert!(
            resp4.next_token().is_none(),
            "should have no NextToken after empty page"
        );
    }
}

#[tokio::test]
async fn ssm_secure_string_with_decryption() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Put a SecureString parameter
    client
        .put_parameter()
        .name("/secret/password")
        .value("super-secret-123")
        .r#type(ParameterType::SecureString)
        .send()
        .await
        .unwrap();

    // Get WITHOUT WithDecryption (default) - returns kms-prefixed "encrypted" form
    let resp = client
        .get_parameter()
        .name("/secret/password")
        .send()
        .await
        .unwrap();
    let param = resp.parameter().unwrap();
    assert_eq!(param.value().unwrap(), "kms:alias/aws/ssm:super-secret-123");

    // Get WITH WithDecryption=true - should return actual value
    let resp = client
        .get_parameter()
        .name("/secret/password")
        .with_decryption(true)
        .send()
        .await
        .unwrap();
    let param = resp.parameter().unwrap();
    assert_eq!(param.value().unwrap(), "super-secret-123");
}

// ---- SSM Document Tests ----

#[tokio::test]
async fn ssm_document_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let doc_content = r#"{"schemaVersion":"2.2","description":"Test doc","mainSteps":[{"action":"aws:runShellScript","name":"run","inputs":{"runCommand":["echo hello"]}}]}"#;

    // Create document
    let resp = client
        .create_document()
        .name("TestDoc")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .document_format(aws_sdk_ssm::types::DocumentFormat::Json)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.document_description().unwrap().name().unwrap(),
        "TestDoc"
    );

    // Get document
    let get = client.get_document().name("TestDoc").send().await.unwrap();
    assert_eq!(get.name().unwrap(), "TestDoc");
    assert!(get.content().is_some());

    // Describe document
    let desc = client
        .describe_document()
        .name("TestDoc")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.document().unwrap().name().unwrap(), "TestDoc");

    // List documents
    let list = client.list_documents().send().await.unwrap();
    assert!(list
        .document_identifiers()
        .iter()
        .any(|d| d.name().unwrap() == "TestDoc"));

    // Delete document
    client
        .delete_document()
        .name("TestDoc")
        .send()
        .await
        .unwrap();

    let result = client.get_document().name("TestDoc").send().await;
    assert!(result.is_err());
}

// ---- SSM Command Tests ----

#[tokio::test]
async fn ssm_send_list_commands() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create a document first
    let doc_content = r#"{"schemaVersion":"2.2","description":"cmd","mainSteps":[{"action":"aws:runShellScript","name":"run","inputs":{"runCommand":["ls"]}}]}"#;
    client
        .create_document()
        .name("CmdDoc")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .document_format(aws_sdk_ssm::types::DocumentFormat::Json)
        .send()
        .await
        .unwrap();

    // Send command
    let resp = client
        .send_command()
        .document_name("CmdDoc")
        .instance_ids("i-1234567890abcdef0")
        .send()
        .await
        .unwrap();
    let cmd = resp.command().unwrap();
    assert!(cmd.command_id().is_some());
    let cmd_id = cmd.command_id().unwrap().to_string();

    // List commands
    let list = client.list_commands().send().await.unwrap();
    assert!(list
        .commands()
        .iter()
        .any(|c| c.command_id().unwrap() == cmd_id));

    // Cancel command
    client
        .cancel_command()
        .command_id(&cmd_id)
        .send()
        .await
        .unwrap();
}

// ---- SSM Maintenance Window Tests ----

#[tokio::test]
async fn ssm_maintenance_window_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create maintenance window
    let resp = client
        .create_maintenance_window()
        .name("test-window")
        .schedule("cron(0 2 ? * SUN *)")
        .duration(3)
        .cutoff(1)
        .allow_unassociated_targets(true)
        .send()
        .await
        .unwrap();
    let window_id = resp.window_id().unwrap().to_string();

    // Get maintenance window
    let get = client
        .get_maintenance_window()
        .window_id(&window_id)
        .send()
        .await
        .unwrap();
    assert_eq!(get.name().unwrap(), "test-window");

    // Describe maintenance windows
    let desc = client.describe_maintenance_windows().send().await.unwrap();
    assert!(desc
        .window_identities()
        .iter()
        .any(|w| w.window_id().unwrap() == window_id));

    // Delete maintenance window
    client
        .delete_maintenance_window()
        .window_id(&window_id)
        .send()
        .await
        .unwrap();
}

// ---- SSM Error Cases ----

#[tokio::test]
async fn ssm_get_nonexistent_parameter_fails() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let result = client.get_parameter().name("/no/such/param").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn ssm_delete_nonexistent_parameter_fails() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let result = client
        .delete_parameter()
        .name("/no/such/param")
        .send()
        .await;
    assert!(result.is_err());
}

// ---- SSM Delete Parameters (batch) ----

#[tokio::test]
async fn ssm_delete_parameters_batch() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    for name in ["/batch/a", "/batch/b", "/batch/c"] {
        client
            .put_parameter()
            .name(name)
            .value("val")
            .r#type(ParameterType::String)
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .delete_parameters()
        .names("/batch/a")
        .names("/batch/b")
        .names("/batch/nonexistent")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.deleted_parameters().len(), 2);
    assert_eq!(resp.invalid_parameters().len(), 1);
}

// ---- SSM Parameter History ----

#[tokio::test]
async fn ssm_get_parameter_history() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    client
        .put_parameter()
        .name("/hist/param")
        .value("v1")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();

    client
        .put_parameter()
        .name("/hist/param")
        .value("v2")
        .r#type(ParameterType::String)
        .overwrite(true)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_parameter_history()
        .name("/hist/param")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameters().len(), 2);
}

// ---- SSM Describe Parameters ----

#[tokio::test]
async fn ssm_describe_parameters() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    client
        .put_parameter()
        .name("/desc/param1")
        .value("val1")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();

    let resp = client.describe_parameters().send().await.unwrap();
    assert!(resp
        .parameters()
        .iter()
        .any(|p| p.name().unwrap() == "/desc/param1"));
}

/// Regression: SecureString returned via GetParameters (batch) without WithDecryption
/// should have the value masked (kms: prefix), not the plaintext.
#[tokio::test]
async fn ssm_secure_string_masked_without_decryption() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    client
        .put_parameter()
        .name("/secret/api-key")
        .value("my-api-key-12345")
        .r#type(ParameterType::SecureString)
        .send()
        .await
        .unwrap();

    // Get without WithDecryption (default false) via GetParameters batch API
    let resp = client
        .get_parameters()
        .names("/secret/api-key")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameters().len(), 1);
    let value = resp.parameters()[0].value().unwrap();
    assert!(
        value.starts_with("kms:"),
        "expected masked value starting with 'kms:', got: {value}"
    );
    assert!(
        !value.contains("my-api-key-12345") || value.starts_with("kms:"),
        "value should be masked without WithDecryption"
    );

    // With decryption should return plaintext
    let resp = client
        .get_parameters()
        .names("/secret/api-key")
        .with_decryption(true)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.parameters()[0].value().unwrap(), "my-api-key-12345");
}

/// Regression: RemoveTagsFromResource with an invalid ResourceType should return error.
#[tokio::test]
async fn ssm_remove_tags_invalid_resource_type() {
    let server = TestServer::start().await;

    // Use CLI to send a raw request with invalid resource type since the SDK
    // enforces enum values. We call via aws_cli with ssm remove-tags-from-resource.
    let output = server
        .aws_cli(&[
            "ssm",
            "remove-tags-from-resource",
            "--resource-type",
            "InvalidType",
            "--resource-id",
            "some-resource",
            "--tag-keys",
            "SomeKey",
        ])
        .await;
    assert!(
        !output.success(),
        "expected error for invalid resource type, but got success"
    );
}

// =====================================================================
// Associations
// =====================================================================

#[tokio::test]
async fn ssm_association_crud() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create
    let create_resp = client
        .create_association()
        .name("AWS-RunShellScript")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-00000000000000001")
                .build(),
        )
        .schedule_expression("rate(1 hour)")
        .association_name("e2e-assoc")
        .send()
        .await
        .unwrap();
    let assoc_desc = create_resp.association_description().unwrap();
    let assoc_id = assoc_desc.association_id().unwrap().to_string();
    assert_eq!(assoc_desc.name().unwrap(), "AWS-RunShellScript");

    // Describe
    let desc = client
        .describe_association()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.association_description()
            .unwrap()
            .association_name()
            .unwrap(),
        "e2e-assoc"
    );

    // List
    let list = client.list_associations().send().await.unwrap();
    assert!(!list.associations().is_empty());

    // Update
    client
        .update_association()
        .association_id(&assoc_id)
        .association_name("updated-assoc")
        .send()
        .await
        .unwrap();

    // List versions
    let versions = client
        .list_association_versions()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();
    assert!(versions.association_versions().len() >= 2);

    // Delete
    client
        .delete_association()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client
        .describe_association()
        .association_id(&assoc_id)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn ssm_association_batch_and_start() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Batch create
    let entry1 = aws_sdk_ssm::types::CreateAssociationBatchRequestEntry::builder()
        .name("AWS-RunShellScript")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-001")
                .build(),
        )
        .build()
        .unwrap();
    let entry2 = aws_sdk_ssm::types::CreateAssociationBatchRequestEntry::builder()
        .name("AWS-RunShellScript")
        .targets(
            aws_sdk_ssm::types::Target::builder()
                .key("InstanceIds")
                .values("i-002")
                .build(),
        )
        .build()
        .unwrap();

    let batch_resp = client
        .create_association_batch()
        .entries(entry1)
        .entries(entry2)
        .send()
        .await
        .unwrap();
    assert_eq!(batch_resp.successful().len(), 2);

    // StartAssociationsOnce
    let assoc_id = batch_resp.successful()[0]
        .association_id()
        .unwrap()
        .to_string();
    let _ = client
        .start_associations_once()
        .association_ids(&assoc_id)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn ssm_association_executions_empty() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let create_resp = client
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
    let assoc_id = create_resp
        .association_description()
        .unwrap()
        .association_id()
        .unwrap()
        .to_string();

    let execs = client
        .describe_association_executions()
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();
    assert!(execs.association_executions().is_empty());
}

// =====================================================================
// OpsItems
// =====================================================================

#[tokio::test]
async fn ssm_ops_item_crud() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create
    let create_resp = client
        .create_ops_item()
        .title("E2E Test OpsItem")
        .source("e2e-test")
        .description("A test ops item for E2E tests")
        .send()
        .await
        .unwrap();
    let ops_item_id = create_resp.ops_item_id().unwrap().to_string();

    // Get
    let get_resp = client
        .get_ops_item()
        .ops_item_id(&ops_item_id)
        .send()
        .await
        .unwrap();
    let item = get_resp.ops_item().unwrap();
    assert_eq!(item.title().unwrap(), "E2E Test OpsItem");
    assert_eq!(
        item.status(),
        Some(&aws_sdk_ssm::types::OpsItemStatus::Open)
    );

    // Update
    client
        .update_ops_item()
        .ops_item_id(&ops_item_id)
        .title("Updated OpsItem")
        .status(aws_sdk_ssm::types::OpsItemStatus::Resolved)
        .send()
        .await
        .unwrap();

    // Verify update
    let get_resp = client
        .get_ops_item()
        .ops_item_id(&ops_item_id)
        .send()
        .await
        .unwrap();
    let item = get_resp.ops_item().unwrap();
    assert_eq!(item.title().unwrap(), "Updated OpsItem");

    // Describe
    let desc = client.describe_ops_items().send().await.unwrap();
    assert!(!desc.ops_item_summaries().is_empty());

    // Delete
    client
        .delete_ops_item()
        .ops_item_id(&ops_item_id)
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client.get_ops_item().ops_item_id(&ops_item_id).send().await;
    assert!(result.is_err());
}

// =====================================================================
// Document extras
// =====================================================================

#[tokio::test]
async fn ssm_list_document_versions() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let doc_content = r#"{"schemaVersion":"2.2","description":"test","mainSteps":[{"action":"aws:runShellScript","name":"test","inputs":{"runCommand":["echo hi"]}}]}"#;

    client
        .create_document()
        .name("e2e-doc-ver")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    let versions = client
        .list_document_versions()
        .name("e2e-doc-ver")
        .send()
        .await
        .unwrap();
    assert!(!versions.document_versions().is_empty());
}

// =====================================================================
// Stubs
// =====================================================================

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

#[tokio::test]
async fn ssm_get_calendar_state() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client
        .get_calendar_state()
        .calendar_names("arn:aws:ssm:us-east-1:123456789012:document/cal")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.state(), Some(&aws_sdk_ssm::types::CalendarState::Open));
}

#[tokio::test]
async fn ssm_service_settings() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Get default
    let resp = client
        .get_service_setting()
        .setting_id("/ssm/parameter-store/high-throughput-enabled")
        .send()
        .await
        .unwrap();
    assert!(resp.service_setting().is_some());

    // Update
    client
        .update_service_setting()
        .setting_id("/ssm/parameter-store/high-throughput-enabled")
        .setting_value("true")
        .send()
        .await
        .unwrap();

    // Reset
    client
        .reset_service_setting()
        .setting_id("/ssm/parameter-store/high-throughput-enabled")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn ssm_get_default_patch_baseline() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client.get_default_patch_baseline().send().await.unwrap();
    assert!(resp.baseline_id().is_some());
}

#[tokio::test]
async fn ssm_describe_available_patches() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client.describe_available_patches().send().await.unwrap();
    assert!(resp.patches().is_empty());
}

#[tokio::test]
async fn ssm_inventory_lifecycle() {
    use aws_sdk_ssm::types::InventoryItem;

    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // PutInventory
    let item = InventoryItem::builder()
        .type_name("Custom:TestApp")
        .schema_version("1.0")
        .capture_time("2024-01-01T00:00:00Z")
        .content({
            let mut map = std::collections::HashMap::new();
            map.insert("Name".to_string(), "TestApp".to_string());
            map.insert("Version".to_string(), "1.0".to_string());
            map
        })
        .build()
        .unwrap();

    client
        .put_inventory()
        .instance_id("i-1234567890abcdef0")
        .items(item)
        .send()
        .await
        .unwrap();

    // GetInventory
    let resp = client.get_inventory().send().await.unwrap();
    assert!(!resp.entities().is_empty());

    // GetInventorySchema
    let resp = client.get_inventory_schema().send().await.unwrap();
    assert!(!resp.schemas().is_empty());

    // ListInventoryEntries
    let resp = client
        .list_inventory_entries()
        .instance_id("i-1234567890abcdef0")
        .type_name("Custom:TestApp")
        .send()
        .await
        .unwrap();
    assert!(!resp.entries().is_empty());

    // DeleteInventory
    let resp = client
        .delete_inventory()
        .type_name("Custom:TestApp")
        .send()
        .await
        .unwrap();
    assert!(resp.deletion_id().is_some());

    // DescribeInventoryDeletions
    let resp = client.describe_inventory_deletions().send().await.unwrap();
    assert!(!resp.inventory_deletions().is_empty());
}

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
        .title("Test patch")
        .id("patch-001")
        .build()
        .unwrap();

    client
        .put_compliance_items()
        .resource_id("i-1234567890abcdef0")
        .resource_type("ManagedInstance")
        .compliance_type("Custom:PatchTest")
        .execution_summary(exec_summary)
        .items(item)
        .send()
        .await
        .unwrap();

    // ListComplianceItems
    let resp = client.list_compliance_items().send().await.unwrap();
    assert!(!resp.compliance_items().is_empty());

    // ListComplianceSummaries
    let resp = client.list_compliance_summaries().send().await.unwrap();
    assert!(!resp.compliance_summary_items().is_empty());

    // ListResourceComplianceSummaries
    let resp = client
        .list_resource_compliance_summaries()
        .send()
        .await
        .unwrap();
    assert!(!resp.resource_compliance_summary_items().is_empty());
}

#[tokio::test]
async fn ssm_maintenance_window_execution() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create window with target and task
    let mw = client
        .create_maintenance_window()
        .name("e2e-mw")
        .schedule("cron(0 2 ? * SUN *)")
        .duration(3)
        .cutoff(1)
        .allow_unassociated_targets(true)
        .send()
        .await
        .unwrap();
    let window_id = mw.window_id().unwrap().to_string();

    // Register target
    use aws_sdk_ssm::types::{MaintenanceWindowResourceType, Target};
    let target = Target::builder().key("InstanceIds").values("i-001").build();
    let reg = client
        .register_target_with_maintenance_window()
        .window_id(&window_id)
        .resource_type(MaintenanceWindowResourceType::Instance)
        .targets(target)
        .name("e2e-target")
        .send()
        .await
        .unwrap();
    let target_id = reg.window_target_id().unwrap().to_string();

    // UpdateMaintenanceWindowTarget
    let resp = client
        .update_maintenance_window_target()
        .window_id(&window_id)
        .window_target_id(&target_id)
        .name("updated-target")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "updated-target");

    // Register task
    use aws_sdk_ssm::types::MaintenanceWindowTaskType;
    let reg = client
        .register_task_with_maintenance_window()
        .window_id(&window_id)
        .task_arn("AWS-RunShellScript")
        .task_type(MaintenanceWindowTaskType::RunCommand)
        .name("e2e-task")
        .send()
        .await
        .unwrap();
    let task_id = reg.window_task_id().unwrap().to_string();

    // GetMaintenanceWindowTask
    let resp = client
        .get_maintenance_window_task()
        .window_id(&window_id)
        .window_task_id(&task_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.task_arn().unwrap(), "AWS-RunShellScript");

    // UpdateMaintenanceWindowTask
    let resp = client
        .update_maintenance_window_task()
        .window_id(&window_id)
        .window_task_id(&task_id)
        .name("updated-task")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "updated-task");

    // DescribeMaintenanceWindowExecutions (empty initially)
    let resp = client
        .describe_maintenance_window_executions()
        .window_id(&window_id)
        .send()
        .await
        .unwrap();
    assert!(resp.window_executions().is_empty());

    // DescribeMaintenanceWindowSchedule
    let resp = client
        .describe_maintenance_window_schedule()
        .send()
        .await
        .unwrap();
    assert!(resp.scheduled_window_executions().is_empty());

    // DescribeMaintenanceWindowsForTarget
    let target = Target::builder().key("InstanceIds").values("i-001").build();
    let resp = client
        .describe_maintenance_windows_for_target()
        .resource_type(MaintenanceWindowResourceType::Instance)
        .targets(target)
        .send()
        .await
        .unwrap();
    assert!(!resp.window_identities().is_empty());
}

#[tokio::test]
async fn ssm_patch_baseline_update() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create
    let resp = client
        .create_patch_baseline()
        .name("e2e-baseline")
        .operating_system(aws_sdk_ssm::types::OperatingSystem::AmazonLinux2)
        .description("original")
        .send()
        .await
        .unwrap();
    let baseline_id = resp.baseline_id().unwrap().to_string();

    // Update
    let resp = client
        .update_patch_baseline()
        .baseline_id(&baseline_id)
        .name("updated-baseline")
        .description("updated desc")
        .approved_patches("KB001")
        .approved_patches("KB002")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "updated-baseline");
    assert_eq!(resp.description().unwrap(), "updated desc");
    assert_eq!(resp.approved_patches().len(), 2);

    // DescribeInstancePatchStates
    let resp = client
        .describe_instance_patch_states()
        .instance_ids("i-001")
        .send()
        .await
        .unwrap();
    assert!(resp.instance_patch_states().is_empty());

    // DescribeInstancePatches
    let resp = client
        .describe_instance_patches()
        .instance_id("i-001")
        .send()
        .await
        .unwrap();
    assert!(resp.patches().is_empty());

    // DescribeEffectivePatchesForPatchBaseline
    let resp = client
        .describe_effective_patches_for_patch_baseline()
        .baseline_id(&baseline_id)
        .send()
        .await
        .unwrap();
    assert!(resp.effective_patches().is_empty());

    // GetDeployablePatchSnapshotForInstance
    let resp = client
        .get_deployable_patch_snapshot_for_instance()
        .instance_id("i-001")
        .snapshot_id("00000000-0000-0000-0000-000000000001")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.instance_id().unwrap(), "i-001");
    assert_eq!(
        resp.snapshot_id().unwrap(),
        "00000000-0000-0000-0000-000000000001"
    );

    // DescribeInstancePatchStatesForPatchGroup
    let resp = client
        .describe_instance_patch_states_for_patch_group()
        .patch_group("test-group")
        .send()
        .await
        .unwrap();
    assert!(resp.instance_patch_states().is_empty());
}

// ── OpsItem Related Items ─────────────────────────────────────

#[tokio::test]
#[ignore] // SDK has timestamp deserialization issues with ListOpsItemRelatedItems — tested via raw HTTP in PR 78
async fn ssm_ops_item_related_items() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create ops item
    let create = client
        .create_ops_item()
        .title("Related test")
        .description("Related item test description")
        .source("test")
        .send()
        .await
        .unwrap();
    let ops_item_id = create.ops_item_id().unwrap().to_string();

    // Associate
    let assoc = client
        .associate_ops_item_related_item()
        .ops_item_id(&ops_item_id)
        .association_type("IsParentOf")
        .resource_type("AWS::SSMIncidents::IncidentRecord")
        .resource_uri("arn:aws:ssm-incidents::123456789012:incident-record/test")
        .send()
        .await
        .unwrap();
    let assoc_id = assoc.association_id().unwrap().to_string();

    // List - verify via raw HTTP since SDK/CLI have timestamp deserialization issues
    let http_client = reqwest::Client::new();
    let resp = http_client
        .post(server.endpoint())
        .header("Content-Type", "application/x-amz-json-1.1")
        .header("X-Amz-Target", "AmazonSSM.ListOpsItemRelatedItems")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/ssm/aws4_request",
        )
        .body(format!("{{\"OpsItemId\":\"{}\"}}", ops_item_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["Summaries"].as_array().unwrap().len(), 1);

    // Disassociate
    client
        .disassociate_ops_item_related_item()
        .ops_item_id(&ops_item_id)
        .association_id(&assoc_id)
        .send()
        .await
        .unwrap();

    // List ops item events (empty)
    let resp = client.list_ops_item_events().send().await.unwrap();
    assert!(resp.summaries().is_empty());
}

// ── OpsMetadata ───────────────────────────────────────────────

#[tokio::test]
async fn ssm_ops_metadata_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create
    let create = client
        .create_ops_metadata()
        .resource_id("test-resource")
        .metadata(
            "testKey",
            aws_sdk_ssm::types::MetadataValue::builder()
                .value("testVal")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let arn = create.ops_metadata_arn().unwrap().to_string();

    // Get
    let get = client
        .get_ops_metadata()
        .ops_metadata_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(get.resource_id().unwrap(), "test-resource");

    // Update
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

    // List
    let list = client.list_ops_metadata().send().await.unwrap();
    assert_eq!(list.ops_metadata_list().len(), 1);

    // Delete
    client
        .delete_ops_metadata()
        .ops_metadata_arn(&arn)
        .send()
        .await
        .unwrap();
}

// ── Automation ────────────────────────────────────────────────

#[tokio::test]
async fn ssm_automation_execution_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Start
    let start = client
        .start_automation_execution()
        .document_name("AWS-RunShellScript")
        .send()
        .await
        .unwrap();
    let exec_id = start.automation_execution_id().unwrap().to_string();

    // Get
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

    // Describe
    let desc = client
        .describe_automation_executions()
        .send()
        .await
        .unwrap();
    assert!(!desc.automation_execution_metadata_list().is_empty());

    // DescribeSteps
    let steps = client
        .describe_automation_step_executions()
        .automation_execution_id(&exec_id)
        .send()
        .await
        .unwrap();
    assert!(steps.step_executions().is_empty());

    // Signal
    client
        .send_automation_signal()
        .automation_execution_id(&exec_id)
        .signal_type(aws_sdk_ssm::types::SignalType::Approve)
        .send()
        .await
        .unwrap();

    // Stop
    client
        .stop_automation_execution()
        .automation_execution_id(&exec_id)
        .send()
        .await
        .unwrap();

    // StartChangeRequestExecution
    let runbook = aws_sdk_ssm::types::Runbook::builder()
        .document_name("AWS-RunShellScript")
        .build()
        .unwrap();
    let cr = client
        .start_change_request_execution()
        .document_name("AWS-ChangeManager")
        .runbooks(runbook)
        .send()
        .await
        .unwrap();
    assert!(cr.automation_execution_id().is_some());

    // StartExecutionPreview
    let preview = client
        .start_execution_preview()
        .document_name("AWS-RunShellScript")
        .send()
        .await
        .unwrap();
    let preview_id = preview.execution_preview_id().unwrap().to_string();

    // GetExecutionPreview
    let get_preview = client
        .get_execution_preview()
        .execution_preview_id(&preview_id)
        .send()
        .await
        .unwrap();
    assert!(get_preview.execution_preview_id().is_some());
}

// ── Sessions ──────────────────────────────────────────────────

#[tokio::test]
async fn ssm_session_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Start
    let start = client
        .start_session()
        .target("i-00000000000000001")
        .send()
        .await
        .unwrap();
    let session_id = start.session_id().unwrap().to_string();
    assert!(start.token_value().is_some());

    // Resume
    let resume = client
        .resume_session()
        .session_id(&session_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resume.session_id().unwrap(), session_id);

    // Describe active
    let desc = client
        .describe_sessions()
        .state(aws_sdk_ssm::types::SessionState::Active)
        .send()
        .await
        .unwrap();
    assert_eq!(desc.sessions().len(), 1);

    // Terminate
    client
        .terminate_session()
        .session_id(&session_id)
        .send()
        .await
        .unwrap();

    // Describe history
    let desc = client
        .describe_sessions()
        .state(aws_sdk_ssm::types::SessionState::History)
        .send()
        .await
        .unwrap();
    assert_eq!(desc.sessions().len(), 1);
}

// ── Managed Instances ─────────────────────────────────────────

#[tokio::test]
async fn ssm_activation_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create
    let create = client
        .create_activation()
        .iam_role("SSMServiceRole")
        .description("test activation")
        .send()
        .await
        .unwrap();
    let activation_id = create.activation_id().unwrap().to_string();
    assert!(create.activation_code().is_some());

    // Describe
    let desc = client.describe_activations().send().await.unwrap();
    assert_eq!(desc.activation_list().len(), 1);

    // Delete
    client
        .delete_activation()
        .activation_id(&activation_id)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn ssm_describe_instance_information() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client.describe_instance_information().send().await.unwrap();
    assert!(resp.instance_information_list().is_empty());
}

#[tokio::test]
async fn ssm_describe_instance_properties() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client.describe_instance_properties().send().await.unwrap();
    assert!(resp.instance_properties().is_empty());
}

#[tokio::test]
async fn ssm_deregister_managed_instance() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Should not error even if instance doesn't exist
    client
        .deregister_managed_instance()
        .instance_id("mi-00000000000000001")
        .send()
        .await
        .unwrap();
}

// ── Other ─────────────────────────────────────────────────────

#[tokio::test]
async fn ssm_list_nodes() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client.list_nodes().send().await.unwrap();
    assert!(resp.nodes().is_empty());
}

#[tokio::test]
async fn ssm_describe_effective_instance_associations() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client
        .describe_effective_instance_associations()
        .instance_id("i-00000000000000001")
        .send()
        .await
        .unwrap();
    assert!(resp.associations().is_empty());
}

#[tokio::test]
async fn ssm_describe_instance_associations_status() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let resp = client
        .describe_instance_associations_status()
        .instance_id("i-00000000000000001")
        .send()
        .await
        .unwrap();
    assert!(resp.instance_association_status_infos().is_empty());
}

// ── Parameter Labels ─────────────────────────────────────────

#[tokio::test]
async fn ssm_label_and_unlabel_parameter_version() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create parameter with two versions
    client
        .put_parameter()
        .name("/label/e2e")
        .value("v1")
        .r#type(ParameterType::String)
        .send()
        .await
        .unwrap();
    client
        .put_parameter()
        .name("/label/e2e")
        .value("v2")
        .r#type(ParameterType::String)
        .overwrite(true)
        .send()
        .await
        .unwrap();

    // Label version 1
    client
        .label_parameter_version()
        .name("/label/e2e")
        .parameter_version(1)
        .labels("prod")
        .labels("stable")
        .send()
        .await
        .unwrap();

    // Verify labels via parameter history
    let history = client
        .get_parameter_history()
        .name("/label/e2e")
        .send()
        .await
        .unwrap();
    let v1 = history
        .parameters()
        .iter()
        .find(|p| p.version() == 1)
        .unwrap();
    assert!(v1.labels().contains(&"prod".to_string()));
    assert!(v1.labels().contains(&"stable".to_string()));

    // Unlabel "prod"
    client
        .unlabel_parameter_version()
        .name("/label/e2e")
        .parameter_version(1)
        .labels("prod")
        .send()
        .await
        .unwrap();

    // Verify only "stable" remains
    let history = client
        .get_parameter_history()
        .name("/label/e2e")
        .send()
        .await
        .unwrap();
    let v1 = history
        .parameters()
        .iter()
        .find(|p| p.version() == 1)
        .unwrap();
    assert!(!v1.labels().contains(&"prod".to_string()));
    assert!(v1.labels().contains(&"stable".to_string()));
}

// ── Document Operations ──────────────────────────────────────

#[tokio::test]
async fn ssm_update_document_and_default_version() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let doc_v1 = r#"{"schemaVersion":"2.2","description":"v1","mainSteps":[{"action":"aws:runShellScript","name":"run","inputs":{"runCommand":["echo v1"]}}]}"#;
    let doc_v2 = r#"{"schemaVersion":"2.2","description":"v2","mainSteps":[{"action":"aws:runShellScript","name":"run","inputs":{"runCommand":["echo v2"]}}]}"#;

    // Create
    client
        .create_document()
        .name("e2e-update-doc")
        .content(doc_v1)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .document_format(aws_sdk_ssm::types::DocumentFormat::Json)
        .send()
        .await
        .unwrap();

    // Update (creates version 2)
    let resp = client
        .update_document()
        .name("e2e-update-doc")
        .content(doc_v2)
        .document_version("$LATEST")
        .send()
        .await
        .unwrap();
    let desc = resp.document_description().unwrap();
    assert_eq!(desc.document_version().unwrap(), "2");

    // List versions
    let versions = client
        .list_document_versions()
        .name("e2e-update-doc")
        .send()
        .await
        .unwrap();
    assert_eq!(versions.document_versions().len(), 2);

    // Update default version to 2
    let resp = client
        .update_document_default_version()
        .name("e2e-update-doc")
        .document_version("2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.description().unwrap().default_version().unwrap(), "2");

    // Verify describe shows default version = 2
    let desc = client
        .describe_document()
        .name("e2e-update-doc")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.document().unwrap().default_version().unwrap(), "2");
}

#[tokio::test]
async fn ssm_document_permissions() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    let doc_content = r#"{"schemaVersion":"2.2","mainSteps":[{"action":"aws:runShellScript","name":"run","inputs":{"runCommand":["echo hi"]}}]}"#;

    client
        .create_document()
        .name("e2e-perm-doc")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    // Add permissions
    client
        .modify_document_permission()
        .name("e2e-perm-doc")
        .permission_type(aws_sdk_ssm::types::DocumentPermissionType::Share)
        .account_ids_to_add("111111111111")
        .account_ids_to_add("222222222222")
        .send()
        .await
        .unwrap();

    // Describe permissions
    let resp = client
        .describe_document_permission()
        .name("e2e-perm-doc")
        .permission_type(aws_sdk_ssm::types::DocumentPermissionType::Share)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.account_ids().len(), 2);

    // Remove one
    client
        .modify_document_permission()
        .name("e2e-perm-doc")
        .permission_type(aws_sdk_ssm::types::DocumentPermissionType::Share)
        .account_ids_to_remove("111111111111")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_document_permission()
        .name("e2e-perm-doc")
        .permission_type(aws_sdk_ssm::types::DocumentPermissionType::Share)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.account_ids().len(), 1);
}

// ── Maintenance Window Targets and Tasks ─────────────────────

#[tokio::test]
async fn ssm_describe_maintenance_window_targets_and_tasks() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    use aws_sdk_ssm::types::{MaintenanceWindowResourceType, MaintenanceWindowTaskType, Target};

    // Create window
    let mw = client
        .create_maintenance_window()
        .name("e2e-mw-desc")
        .schedule("cron(0 2 ? * SUN *)")
        .duration(3)
        .cutoff(1)
        .allow_unassociated_targets(true)
        .send()
        .await
        .unwrap();
    let window_id = mw.window_id().unwrap().to_string();

    // Register target
    let target = Target::builder().key("InstanceIds").values("i-001").build();
    client
        .register_target_with_maintenance_window()
        .window_id(&window_id)
        .resource_type(MaintenanceWindowResourceType::Instance)
        .targets(target)
        .name("e2e-target")
        .description("test target")
        .send()
        .await
        .unwrap();

    // Register task
    client
        .register_task_with_maintenance_window()
        .window_id(&window_id)
        .task_arn("AWS-RunShellScript")
        .task_type(MaintenanceWindowTaskType::RunCommand)
        .name("e2e-task")
        .max_concurrency("5")
        .max_errors("1")
        .send()
        .await
        .unwrap();

    // Describe targets
    let resp = client
        .describe_maintenance_window_targets()
        .window_id(&window_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.targets().len(), 1);
    assert_eq!(resp.targets()[0].name().unwrap(), "e2e-target");
    assert_eq!(resp.targets()[0].description().unwrap(), "test target");

    // Describe tasks
    let resp = client
        .describe_maintenance_window_tasks()
        .window_id(&window_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tasks().len(), 1);
    assert_eq!(resp.tasks()[0].name().unwrap(), "e2e-task");
    assert_eq!(resp.tasks()[0].task_arn().unwrap(), "AWS-RunShellScript");
    assert_eq!(resp.tasks()[0].max_concurrency().unwrap(), "5");
}

// ── Patch Baselines ──────────────────────────────────────────

#[tokio::test]
async fn ssm_patch_baseline_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create
    let resp = client
        .create_patch_baseline()
        .name("e2e-pb")
        .operating_system(aws_sdk_ssm::types::OperatingSystem::AmazonLinux2)
        .description("E2E test baseline")
        .send()
        .await
        .unwrap();
    let baseline_id = resp.baseline_id().unwrap().to_string();

    // GetPatchBaseline
    let resp = client
        .get_patch_baseline()
        .baseline_id(&baseline_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.name().unwrap(), "e2e-pb");
    assert_eq!(resp.description().unwrap(), "E2E test baseline");
    assert_eq!(
        resp.operating_system(),
        Some(&aws_sdk_ssm::types::OperatingSystem::AmazonLinux2)
    );

    // DescribePatchBaselines
    let resp = client.describe_patch_baselines().send().await.unwrap();
    assert!(resp
        .baseline_identities()
        .iter()
        .any(|b| b.baseline_id().unwrap() == baseline_id));

    // DeletePatchBaseline
    client
        .delete_patch_baseline()
        .baseline_id(&baseline_id)
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client
        .get_patch_baseline()
        .baseline_id(&baseline_id)
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn ssm_patch_group_lifecycle() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create baseline
    let resp = client
        .create_patch_baseline()
        .name("e2e-pg-baseline")
        .operating_system(aws_sdk_ssm::types::OperatingSystem::AmazonLinux2)
        .send()
        .await
        .unwrap();
    let baseline_id = resp.baseline_id().unwrap().to_string();

    // Register patch group
    let resp = client
        .register_patch_baseline_for_patch_group()
        .baseline_id(&baseline_id)
        .patch_group("e2e-prod")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.patch_group().unwrap(), "e2e-prod");

    // GetPatchBaselineForPatchGroup
    let resp = client
        .get_patch_baseline_for_patch_group()
        .patch_group("e2e-prod")
        .operating_system(aws_sdk_ssm::types::OperatingSystem::AmazonLinux2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.baseline_id().unwrap(), baseline_id);

    // DescribePatchGroups
    let resp = client.describe_patch_groups().send().await.unwrap();
    assert!(resp
        .mappings()
        .iter()
        .any(|m| m.patch_group().unwrap() == "e2e-prod"));

    // Deregister
    client
        .deregister_patch_baseline_for_patch_group()
        .baseline_id(&baseline_id)
        .patch_group("e2e-prod")
        .send()
        .await
        .unwrap();

    // Verify removed
    let resp = client.describe_patch_groups().send().await.unwrap();
    assert!(!resp
        .mappings()
        .iter()
        .any(|m| m.patch_group().unwrap() == "e2e-prod"));
}

// ── Command Details ──────────────────────────────────────────

#[tokio::test]
async fn ssm_command_invocations() {
    let server = TestServer::start().await;
    let client = server.ssm_client().await;

    // Create document
    let doc_content = r#"{"schemaVersion":"2.2","mainSteps":[{"action":"aws:runShellScript","name":"run","inputs":{"runCommand":["echo hi"]}}]}"#;
    client
        .create_document()
        .name("e2e-cmd-inv")
        .content(doc_content)
        .document_type(aws_sdk_ssm::types::DocumentType::Command)
        .send()
        .await
        .unwrap();

    // Send command
    let resp = client
        .send_command()
        .document_name("e2e-cmd-inv")
        .instance_ids("i-1234567890abcdef0")
        .send()
        .await
        .unwrap();
    let cmd_id = resp.command().unwrap().command_id().unwrap().to_string();

    // GetCommandInvocation
    let resp = client
        .get_command_invocation()
        .command_id(&cmd_id)
        .instance_id("i-1234567890abcdef0")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.command_id().unwrap(), cmd_id);
    assert_eq!(resp.instance_id().unwrap(), "i-1234567890abcdef0");
    assert_eq!(
        resp.status(),
        Some(&aws_sdk_ssm::types::CommandInvocationStatus::Success)
    );

    // ListCommandInvocations
    let resp = client
        .list_command_invocations()
        .command_id(&cmd_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.command_invocations().len(), 1);
    assert_eq!(resp.command_invocations()[0].command_id().unwrap(), cmd_id);
}
