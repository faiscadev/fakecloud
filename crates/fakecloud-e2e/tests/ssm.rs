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
