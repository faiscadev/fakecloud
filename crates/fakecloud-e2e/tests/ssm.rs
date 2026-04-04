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

    // Get WITHOUT WithDecryption (default) - should be masked
    let resp = client
        .get_parameter()
        .name("/secret/password")
        .send()
        .await
        .unwrap();
    let param = resp.parameter().unwrap();
    assert_eq!(param.value().unwrap(), "****");

    // Get WITH WithDecryption=true - should return actual value
    let resp = client
        .get_parameter()
        .name("/secret/password")
        .with_decryption(true)
        .send()
        .await
        .unwrap();
    let param = resp.parameter().unwrap();
    assert_eq!(
        param.value().unwrap(),
        "kms:alias/aws/ssm:super-secret-123"
    );
}
