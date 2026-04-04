mod helpers;

use aws_sdk_ssm::types::ParameterType;
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
