mod helpers;

use aws_sdk_lambda::primitives::Blob;
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ---------------------------------------------------------------------------
// Function lifecycle
// ---------------------------------------------------------------------------

#[test_action("lambda", "CreateFunction", checksum = "46e2786b")]
#[test_action("lambda", "GetFunction", checksum = "2d15e19e")]
#[test_action("lambda", "DeleteFunction", checksum = "70eb2012")]
#[tokio::test]
async fn lambda_create_get_delete_function() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let resp = client
        .create_function()
        .function_name("conf-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake-code"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.function_name().unwrap(), "conf-func");
    assert!(resp.function_arn().unwrap().contains("function:conf-func"));

    let resp = client
        .get_function()
        .function_name("conf-func")
        .send()
        .await
        .unwrap();
    let config = resp.configuration().unwrap();
    assert_eq!(config.function_name().unwrap(), "conf-func");
    assert_eq!(config.runtime().unwrap().as_str(), "python3.12");
    assert_eq!(config.handler().unwrap(), "index.handler");

    client
        .delete_function()
        .function_name("conf-func")
        .send()
        .await
        .unwrap();

    let result = client
        .get_function()
        .function_name("conf-func")
        .send()
        .await;
    assert!(result.is_err());
}

#[test_action("lambda", "ListFunctions", checksum = "fa22d1bf")]
#[tokio::test]
async fn lambda_list_functions() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    for name in &["list-a", "list-b", "list-c"] {
        client
            .create_function()
            .function_name(*name)
            .runtime(aws_sdk_lambda::types::Runtime::Nodejs20x)
            .role("arn:aws:iam::123456789012:role/test-role")
            .handler("index.handler")
            .code(
                aws_sdk_lambda::types::FunctionCode::builder()
                    .zip_file(Blob::new(b"fake"))
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }

    let resp = client.list_functions().send().await.unwrap();
    assert_eq!(resp.functions().len(), 3);
}

// ---------------------------------------------------------------------------
// Invoke
// ---------------------------------------------------------------------------

#[test_action("lambda", "Invoke", checksum = "73c32773")]
#[tokio::test]
async fn lambda_invoke() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("invoke-me")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .invoke()
        .function_name("invoke-me")
        .payload(Blob::new(br#"{"key": "value"}"#))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status_code(), 200);
}

// ---------------------------------------------------------------------------
// PublishVersion
// ---------------------------------------------------------------------------

#[test_action("lambda", "PublishVersion", checksum = "209921df")]
#[tokio::test]
async fn lambda_publish_version() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("version-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .publish_version()
        .function_name("version-func")
        .send()
        .await
        .unwrap();

    assert!(resp.version().is_some());
    assert!(resp.function_arn().unwrap().contains("version-func"));
}

// ---------------------------------------------------------------------------
// Event source mappings
// ---------------------------------------------------------------------------

#[test_action("lambda", "CreateEventSourceMapping", checksum = "b9f5b731")]
#[test_action("lambda", "GetEventSourceMapping", checksum = "abb053d9")]
#[test_action("lambda", "DeleteEventSourceMapping", checksum = "96206508")]
#[tokio::test]
async fn lambda_create_get_delete_event_source_mapping() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("esm-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let create_resp = client
        .create_event_source_mapping()
        .function_name("esm-func")
        .event_source_arn("arn:aws:sqs:us-east-1:123456789012:my-queue")
        .send()
        .await
        .unwrap();

    let uuid = create_resp.uuid().unwrap().to_string();
    assert!(!uuid.is_empty());

    let get_resp = client
        .get_event_source_mapping()
        .uuid(&uuid)
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.function_arn().unwrap(), "esm-func");

    client
        .delete_event_source_mapping()
        .uuid(&uuid)
        .send()
        .await
        .unwrap();
}

#[test_action("lambda", "ListEventSourceMappings", checksum = "6df074f2")]
#[tokio::test]
async fn lambda_list_event_source_mappings() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("esm-list-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake"))
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .create_event_source_mapping()
        .function_name("esm-list-func")
        .event_source_arn("arn:aws:sqs:us-east-1:123456789012:queue-1")
        .send()
        .await
        .unwrap();

    let resp = client.list_event_source_mappings().send().await.unwrap();
    assert!(!resp.event_source_mappings().is_empty());
}
