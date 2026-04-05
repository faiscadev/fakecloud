mod helpers;

use aws_sdk_lambda::primitives::Blob;
use helpers::TestServer;

#[tokio::test]
async fn lambda_create_get_delete_function() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    // Create
    let resp = client
        .create_function()
        .function_name("my-func")
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

    assert_eq!(resp.function_name().unwrap(), "my-func");
    assert!(resp.function_arn().unwrap().contains("function:my-func"));

    // Get
    let resp = client
        .get_function()
        .function_name("my-func")
        .send()
        .await
        .unwrap();
    let config = resp.configuration().unwrap();
    assert_eq!(config.function_name().unwrap(), "my-func");
    assert_eq!(config.runtime().unwrap().as_str(), "python3.12");
    assert_eq!(config.handler().unwrap(), "index.handler");

    // Delete
    client
        .delete_function()
        .function_name("my-func")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client.get_function().function_name("my-func").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn lambda_list_functions() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    for name in &["func-a", "func-b", "func-c"] {
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

#[tokio::test]
async fn lambda_create_function_conflict() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    client
        .create_function()
        .function_name("dup-func")
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

    // Creating again should fail
    let result = client
        .create_function()
        .function_name("dup-func")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(b"fake"))
                .build(),
        )
        .send()
        .await;
    assert!(result.is_err());
}
