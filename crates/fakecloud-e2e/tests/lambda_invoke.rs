mod helpers;

use std::io::Write;

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{Environment, FunctionCode, Runtime};
use helpers::TestServer;

/// Create a ZIP file in memory containing a single file.
fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    for (name, content) in entries {
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
        writer.start_file(*name, options).unwrap();
        writer.write_all(content).unwrap();
    }
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

async fn create_and_invoke(
    client: &aws_sdk_lambda::Client,
    function_name: &str,
    runtime: Runtime,
    handler: &str,
    zip_bytes: Vec<u8>,
    payload: Option<&str>,
) -> String {
    // Create function
    client
        .create_function()
        .function_name(function_name)
        .runtime(runtime)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler(handler)
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(zip_bytes))
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Invoke
    let resp = client
        .invoke()
        .function_name(function_name)
        .payload(Blob::new(payload.unwrap_or("{}").as_bytes().to_vec()))
        .send()
        .await
        .unwrap();

    let payload_bytes = resp
        .payload()
        .map(|p| p.as_ref().to_vec())
        .unwrap_or_default();
    String::from_utf8(payload_bytes).unwrap()
}

// ---- Python runtime tests ----

const PYTHON_HANDLER: &str = r#"
def handler(event, context):
    return {"statusCode": 200, "body": "hello from python"}
"#;

#[tokio::test]
async fn test_invoke_python3_13() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.py", PYTHON_HANDLER.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "py313-func",
        Runtime::Python313,
        "index.handler",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
    assert_eq!(body["body"], "hello from python");
}

#[tokio::test]
async fn test_invoke_python3_12() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.py", PYTHON_HANDLER.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "py312-func",
        Runtime::Python312,
        "index.handler",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
}

#[tokio::test]
async fn test_invoke_python3_11() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.py", PYTHON_HANDLER.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "py311-func",
        Runtime::Python311,
        "index.handler",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
}

// ---- Node.js runtime tests ----

const NODEJS_HANDLER: &str = r#"
exports.handler = async (event) => {
    return { statusCode: 200, body: "hello from nodejs" };
};
"#;

#[tokio::test]
async fn test_invoke_nodejs22() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.js", NODEJS_HANDLER.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "node22-func",
        Runtime::Nodejs22x,
        "index.handler",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
    assert_eq!(body["body"], "hello from nodejs");
}

#[tokio::test]
async fn test_invoke_nodejs20() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.js", NODEJS_HANDLER.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "node20-func",
        Runtime::Nodejs20x,
        "index.handler",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
}

#[tokio::test]
async fn test_invoke_nodejs18() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.js", NODEJS_HANDLER.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "node18-func",
        Runtime::Nodejs18x,
        "index.handler",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
}

// ---- Ruby runtime tests ----

const RUBY_HANDLER: &str = r#"
def handler(event:, context:)
  { statusCode: 200, body: "hello from ruby" }
end
"#;

#[tokio::test]
async fn test_invoke_ruby3_4() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.rb", RUBY_HANDLER.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "ruby34-func",
        Runtime::Ruby34,
        "index.handler",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
    assert_eq!(body["body"], "hello from ruby");
}

#[tokio::test]
async fn test_invoke_ruby3_3() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.rb", RUBY_HANDLER.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "ruby33-func",
        Runtime::Ruby33,
        "index.handler",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
}

// ---- Custom runtime (provided) tests ----

const PROVIDED_BOOTSTRAP: &str = r#"#!/bin/sh
set -euo pipefail

while true; do
  # Get next invocation
  HEADERS=$(mktemp)
  EVENT_DATA=$(curl -sS -LD "$HEADERS" "http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/invocation/next")

  # Extract request ID from headers
  REQUEST_ID=$(grep -Fi Lambda-Runtime-Aws-Request-Id "$HEADERS" | tr -d '[:space:]' | cut -d: -f2)

  # Send response
  RESPONSE='{"statusCode":200,"body":"hello from custom runtime"}'
  curl -sS -X POST "http://${AWS_LAMBDA_RUNTIME_API}/2018-06-01/runtime/invocation/$REQUEST_ID/response" -d "$RESPONSE"
done
"#;

#[tokio::test]
async fn test_invoke_provided_al2023() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("bootstrap", PROVIDED_BOOTSTRAP.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "provided-al2023-func",
        Runtime::Providedal2023,
        "bootstrap",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
    assert_eq!(body["body"], "hello from custom runtime");
}

#[tokio::test]
async fn test_invoke_provided_al2() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("bootstrap", PROVIDED_BOOTSTRAP.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "provided-al2-func",
        Runtime::Providedal2,
        "bootstrap",
        zip,
        None,
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["statusCode"], 200);
}

// ---- Java runtime tests ----
// Java requires precompiled JARs. We test that the runtime mapping works
// but use a minimal approach: create the function and verify Invoke attempts execution.

#[tokio::test]
async fn test_invoke_java21() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    // Create a minimal ZIP (not a valid JAR, so invocation will fail at the runtime level)
    // This tests that container startup and image selection work for java21
    let zip = make_zip(&[("dummy.class", b"not-a-real-class")]);

    client
        .create_function()
        .function_name("java21-func")
        .runtime(Runtime::Java21)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("dummy")
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    // Invoke will start a container but the handler will fail (invalid class).
    // RIE returns 200 with the error in the payload body.
    let resp = client
        .invoke()
        .function_name("java21-func")
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status_code(), 200);
}

#[tokio::test]
async fn test_invoke_java17() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("dummy.class", b"not-a-real-class")]);

    client
        .create_function()
        .function_name("java17-func")
        .runtime(Runtime::Java17)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("dummy")
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    let resp = client
        .invoke()
        .function_name("java17-func")
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status_code(), 200);
}

// ---- .NET runtime test ----

#[tokio::test]
async fn test_invoke_dotnet8() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("dummy.dll", b"not-a-real-dll")]);

    client
        .create_function()
        .function_name("dotnet8-func")
        .runtime(Runtime::Dotnet8)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("dummy")
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    let resp = client
        .invoke()
        .function_name("dotnet8-func")
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status_code(), 200);
}

// ---- Behavior tests ----

#[tokio::test]
async fn test_invoke_warm_start() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;
    let zip = make_zip(&[("index.py", PYTHON_HANDLER.as_bytes())]);

    client
        .create_function()
        .function_name("warm-func")
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    // First invoke (cold start)
    let resp1 = client
        .invoke()
        .function_name("warm-func")
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap();

    // Second invoke (warm start — should reuse container)
    let resp2 = client
        .invoke()
        .function_name("warm-func")
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap();

    let body1: serde_json::Value =
        serde_json::from_slice(resp1.payload().unwrap().as_ref()).unwrap();
    let body2: serde_json::Value =
        serde_json::from_slice(resp2.payload().unwrap().as_ref()).unwrap();
    assert_eq!(body1["statusCode"], 200);
    assert_eq!(body2["statusCode"], 200);
}

#[tokio::test]
async fn test_invoke_with_payload() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let handler = r#"
def handler(event, context):
    name = event.get("name", "world")
    return {"greeting": f"hello {name}"}
"#;
    let zip = make_zip(&[("index.py", handler.as_bytes())]);

    let result = create_and_invoke(
        &client,
        "payload-func",
        Runtime::Python312,
        "index.handler",
        zip,
        Some(r#"{"name": "fakecloud"}"#),
    )
    .await;
    let body: serde_json::Value = serde_json::from_str(&result).unwrap();
    assert_eq!(body["greeting"], "hello fakecloud");
}

#[tokio::test]
async fn test_invoke_with_environment() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    let handler = r#"
import os
def handler(event, context):
    return {"env_value": os.environ.get("MY_VAR", "not set")}
"#;
    let zip = make_zip(&[("index.py", handler.as_bytes())]);

    client
        .create_function()
        .function_name("env-func")
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .environment(
            Environment::builder()
                .variables("MY_VAR", "hello-from-env")
                .build(),
        )
        .code(FunctionCode::builder().zip_file(Blob::new(zip)).build())
        .send()
        .await
        .unwrap();

    let resp = client
        .invoke()
        .function_name("env-func")
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = serde_json::from_slice(resp.payload().unwrap().as_ref()).unwrap();
    assert_eq!(body["env_value"], "hello-from-env");
}

#[tokio::test]
async fn test_invoke_no_code() {
    let server = TestServer::start().await;
    let client = server.lambda_client().await;

    // Create function without ZipFile (empty Code)
    client
        .create_function()
        .function_name("no-code-func")
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(FunctionCode::builder().build())
        .send()
        .await
        .unwrap();

    // Invoke should fail — function has no deployment package
    let result = client
        .invoke()
        .function_name("no-code-func")
        .payload(Blob::new(b"{}".to_vec()))
        .send()
        .await;
    assert!(result.is_err());
}
