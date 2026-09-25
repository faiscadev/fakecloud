//! A Lambda's container must be able to reach fakecloud, not real AWS.
//!
//! Real Lambda injects region and execution-role credentials, and function code
//! relies on them. fakecloud injected none, and nothing pointed the SDK at the
//! emulator, so handler code that called AWS silently targeted the internet —
//! CDK's `BucketDeployment` reported success having copied no files.

mod helpers;

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, Runtime};
use helpers::TestServer;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn build_python_handler_zip(body: &str) -> Vec<u8> {
    use std::io::Write;
    let mut buf = Vec::new();
    {
        let mut zip = zip::ZipWriter::new(std::io::Cursor::new(&mut buf));
        let opts: zip::write::FileOptions<'_, ()> =
            zip::write::FileOptions::default().compression_method(zip::CompressionMethod::Stored);
        zip.start_file("index.py", opts).unwrap();
        zip.write_all(body.as_bytes()).unwrap();
        zip.finish().unwrap();
    }
    buf
}

#[tokio::test]
async fn lambda_container_receives_the_aws_environment() {
    if !docker_available() {
        eprintln!("docker required for Lambda execution; skipping");
        return;
    }
    let server = TestServer::start().await;
    let lambda = server.lambda_client().await;

    // The handler reports the environment it actually sees inside the
    // container, which is the thing that was missing.
    let zip_bytes = build_python_handler_zip(
        "import os\n\
         def handler(event, context):\n\
         \x20   return {k: os.environ.get(k) for k in\n\
         \x20           ('AWS_ENDPOINT_URL','AWS_REGION','AWS_DEFAULT_REGION',\n\
         \x20            'AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY')}\n",
    );
    lambda
        .create_function()
        .function_name("env-probe-fn")
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/env-probe-role")
        .handler("index.handler")
        .timeout(30)
        .code(FunctionCode::builder().zip_file(zip_bytes.into()).build())
        .send()
        .await
        .expect("create_function");

    let invoked = lambda
        .invoke()
        .function_name("env-probe-fn")
        .payload(Blob::new("{}"))
        .send()
        .await
        .expect("invoke");
    let payload = String::from_utf8(
        invoked
            .payload()
            .map(|b| b.as_ref().to_vec())
            .unwrap_or_default(),
    )
    .unwrap_or_default();
    assert!(
        invoked.function_error().is_none(),
        "handler errored: {payload}"
    );

    let env: serde_json::Value = serde_json::from_str(&payload).expect("handler returned JSON");
    let endpoint = env["AWS_ENDPOINT_URL"]
        .as_str()
        .unwrap_or_default()
        .to_string();

    // Points back at fakecloud on the host, on the port this server bound.
    assert!(
        endpoint.ends_with(&format!(":{}", server.port())),
        "endpoint {endpoint} should target this server's port {}",
        server.port()
    );
    // Never `localhost`: inside the container that is the container itself.
    assert!(
        !endpoint.contains("localhost:") && !endpoint.contains("127.0.0.1"),
        "endpoint {endpoint} must use the container's host alias"
    );
    // Real Lambda supplies these; an SDK client without them fails before it
    // ever reaches an endpoint.
    for key in [
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ] {
        assert!(
            env[key].as_str().is_some_and(|v| !v.is_empty()),
            "{key} missing from the container environment: {payload}"
        );
    }
}
