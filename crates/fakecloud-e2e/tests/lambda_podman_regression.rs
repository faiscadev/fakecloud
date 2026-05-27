//! Regression test for issue #1539 Bug 1: Lambda Invoke on podman macOS
//! used to fail at container start because fakecloud passed
//! `--add-host host.docker.internal:host-gateway`, which Docker Desktop
//! understands but podman does not.
//!
//! The main test suite picks `docker` over `podman` whenever both are
//! installed (see `fakecloud_testkit::detect_container_cli`), so the
//! podman code path was never exercised in CI and the bug shipped. This
//! file pins the runtime to podman explicitly. It is `#[ignore]`-gated
//! so it doesn't run by default — opt in with `cargo test
//! --test lambda_podman_regression -- --ignored` (or wire a dedicated
//! CI job that does).
//!
//! Per the project's no-silent-skip rule: when this test does run, it
//! hard-fails if podman is unavailable. It must not pretend to pass on a
//! machine without the toolchain.

mod helpers;

use std::io::Write;
use std::process::{Command, Stdio};

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{FunctionCode, Runtime};
use helpers::TestServer;

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

fn require_podman() {
    let ok = Command::new("podman")
        .arg("info")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    assert!(
        ok,
        "podman is required for this regression test but `podman info` failed. \
         Install podman + start a machine (`podman machine start`) before running with --ignored."
    );
}

#[tokio::test]
#[ignore = "requires podman + machine running; opt in with --ignored. Pins the runtime to podman to exercise the macOS host-alias path that broke in issue #1539."]
async fn lambda_invoke_works_with_podman_backend() {
    require_podman();

    let server = TestServer::start_with_env(&[("FAKECLOUD_CONTAINER_CLI", "podman")]).await;
    let client = server.lambda_client().await;

    let handler = r#"
def handler(event, context):
    return {"ok": True, "echo": event}
"#;
    let zip = make_zip(&[("index.py", handler.as_bytes())]);

    client
        .create_function()
        .function_name("podman-regression-1539")
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(zip))
                .build(),
        )
        .send()
        .await
        .expect("create_function should succeed under podman backend");

    let resp = client
        .invoke()
        .function_name("podman-regression-1539")
        .payload(Blob::new(br#"{"hi":"podman"}"#.to_vec()))
        .send()
        .await
        .expect(
            "Lambda Invoke under podman backend should succeed. \
             Pre-fix: container start failed because `--add-host host.docker.internal:host-gateway` \
             is rejected by podman with 'host containers internal IP address is empty'.",
        );

    assert_eq!(
        resp.status_code(),
        200,
        "Invoke returned non-200 — backend container probably failed to start. \
         Check the fakecloud test log for `host-gateway` or `Lambda invocation failed` lines."
    );

    let body = resp
        .payload()
        .map(|p| String::from_utf8(p.as_ref().to_vec()).unwrap())
        .unwrap_or_default();
    assert!(
        body.contains("\"ok\": true") || body.contains("\"ok\":true"),
        "unexpected invoke payload: {body}"
    );
    assert!(
        body.contains("podman"),
        "invoke payload should echo back the input event: {body}"
    );
}
