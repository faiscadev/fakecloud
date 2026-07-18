//! End-to-end tests for `--imds-link-local`.
//!
//! Binding the AWS link-local addresses (169.254.169.254 / 169.254.170.2 on port
//! 80) needs root plus the addresses already assigned to the loopback interface.
//! fakecloud never creates that alias itself, so enabling the flag mutates no
//! host networking: on a runner without the alias the bind simply fails and the
//! graceful-fallback path runs (true whether or not the runner is root, since
//! fakecloud only binds pre-provisioned addresses). This test therefore verifies
//! the safety property unconditionally: enabling the flag never breaks the main
//! server even when the link-local bind fails. The per-platform alias hint and
//! the bind-failure fallback are unit-tested in `link_local.rs`.

mod helpers;

use helpers::TestServer;

/// The server starts with `--imds-link-local` and the main listener keeps
/// working even when the link-local bind fails (the address is not aliased).
#[tokio::test]
async fn link_local_flag_does_not_break_main_server() {
    let server = TestServer::start_full(&[], &["--imds-link-local"]).await;
    let http = reqwest::Client::new();

    // Health on the main listener.
    let health = http
        .get(format!("{}/_fakecloud/health", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert!(health.status().is_success(), "main server not healthy");

    // The credential endpoint still works on the main listener.
    let creds: serde_json::Value = http
        .get(format!("{}/_fakecloud/credentials", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(creds["AccessKeyId"]
        .as_str()
        .is_some_and(|s| s.starts_with("FSIA")));
}
