//! End-to-end tests for the general-purpose container/instance credential
//! endpoint (`GET /_fakecloud/credentials`).
//!
//! An app deployed on EC2/ECS carries no static keys — the AWS SDK default
//! credential chain fetches temporary credentials from the environment. Point
//! `AWS_CONTAINER_CREDENTIALS_FULL_URI` at this endpoint and the same app runs
//! unmodified against fakecloud. The credentials it vends are minted and
//! registered in IAM state (like AssumeRole), so a request signed with them is
//! accepted even under `--verify-sigv4`.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_sts::Client as StsClient;
use helpers::TestServer;

fn sdk_config_with_temp(
    endpoint: &str,
    ak: &str,
    sk: &str,
    token: &str,
) -> aws_config::ConfigLoader {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(endpoint.to_string())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            ak,
            sk,
            Some(token.to_string()),
            None,
            "fakecloud-credentials-endpoint",
        ))
}

async fn fetch_credentials(endpoint: &str) -> serde_json::Value {
    reqwest::Client::new()
        .get(format!("{endpoint}/_fakecloud/credentials"))
        .send()
        .await
        .expect("credentials request failed")
        .json()
        .await
        .expect("credentials response was not JSON")
}

/// The endpoint returns AWS container-credentials-format JSON with a minted
/// STS temp key, a session token, an expiration, and the configured role ARN.
#[tokio::test]
async fn credentials_endpoint_returns_container_creds_json() {
    let server = TestServer::start().await;
    let creds = fetch_credentials(server.endpoint()).await;

    let ak = creds["AccessKeyId"].as_str().unwrap_or_default();
    assert!(
        ak.starts_with("FSIA"),
        "AccessKeyId should be an STS temp key: {creds}"
    );
    assert!(
        creds["SecretAccessKey"]
            .as_str()
            .is_some_and(|s| !s.is_empty()),
        "missing SecretAccessKey: {creds}"
    );
    assert!(
        creds["Token"].as_str().is_some_and(|s| !s.is_empty()),
        "missing Token: {creds}"
    );
    assert!(
        creds["Expiration"]
            .as_str()
            .is_some_and(|s| s.ends_with('Z')),
        "Expiration should be ISO-8601: {creds}"
    );
    assert_eq!(
        creds["RoleArn"].as_str(),
        Some("arn:aws:iam::123456789012:role/fakecloud"),
        "default RoleArn mismatch: {creds}"
    );
}

/// Credentials fetched from the endpoint are accepted on a subsequent signed
/// request — the whole point of registering them like an AssumeRole session.
/// `GetCallerIdentity` reports the assumed-role principal.
#[tokio::test]
async fn credentials_endpoint_creds_are_usable() {
    let server = TestServer::start().await;
    let creds = fetch_credentials(server.endpoint()).await;

    let conf = sdk_config_with_temp(
        server.endpoint(),
        creds["AccessKeyId"].as_str().unwrap(),
        creds["SecretAccessKey"].as_str().unwrap(),
        creds["Token"].as_str().unwrap(),
    )
    .load()
    .await;
    let ident = StsClient::new(&conf)
        .get_caller_identity()
        .send()
        .await
        .expect("get_caller_identity with endpoint creds failed");

    assert_eq!(ident.account(), Some("123456789012"));
    assert_eq!(
        ident.arn(),
        Some("arn:aws:sts::123456789012:assumed-role/fakecloud/fakecloud-local")
    );
}

/// The minted credentials pass cryptographic SigV4 verification when the server
/// runs with `--verify-sigv4` — proving they are genuinely registered, not just
/// waved through because verification is off.
#[tokio::test]
async fn credentials_endpoint_creds_verify_under_sigv4() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_VERIFY_SIGV4", "true")]).await;
    let creds = fetch_credentials(server.endpoint()).await;

    let conf = sdk_config_with_temp(
        server.endpoint(),
        creds["AccessKeyId"].as_str().unwrap(),
        creds["SecretAccessKey"].as_str().unwrap(),
        creds["Token"].as_str().unwrap(),
    )
    .load()
    .await;
    let ident = StsClient::new(&conf)
        .get_caller_identity()
        .send()
        .await
        .expect("endpoint creds should verify under --verify-sigv4");
    assert_eq!(ident.account(), Some("123456789012"));

    // A bogus secret against the same (registered) key must be rejected, so the
    // success above is real verification, not a blanket accept-any.
    let bad = sdk_config_with_temp(
        server.endpoint(),
        creds["AccessKeyId"].as_str().unwrap(),
        "wrong-secret",
        creds["Token"].as_str().unwrap(),
    )
    .load()
    .await;
    let err = StsClient::new(&bad).get_caller_identity().send().await;
    assert!(
        err.is_err(),
        "wrong secret should fail under --verify-sigv4"
    );
}

/// A configured `--credentials-role-arn` is reflected in the vended creds and
/// the resulting assumed-role principal.
#[tokio::test]
async fn credentials_endpoint_respects_configured_role() {
    let role = "arn:aws:iam::123456789012:role/my-app-role";
    let server = TestServer::start_full(&[], &["--credentials-role-arn", role]).await;
    let creds = fetch_credentials(server.endpoint()).await;
    assert_eq!(creds["RoleArn"].as_str(), Some(role));

    let conf = sdk_config_with_temp(
        server.endpoint(),
        creds["AccessKeyId"].as_str().unwrap(),
        creds["SecretAccessKey"].as_str().unwrap(),
        creds["Token"].as_str().unwrap(),
    )
    .load()
    .await;
    let ident = StsClient::new(&conf)
        .get_caller_identity()
        .send()
        .await
        .unwrap();
    assert_eq!(
        ident.arn(),
        Some("arn:aws:sts::123456789012:assumed-role/my-app-role/fakecloud-local")
    );
}

/// The real AWS SDK default credential chain resolves against the endpoint with
/// no static keys, driven only by `AWS_CONTAINER_CREDENTIALS_FULL_URI` — the
/// "run my app unmodified" story. Exercised through the `aws` CLI so the
/// provider chain (not a hand-built provider) does the resolution.
#[tokio::test]
async fn default_chain_resolves_via_full_uri_env() {
    if !aws_cli_available() {
        if std::env::var("CI").is_ok() {
            panic!("aws CLI is required for default_chain_resolves_via_full_uri_env in CI");
        }
        eprintln!("skipping default_chain_resolves_via_full_uri_env: aws CLI not available");
        return;
    }
    let server = TestServer::start().await;
    let full_uri = format!("{}/_fakecloud/credentials", server.endpoint());

    let output = std::process::Command::new("aws")
        .args(["sts", "get-caller-identity"])
        .arg("--endpoint-url")
        .arg(server.endpoint())
        .arg("--region")
        .arg("us-east-1")
        // No static keys — force the container-credentials provider.
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_PROFILE")
        // Isolate from the developer's `~/.aws/{credentials,config}` — the
        // shared-config providers sit ahead of container credentials in the
        // default chain, so a local profile would otherwise be used instead of
        // the FULL_URI endpoint. A clean container/CI has no such files; point
        // these at nonexistent paths to emulate that here.
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            "/nonexistent/fakecloud-aws-credentials",
        )
        .env("AWS_CONFIG_FILE", "/nonexistent/fakecloud-aws-config")
        .env("AWS_CONTAINER_CREDENTIALS_FULL_URI", &full_uri)
        .output()
        .expect("failed to run aws cli");

    assert!(
        output.status.success(),
        "aws sts get-caller-identity failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("caller identity not JSON");
    assert_eq!(json["Account"].as_str(), Some("123456789012"));
    assert_eq!(
        json["Arn"].as_str(),
        Some("arn:aws:sts::123456789012:assumed-role/fakecloud/fakecloud-local")
    );
}

fn aws_cli_available() -> bool {
    std::process::Command::new("aws")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}
