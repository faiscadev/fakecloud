//! End-to-end tests for the EC2 instance metadata service (IMDS) surface
//! (`/latest/*`).
//!
//! An app that resolves credentials through IMDS runs unmodified against
//! fakecloud by pointing the SDK's IMDS client at it
//! (`AWS_EC2_METADATA_SERVICE_ENDPOINT`). Both IMDSv1 and IMDSv2 (token-first)
//! are supported; the vended credentials are IAM-registered so they verify
//! under `--verify-sigv4`.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_sts::Client as StsClient;
use helpers::TestServer;

fn aws_cli_available() -> bool {
    std::process::Command::new("aws")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

/// IMDSv2 token flow: PUT a token, list the role, fetch its credentials, and
/// use them for a signed call.
#[tokio::test]
async fn imdsv2_token_flow_vends_usable_credentials() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let base = server.endpoint();

    // PUT /latest/api/token
    let token = http
        .put(format!("{base}/latest/api/token"))
        .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(!token.is_empty(), "empty IMDSv2 token");

    // GET the role name.
    let role = http
        .get(format!("{base}/latest/meta-data/iam/security-credentials/"))
        .header("X-aws-ec2-metadata-token", &token)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(role.trim(), "fakecloud");

    // GET the credentials for that role.
    let creds: serde_json::Value = http
        .get(format!(
            "{base}/latest/meta-data/iam/security-credentials/{}",
            role.trim()
        ))
        .header("X-aws-ec2-metadata-token", &token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(creds["Code"].as_str(), Some("Success"));
    assert_eq!(creds["Type"].as_str(), Some("AWS-HMAC"));
    assert!(creds["AccessKeyId"]
        .as_str()
        .is_some_and(|s| s.starts_with("FSIA")));
    assert!(creds["Token"].as_str().is_some_and(|s| !s.is_empty()));

    // The vended credentials work on a signed request.
    let conf = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(base.to_string())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            creds["AccessKeyId"].as_str().unwrap(),
            creds["SecretAccessKey"].as_str().unwrap(),
            Some(creds["Token"].as_str().unwrap().to_string()),
            None,
            "fakecloud-imds",
        ))
        .load()
        .await;
    let ident = StsClient::new(&conf)
        .get_caller_identity()
        .send()
        .await
        .expect("IMDS creds should be usable");
    assert_eq!(
        ident.arn(),
        Some("arn:aws:sts::123456789012:assumed-role/fakecloud/fakecloud-local")
    );
}

/// IMDSv1 (no token) works too, and the credentials verify under
/// `--verify-sigv4` (i.e. they are genuinely registered).
#[tokio::test]
async fn imdsv1_credentials_verify_under_sigv4() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_VERIFY_SIGV4", "true")]).await;
    let base = server.endpoint();

    let creds: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{base}/latest/meta-data/iam/security-credentials/fakecloud"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let conf = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(base.to_string())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            creds["AccessKeyId"].as_str().unwrap(),
            creds["SecretAccessKey"].as_str().unwrap(),
            Some(creds["Token"].as_str().unwrap().to_string()),
            None,
            "fakecloud-imds",
        ))
        .load()
        .await;
    let ident = StsClient::new(&conf)
        .get_caller_identity()
        .send()
        .await
        .expect("IMDS creds should verify under --verify-sigv4");
    assert_eq!(ident.account(), Some("123456789012"));
}

/// The instance identity document reports account, region, AZ, and instance ID.
#[tokio::test]
async fn instance_identity_document_is_served() {
    let server = TestServer::start().await;
    let doc: serde_json::Value = reqwest::Client::new()
        .get(format!(
            "{}/latest/dynamic/instance-identity/document",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(doc["accountId"].as_str(), Some("123456789012"));
    assert_eq!(doc["region"].as_str(), Some("us-east-1"));
    assert_eq!(doc["availabilityZone"].as_str(), Some("us-east-1a"));
    assert!(doc["instanceId"]
        .as_str()
        .is_some_and(|s| s.starts_with("i-")));
}

/// IMDSv2 rejects a token request with a missing or out-of-range TTL (400),
/// matching real IMDS.
#[tokio::test]
async fn imdsv2_token_requires_valid_ttl() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let base = server.endpoint();

    // Missing TTL header -> 400.
    let missing = http
        .put(format!("{base}/latest/api/token"))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status().as_u16(), 400, "missing TTL should be 400");

    // Out-of-range TTL -> 400.
    let bad = http
        .put(format!("{base}/latest/api/token"))
        .header("X-aws-ec2-metadata-token-ttl-seconds", "999999")
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status().as_u16(), 400, "out-of-range TTL should be 400");

    // Valid TTL -> 200 with a token.
    let ok = http
        .put(format!("{base}/latest/api/token"))
        .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status().as_u16(), 200);
    assert!(!ok.text().await.unwrap().is_empty());
}

/// A path-style S3 bucket literally named `latest` still works: signed S3
/// requests to `/latest/<key>` are forwarded to the dispatcher, not shadowed by
/// the IMDS routes.
#[tokio::test]
async fn s3_bucket_named_latest_is_not_shadowed_by_imds() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("latest").send().await.unwrap();
    // Keys that collide with IMDS paths.
    for key in ["api/token", "meta-data/instance-id"] {
        s3.put_object()
            .bucket("latest")
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(
                b"real-object",
            ))
            .send()
            .await
            .unwrap_or_else(|e| panic!("put {key} failed: {e:?}"));

        let got = s3
            .get_object()
            .bucket("latest")
            .key(key)
            .send()
            .await
            .unwrap_or_else(|e| panic!("get {key} failed (shadowed by IMDS?): {e:?}"));
        let body = got.body.collect().await.unwrap().into_bytes();
        assert_eq!(
            &body[..],
            b"real-object",
            "S3 object at {key} was shadowed by IMDS"
        );
    }
}

/// The real AWS SDK default chain resolves via IMDS pointed at fakecloud with
/// no static keys — the "run my app unmodified" story for IMDS-based apps.
#[tokio::test]
async fn default_chain_resolves_via_imds_endpoint() {
    if !aws_cli_available() {
        if std::env::var("CI").is_ok() {
            panic!("aws CLI is required for default_chain_resolves_via_imds_endpoint in CI");
        }
        eprintln!("skipping default_chain_resolves_via_imds_endpoint: aws CLI not available");
        return;
    }
    let server = TestServer::start().await;

    let output = std::process::Command::new("aws")
        .args(["sts", "get-caller-identity"])
        .arg("--endpoint-url")
        .arg(server.endpoint())
        .arg("--region")
        .arg("us-east-1")
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_PROFILE")
        .env_remove("AWS_CONTAINER_CREDENTIALS_FULL_URI")
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            "/nonexistent/fakecloud-aws-credentials",
        )
        .env("AWS_CONFIG_FILE", "/nonexistent/fakecloud-aws-config")
        // Trailing slash: the SDK appends `latest/...` to this base directly.
        .env(
            "AWS_EC2_METADATA_SERVICE_ENDPOINT",
            format!("{}/", server.endpoint()),
        )
        .output()
        .expect("failed to run aws cli");

    assert!(
        output.status.success(),
        "aws sts get-caller-identity via IMDS failed: {}",
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
