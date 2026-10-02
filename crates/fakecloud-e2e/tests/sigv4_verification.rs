//! End-to-end tests for opt-in SigV4 cryptographic verification.
//!
//! Each test spawns a fakecloud process with `FAKECLOUD_VERIFY_SIGV4=true`,
//! drives real signed requests through an `aws-sdk-*` client (or a hand-
//! crafted `reqwest` request for tamper tests), and asserts that the
//! verifier accepts or rejects as expected.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_sts::Client as StsClient;
use helpers::TestServer;

async fn start_verified() -> TestServer {
    TestServer::start_with_env(&[("FAKECLOUD_VERIFY_SIGV4", "true")]).await
}

async fn sdk_config_with(
    server: &TestServer,
    akid: &str,
    secret: &str,
    token: Option<String>,
) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            akid,
            secret,
            token,
            None,
            "fakecloud-sigv4",
        ))
        .load()
        .await
}

#[tokio::test]
async fn verifies_valid_signed_request_from_iam_user() {
    let server = start_verified().await;

    // Bootstrap via root-bypass creds, which always pass verification.
    let boot_config = sdk_config_with(&server, "test", "test", None).await;
    let iam_boot = aws_sdk_iam::Client::new(&boot_config);
    iam_boot
        .create_user()
        .user_name("alice")
        .send()
        .await
        .unwrap();
    let ak = iam_boot
        .create_access_key()
        .user_name("alice")
        .send()
        .await
        .unwrap();
    let key = ak.access_key().unwrap();
    let akid = key.access_key_id();
    let secret = key.secret_access_key();

    // Sign a follow-up request with the real access key. Verification must
    // succeed because the secret is looked up from IAM state.
    let signed_config = sdk_config_with(&server, akid, secret, None).await;
    let iam_signed = aws_sdk_iam::Client::new(&signed_config);
    iam_signed
        .get_user()
        .user_name("alice")
        .send()
        .await
        .expect("verifier should accept a correctly signed request");
}

#[tokio::test]
async fn rejects_unknown_access_key_with_invalid_client_token_id() {
    let server = start_verified().await;

    // An AKID that was never created: verification must fail before the
    // handler runs. Uses an `AKIA`-prefixed id to skip the root bypass.
    let config = sdk_config_with(
        &server,
        "AKIANEVERCREATED1234",
        "fakefakefakefakefakefakefakefakefake1234",
        None,
    )
    .await;
    let sts = StsClient::new(&config);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("InvalidClientTokenId"),
        "expected InvalidClientTokenId, got {msg}"
    );
}

#[tokio::test]
async fn rejects_tampered_signature_with_signature_does_not_match() {
    let server = start_verified().await;

    // Bootstrap a user via root-bypass creds, then sign a request with
    // the real access key ID but the wrong secret. The verifier must
    // reject it.
    let boot = sdk_config_with(&server, "test", "test", None).await;
    let iam_boot = aws_sdk_iam::Client::new(&boot);
    iam_boot
        .create_user()
        .user_name("bob")
        .send()
        .await
        .unwrap();
    let ak = iam_boot
        .create_access_key()
        .user_name("bob")
        .send()
        .await
        .unwrap();
    let akid = ak.access_key().unwrap().access_key_id().to_string();

    let wrong = sdk_config_with(
        &server,
        &akid,
        "wrongwrongwrongwrongwrongwrongwrongwrong",
        None,
    )
    .await;
    let iam_wrong = aws_sdk_iam::Client::new(&wrong);
    let err = iam_wrong
        .get_user()
        .user_name("bob")
        .send()
        .await
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("SignatureDoesNotMatch"),
        "expected SignatureDoesNotMatch, got {msg}"
    );
}

#[tokio::test]
async fn root_bypass_accepts_unsigned_requests_when_verify_is_on() {
    let server = start_verified().await;
    // Default server creds start with `test` → root bypass → accepted even
    // under verify_sigv4.
    let config = sdk_config_with(&server, "test", "test", None).await;
    let sts = StsClient::new(&config);
    let identity = sts
        .get_caller_identity()
        .send()
        .await
        .expect("root-bypass identity should always succeed under verify_sigv4");
    assert!(identity.arn().unwrap().contains(":root"));
}

#[tokio::test]
async fn off_by_default_passes_any_signature() {
    // No FAKECLOUD_VERIFY_SIGV4 env: the default behavior accepts garbage
    // signatures. Regression guard for the off-by-default contract.
    let server = TestServer::start().await;
    let config = sdk_config_with(
        &server,
        "AKIAFAKEFAKEFAKEFAKE",
        "garbagegarbagegarbagegarbagegarbage1234",
        None,
    )
    .await;
    let sts = StsClient::new(&config);
    // Without verification, the request reaches the handler and the
    // default identity is returned.
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert_eq!(identity.account().unwrap(), "123456789012");
}

#[tokio::test]
async fn sts_assume_role_temp_credentials_verify_successfully() {
    // Regression guard for batch 2's STS temp credential persistence: when
    // a client calls AssumeRole and then signs a follow-up request with
    // the returned temporary creds, verification must succeed because the
    // credential was persisted in sts_temp_credentials.
    let server = start_verified().await;

    let boot = sdk_config_with(&server, "test", "test", None).await;
    // AssumeRole requires the role to exist with a trust policy admitting the
    // caller; assuming a non-existent role is denied (matches AWS).
    let iam_boot = aws_sdk_iam::Client::new(&boot);
    iam_boot
        .create_role()
        .role_name("temp-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let sts_boot = StsClient::new(&boot);
    let resp = sts_boot
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/temp-role")
        .role_session_name("e2e-sigv4")
        .send()
        .await
        .unwrap();
    let creds = resp.credentials().unwrap();

    let temp_cfg = sdk_config_with(
        &server,
        creds.access_key_id(),
        creds.secret_access_key(),
        Some(creds.session_token().to_string()),
    )
    .await;
    let sts_temp = StsClient::new(&temp_cfg);
    let identity = sts_temp.get_caller_identity().send().await.unwrap();
    assert!(identity
        .arn()
        .unwrap()
        .contains("assumed-role/temp-role/e2e-sigv4"));
}

/// Bootstrap an IAM user with root-bypass creds and return an SdkConfig
/// signed with that user's real key, so `--verify-sigv4` checks every
/// signature cryptographically.
async fn verified_user_config(server: &TestServer, name: &str) -> aws_config::SdkConfig {
    let boot = sdk_config_with(server, "test", "test", None).await;
    let iam_boot = aws_sdk_iam::Client::new(&boot);
    iam_boot.create_user().user_name(name).send().await.unwrap();
    let ak = iam_boot
        .create_access_key()
        .user_name(name)
        .send()
        .await
        .unwrap();
    let key = ak.access_key().unwrap();
    sdk_config_with(server, key.access_key_id(), key.secret_access_key(), None).await
}

fn python_zip() -> Vec<u8> {
    use std::io::Write;
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    zip.start_file("index.py", zip::write::SimpleFileOptions::default())
        .unwrap();
    zip.write_all(b"def handler(event, context):\n    return 1\n")
        .unwrap();
    zip.finish().unwrap().into_inner()
}

/// S3 signs the canonical URI by encoding the (decoded) key segments once.
/// A key holding characters the SDK percent-encodes on the wire (`=`, space,
/// `+`) must still verify: the server must canonicalize the decoded path, not
/// re-encode the already-encoded wire form.
#[tokio::test]
async fn s3_object_key_with_reserved_characters_verifies() {
    let server = start_verified().await;
    let cfg = verified_user_config(&server, "s3-path-user").await;
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&cfg)
            .force_path_style(true)
            .build(),
    );
    s3.create_bucket()
        .bucket("sigv4-path-bucket")
        .send()
        .await
        .expect("CreateBucket must verify");
    let key = "dt=2024-01-01/a b+c.txt";
    s3.put_object()
        .bucket("sigv4-path-bucket")
        .key(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
        .send()
        .await
        .expect("PutObject with an encoded key must verify");
    let got = s3
        .get_object()
        .bucket("sigv4-path-bucket")
        .key(key)
        .send()
        .await
        .expect("GetObject with an encoded key must verify");
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"hello");
}

/// Non-S3 REST services sign the double-encoded path. An ARN in the path
/// (`:` encoded as `%3A` on the wire) must verify for Lambda GetFunction and
/// ListTags.
#[tokio::test]
async fn lambda_arn_in_path_verifies() {
    let server = start_verified().await;
    let cfg = verified_user_config(&server, "lambda-path-user").await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    let created = lambda
        .create_function()
        .function_name("sigv4-path-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(python_zip()))
                .build(),
        )
        .tags("team", "a b")
        .send()
        .await
        .expect("CreateFunction must verify");
    let arn = created.function_arn().unwrap().to_string();
    lambda
        .get_function()
        .function_name(&arn)
        .send()
        .await
        .expect("GetFunction by ARN must verify");
    let tags = lambda
        .list_tags()
        .resource(&arn)
        .send()
        .await
        .expect("ListTags with an ARN in the path must verify");
    assert_eq!(
        tags.tags().and_then(|t| t.get("team")).map(String::as_str),
        Some("a b")
    );
}
