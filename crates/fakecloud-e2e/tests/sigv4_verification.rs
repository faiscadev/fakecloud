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
