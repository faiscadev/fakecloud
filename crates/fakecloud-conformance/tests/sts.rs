mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("sts", "GetCallerIdentity", checksum = "163a2f0e")]
#[tokio::test]
async fn sts_get_caller_identity() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client.get_caller_identity().send().await.unwrap();
    assert!(resp.account().is_some());
    assert!(resp.arn().is_some());
}

#[test_action("sts", "AssumeRole", checksum = "3a2fbf12")]
#[tokio::test]
async fn sts_assume_role() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    // The role must exist with a trust policy admitting the caller before
    // AssumeRole succeeds — assuming a non-existent role is denied (matches AWS).
    server
        .iam_client()
        .await
        .create_role()
        .role_name("test-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/test-role")
        .role_session_name("test-session")
        .send()
        .await
        .unwrap();
    assert!(resp.credentials().is_some());
}

#[test_action("sts", "AssumeRoleWithWebIdentity", checksum = "fb45529e")]
#[tokio::test]
async fn sts_assume_role_with_web_identity() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client
        .assume_role_with_web_identity()
        .role_arn("arn:aws:iam::123456789012:role/web-role")
        .role_session_name("web-session")
        .web_identity_token("fake-token")
        .send()
        .await
        .unwrap();
    assert!(resp.credentials().is_some());
}

#[test_action("sts", "AssumeRoleWithSAML", checksum = "b2f7f5e1")]
#[tokio::test]
async fn sts_assume_role_with_saml() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client
        .assume_role_with_saml()
        .role_arn("arn:aws:iam::123456789012:role/saml-role")
        .principal_arn("arn:aws:iam::123456789012:saml-provider/test")
        .saml_assertion("fake-assertion")
        .send()
        .await
        .unwrap();
    assert!(resp.credentials().is_some());
}

#[test_action("sts", "GetSessionToken", checksum = "c12501d4")]
#[tokio::test]
async fn sts_get_session_token() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client.get_session_token().send().await.unwrap();
    assert!(resp.credentials().is_some());
}

#[test_action("sts", "GetFederationToken", checksum = "ed833607")]
#[tokio::test]
async fn sts_get_federation_token() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client
        .get_federation_token()
        .name("fed-user")
        .send()
        .await
        .unwrap();
    assert!(resp.credentials().is_some());
}

#[test_action("sts", "GetAccessKeyInfo", checksum = "2c96c5eb")]
#[tokio::test]
async fn sts_get_access_key_info() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client
        .get_access_key_info()
        .access_key_id("AKIAIOSFODNN7EXAMPLE")
        .send()
        .await
        .unwrap();
    assert!(resp.account().is_some());
}

#[test_action("sts", "DecodeAuthorizationMessage", checksum = "4573ceaa")]
#[tokio::test]
async fn sts_decode_authorization_message() {
    // F4 turned this into a real round-trip on tokens produced by
    // `fakecloud_iam::auth_message::encode_deny`. Mint a valid
    // zlib+base64 token here so the decoder has something to chew on.
    use base64::Engine;
    use flate2::write::ZlibEncoder;
    use flate2::Compression;
    use std::io::Write;

    let server = TestServer::start().await;
    let client = server.sts_client().await;

    let payload = serde_json::json!({
        "allowed": false,
        "explicitDeny": true,
        "matchedStatements": { "items": [] },
    });
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&serde_json::to_vec(&payload).unwrap())
        .unwrap();
    let token = base64::engine::general_purpose::STANDARD.encode(encoder.finish().unwrap());

    let result = client
        .decode_authorization_message()
        .encoded_message(&token)
        .send()
        .await
        .unwrap();
    assert!(result.decoded_message().is_some());
}

#[test_action("sts", "AssumeRoot", checksum = "ff632886")]
#[tokio::test]
async fn sts_assume_root() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client
        .assume_root()
        .target_principal("123456789012")
        .task_policy_arn(
            aws_sdk_sts::types::PolicyDescriptorType::builder()
                .arn("arn:aws:iam::aws:policy/IAMAuditRootUserCredentials")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert!(resp.credentials().is_some());
}

#[test_action("sts", "GetWebIdentityToken", checksum = "9ea6bbde")]
#[tokio::test]
async fn sts_get_web_identity_token() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client
        .get_web_identity_token()
        .audience("fakecloud-test")
        .duration_seconds(900)
        .signing_algorithm("RS256")
        .send()
        .await
        .unwrap();
    let token = resp.web_identity_token().unwrap();
    assert!(
        token.split('.').count() == 3,
        "expected JWT triple, got {token}"
    );
}

#[test_action("sts", "GetDelegatedAccessToken", checksum = "93cb2870")]
#[tokio::test]
async fn sts_get_delegated_access_token() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client
        .get_delegated_access_token()
        .trade_in_token("fakecloud-trade-in-token")
        .send()
        .await
        .unwrap();
    assert!(resp.credentials().is_some());
}
