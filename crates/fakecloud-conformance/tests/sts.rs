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
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let result = client
        .decode_authorization_message()
        .encoded_message("encoded-test-message")
        .send()
        .await
        .unwrap();
    assert!(result.decoded_message().is_some());
}
