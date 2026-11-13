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

#[test_action("sts", "AssumeRole", checksum = "fd5402b1")]
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

#[test_action("sts", "AssumeRoleWithWebIdentity", checksum = "5d2b2767")]
#[tokio::test]
async fn sts_assume_role_with_web_identity() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    // The role must exist with a trust policy admitting the federated
    // principal before AssumeRoleWithWebIdentity succeeds — assuming a
    // non-existent role is denied (matches AWS).
    server
        .iam_client()
        .await
        .create_role()
        .role_name("web-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithWebIdentity"}]}"#,
        )
        .send()
        .await
        .unwrap();
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

#[test_action("sts", "AssumeRoleWithSAML", checksum = "7bd82035")]
#[tokio::test]
async fn sts_assume_role_with_saml() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    // The role must exist with a trust policy admitting the SAML
    // principal before AssumeRoleWithSAML succeeds — assuming a
    // non-existent role is denied (matches AWS).
    server
        .iam_client()
        .await
        .create_role()
        .role_name("saml-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithSAML"}]}"#,
        )
        .send()
        .await
        .unwrap();
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

#[test_action("sts", "GetSessionToken", checksum = "794ce371")]
#[tokio::test]
async fn sts_get_session_token() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client.get_session_token().send().await.unwrap();
    assert!(resp.credentials().is_some());
}

#[test_action("sts", "GetFederationToken", checksum = "4937080e")]
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

#[test_action("sts", "AssumeRoot", checksum = "5c6126f8")]
#[tokio::test]
async fn sts_assume_root() {
    let server = TestServer::start().await;
    let client = server.sts_client().await;
    let resp = client
        .assume_root()
        .target_principal("123456789012")
        .task_policy_arn(
            aws_sdk_sts::types::PolicyDescriptorType::builder()
                .arn("arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials")
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

// ---------------------------------------------------------------------------
// MinimumSessionTokenSize / SessionTokenSize / SessionTokenUtilization are
// newer than the vendored aws-sdk-sts, so exercise them via a raw awsQuery POST.
// ---------------------------------------------------------------------------

const STS_RAW_AUTH: &str =
    "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/sts/aws4_request, SignedHeaders=host, Signature=0";

async fn sts_raw(server: &TestServer, body: &str) -> (reqwest::StatusCode, String) {
    let resp = reqwest::Client::new()
        .post(server.endpoint())
        .header("content-type", "application/x-www-form-urlencoded")
        .header("Authorization", STS_RAW_AUTH)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status();
    (status, resp.text().await.unwrap())
}

/// Read the text of the first `<tag>` element in an XML response.
fn xml_text<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(&xml[start..end])
}

#[tokio::test]
async fn sts_minimum_session_token_size_pads_and_reports() {
    let server = TestServer::start().await;

    // Without the parameter the token keeps its natural size, and the response
    // still reports that size and its share of the 4,096-byte maximum.
    let (status, xml) = sts_raw(&server, "Action=GetSessionToken&Version=2011-06-15").await;
    assert!(status.is_success(), "GetSessionToken: {status} {xml}");
    let token = xml_text(&xml, "SessionToken").expect("session token");
    let size: usize = xml_text(&xml, "SessionTokenSize")
        .expect("session token size")
        .parse()
        .unwrap();
    assert_eq!(size, token.len(), "reported size matches the token: {xml}");
    let utilization: usize = xml_text(&xml, "SessionTokenUtilization")
        .expect("session token utilization")
        .parse()
        .unwrap();
    assert_eq!(utilization, size * 100 / 4096, "utilization: {xml}");

    // Asking for a minimum pads the token up to it.
    let (status, xml) = sts_raw(
        &server,
        "Action=GetSessionToken&Version=2011-06-15&MinimumSessionTokenSize=2048",
    )
    .await;
    assert!(status.is_success(), "padded GetSessionToken: {status}");
    let token = xml_text(&xml, "SessionToken").expect("session token");
    assert_eq!(token.len(), 2048, "token padded to the requested minimum");
    assert_eq!(xml_text(&xml, "SessionTokenSize"), Some("2048"));
    assert_eq!(xml_text(&xml, "SessionTokenUtilization"), Some("50"));

    // AssumeRole reports the same members. The role must exist with a trust
    // policy admitting the caller, as assuming a missing role is denied.
    server
        .iam_client()
        .await
        .create_role()
        .role_name("pad-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let (status, xml) = sts_raw(
        &server,
        "Action=AssumeRole&Version=2011-06-15\
         &RoleArn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fpad-role\
         &RoleSessionName=sess&MinimumSessionTokenSize=1024",
    )
    .await;
    assert!(status.is_success(), "AssumeRole: {status} {xml}");
    assert_eq!(
        xml_text(&xml, "SessionToken").map(str::len),
        Some(1024),
        "AssumeRole pads its token too"
    );
    assert_eq!(xml_text(&xml, "SessionTokenSize"), Some("1024"));

    // The model caps the minimum at 4,096 bytes.
    let (status, xml) = sts_raw(
        &server,
        "Action=GetSessionToken&Version=2011-06-15&MinimumSessionTokenSize=5000",
    )
    .await;
    assert_eq!(status, 400, "above the range maximum: {xml}");
    assert!(
        xml.contains("minimumSessionTokenSize"),
        "error names the offending member: {xml}"
    );
}
