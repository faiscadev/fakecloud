//! E2E tests for Phase 3 permission boundary enforcement.
//!
//! Each test spawns fakecloud with `FAKECLOUD_IAM=strict`, creates a
//! user via the reserved root bypass, attaches an inline identity
//! policy + a managed boundary, then signs a follow-up request with
//! the user's own credentials and observes the gated decision.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_iam::Client as IamClient;
use aws_sdk_sts::Client as StsClient;
use helpers::TestServer;

async fn start_strict() -> TestServer {
    TestServer::start_with_env(&[
        ("FAKECLOUD_IAM", "strict"),
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
    ])
    .await
}

async fn sdk_config_with(server: &TestServer, akid: &str, secret: &str) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            akid,
            secret,
            None,
            None,
            "fakecloud-iam-boundary",
        ))
        .load()
        .await
}

async fn root_client(server: &TestServer) -> IamClient {
    let boot = sdk_config_with(server, "test", "test").await;
    IamClient::new(&boot)
}

async fn bootstrap_user(server: &TestServer, name: &str) -> (String, String) {
    let iam = root_client(server).await;
    iam.create_user().user_name(name).send().await.unwrap();
    let ak = iam
        .create_access_key()
        .user_name(name)
        .send()
        .await
        .unwrap();
    let key = ak.access_key().unwrap();
    (
        key.access_key_id().to_string(),
        key.secret_access_key().to_string(),
    )
}

async fn create_managed_policy(server: &TestServer, name: &str, document: &str) -> String {
    let iam = root_client(server).await;
    let resp = iam
        .create_policy()
        .policy_name(name)
        .policy_document(document)
        .send()
        .await
        .unwrap();
    resp.policy().unwrap().arn().unwrap().to_string()
}

async fn attach_inline(server: &TestServer, user: &str, name: &str, document: &str) {
    root_client(server)
        .await
        .put_user_policy()
        .user_name(user)
        .policy_name(name)
        .policy_document(document)
        .send()
        .await
        .unwrap();
}

async fn set_boundary(server: &TestServer, user: &str, boundary_arn: &str) {
    root_client(server)
        .await
        .put_user_permissions_boundary()
        .user_name(user)
        .permissions_boundary(boundary_arn)
        .send()
        .await
        .unwrap();
}

// ======================================================================

#[tokio::test]
async fn boundary_caps_identity_allow_all() {
    // alice has Allow-all on her identity side, but a boundary that
    // only permits sts:GetSessionToken. GetSessionToken must
    // succeed; everything else must be implicit-denied.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "alice").await;

    attach_inline(
        &server,
        "alice",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
    )
    .await;

    let boundary_arn = create_managed_policy(
        &server,
        "BoundaryOnlyGetSessionToken",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:GetSessionToken","Resource":"*"}]}"#,
    )
    .await;
    set_boundary(&server, "alice", &boundary_arn).await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;

    // Allowed: action covered by both identity and boundary.
    let sts = StsClient::new(&cfg);
    let session = sts.get_session_token().send().await.unwrap();
    assert!(session.credentials().is_some());

    // Denied: action allowed by identity but not by boundary.
    let iam = IamClient::new(&cfg);
    let err = iam.list_users().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains(r#"code: Some("AccessDenied")"#),
        "expected AccessDenied, got {msg}"
    );
}

#[tokio::test]
async fn boundary_cannot_escape_itself() {
    // Canonical "can a user escape their boundary" test. bob's
    // boundary forbids iam:DeleteUserPermissionsBoundary, so bob
    // cannot remove his own boundary even though he has Allow-all
    // on the identity side. After Phase 3 this is enforced.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "bob").await;

    attach_inline(
        &server,
        "bob",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
    )
    .await;

    let boundary_arn = create_managed_policy(
        &server,
        "BoundaryNoBoundaryEscape",
        r#"{
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Allow","Action":"*","Resource":"*"},
                {"Effect":"Deny","Action":["iam:DeleteUserPermissionsBoundary","iam:PutUserPermissionsBoundary"],"Resource":"*"}
            ]
        }"#,
    )
    .await;
    set_boundary(&server, "bob", &boundary_arn).await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let iam = IamClient::new(&cfg);

    let err = iam
        .delete_user_permissions_boundary()
        .user_name("bob")
        .send()
        .await
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains(r#"code: Some("AccessDenied")"#),
        "expected AccessDenied, got {msg}"
    );
}

#[tokio::test]
async fn dangling_boundary_arn_denies_everything() {
    // Attach a boundary that points at a managed policy ARN we then
    // delete. Every action with the user's credentials must deny
    // until the boundary is removed, matching AWS behavior.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "carol").await;

    attach_inline(
        &server,
        "carol",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
    )
    .await;

    let boundary_arn = create_managed_policy(
        &server,
        "TransientBoundary",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
    )
    .await;
    set_boundary(&server, "carol", &boundary_arn).await;

    // Delete the boundary managed policy out from under her.
    root_client(&server)
        .await
        .delete_policy()
        .policy_arn(&boundary_arn)
        .send()
        .await
        .unwrap();

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_session_token().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains(r#"code: Some("AccessDenied")"#),
        "expected AccessDenied, got {msg}"
    );
}

#[tokio::test]
async fn boundary_explicit_deny_wins() {
    // boundary contains an explicit Deny — Deny precedence works
    // across layers.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "dave").await;

    attach_inline(
        &server,
        "dave",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
    )
    .await;

    let boundary_arn = create_managed_policy(
        &server,
        "BoundaryDenySessionToken",
        r#"{
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Allow","Action":"*","Resource":"*"},
                {"Effect":"Deny","Action":"sts:GetSessionToken","Resource":"*"}
            ]
        }"#,
    )
    .await;
    set_boundary(&server, "dave", &boundary_arn).await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_session_token().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains(r#"code: Some("AccessDenied")"#),
        "expected AccessDenied, got {msg}"
    );
}

#[tokio::test]
async fn no_boundary_unchanged_behavior() {
    // Regression guard: a user without any boundary behaves exactly
    // as in Phase 1/2 — identity Allow is enough.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "eve").await;
    attach_inline(
        &server,
        "eve",
        "AllowGetSessionToken",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:GetSessionToken","Resource":"*"}]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let session = sts.get_session_token().send().await.unwrap();
    assert!(session.credentials().is_some());
}
