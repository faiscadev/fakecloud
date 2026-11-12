//! E2E tests for Phase 3 session-policy enforcement.
//!
//! Each test spawns fakecloud with `FAKECLOUD_IAM=strict`, creates a
//! role with an Allow-all trust + identity policy, then calls
//! `AssumeRole` with a restrictive `Policy` parameter and verifies that
//! the returned temporary credentials are gated by the session policy.

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
            "fakecloud-iam-session",
        ))
        .load()
        .await
}

async fn sdk_config_with_session(
    server: &TestServer,
    akid: &str,
    secret: &str,
    token: &str,
) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            akid,
            secret,
            Some(token.to_string()),
            None,
            "fakecloud-iam-session",
        ))
        .load()
        .await
}

async fn root_client(server: &TestServer) -> IamClient {
    let boot = sdk_config_with(server, "test", "test").await;
    IamClient::new(&boot)
}

const TRUST_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sts:AssumeRole"}]}"#;

async fn setup_role(server: &TestServer, name: &str) {
    let iam = root_client(server).await;
    iam.create_role()
        .role_name(name)
        .assume_role_policy_document(TRUST_POLICY)
        .send()
        .await
        .unwrap();
    iam.put_role_policy()
        .role_name(name)
        .policy_name("AllowAll")
        .policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
        )
        .send()
        .await
        .unwrap();
}

async fn assume_role_with_policy(
    server: &TestServer,
    role_name: &str,
    session_name: &str,
    policy: &str,
) -> (String, String, String) {
    let boot = sdk_config_with(server, "test", "test").await;
    let sts = StsClient::new(&boot);
    let resp = sts
        .assume_role()
        .role_arn(format!("arn:aws:iam::123456789012:role/{role_name}"))
        .role_session_name(session_name)
        .policy(policy)
        .send()
        .await
        .unwrap();
    let creds = resp.credentials().unwrap();
    (
        creds.access_key_id().to_string(),
        creds.secret_access_key().to_string(),
        creds.session_token().to_string(),
    )
}

// ======================================================================

#[tokio::test]
async fn session_policy_caps_assumed_role_permissions() {
    // Role has Allow-all identity policy. AssumeRole with a session
    // policy that only allows sts:GetCallerIdentity. The returned
    // credentials should be denied on iam:ListUsers but allowed on
    // sts:GetCallerIdentity.
    let server = start_strict().await;
    setup_role(&server, "wide-role").await;

    let (akid, secret, token) = assume_role_with_policy(
        &server,
        "wide-role",
        "narrow-session",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:GetCallerIdentity","Resource":"*"}]}"#,
    )
    .await;

    let cfg = sdk_config_with_session(&server, &akid, &secret, &token).await;

    // Allowed by both role policy and session policy.
    let sts = StsClient::new(&cfg);
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert!(identity
        .arn()
        .unwrap()
        .contains("assumed-role/wide-role/narrow-session"));

    // Denied by session policy (even though role has Allow-all).
    let iam = IamClient::new(&cfg);
    let err = iam.list_users().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains(r#"code: Some("AccessDenied")"#),
        "expected AccessDenied, got {msg}"
    );
}

#[tokio::test]
async fn session_policy_explicit_deny_wins() {
    let server = start_strict().await;
    setup_role(&server, "deny-role").await;

    let (akid, secret, token) = assume_role_with_policy(
        &server,
        "deny-role",
        "deny-session",
        r#"{
            "Version":"2012-10-17",
            "Statement":[
                {"Effect":"Allow","Action":"*","Resource":"*"},
                {"Effect":"Deny","Action":"sts:GetCallerIdentity","Resource":"*"}
            ]
        }"#,
    )
    .await;

    let cfg = sdk_config_with_session(&server, &akid, &secret, &token).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains(r#"code: Some("AccessDenied")"#),
        "expected AccessDenied, got {msg}"
    );
}

#[tokio::test]
async fn no_session_policy_unchanged_behavior() {
    // Regression: AssumeRole without a Policy parameter still works
    // exactly as before — role's identity policy is the sole gate.
    let server = start_strict().await;
    setup_role(&server, "plain-role").await;

    let boot = sdk_config_with(&server, "test", "test").await;
    let sts = StsClient::new(&boot);
    let resp = sts
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/plain-role")
        .role_session_name("plain-session")
        .send()
        .await
        .unwrap();
    let creds = resp.credentials().unwrap();

    let cfg = sdk_config_with_session(
        &server,
        creds.access_key_id(),
        creds.secret_access_key(),
        creds.session_token(),
    )
    .await;
    let iam = IamClient::new(&cfg);
    // Must succeed — the role has Allow-all and no session policy.
    let _users = iam.list_users().send().await.unwrap();
}

#[tokio::test]
async fn get_federation_token_session_policy_restricts() {
    // GetFederationToken also takes a Policy parameter. The explicit
    // Deny in the session policy must block the federated credential
    // even if the calling user had Allow-all.
    let server = start_strict().await;

    let iam = root_client(&server).await;
    iam.create_user().user_name("feduser").send().await.unwrap();
    let ak = iam
        .create_access_key()
        .user_name("feduser")
        .send()
        .await
        .unwrap();
    let key = ak.access_key().unwrap();
    iam.put_user_policy()
        .user_name("feduser")
        .policy_name("AllowAll")
        .policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
        )
        .send()
        .await
        .unwrap();

    let user_cfg = sdk_config_with(&server, key.access_key_id(), key.secret_access_key()).await;
    let sts = StsClient::new(&user_cfg);
    let resp = sts
        .get_federation_token()
        .name("fed-session")
        .policy(
            r#"{
                "Version":"2012-10-17",
                "Statement":[
                    {"Effect":"Allow","Action":"*","Resource":"*"},
                    {"Effect":"Deny","Action":"iam:ListUsers","Resource":"*"}
                ]
            }"#,
        )
        .send()
        .await
        .unwrap();
    let creds = resp.credentials().unwrap();

    let fed_cfg = sdk_config_with_session(
        &server,
        creds.access_key_id(),
        creds.secret_access_key(),
        creds.session_token(),
    )
    .await;

    // Session policy explicit Deny must block even though the calling
    // user had Allow-all. (The federated-user identity-policy path
    // is a known gap — FederatedUser principals don't inherit the
    // calling user's policies yet.)
    let iam_fed = IamClient::new(&fed_cfg);
    let err = iam_fed.list_users().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains(r#"code: Some("AccessDenied")"#),
        "expected AccessDenied, got {msg}"
    );
}
