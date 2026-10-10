mod helpers;

use aws_sdk_cognitoidentityprovider::types::AttributeType;
use helpers::TestServer;

/// User pool + user pool client + admin-created user survive a restart.
#[tokio::test]
async fn persistence_round_trip_pool_client_user() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("persist-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let app_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("persist-client")
        .send()
        .await
        .unwrap();
    let client_id = app_client
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("alice")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("alice@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Drop pre-restart client before restart.
    drop(client);
    server.restart().await;
    let client = server.cognito_client().await;

    let described = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(described.user_pool().unwrap().name(), Some("persist-pool"));

    let described_client = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        described_client.user_pool_client().unwrap().client_name(),
        Some("persist-client")
    );

    let user = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("alice")
        .send()
        .await
        .unwrap();
    let email = user
        .user_attributes()
        .iter()
        .find(|a| a.name() == "email")
        .and_then(|a| a.value());
    assert_eq!(email, Some("alice@example.com"));
}

/// Groups and tag resources round-trip across a restart.
#[tokio::test]
async fn persistence_groups_and_tags() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("tag-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let pool_arn = pool.user_pool().unwrap().arn().unwrap().to_string();

    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("admins")
        .description("Admin group")
        .send()
        .await
        .unwrap();

    client
        .tag_resource()
        .resource_arn(&pool_arn)
        .tags("env", "prod")
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.cognito_client().await;

    let group = client
        .get_group()
        .user_pool_id(&pool_id)
        .group_name("admins")
        .send()
        .await
        .unwrap();
    assert_eq!(group.group().unwrap().description(), Some("Admin group"));

    let tags = client
        .list_tags_for_resource()
        .resource_arn(&pool_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        tags.tags().and_then(|t| t.get("env")).map(String::as_str),
        Some("prod")
    );
}

/// Auth events introspection buffer does NOT persist across restarts.
/// We deliberately trigger a failed AdminInitiateAuth so the pre-restart
/// buffer is non-empty; if we skipped that step this test would pass
/// trivially even if auth-events were persisted.
#[tokio::test]
async fn persistence_auth_events_not_persisted() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("events-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let app_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("events-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AdminNoSrpAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = app_client
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("bob")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("bob")
        .password("CorrectP@ssw0rd1")
        .permanent(true)
        .send()
        .await
        .unwrap();

    // Failed auth attempt — emits a SIGN_IN_FAILURE auth event.
    let _ = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminNoSrpAuth)
        .auth_parameters("USERNAME", "bob")
        .auth_parameters("PASSWORD", "WrongPassword1!")
        .send()
        .await;

    // Pre-restart buffer must be non-empty — otherwise the post-restart
    // assertion is vacuous.
    let pre = reqwest::get(format!(
        "{}/_fakecloud/cognito/auth-events",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let pre_events = pre
        .get("events")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        !pre_events.is_empty(),
        "pre-restart auth_events buffer should be non-empty so the \
         post-restart emptiness assertion is meaningful, got: {pre:?}"
    );

    drop(client);
    server.restart().await;

    // Pool survived.
    let client = server.cognito_client().await;
    let described = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(described.user_pool().unwrap().name(), Some("events-pool"));

    // Auth events buffer reset to empty.
    let post = reqwest::get(format!(
        "{}/_fakecloud/cognito/auth-events",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let events = post
        .get("events")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        events.is_empty(),
        "auth_events buffer should reset on restart, got: {post:?}"
    );
}

/// Custom pool/client ids and a seeded authenticator secret survive a
/// restart. Seeding is the last write before the restart, so the
/// introspection endpoint itself must persist it.
#[tokio::test]
async fn persistence_custom_ids_and_seeded_software_token() {
    use aws_sdk_cognitoidentityprovider::types::{
        AuthFlowType, ChallengeNameType, ExplicitAuthFlowsType, SoftwareTokenMfaConfigType,
        UserPoolMfaType,
    };
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.cognito_client().await;

    client
        .create_user_pool()
        .pool_name("local")
        .user_pool_tags("_custom_id_", "us-east-1_Local")
        .send()
        .await
        .unwrap();
    client
        .set_user_pool_mfa_config()
        .user_pool_id("us-east-1_Local")
        .mfa_configuration(UserPoolMfaType::On)
        .software_token_mfa_configuration(
            SoftwareTokenMfaConfigType::builder().enabled(true).build(),
        )
        .send()
        .await
        .unwrap();
    client
        .create_user_pool_client()
        .user_pool_id("us-east-1_Local")
        .client_name("_custom_id_:localclient")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .send()
        .await
        .unwrap();
    client
        .admin_create_user()
        .user_pool_id("us-east-1_Local")
        .username("dev")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id("us-east-1_Local")
        .username("dev")
        .password("Passw0rd!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    let secret = "JBSWY3DPEHPK3PXP";
    let seeded = reqwest::Client::new()
        .post(format!(
            "{}/_fakecloud/cognito/software-token",
            server.endpoint()
        ))
        .json(&serde_json::json!({
            "userPoolId": "us-east-1_Local",
            "username": "dev",
            "secretCode": secret,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(seeded.status(), 200);

    drop(client);
    server.restart().await;
    let client = server.cognito_client().await;

    let challenge = client
        .admin_initiate_auth()
        .user_pool_id("us-east-1_Local")
        .client_id("localclient")
        .auth_flow(AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "dev")
        .auth_parameters("PASSWORD", "Passw0rd!")
        .send()
        .await
        .unwrap();
    assert_eq!(
        challenge.challenge_name(),
        Some(&ChallengeNameType::SoftwareTokenMfa)
    );
    let done = client
        .admin_respond_to_auth_challenge()
        .user_pool_id("us-east-1_Local")
        .client_id("localclient")
        .challenge_name(ChallengeNameType::SoftwareTokenMfa)
        .session(challenge.session().unwrap())
        .challenge_responses("USERNAME", "dev")
        .challenge_responses(
            "SOFTWARE_TOKEN_MFA_CODE",
            fakecloud_cognito::totp::compute_totp_now(secret).unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(done
        .authentication_result()
        .and_then(|r| r.access_token())
        .is_some());
}
