mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ---------------------------------------------------------------------------
// User Pool lifecycle
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "CreateUserPool", checksum = "42068aef")]
#[tokio::test]
async fn cognito_create_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let resp = client
        .create_user_pool()
        .pool_name("conformance-pool")
        .send()
        .await
        .unwrap();
    let pool = resp.user_pool().unwrap();
    assert!(!pool.id().unwrap().is_empty());
    assert!(pool.arn().unwrap().contains(":userpool/"));
    assert_eq!(pool.name().unwrap(), "conformance-pool");
}

#[test_action("cognito-idp", "DescribeUserPool", checksum = "974e2ffd")]
#[tokio::test]
async fn cognito_describe_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let create = client
        .create_user_pool()
        .pool_name("desc-pool")
        .send()
        .await
        .unwrap();
    let pool_id = create.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.user_pool().unwrap().name().unwrap(), "desc-pool");
}

#[test_action("cognito-idp", "UpdateUserPool", checksum = "556fb3e5")]
#[tokio::test]
async fn cognito_update_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let create = client
        .create_user_pool()
        .pool_name("update-pool")
        .send()
        .await
        .unwrap();
    let pool_id = create.user_pool().unwrap().id().unwrap().to_string();

    client
        .update_user_pool()
        .user_pool_id(&pool_id)
        .mfa_configuration(aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::On)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.user_pool().unwrap().mfa_configuration(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::On),
    );
}

#[test_action("cognito-idp", "DeleteUserPool", checksum = "b1e5f200")]
#[tokio::test]
async fn cognito_delete_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let create = client
        .create_user_pool()
        .pool_name("del-pool")
        .send()
        .await
        .unwrap();
    let pool_id = create.user_pool().unwrap().id().unwrap().to_string();

    client
        .delete_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();

    let err = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap_err();
    let svc_err = err.into_service_error();
    assert!(svc_err.is_resource_not_found_exception());
}

#[test_action("cognito-idp", "ListUserPools", checksum = "f67b8722")]
#[tokio::test]
async fn cognito_list_user_pools() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    client
        .create_user_pool()
        .pool_name("list-pool-1")
        .send()
        .await
        .unwrap();
    client
        .create_user_pool()
        .pool_name("list-pool-2")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_user_pools()
        .max_results(10)
        .send()
        .await
        .unwrap();
    assert!(resp.user_pools().len() >= 2);
}

// ---------------------------------------------------------------------------
// User Pool Client lifecycle
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "CreateUserPoolClient", checksum = "74d65959")]
#[tokio::test]
async fn cognito_create_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("client-test-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("test-client")
        .send()
        .await
        .unwrap();
    let upc = resp.user_pool_client().unwrap();
    assert!(!upc.client_id().unwrap().is_empty());
    assert_eq!(upc.client_name().unwrap(), "test-client");
}

#[test_action("cognito-idp", "DescribeUserPoolClient", checksum = "7dc2fb48")]
#[tokio::test]
async fn cognito_describe_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("desc-client-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let created = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("desc-client")
        .send()
        .await
        .unwrap();
    let client_id = created
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let resp = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.user_pool_client().unwrap().client_name().unwrap(),
        "desc-client"
    );
}

#[test_action("cognito-idp", "UpdateUserPoolClient", checksum = "8ba26c73")]
#[tokio::test]
async fn cognito_update_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("upd-client-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let created = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("upd-client")
        .send()
        .await
        .unwrap();
    let client_id = created
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .update_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .client_name("renamed-client")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.user_pool_client().unwrap().client_name().unwrap(),
        "renamed-client"
    );
}

#[test_action("cognito-idp", "DeleteUserPoolClient", checksum = "954e5fa3")]
#[tokio::test]
async fn cognito_delete_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("del-client-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let created = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("del-client")
        .send()
        .await
        .unwrap();
    let client_id = created
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .delete_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();

    let err = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_resource_not_found_exception());
}

#[test_action("cognito-idp", "ListUserPoolClients", checksum = "3946c12e")]
#[tokio::test]
async fn cognito_list_user_pool_clients() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("list-client-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("c1")
        .send()
        .await
        .unwrap();
    client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("c2")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_user_pool_clients()
        .user_pool_id(&pool_id)
        .max_results(10)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.user_pool_clients().len(), 2);
}

// ---------------------------------------------------------------------------
// User management
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "AdminCreateUser", checksum = "59b00da9")]
#[tokio::test]
async fn cognito_admin_create_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("user-mgmt-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("testuser")
        .user_attributes(
            aws_sdk_cognitoidentityprovider::types::AttributeType::builder()
                .name("email")
                .value("test@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let user = resp.user().unwrap();
    assert_eq!(user.username().unwrap(), "testuser");
}

#[test_action("cognito-idp", "AdminGetUser", checksum = "07298034")]
#[tokio::test]
async fn cognito_admin_get_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("get-user-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("getme")
        .send()
        .await
        .unwrap();

    let resp = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("getme")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.username(), "getme");
}

#[test_action("cognito-idp", "AdminDeleteUser", checksum = "df0f38e1")]
#[tokio::test]
async fn cognito_admin_delete_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("del-user-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("delme")
        .send()
        .await
        .unwrap();

    client
        .admin_delete_user()
        .user_pool_id(&pool_id)
        .username("delme")
        .send()
        .await
        .unwrap();

    let err = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("delme")
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_user_not_found_exception());
}

#[test_action("cognito-idp", "AdminDisableUser", checksum = "dea29c0e")]
#[tokio::test]
async fn cognito_admin_disable_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("disable-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("disableme")
        .send()
        .await
        .unwrap();

    client
        .admin_disable_user()
        .user_pool_id(&pool_id)
        .username("disableme")
        .send()
        .await
        .unwrap();

    let resp = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("disableme")
        .send()
        .await
        .unwrap();
    assert!(!resp.enabled());
}

#[test_action("cognito-idp", "AdminEnableUser", checksum = "4bf631d2")]
#[tokio::test]
async fn cognito_admin_enable_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("enable-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("enableme")
        .send()
        .await
        .unwrap();

    client
        .admin_disable_user()
        .user_pool_id(&pool_id)
        .username("enableme")
        .send()
        .await
        .unwrap();

    client
        .admin_enable_user()
        .user_pool_id(&pool_id)
        .username("enableme")
        .send()
        .await
        .unwrap();

    let resp = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("enableme")
        .send()
        .await
        .unwrap();
    assert!(resp.enabled());
}

#[test_action("cognito-idp", "AdminUpdateUserAttributes", checksum = "52c6f704")]
#[tokio::test]
async fn cognito_admin_update_user_attributes() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("update-attrs-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("attruser")
        .send()
        .await
        .unwrap();

    client
        .admin_update_user_attributes()
        .user_pool_id(&pool_id)
        .username("attruser")
        .user_attributes(
            aws_sdk_cognitoidentityprovider::types::AttributeType::builder()
                .name("email")
                .value("new@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("attruser")
        .send()
        .await
        .unwrap();
    let email = resp
        .user_attributes()
        .iter()
        .find(|a| a.name() == "email")
        .unwrap();
    assert_eq!(email.value().unwrap(), "new@example.com");
}

#[test_action("cognito-idp", "AdminDeleteUserAttributes", checksum = "b27f67be")]
#[tokio::test]
async fn cognito_admin_delete_user_attributes() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("del-attrs-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("delattr")
        .user_attributes(
            aws_sdk_cognitoidentityprovider::types::AttributeType::builder()
                .name("email")
                .value("del@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .admin_delete_user_attributes()
        .user_pool_id(&pool_id)
        .username("delattr")
        .user_attribute_names("email")
        .send()
        .await
        .unwrap();

    let resp = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("delattr")
        .send()
        .await
        .unwrap();
    assert!(!resp.user_attributes().iter().any(|a| a.name() == "email"));
}

#[test_action("cognito-idp", "ListUsers", checksum = "3bf0c621")]
#[tokio::test]
async fn cognito_list_users() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("list-users-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("user1")
        .send()
        .await
        .unwrap();
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("user2")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_users()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.users().len(), 2);
}

// ---------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "AdminSetUserPassword", checksum = "d903c3d1")]
#[tokio::test]
async fn cognito_admin_set_user_password() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("pwd-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("pwduser")
        .send()
        .await
        .unwrap();

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("pwduser")
        .password("Test1234!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    let user = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("pwduser")
        .send()
        .await
        .unwrap();
    assert_eq!(user.user_status().unwrap().as_str(), "CONFIRMED");
}

#[test_action("cognito-idp", "AdminInitiateAuth", checksum = "8890cfdf")]
#[tokio::test]
async fn cognito_admin_initiate_auth() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("auth-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("auth-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AdminNoSrpAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("authuser")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("authuser")
        .password("Test1234!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    let resp = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminNoSrpAuth)
        .auth_parameters("USERNAME", "authuser")
        .auth_parameters("PASSWORD", "Test1234!")
        .send()
        .await
        .unwrap();

    assert!(resp.authentication_result().unwrap().id_token().is_some());
    assert!(resp
        .authentication_result()
        .unwrap()
        .access_token()
        .is_some());
    assert!(resp
        .authentication_result()
        .unwrap()
        .refresh_token()
        .is_some());
}

#[test_action("cognito-idp", "InitiateAuth", checksum = "f2d9f8ac")]
#[tokio::test]
async fn cognito_initiate_auth() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("initauth-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("initauth-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowUserPasswordAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("inituser")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("inituser")
        .password("Test1234!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    let resp = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "inituser")
        .auth_parameters("PASSWORD", "Test1234!")
        .send()
        .await
        .unwrap();
    assert!(resp.authentication_result().unwrap().id_token().is_some());
}

#[test_action("cognito-idp", "AdminRespondToAuthChallenge", checksum = "6f8ae02b")]
#[test_action("cognito-idp", "RespondToAuthChallenge", checksum = "4059d3bd")]
#[tokio::test]
async fn cognito_respond_to_auth_challenge() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("challenge-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("challenge-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AdminNoSrpAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("challengeuser")
        .temporary_password("TempPass1!")
        .send()
        .await
        .unwrap();

    let resp = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminNoSrpAuth)
        .auth_parameters("USERNAME", "challengeuser")
        .auth_parameters("PASSWORD", "TempPass1!")
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.challenge_name(),
        Some(&aws_sdk_cognitoidentityprovider::types::ChallengeNameType::NewPasswordRequired),
    );
    let session = resp.session().unwrap().to_string();

    let resp2 = client
        .admin_respond_to_auth_challenge()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .challenge_name(
            aws_sdk_cognitoidentityprovider::types::ChallengeNameType::NewPasswordRequired,
        )
        .challenge_responses("USERNAME", "challengeuser")
        .challenge_responses("NEW_PASSWORD", "NewPass1234!")
        .session(&session)
        .send()
        .await
        .unwrap();

    assert!(resp2.authentication_result().unwrap().id_token().is_some());
}

#[test_action("cognito-idp", "SignUp", checksum = "295585cc")]
#[test_action("cognito-idp", "ConfirmSignUp", checksum = "a2468bd2")]
#[tokio::test]
async fn cognito_sign_up_and_confirm() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("signup-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("signup-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowUserPasswordAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let signup = client
        .sign_up()
        .client_id(&client_id)
        .username("signupuser")
        .password("SignUp1234!")
        .send()
        .await
        .unwrap();
    assert!(!signup.user_confirmed());
    assert!(!signup.user_sub().is_empty());

    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("signupuser")
        .confirmation_code("123456")
        .send()
        .await
        .unwrap();

    let resp = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "signupuser")
        .auth_parameters("PASSWORD", "SignUp1234!")
        .send()
        .await
        .unwrap();
    assert!(resp
        .authentication_result()
        .unwrap()
        .access_token()
        .is_some());
}

#[test_action("cognito-idp", "AdminConfirmSignUp", checksum = "e13b133c")]
#[tokio::test]
async fn cognito_admin_confirm_sign_up() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("adminconfirm-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("adminconfirm-client")
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("adminconfirm")
        .password("Confirm1234!")
        .send()
        .await
        .unwrap();

    client
        .admin_confirm_sign_up()
        .user_pool_id(&pool_id)
        .username("adminconfirm")
        .send()
        .await
        .unwrap();

    let user = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("adminconfirm")
        .send()
        .await
        .unwrap();
    assert_eq!(user.user_status().unwrap().as_str(), "CONFIRMED");
}

// ---------------------------------------------------------------------------
// Password & Session Management
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "ChangePassword", checksum = "037ca3c2")]
#[tokio::test]
async fn cognito_change_password() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("chgpwd-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("chgpwd-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowUserPasswordAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("chguser")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("chguser")
        .password("OldPass1!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "chguser")
        .auth_parameters("PASSWORD", "OldPass1!")
        .send()
        .await
        .unwrap();
    let access_token = auth
        .authentication_result()
        .unwrap()
        .access_token()
        .unwrap()
        .to_string();

    client
        .change_password()
        .access_token(&access_token)
        .previous_password("OldPass1!")
        .proposed_password("NewPass1!")
        .send()
        .await
        .unwrap();

    // Auth with old password should fail
    let old_auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "chguser")
        .auth_parameters("PASSWORD", "OldPass1!")
        .send()
        .await;
    assert!(old_auth.is_err(), "Old password should no longer work");

    // Auth with new password should succeed
    let new_auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "chguser")
        .auth_parameters("PASSWORD", "NewPass1!")
        .send()
        .await;
    assert!(new_auth.is_ok(), "New password should work");
}

#[test_action("cognito-idp", "ForgotPassword", checksum = "e64c387b")]
#[test_action("cognito-idp", "ConfirmForgotPassword", checksum = "1246f324")]
#[tokio::test]
async fn cognito_forgot_password_flow() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("forgot-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("forgot-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowUserPasswordAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("forgotuser")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("forgotuser")
        .password("OldPass1!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    client
        .forgot_password()
        .client_id(&client_id)
        .username("forgotuser")
        .send()
        .await
        .unwrap();

    // Get confirmation code via introspection
    let code_resp: serde_json::Value = reqwest::get(format!(
        "{}/_fakecloud/cognito/confirmation-codes/{}/forgotuser",
        server.endpoint(),
        pool_id
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    let code = code_resp["confirmationCode"].as_str().unwrap().to_string();

    client
        .confirm_forgot_password()
        .client_id(&client_id)
        .username("forgotuser")
        .confirmation_code(&code)
        .password("ResetPass1!")
        .send()
        .await
        .unwrap();
}

#[test_action("cognito-idp", "AdminResetUserPassword", checksum = "00b62940")]
#[tokio::test]
async fn cognito_admin_reset_user_password() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("reset-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .password("Pass1234!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    client
        .admin_reset_user_password()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .send()
        .await
        .unwrap();

    let user = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .send()
        .await
        .unwrap();
    assert_eq!(user.user_status().unwrap().as_str(), "RESET_REQUIRED");
}

#[test_action("cognito-idp", "GlobalSignOut", checksum = "1b6afd7d")]
#[tokio::test]
async fn cognito_global_sign_out() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("signout-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("signout-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowUserPasswordAuth,
        )
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowRefreshTokenAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("signoutuser")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("signoutuser")
        .password("Pass1234!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "signoutuser")
        .auth_parameters("PASSWORD", "Pass1234!")
        .send()
        .await
        .unwrap();
    let auth_result = auth.authentication_result().unwrap();
    let access_token = auth_result.access_token().unwrap().to_string();
    let refresh_token = auth_result.refresh_token().unwrap().to_string();

    client
        .global_sign_out()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();

    // Verify refresh token no longer works after sign out
    let refresh_auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::RefreshTokenAuth)
        .auth_parameters("REFRESH_TOKEN", &refresh_token)
        .send()
        .await;
    assert!(
        refresh_auth.is_err(),
        "Refresh token should be invalid after global sign out"
    );
}

#[test_action("cognito-idp", "AdminUserGlobalSignOut", checksum = "8461322c")]
#[tokio::test]
async fn cognito_admin_user_global_sign_out() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("adminsignout-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("adminsignout-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowUserPasswordAuth,
        )
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowRefreshTokenAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("adminsignout")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("adminsignout")
        .password("Pass1234!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    // Authenticate to get a refresh token
    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "adminsignout")
        .auth_parameters("PASSWORD", "Pass1234!")
        .send()
        .await
        .unwrap();
    let refresh_token = auth
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap()
        .to_string();

    client
        .admin_user_global_sign_out()
        .user_pool_id(&pool_id)
        .username("adminsignout")
        .send()
        .await
        .unwrap();

    // Verify refresh token no longer works after admin sign out
    let refresh_auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::RefreshTokenAuth)
        .auth_parameters("REFRESH_TOKEN", &refresh_token)
        .send()
        .await;
    assert!(
        refresh_auth.is_err(),
        "Refresh token should be invalid after admin global sign out"
    );
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "CreateGroup", checksum = "8458f036")]
#[tokio::test]
async fn cognito_create_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("grp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("admins")
        .description("Admin group")
        .precedence(1)
        .send()
        .await
        .unwrap();
    let group = resp.group().unwrap();
    assert_eq!(group.group_name().unwrap(), "admins");
    assert_eq!(group.description().unwrap(), "Admin group");
    assert_eq!(group.precedence().unwrap(), 1);
}

#[test_action("cognito-idp", "GetGroup", checksum = "a81d68fe")]
#[tokio::test]
async fn cognito_get_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("getgrp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("readers")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_group()
        .user_pool_id(&pool_id)
        .group_name("readers")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.group().unwrap().group_name().unwrap(), "readers");
}

#[test_action("cognito-idp", "UpdateGroup", checksum = "8c9b60d7")]
#[tokio::test]
async fn cognito_update_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("updgrp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("editors")
        .send()
        .await
        .unwrap();

    client
        .update_group()
        .user_pool_id(&pool_id)
        .group_name("editors")
        .description("Updated editors")
        .precedence(5)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_group()
        .user_pool_id(&pool_id)
        .group_name("editors")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.group().unwrap().description().unwrap(),
        "Updated editors"
    );
    assert_eq!(resp.group().unwrap().precedence().unwrap(), 5);
}

#[test_action("cognito-idp", "DeleteGroup", checksum = "ac33ddbb")]
#[tokio::test]
async fn cognito_delete_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("delgrp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("tempgrp")
        .send()
        .await
        .unwrap();

    client
        .delete_group()
        .user_pool_id(&pool_id)
        .group_name("tempgrp")
        .send()
        .await
        .unwrap();

    let err = client
        .get_group()
        .user_pool_id(&pool_id)
        .group_name("tempgrp")
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_resource_not_found_exception());
}

#[test_action("cognito-idp", "ListGroups", checksum = "75858aba")]
#[tokio::test]
async fn cognito_list_groups() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("listgrp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("g1")
        .send()
        .await
        .unwrap();
    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("g2")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_groups()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.groups().len(), 2);
}

#[test_action("cognito-idp", "AdminAddUserToGroup", checksum = "5fec870a")]
#[test_action("cognito-idp", "AdminRemoveUserFromGroup", checksum = "90421bbd")]
#[tokio::test]
async fn cognito_admin_add_remove_user_to_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("usergrp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("grpuser")
        .send()
        .await
        .unwrap();
    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("devs")
        .send()
        .await
        .unwrap();

    client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("grpuser")
        .group_name("devs")
        .send()
        .await
        .unwrap();

    let users = client
        .list_users_in_group()
        .user_pool_id(&pool_id)
        .group_name("devs")
        .send()
        .await
        .unwrap();
    assert_eq!(users.users().len(), 1);

    client
        .admin_remove_user_from_group()
        .user_pool_id(&pool_id)
        .username("grpuser")
        .group_name("devs")
        .send()
        .await
        .unwrap();

    let users2 = client
        .list_users_in_group()
        .user_pool_id(&pool_id)
        .group_name("devs")
        .send()
        .await
        .unwrap();
    assert!(users2.users().is_empty());
}

#[test_action("cognito-idp", "AdminListGroupsForUser", checksum = "ab20831c")]
#[tokio::test]
async fn cognito_admin_list_groups_for_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("grpforuser-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("multigrp")
        .send()
        .await
        .unwrap();
    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("team-a")
        .send()
        .await
        .unwrap();
    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("team-b")
        .send()
        .await
        .unwrap();

    client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("multigrp")
        .group_name("team-a")
        .send()
        .await
        .unwrap();
    client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("multigrp")
        .group_name("team-b")
        .send()
        .await
        .unwrap();

    let resp = client
        .admin_list_groups_for_user()
        .user_pool_id(&pool_id)
        .username("multigrp")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.groups().len(), 2);
}

#[test_action("cognito-idp", "ListUsersInGroup", checksum = "c3ee8bcd")]
#[tokio::test]
async fn cognito_list_users_in_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("usrsingrp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("qa")
        .send()
        .await
        .unwrap();
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("qa1")
        .send()
        .await
        .unwrap();
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("qa2")
        .send()
        .await
        .unwrap();

    client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("qa1")
        .group_name("qa")
        .send()
        .await
        .unwrap();
    client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("qa2")
        .group_name("qa")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_users_in_group()
        .user_pool_id(&pool_id)
        .group_name("qa")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.users().len(), 2);
}

// ---------------------------------------------------------------------------
// Self-service user operations
// ---------------------------------------------------------------------------

/// Helper: create pool + client + confirmed user, return (pool_id, client_id, access_token)
async fn setup_authenticated_user(
    client: &aws_sdk_cognitoidentityprovider::Client,
    pool_name: &str,
) -> (String, String, String) {
    let pool = client
        .create_user_pool()
        .pool_name(pool_name)
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("self-client")
        .explicit_auth_flows(
            aws_sdk_cognitoidentityprovider::types::ExplicitAuthFlowsType::AllowUserPasswordAuth,
        )
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("selfuser")
        .send()
        .await
        .unwrap();
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("selfuser")
        .password("SelfPass1!")
        .permanent(true)
        .send()
        .await
        .unwrap();

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "selfuser")
        .auth_parameters("PASSWORD", "SelfPass1!")
        .send()
        .await
        .unwrap();
    let access_token = auth
        .authentication_result()
        .unwrap()
        .access_token()
        .unwrap()
        .to_string();

    (pool_id, client_id, access_token)
}

#[test_action("cognito-idp", "GetUser", checksum = "43ac140c")]
#[tokio::test]
async fn cognito_get_user_self() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, _client_id, access_token) =
        setup_authenticated_user(&client, "getuser-self-pool").await;

    let resp = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.username(), "selfuser");
}

#[test_action("cognito-idp", "DeleteUser", checksum = "f81d91ec")]
#[tokio::test]
async fn cognito_delete_user_self() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, _client_id, access_token) =
        setup_authenticated_user(&client, "deluser-self-pool").await;

    client
        .delete_user()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();

    let err = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_not_authorized_exception());
}

#[test_action("cognito-idp", "UpdateUserAttributes", checksum = "23608e20")]
#[test_action("cognito-idp", "DeleteUserAttributes", checksum = "f40bb25d")]
#[tokio::test]
async fn cognito_update_delete_user_attributes_self() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, _client_id, access_token) =
        setup_authenticated_user(&client, "attrs-self-pool").await;

    client
        .update_user_attributes()
        .access_token(&access_token)
        .user_attributes(
            aws_sdk_cognitoidentityprovider::types::AttributeType::builder()
                .name("email")
                .value("self@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let user = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();
    assert!(user
        .user_attributes()
        .iter()
        .any(|a| a.name() == "email" && a.value() == Some("self@example.com")));

    client
        .delete_user_attributes()
        .access_token(&access_token)
        .user_attribute_names("email")
        .send()
        .await
        .unwrap();

    let user2 = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();
    assert!(!user2.user_attributes().iter().any(|a| a.name() == "email"));
}

#[test_action(
    "cognito-idp",
    "GetUserAttributeVerificationCode",
    checksum = "717d600d"
)]
#[test_action("cognito-idp", "VerifyUserAttribute", checksum = "fc368ddf")]
#[tokio::test]
async fn cognito_verify_user_attribute() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (pool_id, _client_id, access_token) =
        setup_authenticated_user(&client, "verify-attr-pool").await;

    // Set an email first
    client
        .update_user_attributes()
        .access_token(&access_token)
        .user_attributes(
            aws_sdk_cognitoidentityprovider::types::AttributeType::builder()
                .name("email")
                .value("verify@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_user_attribute_verification_code()
        .access_token(&access_token)
        .attribute_name("email")
        .send()
        .await
        .unwrap();

    // Get code via introspection
    let code_resp: serde_json::Value = reqwest::get(format!(
        "{}/_fakecloud/cognito/confirmation-codes/{}/selfuser",
        server.endpoint(),
        pool_id
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    let code = code_resp["attributeVerificationCodes"]["email"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .verify_user_attribute()
        .access_token(&access_token)
        .attribute_name("email")
        .code(&code)
        .send()
        .await
        .unwrap();

    let user = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();
    assert!(user
        .user_attributes()
        .iter()
        .any(|a| a.name() == "email_verified" && a.value() == Some("true")));
}

#[test_action("cognito-idp", "ResendConfirmationCode", checksum = "7cece340")]
#[tokio::test]
async fn cognito_resend_confirmation_code() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("resend-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("resend-client")
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("resenduser")
        .password("Resend1234!")
        .send()
        .await
        .unwrap();

    let resp = client
        .resend_confirmation_code()
        .client_id(&client_id)
        .username("resenduser")
        .send()
        .await
        .unwrap();
    assert!(resp.code_delivery_details().is_some());
}

// ---------------------------------------------------------------------------
// MFA / Software Tokens
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "SetUserPoolMfaConfig", checksum = "590320fb")]
#[test_action("cognito-idp", "GetUserPoolMfaConfig", checksum = "de56204f")]
#[tokio::test]
async fn cognito_set_get_user_pool_mfa_config() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("mfa-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .set_user_pool_mfa_config()
        .user_pool_id(&pool_id)
        .mfa_configuration(aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::Optional)
        .software_token_mfa_configuration(
            aws_sdk_cognitoidentityprovider::types::SoftwareTokenMfaConfigType::builder()
                .enabled(true)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_user_pool_mfa_config()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.mfa_configuration(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::Optional),
    );
}

#[test_action("cognito-idp", "AdminSetUserMFAPreference", checksum = "e45639ae")]
#[tokio::test]
async fn cognito_admin_set_user_mfa_preference() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("mfapref-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("mfauser")
        .send()
        .await
        .unwrap();

    client
        .admin_set_user_mfa_preference()
        .user_pool_id(&pool_id)
        .username("mfauser")
        .software_token_mfa_settings(
            aws_sdk_cognitoidentityprovider::types::SoftwareTokenMfaSettingsType::builder()
                .enabled(true)
                .preferred_mfa(true)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("cognito-idp", "SetUserMFAPreference", checksum = "c0f29c1b")]
#[tokio::test]
async fn cognito_set_user_mfa_preference() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, _client_id, access_token) =
        setup_authenticated_user(&client, "setmfapref-pool").await;

    client
        .set_user_mfa_preference()
        .access_token(&access_token)
        .software_token_mfa_settings(
            aws_sdk_cognitoidentityprovider::types::SoftwareTokenMfaSettingsType::builder()
                .enabled(true)
                .preferred_mfa(false)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("cognito-idp", "AssociateSoftwareToken", checksum = "d4a0b55a")]
#[test_action("cognito-idp", "VerifySoftwareToken", checksum = "a56ac88e")]
#[tokio::test]
async fn cognito_associate_verify_software_token() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, _client_id, access_token) = setup_authenticated_user(&client, "totp-pool").await;

    let resp = client
        .associate_software_token()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();
    let secret = resp.secret_code().unwrap();
    assert_eq!(secret.len(), 32);

    let verify = client
        .verify_software_token()
        .access_token(&access_token)
        .user_code("123456")
        .send()
        .await
        .unwrap();
    assert_eq!(
        verify.status(),
        Some(&aws_sdk_cognitoidentityprovider::types::VerifySoftwareTokenResponseType::Success),
    );
}
