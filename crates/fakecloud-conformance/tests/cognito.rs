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

// ---------------------------------------------------------------------------
// Identity Providers
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "CreateIdentityProvider", checksum = "59c9c181")]
#[tokio::test]
async fn cognito_create_identity_provider() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("idp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .create_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("TestOIDC")
        .provider_type(aws_sdk_cognitoidentityprovider::types::IdentityProviderTypeType::Oidc)
        .provider_details("client_id", "test-client")
        .provider_details("authorize_scopes", "openid")
        .send()
        .await
        .unwrap();
    let idp = resp.identity_provider().unwrap();
    assert_eq!(idp.provider_name().unwrap(), "TestOIDC");
}

#[test_action("cognito-idp", "DescribeIdentityProvider", checksum = "94a4d7f7")]
#[tokio::test]
async fn cognito_describe_identity_provider() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("descidp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("DescOIDC")
        .provider_type(aws_sdk_cognitoidentityprovider::types::IdentityProviderTypeType::Oidc)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("DescOIDC")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.identity_provider().unwrap().provider_name().unwrap(),
        "DescOIDC"
    );
}

#[test_action("cognito-idp", "UpdateIdentityProvider", checksum = "7addfa0c")]
#[tokio::test]
async fn cognito_update_identity_provider() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("updidp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("UpdOIDC")
        .provider_type(aws_sdk_cognitoidentityprovider::types::IdentityProviderTypeType::Oidc)
        .send()
        .await
        .unwrap();

    client
        .update_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("UpdOIDC")
        .provider_details("client_id", "updated-client")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("UpdOIDC")
        .send()
        .await
        .unwrap();
    let idp = resp.identity_provider().unwrap();
    assert_eq!(
        idp.provider_details()
            .unwrap()
            .get("client_id")
            .map(|v| v.as_str()),
        Some("updated-client")
    );
}

#[test_action("cognito-idp", "DeleteIdentityProvider", checksum = "9d54dc37")]
#[tokio::test]
async fn cognito_delete_identity_provider() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("delidp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("DelOIDC")
        .provider_type(aws_sdk_cognitoidentityprovider::types::IdentityProviderTypeType::Oidc)
        .send()
        .await
        .unwrap();

    client
        .delete_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("DelOIDC")
        .send()
        .await
        .unwrap();

    let err = client
        .describe_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("DelOIDC")
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_resource_not_found_exception());
}

#[test_action("cognito-idp", "ListIdentityProviders", checksum = "b9551ba1")]
#[tokio::test]
async fn cognito_list_identity_providers() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("listidp-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("IDP1")
        .provider_type(aws_sdk_cognitoidentityprovider::types::IdentityProviderTypeType::Oidc)
        .send()
        .await
        .unwrap();
    client
        .create_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("IDP2")
        .provider_type(aws_sdk_cognitoidentityprovider::types::IdentityProviderTypeType::Saml)
        .send()
        .await
        .unwrap();

    let resp = client
        .list_identity_providers()
        .user_pool_id(&pool_id)
        .max_results(10)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.providers().len(), 2);
}

// ---------------------------------------------------------------------------
// Resource Servers & Domains
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "CreateResourceServer", checksum = "b97cc403")]
#[test_action("cognito-idp", "DescribeResourceServer", checksum = "67f1b947")]
#[tokio::test]
async fn cognito_create_describe_resource_server() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("rs-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .create_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://api.example.com")
        .name("Example API")
        .scopes(
            aws_sdk_cognitoidentityprovider::types::ResourceServerScopeType::builder()
                .scope_name("read")
                .scope_description("Read access")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.resource_server().unwrap().name().unwrap(),
        "Example API"
    );

    let desc = client
        .describe_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://api.example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.resource_server().unwrap().identifier().unwrap(),
        "https://api.example.com"
    );
}

#[test_action("cognito-idp", "UpdateResourceServer", checksum = "5e9ce1ee")]
#[test_action("cognito-idp", "DeleteResourceServer", checksum = "ad92e082")]
#[tokio::test]
async fn cognito_update_delete_resource_server() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("updrs-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://api2.example.com")
        .name("API 2")
        .send()
        .await
        .unwrap();

    client
        .update_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://api2.example.com")
        .name("Updated API 2")
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://api2.example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.resource_server().unwrap().name().unwrap(),
        "Updated API 2"
    );

    client
        .delete_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://api2.example.com")
        .send()
        .await
        .unwrap();

    let err = client
        .describe_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://api2.example.com")
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_resource_not_found_exception());
}

#[test_action("cognito-idp", "ListResourceServers", checksum = "5c4ebddb")]
#[tokio::test]
async fn cognito_list_resource_servers() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("listrs-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://rs1.example.com")
        .name("RS1")
        .send()
        .await
        .unwrap();
    client
        .create_resource_server()
        .user_pool_id(&pool_id)
        .identifier("https://rs2.example.com")
        .name("RS2")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_resource_servers()
        .user_pool_id(&pool_id)
        .max_results(10)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.resource_servers().len(), 2);
}

#[test_action("cognito-idp", "CreateUserPoolDomain", checksum = "28b91b3c")]
#[test_action("cognito-idp", "DescribeUserPoolDomain", checksum = "6ecd5522")]
#[tokio::test]
async fn cognito_create_describe_domain() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("domain-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_user_pool_domain()
        .user_pool_id(&pool_id)
        .domain("my-test-domain")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_user_pool_domain()
        .domain("my-test-domain")
        .send()
        .await
        .unwrap();
    let desc = resp.domain_description().unwrap();
    assert_eq!(desc.domain().unwrap(), "my-test-domain");
    assert_eq!(desc.user_pool_id().unwrap(), pool_id);
}

#[test_action("cognito-idp", "UpdateUserPoolDomain", checksum = "03177020")]
#[test_action("cognito-idp", "DeleteUserPoolDomain", checksum = "f25ae5ad")]
#[tokio::test]
async fn cognito_update_delete_domain() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("deldomain-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_user_pool_domain()
        .user_pool_id(&pool_id)
        .domain("del-test-domain")
        .send()
        .await
        .unwrap();

    client
        .delete_user_pool_domain()
        .user_pool_id(&pool_id)
        .domain("del-test-domain")
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_user_pool_domain()
        .domain("del-test-domain")
        .send()
        .await
        .unwrap();
    // AWS returns empty description for non-existent domains
    assert!(resp.domain_description().unwrap().user_pool_id().is_none());
}

// ---------------------------------------------------------------------------
// Device Management
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "ConfirmDevice", checksum = "d2285f4d")]
#[test_action("cognito-idp", "AdminGetDevice", checksum = "b7ff3b4f")]
#[test_action("cognito-idp", "AdminListDevices", checksum = "52c1799f")]
#[tokio::test]
async fn cognito_device_lifecycle() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (pool_id, _client_id, access_token) =
        setup_authenticated_user(&client, "device-pool").await;

    client
        .confirm_device()
        .access_token(&access_token)
        .device_key("device-key-1")
        .send()
        .await
        .unwrap();

    let dev = client
        .admin_get_device()
        .user_pool_id(&pool_id)
        .username("selfuser")
        .device_key("device-key-1")
        .send()
        .await
        .unwrap();
    assert_eq!(dev.device().unwrap().device_key().unwrap(), "device-key-1");

    let list = client
        .admin_list_devices()
        .user_pool_id(&pool_id)
        .username("selfuser")
        .send()
        .await
        .unwrap();
    assert_eq!(list.devices().len(), 1);
}

#[test_action("cognito-idp", "AdminUpdateDeviceStatus", checksum = "4c7d9838")]
#[test_action("cognito-idp", "AdminForgetDevice", checksum = "3383fe72")]
#[tokio::test]
async fn cognito_admin_update_forget_device() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (pool_id, _client_id, access_token) =
        setup_authenticated_user(&client, "devupd-pool").await;

    client
        .confirm_device()
        .access_token(&access_token)
        .device_key("dev-2")
        .send()
        .await
        .unwrap();

    client
        .admin_update_device_status()
        .user_pool_id(&pool_id)
        .username("selfuser")
        .device_key("dev-2")
        .device_remembered_status(
            aws_sdk_cognitoidentityprovider::types::DeviceRememberedStatusType::Remembered,
        )
        .send()
        .await
        .unwrap();

    client
        .admin_forget_device()
        .user_pool_id(&pool_id)
        .username("selfuser")
        .device_key("dev-2")
        .send()
        .await
        .unwrap();

    let list = client
        .admin_list_devices()
        .user_pool_id(&pool_id)
        .username("selfuser")
        .send()
        .await
        .unwrap();
    assert!(list.devices().is_empty());
}

// ---------------------------------------------------------------------------
// User-facing Device Operations
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "ForgetDevice", checksum = "b406e013")]
#[test_action("cognito-idp", "GetDevice", checksum = "29875916")]
#[test_action("cognito-idp", "ListDevices", checksum = "c3f85481")]
#[test_action("cognito-idp", "UpdateDeviceStatus", checksum = "830e0020")]
#[tokio::test]
async fn cognito_user_device_ops() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, _client_id, access_token) = setup_authenticated_user(&client, "udev-pool").await;

    // Confirm a device
    client
        .confirm_device()
        .access_token(&access_token)
        .device_key("user-dev-1")
        .send()
        .await
        .unwrap();

    // GetDevice
    let dev = client
        .get_device()
        .access_token(&access_token)
        .device_key("user-dev-1")
        .send()
        .await
        .unwrap();
    assert_eq!(dev.device().unwrap().device_key().unwrap(), "user-dev-1");

    // ListDevices
    let list = client
        .list_devices()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();
    assert_eq!(list.devices().len(), 1);

    // UpdateDeviceStatus
    client
        .update_device_status()
        .access_token(&access_token)
        .device_key("user-dev-1")
        .device_remembered_status(
            aws_sdk_cognitoidentityprovider::types::DeviceRememberedStatusType::Remembered,
        )
        .send()
        .await
        .unwrap();

    // ForgetDevice
    client
        .forget_device()
        .access_token(&access_token)
        .device_key("user-dev-1")
        .send()
        .await
        .unwrap();

    let list = client
        .list_devices()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();
    assert!(list.devices().is_empty());
}

// ---------------------------------------------------------------------------
// Token Operations
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "RevokeToken", checksum = "8874ade0")]
#[tokio::test]
async fn cognito_revoke_token() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, client_id, _access_token) =
        setup_authenticated_user(&client, "revoke-pool").await;

    // Sign in to get a refresh token
    let auth = client
        .initiate_auth()
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .client_id(&client_id)
        .auth_parameters("USERNAME", "selfuser")
        .auth_parameters("PASSWORD", "SelfPass1!")
        .send()
        .await
        .unwrap();

    let refresh_token = auth
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap();

    // Revoke it
    client
        .revoke_token()
        .token(refresh_token)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
}

#[test_action("cognito-idp", "GetTokensFromRefreshToken", checksum = "667ba23d")]
#[tokio::test]
async fn cognito_get_tokens_from_refresh_token() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, client_id, _access_token) =
        setup_authenticated_user(&client, "refresh-pool").await;

    // Sign in to get a refresh token
    let auth = client
        .initiate_auth()
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .client_id(&client_id)
        .auth_parameters("USERNAME", "selfuser")
        .auth_parameters("PASSWORD", "SelfPass1!")
        .send()
        .await
        .unwrap();

    let refresh_token = auth
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap();

    // Exchange refresh token for new tokens
    let result = client
        .get_tokens_from_refresh_token()
        .refresh_token(refresh_token)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();

    assert!(result
        .authentication_result()
        .unwrap()
        .access_token()
        .is_some());
    assert!(result.authentication_result().unwrap().id_token().is_some());
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "TagResource", checksum = "b19b19ae")]
#[test_action("cognito-idp", "UntagResource", checksum = "3bd5fe69")]
#[test_action("cognito-idp", "ListTagsForResource", checksum = "a72e0056")]
#[tokio::test]
async fn cognito_tag_untag_list() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("tag-pool")
        .send()
        .await
        .unwrap();
    let pool_arn = pool.user_pool().unwrap().arn().unwrap().to_string();

    client
        .tag_resource()
        .resource_arn(&pool_arn)
        .tags("env", "test")
        .tags("team", "platform")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_resource()
        .resource_arn(&pool_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().unwrap().get("env"), Some(&"test".to_string()));

    client
        .untag_resource()
        .resource_arn(&pool_arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags2 = client
        .list_tags_for_resource()
        .resource_arn(&pool_arn)
        .send()
        .await
        .unwrap();
    assert!(tags2.tags().unwrap().get("env").is_none());
    assert_eq!(
        tags2.tags().unwrap().get("team"),
        Some(&"platform".to_string())
    );
}

// ---------------------------------------------------------------------------
// Import Jobs
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "CreateUserImportJob", checksum = "6cf3fba2")]
#[test_action("cognito-idp", "DescribeUserImportJob", checksum = "1c8e4fe5")]
#[test_action("cognito-idp", "ListUserImportJobs", checksum = "f4ef28a5")]
#[tokio::test]
async fn cognito_import_jobs() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("import-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .create_user_import_job()
        .user_pool_id(&pool_id)
        .job_name("test-import")
        .cloud_watch_logs_role_arn("arn:aws:iam::123456789012:role/CognitoCloudWatchRole")
        .send()
        .await
        .unwrap();
    let job = resp.user_import_job().unwrap();
    assert_eq!(job.job_name().unwrap(), "test-import");
    let job_id = job.job_id().unwrap().to_string();

    let desc = client
        .describe_user_import_job()
        .user_pool_id(&pool_id)
        .job_id(&job_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.user_import_job().unwrap().job_name().unwrap(),
        "test-import"
    );

    let list = client
        .list_user_import_jobs()
        .user_pool_id(&pool_id)
        .max_results(10)
        .send()
        .await
        .unwrap();
    assert_eq!(list.user_import_jobs().len(), 1);
}

#[test_action("cognito-idp", "GetCSVHeader", checksum = "c4b2b3d1")]
#[tokio::test]
async fn cognito_get_csv_header() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("csv-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .get_csv_header()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.user_pool_id().unwrap(), pool_id);
    assert!(!resp.csv_header().is_empty());
}

// ---------------------------------------------------------------------------
// UI Customization
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "SetUICustomization", checksum = "27bc4b26")]
#[test_action("cognito-idp", "GetUICustomization", checksum = "807d92dc")]
#[tokio::test]
async fn cognito_ui_customization() {
// Custom Attributes
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "AddCustomAttributes", checksum = "01878c7f")]
#[tokio::test]
async fn cognito_add_custom_attributes() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("ui-custom-pool")
.pool_name("custom-attr-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .set_ui_customization()
        .user_pool_id(&pool_id)
        .css("body { background: red; }")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_ui_customization()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert!(resp.ui_customization().is_some());
}

// ---------------------------------------------------------------------------
// Log Delivery Configuration
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "SetLogDeliveryConfiguration", checksum = "8a6a037d")]
#[test_action("cognito-idp", "GetLogDeliveryConfiguration", checksum = "37aab735")]
#[tokio::test]
async fn cognito_log_delivery_config() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("log-config-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .set_log_delivery_configuration()
        .user_pool_id(&pool_id)
        .log_configurations(
            aws_sdk_cognitoidentityprovider::types::LogConfigurationType::builder()
                .log_level(aws_sdk_cognitoidentityprovider::types::LogLevel::Error)
                .event_source(
                    aws_sdk_cognitoidentityprovider::types::EventSourceName::UserNotification,
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_log_delivery_configuration()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert!(resp.log_delivery_configuration().is_some());
}

// ---------------------------------------------------------------------------
// Risk Configuration
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "SetRiskConfiguration", checksum = "f74ed3fe")]
#[test_action("cognito-idp", "DescribeRiskConfiguration", checksum = "da8ca179")]
#[tokio::test]
async fn cognito_risk_configuration() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("risk-config-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .set_risk_configuration()
        .user_pool_id(&pool_id)
        .risk_exception_configuration(
            aws_sdk_cognitoidentityprovider::types::RiskExceptionConfigurationType::builder()
                .blocked_ip_range_list("192.168.1.0/24")
.add_custom_attributes()
        .user_pool_id(&pool_id)
        .custom_attributes(
            aws_sdk_cognitoidentityprovider::types::SchemaAttributeType::builder()
                .name("custom:favorite_color")
                .attribute_data_type(
                    aws_sdk_cognitoidentityprovider::types::AttributeDataType::String,
                )
                .mutable(true)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_risk_configuration()
let desc = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert!(resp.risk_configuration().is_some());
let attrs = desc.user_pool().unwrap().schema_attributes();
    assert!(attrs
        .iter()
        .any(|a| a.name() == Some("custom:favorite_color")));
}

// ---------------------------------------------------------------------------
// Client Secrets
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "AddUserPoolClientSecret", checksum = "12841f07")]
#[test_action("cognito-idp", "DeleteUserPoolClientSecret", checksum = "37b816e9")]
#[test_action("cognito-idp", "ListUserPoolClientSecrets", checksum = "ef0fe44f")]
#[tokio::test]
async fn cognito_client_secrets() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("secrets-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("secrets-client")
        .send()
        .await
        .unwrap();
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Add a secret
    let added = client
        .add_user_pool_client_secret()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
    let secret = added.client_secret_descriptor().unwrap();
    let secret_id = secret.client_secret_id().unwrap().to_string();

    // List secrets
    let list = client
        .list_user_pool_client_secrets()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
    assert_eq!(list.client_secrets().len(), 1);

    // Delete secret
    client
        .delete_user_pool_client_secret()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .client_secret_id(&secret_id)
        .send()
        .await
        .unwrap();

    let list = client
        .list_user_pool_client_secrets()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .unwrap();
    assert!(list.client_secrets().is_empty());
}

// ---------------------------------------------------------------------------
// GetIdentityProviderByIdentifier
// ---------------------------------------------------------------------------

#[test_action(
    "cognito-idp",
    "GetIdentityProviderByIdentifier",
    checksum = "02bc980a"
)]
#[tokio::test]
async fn cognito_get_idp_by_identifier() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("idp-identifier-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_identity_provider()
        .user_pool_id(&pool_id)
        .provider_name("MyOIDC")
        .provider_type(aws_sdk_cognitoidentityprovider::types::IdentityProviderTypeType::Oidc)
        .provider_details("client_id", "test-client")
        .provider_details("client_secret", "test-secret")
        .provider_details("authorize_scopes", "openid")
        .provider_details("oidc_issuer", "https://example.com")
        .provider_details("attributes_request_method", "GET")
        .idp_identifiers("my-oidc-id")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_identity_provider_by_identifier()
        .user_pool_id(&pool_id)
        .idp_identifier("my-oidc-id")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.identity_provider().unwrap().provider_name().unwrap(),
        "MyOIDC"
    );
}

// ---------------------------------------------------------------------------
// Import Job State Transitions
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "StartUserImportJob", checksum = "3f2ac87b")]
#[test_action("cognito-idp", "StopUserImportJob", checksum = "07546a5b")]
#[tokio::test]
async fn cognito_start_stop_import_job() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("job-state-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let job = client
        .create_user_import_job()
        .user_pool_id(&pool_id)
        .job_name("state-test")
        .cloud_watch_logs_role_arn("arn:aws:iam::123456789012:role/CognitoRole")
        .send()
        .await
        .unwrap();
    let job_id = job.user_import_job().unwrap().job_id().unwrap().to_string();

    // Start
    let started = client
        .start_user_import_job()
        .user_pool_id(&pool_id)
        .job_id(&job_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        started
            .user_import_job()
            .unwrap()
            .status()
            .unwrap()
            .as_str(),
        "InProgress"
    );

    // Stop
    let stopped = client
        .stop_user_import_job()
        .user_pool_id(&pool_id)
        .job_id(&job_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        stopped
            .user_import_job()
            .unwrap()
            .status()
            .unwrap()
            .as_str(),
        "Stopped"
    );
}

// ---------------------------------------------------------------------------
// GetUserAuthFactors
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "GetUserAuthFactors", checksum = "958af3b1")]
#[tokio::test]
async fn cognito_get_user_auth_factors() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let (_pool_id, _client_id, access_token) =
        setup_authenticated_user(&client, "authfactors-pool").await;

    let resp = client
        .get_user_auth_factors()
        .access_token(&access_token)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.username(), "selfuser");
    assert!(!resp.configured_user_auth_factors().is_empty());
}

// ---------------------------------------------------------------------------
// GetSigningCertificate
// ---------------------------------------------------------------------------

#[test_action("cognito-idp", "GetSigningCertificate", checksum = "03a117ae")]
#[tokio::test]
async fn cognito_get_signing_certificate() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("cert-pool")
        .send()
        .await
        .unwrap();
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let resp = client
        .get_signing_certificate()
        .user_pool_id(&pool_id)
        .send()
        .await
        .unwrap();
    assert!(resp.certificate().is_some());
}
