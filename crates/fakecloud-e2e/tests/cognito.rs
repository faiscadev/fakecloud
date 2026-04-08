mod helpers;
use helpers::TestServer;

use aws_sdk_cognitoidentityprovider::types::{
    AccountRecoverySettingType, AttributeType, ChallengeNameType, DeliveryMediumType,
    DomainStatusType, ExplicitAuthFlowsType, IdentityProviderTypeType, PasswordPolicyType,
    RecoveryOptionNameType, RecoveryOptionType, ResourceServerScopeType, SmsMfaSettingsType,
    SoftwareTokenMfaConfigType, SoftwareTokenMfaSettingsType, UserPoolMfaType, UserPoolPolicyType,
    UserStatusType,
};

#[tokio::test]
async fn cognito_create_describe_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let result = client
        .create_user_pool()
        .pool_name("test-pool")
        .send()
        .await
        .expect("create user pool");

    let pool = result.user_pool().unwrap();
    let id = pool.id().unwrap();
    let name = pool.name().unwrap();

    assert_eq!(name, "test-pool");
    // ID format: region_XXXXXXXXX
    assert!(
        id.starts_with("us-east-1_"),
        "Pool ID should start with region prefix: {id}"
    );
    let suffix = id.strip_prefix("us-east-1_").unwrap();
    assert_eq!(suffix.len(), 9, "Pool ID suffix should be 9 chars: {id}");

    // ARN format
    let arn = pool.arn().unwrap();
    assert!(
        arn.contains("cognito-idp"),
        "ARN should contain cognito-idp: {arn}"
    );
    assert!(arn.contains(id), "ARN should contain pool ID: {arn}");

    // Describe the pool
    let describe = client
        .describe_user_pool()
        .user_pool_id(id)
        .send()
        .await
        .expect("describe user pool");

    let described = describe.user_pool().unwrap();
    assert_eq!(described.name().unwrap(), "test-pool");
    assert_eq!(described.id().unwrap(), id);
    assert_eq!(described.arn().unwrap(), arn);

    // Check default password policy
    let policies = described.policies().unwrap();
    let pp = policies.password_policy().unwrap();
    assert_eq!(pp.minimum_length(), Some(8));
    assert!(pp.require_uppercase());
    assert!(pp.require_lowercase());
    assert!(pp.require_numbers());
    assert!(pp.require_symbols());
    assert_eq!(pp.temporary_password_validity_days(), 7);

    // Check schema attributes contain defaults
    let schema = described.schema_attributes();
    let names: Vec<&str> = schema.iter().filter_map(|a| a.name()).collect();
    assert!(names.contains(&"sub"), "Schema should contain 'sub'");
    assert!(names.contains(&"email"), "Schema should contain 'email'");
    assert!(
        names.contains(&"phone_number"),
        "Schema should contain 'phone_number'"
    );
}

#[tokio::test]
async fn cognito_list_user_pools() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create 3 pools
    for i in 0..3 {
        client
            .create_user_pool()
            .pool_name(format!("pool-{i}"))
            .send()
            .await
            .expect("create user pool");
    }

    // List with MaxResults=2
    let result = client
        .list_user_pools()
        .max_results(2)
        .send()
        .await
        .expect("list user pools");

    let pools = result.user_pools();
    assert_eq!(pools.len(), 2, "Should return 2 pools");

    let next_token = result.next_token().expect("Should have NextToken");

    // Fetch next page
    let result2 = client
        .list_user_pools()
        .max_results(2)
        .next_token(next_token)
        .send()
        .await
        .expect("list user pools page 2");

    let pools2 = result2.user_pools();
    assert_eq!(pools2.len(), 1, "Should return 1 remaining pool");
    assert!(
        result2.next_token().is_none(),
        "Should not have NextToken on last page"
    );
}

#[tokio::test]
async fn cognito_update_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let result = client
        .create_user_pool()
        .pool_name("update-test")
        .send()
        .await
        .expect("create user pool");

    let pool_id = result.user_pool().unwrap().id().unwrap().to_string();

    // Update password policy
    client
        .update_user_pool()
        .user_pool_id(&pool_id)
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(12)
                        .require_uppercase(false)
                        .require_lowercase(true)
                        .require_numbers(true)
                        .require_symbols(false)
                        .temporary_password_validity_days(3)
                        .build(),
                )
                .build(),
        )
        .mfa_configuration(aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::Optional)
        .send()
        .await
        .expect("update user pool");

    // Verify the update
    let describe = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("describe user pool");

    let pool = describe.user_pool().unwrap();
    let pp = pool.policies().unwrap().password_policy().unwrap();
    assert_eq!(pp.minimum_length(), Some(12));
    assert!(!pp.require_uppercase());
    assert!(pp.require_lowercase());
    assert!(pp.require_numbers());
    assert!(!pp.require_symbols());
    assert_eq!(pp.temporary_password_validity_days(), 3);

    assert_eq!(
        pool.mfa_configuration(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::Optional),
    );
}

#[tokio::test]
async fn cognito_delete_user_pool() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let result = client
        .create_user_pool()
        .pool_name("delete-test")
        .send()
        .await
        .expect("create user pool");

    let pool_id = result.user_pool().unwrap().id().unwrap().to_string();

    // Delete it
    client
        .delete_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("delete user pool");

    // Verify it's gone
    let err = client
        .describe_user_pool()
        .user_pool_id(&pool_id)
        .send()
        .await;

    assert!(err.is_err(), "Describe should fail after delete");
}

#[tokio::test]
async fn cognito_create_user_pool_with_config() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let result = client
        .create_user_pool()
        .pool_name("configured-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(10)
                        .require_uppercase(true)
                        .require_lowercase(true)
                        .require_numbers(false)
                        .require_symbols(false)
                        .temporary_password_validity_days(14)
                        .build(),
                )
                .build(),
        )
        .auto_verified_attributes(
            aws_sdk_cognitoidentityprovider::types::VerifiedAttributeType::Email,
        )
        .mfa_configuration(aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::Optional)
        .account_recovery_setting(
            AccountRecoverySettingType::builder()
                .recovery_mechanisms(
                    RecoveryOptionType::builder()
                        .name(RecoveryOptionNameType::VerifiedEmail)
                        .priority(1)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create user pool with config");

    let pool = result.user_pool().unwrap();

    // Verify password policy
    let pp = pool.policies().unwrap().password_policy().unwrap();
    assert_eq!(pp.minimum_length(), Some(10));
    assert!(pp.require_uppercase());
    assert!(pp.require_lowercase());
    assert!(!pp.require_numbers());
    assert!(!pp.require_symbols());
    assert_eq!(pp.temporary_password_validity_days(), 14);

    // Verify auto verified attributes
    let auto_verified = pool.auto_verified_attributes();
    assert!(
        auto_verified
            .contains(&aws_sdk_cognitoidentityprovider::types::VerifiedAttributeType::Email),
        "Should have email in auto verified attributes"
    );

    // Verify MFA
    assert_eq!(
        pool.mfa_configuration(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserPoolMfaType::Optional),
    );

    // Verify account recovery
    let ars = pool.account_recovery_setting().unwrap();
    let mechanisms = ars.recovery_mechanisms();
    assert_eq!(mechanisms.len(), 1);
    assert_eq!(*mechanisms[0].name(), RecoveryOptionNameType::VerifiedEmail);
    assert_eq!(mechanisms[0].priority(), 1);
}

#[tokio::test]
async fn cognito_create_describe_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create a pool first
    let pool_result = client
        .create_user_pool()
        .pool_name("client-test-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create a client
    let result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("my-app-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create user pool client");

    let app_client = result.user_pool_client().unwrap();
    let client_id = app_client.client_id().unwrap();
    let client_name = app_client.client_name().unwrap();

    assert_eq!(client_name, "my-app-client");
    assert_eq!(
        client_id.len(),
        26,
        "Client ID should be 26 chars: {client_id}"
    );
    assert!(
        client_id.chars().all(|c| c.is_ascii_alphanumeric()),
        "Client ID should be alphanumeric: {client_id}"
    );
    assert_eq!(app_client.user_pool_id().unwrap(), pool_id);
    assert!(app_client.client_secret().is_none());

    let auth_flows = app_client.explicit_auth_flows();
    assert!(auth_flows.contains(&ExplicitAuthFlowsType::AllowUserPasswordAuth));
    assert!(auth_flows.contains(&ExplicitAuthFlowsType::AllowRefreshTokenAuth));

    // Describe the client
    let describe = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(client_id)
        .send()
        .await
        .expect("describe user pool client");

    let described = describe.user_pool_client().unwrap();
    assert_eq!(described.client_name().unwrap(), "my-app-client");
    assert_eq!(described.client_id().unwrap(), client_id);
    assert_eq!(described.user_pool_id().unwrap(), pool_id);
}

#[tokio::test]
async fn cognito_create_client_with_secret() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("secret-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("secret-client")
        .generate_secret(true)
        .send()
        .await
        .expect("create client with secret");

    let app_client = result.user_pool_client().unwrap();
    let secret = app_client
        .client_secret()
        .expect("Client secret should be present");
    assert_eq!(
        secret.len(),
        51,
        "Client secret should be 51 chars: {secret}"
    );

    // Describe should also return the secret
    let describe = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(app_client.client_id().unwrap())
        .send()
        .await
        .expect("describe client");
    assert_eq!(
        describe
            .user_pool_client()
            .unwrap()
            .client_secret()
            .unwrap(),
        secret
    );
}

#[tokio::test]
async fn cognito_list_user_pool_clients() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("list-clients-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create 3 clients
    for i in 0..3 {
        client
            .create_user_pool_client()
            .user_pool_id(&pool_id)
            .client_name(format!("client-{i}"))
            .send()
            .await
            .expect("create client");
    }

    // List with MaxResults=2
    let result = client
        .list_user_pool_clients()
        .user_pool_id(&pool_id)
        .max_results(2)
        .send()
        .await
        .expect("list user pool clients");

    let clients = result.user_pool_clients();
    assert_eq!(clients.len(), 2, "Should return 2 clients");
    let next_token = result.next_token().expect("Should have NextToken");

    // Fetch next page
    let result2 = client
        .list_user_pool_clients()
        .user_pool_id(&pool_id)
        .max_results(2)
        .next_token(next_token)
        .send()
        .await
        .expect("list clients page 2");

    let clients2 = result2.user_pool_clients();
    assert_eq!(clients2.len(), 1, "Should return 1 remaining client");
    assert!(
        result2.next_token().is_none(),
        "Should not have NextToken on last page"
    );
}

#[tokio::test]
async fn cognito_update_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("update-client-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let create_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("updatable-client")
        .send()
        .await
        .expect("create client");
    let client_id = create_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Update callback URLs and name
    let update_result = client
        .update_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .client_name("updated-client")
        .callback_urls("https://example.com/callback")
        .callback_urls("https://example.com/callback2")
        .logout_urls("https://example.com/logout")
        .send()
        .await
        .expect("update client");

    let updated = update_result.user_pool_client().unwrap();
    assert_eq!(updated.client_name().unwrap(), "updated-client");
    assert_eq!(updated.callback_urls().len(), 2);
    assert!(updated
        .callback_urls()
        .contains(&"https://example.com/callback".to_string()));
    assert!(updated
        .callback_urls()
        .contains(&"https://example.com/callback2".to_string()));
    assert_eq!(updated.logout_urls().len(), 1);
    assert!(updated
        .logout_urls()
        .contains(&"https://example.com/logout".to_string()));

    // Verify via describe
    let describe = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .expect("describe updated client");
    let described = describe.user_pool_client().unwrap();
    assert_eq!(described.client_name().unwrap(), "updated-client");
    assert_eq!(described.callback_urls().len(), 2);
}

#[tokio::test]
async fn cognito_delete_user_pool_client() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("delete-client-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let create_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("deletable-client")
        .send()
        .await
        .expect("create client");
    let client_id = create_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Delete it
    client
        .delete_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await
        .expect("delete client");

    // Verify it's gone
    let err = client
        .describe_user_pool_client()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .send()
        .await;
    assert!(err.is_err(), "Describe should fail after delete");
}

#[tokio::test]
async fn cognito_admin_create_get_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create a pool
    let pool_result = client
        .create_user_pool()
        .pool_name("user-mgmt-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Admin create user with email attribute
    let create_result = client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("testuser")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("test@example.com")
                .build()
                .unwrap(),
        )
        .temporary_password("TempP@ss1!")
        .send()
        .await
        .expect("admin create user");

    let user = create_result.user().unwrap();
    assert_eq!(user.username().unwrap(), "testuser");
    assert_eq!(
        user.user_status(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserStatusType::ForceChangePassword),
    );
    assert!(user.enabled(), "User should be enabled by default");

    // Verify sub is in attributes
    let attrs = user.attributes();
    let sub_attr = attrs.iter().find(|a| a.name() == "sub");
    assert!(sub_attr.is_some(), "User should have 'sub' attribute");
    let sub_value = sub_attr.unwrap().value().unwrap();
    assert!(!sub_value.is_empty(), "Sub should not be empty");

    // Verify email is in attributes
    let email_attr = attrs.iter().find(|a| a.name() == "email");
    assert!(email_attr.is_some(), "User should have 'email' attribute");
    assert_eq!(email_attr.unwrap().value().unwrap(), "test@example.com");

    // AdminGetUser
    let get_result = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("testuser")
        .send()
        .await
        .expect("admin get user");

    assert_eq!(get_result.username(), "testuser");
    assert_eq!(
        get_result.user_status(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserStatusType::ForceChangePassword),
    );
    assert!(get_result.enabled(), "User should be enabled");

    // Verify attributes from GetUser
    let get_attrs = get_result.user_attributes();
    let get_sub = get_attrs.iter().find(|a| a.name() == "sub");
    assert!(get_sub.is_some(), "GetUser should return sub attribute");
    assert_eq!(get_sub.unwrap().value().unwrap(), sub_value);
}

#[tokio::test]
async fn cognito_admin_disable_enable_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("disable-enable-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("toggleuser")
        .send()
        .await
        .expect("admin create user");

    // Disable user
    client
        .admin_disable_user()
        .user_pool_id(&pool_id)
        .username("toggleuser")
        .send()
        .await
        .expect("admin disable user");

    // Verify disabled
    let get = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("toggleuser")
        .send()
        .await
        .expect("get disabled user");
    assert!(!get.enabled(), "User should be disabled");

    // Enable user
    client
        .admin_enable_user()
        .user_pool_id(&pool_id)
        .username("toggleuser")
        .send()
        .await
        .expect("admin enable user");

    // Verify enabled
    let get = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("toggleuser")
        .send()
        .await
        .expect("get enabled user");
    assert!(get.enabled(), "User should be enabled");
}

#[tokio::test]
async fn cognito_admin_update_delete_user_attributes() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("attr-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create user with email
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("attruser")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("original@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("admin create user");

    // Update email
    client
        .admin_update_user_attributes()
        .user_pool_id(&pool_id)
        .username("attruser")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("updated@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("admin update user attributes");

    // Verify updated
    let get = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("attruser")
        .send()
        .await
        .expect("get user after update");
    let email_attr = get
        .user_attributes()
        .iter()
        .find(|a| a.name() == "email")
        .unwrap();
    assert_eq!(email_attr.value().unwrap(), "updated@example.com");

    // Delete email attribute
    client
        .admin_delete_user_attributes()
        .user_pool_id(&pool_id)
        .username("attruser")
        .user_attribute_names("email")
        .send()
        .await
        .expect("admin delete user attributes");

    // Verify deleted
    let get = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("attruser")
        .send()
        .await
        .expect("get user after delete attr");
    let email_attr = get.user_attributes().iter().find(|a| a.name() == "email");
    assert!(
        email_attr.is_none(),
        "Email attribute should be deleted: {:?}",
        get.user_attributes()
    );
}

#[tokio::test]
async fn cognito_admin_delete_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("delete-user-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("deleteuser")
        .send()
        .await
        .expect("admin create user");

    // Delete the user
    client
        .admin_delete_user()
        .user_pool_id(&pool_id)
        .username("deleteuser")
        .send()
        .await
        .expect("admin delete user");

    // Verify get returns UserNotFoundException
    let err = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("deleteuser")
        .send()
        .await;
    assert!(err.is_err(), "Get should fail after delete");
}

#[tokio::test]
async fn cognito_list_users() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("list-users-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create 3 users
    for i in 0..3 {
        client
            .admin_create_user()
            .user_pool_id(&pool_id)
            .username(format!("user{i}"))
            .send()
            .await
            .expect("admin create user");
    }

    // List all users
    let result = client
        .list_users()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("list users");

    let users = result.users();
    assert_eq!(users.len(), 3, "Should return 3 users");

    // Verify all users are present
    let usernames: Vec<&str> = users.iter().filter_map(|u| u.username()).collect();
    assert!(usernames.contains(&"user0"));
    assert!(usernames.contains(&"user1"));
    assert!(usernames.contains(&"user2"));
}

#[tokio::test]
async fn cognito_list_users_with_filter() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("filter-users-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create users with different emails
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
        .expect("create alice");

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("bob")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("bob@other.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create bob");

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("carol")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("carol@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create carol");

    // Filter by exact email
    let result = client
        .list_users()
        .user_pool_id(&pool_id)
        .filter(r#"email = "alice@example.com""#)
        .send()
        .await
        .expect("list users with filter");

    let users = result.users();
    assert_eq!(users.len(), 1, "Filter should match exactly one user");
    assert_eq!(users[0].username().unwrap(), "alice");

    // Filter by email prefix
    let result = client
        .list_users()
        .user_pool_id(&pool_id)
        .filter(r#"email ^= "carol""#)
        .send()
        .await
        .expect("list users with prefix filter");

    let users = result.users();
    assert_eq!(users.len(), 1, "Prefix filter should match one user");
    assert_eq!(users[0].username().unwrap(), "carol");
}

#[tokio::test]
async fn cognito_admin_set_user_password_and_auth() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool with relaxed password policy
    let pool_result = client
        .create_user_pool()
        .pool_name("auth-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create client with admin auth flows
    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("auth-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("authuser")
        .send()
        .await
        .expect("create user");

    // Set permanent password
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("authuser")
        .password("mypassword")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Verify user status is CONFIRMED
    let get = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("authuser")
        .send()
        .await
        .expect("get user");
    assert_eq!(
        get.user_status(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserStatusType::Confirmed),
    );

    // Admin initiate auth
    let auth_result = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "authuser")
        .auth_parameters("PASSWORD", "mypassword")
        .send()
        .await
        .expect("admin initiate auth");

    let auth = auth_result
        .authentication_result()
        .expect("should have auth result");
    assert!(auth.access_token().is_some(), "should have access token");
    assert!(auth.id_token().is_some(), "should have id token");
    assert!(auth.refresh_token().is_some(), "should have refresh token");
    assert_eq!(auth.token_type().unwrap(), "Bearer");

    // Verify JWT format: 3 dot-separated segments
    let id_token = auth.id_token().unwrap();
    let parts: Vec<&str> = id_token.split('.').collect();
    assert_eq!(parts.len(), 3, "ID token should have 3 segments");
}

#[tokio::test]
async fn cognito_force_change_password_flow() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool with relaxed policy
    let pool_result = client
        .create_user_pool()
        .pool_name("fcp-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("fcp-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user with temp password (FORCE_CHANGE_PASSWORD status)
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("fcpuser")
        .temporary_password("temppass")
        .send()
        .await
        .expect("create user");

    // Auth should return NEW_PASSWORD_REQUIRED challenge
    let auth_result = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "fcpuser")
        .auth_parameters("PASSWORD", "temppass")
        .send()
        .await
        .expect("admin initiate auth");

    assert_eq!(
        auth_result.challenge_name(),
        Some(&ChallengeNameType::NewPasswordRequired),
    );
    let session = auth_result.session().expect("should have session");

    // Respond to challenge with new password
    let respond_result = client
        .admin_respond_to_auth_challenge()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .challenge_name(ChallengeNameType::NewPasswordRequired)
        .challenge_responses("NEW_PASSWORD", "newpassword")
        .challenge_responses("USERNAME", "fcpuser")
        .session(session)
        .send()
        .await
        .expect("respond to challenge");

    let auth = respond_result
        .authentication_result()
        .expect("should have auth result");
    assert!(auth.access_token().is_some());
    assert!(auth.id_token().is_some());
    assert!(auth.refresh_token().is_some());

    // Verify user is now CONFIRMED
    let get = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("fcpuser")
        .send()
        .await
        .expect("get user");
    assert_eq!(
        get.user_status(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserStatusType::Confirmed),
    );
}

#[tokio::test]
async fn cognito_sign_up_and_confirm() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool with relaxed policy
    let pool_result = client
        .create_user_pool()
        .pool_name("signup-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("signup-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Sign up
    let signup_result = client
        .sign_up()
        .client_id(&client_id)
        .username("signupuser")
        .password("mypassword")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("signup@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("sign up");

    assert!(
        !signup_result.user_confirmed(),
        "User should not be confirmed yet"
    );
    assert!(!signup_result.user_sub().is_empty(), "Should have UserSub");

    // Confirm sign up
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("signupuser")
        .confirmation_code("123456")
        .send()
        .await
        .expect("confirm sign up");

    // Now auth should work
    let auth_result = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "signupuser")
        .auth_parameters("PASSWORD", "mypassword")
        .send()
        .await
        .expect("initiate auth after confirm");

    let auth = auth_result
        .authentication_result()
        .expect("should have auth result");
    assert!(auth.access_token().is_some());
    assert!(auth.id_token().is_some());
    assert!(auth.refresh_token().is_some());
}

#[tokio::test]
async fn cognito_admin_confirm_sign_up() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("admin-confirm-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("admin-confirm-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Sign up
    client
        .sign_up()
        .client_id(&client_id)
        .username("adminconfirm")
        .password("mypassword")
        .send()
        .await
        .expect("sign up");

    // Admin confirm
    client
        .admin_confirm_sign_up()
        .user_pool_id(&pool_id)
        .username("adminconfirm")
        .send()
        .await
        .expect("admin confirm sign up");

    // Verify status is CONFIRMED
    let get = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("adminconfirm")
        .send()
        .await
        .expect("get user");
    assert_eq!(
        get.user_status(),
        Some(&aws_sdk_cognitoidentityprovider::types::UserStatusType::Confirmed),
    );
}

#[tokio::test]
async fn cognito_refresh_token_flow() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("refresh-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("refresh-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create and confirm user via sign up
    client
        .sign_up()
        .client_id(&client_id)
        .username("refreshuser")
        .password("mypassword")
        .send()
        .await
        .expect("sign up");

    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("refreshuser")
        .confirmation_code("123456")
        .send()
        .await
        .expect("confirm sign up");

    // Initial auth
    let auth_result = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "refreshuser")
        .auth_parameters("PASSWORD", "mypassword")
        .send()
        .await
        .expect("initial auth");

    let refresh_token = auth_result
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap()
        .to_string();

    // Use refresh token to get new tokens
    let refresh_result = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::RefreshTokenAuth)
        .auth_parameters("REFRESH_TOKEN", &refresh_token)
        .send()
        .await
        .expect("refresh token auth");

    let new_auth = refresh_result
        .authentication_result()
        .expect("should have auth result from refresh");
    assert!(
        new_auth.access_token().is_some(),
        "should have new access token"
    );
    assert!(new_auth.id_token().is_some(), "should have new id token");
}

#[tokio::test]
async fn cognito_auth_wrong_password() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("wrongpw-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("wrongpw-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user with permanent password
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("wrongpwuser")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("wrongpwuser")
        .password("correctpassword")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Try auth with wrong password
    let err = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "wrongpwuser")
        .auth_parameters("PASSWORD", "wrongpassword")
        .send()
        .await;

    assert!(err.is_err(), "Auth with wrong password should fail");
    let err_str = format!("{:?}", err.unwrap_err());
    assert!(
        err_str.contains("NotAuthorizedException")
            || err_str.contains("Incorrect username or password"),
        "Error should be NotAuthorizedException: {err_str}"
    );
}

#[tokio::test]
async fn cognito_change_password() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool with relaxed policy
    let pool_result = client
        .create_user_pool()
        .pool_name("chpw-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create client
    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("chpw-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user and set password
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("chpwuser")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("chpwuser")
        .password("oldpass")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Auth to get access token
    let auth_result = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "chpwuser")
        .auth_parameters("PASSWORD", "oldpass")
        .send()
        .await
        .expect("auth");

    let access_token = auth_result
        .authentication_result()
        .unwrap()
        .access_token()
        .unwrap()
        .to_string();

    // Change password
    client
        .change_password()
        .access_token(&access_token)
        .previous_password("oldpass")
        .proposed_password("newpass")
        .send()
        .await
        .expect("change password");

    // Auth with new password should work
    let auth2 = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "chpwuser")
        .auth_parameters("PASSWORD", "newpass")
        .send()
        .await;
    assert!(auth2.is_ok(), "Auth with new password should work");

    // Auth with old password should fail
    let auth3 = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "chpwuser")
        .auth_parameters("PASSWORD", "oldpass")
        .send()
        .await;
    assert!(auth3.is_err(), "Auth with old password should fail");
}

#[tokio::test]
async fn cognito_forgot_password_flow() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool with relaxed policy
    let pool_result = client
        .create_user_pool()
        .pool_name("forgot-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create client
    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("forgot-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user with email
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("forgotuser")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("user@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("forgotuser")
        .password("oldpass")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Call ForgotPassword
    let forgot_result = client
        .forgot_password()
        .client_id(&client_id)
        .username("forgotuser")
        .send()
        .await
        .expect("forgot password");

    // Check CodeDeliveryDetails
    let delivery = forgot_result.code_delivery_details().unwrap();
    assert_eq!(delivery.delivery_medium().unwrap().as_str(), "EMAIL");
    assert_eq!(delivery.attribute_name().unwrap(), "email");
    let destination = delivery.destination().unwrap();
    assert!(
        destination.contains("***"),
        "Destination should be masked: {destination}"
    );

    // Get confirmation code from introspection endpoint
    let http_client = reqwest::Client::new();
    let code_resp = http_client
        .get(format!(
            "{}/_fakecloud/cognito/confirmation-codes/{}/forgotuser",
            server.endpoint(),
            pool_id
        ))
        .send()
        .await
        .expect("get confirmation code");
    let code_json: serde_json::Value = code_resp.json().await.unwrap();
    let code = code_json["confirmationCode"].as_str().unwrap().to_string();
    assert_eq!(code.len(), 6, "Code should be 6 digits");

    // Confirm forgot password with wrong code should fail
    let wrong_code = if code.starts_with('9') {
        "000001".to_string()
    } else {
        "999999".to_string()
    };
    let bad_confirm = client
        .confirm_forgot_password()
        .client_id(&client_id)
        .username("forgotuser")
        .confirmation_code(&wrong_code)
        .password("newpass")
        .send()
        .await;
    assert!(bad_confirm.is_err(), "Wrong code should fail");

    // Confirm forgot password with correct code
    client
        .confirm_forgot_password()
        .client_id(&client_id)
        .username("forgotuser")
        .confirmation_code(&code)
        .password("newpass")
        .send()
        .await
        .expect("confirm forgot password");

    // Auth with new password should work
    let auth = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "forgotuser")
        .auth_parameters("PASSWORD", "newpass")
        .send()
        .await;
    assert!(auth.is_ok(), "Auth with new password should work");

    // Auth with old password should fail
    let auth_old = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "forgotuser")
        .auth_parameters("PASSWORD", "oldpass")
        .send()
        .await;
    assert!(auth_old.is_err(), "Auth with old password should fail");
}

#[tokio::test]
async fn cognito_admin_reset_user_password() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool
    let pool_result = client
        .create_user_pool()
        .pool_name("reset-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create user and set password
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .password("mypass")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Verify status is CONFIRMED
    let get1 = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .send()
        .await
        .expect("get user");
    assert_eq!(get1.user_status(), Some(&UserStatusType::Confirmed));

    // Admin reset user password
    client
        .admin_reset_user_password()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .send()
        .await
        .expect("admin reset user password");

    // Verify status is RESET_REQUIRED
    let get2 = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("resetuser")
        .send()
        .await
        .expect("get user after reset");
    assert_eq!(get2.user_status(), Some(&UserStatusType::ResetRequired));
}

#[tokio::test]
async fn cognito_global_sign_out() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool with relaxed policy
    let pool_result = client
        .create_user_pool()
        .pool_name("signout-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create client with user password auth + refresh token flows
    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("signout-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user and set password
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("signoutuser")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("signoutuser")
        .password("mypass")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Auth to get tokens
    let auth_result = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "signoutuser")
        .auth_parameters("PASSWORD", "mypass")
        .send()
        .await
        .expect("auth");

    let auth = auth_result.authentication_result().unwrap();
    let access_token = auth.access_token().unwrap().to_string();
    let refresh_token = auth.refresh_token().unwrap().to_string();

    // Global sign out
    client
        .global_sign_out()
        .access_token(&access_token)
        .send()
        .await
        .expect("global sign out");

    // Refresh token should no longer work
    let refresh_err = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::RefreshTokenAuth)
        .auth_parameters("REFRESH_TOKEN", &refresh_token)
        .send()
        .await;
    assert!(
        refresh_err.is_err(),
        "Refresh token should be invalidated after sign out"
    );
}

#[tokio::test]
async fn cognito_admin_user_global_sign_out() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool with relaxed policy
    let pool_result = client
        .create_user_pool()
        .pool_name("admin-signout-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create client
    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("admin-signout-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user and set password
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("adminsignoutuser")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("adminsignoutuser")
        .password("mypass")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Auth to get tokens
    let auth_result = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "adminsignoutuser")
        .auth_parameters("PASSWORD", "mypass")
        .send()
        .await
        .expect("auth");

    let auth = auth_result.authentication_result().unwrap();
    let refresh_token = auth.refresh_token().unwrap().to_string();

    // Admin user global sign out
    client
        .admin_user_global_sign_out()
        .user_pool_id(&pool_id)
        .username("adminsignoutuser")
        .send()
        .await
        .expect("admin user global sign out");

    // Refresh token should no longer work
    let refresh_err = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::RefreshTokenAuth)
        .auth_parameters("REFRESH_TOKEN", &refresh_token)
        .send()
        .await;
    assert!(
        refresh_err.is_err(),
        "Refresh token should be invalidated after admin sign out"
    );
}

// ── Group management E2E tests ──────────────────────────────────────

#[tokio::test]
async fn cognito_create_get_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("group-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    // Create a group with all fields
    let result = client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("admins")
        .description("Admin group")
        .precedence(1)
        .role_arn("arn:aws:iam::123456789012:role/AdminRole")
        .send()
        .await
        .expect("create group");

    let group = result.group().unwrap();
    assert_eq!(group.group_name().unwrap(), "admins");
    assert_eq!(group.user_pool_id().unwrap(), pool_id);
    assert_eq!(group.description().unwrap(), "Admin group");
    assert_eq!(group.precedence(), Some(1));
    assert_eq!(
        group.role_arn().unwrap(),
        "arn:aws:iam::123456789012:role/AdminRole"
    );

    // Get the group
    let get_result = client
        .get_group()
        .user_pool_id(&pool_id)
        .group_name("admins")
        .send()
        .await
        .expect("get group");

    let got = get_result.group().unwrap();
    assert_eq!(got.group_name().unwrap(), "admins");
    assert_eq!(got.description().unwrap(), "Admin group");
    assert_eq!(got.precedence(), Some(1));

    // Get non-existent group should fail
    let err = client
        .get_group()
        .user_pool_id(&pool_id)
        .group_name("nonexistent")
        .send()
        .await;
    assert!(err.is_err(), "Getting non-existent group should fail");

    // Creating duplicate group should fail with GroupExistsException
    let dup_err = client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("admins")
        .send()
        .await;
    assert!(dup_err.is_err(), "Duplicate group should fail");
}

#[tokio::test]
async fn cognito_update_delete_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("upd-group-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("editors")
        .description("Editor group")
        .precedence(5)
        .send()
        .await
        .expect("create group");

    // Update the group
    let updated = client
        .update_group()
        .user_pool_id(&pool_id)
        .group_name("editors")
        .description("Updated editors")
        .precedence(10)
        .role_arn("arn:aws:iam::123456789012:role/EditorRole")
        .send()
        .await
        .expect("update group");

    let g = updated.group().unwrap();
    assert_eq!(g.description().unwrap(), "Updated editors");
    assert_eq!(g.precedence(), Some(10));
    assert_eq!(
        g.role_arn().unwrap(),
        "arn:aws:iam::123456789012:role/EditorRole"
    );

    // Delete the group
    client
        .delete_group()
        .user_pool_id(&pool_id)
        .group_name("editors")
        .send()
        .await
        .expect("delete group");

    // Getting deleted group should fail
    let err = client
        .get_group()
        .user_pool_id(&pool_id)
        .group_name("editors")
        .send()
        .await;
    assert!(err.is_err(), "Deleted group should not be found");

    // Deleting again should fail
    let del_err = client
        .delete_group()
        .user_pool_id(&pool_id)
        .group_name("editors")
        .send()
        .await;
    assert!(del_err.is_err(), "Double delete should fail");
}

#[tokio::test]
async fn cognito_list_groups() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("list-groups-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    // Create several groups
    for name in &["alpha", "beta", "gamma", "delta"] {
        client
            .create_group()
            .user_pool_id(&pool_id)
            .group_name(*name)
            .send()
            .await
            .expect("create group");
    }

    // List all groups
    let list = client
        .list_groups()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("list groups");

    let groups = list.groups();
    assert_eq!(groups.len(), 4, "Should have 4 groups");

    // List with limit for pagination
    let page1 = client
        .list_groups()
        .user_pool_id(&pool_id)
        .limit(2)
        .send()
        .await
        .expect("list groups page 1");

    assert_eq!(page1.groups().len(), 2);
    assert!(
        page1.next_token().is_some(),
        "Should have next token for page 2"
    );

    let page2 = client
        .list_groups()
        .user_pool_id(&pool_id)
        .limit(2)
        .next_token(page1.next_token().unwrap())
        .send()
        .await
        .expect("list groups page 2");

    assert_eq!(page2.groups().len(), 2);

    // Collect all group names across pages
    let mut all_names: Vec<String> = page1
        .groups()
        .iter()
        .chain(page2.groups().iter())
        .map(|g| g.group_name().unwrap().to_string())
        .collect();
    all_names.sort();
    assert_eq!(all_names, vec!["alpha", "beta", "delta", "gamma"]);
}

#[tokio::test]
async fn cognito_add_remove_user_to_group() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("user-group-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    // Create user and group
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("groupuser")
        .send()
        .await
        .expect("create user");

    client
        .create_group()
        .user_pool_id(&pool_id)
        .group_name("testers")
        .send()
        .await
        .expect("create group");

    // Add user to group
    client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("groupuser")
        .group_name("testers")
        .send()
        .await
        .expect("add user to group");

    // List users in group
    let users_in_group = client
        .list_users_in_group()
        .user_pool_id(&pool_id)
        .group_name("testers")
        .send()
        .await
        .expect("list users in group");

    assert_eq!(users_in_group.users().len(), 1);
    assert_eq!(users_in_group.users()[0].username().unwrap(), "groupuser");

    // Adding same user again should be idempotent
    client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("groupuser")
        .group_name("testers")
        .send()
        .await
        .expect("add user to group again (idempotent)");

    // Still only 1 user
    let users_again = client
        .list_users_in_group()
        .user_pool_id(&pool_id)
        .group_name("testers")
        .send()
        .await
        .expect("list users");
    assert_eq!(users_again.users().len(), 1);

    // Remove user from group
    client
        .admin_remove_user_from_group()
        .user_pool_id(&pool_id)
        .username("groupuser")
        .group_name("testers")
        .send()
        .await
        .expect("remove user from group");

    // List users in group should be empty
    let users_empty = client
        .list_users_in_group()
        .user_pool_id(&pool_id)
        .group_name("testers")
        .send()
        .await
        .expect("list users after removal");
    assert!(
        users_empty.users().is_empty(),
        "Group should be empty after removal"
    );

    // Adding user to non-existent group should fail
    let err = client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("groupuser")
        .group_name("nonexistent")
        .send()
        .await;
    assert!(err.is_err(), "Adding to non-existent group should fail");

    // Adding non-existent user to group should fail
    let err2 = client
        .admin_add_user_to_group()
        .user_pool_id(&pool_id)
        .username("nosuchuser")
        .group_name("testers")
        .send()
        .await;
    assert!(err2.is_err(), "Adding non-existent user should fail");
}

#[tokio::test]
async fn cognito_list_groups_for_user() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("multi-group-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    // Create user
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("multiuser")
        .send()
        .await
        .expect("create user");

    // Create multiple groups and add user to them
    for name in &["group-a", "group-b", "group-c"] {
        client
            .create_group()
            .user_pool_id(&pool_id)
            .group_name(*name)
            .send()
            .await
            .expect("create group");

        client
            .admin_add_user_to_group()
            .user_pool_id(&pool_id)
            .username("multiuser")
            .group_name(*name)
            .send()
            .await
            .expect("add user to group");
    }

    // List groups for user
    let result = client
        .admin_list_groups_for_user()
        .user_pool_id(&pool_id)
        .username("multiuser")
        .send()
        .await
        .expect("list groups for user");

    let groups = result.groups();
    assert_eq!(groups.len(), 3, "User should be in 3 groups");

    let mut names: Vec<String> = groups
        .iter()
        .map(|g| g.group_name().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["group-a", "group-b", "group-c"]);

    // Remove user from one group and verify
    client
        .admin_remove_user_from_group()
        .user_pool_id(&pool_id)
        .username("multiuser")
        .group_name("group-b")
        .send()
        .await
        .expect("remove from group-b");

    let result2 = client
        .admin_list_groups_for_user()
        .user_pool_id(&pool_id)
        .username("multiuser")
        .send()
        .await
        .expect("list groups for user after removal");

    assert_eq!(result2.groups().len(), 2);
    let mut names2: Vec<String> = result2
        .groups()
        .iter()
        .map(|g| g.group_name().unwrap().to_string())
        .collect();
    names2.sort();
    assert_eq!(names2, vec!["group-a", "group-c"]);

    // List groups for non-existent user should fail
    let err = client
        .admin_list_groups_for_user()
        .user_pool_id(&pool_id)
        .username("nosuchuser")
        .send()
        .await;
    assert!(
        err.is_err(),
        "Listing groups for non-existent user should fail"
    );
}

// Helper: create pool + client + user + auth, return (server, client, pool_id, client_id, access_token)
async fn setup_authed_user(
    pool_name: &str,
    client_name: &str,
    username: &str,
    password: &str,
    email: &str,
) -> (
    TestServer,
    aws_sdk_cognitoidentityprovider::Client,
    String,
    String,
    String,
) {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name(pool_name)
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name(client_name)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username(username)
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value(email)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username(username)
        .password(password)
        .permanent(true)
        .send()
        .await
        .expect("set password");

    let auth_result = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", username)
        .auth_parameters("PASSWORD", password)
        .send()
        .await
        .expect("auth");

    let access_token = auth_result
        .authentication_result()
        .unwrap()
        .access_token()
        .unwrap()
        .to_string();

    (server, client, pool_id, client_id, access_token)
}

#[tokio::test]
async fn cognito_get_user() {
    let (_server, client, _pool_id, _client_id, access_token) = setup_authed_user(
        "getuser-pool",
        "getuser-client",
        "getuser",
        "mypasswd",
        "get@example.com",
    )
    .await;

    let result = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .expect("get user");

    assert_eq!(result.username(), "getuser");

    // Check attributes contain email
    let attrs = result.user_attributes();
    let email_attr = attrs.iter().find(|a| a.name() == "email");
    assert!(email_attr.is_some(), "Should have email attribute");
    assert_eq!(email_attr.unwrap().value(), Some("get@example.com"));

    // Invalid token should fail
    let err = client.get_user().access_token("bad-token").send().await;
    assert!(err.is_err(), "Invalid token should fail");
}

#[tokio::test]
async fn cognito_delete_user_self() {
    let (_server, client, pool_id, _client_id, access_token) = setup_authed_user(
        "delself-pool",
        "delself-client",
        "delself",
        "mypasswd",
        "del@example.com",
    )
    .await;

    // Delete self
    client
        .delete_user()
        .access_token(&access_token)
        .send()
        .await
        .expect("delete user");

    // GetUser should fail now
    let err = client.get_user().access_token(&access_token).send().await;
    assert!(err.is_err(), "Get user after delete should fail");

    // Admin get user should also fail
    let err = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("delself")
        .send()
        .await;
    assert!(err.is_err(), "Admin get user after delete should fail");
}

#[tokio::test]
async fn cognito_update_delete_user_attributes_self() {
    let (_server, client, _pool_id, _client_id, access_token) = setup_authed_user(
        "upattr-pool",
        "upattr-client",
        "upattr",
        "mypasswd",
        "old@example.com",
    )
    .await;

    // Update email attribute
    client
        .update_user_attributes()
        .access_token(&access_token)
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("new@example.com")
                .build()
                .unwrap(),
        )
        .user_attributes(
            AttributeType::builder()
                .name("custom:team")
                .value("engineering")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("update user attributes");

    // Verify via GetUser
    let user = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .expect("get user");

    let attrs = user.user_attributes();
    let email = attrs.iter().find(|a| a.name() == "email").unwrap();
    assert_eq!(email.value(), Some("new@example.com"));
    let team = attrs.iter().find(|a| a.name() == "custom:team").unwrap();
    assert_eq!(team.value(), Some("engineering"));

    // Delete email attribute
    client
        .delete_user_attributes()
        .access_token(&access_token)
        .user_attribute_names("email")
        .send()
        .await
        .expect("delete user attributes");

    // Verify email is gone
    let user2 = client
        .get_user()
        .access_token(&access_token)
        .send()
        .await
        .expect("get user after delete attr");

    let attrs2 = user2.user_attributes();
    assert!(
        attrs2.iter().find(|a| a.name() == "email").is_none(),
        "email attribute should be deleted"
    );
    // custom:team should still be there
    assert!(
        attrs2.iter().find(|a| a.name() == "custom:team").is_some(),
        "custom:team should remain"
    );
}

#[tokio::test]
async fn cognito_verify_user_attribute() {
    let (_server, client, pool_id, _client_id, access_token) = setup_authed_user(
        "verify-pool",
        "verify-client",
        "verifyuser",
        "mypasswd",
        "verify@example.com",
    )
    .await;

    // Get verification code
    let code_result = client
        .get_user_attribute_verification_code()
        .access_token(&access_token)
        .attribute_name("email")
        .send()
        .await
        .expect("get verification code");

    let delivery = code_result.code_delivery_details().unwrap();
    assert_eq!(
        delivery.delivery_medium().unwrap(),
        &DeliveryMediumType::Email
    );
    assert_eq!(delivery.attribute_name().unwrap(), "email");
    let dest = delivery.destination().unwrap();
    assert!(dest.contains("***"), "Destination should be masked: {dest}");

    // Wrong code should fail with CodeMismatchException
    let err = client
        .verify_user_attribute()
        .access_token(&access_token)
        .attribute_name("email")
        .code("000000")
        .send()
        .await;
    assert!(err.is_err(), "Wrong code should fail");

    // Verify email_verified is not yet set
    let user = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("verifyuser")
        .send()
        .await
        .expect("admin get user");

    let attrs = user.user_attributes();
    let email_verified = attrs.iter().find(|a| a.name() == "email_verified");
    assert!(
        email_verified.is_none() || email_verified.unwrap().value() != Some("true"),
        "email should not be verified yet"
    );

    // Invalid token should fail
    let err = client
        .get_user_attribute_verification_code()
        .access_token("bad-token")
        .attribute_name("email")
        .send()
        .await;
    assert!(err.is_err(), "Invalid token should fail");
}

#[tokio::test]
async fn cognito_resend_confirmation_code() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool with relaxed policy
    let pool_result = client
        .create_user_pool()
        .pool_name("resend-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let client_result = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("resend-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = client_result
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Sign up user
    client
        .sign_up()
        .client_id(&client_id)
        .username("resenduser")
        .password("mypasswd")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("resend@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("sign up");

    // Resend confirmation code
    let result = client
        .resend_confirmation_code()
        .client_id(&client_id)
        .username("resenduser")
        .send()
        .await
        .expect("resend confirmation code");

    let delivery = result.code_delivery_details().unwrap();
    assert_eq!(
        delivery.delivery_medium().unwrap(),
        &DeliveryMediumType::Email
    );
    assert_eq!(delivery.attribute_name().unwrap(), "email");
    let dest = delivery.destination().unwrap();
    assert!(dest.contains("***"), "Destination should be masked: {dest}");
    assert!(
        dest.contains("@example.com"),
        "Should contain domain: {dest}"
    );

    // Confirm with any code (confirm_sign_up accepts any code)
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("resenduser")
        .confirmation_code("123456")
        .send()
        .await
        .expect("confirm sign up");

    // Auth should work now
    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "resenduser")
        .auth_parameters("PASSWORD", "mypasswd")
        .send()
        .await;
    assert!(auth.is_ok(), "Auth after confirm should work");

    // Resend for non-existent user should fail
    let err = client
        .resend_confirmation_code()
        .client_id(&client_id)
        .username("nosuchuser")
        .send()
        .await;
    assert!(err.is_err(), "Resend for non-existent user should fail");
}

#[tokio::test]
async fn cognito_set_get_user_pool_mfa_config() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("mfa-config-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    // Set MFA config to OPTIONAL with software token
    let set_result = client
        .set_user_pool_mfa_config()
        .user_pool_id(&pool_id)
        .mfa_configuration(UserPoolMfaType::Optional)
        .software_token_mfa_configuration(
            SoftwareTokenMfaConfigType::builder().enabled(true).build(),
        )
        .send()
        .await
        .expect("set mfa config");

    assert_eq!(
        set_result.mfa_configuration(),
        Some(&UserPoolMfaType::Optional)
    );
    let stmc = set_result.software_token_mfa_configuration().unwrap();
    assert!(stmc.enabled());

    // Get MFA config and verify
    let get_result = client
        .get_user_pool_mfa_config()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("get mfa config");

    assert_eq!(
        get_result.mfa_configuration(),
        Some(&UserPoolMfaType::Optional)
    );
    let stmc = get_result.software_token_mfa_configuration().unwrap();
    assert!(stmc.enabled());

    // Error for non-existent pool
    let err = client
        .get_user_pool_mfa_config()
        .user_pool_id("us-east-1_NOTEXIST")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent pool");
}

#[tokio::test]
async fn cognito_admin_set_user_mfa_preference() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("admin-mfa-pref-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("mfaprefuser")
        .send()
        .await
        .expect("create user");

    // Set MFA preferences
    let result = client
        .admin_set_user_mfa_preference()
        .user_pool_id(&pool_id)
        .username("mfaprefuser")
        .software_token_mfa_settings(
            SoftwareTokenMfaSettingsType::builder()
                .enabled(true)
                .preferred_mfa(true)
                .build(),
        )
        .sms_mfa_settings(
            SmsMfaSettingsType::builder()
                .enabled(false)
                .preferred_mfa(false)
                .build(),
        )
        .send()
        .await;
    assert!(result.is_ok(), "admin set mfa preference should succeed");

    // Error for non-existent user
    let err = client
        .admin_set_user_mfa_preference()
        .user_pool_id(&pool_id)
        .username("nosuchuser")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent user");
}

#[tokio::test]
async fn cognito_associate_verify_software_token() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool and client
    let pool = client
        .create_user_pool()
        .pool_name("totp-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let pool_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("totp-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = pool_client
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user with password
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("totpuser")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("totpuser")
        .password("passwd")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Auth to get access token
    let auth = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "totpuser")
        .auth_parameters("PASSWORD", "passwd")
        .send()
        .await
        .expect("auth");

    let access_token = auth
        .authentication_result()
        .unwrap()
        .access_token()
        .unwrap()
        .to_string();

    // Associate software token
    let assoc = client
        .associate_software_token()
        .access_token(&access_token)
        .send()
        .await
        .expect("associate software token");

    let secret = assoc.secret_code().unwrap();
    assert_eq!(secret.len(), 32, "Secret should be 32 chars: {secret}");
    assert!(
        secret
            .chars()
            .all(|c| "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".contains(c)),
        "Secret should be base32: {secret}"
    );
    assert!(assoc.session().is_some(), "Should return a session");

    // Verify software token with a 6-digit code
    let verify = client
        .verify_software_token()
        .access_token(&access_token)
        .user_code("123456")
        .send()
        .await
        .expect("verify software token");

    assert_eq!(
        verify.status(),
        Some(&aws_sdk_cognitoidentityprovider::types::VerifySoftwareTokenResponseType::Success)
    );
}

#[tokio::test]
async fn cognito_set_user_mfa_preference() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    // Create pool and client
    let pool = client
        .create_user_pool()
        .pool_name("user-mfa-pref-pool")
        .policies(
            UserPoolPolicyType::builder()
                .password_policy(
                    PasswordPolicyType::builder()
                        .minimum_length(6)
                        .require_uppercase(false)
                        .require_lowercase(false)
                        .require_numbers(false)
                        .require_symbols(false)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let pool_client = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("user-mfa-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = pool_client
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create user with password
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("mfaprefuser2")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("mfaprefuser2")
        .password("passwd")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Auth to get access token
    let auth = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "mfaprefuser2")
        .auth_parameters("PASSWORD", "passwd")
        .send()
        .await
        .expect("auth");

    let access_token = auth
        .authentication_result()
        .unwrap()
        .access_token()
        .unwrap()
        .to_string();

    // Set MFA preference via access token
    let result = client
        .set_user_mfa_preference()
        .access_token(&access_token)
        .software_token_mfa_settings(
            SoftwareTokenMfaSettingsType::builder()
                .enabled(true)
                .preferred_mfa(true)
                .build(),
        )
        .send()
        .await;
    assert!(
        result.is_ok(),
        "set user mfa preference should succeed: {:?}",
        result.err()
    );

    // Invalid token should fail
    let err = client
        .set_user_mfa_preference()
        .access_token("invalid-token")
        .send()
        .await;
    assert!(err.is_err(), "Should fail with invalid token");
}

#[tokio::test]
async fn cognito_create_describe_identity_provider() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("idp-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    // Create an identity provider
    let result = client
        .create_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("MyGoogle")
        .provider_type(IdentityProviderTypeType::Google)
        .provider_details("client_id", "google-client-id")
        .provider_details("client_secret", "google-secret")
        .attribute_mapping("email", "email")
        .idp_identifiers("google.example.com")
        .send()
        .await
        .expect("create identity provider");

    let idp = result.identity_provider().unwrap();
    assert_eq!(idp.provider_name().unwrap(), "MyGoogle");
    assert_eq!(
        idp.provider_type().unwrap(),
        &IdentityProviderTypeType::Google
    );
    let details = idp.provider_details().unwrap();
    assert_eq!(details.get("client_id").unwrap(), "google-client-id");
    let mapping = idp.attribute_mapping().unwrap();
    assert_eq!(mapping.get("email").unwrap(), "email");
    let identifiers = idp.idp_identifiers();
    assert_eq!(identifiers, &["google.example.com"]);

    // Describe it
    let described = client
        .describe_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("MyGoogle")
        .send()
        .await
        .expect("describe identity provider");

    let idp2 = described.identity_provider().unwrap();
    assert_eq!(idp2.provider_name().unwrap(), "MyGoogle");
    assert_eq!(
        idp2.provider_type().unwrap(),
        &IdentityProviderTypeType::Google
    );

    // Describe non-existent should fail
    let err = client
        .describe_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("DoesNotExist")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent provider");
}

#[tokio::test]
async fn cognito_update_identity_provider() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("idp-update-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    client
        .create_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("MySAML")
        .provider_type(IdentityProviderTypeType::Saml)
        .provider_details("MetadataURL", "https://example.com/saml")
        .send()
        .await
        .expect("create identity provider");

    // Update provider details
    let updated = client
        .update_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("MySAML")
        .provider_details("MetadataURL", "https://new.example.com/saml")
        .attribute_mapping("email", "saml:email")
        .send()
        .await
        .expect("update identity provider");

    let idp = updated.identity_provider().unwrap();
    assert_eq!(idp.provider_name().unwrap(), "MySAML");
    let details = idp.provider_details().unwrap();
    assert_eq!(
        details.get("MetadataURL").unwrap(),
        "https://new.example.com/saml"
    );
    let mapping = idp.attribute_mapping().unwrap();
    assert_eq!(mapping.get("email").unwrap(), "saml:email");

    // Update non-existent should fail
    let err = client
        .update_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("DoesNotExist")
        .provider_details("foo", "bar")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent provider");
}

#[tokio::test]
async fn cognito_delete_identity_provider() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("idp-delete-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    client
        .create_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("MyOIDC")
        .provider_type(IdentityProviderTypeType::Oidc)
        .provider_details("client_id", "oidc-id")
        .provider_details("client_secret", "oidc-secret")
        .provider_details("oidc_issuer", "https://auth.example.com")
        .send()
        .await
        .expect("create identity provider");

    // Delete it
    client
        .delete_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("MyOIDC")
        .send()
        .await
        .expect("delete identity provider");

    // Describe should now fail
    let err = client
        .describe_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("MyOIDC")
        .send()
        .await;
    assert!(err.is_err(), "Should fail after deletion");

    // Delete again should fail
    let err = client
        .delete_identity_provider()
        .user_pool_id(pool_id)
        .provider_name("MyOIDC")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for already-deleted provider");
}

#[tokio::test]
async fn cognito_list_identity_providers() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("idp-list-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    // Create multiple providers
    for (name, ptype) in &[
        ("GoogleIdP", IdentityProviderTypeType::Google),
        ("FacebookIdP", IdentityProviderTypeType::Facebook),
        ("AppleIdP", IdentityProviderTypeType::SignInWithApple),
    ] {
        client
            .create_identity_provider()
            .user_pool_id(pool_id)
            .provider_name(*name)
            .provider_type(ptype.clone())
            .provider_details("client_id", "test")
            .send()
            .await
            .unwrap_or_else(|e| panic!("create {name}: {e}"));
    }

    // List all
    let result = client
        .list_identity_providers()
        .user_pool_id(pool_id)
        .max_results(10)
        .send()
        .await
        .expect("list identity providers");

    let providers = result.providers();
    assert_eq!(providers.len(), 3);

    let names: Vec<&str> = providers.iter().filter_map(|p| p.provider_name()).collect();
    assert!(names.contains(&"GoogleIdP"));
    assert!(names.contains(&"FacebookIdP"));
    assert!(names.contains(&"AppleIdP"));

    // List with pagination (max_results=1)
    let page1 = client
        .list_identity_providers()
        .user_pool_id(pool_id)
        .max_results(1)
        .send()
        .await
        .expect("list page 1");

    assert_eq!(page1.providers().len(), 1);
    assert!(
        page1.next_token().is_some(),
        "Should have next_token with more results"
    );

    // List non-existent pool should fail
    let err = client
        .list_identity_providers()
        .user_pool_id("us-east-1_NOTAPOOL")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent pool");
}

#[tokio::test]
async fn cognito_create_describe_resource_server() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("rs-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    let scope = ResourceServerScopeType::builder()
        .scope_name("read")
        .scope_description("Read access")
        .build()
        .unwrap();

    let result = client
        .create_resource_server()
        .user_pool_id(pool_id)
        .identifier("https://api.example.com")
        .name("My API")
        .scopes(scope)
        .send()
        .await
        .expect("create resource server");

    let rs = result.resource_server().unwrap();
    assert_eq!(rs.identifier().unwrap(), "https://api.example.com");
    assert_eq!(rs.name().unwrap(), "My API");
    assert_eq!(rs.scopes().len(), 1);
    assert_eq!(rs.scopes()[0].scope_name(), "read");
    assert_eq!(rs.scopes()[0].scope_description(), "Read access");

    // Describe it
    let described = client
        .describe_resource_server()
        .user_pool_id(pool_id)
        .identifier("https://api.example.com")
        .send()
        .await
        .expect("describe resource server");

    let rs2 = described.resource_server().unwrap();
    assert_eq!(rs2.identifier().unwrap(), "https://api.example.com");
    assert_eq!(rs2.name().unwrap(), "My API");

    // Describe non-existent should fail
    let err = client
        .describe_resource_server()
        .user_pool_id(pool_id)
        .identifier("https://nope.example.com")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent resource server");
}

#[tokio::test]
async fn cognito_update_delete_resource_server() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("rs-update-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    client
        .create_resource_server()
        .user_pool_id(pool_id)
        .identifier("https://api.example.com")
        .name("My API")
        .send()
        .await
        .expect("create resource server");

    // Update with new scopes
    let scope = ResourceServerScopeType::builder()
        .scope_name("write")
        .scope_description("Write access")
        .build()
        .unwrap();

    let updated = client
        .update_resource_server()
        .user_pool_id(pool_id)
        .identifier("https://api.example.com")
        .name("My Updated API")
        .scopes(scope)
        .send()
        .await
        .expect("update resource server");

    let rs = updated.resource_server().unwrap();
    assert_eq!(rs.name().unwrap(), "My Updated API");
    assert_eq!(rs.scopes().len(), 1);
    assert_eq!(rs.scopes()[0].scope_name(), "write");

    // Delete
    client
        .delete_resource_server()
        .user_pool_id(pool_id)
        .identifier("https://api.example.com")
        .send()
        .await
        .expect("delete resource server");

    // Describe after delete should fail
    let err = client
        .describe_resource_server()
        .user_pool_id(pool_id)
        .identifier("https://api.example.com")
        .send()
        .await;
    assert!(err.is_err(), "Should fail after delete");

    // Delete non-existent should fail
    let err = client
        .delete_resource_server()
        .user_pool_id(pool_id)
        .identifier("https://api.example.com")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent resource server");
}

#[tokio::test]
async fn cognito_list_resource_servers() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("rs-list-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    // Create multiple resource servers
    for i in 0..3 {
        client
            .create_resource_server()
            .user_pool_id(pool_id)
            .identifier(format!("https://api{i}.example.com"))
            .name(format!("API {i}"))
            .send()
            .await
            .expect("create resource server");
    }

    // List all
    let list = client
        .list_resource_servers()
        .user_pool_id(pool_id)
        .send()
        .await
        .expect("list resource servers");

    assert_eq!(list.resource_servers().len(), 3);

    // List with pagination
    let page1 = client
        .list_resource_servers()
        .user_pool_id(pool_id)
        .max_results(1)
        .send()
        .await
        .expect("list page 1");

    assert_eq!(page1.resource_servers().len(), 1);
    assert!(
        page1.next_token().is_some(),
        "Should have next_token with more results"
    );

    // List non-existent pool should fail
    let err = client
        .list_resource_servers()
        .user_pool_id("us-east-1_NOTAPOOL")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent pool");
}

#[tokio::test]
async fn cognito_create_describe_domain() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("domain-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    // Create a prefix domain
    client
        .create_user_pool_domain()
        .user_pool_id(pool_id)
        .domain("my-test-domain")
        .send()
        .await
        .expect("create domain");

    // Describe it
    let described = client
        .describe_user_pool_domain()
        .domain("my-test-domain")
        .send()
        .await
        .expect("describe domain");

    let desc = described.domain_description().unwrap();
    assert_eq!(desc.domain().unwrap(), "my-test-domain");
    assert_eq!(desc.user_pool_id().unwrap(), pool_id);
    assert_eq!(desc.status().unwrap(), &DomainStatusType::Active);

    // Describe non-existent should return empty DomainDescription (not an error)
    let result = client
        .describe_user_pool_domain()
        .domain("nonexistent-domain")
        .send()
        .await
        .expect("describe non-existent should succeed");

    let desc2 = result.domain_description().unwrap();
    // domain field should be None for non-existent
    assert!(
        desc2.domain().is_none(),
        "Non-existent domain should return empty description"
    );

    // Duplicate domain should fail
    let err = client
        .create_user_pool_domain()
        .user_pool_id(pool_id)
        .domain("my-test-domain")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for duplicate domain");
}

#[tokio::test]
async fn cognito_delete_domain() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("domain-del-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap();

    client
        .create_user_pool_domain()
        .user_pool_id(pool_id)
        .domain("del-test-domain")
        .send()
        .await
        .expect("create domain");

    // Delete it
    client
        .delete_user_pool_domain()
        .user_pool_id(pool_id)
        .domain("del-test-domain")
        .send()
        .await
        .expect("delete domain");

    // Describe after delete should return empty
    let result = client
        .describe_user_pool_domain()
        .domain("del-test-domain")
        .send()
        .await
        .expect("describe after delete");

    let desc = result.domain_description().unwrap();
    assert!(
        desc.domain().is_none(),
        "Deleted domain should return empty description"
    );

    // Delete non-existent should fail
    let err = client
        .delete_user_pool_domain()
        .user_pool_id(pool_id)
        .domain("del-test-domain")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent domain");
}
