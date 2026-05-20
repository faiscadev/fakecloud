mod helpers;
use helpers::TestServer;

async fn fetch_confirmation_code(server: &TestServer, pool_id: &str, username: &str) -> String {
    let url = format!(
        "{}/_fakecloud/cognito/confirmation-codes/{}/{}",
        server.endpoint(),
        pool_id,
        username
    );
    reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .expect("introspection fetch")
        .json::<serde_json::Value>()
        .await
        .expect("introspection json")["confirmationCode"]
        .as_str()
        .unwrap_or_else(|| panic!("no confirmationCode for {pool_id}/{username}"))
        .to_string()
}

use aws_sdk_cognitoidentityprovider::types::{
    AccountRecoverySettingType, AttributeType, ChallengeNameType, DeliveryMediumType,
    DeviceRememberedStatusType, DomainStatusType, ExplicitAuthFlowsType, IdentityProviderTypeType,
    PasswordPolicyType, RecoveryOptionNameType, RecoveryOptionType, ResourceServerScopeType,
    SmsMfaSettingsType, SoftwareTokenMfaConfigType, SoftwareTokenMfaSettingsType, UserPoolMfaType,
    UserPoolPolicyType, UserStatusType,
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
async fn cognito_user_pool_default_shape() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let result = client
        .create_user_pool()
        .pool_name("default-shape")
        .send()
        .await
        .expect("create user pool");
    let id = result.user_pool().unwrap().id().unwrap().to_string();

    let described = client
        .describe_user_pool()
        .user_pool_id(&id)
        .send()
        .await
        .expect("describe user pool");
    let pool = described.user_pool().unwrap();

    // user_pool_tier defaults to ESSENTIALS
    assert_eq!(
        pool.user_pool_tier().map(|t| t.as_str()),
        Some("ESSENTIALS"),
    );

    // email_configuration default: COGNITO_DEFAULT
    let ec = pool
        .email_configuration()
        .expect("EmailConfiguration must be returned");
    assert_eq!(
        ec.email_sending_account().map(|a| a.as_str()),
        Some("COGNITO_DEFAULT"),
    );

    // verification_message_template default: CONFIRM_WITH_CODE
    let vmt = pool
        .verification_message_template()
        .expect("VerificationMessageTemplate must be returned");
    assert_eq!(
        vmt.default_email_option().map(|o| o.as_str()),
        Some("CONFIRM_WITH_CODE"),
    );

    // sign_in_policy.allowed_first_auth_factors defaults to [PASSWORD]
    let sip = pool
        .policies()
        .and_then(|p| p.sign_in_policy())
        .expect("SignInPolicy must be returned");
    let factors: Vec<&str> = sip
        .allowed_first_auth_factors()
        .iter()
        .map(|f| f.as_str())
        .collect();
    assert_eq!(factors, vec!["PASSWORD"]);

    // account_recovery_setting default: at least one mechanism
    let ars = pool
        .account_recovery_setting()
        .expect("AccountRecoverySetting must be returned");
    assert!(
        !ars.recovery_mechanisms().is_empty(),
        "default account recovery must have at least one mechanism"
    );

    // admin_create_user_config is always returned
    let acuc = pool
        .admin_create_user_config()
        .expect("AdminCreateUserConfig must be returned");
    assert!(!acuc.allow_admin_create_user_only());

    // deletion_protection default: INACTIVE
    assert_eq!(
        pool.deletion_protection().map(|d| d.as_str()),
        Some("INACTIVE"),
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
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "signupuser").await)
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
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "refreshuser").await)
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
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "resenduser").await)
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

// ── Device Management E2E Tests ─────────────────────────────────────

/// Helper: create pool + client + user + auth, return (client, pool_id, username, access_token)
async fn setup_pool_with_auth(
    server: &TestServer,
) -> (
    aws_sdk_cognitoidentityprovider::Client,
    String,
    String,
    String,
) {
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("device-pool")
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
        .client_name("dev-client")
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
        .username("devuser")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("devuser")
        .password("secret")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    let auth_result = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "devuser")
        .auth_parameters("PASSWORD", "secret")
        .send()
        .await
        .expect("auth");

    let access_token = auth_result
        .authentication_result()
        .unwrap()
        .access_token()
        .unwrap()
        .to_string();

    (client, pool_id, "devuser".to_string(), access_token)
}

#[tokio::test]
async fn cognito_confirm_admin_get_device() {
    let server = TestServer::start().await;
    let (client, pool_id, username, access_token) = setup_pool_with_auth(&server).await;

    // Confirm a device
    let confirm = client
        .confirm_device()
        .access_token(&access_token)
        .device_key("test-device-key-1")
        .device_name("My Phone")
        .send()
        .await
        .expect("confirm device");
    assert!(!confirm.user_confirmation_necessary());

    // AdminGetDevice
    let device = client
        .admin_get_device()
        .user_pool_id(&pool_id)
        .username(&username)
        .device_key("test-device-key-1")
        .send()
        .await
        .expect("admin get device");

    let dev = device.device().unwrap();
    assert_eq!(dev.device_key().unwrap(), "test-device-key-1");
    assert!(dev.device_create_date().is_some());

    // Non-existent device should fail
    let err = client
        .admin_get_device()
        .user_pool_id(&pool_id)
        .username(&username)
        .device_key("nonexistent")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent device");
}

#[tokio::test]
async fn cognito_admin_list_devices() {
    let server = TestServer::start().await;
    let (client, pool_id, username, access_token) = setup_pool_with_auth(&server).await;

    // Confirm two devices
    for key in &["dev-a", "dev-b"] {
        client
            .confirm_device()
            .access_token(&access_token)
            .device_key(*key)
            .send()
            .await
            .expect("confirm device");
    }

    // List devices
    let list = client
        .admin_list_devices()
        .user_pool_id(&pool_id)
        .username(&username)
        .limit(10)
        .send()
        .await
        .expect("admin list devices");

    let devices = list.devices();
    assert_eq!(devices.len(), 2, "Should have 2 devices");
}

#[tokio::test]
async fn cognito_admin_forget_device() {
    let server = TestServer::start().await;
    let (client, pool_id, username, access_token) = setup_pool_with_auth(&server).await;

    // Confirm a device
    client
        .confirm_device()
        .access_token(&access_token)
        .device_key("forget-me")
        .send()
        .await
        .expect("confirm device");

    // Forget it
    client
        .admin_forget_device()
        .user_pool_id(&pool_id)
        .username(&username)
        .device_key("forget-me")
        .send()
        .await
        .expect("admin forget device");

    // Get should fail
    let err = client
        .admin_get_device()
        .user_pool_id(&pool_id)
        .username(&username)
        .device_key("forget-me")
        .send()
        .await;
    assert!(err.is_err(), "Device should be forgotten");

    // Forgetting again should fail
    let err = client
        .admin_forget_device()
        .user_pool_id(&pool_id)
        .username(&username)
        .device_key("forget-me")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for already-forgotten device");
}

#[tokio::test]
async fn cognito_admin_update_device_status() {
    let server = TestServer::start().await;
    let (client, pool_id, username, access_token) = setup_pool_with_auth(&server).await;

    // Confirm a device
    client
        .confirm_device()
        .access_token(&access_token)
        .device_key("status-dev")
        .send()
        .await
        .expect("confirm device");

    // Update status to remembered
    client
        .admin_update_device_status()
        .user_pool_id(&pool_id)
        .username(&username)
        .device_key("status-dev")
        .device_remembered_status(DeviceRememberedStatusType::Remembered)
        .send()
        .await
        .expect("update device status");

    // Update to not_remembered should also work
    client
        .admin_update_device_status()
        .user_pool_id(&pool_id)
        .username(&username)
        .device_key("status-dev")
        .device_remembered_status(DeviceRememberedStatusType::NotRemembered)
        .send()
        .await
        .expect("update device status to not_remembered");

    // Non-existent device should fail
    let err = client
        .admin_update_device_status()
        .user_pool_id(&pool_id)
        .username(&username)
        .device_key("nonexistent")
        .device_remembered_status(DeviceRememberedStatusType::Remembered)
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent device");
}

// ── Tags E2E Tests ──────────────────────────────────────────────────

#[tokio::test]
async fn cognito_tag_untag_list_tags() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("tag-pool")
        .send()
        .await
        .expect("create pool");
    let pool = pool_result.user_pool().unwrap();
    let pool_arn = pool.arn().unwrap().to_string();

    // Tag
    client
        .tag_resource()
        .resource_arn(&pool_arn)
        .tags("env", "staging")
        .tags("team", "platform")
        .send()
        .await
        .expect("tag resource");

    // List
    let list = client
        .list_tags_for_resource()
        .resource_arn(&pool_arn)
        .send()
        .await
        .expect("list tags");
    let tags = list.tags().unwrap();
    assert_eq!(tags.get("env").unwrap(), "staging");
    assert_eq!(tags.get("team").unwrap(), "platform");

    // Untag
    client
        .untag_resource()
        .resource_arn(&pool_arn)
        .tag_keys("team")
        .send()
        .await
        .expect("untag resource");

    // Verify
    let list2 = client
        .list_tags_for_resource()
        .resource_arn(&pool_arn)
        .send()
        .await
        .expect("list tags after untag");
    let tags2 = list2.tags().unwrap();
    assert_eq!(tags2.get("env").unwrap(), "staging");
    assert!(tags2.get("team").is_none(), "team tag should be removed");

    // Non-existent ARN should fail
    let err = client
        .list_tags_for_resource()
        .resource_arn("arn:aws:cognito-idp:us-east-1:000000000000:userpool/nonexistent")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent resource");
}

// ── Import Jobs E2E Tests ───────────────────────────────────────────

#[tokio::test]
async fn cognito_create_describe_list_import_jobs() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("import-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    // Create import job
    let create = client
        .create_user_import_job()
        .user_pool_id(&pool_id)
        .job_name("test-import")
        .cloud_watch_logs_role_arn("arn:aws:iam::123456789012:role/CognitoImportRole")
        .send()
        .await
        .expect("create import job");

    let job = create.user_import_job().unwrap();
    assert_eq!(job.job_name().unwrap(), "test-import");
    assert_eq!(job.status().unwrap().as_str(), "Created");
    let job_id = job.job_id().unwrap().to_string();
    assert!(job.pre_signed_url().is_some());

    // Describe
    let describe = client
        .describe_user_import_job()
        .user_pool_id(&pool_id)
        .job_id(&job_id)
        .send()
        .await
        .expect("describe import job");
    let described = describe.user_import_job().unwrap();
    assert_eq!(described.job_name().unwrap(), "test-import");
    assert_eq!(described.user_pool_id().unwrap(), pool_id);

    // Create another job
    client
        .create_user_import_job()
        .user_pool_id(&pool_id)
        .job_name("test-import-2")
        .cloud_watch_logs_role_arn("arn:aws:iam::123456789012:role/CognitoImportRole")
        .send()
        .await
        .expect("create second import job");

    // List
    let list = client
        .list_user_import_jobs()
        .user_pool_id(&pool_id)
        .max_results(10)
        .send()
        .await
        .expect("list import jobs");
    assert_eq!(
        list.user_import_jobs().len(),
        2,
        "Should have 2 import jobs"
    );

    // Describe non-existent should fail
    let err = client
        .describe_user_import_job()
        .user_pool_id(&pool_id)
        .job_id("nonexistent")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent job");
}

#[tokio::test]
async fn cognito_get_csv_header() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool_result = client
        .create_user_pool()
        .pool_name("csv-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool_result.user_pool().unwrap().id().unwrap().to_string();

    let csv = client
        .get_csv_header()
        .user_pool_id(&pool_id)
        .send()
        .await
        .expect("get csv header");

    assert_eq!(csv.user_pool_id().unwrap(), pool_id);
    let headers = csv.csv_header();
    assert!(!headers.is_empty(), "CSV headers should not be empty");
    // Default schema includes 'sub', 'email', 'name', etc.
    assert!(
        headers.contains(&"sub".to_string()),
        "Headers should contain 'sub'"
    );
    assert!(
        headers.contains(&"email".to_string()),
        "Headers should contain 'email'"
    );

    // Non-existent pool should fail
    let err = client
        .get_csv_header()
        .user_pool_id("us-east-1_nonexistent")
        .send()
        .await;
    assert!(err.is_err(), "Should fail for non-existent pool");
}

// --- Simulation / Testing API tests ---

#[tokio::test]
async fn cognito_simulation_confirmation_codes_and_force_confirm() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let http = reqwest::Client::new();

    // Create pool + client
    let pool = client
        .create_user_pool()
        .pool_name("sim-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("sim-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Sign up a user
    client
        .sign_up()
        .client_id(&client_id)
        .username("simuser")
        .password("P@ssw0rd!")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("sim@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("sign up");

    // SignUp generates a verification confirmation code (matches AWS).
    let resp: serde_json::Value = http
        .get(format!(
            "{}/_fakecloud/cognito/confirmation-codes",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let codes = resp["codes"].as_array().unwrap();
    assert_eq!(
        codes.len(),
        1,
        "SignUp should generate one verification code"
    );

    // Force-confirm user so we can then trigger ForgotPassword
    let confirm_resp: serde_json::Value = http
        .post(format!(
            "{}/_fakecloud/cognito/confirm-user",
            server.endpoint()
        ))
        .json(&serde_json::json!({
            "userPoolId": pool_id,
            "username": "simuser"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(confirm_resp["confirmed"], true);

    // Verify user is now CONFIRMED via AdminGetUser
    let user = client
        .admin_get_user()
        .user_pool_id(&pool_id)
        .username("simuser")
        .send()
        .await
        .expect("get user");
    assert_eq!(
        user.user_status(),
        Some(&UserStatusType::Confirmed),
        "User should be CONFIRMED after force-confirm"
    );

    // Trigger ForgotPassword to create a confirmation code
    client
        .forgot_password()
        .client_id(&client_id)
        .username("simuser")
        .send()
        .await
        .expect("forgot password");

    // List codes again - should now have the forgot password code
    let resp: serde_json::Value = http
        .get(format!(
            "{}/_fakecloud/cognito/confirmation-codes",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let codes = resp["codes"].as_array().unwrap();
    assert!(
        codes.iter().any(|c| c["username"] == "simuser"),
        "Should have simuser's code: {codes:?}"
    );

    // Force-confirm non-existent user should 404
    let resp = http
        .post(format!(
            "{}/_fakecloud/cognito/confirm-user",
            server.endpoint()
        ))
        .json(&serde_json::json!({
            "userPoolId": pool_id,
            "username": "nonexistent"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn cognito_simulation_tokens_and_expire() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let http = reqwest::Client::new();

    // Create pool + client
    let pool = client
        .create_user_pool()
        .pool_name("token-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("token-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Create and confirm user
    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("tokenuser")
        .temporary_password("TempP@ss1!")
        .send()
        .await
        .expect("create user");

    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("tokenuser")
        .password("P@ssw0rd!")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    // Authenticate user
    client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "tokenuser")
        .auth_parameters("PASSWORD", "P@ssw0rd!")
        .send()
        .await
        .expect("auth");

    // List tokens
    let resp: serde_json::Value = http
        .get(format!("{}/_fakecloud/cognito/tokens", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let tokens = resp["tokens"].as_array().unwrap();
    let access_count = tokens.iter().filter(|t| t["type"] == "access").count();
    let refresh_count = tokens.iter().filter(|t| t["type"] == "refresh").count();
    assert!(access_count >= 1, "Should have at least 1 access token");
    assert!(refresh_count >= 1, "Should have at least 1 refresh token");

    // All tokens should belong to tokenuser
    for t in tokens {
        assert_eq!(t["username"], "tokenuser");
        assert_eq!(t["poolId"], pool_id);
        assert!(t["issuedAt"].as_f64().unwrap() > 0.0);
    }

    // Expire tokens for this user
    let resp: serde_json::Value = http
        .post(format!(
            "{}/_fakecloud/cognito/expire-tokens",
            server.endpoint()
        ))
        .json(&serde_json::json!({
            "userPoolId": pool_id,
            "username": "tokenuser"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let expired = resp["expiredTokens"].as_u64().unwrap();
    assert!(
        expired >= 2,
        "Should expire at least access + refresh token, got {expired}"
    );

    // Verify tokens are gone
    let resp: serde_json::Value = http
        .get(format!("{}/_fakecloud/cognito/tokens", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let tokens = resp["tokens"].as_array().unwrap();
    assert!(tokens.is_empty(), "Tokens should be empty after expiration");
}

#[tokio::test]
async fn cognito_simulation_auth_events() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;
    let http = reqwest::Client::new();

    // Create pool + client
    let pool = client
        .create_user_pool()
        .pool_name("events-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("events-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    // Sign up user
    client
        .sign_up()
        .client_id(&client_id)
        .username("evuser")
        .password("P@ssw0rd!")
        .user_attributes(
            AttributeType::builder()
                .name("email")
                .value("ev@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("sign up");

    // Confirm user
    client
        .admin_confirm_sign_up()
        .user_pool_id(&pool_id)
        .username("evuser")
        .send()
        .await
        .expect("confirm");

    // Successful sign in
    client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "evuser")
        .auth_parameters("PASSWORD", "P@ssw0rd!")
        .send()
        .await
        .expect("sign in");

    // Failed sign in with wrong password
    let _err = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "evuser")
        .auth_parameters("PASSWORD", "WrongPass1!")
        .send()
        .await;

    // List auth events
    let resp: serde_json::Value = http
        .get(format!(
            "{}/_fakecloud/cognito/auth-events",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let events = resp["events"].as_array().unwrap();

    // Should have: SIGN_UP, SIGN_IN, SIGN_IN_FAILURE
    let event_types: Vec<&str> = events
        .iter()
        .map(|e| e["eventType"].as_str().unwrap())
        .collect();

    assert!(
        event_types.contains(&"SIGN_UP"),
        "Should have SIGN_UP event: {event_types:?}"
    );
    assert!(
        event_types.contains(&"SIGN_IN"),
        "Should have SIGN_IN event: {event_types:?}"
    );
    assert!(
        event_types.contains(&"SIGN_IN_FAILURE"),
        "Should have SIGN_IN_FAILURE event: {event_types:?}"
    );

    // Verify event details
    let signup = events.iter().find(|e| e["eventType"] == "SIGN_UP").unwrap();
    assert_eq!(signup["username"], "evuser");
    assert_eq!(signup["userPoolId"], pool_id);
    assert_eq!(signup["success"], true);
    assert!(signup["timestamp"].as_f64().unwrap() > 0.0);

    let failure = events
        .iter()
        .find(|e| e["eventType"] == "SIGN_IN_FAILURE")
        .unwrap();
    assert_eq!(failure["username"], "evuser");
    assert_eq!(failure["success"], false);
}

#[tokio::test]
async fn cognito_well_known_jwks_returns_public_key() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("jwks-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/{}/.well-known/jwks.json", server.endpoint(), pool_id);
    let body: serde_json::Value = http.get(&url).send().await.unwrap().json().await.unwrap();
    let keys = body["keys"].as_array().expect("keys array");
    assert_eq!(keys.len(), 1);
    let key = &keys[0];
    assert_eq!(key["alg"], "RS256");
    assert_eq!(key["kty"], "RSA");
    assert_eq!(key["use"], "sig");
    let kid = key["kid"].as_str().expect("kid string");
    assert_eq!(kid.len(), 16, "kid is the 16-hex-char SHA-256 prefix");
    assert!(kid.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(key["n"].as_str().unwrap().len() > 100);
    assert!(!key["e"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn cognito_well_known_openid_configuration_uses_pool_region() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oidc-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let http = reqwest::Client::new();
    let url = format!(
        "{}/{}/.well-known/openid-configuration",
        server.endpoint(),
        pool_id
    );
    let body: serde_json::Value = http.get(&url).send().await.unwrap().json().await.unwrap();
    let issuer = body["issuer"].as_str().unwrap();
    assert!(issuer.contains(&pool_id));
    assert!(issuer.contains("us-east-1"));
    assert!(body["jwks_uri"]
        .as_str()
        .unwrap()
        .ends_with(&format!("/{pool_id}/.well-known/jwks.json")));
    let algs = body["id_token_signing_alg_values_supported"]
        .as_array()
        .unwrap();
    assert_eq!(algs[0], "RS256");
    let response_types = body["response_types_supported"].as_array().unwrap();
    assert!(response_types.iter().any(|v| v == "code"));
    assert!(response_types.iter().any(|v| v == "token"));
    assert_eq!(body["subject_types_supported"][0], "public");
    let scopes = body["scopes_supported"].as_array().unwrap();
    for required in ["openid", "email", "profile", "phone"] {
        assert!(
            scopes.iter().any(|v| v == required),
            "scopes_supported must include {required}: {scopes:?}"
        );
    }
    let auth_methods = body["token_endpoint_auth_methods_supported"]
        .as_array()
        .unwrap();
    assert!(auth_methods.iter().any(|v| v == "client_secret_basic"));
    assert!(auth_methods.iter().any(|v| v == "client_secret_post"));
}

/// OIDC discovery omits OAuth2 endpoints until the pool has a hosted-UI
/// domain — matching real Cognito, which does the same. After
/// `CreateUserPoolDomain` the endpoints must appear and resolve to
/// fakecloud's local OAuth2 routes.
#[tokio::test]
async fn cognito_oidc_oauth_endpoints_track_pool_domain() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oidc-domain-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let http = reqwest::Client::new();
    let url = format!(
        "{}/{}/.well-known/openid-configuration",
        server.endpoint(),
        pool_id
    );

    // Domain not configured yet -> OAuth endpoints must be absent.
    let body: serde_json::Value = http.get(&url).send().await.unwrap().json().await.unwrap();
    assert!(
        body.get("authorization_endpoint").is_none(),
        "authorization_endpoint must be omitted before CreateUserPoolDomain: {body}"
    );
    assert!(body.get("token_endpoint").is_none());
    assert!(body.get("userinfo_endpoint").is_none());
    assert!(body.get("revocation_endpoint").is_none());
    assert!(body["jwks_uri"].is_string(), "jwks_uri is unconditional");

    // Attach a hosted-UI domain.
    client
        .create_user_pool_domain()
        .user_pool_id(&pool_id)
        .domain("oidc-domain-test")
        .send()
        .await
        .expect("create user pool domain");

    let body: serde_json::Value = http.get(&url).send().await.unwrap().json().await.unwrap();
    let auth_ep = body["authorization_endpoint"].as_str().unwrap();
    assert!(
        auth_ep.ends_with("/oauth2/authorize"),
        "authorization_endpoint must point at /oauth2/authorize: {auth_ep}"
    );
    let token_ep = body["token_endpoint"].as_str().unwrap();
    assert!(token_ep.ends_with("/oauth2/token"));
    let userinfo_ep = body["userinfo_endpoint"].as_str().unwrap();
    assert!(userinfo_ep.ends_with("/oauth2/userInfo"));
    let revoke_ep = body["revocation_endpoint"].as_str().unwrap();
    assert!(revoke_ep.ends_with("/oauth2/revoke"));
}

/// Y1: every JWT issued by the pool is real RS256-signed against the
/// per-pool RSA-2048 keypair. End-to-end shape:
///   1. CreateUserPool -> pool gets a freshly-generated keypair.
///   2. AdminInitiateAuth issues real ID + access tokens.
///   3. The pool's JWKS endpoint serves the matching public key.
///   4. Tokens verify cryptographically with that public key.
#[tokio::test]
async fn cognito_jwt_is_real_rs256_signed() {
    use base64::Engine as _;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::sha2::Sha256;
    use rsa::signature::Verifier;
    use rsa::traits::PublicKeyParts;
    use rsa::{BigUint, RsaPublicKey};

    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("rs256-pool")
        .send()
        .await
        .expect("create user pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let upc = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("rs256-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowAdminUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = upc
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .admin_create_user()
        .user_pool_id(&pool_id)
        .username("rs256user")
        .temporary_password("TempP@ss1!")
        .send()
        .await
        .expect("create user");
    client
        .admin_set_user_password()
        .user_pool_id(&pool_id)
        .username("rs256user")
        .password("Permanent1!")
        .permanent(true)
        .send()
        .await
        .expect("set password");

    let auth = client
        .admin_initiate_auth()
        .user_pool_id(&pool_id)
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::AdminUserPasswordAuth)
        .auth_parameters("USERNAME", "rs256user")
        .auth_parameters("PASSWORD", "Permanent1!")
        .send()
        .await
        .expect("auth");
    let result = auth.authentication_result().expect("authentication result");
    let id_token = result.id_token().expect("id token").to_string();
    let access_token = result.access_token().expect("access token").to_string();

    // Header is base64url JSON; assert AWS-shaped fields.
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header_segment = id_token.split('.').next().expect("header segment");
    let header_bytes = b64.decode(header_segment).expect("header decodes");
    let header: serde_json::Value = serde_json::from_slice(&header_bytes).expect("header JSON");
    assert_eq!(header["alg"], "RS256", "header.alg must be RS256");
    assert_eq!(header["typ"], "JWT", "header.typ must be JWT");
    let kid = header["kid"].as_str().expect("kid present");
    assert_eq!(kid.len(), 16, "kid is 16-hex-char SHA-256 prefix: {kid}");
    assert!(
        kid.chars().all(|c| c.is_ascii_hexdigit()),
        "kid must be lowercase hex: {kid}"
    );

    // Pull the matching public key from the pool's JWKS endpoint —
    // the same path AWS-side SDKs (aws-jwt-verify, jose, etc.) hit.
    let http = reqwest::Client::new();
    let jwks: serde_json::Value = http
        .get(format!(
            "{}/{}/.well-known/jwks.json",
            server.endpoint(),
            pool_id
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let jwk = jwks["keys"]
        .as_array()
        .and_then(|keys| keys.iter().find(|k| k["kid"] == kid))
        .expect("JWKS exposes the kid we signed with");

    let n_bytes = b64.decode(jwk["n"].as_str().unwrap()).expect("n decodes");
    let e_bytes = b64.decode(jwk["e"].as_str().unwrap()).expect("e decodes");
    let n = BigUint::from_bytes_be(&n_bytes);
    let e = BigUint::from_bytes_be(&e_bytes);
    let public_key = RsaPublicKey::new(n, e).expect("valid RSA public key");
    assert_eq!(
        public_key.n().bits(),
        2048,
        "Cognito mints 2048-bit keys; got {}",
        public_key.n().bits()
    );
    let verifying_key = VerifyingKey::<Sha256>::new(public_key);

    // Verify both ID and access token against the JWKS-published public key.
    for (label, token) in [("id_token", &id_token), ("access_token", &access_token)] {
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "{label} must be a three-segment JWT");
        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let sig_bytes = b64.decode(parts[2]).expect("sig decodes");
        let signature = Signature::try_from(sig_bytes.as_slice()).expect("sig parses");
        verifying_key
            .verify(signing_input.as_bytes(), &signature)
            .unwrap_or_else(|err| {
                panic!("{label} signature must verify against pool's JWKS public key: {err}")
            });
    }

    // Finally, prove the verification is non-trivial: tampering the
    // payload breaks the signature against the same public key.
    let id_parts: Vec<&str> = id_token.split('.').collect();
    let mut payload: serde_json::Value =
        serde_json::from_slice(&b64.decode(id_parts[1]).unwrap()).unwrap();
    payload["sub"] = serde_json::json!("attacker");
    let tampered_payload = b64.encode(payload.to_string().as_bytes());
    let tampered_input = format!("{}.{}", id_parts[0], tampered_payload);
    let sig_bytes = b64.decode(id_parts[2]).unwrap();
    let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();
    verifying_key
        .verify(tampered_input.as_bytes(), &signature)
        .expect_err("tampered payload must NOT verify");
}

#[tokio::test]
async fn cognito_well_known_jwks_404_for_unknown_pool() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let url = format!(
        "{}/us-east-1_NOPE99999/.well-known/jwks.json",
        server.endpoint()
    );
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn cognito_oauth2_token_refresh_grant() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-refresh-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-refresh-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("alice")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("alice")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "alice").await)
        .send()
        .await
        .expect("confirm");

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "alice")
        .auth_parameters("PASSWORD", "hunter22")
        .send()
        .await
        .expect("auth");
    let refresh_token = auth
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap()
        .to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let body =
        format!("grant_type=refresh_token&client_id={client_id}&refresh_token={refresh_token}");
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json["access_token"].as_str().unwrap().split('.').count() == 3);
    assert!(json["id_token"].as_str().unwrap().split('.').count() == 3);
    assert_eq!(json["token_type"], "Bearer");
    assert_eq!(json["expires_in"], 3600);
}

#[tokio::test]
async fn cognito_oauth2_token_client_credentials_grant() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-cc-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-cc-client")
        .generate_secret(true)
        .send()
        .await
        .expect("create client");
    let app_client = app.user_pool_client().unwrap();
    let client_id = app_client.client_id().unwrap().to_string();
    let client_secret = app_client.client_secret().unwrap().to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    // URL-encode the secret (contains '+'/'/'/'=' from base64) so form
    // decoding on the server gets the right value.
    let body = serde_urlencoded::to_string([
        ("grant_type", "client_credentials"),
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("scope", "api/read"),
    ])
    .unwrap();
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert!(json["access_token"].as_str().unwrap().split('.').count() == 3);
    assert!(json.get("id_token").is_none());
    assert!(json.get("refresh_token").is_none());
    assert_eq!(json["token_type"], "Bearer");
}

#[tokio::test]
async fn cognito_oauth2_token_invalid_client_secret() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-bad-secret-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-bad-secret-client")
        .generate_secret(true)
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let body =
        format!("grant_type=client_credentials&client_id={client_id}&client_secret=wrong-secret");
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["error"], "invalid_client");
}

#[tokio::test]
async fn cognito_oauth2_token_unsupported_grant_type() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-ugt-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-ugt-client")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let body = format!("grant_type=password&client_id={client_id}");
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["error"], "unsupported_grant_type");
}

#[tokio::test]
async fn cognito_oauth2_token_authorization_code_grant_issues_tokens() {
    // Y3: authorization_code grant exchanges a single-use code minted
    // via the admin endpoint for real RS256-signed id_token + access_token
    // + refresh_token.
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-ac-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-ac-client")
        .callback_urls("https://example.test/cb")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("alice")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("alice")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "alice").await)
        .send()
        .await
        .expect("confirm");

    let http = reqwest::Client::new();
    // Mint an authorization code via the admin endpoint (Y4 will land
    // /oauth2/authorize; the admin path is the test-only equivalent).
    let mint_url = format!(
        "{}/_fakecloud/cognito/authorization-codes",
        server.endpoint()
    );
    let mint_resp = http
        .post(&mint_url)
        .json(&serde_json::json!({
            "userPoolId": pool_id,
            "clientId": client_id,
            "username": "alice",
            "redirectUri": "https://example.test/cb",
            "scopes": ["openid", "email"],
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(mint_resp.status(), 200);
    let code = mint_resp.json::<serde_json::Value>().await.unwrap()["code"]
        .as_str()
        .unwrap()
        .to_string();

    let token_url = format!("{}/oauth2/token", server.endpoint());
    let body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("client_id", client_id.as_str()),
        ("code", code.as_str()),
        ("redirect_uri", "https://example.test/cb"),
    ])
    .unwrap();
    let resp = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let id_token = json["id_token"].as_str().unwrap();
    let access_token = json["access_token"].as_str().unwrap();
    let refresh_token = json["refresh_token"].as_str().unwrap();
    assert_eq!(id_token.split('.').count(), 3);
    assert_eq!(access_token.split('.').count(), 3);
    assert!(!refresh_token.is_empty());
    assert_eq!(json["token_type"], "Bearer");
    assert_eq!(json["expires_in"], 3600);

    // Code is single-use — the second redemption MUST fail invalid_grant.
    let body2 = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("client_id", client_id.as_str()),
        ("code", code.as_str()),
        ("redirect_uri", "https://example.test/cb"),
    ])
    .unwrap();
    let resp2 = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 400);
    assert_eq!(
        resp2.json::<serde_json::Value>().await.unwrap()["error"],
        "invalid_grant"
    );
}

#[tokio::test]
async fn cognito_oauth2_token_authorization_code_redirect_uri_mismatch() {
    // Tampering with redirect_uri at /token must invalidate the code
    // (RFC 6749 §4.1.3).
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-ac-redir-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-ac-redir-client")
        .callback_urls("https://example.test/cb")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();
    client
        .sign_up()
        .client_id(&client_id)
        .username("bob")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("bob")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "bob").await)
        .send()
        .await
        .expect("confirm");

    let http = reqwest::Client::new();
    let mint_url = format!(
        "{}/_fakecloud/cognito/authorization-codes",
        server.endpoint()
    );
    let code = http
        .post(&mint_url)
        .json(&serde_json::json!({
            "userPoolId": pool_id,
            "clientId": client_id,
            "username": "bob",
            "redirectUri": "https://example.test/cb",
            "scopes": ["openid"],
        }))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["code"]
        .as_str()
        .unwrap()
        .to_string();

    let token_url = format!("{}/oauth2/token", server.endpoint());
    let body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("client_id", client_id.as_str()),
        ("code", code.as_str()),
        ("redirect_uri", "https://attacker.test/cb"),
    ])
    .unwrap();
    let resp = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["error"],
        "invalid_grant"
    );
}

#[tokio::test]
async fn cognito_oauth2_token_authorization_code_with_pkce_s256() {
    // RFC 7636: when /authorize stored a code_challenge, /token must
    // verify the supplied code_verifier hashes back to it.
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-ac-pkce-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-ac-pkce-client")
        .callback_urls("https://example.test/cb")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();
    client
        .sign_up()
        .client_id(&client_id)
        .username("carol")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("carol")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "carol").await)
        .send()
        .await
        .expect("confirm");

    // verifier from RFC 7636 example.
    let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    let challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

    let http = reqwest::Client::new();
    let mint_url = format!(
        "{}/_fakecloud/cognito/authorization-codes",
        server.endpoint()
    );
    let code = http
        .post(&mint_url)
        .json(&serde_json::json!({
            "userPoolId": pool_id,
            "clientId": client_id,
            "username": "carol",
            "redirectUri": "https://example.test/cb",
            "scopes": ["openid"],
            "codeChallenge": challenge,
            "codeChallengeMethod": "S256",
        }))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["code"]
        .as_str()
        .unwrap()
        .to_string();

    let token_url = format!("{}/oauth2/token", server.endpoint());
    // Wrong verifier rejected.
    let bad_body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("client_id", client_id.as_str()),
        ("code", code.as_str()),
        ("redirect_uri", "https://example.test/cb"),
        ("code_verifier", "not-the-real-verifier"),
    ])
    .unwrap();
    let bad = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(bad_body)
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    assert_eq!(
        bad.json::<serde_json::Value>().await.unwrap()["error"],
        "invalid_grant"
    );

    // Mint another code (the previous one was consumed by the failed
    // attempt? No — we only consume on success). To be safe re-mint.
    let code2 = http
        .post(&mint_url)
        .json(&serde_json::json!({
            "userPoolId": pool_id,
            "clientId": client_id,
            "username": "carol",
            "redirectUri": "https://example.test/cb",
            "scopes": ["openid"],
            "codeChallenge": challenge,
            "codeChallengeMethod": "S256",
        }))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap()["code"]
        .as_str()
        .unwrap()
        .to_string();

    let good_body = serde_urlencoded::to_string([
        ("grant_type", "authorization_code"),
        ("client_id", client_id.as_str()),
        ("code", code2.as_str()),
        ("redirect_uri", "https://example.test/cb"),
        ("code_verifier", verifier),
    ])
    .unwrap();
    let good = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(good_body)
        .send()
        .await
        .unwrap();
    assert_eq!(good.status(), 200);
    let json: serde_json::Value = good.json().await.unwrap();
    assert_eq!(json["access_token"].as_str().unwrap().split('.').count(), 3);
}

#[tokio::test]
async fn cognito_oauth2_token_basic_auth_header() {
    // RFC 6749 §2.3.1 — confidential clients MAY send credentials
    // through `Authorization: Basic`. fakecloud accepts both that and
    // form-body credentials and treats Basic as authoritative.
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-basic-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-basic-client")
        .generate_secret(true)
        .send()
        .await
        .expect("create client");
    let app_client = app.user_pool_client().unwrap();
    let client_id = app_client.client_id().unwrap().to_string();
    let client_secret = app_client.client_secret().unwrap().to_string();

    use base64::Engine as _;
    let basic =
        base64::engine::general_purpose::STANDARD.encode(format!("{client_id}:{client_secret}"));

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let body = "grant_type=client_credentials&scope=api%2Fread";
    let resp = http
        .post(&url)
        .header("Authorization", format!("Basic {basic}"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["access_token"].as_str().unwrap().split('.').count(), 3);
}

#[tokio::test]
async fn cognito_oauth2_token_basic_auth_wrong_secret_401() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-basic-bad-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-basic-bad-client")
        .generate_secret(true)
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    use base64::Engine as _;
    let basic =
        base64::engine::general_purpose::STANDARD.encode(format!("{client_id}:wrong-secret"));

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let resp = http
        .post(&url)
        .header("Authorization", format!("Basic {basic}"))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body("grant_type=client_credentials")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn cognito_oauth2_token_client_credentials_no_secret_rejected() {
    // client_credentials grant requires a confidential client (one
    // with a secret). Public clients without a secret get
    // invalid_client.
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-cc-public-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-cc-public-client")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let body = format!("grant_type=client_credentials&client_id={client_id}");
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["error"],
        "invalid_client"
    );
}

#[tokio::test]
async fn cognito_oauth2_token_client_credentials_invalid_scope() {
    // Requested scope must be a subset of the app client's allowed
    // scopes; anything else gets invalid_scope.
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-cc-scope-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-cc-scope-client")
        .generate_secret(true)
        .allowed_o_auth_flows_user_pool_client(true)
        .allowed_o_auth_flows(
            aws_sdk_cognitoidentityprovider::types::OAuthFlowType::ClientCredentials,
        )
        .allowed_o_auth_scopes("api/read")
        .send()
        .await
        .expect("create client");
    let app_client = app.user_pool_client().unwrap();
    let client_id = app_client.client_id().unwrap().to_string();
    let client_secret = app_client.client_secret().unwrap().to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let body = serde_urlencoded::to_string([
        ("grant_type", "client_credentials"),
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
        ("scope", "api/admin"),
    ])
    .unwrap();
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["error"],
        "invalid_scope"
    );
}

#[tokio::test]
async fn cognito_oauth2_token_client_credentials_disallowed_flow() {
    // Even with a valid secret, a client whose AllowedOAuthFlows omits
    // client_credentials must be rejected with unauthorized_client.
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-cc-flow-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();
    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-cc-flow-client")
        .generate_secret(true)
        .allowed_o_auth_flows_user_pool_client(true)
        .allowed_o_auth_flows(aws_sdk_cognitoidentityprovider::types::OAuthFlowType::Code)
        .send()
        .await
        .expect("create client");
    let app_client = app.user_pool_client().unwrap();
    let client_id = app_client.client_id().unwrap().to_string();
    let client_secret = app_client.client_secret().unwrap().to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let body = serde_urlencoded::to_string([
        ("grant_type", "client_credentials"),
        ("client_id", client_id.as_str()),
        ("client_secret", client_secret.as_str()),
    ])
    .unwrap();
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert_eq!(
        resp.json::<serde_json::Value>().await.unwrap()["error"],
        "unauthorized_client"
    );
}

#[tokio::test]
async fn cognito_oauth2_userinfo_returns_user_attrs() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-userinfo-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-userinfo-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("bob")
        .password("hunter22")
        .user_attributes(
            aws_sdk_cognitoidentityprovider::types::AttributeType::builder()
                .name("email")
                .value("bob@example.com")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("bob")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "bob").await)
        .send()
        .await
        .expect("confirm");

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "bob")
        .auth_parameters("PASSWORD", "hunter22")
        .send()
        .await
        .expect("auth");
    let access_token = auth
        .authentication_result()
        .unwrap()
        .access_token()
        .unwrap()
        .to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/userInfo", server.endpoint());
    let resp = http
        .get(&url)
        .header("Authorization", format!("Bearer {access_token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["username"], "bob");
    assert_eq!(json["email"], "bob@example.com");
    assert!(!json["sub"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn cognito_oauth2_userinfo_invalid_token() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/userInfo", server.endpoint());
    let resp = http
        .get(&url)
        .header("Authorization", "Bearer not-a-real-token")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["error"], "invalid_token");
}

#[tokio::test]
async fn cognito_oauth2_userinfo_missing_bearer() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/userInfo", server.endpoint());
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn cognito_oauth2_revoke_refresh_token() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-revoke-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-revoke-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("carol")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("carol")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "carol").await)
        .send()
        .await
        .expect("confirm");

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "carol")
        .auth_parameters("PASSWORD", "hunter22")
        .send()
        .await
        .expect("auth");
    let refresh_token = auth
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap()
        .to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/revoke", server.endpoint());
    let body = format!("token={refresh_token}&client_id={client_id}");
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let token_url = format!("{}/oauth2/token", server.endpoint());
    let token_body =
        format!("grant_type=refresh_token&client_id={client_id}&refresh_token={refresh_token}");
    let token_resp = http
        .post(&token_url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(token_body)
        .send()
        .await
        .unwrap();
    assert_eq!(token_resp.status(), 400);
    let json: serde_json::Value = token_resp.json().await.unwrap();
    assert_eq!(json["error"], "invalid_grant");
}

#[tokio::test]
async fn cognito_oauth2_revoke_unknown_token_returns_200() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-revoke-unknown-pool")
        .send()
        .await
        .expect("create pool");
    let pool_id = pool.user_pool().unwrap().id().unwrap().to_string();

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-revoke-unknown-client")
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/revoke", server.endpoint());
    let body = format!("token=does-not-exist&client_id={client_id}");
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn cognito_oauth2_revoke_unknown_client_id_401() {
    let server = TestServer::start().await;
    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/revoke", server.endpoint());
    let body = "token=foo&client_id=nonexistent-client";
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);
    let json: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(json["error"], "invalid_client");
}

#[tokio::test]
async fn cognito_refresh_token_rotation_enabled_rotates() {
    use aws_sdk_cognitoidentityprovider::types::{FeatureType, RefreshTokenRotationType};
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("rotation-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("rotation-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .refresh_token_rotation(
            RefreshTokenRotationType::builder()
                .feature(FeatureType::Enabled)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("dave")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("dave")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "dave").await)
        .send()
        .await
        .expect("confirm");

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "dave")
        .auth_parameters("PASSWORD", "hunter22")
        .send()
        .await
        .expect("auth");
    let original_refresh = auth
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap()
        .to_string();

    let resp = client
        .get_tokens_from_refresh_token()
        .client_id(&client_id)
        .refresh_token(&original_refresh)
        .send()
        .await
        .expect("get tokens");
    let result = resp.authentication_result().unwrap();
    let new_refresh = result.refresh_token().expect("rotated refresh present");
    assert_ne!(new_refresh, original_refresh.as_str());

    let err = client
        .get_tokens_from_refresh_token()
        .client_id(&client_id)
        .refresh_token(&original_refresh)
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn cognito_refresh_token_rotation_disabled_no_new_token() {
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("no-rotation-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("no-rotation-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("eve")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("eve")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "eve").await)
        .send()
        .await
        .expect("confirm");

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "eve")
        .auth_parameters("PASSWORD", "hunter22")
        .send()
        .await
        .expect("auth");
    let original_refresh = auth
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap()
        .to_string();

    let resp = client
        .get_tokens_from_refresh_token()
        .client_id(&client_id)
        .refresh_token(&original_refresh)
        .send()
        .await
        .expect("get tokens");
    let result = resp.authentication_result().unwrap();
    assert!(result.refresh_token().is_none());

    let resp2 = client
        .get_tokens_from_refresh_token()
        .client_id(&client_id)
        .refresh_token(&original_refresh)
        .send()
        .await
        .expect("get tokens 2nd time");
    assert!(resp2.authentication_result().is_some());
}

#[tokio::test]
async fn cognito_oauth2_token_rotates_refresh_when_enabled() {
    use aws_sdk_cognitoidentityprovider::types::{FeatureType, RefreshTokenRotationType};
    let server = TestServer::start().await;
    let client = server.cognito_client().await;

    let pool = client
        .create_user_pool()
        .pool_name("oauth2-rotation-pool")
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

    let app = client
        .create_user_pool_client()
        .user_pool_id(&pool_id)
        .client_name("oauth2-rotation-client")
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowUserPasswordAuth)
        .explicit_auth_flows(ExplicitAuthFlowsType::AllowRefreshTokenAuth)
        .refresh_token_rotation(
            RefreshTokenRotationType::builder()
                .feature(FeatureType::Enabled)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create client");
    let client_id = app
        .user_pool_client()
        .unwrap()
        .client_id()
        .unwrap()
        .to_string();

    client
        .sign_up()
        .client_id(&client_id)
        .username("frank")
        .password("hunter22")
        .send()
        .await
        .expect("sign up");
    client
        .confirm_sign_up()
        .client_id(&client_id)
        .username("frank")
        .confirmation_code(fetch_confirmation_code(&server, &pool_id, "frank").await)
        .send()
        .await
        .expect("confirm");

    let auth = client
        .initiate_auth()
        .client_id(&client_id)
        .auth_flow(aws_sdk_cognitoidentityprovider::types::AuthFlowType::UserPasswordAuth)
        .auth_parameters("USERNAME", "frank")
        .auth_parameters("PASSWORD", "hunter22")
        .send()
        .await
        .expect("auth");
    let refresh_token = auth
        .authentication_result()
        .unwrap()
        .refresh_token()
        .unwrap()
        .to_string();

    let http = reqwest::Client::new();
    let url = format!("{}/oauth2/token", server.endpoint());
    let body =
        format!("grant_type=refresh_token&client_id={client_id}&refresh_token={refresh_token}");
    let resp = http
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let json: serde_json::Value = resp.json().await.unwrap();
    let rotated = json["refresh_token"].as_str().expect("rotated rt present");
    assert_ne!(rotated, refresh_token.as_str());
}
