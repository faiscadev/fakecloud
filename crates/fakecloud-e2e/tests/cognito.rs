mod helpers;
use helpers::TestServer;

use aws_sdk_cognitoidentityprovider::types::{
    AccountRecoverySettingType, PasswordPolicyType, RecoveryOptionNameType, RecoveryOptionType,
    UserPoolPolicyType,
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
            .iter()
            .any(|a| *a == aws_sdk_cognitoidentityprovider::types::VerifiedAttributeType::Email),
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
