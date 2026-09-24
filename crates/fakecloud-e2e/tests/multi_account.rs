//! End-to-end tests for multi-account isolation (#381).
//!
//! Validates that resources created in one AWS account are invisible to
//! another, and that cross-account operations (STS AssumeRole) work
//! correctly.
//!
//! Each test spawns fakecloud with SigV4 verification + IAM strict mode.
//! The `/_fakecloud/iam/create-admin` endpoint bootstraps admin users in
//! any account, solving the chicken-and-egg problem (root bypass only
//! targets the default account).

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_dynamodb::Client as DynamoClient;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sts::Client as StsClient;
use helpers::TestServer;

const ACCOUNT_A: &str = "123456789012"; // default account
const ACCOUNT_B: &str = "222222222222";

async fn start() -> TestServer {
    TestServer::start_with_env(&[
        ("FAKECLOUD_IAM", "strict"),
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
    ])
    .await
}

async fn config_with(server: &TestServer, akid: &str, secret: &str) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(akid, secret, None, None, "multi-acct"))
        .load()
        .await
}

// ======================================================================
// STS: GetCallerIdentity returns correct account after AssumeRole
// ======================================================================

#[tokio::test]
async fn sts_caller_identity_reflects_account() {
    let server = start().await;

    // Direct credentials in account B
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;
    let sts = StsClient::new(&b_cfg);
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert_eq!(identity.account().unwrap(), ACCOUNT_B);
    assert!(identity.arn().unwrap().contains(ACCOUNT_B));
}

#[tokio::test]
async fn sts_assume_role_routes_to_target_account() {
    let server = start().await;

    // Bootstrap admins in both accounts
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;

    // Create the role in account B with a trust policy that allows anyone
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;
    let iam_b = aws_sdk_iam::Client::new(&b_cfg);
    iam_b
        .create_role()
        .role_name("cross-account-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap();

    // Admin in account A assumes the role in account B
    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let sts = StsClient::new(&a_cfg);
    let role_arn = format!("arn:aws:iam::{ACCOUNT_B}:role/cross-account-role");

    let assumed = sts
        .assume_role()
        .role_arn(&role_arn)
        .role_session_name("test-session")
        .send()
        .await
        .unwrap();

    // Verify the returned credentials indicate account B
    let creds = assumed.credentials().unwrap();
    assert!(
        !creds.access_key_id().is_empty(),
        "should get valid credentials"
    );
    assert!(assumed
        .assumed_role_user()
        .unwrap()
        .arn()
        .contains(ACCOUNT_B));
}

// ======================================================================
// IAM: every entity ARN carries the account it was created in
// ======================================================================

const LIST_BUCKET_POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}"#;

/// Creates a user, a customer-managed policy, a role, and a group through
/// `iam` and asserts each ARN names `account`, and that the policy and user
/// resolve by those ARNs.
async fn assert_iam_entities_in_account(iam: &aws_sdk_iam::Client, account: &str) {
    let user = iam.create_user().user_name("alice").send().await.unwrap();
    let user_arn = user.user().unwrap().arn();
    assert_eq!(user_arn, format!("arn:aws:iam::{account}:user/alice"));

    let policy = iam
        .create_policy()
        .policy_name("list")
        .policy_document(LIST_BUCKET_POLICY)
        .send()
        .await
        .unwrap();
    let policy_arn = policy.policy().unwrap().arn().unwrap().to_string();
    assert_eq!(policy_arn, format!("arn:aws:iam::{account}:policy/list"));

    let role = iam
        .create_role()
        .role_name("svc")
        .assume_role_policy_document(r#"{"Version":"2012-10-17","Statement":[]}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(
        role.role().unwrap().arn(),
        format!("arn:aws:iam::{account}:role/svc")
    );

    let group = iam.create_group().group_name("ops").send().await.unwrap();
    assert_eq!(
        group.group().unwrap().arn(),
        format!("arn:aws:iam::{account}:group/ops")
    );

    // The policy resolves by the ARN CreatePolicy returned, and attaching it
    // by that ARN reaches the user.
    let fetched = iam
        .get_policy()
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(fetched.policy().unwrap().arn(), Some(policy_arn.as_str()));
    iam.attach_user_policy()
        .user_name("alice")
        .policy_arn(&policy_arn)
        .send()
        .await
        .unwrap();
    let attached = iam
        .list_attached_user_policies()
        .user_name("alice")
        .send()
        .await
        .unwrap();
    assert_eq!(
        attached.attached_policies()[0].policy_arn(),
        Some(policy_arn.as_str())
    );

    let fetched_user = iam.get_user().user_name("alice").send().await.unwrap();
    assert_eq!(fetched_user.user().unwrap().arn(), user_arn);
}

#[tokio::test]
async fn iam_entity_arns_use_non_default_account() {
    let server = start().await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;
    let iam_b = aws_sdk_iam::Client::new(&b_cfg);

    assert_iam_entities_in_account(&iam_b, ACCOUNT_B).await;
}

#[tokio::test]
async fn iam_entity_arns_use_assumed_role_account() {
    let server = start().await;
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;

    // An admin role in account B that account A may assume.
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;
    let iam_b = aws_sdk_iam::Client::new(&b_cfg);
    iam_b
        .create_role()
        .role_name("admin-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap();
    iam_b
        .attach_role_policy()
        .role_name("admin-role")
        .policy_arn("arn:aws:iam::aws:policy/AdministratorAccess")
        .send()
        .await
        .unwrap();

    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let assumed = StsClient::new(&a_cfg)
        .assume_role()
        .role_arn(format!("arn:aws:iam::{ACCOUNT_B}:role/admin-role"))
        .role_session_name("cross")
        .send()
        .await
        .unwrap();
    let creds = assumed.credentials().unwrap();
    let session_cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            Some(creds.session_token().to_string()),
            None,
            "multi-acct-session",
        ))
        .load()
        .await;

    assert_iam_entities_in_account(&aws_sdk_iam::Client::new(&session_cfg), ACCOUNT_B).await;
}

// ======================================================================
// SQS: queues isolated per account
// ======================================================================

#[tokio::test]
async fn sqs_queues_isolated_across_accounts() {
    let server = start().await;

    // Bootstrap admins in both accounts
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;

    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;

    let sqs_a = SqsClient::new(&a_cfg);
    let sqs_b = SqsClient::new(&b_cfg);

    // Create queue in account A
    sqs_a
        .create_queue()
        .queue_name("shared-name")
        .send()
        .await
        .unwrap();

    // List queues in account B -> should be empty
    let list = sqs_b.list_queues().send().await.unwrap();
    let urls = list.queue_urls();
    assert!(
        urls.is_empty(),
        "account B should not see account A's queues, got: {urls:?}"
    );

    // Create same-named queue in account B -> should succeed
    sqs_b
        .create_queue()
        .queue_name("shared-name")
        .send()
        .await
        .unwrap();

    // Both accounts now have 1 queue each
    let a_list = sqs_a.list_queues().send().await.unwrap();
    assert_eq!(a_list.queue_urls().len(), 1);
    let b_list = sqs_b.list_queues().send().await.unwrap();
    assert_eq!(b_list.queue_urls().len(), 1);
}

// ======================================================================
// DynamoDB: tables isolated per account
// ======================================================================

#[tokio::test]
async fn dynamodb_tables_isolated_across_accounts() {
    let server = start().await;

    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;

    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;

    let ddb_a = DynamoClient::new(&a_cfg);
    let ddb_b = DynamoClient::new(&b_cfg);

    // Create table in account A
    ddb_a
        .create_table()
        .table_name("shared-table")
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // List tables in account B -> should be empty
    let list = ddb_b.list_tables().send().await.unwrap();
    assert!(
        list.table_names().is_empty(),
        "account B should not see account A's tables"
    );

    // Create same-named table in account B -> should succeed
    ddb_b
        .create_table()
        .table_name("shared-table")
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
}

// ======================================================================
// S3: buckets isolated per account
// ======================================================================

#[tokio::test]
async fn s3_buckets_isolated_across_accounts() {
    let server = start().await;

    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;

    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;

    let s3_a = S3Client::new(&a_cfg);
    let s3_b = S3Client::new(&b_cfg);

    // Create bucket in account A
    s3_a.create_bucket()
        .bucket("account-a-bucket")
        .send()
        .await
        .unwrap();

    // List buckets in account B -> should be empty
    let list = s3_b.list_buckets().send().await.unwrap();
    assert!(
        list.buckets().is_empty(),
        "account B should not see account A's buckets"
    );

    // Create bucket in account B
    s3_b.create_bucket()
        .bucket("account-b-bucket")
        .send()
        .await
        .unwrap();

    // Account A still sees only its bucket
    let a_list = s3_a.list_buckets().send().await.unwrap();
    assert_eq!(a_list.buckets().len(), 1);
    assert_eq!(a_list.buckets()[0].name().unwrap(), "account-a-bucket");
}

// ======================================================================
// Unauthenticated requests stay in default account
// ======================================================================

#[tokio::test]
async fn unauthenticated_uses_default_account() {
    // Start without SigV4 verification so unauthenticated requests work
    let server = TestServer::start().await;

    let cfg = config_with(&server, "test", "test").await;
    let sqs = SqsClient::new(&cfg);

    sqs.create_queue()
        .queue_name("default-queue")
        .send()
        .await
        .unwrap();

    let sts = StsClient::new(&cfg);
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert_eq!(identity.account().unwrap(), ACCOUNT_A);
}
