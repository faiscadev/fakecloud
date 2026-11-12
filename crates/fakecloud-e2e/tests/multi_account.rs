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

// ======================================================================
// S3 -> EventBridge: event scoped to the bucket owner and bucket region
// ======================================================================

async fn config_in_region(
    server: &TestServer,
    akid: &str,
    secret: &str,
    region: &'static str,
) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new(region))
        .credentials_provider(Credentials::new(akid, secret, None, None, "multi-acct"))
        .load()
        .await
}

/// A bucket in a non-default account and region publishes its EventBridge
/// notification to that account's default bus, stamped with that account and
/// region: a rule there matching on both fires.
#[tokio::test]
async fn s3_eventbridge_event_reaches_bucket_owner_rule_in_bucket_region() {
    let server = start().await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;
    let cfg = config_in_region(&server, &b_akid, &b_secret, "eu-west-2").await;
    let s3 = S3Client::new(&cfg);
    let sqs = SqsClient::new(&cfg);
    let eb = aws_sdk_eventbridge::Client::new(&cfg);

    let queue_url = sqs
        .create_queue()
        .queue_name("s3-events")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let queue_arn = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    eb.put_rule()
        .name("owner-s3")
        .event_pattern(format!(
            r#"{{"source":["aws.s3"],"detail-type":["Object Created"],"account":["{ACCOUNT_B}"],"region":["eu-west-2"]}}"#
        ))
        .send()
        .await
        .unwrap();
    eb.put_targets()
        .rule("owner-s3")
        .targets(
            aws_sdk_eventbridge::types::Target::builder()
                .id("q")
                .arn(&queue_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    s3.create_bucket()
        .bucket("owner-b-events")
        .create_bucket_configuration(
            aws_sdk_s3::types::CreateBucketConfiguration::builder()
                .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::EuWest2)
                .build(),
        )
        .send()
        .await
        .unwrap();
    s3.put_bucket_notification_configuration()
        .bucket("owner-b-events")
        .notification_configuration(
            aws_sdk_s3::types::NotificationConfiguration::builder()
                .event_bridge_configuration(
                    aws_sdk_s3::types::EventBridgeConfiguration::builder().build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    s3.put_object()
        .bucket("owner-b-events")
        .key("hello.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hi"))
        .send()
        .await
        .unwrap();

    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .wait_time_seconds(10)
        .send()
        .await
        .unwrap();
    let msgs = msgs.messages();
    assert_eq!(
        msgs.len(),
        1,
        "rule in the bucket owner's account must fire"
    );
    let event: serde_json::Value = serde_json::from_str(msgs[0].body().unwrap()).unwrap();
    assert_eq!(event["account"], ACCOUNT_B);
    assert_eq!(event["region"], "eu-west-2");
    assert_eq!(event["detail"]["bucket"]["name"], "owner-b-events");
    assert_eq!(
        event["resources"],
        serde_json::json!(["arn:aws:s3:::owner-b-events"])
    );
}

// ======================================================================
// S3 replication into a bucket owned by another account
// ======================================================================

/// Account A's versioned `src` bucket replicating everything into account
/// B's versioned `dst` bucket (with ownership translation), B's bucket policy
/// set to `dst_policy`. Returns both clients.
async fn setup_cross_account_replication(
    server: &TestServer,
    src: &'static str,
    dst: &'static str,
    dst_policy: Option<String>,
) -> (S3Client, S3Client) {
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;
    let s3_a = S3Client::new(&config_with(server, &a_akid, &a_secret).await);
    let s3_b = S3Client::new(&config_with(server, &b_akid, &b_secret).await);

    for (s3, bucket) in [(&s3_a, src), (&s3_b, dst)] {
        s3.create_bucket().bucket(bucket).send().await.unwrap();
        s3.put_bucket_versioning()
            .bucket(bucket)
            .versioning_configuration(
                aws_sdk_s3::types::VersioningConfiguration::builder()
                    .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                    .build(),
            )
            .send()
            .await
            .unwrap();
    }
    if let Some(policy) = dst_policy {
        s3_b.put_bucket_policy()
            .bucket(dst)
            .policy(policy)
            .send()
            .await
            .unwrap();
    }

    let rule = aws_sdk_s3::types::ReplicationRule::builder()
        .id("to-b")
        .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(
            aws_sdk_s3::types::ReplicationRuleFilter::builder()
                .prefix("")
                .build(),
        )
        .delete_marker_replication(
            aws_sdk_s3::types::DeleteMarkerReplication::builder()
                .status(aws_sdk_s3::types::DeleteMarkerReplicationStatus::Disabled)
                .build(),
        )
        .destination(
            aws_sdk_s3::types::Destination::builder()
                .bucket(format!("arn:aws:s3:::{dst}"))
                .account(ACCOUNT_B)
                .access_control_translation(
                    aws_sdk_s3::types::AccessControlTranslation::builder()
                        .owner(aws_sdk_s3::types::OwnerOverride::Destination)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    s3_a.put_bucket_replication()
        .bucket(src)
        .replication_configuration(
            aws_sdk_s3::types::ReplicationConfiguration::builder()
                .role(REPLICATION_ROLE)
                .rules(rule)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    s3_a.put_object()
        .bucket(src)
        .key("doc.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(
            b"replicated",
        ))
        .send()
        .await
        .unwrap();
    (s3_a, s3_b)
}

const REPLICATION_ROLE: &str = "arn:aws:iam::123456789012:role/replication";

/// Account B's bucket policy grants account A's replication role the
/// replication actions, so the replica lands in B's bucket (as REPLICA) and
/// the source reports COMPLETED.
#[tokio::test]
async fn s3_replication_delivers_to_bucket_in_another_account() {
    let server = start().await;
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{REPLICATION_ROLE}"}},"Action":["s3:ReplicateObject","s3:ReplicateDelete","s3:ObjectOwnerOverrideToBucketOwner"],"Resource":"arn:aws:s3:::repl-dst-b/*"}}]}}"#
    );
    let (s3_a, s3_b) =
        setup_cross_account_replication(&server, "repl-src-a", "repl-dst-b", Some(policy)).await;

    let got = s3_b
        .get_object()
        .bucket("repl-dst-b")
        .key("doc.txt")
        .send()
        .await
        .expect("replica must land in account B's bucket");
    assert_eq!(
        got.replication_status(),
        Some(&aws_sdk_s3::types::ReplicationStatus::Replica)
    );
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"replicated");

    let head = s3_a
        .head_object()
        .bucket("repl-src-a")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.replication_status(),
        Some(&aws_sdk_s3::types::ReplicationStatus::Completed)
    );
}

/// Without a bucket policy granting the replication role, account B's bucket
/// refuses the replica (strict IAM): nothing lands there and the source
/// reports FAILED.
#[tokio::test]
async fn s3_replication_to_another_account_requires_bucket_policy() {
    let server = start().await;
    let (s3_a, s3_b) =
        setup_cross_account_replication(&server, "repl-src-deny", "repl-dst-deny", None).await;

    let missing = s3_b
        .get_object()
        .bucket("repl-dst-deny")
        .key("doc.txt")
        .send()
        .await;
    assert!(missing.is_err(), "no replica without a bucket policy grant");

    let head = s3_a
        .head_object()
        .bucket("repl-src-deny")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.replication_status(),
        Some(&aws_sdk_s3::types::ReplicationStatus::Failed)
    );
}

// ======================================================================
// KMS: a resource in one account encrypted under another account's key
// ======================================================================

/// A customer key in account B whose key policy grants account A (or, with
/// `grant_a` false, only B itself). Returns the key ARN.
async fn key_in_b(kms_b: &aws_sdk_kms::Client, grant_a: bool) -> String {
    let grantee = if grant_a { ACCOUNT_A } else { ACCOUNT_B };
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[
            {{"Sid":"Owner","Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{ACCOUNT_B}:root"}},"Action":"kms:*","Resource":"*"}},
            {{"Sid":"Use","Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{grantee}:root"}},"Action":["kms:Encrypt","kms:Decrypt","kms:GenerateDataKey*","kms:DescribeKey"],"Resource":"*"}}]}}"#
    );
    kms_b
        .create_key()
        .policy(policy)
        .send()
        .await
        .unwrap()
        .key_metadata
        .unwrap()
        .arn
        .unwrap()
}

/// An SQS queue and a secret in account A can use a customer key of account
/// B by its ARN when B's key policy grants A (the values round-trip); a key
/// whose policy grants only B refuses A, and nothing is stored.
#[tokio::test]
async fn resources_use_another_accounts_key_when_its_policy_allows() {
    let server = start().await;
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;
    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;
    let kms_b = aws_sdk_kms::Client::new(&b_cfg);
    let shared = key_in_b(&kms_b, true).await;
    let private = key_in_b(&kms_b, false).await;
    assert!(shared.contains(&format!(":{ACCOUNT_B}:key/")), "{shared}");

    let sqs = SqsClient::new(&a_cfg);
    let mut urls = Vec::new();
    for (name, key) in [("cross-ok", &shared), ("cross-refused", &private)] {
        urls.push(
            sqs.create_queue()
                .queue_name(name)
                .attributes(aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId, key)
                .send()
                .await
                .unwrap()
                .queue_url
                .unwrap(),
        );
    }
    sqs.send_message()
        .queue_url(&urls[0])
        .message_body("hello")
        .send()
        .await
        .unwrap();
    let got = sqs
        .receive_message()
        .queue_url(&urls[0])
        .send()
        .await
        .unwrap();
    assert_eq!(got.messages()[0].body(), Some("hello"));
    let err = sqs
        .send_message()
        .queue_url(&urls[1])
        .message_body("hello")
        .send()
        .await
        .expect_err("a key whose policy does not grant A refuses it");
    assert_eq!(
        aws_sdk_sqs::error::ProvideErrorMetadata::code(&err),
        Some("KMS.AccessDeniedException"),
        "{err:?}"
    );

    let secrets = aws_sdk_secretsmanager::Client::new(&a_cfg);
    secrets
        .create_secret()
        .name("cross")
        .kms_key_id(&shared)
        .secret_string("s3cret")
        .send()
        .await
        .unwrap();
    let value = secrets
        .get_secret_value()
        .secret_id("cross")
        .send()
        .await
        .unwrap();
    assert_eq!(value.secret_string(), Some("s3cret"));
    let err = secrets
        .create_secret()
        .name("cross-refused")
        .kms_key_id(&private)
        .secret_string("s3cret")
        .send()
        .await
        .expect_err("a key whose policy does not grant A refuses it");
    assert_eq!(
        aws_sdk_secretsmanager::error::ProvideErrorMetadata::code(&err),
        Some("AccessDeniedException"),
        "{err:?}"
    );
    assert!(secrets
        .describe_secret()
        .secret_id("cross-refused")
        .send()
        .await
        .is_err());
}

/// PutObject with SSE-KMS under `key` into `bucket`, then GetObject; returns
/// the body read back.
async fn s3_kms_round_trip(s3: &S3Client, bucket: &str, object: &str, key: &str) -> Vec<u8> {
    s3.put_object()
        .bucket(bucket)
        .key(object)
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
        .ssekms_key_id(key)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send()
        .await
        .unwrap();
    let got = s3
        .get_object()
        .bucket(bucket)
        .key(object)
        .send()
        .await
        .unwrap();
    assert_eq!(got.ssekms_key_id(), Some(key));
    got.body.collect().await.unwrap().into_bytes().to_vec()
}

/// An object in account A's bucket can be encrypted under a customer key of
/// account B by its ARN when B's key policy grants A, or when B gives A's
/// identity a KMS grant for the operations; a key that grants only B refuses
/// A's write with AccessDenied, and nothing is stored.
#[tokio::test]
async fn s3_sse_kms_uses_another_accounts_key_when_its_policy_or_a_grant_allows() {
    let server = start().await;
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;
    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;
    let kms_b = aws_sdk_kms::Client::new(&b_cfg);
    let shared = key_in_b(&kms_b, true).await;
    let private = key_in_b(&kms_b, false).await;

    let s3 = S3Client::new(&a_cfg);
    s3.create_bucket().bucket("a-bucket").send().await.unwrap();
    assert_eq!(
        s3_kms_round_trip(&s3, "a-bucket", "shared.txt", &shared).await,
        b"payload"
    );

    let err = s3
        .put_object()
        .bucket("a-bucket")
        .key("private.txt")
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
        .ssekms_key_id(&private)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send()
        .await
        .expect_err("a key whose policy grants only B refuses A");
    assert_eq!(
        aws_sdk_s3::error::ProvideErrorMetadata::code(&err),
        Some("AccessDenied"),
        "{err:?}"
    );
    assert!(s3
        .head_object()
        .bucket("a-bucket")
        .key("private.txt")
        .send()
        .await
        .is_err());

    // B grants A's admin user the data-key operations on the same key.
    let a_arn = StsClient::new(&a_cfg)
        .get_caller_identity()
        .send()
        .await
        .unwrap()
        .arn
        .unwrap();
    kms_b
        .create_grant()
        .key_id(&private)
        .grantee_principal(&a_arn)
        .operations(aws_sdk_kms::types::GrantOperation::GenerateDataKey)
        .operations(aws_sdk_kms::types::GrantOperation::Decrypt)
        .send()
        .await
        .unwrap();
    assert_eq!(
        s3_kms_round_trip(&s3, "a-bucket", "private.txt", &private).await,
        b"payload"
    );
}
