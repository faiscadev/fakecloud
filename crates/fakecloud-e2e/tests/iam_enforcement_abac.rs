//! End-to-end tests for ABAC (attribute-based access control) tag conditions.
//!
//! Tests `aws:PrincipalTag/<key>`, `aws:ResourceTag/<key>`,
//! `aws:RequestTag/<key>`, and `aws:TagKeys` condition evaluation with
//! `FAKECLOUD_IAM=strict`.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_iam::Client as IamClient;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SqsClient;
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
        .credentials_provider(Credentials::new(akid, secret, None, None, "fakecloud-abac"))
        .load()
        .await
}

async fn bootstrap_tagged_user(
    server: &TestServer,
    name: &str,
    tags: &[(&str, &str)],
) -> (String, String) {
    let boot = sdk_config_with(server, "test", "test").await;
    let iam = IamClient::new(&boot);
    let mut req = iam.create_user().user_name(name);
    for (k, v) in tags {
        req = req.tags(
            aws_sdk_iam::types::Tag::builder()
                .key(*k)
                .value(*v)
                .build()
                .unwrap(),
        );
    }
    req.send().await.unwrap();
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

async fn attach_inline_policy(server: &TestServer, user: &str, name: &str, document: &str) {
    let boot = sdk_config_with(server, "test", "test").await;
    IamClient::new(&boot)
        .put_user_policy()
        .user_name(user)
        .policy_name(name)
        .policy_document(document)
        .send()
        .await
        .unwrap();
}

// ======================================================================
// aws:PrincipalTag tests
// ======================================================================

#[tokio::test]
async fn principal_tag_condition_allows_matching_user() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_tagged_user(&server, "alice", &[("Team", "platform")]).await;

    // Policy: allow sts:GetCallerIdentity only if PrincipalTag/Team == platform
    attach_inline_policy(
        &server,
        "alice",
        "AllowIfPlatform",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"StringEquals":{"aws:PrincipalTag/Team":"platform"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert!(identity.arn().unwrap().contains("user/alice"));
}

#[tokio::test]
async fn principal_tag_condition_denies_non_matching_user() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_tagged_user(&server, "bob", &[("Team", "backend")]).await;

    // Policy: allow sts:GetCallerIdentity only if PrincipalTag/Team == platform
    attach_inline_policy(
        &server,
        "bob",
        "AllowIfPlatform",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"StringEquals":{"aws:PrincipalTag/Team":"platform"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    assert!(
        format!("{err:?}").contains(r#"code: Some("AccessDenied")"#),
        "expected STS's AccessDenied"
    );
}

#[tokio::test]
async fn principal_tag_condition_denies_user_with_no_tags() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_tagged_user(&server, "carol", &[]).await;

    // Policy requires PrincipalTag/Team == platform, but carol has no tags
    attach_inline_policy(
        &server,
        "carol",
        "AllowIfPlatform",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"StringEquals":{"aws:PrincipalTag/Team":"platform"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    assert!(
        format!("{err:?}").contains(r#"code: Some("AccessDenied")"#),
        "expected STS's AccessDenied"
    );
}

#[tokio::test]
async fn principal_tag_case_sensitive_key() {
    let server = start_strict().await;
    // User has tag "team" (lowercase), but policy checks "Team" (titlecase)
    let (akid, secret) = bootstrap_tagged_user(&server, "dave", &[("team", "platform")]).await;

    attach_inline_policy(
        &server,
        "dave",
        "AllowIfPlatform",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"StringEquals":{"aws:PrincipalTag/Team":"platform"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    // Tag key mismatch: "team" != "Team" -> condition fails -> denied
    let err = sts.get_caller_identity().send().await.unwrap_err();
    assert!(
        format!("{err:?}").contains(r#"code: Some("AccessDenied")"#),
        "expected STS's AccessDenied: tag keys are case-sensitive"
    );
}

// ======================================================================
// aws:ResourceTag tests (S3)
// ======================================================================

#[tokio::test]
async fn s3_resource_tag_allows_when_object_tag_matches() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_tagged_user(&server, "s3alice", &[]).await;

    // Allow all S3, but deny GetObject unless ResourceTag/Environment == dev
    attach_inline_policy(
        &server,
        "s3alice",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:*","Resource":"*"}
        ]}"#,
    )
    .await;
    attach_inline_policy(
        &server,
        "s3alice",
        "DenyGetUnlessDev",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny",
            "Action":"s3:GetObject",
            "Resource":"arn:aws:s3:::test-bucket/*",
            "Condition":{"StringNotEquals":{"aws:ResourceTag/Environment":"dev"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = S3Client::new(&cfg);

    // Create bucket and put an object with tags
    let boot_cfg = sdk_config_with(&server, "test", "test").await;
    let boot_s3 = S3Client::new(&boot_cfg);
    boot_s3
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    // Put object with Environment=dev tag
    s3.put_object()
        .bucket("test-bucket")
        .key("dev-file.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
        .tagging("Environment=dev")
        .send()
        .await
        .unwrap();

    // Put object with Environment=prod tag
    s3.put_object()
        .bucket("test-bucket")
        .key("prod-file.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hello"))
        .tagging("Environment=prod")
        .send()
        .await
        .unwrap();

    // GetObject on dev file: allowed (ResourceTag/Environment == dev)
    s3.get_object()
        .bucket("test-bucket")
        .key("dev-file.txt")
        .send()
        .await
        .unwrap();

    // GetObject on prod file: denied (ResourceTag/Environment != dev)
    let err = s3
        .get_object()
        .bucket("test-bucket")
        .key("prod-file.txt")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for prod-tagged object, got {err:?}"
    );
}

// ======================================================================
// aws:RequestTag tests (S3)
// ======================================================================

#[tokio::test]
async fn s3_request_tag_denies_put_with_wrong_tag() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_tagged_user(&server, "s3bob", &[]).await;

    // Allow all S3, but deny PutObject unless RequestTag/CostCenter == 123
    attach_inline_policy(
        &server,
        "s3bob",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:*","Resource":"*"}
        ]}"#,
    )
    .await;
    attach_inline_policy(
        &server,
        "s3bob",
        "DenyPutUnlessCC",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny",
            "Action":"s3:PutObject",
            "Resource":"arn:aws:s3:::tag-bucket/*",
            "Condition":{"StringNotEquals":{"aws:RequestTag/CostCenter":"123"}}
        }]}"#,
    )
    .await;

    let boot_cfg = sdk_config_with(&server, "test", "test").await;
    let boot_s3 = S3Client::new(&boot_cfg);
    boot_s3
        .create_bucket()
        .bucket("tag-bucket")
        .send()
        .await
        .unwrap();

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = S3Client::new(&cfg);

    // PutObject with CostCenter=123: allowed
    s3.put_object()
        .bucket("tag-bucket")
        .key("good.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"ok"))
        .tagging("CostCenter=123")
        .send()
        .await
        .unwrap();

    // PutObject with CostCenter=999: denied
    let err = s3
        .put_object()
        .bucket("tag-bucket")
        .key("bad.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"no"))
        .tagging("CostCenter=999")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for wrong CostCenter"
    );
}

// ======================================================================
// aws:TagKeys tests (S3)
// ======================================================================

#[tokio::test]
async fn s3_tag_keys_for_all_values_restricts_allowed_keys() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_tagged_user(&server, "s3carol", &[]).await;

    // Allow all S3, but deny PutObject with tag keys outside the allowed set
    attach_inline_policy(
        &server,
        "s3carol",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:*","Resource":"*"}
        ]}"#,
    )
    .await;
    attach_inline_policy(
        &server,
        "s3carol",
        "DenyBadTagKeys",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny",
            "Action":"s3:PutObject",
            "Resource":"arn:aws:s3:::keys-bucket/*",
            "Condition":{"ForAnyValue:StringNotEquals":{"aws:TagKeys":["Environment","Team"]}}
        }]}"#,
    )
    .await;

    let boot_cfg = sdk_config_with(&server, "test", "test").await;
    let boot_s3 = S3Client::new(&boot_cfg);
    boot_s3
        .create_bucket()
        .bucket("keys-bucket")
        .send()
        .await
        .unwrap();

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = S3Client::new(&cfg);

    // PutObject with allowed keys only: succeeds
    s3.put_object()
        .bucket("keys-bucket")
        .key("ok.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"ok"))
        .tagging("Environment=dev&Team=platform")
        .send()
        .await
        .unwrap();

    // PutObject with disallowed key "Secret": denied
    let err = s3
        .put_object()
        .bucket("keys-bucket")
        .key("bad.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"no"))
        .tagging("Environment=dev&Secret=classified")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for disallowed tag key"
    );
}

// ======================================================================
// SQS ABAC tests
// ======================================================================

#[tokio::test]
async fn sqs_resource_tag_denies_when_queue_tag_mismatches() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_tagged_user(&server, "sqsuser", &[]).await;

    // Allow all SQS, deny SendMessage unless ResourceTag/Env == dev
    attach_inline_policy(
        &server,
        "sqsuser",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"sqs:*","Resource":"*"}
        ]}"#,
    )
    .await;
    attach_inline_policy(
        &server,
        "sqsuser",
        "DenySendUnlessDev",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny",
            "Action":"sqs:SendMessage",
            "Resource":"*",
            "Condition":{"StringNotEquals":{"aws:ResourceTag/Env":"dev"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sqs = SqsClient::new(&cfg);

    // Create a queue tagged Env=dev
    let dev_queue = sqs
        .create_queue()
        .queue_name("dev-queue")
        .tags("Env", "dev")
        .send()
        .await
        .unwrap();
    let dev_url = dev_queue.queue_url().unwrap();

    // Create a queue tagged Env=prod
    let prod_queue = sqs
        .create_queue()
        .queue_name("prod-queue")
        .tags("Env", "prod")
        .send()
        .await
        .unwrap();
    let prod_url = prod_queue.queue_url().unwrap();

    // SendMessage to dev queue: allowed
    sqs.send_message()
        .queue_url(dev_url)
        .message_body("hello dev")
        .send()
        .await
        .unwrap();

    // SendMessage to prod queue: denied
    let err = sqs
        .send_message()
        .queue_url(prod_url)
        .message_body("hello prod")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for prod-tagged queue"
    );
}

// ======================================================================
// IAM resource tag ABAC tests
// ======================================================================

#[tokio::test]
async fn iam_resource_tag_denies_get_user_without_matching_tag() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_tagged_user(&server, "iamadmin", &[]).await;

    // Allow all IAM, deny GetUser unless ResourceTag/Team == ops
    attach_inline_policy(
        &server,
        "iamadmin",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"iam:*","Resource":"*"}
        ]}"#,
    )
    .await;
    attach_inline_policy(
        &server,
        "iamadmin",
        "DenyGetUserUnlessOps",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny",
            "Action":"iam:GetUser",
            "Resource":"*",
            "Condition":{"StringNotEquals":{"aws:ResourceTag/Team":"ops"}}
        }]}"#,
    )
    .await;

    // Create two users: one with Team=ops, one with Team=dev
    let boot_cfg = sdk_config_with(&server, "test", "test").await;
    let boot_iam = IamClient::new(&boot_cfg);
    boot_iam
        .create_user()
        .user_name("ops-user")
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("Team")
                .value("ops")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    boot_iam
        .create_user()
        .user_name("dev-user")
        .tags(
            aws_sdk_iam::types::Tag::builder()
                .key("Team")
                .value("dev")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let iam = IamClient::new(&cfg);

    // GetUser ops-user: allowed (Team=ops matches)
    iam.get_user().user_name("ops-user").send().await.unwrap();

    // GetUser dev-user: denied (Team=dev != ops)
    let err = iam
        .get_user()
        .user_name("dev-user")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for dev-tagged user"
    );
}

// ======================================================================
// Policy variables
// ======================================================================

fn s3_for(cfg: &aws_config::SdkConfig) -> S3Client {
    S3Client::from_conf(
        aws_sdk_s3::config::Builder::from(cfg)
            .force_path_style(true)
            .build(),
    )
}

/// `${aws:username}` in a Resource and `${aws:PrincipalTag/team}` in a
/// condition are replaced per caller, so one policy scopes each user to their
/// own prefix and team.
#[tokio::test]
async fn policy_variables_scope_one_policy_per_caller() {
    let server = start_strict().await;
    let admin = s3_for(&sdk_config_with(&server, "test", "test").await);
    admin.create_bucket().bucket("homes").send().await.unwrap();
    for key in [
        "alice/doc.txt",
        "bob/doc.txt",
        "blue/plan.txt",
        "red/plan.txt",
    ] {
        admin
            .put_object()
            .bucket("homes")
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::homes/${aws:username}/*"
            },
            {
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::homes/*",
                "Condition": {"StringLike": {"s3:ExistingObjectTag/none": "x"}}
            },
            {
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::homes/${aws:PrincipalTag/team, 'no-team'}/*"
            }
        ]
    })
    .to_string();
    let (akid, secret) = bootstrap_tagged_user(&server, "alice", &[("team", "blue")]).await;
    attach_inline_policy(&server, "alice", "homes", &policy).await;
    let alice = s3_for(&sdk_config_with(&server, &akid, &secret).await);

    let get = |key: &'static str| alice.get_object().bucket("homes").key(key).send();
    get("alice/doc.txt").await.expect("own username prefix");
    get("blue/plan.txt").await.expect("own team prefix");
    let err = get("bob/doc.txt").await.expect_err("another user's prefix");
    assert!(format!("{err:?}").contains("AccessDenied"), "{err:?}");
    let err = get("red/plan.txt")
        .await
        .expect_err("another team's prefix");
    assert!(format!("{err:?}").contains("AccessDenied"), "{err:?}");
}
