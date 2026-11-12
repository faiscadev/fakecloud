//! End-to-end tests for opt-in IAM identity-policy enforcement on IAM + STS.
//!
//! Each test spawns fakecloud with `FAKECLOUD_IAM=strict`, bootstraps a
//! user via the reserved `test*` root-bypass credentials, attaches an
//! inline policy, then signs a follow-up request with the user's own
//! access key to observe Allow / Deny. Batch 7 wires IAM + STS first;
//! batches 7b/8 roll out the rest.

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

async fn start_soft() -> TestServer {
    TestServer::start_with_env(&[("FAKECLOUD_IAM", "soft")]).await
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
            "fakecloud-iam-enf",
        ))
        .load()
        .await
}

/// Bootstrap a user via root-bypass credentials and return their
/// freshly-created (access_key_id, secret_access_key) plus an
/// `aws-sdk-iam` client signed with those credentials.
async fn bootstrap_user(server: &TestServer, name: &str) -> (String, String) {
    let boot = sdk_config_with(server, "test", "test").await;
    let iam_boot = IamClient::new(&boot);
    iam_boot.create_user().user_name(name).send().await.unwrap();
    let ak = iam_boot
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
// STS tests
// ======================================================================

#[tokio::test]
async fn sts_get_caller_identity_denied_without_policy() {
    // A user with no attached policies has implicit-deny on STS actions
    // in strict mode. GetCallerIdentity with their credentials must fail.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "alice").await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains(r#"code: Some("AccessDenied")"#),
        "expected STS's AccessDenied, got {msg}"
    );
}

/// bug-hunt 2026-06-13, finding 5.1: enabling `--verify-sigv4` alongside
/// `--iam` binds a request to a real identity -- an unknown access key id is
/// rejected with InvalidClientTokenId before any handler runs. (Without
/// `--verify-sigv4`, strict mode still rejects a key that resolves to no
/// identity; see `unresolved_access_key_rejected_under_strict_without_sigv4`.
/// What only signature verification adds is proof the caller holds the
/// secret.)
#[tokio::test]
async fn unknown_access_key_rejected_when_sigv4_verification_enabled() {
    // strict IAM *with* SigV4 verification — the secure configuration.
    let server = TestServer::start_with_env(&[
        ("FAKECLOUD_IAM", "strict"),
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
    ])
    .await;

    let cfg = sdk_config_with(&server, "AKIABOGUS000000000000", "bogus-secret").await;
    let iam = IamClient::new(&cfg);
    let err = iam
        .list_users()
        .send()
        .await
        .expect_err("an unknown access key must be rejected when SigV4 is verified");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("InvalidClientTokenId"),
        "expected InvalidClientTokenId for an unknown access key, got {msg}"
    );
}

/// Bug audit 2026-10-01: under `--iam strict` without `--verify-sigv4`, a key
/// that resolves to no identity (unknown, or a deactivated user key) used to
/// skip every check -- and AssumeRole then read the missing principal as the
/// account root and minted real credentials. Strict now rejects it the way
/// AWS does, while the reserved `test*` root identity stays exempt.
#[tokio::test]
async fn unresolved_access_key_rejected_under_strict_without_sigv4() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let iam_boot = IamClient::new(&boot);
    iam_boot
        .create_role()
        .role_name("open-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap();

    // Unknown key: rejected on an enforced service, and AssumeRole mints
    // nothing.
    let bogus = sdk_config_with(&server, "AKIABOGUS000000000000", "bogus-secret").await;
    let err = IamClient::new(&bogus)
        .list_users()
        .send()
        .await
        .expect_err("unknown key must be rejected under strict");
    assert!(
        format!("{err:?}").contains("InvalidClientTokenId"),
        "{err:?}"
    );
    let err = StsClient::new(&bogus)
        .assume_role()
        .role_arn("arn:aws:iam::123456789012:role/open-role")
        .role_session_name("s")
        .send()
        .await
        .expect_err("an unresolved caller must not be treated as root");
    assert!(
        format!("{err:?}").contains("InvalidClientTokenId"),
        "{err:?}"
    );

    // A deactivated user key is rejected the same way.
    let (akid, secret) = bootstrap_user(&server, "inactive-user").await;
    attach_inline_policy(
        &server,
        "inactive-user",
        "all",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
    )
    .await;
    let user_cfg = sdk_config_with(&server, &akid, &secret).await;
    IamClient::new(&user_cfg)
        .list_users()
        .send()
        .await
        .expect("active key with an allow-all policy works");
    iam_boot
        .update_access_key()
        .user_name("inactive-user")
        .access_key_id(&akid)
        .status(aws_sdk_iam::types::StatusType::Inactive)
        .send()
        .await
        .unwrap();
    let err = IamClient::new(&user_cfg)
        .list_users()
        .send()
        .await
        .expect_err("an Inactive key must not authenticate");
    assert!(
        format!("{err:?}").contains("InvalidClientTokenId"),
        "{err:?}"
    );

    // The reserved root identity is still exempt.
    iam_boot.list_users().send().await.unwrap();
}

/// A `Deny` guarded by a negated operator applies when the request carries no
/// value for the key at all (AWS: `StringNotEquals` on an absent key is
/// true). Before the fix the missing key made the condition false and the
/// Deny was skipped.
#[tokio::test]
async fn deny_with_negated_condition_applies_when_key_absent() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "vpce-user").await;
    attach_inline_policy(
        &server,
        "vpce-user",
        "allow-but-only-via-vpce",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"iam:ListUsers","Resource":"*"},
            {"Effect":"Deny","Action":"*","Resource":"*",
             "Condition":{"StringNotEquals":{"aws:SourceVpce":"vpce-0123456789abcdef0"}}}
        ]}"#,
    )
    .await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let err = IamClient::new(&cfg)
        .list_users()
        .send()
        .await
        .expect_err("no aws:SourceVpce in the request: the Deny must apply");
    assert!(format!("{err:?}").contains("AccessDenied"), "{err:?}");
}

fn path_style_s3(cfg: &aws_config::SdkConfig) -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(cfg)
            .force_path_style(true)
            .build(),
    )
}

/// The AWS-documented `DenyIncorrectEncryptionHeader` bucket policy. Its
/// `StringNotEquals` is true when the header is absent (as on AWS), so it
/// only works if fakecloud populates `s3:x-amz-server-side-encryption` from
/// the request: a compliant PutObject must succeed, a plain one is denied.
#[tokio::test]
async fn deny_incorrect_encryption_header_allows_compliant_put() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let root_s3 = path_style_s3(&boot);
    root_s3
        .create_bucket()
        .bucket("enc-bucket")
        .send()
        .await
        .unwrap();
    root_s3
        .put_bucket_policy()
        .bucket("enc-bucket")
        .policy(
            r#"{"Version":"2012-10-17","Statement":[{"Sid":"DenyIncorrectEncryptionHeader",
                "Effect":"Deny","Principal":"*","Action":"s3:PutObject",
                "Resource":"arn:aws:s3:::enc-bucket/*",
                "Condition":{"StringNotEquals":{"s3:x-amz-server-side-encryption":"AES256"}}}]}"#,
        )
        .send()
        .await
        .unwrap();
    let (akid, secret) = bootstrap_user(&server, "writer").await;
    attach_inline_policy(
        &server,
        "writer",
        "s3-all",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    s3.put_object()
        .bucket("enc-bucket")
        .key("ok.txt")
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send()
        .await
        .expect("PutObject with x-amz-server-side-encryption: AES256 is compliant");
    let err = s3
        .put_object()
        .bucket("enc-bucket")
        .key("plain.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send()
        .await
        .expect_err("PutObject without the header is denied, as on AWS");
    assert!(format!("{err:?}").contains("AccessDenied"), "{err:?}");
}

/// A resource-perimeter guardrail (`Deny` unless `aws:ResourceAccount` is the
/// caller's own account) must not block requests to the caller's own
/// resources: fakecloud populates `aws:ResourceAccount`.
#[tokio::test]
async fn resource_account_perimeter_allows_own_account_requests() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    path_style_s3(&boot)
        .create_bucket()
        .bucket("own-bucket")
        .send()
        .await
        .unwrap();
    let (akid, secret) = bootstrap_user(&server, "perimeter").await;
    attach_inline_policy(
        &server,
        "perimeter",
        "perimeter",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:*","Resource":"*"},
            {"Effect":"Deny","Action":"s3:*","Resource":"*",
             "Condition":{"StringNotEquals":{"aws:ResourceAccount":"123456789012"}}}
        ]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    s3.put_object()
        .bucket("own-bucket")
        .key("k")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send()
        .await
        .expect("own-account bucket is inside the perimeter");
    s3.list_objects_v2()
        .bucket("own-bucket")
        .send()
        .await
        .expect("own-account ListObjectsV2 is inside the perimeter");
}

#[tokio::test]
async fn sts_get_caller_identity_allowed_with_explicit_policy() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "bob").await;
    attach_inline_policy(
        &server,
        "bob",
        "AllowGetCallerIdentity",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sts:GetCallerIdentity","Resource":"*"}]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert!(identity.arn().unwrap().contains("user/bob"));
}

#[tokio::test]
async fn sts_explicit_deny_beats_allow_all() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "carol").await;
    attach_inline_policy(
        &server,
        "carol",
        "AllowAll",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
    )
    .await;
    attach_inline_policy(
        &server,
        "carol",
        "DenySts",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"sts:GetCallerIdentity","Resource":"*"}]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains(r#"code: Some("AccessDenied")"#), "got {msg}");
}

#[tokio::test]
async fn root_bypass_skips_enforcement() {
    // The reserved `test`/`test` root bypass should succeed even under
    // strict enforcement with no explicit policy.
    let server = start_strict().await;
    let cfg = sdk_config_with(&server, "test", "test").await;
    let sts = StsClient::new(&cfg);
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert!(identity.arn().unwrap().contains(":root"));
}

// ======================================================================
// IAM tests
// ======================================================================

#[tokio::test]
async fn iam_get_user_resource_scoped_policy() {
    // Bob can read his own user record but not alice's.
    let server = start_strict().await;
    // Create both users via root bypass.
    {
        let boot = sdk_config_with(&server, "test", "test").await;
        let boot_iam = IamClient::new(&boot);
        boot_iam
            .create_user()
            .user_name("alice")
            .send()
            .await
            .unwrap();
    }
    let (akid, secret) = bootstrap_user(&server, "bob").await;
    attach_inline_policy(
        &server,
        "bob",
        "ReadSelf",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:GetUser","Resource":"arn:aws:iam::123456789012:user/bob"}]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let iam = IamClient::new(&cfg);

    // Bob -> self: allowed.
    let self_resp = iam.get_user().user_name("bob").send().await.unwrap();
    assert_eq!(self_resp.user().unwrap().user_name(), "bob");

    // Bob -> alice: denied because resource doesn't match.
    let err = iam.get_user().user_name("alice").send().await.unwrap_err();
    assert!(format!("{err:?}").contains(r#"code: Some("AccessDenied")"#));
}

#[tokio::test]
async fn iam_wildcard_action_allows_everything() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "dave").await;
    attach_inline_policy(
        &server,
        "dave",
        "AllowAllIam",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"iam:*","Resource":"*"}]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let iam = IamClient::new(&cfg);
    iam.list_users().send().await.unwrap();
    iam.get_user().user_name("dave").send().await.unwrap();
}

#[tokio::test]
async fn iam_soft_mode_does_not_fail_denied_requests() {
    // Soft mode should log the deny but let the request through.
    let server = start_soft().await;
    let (akid, secret) = bootstrap_user(&server, "erin").await;
    // No policies attached -> implicit deny in the evaluator, but soft
    // mode lets it through.
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let iam = IamClient::new(&cfg);
    // This would AccessDeny under strict mode; in soft mode it succeeds.
    iam.list_users().send().await.unwrap();
}

#[tokio::test]
async fn off_mode_does_not_enforce() {
    // Regression guard for off-by-default: no FAKECLOUD_IAM env set.
    let server = TestServer::start().await;
    let (akid, secret) = bootstrap_user(&server, "frank").await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let iam = IamClient::new(&cfg);
    // Frank has no policies, but enforcement is off -> succeeds.
    iam.list_users().send().await.unwrap();
}

// ======================================================================
// SQS tests
// ======================================================================

#[tokio::test]
async fn sqs_send_message_denied_without_policy() {
    let server = start_strict().await;
    // Bootstrap queue via root bypass.
    let boot = sdk_config_with(&server, "test", "test").await;
    aws_sdk_sqs::Client::new(&boot)
        .create_queue()
        .queue_name("jobs")
        .send()
        .await
        .unwrap();
    let (akid, secret) = bootstrap_user(&server, "sqsuser1").await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    // GetQueueUrl is denied first — match the error shape.
    let err = sqs
        .get_queue_url()
        .queue_name("jobs")
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("AccessDeniedException"));
}

#[tokio::test]
async fn sqs_resource_scoped_policy_distinguishes_queues() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let boot_sqs = aws_sdk_sqs::Client::new(&boot);
    let jobs_url = boot_sqs
        .create_queue()
        .queue_name("jobs")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let secrets_url = boot_sqs
        .create_queue()
        .queue_name("secrets")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let (akid, secret) = bootstrap_user(&server, "sqsuser2").await;
    attach_inline_policy(
        &server,
        "sqsuser2",
        "AllowJobs",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"sqs:SendMessage","Resource":"arn:aws:sqs:us-east-1:123456789012:jobs"}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);

    // Allowed: SendMessage to jobs.
    sqs.send_message()
        .queue_url(&jobs_url)
        .message_body("hello")
        .send()
        .await
        .unwrap();

    // Denied: SendMessage to secrets (same action, wrong resource).
    let err = sqs
        .send_message()
        .queue_url(&secrets_url)
        .message_body("leak")
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("AccessDeniedException"));
}

// ======================================================================
// SNS tests
// ======================================================================

#[tokio::test]
async fn sns_publish_denied_without_policy() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let topic_arn = aws_sdk_sns::Client::new(&boot)
        .create_topic()
        .name("alerts")
        .send()
        .await
        .unwrap()
        .topic_arn()
        .unwrap()
        .to_string();

    let (akid, secret) = bootstrap_user(&server, "snsuser1").await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sns = aws_sdk_sns::Client::new(&cfg);
    let err = sns
        .publish()
        .topic_arn(&topic_arn)
        .message("oops")
        .send()
        .await
        .unwrap_err();
    // SNS's declared 403.
    assert!(format!("{err:?}").contains(r#"code: Some("AuthorizationError")"#));
}

#[tokio::test]
async fn sns_publish_allowed_on_specific_topic() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let topic_arn = aws_sdk_sns::Client::new(&boot)
        .create_topic()
        .name("news")
        .send()
        .await
        .unwrap()
        .topic_arn()
        .unwrap()
        .to_string();

    let (akid, secret) = bootstrap_user(&server, "snsuser2").await;
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"sns:Publish","Resource":"{topic_arn}"}}]}}"#
    );
    attach_inline_policy(&server, "snsuser2", "AllowPublishNews", &policy).await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sns = aws_sdk_sns::Client::new(&cfg);
    sns.publish()
        .topic_arn(&topic_arn)
        .message("hello")
        .send()
        .await
        .unwrap();
}

// ======================================================================
// S3 tests
// ======================================================================

/// A create that configures the bucket needs the permission for what it
/// configures. This matters because those settings now PERSIST: a `public-read`
/// bucket created by a principal with no `s3:PutBucketAcl` used to lose the
/// grant on restart and now keeps it.
#[tokio::test]
async fn s3_create_bucket_with_acl_needs_put_bucket_acl() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "s3aclcreate").await;
    attach_inline_policy(
        &server,
        "s3aclcreate",
        "create-only",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:CreateBucket","Resource":"*"}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);

    // A plain create is still allowed by s3:CreateBucket alone.
    s3.create_bucket()
        .bucket("aclperm-plain")
        .send()
        .await
        .unwrap();

    // So is `--acl private`: the model'"'"'s CreateBucket permissions exempt it
    // and a no-ACL create from s3:PutBucketAcl, and clients that always send a
    // canned ACL (Terraform'"'"'s legacy `acl` attribute, SDK wrappers) would
    // otherwise be refused where real S3 succeeds.
    s3.create_bucket()
        .bucket("aclperm-private")
        .acl(aws_sdk_s3::types::BucketCannedAcl::Private)
        .send()
        .await
        .expect("a private canned ACL needs only s3:CreateBucket");

    // Asking for an ACL is not.
    let err = s3
        .create_bucket()
        .bucket("aclperm-denied")
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await
        .expect_err("a canned ACL needs s3:PutBucketAcl");
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "unexpected error: {err:?}"
    );

    // Granting it unblocks the create, and the bucket really carries the ACL.
    // The ACL has to be a non-public one with ACLs enabled (which needs
    // s3:PutBucketOwnershipControls): new buckets refuse a public ACL at create
    // under the default Block Public Access, and any ACL under the default
    // BucketOwnerEnforced.
    attach_inline_policy(
        &server,
        "s3aclcreate",
        "create-and-acl",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:CreateBucket","s3:PutBucketAcl","s3:GetBucketAcl","s3:PutBucketOwnershipControls"],"Resource":"*"}
        ]}"#,
    )
    .await;
    s3.create_bucket()
        .bucket("aclperm-allowed")
        .acl(aws_sdk_s3::types::BucketCannedAcl::from(
            "log-delivery-write",
        ))
        .object_ownership(aws_sdk_s3::types::ObjectOwnership::ObjectWriter)
        .send()
        .await
        .unwrap();
    let acl = s3
        .get_bucket_acl()
        .bucket("aclperm-allowed")
        .send()
        .await
        .unwrap();
    assert!(acl.grants().iter().any(|g| {
        g.grantee()
            .and_then(|gr| gr.uri())
            .is_some_and(|u| u.contains("s3/LogDelivery"))
    }));
}

/// The `s3:x-amz-acl` condition key is what a policy uses to pin down WHICH ACL
/// a create may ask for. It was never populated, so such a guardrail silently
/// authorized exactly what it was written to block.
#[tokio::test]
async fn s3_create_bucket_acl_condition_key_gates_the_canned_value() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "s3aclcond").await;
    attach_inline_policy(
        &server,
        "s3aclcond",
        "private-acls-only",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:CreateBucket","s3:PutBucketAcl"],"Resource":"*",
             "Condition":{"StringEquals":{"s3:x-amz-acl":"private"}}}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);

    s3.create_bucket()
        .bucket("aclcond-private")
        .acl(aws_sdk_s3::types::BucketCannedAcl::Private)
        .send()
        .await
        .expect("the permitted canned value must be allowed");

    let err = s3
        .create_bucket()
        .bucket("aclcond-public")
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await
        .expect_err("public-read must be denied by the condition");
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "unexpected error: {err:?}"
    );
}

/// Object lock enables versioning, so AWS wants both permissions.
#[tokio::test]
async fn s3_create_bucket_with_object_lock_needs_its_permissions() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "s3lockcreate").await;
    attach_inline_policy(
        &server,
        "s3lockcreate",
        "create-only",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:CreateBucket","Resource":"*"}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    let err = s3
        .create_bucket()
        .bucket("lockperm-denied")
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .expect_err("object lock needs its own permissions");
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "unexpected error: {err:?}"
    );

    attach_inline_policy(
        &server,
        "s3lockcreate",
        "create-and-lock",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:CreateBucket","s3:PutBucketObjectLockConfiguration","s3:PutBucketVersioning"],"Resource":"*"}
        ]}"#,
    )
    .await;
    s3.create_bucket()
        .bucket("lockperm-allowed")
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
}

/// Sending `x-amz-bypass-governance-retention: true` also needs
/// `s3:BypassGovernanceRetention` on PutObjectRetention and DeleteObject.
#[tokio::test]
async fn s3_governance_bypass_needs_bypass_governance_retention() {
    use aws_sdk_s3::primitives::{ByteStream, DateTime};
    use aws_sdk_s3::types::{ObjectLockMode, ObjectLockRetention, ObjectLockRetentionMode};

    let server = start_strict().await;
    let bucket = "bypass-perm-bucket";
    let root = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    root.create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    let far = DateTime::from_secs(4_070_908_800); // 2099-01-01
    let nearer = DateTime::from_secs(4_039_372_800); // 2098-01-01
    let version = root
        .put_object()
        .bucket(bucket)
        .key("a.txt")
        .body(ByteStream::from_static(b"x"))
        .object_lock_mode(ObjectLockMode::Governance)
        .object_lock_retain_until_date(far)
        .send()
        .await
        .unwrap()
        .version_id()
        .unwrap()
        .to_string();

    let (akid, secret) = bootstrap_user(&server, "s3bypass").await;
    attach_inline_policy(
        &server,
        "s3bypass",
        "all",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#,
    )
    .await;
    attach_inline_policy(
        &server,
        "s3bypass",
        "no-bypass",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:BypassGovernanceRetention","Resource":"*"}]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    let shorten = || {
        s3.put_object_retention()
            .bucket(bucket)
            .key("a.txt")
            .bypass_governance_retention(true)
            .retention(
                ObjectLockRetention::builder()
                    .mode(ObjectLockRetentionMode::Governance)
                    .retain_until_date(nearer)
                    .build(),
            )
    };
    let delete_one = || {
        s3.delete_object()
            .bucket(bucket)
            .key("a.txt")
            .version_id(&version)
            .bypass_governance_retention(true)
    };

    for err in [
        format!(
            "{:?}",
            shorten().send().await.expect_err("retention bypass")
        ),
        format!(
            "{:?}",
            delete_one().send().await.expect_err("delete bypass")
        ),
    ] {
        assert!(err.contains("AccessDenied"), "unexpected error: {err}");
    }

    IamClient::new(&sdk_config_with(&server, "test", "test").await)
        .delete_user_policy()
        .user_name("s3bypass")
        .policy_name("no-bypass")
        .send()
        .await
        .unwrap();
    shorten().send().await.expect("bypass allowed once granted");
    delete_one()
        .send()
        .await
        .expect("delete allowed once granted");
}

/// Setting an object's lock while writing it needs the lock permissions too:
/// `s3:PutObjectRetention` for the mode / retain-until headers and
/// `s3:PutObjectLegalHold` for the legal-hold header, on PutObject, CopyObject
/// and CreateMultipartUpload alike.
#[tokio::test]
async fn s3_object_lock_headers_need_retention_and_legal_hold_permissions() {
    use aws_sdk_s3::primitives::{ByteStream, DateTime};
    use aws_sdk_s3::types::{ObjectLockLegalHoldStatus, ObjectLockMode};

    let server = start_strict().await;
    let bucket = "lock-header-perm-bucket";
    let root = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    root.create_bucket()
        .bucket(bucket)
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    root.put_object()
        .bucket(bucket)
        .key("src.txt")
        .body(ByteStream::from_static(b"source"))
        .send()
        .await
        .unwrap();
    let far = DateTime::from_secs(4_070_908_800); // 2099-01-01

    let (akid, secret) = bootstrap_user(&server, "s3lockhdr").await;
    attach_inline_policy(
        &server,
        "s3lockhdr",
        "all",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#,
    )
    .await;
    attach_inline_policy(
        &server,
        "s3lockhdr",
        "no-lock",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":["s3:PutObjectRetention","s3:PutObjectLegalHold"],"Resource":"*"}]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    let put_retained = || {
        s3.put_object()
            .bucket(bucket)
            .key("put.txt")
            .body(ByteStream::from_static(b"x"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(far)
    };
    let put_held = || {
        s3.put_object()
            .bucket(bucket)
            .key("held.txt")
            .body(ByteStream::from_static(b"x"))
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
    };
    let copy_retained = || {
        s3.copy_object()
            .bucket(bucket)
            .key("copy.txt")
            .copy_source(format!("{bucket}/src.txt"))
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(far)
    };
    let create_retained = || {
        s3.create_multipart_upload()
            .bucket(bucket)
            .key("mpu.bin")
            .object_lock_mode(ObjectLockMode::Governance)
            .object_lock_retain_until_date(far)
    };

    // Writes that set no lock are unaffected by the Deny.
    s3.put_object()
        .bucket(bucket)
        .key("plain.txt")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .expect("a PutObject without lock headers needs no lock permission");

    for err in [
        format!(
            "{:?}",
            put_retained().send().await.expect_err("put retention")
        ),
        format!("{:?}", put_held().send().await.expect_err("put legal hold")),
        format!(
            "{:?}",
            copy_retained().send().await.expect_err("copy retention")
        ),
        format!(
            "{:?}",
            create_retained().send().await.expect_err("mpu retention")
        ),
    ] {
        assert!(err.contains("AccessDenied"), "unexpected error: {err}");
    }

    IamClient::new(&sdk_config_with(&server, "test", "test").await)
        .delete_user_policy()
        .user_name("s3lockhdr")
        .policy_name("no-lock")
        .send()
        .await
        .unwrap();
    put_retained()
        .send()
        .await
        .expect("PutObject with retention allowed once granted");
    put_held()
        .send()
        .await
        .expect("PutObject with legal hold allowed once granted");
    copy_retained()
        .send()
        .await
        .expect("CopyObject with retention allowed once granted");
    create_retained()
        .send()
        .await
        .expect("CreateMultipartUpload with retention allowed once granted");
}

/// A create carrying both tags and an ACL is several authorizations, and AWS
/// evaluates them all against ONE request context. A tag-scoped guardrail that
/// permits the create must therefore permit the `s3:PutBucketAcl` it implies:
/// building the context per action left that one seeing no `aws:RequestTag/*`
/// at all, so the condition matched and denied a create AWS allows.
///
/// The Deny is written with `Null`, deliberately. A `StringNotEquals` Deny cannot
/// catch this: an unpopulated key safe-fails every non-`IfExists` operator to
/// false, so such a Deny would not apply on the broken code either and the test
/// would pass against the bug. `Null` is the one operator that reads an
/// unpopulated key AS null, which is exactly the state being asserted against.
#[tokio::test]
async fn s3_create_bucket_tag_condition_applies_to_the_implied_acl_action() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "s3ctxuser").await;
    attach_inline_policy(
        &server,
        "s3ctxuser",
        "tag-scoped",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:*","Resource":"*"},
            {"Effect":"Deny",
             "Action":["s3:CreateBucket","s3:TagResource","s3:PutBucketAcl"],
             "Resource":"*",
             "Condition":{"Null":{"aws:RequestTag/CostCenter":"true"}}}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);

    // Tagged with the required value AND carrying an ACL: allowed, because
    // every action in the request sees `aws:RequestTag/CostCenter=123`.
    // (A non-public ACL with ACLs enabled: a new bucket refuses a public one,
    // or any ACL under the default BucketOwnerEnforced, after authorization.)
    s3.create_bucket()
        .bucket("ctx-tagged")
        .acl(aws_sdk_s3::types::BucketCannedAcl::from(
            "log-delivery-write",
        ))
        .object_ownership(aws_sdk_s3::types::ObjectOwnership::ObjectWriter)
        .create_bucket_configuration(
            aws_sdk_s3::types::CreateBucketConfiguration::builder()
                .tags(
                    aws_sdk_s3::types::Tag::builder()
                        .key("CostCenter")
                        .value("123")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("a create whose tags satisfy the guardrail must not be denied by its own ACL");

    // The guardrail still bites: a create carrying no such tag is denied.
    let err = s3
        .create_bucket()
        .bucket("ctx-untagged")
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await
        .expect_err("a create with no CostCenter tag must be denied");
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "unexpected error: {err:?}"
    );
}

/// Real S3 requires `s3:TagResource` on top of `s3:CreateBucket` to create a
/// bucket carrying `CreateBucketConfiguration.Tags` (issue #2553). A grant of
/// `s3:CreateBucket` alone still creates untagged buckets, but a tagged create
/// is denied.
#[tokio::test]
async fn s3_create_bucket_with_tags_needs_tag_resource() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "s3taguser").await;
    attach_inline_policy(
        &server,
        "s3taguser",
        "create-only",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:CreateBucket","Resource":"*"}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);

    // Untagged create is allowed by s3:CreateBucket alone.
    s3.create_bucket()
        .bucket("tagperm-untagged")
        .send()
        .await
        .unwrap();

    // The same grant must not authorize a tagged create.
    let err = s3
        .create_bucket()
        .bucket("tagperm-denied")
        .create_bucket_configuration(
            aws_sdk_s3::types::CreateBucketConfiguration::builder()
                .tags(
                    aws_sdk_s3::types::Tag::builder()
                        .key("team")
                        .value("a")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessDenied"),
        "expected AccessDenied for tagged create, got {msg}"
    );

    // Adding s3:TagResource unblocks it, and the bucket comes out tagged.
    attach_inline_policy(
        &server,
        "s3taguser",
        "create-and-tag",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:CreateBucket","s3:TagResource","s3:GetBucketTagging"],"Resource":"*"}
        ]}"#,
    )
    .await;
    s3.create_bucket()
        .bucket("tagperm-allowed")
        .create_bucket_configuration(
            aws_sdk_s3::types::CreateBucketConfiguration::builder()
                .tags(
                    aws_sdk_s3::types::Tag::builder()
                        .key("team")
                        .value("a")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let tags = s3
        .get_bucket_tagging()
        .bucket("tagperm-allowed")
        .send()
        .await
        .unwrap();
    assert!(tags
        .tag_set()
        .iter()
        .any(|t| t.key() == "team" && t.value() == "a"));
}

/// The create-time tag set feeds `aws:RequestTag/*`, so a policy conditioned
/// on the tag value actually decides the create.
#[tokio::test]
async fn s3_create_bucket_tags_drive_request_tag_condition() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "s3reqtaguser").await;
    attach_inline_policy(
        &server,
        "s3reqtaguser",
        "team-a-only",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:CreateBucket","s3:TagResource"],"Resource":"*",
             "Condition":{"StringEquals":{"aws:RequestTag/team":"a"}}}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    let create_with_team = |bucket: &'static str, team: &'static str| {
        let s3 = s3.clone();
        async move {
            s3.create_bucket()
                .bucket(bucket)
                .create_bucket_configuration(
                    aws_sdk_s3::types::CreateBucketConfiguration::builder()
                        .tags(
                            aws_sdk_s3::types::Tag::builder()
                                .key("team")
                                .value(team)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .send()
                .await
        }
    };

    create_with_team("reqtag-allowed", "a").await.unwrap();

    let err = create_with_team("reqtag-denied", "b").await.unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessDenied"),
        "expected AccessDenied for the non-matching tag value, got {msg}"
    );
}

#[tokio::test]
async fn s3_get_object_resource_scoped() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let s3_boot = aws_sdk_s3::Client::new(&boot);
    s3_boot
        .create_bucket()
        .bucket("private-docs")
        .send()
        .await
        .unwrap();
    s3_boot
        .put_object()
        .bucket("private-docs")
        .key("readme.md")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hi"))
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "s3user1").await;
    attach_inline_policy(
        &server,
        "s3user1",
        "ReadDocs",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::private-docs/*"}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    // Allowed: GetObject on private-docs/readme.md.
    s3.get_object()
        .bucket("private-docs")
        .key("readme.md")
        .send()
        .await
        .unwrap();

    // Denied: PutObject (different action not covered by policy).
    let err = s3
        .put_object()
        .bucket("private-docs")
        .key("evil.md")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("AccessDenied"));
}

/// Whether an S3 call was refused with 403 (HEAD responses carry no error
/// code, so the status is what tells).
fn is_access_denied<T, E>(result: Result<T, aws_sdk_s3::error::SdkError<E>>) -> bool {
    matches!(&result, Err(e) if e.raw_response().map(|r| r.status().as_u16()) == Some(403))
}

/// S3 is authorized under its policy action names, not its operation names:
/// `s3:ListBucket` covers ListObjectsV2, ListObjects and HeadBucket,
/// `s3:ListAllMyBuckets` covers ListBuckets, and a policy naming the
/// operations (`s3:ListObjectsV2`, `s3:ListBuckets`, `s3:HeadBucket`, which
/// are not actions) grants none of them.
#[tokio::test]
async fn s3_listing_is_authorized_under_policy_action_names() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    path_style_s3(&boot)
        .create_bucket()
        .bucket("listed")
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "lister").await;
    attach_inline_policy(
        &server,
        "lister",
        "List",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::listed"},
            {"Effect":"Allow","Action":"s3:ListAllMyBuckets","Resource":"*"}
        ]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    s3.list_objects_v2().bucket("listed").send().await.unwrap();
    s3.list_objects().bucket("listed").send().await.unwrap();
    s3.head_bucket().bucket("listed").send().await.unwrap();
    let buckets = s3.list_buckets().send().await.unwrap();
    assert!(buckets.buckets().iter().any(|b| b.name() == Some("listed")));
    // s3:ListBucket does not cover the version listing.
    assert!(is_access_denied(
        s3.list_object_versions().bucket("listed").send().await
    ));

    let (akid, secret) = bootstrap_user(&server, "opnames").await;
    attach_inline_policy(
        &server,
        "opnames",
        "OperationNames",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow",
            "Action":["s3:ListObjectsV2","s3:ListObjects","s3:HeadBucket","s3:ListBuckets"],
            "Resource":"*"}]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    assert!(is_access_denied(
        s3.list_objects_v2().bucket("listed").send().await
    ));
    assert!(is_access_denied(
        s3.list_objects().bucket("listed").send().await
    ));
    assert!(is_access_denied(
        s3.head_bucket().bucket("listed").send().await
    ));
    assert!(is_access_denied(s3.list_buckets().send().await));
}

/// CopyObject needs `s3:GetObject` on the source object and `s3:PutObject` on
/// the destination; either alone is refused.
#[tokio::test]
async fn s3_copy_object_needs_source_read_and_destination_write() {
    let server = start_strict().await;
    let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    for bucket in ["copy-src", "copy-dst"] {
        boot.create_bucket().bucket(bucket).send().await.unwrap();
    }
    boot.put_object()
        .bucket("copy-src")
        .key("orig.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"payload"))
        .send()
        .await
        .unwrap();

    // A source without annotations copies with s3:GetObject + s3:PutObject
    // alone under the default directives (recorded against S3, us-east-1,
    // 2026-11-12).
    let copy = |s3: aws_sdk_s3::Client, key: &'static str| async move {
        s3.copy_object()
            .bucket("copy-dst")
            .key(key)
            .copy_source("copy-src/orig.txt")
            .send()
            .await
    };
    for (user, statements, allowed) in [
        (
            "copier",
            r#"{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::copy-src/orig.txt"},
               {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::copy-dst/*"}"#,
            true,
        ),
        (
            "writeonly",
            r#"{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::copy-dst/*"}"#,
            false,
        ),
        (
            "readonly",
            r#"{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::copy-src/*"}"#,
            false,
        ),
        (
            // Not a policy action: grants nothing.
            "opname",
            r#"{"Effect":"Allow","Action":"s3:CopyObject","Resource":"*"}"#,
            false,
        ),
    ] {
        let (akid, secret) = bootstrap_user(&server, user).await;
        attach_inline_policy(
            &server,
            user,
            "Copy",
            &format!(r#"{{"Version":"2012-10-17","Statement":[{statements}]}}"#),
        )
        .await;
        let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
        let result = copy(s3, "copied.txt").await;
        if allowed {
            result.unwrap();
        } else {
            assert!(is_access_denied(result), "{user} must be refused");
        }
    }
    let got = boot
        .get_object()
        .bucket("copy-dst")
        .key("copied.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        &got.body.collect().await.unwrap().into_bytes()[..],
        b"payload"
    );

    // A source WITH annotations: copying them (the default COPY annotation
    // directive) also needs s3:GetObjectAnnotation on the source and
    // s3:PutObjectAnnotation on the destination; EXCLUDE needs neither. The
    // pinned SDK predates the annotation API, so the annotation is put raw.
    let resp = reqwest::Client::new()
        .put(format!(
            "{}/copy-src/orig.txt?annotation&annotationName=review",
            server.endpoint()
        ))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host, Signature=00",
        )
        .body("note")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{:?}", resp.text().await);
    let (akid, secret) = bootstrap_user(&server, "annotcopier").await;
    let s3_annot = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    let plain = r#"{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::copy-src/*"},
        {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::copy-dst/*"}"#;
    attach_inline_policy(
        &server,
        "annotcopier",
        "Copy",
        &format!(r#"{{"Version":"2012-10-17","Statement":[{plain}]}}"#),
    )
    .await;
    assert!(is_access_denied(
        copy(s3_annot.clone(), "annotated.txt").await
    ));
    let gone = boot
        .head_object()
        .bucket("copy-dst")
        .key("annotated.txt")
        .send()
        .await;
    assert!(gone.is_err(), "a refused copy writes nothing");
    attach_inline_policy(
        &server,
        "annotcopier",
        "Copy",
        &format!(
            r#"{{"Version":"2012-10-17","Statement":[{plain},
            {{"Effect":"Allow","Action":"s3:GetObjectAnnotation","Resource":"arn:aws:s3:::copy-src/*"}},
            {{"Effect":"Allow","Action":"s3:PutObjectAnnotation","Resource":"arn:aws:s3:::copy-dst/*"}}]}}"#
        ),
    )
    .await;
    copy(s3_annot, "annotated.txt").await.unwrap();

    // Excluding annotations needs only the read and the write, annotated
    // source or not. A tag header under the default COPY tagging directive is
    // ignored, so it needs no s3:PutObjectTagging; under REPLACE it does.
    let (akid, secret) = bootstrap_user(&server, "plaincopier").await;
    attach_inline_policy(
        &server,
        "plaincopier",
        "Copy",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::copy-src/*"},
            {"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::copy-dst/*"}]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    let excluding = |directive: Option<aws_sdk_s3::types::TaggingDirective>| {
        s3.copy_object()
            .bucket("copy-dst")
            .key("excluded.txt")
            .copy_source("copy-src/orig.txt")
            .tagging("team=a")
            .set_tagging_directive(directive)
            .customize()
            .mutate_request(|r| {
                r.headers_mut()
                    .insert("x-amz-object-annotation-directive", "EXCLUDE");
            })
            .send()
    };
    excluding(None).await.unwrap();
    excluding(Some(aws_sdk_s3::types::TaggingDirective::Copy))
        .await
        .unwrap();
    assert!(is_access_denied(
        excluding(Some(aws_sdk_s3::types::TaggingDirective::Replace)).await
    ));
}

/// Reading a specific version is `s3:GetObjectVersion` (`s3:GetObject` is not
/// enough, nor needed), and a PutObject that sets tags also needs
/// `s3:PutObjectTagging`.
#[tokio::test]
async fn s3_versioned_reads_and_tagged_writes_need_their_actions() {
    let server = start_strict().await;
    let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    boot.create_bucket().bucket("vers").send().await.unwrap();
    boot.put_bucket_versioning()
        .bucket("vers")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let first = boot
        .put_object()
        .bucket("vers")
        .key("doc")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"v1"))
        .send()
        .await
        .unwrap();
    let v1 = first.version_id().unwrap().to_string();
    boot.put_object()
        .bucket("vers")
        .key("doc")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"v2"))
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "reader").await;
    attach_inline_policy(
        &server,
        "reader",
        "Read",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"arn:aws:s3:::vers/*"}
        ]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    s3.get_object()
        .bucket("vers")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert!(is_access_denied(
        s3.get_object()
            .bucket("vers")
            .key("doc")
            .version_id(&v1)
            .send()
            .await
    ));
    assert!(is_access_denied(
        s3.head_object()
            .bucket("vers")
            .key("doc")
            .version_id(&v1)
            .send()
            .await
    ));
    let put = |tagging: Option<&'static str>| {
        let mut req = s3
            .put_object()
            .bucket("vers")
            .key("tagged")
            .body(aws_sdk_s3::primitives::ByteStream::from_static(b"t"));
        if let Some(t) = tagging {
            req = req.tagging(t);
        }
        req.send()
    };
    put(None).await.unwrap();
    assert!(is_access_denied(put(Some("team=a")).await));

    attach_inline_policy(
        &server,
        "reader",
        "Read",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:GetObjectVersion","s3:PutObject","s3:PutObjectTagging"],
             "Resource":"arn:aws:s3:::vers/*"}
        ]}"#,
    )
    .await;
    let got = s3
        .get_object()
        .bucket("vers")
        .key("doc")
        .version_id(&v1)
        .send()
        .await
        .unwrap();
    assert_eq!(&got.body.collect().await.unwrap().into_bytes()[..], b"v1");
    // The current version is s3:GetObject, which this policy no longer grants.
    assert!(is_access_denied(
        s3.get_object().bucket("vers").key("doc").send().await
    ));
    put(Some("team=a")).await.unwrap();
}

/// DeleteObjects is authorized per key, as on AWS: a denied key is an
/// `AccessDenied` entry of a 200 response and stays, the allowed keys are
/// deleted. Soft enforcement only logs, so every key is deleted.
#[tokio::test]
async fn s3_delete_objects_reports_denied_keys_per_key() {
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    for strict in [true, false] {
        let server = if strict {
            start_strict().await
        } else {
            TestServer::start_with_env(&[("FAKECLOUD_IAM", "soft")]).await
        };
        let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
        boot.create_bucket().bucket("batch").send().await.unwrap();
        for key in ["open/a", "locked/b"] {
            boot.put_object()
                .bucket("batch")
                .key(key)
                .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
                .send()
                .await
                .unwrap();
        }
        let (akid, secret) = bootstrap_user(&server, "batcher").await;
        attach_inline_policy(
            &server,
            "batcher",
            "DeleteOpen",
            r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::batch/open/*"}
            ]}"#,
        )
        .await;
        let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
        let delete = Delete::builder()
            .objects(ObjectIdentifier::builder().key("open/a").build().unwrap())
            .objects(ObjectIdentifier::builder().key("locked/b").build().unwrap())
            .build()
            .unwrap();
        let out = s3
            .delete_objects()
            .bucket("batch")
            .delete(delete)
            .send()
            .await
            .unwrap();
        let deleted: Vec<_> = out.deleted().iter().filter_map(|d| d.key()).collect();
        let left = boot.list_objects_v2().bucket("batch").send().await.unwrap();
        let remaining: Vec<_> = left.contents().iter().filter_map(|o| o.key()).collect();
        if strict {
            assert_eq!(deleted, ["open/a"]);
            let errors = out.errors();
            assert_eq!(errors.len(), 1, "{errors:?}");
            assert_eq!(errors[0].key(), Some("locked/b"));
            assert_eq!(errors[0].code(), Some("AccessDenied"));
            assert_eq!(errors[0].message(), Some("Access Denied"));
            assert_eq!(remaining, ["locked/b"]);
        } else {
            assert_eq!(deleted.len(), 2);
            assert!(out.errors().is_empty());
            assert!(remaining.is_empty());
        }
    }
}

/// A read of a missing key is 404 `NoSuchKey` only for a caller allowed
/// `s3:ListBucket`; anyone else gets 403 `AccessDenied`.
#[tokio::test]
async fn s3_missing_key_is_404_only_with_list_bucket() {
    let server = start_strict().await;
    let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    boot.create_bucket().bucket("probe").send().await.unwrap();
    let (akid, secret) = bootstrap_user(&server, "prober").await;
    let read_only = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::probe/*"}
    ]}"#;
    attach_inline_policy(&server, "prober", "Read", read_only).await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    let get = s3.get_object().bucket("probe").key("nope").send().await;
    assert!(format!("{get:?}").contains("AccessDenied"), "{get:?}");
    assert!(is_access_denied(get));
    assert!(is_access_denied(
        s3.head_object().bucket("probe").key("nope").send().await
    ));

    attach_inline_policy(
        &server,
        "prober",
        "Read",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::probe/*"},
            {"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::probe"}
        ]}"#,
    )
    .await;
    let err = s3
        .get_object()
        .bucket("probe")
        .key("nope")
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_no_such_key());
    let head = s3.head_object().bucket("probe").key("nope").send().await;
    assert_eq!(
        head.as_ref()
            .err()
            .and_then(|e| e.raw_response())
            .map(|r| r.status().as_u16()),
        Some(404)
    );
}

/// A website bucket serves its error document for the 403 a missing key gets
/// without `s3:ListBucket`, with that status.
#[tokio::test]
async fn s3_website_error_document_keeps_the_hidden_missing_key_status() {
    use aws_sdk_s3::types::{ErrorDocument, IndexDocument, WebsiteConfiguration};
    let server = start_strict().await;
    let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    boot.create_bucket().bucket("site").send().await.unwrap();
    boot.put_object()
        .bucket("site")
        .key("error.html")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"oops"))
        .send()
        .await
        .unwrap();
    boot.put_bucket_website()
        .bucket("site")
        .website_configuration(
            WebsiteConfiguration::builder()
                .index_document(
                    IndexDocument::builder()
                        .suffix("index.html")
                        .build()
                        .unwrap(),
                )
                .error_document(ErrorDocument::builder().key("error.html").build().unwrap())
                .build(),
        )
        .send()
        .await
        .unwrap();
    let (akid, secret) = bootstrap_user(&server, "visitor").await;
    attach_inline_policy(
        &server,
        "visitor",
        "Read",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::site/*"}
        ]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    let err = s3
        .get_object()
        .bucket("site")
        .key("missing.html")
        .send()
        .await
        .unwrap_err();
    let raw = err.raw_response().expect("an HTTP response");
    assert_eq!(raw.status().as_u16(), 403);
    assert_eq!(raw.body().bytes(), Some(&b"oops"[..]));
}

/// The key a request addresses is derived once, from the raw path, for both
/// its authorization and its routing: `/b//` addresses the key `/`. So an
/// object operation on such a key is authorized as that operation, and an
/// unsigned request cannot have it authorized as something else (here,
/// DeleteObjects, which dispatch leaves to its handler) and run unchecked.
#[tokio::test]
async fn s3_raw_path_keys_are_authorized_as_the_operation_they_run() {
    let server = start_strict().await;
    let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    boot.create_bucket().bucket("victim").send().await.unwrap();
    let http = reqwest::Client::new();
    // Two resource types are refused as conflicting (400), `?delete` on a
    // key as a method the key does not take (405), both before
    // authorization.
    for (method, query, status) in [
        (reqwest::Method::POST, "delete&uploads", 400),
        (reqwest::Method::POST, "delete&restore", 400),
        (reqwest::Method::POST, "delete&uploadId=u", 400),
        (reqwest::Method::POST, "delete&select&select-type=2", 400),
        (reqwest::Method::POST, "delete", 405),
        (reqwest::Method::PUT, "delete", 405),
        (reqwest::Method::GET, "delete", 405),
    ] {
        let resp = http
            .request(
                method.clone(),
                format!("{}/victim//?{query}", server.endpoint()),
            )
            .body("<Delete><Object><Key>x</Key></Object></Delete>")
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), status, "{method} ?{query}");
    }
    let uploads = boot
        .list_multipart_uploads()
        .bucket("victim")
        .send()
        .await
        .unwrap();
    assert!(uploads.uploads().is_empty(), "{uploads:?}");
    let objects = boot
        .list_objects_v2()
        .bucket("victim")
        .send()
        .await
        .unwrap();
    assert!(objects.contents().is_empty(), "{objects:?}");

    // A signed caller allowed only s3:ListBucket cannot read the key `/`
    // by the bucket-level route, nor one allowed only s3:GetObject on
    // another key reach it.
    boot.put_object()
        .bucket("victim")
        .key("/")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"slash"))
        .send()
        .await
        .unwrap();
    let (akid, secret) = bootstrap_user(&server, "lister2").await;
    attach_inline_policy(
        &server,
        "lister2",
        "List",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:ListBucket","Resource":"arn:aws:s3:::victim"}
        ]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    assert!(is_access_denied(
        s3.get_object().bucket("victim").key("/").send().await
    ));
}

/// Requests S3 refuses before authorizing anything get S3's recorded errors,
/// whoever sends them (anonymous here), and change nothing: conflicting
/// resource-type parameters (400 InvalidArgument), a resource type with a
/// method it does not take (405), an upload part number that is not one.
/// Each was a way to run one operation under another's authorization.
#[tokio::test]
async fn s3_requests_s3_refuses_before_authorization_change_nothing() {
    let server = start_strict().await;
    let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    boot.create_bucket().bucket("victim").send().await.unwrap();
    boot.create_bucket().bucket("empty1").send().await.unwrap();
    boot.put_object()
        .bucket("victim")
        .key("k")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"secret"))
        .send()
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let tagging = "<Tagging><TagSet><Tag><Key>a</Key><Value>b</Value></Tag></TagSet></Tagging>";
    for (method, path, body, status, code, message) in [
        (
            reqwest::Method::PUT,
            "/victim/k?partNumber=x&uploadId=u&acl",
            "",
            400,
            "InvalidArgument",
            "Conflicting query string parameters: acl, partNumber, uploadId",
        ),
        (
            reqwest::Method::PUT,
            "/victim/k?partNumber=x&uploadId=u&tagging",
            tagging,
            400,
            "InvalidArgument",
            "Conflicting query string parameters: partNumber, tagging, uploadId",
        ),
        (
            reqwest::Method::PUT,
            "/victim/k?partNumber=x&uploadId=u",
            "",
            400,
            "InvalidArgument",
            "Part number must be an integer between 1 and 10000, inclusive",
        ),
        (
            reqwest::Method::GET,
            "/victim?versions&cors",
            "",
            400,
            "InvalidArgument",
            "Conflicting query string parameters: cors, versions",
        ),
        (
            reqwest::Method::DELETE,
            "/empty1?location",
            "",
            405,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        ),
        (
            reqwest::Method::PUT,
            "/empty1?location",
            "",
            405,
            "MethodNotAllowed",
            "The specified method is not allowed against this resource.",
        ),
        (
            reqwest::Method::POST,
            "/victim/k?restore&uploadId=u",
            "<RestoreRequest><Days>1</Days></RestoreRequest>",
            400,
            "InvalidArgument",
            "Conflicting query string parameters: restore, uploadId",
        ),
        (
            reqwest::Method::DELETE,
            "/victim?publicAccessBlock&lifecycle",
            "",
            400,
            "InvalidArgument",
            "Conflicting query string parameters: lifecycle, publicAccessBlock",
        ),
    ] {
        let resp = http
            .request(method.clone(), format!("{}{path}", server.endpoint()))
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), status, "{method} {path}");
        let text = resp.text().await.unwrap();
        assert!(
            text.contains(&format!("<Code>{code}</Code>")),
            "{path}: {text}"
        );
        assert!(
            text.contains(&format!("<Message>{message}</Message>")),
            "{path}: {text}"
        );
        if status == 405 {
            assert!(
                text.contains(&format!("<Method>{method}</Method>")),
                "{text}"
            );
            assert!(
                text.contains("<ResourceType>LOCATION</ResourceType>"),
                "{text}"
            );
        } else if code == "InvalidArgument" && message.starts_with("Conflicting") {
            assert!(
                text.contains("<ArgumentName>ResourceType</ArgumentName>"),
                "{text}"
            );
        }
    }
    // Nothing changed.
    let acl = boot
        .get_object_acl()
        .bucket("victim")
        .key("k")
        .send()
        .await
        .unwrap();
    assert_eq!(acl.grants().len(), 1, "{acl:?}");
    let tags = boot
        .get_object_tagging()
        .bucket("victim")
        .key("k")
        .send()
        .await
        .unwrap();
    assert!(tags.tag_set().is_empty());
    let buckets = boot.list_buckets().send().await.unwrap();
    let names: Vec<_> = buckets.buckets().iter().filter_map(|b| b.name()).collect();
    assert!(names.contains(&"empty1"), "{names:?}");
    assert!(!names.contains(&"newbucket-x"), "{names:?}");
}

/// The website error document is an object read of its own: a caller not
/// allowed to read it (here, anonymous with a bucket policy opening only
/// `public/*`) gets a plain 403, never its content.
#[tokio::test]
async fn s3_website_error_document_needs_its_own_read() {
    use aws_sdk_s3::types::{ErrorDocument, IndexDocument, WebsiteConfiguration};
    let server = start_strict().await;
    let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    boot.create_bucket().bucket("site").send().await.unwrap();
    for (key, body) in [
        ("private/err.html", &b"PRIVATE ERROR DOC CONTENT"[..]),
        ("public/ok.html", &b"ok"[..]),
    ] {
        boot.put_object()
            .bucket("site")
            .key(key)
            .body(aws_sdk_s3::primitives::ByteStream::from_static(body))
            .send()
            .await
            .unwrap();
    }
    boot.put_bucket_website()
        .bucket("site")
        .website_configuration(
            WebsiteConfiguration::builder()
                .index_document(
                    IndexDocument::builder()
                        .suffix("index.html")
                        .build()
                        .unwrap(),
                )
                .error_document(
                    ErrorDocument::builder()
                        .key("private/err.html")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    boot.delete_public_access_block()
        .bucket("site")
        .send()
        .await
        .unwrap();
    boot.put_bucket_policy()
        .bucket("site")
        .policy(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",
                "Action":"s3:GetObject","Resource":"arn:aws:s3:::site/public/*"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let ok = http
        .get(format!("{}/site/public/ok.html", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(ok.status(), 200);
    let missing = http
        .get(format!("{}/site/public/missing", server.endpoint()))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 403);
    let text = missing.text().await.unwrap();
    assert!(!text.contains("PRIVATE"), "{text}");
    assert!(text.contains("<Code>AccessDenied</Code>"), "{text}");
}

/// HeadObject / GetObject return an object's retention only to a caller
/// allowed `s3:GetObjectRetention`, and its legal hold only to one allowed
/// `s3:GetObjectLegalHold`.
#[tokio::test]
async fn s3_object_lock_headers_need_their_read_permissions() {
    let server = start_strict().await;
    let boot = path_style_s3(&sdk_config_with(&server, "test", "test").await);
    boot.create_bucket()
        .bucket("vault")
        .object_lock_enabled_for_bucket(true)
        .send()
        .await
        .unwrap();
    boot.put_object()
        .bucket("vault")
        .key("doc")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(aws_sdk_s3::primitives::DateTime::from_secs(4_000_000_000))
        .object_lock_legal_hold_status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "auditor").await;
    attach_inline_policy(
        &server,
        "auditor",
        "Read",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::vault/*"}
        ]}"#,
    )
    .await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    let head = s3
        .head_object()
        .bucket("vault")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert!(head.object_lock_mode().is_none());
    assert!(head.object_lock_retain_until_date().is_none());
    assert!(head.object_lock_legal_hold_status().is_none());
    let get = s3
        .get_object()
        .bucket("vault")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert!(get.object_lock_mode().is_none());
    assert!(get.object_lock_legal_hold_status().is_none());

    attach_inline_policy(
        &server,
        "auditor",
        "Read",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectRetention"],
             "Resource":"arn:aws:s3:::vault/*"}
        ]}"#,
    )
    .await;
    let head = s3
        .head_object()
        .bucket("vault")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.object_lock_mode(),
        Some(&aws_sdk_s3::types::ObjectLockMode::Governance)
    );
    assert!(head.object_lock_retain_until_date().is_some());
    assert!(head.object_lock_legal_hold_status().is_none());

    // The bucket owner (the reserved root bypass here) sees everything.
    let head = boot
        .head_object()
        .bucket("vault")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert!(head.object_lock_legal_hold_status().is_some());
}

#[tokio::test]
async fn s3_list_objects_v2_prefix_condition_key() {
    // A policy that only allows listing (`s3:ListBucket`, which authorizes
    // ListObjectsV2) when `s3:prefix` starts with "logs/" must let
    // `prefix=logs/2026/` through and deny `prefix=secrets/`.
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let s3_boot = aws_sdk_s3::Client::new(&boot);
    s3_boot
        .create_bucket()
        .bucket("prefixed")
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "s3prefixuser").await;
    attach_inline_policy(
        &server,
        "s3prefixuser",
        "AllowListLogs",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"s3:ListBucket",
            "Resource":"arn:aws:s3:::prefixed",
            "Condition":{"StringLike":{"s3:prefix":"logs/*"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);

    // Allowed: prefix=logs/2026/ matches StringLike "logs/*".
    s3.list_objects_v2()
        .bucket("prefixed")
        .prefix("logs/2026/")
        .send()
        .await
        .unwrap();

    // Denied: prefix=secrets/ fails the condition.
    let err = s3
        .list_objects_v2()
        .bucket("prefixed")
        .prefix("secrets/")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied, got {err:?}"
    );

    // Denied: no prefix at all. Without the key populated the
    // condition is unmatched and the statement doesn't apply.
    let err = s3
        .list_objects_v2()
        .bucket("prefixed")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied on missing prefix, got {err:?}"
    );
}

#[tokio::test]
async fn sns_subscribe_protocol_condition_key() {
    // Policy allows sns:Subscribe only when sns:Protocol == "https".
    // HTTPS subscribe succeeds; email subscribe is denied.
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let sns_boot = aws_sdk_sns::Client::new(&boot);
    let topic_arn = sns_boot
        .create_topic()
        .name("cond-topic")
        .send()
        .await
        .unwrap()
        .topic_arn()
        .unwrap()
        .to_string();

    let (akid, secret) = bootstrap_user(&server, "snsproto").await;
    attach_inline_policy(
        &server,
        "snsproto",
        "AllowHttpsOnly",
        &format!(
            r#"{{"Version":"2012-10-17","Statement":[{{
                "Effect":"Allow",
                "Action":"sns:Subscribe",
                "Resource":"{topic_arn}",
                "Condition":{{"StringEquals":{{"sns:Protocol":"https"}}}}
            }}]}}"#
        ),
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sns = aws_sdk_sns::Client::new(&cfg);

    // Allowed: Protocol=https.
    sns.subscribe()
        .topic_arn(&topic_arn)
        .protocol("https")
        .endpoint("https://example.com/hook")
        .send()
        .await
        .unwrap();

    // Denied: Protocol=email fails the condition.
    let err = sns
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("email")
        .endpoint("user@example.com")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains(r#"code: Some("AuthorizationError")"#),
        "expected AuthorizationError for email protocol, got {err:?}"
    );
}

// ======================================================================
// Phase 2: Condition block evaluation
//
// These drive real aws-sdk-rust signed requests against FAKECLOUD_IAM=strict
// to verify that condition keys populated at dispatch time are matched
// against the inline policy. Start soft mode would swallow the deny — all
// four must run under strict so the deny path is observable as an error.
// ======================================================================

#[tokio::test]
async fn condition_string_equals_username_allows_owner() {
    // A policy whose Allow is gated on `aws:username == dana` should
    // let dana in and keep everyone else out of the same action.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "dana").await;
    attach_inline_policy(
        &server,
        "dana",
        "AllowIfDana",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"StringEquals":{"aws:username":"dana"}}
        }]}"#,
    )
    .await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert!(identity.arn().unwrap().contains("user/dana"));
}

#[tokio::test]
async fn condition_string_equals_username_denies_other_user() {
    // Same policy text attached to a different user: the condition
    // should not match and the request should be denied.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "erin").await;
    attach_inline_policy(
        &server,
        "erin",
        "AllowIfDana",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"StringEquals":{"aws:username":"dana"}}
        }]}"#,
    )
    .await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    assert!(format!("{err:?}").contains(r#"code: Some("AccessDenied")"#));
}

#[tokio::test]
async fn condition_ip_address_loopback_allows() {
    // The fakecloud test server binds 127.0.0.1, so aws:SourceIp will
    // always be a loopback address. A policy restricting access to
    // 127.0.0.0/8 must allow.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "frank").await;
    attach_inline_policy(
        &server,
        "frank",
        "AllowFromLoopback",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.0/8"}}
        }]}"#,
    )
    .await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let identity = sts.get_caller_identity().send().await.unwrap();
    assert!(identity.arn().unwrap().contains("user/frank"));
}

#[tokio::test]
async fn condition_ip_address_non_matching_cidr_denies() {
    // Same test but with a CIDR block the loopback address cannot be
    // in — the condition should not match and the request should be
    // denied.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "grace").await;
    attach_inline_policy(
        &server,
        "grace",
        "AllowFromExternal",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"IpAddress":{"aws:SourceIp":"203.0.113.0/24"}}
        }]}"#,
    )
    .await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    assert!(format!("{err:?}").contains(r#"code: Some("AccessDenied")"#));
}

#[tokio::test]
async fn condition_date_less_than_past_deadline_denies() {
    // A policy whose Allow only fires before a deadline in the past
    // should be denied — the current time is always after 2020-01-01.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "henry").await;
    attach_inline_policy(
        &server,
        "henry",
        "AllowBefore2020",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sts:GetCallerIdentity",
            "Resource":"*",
            "Condition":{"DateLessThan":{"aws:CurrentTime":"2020-01-01T00:00:00Z"}}
        }]}"#,
    )
    .await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    assert!(format!("{err:?}").contains(r#"code: Some("AccessDenied")"#));
}

#[tokio::test]
async fn condition_bool_secure_transport_deny_fires_over_http() {
    // A Deny statement gated on `aws:SecureTransport == false` must
    // fire when the request comes in over plain HTTP (fakecloud's
    // test server). The Deny beats the unconditional Allow.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "irene").await;
    attach_inline_policy(
        &server,
        "irene",
        "RequireTls",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"*","Resource":"*"},
            {"Effect":"Deny","Action":"*","Resource":"*",
             "Condition":{"Bool":{"aws:SecureTransport":"false"}}}
        ]}"#,
    )
    .await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sts = StsClient::new(&cfg);
    let err = sts.get_caller_identity().send().await.unwrap_err();
    assert!(format!("{err:?}").contains(r#"code: Some("AccessDenied")"#));
}

// ======================================================================
// Phase 2: Resource-based policies (S3 bucket policies)
//
// Drive the evaluator's resource-policy path end-to-end: a real
// `aws-sdk-s3` signed request hits strict-mode enforcement, the
// dispatch layer fetches the bucket policy via the
// `ResourcePolicyProvider`, and the evaluator combines it with the
// caller's identity policies using AWS cross-account semantics.
//
// fakecloud runs every request against the same account ID today, so
// all of these exercise the same-account Allow-union path. The
// cross-account Allow-intersection semantics live under unit tests in
// `fakecloud-iam::evaluator`; wiring real cross-account requests is a
// future batch with its own infrastructure.
// ======================================================================

/// Create a bucket owned by the root credentials, PutObject one key, then
/// attach the given bucket policy to it. Returns the bucket name.
async fn seed_bucket_with_policy(server: &TestServer, bucket: &str, policy_json: &str) {
    let boot = sdk_config_with(server, "test", "test").await;
    let s3_boot = aws_sdk_s3::Client::new(&boot);
    s3_boot.create_bucket().bucket(bucket).send().await.unwrap();
    s3_boot
        .put_object()
        .bucket(bucket)
        .key("readme.md")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hi"))
        .send()
        .await
        .unwrap();
    // New buckets block public policies; some of these tests install a
    // `Principal:"*"` grant, so lift the block first, as a user would.
    s3_boot
        .delete_public_access_block()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    s3_boot
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy_json)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn bucket_policy_grants_access_without_identity_policy() {
    // A user with no identity policy attached can still read an
    // object when the bucket policy grants them s3:GetObject. This is
    // the same-account Allow-union path — identity side is implicit
    // deny, resource side is Allow, so the request succeeds.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "bp_reader").await;
    seed_bucket_with_policy(
        &server,
        "bp-shared-docs",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":{"AWS":"arn:aws:iam::123456789012:user/bp_reader"},
            "Action":"s3:GetObject",
            "Resource":"arn:aws:s3:::bp-shared-docs/*"
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    let resp = s3
        .get_object()
        .bucket("bp-shared-docs")
        .key("readme.md")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"hi");
}

#[tokio::test]
async fn bucket_policy_explicit_deny_beats_identity_allow() {
    // Identity policy grants s3:GetObject but the bucket policy denies
    // it for this user. Explicit deny on either side wins, regardless
    // of identity-side allow.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "bp_blocked").await;
    attach_inline_policy(
        &server,
        "bp_blocked",
        "AllowReadAll",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}
        ]}"#,
    )
    .await;
    seed_bucket_with_policy(
        &server,
        "bp-locked-docs",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny",
            "Principal":{"AWS":"arn:aws:iam::123456789012:user/bp_blocked"},
            "Action":"s3:GetObject",
            "Resource":"arn:aws:s3:::bp-locked-docs/*"
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    let err = s3
        .get_object()
        .bucket("bp-locked-docs")
        .key("readme.md")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied, got {err:?}"
    );
}

#[tokio::test]
async fn bucket_policy_identity_allow_no_bucket_policy_still_works() {
    // Regression guard: if no bucket policy is attached, same-account
    // identity-only access must keep working exactly as it did in
    // Phase 1. Batch 3 added the provider lookup; this test asserts
    // the lookup returning None does not change identity-only
    // semantics.
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let s3_boot = aws_sdk_s3::Client::new(&boot);
    s3_boot
        .create_bucket()
        .bucket("bp-plain-docs")
        .send()
        .await
        .unwrap();
    s3_boot
        .put_object()
        .bucket("bp-plain-docs")
        .key("readme.md")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"hi"))
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "bp_plain").await;
    attach_inline_policy(
        &server,
        "bp_plain",
        "ReadPlain",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bp-plain-docs/*"}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    s3.get_object()
        .bucket("bp-plain-docs")
        .key("readme.md")
        .send()
        .await
        .unwrap();
}

/// A hand-crafted signed Authorization header carrying `akid`'s access key in
/// the S3 credential scope. With SigV4 verification off the dummy signature is
/// never checked, but dispatch still parses the access key and resolves the
/// principal — letting a test drive requests that the AWS SDK would never
/// emit (here, an S3 sub-resource form with no IAM-action mapping).
fn s3_signed_auth_header(akid: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential={akid}/20240101/us-east-1/s3/aws4_request, \
         SignedHeaders=host, Signature=deadbeef"
    )
}

#[tokio::test]
async fn strict_mode_denies_unmapped_enforceable_action() {
    // Fix 5.2: strict enforcement must fail closed when an `iam_enforceable`
    // service returns no `IamAction` for an operation. `POST /bucket?acl` is
    // not a valid S3 op (ACL is GET/PUT only), so `s3_detect_action` returns
    // None — the enforceable-but-unmapped branch. Previously dispatch warned
    // and fell through to the handler, running the op with no policy check.
    //
    // SigV4 verification is left OFF so the hand-crafted request reaches the
    // IAM layer (a bad signature would otherwise 403 earlier, masking the
    // behavior under test).
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "strict")]).await;

    let boot = sdk_config_with(&server, "test", "test").await;
    aws_sdk_s3::Client::new(&boot)
        .create_bucket()
        .bucket("unmapped-probe")
        .send()
        .await
        .unwrap();

    let (akid, _secret) = bootstrap_user(&server, "unmapped_user").await;
    // Allow-all identity policy: any *mapped* action would be permitted, so a
    // 403 here can only come from the missing IAM-action mapping, not a policy
    // deny. This isolates the fail-closed behavior.
    attach_inline_policy(
        &server,
        "unmapped_user",
        "AllowEverything",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#,
    )
    .await;

    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{}/unmapped-probe?acl", server.endpoint()))
        .header("authorization", s3_signed_auth_header(&akid))
        .send()
        .await
        .unwrap();
    // S3 now classifies every request it routes, and refuses one naming a
    // sub-resource with a method it does not take before authorizing it, as
    // S3 does: 405 MethodNotAllowed, never served and never authorized as
    // some other operation (fail closed).
    assert_eq!(
        resp.status(),
        405,
        "strict mode must refuse an S3 request with no operation before authorizing it"
    );
    let text = resp.text().await.unwrap();
    assert!(text.contains("<ResourceType>ACL</ResourceType>"), "{text}");
}

#[tokio::test]
async fn soft_mode_allows_unmapped_enforceable_action_through() {
    // Counterpart guarantee to `strict_mode_denies_unmapped_enforceable_action`:
    // the new deny is gated on strict mode only. In soft mode the same unmapped
    // enforceable action is NOT denied by the IAM layer — it warns and falls
    // through to the handler, preserving historical rollout behavior.
    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "soft")]).await;

    let boot = sdk_config_with(&server, "test", "test").await;
    aws_sdk_s3::Client::new(&boot)
        .create_bucket()
        .bucket("unmapped-soft-probe")
        .send()
        .await
        .unwrap();

    let (akid, _secret) = bootstrap_user(&server, "unmapped_soft_user").await;

    let http = reqwest::Client::new();
    let resp = http
        .post(format!("{}/unmapped-soft-probe?acl", server.endpoint()))
        .header("authorization", s3_signed_auth_header(&akid))
        .send()
        .await
        .unwrap();
    assert_ne!(
        resp.status(),
        403,
        "soft mode must not deny an unmapped enforceable action; it falls through to the handler"
    );
}

#[tokio::test]
async fn bucket_policy_principal_wildcard_grants_any_user() {
    // Public bucket idiom: `"Principal": "*"`. Any non-root caller
    // signing real SigV4 requests should be able to GetObject,
    // regardless of their identity policy set.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "bp_anyone").await;
    seed_bucket_with_policy(
        &server,
        "bp-public-docs",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":"*",
            "Action":"s3:GetObject",
            "Resource":"arn:aws:s3:::bp-public-docs/*"
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    s3.get_object()
        .bucket("bp-public-docs")
        .key("readme.md")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn bucket_policy_condition_block_gates_access() {
    // Regression guard: Phase 2 condition evaluation still applies to
    // resource-policy statements. Grant is conditional on
    // `aws:SecureTransport == true`, and fakecloud's test server
    // serves plain HTTP, so the condition does not match and the
    // grant does not apply. Result: implicit deny.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "bp_tls_only").await;
    seed_bucket_with_policy(
        &server,
        "bp-tls-docs",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":"*",
            "Action":"s3:GetObject",
            "Resource":"arn:aws:s3:::bp-tls-docs/*",
            "Condition":{"Bool":{"aws:SecureTransport":"true"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let s3 = aws_sdk_s3::Client::new(&cfg);
    let err = s3
        .get_object()
        .bucket("bp-tls-docs")
        .key("readme.md")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied, got {err:?}"
    );
}

// ======================================================================
// Phase 2: SNS topic policies
//
// Drives the resource-policy evaluator path end-to-end for SNS:
// dispatch fetches the topic policy via the `SnsResourcePolicyProvider`
// and hands it to the evaluator alongside the caller's identity
// policies. All of these are same-account — the cross-account
// intersection semantics are covered by unit tests in
// `fakecloud-iam::evaluator`.
// ======================================================================

async fn seed_topic_with_policy(server: &TestServer, name: &str, policy_json: &str) -> String {
    let boot = sdk_config_with(server, "test", "test").await;
    let sns_boot = aws_sdk_sns::Client::new(&boot);
    let topic_arn = sns_boot
        .create_topic()
        .name(name)
        .send()
        .await
        .unwrap()
        .topic_arn()
        .unwrap()
        .to_string();
    sns_boot
        .set_topic_attributes()
        .topic_arn(&topic_arn)
        .attribute_name("Policy")
        .attribute_value(policy_json)
        .send()
        .await
        .unwrap();
    topic_arn
}

#[tokio::test]
async fn topic_policy_grants_publish_without_identity_policy() {
    // Same-account Allow-union: user has no identity policy, but the
    // topic policy names them and grants sns:Publish. The request
    // should succeed.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "tp_reader").await;
    let topic_arn = seed_topic_with_policy(
        &server,
        "tp-shared",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":{"AWS":"arn:aws:iam::123456789012:user/tp_reader"},
            "Action":"sns:Publish",
            "Resource":"*"
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sns = aws_sdk_sns::Client::new(&cfg);
    sns.publish()
        .topic_arn(&topic_arn)
        .message("hello")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn topic_policy_explicit_deny_beats_identity_allow() {
    // Identity policy grants sns:Publish on this topic, but the topic
    // policy denies it explicitly. Explicit Deny wins.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "tp_blocked").await;
    let topic_arn = seed_topic_with_policy(
        &server,
        "tp-locked",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny",
            "Principal":{"AWS":"arn:aws:iam::123456789012:user/tp_blocked"},
            "Action":"sns:Publish",
            "Resource":"*"
        }]}"#,
    )
    .await;
    let identity_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"sns:Publish","Resource":"{topic_arn}"}}]}}"#
    );
    attach_inline_policy(&server, "tp_blocked", "AllowTp", &identity_policy).await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sns = aws_sdk_sns::Client::new(&cfg);
    let err = sns
        .publish()
        .topic_arn(&topic_arn)
        .message("nope")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains(r#"code: Some("AuthorizationError")"#),
        "expected SNS's AuthorizationError, got {err:?}"
    );
}

#[tokio::test]
async fn topic_policy_principal_wildcard_grants_any_user() {
    // `"Principal": "*"` on a topic should let any non-root caller
    // publish, without any identity-side policy.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "tp_anyone").await;
    let topic_arn = seed_topic_with_policy(
        &server,
        "tp-public",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":"*",
            "Action":"sns:Publish",
            "Resource":"*"
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sns = aws_sdk_sns::Client::new(&cfg);
    sns.publish()
        .topic_arn(&topic_arn)
        .message("hi")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn topic_policy_service_principal_does_not_grant_user() {
    // Regression guard for the Service-principal matcher: a topic
    // policy granting `{"Service":"events.amazonaws.com"}` must NOT
    // grant a random IAM user — only a principal whose ARN looks
    // like a Lambda/Events service-linked role would match.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "tp_svc_user").await;
    let topic_arn = seed_topic_with_policy(
        &server,
        "tp-events-only",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":{"Service":"events.amazonaws.com"},
            "Action":"sns:Publish",
            "Resource":"*"
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sns = aws_sdk_sns::Client::new(&cfg);
    let err = sns
        .publish()
        .topic_arn(&topic_arn)
        .message("should deny")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains(r#"code: Some("AuthorizationError")"#),
        "expected SNS's AuthorizationError, got {err:?}"
    );
}

#[tokio::test]
async fn topic_policy_condition_block_gates_access() {
    // Regression guard: Phase 2 condition evaluation still applies
    // to resource-policy statements on SNS. The grant is conditional
    // on `aws:SecureTransport == true`, and fakecloud's test server
    // serves plain HTTP, so the condition does not match and the
    // grant does not apply.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "tp_tls_only").await;
    let topic_arn = seed_topic_with_policy(
        &server,
        "tp-tls-only",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":"*",
            "Action":"sns:Publish",
            "Resource":"*",
            "Condition":{"Bool":{"aws:SecureTransport":"true"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sns = aws_sdk_sns::Client::new(&cfg);
    let err = sns
        .publish()
        .topic_arn(&topic_arn)
        .message("should deny")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains(r#"code: Some("AuthorizationError")"#),
        "expected SNS's AuthorizationError, got {err:?}"
    );
}

// ======================================================================
// Phase 2: Lambda resource policies
//
// Drives the resource-policy evaluator path end-to-end for Lambda:
// AddPermission composes a canonical policy document, dispatch fetches
// it via LambdaResourcePolicyProvider, and the evaluator combines it
// with the caller's identity policies. We never actually invoke the
// function runtime — Invoke requests are denied at the enforcement
// gate before reaching the container layer when enforcement says no,
// and we match on AccessDeniedException / ServiceException shapes that
// the runtime emits when enforcement says yes.
// ======================================================================

/// Bug audit 2026-10-01: `iam:PassRole` was never evaluated for the caller.
/// A principal holding only `lambda:CreateFunction` could attach any role --
/// an admin role included -- to code it controls. CreateFunction now also
/// needs `iam:PassRole` on the role, with `iam:PassedToService` set to
/// `lambda.amazonaws.com`.
#[tokio::test]
async fn lambda_create_function_requires_pass_role_on_the_role() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let iam_boot = IamClient::new(&boot);
    let admin_role = iam_boot
        .create_role()
        .role_name("admin-role")
        .assume_role_policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#,
        )
        .send()
        .await
        .unwrap()
        .role()
        .unwrap()
        .arn()
        .to_string();
    let (akid, secret) = bootstrap_user(&server, "deployer").await;
    attach_inline_policy(
        &server,
        "deployer",
        "create-only",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"lambda:CreateFunction","Resource":"*"}]}"#,
    )
    .await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    let create = |name: &'static str| {
        lambda
            .create_function()
            .function_name(name)
            .runtime(aws_sdk_lambda::types::Runtime::Python312)
            .role(&admin_role)
            .handler("index.handler")
            .code(
                aws_sdk_lambda::types::FunctionCode::builder()
                    .zip_file(aws_sdk_lambda::primitives::Blob::new(
                        make_empty_python_zip(),
                    ))
                    .build(),
            )
            .send()
    };
    let err = create("escalate")
        .await
        .expect_err("CreateFunction without iam:PassRole must be denied");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessDenied") && msg.contains("iam:PassRole"),
        "{msg}"
    );

    // PassRole scoped to a different service does not help.
    attach_inline_policy(
        &server,
        "deployer",
        "pass-to-ec2",
        &format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"iam:PassRole","Resource":"{admin_role}","Condition":{{"StringEquals":{{"iam:PassedToService":"ec2.amazonaws.com"}}}}}}]}}"#
        ),
    )
    .await;
    create("still-denied")
        .await
        .expect_err("PassRole for ec2 must not cover Lambda");

    // PassRole to Lambda for this role: allowed.
    attach_inline_policy(
        &server,
        "deployer",
        "pass-to-lambda",
        &format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"iam:PassRole","Resource":"{admin_role}","Condition":{{"StringEquals":{{"iam:PassedToService":"lambda.amazonaws.com"}}}}}}]}}"#
        ),
    )
    .await;
    create("allowed")
        .await
        .expect("CreateFunction with iam:PassRole to lambda is allowed");
}

fn make_empty_python_zip() -> Vec<u8> {
    use std::io::Write;
    let buf = Vec::new();
    let mut zip = zip::ZipWriter::new(std::io::Cursor::new(buf));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    zip.start_file("index.py", options).unwrap();
    zip.write_all(b"def handler(event, context):\n    return {\"statusCode\": 200}\n")
        .unwrap();
    zip.finish().unwrap().into_inner()
}

async fn seed_function_with_permission(
    server: &TestServer,
    name: &str,
    statement_id: &str,
    principal_arn: &str,
) -> String {
    let boot = sdk_config_with(server, "test", "test").await;
    let lambda_boot = aws_sdk_lambda::Client::new(&boot);
    lambda_boot
        .create_function()
        .function_name(name)
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(
                    make_empty_python_zip(),
                ))
                .build(),
        )
        .send()
        .await
        .unwrap();
    lambda_boot
        .add_permission()
        .function_name(name)
        .statement_id(statement_id)
        // Post-D3 the action is stored verbatim; the IAM evaluator
        // matches `Action` patterns against the request's qualified
        // `lambda:InvokeFunction`, so callers must qualify here too.
        .action("lambda:InvokeFunction")
        .principal(principal_arn)
        .send()
        .await
        .unwrap();
    format!("arn:aws:lambda:us-east-1:123456789012:function:{name}")
}

#[tokio::test]
async fn lambda_function_policy_grants_invoke_without_identity_policy() {
    // Same-account Allow-union on Lambda: no identity policy, but
    // AddPermission installs a resource-policy statement naming the
    // user. The Invoke enforcement gate must let the request through;
    // we observe a ServiceException from the container runtime rather
    // than an AccessDeniedException, which is how we distinguish
    // "enforcement allowed" from "enforcement denied" without a real
    // runtime.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "lam_invoker").await;
    seed_function_with_permission(
        &server,
        "lam-shared-fn",
        "invoker-grant",
        "arn:aws:iam::123456789012:user/lam_invoker",
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    // Tolerate either a successful invocation (when a local runtime
    // can actually execute the stub handler) or a non-Access service
    // error (when it can't) — the only outcome we're guarding against
    // is an enforcement-layer AccessDeniedException.
    if let Err(err) = lambda.invoke().function_name("lam-shared-fn").send().await {
        assert!(
            !format!("{err:?}").contains("AccessDeniedException"),
            "enforcement should have allowed this request, got AccessDenied: {err:?}"
        );
    }
}

/// An alias has a resource policy of its own: a grant on the alias admits
/// invocations of the alias only, not of the function or of the version
/// the alias points to, and GetPolicy reads each qualifier's own document.
#[tokio::test]
async fn lambda_alias_policy_grants_invoking_that_alias_only() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let lambda_boot = aws_sdk_lambda::Client::new(&boot);
    lambda_boot
        .create_function()
        .function_name("lam-alias-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(
                    make_empty_python_zip(),
                ))
                .build(),
        )
        .send()
        .await
        .unwrap();
    lambda_boot
        .publish_version()
        .function_name("lam-alias-fn")
        .send()
        .await
        .unwrap();
    lambda_boot
        .create_alias()
        .function_name("lam-alias-fn")
        .name("PROD")
        .function_version("1")
        .send()
        .await
        .unwrap();
    lambda_boot
        .add_permission()
        .function_name("lam-alias-fn")
        .qualifier("PROD")
        .statement_id("alias-grant")
        .action("lambda:InvokeFunction")
        .principal("arn:aws:iam::123456789012:user/lam_alias_invoker")
        .send()
        .await
        .unwrap();
    let alias_policy = lambda_boot
        .get_policy()
        .function_name("lam-alias-fn")
        .qualifier("PROD")
        .send()
        .await
        .unwrap();
    assert!(alias_policy.policy().unwrap().contains(
        "\"Resource\":\"arn:aws:lambda:us-east-1:123456789012:function:lam-alias-fn:PROD\""
    ));
    for qualifier in [Some("1"), None] {
        let err = lambda_boot
            .get_policy()
            .function_name("lam-alias-fn")
            .set_qualifier(qualifier.map(str::to_string))
            .send()
            .await
            .expect_err("the alias's grant is not the version's or the function's");
        assert!(
            format!("{err:?}").contains("ResourceNotFoundException"),
            "{err:?}"
        );
    }

    let (akid, secret) = bootstrap_user(&server, "lam_alias_invoker").await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    // The alias: allowed (a runtime error at most, never AccessDenied).
    for invoke in [
        lambda
            .invoke()
            .function_name("lam-alias-fn")
            .qualifier("PROD"),
        lambda.invoke().function_name("lam-alias-fn:PROD"),
    ] {
        if let Err(err) = invoke.send().await {
            assert!(
                !format!("{err:?}").contains("AccessDeniedException"),
                "the alias grant should admit this invocation: {err:?}"
            );
        }
    }
    // The function and the version: no grant covers them.
    for invoke in [
        lambda.invoke().function_name("lam-alias-fn"),
        lambda.invoke().function_name("lam-alias-fn").qualifier("1"),
    ] {
        let err = invoke.send().await.unwrap_err();
        assert!(
            format!("{err:?}").contains("AccessDeniedException"),
            "expected AccessDeniedException, got {err:?}"
        );
    }
}

/// An AWS::Lambda::Permission on an alias (its FunctionName is the alias's
/// `Ref`, the alias ARN) is that alias's grant: under strict IAM the
/// grantee may invoke the alias and not the function.
#[tokio::test]
async fn cfn_alias_permission_grants_invoking_that_alias_only() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let cfn = aws_sdk_cloudformation::Client::new(&boot);
    let template = r#"{
      "Resources": {
        "Func": {
          "Type": "AWS::Lambda::Function",
          "Properties": {
            "FunctionName": "cfn-alias-perm-fn",
            "Runtime": "python3.12",
            "Handler": "index.handler",
            "Role": "arn:aws:iam::123456789012:role/test-role",
            "Code": {"ZipFile": "def handler(e, c):\n    return e\n"}
          }
        },
        "Version": {
          "Type": "AWS::Lambda::Version",
          "Properties": {"FunctionName": {"Ref": "Func"}}
        },
        "Alias": {
          "Type": "AWS::Lambda::Alias",
          "Properties": {
            "FunctionName": {"Ref": "Func"},
            "FunctionVersion": {"Fn::GetAtt": ["Version", "Version"]},
            "Name": "live"
          }
        },
        "AliasPermission": {
          "Type": "AWS::Lambda::Permission",
          "Properties": {
            "FunctionName": {"Ref": "Alias"},
            "Action": "lambda:InvokeFunction",
            "Principal": "arn:aws:iam::123456789012:user/cfn_alias_invoker"
          }
        }
      }
    }"#;
    cfn.create_stack()
        .stack_name("cfn-alias-perm")
        .template_body(template)
        .send()
        .await
        .unwrap();
    let status = cfn
        .describe_stacks()
        .stack_name("cfn-alias-perm")
        .send()
        .await
        .unwrap()
        .stacks()[0]
        .clone();
    assert_eq!(
        status.stack_status().unwrap().as_str(),
        "CREATE_COMPLETE",
        "{:?}",
        status.stack_status_reason()
    );
    let lambda_boot = aws_sdk_lambda::Client::new(&boot);
    let alias_policy = lambda_boot
        .get_policy()
        .function_name("cfn-alias-perm-fn")
        .qualifier("live")
        .send()
        .await
        .unwrap();
    assert!(alias_policy.policy().unwrap().contains("cfn_alias_invoker"));

    let (akid, secret) = bootstrap_user(&server, "cfn_alias_invoker").await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    if let Err(err) = lambda
        .invoke()
        .function_name("cfn-alias-perm-fn")
        .qualifier("live")
        .send()
        .await
    {
        assert!(
            !format!("{err:?}").contains("AccessDeniedException"),
            "the stack's alias grant should admit this invocation: {err:?}"
        );
    }
    let err = lambda
        .invoke()
        .function_name("cfn-alias-perm-fn")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDeniedException"),
        "expected AccessDeniedException, got {err:?}"
    );
}

#[tokio::test]
async fn lambda_function_without_policy_denies_non_root_invoke() {
    // Sanity pair: same function shape but without AddPermission.
    // Identity policy is empty, resource policy is empty -> implicit
    // deny under strict mode, observable as AccessDeniedException.
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let lambda_boot = aws_sdk_lambda::Client::new(&boot);
    lambda_boot
        .create_function()
        .function_name("lam-unpolicied")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(
                    make_empty_python_zip(),
                ))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "lam_denied").await;
    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    let err = lambda
        .invoke()
        .function_name("lam-unpolicied")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDeniedException"),
        "expected AccessDeniedException, got {err:?}"
    );
}

#[tokio::test]
async fn lambda_update_function_code_deny_is_honored() {
    // bug-hunt 2026-06-15 §5.1: UpdateFunctionCode (and 70 other ops) were
    // unmapped in `iam_action_for` (_ => None), so Lambda being
    // `iam_enforceable` meant they ran with ZERO policy evaluation. A user
    // with Allow-* but an explicit Deny on lambda:UpdateFunctionCode could
    // still update code. With the action now mapped, the Deny must win.
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    aws_sdk_lambda::Client::new(&boot)
        .create_function()
        .function_name("lam-deny-code")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(
                    make_empty_python_zip(),
                ))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "lam_code_deny").await;
    attach_inline_policy(
        &server,
        "lam_code_deny",
        "AllowAllDenyCode",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"*","Resource":"*"},
            {"Effect":"Deny","Action":"lambda:UpdateFunctionCode","Resource":"*"}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    let err = lambda
        .update_function_code()
        .function_name("lam-deny-code")
        .zip_file(aws_sdk_lambda::primitives::Blob::new(
            make_empty_python_zip(),
        ))
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDeniedException"),
        "explicit Deny on lambda:UpdateFunctionCode must be honored, got {err:?}"
    );
}

#[tokio::test]
async fn lambda_update_function_code_allowed_with_policy() {
    // Allow-side pair for §5.1: a user explicitly allowed
    // lambda:UpdateFunctionCode must NOT be denied by the enforcement gate.
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    aws_sdk_lambda::Client::new(&boot)
        .create_function()
        .function_name("lam-allow-code")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(
                    make_empty_python_zip(),
                ))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "lam_code_allow").await;
    attach_inline_policy(
        &server,
        "lam_code_allow",
        "AllowCode",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"lambda:UpdateFunctionCode","Resource":"*"}
        ]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    lambda
        .update_function_code()
        .function_name("lam-allow-code")
        .zip_file(aws_sdk_lambda::primitives::Blob::new(
            make_empty_python_zip(),
        ))
        .send()
        .await
        .expect("explicit Allow on lambda:UpdateFunctionCode must succeed");
}

#[tokio::test]
async fn lambda_function_policy_wildcard_principal_grants_any_user() {
    // Public-function idiom: AddPermission with Principal="*" lets
    // any non-root user through the enforcement gate.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "lam_anyone").await;
    seed_function_with_permission(&server, "lam-public-fn", "any", "*").await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    if let Err(err) = lambda.invoke().function_name("lam-public-fn").send().await {
        assert!(
            !format!("{err:?}").contains("AccessDeniedException"),
            "enforcement should have allowed this request, got AccessDenied: {err:?}"
        );
    }
}

#[tokio::test]
async fn lambda_function_policy_source_arn_condition_gates_grant() {
    // Regression guard: AddPermission with SourceArn must emit an
    // ArnLike Condition that the evaluator respects. We populate
    // `aws:SourceArn` via a non-matching value, which is not
    // currently plumbed from dispatch — so the condition never
    // matches and the grant does not apply, leaving the user with
    // an implicit deny from the empty identity-policy side.
    //
    // This is the "grant is conditional and condition does not
    // apply -> fall through to identity policy" path. The evaluator
    // treats an unknown condition key as "does not match" rather
    // than "matches everything", so this test asserts the deny path
    // exists end-to-end.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "lam_src_arn").await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let lambda_boot = aws_sdk_lambda::Client::new(&boot);
    lambda_boot
        .create_function()
        .function_name("lam-src-arn-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(
                    make_empty_python_zip(),
                ))
                .build(),
        )
        .send()
        .await
        .unwrap();
    lambda_boot
        .add_permission()
        .function_name("lam-src-arn-fn")
        .statement_id("gated")
        .action("lambda:InvokeFunction")
        .principal("arn:aws:iam::123456789012:user/lam_src_arn")
        .source_arn("arn:aws:events:us-east-1:123456789012:rule/my-rule")
        .send()
        .await
        .unwrap();

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);
    let err = lambda
        .invoke()
        .function_name("lam-src-arn-fn")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDeniedException"),
        "condition-gated grant should not apply; expected AccessDenied, got {err:?}"
    );
}

#[tokio::test]
async fn lambda_add_permission_principal_condition_key() {
    // Policy allows lambda:AddPermission only when lambda:Principal
    // == "s3.amazonaws.com". An s3 grant succeeds; an events grant
    // is denied.
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let lambda_boot = aws_sdk_lambda::Client::new(&boot);
    lambda_boot
        .create_function()
        .function_name("lam-cond-fn")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(
                    make_empty_python_zip(),
                ))
                .build(),
        )
        .send()
        .await
        .unwrap();

    let (akid, secret) = bootstrap_user(&server, "lam_cond").await;
    attach_inline_policy(
        &server,
        "lam_cond",
        "AllowS3Principal",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"lambda:AddPermission",
            "Resource":"arn:aws:lambda:us-east-1:123456789012:function:lam-cond-fn",
            "Condition":{"StringEquals":{"lambda:Principal":"s3.amazonaws.com"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let lambda = aws_sdk_lambda::Client::new(&cfg);

    // Allowed: Principal=s3.amazonaws.com matches the condition.
    lambda
        .add_permission()
        .function_name("lam-cond-fn")
        .statement_id("s3-ok")
        .action("lambda:InvokeFunction")
        .principal("s3.amazonaws.com")
        .send()
        .await
        .unwrap();

    // Denied: Principal=events.amazonaws.com fails the condition.
    let err = lambda
        .add_permission()
        .function_name("lam-cond-fn")
        .statement_id("events-no")
        .action("lambda:InvokeFunction")
        .principal("events.amazonaws.com")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDeniedException"),
        "expected AccessDeniedException for non-s3 principal, got {err:?}"
    );
}

#[tokio::test]
async fn sqs_send_message_attribute_condition_key() {
    // Policy allows sqs:SendMessage only when
    // `sqs:MessageAttribute.Color == "red"`. Verify a red message is
    // accepted and a blue one is denied.
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let sqs_boot = aws_sdk_sqs::Client::new(&boot);
    let queue_url = sqs_boot
        .create_queue()
        .queue_name("cond-attr")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();

    let (akid, secret) = bootstrap_user(&server, "sqsattr").await;
    attach_inline_policy(
        &server,
        "sqsattr",
        "AllowRedOnly",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Action":"sqs:SendMessage",
            "Resource":"arn:aws:sqs:us-east-1:123456789012:cond-attr",
            "Condition":{"StringEquals":{"sqs:MessageAttribute.Color":"red"}}
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);

    // Allowed: Color=red matches the condition.
    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("hi")
        .message_attributes(
            "Color",
            aws_sdk_sqs::types::MessageAttributeValue::builder()
                .data_type("String")
                .string_value("red")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Denied: Color=blue fails the condition.
    let err = sqs
        .send_message()
        .queue_url(&queue_url)
        .message_body("hi")
        .message_attributes(
            "Color",
            aws_sdk_sqs::types::MessageAttributeValue::builder()
                .data_type("String")
                .string_value("blue")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for blue color, got {err:?}"
    );

    // Denied: no attributes at all. The key is not populated so the
    // condition evaluates to doesn't-apply.
    let err = sqs
        .send_message()
        .queue_url(&queue_url)
        .message_body("hi")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for no-attr send, got {err:?}"
    );
}

// ======================================================================
// Phase 5: NotPrincipal
//
// Drive the evaluator's NotPrincipal path end-to-end via an S3 bucket
// policy. The classic pattern is Deny + NotPrincipal: deny everyone
// except a specific user. The named user is exempt from the deny;
// everyone else is blocked.
// ======================================================================

#[tokio::test]
async fn not_principal_deny_exempts_named_user() {
    // Bucket policy:
    //   1. Allow * for s3:GetObject (public read)
    //   2. Deny NotPrincipal alice for s3:GetObject (block everyone except alice)
    //
    // Net effect: only alice can GetObject.
    let server = start_strict().await;
    let (alice_akid, alice_secret) = bootstrap_user(&server, "np_alice").await;
    let (bob_akid, bob_secret) = bootstrap_user(&server, "np_bob").await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "PublicRead",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": ["arn:aws:s3:::np-bucket/*", "arn:aws:s3:::np-bucket"]
            },
            {
                "Sid": "DenyEveryoneExceptAlice",
                "Effect": "Deny",
                "NotPrincipal": {"AWS": "arn:aws:iam::123456789012:user/np_alice"},
                "Action": "s3:*",
                "Resource": ["arn:aws:s3:::np-bucket/*", "arn:aws:s3:::np-bucket"]
            }
        ]
    });

    seed_bucket_with_policy(&server, "np-bucket", &policy.to_string()).await;

    // Alice: Deny does NOT apply (she's the NotPrincipal), Allow applies -> success.
    let alice_cfg = sdk_config_with(&server, &alice_akid, &alice_secret).await;
    let alice_s3 = aws_sdk_s3::Client::new(&alice_cfg);
    let resp = alice_s3
        .get_object()
        .bucket("np-bucket")
        .key("readme.md")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"hi");

    // Bob: Deny applies (he's NOT the NotPrincipal) -> ExplicitDeny -> AccessDenied.
    let bob_cfg = sdk_config_with(&server, &bob_akid, &bob_secret).await;
    let bob_s3 = aws_sdk_s3::Client::new(&bob_cfg);
    let err = bob_s3
        .get_object()
        .bucket("np-bucket")
        .key("readme.md")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for bob, got {err:?}"
    );
}

#[tokio::test]
async fn not_principal_allow_grants_only_non_named_user() {
    // Bucket policy: Allow NotPrincipal for s3:GetObject — grants
    // access to everyone EXCEPT the named user.
    let server = start_strict().await;
    let (alice_akid, alice_secret) = bootstrap_user(&server, "npa_alice").await;
    let (bob_akid, bob_secret) = bootstrap_user(&server, "npa_bob").await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowEveryoneExceptAlice",
            "Effect": "Allow",
            "NotPrincipal": {"AWS": "arn:aws:iam::123456789012:user/npa_alice"},
            "Action": "s3:GetObject",
            "Resource": ["arn:aws:s3:::npa-bucket/*", "arn:aws:s3:::npa-bucket"]
        }]
    });

    seed_bucket_with_policy(&server, "npa-bucket", &policy.to_string()).await;

    // Bob: Allow applies (he's NOT in the NotPrincipal list) -> success.
    let bob_cfg = sdk_config_with(&server, &bob_akid, &bob_secret).await;
    let bob_s3 = aws_sdk_s3::Client::new(&bob_cfg);
    let resp = bob_s3
        .get_object()
        .bucket("npa-bucket")
        .key("readme.md")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"hi");

    // Alice: Allow does NOT apply (she IS the NotPrincipal), no other
    // grants -> ImplicitDeny -> AccessDenied.
    let alice_cfg = sdk_config_with(&server, &alice_akid, &alice_secret).await;
    let alice_s3 = aws_sdk_s3::Client::new(&alice_cfg);
    let err = alice_s3
        .get_object()
        .bucket("npa-bucket")
        .key("readme.md")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for alice, got {err:?}"
    );
}

// ======================================================================
// Phase 5: KMS key policies + IAM enforcement
//
// KMS participates in IAM enforcement with full action mapping and
// resource-ARN resolution. Every key carries a key policy (resource
// policy) that the evaluator combines with the caller's identity
// policies. The default key policy grants kms:* to the account root.
// ======================================================================

#[tokio::test]
async fn kms_identity_policy_explicit_deny_beats_key_policy() {
    // Identity policy explicitly denies kms:Decrypt while allowing
    // kms:Encrypt. The default key policy grants kms:* to root (which
    // matches all same-account users), but explicit deny always wins.
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "kms_enc_user").await;
    attach_inline_policy(
        &server,
        "kms_enc_user",
        "AllowEncryptDenyDecrypt",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"kms:*","Resource":"*"},
            {"Effect":"Deny","Action":"kms:Decrypt","Resource":"*"}
        ]}"#,
    )
    .await;

    // Create a key as root.
    let boot = sdk_config_with(&server, "test", "test").await;
    let kms_boot = aws_sdk_kms::Client::new(&boot);
    let key = kms_boot.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    // User encrypts -> should succeed.
    let user_cfg = sdk_config_with(&server, &akid, &secret).await;
    let kms_user = aws_sdk_kms::Client::new(&user_cfg);
    kms_user
        .encrypt()
        .key_id(&key_id)
        .plaintext(aws_sdk_kms::primitives::Blob::new(b"hello"))
        .send()
        .await
        .unwrap();

    // First encrypt as root to get ciphertext.
    let enc = kms_boot
        .encrypt()
        .key_id(&key_id)
        .plaintext(aws_sdk_kms::primitives::Blob::new(b"hello"))
        .send()
        .await
        .unwrap();
    let ciphertext = enc.ciphertext_blob().unwrap().clone();

    // User decrypts -> explicit deny in identity policy wins.
    let err = kms_user
        .decrypt()
        .key_id(&key_id)
        .ciphertext_blob(ciphertext)
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for decrypt, got {err:?}"
    );
}

#[tokio::test]
async fn kms_no_identity_policy_denied() {
    // The default key policy's "Enable IAM permissions" statement grants the
    // account *root* (kms:* on the key). In AWS this does NOT directly authorize
    // every principal in the account — it *delegates* authorization to IAM, so a
    // principal still needs an identity policy allowing the KMS action. A user
    // whose only identity grant is kms:DescribeKey is therefore denied
    // kms:Encrypt (bug-audit 2026-05-28, 5.5).
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "kms_keypol_user").await;
    attach_inline_policy(
        &server,
        "kms_keypol_user",
        "AllowDescribe",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"kms:DescribeKey","Resource":"*"}
        ]}"#,
    )
    .await;

    // Create a key as root (gets the default IAM-delegating key policy).
    let boot = sdk_config_with(&server, "test", "test").await;
    let kms_boot = aws_sdk_kms::Client::new(&boot);
    let key = kms_boot.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    // User encrypts -> denied: the key policy only delegates to IAM, and the
    // user's identity policy does not allow kms:Encrypt.
    let user_cfg = sdk_config_with(&server, &akid, &secret).await;
    let kms_user = aws_sdk_kms::Client::new(&user_cfg);
    let err = kms_user
        .encrypt()
        .key_id(&key_id)
        .plaintext(aws_sdk_kms::primitives::Blob::new(b"keypol-test"))
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for kms:Encrypt without an identity grant, got {err:?}"
    );
}

#[tokio::test]
async fn kms_key_policy_deny_beats_identity_allow() {
    // Key policy explicitly denies a specific user. Even though bob
    // has kms:* in his identity policy, the key policy deny wins.
    let server = start_strict().await;
    let (alice_akid, alice_secret) = bootstrap_user(&server, "kms_alice").await;
    let (bob_akid, bob_secret) = bootstrap_user(&server, "kms_bob").await;

    // Give both users kms:* identity policy.
    for user in &["kms_alice", "kms_bob"] {
        attach_inline_policy(
            &server,
            user,
            "AllowKmsAll",
            r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"kms:*","Resource":"*"}
            ]}"#,
        )
        .await;
    }

    // Create a key as root, then replace key policy: allow alice, deny bob.
    let boot = sdk_config_with(&server, "test", "test").await;
    let kms_boot = aws_sdk_kms::Client::new(&boot);
    let key = kms_boot.create_key().send().await.unwrap();
    let key_id = key.key_metadata().unwrap().key_id().to_string();

    let restrictive_policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowAlice",
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                "Action": "kms:*",
                "Resource": "*"
            },
            {
                "Sid": "DenyBob",
                "Effect": "Deny",
                "Principal": {"AWS": "arn:aws:iam::123456789012:user/kms_bob"},
                "Action": "kms:*",
                "Resource": "*"
            }
        ]
    });
    kms_boot
        .put_key_policy()
        .key_id(&key_id)
        .policy_name("default")
        .policy(restrictive_policy.to_string())
        .send()
        .await
        .unwrap();

    // Alice can encrypt (identity allows, key policy allows root).
    let alice_cfg = sdk_config_with(&server, &alice_akid, &alice_secret).await;
    let kms_alice = aws_sdk_kms::Client::new(&alice_cfg);
    kms_alice
        .encrypt()
        .key_id(&key_id)
        .plaintext(aws_sdk_kms::primitives::Blob::new(b"alice-test"))
        .send()
        .await
        .unwrap();

    // Bob is explicitly denied by the key policy.
    let bob_cfg = sdk_config_with(&server, &bob_akid, &bob_secret).await;
    let kms_bob = aws_sdk_kms::Client::new(&bob_cfg);
    let err = kms_bob
        .encrypt()
        .key_id(&key_id)
        .plaintext(aws_sdk_kms::primitives::Blob::new(b"bob-test"))
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for bob, got {err:?}"
    );
}

// ======================================================================
// Phase 2b: SQS queue policies
//
// Mirrors the SNS topic-policy block above. Drives the resource-policy
// evaluator path end-to-end for SQS: dispatch fetches the queue policy
// via the `SqsResourcePolicyProvider` and hands it to the evaluator
// alongside the caller's identity policies.
// ======================================================================

async fn seed_queue_with_policy(
    server: &TestServer,
    name: &str,
    policy_json: &str,
) -> (String, String) {
    let boot = sdk_config_with(server, "test", "test").await;
    let sqs_boot = aws_sdk_sqs::Client::new(&boot);
    let queue_url = sqs_boot
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    sqs_boot
        .set_queue_attributes()
        .queue_url(&queue_url)
        .attributes(aws_sdk_sqs::types::QueueAttributeName::Policy, policy_json)
        .send()
        .await
        .unwrap();
    let attrs = sqs_boot
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let arn = attrs
        .attributes()
        .and_then(|m| m.get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn))
        .unwrap()
        .clone();
    (queue_url, arn)
}

#[tokio::test]
async fn queue_policy_grants_send_without_identity_policy() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "qp_sender").await;
    let (queue_url, _) = seed_queue_with_policy(
        &server,
        "qp-shared",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":{"AWS":"arn:aws:iam::123456789012:user/qp_sender"},
            "Action":"sqs:SendMessage",
            "Resource":"*"
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("hello")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn queue_policy_explicit_deny_beats_identity_allow() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "qp_blocked").await;
    let (queue_url, queue_arn) = seed_queue_with_policy(
        &server,
        "qp-locked",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny",
            "Principal":{"AWS":"arn:aws:iam::123456789012:user/qp_blocked"},
            "Action":"sqs:SendMessage",
            "Resource":"*"
        }]}"#,
    )
    .await;
    let identity_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"sqs:SendMessage","Resource":"{queue_arn}"}}]}}"#
    );
    attach_inline_policy(&server, "qp_blocked", "AllowQp", &identity_policy).await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    let err = sqs
        .send_message()
        .queue_url(&queue_url)
        .message_body("nope")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied, got {err:?}"
    );
}

#[tokio::test]
async fn queue_policy_principal_wildcard_grants_any_user() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "qp_anyone").await;
    let (queue_url, _) = seed_queue_with_policy(
        &server,
        "qp-public",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Allow",
            "Principal":"*",
            "Action":"sqs:SendMessage",
            "Resource":"*"
        }]}"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let sqs = aws_sdk_sqs::Client::new(&cfg);
    sqs.send_message()
        .queue_url(&queue_url)
        .message_body("hi")
        .send()
        .await
        .unwrap();
}

/// UpdateObjectEncryption is authorized as `s3:UpdateObjectEncryption` on the
/// object: an Allow on the bucket ARN alone does not reach it, one on
/// `bucket/*` does.
#[tokio::test]
async fn s3_update_object_encryption_is_authorized_on_the_object() {
    let server = start_strict().await;
    let boot = sdk_config_with(&server, "test", "test").await;
    let root_s3 = path_style_s3(&boot);
    root_s3
        .create_bucket()
        .bucket("uoe-bucket")
        .send()
        .await
        .unwrap();
    root_s3
        .put_object()
        .bucket("uoe-bucket")
        .key("doc.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let key_arn = aws_sdk_kms::Client::new(&boot)
        .create_key()
        .send()
        .await
        .unwrap()
        .key_metadata()
        .unwrap()
        .arn()
        .unwrap()
        .to_string();
    let (akid, secret) = bootstrap_user(&server, "rekeyer").await;
    let s3 = path_style_s3(&sdk_config_with(&server, &akid, &secret).await);
    let update = || {
        s3.update_object_encryption()
            .bucket("uoe-bucket")
            .key("doc.txt")
            .object_encryption(aws_sdk_s3::types::ObjectEncryption::Ssekms(
                aws_sdk_s3::types::SsekmsEncryption::builder()
                    .kms_key_arn(&key_arn)
                    .build()
                    .unwrap(),
            ))
            .send()
    };
    let policy = |resource: &str| {
        format!(
            r#"{{"Version":"2012-10-17","Statement":[
                {{"Effect":"Allow","Action":"s3:UpdateObjectEncryption","Resource":"{resource}"}},
                {{"Effect":"Allow","Action":"kms:*","Resource":"*"}}]}}"#
        )
    };

    attach_inline_policy(
        &server,
        "rekeyer",
        "uoe",
        &policy("arn:aws:s3:::uoe-bucket"),
    )
    .await;
    let err = update()
        .await
        .expect_err("an Allow on the bucket ARN does not cover the object");
    assert!(format!("{err:?}").contains("AccessDenied"), "{err:?}");
    // s3:PutObject on the object is not the action either.
    attach_inline_policy(
        &server,
        "rekeyer",
        "uoe",
        &policy("arn:aws:s3:::uoe-bucket").replace(
            "s3:UpdateObjectEncryption\",\"Resource\":\"arn:aws:s3:::uoe-bucket\"",
            "s3:PutObject\",\"Resource\":\"arn:aws:s3:::uoe-bucket/*\"",
        ),
    )
    .await;
    let err = update()
        .await
        .expect_err("s3:PutObject does not authorize UpdateObjectEncryption");
    assert!(format!("{err:?}").contains("AccessDenied"), "{err:?}");

    attach_inline_policy(
        &server,
        "rekeyer",
        "uoe",
        &policy("arn:aws:s3:::uoe-bucket/*"),
    )
    .await;
    update()
        .await
        .expect("an Allow on bucket/* authorizes UpdateObjectEncryption");
}
