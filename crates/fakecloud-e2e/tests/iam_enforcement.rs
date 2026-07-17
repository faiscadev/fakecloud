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
        msg.contains("AccessDeniedException"),
        "expected AccessDeniedException, got {msg}"
    );
}

/// bug-hunt 2026-06-13, finding 5.1: enabling `--verify-sigv4` alongside
/// `--iam` is what actually binds a request to a real identity — an
/// unknown access key id is then rejected with InvalidClientTokenId before
/// any handler runs. (Without `--verify-sigv4`, an unresolved key falls
/// through unenforced — the same path the local-dev bootstrap relies on —
/// which is why the server logs a loud startup warning recommending
/// `--verify-sigv4`; that gap is inherent to not verifying signatures and
/// can't be closed without breaking the bootstrap, so it's surfaced, not
/// silently "fixed".)
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
    assert!(msg.contains("AccessDeniedException"), "got {msg}");
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
    assert!(format!("{err:?}").contains("AccessDeniedException"));
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
    assert!(format!("{err:?}").contains("AccessDeniedException"));
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

#[tokio::test]
async fn s3_list_objects_v2_prefix_condition_key() {
    // A policy that only allows ListObjectsV2 when `s3:prefix` starts
    // with "logs/" must let `prefix=logs/2026/` through and deny
    // `prefix=secrets/`.
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
            "Action":"s3:ListObjectsV2",
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
        format!("{err:?}").contains("AccessDenied"),
        "expected AccessDenied for email protocol, got {err:?}"
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
    assert!(format!("{err:?}").contains("AccessDeniedException"));
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
    assert!(format!("{err:?}").contains("AccessDeniedException"));
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
    assert!(format!("{err:?}").contains("AccessDeniedException"));
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
    assert!(format!("{err:?}").contains("AccessDeniedException"));
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
    assert_eq!(
        resp.status(),
        403,
        "strict mode must deny an enforceable action with no IAM-action mapping (fail closed)"
    );
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
        format!("{err:?}").contains("AccessDeniedException"),
        "expected AccessDeniedException, got {err:?}"
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
        format!("{err:?}").contains("AccessDeniedException"),
        "expected AccessDeniedException, got {err:?}"
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
        format!("{err:?}").contains("AccessDeniedException"),
        "expected AccessDeniedException, got {err:?}"
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
