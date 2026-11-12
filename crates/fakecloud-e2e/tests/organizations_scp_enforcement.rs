//! End-to-end SCP enforcement tests.
//!
//! Drives real `aws-sdk-s3` + `aws-sdk-sqs` against fakecloud running
//! in `FAKECLOUD_IAM=strict`, with an organization and SCPs attached.
//! Covers the Batch 4 behavior contract:
//!
//! - Off by default (no org, `FAKECLOUD_IAM=strict`): no behavior change.
//! - Management account is always exempt from SCP enforcement.
//! - A custom SCP denying a service blocks member accounts.
//! - Detaching `FullAWSAccess` from root + attaching a restrictive
//!   SCP demonstrates allow-list ceiling semantics.
//! - SCPs cap what a resource policy grants a member-account principal, in
//!   its own account or another, and cap the member account's root user.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_iam::Client as IamClient;
use aws_sdk_kms::Client as KmsClient;
use aws_sdk_organizations::Client as OrgsClient;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SqsClient;
use aws_sdk_sts::Client as StsClient;
use helpers::TestServer;

const ACCOUNT_A: &str = "111111111111"; // management
const ACCOUNT_B: &str = "222222222222"; // member

async fn start() -> TestServer {
    TestServer::start_with_env(&[
        ("FAKECLOUD_IAM", "strict"),
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
        ("FAKECLOUD_CONTAINER_CLI", "false"),
    ])
    .await
}

async fn config_with(server: &TestServer, akid: &str, secret: &str) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(akid, secret, None, None, "scp-e2e"))
        .load()
        .await
}

/// Create the organization and return its id, which member bootstraps
/// pass to `create_admin_in_org` to enroll into it.
async fn create_org(orgs: &OrgsClient) -> String {
    orgs.create_organization()
        .send()
        .await
        .unwrap()
        .organization()
        .unwrap()
        .id()
        .unwrap()
        .to_string()
}

const SCP_ALLOW_ALL: &str =
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

const SCP_DENY_SQS: &str = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":"*","Resource":"*"},
        {"Effect":"Deny","Action":"sqs:*","Resource":"*"}
    ]}"#;

#[tokio::test]
async fn member_account_blocked_by_explicit_deny_scp() {
    let server = start().await;
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let orgs = OrgsClient::new(&a_cfg);

    // Management = A.
    let org_id = create_org(&orgs).await;
    let root_id = orgs.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();

    // Account B is bootstrapped as a member so SCPs apply to it.
    let (b_akid, b_secret) = server
        .create_admin_in_org(ACCOUNT_B, "admin-b", &org_id)
        .await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;

    // Baseline: before SCP attached, B can use SQS.
    let sqs_b = SqsClient::new(&b_cfg);
    sqs_b
        .create_queue()
        .queue_name("pre-scp-queue")
        .send()
        .await
        .unwrap();

    // Attach custom SCP that denies sqs:* to root.
    let policy = orgs
        .create_policy()
        .name("DenySqs")
        .description("")
        .r#type(aws_sdk_organizations::types::PolicyType::ServiceControlPolicy)
        .content(SCP_DENY_SQS)
        .send()
        .await
        .unwrap();
    let policy_id = policy
        .policy()
        .unwrap()
        .policy_summary()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    orgs.attach_policy()
        .policy_id(&policy_id)
        .target_id(&root_id)
        .send()
        .await
        .unwrap();

    // Member B is now blocked from SQS.
    let err = sqs_b
        .create_queue()
        .queue_name("post-scp-queue")
        .send()
        .await
        .unwrap_err();
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessDenied") || msg.contains("403"),
        "expected AccessDenied, got: {msg}"
    );

    // S3 still works for B — SCP only denies sqs:*.
    let s3_b = S3Client::new(&b_cfg);
    s3_b.create_bucket()
        .bucket("scp-allowed-bucket")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn management_account_exempt_from_scp() {
    let server = start().await;
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let orgs = OrgsClient::new(&a_cfg);

    orgs.create_organization().send().await.unwrap();
    let root_id = orgs.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();

    // Deny-all SCP attached to root.
    let policy = orgs
        .create_policy()
        .name("DenyEverything")
        .description("")
        .r#type(aws_sdk_organizations::types::PolicyType::ServiceControlPolicy)
        .content(
            r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Deny","Action":"*","Resource":"*"}
            ]}"#,
        )
        .send()
        .await
        .unwrap();
    let policy_id = policy
        .policy()
        .unwrap()
        .policy_summary()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    orgs.attach_policy()
        .policy_id(&policy_id)
        .target_id(&root_id)
        .send()
        .await
        .unwrap();

    // Management account A can still use SQS even with DenyAll SCP
    // attached to root — AWS always exempts the management account.
    let sqs_a = SqsClient::new(&a_cfg);
    sqs_a
        .create_queue()
        .queue_name("management-immune")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn detach_full_aws_access_then_custom_scp_controls_ceiling() {
    let server = start().await;
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let orgs = OrgsClient::new(&a_cfg);

    let org_id = create_org(&orgs).await;
    let root_id = orgs.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();

    let (b_akid, b_secret) = server
        .create_admin_in_org(ACCOUNT_B, "admin-b", &org_id)
        .await;
    let b_cfg = config_with(&server, &b_akid, &b_secret).await;
    let sqs_b = SqsClient::new(&b_cfg);
    let s3_b = S3Client::new(&b_cfg);

    // Detach FullAWSAccess from root. Now no SCP allow-all reaches B.
    orgs.detach_policy()
        .policy_id("p-FullAWSAccess")
        .target_id(&root_id)
        .send()
        .await
        .unwrap();

    // B cannot do anything that flows through an SCP-enforced service.
    let err = s3_b
        .create_bucket()
        .bucket("should-be-denied")
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("AccessDenied") || format!("{err:?}").contains("403"));

    // Attach an SCP that allows only SQS. S3 stays denied; SQS works.
    let policy = orgs
        .create_policy()
        .name("AllowSqsOnly")
        .description("")
        .r#type(aws_sdk_organizations::types::PolicyType::ServiceControlPolicy)
        .content(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*","Resource":"*"}]}"#,
        )
        .send()
        .await
        .unwrap();
    let policy_id = policy
        .policy()
        .unwrap()
        .policy_summary()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    orgs.attach_policy()
        .policy_id(&policy_id)
        .target_id(&root_id)
        .send()
        .await
        .unwrap();

    sqs_b
        .create_queue()
        .queue_name("sqs-allowed")
        .send()
        .await
        .unwrap();
    let err = s3_b
        .create_bucket()
        .bucket("still-denied")
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("AccessDenied") || format!("{err:?}").contains("403"));

    // Re-attach FullAWSAccess and detach the restrictive SCP — B's
    // ceiling is now allow-all again. AWS intersects SCPs across the
    // chain: with both `FullAWSAccess` (allow *) and `AllowSqsOnly`
    // attached, the intersection still caps at sqs only, so S3 stays
    // denied until the narrow SCP comes off.
    orgs.attach_policy()
        .policy_id("p-FullAWSAccess")
        .target_id(&root_id)
        .send()
        .await
        .unwrap();
    orgs.detach_policy()
        .policy_id(&policy_id)
        .target_id(&root_id)
        .send()
        .await
        .unwrap();
    s3_b.create_bucket()
        .bucket("open-again")
        .send()
        .await
        .unwrap();
    let _ = SCP_ALLOW_ALL; // constant kept for readability in the module
}

#[tokio::test]
async fn no_organization_is_zero_behavior_change() {
    let server = start().await;
    let (akid, secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let cfg = config_with(&server, &akid, &secret).await;
    let sqs = SqsClient::new(&cfg);
    // No CreateOrganization call — resolver returns None, evaluator
    // behaves exactly as before SCPs shipped.
    sqs.create_queue()
        .queue_name("pre-org-world")
        .send()
        .await
        .unwrap();
}

/// The org-perimeter bucket policy (`Deny` unless `aws:PrincipalOrgID` is the
/// organization). `StringNotEquals` is true on an absent key, so it only
/// works if fakecloud populates `aws:PrincipalOrgID` from the principal's
/// organization membership: an in-org account is let through, an account
/// outside the organization is denied.
#[tokio::test]
async fn principal_org_id_perimeter_admits_org_members_only() {
    let server = start().await;
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let a_cfg = config_with(&server, &a_akid, &a_secret).await;
    let org_id = create_org(&OrgsClient::new(&a_cfg)).await;
    let (b_akid, b_secret) = server
        .create_admin_in_org(ACCOUNT_B, "admin-b", &org_id)
        .await;
    let (c_akid, c_secret) = server.create_admin("333333333333", "admin-c").await;

    let s3 = |cfg: &aws_config::SdkConfig| {
        S3Client::from_conf(
            aws_sdk_s3::config::Builder::from(cfg)
                .force_path_style(true)
                .build(),
        )
    };
    let s3_a = s3(&a_cfg);
    s3_a.create_bucket()
        .bucket("org-only")
        .send()
        .await
        .unwrap();
    // The Allow statement grants `Principal:"*"`, a public policy the new
    // bucket's default Block Public Access would refuse.
    s3_a.delete_public_access_block()
        .bucket("org-only")
        .send()
        .await
        .unwrap();
    s3_a.put_object()
        .bucket("org-only")
        .key("k")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    s3_a.put_bucket_policy()
        .bucket("org-only")
        .policy(format!(
            r#"{{"Version":"2012-10-17","Statement":[
                {{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::org-only/*"}},
                {{"Effect":"Deny","Principal":"*","Action":"s3:*","Resource":["arn:aws:s3:::org-only","arn:aws:s3:::org-only/*"],
                  "Condition":{{"StringNotEquals":{{"aws:PrincipalOrgID":"{org_id}"}}}}}}
            ]}}"#
        ))
        .send()
        .await
        .unwrap();

    // The management account (the bucket owner) is in the org.
    s3_a.get_object()
        .bucket("org-only")
        .key("k")
        .send()
        .await
        .expect("the management account is in the org");
    // So is the member account: the perimeter Deny must not fire. (Only the
    // authorization decision is asserted here; what the cross-account read
    // returns past it is the S3 handler's business.)
    if let Err(e) = s3(&config_with(&server, &b_akid, &b_secret).await)
        .get_object()
        .bucket("org-only")
        .key("k")
        .send()
        .await
    {
        assert!(
            !format!("{e:?}").contains("AccessDenied"),
            "member account is in the org: {e:?}"
        );
    }
    // An account outside the organization is denied.
    let err = s3(&config_with(&server, &c_akid, &c_secret).await)
        .get_object()
        .bucket("org-only")
        .key("k")
        .send()
        .await
        .expect_err("an account outside the org must be denied");
    assert!(format!("{err:?}").contains("AccessDenied"), "{err:?}");
}

fn path_style_s3(cfg: &aws_config::SdkConfig) -> S3Client {
    S3Client::from_conf(
        aws_sdk_s3::config::Builder::from(cfg)
            .force_path_style(true)
            .build(),
    )
}

/// The organization (A managing, B a member) with B's admin, returning the
/// root id and both admins' configs.
async fn org_with_member(
    server: &TestServer,
) -> (String, aws_config::SdkConfig, aws_config::SdkConfig) {
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let a_cfg = config_with(server, &a_akid, &a_secret).await;
    let orgs = OrgsClient::new(&a_cfg);
    let org_id = create_org(&orgs).await;
    let root_id = orgs.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();
    let (b_akid, b_secret) = server
        .create_admin_in_org(ACCOUNT_B, "admin-b", &org_id)
        .await;
    let b_cfg = config_with(server, &b_akid, &b_secret).await;
    (root_id, a_cfg, b_cfg)
}

/// Create the SCP `content` and attach it to the organization root,
/// returning its id.
async fn attach_root_scp(orgs: &OrgsClient, root_id: &str, name: &str, content: &str) -> String {
    let policy = orgs
        .create_policy()
        .name(name)
        .description("")
        .r#type(aws_sdk_organizations::types::PolicyType::ServiceControlPolicy)
        .content(content)
        .send()
        .await
        .unwrap();
    let policy_id = policy
        .policy()
        .unwrap()
        .policy_summary()
        .unwrap()
        .id()
        .unwrap();
    orgs.attach_policy()
        .policy_id(policy_id)
        .target_id(root_id)
        .send()
        .await
        .unwrap();
    policy_id.to_string()
}

async fn detach_root_scp(orgs: &OrgsClient, root_id: &str, policy_id: &str) {
    orgs.detach_policy()
        .policy_id(policy_id)
        .target_id(root_id)
        .send()
        .await
        .unwrap();
}

async fn seed_bucket(s3: &S3Client, bucket: &str, policy: serde_json::Value) {
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    s3.put_object()
        .bucket(bucket)
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    s3.put_bucket_policy()
        .bucket(bucket)
        .policy(policy.to_string())
        .send()
        .await
        .unwrap();
}

fn read_grant(bucket: &str, principals: &[&str]) -> serde_json::Value {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": principals},
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{bucket}/*")
        }]
    })
}

fn assert_access_denied<E: std::fmt::Debug>(result: Result<impl std::fmt::Debug, E>, what: &str) {
    let err = result.expect_err(what);
    let msg = format!("{err:?}");
    assert!(msg.contains("AccessDenied"), "{what}: {msg}");
}

const SCP_DENY_GRANTED_ACTIONS: &str = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","Action":"*","Resource":"*"},
        {"Effect":"Deny","Action":["s3:GetObject","kms:Encrypt"],"Resource":"*"}
    ]}"#;

/// An allow-list SCP that allows everything but the granted actions.
const SCP_ALLOW_ALL_BUT_GRANTED_ACTIONS: &str = r#"{"Version":"2012-10-17","Statement":[
        {"Effect":"Allow","NotAction":["s3:GetObject","kms:Encrypt"],"Resource":"*"}
    ]}"#;

/// A customer key in `account` (as `cfg`'s admin) whose policy grants
/// `grantee` `kms:Encrypt` directly. Returns its ARN.
async fn key_granting(cfg: &aws_config::SdkConfig, account: &str, grantee: &str) -> String {
    KmsClient::new(cfg)
        .create_key()
        .policy(
            serde_json::json!({
                "Version": "2012-10-17",
                "Statement": [
                    {"Effect": "Allow", "Principal": {"AWS": format!("arn:aws:iam::{account}:root")}, "Action": "kms:*", "Resource": "*"},
                    {"Effect": "Allow", "Principal": {"AWS": grantee}, "Action": "kms:Encrypt", "Resource": "*"}
                ]
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap()
        .key_metadata()
        .unwrap()
        .arn()
        .unwrap()
        .to_string()
}

/// A resource policy cannot grant a member-account principal more than its
/// SCPs allow: not a bucket or key policy of another account, and not a
/// bucket or key policy of its own account naming an IAM user that has no
/// identity policy at all. The management account's principals stay exempt.
#[tokio::test]
async fn scp_caps_resource_policy_grants_to_member_principals() {
    let server = start().await;
    let (root_id, a_cfg, b_cfg) = org_with_member(&server).await;
    let admin_a = format!("arn:aws:iam::{ACCOUNT_A}:user/admin-a");
    let admin_b = format!("arn:aws:iam::{ACCOUNT_B}:user/admin-b");
    let reader_arn = format!("arn:aws:iam::{ACCOUNT_B}:user/reader");

    // A reader in B with no identity policy: only resource policies naming
    // it grant it anything.
    let iam_b = IamClient::new(&b_cfg);
    iam_b
        .create_user()
        .user_name("reader")
        .send()
        .await
        .unwrap();
    let key = iam_b
        .create_access_key()
        .user_name("reader")
        .send()
        .await
        .unwrap();
    let key = key.access_key().unwrap();
    let reader_cfg = config_with(&server, key.access_key_id(), key.secret_access_key()).await;

    // A's bucket and key grant B's admin.
    let s3_a = path_style_s3(&a_cfg);
    seed_bucket(
        &s3_a,
        "scp-a-shared",
        read_grant("scp-a-shared", &[&admin_b]),
    )
    .await;
    let a_key_arn = key_granting(&a_cfg, ACCOUNT_A, &admin_b).await;

    // B's own bucket grants its reader, and the management account's admin.
    let s3_b = path_style_s3(&b_cfg);
    seed_bucket(
        &s3_b,
        "scp-b-own",
        read_grant("scp-b-own", &[&reader_arn, &admin_a]),
    )
    .await;
    // B's own key's policy grants the reader directly.
    let b_key_arn = key_granting(&b_cfg, ACCOUNT_B, &reader_arn).await;

    let b_reads_a = || async {
        path_style_s3(&b_cfg)
            .get_object()
            .bucket("scp-a-shared")
            .key("k")
            .send()
            .await
    };
    let encrypt = |cfg: &aws_config::SdkConfig, key: &str| {
        let kms = KmsClient::new(cfg);
        let key = key.to_string();
        async move {
            kms.encrypt()
                .key_id(key)
                .plaintext(aws_sdk_kms::primitives::Blob::new(b"x".to_vec()))
                .send()
                .await
        }
    };
    let reader_reads_b = || async {
        path_style_s3(&reader_cfg)
            .get_object()
            .bucket("scp-b-own")
            .key("k")
            .send()
            .await
    };

    // The resource policies grant each request while the SCPs allow it.
    b_reads_a()
        .await
        .expect("A's bucket policy grants B's admin");
    encrypt(&b_cfg, &a_key_arn)
        .await
        .expect("A's key policy grants B's admin");
    reader_reads_b()
        .await
        .expect("B's bucket policy grants the reader");
    encrypt(&reader_cfg, &b_key_arn)
        .await
        .expect("B's key policy grants the reader");

    let orgs = OrgsClient::new(&a_cfg);
    // SCPs that deny the actions explicitly, then an allow-list that merely
    // leaves them out: either way no resource policy reaches past them.
    let deny = attach_root_scp(
        &orgs,
        &root_id,
        "DenyGrantedActions",
        SCP_DENY_GRANTED_ACTIONS,
    )
    .await;
    for ceiling in ["explicit deny", "allow-list"] {
        if ceiling == "allow-list" {
            detach_root_scp(&orgs, &root_id, &deny).await;
            detach_root_scp(&orgs, &root_id, "p-FullAWSAccess").await;
            attach_root_scp(
                &orgs,
                &root_id,
                "AllowAllButGrantedActions",
                SCP_ALLOW_ALL_BUT_GRANTED_ACTIONS,
            )
            .await;
        }
        assert_access_denied(
            b_reads_a().await,
            &format!("{ceiling}: cross-account bucket read"),
        );
        assert_access_denied(
            encrypt(&b_cfg, &a_key_arn).await,
            &format!("{ceiling}: cross-account key use"),
        );
        assert_access_denied(
            reader_reads_b().await,
            &format!("{ceiling}: same-account bucket read"),
        );
        assert_access_denied(
            encrypt(&reader_cfg, &b_key_arn).await,
            &format!("{ceiling}: same-account key use"),
        );

        // The management account is never capped by SCPs: its admin still
        // reads B's bucket through B's bucket policy, and its own.
        path_style_s3(&a_cfg)
            .get_object()
            .bucket("scp-b-own")
            .key("k")
            .send()
            .await
            .expect("SCPs do not apply to the management account");
        s3_a.get_object()
            .bucket("scp-a-shared")
            .key("k")
            .send()
            .await
            .expect("SCPs do not apply to the management account");
    }
}

/// Root credentials for `account`, minted by the management account's
/// admin through `sts:AssumeRoot` (centralized root access).
async fn root_session(
    server: &TestServer,
    management: &aws_config::SdkConfig,
    account: &str,
) -> aws_config::SdkConfig {
    let creds = StsClient::new(management)
        .assume_root()
        .target_principal(account)
        .task_policy_arn(
            aws_sdk_sts::types::PolicyDescriptorType::builder()
                .arn("arn:aws:iam::aws:policy/root-task/SQSUnlockQueuePolicy")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let creds = creds.credentials().unwrap();
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            Some(creds.session_token().to_string()),
            None,
            "scp-e2e-root",
        ))
        .load()
        .await
}

/// SCPs restrict a member account's root user: in its own account, and in
/// another account whose bucket policy grants it. The management account's
/// root is not restricted.
#[tokio::test]
async fn scp_caps_member_account_root() {
    let server = start().await;
    let (root_id, a_cfg, _b_cfg) = org_with_member(&server).await;
    IamClient::new(&a_cfg)
        .enable_organizations_root_sessions()
        .send()
        .await
        .unwrap();
    let b_root = root_session(&server, &a_cfg, ACCOUNT_B).await;
    let a_root = root_session(&server, &a_cfg, ACCOUNT_A).await;
    let caller = StsClient::new(&b_root)
        .get_caller_identity()
        .send()
        .await
        .unwrap();
    assert_eq!(
        caller.arn(),
        Some(format!("arn:aws:iam::{ACCOUNT_B}:root").as_str())
    );

    // A's bucket grants B's root.
    seed_bucket(
        &path_style_s3(&a_cfg),
        "scp-a-root-shared",
        read_grant(
            "scp-a-root-shared",
            &[&format!("arn:aws:iam::{ACCOUNT_B}:root")],
        ),
    )
    .await;
    let b_root_reads_a = || async {
        path_style_s3(&b_root)
            .get_object()
            .bucket("scp-a-root-shared")
            .key("k")
            .send()
            .await
    };
    let create_queue = |cfg: &aws_config::SdkConfig, name: &'static str| {
        let sqs = SqsClient::new(cfg);
        async move { sqs.create_queue().queue_name(name).send().await }
    };

    b_root_reads_a()
        .await
        .expect("A's bucket policy grants B's root");
    create_queue(&b_root, "b-root-before")
        .await
        .expect("root needs no identity policy in its own account");

    attach_root_scp(
        &OrgsClient::new(&a_cfg),
        &root_id,
        "DenyRootActions",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"*","Resource":"*"},
            {"Effect":"Deny","Action":["s3:GetObject","sqs:CreateQueue"],"Resource":"*"}
        ]}"#,
    )
    .await;

    let err = b_root_reads_a()
        .await
        .expect_err("B's SCP denies the read the bucket policy grants");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessDenied")
            && msg.contains("with an explicit deny in a service control policy"),
        "{msg}"
    );
    assert_access_denied(
        create_queue(&b_root, "b-root-after").await,
        "member root in its own account",
    );

    // The management account's root is exempt.
    create_queue(&a_root, "a-root-after")
        .await
        .expect("SCPs do not apply to the management account's root");
}
