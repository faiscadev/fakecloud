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
use aws_sdk_s3::error::ProvideErrorMetadata;
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

/// The access-denied code of S3 (`AccessDenied`) and of the JSON services
/// (`AccessDeniedException`).
const S3_DENIED: &str = "AccessDenied";
const JSON_DENIED: &str = "AccessDeniedException";
/// SQS's code as SDKs read it: the awsQuery `AccessDenied` that its JSON
/// errors carry in `x-amzn-query-error`.
const SQS_DENIED: &str = "AccessDenied";

/// Assert `result` is the service's IAM refusal (`code`) whose message
/// names the SCP the way AWS does (`reason`).
fn assert_scp_denied<E>(
    result: Result<impl std::fmt::Debug, E>,
    code: &str,
    reason: &str,
    what: &str,
) where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Debug,
{
    let err = result.expect_err(what);
    assert_eq!(err.code(), Some(code), "{what}: {err:?}");
    let message = err.message().unwrap_or_default();
    assert!(
        message.starts_with("User: ") && message.contains(&format!(" {reason}")),
        "{what}: {message}"
    );
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
        let reason = |action: &str| {
            if ceiling == "allow-list" {
                format!("because no service control policy allows the {action} action")
            } else {
                "with an explicit deny in a service control policy".to_string()
            }
        };
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
        assert_scp_denied(
            b_reads_a().await,
            S3_DENIED,
            &reason("s3:GetObject"),
            &format!("{ceiling}: cross-account bucket read"),
        );
        assert_scp_denied(
            encrypt(&b_cfg, &a_key_arn).await,
            JSON_DENIED,
            &reason("kms:Encrypt"),
            &format!("{ceiling}: cross-account key use"),
        );
        assert_scp_denied(
            reader_reads_b().await,
            S3_DENIED,
            &reason("s3:GetObject"),
            &format!("{ceiling}: same-account bucket read"),
        );
        assert_scp_denied(
            encrypt(&reader_cfg, &b_key_arn).await,
            JSON_DENIED,
            &reason("kms:Encrypt"),
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

/// Root credentials for the member `account`, minted by the management
/// account's admin through `sts:AssumeRoot` (centralized root access) and
/// scoped by AWS to the root-task policy `task_policy`.
async fn root_session(
    server: &TestServer,
    management: &aws_config::SdkConfig,
    account: &str,
    task_policy: &str,
) -> aws_config::SdkConfig {
    let creds = StsClient::new(management)
        .assume_root()
        .target_principal(account)
        .task_policy_arn(
            aws_sdk_sts::types::PolicyDescriptorType::builder()
                .arn(format!("arn:aws:iam::aws:policy/root-task/{task_policy}"))
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

/// SCPs restrict a member account's root user. Each privileged root session
/// is used only for actions its root-task policy allows on AWS
/// (`SQSUnlockQueuePolicy`: reading a queue's attributes;
/// `S3UnlockBucketPolicy`: reading a bucket's policy), so what changes once
/// an SCP is attached is due to the SCP alone. (The cross-account root path
/// is covered by the dispatch unit tests: no root-task policy allows using
/// another account's resource.)
#[tokio::test]
async fn scp_caps_member_account_root() {
    let server = start().await;
    let (root_id, a_cfg, b_cfg) = org_with_member(&server).await;
    IamClient::new(&a_cfg)
        .enable_organizations_root_sessions()
        .send()
        .await
        .unwrap();
    let sqs_root = root_session(&server, &a_cfg, ACCOUNT_B, "SQSUnlockQueuePolicy").await;
    let s3_root = root_session(&server, &a_cfg, ACCOUNT_B, "S3UnlockBucketPolicy").await;
    let caller = StsClient::new(&sqs_root)
        .get_caller_identity()
        .send()
        .await
        .unwrap();
    assert_eq!(
        caller.arn(),
        Some(format!("arn:aws:iam::{ACCOUNT_B}:root").as_str())
    );

    // B's queue and bucket (with a policy to read back).
    let queue_url = SqsClient::new(&b_cfg)
        .create_queue()
        .queue_name("scp-b-root-queue")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    seed_bucket(
        &path_style_s3(&b_cfg),
        "scp-b-root-bucket",
        read_grant(
            "scp-b-root-bucket",
            &[&format!("arn:aws:iam::{ACCOUNT_B}:user/admin-b")],
        ),
    )
    .await;
    let read_queue = || async {
        SqsClient::new(&sqs_root)
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(aws_sdk_sqs::types::QueueAttributeName::All)
            .send()
            .await
    };
    let read_bucket_policy = || async {
        path_style_s3(&s3_root)
            .get_bucket_policy()
            .bucket("scp-b-root-bucket")
            .send()
            .await
    };

    read_queue()
        .await
        .expect("root needs no identity policy in its own account");
    read_bucket_policy()
        .await
        .expect("root needs no identity policy in its own account");

    let orgs = OrgsClient::new(&a_cfg);
    let deny = attach_root_scp(
        &orgs,
        &root_id,
        "DenyRootTasks",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"*","Resource":"*"},
            {"Effect":"Deny","Action":["sqs:GetQueueAttributes","s3:GetBucketPolicy"],"Resource":"*"}
        ]}"#,
    )
    .await;
    assert_scp_denied(
        read_queue().await,
        SQS_DENIED,
        "with an explicit deny in a service control policy",
        "member root reading its queue",
    );
    assert_scp_denied(
        read_bucket_policy().await,
        S3_DENIED,
        "with an explicit deny in a service control policy",
        "member root reading its bucket policy",
    );

    // An allow-list SCP that leaves the actions out refuses them as well.
    detach_root_scp(&orgs, &root_id, &deny).await;
    detach_root_scp(&orgs, &root_id, "p-FullAWSAccess").await;
    attach_root_scp(
        &orgs,
        &root_id,
        "AllowAllButRootTasks",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","NotAction":["sqs:GetQueueAttributes","s3:GetBucketPolicy"],"Resource":"*"}
        ]}"#,
    )
    .await;
    assert_scp_denied(
        read_queue().await,
        SQS_DENIED,
        "because no service control policy allows the sqs:GetQueueAttributes action",
        "member root reading its queue",
    );
    assert_scp_denied(
        read_bucket_policy().await,
        S3_DENIED,
        "because no service control policy allows the s3:GetBucketPolicy action",
        "member root reading its bucket policy",
    );
}

/// The KMS key use S3 makes for a member-account caller (SSE-KMS) is capped
/// by the caller's SCPs, which see `kms:ViaService`; `SimulatePrincipalPolicy`
/// includes the SCPs and says whether they allow each action.
#[tokio::test]
async fn scps_cap_service_key_use_and_show_in_the_simulator() {
    let server = start().await;
    let (root_id, a_cfg, b_cfg) = org_with_member(&server).await;
    let s3 = path_style_s3(&b_cfg);
    s3.create_bucket().bucket("scp-sse").send().await.unwrap();
    let put = |sse: bool| {
        let mut req = s3
            .put_object()
            .bucket("scp-sse")
            .key("k")
            .body(ByteStream::from_static(b"x"));
        if sse {
            req = req.server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms);
        }
        req.send()
    };
    put(true).await.expect("SSE-KMS before any SCP");

    // Region and data-perimeter guardrails see the forwarded KMS call's
    // global keys (its region, the key's account and organization,
    // aws:ViaAWSService), so they let SSE-KMS through.
    let orgs = OrgsClient::new(&a_cfg);
    let guardrails = attach_root_scp(
        &orgs,
        &root_id,
        "RegionAndPerimeter",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Deny","Action":"*","Resource":"*",
             "Condition":{"StringNotEquals":{"aws:RequestedRegion":"us-east-1"}}},
            {"Effect":"Deny","Action":"*","Resource":"*",
             "Condition":{"StringNotEqualsIfExists":{"aws:ResourceOrgID":"${aws:PrincipalOrgID}"}}},
            {"Effect":"Deny","Action":"*","Resource":"*",
             "Condition":{"StringNotEqualsIfExists":{"aws:ResourceAccount":"222222222222"},
                          "BoolIfExists":{"aws:ViaAWSService":"false"}}},
            {"Effect":"Deny","Action":"kms:*","Resource":"*",
             "Condition":{"ForAnyValue:StringEquals":{"aws:CalledVia":"s3.amazonaws.com"}}}
        ]}"#,
    )
    .await;
    put(true).await.expect(
        "SSE-KMS inside the region and the perimeter; S3 is not s3.amazonaws.com in CalledVia",
    );
    detach_root_scp(&orgs, &root_id, &guardrails).await;
    // S3's forwarded calls name it fas.s3.amazonaws.com (recorded).
    let fas = attach_root_scp(
        &orgs,
        &root_id,
        "DenyKmsCalledViaS3",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Deny","Action":"kms:*","Resource":"*",
             "Condition":{"ForAnyValue:StringEquals":{"aws:CalledVia":"fas.s3.amazonaws.com"}}}
        ]}"#,
    )
    .await;
    assert_scp_denied(
        put(true).await,
        S3_DENIED,
        "with an explicit deny in a service control policy",
        "SSE-KMS write called via fas.s3.amazonaws.com",
    );
    detach_root_scp(&orgs, &root_id, &fas).await;

    attach_root_scp(
        &orgs,
        &root_id,
        "DenyKmsThroughS3",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"*","Resource":"*"},
            {"Effect":"Deny","Action":"kms:GenerateDataKey","Resource":"*",
             "Condition":{"StringLike":{"kms:ViaService":"s3.*.amazonaws.com"}}},
            {"Effect":"Deny","Action":"kms:CreateGrant","Resource":"*",
             "Condition":{"StringEquals":{"aws:CalledViaLast":"dynamodb.amazonaws.com"}}},
            {"Effect":"Deny","Action":"sqs:DeleteQueue","Resource":"*"}
        ]}"#,
    )
    .await;
    // DynamoDB creates a table's KMS grant with the caller's credentials.
    let err = aws_sdk_dynamodb::Client::new(&b_cfg)
        .create_table()
        .table_name("scp-sse-table")
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .sse_specification(
            aws_sdk_dynamodb::types::SseSpecification::builder()
                .enabled(true)
                .sse_type(aws_sdk_dynamodb::types::SseType::Kms)
                .kms_master_key_id("alias/aws/dynamodb")
                .build(),
        )
        .send()
        .await
        .expect_err("the SCP denies DynamoDB's kms:CreateGrant");
    assert_eq!(err.code(), Some(JSON_DENIED), "{err:?}");
    assert!(
        err.message()
            .unwrap_or_default()
            .contains("with an explicit deny in a service control policy"),
        "{err:?}"
    );
    assert_scp_denied(
        put(true).await,
        S3_DENIED,
        "with an explicit deny in a service control policy",
        "SSE-KMS write through S3",
    );
    put(false).await.expect("an SSE-S3 write calls no KMS");

    let results = IamClient::new(&b_cfg)
        .simulate_principal_policy()
        .policy_source_arn(format!("arn:aws:iam::{ACCOUNT_B}:user/admin-b"))
        .action_names("s3:GetObject")
        .action_names("sqs:DeleteQueue")
        .send()
        .await
        .unwrap();
    let decisions: Vec<(String, String, Option<bool>)> = results
        .evaluation_results()
        .iter()
        .map(|r| {
            (
                r.eval_action_name().to_string(),
                r.eval_decision().as_str().to_string(),
                r.organizations_decision_detail()
                    .map(|d| d.allowed_by_organizations()),
            )
        })
        .collect();
    assert_eq!(
        decisions,
        vec![
            (
                "s3:GetObject".to_string(),
                "allowed".to_string(),
                Some(true)
            ),
            (
                "sqs:DeleteQueue".to_string(),
                "explicitDeny".to_string(),
                Some(false)
            ),
        ]
    );
}

/// A forwarded KMS call the caller's SCPs refuse fails with each service's
/// own code, as recorded: S3 403 `AccessDenied`, SQS
/// `KMS.AccessDeniedException`, SNS `KMSAccessDenied`, SSM
/// `AccessDeniedException` naming `kms:Encrypt` (what SSM calls for a
/// SecureString), each relaying KMS's message.
#[tokio::test]
async fn refused_forwarded_kms_calls_use_each_services_code() {
    let server = start().await;
    let (root_id, a_cfg, b_cfg) = org_with_member(&server).await;
    let s3 = path_style_s3(&b_cfg);
    s3.create_bucket().bucket("scp-codes").send().await.unwrap();
    let sqs = SqsClient::new(&b_cfg);
    let queue_url = sqs
        .create_queue()
        .queue_name("scp-codes")
        .attributes(
            aws_sdk_sqs::types::QueueAttributeName::KmsMasterKeyId,
            "alias/aws/sqs",
        )
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let sns = aws_sdk_sns::Client::new(&b_cfg);
    let topic_arn = sns
        .create_topic()
        .name("scp-codes")
        .attributes("KmsMasterKeyId", "alias/aws/sns")
        .send()
        .await
        .unwrap()
        .topic_arn()
        .unwrap()
        .to_string();
    attach_root_scp(
        &OrgsClient::new(&a_cfg),
        &root_id,
        "DenyForwardedKms",
        r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"*","Resource":"*"},
            {"Effect":"Deny","Action":"kms:*","Resource":"*",
             "Condition":{"Bool":{"aws:ViaAWSService":"true"}}}
        ]}"#,
    )
    .await;
    let reason = "with an explicit deny in a service control policy";
    let names = |message: Option<&str>, action: &str| {
        let message = message.unwrap_or_default();
        assert!(
            message.starts_with(&format!(
                "User: arn:aws:iam::{ACCOUNT_B}:user/admin-b is not authorized to perform: {action} on resource: arn:aws:kms:"
            )) && message.contains(reason),
            "{message}"
        );
    };

    let err = s3
        .put_object()
        .bucket("scp-codes")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.raw_response().map(|r| r.status().as_u16()), Some(403));
    assert_eq!(err.code(), Some("AccessDenied"), "{err:?}");
    names(err.message(), "kms:GenerateDataKey");

    let err = sqs
        .send_message()
        .queue_url(&queue_url)
        .message_body("x")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("KMS.AccessDeniedException"), "{err:?}");
    names(err.message(), "kms:GenerateDataKey");

    let err = sns
        .publish()
        .topic_arn(&topic_arn)
        .message("x")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("KMSAccessDenied"), "{err:?}");
    names(err.message(), "kms:GenerateDataKey");

    let err = aws_sdk_ssm::Client::new(&b_cfg)
        .put_parameter()
        .name("/scp/codes")
        .value("secret")
        .r#type(aws_sdk_ssm::types::ParameterType::SecureString)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("AccessDeniedException"), "{err:?}");
    names(err.message(), "kms:Encrypt");
}

/// An `sts:AssumeRoot` session is scoped by its root-task policy: it performs
/// the privileged task the policy names and nothing else, while
/// `sts:GetCallerIdentity` needs no permission.
#[tokio::test]
async fn assume_root_session_is_scoped_to_its_task_policy() {
    let server = start().await;
    let (_, a_cfg, b_cfg) = org_with_member(&server).await;
    IamClient::new(&a_cfg)
        .enable_organizations_root_sessions()
        .send()
        .await
        .unwrap();
    let queue_url = SqsClient::new(&b_cfg)
        .create_queue()
        .queue_name("task-scoped-queue")
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    seed_bucket(
        &path_style_s3(&b_cfg),
        "task-scoped-bucket",
        read_grant(
            "task-scoped-bucket",
            &[&format!("arn:aws:iam::{ACCOUNT_B}:user/admin-b")],
        ),
    )
    .await;

    // S3UnlockBucketPolicy: read and delete the bucket policy.
    let s3_root = root_session(&server, &a_cfg, ACCOUNT_B, "S3UnlockBucketPolicy").await;
    let caller = StsClient::new(&s3_root)
        .get_caller_identity()
        .send()
        .await
        .expect("GetCallerIdentity needs no permission");
    assert_eq!(
        caller.arn(),
        Some(format!("arn:aws:iam::{ACCOUNT_B}:root").as_str())
    );
    let s3 = path_style_s3(&s3_root);
    s3.get_bucket_policy()
        .bucket("task-scoped-bucket")
        .send()
        .await
        .expect("s3:GetBucketPolicy");
    let err = s3
        .get_object()
        .bucket("task-scoped-bucket")
        .key("k")
        .send()
        .await
        .expect_err("the task policy denies s3:GetObject");
    assert_eq!(err.code(), Some(S3_DENIED), "{err:?}");
    let err = SqsClient::new(&s3_root)
        .get_queue_attributes()
        .queue_url(&queue_url)
        .send()
        .await
        .expect_err("an S3 task session cannot use SQS");
    assert_eq!(err.code(), Some(SQS_DENIED), "{err:?}");
    s3.delete_bucket_policy()
        .bucket("task-scoped-bucket")
        .send()
        .await
        .expect("s3:DeleteBucketPolicy");

    // SQSUnlockQueuePolicy: read the queue's attributes, not send to it.
    let sqs_root = root_session(&server, &a_cfg, ACCOUNT_B, "SQSUnlockQueuePolicy").await;
    let sqs = SqsClient::new(&sqs_root);
    sqs.get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::All)
        .send()
        .await
        .expect("sqs:GetQueueAttributes");
    let err = sqs
        .send_message()
        .queue_url(&queue_url)
        .message_body("x")
        .send()
        .await
        .expect_err("the task policy denies sqs:SendMessage");
    assert_eq!(err.code(), Some(SQS_DENIED), "{err:?}");

    // IAMAuditRootUserCredentials: audit the root user's own credentials
    // (no UserName: the calling root user), not other IAM users.
    let iam_root = root_session(&server, &a_cfg, ACCOUNT_B, "IAMAuditRootUserCredentials").await;
    let iam = IamClient::new(&iam_root);
    iam.list_access_keys()
        .send()
        .await
        .expect("iam:ListAccessKeys on the root user");
    let err = iam
        .list_access_keys()
        .user_name("admin-b")
        .send()
        .await
        .expect_err("auditing another user is denied");
    assert_eq!(err.code(), Some("AccessDenied"), "{err:?}");
    let err = iam
        .list_users()
        .send()
        .await
        .expect_err("iam:ListUsers is outside the task");
    assert_eq!(err.code(), Some("AccessDenied"), "{err:?}");
}

/// Create role `name` (under `path`) in `cfg`'s account, trusted by the
/// account and with `policy` inline when given, and return a session of it
/// assumed by `cfg`'s admin together with the role's ARN.
async fn role_session(
    server: &TestServer,
    cfg: &aws_config::SdkConfig,
    account: &str,
    path: &str,
    name: &str,
    policy: Option<&str>,
) -> (aws_config::SdkConfig, String) {
    let iam = IamClient::new(cfg);
    let role_arn = iam
        .create_role()
        .role_name(name)
        .path(path)
        .assume_role_policy_document(format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{account}:root"}},"Action":"sts:AssumeRole"}}]}}"#
        ))
        .send()
        .await
        .unwrap()
        .role()
        .unwrap()
        .arn()
        .to_string();
    if let Some(policy) = policy {
        iam.put_role_policy()
            .role_name(name)
            .policy_name("inline")
            .policy_document(policy)
            .send()
            .await
            .unwrap();
    }
    let creds = StsClient::new(cfg)
        .assume_role()
        .role_arn(&role_arn)
        .role_session_name("s1")
        .send()
        .await
        .unwrap();
    let creds = creds.credentials().unwrap();
    let session = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            Some(creds.session_token().to_string()),
            None,
            "scp-e2e-role",
        ))
        .load()
        .await;
    (session, role_arn)
}

/// An assumed-role session's `aws:PrincipalArn` is its role's ARN, path
/// included, not the session ARN: the "deny everyone but the Admin role"
/// SCP lets the Admin role's sessions through, and a bucket policy granting
/// `Principal: "*"` on `ArnEquals aws:PrincipalArn <role>` grants the role's
/// sessions.
#[tokio::test]
async fn principal_arn_of_a_role_session_is_its_role() {
    let server = start().await;
    let (root_id, a_cfg, b_cfg) = org_with_member(&server).await;
    let (admin, _) = role_session(
        &server,
        &b_cfg,
        ACCOUNT_B,
        "/",
        "Admin",
        Some(r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#),
    )
    .await;
    let (reader, reader_role) =
        role_session(&server, &b_cfg, ACCOUNT_B, "/team/", "Reader", None).await;

    seed_bucket(
        &path_style_s3(&b_cfg),
        "scp-principal-arn",
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::scp-principal-arn/*",
                "Condition": {"ArnEquals": {"aws:PrincipalArn": reader_role}}
            }]
        }),
    )
    .await;
    path_style_s3(&reader)
        .get_object()
        .bucket("scp-principal-arn")
        .key("k")
        .send()
        .await
        .expect("the bucket policy grants the Reader role's sessions");
    // A session outlives its role's deletion, and keeps the role's ARN as
    // its `aws:PrincipalArn`.
    IamClient::new(&b_cfg)
        .delete_role()
        .role_name("Reader")
        .send()
        .await
        .unwrap();
    path_style_s3(&reader)
        .get_object()
        .bucket("scp-principal-arn")
        .key("k")
        .send()
        .await
        .expect("the deleted role's session keeps its PrincipalArn");

    attach_root_scp(
        &OrgsClient::new(&a_cfg),
        &root_id,
        "OnlyAdmin",
        r#"{"Version":"2012-10-17","Statement":[{
            "Effect":"Deny","Action":"sqs:*","Resource":"*",
            "Condition":{"ArnNotLike":{"aws:PrincipalArn":"arn:aws:iam::*:role/Admin"}}
        }]}"#,
    )
    .await;
    SqsClient::new(&admin)
        .create_queue()
        .queue_name("admin-only")
        .send()
        .await
        .expect("the SCP exempts the Admin role's sessions");
    assert_scp_denied(
        SqsClient::new(&b_cfg)
            .create_queue()
            .queue_name("not-admin")
            .send()
            .await,
        SQS_DENIED,
        "with an explicit deny in a service control policy",
        "B's admin user is not the Admin role",
    );
}

/// The denial names the SCP only when the SCP layer is the cause: an
/// identity policy's explicit Deny is, even when the SCPs don't allow the
/// action either.
#[tokio::test]
async fn denial_names_the_scp_only_when_it_is_the_cause() {
    let server = start().await;
    let (root_id, a_cfg, b_cfg) = org_with_member(&server).await;
    let iam_b = IamClient::new(&b_cfg);
    iam_b
        .create_user()
        .user_name("denied")
        .send()
        .await
        .unwrap();
    iam_b
        .put_user_policy()
        .user_name("denied")
        .policy_name("deny-sqs")
        .policy_document(
            r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"*","Resource":"*"},
                {"Effect":"Deny","Action":"sqs:CreateQueue","Resource":"*"}
            ]}"#,
        )
        .send()
        .await
        .unwrap();
    let key = iam_b
        .create_access_key()
        .user_name("denied")
        .send()
        .await
        .unwrap();
    let key = key.access_key().unwrap();
    let denied_cfg = config_with(&server, key.access_key_id(), key.secret_access_key()).await;
    // An allow-list SCP that leaves out SQS.
    let orgs = OrgsClient::new(&a_cfg);
    detach_root_scp(&orgs, &root_id, "p-FullAWSAccess").await;
    attach_root_scp(
        &orgs,
        &root_id,
        "AllowAllButSqs",
        r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","NotAction":"sqs:*","Resource":"*"}]}"#,
    )
    .await;

    let err = SqsClient::new(&denied_cfg)
        .create_queue()
        .queue_name("q")
        .send()
        .await
        .expect_err("the identity policy denies");
    assert_eq!(err.code(), Some(SQS_DENIED), "{err:?}");
    // The JSON body keeps SQS's JSON error type.
    let body = err
        .raw_response()
        .and_then(|r| r.body().bytes())
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .unwrap_or_default();
    assert!(body.contains("AccessDeniedException"), "{body}");
    let message = err.message().unwrap_or_default();
    assert!(
        message.starts_with(&format!(
            "User: arn:aws:iam::{ACCOUNT_B}:user/denied is not authorized to perform: sqs:CreateQueue on resource: "
        )) && !message.contains("service control policy"),
        "{message}"
    );
    // B's admin, whom only the SCPs refuse, is told so.
    assert_scp_denied(
        SqsClient::new(&b_cfg)
            .create_queue()
            .queue_name("q")
            .send()
            .await,
        SQS_DENIED,
        "because no service control policy allows the sqs:CreateQueue action",
        "only the SCPs refuse B's admin",
    );
}
