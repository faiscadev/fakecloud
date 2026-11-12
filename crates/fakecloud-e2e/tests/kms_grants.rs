//! KMS grants authorize direct KMS API calls under `--iam strict`.
//!
//! A grant is KMS's third authorization mechanism next to key policies and
//! IAM policies: its grantee may perform the grant's operations on the key
//! without any IAM permission, within the grant's encryption-context
//! constraints, in the key's account or another; an explicit Deny in a
//! policy still wins, and revoking the grant removes the access.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_kms::error::ProvideErrorMetadata;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{GrantConstraints, GrantOperation};
use aws_sdk_kms::Client as KmsClient;
use helpers::TestServer;

const ACCOUNT_A: &str = "123456789012";
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
        .credentials_provider(Credentials::new(akid, secret, None, None, "kms-grants"))
        .load()
        .await
}

/// An admin of `account` and its KMS client.
async fn admin(server: &TestServer, account: &str) -> (aws_config::SdkConfig, KmsClient) {
    let (akid, secret) = server
        .create_admin(account, &format!("admin-{account}"))
        .await;
    let cfg = config_with(server, &akid, &secret).await;
    let kms = KmsClient::new(&cfg);
    (cfg, kms)
}

/// A user `name` with no policies in the account `admin_cfg` belongs to, its
/// ARN, and a KMS client signed with its access key. `policy` (an identity
/// policy document) is attached inline when given.
async fn user(
    server: &TestServer,
    admin_cfg: &aws_config::SdkConfig,
    name: &str,
    policy: Option<&str>,
) -> (String, KmsClient) {
    let iam = aws_sdk_iam::Client::new(admin_cfg);
    let created = iam.create_user().user_name(name).send().await.unwrap();
    let arn = created.user().unwrap().arn().to_string();
    if let Some(policy) = policy {
        iam.put_user_policy()
            .user_name(name)
            .policy_name("kms")
            .policy_document(policy)
            .send()
            .await
            .unwrap();
    }
    let key = iam
        .create_access_key()
        .user_name(name)
        .send()
        .await
        .unwrap()
        .access_key()
        .unwrap()
        .clone();
    let cfg = config_with(server, key.access_key_id(), key.secret_access_key()).await;
    (arn, KmsClient::new(&cfg))
}

/// Assert a KMS call (a plain or boxed SDK error) was refused with
/// `AccessDeniedException`.
fn denied<E: AccessCode, R: std::fmt::Debug>(result: Result<R, E>, what: &str) {
    match result {
        Ok(out) => panic!("{what}: expected AccessDeniedException, got {out:?}"),
        Err(err) => assert_eq!(err.error_code(), Some("AccessDeniedException"), "{what}"),
    }
}

/// The AWS error code of an SDK error, boxed or not.
trait AccessCode {
    fn error_code(&self) -> Option<&str>;
}

impl<E: ProvideErrorMetadata, R> AccessCode for aws_sdk_kms::error::SdkError<E, R> {
    fn error_code(&self) -> Option<&str> {
        self.code()
    }
}

impl<T: AccessCode> AccessCode for Box<T> {
    fn error_code(&self) -> Option<&str> {
        (**self).error_code()
    }
}

async fn encrypt(
    kms: &KmsClient,
    key: &str,
    context: Option<(&str, &str)>,
) -> Result<
    aws_sdk_kms::operation::encrypt::EncryptOutput,
    Box<aws_sdk_kms::error::SdkError<aws_sdk_kms::operation::encrypt::EncryptError>>,
> {
    let mut req = kms
        .encrypt()
        .key_id(key)
        .plaintext(Blob::new(b"granted".to_vec()));
    if let Some((k, v)) = context {
        req = req.encryption_context(k, v);
    }
    req.send().await.map_err(Box::new)
}

async fn decrypt(
    kms: &KmsClient,
    blob: &Blob,
    context: Option<(&str, &str)>,
) -> Result<
    aws_sdk_kms::operation::decrypt::DecryptOutput,
    Box<aws_sdk_kms::error::SdkError<aws_sdk_kms::operation::decrypt::DecryptError>>,
> {
    let mut req = kms.decrypt().ciphertext_blob(blob.clone());
    if let Some((k, v)) = context {
        req = req.encryption_context(k, v);
    }
    req.send().await.map_err(Box::new)
}

async fn create_key(kms: &KmsClient, policy: Option<String>) -> String {
    let mut req = kms.create_key();
    if let Some(policy) = policy {
        req = req.policy(policy);
    }
    req.send()
        .await
        .unwrap()
        .key_metadata()
        .unwrap()
        .arn()
        .unwrap()
        .to_string()
}

async fn grant(
    kms: &KmsClient,
    key: &str,
    grantee: &str,
    operations: &[GrantOperation],
    constraints: Option<GrantConstraints>,
) -> (String, String) {
    let mut req = kms.create_grant().key_id(key).grantee_principal(grantee);
    for op in operations {
        req = req.operations(op.clone());
    }
    if let Some(constraints) = constraints {
        req = req.constraints(constraints);
    }
    let out = req.send().await.unwrap();
    (
        out.grant_id().unwrap().to_string(),
        out.grant_token().unwrap().to_string(),
    )
}

/// A user with no IAM permission is refused until the key owner grants it
/// Encrypt and Decrypt; the grant covers only its operations, its
/// encryption-context constraint gates the operations it covers, and
/// revoking it removes the access.
#[tokio::test]
async fn grants_authorize_their_grantee_in_the_key_account() {
    let server = start().await;
    let (admin_cfg, owner) = admin(&server, ACCOUNT_A).await;
    let key = create_key(&owner, None).await;
    let (grantee_arn, grantee) = user(&server, &admin_cfg, "grantee", None).await;

    let sealed = encrypt(&owner, &key, None).await.unwrap();
    let blob = sealed.ciphertext_blob().unwrap().clone();
    denied(
        encrypt(&grantee, &key, None).await,
        "Encrypt before the grant",
    );
    denied(
        decrypt(&grantee, &blob, None).await,
        "Decrypt before the grant",
    );

    let (grant_id, grant_token) = grant(
        &owner,
        &key,
        &grantee_arn,
        &[GrantOperation::Encrypt, GrantOperation::Decrypt],
        None,
    )
    .await;
    let mine = encrypt(&grantee, &key, None)
        .await
        .expect("granted Encrypt");
    assert_eq!(mine.key_id(), Some(key.as_str()));
    let opened = decrypt(&grantee, &blob, None)
        .await
        .expect("granted Decrypt names no key: the ciphertext's key is authorized");
    assert_eq!(opened.plaintext().unwrap().as_ref(), b"granted");
    // A grant token is accepted (fakecloud's grants are consistent at once).
    grantee
        .encrypt()
        .key_id(&key)
        .plaintext(Blob::new(b"x".to_vec()))
        .grant_tokens(&grant_token)
        .send()
        .await
        .expect("Encrypt carrying the grant token");

    // Operations outside the grant stay refused.
    denied(
        grantee
            .generate_data_key()
            .key_id(&key)
            .number_of_bytes(32)
            .send()
            .await,
        "GenerateDataKey outside the grant",
    );
    denied(
        grantee.describe_key().key_id(&key).send().await,
        "DescribeKey outside the grant",
    );
    denied(
        grantee.disable_key().key_id(&key).send().await,
        "DisableKey (never grantable)",
    );

    // An EncryptionContextSubset constraint: only a request carrying the
    // pair is granted.
    grant(
        &owner,
        &key,
        &grantee_arn,
        &[GrantOperation::GenerateDataKey],
        Some(
            GrantConstraints::builder()
                .encryption_context_subset("purpose", "test")
                .build(),
        ),
    )
    .await;
    let gdk = |context: Option<(&'static str, &'static str)>| {
        let mut req = grantee.generate_data_key().key_id(&key).number_of_bytes(32);
        if let Some((k, v)) = context {
            req = req.encryption_context(k, v);
        }
        req.send()
    };
    gdk(Some(("purpose", "test")))
        .await
        .expect("GenerateDataKey with the constrained context");
    denied(
        gdk(Some(("purpose", "prod"))).await,
        "GenerateDataKey, other context",
    );
    denied(gdk(None).await, "GenerateDataKey, no context");

    // Revoking the grant removes the access.
    owner
        .revoke_grant()
        .key_id(&key)
        .grant_id(&grant_id)
        .send()
        .await
        .unwrap();
    denied(encrypt(&grantee, &key, None).await, "Encrypt after revoke");
    denied(decrypt(&grantee, &blob, None).await, "Decrypt after revoke");
}

/// An explicit Deny in the key policy beats a grant; the grant still
/// authorizes the operations the policy does not deny.
#[tokio::test]
async fn an_explicit_key_policy_deny_beats_a_grant() {
    let server = start().await;
    let (admin_cfg, owner) = admin(&server, ACCOUNT_A).await;
    let (grantee_arn, grantee) = user(&server, &admin_cfg, "denied-grantee", None).await;
    let policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[
            {{"Sid":"Owner","Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{ACCOUNT_A}:root"}},"Action":"kms:*","Resource":"*"}},
            {{"Sid":"NoDecrypt","Effect":"Deny","Principal":{{"AWS":"{grantee_arn}"}},"Action":"kms:Decrypt","Resource":"*"}}]}}"#
    );
    let key = create_key(&owner, Some(policy)).await;
    grant(
        &owner,
        &key,
        &grantee_arn,
        &[GrantOperation::Encrypt, GrantOperation::Decrypt],
        None,
    )
    .await;
    let sealed = encrypt(&grantee, &key, None)
        .await
        .expect("granted Encrypt");
    denied(
        decrypt(&grantee, sealed.ciphertext_blob().unwrap(), None).await,
        "Decrypt the key policy denies",
    );
}

/// A user of another account uses the key through a grant naming it, with
/// no IAM permission and a key policy that grants its account nothing; a
/// grant naming the other account only delegates, so its users then need
/// an IAM policy too.
#[tokio::test]
async fn a_grant_lets_another_accounts_identity_use_the_key() {
    let server = start().await;
    let (_, owner) = admin(&server, ACCOUNT_A).await;
    let (b_admin_cfg, _) = admin(&server, ACCOUNT_B).await;
    let key = create_key(&owner, None).await;
    let (b_arn, b_user) = user(&server, &b_admin_cfg, "b-user", None).await;

    denied(
        encrypt(&b_user, &key, None).await,
        "cross-account Encrypt, no grant",
    );

    grant(
        &owner,
        &key,
        &b_arn,
        &[GrantOperation::Encrypt, GrantOperation::Decrypt],
        None,
    )
    .await;
    let sealed = encrypt(&b_user, &key, Some(("tenant", "b")))
        .await
        .expect("cross-account Encrypt through the grant");
    assert_eq!(sealed.key_id(), Some(key.as_str()));
    let blob = sealed.ciphertext_blob().unwrap().clone();
    let opened = decrypt(&b_user, &blob, Some(("tenant", "b")))
        .await
        .expect("cross-account Decrypt naming no key");
    assert_eq!(opened.plaintext().unwrap().as_ref(), b"granted");
    assert_eq!(opened.key_id(), Some(key.as_str()));
    // The key owner opens what the other account sealed.
    decrypt(&owner, &blob, Some(("tenant", "b")))
        .await
        .expect("the owner decrypts");
    denied(
        b_user.describe_key().key_id(&key).send().await,
        "cross-account DescribeKey outside the grant",
    );

    // A grant naming account B: B's users need an IAM policy as well.
    grant(
        &owner,
        &key,
        &format!("arn:aws:iam::{ACCOUNT_B}:root"),
        &[GrantOperation::GenerateDataKey],
        None,
    )
    .await;
    let gdk = |kms: &KmsClient| {
        kms.generate_data_key()
            .key_id(&key)
            .number_of_bytes(32)
            .send()
    };
    denied(gdk(&b_user).await, "account grant, no IAM policy");
    let iam_policy = format!(
        r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":"kms:GenerateDataKey","Resource":"{key}"}}]}}"#
    );
    let (_, b_delegated) = user(&server, &b_admin_cfg, "b-delegated", Some(&iam_policy)).await;
    gdk(&b_delegated)
        .await
        .expect("account grant delegated by an IAM policy");
}

/// RetireGrant is authorized by the grant it retires: its retiring principal
/// (also from another account, also by grant token), its grantee when the
/// grant allows RetireGrant, or an IAM-allowed principal of the key's
/// account. A grant allowing RetireGrant gives its grantee no say over other
/// grants, and another account's administrator none over the key's grants.
#[tokio::test]
async fn retire_grant_follows_the_grant_it_names() {
    let server = start().await;
    let (admin_cfg, owner) = admin(&server, ACCOUNT_A).await;
    let (b_admin_cfg, b_admin) = admin(&server, ACCOUNT_B).await;
    let key = create_key(&owner, None).await;
    let (alice_arn, alice) = user(&server, &admin_cfg, "alice", None).await;
    let (bob_arn, bob) = user(&server, &admin_cfg, "bob", None).await;
    let (carol_arn, carol) = user(&server, &b_admin_cfg, "carol", None).await;

    let create = |grantee: String, retiring: Option<String>, operations: Vec<GrantOperation>| {
        let mut req = owner
            .create_grant()
            .key_id(&key)
            .grantee_principal(grantee)
            .set_operations(Some(operations));
        if let Some(retiring) = retiring {
            req = req.retiring_principal(retiring);
        }
        async move {
            let out = req.send().await.unwrap();
            (
                out.grant_id().unwrap().to_string(),
                out.grant_token().unwrap().to_string(),
            )
        }
    };
    let (alices, _) = create(
        alice_arn.clone(),
        None,
        vec![GrantOperation::Decrypt, GrantOperation::RetireGrant],
    )
    .await;
    let (bobs, bobs_token) = create(
        bob_arn.clone(),
        Some(carol_arn.clone()),
        vec![GrantOperation::Decrypt],
    )
    .await;
    let (bobs_second, _) = create(bob_arn.clone(), None, vec![GrantOperation::Decrypt]).await;
    let retire =
        |kms: &KmsClient, grant_id: &str| kms.retire_grant().key_id(&key).grant_id(grant_id).send();

    denied(retire(&alice, &bobs).await, "alice retires bob's grant");
    denied(
        retire(&bob, &bobs).await,
        "bob's grant does not allow RetireGrant",
    );
    denied(
        retire(&b_admin, &bobs_second).await,
        "another account's admin retires a grant naming nobody of it",
    );
    retire(&alice, &alices)
        .await
        .expect("alice retires her own grant, which allows RetireGrant");
    carol
        .retire_grant()
        .grant_token(&bobs_token)
        .send()
        .await
        .expect("the retiring principal of another account retires by token");
    retire(&owner, &bobs_second)
        .await
        .expect("an IAM-allowed principal of the key's account retires any grant");
    let remaining = owner.list_grants().key_id(&key).send().await.unwrap();
    assert!(remaining.grants().is_empty(), "{remaining:?}");
}

/// A grant to a role is capped by the session policies of the session using
/// it, as a grant naming the session itself is not.
#[tokio::test]
async fn session_policies_cap_a_grant_to_the_role() {
    let server = start().await;
    let (admin_cfg, owner) = admin(&server, ACCOUNT_A).await;
    let key = create_key(&owner, None).await;
    let iam = aws_sdk_iam::Client::new(&admin_cfg);
    let role_arn = iam
        .create_role()
        .role_name("grantee-role")
        .path("/team/")
        .assume_role_policy_document(format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{ACCOUNT_A}:root"}},"Action":"sts:AssumeRole"}}]}}"#
        ))
        .send()
        .await
        .unwrap()
        .role()
        .unwrap()
        .arn()
        .to_string();
    grant(&owner, &key, &role_arn, &[GrantOperation::Encrypt], None).await;
    let sts = aws_sdk_sts::Client::new(&admin_cfg);
    let session = |name: &'static str, policy: Option<&'static str>| {
        let mut req = sts
            .assume_role()
            .role_arn(&role_arn)
            .role_session_name(name);
        if let Some(policy) = policy {
            req = req.policy(policy);
        }
        let server = &server;
        async move {
            let out = req.send().await.unwrap();
            let creds = out.credentials().unwrap();
            let arn = out.assumed_role_user().unwrap().arn().to_string();
            let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .endpoint_url(server.endpoint())
                .region(aws_config::Region::new("us-east-1"))
                .credentials_provider(Credentials::new(
                    creds.access_key_id(),
                    creds.secret_access_key(),
                    Some(creds.session_token().to_string()),
                    None,
                    "kms-grants-session",
                ))
                .load()
                .await;
            (arn, KmsClient::new(&cfg))
        }
    };
    let s3_only = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;

    let (_, open) = session("open", None).await;
    encrypt(&open, &key, None)
        .await
        .expect("a session of the role uses the grant to the role");
    let (capped_arn, capped) = session("capped", Some(s3_only)).await;
    denied(
        encrypt(&capped, &key, None).await,
        "session policy without kms caps the grant to the role",
    );
    // A grant naming the session itself is not limited by its session policy.
    grant(&owner, &key, &capped_arn, &[GrantOperation::Encrypt], None).await;
    encrypt(&capped, &key, None)
        .await
        .expect("a grant to the session itself");
}

/// A role `name` of account A that A's root may assume, with `boundary` (a
/// managed policy ARN) as its permissions boundary; its ARN.
async fn role_in_a(iam: &aws_sdk_iam::Client, name: &str, boundary: Option<&str>) -> String {
    let mut req = iam
        .create_role()
        .role_name(name)
        .assume_role_policy_document(format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"AWS":"arn:aws:iam::{ACCOUNT_A}:root"}},"Action":"sts:AssumeRole"}}]}}"#
        ));
    if let Some(boundary) = boundary {
        req = req.permissions_boundary(boundary);
    }
    req.send().await.unwrap().role().unwrap().arn().to_string()
}

/// A KMS client signed with a fresh session of `role_arn`, assumed with
/// `admin_cfg`'s credentials, and the session's ARN.
async fn session_of(
    server: &TestServer,
    admin_cfg: &aws_config::SdkConfig,
    role_arn: &str,
) -> (String, KmsClient) {
    let out = aws_sdk_sts::Client::new(admin_cfg)
        .assume_role()
        .role_arn(role_arn)
        .role_session_name("bounded")
        .send()
        .await
        .unwrap();
    let creds = out.credentials().unwrap();
    let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            creds.access_key_id(),
            creds.secret_access_key(),
            Some(creds.session_token().to_string()),
            None,
            "kms-grants-boundary",
        ))
        .load()
        .await;
    (
        out.assumed_role_user().unwrap().arn().to_string(),
        KmsClient::new(&cfg),
    )
}

/// A grant to a role is capped by the role's permissions boundary, as a
/// grant naming the role's session, or an IAM user, is not.
#[tokio::test]
async fn a_roles_permissions_boundary_caps_a_grant_to_the_role() {
    let server = start().await;
    let (admin_cfg, owner) = admin(&server, ACCOUNT_A).await;
    let key = create_key(&owner, None).await;
    let iam = aws_sdk_iam::Client::new(&admin_cfg);
    let boundary = iam
        .create_policy()
        .policy_name("S3Only")
        .policy_document(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#,
        )
        .send()
        .await
        .unwrap()
        .policy()
        .unwrap()
        .arn()
        .unwrap()
        .to_string();

    let open_role = role_in_a(&iam, "unbounded-role", None).await;
    grant(&owner, &key, &open_role, &[GrantOperation::Encrypt], None).await;
    let (_, open) = session_of(&server, &admin_cfg, &open_role).await;
    encrypt(&open, &key, None)
        .await
        .expect("no boundary: the grant to the role stands");

    let bounded_role = role_in_a(&iam, "bounded-role", Some(&boundary)).await;
    grant(
        &owner,
        &key,
        &bounded_role,
        &[GrantOperation::Encrypt],
        None,
    )
    .await;
    let (session_arn, bounded) = session_of(&server, &admin_cfg, &bounded_role).await;
    denied(
        encrypt(&bounded, &key, None).await,
        "the role's boundary (s3 only) caps the grant to the role",
    );
    grant(&owner, &key, &session_arn, &[GrantOperation::Encrypt], None).await;
    encrypt(&bounded, &key, None)
        .await
        .expect("a grant to the session itself is not capped by the boundary");

    // A grant naming an IAM user is not capped by the user's boundary.
    let (user_arn, user_kms) = user(&server, &admin_cfg, "bounded-user", None).await;
    iam.put_user_permissions_boundary()
        .user_name("bounded-user")
        .permissions_boundary(&boundary)
        .send()
        .await
        .unwrap();
    grant(&owner, &key, &user_arn, &[GrantOperation::Encrypt], None).await;
    encrypt(&user_kms, &key, None)
        .await
        .expect("a grant to the user itself");
}

/// Each key of a multi-Region set has its own grants: a key id names the
/// key in the request's region, and a multi-Region ciphertext is decrypted
/// by the related key of the Decrypt call's region, under that key's grants.
#[tokio::test]
async fn multi_region_grants_follow_the_region_of_the_call() {
    let server = start().await;
    let (admin_akid, admin_secret) = server.create_admin(ACCOUNT_A, "admin-mr").await;
    let in_region = |akid: String, secret: String, region: &'static str| {
        let endpoint = server.endpoint().to_string();
        async move {
            let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .endpoint_url(endpoint)
                .region(aws_config::Region::new(region))
                .credentials_provider(Credentials::new(akid, secret, None, None, "kms-mr"))
                .load()
                .await;
            (KmsClient::new(&cfg), cfg)
        }
    };
    let (owner_east, admin_cfg) =
        in_region(admin_akid.clone(), admin_secret.clone(), "us-east-1").await;
    let (owner_west, _) = in_region(admin_akid, admin_secret, "us-west-2").await;
    let iam = aws_sdk_iam::Client::new(&admin_cfg);
    let grantee_arn = iam
        .create_user()
        .user_name("mr-grantee")
        .send()
        .await
        .unwrap()
        .user()
        .unwrap()
        .arn()
        .to_string();
    let keys = iam
        .create_access_key()
        .user_name("mr-grantee")
        .send()
        .await
        .unwrap()
        .access_key()
        .unwrap()
        .clone();
    let (grantee_east, _) = in_region(
        keys.access_key_id().to_string(),
        keys.secret_access_key().to_string(),
        "us-east-1",
    )
    .await;
    let (grantee_west, _) = in_region(
        keys.access_key_id().to_string(),
        keys.secret_access_key().to_string(),
        "us-west-2",
    )
    .await;

    let key_id = owner_east
        .create_key()
        .multi_region(true)
        .send()
        .await
        .unwrap()
        .key_metadata()
        .unwrap()
        .key_id()
        .to_string();
    owner_east
        .replicate_key()
        .key_id(&key_id)
        .replica_region("us-west-2")
        .send()
        .await
        .unwrap();
    // Sealed through the replica, by its bare id in its region.
    let sealed = encrypt(&owner_west, &key_id, None).await.unwrap();
    assert!(sealed.key_id().unwrap().contains(":us-west-2:"));
    let blob = sealed.ciphertext_blob().unwrap().clone();
    // A grant made with the bare id in us-west-2 is the replica's.
    grant(
        &owner_west,
        &key_id,
        &grantee_arn,
        &[GrantOperation::Decrypt],
        None,
    )
    .await;
    let opened = decrypt(&grantee_west, &blob, None)
        .await
        .expect("decrypt in the replica's region under the replica's grant");
    assert!(opened.key_id().unwrap().contains(":us-west-2:"));
    denied(
        decrypt(&grantee_east, &blob, None).await,
        "the primary has no grant: decrypt in its region is refused",
    );
    // A grant on the primary covers the primary's region only.
    let primary_only = "mr-primary-only";
    let other = iam
        .create_user()
        .user_name(primary_only)
        .send()
        .await
        .unwrap()
        .user()
        .unwrap()
        .arn()
        .to_string();
    let other_keys = iam
        .create_access_key()
        .user_name(primary_only)
        .send()
        .await
        .unwrap()
        .access_key()
        .unwrap()
        .clone();
    grant(
        &owner_east,
        &key_id,
        &other,
        &[GrantOperation::Decrypt],
        None,
    )
    .await;
    let (other_east, _) = in_region(
        other_keys.access_key_id().to_string(),
        other_keys.secret_access_key().to_string(),
        "us-east-1",
    )
    .await;
    let (other_west, _) = in_region(
        other_keys.access_key_id().to_string(),
        other_keys.secret_access_key().to_string(),
        "us-west-2",
    )
    .await;
    decrypt(&other_east, &blob, None)
        .await
        .expect("the primary's grant, in the primary's region");
    denied(
        decrypt(&other_west, &blob, None).await,
        "the primary's grant does not cover the replica",
    );
}

/// Encryption-context constraints are refused on a grant whose operations
/// include one that takes no encryption context.
#[tokio::test]
async fn context_constraints_need_context_operations() {
    let server = start().await;
    let (_, owner) = admin(&server, ACCOUNT_A).await;
    let key = create_key(&owner, None).await;
    let err = owner
        .create_grant()
        .key_id(&key)
        .grantee_principal(format!("arn:aws:iam::{ACCOUNT_A}:user/nobody"))
        .operations(GrantOperation::Decrypt)
        .operations(GrantOperation::GetPublicKey)
        .constraints(
            GrantConstraints::builder()
                .encryption_context_subset("k", "v")
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("ValidationException"));
}
