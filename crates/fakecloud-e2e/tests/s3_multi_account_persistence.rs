//! S3 persistence keeps every bucket in the account that created it.
//!
//! The store lays buckets out per account (`s3/accounts/<account>/buckets/`),
//! and a restart hydrates each account from its own directory. Stores written
//! before that kept every bucket in one flat `s3/buckets/` directory; the
//! first start migrates them, each to the account that created it.

mod helpers;

use std::path::Path;

use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use helpers::TestServer;

/// The server's default account (`--account-id`).
const ACCOUNT_A: &str = "123456789012";
const ACCOUNT_B: &str = "222222222222";
const REGION: &str = "us-east-1";

fn s3_client(server: &TestServer, akid: &str, secret: &str) -> S3Client {
    let creds = Credentials::new(akid, secret, None, None, "s3-multi-account-persistence");
    S3Client::from_conf(
        aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .endpoint_url(server.endpoint())
            .region(aws_sdk_s3::config::Region::new(REGION))
            .credentials_provider(creds)
            .force_path_style(true)
            .build(),
    )
}

async fn put(client: &S3Client, bucket: &str, key: &str, body: &'static [u8]) {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from_static(body))
        .send()
        .await
        .unwrap();
}

async fn get_body(client: &S3Client, bucket: &str, key: &str) -> Vec<u8> {
    client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes()
        .to_vec()
}

async fn bucket_names(client: &S3Client) -> Vec<String> {
    client
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets()
        .iter()
        .filter_map(|b| b.name().map(str::to_string))
        .collect()
}

/// Two accounts' clients plus one bucket with one object in each.
async fn seed(server: &TestServer) -> ((String, String), (String, String)) {
    let creds_a = server.create_admin(ACCOUNT_A, "admin-a").await;
    let creds_b = server.create_admin(ACCOUNT_B, "admin-b").await;
    let a = s3_client(server, &creds_a.0, &creds_a.1);
    let b = s3_client(server, &creds_b.0, &creds_b.1);
    a.create_bucket()
        .bucket("bucket-of-a")
        .send()
        .await
        .unwrap();
    b.create_bucket()
        .bucket("bucket-of-b")
        .send()
        .await
        .unwrap();
    put(&a, "bucket-of-a", "obj.txt", b"written by a").await;
    put(&b, "bucket-of-b", "obj.txt", b"written by b").await;
    (creds_a, creds_b)
}

async fn assert_each_account_sees_its_own(a: &S3Client, b: &S3Client) {
    assert_eq!(bucket_names(a).await, ["bucket-of-a"]);
    assert_eq!(bucket_names(b).await, ["bucket-of-b"]);
    assert_eq!(get_body(a, "bucket-of-a", "obj.txt").await, b"written by a");
    assert_eq!(get_body(b, "bucket-of-b", "obj.txt").await, b"written by b");
    // The bucket's owner is still the account that created it.
    let acl = b
        .get_bucket_acl()
        .bucket("bucket-of-b")
        .send()
        .await
        .unwrap();
    assert_eq!(acl.owner().and_then(|o| o.id()), Some(ACCOUNT_B));
}

#[tokio::test]
async fn buckets_of_every_account_survive_a_restart_in_their_own_account() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let ((akid_a, secret_a), (akid_b, secret_b)) = seed(&server).await;

    let s3_root = tmp.path().join("s3");
    for (account, bucket) in [(ACCOUNT_A, "bucket-of-a"), (ACCOUNT_B, "bucket-of-b")] {
        assert!(
            s3_root
                .join("accounts")
                .join(account)
                .join("buckets")
                .join(bucket)
                .join("meta.toml")
                .is_file(),
            "{bucket} should be stored under {account}"
        );
    }

    server.restart().await;
    let a = s3_client(&server, &akid_a, &secret_a);
    let b = s3_client(&server, &akid_b, &secret_b);
    assert_each_account_sees_its_own(&a, &b).await;

    // The secondary account's hydrated buckets are fully wired: writes persist
    // (and stream back) across another restart, and deletes stick.
    put(&b, "bucket-of-b", "after-restart.txt", b"second life").await;
    b.delete_object()
        .bucket("bucket-of-b")
        .key("obj.txt")
        .send()
        .await
        .unwrap();
    server.restart().await;
    let a = s3_client(&server, &akid_a, &secret_a);
    let b = s3_client(&server, &akid_b, &secret_b);
    assert_eq!(
        get_body(&b, "bucket-of-b", "after-restart.txt").await,
        b"second life"
    );
    let gone = b
        .get_object()
        .bucket("bucket-of-b")
        .key("obj.txt")
        .send()
        .await
        .unwrap_err();
    assert!(format!("{gone:?}").contains("NoSuchKey"), "{gone:?}");
    // Still one global namespace after the restart.
    let taken = a
        .create_bucket()
        .bucket("bucket-of-b")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{taken:?}").contains("BucketAlreadyExists"),
        "{taken:?}"
    );
}

/// Rewrite the store into the flat layout fakecloud used before buckets were
/// kept per account: every bucket under `s3/buckets/`, metadata without an
/// `account_id`.
fn rewrite_as_flat_layout(s3_root: &Path) {
    let flat = s3_root.join("buckets");
    std::fs::create_dir_all(&flat).unwrap();
    for account in std::fs::read_dir(s3_root.join("accounts")).unwrap() {
        let buckets = account.unwrap().path().join("buckets");
        let Ok(entries) = std::fs::read_dir(&buckets) else {
            continue;
        };
        for bucket in entries {
            let bucket = bucket.unwrap();
            let dest = flat.join(bucket.file_name());
            std::fs::rename(bucket.path(), &dest).unwrap();
            let meta_path = dest.join("meta.toml");
            let meta = std::fs::read_to_string(&meta_path).unwrap();
            let legacy: String = meta
                .lines()
                .filter(|line| !line.starts_with("account_id"))
                .map(|line| format!("{line}\n"))
                .collect();
            assert_ne!(legacy, meta, "meta.toml should have named the account");
            std::fs::write(&meta_path, legacy).unwrap();
        }
    }
    std::fs::remove_dir_all(s3_root.join("accounts")).unwrap();
}

#[tokio::test]
async fn flat_layout_store_migrates_each_bucket_to_the_account_that_created_it() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let ((akid_a, secret_a), (akid_b, secret_b)) = seed(&server).await;

    let s3_root = tmp.path().join("s3");
    server
        .restart_after(|| rewrite_as_flat_layout(&s3_root))
        .await;

    assert!(!s3_root.join("buckets").exists(), "the flat layout is gone");
    assert!(s3_root
        .join("accounts")
        .join(ACCOUNT_B)
        .join("buckets/bucket-of-b/meta.toml")
        .is_file());
    let a = s3_client(&server, &akid_a, &secret_a);
    let b = s3_client(&server, &akid_b, &secret_b);
    assert_each_account_sees_its_own(&a, &b).await;

    // The migrated layout is the one every later start loads.
    server.restart().await;
    let a = s3_client(&server, &akid_a, &secret_a);
    let b = s3_client(&server, &akid_b, &secret_b);
    assert_each_account_sees_its_own(&a, &b).await;
}
