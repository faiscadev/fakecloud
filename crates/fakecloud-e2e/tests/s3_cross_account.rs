//! Cross-account access to S3 buckets.
//!
//! Bucket names are one global namespace: a request naming account A's bucket
//! is served against A's bucket whichever account sends it. Under
//! `FAKECLOUD_IAM=strict` a principal in account B reaches A's bucket only when
//! both B's identity policy and A's bucket policy allow the action; with IAM
//! enforcement off the request is served in the owner's account unchecked.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client as S3Client;
use helpers::TestServer;

const ACCOUNT_A: &str = "123456789012";
const ACCOUNT_B: &str = "222222222222";
const REGION: &str = "us-east-1";

async fn start_strict() -> TestServer {
    TestServer::start_with_env(&[
        ("FAKECLOUD_IAM", "strict"),
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
    ])
    .await
}

async fn s3_admin_in(server: &TestServer, account: &str, name: &str) -> S3Client {
    let (akid, secret) = server.create_admin(account, name).await;
    let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new(REGION))
        .credentials_provider(Credentials::new(akid, secret, None, None, "s3-x-acct"))
        .load()
        .await;
    S3Client::from_conf(
        aws_sdk_s3::config::Builder::from(&cfg)
            .force_path_style(true)
            .build(),
    )
}

/// A bucket policy granting account B's principals `actions` on `bucket` and
/// its objects.
fn grant_account_b(bucket: &str, actions: &[&str]) -> String {
    serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": format!("arn:aws:iam::{ACCOUNT_B}:root")},
            "Action": actions,
            "Resource": [
                format!("arn:aws:s3:::{bucket}"),
                format!("arn:aws:s3:::{bucket}/*"),
            ]
        }]
    })
    .to_string()
}

fn err_code<T, E>(result: Result<T, aws_sdk_s3::error::SdkError<E>>) -> String
where
    E: aws_sdk_s3::error::ProvideErrorMetadata + std::fmt::Debug,
{
    match result {
        Ok(_) => "Ok".to_string(),
        Err(e) => {
            let status = e.raw_response().map(|r| r.status().as_u16());
            let code = e
                .as_service_error()
                .and_then(|s| s.code())
                .map(str::to_string);
            format!("{status:?} {code:?} {e:?}")
        }
    }
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

#[tokio::test]
async fn cross_account_object_access_needs_the_bucket_policy() {
    let server = start_strict().await;
    let owner = s3_admin_in(&server, ACCOUNT_A, "admin-a").await;
    let caller = s3_admin_in(&server, ACCOUNT_B, "admin-b").await;
    owner
        .create_bucket()
        .bucket("xacct-a")
        .send()
        .await
        .unwrap();
    put(&owner, "xacct-a", "seed.txt", b"from a").await;

    // No bucket policy: account B is refused, not told the bucket is missing.
    let denied = err_code(
        caller
            .get_object()
            .bucket("xacct-a")
            .key("seed.txt")
            .send()
            .await,
    );
    assert!(denied.contains("Some(403)"), "{denied}");
    let denied = err_code(
        caller
            .put_object()
            .bucket("xacct-a")
            .key("from-b.txt")
            .body(ByteStream::from_static(b"x"))
            .send()
            .await,
    );
    assert!(denied.contains("Some(403)"), "{denied}");

    owner
        .put_bucket_policy()
        .bucket("xacct-a")
        .policy(grant_account_b(
            "xacct-a",
            &["s3:GetObject", "s3:PutObject"],
        ))
        .send()
        .await
        .unwrap();

    // Granted: B reads A's object and writes into A's bucket.
    assert_eq!(get_body(&caller, "xacct-a", "seed.txt").await, b"from a");
    put(&caller, "xacct-a", "from-b.txt", b"from b").await;
    assert_eq!(get_body(&owner, "xacct-a", "from-b.txt").await, b"from b");

    // A key that does not exist is 403 for a caller the bucket does not let
    // list it (no s3:ListBucket), as on AWS, not NoSuchKey.
    let missing = err_code(
        caller
            .get_object()
            .bucket("xacct-a")
            .key("absent.txt")
            .send()
            .await,
    );
    assert!(
        missing.contains("Some(403)") && missing.contains("AccessDenied"),
        "{missing}"
    );

    // An action the bucket policy does not grant stays denied.
    let denied = err_code(
        caller
            .delete_object()
            .bucket("xacct-a")
            .key("seed.txt")
            .send()
            .await,
    );
    assert!(denied.contains("Some(403)"), "{denied}");

    // ListBuckets stays per account: B never lists A's bucket.
    let b_buckets = caller.list_buckets().send().await.unwrap();
    assert!(b_buckets.buckets().is_empty());
    let a_buckets = owner.list_buckets().send().await.unwrap();
    assert_eq!(a_buckets.buckets().len(), 1);
}

#[tokio::test]
async fn create_bucket_with_another_accounts_name_is_bucket_already_exists() {
    let server = start_strict().await;
    let owner = s3_admin_in(&server, ACCOUNT_A, "admin-a").await;
    let caller = s3_admin_in(&server, ACCOUNT_B, "admin-b").await;
    owner
        .create_bucket()
        .bucket("xacct-taken")
        .send()
        .await
        .unwrap();

    let taken = err_code(caller.create_bucket().bucket("xacct-taken").send().await);
    assert!(
        taken.contains("Some(409)") && taken.contains("BucketAlreadyExists"),
        "{taken}"
    );
    // The owner re-creating its own bucket in us-east-1 is idempotent.
    owner
        .create_bucket()
        .bucket("xacct-taken")
        .send()
        .await
        .unwrap();
    assert!(caller
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets()
        .is_empty());
}

#[tokio::test]
async fn cross_account_multipart_upload_lands_in_the_owners_bucket() {
    let server = start_strict().await;
    let owner = s3_admin_in(&server, ACCOUNT_A, "admin-a").await;
    let caller = s3_admin_in(&server, ACCOUNT_B, "admin-b").await;
    owner
        .create_bucket()
        .bucket("xacct-mpu")
        .send()
        .await
        .unwrap();
    owner
        .put_bucket_policy()
        .bucket("xacct-mpu")
        .policy(grant_account_b("xacct-mpu", &["s3:*"]))
        .send()
        .await
        .unwrap();

    let upload = caller
        .create_multipart_upload()
        .bucket("xacct-mpu")
        .key("big.bin")
        .send()
        .await
        .unwrap();
    let upload_id = upload.upload_id().unwrap();
    let part = caller
        .upload_part()
        .bucket("xacct-mpu")
        .key("big.bin")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"multipart from b"))
        .send()
        .await
        .unwrap();
    caller
        .complete_multipart_upload()
        .bucket("xacct-mpu")
        .key("big.bin")
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_body(&owner, "xacct-mpu", "big.bin").await,
        b"multipart from b"
    );

    // Tagging and versioning on the owner's bucket are reachable too.
    caller
        .put_object_tagging()
        .bucket("xacct-mpu")
        .key("big.bin")
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("by")
                        .value("b")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let tags = owner
        .get_object_tagging()
        .bucket("xacct-mpu")
        .key("big.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tag_set()[0].value(), "b");
    caller
        .get_bucket_versioning()
        .bucket("xacct-mpu")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn cross_account_copy_needs_read_on_the_source_bucket() {
    let server = start_strict().await;
    let owner = s3_admin_in(&server, ACCOUNT_A, "admin-a").await;
    let caller = s3_admin_in(&server, ACCOUNT_B, "admin-b").await;
    owner
        .create_bucket()
        .bucket("xacct-src")
        .send()
        .await
        .unwrap();
    put(&owner, "xacct-src", "doc.txt", b"copy me").await;
    caller
        .create_bucket()
        .bucket("xacct-dst")
        .send()
        .await
        .unwrap();

    // B owns the destination but A's bucket grants it nothing to read.
    let denied = err_code(
        caller
            .copy_object()
            .bucket("xacct-dst")
            .key("copy.txt")
            .copy_source("xacct-src/doc.txt")
            .send()
            .await,
    );
    assert!(denied.contains("Some(403)"), "{denied}");

    owner
        .put_bucket_policy()
        .bucket("xacct-src")
        .policy(grant_account_b("xacct-src", &["s3:GetObject"]))
        .send()
        .await
        .unwrap();
    caller
        .copy_object()
        .bucket("xacct-dst")
        .key("copy.txt")
        .copy_source("xacct-src/doc.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(get_body(&caller, "xacct-dst", "copy.txt").await, b"copy me");

    // UploadPartCopy reads the cross-account source the same way.
    let upload = caller
        .create_multipart_upload()
        .bucket("xacct-dst")
        .key("parts.txt")
        .send()
        .await
        .unwrap();
    let upload_id = upload.upload_id().unwrap();
    let part = caller
        .upload_part_copy()
        .bucket("xacct-dst")
        .key("parts.txt")
        .upload_id(upload_id)
        .part_number(1)
        .copy_source("xacct-src/doc.txt")
        .send()
        .await
        .unwrap();
    caller
        .complete_multipart_upload()
        .bucket("xacct-dst")
        .key("parts.txt")
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.copy_part_result().unwrap().e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_body(&caller, "xacct-dst", "parts.txt").await,
        b"copy me"
    );
}

#[tokio::test]
async fn cross_account_presigned_get_is_served_from_the_owners_bucket() {
    let server = start_strict().await;
    let owner = s3_admin_in(&server, ACCOUNT_A, "admin-a").await;
    let caller = s3_admin_in(&server, ACCOUNT_B, "admin-b").await;
    owner
        .create_bucket()
        .bucket("xacct-pre")
        .send()
        .await
        .unwrap();
    put(&owner, "xacct-pre", "p.txt", b"presigned").await;
    owner
        .put_bucket_policy()
        .bucket("xacct-pre")
        .policy(grant_account_b("xacct-pre", &["s3:GetObject"]))
        .send()
        .await
        .unwrap();

    let presigned = caller
        .get_object()
        .bucket("xacct-pre")
        .key("p.txt")
        .presigned(
            aws_sdk_s3::presigning::PresigningConfig::expires_in(std::time::Duration::from_secs(
                300,
            ))
            .unwrap(),
        )
        .await
        .unwrap();
    let resp = reqwest::get(presigned.uri()).await.unwrap();
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(&resp.bytes().await.unwrap()[..], b"presigned");
}

#[tokio::test]
async fn without_iam_enforcement_cross_account_requests_reach_the_owners_bucket() {
    let server = TestServer::start().await;
    let owner = s3_admin_in(&server, ACCOUNT_A, "admin-a").await;
    let caller = s3_admin_in(&server, ACCOUNT_B, "admin-b").await;
    owner
        .create_bucket()
        .bucket("xacct-open")
        .send()
        .await
        .unwrap();
    put(&owner, "xacct-open", "seed.txt", b"open").await;

    assert_eq!(get_body(&caller, "xacct-open", "seed.txt").await, b"open");
    put(&caller, "xacct-open", "from-b.txt", b"b wrote").await;
    assert_eq!(
        get_body(&owner, "xacct-open", "from-b.txt").await,
        b"b wrote"
    );
    let listed = caller
        .list_objects_v2()
        .bucket("xacct-open")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = listed.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["from-b.txt", "seed.txt"]);
    let missing = err_code(
        caller
            .get_object()
            .bucket("xacct-open")
            .key("absent.txt")
            .send()
            .await,
    );
    assert!(missing.contains("NoSuchKey"), "{missing}");
    let ghost = err_code(caller.head_bucket().bucket("xacct-ghost").send().await);
    assert!(ghost.contains("Some(404)"), "{ghost}");
    assert!(caller
        .list_buckets()
        .send()
        .await
        .unwrap()
        .buckets()
        .is_empty());
}
