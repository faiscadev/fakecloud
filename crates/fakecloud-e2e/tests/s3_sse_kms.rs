//! S3 SSE-KMS end-to-end:
//! - PutObject with `ServerSideEncryption=aws:kms` triggers
//!   `kms:GenerateDataKey` with the object-ARN encryption context
//! - GetObject decrypts via `kms:Decrypt` and returns the original bytes
//! - Both calls land in `/_fakecloud/kms/usage`
//! - Range reads against an SSE-KMS object slice plaintext, not ciphertext

mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ServerSideEncryption;
use helpers::TestServer;

#[tokio::test]
async fn s3_put_get_sse_kms_round_trip() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let http = reqwest::Client::new();

    s3.create_bucket().bucket("encrypted").send().await.unwrap();
    s3.put_object()
        .bucket("encrypted")
        .key("note.txt")
        .body(ByteStream::from_static(b"top-secret-payload"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("alias/aws/s3")
        .send()
        .await
        .unwrap();

    let got = s3
        .get_object()
        .bucket("encrypted")
        .key("note.txt")
        .send()
        .await
        .unwrap();
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        &bytes[..],
        b"top-secret-payload",
        "GetObject must return plaintext after SSE-KMS round-trip"
    );

    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    let s3_records: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["servicePrincipal"].as_str() == Some("s3.amazonaws.com"))
        .collect();
    assert!(
        s3_records.iter().any(|r| {
            r["operation"].as_str() == Some("GenerateDataKey")
                && r["encryptionContext"]["aws:s3:arn"].as_str()
                    == Some("arn:aws:s3:::encrypted/note.txt")
        }),
        "expected GenerateDataKey bound to bucket arn, got: {s3_records:?}"
    );
    assert!(
        s3_records.iter().any(|r| {
            r["operation"].as_str() == Some("Decrypt")
                && r["encryptionContext"]["aws:s3:arn"].as_str()
                    == Some("arn:aws:s3:::encrypted/note.txt")
        }),
        "expected Decrypt bound to bucket arn, got: {s3_records:?}"
    );
}

#[tokio::test]
async fn s3_sse_kms_range_read_returns_plaintext_slice() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("ranges").send().await.unwrap();
    s3.put_object()
        .bucket("ranges")
        .key("doc")
        .body(ByteStream::from_static(b"abcdefghij"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id("alias/aws/s3")
        .send()
        .await
        .unwrap();

    let got = s3
        .get_object()
        .bucket("ranges")
        .key("doc")
        .range("bytes=2-5")
        .send()
        .await
        .unwrap();
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        &bytes[..],
        b"cdef",
        "ranged read of SSE-KMS object must slice plaintext, not the stored ciphertext envelope"
    );
}

#[tokio::test]
async fn s3_plain_put_does_not_invoke_kms() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let http = reqwest::Client::new();

    s3.create_bucket().bucket("plain").send().await.unwrap();
    s3.put_object()
        .bucket("plain")
        .key("k")
        .body(ByteStream::from_static(b"v"))
        .send()
        .await
        .unwrap();

    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    assert!(
        !records
            .iter()
            .any(|r| r["servicePrincipal"].as_str() == Some("s3.amazonaws.com")),
        "PutObject without SSE-KMS must not record KMS usage, got: {records:?}"
    );
}

// ---- Multipart uploads under SSE-KMS ----

use aws_sdk_s3::types::{
    CompletedMultipartUpload, CompletedPart, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
};

const MPU_MARKER: &[u8] = b"MPU-SSE-KMS-PLAINTEXT-MARKER";

/// The plaintext of the two-part uploads below: a 5 MiB first part and a
/// tail carrying a marker the on-disk check looks for.
fn mpu_parts() -> (Vec<u8>, Vec<u8>) {
    // ASCII, so a body stored unencrypted cannot pass for a non-envelope
    // legacy body on the read path.
    let first: Vec<u8> = (0..5 * 1024 * 1024)
        .map(|i| b'a' + (i % 26) as u8)
        .collect();
    let mut tail = b"tail:".to_vec();
    tail.extend_from_slice(MPU_MARKER);
    (first, tail)
}

async fn set_default_encryption(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    algorithm: ServerSideEncryption,
    key: Option<&str>,
    bucket_key: bool,
) {
    let mut default = ServerSideEncryptionByDefault::builder().sse_algorithm(algorithm);
    if let Some(key) = key {
        default = default.kms_master_key_id(key);
    }
    s3.put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(
            ServerSideEncryptionConfiguration::builder()
                .rules(
                    ServerSideEncryptionRule::builder()
                        .apply_server_side_encryption_by_default(default.build().unwrap())
                        .bucket_key_enabled(bucket_key)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

async fn create_kms_key(server: &TestServer) -> String {
    server
        .kms_client()
        .await
        .create_key()
        .send()
        .await
        .unwrap()
        .key_metadata()
        .unwrap()
        .arn()
        .unwrap()
        .to_string()
}

/// Run a two-part upload of [`mpu_parts`] to `bucket/key`, asserting that
/// CreateMultipartUpload, every UploadPart and CompleteMultipartUpload report
/// `expected_sse` with `expected_key` (and the bucket key setting), as S3
/// does.
async fn multipart_upload(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    explicit_key: Option<&str>,
    expected_sse: &ServerSideEncryption,
    expected_key: Option<&str>,
    expected_bucket_key: Option<bool>,
) {
    let mut create = s3.create_multipart_upload().bucket(bucket).key(key);
    if let Some(k) = explicit_key {
        create = create
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(k);
    }
    let created = create.send().await.unwrap();
    assert_eq!(created.server_side_encryption(), Some(expected_sse));
    assert_eq!(created.ssekms_key_id(), expected_key);
    assert_eq!(created.bucket_key_enabled(), expected_bucket_key);
    let upload_id = created.upload_id().unwrap();

    let (first, tail) = mpu_parts();
    let mut parts = Vec::new();
    for (n, data) in [(1, first), (2, tail)] {
        let out = s3
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .part_number(n)
            .body(ByteStream::from(data))
            .send()
            .await
            .unwrap();
        assert_eq!(out.server_side_encryption(), Some(expected_sse));
        assert_eq!(out.ssekms_key_id(), expected_key);
        assert_eq!(out.bucket_key_enabled(), expected_bucket_key);
        parts.push(
            CompletedPart::builder()
                .part_number(n)
                .e_tag(out.e_tag().unwrap())
                .build(),
        );
    }
    let done = s3
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(parts))
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(done.server_side_encryption(), Some(expected_sse));
    assert_eq!(done.ssekms_key_id(), expected_key);
    assert_eq!(done.bucket_key_enabled(), expected_bucket_key);
}

/// GetObject returns the uploaded plaintext and HeadObject reports the
/// encryption the object was stored under.
async fn assert_reads_back(
    s3: &aws_sdk_s3::Client,
    bucket: &str,
    key: &str,
    expected_sse: &ServerSideEncryption,
    expected_key: Option<&str>,
) {
    let (first, tail) = mpu_parts();
    let got = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(got.server_side_encryption(), Some(expected_sse));
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.len(), first.len() + tail.len());
    assert!(body.starts_with(&first[..1024]));
    assert!(body.ends_with(&tail));

    let head = s3
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(head.server_side_encryption(), Some(expected_sse));
    assert_eq!(head.ssekms_key_id(), expected_key);
    assert_eq!(
        head.content_length(),
        Some((first.len() + tail.len()) as i64)
    );
    assert!(head.e_tag().unwrap().ends_with("-2\""));

    // A range read slices the plaintext.
    let range = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .range(format!("bytes={}-", first.len()))
        .send()
        .await
        .unwrap();
    let range = range.body.collect().await.unwrap().into_bytes();
    assert_eq!(&range[..], &tail[..]);
}

/// A bucket whose default encryption is SSE-KMS encrypts a multipart upload
/// that names no encryption: every multipart response reports `aws:kms` and
/// the key, the stored object decrypts on GetObject, HeadObject reports the
/// key, and a CopyObject of it decrypts the source.
#[tokio::test]
async fn s3_multipart_upload_takes_default_sse_kms() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let key_arn = create_kms_key(&server).await;
    s3.create_bucket()
        .bucket("mpu-kms-default")
        .send()
        .await
        .unwrap();
    set_default_encryption(
        &s3,
        "mpu-kms-default",
        ServerSideEncryption::AwsKms,
        Some(&key_arn),
        true,
    )
    .await;

    multipart_upload(
        &s3,
        "mpu-kms-default",
        "big.bin",
        None,
        &ServerSideEncryption::AwsKms,
        Some(&key_arn),
        Some(true),
    )
    .await;
    assert_reads_back(
        &s3,
        "mpu-kms-default",
        "big.bin",
        &ServerSideEncryption::AwsKms,
        Some(&key_arn),
    )
    .await;

    s3.copy_object()
        .bucket("mpu-kms-default")
        .key("copy.bin")
        .copy_source("mpu-kms-default/big.bin")
        .send()
        .await
        .unwrap();
    let copy = s3
        .get_object()
        .bucket("mpu-kms-default")
        .key("copy.bin")
        .send()
        .await
        .unwrap();
    let copy = copy.body.collect().await.unwrap().into_bytes();
    assert!(copy.ends_with(MPU_MARKER));
}

/// SSE-KMS named on CreateMultipartUpload encrypts the completed object
/// under that key, in a bucket with no default encryption.
#[tokio::test]
async fn s3_multipart_upload_with_explicit_sse_kms() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let key_arn = create_kms_key(&server).await;
    s3.create_bucket()
        .bucket("mpu-kms-explicit")
        .send()
        .await
        .unwrap();

    multipart_upload(
        &s3,
        "mpu-kms-explicit",
        "big.bin",
        Some(&key_arn),
        &ServerSideEncryption::AwsKms,
        Some(&key_arn),
        None,
    )
    .await;
    assert_reads_back(
        &s3,
        "mpu-kms-explicit",
        "big.bin",
        &ServerSideEncryption::AwsKms,
        Some(&key_arn),
    )
    .await;
}

/// A bucket default of SSE-S3 is reported on every multipart response.
#[tokio::test]
async fn s3_multipart_upload_takes_default_sse_s3() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("mpu-sse-s3")
        .send()
        .await
        .unwrap();
    set_default_encryption(&s3, "mpu-sse-s3", ServerSideEncryption::Aes256, None, true).await;

    multipart_upload(
        &s3,
        "mpu-sse-s3",
        "big.bin",
        None,
        &ServerSideEncryption::Aes256,
        None,
        Some(true),
    )
    .await;
    assert_reads_back(
        &s3,
        "mpu-sse-s3",
        "big.bin",
        &ServerSideEncryption::Aes256,
        None,
    )
    .await;
}

/// In persistent mode the SSE-KMS multipart object is stored encrypted (no
/// file under the data directory holds its plaintext) and still decrypts
/// after a restart.
#[tokio::test]
async fn s3_multipart_sse_kms_is_encrypted_at_rest_and_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let s3 = server.s3_client().await;
    let key_arn = create_kms_key(&server).await;
    s3.create_bucket()
        .bucket("mpu-kms-disk")
        .send()
        .await
        .unwrap();
    set_default_encryption(
        &s3,
        "mpu-kms-disk",
        ServerSideEncryption::AwsKms,
        Some(&key_arn),
        false,
    )
    .await;
    multipart_upload(
        &s3,
        "mpu-kms-disk",
        "big.bin",
        None,
        &ServerSideEncryption::AwsKms,
        Some(&key_arn),
        // S3 reports the bucket key header only when it is enabled.
        None,
    )
    .await;
    s3.create_bucket()
        .bucket("mpu-kms-disk-explicit")
        .send()
        .await
        .unwrap();
    multipart_upload(
        &s3,
        "mpu-kms-disk-explicit",
        "big.bin",
        Some(&key_arn),
        &ServerSideEncryption::AwsKms,
        Some(&key_arn),
        None,
    )
    .await;

    let mut stack = vec![tmp.path().to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(bytes) = std::fs::read(&path) {
                assert!(
                    !bytes.windows(MPU_MARKER.len()).any(|w| w == MPU_MARKER),
                    "plaintext found at rest in {}",
                    path.display()
                );
            }
        }
    }

    server.restart().await;
    let s3 = server.s3_client().await;
    for bucket in ["mpu-kms-disk", "mpu-kms-disk-explicit"] {
        assert_reads_back(
            &s3,
            bucket,
            "big.bin",
            &ServerSideEncryption::AwsKms,
            Some(&key_arn),
        )
        .await;
    }
}

/// PutBucketEncryption accepts an `aws:kms` default with no
/// `KMSMasterKeyID`, as AWS does, and GetBucketEncryption returns it as
/// stored. Objects written under it are encrypted under the account's
/// AWS-managed `aws/s3` key, which HeadObject reports by ARN; GetObject
/// returns the plaintext.
#[tokio::test]
async fn s3_default_sse_kms_without_a_key_uses_aws_s3() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("kms-no-key")
        .send()
        .await
        .unwrap();
    s3.put_bucket_encryption()
        .bucket("kms-no-key")
        .server_side_encryption_configuration(
            ServerSideEncryptionConfiguration::builder()
                .rules(
                    ServerSideEncryptionRule::builder()
                        .apply_server_side_encryption_by_default(
                            ServerSideEncryptionByDefault::builder()
                                .sse_algorithm(ServerSideEncryption::AwsKms)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("aws:kms without KMSMasterKeyID is accepted");

    let config = s3
        .get_bucket_encryption()
        .bucket("kms-no-key")
        .send()
        .await
        .unwrap();
    let default = config
        .server_side_encryption_configuration()
        .unwrap()
        .rules()[0]
        .apply_server_side_encryption_by_default()
        .unwrap()
        .clone();
    assert_eq!(default.sse_algorithm(), &ServerSideEncryption::AwsKms);
    assert_eq!(default.kms_master_key_id(), None);

    let put = s3
        .put_object()
        .bucket("kms-no-key")
        .key("doc.txt")
        .body(ByteStream::from_static(b"under the managed key"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        put.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );

    let managed_key = server
        .kms_client()
        .await
        .describe_key()
        .key_id("alias/aws/s3")
        .send()
        .await
        .unwrap()
        .key_metadata()
        .unwrap()
        .arn()
        .unwrap()
        .to_string();
    assert_eq!(put.ssekms_key_id(), Some(managed_key.as_str()));

    let head = s3
        .head_object()
        .bucket("kms-no-key")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    assert_eq!(head.ssekms_key_id(), Some(managed_key.as_str()));

    let got = s3
        .get_object()
        .bucket("kms-no-key")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    let body = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"under the managed key");
}

// ---- CopyObject and UpdateObjectEncryption under SSE-KMS ----

async fn body_of(s3: &aws_sdk_s3::Client, bucket: &str, key: &str) -> Vec<u8> {
    s3.get_object()
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

/// A copy that names no encryption takes the destination bucket's default
/// (S3 API reference, CopyObject): sealed under the default's KMS key with
/// its S3 Bucket Key setting, reported on the copy and HeadObject, and
/// readable. A default key whose policy refuses the use fails the copy.
#[tokio::test]
async fn s3_copy_without_sse_takes_the_destination_default_kms_key() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let key = create_kms_key(&server).await;
    for bucket in ["copy-src", "copy-dst", "copy-denied"] {
        s3.create_bucket().bucket(bucket).send().await.unwrap();
    }
    set_default_encryption(
        &s3,
        "copy-dst",
        ServerSideEncryption::AwsKms,
        Some(&key),
        true,
    )
    .await;
    s3.put_object()
        .bucket("copy-src")
        .key("a.txt")
        .body(ByteStream::from_static(b"copied plaintext"))
        .send()
        .await
        .unwrap();

    let copied = s3
        .copy_object()
        .bucket("copy-dst")
        .key("b.txt")
        .copy_source("copy-src/a.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        copied.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    assert_eq!(copied.ssekms_key_id(), Some(key.as_str()));
    assert_eq!(copied.bucket_key_enabled(), Some(true));
    let head = s3
        .head_object()
        .bucket("copy-dst")
        .key("b.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.server_side_encryption(),
        Some(&ServerSideEncryption::AwsKms)
    );
    assert_eq!(head.ssekms_key_id(), Some(key.as_str()));
    assert_eq!(head.bucket_key_enabled(), Some(true));
    assert_eq!(body_of(&s3, "copy-dst", "b.txt").await, b"copied plaintext");

    // A default key whose policy denies its use: the copy fails.
    let refusing = server
        .kms_client()
        .await
        .create_key()
        .policy(
            r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"kms:*","Resource":"*"},
                {"Effect":"Deny","Principal":"*","Action":"kms:GenerateDataKey","Resource":"*"}]}"#,
        )
        .send()
        .await
        .unwrap()
        .key_metadata()
        .unwrap()
        .arn()
        .unwrap()
        .to_string();
    set_default_encryption(
        &s3,
        "copy-denied",
        ServerSideEncryption::AwsKms,
        Some(&refusing),
        false,
    )
    .await;
    let err = s3
        .copy_object()
        .bucket("copy-denied")
        .key("b.txt")
        .copy_source("copy-src/a.txt")
        .send()
        .await
        .expect_err("the default key refuses GenerateDataKey");
    assert_eq!(
        aws_sdk_s3::error::ProvideErrorMetadata::code(&err),
        Some("AccessDenied"),
        "{err:?}"
    );
}

/// UpdateObjectEncryption to another KMS key reseals the object under it:
/// once the old key is disabled, the object still reads back.
#[tokio::test]
async fn s3_update_object_encryption_moves_the_object_to_the_new_key() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let kms = server.kms_client().await;
    let old_key = create_kms_key(&server).await;
    let new_key = create_kms_key(&server).await;
    s3.create_bucket().bucket("rekey").send().await.unwrap();
    s3.put_object()
        .bucket("rekey")
        .key("doc.txt")
        .body(ByteStream::from_static(b"rekeyed plaintext"))
        .server_side_encryption(ServerSideEncryption::AwsKms)
        .ssekms_key_id(&old_key)
        .send()
        .await
        .unwrap();

    s3.update_object_encryption()
        .bucket("rekey")
        .key("doc.txt")
        .object_encryption(aws_sdk_s3::types::ObjectEncryption::Ssekms(
            aws_sdk_s3::types::SsekmsEncryption::builder()
                .kms_key_arn(&new_key)
                .build()
                .unwrap(),
        ))
        .send()
        .await
        .unwrap();
    let head = s3
        .head_object()
        .bucket("rekey")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(head.ssekms_key_id(), Some(new_key.as_str()));

    kms.disable_key().key_id(&old_key).send().await.unwrap();
    assert_eq!(body_of(&s3, "rekey", "doc.txt").await, b"rekeyed plaintext");
    // And the new key is the one it needs.
    kms.disable_key().key_id(&new_key).send().await.unwrap();
    s3.get_object()
        .bucket("rekey")
        .key("doc.txt")
        .send()
        .await
        .expect_err("the object is sealed under the new key");
}

/// On a versioned bucket UpdateObjectEncryption reseals the version it
/// names: an older version moved to the new key reads back by its version
/// id once the old key is disabled, while the current version, left under
/// the old key, no longer does.
#[tokio::test]
async fn s3_update_object_encryption_reseals_the_named_version() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let kms = server.kms_client().await;
    let old_key = create_kms_key(&server).await;
    let new_key = create_kms_key(&server).await;
    s3.create_bucket().bucket("rekey-ver").send().await.unwrap();
    s3.put_bucket_versioning()
        .bucket("rekey-ver")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let mut versions = Vec::new();
    for body in [&b"first version"[..], b"second version"] {
        let put = s3
            .put_object()
            .bucket("rekey-ver")
            .key("doc.txt")
            .body(ByteStream::from(body.to_vec()))
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(&old_key)
            .send()
            .await
            .unwrap();
        versions.push(put.version_id().unwrap().to_string());
    }

    s3.update_object_encryption()
        .bucket("rekey-ver")
        .key("doc.txt")
        .version_id(&versions[0])
        .object_encryption(aws_sdk_s3::types::ObjectEncryption::Ssekms(
            aws_sdk_s3::types::SsekmsEncryption::builder()
                .kms_key_arn(&new_key)
                .build()
                .unwrap(),
        ))
        .send()
        .await
        .unwrap();
    let head = |version: &str| {
        s3.head_object()
            .bucket("rekey-ver")
            .key("doc.txt")
            .version_id(version)
            .send()
    };
    assert_eq!(
        head(&versions[0]).await.unwrap().ssekms_key_id(),
        Some(new_key.as_str())
    );
    assert_eq!(
        head(&versions[1]).await.unwrap().ssekms_key_id(),
        Some(old_key.as_str())
    );

    kms.disable_key().key_id(&old_key).send().await.unwrap();
    let old_version = s3
        .get_object()
        .bucket("rekey-ver")
        .key("doc.txt")
        .version_id(&versions[0])
        .send()
        .await
        .expect("the resealed version reads under the new key")
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(&old_version[..], b"first version");
    s3.get_object()
        .bucket("rekey-ver")
        .key("doc.txt")
        .send()
        .await
        .expect_err("the current version is still under the disabled key");
}
