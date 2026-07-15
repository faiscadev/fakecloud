mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumAlgorithm;
use helpers::TestServer;

/// Bug-hunt 1.13: PutObject computed the modern `x-amz-checksum-*` value over
/// the received body but never compared it against a client-supplied value, so
/// a corrupt upload with a wrong precomputed checksum stored silently. AWS
/// rejects the mismatch with `BadDigest`. Supplying an explicit (wrong)
/// SHA256 must fail; a correct request still succeeds.
#[tokio::test]
async fn s3_put_object_rejects_checksum_mismatch() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    s3.create_bucket()
        .bucket("checksum-bucket")
        .send()
        .await
        .unwrap();

    // A syntactically-valid base64 SHA256 that does NOT match the body.
    let wrong_sha256 = "3q2+7wAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
    let result = s3
        .put_object()
        .bucket("checksum-bucket")
        .key("obj")
        .body(ByteStream::from_static(b"hello world"))
        .checksum_sha256(wrong_sha256)
        .send()
        .await;
    assert!(
        result.is_err(),
        "a mismatched x-amz-checksum-sha256 must be rejected"
    );

    // Letting the SDK compute the checksum for us round-trips fine.
    s3.put_object()
        .bucket("checksum-bucket")
        .key("obj-ok")
        .body(ByteStream::from_static(b"hello world"))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .expect("a correct checksum must be accepted");
}
