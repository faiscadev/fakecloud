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

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

/// Bug-hunt 5.4: a plain (non-chunked) SigV4 PutObject carries the payload's
/// real SHA-256 in `x-amz-content-sha256`. fakecloud spooled the body but never
/// compared this header against the bytes received, so a corrupt/tampered
/// upload whose header disagreed with its body stored silently. AWS rejects the
/// divergence with `XAmzContentSHA256Mismatch` (400). This is enforced at the
/// S3 layer (the buffered payload is authoritative) rather than in sigv4
/// verification, where aws-chunked/streaming empties the body buffer.
#[tokio::test]
async fn s3_put_object_rejects_content_sha256_mismatch() {
    let s = TestServer::start().await;
    let s3 = s.s3_client().await;
    s3.create_bucket()
        .bucket("sha-bucket")
        .send()
        .await
        .unwrap();

    let body = b"hello world".to_vec();
    let bogus = "0".repeat(64); // valid hex, wrong for the body

    // Raw PUT with a SigV4 Authorization header (routes through the streaming
    // spool path; signature is not verified — auth is off by default) and a
    // mismatched x-amz-content-sha256 hex digest.
    let url = format!("{}/sha-bucket/obj", s.endpoint());
    let resp = reqwest::Client::new()
        .put(&url)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256, Signature=0000000000000000000000000000000000000000000000000000000000000000",
        )
        .header("x-amz-content-sha256", &bogus)
        .header("content-length", body.len().to_string())
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "a mismatched x-amz-content-sha256 must be rejected with 400"
    );
    let text = resp.text().await.unwrap();
    assert!(
        text.contains("XAmzContentSHA256Mismatch"),
        "expected XAmzContentSHA256Mismatch, got: {text}"
    );

    // The correct digest for the same body round-trips fine.
    let good = sha256_hex(&body);
    let ok = reqwest::Client::new()
        .put(&url)
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256, Signature=0000000000000000000000000000000000000000000000000000000000000000",
        )
        .header("x-amz-content-sha256", &good)
        .header("content-length", body.len().to_string())
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(
        ok.status().is_success(),
        "a correct x-amz-content-sha256 must be accepted, got {}",
        ok.status()
    );

    // UNSIGNED-PAYLOAD must skip the check entirely (marker, not a body hash).
    let unsigned = reqwest::Client::new()
        .put(format!("{}/sha-bucket/obj2", s.endpoint()))
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256, Signature=0000000000000000000000000000000000000000000000000000000000000000",
        )
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
        .body(b"anything".to_vec())
        .send()
        .await
        .unwrap();
    assert!(
        unsigned.status().is_success(),
        "UNSIGNED-PAYLOAD must bypass the content-sha256 check, got {}",
        unsigned.status()
    );
}
