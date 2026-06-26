//! Modern AWS S3 clients (aws-cli, boto3 >= 1.36, aws-crt) send PutObject /
//! UploadPart bodies wrapped in `Content-Encoding: aws-chunked` application-layer
//! framing (`<hexsize>;chunk-signature=...\r\n<data>\r\n...0\r\n<trailers>\r\n`).
//! hyper strips only HTTP `Transfer-Encoding: chunked`, not this framing, so
//! without decoding the chunk-size/signature lines + trailers were stored as the
//! object's bytes (corrupt object, wrong ETag). This drives a raw aws-chunked PUT
//! and asserts the stored object is the decoded payload, with the right ETag.

mod helpers;

use helpers::TestServer;

/// Build a signed aws-chunked body for `payload` with `chunk_size` data chunks.
fn aws_chunked_body(payload: &[u8], chunk_size: usize) -> Vec<u8> {
    let sig = "0".repeat(64);
    let mut out = Vec::new();
    for c in payload.chunks(chunk_size) {
        out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", c.len()).as_bytes());
        out.extend_from_slice(c);
        out.extend_from_slice(b"\r\n");
    }
    out.extend_from_slice(format!("0;chunk-signature={sig}\r\n").as_bytes());
    out.extend_from_slice(b"x-amz-checksum-crc32:AAAAAA==\r\n\r\n");
    out
}

fn md5_hex(bytes: &[u8]) -> String {
    use md5::{Digest, Md5};
    let mut h = Md5::new();
    h.update(bytes);
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

#[tokio::test]
async fn put_object_decodes_aws_chunked_framing() {
    let s = TestServer::start().await;
    let s3 = s.s3_client().await;
    s3.create_bucket().bucket("chunked").send().await.unwrap();

    // A payload large enough to span several aws-chunked frames.
    let payload: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    let body = aws_chunked_body(&payload, 8192);

    // Raw PUT exactly as a default modern client frames it (auth is off by
    // default, so a real streaming signature is unnecessary to reach the path).
    let url = format!("{}/chunked/obj", s.endpoint());
    let resp = reqwest::Client::new()
        .put(&url)
        // An S3 SigV4 Authorization header routes the body through the streaming
        // spool path (signature is not verified — auth is off by default).
        .header(
            "authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256, Signature=0000000000000000000000000000000000000000000000000000000000000000",
        )
        .header("content-encoding", "aws-chunked")
        .header("x-amz-content-sha256", "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
        .header("x-amz-decoded-content-length", payload.len().to_string())
        .header("content-length", body.len().to_string())
        .body(body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "raw aws-chunked PUT failed: {}",
        resp.status()
    );

    // GetObject via the SDK must return the DECODED payload, byte-exact.
    let got = s3
        .get_object()
        .bucket("chunked")
        .key("obj")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.content_length(),
        Some(payload.len() as i64),
        "stored size must be the decoded length, not the framed length"
    );
    // The stored aws-chunked transfer encoding must not leak into metadata.
    assert!(
        got.content_encoding().is_none(),
        "aws-chunked must be stripped from stored Content-Encoding, got {:?}",
        got.content_encoding()
    );
    // ETag is the MD5 of the decoded payload (not the framed bytes).
    let etag = got.e_tag().unwrap().trim_matches('"').to_string();
    assert_eq!(
        etag,
        md5_hex(&payload),
        "ETag must hash the decoded payload"
    );

    let bytes = got.body.collect().await.unwrap().to_vec();
    assert_eq!(bytes, payload, "object bytes must be the decoded payload");
}
