//! S3 CopyObject must honor `x-amz-copy-source-if-match`: a mismatched source
//! ETag returns 412 PreconditionFailed (previously the header was never read,
//! so the copy succeeded with a 200).

mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

#[tokio::test]
async fn copy_object_enforces_copy_source_if_match() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    s3.create_bucket()
        .bucket("cp-bucket")
        .send()
        .await
        .expect("create_bucket");

    let put = s3
        .put_object()
        .bucket("cp-bucket")
        .key("src.txt")
        .body(ByteStream::from_static(b"hello copy source"))
        .send()
        .await
        .expect("put_object");
    let src_etag = put.e_tag().expect("etag").to_string();

    // Mismatched copy-source-if-match -> 412 PreconditionFailed, no copy.
    let err = s3
        .copy_object()
        .bucket("cp-bucket")
        .key("dst-bad.txt")
        .copy_source("cp-bucket/src.txt")
        .copy_source_if_match("\"deadbeefdeadbeefdeadbeefdeadbeef\"")
        .send()
        .await
        .expect_err("mismatched copy-source-if-match must fail");
    let raw = err.into_service_error();
    let msg = format!("{raw:?}");
    assert!(
        msg.contains("PreconditionFailed") || msg.contains("412"),
        "expected PreconditionFailed, got: {msg}"
    );

    // The bad copy must not have created the destination object.
    let head_bad = s3
        .head_object()
        .bucket("cp-bucket")
        .key("dst-bad.txt")
        .send()
        .await;
    assert!(head_bad.is_err(), "412 copy must not create the object");

    // Matching copy-source-if-match -> 200, copy succeeds.
    s3.copy_object()
        .bucket("cp-bucket")
        .key("dst-ok.txt")
        .copy_source("cp-bucket/src.txt")
        .copy_source_if_match(&src_etag)
        .send()
        .await
        .expect("matching copy-source-if-match must succeed");

    let got = s3
        .get_object()
        .bucket("cp-bucket")
        .key("dst-ok.txt")
        .send()
        .await
        .expect("get copied object");
    let body = got.body.collect().await.expect("body").into_bytes();
    assert_eq!(&body[..], b"hello copy source");
}
