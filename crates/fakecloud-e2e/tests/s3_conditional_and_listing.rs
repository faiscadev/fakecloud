//! RFC 7232 conditional-header precedence (If-Modified-Since ignored when
//! If-None-Match is present) and ListObjectVersions encoding-type=url.

mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

#[tokio::test]
async fn if_modified_since_ignored_when_if_none_match_present() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("cond").send().await.unwrap();
    let put = s3
        .put_object()
        .bucket("cond")
        .key("k")
        .body(ByteStream::from_static(b"v"))
        .send()
        .await
        .unwrap();
    let etag = put.e_tag().unwrap().to_string();

    // Non-matching If-None-Match + a future If-Modified-Since. RFC 7232 §3.3:
    // If-Modified-Since must be ignored -> 200, not 304.
    let resp = s3
        .get_object()
        .bucket("cond")
        .key("k")
        .if_none_match("\"does-not-match\"")
        .if_modified_since(aws_sdk_s3::primitives::DateTime::from_secs(4102444800)) // year 2100
        .send()
        .await;
    assert!(resp.is_ok(), "should return 200, not 304: {resp:?}");

    // Matching If-None-Match still 304 regardless of If-Modified-Since.
    let not_mod = s3
        .get_object()
        .bucket("cond")
        .key("k")
        .if_none_match(&etag)
        .send()
        .await;
    assert!(not_mod.is_err(), "matching If-None-Match must be 304");
}

#[tokio::test]
async fn list_object_versions_honors_encoding_type_url() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("vers").send().await.unwrap();
    // A key with characters that url-encoding changes.
    s3.put_object()
        .bucket("vers")
        .key("a b/c+d")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();

    let listed = s3
        .list_object_versions()
        .bucket("vers")
        .encoding_type(aws_sdk_s3::types::EncodingType::Url)
        .send()
        .await
        .unwrap();

    assert_eq!(
        listed.encoding_type(),
        Some(&aws_sdk_s3::types::EncodingType::Url)
    );
    // The key is url-encoded in the response (space -> %20, + -> %2B); the
    // Rust SDK surfaces it verbatim. Previously the param was ignored and the
    // key came back raw / XML-escaped.
    let v = listed.versions();
    assert!(
        v.iter().any(|ver| ver.key() == Some("a%20b/c%2Bd")),
        "key should be url-encoded: {v:?}"
    );
}
