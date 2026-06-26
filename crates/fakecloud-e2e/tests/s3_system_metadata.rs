//! S3 stores Cache-Control / Content-Disposition / Content-Language / Expires
//! system metadata on PutObject and echoes them on GET/HEAD, honors the
//! `response-*` override query params, and survives a restart.

mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

#[tokio::test]
async fn system_metadata_round_trips_and_overrides() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("meta").send().await.unwrap();
    s3.put_object()
        .bucket("meta")
        .key("doc")
        .body(ByteStream::from_static(b"hi"))
        .cache_control("max-age=42")
        .content_disposition("attachment; filename=\"r.txt\"")
        .content_language("en-US")
        .content_type("text/plain")
        .send()
        .await
        .unwrap();

    let got = s3
        .get_object()
        .bucket("meta")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert_eq!(got.cache_control(), Some("max-age=42"));
    assert_eq!(
        got.content_disposition(),
        Some("attachment; filename=\"r.txt\"")
    );
    assert_eq!(got.content_language(), Some("en-US"));

    let head = s3
        .head_object()
        .bucket("meta")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert_eq!(head.cache_control(), Some("max-age=42"));
    assert_eq!(
        head.content_disposition(),
        Some("attachment; filename=\"r.txt\"")
    );

    // response-* overrides win over the stored values.
    let overridden = s3
        .get_object()
        .bucket("meta")
        .key("doc")
        .response_cache_control("no-cache")
        .response_content_disposition("inline")
        .response_content_type("application/json")
        .send()
        .await
        .unwrap();
    assert_eq!(overridden.cache_control(), Some("no-cache"));
    assert_eq!(overridden.content_disposition(), Some("inline"));
    assert_eq!(overridden.content_type(), Some("application/json"));
}

#[tokio::test]
async fn system_metadata_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let s3 = server.s3_client().await;

    s3.create_bucket().bucket("meta").send().await.unwrap();
    s3.put_object()
        .bucket("meta")
        .key("doc")
        .body(ByteStream::from_static(b"hi"))
        .cache_control("max-age=99")
        .content_language("fr")
        .send()
        .await
        .unwrap();

    server.restart().await;
    let s3 = server.s3_client().await;
    let got = s3
        .get_object()
        .bucket("meta")
        .key("doc")
        .send()
        .await
        .unwrap();
    assert_eq!(got.cache_control(), Some("max-age=99"));
    assert_eq!(got.content_language(), Some("fr"));
}
