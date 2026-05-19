mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// -- Bucket lifecycle --

#[test_action("s3", "CreateBucket", checksum = "56299072")]
#[test_action("s3", "ListBuckets", checksum = "80b1347a")]
#[test_action("s3", "HeadBucket", checksum = "803cc873")]
#[test_action("s3", "GetBucketLocation", checksum = "f7a8bca0")]
#[test_action("s3", "DeleteBucket", checksum = "05abd839")]
#[tokio::test]
async fn s3_bucket_lifecycle() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-bucket")
        .send()
        .await
        .unwrap();

    let list = client.list_buckets().send().await.unwrap();
    assert!(!list.buckets().is_empty());

    client
        .head_bucket()
        .bucket("conf-bucket")
        .send()
        .await
        .unwrap();

    client
        .get_bucket_location()
        .bucket("conf-bucket")
        .send()
        .await
        .unwrap();

    client
        .delete_bucket()
        .bucket("conf-bucket")
        .send()
        .await
        .unwrap();
}

// -- Object CRUD --

#[test_action("s3", "PutObject", checksum = "8ecba706")]
#[test_action("s3", "GetObject", checksum = "7430c0ca")]
#[test_action("s3", "HeadObject", checksum = "0c13fed3")]
#[test_action("s3", "DeleteObject", checksum = "b50d71d4")]
#[tokio::test]
async fn s3_object_crud() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-obj")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket("conf-obj")
        .key("test.txt")
        .body(ByteStream::from_static(b"hello conformance"))
        .send()
        .await
        .unwrap();

    let get = client
        .get_object()
        .bucket("conf-obj")
        .key("test.txt")
        .send()
        .await
        .unwrap();
    let body = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(&body[..], b"hello conformance");

    client
        .head_object()
        .bucket("conf-obj")
        .key("test.txt")
        .send()
        .await
        .unwrap();

    client
        .delete_object()
        .bucket("conf-obj")
        .key("test.txt")
        .send()
        .await
        .unwrap();
}

// -- CopyObject --

#[test_action("s3", "CopyObject", checksum = "66cd7130")]
#[tokio::test]
async fn s3_copy_object() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-copy")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("conf-copy")
        .key("src.txt")
        .body(ByteStream::from_static(b"source"))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket("conf-copy")
        .key("dst.txt")
        .copy_source("conf-copy/src.txt")
        .send()
        .await
        .unwrap();
}

// -- DeleteObjects (batch) --

#[test_action("s3", "DeleteObjects", checksum = "0cd53c56")]
#[tokio::test]
async fn s3_delete_objects() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-delobj")
        .send()
        .await
        .unwrap();
    for key in ["a.txt", "b.txt"] {
        client
            .put_object()
            .bucket("conf-delobj")
            .key(key)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .delete_objects()
        .bucket("conf-delobj")
        .delete(
            aws_sdk_s3::types::Delete::builder()
                .objects(
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key("a.txt")
                        .build()
                        .unwrap(),
                )
                .objects(
                    aws_sdk_s3::types::ObjectIdentifier::builder()
                        .key("b.txt")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().len(), 2);
}

// -- ListObjectsV2 + ListObjects --

#[test_action("s3", "ListObjectsV2", checksum = "b31ed33c")]
#[test_action("s3", "ListObjects", checksum = "99d4301b")]
#[tokio::test]
async fn s3_list_objects() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-list")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("conf-list")
        .key("item.txt")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();

    let v2 = client
        .list_objects_v2()
        .bucket("conf-list")
        .send()
        .await
        .unwrap();
    assert!(!v2.contents().is_empty());

    let v1 = client
        .list_objects()
        .bucket("conf-list")
        .send()
        .await
        .unwrap();
    assert!(!v1.contents().is_empty());
}

// -- ListObjectVersions --

#[test_action("s3", "ListObjectVersions", checksum = "026c9a1c")]
#[tokio::test]
async fn s3_list_object_versions() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-versions")
        .send()
        .await
        .unwrap();
    client
        .list_object_versions()
        .bucket("conf-versions")
        .send()
        .await
        .unwrap();
}

// -- GetObjectAttributes --

#[test_action("s3", "GetObjectAttributes", checksum = "0adf2c26")]
#[tokio::test]
async fn s3_get_object_attributes() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-attrs")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("conf-attrs")
        .key("a.txt")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object_attributes()
        .bucket("conf-attrs")
        .key("a.txt")
        .object_attributes(aws_sdk_s3::types::ObjectAttributes::ObjectSize)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.object_size().unwrap(),
        4,
        "ObjectSize should match 'data' length"
    );
}

// -- RestoreObject --

#[test_action("s3", "RestoreObject", checksum = "3c027442")]
#[tokio::test]
async fn s3_restore_object() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-restore")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("conf-restore")
        .key("archive.txt")
        .body(ByteStream::from_static(b"archived"))
        .send()
        .await
        .unwrap();

    // RestoreObject returns InvalidObjectState for objects not in an archival
    // storage class — verify we get the expected error rather than a crash.
    let result = client
        .restore_object()
        .bucket("conf-restore")
        .key("archive.txt")
        .restore_request(aws_sdk_s3::types::RestoreRequest::builder().days(1).build())
        .send()
        .await;
    assert!(
        result.is_err(),
        "RestoreObject on STANDARD object should return error"
    );
}

// -- Object tagging --

#[test_action("s3", "PutObjectTagging", checksum = "dd9548bb")]
#[test_action("s3", "GetObjectTagging", checksum = "e0ede0a2")]
#[test_action("s3", "DeleteObjectTagging", checksum = "bdabe7c6")]
#[tokio::test]
async fn s3_object_tagging() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-otag")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("conf-otag")
        .key("tagged.txt")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();

    client
        .put_object_tagging()
        .bucket("conf-otag")
        .key("tagged.txt")
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("env")
                        .value("test")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object_tagging()
        .bucket("conf-otag")
        .key("tagged.txt")
        .send()
        .await
        .unwrap();
    assert!(!resp.tag_set().is_empty());

    client
        .delete_object_tagging()
        .bucket("conf-otag")
        .key("tagged.txt")
        .send()
        .await
        .unwrap();
}

// -- Object ACL --

#[test_action("s3", "PutObjectAcl", checksum = "d76f6ff6")]
#[test_action("s3", "GetObjectAcl", checksum = "aa4c2112")]
#[tokio::test]
async fn s3_object_acl() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-oacl")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("conf-oacl")
        .key("acl.txt")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();

    client
        .put_object_acl()
        .bucket("conf-oacl")
        .key("acl.txt")
        .acl(aws_sdk_s3::types::ObjectCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    client
        .get_object_acl()
        .bucket("conf-oacl")
        .key("acl.txt")
        .send()
        .await
        .unwrap();
}

// -- Object retention + legal hold --

#[test_action("s3", "PutObjectRetention", checksum = "16e8bfef")]
#[test_action("s3", "GetObjectRetention", checksum = "cd7095c1")]
#[tokio::test]
async fn s3_object_retention() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-oret")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("conf-oret")
        .key("ret.txt")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();

    client
        .put_object_retention()
        .bucket("conf-oret")
        .key("ret.txt")
        .retention(
            aws_sdk_s3::types::ObjectLockRetention::builder()
                .mode(aws_sdk_s3::types::ObjectLockRetentionMode::Governance)
                .retain_until_date(aws_sdk_s3::primitives::DateTime::from_secs(4102444800))
                .build(),
        )
        .send()
        .await
        .ok();

    client
        .get_object_retention()
        .bucket("conf-oret")
        .key("ret.txt")
        .send()
        .await
        .ok();
}

#[test_action("s3", "PutObjectLegalHold", checksum = "508ddfd0")]
#[test_action("s3", "GetObjectLegalHold", checksum = "2d6979f4")]
#[tokio::test]
async fn s3_object_legal_hold() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-olh")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("conf-olh")
        .key("hold.txt")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();

    client
        .put_object_legal_hold()
        .bucket("conf-olh")
        .key("hold.txt")
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await
        .ok();

    client
        .get_object_legal_hold()
        .bucket("conf-olh")
        .key("hold.txt")
        .send()
        .await
        .ok();
}

// -- Bucket tagging --

#[test_action("s3", "PutBucketTagging", checksum = "f54b26ed")]
#[test_action("s3", "GetBucketTagging", checksum = "2257d3d6")]
#[test_action("s3", "DeleteBucketTagging", checksum = "e3fe5dcd")]
#[tokio::test]
async fn s3_bucket_tagging() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-btag")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_tagging()
        .bucket("conf-btag")
        .tagging(
            aws_sdk_s3::types::Tagging::builder()
                .tag_set(
                    aws_sdk_s3::types::Tag::builder()
                        .key("env")
                        .value("test")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_tagging()
        .bucket("conf-btag")
        .send()
        .await
        .unwrap();
    assert!(!resp.tag_set().is_empty());

    client
        .delete_bucket_tagging()
        .bucket("conf-btag")
        .send()
        .await
        .unwrap();
}

// -- Bucket ACL --

#[test_action("s3", "PutBucketAcl", checksum = "c3732dfd")]
#[test_action("s3", "GetBucketAcl", checksum = "54d254cd")]
#[tokio::test]
async fn s3_bucket_acl() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-bacl")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_acl()
        .bucket("conf-bacl")
        .acl(aws_sdk_s3::types::BucketCannedAcl::PublicRead)
        .send()
        .await
        .unwrap();

    client
        .get_bucket_acl()
        .bucket("conf-bacl")
        .send()
        .await
        .unwrap();
}

// -- Bucket versioning --

#[test_action("s3", "PutBucketVersioning", checksum = "ebf678c4")]
#[test_action("s3", "GetBucketVersioning", checksum = "2a2834a0")]
#[tokio::test]
async fn s3_bucket_versioning() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-bver")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_versioning()
        .bucket("conf-bver")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_versioning()
        .bucket("conf-bver")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled)
    );
}

// -- Bucket CORS --

#[test_action("s3", "PutBucketCors", checksum = "65016ca1")]
#[test_action("s3", "GetBucketCors", checksum = "ca2bd57e")]
#[test_action("s3", "DeleteBucketCors", checksum = "81962aad")]
#[tokio::test]
async fn s3_bucket_cors() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-cors")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_cors()
        .bucket("conf-cors")
        .cors_configuration(
            aws_sdk_s3::types::CorsConfiguration::builder()
                .cors_rules(
                    aws_sdk_s3::types::CorsRule::builder()
                        .allowed_methods("GET")
                        .allowed_origins("*")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_bucket_cors()
        .bucket("conf-cors")
        .send()
        .await
        .unwrap();

    client
        .delete_bucket_cors()
        .bucket("conf-cors")
        .send()
        .await
        .unwrap();
}

// -- Bucket notification configuration --

#[test_action("s3", "PutBucketNotificationConfiguration", checksum = "6defd1ec")]
#[test_action("s3", "GetBucketNotificationConfiguration", checksum = "c6a077b9")]
#[tokio::test]
async fn s3_bucket_notification() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-notif")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_notification_configuration()
        .bucket("conf-notif")
        .notification_configuration(aws_sdk_s3::types::NotificationConfiguration::builder().build())
        .send()
        .await
        .unwrap();

    client
        .get_bucket_notification_configuration()
        .bucket("conf-notif")
        .send()
        .await
        .unwrap();
}

// -- Bucket website --

#[test_action("s3", "PutBucketWebsite", checksum = "330597aa")]
#[test_action("s3", "GetBucketWebsite", checksum = "02acea7a")]
#[test_action("s3", "DeleteBucketWebsite", checksum = "892d5b36")]
#[tokio::test]
async fn s3_bucket_website() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-web")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_website()
        .bucket("conf-web")
        .website_configuration(
            aws_sdk_s3::types::WebsiteConfiguration::builder()
                .index_document(
                    aws_sdk_s3::types::IndexDocument::builder()
                        .suffix("index.html")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_bucket_website()
        .bucket("conf-web")
        .send()
        .await
        .unwrap();

    client
        .delete_bucket_website()
        .bucket("conf-web")
        .send()
        .await
        .unwrap();
}

// -- Bucket accelerate --

#[test_action("s3", "PutBucketAccelerateConfiguration", checksum = "e71d606a")]
#[test_action("s3", "GetBucketAccelerateConfiguration", checksum = "33fcae8e")]
#[tokio::test]
async fn s3_bucket_accelerate() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-accel")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_accelerate_configuration()
        .bucket("conf-accel")
        .accelerate_configuration(
            aws_sdk_s3::types::AccelerateConfiguration::builder()
                .status(aws_sdk_s3::types::BucketAccelerateStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_bucket_accelerate_configuration()
        .bucket("conf-accel")
        .send()
        .await
        .unwrap();
}

// -- Public access block --

#[test_action("s3", "PutPublicAccessBlock", checksum = "1c1f6372")]
#[test_action("s3", "GetPublicAccessBlock", checksum = "66149497")]
#[test_action("s3", "DeletePublicAccessBlock", checksum = "5fd2aac6")]
#[tokio::test]
async fn s3_public_access_block() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-pab")
        .send()
        .await
        .unwrap();

    client
        .put_public_access_block()
        .bucket("conf-pab")
        .public_access_block_configuration(
            aws_sdk_s3::types::PublicAccessBlockConfiguration::builder()
                .block_public_acls(true)
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_public_access_block()
        .bucket("conf-pab")
        .send()
        .await
        .unwrap();

    client
        .delete_public_access_block()
        .bucket("conf-pab")
        .send()
        .await
        .unwrap();
}

// -- Bucket encryption --

#[test_action("s3", "PutBucketEncryption", checksum = "c030aecc")]
#[test_action("s3", "GetBucketEncryption", checksum = "d7326b12")]
#[test_action("s3", "DeleteBucketEncryption", checksum = "897fff80")]
#[tokio::test]
async fn s3_bucket_encryption() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-enc")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_encryption()
        .bucket("conf-enc")
        .server_side_encryption_configuration(
            aws_sdk_s3::types::ServerSideEncryptionConfiguration::builder()
                .rules(
                    aws_sdk_s3::types::ServerSideEncryptionRule::builder()
                        .apply_server_side_encryption_by_default(
                            aws_sdk_s3::types::ServerSideEncryptionByDefault::builder()
                                .sse_algorithm(aws_sdk_s3::types::ServerSideEncryption::Aes256)
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
        .unwrap();

    client
        .get_bucket_encryption()
        .bucket("conf-enc")
        .send()
        .await
        .unwrap();

    client
        .delete_bucket_encryption()
        .bucket("conf-enc")
        .send()
        .await
        .unwrap();
}

// -- Bucket lifecycle --

#[test_action("s3", "PutBucketLifecycleConfiguration", checksum = "21fcd15c")]
#[test_action("s3", "GetBucketLifecycleConfiguration", checksum = "73010677")]
#[test_action("s3", "DeleteBucketLifecycle", checksum = "335ed098")]
#[tokio::test]
async fn s3_bucket_lifecycle_config() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-lc")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_lifecycle_configuration()
        .bucket("conf-lc")
        .lifecycle_configuration(
            aws_sdk_s3::types::BucketLifecycleConfiguration::builder()
                .rules(
                    aws_sdk_s3::types::LifecycleRule::builder()
                        .id("expire")
                        .status(aws_sdk_s3::types::ExpirationStatus::Enabled)
                        .expiration(
                            aws_sdk_s3::types::LifecycleExpiration::builder()
                                .days(30)
                                .build(),
                        )
                        .filter(
                            aws_sdk_s3::types::LifecycleRuleFilter::builder()
                                .prefix("logs/")
                                .build(),
                        )
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_bucket_lifecycle_configuration()
        .bucket("conf-lc")
        .send()
        .await
        .unwrap();

    client
        .delete_bucket_lifecycle()
        .bucket("conf-lc")
        .send()
        .await
        .unwrap();
}

// -- Bucket logging --

#[test_action("s3", "PutBucketLogging", checksum = "ed386eb1")]
#[test_action("s3", "GetBucketLogging", checksum = "a7325831")]
#[tokio::test]
async fn s3_bucket_logging() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-log")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_logging()
        .bucket("conf-log")
        .bucket_logging_status(aws_sdk_s3::types::BucketLoggingStatus::builder().build())
        .send()
        .await
        .unwrap();

    client
        .get_bucket_logging()
        .bucket("conf-log")
        .send()
        .await
        .unwrap();
}

// -- Bucket policy --

#[test_action("s3", "PutBucketPolicy", checksum = "1b9c9417")]
#[test_action("s3", "GetBucketPolicy", checksum = "d34ae983")]
#[test_action("s3", "DeleteBucketPolicy", checksum = "90cdf847")]
#[tokio::test]
async fn s3_bucket_policy() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-bpol")
        .send()
        .await
        .unwrap();

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::conf-bpol/*"}]}"#;
    client
        .put_bucket_policy()
        .bucket("conf-bpol")
        .policy(policy)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_policy()
        .bucket("conf-bpol")
        .send()
        .await
        .unwrap();
    assert!(resp.policy().is_some());

    client
        .delete_bucket_policy()
        .bucket("conf-bpol")
        .send()
        .await
        .unwrap();
}

// -- Object lock configuration --

#[test_action("s3", "PutObjectLockConfiguration", checksum = "36a5a1a7")]
#[test_action("s3", "GetObjectLockConfiguration", checksum = "8cbd3dcf")]
#[tokio::test]
async fn s3_object_lock_configuration() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-olock")
        .send()
        .await
        .unwrap();

    client
        .put_object_lock_configuration()
        .bucket("conf-olock")
        .object_lock_configuration(
            aws_sdk_s3::types::ObjectLockConfiguration::builder()
                .object_lock_enabled(aws_sdk_s3::types::ObjectLockEnabled::Enabled)
                .build(),
        )
        .send()
        .await
        .ok();

    client
        .get_object_lock_configuration()
        .bucket("conf-olock")
        .send()
        .await
        .ok();
}

// -- Bucket replication --

#[test_action("s3", "PutBucketReplication", checksum = "737a9131")]
#[test_action("s3", "GetBucketReplication", checksum = "5aa6062b")]
#[test_action("s3", "DeleteBucketReplication", checksum = "6b0e2b2e")]
#[tokio::test]
async fn s3_bucket_replication() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-repl")
        .send()
        .await
        .unwrap();

    // Enable versioning first (required for replication)
    client
        .put_bucket_versioning()
        .bucket("conf-repl")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .put_bucket_replication()
        .bucket("conf-repl")
        .replication_configuration(
            aws_sdk_s3::types::ReplicationConfiguration::builder()
                .role("arn:aws:iam::123456789012:role/repl-role")
                .rules(
                    aws_sdk_s3::types::ReplicationRule::builder()
                        .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
                        .destination(
                            aws_sdk_s3::types::Destination::builder()
                                .bucket("arn:aws:s3:::conf-repl-dest")
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .ok();

    client
        .get_bucket_replication()
        .bucket("conf-repl")
        .send()
        .await
        .ok();

    client
        .delete_bucket_replication()
        .bucket("conf-repl")
        .send()
        .await
        .ok();
}

// -- Bucket ownership controls --

#[test_action("s3", "PutBucketOwnershipControls", checksum = "c72a3773")]
#[test_action("s3", "GetBucketOwnershipControls", checksum = "5d7346cb")]
#[test_action("s3", "DeleteBucketOwnershipControls", checksum = "9727d2b1")]
#[tokio::test]
async fn s3_bucket_ownership_controls() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-own")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_ownership_controls()
        .bucket("conf-own")
        .ownership_controls(
            aws_sdk_s3::types::OwnershipControls::builder()
                .rules(
                    aws_sdk_s3::types::OwnershipControlsRule::builder()
                        .object_ownership(aws_sdk_s3::types::ObjectOwnership::BucketOwnerEnforced)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_bucket_ownership_controls()
        .bucket("conf-own")
        .send()
        .await
        .unwrap();

    client
        .delete_bucket_ownership_controls()
        .bucket("conf-own")
        .send()
        .await
        .unwrap();
}

// -- Bucket inventory configuration --

#[test_action("s3", "PutBucketInventoryConfiguration", checksum = "f1431dd8")]
#[test_action("s3", "GetBucketInventoryConfiguration", checksum = "3e191949")]
#[test_action("s3", "DeleteBucketInventoryConfiguration", checksum = "5fb3b7de")]
#[tokio::test]
async fn s3_bucket_inventory() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-inv")
        .send()
        .await
        .unwrap();

    client
        .put_bucket_inventory_configuration()
        .bucket("conf-inv")
        .id("conf-inv-id")
        .inventory_configuration(
            aws_sdk_s3::types::InventoryConfiguration::builder()
                .id("conf-inv-id")
                .is_enabled(true)
                .destination(
                    aws_sdk_s3::types::InventoryDestination::builder()
                        .s3_bucket_destination(
                            aws_sdk_s3::types::InventoryS3BucketDestination::builder()
                                .bucket("arn:aws:s3:::conf-inv-dest")
                                .format(aws_sdk_s3::types::InventoryFormat::Csv)
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .schedule(
                    aws_sdk_s3::types::InventorySchedule::builder()
                        .frequency(aws_sdk_s3::types::InventoryFrequency::Daily)
                        .build()
                        .unwrap(),
                )
                .included_object_versions(
                    aws_sdk_s3::types::InventoryIncludedObjectVersions::Current,
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .get_bucket_inventory_configuration()
        .bucket("conf-inv")
        .id("conf-inv-id")
        .send()
        .await
        .unwrap();

    client
        .delete_bucket_inventory_configuration()
        .bucket("conf-inv")
        .id("conf-inv-id")
        .send()
        .await
        .unwrap();
}

// -- Multipart upload --

#[test_action("s3", "CreateMultipartUpload", checksum = "2107d532")]
#[test_action("s3", "UploadPart", checksum = "40b8899a")]
#[test_action("s3", "CompleteMultipartUpload", checksum = "c7812f62")]
#[test_action("s3", "ListParts", checksum = "a9fcbf29")]
#[test_action("s3", "ListMultipartUploads", checksum = "9378e74e")]
#[tokio::test]
async fn s3_multipart_upload() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-mpu")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("conf-mpu")
        .key("bigfile.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    client
        .list_multipart_uploads()
        .bucket("conf-mpu")
        .send()
        .await
        .unwrap();

    // Upload 5MB part (minimum)
    let part_data = vec![b'A'; 5 * 1024 * 1024];
    let part = client
        .upload_part()
        .bucket("conf-mpu")
        .key("bigfile.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part_data))
        .send()
        .await
        .unwrap();
    let etag = part.e_tag().unwrap().to_string();

    client
        .list_parts()
        .bucket("conf-mpu")
        .key("bigfile.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap();

    client
        .complete_multipart_upload()
        .bucket("conf-mpu")
        .key("bigfile.bin")
        .upload_id(&upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(&etag)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
}

// -- AbortMultipartUpload --

#[test_action("s3", "AbortMultipartUpload", checksum = "0d1d4ebe")]
#[tokio::test]
async fn s3_abort_multipart_upload() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-abort")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("conf-abort")
        .key("abort.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    client
        .abort_multipart_upload()
        .bucket("conf-abort")
        .key("abort.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap();
}

// -- UploadPartCopy --

#[test_action("s3", "UploadPartCopy", checksum = "268db2f3")]
#[tokio::test]
async fn s3_upload_part_copy() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("conf-upc")
        .send()
        .await
        .unwrap();

    // Create source object
    let data = vec![b'B'; 5 * 1024 * 1024];
    client
        .put_object()
        .bucket("conf-upc")
        .key("source.bin")
        .body(ByteStream::from(data))
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("conf-upc")
        .key("dest.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    client
        .upload_part_copy()
        .bucket("conf-upc")
        .key("dest.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source("conf-upc/source.bin")
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Error path tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn s3_get_object_nonexistent_key_returns_error() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("err-bucket")
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket("err-bucket")
        .key("does-not-exist")
        .send()
        .await;
    assert!(result.is_err(), "GetObject on nonexistent key should fail");
}

#[tokio::test]
async fn s3_head_bucket_nonexistent_returns_error() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    let result = client.head_bucket().bucket("no-such-bucket").send().await;
    assert!(
        result.is_err(),
        "HeadBucket on nonexistent bucket should fail"
    );
}

#[tokio::test]
async fn s3_delete_object_nonexistent_bucket_returns_error() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    let result = client
        .delete_object()
        .bucket("no-such-bucket")
        .key("k")
        .send()
        .await;
    assert!(
        result.is_err(),
        "DeleteObject on nonexistent bucket should fail"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Bucket configuration ops added in PR for conformance closure
// ─────────────────────────────────────────────────────────────────────────────

#[test_action("s3", "PutBucketAnalyticsConfiguration", checksum = "c3f87359")]
#[test_action("s3", "GetBucketAnalyticsConfiguration", checksum = "4b96ba94")]
#[test_action("s3", "DeleteBucketAnalyticsConfiguration", checksum = "a7fdb327")]
#[test_action("s3", "ListBucketAnalyticsConfigurations", checksum = "a962073b")]
#[tokio::test]
async fn s3_analytics_configuration_lifecycle() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("analytics-bkt")
        .send()
        .await
        .unwrap();
    let cfg = aws_sdk_s3::types::AnalyticsConfiguration::builder()
        .id("cfg1")
        .storage_class_analysis(aws_sdk_s3::types::StorageClassAnalysis::builder().build())
        .build()
        .unwrap();
    client
        .put_bucket_analytics_configuration()
        .bucket("analytics-bkt")
        .id("cfg1")
        .analytics_configuration(cfg)
        .send()
        .await
        .unwrap();
    client
        .get_bucket_analytics_configuration()
        .bucket("analytics-bkt")
        .id("cfg1")
        .send()
        .await
        .unwrap();
    client
        .list_bucket_analytics_configurations()
        .bucket("analytics-bkt")
        .send()
        .await
        .unwrap();
    client
        .delete_bucket_analytics_configuration()
        .bucket("analytics-bkt")
        .id("cfg1")
        .send()
        .await
        .unwrap();
}

#[test_action(
    "s3",
    "PutBucketIntelligentTieringConfiguration",
    checksum = "c7246f9e"
)]
#[test_action(
    "s3",
    "GetBucketIntelligentTieringConfiguration",
    checksum = "2298a34b"
)]
#[test_action(
    "s3",
    "DeleteBucketIntelligentTieringConfiguration",
    checksum = "51f8be5f"
)]
#[test_action(
    "s3",
    "ListBucketIntelligentTieringConfigurations",
    checksum = "ddb872b0"
)]
#[tokio::test]
async fn s3_intelligent_tiering_lifecycle() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("it-bkt")
        .send()
        .await
        .unwrap();
    let cfg = aws_sdk_s3::types::IntelligentTieringConfiguration::builder()
        .id("itcfg")
        .status(aws_sdk_s3::types::IntelligentTieringStatus::Enabled)
        .tierings(
            aws_sdk_s3::types::Tiering::builder()
                .days(90)
                .access_tier(aws_sdk_s3::types::IntelligentTieringAccessTier::ArchiveAccess)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();
    client
        .put_bucket_intelligent_tiering_configuration()
        .bucket("it-bkt")
        .id("itcfg")
        .intelligent_tiering_configuration(cfg)
        .send()
        .await
        .unwrap();
    client
        .get_bucket_intelligent_tiering_configuration()
        .bucket("it-bkt")
        .id("itcfg")
        .send()
        .await
        .unwrap();
    client
        .list_bucket_intelligent_tiering_configurations()
        .bucket("it-bkt")
        .send()
        .await
        .unwrap();
    client
        .delete_bucket_intelligent_tiering_configuration()
        .bucket("it-bkt")
        .id("itcfg")
        .send()
        .await
        .unwrap();
}

#[test_action("s3", "PutBucketMetricsConfiguration", checksum = "c92bece4")]
#[test_action("s3", "GetBucketMetricsConfiguration", checksum = "33d906da")]
#[test_action("s3", "DeleteBucketMetricsConfiguration", checksum = "3327c4bd")]
#[test_action("s3", "ListBucketMetricsConfigurations", checksum = "ceab5cf0")]
#[tokio::test]
async fn s3_metrics_configuration_lifecycle() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("metrics-bkt")
        .send()
        .await
        .unwrap();
    let cfg = aws_sdk_s3::types::MetricsConfiguration::builder()
        .id("mcfg")
        .build()
        .unwrap();
    client
        .put_bucket_metrics_configuration()
        .bucket("metrics-bkt")
        .id("mcfg")
        .metrics_configuration(cfg)
        .send()
        .await
        .unwrap();
    client
        .get_bucket_metrics_configuration()
        .bucket("metrics-bkt")
        .id("mcfg")
        .send()
        .await
        .unwrap();
    client
        .list_bucket_metrics_configurations()
        .bucket("metrics-bkt")
        .send()
        .await
        .unwrap();
    client
        .delete_bucket_metrics_configuration()
        .bucket("metrics-bkt")
        .id("mcfg")
        .send()
        .await
        .unwrap();
}

#[test_action("s3", "ListBucketInventoryConfigurations", checksum = "0088bbbb")]
#[tokio::test]
async fn s3_list_bucket_inventory_configurations() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("inv-list-bkt")
        .send()
        .await
        .unwrap();
    client
        .list_bucket_inventory_configurations()
        .bucket("inv-list-bkt")
        .send()
        .await
        .unwrap();
}

#[test_action("s3", "PutBucketRequestPayment", checksum = "4825aa35")]
#[test_action("s3", "GetBucketRequestPayment", checksum = "e4a2cc80")]
#[tokio::test]
async fn s3_request_payment_round_trip() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("rp-bkt")
        .send()
        .await
        .unwrap();
    client
        .put_bucket_request_payment()
        .bucket("rp-bkt")
        .request_payment_configuration(
            aws_sdk_s3::types::RequestPaymentConfiguration::builder()
                .payer(aws_sdk_s3::types::Payer::Requester)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let resp = client
        .get_bucket_request_payment()
        .bucket("rp-bkt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.payer().unwrap().as_str(), "Requester");
}

#[test_action("s3", "PutBucketAbac", checksum = "796c51dc")]
#[test_action("s3", "GetBucketAbac", checksum = "19f51e30")]
#[tokio::test]
async fn s3_abac_round_trip() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("abac-bkt")
        .send()
        .await
        .unwrap();
    client
        .put_bucket_abac()
        .bucket("abac-bkt")
        .abac_status(
            aws_sdk_s3::types::AbacStatus::builder()
                .status(aws_sdk_s3::types::BucketAbacStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    client
        .get_bucket_abac()
        .bucket("abac-bkt")
        .send()
        .await
        .unwrap();
}

#[test_action("s3", "GetBucketPolicyStatus", checksum = "ba6e1ab4")]
#[tokio::test]
async fn s3_get_bucket_policy_status() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("ps-bkt")
        .send()
        .await
        .unwrap();
    client
        .get_bucket_policy_status()
        .bucket("ps-bkt")
        .send()
        .await
        .unwrap();
}

const S3_AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=0";

async fn raw_put(server: &TestServer, path: &str, body: &str) {
    let resp = reqwest::Client::new()
        .put(format!("{}{}", server.endpoint(), path))
        .header("content-type", "application/xml")
        .header("Authorization", S3_AUTH)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "PUT {path} -> {:?}",
        resp.status()
    );
}

async fn raw_post(server: &TestServer, path: &str, body: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{}{}", server.endpoint(), path))
        .header("content-type", "application/xml")
        .header("Authorization", S3_AUTH)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "POST {path} -> {:?}",
        resp.status()
    );
}

async fn raw_get(server: &TestServer, path: &str) {
    let resp = reqwest::Client::new()
        .get(format!("{}{}", server.endpoint(), path))
        .header("Authorization", S3_AUTH)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "GET {path} -> {:?}",
        resp.status()
    );
}

async fn raw_delete(server: &TestServer, path: &str) {
    let resp = reqwest::Client::new()
        .delete(format!("{}{}", server.endpoint(), path))
        .header("Authorization", S3_AUTH)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "DELETE {path} -> {:?}",
        resp.status()
    );
}

#[test_action("s3", "CreateBucketMetadataConfiguration", checksum = "55f9905d")]
#[test_action("s3", "GetBucketMetadataConfiguration", checksum = "b35095ce")]
#[test_action("s3", "DeleteBucketMetadataConfiguration", checksum = "00b73c78")]
#[test_action(
    "s3",
    "UpdateBucketMetadataInventoryTableConfiguration",
    checksum = "90e7c0b1"
)]
#[test_action(
    "s3",
    "UpdateBucketMetadataJournalTableConfiguration",
    checksum = "09888cd8"
)]
#[tokio::test]
async fn s3_metadata_configuration_lifecycle() {
    // SDK 1.119 enforces a strict XML root element on the response; raw
    // HTTP exercises every route the conformance probe checks without
    // relying on SDK XML tolerance for these emulator-stub responses.
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("md-bkt")
        .send()
        .await
        .unwrap();
    raw_post(
        &server,
        "/md-bkt?metadataConfiguration",
        "<MetadataConfiguration/>",
    )
    .await;
    raw_get(&server, "/md-bkt?metadataConfiguration").await;
    raw_put(
        &server,
        "/md-bkt?metadataInventoryTable",
        "<InventoryTableConfigurationUpdates/>",
    )
    .await;
    raw_put(
        &server,
        "/md-bkt?metadataJournalTable",
        "<JournalTableConfigurationUpdates/>",
    )
    .await;
    raw_delete(&server, "/md-bkt?metadataConfiguration").await;
}

#[test_action("s3", "CreateBucketMetadataTableConfiguration", checksum = "0f66631c")]
#[test_action("s3", "GetBucketMetadataTableConfiguration", checksum = "a59031ff")]
#[test_action("s3", "DeleteBucketMetadataTableConfiguration", checksum = "e6472b69")]
#[tokio::test]
async fn s3_metadata_table_configuration_lifecycle() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("mt-bkt")
        .send()
        .await
        .unwrap();
    raw_post(
        &server,
        "/mt-bkt?metadataTable",
        "<MetadataTableConfiguration/>",
    )
    .await;
    raw_get(&server, "/mt-bkt?metadataTable").await;
    raw_delete(&server, "/mt-bkt?metadataTable").await;
}

#[test_action("s3", "GetObjectTorrent", checksum = "ab1d8957")]
#[tokio::test]
async fn s3_get_object_torrent() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("torrent-bkt")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("torrent-bkt")
        .key("file")
        .body(aws_sdk_s3::primitives::ByteStream::from(b"hello".to_vec()))
        .send()
        .await
        .unwrap();
    client
        .get_object_torrent()
        .bucket("torrent-bkt")
        .key("file")
        .send()
        .await
        .unwrap();
}

#[test_action("s3", "RenameObject", checksum = "8f30683a")]
#[tokio::test]
async fn s3_rename_object() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("rename-bkt")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("rename-bkt")
        .key("old")
        .body(aws_sdk_s3::primitives::ByteStream::from(b"hello".to_vec()))
        .send()
        .await
        .unwrap();
    client
        .rename_object()
        .bucket("rename-bkt")
        .key("new")
        .rename_source("/old")
        .send()
        .await
        .unwrap();
}

#[test_action("s3", "SelectObjectContent", checksum = "1719d4b6")]
#[tokio::test]
async fn s3_select_object_content() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("select-bkt")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("select-bkt")
        .key("data.csv")
        .body(aws_sdk_s3::primitives::ByteStream::from(
            b"a,b\n1,2\n".to_vec(),
        ))
        .send()
        .await
        .unwrap();
    let resp = client
        .select_object_content()
        .bucket("select-bkt")
        .key("data.csv")
        .expression("SELECT * FROM s3object")
        .expression_type(aws_sdk_s3::types::ExpressionType::Sql)
        .input_serialization(
            aws_sdk_s3::types::InputSerialization::builder()
                .csv(
                    aws_sdk_s3::types::CsvInput::builder()
                        .file_header_info(aws_sdk_s3::types::FileHeaderInfo::Use)
                        .build(),
                )
                .build(),
        )
        .output_serialization(
            aws_sdk_s3::types::OutputSerialization::builder()
                .csv(aws_sdk_s3::types::CsvOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .unwrap();

    let mut stream = resp.payload;
    let mut records = Vec::new();
    let mut got_end = false;
    loop {
        match stream.recv().await {
            Ok(Some(event)) => match event {
                aws_sdk_s3::types::SelectObjectContentEventStream::Records(rec) => {
                    if let Some(payload) = rec.payload() {
                        records.extend_from_slice(payload.as_ref());
                    }
                }
                aws_sdk_s3::types::SelectObjectContentEventStream::Stats(_) => {}
                aws_sdk_s3::types::SelectObjectContentEventStream::End(_) => {
                    got_end = true;
                    break;
                }
                other => panic!("unexpected SelectObjectContent event: {other:?}"),
            },
            Ok(None) => break,
            Err(e) => panic!("SelectObjectContent stream error: {e:?}"),
        }
    }
    assert!(got_end, "expected End event in SelectObjectContent stream");
    assert!(
        !records.is_empty(),
        "expected records in SelectObjectContent response"
    );
    assert_eq!(String::from_utf8_lossy(&records), "1,2\n");
}

#[test_action("s3", "UpdateObjectEncryption", checksum = "32f05360")]
#[tokio::test]
async fn s3_update_object_encryption() {
    // The aws-sdk-s3 1.119 release predates UpdateObjectEncryption, so
    // exercise the route directly with reqwest. The op is PUT
    // /{bucket}/{key}?encryption with x-amz-server-side-encryption.
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("upd-enc-bkt")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("upd-enc-bkt")
        .key("k")
        .body(aws_sdk_s3::primitives::ByteStream::from(b"x".to_vec()))
        .send()
        .await
        .unwrap();
    let resp = reqwest::Client::new()
        .put(format!("{}/upd-enc-bkt/k?encryption", server.endpoint()))
        .header("x-amz-server-side-encryption", "AES256")
        .header("Authorization", S3_AUTH)
        .body(Vec::<u8>::new())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status={:?}", resp.status());
}

#[test_action("s3", "WriteGetObjectResponse", checksum = "51fb114f")]
#[tokio::test]
async fn s3_write_get_object_response() {
    // The SDK signs WriteGetObjectResponse against an Object Lambda
    // endpoint host that fakecloud doesn't model; raw HTTP exercises the
    // route directly.
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/WriteGetObjectResponse", server.endpoint()))
        .header("x-amz-request-route", "route")
        .header("x-amz-request-token", "tok")
        .header("Authorization", S3_AUTH)
        .body(b"out".to_vec())
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status={:?}", resp.status());
}

#[test_action("s3", "ListDirectoryBuckets", checksum = "f4d51582")]
#[tokio::test]
async fn s3_list_directory_buckets() {
    // SDK uses an S3 Express host this emulator doesn't model; raw GET
    // hits the documented route.
    let server = TestServer::start().await;
    let resp = reqwest::Client::new()
        .get(format!("{}/?x-id=ListDirectoryBuckets", server.endpoint()))
        .header("Authorization", S3_AUTH)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "status={:?}", resp.status());
}

#[test_action("s3", "CreateSession", checksum = "8a90adfe")]
#[tokio::test]
async fn s3_create_session() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    client
        .create_bucket()
        .bucket("session-bkt")
        .send()
        .await
        .unwrap();
    client
        .create_session()
        .bucket("session-bkt")
        .send()
        .await
        .unwrap();
}
