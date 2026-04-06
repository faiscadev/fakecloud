mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// -- Bucket lifecycle --

#[test_action("s3", "CreateBucket", checksum = "15accf87")]
#[test_action("s3", "ListBuckets", checksum = "80b1347a")]
#[test_action("s3", "HeadBucket", checksum = "803cc873")]
#[test_action("s3", "GetBucketLocation", checksum = "c6da5a3c")]
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

#[test_action("s3", "PutObject", checksum = "e9dbbbaa")]
#[test_action("s3", "GetObject", checksum = "cd0afbe3")]
#[test_action("s3", "HeadObject", checksum = "dd127249")]
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

#[test_action("s3", "CopyObject", checksum = "03812378")]
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

#[test_action("s3", "DeleteObjects", checksum = "b48fe2d0")]
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

#[test_action("s3", "ListObjectsV2", checksum = "0b2ea04f")]
#[test_action("s3", "ListObjects", checksum = "e0e01f68")]
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

#[test_action("s3", "ListObjectVersions", checksum = "6371c49f")]
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

#[test_action("s3", "GetObjectAttributes", checksum = "1b2f99bd")]
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

    let _ = client
        .get_object_attributes()
        .bucket("conf-attrs")
        .key("a.txt")
        .object_attributes(aws_sdk_s3::types::ObjectAttributes::ObjectSize)
        .send()
        .await;
}

// -- RestoreObject --

#[test_action("s3", "RestoreObject", checksum = "51dbe951")]
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

    let _ = client
        .restore_object()
        .bucket("conf-restore")
        .key("archive.txt")
        .restore_request(aws_sdk_s3::types::RestoreRequest::builder().days(1).build())
        .send()
        .await;
}

// -- Object tagging --

#[test_action("s3", "PutObjectTagging", checksum = "80e8c9eb")]
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

#[test_action("s3", "PutObjectAcl", checksum = "a1356c24")]
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

#[test_action("s3", "PutObjectRetention", checksum = "278f33b6")]
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

    let _ = client
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
        .await;

    let _ = client
        .get_object_retention()
        .bucket("conf-oret")
        .key("ret.txt")
        .send()
        .await;
}

#[test_action("s3", "PutObjectLegalHold", checksum = "4707c231")]
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

    let _ = client
        .put_object_legal_hold()
        .bucket("conf-olh")
        .key("hold.txt")
        .legal_hold(
            aws_sdk_s3::types::ObjectLockLegalHold::builder()
                .status(aws_sdk_s3::types::ObjectLockLegalHoldStatus::On)
                .build(),
        )
        .send()
        .await;

    let _ = client
        .get_object_legal_hold()
        .bucket("conf-olh")
        .key("hold.txt")
        .send()
        .await;
}

// -- Bucket tagging --

#[test_action("s3", "PutBucketTagging", checksum = "5ad8a3c6")]
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

#[test_action("s3", "PutBucketAcl", checksum = "2b56bf7d")]
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

#[test_action("s3", "PutBucketVersioning", checksum = "0b8739d5")]
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

#[test_action("s3", "PutBucketCors", checksum = "d45fcf4a")]
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

#[test_action("s3", "PutBucketWebsite", checksum = "d64c97c0")]
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

#[test_action("s3", "PutBucketAccelerateConfiguration", checksum = "9c5d6ce0")]
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

#[test_action("s3", "PutPublicAccessBlock", checksum = "ddccd75d")]
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

#[test_action("s3", "PutBucketEncryption", checksum = "bfeb2d44")]
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

#[test_action("s3", "PutBucketLifecycleConfiguration", checksum = "29af802c")]
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

#[test_action("s3", "PutBucketLogging", checksum = "50be50fa")]
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

#[test_action("s3", "PutBucketPolicy", checksum = "dd80bd6c")]
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

#[test_action("s3", "PutObjectLockConfiguration", checksum = "5ee132b3")]
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

    let _ = client
        .put_object_lock_configuration()
        .bucket("conf-olock")
        .object_lock_configuration(
            aws_sdk_s3::types::ObjectLockConfiguration::builder()
                .object_lock_enabled(aws_sdk_s3::types::ObjectLockEnabled::Enabled)
                .build(),
        )
        .send()
        .await;

    let _ = client
        .get_object_lock_configuration()
        .bucket("conf-olock")
        .send()
        .await;
}

// -- Bucket replication --

#[test_action("s3", "PutBucketReplication", checksum = "03741feb")]
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

    let _ = client
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
        .await;

    let _ = client
        .get_bucket_replication()
        .bucket("conf-repl")
        .send()
        .await;

    let _ = client
        .delete_bucket_replication()
        .bucket("conf-repl")
        .send()
        .await;
}

// -- Bucket ownership controls --

#[test_action("s3", "PutBucketOwnershipControls", checksum = "aa269fa6")]
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

    let _ = client
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

#[test_action("s3", "CreateMultipartUpload", checksum = "84f77436")]
#[test_action("s3", "UploadPart", checksum = "de83c026")]
#[test_action("s3", "CompleteMultipartUpload", checksum = "0df95972")]
#[test_action("s3", "ListParts", checksum = "61616240")]
#[test_action("s3", "ListMultipartUploads", checksum = "9f3daa98")]
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

    let _ = client
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

    let _ = client
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

#[test_action("s3", "UploadPartCopy", checksum = "49d22a26")]
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

    let _ = client
        .upload_part_copy()
        .bucket("conf-upc")
        .key("dest.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .copy_source("conf-upc/source.bin")
        .send()
        .await;
}
