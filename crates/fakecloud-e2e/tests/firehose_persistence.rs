mod helpers;

use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    DeliveryStreamEncryptionConfigurationInput, DeliveryStreamType,
    ExtendedS3DestinationConfiguration, KeyType, Record, Tag,
};
use helpers::TestServer;

/// A delivery stream, its destination, tags, and encryption state all survive
/// a restart in persistent mode.
#[tokio::test]
async fn persistence_round_trip_stream_tags_and_encryption() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let fh = server.firehose_client().await;

    fh.create_delivery_stream()
        .delivery_stream_name("events")
        .delivery_stream_type(DeliveryStreamType::DirectPut)
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::123456789012:role/fh")
                .bucket_arn("arn:aws:s3:::landing")
                .prefix("raw/")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    fh.tag_delivery_stream()
        .delivery_stream_name("events")
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .send()
        .await
        .unwrap();

    fh.start_delivery_stream_encryption()
        .delivery_stream_name("events")
        .delivery_stream_encryption_configuration_input(
            DeliveryStreamEncryptionConfigurationInput::builder()
                .key_type(KeyType::AwsOwnedCmk)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    server.restart().await;
    let fh = server.firehose_client().await;

    // Stream + destination survive.
    let desc = fh
        .describe_delivery_stream()
        .delivery_stream_name("events")
        .send()
        .await
        .unwrap();
    let d = desc.delivery_stream_description().unwrap();
    assert_eq!(d.delivery_stream_name(), "events");
    assert_eq!(
        d.delivery_stream_encryption_configuration()
            .and_then(|e| e.status())
            .map(|s| s.as_str()),
        Some("ENABLED")
    );
    let dest = &d.destinations()[0];
    let s3 = dest.extended_s3_destination_description().unwrap();
    assert_eq!(s3.bucket_arn(), "arn:aws:s3:::landing");
    assert_eq!(s3.prefix(), Some("raw/"));

    // Tags survive.
    let tags = fh
        .list_tags_for_delivery_stream()
        .delivery_stream_name("events")
        .send()
        .await
        .unwrap();
    assert!(tags
        .tags()
        .iter()
        .any(|t| t.key() == "env" && t.value() == Some("prod")));
}

/// Records delivered to an S3 destination survive a restart. Firehose writes
/// delivered objects straight into S3, and S3 durability is write-through to the
/// on-disk store (no in-memory S3 snapshot); the delivery must therefore route
/// through that store or the object is lost on restart (bug-hunt Tier 0
/// side-channel-persistence).
#[tokio::test]
async fn persistence_delivered_records_survive_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;

    // Destination bucket must exist before delivery.
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("fh-landing")
        .send()
        .await
        .expect("create bucket");

    let fh = server.firehose_client().await;
    fh.create_delivery_stream()
        .delivery_stream_name("durable")
        .delivery_stream_type(DeliveryStreamType::DirectPut)
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::123456789012:role/fh")
                .bucket_arn("arn:aws:s3:::fh-landing")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create stream");

    fh.put_record()
        .delivery_stream_name("durable")
        .record(
            Record::builder()
                .data(Blob::new(b"persist-me"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("put record");

    server.restart().await;

    // After restart S3 is rebuilt from the on-disk store; the delivered object
    // must still be there with its original bytes.
    let s3 = server.s3_client().await;
    let listing = s3
        .list_objects_v2()
        .bucket("fh-landing")
        .send()
        .await
        .expect("list");
    let key = listing
        .contents()
        .first()
        .expect("delivered object must survive restart")
        .key()
        .unwrap()
        .to_string();
    let obj = s3
        .get_object()
        .bucket("fh-landing")
        .key(&key)
        .send()
        .await
        .expect("get delivered object");
    let bytes = obj.body.collect().await.expect("collect").into_bytes();
    assert!(bytes.starts_with(b"persist-me"));
}

/// DeleteDeliveryStream durability: a deleted stream stays gone after restart.
#[tokio::test]
async fn persistence_delete_stream_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let fh = server.firehose_client().await;

    fh.create_delivery_stream()
        .delivery_stream_name("ephemeral")
        .send()
        .await
        .unwrap();
    fh.delete_delivery_stream()
        .delivery_stream_name("ephemeral")
        .send()
        .await
        .unwrap();

    server.restart().await;
    let fh = server.firehose_client().await;

    let streams = fh.list_delivery_streams().send().await.unwrap();
    assert!(!streams
        .delivery_stream_names()
        .iter()
        .any(|s| s == "ephemeral"));
}
