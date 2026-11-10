mod helpers;

use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    CompressionFormat, DeliveryStreamEncryptionConfigurationInput, DeliveryStreamType,
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
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket("landing")
        .send()
        .await
        .unwrap();

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

    // Omitted hints resolve to the real defaults, far above these records.
    let description = fh
        .describe_delivery_stream()
        .delivery_stream_name("durable")
        .send()
        .await
        .unwrap();
    let hints = description
        .delivery_stream_description()
        .unwrap()
        .destinations()[0]
        .extended_s3_destination_description()
        .unwrap()
        .buffering_hints()
        .unwrap();
    assert_eq!(hints.size_in_mbs(), Some(5));
    assert_eq!(hints.interval_in_seconds(), Some(300));

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

    fh.put_record()
        .delivery_stream_name("durable")
        .record(
            Record::builder()
                .data(Blob::new(b"\x00\xff\n"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(s3
        .list_objects_v2()
        .bucket("fh-landing")
        .send()
        .await
        .unwrap()
        .contents()
        .is_empty());
    // Graceful restart drains the pending buffer before S3 is reopened.
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
    assert_eq!(listing.contents().len(), 1);
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
    // The bare MD5 of the one buffered object's body, quoted by GetObject.
    assert_eq!(obj.e_tag(), Some("\"e7f4596cf35e2ba5f4d73e8635602c95\""));
    assert_eq!(obj.content_type(), Some("application/octet-stream"));
    assert_eq!(obj.content_encoding(), None);
    let bytes = obj.body.collect().await.expect("collect").into_bytes();
    assert_eq!(bytes.as_ref(), b"persist-me\x00\xff\n");
}

#[tokio::test]
async fn persistence_shutdown_drains_default_buffer_with_gzip_metadata() {
    use std::io::Read;

    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let s3 = server.s3_client().await;
    s3.create_bucket()
        .bucket("fh-drain-gzip")
        .send()
        .await
        .unwrap();
    let fh = server.firehose_client().await;
    fh.create_delivery_stream()
        .delivery_stream_name("drain-gzip")
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::123456789012:role/fh")
                .bucket_arn("arn:aws:s3:::fh-drain-gzip")
                .compression_format(CompressionFormat::Gzip)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    fh.put_record_batch()
        .delivery_stream_name("drain-gzip")
        .records(
            Record::builder()
                .data(Blob::new(b"first\n"))
                .build()
                .unwrap(),
        )
        .records(Record::builder().data(Blob::new([0, 255])).build().unwrap())
        .send()
        .await
        .unwrap();
    assert!(s3
        .list_objects_v2()
        .bucket("fh-drain-gzip")
        .send()
        .await
        .unwrap()
        .contents()
        .is_empty());
    server.restart().await;
    let s3 = server.s3_client().await;
    let listing = s3
        .list_objects_v2()
        .bucket("fh-drain-gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(listing.contents().len(), 1);
    let key = listing.contents()[0].key().unwrap().to_owned();
    assert!(key.ends_with(".gz"));
    let object = s3
        .get_object()
        .bucket("fh-drain-gzip")
        .key(&key)
        .send()
        .await
        .unwrap();
    assert_eq!(object.content_type(), Some("application/octet-stream"));
    assert_eq!(object.content_encoding(), Some("gzip"));
    let length = object.content_length().unwrap();
    let raw = object.body.collect().await.unwrap().into_bytes();
    assert_eq!(length, raw.len() as i64);
    assert!(raw.starts_with(&[0x1f, 0x8b, 8]));
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(raw.as_ref())
        .read_to_end(&mut decoded)
        .unwrap();
    assert_eq!(decoded, b"first\n\x00\xff");
    // A second restart proves a completed durable object, with no pending
    // snapshot replay or duplicate shutdown delivery.
    server.restart().await;
    let s3 = server.s3_client().await;
    let listing = s3
        .list_objects_v2()
        .bucket("fh-drain-gzip")
        .send()
        .await
        .unwrap();
    assert_eq!(listing.contents().len(), 1);
    assert_eq!(listing.contents()[0].key(), Some(key.as_str()));
    let object = s3
        .get_object()
        .bucket("fh-drain-gzip")
        .key(&key)
        .send()
        .await
        .unwrap();
    assert_eq!(object.content_type(), Some("application/octet-stream"));
    assert_eq!(object.content_encoding(), Some("gzip"));
    assert_eq!(object.content_length(), Some(length));
    assert_eq!(object.body.collect().await.unwrap().into_bytes(), raw);
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
