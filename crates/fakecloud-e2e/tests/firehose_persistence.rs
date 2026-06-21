mod helpers;

use aws_sdk_firehose::types::{
    DeliveryStreamEncryptionConfigurationInput, DeliveryStreamType,
    ExtendedS3DestinationConfiguration, KeyType, Tag,
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
