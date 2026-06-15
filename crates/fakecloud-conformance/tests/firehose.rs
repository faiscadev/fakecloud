mod helpers;

use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    DeliveryStreamEncryptionConfigurationInput, ExtendedS3DestinationConfiguration,
    ExtendedS3DestinationUpdate, KeyType, Record, Tag,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

/// Create a DirectPut delivery stream with an Extended S3 destination so that
/// describe/update/put paths all have a concrete destination to work against.
async fn create_stream(client: &aws_sdk_firehose::Client, name: &str) {
    let dest = ExtendedS3DestinationConfiguration::builder()
        .role_arn("arn:aws:iam::123456789012:role/firehose-role")
        .bucket_arn("arn:aws:s3:::conf-firehose-bucket")
        .build()
        .unwrap();
    client
        .create_delivery_stream()
        .delivery_stream_name(name)
        .extended_s3_destination_configuration(dest)
        .send()
        .await
        .unwrap();
}

#[test_action("firehose", "CreateDeliveryStream", checksum = "54dca75c")]
#[test_action("firehose", "DescribeDeliveryStream", checksum = "21eb5183")]
#[test_action("firehose", "ListDeliveryStreams", checksum = "2fdf2601")]
#[test_action("firehose", "DeleteDeliveryStream", checksum = "6a6b2508")]
#[tokio::test]
async fn firehose_stream_lifecycle() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;

    create_stream(&client, "conf-stream").await;

    let describe = client
        .describe_delivery_stream()
        .delivery_stream_name("conf-stream")
        .send()
        .await
        .unwrap();
    let desc = describe.delivery_stream_description().unwrap();
    assert_eq!(desc.delivery_stream_name(), "conf-stream");
    assert_eq!(desc.delivery_stream_status().as_str(), "ACTIVE");

    let list = client.list_delivery_streams().send().await.unwrap();
    assert!(list
        .delivery_stream_names()
        .contains(&"conf-stream".to_string()));

    client
        .delete_delivery_stream()
        .delivery_stream_name("conf-stream")
        .send()
        .await
        .unwrap();

    let deleted = client
        .describe_delivery_stream()
        .delivery_stream_name("conf-stream")
        .send()
        .await;
    assert!(deleted.is_err());
}

#[test_action("firehose", "TagDeliveryStream", checksum = "749a0e79")]
#[test_action("firehose", "ListTagsForDeliveryStream", checksum = "ce4a0afd")]
#[test_action("firehose", "UntagDeliveryStream", checksum = "60bf9a5f")]
#[tokio::test]
async fn firehose_tag_lifecycle() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;

    create_stream(&client, "conf-tags").await;

    client
        .tag_delivery_stream()
        .delivery_stream_name("conf-tags")
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_delivery_stream()
        .delivery_stream_name("conf-tags")
        .send()
        .await
        .unwrap();
    assert!(tags.tags().iter().any(|t| t.key() == "env"));

    client
        .untag_delivery_stream()
        .delivery_stream_name("conf-tags")
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let after = client
        .list_tags_for_delivery_stream()
        .delivery_stream_name("conf-tags")
        .send()
        .await
        .unwrap();
    assert!(after.tags().iter().all(|t| t.key() != "env"));
}

#[test_action("firehose", "PutRecord", checksum = "167c0889")]
#[tokio::test]
async fn firehose_put_record() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;

    create_stream(&client, "conf-put").await;

    let response = client
        .put_record()
        .delivery_stream_name("conf-put")
        .record(
            Record::builder()
                .data(Blob::new(b"hello\n"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert!(!response.record_id().is_empty());
}

#[test_action("firehose", "PutRecordBatch", checksum = "e01e3cc8")]
#[tokio::test]
async fn firehose_put_record_batch() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;

    create_stream(&client, "conf-batch").await;

    let response = client
        .put_record_batch()
        .delivery_stream_name("conf-batch")
        .records(Record::builder().data(Blob::new(b"one\n")).build().unwrap())
        .records(Record::builder().data(Blob::new(b"two\n")).build().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(response.failed_put_count(), 0);
    assert_eq!(response.request_responses().len(), 2);
}

#[test_action("firehose", "UpdateDestination", checksum = "4e2b8ae3")]
#[tokio::test]
async fn firehose_update_destination() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;

    create_stream(&client, "conf-update").await;

    let describe = client
        .describe_delivery_stream()
        .delivery_stream_name("conf-update")
        .send()
        .await
        .unwrap();
    let desc = describe.delivery_stream_description().unwrap();
    let version_id = desc.version_id().to_string();
    let destination_id = desc.destinations()[0].destination_id().to_string();

    let update = ExtendedS3DestinationUpdate::builder()
        .role_arn("arn:aws:iam::123456789012:role/firehose-role")
        .bucket_arn("arn:aws:s3:::conf-firehose-bucket")
        .prefix("updated/")
        .build();

    client
        .update_destination()
        .delivery_stream_name("conf-update")
        .current_delivery_stream_version_id(version_id)
        .destination_id(destination_id)
        .extended_s3_destination_update(update)
        .send()
        .await
        .unwrap();
}

#[test_action("firehose", "StartDeliveryStreamEncryption", checksum = "af14587d")]
#[test_action("firehose", "StopDeliveryStreamEncryption", checksum = "53c9d850")]
#[tokio::test]
async fn firehose_encryption_lifecycle() {
    let server = TestServer::start().await;
    let client = server.firehose_client().await;

    create_stream(&client, "conf-enc").await;

    client
        .start_delivery_stream_encryption()
        .delivery_stream_name("conf-enc")
        .delivery_stream_encryption_configuration_input(
            DeliveryStreamEncryptionConfigurationInput::builder()
                .key_type(KeyType::CustomerManagedCmk)
                .key_arn("arn:aws:kms:us-east-1:123456789012:key/abc-123")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let enabled = client
        .describe_delivery_stream()
        .delivery_stream_name("conf-enc")
        .send()
        .await
        .unwrap();
    let enc = enabled
        .delivery_stream_description()
        .unwrap()
        .delivery_stream_encryption_configuration()
        .unwrap();
    assert_eq!(enc.status().unwrap().as_str(), "ENABLED");
    assert_eq!(enc.key_type().unwrap().as_str(), "CUSTOMER_MANAGED_CMK");
    assert_eq!(
        enc.key_arn(),
        Some("arn:aws:kms:us-east-1:123456789012:key/abc-123")
    );

    client
        .stop_delivery_stream_encryption()
        .delivery_stream_name("conf-enc")
        .send()
        .await
        .unwrap();

    let disabled = client
        .describe_delivery_stream()
        .delivery_stream_name("conf-enc")
        .send()
        .await
        .unwrap();
    let enc = disabled
        .delivery_stream_description()
        .unwrap()
        .delivery_stream_encryption_configuration()
        .unwrap();
    assert_eq!(enc.status().unwrap().as_str(), "DISABLED");
}
