mod helpers;

use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use aws_sdk_kinesis::types::ShardIteratorType;
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("kinesis", "CreateStream", checksum = "d2d1a234")]
#[test_action("kinesis", "DescribeStream", checksum = "eca54e4c")]
#[test_action("kinesis", "DescribeStreamSummary", checksum = "50667cc4")]
#[test_action("kinesis", "ListStreams", checksum = "ca5dcdd7")]
#[test_action("kinesis", "DeleteStream", checksum = "51c62afa")]
#[tokio::test]
async fn kinesis_stream_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("conf-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let describe = client
        .describe_stream()
        .stream_name("conf-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(
        describe
            .stream_description()
            .unwrap()
            .stream_status()
            .as_str(),
        "ACTIVE"
    );

    let summary = client
        .describe_stream_summary()
        .stream_name("conf-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(
        summary.stream_description_summary().unwrap().stream_name(),
        "conf-stream"
    );

    let list = client.list_streams().send().await.unwrap();
    assert!(list.stream_names().contains(&"conf-stream".to_string()));

    client
        .delete_stream()
        .stream_name("conf-stream")
        .send()
        .await
        .unwrap();

    let deleted = client
        .describe_stream()
        .stream_name("conf-stream")
        .send()
        .await;
    assert!(deleted.is_err());
}

#[test_action("kinesis", "AddTagsToStream", checksum = "1864db43")]
#[test_action("kinesis", "ListTagsForStream", checksum = "493cccaa")]
#[test_action("kinesis", "RemoveTagsFromStream", checksum = "d081af86")]
#[test_action("kinesis", "IncreaseStreamRetentionPeriod", checksum = "2c318c54")]
#[test_action("kinesis", "DecreaseStreamRetentionPeriod", checksum = "551aaa3a")]
#[tokio::test]
async fn kinesis_tags_and_retention() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("conf-tags")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    client
        .add_tags_to_stream()
        .stream_name("conf-tags")
        .tags("env", "test")
        .send()
        .await
        .unwrap();
    let tags = client
        .list_tags_for_stream()
        .stream_name("conf-tags")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);

    client
        .increase_stream_retention_period()
        .stream_name("conf-tags")
        .retention_period_hours(48)
        .send()
        .await
        .unwrap();
    client
        .decrease_stream_retention_period()
        .stream_name("conf-tags")
        .retention_period_hours(24)
        .send()
        .await
        .unwrap();

    let summary = client
        .describe_stream_summary()
        .stream_name("conf-tags")
        .send()
        .await
        .unwrap();
    assert_eq!(
        summary
            .stream_description_summary()
            .unwrap()
            .retention_period_hours(),
        24
    );

    client
        .remove_tags_from_stream()
        .stream_name("conf-tags")
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_stream()
        .stream_name("conf-tags")
        .send()
        .await
        .unwrap();
    assert!(tags.tags().is_empty());
}

#[test_action("kinesis", "PutRecord", checksum = "ebd87879")]
#[tokio::test]
async fn kinesis_put_record() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("conf-put")
        .shard_count(2)
        .send()
        .await
        .unwrap();

    let first = client
        .put_record()
        .stream_name("conf-put")
        .partition_key("conf-key")
        .data(Blob::new(b"first"))
        .send()
        .await
        .unwrap();
    let second = client
        .put_record()
        .stream_name("conf-put")
        .partition_key("conf-key")
        .data(Blob::new(b"second"))
        .send()
        .await
        .unwrap();

    assert_eq!(first.shard_id(), second.shard_id());
    assert!(first.sequence_number() < second.sequence_number());
}

#[test_action("kinesis", "PutRecords", checksum = "27e5bb6b")]
#[tokio::test]
async fn kinesis_put_records() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("conf-batch")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let ok_entry = PutRecordsRequestEntry::builder()
        .data(Blob::new(b"ok"))
        .partition_key("good-key")
        .build()
        .unwrap();
    let bad_entry = PutRecordsRequestEntry::builder()
        .data(Blob::new(b"bad"))
        .partition_key("")
        .build()
        .unwrap();

    let response = client
        .put_records()
        .stream_name("conf-batch")
        .records(ok_entry)
        .records(bad_entry)
        .send()
        .await
        .unwrap();

    assert_eq!(response.failed_record_count(), Some(1));
    assert!(response.records()[0].sequence_number().is_some());
    assert_eq!(
        response.records()[1].error_code(),
        Some("InvalidArgumentException")
    );
}

#[test_action("kinesis", "GetShardIterator", checksum = "8d745e01")]
#[test_action("kinesis", "GetRecords", checksum = "4f940d65")]
#[tokio::test]
async fn kinesis_get_records() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("conf-read")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let write = client
        .put_record()
        .stream_name("conf-read")
        .partition_key("read-key")
        .data(Blob::new(b"payload"))
        .send()
        .await
        .unwrap();

    let iterator = client
        .get_shard_iterator()
        .stream_name("conf-read")
        .shard_id(write.shard_id())
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();

    let records = client
        .get_records()
        .shard_iterator(iterator.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(records.records().len(), 1);
    assert_eq!(records.records()[0].partition_key(), "read-key");
    assert!(records.next_shard_iterator().is_some());
}
