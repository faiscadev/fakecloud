mod helpers;

use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use aws_sdk_kinesis::types::ShardIteratorType;
use helpers::TestServer;

#[tokio::test]
async fn kinesis_create_describe_list_delete_stream() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("orders")
        .shard_count(2)
        .send()
        .await
        .unwrap();

    let describe = client
        .describe_stream()
        .stream_name("orders")
        .send()
        .await
        .unwrap();
    let description = describe.stream_description().unwrap();
    assert_eq!(description.stream_name(), "orders");
    assert_eq!(description.stream_status().as_str(), "ACTIVE");
    assert_eq!(description.shards().len(), 2);

    let summary = client
        .describe_stream_summary()
        .stream_name("orders")
        .send()
        .await
        .unwrap();
    assert_eq!(
        summary
            .stream_description_summary()
            .unwrap()
            .open_shard_count(),
        2
    );

    let list = client.list_streams().send().await.unwrap();
    assert!(list.stream_names().contains(&"orders".to_string()));

    client
        .delete_stream()
        .stream_name("orders")
        .send()
        .await
        .unwrap();
    let result = client.describe_stream().stream_name("orders").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn kinesis_tags_and_retention() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("tagged")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    client
        .add_tags_to_stream()
        .stream_name("tagged")
        .tags("env", "test")
        .tags("team", "fakecloud")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_stream()
        .stream_name("tagged")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 2);

    client
        .increase_stream_retention_period()
        .stream_name("tagged")
        .retention_period_hours(48)
        .send()
        .await
        .unwrap();

    let summary = client
        .describe_stream_summary()
        .stream_name("tagged")
        .send()
        .await
        .unwrap();
    assert_eq!(
        summary
            .stream_description_summary()
            .unwrap()
            .retention_period_hours(),
        48
    );

    client
        .remove_tags_from_stream()
        .stream_name("tagged")
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let tags = client
        .list_tags_for_stream()
        .stream_name("tagged")
        .send()
        .await
        .unwrap();
    assert_eq!(tags.tags().len(), 1);
}

#[tokio::test]
async fn kinesis_put_record_routes_and_sequences_per_shard() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("writes")
        .shard_count(2)
        .send()
        .await
        .unwrap();

    let first = client
        .put_record()
        .stream_name("writes")
        .partition_key("customer-1")
        .data(Blob::new(b"first"))
        .send()
        .await
        .unwrap();
    let second = client
        .put_record()
        .stream_name("writes")
        .partition_key("customer-1")
        .data(Blob::new(b"second"))
        .send()
        .await
        .unwrap();

    assert_eq!(first.shard_id(), second.shard_id());
    assert!(first.sequence_number() < second.sequence_number());
}

#[tokio::test]
async fn kinesis_put_records_reports_partial_failures() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("batch-writes")
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
        .stream_name("batch-writes")
        .records(ok_entry)
        .records(bad_entry)
        .send()
        .await
        .unwrap();

    assert_eq!(response.failed_record_count(), Some(1));
    assert_eq!(response.records().len(), 2);
    assert!(response.records()[0].sequence_number().is_some());
    assert_eq!(
        response.records()[1].error_code(),
        Some("InvalidArgumentException")
    );
}

#[tokio::test]
async fn kinesis_get_records_with_trim_horizon_iterator() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("reads")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let write_one = client
        .put_record()
        .stream_name("reads")
        .partition_key("key")
        .data(Blob::new(b"first"))
        .send()
        .await
        .unwrap();
    client
        .put_record()
        .stream_name("reads")
        .partition_key("key")
        .data(Blob::new(b"second"))
        .send()
        .await
        .unwrap();

    let iterator = client
        .get_shard_iterator()
        .stream_name("reads")
        .shard_id(write_one.shard_id())
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

    assert_eq!(records.records().len(), 2);
    assert_eq!(records.records()[0].partition_key(), "key");
    assert!(records.next_shard_iterator().is_some());
}

#[tokio::test]
async fn kinesis_latest_iterator_starts_after_existing_records() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("latest")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let first = client
        .put_record()
        .stream_name("latest")
        .partition_key("key")
        .data(Blob::new(b"before"))
        .send()
        .await
        .unwrap();

    let iterator = client
        .get_shard_iterator()
        .stream_name("latest")
        .shard_id(first.shard_id())
        .shard_iterator_type(ShardIteratorType::Latest)
        .send()
        .await
        .unwrap();

    client
        .put_record()
        .stream_name("latest")
        .partition_key("key")
        .data(Blob::new(b"after"))
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
    assert_eq!(records.records()[0].partition_key(), "key");
}

#[tokio::test]
async fn kinesis_iterator_can_be_retried_before_expiry() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("retryable")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let write = client
        .put_record()
        .stream_name("retryable")
        .partition_key("key")
        .data(Blob::new(b"payload"))
        .send()
        .await
        .unwrap();

    let iterator = client
        .get_shard_iterator()
        .stream_name("retryable")
        .shard_id(write.shard_id())
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let shard_iterator = iterator.shard_iterator().unwrap().to_string();

    let first = client
        .get_records()
        .shard_iterator(&shard_iterator)
        .limit(1)
        .send()
        .await
        .unwrap();
    let retried = client
        .get_records()
        .shard_iterator(&shard_iterator)
        .limit(1)
        .send()
        .await
        .unwrap();

    assert_eq!(first.records().len(), 1);
    assert_eq!(retried.records().len(), 1);
}

#[tokio::test]
async fn kinesis_reports_millis_behind_latest_when_limit_truncates() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("lag")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    client
        .put_record()
        .stream_name("lag")
        .partition_key("key")
        .data(Blob::new(b"one"))
        .send()
        .await
        .unwrap();
    let write = client
        .put_record()
        .stream_name("lag")
        .partition_key("key")
        .data(Blob::new(b"two"))
        .send()
        .await
        .unwrap();

    let iterator = client
        .get_shard_iterator()
        .stream_name("lag")
        .shard_id(write.shard_id())
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();

    let records = client
        .get_records()
        .shard_iterator(iterator.shard_iterator().unwrap())
        .limit(1)
        .send()
        .await
        .unwrap();

    assert_eq!(records.records().len(), 1);
    assert!(records.millis_behind_latest().unwrap_or_default() > 0);
}
