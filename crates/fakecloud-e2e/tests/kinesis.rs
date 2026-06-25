mod helpers;

use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use aws_sdk_kinesis::types::ShardIteratorType;
use helpers::TestServer;

#[tokio::test]
async fn kinesis_stream_summary_reports_warm_throughput_and_max_record_size() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("warm")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    // Before configuration these fields are absent.
    let summary = client
        .describe_stream_summary()
        .stream_name("warm")
        .send()
        .await
        .unwrap();
    let s = summary.stream_description_summary().unwrap();
    assert!(s.warm_throughput().is_none());
    assert!(s.max_record_size_in_kib().is_none());
    let stream_arn = s.stream_arn().to_string();

    client
        .update_stream_warm_throughput()
        .stream_arn(&stream_arn)
        .warm_throughput_mibps(64)
        .send()
        .await
        .unwrap();
    client
        .update_max_record_size()
        .stream_arn(&stream_arn)
        .max_record_size_in_kib(2048)
        .send()
        .await
        .unwrap();

    // DescribeStreamSummary must now report both (bug-hunt 2026-06-24, 1.13 —
    // previously omitted, so the updates never read back).
    let summary = client
        .describe_stream_summary()
        .stream_name("warm")
        .send()
        .await
        .unwrap();
    let s = summary.stream_description_summary().unwrap();
    assert_eq!(s.warm_throughput().unwrap().target_mibps(), Some(64));
    assert_eq!(s.max_record_size_in_kib(), Some(2048));
}

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

    // Per the conformance baseline recorded from real AWS, a malformed record
    // (empty PartitionKey) is reported as a per-record failure, not a
    // whole-request rejection.
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

#[tokio::test]
async fn kinesis_increase_retention_to_current_value_is_noop() {
    // Regression guard for `TestAccKinesisStream_basic`: the upstream
    // `aws_kinesis_stream` provider unconditionally calls
    // `IncreaseStreamRetentionPeriod(24)` on every create — even when
    // the configured value matches the default 24h. Real AWS treats
    // same-value as a no-op despite the docs, and fakecloud must too,
    // otherwise every basic apply fails with "must be greater".
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("retention-noop")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    // Same value as the default — must succeed.
    client
        .increase_stream_retention_period()
        .stream_name("retention-noop")
        .retention_period_hours(24)
        .send()
        .await
        .unwrap();

    // Strictly less is still a hard error.
    let err = client
        .increase_stream_retention_period()
        .stream_name("retention-noop")
        .retention_period_hours(23)
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn kinesis_update_shard_count_reshard_lineage_matches_aws() {
    use aws_sdk_kinesis::types::ScalingType;

    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("reshard")
        .shard_count(2)
        .send()
        .await
        .unwrap();

    // Scale 2 -> 3. AWS uniform scaling reshards through the shards' common
    // refinement (split every shard, then merge pieces), which leaves exactly
    // 4 closed shards — not 2. The data source asserts this count.
    client
        .update_shard_count()
        .stream_name("reshard")
        .target_shard_count(3)
        .scaling_type(ScalingType::UniformScaling)
        .send()
        .await
        .expect("update shard count");

    // The data source partitions shards by the presence of an ending sequence
    // number: closed shards have one, open shards do not. Scaling 2 -> 3 must
    // leave exactly 4 closed and 3 open.
    let all = client
        .list_shards()
        .stream_name("reshard")
        .send()
        .await
        .expect("list all shards");
    let (closed, open): (Vec<_>, Vec<_>) = all.shards().iter().partition(|s| {
        s.sequence_number_range()
            .and_then(|r| r.ending_sequence_number())
            .is_some()
    });
    assert_eq!(closed.len(), 4, "scaling 2 -> 3 must close 4 shards");
    assert_eq!(open.len(), 3, "scaling 2 -> 3 must leave 3 open shards");
}

#[tokio::test]
async fn kinesis_get_records_reports_real_millis_behind_latest() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    let put = client
        .create_stream()
        .stream_name("lag")
        .shard_count(1)
        .send()
        .await;
    assert!(put.is_ok());

    // First record, then a >1s gap, then a second record. Reading only the
    // first leaves the consumer ~1s behind the tip.
    let first = client
        .put_record()
        .stream_name("lag")
        .partition_key("k")
        .data(Blob::new(b"first"))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    client
        .put_record()
        .stream_name("lag")
        .partition_key("k")
        .data(Blob::new(b"second"))
        .send()
        .await
        .unwrap();

    let it = client
        .get_shard_iterator()
        .stream_name("lag")
        .shard_id(first.shard_id())
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let recs = client
        .get_records()
        .shard_iterator(it.shard_iterator().unwrap())
        .limit(1)
        .send()
        .await
        .unwrap();

    assert_eq!(recs.records().len(), 1);
    // The last returned record is ~1.1s behind the tip, so MillisBehindLatest
    // must reflect a real gap (>= ~1000ms), not the old 0/1 flag.
    let lag = recs.millis_behind_latest().unwrap_or(0);
    assert!(
        lag >= 1000,
        "expected real lag >= 1000ms behind tip, got {lag}"
    );

    // Reading the rest catches up: MillisBehindLatest returns to 0.
    let it2 = recs.next_shard_iterator().unwrap();
    let caught_up = client
        .get_records()
        .shard_iterator(it2)
        .send()
        .await
        .unwrap();
    assert_eq!(caught_up.millis_behind_latest(), Some(0));
}

#[tokio::test]
async fn kinesis_at_sequence_number_works_on_non_zero_shard() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("multishard")
        .shard_count(4)
        .send()
        .await
        .unwrap();

    // Put records across keys until one lands on a non-first shard, capture it.
    let mut target_shard = String::new();
    let mut target_seq = String::new();
    for i in 0..40 {
        let put = client
            .put_record()
            .stream_name("multishard")
            .partition_key(format!("key-{i}"))
            .data(Blob::new(format!("d{i}").into_bytes()))
            .send()
            .await
            .unwrap();
        if put.shard_id() != "shardId-000000000000" {
            target_shard = put.shard_id().to_string();
            target_seq = put.sequence_number().to_string();
            break;
        }
    }
    assert!(
        !target_shard.is_empty(),
        "expected a record on a non-first shard"
    );

    // The shard's advertised StartingSequenceNumber must be a usable anchor for
    // GetShardIterator(AT_SEQUENCE_NUMBER) — previously it advertised 0…01 for
    // every shard, so this 400'd on shard N>0.
    let desc = client
        .describe_stream()
        .stream_name("multishard")
        .send()
        .await
        .unwrap();
    let shard = desc
        .stream_description()
        .unwrap()
        .shards()
        .iter()
        .find(|s| s.shard_id() == target_shard)
        .unwrap();
    let advertised = shard
        .sequence_number_range()
        .unwrap()
        .starting_sequence_number();

    let it = client
        .get_shard_iterator()
        .stream_name("multishard")
        .shard_id(&target_shard)
        .shard_iterator_type(ShardIteratorType::AtSequenceNumber)
        .starting_sequence_number(advertised)
        .send()
        .await
        .expect("AT_SEQUENCE_NUMBER with the advertised StartingSequenceNumber must not 400");
    let recs = client
        .get_records()
        .shard_iterator(it.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    assert!(
        recs.records()
            .iter()
            .any(|r| r.sequence_number() == target_seq),
        "the shard's first record should be readable from its advertised start"
    );
}

#[tokio::test]
async fn kinesis_subscribe_to_shard_streams_records() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    let created = client
        .create_stream()
        .stream_name("efo")
        .shard_count(1)
        .send()
        .await;
    assert!(created.is_ok());

    let stream_arn = client
        .describe_stream_summary()
        .stream_name("efo")
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_string();

    let consumer_arn = client
        .register_stream_consumer()
        .stream_arn(&stream_arn)
        .consumer_name("efo-consumer")
        .send()
        .await
        .unwrap()
        .consumer()
        .unwrap()
        .consumer_arn()
        .to_string();

    let shard_id = client
        .describe_stream()
        .stream_name("efo")
        .send()
        .await
        .unwrap()
        .stream_description()
        .unwrap()
        .shards()[0]
        .shard_id()
        .to_string();

    client
        .put_record()
        .stream_name("efo")
        .partition_key("k")
        .data(Blob::new(b"fan-out"))
        .send()
        .await
        .unwrap();

    // SubscribeToShard must consult the registered consumer and stream the
    // records back — previously it returned ResourceNotFoundException
    // unconditionally.
    let mut resp = client
        .subscribe_to_shard()
        .consumer_arn(&consumer_arn)
        .shard_id(&shard_id)
        .starting_position(
            aws_sdk_kinesis::types::StartingPosition::builder()
                .r#type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("subscribe_to_shard");

    let mut saw_record = false;
    while let Some(event) = resp.event_stream.recv().await.expect("recv event") {
        if let aws_sdk_kinesis::types::SubscribeToShardEventStream::SubscribeToShardEvent(ev) =
            event
        {
            if ev.records().iter().any(|r| r.data().as_ref() == b"fan-out") {
                saw_record = true;
            }
        }
    }
    assert!(
        saw_record,
        "SubscribeToShard must stream the put record to the consumer"
    );
}
