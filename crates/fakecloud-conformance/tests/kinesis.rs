mod helpers;

use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::{
    MetricsName, PutRecordsRequestEntry, ScalingType, ShardIteratorType, StreamMode,
    StreamModeDetails,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;
use serde_json::{json, Value};

#[test_action("kinesis", "CreateStream", checksum = "ec32a7c0")]
#[test_action("kinesis", "DescribeStream", checksum = "833e726c")]
#[test_action("kinesis", "DescribeStreamSummary", checksum = "d5a11feb")]
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

#[test_action("kinesis", "PutRecord", checksum = "8c678ebf")]
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

#[test_action("kinesis", "PutRecords", checksum = "d23495a3")]
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

#[test_action("kinesis", "GetShardIterator", checksum = "02801846")]
#[test_action("kinesis", "GetRecords", checksum = "ca527559")]
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

async fn stream_arn(client: &aws_sdk_kinesis::Client, stream_name: &str) -> String {
    client
        .describe_stream_summary()
        .stream_name(stream_name)
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_string()
}

#[test_action("kinesis", "TagResource", checksum = "58941b22")]
#[test_action("kinesis", "ListTagsForResource", checksum = "0eb8b1e3")]
#[test_action("kinesis", "UntagResource", checksum = "3c1bbd62")]
#[tokio::test]
async fn kinesis_tag_resource_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("tag-rsrc")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let arn = stream_arn(&client, "tag-rsrc").await;

    client
        .tag_resource()
        .resource_arn(&arn)
        .tags("env", "test")
        .send()
        .await
        .unwrap();

    let list = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(list.tags().iter().any(|t| t.key() == "env"));

    client
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let after = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(after.tags().iter().all(|t| t.key() != "env"));
}

#[test_action("kinesis", "PutResourcePolicy", checksum = "c58d3c4b")]
#[test_action("kinesis", "GetResourcePolicy", checksum = "6ef2f6c0")]
#[test_action("kinesis", "DeleteResourcePolicy", checksum = "991bbf6c")]
#[tokio::test]
async fn kinesis_resource_policy_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("rsrc-policy")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let arn = stream_arn(&client, "rsrc-policy").await;
    let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;

    client
        .put_resource_policy()
        .resource_arn(&arn)
        .policy(policy)
        .send()
        .await
        .unwrap();

    let got = client
        .get_resource_policy()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(got.policy(), policy);

    client
        .delete_resource_policy()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();

    let cleared = client
        .get_resource_policy()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(cleared.policy().is_empty());
}

#[test_action("kinesis", "StartStreamEncryption", checksum = "17a30f03")]
#[test_action("kinesis", "StopStreamEncryption", checksum = "b52d83c1")]
#[tokio::test]
async fn kinesis_stream_encryption_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("encrypted")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    client
        .start_stream_encryption()
        .stream_name("encrypted")
        .encryption_type(aws_sdk_kinesis::types::EncryptionType::Kms)
        .key_id("alias/aws/kinesis")
        .send()
        .await
        .unwrap();

    client
        .stop_stream_encryption()
        .stream_name("encrypted")
        .encryption_type(aws_sdk_kinesis::types::EncryptionType::Kms)
        .key_id("alias/aws/kinesis")
        .send()
        .await
        .unwrap();
}

#[test_action("kinesis", "EnableEnhancedMonitoring", checksum = "af33e87b")]
#[test_action("kinesis", "DisableEnhancedMonitoring", checksum = "c3324ae1")]
#[tokio::test]
async fn kinesis_enhanced_monitoring_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("metrics")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let enabled = client
        .enable_enhanced_monitoring()
        .stream_name("metrics")
        .shard_level_metrics(MetricsName::IncomingBytes)
        .shard_level_metrics(MetricsName::OutgoingBytes)
        .send()
        .await
        .unwrap();
    assert!(!enabled.desired_shard_level_metrics().is_empty());

    let disabled = client
        .disable_enhanced_monitoring()
        .stream_name("metrics")
        .shard_level_metrics(MetricsName::IncomingBytes)
        .send()
        .await
        .unwrap();
    assert!(disabled.stream_name().is_some());
}

#[test_action("kinesis", "DescribeAccountSettings", checksum = "f0cf6050")]
#[test_action("kinesis", "UpdateAccountSettings", checksum = "6176d1ee")]
#[tokio::test]
async fn kinesis_account_settings_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    let before = client.describe_account_settings().send().await.unwrap();
    assert!(before.minimum_throughput_billing_commitment().is_some());

    client
        .update_account_settings()
        .minimum_throughput_billing_commitment(
            aws_sdk_kinesis::types::MinimumThroughputBillingCommitmentInput::builder()
                .status(
                    aws_sdk_kinesis::types::MinimumThroughputBillingCommitmentInputStatus::Enabled,
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let after = client.describe_account_settings().send().await.unwrap();
    assert_eq!(
        after
            .minimum_throughput_billing_commitment()
            .map(|c| c.status().as_str()),
        Some("ENABLED")
    );
}

#[test_action("kinesis", "DescribeLimits", checksum = "55f0e704")]
#[tokio::test]
async fn kinesis_describe_limits() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    let response = client.describe_limits().send().await.unwrap();
    assert!(response.shard_limit() > 0);
}

#[test_action("kinesis", "UpdateStreamMode", checksum = "f825cfb0")]
#[tokio::test]
async fn kinesis_update_stream_mode() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("mode-change")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let arn = stream_arn(&client, "mode-change").await;

    client
        .update_stream_mode()
        .stream_arn(arn)
        .stream_mode_details(
            StreamModeDetails::builder()
                .stream_mode(StreamMode::OnDemand)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("kinesis", "UpdateStreamWarmThroughput", checksum = "cd84bfe4")]
#[tokio::test]
async fn kinesis_update_stream_warm_throughput() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("warm-tp")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let response = client
        .update_stream_warm_throughput()
        .stream_name("warm-tp")
        .warm_throughput_mibps(5)
        .send()
        .await
        .unwrap();
    assert_eq!(response.stream_name(), Some("warm-tp"));
}

#[test_action("kinesis", "UpdateMaxRecordSize", checksum = "dcb28843")]
#[tokio::test]
async fn kinesis_update_max_record_size() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("maxrec")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let arn = stream_arn(&client, "maxrec").await;

    client
        .update_max_record_size()
        .stream_arn(arn)
        .max_record_size_in_kib(2048)
        .send()
        .await
        .unwrap();
}

#[test_action("kinesis", "RegisterStreamConsumer", checksum = "1292eb2c")]
#[test_action("kinesis", "DescribeStreamConsumer", checksum = "e3945029")]
#[test_action("kinesis", "ListStreamConsumers", checksum = "874ebf29")]
#[test_action("kinesis", "DeregisterStreamConsumer", checksum = "32ff6b20")]
#[tokio::test]
async fn kinesis_stream_consumer_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("consumer-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let arn = stream_arn(&client, "consumer-stream").await;

    let registered = client
        .register_stream_consumer()
        .stream_arn(&arn)
        .consumer_name("c1")
        .send()
        .await
        .unwrap();

    let consumer_arn = registered.consumer().unwrap().consumer_arn().to_string();

    let described = client
        .describe_stream_consumer()
        .consumer_arn(&consumer_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(
        described.consumer_description().unwrap().consumer_name(),
        "c1"
    );

    let listed = client
        .list_stream_consumers()
        .stream_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(listed.consumers().iter().any(|c| c.consumer_name() == "c1"));

    client
        .deregister_stream_consumer()
        .consumer_arn(&consumer_arn)
        .send()
        .await
        .unwrap();
}

#[test_action("kinesis", "ListShards", checksum = "a453c893")]
#[tokio::test]
async fn kinesis_list_shards() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("shards-list")
        .shard_count(2)
        .send()
        .await
        .unwrap();

    let response = client
        .list_shards()
        .stream_name("shards-list")
        .send()
        .await
        .unwrap();
    assert_eq!(response.shards().len(), 2);
}

#[test_action("kinesis", "MergeShards", checksum = "482408c5")]
#[tokio::test]
async fn kinesis_merge_shards() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("merge-stream")
        .shard_count(2)
        .send()
        .await
        .unwrap();

    let shards = client
        .list_shards()
        .stream_name("merge-stream")
        .send()
        .await
        .unwrap();
    let shard0 = shards.shards()[0].shard_id().to_string();
    let shard1 = shards.shards()[1].shard_id().to_string();

    client
        .merge_shards()
        .stream_name("merge-stream")
        .shard_to_merge(shard0)
        .adjacent_shard_to_merge(shard1)
        .send()
        .await
        .unwrap();
}

#[test_action("kinesis", "SplitShard", checksum = "3888ccfb")]
#[tokio::test]
async fn kinesis_split_shard() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("split-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let shards = client
        .list_shards()
        .stream_name("split-stream")
        .send()
        .await
        .unwrap();
    let shard = &shards.shards()[0];
    let range = shard.hash_key_range().expect("hash_key_range");
    let start: u128 = range.starting_hash_key().parse().unwrap();
    let end: u128 = range.ending_hash_key().parse().unwrap();
    let mid = start + (end - start) / 2;

    client
        .split_shard()
        .stream_name("split-stream")
        .shard_to_split(shard.shard_id())
        .new_starting_hash_key(mid.to_string())
        .send()
        .await
        .unwrap();
}

#[test_action("kinesis", "UpdateShardCount", checksum = "95624a04")]
#[tokio::test]
async fn kinesis_update_shard_count() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("resize")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let response = client
        .update_shard_count()
        .stream_name("resize")
        .target_shard_count(2)
        .scaling_type(ScalingType::UniformScaling)
        .send()
        .await
        .unwrap();
    assert_eq!(response.target_shard_count(), Some(2));
}

#[test_action("kinesis", "SubscribeToShard", checksum = "15626854")]
#[tokio::test]
async fn kinesis_subscribe_to_shard_requires_registered_consumer() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("sub-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    let arn = stream_arn(&client, "sub-stream").await;
    let shards = client
        .list_shards()
        .stream_name("sub-stream")
        .send()
        .await
        .unwrap();
    let shard_id = shards.shards()[0].shard_id().to_string();

    // Fakecloud currently returns ResourceNotFoundException for SubscribeToShard
    // (no enhanced-fan-out delivery). This test wires up the SDK call path and
    // asserts the known error response so the action is exercised end-to-end.
    let fake_consumer_arn = format!("{}/consumer/none:0", arn);
    let err = client
        .subscribe_to_shard()
        .consumer_arn(fake_consumer_arn)
        .shard_id(shard_id)
        .starting_position(
            aws_sdk_kinesis::types::StartingPosition::builder()
                .r#type(ShardIteratorType::Latest)
                .build()
                .unwrap(),
        )
        .send()
        .await;
    assert!(err.is_err());
}

// ── Channels ──
//
// The pinned `aws-sdk-kinesis` in this workspace predates the channel
// operations, so there is no typed client for them; they are driven over raw
// awsJson1_1 HTTP with the `X-Amz-Target` header, the same way the acm-pca
// suite drives a service with no SDK at all.

/// A credential scope naming `region`, so the server resolves the caller's
/// region the way a real SDK client configured for it would.
fn channel_auth(region: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=test/20240101/{region}/kinesis/aws4_request, \
         SignedHeaders=host, Signature=0"
    )
}

/// POST an awsJson1_1 Kinesis action, returning `(status, parsed_body)`.
async fn channel_op(server: &TestServer, op: &str, body: Value) -> (u16, Value) {
    channel_op_in_region(server, "us-east-1", op, body).await
}

/// `channel_op` under a credential scope naming `region`.
async fn channel_op_in_region(
    server: &TestServer,
    region: &str,
    op: &str,
    body: Value,
) -> (u16, Value) {
    let resp = reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header("content-type", "application/x-amz-json-1.1")
        .header("x-amz-target", format!("Kinesis_20131202.{op}"))
        .header("authorization", channel_auth(region))
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    let status = resp.status().as_u16();
    let text = resp.text().await.unwrap();
    let parsed = serde_json::from_str(&text).unwrap_or(Value::Null);
    (status, parsed)
}

#[test_action("kinesis", "CreateChannel", checksum = "367d85db")]
#[test_action("kinesis", "DescribeChannel", checksum = "c98b8d2a")]
#[test_action("kinesis", "ListChannels", checksum = "1fbca5f2")]
#[test_action("kinesis", "UpdateChannel", checksum = "d7b1f070")]
#[test_action("kinesis", "DeleteChannel", checksum = "f9b9bf41")]
#[tokio::test]
async fn kinesis_channel_lifecycle() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("channel-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let source_arn = stream_arn(&client, "channel-stream").await;

    let (status, created) = channel_op(
        &server,
        "CreateChannel",
        json!({
            "ChannelName": "conf-channel",
            "ServiceExecutionRoleARN": "arn:aws:iam::000000000000:role/conf-channel",
            "StreamConfigurationList": [{
                "StreamARN": source_arn,
                "RecordConfiguration": { "RecordFormatType": "JSON" },
            }],
            "S3DestinationConfiguration": {
                "StorageConfiguration": {
                    "BucketARN": "arn:aws:s3:::conf-channel-bucket",
                    "ExpectedBucketOwner": "000000000000",
                    "CompressionType": "ZSTD",
                }
            },
            "Tags": { "suite": "conformance" },
        }),
    )
    .await;
    assert_eq!(status, 200, "create channel: {created}");
    let description = &created["ChannelDescription"];
    assert_eq!(description["ChannelName"], "conf-channel");
    assert_eq!(description["ChannelStatus"], "ACTIVE");
    assert_eq!(
        description["S3DestinationConfiguration"]["DataFreshnessInSeconds"],
        300
    );
    let channel_arn = description["ChannelARN"].as_str().unwrap().to_string();
    let channel_id = description["ChannelId"].as_str().unwrap();
    // AWS keys the channel ARN off the channel id, not the channel name.
    assert!(
        channel_arn.ends_with(&format!(":channel/{channel_id}")),
        "{channel_arn}"
    );

    let (status, described) = channel_op(
        &server,
        "DescribeChannel",
        json!({ "ChannelARN": channel_arn }),
    )
    .await;
    assert_eq!(status, 200, "describe channel: {described}");
    assert_eq!(
        described["ChannelDescription"]["ChannelId"],
        description["ChannelId"]
    );

    let (status, listed) = channel_op(
        &server,
        "ListChannels",
        json!({ "StreamFilter": [{ "StreamARN": source_arn }], "MaxResults": 10 }),
    )
    .await;
    assert_eq!(status, 200, "list channels: {listed}");
    let summaries = listed["ChannelSummaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 1, "{listed}");
    assert_eq!(summaries[0]["ChannelName"], "conf-channel");
    assert_eq!(summaries[0]["ChannelDestinationType"], "S3");

    let (status, updated) = channel_op(
        &server,
        "UpdateChannel",
        json!({
            "ChannelARN": channel_arn,
            "S3DestinationConfiguration": { "DataFreshnessInSeconds": 900 },
            "LoggingConfiguration": {
                "CloudWatchLogs": { "Enabled": true, "LogGroupName": "/aws/kinesis/conf" }
            },
        }),
    )
    .await;
    assert_eq!(status, 200, "update channel: {updated}");
    assert_eq!(
        updated["ChannelDescription"]["S3DestinationConfiguration"]["DataFreshnessInSeconds"],
        900
    );
    assert_eq!(
        updated["ChannelDescription"]["LoggingConfiguration"]["CloudWatchLogs"]["Enabled"],
        true
    );

    let (status, deleted) = channel_op(
        &server,
        "DeleteChannel",
        json!({ "ChannelARN": channel_arn }),
    )
    .await;
    assert_eq!(status, 200, "delete channel: {deleted}");

    let (status, missing) = channel_op(
        &server,
        "DescribeChannel",
        json!({ "ChannelARN": channel_arn }),
    )
    .await;
    assert_eq!(status, 400, "describe deleted channel: {missing}");
    assert_eq!(missing["__type"], "ResourceNotFoundException", "{missing}");
}

#[test_action("kinesis", "TagResource", checksum = "58941b22")]
#[test_action("kinesis", "ListTagsForResource", checksum = "0eb8b1e3")]
#[test_action("kinesis", "UntagResource", checksum = "3c1bbd62")]
#[tokio::test]
async fn kinesis_channel_tags() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    client
        .create_stream()
        .stream_name("channel-tag-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let source_arn = stream_arn(&client, "channel-tag-stream").await;

    let (status, created) = channel_op(
        &server,
        "CreateChannel",
        json!({
            "ChannelName": "tagged-channel",
            "ServiceExecutionRoleARN": "arn:aws:iam::000000000000:role/tagged-channel",
            "StreamConfigurationList": [{
                "StreamARN": source_arn,
                "RecordConfiguration": { "RecordFormatType": "JSON" },
            }],
            "S3DestinationConfiguration": {
                "StorageConfiguration": {
                    "BucketARN": "arn:aws:s3:::conf-channel-bucket",
                    "ExpectedBucketOwner": "000000000000",
                    "CompressionType": "ZSTD",
                }
            },
            "Tags": { "team": "data" },
        }),
    )
    .await;
    assert_eq!(status, 200, "create channel: {created}");
    let channel_arn = created["ChannelDescription"]["ChannelARN"]
        .as_str()
        .unwrap()
        .to_string();

    // The tags CreateChannel accepted are readable through Tags v2, whose
    // ResourceARN covers channels as well as streams.
    let listed = client
        .list_tags_for_resource()
        .resource_arn(&channel_arn)
        .send()
        .await
        .unwrap();
    assert!(listed.tags().iter().any(|t| t.key() == "team"));

    client
        .tag_resource()
        .resource_arn(&channel_arn)
        .tags("env", "test")
        .send()
        .await
        .unwrap();
    client
        .untag_resource()
        .resource_arn(&channel_arn)
        .tag_keys("team")
        .send()
        .await
        .unwrap();

    let after = client
        .list_tags_for_resource()
        .resource_arn(&channel_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(after.tags().len(), 1, "{:?}", after.tags());
    assert_eq!(after.tags()[0].key(), "env");

    // The source stream's own tags were never touched.
    let stream_tags = client
        .list_tags_for_resource()
        .resource_arn(&source_arn)
        .send()
        .await
        .unwrap();
    assert!(stream_tags.tags().is_empty(), "{:?}", stream_tags.tags());
}

#[test_action("kinesis", "ListChannels", checksum = "1fbca5f2")]
#[test_action("kinesis", "DeleteStream", checksum = "51c62afa")]
#[tokio::test]
async fn kinesis_channel_resolution_is_region_scoped() {
    let server = TestServer::start().await;
    let client = server.kinesis_client().await;

    // A us-east-1 stream and a same-named eu-west-1 stream: streams are
    // regional, so each region resolves only its own.
    client
        .create_stream()
        .stream_name("xregion-stream")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let east_arn = stream_arn(&client, "xregion-stream").await;
    let west_arn = east_arn.replace(":us-east-1:", ":eu-west-1:");
    assert_ne!(west_arn, east_arn, "{east_arn}");
    let (status, created) = channel_op_in_region(
        &server,
        "eu-west-1",
        "CreateStream",
        json!({ "StreamName": "xregion-stream", "ShardCount": 1 }),
    )
    .await;
    assert_eq!(status, 200, "create eu-west-1 stream: {created}");

    let channel_body = |stream: &str| {
        json!({
            "ChannelName": "xregion-channel",
            "ServiceExecutionRoleARN": "arn:aws:iam::000000000000:role/xregion-channel",
            "StreamConfigurationList": [{
                "StreamARN": stream,
                "RecordConfiguration": { "RecordFormatType": "JSON" },
            }],
            "S3DestinationConfiguration": {
                "StorageConfiguration": {
                    "BucketARN": "arn:aws:s3:::conf-channel-bucket",
                    "ExpectedBucketOwner": "000000000000",
                    "CompressionType": "ZSTD",
                }
            },
        })
    };
    // A eu-west-1 caller cannot attach the us-east-1 stream.
    let (status, rejected) = channel_op_in_region(
        &server,
        "eu-west-1",
        "CreateChannel",
        channel_body(&east_arn),
    )
    .await;
    assert_eq!(status, 400, "cross-region source: {rejected}");
    assert_eq!(
        rejected["__type"], "ResourceNotFoundException",
        "{rejected}"
    );

    let (status, created) = channel_op_in_region(
        &server,
        "eu-west-1",
        "CreateChannel",
        channel_body(&west_arn),
    )
    .await;
    assert_eq!(status, 200, "create channel: {created}");
    let channel_arn = created["ChannelDescription"]["ChannelARN"]
        .as_str()
        .unwrap()
        .to_string();

    // The channel lists in eu-west-1 under its own stream's filter only.
    let (status, listed) = channel_op_in_region(
        &server,
        "eu-west-1",
        "ListChannels",
        json!({ "StreamFilter": [{ "StreamARN": west_arn }] }),
    )
    .await;
    assert_eq!(status, 200, "list channels: {listed}");
    let summaries = listed["ChannelSummaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 1, "{listed}");
    assert_eq!(summaries[0]["ChannelName"], "xregion-channel");
    let (status, east_listed) =
        channel_op_in_region(&server, "us-east-1", "ListChannels", json!({})).await;
    assert_eq!(status, 200, "list east channels: {east_listed}");
    assert!(
        east_listed["ChannelSummaries"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{east_listed}"
    );

    // The in-use guard holds the eu-west-1 stream; the us-east-1 one is free.
    let (status, in_use) = channel_op_in_region(
        &server,
        "eu-west-1",
        "DeleteStream",
        json!({ "StreamARN": west_arn }),
    )
    .await;
    assert_eq!(status, 400, "delete attached stream: {in_use}");
    assert_eq!(in_use["__type"], "ResourceInUseException", "{in_use}");
    let (status, east_dropped) = channel_op_in_region(
        &server,
        "us-east-1",
        "DeleteStream",
        json!({ "StreamARN": east_arn }),
    )
    .await;
    assert_eq!(
        status, 200,
        "delete unattached us-east-1 stream: {east_dropped}"
    );

    let (status, deleted) = channel_op_in_region(
        &server,
        "eu-west-1",
        "DeleteChannel",
        json!({ "ChannelARN": channel_arn }),
    )
    .await;
    assert_eq!(status, 200, "delete channel: {deleted}");
    let (status, dropped) = channel_op_in_region(
        &server,
        "eu-west-1",
        "DeleteStream",
        json!({ "StreamARN": west_arn }),
    )
    .await;
    assert_eq!(status, 200, "delete detached stream: {dropped}");
}

#[test_action(
    "kinesis",
    "UpdateStreamRecordDistributionStrategy",
    checksum = "48f0bdcf"
)]
#[tokio::test]
async fn kinesis_update_stream_record_distribution_strategy() {
    let server = TestServer::start().await;

    let (status, _) = channel_op(
        &server,
        "CreateStream",
        json!({
            "StreamName": "rds-stream",
            "StreamModeDetails": { "StreamMode": "ON_DEMAND" },
        }),
    )
    .await;
    assert_eq!(status, 200);
    let (_, summary) = channel_op(
        &server,
        "DescribeStreamSummary",
        json!({ "StreamName": "rds-stream" }),
    )
    .await;
    let summary = &summary["StreamDescriptionSummary"];
    assert_eq!(summary["RecordDistributionStrategy"], "USER_PARTITION_KEY");
    let arn = summary["StreamARN"].as_str().unwrap().to_string();

    let (status, _) = channel_op(
        &server,
        "UpdateStreamRecordDistributionStrategy",
        json!({ "StreamARN": arn, "RecordDistributionStrategy": "AUTO" }),
    )
    .await;
    assert_eq!(status, 200);
    let (_, summary) = channel_op(
        &server,
        "DescribeStreamSummary",
        json!({ "StreamName": "rds-stream" }),
    )
    .await;
    assert_eq!(
        summary["StreamDescriptionSummary"]["RecordDistributionStrategy"],
        "AUTO"
    );

    // AUTO makes PartitionKey optional.
    let (status, put) = channel_op(
        &server,
        "PutRecord",
        json!({ "StreamName": "rds-stream", "Data": "aGk=" }),
    )
    .await;
    assert_eq!(status, 200, "{put}");

    let (status, err) = channel_op(
        &server,
        "UpdateStreamRecordDistributionStrategy",
        json!({ "StreamARN": arn, "RecordDistributionStrategy": "BOGUS" }),
    )
    .await;
    assert_eq!(status, 400);
    assert!(err["__type"]
        .as_str()
        .unwrap()
        .contains("ValidationException"));
}
