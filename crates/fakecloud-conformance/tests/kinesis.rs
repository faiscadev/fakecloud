mod helpers;

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
