mod helpers;

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
