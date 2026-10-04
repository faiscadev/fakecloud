//! Kinesis streams are regional resources: the same stream name exists
//! independently in every region of an account, `ListStreams` sees only the
//! request region, records never cross regions, and a stream ARN addresses
//! only the stream in the region it names.

mod helpers;

use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::ShardIteratorType;
use helpers::TestServer;

async fn kinesis_in(server: &TestServer, region: &str) -> aws_sdk_kinesis::Client {
    aws_sdk_kinesis::Client::new(&server.aws_config_in(region).await)
}

async fn create(client: &aws_sdk_kinesis::Client, name: &str) {
    client
        .create_stream()
        .stream_name(name)
        .shard_count(1)
        .send()
        .await
        .expect("CreateStream");
}

async fn list_names(client: &aws_sdk_kinesis::Client) -> Vec<String> {
    let mut names = client
        .list_streams()
        .send()
        .await
        .expect("ListStreams")
        .stream_names
        .clone();
    names.sort();
    names
}

async fn read_all(client: &aws_sdk_kinesis::Client, name: &str) -> Vec<Vec<u8>> {
    let shard = client
        .list_shards()
        .stream_name(name)
        .send()
        .await
        .expect("ListShards")
        .shards()[0]
        .shard_id()
        .to_string();
    let iterator = client
        .get_shard_iterator()
        .stream_name(name)
        .shard_id(shard)
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .expect("GetShardIterator")
        .shard_iterator
        .unwrap();
    client
        .get_records()
        .shard_iterator(iterator)
        .send()
        .await
        .expect("GetRecords")
        .records()
        .iter()
        .map(|r| r.data().as_ref().to_vec())
        .collect()
}

#[tokio::test]
async fn same_stream_name_coexists_in_two_regions() {
    let server = TestServer::start().await;
    let east = kinesis_in(&server, "us-east-1").await;
    let west = kinesis_in(&server, "eu-west-1").await;

    create(&east, "orders").await;
    create(&west, "orders").await;
    create(&west, "west-only").await;

    assert_eq!(list_names(&east).await, ["orders"]);
    assert_eq!(list_names(&west).await, ["orders", "west-only"]);

    let east_arn = east
        .describe_stream_summary()
        .stream_name("orders")
        .send()
        .await
        .unwrap()
        .stream_description_summary
        .unwrap()
        .stream_arn;
    let west_arn = west
        .describe_stream_summary()
        .stream_name("orders")
        .send()
        .await
        .unwrap()
        .stream_description_summary
        .unwrap()
        .stream_arn;
    assert_eq!(
        east_arn,
        "arn:aws:kinesis:us-east-1:123456789012:stream/orders"
    );
    assert_eq!(
        west_arn,
        "arn:aws:kinesis:eu-west-1:123456789012:stream/orders"
    );

    // Deleting one region's stream leaves the other's alone.
    west.delete_stream()
        .stream_name("orders")
        .send()
        .await
        .unwrap();
    assert_eq!(list_names(&east).await, ["orders"]);
    assert_eq!(list_names(&west).await, ["west-only"]);
}

#[tokio::test]
async fn records_stay_in_their_region() {
    let server = TestServer::start().await;
    let east = kinesis_in(&server, "us-east-1").await;
    let west = kinesis_in(&server, "eu-west-1").await;
    create(&east, "events").await;
    create(&west, "events").await;

    west.put_record()
        .stream_name("events")
        .partition_key("pk")
        .data(Blob::new(b"west-record".to_vec()))
        .send()
        .await
        .expect("PutRecord");
    east.put_record()
        .stream_name("events")
        .partition_key("pk")
        .data(Blob::new(b"east-record".to_vec()))
        .send()
        .await
        .expect("PutRecord");

    assert_eq!(read_all(&west, "events").await, [b"west-record".to_vec()]);
    assert_eq!(read_all(&east, "events").await, [b"east-record".to_vec()]);
}

#[tokio::test]
async fn stream_arn_addresses_only_its_own_region() {
    let server = TestServer::start().await;
    let east = kinesis_in(&server, "us-east-1").await;
    let west = kinesis_in(&server, "eu-west-1").await;
    create(&east, "only-east").await;

    let err = west
        .describe_stream_summary()
        .stream_arn("arn:aws:kinesis:us-east-1:123456789012:stream/only-east")
        .send()
        .await
        .expect_err("a us-east-1 stream ARN does not resolve in eu-west-1");
    assert!(
        err.into_service_error().is_resource_not_found_exception(),
        "expected ResourceNotFoundException"
    );
    let err = west
        .describe_stream_summary()
        .stream_name("only-east")
        .send()
        .await
        .expect_err("the stream does not exist in eu-west-1");
    assert!(err.into_service_error().is_resource_not_found_exception());
}
