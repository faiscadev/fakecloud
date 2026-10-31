//! End-to-end coverage for the DynamoDB Streams data plane
//! (`streams.dynamodb.<region>.amazonaws.com`) and the
//! DynamoDB-Streams -> Lambda event-source-mapping bridge.
//!
//! These tests exercise the same flow a user would run with the real
//! AWS SDKs:
//!  1. CreateTable with `StreamSpecification.StreamEnabled=true`,
//!     DescribeTable returns `LatestStreamArn`/`LatestStreamLabel`.
//!  2. ListStreams / DescribeStream / GetShardIterator / GetRecords
//!     read INSERT, MODIFY, REMOVE records back via the dedicated
//!     `DynamoDBStreams_20120810.*` JSON-1.0 service.
//!  3. A Lambda EventSourceMapping pointing at the stream ARN delivers
//!     the records to a real container-runtime invocation.

mod helpers;

use std::io::Write;
use std::time::Duration;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType,
};
use aws_sdk_dynamodbstreams::types::ShardIteratorType;
use helpers::TestServer;

fn make_zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    for (name, content) in entries {
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
        writer.start_file(*name, options).unwrap();
        writer.write_all(content).unwrap();
    }
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

async fn get_lambda_invocations(endpoint: &str) -> serde_json::Value {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{endpoint}/_fakecloud/lambda/invocations"))
        .send()
        .await
        .unwrap();
    resp.json::<serde_json::Value>().await.unwrap()
}

async fn create_streamed_table(
    ddb: &aws_sdk_dynamodb::Client,
    name: &str,
) -> aws_sdk_dynamodb::types::TableDescription {
    ddb.create_table()
        .table_name(name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    ddb.describe_table()
        .table_name(name)
        .send()
        .await
        .unwrap()
        .table()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn create_table_surfaces_latest_stream_arn() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;

    let table = create_streamed_table(&ddb, "StreamedTable").await;
    let arn = table
        .latest_stream_arn()
        .expect("LatestStreamArn must be set when StreamSpecification.StreamEnabled=true");
    assert!(
        arn.contains(":table/StreamedTable/stream/"),
        "expected real stream-ARN format, got {arn}"
    );
    let label = table.latest_stream_label().expect("LatestStreamLabel");
    assert_eq!(arn.rsplit('/').next().unwrap(), label);
}

#[tokio::test]
async fn list_describe_get_records_round_trip_insert_modify_remove() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let streams = server.dynamodb_streams_client().await;

    let table = create_streamed_table(&ddb, "StreamRoundTrip").await;
    let stream_arn = table.latest_stream_arn().unwrap().to_string();

    // ListStreams returns the table's current stream.
    let list = streams.list_streams().send().await.unwrap();
    let s = list.streams();
    assert!(
        s.iter().any(|s| s.stream_arn() == Some(&stream_arn)),
        "ListStreams should include {stream_arn}, got {s:?}",
    );

    // DescribeStream returns ENABLED + a single shard.
    let desc = streams
        .describe_stream()
        .stream_arn(&stream_arn)
        .send()
        .await
        .unwrap();
    let sd = desc.stream_description().unwrap();
    assert_eq!(sd.stream_status().unwrap().as_str(), "ENABLED");
    let shards = sd.shards();
    assert_eq!(shards.len(), 1, "single-shard emulation");
    let shard_id = shards[0].shard_id().unwrap().to_string();

    // INSERT, MODIFY, REMOVE — each mutation appends one stream record.
    ddb.put_item()
        .table_name("StreamRoundTrip")
        .item("pk", AttributeValue::S("k1".into()))
        .item("v", AttributeValue::N("1".into()))
        .send()
        .await
        .unwrap();
    ddb.put_item()
        .table_name("StreamRoundTrip")
        .item("pk", AttributeValue::S("k1".into()))
        .item("v", AttributeValue::N("2".into()))
        .send()
        .await
        .unwrap();
    ddb.delete_item()
        .table_name("StreamRoundTrip")
        .key("pk", AttributeValue::S("k1".into()))
        .send()
        .await
        .unwrap();

    // TRIM_HORIZON iterator -> reads everything that's been written.
    let it = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let iterator = it.shard_iterator().unwrap().to_string();

    let records_resp = streams
        .get_records()
        .shard_iterator(&iterator)
        .send()
        .await
        .unwrap();
    let records = records_resp.records();
    assert_eq!(records.len(), 3, "INSERT + MODIFY + REMOVE");
    assert_eq!(records[0].event_name().unwrap().as_str(), "INSERT");
    assert_eq!(records[1].event_name().unwrap().as_str(), "MODIFY");
    assert_eq!(records[2].event_name().unwrap().as_str(), "REMOVE");
    // Records carry NEW/OLD images per NEW_AND_OLD_IMAGES view type.
    assert!(records[0]
        .dynamodb()
        .unwrap()
        .new_image()
        .expect("INSERT NewImage")
        .contains_key("pk"));
    assert!(records[1]
        .dynamodb()
        .unwrap()
        .old_image()
        .expect("MODIFY OldImage")
        .contains_key("pk"));
    assert!(records[2]
        .dynamodb()
        .unwrap()
        .old_image()
        .expect("REMOVE OldImage")
        .contains_key("pk"));
    // SequenceNumbers are monotonic strings.
    let seqs: Vec<&str> = records
        .iter()
        .map(|r| r.dynamodb().unwrap().sequence_number().unwrap())
        .collect();
    assert!(seqs[0] < seqs[1]);
    assert!(seqs[1] < seqs[2]);
    assert!(records_resp.next_shard_iterator().is_some());

    // AFTER_SEQUENCE_NUMBER skips the previously-consumed record.
    let after = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(ShardIteratorType::AfterSequenceNumber)
        .sequence_number(seqs[0])
        .send()
        .await
        .unwrap();
    let after_records = streams
        .get_records()
        .shard_iterator(after.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(after_records.records().len(), 2);
    assert_eq!(
        after_records.records()[0].event_name().unwrap().as_str(),
        "MODIFY",
    );

    // LATEST iterator yields nothing until a fresh write.
    let latest = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(ShardIteratorType::Latest)
        .send()
        .await
        .unwrap();
    let latest_iterator = latest.shard_iterator().unwrap().to_string();
    let empty = streams
        .get_records()
        .shard_iterator(&latest_iterator)
        .send()
        .await
        .unwrap();
    assert!(empty.records().is_empty());

    ddb.put_item()
        .table_name("StreamRoundTrip")
        .item("pk", AttributeValue::S("k2".into()))
        .send()
        .await
        .unwrap();
    let after_latest = streams
        .get_records()
        .shard_iterator(&latest_iterator)
        .send()
        .await
        .unwrap();
    assert_eq!(after_latest.records().len(), 1);
    assert_eq!(
        after_latest.records()[0].event_name().unwrap().as_str(),
        "INSERT"
    );
}

/// DynamoDB Streams -> Lambda EventSourceMapping. Same shape as the
/// Kinesis -> Lambda cross-service test: create the function, point an
/// ESM at the stream ARN, mutate the table, then poll
/// `/_fakecloud/lambda/invocations` for an `aws:dynamodb` invocation.
#[tokio::test]
async fn dynamodb_streams_lambda_event_source_mapping() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let lambda = server.lambda_client().await;

    let table = create_streamed_table(&ddb, "StreamedEsmTable").await;
    let stream_arn = table.latest_stream_arn().unwrap().to_string();

    lambda
        .create_function()
        .function_name("ddb-stream-processor")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(aws_sdk_lambda::primitives::Blob::new(make_zip(&[(
                    "index.py",
                    br#"def handler(event, context):
    return {"statusCode": 200}
"#,
                )])))
                .build(),
        )
        .send()
        .await
        .unwrap();

    lambda
        .create_event_source_mapping()
        .event_source_arn(&stream_arn)
        .function_name("ddb-stream-processor")
        .batch_size(10)
        .starting_position(aws_sdk_lambda::types::EventSourcePosition::TrimHorizon)
        .enabled(true)
        .send()
        .await
        .unwrap();

    ddb.put_item()
        .table_name("StreamedEsmTable")
        .item("pk", AttributeValue::S("hello".into()))
        .item("v", AttributeValue::N("42".into()))
        .send()
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let inv = loop {
        let invs = get_lambda_invocations(server.endpoint()).await;
        let inv_list = invs["invocations"].as_array().cloned().unwrap_or_default();
        if let Some(inv) = inv_list
            .iter()
            .rev()
            .find(|inv| inv["payload"]["Records"][0]["eventSource"] == "aws:dynamodb")
        {
            break inv.clone();
        }
        if std::time::Instant::now() >= deadline {
            panic!("expected an aws:dynamodb-shaped Lambda invocation within 20s");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    assert!(inv["functionArn"]
        .as_str()
        .unwrap()
        .contains("ddb-stream-processor"));
    let payload = &inv["payload"];
    let record = &payload["Records"][0];
    assert_eq!(record["eventSource"], "aws:dynamodb");
    assert_eq!(record["eventName"], "INSERT");
    assert_eq!(record["eventSourceARN"], stream_arn);
    assert_eq!(record["dynamodb"]["Keys"]["pk"]["S"], "hello");
    assert_eq!(record["dynamodb"]["NewImage"]["v"]["N"], "42");
}
