//! Stream event source mappings (Kinesis, DynamoDB Streams) against
//! functions that fail: retries, bisection, partial-batch responses and the
//! OnFailure destination, as recorded on AWS
//! (fakecloud-lambda/tests/fixtures/stream-esm-failures/aws-recordings.json).
//! Real Docker-backed functions.

mod helpers;

use std::io::Write;
use std::time::Duration;

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{
    DestinationConfig, EventSourcePosition, FunctionCode, FunctionResponseType, OnFailure, Runtime,
};
use helpers::TestServer;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("skipping {test}: docker is not available");
    false
}

/// Raises when any record's id contains "poison" (recording handler
/// `poison`); with `partial`, reports the poison records as batch item
/// failures instead (handler `partial`).
const HANDLER: &str = r#"
import base64, json, os

def ids(event):
    out = []
    for r in event["Records"]:
        if "kinesis" in r:
            out.append((r["kinesis"]["sequenceNumber"], json.loads(base64.b64decode(r["kinesis"]["data"]))["id"]))
        else:
            out.append((r["dynamodb"]["SequenceNumber"], r["dynamodb"]["Keys"]["pk"]["S"]))
    return out

def handler(event, context):
    records = ids(event)
    if os.environ.get("MODE") == "partial":
        return {"batchItemFailures": [{"itemIdentifier": seq} for seq, rid in records if "poison" in rid]}
    if any("poison" in rid for _, rid in records):
        raise ValueError("poison")
    return {}
"#;

fn handler_zip() -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("index.py", zip::write::SimpleFileOptions::default())
        .unwrap();
    writer.write_all(HANDLER.as_bytes()).unwrap();
    writer.finish().unwrap().into_inner()
}

async fn create_function(server: &TestServer, name: &str, mode: &str) -> String {
    let lambda = server.lambda_client().await;
    lambda
        .create_function()
        .function_name(name)
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .environment(
            aws_sdk_lambda::types::Environment::builder()
                .variables("MODE", mode)
                .build(),
        )
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(handler_zip()))
                .build(),
        )
        .send()
        .await
        .unwrap()
        .function_arn()
        .unwrap()
        .to_string()
}

async fn queue(server: &TestServer, name: &str) -> (String, String) {
    let sqs = server.sqs_client().await;
    let url = sqs
        .create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url()
        .unwrap()
        .to_string();
    let arn = format!("arn:aws:sqs:us-east-1:123456789012:{name}");
    (url, arn)
}

/// Wait for one OnFailure record on the queue.
async fn on_failure_record(server: &TestServer, url: &str) -> serde_json::Value {
    let sqs = server.sqs_client().await;
    for _ in 0..90 {
        let out = sqs
            .receive_message()
            .queue_url(url)
            .wait_time_seconds(1)
            .send()
            .await
            .unwrap();
        if let Some(m) = out.messages().first() {
            return serde_json::from_str(m.body().unwrap()).unwrap();
        }
    }
    panic!("no OnFailure record arrived");
}

/// Every batch the function was invoked with, as record ids.
async fn batches(server: &TestServer, function_arn: &str) -> Vec<Vec<String>> {
    let body: serde_json::Value = reqwest::get(format!(
        "{}/_fakecloud/lambda/invocations",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    body["invocations"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|inv| inv["functionArn"] == function_arn)
        .map(|inv| {
            inv["payload"]["Records"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|r| {
                    if let Some(data) = r["kinesis"]["data"].as_str() {
                        use base64::Engine;
                        let raw = base64::engine::general_purpose::STANDARD
                            .decode(data)
                            .unwrap();
                        serde_json::from_slice::<serde_json::Value>(&raw).unwrap()["id"]
                            .as_str()
                            .unwrap()
                            .to_string()
                    } else {
                        r["dynamodb"]["Keys"]["pk"]["S"]
                            .as_str()
                            .unwrap()
                            .to_string()
                    }
                })
                .collect()
        })
        .collect()
}

fn ids(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// Recorded scenario D: a failing Kinesis batch is bisected until the
/// poison record is alone; it uses its two retries, goes to the OnFailure
/// destination, and the record after it is still delivered.
#[tokio::test]
async fn kinesis_bisects_a_failing_batch_and_discards_the_poison_record() {
    if !require_docker_or_skip("kinesis_bisects_a_failing_batch_and_discards_the_poison_record") {
        return;
    }
    let server = TestServer::start().await;
    let kinesis = server.kinesis_client().await;
    kinesis
        .create_stream()
        .stream_name("esm-bisect")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/esm-bisect";
    let function_arn = create_function(&server, "esm-bisect-fn", "raise").await;
    let (queue_url, queue_arn) = queue(&server, "esm-bisect-dest").await;
    let lambda = server.lambda_client().await;
    lambda
        .create_event_source_mapping()
        .function_name("esm-bisect-fn")
        .event_source_arn(stream_arn)
        .starting_position(EventSourcePosition::TrimHorizon)
        .batch_size(10)
        .maximum_retry_attempts(2)
        .bisect_batch_on_function_error(true)
        .destination_config(
            DestinationConfig::builder()
                .on_failure(OnFailure::builder().destination(&queue_arn).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    let mut seqs = Vec::new();
    for id in ["d-1", "d-2", "d-poison-3", "d-4"] {
        let out = kinesis
            .put_record()
            .stream_name("esm-bisect")
            .partition_key("pk")
            .data(aws_sdk_kinesis::primitives::Blob::new(
                serde_json::json!({ "id": id }).to_string(),
            ))
            .send()
            .await
            .unwrap();
        seqs.push(out.sequence_number().to_string());
    }

    let record = on_failure_record(&server, &queue_url).await;
    assert_eq!(
        record["requestContext"]["condition"],
        "RetryAttemptsExhausted"
    );
    assert_eq!(record["requestContext"]["approximateInvokeCount"], 3);
    assert_eq!(record["requestContext"]["functionArn"], function_arn);
    assert_eq!(record["responseContext"]["functionError"], "Unhandled");
    let info = &record["KinesisBatchInfo"];
    assert_eq!(info["batchSize"], 1);
    assert_eq!(info["startSequenceNumber"], seqs[2]);
    assert_eq!(info["endSequenceNumber"], seqs[2]);
    assert_eq!(info["streamArn"], stream_arn);
    assert_eq!(info["shardId"], "shardId-000000000000");

    let mut seen = Vec::new();
    for _ in 0..30 {
        seen = batches(&server, &function_arn).await;
        if seen.len() >= 7 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(
        seen,
        vec![
            ids(&["d-1", "d-2", "d-poison-3", "d-4"]),
            ids(&["d-1", "d-2"]),
            ids(&["d-poison-3", "d-4"]),
            ids(&["d-poison-3"]),
            ids(&["d-poison-3"]),
            ids(&["d-poison-3"]),
            ids(&["d-4"]),
        ]
    );
}

/// Recorded scenario F: a DynamoDB Streams mapping with
/// ReportBatchItemFailures checkpoints past the handled record, retries the
/// failed one, then sends it to the OnFailure destination.
#[tokio::test]
async fn dynamodb_streams_partial_batch_failure_retries_then_discards() {
    if !require_docker_or_skip("dynamodb_streams_partial_batch_failure_retries_then_discards") {
        return;
    }
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = ddb
        .create_table()
        .table_name("esm-partial")
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .stream_specification(
            aws_sdk_dynamodb::types::StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(aws_sdk_dynamodb::types::StreamViewType::KeysOnly)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let stream_arn = table
        .table_description()
        .unwrap()
        .latest_stream_arn()
        .unwrap()
        .to_string();
    let function_arn = create_function(&server, "esm-partial-fn", "partial").await;
    let (queue_url, queue_arn) = queue(&server, "esm-partial-dest").await;
    let lambda = server.lambda_client().await;
    lambda
        .create_event_source_mapping()
        .function_name("esm-partial-fn")
        .event_source_arn(&stream_arn)
        .starting_position(EventSourcePosition::TrimHorizon)
        .batch_size(10)
        .maximum_retry_attempts(1)
        .function_response_types(FunctionResponseType::ReportBatchItemFailures)
        .destination_config(
            DestinationConfig::builder()
                .on_failure(OnFailure::builder().destination(&queue_arn).build())
                .build(),
        )
        .send()
        .await
        .unwrap();
    for pk in ["f-1", "f-poison-2"] {
        ddb.put_item()
            .table_name("esm-partial")
            .item("pk", aws_sdk_dynamodb::types::AttributeValue::S(pk.into()))
            .send()
            .await
            .unwrap();
    }

    let record = on_failure_record(&server, &queue_url).await;
    assert_eq!(
        record["requestContext"]["condition"],
        "RetryAttemptsExhausted"
    );
    assert_eq!(record["requestContext"]["approximateInvokeCount"], 2);
    assert!(record["responseContext"]["functionError"].is_null());
    let info = &record["DDBStreamBatchInfo"];
    assert_eq!(info["batchSize"], 1);
    assert_eq!(info["streamArn"], stream_arn);
    assert_eq!(info["startSequenceNumber"], info["endSequenceNumber"]);
    assert_eq!(
        batches(&server, &function_arn).await,
        vec![
            ids(&["f-1", "f-poison-2"]),
            ids(&["f-poison-2"]),
            ids(&["f-poison-2"]),
        ]
    );
}
