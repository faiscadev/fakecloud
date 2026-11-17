//! Lambda event source mappings on Apache Kafka, end to end: an Amazon MSK
//! mapping and a self-managed Kafka mapping read a topic on a fakecloud MSK
//! cluster's REAL broker container and invoke a real (Docker-backed)
//! function with Lambda's Kafka event, and the consumer group's committed
//! offsets carry over to a new mapping in the same group.
//!
//! Gated like `msk_dataplane.rs` on Docker + `FAKECLOUD_E2E_MSK=1` (the heavy
//! Kafka broker container): it runs in the dedicated `msk-broker` CI job and
//! skips loudly everywhere else.

mod helpers;

use std::collections::BTreeMap;
use std::io::Write;
use std::time::Duration;

use aws_sdk_lambda::primitives::Blob;
use aws_sdk_lambda::types::{
    AmazonManagedKafkaEventSourceConfig, EventSourcePosition, FunctionCode, Runtime,
    SelfManagedEventSource, SelfManagedKafkaEventSourceConfig,
};
use base64::Engine as _;
use helpers::TestServer;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;
use serde_json::Value;

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
    if std::env::var("FAKECLOUD_E2E_MSK").as_deref() != Ok("1") {
        eprintln!(
            "Skipping {test}: FAKECLOUD_E2E_MSK!=1 (runs in the dedicated msk-broker CI job)"
        );
        return false;
    }
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in the msk-broker CI job");
    }
    eprintln!("Skipping {test}: docker not available");
    false
}

const HANDLER: &str = r#"
def handler(event, context):
    return {"records": sum(len(v) for v in event["records"].values())}
"#;

fn handler_zip() -> Vec<u8> {
    let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    writer
        .start_file("index.py", zip::write::SimpleFileOptions::default())
        .unwrap();
    writer.write_all(HANDLER.as_bytes()).unwrap();
    writer.finish().unwrap().into_inner()
}

async fn create_function(lambda: &aws_sdk_lambda::Client, name: &str) -> String {
    lambda
        .create_function()
        .function_name(name)
        .runtime(Runtime::Python312)
        .role("arn:aws:iam::123456789012:role/lambda-role")
        .handler("index.handler")
        .code(
            FunctionCode::builder()
                .zip_file(Blob::new(handler_zip()))
                .build(),
        )
        .send()
        .await
        .expect("create function")
        .function_arn()
        .unwrap()
        .to_string()
}

async fn wait_for_active(client: &aws_sdk_kafka::Client, cluster_arn: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(300);
    loop {
        let resp = client
            .describe_cluster()
            .cluster_arn(cluster_arn)
            .send()
            .await
            .expect("describe cluster");
        let info = resp.cluster_info().expect("cluster info");
        let state = info.state().map(|s| s.as_str()).unwrap_or("");
        if state == "ACTIVE" {
            return;
        }
        assert_ne!(
            state,
            "FAILED",
            "MSK cluster failed: {:?}",
            info.state_info().and_then(|s| s.message())
        );
        assert!(
            std::time::Instant::now() < deadline,
            "cluster did not reach ACTIVE (last state: {state})"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Every Kafka record the function was invoked with, with the event it came
/// in.
async fn delivered(server: &TestServer, function_arn: &str) -> Vec<(Value, Value)> {
    let body: Value = reqwest::get(format!(
        "{}/_fakecloud/lambda/invocations",
        server.endpoint()
    ))
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    let mut out = Vec::new();
    for inv in body["invocations"].as_array().into_iter().flatten() {
        if inv["functionArn"] != function_arn {
            continue;
        }
        let event = &inv["payload"];
        for records in event["records"]
            .as_object()
            .into_iter()
            .flat_map(|m| m.values())
        {
            for r in records.as_array().into_iter().flatten() {
                out.push((event.clone(), r.clone()));
            }
        }
    }
    out
}

async fn wait_for_records(
    server: &TestServer,
    function_arn: &str,
    n: usize,
) -> Vec<(Value, Value)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        let got = delivered(server, function_arn).await;
        if got.len() >= n {
            return got;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{function_arn} got {} of {n} records: {got:?}",
            got.len()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_ok(lambda: &aws_sdk_lambda::Client, uuid: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let m = lambda
            .get_event_source_mapping()
            .uuid(uuid)
            .send()
            .await
            .unwrap();
        if m.last_processing_result() == Some("OK") {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "mapping {uuid} never reported OK: {:?}",
            m.last_processing_result()
        );
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn decode(v: &Value) -> String {
    String::from_utf8(
        base64::engine::general_purpose::STANDARD
            .decode(v.as_str().expect("base64 string"))
            .unwrap(),
    )
    .unwrap()
}

#[tokio::test]
async fn kafka_event_source_mappings_invoke_the_function_and_commit_offsets() {
    if !require_docker_or_skip("kafka_event_source_mappings_invoke_the_function_and_commit_offsets")
    {
        return;
    }
    let server = TestServer::start().await;
    let config = server.aws_config().await;
    let kafka = aws_sdk_kafka::Client::new(&config);
    let lambda = aws_sdk_lambda::Client::new(&config);

    let cluster_arn = kafka
        .create_cluster()
        .cluster_name("fc-lambda-esm")
        .kafka_version("3.6.0")
        .number_of_broker_nodes(1)
        .broker_node_group_info(
            aws_sdk_kafka::types::BrokerNodeGroupInfo::builder()
                .instance_type("kafka.m5.large")
                .client_subnets("subnet-0123456789abcdef0")
                .build(),
        )
        .send()
        .await
        .expect("create cluster")
        .cluster_arn()
        .unwrap()
        .to_string();
    wait_for_active(&kafka, &cluster_arn).await;
    let bootstrap = kafka
        .get_bootstrap_brokers()
        .cluster_arn(&cluster_arn)
        .send()
        .await
        .expect("bootstrap brokers")
        .bootstrap_broker_string()
        .expect("bootstrap broker string")
        .to_string();
    let topic = "fc.esm.orders";
    kafka
        .create_topic()
        .cluster_arn(&cluster_arn)
        .topic_name(topic)
        .partition_count(2)
        .replication_factor(1)
        .send()
        .await
        .expect("create topic");

    let msk_fn = create_function(&lambda, "kafka-msk-consumer").await;
    let self_fn = create_function(&lambda, "kafka-self-managed-consumer").await;
    let resumed_fn = create_function(&lambda, "kafka-resumed-consumer").await;

    let msk_mapping = lambda
        .create_event_source_mapping()
        .function_name("kafka-msk-consumer")
        .event_source_arn(&cluster_arn)
        .topics(topic)
        .starting_position(EventSourcePosition::TrimHorizon)
        .batch_size(10)
        .maximum_batching_window_in_seconds(1)
        .send()
        .await
        .expect("create MSK mapping");
    let msk_uuid = msk_mapping.uuid().unwrap().to_string();
    // The group defaults to the mapping's UUID.
    assert_eq!(
        msk_mapping
            .amazon_managed_kafka_event_source_config()
            .and_then(|c| c.consumer_group_id()),
        Some(msk_uuid.as_str())
    );
    let self_mapping = lambda
        .create_event_source_mapping()
        .function_name("kafka-self-managed-consumer")
        .self_managed_event_source(
            SelfManagedEventSource::builder()
                .endpoints(
                    aws_sdk_lambda::types::EndPointType::KafkaBootstrapServers,
                    vec![bootstrap.clone()],
                )
                .build(),
        )
        .self_managed_kafka_event_source_config(
            SelfManagedKafkaEventSourceConfig::builder()
                .consumer_group_id("fc-self-managed")
                .build(),
        )
        .topics(topic)
        .starting_position(EventSourcePosition::TrimHorizon)
        .send()
        .await
        .expect("create self-managed mapping");
    let self_uuid = self_mapping.uuid().unwrap().to_string();

    // Produce three records to partition 0.
    let producer = ClientBuilder::new(vec![bootstrap.clone()])
        .build()
        .await
        .expect("connect to the broker");
    let partition = producer
        .partition_client(topic.to_string(), 0, UnknownTopicHandling::Retry)
        .await
        .expect("partition client");
    let record = |i: usize| Record {
        key: Some(format!("k{i}").into_bytes()),
        value: Some(format!("{{\"n\":{i}}}").into_bytes()),
        headers: BTreeMap::from([("trace".to_string(), b"abc".to_vec())]),
        timestamp: chrono::Utc::now(),
    };
    partition
        .produce((0..3).map(record).collect(), Compression::NoCompression)
        .await
        .expect("produce");

    // The MSK mapping delivers Lambda's aws:kafka event.
    let got = wait_for_records(&server, &msk_fn, 3).await;
    let (event, _) = &got[0];
    assert_eq!(event["eventSource"], "aws:kafka");
    assert_eq!(event["eventSourceArn"], cluster_arn.as_str());
    assert_eq!(event["bootstrapServers"], bootstrap.as_str());
    let mut offsets: Vec<i64> = got
        .iter()
        .map(|(_, r)| r["offset"].as_i64().unwrap())
        .collect();
    offsets.sort();
    assert_eq!(offsets, vec![0, 1, 2]);
    for (event, r) in &got {
        assert!(event["records"][format!("{topic}-0")].is_array());
        assert_eq!(r["topic"], topic);
        assert_eq!(r["partition"], 0);
        assert_eq!(r["timestampType"], "CREATE_TIME");
        assert!(r["timestamp"].as_i64().unwrap() > 0);
        let n = r["offset"].as_i64().unwrap();
        assert_eq!(decode(&r["key"]), format!("k{n}"));
        assert_eq!(decode(&r["value"]), format!("{{\"n\":{n}}}"));
        assert_eq!(r["headers"], serde_json::json!([{ "trace": [97, 98, 99] }]));
    }
    wait_for_ok(&lambda, &msk_uuid).await;

    // The self-managed mapping reads the same records in its own group.
    let got = wait_for_records(&server, &self_fn, 3).await;
    assert_eq!(got[0].0["eventSource"], "SelfManagedKafka");
    assert!(got[0].0.get("eventSourceArn").is_none());
    assert_eq!(got[0].0["bootstrapServers"], bootstrap.as_str());
    wait_for_ok(&lambda, &self_uuid).await;

    // A new mapping in the MSK mapping's group resumes after the committed
    // offsets: it gets only the record produced after them.
    lambda
        .delete_event_source_mapping()
        .uuid(&msk_uuid)
        .send()
        .await
        .expect("delete MSK mapping");
    lambda
        .create_event_source_mapping()
        .function_name("kafka-resumed-consumer")
        .event_source_arn(&cluster_arn)
        .topics(topic)
        .starting_position(EventSourcePosition::TrimHorizon)
        .amazon_managed_kafka_event_source_config(
            AmazonManagedKafkaEventSourceConfig::builder()
                .consumer_group_id(&msk_uuid)
                .build(),
        )
        .send()
        .await
        .expect("create resumed mapping");
    partition
        .produce(vec![record(3)], Compression::NoCompression)
        .await
        .expect("produce");
    let got = wait_for_records(&server, &resumed_fn, 1).await;
    // Give a wrongly rewound consumer time to redeliver the old records.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let got_after = delivered(&server, &resumed_fn).await;
    assert_eq!(got.len(), 1);
    let offsets: Vec<i64> = got_after
        .iter()
        .map(|(_, r)| r["offset"].as_i64().unwrap())
        .collect();
    assert_eq!(
        offsets,
        vec![3],
        "the group resumes after its committed offsets"
    );

    kafka
        .delete_cluster()
        .cluster_arn(&cluster_arn)
        .send()
        .await
        .expect("delete cluster");
}
