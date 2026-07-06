//! Amazon MSK (Kafka) data-plane E2E: proves a fakecloud MSK cluster is a REAL,
//! connectable Apache Kafka broker, not a formatted-but-dead
//! `*.amazonaws.com:9092` bootstrap string.
//!
//! The test creates a cluster, waits for it to reach `ACTIVE` (which only
//! happens once the backing `apache/kafka:3.8.0` container actually serves),
//! asks `GetBootstrapBrokers` for the real `host:port`, creates a topic through
//! the MSK API (which drives `kafka-topics.sh` on the live broker), then
//! PRODUCES and CONSUMES a real message through that bootstrap endpoint with a
//! genuine pure-Rust Kafka client (`rskafka`). A message round trip is the only thing that
//! proves the data plane works. Finally it confirms `ListTopics` /
//! `DescribeTopicPartitions` reflect the REAL broker's topic + partition count.
//!
//! Gated on Docker + `FAKECLOUD_E2E_MSK=1` (the heavy Kafka container). It runs
//! ONLY in the dedicated, resourced `msk-broker` CI job; in the shared partition
//! the flag is unset and it skips loudly. In that CI job a missing Docker
//! hard-fails rather than silently skipping.

mod helpers;

use std::time::Duration;

use helpers::TestServer;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::record::Record;

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
    // The real Kafka broker container is heavy (a JVM broker in KRaft mode) and
    // strains the shared E2E partition's runner, so this suite runs ONLY in the
    // dedicated, resourced `msk-broker` CI job, which sets FAKECLOUD_E2E_MSK=1.
    // In the shared partition the flag is unset and we skip loudly (by design),
    // rather than hard-failing an environment that was never meant to spawn a
    // Kafka broker.
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

async fn kafka_client(server: &TestServer) -> aws_sdk_kafka::Client {
    aws_sdk_kafka::Client::new(&server.aws_config().await)
}

/// Poll DescribeCluster until the cluster reaches `ACTIVE` (its backing broker
/// serves). Fails loudly on `FAILED`, dumping the recorded `stateInfo`.
async fn wait_for_active(client: &aws_sdk_kafka::Client, cluster_arn: &str, timeout_secs: u64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);
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
        if state == "FAILED" {
            let reason = info
                .state_info()
                .and_then(|si| si.message())
                .unwrap_or("<no stateInfo>");
            panic!("MSK cluster reached FAILED (broker could not come up): {reason}");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cluster {cluster_arn} did not reach ACTIVE within {timeout_secs}s (last state: {state})"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[tokio::test]
async fn msk_cluster_delivers_a_message_through_a_real_kafka_broker() {
    if !require_docker_or_skip("msk_cluster_delivers_a_message_through_a_real_kafka_broker") {
        return;
    }

    let server = TestServer::start().await;
    let client = kafka_client(&server).await;

    // 1. Create a provisioned cluster. The backing Kafka container spins up in
    //    the background (image + KRaft boot), so allow a generous window.
    let created = client
        .create_cluster()
        .cluster_name("fc-msk-dataplane")
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
        .expect("create cluster");
    let cluster_arn = created.cluster_arn().expect("cluster arn").to_string();

    wait_for_active(&client, &cluster_arn, 300).await;

    // 2. GetBootstrapBrokers must return a REAL reachable host:port, not
    //    *.amazonaws.com.
    let bb = client
        .get_bootstrap_brokers()
        .cluster_arn(&cluster_arn)
        .send()
        .await
        .expect("get bootstrap brokers");
    let bootstrap = bb
        .bootstrap_broker_string()
        .expect("bootstrap broker string")
        .to_string();
    assert!(
        !bootstrap.contains("amazonaws.com"),
        "bootstrap must be a real mapped port, got {bootstrap}"
    );
    assert!(
        bootstrap.contains(':'),
        "bootstrap must be host:port, got {bootstrap}"
    );

    // 3. Create a topic through the MSK API (drives kafka-topics.sh on the live
    //    broker).
    let topic = "fc.dataplane.orders";
    client
        .create_topic()
        .cluster_arn(&cluster_arn)
        .topic_name(topic)
        .partition_count(3)
        .replication_factor(1)
        .send()
        .await
        .expect("create topic");

    // 4. Produce and consume a real message through the broker at the bootstrap
    //    endpoint with a pure-Rust Kafka client (rskafka). This round trip is the
    //    proof the data plane actually works.
    let body = b"HELLO_FROM_FAKECLOUD_MSK".to_vec();
    let kafka = ClientBuilder::new(vec![bootstrap.clone()])
        .build()
        .await
        .expect("connect to the live kafka broker");
    let partition = kafka
        .partition_client(topic.to_string(), 0, UnknownTopicHandling::Retry)
        .await
        .expect("partition client");

    let record = Record {
        key: Some(b"k0".to_vec()),
        value: Some(body.clone()),
        headers: std::collections::BTreeMap::new(),
        timestamp: chrono::Utc::now(),
    };
    partition
        .produce(vec![record], Compression::NoCompression)
        .await
        .expect("produce message to the live broker");

    // Fetch from offset 0 (the message we just produced), waiting up to 30s.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let consumed = loop {
        let (records, _high_watermark) = partition
            .fetch_records(0, 1..1_000_000, 30_000)
            .await
            .expect("fetch from the live broker");
        if let Some(first) = records.into_iter().next() {
            break first.record.value.expect("record value");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out awaiting the produced message from the live broker"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(
        consumed, body,
        "the consumed message must equal the produced one"
    );

    // 5. ListTopics / DescribeTopicPartitions must reflect the REAL broker's
    //    topic + partition count (3 partitions on the live broker).
    let topics = client
        .list_topics()
        .cluster_arn(&cluster_arn)
        .send()
        .await
        .expect("list topics");
    let listed = topics
        .topics()
        .iter()
        .find(|t| t.topic_name() == Some(topic))
        .expect("the created topic must appear in ListTopics from the real broker");
    assert_eq!(
        listed.partition_count(),
        Some(3),
        "ListTopics must reflect the real broker partition count"
    );

    let parts = client
        .describe_topic_partitions()
        .cluster_arn(&cluster_arn)
        .topic_name(topic)
        .send()
        .await
        .expect("describe topic partitions");
    assert_eq!(
        parts.partitions().len(),
        3,
        "DescribeTopicPartitions must reflect the real broker's partitions"
    );
    // Every partition on the single-node broker is led by broker 1.
    for p in parts.partitions() {
        assert_eq!(
            p.leader(),
            Some(1),
            "the single-node broker leads every partition (broker id 1)"
        );
    }

    // Deleting the cluster tears the real container down.
    client
        .delete_cluster()
        .cluster_arn(&cluster_arn)
        .send()
        .await
        .expect("delete cluster");
}
