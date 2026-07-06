//! Amazon MSK (Managed Streaming for Apache Kafka) control-plane E2E.
//!
//! Exercises the full create -> describe (settles ACTIVE) -> list -> configure
//! -> tag -> update (records a cluster operation) -> delete lifecycle against a
//! spawned fakecloud server via the AWS Rust SDK, which speaks the real MSK
//! restJson1 wire format (camelCase `jsonName` bodies, ARN path labels).
//!
//! This runs in the shared partition with `FAKECLOUD_KAFKA_DISABLE_BACKEND=1`,
//! so it is a pure CONTROL-PLANE test: no real Kafka container is spawned and
//! the lifecycle auto-settles in memory (CREATING -> ACTIVE on the first
//! describe). The real Docker-backed broker + produce/consume round trip lives
//! in `msk_dataplane.rs`, which runs ONLY in the dedicated `msk-broker` CI job.
//! Without the flag, a docker-capable shared runner would attach the live
//! runtime and spawn a heavy Kafka broker here (and the cluster would not be
//! instantly ACTIVE) -- exactly what the dedicated job exists to isolate.

mod helpers;

use aws_sdk_kafka::primitives::Blob;
use aws_sdk_kafka::types::BrokerNodeGroupInfo;
use helpers::TestServer;

async fn kafka_client(server: &TestServer) -> aws_sdk_kafka::Client {
    aws_sdk_kafka::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn cluster_lifecycle_create_describe_list_tag_update_delete() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_KAFKA_DISABLE_BACKEND", "1")]).await;
    let kafka = kafka_client(&server).await;

    let bngi = BrokerNodeGroupInfo::builder()
        .client_subnets("subnet-0123456789abcdef0")
        .client_subnets("subnet-0123456789abcdef1")
        .instance_type("kafka.m5.large")
        .build();

    // CreateCluster returns CREATING.
    let created = kafka
        .create_cluster()
        .cluster_name("e2e-cluster")
        .kafka_version("3.6.0")
        .number_of_broker_nodes(3)
        .broker_node_group_info(bngi)
        .send()
        .await
        .expect("create cluster");
    let arn = created.cluster_arn().expect("cluster arn").to_string();
    assert!(
        arn.contains(":cluster/e2e-cluster/"),
        "MSK cluster ARN: {arn}"
    );
    assert_eq!(created.state().map(|s| s.as_str()), Some("CREATING"));

    // DescribeCluster settles CREATING -> ACTIVE on the read.
    let described = kafka
        .describe_cluster()
        .cluster_arn(&arn)
        .send()
        .await
        .expect("describe cluster");
    let info = described.cluster_info().expect("cluster info");
    assert_eq!(info.state().map(|s| s.as_str()), Some("ACTIVE"));
    assert_eq!(info.number_of_broker_nodes(), Some(3));
    let current_version = info.current_version().expect("current version").to_string();

    // GetBootstrapBrokers is synthesized from the broker nodes.
    let bb = kafka
        .get_bootstrap_brokers()
        .cluster_arn(&arn)
        .send()
        .await
        .expect("bootstrap brokers");
    let plaintext = bb.bootstrap_broker_string().expect("plaintext brokers");
    assert_eq!(plaintext.split(',').count(), 3, "one endpoint per broker");
    assert!(plaintext.contains(":9092"));

    // ListNodes synthesizes one node per broker.
    let nodes = kafka
        .list_nodes()
        .cluster_arn(&arn)
        .send()
        .await
        .expect("list nodes");
    assert_eq!(nodes.node_info_list().len(), 3);

    // ListClusters reflects the new cluster (region-scoped).
    let listed = kafka.list_clusters().send().await.expect("list clusters");
    assert!(
        listed
            .cluster_info_list()
            .iter()
            .any(|c| c.cluster_arn() == Some(arn.as_str())),
        "created cluster must appear in ListClusters"
    );

    // CreateConfiguration with base64 server properties.
    let config = kafka
        .create_configuration()
        .name("e2e-config")
        .server_properties(Blob::new(b"auto.create.topics.enable=true\n".to_vec()))
        .send()
        .await
        .expect("create configuration");
    let config_arn = config.arn().expect("config arn").to_string();
    assert_eq!(config.latest_revision().and_then(|r| r.revision()), Some(1));
    let desc_config = kafka
        .describe_configuration()
        .arn(&config_arn)
        .send()
        .await
        .expect("describe configuration");
    assert_eq!(desc_config.name(), Some("e2e-config"));

    // TagResource + ListTagsForResource round-trip.
    kafka
        .tag_resource()
        .resource_arn(&arn)
        .tags("team", "streaming")
        .send()
        .await
        .expect("tag resource");
    let tags = kafka
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list tags");
    assert_eq!(
        tags.tags().and_then(|t| t.get("team")).map(String::as_str),
        Some("streaming")
    );

    // UpdateBrokerCount records a cluster operation and moves to UPDATING.
    let update = kafka
        .update_broker_count()
        .cluster_arn(&arn)
        .current_version(&current_version)
        .target_number_of_broker_nodes(6)
        .send()
        .await
        .expect("update broker count");
    let op_arn = update
        .cluster_operation_arn()
        .expect("cluster operation arn")
        .to_string();
    assert!(
        op_arn.contains(":cluster-operation/"),
        "operation ARN: {op_arn}"
    );

    // The operation is listed and settles to UPDATE_COMPLETE on the read.
    let ops = kafka
        .list_cluster_operations()
        .cluster_arn(&arn)
        .send()
        .await
        .expect("list operations");
    assert!(
        ops.cluster_operation_info_list()
            .iter()
            .any(|o| o.operation_arn() == Some(op_arn.as_str())),
        "the update operation must be listed"
    );
    let op = kafka
        .describe_cluster_operation()
        .cluster_operation_arn(&op_arn)
        .send()
        .await
        .expect("describe operation");
    assert_eq!(
        op.cluster_operation_info().and_then(|i| i.operation_type()),
        Some("UPDATE_BROKER_COUNT")
    );

    // The broker count change is reflected once the cluster settles.
    let after = kafka
        .describe_cluster()
        .cluster_arn(&arn)
        .send()
        .await
        .expect("describe after update");
    assert_eq!(
        after
            .cluster_info()
            .and_then(|i| i.number_of_broker_nodes()),
        Some(6)
    );

    // DeleteCluster moves to DELETING; the next describe 404s once reaped.
    kafka
        .delete_cluster()
        .cluster_arn(&arn)
        .send()
        .await
        .expect("delete cluster");
    // A second describe settles the deletion and returns a NotFound error.
    let gone = kafka.describe_cluster().cluster_arn(&arn).send().await;
    assert!(gone.is_err(), "deleted cluster must not be describable");
}

#[tokio::test]
async fn list_kafka_versions_are_active() {
    let server = TestServer::start_with_env(&[("FAKECLOUD_KAFKA_DISABLE_BACKEND", "1")]).await;
    let kafka = kafka_client(&server).await;
    let versions = kafka
        .list_kafka_versions()
        .send()
        .await
        .expect("list kafka versions");
    let list = versions.kafka_versions();
    assert!(!list.is_empty(), "MSK exposes a supported version catalog");
    assert!(
        list.iter()
            .all(|v| v.status().map(|s| s.as_str()) == Some("ACTIVE")),
        "every advertised Kafka version is ACTIVE"
    );
}
