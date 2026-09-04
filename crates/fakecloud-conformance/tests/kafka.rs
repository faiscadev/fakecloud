mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("kafka", "BatchAssociateScramSecret", checksum = "eca7f60f")]
#[test_action("kafka", "BatchDisassociateScramSecret", checksum = "0a717073")]
#[test_action("kafka", "CreateCluster", checksum = "10fd812f")]
#[test_action("kafka", "CreateClusterV2", checksum = "6cbbe886")]
#[test_action("kafka", "CreateConfiguration", checksum = "2d611cc6")]
#[test_action("kafka", "CreateReplicator", checksum = "5efe406b")]
#[test_action("kafka", "CreateTopic", checksum = "292650ae")]
#[test_action("kafka", "CreateVpcConnection", checksum = "9076d349")]
#[test_action("kafka", "DeleteCluster", checksum = "2b0ff8d6")]
#[test_action("kafka", "DeleteClusterPolicy", checksum = "a060a0ca")]
#[test_action("kafka", "DeleteConfiguration", checksum = "4cc3adb2")]
#[test_action("kafka", "DeleteReplicator", checksum = "504b2c9a")]
#[test_action("kafka", "DeleteTopic", checksum = "5909dcbc")]
#[test_action("kafka", "DeleteVpcConnection", checksum = "87067d0e")]
#[test_action("kafka", "DescribeCluster", checksum = "da16f308")]
#[test_action("kafka", "DescribeClusterOperation", checksum = "f891a195")]
#[test_action("kafka", "DescribeClusterOperationV2", checksum = "21dd0cd4")]
#[test_action("kafka", "DescribeClusterV2", checksum = "6d059712")]
#[test_action("kafka", "DescribeConfiguration", checksum = "a7a65191")]
#[test_action("kafka", "DescribeConfigurationRevision", checksum = "7216bcf5")]
#[test_action("kafka", "DescribeReplicator", checksum = "2e79796a")]
#[test_action("kafka", "DescribeTopic", checksum = "6fcad32c")]
#[test_action("kafka", "DescribeTopicPartitions", checksum = "0bdaf5ac")]
#[test_action("kafka", "DescribeVpcConnection", checksum = "f546e177")]
#[test_action("kafka", "GetBootstrapBrokers", checksum = "f3b96ac3")]
#[test_action("kafka", "GetClusterPolicy", checksum = "a3a7c212")]
#[test_action("kafka", "GetCompatibleKafkaVersions", checksum = "6524c074")]
#[test_action("kafka", "ListClientVpcConnections", checksum = "500af701")]
#[test_action("kafka", "ListClusterOperations", checksum = "85935796")]
#[test_action("kafka", "ListClusterOperationsV2", checksum = "b1b59c1a")]
#[test_action("kafka", "ListClusters", checksum = "5ae24236")]
#[test_action("kafka", "ListClustersV2", checksum = "cfbf4cc4")]
#[test_action("kafka", "ListConfigurationRevisions", checksum = "f868abaf")]
#[test_action("kafka", "ListConfigurations", checksum = "4b8cb2df")]
#[test_action("kafka", "ListKafkaVersions", checksum = "9b556949")]
#[test_action("kafka", "ListNodes", checksum = "ab37a892")]
#[test_action("kafka", "ListReplicators", checksum = "8fff88ad")]
#[test_action("kafka", "ListScramSecrets", checksum = "655045e8")]
#[test_action("kafka", "ListTagsForResource", checksum = "a9a04746")]
#[test_action("kafka", "ListTopics", checksum = "6d5e8db4")]
#[test_action("kafka", "ListVpcConnections", checksum = "256e8906")]
#[test_action("kafka", "PutClusterPolicy", checksum = "376d648d")]
#[test_action("kafka", "RebootBroker", checksum = "e86e21c7")]
#[test_action("kafka", "RejectClientVpcConnection", checksum = "261afbe4")]
#[test_action("kafka", "TagResource", checksum = "c39546ad")]
#[test_action("kafka", "UntagResource", checksum = "90bab122")]
#[test_action("kafka", "UpdateBrokerCount", checksum = "fb7eee37")]
#[test_action("kafka", "UpdateBrokerStorage", checksum = "e1cdae4f")]
#[test_action("kafka", "UpdateBrokerType", checksum = "f4970fea")]
#[test_action("kafka", "UpdateClusterConfiguration", checksum = "ca2a7a42")]
#[test_action("kafka", "UpdateClusterKafkaVersion", checksum = "45c184ab")]
#[test_action("kafka", "UpdateConfiguration", checksum = "ceda0807")]
#[test_action("kafka", "UpdateConnectivity", checksum = "4c6361b4")]
#[test_action("kafka", "UpdateMonitoring", checksum = "7b1e3a6a")]
#[test_action("kafka", "UpdateRebalancing", checksum = "513fce61")]
#[test_action("kafka", "UpdateReplicationInfo", checksum = "28e45f73")]
#[test_action("kafka", "UpdateSecurity", checksum = "9714d461")]
#[test_action("kafka", "UpdateStorage", checksum = "5d0cc3ed")]
#[test_action("kafka", "UpdateTopic", checksum = "1c0c9a33")]
#[tokio::test]
async fn kafka_conformance() {
    let _server = TestServer::start().await;
}

const KAFKA_AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/kafka/aws4_request, SignedHeaders=host, Signature=0";

fn enc_arn(arn: &str) -> String {
    arn.replace(':', "%3A").replace('/', "%2F")
}

// Channel operations are newer than the typed aws-sdk-kafka client, so drive
// them over raw restJson1 path calls. They round-trip through the channels
// sub-resource of a provisioned cluster.
#[test_action("kafka", "CreateChannel", checksum = "e38972bb")]
#[test_action("kafka", "DeleteChannel", checksum = "45b159d2")]
#[test_action("kafka", "DescribeChannel", checksum = "abbb1427")]
#[test_action("kafka", "ListChannels", checksum = "5c454c94")]
#[test_action("kafka", "UpdateChannel", checksum = "f5cb6bf2")]
#[tokio::test]
async fn kafka_channel_lifecycle() {
    let server = TestServer::start().await;
    let client = reqwest::Client::new();
    let base = server.endpoint();

    // Provision a cluster to host the channel.
    let resp = client
        .post(format!("{base}/v1/clusters"))
        .header("Authorization", KAFKA_AUTH)
        .header("Content-Type", "application/json")
        .body(
            r#"{"clusterName":"chan-cluster","kafkaVersion":"3.5.1","numberOfBrokerNodes":3,"brokerNodeGroupInfo":{"instanceType":"kafka.m5.large","clientSubnets":["subnet-1","subnet-2","subnet-3"]}}"#,
        )
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create cluster: {}",
        resp.status()
    );
    let cluster_arn = resp.json::<serde_json::Value>().await.unwrap()["clusterArn"]
        .as_str()
        .unwrap()
        .to_string();
    let enc = enc_arn(&cluster_arn);

    // CreateChannel.
    let resp = client
        .post(format!("{base}/v1/clusters/{enc}/channels"))
        .header("Authorization", KAFKA_AUTH)
        .header("Content-Type", "application/json")
        .body(
            r#"{"channelName":"chan-1","topicConfigurationList":[{"topicName":"orders"}],"s3DestinationConfiguration":{"bucketArn":"arn:aws:s3:::b"}}"#,
        )
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "create channel: {}",
        resp.status()
    );
    let channel_arn = resp.json::<serde_json::Value>().await.unwrap()["channelArn"]
        .as_str()
        .unwrap()
        .to_string();
    let cenc = enc_arn(&channel_arn);

    // ListChannels returns it.
    let resp = client
        .get(format!("{base}/v1/clusters/{enc}/channels"))
        .header("Authorization", KAFKA_AUTH)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "list channels: {}",
        resp.status()
    );
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        v["channels"][0]["channelName"].as_str(),
        Some("chan-1"),
        "{v}"
    );

    // DescribeChannel settles CREATING -> ACTIVE.
    let resp = client
        .get(format!("{base}/v1/clusters/{enc}/channels/{cenc}"))
        .header("Authorization", KAFKA_AUTH)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "describe channel: {}",
        resp.status()
    );
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["status"].as_str(), Some("ACTIVE"), "{v}");
    assert_eq!(v["destinationType"].as_str(), Some("S3"), "{v}");

    // UpdateChannel.
    let resp = client
        .put(format!("{base}/v1/clusters/{enc}/channels/{cenc}"))
        .header("Authorization", KAFKA_AUTH)
        .header("Content-Type", "application/json")
        .body(r#"{"s3DestinationUpdate":{"bucketArn":"arn:aws:s3:::b2"}}"#)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "update channel: {}",
        resp.status()
    );

    // DeleteChannel, then it is gone from ListChannels.
    let resp = client
        .delete(format!("{base}/v1/clusters/{enc}/channels/{cenc}"))
        .header("Authorization", KAFKA_AUTH)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "delete channel: {}",
        resp.status()
    );
    let resp = client
        .get(format!("{base}/v1/clusters/{enc}/channels"))
        .header("Authorization", KAFKA_AUTH)
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(
        v["channels"].as_array().map(Vec::is_empty).unwrap_or(true),
        "channel not deleted: {v}"
    );
}
