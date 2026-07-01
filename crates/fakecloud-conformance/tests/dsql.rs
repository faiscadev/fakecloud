mod helpers;

use aws_sdk_dsql::types::{
    KinesisTargetDefinition, StreamFormat, StreamOrdering, TargetDefinition,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

async fn make_cluster(client: &aws_sdk_dsql::Client, deletion_protection: bool) -> String {
    client
        .create_cluster()
        .deletion_protection_enabled(deletion_protection)
        .send()
        .await
        .unwrap()
        .identifier
        .clone()
}

fn kinesis_target() -> TargetDefinition {
    TargetDefinition::Kinesis(
        KinesisTargetDefinition::builder()
            .stream_arn("arn:aws:kinesis:us-east-1:000000000000:stream/dsql-cdc")
            .role_arn("arn:aws:iam::000000000000:role/dsql-stream")
            .build()
            .unwrap(),
    )
}

// ---------------------------------------------------------------------------
// Cluster lifecycle
// ---------------------------------------------------------------------------

#[test_action("dsql", "CreateCluster", checksum = "a9b56c8e")]
#[tokio::test]
async fn dsql_create_cluster() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let resp = client.create_cluster().send().await.unwrap();
    assert_eq!(resp.identifier().len(), 26);
    assert!(resp.arn().contains(":cluster/"));
    assert_eq!(resp.status().as_str(), "CREATING");
    assert!(resp.deletion_protection_enabled());
}

#[test_action("dsql", "GetCluster", checksum = "0cfc214d")]
#[tokio::test]
async fn dsql_get_cluster() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let resp = client.get_cluster().identifier(&id).send().await.unwrap();
    assert_eq!(resp.identifier(), id);
    assert!(resp.endpoint().unwrap().contains("dsql"));
}

#[test_action("dsql", "UpdateCluster", checksum = "6b754515")]
#[tokio::test]
async fn dsql_update_cluster() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let resp = client
        .update_cluster()
        .identifier(&id)
        .deletion_protection_enabled(false)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.identifier(), id);
}

#[test_action("dsql", "DeleteCluster", checksum = "119473a6")]
#[tokio::test]
async fn dsql_delete_cluster() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, false).await;
    let resp = client
        .delete_cluster()
        .identifier(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_str(), "DELETING");
}

#[test_action("dsql", "ListClusters", checksum = "219c2803")]
#[tokio::test]
async fn dsql_list_clusters() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let resp = client.list_clusters().send().await.unwrap();
    assert!(resp.clusters().iter().any(|c| c.identifier() == id));
}

#[test_action("dsql", "GetVpcEndpointServiceName", checksum = "4636592d")]
#[tokio::test]
async fn dsql_get_vpc_endpoint_service_name() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let resp = client
        .get_vpc_endpoint_service_name()
        .identifier(&id)
        .send()
        .await
        .unwrap();
    assert!(resp.service_name().starts_with("com.amazonaws."));
}

// ---------------------------------------------------------------------------
// Cluster policy
// ---------------------------------------------------------------------------

const POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"dsql:DbConnect","Resource":"*"}]}"#;

#[test_action("dsql", "PutClusterPolicy", checksum = "f6b8d5c1")]
#[tokio::test]
async fn dsql_put_cluster_policy() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let resp = client
        .put_cluster_policy()
        .identifier(&id)
        .policy(POLICY)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.policy_version(), "1");
}

#[test_action("dsql", "GetClusterPolicy", checksum = "5a1a54ae")]
#[tokio::test]
async fn dsql_get_cluster_policy() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    client
        .put_cluster_policy()
        .identifier(&id)
        .policy(POLICY)
        .send()
        .await
        .unwrap();
    let resp = client
        .get_cluster_policy()
        .identifier(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.policy(), POLICY);
}

#[test_action("dsql", "DeleteClusterPolicy", checksum = "92086804")]
#[tokio::test]
async fn dsql_delete_cluster_policy() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    client
        .put_cluster_policy()
        .identifier(&id)
        .policy(POLICY)
        .send()
        .await
        .unwrap();
    let resp = client
        .delete_cluster_policy()
        .identifier(&id)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.policy_version(), "2");
}

// ---------------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------------

async fn make_stream(client: &aws_sdk_dsql::Client, cluster_id: &str) -> String {
    client
        .create_stream()
        .cluster_identifier(cluster_id)
        .target_definition(kinesis_target())
        .ordering(StreamOrdering::Unordered)
        .format(StreamFormat::Json)
        .send()
        .await
        .unwrap()
        .stream_identifier
        .clone()
}

#[test_action("dsql", "CreateStream", checksum = "5b4285a1")]
#[tokio::test]
async fn dsql_create_stream() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let resp = client
        .create_stream()
        .cluster_identifier(&id)
        .target_definition(kinesis_target())
        .ordering(StreamOrdering::Unordered)
        .format(StreamFormat::Json)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.cluster_identifier(), id);
    assert_eq!(resp.stream_identifier().len(), 26);
    assert_eq!(resp.status().as_str(), "CREATING");
}

#[test_action("dsql", "GetStream", checksum = "0cbcee24")]
#[tokio::test]
async fn dsql_get_stream() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let sid = make_stream(&client, &id).await;
    let resp = client
        .get_stream()
        .cluster_identifier(&id)
        .stream_identifier(&sid)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.stream_identifier(), sid);
    assert_eq!(resp.format().as_str(), "JSON");
}

#[test_action("dsql", "ListStreams", checksum = "729b145c")]
#[tokio::test]
async fn dsql_list_streams() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let sid = make_stream(&client, &id).await;
    let resp = client
        .list_streams()
        .cluster_identifier(&id)
        .send()
        .await
        .unwrap();
    assert!(resp.streams().iter().any(|s| s.stream_identifier() == sid));
}

#[test_action("dsql", "DeleteStream", checksum = "ce7e583d")]
#[tokio::test]
async fn dsql_delete_stream() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let id = make_cluster(&client, true).await;
    let sid = make_stream(&client, &id).await;
    let resp = client
        .delete_stream()
        .cluster_identifier(&id)
        .stream_identifier(&sid)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.stream_identifier(), sid);
}

// ---------------------------------------------------------------------------
// Tagging
// ---------------------------------------------------------------------------

#[test_action("dsql", "TagResource", checksum = "2c3c57ea")]
#[tokio::test]
async fn dsql_tag_resource() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let arn = client.create_cluster().send().await.unwrap().arn.clone();
    client
        .tag_resource()
        .resource_arn(&arn)
        .tags("team", "platform")
        .send()
        .await
        .unwrap();
}

#[test_action("dsql", "ListTagsForResource", checksum = "d6cc1ab5")]
#[tokio::test]
async fn dsql_list_tags_for_resource() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let arn = client
        .create_cluster()
        .tags("team", "platform")
        .send()
        .await
        .unwrap()
        .arn
        .clone();
    let resp = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().unwrap().get("team").unwrap(), "platform");
}

#[test_action("dsql", "UntagResource", checksum = "986bc5b1")]
#[tokio::test]
async fn dsql_untag_resource() {
    let server = TestServer::start().await;
    let client = server.dsql_client().await;
    let arn = client
        .create_cluster()
        .tags("team", "platform")
        .send()
        .await
        .unwrap()
        .arn
        .clone();
    client
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("team")
        .send()
        .await
        .unwrap();
    let resp = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert!(resp.tags().map(|t| t.is_empty()).unwrap_or(true));
}
