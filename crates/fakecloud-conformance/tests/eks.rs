mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("eks", "CreateCluster", checksum = "89446e5c")]
#[test_action("eks", "DescribeCluster", checksum = "b6b3efc6")]
#[test_action("eks", "ListClusters", checksum = "464ec628")]
#[test_action("eks", "DeleteCluster", checksum = "9ebb66d0")]
#[test_action("eks", "UpdateClusterConfig", checksum = "8dce51aa")]
#[test_action("eks", "UpdateClusterVersion", checksum = "749042fc")]
#[test_action("eks", "DescribeUpdate", checksum = "1f003008")]
#[test_action("eks", "ListUpdates", checksum = "7e756829")]
#[test_action("eks", "TagResource", checksum = "e443c80e")]
#[test_action("eks", "UntagResource", checksum = "94806184")]
#[test_action("eks", "ListTagsForResource", checksum = "7abc6464")]
#[tokio::test]
async fn eks_probe() {
    let _server = TestServer::start().await;
}
