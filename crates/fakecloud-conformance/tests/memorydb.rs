mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("memorydb", "BatchUpdateCluster", checksum = "593b4ae2")]
#[test_action("memorydb", "CopySnapshot", checksum = "c8cd9f37")]
#[test_action("memorydb", "CreateACL", checksum = "47a55dda")]
#[test_action("memorydb", "CreateCluster", checksum = "8c6e4826")]
#[test_action("memorydb", "CreateMultiRegionCluster", checksum = "5e5729aa")]
#[test_action("memorydb", "CreateParameterGroup", checksum = "5a494d9c")]
#[test_action("memorydb", "CreateSnapshot", checksum = "2f445eab")]
#[test_action("memorydb", "CreateSubnetGroup", checksum = "8a09e403")]
#[test_action("memorydb", "CreateUser", checksum = "b5113aaa")]
#[test_action("memorydb", "DeleteACL", checksum = "b3efe645")]
#[test_action("memorydb", "DeleteCluster", checksum = "2e11d3a7")]
#[test_action("memorydb", "DeleteMultiRegionCluster", checksum = "3062dc21")]
#[test_action("memorydb", "DeleteParameterGroup", checksum = "f2495e0a")]
#[test_action("memorydb", "DeleteSnapshot", checksum = "fc8c919b")]
#[test_action("memorydb", "DeleteSubnetGroup", checksum = "f08862a2")]
#[test_action("memorydb", "DeleteUser", checksum = "159b9500")]
#[test_action("memorydb", "DescribeACLs", checksum = "afd5e0ca")]
#[test_action("memorydb", "DescribeClusters", checksum = "bd452d2b")]
#[test_action("memorydb", "DescribeEngineVersions", checksum = "1c72f71d")]
#[test_action("memorydb", "DescribeEvents", checksum = "eb1db1db")]
#[test_action("memorydb", "DescribeMultiRegionClusters", checksum = "aafdbc56")]
#[test_action(
    "memorydb",
    "DescribeMultiRegionParameterGroups",
    checksum = "ce5d7a2e"
)]
#[test_action("memorydb", "DescribeMultiRegionParameters", checksum = "a2a1f7e5")]
#[test_action("memorydb", "DescribeParameterGroups", checksum = "79b6a504")]
#[test_action("memorydb", "DescribeParameters", checksum = "afb2ce45")]
#[test_action("memorydb", "DescribeReservedNodes", checksum = "543f3642")]
#[test_action("memorydb", "DescribeReservedNodesOfferings", checksum = "2e010c45")]
#[test_action("memorydb", "DescribeServiceUpdates", checksum = "3da1086a")]
#[test_action("memorydb", "DescribeSnapshots", checksum = "ca7017eb")]
#[test_action("memorydb", "DescribeSubnetGroups", checksum = "308910bd")]
#[test_action("memorydb", "DescribeUsers", checksum = "1adef92d")]
#[test_action("memorydb", "FailoverShard", checksum = "769f8ad6")]
#[test_action(
    "memorydb",
    "ListAllowedMultiRegionClusterUpdates",
    checksum = "b18cb7a4"
)]
#[test_action("memorydb", "ListAllowedNodeTypeUpdates", checksum = "7ad0f838")]
#[test_action("memorydb", "ListTags", checksum = "33fffc4a")]
#[test_action("memorydb", "PurchaseReservedNodesOffering", checksum = "a30a0fb5")]
#[test_action("memorydb", "ResetParameterGroup", checksum = "9cc608e8")]
#[test_action("memorydb", "TagResource", checksum = "b685b4c6")]
#[test_action("memorydb", "UntagResource", checksum = "841eedfd")]
#[test_action("memorydb", "UpdateACL", checksum = "282838fc")]
#[test_action("memorydb", "UpdateCluster", checksum = "69417bfe")]
#[test_action("memorydb", "UpdateMultiRegionCluster", checksum = "cee3c3ed")]
#[test_action("memorydb", "UpdateParameterGroup", checksum = "cf3c8cbf")]
#[test_action("memorydb", "UpdateSubnetGroup", checksum = "6b5ea619")]
#[test_action("memorydb", "UpdateUser", checksum = "edd7c7a8")]
#[tokio::test]
async fn memorydb_probe() {
    let _server = TestServer::start().await;
}
