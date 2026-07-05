mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("elasticfilesystem", "CreateAccessPoint", checksum = "f30df4f3")]
#[test_action("elasticfilesystem", "CreateFileSystem", checksum = "393dd3fa")]
#[test_action("elasticfilesystem", "CreateMountTarget", checksum = "fd60c6a1")]
#[test_action(
    "elasticfilesystem",
    "CreateReplicationConfiguration",
    checksum = "9a8a81b2"
)]
#[test_action("elasticfilesystem", "CreateTags", checksum = "88e0f06e")]
#[test_action("elasticfilesystem", "DeleteAccessPoint", checksum = "81a09748")]
#[test_action("elasticfilesystem", "DeleteFileSystem", checksum = "b3e3947b")]
#[test_action("elasticfilesystem", "DeleteFileSystemPolicy", checksum = "03af4a6b")]
#[test_action("elasticfilesystem", "DeleteMountTarget", checksum = "c4caccd5")]
#[test_action(
    "elasticfilesystem",
    "DeleteReplicationConfiguration",
    checksum = "4956c044"
)]
#[test_action("elasticfilesystem", "DeleteTags", checksum = "b48caab6")]
#[test_action("elasticfilesystem", "DescribeAccessPoints", checksum = "d9974b96")]
#[test_action(
    "elasticfilesystem",
    "DescribeAccountPreferences",
    checksum = "6b295899"
)]
#[test_action("elasticfilesystem", "DescribeBackupPolicy", checksum = "3c1c9748")]
#[test_action("elasticfilesystem", "DescribeFileSystemPolicy", checksum = "53c22663")]
#[test_action("elasticfilesystem", "DescribeFileSystems", checksum = "c6641cd1")]
#[test_action(
    "elasticfilesystem",
    "DescribeLifecycleConfiguration",
    checksum = "8a43af16"
)]
#[test_action(
    "elasticfilesystem",
    "DescribeMountTargetSecurityGroups",
    checksum = "5ff0fe2a"
)]
#[test_action("elasticfilesystem", "DescribeMountTargets", checksum = "6db3b9e4")]
#[test_action(
    "elasticfilesystem",
    "DescribeReplicationConfigurations",
    checksum = "20366b04"
)]
#[test_action("elasticfilesystem", "DescribeTags", checksum = "80f1258b")]
#[test_action("elasticfilesystem", "ListTagsForResource", checksum = "331fc780")]
#[test_action(
    "elasticfilesystem",
    "ModifyMountTargetSecurityGroups",
    checksum = "6b953382"
)]
#[test_action("elasticfilesystem", "PutAccountPreferences", checksum = "825c4021")]
#[test_action("elasticfilesystem", "PutBackupPolicy", checksum = "38cbbe10")]
#[test_action("elasticfilesystem", "PutFileSystemPolicy", checksum = "02525557")]
#[test_action(
    "elasticfilesystem",
    "PutLifecycleConfiguration",
    checksum = "53e9789a"
)]
#[test_action("elasticfilesystem", "TagResource", checksum = "4b83218d")]
#[test_action("elasticfilesystem", "UntagResource", checksum = "73f5a761")]
#[test_action("elasticfilesystem", "UpdateFileSystem", checksum = "be92b380")]
#[test_action(
    "elasticfilesystem",
    "UpdateFileSystemProtection",
    checksum = "78e4e59f"
)]
#[tokio::test]
async fn efs_conformance() {
    let _server = TestServer::start().await;
}
