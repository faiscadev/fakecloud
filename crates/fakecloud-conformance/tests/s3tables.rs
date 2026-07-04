mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("s3tables", "CreateNamespace", checksum = "47b215a6")]
#[test_action("s3tables", "CreateTable", checksum = "9d39dc98")]
#[test_action("s3tables", "CreateTableBucket", checksum = "0d08fdb7")]
#[test_action("s3tables", "DeleteNamespace", checksum = "1aae4974")]
#[test_action("s3tables", "DeleteTable", checksum = "43e632f0")]
#[test_action("s3tables", "DeleteTableBucket", checksum = "780e2316")]
#[test_action("s3tables", "DeleteTableBucketEncryption", checksum = "dc7368be")]
#[test_action(
    "s3tables",
    "DeleteTableBucketMetricsConfiguration",
    checksum = "f0a6a369"
)]
#[test_action("s3tables", "DeleteTableBucketPolicy", checksum = "8ec56541")]
#[test_action("s3tables", "DeleteTableBucketReplication", checksum = "22119a28")]
#[test_action("s3tables", "DeleteTablePolicy", checksum = "940939cf")]
#[test_action("s3tables", "DeleteTableReplication", checksum = "a06180fa")]
#[test_action("s3tables", "GetNamespace", checksum = "9583982d")]
#[test_action("s3tables", "GetTable", checksum = "a676aa41")]
#[test_action("s3tables", "GetTableBucket", checksum = "301bcddb")]
#[test_action("s3tables", "GetTableBucketEncryption", checksum = "55a942c1")]
#[test_action(
    "s3tables",
    "GetTableBucketMaintenanceConfiguration",
    checksum = "06297c7b"
)]
#[test_action(
    "s3tables",
    "GetTableBucketMetricsConfiguration",
    checksum = "1d8417ca"
)]
#[test_action("s3tables", "GetTableBucketPolicy", checksum = "2dc1043c")]
#[test_action("s3tables", "GetTableBucketReplication", checksum = "6af3e126")]
#[test_action("s3tables", "GetTableBucketStorageClass", checksum = "1641f325")]
#[test_action("s3tables", "GetTableEncryption", checksum = "2c689013")]
#[test_action("s3tables", "GetTableMaintenanceConfiguration", checksum = "ccdff99b")]
#[test_action("s3tables", "GetTableMaintenanceJobStatus", checksum = "abe90675")]
#[test_action("s3tables", "GetTableMetadataLocation", checksum = "71378512")]
#[test_action("s3tables", "GetTablePolicy", checksum = "96574f20")]
#[test_action(
    "s3tables",
    "GetTableRecordExpirationConfiguration",
    checksum = "7f2db7e6"
)]
#[test_action("s3tables", "GetTableRecordExpirationJobStatus", checksum = "211529df")]
#[test_action("s3tables", "GetTableReplication", checksum = "8b8e312c")]
#[test_action("s3tables", "GetTableReplicationStatus", checksum = "97ce4e4b")]
#[test_action("s3tables", "GetTableStorageClass", checksum = "2cabe9b2")]
#[test_action("s3tables", "ListNamespaces", checksum = "d4e5db12")]
#[test_action("s3tables", "ListTableBuckets", checksum = "56b2ac2f")]
#[test_action("s3tables", "ListTables", checksum = "54a93802")]
#[test_action("s3tables", "ListTagsForResource", checksum = "d3069893")]
#[test_action("s3tables", "PutTableBucketEncryption", checksum = "4f08f10c")]
#[test_action(
    "s3tables",
    "PutTableBucketMaintenanceConfiguration",
    checksum = "afae9dc1"
)]
#[test_action(
    "s3tables",
    "PutTableBucketMetricsConfiguration",
    checksum = "ebb1c54b"
)]
#[test_action("s3tables", "PutTableBucketPolicy", checksum = "2781c9da")]
#[test_action("s3tables", "PutTableBucketReplication", checksum = "5b2e2326")]
#[test_action("s3tables", "PutTableBucketStorageClass", checksum = "15e94a8a")]
#[test_action("s3tables", "PutTableMaintenanceConfiguration", checksum = "a713ac3f")]
#[test_action("s3tables", "PutTablePolicy", checksum = "0b578762")]
#[test_action(
    "s3tables",
    "PutTableRecordExpirationConfiguration",
    checksum = "318be759"
)]
#[test_action("s3tables", "PutTableReplication", checksum = "bb11d74c")]
#[test_action("s3tables", "RenameTable", checksum = "9ec9a6fa")]
#[test_action("s3tables", "TagResource", checksum = "f0c67126")]
#[test_action("s3tables", "UntagResource", checksum = "465065d7")]
#[test_action("s3tables", "UpdateTableMetadataLocation", checksum = "f0b371b1")]
#[tokio::test]
async fn s3tables_probe() {
    let _server = TestServer::start().await;
}
