mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action(
    "kinesisanalyticsv2",
    "AddApplicationCloudWatchLoggingOption",
    checksum = "b0cd1d7a"
)]
#[test_action("kinesisanalyticsv2", "AddApplicationInput", checksum = "bdea55f9")]
#[test_action(
    "kinesisanalyticsv2",
    "AddApplicationInputProcessingConfiguration",
    checksum = "4adb8e66"
)]
#[test_action("kinesisanalyticsv2", "AddApplicationOutput", checksum = "30b56a12")]
#[test_action(
    "kinesisanalyticsv2",
    "AddApplicationReferenceDataSource",
    checksum = "e5b42534"
)]
#[test_action(
    "kinesisanalyticsv2",
    "AddApplicationVpcConfiguration",
    checksum = "5383c120"
)]
#[test_action("kinesisanalyticsv2", "CreateApplication", checksum = "1da727c6")]
#[test_action(
    "kinesisanalyticsv2",
    "CreateApplicationPresignedUrl",
    checksum = "0660c8e3"
)]
#[test_action(
    "kinesisanalyticsv2",
    "CreateApplicationSnapshot",
    checksum = "44791e1d"
)]
#[test_action("kinesisanalyticsv2", "DeleteApplication", checksum = "330521a1")]
#[test_action(
    "kinesisanalyticsv2",
    "DeleteApplicationCloudWatchLoggingOption",
    checksum = "1e3c35a2"
)]
#[test_action(
    "kinesisanalyticsv2",
    "DeleteApplicationInputProcessingConfiguration",
    checksum = "b71b7ab6"
)]
#[test_action("kinesisanalyticsv2", "DeleteApplicationOutput", checksum = "9bcd5733")]
#[test_action(
    "kinesisanalyticsv2",
    "DeleteApplicationReferenceDataSource",
    checksum = "6594e484"
)]
#[test_action(
    "kinesisanalyticsv2",
    "DeleteApplicationSnapshot",
    checksum = "ef110bc6"
)]
#[test_action(
    "kinesisanalyticsv2",
    "DeleteApplicationVpcConfiguration",
    checksum = "ebfa1178"
)]
#[test_action("kinesisanalyticsv2", "DescribeApplication", checksum = "59c4d633")]
#[test_action(
    "kinesisanalyticsv2",
    "DescribeApplicationOperation",
    checksum = "f322768e"
)]
#[test_action(
    "kinesisanalyticsv2",
    "DescribeApplicationSnapshot",
    checksum = "9bb66a4d"
)]
#[test_action(
    "kinesisanalyticsv2",
    "DescribeApplicationVersion",
    checksum = "3aae3275"
)]
#[test_action("kinesisanalyticsv2", "DiscoverInputSchema", checksum = "922726a2")]
#[test_action(
    "kinesisanalyticsv2",
    "ListApplicationOperations",
    checksum = "e35ab542"
)]
#[test_action(
    "kinesisanalyticsv2",
    "ListApplicationSnapshots",
    checksum = "35801c2c"
)]
#[test_action("kinesisanalyticsv2", "ListApplicationVersions", checksum = "cce8b35d")]
#[test_action("kinesisanalyticsv2", "ListApplications", checksum = "5dfb6311")]
#[test_action("kinesisanalyticsv2", "ListTagsForResource", checksum = "ed66bc05")]
#[test_action("kinesisanalyticsv2", "RollbackApplication", checksum = "f408ca0a")]
#[test_action("kinesisanalyticsv2", "StartApplication", checksum = "78d183cc")]
#[test_action("kinesisanalyticsv2", "StopApplication", checksum = "406c71cf")]
#[test_action("kinesisanalyticsv2", "TagResource", checksum = "06086e5e")]
#[test_action("kinesisanalyticsv2", "UntagResource", checksum = "9e3481bb")]
#[test_action("kinesisanalyticsv2", "UpdateApplication", checksum = "35a4776a")]
#[test_action(
    "kinesisanalyticsv2",
    "UpdateApplicationMaintenanceConfiguration",
    checksum = "61c38497"
)]
#[tokio::test]
async fn kinesisanalyticsv2_probe() {
    let _server = TestServer::start().await;
}
