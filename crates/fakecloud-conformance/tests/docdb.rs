mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("docdb", "AddSourceIdentifierToSubscription", checksum = "34b88d96")]
#[test_action("docdb", "AddTagsToResource", checksum = "ae5a9cdf")]
#[test_action("docdb", "ApplyPendingMaintenanceAction", checksum = "e66af7e3")]
#[test_action("docdb", "CopyDBClusterParameterGroup", checksum = "7cd1d8ac")]
#[test_action("docdb", "CopyDBClusterSnapshot", checksum = "b8b04fa2")]
#[test_action("docdb", "CreateDBCluster", checksum = "a52eeebc")]
#[test_action("docdb", "CreateDBClusterParameterGroup", checksum = "5e9963a6")]
#[test_action("docdb", "CreateDBClusterSnapshot", checksum = "e576d111")]
#[test_action("docdb", "CreateDBInstance", checksum = "b0124e03")]
#[test_action("docdb", "CreateDBSubnetGroup", checksum = "29f0d10f")]
#[test_action("docdb", "CreateEventSubscription", checksum = "678f6408")]
#[test_action("docdb", "CreateGlobalCluster", checksum = "53e31437")]
#[test_action("docdb", "DeleteDBCluster", checksum = "3cf807ef")]
#[test_action("docdb", "DeleteDBClusterParameterGroup", checksum = "87107a96")]
#[test_action("docdb", "DeleteDBClusterSnapshot", checksum = "9b21b70e")]
#[test_action("docdb", "DeleteDBInstance", checksum = "b2abc97a")]
#[test_action("docdb", "DeleteDBSubnetGroup", checksum = "5b471377")]
#[test_action("docdb", "DeleteEventSubscription", checksum = "b58ce3ff")]
#[test_action("docdb", "DeleteGlobalCluster", checksum = "16454696")]
#[test_action("docdb", "DescribeCertificates", checksum = "9c978533")]
#[test_action("docdb", "DescribeDBClusterParameterGroups", checksum = "44e5b54b")]
#[test_action("docdb", "DescribeDBClusterParameters", checksum = "c4301251")]
#[test_action("docdb", "DescribeDBClusterSnapshotAttributes", checksum = "45323967")]
#[test_action("docdb", "DescribeDBClusterSnapshots", checksum = "ccb623ee")]
#[test_action("docdb", "DescribeDBClusters", checksum = "70703b82")]
#[test_action("docdb", "DescribeDBEngineVersions", checksum = "bbccf21e")]
#[test_action("docdb", "DescribeDBInstances", checksum = "c1f39bae")]
#[test_action("docdb", "DescribeDBSubnetGroups", checksum = "8ce472cc")]
#[test_action(
    "docdb",
    "DescribeEngineDefaultClusterParameters",
    checksum = "2fbe3959"
)]
#[test_action("docdb", "DescribeEventCategories", checksum = "a53a3236")]
#[test_action("docdb", "DescribeEventSubscriptions", checksum = "85ea84d2")]
#[test_action("docdb", "DescribeEvents", checksum = "daf8d02e")]
#[test_action("docdb", "DescribeGlobalClusters", checksum = "b5018327")]
#[test_action("docdb", "DescribeOrderableDBInstanceOptions", checksum = "a3f52509")]
#[test_action("docdb", "DescribePendingMaintenanceActions", checksum = "dfc566cc")]
#[test_action("docdb", "FailoverDBCluster", checksum = "e3277e32")]
#[test_action("docdb", "FailoverGlobalCluster", checksum = "263fb7b2")]
#[test_action("docdb", "ListTagsForResource", checksum = "cefe5ebd")]
#[test_action("docdb", "ModifyDBCluster", checksum = "2022907c")]
#[test_action("docdb", "ModifyDBClusterParameterGroup", checksum = "72c30f87")]
#[test_action("docdb", "ModifyDBClusterSnapshotAttribute", checksum = "5e922a8b")]
#[test_action("docdb", "ModifyDBInstance", checksum = "e4c180bf")]
#[test_action("docdb", "ModifyDBSubnetGroup", checksum = "bab081bc")]
#[test_action("docdb", "ModifyEventSubscription", checksum = "97894d7a")]
#[test_action("docdb", "ModifyGlobalCluster", checksum = "dce9be8f")]
#[test_action("docdb", "RebootDBInstance", checksum = "dc6a7a1d")]
#[test_action("docdb", "RemoveFromGlobalCluster", checksum = "78f873cd")]
#[test_action(
    "docdb",
    "RemoveSourceIdentifierFromSubscription",
    checksum = "1480f1e3"
)]
#[test_action("docdb", "RemoveTagsFromResource", checksum = "229ee44d")]
#[test_action("docdb", "ResetDBClusterParameterGroup", checksum = "5f0e38d9")]
#[test_action("docdb", "RestoreDBClusterFromSnapshot", checksum = "26f4a1bb")]
#[test_action("docdb", "RestoreDBClusterToPointInTime", checksum = "7ce4fe1d")]
#[test_action("docdb", "StartDBCluster", checksum = "558d462b")]
#[test_action("docdb", "StopDBCluster", checksum = "589458c8")]
#[test_action("docdb", "SwitchoverGlobalCluster", checksum = "0854cfcc")]
#[tokio::test]
async fn docdb_probe() {
    let _server = TestServer::start().await;
}

/// DocumentDB shares the `rds` SigV4 scope and endpoint; the `api/docdb`
/// user-agent token is what routes a request to DocumentDB.
async fn docdb_post(server: &TestServer, action: &str, params: &[(&str, &str)]) -> String {
    let mut body = format!("Action={action}&Version=2014-10-31");
    for (k, v) in params {
        body.push_str(&format!("&{k}={v}"));
    }
    let resp = reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header("content-type", "application/x-www-form-urlencoded")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/rds/aws4_request, SignedHeaders=host, Signature=0",
        )
        .header("user-agent", "aws-sdk-rust/1.0 api/docdb/1.0")
        .body(body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let text = resp.text().await.unwrap();
    assert!(status.is_success(), "{action} failed: {status} {text}");
    text
}

#[test_action("docdb", "DescribeServerlessV2PlatformVersions", checksum = "09479f4f")]
#[tokio::test]
async fn docdb_describe_serverless_v2_platform_versions() {
    let server = TestServer::start().await;
    let xml = docdb_post(
        &server,
        "DescribeServerlessV2PlatformVersions",
        &[("Engine", "docdb"), ("DefaultOnly", "true")],
    )
    .await;
    assert!(
        xml.contains("<DescribeServerlessV2PlatformVersionsResult>"),
        "{xml}"
    );
    assert_eq!(
        xml.matches("<ServerlessV2PlatformVersionInfo>").count(),
        1,
        "{xml}"
    );
    assert!(xml.contains("<IsDefault>true</IsDefault>"), "{xml}");

    docdb_post(
        &server,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "sls"),
            ("Engine", "docdb"),
            ("MasterUsername", "admin"),
            ("MasterUserPassword", "password123"),
            ("ServerlessV2ScalingConfiguration.MinCapacity", "0.5"),
            ("ServerlessV2ScalingConfiguration.MaxCapacity", "4"),
        ],
    )
    .await;
    let clusters = docdb_post(
        &server,
        "DescribeDBClusters",
        &[("DBClusterIdentifier", "sls")],
    )
    .await;
    assert!(
        clusters.contains("<ServerlessV2PlatformVersion>2</ServerlessV2PlatformVersion>"),
        "{clusters}"
    );
}
