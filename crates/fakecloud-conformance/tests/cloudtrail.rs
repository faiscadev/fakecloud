//! CloudTrail (`cloudtrail`) awsJson1.1 conformance probe assertions.
//!
//! One `#[test_action]` per operation in the CloudTrail Smithy model.
//! Checksums are harvested from `aws-models/cloudtrail.json`; the probe
//! generates request variants per operation and asserts fakecloud responds
//! AWS-faithfully.

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("cloudtrail", "AddTags", checksum = "737b84a3")]
#[test_action("cloudtrail", "CancelQuery", checksum = "ed16a52e")]
#[test_action("cloudtrail", "CreateChannel", checksum = "038a130e")]
#[test_action("cloudtrail", "CreateDashboard", checksum = "d02fd204")]
#[test_action("cloudtrail", "CreateEventDataStore", checksum = "85a28968")]
#[test_action("cloudtrail", "CreateTrail", checksum = "66c8b2d1")]
#[test_action("cloudtrail", "DeleteChannel", checksum = "c4c5291b")]
#[test_action("cloudtrail", "DeleteDashboard", checksum = "79584041")]
#[test_action("cloudtrail", "DeleteEventDataStore", checksum = "4472b697")]
#[test_action("cloudtrail", "DeleteResourcePolicy", checksum = "bcc3a7cf")]
#[test_action("cloudtrail", "DeleteTrail", checksum = "713e6084")]
#[test_action(
    "cloudtrail",
    "DeregisterOrganizationDelegatedAdmin",
    checksum = "d717211e"
)]
#[test_action("cloudtrail", "DescribeQuery", checksum = "0b31facc")]
#[test_action("cloudtrail", "DescribeTrails", checksum = "17f76359")]
#[test_action("cloudtrail", "DisableFederation", checksum = "c69212d4")]
#[test_action("cloudtrail", "EnableFederation", checksum = "9e122fb1")]
#[test_action("cloudtrail", "GenerateQuery", checksum = "df0f9127")]
#[test_action("cloudtrail", "GetChannel", checksum = "3ed9b521")]
#[test_action("cloudtrail", "GetDashboard", checksum = "65b920d0")]
#[test_action("cloudtrail", "GetEventConfiguration", checksum = "4911890d")]
#[test_action("cloudtrail", "GetEventDataStore", checksum = "7fcd2030")]
#[test_action("cloudtrail", "GetEventSelectors", checksum = "475555b8")]
#[test_action("cloudtrail", "GetImport", checksum = "53783fc5")]
#[test_action("cloudtrail", "GetInsightSelectors", checksum = "6fb75598")]
#[test_action("cloudtrail", "GetQueryResults", checksum = "65b94755")]
#[test_action("cloudtrail", "GetResourcePolicy", checksum = "69887b16")]
#[test_action("cloudtrail", "GetTrail", checksum = "bf794d13")]
#[test_action("cloudtrail", "GetTrailStatus", checksum = "c0e5cb6f")]
#[test_action("cloudtrail", "ListChannels", checksum = "90f1dffe")]
#[test_action("cloudtrail", "ListDashboards", checksum = "8b971717")]
#[test_action("cloudtrail", "ListEventDataStores", checksum = "761701ca")]
#[test_action("cloudtrail", "ListImportFailures", checksum = "f8744220")]
#[test_action("cloudtrail", "ListImports", checksum = "5fff5f7e")]
#[test_action("cloudtrail", "ListInsightsData", checksum = "d2b337ca")]
#[test_action("cloudtrail", "ListInsightsMetricData", checksum = "7d929636")]
#[test_action("cloudtrail", "ListPublicKeys", checksum = "4c0c3d4e")]
#[test_action("cloudtrail", "ListQueries", checksum = "edd23779")]
#[test_action("cloudtrail", "ListTags", checksum = "c55c4b57")]
#[test_action("cloudtrail", "ListTrails", checksum = "cd666f3a")]
#[test_action("cloudtrail", "LookupEvents", checksum = "8371a1f2")]
#[test_action("cloudtrail", "PutEventConfiguration", checksum = "0c49cab9")]
#[test_action("cloudtrail", "PutEventSelectors", checksum = "da42429f")]
#[test_action("cloudtrail", "PutInsightSelectors", checksum = "80aee099")]
#[test_action("cloudtrail", "PutResourcePolicy", checksum = "d9935a52")]
#[test_action(
    "cloudtrail",
    "RegisterOrganizationDelegatedAdmin",
    checksum = "d862be51"
)]
#[test_action("cloudtrail", "RemoveTags", checksum = "0392eb61")]
#[test_action("cloudtrail", "RestoreEventDataStore", checksum = "cf49dfff")]
#[test_action("cloudtrail", "SearchSampleQueries", checksum = "fe5dd545")]
#[test_action("cloudtrail", "StartDashboardRefresh", checksum = "9ca46124")]
#[test_action("cloudtrail", "StartEventDataStoreIngestion", checksum = "d9361003")]
#[test_action("cloudtrail", "StartImport", checksum = "dc7639d1")]
#[test_action("cloudtrail", "StartLogging", checksum = "01a8e698")]
#[test_action("cloudtrail", "StartQuery", checksum = "0961ab48")]
#[test_action("cloudtrail", "StopEventDataStoreIngestion", checksum = "17be8756")]
#[test_action("cloudtrail", "StopImport", checksum = "93d96c22")]
#[test_action("cloudtrail", "StopLogging", checksum = "5d5c0c44")]
#[test_action("cloudtrail", "UpdateChannel", checksum = "c8510015")]
#[test_action("cloudtrail", "UpdateDashboard", checksum = "44eafd97")]
#[test_action("cloudtrail", "UpdateEventDataStore", checksum = "27a96ae1")]
#[test_action("cloudtrail", "UpdateTrail", checksum = "e3cf00bd")]
#[tokio::test]
async fn cloudtrail_probe() {
    let _server = TestServer::start().await;
}
