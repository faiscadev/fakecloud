mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("account", "AcceptPrimaryEmailUpdate", checksum = "2d50a50d")]
#[test_action("account", "DeleteAlternateContact", checksum = "8f8e5351")]
#[test_action("account", "DisableRegion", checksum = "36702371")]
#[test_action("account", "EnableRegion", checksum = "0450bf5c")]
#[test_action("account", "GetAccountInformation", checksum = "fa3f9583")]
#[test_action("account", "GetAlternateContact", checksum = "ff5c6fd2")]
#[test_action("account", "GetContactInformation", checksum = "2d4b8256")]
#[test_action("account", "GetGovCloudAccountInformation", checksum = "85e05e5a")]
#[test_action("account", "GetPrimaryEmail", checksum = "ab6ac0a2")]
#[test_action("account", "GetRegionOptStatus", checksum = "5a2bd2e5")]
#[test_action("account", "ListRegions", checksum = "7f7242ac")]
#[test_action("account", "PutAccountName", checksum = "fe6872ea")]
#[test_action("account", "PutAlternateContact", checksum = "661f88b6")]
#[test_action("account", "PutContactInformation", checksum = "012826c9")]
#[test_action("account", "StartPrimaryEmailUpdate", checksum = "ff079419")]
#[tokio::test]
async fn account_probe() {
    let _server = TestServer::start().await;
}
