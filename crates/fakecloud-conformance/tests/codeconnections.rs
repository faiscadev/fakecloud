mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("codeconnections", "CreateConnection", checksum = "4afcace9")]
#[test_action("codeconnections", "CreateHost", checksum = "ec5a1a2c")]
#[test_action("codeconnections", "CreateRepositoryLink", checksum = "f0559c88")]
#[test_action("codeconnections", "CreateSyncConfiguration", checksum = "6fb6d096")]
#[test_action("codeconnections", "DeleteConnection", checksum = "15e2f38f")]
#[test_action("codeconnections", "DeleteHost", checksum = "659fc712")]
#[test_action("codeconnections", "DeleteRepositoryLink", checksum = "945c2bfe")]
#[test_action("codeconnections", "DeleteSyncConfiguration", checksum = "cfbf02f7")]
#[test_action("codeconnections", "GetConnection", checksum = "31c392e3")]
#[test_action("codeconnections", "GetHost", checksum = "049eb9ff")]
#[test_action("codeconnections", "GetRepositoryLink", checksum = "246cecc2")]
#[test_action("codeconnections", "GetRepositorySyncStatus", checksum = "ce39c293")]
#[test_action("codeconnections", "GetResourceSyncStatus", checksum = "61c394b7")]
#[test_action("codeconnections", "GetSyncBlockerSummary", checksum = "ca35b952")]
#[test_action("codeconnections", "GetSyncConfiguration", checksum = "51a46619")]
#[test_action("codeconnections", "ListConnections", checksum = "897428b4")]
#[test_action("codeconnections", "ListHosts", checksum = "9c8da4d3")]
#[test_action("codeconnections", "ListRepositoryLinks", checksum = "51027f99")]
#[test_action(
    "codeconnections",
    "ListRepositorySyncDefinitions",
    checksum = "cf2ae99b"
)]
#[test_action("codeconnections", "ListSyncConfigurations", checksum = "a7c33d43")]
#[test_action("codeconnections", "ListTagsForResource", checksum = "d5cd06ec")]
#[test_action("codeconnections", "TagResource", checksum = "4ed0145c")]
#[test_action("codeconnections", "UntagResource", checksum = "ad670557")]
#[test_action("codeconnections", "UpdateHost", checksum = "7d65b354")]
#[test_action("codeconnections", "UpdateRepositoryLink", checksum = "1b1cbbea")]
#[test_action("codeconnections", "UpdateSyncBlocker", checksum = "039e1b4e")]
#[test_action("codeconnections", "UpdateSyncConfiguration", checksum = "5d66cf4f")]
#[tokio::test]
async fn codeconnections_probe() {
    let _server = TestServer::start().await;
}
