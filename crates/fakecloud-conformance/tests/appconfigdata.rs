mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("appconfigdata", "GetLatestConfiguration", checksum = "7856b26c")]
#[test_action("appconfigdata", "StartConfigurationSession", checksum = "073a7ba5")]
#[tokio::test]
async fn appconfigdata_probe() {
    let _server = TestServer::start().await;
}
