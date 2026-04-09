mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn elasticache_test_server_sdk_bootstrap_compiles() {
    let server = TestServer::start().await;
    let _client = server.elasticache_client().await;
}
