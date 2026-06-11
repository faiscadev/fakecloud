mod helpers;

use helpers::TestServer;

/// A freshly provisioned ext4 PV always contains a `lost+found` directory at its
/// root. Mounting such a volume directly at `--data-path` used to trip the
/// emptiness guard and crash-loop the server on first start. The guard must now
/// ignore `lost+found` and still initialize a fresh store.
#[tokio::test]
async fn persistence_starts_with_lost_found_in_data_dir() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir(tmp.path().join("lost+found")).unwrap();

    // If the emptiness guard rejected `lost+found`, the server would refuse to
    // start and this would panic.
    let server = TestServer::start_persistent(tmp.path()).await;
    let client = server.sqs_client().await;

    // Server initialized as a fresh store and serves requests.
    let list = client.list_queues().send().await.unwrap();
    assert!(
        list.queue_urls().is_empty(),
        "fresh data dir should start with no queues",
    );
}
