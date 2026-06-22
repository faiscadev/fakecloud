mod helpers;

use helpers::TestServer;

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn require_docker_or_skip(test: &str) -> bool {
    if docker_available() {
        return true;
    }
    if std::env::var("CI").is_ok() {
        panic!("docker is required for {test} in CI");
    }
    eprintln!("Skipping {test}: docker not available");
    false
}

/// Cache cluster endpoint survives a restart. Pre-fix, the cluster row
/// was persisted with `cache_cluster_status=available` but the Docker
/// container was gone, so the endpoint TCP-port wouldn't accept
/// connections after restart. Same bug class as RDS #1338.
#[tokio::test]
async fn persistence_cache_cluster_endpoint_works_after_restart() {
    if !require_docker_or_skip("persistence_cache_cluster_endpoint_works_after_restart") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    let extra_args = ["--storage-mode", "persistent", "--data-path", &data_path];
    let mut server = TestServer::start_full(&[], &extra_args).await;
    let client = server.elasticache_client().await;

    client
        .create_cache_cluster()
        .cache_cluster_id("restart-cache")
        .cache_node_type("cache.t3.micro")
        .preferred_availability_zone("us-east-1a")
        .send()
        .await
        .unwrap();

    // The backing container starts in the background; let the cluster reach
    // "available" before snapshotting so the restart recovers a complete row
    // (bug-audit 2026-05-28, 3.2).
    helpers::wait_for_cache_cluster_available(&client, "restart-cache", 120).await;

    drop(client);
    server.restart().await;
    let client = server.elasticache_client().await;

    // Poll status back to available after recovery.
    let mut status_ok = false;
    let mut port = 0;
    for _ in 0..60 {
        let resp = client
            .describe_cache_clusters()
            .cache_cluster_id("restart-cache")
            .show_cache_node_info(true)
            .send()
            .await
            .unwrap();
        let clusters = resp.cache_clusters();
        if let Some(cluster) = clusters.first() {
            if cluster.cache_cluster_status() == Some("available") {
                let endpoint = cluster.cache_nodes()[0]
                    .endpoint()
                    .expect("cache node endpoint");
                port = endpoint.port().expect("port");
                status_ok = true;
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    assert!(status_ok, "cache cluster did not recover to `available`");
    let stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await;
    assert!(
        stream.is_ok(),
        "recovered cache cluster endpoint must accept connections: {stream:?}",
    );
}

/// Send one RESP command to a Redis endpoint and return the raw reply text.
async fn redis_cmd(stream: &mut tokio::net::TcpStream, cmd: &[u8]) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(cmd).await.expect("write redis command");
    let mut buf = [0u8; 512];
    let n = stream.read(&mut buf).await.expect("read redis reply");
    String::from_utf8_lossy(&buf[..n]).into_owned()
}

/// Poll DescribeCacheClusters until the cluster is `available`, returning the
/// node endpoint port. Panics if it never recovers.
async fn wait_cache_port(client: &aws_sdk_elasticache::Client, id: &str) -> i32 {
    for _ in 0..120 {
        let resp = client
            .describe_cache_clusters()
            .cache_cluster_id(id)
            .show_cache_node_info(true)
            .send()
            .await
            .unwrap();
        if let Some(cluster) = resp.cache_clusters().first() {
            if cluster.cache_cluster_status() == Some("available") {
                if let Some(port) = cluster
                    .cache_nodes()
                    .first()
                    .and_then(|n| n.endpoint())
                    .and_then(|e| e.port())
                {
                    return port;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
    panic!("cache cluster {id} never reached available with an endpoint port");
}

/// Data WRITTEN to a Redis cache survives a restart, not just the endpoint.
/// ElastiCache backs Redis/Valkey with a durable `/data` volume, so a key SET
/// (and SAVEd to the RDB) before restart is still readable after the backing
/// container is recreated. The explicit SAVE forces the RDB to the volume so
/// the abrupt container teardown on restart can't lose the unflushed write.
#[tokio::test]
async fn persistence_cache_data_survives_restart() {
    if !require_docker_or_skip("persistence_cache_data_survives_restart") {
        return;
    }
    let tmp = tempfile::tempdir().unwrap();
    let data_path = tmp.path().display().to_string();
    let extra_args = ["--storage-mode", "persistent", "--data-path", &data_path];
    let mut server = TestServer::start_full(&[], &extra_args).await;
    let client = server.elasticache_client().await;

    client
        .create_cache_cluster()
        .cache_cluster_id("data-cache")
        .engine("redis")
        .cache_node_type("cache.t3.micro")
        .preferred_availability_zone("us-east-1a")
        .send()
        .await
        .unwrap();
    helpers::wait_for_cache_cluster_available(&client, "data-cache", 120).await;
    let port = wait_cache_port(&client, "data-cache").await;

    // SET a key, then SAVE so the RDB hits the durable /data volume.
    {
        let mut s = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .expect("connect redis to seed");
        let set = redis_cmd(
            &mut s,
            b"*3\r\n$3\r\nSET\r\n$11\r\ndurable-key\r\n$13\r\nsurvive-value\r\n",
        )
        .await;
        assert!(set.starts_with("+OK"), "SET should succeed: {set:?}");
        let save = redis_cmd(&mut s, b"*1\r\n$4\r\nSAVE\r\n").await;
        assert!(save.starts_with("+OK"), "SAVE should succeed: {save:?}");
    }

    drop(client);
    server.restart().await;
    let client = server.elasticache_client().await;

    let port = wait_cache_port(&client, "data-cache").await;

    // The recovered container reloads the RDB from the volume; retry GET while
    // redis finishes loading.
    let mut value = String::new();
    for _ in 0..40 {
        if let Ok(mut s) = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}")).await {
            value = redis_cmd(&mut s, b"*2\r\n$3\r\nGET\r\n$11\r\ndurable-key\r\n").await;
            if value.contains("survive-value") {
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(
        value.contains("survive-value"),
        "cached key must survive the restart via the durable /data volume, got {value:?}"
    );
}

/// Users and user groups survive a restart.
#[tokio::test]
async fn persistence_round_trip_user_and_group() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.elasticache_client().await;

    client
        .create_user()
        .user_id("persist-user")
        .user_name("persist-user")
        .engine("redis")
        .access_string("on ~* +@all")
        .no_password_required(true)
        .send()
        .await
        .unwrap();

    client
        .create_user_group()
        .user_group_id("persist-group")
        .engine("redis")
        .user_ids("default")
        .user_ids("persist-user")
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.elasticache_client().await;

    // User survives
    let users = client
        .describe_users()
        .user_id("persist-user")
        .send()
        .await
        .unwrap();
    assert_eq!(users.users().len(), 1);
    assert_eq!(users.users()[0].user_id(), Some("persist-user"));

    // User group survives
    let groups = client
        .describe_user_groups()
        .user_group_id("persist-group")
        .send()
        .await
        .unwrap();
    assert_eq!(groups.user_groups().len(), 1);
    let group = &groups.user_groups()[0];
    assert_eq!(group.user_group_id(), Some("persist-group"));
    assert!(group.user_ids().contains(&"persist-user".to_string()));
    assert!(group.user_ids().contains(&"default".to_string()));
}

/// Subnet groups survive a restart.
#[tokio::test]
async fn persistence_round_trip_subnet_group() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.elasticache_client().await;

    client
        .create_cache_subnet_group()
        .cache_subnet_group_name("persist-sg")
        .cache_subnet_group_description("Persistence test subnet group")
        .subnet_ids("subnet-aaa")
        .subnet_ids("subnet-bbb")
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.elasticache_client().await;

    let groups = client
        .describe_cache_subnet_groups()
        .cache_subnet_group_name("persist-sg")
        .send()
        .await
        .unwrap();
    let sgs = groups.cache_subnet_groups();
    assert_eq!(sgs.len(), 1);
    assert_eq!(sgs[0].cache_subnet_group_name(), Some("persist-sg"));
    assert_eq!(
        sgs[0].cache_subnet_group_description(),
        Some("Persistence test subnet group")
    );
}

/// Tags survive a restart.
#[tokio::test]
async fn persistence_round_trip_tags() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.elasticache_client().await;

    client
        .create_user()
        .user_id("tagged-user")
        .user_name("tagged-user")
        .engine("redis")
        .access_string("on ~* +@all")
        .no_password_required(true)
        .send()
        .await
        .unwrap();

    // Get the ARN
    let users = client
        .describe_users()
        .user_id("tagged-user")
        .send()
        .await
        .unwrap();
    let arn = users.users()[0].arn().unwrap().to_string();

    client
        .add_tags_to_resource()
        .resource_name(&arn)
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("env")
                .value("prod")
                .build(),
        )
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.elasticache_client().await;

    let tags = client
        .list_tags_for_resource()
        .resource_name(&arn)
        .send()
        .await
        .unwrap();
    assert!(tags
        .tag_list()
        .iter()
        .any(|t| t.key() == Some("env") && t.value() == Some("prod")));
}

/// Deletion survives a restart.
#[tokio::test]
async fn persistence_deletion_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let client = server.elasticache_client().await;

    client
        .create_user()
        .user_id("doomed-user")
        .user_name("doomed-user")
        .engine("redis")
        .access_string("on ~* +@all")
        .no_password_required(true)
        .send()
        .await
        .unwrap();

    client
        .delete_user()
        .user_id("doomed-user")
        .send()
        .await
        .unwrap();

    drop(client);
    server.restart().await;
    let client = server.elasticache_client().await;

    let users = client.describe_users().send().await.unwrap();
    assert!(
        !users
            .users()
            .iter()
            .any(|u| u.user_id() == Some("doomed-user")),
        "deleted user should not reappear"
    );
}
