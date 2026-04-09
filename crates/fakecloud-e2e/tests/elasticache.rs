mod helpers;

use helpers::TestServer;

// ---------------------------------------------------------------------------
// CacheSubnetGroup tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_create_subnet_group_and_describe() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("my-subnet-group")
        .cache_subnet_group_description("My test subnet group")
        .subnet_ids("subnet-aaa111")
        .subnet_ids("subnet-bbb222")
        .send()
        .await
        .unwrap();

    let group = create_resp
        .cache_subnet_group()
        .expect("cache subnet group");
    assert_eq!(group.cache_subnet_group_name(), Some("my-subnet-group"));
    assert_eq!(
        group.cache_subnet_group_description(),
        Some("My test subnet group")
    );
    assert!(group.vpc_id().is_some());
    assert_eq!(group.subnets().len(), 2);

    // Verify it appears in describe
    let describe_resp = client.describe_cache_subnet_groups().send().await.unwrap();

    let groups = describe_resp.cache_subnet_groups();
    assert!(groups
        .iter()
        .any(|g| g.cache_subnet_group_name() == Some("my-subnet-group")));
}

#[tokio::test]
async fn elasticache_describe_subnet_groups_with_name_filter() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_subnet_group()
        .cache_subnet_group_name("filtered-group")
        .cache_subnet_group_description("For filtering test")
        .subnet_ids("subnet-aaa111")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_cache_subnet_groups()
        .cache_subnet_group_name("filtered-group")
        .send()
        .await
        .unwrap();

    let groups = response.cache_subnet_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].cache_subnet_group_name(), Some("filtered-group"));
}

#[tokio::test]
async fn elasticache_modify_subnet_group_description() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_subnet_group()
        .cache_subnet_group_name("mod-group")
        .cache_subnet_group_description("Original")
        .subnet_ids("subnet-aaa111")
        .send()
        .await
        .unwrap();

    let modify_resp = client
        .modify_cache_subnet_group()
        .cache_subnet_group_name("mod-group")
        .cache_subnet_group_description("Updated description")
        .send()
        .await
        .unwrap();

    let group = modify_resp
        .cache_subnet_group()
        .expect("cache subnet group");
    assert_eq!(
        group.cache_subnet_group_description(),
        Some("Updated description")
    );

    // Verify via describe
    let describe_resp = client
        .describe_cache_subnet_groups()
        .cache_subnet_group_name("mod-group")
        .send()
        .await
        .unwrap();

    assert_eq!(
        describe_resp.cache_subnet_groups()[0].cache_subnet_group_description(),
        Some("Updated description")
    );
}

#[tokio::test]
async fn elasticache_delete_subnet_group_and_verify_gone() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_subnet_group()
        .cache_subnet_group_name("del-group")
        .cache_subnet_group_description("Will be deleted")
        .subnet_ids("subnet-aaa111")
        .send()
        .await
        .unwrap();

    client
        .delete_cache_subnet_group()
        .cache_subnet_group_name("del-group")
        .send()
        .await
        .unwrap();

    // Verify it's gone
    let result = client
        .describe_cache_subnet_groups()
        .cache_subnet_group_name("del-group")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_create_duplicate_subnet_group_errors() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_subnet_group()
        .cache_subnet_group_name("dup-group")
        .cache_subnet_group_description("First")
        .subnet_ids("subnet-aaa111")
        .send()
        .await
        .unwrap();

    let result = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("dup-group")
        .cache_subnet_group_description("Second")
        .subnet_ids("subnet-bbb222")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_delete_nonexistent_subnet_group_errors() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .delete_cache_subnet_group()
        .cache_subnet_group_name("nonexistent-group")
        .send()
        .await;

    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// ReplicationGroup tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_create_replication_group_and_describe() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_replication_group()
        .replication_group_id("my-repl-group")
        .replication_group_description("My test replication group")
        .send()
        .await
        .unwrap();

    let group = create_resp.replication_group().expect("replication group");
    assert_eq!(group.replication_group_id(), Some("my-repl-group"));
    assert_eq!(group.description(), Some("My test replication group"));
    assert_eq!(group.status(), Some("available"));

    // Verify endpoint is populated and reachable
    let node_groups = group.node_groups();
    assert!(!node_groups.is_empty());
    let primary_endpoint = node_groups[0].primary_endpoint().expect("primary endpoint");
    let port = primary_endpoint.port().expect("endpoint port");
    assert!(port > 0);

    // Try a TCP connect to verify Redis is reachable
    let addr = format!("127.0.0.1:{port}");
    let stream = tokio::net::TcpStream::connect(&addr).await;
    assert!(
        stream.is_ok(),
        "should be able to connect to Redis at {addr}"
    );

    // Verify it appears in describe
    let describe_resp = client
        .describe_replication_groups()
        .replication_group_id("my-repl-group")
        .send()
        .await
        .unwrap();

    let groups = describe_resp.replication_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].replication_group_id(), Some("my-repl-group"));
}

#[tokio::test]
async fn elasticache_delete_replication_group_and_verify_gone() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("del-repl-group")
        .replication_group_description("Will be deleted")
        .send()
        .await
        .unwrap();

    client
        .delete_replication_group()
        .replication_group_id("del-repl-group")
        .send()
        .await
        .unwrap();

    // Verify it's gone
    let result = client
        .describe_replication_groups()
        .replication_group_id("del-repl-group")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_create_duplicate_replication_group_errors() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("dup-repl-group")
        .replication_group_description("First")
        .send()
        .await
        .unwrap();

    let result = client
        .create_replication_group()
        .replication_group_id("dup-repl-group")
        .replication_group_description("Second")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_delete_nonexistent_replication_group_errors() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .delete_replication_group()
        .replication_group_id("nonexistent-group")
        .send()
        .await;

    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Existing tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_describe_cache_engine_versions_all() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_cache_engine_versions()
        .send()
        .await
        .unwrap();

    let versions = response.cache_engine_versions();
    assert!(versions.len() >= 2);

    let redis = versions.iter().find(|v| v.engine() == Some("redis"));
    assert!(redis.is_some());
    assert_eq!(redis.unwrap().engine_version(), Some("7.1"));

    let valkey = versions.iter().find(|v| v.engine() == Some("valkey"));
    assert!(valkey.is_some());
    assert_eq!(valkey.unwrap().engine_version(), Some("8.0"));
}

#[tokio::test]
async fn elasticache_describe_cache_engine_versions_filter_by_engine() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_cache_engine_versions()
        .engine("valkey")
        .send()
        .await
        .unwrap();

    let versions = response.cache_engine_versions();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].engine(), Some("valkey"));
    assert_eq!(versions[0].engine_version(), Some("8.0"));
    assert_eq!(versions[0].cache_parameter_group_family(), Some("valkey8"));
}

#[tokio::test]
async fn elasticache_describe_engine_default_parameters_redis7() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_engine_default_parameters()
        .cache_parameter_group_family("redis7")
        .send()
        .await
        .unwrap();

    let defaults = response.engine_defaults().expect("engine defaults");
    assert_eq!(defaults.cache_parameter_group_family(), Some("redis7"));
    let params = defaults.parameters();
    assert_eq!(params.len(), 3);

    let maxmemory = params
        .iter()
        .find(|p| p.parameter_name() == Some("maxmemory-policy"))
        .expect("maxmemory-policy parameter");
    assert_eq!(maxmemory.parameter_value(), Some("volatile-lru"));
    assert_eq!(maxmemory.is_modifiable(), Some(true));
}

#[tokio::test]
async fn elasticache_describe_engine_default_parameters_valkey8() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_engine_default_parameters()
        .cache_parameter_group_family("valkey8")
        .send()
        .await
        .unwrap();

    let defaults = response.engine_defaults().expect("engine defaults");
    assert_eq!(defaults.cache_parameter_group_family(), Some("valkey8"));
    let params = defaults.parameters();
    assert_eq!(params.len(), 3);
}

#[tokio::test]
async fn elasticache_describe_cache_parameter_groups_all() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_cache_parameter_groups()
        .send()
        .await
        .unwrap();

    let groups = response.cache_parameter_groups();
    assert!(groups.len() >= 2);

    let redis_group = groups
        .iter()
        .find(|g| g.cache_parameter_group_name() == Some("default.redis7"));
    assert!(redis_group.is_some());
    assert_eq!(
        redis_group.unwrap().cache_parameter_group_family(),
        Some("redis7")
    );

    let valkey_group = groups
        .iter()
        .find(|g| g.cache_parameter_group_name() == Some("default.valkey8"));
    assert!(valkey_group.is_some());
}

#[tokio::test]
async fn elasticache_describe_cache_parameter_groups_by_name() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_cache_parameter_groups()
        .cache_parameter_group_name("default.redis7")
        .send()
        .await
        .unwrap();

    let groups = response.cache_parameter_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(
        groups[0].cache_parameter_group_name(),
        Some("default.redis7")
    );
}
