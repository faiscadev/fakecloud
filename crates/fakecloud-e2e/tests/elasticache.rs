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

// CacheCluster tests

#[tokio::test]
async fn elasticache_create_cache_cluster_and_describe() {
    if !require_docker_or_skip("elasticache_create_cache_cluster_and_describe") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_cache_cluster()
        .cache_cluster_id("classic-cluster")
        .cache_node_type("cache.t3.micro")
        .preferred_availability_zone("us-east-1a")
        .send()
        .await
        .unwrap();

    let cluster = create_resp.cache_cluster().expect("cache cluster");
    assert_eq!(cluster.cache_cluster_id(), Some("classic-cluster"));
    assert_eq!(cluster.cache_cluster_status(), Some("creating"));
    assert_eq!(cluster.engine(), Some("redis"));
    let arn = cluster.arn().expect("cluster arn").to_string();

    // The backing container starts in the background, so the cluster comes up
    // as "creating"; wait for "available" before reading its endpoint
    // (bug-audit 2026-05-28, 3.2).
    helpers::wait_for_cache_cluster_available(&client, "classic-cluster", 120).await;

    let describe_resp = client
        .describe_cache_clusters()
        .cache_cluster_id("classic-cluster")
        .show_cache_node_info(true)
        .send()
        .await
        .unwrap();

    let clusters = describe_resp.cache_clusters();
    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0].cache_cluster_id(), Some("classic-cluster"));
    assert_eq!(clusters[0].cache_nodes().len(), 1);
    let endpoint = clusters[0].cache_nodes()[0]
        .endpoint()
        .expect("cache node endpoint");
    let port = endpoint.port().expect("endpoint port");
    let addr = format!("127.0.0.1:{port}");
    assert!(tokio::net::TcpStream::connect(&addr).await.is_ok());

    let tag_resp = client
        .add_tags_to_resource()
        .resource_name(arn)
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("env")
                .value("test")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(tag_resp.tag_list().len(), 1);
}

#[tokio::test]
async fn elasticache_describe_cache_clusters_paginates() {
    if !require_docker_or_skip("elasticache_describe_cache_clusters_paginates") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    for group_name in ["page-subnet-a", "page-subnet-b", "page-subnet-c"] {
        client
            .create_cache_subnet_group()
            .cache_subnet_group_name(group_name)
            .cache_subnet_group_description("Pagination test subnet group")
            .subnet_ids("subnet-aaa111")
            .send()
            .await
            .unwrap();
    }

    let first_page = client
        .describe_cache_subnet_groups()
        .max_records(1)
        .send()
        .await
        .unwrap();
    assert_eq!(first_page.cache_subnet_groups().len(), 1);
    let first_marker = first_page.marker().expect("first page marker").to_string();

    let second_page = client
        .describe_cache_subnet_groups()
        .max_records(1)
        .marker(&first_marker)
        .send()
        .await
        .unwrap();
    assert_eq!(second_page.cache_subnet_groups().len(), 1);
    assert_ne!(
        first_page.cache_subnet_groups()[0].cache_subnet_group_name(),
        second_page.cache_subnet_groups()[0].cache_subnet_group_name()
    );

    let mut seen_names: Vec<String> = first_page
        .cache_subnet_groups()
        .iter()
        .chain(second_page.cache_subnet_groups().iter())
        .filter_map(|group| group.cache_subnet_group_name().map(ToOwned::to_owned))
        .collect();
    let mut marker = second_page.marker().map(ToOwned::to_owned);

    while let Some(next_marker) = marker {
        let page = client
            .describe_cache_subnet_groups()
            .max_records(1)
            .marker(next_marker)
            .send()
            .await
            .unwrap();
        seen_names.extend(
            page.cache_subnet_groups()
                .iter()
                .filter_map(|group| group.cache_subnet_group_name().map(ToOwned::to_owned)),
        );
        marker = page.marker().map(ToOwned::to_owned);
    }

    for expected in ["page-subnet-a", "page-subnet-b", "page-subnet-c"] {
        assert!(seen_names.iter().any(|name| name == expected));
    }
}

#[tokio::test]
async fn elasticache_delete_cache_cluster_and_verify_gone() {
    if !require_docker_or_skip("elasticache_delete_cache_cluster_and_verify_gone") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_cluster()
        .cache_cluster_id("delete-cluster")
        .send()
        .await
        .unwrap();

    let delete_resp = client
        .delete_cache_cluster()
        .cache_cluster_id("delete-cluster")
        .send()
        .await
        .unwrap();
    assert_eq!(
        delete_resp
            .cache_cluster()
            .and_then(|cluster| cluster.cache_cluster_status()),
        Some("deleting")
    );

    let result = client
        .describe_cache_clusters()
        .cache_cluster_id("delete-cluster")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_describe_reserved_cache_nodes_is_empty() {
    if !require_docker_or_skip("elasticache_describe_reserved_cache_nodes_is_empty") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client.describe_reserved_cache_nodes().send().await.unwrap();

    assert!(response.reserved_cache_nodes().is_empty());
    assert!(response.marker().is_none());
}

#[tokio::test]
async fn elasticache_describe_reserved_cache_nodes_offerings_filters_and_paginates() {
    if !require_docker_or_skip(
        "elasticache_describe_reserved_cache_nodes_offerings_filters_and_paginates",
    ) {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let filtered = client
        .describe_reserved_cache_nodes_offerings()
        .product_description("redis")
        .duration("3")
        .send()
        .await
        .unwrap();

    let filtered_offerings = filtered.reserved_cache_nodes_offerings();
    assert_eq!(filtered_offerings.len(), 1);
    assert_eq!(filtered_offerings[0].product_description(), Some("redis"));
    assert_eq!(filtered_offerings[0].duration(), Some(94_608_000));

    let first_page = client
        .describe_reserved_cache_nodes_offerings()
        .max_records(1)
        .send()
        .await
        .unwrap();
    assert_eq!(first_page.reserved_cache_nodes_offerings().len(), 1);
    let marker = first_page.marker().expect("first page marker").to_string();

    let next_page = client
        .describe_reserved_cache_nodes_offerings()
        .max_records(1)
        .marker(marker)
        .send()
        .await
        .unwrap();
    assert_eq!(next_page.reserved_cache_nodes_offerings().len(), 1);
}

// CacheSubnetGroup tests

#[tokio::test]
async fn elasticache_create_subnet_group_and_describe() {
    if !require_docker_or_skip("elasticache_create_subnet_group_and_describe") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_describe_subnet_groups_with_name_filter") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_modify_subnet_group_description") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_delete_subnet_group_and_verify_gone") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_create_duplicate_subnet_group_errors") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_delete_nonexistent_subnet_group_errors") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .delete_cache_subnet_group()
        .cache_subnet_group_name("nonexistent-group")
        .send()
        .await;

    assert!(result.is_err());
}

// Tag tests

#[tokio::test]
async fn elasticache_add_and_list_tags_on_subnet_group() {
    if !require_docker_or_skip("elasticache_add_and_list_tags_on_subnet_group") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("tag-e2e-group")
        .cache_subnet_group_description("For tag e2e test")
        .subnet_ids("subnet-aaa111")
        .send()
        .await
        .unwrap();

    let arn = create
        .cache_subnet_group()
        .and_then(|g| g.arn())
        .expect("subnet group arn");

    // Add tags
    let add_resp = client
        .add_tags_to_resource()
        .resource_name(arn)
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("env")
                .value("prod")
                .build(),
        )
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("team")
                .value("backend")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let tags = add_resp.tag_list();
    assert_eq!(tags.len(), 2);

    // List tags
    let list_resp = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();

    let tags = list_resp.tag_list();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key(), Some("env"));
    assert_eq!(tags[0].value(), Some("prod"));
    assert_eq!(tags[1].key(), Some("team"));
    assert_eq!(tags[1].value(), Some("backend"));
}

#[tokio::test]
async fn elasticache_remove_tags_from_subnet_group() {
    if !require_docker_or_skip("elasticache_remove_tags_from_subnet_group") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("tag-remove-group")
        .cache_subnet_group_description("For remove tag test")
        .subnet_ids("subnet-aaa111")
        .send()
        .await
        .unwrap();

    let arn = create
        .cache_subnet_group()
        .and_then(|g| g.arn())
        .expect("subnet group arn");

    client
        .add_tags_to_resource()
        .resource_name(arn)
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("env")
                .value("prod")
                .build(),
        )
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("team")
                .value("backend")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Remove one tag
    client
        .remove_tags_from_resource()
        .resource_name(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    // Verify only "team" remains
    let list_resp = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();

    let tags = list_resp.tag_list();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), Some("team"));
    assert_eq!(tags[0].value(), Some("backend"));
}

#[tokio::test]
async fn elasticache_tag_update_existing_key() {
    if !require_docker_or_skip("elasticache_tag_update_existing_key") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("tag-update-group")
        .cache_subnet_group_description("For tag update test")
        .subnet_ids("subnet-aaa111")
        .send()
        .await
        .unwrap();

    let arn = create
        .cache_subnet_group()
        .and_then(|g| g.arn())
        .expect("subnet group arn");

    // Add initial tag
    client
        .add_tags_to_resource()
        .resource_name(arn)
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("env")
                .value("dev")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Update the tag value
    client
        .add_tags_to_resource()
        .resource_name(arn)
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("env")
                .value("prod")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let list_resp = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();

    let tags = list_resp.tag_list();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), Some("env"));
    assert_eq!(tags[0].value(), Some("prod"));
}

#[tokio::test]
async fn elasticache_tags_on_unknown_arn_errors() {
    if !require_docker_or_skip("elasticache_tags_on_unknown_arn_errors") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .list_tags_for_resource()
        .resource_name("arn:aws:elasticache:us-east-1:123456789012:subnetgroup:nonexistent")
        .send()
        .await;

    assert!(result.is_err());
}

// ReplicationGroup tests

#[tokio::test]
async fn elasticache_create_replication_group_and_describe() {
    if !require_docker_or_skip("elasticache_create_replication_group_and_describe") {
        return;
    }

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
    assert_eq!(group.status(), Some("creating"));

    // Backing container starts in the background; wait for "available", then
    // read the endpoint from the ready group (bug-audit 2026-05-28, 3.2).
    let group = helpers::wait_for_replication_group_available(&client, "my-repl-group", 120).await;

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
    if !require_docker_or_skip("elasticache_delete_replication_group_and_verify_gone") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_create_duplicate_replication_group_errors") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_delete_nonexistent_replication_group_errors") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .delete_replication_group()
        .replication_group_id("nonexistent-group")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_create_replication_group_round_trips_extended_fields() {
    if !require_docker_or_skip("elasticache_create_replication_group_round_trips_extended_fields") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_replication_group()
        .replication_group_id("ext-rg")
        .replication_group_description("extended fields")
        .engine("redis")
        .engine_version("7.1")
        .auth_token("supersecret")
        .transit_encryption_enabled(true)
        .at_rest_encryption_enabled(true)
        .kms_key_id("arn:aws:kms:us-east-1:123456789012:key/abc-123")
        .multi_az_enabled(true)
        .automatic_failover_enabled(true)
        .num_node_groups(3)
        .replicas_per_node_group(1)
        .port(6390)
        .preferred_maintenance_window("sun:23:00-mon:01:30")
        .snapshot_retention_limit(7)
        .snapshot_window("03:00-04:00")
        .send()
        .await
        .unwrap();

    let group = create_resp.replication_group().expect("replication group");
    // Server emits these on the create response too. Verify the fields
    // that AWS exposes via the SDK round-trip from the request.
    assert_eq!(
        group.transit_encryption_enabled(),
        Some(true),
        "TransitEncryptionEnabled must be echoed"
    );
    assert_eq!(
        group.at_rest_encryption_enabled(),
        Some(true),
        "AtRestEncryptionEnabled must be echoed"
    );
    assert_eq!(
        group.kms_key_id(),
        Some("arn:aws:kms:us-east-1:123456789012:key/abc-123")
    );
    assert_eq!(
        group.auth_token_enabled(),
        Some(true),
        "AuthTokenEnabled flips on whenever AuthToken is supplied"
    );
    assert_eq!(group.cluster_enabled(), Some(true));

    // DescribeReplicationGroups should reflect the same persisted fields.
    let describe_resp = client
        .describe_replication_groups()
        .replication_group_id("ext-rg")
        .send()
        .await
        .unwrap();
    let groups = describe_resp.replication_groups();
    assert_eq!(groups.len(), 1);
    let described = &groups[0];
    assert_eq!(described.transit_encryption_enabled(), Some(true));
    assert_eq!(described.at_rest_encryption_enabled(), Some(true));
    assert_eq!(
        described.kms_key_id(),
        Some("arn:aws:kms:us-east-1:123456789012:key/abc-123")
    );
    assert_eq!(described.auth_token_enabled(), Some(true));
    assert_eq!(
        described.multi_az(),
        Some(&aws_sdk_elasticache::types::MultiAzStatus::Enabled)
    );
    assert_eq!(
        described.automatic_failover(),
        Some(&aws_sdk_elasticache::types::AutomaticFailoverStatus::Enabled)
    );
    assert_eq!(described.cluster_enabled(), Some(true));
    assert_eq!(described.engine(), Some("redis"));
    // num_node_groups=3 -> 3 NodeGroups with shard ids 0001..0003.
    let node_groups = described.node_groups();
    assert_eq!(node_groups.len(), 3, "NumNodeGroups must drive shard count");
    let ids: Vec<&str> = node_groups
        .iter()
        .filter_map(|n| n.node_group_id())
        .collect();
    assert!(ids.contains(&"0001"));
    assert!(ids.contains(&"0002"));
    assert!(ids.contains(&"0003"));
    assert_eq!(described.snapshot_retention_limit(), Some(7));
    assert_eq!(described.snapshot_window(), Some("03:00-04:00"));
    // AuthToken is never echoed on describe, even when stored internally.
    // SDK has no field for it, so the test inherently asserts that.
}

#[tokio::test]
async fn elasticache_create_replication_group_round_trips_log_delivery() {
    if !require_docker_or_skip("elasticache_create_replication_group_round_trips_log_delivery") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let cw_details = aws_sdk_elasticache::types::CloudWatchLogsDestinationDetails::builder()
        .log_group("/aws/elasticache/slow")
        .build();
    let dest = aws_sdk_elasticache::types::DestinationDetails::builder()
        .cloud_watch_logs_details(cw_details)
        .build();
    let log_cfg = aws_sdk_elasticache::types::LogDeliveryConfigurationRequest::builder()
        .log_type(aws_sdk_elasticache::types::LogType::SlowLog)
        .destination_type(aws_sdk_elasticache::types::DestinationType::CloudWatchLogs)
        .destination_details(dest)
        .log_format(aws_sdk_elasticache::types::LogFormat::Json)
        .enabled(true)
        .build();

    client
        .create_replication_group()
        .replication_group_id("log-rg")
        .replication_group_description("log delivery round-trip")
        .log_delivery_configurations(log_cfg)
        .send()
        .await
        .unwrap();

    let describe_resp = client
        .describe_replication_groups()
        .replication_group_id("log-rg")
        .send()
        .await
        .unwrap();
    let groups = describe_resp.replication_groups();
    assert_eq!(groups.len(), 1);
    let log_configs = groups[0].log_delivery_configurations();
    assert_eq!(log_configs.len(), 1, "expected one log delivery config");
    let cfg = &log_configs[0];
    assert_eq!(
        cfg.log_type(),
        Some(&aws_sdk_elasticache::types::LogType::SlowLog)
    );
    assert_eq!(
        cfg.destination_type(),
        Some(&aws_sdk_elasticache::types::DestinationType::CloudWatchLogs)
    );
    assert_eq!(
        cfg.log_format(),
        Some(&aws_sdk_elasticache::types::LogFormat::Json)
    );
    let log_group = cfg
        .destination_details()
        .and_then(|d| d.cloud_watch_logs_details())
        .and_then(|c| c.log_group());
    assert_eq!(log_group, Some("/aws/elasticache/slow"));
}

#[tokio::test]
async fn elasticache_global_replication_group_lifecycle() {
    if !require_docker_or_skip("elasticache_global_replication_group_lifecycle") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("global-primary-rg")
        .replication_group_description("Primary for global lifecycle")
        .send()
        .await
        .unwrap();

    let create_resp = client
        .create_global_replication_group()
        .global_replication_group_id_suffix("lifecycle")
        .primary_replication_group_id("global-primary-rg")
        .global_replication_group_description("Lifecycle global group")
        .send()
        .await
        .unwrap();

    let global_group = create_resp
        .global_replication_group()
        .expect("global replication group");
    let global_id = global_group
        .global_replication_group_id()
        .expect("global replication group id")
        .to_string();
    assert_eq!(
        global_group.global_replication_group_description(),
        Some("Lifecycle global group")
    );
    assert_eq!(global_group.members().len(), 1);
    assert_eq!(
        global_group.members()[0].replication_group_id(),
        Some("global-primary-rg")
    );

    let describe_resp = client
        .describe_global_replication_groups()
        .global_replication_group_id(&global_id)
        .show_member_info(true)
        .send()
        .await
        .unwrap();
    assert_eq!(describe_resp.global_replication_groups().len(), 1);
    assert_eq!(
        describe_resp.global_replication_groups()[0].members()[0].role(),
        Some("primary")
    );

    let replication_group = client
        .describe_replication_groups()
        .replication_group_id("global-primary-rg")
        .send()
        .await
        .unwrap()
        .replication_groups()[0]
        .clone();
    let global_info = replication_group
        .global_replication_group_info()
        .expect("global replication group info");
    assert_eq!(
        global_info.global_replication_group_id(),
        Some(global_id.as_str())
    );
    assert_eq!(
        global_info.global_replication_group_member_role(),
        Some("primary")
    );

    let modify_resp = client
        .modify_global_replication_group()
        .global_replication_group_id(&global_id)
        .apply_immediately(true)
        .global_replication_group_description("Updated lifecycle global group")
        .automatic_failover_enabled(true)
        .cache_node_type("cache.m5.large")
        .engine_version("7.2")
        .send()
        .await
        .unwrap();
    assert_eq!(
        modify_resp
            .global_replication_group()
            .and_then(|group| group.global_replication_group_description()),
        Some("Updated lifecycle global group")
    );

    let failover_resp = client
        .failover_global_replication_group()
        .global_replication_group_id(&global_id)
        .primary_region("us-east-1")
        .primary_replication_group_id("global-primary-rg")
        .send()
        .await
        .unwrap();
    assert_eq!(
        failover_resp
            .global_replication_group()
            .map(|group| group.members().len()),
        Some(1)
    );

    let disassociate_resp = client
        .disassociate_global_replication_group()
        .global_replication_group_id(&global_id)
        .replication_group_id("global-primary-rg")
        .replication_group_region("us-east-1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        disassociate_resp
            .global_replication_group()
            .map(|group| group.members().len()),
        Some(1)
    );

    let delete_resp = client
        .delete_global_replication_group()
        .global_replication_group_id(&global_id)
        .retain_primary_replication_group(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        delete_resp
            .global_replication_group()
            .and_then(|group| group.status()),
        Some("deleting")
    );

    let result = client
        .describe_global_replication_groups()
        .global_replication_group_id(global_id)
        .send()
        .await;
    assert!(result.is_err());

    let replication_group = client
        .describe_replication_groups()
        .replication_group_id("global-primary-rg")
        .send()
        .await
        .unwrap()
        .replication_groups()[0]
        .clone();
    assert!(replication_group.global_replication_group_info().is_none());
}

// User tests

#[tokio::test]
async fn elasticache_create_user_and_verify_in_describe() {
    if !require_docker_or_skip("elasticache_create_user_and_verify_in_describe") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_user()
        .user_id("myuser")
        .user_name("myuser")
        .engine("redis")
        .access_string("on ~* +@all")
        .no_password_required(true)
        .send()
        .await
        .unwrap();

    assert_eq!(create_resp.user_id(), Some("myuser"));
    assert_eq!(create_resp.user_name(), Some("myuser"));
    assert_eq!(create_resp.status(), Some("active"));
    assert_eq!(create_resp.engine(), Some("redis"));

    // Verify it appears in describe
    let describe_resp = client
        .describe_users()
        .user_id("myuser")
        .send()
        .await
        .unwrap();

    let users = describe_resp.users();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].user_id(), Some("myuser"));
}

#[tokio::test]
async fn elasticache_delete_user_and_verify_gone() {
    if !require_docker_or_skip("elasticache_delete_user_and_verify_gone") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_user()
        .user_id("deluser")
        .user_name("deluser")
        .engine("redis")
        .access_string("on ~* +@all")
        .no_password_required(true)
        .send()
        .await
        .unwrap();

    client
        .delete_user()
        .user_id("deluser")
        .send()
        .await
        .unwrap();

    // Verify it's gone
    let result = client.describe_users().user_id("deluser").send().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_cannot_delete_default_user() {
    if !require_docker_or_skip("elasticache_cannot_delete_default_user") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client.delete_user().user_id("default").send().await;

    assert!(result.is_err());
}

// UserGroup tests

#[tokio::test]
async fn elasticache_create_user_group_with_user_references() {
    if !require_docker_or_skip("elasticache_create_user_group_with_user_references") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    // Create a user first
    client
        .create_user()
        .user_id("groupuser")
        .user_name("groupuser")
        .engine("redis")
        .access_string("on ~* +@all")
        .no_password_required(true)
        .send()
        .await
        .unwrap();

    let create_resp = client
        .create_user_group()
        .user_group_id("mygroup")
        .engine("redis")
        .user_ids("default")
        .user_ids("groupuser")
        .send()
        .await
        .unwrap();

    assert_eq!(create_resp.user_group_id(), Some("mygroup"));
    assert_eq!(create_resp.status(), Some("active"));
    assert_eq!(create_resp.engine(), Some("redis"));
    let user_ids = create_resp.user_ids();
    assert!(user_ids.contains(&"default".to_string()));
    assert!(user_ids.contains(&"groupuser".to_string()));
}

#[tokio::test]
async fn elasticache_describe_user_groups() {
    if !require_docker_or_skip("elasticache_describe_user_groups") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_user_group()
        .user_group_id("descgroup")
        .engine("redis")
        .user_ids("default")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_user_groups()
        .user_group_id("descgroup")
        .send()
        .await
        .unwrap();

    let groups = response.user_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].user_group_id(), Some("descgroup"));
}

#[tokio::test]
async fn elasticache_delete_user_group() {
    if !require_docker_or_skip("elasticache_delete_user_group") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_user_group()
        .user_group_id("delgroup")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let resp = client
        .delete_user_group()
        .user_group_id("delgroup")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.user_group_id(), Some("delgroup"));

    // Verify it's gone
    let result = client
        .describe_user_groups()
        .user_group_id("delgroup")
        .send()
        .await;

    assert!(result.is_err());
}

// Existing tests

#[tokio::test]
async fn elasticache_describe_cache_engine_versions_all() {
    if !require_docker_or_skip("elasticache_describe_cache_engine_versions_all") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_describe_cache_engine_versions_filter_by_engine") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_describe_engine_default_parameters_redis7") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_describe_engine_default_parameters_valkey8") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_describe_cache_parameter_groups_all") {
        return;
    }

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
    if !require_docker_or_skip("elasticache_describe_cache_parameter_groups_by_name") {
        return;
    }

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

// ReplicationGroup operational tests

#[tokio::test]
async fn elasticache_modify_replication_group_description() {
    if !require_docker_or_skip("elasticache_modify_replication_group_description") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("mod-desc-rg")
        .replication_group_description("Original")
        .send()
        .await
        .unwrap();

    let response = client
        .modify_replication_group()
        .replication_group_id("mod-desc-rg")
        .replication_group_description("Updated description")
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.description(), Some("Updated description"));

    // Verify persistence via describe
    let describe = client
        .describe_replication_groups()
        .replication_group_id("mod-desc-rg")
        .send()
        .await
        .unwrap();

    let groups = describe.replication_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].description(), Some("Updated description"));
}

#[tokio::test]
async fn elasticache_modify_replication_group_kitchen_sink() {
    if !require_docker_or_skip("elasticache_modify_replication_group_kitchen_sink") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    // Seed two user groups so we can both add and remove memberships.
    client
        .create_user_group()
        .user_group_id("ks-ug-add")
        .engine("redis")
        .user_ids("default")
        .send()
        .await
        .unwrap();
    client
        .create_user_group()
        .user_group_id("ks-ug-remove")
        .engine("redis")
        .user_ids("default")
        .send()
        .await
        .unwrap();

    // Bring up the replication group with the existing membership pre-attached
    // so the modify call can prove RemoveUserGroupIds drops it. UserGroupIds
    // is required at create time when AuthToken+TransitEncryption are set.
    client
        .create_replication_group()
        .replication_group_id("ks-mod-rg")
        .replication_group_description("kitchen sink modify base")
        .engine("redis")
        .engine_version("7.1")
        .auth_token("initial-token")
        .transit_encryption_enabled(true)
        .at_rest_encryption_enabled(true)
        .multi_az_enabled(false)
        .automatic_failover_enabled(false)
        .user_group_ids("ks-ug-remove")
        .send()
        .await
        .unwrap();

    let cw_details = aws_sdk_elasticache::types::CloudWatchLogsDestinationDetails::builder()
        .log_group("/aws/elasticache/slow-modified")
        .build();
    let dest = aws_sdk_elasticache::types::DestinationDetails::builder()
        .cloud_watch_logs_details(cw_details)
        .build();
    let log_cfg = aws_sdk_elasticache::types::LogDeliveryConfigurationRequest::builder()
        .log_type(aws_sdk_elasticache::types::LogType::SlowLog)
        .destination_type(aws_sdk_elasticache::types::DestinationType::CloudWatchLogs)
        .destination_details(dest)
        .log_format(aws_sdk_elasticache::types::LogFormat::Json)
        .enabled(true)
        .build();

    let modify_resp = client
        .modify_replication_group()
        .replication_group_id("ks-mod-rg")
        .replication_group_description("kitchen sink modified")
        .apply_immediately(true)
        .auth_token("rotated-token")
        .auth_token_update_strategy(aws_sdk_elasticache::types::AuthTokenUpdateStrategyType::Rotate)
        .transit_encryption_enabled(true)
        .transit_encryption_mode(aws_sdk_elasticache::types::TransitEncryptionMode::Required)
        .multi_az_enabled(true)
        .automatic_failover_enabled(true)
        .ip_discovery(aws_sdk_elasticache::types::IpDiscovery::Ipv6)
        .cluster_mode(aws_sdk_elasticache::types::ClusterMode::Compatible)
        .snapshot_retention_limit(14)
        .snapshot_window("06:00-07:00")
        .preferred_maintenance_window("mon:02:00-mon:03:00")
        .notification_topic_arn("arn:aws:sns:us-east-1:123456789012:rg-events")
        .notification_topic_status("active")
        .cache_parameter_group_name("default.redis7")
        .auto_minor_version_upgrade(false)
        .engine_version("7.1")
        .cache_node_type("cache.r6g.large")
        .user_group_ids_to_add("ks-ug-add")
        .user_group_ids_to_remove("ks-ug-remove")
        .log_delivery_configurations(log_cfg)
        .send()
        .await
        .unwrap();

    // Modify response must echo all updates synchronously.
    let modified = modify_resp.replication_group().expect("replication group");
    assert_eq!(modified.description(), Some("kitchen sink modified"));
    assert_eq!(modified.transit_encryption_enabled(), Some(true));
    assert_eq!(
        modified.transit_encryption_mode(),
        Some(&aws_sdk_elasticache::types::TransitEncryptionMode::Required)
    );
    assert_eq!(
        modified.multi_az(),
        Some(&aws_sdk_elasticache::types::MultiAzStatus::Enabled)
    );
    assert_eq!(
        modified.automatic_failover(),
        Some(&aws_sdk_elasticache::types::AutomaticFailoverStatus::Enabled)
    );
    assert_eq!(
        modified.ip_discovery(),
        Some(&aws_sdk_elasticache::types::IpDiscovery::Ipv6)
    );
    assert_eq!(
        modified.cluster_mode(),
        Some(&aws_sdk_elasticache::types::ClusterMode::Compatible)
    );

    // Re-describe and assert each persisted field.
    let describe = client
        .describe_replication_groups()
        .replication_group_id("ks-mod-rg")
        .send()
        .await
        .unwrap();
    let groups = describe.replication_groups();
    assert_eq!(groups.len(), 1);
    let g = &groups[0];

    assert_eq!(g.description(), Some("kitchen sink modified"));
    assert_eq!(g.snapshot_retention_limit(), Some(14));
    assert_eq!(g.snapshot_window(), Some("06:00-07:00"));
    assert_eq!(g.cache_node_type(), Some("cache.r6g.large"));
    assert_eq!(g.auth_token_enabled(), Some(true));
    assert_eq!(g.transit_encryption_enabled(), Some(true));
    assert_eq!(
        g.transit_encryption_mode(),
        Some(&aws_sdk_elasticache::types::TransitEncryptionMode::Required)
    );
    assert_eq!(g.at_rest_encryption_enabled(), Some(true));
    assert_eq!(g.auto_minor_version_upgrade(), Some(false));
    assert_eq!(
        g.multi_az(),
        Some(&aws_sdk_elasticache::types::MultiAzStatus::Enabled)
    );
    assert_eq!(
        g.automatic_failover(),
        Some(&aws_sdk_elasticache::types::AutomaticFailoverStatus::Enabled)
    );
    assert_eq!(
        g.ip_discovery(),
        Some(&aws_sdk_elasticache::types::IpDiscovery::Ipv6)
    );
    assert_eq!(
        g.cluster_mode(),
        Some(&aws_sdk_elasticache::types::ClusterMode::Compatible)
    );
    assert_eq!(g.cluster_enabled(), Some(true));

    // User-group membership: ks-ug-add added, ks-ug-remove removed.
    let user_group_ids: Vec<&str> = g.user_group_ids().iter().map(String::as_str).collect();
    assert!(
        user_group_ids.contains(&"ks-ug-add"),
        "ks-ug-add must be attached: {user_group_ids:?}"
    );
    assert!(
        !user_group_ids.contains(&"ks-ug-remove"),
        "ks-ug-remove must be detached: {user_group_ids:?}"
    );

    // CacheParameterGroup, NotificationConfiguration, and
    // PreferredMaintenanceWindow are not modeled on the SDK ReplicationGroup
    // type but they are emitted in the raw XML AWS returns. Verify those
    // wire-level fields by querying the endpoint directly.
    let raw = reqwest::Client::new()
        .post(server.endpoint())
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=fake/20260101/us-east-1/elasticache/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .body("Action=DescribeReplicationGroups&Version=2015-02-02&ReplicationGroupId=ks-mod-rg")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        raw.contains("<CacheParameterGroupName>default.redis7</CacheParameterGroupName>"),
        "missing CacheParameterGroup XML: {raw}"
    );
    assert!(
        raw.contains("<TopicArn>arn:aws:sns:us-east-1:123456789012:rg-events</TopicArn>"),
        "missing NotificationConfiguration TopicArn: {raw}"
    );
    assert!(
        raw.contains("<TopicStatus>active</TopicStatus>"),
        "missing NotificationConfiguration TopicStatus: {raw}"
    );
    assert!(
        raw.contains(
            "<PreferredMaintenanceWindow>mon:02:00-mon:03:00</PreferredMaintenanceWindow>"
        ),
        "missing PreferredMaintenanceWindow: {raw}"
    );

    // Log delivery: the modify call replaced the set with one CW Logs entry
    // pointing at the new log group.
    let log_configs = g.log_delivery_configurations();
    assert_eq!(log_configs.len(), 1);
    let cfg = &log_configs[0];
    assert_eq!(
        cfg.log_type(),
        Some(&aws_sdk_elasticache::types::LogType::SlowLog)
    );
    assert_eq!(
        cfg.destination_type(),
        Some(&aws_sdk_elasticache::types::DestinationType::CloudWatchLogs)
    );
    let log_group = cfg
        .destination_details()
        .and_then(|d| d.cloud_watch_logs_details())
        .and_then(|c| c.log_group());
    assert_eq!(log_group, Some("/aws/elasticache/slow-modified"));

    // Reverse-side: ks-ug-remove no longer references the rg, ks-ug-add does.
    let ugs = client
        .describe_user_groups()
        .send()
        .await
        .unwrap()
        .user_groups()
        .to_vec();
    let ug_add = ugs
        .iter()
        .find(|g| g.user_group_id() == Some("ks-ug-add"))
        .expect("ks-ug-add user group");
    let ug_remove = ugs
        .iter()
        .find(|g| g.user_group_id() == Some("ks-ug-remove"))
        .expect("ks-ug-remove user group");
    assert!(ug_add
        .replication_groups()
        .iter()
        .any(|id| id == "ks-mod-rg"));
    assert!(!ug_remove
        .replication_groups()
        .iter()
        .any(|id| id == "ks-mod-rg"));
}

#[tokio::test]
async fn elasticache_modify_replication_group_remove_user_groups_clears_all() {
    if !require_docker_or_skip("elasticache_modify_replication_group_remove_user_groups_clears_all")
    {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_user_group()
        .user_group_id("rm-ug-1")
        .engine("redis")
        .user_ids("default")
        .send()
        .await
        .unwrap();
    client
        .create_user_group()
        .user_group_id("rm-ug-2")
        .engine("redis")
        .user_ids("default")
        .send()
        .await
        .unwrap();

    client
        .create_replication_group()
        .replication_group_id("rm-rg")
        .replication_group_description("remove user groups")
        .auth_token("token")
        .transit_encryption_enabled(true)
        .user_group_ids("rm-ug-1")
        .user_group_ids("rm-ug-2")
        .send()
        .await
        .unwrap();

    client
        .modify_replication_group()
        .replication_group_id("rm-rg")
        .remove_user_groups(true)
        .send()
        .await
        .unwrap();

    let g = client
        .describe_replication_groups()
        .replication_group_id("rm-rg")
        .send()
        .await
        .unwrap()
        .replication_groups()
        .first()
        .cloned()
        .expect("rg");
    assert!(g.user_group_ids().is_empty());
}

#[tokio::test]
async fn elasticache_modify_replication_group_auth_token_delete_clears() {
    if !require_docker_or_skip("elasticache_modify_replication_group_auth_token_delete_clears") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("auth-del-rg")
        .replication_group_description("delete auth token")
        .auth_token("initial")
        .transit_encryption_enabled(true)
        .send()
        .await
        .unwrap();

    let resp = client
        .modify_replication_group()
        .replication_group_id("auth-del-rg")
        .auth_token_update_strategy(aws_sdk_elasticache::types::AuthTokenUpdateStrategyType::Delete)
        .send()
        .await
        .unwrap();

    let g = resp.replication_group().expect("rg");
    assert_eq!(g.auth_token_enabled(), Some(false));
}

#[tokio::test]
async fn elasticache_modify_replication_group_rejects_invalid_cluster_mode() {
    if !require_docker_or_skip("elasticache_modify_replication_group_rejects_invalid_cluster_mode")
    {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("bad-cm-rg")
        .replication_group_description("invalid cluster mode")
        .send()
        .await
        .unwrap();

    // SDK accepts unknown enum strings via Unknown(_); send raw via the
    // customizable ModifyReplicationGroup path is overkill, so instead use a
    // raw HTTP request for the invalid path. The build-time enum on the SDK
    // prevents passing arbitrary strings through the typed builder.
    let raw = reqwest::Client::new()
        .post(server.endpoint())
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=fake/20260101/us-east-1/elasticache/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(
            "Action=ModifyReplicationGroup\
             &Version=2015-02-02\
             &ReplicationGroupId=bad-cm-rg\
             &ClusterMode=wat",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(raw.status().as_u16(), 400);
    let body = raw.text().await.unwrap();
    assert!(body.contains("InvalidParameterValue"), "body: {body}");
    assert!(body.contains("ClusterMode"), "body: {body}");
}

#[tokio::test]
async fn elasticache_increase_replica_count() {
    if !require_docker_or_skip("elasticache_increase_replica_count") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("inc-rg")
        .replication_group_description("For increase test")
        .send()
        .await
        .unwrap();

    let response = client
        .increase_replica_count()
        .replication_group_id("inc-rg")
        .new_replica_count(2)
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.member_clusters().len(), 3);
    assert_eq!(group.member_clusters()[0], "inc-rg-001");
    assert_eq!(group.member_clusters()[1], "inc-rg-002");
    assert_eq!(group.member_clusters()[2], "inc-rg-003");
}

#[tokio::test]
async fn elasticache_decrease_replica_count() {
    if !require_docker_or_skip("elasticache_decrease_replica_count") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("dec-rg")
        .replication_group_description("For decrease test")
        .num_cache_clusters(3)
        .send()
        .await
        .unwrap();

    let response = client
        .decrease_replica_count()
        .replication_group_id("dec-rg")
        .new_replica_count(1)
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.member_clusters().len(), 2);
    assert_eq!(group.member_clusters()[0], "dec-rg-001");
    assert_eq!(group.member_clusters()[1], "dec-rg-002");
}

#[tokio::test]
async fn elasticache_test_failover() {
    if !require_docker_or_skip("elasticache_test_failover") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("fo-rg")
        .replication_group_description("For failover test")
        .send()
        .await
        .unwrap();
    helpers::wait_for_replication_group_available(&client, "fo-rg", 120).await;

    let response = client
        .test_failover()
        .replication_group_id("fo-rg")
        .node_group_id("0001")
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.replication_group_id(), Some("fo-rg"));
    assert_eq!(group.status(), Some("available"));
}

// Snapshot tests

#[tokio::test]
async fn elasticache_create_snapshot_and_describe() {
    if !require_docker_or_skip("elasticache_create_snapshot_and_describe") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("snap-rg")
        .replication_group_description("For snapshot test")
        .send()
        .await
        .unwrap();

    let create_resp = client
        .create_snapshot()
        .snapshot_name("my-snapshot")
        .replication_group_id("snap-rg")
        .send()
        .await
        .unwrap();

    let snapshot = create_resp.snapshot().expect("snapshot");
    assert_eq!(snapshot.snapshot_name(), Some("my-snapshot"));
    assert_eq!(snapshot.replication_group_id(), Some("snap-rg"));
    assert_eq!(
        snapshot.replication_group_description(),
        Some("For snapshot test")
    );
    assert_eq!(snapshot.engine(), Some("redis"));
    assert_eq!(snapshot.snapshot_source(), Some("manual"));

    // Verify it appears in describe
    let describe_resp = client
        .describe_snapshots()
        .snapshot_name("my-snapshot")
        .send()
        .await
        .unwrap();

    let snapshots = describe_resp.snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].snapshot_name(), Some("my-snapshot"));
}

#[tokio::test]
async fn elasticache_describe_snapshots_with_replication_group_filter() {
    if !require_docker_or_skip("elasticache_describe_snapshots_with_replication_group_filter") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("filt-snap-rg")
        .replication_group_description("For filter test")
        .send()
        .await
        .unwrap();

    client
        .create_snapshot()
        .snapshot_name("filt-snap-1")
        .replication_group_id("filt-snap-rg")
        .send()
        .await
        .unwrap();

    client
        .create_snapshot()
        .snapshot_name("filt-snap-2")
        .replication_group_id("filt-snap-rg")
        .send()
        .await
        .unwrap();

    // Filter by replication group
    let response = client
        .describe_snapshots()
        .replication_group_id("filt-snap-rg")
        .send()
        .await
        .unwrap();

    let snapshots = response.snapshots();
    assert_eq!(snapshots.len(), 2);

    // Filter by non-matching group returns empty
    let response = client
        .describe_snapshots()
        .replication_group_id("nonexistent-rg")
        .send()
        .await
        .unwrap();

    assert!(response.snapshots().is_empty());
}

#[tokio::test]
async fn elasticache_delete_snapshot_and_verify_gone() {
    if !require_docker_or_skip("elasticache_delete_snapshot_and_verify_gone") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("del-snap-rg")
        .replication_group_description("For delete snapshot test")
        .send()
        .await
        .unwrap();

    client
        .create_snapshot()
        .snapshot_name("del-snapshot")
        .replication_group_id("del-snap-rg")
        .send()
        .await
        .unwrap();

    let resp = client
        .delete_snapshot()
        .snapshot_name("del-snapshot")
        .send()
        .await
        .unwrap();

    let snapshot = resp.snapshot().expect("snapshot");
    assert_eq!(snapshot.snapshot_name(), Some("del-snapshot"));

    // Verify it's gone
    let result = client
        .describe_snapshots()
        .snapshot_name("del-snapshot")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_create_duplicate_snapshot_errors() {
    if !require_docker_or_skip("elasticache_create_duplicate_snapshot_errors") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("dup-snap-rg")
        .replication_group_description("For dup snapshot test")
        .send()
        .await
        .unwrap();

    client
        .create_snapshot()
        .snapshot_name("dup-snapshot")
        .replication_group_id("dup-snap-rg")
        .send()
        .await
        .unwrap();

    let result = client
        .create_snapshot()
        .snapshot_name("dup-snapshot")
        .replication_group_id("dup-snap-rg")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_delete_nonexistent_snapshot_errors() {
    if !require_docker_or_skip("elasticache_delete_nonexistent_snapshot_errors") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .delete_snapshot()
        .snapshot_name("nonexistent-snapshot")
        .send()
        .await;

    assert!(result.is_err());
}

// ServerlessCache tests

#[tokio::test]
async fn elasticache_create_serverless_cache_and_describe() {
    if !require_docker_or_skip("elasticache_create_serverless_cache_and_describe") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_serverless_cache()
        .serverless_cache_name("serverless-main")
        .engine("redis")
        .description("Main serverless cache")
        .security_group_ids("sg-123")
        .subnet_ids("subnet-123")
        .snapshot_retention_limit(7)
        .daily_snapshot_time("04:00")
        .send()
        .await
        .unwrap();

    let cache = create_resp.serverless_cache().expect("serverless cache");
    assert_eq!(cache.serverless_cache_name(), Some("serverless-main"));
    assert_eq!(cache.status(), Some("creating"));
    // Backing container starts in the background; wait for "available" then read
    // the endpoint from the ready cache (bug-audit 2026-05-28, 3.2).
    let cache = helpers::wait_for_serverless_cache_available(&client, "serverless-main", 120).await;
    let endpoint = cache.endpoint().expect("endpoint");
    let addr = endpoint.address().expect("endpoint address");
    let port = endpoint.port().expect("endpoint port");
    assert_eq!(addr, "127.0.0.1");
    assert!(tokio::net::TcpStream::connect(format!("{addr}:{port}"))
        .await
        .is_ok());

    let describe_resp = client
        .describe_serverless_caches()
        .serverless_cache_name("serverless-main")
        .send()
        .await
        .unwrap();
    let caches = describe_resp.serverless_caches();
    assert_eq!(caches.len(), 1);
    assert_eq!(caches[0].serverless_cache_name(), Some("serverless-main"));
}

#[tokio::test]
async fn elasticache_describe_serverless_caches_paginates() {
    if !require_docker_or_skip("elasticache_describe_serverless_caches_paginates") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    for name in [
        "page-serverless-a",
        "page-serverless-b",
        "page-serverless-c",
    ] {
        client
            .create_serverless_cache()
            .serverless_cache_name(name)
            .engine("redis")
            .send()
            .await
            .unwrap();
    }

    let first = client
        .describe_serverless_caches()
        .max_results(1)
        .send()
        .await
        .unwrap();
    assert_eq!(first.serverless_caches().len(), 1);
    let next_token = first.next_token().expect("next token").to_string();

    let second = client
        .describe_serverless_caches()
        .max_results(1)
        .next_token(next_token)
        .send()
        .await
        .unwrap();
    assert_eq!(second.serverless_caches().len(), 1);
}

#[tokio::test]
async fn elasticache_modify_serverless_cache_updates_fields() {
    if !require_docker_or_skip("elasticache_modify_serverless_cache_updates_fields") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("serverless-mod")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let resp = client
        .modify_serverless_cache()
        .serverless_cache_name("serverless-mod")
        .description("Updated serverless cache")
        .security_group_ids("sg-999")
        .snapshot_retention_limit(10)
        .daily_snapshot_time("05:00")
        .send()
        .await
        .unwrap();

    let cache = resp.serverless_cache().expect("serverless cache");
    assert_eq!(cache.description(), Some("Updated serverless cache"));
    assert_eq!(cache.snapshot_retention_limit(), Some(10));
    assert_eq!(cache.daily_snapshot_time(), Some("05:00"));

    // Verify security groups via describe (modify response may not include all list fields)
    let desc = client
        .describe_serverless_caches()
        .serverless_cache_name("serverless-mod")
        .send()
        .await
        .unwrap();
    let described = &desc.serverless_caches()[0];
    assert_eq!(described.security_group_ids(), ["sg-999"]);
}

#[tokio::test]
async fn elasticache_delete_serverless_cache_and_verify_gone() {
    if !require_docker_or_skip("elasticache_delete_serverless_cache_and_verify_gone") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("serverless-del")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let delete_resp = client
        .delete_serverless_cache()
        .serverless_cache_name("serverless-del")
        .send()
        .await
        .unwrap();
    assert_eq!(
        delete_resp
            .serverless_cache()
            .and_then(|cache| cache.status()),
        Some("deleting")
    );

    let result = client
        .describe_serverless_caches()
        .serverless_cache_name("serverless-del")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_create_serverless_cache_rejects_invalid_engine() {
    if !require_docker_or_skip("elasticache_create_serverless_cache_rejects_invalid_engine") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .create_serverless_cache()
        .serverless_cache_name("bad-serverless")
        .engine("memcached")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_create_serverless_cache_snapshot_and_describe() {
    if !require_docker_or_skip("elasticache_create_serverless_cache_snapshot_and_describe") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("serverless-snap")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let create_resp = client
        .create_serverless_cache_snapshot()
        .serverless_cache_name("serverless-snap")
        .serverless_cache_snapshot_name("serverless-snapshot-1")
        .send()
        .await
        .unwrap();

    let snapshot = create_resp
        .serverless_cache_snapshot()
        .expect("serverless cache snapshot");
    assert_eq!(
        snapshot.serverless_cache_snapshot_name(),
        Some("serverless-snapshot-1")
    );

    let describe_resp = client
        .describe_serverless_cache_snapshots()
        .serverless_cache_name("serverless-snap")
        .send()
        .await
        .unwrap();
    let snapshots = describe_resp.serverless_cache_snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(
        snapshots[0].serverless_cache_snapshot_name(),
        Some("serverless-snapshot-1")
    );
}

#[tokio::test]
async fn elasticache_describe_serverless_cache_snapshots_paginates() {
    if !require_docker_or_skip("elasticache_describe_serverless_cache_snapshots_paginates") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("serverless-snap-pages")
        .engine("redis")
        .send()
        .await
        .unwrap();
    for name in ["serverless-page-snap-a", "serverless-page-snap-b"] {
        client
            .create_serverless_cache_snapshot()
            .serverless_cache_name("serverless-snap-pages")
            .serverless_cache_snapshot_name(name)
            .send()
            .await
            .unwrap();
    }

    let first = client
        .describe_serverless_cache_snapshots()
        .serverless_cache_name("serverless-snap-pages")
        .max_results(1)
        .send()
        .await
        .unwrap();
    assert_eq!(first.serverless_cache_snapshots().len(), 1);
    let next_token = first.next_token().expect("next token").to_string();

    let second = client
        .describe_serverless_cache_snapshots()
        .serverless_cache_name("serverless-snap-pages")
        .max_results(1)
        .next_token(next_token)
        .send()
        .await
        .unwrap();
    assert_eq!(second.serverless_cache_snapshots().len(), 1);
}

#[tokio::test]
async fn elasticache_delete_serverless_cache_snapshot_and_verify_gone() {
    if !require_docker_or_skip("elasticache_delete_serverless_cache_snapshot_and_verify_gone") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("serverless-snap-del")
        .engine("redis")
        .send()
        .await
        .unwrap();
    client
        .create_serverless_cache_snapshot()
        .serverless_cache_name("serverless-snap-del")
        .serverless_cache_snapshot_name("serverless-snapshot-del")
        .send()
        .await
        .unwrap();

    let delete_resp = client
        .delete_serverless_cache_snapshot()
        .serverless_cache_snapshot_name("serverless-snapshot-del")
        .send()
        .await
        .unwrap();
    assert_eq!(
        delete_resp
            .serverless_cache_snapshot()
            .and_then(|snapshot| snapshot.status()),
        Some("deleting")
    );

    let result = client
        .describe_serverless_cache_snapshots()
        .serverless_cache_snapshot_name("serverless-snapshot-del")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_delete_nonexistent_serverless_cache_snapshot_errors() {
    if !require_docker_or_skip("elasticache_delete_nonexistent_serverless_cache_snapshot_errors") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .delete_serverless_cache_snapshot()
        .serverless_cache_snapshot_name("missing-serverless-snapshot")
        .send()
        .await;

    assert!(result.is_err());
}

// Memcached engine tests

#[tokio::test]
async fn elasticache_create_memcached_cluster_and_connect() {
    if !require_docker_or_skip("elasticache_create_memcached_cluster_and_connect") {
        return;
    }

    if !docker_available() {
        return;
    }
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_cache_cluster()
        .cache_cluster_id("mc-cluster")
        .cache_node_type("cache.t3.micro")
        .engine("memcached")
        .send()
        .await
        .unwrap();

    let cluster = create_resp.cache_cluster().expect("cache cluster");
    assert_eq!(cluster.engine(), Some("memcached"));
    assert_eq!(cluster.engine_version(), Some("1.6.22"));
    assert_eq!(cluster.cache_cluster_status(), Some("creating"));

    // Backing container starts in the background; wait before reading endpoint.
    helpers::wait_for_cache_cluster_available(&client, "mc-cluster", 120).await;

    let describe = client
        .describe_cache_clusters()
        .cache_cluster_id("mc-cluster")
        .show_cache_node_info(true)
        .send()
        .await
        .unwrap();
    let endpoint = describe.cache_clusters()[0].cache_nodes()[0]
        .endpoint()
        .expect("endpoint");
    let port = endpoint.port().expect("port");

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .expect("memcached connect");
    stream.write_all(b"set foo 0 0 3\r\nbar\r\n").await.unwrap();
    let mut buf = vec![0u8; 32];
    let n = stream.read(&mut buf).await.unwrap();
    assert!(
        std::str::from_utf8(&buf[..n])
            .unwrap()
            .starts_with("STORED"),
        "memcached SET should return STORED"
    );
    stream.write_all(b"get foo\r\n").await.unwrap();
    let mut buf = vec![0u8; 64];
    let n = stream.read(&mut buf).await.unwrap();
    let response = std::str::from_utf8(&buf[..n]).unwrap();
    assert!(response.contains("bar"), "expected bar in {response}");

    client
        .delete_cache_cluster()
        .cache_cluster_id("mc-cluster")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn elasticache_memcached_replication_group_rejected() {
    if !require_docker_or_skip("elasticache_memcached_replication_group_rejected") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .create_replication_group()
        .replication_group_id("mc-rg")
        .replication_group_description("memcached should be rejected")
        .engine("memcached")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn elasticache_describe_engine_default_parameters_memcached() {
    if !require_docker_or_skip("elasticache_describe_engine_default_parameters_memcached") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_engine_default_parameters()
        .cache_parameter_group_family("memcached1.6")
        .send()
        .await
        .unwrap();

    let defaults = response.engine_defaults().expect("engine defaults");
    assert_eq!(
        defaults.cache_parameter_group_family(),
        Some("memcached1.6")
    );
    let params = defaults.parameters();
    assert_eq!(params.len(), 2);
    assert!(params
        .iter()
        .any(|p| p.parameter_name() == Some("max_item_size")));
}

#[tokio::test]
async fn elasticache_describe_cache_engine_versions_includes_memcached() {
    if !require_docker_or_skip("elasticache_describe_cache_engine_versions_includes_memcached") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_cache_engine_versions()
        .engine("memcached")
        .send()
        .await
        .unwrap();

    let versions = response.cache_engine_versions();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].engine(), Some("memcached"));
    assert_eq!(versions[0].engine_version(), Some("1.6.22"));
    assert_eq!(
        versions[0].cache_parameter_group_family(),
        Some("memcached1.6")
    );
}

#[tokio::test]
async fn elasticache_create_cache_cluster_round_trips_extended_fields() {
    if !require_docker_or_skip("elasticache_create_cache_cluster_round_trips_extended_fields") {
        return;
    }

    // Kitchen-sink CreateCacheCluster: every documented input AWS supports
    // through the SDK builder. Asserts the create + describe responses echo
    // every field that maps to a slot on the CacheCluster shape, and that
    // input-only fields persist on the in-memory state via subsequent SDK
    // calls (ListTagsForResource).
    if !docker_available() {
        return;
    }
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_cache_cluster()
        .cache_cluster_id("ext-cc")
        .engine("redis")
        .engine_version("7.1")
        .cache_node_type("cache.t3.micro")
        .num_cache_nodes(1)
        .cache_parameter_group_name("default.redis7")
        .cache_subnet_group_name("default")
        .security_group_ids("sg-aaa")
        .security_group_ids("sg-bbb")
        .port(6390)
        .preferred_maintenance_window("sun:05:00-sun:09:00")
        .preferred_availability_zone("us-east-1a")
        .auto_minor_version_upgrade(false)
        .notification_topic_arn("arn:aws:sns:us-east-1:123456789012:topic")
        .auth_token("supersecret-XYZ")
        .transit_encryption_enabled(true)
        .network_type(aws_sdk_elasticache::types::NetworkType::Ipv4)
        .ip_discovery(aws_sdk_elasticache::types::IpDiscovery::Ipv4)
        .outpost_mode(aws_sdk_elasticache::types::OutpostMode::SingleOutpost)
        .preferred_outpost_arn("arn:aws:outposts:us-east-1:123456789012:outpost/op-abc")
        .snapshot_retention_limit(7)
        .snapshot_window("03:00-05:00")
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("team")
                .value("platform")
                .build(),
        )
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("env")
                .value("prod")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let cluster = create_resp.cache_cluster().expect("cache cluster");
    assert_eq!(cluster.cache_cluster_id(), Some("ext-cc"));
    assert_eq!(cluster.engine(), Some("redis"));
    assert_eq!(cluster.engine_version(), Some("7.1"));
    assert_eq!(cluster.cache_node_type(), Some("cache.t3.micro"));
    assert_eq!(cluster.num_cache_nodes(), Some(1));
    assert_eq!(cluster.transit_encryption_enabled(), Some(true));
    assert_eq!(cluster.auth_token_enabled(), Some(true));
    assert_eq!(cluster.auto_minor_version_upgrade(), Some(false));
    assert_eq!(cluster.cache_subnet_group_name(), Some("default"));
    let arn = cluster.arn().expect("cluster arn").to_string();

    // DescribeCacheClusters should reflect the same persisted fields.
    let describe = client
        .describe_cache_clusters()
        .cache_cluster_id("ext-cc")
        .send()
        .await
        .unwrap();
    let clusters = describe.cache_clusters();
    assert_eq!(clusters.len(), 1);
    let described = &clusters[0];
    assert_eq!(described.engine(), Some("redis"));
    assert_eq!(described.engine_version(), Some("7.1"));
    assert_eq!(described.transit_encryption_enabled(), Some(true));
    assert_eq!(described.auth_token_enabled(), Some(true));
    assert_eq!(described.auto_minor_version_upgrade(), Some(false));
    assert_eq!(described.cache_subnet_group_name(), Some("default"));
    let cache_param_group = described
        .cache_parameter_group()
        .expect("cache parameter group");
    assert_eq!(
        cache_param_group.cache_parameter_group_name(),
        Some("default.redis7")
    );
    let security_groups = described.security_groups();
    let sg_ids: Vec<&str> = security_groups
        .iter()
        .filter_map(|sg| sg.security_group_id())
        .collect();
    assert!(sg_ids.contains(&"sg-aaa"));
    assert!(sg_ids.contains(&"sg-bbb"));
    let notification = described
        .notification_configuration()
        .expect("notification configuration");
    assert_eq!(
        notification.topic_arn(),
        Some("arn:aws:sns:us-east-1:123456789012:topic")
    );
    assert_eq!(described.snapshot_retention_limit(), Some(7));
    assert_eq!(described.snapshot_window(), Some("03:00-05:00"));
    assert_eq!(
        described.preferred_maintenance_window(),
        Some("sun:05:00-sun:09:00")
    );
    assert_eq!(described.preferred_availability_zone(), Some("us-east-1a"));
    assert_eq!(
        described.preferred_outpost_arn(),
        Some("arn:aws:outposts:us-east-1:123456789012:outpost/op-abc")
    );

    // Tags supplied at create time must be visible via ListTagsForResource.
    let list_tags = client
        .list_tags_for_resource()
        .resource_name(&arn)
        .send()
        .await
        .unwrap();
    let tag_list = list_tags.tag_list();
    let tag_pairs: std::collections::BTreeMap<String, String> = tag_list
        .iter()
        .filter_map(|t| {
            t.key()
                .and_then(|k| t.value().map(|v| (k.to_string(), v.to_string())))
        })
        .collect();
    assert_eq!(tag_pairs.get("team").map(String::as_str), Some("platform"));
    assert_eq!(tag_pairs.get("env").map(String::as_str), Some("prod"));

    // AuthToken must never be echoed back through the response. Re-check the
    // full describe XML body via the SDK by inspecting the persisted fields
    // — AuthToken is intentionally absent from the SDK's CacheCluster shape
    // (only AuthTokenEnabled is exposed), so we just confirm the bool.

    client
        .delete_cache_cluster()
        .cache_cluster_id("ext-cc")
        .send()
        .await
        .unwrap();
}

// ── Replica + shard-count mutation tests ──

#[tokio::test]
async fn elasticache_modify_replication_group_shard_configuration_changes_shard_count() {
    if !require_docker_or_skip(
        "elasticache_modify_replication_group_shard_configuration_changes_shard_count",
    ) {
        return;
    }

    if !docker_available() {
        return;
    }
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("shard-rg")
        .replication_group_description("multi-shard")
        .num_node_groups(2)
        .replicas_per_node_group(1)
        .send()
        .await
        .unwrap();

    // Increase shard count: 2 -> 3.
    client
        .modify_replication_group_shard_configuration()
        .replication_group_id("shard-rg")
        .node_group_count(3)
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let described = client
        .describe_replication_groups()
        .replication_group_id("shard-rg")
        .send()
        .await
        .unwrap();
    let group = &described.replication_groups()[0];
    assert_eq!(
        group.node_groups().len(),
        3,
        "ModifyReplicationGroupShardConfiguration must update NodeGroups"
    );
    let ids: Vec<&str> = group
        .node_groups()
        .iter()
        .filter_map(|n| n.node_group_id())
        .collect();
    assert!(ids.contains(&"0001"));
    assert!(ids.contains(&"0002"));
    assert!(ids.contains(&"0003"));

    // Decrease shard count: 3 -> 2 with NodeGroupsToRetain.
    client
        .modify_replication_group_shard_configuration()
        .replication_group_id("shard-rg")
        .node_group_count(2)
        .apply_immediately(true)
        .node_groups_to_retain("0001")
        .node_groups_to_retain("0002")
        .send()
        .await
        .unwrap();

    let described = client
        .describe_replication_groups()
        .replication_group_id("shard-rg")
        .send()
        .await
        .unwrap();
    assert_eq!(described.replication_groups()[0].node_groups().len(), 2);
}

#[tokio::test]
async fn elasticache_modify_replication_group_shard_configuration_rejects_non_cluster_change() {
    if !require_docker_or_skip(
        "elasticache_modify_replication_group_shard_configuration_rejects_non_cluster_change",
    ) {
        return;
    }

    if !docker_available() {
        return;
    }
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("single-rg")
        .replication_group_description("single shard")
        .send()
        .await
        .unwrap();

    // Non-cluster mode: NodeGroupCount must stay 1.
    let result = client
        .modify_replication_group_shard_configuration()
        .replication_group_id("single-rg")
        .node_group_count(3)
        .apply_immediately(true)
        .send()
        .await;
    assert!(
        result.is_err(),
        "non-cluster replication group should reject NodeGroupCount change"
    );
}

#[tokio::test]
async fn elasticache_increase_replica_count_per_shard() {
    if !require_docker_or_skip("elasticache_increase_replica_count_per_shard") {
        return;
    }

    if !docker_available() {
        return;
    }
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("rep-up-rg")
        .replication_group_description("replica scale up")
        .num_node_groups(2)
        .replicas_per_node_group(1)
        .send()
        .await
        .unwrap();

    client
        .increase_replica_count()
        .replication_group_id("rep-up-rg")
        .apply_immediately(true)
        .new_replica_count(2)
        .send()
        .await
        .unwrap();

    let described = client
        .describe_replication_groups()
        .replication_group_id("rep-up-rg")
        .send()
        .await
        .unwrap();
    let group = &described.replication_groups()[0];
    // 2 shards × (1 primary + 2 replicas) = 6 member clusters.
    assert_eq!(
        group.member_clusters().len(),
        6,
        "IncreaseReplicaCount must rebuild member_clusters per shard"
    );
}

#[tokio::test]
async fn elasticache_decrease_replica_count_per_shard() {
    if !require_docker_or_skip("elasticache_decrease_replica_count_per_shard") {
        return;
    }

    if !docker_available() {
        return;
    }
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("rep-dn-rg")
        .replication_group_description("replica scale down")
        .num_node_groups(2)
        .replicas_per_node_group(2)
        .send()
        .await
        .unwrap();

    client
        .decrease_replica_count()
        .replication_group_id("rep-dn-rg")
        .apply_immediately(true)
        .new_replica_count(1)
        .send()
        .await
        .unwrap();

    let described = client
        .describe_replication_groups()
        .replication_group_id("rep-dn-rg")
        .send()
        .await
        .unwrap();
    let group = &described.replication_groups()[0];
    // 2 shards × (1 primary + 1 replica) = 4 member clusters.
    assert_eq!(group.member_clusters().len(), 4);
}

#[tokio::test]
async fn elasticache_memcached_cluster_emits_configuration_endpoint() {
    if !require_docker_or_skip("elasticache_memcached_cluster_emits_configuration_endpoint") {
        return;
    }

    if !docker_available() {
        return;
    }
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create_resp = client
        .create_cache_cluster()
        .cache_cluster_id("memc-config")
        .engine("memcached")
        .cache_node_type("cache.t3.micro")
        .send()
        .await
        .unwrap();

    assert_eq!(
        create_resp.cache_cluster().and_then(|c| c.engine()),
        Some("memcached")
    );
    // Backing container starts in the background; the configuration endpoint
    // port is filled in once it is ready (bug-audit 2026-05-28, 3.2).
    let cluster = helpers::wait_for_cache_cluster_available(&client, "memc-config", 120).await;
    let ep = cluster
        .configuration_endpoint()
        .expect("memcached cluster must emit ConfigurationEndpoint");
    assert!(!ep.address().unwrap_or("").is_empty());
    assert!(ep.port().unwrap_or(0) > 0);
}

/// Probe a Redis/Valkey container to check whether it supports the ACL
/// command set.  Some CI environments ship stripped-down builds that
/// lack ACL support, so we skip the wire-level assertion rather than
/// hard-failing.
async fn redis_acls_supported(addr: &str) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await else {
        return false;
    };
    // Correct RESP array: ACL LIST is two elements, not one.
    if stream
        .write_all(b"*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n")
        .await
        .is_err()
    {
        return false;
    }
    let mut buf = vec![0u8; 1024];
    let Ok(n) = stream.read(&mut buf).await else {
        return false;
    };
    let response = String::from_utf8_lossy(&buf[..n]);
    !response.contains("unknown command")
}

/// Probe a Redis/Valkey container to check whether it supports CONFIG
/// commands including CONFIG SET.  Stripped-down builds may lack CONFIG
/// support, and some hardened images allow GET but not SET.  The probe uses
/// `tcp-keepalive` (default 300) rather than `maxmemory-policy` so it does
/// not mutate the value the test later asserts.
async fn redis_config_supported(addr: &str) -> bool {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Step 1: CONFIG SET tcp-keepalive 301 (value different from default 300).
    let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await else {
        return false;
    };
    if stream
        .write_all(b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\ntcp-keepalive\r\n$3\r\n301\r\n")
        .await
        .is_err()
    {
        return false;
    }
    let mut buf = vec![0u8; 1024];
    let Ok(n) = stream.read(&mut buf).await else {
        return false;
    };
    let response = String::from_utf8_lossy(&buf[..n]);
    if response.contains("unknown command") || response.contains("ERR") {
        return false;
    }

    // Step 2: CONFIG GET tcp-keepalive — verify the value actually changed.
    let Ok(mut stream) = tokio::net::TcpStream::connect(addr).await else {
        return false;
    };
    if stream
        .write_all(b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\ntcp-keepalive\r\n")
        .await
        .is_err()
    {
        return false;
    }
    let mut buf = vec![0u8; 1024];
    let Ok(n) = stream.read(&mut buf).await else {
        return false;
    };
    let response = String::from_utf8_lossy(&buf[..n]);
    let works = response.contains("301");

    // Step 3: restore default so the probe is non-destructive.
    if works {
        if let Ok(mut s) = tokio::net::TcpStream::connect(addr).await {
            let _ = s
                .write_all(
                    b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$14\r\ntcp-keepalive\r\n$3\r\n300\r\n",
                )
                .await;
        }
    }
    works
}

#[tokio::test]
async fn elasticache_modify_user_applies_acl_to_running_redis() {
    if !require_docker_or_skip("elasticache_modify_user_applies_acl_to_running_redis") {
        return;
    }

    if !docker_available() {
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_user()
        .user_id("acluser")
        .user_name("acluser")
        .engine("redis")
        .access_string("on ~* +@all")
        .send()
        .await
        .unwrap();

    client
        .create_user_group()
        .user_group_id("aclgroup")
        .engine("redis")
        .user_ids("acluser")
        .send()
        .await
        .unwrap();

    client
        .create_replication_group()
        .replication_group_id("acl-rg")
        .replication_group_description("acl test")
        .user_group_ids("aclgroup")
        .send()
        .await
        .unwrap();

    // Backing container starts in the background; wait for the endpoint port.
    let rg = helpers::wait_for_replication_group_available(&client, "acl-rg", 120).await;
    let port = rg
        .node_groups()
        .first()
        .expect("node group")
        .primary_endpoint()
        .expect("primary endpoint")
        .port()
        .unwrap_or(0) as u16;

    client
        .modify_user()
        .user_id("acluser")
        .access_string("on ~key* +get")
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let addr = format!("127.0.0.1:{port}");
    if !redis_acls_supported(&addr).await {
        // Container runtime lacks ACL support — skip wire-level check.
        return;
    }

    let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
    stream
        .write_all(b"*2\r\n$3\r\nACL\r\n$4\r\nLIST\r\n")
        .await
        .unwrap();
    let mut buf = vec![0u8; 1024];
    let n = stream.read(&mut buf).await.unwrap();
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("user acluser")
            && response.contains("~key*")
            && response.contains("+get"),
        "ACL not applied: {response}"
    );
}

#[tokio::test]
async fn elasticache_modify_cache_parameter_group_applies_config_set() {
    if !require_docker_or_skip("elasticache_modify_cache_parameter_group_applies_config_set") {
        return;
    }

    if !docker_available() {
        return;
    }
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_parameter_group()
        .cache_parameter_group_family("redis7")
        .cache_parameter_group_name("param-test")
        .description("test params")
        .send()
        .await
        .unwrap();

    client
        .create_cache_cluster()
        .cache_cluster_id("param-cluster")
        .cache_parameter_group_name("param-test")
        .cache_node_type("cache.t3.micro")
        .send()
        .await
        .unwrap();

    // Backing container starts in the background; wait for the endpoint port.
    let cluster = helpers::wait_for_cache_cluster_available(&client, "param-cluster", 120).await;
    let port = cluster.cache_nodes()[0]
        .endpoint()
        .expect("cache node endpoint")
        .port()
        .unwrap_or(0) as u16;

    client
        .modify_cache_parameter_group()
        .cache_parameter_group_name("param-test")
        .set_parameter_name_values(Some(vec![
            aws_sdk_elasticache::types::ParameterNameValue::builder()
                .parameter_name("maxmemory-policy")
                .parameter_value("allkeys-lru")
                .build(),
        ]))
        .send()
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let addr = format!("127.0.0.1:{port}");
    if !redis_config_supported(&addr).await {
        // Container runtime lacks full CONFIG support — skip wire-level check.
        return;
    }

    let mut last_response = String::new();
    let mut found = false;
    for _ in 0..10 {
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$14\r\nmaxmemory-policy\r\n")
            .await
            .unwrap();
        let mut buf = vec![0u8; 1024];
        let n = stream.read(&mut buf).await.unwrap();
        let response = String::from_utf8_lossy(&buf[..n]);
        last_response = response.to_string();
        if response.contains("allkeys-lru") {
            found = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    assert!(
        found,
        "CONFIG SET not applied after retries. Last response: {last_response}"
    );
}

// bug-audit 2026-05-28, 4.3: deleting a cluster while it is still creating must
// not leave an orphan or resurrect the cluster — a same-id re-create must work.
#[tokio::test]
async fn delete_during_create_does_not_orphan() {
    if !require_docker_or_skip("elasticache_delete_during_create_does_not_orphan") {
        return;
    }

    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_cluster()
        .cache_cluster_id("race-redis")
        .cache_node_type("cache.t3.micro")
        .preferred_availability_zone("us-east-1a")
        .send()
        .await
        .unwrap();

    // Delete immediately, while the background container start may still be in
    // flight.
    client
        .delete_cache_cluster()
        .cache_cluster_id("race-redis")
        .send()
        .await
        .expect("delete cache cluster");

    // Let the racing create-finish + delete-reap settle.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // The cluster must be gone (not resurrected to "available").
    let desc = client
        .describe_cache_clusters()
        .cache_cluster_id("race-redis")
        .send()
        .await;
    assert!(
        desc.is_err(),
        "deleted cluster must not be resurrected by the create-finish step"
    );

    // A same-id re-create must succeed.
    client
        .create_cache_cluster()
        .cache_cluster_id("race-redis")
        .cache_node_type("cache.t3.micro")
        .preferred_availability_zone("us-east-1a")
        .send()
        .await
        .expect("re-create with same id must succeed after delete-during-create");
}

// ── Control-plane resources that don't need a cache container ──────────────

#[tokio::test]
async fn elasticache_subnet_group_name_lowercased_and_tags_round_trip() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    // AWS lowercases subnet-group names; create with mixed case and the
    // response (and later reads) must use the lowercase form.
    let create = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("Mixed-Case-SG")
        .cache_subnet_group_description("d")
        .subnet_ids("subnet-aaa111")
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("Name")
                .value("Mixed-Case-SG")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let group = create.cache_subnet_group().unwrap();
    assert_eq!(group.cache_subnet_group_name(), Some("mixed-case-sg"));

    // Describe by the lowercase name resolves it.
    let described = client
        .describe_cache_subnet_groups()
        .cache_subnet_group_name("mixed-case-sg")
        .send()
        .await
        .expect("describe by lowercase name");
    assert_eq!(described.cache_subnet_groups().len(), 1);

    // Create-time tags round-trip via ListTagsForResource.
    let arn = group.arn().unwrap();
    let tags = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .expect("list tags");
    assert!(tags
        .tag_list()
        .iter()
        .any(|t| t.key() == Some("Name") && t.value() == Some("Mixed-Case-SG")));
}

#[tokio::test]
async fn elasticache_user_group_engine_case_insensitive_and_modify_settles_active() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    // The provider sends the engine uppercase ("REDIS"); AWS accepts it and
    // stores it lowercase.
    client
        .create_user()
        .user_id("u1")
        .user_name("u1")
        .engine("REDIS")
        .access_string("on ~* +@all")
        .passwords("abcdefghijabcdefghij")
        .send()
        .await
        .expect("create user with REDIS engine");
    client
        .create_user()
        .user_id("u2")
        .user_name("u2")
        .engine("REDIS")
        .access_string("on ~* +@all")
        .passwords("abcdefghijabcdefghij")
        .send()
        .await
        .expect("create second user");

    let ug = client
        .create_user_group()
        .user_group_id("ug1")
        .engine("REDIS")
        .user_ids("u1")
        .send()
        .await
        .expect("create user group with REDIS engine");
    assert_eq!(ug.engine(), Some("redis"));
    assert_eq!(ug.status(), Some("active"));

    // Modifying the membership must settle back to `active`, not stay
    // `modifying` (which would hang the provider's update waiter).
    let modified = client
        .modify_user_group()
        .user_group_id("ug1")
        .user_ids_to_add("u2")
        .send()
        .await
        .expect("modify user group");
    assert_eq!(modified.status(), Some("active"));
    assert_eq!(modified.user_ids().len(), 2);
}
