mod helpers;

use helpers::TestServer;

// ---------------------------------------------------------------------------
// CacheCluster tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_create_cache_cluster_and_describe() {
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
    assert_eq!(cluster.cache_cluster_status(), Some("available"));
    assert_eq!(cluster.engine(), Some("redis"));
    let arn = cluster.arn().expect("cluster arn");

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
// Tag tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_add_and_list_tags_on_subnet_group() {
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
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .list_tags_for_resource()
        .resource_name("arn:aws:elasticache:us-east-1:123456789012:subnetgroup:nonexistent")
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

#[tokio::test]
async fn elasticache_global_replication_group_lifecycle() {
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

// ---------------------------------------------------------------------------
// User tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_create_user_and_verify_in_describe() {
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
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client.delete_user().user_id("default").send().await;

    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// UserGroup tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_create_user_group_with_user_references() {
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

// ---------------------------------------------------------------------------
// ReplicationGroup operational tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_modify_replication_group_description() {
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
async fn elasticache_increase_replica_count() {
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
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("fo-rg")
        .replication_group_description("For failover test")
        .send()
        .await
        .unwrap();

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

// ---------------------------------------------------------------------------
// Snapshot tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_create_snapshot_and_describe() {
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
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .delete_snapshot()
        .snapshot_name("nonexistent-snapshot")
        .send()
        .await;

    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// ServerlessCache tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn elasticache_create_serverless_cache_and_describe() {
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
    assert_eq!(cache.status(), Some("available"));
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
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let result = client
        .delete_serverless_cache_snapshot()
        .serverless_cache_snapshot_name("missing-serverless-snapshot")
        .send()
        .await;

    assert!(result.is_err());
}
