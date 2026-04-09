mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("elasticache", "CreateCacheSubnetGroup", checksum = "84cb3eb4")]
#[tokio::test]
async fn elasticache_create_cache_subnet_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("test-subnet-group")
        .cache_subnet_group_description("Test subnet group")
        .subnet_ids("subnet-abc123")
        .send()
        .await
        .unwrap();

    let group = response.cache_subnet_group().expect("cache subnet group");
    assert_eq!(group.cache_subnet_group_name(), Some("test-subnet-group"));
    assert_eq!(
        group.cache_subnet_group_description(),
        Some("Test subnet group")
    );
}

#[test_action("elasticache", "DeleteCacheSubnetGroup", checksum = "9ffab4c4")]
#[tokio::test]
async fn elasticache_delete_cache_subnet_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_subnet_group()
        .cache_subnet_group_name("to-delete")
        .cache_subnet_group_description("Will be deleted")
        .subnet_ids("subnet-abc123")
        .send()
        .await
        .unwrap();

    client
        .delete_cache_subnet_group()
        .cache_subnet_group_name("to-delete")
        .send()
        .await
        .unwrap();
}

#[test_action("elasticache", "DescribeCacheSubnetGroups", checksum = "0f6a2b15")]
#[tokio::test]
async fn elasticache_describe_cache_subnet_groups() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client.describe_cache_subnet_groups().send().await.unwrap();

    let groups = response.cache_subnet_groups();
    assert!(!groups.is_empty());
    assert!(groups
        .iter()
        .any(|g| g.cache_subnet_group_name() == Some("default")));
}

#[test_action("elasticache", "ModifyCacheSubnetGroup", checksum = "ebab21f4")]
#[tokio::test]
async fn elasticache_modify_cache_subnet_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_subnet_group()
        .cache_subnet_group_name("to-modify")
        .cache_subnet_group_description("Original description")
        .subnet_ids("subnet-abc123")
        .send()
        .await
        .unwrap();

    let response = client
        .modify_cache_subnet_group()
        .cache_subnet_group_name("to-modify")
        .cache_subnet_group_description("Updated description")
        .send()
        .await
        .unwrap();

    let group = response.cache_subnet_group().expect("cache subnet group");
    assert_eq!(
        group.cache_subnet_group_description(),
        Some("Updated description")
    );
}

#[test_action("elasticache", "DescribeCacheEngineVersions", checksum = "a71c9f1a")]
#[tokio::test]
async fn elasticache_describe_cache_engine_versions() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_cache_engine_versions()
        .engine("redis")
        .send()
        .await
        .unwrap();

    let versions = response.cache_engine_versions();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].engine(), Some("redis"));
    assert_eq!(versions[0].engine_version(), Some("7.1"));
    assert_eq!(versions[0].cache_parameter_group_family(), Some("redis7"));
}

#[test_action(
    "elasticache",
    "DescribeEngineDefaultParameters",
    checksum = "0b34416b"
)]
#[tokio::test]
async fn elasticache_describe_engine_default_parameters() {
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
    assert!(!params.is_empty());
    assert_eq!(params[0].parameter_name(), Some("maxmemory-policy"));
}

#[test_action("elasticache", "CreateReplicationGroup", checksum = "d97235ac")]
#[tokio::test]
async fn elasticache_create_replication_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .create_replication_group()
        .replication_group_id("test-repl-group")
        .replication_group_description("Test replication group")
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.replication_group_id(), Some("test-repl-group"));
    assert_eq!(group.status(), Some("available"));
}

#[test_action("elasticache", "CreateGlobalReplicationGroup", checksum = "5a6b779c")]
#[tokio::test]
async fn elasticache_create_global_replication_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("primary-global-rg")
        .replication_group_description("Primary for global test")
        .send()
        .await
        .unwrap();

    let response = client
        .create_global_replication_group()
        .global_replication_group_id_suffix("global-a")
        .primary_replication_group_id("primary-global-rg")
        .global_replication_group_description("Global test group")
        .send()
        .await
        .unwrap();

    let group = response
        .global_replication_group()
        .expect("global replication group");
    assert_eq!(
        group.global_replication_group_description(),
        Some("Global test group")
    );
    assert_eq!(group.engine(), Some("redis"));
}

#[test_action("elasticache", "CreateCacheCluster", checksum = "d1b7b330")]
#[tokio::test]
async fn elasticache_create_cache_cluster() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .create_cache_cluster()
        .cache_cluster_id("test-cache-cluster")
        .send()
        .await
        .unwrap();

    let cluster = response.cache_cluster().expect("cache cluster");
    assert_eq!(cluster.cache_cluster_id(), Some("test-cache-cluster"));
    assert_eq!(cluster.cache_cluster_status(), Some("available"));
    assert_eq!(cluster.engine(), Some("redis"));
}

#[test_action("elasticache", "DescribeReplicationGroups", checksum = "70aa64c5")]
#[tokio::test]
async fn elasticache_describe_replication_groups() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("desc-repl-group")
        .replication_group_description("For describe test")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_replication_groups()
        .replication_group_id("desc-repl-group")
        .send()
        .await
        .unwrap();

    let groups = response.replication_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].replication_group_id(), Some("desc-repl-group"));
}

#[test_action(
    "elasticache",
    "DescribeGlobalReplicationGroups",
    checksum = "57ffca32"
)]
#[tokio::test]
async fn elasticache_describe_global_replication_groups() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("desc-global-primary")
        .replication_group_description("Primary for global describe")
        .send()
        .await
        .unwrap();

    let created = client
        .create_global_replication_group()
        .global_replication_group_id_suffix("global-desc")
        .primary_replication_group_id("desc-global-primary")
        .send()
        .await
        .unwrap();
    let global_id = created
        .global_replication_group()
        .and_then(|group| group.global_replication_group_id())
        .expect("global replication group id")
        .to_string();

    let response = client
        .describe_global_replication_groups()
        .global_replication_group_id(global_id)
        .show_member_info(true)
        .send()
        .await
        .unwrap();

    let groups = response.global_replication_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].members().len(), 1);
    assert_eq!(groups[0].members()[0].role(), Some("primary"));
}

#[test_action("elasticache", "DescribeCacheClusters", checksum = "7488fca6")]
#[tokio::test]
async fn elasticache_describe_cache_clusters() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_cluster()
        .cache_cluster_id("desc-cache-cluster")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_cache_clusters()
        .cache_cluster_id("desc-cache-cluster")
        .show_cache_node_info(true)
        .send()
        .await
        .unwrap();

    let clusters = response.cache_clusters();
    assert_eq!(clusters.len(), 1);
    assert_eq!(clusters[0].cache_cluster_id(), Some("desc-cache-cluster"));
    assert_eq!(clusters[0].cache_nodes().len(), 1);
}

#[test_action("elasticache", "DeleteReplicationGroup", checksum = "e3cec3b6")]
#[tokio::test]
async fn elasticache_delete_replication_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("del-repl-group")
        .replication_group_description("Will be deleted")
        .send()
        .await
        .unwrap();

    let response = client
        .delete_replication_group()
        .replication_group_id("del-repl-group")
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.replication_group_id(), Some("del-repl-group"));
}

#[test_action("elasticache", "ModifyGlobalReplicationGroup", checksum = "046c2c9e")]
#[tokio::test]
async fn elasticache_modify_global_replication_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("modify-global-primary")
        .replication_group_description("Primary for global modify")
        .send()
        .await
        .unwrap();

    let created = client
        .create_global_replication_group()
        .global_replication_group_id_suffix("global-mod")
        .primary_replication_group_id("modify-global-primary")
        .send()
        .await
        .unwrap();
    let global_id = created
        .global_replication_group()
        .and_then(|group| group.global_replication_group_id())
        .expect("global replication group id")
        .to_string();

    let response = client
        .modify_global_replication_group()
        .global_replication_group_id(global_id)
        .apply_immediately(true)
        .global_replication_group_description("Updated global description")
        .automatic_failover_enabled(true)
        .send()
        .await
        .unwrap();

    let group = response
        .global_replication_group()
        .expect("global replication group");
    assert_eq!(
        group.global_replication_group_description(),
        Some("Updated global description")
    );
}

#[test_action("elasticache", "DeleteGlobalReplicationGroup", checksum = "a7da3da4")]
#[tokio::test]
async fn elasticache_delete_global_replication_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("delete-global-primary")
        .replication_group_description("Primary for global delete")
        .send()
        .await
        .unwrap();

    let created = client
        .create_global_replication_group()
        .global_replication_group_id_suffix("global-del")
        .primary_replication_group_id("delete-global-primary")
        .send()
        .await
        .unwrap();
    let global_id = created
        .global_replication_group()
        .and_then(|group| group.global_replication_group_id())
        .expect("global replication group id")
        .to_string();

    let response = client
        .delete_global_replication_group()
        .global_replication_group_id(global_id)
        .retain_primary_replication_group(true)
        .send()
        .await
        .unwrap();

    let group = response
        .global_replication_group()
        .expect("global replication group");
    assert_eq!(group.status(), Some("deleting"));
}

#[test_action("elasticache", "DeleteCacheCluster", checksum = "72e1dd2c")]
#[tokio::test]
async fn elasticache_delete_cache_cluster() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_cache_cluster()
        .cache_cluster_id("del-cache-cluster")
        .send()
        .await
        .unwrap();

    let response = client
        .delete_cache_cluster()
        .cache_cluster_id("del-cache-cluster")
        .send()
        .await
        .unwrap();

    let cluster = response.cache_cluster().expect("cache cluster");
    assert_eq!(cluster.cache_cluster_id(), Some("del-cache-cluster"));
    assert_eq!(cluster.cache_cluster_status(), Some("deleting"));
}

#[test_action("elasticache", "AddTagsToResource", checksum = "cf656420")]
#[tokio::test]
async fn elasticache_add_tags_to_resource() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("tag-test-group")
        .cache_subnet_group_description("For tag test")
        .subnet_ids("subnet-abc123")
        .send()
        .await
        .unwrap();

    let arn = create
        .cache_subnet_group()
        .and_then(|g| g.arn())
        .expect("subnet group arn");

    let response = client
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

    let tags = response.tag_list();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), Some("env"));
    assert_eq!(tags[0].value(), Some("dev"));
}

#[test_action("elasticache", "ListTagsForResource", checksum = "a3fcc3e4")]
#[tokio::test]
async fn elasticache_list_tags_for_resource() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("list-tag-group")
        .cache_subnet_group_description("For list tag test")
        .subnet_ids("subnet-abc123")
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
                .value("dev")
                .build(),
        )
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("team")
                .value("core")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let response = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();

    let tags = response.tag_list();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key(), Some("env"));
    assert_eq!(tags[1].key(), Some("team"));
}

#[test_action("elasticache", "RemoveTagsFromResource", checksum = "7e9e103c")]
#[tokio::test]
async fn elasticache_remove_tags_from_resource() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let create = client
        .create_cache_subnet_group()
        .cache_subnet_group_name("remove-tag-group")
        .cache_subnet_group_description("For remove tag test")
        .subnet_ids("subnet-abc123")
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
                .value("dev")
                .build(),
        )
        .tags(
            aws_sdk_elasticache::types::Tag::builder()
                .key("team")
                .value("core")
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .remove_tags_from_resource()
        .resource_name(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let response = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();

    let tags = response.tag_list();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), Some("team"));
}

#[test_action("elasticache", "CreateUser", checksum = "eeb45fb0")]
#[tokio::test]
async fn elasticache_create_user() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .create_user()
        .user_id("testuser")
        .user_name("testuser")
        .engine("redis")
        .access_string("on ~* +@all")
        .no_password_required(true)
        .send()
        .await
        .unwrap();

    assert_eq!(response.user_id(), Some("testuser"));
    assert_eq!(response.user_name(), Some("testuser"));
    assert_eq!(response.status(), Some("active"));
}

#[test_action("elasticache", "DescribeUsers", checksum = "e5c2e676")]
#[tokio::test]
async fn elasticache_describe_users() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client.describe_users().send().await.unwrap();

    let users = response.users();
    assert!(!users.is_empty());
    assert!(users.iter().any(|u| u.user_id() == Some("default")));
}

#[test_action("elasticache", "DeleteUser", checksum = "98b86a69")]
#[tokio::test]
async fn elasticache_delete_user() {
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

    let response = client
        .delete_user()
        .user_id("deluser")
        .send()
        .await
        .unwrap();

    assert_eq!(response.user_id(), Some("deluser"));
}

#[test_action("elasticache", "CreateUserGroup", checksum = "294a66f3")]
#[tokio::test]
async fn elasticache_create_user_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .create_user_group()
        .user_group_id("test-group")
        .engine("redis")
        .user_ids("default")
        .send()
        .await
        .unwrap();

    assert_eq!(response.user_group_id(), Some("test-group"));
    assert_eq!(response.status(), Some("active"));
}

#[test_action("elasticache", "DescribeUserGroups", checksum = "94732ab4")]
#[tokio::test]
async fn elasticache_describe_user_groups() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_user_group()
        .user_group_id("desc-group")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_user_groups()
        .user_group_id("desc-group")
        .send()
        .await
        .unwrap();

    let groups = response.user_groups();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0].user_group_id(), Some("desc-group"));
}

#[test_action("elasticache", "DeleteUserGroup", checksum = "39a6b59a")]
#[tokio::test]
async fn elasticache_delete_user_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_user_group()
        .user_group_id("del-group")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let response = client
        .delete_user_group()
        .user_group_id("del-group")
        .send()
        .await
        .unwrap();

    assert_eq!(response.user_group_id(), Some("del-group"));
}

#[test_action("elasticache", "DescribeCacheParameterGroups", checksum = "f2d641d8")]
#[tokio::test]
async fn elasticache_describe_cache_parameter_groups() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .describe_cache_parameter_groups()
        .send()
        .await
        .unwrap();

    let groups = response.cache_parameter_groups();
    assert!(groups.len() >= 2);
    assert_eq!(
        groups[0].cache_parameter_group_name(),
        Some("default.redis7")
    );
    assert_eq!(
        groups[1].cache_parameter_group_name(),
        Some("default.valkey8")
    );
}

#[test_action("elasticache", "ModifyReplicationGroup", checksum = "df9899e6")]
#[tokio::test]
async fn elasticache_modify_replication_group() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("mod-repl-group")
        .replication_group_description("Original description")
        .send()
        .await
        .unwrap();

    let response = client
        .modify_replication_group()
        .replication_group_id("mod-repl-group")
        .replication_group_description("Updated description")
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.replication_group_id(), Some("mod-repl-group"));
    assert_eq!(group.description(), Some("Updated description"));
}

#[test_action("elasticache", "IncreaseReplicaCount", checksum = "e5ca0f20")]
#[tokio::test]
async fn elasticache_increase_replica_count() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("inc-repl-group")
        .replication_group_description("For increase test")
        .send()
        .await
        .unwrap();

    let response = client
        .increase_replica_count()
        .replication_group_id("inc-repl-group")
        .new_replica_count(2)
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.replication_group_id(), Some("inc-repl-group"));
    assert_eq!(group.member_clusters().len(), 3);
}

#[test_action("elasticache", "DecreaseReplicaCount", checksum = "cab83215")]
#[tokio::test]
async fn elasticache_decrease_replica_count() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("dec-repl-group")
        .replication_group_description("For decrease test")
        .num_cache_clusters(3)
        .send()
        .await
        .unwrap();

    let response = client
        .decrease_replica_count()
        .replication_group_id("dec-repl-group")
        .new_replica_count(1)
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.replication_group_id(), Some("dec-repl-group"));
    assert_eq!(group.member_clusters().len(), 2);
}

#[test_action("elasticache", "CreateSnapshot", checksum = "10b847ad")]
#[tokio::test]
async fn elasticache_create_snapshot() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("snap-repl-group")
        .replication_group_description("For snapshot test")
        .send()
        .await
        .unwrap();

    let response = client
        .create_snapshot()
        .snapshot_name("test-snapshot")
        .replication_group_id("snap-repl-group")
        .send()
        .await
        .unwrap();

    let snapshot = response.snapshot().expect("snapshot");
    assert_eq!(snapshot.snapshot_name(), Some("test-snapshot"));
    assert_eq!(snapshot.replication_group_id(), Some("snap-repl-group"));
}

#[test_action("elasticache", "DescribeSnapshots", checksum = "00f83d10")]
#[tokio::test]
async fn elasticache_describe_snapshots() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("desc-snap-rg")
        .replication_group_description("For describe snapshot test")
        .send()
        .await
        .unwrap();

    client
        .create_snapshot()
        .snapshot_name("desc-snapshot")
        .replication_group_id("desc-snap-rg")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_snapshots()
        .snapshot_name("desc-snapshot")
        .send()
        .await
        .unwrap();

    let snapshots = response.snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].snapshot_name(), Some("desc-snapshot"));
}

#[test_action("elasticache", "DeleteSnapshot", checksum = "85aa2082")]
#[tokio::test]
async fn elasticache_delete_snapshot() {
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

    let response = client
        .delete_snapshot()
        .snapshot_name("del-snapshot")
        .send()
        .await
        .unwrap();

    let snapshot = response.snapshot().expect("snapshot");
    assert_eq!(snapshot.snapshot_name(), Some("del-snapshot"));
}

#[test_action("elasticache", "TestFailover", checksum = "c08470ff")]
#[tokio::test]
async fn elasticache_test_failover() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_replication_group()
        .replication_group_id("fo-repl-group")
        .replication_group_description("For failover test")
        .send()
        .await
        .unwrap();

    let response = client
        .test_failover()
        .replication_group_id("fo-repl-group")
        .node_group_id("0001")
        .send()
        .await
        .unwrap();

    let group = response.replication_group().expect("replication group");
    assert_eq!(group.replication_group_id(), Some("fo-repl-group"));
    assert_eq!(group.status(), Some("available"));
}

#[test_action("elasticache", "CreateServerlessCache", checksum = "f551fb86")]
#[tokio::test]
async fn elasticache_create_serverless_cache() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    let response = client
        .create_serverless_cache()
        .serverless_cache_name("test-serverless")
        .engine("redis")
        .description("Serverless cache")
        .security_group_ids("sg-123")
        .subnet_ids("subnet-123")
        .snapshot_retention_limit(7)
        .daily_snapshot_time("04:00")
        .send()
        .await
        .unwrap();

    let cache = response.serverless_cache().expect("serverless cache");
    assert_eq!(cache.serverless_cache_name(), Some("test-serverless"));
    assert_eq!(cache.engine(), Some("redis"));
    assert_eq!(cache.status(), Some("available"));
}

#[test_action("elasticache", "DescribeServerlessCaches", checksum = "130bb42b")]
#[tokio::test]
async fn elasticache_describe_serverless_caches() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("desc-serverless")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_serverless_caches()
        .serverless_cache_name("desc-serverless")
        .send()
        .await
        .unwrap();

    let caches = response.serverless_caches();
    assert_eq!(caches.len(), 1);
    assert_eq!(caches[0].serverless_cache_name(), Some("desc-serverless"));
}

#[test_action("elasticache", "ModifyServerlessCache", checksum = "309e3779")]
#[tokio::test]
async fn elasticache_modify_serverless_cache() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("mod-serverless")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let response = client
        .modify_serverless_cache()
        .serverless_cache_name("mod-serverless")
        .description("Updated description")
        .security_group_ids("sg-999")
        .snapshot_retention_limit(9)
        .daily_snapshot_time("05:00")
        .send()
        .await
        .unwrap();

    let cache = response.serverless_cache().expect("serverless cache");
    assert_eq!(cache.description(), Some("Updated description"));
    assert_eq!(cache.snapshot_retention_limit(), Some(9));
}

#[test_action("elasticache", "DeleteServerlessCache", checksum = "5a8a697e")]
#[tokio::test]
async fn elasticache_delete_serverless_cache() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("del-serverless")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let response = client
        .delete_serverless_cache()
        .serverless_cache_name("del-serverless")
        .send()
        .await
        .unwrap();

    let cache = response.serverless_cache().expect("serverless cache");
    assert_eq!(cache.serverless_cache_name(), Some("del-serverless"));
    assert_eq!(cache.status(), Some("deleting"));
}

#[test_action("elasticache", "CreateServerlessCacheSnapshot", checksum = "04326152")]
#[tokio::test]
async fn elasticache_create_serverless_cache_snapshot() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("snap-serverless")
        .engine("redis")
        .send()
        .await
        .unwrap();

    let response = client
        .create_serverless_cache_snapshot()
        .serverless_cache_name("snap-serverless")
        .serverless_cache_snapshot_name("snap-1")
        .send()
        .await
        .unwrap();

    let snapshot = response
        .serverless_cache_snapshot()
        .expect("serverless cache snapshot");
    assert_eq!(snapshot.serverless_cache_snapshot_name(), Some("snap-1"));
    assert_eq!(snapshot.status(), Some("available"));
}

#[test_action(
    "elasticache",
    "DescribeServerlessCacheSnapshots",
    checksum = "132b4ec8"
)]
#[tokio::test]
async fn elasticache_describe_serverless_cache_snapshots() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("desc-snap-serverless")
        .engine("redis")
        .send()
        .await
        .unwrap();
    client
        .create_serverless_cache_snapshot()
        .serverless_cache_name("desc-snap-serverless")
        .serverless_cache_snapshot_name("snap-2")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_serverless_cache_snapshots()
        .serverless_cache_name("desc-snap-serverless")
        .send()
        .await
        .unwrap();

    let snapshots = response.serverless_cache_snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(
        snapshots[0].serverless_cache_snapshot_name(),
        Some("snap-2")
    );
}

#[test_action("elasticache", "DeleteServerlessCacheSnapshot", checksum = "f2cf7742")]
#[tokio::test]
async fn elasticache_delete_serverless_cache_snapshot() {
    let server = TestServer::start().await;
    let client = server.elasticache_client().await;

    client
        .create_serverless_cache()
        .serverless_cache_name("del-snap-serverless")
        .engine("redis")
        .send()
        .await
        .unwrap();
    client
        .create_serverless_cache_snapshot()
        .serverless_cache_name("del-snap-serverless")
        .serverless_cache_snapshot_name("snap-3")
        .send()
        .await
        .unwrap();

    let response = client
        .delete_serverless_cache_snapshot()
        .serverless_cache_snapshot_name("snap-3")
        .send()
        .await
        .unwrap();

    let snapshot = response
        .serverless_cache_snapshot()
        .expect("serverless cache snapshot");
    assert_eq!(snapshot.serverless_cache_snapshot_name(), Some("snap-3"));
    assert_eq!(snapshot.status(), Some("deleting"));
}
