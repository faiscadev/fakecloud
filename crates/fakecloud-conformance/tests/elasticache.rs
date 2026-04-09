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
