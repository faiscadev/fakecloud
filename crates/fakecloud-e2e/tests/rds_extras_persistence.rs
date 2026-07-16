//! End-to-end coverage for the RDS Copy/Modify/Restore "extras" that
//! previously returned canned XML without persisting. Each op below now
//! writes real state; these tests assert the write round-trips through the
//! matching Describe using the real AWS SDK. All ops here are container-free
//! (cluster/parameter-group/subnet-group metadata), so they need no Docker.

mod helpers;

use helpers::TestServer;

/// Create an Aurora cluster and return its real ARN (read back from
/// Describe rather than constructed, so the test doesn't hardcode the
/// server's default account id).
async fn create_cluster(client: &aws_sdk_rds::Client, id: &str) -> String {
    let created = client
        .create_db_cluster()
        .db_cluster_identifier(id)
        .engine("aurora-postgresql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();
    created
        .db_cluster()
        .and_then(|c| c.db_cluster_arn())
        .expect("cluster arn")
        .to_string()
}

#[tokio::test]
async fn enable_disable_http_endpoint_round_trips_describe() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;
    let arn = create_cluster(&client, "http-clus").await;

    let enabled = client
        .enable_http_endpoint()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(enabled.http_endpoint_enabled(), Some(true));

    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("http-clus")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described
            .db_clusters()
            .first()
            .and_then(|c| c.http_endpoint_enabled()),
        Some(true)
    );

    client
        .disable_http_endpoint()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("http-clus")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described
            .db_clusters()
            .first()
            .and_then(|c| c.http_endpoint_enabled()),
        Some(false)
    );
}

#[tokio::test]
async fn modify_current_db_cluster_capacity_echoes_requested_capacity() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;
    create_cluster(&client, "cap-clus").await;

    let out = client
        .modify_current_db_cluster_capacity()
        .db_cluster_identifier("cap-clus")
        .capacity(8)
        .send()
        .await
        .unwrap();
    assert_eq!(out.db_cluster_identifier(), Some("cap-clus"));
    assert_eq!(out.current_capacity(), Some(8));
}

#[tokio::test]
async fn copy_db_parameter_group_persists_and_is_describable() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_parameter_group()
        .db_parameter_group_name("src-pg")
        .db_parameter_group_family("postgres16")
        .description("source")
        .send()
        .await
        .unwrap();

    let copied = client
        .copy_db_parameter_group()
        .source_db_parameter_group_identifier("src-pg")
        .target_db_parameter_group_identifier("copy-pg")
        .target_db_parameter_group_description("the copy")
        .send()
        .await
        .unwrap();
    assert_eq!(
        copied
            .db_parameter_group()
            .and_then(|g| g.db_parameter_group_name()),
        Some("copy-pg")
    );

    let described = client
        .describe_db_parameter_groups()
        .db_parameter_group_name("copy-pg")
        .send()
        .await
        .unwrap();
    let group = described
        .db_parameter_groups()
        .first()
        .expect("copy persisted");
    assert_eq!(group.db_parameter_group_name(), Some("copy-pg"));
    assert_eq!(group.description(), Some("the copy"));
    assert_eq!(group.db_parameter_group_family(), Some("postgres16"));
}

#[tokio::test]
async fn restore_db_cluster_from_s3_creates_describable_cluster() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .restore_db_cluster_from_s3()
        .db_cluster_identifier("s3-clus")
        .engine("aurora-mysql")
        .master_username("admin")
        .master_user_password("secret123")
        .source_engine("mysql")
        .source_engine_version("8.0.28")
        .s3_bucket_name("my-backups")
        .s3_ingestion_role_arn("arn:aws:iam::123456789012:role/rds-s3-import")
        .send()
        .await
        .unwrap();

    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("s3-clus")
        .send()
        .await
        .unwrap();
    let cluster = described
        .db_clusters()
        .first()
        .expect("restored cluster present");
    assert_eq!(cluster.db_cluster_identifier(), Some("s3-clus"));
    assert_eq!(cluster.status(), Some("available"));
}

#[tokio::test]
async fn start_stop_activity_stream_round_trips_on_cluster() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;
    let arn = create_cluster(&client, "das-clus").await;

    let started = client
        .start_activity_stream()
        .resource_arn(&arn)
        .mode(aws_sdk_rds::types::ActivityStreamMode::Async)
        .kms_key_id("1234abcd-12ab-34cd-56ef-1234567890ab")
        .send()
        .await
        .unwrap();
    assert_eq!(
        started.status(),
        Some(&aws_sdk_rds::types::ActivityStreamStatus::Started)
    );

    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("das-clus")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described
            .db_clusters()
            .first()
            .and_then(|c| c.activity_stream_status()),
        Some(&aws_sdk_rds::types::ActivityStreamStatus::Started)
    );

    client
        .stop_activity_stream()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("das-clus")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described
            .db_clusters()
            .first()
            .and_then(|c| c.activity_stream_status()),
        Some(&aws_sdk_rds::types::ActivityStreamStatus::Stopped)
    );
}

#[tokio::test]
async fn modify_db_subnet_group_applies_description() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_subnet_group()
        .db_subnet_group_name("sng-desc")
        .db_subnet_group_description("original")
        .subnet_ids("subnet-aaaa1111")
        .subnet_ids("subnet-bbbb2222")
        .send()
        .await
        .unwrap();

    client
        .modify_db_subnet_group()
        .db_subnet_group_name("sng-desc")
        .db_subnet_group_description("updated description")
        .subnet_ids("subnet-aaaa1111")
        .subnet_ids("subnet-bbbb2222")
        .send()
        .await
        .unwrap();

    let described = client
        .describe_db_subnet_groups()
        .db_subnet_group_name("sng-desc")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described
            .db_subnet_groups()
            .first()
            .and_then(|g| g.db_subnet_group_description()),
        Some("updated description")
    );
}
