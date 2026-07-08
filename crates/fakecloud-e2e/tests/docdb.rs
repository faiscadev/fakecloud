mod helpers;

use helpers::TestServer;

/// Full control-plane round-trip against the DocumentDB service via the real
/// `aws-sdk-docdb` client: subnet group -> cluster -> instance -> describe ->
/// snapshot -> restore-from-snapshot -> tag -> delete.
#[tokio::test]
async fn docdb_full_control_plane_round_trip() {
    let server = TestServer::start().await;
    let client = server.docdb_client().await;

    // 1. Subnet group.
    let sng = client
        .create_db_subnet_group()
        .db_subnet_group_name("docdb-subnets")
        .db_subnet_group_description("docdb test subnets")
        .subnet_ids("subnet-11111111")
        .subnet_ids("subnet-22222222")
        .send()
        .await
        .expect("create subnet group")
        .db_subnet_group
        .expect("subnet group present");
    assert_eq!(sng.db_subnet_group_name(), Some("docdb-subnets"));
    assert_eq!(sng.subnets().len(), 2);

    // 2. Cluster.
    let cluster = client
        .create_db_cluster()
        .db_cluster_identifier("docdb-cluster")
        .engine("docdb")
        .master_username("docdbadmin")
        .master_user_password("SuperSecret123")
        .db_subnet_group_name("docdb-subnets")
        .send()
        .await
        .expect("create cluster")
        .db_cluster
        .expect("cluster present");
    assert_eq!(cluster.db_cluster_identifier(), Some("docdb-cluster"));
    assert_eq!(cluster.engine(), Some("docdb"));
    let cluster_arn = cluster.db_cluster_arn().unwrap();
    assert!(
        cluster_arn.starts_with("arn:aws:rds:us-east-1:")
            && cluster_arn.ends_with(":cluster:docdb-cluster"),
        "unexpected cluster ARN: {cluster_arn}"
    );
    assert!(cluster.endpoint().unwrap().contains(".docdb.amazonaws.com"));
    assert!(cluster.reader_endpoint().unwrap().contains("cluster-ro-"));
    assert!(cluster
        .db_cluster_resource_id()
        .unwrap()
        .starts_with("cluster-"));

    // 3. Instance attached to the cluster.
    let instance = client
        .create_db_instance()
        .db_instance_identifier("docdb-instance-1")
        .db_instance_class("db.r5.large")
        .engine("docdb")
        .db_cluster_identifier("docdb-cluster")
        .send()
        .await
        .expect("create instance")
        .db_instance
        .expect("instance present");
    assert_eq!(instance.db_instance_identifier(), Some("docdb-instance-1"));
    assert_eq!(instance.db_cluster_identifier(), Some("docdb-cluster"));
    assert_eq!(instance.db_instance_status(), Some("available"));
    assert!(instance
        .endpoint()
        .and_then(|e| e.address())
        .unwrap()
        .contains("docdb-instance-1"));

    // 4. Describe: cluster lists the instance as its writer member.
    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("docdb-cluster")
        .send()
        .await
        .expect("describe clusters");
    let dc = &described.db_clusters()[0];
    let members = dc.db_cluster_members();
    assert_eq!(members.len(), 1);
    assert_eq!(
        members[0].db_instance_identifier(),
        Some("docdb-instance-1")
    );
    assert_eq!(members[0].is_cluster_writer(), Some(true));

    // 5. Snapshot.
    let snap = client
        .create_db_cluster_snapshot()
        .db_cluster_snapshot_identifier("docdb-snap")
        .db_cluster_identifier("docdb-cluster")
        .send()
        .await
        .expect("create snapshot")
        .db_cluster_snapshot
        .expect("snapshot present");
    assert_eq!(snap.db_cluster_snapshot_identifier(), Some("docdb-snap"));
    assert_eq!(snap.status(), Some("available"));
    assert_eq!(snap.db_cluster_identifier(), Some("docdb-cluster"));

    // 6. Restore into a new cluster from the snapshot.
    let restored = client
        .restore_db_cluster_from_snapshot()
        .db_cluster_identifier("docdb-restored")
        .snapshot_identifier("docdb-snap")
        .engine("docdb")
        .send()
        .await
        .expect("restore from snapshot")
        .db_cluster
        .expect("restored cluster present");
    assert_eq!(restored.db_cluster_identifier(), Some("docdb-restored"));

    let restored_desc = client
        .describe_db_clusters()
        .db_cluster_identifier("docdb-restored")
        .send()
        .await
        .expect("describe restored");
    assert_eq!(restored_desc.db_clusters().len(), 1);

    // 7. Tagging round-trip on the cluster ARN.
    let arn = cluster.db_cluster_arn().unwrap();
    client
        .add_tags_to_resource()
        .resource_name(arn)
        .tags(
            aws_sdk_docdb::types::Tag::builder()
                .key("env")
                .value("test")
                .build(),
        )
        .send()
        .await
        .expect("add tags");
    let tags = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .expect("list tags");
    let tag_list = tags.tag_list();
    assert_eq!(tag_list.len(), 1);
    assert_eq!(tag_list[0].key(), Some("env"));
    assert_eq!(tag_list[0].value(), Some("test"));

    // 8. Delete instance then clusters; deletes echo the deleted resource.
    client
        .delete_db_instance()
        .db_instance_identifier("docdb-instance-1")
        .send()
        .await
        .expect("delete instance");
    for id in ["docdb-cluster", "docdb-restored"] {
        client
            .delete_db_cluster()
            .db_cluster_identifier(id)
            .send()
            .await
            .unwrap_or_else(|e| panic!("delete cluster {id}: {e:?}"));
    }

    // Cluster is gone: describe returns DBClusterNotFoundFault.
    let err = client
        .describe_db_clusters()
        .db_cluster_identifier("docdb-cluster")
        .send()
        .await
        .expect_err("cluster should be gone");
    let msg = format!("{err:?}");
    assert!(msg.contains("DBClusterNotFound"), "unexpected error: {msg}");
}

/// Read-only catalog operations return well-formed data.
#[tokio::test]
async fn docdb_engine_versions_and_certificates() {
    let server = TestServer::start().await;
    let client = server.docdb_client().await;

    let versions = client
        .describe_db_engine_versions()
        .engine("docdb")
        .send()
        .await
        .expect("engine versions");
    assert!(versions
        .db_engine_versions()
        .iter()
        .all(|v| v.engine() == Some("docdb")));
    assert!(versions
        .db_engine_versions()
        .iter()
        .any(|v| v.engine_version() == Some("5.0.0")));

    let certs = client
        .describe_certificates()
        .send()
        .await
        .expect("certificates");
    assert!(!certs.certificates().is_empty());
}
