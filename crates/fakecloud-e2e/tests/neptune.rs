mod helpers;

use helpers::TestServer;

/// Full control-plane round-trip against the Neptune service via the real
/// `aws-sdk-neptune` client: subnet group -> cluster -> instance -> cluster
/// endpoint -> describe -> snapshot -> restore-from-snapshot -> tag -> delete.
#[tokio::test]
async fn neptune_full_control_plane_round_trip() {
    let server = TestServer::start().await;
    let client = server.neptune_client().await;

    // 1. Subnet group.
    let sng = client
        .create_db_subnet_group()
        .db_subnet_group_name("neptune-subnets")
        .db_subnet_group_description("neptune test subnets")
        .subnet_ids("subnet-11111111")
        .subnet_ids("subnet-22222222")
        .send()
        .await
        .expect("create subnet group")
        .db_subnet_group
        .expect("subnet group present");
    assert_eq!(sng.db_subnet_group_name(), Some("neptune-subnets"));
    assert_eq!(sng.subnets().len(), 2);

    // 2. Cluster.
    let cluster = client
        .create_db_cluster()
        .db_cluster_identifier("neptune-cluster")
        .engine("neptune")
        .db_subnet_group_name("neptune-subnets")
        .send()
        .await
        .expect("create cluster")
        .db_cluster
        .expect("cluster present");
    assert_eq!(cluster.db_cluster_identifier(), Some("neptune-cluster"));
    assert_eq!(cluster.engine(), Some("neptune"));
    let cluster_arn = cluster.db_cluster_arn().unwrap();
    assert!(
        cluster_arn.starts_with("arn:aws:rds:us-east-1:")
            && cluster_arn.ends_with(":cluster:neptune-cluster"),
        "unexpected cluster ARN: {cluster_arn}"
    );
    assert!(cluster
        .endpoint()
        .unwrap()
        .contains(".neptune.amazonaws.com"));
    assert!(cluster.reader_endpoint().unwrap().contains("cluster-ro-"));
    assert!(cluster
        .db_cluster_resource_id()
        .unwrap()
        .starts_with("cluster-"));

    // 3. Instance attached to the cluster.
    let instance = client
        .create_db_instance()
        .db_instance_identifier("neptune-instance-1")
        .db_instance_class("db.r5.large")
        .engine("neptune")
        .db_cluster_identifier("neptune-cluster")
        .send()
        .await
        .expect("create instance")
        .db_instance
        .expect("instance present");
    assert_eq!(
        instance.db_instance_identifier(),
        Some("neptune-instance-1")
    );
    assert_eq!(instance.db_cluster_identifier(), Some("neptune-cluster"));
    assert_eq!(instance.db_instance_status(), Some("available"));
    assert!(instance
        .endpoint()
        .and_then(|e| e.address())
        .unwrap()
        .contains("neptune-instance-1"));

    // 4. Custom cluster endpoint (Neptune-specific).
    let endpoint = client
        .create_db_cluster_endpoint()
        .db_cluster_identifier("neptune-cluster")
        .db_cluster_endpoint_identifier("neptune-reader-ep")
        .endpoint_type("READER")
        .send()
        .await
        .expect("create cluster endpoint");
    assert_eq!(
        endpoint.db_cluster_endpoint_identifier(),
        Some("neptune-reader-ep")
    );
    // This operation only creates CUSTOM endpoints: the request's
    // EndpointType is the custom type, and the endpoint reads back as
    // CUSTOM. Same mapping as RDS, per the Neptune model.
    assert_eq!(endpoint.endpoint_type(), Some("CUSTOM"));
    assert_eq!(endpoint.custom_endpoint_type(), Some("READER"));
    assert!(endpoint
        .endpoint()
        .unwrap()
        .contains(".neptune.amazonaws.com"));

    let described_eps = client
        .describe_db_cluster_endpoints()
        .db_cluster_identifier("neptune-cluster")
        .send()
        .await
        .expect("describe cluster endpoints");
    assert_eq!(described_eps.db_cluster_endpoints().len(), 1);

    // 5. Describe: cluster lists the instance as its writer member.
    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("neptune-cluster")
        .send()
        .await
        .expect("describe clusters");
    let dc = &described.db_clusters()[0];
    let members = dc.db_cluster_members();
    assert_eq!(members.len(), 1);
    assert_eq!(
        members[0].db_instance_identifier(),
        Some("neptune-instance-1")
    );
    assert_eq!(members[0].is_cluster_writer(), Some(true));

    // 6. Snapshot.
    let snap = client
        .create_db_cluster_snapshot()
        .db_cluster_snapshot_identifier("neptune-snap")
        .db_cluster_identifier("neptune-cluster")
        .send()
        .await
        .expect("create snapshot")
        .db_cluster_snapshot
        .expect("snapshot present");
    assert_eq!(snap.db_cluster_snapshot_identifier(), Some("neptune-snap"));
    assert_eq!(snap.status(), Some("available"));
    assert_eq!(snap.db_cluster_identifier(), Some("neptune-cluster"));

    // 7. Restore into a new cluster from the snapshot.
    let restored = client
        .restore_db_cluster_from_snapshot()
        .db_cluster_identifier("neptune-restored")
        .snapshot_identifier("neptune-snap")
        .engine("neptune")
        .send()
        .await
        .expect("restore from snapshot")
        .db_cluster
        .expect("restored cluster present");
    assert_eq!(restored.db_cluster_identifier(), Some("neptune-restored"));

    let restored_desc = client
        .describe_db_clusters()
        .db_cluster_identifier("neptune-restored")
        .send()
        .await
        .expect("describe restored");
    assert_eq!(restored_desc.db_clusters().len(), 1);

    // 8. Tagging round-trip on the cluster ARN.
    let arn = cluster.db_cluster_arn().unwrap();
    client
        .add_tags_to_resource()
        .resource_name(arn)
        .tags(
            aws_sdk_neptune::types::Tag::builder()
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

    // 9. Delete the cluster endpoint, then instance, then clusters.
    client
        .delete_db_cluster_endpoint()
        .db_cluster_endpoint_identifier("neptune-reader-ep")
        .send()
        .await
        .expect("delete cluster endpoint");
    client
        .delete_db_instance()
        .db_instance_identifier("neptune-instance-1")
        .send()
        .await
        .expect("delete instance");
    for id in ["neptune-cluster", "neptune-restored"] {
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
        .db_cluster_identifier("neptune-cluster")
        .send()
        .await
        .expect_err("cluster should be gone");
    let msg = format!("{err:?}");
    assert!(msg.contains("DBClusterNotFound"), "unexpected error: {msg}");
}

/// Read-only catalog operations return well-formed data.
#[tokio::test]
async fn neptune_engine_versions_and_parameter_groups() {
    let server = TestServer::start().await;
    let client = server.neptune_client().await;

    let versions = client
        .describe_db_engine_versions()
        .engine("neptune")
        .send()
        .await
        .expect("engine versions");
    assert!(versions
        .db_engine_versions()
        .iter()
        .all(|v| v.engine() == Some("neptune")));
    assert!(!versions.db_engine_versions().is_empty());

    // DB parameter group create + describe.
    client
        .create_db_parameter_group()
        .db_parameter_group_name("neptune-pg")
        .db_parameter_group_family("neptune1.3")
        .description("test pg")
        .send()
        .await
        .expect("create db parameter group");
    let params = client
        .describe_db_parameters()
        .db_parameter_group_name("neptune-pg")
        .send()
        .await
        .expect("describe db parameters");
    assert!(!params.parameters().is_empty());
}
