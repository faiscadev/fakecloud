use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse};

use super::*;

fn service() -> DocDbService {
    let state: SharedDocDbState = Arc::new(RwLock::new(MultiAccountState::new(
        "123456789012",
        "us-east-1",
        "http://localhost",
    )));
    DocDbService::new(state)
}

fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut query_params = HashMap::new();
    for (k, v) in params {
        query_params.insert((*k).to_string(), (*v).to_string());
    }
    AwsRequest {
        service: "docdb".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-request-id".to_string(),
        headers: http::HeaderMap::new(),
        query_params,
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

async fn call(svc: &DocDbService, action: &str, params: &[(&str, &str)]) -> AwsResponse {
    svc.handle(req(action, params)).await.expect("handler ok")
}

async fn call_err(
    svc: &DocDbService,
    action: &str,
    params: &[(&str, &str)],
) -> fakecloud_core::service::AwsServiceError {
    match svc.handle(req(action, params)).await {
        Ok(_) => panic!("expected error from {action}"),
        Err(e) => e,
    }
}

fn body(resp: &AwsResponse) -> String {
    match &resp.body {
        fakecloud_core::service::ResponseBody::Bytes(b) => String::from_utf8_lossy(b).to_string(),
        _ => panic!("expected bytes body"),
    }
}

/// `Filters` was accepted and ignored, so a caller narrowing a listing
/// got every resource in the account back.
#[tokio::test]
async fn describe_db_clusters_honors_the_db_cluster_id_filter() {
    let svc = service();
    for id in ["clu-a", "clu-b"] {
        call(
            &svc,
            "CreateDBCluster",
            &[("DBClusterIdentifier", id), ("Engine", "docdb")],
        )
        .await;
    }

    let xml = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "clu-b"),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<DBClusterIdentifier>clu-b</DBClusterIdentifier>"),
        "{xml}"
    );
    assert!(
        !xml.contains("<DBClusterIdentifier>clu-a</DBClusterIdentifier>"),
        "the filter kept an unmatched cluster: {xml}"
    );

    // The ARN form selects the same cluster -- AWS documents this filter
    // as accepting identifiers and ARNs.
    let arn = xml
        .split("<DBClusterArn>")
        .nth(1)
        .and_then(|rest| rest.split("</DBClusterArn>").next())
        .expect("an ARN")
        .to_string();
    let xml = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", &arn),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<DBClusterIdentifier>clu-b</DBClusterIdentifier>"),
        "{xml}"
    );
    assert_eq!(
        xml.matches("<DBClusterIdentifier>").count(),
        1,
        "the ARN form returned more than the cluster it names: {xml}"
    );

    // An unrecognized name matches nothing rather than returning the
    // full list: DocumentDB declares no InvalidParameterValue-equivalent
    // on this operation, so rejecting would be an undeclared shape.
    let xml = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[
                ("Filters.Filter.1.Name", "not-a-filter"),
                ("Filters.Filter.1.Values.Value.1", "clu-b"),
            ],
        )
        .await,
    );
    assert!(
        !xml.contains("<DBClusterIdentifier>"),
        "an unknown filter returned rows: {xml}"
    );
}

#[tokio::test]
async fn describe_db_instances_filters_by_cluster_and_instance() {
    let svc = service();
    for cluster in ["clu-a", "clu-b"] {
        call(
            &svc,
            "CreateDBCluster",
            &[("DBClusterIdentifier", cluster), ("Engine", "docdb")],
        )
        .await;
    }
    for (instance, cluster) in [("inst-a", "clu-a"), ("inst-b", "clu-b")] {
        call(
            &svc,
            "CreateDBInstance",
            &[
                ("DBInstanceIdentifier", instance),
                ("DBClusterIdentifier", cluster),
                ("DBInstanceClass", "db.r5.large"),
                ("Engine", "docdb"),
            ],
        )
        .await;
    }

    // By the instance's own id.
    let xml = body(
        &call(
            &svc,
            "DescribeDBInstances",
            &[
                ("Filters.Filter.1.Name", "db-instance-id"),
                ("Filters.Filter.1.Values.Value.1", "inst-b"),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<DBInstanceIdentifier>inst-b</DBInstanceIdentifier>"),
        "{xml}"
    );
    assert!(
        !xml.contains("<DBInstanceIdentifier>inst-a</DBInstanceIdentifier>"),
        "{xml}"
    );

    // By the cluster it belongs to.
    let xml = body(
        &call(
            &svc,
            "DescribeDBInstances",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "clu-a"),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<DBInstanceIdentifier>inst-a</DBInstanceIdentifier>"),
        "{xml}"
    );
    assert!(
        !xml.contains("<DBInstanceIdentifier>inst-b</DBInstanceIdentifier>"),
        "{xml}"
    );
}

#[tokio::test]
async fn describe_global_clusters_filters_by_member_cluster() {
    let svc = service();
    call(
        &svc,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", "glob-1"), ("Engine", "docdb")],
    )
    .await;
    call(
        &svc,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", "glob-2"), ("Engine", "docdb")],
    )
    .await;

    // `db-cluster-id` names a DB CLUSTER, not the global cluster
    // wrapping it, so the global cluster's own identifier selects
    // nothing -- matching it would return rows AWS does not.
    let xml = body(
        &call(
            &svc,
            "DescribeGlobalClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "glob-2"),
            ],
        )
        .await,
    );
    assert!(
        !xml.contains("<GlobalClusterIdentifier>"),
        "a global cluster's own identifier matched db-cluster-id: {xml}"
    );

    // And by a MEMBER cluster -- the point of the filter, and the only
    // way a caller holding a regional cluster reaches its global parent.
    // Both the bare identifier and the ARN, as AWS documents.
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "member-a"), ("Engine", "docdb")],
    )
    .await;
    call(
        &svc,
        "CreateGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "glob-3"),
            ("Engine", "docdb"),
            ("SourceDBClusterIdentifier", "member-a"),
        ],
    )
    .await;

    for value in [
        "member-a",
        "arn:aws:rds:us-east-1:123456789012:cluster:member-a",
    ] {
        let xml = body(
            &call(
                &svc,
                "DescribeGlobalClusters",
                &[
                    ("Filters.Filter.1.Name", "db-cluster-id"),
                    ("Filters.Filter.1.Values.Value.1", value),
                ],
            )
            .await,
        );
        assert!(
            xml.contains("<GlobalClusterIdentifier>glob-3</GlobalClusterIdentifier>"),
            "member {value} did not select its global cluster: {xml}"
        );
        assert!(
            !xml.contains("<GlobalClusterIdentifier>glob-1</GlobalClusterIdentifier>"),
            "member {value} selected an unrelated global cluster: {xml}"
        );
    }
}

/// A cluster joins a global cluster at create time and leaves it on
/// delete -- the membership the `db-cluster-id` filter matches against.
#[tokio::test]
async fn global_cluster_membership_follows_its_clusters() {
    let svc = service();
    call(
        &svc,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", "glob-1"), ("Engine", "docdb")],
    )
    .await;
    // The normal flow: create the cluster INTO the global cluster.
    call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("Engine", "docdb"),
            ("GlobalClusterIdentifier", "glob-1"),
        ],
    )
    .await;

    let xml = body(
        &call(
            &svc,
            "DescribeGlobalClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "clu-1"),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<GlobalClusterIdentifier>glob-1</GlobalClusterIdentifier>"),
        "a cluster created into a global cluster was not a member: {xml}"
    );

    // Deleting the cluster removes it from the global cluster rather
    // than leaving a dangling member ARN.
    call(&svc, "DeleteDBCluster", &[("DBClusterIdentifier", "clu-1")]).await;
    let xml = body(&call(&svc, "DescribeGlobalClusters", &[]).await);
    assert!(
        !xml.contains("cluster:clu-1"),
        "a deleted cluster stayed a member: {xml}"
    );
    let xml = body(
        &call(
            &svc,
            "DescribeGlobalClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "clu-1"),
            ],
        )
        .await,
    );
    assert!(
        !xml.contains("<GlobalClusterIdentifier>"),
        "a deleted cluster still selected its global cluster: {xml}"
    );

    // An unknown global cluster is the declared fault, not a silent
    // create.
    let err = call_err(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "clu-2"),
            ("Engine", "docdb"),
            ("GlobalClusterIdentifier", "no-such-global"),
        ],
    )
    .await;
    assert_eq!(err.code(), "GlobalClusterNotFoundFault");
}

/// A failover target that names no member must not clear every writer.
#[tokio::test]
async fn failover_global_cluster_rejects_an_unknown_target() {
    let svc = service();
    call(
        &svc,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", "glob-1"), ("Engine", "docdb")],
    )
    .await;
    call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("Engine", "docdb"),
            ("GlobalClusterIdentifier", "glob-1"),
        ],
    )
    .await;

    let err = call_err(
        &svc,
        "FailoverGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "glob-1"),
            ("TargetDbClusterIdentifier", "not-a-member"),
        ],
    )
    .await;
    assert_eq!(err.code(), "DBClusterNotFoundFault");

    // The writer is intact -- the unconditional assignment used to clear
    // it on every member when nothing matched.
    let xml = body(&call(&svc, "DescribeGlobalClusters", &[]).await);
    assert!(
        xml.contains("<IsWriter>true</IsWriter>"),
        "the failed failover left the global cluster with no writer: {xml}"
    );

    // The bare identifier works as a target, as does the ARN.
    call(
        &svc,
        "FailoverGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "glob-1"),
            ("TargetDbClusterIdentifier", "clu-1"),
        ],
    )
    .await;
}

/// A rename carries every reference to the cluster with it.
///
/// The identifier appears in a global cluster's member ARN and on each
/// of the cluster's instances; leaving either behind means the new name
/// matches nothing and the old one still does.
#[tokio::test]
async fn renaming_a_cluster_carries_its_references() {
    let svc = service();
    call(
        &svc,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", "glob-1"), ("Engine", "docdb")],
    )
    .await;
    call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "clu-old"),
            ("Engine", "docdb"),
            ("GlobalClusterIdentifier", "glob-1"),
        ],
    )
    .await;
    call(
        &svc,
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "inst-1"),
            ("DBClusterIdentifier", "clu-old"),
            ("DBInstanceClass", "db.r5.large"),
            ("Engine", "docdb"),
        ],
    )
    .await;

    call(
        &svc,
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "clu-old"),
            ("NewDBClusterIdentifier", "clu-new"),
        ],
    )
    .await;

    // The global cluster follows the rename.
    let xml = body(
        &call(
            &svc,
            "DescribeGlobalClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "clu-new"),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<GlobalClusterIdentifier>glob-1</GlobalClusterIdentifier>"),
        "the member ARN kept the old name: {xml}"
    );
    let stale = body(
        &call(
            &svc,
            "DescribeGlobalClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "clu-old"),
            ],
        )
        .await,
    );
    assert!(
        !stale.contains("<GlobalClusterIdentifier>"),
        "the old name still selected the global cluster: {stale}"
    );

    // And so do the cluster's instances.
    let xml = body(
        &call(
            &svc,
            "DescribeDBInstances",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "clu-new"),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<DBInstanceIdentifier>inst-1</DBInstanceIdentifier>"),
        "the instance kept the old cluster name: {xml}"
    );

    // And its snapshots.
    call(
        &svc,
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "snap-1"),
            ("DBClusterIdentifier", "clu-new"),
        ],
    )
    .await;
    call(
        &svc,
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "clu-new"),
            ("NewDBClusterIdentifier", "clu-final"),
        ],
    )
    .await;
    let xml = body(
        &call(
            &svc,
            "DescribeDBClusterSnapshots",
            &[("DBClusterIdentifier", "clu-final")],
        )
        .await,
    );
    assert!(
        xml.contains("<DBClusterSnapshotIdentifier>snap-1</DBClusterSnapshotIdentifier>"),
        "the snapshot kept the old cluster name: {xml}"
    );
}

/// A source this account cannot resolve names no cluster.
///
/// Reducing `SourceDBClusterIdentifier` by its last colon turned any
/// colon-bearing value into a local identifier, so another account's
/// cluster ARN resolved against this account's same-named cluster.
#[tokio::test]
async fn create_global_cluster_does_not_alias_a_foreign_source() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "clu-a"), ("Engine", "docdb")],
    )
    .await;

    for source in [
        // Another account's ARN for a cluster THIS account also has.
        "arn:aws:rds:us-east-1:999999999999:cluster:clu-a",
        // An ARN of the wrong resource type.
        "arn:aws:rds:us-east-1:123456789012:db:clu-a",
        // A cluster that simply doesn't exist.
        "ghost",
    ] {
        let err = call_err(
            &svc,
            "CreateGlobalCluster",
            &[
                ("GlobalClusterIdentifier", "glob-x"),
                ("Engine", "docdb"),
                ("SourceDBClusterIdentifier", source),
            ],
        )
        .await;
        assert_eq!(
            err.code(),
            "DBClusterNotFoundFault",
            "source {source} resolved to a local cluster"
        );
    }

    // This account's own ARN still resolves, and seeds the member.
    call(
        &svc,
        "CreateGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "glob-1"),
            ("Engine", "docdb"),
            (
                "SourceDBClusterIdentifier",
                "arn:aws:rds:us-east-1:123456789012:cluster:clu-a",
            ),
        ],
    )
    .await;
    let xml = body(
        &call(
            &svc,
            "DescribeGlobalClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                ("Filters.Filter.1.Values.Value.1", "clu-a"),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<GlobalClusterIdentifier>glob-1</GlobalClusterIdentifier>"),
        "the source was not recorded as a member: {xml}"
    );
}

#[tokio::test]
async fn envelope_shape_is_correct() {
    let svc = service();
    let resp = call(&svc, "DescribeDBClusters", &[]).await;
    let xml = body(&resp);
    assert!(xml.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));
    assert!(xml.contains("<DescribeDBClustersResponse"));
    assert!(xml.contains("<DescribeDBClustersResult>"));
    assert!(
        xml.contains("<ResponseMetadata><RequestId>test-request-id</RequestId></ResponseMetadata>")
    );
    assert!(xml.contains("</DescribeDBClustersResponse>"));
}

#[tokio::test]
async fn cluster_lifecycle_and_resource_shape() {
    let svc = service();
    let resp = call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "my-docdb"), ("Engine", "docdb")],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<DBClusterIdentifier>my-docdb</DBClusterIdentifier>"));
    assert!(xml.contains("arn:aws:rds:us-east-1:123456789012:cluster:my-docdb"));
    assert!(xml.contains("<DbClusterResourceId>cluster-"));
    assert!(xml.contains(".docdb.amazonaws.com</Endpoint>"));
    assert!(xml.contains("cluster-ro-"));
    assert!(xml.contains("<Engine>docdb</Engine>"));
    assert!(xml.contains("<Status>available</Status>"));

    // Describe finds it.
    let resp = call(
        &svc,
        "DescribeDBClusters",
        &[("DBClusterIdentifier", "my-docdb")],
    )
    .await;
    assert!(body(&resp).contains("my-docdb"));

    // Stop -> stopped.
    let resp = call(
        &svc,
        "StopDBCluster",
        &[("DBClusterIdentifier", "my-docdb")],
    )
    .await;
    assert!(body(&resp).contains("<Status>stopped</Status>"));

    // Delete.
    let resp = call(
        &svc,
        "DeleteDBCluster",
        &[("DBClusterIdentifier", "my-docdb")],
    )
    .await;
    assert!(body(&resp).contains("my-docdb"));

    // Now gone.
    let err = call_err(
        &svc,
        "DescribeDBClusters",
        &[("DBClusterIdentifier", "my-docdb")],
    )
    .await;
    assert_eq!(err.code(), "DBClusterNotFoundFault");
    assert_eq!(err.status(), http::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn instance_attaches_to_cluster() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "c1"), ("Engine", "docdb")],
    )
    .await;
    let resp = call(
        &svc,
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "i1"),
            ("DBInstanceClass", "db.r5.large"),
            ("Engine", "docdb"),
            ("DBClusterIdentifier", "c1"),
        ],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<DBInstanceIdentifier>i1</DBInstanceIdentifier>"));
    assert!(xml.contains("<DBClusterIdentifier>c1</DBClusterIdentifier>"));
    assert!(xml.contains("<Address>i1."));

    // Cluster now lists the member as writer.
    let resp = call(&svc, "DescribeDBClusters", &[("DBClusterIdentifier", "c1")]).await;
    let xml = body(&resp);
    assert!(xml.contains("<DBInstanceIdentifier>i1</DBInstanceIdentifier>"));
    assert!(xml.contains("<IsClusterWriter>true</IsClusterWriter>"));

    // Creating an instance against a missing cluster returns the declared fault.
    let err = call_err(
        &svc,
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "i2"),
            ("DBInstanceClass", "db.r5.large"),
            ("Engine", "docdb"),
            ("DBClusterIdentifier", "nope"),
        ],
    )
    .await;
    assert_eq!(err.code(), "DBClusterNotFoundFault");
}

#[tokio::test]
async fn snapshot_and_restore() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "src"), ("Engine", "docdb")],
    )
    .await;
    let resp = call(
        &svc,
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "snap1"),
            ("DBClusterIdentifier", "src"),
        ],
    )
    .await;
    assert!(
        body(&resp).contains("<DBClusterSnapshotIdentifier>snap1</DBClusterSnapshotIdentifier>")
    );

    // Restore into a new cluster.
    let resp = call(
        &svc,
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "restored"),
            ("SnapshotIdentifier", "snap1"),
            ("Engine", "docdb"),
        ],
    )
    .await;
    assert!(body(&resp).contains("<DBClusterIdentifier>restored</DBClusterIdentifier>"));

    let resp = call(
        &svc,
        "DescribeDBClusters",
        &[("DBClusterIdentifier", "restored")],
    )
    .await;
    assert!(body(&resp).contains("restored"));

    // Restore from a missing snapshot -> declared fault.
    let err = call_err(
        &svc,
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "x"),
            ("SnapshotIdentifier", "missing"),
            ("Engine", "docdb"),
        ],
    )
    .await;
    assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault");
}

#[tokio::test]
async fn parameter_group_roundtrip() {
    let svc = service();
    call(
        &svc,
        "CreateDBClusterParameterGroup",
        &[
            ("DBClusterParameterGroupName", "pg1"),
            ("DBParameterGroupFamily", "docdb5.0"),
            ("Description", "test group"),
        ],
    )
    .await;
    call(
        &svc,
        "ModifyDBClusterParameterGroup",
        &[
            ("DBClusterParameterGroupName", "pg1"),
            ("Parameters.Parameter.1.ParameterName", "tls"),
            ("Parameters.Parameter.1.ParameterValue", "disabled"),
            ("Parameters.Parameter.1.ApplyMethod", "pending-reboot"),
        ],
    )
    .await;
    let resp = call(
        &svc,
        "DescribeDBClusterParameters",
        &[("DBClusterParameterGroupName", "pg1")],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<ParameterName>tls</ParameterName>"));
    assert!(xml.contains("<ParameterValue>disabled</ParameterValue>"));
    assert!(xml.contains("<Source>user</Source>"));

    // Missing group -> declared fault (wire code).
    let err = call_err(
        &svc,
        "DescribeDBClusterParameters",
        &[("DBClusterParameterGroupName", "nope")],
    )
    .await;
    assert_eq!(err.code(), "DBParameterGroupNotFound");
}

#[tokio::test]
async fn subnet_group_lifecycle() {
    let svc = service();
    let resp = call(
        &svc,
        "CreateDBSubnetGroup",
        &[
            ("DBSubnetGroupName", "sng"),
            ("DBSubnetGroupDescription", "subnets"),
            ("SubnetIds.SubnetIdentifier.1", "subnet-aaa"),
            ("SubnetIds.SubnetIdentifier.2", "subnet-bbb"),
        ],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<DBSubnetGroupName>sng</DBSubnetGroupName>"));
    assert!(xml.contains("<SubnetIdentifier>subnet-aaa</SubnetIdentifier>"));
    assert!(xml.contains("<SubnetIdentifier>subnet-bbb</SubnetIdentifier>"));

    call(&svc, "DeleteDBSubnetGroup", &[("DBSubnetGroupName", "sng")]).await;
    let err = call_err(
        &svc,
        "DescribeDBSubnetGroups",
        &[("DBSubnetGroupName", "sng")],
    )
    .await;
    assert_eq!(err.code(), "DBSubnetGroupNotFoundFault");
}

#[tokio::test]
async fn global_cluster_lifecycle() {
    let svc = service();
    let resp = call(
        &svc,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", "global1"), ("Engine", "docdb")],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<GlobalClusterIdentifier>global1</GlobalClusterIdentifier>"));
    assert!(xml.contains("arn:aws:rds::123456789012:global-cluster:global1"));

    let resp = call(&svc, "DescribeGlobalClusters", &[]).await;
    assert!(body(&resp).contains("global1"));

    call(
        &svc,
        "DeleteGlobalCluster",
        &[("GlobalClusterIdentifier", "global1")],
    )
    .await;
    let err = call_err(
        &svc,
        "DescribeGlobalClusters",
        &[("GlobalClusterIdentifier", "global1")],
    )
    .await;
    assert_eq!(err.code(), "GlobalClusterNotFoundFault");

    // Over-long identifier is rejected.
    let long = "g".repeat(300);
    let err = call_err(
        &svc,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", &long)],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue");
}

#[tokio::test]
async fn event_subscription_lifecycle() {
    let svc = service();
    let resp = call(
        &svc,
        "CreateEventSubscription",
        &[
            ("SubscriptionName", "sub1"),
            ("SnsTopicArn", "arn:aws:sns:us-east-1:123456789012:topic"),
            ("SourceType", "db-cluster"),
        ],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<CustSubscriptionId>sub1</CustSubscriptionId>"));
    assert!(xml.contains("<SnsTopicArn>arn:aws:sns:us-east-1:123456789012:topic</SnsTopicArn>"));

    call(
        &svc,
        "AddSourceIdentifierToSubscription",
        &[
            ("SubscriptionName", "sub1"),
            ("SourceIdentifier", "my-docdb"),
        ],
    )
    .await;
    let resp = call(
        &svc,
        "DescribeEventSubscriptions",
        &[("SubscriptionName", "sub1")],
    )
    .await;
    assert!(body(&resp).contains("<SourceId>my-docdb</SourceId>"));

    let err = call_err(
        &svc,
        "ModifyEventSubscription",
        &[("SubscriptionName", "missing")],
    )
    .await;
    assert_eq!(err.code(), "SubscriptionNotFound");
}

#[tokio::test]
async fn missing_required_parameter_is_rejected() {
    let svc = service();
    let err = call_err(&svc, "CreateDBCluster", &[]).await;
    assert_eq!(err.code(), "MissingParameter");
    assert_eq!(err.status(), http::StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn describe_events_rejects_invalid_source_type() {
    let svc = service();
    let err = call_err(
        &svc,
        "DescribeEvents",
        &[("SourceType", "__INVALID_ENUM_VALUE__")],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue");

    // A valid source type succeeds.
    let resp = call(&svc, "DescribeEvents", &[("SourceType", "db-cluster")]).await;
    assert!(body(&resp).contains("<Events/>"));
}

/// `CopyTagsToSnapshot` is stored, rendered, toggled by ModifyDBCluster,
/// and hands the cluster's tags to a snapshot taken without its own tags.
#[tokio::test]
async fn copy_tags_to_snapshot_copies_cluster_tags() {
    let svc = service();
    let resp = call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "ctts"),
            ("Engine", "docdb"),
            ("CopyTagsToSnapshot", "true"),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "data"),
        ],
    )
    .await;
    assert!(body(&resp).contains("<CopyTagsToSnapshot>true</CopyTagsToSnapshot>"));

    call(
        &svc,
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "ctts-inherit"),
            ("DBClusterIdentifier", "ctts"),
        ],
    )
    .await;
    let arn = "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:ctts-inherit";
    let tags = body(&call(&svc, "ListTagsForResource", &[("ResourceName", arn)]).await);
    assert!(tags.contains("<Key>team</Key>"), "{tags}");

    // Tags on the snapshot request replace the cluster's.
    call(
        &svc,
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "ctts-own"),
            ("DBClusterIdentifier", "ctts"),
            ("Tags.Tag.1.Key", "own"),
            ("Tags.Tag.1.Value", "yes"),
        ],
    )
    .await;
    let arn = "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:ctts-own";
    let tags = body(&call(&svc, "ListTagsForResource", &[("ResourceName", arn)]).await);
    assert!(tags.contains("<Key>own</Key>") && !tags.contains("<Key>team</Key>"));

    let resp = call(
        &svc,
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "ctts"),
            ("CopyTagsToSnapshot", "false"),
        ],
    )
    .await;
    assert!(body(&resp).contains("<CopyTagsToSnapshot>false</CopyTagsToSnapshot>"));
    call(
        &svc,
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "ctts-off"),
            ("DBClusterIdentifier", "ctts"),
        ],
    )
    .await;
    let arn = "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:ctts-off";
    let tags = body(&call(&svc, "ListTagsForResource", &[("ResourceName", arn)]).await);
    assert!(!tags.contains("<Key>team</Key>"));

    let resp = call(
        &svc,
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "ctts-restored"),
            ("SnapshotIdentifier", "ctts-own"),
            ("Engine", "docdb"),
            ("CopyTagsToSnapshot", "true"),
        ],
    )
    .await;
    assert!(body(&resp).contains("<CopyTagsToSnapshot>true</CopyTagsToSnapshot>"));

    // The final snapshot DeleteDBCluster takes honors the flag too.
    call(
        &svc,
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "ctts"),
            ("CopyTagsToSnapshot", "true"),
        ],
    )
    .await;
    call(
        &svc,
        "DeleteDBCluster",
        &[
            ("DBClusterIdentifier", "ctts"),
            ("FinalDBSnapshotIdentifier", "ctts-final"),
        ],
    )
    .await;
    let arn = "arn:aws:rds:us-east-1:123456789012:cluster-snapshot:ctts-final";
    let tags = body(&call(&svc, "ListTagsForResource", &[("ResourceName", arn)]).await);
    assert!(tags.contains("<Key>team</Key>"), "{tags}");
}

#[tokio::test]
async fn tagging_roundtrip() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "tagged"), ("Engine", "docdb")],
    )
    .await;
    let arn = "arn:aws:rds:us-east-1:123456789012:cluster:tagged";
    call(
        &svc,
        "AddTagsToResource",
        &[
            ("ResourceName", arn),
            ("Tags.Tag.1.Key", "env"),
            ("Tags.Tag.1.Value", "prod"),
        ],
    )
    .await;
    let resp = call(&svc, "ListTagsForResource", &[("ResourceName", arn)]).await;
    let xml = body(&resp);
    assert!(xml.contains("<Key>env</Key>"));
    assert!(xml.contains("<Value>prod</Value>"));

    call(
        &svc,
        "RemoveTagsFromResource",
        &[("ResourceName", arn), ("TagKeys.member.1", "env")],
    )
    .await;
    let resp = call(&svc, "ListTagsForResource", &[("ResourceName", arn)]).await;
    assert!(!body(&resp).contains("<Key>env</Key>"));
}

#[tokio::test]
async fn supported_actions_cover_full_surface() {
    let svc = service();
    assert_eq!(svc.supported_actions().len(), 56);
}

#[tokio::test]
async fn modify_cluster_applies_vpc_sgs_and_log_exports() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "mc"),
            ("Engine", "docdb"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-initial"),
            ("EnableCloudwatchLogsExports.member.1", "audit"),
        ],
    )
    .await;

    let resp = call(
        &svc,
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "mc"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-new-a"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.2", "sg-new-b"),
            (
                "CloudwatchLogsExportConfiguration.EnableLogTypes.member.1",
                "profiler",
            ),
            (
                "CloudwatchLogsExportConfiguration.DisableLogTypes.member.1",
                "audit",
            ),
        ],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<VpcSecurityGroupId>sg-new-a</VpcSecurityGroupId>"));
    assert!(xml.contains("<VpcSecurityGroupId>sg-new-b</VpcSecurityGroupId>"));
    assert!(!xml.contains("sg-initial"));
    assert!(xml.contains("profiler"));
    assert!(!xml.contains("audit"));

    let resp = call(&svc, "DescribeDBClusters", &[("DBClusterIdentifier", "mc")]).await;
    let xml = body(&resp);
    assert!(xml.contains("<VpcSecurityGroupId>sg-new-a</VpcSecurityGroupId>"));
    assert!(xml.contains("<VpcSecurityGroupId>sg-new-b</VpcSecurityGroupId>"));
    assert!(!xml.contains("sg-initial"));
    assert!(xml.contains("profiler"));
    assert!(!xml.contains("audit"));
}

#[tokio::test]
async fn china_region_arns_use_the_china_partition_and_resolve_for_tagging() {
    let svc = service();
    let in_cn = |action: &str, params: &[(&str, &str)]| {
        let mut r = req(action, params);
        r.region = "cn-north-1".to_string();
        r
    };
    let xml = body(
        &svc.handle(in_cn(
            "CreateDBCluster",
            &[("DBClusterIdentifier", "cn-clu"), ("Engine", "docdb")],
        ))
        .await
        .unwrap(),
    );
    let arn = "arn:aws-cn:rds:cn-north-1:123456789012:cluster:cn-clu";
    assert!(
        xml.contains(&format!("<DBClusterArn>{arn}</DBClusterArn>")),
        "{xml}"
    );
    svc.handle(in_cn(
        "AddTagsToResource",
        &[
            ("ResourceName", arn),
            ("Tags.Tag.1.Key", "team"),
            ("Tags.Tag.1.Value", "data"),
        ],
    ))
    .await
    .unwrap();
    let xml = body(
        &svc.handle(in_cn("ListTagsForResource", &[("ResourceName", arn)]))
            .await
            .unwrap(),
    );
    assert!(xml.contains("<Key>team</Key>"), "{xml}");

    let xml = body(
        &svc.handle(in_cn(
            "CreateGlobalCluster",
            &[("GlobalClusterIdentifier", "cn-glob"), ("Engine", "docdb")],
        ))
        .await
        .unwrap(),
    );
    assert!(
        xml.contains("arn:aws-cn:rds::123456789012:global-cluster:cn-glob"),
        "{xml}"
    );
}

#[tokio::test]
async fn certificate_arns_use_the_queried_region_and_its_partition() {
    let svc = service();
    for (region, expected) in [
        (
            "us-west-2",
            "<CertificateArn>arn:aws:rds:us-west-2::cert:rds-ca-rsa2048-g1</CertificateArn>",
        ),
        (
            "cn-north-1",
            "<CertificateArn>arn:aws-cn:rds:cn-north-1::cert:rds-ca-rsa2048-g1</CertificateArn>",
        ),
    ] {
        let mut r = req("DescribeCertificates", &[]);
        r.region = region.to_string();
        let xml = body(&svc.handle(r).await.unwrap());
        assert!(xml.contains(expected), "{region}: {xml}");
    }
}

/// The `<KmsKeyId>` a response reports, if any.
fn reported_key(xml: &str) -> Option<String> {
    let start = xml.find("<KmsKeyId>")? + "<KmsKeyId>".len();
    let end = xml[start..].find("</KmsKeyId>")? + start;
    Some(xml[start..end].to_string())
}

/// Storage encrypted without a named key reports the account's AWS-managed
/// `aws/rds` key for the region (a real KMS key), as on AWS; the key follows
/// the cluster into its member instances, snapshots, copies and restores, and
/// a restore that names a key is encrypted with that key's ARN.
#[tokio::test]
async fn encrypted_storage_reports_the_aws_managed_rds_key() {
    let acct = "123456789012";
    let (kms, hook) = fakecloud_kms::test_support::kms_hook(acct);
    let svc = service().with_kms_hook(hook);
    let xml = body(
        &call(
            &svc,
            "CreateDBCluster",
            &[
                ("DBClusterIdentifier", "enc"),
                ("Engine", "docdb"),
                ("StorageEncrypted", "true"),
            ],
        )
        .await,
    );
    let key = reported_key(&xml).expect("encrypted cluster reports a key");
    fakecloud_kms::test_support::assert_aws_managed_key(
        &kms,
        acct,
        "us-east-1",
        &key,
        "alias/aws/rds",
    );
    let described = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[("DBClusterIdentifier", "enc")],
        )
        .await,
    );
    assert_eq!(reported_key(&described).as_deref(), Some(key.as_str()));

    // An unencrypted cluster reports no key.
    let plain = body(
        &call(
            &svc,
            "CreateDBCluster",
            &[("DBClusterIdentifier", "plain"), ("Engine", "docdb")],
        )
        .await,
    );
    assert_eq!(reported_key(&plain), None, "{plain}");

    // A member instance's storage is the cluster's.
    let inst = body(
        &call(
            &svc,
            "CreateDBInstance",
            &[
                ("DBInstanceIdentifier", "enc-1"),
                ("DBInstanceClass", "db.r5.large"),
                ("Engine", "docdb"),
                ("DBClusterIdentifier", "enc"),
            ],
        )
        .await,
    );
    assert!(
        inst.contains("<StorageEncrypted>true</StorageEncrypted>"),
        "{inst}"
    );
    assert_eq!(reported_key(&inst).as_deref(), Some(key.as_str()));

    // Snapshot, copy and restores carry the key.
    let snap = body(
        &call(
            &svc,
            "CreateDBClusterSnapshot",
            &[
                ("DBClusterSnapshotIdentifier", "s1"),
                ("DBClusterIdentifier", "enc"),
            ],
        )
        .await,
    );
    assert_eq!(reported_key(&snap).as_deref(), Some(key.as_str()));
    let copy = body(
        &call(
            &svc,
            "CopyDBClusterSnapshot",
            &[
                ("SourceDBClusterSnapshotIdentifier", "s1"),
                ("TargetDBClusterSnapshotIdentifier", "s1-copy"),
            ],
        )
        .await,
    );
    assert_eq!(reported_key(&copy).as_deref(), Some(key.as_str()));
    let restored = body(
        &call(
            &svc,
            "RestoreDBClusterFromSnapshot",
            &[
                ("DBClusterIdentifier", "restored"),
                ("SnapshotIdentifier", "s1"),
                ("Engine", "docdb"),
            ],
        )
        .await,
    );
    assert_eq!(reported_key(&restored).as_deref(), Some(key.as_str()));
    let pitr = body(
        &call(
            &svc,
            "RestoreDBClusterToPointInTime",
            &[
                ("DBClusterIdentifier", "pitr"),
                ("SourceDBClusterIdentifier", "enc"),
            ],
        )
        .await,
    );
    assert_eq!(reported_key(&pitr).as_deref(), Some(key.as_str()));

    // Restoring an unencrypted snapshot with a named key encrypts the new
    // cluster, reporting the key's ARN rather than the alias given.
    call(
        &svc,
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "plain-snap"),
            ("DBClusterIdentifier", "plain"),
        ],
    )
    .await;
    let named = body(
        &call(
            &svc,
            "RestoreDBClusterFromSnapshot",
            &[
                ("DBClusterIdentifier", "named"),
                ("SnapshotIdentifier", "plain-snap"),
                ("Engine", "docdb"),
                ("KmsKeyId", "alias/aws/rds"),
            ],
        )
        .await,
    );
    assert!(
        named.contains("<StorageEncrypted>true</StorageEncrypted>"),
        "{named}"
    );
    assert_eq!(reported_key(&named).as_deref(), Some(key.as_str()));
}

/// Encryption resolves the key per region: a cluster in another region
/// reports that region's own AWS-managed key.
#[tokio::test]
async fn aws_managed_rds_key_is_per_region() {
    let acct = "123456789012";
    let (kms, hook) = fakecloud_kms::test_support::kms_hook(acct);
    let svc = service().with_kms_hook(hook);
    let mut request = req(
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "west"),
            ("Engine", "docdb"),
            ("StorageEncrypted", "true"),
        ],
    );
    request.region = "us-west-2".to_string();
    let xml = body(&svc.handle(request).await.expect("handler ok"));
    let key = reported_key(&xml).expect("encrypted cluster reports a key");
    fakecloud_kms::test_support::assert_aws_managed_key(
        &kms,
        acct,
        "us-west-2",
        &key,
        "alias/aws/rds",
    );
}

/// An unencrypted cluster snapshot cannot be encrypted by copying it: a copy
/// that names a KmsKeyId fails (and records nothing), while a plain copy
/// succeeds unencrypted.
#[tokio::test]
async fn copying_an_unencrypted_cluster_snapshot_with_a_key_fails() {
    let (kms, hook) = fakecloud_kms::test_support::kms_hook("123456789012");
    let svc = service().with_kms_hook(hook);
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "plain"), ("Engine", "docdb")],
    )
    .await;
    call(
        &svc,
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "plain-snap"),
            ("DBClusterIdentifier", "plain"),
        ],
    )
    .await;
    let err = call_err(
        &svc,
        "CopyDBClusterSnapshot",
        &[
            ("SourceDBClusterSnapshotIdentifier", "plain-snap"),
            ("TargetDBClusterSnapshotIdentifier", "keyed-copy"),
            ("KmsKeyId", "alias/aws/rds"),
        ],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterCombination");
    // The rejected copy minted no AWS-managed key.
    assert!(kms
        .read()
        .get("123456789012")
        .is_none_or(|s| s.keys.is_empty()));
    let missing = call_err(
        &svc,
        "DescribeDBClusterSnapshots",
        &[("DBClusterSnapshotIdentifier", "keyed-copy")],
    )
    .await;
    assert_eq!(missing.code(), "DBClusterSnapshotNotFoundFault");
    let copy = body(
        &call(
            &svc,
            "CopyDBClusterSnapshot",
            &[
                ("SourceDBClusterSnapshotIdentifier", "plain-snap"),
                ("TargetDBClusterSnapshotIdentifier", "plain-copy"),
            ],
        )
        .await,
    );
    assert!(
        copy.contains("<StorageEncrypted>false</StorageEncrypted>"),
        "{copy}"
    );
    assert_eq!(reported_key(&copy), None);
}

/// ModifyDBCluster ignored `StorageType` and
/// `ServerlessV2ScalingConfiguration`, and CreateDBCluster dropped the
/// latter; both are now stored and read back.
#[tokio::test]
async fn cluster_storage_type_and_serverless_round_trip() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "st"),
            ("Engine", "docdb"),
            ("ServerlessV2ScalingConfiguration.MinCapacity", "0.5"),
            ("ServerlessV2ScalingConfiguration.MaxCapacity", "16"),
        ],
    )
    .await;
    let xml = body(&call(&svc, "DescribeDBClusters", &[("DBClusterIdentifier", "st")]).await);
    assert!(xml.contains("<StorageType>standard</StorageType>"), "{xml}");
    assert!(
        xml.contains(
            "<ServerlessV2ScalingConfiguration><MinCapacity>0.5</MinCapacity>\
             <MaxCapacity>16</MaxCapacity></ServerlessV2ScalingConfiguration>"
        ),
        "{xml}"
    );

    call(
        &svc,
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "st"),
            ("StorageType", "iopt1"),
            ("ServerlessV2ScalingConfiguration.MinCapacity", "1"),
        ],
    )
    .await;
    let xml = body(&call(&svc, "DescribeDBClusters", &[("DBClusterIdentifier", "st")]).await);
    assert!(xml.contains("<StorageType>iopt1</StorageType>"), "{xml}");
    assert!(
        xml.contains("<MinCapacity>1</MinCapacity><MaxCapacity>16</MaxCapacity>"),
        "{xml}"
    );
}

/// DescribeServerlessV2PlatformVersions lists active versions by default,
/// disabled ones only under `IncludeAll`, narrows by version/engine/default,
/// and a serverless cluster reports the default version it runs on.
#[tokio::test]
async fn serverless_v2_platform_versions_and_cluster_platform() {
    let svc = service();
    let xml = body(&call(&svc, "DescribeServerlessV2PlatformVersions", &[]).await);
    assert_eq!(
        xml.matches("<ServerlessV2PlatformVersionInfo>").count(),
        1,
        "{xml}"
    );
    assert!(
        xml.contains("<ServerlessV2PlatformVersion>2</ServerlessV2PlatformVersion>"),
        "{xml}"
    );
    assert!(xml.contains("<IsDefault>true</IsDefault>"), "{xml}");
    assert!(xml.contains("<Status>enabled</Status>"), "{xml}");

    let all = body(
        &call(
            &svc,
            "DescribeServerlessV2PlatformVersions",
            &[("IncludeAll", "true")],
        )
        .await,
    );
    assert_eq!(
        all.matches("<ServerlessV2PlatformVersionInfo>").count(),
        2,
        "{all}"
    );
    assert!(all.contains("<Status>disabled</Status>"), "{all}");

    let only_v1 = body(
        &call(
            &svc,
            "DescribeServerlessV2PlatformVersions",
            &[
                ("IncludeAll", "true"),
                ("DefaultOnly", "true"),
                ("ServerlessV2PlatformVersion", "1"),
            ],
        )
        .await,
    );
    assert!(
        !only_v1.contains("<ServerlessV2PlatformVersionInfo>"),
        "{only_v1}"
    );
    let other_engine = body(
        &call(
            &svc,
            "DescribeServerlessV2PlatformVersions",
            &[("Engine", "mysql")],
        )
        .await,
    );
    assert!(
        !other_engine.contains("<ServerlessV2PlatformVersionInfo>"),
        "{other_engine}"
    );

    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "prov"), ("Engine", "docdb")],
    )
    .await;
    let prov = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[("DBClusterIdentifier", "prov")],
        )
        .await,
    );
    assert!(!prov.contains("<ServerlessV2PlatformVersion>"), "{prov}");
    call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "sls"),
            ("Engine", "docdb"),
            ("ServerlessV2ScalingConfiguration.MinCapacity", "0.5"),
            ("ServerlessV2ScalingConfiguration.MaxCapacity", "8"),
        ],
    )
    .await;
    let sls = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[("DBClusterIdentifier", "sls")],
        )
        .await,
    );
    assert!(
        sls.contains("<ServerlessV2PlatformVersion>2</ServerlessV2PlatformVersion>"),
        "{sls}"
    );
}
