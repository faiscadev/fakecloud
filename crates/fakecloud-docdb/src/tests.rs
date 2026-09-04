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
    assert_eq!(svc.supported_actions().len(), 55);
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
