use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::{AwsRequest, AwsResponse};

use super::*;

fn service() -> NeptuneService {
    let state: SharedNeptuneState = Arc::new(RwLock::new(MultiAccountState::new(
        "123456789012",
        "us-east-1",
        "http://localhost",
    )));
    NeptuneService::new(state)
}

fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut query_params = HashMap::new();
    for (k, v) in params {
        query_params.insert((*k).to_string(), (*v).to_string());
    }
    AwsRequest {
        service: "neptune".to_string(),
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

async fn call(svc: &NeptuneService, action: &str, params: &[(&str, &str)]) -> AwsResponse {
    svc.handle(req(action, params)).await.expect("handler ok")
}

async fn call_err(
    svc: &NeptuneService,
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
        &[("DBClusterIdentifier", "my-neptune"), ("Engine", "neptune")],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<DBClusterIdentifier>my-neptune</DBClusterIdentifier>"));
    assert!(xml.contains("arn:aws:rds:us-east-1:123456789012:cluster:my-neptune"));
    assert!(xml.contains("<DbClusterResourceId>cluster-"));
    assert!(xml.contains(".neptune.amazonaws.com</Endpoint>"));
    assert!(xml.contains("cluster-ro-"));
    assert!(xml.contains("<Engine>neptune</Engine>"));
    assert!(xml.contains("<Status>available</Status>"));

    // Describe finds it.
    let resp = call(
        &svc,
        "DescribeDBClusters",
        &[("DBClusterIdentifier", "my-neptune")],
    )
    .await;
    assert!(body(&resp).contains("my-neptune"));

    // Stop -> stopped.
    let resp = call(
        &svc,
        "StopDBCluster",
        &[("DBClusterIdentifier", "my-neptune")],
    )
    .await;
    assert!(body(&resp).contains("<Status>stopped</Status>"));

    // Delete.
    let resp = call(
        &svc,
        "DeleteDBCluster",
        &[("DBClusterIdentifier", "my-neptune")],
    )
    .await;
    assert!(body(&resp).contains("my-neptune"));

    // Now gone.
    let err = call_err(
        &svc,
        "DescribeDBClusters",
        &[("DBClusterIdentifier", "my-neptune")],
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
        &[("DBClusterIdentifier", "c1"), ("Engine", "neptune")],
    )
    .await;
    let resp = call(
        &svc,
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "i1"),
            ("DBInstanceClass", "db.r5.large"),
            ("Engine", "neptune"),
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
            ("Engine", "neptune"),
            ("DBClusterIdentifier", "nope"),
        ],
    )
    .await;
    assert_eq!(err.code(), "DBClusterNotFoundFault");
}

#[tokio::test]
async fn cluster_endpoint_lifecycle() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "ce-cluster"), ("Engine", "neptune")],
    )
    .await;
    let resp = call(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterIdentifier", "ce-cluster"),
            ("DBClusterEndpointIdentifier", "custom-ep"),
            ("EndpointType", "READER"),
        ],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<DBClusterEndpointIdentifier>custom-ep</DBClusterEndpointIdentifier>"));
    assert!(xml.contains("<EndpointType>READER</EndpointType>"));
    assert!(xml.contains(".cluster-custom-"));
    assert!(xml.contains("arn:aws:rds:us-east-1:123456789012:cluster-endpoint:custom-ep"));

    // Modify.
    let resp = call(
        &svc,
        "ModifyDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "custom-ep"),
            ("EndpointType", "ANY"),
        ],
    )
    .await;
    assert!(body(&resp).contains("<EndpointType>ANY</EndpointType>"));

    // Describe.
    let resp = call(&svc, "DescribeDBClusterEndpoints", &[]).await;
    assert!(body(&resp)
        .contains("<DBClusterEndpointIdentifier>custom-ep</DBClusterEndpointIdentifier>"));

    // Delete.
    call(
        &svc,
        "DeleteDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", "custom-ep")],
    )
    .await;
    // Describe with an unknown endpoint filter resolves to an empty list
    // (DescribeDBClusterEndpoints declares no endpoint-not-found fault).
    let resp = call(
        &svc,
        "DescribeDBClusterEndpoints",
        &[("DBClusterEndpointIdentifier", "custom-ep")],
    )
    .await;
    assert!(!body(&resp).contains("<DBClusterEndpointIdentifier>custom-ep"));

    // Deleting an unknown endpoint returns the declared fault.
    let err = call_err(
        &svc,
        "DeleteDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", "custom-ep")],
    )
    .await;
    assert_eq!(err.code(), "DBClusterEndpointNotFoundFault");

    // Endpoint against a missing cluster returns the cluster fault.
    let err = call_err(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterIdentifier", "nope"),
            ("DBClusterEndpointIdentifier", "ep2"),
            ("EndpointType", "READER"),
        ],
    )
    .await;
    assert_eq!(err.code(), "DBClusterNotFoundFault");
}

#[tokio::test]
async fn cluster_role_association() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "r-cluster"), ("Engine", "neptune")],
    )
    .await;
    let arn = "arn:aws:iam::123456789012:role/NeptuneLoad";
    call(
        &svc,
        "AddRoleToDBCluster",
        &[("DBClusterIdentifier", "r-cluster"), ("RoleArn", arn)],
    )
    .await;
    let resp = call(
        &svc,
        "DescribeDBClusters",
        &[("DBClusterIdentifier", "r-cluster")],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<AssociatedRoles><DBClusterRole>"));
    assert!(xml.contains(arn));

    // Adding the same role again is rejected.
    let err = call_err(
        &svc,
        "AddRoleToDBCluster",
        &[("DBClusterIdentifier", "r-cluster"), ("RoleArn", arn)],
    )
    .await;
    assert_eq!(err.code(), "DBClusterRoleAlreadyExists");

    // Remove it.
    call(
        &svc,
        "RemoveRoleFromDBCluster",
        &[("DBClusterIdentifier", "r-cluster"), ("RoleArn", arn)],
    )
    .await;
    let err = call_err(
        &svc,
        "RemoveRoleFromDBCluster",
        &[("DBClusterIdentifier", "r-cluster"), ("RoleArn", arn)],
    )
    .await;
    assert_eq!(err.code(), "DBClusterRoleNotFound");
}

#[tokio::test]
async fn snapshot_and_restore() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "src"), ("Engine", "neptune")],
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
            ("Engine", "neptune"),
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
            ("Engine", "neptune"),
        ],
    )
    .await;
    assert_eq!(err.code(), "DBClusterSnapshotNotFoundFault");
}

#[tokio::test]
async fn cluster_parameter_group_roundtrip() {
    let svc = service();
    call(
        &svc,
        "CreateDBClusterParameterGroup",
        &[
            ("DBClusterParameterGroupName", "pg1"),
            ("DBParameterGroupFamily", "neptune1.3"),
            ("Description", "test group"),
        ],
    )
    .await;
    call(
        &svc,
        "ModifyDBClusterParameterGroup",
        &[
            ("DBClusterParameterGroupName", "pg1"),
            (
                "Parameters.Parameter.1.ParameterName",
                "neptune_query_timeout",
            ),
            ("Parameters.Parameter.1.ParameterValue", "60000"),
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
    assert!(xml.contains("<ParameterName>neptune_query_timeout</ParameterName>"));
    assert!(xml.contains("<ParameterValue>60000</ParameterValue>"));
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
async fn db_parameter_group_roundtrip() {
    let svc = service();
    call(
        &svc,
        "CreateDBParameterGroup",
        &[
            ("DBParameterGroupName", "dpg1"),
            ("DBParameterGroupFamily", "neptune1.3"),
            ("Description", "instance group"),
        ],
    )
    .await;
    call(
        &svc,
        "ModifyDBParameterGroup",
        &[
            ("DBParameterGroupName", "dpg1"),
            (
                "Parameters.Parameter.1.ParameterName",
                "neptune_query_timeout",
            ),
            ("Parameters.Parameter.1.ParameterValue", "90000"),
        ],
    )
    .await;
    let resp = call(
        &svc,
        "DescribeDBParameters",
        &[("DBParameterGroupName", "dpg1")],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<ParameterValue>90000</ParameterValue>"));

    let resp = call(&svc, "DescribeDBParameterGroups", &[]).await;
    assert!(body(&resp).contains("<DBParameterGroupName>dpg1</DBParameterGroupName>"));

    call(
        &svc,
        "DeleteDBParameterGroup",
        &[("DBParameterGroupName", "dpg1")],
    )
    .await;
    let err = call_err(
        &svc,
        "DescribeDBParameters",
        &[("DBParameterGroupName", "dpg1")],
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
        &[
            ("GlobalClusterIdentifier", "global1"),
            ("Engine", "neptune"),
        ],
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
            ("SourceIdentifier", "my-neptune"),
        ],
    )
    .await;
    let resp = call(
        &svc,
        "DescribeEventSubscriptions",
        &[("SubscriptionName", "sub1")],
    )
    .await;
    assert!(body(&resp).contains("<SourceId>my-neptune</SourceId>"));

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
        &[("DBClusterIdentifier", "tagged"), ("Engine", "neptune")],
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
    assert_eq!(svc.supported_actions().len(), 70);
}

#[tokio::test]
async fn modify_cluster_applies_vpc_sgs_and_log_exports() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "mc"),
            ("Engine", "neptune"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-initial"),
            ("EnableCloudwatchLogsExports.member.1", "audit"),
        ],
    )
    .await;

    // Modify: swap the SGs and enable an extra log type while disabling audit.
    let resp = call(
        &svc,
        "ModifyDBCluster",
        &[
            ("DBClusterIdentifier", "mc"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.1", "sg-new-a"),
            ("VpcSecurityGroupIds.VpcSecurityGroupId.2", "sg-new-b"),
            (
                "CloudwatchLogsExportConfiguration.EnableLogTypes.member.1",
                "slowquery",
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
    assert!(xml.contains("slowquery"));
    assert!(!xml.contains("audit"));

    // Persisted: Describe reflects the modified values.
    let resp = call(&svc, "DescribeDBClusters", &[("DBClusterIdentifier", "mc")]).await;
    let xml = body(&resp);
    assert!(xml.contains("<VpcSecurityGroupId>sg-new-a</VpcSecurityGroupId>"));
    assert!(xml.contains("<VpcSecurityGroupId>sg-new-b</VpcSecurityGroupId>"));
    assert!(!xml.contains("sg-initial"));
    assert!(xml.contains("slowquery"));
    assert!(!xml.contains("audit"));
}
