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

/// `Filters` was accepted and ignored, so a caller narrowing a listing
/// got every resource in the account back.
#[tokio::test]
async fn describe_db_clusters_honors_its_filters() {
    let svc = service();
    for id in ["clu-a", "clu-b"] {
        call(
            &svc,
            "CreateDBCluster",
            &[("DBClusterIdentifier", id), ("Engine", "neptune")],
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

    // The ARN form selects the same cluster.
    let xml = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[
                ("Filters.Filter.1.Name", "db-cluster-id"),
                (
                    "Filters.Filter.1.Values.Value.1",
                    "arn:aws:rds:us-east-1:123456789012:cluster:clu-b",
                ),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<DBClusterIdentifier>clu-b</DBClusterIdentifier>"),
        "{xml}"
    );

    // `engine` selects both; a different engine selects neither.
    let xml = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[
                ("Filters.Filter.1.Name", "engine"),
                ("Filters.Filter.1.Values.Value.1", "neptune"),
            ],
        )
        .await,
    );
    assert_eq!(xml.matches("<DBClusterIdentifier>").count(), 2, "{xml}");
    let xml = body(
        &call(
            &svc,
            "DescribeDBClusters",
            &[
                ("Filters.Filter.1.Name", "engine"),
                ("Filters.Filter.1.Values.Value.1", "aurora"),
            ],
        )
        .await,
    );
    assert!(!xml.contains("<DBClusterIdentifier>"), "{xml}");

    // An unrecognized name matches nothing rather than returning the
    // full list.
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
    assert!(!xml.contains("<DBClusterIdentifier>"), "{xml}");
}

#[tokio::test]
async fn describe_db_instances_honors_its_filters() {
    let svc = service();
    for cluster in ["clu-a", "clu-b"] {
        call(
            &svc,
            "CreateDBCluster",
            &[("DBClusterIdentifier", cluster), ("Engine", "neptune")],
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
                ("Engine", "neptune"),
            ],
        )
        .await;
    }

    // By the cluster the instance belongs to, as identifier and as ARN.
    for value in ["clu-a", "arn:aws:rds:us-east-1:123456789012:cluster:clu-a"] {
        let xml = body(
            &call(
                &svc,
                "DescribeDBInstances",
                &[
                    ("Filters.Filter.1.Name", "db-cluster-id"),
                    ("Filters.Filter.1.Values.Value.1", value),
                ],
            )
            .await,
        );
        assert!(
            xml.contains("<DBInstanceIdentifier>inst-a</DBInstanceIdentifier>"),
            "db-cluster-id={value} selected nothing: {xml}"
        );
        assert!(
            !xml.contains("<DBInstanceIdentifier>inst-b</DBInstanceIdentifier>"),
            "db-cluster-id={value} kept the other cluster's instance: {xml}"
        );
    }

    // `engine` selects both; a different engine selects neither.
    let xml = body(
        &call(
            &svc,
            "DescribeDBInstances",
            &[
                ("Filters.Filter.1.Name", "engine"),
                ("Filters.Filter.1.Values.Value.1", "neptune"),
            ],
        )
        .await,
    );
    assert_eq!(xml.matches("<DBInstanceIdentifier>").count(), 2, "{xml}");
    let xml = body(
        &call(
            &svc,
            "DescribeDBInstances",
            &[
                ("Filters.Filter.1.Name", "engine"),
                ("Filters.Filter.1.Values.Value.1", "docdb"),
            ],
        )
        .await,
    );
    assert!(!xml.contains("<DBInstanceIdentifier>"), "{xml}");

    // An unrecognized name matches nothing rather than returning all.
    let xml = body(
        &call(
            &svc,
            "DescribeDBInstances",
            &[
                ("Filters.Filter.1.Name", "not-a-filter"),
                ("Filters.Filter.1.Values.Value.1", "inst-a"),
            ],
        )
        .await,
    );
    assert!(!xml.contains("<DBInstanceIdentifier>"), "{xml}");
}

#[tokio::test]
async fn describe_db_cluster_endpoints_honors_its_filters() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "clu-1"), ("Engine", "neptune")],
    )
    .await;
    // TWO endpoints, differing in custom type: with only one, every
    // positive assertion below passes even when the predicate is
    // ignored entirely.
    call(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("DBClusterEndpointIdentifier", "ep-1"),
            ("EndpointType", "READER"),
        ],
    )
    .await;
    call(
        &svc,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterIdentifier", "clu-1"),
            ("DBClusterEndpointIdentifier", "ep-2"),
            ("EndpointType", "ANY"),
        ],
    )
    .await;

    // The documented values are lowercase while the API returns them
    // uppercase, so an exact comparison would select nothing for a
    // caller copying the documented command. `db-cluster-endpoint-type`
    // is the case that actually folds: it is stored as sent (READER) and
    // queried as documented (reader).
    for (name, value) in [
        ("db-cluster-endpoint-id", "ep-1"),
        ("db-cluster-endpoint-status", "available"),
        // The endpoint reads back as CUSTOM; the READER it was created
        // with is its custom type. Both spellings of each, since the
        // documented filter values are lowercase while the API returns
        // uppercase.
        ("db-cluster-endpoint-type", "custom"),
        ("db-cluster-endpoint-type", "CUSTOM"),
        ("db-cluster-endpoint-custom-type", "reader"),
        ("db-cluster-endpoint-custom-type", "READER"),
    ] {
        let xml = body(
            &call(
                &svc,
                "DescribeDBClusterEndpoints",
                &[
                    ("Filters.Filter.1.Name", name),
                    ("Filters.Filter.1.Values.Value.1", value),
                ],
            )
            .await,
        );
        assert!(
            xml.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
            "{name}={value} selected nothing: {xml}"
        );
        // `db-cluster-endpoint-type` and `-status` are shared by both
        // endpoints; the id and custom-type filters name ep-1 alone, so
        // ep-2 has to be excluded for those.
        if name == "db-cluster-endpoint-id" || name == "db-cluster-endpoint-custom-type" {
            assert!(
                !xml.contains("<DBClusterEndpointIdentifier>ep-2</DBClusterEndpointIdentifier>"),
                "{name}={value} also returned ep-2: {xml}"
            );
        }
    }

    // The other endpoint's custom type selects it and not ep-1.
    let xml = body(
        &call(
            &svc,
            "DescribeDBClusterEndpoints",
            &[
                ("Filters.Filter.1.Name", "db-cluster-endpoint-custom-type"),
                ("Filters.Filter.1.Values.Value.1", "any"),
            ],
        )
        .await,
    );
    assert!(
        xml.contains("<DBClusterEndpointIdentifier>ep-2</DBClusterEndpointIdentifier>"),
        "{xml}"
    );
    assert!(
        !xml.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "the custom-type filter returned both endpoints: {xml}"
    );

    // The cluster's built-in endpoints are reported too, which is what
    // makes `db-cluster-endpoint-type=reader` able to match at all --
    // CreateDBClusterEndpoint only ever makes CUSTOM ones.
    for value in ["reader", "writer"] {
        let xml = body(
            &call(
                &svc,
                "DescribeDBClusterEndpoints",
                &[
                    ("Filters.Filter.1.Name", "db-cluster-endpoint-type"),
                    ("Filters.Filter.1.Values.Value.1", value),
                ],
            )
            .await,
        );
        assert!(
            xml.contains(&format!(
                "<EndpointType>{}</EndpointType>",
                value.to_uppercase()
            )),
            "db-cluster-endpoint-type={value} found no built-in endpoint: {xml}"
        );
        // A built-in has no identifier of its own, so NEITHER custom
        // endpoint should come back under this filter.
        assert!(
            !xml.contains("<DBClusterEndpointIdentifier>"),
            "db-cluster-endpoint-type={value} returned a custom endpoint: {xml}"
        );
        // And the built-in renders no empty optional members: an empty
        // identifier or ARN reads as a resource named "".
        for tag in [
            "DBClusterEndpointIdentifier",
            "DBClusterEndpointResourceIdentifier",
            "CustomEndpointType",
            "DBClusterEndpointArn",
        ] {
            assert!(
                !xml.contains(&format!("<{tag}></{tag}>")),
                "built-in rendered an empty <{tag}>: {xml}"
            );
        }
    }

    // MaxRecords / Marker are modeled here and were never honored.
    // Rows are identified by their Endpoint address: a built-in carries
    // no identifier, so that is the only per-row identity, and
    // comparing whole response bodies would pass on nothing more than a
    // differing <Marker>.
    let addresses = |xml: &str| -> Vec<String> {
        xml.split("<Endpoint>")
            .skip(1)
            .filter_map(|rest| rest.split("</Endpoint>").next())
            .map(str::to_string)
            .collect()
    };
    let unpaged = addresses(&body(&call(&svc, "DescribeDBClusterEndpoints", &[]).await));
    assert!(
        unpaged.len() >= 4,
        "expected two custom endpoints and the cluster's two built-ins: {unpaged:?}"
    );

    // Walk to exhaustion one row at a time.
    let mut seen: Vec<String> = Vec::new();
    let mut marker: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut params: Vec<(&str, &str)> = vec![("MaxRecords", "1")];
        let held;
        if let Some(value) = marker.as_deref() {
            held = value.to_string();
            params.push(("Marker", &held));
        }
        let xml = body(&call(&svc, "DescribeDBClusterEndpoints", &params).await);
        let rows = addresses(&xml);
        assert_eq!(
            rows.len(),
            1,
            "MaxRecords=1 returned {} rows: {xml}",
            rows.len()
        );
        seen.extend(rows);
        pages += 1;
        assert!(pages <= unpaged.len(), "pagination did not terminate");
        marker = xml
            .split("<Marker>")
            .nth(1)
            .and_then(|rest| rest.split("</Marker>").next())
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        if marker.is_none() {
            break;
        }
    }

    // Every row, once, in the unpaginated order -- built-ins included.
    assert_eq!(
        seen, unpaged,
        "the paged walk did not reproduce the unpaginated listing"
    );
    let mut unique = seen.clone();
    unique.sort();
    unique.dedup();
    assert_eq!(
        unique.len(),
        seen.len(),
        "a row was returned twice: {seen:?}"
    );

    // A status the endpoint is not in selects nothing.
    let xml = body(
        &call(
            &svc,
            "DescribeDBClusterEndpoints",
            &[
                ("Filters.Filter.1.Name", "db-cluster-endpoint-status"),
                ("Filters.Filter.1.Values.Value.1", "deleting"),
            ],
        )
        .await,
    );
    assert!(
        !xml.contains("<DBClusterEndpointIdentifier>ep-1</DBClusterEndpointIdentifier>"),
        "a non-matching status still returned the endpoint: {xml}"
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
async fn failover_flips_cluster_writer() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[("DBClusterIdentifier", "c1"), ("Engine", "neptune")],
    )
    .await;
    for id in ["writer", "reader"] {
        call(
            &svc,
            "CreateDBInstance",
            &[
                ("DBInstanceIdentifier", id),
                ("DBInstanceClass", "db.r5.large"),
                ("Engine", "neptune"),
                ("DBClusterIdentifier", "c1"),
            ],
        )
        .await;
    }
    // The first instance is the writer.
    {
        let st = svc.state.read();
        let members = &st
            .get("123456789012")
            .unwrap()
            .clusters
            .get("c1")
            .unwrap()
            .members;
        assert!(
            members
                .iter()
                .find(|m| m.db_instance_identifier == "writer")
                .unwrap()
                .is_writer
        );
        assert!(
            !members
                .iter()
                .find(|m| m.db_instance_identifier == "reader")
                .unwrap()
                .is_writer
        );
    }

    // Failover with an explicit target promotes it.
    call(
        &svc,
        "FailoverDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("TargetDBInstanceIdentifier", "reader"),
        ],
    )
    .await;
    let st = svc.state.read();
    let members = &st
        .get("123456789012")
        .unwrap()
        .clusters
        .get("c1")
        .unwrap()
        .members;
    assert!(
        members
            .iter()
            .find(|m| m.db_instance_identifier == "reader")
            .unwrap()
            .is_writer
    );
    assert!(
        !members
            .iter()
            .find(|m| m.db_instance_identifier == "writer")
            .unwrap()
            .is_writer
    );
}

#[tokio::test]
async fn instance_inherits_cluster_encryption_and_modify_persists_fields() {
    let svc = service();
    call(
        &svc,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "enc"),
            ("Engine", "neptune"),
            ("StorageEncrypted", "true"),
        ],
    )
    .await;
    let resp = call(
        &svc,
        "CreateDBInstance",
        &[
            ("DBInstanceIdentifier", "i1"),
            ("DBInstanceClass", "db.r5.large"),
            ("Engine", "neptune"),
            ("DBClusterIdentifier", "enc"),
        ],
    )
    .await;
    // The instance inherits the cluster's encryption rather than hardcoding false.
    assert!(body(&resp).contains("<StorageEncrypted>true</StorageEncrypted>"));

    // ModifyDBInstance persists PubliclyAccessible + EngineVersion.
    let resp = call(
        &svc,
        "ModifyDBInstance",
        &[
            ("DBInstanceIdentifier", "i1"),
            ("PubliclyAccessible", "true"),
            ("EngineVersion", "1.3.0.0"),
        ],
    )
    .await;
    let xml = body(&resp);
    assert!(
        xml.contains("<PubliclyAccessible>true</PubliclyAccessible>"),
        "{xml}"
    );
    assert!(
        xml.contains("<EngineVersion>1.3.0.0</EngineVersion>"),
        "{xml}"
    );

    // And they survive a subsequent Describe.
    let resp = call(
        &svc,
        "DescribeDBInstances",
        &[("DBInstanceIdentifier", "i1")],
    )
    .await;
    let xml = body(&resp);
    assert!(xml.contains("<PubliclyAccessible>true</PubliclyAccessible>"));
    assert!(xml.contains("<EngineVersion>1.3.0.0</EngineVersion>"));
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
    // The request's EndpointType is the CUSTOM type; the endpoint reads
    // back as CUSTOM, matching the model and the RDS behaviour.
    assert!(xml.contains("<EndpointType>CUSTOM</EndpointType>"), "{xml}");
    assert!(
        xml.contains("<CustomEndpointType>READER</CustomEndpointType>"),
        "{xml}"
    );
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
    // Modify retargets the custom endpoint; it stays CUSTOM.
    let xml = body(&resp);
    assert!(
        xml.contains("<CustomEndpointType>ANY</CustomEndpointType>"),
        "{xml}"
    );
    assert!(xml.contains("<EndpointType>CUSTOM</EndpointType>"), "{xml}");

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
