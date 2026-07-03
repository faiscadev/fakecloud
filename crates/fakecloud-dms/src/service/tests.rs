use super::*;
use bytes::Bytes;
use fakecloud_core::multi_account::MultiAccountState;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;

fn svc() -> DmsService {
    DmsService::new(Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    ))))
}

fn req(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "dms".into(),
        action: action.into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        request_id: "req".into(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: Mutex::new(None),
        path_segments: vec![],
        raw_path: String::new(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn call(s: &DmsService, action: &str, body: Value) -> Value {
    let resp = dispatch(s, &req(action, body)).expect("op ok");
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn err(s: &DmsService, action: &str, body: Value) -> AwsServiceError {
    dispatch(s, &req(action, body))
        .err()
        .expect("expected error")
}

fn new_instance(s: &DmsService) -> String {
    call(
        s,
        "CreateReplicationInstance",
        json!({ "ReplicationInstanceIdentifier": "ri-1", "ReplicationInstanceClass": "dms.t3.micro" }),
    )["ReplicationInstance"]["ReplicationInstanceArn"]
        .as_str()
        .unwrap()
        .to_string()
}

fn new_endpoint(s: &DmsService, id: &str, ty: &str) -> String {
    call(
        s,
        "CreateEndpoint",
        json!({ "EndpointIdentifier": id, "EndpointType": ty, "EngineName": "mysql" }),
    )["Endpoint"]["EndpointArn"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn replication_instance_crud() {
    let s = svc();
    let arn = new_instance(&s);
    assert!(arn.contains(":rep:"));
    let desc = call(&s, "DescribeReplicationInstances", json!({}));
    let list = desc["ReplicationInstances"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["ReplicationInstanceStatus"], json!("available"));
    // Modify persists.
    let modified = call(
        &s,
        "ModifyReplicationInstance",
        json!({ "ReplicationInstanceArn": arn, "AllocatedStorage": 100 }),
    );
    assert_eq!(
        modified["ReplicationInstance"]["AllocatedStorage"],
        json!(100)
    );
    // Delete removes.
    call(
        &s,
        "DeleteReplicationInstance",
        json!({ "ReplicationInstanceArn": arn }),
    );
    let after = call(&s, "DescribeReplicationInstances", json!({}));
    assert!(after["ReplicationInstances"].as_array().unwrap().is_empty());
}

#[test]
fn replication_instance_dup_rejected() {
    let s = svc();
    call(
        &s,
        "CreateReplicationInstance",
        json!({ "ReplicationInstanceIdentifier": "dup", "ReplicationInstanceClass": "dms.t3.micro" }),
    );
    let e = err(
        &s,
        "CreateReplicationInstance",
        json!({ "ReplicationInstanceIdentifier": "dup", "ReplicationInstanceClass": "dms.t3.micro" }),
    );
    assert_eq!(e.code(), "ResourceAlreadyExistsFault");
}

#[test]
fn replication_instance_class_too_long() {
    let s = svc();
    let e = err(
        &s,
        "CreateReplicationInstance",
        json!({ "ReplicationInstanceIdentifier": "x", "ReplicationInstanceClass": "a".repeat(40) }),
    );
    assert_eq!(e.code(), "InvalidParameterValueException");
}

#[test]
fn modify_unknown_instance_not_found() {
    let s = svc();
    let e = err(
        &s,
        "ModifyReplicationInstance",
        json!({ "ReplicationInstanceArn": "arn:aws:dms:us-east-1:000000000000:rep:NOPE" }),
    );
    assert_eq!(e.code(), "ResourceNotFoundFault");
}

#[test]
fn endpoint_crud() {
    let s = svc();
    let arn = new_endpoint(&s, "src", "source");
    assert!(arn.contains(":endpoint:"));
    let desc = call(&s, "DescribeEndpoints", json!({}));
    assert_eq!(desc["Endpoints"].as_array().unwrap().len(), 1);
    assert_eq!(desc["Endpoints"][0]["EndpointType"], json!("source"));
    let modified = call(
        &s,
        "ModifyEndpoint",
        json!({ "EndpointArn": arn, "DatabaseName": "newdb" }),
    );
    assert_eq!(modified["Endpoint"]["DatabaseName"], json!("newdb"));
    call(&s, "DeleteEndpoint", json!({ "EndpointArn": arn }));
    assert!(call(&s, "DescribeEndpoints", json!({}))["Endpoints"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn endpoint_settings_round_trip() {
    let s = svc();
    let created = call(
        &s,
        "CreateEndpoint",
        json!({
            "EndpointIdentifier": "s3ep",
            "EndpointType": "target",
            "EngineName": "s3",
            "S3Settings": { "BucketName": "my-bucket", "CompressionType": "gzip" }
        }),
    );
    assert_eq!(
        created["Endpoint"]["S3Settings"]["BucketName"],
        json!("my-bucket")
    );
}

#[test]
fn test_connection_success_and_missing() {
    let s = svc();
    let inst = new_instance(&s);
    let ep = new_endpoint(&s, "src", "source");
    let conn = call(
        &s,
        "TestConnection",
        json!({ "ReplicationInstanceArn": inst, "EndpointArn": ep }),
    );
    assert_eq!(conn["Connection"]["Status"], json!("successful"));
    // Missing endpoint -> ResourceNotFoundFault.
    let e = err(
        &s,
        "TestConnection",
        json!({ "ReplicationInstanceArn": inst, "EndpointArn": "arn:aws:dms:us-east-1:000000000000:endpoint:NOPE" }),
    );
    assert_eq!(e.code(), "ResourceNotFoundFault");
}

#[test]
fn replication_task_lifecycle() {
    let s = svc();
    let inst = new_instance(&s);
    let src = new_endpoint(&s, "src", "source");
    let tgt = new_endpoint(&s, "tgt", "target");
    let task = call(
        &s,
        "CreateReplicationTask",
        json!({
            "ReplicationTaskIdentifier": "t1",
            "SourceEndpointArn": src,
            "TargetEndpointArn": tgt,
            "ReplicationInstanceArn": inst,
            "MigrationType": "full-load",
            "TableMappings": "{}"
        }),
    );
    let task_arn = task["ReplicationTask"]["ReplicationTaskArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(task["ReplicationTask"]["Status"], json!("ready"));
    let started = call(
        &s,
        "StartReplicationTask",
        json!({ "ReplicationTaskArn": task_arn, "StartReplicationTaskType": "start-replication" }),
    );
    assert_eq!(started["ReplicationTask"]["Status"], json!("running"));
    let stopped = call(
        &s,
        "StopReplicationTask",
        json!({ "ReplicationTaskArn": task_arn }),
    );
    assert_eq!(stopped["ReplicationTask"]["Status"], json!("stopped"));
}

#[test]
fn replication_task_bad_migration_type() {
    let s = svc();
    let inst = new_instance(&s);
    let src = new_endpoint(&s, "src", "source");
    let tgt = new_endpoint(&s, "tgt", "target");
    let e = err(
        &s,
        "CreateReplicationTask",
        json!({
            "ReplicationTaskIdentifier": "t1",
            "SourceEndpointArn": src,
            "TargetEndpointArn": tgt,
            "ReplicationInstanceArn": inst,
            "MigrationType": "bogus",
            "TableMappings": "{}"
        }),
    );
    assert_eq!(e.code(), "InvalidParameterValueException");
}

#[test]
fn replication_task_missing_endpoint() {
    let s = svc();
    let inst = new_instance(&s);
    let e = err(
        &s,
        "CreateReplicationTask",
        json!({
            "ReplicationTaskIdentifier": "t1",
            "SourceEndpointArn": "arn:aws:dms:us-east-1:000000000000:endpoint:NOPE",
            "TargetEndpointArn": "arn:aws:dms:us-east-1:000000000000:endpoint:NOPE2",
            "ReplicationInstanceArn": inst,
            "MigrationType": "full-load",
            "TableMappings": "{}"
        }),
    );
    assert_eq!(e.code(), "ResourceNotFoundFault");
}

#[test]
fn subnet_group_crud() {
    let s = svc();
    let created = call(
        &s,
        "CreateReplicationSubnetGroup",
        json!({
            "ReplicationSubnetGroupIdentifier": "sg1",
            "ReplicationSubnetGroupDescription": "desc",
            "SubnetIds": ["subnet-a", "subnet-b"]
        }),
    );
    assert_eq!(
        created["ReplicationSubnetGroup"]["Subnets"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let desc = call(&s, "DescribeReplicationSubnetGroups", json!({}));
    assert_eq!(desc["ReplicationSubnetGroups"].as_array().unwrap().len(), 1);
    call(
        &s,
        "DeleteReplicationSubnetGroup",
        json!({ "ReplicationSubnetGroupIdentifier": "sg1" }),
    );
    assert!(
        call(&s, "DescribeReplicationSubnetGroups", json!({}))["ReplicationSubnetGroups"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn event_subscription_crud() {
    let s = svc();
    call(
        &s,
        "CreateEventSubscription",
        json!({ "SubscriptionName": "sub1", "SnsTopicArn": "arn:aws:sns:us-east-1:000000000000:t" }),
    );
    let desc = call(&s, "DescribeEventSubscriptions", json!({}));
    assert_eq!(desc["EventSubscriptionsList"].as_array().unwrap().len(), 1);
    let modified = call(
        &s,
        "ModifyEventSubscription",
        json!({ "SubscriptionName": "sub1", "Enabled": false }),
    );
    assert_eq!(modified["EventSubscription"]["Enabled"], json!(false));
    call(
        &s,
        "DeleteEventSubscription",
        json!({ "SubscriptionName": "sub1" }),
    );
    assert!(
        call(&s, "DescribeEventSubscriptions", json!({}))["EventSubscriptionsList"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn certificate_import_and_delete() {
    let s = svc();
    let cert = call(
        &s,
        "ImportCertificate",
        json!({ "CertificateIdentifier": "c1", "CertificatePem": "----" }),
    );
    let arn = cert["Certificate"]["CertificateArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        call(&s, "DescribeCertificates", json!({}))["Certificates"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    call(&s, "DeleteCertificate", json!({ "CertificateArn": arn }));
    assert!(call(&s, "DescribeCertificates", json!({}))["Certificates"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn replication_config_and_serverless_replication() {
    let s = svc();
    let src = new_endpoint(&s, "src", "source");
    let tgt = new_endpoint(&s, "tgt", "target");
    let cfg = call(
        &s,
        "CreateReplicationConfig",
        json!({
            "ReplicationConfigIdentifier": "rc1",
            "SourceEndpointArn": src,
            "TargetEndpointArn": tgt,
            "ReplicationType": "cdc",
            "TableMappings": "{}",
            "ComputeConfig": { "MaxCapacityUnits": 8, "MinCapacityUnits": 2 }
        }),
    );
    let arn = cfg["ReplicationConfig"]["ReplicationConfigArn"]
        .as_str()
        .unwrap()
        .to_string();
    let rep = call(
        &s,
        "StartReplication",
        json!({ "ReplicationConfigArn": arn, "StartReplicationType": "start-replication" }),
    );
    assert_eq!(rep["Replication"]["Status"], json!("running"));
    let stopped = call(
        &s,
        "StopReplication",
        json!({ "ReplicationConfigArn": arn }),
    );
    assert_eq!(stopped["Replication"]["Status"], json!("stopped"));
}

#[test]
fn data_provider_crud_by_name() {
    let s = svc();
    let dp = call(
        &s,
        "CreateDataProvider",
        json!({ "DataProviderName": "dp1", "Engine": "postgres", "Settings": { "PostgreSqlSettings": { "Port": 5432 } } }),
    );
    assert_eq!(dp["DataProvider"]["Engine"], json!("postgres"));
    let modified = call(
        &s,
        "ModifyDataProvider",
        json!({ "DataProviderIdentifier": "dp1", "Description": "updated" }),
    );
    assert_eq!(modified["DataProvider"]["Description"], json!("updated"));
    call(
        &s,
        "DeleteDataProvider",
        json!({ "DataProviderIdentifier": "dp1" }),
    );
    assert!(
        call(&s, "DescribeDataProviders", json!({}))["DataProviders"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn instance_profile_and_migration_project() {
    let s = svc();
    call(
        &s,
        "CreateInstanceProfile",
        json!({ "InstanceProfileName": "ip1", "SubnetGroupIdentifier": "sg" }),
    );
    let mp = call(
        &s,
        "CreateMigrationProject",
        json!({
            "MigrationProjectName": "mp1",
            "InstanceProfileIdentifier": "ip1",
            "SourceDataProviderDescriptors": [{ "DataProviderIdentifier": "arn:aws:dms:us-east-1:000000000000:data-provider:src" }],
            "TargetDataProviderDescriptors": [{ "DataProviderIdentifier": "arn:aws:dms:us-east-1:000000000000:data-provider:tgt" }]
        }),
    );
    assert_eq!(mp["MigrationProject"]["MigrationProjectName"], json!("mp1"));
    assert_eq!(
        mp["MigrationProject"]["SourceDataProviderDescriptors"][0]["DataProviderName"],
        json!("src")
    );
    // Conversion configuration round-trips.
    call(
        &s,
        "ModifyConversionConfiguration",
        json!({ "MigrationProjectIdentifier": "mp1", "ConversionConfiguration": "{\"a\":1}" }),
    );
    let cc = call(
        &s,
        "DescribeConversionConfiguration",
        json!({ "MigrationProjectIdentifier": "mp1" }),
    );
    assert_eq!(cc["ConversionConfiguration"], json!("{\"a\":1}"));
}

#[test]
fn metadata_model_start_then_describe() {
    let s = svc();
    call(
        &s,
        "CreateInstanceProfile",
        json!({ "InstanceProfileName": "ip1" }),
    );
    call(
        &s,
        "CreateMigrationProject",
        json!({
            "MigrationProjectName": "mp1",
            "InstanceProfileIdentifier": "ip1",
            "SourceDataProviderDescriptors": [{ "DataProviderIdentifier": "src" }],
            "TargetDataProviderDescriptors": [{ "DataProviderIdentifier": "tgt" }]
        }),
    );
    let started = call(
        &s,
        "StartMetadataModelAssessment",
        json!({ "MigrationProjectIdentifier": "mp1", "SelectionRules": "{}" }),
    );
    assert!(started["RequestIdentifier"].is_string());
    let desc = call(
        &s,
        "DescribeMetadataModelAssessments",
        json!({ "MigrationProjectIdentifier": "mp1" }),
    );
    assert_eq!(desc["Requests"].as_array().unwrap().len(), 1);
}

#[test]
fn data_migration_lifecycle() {
    let s = svc();
    let dm = call(
        &s,
        "CreateDataMigration",
        json!({
            "DataMigrationName": "dm1",
            "MigrationProjectIdentifier": "arn:aws:dms:us-east-1:000000000000:migration-project:mp",
            "DataMigrationType": "full-load",
            "ServiceAccessRoleArn": "arn:aws:iam::000000000000:role/r"
        }),
    );
    assert_eq!(dm["DataMigration"]["DataMigrationStatus"], json!("READY"));
    let started = call(
        &s,
        "StartDataMigration",
        json!({ "DataMigrationIdentifier": "dm1", "StartType": "start-replication" }),
    );
    assert_eq!(
        started["DataMigration"]["DataMigrationStatus"],
        json!("RUNNING")
    );
    call(
        &s,
        "DeleteDataMigration",
        json!({ "DataMigrationIdentifier": "dm1" }),
    );
    assert!(
        call(&s, "DescribeDataMigrations", json!({}))["DataMigrations"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn fleet_advisor_collector_crud() {
    let s = svc();
    let created = call(
        &s,
        "CreateFleetAdvisorCollector",
        json!({ "CollectorName": "col1", "ServiceAccessRoleArn": "arn:aws:iam::000000000000:role/r", "S3BucketName": "b" }),
    );
    let id = created["CollectorReferencedId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        call(&s, "DescribeFleetAdvisorCollectors", json!({}))["Collectors"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    call(
        &s,
        "DeleteFleetAdvisorCollector",
        json!({ "CollectorReferencedId": id }),
    );
    assert!(
        call(&s, "DescribeFleetAdvisorCollectors", json!({}))["Collectors"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    // Deleting unknown collector -> CollectorNotFoundFault.
    let e = err(
        &s,
        "DeleteFleetAdvisorCollector",
        json!({ "CollectorReferencedId": "nope" }),
    );
    assert_eq!(e.code(), "CollectorNotFoundFault");
}

#[test]
fn tagging_round_trip() {
    let s = svc();
    let arn = new_endpoint(&s, "src", "source");
    call(
        &s,
        "AddTagsToResource",
        json!({ "ResourceArn": arn, "Tags": [{ "Key": "env", "Value": "prod" }, { "Key": "team", "Value": "data" }] }),
    );
    let listed = call(&s, "ListTagsForResource", json!({ "ResourceArn": arn }));
    assert_eq!(listed["TagList"].as_array().unwrap().len(), 2);
    call(
        &s,
        "RemoveTagsFromResource",
        json!({ "ResourceArn": arn, "TagKeys": ["env"] }),
    );
    let after = call(&s, "ListTagsForResource", json!({ "ResourceArn": arn }));
    assert_eq!(after["TagList"].as_array().unwrap().len(), 1);
    assert_eq!(after["TagList"][0]["Key"], json!("team"));
}

#[test]
fn account_attributes_reflect_counts() {
    let s = svc();
    new_instance(&s);
    let attrs = call(&s, "DescribeAccountAttributes", json!({}));
    let quotas = attrs["AccountQuotas"].as_array().unwrap();
    let ri = quotas
        .iter()
        .find(|q| q["AccountQuotaName"] == json!("ReplicationInstances"))
        .unwrap();
    assert_eq!(ri["Used"], json!(1));
    assert!(attrs["UniqueAccountIdentifier"].is_string());
}

#[test]
fn describe_endpoint_types_non_empty() {
    let s = svc();
    let types = call(&s, "DescribeEndpointTypes", json!({}));
    assert!(!types["SupportedEndpointTypes"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn pagination_marker() {
    let s = svc();
    for i in 0..5 {
        call(
            &s,
            "CreateEndpoint",
            json!({ "EndpointIdentifier": format!("ep{i}"), "EndpointType": "source", "EngineName": "mysql" }),
        );
    }
    let page1 = call(&s, "DescribeEndpoints", json!({ "MaxRecords": 2 }));
    assert_eq!(page1["Endpoints"].as_array().unwrap().len(), 2);
    let marker = page1["Marker"].as_str().unwrap().to_string();
    let page2 = call(
        &s,
        "DescribeEndpoints",
        json!({ "MaxRecords": 2, "Marker": marker }),
    );
    assert_eq!(page2["Endpoints"].as_array().unwrap().len(), 2);
}

#[test]
fn filters_endpoints_by_engine() {
    let s = svc();
    new_endpoint(&s, "m", "source");
    call(
        &s,
        "CreateEndpoint",
        json!({ "EndpointIdentifier": "p", "EndpointType": "source", "EngineName": "postgres" }),
    );
    let filtered = call(
        &s,
        "DescribeEndpoints",
        json!({ "Filters": [{ "Name": "engine-name", "Values": ["postgres"] }] }),
    );
    let list = filtered["Endpoints"].as_array().unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0]["EngineName"], json!("postgres"));
}

#[test]
fn unknown_filter_name_ignored() {
    let s = svc();
    new_endpoint(&s, "m", "source");
    // Placeholder filter name the conformance probe sends; must not error and
    // must not drop everything to an error.
    let out = call(
        &s,
        "DescribeConnections",
        json!({ "Filters": [{ "Name": "string", "Values": ["string"] }] }),
    );
    assert!(out["Connections"].as_array().unwrap().is_empty());
}
