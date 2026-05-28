//! Unit tests for the control-plane areas added alongside the Data Catalog
//! and Jobs handlers. Each test drives the handler methods directly through a
//! `GlueService` and asserts persisted state round-trips and status
//! transitions behave.

#![cfg(test)]

use std::collections::HashMap;

use bytes::Bytes;
use http::{HeaderMap, Method};
use serde_json::{json, Value};

use fakecloud_core::service::AwsRequest;

use crate::service::GlueService;

fn req(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "glue".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test".to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn body_of(resp: fakecloud_core::service::AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

#[test]
fn crawler_state_transitions() {
    let svc = GlueService::default();
    svc.create_crawler(&req(
        "CreateCrawler",
        json!({"Name": "c", "Role": "r", "Targets": {"S3Targets": [{"Path": "s3://b"}]}}),
    ))
    .unwrap();

    let got = body_of(
        svc.get_crawler(&req("GetCrawler", json!({"Name": "c"})))
            .unwrap(),
    );
    assert_eq!(got["Crawler"]["State"], "READY");

    svc.start_crawler(&req("StartCrawler", json!({"Name": "c"})))
        .unwrap();
    let got = body_of(
        svc.get_crawler(&req("GetCrawler", json!({"Name": "c"})))
            .unwrap(),
    );
    assert_eq!(got["Crawler"]["State"], "RUNNING");
    // double-start is rejected
    assert!(svc
        .start_crawler(&req("StartCrawler", json!({"Name": "c"})))
        .is_err());

    svc.stop_crawler(&req("StopCrawler", json!({"Name": "c"})))
        .unwrap();
    let got = body_of(
        svc.get_crawler(&req("GetCrawler", json!({"Name": "c"})))
            .unwrap(),
    );
    assert_eq!(got["Crawler"]["State"], "READY");
    // stop-when-idle is rejected
    assert!(svc
        .stop_crawler(&req("StopCrawler", json!({"Name": "c"})))
        .is_err());

    svc.delete_crawler(&req("DeleteCrawler", json!({"Name": "c"})))
        .unwrap();
    assert!(svc
        .get_crawler(&req("GetCrawler", json!({"Name": "c"})))
        .is_err());
}

#[test]
fn trigger_round_trip_and_missing_required() {
    let svc = GlueService::default();
    // Missing required Type -> InvalidInputException.
    assert!(svc
        .create_trigger(&req("CreateTrigger", json!({"Name": "t", "Actions": []})))
        .is_err());

    svc.create_trigger(&req(
        "CreateTrigger",
        json!({"Name": "t", "Type": "ON_DEMAND", "Actions": [{"JobName": "j"}]}),
    ))
    .unwrap();
    let got = body_of(
        svc.get_trigger(&req("GetTrigger", json!({"Name": "t"})))
            .unwrap(),
    );
    assert_eq!(got["Trigger"]["Type"], "ON_DEMAND");
    assert_eq!(got["Trigger"]["Actions"][0]["JobName"], "j");

    svc.start_trigger(&req("StartTrigger", json!({"Name": "t"})))
        .unwrap();
    let got = body_of(
        svc.get_trigger(&req("GetTrigger", json!({"Name": "t"})))
            .unwrap(),
    );
    assert_eq!(got["Trigger"]["State"], "ACTIVATED");
}

#[test]
fn workflow_run_properties_round_trip() {
    let svc = GlueService::default();
    svc.create_workflow(&req("CreateWorkflow", json!({"Name": "w"})))
        .unwrap();
    let run = body_of(
        svc.start_workflow_run(&req("StartWorkflowRun", json!({"Name": "w"})))
            .unwrap(),
    );
    let run_id = run["RunId"].as_str().unwrap().to_string();
    svc.put_workflow_run_properties(&req(
        "PutWorkflowRunProperties",
        json!({"Name": "w", "RunId": run_id, "RunProperties": {"k": "v"}}),
    ))
    .unwrap();
    let props = body_of(
        svc.get_workflow_run_properties(&req(
            "GetWorkflowRunProperties",
            json!({"Name": "w", "RunId": run_id}),
        ))
        .unwrap(),
    );
    assert_eq!(props["RunProperties"]["k"], "v");
}

#[test]
fn schema_registry_version_round_trip() {
    let svc = GlueService::default();
    svc.create_registry(&req("CreateRegistry", json!({"RegistryName": "reg"})))
        .unwrap();
    let created = body_of(
        svc.create_schema(&req(
            "CreateSchema",
            json!({
                "RegistryId": {"RegistryName": "reg"},
                "SchemaName": "s", "DataFormat": "AVRO",
                "SchemaDefinition": "{}"
            }),
        ))
        .unwrap(),
    );
    let vid = created["SchemaVersionId"].as_str().unwrap();
    assert_eq!(vid.len(), 36, "schema version id must be a hyphenated UUID");

    let v = body_of(
        svc.get_schema_version(&req("GetSchemaVersion", json!({"SchemaVersionId": vid})))
            .unwrap(),
    );
    assert_eq!(v["SchemaDefinition"], "{}");
    assert_eq!(v["VersionNumber"], 1);

    let rsv = body_of(
        svc.register_schema_version(&req(
            "RegisterSchemaVersion",
            json!({"SchemaId": {"RegistryName": "reg", "SchemaName": "s"}, "SchemaDefinition": "{\"x\":1}"}),
        ))
        .unwrap(),
    );
    assert_eq!(rsv["VersionNumber"], 2);
}

#[test]
fn session_statement_increments() {
    let svc = GlueService::default();
    svc.create_session(&req(
        "CreateSession",
        json!({"Id": "s", "Role": "r", "Command": {"Name": "glueetl"}}),
    ))
    .unwrap();
    let r1 = body_of(
        svc.run_statement(&req("RunStatement", json!({"SessionId": "s", "Code": "1"})))
            .unwrap(),
    );
    let r2 = body_of(
        svc.run_statement(&req("RunStatement", json!({"SessionId": "s", "Code": "2"})))
            .unwrap(),
    );
    assert_eq!(r1["Id"], 1);
    assert_eq!(r2["Id"], 2);
    let st = body_of(
        svc.get_statement(&req("GetStatement", json!({"SessionId": "s", "Id": 1})))
            .unwrap(),
    );
    assert_eq!(st["Statement"]["Code"], "1");
}

#[test]
fn ml_transform_and_task_run() {
    let svc = GlueService::default();
    let created = body_of(
        svc.create_ml_transform(&req(
            "CreateMLTransform",
            json!({
                "Name": "m", "Role": "r",
                "InputRecordTables": [{"DatabaseName": "db", "TableName": "t"}],
                "Parameters": {"TransformType": "FIND_MATCHES"}
            }),
        ))
        .unwrap(),
    );
    let tid = created["TransformId"].as_str().unwrap().to_string();
    let run = body_of(
        svc.start_ml_evaluation_task_run(&req(
            "StartMLEvaluationTaskRun",
            json!({"TransformId": tid}),
        ))
        .unwrap(),
    );
    let task = run["TaskRunId"].as_str().unwrap().to_string();
    let got = body_of(
        svc.get_ml_task_run(&req(
            "GetMLTaskRun",
            json!({"TransformId": tid, "TaskRunId": task}),
        ))
        .unwrap(),
    );
    assert_eq!(got["Status"], "RUNNING");
}

#[test]
fn catalog_and_udf_round_trip() {
    let svc = GlueService::default();
    svc.create_catalog(&req(
        "CreateCatalog",
        json!({"Name": "cat", "CatalogInput": {"Description": "d"}}),
    ))
    .unwrap();
    let got = body_of(
        svc.get_catalog(&req("GetCatalog", json!({"CatalogId": "cat"})))
            .unwrap(),
    );
    assert_eq!(got["Catalog"]["Name"], "cat");
    assert_eq!(got["Catalog"]["Description"], "d");

    svc.create_database(&req(
        "CreateDatabase",
        json!({"DatabaseInput": {"Name": "db"}}),
    ))
    .unwrap();
    svc.create_user_defined_function(&req(
        "CreateUserDefinedFunction",
        json!({"DatabaseName": "db", "FunctionInput": {"FunctionName": "fn", "ClassName": "C"}}),
    ))
    .unwrap();
    let f = body_of(
        svc.get_user_defined_function(&req(
            "GetUserDefinedFunction",
            json!({"DatabaseName": "db", "FunctionName": "fn"}),
        ))
        .unwrap(),
    );
    assert_eq!(f["UserDefinedFunction"]["ClassName"], "C");
}

#[test]
fn integration_create_and_describe() {
    let svc = GlueService::default();
    let created = body_of(
        svc.create_integration(&req(
            "CreateIntegration",
            json!({"IntegrationName": "i", "SourceArn": "arn:src", "TargetArn": "arn:tgt"}),
        ))
        .unwrap(),
    );
    assert_eq!(created["Status"], "ACTIVE");
    assert_eq!(created["IntegrationName"], "i");
    assert!(created["CreateTime"].is_number());

    let list = body_of(
        svc.describe_integrations(&req("DescribeIntegrations", json!({})))
            .unwrap(),
    );
    assert_eq!(list["Integrations"][0]["IntegrationName"], "i");
}

#[test]
fn column_statistics_round_trip() {
    let svc = GlueService::default();
    svc.update_column_statistics_for_table(&req(
        "UpdateColumnStatisticsForTable",
        json!({
            "DatabaseName": "db", "TableName": "t",
            "ColumnStatisticsList": [{"ColumnName": "id", "ColumnType": "bigint"}]
        }),
    ))
    .unwrap();
    let got = body_of(
        svc.get_column_statistics_for_table(&req(
            "GetColumnStatisticsForTable",
            json!({"DatabaseName": "db", "TableName": "t", "ColumnNames": ["id"]}),
        ))
        .unwrap(),
    );
    assert_eq!(got["ColumnStatisticsList"][0]["ColumnName"], "id");
}

#[test]
fn constraint_validation_rejects_bad_input() {
    // Enum violation on CreateTrigger.Type is rejected before the handler runs.
    let bad = crate::common::validate_constraints(
        "CreateTrigger",
        &json!({"Name": "t", "Type": "NOT_A_TYPE", "Actions": []}),
    );
    assert!(bad.is_err());
    // Valid enum passes.
    let ok = crate::common::validate_constraints(
        "CreateTrigger",
        &json!({"Name": "t", "Type": "ON_DEMAND", "Actions": []}),
    );
    assert!(ok.is_ok());
}
