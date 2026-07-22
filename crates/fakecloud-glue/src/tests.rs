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
    // A back-to-back start (before the crawl is polled) is still rejected: the
    // crawler is RUNNING in storage until the first read settles it.
    assert!(svc
        .start_crawler(&req("StartCrawler", json!({"Name": "c"})))
        .is_err());

    // Reading the crawler settles the finished crawl to READY with a LastCrawl
    // summary. Before the fix it stayed RUNNING forever, hanging poll loops.
    let got = body_of(
        svc.get_crawler(&req("GetCrawler", json!({"Name": "c"})))
            .unwrap(),
    );
    assert_eq!(got["Crawler"]["State"], "READY");
    assert_eq!(got["Crawler"]["LastCrawl"]["Status"], "SUCCEEDED");

    // stop-when-idle is rejected
    assert!(svc
        .stop_crawler(&req("StopCrawler", json!({"Name": "c"})))
        .is_err());

    // A second start after completion now succeeds.
    svc.start_crawler(&req("StartCrawler", json!({"Name": "c"})))
        .unwrap();
    // Poll again to settle, then delete is no longer permanently blocked.
    svc.get_crawler(&req("GetCrawler", json!({"Name": "c"})))
        .unwrap();
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

    // StartTrigger on an ON_DEMAND trigger fires a run but leaves it in the
    // CREATED state (matching AWS); only scheduled/conditional triggers go
    // ACTIVATED.
    svc.start_trigger(&req("StartTrigger", json!({"Name": "t"})))
        .unwrap();
    let got = body_of(
        svc.get_trigger(&req("GetTrigger", json!({"Name": "t"})))
            .unwrap(),
    );
    assert_eq!(got["Trigger"]["State"], "CREATED");
}

#[test]
fn start_trigger_activates_scheduled_trigger() {
    let svc = GlueService::default();
    svc.create_trigger(&req(
        "CreateTrigger",
        json!({
            "Name": "sched",
            "Type": "SCHEDULED",
            "Schedule": "cron(0 12 * * ? *)",
            "Actions": [{"JobName": "j"}]
        }),
    ))
    .unwrap();
    svc.start_trigger(&req("StartTrigger", json!({"Name": "sched"})))
        .unwrap();
    let got = body_of(
        svc.get_trigger(&req("GetTrigger", json!({"Name": "sched"})))
            .unwrap(),
    );
    assert_eq!(got["Trigger"]["State"], "ACTIVATED");
}

#[test]
fn create_job_defaults_timeout_to_2880() {
    let svc = GlueService::default();
    svc.create_job(&req(
        "CreateJob",
        json!({"Name": "j", "Role": "r", "Command": {"Name": "glueetl"}}),
    ))
    .unwrap();
    let got = body_of(
        svc.get_job(&req("GetJob", json!({"JobName": "j"})))
            .unwrap(),
    );
    assert_eq!(got["Job"]["Timeout"], 2880);

    // Streaming jobs have no default timeout.
    svc.create_job(&req(
        "CreateJob",
        json!({"Name": "s", "Role": "r", "Command": {"Name": "gluestreaming"}}),
    ))
    .unwrap();
    let got = body_of(
        svc.get_job(&req("GetJob", json!({"JobName": "s"})))
            .unwrap(),
    );
    assert!(got["Job"]["Timeout"].is_null());

    // An explicit timeout is preserved.
    svc.create_job(&req(
        "CreateJob",
        json!({"Name": "x", "Role": "r", "Command": {"Name": "glueetl"}, "Timeout": 60}),
    ))
    .unwrap();
    let got = body_of(
        svc.get_job(&req("GetJob", json!({"JobName": "x"})))
            .unwrap(),
    );
    assert_eq!(got["Job"]["Timeout"], 60);
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
fn get_session_endpoint_requires_existing_session() {
    let svc = GlueService::default();
    // Missing session -> EntityNotFoundException.
    let err = svc
        .get_session_endpoint(&req("GetSessionEndpoint", json!({"SessionId": "nope"})))
        .err()
        .unwrap();
    assert!(format!("{err:?}").contains("EntityNotFound"));

    svc.create_session(&req(
        "CreateSession",
        json!({"Id": "s", "Role": "r", "Command": {"Name": "glueetl"}}),
    ))
    .unwrap();
    let ep = body_of(
        svc.get_session_endpoint(&req("GetSessionEndpoint", json!({"SessionId": "s"})))
            .unwrap(),
    );
    let sc = &ep["SparkConnect"];
    assert!(sc["Url"].as_str().unwrap().contains("/s"));
    assert!(sc["AuthToken"].as_str().unwrap().contains("s"));
    assert!(sc["AuthTokenExpirationTime"].as_f64().unwrap() > 0.0);
}

#[test]
fn get_dashboard_url_validates_type_and_session() {
    let svc = GlueService::default();
    // Bad ResourceType -> InvalidInputException.
    let err = svc
        .get_dashboard_url(&req(
            "GetDashboardUrl",
            json!({"ResourceId": "x", "ResourceType": "BOGUS"}),
        ))
        .err()
        .unwrap();
    assert!(format!("{err:?}").contains("InvalidInput"));

    // SESSION type requires the session to exist.
    let err = svc
        .get_dashboard_url(&req(
            "GetDashboardUrl",
            json!({"ResourceId": "missing", "ResourceType": "SESSION"}),
        ))
        .err()
        .unwrap();
    assert!(format!("{err:?}").contains("EntityNotFound"));

    svc.create_session(&req(
        "CreateSession",
        json!({"Id": "s", "Role": "r", "Command": {"Name": "glueetl"}}),
    ))
    .unwrap();
    let r = body_of(
        svc.get_dashboard_url(&req(
            "GetDashboardUrl",
            json!({"ResourceId": "s", "ResourceType": "SESSION"}),
        ))
        .unwrap(),
    );
    assert!(r["Url"].as_str().unwrap().contains("/session/s"));

    // JOB type does not require existence (no job-run lookup).
    let r = body_of(
        svc.get_dashboard_url(&req(
            "GetDashboardUrl",
            json!({"ResourceId": "jr_123", "ResourceType": "JOB"}),
        ))
        .unwrap(),
    );
    assert!(r["Url"].as_str().unwrap().contains("/job/jr_123"));
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
    // The task run settles to a terminal state on read instead of hanging in
    // RUNNING forever.
    assert_eq!(got["Status"], "SUCCEEDED");
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

#[test]
fn update_table_applies_view_text_and_retention() {
    let svc = GlueService::default();
    svc.create_database(&req(
        "CreateDatabase",
        json!({"DatabaseInput": {"Name": "db"}}),
    ))
    .unwrap();
    svc.create_table(&req(
        "CreateTable",
        json!({
            "DatabaseName": "db",
            "TableInput": {
                "Name": "v",
                "TableType": "VIRTUAL_VIEW",
                "ViewOriginalText": "SELECT 1",
                "ViewExpandedText": "SELECT 1 FROM db.t",
                "Retention": 5,
            }
        }),
    ))
    .unwrap();

    // Update the view text and retention.
    svc.update_table(&req(
        "UpdateTable",
        json!({
            "DatabaseName": "db",
            "TableInput": {
                "Name": "v",
                "TableType": "VIRTUAL_VIEW",
                "ViewOriginalText": "SELECT 2",
                "ViewExpandedText": "SELECT 2 FROM db.t",
                "Retention": 9,
            }
        }),
    ))
    .unwrap();

    let got = body_of(
        svc.get_table(&req("GetTable", json!({"DatabaseName": "db", "Name": "v"})))
            .unwrap(),
    );
    assert_eq!(got["Table"]["ViewOriginalText"], "SELECT 2");
    assert_eq!(got["Table"]["ViewExpandedText"], "SELECT 2 FROM db.t");
    assert_eq!(got["Table"]["Retention"], 9);
}

// --- bug-hunt 2026-07-16 regressions ---

/// Create a database `db` with table `t` (no partition keys), so version and
/// partition tests share a fixture.
fn setup_table(svc: &GlueService) {
    svc.create_database(&req(
        "CreateDatabase",
        json!({"DatabaseInput": {"Name": "db"}}),
    ))
    .unwrap();
    svc.create_table(&req(
        "CreateTable",
        json!({"DatabaseName": "db", "TableInput": {"Name": "t"}}),
    ))
    .unwrap();
}

#[test]
fn get_table_version_missing_id_returns_not_found() {
    let svc = GlueService::default();
    setup_table(&svc);
    // The initial version (1) is archived and returns real data.
    let v1 = body_of(
        svc.get_table_version(&req(
            "GetTableVersion",
            json!({"DatabaseName": "db", "TableName": "t", "VersionId": "1"}),
        ))
        .unwrap(),
    );
    assert_eq!(v1["TableVersion"]["VersionId"], "1");
    // A phantom VersionId must 404 instead of returning the live table stamped
    // with the bogus id.
    let err = svc
        .get_table_version(&req(
            "GetTableVersion",
            json!({"DatabaseName": "db", "TableName": "t", "VersionId": "9999"}),
        ))
        .err()
        .unwrap();
    assert!(format!("{err:?}").contains("EntityNotFound"));
}

#[test]
fn batch_delete_table_version_removes_and_reports_errors() {
    let svc = GlueService::default();
    setup_table(&svc);
    // UpdateTable archives version 2.
    svc.update_table(&req(
        "UpdateTable",
        json!({"DatabaseName": "db", "TableInput": {"Name": "t", "Description": "v2"}}),
    ))
    .unwrap();
    let versions = body_of(
        svc.get_table_versions(&req(
            "GetTableVersions",
            json!({"DatabaseName": "db", "TableName": "t"}),
        ))
        .unwrap(),
    );
    assert_eq!(versions["TableVersions"].as_array().unwrap().len(), 2);

    // Delete v1 (exists) and v9999 (missing).
    let out = body_of(
        svc.batch_delete_table_version(&req(
            "BatchDeleteTableVersion",
            json!({"DatabaseName": "db", "TableName": "t", "VersionIds": ["1", "9999"]}),
        ))
        .unwrap(),
    );
    let errors = out["Errors"].as_array().unwrap();
    assert_eq!(errors.len(), 1, "only the missing id errors: {out}");
    assert_eq!(errors[0]["VersionId"], "9999");
    assert_eq!(
        errors[0]["ErrorDetail"]["ErrorCode"],
        "EntityNotFoundException"
    );

    // v1 is really gone; only v2 remains.
    let versions = body_of(
        svc.get_table_versions(&req(
            "GetTableVersions",
            json!({"DatabaseName": "db", "TableName": "t"}),
        ))
        .unwrap(),
    );
    let ids: Vec<&str> = versions["TableVersions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["VersionId"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["2"]);
}

#[test]
fn get_partitions_parallel_scan_union_equals_full_set_no_dups() {
    let svc = GlueService::default();
    setup_table(&svc);
    for i in 0..10 {
        svc.create_partition(&req(
            "CreatePartition",
            json!({
                "DatabaseName": "db",
                "TableName": "t",
                "PartitionInput": {"Values": [format!("p{i}")]}
            }),
        ))
        .unwrap();
    }
    let total = 3u64;
    let mut seen: Vec<String> = Vec::new();
    for seg in 0..total {
        let out = body_of(
            svc.get_partitions(&req(
                "GetPartitions",
                json!({
                    "DatabaseName": "db",
                    "TableName": "t",
                    "Segment": {"SegmentNumber": seg, "TotalSegments": total}
                }),
            ))
            .unwrap(),
        );
        for p in out["Partitions"].as_array().unwrap() {
            seen.push(p["Values"][0].as_str().unwrap().to_string());
        }
    }
    seen.sort();
    let unique: std::collections::BTreeSet<_> = seen.iter().cloned().collect();
    assert_eq!(seen.len(), 10, "no duplicates across segments: {seen:?}");
    assert_eq!(unique.len(), 10, "union covers the full set");
}

#[test]
fn resume_workflow_run_returns_observable_run_id() {
    let svc = GlueService::default();
    svc.create_workflow(&req("CreateWorkflow", json!({"Name": "wf"})))
        .unwrap();
    let started = body_of(
        svc.start_workflow_run(&req("StartWorkflowRun", json!({"Name": "wf"})))
            .unwrap(),
    );
    let run_id = started["RunId"].as_str().unwrap().to_string();
    let resumed = body_of(
        svc.resume_workflow_run(&req(
            "ResumeWorkflowRun",
            json!({"Name": "wf", "RunId": run_id, "NodeIds": ["n1"]}),
        ))
        .unwrap(),
    );
    let new_id = resumed["RunId"].as_str().unwrap().to_string();
    assert_ne!(new_id, "");
    // The resumed run must be persisted and readable.
    let got = body_of(
        svc.get_workflow_run(&req(
            "GetWorkflowRun",
            json!({"Name": "wf", "RunId": new_id}),
        ))
        .unwrap(),
    );
    assert_eq!(got["Run"]["WorkflowRunId"], new_id);
    // GetWorkflowRun settles the run to a terminal state on read.
    assert_eq!(got["Run"]["Status"], "COMPLETED");
}

#[test]
fn create_job_persists_source_control_and_extra_fields() {
    let svc = GlueService::default();
    svc.create_job(&req(
        "CreateJob",
        json!({
            "Name": "j",
            "Role": "r",
            "Command": {"Name": "glueetl"},
            "LogUri": "s3://logs/j",
            "MaintenanceWindow": "Sun:23:00",
            "AllocatedCapacity": 5,
            "SourceControlDetails": {"Provider": "GITHUB", "Repository": "repo", "Branch": "main"}
        }),
    ))
    .unwrap();
    let j = body_of(
        svc.get_job(&req("GetJob", json!({"JobName": "j"})))
            .unwrap(),
    )["Job"]
        .clone();
    assert_eq!(j["LogUri"], "s3://logs/j");
    assert_eq!(j["MaintenanceWindow"], "Sun:23:00");
    assert_eq!(j["AllocatedCapacity"], 5);
    assert_eq!(j["SourceControlDetails"]["Provider"], "GITHUB");

    // UpdateJobFromSourceControl persists new SourceControlDetails.
    svc.update_job_from_source_control(&req(
        "UpdateJobFromSourceControl",
        json!({
            "JobName": "j",
            "Provider": "GITHUB",
            "RepositoryName": "repo2",
            "BranchName": "dev",
            "CommitId": "abc123"
        }),
    ))
    .unwrap();
    let j = body_of(
        svc.get_job(&req("GetJob", json!({"JobName": "j"})))
            .unwrap(),
    )["Job"]
        .clone();
    assert_eq!(j["SourceControlDetails"]["Repository"], "repo2");
    assert_eq!(j["SourceControlDetails"]["Branch"], "dev");
    assert_eq!(j["SourceControlDetails"]["LastCommitId"], "abc123");
}

#[test]
fn get_connection_hide_password_redacts() {
    let svc = GlueService::default();
    svc.create_connection(&req(
        "CreateConnection",
        json!({
            "ConnectionInput": {
                "Name": "c",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {"USERNAME": "u", "PASSWORD": "secret"}
            }
        }),
    ))
    .unwrap();
    // Default (HidePassword absent) returns the password verbatim.
    let shown = body_of(
        svc.get_connection(&req("GetConnection", json!({"Name": "c"})))
            .unwrap(),
    );
    assert_eq!(
        shown["Connection"]["ConnectionProperties"]["PASSWORD"],
        "secret"
    );
    // HidePassword=true strips it.
    let hidden = body_of(
        svc.get_connection(&req(
            "GetConnection",
            json!({"Name": "c", "HidePassword": true}),
        ))
        .unwrap(),
    );
    assert!(hidden["Connection"]["ConnectionProperties"]["PASSWORD"].is_null());
    assert_eq!(
        hidden["Connection"]["ConnectionProperties"]["USERNAME"],
        "u"
    );
}

#[test]
fn get_tables_pagination_round_trips_next_token() {
    let svc = GlueService::default();
    svc.create_database(&req(
        "CreateDatabase",
        json!({"DatabaseInput": {"Name": "db"}}),
    ))
    .unwrap();
    for n in ["a", "b", "c"] {
        svc.create_table(&req(
            "CreateTable",
            json!({"DatabaseName": "db", "TableInput": {"Name": n}}),
        ))
        .unwrap();
    }
    let page1 = body_of(
        svc.get_tables(&req(
            "GetTables",
            json!({"DatabaseName": "db", "MaxResults": 2}),
        ))
        .unwrap(),
    );
    assert_eq!(page1["TableList"].as_array().unwrap().len(), 2);
    let token = page1["NextToken"].as_str().unwrap().to_string();
    // Each table reports CatalogId + VersionId.
    assert_eq!(page1["TableList"][0]["CatalogId"], "123456789012");
    assert_eq!(page1["TableList"][0]["VersionId"], "1");

    let page2 = body_of(
        svc.get_tables(&req(
            "GetTables",
            json!({"DatabaseName": "db", "MaxResults": 2, "NextToken": token}),
        ))
        .unwrap(),
    );
    assert_eq!(page2["TableList"].as_array().unwrap().len(), 1);
    assert!(
        page2["NextToken"].is_null(),
        "last page has no token: {page2}"
    );
}

#[test]
fn create_database_persists_target_and_federated() {
    // CreateDatabase dropped TargetDatabase/FederatedDatabase; GetDatabase must
    // echo them back (bug-hunt).
    let svc = GlueService::default();
    svc.create_database(&req(
        "CreateDatabase",
        json!({"DatabaseInput": {
            "Name": "linked",
            "TargetDatabase": {"CatalogId": "210987654321", "DatabaseName": "src", "Region": "us-west-2"},
            "FederatedDatabase": {"Identifier": "fed-id", "ConnectionName": "conn"}
        }}),
    ))
    .unwrap();

    let got = body_of(
        svc.get_database(&req("GetDatabase", json!({"Name": "linked"})))
            .unwrap(),
    );
    let db = &got["Database"];
    assert_eq!(db["TargetDatabase"]["DatabaseName"], "src");
    assert_eq!(db["TargetDatabase"]["Region"], "us-west-2");
    assert_eq!(db["FederatedDatabase"]["Identifier"], "fed-id");
    assert_eq!(db["FederatedDatabase"]["ConnectionName"], "conn");
}

#[test]
fn update_classifier_bumps_version_and_preserves_creation_time() {
    // UpdateClassifier dropped CreationTime and never bumped Version (bug-hunt).
    let svc = GlueService::default();
    svc.create_classifier(&req(
        "CreateClassifier",
        json!({"CsvClassifier": {"Name": "c", "Delimiter": ","}}),
    ))
    .unwrap();

    let got = body_of(
        svc.get_classifier(&req("GetClassifier", json!({"Name": "c"})))
            .unwrap(),
    );
    let created = &got["Classifier"]["CsvClassifier"];
    assert_eq!(created["Version"], 1);
    let creation_time = created["CreationTime"].clone();
    assert!(
        !creation_time.is_null(),
        "CreationTime present after create"
    );

    svc.update_classifier(&req(
        "UpdateClassifier",
        json!({"CsvClassifier": {"Name": "c", "Delimiter": ";"}}),
    ))
    .unwrap();

    let got = body_of(
        svc.get_classifier(&req("GetClassifier", json!({"Name": "c"})))
            .unwrap(),
    );
    let updated = &got["Classifier"]["CsvClassifier"];
    assert_eq!(updated["Version"], 2, "version bumped: {got}");
    assert_eq!(updated["Delimiter"], ";", "update applied");
    assert_eq!(
        updated["CreationTime"], creation_time,
        "CreationTime preserved across update"
    );
    assert!(!updated["LastUpdated"].is_null());
}

#[test]
fn schema_versions_diff_reflects_stored_definitions() {
    // Regression: GetSchemaVersionsDiff validated inputs then returned a constant
    // {"Diff": ""} without reading the two stored schema versions.
    let svc = GlueService::default();
    svc.create_schema(&req(
        "CreateSchema",
        json!({
            "SchemaName": "s",
            "DataFormat": "JSON",
            "Compatibility": "NONE",
            "SchemaDefinition": "{\"type\":\"record\",\"a\":1}"
        }),
    ))
    .unwrap();
    svc.register_schema_version(&req(
        "RegisterSchemaVersion",
        json!({
            "SchemaId": {"SchemaName": "s"},
            "SchemaDefinition": "{\"type\":\"record\",\"a\":2,\"b\":3}"
        }),
    ))
    .unwrap();

    // Two different versions produce a non-empty diff.
    let diff = body_of(
        svc.get_schema_versions_diff(&req(
            "GetSchemaVersionsDiff",
            json!({
                "SchemaId": {"SchemaName": "s"},
                "FirstSchemaVersionNumber": {"VersionNumber": 1},
                "SecondSchemaVersionNumber": {"VersionNumber": 2},
                "SchemaDiffType": "SYNTAX_DIFF"
            }),
        ))
        .unwrap(),
    );
    let diff_str = diff["Diff"].as_str().unwrap();
    assert!(
        !diff_str.is_empty(),
        "expected a non-empty diff, got {diff_str:?}"
    );
    assert!(
        diff_str.contains('b'),
        "diff should mention the added field: {diff_str}"
    );

    // Identical versions produce an empty diff.
    let same = body_of(
        svc.get_schema_versions_diff(&req(
            "GetSchemaVersionsDiff",
            json!({
                "SchemaId": {"SchemaName": "s"},
                "FirstSchemaVersionNumber": {"VersionNumber": 2},
                "SecondSchemaVersionNumber": {"VersionNumber": 2},
                "SchemaDiffType": "SYNTAX_DIFF"
            }),
        ))
        .unwrap(),
    );
    assert_eq!(same["Diff"], "");
}

#[test]
fn data_quality_ruleset_run_settles_and_publishes_result() {
    // Regression: a DataQuality ruleset evaluation run stayed RUNNING forever.
    let svc = GlueService::default();
    let started = body_of(
        svc.start_data_quality_ruleset_evaluation_run(&req(
            "StartDataQualityRulesetEvaluationRun",
            json!({
                "DataSource": {"GlueTable": {"DatabaseName": "db", "TableName": "t"}},
                "Role": "r",
                "RulesetNames": ["rs1"]
            }),
        ))
        .unwrap(),
    );
    let run_id = started["RunId"].as_str().unwrap().to_string();
    let got = body_of(
        svc.get_data_quality_ruleset_evaluation_run(&req(
            "GetDataQualityRulesetEvaluationRun",
            json!({"RunId": run_id}),
        ))
        .unwrap(),
    );
    assert_eq!(got["Status"], "SUCCEEDED");
    let result_id = got["ResultIds"][0].as_str().unwrap().to_string();
    // The published result resolves via GetDataQualityResult.
    let result = body_of(
        svc.get_data_quality_result(&req("GetDataQualityResult", json!({"ResultId": result_id})))
            .unwrap(),
    );
    assert_eq!(result["ResultId"], got["ResultIds"][0]);
    assert_eq!(result["RulesetName"], "rs1");
}

#[test]
fn blueprint_run_settles_to_terminal_on_read() {
    let svc = GlueService::default();
    svc.create_blueprint(&req(
        "CreateBlueprint",
        json!({"Name": "bp", "BlueprintLocation": "s3://b/bp.zip"}),
    ))
    .unwrap();
    let started = body_of(
        svc.start_blueprint_run(&req(
            "StartBlueprintRun",
            json!({"BlueprintName": "bp", "RoleArn": "arn:aws:iam::123456789012:role/r"}),
        ))
        .unwrap(),
    );
    let run_id = started["RunId"].as_str().unwrap().to_string();
    let got = body_of(
        svc.get_blueprint_run(&req(
            "GetBlueprintRun",
            json!({"BlueprintName": "bp", "RunId": run_id}),
        ))
        .unwrap(),
    );
    assert_eq!(got["BlueprintRun"]["State"], "SUCCEEDED");
}
