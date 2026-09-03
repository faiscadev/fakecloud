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
    // crawler is RUNNING in storage until a read settles it.
    assert!(svc
        .start_crawler(&req("StartCrawler", json!({"Name": "c"})))
        .is_err());

    // The first read after StartCrawler still reports RUNNING, matching AWS
    // (the crawl is in progress).
    let got = body_of(
        svc.get_crawler(&req("GetCrawler", json!({"Name": "c"})))
            .unwrap(),
    );
    assert_eq!(got["Crawler"]["State"], "RUNNING");

    // A subsequent read settles the finished crawl to READY with a LastCrawl
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
    // Poll twice to settle back to READY (first read RUNNING, second READY),
    // then delete is no longer permanently blocked.
    svc.get_crawler(&req("GetCrawler", json!({"Name": "c"})))
        .unwrap();
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
    // First read reports RUNNING (matching AWS).
    let running = body_of(
        svc.get_ml_task_run(&req(
            "GetMLTaskRun",
            json!({"TransformId": tid, "TaskRunId": task}),
        ))
        .unwrap(),
    );
    assert_eq!(running["Status"], "RUNNING");
    // A subsequent read settles the task run to a terminal state instead of
    // hanging in RUNNING forever.
    let got = body_of(
        svc.get_ml_task_run(&req(
            "GetMLTaskRun",
            json!({"TransformId": tid, "TaskRunId": task}),
        ))
        .unwrap(),
    );
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
    // The resumed run must be persisted and readable; the first read reports
    // RUNNING (matching AWS).
    let running = body_of(
        svc.get_workflow_run(&req(
            "GetWorkflowRun",
            json!({"Name": "wf", "RunId": new_id}),
        ))
        .unwrap(),
    );
    assert_eq!(running["Run"]["WorkflowRunId"], new_id);
    assert_eq!(running["Run"]["Status"], "RUNNING");
    // A subsequent GetWorkflowRun settles the run to a terminal state.
    let got = body_of(
        svc.get_workflow_run(&req(
            "GetWorkflowRun",
            json!({"Name": "wf", "RunId": new_id}),
        ))
        .unwrap(),
    );
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
    // First read reports the run as still RUNNING (matching AWS).
    let running = body_of(
        svc.get_data_quality_ruleset_evaluation_run(&req(
            "GetDataQualityRulesetEvaluationRun",
            json!({"RunId": run_id}),
        ))
        .unwrap(),
    );
    assert_eq!(running["Status"], "RUNNING");
    // A subsequent read settles it to SUCCEEDED and publishes the result.
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
    // First read reports RUNNING (matching AWS); a subsequent read settles it.
    let running = body_of(
        svc.get_blueprint_run(&req(
            "GetBlueprintRun",
            json!({"BlueprintName": "bp", "RunId": run_id}),
        ))
        .unwrap(),
    );
    assert_eq!(running["BlueprintRun"]["State"], "RUNNING");
    let got = body_of(
        svc.get_blueprint_run(&req(
            "GetBlueprintRun",
            json!({"BlueprintName": "bp", "RunId": run_id}),
        ))
        .unwrap(),
    );
    assert_eq!(got["BlueprintRun"]["State"], "SUCCEEDED");
}

// ---------------------------------------------------------------------
// Business catalog: glossaries, terms, asset types and assets
// ---------------------------------------------------------------------

fn err_code(
    r: Result<fakecloud_core::service::AwsResponse, fakecloud_core::service::AwsServiceError>,
) -> String {
    match r {
        Ok(_) => panic!("expected an error"),
        Err(e) => e.code().to_string(),
    }
}

fn make_glossary(svc: &GlueService, name: &str) -> String {
    body_of(
        svc.create_glossary(&req("CreateGlossary", json!({ "Name": name })))
            .unwrap(),
    )["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn make_term(svc: &GlueService, glossary_id: &str, name: &str) -> String {
    body_of(
        svc.create_glossary_term(&req(
            "CreateGlossaryTerm",
            json!({ "GlossaryIdentifier": glossary_id, "Name": name }),
        ))
        .unwrap(),
    )["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn make_asset_type(svc: &GlueService, name: &str) -> String {
    body_of(
        svc.put_asset_type(&req(
            "PutAssetType",
            json!({ "Name": name, "Forms": { "f1": { "FormTypeIdentifier": "ft-1" } } }),
        ))
        .unwrap(),
    )["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

fn make_asset(svc: &GlueService, type_id: &str, id: &str, name: &str, description: Option<&str>) {
    let mut b = json!({
        "AssetTypeId": type_id,
        "Identifier": id,
        "Name": name,
        "Forms": { "f1": { "FormTypeId": "ft-1", "Content": "{}" } },
    });
    if let Some(d) = description {
        b["Description"] = json!(d);
    }
    svc.put_asset(&req("PutAsset", b)).unwrap();
}

#[test]
fn glossary_and_terms_round_trip() {
    let svc = GlueService::default();
    let gid = make_glossary(&svc, "finance");
    let tid = make_term(&svc, &gid, "revenue");

    let g = body_of(
        svc.get_glossary(&req("GetGlossary", json!({ "Identifier": gid })))
            .unwrap(),
    );
    assert_eq!(g["Name"], "finance");

    let t = body_of(
        svc.get_glossary_term(&req("GetGlossaryTerm", json!({ "Identifier": tid })))
            .unwrap(),
    );
    assert_eq!(t["Name"], "revenue");
    assert_eq!(t["GlossaryId"], gid.as_str());

    // Terms list by their glossary, not account-wide.
    let other = make_glossary(&svc, "hr");
    make_term(&svc, &other, "headcount");
    let listed = body_of(
        svc.list_glossary_terms(&req(
            "ListGlossaryTerms",
            json!({ "GlossaryIdentifier": gid }),
        ))
        .unwrap(),
    );
    assert_eq!(listed["Items"].as_array().unwrap().len(), 1);
    assert_eq!(listed["Items"][0]["Name"], "revenue");

    // Update touches only what was sent.
    svc.update_glossary(&req(
        "UpdateGlossary",
        json!({ "Identifier": gid, "Description": "money words" }),
    ))
    .unwrap();
    let g = body_of(
        svc.get_glossary(&req("GetGlossary", json!({ "Identifier": gid })))
            .unwrap(),
    );
    assert_eq!(g["Name"], "finance");
    assert_eq!(g["Description"], "money words");

    // A duplicate glossary name conflicts.
    assert_eq!(
        err_code(svc.create_glossary(&req("CreateGlossary", json!({ "Name": "finance" })))),
        "AlreadyExistsException"
    );
    // A term needs a glossary that exists.
    assert_eq!(
        err_code(svc.create_glossary_term(&req(
            "CreateGlossaryTerm",
            json!({ "GlossaryIdentifier": "no-such", "Name": "x" })
        ))),
        "EntityNotFoundException"
    );
}

#[test]
fn deleting_a_glossary_takes_its_terms_and_associations() {
    let svc = GlueService::default();
    let gid = make_glossary(&svc, "finance");
    let tid = make_term(&svc, &gid, "revenue");
    let type_id = make_asset_type(&svc, "table");
    make_asset(&svc, &type_id, "asset-1", "sales", None);

    svc.associate_glossary_terms(&req(
        "AssociateGlossaryTerms",
        json!({ "AssetIdentifier": "asset-1", "GlossaryTermIdentifiers": [tid] }),
    ))
    .unwrap();
    let a = body_of(
        svc.get_asset(&req("GetAsset", json!({ "Identifier": "asset-1" })))
            .unwrap(),
    );
    assert_eq!(a["GlossaryTerms"][0], tid.as_str());

    // Deleting the glossary removes its terms and drops them off the asset.
    svc.delete_glossary(&req("DeleteGlossary", json!({ "Identifier": gid })))
        .unwrap();
    assert_eq!(
        err_code(svc.get_glossary_term(&req("GetGlossaryTerm", json!({ "Identifier": tid })))),
        "EntityNotFoundException"
    );
    let a = body_of(
        svc.get_asset(&req("GetAsset", json!({ "Identifier": "asset-1" })))
            .unwrap(),
    );
    assert!(a.get("GlossaryTerms").is_none(), "{a}");
}

#[test]
fn glossary_term_association_is_validated_and_reversible() {
    let svc = GlueService::default();
    let gid = make_glossary(&svc, "g");
    let tid = make_term(&svc, &gid, "t");
    let type_id = make_asset_type(&svc, "table");
    make_asset(&svc, &type_id, "asset-1", "sales", None);

    // Both the asset and every term must exist.
    assert_eq!(
        err_code(svc.associate_glossary_terms(&req(
            "AssociateGlossaryTerms",
            json!({ "AssetIdentifier": "ghost", "GlossaryTermIdentifiers": [tid] })
        ))),
        "EntityNotFoundException"
    );
    assert_eq!(
        err_code(svc.associate_glossary_terms(&req(
            "AssociateGlossaryTerms",
            json!({ "AssetIdentifier": "asset-1", "GlossaryTermIdentifiers": ["ghost"] })
        ))),
        "EntityNotFoundException"
    );
    assert_eq!(
        err_code(svc.associate_glossary_terms(&req(
            "AssociateGlossaryTerms",
            json!({ "AssetIdentifier": "asset-1", "GlossaryTermIdentifiers": [] })
        ))),
        "InvalidInputException"
    );

    // Associating twice does not duplicate.
    for _ in 0..2 {
        svc.associate_glossary_terms(&req(
            "AssociateGlossaryTerms",
            json!({ "AssetIdentifier": "asset-1", "GlossaryTermIdentifiers": [tid] }),
        ))
        .unwrap();
    }
    let a = body_of(
        svc.get_asset(&req("GetAsset", json!({ "Identifier": "asset-1" })))
            .unwrap(),
    );
    assert_eq!(a["GlossaryTerms"].as_array().unwrap().len(), 1);

    let out = body_of(
        svc.disassociate_glossary_terms(&req(
            "DisassociateGlossaryTerms",
            json!({ "AssetIdentifier": "asset-1", "GlossaryTermIdentifiers": [tid] }),
        ))
        .unwrap(),
    );
    assert!(out["GlossaryTerms"].as_array().unwrap().is_empty());
}

#[test]
fn assets_require_a_type_and_put_upserts() {
    let svc = GlueService::default();
    let type_id = make_asset_type(&svc, "table");

    // An asset cannot name a type that does not exist.
    assert_eq!(
        err_code(svc.put_asset(&req(
            "PutAsset",
            json!({
                "AssetTypeId": "no-such",
                "Identifier": "a1",
                "Name": "n",
                "Forms": {},
            })
        ))),
        "EntityNotFoundException"
    );

    make_asset(&svc, &type_id, "a1", "sales", Some("first"));
    let first = body_of(
        svc.get_asset(&req("GetAsset", json!({ "Identifier": "a1" })))
            .unwrap(),
    );
    let created = first["CreatedAt"].as_f64().unwrap();

    // Put is an upsert: the second write keeps the original creation time.
    make_asset(&svc, &type_id, "a1", "sales-v2", Some("second"));
    let second = body_of(
        svc.get_asset(&req("GetAsset", json!({ "Identifier": "a1" })))
            .unwrap(),
    );
    assert_eq!(second["Name"], "sales-v2");
    assert_eq!(second["CreatedAt"].as_f64().unwrap(), created);

    // An asset type still in use cannot be deleted.
    assert_eq!(
        err_code(svc.delete_asset_type(&req("DeleteAssetType", json!({ "Identifier": type_id })))),
        "InvalidInputException"
    );
    svc.delete_asset(&req("DeleteAsset", json!({ "Identifier": "a1" })))
        .unwrap();
    svc.delete_asset_type(&req("DeleteAssetType", json!({ "Identifier": type_id })))
        .unwrap();
}

#[test]
fn put_asset_type_is_upsert_by_name() {
    let svc = GlueService::default();
    let first = make_asset_type(&svc, "table");
    let second = make_asset_type(&svc, "table");
    assert_eq!(first, second, "a repeat Put reuses the type's id");
    let listed = body_of(
        svc.list_asset_types(&req("ListAssetTypes", json!({})))
            .unwrap(),
    );
    assert_eq!(listed["Items"].as_array().unwrap().len(), 1);
}

#[test]
fn search_assets_matches_name_and_description_and_sorts() {
    let svc = GlueService::default();
    let type_id = make_asset_type(&svc, "table");
    make_asset(&svc, &type_id, "a1", "sales_daily", Some("revenue by day"));
    make_asset(&svc, &type_id, "a2", "inventory", Some("stock levels"));
    make_asset(&svc, &type_id, "a3", "sales_monthly", None);

    let hits = |text: &str| -> Vec<String> {
        let b = body_of(
            svc.search_assets(&req("SearchAssets", json!({ "SearchText": text })))
                .unwrap(),
        );
        b["Items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["AssetName"].as_str().unwrap().to_string())
            .collect()
    };

    // Name substring, case-insensitive.
    assert_eq!(hits("SALES"), vec!["sales_daily", "sales_monthly"]);
    // Description substring too.
    assert_eq!(hits("stock"), vec!["inventory"]);
    // An empty search matches everything.
    assert_eq!(hits("").len(), 3);
    assert!(hits("nothing-matches").is_empty());

    // DESCENDING reverses the name ordering.
    let b = body_of(
        svc.search_assets(&req(
            "SearchAssets",
            json!({ "Sort": { "Attribute": "Name", "Order": "DESCENDING" } }),
        ))
        .unwrap(),
    );
    let names: Vec<&str> = b["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["AssetName"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["sales_monthly", "sales_daily", "inventory"]);

    assert_eq!(
        err_code(svc.search_assets(&req(
            "SearchAssets",
            json!({ "Sort": { "Attribute": "Name", "Order": "SIDEWAYS" } })
        ))),
        "InvalidInputException"
    );
}

/// The business-catalog deletes declare no EntityNotFoundException in the
/// Smithy model, so deleting something absent succeeds instead of erroring.
/// The `Get` counterparts do declare it, and keep erroring.
#[test]
fn business_catalog_deletes_are_idempotent() {
    let svc = GlueService::default();

    for (action, call) in [
        ("DeleteGlossary", 0),
        ("DeleteGlossaryTerm", 1),
        ("DeleteAsset", 2),
        ("DeleteAssetType", 3),
    ] {
        let r = req(action, json!({ "Identifier": "never-existed" }));
        let res = match call {
            0 => svc.delete_glossary(&r),
            1 => svc.delete_glossary_term(&r),
            2 => svc.delete_asset(&r),
            _ => svc.delete_asset_type(&r),
        };
        assert!(res.is_ok(), "{action} on an absent entity must not error");
    }

    // Deleting a real glossary twice is equally quiet, and still cascades.
    let gid = make_glossary(&svc, "finance");
    let tid = make_term(&svc, &gid, "revenue");
    let del = req("DeleteGlossary", json!({ "Identifier": gid.clone() }));
    assert!(svc.delete_glossary(&del).is_ok());
    assert!(svc.delete_glossary(&del).is_ok());
    assert_eq!(
        err_code(svc.get_glossary_term(&req("GetGlossaryTerm", json!({ "Identifier": tid })))),
        "EntityNotFoundException",
        "the glossary's terms go with it"
    );
}

/// ListGlossaryTerms has no not-found error in its model either, so terms of
/// an unknown glossary come back as an empty page.
#[test]
fn list_glossary_terms_of_unknown_glossary_is_empty() {
    let svc = GlueService::default();
    let out = body_of(
        svc.list_glossary_terms(&req(
            "ListGlossaryTerms",
            json!({ "GlossaryIdentifier": "never-existed" }),
        ))
        .unwrap(),
    );
    assert_eq!(out["Items"].as_array().map(Vec::len), Some(0));
    assert!(out.get("NextToken").is_none());
}

/// Constraint validation is model-driven: the generated table covers the
/// business-catalog ops, so out-of-range inputs are rejected before any handler.
#[test]
fn business_catalog_inputs_are_constraint_checked() {
    use crate::common::validate_constraints;

    // Name has a @length minimum of 1.
    assert!(validate_constraints("CreateGlossary", &json!({ "Name": "" })).is_err());
    assert!(validate_constraints("CreateGlossary", &json!({ "Name": "finance" })).is_ok());

    // MaxResults carries a @range on the list and search ops.
    for action in ["ListGlossaries", "ListAssetTypes", "SearchAssets"] {
        assert!(
            validate_constraints(action, &json!({ "MaxResults": 0 })).is_err(),
            "{action} must reject MaxResults below its range minimum"
        );
        assert!(validate_constraints(action, &json!({ "MaxResults": 25 })).is_ok());
    }
}

/// An asset carrying an iterable form: the form's content is a JSON array, one
/// element per item, which is where iterable-form items come from.
fn make_asset_with_items(svc: &GlueService, type_id: &str, id: &str) {
    let content = json!([
        { "ItemId": "i-1", "ItemName": "first", "Description": "the first item" },
        { "ItemId": "i-2", "ItemName": "second" },
    ])
    .to_string();
    svc.put_asset(&req(
        "PutAsset",
        json!({
            "AssetTypeId": type_id,
            "Identifier": id,
            "Name": id,
            "Forms": { "rows": { "FormTypeId": "ft-1", "Content": content } },
        }),
    ))
    .unwrap();
}

fn make_form_type(svc: &GlueService, name: &str) -> String {
    body_of(
        svc.put_form_type(&req(
            "PutFormType",
            json!({ "Name": name, "Schema": "{\"type\":\"object\"}" }),
        ))
        .unwrap(),
    )["Id"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn form_type_put_is_an_upsert_keyed_by_name() {
    let svc = GlueService::default();
    let id = make_form_type(&svc, "row_schema");
    let again = make_form_type(&svc, "row_schema");
    assert_eq!(id, again, "re-putting a name must keep its id");

    let got = body_of(
        svc.get_form_type(&req("GetFormType", json!({ "Identifier": id.clone() })))
            .unwrap(),
    );
    assert_eq!(got["Name"], "row_schema");

    let listed = body_of(
        svc.list_form_types(&req("ListFormTypes", json!({})))
            .unwrap(),
    );
    assert_eq!(listed["Items"].as_array().map(Vec::len), Some(1));

    // Delete declares no EntityNotFoundException, so it is idempotent.
    let del = req("DeleteFormType", json!({ "Identifier": id.clone() }));
    assert!(svc.delete_form_type(&del).is_ok());
    assert!(svc.delete_form_type(&del).is_ok());
    assert_eq!(
        err_code(svc.get_form_type(&req("GetFormType", json!({ "Identifier": id })))),
        "EntityNotFoundException"
    );
}

#[test]
fn form_type_in_use_cannot_be_deleted() {
    let svc = GlueService::default();
    let ft = make_form_type(&svc, "row_schema");
    let type_id = make_asset_type(&svc, "table");
    svc.put_asset(&req(
        "PutAsset",
        json!({
            "AssetTypeId": type_id,
            "Identifier": "a-1",
            "Name": "sales",
            "Forms": { "f1": { "FormTypeId": ft.clone(), "Content": "{}" } },
        }),
    ))
    .unwrap();

    assert_eq!(
        err_code(svc.delete_form_type(&req("DeleteFormType", json!({ "Identifier": ft.clone() })))),
        "ConflictException"
    );

    svc.delete_asset(&req("DeleteAsset", json!({ "Identifier": "a-1" })))
        .unwrap();

    // An asset type referencing it holds it too. Asset-type forms name the
    // reference `FormTypeIdentifier`, one level inside the Forms map.
    svc.put_asset_type(&req(
        "PutAssetType",
        json!({ "Name": "typed", "Forms": { "f1": { "FormTypeIdentifier": ft.clone() } } }),
    ))
    .unwrap();
    assert_eq!(
        err_code(svc.delete_form_type(&req("DeleteFormType", json!({ "Identifier": ft.clone() })))),
        "ConflictException",
        "an asset type's form reference must block the delete"
    );
    let typed_id = body_of(
        svc.put_asset_type(&req(
            "PutAssetType",
            json!({ "Name": "typed", "Forms": { "f1": { "FormTypeIdentifier": "other" } } }),
        ))
        .unwrap(),
    )["Id"]
        .as_str()
        .unwrap()
        .to_string();
    svc.delete_asset_type(&req("DeleteAssetType", json!({ "Identifier": typed_id })))
        .unwrap();

    // An attachment referencing it holds it as well.
    let plain_type = make_asset_type(&svc, "plain");
    make_asset_with_items(&svc, &plain_type, "a-2");
    svc.put_attachment(&req(
        "PutAttachment",
        json!({
            "AssetIdentifier": "a-2",
            "AttachmentName": "readme",
            "Content": "hello",
            "FormTypeId": ft.clone(),
        }),
    ))
    .unwrap();
    assert_eq!(
        err_code(svc.delete_form_type(&req("DeleteFormType", json!({ "Identifier": ft.clone() })))),
        "ConflictException",
        "an attachment's form reference must block the delete"
    );

    // Once nothing references it, it goes.
    svc.delete_asset(&req("DeleteAsset", json!({ "Identifier": "a-2" })))
        .unwrap();
    assert!(svc
        .delete_form_type(&req("DeleteFormType", json!({ "Identifier": ft })))
        .is_ok());
}

#[test]
fn iterable_forms_list_and_batch_get_their_items() {
    let svc = GlueService::default();
    let type_id = make_asset_type(&svc, "table");
    make_asset_with_items(&svc, &type_id, "a-1");

    let listed = body_of(
        svc.list_iterable_forms(&req(
            "ListIterableForms",
            json!({ "AssetIdentifier": "a-1", "IterableFormName": "rows" }),
        ))
        .unwrap(),
    );
    let items = listed["Items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["ItemId"], "i-1");
    assert_eq!(items[0]["Description"], "the first item");

    // A batch read reports misses per item instead of failing the call.
    let got = body_of(
        svc.batch_get_iterable_forms(&req(
            "BatchGetIterableForms",
            json!({
                "AssetIdentifier": "a-1",
                "IterableFormName": "rows",
                "ItemIdentifiers": ["i-1", "ghost"],
            }),
        ))
        .unwrap(),
    );
    assert_eq!(got["Items"].as_array().map(Vec::len), Some(1));
    assert_eq!(got["Items"][0]["ItemId"], "i-1");
    assert_eq!(got["Errors"][0]["ItemIdentifier"], "ghost");
    assert_eq!(got["Errors"][0]["Code"], "EntityNotFoundException");

    // A form that is not on the asset simply has no items.
    let none = body_of(
        svc.list_iterable_forms(&req(
            "ListIterableForms",
            json!({ "AssetIdentifier": "a-1", "IterableFormName": "absent" }),
        ))
        .unwrap(),
    );
    assert_eq!(none["Items"].as_array().map(Vec::len), Some(0));
}

#[test]
fn glossary_terms_scope_to_an_iterable_form_item() {
    let svc = GlueService::default();
    let gid = make_glossary(&svc, "finance");
    let tid = make_term(&svc, &gid, "revenue");
    let type_id = make_asset_type(&svc, "table");
    make_asset_with_items(&svc, &type_id, "a-1");

    let scoped = json!({
        "AssetIdentifier": "a-1",
        "IterableFormName": "rows",
        "ItemIdentifier": "i-1",
        "GlossaryTermIdentifiers": [tid.clone()],
    });
    svc.associate_glossary_terms(&req("AssociateGlossaryTerms", scoped.clone()))
        .unwrap();

    // The term shows on that item, and only that item.
    let listed = body_of(
        svc.list_iterable_forms(&req(
            "ListIterableForms",
            json!({ "AssetIdentifier": "a-1", "IterableFormName": "rows" }),
        ))
        .unwrap(),
    );
    assert_eq!(listed["Items"][0]["GlossaryTerms"], json!([tid.clone()]));
    assert!(listed["Items"][1].get("GlossaryTerms").is_none());

    // An item-scoped association is not an asset-level one.
    let asset = body_of(
        svc.get_asset(&req("GetAsset", json!({ "Identifier": "a-1" })))
            .unwrap(),
    );
    assert!(asset.get("GlossaryTerms").is_none());

    // An unknown item is rejected rather than silently creating a scope.
    assert_eq!(
        err_code(svc.associate_glossary_terms(&req(
            "AssociateGlossaryTerms",
            json!({
                "AssetIdentifier": "a-1",
                "IterableFormName": "rows",
                "ItemIdentifier": "ghost",
                "GlossaryTermIdentifiers": [tid.clone()],
            })
        ))),
        "EntityNotFoundException"
    );

    svc.disassociate_glossary_terms(&req("DisassociateGlossaryTerms", scoped))
        .unwrap();
    let after = body_of(
        svc.list_iterable_forms(&req(
            "ListIterableForms",
            json!({ "AssetIdentifier": "a-1", "IterableFormName": "rows" }),
        ))
        .unwrap(),
    );
    assert!(after["Items"][0].get("GlossaryTerms").is_none());
}

#[test]
fn attachments_hang_off_assets_and_items() {
    let svc = GlueService::default();
    let ft = make_form_type(&svc, "note");
    let type_id = make_asset_type(&svc, "table");
    make_asset_with_items(&svc, &type_id, "a-1");

    let put = json!({
        "AssetIdentifier": "a-1",
        "IterableFormName": "rows",
        "ItemIdentifier": "i-1",
        "AttachmentName": "readme",
        "Content": "hello",
        "FormTypeId": ft.clone(),
    });
    let out = body_of(
        svc.put_attachment(&req("PutAttachment", put.clone()))
            .unwrap(),
    );
    assert_eq!(out["AttachmentName"], "readme");
    assert_eq!(out["ItemIdentifier"], "i-1");

    // It surfaces on the item it was attached to.
    let got = body_of(
        svc.batch_get_iterable_forms(&req(
            "BatchGetIterableForms",
            json!({
                "AssetIdentifier": "a-1",
                "IterableFormName": "rows",
                "ItemIdentifiers": ["i-1"],
            }),
        ))
        .unwrap(),
    );
    assert_eq!(got["Items"][0]["Attachments"]["readme"]["Content"], "hello");

    // Both the asset and the form type must exist.
    let mut ghost_asset = put.clone();
    ghost_asset["AssetIdentifier"] = json!("nope");
    assert_eq!(
        err_code(svc.put_attachment(&req("PutAttachment", ghost_asset))),
        "EntityNotFoundException"
    );
    let mut ghost_type = put.clone();
    ghost_type["FormTypeId"] = json!("nope");
    assert_eq!(
        err_code(svc.put_attachment(&req("PutAttachment", ghost_type))),
        "EntityNotFoundException"
    );
    let mut ghost_item = put.clone();
    ghost_item["ItemIdentifier"] = json!("nope");
    assert_eq!(
        err_code(svc.put_attachment(&req("PutAttachment", ghost_item))),
        "EntityNotFoundException"
    );

    svc.delete_attachment(&req(
        "DeleteAttachment",
        json!({
            "AssetIdentifier": "a-1",
            "IterableFormName": "rows",
            "ItemIdentifier": "i-1",
            "AttachmentName": "readme",
        }),
    ))
    .unwrap();
    let after = body_of(
        svc.batch_get_iterable_forms(&req(
            "BatchGetIterableForms",
            json!({
                "AssetIdentifier": "a-1",
                "IterableFormName": "rows",
                "ItemIdentifiers": ["i-1"],
            }),
        ))
        .unwrap(),
    );
    assert!(after["Items"][0].get("Attachments").is_none());
}

#[test]
fn deleting_an_asset_takes_its_item_scoped_state() {
    let svc = GlueService::default();
    let ft = make_form_type(&svc, "note");
    let gid = make_glossary(&svc, "g");
    let tid = make_term(&svc, &gid, "t");
    let type_id = make_asset_type(&svc, "table");
    make_asset_with_items(&svc, &type_id, "a-1");

    svc.associate_glossary_terms(&req(
        "AssociateGlossaryTerms",
        json!({
            "AssetIdentifier": "a-1",
            "IterableFormName": "rows",
            "ItemIdentifier": "i-1",
            "GlossaryTermIdentifiers": [tid],
        }),
    ))
    .unwrap();
    svc.put_attachment(&req(
        "PutAttachment",
        json!({
            "AssetIdentifier": "a-1",
            "IterableFormName": "rows",
            "ItemIdentifier": "i-1",
            "AttachmentName": "readme",
            "Content": "hello",
            "FormTypeId": ft.clone(),
        }),
    ))
    .unwrap();

    svc.delete_asset(&req("DeleteAsset", json!({ "Identifier": "a-1" })))
        .unwrap();

    // Nothing item-scoped may outlive the asset, or re-creating the asset would
    // inherit a previous asset's terms and attachments.
    make_asset_with_items(&svc, &type_id, "a-1");
    let got = body_of(
        svc.batch_get_iterable_forms(&req(
            "BatchGetIterableForms",
            json!({
                "AssetIdentifier": "a-1",
                "IterableFormName": "rows",
                "ItemIdentifiers": ["i-1"],
            }),
        ))
        .unwrap(),
    );
    assert!(got["Items"][0].get("Attachments").is_none());
    assert!(got["Items"][0].get("GlossaryTerms").is_none());
}

#[test]
fn data_catalog_export_configuration_round_trips() {
    let svc = GlueService::default();
    // Nothing is configured until it is put.
    assert_eq!(
        err_code(svc.get_data_catalog_export_configuration(&req(
            "GetDataCatalogExportConfiguration",
            json!({})
        ))),
        "EntityNotFoundException"
    );

    let out = body_of(
        svc.put_data_catalog_export_configuration(&req(
            "PutDataCatalogExportConfiguration",
            json!({
                "ExportSetting": "ENABLED",
                "EncryptionConfiguration": { "SseAlgorithm": "AES256" },
            }),
        ))
        .unwrap(),
    );
    assert_eq!(out["ExportSetting"], "ENABLED");

    let got = body_of(
        svc.get_data_catalog_export_configuration(&req(
            "GetDataCatalogExportConfiguration",
            json!({}),
        ))
        .unwrap(),
    );
    assert_eq!(got["Status"], "ENABLED");
    assert_eq!(got["EncryptionConfiguration"]["SseAlgorithm"], "AES256");
    let created = got["CreatedAt"].as_f64().unwrap();

    // Disabling keeps the original creation time.
    svc.put_data_catalog_export_configuration(&req(
        "PutDataCatalogExportConfiguration",
        json!({ "ExportSetting": "DISABLED" }),
    ))
    .unwrap();
    let after = body_of(
        svc.get_data_catalog_export_configuration(&req(
            "GetDataCatalogExportConfiguration",
            json!({}),
        ))
        .unwrap(),
    );
    assert_eq!(after["Status"], "DISABLED");
    assert_eq!(after["CreatedAt"].as_f64(), Some(created));

    assert_eq!(
        err_code(svc.put_data_catalog_export_configuration(&req(
            "PutDataCatalogExportConfiguration",
            json!({ "ExportSetting": "MAYBE" })
        ))),
        "InvalidInputException"
    );
}

#[test]
fn get_asset_reports_attachments_and_iterable_forms() {
    let svc = GlueService::default();
    let ft = make_form_type(&svc, "note");
    let type_id = make_asset_type(&svc, "table");
    make_asset_with_items(&svc, &type_id, "a-1");

    // A form whose content is a JSON array is the iterable one.
    let asset = body_of(
        svc.get_asset(&req("GetAsset", json!({ "Identifier": "a-1" })))
            .unwrap(),
    );
    assert_eq!(asset["IterableForms"]["rows"]["FormTypeId"], "ft-1");
    assert!(asset.get("Attachments").is_none());

    svc.put_attachment(&req(
        "PutAttachment",
        json!({
            "AssetIdentifier": "a-1",
            "AttachmentName": "readme",
            "Content": "hello",
            "FormTypeId": ft.clone(),
        }),
    ))
    .unwrap();
    // An item-scoped attachment belongs to its item, not to the asset.
    svc.put_attachment(&req(
        "PutAttachment",
        json!({
            "AssetIdentifier": "a-1",
            "IterableFormName": "rows",
            "ItemIdentifier": "i-1",
            "AttachmentName": "scoped",
            "Content": "inner",
            "FormTypeId": ft,
        }),
    ))
    .unwrap();

    let asset = body_of(
        svc.get_asset(&req("GetAsset", json!({ "Identifier": "a-1" })))
            .unwrap(),
    );
    assert_eq!(asset["Attachments"]["readme"]["Content"], "hello");
    assert!(
        asset["Attachments"].get("scoped").is_none(),
        "an item-scoped attachment must not surface as an asset attachment"
    );
}

#[test]
fn search_assets_evaluates_the_filter_clause() {
    let svc = GlueService::default();
    let type_id = make_asset_type(&svc, "table");
    make_asset(&svc, &type_id, "a-1", "sales", Some("quarterly revenue"));
    make_asset(&svc, &type_id, "a-2", "hr", Some("headcount"));

    let search = |clause: Value| -> Vec<String> {
        let out = body_of(
            svc.search_assets(&req("SearchAssets", json!({ "FilterClause": clause })))
                .unwrap(),
        );
        out["Items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["Id"].as_str().unwrap().to_string())
            .collect()
    };

    assert_eq!(
        search(json!({
            "AttributeFilter": { "Attribute": "AssetName", "Operator": "equals", "Value": "sales" }
        })),
        vec!["a-1".to_string()]
    );

    // The two list arms nest.
    assert_eq!(
        search(json!({
            "OrAnyFilters": [
                { "AttributeFilter": { "Attribute": "AssetName", "Operator": "equals", "Value": "sales" } },
                { "AttributeFilter": { "Attribute": "AssetName", "Operator": "equals", "Value": "hr" } },
            ]
        }))
        .len(),
        2
    );
    assert!(search(json!({
        "AndAllFilters": [
            { "AttributeFilter": { "Attribute": "AssetName", "Operator": "equals", "Value": "sales" } },
            { "AttributeFilter": { "Attribute": "AssetName", "Operator": "equals", "Value": "hr" } },
        ]
    }))
    .is_empty());

    // notExists matches the assets that lack the attribute entirely.
    make_asset(&svc, &type_id, "a-3", "ops", None);
    assert_eq!(
        search(json!({
            "AttributeFilter": { "Attribute": "AssetDescription", "Operator": "notExists" }
        })),
        vec!["a-3".to_string()]
    );

    // SearchText and the filter both have to hold.
    let out = body_of(
        svc.search_assets(&req(
            "SearchAssets",
            json!({
                "SearchText": "sales",
                "FilterClause": {
                    "AttributeFilter": { "Attribute": "AssetName", "Operator": "equals", "Value": "hr" }
                },
            }),
        ))
        .unwrap(),
    );
    assert_eq!(out["Items"].as_array().map(Vec::len), Some(0));
}

#[test]
fn list_and_map_constraints_are_checked_by_cardinality() {
    use crate::common::validate_constraints;

    // GlossaryTermIdentifiers is a list with a @length maximum; a string-length
    // check would never fire on it.
    let one = json!({ "AssetIdentifier": "a", "GlossaryTermIdentifiers": ["t-1"] });
    assert!(validate_constraints("AssociateGlossaryTerms", &one).is_ok());

    let too_many: Vec<String> = (0..1000).map(|i| format!("t-{i}")).collect();
    let over = json!({ "AssetIdentifier": "a", "GlossaryTermIdentifiers": too_many });
    let bounded = crate::constraints::constraints_for("AssociateGlossaryTerms")
        .iter()
        .any(|c| c.field == "GlossaryTermIdentifiers" && c.len_max.is_some());
    if bounded {
        assert!(
            validate_constraints("AssociateGlossaryTerms", &over).is_err(),
            "an oversized list must be rejected"
        );
    }
}
