//! Glue admin introspection helpers consumed by `/_fakecloud/glue/*` routes.
//!
//! These read the in-memory state cross-account and produce assertion-friendly
//! rows. They intentionally bypass IAM — admin endpoints never authenticate.

use crate::state::{Job, JobRun, SharedGlueState};

#[derive(Debug, Clone)]
pub struct CrawlerRow {
    pub account_id: String,
    pub name: String,
    pub role: String,
    pub database_name: Option<String>,
    pub state: String,
    /// Short human-readable summary of the configured targets, e.g.
    /// "2 S3, 1 JDBC".
    pub target_summary: String,
    pub schedule: Option<String>,
    pub creation_time: chrono::DateTime<chrono::Utc>,
    pub last_updated: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct JobRow {
    pub account_id: String,
    pub name: String,
    pub role: String,
    pub command: serde_json::Value,
    pub default_arguments: std::collections::BTreeMap<String, String>,
    pub max_capacity: Option<f64>,
    pub max_retries: i64,
    pub timeout: Option<i64>,
    pub glue_version: Option<String>,
    pub worker_type: Option<String>,
    pub number_of_workers: Option<i64>,
    pub created_on: chrono::DateTime<chrono::Utc>,
    pub last_modified_on: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct JobRunRow {
    pub account_id: String,
    pub id: String,
    pub job_name: String,
    pub attempt: i64,
    pub started_on: chrono::DateTime<chrono::Utc>,
    pub completed_on: Option<chrono::DateTime<chrono::Utc>>,
    pub job_run_state: String,
    pub arguments: std::collections::BTreeMap<String, String>,
    pub error_message: Option<String>,
    pub execution_time: i64,
}

fn job_to_row(account_id: &str, j: &Job) -> JobRow {
    JobRow {
        account_id: account_id.to_string(),
        name: j.name.clone(),
        role: j.role.clone(),
        command: j.command.clone(),
        default_arguments: j.default_arguments.clone(),
        max_capacity: j.max_capacity,
        max_retries: j.max_retries,
        timeout: j.timeout,
        glue_version: j.glue_version.clone(),
        worker_type: j.worker_type.clone(),
        number_of_workers: j.number_of_workers,
        created_on: j.created_on,
        last_modified_on: j.last_modified_on,
    }
}

fn run_to_row(account_id: &str, r: &JobRun) -> JobRunRow {
    JobRunRow {
        account_id: account_id.to_string(),
        id: r.id.clone(),
        job_name: r.job_name.clone(),
        attempt: r.attempt,
        started_on: r.started_on,
        completed_on: r.completed_on,
        job_run_state: r.state.clone(),
        arguments: r.arguments.clone(),
        error_message: r.error_message.clone(),
        execution_time: r.execution_time,
    }
}

pub fn list_all_jobs(state: &SharedGlueState) -> Vec<JobRow> {
    let accounts = state.read();
    let mut rows: Vec<JobRow> = accounts
        .accounts
        .iter()
        .flat_map(|(account_id, s)| s.jobs.values().map(|j| job_to_row(account_id, j)))
        .collect();
    rows.sort_by(|a, b| {
        a.account_id
            .cmp(&b.account_id)
            .then_with(|| a.name.cmp(&b.name))
    });
    rows
}

pub fn list_all_job_runs(state: &SharedGlueState, job_name: Option<&str>) -> Vec<JobRunRow> {
    let accounts = state.read();
    let mut rows: Vec<JobRunRow> = accounts
        .accounts
        .iter()
        .flat_map(|(account_id, s)| {
            s.job_runs
                .values()
                .filter(|r| job_name.is_none_or(|n| r.job_name == n))
                .map(|r| run_to_row(account_id, r))
        })
        .collect();
    rows.sort_by(|a, b| {
        a.account_id
            .cmp(&b.account_id)
            .then_with(|| a.started_on.cmp(&b.started_on))
            .then_with(|| a.id.cmp(&b.id))
    });
    rows
}

/// Convert an epoch-seconds JSON number (the wire form Glue stores for
/// `Timestamp` members) into a UTC datetime. Falls back to the epoch on
/// malformed/missing input so a row is always emitted.
fn ts_from_json(v: Option<&serde_json::Value>) -> chrono::DateTime<chrono::Utc> {
    let secs = v.and_then(serde_json::Value::as_f64).unwrap_or(0.0);
    chrono::DateTime::from_timestamp(secs as i64, 0).unwrap_or_else(chrono::Utc::now)
}

/// Build a "N S3, M JDBC, ..." summary from a crawler's `Targets` object.
fn target_summary(targets: Option<&serde_json::Value>) -> String {
    let Some(obj) = targets.and_then(serde_json::Value::as_object) else {
        return "0 targets".to_string();
    };
    let mut parts = Vec::new();
    for (key, label) in [
        ("S3Targets", "S3"),
        ("JdbcTargets", "JDBC"),
        ("MongoDBTargets", "MongoDB"),
        ("DynamoDBTargets", "DynamoDB"),
        ("CatalogTargets", "Catalog"),
        ("DeltaTargets", "Delta"),
        ("IcebergTargets", "Iceberg"),
        ("HudiTargets", "Hudi"),
    ] {
        let n = obj
            .get(key)
            .and_then(serde_json::Value::as_array)
            .map(Vec::len)
            .unwrap_or(0);
        if n > 0 {
            parts.push(format!("{n} {label}"));
        }
    }
    if parts.is_empty() {
        "0 targets".to_string()
    } else {
        parts.join(", ")
    }
}

fn crawler_to_row(account_id: &str, name: &str, c: &serde_json::Value) -> CrawlerRow {
    let schedule = c
        .get("Schedule")
        .and_then(|s| s.get("ScheduleExpression"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);
    CrawlerRow {
        account_id: account_id.to_string(),
        name: name.to_string(),
        role: c
            .get("Role")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string(),
        database_name: c
            .get("DatabaseName")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string),
        state: c
            .get("State")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("READY")
            .to_string(),
        target_summary: target_summary(c.get("Targets")),
        schedule,
        creation_time: ts_from_json(c.get("CreationTime")),
        last_updated: ts_from_json(c.get("LastUpdated")),
    }
}

pub fn list_all_crawlers(state: &SharedGlueState) -> Vec<CrawlerRow> {
    let accounts = state.read();
    let mut rows: Vec<CrawlerRow> = accounts
        .accounts
        .iter()
        .flat_map(|(account_id, s)| {
            s.crawlers
                .iter()
                .map(|(name, c)| crawler_to_row(account_id, name, c))
        })
        .collect();
    rows.sort_by(|a, b| {
        a.account_id
            .cmp(&b.account_id)
            .then_with(|| a.name.cmp(&b.name))
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::GlueService;
    use fakecloud_core::service::AwsRequest;
    use serde_json::json;

    fn req(action: &str, body: serde_json::Value, account: &str) -> AwsRequest {
        use bytes::Bytes;
        use http::{HeaderMap, Method};
        use std::collections::HashMap;
        AwsRequest {
            service: "glue".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: account.to_string(),
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

    #[test]
    fn lists_jobs_and_runs_across_accounts() {
        let svc = GlueService::default();
        // Account A: 1 job + 1 run
        svc.create_job(&req(
            "CreateJob",
            json!({
                "Name": "etl",
                "Role": "arn:aws:iam::111111111111:role/glue",
                "Command": {"Name": "glueetl", "ScriptLocation": "s3://b/s.py"}
            }),
            "111111111111",
        ))
        .unwrap();
        svc.start_job_run(&req(
            "StartJobRun",
            json!({"JobName": "etl"}),
            "111111111111",
        ))
        .unwrap();
        // Account B: 1 job
        svc.create_job(&req(
            "CreateJob",
            json!({
                "Name": "other",
                "Role": "arn:aws:iam::222222222222:role/glue",
                "Command": {"Name": "glueetl"}
            }),
            "222222222222",
        ))
        .unwrap();

        let jobs = list_all_jobs(&svc.state);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].account_id, "111111111111");
        assert_eq!(jobs[0].name, "etl");
        assert_eq!(jobs[1].account_id, "222222222222");

        let runs = list_all_job_runs(&svc.state, None);
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].job_name, "etl");
        assert_eq!(runs[0].job_run_state, "SUCCEEDED");

        // Filter
        let filtered = list_all_job_runs(&svc.state, Some("etl"));
        assert_eq!(filtered.len(), 1);
        let none = list_all_job_runs(&svc.state, Some("missing"));
        assert!(none.is_empty());
    }

    #[test]
    fn lists_crawlers_across_accounts() {
        let svc = GlueService::default();
        svc.create_crawler(&req(
            "CreateCrawler",
            json!({
                "Name": "inventory",
                "Role": "arn:aws:iam::111111111111:role/glue",
                "DatabaseName": "analytics",
                "Targets": {
                    "S3Targets": [{"Path": "s3://b/a"}, {"Path": "s3://b/c"}],
                    "JdbcTargets": [{"ConnectionName": "conn"}]
                }
            }),
            "111111111111",
        ))
        .unwrap();
        svc.create_crawler(&req(
            "CreateCrawler",
            json!({
                "Name": "other",
                "Role": "arn:aws:iam::222222222222:role/glue",
                "Targets": {"S3Targets": [{"Path": "s3://x/y"}]}
            }),
            "222222222222",
        ))
        .unwrap();

        let crawlers = list_all_crawlers(&svc.state);
        assert_eq!(crawlers.len(), 2);
        assert_eq!(crawlers[0].account_id, "111111111111");
        assert_eq!(crawlers[0].name, "inventory");
        assert_eq!(crawlers[0].role, "arn:aws:iam::111111111111:role/glue");
        assert_eq!(crawlers[0].database_name.as_deref(), Some("analytics"));
        assert_eq!(crawlers[0].state, "READY");
        assert_eq!(crawlers[0].target_summary, "2 S3, 1 JDBC");
        assert!(crawlers[0].schedule.is_none());

        assert_eq!(crawlers[1].account_id, "222222222222");
        assert_eq!(crawlers[1].name, "other");
        assert_eq!(crawlers[1].database_name, None);
        assert_eq!(crawlers[1].target_summary, "1 S3");
    }
}
