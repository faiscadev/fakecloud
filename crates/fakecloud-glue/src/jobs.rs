//! Glue Jobs control plane (Create/Get/Update/Delete/List + JobRun).

use std::collections::BTreeMap;

use chrono::Utc;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::GlueService;
use crate::state::{Job, JobRun};

fn missing(field: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "InvalidInputException",
        format!("Missing required field: {field}"),
    )
}

fn entity_not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "EntityNotFoundException",
        msg.into(),
    )
}

fn already_exists(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "AlreadyExistsException",
        msg.into(),
    )
}

fn parse_string_map(val: &Value) -> BTreeMap<String, String> {
    let mut m = BTreeMap::new();
    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                m.insert(k.clone(), s.to_string());
            }
        }
    }
    m
}

pub(crate) fn job_to_json(j: &Job) -> Value {
    json!({
        "Name": j.name,
        "Description": j.description,
        "Role": j.role,
        "CreatedOn": j.created_on.timestamp(),
        "LastModifiedOn": j.last_modified_on.timestamp(),
        "Command": j.command,
        "DefaultArguments": j.default_arguments,
        "MaxRetries": j.max_retries,
        "Timeout": j.timeout,
        "GlueVersion": j.glue_version,
        "MaxCapacity": j.max_capacity,
        "WorkerType": j.worker_type,
        "NumberOfWorkers": j.number_of_workers,
        "ExecutionClass": j.execution_class,
        "Connections": j.connections,
        "ExecutionProperty": j.execution_property,
        "SecurityConfiguration": j.security_configuration,
        "NotificationProperty": j.notification_property,
        "NonOverridableArguments": j.non_overridable_arguments,
        "SourceControlDetails": j.source_control_details,
        "CodeGenConfigurationNodes": j.code_gen_configuration_nodes,
        "MaintenanceWindow": j.maintenance_window,
        "LogUri": j.log_uri,
        "AllocatedCapacity": j.allocated_capacity,
        "JobMode": j.job_mode,
        "JobRunQueuingEnabled": j.job_run_queuing_enabled,
    })
}

/// Clone a JSON value into `Some` unless it's null/absent.
fn opt_value(v: &Value) -> Option<Value> {
    if v.is_null() {
        None
    } else {
        Some(v.clone())
    }
}

/// A job run as `GetJobRun` reports it, with the settings it started with. An
/// absent field is left out, as AWS leaves it out.
fn job_run_to_json(r: &JobRun) -> Value {
    let settings = &r.settings;
    let mut run = json!({
        "Id": r.id,
        "JobName": r.job_name,
        "StartedOn": r.started_on.timestamp(),
        "LastModifiedOn": r.completed_on.unwrap_or(r.started_on).timestamp(),
        "JobRunState": r.state,
        "ExecutionTime": r.execution_time,
        "Attempt": r.attempt,
        "PredecessorRuns": [],
    });
    let optional = [
        ("CompletedOn", r.completed_on.map(|d| json!(d.timestamp()))),
        (
            "Arguments",
            (!r.arguments.is_empty()).then(|| json!(r.arguments)),
        ),
        ("ErrorMessage", r.error_message.as_ref().map(|m| json!(m))),
        ("Timeout", settings.timeout.map(|t| json!(t))),
        ("MaxCapacity", settings.max_capacity.map(|c| json!(c))),
        (
            "AllocatedCapacity",
            settings.allocated_capacity.map(|c| json!(c)),
        ),
        (
            "GlueVersion",
            settings.glue_version.as_ref().map(|v| json!(v)),
        ),
        (
            "WorkerType",
            settings.worker_type.as_ref().map(|v| json!(v)),
        ),
        (
            "NumberOfWorkers",
            settings.number_of_workers.map(|n| json!(n)),
        ),
        (
            "ExecutionClass",
            settings.execution_class.as_ref().map(|v| json!(v)),
        ),
        ("JobMode", settings.job_mode.as_ref().map(|v| json!(v))),
        (
            "LogGroupName",
            settings.log_group_name.as_ref().map(|v| json!(v)),
        ),
    ];
    for (field, value) in optional {
        if let Some(value) = value {
            run[field] = value;
        }
    }
    run
}

/// The Glue version a job gets when it names none, as recorded.
const DEFAULT_GLUE_VERSION: &str = "6.0";

/// A job's default timeout in minutes, as recorded: 480 from Glue 5.0 on and 2880
/// before it; a streaming job has none.
fn default_timeout(command: &Value, glue_version: &str) -> Option<i64> {
    if command["Name"].as_str() == Some("gluestreaming") {
        return None;
    }
    let major = glue_version
        .split('.')
        .next()
        .and_then(|major| major.parse::<u32>().ok())
        .unwrap_or(0);
    Some(if major >= 5 { 480 } else { 2880 })
}

/// The DPUs one worker of a type provides.
fn worker_dpus(worker_type: &str) -> Option<f64> {
    Some(match worker_type {
        "G.025X" => 0.25,
        "G.1X" | "Standard" => 1.0,
        "G.2X" | "Z.2X" => 2.0,
        "G.4X" => 4.0,
        "G.8X" => 8.0,
        "G.12X" => 12.0,
        "G.16X" => 16.0,
        "R.1X" => 1.0,
        "R.2X" => 2.0,
        "R.4X" => 4.0,
        "R.8X" => 8.0,
        _ => return None,
    })
}

/// A job's capacity in DPUs, as recorded: its workers' when it names them, else
/// 0.0625 for a Python shell job and 10 for any other.
fn default_max_capacity(
    command: &Value,
    worker_type: Option<&str>,
    number_of_workers: Option<i64>,
) -> f64 {
    if let (Some(dpus), Some(workers)) = (worker_type.and_then(worker_dpus), number_of_workers) {
        return dpus * workers as f64;
    }
    if command["Name"].as_str() == Some("pythonshell") {
        0.0625
    } else {
        10.0
    }
}

/// `AllocatedCapacity`, the deprecated whole-DPU count: the capacity's whole
/// DPUs, as recorded (0.0625 is 0, 10 is 10).
fn allocated_capacity(max_capacity: f64) -> i64 {
    max_capacity.floor() as i64
}

/// The settings a run of `job` starts with, the run's own overrides applied.
fn run_settings(job: &Job, body: &Value) -> crate::state::JobRunSettings {
    let worker_type = body["WorkerType"]
        .as_str()
        .map(String::from)
        .or_else(|| job.worker_type.clone());
    let number_of_workers = body["NumberOfWorkers"].as_i64().or(job.number_of_workers);
    let workers_overridden = body["WorkerType"].is_string() || body["NumberOfWorkers"].is_i64();
    let max_capacity = body["MaxCapacity"]
        .as_f64()
        .or_else(|| {
            workers_overridden.then(|| {
                default_max_capacity(&job.command, worker_type.as_deref(), number_of_workers)
            })
        })
        .or(job.max_capacity);
    // A run's own capacity (or workers) first, then the job's stored count,
    // then the whole DPUs of the capacity.
    let capacity_overridden = workers_overridden || body["MaxCapacity"].is_number();
    let allocated = body["AllocatedCapacity"]
        .as_i64()
        .or_else(|| {
            capacity_overridden
                .then(|| max_capacity.map(allocated_capacity))
                .flatten()
        })
        .or(job.allocated_capacity)
        .or_else(|| max_capacity.map(allocated_capacity));
    crate::state::JobRunSettings {
        timeout: body["Timeout"].as_i64().or(job.timeout),
        max_capacity,
        allocated_capacity: allocated,
        glue_version: job.glue_version.clone(),
        worker_type,
        number_of_workers,
        execution_class: body["ExecutionClass"]
            .as_str()
            .map(String::from)
            .or_else(|| job.execution_class.clone()),
        job_mode: job.job_mode.clone(),
        log_group_name: Some(
            if job.command["Name"] == "pythonshell" {
                "/aws-glue/python-jobs"
            } else {
                "/aws-glue/jobs"
            }
            .to_string(),
        ),
    }
}

impl GlueService {
    pub(crate) fn create_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let role = body["Role"].as_str().ok_or_else(|| missing("Role"))?;
        let command = body["Command"].clone();
        if command.is_null() {
            return Err(missing("Command"));
        }
        // Unset settings take AWS's defaults, as recorded: the current Glue
        // version, its timeout, and the capacity of the job's command or workers.
        let glue_version = body["GlueVersion"]
            .as_str()
            .unwrap_or(DEFAULT_GLUE_VERSION)
            .to_string();
        let timeout = body["Timeout"]
            .as_i64()
            .or_else(|| default_timeout(&command, &glue_version));
        let worker_type = body["WorkerType"].as_str().map(String::from);
        let number_of_workers = body["NumberOfWorkers"].as_i64();
        let max_capacity = body["MaxCapacity"].as_f64().unwrap_or_else(|| {
            default_max_capacity(&command, worker_type.as_deref(), number_of_workers)
        });
        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        if state.jobs.contains_key(name) {
            return Err(already_exists(format!("Job {name} already exists")));
        }
        let job = Job {
            name: name.to_string(),
            description: body["Description"].as_str().map(String::from),
            role: role.to_string(),
            created_on: now,
            last_modified_on: now,
            command,
            default_arguments: parse_string_map(&body["DefaultArguments"]),
            max_retries: body["MaxRetries"].as_i64().unwrap_or(0),
            timeout,
            glue_version: Some(glue_version),
            max_capacity: Some(max_capacity),
            worker_type,
            number_of_workers,
            execution_class: body["ExecutionClass"].as_str().map(String::from),
            connections: opt_value(&body["Connections"]),
            execution_property: opt_value(&body["ExecutionProperty"]),
            security_configuration: body["SecurityConfiguration"].as_str().map(String::from),
            notification_property: opt_value(&body["NotificationProperty"]),
            non_overridable_arguments: parse_string_map(&body["NonOverridableArguments"]),
            source_control_details: opt_value(&body["SourceControlDetails"]),
            code_gen_configuration_nodes: opt_value(&body["CodeGenConfigurationNodes"]),
            maintenance_window: body["MaintenanceWindow"].as_str().map(String::from),
            log_uri: body["LogUri"].as_str().map(String::from),
            allocated_capacity: Some(
                body["AllocatedCapacity"]
                    .as_i64()
                    .unwrap_or_else(|| allocated_capacity(max_capacity)),
            ),
            job_mode: Some(body["JobMode"].as_str().unwrap_or("SCRIPT").to_string()),
            job_run_queuing_enabled: body["JobRunQueuingEnabled"].as_bool(),
        };
        state.jobs.insert(name.to_string(), job);
        let arn = crate::common::resource_arn(&req.region, &req.account_id, "job", name);
        state.put_create_tags(&arn, &body);
        Ok(AwsResponse::ok_json(json!({ "Name": name })))
    }

    pub(crate) fn get_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["JobName"].as_str().ok_or_else(|| missing("JobName"))?;
        let accounts = self.state.read();
        let job = accounts
            .get(&req.account_id)
            .and_then(|s| s.jobs.get(name))
            .ok_or_else(|| entity_not_found(format!("Job {name} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "Job": job_to_json(job) })))
    }

    pub(crate) fn get_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let accounts = self.state.read();
        let jobs: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| s.jobs.values().map(job_to_json).collect())
            .unwrap_or_default();
        let (page, token) = crate::common::paginate_body(&req.action, &body, jobs)?;
        let mut resp = json!({ "Jobs": page });
        if let Some(t) = token {
            resp["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(crate) fn list_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let accounts = self.state.read();
        let names: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                s.jobs
                    .keys()
                    .filter(|k| {
                        s.matches_tag_filter(
                            &crate::common::resource_arn(&req.region, &req.account_id, "job", k),
                            &body,
                        )
                    })
                    .map(|k| json!(k))
                    .collect()
            })
            .unwrap_or_default();
        let (page, token) = crate::common::paginate_body(&req.action, &body, names)?;
        let mut resp = json!({ "JobNames": page });
        if let Some(t) = token {
            resp["NextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(crate) fn update_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["JobName"].as_str().ok_or_else(|| missing("JobName"))?;
        let update = &body["JobUpdate"];
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let job = state
            .jobs
            .get_mut(name)
            .ok_or_else(|| entity_not_found(format!("Job {name} not found")))?;
        if let Some(s) = update["Role"].as_str() {
            job.role = s.to_string();
        }
        if let Some(s) = update["Description"].as_str() {
            job.description = Some(s.to_string());
        }
        if !update["Command"].is_null() {
            job.command = update["Command"].clone();
        }
        if update["DefaultArguments"].is_object() {
            job.default_arguments = parse_string_map(&update["DefaultArguments"]);
        }
        if let Some(n) = update["MaxRetries"].as_i64() {
            job.max_retries = n;
        }
        if let Some(n) = update["Timeout"].as_i64() {
            job.timeout = Some(n);
        }
        if let Some(s) = update["GlueVersion"].as_str() {
            job.glue_version = Some(s.to_string());
        }
        if let Some(n) = update["MaxCapacity"].as_f64() {
            job.max_capacity = Some(n);
        }
        if let Some(s) = update["WorkerType"].as_str() {
            job.worker_type = Some(s.to_string());
        }
        if let Some(n) = update["NumberOfWorkers"].as_i64() {
            job.number_of_workers = Some(n);
        }
        if let Some(s) = update["ExecutionClass"].as_str() {
            job.execution_class = Some(s.to_string());
        }
        if !update["Connections"].is_null() {
            job.connections = opt_value(&update["Connections"]);
        }
        if !update["ExecutionProperty"].is_null() {
            job.execution_property = opt_value(&update["ExecutionProperty"]);
        }
        if let Some(s) = update["SecurityConfiguration"].as_str() {
            job.security_configuration = Some(s.to_string());
        }
        if !update["NotificationProperty"].is_null() {
            job.notification_property = opt_value(&update["NotificationProperty"]);
        }
        if update["NonOverridableArguments"].is_object() {
            job.non_overridable_arguments = parse_string_map(&update["NonOverridableArguments"]);
        }
        if !update["SourceControlDetails"].is_null() {
            job.source_control_details = opt_value(&update["SourceControlDetails"]);
        }
        if !update["CodeGenConfigurationNodes"].is_null() {
            job.code_gen_configuration_nodes = opt_value(&update["CodeGenConfigurationNodes"]);
        }
        if let Some(s) = update["MaintenanceWindow"].as_str() {
            job.maintenance_window = Some(s.to_string());
        }
        if let Some(s) = update["LogUri"].as_str() {
            job.log_uri = Some(s.to_string());
        }
        if let Some(n) = update["AllocatedCapacity"].as_i64() {
            job.allocated_capacity = Some(n);
        }
        if let Some(s) = update["JobMode"].as_str() {
            job.job_mode = Some(s.to_string());
        }
        if let Some(b) = update["JobRunQueuingEnabled"].as_bool() {
            job.job_run_queuing_enabled = Some(b);
        }
        job.last_modified_on = Utc::now();
        Ok(AwsResponse::ok_json(json!({ "JobName": name })))
    }

    pub(crate) fn delete_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["JobName"].as_str().ok_or_else(|| missing("JobName"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        // DeleteJob does not declare EntityNotFoundException; idempotent delete.
        if state.jobs.remove(name).is_some() {
            let arn = crate::common::resource_arn(&req.region, &req.account_id, "job", name);
            state.remove_tags(&arn);
        }
        Ok(AwsResponse::ok_json(json!({ "JobName": name })))
    }

    pub(crate) fn start_job_run(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = body["JobName"].as_str().ok_or_else(|| missing("JobName"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let Some(job) = state.jobs.get(name) else {
            return Err(entity_not_found(format!("Job {name} not found")));
        };
        let settings = run_settings(job, &body);
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let run = JobRun {
            id: id.clone(),
            job_name: name.to_string(),
            started_on: now,
            completed_on: Some(now),
            state: "SUCCEEDED".to_string(),
            arguments: parse_string_map(&body["Arguments"]),
            error_message: None,
            execution_time: 0,
            // The first attempt is number 0, as recorded.
            attempt: 0,
            settings,
        };
        state.job_runs.insert(id.clone(), run);
        Ok(AwsResponse::ok_json(json!({ "JobRunId": id })))
    }

    pub(crate) fn get_job_run(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job_name = body["JobName"].as_str().ok_or_else(|| missing("JobName"))?;
        let run_id = body["RunId"].as_str().ok_or_else(|| missing("RunId"))?;
        let accounts = self.state.read();
        let run = accounts
            .get(&req.account_id)
            .and_then(|s| s.job_runs.get(run_id))
            .filter(|r| r.job_name == job_name)
            .ok_or_else(|| entity_not_found(format!("JobRun {run_id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "JobRun": job_run_to_json(run) }),
        ))
    }

    /// Return the download URL for a job run's system logs. The run must exist
    /// under the named job.
    pub(crate) fn get_system_logs_for_job_run(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job_name = body["JobName"].as_str().ok_or_else(|| missing("JobName"))?;
        let run_id = body["RunId"].as_str().ok_or_else(|| missing("RunId"))?;
        let accounts = self.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.job_runs.get(run_id))
            .filter(|r| r.job_name == job_name)
            .ok_or_else(|| entity_not_found(format!("JobRun {run_id} not found")))?;
        // Nothing here executes the run, so no system-log archive is ever
        // written: SystemLogsUrl is optional and stays absent.
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_job_runs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job_name = body["JobName"].as_str().ok_or_else(|| missing("JobName"))?;
        let accounts = self.state.read();
        let runs: Vec<Value> = accounts
            .get(&req.account_id)
            .map(|s| {
                // AWS returns job runs most-recent-first; clients read
                // JobRuns[0] as the latest run. The state map is UUID-keyed
                // (random order), so sort by StartedOn descending.
                let mut matching: Vec<_> = s
                    .job_runs
                    .values()
                    .filter(|r| r.job_name == job_name)
                    .collect();
                matching.sort_by_key(|r| std::cmp::Reverse(r.started_on));
                matching.into_iter().map(job_run_to_json).collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "JobRuns": runs })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::GlueService;
    use fakecloud_core::service::AwsRequest;
    use serde_json::json;

    fn req(action: &str, body: Value) -> AwsRequest {
        use bytes::Bytes;
        use http::{HeaderMap, Method};
        use std::collections::HashMap;
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

    #[test]
    fn job_lifecycle() {
        let svc = GlueService::default();
        svc.create_job(&req(
            "CreateJob",
            json!({
                "Name": "etl",
                "Role": "arn:aws:iam::123456789012:role/glue",
                "Command": {"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
                "DefaultArguments": {"--enable-metrics": "true"}
            }),
        ))
        .unwrap();

        let resp = svc
            .get_job(&req("GetJob", json!({"JobName": "etl"})))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["Job"]["Name"], "etl");

        let resp = svc.list_jobs(&req("ListJobs", json!({}))).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["JobNames"][0], "etl");

        let resp = svc
            .start_job_run(&req("StartJobRun", json!({"JobName": "etl"})))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let run_id = body["JobRunId"].as_str().unwrap().to_string();

        let resp = svc
            .get_job_run(&req(
                "GetJobRun",
                json!({"JobName": "etl", "RunId": &run_id}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["JobRun"]["JobRunState"], "SUCCEEDED");

        svc.delete_job(&req("DeleteJob", json!({"JobName": "etl"})))
            .unwrap();
        assert!(svc
            .get_job(&req("GetJob", json!({"JobName": "etl"})))
            .is_err());
    }

    #[test]
    fn create_and_update_job_round_trip_extended_fields() {
        // Connections / ExecutionProperty / SecurityConfiguration /
        // NotificationProperty / NonOverridableArguments were dropped
        // (bug-audit 2026-06-20, 1.24).
        let svc = GlueService::default();
        svc.create_job(&req(
            "CreateJob",
            json!({
                "Name": "etl",
                "Role": "arn:aws:iam::123456789012:role/glue",
                "Command": {"Name": "glueetl"},
                "Connections": {"Connections": ["conn-a"]},
                "ExecutionProperty": {"MaxConcurrentRuns": 3},
                "SecurityConfiguration": "sec-cfg",
                "NotificationProperty": {"NotifyDelayAfter": 10},
                "NonOverridableArguments": {"--locked": "1"}
            }),
        ))
        .unwrap();

        let got = |svc: &GlueService| -> Value {
            let resp = svc
                .get_job(&req("GetJob", json!({"JobName": "etl"})))
                .unwrap();
            serde_json::from_slice::<Value>(resp.body.expect_bytes()).unwrap()["Job"].clone()
        };
        let j = got(&svc);
        assert_eq!(j["Connections"]["Connections"][0], "conn-a", "{j}");
        assert_eq!(j["ExecutionProperty"]["MaxConcurrentRuns"], 3);
        assert_eq!(j["SecurityConfiguration"], "sec-cfg");
        assert_eq!(j["NotificationProperty"]["NotifyDelayAfter"], 10);
        assert_eq!(j["NonOverridableArguments"]["--locked"], "1");

        // UpdateJob applies the same fields.
        svc.update_job(&req(
            "UpdateJob",
            json!({
                "JobName": "etl",
                "JobUpdate": {
                    "SecurityConfiguration": "sec-cfg-2",
                    "ExecutionProperty": {"MaxConcurrentRuns": 7}
                }
            }),
        ))
        .unwrap();
        let j = got(&svc);
        assert_eq!(j["SecurityConfiguration"], "sec-cfg-2");
        assert_eq!(j["ExecutionProperty"]["MaxConcurrentRuns"], 7);
    }

    #[test]
    fn create_job_duplicate_errors() {
        let svc = GlueService::default();
        let body = json!({
            "Name": "j",
            "Role": "r",
            "Command": {"Name": "glueetl"}
        });
        svc.create_job(&req("CreateJob", body.clone())).unwrap();
        assert!(svc.create_job(&req("CreateJob", body)).is_err());
    }
}
