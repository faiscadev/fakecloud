use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{AutomationExecution, ExecutionPreview};

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn start_automation_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ClientToken", body["ClientToken"].as_str(), 36, 36)?;
        validate_optional_string_length("MaxConcurrency", body["MaxConcurrency"].as_str(), 1, 7)?;
        validate_optional_string_length("MaxErrors", body["MaxErrors"].as_str(), 1, 7)?;
        validate_optional_enum("Mode", body["Mode"].as_str(), &["Auto", "Interactive"])?;
        validate_optional_string_length(
            "TargetParameterName",
            body["TargetParameterName"].as_str(),
            1,
            50,
        )?;
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();
        let document_version = body["DocumentVersion"].as_str().map(|s| s.to_string());
        let parameters: HashMap<String, Vec<String>> = body["Parameters"]
            .as_object()
            .map(|obj| {
                obj.iter()
                    .map(|(k, v)| {
                        let vals = v
                            .as_array()
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|i| i.as_str().map(|s| s.to_string()))
                                    .collect()
                            })
                            .unwrap_or_default();
                        (k.clone(), vals)
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mode = body["Mode"].as_str().unwrap_or("Auto").to_string();
        let target = body["TargetParameterName"].as_str().map(|s| s.to_string());
        let targets: Vec<serde_json::Value> =
            body["Targets"].as_array().cloned().unwrap_or_default();
        let max_concurrency = body["MaxConcurrency"].as_str().map(|s| s.to_string());
        let max_errors = body["MaxErrors"].as_str().map(|s| s.to_string());

        let now = Utc::now();
        let mut state = self.state.write();
        state.automation_execution_counter += 1;
        let exec_id = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            state.automation_execution_counter, 0, 0, 0, state.automation_execution_counter
        );
        let account_id = state.account_id.clone();

        let execution = AutomationExecution {
            automation_execution_id: exec_id.clone(),
            document_name,
            document_version,
            automation_execution_status: "InProgress".to_string(),
            execution_start_time: now,
            execution_end_time: None,
            parameters,
            outputs: HashMap::new(),
            mode,
            target,
            targets,
            max_concurrency,
            max_errors,
            executed_by: format!("arn:aws:iam::{account_id}:root"),
            step_executions: Vec::new(),
            automation_subtype: None,
            runbooks: Vec::new(),
            change_request_name: None,
            scheduled_time: None,
        };

        state
            .automation_executions
            .insert(exec_id.clone(), execution);

        Ok(json_resp(json!({ "AutomationExecutionId": exec_id })))
    }

    pub(super) fn stop_automation_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let exec_id = body["AutomationExecutionId"]
            .as_str()
            .ok_or_else(|| missing("AutomationExecutionId"))?;

        let mut state = self.state.write();
        let exec = state
            .automation_executions
            .get_mut(exec_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "AutomationExecutionNotFoundException",
                    format!("Automation execution {exec_id} not found"),
                )
            })?;

        exec.automation_execution_status = "Cancelled".to_string();
        exec.execution_end_time = Some(Utc::now());

        Ok(json_resp(json!({})))
    }

    pub(super) fn get_automation_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let exec_id = body["AutomationExecutionId"]
            .as_str()
            .ok_or_else(|| missing("AutomationExecutionId"))?;

        let state = self.state.read();
        let exec = state.automation_executions.get(exec_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AutomationExecutionNotFoundException",
                format!("Automation execution {exec_id} not found"),
            )
        })?;

        Ok(json_resp(
            json!({ "AutomationExecution": automation_execution_to_json(exec) }),
        ))
    }

    pub(super) fn describe_automation_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let state = self.state.read();
        let items: Vec<Value> = state
            .automation_executions
            .values()
            .map(|e| {
                json!({
                    "AutomationExecutionId": e.automation_execution_id,
                    "DocumentName": e.document_name,
                    "AutomationExecutionStatus": e.automation_execution_status,
                    "ExecutionStartTime": e.execution_start_time.timestamp_millis() as f64 / 1000.0,
                    "ExecutedBy": e.executed_by,
                    "Mode": e.mode,
                    "Targets": e.targets,
                })
            })
            .collect();

        Ok(json_resp(
            json!({ "AutomationExecutionMetadataList": items }),
        ))
    }

    pub(super) fn describe_automation_step_executions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let exec_id = body["AutomationExecutionId"]
            .as_str()
            .ok_or_else(|| missing("AutomationExecutionId"))?;

        let state = self.state.read();
        let exec = state.automation_executions.get(exec_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AutomationExecutionNotFoundException",
                format!("Automation execution {exec_id} not found"),
            )
        })?;

        let steps: Vec<Value> = exec
            .step_executions
            .iter()
            .map(|s| {
                json!({
                    "StepName": s.step_name,
                    "Action": s.action,
                    "StepStatus": s.step_status,
                    "StepExecutionId": s.step_execution_id,
                    "Inputs": s.inputs,
                    "Outputs": s.outputs,
                })
            })
            .collect();

        Ok(json_resp(json!({ "StepExecutions": steps })))
    }

    pub(super) fn send_automation_signal(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let exec_id = body["AutomationExecutionId"]
            .as_str()
            .ok_or_else(|| missing("AutomationExecutionId"))?;
        let _signal_type = body["SignalType"]
            .as_str()
            .ok_or_else(|| missing("SignalType"))?;

        let state = self.state.read();
        if !state.automation_executions.contains_key(exec_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AutomationExecutionNotFoundException",
                format!("Automation execution {exec_id} not found"),
            ));
        }

        Ok(json_resp(json!({})))
    }

    pub(super) fn start_change_request_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("ClientToken", body["ClientToken"].as_str(), 36, 36)?;
        validate_optional_string_length(
            "ChangeRequestName",
            body["ChangeRequestName"].as_str(),
            1,
            1024,
        )?;
        validate_optional_string_length("ChangeDetails", body["ChangeDetails"].as_str(), 1, 32768)?;
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();
        let _runbooks = body["Runbooks"]
            .as_array()
            .ok_or_else(|| missing("Runbooks"))?;
        let change_request_name = body["ChangeRequestName"].as_str().map(|s| s.to_string());
        let runbooks: Vec<serde_json::Value> =
            body["Runbooks"].as_array().cloned().unwrap_or_default();

        let now = Utc::now();
        let mut state = self.state.write();
        state.automation_execution_counter += 1;
        let exec_id = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            state.automation_execution_counter, 0, 0, 0, state.automation_execution_counter
        );
        let account_id = state.account_id.clone();

        let execution = AutomationExecution {
            automation_execution_id: exec_id.clone(),
            document_name,
            document_version: None,
            automation_execution_status: "Pending".to_string(),
            execution_start_time: now,
            execution_end_time: None,
            parameters: HashMap::new(),
            outputs: HashMap::new(),
            mode: "Auto".to_string(),
            target: None,
            targets: Vec::new(),
            max_concurrency: None,
            max_errors: None,
            executed_by: format!("arn:aws:iam::{account_id}:root"),
            step_executions: Vec::new(),
            automation_subtype: Some("ChangeRequest".to_string()),
            runbooks,
            change_request_name,
            scheduled_time: None,
        };

        state
            .automation_executions
            .insert(exec_id.clone(), execution);

        Ok(json_resp(json!({ "AutomationExecutionId": exec_id })))
    }

    pub(super) fn start_execution_preview(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let document_name = body["DocumentName"]
            .as_str()
            .ok_or_else(|| missing("DocumentName"))?
            .to_string();

        let now = Utc::now();
        let mut state = self.state.write();
        state.execution_preview_counter += 1;
        let preview_id = format!(
            "{:08x}-{:04x}-{:04x}-{:04x}-{:012x}",
            state.execution_preview_counter, 0, 0, 0, state.execution_preview_counter
        );

        let preview = ExecutionPreview {
            execution_preview_id: preview_id.clone(),
            document_name,
            status: "Success".to_string(),
            created_time: now,
        };
        state.execution_previews.insert(preview_id.clone(), preview);

        Ok(json_resp(json!({ "ExecutionPreviewId": preview_id })))
    }

    pub(super) fn get_execution_preview(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let preview_id = body["ExecutionPreviewId"]
            .as_str()
            .ok_or_else(|| missing("ExecutionPreviewId"))?;

        let state = self.state.read();
        let preview = state.execution_previews.get(preview_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Execution preview {preview_id} not found"),
            )
        })?;

        Ok(json_resp(json!({
            "ExecutionPreviewId": preview.execution_preview_id,
            "Status": preview.status,
            "EndedAt": preview.created_time.timestamp_millis() as f64 / 1000.0,
        })))
    }

    // ── Sessions ──────────────────────────────────────────────────
}

pub(super) fn automation_execution_to_json(e: &AutomationExecution) -> Value {
    let mut v = json!({
        "AutomationExecutionId": e.automation_execution_id,
        "DocumentName": e.document_name,
        "AutomationExecutionStatus": e.automation_execution_status,
        "ExecutionStartTime": e.execution_start_time.timestamp_millis() as f64 / 1000.0,
        "ExecutedBy": e.executed_by,
        "Mode": e.mode,
        "Parameters": e.parameters,
        "Outputs": e.outputs,
        "Targets": e.targets,
        "StepExecutions": e.step_executions.iter().map(|s| json!({
            "StepName": s.step_name,
            "Action": s.action,
            "StepStatus": s.step_status,
            "StepExecutionId": s.step_execution_id,
            "Inputs": s.inputs,
            "Outputs": s.outputs,
        })).collect::<Vec<Value>>(),
    });
    if let Some(ref dv) = e.document_version {
        v["DocumentVersion"] = json!(dv);
    }
    if let Some(ref end) = e.execution_end_time {
        v["ExecutionEndTime"] = json!(end.timestamp_millis() as f64 / 1000.0);
    }
    if let Some(ref target) = e.target {
        v["TargetParameterName"] = json!(target);
    }
    if let Some(ref mc) = e.max_concurrency {
        v["MaxConcurrency"] = json!(mc);
    }
    if let Some(ref me) = e.max_errors {
        v["MaxErrors"] = json!(me);
    }
    if let Some(ref subtype) = e.automation_subtype {
        v["AutomationSubtype"] = json!(subtype);
    }
    if !e.runbooks.is_empty() {
        v["Runbooks"] = json!(e.runbooks);
    }
    if let Some(ref crn) = e.change_request_name {
        v["ChangeRequestName"] = json!(crn);
    }
    v
}
