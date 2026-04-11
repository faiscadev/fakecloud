use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use std::collections::HashMap;

use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use crate::interpreter;
use crate::state::{
    Execution, ExecutionStatus, SharedStepFunctionsState, StateMachine, StateMachineStatus,
    StateMachineType,
};

const SUPPORTED: &[&str] = &[
    "CreateStateMachine",
    "DescribeStateMachine",
    "ListStateMachines",
    "DeleteStateMachine",
    "UpdateStateMachine",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
    "StartExecution",
    "StopExecution",
    "DescribeExecution",
    "ListExecutions",
    "GetExecutionHistory",
    "DescribeStateMachineForExecution",
];

pub struct StepFunctionsService {
    state: SharedStepFunctionsState,
}

impl StepFunctionsService {
    pub fn new(state: SharedStepFunctionsState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for StepFunctionsService {
    fn service_name(&self) -> &str {
        "states"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateStateMachine" => self.create_state_machine(&req),
            "DescribeStateMachine" => self.describe_state_machine(&req),
            "ListStateMachines" => self.list_state_machines(&req),
            "DeleteStateMachine" => self.delete_state_machine(&req),
            "UpdateStateMachine" => self.update_state_machine(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "StartExecution" => self.start_execution(&req),
            "StopExecution" => self.stop_execution(&req),
            "DescribeExecution" => self.describe_execution(&req),
            "ListExecutions" => self.list_executions(&req),
            "GetExecutionHistory" => self.get_execution_history(&req),
            "DescribeStateMachineForExecution" => self.describe_state_machine_for_execution(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "states",
                &req.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED
    }
}

impl StepFunctionsService {
    // ─── State Machine CRUD ─────────────────────────────────────────────

    fn create_state_machine(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        validate_required("name", &body["name"])?;
        let name = body["name"].as_str().ok_or_else(|| missing("name"))?;
        validate_name(name)?;

        validate_required("definition", &body["definition"])?;
        let definition = body["definition"]
            .as_str()
            .ok_or_else(|| missing("definition"))?;
        validate_definition(definition)?;

        validate_required("roleArn", &body["roleArn"])?;
        let role_arn = body["roleArn"].as_str().ok_or_else(|| missing("roleArn"))?;
        validate_arn(role_arn)?;

        let machine_type = if let Some(t) = body["type"].as_str() {
            StateMachineType::parse(t).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "Value '{t}' at 'type' failed to satisfy constraint: \
                         Member must satisfy enum value set: [STANDARD, EXPRESS]"
                    ),
                )
            })?
        } else {
            StateMachineType::Standard
        };

        let mut state = self.state.write();
        let arn = state.state_machine_arn(name);

        // Check if name already exists
        if state.state_machines.values().any(|sm| sm.name == name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "StateMachineAlreadyExists",
                format!("State Machine Already Exists: '{arn}'"),
            ));
        }

        let now = Utc::now();
        let revision_id = uuid::Uuid::new_v4().to_string();

        let mut tags = HashMap::new();
        if !body["tags"].is_null() {
            fakecloud_core::tags::apply_tags(&mut tags, &body, "tags", "key", "value").map_err(
                |f| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!("{f} must be a list"),
                    )
                },
            )?;
        }

        let sm = StateMachine {
            name: name.to_string(),
            arn: arn.clone(),
            definition: definition.to_string(),
            role_arn: role_arn.to_string(),
            machine_type,
            status: StateMachineStatus::Active,
            creation_date: now,
            update_date: now,
            tags,
            revision_id: revision_id.clone(),
            logging_configuration: body.get("loggingConfiguration").cloned(),
            tracing_configuration: body.get("tracingConfiguration").cloned(),
            description: body["description"].as_str().unwrap_or("").to_string(),
        };

        state.state_machines.insert(arn.clone(), sm);

        Ok(AwsResponse::ok_json(json!({
            "stateMachineArn": arn,
            "creationDate": now.timestamp() as f64,
            "stateMachineVersionArn": arn,
        })))
    }

    fn describe_state_machine(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(arn)?;

        let state = self.state.read();
        let sm = state
            .state_machines
            .get(arn)
            .ok_or_else(|| state_machine_not_found(arn))?;

        Ok(AwsResponse::ok_json(state_machine_to_json(sm)))
    }

    fn list_state_machines(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let max_results = body["maxResults"].as_i64().unwrap_or(100) as usize;
        validate_range_i64("maxResults", max_results as i64, 1, 1000)?;
        let next_token = body["nextToken"].as_str();

        let state = self.state.read();
        let mut machines: Vec<&StateMachine> = state.state_machines.values().collect();
        machines.sort_by(|a, b| a.name.cmp(&b.name));

        let items: Vec<Value> = machines
            .iter()
            .map(|sm| {
                json!({
                    "name": sm.name,
                    "stateMachineArn": sm.arn,
                    "type": sm.machine_type.as_str(),
                    "creationDate": sm.creation_date.timestamp() as f64,
                })
            })
            .collect();

        let (page, token) = paginate(&items, next_token, max_results);

        let mut resp = json!({ "stateMachines": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn delete_state_machine(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(arn)?;

        let mut state = self.state.write();
        // AWS returns success even if it doesn't exist
        state.state_machines.remove(arn);

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn update_state_machine(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(arn)?;

        let mut state = self.state.write();
        let sm = state
            .state_machines
            .get_mut(arn)
            .ok_or_else(|| state_machine_not_found(arn))?;

        if let Some(definition) = body["definition"].as_str() {
            validate_definition(definition)?;
            sm.definition = definition.to_string();
        }

        if let Some(role_arn) = body["roleArn"].as_str() {
            validate_arn(role_arn)?;
            sm.role_arn = role_arn.to_string();
        }

        if let Some(logging) = body.get("loggingConfiguration") {
            sm.logging_configuration = Some(logging.clone());
        }

        if let Some(tracing) = body.get("tracingConfiguration") {
            sm.tracing_configuration = Some(tracing.clone());
        }

        if let Some(description) = body["description"].as_str() {
            sm.description = description.to_string();
        }

        let now = Utc::now();
        sm.update_date = now;
        sm.revision_id = uuid::Uuid::new_v4().to_string();

        let revision_id = sm.revision_id.clone();
        let sm_arn = sm.arn.clone();

        Ok(AwsResponse::ok_json(json!({
            "updateDate": now.timestamp() as f64,
            "revisionId": revision_id,
            "stateMachineVersionArn": sm_arn,
        })))
    }

    // ─── Execution Lifecycle ──────────────────────────────────────────

    fn start_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let sm_arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(sm_arn)?;

        let input = body["input"].as_str().map(|s| s.to_string());

        // Validate input is valid JSON if provided
        if let Some(ref input_str) = input {
            let _: serde_json::Value = serde_json::from_str(input_str).map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidExecutionInput",
                    "Invalid execution input: must be valid JSON".to_string(),
                )
            })?;
        }

        let execution_name = body["name"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

        if let Some(name) = body["name"].as_str() {
            validate_name(name)?;
        }

        let mut state = self.state.write();
        let sm = state
            .state_machines
            .get(sm_arn)
            .ok_or_else(|| state_machine_not_found(sm_arn))?;

        let sm_name = sm.name.clone();
        let definition = sm.definition.clone();
        let exec_arn = state.execution_arn(&sm_name, &execution_name);

        // Check for duplicate execution name
        if state.executions.contains_key(&exec_arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ExecutionAlreadyExists",
                format!("Execution Already Exists: '{exec_arn}'"),
            ));
        }

        let now = Utc::now();
        let execution = Execution {
            execution_arn: exec_arn.clone(),
            state_machine_arn: sm_arn.to_string(),
            state_machine_name: sm_name,
            name: execution_name,
            status: ExecutionStatus::Running,
            input: input.clone(),
            output: None,
            start_date: now,
            stop_date: None,
            error: None,
            cause: None,
            history_events: vec![],
        };

        state.executions.insert(exec_arn.clone(), execution);
        drop(state);

        // Spawn async execution
        let shared_state = self.state.clone();
        let exec_arn_clone = exec_arn.clone();
        let input_clone = input;
        tokio::spawn(async move {
            interpreter::execute_state_machine(
                shared_state,
                exec_arn_clone,
                definition,
                input_clone,
            )
            .await;
        });

        Ok(AwsResponse::ok_json(json!({
            "executionArn": exec_arn,
            "startDate": now.timestamp() as f64,
        })))
    }

    fn stop_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;

        let error = body["error"].as_str().map(|s| s.to_string());
        let cause = body["cause"].as_str().map(|s| s.to_string());

        let mut state = self.state.write();
        let exec = state
            .executions
            .get_mut(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        if exec.status != ExecutionStatus::Running {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ExecutionNotRunning",
                format!("Execution is not running: '{exec_arn}'"),
            ));
        }

        let now = Utc::now();
        exec.status = ExecutionStatus::Aborted;
        exec.stop_date = Some(now);
        exec.error = error;
        exec.cause = cause;

        Ok(AwsResponse::ok_json(json!({
            "stopDate": now.timestamp() as f64,
        })))
    }

    fn describe_execution(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;

        let state = self.state.read();
        let exec = state
            .executions
            .get(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        Ok(AwsResponse::ok_json(execution_to_json(exec)))
    }

    fn list_executions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("stateMachineArn", &body["stateMachineArn"])?;
        let sm_arn = body["stateMachineArn"]
            .as_str()
            .ok_or_else(|| missing("stateMachineArn"))?;
        validate_arn(sm_arn)?;

        let max_results = body["maxResults"].as_i64().unwrap_or(100) as usize;
        validate_range_i64("maxResults", max_results as i64, 1, 1000)?;
        let next_token = body["nextToken"].as_str();
        let status_filter = body["statusFilter"].as_str();

        let state = self.state.read();

        // Verify state machine exists
        if !state.state_machines.contains_key(sm_arn) {
            return Err(state_machine_not_found(sm_arn));
        }

        let mut executions: Vec<&Execution> = state
            .executions
            .values()
            .filter(|e| e.state_machine_arn == sm_arn)
            .filter(|e| {
                status_filter
                    .map(|sf| e.status.as_str() == sf)
                    .unwrap_or(true)
            })
            .collect();

        // Sort by start date descending (most recent first)
        executions.sort_by(|a, b| b.start_date.cmp(&a.start_date));

        let items: Vec<Value> = executions
            .iter()
            .map(|e| {
                let mut item = json!({
                    "executionArn": e.execution_arn,
                    "stateMachineArn": e.state_machine_arn,
                    "name": e.name,
                    "status": e.status.as_str(),
                    "startDate": e.start_date.timestamp() as f64,
                });
                if let Some(stop) = e.stop_date {
                    item["stopDate"] = json!(stop.timestamp() as f64);
                }
                item
            })
            .collect();

        let (page, token) = paginate(&items, next_token, max_results);

        let mut resp = json!({ "executions": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn get_execution_history(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;

        let max_results = body["maxResults"].as_i64().unwrap_or(100) as usize;
        validate_range_i64("maxResults", max_results as i64, 1, 1000)?;
        let next_token = body["nextToken"].as_str();
        let reverse_order = body["reverseOrder"].as_bool().unwrap_or(false);

        let state = self.state.read();
        let exec = state
            .executions
            .get(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        let mut events: Vec<Value> = exec
            .history_events
            .iter()
            .map(|e| {
                json!({
                    "id": e.id,
                    "type": e.event_type,
                    "timestamp": e.timestamp.timestamp() as f64,
                    "previousEventId": e.previous_event_id,
                    format!("{}EventDetails", camel_to_details_key(&e.event_type)): e.details,
                })
            })
            .collect();

        if reverse_order {
            events.reverse();
        }

        let (page, token) = paginate(&events, next_token, max_results);

        let mut resp = json!({ "events": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }

    fn describe_state_machine_for_execution(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("executionArn", &body["executionArn"])?;
        let exec_arn = body["executionArn"]
            .as_str()
            .ok_or_else(|| missing("executionArn"))?;

        let state = self.state.read();
        let exec = state
            .executions
            .get(exec_arn)
            .ok_or_else(|| execution_not_found(exec_arn))?;

        let sm = state
            .state_machines
            .get(&exec.state_machine_arn)
            .ok_or_else(|| state_machine_not_found(&exec.state_machine_arn))?;

        Ok(AwsResponse::ok_json(state_machine_to_json(sm)))
    }

    // ─── Tagging ────────────────────────────────────────────────────────

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("resourceArn", &body["resourceArn"])?;
        let arn = body["resourceArn"]
            .as_str()
            .ok_or_else(|| missing("resourceArn"))?;
        validate_arn(arn)?;
        validate_required("tags", &body["tags"])?;

        let mut state = self.state.write();
        let sm = state
            .state_machines
            .get_mut(arn)
            .ok_or_else(|| resource_not_found(arn))?;

        fakecloud_core::tags::apply_tags(&mut sm.tags, &body, "tags", "key", "value").map_err(
            |f| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("{f} must be a list"),
                )
            },
        )?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("resourceArn", &body["resourceArn"])?;
        let arn = body["resourceArn"]
            .as_str()
            .ok_or_else(|| missing("resourceArn"))?;
        validate_arn(arn)?;
        validate_required("tagKeys", &body["tagKeys"])?;

        let mut state = self.state.write();
        let sm = state
            .state_machines
            .get_mut(arn)
            .ok_or_else(|| resource_not_found(arn))?;

        fakecloud_core::tags::remove_tags(&mut sm.tags, &body, "tagKeys").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("resourceArn", &body["resourceArn"])?;
        let arn = body["resourceArn"]
            .as_str()
            .ok_or_else(|| missing("resourceArn"))?;
        validate_arn(arn)?;

        let state = self.state.read();
        let sm = state
            .state_machines
            .get(arn)
            .ok_or_else(|| resource_not_found(arn))?;

        let tags = fakecloud_core::tags::tags_to_json(&sm.tags, "key", "value");

        Ok(AwsResponse::ok_json(json!({ "tags": tags })))
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────

fn state_machine_to_json(sm: &StateMachine) -> Value {
    let mut resp = json!({
        "name": sm.name,
        "stateMachineArn": sm.arn,
        "definition": sm.definition,
        "roleArn": sm.role_arn,
        "type": sm.machine_type.as_str(),
        "status": sm.status.as_str(),
        "creationDate": sm.creation_date.timestamp() as f64,
        "updateDate": sm.update_date.timestamp() as f64,
        "revisionId": sm.revision_id,
        "label": sm.name,
    });

    if !sm.description.is_empty() {
        resp["description"] = json!(sm.description);
    }

    if let Some(ref logging) = sm.logging_configuration {
        resp["loggingConfiguration"] = logging.clone();
    } else {
        resp["loggingConfiguration"] = json!({
            "level": "OFF",
            "includeExecutionData": false,
            "destinations": [],
        });
    }

    if let Some(ref tracing) = sm.tracing_configuration {
        resp["tracingConfiguration"] = tracing.clone();
    } else {
        resp["tracingConfiguration"] = json!({
            "enabled": false,
        });
    }

    resp
}

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("The request must contain the parameter {name}."),
    )
}

fn state_machine_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "StateMachineDoesNotExist",
        format!("State Machine Does Not Exist: '{arn}'"),
    )
}

fn resource_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFound",
        format!("Resource not found: '{arn}'"),
    )
}

fn validate_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 80 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidName",
            format!("Invalid Name: '{name}' (length must be between 1 and 80 characters)"),
        ));
    }
    // Only allow alphanumeric, hyphens, and underscores
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidName",
            format!(
                "Invalid Name: '{name}' (must only contain alphanumeric characters, hyphens, and underscores)"
            ),
        ));
    }
    Ok(())
}

fn validate_definition(definition: &str) -> Result<(), AwsServiceError> {
    let parsed: Value = serde_json::from_str(definition).map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidDefinition",
            format!("Invalid State Machine Definition: '{e}'"),
        )
    })?;

    if parsed.get("StartAt").and_then(|v| v.as_str()).is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidDefinition",
            "Invalid State Machine Definition: 'MISSING_START_AT' (StartAt field is required)"
                .to_string(),
        ));
    }

    let states = parsed.get("States").and_then(|v| v.as_object());
    if states.is_none() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidDefinition",
            "Invalid State Machine Definition: 'MISSING_STATES' (States field is required)"
                .to_string(),
        ));
    }

    let start_at = parsed["StartAt"].as_str().unwrap();
    let states_obj = states.unwrap();
    if !states_obj.contains_key(start_at) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidDefinition",
            format!(
                "Invalid State Machine Definition: 'MISSING_TRANSITION_TARGET' \
                 (StartAt '{start_at}' does not reference a valid state)"
            ),
        ));
    }

    Ok(())
}

fn execution_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ExecutionDoesNotExist",
        format!("Execution Does Not Exist: '{arn}'"),
    )
}

fn execution_to_json(exec: &Execution) -> Value {
    let mut resp = json!({
        "executionArn": exec.execution_arn,
        "stateMachineArn": exec.state_machine_arn,
        "name": exec.name,
        "status": exec.status.as_str(),
        "startDate": exec.start_date.timestamp() as f64,
    });

    if let Some(ref input) = exec.input {
        resp["input"] = json!(input);
    }
    if let Some(ref output) = exec.output {
        resp["output"] = json!(output);
    }
    if let Some(stop) = exec.stop_date {
        resp["stopDate"] = json!(stop.timestamp() as f64);
    }
    if let Some(ref error) = exec.error {
        resp["error"] = json!(error);
    }
    if let Some(ref cause) = exec.cause {
        resp["cause"] = json!(cause);
    }

    resp
}

/// Convert event type like "PassStateEntered" to the details key format "passStateEntered".
fn camel_to_details_key(event_type: &str) -> String {
    let mut chars = event_type.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_lowercase().to_string() + chars.as_str(),
    }
}

fn validate_arn(arn: &str) -> Result<(), AwsServiceError> {
    if !arn.starts_with("arn:") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidArn",
            format!("Invalid Arn: '{arn}'"),
        ));
    }
    Ok(())
}
