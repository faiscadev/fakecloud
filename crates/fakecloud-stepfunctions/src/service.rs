use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;
use fakecloud_dynamodb::state::SharedDynamoDbState;
use fakecloud_persistence::SnapshotStore;

use crate::interpreter;
use crate::state::{
    Execution, ExecutionStatus, SharedStepFunctionsState, StateMachine, StateMachineStatus,
    StateMachineType, StepFunctionsSnapshot, StepFunctionsState,
    STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION,
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
    delivery: Option<Arc<DeliveryBus>>,
    dynamodb_state: Option<SharedDynamoDbState>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl StepFunctionsService {
    pub fn new(state: SharedStepFunctionsState) -> Self {
        Self {
            state,
            delivery: None,
            dynamodb_state: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_delivery(mut self, delivery: Arc<DeliveryBus>) -> Self {
        self.delivery = Some(delivery);
        self
    }

    pub fn with_dynamodb(mut self, dynamodb_state: SharedDynamoDbState) -> Self {
        self.dynamodb_state = Some(dynamodb_state);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save_snapshot(&self) {
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let snapshot = StepFunctionsSnapshot {
            schema_version: STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION,
            state: None,
            accounts: Some(self.state.read().clone()),
        };
        let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let bytes = serde_json::to_vec(&snapshot)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            store.save(&bytes)
        })
        .await;
        match join {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(%err, "failed to write stepfunctions snapshot"),
            Err(err) => tracing::error!(%err, "stepfunctions snapshot task panicked"),
        }
    }
}

fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateStateMachine"
            | "DeleteStateMachine"
            | "UpdateStateMachine"
            | "TagResource"
            | "UntagResource"
            | "StartExecution"
            | "StopExecution"
    )
}

#[async_trait]
impl AwsService for StepFunctionsService {
    fn service_name(&self) -> &str {
        "states"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(req.action.as_str());
        let result = match req.action.as_str() {
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
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        drop(accounts);

        // Spawn async execution
        let shared_state = self.state.clone();
        let exec_arn_clone = exec_arn.clone();
        let input_clone = input;
        let delivery = self.delivery.clone();
        let dynamodb_state = self.dynamodb_state.clone();
        tokio::spawn(async move {
            interpreter::execute_state_machine(
                shared_state,
                exec_arn_clone,
                definition,
                input_clone,
                delivery,
                dynamodb_state,
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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
        executions.sort_by_key(|e| std::cmp::Reverse(e.start_date));

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

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let accounts = self.state.read();
        let empty = StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

    let states_obj = parsed
        .get("States")
        .and_then(|v| v.as_object())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDefinition",
                "Invalid State Machine Definition: 'MISSING_STATES' (States field is required)"
                    .to_string(),
            )
        })?;

    let start_at = parsed["StartAt"].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidDefinition",
            "Invalid State Machine Definition: 'MISSING_START_AT' (StartAt field is required)"
                .to_string(),
        )
    })?;
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

/// Start a Step Functions execution from a cross-service delivery (e.g. EventBridge).
///
/// This is the public entry point used by `StepFunctionsDeliveryImpl` in the server crate.
/// It mirrors the logic from `StartExecution` but without the AWS request/response wrapper.
/// Start a Step Functions execution from a cross-service delivery (e.g. EventBridge).
///
/// This is the public entry point used by `StepFunctionsDeliveryImpl` in the server crate.
/// It mirrors the logic from `StartExecution` but without the AWS request/response wrapper.
pub fn start_execution_from_delivery(
    state: &SharedStepFunctionsState,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    state_machine_arn: &str,
    input: &str,
) {
    // Validate input is valid JSON
    if serde_json::from_str::<serde_json::Value>(input).is_err() {
        tracing::warn!(
            state_machine_arn,
            "Step Functions delivery: invalid JSON input, skipping execution"
        );
        return;
    }

    let execution_name = uuid::Uuid::new_v4().to_string();

    // Extract account_id from the state machine ARN
    let account_id = state_machine_arn
        .split(':')
        .nth(4)
        .unwrap_or("000000000000")
        .to_string();

    let mut accounts = state.write();
    let st = accounts.get_or_create(&account_id);
    let sm = match st.state_machines.get(state_machine_arn) {
        Some(sm) => sm,
        None => {
            tracing::warn!(
                state_machine_arn,
                "Step Functions delivery: state machine not found"
            );
            return;
        }
    };

    let sm_name = sm.name.clone();
    let definition = sm.definition.clone();
    let exec_arn = st.execution_arn(&sm_name, &execution_name);

    let now = Utc::now();
    let execution = Execution {
        execution_arn: exec_arn.clone(),
        state_machine_arn: state_machine_arn.to_string(),
        state_machine_name: sm_name,
        name: execution_name,
        status: ExecutionStatus::Running,
        input: Some(input.to_string()),
        output: None,
        start_date: now,
        stop_date: None,
        error: None,
        cause: None,
        history_events: vec![],
    };

    st.executions.insert(exec_arn.clone(), execution);
    drop(accounts);

    let shared_state = state.clone();
    let delivery = delivery.clone();
    let dynamodb_state = dynamodb_state.clone();
    let input = Some(input.to_string());
    tokio::spawn(async move {
        interpreter::execute_state_machine(
            shared_state,
            exec_arn,
            definition,
            input,
            delivery,
            dynamodb_state,
        )
        .await;
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use serde_json::Value;
    use std::sync::Arc;

    fn make_state() -> SharedStepFunctionsState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn make_request(action: &str, body: &str) -> AwsRequest {
        AwsRequest {
            service: "states".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-id".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: body.as_bytes().to_vec().into(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_json(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
        match result {
            Err(e) => e,
            Ok(_) => panic!("expected error, got Ok"),
        }
    }

    const VALID_DEF: &str = r#"{"StartAt":"Pass","States":{"Pass":{"Type":"Pass","End":true}}}"#;

    fn create_sm(svc: &StepFunctionsService, name: &str) -> String {
        let body = json!({
            "name": name,
            "definition": VALID_DEF,
            "roleArn": "arn:aws:iam::123456789012:role/test",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let resp = svc.create_state_machine(&req).unwrap();
        let b = body_json(&resp);
        b["stateMachineArn"].as_str().unwrap().to_string()
    }

    // ── CreateStateMachine ──

    #[test]
    fn create_state_machine_basic() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "test-sm");
        assert!(arn.contains("test-sm"));
    }

    #[test]
    fn create_state_machine_with_express_type() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "name": "express-sm",
            "definition": VALID_DEF,
            "roleArn": "arn:aws:iam::123456789012:role/r",
            "type": "EXPRESS",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let resp = svc.create_state_machine(&req).unwrap();
        let b = body_json(&resp);
        assert!(b["stateMachineArn"].as_str().is_some());
    }

    #[test]
    fn create_state_machine_duplicate_fails() {
        let svc = StepFunctionsService::new(make_state());
        create_sm(&svc, "dup-sm");
        let body = json!({
            "name": "dup-sm",
            "definition": VALID_DEF,
            "roleArn": "arn:aws:iam::123456789012:role/r",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let err = expect_err(svc.create_state_machine(&req));
        assert!(err.to_string().contains("StateMachineAlreadyExists"));
    }

    #[test]
    fn create_state_machine_missing_name() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "definition": VALID_DEF,
            "roleArn": "arn:aws:iam::123456789012:role/r",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        assert!(svc.create_state_machine(&req).is_err());
    }

    #[test]
    fn create_state_machine_invalid_definition() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "name": "bad-def",
            "definition": "not json",
            "roleArn": "arn:aws:iam::123456789012:role/r",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let err = expect_err(svc.create_state_machine(&req));
        assert!(err.to_string().contains("InvalidDefinition"));
    }

    #[test]
    fn create_state_machine_definition_missing_start_at() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "name": "no-start",
            "definition": r#"{"States":{"S":{"Type":"Pass","End":true}}}"#,
            "roleArn": "arn:aws:iam::123456789012:role/r",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let err = expect_err(svc.create_state_machine(&req));
        assert!(err.to_string().contains("InvalidDefinition"));
    }

    #[test]
    fn create_state_machine_definition_missing_states() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "name": "no-states",
            "definition": r#"{"StartAt":"S"}"#,
            "roleArn": "arn:aws:iam::123456789012:role/r",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let err = expect_err(svc.create_state_machine(&req));
        assert!(err.to_string().contains("InvalidDefinition"));
    }

    #[test]
    fn create_state_machine_definition_start_at_not_in_states() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "name": "bad-start",
            "definition": r#"{"StartAt":"Missing","States":{"S":{"Type":"Pass","End":true}}}"#,
            "roleArn": "arn:aws:iam::123456789012:role/r",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let err = expect_err(svc.create_state_machine(&req));
        assert!(err.to_string().contains("MISSING_TRANSITION_TARGET"));
    }

    #[test]
    fn create_state_machine_invalid_type() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "name": "bad-type",
            "definition": VALID_DEF,
            "roleArn": "arn:aws:iam::123456789012:role/r",
            "type": "INVALID",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        assert!(svc.create_state_machine(&req).is_err());
    }

    #[test]
    fn create_state_machine_invalid_arn() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "name": "bad-arn",
            "definition": VALID_DEF,
            "roleArn": "not-an-arn",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let err = expect_err(svc.create_state_machine(&req));
        assert!(err.to_string().contains("InvalidArn"));
    }

    #[test]
    fn create_state_machine_invalid_name() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "name": "has spaces!",
            "definition": VALID_DEF,
            "roleArn": "arn:aws:iam::123456789012:role/r",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let err = expect_err(svc.create_state_machine(&req));
        assert!(err.to_string().contains("InvalidName"));
    }

    #[test]
    fn create_state_machine_name_too_long() {
        let svc = StepFunctionsService::new(make_state());
        let long_name = "a".repeat(81);
        let body = json!({
            "name": long_name,
            "definition": VALID_DEF,
            "roleArn": "arn:aws:iam::123456789012:role/r",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let err = expect_err(svc.create_state_machine(&req));
        assert!(err.to_string().contains("InvalidName"));
    }

    // ── DescribeStateMachine ──

    #[test]
    fn describe_state_machine_found() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "desc-sm");

        let req = make_request(
            "DescribeStateMachine",
            &json!({"stateMachineArn": arn}).to_string(),
        );
        let resp = svc.describe_state_machine(&req).unwrap();
        let b = body_json(&resp);
        assert_eq!(b["name"], "desc-sm");
        assert_eq!(b["status"], "ACTIVE");
        assert!(b["definition"].as_str().is_some());
    }

    #[test]
    fn describe_state_machine_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let req = make_request(
            "DescribeStateMachine",
            &json!({"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope"})
                .to_string(),
        );
        let err = expect_err(svc.describe_state_machine(&req));
        assert!(err.to_string().contains("StateMachineDoesNotExist"));
    }

    // ── ListStateMachines ──

    #[test]
    fn list_state_machines_empty() {
        let svc = StepFunctionsService::new(make_state());
        let req = make_request("ListStateMachines", "{}");
        let resp = svc.list_state_machines(&req).unwrap();
        let b = body_json(&resp);
        assert!(b["stateMachines"].as_array().unwrap().is_empty());
    }

    #[test]
    fn list_state_machines_returns_created() {
        let svc = StepFunctionsService::new(make_state());
        create_sm(&svc, "sm-1");
        create_sm(&svc, "sm-2");

        let req = make_request("ListStateMachines", "{}");
        let resp = svc.list_state_machines(&req).unwrap();
        let b = body_json(&resp);
        assert_eq!(b["stateMachines"].as_array().unwrap().len(), 2);
    }

    // ── DeleteStateMachine ──

    #[test]
    fn delete_state_machine() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "del-sm");

        let req = make_request(
            "DeleteStateMachine",
            &json!({"stateMachineArn": arn}).to_string(),
        );
        svc.delete_state_machine(&req).unwrap();

        // Describe should fail
        let req = make_request(
            "DescribeStateMachine",
            &json!({"stateMachineArn": arn}).to_string(),
        );
        assert!(svc.describe_state_machine(&req).is_err());
    }

    #[test]
    fn delete_state_machine_nonexistent_succeeds() {
        let svc = StepFunctionsService::new(make_state());
        let req = make_request(
            "DeleteStateMachine",
            &json!({"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope"})
                .to_string(),
        );
        // AWS returns success even for nonexistent
        svc.delete_state_machine(&req).unwrap();
    }

    // ── UpdateStateMachine ──

    #[test]
    fn update_state_machine() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "upd-sm");

        let new_def = r#"{"StartAt":"NewPass","States":{"NewPass":{"Type":"Pass","End":true}}}"#;
        let body = json!({
            "stateMachineArn": arn,
            "definition": new_def,
            "description": "updated",
        });
        let req = make_request("UpdateStateMachine", &body.to_string());
        let resp = svc.update_state_machine(&req).unwrap();
        let b = body_json(&resp);
        assert!(b["updateDate"].as_f64().is_some());

        // Verify
        let req = make_request(
            "DescribeStateMachine",
            &json!({"stateMachineArn": arn}).to_string(),
        );
        let resp = svc.describe_state_machine(&req).unwrap();
        let b = body_json(&resp);
        assert!(b["definition"].as_str().unwrap().contains("NewPass"));
        assert_eq!(b["description"], "updated");
    }

    #[test]
    fn update_state_machine_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope",
            "definition": VALID_DEF,
        });
        let req = make_request("UpdateStateMachine", &body.to_string());
        let err = expect_err(svc.update_state_machine(&req));
        assert!(err.to_string().contains("StateMachineDoesNotExist"));
    }

    // ── StartExecution ──

    #[tokio::test]
    async fn start_execution_basic() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "exec-sm");

        let body = json!({
            "stateMachineArn": arn,
            "input": r#"{"key":"value"}"#,
        });
        let req = make_request("StartExecution", &body.to_string());
        let resp = svc.start_execution(&req).unwrap();
        let b = body_json(&resp);
        assert!(b["executionArn"].as_str().is_some());
        assert!(b["startDate"].as_f64().is_some());
    }

    #[tokio::test]
    async fn start_execution_with_name() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "named-exec");

        let body = json!({
            "stateMachineArn": arn,
            "name": "my-execution",
        });
        let req = make_request("StartExecution", &body.to_string());
        let resp = svc.start_execution(&req).unwrap();
        let b = body_json(&resp);
        assert!(b["executionArn"].as_str().unwrap().contains("my-execution"));
    }

    #[tokio::test]
    async fn start_execution_sm_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope",
        });
        let req = make_request("StartExecution", &body.to_string());
        let err = expect_err(svc.start_execution(&req));
        assert!(err.to_string().contains("StateMachineDoesNotExist"));
    }

    #[tokio::test]
    async fn start_execution_invalid_input() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "bad-input");

        let body = json!({
            "stateMachineArn": arn,
            "input": "not json",
        });
        let req = make_request("StartExecution", &body.to_string());
        let err = expect_err(svc.start_execution(&req));
        assert!(err.to_string().contains("InvalidExecutionInput"));
    }

    #[tokio::test]
    async fn start_execution_duplicate_name() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "dup-exec");

        let body = json!({
            "stateMachineArn": arn,
            "name": "same-name",
        });
        let req = make_request("StartExecution", &body.to_string());
        svc.start_execution(&req).unwrap();

        let req = make_request("StartExecution", &body.to_string());
        let err = expect_err(svc.start_execution(&req));
        assert!(err.to_string().contains("ExecutionAlreadyExists"));
    }

    // ── DescribeExecution ──

    #[tokio::test]
    async fn describe_execution_found() {
        let svc = StepFunctionsService::new(make_state());
        let sm_arn = create_sm(&svc, "desc-exec");

        let body = json!({"stateMachineArn": sm_arn, "name": "e1"});
        let req = make_request("StartExecution", &body.to_string());
        let resp = svc.start_execution(&req).unwrap();
        let exec_arn = body_json(&resp)["executionArn"]
            .as_str()
            .unwrap()
            .to_string();

        let req = make_request(
            "DescribeExecution",
            &json!({"executionArn": exec_arn}).to_string(),
        );
        let resp = svc.describe_execution(&req).unwrap();
        let b = body_json(&resp);
        assert_eq!(b["name"], "e1");
        assert_eq!(b["status"], "RUNNING");
    }

    #[tokio::test]
    async fn describe_execution_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let req = make_request(
            "DescribeExecution",
            &json!({"executionArn": "arn:aws:states:us-east-1:123456789012:execution:sm:nope"})
                .to_string(),
        );
        let err = expect_err(svc.describe_execution(&req));
        assert!(err.to_string().contains("ExecutionDoesNotExist"));
    }

    // ── StopExecution ──

    #[tokio::test]
    async fn stop_execution() {
        let svc = StepFunctionsService::new(make_state());
        let sm_arn = create_sm(&svc, "stop-sm");

        let body = json!({"stateMachineArn": sm_arn, "name": "stop-e"});
        let req = make_request("StartExecution", &body.to_string());
        let resp = svc.start_execution(&req).unwrap();
        let exec_arn = body_json(&resp)["executionArn"]
            .as_str()
            .unwrap()
            .to_string();

        let body = json!({
            "executionArn": exec_arn,
            "error": "UserAborted",
            "cause": "test stop",
        });
        let req = make_request("StopExecution", &body.to_string());
        let resp = svc.stop_execution(&req).unwrap();
        let b = body_json(&resp);
        assert!(b["stopDate"].as_f64().is_some());

        // Verify aborted
        let req = make_request(
            "DescribeExecution",
            &json!({"executionArn": exec_arn}).to_string(),
        );
        let resp = svc.describe_execution(&req).unwrap();
        let b = body_json(&resp);
        assert_eq!(b["status"], "ABORTED");
        assert_eq!(b["error"], "UserAborted");
    }

    #[tokio::test]
    async fn stop_execution_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let req = make_request(
            "StopExecution",
            &json!({"executionArn": "arn:aws:states:us-east-1:123456789012:execution:sm:nope"})
                .to_string(),
        );
        let err = expect_err(svc.stop_execution(&req));
        assert!(err.to_string().contains("ExecutionDoesNotExist"));
    }

    // ── ListExecutions ──

    #[tokio::test]
    async fn list_executions() {
        let svc = StepFunctionsService::new(make_state());
        let sm_arn = create_sm(&svc, "list-exec");

        for i in 0..3 {
            let body = json!({"stateMachineArn": sm_arn, "name": format!("e{i}")});
            let req = make_request("StartExecution", &body.to_string());
            svc.start_execution(&req).unwrap();
        }

        let req = make_request(
            "ListExecutions",
            &json!({"stateMachineArn": sm_arn}).to_string(),
        );
        let resp = svc.list_executions(&req).unwrap();
        let b = body_json(&resp);
        assert_eq!(b["executions"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn list_executions_sm_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let req = make_request(
            "ListExecutions",
            &json!({"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope"})
                .to_string(),
        );
        let err = expect_err(svc.list_executions(&req));
        assert!(err.to_string().contains("StateMachineDoesNotExist"));
    }

    // ── GetExecutionHistory ──

    #[tokio::test]
    async fn get_execution_history_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let req = make_request(
            "GetExecutionHistory",
            &json!({"executionArn": "arn:aws:states:us-east-1:123456789012:execution:sm:nope"})
                .to_string(),
        );
        let err = expect_err(svc.get_execution_history(&req));
        assert!(err.to_string().contains("ExecutionDoesNotExist"));
    }

    // ── DescribeStateMachineForExecution ──

    #[tokio::test]
    async fn describe_sm_for_execution() {
        let svc = StepFunctionsService::new(make_state());
        let sm_arn = create_sm(&svc, "sm-for-exec");

        let body = json!({"stateMachineArn": sm_arn, "name": "e1"});
        let req = make_request("StartExecution", &body.to_string());
        let resp = svc.start_execution(&req).unwrap();
        let exec_arn = body_json(&resp)["executionArn"]
            .as_str()
            .unwrap()
            .to_string();

        let req = make_request(
            "DescribeStateMachineForExecution",
            &json!({"executionArn": exec_arn}).to_string(),
        );
        let resp = svc.describe_state_machine_for_execution(&req).unwrap();
        let b = body_json(&resp);
        assert_eq!(b["name"], "sm-for-exec");
    }

    // ── Tags ──

    #[test]
    fn tag_untag_list_tags() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "tagged-sm");

        // Tag
        let body = json!({
            "resourceArn": arn,
            "tags": [{"key": "env", "value": "prod"}],
        });
        let req = make_request("TagResource", &body.to_string());
        svc.tag_resource(&req).unwrap();

        // List
        let req = make_request(
            "ListTagsForResource",
            &json!({"resourceArn": arn}).to_string(),
        );
        let resp = svc.list_tags_for_resource(&req).unwrap();
        let b = body_json(&resp);
        let tags = b["tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["key"], "env");

        // Untag
        let body = json!({
            "resourceArn": arn,
            "tagKeys": ["env"],
        });
        let req = make_request("UntagResource", &body.to_string());
        svc.untag_resource(&req).unwrap();

        // Verify empty
        let req = make_request(
            "ListTagsForResource",
            &json!({"resourceArn": arn}).to_string(),
        );
        let resp = svc.list_tags_for_resource(&req).unwrap();
        let b = body_json(&resp);
        assert!(b["tags"].as_array().unwrap().is_empty());
    }

    #[test]
    fn tag_resource_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "resourceArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope",
            "tags": [{"key": "k", "value": "v"}],
        });
        let req = make_request("TagResource", &body.to_string());
        let err = expect_err(svc.tag_resource(&req));
        assert!(err.to_string().contains("ResourceNotFound"));
    }

    // ── Helper function tests ──

    #[test]
    fn test_validate_name() {
        assert!(validate_name("valid-name").is_ok());
        assert!(validate_name("under_score").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("has spaces").is_err());
        assert!(validate_name(&"a".repeat(81)).is_err());
    }

    #[test]
    fn test_validate_definition() {
        assert!(validate_definition(VALID_DEF).is_ok());
        assert!(validate_definition("not json").is_err());
        assert!(validate_definition(r#"{"States":{}}"#).is_err()); // missing StartAt
        assert!(validate_definition(r#"{"StartAt":"S"}"#).is_err()); // missing States
    }

    #[test]
    fn test_validate_arn() {
        assert!(validate_arn("arn:aws:states:us-east-1:123:sm:test").is_ok());
        assert!(validate_arn("not-an-arn").is_err());
    }

    #[test]
    fn test_camel_to_details_key() {
        assert_eq!(camel_to_details_key("PassStateEntered"), "passStateEntered");
        assert_eq!(camel_to_details_key(""), "");
    }

    #[test]
    fn test_is_mutating_action() {
        assert!(is_mutating_action("CreateStateMachine"));
        assert!(is_mutating_action("StartExecution"));
        assert!(!is_mutating_action("DescribeStateMachine"));
        assert!(!is_mutating_action("ListStateMachines"));
    }
}
