use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::registry::ServiceRegistry;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;
use fakecloud_dynamodb::SharedDynamoDbState;
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
    "CreateActivity",
    "DeleteActivity",
    "DescribeActivity",
    "ListActivities",
    "GetActivityTask",
    "SendTaskFailure",
    "SendTaskHeartbeat",
    "SendTaskSuccess",
    "PublishStateMachineVersion",
    "DeleteStateMachineVersion",
    "ListStateMachineVersions",
    "CreateStateMachineAlias",
    "DeleteStateMachineAlias",
    "DescribeStateMachineAlias",
    "ListStateMachineAliases",
    "UpdateStateMachineAlias",
    "DescribeMapRun",
    "ListMapRuns",
    "UpdateMapRun",
    "RedriveExecution",
    "StartSyncExecution",
    "TestState",
    "ValidateStateMachineDefinition",
];

/// Handle to the central service registry, set by `main.rs` after every service
/// has been registered. Wrapped in `OnceLock` so `StepFunctionsService` can be
/// constructed (and registered into the very registry it later reads back) before
/// the registry itself is finalized. The interpreter snapshots the inner `Arc`
/// when it needs to dispatch generic `aws-sdk:*` Task integrations.
pub type SharedServiceRegistry = Arc<std::sync::OnceLock<Arc<ServiceRegistry>>>;

pub struct StepFunctionsService {
    state: SharedStepFunctionsState,
    delivery: Option<Arc<DeliveryBus>>,
    dynamodb_state: Option<SharedDynamoDbState>,
    registry: Option<SharedServiceRegistry>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

mod activities;
mod executions;
mod map_runs;
mod state_machines;
mod tags;
mod tasks;
mod validation;

impl StepFunctionsService {
    pub fn new(state: SharedStepFunctionsState) -> Self {
        Self {
            state,
            delivery: None,
            dynamodb_state: None,
            registry: None,
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

    /// Hand the service a deferred-fill handle to the central [`ServiceRegistry`].
    /// `main.rs` calls [`OnceLock::set`] on the inner cell after every service
    /// has been registered; until then the interpreter falls back to its
    /// hand-coded SDK integrations (lambda invoke, sqs sendMessage, …).
    pub fn with_registry(mut self, registry: SharedServiceRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save_snapshot(&self) {
        save_stepfunctions_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Step Functions state when invoked,
    /// or `None` in memory mode (no snapshot store). The CloudFormation
    /// provisioner mutates `state` directly and uses this to write a
    /// CFN-provisioned resource through to disk, the same way a direct mutating
    /// API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_stepfunctions_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current Step Functions state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `StepFunctionsService::save_snapshot` and the
/// CloudFormation provisioner's post-provision persist hook so both route
/// through the same serialize-and-write path.
pub async fn save_stepfunctions_snapshot(
    state: &SharedStepFunctionsState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = StepFunctionsSnapshot {
        schema_version: STEPFUNCTIONS_SNAPSHOT_SCHEMA_VERSION,
        state: None,
        accounts: Some(state.read().clone()),
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

/// Abort executions that were RUNNING (or PENDING_REDRIVE) when the server
/// stopped, called once after a persistence snapshot is loaded on startup.
///
/// The in-memory interpreter that was driving each one is gone after a restart,
/// so the execution can never advance. Real Step Functions resumes from durable
/// interpreter state; fakecloud can't, so leaving them RUNNING would strand them
/// forever (DescribeExecution would report RUNNING with no way to ever finish).
/// Aborting with a terminal stop date + error/cause is the honest outcome
/// (bug-audit 2026-06-20, 0.A2). Returns the number reconciled.
pub fn reconcile_interrupted_executions(state: &SharedStepFunctionsState) -> usize {
    let now = Utc::now();
    let mut count = 0;
    let mut accounts = state.write();
    let account_ids: Vec<String> = accounts.iter().map(|(id, _)| id.to_string()).collect();
    for account_id in account_ids {
        let Some(s) = accounts.get_mut(&account_id) else {
            continue;
        };
        for exec in s.executions.values_mut() {
            if matches!(
                exec.status,
                ExecutionStatus::Running | ExecutionStatus::PendingRedrive
            ) {
                exec.status = ExecutionStatus::Aborted;
                exec.stop_date = Some(now);
                exec.error = Some("Fakecloud.Restart".to_string());
                exec.cause = Some(
                    "Execution was interrupted by a fakecloud restart and cannot be resumed"
                        .to_string(),
                );
                count += 1;
            }
        }
    }
    count
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
            | "CreateActivity"
            | "DeleteActivity"
            | "GetActivityTask"
            | "SendTaskFailure"
            | "SendTaskHeartbeat"
            | "SendTaskSuccess"
            | "PublishStateMachineVersion"
            | "DeleteStateMachineVersion"
            | "CreateStateMachineAlias"
            | "DeleteStateMachineAlias"
            | "UpdateStateMachineAlias"
            | "UpdateMapRun"
            | "RedriveExecution"
            | "StartSyncExecution"
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
            "CreateActivity" => self.create_activity(&req),
            "DeleteActivity" => self.delete_activity(&req),
            "DescribeActivity" => self.describe_activity(&req),
            "ListActivities" => self.list_activities(&req),
            "GetActivityTask" => self.get_activity_task(&req).await,
            "SendTaskFailure" => self.send_task_failure(&req),
            "SendTaskHeartbeat" => self.send_task_heartbeat(&req),
            "SendTaskSuccess" => self.send_task_success(&req),
            "PublishStateMachineVersion" => self.publish_state_machine_version(&req),
            "DeleteStateMachineVersion" => self.delete_state_machine_version(&req),
            "ListStateMachineVersions" => self.list_state_machine_versions(&req),
            "CreateStateMachineAlias" => self.create_state_machine_alias(&req),
            "DeleteStateMachineAlias" => self.delete_state_machine_alias(&req),
            "DescribeStateMachineAlias" => self.describe_state_machine_alias(&req),
            "ListStateMachineAliases" => self.list_state_machine_aliases(&req),
            "UpdateStateMachineAlias" => self.update_state_machine_alias(&req),
            "DescribeMapRun" => self.describe_map_run(&req),
            "ListMapRuns" => self.list_map_runs(&req),
            "UpdateMapRun" => self.update_map_run(&req),
            "RedriveExecution" => self.redrive_execution(&req),
            "StartSyncExecution" => self.start_sync_execution(&req).await,
            "TestState" => self.test_state(&req),
            "ValidateStateMachineDefinition" => self.validate_state_machine_definition(&req),
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
    fn list_activities(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let raw_max_results = body["maxResults"].as_i64();
        if let Some(mr) = raw_max_results {
            validate_max_results(mr)?;
        }
        let next_token = body["nextToken"].as_str();
        if let Some(t) = next_token {
            validate_page_token(t)?;
        }
        // Smithy default: maxResults=0 means "use the service default"
        // (100 for Step Functions), which we honour by treating both
        // `None` and `0` as 100.
        let max_results = match raw_max_results.unwrap_or(0) {
            0 => 100,
            n => n as usize,
        };
        let accounts = self.state.read();
        let empty = crate::state::StepFunctionsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut activities: Vec<&crate::state::Activity> = state.activities.values().collect();
        activities.sort_by(|a, b| a.name.cmp(&b.name));
        let items: Vec<Value> = activities
            .iter()
            .map(|a| {
                json!({
                    "activityArn": a.arn,
                    "name": a.name,
                    "creationDate": a.creation_date.timestamp(),
                })
            })
            .collect();
        let (page, token) =
            paginate_checked(&items, next_token, max_results).map_err(|_| invalid_token())?;
        let mut resp = json!({ "activities": page });
        if let Some(t) = token {
            resp["nextToken"] = json!(t);
        }
        Ok(AwsResponse::ok_json(resp))
    }
}

fn state_machine_alias_to_json(alias: &crate::state::StateMachineAlias) -> Value {
    json!({
        "stateMachineAliasArn": alias.arn,
        "name": alias.name,
        "description": alias.description,
        "routingConfiguration": alias.routing_configuration.iter().map(|r| json!({
            "stateMachineVersionArn": r.state_machine_version_arn,
            "weight": r.weight,
        })).collect::<Vec<_>>(),
        "creationDate": alias.creation_date.timestamp(),
        "updateDate": alias.update_date.timestamp(),
    })
}

fn map_run_to_json(mr: &crate::state::MapRun) -> Value {
    json!({
        "mapRunArn": mr.map_run_arn,
        "executionArn": mr.execution_arn,
        "maxConcurrency": mr.max_concurrency,
        "toleratedFailurePercentage": mr.tolerated_failure_percentage,
        "toleratedFailureCount": mr.tolerated_failure_count,
        "status": mr.status,
        "startDate": mr.start_date.timestamp(),
        "stopDate": mr.stop_date.map(|d| d.timestamp()),
    })
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

fn activity_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ActivityDoesNotExist",
        format!("Activity does not exist: {arn}"),
    )
}

fn task_does_not_exist(token: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "TaskDoesNotExist",
        format!("Task does not exist: {token}"),
    )
}

fn resource_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFound",
        format!("Resource not found: '{arn}'"),
    )
}

/// Parse + validate an alias `routingConfiguration` array.
///
/// AWS rules: 1 or 2 routes; weights are 0-100 and sum to 100; each
/// route must include `stateMachineVersionArn`.
fn parse_routing_configuration(
    routes: &[serde_json::Value],
) -> Result<Vec<crate::state::AliasRoute>, AwsServiceError> {
    if routes.is_empty() || routes.len() > 2 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "routingConfiguration must contain 1 or 2 routes.",
        ));
    }
    let parsed: Vec<crate::state::AliasRoute> = routes
        .iter()
        .map(|r| {
            let arn = r["stateMachineVersionArn"].as_str().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "routingConfiguration entries must contain stateMachineVersionArn.",
                )
            })?;
            let weight = r["weight"].as_i64().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "routingConfiguration entries must contain a numeric weight.",
                )
            })?;
            if !(0..=100).contains(&weight) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Invalid routing weight {weight}; must be 0-100."),
                ));
            }
            Ok(crate::state::AliasRoute {
                state_machine_version_arn: arn.to_string(),
                weight: weight as i32,
            })
        })
        .collect::<Result<_, _>>()?;
    let total: i32 = parsed.iter().map(|r| r.weight).sum();
    if total != 100 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("routingConfiguration weights must sum to 100, got {total}."),
        ));
    }
    Ok(parsed)
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

    // Reject malformed JSONPath reference fields. AWS rejects bad reference
    // paths at CreateStateMachine; accepting them here lets a panic-inducing
    // path reach the interpreter at execution time.
    for (state_name, state_val) in states_obj {
        validate_state_paths(state_name, state_val)?;
    }

    Ok(())
}

/// Validate the JSONPath reference fields of a single state. Recurses into the
/// `Choices` array of Choice states. Returns an `InvalidDefinition` error for
/// any syntactically malformed path.
fn validate_state_paths(state_name: &str, state: &Value) -> Result<(), AwsServiceError> {
    for field in ["InputPath", "OutputPath", "ResultPath"] {
        if let Some(p) = state.get(field).and_then(|v| v.as_str()) {
            // `InputPath`/`OutputPath` accept the literal "null" to mean "no
            // value"; `ResultPath` accepts JSON null (handled separately) but
            // not the string "null". Both accept "$"-rooted reference paths.
            if (field != "ResultPath" && p == "null") || is_valid_reference_path(p) {
                continue;
            }
            return Err(invalid_reference_path(state_name, field, p));
        }
    }

    if state.get("Type").and_then(|v| v.as_str()) == Some("Choice") {
        if let Some(choices) = state.get("Choices").and_then(|v| v.as_array()) {
            for rule in choices {
                validate_choice_variables(state_name, rule)?;
            }
        }
    }

    Ok(())
}

/// Validate `Variable` reference paths inside a Choice rule, recursing through
/// nested `And` / `Or` / `Not` boolean combinators.
fn validate_choice_variables(state_name: &str, rule: &Value) -> Result<(), AwsServiceError> {
    if let Some(v) = rule.get("Variable").and_then(|v| v.as_str()) {
        if !is_valid_reference_path(v) {
            return Err(invalid_reference_path(state_name, "Variable", v));
        }
    }
    for combinator in ["And", "Or"] {
        if let Some(nested) = rule.get(combinator).and_then(|v| v.as_array()) {
            for n in nested {
                validate_choice_variables(state_name, n)?;
            }
        }
    }
    if let Some(n) = rule.get("Not") {
        validate_choice_variables(state_name, n)?;
    }
    Ok(())
}

/// A reference path must start with `$` and every `[...]` index segment must be
/// a balanced, non-empty, decimal-integer index. This mirrors the subset of
/// JSONPath the interpreter understands; anything it cannot parse is rejected.
fn is_valid_reference_path(path: &str) -> bool {
    if path != "$" && !path.starts_with("$.") && !path.starts_with("$[") {
        return false;
    }
    let body = path
        .strip_prefix("$.")
        .or_else(|| path.strip_prefix('$'))
        .unwrap_or(path);
    for part in body.split('.') {
        if !segment_is_valid(part) {
            return false;
        }
    }
    true
}

fn segment_is_valid(part: &str) -> bool {
    match part.find('[') {
        None => !part.contains(']'),
        Some(open) => {
            if !part.ends_with(']') {
                return false;
            }
            let inner = &part[open + 1..part.len() - 1];
            // Empty `[]` or non-integer (incl. multibyte/garbage) indices are invalid.
            !inner.is_empty() && inner.parse::<usize>().is_ok()
        }
    }
}

fn invalid_reference_path(state_name: &str, field: &str, path: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidDefinition",
        format!(
            "Invalid State Machine Definition: 'SCHEMA_VALIDATION_FAILED' \
             (The {field} field of state '{state_name}' is not a valid reference path: '{path}')"
        ),
    )
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

/// Convert an event type to the JSON key prefix used in the `HistoryEvent`
/// shape (the full key is `<prefix>EventDetails`).
///
/// AWS collapses every `*StateEntered` event type (Pass/Task/Choice/Wait/Map/
/// Parallel/Succeed/...) into a single `stateEnteredEventDetails` member, and
/// every `*StateExited` into `stateExitedEventDetails`. All other event types
/// map by lowercasing the first character (`TaskScheduled` ->
/// `taskScheduledEventDetails`). Without the collapse, SDK deserializers see an
/// unknown key like `passStateEnteredEventDetails` and return null name/input.
fn camel_to_details_key(event_type: &str) -> String {
    if event_type.ends_with("StateEntered") {
        return "stateEntered".to_string();
    }
    if event_type.ends_with("StateExited") {
        return "stateExited".to_string();
    }
    let mut chars = event_type.chars();
    match chars.next() {
        None => String::new(),
        Some(c) => c.to_lowercase().to_string() + chars.as_str(),
    }
}

/// Compare two StartExecution inputs for STANDARD idempotency. A missing input
/// defaults to `{}` (AWS's default execution input). Comparison is structural
/// (parsed JSON), so insignificant whitespace differences still dedup; inputs
/// that fail to parse fall back to an exact string match.
fn execution_input_matches(stored: Option<&str>, incoming: Option<&str>) -> bool {
    let stored = stored.unwrap_or("{}");
    let incoming = incoming.unwrap_or("{}");
    match (
        serde_json::from_str::<Value>(stored),
        serde_json::from_str::<Value>(incoming),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => stored == incoming,
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

/// Enforce the Smithy @length on an ARN-typed field (`Arn` = 1..=256,
/// `LongArn` = 1..=2000). Empty / oversize ARNs map to `InvalidArn`, which
/// every ARN-bearing Step Functions operation declares.
fn validate_arn_length(field: &str, value: &str, max: usize) -> Result<(), AwsServiceError> {
    if value.is_empty() || value.len() > max {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidArn",
            format!("Invalid Arn at '{field}': must be 1..={max} characters"),
        ));
    }
    Ok(())
}

/// Shared error for a malformed (unparseable) `nextToken` reaching the
/// pagination helper — `InvalidToken` is the only pagination error code
/// declared on every list op.
pub(super) fn invalid_token() -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidToken", "Invalid nextToken")
}

/// `nextToken` is declared as `PageToken` (length 1..=1024). The only
/// error code declared on every paginated list op is `InvalidToken`, so
/// route both empty and oversize tokens through it.
fn validate_page_token(value: &str) -> Result<(), AwsServiceError> {
    if value.is_empty() || value.len() > 1024 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidToken",
            "nextToken must be 1..=1024 characters",
        ));
    }
    Ok(())
}

/// `maxResults` is typed as `PageSize` (range 0..=1000). The negative
/// probes flip below-min / above-max; since the only error declared on
/// `ListActivities` is `InvalidToken`, we map page-size violations to
/// the same code (matches how AWS surfaces malformed pagination input
/// for ops that don't model a separate `ValidationException`).
fn validate_max_results(value: i64) -> Result<(), AwsServiceError> {
    if !(0..=1000).contains(&value) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidToken",
            format!("maxResults '{value}' is outside 0..=1000"),
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
    registry: &Option<SharedServiceRegistry>,
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
        parent_execution_arn: None,
        is_sync: false,
        billed_duration_ms: None,
        billed_memory_mb: None,
    };

    st.executions.insert(exec_arn.clone(), execution);
    let logging_config = sm.logging_configuration.clone();
    drop(accounts);

    let shared_state = state.clone();
    let delivery = delivery.clone();
    let dynamodb_state = dynamodb_state.clone();
    let registry = registry.clone();
    let input = Some(input.to_string());
    tokio::spawn(async move {
        interpreter::execute_state_machine(
            shared_state,
            exec_arn,
            definition,
            input,
            delivery,
            dynamodb_state,
            registry,
            logging_config,
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
    use std::collections::HashMap;
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
    async fn start_execution_same_name_same_input_is_idempotent() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "dup-exec");

        let body = json!({
            "stateMachineArn": arn,
            "name": "same-name",
            "input": "{\"a\":1}",
        });
        let req = make_request("StartExecution", &body.to_string());
        let first = body_json(&svc.start_execution(&req).unwrap());

        // Same name AND same input -> 200 with the existing executionArn.
        let req = make_request("StartExecution", &body.to_string());
        let second = body_json(&svc.start_execution(&req).unwrap());
        assert_eq!(first["executionArn"], second["executionArn"]);
        assert_eq!(first["startDate"], second["startDate"]);
    }

    #[tokio::test]
    async fn start_execution_same_name_different_input_conflicts() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "dup-exec-diff");

        let req = make_request(
            "StartExecution",
            &json!({
                "stateMachineArn": arn,
                "name": "same-name",
                "input": "{\"a\":1}",
            })
            .to_string(),
        );
        svc.start_execution(&req).unwrap();

        // Same name, DIFFERENT input -> 400 ExecutionAlreadyExists.
        let req = make_request(
            "StartExecution",
            &json!({
                "stateMachineArn": arn,
                "name": "same-name",
                "input": "{\"a\":2}",
            })
            .to_string(),
        );
        let err = expect_err(svc.start_execution(&req));
        assert!(err.to_string().contains("ExecutionAlreadyExists"));
    }

    #[tokio::test]
    async fn start_execution_express_name_collision_never_idempotent() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_express_sm(&svc, "dup-exec-express");

        let body = json!({
            "stateMachineArn": arn,
            "name": "same-name",
            "input": "{\"a\":1}",
        });
        let req = make_request("StartExecution", &body.to_string());
        svc.start_execution(&req).unwrap();

        // EXPRESS has no idempotency: even an identical re-issue conflicts.
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
    fn test_validate_definition_rejects_malformed_paths() {
        // Unterminated bracket in InputPath.
        let def =
            r#"{"StartAt":"P","States":{"P":{"Type":"Pass","InputPath":"$.arr[","End":true}}}"#;
        assert!(validate_definition(def).is_err());

        // Multibyte char where the close bracket would be, in OutputPath.
        let def =
            "{\"StartAt\":\"P\",\"States\":{\"P\":{\"Type\":\"Pass\",\"OutputPath\":\"$.x[\u{00e9}\",\"End\":true}}}";
        assert!(validate_definition(def).is_err());

        // Malformed Choice Variable.
        let def = r#"{"StartAt":"C","States":{"C":{"Type":"Choice","Choices":[{"Variable":"$.n[","NumericEquals":1,"Next":"P"}]},"P":{"Type":"Pass","End":true}}}"#;
        assert!(validate_definition(def).is_err());

        // Path not rooted at $.
        let def =
            r#"{"StartAt":"P","States":{"P":{"Type":"Pass","InputPath":"foo.bar","End":true}}}"#;
        assert!(validate_definition(def).is_err());

        // Empty index brackets.
        let def =
            r#"{"StartAt":"P","States":{"P":{"Type":"Pass","ResultPath":"$.x[]","End":true}}}"#;
        assert!(validate_definition(def).is_err());
    }

    #[test]
    fn test_validate_definition_accepts_well_formed_paths() {
        // Valid reference paths, the literal "null" for Input/OutputPath, and
        // nested Choice combinators must all be accepted.
        let def = r#"{"StartAt":"P","States":{
            "P":{"Type":"Pass","InputPath":"$.a.b[0].c","OutputPath":"$","ResultPath":"$.out","Next":"C"},
            "C":{"Type":"Choice","Choices":[
                {"And":[{"Variable":"$.items[2]","NumericEquals":1},{"Variable":"$.flag","BooleanEquals":true}],"Next":"S"}
            ],"Default":"S"},
            "S":{"Type":"Succeed","InputPath":"null"}
        }}"#;
        assert!(validate_definition(def).is_ok());
    }

    #[test]
    fn test_is_valid_reference_path() {
        assert!(is_valid_reference_path("$"));
        assert!(is_valid_reference_path("$.foo"));
        assert!(is_valid_reference_path("$.foo.bar[3].baz"));
        assert!(is_valid_reference_path("$[0]"));
        assert!(!is_valid_reference_path("$.arr["));
        assert!(!is_valid_reference_path("$.x[\u{00e9}"));
        assert!(!is_valid_reference_path("$.x[]"));
        assert!(!is_valid_reference_path("$.x[abc]"));
        assert!(!is_valid_reference_path("foo.bar"));
        assert!(!is_valid_reference_path(""));
    }

    #[test]
    fn test_validate_arn() {
        assert!(validate_arn("arn:aws:states:us-east-1:123:sm:test").is_ok());
        assert!(validate_arn("not-an-arn").is_err());
    }

    #[test]
    fn test_camel_to_details_key() {
        // All *StateEntered / *StateExited types collapse to one key each.
        assert_eq!(camel_to_details_key("PassStateEntered"), "stateEntered");
        assert_eq!(camel_to_details_key("TaskStateEntered"), "stateEntered");
        assert_eq!(camel_to_details_key("ChoiceStateExited"), "stateExited");
        assert_eq!(camel_to_details_key("MapStateExited"), "stateExited");
        // Other event types keep first-char-lowercased prefix.
        assert_eq!(camel_to_details_key("TaskScheduled"), "taskScheduled");
        assert_eq!(camel_to_details_key("ExecutionStarted"), "executionStarted");
        assert_eq!(camel_to_details_key(""), "");
    }

    #[test]
    fn test_is_mutating_action() {
        assert!(is_mutating_action("CreateStateMachine"));
        assert!(is_mutating_action("StartExecution"));
        assert!(!is_mutating_action("DescribeStateMachine"));
        assert!(!is_mutating_action("ListStateMachines"));
    }

    // ── StartSyncExecution ──

    fn create_express_sm(svc: &StepFunctionsService, name: &str) -> String {
        let body = json!({
            "name": name,
            "definition": VALID_DEF,
            "roleArn": "arn:aws:iam::123456789012:role/test",
            "type": "EXPRESS",
        });
        let req = make_request("CreateStateMachine", &body.to_string());
        let resp = svc.create_state_machine(&req).unwrap();
        let b = body_json(&resp);
        b["stateMachineArn"].as_str().unwrap().to_string()
    }

    #[tokio::test]
    async fn start_sync_execution_basic() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_express_sm(&svc, "sync-sm");

        let body = json!({
            "stateMachineArn": arn,
            "input": r#"{"key":"value"}"#,
        });
        let req = make_request("StartSyncExecution", &body.to_string());
        let resp = svc.start_sync_execution(&req).await.unwrap();
        let b = body_json(&resp);
        assert!(b["executionArn"]
            .as_str()
            .unwrap()
            .contains("express:sync-sm"));
        assert_eq!(b["stateMachineArn"], arn);
        assert_eq!(b["status"], "SUCCEEDED");
        assert!(b["startDate"].as_i64().is_some());
        assert!(b["stopDate"].as_i64().is_some());
        assert!(b["output"].as_str().is_some());
        assert!(b["billingDetails"]["billedDurationInMilliseconds"]
            .as_i64()
            .is_some());
    }

    #[tokio::test]
    async fn start_sync_execution_not_express() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_sm(&svc, "std-sm");

        let body = json!({"stateMachineArn": arn});
        let req = make_request("StartSyncExecution", &body.to_string());
        let err = expect_err(svc.start_sync_execution(&req).await);
        assert!(err.to_string().contains("StateMachineTypeNotSupported"));
    }

    #[tokio::test]
    async fn start_sync_execution_sm_not_found() {
        let svc = StepFunctionsService::new(make_state());
        let body = json!({
            "stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nope",
        });
        let req = make_request("StartSyncExecution", &body.to_string());
        let err = expect_err(svc.start_sync_execution(&req).await);
        assert!(err.to_string().contains("StateMachineDoesNotExist"));
    }

    #[tokio::test]
    async fn start_sync_execution_records_introspection_fields() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_express_sm(&svc, "sync-introspect");

        let body = json!({"stateMachineArn": arn, "input": "{}"});
        let req = make_request("StartSyncExecution", &body.to_string());
        let resp = svc.start_sync_execution(&req).await.unwrap();
        let b = body_json(&resp);
        let exec_arn = b["executionArn"].as_str().unwrap().to_string();

        let accounts = svc.state.read();
        let state = accounts.get("123456789012").unwrap();
        let stored = state
            .executions
            .get(&exec_arn)
            .expect("sync execution should be persisted for introspection");
        assert!(stored.is_sync, "sync executions must be marked is_sync");
        assert_eq!(stored.billed_memory_mb, Some(64));
        assert!(
            stored.billed_duration_ms.is_some(),
            "billed_duration_ms must be populated after sync run"
        );
        assert!(
            stored.parent_execution_arn.is_none(),
            "top-level sync execution has no parent"
        );
    }

    #[tokio::test]
    async fn start_sync_execution_invalid_input() {
        let svc = StepFunctionsService::new(make_state());
        let arn = create_express_sm(&svc, "bad-input-sync");

        let body = json!({
            "stateMachineArn": arn,
            "input": "not json",
        });
        let req = make_request("StartSyncExecution", &body.to_string());
        let err = expect_err(svc.start_sync_execution(&req).await);
        assert!(err.to_string().contains("InvalidExecutionInput"));
    }

    /// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
    #[test]
    fn snapshot_hook_is_none_without_store() {
        let svc = StepFunctionsService::new(make_state());
        assert!(svc.snapshot_hook().is_none());
    }

    /// With a store, the hook is present and invoking it runs the whole-state
    /// persist path the CloudFormation provisioner uses after mutating Step
    /// Functions state directly.
    #[tokio::test]
    async fn snapshot_hook_fires_with_store() {
        let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
            Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
        let svc = StepFunctionsService::new(make_state()).with_snapshot_store(store);
        let hook = svc
            .snapshot_hook()
            .expect("hook present when a store is set");
        hook().await;
    }

    fn make_execution(arn: &str, status: ExecutionStatus) -> Execution {
        Execution {
            execution_arn: arn.to_string(),
            state_machine_arn: "arn:aws:states:us-east-1:123456789012:stateMachine:sm".to_string(),
            state_machine_name: "sm".to_string(),
            name: arn.to_string(),
            status,
            input: None,
            output: None,
            start_date: Utc::now(),
            stop_date: None,
            error: None,
            cause: None,
            history_events: vec![],
            parent_execution_arn: None,
            is_sync: false,
            billed_duration_ms: None,
            billed_memory_mb: None,
        }
    }

    #[test]
    fn reconcile_aborts_running_executions_on_restart() {
        // After a restart a RUNNING execution has no interpreter driving it, so
        // it must be aborted rather than left RUNNING forever (0.A2). A
        // completed execution is untouched.
        let state = make_state();
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create("123456789012");
            s.executions.insert(
                "running".into(),
                make_execution("running", ExecutionStatus::Running),
            );
            s.executions.insert(
                "done".into(),
                make_execution("done", ExecutionStatus::Succeeded),
            );
        }

        let n = reconcile_interrupted_executions(&state);
        assert_eq!(n, 1, "only the RUNNING execution is reconciled");

        let accounts = state.read();
        let s = accounts.get("123456789012").unwrap();
        let running = &s.executions["running"];
        assert_eq!(running.status, ExecutionStatus::Aborted);
        assert!(running.stop_date.is_some());
        assert_eq!(running.error.as_deref(), Some("Fakecloud.Restart"));
        assert_eq!(s.executions["done"].status, ExecutionStatus::Succeeded);
    }
}

#[cfg(test)]
mod pagination_reject_test {
    #[test]
    fn paginate_checked_rejects_invalid_token() {
        use fakecloud_core::pagination::paginate_checked;
        let items: Vec<i32> = (0..5).collect();
        assert!(paginate_checked(&items, Some("bad"), 3).is_err());
        assert!(paginate_checked(&items, Some("2"), 3).is_ok());
    }
}
