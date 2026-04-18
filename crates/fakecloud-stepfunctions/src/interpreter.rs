use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};
use tracing::{debug, warn};

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_dynamodb::state::SharedDynamoDbState;

use crate::choice::evaluate_choice;
use crate::error_handling::{find_catcher, should_retry};
use crate::io_processing::{apply_input_path, apply_output_path, apply_result_path};
use crate::state::{ExecutionStatus, HistoryEvent, SharedStepFunctionsState};

/// Execute a state machine definition with the given input.
/// Updates the execution record in shared state as it progresses.
pub async fn execute_state_machine(
    state: SharedStepFunctionsState,
    execution_arn: String,
    definition: String,
    input: Option<String>,
    delivery: Option<Arc<DeliveryBus>>,
    dynamodb_state: Option<SharedDynamoDbState>,
) {
    let def: Value = match serde_json::from_str(&definition) {
        Ok(v) => v,
        Err(e) => {
            fail_execution(
                &state,
                &execution_arn,
                "States.Runtime",
                &format!("Failed to parse definition: {e}"),
            );
            return;
        }
    };

    let raw_input: Value = input
        .as_deref()
        .and_then(|s| serde_json::from_str(s).ok())
        .unwrap_or(json!({}));

    // Record ExecutionStarted event
    add_event(
        &state,
        &execution_arn,
        "ExecutionStarted",
        0,
        json!({
            "input": serde_json::to_string(&raw_input).unwrap_or_default(),
            "roleArn": "arn:aws:iam::123456789012:role/execution-role"
        }),
    );

    // Run the state machine inside an inner tokio::spawn so that any panic
    // bubbles up as a JoinError instead of tearing down the caller. Without
    // this the panic propagates through the outer spawn in `start_execution`
    // which leaves the execution stuck in Running and leaks the panic to
    // tokio's default hook.
    let def_owned = def;
    let state_clone = state.clone();
    let execution_arn_clone = execution_arn.clone();
    let delivery_clone = delivery.clone();
    let dynamodb_state_clone = dynamodb_state.clone();
    let handle = tokio::spawn(async move {
        run_states(
            &def_owned,
            raw_input,
            &delivery_clone,
            &dynamodb_state_clone,
            &state_clone,
            &execution_arn_clone,
        )
        .await
    });

    match handle.await {
        Ok(Ok(output)) => {
            succeed_execution(&state, &execution_arn, &output);
        }
        Ok(Err((error, cause))) => {
            fail_execution(&state, &execution_arn, &error, &cause);
        }
        Err(join_err) => {
            let msg = if join_err.is_panic() {
                let payload = join_err.into_panic();
                if let Some(s) = payload.downcast_ref::<String>() {
                    s.clone()
                } else if let Some(s) = payload.downcast_ref::<&'static str>() {
                    (*s).to_string()
                } else {
                    "execution task panicked".to_string()
                }
            } else {
                format!("execution task cancelled: {join_err}")
            };
            tracing::error!(
                execution_arn = %execution_arn,
                panic = %msg,
                "Step Functions execution panicked"
            );
            fail_execution(&state, &execution_arn, "States.Runtime", &msg);
        }
    }
}

type StatesResult<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Value, (String, String)>> + Send + 'a>,
>;

/// Core execution loop: runs through states in a definition and returns the output.
/// Used by the top-level executor, Parallel branches, and Map iterations.
fn run_states<'a>(
    def: &'a Value,
    input: Value,
    delivery: &'a Option<Arc<DeliveryBus>>,
    dynamodb_state: &'a Option<SharedDynamoDbState>,
    shared_state: &'a SharedStepFunctionsState,
    execution_arn: &'a str,
) -> StatesResult<'a> {
    Box::pin(async move {
        let start_at = def["StartAt"]
            .as_str()
            .ok_or_else(|| {
                (
                    "States.Runtime".to_string(),
                    "Missing StartAt in definition".to_string(),
                )
            })?
            .to_string();

        let states = def.get("States").ok_or_else(|| {
            (
                "States.Runtime".to_string(),
                "Missing States in definition".to_string(),
            )
        })?;

        let mut current_state = start_at;
        let mut effective_input = input;
        let mut iteration = 0;
        let max_iterations = 500;

        loop {
            iteration += 1;
            if iteration > max_iterations {
                return Err((
                    "States.Runtime".to_string(),
                    "Maximum number of state transitions exceeded".to_string(),
                ));
            }

            let state_def = states.get(&current_state).cloned().ok_or_else(|| {
                (
                    "States.Runtime".to_string(),
                    format!("State '{current_state}' not found in definition"),
                )
            })?;

            let state_type = state_def["Type"]
                .as_str()
                .ok_or_else(|| {
                    (
                        "States.Runtime".to_string(),
                        format!("State '{current_state}' missing Type field"),
                    )
                })?
                .to_string();

            debug!(
                execution_arn = %execution_arn,
                state = %current_state,
                state_type = %state_type,
                "Executing state"
            );

            let advance = match state_type.as_str() {
                "Pass" => run_pass_state(
                    &current_state,
                    &state_def,
                    effective_input,
                    shared_state,
                    execution_arn,
                ),
                "Succeed" => run_succeed_state(
                    &current_state,
                    &state_def,
                    effective_input,
                    shared_state,
                    execution_arn,
                ),
                "Fail" => run_fail_state(
                    &current_state,
                    &state_def,
                    effective_input,
                    shared_state,
                    execution_arn,
                ),
                "Choice" => run_choice_state(
                    &current_state,
                    &state_def,
                    effective_input,
                    shared_state,
                    execution_arn,
                ),
                "Wait" => {
                    run_wait_state(
                        &current_state,
                        &state_def,
                        effective_input,
                        shared_state,
                        execution_arn,
                    )
                    .await
                }
                "Task" => {
                    run_task_state(
                        &current_state,
                        &state_def,
                        effective_input,
                        delivery,
                        dynamodb_state,
                        shared_state,
                        execution_arn,
                    )
                    .await
                }
                "Parallel" => {
                    run_parallel_state(
                        &current_state,
                        &state_def,
                        effective_input,
                        delivery,
                        dynamodb_state,
                        shared_state,
                        execution_arn,
                    )
                    .await
                }
                "Map" => {
                    run_map_state(
                        &current_state,
                        &state_def,
                        effective_input,
                        delivery,
                        dynamodb_state,
                        shared_state,
                        execution_arn,
                    )
                    .await
                }
                other => Advance::Fail(
                    "States.Runtime".to_string(),
                    format!("Unsupported state type: '{other}'"),
                ),
            };

            match advance {
                Advance::Next(next, new_input) => {
                    effective_input = new_input;
                    current_state = next;
                }
                Advance::End(output) => return Ok(output),
                Advance::Fail(error, cause) => return Err((error, cause)),
            }
        }
    })
}

/// Result of executing a single state in the state machine.
enum Advance {
    /// Continue to the given state with the given input.
    Next(String, Value),
    /// Terminate the state machine with the given output.
    End(Value),
    /// Fail the state machine with the given error and cause.
    Fail(String, String),
}

fn advance_from_next(state_def: &Value, input: Value) -> Advance {
    match next_state(state_def) {
        NextState::Name(next) => Advance::Next(next, input),
        NextState::End => Advance::End(input),
        NextState::Error(msg) => Advance::Fail("States.Runtime".to_string(), msg),
    }
}

fn advance_from_error(state_def: &Value, input: &Value, error: String, cause: String) -> Advance {
    match apply_state_catcher(state_def, input, &error, &cause) {
        Some((next, new_input)) => Advance::Next(next, new_input),
        None => Advance::Fail(error, cause),
    }
}

fn run_pass_state(
    name: &str,
    state_def: &Value,
    input: Value,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let entered_event_id = add_event(
        shared_state,
        execution_arn,
        "PassStateEntered",
        0,
        json!({
            "name": name,
            "input": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    let result = execute_pass_state(state_def, &input);

    add_event(
        shared_state,
        execution_arn,
        "PassStateExited",
        entered_event_id,
        json!({
            "name": name,
            "output": serde_json::to_string(&result).unwrap_or_default(),
        }),
    );

    advance_from_next(state_def, result)
}

fn run_succeed_state(
    name: &str,
    state_def: &Value,
    input: Value,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    add_event(
        shared_state,
        execution_arn,
        "SucceedStateEntered",
        0,
        json!({
            "name": name,
            "input": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    let input_path = state_def["InputPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    let processed = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(&input, input_path)
    };

    let output = if output_path == Some("null") {
        json!({})
    } else {
        apply_output_path(&processed, output_path)
    };

    add_event(
        shared_state,
        execution_arn,
        "SucceedStateExited",
        0,
        json!({
            "name": name,
            "output": serde_json::to_string(&output).unwrap_or_default(),
        }),
    );

    Advance::End(output)
}

fn run_fail_state(
    name: &str,
    state_def: &Value,
    input: Value,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let error = state_def["Error"]
        .as_str()
        .unwrap_or("States.Fail")
        .to_string();
    let cause = state_def["Cause"].as_str().unwrap_or("").to_string();

    add_event(
        shared_state,
        execution_arn,
        "FailStateEntered",
        0,
        json!({
            "name": name,
            "input": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    Advance::Fail(error, cause)
}

fn run_choice_state(
    name: &str,
    state_def: &Value,
    input: Value,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let entered_event_id = add_event(
        shared_state,
        execution_arn,
        "ChoiceStateEntered",
        0,
        json!({
            "name": name,
            "input": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    let input_path = state_def["InputPath"].as_str();
    let processed_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(&input, input_path)
    };

    match evaluate_choice(state_def, &processed_input) {
        Some(next) => {
            add_event(
                shared_state,
                execution_arn,
                "ChoiceStateExited",
                entered_event_id,
                json!({
                    "name": name,
                    "output": serde_json::to_string(&input).unwrap_or_default(),
                }),
            );
            Advance::Next(next, input)
        }
        None => Advance::Fail(
            "States.NoChoiceMatched".to_string(),
            format!("No choice rule matched and no Default in state '{name}'"),
        ),
    }
}

async fn run_wait_state(
    name: &str,
    state_def: &Value,
    input: Value,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let entered_event_id = add_event(
        shared_state,
        execution_arn,
        "WaitStateEntered",
        0,
        json!({
            "name": name,
            "input": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    execute_wait_state(state_def, &input).await;

    add_event(
        shared_state,
        execution_arn,
        "WaitStateExited",
        entered_event_id,
        json!({
            "name": name,
            "output": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    advance_from_next(state_def, input)
}

#[allow(clippy::too_many_arguments)]
async fn run_task_state(
    name: &str,
    state_def: &Value,
    input: Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let entered_event_id = add_event(
        shared_state,
        execution_arn,
        "TaskStateEntered",
        0,
        json!({
            "name": name,
            "input": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    let result = execute_task_state(
        state_def,
        &input,
        delivery,
        dynamodb_state,
        shared_state,
        execution_arn,
        entered_event_id,
    )
    .await;

    match result {
        Ok(output) => {
            add_event(
                shared_state,
                execution_arn,
                "TaskStateExited",
                entered_event_id,
                json!({
                    "name": name,
                    "output": serde_json::to_string(&output).unwrap_or_default(),
                }),
            );
            advance_from_next(state_def, output)
        }
        Err((error, cause)) => advance_from_error(state_def, &input, error, cause),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_parallel_state(
    name: &str,
    state_def: &Value,
    input: Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let entered_event_id = add_event(
        shared_state,
        execution_arn,
        "ParallelStateEntered",
        0,
        json!({
            "name": name,
            "input": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    let result = execute_parallel_state(
        state_def,
        &input,
        delivery,
        dynamodb_state,
        shared_state,
        execution_arn,
    )
    .await;

    match result {
        Ok(output) => {
            add_event(
                shared_state,
                execution_arn,
                "ParallelStateExited",
                entered_event_id,
                json!({
                    "name": name,
                    "output": serde_json::to_string(&output).unwrap_or_default(),
                }),
            );
            advance_from_next(state_def, output)
        }
        Err((error, cause)) => advance_from_error(state_def, &input, error, cause),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_map_state(
    name: &str,
    state_def: &Value,
    input: Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let entered_event_id = add_event(
        shared_state,
        execution_arn,
        "MapStateEntered",
        0,
        json!({
            "name": name,
            "input": serde_json::to_string(&input).unwrap_or_default(),
        }),
    );

    let result = execute_map_state(
        state_def,
        &input,
        delivery,
        dynamodb_state,
        shared_state,
        execution_arn,
    )
    .await;

    match result {
        Ok(output) => {
            add_event(
                shared_state,
                execution_arn,
                "MapStateExited",
                entered_event_id,
                json!({
                    "name": name,
                    "output": serde_json::to_string(&output).unwrap_or_default(),
                }),
            );
            advance_from_next(state_def, output)
        }
        Err((error, cause)) => advance_from_error(state_def, &input, error, cause),
    }
}

/// Execute a Wait state: pause execution for a specified duration or until a timestamp.
async fn execute_wait_state(state_def: &Value, input: &Value) {
    if let Some(seconds) = state_def["Seconds"].as_u64() {
        tokio::time::sleep(tokio::time::Duration::from_secs(seconds)).await;
        return;
    }

    if let Some(path) = state_def["SecondsPath"].as_str() {
        let val = crate::io_processing::resolve_path(input, path);
        if let Some(seconds) = val.as_u64() {
            tokio::time::sleep(tokio::time::Duration::from_secs(seconds)).await;
        }
        return;
    }

    if let Some(ts_str) = state_def["Timestamp"].as_str() {
        if let Ok(target) = chrono::DateTime::parse_from_rfc3339(ts_str) {
            let now = Utc::now();
            let target_utc = target.with_timezone(&chrono::Utc);
            if target_utc > now {
                let duration = (target_utc - now).to_std().unwrap_or_default();
                tokio::time::sleep(duration).await;
            }
        }
        return;
    }

    if let Some(path) = state_def["TimestampPath"].as_str() {
        let val = crate::io_processing::resolve_path(input, path);
        if let Some(ts_str) = val.as_str() {
            if let Ok(target) = chrono::DateTime::parse_from_rfc3339(ts_str) {
                let now = Utc::now();
                let target_utc = target.with_timezone(&chrono::Utc);
                if target_utc > now {
                    let duration = (target_utc - now).to_std().unwrap_or_default();
                    tokio::time::sleep(duration).await;
                }
            }
        }
        return;
    }

    warn!(
        "Wait state has no valid Seconds, SecondsPath, Timestamp, or TimestampPath — skipping wait"
    );
}

/// Execute a Pass state: apply InputPath, use Result if present, apply ResultPath and OutputPath.
fn execute_pass_state(state_def: &Value, input: &Value) -> Value {
    let input_path = state_def["InputPath"].as_str();
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)
    };

    let result = if let Some(r) = state_def.get("Result") {
        r.clone()
    } else {
        effective_input.clone()
    };

    let after_result = if result_path == Some("null") {
        input.clone()
    } else {
        apply_result_path(input, &result, result_path)
    };

    if output_path == Some("null") {
        json!({})
    } else {
        apply_output_path(&after_result, output_path)
    }
}

/// Execute a Task state: invoke the resource (Lambda, SQS, SNS, EventBridge, DynamoDB),
/// apply I/O processing, handle Retry.
async fn execute_task_state(
    state_def: &Value,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
    entered_event_id: i64,
) -> Result<Value, (String, String)> {
    let resource = state_def["Resource"].as_str().unwrap_or("").to_string();

    let input_path = state_def["InputPath"].as_str();
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)
    };

    let task_input = if let Some(params) = state_def.get("Parameters") {
        apply_parameters(params, &effective_input)
    } else {
        effective_input
    };

    let retriers = state_def["Retry"].as_array().cloned().unwrap_or_default();
    let timeout_seconds = state_def["TimeoutSeconds"].as_u64();
    let mut attempt = 0u32;

    loop {
        add_event(
            shared_state,
            execution_arn,
            "TaskScheduled",
            entered_event_id,
            json!({
                "resource": resource,
                "region": "us-east-1",
                "parameters": serde_json::to_string(&task_input).unwrap_or_default(),
            }),
        );

        add_event(
            shared_state,
            execution_arn,
            "TaskStarted",
            entered_event_id,
            json!({ "resource": resource }),
        );

        let invoke_result = invoke_resource(
            &resource,
            &task_input,
            delivery,
            dynamodb_state,
            timeout_seconds,
        )
        .await;

        match invoke_result {
            Ok(result) => {
                add_event(
                    shared_state,
                    execution_arn,
                    "TaskSucceeded",
                    entered_event_id,
                    json!({
                        "resource": resource,
                        "output": serde_json::to_string(&result).unwrap_or_default(),
                    }),
                );

                let selected = if let Some(selector) = state_def.get("ResultSelector") {
                    apply_parameters(selector, &result)
                } else {
                    result
                };

                let after_result = if result_path == Some("null") {
                    input.clone()
                } else {
                    apply_result_path(input, &selected, result_path)
                };

                let output = if output_path == Some("null") {
                    json!({})
                } else {
                    apply_output_path(&after_result, output_path)
                };

                return Ok(output);
            }
            Err((error, cause)) => {
                add_event(
                    shared_state,
                    execution_arn,
                    "TaskFailed",
                    entered_event_id,
                    json!({ "error": error, "cause": cause }),
                );

                if let Some(delay_ms) = should_retry(&retriers, &error, attempt) {
                    attempt += 1;
                    let actual_delay = delay_ms.min(5000);
                    tokio::time::sleep(tokio::time::Duration::from_millis(actual_delay)).await;
                    continue;
                }

                return Err((error, cause));
            }
        }
    }
}

/// Execute a Parallel state: run all branches concurrently, collect results into an array.
async fn execute_parallel_state(
    state_def: &Value,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Result<Value, (String, String)> {
    let input_path = state_def["InputPath"].as_str();
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)
    };

    let branches = state_def["Branches"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    if branches.is_empty() {
        return Err((
            "States.Runtime".to_string(),
            "Parallel state has no Branches".to_string(),
        ));
    }

    // Spawn all branches concurrently
    let mut handles = Vec::new();
    for branch_def in &branches {
        let branch = branch_def.clone();
        let branch_input = effective_input.clone();
        let delivery = delivery.clone();
        let ddb = dynamodb_state.clone();
        let state = shared_state.clone();
        let arn = execution_arn.to_string();

        handles.push(tokio::spawn(async move {
            run_states(&branch, branch_input, &delivery, &ddb, &state, &arn).await
        }));
    }

    // Collect results in order
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        let result = handle.await.map_err(|e| {
            (
                "States.Runtime".to_string(),
                format!("Branch execution panicked: {e}"),
            )
        })??;
        results.push(result);
    }

    let branch_output = Value::Array(results);

    // Apply ResultSelector if present
    let selected = if let Some(selector) = state_def.get("ResultSelector") {
        apply_parameters(selector, &branch_output)
    } else {
        branch_output
    };

    // Apply ResultPath
    let after_result = if result_path == Some("null") {
        input.clone()
    } else {
        apply_result_path(input, &selected, result_path)
    };

    // Apply OutputPath
    let output = if output_path == Some("null") {
        json!({})
    } else {
        apply_output_path(&after_result, output_path)
    };

    Ok(output)
}

/// Execute a Map state: iterate over an array and run a sub-state machine per item.
async fn execute_map_state(
    state_def: &Value,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Result<Value, (String, String)> {
    let input_path = state_def["InputPath"].as_str();
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)
    };

    // Get the items to iterate over
    let items_path = state_def["ItemsPath"].as_str().unwrap_or("$");
    let items_value = crate::io_processing::resolve_path(&effective_input, items_path);
    let items = items_value.as_array().cloned().unwrap_or_default();

    // Get the iterator definition (ItemProcessor or Iterator for backwards compat)
    let iterator_def = state_def
        .get("ItemProcessor")
        .or_else(|| state_def.get("Iterator"))
        .cloned()
        .ok_or_else(|| {
            (
                "States.Runtime".to_string(),
                "Map state has no ItemProcessor or Iterator".to_string(),
            )
        })?;

    let max_concurrency = state_def["MaxConcurrency"].as_u64().unwrap_or(0);
    let effective_concurrency = if max_concurrency == 0 {
        40
    } else {
        max_concurrency as usize
    };

    let semaphore = Arc::new(tokio::sync::Semaphore::new(effective_concurrency));

    // Process all items
    let mut handles = Vec::new();
    for (index, item) in items.into_iter().enumerate() {
        let iter_def = iterator_def.clone();
        let delivery = delivery.clone();
        let ddb = dynamodb_state.clone();
        let state = shared_state.clone();
        let arn = execution_arn.to_string();
        let sem = semaphore.clone();

        // Apply ItemSelector if present
        let item_input = if let Some(selector) = state_def.get("ItemSelector") {
            let mut ctx = serde_json::Map::new();
            ctx.insert("value".to_string(), item.clone());
            ctx.insert("index".to_string(), json!(index));
            apply_parameters(selector, &Value::Object(ctx))
        } else {
            item
        };

        add_event(
            shared_state,
            execution_arn,
            "MapIterationStarted",
            0,
            json!({ "index": index }),
        );

        handles.push(tokio::spawn(async move {
            let _permit = sem
                .acquire()
                .await
                .map_err(|_| ("States.Runtime".to_string(), "Semaphore closed".to_string()))?;
            let result = run_states(&iter_def, item_input, &delivery, &ddb, &state, &arn).await;
            Ok::<(usize, Result<Value, (String, String)>), (String, String)>((index, result))
        }));
    }

    // Collect results in order
    let mut results: Vec<(usize, Value)> = Vec::with_capacity(handles.len());
    for handle in handles {
        let (index, result) = handle.await.map_err(|e| {
            (
                "States.Runtime".to_string(),
                format!("Map iteration panicked: {e}"),
            )
        })??;

        match result {
            Ok(output) => {
                add_event(
                    shared_state,
                    execution_arn,
                    "MapIterationSucceeded",
                    0,
                    json!({ "index": index }),
                );
                results.push((index, output));
            }
            Err((error, cause)) => {
                add_event(
                    shared_state,
                    execution_arn,
                    "MapIterationFailed",
                    0,
                    json!({ "index": index, "error": error }),
                );
                return Err((error, cause));
            }
        }
    }

    // Sort by index to maintain order
    results.sort_by_key(|(i, _)| *i);
    let map_output = Value::Array(results.into_iter().map(|(_, v)| v).collect());

    // Apply ResultSelector if present
    let selected = if let Some(selector) = state_def.get("ResultSelector") {
        apply_parameters(selector, &map_output)
    } else {
        map_output
    };

    // Apply ResultPath
    let after_result = if result_path == Some("null") {
        input.clone()
    } else {
        apply_result_path(input, &selected, result_path)
    };

    // Apply OutputPath
    let output = if output_path == Some("null") {
        json!({})
    } else {
        apply_output_path(&after_result, output_path)
    };

    Ok(output)
}

/// Invoke a resource (Lambda function or SDK integration).
async fn invoke_resource(
    resource: &str,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    // Direct Lambda ARN: arn:aws:lambda:<region>:<account>:function:<name>
    if resource.contains(":lambda:") && resource.contains(":function:") {
        return invoke_lambda_direct(resource, input, delivery, timeout_seconds).await;
    }

    // SDK integration patterns: arn:aws:states:::<service>:<action>
    if resource.starts_with("arn:aws:states:::lambda:invoke") {
        let function_name = input["FunctionName"].as_str().unwrap_or("");
        let payload = if let Some(p) = input.get("Payload") {
            p.clone()
        } else {
            input.clone()
        };
        return invoke_lambda_direct(function_name, &payload, delivery, timeout_seconds).await;
    }

    if resource.starts_with("arn:aws:states:::sqs:sendMessage") {
        return invoke_sqs_send_message(input, delivery);
    }

    if resource.starts_with("arn:aws:states:::sns:publish") {
        return invoke_sns_publish(input, delivery);
    }

    if resource.starts_with("arn:aws:states:::events:putEvents") {
        return invoke_eventbridge_put_events(input, delivery);
    }

    if resource.starts_with("arn:aws:states:::dynamodb:getItem") {
        return invoke_dynamodb_get_item(input, dynamodb_state);
    }

    if resource.starts_with("arn:aws:states:::dynamodb:putItem") {
        return invoke_dynamodb_put_item(input, dynamodb_state);
    }

    if resource.starts_with("arn:aws:states:::dynamodb:deleteItem") {
        return invoke_dynamodb_delete_item(input, dynamodb_state);
    }

    if resource.starts_with("arn:aws:states:::dynamodb:updateItem") {
        return invoke_dynamodb_update_item(input, dynamodb_state);
    }

    Err((
        "States.TaskFailed".to_string(),
        format!("Unsupported resource: {resource}"),
    ))
}

/// Send a message to an SQS queue via DeliveryBus.
fn invoke_sqs_send_message(
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
) -> Result<Value, (String, String)> {
    let delivery = delivery.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No delivery bus configured for SQS".to_string(),
        )
    })?;

    let queue_url = input["QueueUrl"].as_str().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "Missing QueueUrl in SQS sendMessage parameters".to_string(),
        )
    })?;

    let message_body = input["MessageBody"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            // If MessageBody is not a string, serialize the value
            serde_json::to_string(&input["MessageBody"]).unwrap_or_default()
        });

    // Convert QueueUrl to ARN format for the delivery bus
    // QueueUrl format: http://.../<account>/<queue-name>
    // ARN format: arn:aws:sqs:<region>:<account>:<queue-name>
    let queue_arn = queue_url_to_arn(queue_url);

    delivery.send_to_sqs(&queue_arn, &message_body, &HashMap::new());

    Ok(json!({
        "MessageId": uuid::Uuid::new_v4().to_string(),
        "MD5OfMessageBody": md5_hex(&message_body),
    }))
}

/// Publish a message to an SNS topic via DeliveryBus.
fn invoke_sns_publish(
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
) -> Result<Value, (String, String)> {
    let delivery = delivery.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No delivery bus configured for SNS".to_string(),
        )
    })?;

    let topic_arn = input["TopicArn"].as_str().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "Missing TopicArn in SNS publish parameters".to_string(),
        )
    })?;

    let message = input["Message"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_else(|| serde_json::to_string(&input["Message"]).unwrap_or_default());

    let subject = input["Subject"].as_str();

    delivery.publish_to_sns(topic_arn, &message, subject);

    Ok(json!({
        "MessageId": uuid::Uuid::new_v4().to_string(),
    }))
}

/// Put events onto an EventBridge bus via DeliveryBus.
fn invoke_eventbridge_put_events(
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
) -> Result<Value, (String, String)> {
    let delivery = delivery.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No delivery bus configured for EventBridge".to_string(),
        )
    })?;

    let entries = input["Entries"]
        .as_array()
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                "Missing Entries in EventBridge putEvents parameters".to_string(),
            )
        })?
        .clone();

    let mut event_ids = Vec::new();
    for entry in &entries {
        let source = entry["Source"].as_str().unwrap_or("aws.stepfunctions");
        let detail_type = entry["DetailType"].as_str().unwrap_or("StepFunctionsEvent");
        let detail = entry["Detail"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| serde_json::to_string(&entry["Detail"]).unwrap_or("{}".to_string()));
        let bus_name = entry["EventBusName"].as_str().unwrap_or("default");

        delivery.put_event_to_eventbridge(source, detail_type, &detail, bus_name);
        event_ids.push(uuid::Uuid::new_v4().to_string());
    }

    Ok(json!({
        "Entries": event_ids.iter().map(|id| json!({"EventId": id})).collect::<Vec<_>>(),
        "FailedEntryCount": 0,
    }))
}

/// Get an item from DynamoDB via direct state access.
fn invoke_dynamodb_get_item(
    input: &Value,
    dynamodb_state: &Option<SharedDynamoDbState>,
) -> Result<Value, (String, String)> {
    let ddb = dynamodb_state.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No DynamoDB state configured".to_string(),
        )
    })?;

    let table_name = input["TableName"].as_str().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "Missing TableName in DynamoDB getItem parameters".to_string(),
        )
    })?;

    let key = input
        .get("Key")
        .and_then(|k| k.as_object())
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                "Missing Key in DynamoDB getItem parameters".to_string(),
            )
        })?;

    let key_map: HashMap<String, Value> = key.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

    let __mas = ddb.read();
    let state = __mas.default_ref();
    let table = state.tables.get(table_name).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("Table '{table_name}' not found"),
        )
    })?;

    let item = table
        .find_item_index(&key_map)
        .map(|idx| table.items[idx].clone());

    match item {
        Some(item_map) => {
            let item_value: serde_json::Map<String, Value> = item_map.into_iter().collect();
            Ok(json!({ "Item": item_value }))
        }
        None => Ok(json!({})),
    }
}

/// Put an item into DynamoDB via direct state access.
fn invoke_dynamodb_put_item(
    input: &Value,
    dynamodb_state: &Option<SharedDynamoDbState>,
) -> Result<Value, (String, String)> {
    let ddb = dynamodb_state.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No DynamoDB state configured".to_string(),
        )
    })?;

    let table_name = input["TableName"].as_str().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "Missing TableName in DynamoDB putItem parameters".to_string(),
        )
    })?;

    let item = input
        .get("Item")
        .and_then(|i| i.as_object())
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                "Missing Item in DynamoDB putItem parameters".to_string(),
            )
        })?;

    let item_map: HashMap<String, Value> =
        item.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

    let mut __mas = ddb.write();
    let state = __mas.default_mut();
    let table = state.tables.get_mut(table_name).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("Table '{table_name}' not found"),
        )
    })?;

    // Replace existing item with same key, or insert new
    if let Some(idx) = table.find_item_index(&item_map) {
        table.items[idx] = item_map;
    } else {
        table.items.push(item_map);
    }

    Ok(json!({}))
}

/// Delete an item from DynamoDB via direct state access.
fn invoke_dynamodb_delete_item(
    input: &Value,
    dynamodb_state: &Option<SharedDynamoDbState>,
) -> Result<Value, (String, String)> {
    let ddb = dynamodb_state.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No DynamoDB state configured".to_string(),
        )
    })?;

    let table_name = input["TableName"].as_str().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "Missing TableName in DynamoDB deleteItem parameters".to_string(),
        )
    })?;

    let key = input
        .get("Key")
        .and_then(|k| k.as_object())
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                "Missing Key in DynamoDB deleteItem parameters".to_string(),
            )
        })?;

    let key_map: HashMap<String, Value> = key.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

    let mut __mas = ddb.write();
    let state = __mas.default_mut();
    let table = state.tables.get_mut(table_name).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("Table '{table_name}' not found"),
        )
    })?;

    if let Some(idx) = table.find_item_index(&key_map) {
        table.items.remove(idx);
    }

    Ok(json!({}))
}

/// Update an item in DynamoDB via direct state access.
/// Simplified: merges Key+ExpressionAttributeValues into the existing item.
fn invoke_dynamodb_update_item(
    input: &Value,
    dynamodb_state: &Option<SharedDynamoDbState>,
) -> Result<Value, (String, String)> {
    let ddb = dynamodb_state.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No DynamoDB state configured".to_string(),
        )
    })?;

    let table_name = input["TableName"].as_str().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "Missing TableName in DynamoDB updateItem parameters".to_string(),
        )
    })?;

    let key = input
        .get("Key")
        .and_then(|k| k.as_object())
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                "Missing Key in DynamoDB updateItem parameters".to_string(),
            )
        })?;

    let key_map: HashMap<String, Value> = key.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

    let mut __mas = ddb.write();
    let state = __mas.default_mut();
    let table = state.tables.get_mut(table_name).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("Table '{table_name}' not found"),
        )
    })?;

    // Parse UpdateExpression to apply SET operations
    if let Some(update_expr) = input["UpdateExpression"].as_str() {
        let attr_values = input
            .get("ExpressionAttributeValues")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let attr_names = input
            .get("ExpressionAttributeNames")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();

        if let Some(idx) = table.find_item_index(&key_map) {
            apply_update_expression(
                &mut table.items[idx],
                update_expr,
                &attr_values,
                &attr_names,
            );
        } else {
            // Create new item with key + update expression values
            let mut new_item = key_map;
            apply_update_expression(&mut new_item, update_expr, &attr_values, &attr_names);
            table.items.push(new_item);
        }
    }

    Ok(json!({}))
}

/// Apply a simple SET UpdateExpression to an item.
fn apply_update_expression(
    item: &mut HashMap<String, Value>,
    expr: &str,
    attr_values: &serde_json::Map<String, Value>,
    attr_names: &serde_json::Map<String, Value>,
) {
    // Parse "SET #name1 = :val1, #name2 = :val2" or "SET field = :val"
    // DynamoDB keywords are case-insensitive
    let trimmed = expr.trim();
    let set_part = if trimmed.len() >= 4 && trimmed[..4].eq_ignore_ascii_case("SET ") {
        &trimmed[4..]
    } else {
        trimmed
    };

    for assignment in set_part.split(',') {
        let parts: Vec<&str> = assignment.splitn(2, '=').collect();
        if parts.len() == 2 {
            let attr_ref = parts[0].trim();
            let val_ref = parts[1].trim();

            // Resolve attribute name (could be #alias or direct name)
            let attr_name = if attr_ref.starts_with('#') {
                attr_names
                    .get(attr_ref)
                    .and_then(|v| v.as_str())
                    .unwrap_or(attr_ref)
                    .to_string()
            } else {
                attr_ref.to_string()
            };

            // Resolve value
            if val_ref.starts_with(':') {
                if let Some(val) = attr_values.get(val_ref) {
                    item.insert(attr_name, val.clone());
                }
            }
        }
    }
}

/// Convert an SQS queue URL to an ARN.
/// QueueUrl format: http://localhost:4566/123456789012/my-queue
fn queue_url_to_arn(url: &str) -> String {
    let parts: Vec<&str> = url.rsplitn(3, '/').collect();
    if parts.len() >= 2 {
        let queue_name = parts[0];
        let account_id = parts[1];
        Arn::new("sqs", "us-east-1", account_id, queue_name).to_string()
    } else {
        url.to_string()
    }
}

/// Compute MD5 hex digest for SQS message response format.
fn md5_hex(data: &str) -> String {
    use md5::Digest;
    let result = md5::Md5::digest(data.as_bytes());
    format!("{result:032x}")
}

/// Invoke a Lambda function directly via DeliveryBus.
async fn invoke_lambda_direct(
    function_arn: &str,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    let delivery = delivery.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No delivery bus configured for Lambda invocation".to_string(),
        )
    })?;

    let payload = serde_json::to_string(input).unwrap_or_default();

    let invoke_future = delivery.invoke_lambda(function_arn, &payload);

    let result = if let Some(timeout) = timeout_seconds {
        match tokio::time::timeout(tokio::time::Duration::from_secs(timeout), invoke_future).await {
            Ok(r) => r,
            Err(_) => {
                return Err((
                    "States.Timeout".to_string(),
                    format!("Task timed out after {timeout} seconds"),
                ));
            }
        }
    } else {
        invoke_future.await
    };

    match result {
        Some(Ok(bytes)) => {
            let response_str = String::from_utf8_lossy(&bytes);
            let value: Value =
                serde_json::from_str(&response_str).unwrap_or(json!(response_str.to_string()));
            Ok(value)
        }
        Some(Err(e)) => Err(("States.TaskFailed".to_string(), e)),
        None => {
            // No runtime available — return empty result
            Ok(json!({}))
        }
    }
}

/// Apply Parameters template: keys ending with .$ are treated as JsonPath references.
fn apply_parameters(template: &Value, input: &Value) -> Value {
    match template {
        Value::Object(map) => {
            let mut result = serde_json::Map::new();
            for (key, value) in map {
                if let Some(stripped) = key.strip_suffix(".$") {
                    if let Some(path) = value.as_str() {
                        result.insert(
                            stripped.to_string(),
                            crate::io_processing::resolve_path(input, path),
                        );
                    }
                } else {
                    result.insert(key.clone(), apply_parameters(value, input));
                }
            }
            Value::Object(result)
        }
        Value::Array(arr) => Value::Array(arr.iter().map(|v| apply_parameters(v, input)).collect()),
        other => other.clone(),
    }
}

enum NextState {
    Name(String),
    End,
    Error(String),
}

fn next_state(state_def: &Value) -> NextState {
    if state_def["End"].as_bool() == Some(true) {
        return NextState::End;
    }
    match state_def["Next"].as_str() {
        Some(next) => NextState::Name(next.to_string()),
        None => NextState::Error("State has neither 'End' nor 'Next' field".to_string()),
    }
}

/// Find the first `Catch` clause on `state_def` that matches `error` and
/// apply its `ResultPath` to produce the state to transition to and the
/// new effective input. Returns None when no catcher applies, in which
/// case the error should propagate up.
fn apply_state_catcher(
    state_def: &Value,
    effective_input: &Value,
    error: &str,
    cause: &str,
) -> Option<(String, Value)> {
    let catchers = state_def["Catch"].as_array().cloned().unwrap_or_default();
    let (next, result_path) = find_catcher(&catchers, error)?;
    let error_output = json!({
        "Error": error,
        "Cause": cause,
    });
    let new_input = apply_result_path(effective_input, &error_output, result_path.as_deref());
    Some((next, new_input))
}

/// Extract account ID from an execution ARN (`arn:aws:states:region:account_id:...`).
fn account_id_from_arn(arn: &str) -> &str {
    arn.split(':').nth(4).unwrap_or("000000000000")
}

fn add_event(
    state: &SharedStepFunctionsState,
    execution_arn: &str,
    event_type: &str,
    previous_event_id: i64,
    details: Value,
) -> i64 {
    let account_id = account_id_from_arn(execution_arn).to_string();
    let mut accounts = state.write();
    let s = accounts.get_or_create(&account_id);
    if let Some(exec) = s.executions.get_mut(execution_arn) {
        let id = exec.history_events.len() as i64 + 1;
        exec.history_events.push(HistoryEvent {
            id,
            event_type: event_type.to_string(),
            timestamp: Utc::now(),
            previous_event_id,
            details,
        });
        id
    } else {
        0
    }
}

fn succeed_execution(state: &SharedStepFunctionsState, execution_arn: &str, output: &Value) {
    let account_id = account_id_from_arn(execution_arn).to_string();
    // Check terminal status before recording events to avoid inconsistent history
    {
        let accounts = state.read();
        if let Some(s) = accounts.get(&account_id) {
            if let Some(exec) = s.executions.get(execution_arn) {
                if exec.status != ExecutionStatus::Running {
                    return;
                }
            }
        }
    }

    let output_str = serde_json::to_string(output).unwrap_or_default();

    add_event(
        state,
        execution_arn,
        "ExecutionSucceeded",
        0,
        json!({ "output": output_str }),
    );

    let mut accounts = state.write();
    let s = accounts.get_or_create(&account_id);
    if let Some(exec) = s.executions.get_mut(execution_arn) {
        exec.status = ExecutionStatus::Succeeded;
        exec.output = Some(output_str);
        exec.stop_date = Some(Utc::now());
    }
}

fn fail_execution(state: &SharedStepFunctionsState, execution_arn: &str, error: &str, cause: &str) {
    let account_id = account_id_from_arn(execution_arn).to_string();
    // Check terminal status before recording events to avoid inconsistent history
    {
        let accounts = state.read();
        if let Some(s) = accounts.get(&account_id) {
            if let Some(exec) = s.executions.get(execution_arn) {
                if exec.status != ExecutionStatus::Running {
                    return;
                }
            }
        }
    }

    add_event(
        state,
        execution_arn,
        "ExecutionFailed",
        0,
        json!({ "error": error, "cause": cause }),
    );

    let mut accounts = state.write();
    let s = accounts.get_or_create(&account_id);
    if let Some(exec) = s.executions.get_mut(execution_arn) {
        exec.status = ExecutionStatus::Failed;
        exec.error = Some(error.to_string());
        exec.cause = Some(cause.to_string());
        exec.stop_date = Some(Utc::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Execution;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn make_state() -> SharedStepFunctionsState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn create_execution(state: &SharedStepFunctionsState, arn: &str, input: Option<String>) {
        let mut accounts = state.write();
        let s = accounts.get_or_create("123456789012");
        s.executions.insert(
            arn.to_string(),
            Execution {
                execution_arn: arn.to_string(),
                state_machine_arn: "arn:aws:states:us-east-1:123456789012:stateMachine:test"
                    .to_string(),
                state_machine_name: "test".to_string(),
                name: "exec-1".to_string(),
                status: ExecutionStatus::Running,
                input,
                output: None,
                start_date: Utc::now(),
                stop_date: None,
                error: None,
                cause: None,
                history_events: vec![],
            },
        );
    }

    #[tokio::test]
    async fn test_simple_pass_state() {
        let state = make_state();
        let arn = "arn:aws:states:us-east-1:123456789012:execution:test:exec-1";
        create_execution(&state, arn, Some(r#"{"hello":"world"}"#.to_string()));

        let definition = json!({
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "Result": {"processed": true},
                    "End": true
                }
            }
        })
        .to_string();

        execute_state_machine(
            state.clone(),
            arn.to_string(),
            definition,
            Some(r#"{"hello":"world"}"#.to_string()),
            None,
            None,
        )
        .await;

        let __a = state.read();
        let s = __a.default_ref();
        let exec = s.executions.get(arn).unwrap();
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        assert!(exec.output.is_some());
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output, json!({"processed": true}));
    }

    #[tokio::test]
    async fn test_pass_chain() {
        let state = make_state();
        let arn = "arn:aws:states:us-east-1:123456789012:execution:test:exec-1";
        create_execution(&state, arn, Some(r#"{}"#.to_string()));

        let definition = json!({
            "StartAt": "First",
            "States": {
                "First": {
                    "Type": "Pass",
                    "Result": "step1",
                    "ResultPath": "$.first",
                    "Next": "Second"
                },
                "Second": {
                    "Type": "Pass",
                    "Result": "step2",
                    "ResultPath": "$.second",
                    "End": true
                }
            }
        })
        .to_string();

        execute_state_machine(
            state.clone(),
            arn.to_string(),
            definition,
            Some("{}".to_string()),
            None,
            None,
        )
        .await;

        let __a = state.read();
        let s = __a.default_ref();
        let exec = s.executions.get(arn).unwrap();
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output["first"], json!("step1"));
        assert_eq!(output["second"], json!("step2"));
    }

    #[tokio::test]
    async fn test_succeed_state() {
        let state = make_state();
        let arn = "arn:aws:states:us-east-1:123456789012:execution:test:exec-1";
        create_execution(&state, arn, Some(r#"{"data": "value"}"#.to_string()));

        let definition = json!({
            "StartAt": "Done",
            "States": {
                "Done": {
                    "Type": "Succeed"
                }
            }
        })
        .to_string();

        execute_state_machine(
            state.clone(),
            arn.to_string(),
            definition,
            Some(r#"{"data": "value"}"#.to_string()),
            None,
            None,
        )
        .await;

        let __a = state.read();
        let s = __a.default_ref();
        let exec = s.executions.get(arn).unwrap();
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
    }

    #[tokio::test]
    async fn test_fail_state() {
        let state = make_state();
        let arn = "arn:aws:states:us-east-1:123456789012:execution:test:exec-1";
        create_execution(&state, arn, None);

        let definition = json!({
            "StartAt": "FailState",
            "States": {
                "FailState": {
                    "Type": "Fail",
                    "Error": "CustomError",
                    "Cause": "Something went wrong"
                }
            }
        })
        .to_string();

        execute_state_machine(state.clone(), arn.to_string(), definition, None, None, None).await;

        let __a = state.read();
        let s = __a.default_ref();
        let exec = s.executions.get(arn).unwrap();
        assert_eq!(exec.status, ExecutionStatus::Failed);
        assert_eq!(exec.error.as_deref(), Some("CustomError"));
        assert_eq!(exec.cause.as_deref(), Some("Something went wrong"));
    }

    #[tokio::test]
    async fn test_history_events_recorded() {
        let state = make_state();
        let arn = "arn:aws:states:us-east-1:123456789012:execution:test:exec-1";
        create_execution(&state, arn, Some("{}".to_string()));

        let definition = json!({
            "StartAt": "PassState",
            "States": {
                "PassState": {
                    "Type": "Pass",
                    "End": true
                }
            }
        })
        .to_string();

        execute_state_machine(
            state.clone(),
            arn.to_string(),
            definition,
            Some("{}".to_string()),
            None,
            None,
        )
        .await;

        let __a = state.read();
        let s = __a.default_ref();
        let exec = s.executions.get(arn).unwrap();
        let event_types: Vec<&str> = exec
            .history_events
            .iter()
            .map(|e| e.event_type.as_str())
            .collect();
        assert_eq!(
            event_types,
            vec![
                "ExecutionStarted",
                "PassStateEntered",
                "PassStateExited",
                "ExecutionSucceeded"
            ]
        );
    }

    fn drive(state: &SharedStepFunctionsState, arn: &str, def: Value, input: Option<&str>) {
        create_execution(state, arn, input.map(|s| s.to_string()));
        let fut = execute_state_machine(
            state.clone(),
            arn.to_string(),
            def.to_string(),
            input.map(|s| s.to_string()),
            None,
            None,
        );
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(fut);
    }

    fn read_exec<R>(
        state: &SharedStepFunctionsState,
        arn: &str,
        f: impl FnOnce(&Execution) -> R,
    ) -> R {
        let __a = state.read();
        let s = __a.default_ref();
        f(s.executions.get(arn).expect("execution missing"))
    }

    fn arn_for(name: &str) -> String {
        format!("arn:aws:states:us-east-1:123456789012:execution:test:{name}")
    }

    // ── Pass state: InputPath / OutputPath ───────────────────────────

    #[test]
    fn pass_state_input_output_path_select_fields() {
        let state = make_state();
        let arn = arn_for("pass-paths");
        let def = json!({
            "StartAt": "P",
            "States": {
                "P": {
                    "Type": "Pass",
                    "InputPath": "$.inner",
                    "OutputPath": "$.kept",
                    "End": true
                }
            }
        });
        drive(
            &state,
            &arn,
            def,
            Some(r#"{"inner":{"kept":"yes","dropped":true},"sibling":1}"#),
        );

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
            let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
            assert_eq!(output, json!("yes"));
        });
    }

    // ── Succeed / Fail variants ──────────────────────────────────────

    #[test]
    fn succeed_state_honors_input_path_null() {
        let state = make_state();
        let arn = arn_for("succeed-null");
        let def = json!({
            "StartAt": "S",
            "States": { "S": { "Type": "Succeed", "InputPath": "null" } }
        });
        drive(&state, &arn, def, Some(r#"{"a":1}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
            let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
            assert_eq!(output, json!({}));
        });
    }

    #[test]
    fn fail_state_defaults_when_fields_missing() {
        let state = make_state();
        let arn = arn_for("fail-default");
        let def = json!({
            "StartAt": "F",
            "States": { "F": { "Type": "Fail" } }
        });
        drive(&state, &arn, def, None);

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert_eq!(exec.error.as_deref(), Some("States.Fail"));
            assert_eq!(exec.cause.as_deref(), Some(""));
        });
    }

    // ── Choice ───────────────────────────────────────────────────────

    fn choice_def() -> Value {
        json!({
            "StartAt": "C",
            "States": {
                "C": {
                    "Type": "Choice",
                    "Choices": [
                        {
                            "Variable": "$.n",
                            "NumericGreaterThan": 10,
                            "Next": "Big"
                        }
                    ],
                    "Default": "Small"
                },
                "Big":   { "Type": "Pass", "Result": "big",   "End": true },
                "Small": { "Type": "Pass", "Result": "small", "End": true }
            }
        })
    }

    #[test]
    fn choice_routes_to_matching_branch() {
        let state = make_state();
        let arn = arn_for("choice-big");
        drive(&state, &arn, choice_def(), Some(r#"{"n":42}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
            assert_eq!(
                serde_json::from_str::<Value>(exec.output.as_ref().unwrap()).unwrap(),
                json!("big")
            );
        });
    }

    #[test]
    fn choice_falls_through_to_default() {
        let state = make_state();
        let arn = arn_for("choice-default");
        drive(&state, &arn, choice_def(), Some(r#"{"n":3}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
            assert_eq!(
                serde_json::from_str::<Value>(exec.output.as_ref().unwrap()).unwrap(),
                json!("small")
            );
        });
    }

    #[test]
    fn choice_no_match_and_no_default_fails() {
        let state = make_state();
        let arn = arn_for("choice-nomatch");
        let def = json!({
            "StartAt": "C",
            "States": {
                "C": {
                    "Type": "Choice",
                    "Choices": [
                        { "Variable": "$.n", "NumericEquals": 1, "Next": "End1" }
                    ]
                },
                "End1": { "Type": "Pass", "End": true }
            }
        });
        drive(&state, &arn, def, Some(r#"{"n":99}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert_eq!(exec.error.as_deref(), Some("States.NoChoiceMatched"));
        });
    }

    // ── Wait ─────────────────────────────────────────────────────────

    #[test]
    fn wait_seconds_then_advances() {
        let state = make_state();
        let arn = arn_for("wait-secs");
        let def = json!({
            "StartAt": "W",
            "States": {
                "W": { "Type": "Wait", "Seconds": 0, "Next": "Done" },
                "Done": { "Type": "Succeed" }
            }
        });
        drive(&state, &arn, def, Some(r#"{"k":1}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
        });
    }

    #[test]
    fn wait_timestamp_in_past_is_noop() {
        let state = make_state();
        let arn = arn_for("wait-past");
        let def = json!({
            "StartAt": "W",
            "States": {
                "W": {
                    "Type": "Wait",
                    "Timestamp": "2000-01-01T00:00:00Z",
                    "End": true
                }
            }
        });
        drive(&state, &arn, def, Some(r#"{"k":1}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
        });
    }

    #[test]
    fn wait_without_any_duration_falls_through() {
        let state = make_state();
        let arn = arn_for("wait-none");
        let def = json!({
            "StartAt": "W",
            "States": { "W": { "Type": "Wait", "End": true } }
        });
        drive(&state, &arn, def, None);

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
        });
    }

    // ── Parallel ─────────────────────────────────────────────────────

    #[test]
    fn parallel_runs_two_pass_branches_and_collects_results() {
        let state = make_state();
        let arn = arn_for("parallel-ok");
        let def = json!({
            "StartAt": "P",
            "States": {
                "P": {
                    "Type": "Parallel",
                    "End": true,
                    "Branches": [
                        {
                            "StartAt": "A",
                            "States": { "A": { "Type": "Pass", "Result": "a", "End": true } }
                        },
                        {
                            "StartAt": "B",
                            "States": { "B": { "Type": "Pass", "Result": "b", "End": true } }
                        }
                    ]
                }
            }
        });
        drive(&state, &arn, def, Some(r#"{}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
            let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
            assert_eq!(output, json!(["a", "b"]));
        });
    }

    #[test]
    fn parallel_empty_branches_fails() {
        let state = make_state();
        let arn = arn_for("parallel-empty");
        let def = json!({
            "StartAt": "P",
            "States": { "P": { "Type": "Parallel", "Branches": [], "End": true } }
        });
        drive(&state, &arn, def, None);

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert_eq!(exec.error.as_deref(), Some("States.Runtime"));
        });
    }

    // ── Map ──────────────────────────────────────────────────────────

    #[test]
    fn map_iterates_pass_state_over_items_path() {
        let state = make_state();
        let arn = arn_for("map-ok");
        let def = json!({
            "StartAt": "M",
            "States": {
                "M": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "Iterator": {
                        "StartAt": "Item",
                        "States": { "Item": { "Type": "Pass", "End": true } }
                    },
                    "End": true
                }
            }
        });
        drive(&state, &arn, def, Some(r#"{"items":[1,2,3]}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
            let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
            assert_eq!(output, json!([1, 2, 3]));
        });
    }

    // ── Task: unsupported resources / delivery == None ───────────────

    #[test]
    fn task_unsupported_resource_propagates_failure() {
        let state = make_state();
        let arn = arn_for("task-unsupported");
        let def = json!({
            "StartAt": "T",
            "States": {
                "T": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::nothing:here",
                    "End": true
                }
            }
        });
        drive(&state, &arn, def, None);

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert_eq!(exec.error.as_deref(), Some("States.TaskFailed"));
            assert!(exec.cause.as_deref().unwrap().contains("Unsupported"));
        });
    }

    #[test]
    fn task_sqs_send_without_delivery_fails() {
        let state = make_state();
        let arn = arn_for("task-sqs-nodelivery");
        let def = json!({
            "StartAt": "T",
            "States": {
                "T": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::sqs:sendMessage",
                    "Parameters": { "QueueUrl": "http://localhost/123/q", "MessageBody": "hi" },
                    "End": true
                }
            }
        });
        drive(&state, &arn, def, Some("{}"));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert!(exec.cause.as_deref().unwrap().contains("delivery bus"));
        });
    }

    // ── Task: Catch clause ───────────────────────────────────────────

    #[test]
    fn task_catch_routes_error_into_handler() {
        let state = make_state();
        let arn = arn_for("task-catch");
        let def = json!({
            "StartAt": "T",
            "States": {
                "T": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::nothing:here",
                    "Catch": [
                        { "ErrorEquals": ["States.ALL"], "Next": "Handler", "ResultPath": "$.err" }
                    ],
                    "End": true
                },
                "Handler": { "Type": "Pass", "End": true }
            }
        });
        drive(&state, &arn, def, Some(r#"{"orig":"v"}"#));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Succeeded);
            let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
            // Handler is Pass with no Result — effective input flows through.
            assert_eq!(output["orig"], json!("v"));
            assert_eq!(output["err"]["Error"], json!("States.TaskFailed"));
        });
    }

    // ── Top-level errors: definition / start-at / missing states ─────

    #[test]
    fn invalid_definition_json_fails_execution() {
        let state = make_state();
        let arn = arn_for("bad-json");
        create_execution(&state, &arn, None);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(execute_state_machine(
            state.clone(),
            arn.clone(),
            "not json{".to_string(),
            None,
            None,
            None,
        ));

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert_eq!(exec.error.as_deref(), Some("States.Runtime"));
            assert!(exec.cause.as_deref().unwrap().contains("Failed to parse"));
        });
    }

    #[test]
    fn missing_start_at_fails_execution() {
        let state = make_state();
        let arn = arn_for("no-startat");
        let def = json!({ "States": { "X": { "Type": "Succeed" } } });
        drive(&state, &arn, def, None);

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert!(exec.cause.as_deref().unwrap().contains("StartAt"));
        });
    }

    #[test]
    fn next_state_not_found_fails_execution() {
        let state = make_state();
        let arn = arn_for("dangling-next");
        let def = json!({
            "StartAt": "A",
            "States": { "A": { "Type": "Pass", "Next": "DoesNotExist" } }
        });
        drive(&state, &arn, def, None);

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert!(exec.cause.as_deref().unwrap().contains("not found"));
        });
    }

    #[test]
    fn unsupported_state_type_fails_execution() {
        let state = make_state();
        let arn = arn_for("bad-type");
        let def = json!({
            "StartAt": "X",
            "States": { "X": { "Type": "WatChoo", "End": true } }
        });
        drive(&state, &arn, def, None);

        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert!(exec
                .cause
                .as_deref()
                .unwrap()
                .contains("Unsupported state type"));
        });
    }

    // ── Pure helpers ─────────────────────────────────────────────────

    #[test]
    fn apply_parameters_substitutes_json_path_refs() {
        let template = json!({
            "literal": "constant",
            "ref.$": "$.user.id",
            "nested": { "inner.$": "$.user.name" },
            "list": [ { "x.$": "$.user.id" } ]
        });
        let input = json!({ "user": { "id": 42, "name": "zoe" } });
        let out = apply_parameters(&template, &input);
        assert_eq!(out["literal"], json!("constant"));
        assert_eq!(out["ref"], json!(42));
        assert_eq!(out["nested"]["inner"], json!("zoe"));
        assert_eq!(out["list"][0]["x"], json!(42));
    }

    #[test]
    fn next_state_returns_end_name_or_error() {
        match next_state(&json!({ "End": true })) {
            NextState::End => {}
            _ => panic!("expected End"),
        }
        match next_state(&json!({ "Next": "A" })) {
            NextState::Name(n) => assert_eq!(n, "A"),
            _ => panic!("expected Name"),
        }
        match next_state(&json!({})) {
            NextState::Error(_) => {}
            _ => panic!("expected Error"),
        }
    }

    #[test]
    fn apply_state_catcher_matches_wildcard_and_stashes_error() {
        let state_def = json!({
            "Catch": [
                { "ErrorEquals": ["States.ALL"], "Next": "H", "ResultPath": "$.caught" }
            ]
        });
        let input = json!({ "a": 1 });
        let (next, new_input) =
            apply_state_catcher(&state_def, &input, "Boom", "it exploded").unwrap();
        assert_eq!(next, "H");
        assert_eq!(new_input["a"], json!(1));
        assert_eq!(new_input["caught"]["Error"], json!("Boom"));
        assert_eq!(new_input["caught"]["Cause"], json!("it exploded"));
    }

    #[test]
    fn apply_state_catcher_returns_none_without_match() {
        let state_def = json!({
            "Catch": [
                { "ErrorEquals": ["Specific.Error"], "Next": "H" }
            ]
        });
        let input = json!({});
        assert!(apply_state_catcher(&state_def, &input, "Other", "why").is_none());
    }

    #[test]
    fn queue_url_to_arn_parses_account_and_name() {
        assert_eq!(
            queue_url_to_arn("http://sqs.local:4566/123456789012/my-queue"),
            "arn:aws:sqs:us-east-1:123456789012:my-queue"
        );
    }

    #[test]
    fn queue_url_to_arn_falls_back_for_unparseable_input() {
        assert_eq!(queue_url_to_arn("bad"), "bad");
    }

    #[test]
    fn md5_hex_is_deterministic_and_32_chars() {
        let a = md5_hex("hello");
        let b = md5_hex("hello");
        assert_eq!(a, b);
        assert_eq!(a.len(), 32);
        assert_ne!(a, md5_hex("world"));
    }

    #[test]
    fn apply_update_expression_sets_direct_and_aliased_attrs() {
        let mut item: HashMap<String, Value> = HashMap::new();
        item.insert("id".to_string(), json!({"S": "1"}));

        let mut attr_values = serde_json::Map::new();
        attr_values.insert(":n".to_string(), json!({"S": "Alice"}));
        attr_values.insert(":c".to_string(), json!({"N": "5"}));

        let mut attr_names = serde_json::Map::new();
        attr_names.insert("#name".to_string(), json!("name"));

        apply_update_expression(
            &mut item,
            "SET #name = :n, count = :c",
            &attr_values,
            &attr_names,
        );

        assert_eq!(item.get("name").unwrap(), &json!({"S": "Alice"}));
        assert_eq!(item.get("count").unwrap(), &json!({"N": "5"}));
        assert_eq!(item.get("id").unwrap(), &json!({"S": "1"}));
    }

    #[test]
    fn apply_update_expression_accepts_lowercase_set_keyword() {
        let mut item: HashMap<String, Value> = HashMap::new();
        let mut attr_values = serde_json::Map::new();
        attr_values.insert(":v".to_string(), json!({"S": "x"}));
        apply_update_expression(
            &mut item,
            "set field = :v",
            &attr_values,
            &serde_json::Map::new(),
        );
        assert_eq!(item.get("field").unwrap(), &json!({"S": "x"}));
    }

    // ── DynamoDB invoke: error paths without delivery bus ────────────

    #[test]
    fn task_dynamodb_get_item_without_state_fails() {
        let state = make_state();
        let arn = arn_for("ddb-get-nostate");
        let def = json!({
            "StartAt": "T",
            "States": {
                "T": {
                    "Type": "Task",
                    "Resource": "arn:aws:states:::dynamodb:getItem",
                    "Parameters": { "TableName": "t", "Key": { "id": { "S": "1" } } },
                    "End": true
                }
            }
        });
        drive(&state, &arn, def, Some("{}"));
        read_exec(&state, &arn, |exec| {
            assert_eq!(exec.status, ExecutionStatus::Failed);
            assert!(exec.cause.as_deref().unwrap().contains("DynamoDB"));
        });
    }

    // ── Terminal guards on succeed/fail helpers ──────────────────────

    #[test]
    fn succeed_execution_is_noop_when_already_terminal() {
        let state = make_state();
        let arn = "arn:aws:states:us-east-1:123456789012:execution:test:already";
        create_execution(&state, arn, None);
        {
            let mut __a = state.write();
            let s = __a.default_mut();
            s.executions.get_mut(arn).unwrap().status = ExecutionStatus::Failed;
        }
        succeed_execution(&state, arn, &json!({"x":1}));
        let __a = state.read();
        let s = __a.default_ref();
        let exec = s.executions.get(arn).unwrap();
        assert_eq!(exec.status, ExecutionStatus::Failed);
        assert!(exec.output.is_none());
    }

    #[test]
    fn fail_execution_is_noop_when_already_terminal() {
        let state = make_state();
        let arn = "arn:aws:states:us-east-1:123456789012:execution:test:already2";
        create_execution(&state, arn, None);
        {
            let mut __a = state.write();
            let s = __a.default_mut();
            s.executions.get_mut(arn).unwrap().status = ExecutionStatus::Succeeded;
        }
        fail_execution(&state, arn, "Oops", "nope");
        let __a = state.read();
        let s = __a.default_ref();
        let exec = s.executions.get(arn).unwrap();
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        assert!(exec.error.is_none());
    }

    // ── Pass state with ResultPath ──

    #[test]
    fn pass_state_result_path_merges_into_input() {
        let state = make_state();
        let arn = arn_for("result-path");
        let def = json!({
            "StartAt": "P",
            "States": {
                "P": {"Type": "Pass", "Result": {"x": 2}, "ResultPath": "$.data", "End": true}
            }
        });
        drive(&state, &arn, def, Some(r#"{"a":1}"#));
        let output = read_exec(&state, &arn, |e| e.output.clone().unwrap_or_default());
        let v: Value = serde_json::from_str(&output).unwrap();
        assert_eq!(v["a"], 1);
        assert_eq!(v["data"]["x"], 2);
    }

    // ── Choice with many operators ──

    #[test]
    fn choice_string_greater_than_equals() {
        let state = make_state();
        let arn = arn_for("choice-sgte");
        let def = json!({
            "StartAt": "C",
            "States": {
                "C": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.val", "StringGreaterThanEquals": "apple", "Next": "End"}
                    ],
                    "Default": "Fail"
                },
                "End": {"Type": "Pass", "End": true},
                "Fail": {"Type": "Fail"}
            }
        });
        drive(&state, &arn, def, Some(r#"{"val":"banana"}"#));
        let status = read_exec(&state, &arn, |e| e.status);
        assert_eq!(status, ExecutionStatus::Succeeded);
    }

    #[test]
    fn choice_is_present_and_is_null() {
        let state = make_state();
        let arn = arn_for("choice-ispres");
        let def = json!({
            "StartAt": "C",
            "States": {
                "C": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.foo", "IsPresent": true, "Next": "End"}
                    ],
                    "Default": "Fail"
                },
                "End": {"Type": "Pass", "End": true},
                "Fail": {"Type": "Fail"}
            }
        });
        drive(&state, &arn, def, Some(r#"{"foo":null}"#));
        assert_eq!(
            read_exec(&state, &arn, |e| e.status),
            ExecutionStatus::Succeeded
        );
    }

    #[test]
    fn choice_or_short_circuits() {
        let state = make_state();
        let arn = arn_for("choice-or");
        let def = json!({
            "StartAt": "C",
            "States": {
                "C": {
                    "Type": "Choice",
                    "Choices": [{
                        "Or": [
                            {"Variable": "$.x", "NumericEquals": 99},
                            {"Variable": "$.y", "StringEquals": "b"}
                        ],
                        "Next": "End"
                    }],
                    "Default": "Fail"
                },
                "End": {"Type": "Pass", "End": true},
                "Fail": {"Type": "Fail"}
            }
        });
        drive(&state, &arn, def, Some(r#"{"x":1,"y":"b"}"#));
        assert_eq!(
            read_exec(&state, &arn, |e| e.status),
            ExecutionStatus::Succeeded
        );
    }

    #[test]
    fn choice_not_negates() {
        let state = make_state();
        let arn = arn_for("choice-not");
        let def = json!({
            "StartAt": "C",
            "States": {
                "C": {
                    "Type": "Choice",
                    "Choices": [{
                        "Not": {"Variable": "$.x", "NumericEquals": 99},
                        "Next": "End"
                    }],
                    "Default": "Fail"
                },
                "End": {"Type": "Pass", "End": true},
                "Fail": {"Type": "Fail"}
            }
        });
        drive(&state, &arn, def, Some(r#"{"x":1}"#));
        assert_eq!(
            read_exec(&state, &arn, |e| e.status),
            ExecutionStatus::Succeeded
        );
    }

    #[test]
    fn choice_boolean_equals() {
        let state = make_state();
        let arn = arn_for("choice-bool");
        let def = json!({
            "StartAt": "C",
            "States": {
                "C": {
                    "Type": "Choice",
                    "Choices": [
                        {"Variable": "$.ok", "BooleanEquals": true, "Next": "End"}
                    ],
                    "Default": "Fail"
                },
                "End": {"Type": "Pass", "End": true},
                "Fail": {"Type": "Fail"}
            }
        });
        drive(&state, &arn, def, Some(r#"{"ok":true}"#));
        assert_eq!(
            read_exec(&state, &arn, |e| e.status),
            ExecutionStatus::Succeeded
        );
    }

    // ── Wait with SecondsPath ──

    #[test]
    fn wait_seconds_path_uses_input_value() {
        let state = make_state();
        let arn = arn_for("wait-sp");
        let def = json!({
            "StartAt": "W",
            "States": {
                "W": {"Type": "Wait", "SecondsPath": "$.wait", "End": true}
            }
        });
        drive(&state, &arn, def, Some(r#"{"wait":0}"#));
        assert_eq!(
            read_exec(&state, &arn, |e| e.status),
            ExecutionStatus::Succeeded
        );
    }

    // ── Map state with empty input array ──

    #[test]
    fn map_state_empty_array_succeeds() {
        let state = make_state();
        let arn = arn_for("map-empty");
        let def = json!({
            "StartAt": "M",
            "States": {
                "M": {
                    "Type": "Map",
                    "ItemsPath": "$.items",
                    "ItemProcessor": {
                        "StartAt": "P",
                        "States": {
                            "P": {"Type": "Pass", "End": true}
                        }
                    },
                    "End": true
                }
            }
        });
        drive(&state, &arn, def, Some(r#"{"items":[]}"#));
        assert_eq!(
            read_exec(&state, &arn, |e| e.status),
            ExecutionStatus::Succeeded
        );
    }

    // ── Fail state with Error + Cause ──

    #[test]
    fn fail_state_with_explicit_error_and_cause() {
        let state = make_state();
        let arn = arn_for("fail-fields");
        create_execution(&state, &arn, None);
        let def = json!({
            "StartAt": "F",
            "States": {
                "F": {"Type": "Fail", "Error": "MyError", "Cause": "my cause"}
            }
        });
        drive(&state, &arn, def, None);
        let status = read_exec(&state, &arn, |e| e.status);
        assert_eq!(status, ExecutionStatus::Failed);
        let err = read_exec(&state, &arn, |e| e.error.clone().unwrap_or_default());
        assert_eq!(err, "MyError");
    }
}
