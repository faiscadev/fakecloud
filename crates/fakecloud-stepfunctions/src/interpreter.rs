use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};
use tracing::debug;

use fakecloud_core::delivery::DeliveryBus;

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

    let start_at = match def["StartAt"].as_str() {
        Some(s) => s.to_string(),
        None => {
            fail_execution(
                &state,
                &execution_arn,
                "States.Runtime",
                "Missing StartAt in definition",
            );
            return;
        }
    };

    let states = match def.get("States") {
        Some(s) => s,
        None => {
            fail_execution(
                &state,
                &execution_arn,
                "States.Runtime",
                "Missing States in definition",
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

    let mut current_state = start_at;
    let mut effective_input = raw_input;
    let mut iteration = 0;
    let max_iterations = 500; // safety limit

    loop {
        iteration += 1;
        if iteration > max_iterations {
            fail_execution(
                &state,
                &execution_arn,
                "States.Runtime",
                "Maximum number of state transitions exceeded",
            );
            return;
        }

        let state_def = match states.get(&current_state) {
            Some(s) => s.clone(),
            None => {
                fail_execution(
                    &state,
                    &execution_arn,
                    "States.Runtime",
                    &format!("State '{current_state}' not found in definition"),
                );
                return;
            }
        };

        let state_type = match state_def["Type"].as_str() {
            Some(t) => t.to_string(),
            None => {
                fail_execution(
                    &state,
                    &execution_arn,
                    "States.Runtime",
                    &format!("State '{current_state}' missing Type field"),
                );
                return;
            }
        };

        debug!(
            execution_arn = %execution_arn,
            state = %current_state,
            state_type = %state_type,
            "Executing state"
        );

        match state_type.as_str() {
            "Pass" => {
                let entered_event_id = add_event(
                    &state,
                    &execution_arn,
                    "PassStateEntered",
                    0,
                    json!({
                        "name": current_state,
                        "input": serde_json::to_string(&effective_input).unwrap_or_default(),
                    }),
                );

                let result = execute_pass_state(&state_def, &effective_input);

                add_event(
                    &state,
                    &execution_arn,
                    "PassStateExited",
                    entered_event_id,
                    json!({
                        "name": current_state,
                        "output": serde_json::to_string(&result).unwrap_or_default(),
                    }),
                );

                effective_input = result;

                match next_state(&state_def) {
                    NextState::Name(next) => current_state = next,
                    NextState::End => {
                        succeed_execution(&state, &execution_arn, &effective_input);
                        return;
                    }
                    NextState::Error(msg) => {
                        fail_execution(&state, &execution_arn, "States.Runtime", &msg);
                        return;
                    }
                }
            }

            "Succeed" => {
                add_event(
                    &state,
                    &execution_arn,
                    "SucceedStateEntered",
                    0,
                    json!({
                        "name": current_state,
                        "input": serde_json::to_string(&effective_input).unwrap_or_default(),
                    }),
                );

                // Apply InputPath and OutputPath
                let input_path = state_def["InputPath"].as_str();
                let output_path = state_def["OutputPath"].as_str();

                let processed = if input_path == Some("null") {
                    json!({})
                } else {
                    apply_input_path(&effective_input, input_path)
                };

                let output = if output_path == Some("null") {
                    json!({})
                } else {
                    apply_output_path(&processed, output_path)
                };

                add_event(
                    &state,
                    &execution_arn,
                    "SucceedStateExited",
                    0,
                    json!({
                        "name": current_state,
                        "output": serde_json::to_string(&output).unwrap_or_default(),
                    }),
                );

                succeed_execution(&state, &execution_arn, &output);
                return;
            }

            "Fail" => {
                let error = state_def["Error"]
                    .as_str()
                    .unwrap_or("States.Fail")
                    .to_string();
                let cause = state_def["Cause"].as_str().unwrap_or("").to_string();

                add_event(
                    &state,
                    &execution_arn,
                    "FailStateEntered",
                    0,
                    json!({
                        "name": current_state,
                        "input": serde_json::to_string(&effective_input).unwrap_or_default(),
                    }),
                );

                fail_execution(&state, &execution_arn, &error, &cause);
                return;
            }

            "Task" => {
                let entered_event_id = add_event(
                    &state,
                    &execution_arn,
                    "TaskStateEntered",
                    0,
                    json!({
                        "name": current_state,
                        "input": serde_json::to_string(&effective_input).unwrap_or_default(),
                    }),
                );

                let result = execute_task_state(
                    &state_def,
                    &effective_input,
                    &delivery,
                    &state,
                    &execution_arn,
                    entered_event_id,
                )
                .await;

                match result {
                    Ok(output) => {
                        add_event(
                            &state,
                            &execution_arn,
                            "TaskStateExited",
                            entered_event_id,
                            json!({
                                "name": current_state,
                                "output": serde_json::to_string(&output).unwrap_or_default(),
                            }),
                        );

                        effective_input = output;

                        match next_state(&state_def) {
                            NextState::Name(next) => current_state = next,
                            NextState::End => {
                                succeed_execution(&state, &execution_arn, &effective_input);
                                return;
                            }
                            NextState::Error(msg) => {
                                fail_execution(&state, &execution_arn, "States.Runtime", &msg);
                                return;
                            }
                        }
                    }
                    Err((error, cause)) => {
                        // Try Catch
                        let catchers = state_def["Catch"].as_array().cloned().unwrap_or_default();

                        if let Some((next, result_path)) = find_catcher(&catchers, &error) {
                            let error_output = json!({
                                "Error": error,
                                "Cause": cause,
                            });
                            effective_input = apply_result_path(
                                &effective_input,
                                &error_output,
                                result_path.as_deref(),
                            );
                            current_state = next;
                        } else {
                            fail_execution(&state, &execution_arn, &error, &cause);
                            return;
                        }
                    }
                }
            }

            other => {
                fail_execution(
                    &state,
                    &execution_arn,
                    "States.Runtime",
                    &format!("Unsupported state type: '{other}'"),
                );
                return;
            }
        }
    }
}

/// Execute a Pass state: apply InputPath, use Result if present, apply ResultPath and OutputPath.
fn execute_pass_state(state_def: &Value, input: &Value) -> Value {
    let input_path = state_def["InputPath"].as_str();
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    // Step 1: Apply InputPath
    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)
    };

    // Step 2: Determine result (Pass can have a literal "Result" field)
    let result = if let Some(r) = state_def.get("Result") {
        r.clone()
    } else {
        effective_input.clone()
    };

    // Step 3: Apply ResultPath (merge result into original input, not effective_input)
    let after_result = if result_path == Some("null") {
        input.clone()
    } else {
        apply_result_path(input, &result, result_path)
    };

    // Step 4: Apply OutputPath
    if output_path == Some("null") {
        json!({})
    } else {
        apply_output_path(&after_result, output_path)
    }
}

/// Execute a Task state: invoke the resource (Lambda), apply I/O processing, handle Retry.
async fn execute_task_state(
    state_def: &Value,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
    entered_event_id: i64,
) -> Result<Value, (String, String)> {
    let resource = state_def["Resource"].as_str().unwrap_or("").to_string();

    let input_path = state_def["InputPath"].as_str();
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    // Step 1: Apply InputPath
    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)
    };

    // Step 2: Apply Parameters (template with .$ suffix for JsonPath extraction)
    let task_input = if let Some(params) = state_def.get("Parameters") {
        apply_parameters(params, &effective_input)
    } else {
        effective_input
    };

    // Retry configuration
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

        let invoke_result =
            invoke_resource(&resource, &task_input, delivery, timeout_seconds).await;

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

                // Apply ResultSelector if present
                let selected = if let Some(selector) = state_def.get("ResultSelector") {
                    apply_parameters(selector, &result)
                } else {
                    result
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

                // Check Retry
                if let Some(delay_ms) = should_retry(&retriers, &error, attempt) {
                    attempt += 1;
                    // Cap the actual delay for testing (don't wait minutes)
                    let actual_delay = delay_ms.min(5000);
                    tokio::time::sleep(tokio::time::Duration::from_millis(actual_delay)).await;
                    continue;
                }

                return Err((error, cause));
            }
        }
    }
}

/// Invoke a resource (Lambda function or SDK integration).
async fn invoke_resource(
    resource: &str,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    // Lambda direct invocation: arn:aws:lambda:...
    if resource.contains(":lambda:") && resource.contains(":function:") {
        return invoke_lambda_direct(resource, input, delivery, timeout_seconds).await;
    }

    // SDK integration: arn:aws:states:::lambda:invoke
    if resource.starts_with("arn:aws:states:::lambda:invoke") {
        let function_name = input["FunctionName"]
            .as_str()
            .or_else(|| input["Payload"].as_object().and(None))
            .unwrap_or("");
        let payload = if let Some(p) = input.get("Payload") {
            p.clone()
        } else {
            input.clone()
        };
        return invoke_lambda_direct(function_name, &payload, delivery, timeout_seconds).await;
    }

    Err((
        "States.TaskFailed".to_string(),
        format!("Unsupported resource: {resource}"),
    ))
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
                    // JsonPath reference
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

fn add_event(
    state: &SharedStepFunctionsState,
    execution_arn: &str,
    event_type: &str,
    previous_event_id: i64,
    details: Value,
) -> i64 {
    let mut s = state.write();
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
    let output_str = serde_json::to_string(output).unwrap_or_default();

    add_event(
        state,
        execution_arn,
        "ExecutionSucceeded",
        0,
        json!({ "output": output_str }),
    );

    let mut s = state.write();
    if let Some(exec) = s.executions.get_mut(execution_arn) {
        exec.status = ExecutionStatus::Succeeded;
        exec.output = Some(output_str);
        exec.stop_date = Some(Utc::now());
    }
}

fn fail_execution(state: &SharedStepFunctionsState, execution_arn: &str, error: &str, cause: &str) {
    add_event(
        state,
        execution_arn,
        "ExecutionFailed",
        0,
        json!({ "error": error, "cause": cause }),
    );

    let mut s = state.write();
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
    use crate::state::{Execution, StepFunctionsState};
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn make_state() -> SharedStepFunctionsState {
        Arc::new(RwLock::new(StepFunctionsState::new(
            "123456789012",
            "us-east-1",
        )))
    }

    fn create_execution(state: &SharedStepFunctionsState, arn: &str, input: Option<String>) {
        let mut s = state.write();
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
        )
        .await;

        let s = state.read();
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
        )
        .await;

        let s = state.read();
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
        )
        .await;

        let s = state.read();
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

        execute_state_machine(state.clone(), arn.to_string(), definition, None, None).await;

        let s = state.read();
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
        )
        .await;

        let s = state.read();
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
}
