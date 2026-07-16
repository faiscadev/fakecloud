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
            parent_execution_arn: None,
            is_sync: false,
            billed_duration_ms: None,
            billed_memory_mb: None,
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

    execute_state_machine(
        state.clone(),
        arn.to_string(),
        definition,
        None,
        None,
        None,
        None,
        None,
    )
    .await;

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
        None,
        None,
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(fut);
}

fn read_exec<R>(state: &SharedStepFunctionsState, arn: &str, f: impl FnOnce(&Execution) -> R) -> R {
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

#[test]
fn map_tolerated_failure_percentage_allows_at_threshold() {
    let state = make_state();
    let arn = arn_for("map-tolerated-ok");
    let def = json!({
        "StartAt": "M",
        "States": {
            "M": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "Iterator": {
                    "StartAt": "Item",
                    "States": {
                        "Item": {
                            "Type": "Choice",
                            "Choices": [
                                { "Variable": "$", "NumericEquals": 2, "Next": "FailItem" }
                            ],
                            "Default": "Ok"
                        },
                        "FailItem": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::nothing:here",
                            "End": true
                        },
                        "Ok": { "Type": "Pass", "End": true }
                    }
                },
                "ToleratedFailurePercentage": 50,
                "End": true
            }
        }
    });
    drive(&state, &arn, def, Some(r#"{"items":[1,2]}"#));

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
    });
}

#[test]
fn map_tolerated_failure_percentage_fails_when_exceeded() {
    let state = make_state();
    let arn = arn_for("map-tolerated-fail");
    let def = json!({
        "StartAt": "M",
        "States": {
            "M": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "Iterator": {
                    "StartAt": "Item",
                    "States": {
                        "Item": {
                            "Type": "Choice",
                            "Choices": [
                                { "Variable": "$", "NumericGreaterThan": 1, "Next": "FailItem" }
                            ],
                            "Default": "Ok"
                        },
                        "FailItem": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::nothing:here",
                            "End": true
                        },
                        "Ok": { "Type": "Pass", "End": true }
                    }
                },
                "ToleratedFailurePercentage": 50,
                "End": true
            }
        }
    });
    drive(&state, &arn, def, Some(r#"{"items":[1,2,3]}"#));

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Failed);
    });
}

#[test]
fn map_max_concurrency_path_reads_from_input() {
    let state = make_state();
    let arn = arn_for("map-concurrency");
    let def = json!({
        "StartAt": "M",
        "States": {
            "M": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "MaxConcurrencyPath": "$.maxConcurrency",
                "Iterator": {
                    "StartAt": "Item",
                    "States": { "Item": { "Type": "Pass", "End": true } }
                },
                "End": true
            }
        }
    });
    drive(
        &state,
        &arn,
        def,
        Some(r#"{"items":[1,2,3],"maxConcurrency":2}"#),
    );

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output, json!([1, 2, 3]));
    });
}

#[test]
fn map_item_batcher_groups_items() {
    let state = make_state();
    let arn = arn_for("map-batcher");
    let def = json!({
        "StartAt": "M",
        "States": {
            "M": {
                "Type": "Map",
                "ItemsPath": "$.items",
                "ItemBatcher": {
                    "MaxItemsPerBatch": 2,
                    "BatchInput": { "extra": "data" }
                },
                "Iterator": {
                    "StartAt": "Item",
                    "States": { "Item": { "Type": "Pass", "End": true } }
                },
                "End": true
            }
        }
    });
    drive(&state, &arn, def, Some(r#"{"items":[1,2,3,4]}"#));

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        // Batched into 2 groups of 2 items each
        assert_eq!(output.as_array().unwrap().len(), 2);
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
    let out = apply_parameters(&template, &input, None);
    assert_eq!(out["literal"], json!("constant"));
    assert_eq!(out["ref"], json!(42));
    assert_eq!(out["nested"]["inner"], json!("zoe"));
    assert_eq!(out["list"][0]["x"], json!(42));
}

#[test]
fn apply_parameters_resolves_context_object() {
    let template = json!({
        "token.$": "$$.Task.Token",
        "exec.$": "$$.Execution.Id",
        "literal": "static"
    });
    let input = json!({ "user": { "id": 42 } });
    let context = json!({
        "Task": { "Token": "abc123" },
        "Execution": { "Id": "arn:aws:states:us-east-1:123:execution:sm:exec" }
    });
    let out = apply_parameters(&template, &input, Some(&context));
    assert_eq!(out["token"], json!("abc123"));
    assert_eq!(
        out["exec"],
        json!("arn:aws:states:us-east-1:123:execution:sm:exec")
    );
    assert_eq!(out["literal"], json!("static"));
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
        apply_state_catcher(&state_def, &input, "Boom", "it exploded", true).unwrap();
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
    assert!(apply_state_catcher(&state_def, &input, "Other", "why", true).is_none());
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

#[test]
fn apply_update_expression_set_arithmetic_increments_counter() {
    let mut item: HashMap<String, Value> = HashMap::new();
    item.insert("count".to_string(), json!({"N": "10"}));
    let mut attr_values = serde_json::Map::new();
    attr_values.insert(":inc".to_string(), json!({"N": "3"}));
    apply_update_expression(
        &mut item,
        "SET count = count + :inc",
        &attr_values,
        &serde_json::Map::new(),
    );
    assert_eq!(item.get("count").unwrap(), &json!({"N": "13"}));
}

#[test]
fn apply_update_expression_set_decrement() {
    let mut item: HashMap<String, Value> = HashMap::new();
    item.insert("count".to_string(), json!({"N": "10"}));
    let mut attr_values = serde_json::Map::new();
    attr_values.insert(":d".to_string(), json!({"N": "4"}));
    apply_update_expression(
        &mut item,
        "SET count = count - :d",
        &attr_values,
        &serde_json::Map::new(),
    );
    assert_eq!(item.get("count").unwrap(), &json!({"N": "6"}));
}

#[test]
fn apply_update_expression_remove_drops_attributes() {
    let mut item: HashMap<String, Value> = HashMap::new();
    item.insert("a".to_string(), json!({"S": "x"}));
    item.insert("b".to_string(), json!({"S": "y"}));
    item.insert("c".to_string(), json!({"S": "z"}));
    apply_update_expression(
        &mut item,
        "REMOVE a, c",
        &serde_json::Map::new(),
        &serde_json::Map::new(),
    );
    assert!(!item.contains_key("a"));
    assert!(item.contains_key("b"));
    assert!(!item.contains_key("c"));
}

#[test]
fn apply_update_expression_add_increments_existing_or_initializes() {
    // Existing attribute -> sum
    let mut item: HashMap<String, Value> = HashMap::new();
    item.insert("count".to_string(), json!({"N": "5"}));
    let mut attr_values = serde_json::Map::new();
    attr_values.insert(":inc".to_string(), json!({"N": "2"}));
    apply_update_expression(
        &mut item,
        "ADD count :inc",
        &attr_values,
        &serde_json::Map::new(),
    );
    assert_eq!(item.get("count").unwrap(), &json!({"N": "7"}));

    // Absent attribute -> initialized to value
    let mut item2: HashMap<String, Value> = HashMap::new();
    apply_update_expression(
        &mut item2,
        "ADD count :inc",
        &attr_values,
        &serde_json::Map::new(),
    );
    assert_eq!(item2.get("count").unwrap(), &json!({"N": "2"}));
}

#[test]
fn apply_update_expression_delete_removes_set_elements() {
    let mut item: HashMap<String, Value> = HashMap::new();
    item.insert("tags".to_string(), json!({"SS": ["a", "b", "c"]}));
    let mut attr_values = serde_json::Map::new();
    attr_values.insert(":rm".to_string(), json!({"SS": ["b"]}));
    apply_update_expression(
        &mut item,
        "DELETE tags :rm",
        &attr_values,
        &serde_json::Map::new(),
    );
    assert_eq!(item.get("tags").unwrap(), &json!({"SS": ["a", "c"]}));
}

#[test]
fn apply_update_expression_if_not_exists_initializes_only_when_absent() {
    // Absent -> initialize.
    let mut item: HashMap<String, Value> = HashMap::new();
    let mut attr_values = serde_json::Map::new();
    attr_values.insert(":zero".to_string(), json!({"N": "0"}));
    apply_update_expression(
        &mut item,
        "SET count = if_not_exists(count, :zero)",
        &attr_values,
        &serde_json::Map::new(),
    );
    assert_eq!(item.get("count").unwrap(), &json!({"N": "0"}));

    // Present -> preserve existing.
    let mut item2: HashMap<String, Value> = HashMap::new();
    item2.insert("count".to_string(), json!({"N": "42"}));
    apply_update_expression(
        &mut item2,
        "SET count = if_not_exists(count, :zero)",
        &attr_values,
        &serde_json::Map::new(),
    );
    assert_eq!(item2.get("count").unwrap(), &json!({"N": "42"}));
}

#[test]
fn apply_update_expression_combines_clauses() {
    let mut item: HashMap<String, Value> = HashMap::new();
    item.insert("a".to_string(), json!({"S": "old"}));
    item.insert("b".to_string(), json!({"N": "1"}));
    item.insert("c".to_string(), json!({"S": "drop"}));
    let mut attr_values = serde_json::Map::new();
    attr_values.insert(":new".to_string(), json!({"S": "new"}));
    attr_values.insert(":one".to_string(), json!({"N": "1"}));
    apply_update_expression(
        &mut item,
        "SET a = :new ADD b :one REMOVE c",
        &attr_values,
        &serde_json::Map::new(),
    );
    assert_eq!(item.get("a").unwrap(), &json!({"S": "new"}));
    assert_eq!(item.get("b").unwrap(), &json!({"N": "2"}));
    assert!(!item.contains_key("c"));
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

fn make_exec(status: ExecutionStatus) -> Execution {
    Execution {
        execution_arn: "arn:aws:states:us-east-1:123456789012:execution:test:x".to_string(),
        state_machine_arn: "arn:aws:states:us-east-1:123456789012:stateMachine:test".to_string(),
        state_machine_name: "test".to_string(),
        name: "x".to_string(),
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

// Deterministic unit test of the write-guard re-check: a concurrent
// StopExecution sets Aborted in the TOCTOU window, and the late terminal
// transition from the interpreter must NOT overwrite it.
#[test]
fn terminal_transition_skips_when_aborted() {
    let mut exec = make_exec(ExecutionStatus::Aborted);
    exec.stop_date = Some(Utc::now());
    let applied = apply_terminal_transition_if_running(&mut exec, |e| {
        e.status = ExecutionStatus::Succeeded;
        e.output = Some("clobbered".to_string());
    });
    assert!(
        !applied,
        "must not transition an already-terminal execution"
    );
    assert_eq!(exec.status, ExecutionStatus::Aborted);
    assert!(exec.output.is_none(), "Aborted output must be preserved");
}

#[test]
fn terminal_transition_skips_when_timed_out() {
    let mut exec = make_exec(ExecutionStatus::TimedOut);
    let applied = apply_terminal_transition_if_running(&mut exec, |e| {
        e.status = ExecutionStatus::Failed;
    });
    assert!(!applied);
    assert_eq!(exec.status, ExecutionStatus::TimedOut);
}

#[test]
fn terminal_transition_applies_when_running() {
    let mut exec = make_exec(ExecutionStatus::Running);
    let applied = apply_terminal_transition_if_running(&mut exec, |e| {
        e.status = ExecutionStatus::Succeeded;
        e.output = Some("ok".to_string());
    });
    assert!(applied);
    assert_eq!(exec.status, ExecutionStatus::Succeeded);
    assert_eq!(exec.output.as_deref(), Some("ok"));
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

// ── Task: lambda:invoke integration — function errors, transport, envelope ──

use fakecloud_core::delivery::{DeliveryBus, LambdaDelivery};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Stub `LambdaDelivery` that returns canned results, one per call. Once the
/// canned list is exhausted it repeats the last entry — so "fail once, then
/// succeed forever" needs only two entries.
struct StubLambda {
    responses: Vec<Result<Vec<u8>, String>>,
    calls: Arc<AtomicUsize>,
}

impl StubLambda {
    fn bus(responses: Vec<Result<Vec<u8>, String>>) -> (Arc<DeliveryBus>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let stub = Arc::new(StubLambda {
            responses,
            calls: calls.clone(),
        });
        (Arc::new(DeliveryBus::new().with_lambda(stub)), calls)
    }
}

impl LambdaDelivery for StubLambda {
    fn invoke_lambda(
        &self,
        _function_arn: &str,
        _payload: &str,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>> {
        let idx = self.calls.fetch_add(1, Ordering::SeqCst);
        let resp = self
            .responses
            .get(idx)
            .or_else(|| self.responses.last())
            .cloned()
            .unwrap_or_else(|| Ok(b"{}".to_vec()));
        Box::pin(async move { resp })
    }
}

fn drive_with_delivery(
    state: &SharedStepFunctionsState,
    arn: &str,
    def: Value,
    input: Option<&str>,
    delivery: Arc<DeliveryBus>,
) {
    create_execution(state, arn, input.map(|s| s.to_string()));
    let fut = execute_state_machine(
        state.clone(),
        arn.to_string(),
        def.to_string(),
        input.map(|s| s.to_string()),
        Some(delivery),
        None,
        None,
        None,
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(fut);
}

/// A single `lambda:invoke` Task ending the machine, with optional Retry/Catch.
fn lambda_invoke_def(extra: Value) -> Value {
    let mut task = json!({
        "Type": "Task",
        "Resource": "arn:aws:states:::lambda:invoke",
        "Parameters": { "FunctionName": "fn" },
        "End": true
    });
    if let (Some(task_obj), Some(extra_obj)) = (task.as_object_mut(), extra.as_object()) {
        for (k, v) in extra_obj {
            task_obj.insert(k.clone(), v.clone());
        }
    }
    json!({ "StartAt": "T", "States": { "T": task } })
}

// Case 1: a function error fails the task with the errorType as the Error.
#[test]
fn lambda_function_error_fails_task_with_error_type() {
    let state = make_state();
    let arn = arn_for("lambda-fn-error");
    let (bus, calls) = StubLambda::bus(vec![Ok(
        br#"{"errorMessage":"boom","errorType":"MyCustomError","stackTrace":[]}"#.to_vec(),
    )]);
    drive_with_delivery(&state, &arn, lambda_invoke_def(json!({})), Some("{}"), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Failed);
        assert_eq!(exec.error.as_deref(), Some("MyCustomError"));
        assert!(exec.cause.as_deref().unwrap().contains("errorMessage"));
    });
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

// Case 2: Retry on a custom errorType fires and the second attempt succeeds.
#[test]
fn lambda_retry_on_custom_error_type_succeeds() {
    let state = make_state();
    let arn = arn_for("lambda-retry");
    let (bus, calls) = StubLambda::bus(vec![
        Ok(br#"{"errorMessage":"boom","errorType":"MyCustomError"}"#.to_vec()),
        Ok(br#"{"ok":true}"#.to_vec()),
    ]);
    let def = lambda_invoke_def(json!({
        "Retry": [{ "ErrorEquals": ["MyCustomError"], "MaxAttempts": 2, "IntervalSeconds": 0 }]
    }));
    drive_with_delivery(&state, &arn, def, Some("{}"), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
    });
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

// Case 3: Catch on the errorType routes into the handler with Error/Cause.
#[test]
fn lambda_catch_on_custom_error_type_routes() {
    let state = make_state();
    let arn = arn_for("lambda-catch");
    let (bus, _calls) = StubLambda::bus(vec![Ok(
        br#"{"errorMessage":"boom","errorType":"MyCustomError"}"#.to_vec(),
    )]);
    let def = lambda_invoke_def(json!({
        "Catch": [{ "ErrorEquals": ["MyCustomError"], "Next": "Handler", "ResultPath": "$.err" }]
    }));
    let mut def = def;
    def["States"]["Handler"] = json!({ "Type": "Pass", "End": true });
    drive_with_delivery(&state, &arn, def, Some(r#"{"orig":"v"}"#), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output["err"]["Error"], json!("MyCustomError"));
        assert!(output["err"]["Cause"]
            .as_str()
            .unwrap()
            .contains("errorMessage"));
    });
}

// Case 4a: a transport error surfaces as Lambda.Unknown and is retryable.
#[test]
fn lambda_transport_error_is_lambda_unknown_and_retryable() {
    let state = make_state();
    let arn = arn_for("lambda-transport-retry");
    let (bus, calls) = StubLambda::bus(vec![
        Err("invocation failed: error sending request for url".to_string()),
        Ok(br#"{"ok":true}"#.to_vec()),
    ]);
    let def = lambda_invoke_def(json!({
        "Retry": [{ "ErrorEquals": ["Lambda.Unknown"], "MaxAttempts": 2, "IntervalSeconds": 0 }]
    }));
    drive_with_delivery(&state, &arn, def, Some("{}"), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
    });
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

// Case 4b: without a retry the transport error fails with Lambda.Unknown.
#[test]
fn lambda_transport_error_fails_as_lambda_unknown() {
    let state = make_state();
    let arn = arn_for("lambda-transport-fail");
    let (bus, _calls) = StubLambda::bus(vec![Err(
        "invocation failed: error sending request for url".to_string(),
    )]);
    drive_with_delivery(&state, &arn, lambda_invoke_def(json!({})), Some("{}"), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Failed);
        assert_eq!(exec.error.as_deref(), Some("Lambda.Unknown"));
    });
}

// H1: Catch on States.TaskFailed is a wildcard for a task's custom errorType.
#[test]
fn lambda_catch_on_states_task_failed_wildcard_routes() {
    let state = make_state();
    let arn = arn_for("lambda-taskfailed-wildcard");
    let (bus, _calls) = StubLambda::bus(vec![Ok(
        br#"{"errorMessage":"boom","errorType":"MyCustomError"}"#.to_vec(),
    )]);
    let def = lambda_invoke_def(json!({
        "Catch": [{
            "ErrorEquals": ["States.TaskFailed"],
            "Next": "Handler",
            "ResultPath": "$.err"
        }]
    }));
    let mut def = def;
    def["States"]["Handler"] = json!({ "Type": "Pass", "End": true });
    drive_with_delivery(&state, &arn, def, Some(r#"{"orig":"v"}"#), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        // The custom errorType was caught by the States.TaskFailed wildcard.
        assert_eq!(output["err"]["Error"], json!("MyCustomError"));
        assert_eq!(output["orig"], json!("v"));
    });
}

// H1: Retry on States.TaskFailed wildcard retries a custom task errorType.
#[test]
fn lambda_retry_on_states_task_failed_wildcard() {
    let state = make_state();
    let arn = arn_for("lambda-taskfailed-retry");
    let (bus, calls) = StubLambda::bus(vec![
        Ok(br#"{"errorMessage":"boom","errorType":"MyCustomError"}"#.to_vec()),
        Ok(br#"{"ok":true}"#.to_vec()),
    ]);
    let def = lambda_invoke_def(json!({
        "Retry": [{ "ErrorEquals": ["States.TaskFailed"], "MaxAttempts": 2, "IntervalSeconds": 0 }]
    }));
    drive_with_delivery(&state, &arn, def, Some("{}"), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
    });
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

// M4: an execution exceeding the old 500-transition ceiling still completes.
#[test]
fn long_loop_exceeds_old_ceiling_completes() {
    let state = make_state();
    let arn = arn_for("long-loop");
    let def = json!({
        "StartAt": "Init",
        "States": {
            "Init": { "Type": "Pass", "Result": {"count": 0}, "Next": "Check" },
            "Check": {
                "Type": "Choice",
                "Choices": [{ "Variable": "$.count", "NumericLessThan": 600, "Next": "Inc" }],
                "Default": "Done"
            },
            "Inc": {
                "Type": "Pass",
                "Parameters": { "count.$": "States.MathAdd($.count, 1)" },
                "Next": "Check"
            },
            "Done": { "Type": "Succeed" }
        }
    });
    drive(&state, &arn, def, Some("{}"));

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output["count"], json!(600));
    });
}

// Case 5: lambda:invoke wraps the function output in the AWS response
// envelope, so a ResultSelector picking `$.Payload` sees the function output
// and the raw task result carries StatusCode: 200.
#[test]
fn lambda_invoke_wraps_result_in_envelope() {
    let state = make_state();
    let arn = arn_for("lambda-envelope");
    let (bus, _calls) = StubLambda::bus(vec![Ok(br#"{"answer":42}"#.to_vec())]);
    let def = lambda_invoke_def(json!({
        "ResultSelector": { "value.$": "$.Payload", "status.$": "$.StatusCode" }
    }));
    drive_with_delivery(&state, &arn, def, Some("{}"), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output["value"]["answer"], json!(42));
        assert_eq!(output["status"], json!(200));
    });
}

// Case 6: the direct-ARN path returns the bare payload (no envelope), matching
// real AWS where only the optimized lambda:invoke integration wraps the result.
#[test]
fn lambda_direct_arn_is_not_wrapped() {
    let state = make_state();
    let arn = arn_for("lambda-direct");
    let (bus, _calls) = StubLambda::bus(vec![Ok(br#"{"answer":42}"#.to_vec())]);
    let def = json!({
        "StartAt": "T",
        "States": {
            "T": {
                "Type": "Task",
                "Resource": "arn:aws:lambda:us-east-1:123456789012:function:fn",
                "End": true
            }
        }
    });
    drive_with_delivery(&state, &arn, def, Some("{}"), bus);

    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output, json!({ "answer": 42 }));
    });
}

// Case 7: plain success payloads still pass through — objects lacking both
// error keys, and non-object (string) payloads.
#[test]
fn lambda_plain_payloads_pass_through() {
    // Object containing only one of the error field names is NOT an error.
    let state = make_state();
    let arn = arn_for("lambda-one-key");
    let (bus, _c) = StubLambda::bus(vec![Ok(br#"{"errorType":"NotReallyAnError"}"#.to_vec())]);
    drive_with_delivery(&state, &arn, lambda_invoke_def(json!({})), Some("{}"), bus);
    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output["Payload"]["errorType"], json!("NotReallyAnError"));
    });

    // A bare string payload passes through inside the envelope.
    let state = make_state();
    let arn = arn_for("lambda-string");
    let (bus, _c) = StubLambda::bus(vec![Ok(b"\"hello\"".to_vec())]);
    drive_with_delivery(&state, &arn, lambda_invoke_def(json!({})), Some("{}"), bus);
    read_exec(&state, &arn, |exec| {
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
        let output: Value = serde_json::from_str(exec.output.as_ref().unwrap()).unwrap();
        assert_eq!(output["Payload"], json!("hello"));
    });
}

// ── Regression: malformed JSONPath must not panic the interpreter ─────
//
// A state machine with a malformed InputPath/OutputPath (e.g. an unterminated
// `[` bracket or a multibyte char where the close bracket would be) is accepted
// today and reaches the interpreter. Before hardening, parsing the path sliced
// `[bracket_pos + 1 .. part.len() - 1]`, which underflowed (panic) or split a
// multibyte char (panic). A panic here drops the StartSyncExecution connection
// (EXPRESS, inline) and silently aborts a detached Standard execution so it
// stays RUNNING forever. The execution must instead complete cleanly.

#[tokio::test]
async fn malformed_input_path_unclosed_bracket_does_not_panic() {
    let state = make_state();
    let arn = arn_for("malformed-unclosed");
    create_execution(&state, &arn, Some(r#"{"arr":[1,2,3]}"#.to_string()));

    let definition = json!({
        "StartAt": "P",
        "States": {
            "P": { "Type": "Pass", "InputPath": "$.arr[", "End": true }
        }
    })
    .to_string();

    execute_state_machine(
        state.clone(),
        arn.clone(),
        definition,
        Some(r#"{"arr":[1,2,3]}"#.to_string()),
        None,
        None,
        None,
        None,
    )
    .await;

    // No panic, and the execution reached a terminal state (did not hang RUNNING).
    read_exec(&state, &arn, |exec| {
        assert_ne!(exec.status, ExecutionStatus::Running);
        assert_eq!(exec.status, ExecutionStatus::Succeeded);
    });
}

#[tokio::test]
async fn malformed_output_path_multibyte_tail_does_not_panic() {
    let state = make_state();
    let arn = arn_for("malformed-multibyte");
    create_execution(&state, &arn, Some(r#"{"x":[1,2,3]}"#.to_string()));

    let definition = json!({
        "StartAt": "P",
        "States": {
            "P": { "Type": "Pass", "OutputPath": "$.x[\u{00e9}", "End": true }
        }
    })
    .to_string();

    execute_state_machine(
        state.clone(),
        arn.clone(),
        definition,
        Some(r#"{"x":[1,2,3]}"#.to_string()),
        None,
        None,
        None,
        None,
    )
    .await;

    read_exec(&state, &arn, |exec| {
        assert_ne!(exec.status, ExecutionStatus::Running);
    });
}
