use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};
use tracing::{debug, warn};

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_dynamodb::SharedDynamoDbState;

use crate::choice::evaluate_choice;
use crate::error_handling::{find_catcher, should_retry};
use crate::io_processing::{apply_input_path, apply_output_path, apply_result_path};
use crate::service::SharedServiceRegistry;
use crate::state::{ExecutionStatus, HistoryEvent, MapRun, SharedStepFunctionsState};

/// Execute a state machine definition with the given input.
/// Updates the execution record in shared state as it progresses.
#[allow(clippy::too_many_arguments)]
pub async fn execute_state_machine(
    state: SharedStepFunctionsState,
    execution_arn: String,
    definition: String,
    input: Option<String>,
    delivery: Option<Arc<DeliveryBus>>,
    dynamodb_state: Option<SharedDynamoDbState>,
    registry: Option<SharedServiceRegistry>,
    logging_configuration: Option<Value>,
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
            "input": serde_json::to_string(&raw_input).expect("serde_json::Value serialization is infallible"),
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
    let registry_clone = registry.clone();
    let handle = tokio::spawn(async move {
        run_states(
            &def_owned,
            raw_input,
            &delivery_clone,
            &dynamodb_state_clone,
            &registry_clone,
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

    // Deliver execution history to CloudWatch Logs when logging is configured.
    deliver_execution_logs(
        &state,
        &execution_arn,
        delivery.as_ref(),
        logging_configuration.as_ref(),
    );
}

type StatesResult<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Value, (String, String)>> + Send + 'a>,
>;

/// Result of executing a single state in the state machine.
pub(crate) enum Advance {
    /// Continue to the given state with the given input.
    Next(String, Value),
    /// Terminate the state machine with the given output.
    End(Value),
    /// Fail the state machine with the given error and cause.
    Fail(String, String),
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
            "input": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
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
            "output": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
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
    registry: &Option<SharedServiceRegistry>,
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
            "input": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
        }),
    );

    let result = execute_task_state(
        name,
        state_def,
        &input,
        delivery,
        dynamodb_state,
        registry,
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
                    "output": serde_json::to_string(&output).expect("serde_json::Value serialization is infallible"),
                }),
            );
            advance_from_next(state_def, output)
        }
        // Task-originated error → the States.TaskFailed wildcard applies.
        Err((error, cause)) => advance_from_error(state_def, &input, error, cause, true),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_parallel_state(
    name: &str,
    state_def: &Value,
    input: Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
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
            "input": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
        }),
    );

    let result = execute_parallel_state(
        state_def,
        &input,
        delivery,
        dynamodb_state,
        registry,
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
                    "output": serde_json::to_string(&output).expect("serde_json::Value serialization is infallible"),
                }),
            );
            advance_from_next(state_def, output)
        }
        // A Parallel branch failure is not itself a task error; the
        // States.TaskFailed wildcard must not swallow branch-level errors.
        Err((error, cause)) => advance_from_error(state_def, &input, error, cause, false),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_map_state(
    name: &str,
    state_def: &Value,
    input: Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
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
            "input": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
        }),
    );

    let result = execute_map_state(
        state_def,
        &input,
        delivery,
        dynamodb_state,
        registry,
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
                    "output": serde_json::to_string(&output).expect("serde_json::Value serialization is infallible"),
                }),
            );
            advance_from_next(state_def, output)
        }
        // A Map iteration failure is not itself a task error; the
        // States.TaskFailed wildcard must not swallow iteration-level errors.
        Err((error, cause)) => advance_from_error(state_def, &input, error, cause, false),
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

/// Execute a Task state: invoke the resource (Lambda, SQS, SNS, EventBridge, DynamoDB),
/// apply I/O processing, handle Retry.
#[allow(clippy::too_many_arguments)]
async fn execute_task_state(
    name: &str,
    state_def: &Value,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
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

    let retriers = state_def["Retry"].as_array().cloned().unwrap_or_default();
    let timeout_seconds = state_def["TimeoutSeconds"].as_u64();
    let heartbeat_seconds = state_def["HeartbeatSeconds"].as_u64();
    let mut attempt = 0u32;

    let is_wait_for_task_token = resource.contains(".waitForTaskToken");
    let task_token = if is_wait_for_task_token {
        let token = format!(
            "FCToken-{}-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            uuid::Uuid::new_v4().simple(),
        );
        let account_id = account_id_from_arn(execution_arn);
        let context = json!({
            "Task": { "Token": token.clone() },
            "Execution": { "Id": execution_arn },
            "State": { "Name": name },
        });
        {
            let mut accounts = shared_state.write();
            let state = accounts.get_or_create(account_id);
            state.task_tokens.insert(
                token.clone(),
                crate::state::TaskTokenState {
                    activity_arn: String::new(),
                    status: "PENDING".to_string(),
                    output: None,
                    error: None,
                    cause: None,
                    input: None,
                    created_at: chrono::Utc::now(),
                    last_heartbeat_at: None,
                    heartbeat_seconds: heartbeat_seconds.map(|s| s as i64),
                    timeout_seconds: timeout_seconds.map(|s| s as i64),
                },
            );
        }
        Some((token, context))
    } else {
        None
    };

    let task_input = if let Some(params) = state_def.get("Parameters") {
        if let Some((_, ctx)) = &task_token {
            apply_parameters(params, &effective_input, Some(ctx))
        } else {
            apply_parameters(params, &effective_input, None)
        }
    } else {
        effective_input
    };

    loop {
        add_event(
            shared_state,
            execution_arn,
            "TaskScheduled",
            entered_event_id,
            json!({
                "resource": resource,
                "region": "us-east-1",
                "parameters": serde_json::to_string(&task_input).expect("serde_json::Value serialization is infallible"),
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
            registry,
            execution_arn,
            timeout_seconds,
            heartbeat_seconds,
            shared_state,
        )
        .await;

        match invoke_result {
            Ok(result) => {
                if let Some((token, _)) = &task_token {
                    let account_id = account_id_from_arn(execution_arn);
                    match poll_task_token(
                        shared_state,
                        account_id,
                        token,
                        timeout_seconds,
                        heartbeat_seconds,
                    )
                    .await
                    {
                        Ok(output) => {
                            add_event(
                                shared_state,
                                execution_arn,
                                "TaskSucceeded",
                                entered_event_id,
                                json!({
                                    "resource": resource,
                                    "output": serde_json::to_string(&output).expect("serde_json::Value serialization is infallible"),
                                }),
                            );

                            let selected = if let Some(selector) = state_def.get("ResultSelector") {
                                apply_parameters(selector, &output, None)
                            } else {
                                output
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

                            if let Some(delay_ms) = should_retry(&retriers, &error, attempt, true) {
                                attempt += 1;
                                // Honour the ASL-computed backoff (already bounded
                                // by MaxDelaySeconds inside `should_retry`).
                                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms))
                                    .await;
                                continue;
                            }

                            return Err((error, cause));
                        }
                    }
                }

                add_event(
                    shared_state,
                    execution_arn,
                    "TaskSucceeded",
                    entered_event_id,
                    json!({
                        "resource": resource,
                        "output": serde_json::to_string(&result).expect("serde_json::Value serialization is infallible"),
                    }),
                );

                let selected = if let Some(selector) = state_def.get("ResultSelector") {
                    apply_parameters(selector, &result, None)
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

                if let Some(delay_ms) = should_retry(&retriers, &error, attempt, true) {
                    attempt += 1;
                    // Honour the ASL-computed backoff (already bounded by
                    // MaxDelaySeconds inside `should_retry`).
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
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
    registry: &Option<SharedServiceRegistry>,
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
        let reg = registry.clone();
        let state = shared_state.clone();
        let arn = execution_arn.to_string();

        handles.push(tokio::spawn(async move {
            run_states(&branch, branch_input, &delivery, &ddb, &reg, &state, &arn).await
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
        apply_parameters(selector, &branch_output, None)
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
/// Supports distributed mode: ItemReader (S3), ItemBatcher, ResultWriter (S3),
/// ToleratedFailurePercentage, and MaxConcurrencyPath.
#[allow(clippy::too_many_arguments)]
async fn execute_map_state(
    state_def: &Value,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
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

    // Resolve MaxConcurrencyPath if present
    let max_concurrency = if let Some(path) = state_def["MaxConcurrencyPath"].as_str() {
        crate::io_processing::resolve_path(&effective_input, path)
            .as_u64()
            .unwrap_or(0)
    } else {
        state_def["MaxConcurrency"].as_u64().unwrap_or(0)
    };
    let effective_concurrency = if max_concurrency == 0 {
        40
    } else {
        max_concurrency as usize
    };

    // Read items from ItemReader (S3) or ItemsPath
    let items = if let Some(item_reader) = state_def.get("ItemReader") {
        read_items_from_s3(item_reader, registry, execution_arn).await?
    } else {
        let items_path = state_def["ItemsPath"].as_str().unwrap_or("$");
        let items_value = crate::io_processing::resolve_path(&effective_input, items_path);
        items_value.as_array().cloned().unwrap_or_default()
    };

    // Apply ItemBatcher if present
    let batch_config = state_def.get("ItemBatcher").cloned();
    let batched_items = if let Some(ref batcher) = batch_config {
        apply_item_batcher(&items, batcher, &effective_input)
    } else {
        items
    };

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

    let tolerated_failure_percentage = state_def["ToleratedFailurePercentage"]
        .as_f64()
        .unwrap_or(0.0);
    let tolerated_failure_count = state_def["ToleratedFailureCount"].as_i64().unwrap_or(0);
    let total_items = batched_items.len() as f64;
    let mut failure_count = 0usize;

    // A distributed Map (`ItemProcessor.ProcessorConfig.Mode == "DISTRIBUTED"`)
    // materialises a MapRun record that `ListMapRuns` / `DescribeMapRun`
    // surface. Inline Map states do not create one, matching AWS.
    let is_distributed = iterator_def
        .get("ProcessorConfig")
        .and_then(|c| c.get("Mode"))
        .and_then(Value::as_str)
        == Some("DISTRIBUTED");
    let map_run_arn = if is_distributed {
        map_run_arn_for(execution_arn).inspect(|arn| {
            start_map_run(
                shared_state,
                arn,
                execution_arn,
                // The configured MaxConcurrency (0 == unlimited), as AWS reports
                // it, not the internal `effective_concurrency` (which defaults
                // an unset value to 40).
                max_concurrency as usize,
                tolerated_failure_percentage,
                tolerated_failure_count,
                batched_items.len(),
            );
        })
    } else {
        None
    };

    let semaphore = Arc::new(tokio::sync::Semaphore::new(effective_concurrency));

    // Process all items
    let mut handles = Vec::new();
    for (index, batch_item) in batched_items.into_iter().enumerate() {
        let iter_def = iterator_def.clone();
        let delivery = delivery.clone();
        let ddb = dynamodb_state.clone();
        let reg = registry.clone();
        let state = shared_state.clone();
        let arn = execution_arn.to_string();
        let sem = semaphore.clone();

        // Apply ItemSelector if present
        let item_input = if let Some(selector) = state_def.get("ItemSelector") {
            let mut ctx = serde_json::Map::new();
            ctx.insert("value".to_string(), batch_item.clone());
            ctx.insert("index".to_string(), json!(index));
            apply_parameters(selector, &Value::Object(ctx), None)
        } else {
            batch_item
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
            let result =
                run_states(&iter_def, item_input, &delivery, &ddb, &reg, &state, &arn).await;
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
                failure_count += 1;
                let failure_percentage = (failure_count as f64 / total_items) * 100.0;
                if failure_percentage > tolerated_failure_percentage {
                    if let Some(arn) = &map_run_arn {
                        finish_map_run(
                            shared_state,
                            arn,
                            execution_arn,
                            results.len() as i64,
                            failure_count as i64,
                            "FAILED",
                        );
                    }
                    return Err((error, cause));
                }
            }
        }
    }

    // Every iteration is accounted for and the tolerated-failure threshold was
    // not exceeded: the map run succeeded.
    if let Some(arn) = &map_run_arn {
        finish_map_run(
            shared_state,
            arn,
            execution_arn,
            results.len() as i64,
            failure_count as i64,
            "SUCCEEDED",
        );
    }

    // Sort by index to maintain order
    results.sort_by_key(|(i, _)| *i);
    let map_output = Value::Array(results.into_iter().map(|(_, v)| v).collect());

    // Write results to S3 if ResultWriter is configured
    if let Some(result_writer) = state_def.get("ResultWriter") {
        write_map_results_to_s3(result_writer, registry, execution_arn, &map_output).await?;
    }

    // Apply ResultSelector if present
    let selected = if let Some(selector) = state_def.get("ResultSelector") {
        apply_parameters(selector, &map_output, None)
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

/// Build a MapRun ARN for a distributed Map running inside `execution_arn`.
/// `arn:aws:states:R:A:execution:SM:Exec` -> `arn:aws:states:R:A:mapRun:SM/Exec:<label>`.
fn map_run_arn_for(execution_arn: &str) -> Option<String> {
    let (prefix, tail) = execution_arn.split_once(":execution:")?;
    // `tail` is `StateMachineName:ExecutionName`; the MapRun ARN joins them with
    // a slash and appends a unique label.
    let sm_exec = tail.replacen(':', "/", 1);
    let label = uuid::Uuid::new_v4().simple().to_string();
    Some(format!("{prefix}:mapRun:{sm_exec}:{label}"))
}

/// Persist a fresh RUNNING MapRun record for a distributed Map.
fn start_map_run(
    shared_state: &SharedStepFunctionsState,
    map_run_arn: &str,
    execution_arn: &str,
    max_concurrency: usize,
    tolerated_failure_percentage: f64,
    tolerated_failure_count: i64,
    total: usize,
) {
    let account = account_id_from_arn(execution_arn).to_string();
    let mut accounts = shared_state.write();
    let s = accounts.get_or_create(&account);
    s.map_runs.insert(
        map_run_arn.to_string(),
        MapRun {
            map_run_arn: map_run_arn.to_string(),
            execution_arn: execution_arn.to_string(),
            max_concurrency: max_concurrency as i32,
            tolerated_failure_percentage,
            tolerated_failure_count,
            status: "RUNNING".to_string(),
            start_date: Utc::now(),
            stop_date: None,
            total_count: total as i64,
            succeeded_count: 0,
            failed_count: 0,
        },
    );
}

/// Transition a MapRun to a terminal status, recording the final iteration
/// tallies. No-op if the record was reaped (e.g. a state reset mid-run).
fn finish_map_run(
    shared_state: &SharedStepFunctionsState,
    map_run_arn: &str,
    execution_arn: &str,
    succeeded: i64,
    failed: i64,
    status: &str,
) {
    let account = account_id_from_arn(execution_arn).to_string();
    let mut accounts = shared_state.write();
    let s = accounts.get_or_create(&account);
    if let Some(mr) = s.map_runs.get_mut(map_run_arn) {
        mr.status = status.to_string();
        mr.succeeded_count = succeeded;
        mr.failed_count = failed;
        mr.stop_date = Some(Utc::now());
    }
}

/// Read items from S3 for distributed Map mode ItemReader.
async fn read_items_from_s3(
    item_reader: &Value,
    registry: &Option<SharedServiceRegistry>,
    execution_arn: &str,
) -> Result<Vec<Value>, (String, String)> {
    let resource = item_reader["Resource"]
        .as_str()
        .unwrap_or("arn:aws:states:::s3:getObject");
    if !resource.contains("s3:getObject") {
        return Err((
            "States.Runtime".to_string(),
            format!("ItemReader unsupported resource: {resource}"),
        ));
    }

    let params = item_reader
        .get("Parameters")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let bucket = params["Bucket"].as_str().ok_or_else(|| {
        (
            "States.Runtime".to_string(),
            "ItemReader missing Bucket".to_string(),
        )
    })?;
    let key = params["Key"].as_str().ok_or_else(|| {
        (
            "States.Runtime".to_string(),
            "ItemReader missing Key".to_string(),
        )
    })?;

    let registry_arc = resolve_registry(registry)?;
    let account_id = account_from_execution_arn(execution_arn);

    let body = call_sdk_action_raw_bytes(
        &registry_arc,
        "s3",
        "GetObject",
        &json!({ "Bucket": bucket, "Key": key }),
        &account_id,
    )
    .await?;

    // Parse JSON array from the S3 object body
    let parsed: Value = serde_json::from_slice(&body).map_err(|e| {
        (
            "States.Runtime".to_string(),
            format!("ItemReader failed to parse S3 object as JSON: {e}"),
        )
    })?;

    parsed.as_array().cloned().ok_or_else(|| {
        (
            "States.Runtime".to_string(),
            "ItemReader S3 object is not a JSON array".to_string(),
        )
    })
}

/// Write Map results to S3 for distributed Map mode ResultWriter.
async fn write_map_results_to_s3(
    result_writer: &Value,
    registry: &Option<SharedServiceRegistry>,
    execution_arn: &str,
    results: &Value,
) -> Result<(), (String, String)> {
    let resource = result_writer["Resource"]
        .as_str()
        .unwrap_or("arn:aws:states:::s3:putObject");
    if !resource.contains("s3:putObject") {
        return Err((
            "States.Runtime".to_string(),
            format!("ResultWriter unsupported resource: {resource}"),
        ));
    }

    let params = result_writer
        .get("Parameters")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let bucket = params["Bucket"].as_str().ok_or_else(|| {
        (
            "States.Runtime".to_string(),
            "ResultWriter missing Bucket".to_string(),
        )
    })?;
    let prefix = params["Prefix"].as_str().unwrap_or("map-results/");

    let registry_arc = resolve_registry(registry)?;
    let account_id = account_from_execution_arn(execution_arn);

    use bytes::Bytes;
    let body = Bytes::from(
        serde_json::to_vec(results).expect("serde_json::Value serialization is infallible"),
    );

    // Build a raw S3 PutObject request with the JSON body
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};
    let service = registry_arc.get("s3").ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "S3 service not available for ResultWriter".to_string(),
        )
    })?;

    let req = AwsRequest {
        service: "s3".to_string(),
        action: "PutObject".to_string(),
        region: "us-east-1".to_string(),
        account_id: account_id.to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        headers: HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body,
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![bucket.to_string(), format!("{prefix}result.json")],
        raw_path: format!("/{bucket}/{prefix}result.json"),
        raw_query: String::new(),
        method: Method::PUT,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };

    service.handle(req).await.map_err(|err| {
        let code = err.code().to_string();
        let msg = err.message();
        (
            format!("S3.{code}"),
            format!("ResultWriter PutObject failed: {msg}"),
        )
    })?;

    Ok(())
}

/// Apply ItemBatcher configuration to group items into batches.
fn apply_item_batcher(items: &[Value], batcher: &Value, _effective_input: &Value) -> Vec<Value> {
    let max_per_batch = batcher["MaxItemsPerBatch"].as_u64().unwrap_or(u64::MAX) as usize;
    let max_bytes = batcher["MaxInputBytesPerBatch"].as_u64().unwrap_or(0) as usize;
    let batch_input = batcher.get("BatchInput").cloned();

    let mut batches: Vec<Vec<Value>> = Vec::new();
    let mut current_batch: Vec<Value> = Vec::new();
    let mut current_bytes = 0usize;

    for item in items.iter().cloned() {
        let item_bytes = serde_json::to_vec(&item).unwrap_or_default().len();
        if !current_batch.is_empty()
            && (current_batch.len() >= max_per_batch
                || (max_bytes > 0 && current_bytes + item_bytes > max_bytes))
        {
            batches.push(current_batch);
            current_batch = Vec::new();
            current_bytes = 0;
        }
        current_bytes += item_bytes;
        current_batch.push(item);
    }
    if !current_batch.is_empty() {
        batches.push(current_batch);
    }

    batches
        .into_iter()
        .enumerate()
        .map(|(index, batch)| {
            let mut map = serde_json::Map::new();
            map.insert("index".to_string(), json!(index));
            map.insert("items".to_string(), Value::Array(batch));
            if let Some(Value::Object(ref obj)) = batch_input {
                for (k, v) in obj {
                    map.insert(k.clone(), v.clone());
                }
            }
            Value::Object(map)
        })
        .collect()
}

/// Invoke a resource (Lambda function or SDK integration).
#[allow(clippy::too_many_arguments)]
async fn invoke_resource(
    resource: &str,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
    execution_arn: &str,
    timeout_seconds: Option<u64>,
    heartbeat_seconds: Option<u64>,
    shared_state: &SharedStepFunctionsState,
) -> Result<Value, (String, String)> {
    // Direct activity ARN: arn:aws:states:<region>:<account>:activity:<name>
    if resource.contains(":states:") && resource.contains(":activity:") {
        return invoke_activity(
            resource,
            input,
            shared_state,
            timeout_seconds,
            heartbeat_seconds,
        )
        .await;
    }

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
        // The optimized `lambda:invoke` integration returns the AWS Invoke API
        // response envelope, not the bare function output — `ResultSelector`
        // expressions like `{"value.$": "$.Payload"}` depend on it. Wrap only
        // the successful result; error results (function errors and transport
        // failures) pass through unchanged, since a failed lambda:invoke task
        // has no result envelope on AWS. The direct-ARN path above
        // intentionally stays unwrapped — there real AWS returns the bare
        // payload. `SdkHttpMetadata`/`SdkResponseMetadata` are omitted; real
        // templates select `Payload`/`StatusCode`.
        return invoke_lambda_direct(function_name, &payload, delivery, timeout_seconds)
            .await
            .map(|payload| {
                json!({
                    "ExecutedVersion": "$LATEST",
                    "Payload": payload,
                    "StatusCode": 200,
                })
            });
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

    // Nested Step Functions execution:
    //   arn:aws:states:::states:startExecution[.sync|.waitForTaskToken]
    // Routes through the same registry path as the generic aws-sdk
    // integration so the inner execution sees a real `StartExecution`
    // call. `.sync` polls `DescribeExecution` until the inner execution
    // reaches a terminal state, mirroring AWS' behavior of bubbling the
    // inner Output back to the parent.
    if let Some(tail) = resource.strip_prefix("arn:aws:states:::") {
        if tail.starts_with("states:startExecution") {
            let account_id = account_from_execution_arn(execution_arn);
            let result =
                invoke_aws_sdk_integration(tail, input, registry, &account_id, timeout_seconds)
                    .await;
            // Patch the inner execution's parent_execution_arn so the
            // `/execution-tree` introspection can walk back-references.
            // For `.sync`, the result is the DescribeExecution shape
            // (has `executionArn`); for fire-and-forget StartExecution the
            // initial StartExecution response also carries `executionArn`.
            if let Ok(ref value) = result {
                if let Some(inner_arn) = value
                    .get("executionArn")
                    .or_else(|| value.get("ExecutionArn"))
                    .and_then(Value::as_str)
                {
                    let mut accounts = shared_state.write();
                    if let Some(state) = accounts.get_mut(&account_id) {
                        if let Some(exec) = state.executions.get_mut(inner_arn) {
                            exec.parent_execution_arn = Some(execution_arn.to_string());
                        }
                    }
                }
            }
            return result;
        }
    }

    // Generic AWS SDK integration: arn:aws:states:::aws-sdk:<service>:<action>[.<wait>]
    // Routes the call to the registered service via the central
    // ServiceRegistry, passing the Task's `Parameters` block as the
    // request body. Mirrors the AWS SDK service integration pattern in
    // real Step Functions.
    if let Some(rest) = resource.strip_prefix("arn:aws:states:::aws-sdk:") {
        let account_id = account_from_execution_arn(execution_arn);
        return invoke_aws_sdk_integration(rest, input, registry, &account_id, timeout_seconds)
            .await;
    }

    // Optimized service integrations expose `.sync` variants for ECS,
    // Athena, and Glue. Route them through the same waiter machinery as
    // `aws-sdk:` so callers can write the AWS-blessed ARN forms.
    if let Some(tail) = resource.strip_prefix("arn:aws:states:::") {
        if tail.contains(".sync") {
            let account_id = account_from_execution_arn(execution_arn);
            return invoke_aws_sdk_integration(tail, input, registry, &account_id, timeout_seconds)
                .await;
        }
    }

    Err((
        "States.TaskFailed".to_string(),
        format!("Unsupported resource: {resource}"),
    ))
}

/// Convert an SDK integration action (camelCase, e.g. `getItem`) to its
/// PascalCase wire form (`GetItem`). Step Functions Task ARNs use
/// camelCase, but the underlying AWS service handlers expect PascalCase
/// `X-Amz-Target`-style action names.
fn camel_to_pascal(action: &str) -> String {
    let mut chars = action.chars();
    match chars.next() {
        None => String::new(),
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
    }
}

/// Map a Step Functions SDK integration service id to the corresponding
/// fakecloud `service_name()`. Most match 1:1, but a handful of AWS SDK
/// service ids differ from the SigV4 service identifier we register
/// services under (e.g. `sfn` -> `states`, `cloudwatchlogs` -> `logs`).
fn map_sdk_service_id(service_id: &str) -> &str {
    match service_id {
        "sfn" => "states",
        "cloudwatchlogs" => "logs",
        // Default: pass through unchanged.
        other => other,
    }
}

/// Extract the AWS account id from a Step Functions execution ARN
/// (`arn:aws:states:<region>:<account>:execution:...`). Falls back to
/// the AWS-conventional fixture id if the ARN is malformed.
fn account_from_execution_arn(execution_arn: &str) -> String {
    execution_arn
        .split(':')
        .nth(4)
        .filter(|s| !s.is_empty())
        .unwrap_or("123456789012")
        .to_string()
}

/// Dispatch a Step Functions `aws-sdk:<service>:<action>` Task to the
/// registered service via the central [`fakecloud_core::registry::ServiceRegistry`].
/// `tail` is the trailing portion of the resource ARN after the
/// `aws-sdk:` prefix (e.g. `dynamodb:getItem` or `sqs:sendMessage.waitForTaskToken`).
async fn invoke_aws_sdk_integration(
    tail: &str,
    input: &Value,
    registry: &Option<SharedServiceRegistry>,
    account_id: &str,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    let registry_arc = resolve_registry(registry)?;

    // Split `<service>:<action>[.<modifier>]`. The `.waitForTaskToken`
    // modifier is accepted but ignored — the integration runs synchronously
    // regardless. The `.sync` modifier triggers a polling loop after the
    // initial call to wait for the downstream operation to reach a terminal
    // state.
    let mut parts = tail.splitn(2, ':');
    let service_id = parts.next().filter(|s| !s.is_empty()).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("Invalid aws-sdk Resource ARN: missing service in '{tail}'"),
        )
    })?;
    let action_with_mod = parts.next().filter(|s| !s.is_empty()).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("Invalid aws-sdk Resource ARN: missing action in '{tail}'"),
        )
    })?;
    let action_camel = action_with_mod
        .split('.')
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                format!("Invalid aws-sdk Resource ARN: empty action in '{tail}'"),
            )
        })?;
    let modifiers: Vec<&str> = action_with_mod.split('.').skip(1).collect();
    let is_sync = modifiers.iter().any(|m| *m == "sync" || *m == "sync:2");

    let action_pascal = camel_to_pascal(action_camel);
    let service_name = map_sdk_service_id(service_id).to_string();

    // ECS's wire format is camelCase, but Step Functions's optimized
    // integration takes PascalCase parameter names in the state machine
    // definition. Translate top-level keys for the ECS service so AWS
    // Step Functions definitions Just Work against the fake.
    let translated_input = match service_name.as_str() {
        "ecs" => translate_ecs_keys_to_camel(input),
        _ => input.clone(),
    };

    let initial = call_sdk_action(
        &registry_arc,
        &service_name,
        &action_pascal,
        &translated_input,
        account_id,
    )
    .await?;

    if !is_sync {
        return Ok(initial);
    }

    // `.sync` pattern: dispatch to the per-service waiter that polls until
    // the downstream operation reaches a terminal state. Returns the full
    // describe-shape result, or surfaces a terminal failure as
    // `States.TaskFailed`.
    sync_wait(
        &registry_arc,
        &service_name,
        &action_pascal,
        &initial,
        &translated_input,
        account_id,
        timeout_seconds,
    )
    .await
}

/// Translate the top-level PascalCase keys that AWS Step Functions
/// state machines use for `ecs:runTask` Parameters into the camelCase
/// shape that the AWS ECS API (and our handler) expects. Unknown keys
/// pass through unchanged so callers can still send raw camelCase. The
/// translation is shallow on purpose — nested overrides keep their
/// existing camelCase shape on real AWS too.
fn translate_ecs_keys_to_camel(input: &Value) -> Value {
    let Some(obj) = input.as_object() else {
        return input.clone();
    };
    let mut out = serde_json::Map::with_capacity(obj.len());
    for (k, v) in obj.iter() {
        let camel = match k.as_str() {
            "Cluster" => "cluster",
            "TaskDefinition" => "taskDefinition",
            "LaunchType" => "launchType",
            "Group" => "group",
            "Overrides" => "overrides",
            "PlatformVersion" => "platformVersion",
            "NetworkConfiguration" => "networkConfiguration",
            "Tags" => "tags",
            "EnableExecuteCommand" => "enableExecuteCommand",
            "PropagateTags" => "propagateTags",
            "ReferenceId" => "referenceId",
            "StartedBy" => "startedBy",
            "Count" => "count",
            "CapacityProviderStrategy" => "capacityProviderStrategy",
            "PlacementConstraints" => "placementConstraints",
            "PlacementStrategy" => "placementStrategy",
            other => other,
        };
        out.insert(camel.to_string(), v.clone());
    }
    Value::Object(out)
}

fn resolve_registry(
    registry: &Option<SharedServiceRegistry>,
) -> Result<Arc<fakecloud_core::registry::ServiceRegistry>, (String, String)> {
    let registry_handle = registry.as_ref().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "No service registry configured for aws-sdk integration".to_string(),
        )
    })?;
    registry_handle.get().cloned().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "Service registry not yet initialised; aws-sdk integration unavailable".to_string(),
        )
    })
}

/// Call a single AWS SDK action against the registered service handler.
async fn call_sdk_action(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    service_name: &str,
    action_pascal: &str,
    input: &Value,
    account_id: &str,
) -> Result<Value, (String, String)> {
    use bytes::Bytes;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};

    let service = registry.get(service_name).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("Unknown aws-sdk service '{service_name}'"),
        )
    })?;

    let body_bytes = Bytes::from(
        serde_json::to_vec(input).expect("serde_json::Value serialization is infallible"),
    );

    let req = AwsRequest {
        service: service_name.to_string(),
        action: action_pascal.to_string(),
        region: "us-east-1".to_string(),
        account_id: account_id.to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        headers: HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: body_bytes,
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };

    let response = service.handle(req).await.map_err(|err| {
        let code = err.code().to_string();
        let msg = err.message();
        let prefix_service = match service_name {
            "dynamodb" => "DynamoDb".to_string(),
            "states" => "Sfn".to_string(),
            other => camel_to_pascal(other),
        };
        (
            format!("{prefix_service}.{code}"),
            format!("{action_pascal} failed: {msg}"),
        )
    })?;

    let response_bytes = match response.body {
        fakecloud_core::service::ResponseBody::Bytes(b) => b,
        fakecloud_core::service::ResponseBody::File { .. } => {
            return Err((
                "States.TaskFailed".to_string(),
                "aws-sdk integration: file-backed response not supported".to_string(),
            ));
        }
    };

    if response_bytes.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&response_bytes).map_err(|e| {
        (
            "States.TaskFailed".to_string(),
            format!("aws-sdk integration: failed to parse response JSON: {e}"),
        )
    })
}

/// Call an SDK action and return raw bytes without JSON parsing.
/// Used by distributed Map mode to read/write S3 objects.
async fn call_sdk_action_raw_bytes(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    service_name: &str,
    action_pascal: &str,
    input: &Value,
    account_id: &str,
) -> Result<bytes::Bytes, (String, String)> {
    use bytes::Bytes;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};

    let service = registry.get(service_name).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("Unknown aws-sdk service '{service_name}'"),
        )
    })?;

    let body_bytes = Bytes::from(
        serde_json::to_vec(input).expect("serde_json::Value serialization is infallible"),
    );

    let req = AwsRequest {
        service: service_name.to_string(),
        action: action_pascal.to_string(),
        region: "us-east-1".to_string(),
        account_id: account_id.to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        headers: HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: body_bytes,
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    };

    let response = service.handle(req).await.map_err(|err| {
        let code = err.code().to_string();
        let msg = err.message();
        let prefix_service = match service_name {
            "dynamodb" => "DynamoDb".to_string(),
            "states" => "Sfn".to_string(),
            other => camel_to_pascal(other),
        };
        (
            format!("{prefix_service}.{code}"),
            format!("{action_pascal} failed: {msg}"),
        )
    })?;

    match response.body {
        fakecloud_core::service::ResponseBody::Bytes(b) => Ok(b),
        fakecloud_core::service::ResponseBody::File { .. } => Err((
            "States.TaskFailed".to_string(),
            "aws-sdk integration: file-backed response not supported".to_string(),
        )),
    }
}

/// Cap on `.sync` polling so a stuck downstream task can't hang an
/// execution forever. Mirrors the `TimeoutSeconds` knob on the Task state
/// when one is set; otherwise defaults to 5 minutes which is enough for
/// the in-process services that ship today (Athena returns synchronously,
/// ECS tasks finish within seconds even when docker-less).
const SYNC_DEFAULT_TIMEOUT_SECS: u64 = 300;
const SYNC_POLL_INTERVAL_MS: u64 = 200;

/// Dispatch `.sync` waiters by service+action. Each waiter polls the
/// matching describe-style API until the downstream operation reaches a
/// terminal state, then returns the full describe response.
async fn sync_wait(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    service_name: &str,
    action_pascal: &str,
    initial: &Value,
    input: &Value,
    account_id: &str,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    match (service_name, action_pascal) {
        ("ecs", "RunTask") => {
            sync_wait_ecs_run_task(registry, initial, input, account_id, timeout_seconds).await
        }
        ("athena", "StartQueryExecution") => {
            sync_wait_athena_query(registry, initial, account_id, timeout_seconds).await
        }
        ("states", "StartExecution") => {
            sync_wait_states_start_execution(registry, initial, account_id, timeout_seconds).await
        }
        ("glue", "StartJobRun") => {
            // Poll the real Glue `GetJobRun` (the job run was created by the
            // StartJobRun integration that ran just before this waiter), so the
            // `.sync` result is the actual run — full JobRun shape, and a
            // FAILED/STOPPED/TIMEOUT state surfaces as a task failure rather
            // than a hardcoded SUCCEEDED.
            let job_run_id = initial
                .get("JobRunId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let job_name = input
                .get("JobName")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let deadline = sync_deadline(timeout_seconds);
            loop {
                let described = call_sdk_action(
                    registry,
                    "glue",
                    "GetJobRun",
                    &json!({ "JobName": job_name, "RunId": job_run_id }),
                    account_id,
                )
                .await?;
                let state = described
                    .get("JobRun")
                    .and_then(|r| r.get("JobRunState"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                match state {
                    "SUCCEEDED" => return Ok(described),
                    "FAILED" | "STOPPED" | "TIMEOUT" | "ERROR" => {
                        return Err((
                            "States.TaskFailed".to_string(),
                            format!("Glue job run {job_run_id} ended in state {state}"),
                        ));
                    }
                    _ => {}
                }
                if std::time::Instant::now() >= deadline {
                    return Err((
                        "States.Timeout".to_string(),
                        format!(
                            "glue:startJobRun.sync timed out after {}s for run {job_run_id}",
                            sync_timeout_secs(timeout_seconds)
                        ),
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(SYNC_POLL_INTERVAL_MS)).await;
            }
        }
        _ => Err((
            "States.TaskFailed".to_string(),
            format!(
                "`.sync` is not supported for {service_name}:{action_pascal} yet — \
                 supported: ecs:RunTask, athena:StartQueryExecution, glue:StartJobRun, states:StartExecution"
            ),
        )),
    }
}

async fn sync_wait_ecs_run_task(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    initial: &Value,
    input: &Value,
    account_id: &str,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    let tasks = initial
        .get("tasks")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                "ecs:RunTask.sync: response missing 'tasks' array".to_string(),
            )
        })?;
    if tasks.is_empty() {
        return Err((
            "States.TaskFailed".to_string(),
            "ecs:RunTask.sync: no tasks were started".to_string(),
        ));
    }
    let task_arns: Vec<String> = tasks
        .iter()
        .filter_map(|t| t.get("taskArn").and_then(Value::as_str).map(String::from))
        .collect();
    let cluster = input
        .get("cluster")
        .or_else(|| input.get("Cluster"))
        .and_then(Value::as_str)
        .map(String::from);

    let deadline = sync_deadline(timeout_seconds);
    loop {
        let mut describe_input = json!({ "tasks": task_arns });
        if let Some(c) = &cluster {
            describe_input["cluster"] = json!(c);
        }
        let described = call_sdk_action(
            registry,
            "ecs",
            "DescribeTasks",
            &describe_input,
            account_id,
        )
        .await?;
        let described_tasks = described
            .get("tasks")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let all_stopped = !described_tasks.is_empty()
            && described_tasks
                .iter()
                .all(|t| t.get("lastStatus").and_then(Value::as_str) == Some("STOPPED"));
        if all_stopped {
            // Surface non-zero container exit codes or stop codes that map
            // to AWS-style failures. Per AWS docs, ECS RunTask.sync raises
            // States.TaskFailed when any container exits non-zero or the
            // task stops with a failure code.
            let any_failed = described_tasks.iter().any(|t| {
                let stop_code = t.get("stopCode").and_then(Value::as_str);
                let bad_stop = matches!(
                    stop_code,
                    Some(
                        "TaskFailedToStart"
                            | "EssentialContainerExited"
                            | "ServiceSchedulerInitiated"
                    )
                );
                let bad_exit = t
                    .get("containers")
                    .and_then(Value::as_array)
                    .map(|cs| {
                        cs.iter().any(|c| {
                            c.get("exitCode")
                                .and_then(Value::as_i64)
                                .map(|n| n != 0)
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);
                bad_stop || bad_exit
            });
            if any_failed {
                let cause = described_tasks
                    .iter()
                    .find_map(|t| {
                        t.get("stoppedReason")
                            .and_then(Value::as_str)
                            .map(String::from)
                    })
                    .unwrap_or_else(|| "ECS task failed".to_string());
                return Err(("States.TaskFailed".to_string(), cause));
            }
            return Ok(described);
        }
        if std::time::Instant::now() >= deadline {
            return Err((
                "States.Timeout".to_string(),
                format!(
                    "ecs:RunTask.sync timed out after {}s waiting for {} task(s) to STOP",
                    sync_timeout_secs(timeout_seconds),
                    task_arns.len()
                ),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(SYNC_POLL_INTERVAL_MS)).await;
    }
}

async fn sync_wait_athena_query(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    initial: &Value,
    account_id: &str,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    let qid = initial
        .get("QueryExecutionId")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                "athena:StartQueryExecution.sync: response missing QueryExecutionId".to_string(),
            )
        })?
        .to_string();

    let deadline = sync_deadline(timeout_seconds);
    loop {
        let described = call_sdk_action(
            registry,
            "athena",
            "GetQueryExecution",
            &json!({ "QueryExecutionId": qid }),
            account_id,
        )
        .await?;
        let state = described
            .get("QueryExecution")
            .and_then(|qe| qe.get("Status"))
            .and_then(|s| s.get("State"))
            .and_then(Value::as_str)
            .unwrap_or("");
        match state {
            "SUCCEEDED" => return Ok(described),
            "FAILED" | "CANCELLED" => {
                let cause = described
                    .get("QueryExecution")
                    .and_then(|qe| qe.get("Status"))
                    .and_then(|s| s.get("StateChangeReason"))
                    .and_then(Value::as_str)
                    .unwrap_or("Athena query reached terminal failure state")
                    .to_string();
                return Err(("States.TaskFailed".to_string(), cause));
            }
            _ => {}
        }
        if std::time::Instant::now() >= deadline {
            return Err((
                "States.Timeout".to_string(),
                format!(
                    "athena:StartQueryExecution.sync timed out after {}s for query {qid}",
                    sync_timeout_secs(timeout_seconds)
                ),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(SYNC_POLL_INTERVAL_MS)).await;
    }
}

/// Wait for a nested Step Functions execution to reach a terminal
/// state, then return the bubbled `Output` like AWS does for
/// `arn:aws:states:::states:startExecution.sync`. The initial
/// `StartExecution` response carries `executionArn`; we poll
/// `DescribeExecution` until status leaves `RUNNING`.
async fn sync_wait_states_start_execution(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    initial: &Value,
    account_id: &str,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    let exec_arn = initial
        .get("executionArn")
        .or_else(|| initial.get("ExecutionArn"))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            (
                "States.TaskFailed".to_string(),
                "states:startExecution.sync: response missing executionArn".to_string(),
            )
        })?
        .to_string();

    let deadline = sync_deadline(timeout_seconds);
    loop {
        let described = call_sdk_action(
            registry,
            "states",
            "DescribeExecution",
            &json!({ "executionArn": exec_arn }),
            account_id,
        )
        .await?;
        let status = described
            .get("status")
            .or_else(|| described.get("Status"))
            .and_then(Value::as_str)
            .unwrap_or("");
        match status {
            "SUCCEEDED" => return Ok(described),
            "FAILED" | "TIMED_OUT" | "ABORTED" => {
                let cause = described
                    .get("cause")
                    .or_else(|| described.get("Cause"))
                    .and_then(Value::as_str)
                    .unwrap_or("Nested execution reached terminal failure state")
                    .to_string();
                return Err(("States.TaskFailed".to_string(), cause));
            }
            _ => {}
        }
        if std::time::Instant::now() >= deadline {
            return Err((
                "States.Timeout".to_string(),
                format!(
                    "states:startExecution.sync timed out after {}s for {exec_arn}",
                    sync_timeout_secs(timeout_seconds)
                ),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(SYNC_POLL_INTERVAL_MS)).await;
    }
}

fn sync_timeout_secs(timeout_seconds: Option<u64>) -> u64 {
    timeout_seconds.unwrap_or(SYNC_DEFAULT_TIMEOUT_SECS)
}

fn sync_deadline(timeout_seconds: Option<u64>) -> std::time::Instant {
    std::time::Instant::now() + std::time::Duration::from_secs(sync_timeout_secs(timeout_seconds))
}

#[derive(Clone, Copy)]
pub(crate) enum UpdateClause {
    Set,
    Remove,
    Add,
    Delete,
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

    let payload =
        serde_json::to_string(input).expect("serde_json::Value serialization is infallible");

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

            // The Lambda runtime returns HTTP 200 even for unhandled function
            // errors, encoding the failure in the payload as
            // `{"errorMessage", "errorType", "stackTrace"}`. The delivery trait
            // only conveys bytes, so the payload is the only available signal —
            // the same heuristic the Lambda service uses for async destination
            // routing. Mirror real AWS: a function error fails the task with the
            // function's `errorType` as the Error and the serialized payload as
            // the Cause, which is exactly what makes Retry/Catch on custom
            // exception names work.
            //
            // Require BOTH `errorType` and `errorMessage` (not either) to avoid
            // misreading a legitimate success payload that merely happens to
            // contain one of those field names as a function error.
            if let Some(obj) = value.as_object() {
                if obj.contains_key("errorType") && obj.contains_key("errorMessage") {
                    let error_type = obj
                        .get("errorType")
                        .and_then(Value::as_str)
                        .unwrap_or("Exception")
                        .to_string();
                    let cause = serde_json::to_string(&value)
                        .expect("serde_json::Value serialization is infallible");
                    return Err((error_type, cause));
                }
            }

            Ok(value)
        }
        Some(Err(e)) => {
            // Failures of the Lambda Invoke API call itself surface on real AWS
            // with `Lambda.`-prefixed error names (`Lambda.ServiceException`,
            // `Lambda.SdkClientException`, `Lambda.Unknown`, ...) — never
            // `States.TaskFailed` — and those are the names AWS documents for
            // Retry rules. The delivery error is an unstructured string with
            // the outcome flattened, so the honest default is `Lambda.Unknown`
            // ("outcome unknown"). The one case we can cheaply distinguish is a
            // missing function, which AWS reports as
            // `Lambda.ResourceNotFoundException`.
            if e.starts_with("Function not found") {
                Err(("Lambda.ResourceNotFoundException".to_string(), e))
            } else {
                Err(("Lambda.Unknown".to_string(), e))
            }
        }
        None => {
            // No runtime available — return empty result
            Ok(json!({}))
        }
    }
}

/// Invoke an activity worker. Inserts a `PENDING` token into shared state
/// so a worker can claim it via `GetActivityTask`, then polls until the
/// worker calls `SendTaskSuccess` / `SendTaskFailure` or the heartbeat /
/// timeout windows expire.
async fn invoke_activity(
    activity_arn: &str,
    input: &Value,
    shared_state: &SharedStepFunctionsState,
    timeout_seconds: Option<u64>,
    heartbeat_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    use crate::state::TaskTokenState;

    // Activity must exist (look up across accounts via ARN segment).
    let activity_account = activity_arn.split(':').nth(4).unwrap_or("").to_string();
    {
        let accounts = shared_state.read();
        let exists = accounts
            .get(&activity_account)
            .map(|s| s.activities.contains_key(activity_arn))
            .unwrap_or(false);
        if !exists {
            return Err((
                "States.TaskFailed".to_string(),
                format!("Activity does not exist: {activity_arn}"),
            ));
        }
    }

    let token = format!(
        "FCToken-{}-{}",
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        uuid::Uuid::new_v4().simple(),
    );
    let now = chrono::Utc::now();
    let input_str =
        serde_json::to_string(input).expect("serde_json::Value serialization is infallible");
    {
        let mut accounts = shared_state.write();
        let state = accounts.get_or_create(&activity_account);
        state.task_tokens.insert(
            token.clone(),
            TaskTokenState {
                activity_arn: activity_arn.to_string(),
                status: "PENDING".to_string(),
                output: None,
                error: None,
                cause: None,
                input: Some(input_str),
                created_at: now,
                last_heartbeat_at: None,
                heartbeat_seconds: heartbeat_seconds.map(|s| s as i64),
                timeout_seconds: timeout_seconds.map(|s| s as i64),
            },
        );
    }

    poll_task_token(
        shared_state,
        &activity_account,
        &token,
        timeout_seconds,
        heartbeat_seconds,
    )
    .await
}

pub(crate) enum NextState {
    Name(String),
    End,
    Error(String),
}

#[path = "interpreter_helpers.rs"]
mod interpreter_helpers;
pub(crate) use interpreter_helpers::*;

#[cfg(test)]
#[path = "interpreter_tests.rs"]
mod tests;
