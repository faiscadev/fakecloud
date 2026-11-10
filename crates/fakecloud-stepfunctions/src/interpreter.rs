use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde_json::{json, Value};
use tracing::{debug, warn};

use fakecloud_aws::arn::arn_resource;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_dynamodb::SharedDynamoDbState;

use crate::error_handling::should_retry;
use crate::io_processing::{apply_input_path, apply_output_path, apply_result_path};
use crate::optimized_results::{
    dynamodb_metadata, ecs_run_task_result, ecs_sync_result, glue_start_job_run_result,
    glue_sync_result, lambda_invoke_metadata, sns_metadata, sqs_metadata, start_execution_result,
    sync_execution_result, sync_failure_cause, SyncForm,
};
use crate::sdk_protocol::{Caller, DateStyle};
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
    let role_arn = execution_role_arn(&state, &execution_arn);
    add_event(
        &state,
        &execution_arn,
        "ExecutionStarted",
        0,
        json!({
            "input": serde_json::to_string(&raw_input).expect("serde_json::Value serialization is infallible"),
            "roleArn": role_arn
        }),
    );

    // Run the state machine inside an inner tokio::spawn so that any panic
    // bubbles up as a JoinError instead of tearing down the caller. Without
    // this the panic propagates through the outer spawn in `start_execution`
    // which leaves the execution stuck in Running and leaks the panic to
    // tokio's default hook.
    let mut def_owned = def;
    let state_clone = state.clone();
    let execution_arn_clone = execution_arn.clone();
    let delivery_clone = delivery.clone();
    let dynamodb_state_clone = dynamodb_state.clone();
    let registry_clone = registry.clone();
    let handle = tokio::spawn(crate::variables::in_scope(
        crate::variables::VariableScope::root(),
        async move {
            crate::jsonata::propagate_query_language(&mut def_owned, None);
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
        },
    ));

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
    /// A failure a catcher took; the state loop applies the catcher's fields.
    Caught(CaughtError),
}

/// `Assign` on a JSONPath Wait state reads the effective input.
fn assign_wait_variables(
    name: &str,
    state_def: &Value,
    input: &Value,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Result<(), (String, String)> {
    if crate::jsonata::is_jsonata(state_def) || state_def.get("Assign").is_none() {
        return Ok(());
    }
    let effective = match state_def["InputPath"].as_str() {
        Some("null") => json!({}),
        path => apply_input_path(input, path)?,
    };
    let ctx = context_object(shared_state, execution_arn, name, Utc::now(), 0);
    crate::variables::assign_jsonpath(state_def.get("Assign"), &effective, Some(&ctx))
}

async fn run_wait_state(
    entry: &StateEntry<'_>,
    state_def: &Value,
    input: Value,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let ctx = entry.context(shared_state, execution_arn, 0);
    if let Err(failure) = execute_wait_state(state_def, &input, &ctx).await {
        return fail_own(shared_state, execution_arn, entry, failure);
    }
    if let Err(failure) =
        assign_wait_variables(entry.name, state_def, &input, shared_state, execution_arn)
    {
        return fail_own(shared_state, execution_arn, entry, failure);
    }

    add_state_exit_event(
        state_def,
        shared_state,
        execution_arn,
        "WaitStateExited",
        entry.entered,
        json!({
            "name": entry.name,
            "output": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
        }),
    );

    advance_from_next(state_def, input)
}

/// How a Task, Parallel or Map state failed.
#[derive(Debug)]
pub(crate) enum StateFailure {
    /// Its own fields failed: paths, templates, expressions or settings. It is
    /// reported in the state (see [`own_failure`]).
    Own((String, String)),
    /// What it ran failed: a task's resource, a branch or an iteration. That has
    /// been recorded where it happened, and the failure goes on as it is.
    Ran((String, String)),
}

impl From<(String, String)> for StateFailure {
    fn from(failure: (String, String)) -> Self {
        StateFailure::Own(failure)
    }
}

/// A Task, Parallel or Map state's outcome: its output on to the next state, or
/// its failure.
fn task_like_outcome(
    entry: &StateEntry<'_>,
    state_def: &Value,
    exited_type: &str,
    result: Result<Value, StateFailure>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    match result {
        Ok(output) => {
            // A JSONata state exits once its `Output` has evaluated: a Map's
            // has by now (see `finish_map_output`), any other's hasn't.
            let exits_here =
                !crate::jsonata::is_jsonata(state_def) || exited_type == "MapStateExited";
            if exits_here {
                add_event(
                    shared_state,
                    execution_arn,
                    exited_type,
                    entry.entered,
                    json!({
                        "name": entry.name,
                        "output": serde_json::to_string(&output).expect("serde_json::Value serialization is infallible"),
                    }),
                );
            }
            advance_from_next(state_def, output)
        }
        Err(StateFailure::Own(failure)) => fail_own(shared_state, execution_arn, entry, failure),
        Err(StateFailure::Ran((error, cause))) => Advance::Fail(error, cause),
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_task_state(
    entry: &StateEntry<'_>,
    state_def: &Value,
    input: Value,
    attempt: &mut u32,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let result = execute_task_state(
        entry,
        state_def,
        &input,
        attempt,
        delivery,
        dynamodb_state,
        registry,
        shared_state,
        execution_arn,
    )
    .await;
    task_like_outcome(
        entry,
        state_def,
        "TaskStateExited",
        result,
        shared_state,
        execution_arn,
    )
}

/// A Parallel state, on attempt `attempt` of the whole state.
#[allow(clippy::too_many_arguments)]
async fn run_parallel_state(
    entry: &StateEntry<'_>,
    state_def: &Value,
    input: Value,
    attempt: u32,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let ctx = entry.context(shared_state, execution_arn, attempt);
    let result = execute_parallel_state(
        state_def,
        &ctx,
        &input,
        entry.entered,
        delivery,
        dynamodb_state,
        registry,
        shared_state,
        execution_arn,
    )
    .await;
    task_like_outcome(
        entry,
        state_def,
        "ParallelStateExited",
        result,
        shared_state,
        execution_arn,
    )
}

/// A Map state, on attempt `attempt` of the whole state.
#[allow(clippy::too_many_arguments)]
async fn run_map_state(
    entry: &StateEntry<'_>,
    state_def: &Value,
    input: Value,
    attempt: u32,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Advance {
    let ctx = entry.context(shared_state, execution_arn, attempt);
    let result = execute_map_state(
        state_def,
        &ctx,
        &input,
        entry.entered,
        delivery,
        dynamodb_state,
        registry,
        shared_state,
        execution_arn,
    )
    .await;
    task_like_outcome(
        entry,
        state_def,
        "MapStateExited",
        result,
        shared_state,
        execution_arn,
    )
}

/// The longest a Wait state waits, in seconds, as recorded: one more fails it.
const MAX_WAIT_SECONDS: u64 = 2_147_483_646;

fn wait_delay(seconds: u64) -> Result<tokio::time::Duration, (String, String)> {
    if seconds > MAX_WAIT_SECONDS {
        return Err(crate::io_processing::runtime_error(
            "Wait state delay is too long.".to_string(),
        ));
    }
    Ok(tokio::time::Duration::from_secs(seconds))
}

/// Execute a Wait state: pause execution for a specified duration or until a timestamp.
/// A `SecondsPath` / `TimestampPath` that matches nothing, or selects a value
/// of the wrong type, fails the state with `States.Runtime`, as on AWS.
async fn execute_wait_state(
    state_def: &Value,
    input: &Value,
    ctx: &Value,
) -> Result<(), (String, String)> {
    let text_of = crate::io_processing::value_text;

    if let Some(seconds) = state_def.get("Seconds") {
        let seconds = seconds.as_u64().ok_or_else(|| {
            crate::io_processing::runtime_error(format!(
                "The Seconds field must be a non-negative integer but was: {seconds}"
            ))
        })?;
        tokio::time::sleep(wait_delay(seconds)?).await;
        return Ok(());
    }

    if let Some(path) = state_def["SecondsPath"].as_str() {
        let val = crate::io_processing::resolve_reference_with_context(input, Some(ctx), path)?;
        let seconds = val.as_u64().ok_or_else(|| {
            crate::io_processing::runtime_error(format!(
                "The SecondsPath parameter cannot be parsed as a long value: {path} == {}",
                text_of(&val)
            ))
        })?;
        tokio::time::sleep(wait_delay(seconds)?).await;
        return Ok(());
    }

    let target = if let Some(timestamp) = state_def.get("Timestamp") {
        let text = text_of(timestamp);
        Some(chrono::DateTime::parse_from_rfc3339(&text).map_err(|_| {
            crate::io_processing::runtime_error(format!(
                "The Timestamp field cannot be parsed as an ISO-8601 extended offset date-time \
                 format string: {text}"
            ))
        })?)
    } else if let Some(path) = state_def["TimestampPath"].as_str() {
        let val = crate::io_processing::resolve_reference_with_context(input, Some(ctx), path)?;
        let text = text_of(&val);
        Some(chrono::DateTime::parse_from_rfc3339(&text).map_err(|_| {
            crate::io_processing::runtime_error(format!(
                "The TimestampPath parameter does not reference a valid ISO-8601 extended offset \
                 date-time format string: {path} == {text}"
            ))
        })?)
    } else {
        warn!("Wait state has no Seconds, SecondsPath, Timestamp, or TimestampPath: skipping wait");
        return Ok(());
    };

    if let Some(target) = target {
        let now = Utc::now();
        let target_utc = target.with_timezone(&chrono::Utc);
        if target_utc > now {
            let duration = (target_utc - now).to_std().unwrap_or_default();
            tokio::time::sleep(duration).await;
        }
    }
    Ok(())
}

/// Turn a Task's raw result into its output.
#[allow(clippy::too_many_arguments)]
fn finish_task_result(
    state_def: &Value,
    name: &str,
    input: &Value,
    result: Value,
    entered: chrono::DateTime<Utc>,
    attempt: u32,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Result<Value, (String, String)> {
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();
    let ctx = context_object(shared_state, execution_arn, name, entered, attempt);
    let selected = match state_def.get("ResultSelector") {
        Some(selector) => apply_parameters(selector, &result, Some(&ctx))?,
        None => result.clone(),
    };
    let after_result = apply_result_path(input, &selected, result_path)?;
    let output = if output_path == Some("null") {
        json!({})
    } else {
        apply_output_path(&after_result, output_path)?
    };
    // `$` is the raw result. Staged last: a state failing above assigns nothing.
    if !crate::jsonata::is_jsonata(state_def) {
        crate::variables::assign_jsonpath(state_def.get("Assign"), &result, Some(&ctx))?;
    }
    Ok(output)
}

/// The most a task's result can hold, per the ASL service quota.
const MAX_TASK_RESULT_BYTES: usize = 256 * 1024;

/// A result over the quota fails the state with an error no wildcard catches.
fn within_result_limit(resource: &str, result: Value) -> Result<Value, (String, String)> {
    if serde_json::to_vec(&result).map_or(0, |bytes| bytes.len()) <= MAX_TASK_RESULT_BYTES {
        return Ok(result);
    }
    // As recorded, an SDK integration goes by its service: `aws-sdk:s3`.
    let task = resource
        .split_once(":::aws-sdk:")
        .map(|(_, tail)| format!("aws-sdk:{}", tail.split(':').next().unwrap_or_default()))
        .unwrap_or_else(|| resource.to_string());
    Err((
        "States.DataLimitExceeded".to_string(),
        format!(
            "The state/task '{task}' returned a result with a size exceeding the maximum number \
             of bytes service limit."
        ),
    ))
}

/// Execute a Task state: invoke the resource (Lambda, SQS, SNS, EventBridge, DynamoDB),
/// apply I/O processing, handle Retry.
#[allow(clippy::too_many_arguments)]
async fn execute_task_state(
    entry: &StateEntry<'_>,
    state_def: &Value,
    input: &Value,
    attempt: &mut u32,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Result<Value, StateFailure> {
    let name = entry.name;
    let entered = entry.entered_at;
    let entered_event_id = entry.entered;
    let resource = state_def["Resource"].as_str().unwrap_or("").to_string();
    let base_ctx = context_object(shared_state, execution_arn, name, entered, 0);

    let input_path = state_def["InputPath"].as_str();

    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)?
    };

    let retriers: &[Value] = state_def["Retry"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let timeout_seconds = state_def["TimeoutSeconds"].as_u64();
    let heartbeat_seconds = state_def["HeartbeatSeconds"].as_u64();

    let is_wait_for_task_token = resource.contains(".waitForTaskToken");
    let task_token = if is_wait_for_task_token {
        let token = format!(
            "FCToken-{}-{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            uuid::Uuid::new_v4().simple(),
        );
        let account_id = account_id_from_arn(execution_arn);
        let mut context = base_ctx.clone();
        context["Task"] = json!({ "Token": token.clone() });
        {
            let mut accounts = shared_state.write();
            let region = arn_region(execution_arn, accounts.region());
            let state = accounts.regional_mut(account_id, &region);
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
                    heartbeat_seconds: heartbeat_seconds
                        .map(|s| i64::try_from(s).unwrap_or(i64::MAX)),
                    timeout_seconds: timeout_seconds.map(|s| s as i64),
                },
            );
        }
        Some((token, context))
    } else {
        None
    };

    loop {
        // Parameters are re-evaluated for every attempt, so
        // `$$.State.RetryCount` reflects the current retry as on AWS.
        let task_input = if let Some(params) = state_def.get("Parameters") {
            let mut ctx = context_object(shared_state, execution_arn, name, entered, *attempt);
            if let Some((_, token_ctx)) = &task_token {
                ctx["Task"] = token_ctx["Task"].clone();
            }
            apply_parameters(params, &effective_input, Some(&ctx))?
        } else if let Some(arguments) = state_def.get("Arguments") {
            let mut ctx = context_object(shared_state, execution_arn, name, entered, *attempt);
            if let Some((_, token_ctx)) = &task_token {
                ctx["Task"] = token_ctx["Task"].clone();
            }
            // On failure nothing was scheduled; the state loop decides on `Retry`.
            crate::jsonata::eval_template(
                "Arguments",
                arguments,
                &crate::jsonata::Scope {
                    input: &effective_input,
                    context: &ctx,
                    result: None,
                    error_output: None,
                },
            )?
        } else {
            effective_input.clone()
        };
        add_event(
            shared_state,
            execution_arn,
            "TaskScheduled",
            entered_event_id,
            json!({
                "resource": resource,
                "region": region_from_execution_arn(execution_arn),
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
        .await
        .and_then(|result| within_result_limit(&resource, result));

        match invoke_result {
            Ok(result) => {
                if let Some((token, _)) = &task_token {
                    let account_id = account_id_from_arn(execution_arn);
                    let region = arn_region(execution_arn, shared_state.read().region());
                    match poll_task_token(
                        shared_state,
                        account_id,
                        &region,
                        token,
                        timeout_seconds,
                        heartbeat_seconds,
                    )
                    .await
                    .and_then(|output| within_result_limit(&resource, output))
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

                            return Ok(finish_task_result(
                                state_def,
                                name,
                                input,
                                output,
                                entered,
                                *attempt,
                                shared_state,
                                execution_arn,
                            )?);
                        }
                        Err((error, cause)) => {
                            add_event(
                                shared_state,
                                execution_arn,
                                "TaskFailed",
                                entered_event_id,
                                json!({ "error": error, "cause": cause }),
                            );

                            if let Some(delay_ms) = should_retry(retriers, &error, *attempt, true) {
                                *attempt += 1;
                                // Honour the ASL-computed backoff (already bounded
                                // by MaxDelaySeconds inside `should_retry`).
                                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms))
                                    .await;
                                continue;
                            }

                            return Err(StateFailure::Ran((error, cause)));
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

                return Ok(finish_task_result(
                    state_def,
                    name,
                    input,
                    result,
                    entered,
                    *attempt,
                    shared_state,
                    execution_arn,
                )?);
            }
            Err((error, cause)) => {
                add_event(
                    shared_state,
                    execution_arn,
                    "TaskFailed",
                    entered_event_id,
                    json!({ "error": error, "cause": cause }),
                );

                if let Some(delay_ms) = should_retry(retriers, &error, *attempt, true) {
                    *attempt += 1;
                    // Honour the ASL-computed backoff (already bounded by
                    // MaxDelaySeconds inside `should_retry`).
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                    continue;
                }

                return Err(StateFailure::Ran((error, cause)));
            }
        }
    }
}

/// Execute a Parallel state: run all branches concurrently, collect results into an array.
#[allow(clippy::too_many_arguments)]
async fn execute_parallel_state(
    state_def: &Value,
    ctx: &Value,
    input: &Value,
    entered_event_id: i64,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Result<Value, StateFailure> {
    let input_path = state_def["InputPath"].as_str();
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)?
    };
    // A JSONPath state's `Parameters` give each branch its input, evaluated per
    // attempt (`$$.State.RetryCount`), as recorded. A JSONata state's
    // `Arguments` were evaluated into `input` when it was prepared.
    let effective_input = match state_def.get("Parameters") {
        Some(template) if !crate::jsonata::is_jsonata(state_def) => {
            apply_parameters(template, &effective_input, Some(ctx))?
        }
        _ => effective_input,
    };

    let branches = state_def["Branches"]
        .as_array()
        .cloned()
        .unwrap_or_default();

    if branches.is_empty() {
        return Err(StateFailure::Own((
            "States.Runtime".to_string(),
            "Parallel state has no Branches".to_string(),
        )));
    }

    add_event(
        shared_state,
        execution_arn,
        "ParallelStateStarted",
        entered_event_id,
        json!({}),
    );

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

        let scope = crate::variables::child_scope(true);
        handles.push(tokio::spawn(crate::variables::in_scope(scope, async move {
            run_states(&branch, branch_input, &delivery, &ddb, &reg, &state, &arn).await
        })));
    }

    // Collect results in order
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        let result = handle.await.map_err(|e| {
            (
                "States.Runtime".to_string(),
                format!("Branch execution panicked: {e}"),
            )
        })?;
        results.push(result.map_err(StateFailure::Ran)?);
    }

    // The branches have succeeded, whatever becomes of their results, as
    // recorded: a failing `ResultSelector` follows this event.
    add_event(
        shared_state,
        execution_arn,
        "ParallelStateSucceeded",
        entered_event_id,
        json!({}),
    );

    let branch_output = Value::Array(results);

    // Apply ResultSelector if present
    let selected = if let Some(selector) = state_def.get("ResultSelector") {
        apply_parameters(selector, &branch_output, Some(ctx))?
    } else {
        branch_output.clone()
    };

    // Apply ResultPath
    let after_result = apply_result_path(input, &selected, result_path)?;

    // Apply OutputPath
    let output = if output_path == Some("null") {
        json!({})
    } else {
        apply_output_path(&after_result, output_path)?
    };
    if !crate::jsonata::is_jsonata(state_def) {
        crate::variables::assign_jsonpath(state_def.get("Assign"), &branch_output, Some(ctx))?;
    }

    Ok(output)
}

/// Execute a Map state: iterate over an array and run a sub-state machine per item.
/// Supports distributed mode: ItemReader (S3), ItemBatcher, ResultWriter (S3),
/// ToleratedFailurePercentage, and MaxConcurrencyPath.
#[allow(clippy::too_many_arguments)]
async fn execute_map_state(
    state_def: &Value,
    ctx: &Value,
    input: &Value,
    entered_event_id: i64,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
) -> Result<Value, StateFailure> {
    // A distributed Map (`ItemProcessor.ProcessorConfig.Mode == "DISTRIBUTED"`)
    // materialises a MapRun record that `ListMapRuns` / `DescribeMapRun`
    // surface, from before it reads its items. Inline Map states do not create
    // one, matching AWS.
    let is_distributed = state_def
        .get("ItemProcessor")
        .or_else(|| state_def.get("Iterator"))
        .and_then(|processor| processor["ProcessorConfig"]["Mode"].as_str())
        == Some("DISTRIBUTED");
    let map_run_arn = if is_distributed {
        map_run_arn_for(execution_arn)
    } else {
        None
    };
    if let Some(arn) = &map_run_arn {
        start_map_run(shared_state, arn, execution_arn, state_def);
    }

    let result = run_map_iterations(
        state_def,
        ctx,
        input,
        delivery,
        dynamodb_state,
        registry,
        shared_state,
        execution_arn,
        map_run_arn.as_deref(),
    )
    .await;
    let fail_run = |failure: &StateFailure, failed_event: bool| {
        let Some(arn) = &map_run_arn else {
            return;
        };
        finish_map_run(shared_state, arn, execution_arn, None, "FAILED");
        if failed_event {
            let (StateFailure::Own((error, cause)) | StateFailure::Ran((error, cause))) = failure;
            add_event(
                shared_state,
                execution_arn,
                "MapRunFailed",
                0,
                json!({ "error": error, "cause": cause }),
            );
        }
    };

    let results = match result {
        Ok(results) => results,
        Err(MapRunError {
            failure,
            failed_event,
        }) => {
            fail_run(&failure, failed_event);
            return Err(failure);
        }
    };
    // The iterations have succeeded, whatever becomes of their results, as
    // recorded: a failing `ResultSelector` follows this event.
    add_event(
        shared_state,
        execution_arn,
        "MapStateSucceeded",
        entered_event_id,
        json!({}),
    );

    // A map run ends with its state's own processing: one failing here fails it.
    match finish_map_output(state_def, ctx, input, &results.output) {
        Ok(output) => {
            if let Some(arn) = &map_run_arn {
                finish_map_run(
                    shared_state,
                    arn,
                    execution_arn,
                    Some(results.tallies),
                    "SUCCEEDED",
                );
            }
            Ok(output)
        }
        Err(failure) => {
            let failure = StateFailure::Own(failure);
            fail_run(&failure, true);
            Err(failure)
        }
    }
}

/// A Map state's own processing of its iterations' results: a JSONPath state's
/// `ResultSelector`, `ResultPath`, `OutputPath` and `Assign`, a JSONata state's
/// `Assign` and `Output`. A map run ends with it.
fn finish_map_output(
    state_def: &Value,
    ctx: &Value,
    input: &Value,
    map_output: &Value,
) -> Result<Value, (String, String)> {
    if crate::jsonata::is_jsonata(state_def) {
        return crate::jsonata::finish_state(state_def, "", input, ctx, map_output.clone(), false);
    }
    let selected = match state_def.get("ResultSelector") {
        Some(selector) => apply_parameters(selector, map_output, Some(ctx))?,
        None => map_output.clone(),
    };
    let after_result = apply_result_path(input, &selected, state_def["ResultPath"].as_str())?;
    let output = match state_def["OutputPath"].as_str() {
        Some("null") => json!({}),
        output_path => apply_output_path(&after_result, output_path)?,
    };
    crate::variables::assign_jsonpath(state_def.get("Assign"), map_output, Some(ctx))?;
    Ok(output)
}

/// A Map's iterations, done: their results and the map run's tallies
/// (succeeded, failed).
struct MapResults {
    output: Value,
    tallies: (i64, i64),
}

/// How a Map's iterations failed, and whether its map run records it with a
/// `MapRunFailed` event: as recorded, a tolerance that isn't one does not.
struct MapRunError {
    failure: StateFailure,
    failed_event: bool,
}

impl From<StateFailure> for MapRunError {
    fn from(failure: StateFailure) -> Self {
        MapRunError {
            failure,
            failed_event: true,
        }
    }
}

impl From<(String, String)> for MapRunError {
    fn from(failure: (String, String)) -> Self {
        StateFailure::Own(failure).into()
    }
}

/// How many failed iterations a map run tolerates. With neither limit set, none;
/// when both are set, passing either fails the run.
#[derive(Clone, Copy, Debug)]
struct FailureTolerance {
    count: Option<u64>,
    percentage: Option<f64>,
}

impl FailureTolerance {
    fn resolve(
        state_def: &Value,
        jsonata: bool,
        input: &Value,
        ctx: &Value,
    ) -> Result<Self, (String, String)> {
        let resolved = |path: &str, field: &str| -> Result<Option<Value>, (String, String)> {
            match state_def[path].as_str() {
                Some(reference) => crate::io_processing::resolve_reference_with_context(
                    input,
                    Some(ctx),
                    reference,
                )
                .map(Some),
                None => Ok(state_def.get(field).cloned()),
            }
        };
        let invalid = |field: &str, value: &Value, wanted: &str| {
            crate::io_processing::runtime_error(format!(
                "The {field} must be {wanted}, got {value}"
            ))
        };
        let count = match resolved("ToleratedFailureCountPath", "ToleratedFailureCount")? {
            None => None,
            // As recorded: a JSONata expression's whole number counts, `2.0`
            // too; a path's value must be an integer as it stands.
            Some(value) if jsonata => {
                let query = |cause: String| ("States.QueryEvaluationError".to_string(), cause);
                let whole = crate::jsonata::whole_number(&value).ok_or_else(|| {
                    query(format!(
                        "The ToleratedFailureCount field cannot be parsed as an integer: {value}"
                    ))
                })?;
                // Worded as a negative Wait `Seconds` is, as recorded there.
                Some(u64::try_from(whole).map_err(|_| {
                    query(format!(
                        "ToleratedFailureCount cannot be negative but was: {value}"
                    ))
                })?)
            }
            Some(value) => Some(value.as_u64().ok_or_else(|| {
                match state_def["ToleratedFailureCountPath"].as_str() {
                    Some(path) => {
                        let text = crate::io_processing::value_text(&value);
                        crate::io_processing::runtime_error(format!(
                            "The ToleratedFailureCountPath field refers to value \"{text}\" which \
                         is not a valid integer: {path}"
                        ))
                    }
                    None => invalid("ToleratedFailureCount", &value, "a non-negative integer"),
                }
            })?),
        };
        let percentage = match resolved(
            "ToleratedFailurePercentagePath",
            "ToleratedFailurePercentage",
        )? {
            None => None,
            Some(value) => Some(
                value
                    .as_f64()
                    .filter(|p| (0.0..=100.0).contains(p))
                    .ok_or_else(|| {
                        invalid(
                            "ToleratedFailurePercentage",
                            &value,
                            "a number from 0 to 100",
                        )
                    })?,
            ),
        };
        Ok(Self { count, percentage })
    }

    fn exceeded(&self, failed: usize, total: usize) -> bool {
        let over_count = self.count.map(|limit| failed as u64 > limit);
        let over_percentage = self
            .percentage
            .map(|limit| failed as f64 / total.max(1) as f64 * 100.0 > limit);
        match (over_count, over_percentage) {
            (None, None) => failed > 0,
            (count, percentage) => count == Some(true) || percentage == Some(true),
        }
    }
}

/// Stops a failing Map starting new iterations. For a map run, as recorded (40
/// items, concurrency 2, no tolerance: 3 ran, 37 stayed pending), the dispatcher
/// has one `MaxConcurrency` window committed beyond the iterations already
/// finished when the threshold is crossed. An inline Map has no such window.
struct MapProgress {
    window: usize,
    started: std::sync::atomic::AtomicUsize,
    finished: std::sync::atomic::AtomicUsize,
    failed: std::sync::atomic::AtomicUsize,
    /// How many iterations may start in all, once the run is failing.
    start_limit: std::sync::atomic::AtomicUsize,
}

impl MapProgress {
    fn new(window: usize) -> Self {
        Self {
            window,
            started: Default::default(),
            finished: Default::default(),
            failed: Default::default(),
            start_limit: std::sync::atomic::AtomicUsize::new(usize::MAX),
        }
    }

    fn may_start(&self) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        if self.started.load(SeqCst) >= self.start_limit.load(SeqCst) {
            return false;
        }
        self.started.fetch_add(1, SeqCst);
        true
    }

    fn record_finish(&self, is_failure: bool, tolerance: FailureTolerance, total: usize) {
        use std::sync::atomic::Ordering::SeqCst;
        let finished = self.finished.fetch_add(1, SeqCst) + 1;
        if !is_failure {
            return;
        }
        let failed = self.failed.fetch_add(1, SeqCst) + 1;
        if tolerance.exceeded(failed, total) {
            let limit = finished + self.window;
            let _ = self
                .start_limit
                .compare_exchange(usize::MAX, limit, SeqCst, SeqCst);
        }
    }
}

const THRESHOLD_ERROR: &str = "States.ExceedToleratedFailureThreshold";
const THRESHOLD_CAUSE: &str = "The specified tolerated failure threshold was exceeded";

#[allow(clippy::too_many_arguments)]
async fn run_map_iterations(
    state_def: &Value,
    ctx: &Value,
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    dynamodb_state: &Option<SharedDynamoDbState>,
    registry: &Option<SharedServiceRegistry>,
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
    map_run_arn: Option<&str>,
) -> Result<MapResults, MapRunError> {
    let jsonata = crate::jsonata::is_jsonata(state_def);
    let input_path = state_def["InputPath"].as_str();

    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)?
    };

    // Resolve MaxConcurrencyPath if present
    let max_concurrency = if let Some(path) = state_def["MaxConcurrencyPath"].as_str() {
        crate::io_processing::resolve_reference_with_context(&effective_input, Some(ctx), path)?
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

    // A ResultWriter that cannot be honored must fail before any iteration runs.
    let result_writer = state_def.get("ResultWriter");
    let writer_config = result_writer
        .map(crate::map_s3::WriterConfig::parse)
        .transpose()?;

    // Read items from ItemReader (S3) or ItemsPath
    let items = if let Some(item_reader) = state_def.get("ItemReader") {
        read_items_from_s3(
            item_reader,
            jsonata,
            &effective_input,
            ctx,
            registry,
            execution_arn,
        )
        .await?
    } else {
        // A JSONata state's `Items`, already evaluated, or a JSONPath `ItemsPath`.
        let items_value = match state_def.get("Items") {
            Some(items) => items.clone(),
            None => crate::io_processing::resolve_reference_with_context(
                &effective_input,
                Some(ctx),
                state_def["ItemsPath"].as_str().unwrap_or("$"),
            )?,
        };
        match items_value {
            Value::Array(items) => items,
            other => {
                return Err(crate::io_processing::runtime_error(format!(
                    "Map state input must be an array but was: {}",
                    json_type_name(&other)
                ))
                .into())
            }
        }
    };

    // Apply ItemBatcher if present
    let batch_config = state_def.get("ItemBatcher").cloned();
    let batched_items = if let Some(ref batcher) = batch_config {
        apply_item_batcher(&items, batcher, jsonata, &effective_input, ctx)?
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

    let total_items = batched_items.len();
    add_event(
        shared_state,
        execution_arn,
        "MapStateStarted",
        0,
        json!({ "length": total_items }),
    );
    // As recorded, a tolerance that isn't one fails the state once it has started.
    let tolerance = FailureTolerance::resolve(state_def, jsonata, &effective_input, ctx).map_err(
        |failure| MapRunError {
            failure: StateFailure::Own(failure),
            failed_event: false,
        },
    )?;
    if let Some(arn) = map_run_arn {
        set_map_run_total(
            shared_state,
            arn,
            execution_arn,
            total_items,
            max_concurrency,
            tolerance,
        );
    }

    // Apply ItemSelector to every item before any iteration starts, so a
    // failing selector fails the state without leaving earlier iterations
    // running in the background. `$` paths read the Map state's effective
    // input; the current item is exposed through the context object as
    // `$$.Map.Item.Value` / `$$.Map.Item.Index`, as on AWS.
    // `Parameters` is a JSONPath Map's older name for `ItemSelector`.
    let item_selector = state_def
        .get("ItemSelector")
        .or_else(|| (!jsonata).then(|| state_def.get("Parameters")).flatten());
    let item_inputs: Vec<Value> = match item_selector {
        Some(selector) => {
            let mut out = Vec::with_capacity(batched_items.len());
            for (index, batch_item) in batched_items.into_iter().enumerate() {
                let mut item_ctx = ctx.clone();
                item_ctx["Map"] = json!({"Item": {"Value": batch_item, "Index": index}});
                let selected = if jsonata {
                    crate::jsonata::eval_template(
                        "ItemSelector",
                        selector,
                        &crate::jsonata::Scope {
                            input: &effective_input,
                            context: &item_ctx,
                            result: None,
                            error_output: None,
                        },
                    )?
                } else {
                    apply_parameters(selector, &effective_input, Some(&item_ctx))?
                };
                out.push(selected);
            }
            out
        }
        None => batched_items,
    };

    let semaphore = Arc::new(tokio::sync::Semaphore::new(effective_concurrency));
    let is_distributed = map_run_arn.is_some();
    let progress = Arc::new(MapProgress::new(if is_distributed {
        effective_concurrency
    } else {
        0
    }));

    // Process all items
    let mut handles = Vec::new();
    for (index, item_input) in item_inputs.into_iter().enumerate() {
        let iter_def = iterator_def.clone();
        let delivery = delivery.clone();
        let ddb = dynamodb_state.clone();
        let reg = registry.clone();
        let state = shared_state.clone();
        let arn = execution_arn.to_string();
        // Iterations start in item order, so what a failing run leaves pending is
        // the items at the end. A permit is held until the iteration has finished.
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ("States.Runtime".to_string(), "Semaphore closed".to_string()))?;
        let may_start = progress.may_start();
        let recorded_input = result_writer.map(|_| item_input.clone());
        let progress = progress.clone();

        if may_start {
            add_event(
                shared_state,
                execution_arn,
                "MapIterationStarted",
                0,
                json!({ "index": index }),
            );
        }

        // A distributed Map's iterations cannot read outer variables.
        let scope = crate::variables::child_scope(!is_distributed);
        handles.push(tokio::spawn(crate::variables::in_scope(
            scope,
            async move {
                let _permit = permit;
                let started = Utc::now();
                if !may_start {
                    let record = recorded_input.map(|input| crate::map_s3::IterationRecord {
                        input,
                        started,
                        stopped: started,
                        outcome: crate::map_s3::IterationOutcome::Pending,
                    });
                    return Ok::<_, (String, String)>((index, None, record));
                }
                let result =
                    run_states(&iter_def, item_input, &delivery, &ddb, &reg, &state, &arn).await;
                progress.record_finish(result.is_err(), tolerance, total_items);
                let record = recorded_input.map(|input| crate::map_s3::IterationRecord {
                    input,
                    started,
                    stopped: Utc::now(),
                    outcome: match &result {
                        Ok(output) => crate::map_s3::IterationOutcome::Succeeded(output.clone()),
                        Err((error, cause)) => {
                            crate::map_s3::IterationOutcome::Failed(error.clone(), cause.clone())
                        }
                    },
                });
                Ok((index, Some(result), record))
            },
        )));
    }

    // Collect results in order
    let mut results: Vec<(usize, Value)> = Vec::with_capacity(handles.len());
    let mut iteration_records = Vec::new();
    let mut failure_count = 0usize;
    for handle in handles {
        let (index, result, record) = handle.await.map_err(|e| {
            (
                "States.Runtime".to_string(),
                format!("Map iteration panicked: {e}"),
            )
        })??;
        iteration_records.extend(record);

        match result {
            Some(Ok(output)) => {
                add_event(
                    shared_state,
                    execution_arn,
                    "MapIterationSucceeded",
                    0,
                    json!({ "index": index }),
                );
                results.push((index, output));
            }
            // As recorded, an inline iteration failing so that the execution
            // ends records no MapIterationFailed: the execution fails at once. A
            // map run's iterations are child executions, which fail on their own.
            Some(Err((error, cause)))
                if !is_distributed && crate::error_handling::ends_execution(&error) =>
            {
                return Err(StateFailure::Ran((error, cause)).into());
            }
            Some(Err((error, cause))) => {
                add_event(
                    shared_state,
                    execution_arn,
                    "MapIterationFailed",
                    0,
                    json!({ "index": index, "error": error }),
                );
                failure_count += 1;
                // An inline Map fails with the iteration's own error, at once.
                // A map run finishes its window and reports the threshold.
                if !is_distributed && tolerance.exceeded(failure_count, total_items) {
                    return Err(StateFailure::Ran((error, cause)).into());
                }
            }
            None => {}
        }
    }

    // Sort by index to maintain order
    results.sort_by_key(|(i, _)| *i);
    let succeeded_count = results.len() as i64;
    let threshold_exceeded = is_distributed && tolerance.exceeded(failure_count, total_items);

    // With a ResultWriter the results take its form, and go to S3 when it names
    // a destination, whether or not the run went on to fail.
    let map_output = match (result_writer, writer_config) {
        (Some(writer), Some(config)) => {
            write_map_results(
                writer,
                jsonata,
                config,
                &effective_input,
                ctx,
                registry,
                execution_arn,
                map_run_arn,
                &iteration_records,
            )
            .await
        }
        _ => Ok(Value::Array(results.into_iter().map(|(_, v)| v).collect())),
    };

    if threshold_exceeded {
        if let Some(arn) = map_run_arn {
            finish_map_run(
                shared_state,
                arn,
                execution_arn,
                Some((succeeded_count, failure_count as i64)),
                "FAILED",
            );
        }
        return Err(
            StateFailure::Ran((THRESHOLD_ERROR.to_string(), THRESHOLD_CAUSE.to_string())).into(),
        );
    }

    // Every iteration is accounted for and the tolerated-failure threshold was
    // not exceeded. Results that weren't delivered fail the map run; otherwise
    // it ends with the state's own processing of them.
    let tallies = (succeeded_count, failure_count as i64);
    match map_output {
        Ok(output) => Ok(MapResults { output, tallies }),
        Err(failure) => {
            if let Some(arn) = map_run_arn {
                finish_map_run(shared_state, arn, execution_arn, Some(tallies), "FAILED");
            }
            Err(failure.into())
        }
    }
}

/// Build a MapRun ARN for a distributed Map running inside `execution_arn`.
/// `arn:aws:states:R:A:execution:SM:Exec` -> `arn:aws:states:R:A:mapRun:SM/Exec:<label>`.
fn map_run_arn_for(execution_arn: &str) -> Option<String> {
    let (prefix, tail) = execution_arn.split_once(":execution:")?;
    // `tail` is `StateMachineName:ExecutionName`; the MapRun ARN joins them with
    // a slash and appends a unique label.
    let sm_exec = tail.replacen(':', "/", 1);
    let label = uuid::Uuid::new_v4();
    Some(format!("{prefix}:mapRun:{sm_exec}:{label}"))
}

/// Persist a fresh RUNNING MapRun record for a distributed Map, from before it
/// knows how many items it has.
fn start_map_run(
    shared_state: &SharedStepFunctionsState,
    map_run_arn: &str,
    execution_arn: &str,
    state_def: &Value,
) {
    let account = account_id_from_arn(execution_arn).to_string();
    let mut accounts = shared_state.write();
    let region = arn_region(execution_arn, accounts.region());
    let s = accounts.regional_mut(&account, &region);
    s.map_runs.insert(
        map_run_arn.to_string(),
        MapRun {
            map_run_arn: map_run_arn.to_string(),
            execution_arn: execution_arn.to_string(),
            max_concurrency: state_def["MaxConcurrency"].as_i64().unwrap_or(0) as i32,
            tolerated_failure_percentage: state_def["ToleratedFailurePercentage"]
                .as_f64()
                .unwrap_or(0.0),
            tolerated_failure_count: state_def["ToleratedFailureCount"].as_i64().unwrap_or(0),
            status: "RUNNING".to_string(),
            start_date: Utc::now(),
            stop_date: None,
            total_count: 0,
            succeeded_count: 0,
            failed_count: 0,
        },
    );
}

/// Record what a MapRun learned once it read its items and resolved its paths.
fn set_map_run_total(
    shared_state: &SharedStepFunctionsState,
    map_run_arn: &str,
    execution_arn: &str,
    total: usize,
    max_concurrency: u64,
    tolerance: FailureTolerance,
) {
    let account = account_id_from_arn(execution_arn).to_string();
    let mut accounts = shared_state.write();
    let region = arn_region(execution_arn, accounts.region());
    let s = accounts.regional_mut(&account, &region);
    if let Some(mr) = s.map_runs.get_mut(map_run_arn) {
        mr.total_count = total as i64;
        // The configured MaxConcurrency (0 == unlimited), as AWS reports it, not
        // the internal effective concurrency.
        mr.max_concurrency = max_concurrency as i32;
        // What the run actually enforces, `*Path` forms resolved.
        mr.tolerated_failure_count = tolerance.count.unwrap_or(0) as i64;
        mr.tolerated_failure_percentage = tolerance.percentage.unwrap_or(0.0);
    }
}

/// Transition a MapRun to a terminal status, recording the final iteration
/// tallies when known. The first terminal status stands, and a record reaped
/// mid-run (a state reset) is left alone.
fn finish_map_run(
    shared_state: &SharedStepFunctionsState,
    map_run_arn: &str,
    execution_arn: &str,
    tallies: Option<(i64, i64)>,
    status: &str,
) {
    let account = account_id_from_arn(execution_arn).to_string();
    let mut accounts = shared_state.write();
    let region = arn_region(execution_arn, accounts.region());
    let s = accounts.regional_mut(&account, &region);
    if let Some(mr) = s.map_runs.get_mut(map_run_arn) {
        if mr.status != "RUNNING" {
            return;
        }
        mr.status = status.to_string();
        if let Some((succeeded, failed)) = tallies {
            mr.succeeded_count = succeeded;
            mr.failed_count = failed;
        }
        mr.stop_date = Some(Utc::now());
    }
}

/// The S3 service as the execution that is calling it.
fn s3_caller<'a>(
    registry: &'a Arc<fakecloud_core::registry::ServiceRegistry>,
    account_id: &'a str,
    region: &'a str,
) -> Result<crate::s3_integration::S3Caller<'a>, (String, String)> {
    crate::s3_integration::S3Caller::new(Caller {
        registry,
        account_id,
        region,
        principal: None,
    })
    .ok_or_else(|| {
        (
            "States.Runtime".to_string(),
            "S3 is not available".to_string(),
        )
    })
}

/// Read items from S3 for distributed Map mode ItemReader.
async fn read_items_from_s3(
    item_reader: &Value,
    jsonata: bool,
    effective_input: &Value,
    ctx: &Value,
    registry: &Option<SharedServiceRegistry>,
    execution_arn: &str,
) -> Result<Vec<Value>, (String, String)> {
    let resource = item_reader["Resource"]
        .as_str()
        .unwrap_or("arn:aws:states:::s3:getObject");
    let action = arn_resource(resource, "states")
        .and_then(|r| r.strip_prefix("::"))
        .filter(|action| crate::map_s3::ItemReader::supports(action))
        .ok_or_else(|| {
            (
                "States.Runtime".to_string(),
                format!("ItemReader unsupported resource: {resource}"),
            )
        })?;

    let params = match item_reader.get("Parameters") {
        Some(template) => resolve_template(template, jsonata, effective_input, ctx)?,
        None => json!({}),
    };
    let config = item_reader
        .get("ReaderConfig")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let max_items = match config["MaxItemsPath"].as_str() {
        Some(path) => {
            crate::io_processing::resolve_reference_with_context(effective_input, Some(ctx), path)?
                .as_u64()
        }
        None => config["MaxItems"].as_u64(),
    };

    let registry_arc = resolve_registry(registry)?;
    let account_id = account_from_execution_arn(execution_arn);
    let region = region_from_execution_arn(execution_arn);
    crate::map_s3::ItemReader {
        action: action.to_string(),
        params,
        config,
        max_items: max_items.filter(|max| *max > 0).map(|max| max as usize),
    }
    .read(&s3_caller(&registry_arc, &account_id, &region)?)
    .await
}

/// Deliver a map run's results as its ResultWriter says, returning the Map
/// state's result.
#[allow(clippy::too_many_arguments)]
async fn write_map_results(
    result_writer: &Value,
    jsonata: bool,
    config: crate::map_s3::WriterConfig<'_>,
    effective_input: &Value,
    ctx: &Value,
    registry: &Option<SharedServiceRegistry>,
    execution_arn: &str,
    map_run_arn: Option<&str>,
    records: &[crate::map_s3::IterationRecord],
) -> Result<Value, (String, String)> {
    let map_run_arn = map_run_arn.ok_or_else(|| {
        (
            "States.Runtime".to_string(),
            "ResultWriter requires a distributed Map".to_string(),
        )
    })?;
    let run = crate::map_s3::MapRunIdentity { map_run_arn };

    // No S3 destination: the configuration only shapes the state's output.
    let Some(resource) = result_writer["Resource"].as_str() else {
        return crate::map_s3::format_state_output(config, &run, records);
    };
    if !resource.contains("s3:putObject") {
        return Err((
            "States.Runtime".to_string(),
            format!("ResultWriter unsupported resource: {resource}"),
        ));
    }

    let params = match result_writer.get("Parameters") {
        Some(template) => resolve_template(template, jsonata, effective_input, ctx)?,
        None => json!({}),
    };
    let bucket = params["Bucket"].as_str().ok_or_else(|| {
        (
            "States.ResultWriterFailed".to_string(),
            "ResultWriter missing Bucket".to_string(),
        )
    })?;

    let registry_arc = resolve_registry(registry)?;
    let account_id = account_from_execution_arn(execution_arn);
    let region = region_from_execution_arn(execution_arn);
    crate::map_s3::write_results(
        &s3_caller(&registry_arc, &account_id, &region)?,
        bucket,
        params["Prefix"].as_str().unwrap_or(""),
        config,
        &run,
        records,
    )
    .await
}

/// A JSONPath payload template resolved against the state's input. A JSONata
/// state's was evaluated when the state was prepared, and is taken as it is: a
/// key its expressions produced that ends in `.$` is not a path.
fn resolve_template(
    template: &Value,
    jsonata: bool,
    effective_input: &Value,
    ctx: &Value,
) -> Result<Value, (String, String)> {
    if jsonata {
        Ok(template.clone())
    } else {
        apply_parameters(template, effective_input, Some(ctx))
    }
}

/// Apply ItemBatcher configuration to group items into batches.
fn apply_item_batcher(
    items: &[Value],
    batcher: &Value,
    jsonata: bool,
    effective_input: &Value,
    ctx: &Value,
) -> Result<Vec<Value>, (String, String)> {
    // A limit is a literal (a JSONata expression has been evaluated by now) or a
    // reference path, and must come out a positive integer.
    let limit = |field: &str| -> Result<Option<u64>, (String, String)> {
        let path_field = format!("{field}Path");
        let value = match batcher[path_field.as_str()].as_str() {
            Some(path) => Some(crate::io_processing::resolve_reference_with_context(
                effective_input,
                Some(ctx),
                path,
            )?),
            None => batcher.get(field).cloned(),
        };
        match value {
            None => Ok(None),
            Some(value) => match value.as_u64().filter(|n| *n > 0) {
                Some(n) => Ok(Some(n)),
                None => Err(crate::io_processing::runtime_error(format!(
                    "The ItemBatcher field '{field}' must be a positive integer, got {value}"
                ))),
            },
        }
    };
    let max_per_batch = limit("MaxItemsPerBatch")?.unwrap_or(u64::MAX) as usize;
    let max_bytes = limit("MaxInputBytesPerBatch")?.unwrap_or(0) as usize;
    let batch_input = batcher
        .get("BatchInput")
        .map(|template| resolve_template(template, jsonata, effective_input, ctx))
        .transpose()?;

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

    Ok(batches
        .into_iter()
        .map(|batch| {
            let mut map = serde_json::Map::new();
            if let Some(batch_input) = &batch_input {
                map.insert("BatchInput".to_string(), batch_input.clone());
            }
            map.insert("Items".to_string(), Value::Array(batch));
            Value::Object(map)
        })
        .collect())
}

/// The full function ARN a `lambda:invoke` task's `FunctionName` refers to.
/// A full ARN is used as is; a partial ARN (`ACCOUNT:function:NAME[:Q]`) or a
/// bare name (`NAME[:Q]`) names a function in the execution's region, and a
/// bare name also in the execution's account, the way Lambda resolves it for
/// the state machine's role.
fn qualify_lambda_function_ref(function_ref: &str, execution_arn: &str) -> String {
    if function_ref.starts_with("arn:") {
        return function_ref.to_string();
    }
    let (Some(region), Some(exec_account)) = (
        fakecloud_aws::arn::region_of(execution_arn),
        fakecloud_aws::arn::account_of(execution_arn),
    ) else {
        return function_ref.to_string();
    };
    let parts: Vec<&str> = function_ref.splitn(3, ':').collect();
    let (account, resource) = if parts.len() == 3
        && parts[1] == "function"
        && !parts[0].is_empty()
        && parts[0].chars().all(|c| c.is_ascii_digit())
    {
        (parts[0], format!("function:{}", parts[2]))
    } else {
        (exec_account, format!("function:{function_ref}"))
    };
    fakecloud_aws::arn::Arn::regional("lambda", region, account, &resource).to_string()
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

    // SDK integration patterns: arn:<partition>:states:::<service>:<action>
    let integration = arn_resource(resource, "states").and_then(|r| r.strip_prefix("::"));
    let is_integration = |prefix: &str| integration.is_some_and(|t| t.starts_with(prefix));

    if is_integration("lambda:invoke") {
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
        let function_arn = qualify_lambda_function_ref(function_name, execution_arn);
        return invoke_lambda_direct(&function_arn, &payload, delivery, timeout_seconds)
            .await
            .map(|payload| {
                let mut envelope = json!({
                    "ExecutedVersion": "$LATEST",
                    "Payload": payload,
                    "StatusCode": 200,
                });
                lambda_invoke_metadata(&mut envelope);
                envelope
            });
    }

    if is_integration("sqs:sendMessage") {
        let region = fakecloud_aws::arn::region_of(execution_arn).unwrap_or("us-east-1");
        return invoke_sqs_send_message(input, delivery, region).map(|mut result| {
            sqs_metadata(&mut result);
            result
        });
    }

    if is_integration("sns:publish") {
        return invoke_sns_publish(input, delivery).map(|mut result| {
            sns_metadata(&mut result);
            result
        });
    }

    if is_integration("events:putEvents") {
        return invoke_eventbridge_put_events(
            input,
            delivery,
            execution_arn,
            &execution_role_arn(shared_state, execution_arn),
        );
    }

    // Optimized DynamoDB integrations call DynamoDB itself, in the
    // execution's account and region, so a task's write goes through the
    // same path as any client's: validation, streams, Kinesis destinations,
    // global-table replication and persistence. The direct state access
    // below is only for an interpreter run without a service registry.
    for (task, action) in [
        ("dynamodb:getItem", "GetItem"),
        ("dynamodb:putItem", "PutItem"),
        ("dynamodb:deleteItem", "DeleteItem"),
        ("dynamodb:updateItem", "UpdateItem"),
    ] {
        if !is_integration(task) {
            continue;
        }
        if let Some(registry_arc) = resolve_registry(registry)
            .ok()
            .filter(|r| r.get("dynamodb").is_some())
        {
            let account = account_from_execution_arn(execution_arn);
            let region = fakecloud_aws::arn::region_of(execution_arn).unwrap_or("us-east-1");
            return call_sdk_action(&registry_arc, "dynamodb", action, input, &account, region)
                .await
                .map_err(optimized_dynamodb_error)
                .map(|mut result| {
                    dynamodb_metadata(&mut result);
                    result
                });
        }
        return match action {
            "GetItem" => invoke_dynamodb_get_item(input, dynamodb_state, execution_arn),
            "PutItem" => invoke_dynamodb_put_item(input, dynamodb_state, execution_arn),
            "DeleteItem" => invoke_dynamodb_delete_item(input, dynamodb_state, execution_arn),
            _ => invoke_dynamodb_update_item(input, dynamodb_state, execution_arn),
        }
        .map(|mut result| {
            dynamodb_metadata(&mut result);
            result
        });
    }

    // Nested Step Functions execution:
    //   arn:aws:states:::states:startExecution[.sync|.waitForTaskToken]
    // Routes through the same registry path as the generic aws-sdk
    // integration so the inner execution sees a real `StartExecution`
    // call. `.sync` polls `DescribeExecution` until the inner execution
    // reaches a terminal state, mirroring AWS' behavior of bubbling the
    // inner Output back to the parent.
    if let Some(tail) = integration {
        if tail.starts_with("states:startExecution") {
            let account_id = account_from_execution_arn(execution_arn);
            let region = &region_from_execution_arn(execution_arn);
            return invoke_aws_sdk_integration(
                tail,
                input,
                registry,
                &account_id,
                region,
                timeout_seconds,
                true,
                crate::sdk_protocol::execution_principal(
                    &execution_role_arn(shared_state, execution_arn),
                    execution_arn,
                ),
                Parent {
                    state: shared_state,
                    execution_arn,
                },
            )
            .await;
        }
    }

    // Generic AWS SDK integration: arn:aws:states:::aws-sdk:<service>:<action>[.<wait>]
    // Routes the call to the registered service via the central
    // ServiceRegistry, passing the Task's `Parameters` block as the
    // request body. Mirrors the AWS SDK service integration pattern in
    // real Step Functions.
    if let Some(rest) = integration.and_then(|t| t.strip_prefix("aws-sdk:")) {
        let account_id = account_from_execution_arn(execution_arn);
        let region = &region_from_execution_arn(execution_arn);
        return invoke_aws_sdk_integration(
            rest,
            input,
            registry,
            &account_id,
            region,
            timeout_seconds,
            false,
            crate::sdk_protocol::execution_principal(
                &execution_role_arn(shared_state, execution_arn),
                execution_arn,
            ),
            Parent {
                state: shared_state,
                execution_arn,
            },
        )
        .await;
    }

    // Optimized ECS, Athena and Glue integrations, in their request-response,
    // `.sync` and `.waitForTaskToken` forms, route through the same machinery
    // as `aws-sdk:` so callers can write the AWS-blessed ARN forms.
    if let Some(tail) = integration {
        let is_optimized = ["ecs:", "athena:", "glue:"]
            .iter()
            .any(|service| tail.starts_with(service));
        if is_optimized || tail.contains(".sync") || tail.contains(".waitForTaskToken") {
            let account_id = account_from_execution_arn(execution_arn);
            let region = &region_from_execution_arn(execution_arn);
            return invoke_aws_sdk_integration(
                tail,
                input,
                registry,
                &account_id,
                region,
                timeout_seconds,
                true,
                crate::sdk_protocol::execution_principal(
                    &execution_role_arn(shared_state, execution_arn),
                    execution_arn,
                ),
                Parent {
                    state: shared_state,
                    execution_arn,
                },
            )
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

/// The role recorded on an execution when it was started.
fn execution_role_arn(state: &SharedStepFunctionsState, execution_arn: &str) -> String {
    let accounts = state.read();
    accounts
        .by_arn(execution_arn)
        .and_then(|s| s.executions.get(execution_arn))
        .map(|exec| exec.role_arn.clone())
        .unwrap_or_default()
}

/// The region a Step Functions ARN names, or `fallback` (the server region)
/// for a malformed one. An execution's map runs, task tokens and child
/// executions live in its own region.
fn arn_region(arn: &str, fallback: &str) -> String {
    fakecloud_aws::arn::region_of(arn)
        .unwrap_or(fallback)
        .to_string()
}

/// The region of a Step Functions execution ARN: an execution's
/// `aws-sdk:` and optimized integrations call services in its own region.
/// Falls back to the AWS-conventional default for a malformed ARN.
fn region_from_execution_arn(execution_arn: &str) -> String {
    fakecloud_aws::arn::region_of(execution_arn)
        .unwrap_or("us-east-1")
        .to_string()
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
#[allow(clippy::too_many_arguments)]
async fn invoke_aws_sdk_integration(
    tail: &str,
    input: &Value,
    registry: &Option<SharedServiceRegistry>,
    account_id: &str,
    region: &str,
    timeout_seconds: Option<u64>,
    optimized: bool,
    principal: Option<fakecloud_core::auth::Principal>,
    parent: Parent<'_>,
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
    let sync_form = modifiers.iter().find_map(|m| match *m {
        "sync" => Some(SyncForm::Escaped),
        "sync:2" => Some(SyncForm::Json),
        _ => None,
    });

    let action_pascal = camel_to_pascal(action_camel);
    let unknown_service = || {
        (
            "States.TaskFailed".to_string(),
            format!("Unknown aws-sdk service '{service_id}'"),
        )
    };
    let service = crate::sdk_protocol::service(service_id).ok_or_else(unknown_service)?;
    let (_, service_name) = service
        .registered_in(&registry_arc)
        .ok_or_else(unknown_service)?;
    let operation = service.operation(&action_pascal).ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            format!("{service_id} has no action '{action_camel}'"),
        )
    })?;

    // A `.sync` Task nothing can wait for fails before it starts anything, rather
    // than reporting a job it never watched.
    if sync_form.is_some() && !has_sync_waiter(&service_name, &operation) {
        return Err((
            "States.TaskFailed".to_string(),
            format!(
                "`.sync` is not supported for {service_name}:{operation} yet; supported: \
                 ecs:RunTask, athena:StartQueryExecution, glue:StartJobRun, \
                 states:StartExecution, batch:SubmitJob, codebuild:StartBuild"
            ),
        ));
    }

    let caller = Caller {
        registry: &registry_arc,
        account_id,
        region,
        principal,
    };

    // AWS Batch is REST, so its waiter speaks the service's own protocol.
    if sync_form.is_some() && service.protocol() != "json" {
        return sync_wait_batch_job(&service, &caller, &operation, input, timeout_seconds).await;
    }

    // An `aws-sdk:*` Task, and any integration over a protocol other than JSON,
    // takes the service's own protocol end to end.
    if (!optimized && sync_form.is_none()) || service.protocol() != "json" {
        let result = service
            .invoke(&caller, &operation, input, DateStyle::Iso)
            .await
            .map_err(|failure| crate::sdk_protocol::task_error(&service, &failure));
        if let Ok(started) = &result {
            parent.link_started_child(&service_name, &operation, started);
        }
        return result;
    }

    // Optimized integrations: the waiters below read the camelCase wire shapes
    // of ECS and Step Functions, and results present dates in epoch millis.
    let mut wire_input = service.request_json(&operation, input);
    // As recorded, a task a `.sync` integration waits on says who started it.
    if sync_form.is_some() && service_name == "ecs" && wire_input.get("startedBy").is_none() {
        wire_input["startedBy"] = json!("AWS Step Functions");
    }
    let initial = call_sdk_action(
        &registry_arc,
        &service_name,
        &operation,
        &wire_input,
        account_id,
        region,
    )
    .await?;
    // Link a started child now, so `/execution-tree` shows it while a `.sync`
    // Task waits on it and after it fails.
    parent.link_started_child(&service_name, &operation, &initial);

    let Some(form) = sync_form else {
        return Ok(match (service_name.as_str(), operation.as_str()) {
            ("states", "StartExecution") => start_execution_result(&service, &initial),
            ("ecs", "RunTask") => ecs_run_task_result(&service, &initial),
            ("glue", "StartJobRun") => {
                glue_start_job_run_result(&service, &initial, &wire_input["JobName"])
            }
            _ => service.result_json(&operation, &initial, DateStyle::EpochMillis),
        });
    };

    // `.sync` pattern: dispatch to the per-service waiter that polls until the
    // downstream operation reaches a terminal state. Returns the full
    // describe-shape result, or surfaces a terminal failure as
    // `States.TaskFailed`.
    let described = sync_wait(
        &registry_arc,
        &service_name,
        &operation,
        &initial,
        &wire_input,
        account_id,
        region,
        timeout_seconds,
    )
    .await?;
    Ok(match (service_name.as_str(), operation.as_str()) {
        ("states", _) => sync_execution_result(&service, &described, form),
        ("ecs", _) => ecs_sync_result(&service, &described),
        ("athena", _) => {
            service.result_json("GetQueryExecution", &described, DateStyle::EpochMillis)
        }
        ("glue", _) => glue_sync_result(&service, &described),
        ("codebuild", _) => service.result_json("StartBuild", &described, DateStyle::EpochMillis),
        _ => described,
    })
}

/// The execution a Task runs in, so an execution the Task starts records it as
/// its parent for `/_fakecloud/stepfunctions/execution-tree`.
#[derive(Clone, Copy)]
struct Parent<'a> {
    state: &'a SharedStepFunctionsState,
    execution_arn: &'a str,
}

impl Parent<'_> {
    /// Record this execution as the parent of the one a `states:StartExecution`
    /// call started, read from the call's response (`executionArn` on the wire,
    /// `ExecutionArn` in an `aws-sdk:sfn` result).
    fn link_started_child(&self, service_name: &str, operation: &str, response: &Value) {
        if service_name != "states" || operation != "StartExecution" {
            return;
        }
        let Some(child_arn) = response
            .get("executionArn")
            .or_else(|| response.get("ExecutionArn"))
            .and_then(Value::as_str)
        else {
            return;
        };
        let mut accounts = self.state.write();
        if let Some(child) = accounts
            .by_arn_mut(child_arn)
            .and_then(|state| state.executions.get_mut(child_arn))
        {
            child.parent_execution_arn = Some(self.execution_arn.to_string());
        }
    }
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

/// The error name an optimized DynamoDB task fails with: `DynamoDB.<code>`,
/// with request validation failures reported as
/// `DynamoDB.AmazonDynamoDBException`, as Step Functions names them.
fn optimized_dynamodb_error((error, cause): (String, String)) -> (String, String) {
    match error.strip_prefix("DynamoDb.") {
        Some("ValidationException") => ("DynamoDB.AmazonDynamoDBException".to_string(), cause),
        Some(code) => (format!("DynamoDB.{code}"), cause),
        None => (error, cause),
    }
}

/// Call a single AWS SDK action against the registered service handler, as
/// the execution's account in the execution's region.
async fn call_sdk_action(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    service_name: &str,
    action_pascal: &str,
    input: &Value,
    account_id: &str,
    region: &str,
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
        region: region.to_string(),
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

    // `<Service>.<Code>`, with the message and details the SDK behind an optimized
    // integration reports, as recorded for Glue and ECS.
    let response = service.handle(req).await.map_err(|err| {
        let code = err.code().to_string();
        let msg = err.message();
        let (prefix_service, sdk_service) = match service_name {
            "dynamodb" => ("DynamoDb".to_string(), "AmazonDynamoDBv2".to_string()),
            "states" => ("Sfn".to_string(), "AWSStepFunctions".to_string()),
            "ecs" => ("ECS".to_string(), "AmazonECS".to_string()),
            "glue" => ("Glue".to_string(), "AWSGlue".to_string()),
            "athena" => ("Athena".to_string(), "AmazonAthena".to_string()),
            other => (camel_to_pascal(other), camel_to_pascal(other)),
        };
        let status = err.status().as_u16();
        let request_id = uuid::Uuid::new_v4();
        (
            format!("{prefix_service}.{code}"),
            format!(
                "{msg} (Service: {sdk_service}; Status Code: {status}; Error Code: {code}; \
                 Request ID: {request_id}; Proxy: null)"
            ),
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

/// Cap on `.sync` polling so a stuck downstream task can't hang an
/// execution forever. Mirrors the `TimeoutSeconds` knob on the Task state
/// when one is set; otherwise defaults to 5 minutes which is enough for
/// the in-process services that ship today (Athena queries finish within
/// milliseconds, ECS tasks within seconds even when docker-less).
const SYNC_DEFAULT_TIMEOUT_SECS: u64 = 300;
const SYNC_POLL_INTERVAL_MS: u64 = 200;

fn has_sync_waiter(service_name: &str, operation: &str) -> bool {
    matches!(
        (service_name, operation),
        ("ecs", "RunTask")
            | ("athena", "StartQueryExecution")
            | ("states", "StartExecution")
            | ("glue", "StartJobRun")
            | ("batch", "SubmitJob")
            | ("codebuild", "StartBuild")
    )
}

/// `batch:submitJob.sync`: submit, then poll `DescribeJobs` until the job ends.
async fn sync_wait_batch_job(
    service: &crate::sdk_protocol::SdkService,
    caller: &Caller<'_>,
    operation: &str,
    input: &Value,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    let failed = |failure: crate::sdk_protocol::SdkFailure| {
        crate::sdk_protocol::task_error(service, &failure)
    };
    let submitted = service
        .invoke(caller, operation, input, DateStyle::EpochMillis)
        .await
        .map_err(failed)?;
    let job_id = submitted["JobId"].as_str().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "batch:submitJob.sync: response missing JobId".to_string(),
        )
    })?;
    let deadline = sync_deadline(timeout_seconds);
    loop {
        let described = service
            .invoke(
                caller,
                "DescribeJobs",
                &json!({ "Jobs": [job_id] }),
                DateStyle::EpochMillis,
            )
            .await
            .map_err(failed)?;
        let job = &described["Jobs"][0];
        match job["Status"].as_str() {
            Some("SUCCEEDED") => return Ok(job.clone()),
            Some("FAILED") => return Err(("States.TaskFailed".to_string(), job.to_string())),
            _ => {}
        }
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return Err((
                "States.Timeout".to_string(),
                format!(
                    "batch:submitJob.sync timed out after {}s for job {job_id}",
                    sync_timeout_secs(timeout_seconds)
                ),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(SYNC_POLL_INTERVAL_MS)).await;
    }
}

/// `codebuild:startBuild.sync`: poll `BatchGetBuilds` until the build ends. The
/// result is `{ build }`, the shape `StartBuild` returns.
async fn sync_wait_codebuild_build(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    initial: &Value,
    account_id: &str,
    region: &str,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    let build_id = initial["build"]["id"].as_str().ok_or_else(|| {
        (
            "States.TaskFailed".to_string(),
            "codebuild:startBuild.sync: response missing build.id".to_string(),
        )
    })?;
    let deadline = sync_deadline(timeout_seconds);
    loop {
        let described = call_sdk_action(
            registry,
            "codebuild",
            "BatchGetBuilds",
            &json!({ "ids": [build_id] }),
            account_id,
            region,
        )
        .await?;
        let build = &described["builds"][0];
        match build["buildStatus"].as_str() {
            Some("SUCCEEDED") => return Ok(json!({ "build": build })),
            Some("FAILED" | "FAULT" | "STOPPED" | "TIMED_OUT") => {
                return Err(("States.TaskFailed".to_string(), build.to_string()))
            }
            _ => {}
        }
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            return Err((
                "States.Timeout".to_string(),
                format!(
                    "codebuild:startBuild.sync timed out after {}s for build {build_id}",
                    sync_timeout_secs(timeout_seconds)
                ),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(SYNC_POLL_INTERVAL_MS)).await;
    }
}

/// Dispatch `.sync` waiters by service+action. Each waiter polls the
/// matching describe-style API until the downstream operation reaches a
/// terminal state, then returns the full describe response.
#[allow(clippy::too_many_arguments)]
async fn sync_wait(
    registry: &Arc<fakecloud_core::registry::ServiceRegistry>,
    service_name: &str,
    action_pascal: &str,
    initial: &Value,
    input: &Value,
    account_id: &str,
    region: &str,
    timeout_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    match (service_name, action_pascal) {
        ("ecs", "RunTask") => {
            sync_wait_ecs_run_task(registry, initial, input, account_id, region, timeout_seconds).await
        }
        ("athena", "StartQueryExecution") => {
            sync_wait_athena_query(registry, initial, account_id, region, timeout_seconds).await
        }
        ("states", "StartExecution") => {
            sync_wait_states_start_execution(registry, initial, account_id, region, timeout_seconds).await
        }
        ("codebuild", "StartBuild") => {
            sync_wait_codebuild_build(registry, initial, account_id, region, timeout_seconds).await
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
                    account_id, region,
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
                if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
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
    region: &str,
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
            region,
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
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
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
    region: &str,
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
            region,
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
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
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
    region: &str,
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
            region,
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
                let cause = crate::sdk_protocol::service("states")
                    .map(|service| sync_failure_cause(&service, &described))
                    .unwrap_or_else(|| described.to_string());
                return Err(("States.TaskFailed".to_string(), cause));
            }
            _ => {}
        }
        if deadline.is_some_and(|d| std::time::Instant::now() >= d) {
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

fn sync_deadline(timeout_seconds: Option<u64>) -> Option<std::time::Instant> {
    deadline_after_secs(sync_timeout_secs(timeout_seconds))
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
            .by_arn(activity_arn)
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
        let region = arn_region(activity_arn, accounts.region());
        let state = accounts.regional_mut(&activity_account, &region);
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
                heartbeat_seconds: heartbeat_seconds.map(|s| i64::try_from(s).unwrap_or(i64::MAX)),
                timeout_seconds: timeout_seconds.map(|s| s as i64),
            },
        );
    }

    let activity_region = arn_region(activity_arn, shared_state.read().region());
    poll_task_token(
        shared_state,
        &activity_account,
        &activity_region,
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

/// JSON type name as Step Functions reports it in Map input errors.
fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
#[path = "interpreter_tests.rs"]
mod tests;
