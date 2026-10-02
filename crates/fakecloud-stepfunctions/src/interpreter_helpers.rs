use super::*;

/// Core execution loop: runs through states in a definition and returns the output.
/// Used by the top-level executor, Parallel branches, and Map iterations.
pub(crate) fn run_states<'a>(
    def: &'a Value,
    input: Value,
    delivery: &'a Option<Arc<DeliveryBus>>,
    dynamodb_state: &'a Option<SharedDynamoDbState>,
    registry: &'a Option<crate::service::SharedServiceRegistry>,
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
        // AWS Standard workflows allow up to 25,000 state transitions per
        // execution before failing with States.Runtime.
        let max_iterations = 25_000;

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
                        registry,
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
                        registry,
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
                        registry,
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

/// Build the Step Functions context object (`$$`) for a state of
/// `execution_arn` entered at `entered`: `Execution` (Id, Input, Name,
/// RoleArn, StartTime, RedriveCount), `State` (EnteredTime, Name, RetryCount)
/// and `StateMachine` (Id, Name). Callers add `Task` (task-token
/// integrations) and `Map.Item` (ItemSelector) where they apply.
pub(crate) fn context_object(
    shared_state: &SharedStepFunctionsState,
    execution_arn: &str,
    state_name: &str,
    entered: chrono::DateTime<chrono::Utc>,
    retry_count: u32,
) -> Value {
    let fmt = |t: chrono::DateTime<chrono::Utc>| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let accounts = shared_state.read();
    let exec = accounts
        .get(account_id_from_arn(execution_arn))
        .and_then(|s| s.executions.get(execution_arn));
    let (input, name, role_arn, start, sm_arn, sm_name, redrive_count) = match exec {
        Some(e) => (
            e.input
                .as_deref()
                .and_then(|i| serde_json::from_str::<Value>(i).ok())
                .unwrap_or_else(|| json!({})),
            e.name.clone(),
            e.role_arn.clone(),
            fmt(e.start_date),
            e.state_machine_arn.clone(),
            e.state_machine_name.clone(),
            e.redrive_count,
        ),
        None => (
            json!({}),
            execution_arn
                .rsplit(':')
                .next()
                .unwrap_or_default()
                .to_string(),
            String::new(),
            fmt(entered),
            String::new(),
            String::new(),
            0,
        ),
    };
    json!({
        "Execution": {
            "Id": execution_arn,
            "Input": input,
            "Name": name,
            "RoleArn": role_arn,
            "StartTime": start,
            "RedriveCount": redrive_count,
        },
        "State": {
            "EnteredTime": fmt(entered),
            "Name": state_name,
            "RetryCount": retry_count,
        },
        "StateMachine": {
            "Id": sm_arn,
            "Name": sm_name,
        },
    })
}

pub(crate) fn advance_from_next(state_def: &Value, input: Value) -> Advance {
    match next_state(state_def) {
        NextState::Name(next) => Advance::Next(next, input),
        NextState::End => Advance::End(input),
        NextState::Error(msg) => Advance::Fail("States.Runtime".to_string(), msg),
    }
}

pub(crate) fn advance_from_error(
    state_def: &Value,
    input: &Value,
    error: String,
    cause: String,
    is_task_error: bool,
) -> Advance {
    match apply_state_catcher(state_def, input, &error, &cause, is_task_error) {
        Some((next, new_input)) => Advance::Next(next, new_input),
        None => Advance::Fail(error, cause),
    }
}

pub(crate) fn run_pass_state(
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
            "input": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
        }),
    );

    let ctx = context_object(shared_state, execution_arn, name, chrono::Utc::now(), 0);
    let result = match execute_pass_state(state_def, &input, Some(&ctx)) {
        Ok(r) => r,
        Err((error, cause)) => return Advance::Fail(error, cause),
    };

    add_event(
        shared_state,
        execution_arn,
        "PassStateExited",
        entered_event_id,
        json!({
            "name": name,
            "output": serde_json::to_string(&result).expect("serde_json::Value serialization is infallible"),
        }),
    );

    advance_from_next(state_def, result)
}

pub(crate) fn run_succeed_state(
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
            "input": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
        }),
    );

    let input_path = state_def["InputPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    let processed = if input_path == Some("null") {
        json!({})
    } else {
        match apply_input_path(&input, input_path) {
            Ok(v) => v,
            Err((error, cause)) => return Advance::Fail(error, cause),
        }
    };

    let output = if output_path == Some("null") {
        json!({})
    } else {
        match apply_output_path(&processed, output_path) {
            Ok(v) => v,
            Err((error, cause)) => return Advance::Fail(error, cause),
        }
    };

    add_event(
        shared_state,
        execution_arn,
        "SucceedStateExited",
        0,
        json!({
            "name": name,
            "output": serde_json::to_string(&output).expect("serde_json::Value serialization is infallible"),
        }),
    );

    Advance::End(output)
}

pub(crate) fn run_fail_state(
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
            "input": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
        }),
    );

    Advance::Fail(error, cause)
}

pub(crate) fn run_choice_state(
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
            "input": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
        }),
    );

    let input_path = state_def["InputPath"].as_str();
    let processed_input = if input_path == Some("null") {
        json!({})
    } else {
        match apply_input_path(&input, input_path) {
            Ok(v) => v,
            Err((error, cause)) => return Advance::Fail(error, cause),
        }
    };

    let ctx = context_object(shared_state, execution_arn, name, chrono::Utc::now(), 0);
    let chosen = match evaluate_choice(state_def, &processed_input, Some(&ctx)) {
        Ok(chosen) => chosen,
        Err((error, cause)) => return Advance::Fail(error, cause),
    };
    match chosen {
        Some(next) => {
            // A Choice state has no Parameters/ResultPath, so its effective
            // result is the InputPath-filtered input; OutputPath then filters
            // what flows to the next state. Previously the raw input was
            // forwarded, silently ignoring OutputPath.
            let output_path = state_def["OutputPath"].as_str();
            let output = if output_path == Some("null") {
                json!({})
            } else {
                match apply_output_path(&processed_input, output_path) {
                    Ok(v) => v,
                    Err((error, cause)) => return Advance::Fail(error, cause),
                }
            };
            add_event(
                shared_state,
                execution_arn,
                "ChoiceStateExited",
                entered_event_id,
                json!({
                    "name": name,
                    "output": serde_json::to_string(&output).expect("serde_json::Value serialization is infallible"),
                }),
            );
            Advance::Next(next, output)
        }
        None => Advance::Fail(
            "States.NoChoiceMatched".to_string(),
            format!("No choice rule matched and no Default in state '{name}'"),
        ),
    }
}

/// Execute a Pass state: apply InputPath, use Result if present, apply ResultPath and OutputPath.
pub(crate) fn execute_pass_state(
    state_def: &Value,
    input: &Value,
    context: Option<&Value>,
) -> Result<Value, (String, String)> {
    let input_path = state_def["InputPath"].as_str();
    let result_path = state_def["ResultPath"].as_str();
    let output_path = state_def["OutputPath"].as_str();

    let effective_input = if input_path == Some("null") {
        json!({})
    } else {
        apply_input_path(input, input_path)?
    };

    // A Pass state may carry a Parameters template that builds a new payload
    // from the effective input (and intrinsics). It transforms the effective
    // input before Result/ResultPath; previously it was ignored entirely.
    let transformed = if let Some(params) = state_def.get("Parameters") {
        apply_parameters(params, &effective_input, context)?
    } else {
        effective_input
    };

    let result = if let Some(r) = state_def.get("Result") {
        r.clone()
    } else {
        transformed
    };

    let after_result = if result_path == Some("null") {
        input.clone()
    } else {
        apply_result_path(input, &result, result_path)
    };

    if output_path == Some("null") {
        Ok(json!({}))
    } else {
        apply_output_path(&after_result, output_path)
    }
}

/// Send a message to an SQS queue via DeliveryBus.
pub(crate) fn invoke_sqs_send_message(
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    region: &str,
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
            serde_json::to_string(&input["MessageBody"])
                .expect("serde_json::Value serialization is infallible")
        });

    // A QueueUrl (`<endpoint>/<account>/<name>`) carries no region: like the
    // SQS client a state machine's role would use, it addresses the queue of
    // that account and name in the execution's region, resolved to its
    // stored ARN.
    let queue_arn = delivery
        .sqs_queue_arn_for_url(region, queue_url)
        .ok_or_else(|| {
            (
                "SQS.QueueDoesNotExistException".to_string(),
                format!("The specified queue does not exist: {queue_url}"),
            )
        })?;

    delivery.send_to_sqs(&queue_arn, &message_body, &HashMap::new());

    Ok(json!({
        "MessageId": uuid::Uuid::new_v4().to_string(),
        "MD5OfMessageBody": md5_hex(&message_body),
    }))
}

/// Publish a message to an SNS topic via DeliveryBus.
pub(crate) fn invoke_sns_publish(
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
        .unwrap_or_else(|| {
            serde_json::to_string(&input["Message"])
                .expect("serde_json::Value serialization is infallible")
        });

    let subject = input["Subject"].as_str();

    delivery.publish_to_sns(topic_arn, &message, subject);

    Ok(json!({
        "MessageId": uuid::Uuid::new_v4().to_string(),
    }))
}

/// Put events onto an EventBridge bus via DeliveryBus. The events originate
/// in the execution's account and region (both read from `execution_arn`);
/// an `EventBusName` ARN routes to that bus's account, which must allow the
/// execution role (`role_arn`) through its resource policy. As on AWS, a
/// response with `FailedEntryCount > 0` fails the task with
/// `EventBridge.FailedEntry`, the PutEvents response as the cause.
pub(crate) fn invoke_eventbridge_put_events(
    input: &Value,
    delivery: &Option<Arc<DeliveryBus>>,
    execution_arn: &str,
    role_arn: &str,
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

    let mut result_entries = Vec::new();
    let mut failed_count = 0;
    for entry in &entries {
        let source = entry["Source"].as_str().unwrap_or("aws.stepfunctions");
        let detail_type = entry["DetailType"].as_str().unwrap_or("StepFunctionsEvent");
        let detail = entry["Detail"]
            .as_str()
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                serde_json::to_string(&entry["Detail"])
                    .expect("serde_json::Value serialization is infallible")
            });
        let bus_name = entry["EventBusName"].as_str().unwrap_or("default");
        let resources: Vec<String> = entry["Resources"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        let put = delivery.put_event_to_eventbridge(&fakecloud_core::delivery::CrossServiceEvent {
            source,
            detail_type,
            detail: &detail,
            event_bus: bus_name,
            account_id: account_id_from_arn(execution_arn),
            region: region_from_arn(execution_arn),
            resources: &resources,
            principal_arn: (!role_arn.is_empty()).then_some(role_arn),
        });
        match put {
            Ok(id) => result_entries.push(json!({
                "EventId": id.unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            })),
            Err(err) => {
                use fakecloud_core::delivery::EventBridgeDeliveryError as E;
                let (code, message) = match err {
                    E::AccessDenied(message) => ("AccessDeniedException", message),
                    E::Unavailable(message) => ("InternalException", message),
                };
                failed_count += 1;
                result_entries.push(json!({
                    "ErrorCode": code,
                    "ErrorMessage": message,
                }));
            }
        }
    }

    let response = json!({
        "Entries": result_entries,
        "FailedEntryCount": failed_count,
    });
    if failed_count > 0 {
        return Err(("EventBridge.FailedEntry".to_string(), response.to_string()));
    }
    Ok(response)
}

/// Get an item from DynamoDB via direct state access.
pub(crate) fn invoke_dynamodb_get_item(
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
        .map(|idx| table.items()[idx].clone());

    match item {
        Some(item_map) => {
            let item_value: serde_json::Map<String, Value> = item_map.into_iter().collect();
            Ok(json!({ "Item": item_value }))
        }
        None => Ok(json!({})),
    }
}

/// Put an item into DynamoDB via direct state access.
pub(crate) fn invoke_dynamodb_put_item(
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

    // Replace existing item with same key, or insert new. Goes through the
    // table helper so `items`, the key index and the cached item_count /
    // size_bytes stay in step -- a bare `items.push` here left the index
    // pointing at the wrong rows for every later write (#2502).
    table.put_item_at_key(item_map);

    Ok(json!({}))
}

/// Delete an item from DynamoDB via direct state access.
pub(crate) fn invoke_dynamodb_delete_item(
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

    table.remove_item_by_key(&key_map);

    Ok(json!({}))
}

/// Update an item in DynamoDB via direct state access. Honors UpdateExpression
/// SET (with `=`, `+`, `-`, `if_not_exists`), REMOVE, ADD (numeric), and
/// DELETE (set elements). Creates the item from `Key` plus the expression
/// when no matching item exists, mirroring DynamoDB upsert semantics.
pub(crate) fn invoke_dynamodb_update_item(
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

        // DynamoDB rejects an update that writes a key attribute; the task
        // fails with the error Step Functions maps it to.
        let names: HashMap<String, String> = attr_names
            .iter()
            .filter_map(|(k, v)| Some((k.clone(), v.as_str()?.to_string())))
            .collect();
        if let Some(attr) = table.key_attribute_in_update_expression(update_expr, &names) {
            return Err((
                "DynamoDB.AmazonDynamoDBException".to_string(),
                fakecloud_dynamodb::DynamoTable::key_attribute_update_message(&attr),
            ));
        }

        table.ensure_key_index();
        if let Some(idx) = table.find_item_index(&key_map) {
            // Settles the cached size and keeps the key index in step.
            table.mutate_item_at(idx, |item| {
                apply_update_expression(item, update_expr, &attr_values, &attr_names);
            });
        } else {
            // Create new item with key + update expression values
            let mut new_item = key_map;
            apply_update_expression(&mut new_item, update_expr, &attr_values, &attr_names);
            table.put_item_at_key(new_item);
        }
    }

    Ok(json!({}))
}

/// Apply a simple SET UpdateExpression to an item.
pub(crate) fn apply_update_expression(
    item: &mut HashMap<String, Value>,
    expr: &str,
    attr_values: &serde_json::Map<String, Value>,
    attr_names: &serde_json::Map<String, Value>,
) {
    // DynamoDB UpdateExpression has up to four clauses: SET, REMOVE, ADD, DELETE.
    // The clauses are separated by whitespace; we tokenize by walking the string
    // and switching mode whenever we hit a keyword, then split the body of each
    // clause on commas.
    let clauses = split_update_clauses(expr);
    for (clause, body) in clauses {
        match clause {
            UpdateClause::Set => apply_set(item, &body, attr_values, attr_names),
            UpdateClause::Remove => apply_remove(item, &body, attr_names),
            UpdateClause::Add => apply_add(item, &body, attr_values, attr_names),
            UpdateClause::Delete => apply_delete(item, &body, attr_values, attr_names),
        }
    }
}

pub(crate) fn split_update_clauses(expr: &str) -> Vec<(UpdateClause, String)> {
    let mut out = Vec::new();
    let mut current: Option<UpdateClause> = None;
    let mut buf = String::new();
    for token in expr.split_whitespace() {
        let upper = token.to_ascii_uppercase();
        let next_clause = match upper.as_str() {
            "SET" => Some(UpdateClause::Set),
            "REMOVE" => Some(UpdateClause::Remove),
            "ADD" => Some(UpdateClause::Add),
            "DELETE" => Some(UpdateClause::Delete),
            _ => None,
        };
        if let Some(nc) = next_clause {
            if let Some(prev) = current.take() {
                out.push((prev, buf.trim().to_string()));
                buf.clear();
            }
            current = Some(nc);
        } else if current.is_some() {
            if !buf.is_empty() {
                buf.push(' ');
            }
            buf.push_str(token);
        }
    }
    if let Some(c) = current {
        out.push((c, buf.trim().to_string()));
    }
    out
}

pub(crate) fn resolve_attr_name(
    token: &str,
    attr_names: &serde_json::Map<String, Value>,
) -> String {
    if token.starts_with('#') {
        attr_names
            .get(token)
            .and_then(|v| v.as_str())
            .unwrap_or(token)
            .to_string()
    } else {
        token.to_string()
    }
}

pub(crate) fn apply_set(
    item: &mut HashMap<String, Value>,
    body: &str,
    attr_values: &serde_json::Map<String, Value>,
    attr_names: &serde_json::Map<String, Value>,
) {
    for assignment in split_top_commas(body) {
        let Some((lhs, rhs)) = assignment.split_once('=') else {
            continue;
        };
        let attr_name = resolve_attr_name(lhs.trim(), attr_names);
        let value = evaluate_set_rhs(rhs.trim(), &attr_name, item, attr_values, attr_names);
        if let Some(v) = value {
            item.insert(attr_name, v);
        }
    }
}

pub(crate) fn evaluate_set_rhs(
    rhs: &str,
    attr_name: &str,
    item: &HashMap<String, Value>,
    attr_values: &serde_json::Map<String, Value>,
    attr_names: &serde_json::Map<String, Value>,
) -> Option<Value> {
    // if_not_exists(path, :val)
    if let Some(args) = rhs
        .strip_prefix("if_not_exists(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let parts: Vec<&str> = args.splitn(2, ',').collect();
        if parts.len() == 2 {
            let path = resolve_attr_name(parts[0].trim(), attr_names);
            if item.contains_key(&path) {
                return item.get(&path).cloned();
            }
            return resolve_value(parts[1].trim(), attr_values);
        }
        return None;
    }
    // path + :inc / path - :dec — DynamoDB stores numbers as {"N":"<str>"}.
    for op in ['+', '-'] {
        if let Some((left, right)) = split_top_op(rhs, op) {
            let left = left.trim();
            let right = right.trim();
            let left_val = if left.starts_with(':') {
                resolve_value(left, attr_values)
            } else {
                let name = resolve_attr_name(left, attr_names);
                item.get(&name).cloned()
            };
            let right_val = if right.starts_with(':') {
                resolve_value(right, attr_values)
            } else {
                let name = resolve_attr_name(right, attr_names);
                item.get(&name).cloned()
            };
            return arithmetic(left_val.as_ref(), op, right_val.as_ref());
        }
    }
    // bare value or attribute reference
    if rhs.starts_with(':') {
        return resolve_value(rhs, attr_values);
    }
    if rhs.starts_with('#') {
        let _ = attr_name;
        let name = resolve_attr_name(rhs, attr_names);
        return item.get(&name).cloned();
    }
    None
}

pub(crate) fn arithmetic(left: Option<&Value>, op: char, right: Option<&Value>) -> Option<Value> {
    let lf = number_from_dynamo(left?)?;
    let rf = number_from_dynamo(right?)?;
    let out = match op {
        '+' => lf + rf,
        '-' => lf - rf,
        _ => return None,
    };
    Some(json!({ "N": format_number(out) }))
}

pub(crate) fn number_from_dynamo(v: &Value) -> Option<f64> {
    v.get("N")?.as_str()?.parse().ok()
}

pub(crate) fn format_number(n: f64) -> String {
    // i64::MAX is 2^63-1 which is not exactly representable in f64; `i64::MAX as f64`
    // rounds up to 2^63, and casting 2^63 back to i64 saturates. Use an exclusive upper
    // bound so we never hand `n as i64` a value it can't faithfully represent.
    if n.fract() == 0.0 && n.is_finite() && n >= i64::MIN as f64 && n < i64::MAX as f64 {
        format!("{}", n as i64)
    } else {
        format!("{n}")
    }
}

pub(crate) fn resolve_value(
    token: &str,
    attr_values: &serde_json::Map<String, Value>,
) -> Option<Value> {
    attr_values.get(token).cloned()
}

pub(crate) fn apply_remove(
    item: &mut HashMap<String, Value>,
    body: &str,
    attr_names: &serde_json::Map<String, Value>,
) {
    for path in split_top_commas(body) {
        let name = resolve_attr_name(path.trim(), attr_names);
        item.remove(&name);
    }
}

pub(crate) fn apply_add(
    item: &mut HashMap<String, Value>,
    body: &str,
    attr_values: &serde_json::Map<String, Value>,
    attr_names: &serde_json::Map<String, Value>,
) {
    // ADD #path :inc — numeric increment (value initialized to :inc when the
    // attribute is absent) OR set union for the set types (NS/SS/BS).
    // bug-audit 2026-05-28, 1.16: set union used to be unimplemented (no-op).
    for clause in split_top_commas(body) {
        let mut parts = clause.split_whitespace();
        let Some(path) = parts.next() else { continue };
        let Some(value_ref) = parts.next() else {
            continue;
        };
        let attr_name = resolve_attr_name(path, attr_names);
        let Some(delta) = resolve_value(value_ref, attr_values) else {
            continue;
        };
        let current = item.get(&attr_name).cloned();
        let next = match (current.as_ref(), &delta) {
            (None, _) => delta.clone(),
            (Some(cur), _) => {
                if let Some(unioned) = add_to_set(cur, &delta) {
                    unioned
                } else {
                    arithmetic(Some(cur), '+', Some(&delta)).unwrap_or(delta.clone())
                }
            }
        };
        item.insert(attr_name, next);
    }
}

/// DynamoDB `ADD` set-union semantics: when both the current attribute and the
/// delta are the same set type (`SS`/`NS`/`BS`), return the order-preserving,
/// deduplicated union. Returns `None` when either side isn't a matching set, so
/// the caller can fall back to numeric arithmetic.
fn add_to_set(current: &Value, delta: &Value) -> Option<Value> {
    for set_type in ["SS", "NS", "BS"] {
        let (Some(cur_arr), Some(add_arr)) = (
            current.get(set_type).and_then(|v| v.as_array()),
            delta.get(set_type).and_then(|v| v.as_array()),
        ) else {
            continue;
        };
        let mut elems = cur_arr.clone();
        for e in add_arr {
            if !elems.contains(e) {
                elems.push(e.clone());
            }
        }
        return Some(serde_json::json!({ set_type: elems }));
    }
    None
}

pub(crate) fn apply_delete(
    item: &mut HashMap<String, Value>,
    body: &str,
    attr_values: &serde_json::Map<String, Value>,
    attr_names: &serde_json::Map<String, Value>,
) {
    // DELETE #path :elements — remove each element of the set value from the
    // attribute's set. Drops the attribute when the resulting set is empty.
    for clause in split_top_commas(body) {
        let mut parts = clause.split_whitespace();
        let Some(path) = parts.next() else { continue };
        let Some(value_ref) = parts.next() else {
            continue;
        };
        let attr_name = resolve_attr_name(path, attr_names);
        let Some(elements) = resolve_value(value_ref, attr_values) else {
            continue;
        };
        let Some(current) = item.get_mut(&attr_name) else {
            continue;
        };
        for set_kind in ["SS", "NS", "BS"] {
            if let (Some(cur_arr), Some(rem_arr)) = (
                current.get_mut(set_kind).and_then(|v| v.as_array_mut()),
                elements.get(set_kind).and_then(|v| v.as_array()),
            ) {
                let to_remove: std::collections::HashSet<String> = rem_arr
                    .iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect();
                cur_arr.retain(|v| !v.as_str().is_some_and(|s| to_remove.contains(s)));
                if cur_arr.is_empty() {
                    item.remove(&attr_name);
                }
                break;
            }
        }
    }
}

pub(crate) fn split_top_commas(s: &str) -> Vec<String> {
    // Splits on `,` while respecting paren depth (so commas inside
    // `if_not_exists(a, :b)` don't split the assignment).
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut buf = String::new();
    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                buf.push(c);
            }
            ')' => {
                depth -= 1;
                buf.push(c);
            }
            ',' if depth == 0 => {
                out.push(std::mem::take(&mut buf).trim().to_string());
            }
            _ => buf.push(c),
        }
    }
    if !buf.trim().is_empty() {
        out.push(buf.trim().to_string());
    }
    out
}

pub(crate) fn split_top_op(s: &str, op: char) -> Option<(&str, &str)> {
    let mut depth = 0i32;
    for (i, c) in s.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            c if c == op && depth == 0 && i > 0 => {
                return Some((&s[..i], &s[i + c.len_utf8()..]));
            }
            _ => {}
        }
    }
    None
}

/// Compute MD5 hex digest for SQS message response format.
pub(crate) fn md5_hex(data: &str) -> String {
    use md5::Digest;
    let result = md5::Md5::digest(data.as_bytes());
    format!("{result:032x}")
}

pub(crate) fn cleanup_token(
    shared_state: &SharedStepFunctionsState,
    account_id: &str,
    token: &str,
) {
    let mut accounts = shared_state.write();
    if let Some(state) = accounts.get_mut(account_id) {
        state.task_tokens.remove(token);
    }
}

/// `now + secs` as a monotonic deadline, or `None` when the sum is not
/// representable (a timeout that far out never fires in practice). Avoids the
/// `Instant + Duration` overflow panic on absurd `TimeoutSeconds` values that
/// may already be persisted in a state machine definition.
pub(crate) fn deadline_after_secs(secs: u64) -> Option<std::time::Instant> {
    std::time::Instant::now().checked_add(std::time::Duration::from_secs(secs))
}

/// Poll a task token until the worker calls `SendTaskSuccess`,
/// `SendTaskFailure`, or the heartbeat / timeout windows expire.
/// Mirrors the polling loop used by `invoke_activity` but is shared
/// for `.waitForTaskToken` SDK integrations.
pub(crate) async fn poll_task_token(
    shared_state: &SharedStepFunctionsState,
    account_id: &str,
    token: &str,
    timeout_seconds: Option<u64>,
    heartbeat_seconds: Option<u64>,
) -> Result<Value, (String, String)> {
    let absolute_deadline = deadline_after_secs(timeout_seconds.unwrap_or(3600));
    loop {
        let now_ts = chrono::Utc::now();
        let snapshot = {
            let accounts = shared_state.read();
            accounts
                .get(account_id)
                .and_then(|s| s.task_tokens.get(token).cloned())
        };
        let Some(entry) = snapshot else {
            return Err((
                "States.TaskFailed".to_string(),
                "Task token disappeared".to_string(),
            ));
        };
        match entry.status.as_str() {
            "SUCCEEDED" => {
                cleanup_token(shared_state, account_id, token);
                let output = entry.output.unwrap_or_else(|| "{}".to_string());
                let value: Value = serde_json::from_str(&output).unwrap_or(Value::String(output));
                return Ok(value);
            }
            "FAILED" => {
                cleanup_token(shared_state, account_id, token);
                return Err((
                    entry
                        .error
                        .unwrap_or_else(|| "States.TaskFailed".to_string()),
                    entry.cause.unwrap_or_default(),
                ));
            }
            _ => {}
        }
        // Heartbeat timeout: only enforced once the worker has picked up the
        // task (status == IN_PROGRESS) and a heartbeat window is set.
        if entry.status == "IN_PROGRESS" {
            if let Some(hb) = heartbeat_seconds {
                let last = entry.last_heartbeat_at.unwrap_or(entry.created_at);
                if (now_ts - last).num_seconds() > i64::try_from(hb).unwrap_or(i64::MAX) {
                    cleanup_token(shared_state, account_id, token);
                    return Err((
                        "States.HeartbeatTimeout".to_string(),
                        format!("Worker missed heartbeat ({hb}s window)"),
                    ));
                }
            }
        }
        if absolute_deadline.is_some_and(|d| std::time::Instant::now() >= d) {
            cleanup_token(shared_state, account_id, token);
            let secs = timeout_seconds.unwrap_or(3600);
            return Err((
                "States.Timeout".to_string(),
                format!("Task timed out after {secs} seconds"),
            ));
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Apply Parameters template: keys ending with .$ are treated as
/// JsonPath references *or* ASL intrinsic invocations
/// (`States.Foo(...)`).
///
/// When `context` is provided, expressions starting with `$$.` resolve
/// against the context object (Step Functions context object) instead of
/// the state input. This is how `$$.Task.Token` is substituted for
/// `.waitForTaskToken` integrations.
pub(crate) fn apply_parameters(
    template: &Value,
    input: &Value,
    context: Option<&Value>,
) -> Result<Value, (String, String)> {
    match template {
        Value::Object(map) => {
            let mut result = serde_json::Map::new();
            for (key, value) in map {
                if let Some(stripped) = key.strip_suffix(".$") {
                    if let Some(expr) = value.as_str() {
                        let resolved = if crate::intrinsics::is_intrinsic_call(expr) {
                            // A failing intrinsic fails the state
                            // (States.IntrinsicFailure; a path argument that
                            // matches nothing is States.Runtime).
                            crate::intrinsics::evaluate_with_context(expr, input, context)
                                .map_err(|e| e.into_states_error())?
                        } else {
                            crate::io_processing::resolve_with_context(input, context, expr)
                                .map_err(|_| {
                                    let scope = if expr.starts_with("$$") {
                                        context.cloned().unwrap_or(Value::Null)
                                    } else {
                                        input.clone()
                                    };
                                    (
                                        "States.Runtime".to_string(),
                                        format!(
                                            "The JSONPath '{expr}' specified for the field '{key}' \
                                             could not be found in the input '{scope}'"
                                        ),
                                    )
                                })?
                        };
                        result.insert(stripped.to_string(), resolved);
                    }
                } else {
                    result.insert(key.clone(), apply_parameters(value, input, context)?);
                }
            }
            Ok(Value::Object(result))
        }
        Value::Array(arr) => Ok(Value::Array(
            arr.iter()
                .map(|v| apply_parameters(v, input, context))
                .collect::<Result<_, _>>()?,
        )),
        other => Ok(other.clone()),
    }
}

pub(crate) fn next_state(state_def: &Value) -> NextState {
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
pub(crate) fn apply_state_catcher(
    state_def: &Value,
    effective_input: &Value,
    error: &str,
    cause: &str,
    is_task_error: bool,
) -> Option<(String, Value)> {
    let catchers = state_def["Catch"].as_array().cloned().unwrap_or_default();
    let (next, result_path) = find_catcher(&catchers, error, is_task_error)?;
    let error_output = json!({
        "Error": error,
        "Cause": cause,
    });
    let new_input = apply_result_path(effective_input, &error_output, result_path.as_deref());
    Some((next, new_input))
}

/// Extract the region from an execution ARN (`arn:aws:states:region:account_id:...`).
pub(crate) fn region_from_arn(arn: &str) -> &str {
    arn.split(':').nth(3).unwrap_or("")
}

/// Extract account ID from an execution ARN (`arn:aws:states:region:account_id:...`).
pub(crate) fn account_id_from_arn(arn: &str) -> &str {
    arn.split(':').nth(4).unwrap_or("000000000000")
}

pub(crate) fn add_event(
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

/// Apply a terminal transition to `exec` only if it is still `Running`.
/// Returns `true` when the transition was applied.
///
/// This centralizes the write-guard re-check that `succeed_execution` /
/// `fail_execution` perform: a concurrent `StopExecution` may set the
/// execution to `Aborted` (or a timeout to `TimedOut`) in the window after the
/// interpreter's initial read-guard check but before it re-acquires the write
/// guard. Overwriting that terminal state would make `DescribeExecution` report
/// `SUCCEEDED`/`FAILED` for an execution the caller already stopped. Keeping the
/// guard here means the whole terminal-transition stays atomic under the held
/// write guard.
pub(crate) fn apply_terminal_transition_if_running(
    exec: &mut crate::state::Execution,
    transition: impl FnOnce(&mut crate::state::Execution),
) -> bool {
    if exec.status == ExecutionStatus::Running {
        transition(exec);
        true
    } else {
        false
    }
}

pub(crate) fn succeed_execution(
    state: &SharedStepFunctionsState,
    execution_arn: &str,
    output: &Value,
) {
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

    let output_str =
        serde_json::to_string(output).expect("serde_json::Value serialization is infallible");

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
        apply_terminal_transition_if_running(exec, |exec| {
            exec.status = ExecutionStatus::Succeeded;
            exec.output = Some(output_str);
            exec.stop_date = Some(Utc::now());
        });
    }
}

pub(crate) fn fail_execution(
    state: &SharedStepFunctionsState,
    execution_arn: &str,
    error: &str,
    cause: &str,
) {
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
        apply_terminal_transition_if_running(exec, |exec| {
            exec.status = ExecutionStatus::Failed;
            exec.error = Some(error.to_string());
            exec.cause = Some(cause.to_string());
            exec.stop_date = Some(Utc::now());
        });
    }
}

/// Deliver execution history events to CloudWatch Logs when the state
/// machine has a logging configuration.
pub(crate) fn deliver_execution_logs(
    state: &SharedStepFunctionsState,
    execution_arn: &str,
    delivery: Option<&Arc<DeliveryBus>>,
    logging_configuration: Option<&Value>,
) {
    let config = match logging_configuration {
        Some(c) => c,
        None => return,
    };

    let level = config["level"].as_str().unwrap_or("OFF");
    if level == "OFF" {
        return;
    }

    let destinations = config["destinations"].as_array();
    let log_group_arn = destinations.and_then(|d| {
        d.iter()
            .find_map(|dest| dest["cloudWatchLogsLogGroup"]["logGroupArn"].as_str())
    });

    let log_group_arn = match log_group_arn {
        Some(a) => a,
        None => return,
    };

    // Parse log group ARN: arn:aws:logs:region:account-id:log-group:group-name
    let parts: Vec<&str> = log_group_arn.split(':').collect();
    if parts.len() < 6 {
        return;
    }
    let log_account_id = parts[4];
    let log_group_name = parts
        .last()
        .map_or("", |v| v)
        .trim_start_matches("log-group:")
        .to_string();
    let log_group_name = log_group_name.trim_end_matches(":*");

    let account_id = account_id_from_arn(execution_arn).to_string();
    let accounts = state.read();
    let s = match accounts.get(&account_id) {
        Some(st) => st,
        None => return,
    };
    let exec = match s.executions.get(execution_arn) {
        Some(e) => e,
        None => return,
    };

    let _now = Utc::now().timestamp_millis();
    let stream_name = exec.name.clone();

    let include_data = config["includeExecutionData"].as_bool().unwrap_or(false);

    let events: Vec<(i64, String)> = exec
        .history_events
        .iter()
        .filter_map(|ev| {
            // Skip non-error events when level is ERROR unless it's a terminal failure.
            if level == "ERROR"
                && !matches!(
                    ev.event_type.as_str(),
                    "ExecutionFailed" | "TaskFailed" | "StateFailed"
                )
            {
                return None;
            }
            let mut detail = json!({
                "id": ev.id,
                "type": ev.event_type,
                "timestamp": ev.timestamp.timestamp_millis(),
                "previousEventId": ev.previous_event_id,
            });
            if include_data {
                detail["details"] = ev.details.clone();
            }
            Some((ev.timestamp.timestamp_millis(), detail.to_string()))
        })
        .collect();

    drop(accounts);

    if let Some(d) = delivery {
        d.put_log_events(log_account_id, log_group_name, &stream_name, &events);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Records each event and refuses those addressed to a bus ARN in
    /// another account.
    /// (account, region, bus, principal) of each recorded event.
    type GatedCall = (String, String, String, Option<String>);

    #[derive(Default)]
    struct GatedEb(std::sync::Mutex<Vec<GatedCall>>);

    impl fakecloud_core::delivery::EventBridgeDelivery for GatedEb {
        fn put_event(
            &self,
            e: &fakecloud_core::delivery::CrossServiceEvent<'_>,
        ) -> Result<String, fakecloud_core::delivery::EventBridgeDeliveryError> {
            self.0.lock().unwrap().push((
                e.account_id.to_string(),
                e.region.to_string(),
                e.event_bus.to_string(),
                e.principal_arn.map(str::to_string),
            ));
            if e.event_bus.starts_with("arn:") {
                return Err(
                    fakecloud_core::delivery::EventBridgeDeliveryError::AccessDenied(
                        "denied".to_string(),
                    ),
                );
            }
            Ok("evt-1".to_string())
        }
    }

    const EXEC_ARN: &str = "arn:aws:states:eu-west-2:111111111111:execution:sm:run";
    const ROLE: &str = "arn:aws:iam::111111111111:role/sfn";

    /// events:putEvents puts as the execution's account/region/role and
    /// returns the real event id.
    #[test]
    fn eventbridge_put_events_uses_execution_scope_and_role() {
        let eb = Arc::new(GatedEb::default());
        let delivery = Some(Arc::new(DeliveryBus::new().with_eventbridge(eb.clone())));
        let out = invoke_eventbridge_put_events(
            &json!({"Entries": [{"Source": "app", "DetailType": "T", "Detail": "{}"}]}),
            &delivery,
            EXEC_ARN,
            ROLE,
        )
        .unwrap();
        assert_eq!(out["FailedEntryCount"], 0);
        assert_eq!(out["Entries"][0]["EventId"], "evt-1");
        let calls = eb.0.lock().unwrap();
        assert_eq!(calls[0].0, "111111111111");
        assert_eq!(calls[0].1, "eu-west-2");
        assert_eq!(calls[0].3.as_deref(), Some(ROLE));
    }

    /// An event EventBridge could not take (delivery not wired yet) is a
    /// failed entry, never a success with an empty EventId.
    #[test]
    fn eventbridge_put_events_unavailable_is_a_failed_entry() {
        struct Unwired;
        impl fakecloud_core::delivery::EventBridgeDelivery for Unwired {
            fn put_event(
                &self,
                _e: &fakecloud_core::delivery::CrossServiceEvent<'_>,
            ) -> Result<String, fakecloud_core::delivery::EventBridgeDeliveryError> {
                Err(
                    fakecloud_core::delivery::EventBridgeDeliveryError::Unavailable(
                        "not wired".to_string(),
                    ),
                )
            }
        }
        let delivery = Some(Arc::new(
            DeliveryBus::new().with_eventbridge(Arc::new(Unwired)),
        ));
        let (error, cause) = invoke_eventbridge_put_events(
            &json!({"Entries": [{"Source": "app", "DetailType": "T", "Detail": "{}"}]}),
            &delivery,
            EXEC_ARN,
            ROLE,
        )
        .unwrap_err();
        assert_eq!(error, "EventBridge.FailedEntry");
        let cause: Value = serde_json::from_str(&cause).unwrap();
        assert_eq!(cause["Entries"][0]["ErrorCode"], "InternalException");
    }

    /// A refused entry fails the task with EventBridge.FailedEntry, as AWS's
    /// optimized integration does when FailedEntryCount > 0.
    #[test]
    fn eventbridge_put_events_refused_entry_fails_task() {
        let eb = Arc::new(GatedEb::default());
        let delivery = Some(Arc::new(DeliveryBus::new().with_eventbridge(eb)));
        let (error, cause) = invoke_eventbridge_put_events(
            &json!({"Entries": [{
                "Source": "app", "DetailType": "T", "Detail": "{}",
                "EventBusName": "arn:aws:events:eu-west-2:222222222222:event-bus/other"
            }]}),
            &delivery,
            EXEC_ARN,
            ROLE,
        )
        .unwrap_err();
        assert_eq!(error, "EventBridge.FailedEntry");
        let cause: Value = serde_json::from_str(&cause).unwrap();
        assert_eq!(cause["FailedEntryCount"], 1);
        assert_eq!(cause["Entries"][0]["ErrorCode"], "AccessDeniedException");
    }

    /// The DynamoDB updateItem integration must refuse to write a key
    /// attribute, as DynamoDB does, rather than move the row to another key.
    #[test]
    fn dynamodb_update_item_rejects_writing_a_key_attribute() {
        let ddb: SharedDynamoDbState = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        {
            let mut accounts = ddb.write();
            let mut table = fakecloud_dynamodb::DynamoTable::new(
                "t".to_string(),
                "arn:aws:dynamodb:us-east-1:123456789012:table/t".to_string(),
                "id".to_string(),
                vec![fakecloud_dynamodb::KeySchemaElement {
                    attribute_name: "pk".to_string(),
                    key_type: "HASH".to_string(),
                }],
                vec![],
                fakecloud_dynamodb::ProvisionedThroughput {
                    read_capacity_units: 1,
                    write_capacity_units: 1,
                },
                "PAY_PER_REQUEST".to_string(),
                chrono::Utc::now(),
            );
            let mut row = HashMap::new();
            row.insert("pk".to_string(), json!({"S": "a"}));
            table.put_item_at_key(row);
            accounts.default_mut().tables.insert("t".to_string(), table);
        }
        let state = Some(ddb.clone());

        let err = invoke_dynamodb_update_item(
            &json!({
                "TableName": "t",
                "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET #k = :v",
                "ExpressionAttributeNames": {"#k": "pk"},
                "ExpressionAttributeValues": {":v": {"S": "b"}}
            }),
            &state,
        )
        .expect_err("key write accepted");
        assert_eq!(err.0, "DynamoDB.AmazonDynamoDBException");
        assert!(err.1.contains("Cannot update attribute pk"), "{}", err.1);

        let accounts = ddb.read();
        let rows: Vec<_> = accounts.default_ref().tables["t"]
            .items()
            .iter()
            .cloned()
            .collect();
        assert_eq!(
            rows,
            vec![HashMap::from([("pk".to_string(), json!({"S": "a"}))])]
        );

        // A non-key update still applies.
        drop(accounts);
        invoke_dynamodb_update_item(
            &json!({
                "TableName": "t",
                "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET v = :v",
                "ExpressionAttributeValues": {":v": {"S": "b"}}
            }),
            &state,
        )
        .unwrap();
    }

    #[test]
    fn apply_parameters_resolves_intrinsic_calls() {
        // States.Format intrinsic with $-references plus a literal.
        let template = json!({
            "greeting.$": "States.Format('Hello {}, count is {}', $.name, $.n)",
            "literal": "static",
        });
        let input = json!({"name": "Eve", "n": 7});
        let out = apply_parameters(&template, &input, None).unwrap();
        assert_eq!(out["greeting"], json!("Hello Eve, count is 7"));
        assert_eq!(out["literal"], json!("static"));
    }

    #[test]
    fn apply_parameters_falls_back_to_jsonpath_for_non_intrinsics() {
        let template = json!({"x.$": "$.value"});
        let input = json!({"value": 42});
        let out = apply_parameters(&template, &input, None).unwrap();
        assert_eq!(out["x"], json!(42));
    }

    #[test]
    fn apply_parameters_intrinsic_failure_fails_the_state() {
        // Bad call: missing closing paren.
        let template = json!({"y.$": "States.Format('{}'"});
        let (error, _) = apply_parameters(&template, &Value::Null, None).unwrap_err();
        assert_eq!(error, "States.IntrinsicFailure");
        // Nested inside an array/object too.
        let template = json!({"a": [{"r.$": "States.ArrayGetItem($.arr, -1)"}]});
        let (error, _) = apply_parameters(&template, &json!({"arr": [1]}), None).unwrap_err();
        assert_eq!(error, "States.IntrinsicFailure");
    }

    #[test]
    fn apply_parameters_missing_path_is_states_runtime() {
        let template = json!({"x.$": "$.missing"});
        let (error, cause) = apply_parameters(&template, &json!({"a": 1}), None).unwrap_err();
        assert_eq!(error, "States.Runtime");
        assert_eq!(
            cause,
            r#"The JSONPath '$.missing' specified for the field 'x.$' could not be found in the input '{"a":1}'"#
        );
        // An intrinsic path argument that matches nothing is States.Runtime too.
        let template = json!({"x.$": "States.Format('{}', $.missing)"});
        let (error, _) = apply_parameters(&template, &json!({}), None).unwrap_err();
        assert_eq!(error, "States.Runtime");
    }

    #[test]
    fn missing_paths_fail_pass_state() {
        for def in [
            json!({"Type": "Pass", "InputPath": "$.nope", "End": true}),
            json!({"Type": "Pass", "OutputPath": "$.nope", "End": true}),
            json!({"Type": "Pass", "Parameters": {"a.$": "$.nope"}, "End": true}),
        ] {
            let (error, _) = execute_pass_state(&def, &json!({"x": 1}), None).unwrap_err();
            assert_eq!(error, "States.Runtime", "{def}");
        }
    }

    #[test]
    fn pass_state_evaluates_parameters_template() {
        // A Pass state with Parameters builds a fresh result from the template,
        // resolving $-references against the (InputPath-filtered) input.
        let state_def = json!({
            "Type": "Pass",
            "Parameters": {
                "renamed.$": "$.value",
                "constant": "fixed",
            },
            "End": true,
        });
        let input = json!({"value": 99, "ignored": "x"});
        let out = execute_pass_state(&state_def, &input, None).unwrap();
        assert_eq!(out["renamed"], json!(99));
        assert_eq!(out["constant"], json!("fixed"));
        // The non-templated field is dropped (Parameters builds a new payload).
        assert!(out.get("ignored").is_none());
    }

    #[test]
    fn pass_state_parameters_then_output_path() {
        // Parameters builds the result, OutputPath then narrows it.
        let state_def = json!({
            "Type": "Pass",
            "Parameters": {"a.$": "$.n", "b": 1},
            "OutputPath": "$.a",
            "End": true,
        });
        let out = execute_pass_state(&state_def, &json!({"n": 7}), None).unwrap();
        assert_eq!(out, json!(7));
    }

    #[test]
    fn pass_and_task_parameters_intrinsic_failure_is_states_intrinsic_failure() {
        let state_def = json!({
            "Type": "Pass",
            "Parameters": {"r.$": "States.ArrayRange(1, 5000, 1)"},
            "End": true,
        });
        let (error, cause) = execute_pass_state(&state_def, &json!({}), None).unwrap_err();
        assert_eq!(error, "States.IntrinsicFailure");
        assert!(cause.contains("1000"), "{cause}");
        let (error, _) = apply_parameters(
            &json!({"nested": [{"r.$": "States.ArrayRange(1, 5000, 1)"}]}),
            &json!({}),
            None,
        )
        .unwrap_err();
        assert_eq!(error, "States.IntrinsicFailure");
    }
}

#[cfg(test)]
mod apply_add_set_tests {
    use super::apply_add;
    use serde_json::{json, Map, Value};
    use std::collections::HashMap;

    fn values(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    // bug-audit 2026-05-28, 1.16: ADD to a string set must union elements.
    #[test]
    fn add_to_string_set_unions_elements() {
        let mut item: HashMap<String, Value> = HashMap::new();
        item.insert("tags".to_string(), json!({ "SS": ["a", "b"] }));
        apply_add(
            &mut item,
            "tags :v",
            &values(json!({ ":v": { "SS": ["b", "c"] } })),
            &Map::new(),
        );
        assert_eq!(item["tags"], json!({ "SS": ["a", "b", "c"] }));
    }

    #[test]
    fn add_to_missing_set_creates_it() {
        let mut item: HashMap<String, Value> = HashMap::new();
        apply_add(
            &mut item,
            "nums :v",
            &values(json!({ ":v": { "NS": ["1", "2"] } })),
            &Map::new(),
        );
        assert_eq!(item["nums"], json!({ "NS": ["1", "2"] }));
    }

    #[test]
    fn add_numeric_still_increments() {
        let mut item: HashMap<String, Value> = HashMap::new();
        item.insert("count".to_string(), json!({ "N": "5" }));
        apply_add(
            &mut item,
            "count :v",
            &values(json!({ ":v": { "N": "3" } })),
            &Map::new(),
        );
        assert_eq!(item["count"], json!({ "N": "8" }));
    }
}
