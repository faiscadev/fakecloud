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
) -> Advance {
    match apply_state_catcher(state_def, input, &error, &cause) {
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

    let result = execute_pass_state(state_def, &input);

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
                    "output": serde_json::to_string(&input).expect("serde_json::Value serialization is infallible"),
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

/// Execute a Pass state: apply InputPath, use Result if present, apply ResultPath and OutputPath.
pub(crate) fn execute_pass_state(state_def: &Value, input: &Value) -> Value {
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

/// Send a message to an SQS queue via DeliveryBus.
pub(crate) fn invoke_sqs_send_message(
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
            serde_json::to_string(&input["MessageBody"])
                .expect("serde_json::Value serialization is infallible")
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

/// Put events onto an EventBridge bus via DeliveryBus.
pub(crate) fn invoke_eventbridge_put_events(
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
            .unwrap_or_else(|| {
                serde_json::to_string(&entry["Detail"])
                    .expect("serde_json::Value serialization is infallible")
            });
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

    // Replace existing item with same key, or insert new
    if let Some(idx) = table.find_item_index(&item_map) {
        table.items[idx] = item_map;
    } else {
        table.items.push(item_map);
    }

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

    if let Some(idx) = table.find_item_index(&key_map) {
        table.items.remove(idx);
    }

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

/// Convert an SQS queue URL to an ARN.
/// QueueUrl format: http://localhost:4566/123456789012/my-queue
pub(crate) fn queue_url_to_arn(url: &str) -> String {
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
    let absolute_deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(timeout_seconds.unwrap_or(3600));
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
                if (now_ts - last).num_seconds() > hb as i64 {
                    cleanup_token(shared_state, account_id, token);
                    return Err((
                        "States.HeartbeatTimeout".to_string(),
                        format!("Worker missed heartbeat ({hb}s window)"),
                    ));
                }
            }
        }
        if std::time::Instant::now() >= absolute_deadline {
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
pub(crate) fn apply_parameters(template: &Value, input: &Value, context: Option<&Value>) -> Value {
    match template {
        Value::Object(map) => {
            let mut result = serde_json::Map::new();
            for (key, value) in map {
                if let Some(stripped) = key.strip_suffix(".$") {
                    if let Some(expr) = value.as_str() {
                        let resolved = if crate::intrinsics::is_intrinsic_call(expr) {
                            crate::intrinsics::evaluate(expr, input).unwrap_or_else(|err| {
                                tracing::warn!(error = %err, "States intrinsic failed");
                                Value::Null
                            })
                        } else if expr.starts_with("$$.") {
                            if let Some(ctx) = context {
                                let path = expr.strip_prefix("$$.").unwrap_or(expr);
                                crate::io_processing::resolve_path(ctx, path)
                            } else {
                                Value::Null
                            }
                        } else {
                            crate::io_processing::resolve_path(input, expr)
                        };
                        result.insert(stripped.to_string(), resolved);
                    }
                } else {
                    result.insert(key.clone(), apply_parameters(value, input, context));
                }
            }
            Value::Object(result)
        }
        Value::Array(arr) => Value::Array(
            arr.iter()
                .map(|v| apply_parameters(v, input, context))
                .collect(),
        ),
        other => other.clone(),
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
        exec.status = ExecutionStatus::Succeeded;
        exec.output = Some(output_str);
        exec.stop_date = Some(Utc::now());
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
        exec.status = ExecutionStatus::Failed;
        exec.error = Some(error.to_string());
        exec.cause = Some(cause.to_string());
        exec.stop_date = Some(Utc::now());
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

    #[test]
    fn apply_parameters_resolves_intrinsic_calls() {
        // States.Format intrinsic with $-references plus a literal.
        let template = json!({
            "greeting.$": "States.Format('Hello {}, count is {}', $.name, $.n)",
            "literal": "static",
        });
        let input = json!({"name": "Eve", "n": 7});
        let out = apply_parameters(&template, &input, None);
        assert_eq!(out["greeting"], json!("Hello Eve, count is 7"));
        assert_eq!(out["literal"], json!("static"));
    }

    #[test]
    fn apply_parameters_falls_back_to_jsonpath_for_non_intrinsics() {
        let template = json!({"x.$": "$.value"});
        let input = json!({"value": 42});
        let out = apply_parameters(&template, &input, None);
        assert_eq!(out["x"], json!(42));
    }

    #[test]
    fn apply_parameters_intrinsic_failure_yields_null() {
        // Bad call: missing closing paren.
        let template = json!({"y.$": "States.Format('{}'"});
        let out = apply_parameters(&template, &Value::Null, None);
        // Falls through resolve_path since not detected as intrinsic
        // (no `(` after States.Format... actually has `(`, so this *is*
        // an intrinsic and should return Null on failure).
        // But this missing-) call is detected: `is_intrinsic_call` =>
        // contains '(' and starts with States. -> true, then evaluate
        // returns Err -> Null.
        assert_eq!(out["y"], Value::Null);
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
