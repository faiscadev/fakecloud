use std::collections::HashMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::AttributeValue;

/// A queued Kinesis delivery for a single transact write — fired after
/// the apply phase succeeds and the write lock is dropped. Tuple shape:
/// (target, event_name, keys, old_image, new_image).
type PendingKinesis = (
    super::KinesisDeliveryTarget,
    String,
    HashMap<String, AttributeValue>,
    Option<HashMap<String, AttributeValue>>,
    Option<HashMap<String, AttributeValue>>,
);

use super::{
    apply_update_expression, build_consumed_capacity, evaluate_condition, execute_partiql_in_state,
    extract_key, get_table, get_table_mut, parse_expression_attribute_names,
    parse_expression_attribute_values, require_str_with_code, return_consumed_mode,
    return_icm_mode, validate_key_attributes_in_key, validate_key_in_item, DynamoDbService,
};

impl DynamoDbService {
    pub(super) fn batch_get_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;

        validate_optional_enum_value(
            "returnConsumedCapacity",
            &body["ReturnConsumedCapacity"],
            &["INDEXES", "TOTAL", "NONE"],
        )?;

        let return_consumed = return_consumed_mode(&body).to_string();

        let request_items = body["RequestItems"]
            .as_object()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "RequestItems is required",
                )
            })?
            .clone();

        // AWS limits a single BatchGetItem to 100 keys across all
        // tables; over that it returns a ValidationException rather than
        // silently processing the whole oversized batch.
        let total_keys: usize = request_items
            .values()
            .filter_map(|p| p["Keys"].as_array().map(|k| k.len()))
            .sum();
        if total_keys > 100 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "Too many items requested for the BatchGetItem call: {total_keys} \
                     (max 100)"
                ),
            ));
        }

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let mut responses: HashMap<String, Vec<Value>> = HashMap::new();
        let mut consumed_capacity: Vec<Value> = Vec::new();

        for (table_name, params) in &request_items {
            let table = get_table(&state.tables, table_name)?;
            let keys = params["Keys"].as_array().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Keys is required",
                )
            })?;

            let mut items = Vec::new();
            for key_val in keys {
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(key_val.clone()).unwrap_or_default();
                // Reject malformed/under-specified keys the same way
                // GetItem does instead of coercing to `{}`.
                validate_key_attributes_in_key(table, &key)?;
                if let Some(idx) = table.find_item_index(&key) {
                    // Honor the per-table ProjectionExpression /
                    // AttributesToGet so callers only get the attributes
                    // they asked for (GetItem already does this).
                    let projected = super::project_item(&table.items[idx], params);
                    items.push(json!(projected));
                }
            }
            let key_count = keys.len().max(1) as f64;
            responses.insert(table_name.clone(), items);

            let cc = build_consumed_capacity(&return_consumed, table_name, key_count * 0.5, 0.0);
            if !cc.is_null() {
                consumed_capacity.push(cc);
            }
        }

        let mut result = json!({
            "Responses": responses,
            "UnprocessedKeys": {},
        });

        if !consumed_capacity.is_empty() {
            result["ConsumedCapacity"] = json!(consumed_capacity);
        }

        Self::ok_json(result)
    }

    pub(super) fn batch_write_item(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;

        validate_optional_enum_value(
            "returnConsumedCapacity",
            &body["ReturnConsumedCapacity"],
            &["INDEXES", "TOTAL", "NONE"],
        )?;
        validate_optional_enum_value(
            "returnItemCollectionMetrics",
            &body["ReturnItemCollectionMetrics"],
            &["SIZE", "NONE"],
        )?;

        let return_consumed = return_consumed_mode(&body).to_string();
        let return_icm = return_icm_mode(&body).to_string();

        let request_items = body["RequestItems"]
            .as_object()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "RequestItems is required",
                )
            })?
            .clone();

        // AWS caps a single BatchWriteItem at 25 write requests across
        // all tables; over that it returns a ValidationException rather
        // than processing the oversized batch.
        let total_requests: usize = request_items
            .values()
            .filter_map(|r| r.as_array().map(|a| a.len()))
            .sum();
        if total_requests > 25 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "Too many items requested for the BatchWriteItem call: {total_requests} \
                     (max 25)"
                ),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut consumed_capacity: Vec<Value> = Vec::new();
        let mut item_collection_metrics: HashMap<String, Vec<Value>> = HashMap::new();

        // Validate every request before mutating any state so a
        // malformed/keyless item or a duplicate key in the batch fails
        // the whole call (AWS rejects these up-front, not after partial
        // application).
        for (table_name, requests) in &request_items {
            let table = state.tables.get(table_name.as_str()).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Requested resource not found: Table: {table_name} not found"),
                )
            })?;
            let reqs = requests.as_array().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Request list must be an array",
                )
            })?;
            let mut seen_keys: Vec<HashMap<String, AttributeValue>> = Vec::new();
            for request in reqs {
                let key = if let Some(put_req) = request.get("PutRequest") {
                    let item: HashMap<String, AttributeValue> =
                        serde_json::from_value(put_req["Item"].clone()).map_err(|_| {
                            AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                "PutRequest.Item is not a valid item",
                            )
                        })?;
                    validate_key_in_item(table, &item)?;
                    extract_key(table, &item)
                } else if let Some(del_req) = request.get("DeleteRequest") {
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(del_req["Key"].clone()).map_err(|_| {
                            AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                "DeleteRequest.Key is not a valid key",
                            )
                        })?;
                    validate_key_attributes_in_key(table, &key)?;
                    key
                } else {
                    continue;
                };
                if seen_keys.contains(&key) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        "Provided list of item keys contains duplicates",
                    ));
                }
                seen_keys.push(key);
            }
        }

        for (table_name, requests) in &request_items {
            let table = state.tables.get_mut(table_name.as_str()).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Requested resource not found: Table: {table_name} not found"),
                )
            })?;

            let reqs = requests.as_array().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Request list must be an array",
                )
            })?;

            let mut write_count = 0u32;
            let mut keys_for_icm: Vec<HashMap<String, AttributeValue>> = Vec::new();
            for request in reqs {
                if let Some(put_req) = request.get("PutRequest") {
                    let item: HashMap<String, AttributeValue> =
                        serde_json::from_value(put_req["Item"].clone()).unwrap_or_default();
                    let key = extract_key(table, &item);
                    keys_for_icm.push(key.clone());
                    if let Some(idx) = table.find_item_index(&key) {
                        table.items[idx] = item;
                    } else {
                        table.items.push(item);
                    }
                    write_count += 1;
                } else if let Some(del_req) = request.get("DeleteRequest") {
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(del_req["Key"].clone()).unwrap_or_default();
                    keys_for_icm.push(key.clone());
                    if let Some(idx) = table.find_item_index(&key) {
                        table.items.remove(idx);
                    }
                    write_count += 1;
                }
            }

            table.recalculate_stats();

            let cc = build_consumed_capacity(
                &return_consumed,
                table_name,
                0.0,
                write_count.max(1) as f64,
            );
            if !cc.is_null() {
                consumed_capacity.push(cc);
            }

            if return_icm == "SIZE" && !table.lsi.is_empty() {
                let entries: Vec<Value> = keys_for_icm
                    .iter()
                    .map(|k| super::helpers::build_item_collection_metrics(&return_icm, table, k))
                    .filter(|v| !v.is_null())
                    .collect();
                if !entries.is_empty() {
                    item_collection_metrics.insert(table_name.clone(), entries);
                }
            }
        }

        let mut result = json!({
            "UnprocessedItems": {},
        });

        if !consumed_capacity.is_empty() {
            result["ConsumedCapacity"] = json!(consumed_capacity);
        }

        if return_icm == "SIZE" && !item_collection_metrics.is_empty() {
            result["ItemCollectionMetrics"] = json!(item_collection_metrics);
        }

        Self::ok_json(result)
    }

    pub(super) fn transact_get_items(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_enum_value(
            "returnConsumedCapacity",
            &body["ReturnConsumedCapacity"],
            &["INDEXES", "TOTAL", "NONE"],
        )?;
        let return_consumed = return_consumed_mode(&body).to_string();
        let transact_items = body["TransactItems"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TransactItems is required",
            )
        })?;

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let mut responses: Vec<Value> = Vec::new();
        let mut per_table_count: HashMap<String, u32> = HashMap::new();

        for ti in transact_items {
            let get = &ti["Get"];
            let table_name = get["TableName"].as_str().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "TableName is required in Get",
                )
            })?;
            let key: HashMap<String, AttributeValue> =
                serde_json::from_value(get["Key"].clone()).unwrap_or_default();

            let table = get_table(&state.tables, table_name)?;
            match table.find_item_index(&key) {
                Some(idx) => {
                    responses.push(json!({ "Item": table.items[idx] }));
                }
                None => {
                    responses.push(json!({}));
                }
            }
            *per_table_count.entry(table_name.to_string()).or_insert(0) += 1;
        }

        let mut result = json!({ "Responses": responses });
        let consumed: Vec<Value> = per_table_count
            .iter()
            .filter_map(|(t, n)| {
                let cc = build_consumed_capacity(&return_consumed, t, (*n as f64) * 2.0, 0.0);
                if cc.is_null() {
                    None
                } else {
                    Some(cc)
                }
            })
            .collect();
        if !consumed.is_empty() {
            result["ConsumedCapacity"] = json!(consumed);
        }

        Self::ok_json(result)
    }

    pub(super) fn transact_write_items(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length(
            "clientRequestToken",
            body["ClientRequestToken"].as_str(),
            1,
            36,
        )?;
        validate_optional_enum_value(
            "returnConsumedCapacity",
            &body["ReturnConsumedCapacity"],
            &["INDEXES", "TOTAL", "NONE"],
        )?;
        validate_optional_enum_value(
            "returnItemCollectionMetrics",
            &body["ReturnItemCollectionMetrics"],
            &["SIZE", "NONE"],
        )?;
        let return_consumed = return_consumed_mode(&body).to_string();
        let return_icm = return_icm_mode(&body).to_string();
        let transact_items = body["TransactItems"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TransactItems is required",
            )
        })?;

        // Per-operation `ReturnValuesOnConditionCheckFailure` is its own
        // enum; validate it up-front so a malformed value short-circuits
        // before we touch the state lock. Real DDB rejects unknown values
        // with a top-level ValidationException, not a CancellationReason.
        for ti in transact_items {
            for op_key in ["Put", "Delete", "Update", "ConditionCheck"] {
                if let Some(op) = ti.get(op_key) {
                    validate_optional_enum_value(
                        "returnValuesOnConditionCheckFailure",
                        &op["ReturnValuesOnConditionCheckFailure"],
                        &["ALL_OLD", "NONE"],
                    )?;
                }
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate every referenced table exists up-front. Without this
        // check a missing TableName on a Put with no condition would fail
        // partway through the apply loop and leave earlier writes
        // committed — TransactWriteItems must be all-or-nothing.
        for ti in transact_items {
            for op_key in ["Put", "Delete", "Update", "ConditionCheck"] {
                if let Some(op) = ti.get(op_key) {
                    let table_name = op["TableName"].as_str().unwrap_or_default();
                    get_table(&state.tables, table_name)?;
                }
            }
        }

        // First pass: validate all conditions. We collect every
        // operation's outcome so the per-index `CancellationReasons`
        // array has a 1:1 alignment with `TransactItems` even when
        // multiple ops fail. When a Put/Update/Delete/ConditionCheck
        // sets `ReturnValuesOnConditionCheckFailure=ALL_OLD` and its
        // ConditionExpression fails, the existing item is surfaced
        // under the reason's `Item` field — matching the real DDB
        // response shape used by aws-sdk-go's
        // `ConditionalCheckFailedException.Item` field.
        let mut cancellation_reasons: Vec<Value> = Vec::new();
        let mut failed_codes: Vec<String> = Vec::new();
        let mut per_table_writes: HashMap<String, u32> = HashMap::new();

        let push_cond_failure =
            |reasons: &mut Vec<Value>,
             codes: &mut Vec<String>,
             return_values: Option<&str>,
             existing: Option<&HashMap<String, AttributeValue>>| {
                let mut reason = json!({
                    "Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed",
                });
                if return_values == Some("ALL_OLD") {
                    if let Some(item) = existing {
                        reason["Item"] = json!(item);
                    }
                }
                reasons.push(reason);
                codes.push("ConditionalCheckFailed".to_string());
            };

        for ti in transact_items {
            if let Some(put) = ti.get("Put") {
                let table_name = put["TableName"].as_str().unwrap_or_default();
                let item: HashMap<String, AttributeValue> =
                    serde_json::from_value(put["Item"].clone()).unwrap_or_default();
                let condition = put["ConditionExpression"].as_str();
                let return_values = put["ReturnValuesOnConditionCheckFailure"].as_str();

                if let Some(cond) = condition {
                    let table = get_table(&state.tables, table_name)?;
                    let expr_attr_names = parse_expression_attribute_names(put);
                    let expr_attr_values = parse_expression_attribute_values(put);
                    let key = extract_key(table, &item);
                    let existing_idx = table.find_item_index(&key);
                    let existing = existing_idx.map(|i| &table.items[i]);
                    if evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)
                        .is_err()
                    {
                        push_cond_failure(
                            &mut cancellation_reasons,
                            &mut failed_codes,
                            return_values,
                            existing,
                        );
                        continue;
                    }
                }
                cancellation_reasons.push(json!({ "Code": "None" }));
            } else if let Some(delete) = ti.get("Delete") {
                let table_name = delete["TableName"].as_str().unwrap_or_default();
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(delete["Key"].clone()).unwrap_or_default();
                let condition = delete["ConditionExpression"].as_str();
                let return_values = delete["ReturnValuesOnConditionCheckFailure"].as_str();

                if let Some(cond) = condition {
                    let table = get_table(&state.tables, table_name)?;
                    let expr_attr_names = parse_expression_attribute_names(delete);
                    let expr_attr_values = parse_expression_attribute_values(delete);
                    let existing_idx = table.find_item_index(&key);
                    let existing = existing_idx.map(|i| &table.items[i]);
                    if evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)
                        .is_err()
                    {
                        push_cond_failure(
                            &mut cancellation_reasons,
                            &mut failed_codes,
                            return_values,
                            existing,
                        );
                        continue;
                    }
                }
                cancellation_reasons.push(json!({ "Code": "None" }));
            } else if let Some(update) = ti.get("Update") {
                let table_name = update["TableName"].as_str().unwrap_or_default();
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(update["Key"].clone()).unwrap_or_default();
                let condition = update["ConditionExpression"].as_str();
                let return_values = update["ReturnValuesOnConditionCheckFailure"].as_str();

                if let Some(cond) = condition {
                    let table = get_table(&state.tables, table_name)?;
                    let expr_attr_names = parse_expression_attribute_names(update);
                    let expr_attr_values = parse_expression_attribute_values(update);
                    let existing_idx = table.find_item_index(&key);
                    let existing = existing_idx.map(|i| &table.items[i]);
                    if evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)
                        .is_err()
                    {
                        push_cond_failure(
                            &mut cancellation_reasons,
                            &mut failed_codes,
                            return_values,
                            existing,
                        );
                        continue;
                    }
                }
                cancellation_reasons.push(json!({ "Code": "None" }));
            } else if let Some(check) = ti.get("ConditionCheck") {
                let table_name = check["TableName"].as_str().unwrap_or_default();
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(check["Key"].clone()).unwrap_or_default();
                let cond = check["ConditionExpression"].as_str().unwrap_or_default();
                let return_values = check["ReturnValuesOnConditionCheckFailure"].as_str();

                let table = get_table(&state.tables, table_name)?;
                let expr_attr_names = parse_expression_attribute_names(check);
                let expr_attr_values = parse_expression_attribute_values(check);
                let existing_idx = table.find_item_index(&key);
                let existing = existing_idx.map(|i| &table.items[i]);
                if evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values).is_err()
                {
                    push_cond_failure(
                        &mut cancellation_reasons,
                        &mut failed_codes,
                        return_values,
                        existing,
                    );
                    continue;
                }
                cancellation_reasons.push(json!({ "Code": "None" }));
            } else {
                cancellation_reasons.push(json!({ "Code": "None" }));
            }
        }

        if !failed_codes.is_empty() {
            // Real DDB lists every failing code (deduped, in order) inside
            // square brackets so the SDKs that match on this string still
            // work when multiple operations fail.
            let mut seen: Vec<String> = Vec::new();
            for code in &failed_codes {
                if !seen.contains(code) {
                    seen.push(code.clone());
                }
            }
            let codes_str = seen.join(", ");
            let error_body = json!({
                "__type": "TransactionCanceledException",
                "message": format!("Transaction cancelled, please refer cancellation reasons for specific reasons [{codes_str}]"),
                "CancellationReasons": cancellation_reasons
            });
            return Ok(AwsResponse::json(
                StatusCode::BAD_REQUEST,
                serde_json::to_vec(&error_body).unwrap(),
            ));
        }

        // Snapshot the items vector of every referenced table so we can
        // revert on any apply-phase failure (e.g. an unparseable
        // UpdateExpression). DDB transactions are all-or-nothing — without
        // this, an UpdateExpression error after a successful Put would
        // leave the Put committed.
        let mut snapshots: HashMap<String, Vec<HashMap<String, AttributeValue>>> = HashMap::new();
        for ti in transact_items {
            for op_key in ["Put", "Delete", "Update"] {
                if let Some(op) = ti.get(op_key) {
                    let table_name = op["TableName"].as_str().unwrap_or_default();
                    snapshots.entry(table_name.to_string()).or_insert_with(|| {
                        state
                            .tables
                            .get(table_name)
                            .map(|t| t.items.clone())
                            .unwrap_or_default()
                    });
                }
            }
        }

        // Stream records pending append + kinesis deliveries pending
        // dispatch — collected during apply, fired after all writes
        // succeed so a mid-batch failure leaves no observable side
        // effects.
        let mut pending_stream: Vec<(String, crate::state::StreamRecord)> = Vec::new();
        let mut pending_kinesis: Vec<PendingKinesis> = Vec::new();
        let region = req.region.clone();

        // Second pass: apply all writes. The closure returns the
        // transact-items index that failed alongside the underlying
        // error so we can build a properly-aligned CancellationReasons
        // array on revert.
        let apply_result = (|| -> Result<(), (usize, AwsServiceError)> {
            for (op_idx, ti) in transact_items.iter().enumerate() {
                if let Some(put) = ti.get("Put") {
                    let table_name = put["TableName"].as_str().unwrap_or_default();
                    let item: HashMap<String, AttributeValue> =
                        serde_json::from_value(put["Item"].clone()).unwrap_or_default();
                    let table =
                        get_table_mut(&mut state.tables, table_name).map_err(|e| (op_idx, e))?;
                    let key = extract_key(table, &item);
                    let old_image = table.find_item_index(&key).map(|i| table.items[i].clone());
                    let is_modify = old_image.is_some();
                    if let Some(idx) = table.find_item_index(&key) {
                        table.items[idx] = item.clone();
                    } else {
                        table.items.push(item.clone());
                    }
                    table.recalculate_stats();
                    let event_name = if is_modify { "MODIFY" } else { "INSERT" };
                    if let Some(record) = crate::streams::generate_stream_record(
                        table,
                        event_name,
                        key.clone(),
                        old_image.clone(),
                        Some(item.clone()),
                        &region,
                    ) {
                        pending_stream.push((table_name.to_string(), record));
                    }
                    if let Some(target) = DynamoDbService::kinesis_target(table) {
                        pending_kinesis.push((
                            target,
                            event_name.to_string(),
                            key,
                            old_image,
                            Some(item),
                        ));
                    }
                    *per_table_writes.entry(table_name.to_string()).or_insert(0) += 1;
                } else if let Some(delete) = ti.get("Delete") {
                    let table_name = delete["TableName"].as_str().unwrap_or_default();
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(delete["Key"].clone()).unwrap_or_default();
                    let table =
                        get_table_mut(&mut state.tables, table_name).map_err(|e| (op_idx, e))?;
                    let old_image = table.find_item_index(&key).map(|i| table.items[i].clone());
                    if let Some(idx) = table.find_item_index(&key) {
                        table.items.remove(idx);
                    }
                    table.recalculate_stats();
                    if old_image.is_some() {
                        if let Some(record) = crate::streams::generate_stream_record(
                            table,
                            "REMOVE",
                            key.clone(),
                            old_image.clone(),
                            None,
                            &region,
                        ) {
                            pending_stream.push((table_name.to_string(), record));
                        }
                        if let Some(target) = DynamoDbService::kinesis_target(table) {
                            pending_kinesis.push((
                                target,
                                "REMOVE".to_string(),
                                key,
                                old_image,
                                None,
                            ));
                        }
                    }
                    *per_table_writes.entry(table_name.to_string()).or_insert(0) += 1;
                } else if let Some(update) = ti.get("Update") {
                    let table_name = update["TableName"].as_str().unwrap_or_default();
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(update["Key"].clone()).unwrap_or_default();
                    let update_expression = update["UpdateExpression"].as_str();
                    let expr_attr_names = parse_expression_attribute_names(update);
                    let expr_attr_values = parse_expression_attribute_values(update);

                    let table =
                        get_table_mut(&mut state.tables, table_name).map_err(|e| (op_idx, e))?;
                    let old_image = table.find_item_index(&key).map(|i| table.items[i].clone());
                    let is_modify = old_image.is_some();
                    let idx = match table.find_item_index(&key) {
                        Some(i) => i,
                        None => {
                            let mut new_item = HashMap::new();
                            for (k, v) in &key {
                                new_item.insert(k.clone(), v.clone());
                            }
                            table.items.push(new_item);
                            table.items.len() - 1
                        }
                    };

                    if let Some(expr) = update_expression {
                        apply_update_expression(
                            &mut table.items[idx],
                            expr,
                            &expr_attr_names,
                            &expr_attr_values,
                        )
                        .map_err(|e| (op_idx, e))?;
                    }
                    let new_image = table.items[idx].clone();
                    table.recalculate_stats();
                    let event_name = if is_modify { "MODIFY" } else { "INSERT" };
                    if let Some(record) = crate::streams::generate_stream_record(
                        table,
                        event_name,
                        key.clone(),
                        old_image.clone(),
                        Some(new_image.clone()),
                        &region,
                    ) {
                        pending_stream.push((table_name.to_string(), record));
                    }
                    if let Some(target) = DynamoDbService::kinesis_target(table) {
                        pending_kinesis.push((
                            target,
                            event_name.to_string(),
                            key,
                            old_image,
                            Some(new_image),
                        ));
                    }
                    *per_table_writes.entry(table_name.to_string()).or_insert(0) += 1;
                }
                // ConditionCheck: no write needed
            }
            Ok(())
        })();

        if let Err((failed_idx, err)) = apply_result {
            // Revert items on every touched table so the partial writes
            // before the failure leave no observable side effects, then
            // surface the failure as a TransactionCanceledException
            // whose CancellationReasons array marks the offending op
            // with `ValidationError` and leaves siblings as `None`.
            for (table_name, items) in snapshots {
                if let Some(table) = state.tables.get_mut(&table_name) {
                    table.items = items;
                    table.recalculate_stats();
                }
            }
            let msg = err.to_string();
            let reasons: Vec<Value> = (0..transact_items.len())
                .map(|i| {
                    if i == failed_idx {
                        json!({
                            "Code": "ValidationError",
                            "Message": msg.clone(),
                        })
                    } else {
                        json!({ "Code": "None" })
                    }
                })
                .collect();
            let error_body = json!({
                "__type": "TransactionCanceledException",
                "message": "Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]",
                "CancellationReasons": reasons
            });
            return Ok(AwsResponse::json(
                StatusCode::BAD_REQUEST,
                serde_json::to_vec(&error_body).unwrap(),
            ));
        }

        // Append all pending stream records under each table's
        // stream_records lock now that the transaction has committed.
        for (table_name, record) in pending_stream {
            if let Some(table) = state.tables.get_mut(&table_name) {
                crate::streams::add_stream_record(table, record);
            }
        }

        let mut result = json!({});
        let consumed: Vec<Value> = per_table_writes
            .iter()
            .filter_map(|(t, n)| {
                let cc = build_consumed_capacity(&return_consumed, t, 0.0, (*n as f64) * 2.0);
                if cc.is_null() {
                    None
                } else {
                    Some(cc)
                }
            })
            .collect();
        if !consumed.is_empty() {
            result["ConsumedCapacity"] = json!(consumed);
        }
        if return_icm == "SIZE" {
            let icm: HashMap<String, Vec<Value>> = per_table_writes
                .keys()
                .map(|t| (t.clone(), vec![]))
                .collect();
            result["ItemCollectionMetrics"] = json!(icm);
        }

        // Drop the write lock before firing kinesis deliveries so the
        // delivery bus (which may take a read lock to look up the target
        // stream) doesn't deadlock against us.
        drop(accounts);
        for (target, event_name, keys, old_image, new_image) in pending_kinesis {
            self.deliver_to_kinesis_destinations(
                &target,
                &event_name,
                &keys,
                old_image.as_ref(),
                new_image.as_ref(),
            );
        }

        Self::ok_json(result)
    }

    // ── PartiQL ─────────────────────────────────────────────────────────

    pub(super) fn execute_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        // A genuinely missing table is the only PartiQL failure AWS maps
        // to ResourceNotFoundException; the shared engine already returns
        // that code via `get_table`. Malformed-PartiQL and key/type
        // errors must stay ValidationException with the correct `__type`,
        // so we do NOT blanket-remap them. A missing `Statement` field is
        // itself a ValidationException.
        let statement = require_str_with_code(&body, "Statement", "ValidationException")?;
        let parameters = body["Parameters"].as_array().cloned().unwrap_or_default();

        // Hold the write lock only long enough to mutate state and
        // capture the change capture record. Stream records are
        // appended under the write lock; Kinesis delivery happens
        // after the lock is released so the delivery bus (which may
        // take a read lock to look up the destination stream) doesn't
        // deadlock against us. This mirrors items.rs::put_item.
        let (response, pending_kinesis) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let region = state.region.clone();
            let outcome = execute_partiql_in_state(state, statement, &parameters)?;
            let response = outcome.response.clone();

            let kinesis_info = if let (Some(table_name), Some(event_name)) =
                (outcome.table_name.as_ref(), outcome.event_name.as_ref())
            {
                if let Some(table) = state.tables.get_mut(table_name) {
                    let keys = outcome.keys.clone().unwrap_or_default();
                    if table.stream_enabled {
                        if let Some(record) = crate::streams::generate_stream_record(
                            table,
                            event_name,
                            keys.clone(),
                            outcome.old_image.clone(),
                            outcome.new_image.clone(),
                            &region,
                        ) {
                            crate::streams::add_stream_record(table, record);
                        }
                    }
                    DynamoDbService::kinesis_target(table).map(|target| {
                        (
                            target,
                            event_name.clone(),
                            keys,
                            outcome.old_image,
                            outcome.new_image,
                        )
                    })
                } else {
                    None
                }
            } else {
                None
            };

            (response, kinesis_info)
        };

        if let Some((target, event_name, keys, old_image, new_image)) = pending_kinesis {
            self.deliver_to_kinesis_destinations(
                &target,
                &event_name,
                &keys,
                old_image.as_ref(),
                new_image.as_ref(),
            );
        }

        Self::ok_json(response)
    }

    pub(super) fn batch_execute_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_enum_value(
            "returnConsumedCapacity",
            &body["ReturnConsumedCapacity"],
            &["INDEXES", "TOTAL", "NONE"],
        )?;
        let statements = body["Statements"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Statements is required",
            )
        })?;

        let (responses, pending_kinesis) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let region = state.region.clone();
            let mut responses: Vec<Value> = Vec::with_capacity(statements.len());
            let mut pending_kinesis: Vec<PendingKinesis> = Vec::new();

            for stmt_obj in statements {
                let statement = stmt_obj["Statement"].as_str().unwrap_or_default();
                let parameters = stmt_obj["Parameters"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default();

                match execute_partiql_in_state(state, statement, &parameters) {
                    Ok(outcome) => {
                        responses.push(outcome.response.clone());
                        if let (Some(table_name), Some(event_name)) =
                            (outcome.table_name.as_ref(), outcome.event_name.as_ref())
                        {
                            if let Some(table) = state.tables.get_mut(table_name) {
                                let keys = outcome.keys.clone().unwrap_or_default();
                                if table.stream_enabled {
                                    if let Some(record) = crate::streams::generate_stream_record(
                                        table,
                                        event_name,
                                        keys.clone(),
                                        outcome.old_image.clone(),
                                        outcome.new_image.clone(),
                                        &region,
                                    ) {
                                        crate::streams::add_stream_record(table, record);
                                    }
                                }
                                if let Some(target) = DynamoDbService::kinesis_target(table) {
                                    pending_kinesis.push((
                                        target,
                                        event_name.clone(),
                                        keys,
                                        outcome.old_image,
                                        outcome.new_image,
                                    ));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        responses.push(json!({
                            "Error": {
                                "Code": "ValidationException",
                                "Message": e.to_string()
                            }
                        }));
                    }
                }
            }

            (responses, pending_kinesis)
        };

        for (target, event_name, keys, old_image, new_image) in pending_kinesis {
            self.deliver_to_kinesis_destinations(
                &target,
                &event_name,
                &keys,
                old_image.as_ref(),
                new_image.as_ref(),
            );
        }

        Self::ok_json(json!({ "Responses": responses }))
    }

    pub(super) fn execute_transaction(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length(
            "clientRequestToken",
            body["ClientRequestToken"].as_str(),
            1,
            36,
        )?;
        validate_optional_enum_value(
            "returnConsumedCapacity",
            &body["ReturnConsumedCapacity"],
            &["INDEXES", "TOTAL", "NONE"],
        )?;
        let transact_statements = body["TransactStatements"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TransactStatements is required",
            )
        })?;

        // Acquire the write lock once for the whole batch — DDB
        // ExecuteTransaction is all-or-nothing so we cannot release
        // the lock between phases or another writer could observe
        // partial state. Use the caller's account_id so cross-account
        // STS callers don't accidentally write to the default account.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let region = req.region.clone();

        // Phase 1: validate every statement against a cloned state.
        // Cloning the tables map (Vec<HashMap<...>> per table) is
        // cheap for typical transaction sizes (<=25 items per AWS
        // limits) and lets us collect a CancellationReason per
        // statement without mutating real state. Each clone-write
        // within this phase is discarded — we only keep the
        // per-statement reason so phase 2 can replay against the
        // real state.
        let mut clone_state = state.clone();

        let mut cancellation_reasons: Vec<Value> = Vec::with_capacity(transact_statements.len());
        let mut any_failed = false;

        for stmt_obj in transact_statements.iter() {
            let statement = stmt_obj["Statement"].as_str().unwrap_or_default();
            let parameters = stmt_obj["Parameters"]
                .as_array()
                .cloned()
                .unwrap_or_default();

            match execute_partiql_in_state(&mut clone_state, statement, &parameters) {
                Ok(_) => {
                    cancellation_reasons.push(json!({ "Code": "None" }));
                }
                Err(e) => {
                    any_failed = true;
                    let dbg = format!("{e:?}");
                    let code = if dbg.contains("ConditionalCheckFailed") {
                        "ConditionalCheckFailed"
                    } else if dbg.contains("DuplicateItemException") {
                        "DuplicateItem"
                    } else if dbg.contains("ResourceNotFoundException") {
                        "ResourceNotFound"
                    } else {
                        "ValidationError"
                    };
                    cancellation_reasons.push(json!({
                        "Code": code,
                        "Message": e.to_string(),
                    }));
                }
            }
        }

        if any_failed {
            // Build the dedup'd code list real DDB embeds in the
            // top-level message so SDKs that match on `[Code, ...]`
            // still work.
            let mut seen: Vec<String> = Vec::new();
            for r in &cancellation_reasons {
                if let Some(code) = r.get("Code").and_then(|c| c.as_str()) {
                    if code != "None" && !seen.iter().any(|s| s == code) {
                        seen.push(code.to_string());
                    }
                }
            }
            let codes_str = seen.join(", ");
            let error_body = json!({
                "__type": "TransactionCanceledException",
                "message": format!("Transaction cancelled, please refer cancellation reasons for specific reasons [{codes_str}]"),
                "CancellationReasons": cancellation_reasons,
            });
            return Ok(AwsResponse::json(
                StatusCode::BAD_REQUEST,
                serde_json::to_vec(&error_body).unwrap(),
            ));
        }

        // Phase 2: apply for real. We replay every statement against
        // the live tables map. By construction the validation pass
        // succeeded against the cloned state so this should not fail,
        // but if a statement does fail (defensive), we still revert
        // by snapshotting before we begin and restoring on error.
        let snapshot_tables = state.tables.clone();
        let mut pending_stream: Vec<(String, crate::state::StreamRecord)> = Vec::new();
        let mut pending_kinesis: Vec<PendingKinesis> = Vec::new();
        let mut apply_failure: Option<(usize, String)> = None;
        let mut applied_responses: Vec<Value> = Vec::with_capacity(transact_statements.len());

        for (i, stmt_obj) in transact_statements.iter().enumerate() {
            let statement = stmt_obj["Statement"].as_str().unwrap_or_default();
            let parameters = stmt_obj["Parameters"]
                .as_array()
                .cloned()
                .unwrap_or_default();

            match execute_partiql_in_state(state, statement, &parameters) {
                Ok(outcome) => {
                    applied_responses.push(outcome.response);
                    let table_name = match outcome.table_name {
                        Some(n) => n,
                        None => continue,
                    };
                    let event_name = match outcome.event_name {
                        Some(e) => e,
                        None => continue,
                    };
                    let keys = outcome.keys.unwrap_or_default();
                    if let Some(table) = state.tables.get(&table_name) {
                        if let Some(record) = crate::streams::generate_stream_record(
                            table,
                            &event_name,
                            keys.clone(),
                            outcome.old_image.clone(),
                            outcome.new_image.clone(),
                            &region,
                        ) {
                            pending_stream.push((table_name.clone(), record));
                        }
                        if let Some(target) = DynamoDbService::kinesis_target(table) {
                            pending_kinesis.push((
                                target,
                                event_name,
                                keys,
                                outcome.old_image,
                                outcome.new_image,
                            ));
                        }
                    }
                }
                Err(e) => {
                    apply_failure = Some((i, e.to_string()));
                    break;
                }
            }
        }

        if let Some((failed_idx, msg)) = apply_failure {
            // Revert to pre-apply snapshot — drops every partial
            // write from earlier statements in this transaction.
            state.tables = snapshot_tables;
            let reasons: Vec<Value> = (0..transact_statements.len())
                .map(|i| {
                    if i == failed_idx {
                        json!({
                            "Code": "ValidationError",
                            "Message": msg.clone(),
                        })
                    } else {
                        json!({ "Code": "None" })
                    }
                })
                .collect();
            let error_body = json!({
                "__type": "TransactionCanceledException",
                "message": "Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]",
                "CancellationReasons": reasons,
            });
            return Ok(AwsResponse::json(
                StatusCode::BAD_REQUEST,
                serde_json::to_vec(&error_body).unwrap(),
            ));
        }

        // Append pending stream records under each table's lock so
        // observers (DescribeStream/GetRecords) only see them once
        // the transaction has fully committed.
        for (table_name, record) in pending_stream {
            if let Some(table) = state.tables.get_mut(&table_name) {
                crate::streams::add_stream_record(table, record);
            }
        }

        // Drop the write lock before firing kinesis deliveries so
        // the delivery bus (which may take a read lock to look up
        // the target stream) doesn't deadlock against us.
        drop(accounts);
        for (target, event_name, keys, old_image, new_image) in pending_kinesis {
            self.deliver_to_kinesis_destinations(
                &target,
                &event_name,
                &keys,
                old_image.as_ref(),
                new_image.as_ref(),
            );
        }

        Self::ok_json(json!({ "Responses": applied_responses }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{DynamoTable, KeySchemaElement, ProvisionedThroughput, SharedDynamoDbState};
    use bytes::Bytes;
    use chrono::Utc;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn req_for(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "dynamodb".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "r".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn make_state() -> SharedDynamoDbState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn seed_table_with_stream(state: &SharedDynamoDbState, name: &str) {
        let mut accts = state.write();
        let s = accts.get_or_create("123456789012");
        let table = DynamoTable {
            name: name.to_string(),
            arn: format!("arn:aws:dynamodb:us-east-1:123456789012:table/{name}"),
            table_id: "id".to_string(),
            key_schema: vec![KeySchemaElement {
                attribute_name: "pk".into(),
                key_type: "HASH".into(),
            }],
            attribute_definitions: vec![],
            provisioned_throughput: ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            },
            items: vec![],
            gsi: vec![],
            lsi: vec![],
            tags: BTreeMap::new(),
            created_at: Utc::now(),
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: "PAY_PER_REQUEST".to_string(),
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: vec![],
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: BTreeMap::new(),
            stream_enabled: true,
            stream_view_type: Some("NEW_AND_OLD_IMAGES".to_string()),
            stream_arn: Some(format!(
                "arn:aws:dynamodb:us-east-1:123456789012:table/{name}/stream/lbl"
            )),
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,
            deletion_protection_enabled: false,
            on_demand_throughput: None,
            table_class: "STANDARD".to_string(),
        };
        s.tables.insert(name.to_string(), table);
    }

    /// 1.11/1.12: BatchGetItem must honor the per-table
    /// ProjectionExpression / AttributesToGet instead of returning the
    /// whole stored item.
    #[tokio::test]
    async fn batch_get_item_honors_projection_and_legacy_attributes_to_get() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        svc.batch_write_item(&req_for(
            "BatchWriteItem",
            json!({"RequestItems": {"Widgets": [
                {"PutRequest": {"Item": {"pk": {"S": "a"}, "x": {"S": "1"}, "y": {"S": "2"}}}},
            ]}}),
        ))
        .unwrap();

        // ProjectionExpression
        let resp = svc
            .batch_get_item(&req_for(
                "BatchGetItem",
                json!({"RequestItems": {"Widgets": {
                    "Keys": [{"pk": {"S": "a"}}],
                    "ProjectionExpression": "x",
                }}}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let item = &body["Responses"]["Widgets"][0];
        assert!(item.get("x").is_some());
        assert!(item.get("y").is_none(), "projection must drop y");
        assert!(item.get("pk").is_none(), "projection only returns x");

        // Legacy AttributesToGet
        let resp = svc
            .batch_get_item(&req_for(
                "BatchGetItem",
                json!({"RequestItems": {"Widgets": {
                    "Keys": [{"pk": {"S": "a"}}],
                    "AttributesToGet": ["pk", "y"],
                }}}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let item = &body["Responses"]["Widgets"][0];
        assert!(item.get("pk").is_some());
        assert!(item.get("y").is_some());
        assert!(item.get("x").is_none(), "AttributesToGet must drop x");
    }

    /// 1.14: BatchGetItem must reject >100 keys.
    #[tokio::test]
    async fn batch_get_item_rejects_over_100_keys() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);
        let keys: Vec<Value> = (0..101)
            .map(|i| json!({"pk": {"S": i.to_string()}}))
            .collect();
        let err = svc
            .batch_get_item(&req_for(
                "BatchGetItem",
                json!({"RequestItems": {"Widgets": {"Keys": keys}}}),
            ))
            .err()
            .expect("over-100 batch rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
    }

    /// 1.14: BatchWriteItem must reject >25 requests.
    #[tokio::test]
    async fn batch_write_item_rejects_over_25_requests() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);
        let reqs: Vec<Value> = (0..26)
            .map(|i| json!({"PutRequest": {"Item": {"pk": {"S": i.to_string()}}}}))
            .collect();
        let err = svc
            .batch_write_item(&req_for(
                "BatchWriteItem",
                json!({"RequestItems": {"Widgets": reqs}}),
            ))
            .err()
            .expect("over-25 batch rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
    }

    /// 1.14: BatchWriteItem must reject duplicate keys within one batch.
    #[tokio::test]
    async fn batch_write_item_rejects_duplicate_keys() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);
        let err = svc
            .batch_write_item(&req_for(
                "BatchWriteItem",
                json!({"RequestItems": {"Widgets": [
                    {"PutRequest": {"Item": {"pk": {"S": "a"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "a"}}}},
                ]}}),
            ))
            .err()
            .expect("duplicate key rejected");
        assert!(format!("{err:?}").contains("duplicates"));
    }

    /// 1.14: BatchWriteItem must reject keyless items instead of coercing
    /// them to `{}` and writing them.
    #[tokio::test]
    async fn batch_write_item_rejects_keyless_item() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        let err = svc
            .batch_write_item(&req_for(
                "BatchWriteItem",
                json!({"RequestItems": {"Widgets": [
                    {"PutRequest": {"Item": {"notthekey": {"S": "x"}}}},
                ]}}),
            ))
            .err()
            .expect("keyless item rejected");
        assert!(format!("{err:?}").contains("Missing the key pk"));
        // Nothing should have been written.
        let accts = state.read();
        let table = accts
            .get("123456789012")
            .unwrap()
            .tables
            .get("Widgets")
            .unwrap();
        assert_eq!(table.items.len(), 0);
    }

    /// 1.26: ExecuteStatement must keep a genuine PartiQL
    /// ValidationException as ValidationException (not remap to
    /// ResourceNotFoundException) while still mapping a missing table to
    /// ResourceNotFoundException.
    #[tokio::test]
    async fn execute_statement_preserves_validation_vs_not_found() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);

        // Malformed PartiQL -> ValidationException.
        let err = svc
            .execute_statement(&req_for(
                "ExecuteStatement",
                json!({"Statement": "BOGUS NOT A REAL PARTIQL STATEMENT"}),
            ))
            .err()
            .expect("malformed partiql");
        assert!(
            format!("{err:?}").contains("ValidationException"),
            "malformed PartiQL must stay ValidationException, got {err:?}"
        );

        // Missing table -> ResourceNotFoundException.
        let err = svc
            .execute_statement(&req_for(
                "ExecuteStatement",
                json!({"Statement": "SELECT * FROM \"Nope\""}),
            ))
            .err()
            .expect("missing table");
        assert!(
            format!("{err:?}").contains("ResourceNotFoundException"),
            "missing table must be ResourceNotFoundException, got {err:?}"
        );
    }

    #[tokio::test]
    async fn transact_write_emits_stream_records_per_write() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        let req = req_for(
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}}}},
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "b"}}}},
                ]
            }),
        );
        svc.transact_write_items(&req).unwrap();

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        let records = table.stream_records.read();
        assert_eq!(records.len(), 2, "one stream record per Put");
        assert!(records.iter().all(|r| r.event_name == "INSERT"));
    }

    #[tokio::test]
    async fn transact_write_unknown_table_rejects_atomically() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        let req = req_for(
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}}}},
                    {"Put": {"TableName": "Missing", "Item": {"pk": {"S": "b"}}}},
                ]
            }),
        );
        let _ = svc.transact_write_items(&req);

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        assert_eq!(
            table.items.len(),
            0,
            "the Put on Widgets must not commit when a sibling table is missing"
        );
    }

    #[tokio::test]
    async fn transact_write_condition_failure_returns_old_item_when_requested() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        // Seed an existing item so attribute_not_exists fails.
        svc.transact_write_items(&req_for(
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}, "v": {"S": "old"}}}},
                ]
            }),
        ))
        .unwrap();

        // Now attempt a Put with an attribute_not_exists guard that
        // will fail. ALL_OLD asks the service to surface the existing
        // item back through the cancellation reason.
        let resp = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({
                    "TransactItems": [
                        {"Put": {
                            "TableName": "Widgets",
                            "Item": {"pk": {"S": "a"}, "v": {"S": "new"}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                            "ReturnValuesOnConditionCheckFailure": "ALL_OLD"
                        }},
                    ]
                }),
            ))
            .unwrap();
        assert_eq!(resp.status, http::StatusCode::BAD_REQUEST);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["__type"].as_str().unwrap(),
            "TransactionCanceledException"
        );
        let reasons = body["CancellationReasons"].as_array().unwrap();
        assert_eq!(reasons.len(), 1);
        assert_eq!(
            reasons[0]["Code"].as_str().unwrap(),
            "ConditionalCheckFailed"
        );
        let surfaced = reasons[0]["Item"].as_object().expect("Item attached");
        assert_eq!(surfaced["v"]["S"].as_str().unwrap(), "old");
    }

    #[tokio::test]
    async fn transact_write_condition_failure_omits_old_item_when_not_requested() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        svc.transact_write_items(&req_for(
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}, "v": {"S": "old"}}}},
                ]
            }),
        ))
        .unwrap();

        let resp = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({
                    "TransactItems": [
                        {"Put": {
                            "TableName": "Widgets",
                            "Item": {"pk": {"S": "a"}, "v": {"S": "new"}},
                            "ConditionExpression": "attribute_not_exists(pk)",
                        }},
                    ]
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let reasons = body["CancellationReasons"].as_array().unwrap();
        assert!(
            reasons[0].get("Item").is_none(),
            "default ReturnValuesOnConditionCheckFailure=NONE must omit the Item field"
        );
    }

    #[tokio::test]
    async fn transact_write_per_op_cancellation_reasons_align_to_index() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        // Seed two items.
        svc.transact_write_items(&req_for(
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}}}},
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "b"}}}},
                ]
            }),
        ))
        .unwrap();

        // Three ops: succeed, fail, succeed. We expect three reasons,
        // index-aligned. After cancel, the surrounding successful Puts
        // must NOT have committed.
        let resp = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({
                    "TransactItems": [
                        {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "c"}}}},
                        {"ConditionCheck": {
                            "TableName": "Widgets",
                            "Key": {"pk": {"S": "missing"}},
                            "ConditionExpression": "attribute_exists(pk)"
                        }},
                        {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "d"}}}},
                    ]
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let reasons = body["CancellationReasons"].as_array().unwrap();
        assert_eq!(reasons.len(), 3);
        assert_eq!(reasons[0]["Code"].as_str().unwrap(), "None");
        assert_eq!(
            reasons[1]["Code"].as_str().unwrap(),
            "ConditionalCheckFailed"
        );
        assert_eq!(reasons[2]["Code"].as_str().unwrap(), "None");

        let accts = state.read();
        let table = accts
            .get("123456789012")
            .unwrap()
            .tables
            .get("Widgets")
            .unwrap();
        let pks: Vec<String> = table
            .items
            .iter()
            .map(|i| i["pk"]["S"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(pks, vec!["a".to_string(), "b".to_string()]);
    }

    #[tokio::test]
    async fn transact_write_apply_failure_reverts_and_emits_validation_error() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        // First op is a valid Put; second op carries a malformed
        // UpdateExpression so the apply-phase fails. The Put before it
        // must be reverted.
        let resp = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({
                    "TransactItems": [
                        {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}}}},
                        {"Update": {
                            "TableName": "Widgets",
                            "Key": {"pk": {"S": "a"}},
                            "UpdateExpression": "BOGUS expression that won't parse"
                        }},
                    ]
                }),
            ))
            .unwrap();
        assert_eq!(resp.status, http::StatusCode::BAD_REQUEST);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["__type"].as_str().unwrap(),
            "TransactionCanceledException"
        );
        let reasons = body["CancellationReasons"].as_array().unwrap();
        assert_eq!(reasons.len(), 2);
        assert_eq!(reasons[0]["Code"].as_str().unwrap(), "None");
        assert_eq!(reasons[1]["Code"].as_str().unwrap(), "ValidationError");

        // Confirm revert: the Put on index 0 must NOT have committed.
        let accts = state.read();
        let table = accts
            .get("123456789012")
            .unwrap()
            .tables
            .get("Widgets")
            .unwrap();
        assert_eq!(
            table.items.len(),
            0,
            "apply-phase failure must revert earlier writes"
        );
        // No stream record should have been emitted either.
        assert_eq!(table.stream_records.read().len(), 0);
    }

    #[tokio::test]
    async fn execute_transaction_emits_stream_record_per_write() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        let req = req_for(
            "ExecuteTransaction",
            json!({
                "TransactStatements": [
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}"},
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'b'}"},
                ]
            }),
        );
        let resp = svc.execute_transaction(&req).unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        assert_eq!(table.items.len(), 2);
        assert_eq!(
            table.stream_records.read().len(),
            2,
            "each PartiQL INSERT must emit one stream record"
        );
    }

    #[tokio::test]
    async fn execute_statement_insert_emits_stream_record() {
        // L4: a single ExecuteStatement INSERT (not via Transaction) on
        // a stream-enabled table must emit a Stream record so log/CDC
        // consumers see the same event they would for a PutItem call.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}"}),
        ))
        .unwrap();

        let accts = state.read();
        let table = accts
            .get("123456789012")
            .unwrap()
            .tables
            .get("Widgets")
            .unwrap();
        assert_eq!(table.items.len(), 1);
        let records = table.stream_records.read();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].event_name, "INSERT");
    }

    #[tokio::test]
    async fn batch_execute_statement_emits_stream_record_per_write() {
        // L4: each statement in a BatchExecuteStatement that succeeds
        // and mutates the table must emit its own Stream record.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        svc.batch_execute_statement(&req_for(
            "BatchExecuteStatement",
            json!({
                "Statements": [
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}"},
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'b'}"},
                ]
            }),
        ))
        .unwrap();

        let accts = state.read();
        let table = accts
            .get("123456789012")
            .unwrap()
            .tables
            .get("Widgets")
            .unwrap();
        assert_eq!(table.items.len(), 2);
        assert_eq!(table.stream_records.read().len(), 2);
    }

    #[tokio::test]
    async fn partiql_insert_rejects_missing_key_attribute() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        let req = req_for(
            "ExecuteStatement",
            json!({
                "Statement": "INSERT INTO \"Widgets\" VALUE {'other': 'a'}",
            }),
        );
        let err = svc.execute_statement(&req).err().expect("missing key");
        assert!(format!("{err:?}").contains("Missing the key pk"));
    }

    #[tokio::test]
    async fn partiql_select_isolated_per_account() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        // Insert into the default account.
        let svc = DynamoDbService::new(state.clone());
        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({
                "Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}",
            }),
        ))
        .unwrap();

        // Foreign account selecting the same table sees an empty
        // namespace (the table isn't created on demand for SELECT).
        let mut foreign = req_for(
            "ExecuteStatement",
            json!({
                "Statement": "SELECT * FROM \"Widgets\"",
            }),
        );
        foreign.account_id = "999999999999".into();
        let err = svc.execute_statement(&foreign).err().expect("not found");
        assert!(format!("{err:?}").contains("ResourceNotFoundException"));
    }

    #[tokio::test]
    async fn partiql_select_with_comparator_filters() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        for v in ["a", "b", "c"] {
            svc.execute_statement(&req_for(
                "ExecuteStatement",
                json!({
                    "Statement": format!("INSERT INTO \"Widgets\" VALUE {{'pk': '{v}'}}"),
                }),
            ))
            .unwrap();
        }
        let resp = svc
            .execute_statement(&req_for(
                "ExecuteStatement",
                json!({
                    "Statement": "SELECT * FROM \"Widgets\" WHERE pk > 'a'",
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["Items"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn execute_transaction_reverts_on_mid_batch_failure() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        // First INSERT succeeds, second targets a missing table.
        let req = req_for(
            "ExecuteTransaction",
            json!({
                "TransactStatements": [
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}"},
                    {"Statement": "INSERT INTO \"Missing\" VALUE {'pk': 'b'}"},
                ]
            }),
        );
        let resp = svc.execute_transaction(&req).unwrap();
        assert_eq!(resp.status, http::StatusCode::BAD_REQUEST);

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        assert_eq!(
            table.items.len(),
            0,
            "first INSERT must be reverted when the second statement fails"
        );
    }

    #[tokio::test]
    async fn execute_transaction_three_writes_middle_fails_reverts_all() {
        // L3 spec: 3 writes where #2 fails (duplicate-key on a seeded
        // item) — all 3 must be reverted, no stream records emitted,
        // CancellationReasons array length 3 with #2 marked.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        // Pre-seed pk=b so the 2nd INSERT in the transaction collides.
        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'b'}"}),
        ))
        .unwrap();
        // Reset stream records so we only count what the txn emits.
        {
            let accts = state.read();
            let s = accts.get("123456789012").unwrap();
            let table = s.tables.get("Widgets").unwrap();
            table.stream_records.write().clear();
        }

        let req = req_for(
            "ExecuteTransaction",
            json!({
                "TransactStatements": [
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}"},
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'b'}"}, // dup
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'c'}"},
                ]
            }),
        );
        let resp = svc.execute_transaction(&req).unwrap();
        assert_eq!(resp.status, http::StatusCode::BAD_REQUEST);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            body["__type"].as_str().unwrap(),
            "TransactionCanceledException"
        );
        let reasons = body["CancellationReasons"].as_array().unwrap();
        assert_eq!(reasons.len(), 3, "one CancellationReason per statement");
        assert_eq!(reasons[0]["Code"].as_str().unwrap(), "None");
        assert_eq!(reasons[1]["Code"].as_str().unwrap(), "DuplicateItem");
        assert_eq!(reasons[2]["Code"].as_str().unwrap(), "None");

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        // Only the pre-seed should remain — neither 'a' nor 'c' from
        // the rolled-back txn must persist.
        let pks: Vec<String> = table
            .items
            .iter()
            .map(|i| i["pk"]["S"].as_str().unwrap_or_default().to_string())
            .collect();
        assert_eq!(pks, vec!["b".to_string()], "all 3 statements reverted");
        // No stream records should have been emitted from the failed
        // txn — the apply phase never ran.
        assert_eq!(
            table.stream_records.read().len(),
            0,
            "no stream records on failed txn"
        );
    }

    #[tokio::test]
    async fn execute_transaction_happy_path_commits_and_emits_per_write() {
        // L3 spec: happy-path commits all + each write emits a stream
        // record. Mirrors items.rs::put_item per-write hook semantics.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        let req = req_for(
            "ExecuteTransaction",
            json!({
                "TransactStatements": [
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}"},
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'b'}"},
                    {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'c'}"},
                ]
            }),
        );
        let resp = svc.execute_transaction(&req).unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["Responses"].as_array().unwrap().len(), 3);

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        assert_eq!(table.items.len(), 3);
        let records = table.stream_records.read();
        assert_eq!(records.len(), 3, "one stream record per write");
        assert!(records.iter().all(|r| r.event_name == "INSERT"));
    }
}
