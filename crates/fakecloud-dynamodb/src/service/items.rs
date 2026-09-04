use std::collections::HashMap;

use http::StatusCode;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{
    apply_update_expression, build_consumed_capacity, build_item_collection_metrics,
    evaluate_condition_with_return, extract_key, get_table, get_table_mut, ns_members_equal,
    parse_expression_attribute_names, parse_expression_attribute_values, project_item,
    require_object, require_str, resolve_write_condition, return_consumed_mode, return_icm_mode,
    validate_attribute_value, validate_item_attribute_values, validate_key_attributes_in_key,
    validate_key_in_item, AttributeValue, DynamoDbService,
};

impl DynamoDbService {
    pub(super) fn put_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // --- Parse request body and expression attributes WITHOUT holding any lock ---
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let item = require_object(&body, "Item")?;
        let mut expr_attr_names = parse_expression_attribute_names(&body);
        let mut expr_attr_values = parse_expression_attribute_values(&body);
        let condition =
            resolve_write_condition(&body, &mut expr_attr_names, &mut expr_attr_values)?;
        let return_values = body["ReturnValues"].as_str().unwrap_or("NONE").to_string();
        let return_values_on_failure = body["ReturnValuesOnConditionCheckFailure"]
            .as_str()
            .map(String::from);
        let return_consumed = return_consumed_mode(&body).to_string();
        let return_icm = return_icm_mode(&body).to_string();

        // --- Acquire write lock ONLY for validation + mutation ---
        // Capture kinesis delivery info alongside the return value
        let (old_item, kinesis_info, kms_audit, icm) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let region = state.region.clone();
            let table = get_table_mut(&mut state.tables, table_name)?;

            validate_key_in_item(table, &item)?;
            // Validate every attribute value (not just keys): a malformed number
            // like {"N":"abc"} is a ValidationException in real DynamoDB.
            validate_item_attribute_values(&item)?;

            let key = extract_key(table, &item);
            table.ensure_key_index();
            let existing_idx = table.find_item_index(&key);

            if let Some(cond) = condition.as_deref() {
                let existing = existing_idx.map(|i| &table.items[i]);
                evaluate_condition_with_return(
                    cond,
                    existing,
                    &expr_attr_names,
                    &expr_attr_values,
                    return_values_on_failure.as_deref(),
                )?;
            }

            let old_item_for_return = if return_values == "ALL_OLD" {
                existing_idx.map(|i| table.items[i].clone())
            } else {
                None
            };

            // Capture old item for stream/kinesis if needed
            let needs_change_capture = table.stream_enabled
                || table
                    .kinesis_destinations
                    .iter()
                    .any(|d| d.destination_status == "ACTIVE");
            let old_item_for_stream = if needs_change_capture {
                existing_idx.map(|i| table.items[i].clone())
            } else {
                None
            };

            let is_modify = existing_idx.is_some();

            // Maintains item_count, size_bytes and the key index incrementally
            // rather than re-summing the whole table (#2502).
            table.put_item_at_key(item.clone());

            table.record_item_access(&item);

            let event_name = if is_modify { "MODIFY" } else { "INSERT" };
            let key = extract_key(table, &item);

            // Generate stream record
            if table.stream_enabled {
                if let Some(record) = crate::streams::generate_stream_record(
                    table,
                    event_name,
                    key.clone(),
                    old_item_for_stream.clone(),
                    Some(item.clone()),
                    &region,
                ) {
                    crate::streams::add_stream_record(table, record);
                }
            }

            // Capture kinesis delivery info (delivered after lock release)
            let kinesis_info = DynamoDbService::kinesis_target(table).map(|target| {
                (
                    target,
                    event_name.to_string(),
                    key.clone(),
                    old_item_for_stream,
                    Some(item.clone()),
                )
            });

            // Snapshot KMS-audit inputs while we still hold the table
            // borrow, then emit the records below the lock.
            let kms_audit = if table.sse_type.as_deref() == Some("KMS") {
                Some((table.arn.clone(), table.sse_kms_key_arn.clone()))
            } else {
                None
            };

            let icm = build_item_collection_metrics(&return_icm, table, &key);

            (old_item_for_return, kinesis_info, kms_audit, icm)
        };
        // --- Write lock released, build response ---

        if let Some((arn, key_arn)) = kms_audit {
            self.record_table_kms_usage(
                &req.account_id,
                &arn,
                key_arn.as_deref(),
                super::TableKmsOp::Write,
            );
        }

        // Deliver to Kinesis destinations outside the lock
        if let Some((target, event_name, keys, old_image, new_image)) = kinesis_info {
            self.deliver_to_kinesis_destinations(
                &target,
                &event_name,
                &keys,
                old_image.as_ref(),
                new_image.as_ref(),
            );
        }

        let mut result = json!({});
        if let Some(old) = old_item {
            result["Attributes"] = json!(old);
        }
        let cc = build_consumed_capacity(&return_consumed, table_name, 0.0, 1.0);
        if !cc.is_null() {
            result["ConsumedCapacity"] = cc;
        }
        if !icm.is_null() {
            result["ItemCollectionMetrics"] = icm;
        }

        Self::ok_json(result)
    }

    pub(super) fn get_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // --- Parse request body WITHOUT holding any lock ---
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let key = require_object(&body, "Key")?;
        let return_consumed = return_consumed_mode(&body).to_string();

        // --- Use a read lock for the lookup (allows concurrent GetItem calls) ---
        let (result, needs_insights, kms_audit) = {
            let accounts = self.state.read();
            let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
            let table = get_table(&state.tables, table_name)?;
            validate_key_attributes_in_key(table, &key)?;
            let needs_insights = table.contributor_insights_status == "ENABLED";

            let mut result = match table.find_item_index(&key) {
                Some(idx) => {
                    let item = &table.items[idx];
                    let projected = project_item(item, &body);
                    json!({ "Item": projected })
                }
                None => json!({}),
            };
            let cc = build_consumed_capacity(&return_consumed, table_name, 0.5, 0.0);
            if !cc.is_null() {
                result["ConsumedCapacity"] = cc;
            }
            let kms_audit = if table.sse_type.as_deref() == Some("KMS") {
                Some((table.arn.clone(), table.sse_kms_key_arn.clone()))
            } else {
                None
            };
            (result, needs_insights, kms_audit)
        };
        // --- Read lock released ---

        if let Some((arn, key_arn)) = kms_audit {
            self.record_table_kms_usage(
                &req.account_id,
                &arn,
                key_arn.as_deref(),
                super::TableKmsOp::Read,
            );
        }

        // Only acquire write lock if contributor insights tracking is enabled
        if needs_insights {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            if let Some(table) = state.tables.get_mut(table_name) {
                table.record_key_access(&key);
            }
        }

        Self::ok_json(result)
    }

    pub(super) fn delete_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;

        validate_optional_enum_value(
            "conditionalOperator",
            &body["ConditionalOperator"],
            &["AND", "OR"],
        )?;
        validate_optional_enum_value(
            "returnConsumedCapacity",
            &body["ReturnConsumedCapacity"],
            &["INDEXES", "TOTAL", "NONE"],
        )?;
        validate_optional_enum_value(
            "returnValues",
            &body["ReturnValues"],
            &["NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW"],
        )?;
        validate_optional_enum_value(
            "returnItemCollectionMetrics",
            &body["ReturnItemCollectionMetrics"],
            &["SIZE", "NONE"],
        )?;
        validate_optional_enum_value(
            "returnValuesOnConditionCheckFailure",
            &body["ReturnValuesOnConditionCheckFailure"],
            &["ALL_OLD", "NONE"],
        )?;

        let table_name = require_str(&body, "TableName")?;
        let key = require_object(&body, "Key")?;

        let (result, kinesis_info) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let region = state.region.clone();
            let table = get_table_mut(&mut state.tables, table_name)?;

            let mut expr_attr_names = parse_expression_attribute_names(&body);
            let mut expr_attr_values = parse_expression_attribute_values(&body);
            let condition =
                resolve_write_condition(&body, &mut expr_attr_names, &mut expr_attr_values)?;

            table.ensure_key_index();
            let existing_idx = table.find_item_index(&key);

            if let Some(cond) = condition.as_deref() {
                let existing = existing_idx.map(|i| &table.items[i]);
                evaluate_condition_with_return(
                    cond,
                    existing,
                    &expr_attr_names,
                    &expr_attr_values,
                    body["ReturnValuesOnConditionCheckFailure"].as_str(),
                )?;
            }

            let return_values = body["ReturnValues"].as_str().unwrap_or("NONE");

            let mut result = json!({});
            let mut kinesis_info = None;

            if let Some(idx) = existing_idx {
                let old_item = table.items[idx].clone();
                if return_values == "ALL_OLD" {
                    result["Attributes"] = json!(old_item.clone());
                }

                // Generate stream record before removing
                if table.stream_enabled {
                    if let Some(record) = crate::streams::generate_stream_record(
                        table,
                        "REMOVE",
                        key.clone(),
                        Some(old_item.clone()),
                        None,
                        &region,
                    ) {
                        crate::streams::add_stream_record(table, record);
                    }
                }

                // Capture kinesis delivery info
                if let Some(target) = DynamoDbService::kinesis_target(table) {
                    kinesis_info = Some((target, key.clone(), Some(old_item)));
                }

                table.remove_item_by_key(&key);
            }

            let return_consumed = body["ReturnConsumedCapacity"].as_str().unwrap_or("NONE");
            let return_icm = body["ReturnItemCollectionMetrics"]
                .as_str()
                .unwrap_or("NONE");

            let cc = build_consumed_capacity(return_consumed, table_name, 0.0, 1.0);
            if !cc.is_null() {
                result["ConsumedCapacity"] = cc;
            }

            let icm = build_item_collection_metrics(return_icm, table, &key);
            if !icm.is_null() {
                result["ItemCollectionMetrics"] = icm;
            }

            (result, kinesis_info)
        };

        // Deliver to Kinesis destinations outside the lock
        if let Some((target, keys, old_image)) = kinesis_info {
            self.deliver_to_kinesis_destinations(
                &target,
                "REMOVE",
                &keys,
                old_image.as_ref(),
                None,
            );
        }

        Self::ok_json(result)
    }

    pub(super) fn update_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let key = require_object(&body, "Key")?;
        let return_consumed = return_consumed_mode(&body).to_string();
        let return_icm = return_icm_mode(&body).to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let region = state.region.clone();
        let table = get_table_mut(&mut state.tables, table_name)?;

        validate_key_attributes_in_key(table, &key)?;
        // Build the index up front: the `&self` lookup below cannot, so
        // without this an UpdateItem-only workload against a restored table
        // would scan forever.
        table.ensure_key_index();

        let mut expr_attr_names = parse_expression_attribute_names(&body);
        let mut expr_attr_values = parse_expression_attribute_values(&body);
        let condition =
            resolve_write_condition(&body, &mut expr_attr_names, &mut expr_attr_values)?;
        let update_expression = body["UpdateExpression"].as_str();

        // Validate the attribute values an UpdateExpression / AttributeUpdates
        // will write (empty or duplicate SS/BS/NS, malformed numbers) before
        // they corrupt the item — matches PutItem's item validation.
        for v in expr_attr_values.values() {
            validate_attribute_value(v)?;
        }
        if let Some(updates) = body["AttributeUpdates"].as_object() {
            for upd in updates.values() {
                if let Some(v) = upd.get("Value") {
                    validate_attribute_value(v)?;
                }
            }
        }

        let existing_idx = table.find_item_index(&key);

        if let Some(cond) = condition.as_deref() {
            let existing = existing_idx.map(|i| &table.items[i]);
            evaluate_condition_with_return(
                cond,
                existing,
                &expr_attr_names,
                &expr_attr_values,
                body["ReturnValuesOnConditionCheckFailure"].as_str(),
            )?;
        }

        let return_values = body["ReturnValues"].as_str().unwrap_or("NONE");

        let is_insert = existing_idx.is_none();
        let idx = match existing_idx {
            Some(i) => i,
            None => {
                let mut new_item = HashMap::new();
                for (k, v) in &key {
                    new_item.insert(k.clone(), v.clone());
                }
                // Registers the row in the key index and the stats; the
                // attribute updates below then mutate it in place, and the
                // deltas for that are settled by `sync_item_at`.
                table.put_item_at_key(new_item).0
            }
        };

        // What this row contributes before the update is applied; paired with
        // `sync_item_at` after it to keep size_bytes and the key index exact.
        let slot_before = table.snapshot_item_at(idx);

        // Capture old item for stream/kinesis (before update)
        let needs_change_capture = table.stream_enabled
            || table
                .kinesis_destinations
                .iter()
                .any(|d| d.destination_status == "ACTIVE");
        let old_item_for_stream = if needs_change_capture {
            Some(table.items[idx].clone())
        } else {
            None
        };

        // Snapshot the pre-update item when the requested ReturnValues
        // needs it: ALL_OLD returns it whole, and UPDATED_NEW/UPDATED_OLD
        // need it to diff against the post-update item so they can return
        // ONLY the changed attributes (bug-audit 2026-05-28, 1.8 — we used
        // to return the whole item for UPDATED_NEW and nothing for
        // UPDATED_OLD).
        let pre_update_item = if matches!(return_values, "ALL_OLD" | "UPDATED_OLD" | "UPDATED_NEW")
        {
            Some(table.items[idx].clone())
        } else {
            None
        };

        // An UpdateExpression is applied clause by clause and can fail partway
        // (a type error on a later operand), leaving the row already rewritten
        // -- possibly under a different key. Settle the cached size and the
        // index against whatever landed before propagating, or a later write
        // addresses the row under a key it no longer has.
        let applied = if let Some(expr) = update_expression {
            apply_update_expression(
                &mut table.items[idx],
                expr,
                &expr_attr_names,
                &expr_attr_values,
            )
        } else if let Some(updates) = body["AttributeUpdates"].as_object() {
            // Legacy AttributeUpdates (pre-2014 UpdateItem), still emitted by
            // the AWS SDK for Java v1, older boto3, and the Terraform provider.
            // Without this an UpdateItem using AttributeUpdates wrote nothing and
            // (on a missing key) left a key-only stub item -- silent data loss
            // (bug-audit 2026-06-20, 1.2).
            apply_attribute_updates(&mut table.items[idx], updates)
        } else {
            Ok(())
        };
        if let Err(err) = applied {
            table.sync_item_at(idx, slot_before);
            return Err(err);
        }

        // Compute the ReturnValues payload per AWS semantics:
        // - ALL_NEW   : the whole post-update item
        // - ALL_OLD   : the whole pre-update item
        // - UPDATED_NEW: only attributes that changed, with NEW values
        // - UPDATED_OLD: only attributes that changed, with OLD values
        // - NONE      : nothing
        // On an upsert-insert there is no pre-existing item, so the OLD-value
        // return modes yield nothing — not the key-only stub we just pushed.
        // AWS returns no Attributes for ALL_OLD/UPDATED_OLD on an insert
        // (bug-hunt 2026-07-01, DynamoDB ALL_OLD-on-insert).
        let old_snapshot = if is_insert {
            None
        } else {
            pre_update_item.as_ref()
        };
        let response_attributes: Option<HashMap<String, AttributeValue>> = match return_values {
            "ALL_NEW" => Some(table.items[idx].clone()),
            "ALL_OLD" => old_snapshot.cloned(),
            // UPDATED_NEW diffs against the pre-update image (the key-only stub
            // on insert) so it returns only the attributes the update set.
            "UPDATED_NEW" => Some(diff_updated_attributes(
                pre_update_item.as_ref(),
                &table.items[idx],
                UpdatedSide::New,
            )),
            "UPDATED_OLD" => Some(diff_updated_attributes(
                old_snapshot,
                &table.items[idx],
                UpdatedSide::Old,
            )),
            _ => None,
        };

        let event_name = if is_insert { "INSERT" } else { "MODIFY" };
        let new_item_for_stream = table.items[idx].clone();

        // Generate stream record after update
        if table.stream_enabled {
            if let Some(record) = crate::streams::generate_stream_record(
                table,
                event_name,
                key.clone(),
                old_item_for_stream.clone(),
                Some(new_item_for_stream.clone()),
                &region,
            ) {
                crate::streams::add_stream_record(table, record);
            }
        }

        // Capture kinesis delivery info
        let kinesis_info = DynamoDbService::kinesis_target(table).map(|target| {
            (
                target,
                event_name.to_string(),
                key.clone(),
                old_item_for_stream,
                Some(new_item_for_stream),
            )
        });

        // The update rewrote the item at `idx` in place; only that row's
        // contribution to size_bytes and its indexed key can have changed, so
        // settle those rather than re-summing the whole table (#2502).
        table.sync_item_at(idx, slot_before);

        let icm = build_item_collection_metrics(&return_icm, table, &key);

        // Release the write lock (drop `state`)
        drop(accounts);

        // Deliver to Kinesis destinations outside the lock
        if let Some((target, ev, keys, old_image, new_image)) = kinesis_info {
            self.deliver_to_kinesis_destinations(
                &target,
                &ev,
                &keys,
                old_image.as_ref(),
                new_image.as_ref(),
            );
        }

        let mut result = json!({});
        if let Some(attrs) = response_attributes {
            // UPDATED_NEW/UPDATED_OLD with no changed attributes still
            // omit `Attributes` entirely, matching AWS.
            if !attrs.is_empty() {
                result["Attributes"] = json!(attrs);
            }
        }
        let cc = build_consumed_capacity(&return_consumed, table_name, 0.0, 1.0);
        if !cc.is_null() {
            result["ConsumedCapacity"] = cc;
        }
        if !icm.is_null() {
            result["ItemCollectionMetrics"] = icm;
        }

        Self::ok_json(result)
    }
}

/// Which side of an UpdateItem diff to return for the `UPDATED_*`
/// `ReturnValues` modes.
enum UpdatedSide {
    /// `UPDATED_NEW`: the post-update value of each changed attribute.
    New,
    /// `UPDATED_OLD`: the pre-update value of each changed attribute.
    Old,
}

/// Compute the `UPDATED_NEW` / `UPDATED_OLD` ReturnValues payload: the set
/// of attributes whose value differs between the pre- and post-update item,
/// projected to the requested side. AWS returns only the attributes the
/// update touched — added, removed, or modified — not the whole item.
///
/// - An attribute present after but not before (or with a changed value) is
///   "changed". For `New` we emit its post value; for `Old` its pre value
///   (absent on the old side -> omitted).
/// - An attribute removed by the update is "changed". For `Old` we emit its
///   pre value; for `New` it's gone, so it's omitted.
///
/// `pre` is `None` only when the item didn't exist before (pure insert), in
/// which case every post attribute is "new".
fn diff_updated_attributes(
    pre: Option<&HashMap<String, AttributeValue>>,
    post: &HashMap<String, AttributeValue>,
    side: UpdatedSide,
) -> HashMap<String, AttributeValue> {
    let empty = HashMap::new();
    let pre = pre.unwrap_or(&empty);
    let mut out = HashMap::new();

    // Attributes present (or changed) in the post item.
    for (k, new_v) in post {
        let changed = match pre.get(k) {
            Some(old_v) => old_v != new_v,
            None => true,
        };
        if changed {
            match side {
                UpdatedSide::New => {
                    out.insert(k.clone(), new_v.clone());
                }
                UpdatedSide::Old => {
                    if let Some(old_v) = pre.get(k) {
                        out.insert(k.clone(), old_v.clone());
                    }
                }
            }
        }
    }

    // Attributes removed by the update (present before, gone after). Only
    // the OLD side can surface their value; the NEW side has nothing.
    if let UpdatedSide::Old = side {
        for (k, old_v) in pre {
            if !post.contains_key(k) {
                out.insert(k.clone(), old_v.clone());
            }
        }
    }

    out
}

/// Apply a legacy `AttributeUpdates` map (pre-2014 UpdateItem) to an item.
/// Each entry is `{ "Value": <AttributeValue>, "Action": "PUT"|"DELETE"|"ADD" }`
/// (Action defaults to PUT). PUT sets the attribute; DELETE removes it (or
/// removes set elements when a Value is given); ADD increments a number or
/// unions a set (creating it when absent). See bug-audit 2026-06-20, 1.2.
fn apply_attribute_updates(
    item: &mut HashMap<String, AttributeValue>,
    updates: &serde_json::Map<String, AttributeValue>,
) -> Result<(), AwsServiceError> {
    let invalid =
        |m: String| AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", m);
    for (attr, spec) in updates {
        let action = spec.get("Action").and_then(|v| v.as_str()).unwrap_or("PUT");
        match action {
            "PUT" => {
                if let Some(val) = spec.get("Value") {
                    item.insert(attr.clone(), val.clone());
                }
            }
            "DELETE" => match spec.get("Value") {
                None => {
                    item.remove(attr);
                }
                Some(val) => remove_set_elements(item, attr, val),
            },
            "ADD" => {
                if let Some(val) = spec.get("Value") {
                    add_to_attribute(item, attr, val)?;
                }
            }
            other => return Err(invalid(format!("Unknown AttributeUpdates action: {other}"))),
        }
    }
    Ok(())
}

/// ADD semantics: numeric increment for `N`, set union for `SS`/`NS`/`BS`.
/// Creates the attribute when absent. Mismatched types are a ValidationException.
fn add_to_attribute(
    item: &mut HashMap<String, AttributeValue>,
    attr: &str,
    val: &AttributeValue,
) -> Result<(), AwsServiceError> {
    let invalid =
        |m: String| AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", m);
    let existing = item.get(attr).cloned();
    match existing {
        None => {
            item.insert(attr.to_string(), val.clone());
        }
        Some(cur) => {
            if let (Some(a), Some(b)) = (
                cur.get("N").and_then(|v| v.as_str()),
                val.get("N").and_then(|v| v.as_str()),
            ) {
                // Arbitrary-precision decimal add: f64 silently rounds past
                // 2^53 and an `as i64` cast saturates past ~9.2e18, corrupting
                // large counters. Integral results carry no trailing `.0`.
                let s = crate::service::helpers::decimal_add_sub(a, b, true)
                    .ok_or_else(|| invalid("ADD operand is not a number".into()))?;
                item.insert(attr.to_string(), json!({ "N": s }));
            } else {
                // Set union for SS/NS/BS.
                for set_type in ["SS", "NS", "BS"] {
                    if let (Some(a), Some(b)) = (
                        cur.get(set_type).and_then(|v| v.as_array()),
                        val.get(set_type).and_then(|v| v.as_array()),
                    ) {
                        let mut merged = a.clone();
                        for e in b {
                            // A Number Set dedups by numeric value ("1" == "1.0"),
                            // so a raw `contains` would build an invalid set.
                            // SS/BS keep exact equality.
                            let already = if set_type == "NS" {
                                merged.iter().any(|m| ns_members_equal(m, e))
                            } else {
                                merged.contains(e)
                            };
                            if !already {
                                merged.push(e.clone());
                            }
                        }
                        item.insert(attr.to_string(), json!({ set_type: merged }));
                        return Ok(());
                    }
                }
                return Err(invalid(format!(
                    "ADD is only supported for number and set types for attribute {attr}"
                )));
            }
        }
    }
    Ok(())
}

/// DELETE with a Value removes the given elements from a set attribute.
fn remove_set_elements(
    item: &mut HashMap<String, AttributeValue>,
    attr: &str,
    val: &AttributeValue,
) {
    for set_type in ["SS", "NS", "BS"] {
        if let Some(remove) = val.get(set_type).and_then(|v| v.as_array()) {
            if let Some(cur) = item.get_mut(attr).and_then(|v| v.get_mut(set_type)) {
                if let Some(arr) = cur.as_array_mut() {
                    // Number-set members are removed by numeric value, so
                    // DELETEing "1.0" drops the stored "1". SS/BS use exact
                    // string equality.
                    if set_type == "NS" {
                        arr.retain(|e| !remove.iter().any(|r| ns_members_equal(r, e)));
                    } else {
                        arr.retain(|e| !remove.contains(e));
                    }
                }
            }
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // AttributeValue is `serde_json::Value`; a DynamoDB number attribute
    // is the JSON object `{"N": "<digits>"}`.
    fn n(v: &str) -> AttributeValue {
        json!({ "N": v })
    }

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
        pairs.iter().map(|(k, v)| (k.to_string(), n(v))).collect()
    }

    // bug-audit 2026-05-28, 1.8: UPDATED_NEW returns only changed
    // attributes (new values), not the whole item.
    #[test]
    fn updated_new_returns_only_changed_new_values() {
        let pre = map(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let post = map(&[("a", "1"), ("b", "20"), ("c", "3"), ("d", "4")]);
        let got = diff_updated_attributes(Some(&pre), &post, UpdatedSide::New);
        // b changed, d added; a and c unchanged so omitted.
        assert_eq!(got, map(&[("b", "20"), ("d", "4")]));
    }

    // bug-audit 2026-05-28, 1.8: UPDATED_OLD returns the OLD value of each
    // changed attribute (was previously empty).
    #[test]
    fn updated_old_returns_only_changed_old_values() {
        let pre = map(&[("a", "1"), ("b", "2"), ("e", "9")]);
        let post = map(&[("a", "1"), ("b", "20")]); // b changed, e removed
        let got = diff_updated_attributes(Some(&pre), &post, UpdatedSide::Old);
        assert_eq!(got, map(&[("b", "2"), ("e", "9")]));
    }

    #[test]
    fn updated_new_on_insert_returns_all_attributes() {
        let post = map(&[("a", "1"), ("b", "2")]);
        let got = diff_updated_attributes(None, &post, UpdatedSide::New);
        assert_eq!(got, post);
    }

    #[tokio::test]
    async fn all_old_on_upsert_insert_returns_no_attributes() {
        // ReturnValues=ALL_OLD on an insert-via-update must return no Attributes
        // (there was no prior item), not the key-only stub (bug-hunt 2026-07-01).
        use crate::state::{
            DynamoTable, KeySchemaElement, ProvisionedThroughput, SharedDynamoDbState,
        };
        use std::collections::BTreeMap;
        use std::sync::Arc;

        let state: SharedDynamoDbState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        {
            let mut accts = state.write();
            let s = accts.get_or_create("123456789012");
            s.tables.insert(
                "T".to_string(),
                DynamoTable {
                    name: "T".into(),
                    arn: "arn:aws:dynamodb:us-east-1:123456789012:table/T".into(),
                    table_id: "id".into(),
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
                    key_index: Default::default(),
                    gsi: vec![],
                    lsi: vec![],
                    tags: BTreeMap::new(),
                    created_at: chrono::Utc::now(),
                    status: "ACTIVE".into(),
                    item_count: 0,
                    size_bytes: 0,
                    billing_mode: "PAY_PER_REQUEST".into(),
                    ttl_attribute: None,
                    ttl_enabled: false,
                    resource_policy: None,
                    pitr_enabled: false,
                    kinesis_destinations: vec![],
                    contributor_insights_status: "DISABLED".into(),
                    contributor_insights_counters: BTreeMap::new(),
                    stream_enabled: false,
                    stream_view_type: None,
                    stream_arn: None,
                    stream_records: Arc::new(parking_lot::RwLock::new(Vec::new())),
                    sse_type: None,
                    sse_kms_key_arn: None,
                    deletion_protection_enabled: false,
                    on_demand_throughput: None,
                    table_class: "STANDARD".into(),
                    vector_indexes: Vec::new(),
                },
            );
        }
        let svc = DynamoDbService::new(state);
        let req = AwsRequest {
            service: "dynamodb".into(),
            action: "UpdateItem".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "r".into(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_vec(&json!({
                    "TableName": "T",
                    "Key": {"pk": {"S": "new"}},
                    "UpdateExpression": "SET x = :x",
                    "ExpressionAttributeValues": {":x": {"N": "1"}},
                    "ReturnValues": "ALL_OLD"
                }))
                .unwrap(),
            ),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let resp = svc.update_item(&req).unwrap();
        let body: serde_json::Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert!(
            body.get("Attributes").is_none(),
            "ALL_OLD on insert must return no Attributes, got: {body}"
        );
    }

    #[test]
    fn no_changes_yields_empty() {
        let pre = map(&[("a", "1")]);
        let post = map(&[("a", "1")]);
        assert!(diff_updated_attributes(Some(&pre), &post, UpdatedSide::New).is_empty());
        assert!(diff_updated_attributes(Some(&pre), &post, UpdatedSide::Old).is_empty());
    }

    // Legacy AttributeUpdates ADD/DELETE path: a Number Set compares members by
    // numeric value, so "1" and "1.0" are the same member (bug-hunt 2026-07-22).
    #[test]
    fn legacy_add_number_set_dedups_by_numeric_value() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("scores".to_string(), json!({"NS": ["1", "2"]}));
        add_to_attribute(&mut item, "scores", &json!({"NS": ["1.0"]})).unwrap();
        assert_eq!(item["scores"], json!({"NS": ["1", "2"]}));
    }

    #[test]
    fn legacy_add_string_set_keeps_exact_equality() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("tags".to_string(), json!({"SS": ["1", "2"]}));
        add_to_attribute(&mut item, "tags", &json!({"SS": ["1.0"]})).unwrap();
        assert_eq!(item["tags"], json!({"SS": ["1", "2", "1.0"]}));
    }

    #[test]
    fn legacy_delete_number_set_removes_by_numeric_value() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("scores".to_string(), json!({"NS": ["1", "2"]}));
        remove_set_elements(&mut item, "scores", &json!({"NS": ["1.0"]}));
        assert_eq!(item["scores"], json!({"NS": ["2"]}));
    }
}
