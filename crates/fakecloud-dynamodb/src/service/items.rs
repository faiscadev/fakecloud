use std::collections::HashMap;

use http::StatusCode;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{
    apply_update_expression_tracked, build_capacity, build_consumed_capacity,
    build_item_collection_metrics, check_put_item_size, compact_projected_lists,
    evaluate_condition_with_return, extract_key, get_data_table, get_data_table_mut,
    insert_nested_value_segments, item_size, item_write_consumed, normalize_item_numbers,
    normalize_value_numbers, ns_members_equal, parse_expression_attribute_names,
    parse_expression_attribute_values, project_item, read_units, require_object, resolve_doc_path,
    resolve_write_condition, return_consumed_mode, return_icm_mode, validate_attribute_value,
    validate_data_table_name, validate_first_request_enum, validate_item_attribute_values,
    validate_key_attributes_in_key, validate_key_in_item, validate_request_enums,
    validate_request_expressions, AttributeValue, CapacitySplit, Consumed, DocPath,
    DynamoDbService, ExprOp, PathElem, PathSegment, UpdateCharge, RETURN_CONSUMED_CAPACITY_VALUES,
    RETURN_ITEM_COLLECTION_METRICS_VALUES, RETURN_VALUES, RETURN_VALUES_ON_FAILURE_VALUES,
};

impl DynamoDbService {
    pub(super) fn put_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // --- Parse request body and expression attributes WITHOUT holding any lock ---
        let body = Self::parse_body(req)?;
        let table_name = validate_data_table_name(&body)?;
        validate_request_enums(
            &body,
            &[
                (
                    "ReturnConsumedCapacity",
                    "returnConsumedCapacity",
                    RETURN_CONSUMED_CAPACITY_VALUES,
                ),
                (
                    "ReturnItemCollectionMetrics",
                    "returnItemCollectionMetrics",
                    RETURN_ITEM_COLLECTION_METRICS_VALUES,
                ),
                ("ReturnValues", "returnValues", RETURN_VALUES),
                (
                    "ReturnValuesOnConditionCheckFailure",
                    "returnValuesOnConditionCheckFailure",
                    RETURN_VALUES_ON_FAILURE_VALUES,
                ),
                ("ConditionalOperator", "conditionalOperator", &["AND", "OR"]),
            ],
        )?;
        let mut item = require_object(&body, "Item")?;
        validate_request_expressions(&body, ExprOp::PutItem)?;
        if !matches!(
            body["ReturnValues"].as_str(),
            None | Some("NONE" | "ALL_OLD")
        ) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "ReturnValues can only be ALL_OLD or NONE",
            ));
        }
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
        let (old_item, kinesis_info, kms_audit, icm, consumed) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let region = state.region.clone();
            let table = get_data_table_mut(&mut state.tables, table_name)?;

            validate_key_in_item(table, &item)?;
            // Validate every attribute value (not just keys): a malformed number
            // like {"N":"abc"} is a ValidationException in real DynamoDB.
            validate_item_attribute_values(&item)?;
            // Secondary-index key values must be non-empty and of the
            // index's declared type.
            super::validate_index_keys_in_item(table, &item)?;
            normalize_item_numbers(&mut item);
            check_put_item_size(&item)?;
            super::vectors::validate_vector_item(
                &table.vector_indexes,
                &table.attribute_definitions,
                &item,
            )?;

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

            let consumed = (return_consumed != "NONE").then(|| {
                item_write_consumed(table, existing_idx.map(|i| &table.items[i]), Some(&item))
            });

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

            (old_item_for_return, kinesis_info, kms_audit, icm, consumed)
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
        if let Some(consumed) = consumed {
            let cc = build_capacity(&return_consumed, table_name, &consumed, CapacitySplit::None);
            if !cc.is_null() {
                result["ConsumedCapacity"] = cc;
            }
        }
        if !icm.is_null() {
            result["ItemCollectionMetrics"] = icm;
        }

        Self::ok_json(result)
    }

    pub(super) fn get_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // --- Parse request body WITHOUT holding any lock ---
        let body = Self::parse_body(req)?;
        let table_name = validate_data_table_name(&body)?;
        validate_request_enums(
            &body,
            &[(
                "ReturnConsumedCapacity",
                "returnConsumedCapacity",
                RETURN_CONSUMED_CAPACITY_VALUES,
            )],
        )?;
        let key = require_object(&body, "Key")?;
        validate_request_expressions(&body, ExprOp::GetItem)?;
        let return_consumed = return_consumed_mode(&body).to_string();

        // --- Use a read lock for the lookup (allows concurrent GetItem calls) ---
        let (result, needs_insights, kms_audit) = {
            let accounts = self.state.read();
            let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
            let table = get_data_table(&state.tables, table_name)?;
            validate_key_attributes_in_key(table, &key)?;
            let needs_insights = table.contributor_insights_status == "ENABLED";

            let consistent = body["ConsistentRead"].as_bool().unwrap_or(false);
            let mut read_bytes = 0;
            let mut result = match table.find_item_index(&key) {
                Some(idx) => {
                    let item = &table.items[idx];
                    // Capacity is charged on the whole item, whatever the
                    // projection returns.
                    read_bytes = item_size(item);
                    let projected = project_item(item, &body);
                    json!({ "Item": projected })
                }
                None => json!({}),
            };
            let cc = build_consumed_capacity(
                &return_consumed,
                table_name,
                read_units(read_bytes, consistent),
                0.0,
            );
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
            if let Some(table) = state.tables.get_mut(super::resolve_table_name(table_name)) {
                table.record_key_access(&key);
            }
        }

        Self::ok_json(result)
    }

    pub(super) fn delete_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;

        let table_name = validate_data_table_name(&body)?;
        validate_request_enums(
            &body,
            &[
                ("ConditionalOperator", "conditionalOperator", &["AND", "OR"]),
                (
                    "ReturnConsumedCapacity",
                    "returnConsumedCapacity",
                    RETURN_CONSUMED_CAPACITY_VALUES,
                ),
                ("ReturnValues", "returnValues", RETURN_VALUES),
                (
                    "ReturnItemCollectionMetrics",
                    "returnItemCollectionMetrics",
                    RETURN_ITEM_COLLECTION_METRICS_VALUES,
                ),
                (
                    "ReturnValuesOnConditionCheckFailure",
                    "returnValuesOnConditionCheckFailure",
                    RETURN_VALUES_ON_FAILURE_VALUES,
                ),
            ],
        )?;
        let key = require_object(&body, "Key")?;
        validate_request_expressions(&body, ExprOp::DeleteItem)?;
        if !matches!(
            body["ReturnValues"].as_str(),
            None | Some("NONE" | "ALL_OLD")
        ) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "ReturnValues can only be ALL_OLD or NONE",
            ));
        }

        let (result, kinesis_info) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let region = state.region.clone();
            let table = get_data_table_mut(&mut state.tables, table_name)?;
            // The same key validation GetItem and UpdateItem apply: a missing,
            // wrong-typed or empty key attribute is a ValidationException, not a
            // silent no-op delete.
            validate_key_attributes_in_key(table, &key)?;

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
            let return_consumed = body["ReturnConsumedCapacity"].as_str().unwrap_or("NONE");
            let consumed = (return_consumed != "NONE")
                .then(|| item_write_consumed(table, existing_idx.map(|i| &table.items[i]), None));

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

            let return_icm = body["ReturnItemCollectionMetrics"]
                .as_str()
                .unwrap_or("NONE");

            if let Some(consumed) = consumed {
                let cc =
                    build_capacity(return_consumed, table_name, &consumed, CapacitySplit::None);
                if !cc.is_null() {
                    result["ConsumedCapacity"] = cc;
                }
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
        let table_name = validate_data_table_name(&body)?;
        // UpdateItem stops at the first invalid enum member rather than
        // aggregating them.
        validate_first_request_enum(
            &body,
            &[
                ("ReturnValues", "returnValues", RETURN_VALUES),
                (
                    "ReturnConsumedCapacity",
                    "returnConsumedCapacity",
                    RETURN_CONSUMED_CAPACITY_VALUES,
                ),
                (
                    "ReturnItemCollectionMetrics",
                    "returnItemCollectionMetrics",
                    RETURN_ITEM_COLLECTION_METRICS_VALUES,
                ),
                (
                    "ReturnValuesOnConditionCheckFailure",
                    "returnValuesOnConditionCheckFailure",
                    RETURN_VALUES_ON_FAILURE_VALUES,
                ),
                ("ConditionalOperator", "conditionalOperator", &["AND", "OR"]),
            ],
        )?;
        let key = require_object(&body, "Key")?;
        let parsed = validate_request_expressions(&body, ExprOp::UpdateItem)?;
        // The paths the update writes, for the UPDATED_* return values.
        let updated_paths: Vec<DocPath> =
            match (&parsed.update, body["AttributeUpdates"].as_object()) {
                (Some(ast), _) => ast.target_paths(),
                (None, Some(updates)) => updates
                    .keys()
                    .map(|k| vec![PathElem::Attr(k.clone())])
                    .collect(),
                _ => Vec::new(),
            };
        // REMOVE targets (right after the SET targets in `updated_paths`)
        // set nothing, so UPDATED_NEW leaves them out; UPDATED_OLD reports
        // their old values.
        let removed_range = parsed.update.as_ref().map_or(0..0, |ast| {
            ast.sets.len()..ast.sets.len() + ast.removes.len()
        });
        let return_consumed = return_consumed_mode(&body).to_string();
        let return_icm = return_icm_mode(&body).to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let region = state.region.clone();
        let table = get_data_table_mut(&mut state.tables, table_name)?;

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
        if let Some(expr) = update_expression {
            super::reject_key_attribute_update_expression(table, expr, &expr_attr_names)?;
        } else if let Some(updates) = body["AttributeUpdates"].as_object() {
            super::reject_key_attribute_updates(table, updates)?;
        }

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
        // Numbers are stored in canonical form, so the values this update
        // writes are normalized on the way in. Only those: the rest of the
        // stored row is left exactly as it is, so an attribute the update
        // does not touch never shows up as changed.
        for v in expr_attr_values.values_mut() {
            normalize_value_numbers(v);
        }
        let attribute_updates = body["AttributeUpdates"]
            .as_object()
            .cloned()
            .map(|mut updates| {
                for upd in updates.values_mut() {
                    if let Some(v) = upd.get_mut("Value") {
                        normalize_value_numbers(v);
                    }
                }
                updates
            });

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
                normalize_item_numbers(&mut new_item);
                // Registers the row in the key index and the stats; the
                // attribute updates below then mutate it in place through
                // `update_item_at`, which settles the deltas.
                table.put_item_at_key(new_item).0
            }
        };

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
            || return_consumed != "NONE"
        {
            Some(table.items[idx].clone())
        } else {
            None
        };

        // An UpdateExpression is applied clause by clause and can fail partway
        // (a type error on a later operand) with earlier clauses already
        // written -- possibly a rewritten key. `update_item_at` puts the row
        // back as it was, so a rejected UpdateItem changes nothing, as on AWS.
        let charge = match (update_expression, attribute_updates.as_ref()) {
            (Some(expr), _) => UpdateCharge::for_expression(expr, &expr_attr_names),
            (None, Some(updates)) => UpdateCharge::for_attribute_updates(updates),
            (None, None) => UpdateCharge::default(),
        };
        let index_keys = super::index_key_specs(table);
        // Where each SET's value ended up once the whole expression ran.
        let mut written_paths: Vec<(usize, DocPath)> = Vec::new();
        // A vector index judges the item the update leaves behind.
        let vector_indexes = table.vector_indexes.clone();
        let vector_defs = if vector_indexes.is_empty() {
            Vec::new()
        } else {
            table.attribute_definitions.clone()
        };
        let applied = table.update_item_at(idx, |item| {
            let before = (!index_keys.is_empty()).then(|| item.clone());
            if let Some(expr) = update_expression {
                written_paths = apply_update_expression_tracked(
                    item,
                    expr,
                    &expr_attr_names,
                    &expr_attr_values,
                )?;
            } else if let Some(updates) = attribute_updates.as_ref() {
                // Legacy AttributeUpdates (pre-2014 UpdateItem), still emitted by
                // the AWS SDK for Java v1, older boto3, and the Terraform provider.
                // Without this an UpdateItem using AttributeUpdates wrote nothing and
                // (on a missing key) left a key-only stub item -- silent data loss
                // (bug-audit 2026-06-20, 1.2).
                apply_attribute_updates(item, updates)?;
            }
            // A secondary-index key the update writes must be non-empty and
            // of the index's declared type; the row is put back otherwise.
            if let Some(fault) = super::index_key_fault(&index_keys, item, before.as_ref()) {
                return Err(fault.update_error());
            }
            charge.check(item)?;
            super::vectors::validate_vector_item(&vector_indexes, &vector_defs, item)
        });
        if let Err(err) = applied {
            // An upsert that fails must not leave behind the key-only row it
            // registered above.
            if is_insert {
                table.remove_item_at(idx);
            }
            return Err(err);
        }
        // UPDATED_NEW reads the post-update item, so it takes each SET target
        // (the first entries of `updated_paths`, in expression order) where
        // its value ended up -- a list index past the end appends, and a
        // later REMOVE of an earlier element shifts it down -- and leaves
        // out REMOVE targets, which set nothing. UPDATED_OLD keeps
        // the expression's own paths against the pre-update item, where an
        // out-of-range index simply projects nothing.
        let mut new_paths: Vec<DocPath> = updated_paths.clone();
        for (ordinal, written) in written_paths {
            if let Some(path) = new_paths.get_mut(ordinal) {
                *path = written;
            }
        }
        let new_paths: Vec<DocPath> = new_paths
            .into_iter()
            .enumerate()
            .filter(|(i, _)| !removed_range.contains(i))
            .map(|(_, p)| p)
            .collect();

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
            "UPDATED_NEW" => Some(project_paths(Some(&table.items[idx]), &new_paths)),
            "UPDATED_OLD" => Some(project_paths(old_snapshot, &updated_paths)),
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

        let icm = build_item_collection_metrics(&return_icm, table, &key);
        let consumed = if return_consumed == "NONE" {
            Consumed::default()
        } else {
            item_write_consumed(table, old_snapshot, Some(&table.items[idx]))
        };

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
        let cc = build_capacity(&return_consumed, table_name, &consumed, CapacitySplit::None);
        if !cc.is_null() {
            result["ConsumedCapacity"] = cc;
        }
        if !icm.is_null() {
            result["ItemCollectionMetrics"] = icm;
        }

        Self::ok_json(result)
    }
}

/// Compute the `UPDATED_NEW` / `UPDATED_OLD` ReturnValues payload: the
/// value at each document path the update wrote, taken from the post-update
/// (`UPDATED_NEW`) or pre-update (`UPDATED_OLD`) item. A nested write returns
/// only its fragment (`parent.child`, not the whole `parent` map), and a path
/// absent on that side (a REMOVE for NEW, a new attribute for OLD) is omitted.
fn project_paths(
    item: Option<&HashMap<String, AttributeValue>>,
    paths: &[DocPath],
) -> HashMap<String, AttributeValue> {
    let mut out = HashMap::new();
    let Some(item) = item else {
        return out;
    };
    for path in paths {
        let Some(v) = resolve_doc_path(item, path) else {
            continue;
        };
        let segments: Vec<PathSegment> = path
            .iter()
            .map(|e| match e {
                PathElem::Attr(a) => PathSegment::Key(a.clone()),
                PathElem::Index(i) => PathSegment::Index(*i),
            })
            .collect();
        insert_nested_value_segments(&mut out, &segments, v.clone());
    }
    for v in out.values_mut() {
        compact_projected_lists(v);
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

    fn path(elems: &[&str]) -> DocPath {
        elems
            .iter()
            .map(|e| PathElem::Attr(e.to_string()))
            .collect()
    }

    // UPDATED_NEW / UPDATED_OLD return the value at each path the update
    // wrote, not the whole item.
    #[test]
    fn updated_values_project_only_written_paths() {
        let item = map(&[("a", "1"), ("b", "2"), ("c", "3")]);
        let got = project_paths(Some(&item), &[path(&["b"]), path(&["d"])]);
        // d is absent on this side, so it is omitted.
        assert_eq!(got, map(&[("b", "2")]));
        assert!(project_paths(None, &[path(&["b"])]).is_empty());
    }

    // Clauses apply in order: after `REMOVE l[0]` the list has one element,
    // so `SET l[10]` appends at index 1, and UPDATED_NEW reports it there.
    #[test]
    fn list_append_after_remove_reports_written_index() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("l".into(), json!({"L": [{"S": "a"}, {"S": "b"}]}));
        let values: HashMap<String, serde_json::Value> =
            HashMap::from([(":v".to_string(), json!({"S": "z"}))]);
        let written = apply_update_expression_tracked(
            &mut item,
            "REMOVE l[0] SET l[10] = :v",
            &HashMap::new(),
            &values,
        )
        .unwrap();
        assert_eq!(last_indexes(written), vec![(0, 1)]);
        assert_eq!(item["l"], json!({"L": [{"S": "b"}, {"S": "z"}]}));
        let got = project_paths(
            Some(&item),
            &[vec![PathElem::Attr("l".into()), PathElem::Index(1)]],
        );
        assert_eq!(got["l"], json!({"L": [{"S": "z"}]}));
        // UPDATED_OLD takes the expression's paths (l[0], l[10]) against the
        // pre-update list: l[0] was "a", and l[10] had no old value.
        let pre: HashMap<String, AttributeValue> =
            HashMap::from([("l".to_string(), json!({"L": [{"S": "a"}, {"S": "b"}]}))]);
        let old = project_paths(
            Some(&pre),
            &[
                vec![PathElem::Attr("l".into()), PathElem::Index(10)],
                vec![PathElem::Attr("l".into()), PathElem::Index(0)],
            ],
        );
        assert_eq!(old["l"], json!({"L": [{"S": "a"}]}));
    }

    /// `(SET ordinal, final list index)` for each recorded path ending in one.
    fn last_indexes(written: Vec<(usize, DocPath)>) -> Vec<(usize, usize)> {
        written
            .into_iter()
            .filter_map(|(o, p)| match p.last() {
                Some(PathElem::Index(i)) => Some((o, *i)),
                _ => None,
            })
            .collect()
    }

    fn apply(item: &mut HashMap<String, AttributeValue>, expr: &str) -> Vec<(usize, usize)> {
        let values: HashMap<String, serde_json::Value> =
            HashMap::from([(":v".to_string(), json!({"S": "v"}))]);
        last_indexes(apply_update_expression_tracked(item, expr, &HashMap::new(), &values).unwrap())
    }

    // A REMOVE later in the expression shifts the list the SET wrote into:
    // `SET l[1] = :v REMOVE l[0]` on [a, b] leaves [v], so the value is
    // reported at l[0] (and a nested SET through the list shifts the same way).
    #[test]
    fn later_remove_shifts_recorded_set_paths() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("l".into(), json!({"L": [{"S": "a"}, {"S": "b"}]}));
        assert_eq!(apply(&mut item, "SET l[1] = :v REMOVE l[0]"), vec![(0, 0)]);
        assert_eq!(item["l"], json!({"L": [{"S": "v"}]}));
        let got = project_paths(
            Some(&item),
            &[vec![PathElem::Attr("l".into()), PathElem::Index(0)]],
        );
        assert_eq!(got["l"], json!({"L": [{"S": "v"}]}));

        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert(
            "l".into(),
            json!({"L": [{"M": {"x": {"S": "0"}}}, {"M": {"x": {"S": "1"}}}]}),
        );
        let values: HashMap<String, serde_json::Value> =
            HashMap::from([(":v".to_string(), json!({"S": "v"}))]);
        let written = apply_update_expression_tracked(
            &mut item,
            "SET l[1].x = :v REMOVE l[0]",
            &HashMap::new(),
            &values,
        )
        .unwrap();
        assert_eq!(
            written,
            vec![(
                0,
                vec![
                    PathElem::Attr("l".into()),
                    PathElem::Index(0),
                    PathElem::Attr("x".into())
                ]
            )]
        );
    }

    // SET/REMOVE targets use the expression grammar's path segmentation, so
    // every `[N]` of a part is a list step.
    #[test]
    fn multi_index_paths_set_and_remove() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("l".into(), json!({"L": [{"L": [{"S": "a"}, {"S": "b"}]}]}));
        item.insert(
            "a".into(),
            json!({"M": {"m": {"L": [{"L": [{"S": "x"}, {"S": "y"}, {"S": "z"}]}]}}}),
        );

        assert_eq!(apply(&mut item, "SET l[0][1] = :v"), vec![(0, 1)]);
        assert_eq!(item["l"], json!({"L": [{"L": [{"S": "a"}, {"S": "v"}]}]}));

        assert_eq!(apply(&mut item, "SET a.m[0][2] = :v"), vec![(0, 2)]);
        assert_eq!(
            item["a"],
            json!({"M": {"m": {"L": [{"L": [{"S": "x"}, {"S": "y"}, {"S": "v"}]}]}}})
        );

        apply(&mut item, "REMOVE l[0][1]");
        assert_eq!(item["l"], json!({"L": [{"L": [{"S": "a"}]}]}));
        assert!(!item.contains_key("l[0]"));
    }

    // Appending past the end of an inner list lands at its length, and the
    // UPDATED_* payloads follow the written (NEW) and requested (OLD) paths.
    #[test]
    fn inner_list_append_updated_values() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("l".into(), json!({"L": [{"L": [{"S": "a"}]}]}));
        let pre = item.clone();
        let written = apply(&mut item, "SET l[0][9] = :v");
        assert_eq!(written, vec![(0, 1)]);
        assert_eq!(item["l"], json!({"L": [{"L": [{"S": "a"}, {"S": "v"}]}]}));
        let requested = vec![
            PathElem::Attr("l".into()),
            PathElem::Index(0),
            PathElem::Index(9),
        ];
        let mut landed = requested.clone();
        landed[2] = PathElem::Index(written[0].1);
        let new = project_paths(Some(&item), &[landed]);
        assert_eq!(new["l"], json!({"L": [{"L": [{"S": "v"}]}]}));
        assert!(project_paths(Some(&pre), &[requested]).is_empty());
    }

    // A nested list-index SET reports its written index the same way.
    #[test]
    fn nested_list_append_reports_written_index() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("a".into(), json!({"M": {"l": {"L": [{"S": "x"}]}}}));
        let values: HashMap<String, serde_json::Value> =
            HashMap::from([(":v".to_string(), json!({"S": "y"}))]);
        let written =
            apply_update_expression_tracked(&mut item, "SET a.l[5] = :v", &HashMap::new(), &values)
                .unwrap();
        assert_eq!(last_indexes(written), vec![(0, 1)]);
        assert_eq!(
            item["a"],
            json!({"M": {"l": {"L": [{"S": "x"}, {"S": "y"}]}}})
        );
        assert!(!item.contains_key("a.l[5]"));
    }

    // A nested SET returns only the written fragment of the parent map.
    #[test]
    fn updated_values_return_nested_fragment() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert(
            "parent".into(),
            json!({"M": {"keep": {"S": "k"}, "child": {"S": "new"}}}),
        );
        let got = project_paths(Some(&item), &[path(&["parent", "child"])]);
        assert_eq!(got["parent"], json!({"M": {"child": {"S": "new"}}}));
    }

    // PutItem validates ConditionalOperator like DeleteItem and UpdateItem,
    // rather than silently treating an unknown value as AND.
    #[test]
    fn put_item_rejects_invalid_conditional_operator() {
        use crate::state::SharedDynamoDbState;
        use std::sync::Arc;
        let state: SharedDynamoDbState = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let svc = DynamoDbService::new(state);
        let req = AwsRequest {
            service: "dynamodb".into(),
            action: "PutItem".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "r".into(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_vec(&json!({
                    "TableName": "T",
                    "Item": {"pk": {"S": "a"}},
                    "Expected": {"pk": {"Exists": false}},
                    "ConditionalOperator": "XOR",
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
        let err = svc.put_item(&req).err().expect("XOR rejected");
        assert_eq!(err.code(), "ValidationException");
        assert!(
            err.message().contains("at 'conditionalOperator'"),
            "{}",
            err.message()
        );
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
                    items: Default::default(),
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
                    pitr_history: Default::default(),
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
