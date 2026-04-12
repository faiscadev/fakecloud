use std::collections::HashMap;

use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use super::{
    apply_update_expression, evaluate_condition, extract_key, get_table, get_table_mut,
    parse_expression_attribute_names, parse_expression_attribute_values, project_item,
    require_object, require_str, validate_key_attributes_in_key, validate_key_in_item,
    DynamoDbService,
};

impl DynamoDbService {
    pub(super) fn put_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // --- Parse request body and expression attributes WITHOUT holding any lock ---
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let item = require_object(&body, "Item")?;
        let condition = body["ConditionExpression"].as_str().map(|s| s.to_string());
        let expr_attr_names = parse_expression_attribute_names(&body);
        let expr_attr_values = parse_expression_attribute_values(&body);
        let return_values = body["ReturnValues"].as_str().unwrap_or("NONE").to_string();

        // --- Acquire write lock ONLY for validation + mutation ---
        // Capture kinesis delivery info alongside the return value
        let (old_item, kinesis_info) = {
            let mut state = self.state.write();
            let region = state.region.clone();
            let table = get_table_mut(&mut state.tables, table_name)?;

            validate_key_in_item(table, &item)?;

            let key = extract_key(table, &item);
            let existing_idx = table.find_item_index(&key);

            if let Some(ref cond) = condition {
                let existing = existing_idx.map(|i| &table.items[i]);
                evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)?;
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

            if let Some(idx) = existing_idx {
                table.items[idx] = item.clone();
            } else {
                table.items.push(item.clone());
            }

            table.record_item_access(&item);
            table.recalculate_stats();

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
            let kinesis_info = if table
                .kinesis_destinations
                .iter()
                .any(|d| d.destination_status == "ACTIVE")
            {
                Some((
                    table.clone(),
                    event_name.to_string(),
                    key,
                    old_item_for_stream,
                    Some(item.clone()),
                ))
            } else {
                None
            };

            (old_item_for_return, kinesis_info)
        };
        // --- Write lock released, build response ---

        // Deliver to Kinesis destinations outside the lock
        if let Some((table, event_name, keys, old_image, new_image)) = kinesis_info {
            self.deliver_to_kinesis_destinations(
                &table,
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

        Self::ok_json(result)
    }

    pub(super) fn get_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // --- Parse request body WITHOUT holding any lock ---
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let key = require_object(&body, "Key")?;

        // --- Use a read lock for the lookup (allows concurrent GetItem calls) ---
        let (result, needs_insights) = {
            let state = self.state.read();
            let table = get_table(&state.tables, table_name)?;
            let needs_insights = table.contributor_insights_status == "ENABLED";

            let result = match table.find_item_index(&key) {
                Some(idx) => {
                    let item = &table.items[idx];
                    let projected = project_item(item, &body);
                    json!({ "Item": projected })
                }
                None => json!({}),
            };
            (result, needs_insights)
        };
        // --- Read lock released ---

        // Only acquire write lock if contributor insights tracking is enabled
        if needs_insights {
            let mut state = self.state.write();
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
            let mut state = self.state.write();
            let region = state.region.clone();
            let table = get_table_mut(&mut state.tables, table_name)?;

            let condition = body["ConditionExpression"].as_str();
            let expr_attr_names = parse_expression_attribute_names(&body);
            let expr_attr_values = parse_expression_attribute_values(&body);

            let existing_idx = table.find_item_index(&key);

            if let Some(cond) = condition {
                let existing = existing_idx.map(|i| &table.items[i]);
                evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)?;
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
                if table
                    .kinesis_destinations
                    .iter()
                    .any(|d| d.destination_status == "ACTIVE")
                {
                    kinesis_info = Some((table.clone(), key.clone(), Some(old_item)));
                }

                table.items.remove(idx);
                table.recalculate_stats();
            }

            let return_consumed = body["ReturnConsumedCapacity"].as_str().unwrap_or("NONE");
            let return_icm = body["ReturnItemCollectionMetrics"]
                .as_str()
                .unwrap_or("NONE");

            if return_consumed == "TOTAL" || return_consumed == "INDEXES" {
                result["ConsumedCapacity"] = json!({
                    "TableName": table_name,
                    "CapacityUnits": 1.0,
                });
            }

            if return_icm == "SIZE" {
                result["ItemCollectionMetrics"] = json!({});
            }

            (result, kinesis_info)
        };

        // Deliver to Kinesis destinations outside the lock
        if let Some((table, keys, old_image)) = kinesis_info {
            self.deliver_to_kinesis_destinations(&table, "REMOVE", &keys, old_image.as_ref(), None);
        }

        Self::ok_json(result)
    }

    pub(super) fn update_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let key = require_object(&body, "Key")?;

        let mut state = self.state.write();
        let region = state.region.clone();
        let table = get_table_mut(&mut state.tables, table_name)?;

        validate_key_attributes_in_key(table, &key)?;

        let condition = body["ConditionExpression"].as_str();
        let expr_attr_names = parse_expression_attribute_names(&body);
        let expr_attr_values = parse_expression_attribute_values(&body);
        let update_expression = body["UpdateExpression"].as_str();

        let existing_idx = table.find_item_index(&key);

        if let Some(cond) = condition {
            let existing = existing_idx.map(|i| &table.items[i]);
            evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)?;
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
                table.items.push(new_item);
                table.items.len() - 1
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

        let old_item = if return_values == "ALL_OLD" {
            Some(table.items[idx].clone())
        } else {
            None
        };

        if let Some(expr) = update_expression {
            apply_update_expression(
                &mut table.items[idx],
                expr,
                &expr_attr_names,
                &expr_attr_values,
            )?;
        }

        let new_item = if return_values == "ALL_NEW" || return_values == "UPDATED_NEW" {
            Some(table.items[idx].clone())
        } else {
            None
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
        let kinesis_info = if table
            .kinesis_destinations
            .iter()
            .any(|d| d.destination_status == "ACTIVE")
        {
            Some((
                table.clone(),
                event_name.to_string(),
                key.clone(),
                old_item_for_stream,
                Some(new_item_for_stream),
            ))
        } else {
            None
        };

        table.recalculate_stats();

        // Release the write lock (drop `state`)
        drop(state);

        // Deliver to Kinesis destinations outside the lock
        if let Some((tbl, ev, keys, old_image, new_image)) = kinesis_info {
            self.deliver_to_kinesis_destinations(
                &tbl,
                &ev,
                &keys,
                old_image.as_ref(),
                new_image.as_ref(),
            );
        }

        let mut result = json!({});
        if let Some(old) = old_item {
            result["Attributes"] = json!(old);
        } else if let Some(new) = new_item {
            result["Attributes"] = json!(new);
        }

        Self::ok_json(result)
    }
}
