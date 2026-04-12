use std::collections::HashMap;

use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::AttributeValue;

use super::{
    compare_attribute_values, evaluate_filter_expression, evaluate_key_condition,
    extract_key_for_schema, get_table, item_matches_key, parse_expression_attribute_names,
    parse_expression_attribute_values, parse_key_map, project_item, require_str, DynamoDbService,
};

impl DynamoDbService {
    pub(super) fn query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let expr_attr_names = parse_expression_attribute_names(&body);
        let expr_attr_values = parse_expression_attribute_values(&body);

        let key_condition = body["KeyConditionExpression"].as_str();
        let filter_expression = body["FilterExpression"].as_str();
        let scan_forward = body["ScanIndexForward"].as_bool().unwrap_or(true);
        let limit = body["Limit"].as_i64().map(|l| l as usize);
        let index_name = body["IndexName"].as_str();
        let exclusive_start_key: Option<HashMap<String, AttributeValue>> =
            parse_key_map(&body["ExclusiveStartKey"]);

        let (items_to_scan, hash_key_name, range_key_name): (
            &[HashMap<String, AttributeValue>],
            String,
            Option<String>,
        ) = if let Some(idx_name) = index_name {
            if let Some(gsi) = table.gsi.iter().find(|g| g.index_name == idx_name) {
                let hk = gsi
                    .key_schema
                    .iter()
                    .find(|k| k.key_type == "HASH")
                    .map(|k| k.attribute_name.clone())
                    .unwrap_or_default();
                let rk = gsi
                    .key_schema
                    .iter()
                    .find(|k| k.key_type == "RANGE")
                    .map(|k| k.attribute_name.clone());
                (&table.items, hk, rk)
            } else if let Some(lsi) = table.lsi.iter().find(|l| l.index_name == idx_name) {
                let hk = lsi
                    .key_schema
                    .iter()
                    .find(|k| k.key_type == "HASH")
                    .map(|k| k.attribute_name.clone())
                    .unwrap_or_default();
                let rk = lsi
                    .key_schema
                    .iter()
                    .find(|k| k.key_type == "RANGE")
                    .map(|k| k.attribute_name.clone());
                (&table.items, hk, rk)
            } else {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("The table does not have the specified index: {idx_name}"),
                ));
            }
        } else {
            (
                &table.items[..],
                table.hash_key_name().to_string(),
                table.range_key_name().map(|s| s.to_string()),
            )
        };

        let mut matched: Vec<&HashMap<String, AttributeValue>> = items_to_scan
            .iter()
            .filter(|item| {
                if let Some(kc) = key_condition {
                    evaluate_key_condition(
                        kc,
                        item,
                        &hash_key_name,
                        range_key_name.as_deref(),
                        &expr_attr_names,
                        &expr_attr_values,
                    )
                } else {
                    true
                }
            })
            .collect();

        if let Some(ref rk) = range_key_name {
            matched.sort_by(|a, b| {
                let av = a.get(rk.as_str());
                let bv = b.get(rk.as_str());
                compare_attribute_values(av, bv)
            });
            if !scan_forward {
                matched.reverse();
            }
        }

        // For GSI queries, we need the table's primary key attributes to uniquely
        // identify items (GSI keys are not unique).
        let table_pk_hash = table.hash_key_name().to_string();
        let table_pk_range = table.range_key_name().map(|s| s.to_string());
        let is_gsi_query = index_name.is_some()
            && (hash_key_name != table_pk_hash
                || range_key_name.as_deref() != table_pk_range.as_deref());

        // Apply ExclusiveStartKey: skip items up to and including the start key.
        // For GSI queries the start key contains both index keys and table PK, so
        // we must match on ALL of them to find the exact item.
        if let Some(ref start_key) = exclusive_start_key {
            if let Some(pos) = matched.iter().position(|item| {
                let index_match =
                    item_matches_key(item, start_key, &hash_key_name, range_key_name.as_deref());
                if is_gsi_query {
                    index_match
                        && item_matches_key(
                            item,
                            start_key,
                            &table_pk_hash,
                            table_pk_range.as_deref(),
                        )
                } else {
                    index_match
                }
            }) {
                matched = matched.split_off(pos + 1);
            }
        }

        if let Some(filter) = filter_expression {
            matched.retain(|item| {
                evaluate_filter_expression(filter, item, &expr_attr_names, &expr_attr_values)
            });
        }

        let scanned_count = matched.len();

        let has_more = if let Some(lim) = limit {
            let more = matched.len() > lim;
            matched.truncate(lim);
            more
        } else {
            false
        };

        // Build LastEvaluatedKey from the last returned item if there are more results.
        // For GSI queries, include both the index keys and the table's primary key
        // so the item can be uniquely identified on resume.
        let last_evaluated_key = if has_more {
            matched.last().map(|item| {
                let mut key =
                    extract_key_for_schema(item, &hash_key_name, range_key_name.as_deref());
                if is_gsi_query {
                    let table_key =
                        extract_key_for_schema(item, &table_pk_hash, table_pk_range.as_deref());
                    key.extend(table_key);
                }
                key
            })
        } else {
            None
        };

        // Collect partition key values for contributor insights
        let insights_enabled = table.contributor_insights_status == "ENABLED";
        let pk_name = table.hash_key_name().to_string();
        let accessed_keys: Vec<String> = if insights_enabled {
            matched
                .iter()
                .filter_map(|item| item.get(&pk_name).map(|v| v.to_string()))
                .collect()
        } else {
            Vec::new()
        };

        let items: Vec<Value> = matched
            .iter()
            .map(|item| {
                let projected = project_item(item, &body);
                json!(projected)
            })
            .collect();

        let mut result = json!({
            "Items": items,
            "Count": items.len(),
            "ScannedCount": scanned_count,
        });

        if let Some(lek) = last_evaluated_key {
            result["LastEvaluatedKey"] = json!(lek);
        }

        drop(state);

        if !accessed_keys.is_empty() {
            let mut state = self.state.write();
            if let Some(table) = state.tables.get_mut(table_name) {
                // Re-check insights status after acquiring write lock in case it
                // was disabled between the read and write lock acquisitions.
                if table.contributor_insights_status == "ENABLED" {
                    for key_str in accessed_keys {
                        *table
                            .contributor_insights_counters
                            .entry(key_str)
                            .or_insert(0) += 1;
                    }
                }
            }
        }

        Self::ok_json(result)
    }

    pub(super) fn scan(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let expr_attr_names = parse_expression_attribute_names(&body);
        let expr_attr_values = parse_expression_attribute_values(&body);
        let filter_expression = body["FilterExpression"].as_str();
        let limit = body["Limit"].as_i64().map(|l| l as usize);
        let exclusive_start_key: Option<HashMap<String, AttributeValue>> =
            parse_key_map(&body["ExclusiveStartKey"]);

        let hash_key_name = table.hash_key_name().to_string();
        let range_key_name = table.range_key_name().map(|s| s.to_string());

        let mut matched: Vec<&HashMap<String, AttributeValue>> = table.items.iter().collect();

        // Apply ExclusiveStartKey: skip items up to and including the start key
        if let Some(ref start_key) = exclusive_start_key {
            if let Some(pos) = matched.iter().position(|item| {
                item_matches_key(item, start_key, &hash_key_name, range_key_name.as_deref())
            }) {
                matched = matched.split_off(pos + 1);
            }
        }

        let scanned_count = matched.len();

        if let Some(filter) = filter_expression {
            matched.retain(|item| {
                evaluate_filter_expression(filter, item, &expr_attr_names, &expr_attr_values)
            });
        }

        let has_more = if let Some(lim) = limit {
            let more = matched.len() > lim;
            matched.truncate(lim);
            more
        } else {
            false
        };

        // Build LastEvaluatedKey from the last returned item if there are more results
        let last_evaluated_key = if has_more {
            matched
                .last()
                .map(|item| extract_key_for_schema(item, &hash_key_name, range_key_name.as_deref()))
        } else {
            None
        };

        // Collect partition key values for contributor insights
        let insights_enabled = table.contributor_insights_status == "ENABLED";
        let pk_name = table.hash_key_name().to_string();
        let accessed_keys: Vec<String> = if insights_enabled {
            matched
                .iter()
                .filter_map(|item| item.get(&pk_name).map(|v| v.to_string()))
                .collect()
        } else {
            Vec::new()
        };

        let items: Vec<Value> = matched
            .iter()
            .map(|item| {
                let projected = project_item(item, &body);
                json!(projected)
            })
            .collect();

        let mut result = json!({
            "Items": items,
            "Count": items.len(),
            "ScannedCount": scanned_count,
        });

        if let Some(lek) = last_evaluated_key {
            result["LastEvaluatedKey"] = json!(lek);
        }

        drop(state);

        if !accessed_keys.is_empty() {
            let mut state = self.state.write();
            if let Some(table) = state.tables.get_mut(table_name) {
                // Re-check insights status after acquiring write lock in case it
                // was disabled between the read and write lock acquisitions.
                if table.contributor_insights_status == "ENABLED" {
                    for key_str in accessed_keys {
                        *table
                            .contributor_insights_counters
                            .entry(key_str)
                            .or_insert(0) += 1;
                    }
                }
            }
        }

        Self::ok_json(result)
    }
}
