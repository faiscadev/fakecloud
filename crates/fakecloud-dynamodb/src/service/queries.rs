use std::collections::HashMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{AttributeValue, Projection};

use super::{
    build_consumed_capacity, compare_attribute_values, evaluate_filter_expression,
    evaluate_key_condition, extract_key_for_schema, get_table, item_matches_key,
    parse_expression_attribute_names, parse_expression_attribute_values, parse_key_map,
    project_item, require_str, return_consumed_mode, translate_legacy_conditions, DynamoDbService,
    LegacyConditionRole,
};

impl DynamoDbService {
    pub(super) fn query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let return_consumed = return_consumed_mode(&body).to_string();

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let table = get_table(&state.tables, table_name)?;

        let mut expr_attr_names = parse_expression_attribute_names(&body);
        let mut expr_attr_values = parse_expression_attribute_values(&body);

        // Query REQUIRES a key condition. Accept either the modern
        // `KeyConditionExpression` or the legacy `KeyConditions` parameter
        // (AWS-deprecated in 2014 but still accepted by real DynamoDB and
        // every SDK), which we translate into the equivalent expression and
        // evaluate through the same machinery. Without any key condition,
        // AWS rejects the request rather than scanning the whole table —
        // returning every item would be a silent wrong-result bug
        // (bug-audit 2026-05-28, 1.1).
        let key_condition = resolve_legacy_or_expression(
            &body,
            "KeyConditionExpression",
            "KeyConditions",
            LegacyConditionRole::Key,
            &mut expr_attr_names,
            &mut expr_attr_values,
        )?;
        let key_condition = match key_condition {
            Some(kc) if !kc.trim().is_empty() => kc,
            _ => {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.",
                ));
            }
        };
        let filter_expression = resolve_legacy_or_expression(
            &body,
            "FilterExpression",
            "QueryFilter",
            LegacyConditionRole::Filter,
            &mut expr_attr_names,
            &mut expr_attr_values,
        )?;
        let scan_forward = body["ScanIndexForward"].as_bool().unwrap_or(true);
        let limit = validate_limit(&body)?;
        let index_name = body["IndexName"].as_str();
        let count_only = resolve_select(&body, index_name.is_some())?;
        let exclusive_start_key: Option<HashMap<String, AttributeValue>> =
            parse_key_map(&body["ExclusiveStartKey"]);

        let consistent_read = body["ConsistentRead"].as_bool().unwrap_or(false);
        let (items_to_scan, hash_key_name, range_key_name): (
            &[HashMap<String, AttributeValue>],
            String,
            Option<String>,
        ) = if let Some(idx_name) = index_name {
            if let Some(gsi) = table.gsi.iter().find(|g| g.index_name == idx_name) {
                if consistent_read {
                    return Err(AwsServiceError::aws_error(
                        http::StatusCode::BAD_REQUEST,
                        "ValidationException",
                        "Consistent reads are not supported on global secondary indexes",
                    ));
                }
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
                evaluate_key_condition(&key_condition, item, &expr_attr_names, &expr_attr_values)
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
        // Pull the index's Projection + key attributes once, so the
        // per-item closure below doesn't need to re-walk gsi/lsi.
        let query_index_projection: Option<(Projection, Vec<String>)> =
            index_name.and_then(|idx| {
                table
                    .gsi
                    .iter()
                    .find(|g| g.index_name == idx)
                    .map(|g| {
                        (
                            g.projection.clone(),
                            g.key_schema
                                .iter()
                                .map(|k| k.attribute_name.clone())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .or_else(|| {
                        table.lsi.iter().find(|l| l.index_name == idx).map(|l| {
                            (
                                l.projection.clone(),
                                l.key_schema
                                    .iter()
                                    .map(|k| k.attribute_name.clone())
                                    .collect::<Vec<_>>(),
                            )
                        })
                    })
            });

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

        // AWS semantics: `Limit` caps the number of items *examined*
        // (post-key-condition, pre-FilterExpression). FilterExpression
        // then runs on the limited slice, and `LastEvaluatedKey`
        // points at the last item examined — even if the filter
        // dropped it. Without this ordering a paginating client never
        // converges: the filter would shrink the set before the
        // truncation tracked progress.
        let has_more;
        let last_examined_idx;
        if let Some(lim) = limit {
            has_more = matched.len() > lim;
            last_examined_idx = if has_more { Some(lim - 1) } else { None };
            matched.truncate(lim);
        } else {
            has_more = false;
            last_examined_idx = None;
        }

        // Snapshot the key of the last examined item before the filter
        // can drop it.
        let last_examined_key =
            last_examined_idx
                .and_then(|i| matched.get(i).copied())
                .map(|item| {
                    let mut key =
                        extract_key_for_schema(item, &hash_key_name, range_key_name.as_deref());
                    if is_gsi_query {
                        let table_key =
                            extract_key_for_schema(item, &table_pk_hash, table_pk_range.as_deref());
                        key.extend(table_key);
                    }
                    key
                });

        let scanned_count = matched.len();

        if let Some(filter) = filter_expression.as_deref() {
            matched.retain(|item| {
                // On an index query with a non-ALL projection, the filter can
                // only see attributes projected into the index — DynamoDB does
                // not fetch non-projected attributes from the base table, so a
                // filter referencing one sees it as absent.
                if let Some((proj, key_attrs)) = query_index_projection.as_ref() {
                    if proj.projection_type != "ALL" {
                        let projected = apply_index_projection(
                            (*item).clone(),
                            proj,
                            key_attrs,
                            &table_pk_hash,
                            table_pk_range.as_deref(),
                        );
                        return evaluate_filter_expression(
                            filter,
                            &projected,
                            &expr_attr_names,
                            &expr_attr_values,
                        );
                    }
                }
                evaluate_filter_expression(filter, item, &expr_attr_names, &expr_attr_values)
            });
        }

        let last_evaluated_key = if has_more { last_examined_key } else { None };

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

        let items: Vec<Value> = if count_only {
            Vec::new()
        } else {
            matched
                .iter()
                .map(|item| {
                    let mut projected = project_item(item, &body);
                    if let Some((proj, key_attrs)) = query_index_projection.as_ref() {
                        projected = apply_index_projection(
                            projected,
                            proj,
                            key_attrs,
                            &table_pk_hash,
                            table_pk_range.as_deref(),
                        );
                    }
                    json!(projected)
                })
                .collect()
        };
        let count = matched.len();
        let mut result = if count_only {
            json!({
                "Count": count,
                "ScannedCount": scanned_count,
            })
        } else {
            json!({
                "Items": items,
                "Count": count,
                "ScannedCount": scanned_count,
            })
        };

        if let Some(lek) = last_evaluated_key {
            result["LastEvaluatedKey"] = json!(lek);
        }

        let cc = build_consumed_capacity(
            &return_consumed,
            table_name,
            (scanned_count.max(1) as f64) * 0.5,
            0.0,
        );
        if !cc.is_null() {
            result["ConsumedCapacity"] = cc;
        }

        drop(accounts);

        if !accessed_keys.is_empty() {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
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
        let return_consumed = return_consumed_mode(&body).to_string();

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let table = get_table(&state.tables, table_name)?;

        let mut expr_attr_names = parse_expression_attribute_names(&body);
        let mut expr_attr_values = parse_expression_attribute_values(&body);
        // Accept the legacy `ScanFilter` parameter alongside the modern
        // `FilterExpression` (same parity rationale as Query's QueryFilter).
        let filter_expression = resolve_legacy_or_expression(
            &body,
            "FilterExpression",
            "ScanFilter",
            LegacyConditionRole::Filter,
            &mut expr_attr_names,
            &mut expr_attr_values,
        )?;
        let limit = validate_limit(&body)?;
        let exclusive_start_key: Option<HashMap<String, AttributeValue>> =
            parse_key_map(&body["ExclusiveStartKey"]);
        let count_only = resolve_select(&body, body["IndexName"].as_str().is_some())?;

        // IndexName: when present, items still come from the base
        // table (fakecloud doesn't keep separate per-index storage)
        // but the projection is restricted to what the index defines.
        let index_name = body["IndexName"].as_str();
        let consistent_read = body["ConsistentRead"].as_bool().unwrap_or(false);
        let (index_projection, index_key_attrs): (Option<Projection>, Vec<String>) =
            if let Some(idx) = index_name {
                if let Some(g) = table.gsi.iter().find(|g| g.index_name == idx) {
                    if consistent_read {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ValidationException",
                            "Consistent reads are not supported on global secondary indexes",
                        ));
                    }
                    (
                        Some(g.projection.clone()),
                        g.key_schema
                            .iter()
                            .map(|k| k.attribute_name.clone())
                            .collect(),
                    )
                } else if let Some(l) = table.lsi.iter().find(|l| l.index_name == idx) {
                    (
                        Some(l.projection.clone()),
                        l.key_schema
                            .iter()
                            .map(|k| k.attribute_name.clone())
                            .collect(),
                    )
                } else {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!("Index '{idx}' does not exist on the table"),
                    ));
                }
            } else {
                (None, Vec::new())
            };

        let hash_key_name = table.hash_key_name().to_string();
        let range_key_name = table.range_key_name().map(|s| s.to_string());

        // Parallel Scan: Segment / TotalSegments split the table into
        // disjoint shards by hashing the partition key. Real DDB
        // doesn't document the hash function, so we use stdlib
        // `DefaultHasher` over the rendered hash-key value — stable
        // across a single fakecloud run, which is enough for the
        // disjoint-shard contract clients depend on.
        let total_segments = body["TotalSegments"].as_i64().map(|v| v as usize);
        let segment = body["Segment"].as_i64().map(|v| v as usize);
        if total_segments.is_some() != segment.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Both Segment and TotalSegments must be supplied together",
            ));
        }
        if let (Some(seg), Some(total)) = (segment, total_segments) {
            if total == 0 || seg >= total {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Segment must be less than TotalSegments and TotalSegments must be > 0",
                ));
            }
        }

        let mut matched: Vec<&HashMap<String, AttributeValue>> = table
            .items
            .iter()
            .filter(|item| match (segment, total_segments) {
                (Some(seg), Some(total)) => {
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut h = DefaultHasher::new();
                    item.get(hash_key_name.as_str())
                        .map(|v| v.to_string())
                        .unwrap_or_default()
                        .hash(&mut h);
                    (h.finish() as usize) % total == seg
                }
                _ => true,
            })
            .collect();

        // Apply ExclusiveStartKey: skip items up to and including the start key
        if let Some(ref start_key) = exclusive_start_key {
            if let Some(pos) = matched.iter().position(|item| {
                item_matches_key(item, start_key, &hash_key_name, range_key_name.as_deref())
            }) {
                matched = matched.split_off(pos + 1);
            }
        }

        // Same Limit-before-Filter ordering as Query (see comment
        // there): pagination only converges if `LastEvaluatedKey`
        // tracks examined items, not surviving items.
        let has_more;
        let last_examined_idx;
        if let Some(lim) = limit {
            has_more = matched.len() > lim;
            last_examined_idx = if has_more { Some(lim - 1) } else { None };
            matched.truncate(lim);
        } else {
            has_more = false;
            last_examined_idx = None;
        }
        let last_examined_key = last_examined_idx
            .and_then(|i| matched.get(i).copied())
            .map(|item| extract_key_for_schema(item, &hash_key_name, range_key_name.as_deref()));

        let scanned_count = matched.len();

        if let Some(filter) = filter_expression.as_deref() {
            matched.retain(|item| {
                // On an index scan with a non-ALL projection, the filter can
                // only see attributes projected into the index — DynamoDB does
                // not fetch non-projected attributes from the base table, so a
                // filter referencing one sees it as absent.
                if let Some(ref proj) = index_projection {
                    if proj.projection_type != "ALL" {
                        let projected = apply_index_projection(
                            (*item).clone(),
                            proj,
                            &index_key_attrs,
                            &hash_key_name,
                            range_key_name.as_deref(),
                        );
                        return evaluate_filter_expression(
                            filter,
                            &projected,
                            &expr_attr_names,
                            &expr_attr_values,
                        );
                    }
                }
                evaluate_filter_expression(filter, item, &expr_attr_names, &expr_attr_values)
            });
        }

        let last_evaluated_key = if has_more { last_examined_key } else { None };

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

        let items: Vec<Value> = if count_only {
            Vec::new()
        } else {
            matched
                .iter()
                .map(|item| {
                    let mut projected = project_item(item, &body);
                    if let Some(ref proj) = index_projection {
                        projected = apply_index_projection(
                            projected,
                            proj,
                            &index_key_attrs,
                            &hash_key_name,
                            range_key_name.as_deref(),
                        );
                    }
                    json!(projected)
                })
                .collect()
        };
        let count = matched.len();
        let mut result = if count_only {
            json!({
                "Count": count,
                "ScannedCount": scanned_count,
            })
        } else {
            json!({
                "Items": items,
                "Count": count,
                "ScannedCount": scanned_count,
            })
        };

        if let Some(lek) = last_evaluated_key {
            result["LastEvaluatedKey"] = json!(lek);
        }

        let cc = build_consumed_capacity(
            &return_consumed,
            table_name,
            (scanned_count.max(1) as f64) * 0.5,
            0.0,
        );
        if !cc.is_null() {
            result["ConsumedCapacity"] = cc;
        }

        drop(accounts);

        if !accessed_keys.is_empty() {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
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

/// Resolve an expression-API parameter (`KeyConditionExpression` /
/// `FilterExpression`) against its legacy non-expression counterpart
/// (`KeyConditions` / `QueryFilter` / `ScanFilter`) for one role.
///
/// Returns the expression string to evaluate, or `None` when neither form
/// was supplied (callers decide whether that's an error). When only the
/// legacy form is present it is translated and its placeholders are injected
/// into `names`/`values`. Supplying both forms for the same role is rejected,
/// matching real DynamoDB.
fn resolve_legacy_or_expression(
    body: &Value,
    expression_param: &str,
    legacy_param: &str,
    role: LegacyConditionRole,
    names: &mut HashMap<String, String>,
    values: &mut HashMap<String, Value>,
) -> Result<Option<String>, AwsServiceError> {
    let conditional_operator = body["ConditionalOperator"].as_str().unwrap_or("AND");
    let expression = body[expression_param]
        .as_str()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let legacy = body[legacy_param].as_object().filter(|m| !m.is_empty());

    match (expression, legacy) {
        (Some(_), Some(_)) => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "Can not use both expression and non-expression parameters in the same request: \
                 Non-expression parameters: {{{legacy_param}}} Expression parameters: {{{expression_param}}}"
            ),
        )),
        (Some(expr), None) => Ok(Some(expr.to_string())),
        (None, Some(legacy)) => Ok(Some(translate_legacy_conditions(
            legacy,
            role,
            conditional_operator,
            names,
            values,
        )?)),
        (None, None) => Ok(None),
    }
}

/// Validate the `Limit` parameter shared by Query and Scan. AWS rejects
/// any non-positive limit with a ValidationException rather than casting
/// `-1` to `usize::MAX` or returning an empty page (which spins
/// paginators). Returns the validated limit as `Option<usize>`.
fn validate_limit(body: &Value) -> Result<Option<usize>, AwsServiceError> {
    match body["Limit"].as_i64() {
        Some(l) if l < 1 => Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Limit must be >= 1",
        )),
        Some(l) => Ok(Some(l as usize)),
        None => Ok(None),
    }
}

/// Validate the `Select` parameter for a Query/Scan and decide whether
/// the operation returns only a count. `is_index_query` is true when an
/// IndexName is present (only then is ALL_PROJECTED_ATTRIBUTES legal).
///
/// AWS rules enforced here:
/// - `Select` must be one of the four documented enum values.
/// - `SPECIFIC_ATTRIBUTES` requires a ProjectionExpression or
///   AttributesToGet, and conversely any projection forces
///   SPECIFIC_ATTRIBUTES (any other explicit Select is rejected).
/// - `ALL_PROJECTED_ATTRIBUTES` is only valid on an index query.
fn resolve_select(body: &Value, is_index_query: bool) -> Result<bool, AwsServiceError> {
    let select = body["Select"].as_str();
    let has_projection = body["ProjectionExpression"]
        .as_str()
        .is_some_and(|p| !p.is_empty())
        || body["AttributesToGet"]
            .as_array()
            .is_some_and(|a| !a.is_empty());

    if let Some(sel) = select {
        if !matches!(
            sel,
            "ALL_ATTRIBUTES" | "ALL_PROJECTED_ATTRIBUTES" | "SPECIFIC_ATTRIBUTES" | "COUNT"
        ) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{sel}' at 'select' failed to satisfy \
                     constraint: Member must satisfy enum value set: \
                     [SPECIFIC_ATTRIBUTES, COUNT, ALL_ATTRIBUTES, ALL_PROJECTED_ATTRIBUTES]"
                ),
            ));
        }
        if sel == "ALL_PROJECTED_ATTRIBUTES" && !is_index_query {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Select type ALL_PROJECTED_ATTRIBUTES is not supported for queries on the table; \
                 it is only supported on index queries",
            ));
        }
        if sel == "SPECIFIC_ATTRIBUTES" && !has_projection {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Select type SPECIFIC_ATTRIBUTES requires a ProjectionExpression or AttributesToGet",
            ));
        }
        if has_projection && sel != "SPECIFIC_ATTRIBUTES" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "Cannot use both Select and ProjectionExpression/AttributesToGet unless \
                     Select is SPECIFIC_ATTRIBUTES (got {sel})"
                ),
            ));
        }
        Ok(sel == "COUNT")
    } else {
        Ok(false)
    }
}

/// Apply a GSI/LSI projection to an item already projected via the
/// caller's `ProjectionExpression`. AWS retains the table's primary key
/// plus the index key; INCLUDE adds the listed non-key attributes;
/// KEYS_ONLY drops everything else; ALL leaves the item alone.
fn apply_index_projection(
    item: HashMap<String, AttributeValue>,
    projection: &Projection,
    index_key_attrs: &[String],
    table_hash_key: &str,
    table_range_key: Option<&str>,
) -> HashMap<String, AttributeValue> {
    if projection.projection_type == "ALL" {
        return item;
    }
    let mut allowed: Vec<String> = Vec::new();
    allowed.push(table_hash_key.to_string());
    if let Some(rk) = table_range_key {
        allowed.push(rk.to_string());
    }
    for k in index_key_attrs {
        if !allowed.contains(k) {
            allowed.push(k.clone());
        }
    }
    if projection.projection_type == "INCLUDE" {
        for k in &projection.non_key_attributes {
            if !allowed.contains(k) {
                allowed.push(k.clone());
            }
        }
    }
    let mut out = HashMap::new();
    for k in &allowed {
        if let Some(v) = item.get(k) {
            out.insert(k.clone(), v.clone());
        }
    }
    out
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

    fn seed_table(
        state: &SharedDynamoDbState,
        name: &str,
        items: Vec<HashMap<String, AttributeValue>>,
    ) {
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
            items,
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
            stream_enabled: false,
            stream_view_type: None,
            stream_arn: None,
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,
            deletion_protection_enabled: false,
            on_demand_throughput: None,
            table_class: "STANDARD".to_string(),
        };
        s.tables.insert(name.to_string(), table);
    }

    fn item(pk: &str) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert("pk".to_string(), json!({"S": pk}));
        m
    }

    fn item_with(pk: &str, attr: &str, val: &str) -> HashMap<String, AttributeValue> {
        let mut m = HashMap::new();
        m.insert("pk".to_string(), json!({"S": pk}));
        m.insert(attr.to_string(), json!({"S": val}));
        m
    }

    #[tokio::test]
    async fn scan_limit_caps_examined_not_filtered() {
        // 4 items: 2 match the filter, 2 don't. Limit=2 must examine
        // the first 2 only (one of which matches), so Items.len() = 1
        // and ScannedCount = 2.
        let state = make_state();
        seed_table(
            &state,
            "T",
            vec![
                item_with("a", "color", "red"),
                item_with("b", "color", "blue"),
                item_with("c", "color", "red"),
                item_with("d", "color", "red"),
            ],
        );
        let svc = DynamoDbService::new(state);
        let resp = svc
            .scan(&req_for(
                "Scan",
                json!({
                    "TableName": "T",
                    "Limit": 2,
                    "FilterExpression": "color = :v",
                    "ExpressionAttributeValues": {":v": {"S": "red"}},
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["ScannedCount"].as_i64().unwrap(), 2);
        assert_eq!(body["Count"].as_i64().unwrap(), 1);
        assert!(
            body["LastEvaluatedKey"].is_object(),
            "LastEvaluatedKey must point at the last examined item, not the last surviving"
        );
    }

    /// 1.25: Scan must reject Limit <= 0 with ValidationException rather
    /// than casting -1 to usize::MAX or returning an empty page with a
    /// LastEvaluatedKey.
    #[tokio::test]
    async fn scan_rejects_non_positive_limit() {
        let state = make_state();
        seed_table(&state, "T", vec![item("a"), item("b")]);
        let svc = DynamoDbService::new(state);
        for bad in [-1, 0] {
            let err = svc
                .scan(&req_for("Scan", json!({"TableName": "T", "Limit": bad})))
                .err()
                .unwrap_or_else(|| panic!("Limit={bad} must be rejected"));
            assert!(format!("{err:?}").contains("ValidationException"));
        }
    }

    /// 1.25: Query must reject Limit <= 0 too.
    #[tokio::test]
    async fn query_rejects_non_positive_limit() {
        let state = make_state();
        seed_table(&state, "T", vec![item("a")]);
        let svc = DynamoDbService::new(state);
        let err = svc
            .query(&req_for(
                "Query",
                json!({
                    "TableName": "T",
                    "Limit": 0,
                    "KeyConditionExpression": "pk = :v",
                    "ExpressionAttributeValues": {":v": {"S": "a"}},
                }),
            ))
            .err()
            .expect("Limit=0 rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
    }

    /// 1.13: Scan Select=SPECIFIC_ATTRIBUTES with a ProjectionExpression
    /// projects to the requested attributes; Select=COUNT counts only.
    #[tokio::test]
    async fn scan_select_specific_attributes_projects() {
        let state = make_state();
        seed_table(&state, "T", vec![item_with("a", "color", "red")]);
        let svc = DynamoDbService::new(state);
        let resp = svc
            .scan(&req_for(
                "Scan",
                json!({
                    "TableName": "T",
                    "Select": "SPECIFIC_ATTRIBUTES",
                    "ProjectionExpression": "color",
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let item0 = &body["Items"][0];
        assert!(item0.get("color").is_some());
        assert!(item0.get("pk").is_none());
    }

    /// 1.13: Scan Select=ALL_ATTRIBUTES returns full items.
    #[tokio::test]
    async fn scan_select_all_attributes_returns_full_item() {
        let state = make_state();
        seed_table(&state, "T", vec![item_with("a", "color", "red")]);
        let svc = DynamoDbService::new(state);
        let resp = svc
            .scan(&req_for(
                "Scan",
                json!({"TableName": "T", "Select": "ALL_ATTRIBUTES"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let item0 = &body["Items"][0];
        assert!(item0.get("pk").is_some());
        assert!(item0.get("color").is_some());
    }

    /// 1.13: Invalid Select values and incompatible combinations are
    /// rejected with ValidationException.
    #[tokio::test]
    async fn scan_rejects_invalid_and_incompatible_select() {
        let state = make_state();
        seed_table(&state, "T", vec![item("a")]);
        let svc = DynamoDbService::new(state);

        // garbage enum value
        let err = svc
            .scan(&req_for(
                "Scan",
                json!({"TableName": "T", "Select": "NONSENSE"}),
            ))
            .err()
            .expect("invalid Select rejected");
        assert!(format!("{err:?}").contains("ValidationException"));

        // SPECIFIC_ATTRIBUTES with no projection
        let err = svc
            .scan(&req_for(
                "Scan",
                json!({"TableName": "T", "Select": "SPECIFIC_ATTRIBUTES"}),
            ))
            .err()
            .expect("SPECIFIC_ATTRIBUTES needs projection");
        assert!(format!("{err:?}").contains("ValidationException"));

        // ALL_PROJECTED_ATTRIBUTES on a non-index (table) scan
        let err = svc
            .scan(&req_for(
                "Scan",
                json!({"TableName": "T", "Select": "ALL_PROJECTED_ATTRIBUTES"}),
            ))
            .err()
            .expect("ALL_PROJECTED needs an index");
        assert!(format!("{err:?}").contains("ValidationException"));

        // ProjectionExpression with Select=ALL_ATTRIBUTES is incompatible
        let err = svc
            .scan(&req_for(
                "Scan",
                json!({
                    "TableName": "T",
                    "Select": "ALL_ATTRIBUTES",
                    "ProjectionExpression": "pk",
                }),
            ))
            .err()
            .expect("projection forces SPECIFIC_ATTRIBUTES");
        assert!(format!("{err:?}").contains("ValidationException"));
    }

    /// 1.12: Scan honors the legacy AttributesToGet parameter.
    #[tokio::test]
    async fn scan_honors_legacy_attributes_to_get() {
        let state = make_state();
        seed_table(&state, "T", vec![item_with("a", "color", "red")]);
        let svc = DynamoDbService::new(state);
        let resp = svc
            .scan(&req_for(
                "Scan",
                json!({"TableName": "T", "AttributesToGet": ["color"]}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let item0 = &body["Items"][0];
        assert!(item0.get("color").is_some());
        assert!(item0.get("pk").is_none());
    }

    #[tokio::test]
    async fn scan_parallel_segments_partition_table() {
        let state = make_state();
        seed_table(
            &state,
            "T",
            (0..16).map(|i| item(&format!("k{i}"))).collect(),
        );
        let svc = DynamoDbService::new(state);
        let mut union = std::collections::HashSet::new();
        for seg in 0..4 {
            let resp = svc
                .scan(&req_for(
                    "Scan",
                    json!({
                        "TableName": "T",
                        "TotalSegments": 4,
                        "Segment": seg,
                    }),
                ))
                .unwrap();
            let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            for it in body["Items"].as_array().unwrap() {
                let pk = it["pk"]["S"].as_str().unwrap().to_string();
                assert!(
                    union.insert(pk.clone()),
                    "key {pk} appeared in two segments — shards must be disjoint"
                );
            }
        }
        assert_eq!(
            union.len(),
            16,
            "every item must land in exactly one segment"
        );
    }

    #[tokio::test]
    async fn scan_segment_without_total_segments_rejected() {
        let state = make_state();
        seed_table(&state, "T", vec![item("a")]);
        let svc = DynamoDbService::new(state);
        let err = svc
            .scan(&req_for("Scan", json!({"TableName": "T", "Segment": 0})))
            .err()
            .expect("should reject Segment without TotalSegments");
        assert!(format!("{err:?}").contains("Segment and TotalSegments"));
    }
}
