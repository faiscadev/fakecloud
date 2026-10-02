use std::collections::HashMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{AttributeValue, Projection};

use super::{
    build_capacity, compare_attribute_values, eval_cond, evaluate_filter_expression,
    evaluate_key_condition, extract_key_for_schema, framework_validation_error, get_data_table,
    item_matches_key, item_size, parse_condition_lenient, parse_expression_attribute_names,
    parse_expression_attribute_values, parse_key_map, project_item, read_units,
    request_enum_violations, resolve_attr_name, return_consumed_mode, split_on_and,
    strip_outer_parens, translate_legacy_conditions, validate_data_table_name,
    validate_request_enums, validate_request_expressions, value_type, CapacitySplit, Consumed,
    DynamoDbService, DynamoTable, ExprOp, LegacyConditionRole, RETURN_CONSUMED_CAPACITY_VALUES,
};

impl DynamoDbService {
    pub(super) fn query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = validate_data_table_name(&body)?;
        validate_request_enums(
            &body,
            &[
                ("Select", "select", SELECT_VALUES),
                (
                    "ReturnConsumedCapacity",
                    "returnConsumedCapacity",
                    RETURN_CONSUMED_CAPACITY_VALUES,
                ),
                ("ConditionalOperator", "conditionalOperator", &["AND", "OR"]),
            ],
        )?;
        // Query's Limit error really differs from Scan's: AWS names the
        // member `Limit` (capitalised) and echoes no value, where Scan says
        // `Value '0' at 'limit'`. Both forms are pinned from live captures.
        if body["Limit"].as_i64().is_some_and(|l| l < 1) {
            return Err(framework_validation_error(&[
                "Value at 'Limit' failed to satisfy constraint: Member must have value greater \
                 than or equal to 1"
                    .to_string(),
            ]));
        }
        let index_name = body["IndexName"].as_str();
        let count_only = resolve_select(&body, index_name.is_some(), true)?;
        validate_request_expressions(&body, ExprOp::Query)?;
        let return_consumed = return_consumed_mode(&body).to_string();

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let table = get_data_table(&state.tables, table_name)?;

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
        let exclusive_start_key: Option<HashMap<String, AttributeValue>> =
            parse_key_map(&body["ExclusiveStartKey"]);

        let consistent_read = body["ConsistentRead"].as_bool().unwrap_or(false);
        let (items_to_scan, hash_key_name, range_key_name): (
            &crate::state::TableItems,
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
                super::vectors::reject_vector_index_read(table, idx_name, "Query")?;
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("The table does not have the specified index: {idx_name}"),
                ));
            }
        } else {
            (
                &table.items,
                table.hash_key_name().to_string(),
                table.range_key_name().map(|s| s.to_string()),
            )
        };

        // On an index query the starting key names a position in the index,
        // so it must carry the index key as well as the table's primary key.
        if let (Some(_), Some(start_key)) = (index_name, exclusive_start_key.as_ref()) {
            let required = [
                Some(table.hash_key_name()),
                table.range_key_name(),
                Some(hash_key_name.as_str()),
                range_key_name.as_deref(),
            ];
            if required
                .into_iter()
                .flatten()
                .any(|attr| !start_key.contains_key(attr))
            {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "The provided starting key is invalid: The provided key element does not \
                     match the schema",
                ));
            }
        }

        // The partition key MUST be constrained with `=`. Without this check a
        // condition that omits the partition key (or uses a range operator on
        // it, e.g. `sk = :v` or `pk > :v`) would fall through to a generic
        // filter over EVERY partition and silently return cross-partition
        // matches (bug-hunt 2026-07-01).
        validate_partition_key_condition(&key_condition, &hash_key_name, &expr_attr_names)?;
        if let Some(esk) = exclusive_start_key.as_ref() {
            let index_keys: Vec<&str> = std::iter::once(hash_key_name.as_str())
                .chain(range_key_name.as_deref())
                .collect();
            if !start_key_matches_schema(table, esk, &index_keys) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "The provided starting key is invalid",
                ));
            }
        }
        // Parse once, evaluate per row.
        let key_cond_ast =
            parse_condition_lenient(&key_condition, &expr_attr_names, &expr_attr_values);
        let filter_ast = filter_expression
            .as_deref()
            .and_then(|f| parse_condition_lenient(f, &expr_attr_names, &expr_attr_values));
        let filter_matches =
            |item: &HashMap<String, AttributeValue>, filter: &str| match &filter_ast {
                Some(ast) => eval_cond(ast, item, &expr_attr_values),
                None => {
                    evaluate_filter_expression(filter, item, &expr_attr_names, &expr_attr_values)
                }
            };

        let mut matched: Vec<&HashMap<String, AttributeValue>> = items_to_scan
            .iter()
            .filter(|item| {
                // Sparse index: an item only appears in a GSI/LSI when it
                // carries every one of that index's key attributes. AWS never
                // returns (or counts) an item missing the index hash/range key
                // on an index query, so skip those before evaluating the key
                // condition (mirrors the scan() guard, bug-hunt 2026-07-16).
                if index_name.is_some() {
                    if !item.contains_key(hash_key_name.as_str()) {
                        return false;
                    }
                    if let Some(ref rk) = range_key_name {
                        if !item.contains_key(rk.as_str()) {
                            return false;
                        }
                    }
                }
                match &key_cond_ast {
                    Some(ast) => eval_cond(ast, item, &expr_attr_values),
                    None => evaluate_key_condition(
                        &key_condition,
                        item,
                        &expr_attr_names,
                        &expr_attr_values,
                    ),
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
            let matches_start = |item: &&HashMap<String, AttributeValue>| {
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
            };
            if let Some(pos) = matched.iter().position(&matches_start) {
                matched = matched.split_off(pos + 1);
            } else if let Some(rk) = range_key_name.as_deref() {
                // The ExclusiveStartKey item was deleted between pages. `matched`
                // is sorted by the range key in the scan direction, so resume by
                // order — drop every item at-or-before the start key's range
                // value — instead of leaving the full list, which would restart
                // at page 1 (a non-terminating delete-drain loop).
                let sk_rv = start_key.get(rk).cloned();
                let before = matched.partition_point(|item| {
                    let ord = compare_attribute_values(item.get(rk), sk_rv.as_ref());
                    let ord = if scan_forward { ord } else { ord.reverse() };
                    ord != std::cmp::Ordering::Greater
                });
                matched = matched.split_off(before);
            }
        }

        // AWS semantics: `Limit` caps the number of items *examined*
        // (post-key-condition, pre-FilterExpression). FilterExpression
        // then runs on the limited slice, and `LastEvaluatedKey`
        // points at the last item examined — even if the filter
        // dropped it. Without this ordering a paginating client never
        // converges: the filter would shrink the set before the
        // truncation tracked progress.
        // A page also ends once it has read 1MB of data.
        let page_len = page_length(matched.iter().copied(), limit);
        let has_more = matched.len() > page_len;
        let last_examined_idx = if has_more { Some(page_len - 1) } else { None };
        matched.truncate(page_len);

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
        // Sizing the examined rows (projecting each one for an index read) is
        // only worth doing when the caller asked for the figure.
        let consumed = (return_consumed != "NONE")
            .then(|| read_consumed(table, index_name, &matched, consistent_read));

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
                        return filter_matches(&projected, filter);
                    }
                }
                filter_matches(item, filter)
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

        if let Some(consumed) = consumed {
            let cc = build_capacity(&return_consumed, table_name, &consumed, CapacitySplit::None);
            if !cc.is_null() {
                result["ConsumedCapacity"] = cc;
            }
        }

        drop(accounts);

        if !accessed_keys.is_empty() {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            if let Some(table) = state.tables.get_mut(super::resolve_table_name(table_name)) {
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
        let table_name = validate_data_table_name(&body)?;
        let mut errors = request_enum_violations(
            &body,
            &[
                ("Select", "select", SELECT_VALUES),
                (
                    "ReturnConsumedCapacity",
                    "returnConsumedCapacity",
                    RETURN_CONSUMED_CAPACITY_VALUES,
                ),
                ("ConditionalOperator", "conditionalOperator", &["AND", "OR"]),
            ],
        )?;
        let int_member = |field: &str| body[field].as_i64();
        if let Some(l) = int_member("Limit").filter(|l| *l < 1) {
            errors.push(format!(
                "Value '{l}' at 'limit' failed to satisfy constraint: Member must have value \
                 greater than or equal to 1"
            ));
        }
        if let Some(seg) = int_member("Segment").filter(|s| *s < 0) {
            errors.push(format!(
                "Value '{seg}' at 'segment' failed to satisfy constraint: Member must have value \
                 greater than or equal to 0"
            ));
        }
        match int_member("TotalSegments") {
            Some(t) if t < 1 => errors.push(format!(
                "Value '{t}' at 'totalSegments' failed to satisfy constraint: Member must have \
                 value greater than or equal to 1"
            )),
            Some(t) if t > MAX_TOTAL_SEGMENTS => errors.push(format!(
                "Value '{t}' at 'totalSegments' failed to satisfy constraint: Member must have \
                 value less than or equal to {MAX_TOTAL_SEGMENTS}"
            )),
            _ => {}
        }
        if !errors.is_empty() {
            return Err(framework_validation_error(&errors));
        }
        let total_segments = int_member("TotalSegments").map(|v| v as usize);
        let segment = int_member("Segment").map(|v| v as usize);
        match (segment, total_segments) {
            (Some(_), None) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "The TotalSegments parameter is required but was not present in the request \
                     when Segment parameter is present",
                ))
            }
            (None, Some(_)) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "The Segment parameter is required but was not present in the request when \
                     parameter TotalSegments is present",
                ))
            }
            (Some(seg), Some(total)) if seg >= total => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "The Segment parameter is zero-based and must be less than parameter \
                         TotalSegments: Segment: {seg} is not less than TotalSegments: {total}"
                    ),
                ))
            }
            _ => {}
        }
        let count_only = resolve_select(&body, body["IndexName"].as_str().is_some(), false)?;
        validate_request_expressions(&body, ExprOp::Scan)?;
        let return_consumed = return_consumed_mode(&body).to_string();

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let table = get_data_table(&state.tables, table_name)?;

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
                    super::vectors::reject_vector_index_read(table, idx, "Scan")?;
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        format!("The table does not have the specified index: {idx}"),
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
        // `DefaultHasher` over the rendered hash-key value -- stable
        // across a single fakecloud run, which is enough for the
        // disjoint-shard contract clients depend on.
        if let Some(esk) = exclusive_start_key.as_ref() {
            let index_keys: Vec<&str> = index_key_attrs.iter().map(String::as_str).collect();
            if !start_key_matches_schema(table, esk, &index_keys) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "The provided starting key is invalid: The provided key element does not \
                     match the schema",
                ));
            }
        }
        let filter_ast = filter_expression
            .as_deref()
            .and_then(|f| parse_condition_lenient(f, &expr_attr_names, &expr_attr_values));
        let filter_matches =
            |item: &HashMap<String, AttributeValue>, filter: &str| match &filter_ast {
                Some(ast) => eval_cond(ast, item, &expr_attr_values),
                None => {
                    evaluate_filter_expression(filter, item, &expr_attr_names, &expr_attr_values)
                }
            };

        // Rows come in Scan order starting just after ExclusiveStartKey. That
        // order depends only on key values, so the page resumes in the right
        // place even when the start-key row was deleted between pages -- a
        // client draining the table page by page deletes exactly that row.
        // It used to be looked up by position among the rows, and a missing
        // row ended the scan early with rows still unread (#2504 follow-up).
        let candidates = table
            .scan_rows_after(exclusive_start_key.as_ref())
            .filter(|item| {
                // Sparse index: an index only contains items that carry every
                // one of its key attributes. AWS never returns (or counts) an
                // item missing the index hash/range key on an index scan, so
                // skip those before the segment filter and the count.
                if index_name.is_some() && !index_key_attrs.iter().all(|k| item.contains_key(k)) {
                    return false;
                }
                true
            })
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
            });
        // A page ends at Limit items or 1MB of data read, whichever comes
        // first; only one more row is needed to know whether another page
        // follows. Same Limit-before-Filter ordering as Query (see comment
        // there): pagination only converges if `LastEvaluatedKey` tracks
        // examined items, not surviving items.
        let mut candidates = candidates.peekable();
        let mut matched: Vec<&HashMap<String, AttributeValue>> = Vec::new();
        let mut bytes = 0usize;
        let mut has_more = false;
        while let Some(item) = candidates.next() {
            matched.push(item);
            bytes += item_bytes(item);
            if limit.is_some_and(|l| matched.len() >= l) || bytes >= MAX_PAGE_BYTES {
                has_more = candidates.peek().is_some();
                break;
            }
        }
        drop(candidates);
        let last_examined_idx = if has_more {
            Some(matched.len() - 1)
        } else {
            None
        };
        // The cursor resumes by the table's primary key, which is always in
        // it. On an index scan AWS also includes the index's key attributes.
        let last_examined_key =
            last_examined_idx
                .and_then(|i| matched.get(i).copied())
                .map(|item| {
                    let mut key =
                        extract_key_for_schema(item, &hash_key_name, range_key_name.as_deref());
                    for attr in &index_key_attrs {
                        if let Some(v) = item.get(attr) {
                            key.insert(attr.clone(), v.clone());
                        }
                    }
                    key
                });

        let scanned_count = matched.len();
        // Sizing the examined rows (projecting each one for an index read) is
        // only worth doing when the caller asked for the figure.
        let consumed = (return_consumed != "NONE")
            .then(|| read_consumed(table, index_name, &matched, consistent_read));

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
                        return filter_matches(&projected, filter);
                    }
                }
                filter_matches(item, filter)
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

        if let Some(consumed) = consumed {
            let cc = build_capacity(&return_consumed, table_name, &consumed, CapacitySplit::None);
            if !cc.is_null() {
                result["ConsumedCapacity"] = cc;
            }
        }

        drop(accounts);

        if !accessed_keys.is_empty() {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            if let Some(table) = state.tables.get_mut(super::resolve_table_name(table_name)) {
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

/// A Query's `KeyConditionExpression` must constrain the table (or index)
/// partition key with an equality (`=`). Real DynamoDB rejects a condition
/// that omits the partition key with "Query condition missed key schema
/// element: <pk>", and one that uses any non-`=` operator on the partition
/// key with "Query key condition not supported" — rather than evaluating the
/// expression as a generic filter over every partition.
fn validate_partition_key_condition(
    key_condition: &str,
    partition_key: &str,
    expr_attr_names: &HashMap<String, String>,
) -> Result<(), AwsServiceError> {
    let mut pk_seen = false;
    for raw in split_on_and(key_condition) {
        let part = strip_outer_parens(raw.trim()).trim();

        // begins_with(...) targets the sort key, never the partition key.
        let lower = part.to_ascii_lowercase();
        if lower.starts_with("begins_with(") || lower.starts_with("begins_with (") {
            continue;
        }

        // A word-boundaried BETWEEN is a range condition (sort key only). If it
        // sits on the partition key it's an unsupported non-`=` condition.
        let part_upper = part.to_ascii_uppercase();
        let between_at = part_upper.match_indices("BETWEEN").find(|(i, _)| {
            let before_ws = *i == 0 || part_upper.as_bytes()[*i - 1].is_ascii_whitespace();
            let after = *i + "BETWEEN".len();
            let after_ws =
                after >= part_upper.len() || part_upper.as_bytes()[after].is_ascii_whitespace();
            before_ws && after_ws
        });

        let (op, left): (&str, &str) = if let Some((bpos, _)) = between_at {
            ("BETWEEN", part[..bpos].trim())
        } else {
            let mut found: Option<(&'static str, &str)> = None;
            for cand in ["<=", ">=", "<>", "=", "<", ">"] {
                if let Some(pos) = part.find(cand) {
                    found = Some((cand, part[..pos].trim()));
                    break;
                }
            }
            match found {
                Some(v) => v,
                None => continue,
            }
        };

        if resolve_attr_name(left.trim_matches('"'), expr_attr_names) == partition_key {
            pk_seen = true;
            if op != "=" {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Query key condition not supported",
                ));
            }
        }
    }

    if !pk_seen {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("Query condition missed key schema element: {partition_key}"),
        ));
    }
    Ok(())
}

/// The `Select` enum set, in the order AWS lists it.
const SELECT_VALUES: &[&str] = &[
    "SPECIFIC_ATTRIBUTES",
    "COUNT",
    "ALL_ATTRIBUTES",
    "ALL_PROJECTED_ATTRIBUTES",
];

/// Check `Select` against the projection parameters and decide whether the
/// operation returns only a count. `is_index_query` is true when an IndexName
/// is present (only then is ALL_PROJECTED_ATTRIBUTES legal). The Select enum
/// itself is validated with the other enum members.
fn resolve_select(
    body: &Value,
    is_index_query: bool,
    is_query: bool,
) -> Result<bool, AwsServiceError> {
    let invalid =
        |m: String| AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", m);
    let projection_param = ["ProjectionExpression", "AttributesToGet"]
        .into_iter()
        .find(|p| !body[*p].is_null());
    let Some(select) = body["Select"].as_str() else {
        return Ok(false);
    };
    if let Some(param) = projection_param {
        let target = match select {
            "ALL_ATTRIBUTES" => Some("ALL_ATTRIBUTES"),
            "ALL_PROJECTED_ATTRIBUTES" => Some("ALL_PROJECTED_ATTRIBUTES"),
            "COUNT" => Some("only the Count"),
            _ => None,
        };
        if let Some(target) = target {
            return Err(invalid(format!(
                "Cannot specify the {param} when choosing to get {target}"
            )));
        }
    }
    if select == "ALL_PROJECTED_ATTRIBUTES" && !is_index_query {
        return Err(invalid(
            "ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName".into(),
        ));
    }
    if select == "SPECIFIC_ATTRIBUTES" && projection_param.is_none() {
        let message = "Must specify the AttributesToGet or ProjectionExpression when choosing \
                       to get SPECIFIC_ATTRIBUTES";
        return Err(if is_query {
            framework_validation_error(&[message.to_string()])
        } else {
            invalid(message.to_string())
        });
    }
    Ok(select == "COUNT")
}

/// The largest page Query and Scan return before paginating.
const MAX_PAGE_BYTES: usize = 1024 * 1024;

/// The largest TotalSegments a parallel Scan accepts.
const MAX_TOTAL_SEGMENTS: i64 = 1_000_000;

fn item_bytes(item: &HashMap<String, AttributeValue>) -> usize {
    item_size(item)
}

/// How many of `rows` fit in one page: at most `limit` rows, ending with the
/// row that brings the data read to 1MB. Always at least one row when any
/// exist.
fn page_length<'a>(
    rows: impl Iterator<Item = &'a HashMap<String, AttributeValue>>,
    limit: Option<usize>,
) -> usize {
    let mut count = 0;
    let mut bytes = 0;
    for row in rows {
        count += 1;
        bytes += item_bytes(row);
        if limit.is_some_and(|l| count >= l) || bytes >= MAX_PAGE_BYTES {
            break;
        }
    }
    count
}

/// Whether an ExclusiveStartKey names exactly the table's key attributes
/// plus the queried index's (`index_keys`), each with its declared type.
fn start_key_matches_schema(
    table: &DynamoTable,
    esk: &HashMap<String, AttributeValue>,
    index_keys: &[&str],
) -> bool {
    let mut required: Vec<&str> = std::iter::once(table.hash_key_name())
        .chain(table.range_key_name())
        .collect();
    for k in index_keys {
        if !required.contains(k) {
            required.push(k);
        }
    }
    if esk.len() != required.len() {
        return false;
    }
    required.iter().all(|attr| {
        let Some(v) = esk.get(*attr) else {
            return false;
        };
        let declared = table
            .attribute_definitions
            .iter()
            .find(|d| d.attribute_name == *attr)
            .map(|d| d.attribute_type.as_str());
        match declared {
            Some(t) => value_type(v) == Some(t),
            None => true,
        }
    })
}

/// Apply a GSI/LSI projection to an item already projected via the
/// caller's `ProjectionExpression`. AWS retains the table's primary key
/// plus the index key; INCLUDE adds the listed non-key attributes;
/// KEYS_ONLY drops everything else; ALL leaves the item alone.
/// The read capacity a Query or Scan consumed.
///
/// Capacity is charged on every row the read examined -- before the filter
/// drops any and whatever the projection returns -- summed and then rounded up
/// to the next 4KB, not per row. A read served by a secondary index is charged
/// to that index, on the entries it stores, and costs the base table nothing.
fn read_consumed(
    table: &crate::state::DynamoTable,
    index_name: Option<&str>,
    examined: &[&HashMap<String, AttributeValue>],
    consistent: bool,
) -> Consumed {
    let gsi = index_name.and_then(|n| table.gsi.iter().find(|g| g.index_name == n));
    let lsi = index_name.and_then(|n| table.lsi.iter().find(|l| l.index_name == n));
    let index = gsi
        .map(|g| (&g.index_name, &g.key_schema, &g.projection))
        .or_else(|| lsi.map(|l| (&l.index_name, &l.key_schema, &l.projection)));
    let Some((name, key_schema, projection)) = index else {
        let bytes = examined.iter().map(|item| item_size(item)).sum();
        return Consumed::table(read_units(bytes, consistent));
    };
    let key_attrs: Vec<String> = key_schema
        .iter()
        .map(|k| k.attribute_name.clone())
        .collect();
    let table_hash = table.hash_key_name();
    let table_range = table.range_key_name();
    let bytes = examined
        .iter()
        .map(|item| {
            item_size(&apply_index_projection(
                (*item).clone(),
                projection,
                &key_attrs,
                table_hash,
                table_range,
            ))
        })
        .sum();
    let mut consumed = Consumed::table(0.0);
    let units = read_units(bytes, consistent);
    if gsi.is_some() {
        consumed.gsi.insert(name.clone(), units);
    } else {
        consumed.lsi.insert(name.clone(), units);
    }
    consumed
}

pub(crate) fn apply_index_projection(
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
    use crate::state::{
        DynamoTable, KeySchemaElement, ProvisionedThroughput, SharedDynamoDbState, TableItems,
    };
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
            items: TableItems::new(items),
            key_index: Default::default(),
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
            vector_indexes: Vec::new(),
            pitr_history: Default::default(),
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
        assert_eq!(
            err.message(),
            "1 validation error detected: Value at 'Limit' failed to satisfy constraint: Member \
             must have value greater than or equal to 1"
        );
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

    // bug-hunt 2026-07-01, finding 1: a Query MUST constrain the partition key
    // with `=`. Omitting it, or using a range operator on it, is a
    // ValidationException — not a silent cross-partition filter.
    #[tokio::test]
    async fn query_requires_partition_key_equality() {
        let state = make_state();
        seed_table(&state, "T", vec![item("a"), item("b")]);
        let svc = DynamoDbService::new(state);

        // Condition references a non-key attribute -> partition key missing.
        let missing = svc
            .query(&req_for(
                "Query",
                json!({
                    "TableName": "T",
                    "KeyConditionExpression": "sk = :v",
                    "ExpressionAttributeValues": {":v": {"S": "a"}},
                }),
            ))
            .err()
            .expect("missing partition key rejected");
        assert!(format!("{missing:?}").contains("missed key schema element"));

        // Range operator on the partition key -> unsupported.
        let non_eq = svc
            .query(&req_for(
                "Query",
                json!({
                    "TableName": "T",
                    "KeyConditionExpression": "pk > :v",
                    "ExpressionAttributeValues": {":v": {"S": "a"}},
                }),
            ))
            .err()
            .expect("non-equality partition key rejected");
        assert!(format!("{non_eq:?}").contains("Query key condition not supported"));
    }

    #[tokio::test]
    async fn query_accepts_partition_key_equality() {
        let state = make_state();
        seed_table(&state, "T", vec![item("a"), item("b")]);
        let svc = DynamoDbService::new(state);
        let resp = svc
            .query(&req_for(
                "Query",
                json!({
                    "TableName": "T",
                    "KeyConditionExpression": "pk = :v",
                    "ExpressionAttributeValues": {":v": {"S": "a"}},
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["Count"].as_i64().unwrap(), 1);
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
        assert!(format!("{err:?}").contains(
            "The TotalSegments parameter is required but was not present in the request when \
             Segment parameter is present"
        ));
    }
}
