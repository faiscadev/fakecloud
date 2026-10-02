use std::collections::HashMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{AttributeValue, DynamoTable};

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
    apply_update_expression, build_capacity, check_put_item_size, check_update_item_size,
    empty_table_key_error, evaluate_condition, extract_key, get_table, get_table_mut,
    index_key_fault, index_key_specs, item_key_type_mismatch, item_size, item_write_consumed,
    key_matches_schema, keys_equal, missing_item_key_error, normalize_item_numbers,
    normalize_value_numbers, parse_expression_attribute_names, parse_expression_attribute_values,
    read_units, return_consumed_mode, return_icm_mode, validate_attribute_value,
    validate_index_keys_in_item, validate_item_attribute_values, validate_key_in_item,
    validate_read_projection, write_units, CapacitySplit, Consumed, DynamoDbService,
    KEY_SCHEMA_MISMATCH,
};

fn schema_mismatch() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        KEY_SCHEMA_MISMATCH,
    )
}

/// The ceiling on the aggregate size of the items one transaction writes.
const MAX_TRANSACTION_BYTES: i64 = 4 * 1024 * 1024;

/// The size of the item a transact Update leaves behind: the update applied
/// to the stored item (or to a key-only item on an upsert). When the
/// expression cannot be applied, the key plus every value the expression
/// could write is counted instead; the apply pass reports that failure.
fn update_write_size(
    table: &DynamoTable,
    op: &Value,
    key: &HashMap<String, AttributeValue>,
) -> i64 {
    let before = table
        .find_item_index(key)
        .map(|i| table.items[i].clone())
        .unwrap_or_else(|| key.clone());
    let names = parse_expression_attribute_names(op);
    let values = parse_expression_attribute_values(op);
    let Some(expr) = op["UpdateExpression"].as_str() else {
        return DynamoTable::estimate_item_size(&before);
    };
    let mut after = before;
    match apply_update_expression(&mut after, expr, &names, &values) {
        Ok(()) => DynamoTable::estimate_item_size(&after),
        Err(_) => DynamoTable::estimate_item_size(key) + DynamoTable::estimate_item_size(&values),
    }
}

/// The single action of a TransactWriteItem union and its member name. The
/// union shape is validated before this is used.
fn transact_op(ti: &Value) -> (&'static str, &Value) {
    static NO_OP: Value = Value::Null;
    ["Put", "Update", "Delete", "ConditionCheck"]
        .into_iter()
        .find_map(|k| ti.get(k).map(|op| (k, op)))
        .unwrap_or(("ConditionCheck", &NO_OP))
}

/// A `ValidationError` cancellation reason.
fn validation_reason(message: &str) -> Value {
    json!({ "Code": "ValidationError", "Message": message })
}

/// Build the TransactionCanceledException response for per-action
/// `reasons`. The message lists every action's code in request order,
/// including `None` for the actions that would have succeeded.
fn transaction_canceled(reasons: Vec<Value>) -> AwsResponse {
    let codes: Vec<&str> = reasons
        .iter()
        .map(|r| r["Code"].as_str().unwrap_or("None"))
        .collect();
    let error_body = json!({
        "__type": "TransactionCanceledException",
        "message": format!(
            "Transaction cancelled, please refer cancellation reasons for specific reasons [{}]",
            codes.join(", ")
        ),
        "CancellationReasons": reasons,
    });
    AwsResponse::json(
        StatusCode::BAD_REQUEST,
        serde_json::to_vec(&error_body).unwrap_or_default(),
    )
}

/// Render a request list the way DynamoDB's validation layer echoes it in a
/// length-constraint message: one Java object reference per member.
fn java_list_dump(class: &str, members: &[Value]) -> String {
    use std::hash::{Hash, Hasher};
    let refs: Vec<String> = members
        .iter()
        .map(|m| {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            m.to_string().hash(&mut h);
            format!(
                "com.amazonaws.dynamodb.v20120810.{class}@{:08x}",
                h.finish() as u32
            )
        })
        .collect();
    format!("[{}]", refs.join(", "))
}

use super::cross_account::{table_id, tables_of, tables_of_mut};

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
        if request_items.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value at 'RequestItems' failed to satisfy \
                 constraint: Member must have length greater than or equal to 1",
            ));
        }

        // Each table's Keys list is capped at 100 by the input model, then
        // the whole request is capped at 100 keys across all tables.
        for (table_name, params) in &request_items {
            if params["Keys"].as_array().is_some_and(|k| k.len() > 100) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value at \
                         'RequestItems.{table_name}.member.Keys' failed to satisfy constraint: \
                         Member must have length less than or equal to 100"
                    ),
                ));
            }
        }
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

        // Projection parameters are validated for every table before any
        // read: one bad entry rejects the whole batch. The request must also
        // use one projection style throughout, not a ProjectionExpression on
        // one table and AttributesToGet on another.
        for params in request_items.values() {
            validate_read_projection(params)?;
        }
        let uses = |field: &str| {
            request_items
                .values()
                .any(|p| p.get(field).is_some_and(|v| !v.is_null()))
        };
        if uses("ProjectionExpression") && uses("AttributesToGet") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Can not use both expression and non-expression parameters in the same \
                 request: Non-expression parameters: {AttributesToGet} Expression parameters: \
                 {ProjectionExpression}",
            ));
        }

        // Each table is looked up in the account that owns it: a table ARN
        // may name another account's table.
        let accounts = self.state.read();
        let mut responses: HashMap<String, Vec<Value>> = HashMap::new();
        let mut consumed_capacity: Vec<Value> = Vec::new();

        for (table_name, params) in &request_items {
            let table = super::get_data_table(tables_of(&accounts, req, table_name), table_name)?;
            let keys = params["Keys"].as_array().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Keys is required",
                )
            })?;
            // AWS rejects an empty Keys list rather than returning an empty,
            // successful response (bug-hunt 2026-07-01, DynamoDB BatchGetItem).
            if keys.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "1 validation error detected: Value '[]' at 'requestItems' failed to \
                     satisfy constraint: Member must have length greater than or equal to 1",
                ));
            }

            let mut items = Vec::new();
            let mut seen_keys: Vec<HashMap<String, AttributeValue>> = Vec::new();
            // Each key is a read of its own, rounded up on its own.
            let consistent = params["ConsistentRead"].as_bool().unwrap_or(false);
            let mut units = 0.0;
            for key_val in keys {
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(key_val.clone()).unwrap_or_default();
                // Reject malformed/under-specified keys instead of coercing
                // them to `{}`: an empty String/Binary key value first, then a
                // key that does not fit the schema.
                if let Some(err) = empty_table_key_error(table, &key) {
                    return Err(err);
                }
                if !key_matches_schema(table, &key) {
                    return Err(schema_mismatch());
                }
                // AWS rejects a Keys list containing duplicate primary keys.
                if seen_keys.iter().any(|k| keys_equal(table, k, &key)) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        "Provided list of item keys contains duplicates",
                    ));
                }
                seen_keys.push(key.clone());
                let found = table.find_item_index(&key).map(|idx| &table.items[idx]);
                units += read_units(found.map_or(0, item_size), consistent);
                if let Some(item) = found {
                    // Honor the per-table ProjectionExpression /
                    // AttributesToGet so callers only get the attributes
                    // they asked for (GetItem already does this).
                    let projected = super::project_item(item, params);
                    items.push(json!(projected));
                }
            }
            responses.insert(table_name.clone(), items);

            let cc = build_capacity(
                &return_consumed,
                table_name,
                &Consumed::table(units),
                CapacitySplit::None,
            );
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
        if request_items.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "The requestItems parameter is required for BatchWriteItem",
            ));
        }

        // Each table's WriteRequest list is capped at 25 by the input model,
        // then the whole request is capped at 25 writes across all tables.
        for (table_name, requests) in &request_items {
            if requests.as_array().is_some_and(|r| r.len() > 25) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value at 'RequestItems.{table_name}.member' \
                         failed to satisfy constraint: Member must have length less than or \
                         equal to 25"
                    ),
                ));
            }
        }
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
        let mut consumed_capacity: Vec<Value> = Vec::new();
        let mut item_collection_metrics: HashMap<String, Vec<Value>> = HashMap::new();

        // Validate every request before mutating any state so a
        // malformed/keyless item or a duplicate key in the batch fails
        // the whole call (AWS rejects these up-front, not after partial
        // application).
        for (table_name, requests) in &request_items {
            let table = super::get_data_table(tables_of(&accounts, req, table_name), table_name)?;
            let reqs = requests.as_array().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Request list must be an array",
                )
            })?;
            let mut seen_keys: Vec<HashMap<String, AttributeValue>> = Vec::new();
            for request in reqs {
                // A WriteRequest is a union: exactly one of PutRequest or
                // DeleteRequest must be set. AWS rejects a request with both or
                // neither; previously the neither case was silently skipped and
                // the both case took the Put branch (bug-hunt 2026-07-01).
                let has_put = request.get("PutRequest").is_some();
                let has_delete = request.get("DeleteRequest").is_some();
                if has_put == has_delete {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ValidationException",
                        "1 validation error detected: Member must contain exactly one \
                         of PutRequest or DeleteRequest",
                    ));
                }
                // BatchWriteItem validates every request up front, so each
                // bad key or index value is a top-level ValidationException;
                // a wrong-typed table key reads as a schema mismatch.
                let key = if let Some(put_req) = request.get("PutRequest") {
                    let item: HashMap<String, AttributeValue> =
                        serde_json::from_value(put_req["Item"].clone()).map_err(|_| {
                            AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "ValidationException",
                                "PutRequest.Item is not a valid item",
                            )
                        })?;
                    if let Some(err) = empty_table_key_error(table, &item) {
                        return Err(err);
                    }
                    if item_key_type_mismatch(table, &item).is_some() {
                        return Err(schema_mismatch());
                    }
                    validate_key_in_item(table, &item)?;
                    validate_index_keys_in_item(table, &item)?;
                    // Reject malformed values (bad numbers, empty/duplicate
                    // sets) up front, before applying any write, so
                    // BatchWriteItem enforces the same per-attribute validation
                    // single PutItem does and a bad item persists nothing.
                    validate_item_attribute_values(&item)?;
                    let mut item = item;
                    normalize_item_numbers(&mut item);
                    check_put_item_size(&item)?;
                    super::vectors::validate_vector_item(
                        &table.vector_indexes,
                        &table.attribute_definitions,
                        &item,
                    )?;
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
                    if let Some(err) = empty_table_key_error(table, &key) {
                        return Err(err);
                    }
                    if !key_matches_schema(table, &key) {
                        return Err(schema_mismatch());
                    }
                    key
                } else {
                    continue;
                };
                // Numeric-aware comparison: {"N":"1"} and {"N":"1.0"} are the
                // same key to DynamoDB, so a raw HashMap `==` would miss that
                // duplicate. keys_equal normalizes numbers (as BatchGetItem
                // already does).
                if seen_keys.iter().any(|k| keys_equal(table, k, &key)) {
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
            let table = tables_of_mut(&mut accounts, req, table_name)
                .get_mut(super::resolve_table_name(table_name))
                .ok_or_else(super::data_table_not_found)?;

            let reqs = requests.as_array().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Request list must be an array",
                )
            })?;

            // Sizing each write (and projecting it into every index) is only
            // worth doing when the caller asked for the figure.
            let wants_capacity = return_consumed != "NONE";
            let mut consumed = Consumed::default();
            let mut keys_for_icm: Vec<HashMap<String, AttributeValue>> = Vec::new();
            for request in reqs {
                if let Some(put_req) = request.get("PutRequest") {
                    let mut item: HashMap<String, AttributeValue> =
                        serde_json::from_value(put_req["Item"].clone()).unwrap_or_default();
                    normalize_item_numbers(&mut item);
                    let key = extract_key(table, &item);
                    keys_for_icm.push(key.clone());
                    if wants_capacity {
                        table.ensure_key_index();
                        let old = table.find_item_index(&key).map(|i| &table.items[i]);
                        consumed.add(&item_write_consumed(table, old, Some(&item)));
                    }
                    table.put_item_at_key(item);
                } else if let Some(del_req) = request.get("DeleteRequest") {
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(del_req["Key"].clone()).unwrap_or_default();
                    keys_for_icm.push(key.clone());
                    if wants_capacity {
                        table.ensure_key_index();
                        let old = table.find_item_index(&key).map(|i| &table.items[i]);
                        consumed.add(&item_write_consumed(table, old, None));
                    }
                    table.remove_item_by_key(&key);
                }
            }

            // No `recalculate_stats()` here: `put_item_at_key` /
            // `remove_item_by_key` keep item_count, size_bytes and the key
            // index current. Re-summing the whole table once per batch was
            // half of the quadratic cost in #2502.

            let cc = build_capacity(&return_consumed, table_name, &consumed, CapacitySplit::None);
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

        // AWS rejects an empty TransactItems list and one over the 100-action
        // ceiling up-front with a ValidationException, mirroring
        // TransactWriteItems. Previously an empty batch returned success and an
        // oversized one was processed in full (bug-hunt 2026-07-01).
        if transact_items.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value '[]' at 'transactItems' \
                 failed to satisfy constraint: Member must have length greater \
                 than or equal to 1",
            ));
        }
        if transact_items.len() > 100 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'transactItems' failed to \
                     satisfy constraint: Member must have length less than or equal to 100",
                    java_list_dump("TransactGetItem", transact_items)
                ),
            ));
        }

        // Projections are parsed before any per-action processing, so a bad
        // one is a request-level ValidationException.
        for ti in transact_items {
            validate_read_projection(&ti["Get"])?;
        }

        // Each table is looked up in the account that owns it: a table ARN
        // may name another account's table.
        let accounts = self.state.read();
        let mut per_table_units: HashMap<String, f64> = HashMap::new();
        let mut seen_keys: Vec<((String, String), HashMap<String, AttributeValue>)> = Vec::new();
        // Per action: the key to read, or the ValidationError that cancels
        // the transaction.
        let mut lookups: Vec<Result<HashMap<String, AttributeValue>, &'static str>> = Vec::new();

        for ti in transact_items {
            let get = &ti["Get"];
            let table_name = get["TableName"].as_str().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "TableName is required in Get",
                )
            })?;

            let table = super::get_data_table(tables_of(&accounts, req, table_name), table_name)?;
            // Parse the Key strictly instead of coercing it to `{}` (which
            // matched nothing and returned a phantom miss).
            let key: HashMap<String, AttributeValue> = serde_json::from_value(get["Key"].clone())
                .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Get.Key is not a valid key",
                )
            })?;
            // An empty String/Binary key value fails DynamoDB's up-front
            // validation; a key that does not fit the schema is caught per
            // action and cancels the transaction instead.
            if let Some(err) = empty_table_key_error(table, &key) {
                return Err(err);
            }
            if !key_matches_schema(table, &key) {
                lookups.push(Err(KEY_SCHEMA_MISMATCH));
                continue;
            }

            // AWS rejects a transaction that reads the same item more than once.
            let id = table_id(req, table_name);
            if seen_keys
                .iter()
                .any(|(t, k)| *t == id && keys_equal(table, k, &key))
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Transaction request cannot include multiple operations on one item",
                ));
            }
            seen_keys.push((id, key.clone()));
            lookups.push(Ok(key));
        }

        if lookups.iter().any(Result::is_err) {
            let reasons: Vec<Value> = lookups
                .iter()
                .map(|l| match l {
                    Ok(_) => json!({ "Code": "None" }),
                    Err(msg) => json!({ "Code": "ValidationError", "Message": msg }),
                })
                .collect();
            return Ok(transaction_canceled(reasons));
        }

        let mut responses: Vec<Value> = Vec::new();
        for (ti, key) in transact_items.iter().zip(lookups) {
            let get = &ti["Get"];
            let table_name = get["TableName"].as_str().unwrap_or_default();
            let table = super::get_data_table(tables_of(&accounts, req, table_name), table_name)?;
            let key = key.unwrap_or_default();
            let found = table.find_item_index(&key).map(|idx| &table.items[idx]);
            // A present item whose projection selects nothing is reported
            // like a missing one: the response omits Item entirely.
            let projected = found
                .map(|item| super::project_item(item, get))
                .filter(|item| !item.is_empty());
            match projected {
                Some(item) => responses.push(json!({ "Item": item })),
                None => responses.push(json!({})),
            }
            // A transactional read costs twice a strongly-consistent one.
            *per_table_units.entry(table_name.to_string()).or_insert(0.0) +=
                2.0 * read_units(found.map_or(0, item_size), true);
        }

        let mut result = json!({ "Responses": responses });
        let consumed: Vec<Value> = per_table_units
            .iter()
            .filter_map(|(t, units)| {
                let cc = build_capacity(
                    &return_consumed,
                    t,
                    &Consumed::table(*units),
                    CapacitySplit::Read,
                );
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

        // Idempotency: a retried transaction carrying the same ClientRequestToken
        // (within the window) must be applied at most once and replay the
        // original result. Reusing a token with a different body is rejected.
        // The hash covers the whole request body, so the token field itself is
        // part of the identity — only an identical retry replays. The actual
        // check-and-reserve happens under the state write lock below so that a
        // concurrent same-token retry serializes with (and replays) the apply
        // instead of double-applying across the lock gap.
        let client_token = body["ClientRequestToken"].as_str().map(str::to_string);
        let request_hash = {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            req.body.hash(&mut h);
            h.finish()
        };

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

        // AWS rejects an empty transaction and one over the 100-action ceiling
        // up-front with a ValidationException; previously both were silently
        // accepted (an empty transaction returned success, an oversized one
        // applied every action).
        if transact_items.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value '[]' at 'transactItems' \
                 failed to satisfy constraint: Member must have length greater \
                 than or equal to 1",
            ));
        }
        if transact_items.len() > 100 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "1 validation error detected: Value '{}' at 'transactItems' failed to \
                     satisfy constraint: Member must have length less than or equal to 100",
                    java_list_dump("TransactWriteItem", transact_items)
                ),
            ));
        }

        // Each TransactWriteItem is a union: exactly one of Put / Update /
        // Delete / ConditionCheck must be set. AWS rejects an item with zero or
        // more than one member with a ValidationException; previously a
        // zero-member item was silently treated as a no-op and a multi-member
        // item processed only its first branch (bug-hunt 2026-07-01).
        for ti in transact_items {
            let op_count = ["Put", "Update", "Delete", "ConditionCheck"]
                .iter()
                .filter(|k| ti.get(**k).is_some())
                .count();
            if op_count != 1 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "1 validation error detected: Member must contain exactly one \
                     of Put, Update, Delete, or ConditionCheck",
                ));
            }
        }

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

        // Idempotency check-and-reserve under the same write lock that guards
        // the apply below. Doing the lookup here (rather than before taking the
        // lock) means two concurrent retries carrying the same token serialize
        // on this lock: the first applies and stores its result while holding
        // the lock, so the second observes the cached outcome and replays it
        // instead of applying the transaction a second time.
        if let Some(token) = client_token.as_deref() {
            if let Some(cached) =
                self.transact_idempotency_lookup(&req.account_id, token, request_hash)?
            {
                return Ok(cached);
            }
        }

        // Each table is looked up, validated, snapshotted and written in the
        // account that owns it: a table ARN may name another account's table.

        // Validate every referenced table exists up-front. Without this
        // check a missing TableName on a Put with no condition would fail
        // partway through the apply loop and leave earlier writes
        // committed — TransactWriteItems must be all-or-nothing.
        for ti in transact_items {
            let (_, op) = transact_op(ti);
            let table_name = op["TableName"].as_str().unwrap_or_default();
            super::get_data_table(tables_of(&accounts, req, table_name), table_name)?;
        }

        // DynamoDB's up-front input validation, run on every action before
        // the transaction executes. Everything it catches is a top-level
        // ValidationException, never a cancellation reason: a missing key
        // attribute, an empty String/Binary table or secondary-index key
        // value, a malformed attribute value, an update that writes a key
        // attribute. Wrong-typed keys are NOT caught here; they cancel the
        // transaction below.
        let mut transaction_bytes: i64 = 0;
        for ti in transact_items {
            let (op_key, op) = transact_op(ti);
            let table_name = op["TableName"].as_str().unwrap_or_default();
            let table = super::get_data_table(tables_of(&accounts, req, table_name), table_name)?;
            if op_key == "Put" {
                let item: HashMap<String, AttributeValue> =
                    serde_json::from_value(op["Item"].clone()).unwrap_or_default();
                if let Some(err) = missing_item_key_error(table, &item) {
                    return Err(err);
                }
                if let Some(err) = empty_table_key_error(table, &item) {
                    return Err(err);
                }
                if let Some(fault) = index_key_fault(&index_key_specs(table), &item, None) {
                    if fault.is_empty_value() {
                        return Err(fault.put_error());
                    }
                }
                validate_item_attribute_values(&item)?;
                // A Put's size is known from the request alone, so an item
                // over the limit is refused before the transaction opens.
                check_put_item_size(&item)?;
                // Vector indexes judge the item last, in PutItem's order.
                super::vectors::validate_vector_item(
                    &table.vector_indexes,
                    &table.attribute_definitions,
                    &item,
                )?;
                transaction_bytes += DynamoTable::estimate_item_size(&item);
            } else {
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(op["Key"].clone()).unwrap_or_default();
                if let Some(err) = empty_table_key_error(table, &key) {
                    return Err(err);
                }
                if op_key == "Update" {
                    if let Some(expr) = op["UpdateExpression"].as_str() {
                        super::reject_key_attribute_update_expression(
                            table,
                            expr,
                            &parse_expression_attribute_names(op),
                        )?;
                    }
                    // An Update's ExpressionAttributeValues get the same value
                    // validation single UpdateItem enforces, so a malformed
                    // number or empty/duplicate set never reaches an item.
                    for v in parse_expression_attribute_values(op).values() {
                        validate_attribute_value(v)?;
                    }
                }
                transaction_bytes += match op_key {
                    "Update" => update_write_size(table, op, &key),
                    "Delete" => DynamoTable::estimate_item_size(&key),
                    // A ConditionCheck reads an item but writes nothing.
                    _ => 0,
                };
            }
        }
        // The items a transaction writes may total at most 4 MB: a Put's
        // item, an Update's resulting item, a Delete's key.
        if transaction_bytes > MAX_TRANSACTION_BYTES {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Transaction request cannot include more than 4 MB of data",
            ));
        }

        // AWS rejects a transaction that targets the same item more than once
        // (by table + primary key) with a ValidationException; previously such
        // a transaction applied last-writer-wins and reported success. The key
        // is the table's primary key, extracted from a Put's Item or the
        // Key field of Update/Delete/ConditionCheck.
        let mut seen_keys: Vec<((String, String), HashMap<String, AttributeValue>)> = Vec::new();
        for ti in transact_items {
            let (op_key, op) = transact_op(ti);
            let table_name = op["TableName"].as_str().unwrap_or_default();
            let table = super::get_data_table(tables_of(&accounts, req, table_name), table_name)?;
            let key = if op_key == "Put" {
                let item: HashMap<String, AttributeValue> =
                    serde_json::from_value(op["Item"].clone()).unwrap_or_default();
                extract_key(table, &item)
            } else {
                serde_json::from_value(op["Key"].clone()).unwrap_or_default()
            };
            let id = table_id(req, table_name);
            if seen_keys
                .iter()
                .any(|(t, k)| *t == id && keys_equal(table, k, &key))
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Transaction request cannot include multiple operations on one item",
                ));
            }
            seen_keys.push((id, key));
        }

        // First pass: judge every action without writing. Each gets exactly
        // one reason, so `CancellationReasons` aligns 1:1 with
        // `TransactItems` even when several actions fail: `ValidationError`
        // for a wrong-typed table or index key (caught while the transaction
        // executes, unlike the up-front checks above),
        // `ConditionalCheckFailed` for a failed ConditionExpression, `None`
        // otherwise. With `ReturnValuesOnConditionCheckFailure=ALL_OLD` a
        // failed condition surfaces the existing item under the reason's
        // `Item` field, as aws-sdk-go's
        // `ConditionalCheckFailedException.Item` expects.
        let mut cancellation_reasons: Vec<Value> = Vec::with_capacity(transact_items.len());
        let mut per_table_writes: HashMap<String, u32> = HashMap::new();
        // What each table's actions consumed, and what re-reading their
        // results costs when an idempotent retry replays the transaction.
        let mut per_table_consumed: HashMap<String, Consumed> = HashMap::new();
        let mut per_table_replay: HashMap<String, f64> = HashMap::new();
        let wants_capacity = return_consumed != "NONE";

        for ti in transact_items {
            let (op_key, op) = transact_op(ti);
            let table_name = op["TableName"].as_str().unwrap_or_default();
            let table = super::get_data_table(tables_of(&accounts, req, table_name), table_name)?;
            let key: HashMap<String, AttributeValue> = if op_key == "Put" {
                let item: HashMap<String, AttributeValue> =
                    serde_json::from_value(op["Item"].clone()).unwrap_or_default();
                if let Some(msg) = item_key_type_mismatch(table, &item) {
                    cancellation_reasons.push(validation_reason(&msg));
                    continue;
                }
                if let Some(fault) = index_key_fault(&index_key_specs(table), &item, None) {
                    cancellation_reasons.push(validation_reason(&fault.put_message()));
                    continue;
                }
                extract_key(table, &item)
            } else {
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(op["Key"].clone()).unwrap_or_default();
                if !key_matches_schema(table, &key) {
                    cancellation_reasons.push(validation_reason(KEY_SCHEMA_MISMATCH));
                    continue;
                }
                key
            };
            let existing = table.find_item_index(&key).map(|i| &table.items[i]);
            let expr_attr_names = parse_expression_attribute_names(op);
            let expr_attr_values = parse_expression_attribute_values(op);

            // Dry-run an Update against the current item to judge the index
            // key values it would write. An empty value is DynamoDB's
            // up-front validation and fails the whole request; a wrong type
            // cancels. A failure to apply the expression itself is left to
            // the apply pass, which cancels with the underlying error.
            if op_key == "Update" {
                if let Some(expr) = op["UpdateExpression"].as_str() {
                    let before = existing.cloned().unwrap_or_else(|| key.clone());
                    let mut after = before.clone();
                    if apply_update_expression(
                        &mut after,
                        expr,
                        &expr_attr_names,
                        &expr_attr_values,
                    )
                    .is_ok()
                    {
                        let specs = index_key_specs(table);
                        if let Some(fault) = index_key_fault(&specs, &after, Some(&before)) {
                            if fault.is_empty_value() {
                                return Err(fault.update_error());
                            }
                            cancellation_reasons.push(validation_reason(&fault.update_message()));
                            continue;
                        }
                    }
                }
            }

            let condition = if op_key == "ConditionCheck" {
                Some(op["ConditionExpression"].as_str().unwrap_or_default())
            } else {
                op["ConditionExpression"].as_str()
            };
            let failed = condition.is_some_and(|cond| {
                evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values).is_err()
            });
            if failed {
                let mut reason = json!({
                    "Code": "ConditionalCheckFailed",
                    "Message": "The conditional request failed",
                });
                if op["ReturnValuesOnConditionCheckFailure"].as_str() == Some("ALL_OLD") {
                    if let Some(item) = existing {
                        reason["Item"] = json!(item);
                    }
                }
                cancellation_reasons.push(reason);
            } else {
                cancellation_reasons.push(json!({ "Code": "None" }));
            }
        }

        if cancellation_reasons.iter().any(|r| r["Code"] != "None") {
            return Ok(transaction_canceled(cancellation_reasons));
        }

        // Snapshot the items vector of every referenced table so we can
        // revert on any apply-phase failure (e.g. an unparseable
        // UpdateExpression). DDB transactions are all-or-nothing — without
        // this, an UpdateExpression error after a successful Put would
        // leave the Put committed.
        // Each touched table's rows and point-in-time history length, so a
        // revert also drops the history entries the partial writes recorded.
        #[allow(clippy::type_complexity)]
        let mut snapshots: HashMap<
            (String, String),
            (Vec<HashMap<String, AttributeValue>>, usize),
        > = HashMap::new();
        for ti in transact_items {
            for op_key in ["Put", "Delete", "Update"] {
                if let Some(op) = ti.get(op_key) {
                    // Keyed by owner account and resolved name, so a table
                    // named once by name and once by ARN is snapshotted, and
                    // reverted, once.
                    let table_name = op["TableName"].as_str().unwrap_or_default();
                    snapshots
                        .entry(table_id(req, table_name))
                        .or_insert_with(|| {
                            tables_of(&accounts, req, table_name)
                                .get(super::resolve_table_name(table_name))
                                .map(|t| (t.items.to_vec(), t.change_count()))
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
                    let mut item: HashMap<String, AttributeValue> =
                        serde_json::from_value(put["Item"].clone()).unwrap_or_default();
                    normalize_item_numbers(&mut item);
                    let table =
                        get_table_mut(tables_of_mut(&mut accounts, req, table_name), table_name)
                            .map_err(|e| (op_idx, e))?;
                    let key = extract_key(table, &item);
                    let old_image = table.find_item_index(&key).map(|i| table.items[i].clone());
                    let is_modify = old_image.is_some();
                    if wants_capacity {
                        per_table_consumed
                            .entry(table_name.to_string())
                            .or_default()
                            .add(&item_write_consumed(table, old_image.as_ref(), Some(&item)));
                        *per_table_replay.entry(table_name.to_string()).or_default() +=
                            2.0 * read_units(item_size(&item), true);
                    }
                    table.put_item_at_key(item.clone());
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
                        get_table_mut(tables_of_mut(&mut accounts, req, table_name), table_name)
                            .map_err(|e| (op_idx, e))?;
                    let old_image = table.find_item_index(&key).map(|i| table.items[i].clone());
                    if wants_capacity {
                        per_table_consumed
                            .entry(table_name.to_string())
                            .or_default()
                            .add(&item_write_consumed(table, old_image.as_ref(), None));
                        *per_table_replay.entry(table_name.to_string()).or_default() +=
                            2.0 * read_units(old_image.as_ref().map_or(0, item_size), true);
                    }
                    table.remove_item_by_key(&key);
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
                    let mut expr_attr_values = parse_expression_attribute_values(update);
                    // Only the values the update writes are normalized; the
                    // rest of the stored row is left as it is.
                    for v in expr_attr_values.values_mut() {
                        normalize_value_numbers(v);
                    }

                    let table =
                        get_table_mut(tables_of_mut(&mut accounts, req, table_name), table_name)
                            .map_err(|e| (op_idx, e))?;
                    // The `&self` lookups below cannot build the index, so a
                    // restored table would scan on every transactional update
                    // without this.
                    table.ensure_key_index();
                    let existing_idx = table.find_item_index(&key);
                    let old_image = existing_idx.map(|i| table.items[i].clone());
                    let is_modify = old_image.is_some();
                    let idx = match existing_idx {
                        Some(i) => i,
                        None => {
                            let mut new_item = HashMap::new();
                            for (k, v) in &key {
                                new_item.insert(k.clone(), v.clone());
                            }
                            normalize_item_numbers(&mut new_item);
                            table.put_item_at_key(new_item).0
                        }
                    };
                    // A failure here cancels the whole transaction, and the
                    // revert below restores every touched table. An Update's
                    // size depends on the stored item, so one over the limit
                    // is measured here, flat against the finished item, and
                    // cancels rather than failing up front.
                    let vector_indexes = table.vector_indexes.clone();
                    let vector_defs = table.attribute_definitions.clone();
                    table
                        .update_item_at(idx, |item| {
                            if let Some(expr) = update_expression {
                                apply_update_expression(
                                    item,
                                    expr,
                                    &expr_attr_names,
                                    &expr_attr_values,
                                )?;
                            }
                            check_update_item_size(item)?;
                            super::vectors::validate_vector_item(
                                &vector_indexes,
                                &vector_defs,
                                item,
                            )
                        })
                        .map_err(|e| (op_idx, e))?;
                    let new_image = table.items[idx].clone();
                    if wants_capacity {
                        per_table_consumed
                            .entry(table_name.to_string())
                            .or_default()
                            .add(&item_write_consumed(
                                table,
                                old_image.as_ref(),
                                Some(&new_image),
                            ));
                        *per_table_replay.entry(table_name.to_string()).or_default() +=
                            2.0 * read_units(item_size(&new_image), true);
                    }
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
                } else if let Some(check) = ti.get("ConditionCheck").filter(|_| wants_capacity) {
                    // No write, but a ConditionCheck is billed as a
                    // transactional write of the item it checks.
                    let table_name = check["TableName"].as_str().unwrap_or_default();
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(check["Key"].clone()).unwrap_or_default();
                    let table = get_table(tables_of(&accounts, req, table_name), table_name)
                        .map_err(|e| (op_idx, e))?;
                    let bytes = table
                        .find_item_index(&key)
                        .map_or(0, |i| item_size(&table.items[i]));
                    per_table_consumed
                        .entry(table_name.to_string())
                        .or_default()
                        .add(&Consumed::table(write_units(bytes)));
                    *per_table_replay.entry(table_name.to_string()).or_default() +=
                        2.0 * read_units(bytes, true);
                }
            }
            Ok(())
        })();

        if let Err((failed_idx, err)) = apply_result {
            // Revert items on every touched table so the partial writes
            // before the failure leave no observable side effects, then
            // surface the failure as a TransactionCanceledException
            // whose CancellationReasons array marks the offending op
            // with `ValidationError` and leaves siblings as `None`.
            for ((account, table_name), (items, change_count)) in snapshots {
                if let Some(table) = accounts
                    .get_mut(&account)
                    .and_then(|state| state.tables.get_mut(&table_name))
                {
                    table.replace_items(items);
                    table.truncate_changes(change_count);
                }
            }
            let reasons: Vec<Value> = (0..transact_items.len())
                .map(|i| {
                    if i == failed_idx {
                        validation_reason(&err.message())
                    } else {
                        json!({ "Code": "None" })
                    }
                })
                .collect();
            return Ok(transaction_canceled(reasons));
        }

        // Append all pending stream records under each table's
        // stream_records lock now that the transaction has committed.
        for (table_name, record) in pending_stream {
            if let Some(table) = tables_of_mut(&mut accounts, req, &table_name)
                .get_mut(super::resolve_table_name(&table_name))
            {
                crate::streams::add_stream_record(table, record);
            }
        }

        let mut result = json!({});
        // A transactional write costs twice a standard one, reported with the
        // write split.
        let consumed: Vec<Value> = per_table_consumed
            .iter()
            .map(|(t, c)| {
                build_capacity(
                    &return_consumed,
                    t,
                    &c.clone().scaled(2.0),
                    CapacitySplit::Write,
                )
            })
            .filter(|cc| !cc.is_null())
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

        // Cache the committed outcome while still holding the state write lock
        // so the store is atomic with the apply: an identical retry with the
        // same ClientRequestToken replays this result rather than re-applying.
        // A replay does not write again: it re-reads the stored result, so it
        // reports transactional read capacity sized on the items instead of
        // the write capacity the first call reported.
        if let Some(token) = client_token.as_deref() {
            let mut replay = result.clone();
            let replay_consumed: Vec<Value> = per_table_replay
                .iter()
                .map(|(t, units)| {
                    build_capacity(
                        &return_consumed,
                        t,
                        &Consumed::table(*units),
                        CapacitySplit::Read,
                    )
                })
                .filter(|cc| !cc.is_null())
                .collect();
            if !replay_consumed.is_empty() {
                replay["ConsumedCapacity"] = json!(replay_consumed);
            }
            self.transact_idempotency_store(&req.account_id, token, request_hash, &replay);
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

    fn response_body(response: &AwsResponse) -> Value {
        serde_json::from_slice(response.body.expect_bytes()).unwrap()
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
            items: Default::default(),
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
            vector_indexes: Vec::new(),
            pitr_history: Default::default(),
        };
        s.tables.insert(name.to_string(), table);
    }

    /// ExecuteStatement's NextToken must resume a SELECT by key. It was an
    /// offset into the result set, so deleting rows a page had already
    /// returned shifted every later row down and the next page skipped rows.
    #[tokio::test]
    async fn execute_statement_select_pages_survive_deletes_between_pages() {
        select_pages_survive_deletes("Widgets").await;
    }

    /// Same, naming the table by ARN, which the statement accepts: the pager
    /// must find the table too, or it loses the key cursor.
    #[tokio::test]
    async fn execute_statement_select_pages_by_table_arn_survive_deletes() {
        select_pages_survive_deletes("arn:aws:dynamodb:us-east-1:123456789012:table/Widgets").await;
    }

    async fn select_pages_survive_deletes(table_ref: &str) {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        {
            let mut accts = state.write();
            let table = accts
                .get_or_create("123456789012")
                .tables
                .get_mut("Widgets")
                .unwrap();
            for i in 0..14 {
                let mut item = HashMap::new();
                item.insert("pk".to_string(), json!({ "S": format!("w{i:02}") }));
                table.put_item_at_key(item);
            }
        }
        let svc = DynamoDbService::new(state.clone());
        let all_rows: Vec<String> = (0..14).map(|i| format!("w{i:02}")).collect();
        // Deleted ahead of the pager, on a schedule fixed up front.
        let mut ahead = ["w13", "w02", "w09"].into_iter();
        let mut deleted_unseen = Vec::new();
        let mut delivered: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut body =
                json!({"Statement": format!("SELECT * FROM \"{table_ref}\""), "Limit": 3});
            if let Some(t) = &token {
                body["NextToken"] = json!(t);
            }
            let resp = response_body(
                &svc.execute_statement(&req_for("ExecuteStatement", body))
                    .unwrap(),
            );
            let page: Vec<String> = resp["Items"]
                .as_array()
                .unwrap()
                .iter()
                .map(|item| item["pk"]["S"].as_str().unwrap().to_string())
                .collect();
            delivered.extend(page.iter().cloned());
            token = resp["NextToken"].as_str().map(str::to_string);
            if token.is_none() {
                break;
            }
            let mut accts = state.write();
            let table = accts
                .get_or_create("123456789012")
                .tables
                .get_mut("Widgets")
                .unwrap();
            let mut delete = |pk: &str| {
                let mut key = HashMap::new();
                key.insert("pk".to_string(), json!({ "S": pk }));
                table.remove_item_by_key(&key);
            };
            // Every row this page returned, including the one the cursor names...
            for pk in &page {
                delete(pk);
            }
            // ...and one row ahead, unless a page already returned it.
            if let Some(pk) = ahead.next() {
                if !delivered.iter().any(|d| d == pk) {
                    delete(pk);
                    deleted_unseen.push(pk.to_string());
                }
            }
        }
        let mut unique = delivered.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), delivered.len(), "repeated: {delivered:?}");
        let expected: Vec<String> = all_rows
            .into_iter()
            .filter(|pk| !deleted_unseen.contains(pk))
            .collect();
        assert_eq!(unique, expected, "skipped a surviving row");
        assert!(!deleted_unseen.is_empty());
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

    #[tokio::test]
    async fn batch_get_item_rejects_empty_and_duplicate_keys() {
        // bug-hunt 2026-07-01: empty Keys and duplicate keys are both
        // ValidationExceptions, not a silent success / doubled item.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);

        let empty = svc
            .batch_get_item(&req_for(
                "BatchGetItem",
                json!({"RequestItems": {"Widgets": {"Keys": []}}}),
            ))
            .err()
            .expect("empty Keys rejected");
        assert!(format!("{empty:?}").contains("ValidationException"));

        let dup = svc
            .batch_get_item(&req_for(
                "BatchGetItem",
                json!({"RequestItems": {"Widgets": {"Keys": [
                    {"pk": {"S": "a"}}, {"pk": {"S": "a"}}
                ]}}}),
            ))
            .err()
            .expect("duplicate keys rejected");
        assert!(format!("{dup:?}").contains("ValidationException"));
    }

    #[tokio::test]
    async fn transact_write_items_validates_put_key() {
        // A Put whose Item lacks the primary key is a ValidationException, not a
        // stored orphan row (bug-hunt 2026-07-01).
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        let err = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"notpk": {"S": "x"}}}}
                ]}),
            ))
            .err()
            .expect("missing-key Put rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
        // No orphan row was stored.
        assert!(state.read().get("123456789012").unwrap().tables["Widgets"]
            .items
            .is_empty());
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

    // bug-hunt 2026-07-22: BatchWriteItem PutRequest must run the same
    // per-attribute value validation single PutItem does. A malformed number
    // is rejected with ValidationException and nothing is persisted.
    #[tokio::test]
    async fn batch_write_item_rejects_malformed_value() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        let err = svc
            .batch_write_item(&req_for(
                "BatchWriteItem",
                json!({"RequestItems": {"Widgets": [
                    {"PutRequest": {"Item": {"pk": {"S": "a"}, "n": {"N": "abc"}}}},
                ]}}),
            ))
            .err()
            .expect("malformed number rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
        // All-or-nothing up front: nothing persisted.
        let accts = state.read();
        let table = accts
            .get("123456789012")
            .unwrap()
            .tables
            .get("Widgets")
            .unwrap();
        assert_eq!(table.items.len(), 0);
    }

    #[tokio::test]
    async fn batch_write_item_valid_batch_still_succeeds() {
        // Regression guard: a well-formed batch is unaffected by the new
        // per-value validation.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        svc.batch_write_item(&req_for(
            "BatchWriteItem",
            json!({"RequestItems": {"Widgets": [
                {"PutRequest": {"Item": {"pk": {"S": "a"}, "n": {"N": "1"}, "s": {"SS": ["x", "y"]}}}},
                {"PutRequest": {"Item": {"pk": {"S": "b"}, "n": {"N": "2.5"}}}},
            ]}}),
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
    }

    // bug-hunt 2026-07-22: TransactWriteItems Put must reject a malformed value
    // (here an empty set) with ValidationException, persisting nothing.
    #[tokio::test]
    async fn transact_write_items_put_rejects_empty_set() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        let err = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}, "s": {"SS": []}}}},
                ]}),
            ))
            .err()
            .expect("empty set rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
        let accts = state.read();
        let table = accts
            .get("123456789012")
            .unwrap()
            .tables
            .get("Widgets")
            .unwrap();
        assert_eq!(table.items.len(), 0);
    }

    #[tokio::test]
    async fn transact_write_items_put_rejects_duplicate_set_member() {
        // A Number Set with two numerically-equal members ("1"/"1.0") is a
        // duplicate-member set: rejected with ValidationException.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        let err = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}, "ns": {"NS": ["1", "1.0"]}}}},
                ]}),
            ))
            .err()
            .expect("duplicate-member set rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
    }

    #[tokio::test]
    async fn transact_write_items_update_validates_expr_attr_values() {
        // The Update path's ExpressionAttributeValues get the same validation
        // single UpdateItem enforces: a malformed number is rejected.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        let err = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [
                    {"Update": {
                        "TableName": "Widgets",
                        "Key": {"pk": {"S": "a"}},
                        "UpdateExpression": "SET #c = :bad",
                        "ExpressionAttributeNames": {"#c": "c"},
                        "ExpressionAttributeValues": {":bad": {"N": "abc"}},
                    }},
                ]}),
            ))
            .err()
            .expect("malformed expr value rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
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
    async fn transact_write_same_client_token_applies_once() {
        // A retried TransactWriteItems carrying the same ClientRequestToken must
        // be applied at most once (idempotent replay), not double-applied. An
        // ADD that increments a counter must move it by exactly 1 across two
        // identical calls, and the second call must still return success.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        let body = json!({
            "ClientRequestToken": "tok-fixed-1",
            "TransactItems": [
                {"Update": {
                    "TableName": "Widgets",
                    "Key": {"pk": {"S": "counter"}},
                    "UpdateExpression": "ADD #c :inc",
                    "ExpressionAttributeNames": {"#c": "count"},
                    "ExpressionAttributeValues": {":inc": {"N": "1"}}
                }}
            ]
        });

        svc.transact_write_items(&req_for("TransactWriteItems", body.clone()))
            .unwrap();
        // Second call with the same token + body: replays, does not re-apply.
        let resp = svc
            .transact_write_items(&req_for("TransactWriteItems", body))
            .unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        let item = table
            .items
            .iter()
            .find(|i| i["pk"]["S"] == json!("counter"))
            .expect("counter item");
        assert_eq!(
            item["count"]["N"],
            json!("1"),
            "counter must be incremented exactly once for a replayed token"
        );
    }

    #[tokio::test]
    async fn transact_write_distinct_tokens_apply_each_time() {
        // Control for the idempotency test: two calls with DIFFERENT tokens are
        // two separate transactions, so the ADD increments the counter twice.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        for tok in ["tok-a", "tok-b"] {
            let body = json!({
                "ClientRequestToken": tok,
                "TransactItems": [
                    {"Update": {
                        "TableName": "Widgets",
                        "Key": {"pk": {"S": "counter"}},
                        "UpdateExpression": "ADD #c :inc",
                        "ExpressionAttributeNames": {"#c": "count"},
                        "ExpressionAttributeValues": {":inc": {"N": "1"}}
                    }}
                ]
            });
            svc.transact_write_items(&req_for("TransactWriteItems", body))
                .unwrap();
        }

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        let item = table
            .items
            .iter()
            .find(|i| i["pk"]["S"] == json!("counter"))
            .expect("counter item");
        assert_eq!(item["count"]["N"], json!("2"));
    }

    #[tokio::test]
    async fn execute_transaction_same_client_token_applies_once() {
        // ExecuteTransaction shares the same idempotency machinery: a retried
        // INSERT with the same ClientRequestToken replays success instead of
        // re-applying (which would otherwise cancel with a DuplicateItem).
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        let body = json!({
            "ClientRequestToken": "tok-exec-1",
            "TransactStatements": [
                {"Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}"},
            ]
        });

        let resp = svc
            .execute_transaction(&req_for("ExecuteTransaction", body.clone()))
            .unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
        // Replay: same token + body must succeed and not insert a duplicate.
        let resp = svc
            .execute_transaction(&req_for("ExecuteTransaction", body))
            .unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);

        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("Widgets").unwrap();
        assert_eq!(
            table.items.len(),
            1,
            "a replayed ExecuteTransaction must not apply the INSERT twice"
        );
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
    async fn transact_write_rejects_empty_oversized_and_duplicate_keys() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());

        let err_code =
            |body: Value| match svc.transact_write_items(&req_for("TransactWriteItems", body)) {
                Ok(_) => panic!("transaction must be rejected"),
                Err(e) => e.code().to_string(),
            };

        // Empty transaction.
        assert_eq!(
            err_code(json!({"TransactItems": []})),
            "ValidationException"
        );

        // Over the 100-action ceiling.
        let many: Vec<Value> = (0..101)
            .map(|i| json!({"Put": {"TableName": "Widgets", "Item": {"pk": {"S": i.to_string()}}}}))
            .collect();
        assert_eq!(
            err_code(json!({"TransactItems": many})),
            "ValidationException"
        );

        // Two operations on the same item key.
        assert_eq!(
            err_code(json!({
                "TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "x"}}}},
                    {"Delete": {"TableName": "Widgets", "Key": {"pk": {"S": "x"}}}},
                ]
            })),
            "ValidationException"
        );

        // Sanity: nothing committed.
        assert_eq!(
            state
                .read()
                .get("123456789012")
                .unwrap()
                .tables
                .get("Widgets")
                .unwrap()
                .items
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn reverted_transaction_leaves_no_pitr_history() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        state
            .write()
            .get_or_create("123456789012")
            .tables
            .get_mut("Widgets")
            .unwrap()
            .set_pitr(true);
        let svc = DynamoDbService::new(state.clone());
        svc.transact_write_items(&req_for(
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {"Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}}}},
                    {"Update": {
                        "TableName": "Widgets",
                        "Key": {"pk": {"S": "b"}},
                        "UpdateExpression": "BOGUS expression that won't parse"
                    }},
                ]
            }),
        ))
        .unwrap();
        let accts = state.read();
        let table = &accts.get("123456789012").unwrap().tables["Widgets"];
        assert_eq!(table.items.len(), 0);
        assert_eq!(
            table.change_count(),
            0,
            "a reverted transaction must not leave writes in the PITR history"
        );
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
                            "Key": {"pk": {"S": "b"}},
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

    /// A statement may name its table by ARN. ExecuteStatement,
    /// BatchExecuteStatement and ExecuteTransaction all resolve it to find
    /// the table they append stream records to, or the write lands with no
    /// change record.
    #[tokio::test]
    async fn partiql_writes_naming_the_table_by_arn_emit_stream_records() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        let arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Widgets";

        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({"Statement": format!("INSERT INTO \"{arn}\" VALUE {{'pk': 'a'}}")}),
        ))
        .unwrap();
        svc.batch_execute_statement(&req_for(
            "BatchExecuteStatement",
            json!({"Statements": [
                {"Statement": format!("INSERT INTO \"{arn}\" VALUE {{'pk': 'b'}}")}
            ]}),
        ))
        .unwrap();
        svc.execute_transaction(&req_for(
            "ExecuteTransaction",
            json!({"TransactStatements": [
                {"Statement": format!("INSERT INTO \"{arn}\" VALUE {{'pk': 'c'}}")}
            ]}),
        ))
        .unwrap();

        let accts = state.read();
        let table = &accts.get("123456789012").unwrap().tables["Widgets"];
        assert_eq!(table.items.len(), 3);
        assert_eq!(table.stream_records.read().len(), 3);
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

    #[test]
    fn batch_execute_statement_returns_single_items_and_updates() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);

        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({ "Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}" }),
        ))
        .unwrap();

        let select = svc
            .batch_execute_statement(&req_for(
                "BatchExecuteStatement",
                json!({
                    "Statements": [{
                        "Statement": "SELECT * FROM \"Widgets\" WHERE \"pk\" = ?",
                        "Parameters": [{ "S": "a" }]
                    }]
                }),
            ))
            .unwrap();
        let select_body = response_body(&select);
        assert_eq!(select_body["Responses"][0]["Item"]["pk"]["S"], "a");

        let scan = svc
            .batch_execute_statement(&req_for(
                "BatchExecuteStatement",
                json!({ "Statements": [{ "Statement": "SELECT * FROM \"Widgets\"" }] }),
            ))
            .unwrap();
        let scan_body = response_body(&scan);
        assert_eq!(
            scan_body["Responses"][0]["Error"]["Message"],
            "Select statements within BatchExecuteStatement must specify the primary key in the where clause."
        );

        let non_key_select = svc
            .batch_execute_statement(&req_for(
                "BatchExecuteStatement",
                json!({
                    "Statements": [{
                        "Statement": "SELECT * FROM \"Widgets\" WHERE data = ?",
                        "Parameters": [{ "S": "missing" }]
                    }]
                }),
            ))
            .unwrap();
        assert_eq!(
            response_body(&non_key_select)["Responses"][0]["Error"]["Message"],
            "Select statements within BatchExecuteStatement must specify the primary key in the where clause."
        );

        let update = svc
            .batch_execute_statement(&req_for(
                "BatchExecuteStatement",
                json!({
                    "Statements": [{
                        "Statement": "UPDATE \"Widgets\" SET \"results\" = list_append(if_not_exists(\"results\", ?), ?) SET \"updatedAt\" = ? WHERE \"pk\" = ? RETURNING ALL NEW *",
                        "Parameters": [
                            { "L": [] },
                            { "L": [{ "S": "new" }] },
                            { "S": "now" },
                            { "S": "a" }
                        ]
                    }]
                }),
            ))
            .unwrap();
        let update_body = response_body(&update);
        assert_eq!(
            update_body["Responses"][0]["Item"]["updatedAt"]["S"], "now",
            "unexpected update response: {update_body}"
        );
        assert_eq!(
            update_body["Responses"][0]["Item"]["results"]["L"][0]["S"],
            "new"
        );

        let literal_update = svc
            .execute_statement(&req_for(
                "ExecuteStatement",
                json!({
                    "Statement": "UPDATE \"Widgets\" SET \"state\" = 'ready' WHERE \"pk\" = ?",
                    "Parameters": [{ "S": "a" }]
                }),
            ))
            .unwrap();
        assert!(response_body(&literal_update).get("Item").is_none());

        let literal_select = svc
            .batch_execute_statement(&req_for(
                "BatchExecuteStatement",
                json!({
                    "Statements": [{
                        "Statement": "SELECT * FROM \"Widgets\" WHERE \"pk\" = ?",
                        "Parameters": [{ "S": "a" }]
                    }]
                }),
            ))
            .unwrap();
        assert_eq!(
            response_body(&literal_select)["Responses"][0]["Item"]["state"]["S"],
            "ready"
        );
    }

    #[test]
    fn batch_execute_statement_handles_non_ascii_and_keyword_literals() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);

        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({ "Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}" }),
        ))
        .unwrap();

        // Non-ASCII value in a quoted literal must round-trip byte-for-byte
        // (previously corrupted to mojibake by byte-wise expression rebuild).
        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({
                "Statement": "UPDATE \"Widgets\" SET \"label\" = 'José 名前 🚀' WHERE \"pk\" = ?",
                "Parameters": [{ "S": "a" }]
            }),
        ))
        .unwrap();

        // A keyword-looking word inside a quoted literal is data, not a clause:
        // the write must land instead of being silently dropped.
        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({
                "Statement": "UPDATE \"Widgets\" SET \"note\" = 'please REMOVE this' WHERE \"pk\" = ?",
                "Parameters": [{ "S": "a" }]
            }),
        ))
        .unwrap();

        // A comma inside a quoted literal must not split the SET assignment and
        // drop the write (`SET addr = 'City, State'`).
        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({
                "Statement": "UPDATE \"Widgets\" SET \"addr\" = 'City, State' WHERE \"pk\" = ?",
                "Parameters": [{ "S": "a" }]
            }),
        ))
        .unwrap();

        // A non-ASCII, non-key attribute in the WHERE predicate must not panic
        // the RETURNING scan. It is not the key, so DynamoDB rejects it.
        let err = svc
            .execute_statement(&req_for(
                "ExecuteStatement",
                json!({
                    "Statement": "UPDATE \"Widgets\" SET \"x\" = ? WHERE \"café\" = ?",
                    "Parameters": [{ "S": "1" }, { "S": "z" }]
                }),
            ))
            .err()
            .expect("a WHERE without the key is rejected");
        assert_eq!(err.code(), "ValidationException");

        // Read back through a batch single-item SELECT with tight `"pk"=?`
        // spacing (no surrounding spaces) to confirm the key-check tolerates it.
        let select = svc
            .batch_execute_statement(&req_for(
                "BatchExecuteStatement",
                json!({
                    "Statements": [{
                        "Statement": "SELECT * FROM \"Widgets\" WHERE \"pk\"=?",
                        "Parameters": [{ "S": "a" }]
                    }]
                }),
            ))
            .unwrap();
        let item = &response_body(&select)["Responses"][0]["Item"];
        assert_eq!(item["label"]["S"], "José 名前 🚀");
        assert_eq!(item["note"]["S"], "please REMOVE this");
        assert_eq!(item["addr"]["S"], "City, State");
    }

    #[test]
    fn partiql_update_returning_without_where_does_not_corrupt_set() {
        // `RETURNING` after a WHERE-less UPDATE must be stripped from the SET
        // clause, not fed into the update-expression evaluator. The update
        // matches every item (no WHERE) and returns the new image.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);

        svc.execute_statement(&req_for(
            "ExecuteStatement",
            json!({ "Statement": "INSERT INTO \"Widgets\" VALUE {'pk': 'a'}" }),
        ))
        .unwrap();

        // Without a WHERE the statement names no item, which DynamoDB rejects;
        // the RETURNING clause is still stripped rather than parsed as part of
        // the SET expression, so the error is the key check, not a garbled
        // update expression.
        let err = svc
            .execute_statement(&req_for(
                "ExecuteStatement",
                json!({
                    "Statement": "UPDATE \"Widgets\" SET \"flag\" = ? RETURNING ALL NEW *",
                    "Parameters": [{ "S": "on" }]
                }),
            ))
            .err()
            .expect("an UPDATE without a key WHERE is rejected");
        assert_eq!(
            err.to_string(),
            "ValidationException: Where clause does not contain a mandatory equality on all key attributes"
        );
        let updated = svc
            .execute_statement(&req_for(
                "ExecuteStatement",
                json!({
                    "Statement": "UPDATE \"Widgets\" SET \"flag\" = ? WHERE \"pk\" = 'a' RETURNING ALL NEW *",
                    "Parameters": [{ "S": "on" }]
                }),
            ))
            .unwrap();
        assert_eq!(response_body(&updated)["Items"][0]["flag"]["S"], "on");
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
        let err = svc.execute_transaction(&req).err().unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");

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
    async fn transact_get_items_rejects_empty_oversized_and_malformed_key() {
        // bug-hunt 2026-07-01, finding 5: TransactGetItems must enforce the
        // 1..=100 bound and validate each Get.Key instead of coercing a
        // malformed key to {}.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);

        let empty = svc
            .transact_get_items(&req_for("TransactGetItems", json!({"TransactItems": []})))
            .err()
            .expect("empty TransactItems rejected");
        assert!(format!("{empty:?}").contains("ValidationException"));

        // A Get whose Key omits the partition key does not fit the schema.
        // That is caught per action, so it cancels the transaction with a
        // ValidationError reason rather than failing the request outright.
        let malformed = svc
            .transact_get_items(&req_for(
                "TransactGetItems",
                json!({"TransactItems": [
                    {"Get": {"TableName": "Widgets", "Key": {"other": {"S": "x"}}}}
                ]}),
            ))
            .expect("cancellation is a response");
        assert_eq!(malformed.status, StatusCode::BAD_REQUEST);
        let body = response_body(&malformed);
        assert_eq!(body["__type"], "TransactionCanceledException");
        assert_eq!(
            body["CancellationReasons"],
            json!([{
                "Code": "ValidationError",
                "Message": "The provided key element does not match the schema",
            }])
        );

        // Duplicate keys in one transaction are rejected.
        let dup = svc
            .transact_get_items(&req_for(
                "TransactGetItems",
                json!({"TransactItems": [
                    {"Get": {"TableName": "Widgets", "Key": {"pk": {"S": "a"}}}},
                    {"Get": {"TableName": "Widgets", "Key": {"pk": {"S": "a"}}}}
                ]}),
            ))
            .err()
            .expect("duplicate keys rejected");
        assert!(format!("{dup:?}").contains("ValidationException"));
    }

    #[tokio::test]
    async fn batch_write_item_rejects_both_or_neither_put_delete() {
        // bug-hunt 2026-07-01, finding 11.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);

        let neither = svc
            .batch_write_item(&req_for(
                "BatchWriteItem",
                json!({"RequestItems": {"Widgets": [{}]}}),
            ))
            .err()
            .expect("neither Put nor Delete rejected");
        assert!(format!("{neither:?}").contains("ValidationException"));

        let both = svc
            .batch_write_item(&req_for(
                "BatchWriteItem",
                json!({"RequestItems": {"Widgets": [{
                    "PutRequest": {"Item": {"pk": {"S": "a"}}},
                    "DeleteRequest": {"Key": {"pk": {"S": "a"}}}
                }]}}),
            ))
            .err()
            .expect("both Put and Delete rejected");
        assert!(format!("{both:?}").contains("ValidationException"));
    }

    #[tokio::test]
    async fn transact_write_item_rejects_zero_or_multi_op_member() {
        // bug-hunt 2026-07-01, finding 11.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);

        let none = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [{}]}),
            ))
            .err()
            .expect("empty member rejected");
        assert!(format!("{none:?}").contains("ValidationException"));

        let multi = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [{
                    "Put": {"TableName": "Widgets", "Item": {"pk": {"S": "a"}}},
                    "Delete": {"TableName": "Widgets", "Key": {"pk": {"S": "a"}}}
                }]}),
            ))
            .err()
            .expect("multi-op member rejected");
        assert!(format!("{multi:?}").contains("ValidationException"));
    }

    #[tokio::test]
    async fn execute_statement_rejects_zero_limit() {
        // bug-hunt 2026-07-01, finding 10.
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);
        let err = svc
            .execute_statement(&req_for(
                "ExecuteStatement",
                json!({"Statement": "SELECT * FROM Widgets", "Limit": 0}),
            ))
            .err()
            .expect("Limit 0 rejected");
        assert!(format!("{err:?}").contains("ValidationException"));
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

    /// Seed `Typed`: a `pk: S` table whose attribute definitions are known,
    /// with a String-keyed index `gsi1` on `idx` and a Binary-keyed index
    /// `gsib` on `bidx`.
    fn seed_typed_indexed_table(state: &SharedDynamoDbState) {
        use crate::state::{AttributeDefinition, GlobalSecondaryIndex, Projection};
        seed_table_with_stream(state, "Typed");
        let mut accts = state.write();
        let table = accts
            .get_or_create("123456789012")
            .tables
            .get_mut("Typed")
            .unwrap();
        let def = |name: &str, ty: &str| AttributeDefinition {
            attribute_name: name.into(),
            attribute_type: ty.into(),
        };
        table.attribute_definitions = vec![def("pk", "S"), def("idx", "S"), def("bidx", "B")];
        let gsi = |name: &str, attr: &str| GlobalSecondaryIndex {
            index_name: name.into(),
            key_schema: vec![KeySchemaElement {
                attribute_name: attr.into(),
                key_type: "HASH".into(),
            }],
            projection: Projection {
                projection_type: "ALL".into(),
                non_key_attributes: vec![],
            },
            provisioned_throughput: None,
            on_demand_throughput: None,
        };
        table.gsi = vec![gsi("gsib", "bidx"), gsi("gsi1", "idx")];
    }

    fn typed_item_count(state: &SharedDynamoDbState) -> usize {
        state.read().get("123456789012").unwrap().tables["Typed"]
            .items
            .len()
    }

    fn cancellation(response: &AwsResponse) -> (String, Vec<Value>) {
        let body = response_body(response);
        assert_eq!(body["__type"], "TransactionCanceledException", "{body}");
        (
            body["message"].as_str().unwrap().to_string(),
            body["CancellationReasons"].as_array().unwrap().clone(),
        )
    }

    // A wrong-typed table key is caught while the transaction executes, so it
    // cancels with a ValidationError reason; the Put form names the types,
    // the Key form (Update/Delete/ConditionCheck) reports a schema mismatch.
    #[tokio::test]
    async fn transact_write_wrong_typed_keys_cancel_with_validation_error() {
        let state = make_state();
        seed_typed_indexed_table(&state);
        let svc = DynamoDbService::new(state.clone());
        let cases = [
            (
                json!({"Put": {"TableName": "Typed", "Item": {"pk": {"N": "5"}}}}),
                "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N",
            ),
            (
                json!({"Put": {"TableName": "Typed", "Item": {"pk": {"L": [{"S": "x"}]}}}}),
                "One or more parameter values were invalid: Type mismatch for key pk expected: S actual: L",
            ),
            (
                json!({"Delete": {"TableName": "Typed", "Key": {"pk": {"N": "5"}}}}),
                KEY_SCHEMA_MISMATCH,
            ),
            (
                json!({"ConditionCheck": {
                    "TableName": "Typed",
                    "Key": {"pk": {"L": [{"S": "x"}]}},
                    "ConditionExpression": "attribute_not_exists(pk)"
                }}),
                KEY_SCHEMA_MISMATCH,
            ),
            (
                json!({"Update": {
                    "TableName": "Typed",
                    "Key": {"pk": {"N": "5"}},
                    "UpdateExpression": "SET a = :v",
                    "ExpressionAttributeValues": {":v": {"S": "x"}}
                }}),
                KEY_SCHEMA_MISMATCH,
            ),
        ];
        for (action, message) in cases {
            let resp = svc
                .transact_write_items(&req_for(
                    "TransactWriteItems",
                    json!({"TransactItems": [action]}),
                ))
                .unwrap();
            let (summary, reasons) = cancellation(&resp);
            assert_eq!(
                summary,
                "Transaction cancelled, please refer cancellation reasons for specific reasons [ValidationError]"
            );
            assert_eq!(
                reasons,
                vec![json!({"Code": "ValidationError", "Message": message})]
            );
        }
        assert_eq!(typed_item_count(&state), 0);
    }

    // An empty key value is DynamoDB's up-front validation: a top-level
    // ValidationException even inside a transaction, including ConditionCheck.
    #[tokio::test]
    async fn transact_write_empty_key_values_are_top_level_errors() {
        let state = make_state();
        seed_typed_indexed_table(&state);
        let svc = DynamoDbService::new(state.clone());
        let cases = [
            (
                json!({"ConditionCheck": {
                    "TableName": "Typed",
                    "Key": {"pk": {"S": ""}},
                    "ConditionExpression": "attribute_not_exists(pk)"
                }}),
                "One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: pk",
            ),
            (
                json!({"Put": {"TableName": "Typed", "Item": {"pk": {"S": "a"}, "idx": {"S": ""}}}}),
                "One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: gsi1, IndexKey: idx",
            ),
            (
                json!({"Update": {
                    "TableName": "Typed",
                    "Key": {"pk": {"S": "a"}},
                    "UpdateExpression": "SET bidx = :v",
                    "ExpressionAttributeValues": {":v": {"B": ""}}
                }}),
                "One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty binary value.",
            ),
        ];
        for (action, message) in cases {
            let err = svc
                .transact_write_items(&req_for(
                    "TransactWriteItems",
                    json!({"TransactItems": [action]}),
                ))
                .err()
                .expect("top-level ValidationException");
            assert_eq!(err.code(), "ValidationException");
            assert_eq!(err.message(), message);
        }
        assert_eq!(typed_item_count(&state), 0);
    }

    // A wrong-typed index key value cancels, naming the alphabetically-first
    // index keyed on the attribute; an Update is judged on what it writes.
    #[tokio::test]
    async fn transact_write_wrong_typed_index_key_cancels() {
        let state = make_state();
        seed_typed_indexed_table(&state);
        let svc = DynamoDbService::new(state.clone());
        let resp = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [
                    {"Put": {"TableName": "Typed", "Item": {"pk": {"S": "a"}}}},
                    {"Update": {
                        "TableName": "Typed",
                        "Key": {"pk": {"S": "b"}},
                        "UpdateExpression": "SET idx = :v",
                        "ExpressionAttributeValues": {":v": {"N": "5"}}
                    }},
                ]}),
            ))
            .unwrap();
        let (summary, reasons) = cancellation(&resp);
        assert_eq!(
            summary,
            "Transaction cancelled, please refer cancellation reasons for specific reasons [None, ValidationError]"
        );
        assert_eq!(reasons[0], json!({"Code": "None"}));
        assert_eq!(
            reasons[1]["Message"],
            "One or more parameter values were invalid: Type mismatch for Index Key idx Expected: S Actual: N IndexName: gsi1"
        );
        assert_eq!(typed_item_count(&state), 0);
    }

    // The summary lists every action's code positionally, not deduplicated.
    #[tokio::test]
    async fn transact_write_summary_lists_every_code_in_order() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);
        let cond = |pk: &str| {
            json!({"ConditionCheck": {
                "TableName": "Widgets",
                "Key": {"pk": {"S": pk}},
                "ConditionExpression": "attribute_exists(pk)"
            }})
        };
        let resp = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [cond("x"), cond("y")]}),
            ))
            .unwrap();
        let (summary, _) = cancellation(&resp);
        assert!(
            summary.ends_with("[ConditionalCheckFailed, ConditionalCheckFailed]"),
            "{summary}"
        );
    }

    #[tokio::test]
    async fn transact_write_rejects_transactions_over_4mb() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);
        let put = |i: usize| {
            json!({"Put": {"TableName": "Widgets", "Item": {
                "pk": {"S": format!("k{i}")},
                "payload": {"S": "x".repeat(350_000)}
            }}})
        };
        let under: Vec<Value> = (0..10).map(put).collect();
        assert!(svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": under})
            ))
            .is_ok());
        let over: Vec<Value> = (0..12).map(put).collect();
        let err = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": over}),
            ))
            .err()
            .expect("over 4 MB rejected");
        assert_eq!(err.code(), "ValidationException");
    }

    // The 4 MB cap counts what an Update writes (its resulting item), not
    // just its key, so large SET values add up.
    #[tokio::test]
    async fn transact_write_4mb_cap_counts_update_values() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state.clone());
        let update = |i: usize| {
            json!({"Update": {
                "TableName": "Widgets",
                "Key": {"pk": {"S": format!("u{i}")}},
                "UpdateExpression": "SET payload = :v",
                "ExpressionAttributeValues": {":v": {"S": "x".repeat(390_000)}}
            }})
        };
        let under: Vec<Value> = (0..10).map(update).collect();
        assert!(svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": under})
            ))
            .is_ok());
        // Fresh keys so the Updates are upserts of the same size.
        let over: Vec<Value> = (100..111).map(update).collect();
        let err = svc
            .transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": over}),
            ))
            .err()
            .expect("over 4 MB of updated items rejected");
        assert_eq!(err.code(), "ValidationException");
        let count = state.read().get("123456789012").unwrap().tables["Widgets"]
            .items
            .len();
        assert_eq!(count, 10, "the rejected transaction wrote nothing");
    }

    // Batch and transaction operations name no table in their not-found error.
    #[tokio::test]
    async fn batch_and_transact_missing_table_message() {
        let svc = DynamoDbService::new(make_state());
        let errs = [
            svc.batch_get_item(&req_for(
                "BatchGetItem",
                json!({"RequestItems": {"Nope": {"Keys": [{"pk": {"S": "a"}}]}}}),
            )),
            svc.batch_write_item(&req_for(
                "BatchWriteItem",
                json!({"RequestItems": {"Nope": [{"PutRequest": {"Item": {"pk": {"S": "a"}}}}]}}),
            )),
            svc.transact_get_items(&req_for(
                "TransactGetItems",
                json!({"TransactItems": [{"Get": {"TableName": "Nope", "Key": {"pk": {"S": "a"}}}}]}),
            )),
            svc.transact_write_items(&req_for(
                "TransactWriteItems",
                json!({"TransactItems": [{"Put": {"TableName": "Nope", "Item": {"pk": {"S": "a"}}}}]}),
            )),
        ];
        for result in errs {
            let err = result.err().expect("missing table rejected");
            assert_eq!(err.code(), "ResourceNotFoundException");
            assert_eq!(err.message(), "Requested resource not found");
        }
    }

    #[tokio::test]
    async fn batch_write_validates_keys_and_index_keys_up_front() {
        let state = make_state();
        seed_typed_indexed_table(&state);
        let svc = DynamoDbService::new(state.clone());
        let cases = [
            (json!({"PutRequest": {"Item": {"pk": {"N": "5"}}}}), KEY_SCHEMA_MISMATCH),
            (json!({"DeleteRequest": {"Key": {"pk": {"L": []}}}}), KEY_SCHEMA_MISMATCH),
            (
                json!({"PutRequest": {"Item": {"pk": {"S": "a"}, "idx": {"L": [{"S": "x"}]}}}}),
                "One or more parameter values were invalid: Type mismatch for Index Key idx Expected: S Actual: L IndexName: gsi1",
            ),
            (
                json!({"PutRequest": {"Item": {"pk": {"S": "a"}, "bidx": {"B": ""}}}}),
                "One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty binary value. IndexName: gsib, IndexKey: bidx",
            ),
        ];
        for (request, message) in cases {
            let err = svc
                .batch_write_item(&req_for(
                    "BatchWriteItem",
                    json!({"RequestItems": {"Typed": [request]}}),
                ))
                .err()
                .expect("rejected");
            assert_eq!(err.message(), message);
        }
        assert_eq!(typed_item_count(&state), 0);

        let empty = svc
            .batch_write_item(&req_for("BatchWriteItem", json!({"RequestItems": {}})))
            .err()
            .expect("empty RequestItems rejected");
        assert_eq!(
            empty.message(),
            "The requestItems parameter is required for BatchWriteItem"
        );
    }

    #[tokio::test]
    async fn batch_get_validates_request_shape_and_projection() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        seed_table_with_stream(&state, "Gadgets");
        let svc = DynamoDbService::new(state);
        let get = |items: Value| {
            svc.batch_get_item(&req_for("BatchGetItem", json!({"RequestItems": items})))
                .err()
                .expect("rejected")
                .message()
                .to_string()
        };
        assert_eq!(
            get(json!({})),
            "1 validation error detected: Value at 'RequestItems' failed to satisfy constraint: Member must have length greater than or equal to 1"
        );
        let keys: Vec<Value> = (0..101)
            .map(|i| json!({"pk": {"S": format!("k{i}")}}))
            .collect();
        assert_eq!(
            get(json!({"Widgets": {"Keys": keys}})),
            "1 validation error detected: Value at 'RequestItems.Widgets.member.Keys' failed to satisfy constraint: Member must have length less than or equal to 100"
        );
        assert_eq!(
            get(json!({
                "Widgets": {"Keys": [{"pk": {"S": "a"}}], "ProjectionExpression": "a, a.b"},
                "Gadgets": {"Keys": [{"pk": {"S": "a"}}], "ProjectionExpression": "a"}
            })),
            "Invalid ProjectionExpression: Two document paths overlap with each other; must remove or rewrite one of these paths; path one: [a], path two: [a, b]"
        );
        assert!(get(json!({
            "Widgets": {"Keys": [{"pk": {"S": "a"}}], "ProjectionExpression": "pk"},
            "Gadgets": {"Keys": [{"pk": {"S": "a"}}], "AttributesToGet": ["pk"]}
        }))
        .starts_with("Can not use both expression and non-expression parameters"));
    }

    // TransactGetItems applies the projection and omits Item when it selects
    // nothing; a bad projection fails the whole request up front.
    #[tokio::test]
    async fn transact_get_projects_and_omits_empty_projection() {
        let state = make_state();
        seed_table_with_stream(&state, "Widgets");
        let svc = DynamoDbService::new(state);
        svc.transact_write_items(&req_for(
            "TransactWriteItems",
            json!({"TransactItems": [{"Put": {"TableName": "Widgets", "Item": {
                "pk": {"S": "a"}, "real": {"S": "here"}, "other": {"S": "x"}
            }}}]}),
        ))
        .unwrap();
        let resp = svc
            .transact_get_items(&req_for(
                "TransactGetItems",
                json!({"TransactItems": [
                    {"Get": {"TableName": "Widgets", "Key": {"pk": {"S": "a"}},
                             "ProjectionExpression": "#x",
                             "ExpressionAttributeNames": {"#x": "missing"}}},
                ]}),
            ))
            .unwrap();
        assert_eq!(response_body(&resp)["Responses"], json!([{}]));

        let resp = svc
            .transact_get_items(&req_for(
                "TransactGetItems",
                json!({"TransactItems": [
                    {"Get": {"TableName": "Widgets", "Key": {"pk": {"S": "a"}},
                             "ProjectionExpression": "#r",
                             "ExpressionAttributeNames": {"#r": "real"}}},
                ]}),
            ))
            .unwrap();
        assert_eq!(
            response_body(&resp)["Responses"],
            json!([{"Item": {"real": {"S": "here"}}}])
        );

        let err = svc
            .transact_get_items(&req_for(
                "TransactGetItems",
                json!({"TransactItems": [
                    {"Get": {"TableName": "Widgets", "Key": {"pk": {"S": "a"}},
                             "ProjectionExpression": "!!!"}},
                ]}),
            ))
            .err()
            .expect("bad projection rejected");
        assert_eq!(
            err.message(),
            "Invalid ProjectionExpression: Syntax error; token: \"!\", near: \"!!\""
        );
    }
}
