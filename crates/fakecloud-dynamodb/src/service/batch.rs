use std::collections::HashMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::AttributeValue;

use super::{
    apply_update_expression, evaluate_condition, execute_partiql_statement, extract_key, get_table,
    get_table_mut, parse_expression_attribute_names, parse_expression_attribute_values,
    require_str, DynamoDbService,
};

impl DynamoDbService {
    pub(super) fn batch_get_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;

        validate_optional_enum_value(
            "returnConsumedCapacity",
            &body["ReturnConsumedCapacity"],
            &["INDEXES", "TOTAL", "NONE"],
        )?;

        let return_consumed = body["ReturnConsumedCapacity"].as_str().unwrap_or("NONE");

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

        let state = self.state.read();
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
                if let Some(idx) = table.find_item_index(&key) {
                    items.push(json!(table.items[idx]));
                }
            }
            responses.insert(table_name.clone(), items);

            if return_consumed == "TOTAL" || return_consumed == "INDEXES" {
                consumed_capacity.push(json!({
                    "TableName": table_name,
                    "CapacityUnits": 1.0,
                }));
            }
        }

        let mut result = json!({
            "Responses": responses,
            "UnprocessedKeys": {},
        });

        if return_consumed == "TOTAL" || return_consumed == "INDEXES" {
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

        let return_consumed = body["ReturnConsumedCapacity"].as_str().unwrap_or("NONE");
        let return_icm = body["ReturnItemCollectionMetrics"]
            .as_str()
            .unwrap_or("NONE");

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

        let mut state = self.state.write();
        let mut consumed_capacity: Vec<Value> = Vec::new();
        let mut item_collection_metrics: HashMap<String, Vec<Value>> = HashMap::new();

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

            for request in reqs {
                if let Some(put_req) = request.get("PutRequest") {
                    let item: HashMap<String, AttributeValue> =
                        serde_json::from_value(put_req["Item"].clone()).unwrap_or_default();
                    let key = extract_key(table, &item);
                    if let Some(idx) = table.find_item_index(&key) {
                        table.items[idx] = item;
                    } else {
                        table.items.push(item);
                    }
                } else if let Some(del_req) = request.get("DeleteRequest") {
                    let key: HashMap<String, AttributeValue> =
                        serde_json::from_value(del_req["Key"].clone()).unwrap_or_default();
                    if let Some(idx) = table.find_item_index(&key) {
                        table.items.remove(idx);
                    }
                }
            }

            table.recalculate_stats();

            if return_consumed == "TOTAL" || return_consumed == "INDEXES" {
                consumed_capacity.push(json!({
                    "TableName": table_name,
                    "CapacityUnits": 1.0,
                }));
            }

            if return_icm == "SIZE" {
                item_collection_metrics.insert(table_name.clone(), vec![]);
            }
        }

        let mut result = json!({
            "UnprocessedItems": {},
        });

        if return_consumed == "TOTAL" || return_consumed == "INDEXES" {
            result["ConsumedCapacity"] = json!(consumed_capacity);
        }

        if return_icm == "SIZE" {
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
        let transact_items = body["TransactItems"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TransactItems is required",
            )
        })?;

        let state = self.state.read();
        let mut responses: Vec<Value> = Vec::new();

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
        }

        Self::ok_json(json!({ "Responses": responses }))
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
        let transact_items = body["TransactItems"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TransactItems is required",
            )
        })?;

        let mut state = self.state.write();

        // First pass: validate all conditions
        let mut cancellation_reasons: Vec<Value> = Vec::new();
        let mut any_failed = false;

        for ti in transact_items {
            if let Some(put) = ti.get("Put") {
                let table_name = put["TableName"].as_str().unwrap_or_default();
                let item: HashMap<String, AttributeValue> =
                    serde_json::from_value(put["Item"].clone()).unwrap_or_default();
                let condition = put["ConditionExpression"].as_str();

                if let Some(cond) = condition {
                    let table = get_table(&state.tables, table_name)?;
                    let expr_attr_names = parse_expression_attribute_names(put);
                    let expr_attr_values = parse_expression_attribute_values(put);
                    let key = extract_key(table, &item);
                    let existing = table.find_item_index(&key).map(|i| &table.items[i]);
                    if evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)
                        .is_err()
                    {
                        cancellation_reasons.push(json!({
                            "Code": "ConditionalCheckFailed",
                            "Message": "The conditional request failed"
                        }));
                        any_failed = true;
                        continue;
                    }
                }
                cancellation_reasons.push(json!({ "Code": "None" }));
            } else if let Some(delete) = ti.get("Delete") {
                let table_name = delete["TableName"].as_str().unwrap_or_default();
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(delete["Key"].clone()).unwrap_or_default();
                let condition = delete["ConditionExpression"].as_str();

                if let Some(cond) = condition {
                    let table = get_table(&state.tables, table_name)?;
                    let expr_attr_names = parse_expression_attribute_names(delete);
                    let expr_attr_values = parse_expression_attribute_values(delete);
                    let existing = table.find_item_index(&key).map(|i| &table.items[i]);
                    if evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)
                        .is_err()
                    {
                        cancellation_reasons.push(json!({
                            "Code": "ConditionalCheckFailed",
                            "Message": "The conditional request failed"
                        }));
                        any_failed = true;
                        continue;
                    }
                }
                cancellation_reasons.push(json!({ "Code": "None" }));
            } else if let Some(update) = ti.get("Update") {
                let table_name = update["TableName"].as_str().unwrap_or_default();
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(update["Key"].clone()).unwrap_or_default();
                let condition = update["ConditionExpression"].as_str();

                if let Some(cond) = condition {
                    let table = get_table(&state.tables, table_name)?;
                    let expr_attr_names = parse_expression_attribute_names(update);
                    let expr_attr_values = parse_expression_attribute_values(update);
                    let existing = table.find_item_index(&key).map(|i| &table.items[i]);
                    if evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values)
                        .is_err()
                    {
                        cancellation_reasons.push(json!({
                            "Code": "ConditionalCheckFailed",
                            "Message": "The conditional request failed"
                        }));
                        any_failed = true;
                        continue;
                    }
                }
                cancellation_reasons.push(json!({ "Code": "None" }));
            } else if let Some(check) = ti.get("ConditionCheck") {
                let table_name = check["TableName"].as_str().unwrap_or_default();
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(check["Key"].clone()).unwrap_or_default();
                let cond = check["ConditionExpression"].as_str().unwrap_or_default();

                let table = get_table(&state.tables, table_name)?;
                let expr_attr_names = parse_expression_attribute_names(check);
                let expr_attr_values = parse_expression_attribute_values(check);
                let existing = table.find_item_index(&key).map(|i| &table.items[i]);
                if evaluate_condition(cond, existing, &expr_attr_names, &expr_attr_values).is_err()
                {
                    cancellation_reasons.push(json!({
                        "Code": "ConditionalCheckFailed",
                        "Message": "The conditional request failed"
                    }));
                    any_failed = true;
                    continue;
                }
                cancellation_reasons.push(json!({ "Code": "None" }));
            } else {
                cancellation_reasons.push(json!({ "Code": "None" }));
            }
        }

        if any_failed {
            let error_body = json!({
                "__type": "TransactionCanceledException",
                "message": "Transaction cancelled, please refer cancellation reasons for specific reasons [ConditionalCheckFailed]",
                "CancellationReasons": cancellation_reasons
            });
            return Ok(AwsResponse::json(
                StatusCode::BAD_REQUEST,
                serde_json::to_vec(&error_body).unwrap(),
            ));
        }

        // Second pass: apply all writes
        for ti in transact_items {
            if let Some(put) = ti.get("Put") {
                let table_name = put["TableName"].as_str().unwrap_or_default();
                let item: HashMap<String, AttributeValue> =
                    serde_json::from_value(put["Item"].clone()).unwrap_or_default();
                let table = get_table_mut(&mut state.tables, table_name)?;
                let key = extract_key(table, &item);
                if let Some(idx) = table.find_item_index(&key) {
                    table.items[idx] = item;
                } else {
                    table.items.push(item);
                }
                table.recalculate_stats();
            } else if let Some(delete) = ti.get("Delete") {
                let table_name = delete["TableName"].as_str().unwrap_or_default();
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(delete["Key"].clone()).unwrap_or_default();
                let table = get_table_mut(&mut state.tables, table_name)?;
                if let Some(idx) = table.find_item_index(&key) {
                    table.items.remove(idx);
                }
                table.recalculate_stats();
            } else if let Some(update) = ti.get("Update") {
                let table_name = update["TableName"].as_str().unwrap_or_default();
                let key: HashMap<String, AttributeValue> =
                    serde_json::from_value(update["Key"].clone()).unwrap_or_default();
                let update_expression = update["UpdateExpression"].as_str();
                let expr_attr_names = parse_expression_attribute_names(update);
                let expr_attr_values = parse_expression_attribute_values(update);

                let table = get_table_mut(&mut state.tables, table_name)?;
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
                    )?;
                }
                table.recalculate_stats();
            }
            // ConditionCheck: no write needed
        }

        Self::ok_json(json!({}))
    }

    // ── PartiQL ─────────────────────────────────────────────────────────

    pub(super) fn execute_statement(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let statement = require_str(&body, "Statement")?;
        let parameters = body["Parameters"].as_array().cloned().unwrap_or_default();

        execute_partiql_statement(&self.state, statement, &parameters)
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

        let mut responses: Vec<Value> = Vec::new();
        for stmt_obj in statements {
            let statement = stmt_obj["Statement"].as_str().unwrap_or_default();
            let parameters = stmt_obj["Parameters"]
                .as_array()
                .cloned()
                .unwrap_or_default();

            match execute_partiql_statement(&self.state, statement, &parameters) {
                Ok(resp) => {
                    let resp_body: Value = serde_json::from_slice(&resp.body).unwrap_or_default();
                    responses.push(resp_body);
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

        // Collect all results; if any fail, return TransactionCanceledException
        let mut results: Vec<Result<Value, String>> = Vec::new();
        for stmt_obj in transact_statements {
            let statement = stmt_obj["Statement"].as_str().unwrap_or_default();
            let parameters = stmt_obj["Parameters"]
                .as_array()
                .cloned()
                .unwrap_or_default();

            match execute_partiql_statement(&self.state, statement, &parameters) {
                Ok(resp) => {
                    let resp_body: Value = serde_json::from_slice(&resp.body).unwrap_or_default();
                    results.push(Ok(resp_body));
                }
                Err(e) => {
                    results.push(Err(e.to_string()));
                }
            }
        }

        let any_failed = results.iter().any(|r| r.is_err());
        if any_failed {
            let reasons: Vec<Value> = results
                .iter()
                .map(|r| match r {
                    Ok(_) => json!({ "Code": "None" }),
                    Err(msg) => json!({
                        "Code": "ValidationException",
                        "Message": msg
                    }),
                })
                .collect();
            let error_body = json!({
                "__type": "TransactionCanceledException",
                "message": "Transaction cancelled due to validation errors",
                "CancellationReasons": reasons
            });
            return Ok(AwsResponse::json(
                StatusCode::BAD_REQUEST,
                serde_json::to_vec(&error_body).unwrap(),
            ));
        }

        let responses: Vec<Value> = results.into_iter().filter_map(|r| r.ok()).collect();
        Self::ok_json(json!({ "Responses": responses }))
    }
}
