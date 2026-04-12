use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use fakecloud_s3::state::SharedS3State;

use crate::state::{
    attribute_type_and_value, AttributeDefinition, AttributeValue, BackupDescription, DynamoTable,
    ExportDescription, GlobalSecondaryIndex, GlobalTableDescription, ImportDescription,
    KeySchemaElement, KinesisDestination, LocalSecondaryIndex, Projection, ProvisionedThroughput,
    ReplicaDescription, SharedDynamoDbState,
};

pub struct DynamoDbService {
    state: SharedDynamoDbState,
    s3_state: Option<SharedS3State>,
    delivery: Option<Arc<DeliveryBus>>,
}

impl DynamoDbService {
    pub fn new(state: SharedDynamoDbState) -> Self {
        Self {
            state,
            s3_state: None,
            delivery: None,
        }
    }

    pub fn with_s3(mut self, s3_state: SharedS3State) -> Self {
        self.s3_state = Some(s3_state);
        self
    }

    pub fn with_delivery(mut self, delivery: Arc<DeliveryBus>) -> Self {
        self.delivery = Some(delivery);
        self
    }

    /// Deliver a change record to all active Kinesis streaming destinations for a table.
    fn deliver_to_kinesis_destinations(
        &self,
        table: &DynamoTable,
        event_name: &str,
        keys: &HashMap<String, AttributeValue>,
        old_image: Option<&HashMap<String, AttributeValue>>,
        new_image: Option<&HashMap<String, AttributeValue>>,
    ) {
        let delivery = match &self.delivery {
            Some(d) => d,
            None => return,
        };

        let active_destinations: Vec<_> = table
            .kinesis_destinations
            .iter()
            .filter(|d| d.destination_status == "ACTIVE")
            .collect();

        if active_destinations.is_empty() {
            return;
        }

        let mut record = json!({
            "eventID": uuid::Uuid::new_v4().to_string(),
            "eventName": event_name,
            "eventVersion": "1.1",
            "eventSource": "aws:dynamodb",
            "awsRegion": table.arn.split(':').nth(3).unwrap_or("us-east-1"),
            "dynamodb": {
                "Keys": keys,
                "SequenceNumber": chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0).to_string(),
                "SizeBytes": serde_json::to_string(keys).map(|s| s.len()).unwrap_or(0),
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
            "eventSourceARN": &table.arn,
            "tableName": &table.name,
        });

        if let Some(old) = old_image {
            record["dynamodb"]["OldImage"] = json!(old);
        }
        if let Some(new) = new_image {
            record["dynamodb"]["NewImage"] = json!(new);
        }

        let record_str = serde_json::to_string(&record).unwrap_or_default();
        let encoded = base64::engine::general_purpose::STANDARD.encode(&record_str);
        let partition_key = serde_json::to_string(keys).unwrap_or_default();

        for dest in active_destinations {
            delivery.send_to_kinesis(&dest.stream_arn, &encoded, &partition_key);
        }
    }

    fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
        serde_json::from_slice(&req.body).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "SerializationException",
                format!("Invalid JSON: {e}"),
            )
        })
    }

    fn ok_json(body: Value) -> Result<AwsResponse, AwsServiceError> {
        Ok(AwsResponse::ok_json(body))
    }

    // ── Table Operations ────────────────────────────────────────────────

    fn create_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;

        let table_name = body["TableName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "TableName is required",
                )
            })?
            .to_string();

        let key_schema = parse_key_schema(&body["KeySchema"])?;
        let attribute_definitions = parse_attribute_definitions(&body["AttributeDefinitions"])?;

        // Validate that key schema attributes are defined
        for ks in &key_schema {
            if !attribute_definitions
                .iter()
                .any(|ad| ad.attribute_name == ks.attribute_name)
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "One or more parameter values were invalid: \
                         Some index key attributes are not defined in AttributeDefinitions. \
                         Keys: [{}], AttributeDefinitions: [{}]",
                        ks.attribute_name,
                        attribute_definitions
                            .iter()
                            .map(|ad| ad.attribute_name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                ));
            }
        }

        let billing_mode = body["BillingMode"]
            .as_str()
            .unwrap_or("PROVISIONED")
            .to_string();

        let provisioned_throughput = if billing_mode == "PAY_PER_REQUEST" {
            ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            }
        } else {
            parse_provisioned_throughput(&body["ProvisionedThroughput"])?
        };

        let gsi = parse_gsi(&body["GlobalSecondaryIndexes"]);
        let lsi = parse_lsi(&body["LocalSecondaryIndexes"]);
        let tags = parse_tags(&body["Tags"]);

        // Parse StreamSpecification
        let (stream_enabled, stream_view_type) =
            if let Some(stream_spec) = body.get("StreamSpecification") {
                let enabled = stream_spec
                    .get("StreamEnabled")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let view_type = if enabled {
                    stream_spec
                        .get("StreamViewType")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string())
                } else {
                    None
                };
                (enabled, view_type)
            } else {
                (false, None)
            };

        // Parse SSESpecification
        let (sse_type, sse_kms_key_arn) = if let Some(sse_spec) = body.get("SSESpecification") {
            let enabled = sse_spec
                .get("Enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if enabled {
                let sse_type = sse_spec
                    .get("SSEType")
                    .and_then(|v| v.as_str())
                    .unwrap_or("KMS")
                    .to_string();
                let kms_key = sse_spec
                    .get("KMSMasterKeyId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
                (Some(sse_type), kms_key)
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

        let mut state = self.state.write();

        if state.tables.contains_key(&table_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!("Table already exists: {table_name}"),
            ));
        }

        let now = Utc::now();
        let arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}",
            state.region, state.account_id, table_name
        );
        let stream_arn = if stream_enabled {
            Some(format!(
                "arn:aws:dynamodb:{}:{}:table/{}/stream/{}",
                state.region,
                state.account_id,
                table_name,
                now.format("%Y-%m-%dT%H:%M:%S.%3f")
            ))
        } else {
            None
        };

        let table = DynamoTable {
            name: table_name.clone(),
            arn: arn.clone(),
            key_schema: key_schema.clone(),
            attribute_definitions: attribute_definitions.clone(),
            provisioned_throughput: provisioned_throughput.clone(),
            items: Vec::new(),
            gsi: gsi.clone(),
            lsi: lsi.clone(),
            tags,
            created_at: now,
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: billing_mode.clone(),
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: Vec::new(),
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: HashMap::new(),
            stream_enabled,
            stream_view_type,
            stream_arn,
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type,
            sse_kms_key_arn,
        };

        state.tables.insert(table_name, table);

        let table_desc = build_table_description_json(
            &arn,
            &key_schema,
            &attribute_definitions,
            &provisioned_throughput,
            &gsi,
            &lsi,
            &billing_mode,
            now,
            0,
            0,
            "ACTIVE",
        );

        Self::ok_json(json!({ "TableDescription": table_desc }))
    }

    fn delete_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let mut state = self.state.write();
        let table = state.tables.remove(table_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Requested resource not found: Table: {table_name} not found"),
            )
        })?;

        let table_desc = build_table_description_json(
            &table.arn,
            &table.key_schema,
            &table.attribute_definitions,
            &table.provisioned_throughput,
            &table.gsi,
            &table.lsi,
            &table.billing_mode,
            table.created_at,
            table.item_count,
            table.size_bytes,
            "DELETING",
        );

        Self::ok_json(json!({ "TableDescription": table_desc }))
    }

    fn describe_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let table_desc = build_table_description(table);

        Self::ok_json(json!({ "Table": table_desc }))
    }

    fn list_tables(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;

        validate_optional_string_length(
            "exclusiveStartTableName",
            body["ExclusiveStartTableName"].as_str(),
            3,
            255,
        )?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;

        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;
        let exclusive_start = body["ExclusiveStartTableName"]
            .as_str()
            .map(|s| s.to_string());

        let state = self.state.read();
        let mut names: Vec<&String> = state.tables.keys().collect();
        names.sort();

        let start_idx = match &exclusive_start {
            Some(start) => names
                .iter()
                .position(|n| n.as_str() > start.as_str())
                .unwrap_or(names.len()),
            None => 0,
        };

        let page: Vec<&str> = names
            .iter()
            .skip(start_idx)
            .take(limit)
            .map(|n| n.as_str())
            .collect();

        let mut result = json!({ "TableNames": page });

        if start_idx + limit < names.len() {
            if let Some(last) = page.last() {
                result["LastEvaluatedTableName"] = json!(last);
            }
        }

        Self::ok_json(result)
    }

    fn update_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let mut state = self.state.write();
        let table = state.tables.get_mut(table_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Requested resource not found: Table: {table_name} not found"),
            )
        })?;

        if let Some(pt) = body.get("ProvisionedThroughput") {
            if let Ok(throughput) = parse_provisioned_throughput(pt) {
                table.provisioned_throughput = throughput;
            }
        }

        if let Some(bm) = body["BillingMode"].as_str() {
            table.billing_mode = bm.to_string();
        }

        // Handle SSESpecification update
        if let Some(sse_spec) = body.get("SSESpecification") {
            let enabled = sse_spec
                .get("Enabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            if enabled {
                table.sse_type = Some(
                    sse_spec
                        .get("SSEType")
                        .and_then(|v| v.as_str())
                        .unwrap_or("KMS")
                        .to_string(),
                );
                table.sse_kms_key_arn = sse_spec
                    .get("KMSMasterKeyId")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());
            } else {
                table.sse_type = None;
                table.sse_kms_key_arn = None;
            }
        }

        let table_desc = build_table_description_json(
            &table.arn,
            &table.key_schema,
            &table.attribute_definitions,
            &table.provisioned_throughput,
            &table.gsi,
            &table.lsi,
            &table.billing_mode,
            table.created_at,
            table.item_count,
            table.size_bytes,
            &table.status,
        );

        Self::ok_json(json!({ "TableDescription": table_desc }))
    }

    // ── Item Operations ─────────────────────────────────────────────────

    fn put_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn get_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn delete_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn update_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    // ── Query & Scan ────────────────────────────────────────────────────

    fn query(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
                    StatusCode::BAD_REQUEST,
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

    fn scan(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    // ── Batch Operations ────────────────────────────────────────────────

    fn batch_get_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn batch_write_item(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    // ── Tags ────────────────────────────────────────────────────────────

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;
        validate_required("Tags", &body["Tags"])?;

        let mut state = self.state.write();
        let table = find_table_by_arn_mut(&mut state.tables, resource_arn)?;

        fakecloud_core::tags::apply_tags(&mut table.tags, &body, "Tags", "Key", "Value").map_err(
            |f| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("{f} must be a list"),
                )
            },
        )?;

        Self::ok_json(json!({}))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;
        validate_required("TagKeys", &body["TagKeys"])?;

        let mut state = self.state.write();
        let table = find_table_by_arn_mut(&mut state.tables, resource_arn)?;

        fakecloud_core::tags::remove_tags(&mut table.tags, &body, "TagKeys").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Self::ok_json(json!({}))
    }

    fn list_tags_of_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;

        let state = self.state.read();
        let table = find_table_by_arn(&state.tables, resource_arn)?;

        let tags = fakecloud_core::tags::tags_to_json(&table.tags, "Key", "Value");

        Self::ok_json(json!({ "Tags": tags }))
    }

    // ── Transactions ────────────────────────────────────────────────────

    fn transact_get_items(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn transact_write_items(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn execute_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let statement = require_str(&body, "Statement")?;
        let parameters = body["Parameters"].as_array().cloned().unwrap_or_default();

        execute_partiql_statement(&self.state, statement, &parameters)
    }

    fn batch_execute_statement(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn execute_transaction(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    // ── TTL ─────────────────────────────────────────────────────────────

    fn update_time_to_live(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let spec = &body["TimeToLiveSpecification"];
        let attr_name = spec["AttributeName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TimeToLiveSpecification.AttributeName is required",
            )
        })?;
        let enabled = spec["Enabled"].as_bool().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TimeToLiveSpecification.Enabled is required",
            )
        })?;

        let mut state = self.state.write();
        let table = get_table_mut(&mut state.tables, table_name)?;

        if enabled {
            table.ttl_attribute = Some(attr_name.to_string());
            table.ttl_enabled = true;
        } else {
            table.ttl_enabled = false;
        }

        Self::ok_json(json!({
            "TimeToLiveSpecification": {
                "AttributeName": attr_name,
                "Enabled": enabled
            }
        }))
    }

    fn describe_time_to_live(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let status = if table.ttl_enabled {
            "ENABLED"
        } else {
            "DISABLED"
        };

        let mut desc = json!({
            "TimeToLiveDescription": {
                "TimeToLiveStatus": status
            }
        });

        if let Some(ref attr) = table.ttl_attribute {
            desc["TimeToLiveDescription"]["AttributeName"] = json!(attr);
        }

        Self::ok_json(desc)
    }

    // ── Resource Policies ───────────────────────────────────────────────

    fn put_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;
        let policy = require_str(&body, "Policy")?;

        let mut state = self.state.write();
        let table = find_table_by_arn_mut(&mut state.tables, resource_arn)?;
        table.resource_policy = Some(policy.to_string());

        let revision_id = uuid::Uuid::new_v4().to_string();
        Self::ok_json(json!({ "RevisionId": revision_id }))
    }

    fn get_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;

        let state = self.state.read();
        let table = find_table_by_arn(&state.tables, resource_arn)?;

        match &table.resource_policy {
            Some(policy) => {
                let revision_id = uuid::Uuid::new_v4().to_string();
                Self::ok_json(json!({
                    "Policy": policy,
                    "RevisionId": revision_id
                }))
            }
            None => Self::ok_json(json!({ "Policy": null })),
        }
    }

    fn delete_resource_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;

        let mut state = self.state.write();
        let table = find_table_by_arn_mut(&mut state.tables, resource_arn)?;
        table.resource_policy = None;

        Self::ok_json(json!({}))
    }

    // ── Stubs ──────────────────────────────────────────────────────────

    fn describe_endpoints(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        Self::ok_json(json!({
            "Endpoints": [{
                "Address": "dynamodb.us-east-1.amazonaws.com",
                "CachePeriodInMinutes": 1440
            }]
        }))
    }

    fn describe_limits(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        Self::ok_json(json!({
            "AccountMaxReadCapacityUnits": 80000,
            "AccountMaxWriteCapacityUnits": 80000,
            "TableMaxReadCapacityUnits": 40000,
            "TableMaxWriteCapacityUnits": 40000
        }))
    }

    // ── Backups ────────────────────────────────────────────────────────

    fn create_backup(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let backup_name = require_str(&body, "BackupName")?;

        let mut state = self.state.write();
        let table = get_table(&state.tables, table_name)?;

        let backup_arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}/backup/{}",
            state.region,
            state.account_id,
            table_name,
            Utc::now().format("%Y%m%d%H%M%S")
        );
        let now = Utc::now();

        let backup = BackupDescription {
            backup_arn: backup_arn.clone(),
            backup_name: backup_name.to_string(),
            table_name: table_name.to_string(),
            table_arn: table.arn.clone(),
            backup_status: "AVAILABLE".to_string(),
            backup_type: "USER".to_string(),
            backup_creation_date: now,
            key_schema: table.key_schema.clone(),
            attribute_definitions: table.attribute_definitions.clone(),
            provisioned_throughput: table.provisioned_throughput.clone(),
            billing_mode: table.billing_mode.clone(),
            item_count: table.item_count,
            size_bytes: table.size_bytes,
            items: table.items.clone(),
        };

        state.backups.insert(backup_arn.clone(), backup);

        Self::ok_json(json!({
            "BackupDetails": {
                "BackupArn": backup_arn,
                "BackupName": backup_name,
                "BackupStatus": "AVAILABLE",
                "BackupType": "USER",
                "BackupCreationDateTime": now.timestamp() as f64,
                "BackupSizeBytes": 0
            }
        }))
    }

    fn delete_backup(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let backup_arn = require_str(&body, "BackupArn")?;

        let mut state = self.state.write();
        let backup = state.backups.remove(backup_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BackupNotFoundException",
                format!("Backup not found: {backup_arn}"),
            )
        })?;

        Self::ok_json(json!({
            "BackupDescription": {
                "BackupDetails": {
                    "BackupArn": backup.backup_arn,
                    "BackupName": backup.backup_name,
                    "BackupStatus": "DELETED",
                    "BackupType": backup.backup_type,
                    "BackupCreationDateTime": backup.backup_creation_date.timestamp() as f64,
                    "BackupSizeBytes": backup.size_bytes
                },
                "SourceTableDetails": {
                    "TableName": backup.table_name,
                    "TableArn": backup.table_arn,
                    "TableId": uuid::Uuid::new_v4().to_string(),
                    "KeySchema": backup.key_schema.iter().map(|ks| json!({
                        "AttributeName": ks.attribute_name,
                        "KeyType": ks.key_type
                    })).collect::<Vec<_>>(),
                    "TableCreationDateTime": backup.backup_creation_date.timestamp() as f64,
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": backup.provisioned_throughput.read_capacity_units,
                        "WriteCapacityUnits": backup.provisioned_throughput.write_capacity_units
                    },
                    "ItemCount": backup.item_count,
                    "BillingMode": backup.billing_mode,
                    "TableSizeBytes": backup.size_bytes
                },
                "SourceTableFeatureDetails": {}
            }
        }))
    }

    fn describe_backup(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let backup_arn = require_str(&body, "BackupArn")?;

        let state = self.state.read();
        let backup = state.backups.get(backup_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BackupNotFoundException",
                format!("Backup not found: {backup_arn}"),
            )
        })?;

        Self::ok_json(json!({
            "BackupDescription": {
                "BackupDetails": {
                    "BackupArn": backup.backup_arn,
                    "BackupName": backup.backup_name,
                    "BackupStatus": backup.backup_status,
                    "BackupType": backup.backup_type,
                    "BackupCreationDateTime": backup.backup_creation_date.timestamp() as f64,
                    "BackupSizeBytes": backup.size_bytes
                },
                "SourceTableDetails": {
                    "TableName": backup.table_name,
                    "TableArn": backup.table_arn,
                    "TableId": uuid::Uuid::new_v4().to_string(),
                    "KeySchema": backup.key_schema.iter().map(|ks| json!({
                        "AttributeName": ks.attribute_name,
                        "KeyType": ks.key_type
                    })).collect::<Vec<_>>(),
                    "TableCreationDateTime": backup.backup_creation_date.timestamp() as f64,
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": backup.provisioned_throughput.read_capacity_units,
                        "WriteCapacityUnits": backup.provisioned_throughput.write_capacity_units
                    },
                    "ItemCount": backup.item_count,
                    "BillingMode": backup.billing_mode,
                    "TableSizeBytes": backup.size_bytes
                },
                "SourceTableFeatureDetails": {}
            }
        }))
    }

    fn list_backups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length("tableName", body["TableName"].as_str(), 1, 1024)?;
        validate_optional_string_length(
            "exclusiveStartBackupArn",
            body["ExclusiveStartBackupArn"].as_str(),
            37,
            1024,
        )?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        validate_optional_enum_value(
            "backupType",
            &body["BackupType"],
            &["USER", "SYSTEM", "AWS_BACKUP", "ALL"],
        )?;
        let table_name = body["TableName"].as_str();

        let state = self.state.read();
        let summaries: Vec<Value> = state
            .backups
            .values()
            .filter(|b| table_name.is_none() || table_name == Some(b.table_name.as_str()))
            .map(|b| {
                json!({
                    "TableName": b.table_name,
                    "TableArn": b.table_arn,
                    "BackupArn": b.backup_arn,
                    "BackupName": b.backup_name,
                    "BackupCreationDateTime": b.backup_creation_date.timestamp() as f64,
                    "BackupStatus": b.backup_status,
                    "BackupType": b.backup_type,
                    "BackupSizeBytes": b.size_bytes
                })
            })
            .collect();

        Self::ok_json(json!({
            "BackupSummaries": summaries
        }))
    }

    fn restore_table_from_backup(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let backup_arn = require_str(&body, "BackupArn")?;
        let target_table_name = require_str(&body, "TargetTableName")?;

        let mut state = self.state.write();
        let backup = state.backups.get(backup_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BackupNotFoundException",
                format!("Backup not found: {backup_arn}"),
            )
        })?;

        if state.tables.contains_key(target_table_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TableAlreadyExistsException",
                format!("Table already exists: {target_table_name}"),
            ));
        }

        let now = Utc::now();
        let arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}",
            state.region, state.account_id, target_table_name
        );

        let restored_items = backup.items.clone();
        let mut table = DynamoTable {
            name: target_table_name.to_string(),
            arn: arn.clone(),
            key_schema: backup.key_schema.clone(),
            attribute_definitions: backup.attribute_definitions.clone(),
            provisioned_throughput: backup.provisioned_throughput.clone(),
            items: restored_items,
            gsi: Vec::new(),
            lsi: Vec::new(),
            tags: HashMap::new(),
            created_at: now,
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: backup.billing_mode.clone(),
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: Vec::new(),
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: HashMap::new(),
            stream_enabled: false,
            stream_view_type: None,
            stream_arn: None,
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,
        };
        table.recalculate_stats();

        let desc = build_table_description(&table);
        state.tables.insert(target_table_name.to_string(), table);

        Self::ok_json(json!({
            "TableDescription": desc
        }))
    }

    fn restore_table_to_point_in_time(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let target_table_name = require_str(&body, "TargetTableName")?;
        let source_table_name = body["SourceTableName"].as_str();
        let source_table_arn = body["SourceTableArn"].as_str();

        let mut state = self.state.write();

        // Resolve source table
        let source = if let Some(name) = source_table_name {
            get_table(&state.tables, name)?.clone()
        } else if let Some(arn) = source_table_arn {
            find_table_by_arn(&state.tables, arn)?.clone()
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "SourceTableName or SourceTableArn is required",
            ));
        };

        if state.tables.contains_key(target_table_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "TableAlreadyExistsException",
                format!("Table already exists: {target_table_name}"),
            ));
        }

        let now = Utc::now();
        let arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}",
            state.region, state.account_id, target_table_name
        );

        let mut table = DynamoTable {
            name: target_table_name.to_string(),
            arn: arn.clone(),
            key_schema: source.key_schema.clone(),
            attribute_definitions: source.attribute_definitions.clone(),
            provisioned_throughput: source.provisioned_throughput.clone(),
            items: source.items.clone(),
            gsi: Vec::new(),
            lsi: Vec::new(),
            tags: HashMap::new(),
            created_at: now,
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: source.billing_mode.clone(),
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: Vec::new(),
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: HashMap::new(),
            stream_enabled: false,
            stream_view_type: None,
            stream_arn: None,
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,
        };
        table.recalculate_stats();

        let desc = build_table_description(&table);
        state.tables.insert(target_table_name.to_string(), table);

        Self::ok_json(json!({
            "TableDescription": desc
        }))
    }

    fn update_continuous_backups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let pitr_spec = body["PointInTimeRecoverySpecification"]
            .as_object()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "PointInTimeRecoverySpecification is required",
                )
            })?;
        let enabled = pitr_spec
            .get("PointInTimeRecoveryEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let mut state = self.state.write();
        let table = get_table_mut(&mut state.tables, table_name)?;
        table.pitr_enabled = enabled;

        let status = if enabled { "ENABLED" } else { "DISABLED" };
        Self::ok_json(json!({
            "ContinuousBackupsDescription": {
                "ContinuousBackupsStatus": status,
                "PointInTimeRecoveryDescription": {
                    "PointInTimeRecoveryStatus": status,
                    "EarliestRestorableDateTime": Utc::now().timestamp() as f64,
                    "LatestRestorableDateTime": Utc::now().timestamp() as f64
                }
            }
        }))
    }

    fn describe_continuous_backups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let status = if table.pitr_enabled {
            "ENABLED"
        } else {
            "DISABLED"
        };
        Self::ok_json(json!({
            "ContinuousBackupsDescription": {
                "ContinuousBackupsStatus": status,
                "PointInTimeRecoveryDescription": {
                    "PointInTimeRecoveryStatus": status,
                    "EarliestRestorableDateTime": Utc::now().timestamp() as f64,
                    "LatestRestorableDateTime": Utc::now().timestamp() as f64
                }
            }
        }))
    }

    // ── Global Tables ──────────────────────────────────────────────────

    fn create_global_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let global_table_name = require_str(&body, "GlobalTableName")?;
        validate_string_length("globalTableName", global_table_name, 3, 255)?;

        let replication_group = body["ReplicationGroup"]
            .as_array()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "ReplicationGroup is required",
                )
            })?
            .iter()
            .filter_map(|r| {
                r["RegionName"].as_str().map(|rn| ReplicaDescription {
                    region_name: rn.to_string(),
                    replica_status: "ACTIVE".to_string(),
                })
            })
            .collect::<Vec<_>>();

        let mut state = self.state.write();

        if state.global_tables.contains_key(global_table_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "GlobalTableAlreadyExistsException",
                format!("Global table already exists: {global_table_name}"),
            ));
        }

        let arn = format!(
            "arn:aws:dynamodb::{}:global-table/{}",
            state.account_id, global_table_name
        );
        let now = Utc::now();

        let gt = GlobalTableDescription {
            global_table_name: global_table_name.to_string(),
            global_table_arn: arn.clone(),
            global_table_status: "ACTIVE".to_string(),
            creation_date: now,
            replication_group: replication_group.clone(),
        };

        state
            .global_tables
            .insert(global_table_name.to_string(), gt);

        Self::ok_json(json!({
            "GlobalTableDescription": {
                "GlobalTableName": global_table_name,
                "GlobalTableArn": arn,
                "GlobalTableStatus": "ACTIVE",
                "CreationDateTime": now.timestamp() as f64,
                "ReplicationGroup": replication_group.iter().map(|r| json!({
                    "RegionName": r.region_name,
                    "ReplicaStatus": r.replica_status
                })).collect::<Vec<_>>()
            }
        }))
    }

    fn describe_global_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let global_table_name = require_str(&body, "GlobalTableName")?;
        validate_string_length("globalTableName", global_table_name, 3, 255)?;

        let state = self.state.read();
        let gt = state.global_tables.get(global_table_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "GlobalTableNotFoundException",
                format!("Global table not found: {global_table_name}"),
            )
        })?;

        Self::ok_json(json!({
            "GlobalTableDescription": {
                "GlobalTableName": gt.global_table_name,
                "GlobalTableArn": gt.global_table_arn,
                "GlobalTableStatus": gt.global_table_status,
                "CreationDateTime": gt.creation_date.timestamp() as f64,
                "ReplicationGroup": gt.replication_group.iter().map(|r| json!({
                    "RegionName": r.region_name,
                    "ReplicaStatus": r.replica_status
                })).collect::<Vec<_>>()
            }
        }))
    }

    fn describe_global_table_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let global_table_name = require_str(&body, "GlobalTableName")?;
        validate_string_length("globalTableName", global_table_name, 3, 255)?;

        let state = self.state.read();
        let gt = state.global_tables.get(global_table_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "GlobalTableNotFoundException",
                format!("Global table not found: {global_table_name}"),
            )
        })?;

        let replica_settings: Vec<Value> = gt
            .replication_group
            .iter()
            .map(|r| {
                json!({
                    "RegionName": r.region_name,
                    "ReplicaStatus": r.replica_status,
                    "ReplicaProvisionedReadCapacityUnits": 0,
                    "ReplicaProvisionedWriteCapacityUnits": 0
                })
            })
            .collect();

        Self::ok_json(json!({
            "GlobalTableName": gt.global_table_name,
            "ReplicaSettings": replica_settings
        }))
    }

    fn list_global_tables(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length(
            "exclusiveStartGlobalTableName",
            body["ExclusiveStartGlobalTableName"].as_str(),
            3,
            255,
        )?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, i64::MAX)?;
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let state = self.state.read();
        let tables: Vec<Value> = state
            .global_tables
            .values()
            .take(limit)
            .map(|gt| {
                json!({
                    "GlobalTableName": gt.global_table_name,
                    "ReplicationGroup": gt.replication_group.iter().map(|r| json!({
                        "RegionName": r.region_name
                    })).collect::<Vec<_>>()
                })
            })
            .collect();

        Self::ok_json(json!({
            "GlobalTables": tables
        }))
    }

    fn update_global_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let global_table_name = require_str(&body, "GlobalTableName")?;
        validate_string_length("globalTableName", global_table_name, 3, 255)?;
        validate_required("replicaUpdates", &body["ReplicaUpdates"])?;

        let mut state = self.state.write();
        let gt = state
            .global_tables
            .get_mut(global_table_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "GlobalTableNotFoundException",
                    format!("Global table not found: {global_table_name}"),
                )
            })?;

        if let Some(updates) = body["ReplicaUpdates"].as_array() {
            for update in updates {
                if let Some(create) = update["Create"].as_object() {
                    if let Some(region) = create.get("RegionName").and_then(|v| v.as_str()) {
                        gt.replication_group.push(ReplicaDescription {
                            region_name: region.to_string(),
                            replica_status: "ACTIVE".to_string(),
                        });
                    }
                }
                if let Some(delete) = update["Delete"].as_object() {
                    if let Some(region) = delete.get("RegionName").and_then(|v| v.as_str()) {
                        gt.replication_group.retain(|r| r.region_name != region);
                    }
                }
            }
        }

        Self::ok_json(json!({
            "GlobalTableDescription": {
                "GlobalTableName": gt.global_table_name,
                "GlobalTableArn": gt.global_table_arn,
                "GlobalTableStatus": gt.global_table_status,
                "CreationDateTime": gt.creation_date.timestamp() as f64,
                "ReplicationGroup": gt.replication_group.iter().map(|r| json!({
                    "RegionName": r.region_name,
                    "ReplicaStatus": r.replica_status
                })).collect::<Vec<_>>()
            }
        }))
    }

    fn update_global_table_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let global_table_name = require_str(&body, "GlobalTableName")?;
        validate_string_length("globalTableName", global_table_name, 3, 255)?;
        validate_optional_enum_value(
            "globalTableBillingMode",
            &body["GlobalTableBillingMode"],
            &["PROVISIONED", "PAY_PER_REQUEST"],
        )?;
        validate_optional_range_i64(
            "globalTableProvisionedWriteCapacityUnits",
            body["GlobalTableProvisionedWriteCapacityUnits"].as_i64(),
            1,
            i64::MAX,
        )?;

        let state = self.state.read();
        let gt = state.global_tables.get(global_table_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "GlobalTableNotFoundException",
                format!("Global table not found: {global_table_name}"),
            )
        })?;

        let replica_settings: Vec<Value> = gt
            .replication_group
            .iter()
            .map(|r| {
                json!({
                    "RegionName": r.region_name,
                    "ReplicaStatus": r.replica_status,
                    "ReplicaProvisionedReadCapacityUnits": 0,
                    "ReplicaProvisionedWriteCapacityUnits": 0
                })
            })
            .collect();

        Self::ok_json(json!({
            "GlobalTableName": gt.global_table_name,
            "ReplicaSettings": replica_settings
        }))
    }

    fn describe_table_replica_auto_scaling(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        Self::ok_json(json!({
            "TableAutoScalingDescription": {
                "TableName": table.name,
                "TableStatus": table.status,
                "Replicas": []
            }
        }))
    }

    fn update_table_replica_auto_scaling(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        Self::ok_json(json!({
            "TableAutoScalingDescription": {
                "TableName": table.name,
                "TableStatus": table.status,
                "Replicas": []
            }
        }))
    }

    // ── Kinesis Streaming ──────────────────────────────────────────────

    fn enable_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let stream_arn = require_str(&body, "StreamArn")?;
        let precision = body["EnableKinesisStreamingConfiguration"]
            ["ApproximateCreationDateTimePrecision"]
            .as_str()
            .unwrap_or("MILLISECOND");

        let mut state = self.state.write();
        let table = get_table_mut(&mut state.tables, table_name)?;

        table.kinesis_destinations.push(KinesisDestination {
            stream_arn: stream_arn.to_string(),
            destination_status: "ACTIVE".to_string(),
            approximate_creation_date_time_precision: precision.to_string(),
        });

        Self::ok_json(json!({
            "TableName": table_name,
            "StreamArn": stream_arn,
            "DestinationStatus": "ACTIVE",
            "EnableKinesisStreamingConfiguration": {
                "ApproximateCreationDateTimePrecision": precision
            }
        }))
    }

    fn disable_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let stream_arn = require_str(&body, "StreamArn")?;

        let mut state = self.state.write();
        let table = get_table_mut(&mut state.tables, table_name)?;

        if let Some(dest) = table
            .kinesis_destinations
            .iter_mut()
            .find(|d| d.stream_arn == stream_arn)
        {
            dest.destination_status = "DISABLED".to_string();
        }

        Self::ok_json(json!({
            "TableName": table_name,
            "StreamArn": stream_arn,
            "DestinationStatus": "DISABLED"
        }))
    }

    fn describe_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let destinations: Vec<Value> = table
            .kinesis_destinations
            .iter()
            .map(|d| {
                json!({
                    "StreamArn": d.stream_arn,
                    "DestinationStatus": d.destination_status,
                    "ApproximateCreationDateTimePrecision": d.approximate_creation_date_time_precision
                })
            })
            .collect();

        Self::ok_json(json!({
            "TableName": table_name,
            "KinesisDataStreamDestinations": destinations
        }))
    }

    fn update_kinesis_streaming_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let stream_arn = require_str(&body, "StreamArn")?;
        let precision = body["UpdateKinesisStreamingConfiguration"]
            ["ApproximateCreationDateTimePrecision"]
            .as_str()
            .unwrap_or("MILLISECOND");

        let mut state = self.state.write();
        let table = get_table_mut(&mut state.tables, table_name)?;

        if let Some(dest) = table
            .kinesis_destinations
            .iter_mut()
            .find(|d| d.stream_arn == stream_arn)
        {
            dest.approximate_creation_date_time_precision = precision.to_string();
        }

        Self::ok_json(json!({
            "TableName": table_name,
            "StreamArn": stream_arn,
            "DestinationStatus": "ACTIVE",
            "UpdateKinesisStreamingConfiguration": {
                "ApproximateCreationDateTimePrecision": precision
            }
        }))
    }

    // ── Contributor Insights ───────────────────────────────────────────

    fn describe_contributor_insights(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let index_name = body["IndexName"].as_str();

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let top = table.top_contributors(10);
        let contributors: Vec<Value> = top
            .iter()
            .map(|(key, count)| {
                json!({
                    "Key": key,
                    "Count": count
                })
            })
            .collect();

        let mut result = json!({
            "TableName": table_name,
            "ContributorInsightsStatus": table.contributor_insights_status,
            "ContributorInsightsRuleList": ["DynamoDBContributorInsights"],
            "TopContributors": contributors
        });
        if let Some(idx) = index_name {
            result["IndexName"] = json!(idx);
        }

        Self::ok_json(result)
    }

    fn update_contributor_insights(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;
        let action = require_str(&body, "ContributorInsightsAction")?;
        let index_name = body["IndexName"].as_str();

        let mut state = self.state.write();
        let table = get_table_mut(&mut state.tables, table_name)?;

        let status = match action {
            "ENABLE" => "ENABLED",
            "DISABLE" => "DISABLED",
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Invalid ContributorInsightsAction: {action}"),
                ))
            }
        };
        table.contributor_insights_status = status.to_string();
        if status == "DISABLED" {
            table.contributor_insights_counters.clear();
        }

        let mut result = json!({
            "TableName": table_name,
            "ContributorInsightsStatus": status
        });
        if let Some(idx) = index_name {
            result["IndexName"] = json!(idx);
        }

        Self::ok_json(result)
    }

    fn list_contributor_insights(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length("tableName", body["TableName"].as_str(), 1, 1024)?;
        validate_optional_range_i64("maxResults", body["MaxResults"].as_i64(), 0, 100)?;
        let table_name = body["TableName"].as_str();

        let state = self.state.read();
        let summaries: Vec<Value> = state
            .tables
            .values()
            .filter(|t| table_name.is_none() || table_name == Some(t.name.as_str()))
            .map(|t| {
                json!({
                    "TableName": t.name,
                    "ContributorInsightsStatus": t.contributor_insights_status
                })
            })
            .collect();

        Self::ok_json(json!({
            "ContributorInsightsSummaries": summaries
        }))
    }

    // ── Import/Export ──────────────────────────────────────────────────

    fn export_table_to_point_in_time(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_arn = require_str(&body, "TableArn")?;
        let s3_bucket = require_str(&body, "S3Bucket")?;
        let s3_prefix = body["S3Prefix"].as_str();
        let export_format = body["ExportFormat"].as_str().unwrap_or("DYNAMODB_JSON");

        let state = self.state.read();
        // Verify table exists and get items
        let table = find_table_by_arn(&state.tables, table_arn)?;
        let items = table.items.clone();
        let item_count = items.len() as i64;

        let now = Utc::now();
        let export_arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}/export/{}",
            state.region,
            state.account_id,
            table_arn.rsplit('/').next().unwrap_or("unknown"),
            uuid::Uuid::new_v4()
        );

        drop(state);

        // Serialize items as JSON Lines and write to S3
        let mut json_lines = String::new();
        for item in &items {
            let item_json = if export_format == "DYNAMODB_JSON" {
                json!({ "Item": item })
            } else {
                json!(item)
            };
            json_lines.push_str(&serde_json::to_string(&item_json).unwrap_or_default());
            json_lines.push('\n');
        }
        let data_size = json_lines.len() as i64;

        // Build S3 key for the export data
        let s3_key = if let Some(prefix) = s3_prefix {
            format!("{prefix}/data/manifest-files.json")
        } else {
            "data/manifest-files.json".to_string()
        };

        // Write to S3 if we have access to S3 state
        let mut export_failed = false;
        let mut failure_reason = String::new();
        if let Some(ref s3_state) = self.s3_state {
            let mut s3 = s3_state.write();
            if let Some(bucket) = s3.buckets.get_mut(s3_bucket) {
                let etag = uuid::Uuid::new_v4().to_string().replace('-', "");
                let obj = fakecloud_s3::state::S3Object {
                    key: s3_key.clone(),
                    data: bytes::Bytes::from(json_lines),
                    content_type: "application/json".to_string(),
                    etag,
                    size: data_size as u64,
                    last_modified: now,
                    metadata: HashMap::new(),
                    storage_class: "STANDARD".to_string(),
                    tags: HashMap::new(),
                    acl_grants: Vec::new(),
                    acl_owner_id: None,
                    parts_count: None,
                    part_sizes: None,
                    sse_algorithm: None,
                    sse_kms_key_id: None,
                    bucket_key_enabled: None,
                    version_id: None,
                    is_delete_marker: false,
                    content_encoding: None,
                    website_redirect_location: None,
                    restore_ongoing: None,
                    restore_expiry: None,
                    checksum_algorithm: None,
                    checksum_value: None,
                    lock_mode: None,
                    lock_retain_until: None,
                    lock_legal_hold: None,
                };
                bucket.objects.insert(s3_key, obj);
            } else {
                export_failed = true;
                failure_reason = format!("S3 bucket does not exist: {s3_bucket}");
            }
        }

        let export_status = if export_failed { "FAILED" } else { "COMPLETED" };

        let export = ExportDescription {
            export_arn: export_arn.clone(),
            export_status: export_status.to_string(),
            table_arn: table_arn.to_string(),
            s3_bucket: s3_bucket.to_string(),
            s3_prefix: s3_prefix.map(|s| s.to_string()),
            export_format: export_format.to_string(),
            start_time: now,
            end_time: now,
            export_time: now,
            item_count,
            billed_size_bytes: data_size,
        };

        let mut state = self.state.write();
        state.exports.insert(export_arn.clone(), export);

        let mut response = json!({
            "ExportDescription": {
                "ExportArn": export_arn,
                "ExportStatus": export_status,
                "TableArn": table_arn,
                "S3Bucket": s3_bucket,
                "S3Prefix": s3_prefix,
                "ExportFormat": export_format,
                "StartTime": now.timestamp() as f64,
                "EndTime": now.timestamp() as f64,
                "ExportTime": now.timestamp() as f64,
                "ItemCount": item_count,
                "BilledSizeBytes": data_size
            }
        });
        if export_failed {
            response["ExportDescription"]["FailureCode"] = json!("S3NoSuchBucket");
            response["ExportDescription"]["FailureMessage"] = json!(failure_reason);
        }
        Self::ok_json(response)
    }

    fn describe_export(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let export_arn = require_str(&body, "ExportArn")?;

        let state = self.state.read();
        let export = state.exports.get(export_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ExportNotFoundException",
                format!("Export not found: {export_arn}"),
            )
        })?;

        Self::ok_json(json!({
            "ExportDescription": {
                "ExportArn": export.export_arn,
                "ExportStatus": export.export_status,
                "TableArn": export.table_arn,
                "S3Bucket": export.s3_bucket,
                "S3Prefix": export.s3_prefix,
                "ExportFormat": export.export_format,
                "StartTime": export.start_time.timestamp() as f64,
                "EndTime": export.end_time.timestamp() as f64,
                "ExportTime": export.export_time.timestamp() as f64,
                "ItemCount": export.item_count,
                "BilledSizeBytes": export.billed_size_bytes
            }
        }))
    }

    fn list_exports(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length("tableArn", body["TableArn"].as_str(), 1, 1024)?;
        validate_optional_range_i64("maxResults", body["MaxResults"].as_i64(), 1, 25)?;
        let table_arn = body["TableArn"].as_str();

        let state = self.state.read();
        let summaries: Vec<Value> = state
            .exports
            .values()
            .filter(|e| table_arn.is_none() || table_arn == Some(e.table_arn.as_str()))
            .map(|e| {
                json!({
                    "ExportArn": e.export_arn,
                    "ExportStatus": e.export_status,
                    "TableArn": e.table_arn
                })
            })
            .collect();

        Self::ok_json(json!({
            "ExportSummaries": summaries
        }))
    }

    fn import_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let input_format = require_str(&body, "InputFormat")?;
        let s3_source = body["S3BucketSource"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "S3BucketSource is required",
            )
        })?;
        let s3_bucket = s3_source
            .get("S3Bucket")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let s3_key_prefix = s3_source
            .get("S3KeyPrefix")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let table_params = body["TableCreationParameters"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TableCreationParameters is required",
            )
        })?;
        let table_name = table_params
            .get("TableName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "TableCreationParameters.TableName is required",
                )
            })?;

        let key_schema = parse_key_schema(table_params.get("KeySchema").unwrap_or(&Value::Null))?;
        let attribute_definitions = parse_attribute_definitions(
            table_params
                .get("AttributeDefinitions")
                .unwrap_or(&Value::Null),
        )?;

        // Read items from S3 if we have access
        let mut imported_items: Vec<HashMap<String, Value>> = Vec::new();
        let mut processed_size_bytes: i64 = 0;
        if let Some(ref s3_state) = self.s3_state {
            let s3 = s3_state.read();
            let bucket = s3.buckets.get(s3_bucket).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ImportConflictException",
                    format!("S3 bucket does not exist: {s3_bucket}"),
                )
            })?;
            // Find all objects under the prefix and try to parse JSON Lines from each
            let prefix = if s3_key_prefix.is_empty() {
                String::new()
            } else {
                s3_key_prefix.to_string()
            };
            for (key, obj) in &bucket.objects {
                if !prefix.is_empty() && !key.starts_with(&prefix) {
                    continue;
                }
                let data = std::str::from_utf8(&obj.data).unwrap_or("");
                processed_size_bytes += obj.size as i64;
                for line in data.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    if let Ok(parsed) = serde_json::from_str::<Value>(line) {
                        // DYNAMODB_JSON format wraps items in {"Item": {...}}
                        let item = if input_format == "DYNAMODB_JSON" {
                            if let Some(item_obj) = parsed.get("Item") {
                                item_obj.as_object().cloned().unwrap_or_default()
                            } else {
                                parsed.as_object().cloned().unwrap_or_default()
                            }
                        } else {
                            parsed.as_object().cloned().unwrap_or_default()
                        };
                        if !item.is_empty() {
                            imported_items
                                .push(item.into_iter().collect::<HashMap<String, Value>>());
                        }
                    }
                }
            }
        }

        let mut state = self.state.write();

        if state.tables.contains_key(table_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceInUseException",
                format!("Table already exists: {table_name}"),
            ));
        }

        let now = Utc::now();
        let table_arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}",
            state.region, state.account_id, table_name
        );
        let import_arn = format!(
            "arn:aws:dynamodb:{}:{}:table/{}/import/{}",
            state.region,
            state.account_id,
            table_name,
            uuid::Uuid::new_v4()
        );

        let processed_item_count = imported_items.len() as i64;

        let mut table = DynamoTable {
            name: table_name.to_string(),
            arn: table_arn.clone(),
            key_schema,
            attribute_definitions,
            provisioned_throughput: ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            },
            items: imported_items,
            gsi: Vec::new(),
            lsi: Vec::new(),
            tags: HashMap::new(),
            created_at: now,
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: "PAY_PER_REQUEST".to_string(),
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: Vec::new(),
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: HashMap::new(),
            stream_enabled: false,
            stream_view_type: None,
            stream_arn: None,
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,
        };
        table.recalculate_stats();
        state.tables.insert(table_name.to_string(), table);

        let import_desc = ImportDescription {
            import_arn: import_arn.clone(),
            import_status: "COMPLETED".to_string(),
            table_arn: table_arn.clone(),
            table_name: table_name.to_string(),
            s3_bucket_source: s3_bucket.to_string(),
            input_format: input_format.to_string(),
            start_time: now,
            end_time: now,
            processed_item_count,
            processed_size_bytes,
        };
        state.imports.insert(import_arn.clone(), import_desc);

        let table_ref = state.tables.get(table_name).unwrap();
        let ks: Vec<Value> = table_ref
            .key_schema
            .iter()
            .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
            .collect();
        let ad: Vec<Value> = table_ref
            .attribute_definitions
            .iter()
            .map(|a| json!({"AttributeName": a.attribute_name, "AttributeType": a.attribute_type}))
            .collect();

        Self::ok_json(json!({
            "ImportTableDescription": {
                "ImportArn": import_arn,
                "ImportStatus": "COMPLETED",
                "TableArn": table_arn,
                "TableId": uuid::Uuid::new_v4().to_string(),
                "S3BucketSource": {
                    "S3Bucket": s3_bucket
                },
                "InputFormat": input_format,
                "TableCreationParameters": {
                    "TableName": table_name,
                    "KeySchema": ks,
                    "AttributeDefinitions": ad
                },
                "StartTime": now.timestamp() as f64,
                "EndTime": now.timestamp() as f64,
                "ProcessedItemCount": processed_item_count,
                "ProcessedSizeBytes": processed_size_bytes
            }
        }))
    }

    fn describe_import(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let import_arn = require_str(&body, "ImportArn")?;

        let state = self.state.read();
        let import = state.imports.get(import_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ImportNotFoundException",
                format!("Import not found: {import_arn}"),
            )
        })?;

        Self::ok_json(json!({
            "ImportTableDescription": {
                "ImportArn": import.import_arn,
                "ImportStatus": import.import_status,
                "TableArn": import.table_arn,
                "S3BucketSource": {
                    "S3Bucket": import.s3_bucket_source
                },
                "InputFormat": import.input_format,
                "StartTime": import.start_time.timestamp() as f64,
                "EndTime": import.end_time.timestamp() as f64,
                "ProcessedItemCount": import.processed_item_count,
                "ProcessedSizeBytes": import.processed_size_bytes
            }
        }))
    }

    fn list_imports(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        validate_optional_string_length("tableArn", body["TableArn"].as_str(), 1, 1024)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 112, 1024)?;
        validate_optional_range_i64("pageSize", body["PageSize"].as_i64(), 1, 25)?;
        let table_arn = body["TableArn"].as_str();

        let state = self.state.read();
        let summaries: Vec<Value> = state
            .imports
            .values()
            .filter(|i| table_arn.is_none() || table_arn == Some(i.table_arn.as_str()))
            .map(|i| {
                json!({
                    "ImportArn": i.import_arn,
                    "ImportStatus": i.import_status,
                    "TableArn": i.table_arn
                })
            })
            .collect();

        Self::ok_json(json!({
            "ImportSummaryList": summaries
        }))
    }
}

#[async_trait]
impl AwsService for DynamoDbService {
    fn service_name(&self) -> &str {
        "dynamodb"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateTable" => self.create_table(&req),
            "DeleteTable" => self.delete_table(&req),
            "DescribeTable" => self.describe_table(&req),
            "ListTables" => self.list_tables(&req),
            "UpdateTable" => self.update_table(&req),
            "PutItem" => self.put_item(&req),
            "GetItem" => self.get_item(&req),
            "DeleteItem" => self.delete_item(&req),
            "UpdateItem" => self.update_item(&req),
            "Query" => self.query(&req),
            "Scan" => self.scan(&req),
            "BatchGetItem" => self.batch_get_item(&req),
            "BatchWriteItem" => self.batch_write_item(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsOfResource" => self.list_tags_of_resource(&req),
            "TransactGetItems" => self.transact_get_items(&req),
            "TransactWriteItems" => self.transact_write_items(&req),
            "ExecuteStatement" => self.execute_statement(&req),
            "BatchExecuteStatement" => self.batch_execute_statement(&req),
            "ExecuteTransaction" => self.execute_transaction(&req),
            "UpdateTimeToLive" => self.update_time_to_live(&req),
            "DescribeTimeToLive" => self.describe_time_to_live(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            // Stubs
            "DescribeEndpoints" => self.describe_endpoints(&req),
            "DescribeLimits" => self.describe_limits(&req),
            // Backups
            "CreateBackup" => self.create_backup(&req),
            "DeleteBackup" => self.delete_backup(&req),
            "DescribeBackup" => self.describe_backup(&req),
            "ListBackups" => self.list_backups(&req),
            "RestoreTableFromBackup" => self.restore_table_from_backup(&req),
            "RestoreTableToPointInTime" => self.restore_table_to_point_in_time(&req),
            "UpdateContinuousBackups" => self.update_continuous_backups(&req),
            "DescribeContinuousBackups" => self.describe_continuous_backups(&req),
            // Global tables
            "CreateGlobalTable" => self.create_global_table(&req),
            "DescribeGlobalTable" => self.describe_global_table(&req),
            "DescribeGlobalTableSettings" => self.describe_global_table_settings(&req),
            "ListGlobalTables" => self.list_global_tables(&req),
            "UpdateGlobalTable" => self.update_global_table(&req),
            "UpdateGlobalTableSettings" => self.update_global_table_settings(&req),
            "DescribeTableReplicaAutoScaling" => self.describe_table_replica_auto_scaling(&req),
            "UpdateTableReplicaAutoScaling" => self.update_table_replica_auto_scaling(&req),
            // Kinesis streaming
            "EnableKinesisStreamingDestination" => self.enable_kinesis_streaming_destination(&req),
            "DisableKinesisStreamingDestination" => {
                self.disable_kinesis_streaming_destination(&req)
            }
            "DescribeKinesisStreamingDestination" => {
                self.describe_kinesis_streaming_destination(&req)
            }
            "UpdateKinesisStreamingDestination" => self.update_kinesis_streaming_destination(&req),
            // Contributor insights
            "DescribeContributorInsights" => self.describe_contributor_insights(&req),
            "UpdateContributorInsights" => self.update_contributor_insights(&req),
            "ListContributorInsights" => self.list_contributor_insights(&req),
            // Import/Export
            "ExportTableToPointInTime" => self.export_table_to_point_in_time(&req),
            "DescribeExport" => self.describe_export(&req),
            "ListExports" => self.list_exports(&req),
            "ImportTable" => self.import_table(&req),
            "DescribeImport" => self.describe_import(&req),
            "ListImports" => self.list_imports(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "dynamodb",
                &req.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateTable",
            "DeleteTable",
            "DescribeTable",
            "ListTables",
            "UpdateTable",
            "PutItem",
            "GetItem",
            "DeleteItem",
            "UpdateItem",
            "Query",
            "Scan",
            "BatchGetItem",
            "BatchWriteItem",
            "TagResource",
            "UntagResource",
            "ListTagsOfResource",
            "TransactGetItems",
            "TransactWriteItems",
            "ExecuteStatement",
            "BatchExecuteStatement",
            "ExecuteTransaction",
            "UpdateTimeToLive",
            "DescribeTimeToLive",
            "PutResourcePolicy",
            "GetResourcePolicy",
            "DeleteResourcePolicy",
            "DescribeEndpoints",
            "DescribeLimits",
            "CreateBackup",
            "DeleteBackup",
            "DescribeBackup",
            "ListBackups",
            "RestoreTableFromBackup",
            "RestoreTableToPointInTime",
            "UpdateContinuousBackups",
            "DescribeContinuousBackups",
            "CreateGlobalTable",
            "DescribeGlobalTable",
            "DescribeGlobalTableSettings",
            "ListGlobalTables",
            "UpdateGlobalTable",
            "UpdateGlobalTableSettings",
            "DescribeTableReplicaAutoScaling",
            "UpdateTableReplicaAutoScaling",
            "EnableKinesisStreamingDestination",
            "DisableKinesisStreamingDestination",
            "DescribeKinesisStreamingDestination",
            "UpdateKinesisStreamingDestination",
            "DescribeContributorInsights",
            "UpdateContributorInsights",
            "ListContributorInsights",
            "ExportTableToPointInTime",
            "DescribeExport",
            "ListExports",
            "ImportTable",
            "DescribeImport",
            "ListImports",
        ]
    }
}

// ── Helper functions ────────────────────────────────────────────────────

fn require_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body[field].as_str().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("{field} is required"),
        )
    })
}

fn require_object(
    body: &Value,
    field: &str,
) -> Result<HashMap<String, AttributeValue>, AwsServiceError> {
    let obj = body[field].as_object().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("{field} is required"),
        )
    })?;
    Ok(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

fn get_table<'a>(
    tables: &'a HashMap<String, DynamoTable>,
    name: &str,
) -> Result<&'a DynamoTable, AwsServiceError> {
    tables.get(name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("Requested resource not found: Table: {name} not found"),
        )
    })
}

fn get_table_mut<'a>(
    tables: &'a mut HashMap<String, DynamoTable>,
    name: &str,
) -> Result<&'a mut DynamoTable, AwsServiceError> {
    tables.get_mut(name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("Requested resource not found: Table: {name} not found"),
        )
    })
}

fn find_table_by_arn<'a>(
    tables: &'a HashMap<String, DynamoTable>,
    arn: &str,
) -> Result<&'a DynamoTable, AwsServiceError> {
    tables.values().find(|t| t.arn == arn).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("Requested resource not found: {arn}"),
        )
    })
}

fn find_table_by_arn_mut<'a>(
    tables: &'a mut HashMap<String, DynamoTable>,
    arn: &str,
) -> Result<&'a mut DynamoTable, AwsServiceError> {
    tables.values_mut().find(|t| t.arn == arn).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("Requested resource not found: {arn}"),
        )
    })
}

fn parse_key_schema(val: &Value) -> Result<Vec<KeySchemaElement>, AwsServiceError> {
    let arr = val.as_array().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "KeySchema is required",
        )
    })?;
    Ok(arr
        .iter()
        .map(|elem| KeySchemaElement {
            attribute_name: elem["AttributeName"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            key_type: elem["KeyType"].as_str().unwrap_or("HASH").to_string(),
        })
        .collect())
}

fn parse_attribute_definitions(val: &Value) -> Result<Vec<AttributeDefinition>, AwsServiceError> {
    let arr = val.as_array().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "AttributeDefinitions is required",
        )
    })?;
    Ok(arr
        .iter()
        .map(|elem| AttributeDefinition {
            attribute_name: elem["AttributeName"]
                .as_str()
                .unwrap_or_default()
                .to_string(),
            attribute_type: elem["AttributeType"].as_str().unwrap_or("S").to_string(),
        })
        .collect())
}

fn parse_provisioned_throughput(val: &Value) -> Result<ProvisionedThroughput, AwsServiceError> {
    Ok(ProvisionedThroughput {
        read_capacity_units: val["ReadCapacityUnits"].as_i64().unwrap_or(5),
        write_capacity_units: val["WriteCapacityUnits"].as_i64().unwrap_or(5),
    })
}

fn parse_gsi(val: &Value) -> Vec<GlobalSecondaryIndex> {
    let Some(arr) = val.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|g| {
            Some(GlobalSecondaryIndex {
                index_name: g["IndexName"].as_str()?.to_string(),
                key_schema: parse_key_schema(&g["KeySchema"]).ok()?,
                projection: parse_projection(&g["Projection"]),
                provisioned_throughput: parse_provisioned_throughput(&g["ProvisionedThroughput"])
                    .ok(),
            })
        })
        .collect()
}

fn parse_lsi(val: &Value) -> Vec<LocalSecondaryIndex> {
    let Some(arr) = val.as_array() else {
        return Vec::new();
    };
    arr.iter()
        .filter_map(|l| {
            Some(LocalSecondaryIndex {
                index_name: l["IndexName"].as_str()?.to_string(),
                key_schema: parse_key_schema(&l["KeySchema"]).ok()?,
                projection: parse_projection(&l["Projection"]),
            })
        })
        .collect()
}

fn parse_projection(val: &Value) -> Projection {
    Projection {
        projection_type: val["ProjectionType"].as_str().unwrap_or("ALL").to_string(),
        non_key_attributes: val["NonKeyAttributes"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default(),
    }
}

fn parse_tags(val: &Value) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    if let Some(arr) = val.as_array() {
        for tag in arr {
            if let (Some(k), Some(v)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                tags.insert(k.to_string(), v.to_string());
            }
        }
    }
    tags
}

fn parse_expression_attribute_names(body: &Value) -> HashMap<String, String> {
    let mut names = HashMap::new();
    if let Some(obj) = body["ExpressionAttributeNames"].as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                names.insert(k.clone(), s.to_string());
            }
        }
    }
    names
}

fn parse_expression_attribute_values(body: &Value) -> HashMap<String, Value> {
    let mut values = HashMap::new();
    if let Some(obj) = body["ExpressionAttributeValues"].as_object() {
        for (k, v) in obj {
            values.insert(k.clone(), v.clone());
        }
    }
    values
}

fn resolve_attr_name(name: &str, expr_attr_names: &HashMap<String, String>) -> String {
    if name.starts_with('#') {
        expr_attr_names
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    } else {
        name.to_string()
    }
}

fn extract_key(
    table: &DynamoTable,
    item: &HashMap<String, AttributeValue>,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    let hash_key = table.hash_key_name();
    if let Some(v) = item.get(hash_key) {
        key.insert(hash_key.to_string(), v.clone());
    }
    if let Some(range_key) = table.range_key_name() {
        if let Some(v) = item.get(range_key) {
            key.insert(range_key.to_string(), v.clone());
        }
    }
    key
}

/// Parse a JSON object into a key map (used for ExclusiveStartKey).
fn parse_key_map(value: &Value) -> Option<HashMap<String, AttributeValue>> {
    let obj = value.as_object()?;
    if obj.is_empty() {
        return None;
    }
    Some(obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
}

/// Check whether an item's key attributes match the given key map.
fn item_matches_key(
    item: &HashMap<String, AttributeValue>,
    key: &HashMap<String, AttributeValue>,
    hash_key_name: &str,
    range_key_name: Option<&str>,
) -> bool {
    let hash_match = match (item.get(hash_key_name), key.get(hash_key_name)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    };
    if !hash_match {
        return false;
    }
    match range_key_name {
        Some(rk) => match (item.get(rk), key.get(rk)) {
            (Some(a), Some(b)) => a == b,
            (None, None) => true,
            _ => false,
        },
        None => true,
    }
}

/// Extract the primary key from an item given explicit key attribute names.
fn extract_key_for_schema(
    item: &HashMap<String, AttributeValue>,
    hash_key_name: &str,
    range_key_name: Option<&str>,
) -> HashMap<String, AttributeValue> {
    let mut key = HashMap::new();
    if let Some(v) = item.get(hash_key_name) {
        key.insert(hash_key_name.to_string(), v.clone());
    }
    if let Some(rk) = range_key_name {
        if let Some(v) = item.get(rk) {
            key.insert(rk.to_string(), v.clone());
        }
    }
    key
}

fn validate_key_in_item(
    table: &DynamoTable,
    item: &HashMap<String, AttributeValue>,
) -> Result<(), AwsServiceError> {
    let hash_key = table.hash_key_name();
    if !item.contains_key(hash_key) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("Missing the key {hash_key} in the item"),
        ));
    }
    if let Some(range_key) = table.range_key_name() {
        if !item.contains_key(range_key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("Missing the key {range_key} in the item"),
            ));
        }
    }
    Ok(())
}

fn validate_key_attributes_in_key(
    table: &DynamoTable,
    key: &HashMap<String, AttributeValue>,
) -> Result<(), AwsServiceError> {
    let hash_key = table.hash_key_name();
    if !key.contains_key(hash_key) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("Missing the key {hash_key} in the item"),
        ));
    }
    Ok(())
}

fn project_item(
    item: &HashMap<String, AttributeValue>,
    body: &Value,
) -> HashMap<String, AttributeValue> {
    let projection = body["ProjectionExpression"].as_str();
    match projection {
        Some(proj) if !proj.is_empty() => {
            let expr_attr_names = parse_expression_attribute_names(body);
            let attrs: Vec<String> = proj
                .split(',')
                .map(|s| resolve_projection_path(s.trim(), &expr_attr_names))
                .collect();
            let mut result = HashMap::new();
            for attr in &attrs {
                if let Some(v) = resolve_nested_path(item, attr) {
                    insert_nested_value(&mut result, attr, v);
                }
            }
            result
        }
        _ => item.clone(),
    }
}

/// Resolve expression attribute names within each segment of a projection path.
/// For example, "people[0].#n" with {"#n": "name"} => "people[0].name".
fn resolve_projection_path(path: &str, expr_attr_names: &HashMap<String, String>) -> String {
    // Split on dots, resolve each part, rejoin
    let mut result = String::new();
    for (i, segment) in path.split('.').enumerate() {
        if i > 0 {
            result.push('.');
        }
        // A segment might be like "#n" or "people[0]" or "#attr[0]"
        if let Some(bracket_pos) = segment.find('[') {
            let key_part = &segment[..bracket_pos];
            let index_part = &segment[bracket_pos..];
            result.push_str(&resolve_attr_name(key_part, expr_attr_names));
            result.push_str(index_part);
        } else {
            result.push_str(&resolve_attr_name(segment, expr_attr_names));
        }
    }
    result
}

/// Resolve a potentially nested path like "a.b.c" or "a[0].b" from an item.
fn resolve_nested_path(item: &HashMap<String, AttributeValue>, path: &str) -> Option<Value> {
    let segments = parse_path_segments(path);
    if segments.is_empty() {
        return None;
    }

    let first = &segments[0];
    let top_key = match first {
        PathSegment::Key(k) => k.as_str(),
        _ => return None,
    };

    let mut current = item.get(top_key)?.clone();

    for segment in &segments[1..] {
        match segment {
            PathSegment::Key(k) => {
                // Navigate into a Map: {"M": {"key": ...}}
                current = current.get("M")?.get(k)?.clone();
            }
            PathSegment::Index(idx) => {
                // Navigate into a List: {"L": [...]}
                current = current.get("L")?.get(*idx)?.clone();
            }
        }
    }

    Some(current)
}

#[derive(Debug)]
enum PathSegment {
    Key(String),
    Index(usize),
}

/// Parse a path like "a.b[0].c" into segments: [Key("a"), Key("b"), Index(0), Key("c")]
fn parse_path_segments(path: &str) -> Vec<PathSegment> {
    let mut segments = Vec::new();
    let mut current = String::new();

    let chars: Vec<char> = path.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '.' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Key(current.clone()));
                    current.clear();
                }
            }
            '[' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Key(current.clone()));
                    current.clear();
                }
                i += 1;
                let mut num = String::new();
                while i < chars.len() && chars[i] != ']' {
                    num.push(chars[i]);
                    i += 1;
                }
                if let Ok(idx) = num.parse::<usize>() {
                    segments.push(PathSegment::Index(idx));
                }
                // skip ']'
            }
            c => {
                current.push(c);
            }
        }
        i += 1;
    }
    if !current.is_empty() {
        segments.push(PathSegment::Key(current));
    }
    segments
}

/// Insert a value at a nested path in the result HashMap.
/// For a path like "a.b", we set result["a"] = {"M": {"b": value}}.
fn insert_nested_value(result: &mut HashMap<String, AttributeValue>, path: &str, value: Value) {
    // Simple case: no nesting
    if !path.contains('.') && !path.contains('[') {
        result.insert(path.to_string(), value);
        return;
    }

    let segments = parse_path_segments(path);
    if segments.is_empty() {
        return;
    }

    let top_key = match &segments[0] {
        PathSegment::Key(k) => k.clone(),
        _ => return,
    };

    if segments.len() == 1 {
        result.insert(top_key, value);
        return;
    }

    // For nested paths, wrap the value back into the nested structure
    let wrapped = wrap_value_in_path(&segments[1..], value);
    // Merge into existing value if present
    let existing = result.remove(&top_key);
    let merged = match existing {
        Some(existing) => merge_attribute_values(existing, wrapped),
        None => wrapped,
    };
    result.insert(top_key, merged);
}

/// Wrap a value in the nested path structure.
fn wrap_value_in_path(segments: &[PathSegment], value: Value) -> Value {
    if segments.is_empty() {
        return value;
    }
    let inner = wrap_value_in_path(&segments[1..], value);
    match &segments[0] {
        PathSegment::Key(k) => {
            json!({"M": {k.clone(): inner}})
        }
        PathSegment::Index(idx) => {
            let mut arr = vec![Value::Null; idx + 1];
            arr[*idx] = inner;
            json!({"L": arr})
        }
    }
}

/// Merge two attribute values (for overlapping projections).
fn merge_attribute_values(a: Value, b: Value) -> Value {
    if let (Some(a_map), Some(b_map)) = (
        a.get("M").and_then(|v| v.as_object()),
        b.get("M").and_then(|v| v.as_object()),
    ) {
        let mut merged = a_map.clone();
        for (k, v) in b_map {
            if let Some(existing) = merged.get(k) {
                merged.insert(
                    k.clone(),
                    merge_attribute_values(existing.clone(), v.clone()),
                );
            } else {
                merged.insert(k.clone(), v.clone());
            }
        }
        json!({"M": merged})
    } else {
        b
    }
}

fn evaluate_condition(
    condition: &str,
    existing: Option<&HashMap<String, AttributeValue>>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    // ConditionExpression and FilterExpression share the same DynamoDB grammar,
    // so we delegate to evaluate_filter_expression. An empty map models "item
    // doesn't exist" correctly: attribute_exists → false, attribute_not_exists
    // → true, comparisons against missing attributes → None vs Some(val).
    let empty = HashMap::new();
    let item = existing.unwrap_or(&empty);
    if evaluate_filter_expression(condition, item, expr_attr_names, expr_attr_values) {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ConditionalCheckFailedException",
            "The conditional request failed",
        ))
    }
}

fn extract_function_arg<'a>(expr: &'a str, func_name: &str) -> Option<&'a str> {
    // aws-sdk-go v2's expression builder emits function calls with a space
    // between the name and the opening paren (`attribute_exists (#0)`),
    // while hand-written expressions usually don't — accept both.
    let with_paren = format!("{func_name}(");
    let with_space = format!("{func_name} (");
    let rest = expr
        .strip_prefix(&with_paren)
        .or_else(|| expr.strip_prefix(&with_space))?;
    let inner = rest.strip_suffix(')')?;
    Some(inner.trim())
}

fn evaluate_key_condition(
    expr: &str,
    item: &HashMap<String, AttributeValue>,
    hash_key_name: &str,
    _range_key_name: Option<&str>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    let parts: Vec<&str> = split_on_and(expr);
    for part in &parts {
        let part = part.trim();
        if !evaluate_single_key_condition(
            part,
            item,
            hash_key_name,
            expr_attr_names,
            expr_attr_values,
        ) {
            return false;
        }
    }
    true
}

fn split_on_and(expr: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let len = expr.len();
    let mut i = 0;
    let mut depth = 0;
    while i < len {
        let ch = expr.as_bytes()[i];
        if ch == b'(' {
            depth += 1;
        } else if ch == b')' {
            if depth > 0 {
                depth -= 1;
            }
        } else if depth == 0 && i + 5 <= len && expr[i..i + 5].eq_ignore_ascii_case(" AND ") {
            parts.push(&expr[start..i]);
            start = i + 5;
            i = start;
            continue;
        }
        i += 1;
    }
    parts.push(&expr[start..]);
    parts
}

fn split_on_or(expr: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let len = expr.len();
    let mut i = 0;
    let mut depth = 0;
    while i < len {
        let ch = expr.as_bytes()[i];
        if ch == b'(' {
            depth += 1;
        } else if ch == b')' {
            if depth > 0 {
                depth -= 1;
            }
        } else if depth == 0 && i + 4 <= len && expr[i..i + 4].eq_ignore_ascii_case(" OR ") {
            parts.push(&expr[start..i]);
            start = i + 4;
            i = start;
            continue;
        }
        i += 1;
    }
    parts.push(&expr[start..]);
    parts
}

fn evaluate_single_key_condition(
    part: &str,
    item: &HashMap<String, AttributeValue>,
    _hash_key_name: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    let part = part.trim();

    // begins_with(attr, :val) — S type only
    if let Some(rest) = part
        .strip_prefix("begins_with(")
        .or_else(|| part.strip_prefix("begins_with ("))
    {
        if let Some(inner) = rest.strip_suffix(')') {
            let mut split = inner.splitn(2, ',');
            if let (Some(attr_ref), Some(val_ref)) = (split.next(), split.next()) {
                let attr_name = resolve_attr_name(attr_ref.trim(), expr_attr_names);
                let val_ref = val_ref.trim();
                let expected = expr_attr_values.get(val_ref);
                let actual = item.get(&attr_name);
                return match (actual, expected) {
                    (Some(a), Some(e)) => {
                        let a_str = a.get("S").and_then(|v| v.as_str());
                        let e_str = e.get("S").and_then(|v| v.as_str());
                        matches!((a_str, e_str), (Some(a), Some(e)) if a.starts_with(e))
                    }
                    _ => false,
                };
            }
        }
        return false;
    }

    // BETWEEN
    if let Some(between_pos) = part.to_ascii_uppercase().find("BETWEEN") {
        let attr_part = part[..between_pos].trim();
        let attr_name = resolve_attr_name(attr_part, expr_attr_names);
        let range_part = &part[between_pos + 7..];
        if let Some(and_pos) = range_part.to_ascii_uppercase().find(" AND ") {
            let lo_ref = range_part[..and_pos].trim();
            let hi_ref = range_part[and_pos + 5..].trim();
            let lo = expr_attr_values.get(lo_ref);
            let hi = expr_attr_values.get(hi_ref);
            let actual = item.get(&attr_name);
            return match (actual, lo, hi) {
                (Some(a), Some(l), Some(h)) => {
                    compare_attribute_values(Some(a), Some(l)) != std::cmp::Ordering::Less
                        && compare_attribute_values(Some(a), Some(h)) != std::cmp::Ordering::Greater
                }
                _ => false,
            };
        }
    }

    // Simple comparison: attr <op> :val
    for op in &["<=", ">=", "<>", "=", "<", ">"] {
        if let Some(pos) = part.find(op) {
            let left = part[..pos].trim();
            let right = part[pos + op.len()..].trim();
            let attr_name = resolve_attr_name(left, expr_attr_names);
            let expected = expr_attr_values.get(right);
            let actual = item.get(&attr_name);

            return match *op {
                "=" => actual == expected,
                "<>" => actual != expected,
                "<" => compare_attribute_values(actual, expected) == std::cmp::Ordering::Less,
                ">" => compare_attribute_values(actual, expected) == std::cmp::Ordering::Greater,
                "<=" => {
                    let cmp = compare_attribute_values(actual, expected);
                    cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal
                }
                ">=" => {
                    let cmp = compare_attribute_values(actual, expected);
                    cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal
                }
                _ => false,
            };
        }
    }

    false
}

/// Returns the "size" of a DynamoDB attribute value per AWS docs:
/// S → character count, N → always 0 (AWS returns size of internal representation, we approximate),
/// B → byte count, SS/NS/BS → element count, L → element count, M → element count,
/// BOOL/NULL → 1.
fn attribute_size(val: &Value) -> Option<usize> {
    if let Some(s) = val.get("S").and_then(|v| v.as_str()) {
        return Some(s.len());
    }
    if let Some(b) = val.get("B").and_then(|v| v.as_str()) {
        // B is base64-encoded — return decoded byte count
        let decoded_len = base64::engine::general_purpose::STANDARD
            .decode(b)
            .map(|v| v.len())
            .unwrap_or(b.len());
        return Some(decoded_len);
    }
    if let Some(arr) = val.get("SS").and_then(|v| v.as_array()) {
        return Some(arr.len());
    }
    if let Some(arr) = val.get("NS").and_then(|v| v.as_array()) {
        return Some(arr.len());
    }
    if let Some(arr) = val.get("BS").and_then(|v| v.as_array()) {
        return Some(arr.len());
    }
    if let Some(arr) = val.get("L").and_then(|v| v.as_array()) {
        return Some(arr.len());
    }
    if let Some(obj) = val.get("M").and_then(|v| v.as_object()) {
        return Some(obj.len());
    }
    if val.get("N").is_some() {
        // AWS returns numeric representation size; approximate with string length
        return val.get("N").and_then(|v| v.as_str()).map(|s| s.len());
    }
    if val.get("BOOL").is_some() || val.get("NULL").is_some() {
        return Some(1);
    }
    None
}

/// Evaluate a `size(path) op :val` comparison expression.
fn evaluate_size_comparison(
    part: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Option<bool> {
    // Find the closing paren of size(...)
    let open = part.find('(')?;
    let close = part[open..].find(')')? + open;
    let path = part[open + 1..close].trim();
    let remainder = part[close + 1..].trim();

    // Parse operator and value ref
    let (op, val_ref) = if let Some(rest) = remainder.strip_prefix("<=") {
        ("<=", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix(">=") {
        (">=", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix("<>") {
        ("<>", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix('<') {
        ("<", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix('>') {
        (">", rest.trim())
    } else if let Some(rest) = remainder.strip_prefix('=') {
        ("=", rest.trim())
    } else {
        return None;
    };

    let attr_name = resolve_attr_name(path, expr_attr_names);
    let actual = item.get(&attr_name)?;
    let size = attribute_size(actual)? as f64;

    let expected = extract_number(&expr_attr_values.get(val_ref).cloned())?;

    Some(match op {
        "=" => (size - expected).abs() < f64::EPSILON,
        "<>" => (size - expected).abs() >= f64::EPSILON,
        "<" => size < expected,
        ">" => size > expected,
        "<=" => size <= expected,
        ">=" => size >= expected,
        _ => false,
    })
}

fn compare_attribute_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => {
            let a_type = attribute_type_and_value(a);
            let b_type = attribute_type_and_value(b);
            match (a_type, b_type) {
                (Some(("S", a_val)), Some(("S", b_val))) => {
                    let a_str = a_val.as_str().unwrap_or("");
                    let b_str = b_val.as_str().unwrap_or("");
                    a_str.cmp(b_str)
                }
                (Some(("N", a_val)), Some(("N", b_val))) => {
                    let a_num: f64 = a_val.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let b_num: f64 = b_val.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    a_num
                        .partial_cmp(&b_num)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
                (Some(("B", a_val)), Some(("B", b_val))) => {
                    let a_str = a_val.as_str().unwrap_or("");
                    let b_str = b_val.as_str().unwrap_or("");
                    a_str.cmp(b_str)
                }
                _ => std::cmp::Ordering::Equal,
            }
        }
    }
}

fn evaluate_filter_expression(
    expr: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    let trimmed = expr.trim();

    // Split on OR first (lower precedence), respecting parentheses
    let or_parts = split_on_or(trimmed);
    if or_parts.len() > 1 {
        return or_parts.iter().any(|part| {
            evaluate_filter_expression(part.trim(), item, expr_attr_names, expr_attr_values)
        });
    }

    // Then split on AND (higher precedence), respecting parentheses
    let and_parts = split_on_and(trimmed);
    if and_parts.len() > 1 {
        return and_parts.iter().all(|part| {
            evaluate_filter_expression(part.trim(), item, expr_attr_names, expr_attr_values)
        });
    }

    // Strip outer parentheses if present
    let stripped = strip_outer_parens(trimmed);
    if stripped != trimmed {
        return evaluate_filter_expression(stripped, item, expr_attr_names, expr_attr_values);
    }

    // Handle NOT prefix (case-insensitive)
    if trimmed.len() > 4 && trimmed[..4].eq_ignore_ascii_case("NOT ") {
        return !evaluate_filter_expression(&trimmed[4..], item, expr_attr_names, expr_attr_values);
    }

    evaluate_single_filter_condition(trimmed, item, expr_attr_names, expr_attr_values)
}

/// Strip matching outer parentheses from an expression.
fn strip_outer_parens(expr: &str) -> &str {
    let trimmed = expr.trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return trimmed;
    }
    // Verify the outer parens actually match each other
    let inner = &trimmed[1..trimmed.len() - 1];
    let mut depth = 0;
    for ch in inner.bytes() {
        match ch {
            b'(' => depth += 1,
            b')' => {
                if depth == 0 {
                    return trimmed; // closing paren matches something inside, not the outer one
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    if depth == 0 {
        inner
    } else {
        trimmed
    }
}

fn evaluate_single_filter_condition(
    part: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    if let Some(inner) = extract_function_arg(part, "attribute_exists") {
        let attr = resolve_attr_name(inner, expr_attr_names);
        return item.contains_key(&attr);
    }

    if let Some(inner) = extract_function_arg(part, "attribute_not_exists") {
        let attr = resolve_attr_name(inner, expr_attr_names);
        return !item.contains_key(&attr);
    }

    // begins_with only works on S (string) type — not N
    if let Some(rest) = part
        .strip_prefix("begins_with(")
        .or_else(|| part.strip_prefix("begins_with ("))
    {
        if let Some(inner) = rest.strip_suffix(')') {
            let mut split = inner.splitn(2, ',');
            if let (Some(attr_ref), Some(val_ref)) = (split.next(), split.next()) {
                let attr_name = resolve_attr_name(attr_ref.trim(), expr_attr_names);
                let expected = expr_attr_values.get(val_ref.trim());
                let actual = item.get(&attr_name);
                return match (actual, expected) {
                    (Some(a), Some(e)) => {
                        let a_str = a.get("S").and_then(|v| v.as_str());
                        let e_str = e.get("S").and_then(|v| v.as_str());
                        matches!((a_str, e_str), (Some(a), Some(e)) if a.starts_with(e))
                    }
                    _ => false,
                };
            }
        }
    }

    // contains: works on S (substring), SS/NS/BS/L (set membership)
    if let Some(rest) = part
        .strip_prefix("contains(")
        .or_else(|| part.strip_prefix("contains ("))
    {
        if let Some(inner) = rest.strip_suffix(')') {
            let mut split = inner.splitn(2, ',');
            if let (Some(attr_ref), Some(val_ref)) = (split.next(), split.next()) {
                let attr_name = resolve_attr_name(attr_ref.trim(), expr_attr_names);
                let expected = expr_attr_values.get(val_ref.trim());
                let actual = item.get(&attr_name);
                return match (actual, expected) {
                    (Some(a), Some(e)) => {
                        // String substring check (S type only)
                        if let (Some(a_s), Some(e_s)) = (
                            a.get("S").and_then(|v| v.as_str()),
                            e.get("S").and_then(|v| v.as_str()),
                        ) {
                            return a_s.contains(e_s);
                        }
                        // Set/list membership
                        if let Some(set) = a.get("SS").and_then(|v| v.as_array()) {
                            if let Some(val) = e.get("S") {
                                return set.contains(val);
                            }
                        }
                        if let Some(set) = a.get("NS").and_then(|v| v.as_array()) {
                            if let Some(val) = e.get("N") {
                                return set.contains(val);
                            }
                        }
                        if let Some(set) = a.get("BS").and_then(|v| v.as_array()) {
                            if let Some(val) = e.get("B") {
                                return set.contains(val);
                            }
                        }
                        if let Some(list) = a.get("L").and_then(|v| v.as_array()) {
                            return list.contains(e);
                        }
                        false
                    }
                    _ => false,
                };
            }
        }
    }

    // size(path) op :val — attribute size comparison
    if part.starts_with("size(") || part.starts_with("size (") {
        if let Some(result) =
            evaluate_size_comparison(part, item, expr_attr_names, expr_attr_values)
        {
            return result;
        }
    }

    // attribute_type(path, :type)
    if part.starts_with("attribute_type(") || part.starts_with("attribute_type (") {
        if let Some(rest) = part
            .strip_prefix("attribute_type(")
            .or_else(|| part.strip_prefix("attribute_type ("))
        {
            if let Some(inner) = rest.strip_suffix(')') {
                let mut split = inner.splitn(2, ',');
                if let (Some(attr_ref), Some(val_ref)) = (split.next(), split.next()) {
                    let attr_name = resolve_attr_name(attr_ref.trim(), expr_attr_names);
                    let expected_type = expr_attr_values
                        .get(val_ref.trim())
                        .and_then(|v| v.get("S"))
                        .and_then(|v| v.as_str());
                    let actual = item.get(&attr_name);
                    return match (actual, expected_type) {
                        (Some(val), Some(t)) => match t {
                            "S" => val.get("S").is_some(),
                            "N" => val.get("N").is_some(),
                            "B" => val.get("B").is_some(),
                            "BOOL" => val.get("BOOL").is_some(),
                            "NULL" => val.get("NULL").is_some(),
                            "SS" => val.get("SS").is_some(),
                            "NS" => val.get("NS").is_some(),
                            "BS" => val.get("BS").is_some(),
                            "L" => val.get("L").is_some(),
                            "M" => val.get("M").is_some(),
                            _ => false,
                        },
                        _ => false,
                    };
                }
            }
        }
    }

    if let Some((attr_ref, value_refs)) = parse_in_expression(part) {
        let attr_name = resolve_attr_name(attr_ref, expr_attr_names);
        let actual = item.get(&attr_name);
        return evaluate_in_match(actual, &value_refs, expr_attr_values);
    }

    evaluate_single_key_condition(part, item, "", expr_attr_names, expr_attr_values)
}

/// Parse an `attr IN (:v1, :v2, ...)` expression. Mirrors the DynamoDB
/// ConditionExpression / FilterExpression grammar where IN takes a single
/// operand on the left and 1–100 comma-separated value refs inside parens
/// on the right. Case-insensitive; tolerates missing spaces after commas
/// (aws-sdk-go's `expression` builder emits ", " but hand-built expressions
/// often use `strings.Join(..., ",")`). Returns None for non-IN inputs so
/// callers can fall through to their other grammar branches.
fn parse_in_expression(expr: &str) -> Option<(&str, Vec<&str>)> {
    let upper = expr.to_ascii_uppercase();
    let in_pos = upper.find(" IN ")?;
    let attr_ref = expr[..in_pos].trim();
    if attr_ref.is_empty() {
        return None;
    }
    let rest = expr[in_pos + 4..].trim_start();
    let inner = rest.strip_prefix('(')?.strip_suffix(')')?;
    let values: Vec<&str> = inner
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();
    if values.is_empty() {
        return None;
    }
    Some((attr_ref, values))
}

/// Return true iff `actual` equals any of the `value_refs` resolved through
/// `expr_attr_values`. A missing attribute never matches (mirrors AWS, which
/// evaluates `IN` against undefined attributes as false).
fn evaluate_in_match(
    actual: Option<&AttributeValue>,
    value_refs: &[&str],
    expr_attr_values: &HashMap<String, Value>,
) -> bool {
    value_refs.iter().any(|v_ref| {
        let expected = expr_attr_values.get(*v_ref);
        matches!((actual, expected), (Some(a), Some(e)) if a == e)
    })
}

fn apply_update_expression(
    item: &mut HashMap<String, AttributeValue>,
    expr: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    let clauses = parse_update_clauses(expr);
    if clauses.is_empty() && !expr.trim().is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid UpdateExpression: Syntax error; token: \"<expression>\"",
        ));
    }
    for (action, assignments) in &clauses {
        match action.to_ascii_uppercase().as_str() {
            "SET" => {
                for assignment in assignments {
                    apply_set_assignment(item, assignment, expr_attr_names, expr_attr_values)?;
                }
            }
            "REMOVE" => {
                for attr_ref in assignments {
                    let attr = resolve_attr_name(attr_ref.trim(), expr_attr_names);
                    item.remove(&attr);
                }
            }
            "ADD" => {
                for assignment in assignments {
                    apply_add_assignment(item, assignment, expr_attr_names, expr_attr_values)?;
                }
            }
            "DELETE" => {
                for assignment in assignments {
                    apply_delete_assignment(item, assignment, expr_attr_names, expr_attr_values)?;
                }
            }
            other => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!("Invalid UpdateExpression: Invalid action: {}", other),
                ));
            }
        }
    }
    Ok(())
}

fn parse_update_clauses(expr: &str) -> Vec<(String, Vec<String>)> {
    let mut clauses: Vec<(String, Vec<String>)> = Vec::new();
    let upper = expr.to_ascii_uppercase();
    let keywords = ["SET", "REMOVE", "ADD", "DELETE"];
    let mut positions: Vec<(usize, &str)> = Vec::new();

    for kw in &keywords {
        let mut search_from = 0;
        while let Some(pos) = upper[search_from..].find(kw) {
            let abs_pos = search_from + pos;
            let before_ok = abs_pos == 0 || !expr.as_bytes()[abs_pos - 1].is_ascii_alphanumeric();
            let after_pos = abs_pos + kw.len();
            let after_ok =
                after_pos >= expr.len() || !expr.as_bytes()[after_pos].is_ascii_alphanumeric();
            if before_ok && after_ok {
                positions.push((abs_pos, kw));
            }
            search_from = abs_pos + kw.len();
        }
    }

    positions.sort_by_key(|(pos, _)| *pos);

    for (i, &(pos, kw)) in positions.iter().enumerate() {
        let start = pos + kw.len();
        let end = if i + 1 < positions.len() {
            positions[i + 1].0
        } else {
            expr.len()
        };
        let content = expr[start..end].trim();
        let assignments: Vec<String> = content.split(',').map(|s| s.trim().to_string()).collect();
        clauses.push((kw.to_string(), assignments));
    }

    clauses
}

fn apply_set_assignment(
    item: &mut HashMap<String, AttributeValue>,
    assignment: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    let Some((left, right)) = assignment.split_once('=') else {
        return Ok(());
    };

    let left_trimmed = left.trim();
    // Split off a trailing `[N]` list-index suffix so we can resolve the
    // attribute name ref on its own. Without this, `resolve_attr_name` sees
    // "#items[0]" as a whole and misses the `#items` → `items` mapping.
    let (attr_ref, list_index) = match parse_list_index_suffix(left_trimmed) {
        Some((name, idx)) => (name, Some(idx)),
        None => (left_trimmed, None),
    };
    let attr = resolve_attr_name(attr_ref, expr_attr_names);
    let right = right.trim();

    // if_not_exists(attr, :val)
    if let Some(rest) = right
        .strip_prefix("if_not_exists(")
        .or_else(|| right.strip_prefix("if_not_exists ("))
    {
        if let Some(inner) = rest.strip_suffix(')') {
            let mut split = inner.splitn(2, ',');
            if let (Some(check_attr), Some(default_ref)) = (split.next(), split.next()) {
                let check_name = resolve_attr_name(check_attr.trim(), expr_attr_names);
                if !item.contains_key(&check_name) {
                    if let Some(val) = expr_attr_values.get(default_ref.trim()) {
                        item.insert(attr, val.clone());
                    }
                }
                return Ok(());
            }
        }
    }

    // list_append(a, b)
    if let Some(rest) = right
        .strip_prefix("list_append(")
        .or_else(|| right.strip_prefix("list_append ("))
    {
        if let Some(inner) = rest.strip_suffix(')') {
            let mut split = inner.splitn(2, ',');
            if let (Some(a_ref), Some(b_ref)) = (split.next(), split.next()) {
                let a_val = resolve_value(a_ref.trim(), item, expr_attr_names, expr_attr_values);
                let b_val = resolve_value(b_ref.trim(), item, expr_attr_names, expr_attr_values);

                let mut merged = Vec::new();
                if let Some(Value::Object(obj)) = &a_val {
                    if let Some(Value::Array(arr)) = obj.get("L") {
                        merged.extend(arr.clone());
                    }
                }
                if let Some(Value::Object(obj)) = &b_val {
                    if let Some(Value::Array(arr)) = obj.get("L") {
                        merged.extend(arr.clone());
                    }
                }

                item.insert(attr, json!({"L": merged}));
                return Ok(());
            }
        }
    }

    // Arithmetic: attr + :val or attr - :val
    if let Some((arith_left, arith_right, is_add)) = parse_arithmetic(right) {
        let left_val = resolve_value(arith_left.trim(), item, expr_attr_names, expr_attr_values);
        let right_val = resolve_value(arith_right.trim(), item, expr_attr_names, expr_attr_values);

        // Both operands must be numeric (N type)
        let left_num = match extract_number(&left_val) {
            Some(n) => n,
            None if left_val.is_some() => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "An operand in the update expression has an incorrect data type",
                ));
            }
            None => 0.0, // attribute doesn't exist yet — treat as 0
        };
        let right_num = match extract_number(&right_val) {
            Some(n) => n,
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "An operand in the update expression has an incorrect data type",
                ));
            }
        };

        let result = if is_add {
            left_num + right_num
        } else {
            left_num - right_num
        };

        let num_str = if result == result.trunc() {
            format!("{}", result as i64)
        } else {
            format!("{result}")
        };

        item.insert(attr, json!({"N": num_str}));
        return Ok(());
    }

    // Simple assignment
    let val = resolve_value(right, item, expr_attr_names, expr_attr_values);
    if let Some(v) = val {
        match list_index {
            Some(idx) => assign_list_index(item, &attr, idx, v)?,
            None => {
                item.insert(attr, v);
            }
        }
    }

    Ok(())
}

/// Parse a trailing `[N]` list-index suffix off the LHS of a SET assignment.
/// Returns the bare attribute reference and the index, or None when the LHS
/// is a plain attribute (or a path shape we don't yet support).
fn parse_list_index_suffix(path: &str) -> Option<(&str, usize)> {
    let path = path.trim();
    if !path.ends_with(']') {
        return None;
    }
    let open = path.rfind('[')?;
    // Require no further `.` / `[` / `]` inside the bracketed portion and no
    // further path segments after — we only handle the single-index case
    // `name[N]`, not nested shapes like `a.b[0].c`.
    let idx_str = &path[open + 1..path.len() - 1];
    let idx: usize = idx_str.parse().ok()?;
    let name = &path[..open];
    if name.is_empty() || name.contains('[') || name.contains(']') || name.contains('.') {
        return None;
    }
    Some((name, idx))
}

/// Assign a value to a specific index of a `L`-typed attribute. If `idx` is
/// within the current list, replaces that slot; if it's at the end, appends.
/// AWS rejects writes beyond `len`, so we return a `ValidationException` for
/// out-of-range indices and non-list attributes.
fn assign_list_index(
    item: &mut HashMap<String, AttributeValue>,
    attr: &str,
    idx: usize,
    value: Value,
) -> Result<(), AwsServiceError> {
    let Some(existing) = item.get_mut(attr) else {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "The document path provided in the update expression is invalid for update",
        ));
    };
    let Some(list) = existing.get_mut("L").and_then(|l| l.as_array_mut()) else {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "The document path provided in the update expression is invalid for update",
        ));
    };
    if idx < list.len() {
        list[idx] = value;
    } else if idx == list.len() {
        list.push(value);
    } else {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "The document path provided in the update expression is invalid for update",
        ));
    }
    Ok(())
}

fn resolve_value(
    reference: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Option<Value> {
    let reference = reference.trim();
    if reference.starts_with(':') {
        expr_attr_values.get(reference).cloned()
    } else {
        let attr_name = resolve_attr_name(reference, expr_attr_names);
        item.get(&attr_name).cloned()
    }
}

fn extract_number(val: &Option<Value>) -> Option<f64> {
    val.as_ref()
        .and_then(|v| v.get("N"))
        .and_then(|n| n.as_str())
        .and_then(|s| s.parse().ok())
}

fn parse_arithmetic(expr: &str) -> Option<(&str, &str, bool)> {
    let mut depth = 0;
    for (i, c) in expr.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            '+' if depth == 0 && i > 0 => {
                return Some((&expr[..i], &expr[i + 1..], true));
            }
            '-' if depth == 0 && i > 0 => {
                return Some((&expr[..i], &expr[i + 1..], false));
            }
            _ => {}
        }
    }
    None
}

fn apply_add_assignment(
    item: &mut HashMap<String, AttributeValue>,
    assignment: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    let parts: Vec<&str> = assignment.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Ok(());
    }

    let attr = resolve_attr_name(parts[0].trim(), expr_attr_names);
    let val_ref = parts[1].trim();
    let add_val = expr_attr_values.get(val_ref);

    if let Some(add_val) = add_val {
        if let Some(existing) = item.get(&attr) {
            if let (Some(existing_num), Some(add_num)) = (
                extract_number(&Some(existing.clone())),
                extract_number(&Some(add_val.clone())),
            ) {
                let result = existing_num + add_num;
                let num_str = if result == result.trunc() {
                    format!("{}", result as i64)
                } else {
                    format!("{result}")
                };
                item.insert(attr, json!({"N": num_str}));
            } else if let Some(existing_set) = existing.get("SS").and_then(|v| v.as_array()) {
                if let Some(add_set) = add_val.get("SS").and_then(|v| v.as_array()) {
                    let mut merged: Vec<Value> = existing_set.clone();
                    for v in add_set {
                        if !merged.contains(v) {
                            merged.push(v.clone());
                        }
                    }
                    item.insert(attr, json!({"SS": merged}));
                }
            } else if let Some(existing_set) = existing.get("NS").and_then(|v| v.as_array()) {
                if let Some(add_set) = add_val.get("NS").and_then(|v| v.as_array()) {
                    let mut merged: Vec<Value> = existing_set.clone();
                    for v in add_set {
                        if !merged.contains(v) {
                            merged.push(v.clone());
                        }
                    }
                    item.insert(attr, json!({"NS": merged}));
                }
            } else if let Some(existing_set) = existing.get("BS").and_then(|v| v.as_array()) {
                if let Some(add_set) = add_val.get("BS").and_then(|v| v.as_array()) {
                    let mut merged: Vec<Value> = existing_set.clone();
                    for v in add_set {
                        if !merged.contains(v) {
                            merged.push(v.clone());
                        }
                    }
                    item.insert(attr, json!({"BS": merged}));
                }
            }
        } else {
            item.insert(attr, add_val.clone());
        }
    }

    Ok(())
}

fn apply_delete_assignment(
    item: &mut HashMap<String, AttributeValue>,
    assignment: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    let parts: Vec<&str> = assignment.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Ok(());
    }

    let attr = resolve_attr_name(parts[0].trim(), expr_attr_names);
    let val_ref = parts[1].trim();
    let del_val = expr_attr_values.get(val_ref);

    if let (Some(existing), Some(del_val)) = (item.get(&attr).cloned(), del_val) {
        if let (Some(existing_set), Some(del_set)) = (
            existing.get("SS").and_then(|v| v.as_array()),
            del_val.get("SS").and_then(|v| v.as_array()),
        ) {
            let filtered: Vec<Value> = existing_set
                .iter()
                .filter(|v| !del_set.contains(v))
                .cloned()
                .collect();
            if filtered.is_empty() {
                item.remove(&attr);
            } else {
                item.insert(attr, json!({"SS": filtered}));
            }
        } else if let (Some(existing_set), Some(del_set)) = (
            existing.get("NS").and_then(|v| v.as_array()),
            del_val.get("NS").and_then(|v| v.as_array()),
        ) {
            let filtered: Vec<Value> = existing_set
                .iter()
                .filter(|v| !del_set.contains(v))
                .cloned()
                .collect();
            if filtered.is_empty() {
                item.remove(&attr);
            } else {
                item.insert(attr, json!({"NS": filtered}));
            }
        } else if let (Some(existing_set), Some(del_set)) = (
            existing.get("BS").and_then(|v| v.as_array()),
            del_val.get("BS").and_then(|v| v.as_array()),
        ) {
            let filtered: Vec<Value> = existing_set
                .iter()
                .filter(|v| !del_set.contains(v))
                .cloned()
                .collect();
            if filtered.is_empty() {
                item.remove(&attr);
            } else {
                item.insert(attr, json!({"BS": filtered}));
            }
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn build_table_description_json(
    arn: &str,
    key_schema: &[KeySchemaElement],
    attribute_definitions: &[AttributeDefinition],
    provisioned_throughput: &ProvisionedThroughput,
    gsi: &[GlobalSecondaryIndex],
    lsi: &[LocalSecondaryIndex],
    billing_mode: &str,
    created_at: chrono::DateTime<chrono::Utc>,
    item_count: i64,
    size_bytes: i64,
    status: &str,
) -> Value {
    let table_name = arn.rsplit('/').next().unwrap_or("");
    let creation_timestamp =
        created_at.timestamp() as f64 + created_at.timestamp_subsec_millis() as f64 / 1000.0;

    let ks: Vec<Value> = key_schema
        .iter()
        .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
        .collect();

    let ad: Vec<Value> = attribute_definitions
        .iter()
        .map(|a| json!({"AttributeName": a.attribute_name, "AttributeType": a.attribute_type}))
        .collect();

    let mut desc = json!({
        "TableName": table_name,
        "TableArn": arn,
        "TableId": uuid::Uuid::new_v4().to_string().replace('-', ""),
        "TableStatus": status,
        "KeySchema": ks,
        "AttributeDefinitions": ad,
        "CreationDateTime": creation_timestamp,
        "ItemCount": item_count,
        "TableSizeBytes": size_bytes,
        "BillingModeSummary": { "BillingMode": billing_mode },
    });

    if billing_mode != "PAY_PER_REQUEST" {
        desc["ProvisionedThroughput"] = json!({
            "ReadCapacityUnits": provisioned_throughput.read_capacity_units,
            "WriteCapacityUnits": provisioned_throughput.write_capacity_units,
            "NumberOfDecreasesToday": 0,
        });
    } else {
        desc["ProvisionedThroughput"] = json!({
            "ReadCapacityUnits": 0,
            "WriteCapacityUnits": 0,
            "NumberOfDecreasesToday": 0,
        });
    }

    if !gsi.is_empty() {
        let gsi_json: Vec<Value> = gsi
            .iter()
            .map(|g| {
                let gks: Vec<Value> = g
                    .key_schema
                    .iter()
                    .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
                    .collect();
                let mut idx = json!({
                    "IndexName": g.index_name,
                    "KeySchema": gks,
                    "Projection": { "ProjectionType": g.projection.projection_type },
                    "IndexStatus": "ACTIVE",
                    "IndexArn": format!("{arn}/index/{}", g.index_name),
                    "ItemCount": 0,
                    "IndexSizeBytes": 0,
                });
                if !g.projection.non_key_attributes.is_empty() {
                    idx["Projection"]["NonKeyAttributes"] = json!(g.projection.non_key_attributes);
                }
                if let Some(ref pt) = g.provisioned_throughput {
                    idx["ProvisionedThroughput"] = json!({
                        "ReadCapacityUnits": pt.read_capacity_units,
                        "WriteCapacityUnits": pt.write_capacity_units,
                        "NumberOfDecreasesToday": 0,
                    });
                }
                idx
            })
            .collect();
        desc["GlobalSecondaryIndexes"] = json!(gsi_json);
    }

    if !lsi.is_empty() {
        let lsi_json: Vec<Value> = lsi
            .iter()
            .map(|l| {
                let lks: Vec<Value> = l
                    .key_schema
                    .iter()
                    .map(|k| json!({"AttributeName": k.attribute_name, "KeyType": k.key_type}))
                    .collect();
                let mut idx = json!({
                    "IndexName": l.index_name,
                    "KeySchema": lks,
                    "Projection": { "ProjectionType": l.projection.projection_type },
                    "IndexArn": format!("{arn}/index/{}", l.index_name),
                    "ItemCount": 0,
                    "IndexSizeBytes": 0,
                });
                if !l.projection.non_key_attributes.is_empty() {
                    idx["Projection"]["NonKeyAttributes"] = json!(l.projection.non_key_attributes);
                }
                idx
            })
            .collect();
        desc["LocalSecondaryIndexes"] = json!(lsi_json);
    }

    desc
}

fn build_table_description(table: &DynamoTable) -> Value {
    let mut desc = build_table_description_json(
        &table.arn,
        &table.key_schema,
        &table.attribute_definitions,
        &table.provisioned_throughput,
        &table.gsi,
        &table.lsi,
        &table.billing_mode,
        table.created_at,
        table.item_count,
        table.size_bytes,
        &table.status,
    );

    // Add stream specification if streams are enabled
    if table.stream_enabled {
        if let Some(ref stream_arn) = table.stream_arn {
            desc["LatestStreamArn"] = json!(stream_arn);
            desc["LatestStreamLabel"] = json!(stream_arn.rsplit('/').next().unwrap_or(""));
        }
        if let Some(ref view_type) = table.stream_view_type {
            desc["StreamSpecification"] = json!({
                "StreamEnabled": true,
                "StreamViewType": view_type,
            });
        }
    }

    // Add SSE description
    if let Some(ref sse_type) = table.sse_type {
        let mut sse_desc = json!({
            "Status": "ENABLED",
            "SSEType": sse_type,
        });
        if let Some(ref key_arn) = table.sse_kms_key_arn {
            sse_desc["KMSMasterKeyArn"] = json!(key_arn);
        }
        desc["SSEDescription"] = sse_desc;
    } else {
        // Default: AWS owned key encryption (always enabled in real AWS)
        desc["SSEDescription"] = json!({
            "Status": "ENABLED",
            "SSEType": "AES256",
        });
    }

    desc
}

fn execute_partiql_statement(
    state: &SharedDynamoDbState,
    statement: &str,
    parameters: &[Value],
) -> Result<AwsResponse, AwsServiceError> {
    let trimmed = statement.trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("SELECT") {
        execute_partiql_select(state, trimmed, parameters)
    } else if upper.starts_with("INSERT") {
        execute_partiql_insert(state, trimmed, parameters)
    } else if upper.starts_with("UPDATE") {
        execute_partiql_update(state, trimmed, parameters)
    } else if upper.starts_with("DELETE") {
        execute_partiql_delete(state, trimmed, parameters)
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("Unsupported PartiQL statement: {trimmed}"),
        ))
    }
}

/// Parse a simple `SELECT * FROM tablename WHERE pk = 'value'` or with parameters.
fn execute_partiql_select(
    state: &SharedDynamoDbState,
    statement: &str,
    parameters: &[Value],
) -> Result<AwsResponse, AwsServiceError> {
    // Pattern: SELECT * FROM "tablename" [WHERE col = 'val' | WHERE col = ?]
    let upper = statement.to_ascii_uppercase();
    let from_pos = upper.find("FROM").ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid SELECT statement: missing FROM",
        )
    })?;

    let after_from = statement[from_pos + 4..].trim();
    let (table_name, rest) = parse_partiql_table_name(after_from);

    let state = state.read();
    let table = get_table(&state.tables, &table_name)?;

    let rest_upper = rest.trim().to_ascii_uppercase();
    if rest_upper.starts_with("WHERE") {
        let where_clause = rest.trim()[5..].trim();
        let matched = evaluate_partiql_where(table, where_clause, parameters)?;
        let items: Vec<Value> = matched.iter().map(|item| json!(item)).collect();
        DynamoDbService::ok_json(json!({ "Items": items }))
    } else {
        // No WHERE, return all items
        let items: Vec<Value> = table.items.iter().map(|item| json!(item)).collect();
        DynamoDbService::ok_json(json!({ "Items": items }))
    }
}

fn execute_partiql_insert(
    state: &SharedDynamoDbState,
    statement: &str,
    parameters: &[Value],
) -> Result<AwsResponse, AwsServiceError> {
    // Pattern: INSERT INTO "tablename" VALUE {'pk': 'val', 'attr': 'val'}
    // or with parameters: INSERT INTO "tablename" VALUE {'pk': ?, 'attr': ?}
    let upper = statement.to_ascii_uppercase();
    let into_pos = upper.find("INTO").ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid INSERT statement: missing INTO",
        )
    })?;

    let after_into = statement[into_pos + 4..].trim();
    let (table_name, rest) = parse_partiql_table_name(after_into);

    let rest_upper = rest.trim().to_ascii_uppercase();
    let value_pos = rest_upper.find("VALUE").ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid INSERT statement: missing VALUE",
        )
    })?;

    let value_str = rest.trim()[value_pos + 5..].trim();
    let item = parse_partiql_value_object(value_str, parameters)?;

    let mut state = state.write();
    let table = get_table_mut(&mut state.tables, &table_name)?;
    let key = extract_key(table, &item);
    if table.find_item_index(&key).is_some() {
        // DynamoDB PartiQL INSERT fails if item exists
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "DuplicateItemException",
            "Duplicate primary key exists in table",
        ));
    } else {
        table.items.push(item);
    }
    table.recalculate_stats();

    DynamoDbService::ok_json(json!({}))
}

fn execute_partiql_update(
    state: &SharedDynamoDbState,
    statement: &str,
    parameters: &[Value],
) -> Result<AwsResponse, AwsServiceError> {
    // Pattern: UPDATE "tablename" SET attr='val' WHERE pk='val'
    // or: UPDATE "tablename" SET attr=? WHERE pk=?
    let after_update = statement[6..].trim(); // skip "UPDATE"
    let (table_name, rest) = parse_partiql_table_name(after_update);

    let rest_upper = rest.trim().to_ascii_uppercase();
    let set_pos = rest_upper.find("SET").ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid UPDATE statement: missing SET",
        )
    })?;

    let after_set = rest.trim()[set_pos + 3..].trim();

    // Split on WHERE
    let where_pos = after_set.to_ascii_uppercase().find("WHERE");
    let (set_clause, where_clause) = if let Some(wp) = where_pos {
        (&after_set[..wp], after_set[wp + 5..].trim())
    } else {
        (after_set, "")
    };

    let mut state = state.write();
    let table = get_table_mut(&mut state.tables, &table_name)?;

    let matched_indices = if !where_clause.is_empty() {
        find_partiql_where_indices(table, where_clause, parameters)?
    } else {
        (0..table.items.len()).collect()
    };

    // Parse SET assignments: attr=value, attr2=value2
    let param_offset = count_params_in_str(where_clause);
    let assignments: Vec<&str> = set_clause.split(',').collect();
    for idx in &matched_indices {
        let mut local_offset = param_offset;
        for assignment in &assignments {
            let assignment = assignment.trim();
            if let Some((attr, val_str)) = assignment.split_once('=') {
                let attr = attr.trim().trim_matches('"');
                let val_str = val_str.trim();
                let value = parse_partiql_literal(val_str, parameters, &mut local_offset);
                if let Some(v) = value {
                    table.items[*idx].insert(attr.to_string(), v);
                }
            }
        }
    }
    table.recalculate_stats();

    DynamoDbService::ok_json(json!({}))
}

fn execute_partiql_delete(
    state: &SharedDynamoDbState,
    statement: &str,
    parameters: &[Value],
) -> Result<AwsResponse, AwsServiceError> {
    // Pattern: DELETE FROM "tablename" WHERE pk='val'
    let upper = statement.to_ascii_uppercase();
    let from_pos = upper.find("FROM").ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Invalid DELETE statement: missing FROM",
        )
    })?;

    let after_from = statement[from_pos + 4..].trim();
    let (table_name, rest) = parse_partiql_table_name(after_from);

    let rest_upper = rest.trim().to_ascii_uppercase();
    if !rest_upper.starts_with("WHERE") {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "DELETE requires a WHERE clause",
        ));
    }
    let where_clause = rest.trim()[5..].trim();

    let mut state = state.write();
    let table = get_table_mut(&mut state.tables, &table_name)?;

    let mut indices = find_partiql_where_indices(table, where_clause, parameters)?;
    // Remove from highest index first to avoid invalidating lower indices
    indices.sort_unstable();
    indices.reverse();
    for idx in indices {
        table.items.remove(idx);
    }
    table.recalculate_stats();

    DynamoDbService::ok_json(json!({}))
}

/// Parse a table name that may be quoted with double quotes.
/// Returns (table_name, rest_of_string).
fn parse_partiql_table_name(s: &str) -> (String, &str) {
    let s = s.trim();
    if let Some(stripped) = s.strip_prefix('"') {
        // Quoted name
        if let Some(end) = stripped.find('"') {
            let name = &stripped[..end];
            let rest = &stripped[end + 1..];
            (name.to_string(), rest)
        } else {
            let end = s.find(' ').unwrap_or(s.len());
            (s[..end].trim_matches('"').to_string(), &s[end..])
        }
    } else {
        let end = s.find(|c: char| c.is_whitespace()).unwrap_or(s.len());
        (s[..end].to_string(), &s[end..])
    }
}

/// Evaluate a simple WHERE clause: `col = 'value'` or `col = ?`
/// Returns matching items.
fn evaluate_partiql_where<'a>(
    table: &'a DynamoTable,
    where_clause: &str,
    parameters: &[Value],
) -> Result<Vec<&'a HashMap<String, AttributeValue>>, AwsServiceError> {
    let indices = find_partiql_where_indices(table, where_clause, parameters)?;
    Ok(indices.iter().map(|i| &table.items[*i]).collect())
}

fn find_partiql_where_indices(
    table: &DynamoTable,
    where_clause: &str,
    parameters: &[Value],
) -> Result<Vec<usize>, AwsServiceError> {
    // Support: col = 'val' AND col2 = 'val2'  or  col = ? AND col2 = ?
    // Case-insensitive AND splitting
    let upper = where_clause.to_uppercase();
    let conditions = if upper.contains(" AND ") {
        // Find positions of " AND " case-insensitively and split
        let mut parts = Vec::new();
        let mut last = 0;
        for (i, _) in upper.match_indices(" AND ") {
            parts.push(where_clause[last..i].trim());
            last = i + 5;
        }
        parts.push(where_clause[last..].trim());
        parts
    } else {
        vec![where_clause.trim()]
    };

    let mut param_idx = 0usize;
    let mut parsed_conditions: Vec<(String, Value)> = Vec::new();

    for cond in &conditions {
        let cond = cond.trim();
        if let Some((left, right)) = cond.split_once('=') {
            let attr = left.trim().trim_matches('"').to_string();
            let val_str = right.trim();
            let value = parse_partiql_literal(val_str, parameters, &mut param_idx);
            if let Some(v) = value {
                parsed_conditions.push((attr, v));
            }
        }
    }

    let mut indices = Vec::new();
    for (i, item) in table.items.iter().enumerate() {
        let all_match = parsed_conditions
            .iter()
            .all(|(attr, expected)| item.get(attr) == Some(expected));
        if all_match {
            indices.push(i);
        }
    }

    Ok(indices)
}

/// Parse a PartiQL literal value. Supports:
/// - 'string' -> {"S": "string"}
/// - 123 -> {"N": "123"}
/// - ? -> parameter from list
fn parse_partiql_literal(s: &str, parameters: &[Value], param_idx: &mut usize) -> Option<Value> {
    let s = s.trim();
    if s == "?" {
        let idx = *param_idx;
        *param_idx += 1;
        parameters.get(idx).cloned()
    } else if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        Some(json!({"S": inner}))
    } else if let Ok(n) = s.parse::<f64>() {
        let num_str = if n == n.trunc() {
            format!("{}", n as i64)
        } else {
            format!("{n}")
        };
        Some(json!({"N": num_str}))
    } else {
        None
    }
}

/// Parse a PartiQL VALUE object like `{'pk': 'val1', 'attr': 'val2'}` or with ? params.
fn parse_partiql_value_object(
    s: &str,
    parameters: &[Value],
) -> Result<HashMap<String, AttributeValue>, AwsServiceError> {
    let s = s.trim();
    let inner = s
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid VALUE: expected object literal",
            )
        })?;

    let mut item = HashMap::new();
    let mut param_idx = 0usize;

    // Simple comma-separated key:value parsing
    for pair in split_partiql_pairs(inner) {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((key_part, val_part)) = pair.split_once(':') {
            let key = key_part
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            if let Some(val) = parse_partiql_literal(val_part.trim(), parameters, &mut param_idx) {
                item.insert(key, val);
            }
        }
    }

    Ok(item)
}

/// Split PartiQL object pairs on commas, respecting nested braces and quotes.
fn split_partiql_pairs(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    let mut in_quote = false;

    for (i, c) in s.char_indices() {
        match c {
            '\'' if !in_quote => in_quote = true,
            '\'' if in_quote => in_quote = false,
            '{' if !in_quote => depth += 1,
            '}' if !in_quote => depth -= 1,
            ',' if !in_quote && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Count ? parameters in a string.
fn count_params_in_str(s: &str) -> usize {
    s.chars().filter(|c| *c == '?').count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_update_clauses_set() {
        let clauses = parse_update_clauses("SET #a = :val1, #b = :val2");
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0].0, "SET");
        assert_eq!(clauses[0].1.len(), 2);
    }

    #[test]
    fn test_parse_update_clauses_set_and_remove() {
        let clauses = parse_update_clauses("SET #a = :val1 REMOVE #b");
        assert_eq!(clauses.len(), 2);
        assert_eq!(clauses[0].0, "SET");
        assert_eq!(clauses[1].0, "REMOVE");
    }

    #[test]
    fn test_evaluate_key_condition_simple() {
        let mut item = HashMap::new();
        item.insert("pk".to_string(), json!({"S": "user1"}));
        item.insert("sk".to_string(), json!({"S": "order1"}));

        let mut expr_values = HashMap::new();
        expr_values.insert(":pk".to_string(), json!({"S": "user1"}));

        assert!(evaluate_key_condition(
            "pk = :pk",
            &item,
            "pk",
            Some("sk"),
            &HashMap::new(),
            &expr_values,
        ));
    }

    #[test]
    fn test_compare_attribute_values_numbers() {
        let a = json!({"N": "10"});
        let b = json!({"N": "20"});
        assert_eq!(
            compare_attribute_values(Some(&a), Some(&b)),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn test_compare_attribute_values_strings() {
        let a = json!({"S": "apple"});
        let b = json!({"S": "banana"});
        assert_eq!(
            compare_attribute_values(Some(&a), Some(&b)),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn test_split_on_and() {
        let parts = split_on_and("pk = :pk AND sk > :sk");
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].trim(), "pk = :pk");
        assert_eq!(parts[1].trim(), "sk > :sk");
    }

    #[test]
    fn test_split_on_and_respects_parentheses() {
        // Before fix: split_on_and would split inside the parens
        let parts = split_on_and("(a = :a AND b = :b) OR c = :c");
        // Should NOT split on the AND inside parentheses
        assert_eq!(parts.len(), 1);
        assert_eq!(parts[0].trim(), "(a = :a AND b = :b) OR c = :c");
    }

    #[test]
    fn test_evaluate_filter_expression_parenthesized_and_with_or() {
        // (a AND b) OR c — should match when c is true but a is false
        let mut item = HashMap::new();
        item.insert("x".to_string(), json!({"S": "no"}));
        item.insert("y".to_string(), json!({"S": "no"}));
        item.insert("z".to_string(), json!({"S": "yes"}));

        let mut expr_values = HashMap::new();
        expr_values.insert(":yes".to_string(), json!({"S": "yes"}));

        // x=yes AND y=yes => false, but z=yes => true => overall true
        let result = evaluate_filter_expression(
            "(x = :yes AND y = :yes) OR z = :yes",
            &item,
            &HashMap::new(),
            &expr_values,
        );
        assert!(result, "should match because z = :yes is true");

        // x=yes AND y=yes => false, z=yes => false => overall false
        let mut item2 = HashMap::new();
        item2.insert("x".to_string(), json!({"S": "no"}));
        item2.insert("y".to_string(), json!({"S": "no"}));
        item2.insert("z".to_string(), json!({"S": "no"}));

        let result2 = evaluate_filter_expression(
            "(x = :yes AND y = :yes) OR z = :yes",
            &item2,
            &HashMap::new(),
            &expr_values,
        );
        assert!(!result2, "should not match because nothing is true");
    }

    #[test]
    fn test_project_item_nested_path() {
        // Item with a list attribute containing maps
        let mut item = HashMap::new();
        item.insert("pk".to_string(), json!({"S": "key1"}));
        item.insert(
            "data".to_string(),
            json!({"L": [{"M": {"name": {"S": "Alice"}, "age": {"N": "30"}}}, {"M": {"name": {"S": "Bob"}}}]}),
        );

        let body = json!({
            "ProjectionExpression": "data[0].name"
        });

        let projected = project_item(&item, &body);
        // Should contain data[0].name = "Alice", not the entire data[0] element
        let name = projected
            .get("data")
            .and_then(|v| v.get("L"))
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("M"))
            .and_then(|v| v.get("name"))
            .and_then(|v| v.get("S"))
            .and_then(|v| v.as_str());
        assert_eq!(name, Some("Alice"));

        // Should NOT contain the "age" field
        let age = projected
            .get("data")
            .and_then(|v| v.get("L"))
            .and_then(|v| v.get(0))
            .and_then(|v| v.get("M"))
            .and_then(|v| v.get("age"));
        assert!(age.is_none(), "age should not be present in projection");
    }

    #[test]
    fn test_resolve_nested_path_map() {
        let mut item = HashMap::new();
        item.insert(
            "info".to_string(),
            json!({"M": {"address": {"M": {"city": {"S": "NYC"}}}}}),
        );

        let result = resolve_nested_path(&item, "info.address.city");
        assert_eq!(result, Some(json!({"S": "NYC"})));
    }

    #[test]
    fn test_resolve_nested_path_list_then_map() {
        let mut item = HashMap::new();
        item.insert(
            "items".to_string(),
            json!({"L": [{"M": {"sku": {"S": "ABC"}}}]}),
        );

        let result = resolve_nested_path(&item, "items[0].sku");
        assert_eq!(result, Some(json!({"S": "ABC"})));
    }

    // -- Integration-style tests using DynamoDbService --

    use crate::state::SharedDynamoDbState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn make_service() -> DynamoDbService {
        let state: SharedDynamoDbState = Arc::new(RwLock::new(crate::state::DynamoDbState::new(
            "123456789012",
            "us-east-1",
        )));
        DynamoDbService::new(state)
    }

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "dynamodb".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-id".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    fn create_test_table(svc: &DynamoDbService) {
        let req = make_request(
            "CreateTable",
            json!({
                "TableName": "test-table",
                "KeySchema": [
                    { "AttributeName": "pk", "KeyType": "HASH" }
                ],
                "AttributeDefinitions": [
                    { "AttributeName": "pk", "AttributeType": "S" }
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        );
        svc.create_table(&req).unwrap();
    }

    #[test]
    fn delete_item_return_values_all_old() {
        let svc = make_service();
        create_test_table(&svc);

        // Put an item
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": "key1" },
                    "name": { "S": "Alice" },
                    "age": { "N": "30" }
                }
            }),
        );
        svc.put_item(&req).unwrap();

        // Delete with ReturnValues=ALL_OLD
        let req = make_request(
            "DeleteItem",
            json!({
                "TableName": "test-table",
                "Key": { "pk": { "S": "key1" } },
                "ReturnValues": "ALL_OLD"
            }),
        );
        let resp = svc.delete_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();

        // Verify the old item is returned
        let attrs = &body["Attributes"];
        assert_eq!(attrs["pk"]["S"].as_str().unwrap(), "key1");
        assert_eq!(attrs["name"]["S"].as_str().unwrap(), "Alice");
        assert_eq!(attrs["age"]["N"].as_str().unwrap(), "30");

        // Verify the item is actually deleted
        let req = make_request(
            "GetItem",
            json!({
                "TableName": "test-table",
                "Key": { "pk": { "S": "key1" } }
            }),
        );
        let resp = svc.get_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.get("Item").is_none(), "item should be deleted");
    }

    #[test]
    fn transact_get_items_returns_existing_and_missing() {
        let svc = make_service();
        create_test_table(&svc);

        // Put one item
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": "exists" },
                    "val": { "S": "hello" }
                }
            }),
        );
        svc.put_item(&req).unwrap();

        let req = make_request(
            "TransactGetItems",
            json!({
                "TransactItems": [
                    { "Get": { "TableName": "test-table", "Key": { "pk": { "S": "exists" } } } },
                    { "Get": { "TableName": "test-table", "Key": { "pk": { "S": "missing" } } } }
                ]
            }),
        );
        let resp = svc.transact_get_items(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let responses = body["Responses"].as_array().unwrap();
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0]["Item"]["pk"]["S"].as_str().unwrap(), "exists");
        assert!(responses[1].get("Item").is_none());
    }

    #[test]
    fn transact_write_items_put_and_delete() {
        let svc = make_service();
        create_test_table(&svc);

        // Put initial item
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": "to-delete" },
                    "val": { "S": "bye" }
                }
            }),
        );
        svc.put_item(&req).unwrap();

        // TransactWrite: put new + delete existing
        let req = make_request(
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {
                        "Put": {
                            "TableName": "test-table",
                            "Item": {
                                "pk": { "S": "new-item" },
                                "val": { "S": "hi" }
                            }
                        }
                    },
                    {
                        "Delete": {
                            "TableName": "test-table",
                            "Key": { "pk": { "S": "to-delete" } }
                        }
                    }
                ]
            }),
        );
        let resp = svc.transact_write_items(&req).unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        // Verify new item exists
        let req = make_request(
            "GetItem",
            json!({
                "TableName": "test-table",
                "Key": { "pk": { "S": "new-item" } }
            }),
        );
        let resp = svc.get_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Item"]["val"]["S"].as_str().unwrap(), "hi");

        // Verify deleted item is gone
        let req = make_request(
            "GetItem",
            json!({
                "TableName": "test-table",
                "Key": { "pk": { "S": "to-delete" } }
            }),
        );
        let resp = svc.get_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body.get("Item").is_none());
    }

    #[test]
    fn transact_write_items_condition_check_failure() {
        let svc = make_service();
        create_test_table(&svc);

        // TransactWrite with a ConditionCheck that fails (item doesn't exist)
        let req = make_request(
            "TransactWriteItems",
            json!({
                "TransactItems": [
                    {
                        "ConditionCheck": {
                            "TableName": "test-table",
                            "Key": { "pk": { "S": "nonexistent" } },
                            "ConditionExpression": "attribute_exists(pk)"
                        }
                    }
                ]
            }),
        );
        let resp = svc.transact_write_items(&req).unwrap();
        // Should be a 400 error response
        assert_eq!(resp.status, StatusCode::BAD_REQUEST);
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["__type"].as_str().unwrap(),
            "TransactionCanceledException"
        );
        assert!(body["CancellationReasons"].as_array().is_some());
    }

    #[test]
    fn update_and_describe_time_to_live() {
        let svc = make_service();
        create_test_table(&svc);

        // Enable TTL
        let req = make_request(
            "UpdateTimeToLive",
            json!({
                "TableName": "test-table",
                "TimeToLiveSpecification": {
                    "AttributeName": "ttl",
                    "Enabled": true
                }
            }),
        );
        let resp = svc.update_time_to_live(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["TimeToLiveSpecification"]["AttributeName"]
                .as_str()
                .unwrap(),
            "ttl"
        );
        assert!(body["TimeToLiveSpecification"]["Enabled"]
            .as_bool()
            .unwrap());

        // Describe TTL
        let req = make_request("DescribeTimeToLive", json!({ "TableName": "test-table" }));
        let resp = svc.describe_time_to_live(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["TimeToLiveDescription"]["TimeToLiveStatus"]
                .as_str()
                .unwrap(),
            "ENABLED"
        );
        assert_eq!(
            body["TimeToLiveDescription"]["AttributeName"]
                .as_str()
                .unwrap(),
            "ttl"
        );

        // Disable TTL
        let req = make_request(
            "UpdateTimeToLive",
            json!({
                "TableName": "test-table",
                "TimeToLiveSpecification": {
                    "AttributeName": "ttl",
                    "Enabled": false
                }
            }),
        );
        svc.update_time_to_live(&req).unwrap();

        let req = make_request("DescribeTimeToLive", json!({ "TableName": "test-table" }));
        let resp = svc.describe_time_to_live(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["TimeToLiveDescription"]["TimeToLiveStatus"]
                .as_str()
                .unwrap(),
            "DISABLED"
        );
    }

    #[test]
    fn resource_policy_lifecycle() {
        let svc = make_service();
        create_test_table(&svc);

        let table_arn = {
            let state = svc.state.read();
            state.tables.get("test-table").unwrap().arn.clone()
        };

        // Put policy
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let req = make_request(
            "PutResourcePolicy",
            json!({
                "ResourceArn": table_arn,
                "Policy": policy_doc
            }),
        );
        let resp = svc.put_resource_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["RevisionId"].as_str().is_some());

        // Get policy
        let req = make_request("GetResourcePolicy", json!({ "ResourceArn": table_arn }));
        let resp = svc.get_resource_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Policy"].as_str().unwrap(), policy_doc);

        // Delete policy
        let req = make_request("DeleteResourcePolicy", json!({ "ResourceArn": table_arn }));
        svc.delete_resource_policy(&req).unwrap();

        // Get should return null now
        let req = make_request("GetResourcePolicy", json!({ "ResourceArn": table_arn }));
        let resp = svc.get_resource_policy(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(body["Policy"].is_null());
    }

    #[test]
    fn describe_endpoints() {
        let svc = make_service();
        let req = make_request("DescribeEndpoints", json!({}));
        let resp = svc.describe_endpoints(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Endpoints"][0]["CachePeriodInMinutes"], 1440);
    }

    #[test]
    fn describe_limits() {
        let svc = make_service();
        let req = make_request("DescribeLimits", json!({}));
        let resp = svc.describe_limits(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["TableMaxReadCapacityUnits"], 40000);
    }

    #[test]
    fn backup_lifecycle() {
        let svc = make_service();
        create_test_table(&svc);

        // Create backup
        let req = make_request(
            "CreateBackup",
            json!({ "TableName": "test-table", "BackupName": "my-backup" }),
        );
        let resp = svc.create_backup(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let backup_arn = body["BackupDetails"]["BackupArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(body["BackupDetails"]["BackupStatus"], "AVAILABLE");

        // Describe backup
        let req = make_request("DescribeBackup", json!({ "BackupArn": backup_arn }));
        let resp = svc.describe_backup(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["BackupDescription"]["BackupDetails"]["BackupName"],
            "my-backup"
        );

        // List backups
        let req = make_request("ListBackups", json!({}));
        let resp = svc.list_backups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["BackupSummaries"].as_array().unwrap().len(), 1);

        // Restore from backup
        let req = make_request(
            "RestoreTableFromBackup",
            json!({ "BackupArn": backup_arn, "TargetTableName": "restored-table" }),
        );
        svc.restore_table_from_backup(&req).unwrap();

        // Verify restored table exists
        let req = make_request("DescribeTable", json!({ "TableName": "restored-table" }));
        let resp = svc.describe_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Table"]["TableStatus"], "ACTIVE");

        // Delete backup
        let req = make_request("DeleteBackup", json!({ "BackupArn": backup_arn }));
        svc.delete_backup(&req).unwrap();

        // List should be empty
        let req = make_request("ListBackups", json!({}));
        let resp = svc.list_backups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["BackupSummaries"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn continuous_backups() {
        let svc = make_service();
        create_test_table(&svc);

        // Initially disabled
        let req = make_request(
            "DescribeContinuousBackups",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_continuous_backups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
                ["PointInTimeRecoveryStatus"],
            "DISABLED"
        );

        // Enable
        let req = make_request(
            "UpdateContinuousBackups",
            json!({
                "TableName": "test-table",
                "PointInTimeRecoverySpecification": {
                    "PointInTimeRecoveryEnabled": true
                }
            }),
        );
        svc.update_continuous_backups(&req).unwrap();

        // Verify
        let req = make_request(
            "DescribeContinuousBackups",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_continuous_backups(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
                ["PointInTimeRecoveryStatus"],
            "ENABLED"
        );
    }

    #[test]
    fn restore_table_to_point_in_time() {
        let svc = make_service();
        create_test_table(&svc);

        let req = make_request(
            "RestoreTableToPointInTime",
            json!({
                "SourceTableName": "test-table",
                "TargetTableName": "pitr-restored"
            }),
        );
        svc.restore_table_to_point_in_time(&req).unwrap();

        let req = make_request("DescribeTable", json!({ "TableName": "pitr-restored" }));
        let resp = svc.describe_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Table"]["TableStatus"], "ACTIVE");
    }

    #[test]
    fn global_table_lifecycle() {
        let svc = make_service();

        // Create global table
        let req = make_request(
            "CreateGlobalTable",
            json!({
                "GlobalTableName": "my-global",
                "ReplicationGroup": [
                    { "RegionName": "us-east-1" },
                    { "RegionName": "eu-west-1" }
                ]
            }),
        );
        let resp = svc.create_global_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["GlobalTableDescription"]["GlobalTableStatus"],
            "ACTIVE"
        );

        // Describe
        let req = make_request(
            "DescribeGlobalTable",
            json!({ "GlobalTableName": "my-global" }),
        );
        let resp = svc.describe_global_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["GlobalTableDescription"]["ReplicationGroup"]
                .as_array()
                .unwrap()
                .len(),
            2
        );

        // List
        let req = make_request("ListGlobalTables", json!({}));
        let resp = svc.list_global_tables(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["GlobalTables"].as_array().unwrap().len(), 1);

        // Update - add a region
        let req = make_request(
            "UpdateGlobalTable",
            json!({
                "GlobalTableName": "my-global",
                "ReplicaUpdates": [
                    { "Create": { "RegionName": "ap-southeast-1" } }
                ]
            }),
        );
        let resp = svc.update_global_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["GlobalTableDescription"]["ReplicationGroup"]
                .as_array()
                .unwrap()
                .len(),
            3
        );

        // Describe settings
        let req = make_request(
            "DescribeGlobalTableSettings",
            json!({ "GlobalTableName": "my-global" }),
        );
        let resp = svc.describe_global_table_settings(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ReplicaSettings"].as_array().unwrap().len(), 3);

        // Update settings (no-op, just verify no error)
        let req = make_request(
            "UpdateGlobalTableSettings",
            json!({ "GlobalTableName": "my-global" }),
        );
        svc.update_global_table_settings(&req).unwrap();
    }

    #[test]
    fn table_replica_auto_scaling() {
        let svc = make_service();
        create_test_table(&svc);

        let req = make_request(
            "DescribeTableReplicaAutoScaling",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_table_replica_auto_scaling(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["TableAutoScalingDescription"]["TableName"],
            "test-table"
        );

        let req = make_request(
            "UpdateTableReplicaAutoScaling",
            json!({ "TableName": "test-table" }),
        );
        svc.update_table_replica_auto_scaling(&req).unwrap();
    }

    #[test]
    fn kinesis_streaming_lifecycle() {
        let svc = make_service();
        create_test_table(&svc);

        // Enable
        let req = make_request(
            "EnableKinesisStreamingDestination",
            json!({
                "TableName": "test-table",
                "StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
            }),
        );
        let resp = svc.enable_kinesis_streaming_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DestinationStatus"], "ACTIVE");

        // Describe
        let req = make_request(
            "DescribeKinesisStreamingDestination",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_kinesis_streaming_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["KinesisDataStreamDestinations"]
                .as_array()
                .unwrap()
                .len(),
            1
        );

        // Update
        let req = make_request(
            "UpdateKinesisStreamingDestination",
            json!({
                "TableName": "test-table",
                "StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
                "UpdateKinesisStreamingConfiguration": {
                    "ApproximateCreationDateTimePrecision": "MICROSECOND"
                }
            }),
        );
        svc.update_kinesis_streaming_destination(&req).unwrap();

        // Disable
        let req = make_request(
            "DisableKinesisStreamingDestination",
            json!({
                "TableName": "test-table",
                "StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
            }),
        );
        let resp = svc.disable_kinesis_streaming_destination(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["DestinationStatus"], "DISABLED");
    }

    #[test]
    fn contributor_insights_lifecycle() {
        let svc = make_service();
        create_test_table(&svc);

        // Initially disabled
        let req = make_request(
            "DescribeContributorInsights",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_contributor_insights(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ContributorInsightsStatus"], "DISABLED");

        // Enable
        let req = make_request(
            "UpdateContributorInsights",
            json!({
                "TableName": "test-table",
                "ContributorInsightsAction": "ENABLE"
            }),
        );
        let resp = svc.update_contributor_insights(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ContributorInsightsStatus"], "ENABLED");

        // List
        let req = make_request("ListContributorInsights", json!({}));
        let resp = svc.list_contributor_insights(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["ContributorInsightsSummaries"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn export_lifecycle() {
        let svc = make_service();
        create_test_table(&svc);

        let table_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/test-table".to_string();

        // Export
        let req = make_request(
            "ExportTableToPointInTime",
            json!({
                "TableArn": table_arn,
                "S3Bucket": "my-bucket"
            }),
        );
        let resp = svc.export_table_to_point_in_time(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let export_arn = body["ExportDescription"]["ExportArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(body["ExportDescription"]["ExportStatus"], "COMPLETED");

        // Describe
        let req = make_request("DescribeExport", json!({ "ExportArn": export_arn }));
        let resp = svc.describe_export(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ExportDescription"]["S3Bucket"], "my-bucket");

        // List
        let req = make_request("ListExports", json!({}));
        let resp = svc.list_exports(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ExportSummaries"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn import_lifecycle() {
        let svc = make_service();

        let req = make_request(
            "ImportTable",
            json!({
                "InputFormat": "DYNAMODB_JSON",
                "S3BucketSource": { "S3Bucket": "import-bucket" },
                "TableCreationParameters": {
                    "TableName": "imported-table",
                    "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                    "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }]
                }
            }),
        );
        let resp = svc.import_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let import_arn = body["ImportTableDescription"]["ImportArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(body["ImportTableDescription"]["ImportStatus"], "COMPLETED");

        // Describe import
        let req = make_request("DescribeImport", json!({ "ImportArn": import_arn }));
        let resp = svc.describe_import(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ImportTableDescription"]["ImportStatus"], "COMPLETED");

        // List imports
        let req = make_request("ListImports", json!({}));
        let resp = svc.list_imports(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ImportSummaryList"].as_array().unwrap().len(), 1);

        // Verify the table was created
        let req = make_request("DescribeTable", json!({ "TableName": "imported-table" }));
        let resp = svc.describe_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Table"]["TableStatus"], "ACTIVE");
    }

    #[test]
    fn backup_restore_preserves_items() {
        let svc = make_service();
        create_test_table(&svc);

        // Put 3 items
        for i in 1..=3 {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "test-table",
                    "Item": {
                        "pk": { "S": format!("key{i}") },
                        "data": { "S": format!("value{i}") }
                    }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        // Create backup
        let req = make_request(
            "CreateBackup",
            json!({
                "TableName": "test-table",
                "BackupName": "my-backup"
            }),
        );
        let resp = svc.create_backup(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let backup_arn = body["BackupDetails"]["BackupArn"]
            .as_str()
            .unwrap()
            .to_string();

        // Delete all items from the original table
        for i in 1..=3 {
            let req = make_request(
                "DeleteItem",
                json!({
                    "TableName": "test-table",
                    "Key": { "pk": { "S": format!("key{i}") } }
                }),
            );
            svc.delete_item(&req).unwrap();
        }

        // Verify original table is empty
        let req = make_request("Scan", json!({ "TableName": "test-table" }));
        let resp = svc.scan(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Count"], 0);

        // Restore from backup
        let req = make_request(
            "RestoreTableFromBackup",
            json!({
                "BackupArn": backup_arn,
                "TargetTableName": "restored-table"
            }),
        );
        svc.restore_table_from_backup(&req).unwrap();

        // Scan restored table — should have 3 items
        let req = make_request("Scan", json!({ "TableName": "restored-table" }));
        let resp = svc.scan(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Count"], 3);
        assert_eq!(body["Items"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn global_table_replicates_writes() {
        let svc = make_service();
        create_test_table(&svc);

        // Create global table with replicas
        let req = make_request(
            "CreateGlobalTable",
            json!({
                "GlobalTableName": "test-table",
                "ReplicationGroup": [
                    { "RegionName": "us-east-1" },
                    { "RegionName": "eu-west-1" }
                ]
            }),
        );
        let resp = svc.create_global_table(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["GlobalTableDescription"]["GlobalTableStatus"],
            "ACTIVE"
        );

        // Put an item
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": "replicated-key" },
                    "data": { "S": "replicated-value" }
                }
            }),
        );
        svc.put_item(&req).unwrap();

        // Verify the item is readable (since all replicas share the same table)
        let req = make_request(
            "GetItem",
            json!({
                "TableName": "test-table",
                "Key": { "pk": { "S": "replicated-key" } }
            }),
        );
        let resp = svc.get_item(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Item"]["pk"]["S"], "replicated-key");
        assert_eq!(body["Item"]["data"]["S"], "replicated-value");
    }

    #[test]
    fn contributor_insights_tracks_access() {
        let svc = make_service();
        create_test_table(&svc);

        // Enable contributor insights
        let req = make_request(
            "UpdateContributorInsights",
            json!({
                "TableName": "test-table",
                "ContributorInsightsAction": "ENABLE"
            }),
        );
        svc.update_contributor_insights(&req).unwrap();

        // Put items with different partition keys
        for key in &["alpha", "beta", "alpha", "alpha", "beta"] {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "test-table",
                    "Item": {
                        "pk": { "S": key },
                        "data": { "S": "value" }
                    }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        // Get items (to also track read access)
        for _ in 0..3 {
            let req = make_request(
                "GetItem",
                json!({
                    "TableName": "test-table",
                    "Key": { "pk": { "S": "alpha" } }
                }),
            );
            svc.get_item(&req).unwrap();
        }

        // Describe contributor insights — should show top contributors
        let req = make_request(
            "DescribeContributorInsights",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_contributor_insights(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ContributorInsightsStatus"], "ENABLED");

        let contributors = body["TopContributors"].as_array().unwrap();
        assert!(
            !contributors.is_empty(),
            "TopContributors should not be empty"
        );

        // alpha was accessed 3 (put) + 3 (get) = 6 times, beta 2 times
        // alpha should be the top contributor
        let top = &contributors[0];
        assert!(top["Count"].as_u64().unwrap() > 0);

        // Verify the rule list is populated
        let rules = body["ContributorInsightsRuleList"].as_array().unwrap();
        assert!(!rules.is_empty());
    }

    #[test]
    fn contributor_insights_not_tracked_when_disabled() {
        let svc = make_service();
        create_test_table(&svc);

        // Put items without enabling insights
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": "key1" },
                    "data": { "S": "value" }
                }
            }),
        );
        svc.put_item(&req).unwrap();

        // Describe — should show empty contributors
        let req = make_request(
            "DescribeContributorInsights",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_contributor_insights(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ContributorInsightsStatus"], "DISABLED");

        let contributors = body["TopContributors"].as_array().unwrap();
        assert!(contributors.is_empty());
    }

    #[test]
    fn contributor_insights_disabled_table_no_counters_after_scan() {
        let svc = make_service();
        create_test_table(&svc);

        // Put items
        for key in &["alpha", "beta"] {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "test-table",
                    "Item": { "pk": { "S": key } }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        // Enable insights, then scan, then disable, then check counters are cleared
        let req = make_request(
            "UpdateContributorInsights",
            json!({
                "TableName": "test-table",
                "ContributorInsightsAction": "ENABLE"
            }),
        );
        svc.update_contributor_insights(&req).unwrap();

        // Scan to trigger counter collection
        let req = make_request("Scan", json!({ "TableName": "test-table" }));
        svc.scan(&req).unwrap();

        // Verify counters were collected
        let req = make_request(
            "DescribeContributorInsights",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_contributor_insights(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let contributors = body["TopContributors"].as_array().unwrap();
        assert!(
            !contributors.is_empty(),
            "counters should be non-empty while enabled"
        );

        // Disable insights (this clears counters)
        let req = make_request(
            "UpdateContributorInsights",
            json!({
                "TableName": "test-table",
                "ContributorInsightsAction": "DISABLE"
            }),
        );
        svc.update_contributor_insights(&req).unwrap();

        // Scan again -- should NOT accumulate counters since insights is disabled
        let req = make_request("Scan", json!({ "TableName": "test-table" }));
        svc.scan(&req).unwrap();

        // Verify counters are still empty
        let req = make_request(
            "DescribeContributorInsights",
            json!({ "TableName": "test-table" }),
        );
        let resp = svc.describe_contributor_insights(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let contributors = body["TopContributors"].as_array().unwrap();
        assert!(
            contributors.is_empty(),
            "counters should be empty after disabling insights"
        );
    }

    #[test]
    fn scan_pagination_with_limit() {
        let svc = make_service();
        create_test_table(&svc);

        // Insert 5 items
        for i in 0..5 {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "test-table",
                    "Item": {
                        "pk": { "S": format!("item{i}") },
                        "data": { "S": format!("value{i}") }
                    }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        // Scan with limit=2
        let req = make_request("Scan", json!({ "TableName": "test-table", "Limit": 2 }));
        let resp = svc.scan(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Count"], 2);
        assert!(
            body["LastEvaluatedKey"].is_object(),
            "should have LastEvaluatedKey when limit < total items"
        );
        assert!(body["LastEvaluatedKey"]["pk"].is_object());

        // Page through all items
        let mut all_items: Vec<Value> = body["Items"].as_array().unwrap().clone();
        let mut lek = body["LastEvaluatedKey"].clone();

        while lek.is_object() {
            let req = make_request(
                "Scan",
                json!({
                    "TableName": "test-table",
                    "Limit": 2,
                    "ExclusiveStartKey": lek
                }),
            );
            let resp = svc.scan(&req).unwrap();
            let body: Value = serde_json::from_slice(&resp.body).unwrap();
            all_items.extend(body["Items"].as_array().unwrap().iter().cloned());
            lek = body["LastEvaluatedKey"].clone();
        }

        assert_eq!(
            all_items.len(),
            5,
            "should retrieve all 5 items via pagination"
        );
    }

    #[test]
    fn scan_no_pagination_when_all_fit() {
        let svc = make_service();
        create_test_table(&svc);

        for i in 0..3 {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "test-table",
                    "Item": {
                        "pk": { "S": format!("item{i}") }
                    }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        // Scan with limit > item count
        let req = make_request("Scan", json!({ "TableName": "test-table", "Limit": 10 }));
        let resp = svc.scan(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Count"], 3);
        assert!(
            body["LastEvaluatedKey"].is_null(),
            "should not have LastEvaluatedKey when all items fit"
        );

        // Scan without limit
        let req = make_request("Scan", json!({ "TableName": "test-table" }));
        let resp = svc.scan(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Count"], 3);
        assert!(body["LastEvaluatedKey"].is_null());
    }

    fn create_composite_table(svc: &DynamoDbService) {
        let req = make_request(
            "CreateTable",
            json!({
                "TableName": "composite-table",
                "KeySchema": [
                    { "AttributeName": "pk", "KeyType": "HASH" },
                    { "AttributeName": "sk", "KeyType": "RANGE" }
                ],
                "AttributeDefinitions": [
                    { "AttributeName": "pk", "AttributeType": "S" },
                    { "AttributeName": "sk", "AttributeType": "S" }
                ],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        );
        svc.create_table(&req).unwrap();
    }

    #[test]
    fn query_pagination_with_composite_key() {
        let svc = make_service();
        create_composite_table(&svc);

        // Insert 5 items under the same partition key
        for i in 0..5 {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "composite-table",
                    "Item": {
                        "pk": { "S": "user1" },
                        "sk": { "S": format!("item{i:03}") },
                        "data": { "S": format!("value{i}") }
                    }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        // Query with limit=2
        let req = make_request(
            "Query",
            json!({
                "TableName": "composite-table",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": { ":pk": { "S": "user1" } },
                "Limit": 2
            }),
        );
        let resp = svc.query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Count"], 2);
        assert!(body["LastEvaluatedKey"].is_object());
        assert!(body["LastEvaluatedKey"]["pk"].is_object());
        assert!(body["LastEvaluatedKey"]["sk"].is_object());

        // Page through all items
        let mut all_items: Vec<Value> = body["Items"].as_array().unwrap().clone();
        let mut lek = body["LastEvaluatedKey"].clone();

        while lek.is_object() {
            let req = make_request(
                "Query",
                json!({
                    "TableName": "composite-table",
                    "KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": { ":pk": { "S": "user1" } },
                    "Limit": 2,
                    "ExclusiveStartKey": lek
                }),
            );
            let resp = svc.query(&req).unwrap();
            let body: Value = serde_json::from_slice(&resp.body).unwrap();
            all_items.extend(body["Items"].as_array().unwrap().iter().cloned());
            lek = body["LastEvaluatedKey"].clone();
        }

        assert_eq!(
            all_items.len(),
            5,
            "should retrieve all 5 items via pagination"
        );

        // Verify items came back sorted by sort key
        let sks: Vec<String> = all_items
            .iter()
            .map(|item| item["sk"]["S"].as_str().unwrap().to_string())
            .collect();
        let mut sorted = sks.clone();
        sorted.sort();
        assert_eq!(sks, sorted, "items should be sorted by sort key");
    }

    #[test]
    fn query_no_pagination_when_all_fit() {
        let svc = make_service();
        create_composite_table(&svc);

        for i in 0..2 {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "composite-table",
                    "Item": {
                        "pk": { "S": "user1" },
                        "sk": { "S": format!("item{i}") }
                    }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        let req = make_request(
            "Query",
            json!({
                "TableName": "composite-table",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": { ":pk": { "S": "user1" } },
                "Limit": 10
            }),
        );
        let resp = svc.query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Count"], 2);
        assert!(
            body["LastEvaluatedKey"].is_null(),
            "should not have LastEvaluatedKey when all items fit"
        );
    }

    fn create_gsi_table(svc: &DynamoDbService) {
        let req = make_request(
            "CreateTable",
            json!({
                "TableName": "gsi-table",
                "KeySchema": [
                    { "AttributeName": "pk", "KeyType": "HASH" }
                ],
                "AttributeDefinitions": [
                    { "AttributeName": "pk", "AttributeType": "S" },
                    { "AttributeName": "gsi_pk", "AttributeType": "S" },
                    { "AttributeName": "gsi_sk", "AttributeType": "S" }
                ],
                "BillingMode": "PAY_PER_REQUEST",
                "GlobalSecondaryIndexes": [
                    {
                        "IndexName": "gsi-index",
                        "KeySchema": [
                            { "AttributeName": "gsi_pk", "KeyType": "HASH" },
                            { "AttributeName": "gsi_sk", "KeyType": "RANGE" }
                        ],
                        "Projection": { "ProjectionType": "ALL" }
                    }
                ]
            }),
        );
        svc.create_table(&req).unwrap();
    }

    #[test]
    fn gsi_query_last_evaluated_key_includes_table_pk() {
        let svc = make_service();
        create_gsi_table(&svc);

        // Insert 3 items with the SAME GSI key but different table PKs
        for i in 0..3 {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "gsi-table",
                    "Item": {
                        "pk": { "S": format!("item{i}") },
                        "gsi_pk": { "S": "shared" },
                        "gsi_sk": { "S": "sort" }
                    }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        // Query GSI with Limit=1 to trigger pagination
        let req = make_request(
            "Query",
            json!({
                "TableName": "gsi-table",
                "IndexName": "gsi-index",
                "KeyConditionExpression": "gsi_pk = :v",
                "ExpressionAttributeValues": { ":v": { "S": "shared" } },
                "Limit": 1
            }),
        );
        let resp = svc.query(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Count"], 1);
        let lek = &body["LastEvaluatedKey"];
        assert!(lek.is_object(), "should have LastEvaluatedKey");
        // Must contain the index keys
        assert!(lek["gsi_pk"].is_object(), "LEK must contain gsi_pk");
        assert!(lek["gsi_sk"].is_object(), "LEK must contain gsi_sk");
        // Must also contain the table PK
        assert!(
            lek["pk"].is_object(),
            "LEK must contain table PK for GSI queries"
        );
    }

    #[test]
    fn gsi_query_pagination_returns_all_items() {
        let svc = make_service();
        create_gsi_table(&svc);

        // Insert 4 items with the SAME GSI key but different table PKs
        for i in 0..4 {
            let req = make_request(
                "PutItem",
                json!({
                    "TableName": "gsi-table",
                    "Item": {
                        "pk": { "S": format!("item{i:03}") },
                        "gsi_pk": { "S": "shared" },
                        "gsi_sk": { "S": "sort" }
                    }
                }),
            );
            svc.put_item(&req).unwrap();
        }

        // Paginate through all items with Limit=2
        let mut all_pks = Vec::new();
        let mut lek: Option<Value> = None;

        loop {
            let mut query = json!({
                "TableName": "gsi-table",
                "IndexName": "gsi-index",
                "KeyConditionExpression": "gsi_pk = :v",
                "ExpressionAttributeValues": { ":v": { "S": "shared" } },
                "Limit": 2
            });
            if let Some(ref start_key) = lek {
                query["ExclusiveStartKey"] = start_key.clone();
            }

            let req = make_request("Query", query);
            let resp = svc.query(&req).unwrap();
            let body: Value = serde_json::from_slice(&resp.body).unwrap();

            for item in body["Items"].as_array().unwrap() {
                let pk = item["pk"]["S"].as_str().unwrap().to_string();
                all_pks.push(pk);
            }

            if body["LastEvaluatedKey"].is_object() {
                lek = Some(body["LastEvaluatedKey"].clone());
            } else {
                break;
            }
        }

        all_pks.sort();
        assert_eq!(
            all_pks,
            vec!["item000", "item001", "item002", "item003"],
            "pagination should return all items without duplicates"
        );
    }

    fn cond_item(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), json!({"S": v})))
            .collect()
    }

    fn cond_names(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn cond_values(pairs: &[(&str, &str)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), json!({"S": v})))
            .collect()
    }

    #[test]
    fn test_evaluate_condition_bare_not_equal() {
        let item = cond_item(&[("state", "active")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":c", "complete")]);

        assert!(evaluate_condition("#s <> :c", Some(&item), &names, &values).is_ok());

        let item2 = cond_item(&[("state", "complete")]);
        assert!(evaluate_condition("#s <> :c", Some(&item2), &names, &values).is_err());
    }

    #[test]
    fn test_evaluate_condition_parenthesized_not_equal() {
        let item = cond_item(&[("state", "active")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":c", "complete")]);

        assert!(evaluate_condition("(#s <> :c)", Some(&item), &names, &values).is_ok());
    }

    #[test]
    fn test_evaluate_condition_parenthesized_equal_mismatch() {
        let item = cond_item(&[("state", "active")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":c", "complete")]);

        assert!(evaluate_condition("(#s = :c)", Some(&item), &names, &values).is_err());
    }

    #[test]
    fn test_evaluate_condition_compound_and() {
        let item = cond_item(&[("state", "active")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":c", "complete"), (":f", "failed")]);

        // active <> complete AND active <> failed => true
        assert!(
            evaluate_condition("(#s <> :c) AND (#s <> :f)", Some(&item), &names, &values).is_ok()
        );
    }

    #[test]
    fn test_evaluate_condition_compound_and_mismatch() {
        let item = cond_item(&[("state", "inactive")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":a", "active"), (":b", "active")]);

        // inactive = active AND inactive = active => false
        assert!(
            evaluate_condition("(#s = :a) AND (#s = :b)", Some(&item), &names, &values).is_err()
        );
    }

    #[test]
    fn test_evaluate_condition_compound_or() {
        let item = cond_item(&[("state", "running")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":a", "active"), (":b", "idle")]);

        // running = active OR running = idle => false
        assert!(
            evaluate_condition("(#s = :a) OR (#s = :b)", Some(&item), &names, &values).is_err()
        );

        // running = active OR running = running => true
        let values2 = cond_values(&[(":a", "active"), (":b", "running")]);
        assert!(
            evaluate_condition("(#s = :a) OR (#s = :b)", Some(&item), &names, &values2).is_ok()
        );
    }

    #[test]
    fn test_evaluate_condition_not_operator() {
        let item = cond_item(&[("state", "active")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":c", "complete")]);

        // NOT (active = complete) => NOT false => true
        assert!(evaluate_condition("NOT (#s = :c)", Some(&item), &names, &values).is_ok());

        // NOT (active <> complete) => NOT true => false
        assert!(evaluate_condition("NOT (#s <> :c)", Some(&item), &names, &values).is_err());

        // NOT attribute_exists(#s) on existing item => NOT true => false
        assert!(
            evaluate_condition("NOT attribute_exists(#s)", Some(&item), &names, &values).is_err()
        );

        // NOT attribute_exists(#s) on missing item => NOT false => true
        assert!(evaluate_condition("NOT attribute_exists(#s)", None, &names, &values).is_ok());
    }

    #[test]
    fn test_evaluate_condition_begins_with() {
        // After unification, conditions support begins_with via
        // evaluate_single_filter_condition (previously only filters had it).
        let item = cond_item(&[("name", "fakecloud-dynamodb")]);
        let names = cond_names(&[("#n", "name")]);
        let values = cond_values(&[(":p", "fakecloud")]);

        assert!(evaluate_condition("begins_with(#n, :p)", Some(&item), &names, &values).is_ok());

        let values2 = cond_values(&[(":p", "realcloud")]);
        assert!(evaluate_condition("begins_with(#n, :p)", Some(&item), &names, &values2).is_err());
    }

    #[test]
    fn test_evaluate_condition_contains() {
        let item = cond_item(&[("tags", "alpha,beta,gamma")]);
        let names = cond_names(&[("#t", "tags")]);
        let values = cond_values(&[(":v", "beta")]);

        assert!(evaluate_condition("contains(#t, :v)", Some(&item), &names, &values).is_ok());

        let values2 = cond_values(&[(":v", "delta")]);
        assert!(evaluate_condition("contains(#t, :v)", Some(&item), &names, &values2).is_err());
    }

    #[test]
    fn test_evaluate_condition_no_existing_item() {
        // When no item exists (PutItem with condition), attribute_not_exists
        // should succeed and attribute_exists should fail.
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":v", "active")]);

        assert!(evaluate_condition("attribute_not_exists(#s)", None, &names, &values).is_ok());
        assert!(evaluate_condition("attribute_exists(#s)", None, &names, &values).is_err());
        // Comparison against missing item: None != Some(val) => true for <>
        assert!(evaluate_condition("#s <> :v", None, &names, &values).is_ok());
        // None == Some(val) => false for =
        assert!(evaluate_condition("#s = :v", None, &names, &values).is_err());
    }

    #[test]
    fn test_evaluate_filter_not_operator() {
        let item = cond_item(&[("status", "pending")]);
        let names = cond_names(&[("#s", "status")]);
        let values = cond_values(&[(":v", "pending")]);

        assert!(!evaluate_filter_expression(
            "NOT (#s = :v)",
            &item,
            &names,
            &values
        ));
        assert!(evaluate_filter_expression(
            "NOT (#s <> :v)",
            &item,
            &names,
            &values
        ));
    }

    #[test]
    fn test_evaluate_filter_expression_in_match() {
        // aws-sdk-go v2's expression.Name("state").In(Value("active"), Value("pending"))
        // emits "#0 IN (:0, :1)". Before fix: neither evaluate_single_filter_condition
        // nor evaluate_single_key_condition handled IN, so the filter leaf fell through
        // to the simple-comparison loop, hit no operators, and returned `true` — meaning
        // every item matched every IN filter regardless of value.
        let item = cond_item(&[("state", "active")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":a", "active"), (":p", "pending")]);

        assert!(
            evaluate_filter_expression("#s IN (:a, :p)", &item, &names, &values),
            "state=active should match IN (active, pending)"
        );
    }

    #[test]
    fn test_evaluate_filter_expression_in_no_match() {
        let item = cond_item(&[("state", "complete")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":a", "active"), (":p", "pending")]);

        assert!(
            !evaluate_filter_expression("#s IN (:a, :p)", &item, &names, &values),
            "state=complete should not match IN (active, pending)"
        );
    }

    #[test]
    fn test_evaluate_filter_expression_in_no_spaces() {
        // orderbot emits the raw form
        //     "#status IN (" + strings.Join(keys, ",") + ")"
        // which produces "IN (:v0,:v1,:v2)" — no spaces after commas. Must parse.
        let item = cond_item(&[("status", "shipped")]);
        let names = cond_names(&[("#s", "status")]);
        let values = cond_values(&[(":a", "pending"), (":b", "shipped"), (":c", "delivered")]);

        assert!(
            evaluate_filter_expression("#s IN (:a,:b,:c)", &item, &names, &values),
            "no-space IN list should still parse"
        );
    }

    #[test]
    fn test_evaluate_filter_expression_in_missing_attribute() {
        // A missing attribute must not match any IN list — the silent-true
        // fallthrough would wrongly accept these items.
        let item: HashMap<String, AttributeValue> = HashMap::new();
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":a", "active")]);

        assert!(
            !evaluate_filter_expression("#s IN (:a)", &item, &names, &values),
            "missing attribute should not match any IN list"
        );
    }

    #[test]
    fn test_evaluate_filter_expression_compound_in_and_eq() {
        // Shape emitted by `Name("state").In(...).And(Name("priority").Equal(...))`:
        //     "(#0 IN (:0, :1)) AND (#1 = :2)"
        // split_on_and handles the outer parens, but the IN leaf had the
        // silent-true fallthrough, so any item with priority=high would match
        // regardless of state.
        let item = cond_item(&[("state", "active"), ("priority", "high")]);
        let names = cond_names(&[("#s", "state"), ("#p", "priority")]);
        let values = cond_values(&[(":a", "active"), (":pe", "pending"), (":h", "high")]);

        assert!(
            evaluate_filter_expression("(#s IN (:a, :pe)) AND (#p = :h)", &item, &names, &values,),
            "(active IN (active, pending)) AND (high = high) should match"
        );

        let item2 = cond_item(&[("state", "complete"), ("priority", "high")]);
        assert!(
            !evaluate_filter_expression("(#s IN (:a, :pe)) AND (#p = :h)", &item2, &names, &values,),
            "(complete IN (active, pending)) AND (high = high) should not match"
        );
    }

    #[test]
    fn test_evaluate_condition_attribute_exists_with_space() {
        // aws-sdk-go v2's expression.NewBuilder emits function calls with a
        // space between the name and the opening paren:
        //     "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))"
        // Before fix: extract_function_arg used strip_prefix("attribute_exists(")
        // with no space, so these fell through the filter leaf entirely and
        // hit evaluate_single_key_condition's silent-true fallthrough —
        // every conditional write was silently accepted.
        let item = cond_item(&[("store_id", "s-1")]);
        let names = cond_names(&[("#0", "store_id"), ("#1", "active_viewer_tab_id")]);
        let values = cond_values(&[(":0", "tab-A")]);

        // On an existing item without active_viewer_tab_id: exists(store_id)
        // is true, not_exists(active_viewer_tab_id) is true → OK.
        assert!(
            evaluate_condition(
                "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))",
                Some(&item),
                &names,
                &values,
            )
            .is_ok(),
            "claim-lease compound on free item should succeed"
        );

        // On a missing item: exists(store_id) is false → whole AND false → Err.
        assert!(
            evaluate_condition(
                "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))",
                None,
                &names,
                &values,
            )
            .is_err(),
            "claim-lease compound on missing item must fail attribute_exists branch"
        );

        // On an item already held by tab-B: exists ✓, not_exists ✗, #1 = :0 ✗
        // → (✓) AND ((✗) OR (✗)) → false → Err.
        let held = cond_item(&[("store_id", "s-1"), ("active_viewer_tab_id", "tab-B")]);
        assert!(
            evaluate_condition(
                "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))",
                Some(&held),
                &names,
                &values,
            )
            .is_err(),
            "claim-lease compound on item held by another tab must fail"
        );

        // Same tab re-claiming: exists ✓, not_exists ✗, #1 = :0 ✓
        // → (✓) AND ((✗) OR (✓)) → true → Ok.
        let self_held = cond_item(&[("store_id", "s-1"), ("active_viewer_tab_id", "tab-A")]);
        assert!(
            evaluate_condition(
                "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))",
                Some(&self_held),
                &names,
                &values,
            )
            .is_ok(),
            "same-tab re-claim must succeed"
        );
    }

    #[test]
    fn test_evaluate_condition_in_match() {
        // evaluate_condition delegates to evaluate_filter_expression, so this
        // also proves the ConditionExpression path. Before fix: silently Ok.
        let item = cond_item(&[("state", "active")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":a", "active"), (":p", "pending")]);

        assert!(
            evaluate_condition("#s IN (:a, :p)", Some(&item), &names, &values).is_ok(),
            "IN should succeed when actual value is in the list"
        );
    }

    #[test]
    fn test_evaluate_condition_in_no_match() {
        // Before fix: evaluate_condition silently returned Ok(()) for IN — any
        // conditional write was accepted regardless of actual state, the
        // opposite of what the caller asked for.
        let item = cond_item(&[("state", "complete")]);
        let names = cond_names(&[("#s", "state")]);
        let values = cond_values(&[(":a", "active"), (":p", "pending")]);

        assert!(
            evaluate_condition("#s IN (:a, :p)", Some(&item), &names, &values).is_err(),
            "IN should fail when actual value is not in the list"
        );
    }

    #[test]
    fn test_apply_update_set_list_index_replaces_existing() {
        // Shape emitted by orderbot's order-item update retry loop:
        //     UpdateExpression: fmt.Sprintf("SET #items[%d] = :item", index)
        // Before fix: apply_set_assignment called resolve_attr_name on the
        // whole "#items[0]" token, which misses the name map, and then
        // item.insert("#items[0]", :item), producing a top-level key
        // literally named "#items[0]" rather than mutating the list.
        let mut item = HashMap::new();
        item.insert(
            "items".to_string(),
            json!({"L": [
                {"M": {"sku": {"S": "OLD-A"}}},
                {"M": {"sku": {"S": "OLD-B"}}},
            ]}),
        );

        let names = cond_names(&[("#items", "items")]);
        let mut values = HashMap::new();
        values.insert(":item".to_string(), json!({"M": {"sku": {"S": "NEW-A"}}}));

        apply_update_expression(&mut item, "SET #items[0] = :item", &names, &values).unwrap();

        let items_list = item
            .get("items")
            .and_then(|v| v.get("L"))
            .and_then(|v| v.as_array())
            .expect("items should still be a list");
        assert_eq!(items_list.len(), 2, "list length should be unchanged");
        let sku0 = items_list[0]
            .get("M")
            .and_then(|m| m.get("sku"))
            .and_then(|s| s.get("S"))
            .and_then(|s| s.as_str());
        assert_eq!(sku0, Some("NEW-A"), "index 0 should be replaced");
        let sku1 = items_list[1]
            .get("M")
            .and_then(|m| m.get("sku"))
            .and_then(|s| s.get("S"))
            .and_then(|s| s.as_str());
        assert_eq!(sku1, Some("OLD-B"), "index 1 should be untouched");

        assert!(!item.contains_key("items[0]"));
        assert!(!item.contains_key("#items[0]"));
    }

    #[test]
    fn test_apply_update_set_list_index_second_slot() {
        let mut item = HashMap::new();
        item.insert(
            "items".to_string(),
            json!({"L": [
                {"M": {"sku": {"S": "A"}}},
                {"M": {"sku": {"S": "B"}}},
                {"M": {"sku": {"S": "C"}}},
            ]}),
        );

        let names = cond_names(&[("#items", "items")]);
        let mut values = HashMap::new();
        values.insert(":item".to_string(), json!({"M": {"sku": {"S": "B-PRIME"}}}));

        apply_update_expression(&mut item, "SET #items[1] = :item", &names, &values).unwrap();

        let items_list = item
            .get("items")
            .and_then(|v| v.get("L"))
            .and_then(|v| v.as_array())
            .unwrap();
        let skus: Vec<&str> = items_list
            .iter()
            .map(|v| {
                v.get("M")
                    .and_then(|m| m.get("sku"))
                    .and_then(|s| s.get("S"))
                    .and_then(|s| s.as_str())
                    .unwrap()
            })
            .collect();
        assert_eq!(skus, vec!["A", "B-PRIME", "C"]);
    }

    #[test]
    fn test_apply_update_set_list_index_without_name_ref() {
        // Same fix must also work when the LHS is a literal attribute name,
        // not an expression attribute name ref.
        let mut item = HashMap::new();
        item.insert(
            "tags".to_string(),
            json!({"L": [{"S": "red"}, {"S": "blue"}]}),
        );

        let names: HashMap<String, String> = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":t".to_string(), json!({"S": "green"}));

        apply_update_expression(&mut item, "SET tags[1] = :t", &names, &values).unwrap();

        let tags = item
            .get("tags")
            .and_then(|v| v.get("L"))
            .and_then(|v| v.as_array())
            .unwrap();
        assert_eq!(tags[0].get("S").and_then(|s| s.as_str()), Some("red"));
        assert_eq!(tags[1].get("S").and_then(|s| s.as_str()), Some("green"));
    }

    #[test]
    fn test_unrecognized_expression_returns_false() {
        // evaluate_single_key_condition must fail-closed: an expression shape
        // it doesn't recognize should return false (reject), not true (accept).
        let item = cond_item(&[("x", "1")]);
        let names: HashMap<String, String> = HashMap::new();
        let values: HashMap<String, Value> = HashMap::new();

        assert!(
            !evaluate_single_key_condition("GARBAGE NONSENSE", &item, "", &names, &values),
            "unrecognized expression must return false"
        );
    }

    #[test]
    fn test_set_list_index_out_of_range_returns_error() {
        // SET list[N] where N > len must return a ValidationException,
        // not silently no-op.
        let mut item = HashMap::new();
        item.insert("items".to_string(), json!({"L": [{"S": "a"}, {"S": "b"}]}));

        let names: HashMap<String, String> = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":v".to_string(), json!({"S": "z"}));

        let result = apply_update_expression(&mut item, "SET items[5] = :v", &names, &values);
        assert!(
            result.is_err(),
            "out-of-range list index must return an error"
        );

        // List should be unchanged
        let list = item
            .get("items")
            .and_then(|v| v.get("L"))
            .and_then(|v| v.as_array())
            .unwrap();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn test_set_list_index_on_non_list_returns_error() {
        // SET attr[0] = :v where attr is a string (not a list) must return
        // a ValidationException.
        let mut item = HashMap::new();
        item.insert("name".to_string(), json!({"S": "hello"}));

        let names: HashMap<String, String> = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":v".to_string(), json!({"S": "z"}));

        let result = apply_update_expression(&mut item, "SET name[0] = :v", &names, &values);
        assert!(
            result.is_err(),
            "list index on non-list attribute must return an error"
        );
    }

    #[test]
    fn test_unrecognized_update_action_returns_error() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), json!({"S": "hello"}));

        let names: HashMap<String, String> = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":bar".to_string(), json!({"S": "baz"}));

        let result = apply_update_expression(&mut item, "INVALID foo = :bar", &names, &values);
        assert!(
            result.is_err(),
            "unrecognized UpdateExpression action must return an error"
        );
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Invalid UpdateExpression") || err_msg.contains("Syntax error"),
            "error should mention Invalid UpdateExpression, got: {err_msg}"
        );
    }

    // ── size() function tests ──────────────────────────────────────────

    #[test]
    fn test_size_string() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), json!({"S": "hello"}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":limit".to_string(), json!({"N": "5"}));

        assert!(evaluate_single_filter_condition(
            "size(name) = :limit",
            &item,
            &names,
            &values,
        ));
        values.insert(":limit".to_string(), json!({"N": "4"}));
        assert!(evaluate_single_filter_condition(
            "size(name) > :limit",
            &item,
            &names,
            &values,
        ));
    }

    #[test]
    fn test_size_list() {
        let mut item = HashMap::new();
        item.insert(
            "items".to_string(),
            json!({"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]}),
        );
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":limit".to_string(), json!({"N": "3"}));

        assert!(evaluate_single_filter_condition(
            "size(items) = :limit",
            &item,
            &names,
            &values,
        ));
    }

    #[test]
    fn test_size_map() {
        let mut item = HashMap::new();
        item.insert(
            "data".to_string(),
            json!({"M": {"a": {"S": "1"}, "b": {"S": "2"}}}),
        );
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":limit".to_string(), json!({"N": "2"}));

        assert!(evaluate_single_filter_condition(
            "size(data) = :limit",
            &item,
            &names,
            &values,
        ));
    }

    #[test]
    fn test_size_set() {
        let mut item = HashMap::new();
        item.insert("tags".to_string(), json!({"SS": ["a", "b", "c", "d"]}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":limit".to_string(), json!({"N": "3"}));

        assert!(evaluate_single_filter_condition(
            "size(tags) > :limit",
            &item,
            &names,
            &values,
        ));
    }

    // ── attribute_type() function tests ────────────────────────────────

    #[test]
    fn test_attribute_type_string() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), json!({"S": "hello"}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":t".to_string(), json!({"S": "S"}));

        assert!(evaluate_single_filter_condition(
            "attribute_type(name, :t)",
            &item,
            &names,
            &values,
        ));

        values.insert(":t".to_string(), json!({"S": "N"}));
        assert!(!evaluate_single_filter_condition(
            "attribute_type(name, :t)",
            &item,
            &names,
            &values,
        ));
    }

    #[test]
    fn test_attribute_type_number() {
        let mut item = HashMap::new();
        item.insert("age".to_string(), json!({"N": "42"}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":t".to_string(), json!({"S": "N"}));

        assert!(evaluate_single_filter_condition(
            "attribute_type(age, :t)",
            &item,
            &names,
            &values,
        ));
    }

    #[test]
    fn test_attribute_type_list() {
        let mut item = HashMap::new();
        item.insert("items".to_string(), json!({"L": [{"S": "a"}]}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":t".to_string(), json!({"S": "L"}));

        assert!(evaluate_single_filter_condition(
            "attribute_type(items, :t)",
            &item,
            &names,
            &values,
        ));
    }

    #[test]
    fn test_attribute_type_map() {
        let mut item = HashMap::new();
        item.insert("data".to_string(), json!({"M": {"key": {"S": "val"}}}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":t".to_string(), json!({"S": "M"}));

        assert!(evaluate_single_filter_condition(
            "attribute_type(data, :t)",
            &item,
            &names,
            &values,
        ));
    }

    #[test]
    fn test_attribute_type_bool() {
        let mut item = HashMap::new();
        item.insert("active".to_string(), json!({"BOOL": true}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":t".to_string(), json!({"S": "BOOL"}));

        assert!(evaluate_single_filter_condition(
            "attribute_type(active, :t)",
            &item,
            &names,
            &values,
        ));
    }

    // ── begins_with rejects non-string types ───────────────────────────

    #[test]
    fn test_begins_with_rejects_number_type() {
        let mut item = HashMap::new();
        item.insert("code".to_string(), json!({"N": "12345"}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":prefix".to_string(), json!({"S": "123"}));

        assert!(
            !evaluate_single_filter_condition("begins_with(code, :prefix)", &item, &names, &values,),
            "begins_with must return false for N-type attributes"
        );
    }

    #[test]
    fn test_begins_with_works_on_string_type() {
        let mut item = HashMap::new();
        item.insert("code".to_string(), json!({"S": "abc123"}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":prefix".to_string(), json!({"S": "abc"}));

        assert!(evaluate_single_filter_condition(
            "begins_with(code, :prefix)",
            &item,
            &names,
            &values,
        ));
    }

    // ── contains on sets ───────────────────────────────────────────────

    #[test]
    fn test_contains_string_set() {
        let mut item = HashMap::new();
        item.insert("tags".to_string(), json!({"SS": ["red", "blue", "green"]}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":val".to_string(), json!({"S": "blue"}));

        assert!(evaluate_single_filter_condition(
            "contains(tags, :val)",
            &item,
            &names,
            &values,
        ));

        values.insert(":val".to_string(), json!({"S": "yellow"}));
        assert!(!evaluate_single_filter_condition(
            "contains(tags, :val)",
            &item,
            &names,
            &values,
        ));
    }

    #[test]
    fn test_contains_number_set() {
        let mut item = HashMap::new();
        item.insert("scores".to_string(), json!({"NS": ["1", "2", "3"]}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":val".to_string(), json!({"N": "2"}));

        assert!(evaluate_single_filter_condition(
            "contains(scores, :val)",
            &item,
            &names,
            &values,
        ));
    }

    // ── SET arithmetic type validation ─────────────────────────────────

    #[test]
    fn test_set_arithmetic_rejects_string_operand() {
        let mut item = HashMap::new();
        item.insert("name".to_string(), json!({"S": "hello"}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":val".to_string(), json!({"N": "1"}));

        let result = apply_update_expression(&mut item, "SET name = name + :val", &names, &values);
        assert!(
            result.is_err(),
            "arithmetic on S-type attribute must return a ValidationException"
        );
    }

    #[test]
    fn test_set_arithmetic_rejects_string_value() {
        let mut item = HashMap::new();
        item.insert("count".to_string(), json!({"N": "5"}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":val".to_string(), json!({"S": "notanumber"}));

        let result =
            apply_update_expression(&mut item, "SET count = count + :val", &names, &values);
        assert!(
            result.is_err(),
            "arithmetic with S-type value must return a ValidationException"
        );
    }

    #[test]
    fn test_set_arithmetic_valid_numbers() {
        let mut item = HashMap::new();
        item.insert("count".to_string(), json!({"N": "10"}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":val".to_string(), json!({"N": "3"}));

        let result =
            apply_update_expression(&mut item, "SET count = count + :val", &names, &values);
        assert!(result.is_ok());
        assert_eq!(item["count"], json!({"N": "13"}));
    }

    // ── Binary Set (BS) support in ADD/DELETE ──────────────────────────

    #[test]
    fn test_add_binary_set() {
        let mut item = HashMap::new();
        item.insert("data".to_string(), json!({"BS": ["YQ==", "Yg=="]}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":val".to_string(), json!({"BS": ["Yw==", "YQ=="]}));

        let result = apply_update_expression(&mut item, "ADD data :val", &names, &values);
        assert!(result.is_ok());
        let bs = item["data"]["BS"].as_array().unwrap();
        assert_eq!(bs.len(), 3, "should merge sets without duplicates");
        assert!(bs.contains(&json!("YQ==")));
        assert!(bs.contains(&json!("Yg==")));
        assert!(bs.contains(&json!("Yw==")));
    }

    #[test]
    fn test_delete_binary_set() {
        let mut item = HashMap::new();
        item.insert("data".to_string(), json!({"BS": ["YQ==", "Yg==", "Yw=="]}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":val".to_string(), json!({"BS": ["Yg=="]}));

        let result = apply_update_expression(&mut item, "DELETE data :val", &names, &values);
        assert!(result.is_ok());
        let bs = item["data"]["BS"].as_array().unwrap();
        assert_eq!(bs.len(), 2);
        assert!(!bs.contains(&json!("Yg==")));
    }

    #[test]
    fn test_delete_binary_set_removes_attr_when_empty() {
        let mut item = HashMap::new();
        item.insert("data".to_string(), json!({"BS": ["YQ=="]}));
        let names = HashMap::new();
        let mut values = HashMap::new();
        values.insert(":val".to_string(), json!({"BS": ["YQ=="]}));

        let result = apply_update_expression(&mut item, "DELETE data :val", &names, &values);
        assert!(result.is_ok());
        assert!(
            !item.contains_key("data"),
            "attribute should be removed when set becomes empty"
        );
    }
}
