use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use http::StatusCode;
use parking_lot::RwLock;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{
    BackupDescription, DynamoTable, ExportDescription, ImportDescription, ProvisionedThroughput,
};

use super::{
    build_table_description, build_table_description_json, find_table_by_arn,
    find_table_by_arn_mut, get_table, get_table_mut, parse_attribute_definitions, parse_gsi,
    parse_key_schema, parse_lsi, parse_provisioned_throughput, parse_tags, require_str,
    DynamoDbService,
};

impl DynamoDbService {
    pub(super) fn create_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn describe_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let table_name = require_str(&body, "TableName")?;

        let state = self.state.read();
        let table = get_table(&state.tables, table_name)?;

        let table_desc = build_table_description(table);

        Self::ok_json(json!({ "Table": table_desc }))
    }

    pub(super) fn list_tables(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn update_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    // ── TTL ─────────────────────────────────────────────────────────────

    pub(super) fn update_time_to_live(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn describe_time_to_live(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    // ── Tags ────────────────────────────────────────────────────────────

    pub(super) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_tags_of_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;

        let state = self.state.read();
        let table = find_table_by_arn(&state.tables, resource_arn)?;

        let tags = fakecloud_core::tags::tags_to_json(&table.tags, "Key", "Value");

        Self::ok_json(json!({ "Tags": tags }))
    }

    // ── Resource Policies ───────────────────────────────────────────────

    pub(super) fn put_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;
        let policy = require_str(&body, "Policy")?;

        let mut state = self.state.write();
        let table = find_table_by_arn_mut(&mut state.tables, resource_arn)?;
        table.resource_policy = Some(policy.to_string());

        let revision_id = uuid::Uuid::new_v4().to_string();
        Self::ok_json(json!({ "RevisionId": revision_id }))
    }

    pub(super) fn get_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let resource_arn = require_str(&body, "ResourceArn")?;

        let mut state = self.state.write();
        let table = find_table_by_arn_mut(&mut state.tables, resource_arn)?;
        table.resource_policy = None;

        Self::ok_json(json!({}))
    }

    // ── Backups ─────────────────────────────────────────────────────────

    pub(super) fn create_backup(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_backup(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn describe_backup(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_backups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn restore_table_from_backup(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn restore_table_to_point_in_time(
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

    // ── Continuous Backups ───────────────────────────────────────────────

    pub(super) fn update_continuous_backups(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn describe_continuous_backups(
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

    // ── Import/Export ──────────────────────────────────────────────────

    pub(super) fn export_table_to_point_in_time(
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

    pub(super) fn describe_export(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_exports(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn import_table(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn describe_import(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_imports(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
