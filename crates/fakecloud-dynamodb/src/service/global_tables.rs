use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{GlobalTableDescription, ReplicaDescription};

use super::{require_str, DynamoDbService};

impl DynamoDbService {
    pub(super) fn create_global_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn describe_global_table(
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

    pub(super) fn describe_global_table_settings(
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

    pub(super) fn list_global_tables(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn update_global_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn update_global_table_settings(
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
}
