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
                    read_capacity_auto_scaling: None,
                    write_capacity_auto_scaling: None,
                    read_capacity_units: None,
                })
            })
            .collect::<Vec<_>>();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
            billing_mode: "PROVISIONED".to_string(),
            provisioned_write_capacity_units: None,
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

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
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

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        let gt = state.global_tables.get(global_table_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "GlobalTableNotFoundException",
                format!("Global table not found: {global_table_name}"),
            )
        })?;

        Self::ok_json(global_table_settings_response(gt))
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
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;
        let start = body["ExclusiveStartGlobalTableName"].as_str();

        let accounts = self.state.read();
        let empty_ddb = crate::state::DynamoDbState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty_ddb);
        // Resume after ExclusiveStartGlobalTableName (BTreeMap is name-ordered)
        // and emit LastEvaluatedGlobalTableName when the page is truncated.
        // Previously the marker was ignored and the remainder was unreachable.
        let matched: Vec<_> = state
            .global_tables
            .values()
            .filter(|gt| match start {
                Some(s) => gt.global_table_name.as_str() > s,
                None => true,
            })
            .collect();
        let truncated = matched.len() > limit;
        let tables: Vec<Value> = matched
            .iter()
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

        let mut resp = json!({ "GlobalTables": tables });
        if truncated {
            resp["LastEvaluatedGlobalTableName"] = json!(matched[limit - 1].global_table_name);
        }
        Self::ok_json(resp)
    }

    pub(super) fn update_global_table(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let global_table_name = require_str(&body, "GlobalTableName")?;
        validate_string_length("globalTableName", global_table_name, 3, 255)?;
        validate_required("replicaUpdates", &body["ReplicaUpdates"])?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
                            read_capacity_auto_scaling: None,
                            write_capacity_auto_scaling: None,
                            read_capacity_units: None,
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        // Persist the billing mode + global provisioned write capacity so
        // they round-trip through DescribeGlobalTableSettings instead of
        // being validated and dropped.
        if let Some(mode) = body["GlobalTableBillingMode"].as_str() {
            gt.billing_mode = mode.to_string();
        }
        if let Some(wcu) = body["GlobalTableProvisionedWriteCapacityUnits"].as_i64() {
            gt.provisioned_write_capacity_units = Some(wcu);
        }
        if gt.billing_mode == "PAY_PER_REQUEST" {
            // On-demand tables have no provisioned capacity.
            gt.provisioned_write_capacity_units = None;
        }

        // Apply per-replica read-capacity updates from ReplicaSettingsUpdate.
        if let Some(updates) = body["ReplicaSettingsUpdate"].as_array() {
            for upd in updates {
                let Some(region) = upd["RegionName"].as_str() else {
                    continue;
                };
                let rcu = upd["ReplicaProvisionedReadCapacityUnits"].as_i64();
                if let Some(replica) = gt
                    .replication_group
                    .iter_mut()
                    .find(|r| r.region_name == region)
                {
                    if let Some(rcu) = rcu {
                        replica.read_capacity_units = Some(rcu);
                    }
                }
            }
        }

        let gt = &state.global_tables[global_table_name];
        Self::ok_json(global_table_settings_response(gt))
    }
}

/// Build the GlobalTableSettings response shape shared by
/// `UpdateGlobalTableSettings` and `DescribeGlobalTableSettings`,
/// reflecting the persisted billing mode and per-replica capacity.
fn global_table_settings_response(gt: &GlobalTableDescription) -> Value {
    let write_cu = gt.provisioned_write_capacity_units.unwrap_or(0);
    let replica_settings: Vec<Value> = gt
        .replication_group
        .iter()
        .map(|r| {
            json!({
                "RegionName": r.region_name,
                "ReplicaStatus": r.replica_status,
                "ReplicaBillingModeSummary": {
                    "BillingMode": gt.billing_mode,
                },
                "ReplicaProvisionedReadCapacityUnits": r.read_capacity_units.unwrap_or(0),
                "ReplicaProvisionedWriteCapacityUnits": write_cu,
            })
        })
        .collect();
    json!({
        "GlobalTableName": gt.global_table_name,
        "ReplicaSettings": replica_settings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::DynamoDbService;
    use crate::state::SharedDynamoDbState;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
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

    /// 1.15: UpdateGlobalTableSettings must persist the billing mode and
    /// provisioned write capacity and reflect them in
    /// DescribeGlobalTableSettings (it previously validated then dropped
    /// the update behind a read lock).
    #[tokio::test]
    async fn update_global_table_settings_persists_and_round_trips() {
        let state = make_state();
        let svc = DynamoDbService::new(state);

        svc.create_global_table(&req_for(
            "CreateGlobalTable",
            json!({
                "GlobalTableName": "Widgets",
                "ReplicationGroup": [{"RegionName": "us-east-1"}, {"RegionName": "eu-west-1"}],
            }),
        ))
        .unwrap();

        let resp = svc
            .update_global_table_settings(&req_for(
                "UpdateGlobalTableSettings",
                json!({
                    "GlobalTableName": "Widgets",
                    "GlobalTableBillingMode": "PROVISIONED",
                    "GlobalTableProvisionedWriteCapacityUnits": 50,
                    "ReplicaSettingsUpdate": [
                        {"RegionName": "us-east-1", "ReplicaProvisionedReadCapacityUnits": 17},
                    ],
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let settings = body["ReplicaSettings"].as_array().unwrap();
        let us = settings
            .iter()
            .find(|r| r["RegionName"] == "us-east-1")
            .unwrap();
        assert_eq!(
            us["ReplicaProvisionedWriteCapacityUnits"].as_i64(),
            Some(50)
        );
        assert_eq!(us["ReplicaProvisionedReadCapacityUnits"].as_i64(), Some(17));
        assert_eq!(
            us["ReplicaBillingModeSummary"]["BillingMode"],
            "PROVISIONED"
        );

        // Describe must reflect the persisted update, not zeros.
        let desc = svc
            .describe_global_table_settings(&req_for(
                "DescribeGlobalTableSettings",
                json!({"GlobalTableName": "Widgets"}),
            ))
            .unwrap();
        let dbody: Value = serde_json::from_slice(desc.body.expect_bytes()).unwrap();
        let dsettings = dbody["ReplicaSettings"].as_array().unwrap();
        let dus = dsettings
            .iter()
            .find(|r| r["RegionName"] == "us-east-1")
            .unwrap();
        assert_eq!(
            dus["ReplicaProvisionedWriteCapacityUnits"].as_i64(),
            Some(50)
        );
        assert_eq!(
            dus["ReplicaProvisionedReadCapacityUnits"].as_i64(),
            Some(17)
        );
    }

    /// PAY_PER_REQUEST clears provisioned write capacity.
    #[tokio::test]
    async fn update_global_table_settings_pay_per_request_clears_write_capacity() {
        let state = make_state();
        let svc = DynamoDbService::new(state);
        svc.create_global_table(&req_for(
            "CreateGlobalTable",
            json!({
                "GlobalTableName": "Widgets",
                "ReplicationGroup": [{"RegionName": "us-east-1"}],
            }),
        ))
        .unwrap();
        let resp = svc
            .update_global_table_settings(&req_for(
                "UpdateGlobalTableSettings",
                json!({
                    "GlobalTableName": "Widgets",
                    "GlobalTableBillingMode": "PAY_PER_REQUEST",
                }),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let us = body["ReplicaSettings"][0].clone();
        assert_eq!(us["ReplicaProvisionedWriteCapacityUnits"].as_i64(), Some(0));
        assert_eq!(
            us["ReplicaBillingModeSummary"]["BillingMode"],
            "PAY_PER_REQUEST"
        );
    }
}
