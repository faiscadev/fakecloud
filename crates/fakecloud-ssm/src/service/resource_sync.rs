use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{ResourceDataSync, SsmState};

use super::{missing, SsmService};

impl SsmService {
    pub(super) fn create_resource_data_sync(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        let sync_name = body["SyncName"]
            .as_str()
            .ok_or_else(|| missing("SyncName"))?
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.resource_data_syncs.contains_key(&sync_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceDataSyncAlreadyExistsException",
                format!("Sync {sync_name} already exists"),
            ));
        }

        let now = Utc::now();
        let sync = ResourceDataSync {
            sync_name: sync_name.clone(),
            sync_type: body["SyncType"].as_str().map(|s| s.to_string()),
            sync_source: body.get("SyncSource").cloned(),
            s3_destination: body.get("S3Destination").cloned(),
            created_date: now,
            last_sync_time: None,
            last_successful_sync_time: None,
            last_status: "Successful".to_string(),
            sync_last_modified_time: now,
        };
        state.resource_data_syncs.insert(sync_name, sync);

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_resource_data_sync(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("SyncName", body["SyncName"].as_str(), 1, 64)?;
        let sync_name = body["SyncName"]
            .as_str()
            .ok_or_else(|| missing("SyncName"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.resource_data_syncs.remove(sync_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceDataSyncNotFoundException",
                format!("Sync {sync_name} not found"),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_resource_data_sync(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("SyncType", body["SyncType"].as_str(), 1, 64)?;
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 50)?;
        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let syncs: Vec<Value> = state
            .resource_data_syncs
            .values()
            .map(|s| {
                let mut v = json!({
                    "SyncName": s.sync_name,
                    "LastStatus": s.last_status,
                    "SyncCreatedTime": s.created_date.timestamp_millis() as f64 / 1000.0,
                    "LastSyncStatusMessage": "",
                    "SyncLastModifiedTime": s.sync_last_modified_time.timestamp_millis() as f64 / 1000.0,
                });
                if let Some(ref st) = s.sync_type {
                    v["SyncType"] = json!(st);
                }
                if let Some(ref src) = s.sync_source {
                    v["SyncSource"] = src.clone();
                }
                if let Some(ref dst) = s.s3_destination {
                    v["S3Destination"] = dst.clone();
                }
                if let Some(ref lst) = s.last_sync_time {
                    v["LastSyncTime"] = json!(lst.timestamp_millis() as f64 / 1000.0);
                }
                if let Some(ref lsst) = s.last_successful_sync_time {
                    v["LastSuccessfulSyncTime"] =
                        json!(lsst.timestamp_millis() as f64 / 1000.0);
                }
                v
            })
            .collect();
        Ok(AwsResponse::ok_json(
            json!({ "ResourceDataSyncItems": syncs }),
        ))
    }

    pub(super) fn update_resource_data_sync(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let sync_name = body["SyncName"]
            .as_str()
            .ok_or_else(|| missing("SyncName"))?;
        let sync_type = body["SyncType"]
            .as_str()
            .ok_or_else(|| missing("SyncType"))?;
        let sync_source = body
            .get("SyncSource")
            .cloned()
            .ok_or_else(|| missing("SyncSource"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let sync = state
            .resource_data_syncs
            .get_mut(sync_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceDataSyncNotFoundException",
                    format!("Sync {sync_name} not found"),
                )
            })?;
        // UpdateResourceDataSync only updates SyncFromSource syncs, and the
        // SyncType must match the stored sync — AWS rejects a mismatch rather
        // than silently ignoring the field.
        if sync.sync_type.as_deref() != Some(sync_type) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceDataSyncInvalidConfigurationException",
                format!("SyncType '{sync_type}' does not match the existing sync's type"),
            ));
        }
        sync.sync_source = Some(sync_source);
        sync.sync_last_modified_time = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── GetOpsSummary ─────────────────────────────────────────────
}
