//! Multi-region user pool replicas: `CreateUserPoolReplica`,
//! `DeleteUserPoolReplica`, `ListUserPoolReplicas`, `UpdateUserPoolReplica`.
//!
//! A user pool always has a `PRIMARY` replica in its own (creation) region,
//! synthesized on read. Secondary replicas created in other regions are
//! persisted per pool. fakecloud is single-region in practice, so a replica is
//! a control-plane record (region + status) rather than a real second copy of
//! the directory — but the records round-trip exactly as real Cognito's do.

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::ReplicaEntry;

use super::{ensure_user_pool_exists, require_str, CognitoService};

fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterException",
        msg.into(),
    )
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        msg.into(),
    )
}

fn replica_json(account_id: &str, pool_id: &str, region: &str, status: &str, role: &str) -> Value {
    json!({
        "RegionName": region,
        "Status": status,
        "Role": role,
        "UserPoolArn": format!("arn:aws:cognito-idp:{region}:{account_id}:userpool/{pool_id}"),
    })
}

impl CognitoService {
    pub(super) fn create_user_pool_replica(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?.to_string();
        let region = require_str(&body, "RegionName")?.to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        ensure_user_pool_exists(state, &pool_id)?;
        if region == state.region {
            return Err(invalid_parameter(format!(
                "RegionName {region} is the user pool's primary region; a replica must target a different region."
            )));
        }
        // Replicas reach ACTIVE immediately — fakecloud has no async region
        // bring-up to model.
        state
            .user_pool_replicas
            .entry(pool_id.clone())
            .or_default()
            .insert(
                region.clone(),
                ReplicaEntry {
                    region: region.clone(),
                    status: "ACTIVE".to_string(),
                },
            );
        Ok(AwsResponse::ok_json(json!({
            "UserPoolReplica": replica_json(&req.account_id, &pool_id, &region, "ACTIVE", "SECONDARY"),
        })))
    }

    pub(super) fn delete_user_pool_replica(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?.to_string();
        let region = require_str(&body, "RegionName")?.to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        ensure_user_pool_exists(state, &pool_id)?;
        let removed = state
            .user_pool_replicas
            .get_mut(&pool_id)
            .and_then(|m| m.remove(&region));
        if removed.is_none() {
            return Err(not_found(format!(
                "User pool {pool_id} has no replica in region {region}."
            )));
        }
        Ok(AwsResponse::ok_json(json!({
            "UserPoolReplica": replica_json(&req.account_id, &pool_id, &region, "DELETING", "SECONDARY"),
        })))
    }

    pub(super) fn list_user_pool_replicas(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?.to_string();

        let accounts = self.state.read();
        let empty = crate::state::CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        ensure_user_pool_exists(state, &pool_id)?;

        // The primary region is always present and is synthesized rather than
        // stored; secondary regions follow in sorted order.
        let mut replicas = vec![replica_json(
            &req.account_id,
            &pool_id,
            &state.region,
            "ACTIVE",
            "PRIMARY",
        )];
        if let Some(secondaries) = state.user_pool_replicas.get(&pool_id) {
            for entry in secondaries.values() {
                replicas.push(replica_json(
                    &req.account_id,
                    &pool_id,
                    &entry.region,
                    &entry.status,
                    "SECONDARY",
                ));
            }
        }
        Ok(AwsResponse::ok_json(
            json!({ "UserPoolReplicas": replicas }),
        ))
    }

    pub(super) fn update_user_pool_replica(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?.to_string();
        let region = require_str(&body, "RegionName")?.to_string();
        let status = require_str(&body, "Status")?.to_string();
        if status != "ACTIVE" && status != "INACTIVE" {
            return Err(invalid_parameter(format!(
                "Status must be one of ACTIVE, INACTIVE; got {status}"
            )));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        ensure_user_pool_exists(state, &pool_id)?;
        let entry = state
            .user_pool_replicas
            .get_mut(&pool_id)
            .and_then(|m| m.get_mut(&region))
            .ok_or_else(|| {
                not_found(format!(
                    "User pool {pool_id} has no replica in region {region}."
                ))
            })?;
        entry.status = status.clone();
        Ok(AwsResponse::ok_json(json!({
            "UserPoolReplica": replica_json(&req.account_id, &pool_id, &region, &status, "SECONDARY"),
        })))
    }
}
