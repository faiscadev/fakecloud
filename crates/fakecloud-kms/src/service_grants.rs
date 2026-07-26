// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl KmsService {
    pub(super) fn create_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let grantee_principal = body["GranteePrincipal"].as_str().unwrap_or("").to_string();
        let retiring_principal = body["RetiringPrincipal"].as_str().map(|s| s.to_string());
        let operations: Vec<String> = body["Operations"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let constraints = if body["Constraints"].is_null() {
            None
        } else {
            Some(body["Constraints"].clone())
        };
        let name = body["Name"].as_str().map(|s| s.to_string());

        let grant_id = Uuid::new_v4().to_string();
        let grant_token = Uuid::new_v4().to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.grants.push(KmsGrant {
            grant_id: grant_id.clone(),
            grant_token: grant_token.clone(),
            key_id: resolved,
            grantee_principal,
            retiring_principal,
            operations,
            constraints,
            name,
            creation_date: Utc::now().timestamp() as f64,
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "GrantId": grant_id,
                "GrantToken": grant_token,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn list_grants(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let grant_id_filter = body["GrantId"].as_str();

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let grants: Vec<Value> = state
            .grants
            .iter()
            .filter(|g| g.key_id == resolved)
            .filter(|g| {
                if let Some(gid) = grant_id_filter {
                    g.grant_id == gid
                } else {
                    true
                }
            })
            .map(|g| grant_to_json(g, &req.account_id))
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Grants": grants,
                "Truncated": false,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn list_retirable_grants(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        // ListRetirableGrants declares InvalidArn / InvalidMarker / NotFound /
        // KMSInternal / DependencyTimeout. Map RetiringPrincipal shape
        // failures onto InvalidArnException and Limit/Marker onto
        // InvalidMarkerException.
        recoded("InvalidArnException", || {
            validate_required("RetiringPrincipal", &body["RetiringPrincipal"])
        })?;
        let retiring_principal = body["RetiringPrincipal"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArnException",
                "RetiringPrincipal must be a string",
            )
        })?;
        recoded("InvalidArnException", || {
            validate_string_length("retiringPrincipal", retiring_principal, 1, 256)
        })?;
        recoded("InvalidMarkerException", || {
            validate_optional_json_range("limit", &body["Limit"], 1, 1000)
        })?;
        recoded("InvalidMarkerException", || {
            validate_optional_string_length("marker", body["Marker"].as_str(), 1, 320)
        })?;

        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut all_grants: Vec<Value> = state
            .grants
            .iter()
            .filter(|g| {
                g.retiring_principal
                    .as_deref()
                    .is_some_and(|rp| rp == retiring_principal)
            })
            .map(|g| grant_to_json(g, &req.account_id))
            .collect();
        // Sort by GrantId so the marker (the GrantId of the last item on the
        // previous page) is a stable total-ordering key. Grants are stored in an
        // insertion-order Vec, so without this the marker could not be resolved
        // by comparison.
        all_grants.sort_by(|a, b| a["GrantId"].as_str().cmp(&b["GrantId"].as_str()));

        // Resume at the first grant whose GrantId is strictly greater than the
        // marker, so a marker whose grant was retired/revoked between pages still
        // advances instead of falling back to 0 and restarting the listing.
        let start = match marker {
            Some(m) => all_grants
                .iter()
                .position(|g| g["GrantId"].as_str() > Some(m))
                .unwrap_or(all_grants.len()),
            None => 0,
        };

        let page = &all_grants[start..all_grants.len().min(start + limit)];
        let truncated = start + limit < all_grants.len();

        let mut result = json!({
            "Grants": page,
            "Truncated": truncated,
        });

        if truncated {
            if let Some(last) = page.last() {
                result["NextMarker"] = last["GrantId"].clone();
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(super) fn revoke_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let grant_id = body["GrantId"].as_str().unwrap_or("");

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let idx = state
            .grants
            .iter()
            .position(|g| g.key_id == resolved && g.grant_id == grant_id);

        match idx {
            Some(i) => {
                state.grants.remove(i);
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Grant ID {grant_id} not found"),
            )),
        }
    }

    pub(super) fn retire_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let grant_token = body["GrantToken"].as_str();
        let grant_id = body["GrantId"].as_str();
        let key_id = body["KeyId"].as_str();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let idx = if let Some(token) = grant_token {
            state.grants.iter().position(|g| g.grant_token == token)
        } else if let (Some(kid), Some(gid)) = (key_id, grant_id) {
            let resolved = Self::resolve_key_id_with_state(state, kid);
            resolved.and_then(|r| {
                state
                    .grants
                    .iter()
                    .position(|g| g.key_id == r && g.grant_id == gid)
            })
        } else {
            None
        };

        match idx {
            Some(i) => {
                state.grants.remove(i);
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                "Grant not found",
            )),
        }
    }
}
