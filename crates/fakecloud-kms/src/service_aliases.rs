// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl KmsService {
    pub(super) fn create_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // CreateAlias' Smithy contract does not declare `ValidationException`;
        // input/length problems surface as `InvalidAliasNameException`.
        let alias_name = require_string_field(&body, "AliasName")
            .map_err(|e| recode_validation(e, "InvalidAliasNameException"))?;
        let target_key_id = require_string_field(&body, "TargetKeyId")
            .map_err(|e| recode_validation(e, "InvalidAliasNameException"))?;

        validate_alias_name(&alias_name)
            .map_err(|e| recode_validation(e, "InvalidAliasNameException"))?;
        validate_alias_target(&target_key_id)
            .map_err(|e| recode_validation(e, "InvalidAliasNameException"))?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &target_key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{target_key_id}' does not exist"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.aliases.contains_key(&alias_name) {
            let alias_arn = format!(
                "arn:aws:kms:{}:{}:{}",
                state.region, state.account_id, alias_name
            );
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AlreadyExistsException",
                format!("An alias with the name {alias_arn} already exists"),
            ));
        }

        let alias_arn = format!(
            "arn:aws:kms:{}:{}:{}",
            state.region, state.account_id, alias_name
        );

        state.aliases.insert(
            alias_name.clone(),
            KmsAlias {
                alias_name,
                alias_arn,
                target_key_id: resolved,
                creation_date: Utc::now().timestamp() as f64,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn delete_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // DeleteAlias only declares NotFound / KMSInternal / DependencyTimeout /
        // KMSInvalidState in its Smithy contract. Surface missing/malformed
        // input as NotFoundException to stay within the declared error set.
        let alias_name = body["AliasName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                "AliasName is required",
            )
        })?;

        if !alias_name.starts_with("alias/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                "Invalid identifier",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.aliases.remove(alias_name).is_none() {
            let alias_arn = format!(
                "arn:aws:kms:{}:{}:{}",
                state.region, state.account_id, alias_name
            );
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Alias {alias_arn} is not found."),
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn update_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let alias_name = body["AliasName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "AliasName is required",
            )
        })?;
        let target_key_id = body["TargetKeyId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "TargetKeyId is required",
            )
        })?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, target_key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{target_key_id}' does not exist"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let alias = state.aliases.get_mut(alias_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Alias '{alias_name}' does not exist"),
            )
        })?;

        alias.target_key_id = resolved;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn list_aliases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        // ListAliases declares InvalidArnException + InvalidMarkerException
        // (and a few infrastructure errors), but not ValidationException.
        // Map Marker/Limit failures onto InvalidMarkerException and KeyId
        // failures onto InvalidArnException.
        recoded("InvalidMarkerException", || {
            validate_optional_json_range("limit", &body["Limit"], 1, 100)
        })?;
        recoded("InvalidMarkerException", || {
            validate_optional_string_length("marker", body["Marker"].as_str(), 1, 320)
        })?;

        if !body["KeyId"].is_null() && !body["KeyId"].is_string() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArnException",
                "KeyId must be a string",
            ));
        }
        recoded("InvalidArnException", || {
            validate_optional_string_length("keyId", body["KeyId"].as_str(), 1, 2048)
        })?;

        let key_id_filter = body["KeyId"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;
        let marker = body["Marker"].as_str();

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Resolve key_id_filter to actual key ID if needed
        let resolved_filter =
            key_id_filter.and_then(|kid| Self::resolve_key_id_with_state(state, kid));

        let all_aliases: Vec<Value> = state
            .aliases
            .values()
            .filter(|a| match (&resolved_filter, key_id_filter) {
                (Some(r), _) => a.target_key_id == *r,
                (None, Some(_)) => false,
                (None, None) => true,
            })
            .map(|a| {
                json!({
                    "AliasName": a.alias_name,
                    "AliasArn": a.alias_arn,
                    "TargetKeyId": a.target_key_id,
                })
            })
            .collect();

        // Real Limit/Marker pagination (mirrors ListGrants): the marker
        // is the AliasName of the last item on the previous page; resume
        // after it.
        let start = if let Some(m) = marker {
            all_aliases
                .iter()
                .position(|a| a["AliasName"].as_str() == Some(m))
                .map(|pos| pos + 1)
                .unwrap_or(0)
        } else {
            0
        };

        let page = &all_aliases[start..all_aliases.len().min(start + limit)];
        let truncated = start + limit < all_aliases.len();

        let mut result = json!({
            "Aliases": page,
            "Truncated": truncated,
        });
        if truncated {
            if let Some(last) = page.last() {
                result["NextMarker"] = last["AliasName"].clone();
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }
}
