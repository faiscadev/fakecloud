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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved =
            Self::resolve_alias_target(state, &req.region, &target_key_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{target_key_id}' does not exist"),
                )
            })?;

        let alias_arn = kms_alias_arn(&req.region, &state.account_id, &alias_name);
        if state.alias(&req.region, &alias_name).is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "AlreadyExistsException",
                format!("An alias with the name {alias_arn} already exists"),
            ));
        }

        state.insert_alias(
            &req.region,
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
        if state.remove_alias(&req.region, alias_name).is_none() {
            let alias_arn = kms_alias_arn(&req.region, &state.account_id, alias_name);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let resolved =
            Self::resolve_alias_target(state, &req.region, target_key_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{target_key_id}' does not exist"),
                )
            })?;
        let alias = state.alias_mut(&req.region, alias_name).ok_or_else(|| {
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

        // Resolve key_id_filter to the stored id of the request region's key,
        // as CreateAlias resolves a target (so a multi-region key id finds
        // aliases on this region's replica).
        let resolved_filter =
            key_id_filter.and_then(|kid| Self::resolve_alias_target(state, &req.region, kid));

        // Aliases are regional: list only the request region's, in
        // AliasName order. AWS also lists the predefined AWS-managed aliases
        // of every account (recorded: `alias/aws/dynamodb`, `alias/aws/rds`,
        // ...) before their service first uses them, without a target key;
        // the key exists from its first use (see `crate::aws_managed`). A
        // KeyId filter only matches aliases with a target.
        let mut entries: BTreeMap<String, Value> = state
            .aliases_in(&req.region)
            .filter(|a| match (&resolved_filter, key_id_filter) {
                (Some(r), _) => a.target_key_id == *r,
                (None, Some(_)) => false,
                (None, None) => true,
            })
            .map(|a| {
                // A replica is stored under `{region}:{id}`; report its key id.
                let target = state
                    .keys
                    .get(&a.target_key_id)
                    .map_or(a.target_key_id.as_str(), |k| k.key_id.as_str());
                (
                    a.alias_name.clone(),
                    json!({
                        "AliasName": a.alias_name,
                        "AliasArn": a.alias_arn,
                        "TargetKeyId": target,
                    }),
                )
            })
            .collect();
        if key_id_filter.is_none() {
            for service in crate::aws_managed::predefined() {
                let name = format!("alias/aws/{service}");
                if state.alias(&req.region, &name).is_none() {
                    let arn = kms_alias_arn(&req.region, &req.account_id, &name);
                    entries.insert(name.clone(), json!({"AliasName": name, "AliasArn": arn}));
                }
            }
        }
        let all_aliases: Vec<Value> = entries.into_values().collect();

        // Real Limit/Marker pagination (mirrors ListGrants): the marker is the
        // AliasName of the last item on the previous page. all_aliases is in
        // AliasName order (the region's BTreeMap), so resume at the first alias strictly
        // greater than the marker; that way a marker whose alias was deleted
        // between pages still advances instead of restarting from 0.
        let start = match marker {
            Some(m) => all_aliases
                .iter()
                .position(|a| a["AliasName"].as_str() > Some(m))
                .unwrap_or(all_aliases.len()),
            None => 0,
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
