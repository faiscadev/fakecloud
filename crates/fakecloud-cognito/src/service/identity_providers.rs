use std::collections::BTreeMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::IdentityProvider;

use crate::state::CognitoState;

use super::{
    ensure_user_pool_exists, identity_provider_to_json, parse_string_map, require_str,
    validate_provider_type, CognitoService,
};

impl CognitoService {
    pub(super) fn create_identity_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let provider_name = require_str(&body, "ProviderName")?;
        let provider_type = require_str(&body, "ProviderType")?;

        validate_provider_type(provider_type)?;

        let provider_details = parse_string_map(&body["ProviderDetails"]);
        let attribute_mapping = parse_string_map(&body["AttributeMapping"]);
        let idp_identifiers = body["IdpIdentifiers"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        let pool_providers = state
            .identity_providers
            .entry(pool_id.to_string())
            .or_default();
        if pool_providers.contains_key(provider_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DuplicateProviderException",
                format!(
                    "A provider with the name {provider_name} already exists in this user pool."
                ),
            ));
        }

        let now = Utc::now();
        let idp = IdentityProvider {
            user_pool_id: pool_id.to_string(),
            provider_name: provider_name.to_string(),
            provider_type: provider_type.to_string(),
            provider_details,
            attribute_mapping,
            idp_identifiers,
            creation_date: now,
            last_modified_date: now,
        };

        pool_providers.insert(provider_name.to_string(), idp.clone());

        Ok(AwsResponse::ok_json(json!({
            "IdentityProvider": identity_provider_to_json(&idp)
        })))
    }

    pub(super) fn describe_identity_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let provider_name = require_str(&body, "ProviderName")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let idp = state
            .identity_providers
            .get(pool_id)
            .and_then(|providers| providers.get(provider_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Identity provider {provider_name} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "IdentityProvider": identity_provider_to_json(idp)
        })))
    }

    pub(super) fn update_identity_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let provider_name = require_str(&body, "ProviderName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        let idp = state
            .identity_providers
            .get_mut(pool_id)
            .and_then(|providers| providers.get_mut(provider_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Identity provider {provider_name} does not exist."),
                )
            })?;

        if body["ProviderDetails"].is_object() {
            idp.provider_details = parse_string_map(&body["ProviderDetails"]);
        }
        if body["AttributeMapping"].is_object() {
            idp.attribute_mapping = parse_string_map(&body["AttributeMapping"]);
        }
        if let Some(arr) = body["IdpIdentifiers"].as_array() {
            idp.idp_identifiers = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        idp.last_modified_date = Utc::now();

        let idp = idp.clone();

        Ok(AwsResponse::ok_json(json!({
            "IdentityProvider": identity_provider_to_json(&idp)
        })))
    }

    pub(super) fn delete_identity_provider(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let provider_name = require_str(&body, "ProviderName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        let removed = state
            .identity_providers
            .get_mut(pool_id)
            .and_then(|providers| providers.remove(provider_name));

        if removed.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Identity provider {provider_name} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_identity_providers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(60).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let empty = BTreeMap::new();
        let pool_providers = state.identity_providers.get(pool_id).unwrap_or(&empty);

        let mut providers: Vec<&IdentityProvider> = pool_providers.values().collect();
        providers.sort_by_key(|p| p.creation_date);

        let start_idx = if let Some(token) = next_token {
            providers
                .iter()
                .position(|p| p.provider_name == token)
                .unwrap_or(providers.len())
        } else {
            0
        };

        let page: Vec<Value> = providers
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|p| {
                json!({
                    "ProviderName": p.provider_name,
                    "ProviderType": p.provider_type,
                    "CreationDate": p.creation_date.timestamp() as f64,
                    "LastModifiedDate": p.last_modified_date.timestamp() as f64,
                })
            })
            .collect();

        let has_more = start_idx + max_results < providers.len();
        let mut response = json!({ "Providers": page });
        if has_more {
            if let Some(last) = providers.get(start_idx + max_results) {
                response["NextToken"] = json!(last.provider_name);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn get_identity_provider_by_identifier(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let idp_identifier = require_str(&body, "IdpIdentifier")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let empty = BTreeMap::new();
        let pool_providers = state.identity_providers.get(pool_id).unwrap_or(&empty);

        let idp = pool_providers
            .values()
            .find(|p| p.idp_identifiers.iter().any(|id| id == idp_identifier))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Identity provider with identifier {idp_identifier} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "IdentityProvider": identity_provider_to_json(idp)
        })))
    }
}
