use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{IdentityProvider, ResourceServer};

use super::{
    identity_provider_to_json, parse_resource_server_scopes, parse_string_map, require_str,
    resource_server_to_json, validate_provider_type, CognitoService,
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

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

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

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

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

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

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

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

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

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let empty = HashMap::new();
        let pool_providers = state.identity_providers.get(pool_id).unwrap_or(&empty);

        let mut providers: Vec<&IdentityProvider> = pool_providers.values().collect();
        providers.sort_by_key(|p| p.creation_date);

        let start_idx = if let Some(token) = next_token {
            providers
                .iter()
                .position(|p| p.provider_name == token)
                .unwrap_or(0)
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

    // ── Resource Servers ──────────────────────────────────────────────

    pub(super) fn create_resource_server(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;
        let name = require_str(&body, "Name")?;

        let scopes = parse_resource_server_scopes(&body["Scopes"]);

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let pool_servers = state
            .resource_servers
            .entry(pool_id.to_string())
            .or_default();
        if pool_servers.contains_key(identifier) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!(
                    "A resource server with identifier {identifier} already exists in this user pool."
                ),
            ));
        }

        let rs = ResourceServer {
            user_pool_id: pool_id.to_string(),
            identifier: identifier.to_string(),
            name: name.to_string(),
            scopes,
        };

        pool_servers.insert(identifier.to_string(), rs.clone());

        Ok(AwsResponse::ok_json(json!({
            "ResourceServer": resource_server_to_json(&rs)
        })))
    }

    pub(super) fn describe_resource_server(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let rs = state
            .resource_servers
            .get(pool_id)
            .and_then(|m| m.get(identifier))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Resource server {identifier} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "ResourceServer": resource_server_to_json(rs)
        })))
    }

    pub(super) fn update_resource_server(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;
        let name = require_str(&body, "Name")?;
        let scopes = parse_resource_server_scopes(&body["Scopes"]);

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let rs = state
            .resource_servers
            .get_mut(pool_id)
            .and_then(|m| m.get_mut(identifier))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Resource server {identifier} does not exist."),
                )
            })?;

        rs.name = name.to_string();
        rs.scopes = scopes;

        Ok(AwsResponse::ok_json(json!({
            "ResourceServer": resource_server_to_json(rs)
        })))
    }

    pub(super) fn delete_resource_server(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let removed = state
            .resource_servers
            .get_mut(pool_id)
            .and_then(|m| m.remove(identifier));

        if removed.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Resource server {identifier} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_resource_servers(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50).clamp(1, 50) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let empty = HashMap::new();
        let pool_servers = state.resource_servers.get(pool_id).unwrap_or(&empty);

        let mut servers: Vec<&ResourceServer> = pool_servers.values().collect();
        servers.sort_by_key(|s| &s.identifier);

        let start_idx = if let Some(token) = next_token {
            servers
                .iter()
                .position(|s| s.identifier == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = servers
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|s| resource_server_to_json(s))
            .collect();

        let has_more = start_idx + max_results < servers.len();
        let mut response = json!({ "ResourceServers": page });
        if has_more {
            if let Some(last) = servers.get(start_idx + max_results) {
                response["NextToken"] = json!(last.identifier);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }
}
