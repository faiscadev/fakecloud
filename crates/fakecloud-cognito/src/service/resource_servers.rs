use std::collections::BTreeMap;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::ResourceServer;

use crate::state::CognitoState;

use super::{
    ensure_user_pool_exists, parse_resource_server_scopes, require_str, resource_server_to_json,
    CognitoService,
};

impl CognitoService {
    pub(super) fn create_resource_server(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;
        let name = require_str(&body, "Name")?;

        let scopes = parse_resource_server_scopes(&body["Scopes"]);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

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

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

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

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let empty = BTreeMap::new();
        let pool_servers = state.resource_servers.get(pool_id).unwrap_or(&empty);

        let mut servers: Vec<&ResourceServer> = pool_servers.values().collect();
        servers.sort_by_key(|s| &s.identifier);

        let start_idx = if let Some(token) = next_token {
            servers
                .iter()
                .position(|s| s.identifier == token)
                .unwrap_or(servers.len())
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
