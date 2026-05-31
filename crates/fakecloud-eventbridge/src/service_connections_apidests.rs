// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl EventBridgeService {
    pub(super) fn deauthorize_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let conn = state.connections.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        conn.connection_state = "DEAUTHORIZING".to_string();
        conn.last_modified_time = Utc::now();

        let resp = json!({
            "ConnectionArn": conn.arn,
            "ConnectionState": conn.connection_state,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        });

        Ok(AwsResponse::ok_json(resp))
    }

    // ─── PutEvents ──────────────────────────────────────────────────────

    pub(super) fn create_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_required("AuthorizationType", &body["AuthorizationType"])?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let auth_type = body["AuthorizationType"]
            .as_str()
            .ok_or_else(|| missing("AuthorizationType"))?
            .to_string();
        validate_enum(
            "authorizationType",
            &auth_type,
            &["BASIC", "OAUTH_CLIENT_CREDENTIALS", "API_KEY"],
        )?;
        validate_optional_string_length(
            "kmsKeyIdentifier",
            body["KmsKeyIdentifier"].as_str(),
            0,
            2048,
        )?;
        validate_required("AuthParameters", &body["AuthParameters"])?;
        let auth_params = body["AuthParameters"].clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let now = Utc::now();
        let conn_uuid = uuid::Uuid::new_v4();
        let arn = format!(
            "arn:aws:events:{}:{}:connection/{}/{}",
            req.region, state.account_id, name, conn_uuid
        );
        let secret_arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:events!connection/{}/{}",
            req.region, state.account_id, name, conn_uuid
        );

        let conn = Connection {
            name: name.clone(),
            arn: arn.clone(),
            description,
            authorization_type: auth_type.clone(),
            auth_parameters: auth_params,
            connection_state: "AUTHORIZED".to_string(),
            secret_arn: secret_arn.clone(),
            creation_time: now,
            last_modified_time: now,
            last_authorized_time: now,
        };
        state.connections.insert(name, conn);

        Ok(AwsResponse::ok_json(json!({
            "ConnectionArn": arn,
            "ConnectionState": "AUTHORIZED",
            "CreationTime": now.timestamp() as f64,
            "LastModifiedTime": now.timestamp() as f64,
        })))
    }

    pub(super) fn describe_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let conn = state.connections.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        // Build auth parameters response - strip secrets
        let auth_params_response =
            build_auth_params_response(&conn.authorization_type, &conn.auth_parameters);

        let mut resp = json!({
            "ConnectionArn": conn.arn,
            "Name": conn.name,
            "AuthorizationType": conn.authorization_type,
            "AuthParameters": auth_params_response,
            "ConnectionState": conn.connection_state,
            "SecretArn": conn.secret_arn,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        });
        if let Some(ref desc) = conn.description {
            resp["Description"] = json!(desc);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn list_connections(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 64)?;
        validate_optional_enum(
            "connectionState",
            body["ConnectionState"].as_str(),
            &[
                "CREATING",
                "UPDATING",
                "DELETING",
                "AUTHORIZED",
                "DEAUTHORIZED",
                "AUTHORIZING",
                "DEAUTHORIZING",
                "ACTIVE",
                "FAILED_CONNECTIVITY",
            ],
        )?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;

        let name_prefix = body["NamePrefix"].as_str();
        let connection_state = body["ConnectionState"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .connections
            .values()
            .filter(|c| {
                if let Some(prefix) = name_prefix {
                    if !c.name.starts_with(prefix) {
                        return false;
                    }
                }
                if let Some(cs) = connection_state {
                    if c.connection_state != cs {
                        return false;
                    }
                }
                true
            })
            .map(|c| {
                json!({
                    "ConnectionArn": c.arn,
                    "Name": c.name,
                    "AuthorizationType": c.authorization_type,
                    "ConnectionState": c.connection_state,
                    "CreationTime": c.creation_time.timestamp() as f64,
                    "LastModifiedTime": c.last_modified_time.timestamp() as f64,
                    "LastAuthorizedTime": c.last_authorized_time.timestamp() as f64,
                })
            })
            .collect();

        let (conns, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "Connections": conns });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_enum(
            "authorizationType",
            body["AuthorizationType"].as_str(),
            &["BASIC", "OAUTH_CLIENT_CREDENTIALS", "API_KEY"],
        )?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let conn = state.connections.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            conn.description = Some(desc.to_string());
        }
        if let Some(auth_type) = body["AuthorizationType"].as_str() {
            conn.authorization_type = auth_type.to_string();
        }
        if body.get("AuthParameters").is_some() {
            conn.auth_parameters = body["AuthParameters"].clone();
        }
        conn.last_modified_time = Utc::now();

        Ok(AwsResponse::ok_json(json!({
            "ConnectionArn": conn.arn,
            "ConnectionState": conn.connection_state,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        })))
    }

    pub(super) fn delete_connection(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let conn = state.connections.remove(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "ConnectionArn": conn.arn,
            "ConnectionState": conn.connection_state,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        })))
    }

    // ─── API Destination Operations ─────────────────────────────────────

    pub(super) fn create_api_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_required("ConnectionArn", &body["ConnectionArn"])?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let connection_arn = body["ConnectionArn"]
            .as_str()
            .ok_or_else(|| missing("ConnectionArn"))?
            .to_string();
        validate_string_length("connectionArn", &connection_arn, 1, 1600)?;
        validate_required("InvocationEndpoint", &body["InvocationEndpoint"])?;
        let endpoint = body["InvocationEndpoint"]
            .as_str()
            .ok_or_else(|| missing("InvocationEndpoint"))?
            .to_string();
        validate_string_length("invocationEndpoint", &endpoint, 1, 2048)?;
        validate_required("HttpMethod", &body["HttpMethod"])?;
        let http_method = body["HttpMethod"]
            .as_str()
            .ok_or_else(|| missing("HttpMethod"))?
            .to_string();
        validate_enum(
            "httpMethod",
            &http_method,
            &["POST", "GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"],
        )?;
        let rate_limit = body["InvocationRateLimitPerSecond"].as_i64();
        if let Some(r) = rate_limit {
            validate_range_i64("invocationRateLimitPerSecond", r, 1, i64::MAX)?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let now = Utc::now();
        let dest_uuid = uuid::Uuid::new_v4();
        let arn = format!(
            "arn:aws:events:{}:{}:api-destination/{}/{}",
            req.region, state.account_id, name, dest_uuid
        );

        let dest = ApiDestination {
            name: name.clone(),
            arn: arn.clone(),
            description,
            connection_arn,
            invocation_endpoint: endpoint,
            http_method,
            invocation_rate_limit_per_second: rate_limit,
            state: "ACTIVE".to_string(),
            creation_time: now,
            last_modified_time: now,
        };
        state.api_destinations.insert(name, dest);

        Ok(AwsResponse::ok_json(json!({
            "ApiDestinationArn": arn,
            "ApiDestinationState": "ACTIVE",
            "CreationTime": now.timestamp() as f64,
            "LastModifiedTime": now.timestamp() as f64,
        })))
    }

    pub(super) fn describe_api_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let dest = state.api_destinations.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("An api-destination '{name}' does not exist."),
            )
        })?;

        let mut resp = json!({
            "ApiDestinationArn": dest.arn,
            "Name": dest.name,
            "ConnectionArn": dest.connection_arn,
            "InvocationEndpoint": dest.invocation_endpoint,
            "HttpMethod": dest.http_method,
            "ApiDestinationState": dest.state,
            "CreationTime": dest.creation_time.timestamp() as f64,
            "LastModifiedTime": dest.last_modified_time.timestamp() as f64,
        });
        if let Some(ref desc) = dest.description {
            resp["Description"] = json!(desc);
        }
        if let Some(rate) = dest.invocation_rate_limit_per_second {
            resp["InvocationRateLimitPerSecond"] = json!(rate);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn list_api_destinations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 64)?;
        validate_optional_string_length("connectionArn", body["ConnectionArn"].as_str(), 1, 1600)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;

        let name_prefix = body["NamePrefix"].as_str();
        let connection_arn = body["ConnectionArn"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .api_destinations
            .values()
            .filter(|d| {
                if let Some(prefix) = name_prefix {
                    if !d.name.starts_with(prefix) {
                        return false;
                    }
                }
                if let Some(arn) = connection_arn {
                    if d.connection_arn != arn {
                        return false;
                    }
                }
                true
            })
            .map(|d| {
                let mut obj = json!({
                    "ApiDestinationArn": d.arn,
                    "Name": d.name,
                    "ConnectionArn": d.connection_arn,
                    "InvocationEndpoint": d.invocation_endpoint,
                    "HttpMethod": d.http_method,
                    "ApiDestinationState": d.state,
                    "CreationTime": d.creation_time.timestamp() as f64,
                    "LastModifiedTime": d.last_modified_time.timestamp() as f64,
                });
                if let Some(rate) = d.invocation_rate_limit_per_second {
                    obj["InvocationRateLimitPerSecond"] = json!(rate);
                }
                obj
            })
            .collect();

        let (dests, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "ApiDestinations": dests });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_api_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length("connectionArn", body["ConnectionArn"].as_str(), 1, 1600)?;
        validate_optional_string_length(
            "invocationEndpoint",
            body["InvocationEndpoint"].as_str(),
            1,
            2048,
        )?;
        validate_optional_enum(
            "httpMethod",
            body["HttpMethod"].as_str(),
            &["POST", "GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"],
        )?;
        if let Some(r) = body["InvocationRateLimitPerSecond"].as_i64() {
            validate_range_i64("invocationRateLimitPerSecond", r, 1, i64::MAX)?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let dest = state.api_destinations.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("An api-destination '{name}' does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            dest.description = Some(desc.to_string());
        }
        if let Some(endpoint) = body["InvocationEndpoint"].as_str() {
            dest.invocation_endpoint = endpoint.to_string();
        }
        if let Some(method) = body["HttpMethod"].as_str() {
            dest.http_method = method.to_string();
        }
        if let Some(rate) = body["InvocationRateLimitPerSecond"].as_i64() {
            dest.invocation_rate_limit_per_second = Some(rate);
        }
        if let Some(conn) = body["ConnectionArn"].as_str() {
            dest.connection_arn = conn.to_string();
        }
        dest.last_modified_time = Utc::now();

        Ok(AwsResponse::ok_json(json!({
            "ApiDestinationArn": dest.arn,
            "ApiDestinationState": dest.state,
            "CreationTime": dest.creation_time.timestamp() as f64,
            "LastModifiedTime": dest.last_modified_time.timestamp() as f64,
        })))
    }

    pub(super) fn delete_api_destination(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.api_destinations.contains_key(name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("An api-destination '{name}' does not exist."),
            ));
        }
        state.api_destinations.remove(name);

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ─── Replay Operations ──────────────────────────────────────────────
}
