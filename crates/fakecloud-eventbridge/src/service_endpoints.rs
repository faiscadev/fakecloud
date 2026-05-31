// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl EventBridgeService {
    pub(super) fn create_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 64)?;
        validate_required("RoutingConfig", &body["RoutingConfig"])?;
        validate_required("EventBuses", &body["EventBuses"])?;

        let description = body["Description"].as_str().map(|s| s.to_string());
        let routing_config = body["RoutingConfig"].clone();
        let replication_config = body.get("ReplicationConfig").cloned();
        let event_buses = body["EventBuses"].as_array().cloned().unwrap_or_default();
        let role_arn = body["RoleArn"].as_str().map(|s| s.to_string());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.endpoints.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceAlreadyExistsException",
                format!("Endpoint {name} already exists."),
            ));
        }

        let endpoint_id = format!("{}.abc123", name);
        let arn = format!(
            "arn:aws:events:{}:{}:endpoint/{}",
            req.region, state.account_id, name
        );
        let endpoint_url = format!(
            "https://{}.endpoint.events.{}.amazonaws.com",
            endpoint_id, req.region
        );
        let now = Utc::now();

        let endpoint = Endpoint {
            name: name.clone(),
            arn: arn.clone(),
            endpoint_id: endpoint_id.clone(),
            endpoint_url: Some(endpoint_url),
            description,
            routing_config: routing_config.clone(),
            replication_config: replication_config.clone(),
            event_buses: event_buses.clone(),
            role_arn: role_arn.clone(),
            state: "ACTIVE".to_string(),
            creation_time: now,
            last_modified_time: now,
        };
        state.endpoints.insert(name.clone(), endpoint);

        let mut resp = json!({
            "Name": name,
            "Arn": arn,
            "State": "ACTIVE",
            "RoutingConfig": routing_config,
            "EventBuses": event_buses,
        });
        if let Some(ref rc) = replication_config {
            resp["ReplicationConfig"] = rc.clone();
        }
        if let Some(ref ra) = role_arn {
            resp["RoleArn"] = json!(ra);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn delete_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.endpoints.remove(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Endpoint '{name}' does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let ep = state.endpoints.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Endpoint '{name}' does not exist."),
            )
        })?;

        let mut resp = json!({
            "Name": ep.name,
            "Arn": ep.arn,
            "EndpointId": ep.endpoint_id,
            "State": ep.state,
            "RoutingConfig": ep.routing_config,
            "EventBuses": ep.event_buses,
            "CreationTime": ep.creation_time.timestamp() as f64,
            "LastModifiedTime": ep.last_modified_time.timestamp() as f64,
        });
        if let Some(ref url) = ep.endpoint_url {
            resp["EndpointUrl"] = json!(url);
        }
        if let Some(ref desc) = ep.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref rc) = ep.replication_config {
            resp["ReplicationConfig"] = rc.clone();
        }
        if let Some(ref ra) = ep.role_arn {
            resp["RoleArn"] = json!(ra);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn list_endpoints(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 64)?;
        validate_optional_string_length("homeRegion", body["HomeRegion"].as_str(), 9, 20)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("maxResults", body["MaxResults"].as_i64(), 1, 100)?;
        let name_prefix = body["NamePrefix"].as_str();
        let limit = body["MaxResults"].as_i64().unwrap_or(100) as usize;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let all: Vec<Value> = state
            .endpoints
            .values()
            .filter(|ep| match name_prefix {
                Some(prefix) => ep.name.starts_with(prefix),
                None => true,
            })
            .map(|ep| {
                let mut obj = json!({
                    "Name": ep.name,
                    "Arn": ep.arn,
                    "EndpointId": ep.endpoint_id,
                    "State": ep.state,
                    "RoutingConfig": ep.routing_config,
                    "EventBuses": ep.event_buses,
                    "CreationTime": ep.creation_time.timestamp() as f64,
                    "LastModifiedTime": ep.last_modified_time.timestamp() as f64,
                });
                if let Some(ref url) = ep.endpoint_url {
                    obj["EndpointUrl"] = json!(url);
                }
                obj
            })
            .collect();

        let (endpoints, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "Endpoints": endpoints });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn update_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let ep = state.endpoints.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Endpoint '{name}' does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            ep.description = Some(desc.to_string());
        }
        if !body["RoutingConfig"].is_null() {
            ep.routing_config = body["RoutingConfig"].clone();
        }
        if let Some(rc) = body.get("ReplicationConfig") {
            ep.replication_config = Some(rc.clone());
        }
        if let Some(buses) = body["EventBuses"].as_array() {
            ep.event_buses = buses.clone();
        }
        if let Some(ra) = body["RoleArn"].as_str() {
            ep.role_arn = Some(ra.to_string());
        }
        ep.last_modified_time = Utc::now();

        let resp = json!({
            "Name": ep.name,
            "Arn": ep.arn,
            "EndpointId": ep.endpoint_id,
            "State": ep.state,
            "RoutingConfig": ep.routing_config,
            "EventBuses": ep.event_buses,
        });

        Ok(AwsResponse::ok_json(resp))
    }

    // ─── DeauthorizeConnection ──────────────────────────────────────────
}
