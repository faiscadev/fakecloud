use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{CloudConnector, SsmState};

use super::{missing, SsmService};

/// Epoch seconds (with millisecond fraction) as SSM serializes timestamps.
fn ts(dt: &chrono::DateTime<Utc>) -> f64 {
    dt.timestamp_millis() as f64 / 1000.0
}

fn not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("Cloud connector {id} not found"),
    )
}

impl SsmService {
    pub(super) fn create_cloud_connector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let display_name = body["DisplayName"]
            .as_str()
            .ok_or_else(|| missing("DisplayName"))?
            .to_string();
        validate_string_length("DisplayName", &display_name, 1, 256)?;
        let role_arn = body["RoleArn"]
            .as_str()
            .ok_or_else(|| missing("RoleArn"))?
            .to_string();
        let config_connector_arn = body["ConfigConnectorArn"]
            .as_str()
            .ok_or_else(|| missing("ConfigConnectorArn"))?
            .to_string();
        let configuration = body
            .get("Configuration")
            .filter(|v| v.is_object())
            .cloned()
            .ok_or_else(|| missing("Configuration"))?;
        // Configuration is a union whose only variant is AzureConfiguration.
        if !configuration
            .get("AzureConfiguration")
            .is_some_and(Value::is_object)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Configuration must contain an AzureConfiguration",
            ));
        }
        let description = body["Description"].as_str().map(str::to_string);
        if let Some(ref d) = description {
            validate_string_length("Description", d, 0, 1024)?;
        }
        let tags = parse_tags(body.get("Tags"));

        let id = uuid::Uuid::new_v4().to_string();
        let arn = format!(
            "arn:aws:ssm:{}:{}:cloud-connector/{id}",
            req.region, req.account_id
        );
        let now = Utc::now();
        let connector = CloudConnector {
            id: id.clone(),
            arn,
            display_name,
            description,
            role_arn,
            configuration,
            config_connector_arn,
            tags,
            created_at: now,
            updated_at: now,
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.cloud_connectors.insert(id.clone(), connector);

        Ok(AwsResponse::ok_json(json!({ "CloudConnectorId": id })))
    }

    pub(super) fn get_cloud_connector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = body["CloudConnectorId"]
            .as_str()
            .ok_or_else(|| missing("CloudConnectorId"))?;
        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let c = state
            .cloud_connectors
            .get(id)
            .ok_or_else(|| not_found(id))?;
        let mut out = json!({
            "CloudConnectorArn": c.arn,
            "DisplayName": c.display_name,
            "RoleArn": c.role_arn,
            "Configuration": c.configuration,
            "ConfigConnectorArn": c.config_connector_arn,
            "CreatedAt": ts(&c.created_at),
            "UpdatedAt": ts(&c.updated_at),
        });
        if let Some(ref d) = c.description {
            out["Description"] = json!(d);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn list_cloud_connectors(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 10)?;
        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let connectors: Vec<Value> = state
            .cloud_connectors
            .values()
            .map(|c| {
                let mut v = json!({
                    "CloudConnectorId": c.id,
                    "DisplayName": c.display_name,
                    "RoleArn": c.role_arn,
                    "CreatedAt": ts(&c.created_at),
                    "UpdatedAt": ts(&c.updated_at),
                });
                if let Some(ref d) = c.description {
                    v["Description"] = json!(d);
                }
                v
            })
            .collect();
        Ok(AwsResponse::ok_json(
            json!({ "CloudConnectors": connectors }),
        ))
    }

    pub(super) fn update_cloud_connector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = body["CloudConnectorId"]
            .as_str()
            .ok_or_else(|| missing("CloudConnectorId"))?
            .to_string();
        if let Some(dn) = body["DisplayName"].as_str() {
            validate_string_length("DisplayName", dn, 1, 256)?;
        }
        if let Some(d) = body["Description"].as_str() {
            validate_string_length("Description", d, 0, 1024)?;
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let c = state
            .cloud_connectors
            .get_mut(&id)
            .ok_or_else(|| not_found(&id))?;
        if let Some(dn) = body["DisplayName"].as_str() {
            c.display_name = dn.to_string();
        }
        if let Some(d) = body["Description"].as_str() {
            c.description = Some(d.to_string());
        }
        if let Some(cfg) = body.get("Configuration").filter(|v| v.is_object()) {
            c.configuration = cfg.clone();
        }
        c.updated_at = Utc::now();

        Ok(AwsResponse::ok_json(json!({ "CloudConnectorId": id })))
    }

    pub(super) fn delete_cloud_connector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = body["CloudConnectorId"]
            .as_str()
            .ok_or_else(|| missing("CloudConnectorId"))?
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.cloud_connectors.remove(&id).is_none() {
            return Err(not_found(&id));
        }
        Ok(AwsResponse::ok_json(json!({ "CloudConnectorId": id })))
    }

    pub(super) fn validate_cloud_connector(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_range_i64("MaxResults", body["MaxResults"].as_i64(), 1, 75)?;
        let id = body["CloudConnectorId"]
            .as_str()
            .ok_or_else(|| missing("CloudConnectorId"))?;
        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let _c = state
            .cloud_connectors
            .get(id)
            .ok_or_else(|| not_found(id))?;
        // A healthy connector reports an informational tenant-summary finding.
        let findings = json!([{
            "Type": "INFO",
            "Code": "TENANT_SUMMARY",
            "Message": "Cloud connector configuration validated successfully",
        }]);
        Ok(AwsResponse::ok_json(
            json!({ "ValidationFindings": findings }),
        ))
    }
}

/// Parse an SSM `TagList` (`[{ "Key": .., "Value": .. }]`) into a map.
fn parse_tags(v: Option<&Value>) -> std::collections::BTreeMap<String, String> {
    let mut m = std::collections::BTreeMap::new();
    if let Some(arr) = v.and_then(|x| x.as_array()) {
        for t in arr {
            if let (Some(k), Some(val)) = (
                t.get("Key").and_then(Value::as_str),
                t.get("Value").and_then(Value::as_str),
            ) {
                m.insert(k.to_string(), val.to_string());
            }
        }
    }
    m
}
