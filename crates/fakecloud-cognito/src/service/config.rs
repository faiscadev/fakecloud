use chrono::Utc;
use http::StatusCode;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{require_str, CognitoService};

impl CognitoService {
    // ── UI Customization ──────────────────────────────────────────────

    pub(super) fn get_ui_customization(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = body["ClientId"].as_str().unwrap_or("");

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let key = format!("{pool_id}:{client_id}");

        // Try client-specific first, then pool-level, then empty default
        let customization = state
            .ui_customizations
            .get(&key)
            .or_else(|| {
                if !client_id.is_empty() {
                    state.ui_customizations.get(&format!("{pool_id}:"))
                } else {
                    None
                }
            })
            .cloned()
            .unwrap_or_else(|| {
                json!({
                    "UserPoolId": pool_id,
                    "ClientId": if client_id.is_empty() { "ALL" } else { client_id },
                    "CreationDate": Utc::now().timestamp() as f64,
                    "LastModifiedDate": Utc::now().timestamp() as f64,
                })
            });

        Ok(AwsResponse::ok_json(json!({
            "UICustomization": customization
        })))
    }

    pub(super) fn set_ui_customization(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = body["ClientId"].as_str().unwrap_or("");
        let css = body["CSS"].as_str().unwrap_or("");
        let image_file = body["ImageFile"].as_str();

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let now = Utc::now();
        let key = format!("{pool_id}:{client_id}");

        let mut customization = json!({
            "UserPoolId": pool_id,
            "ClientId": if client_id.is_empty() { "ALL" } else { client_id },
            "CreationDate": now.timestamp() as f64,
            "LastModifiedDate": now.timestamp() as f64,
        });

        if !css.is_empty() {
            customization["CSS"] = json!(css);
            customization["CSSVersion"] = json!("20190128");
        }

        if let Some(img) = image_file {
            customization["ImageUrl"] = json!(format!(
                "https://fakecloud-ui.s3.amazonaws.com/{pool_id}/logo.png"
            ));
            customization["ImageFile"] = json!(img);
        }

        state.ui_customizations.insert(key, customization.clone());

        Ok(AwsResponse::ok_json(json!({
            "UICustomization": customization
        })))
    }

    // ── Log Delivery Configuration ────────────────────────────────────

    pub(super) fn get_log_delivery_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let config = state
            .log_delivery_configs
            .get(pool_id)
            .cloned()
            .unwrap_or_else(|| {
                json!({
                    "UserPoolId": pool_id,
                    "LogConfigurations": []
                })
            });

        Ok(AwsResponse::ok_json(json!({
            "LogDeliveryConfiguration": config
        })))
    }

    pub(super) fn set_log_delivery_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let log_configs = body["LogConfigurations"].clone();

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let config = json!({
            "UserPoolId": pool_id,
            "LogConfigurations": log_configs
        });

        state
            .log_delivery_configs
            .insert(pool_id.to_string(), config.clone());

        Ok(AwsResponse::ok_json(json!({
            "LogDeliveryConfiguration": config
        })))
    }

    // ── Risk Configuration ────────────────────────────────────────────

    pub(super) fn describe_risk_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = body["ClientId"].as_str().unwrap_or("");

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let key = format!("{pool_id}:{client_id}");

        let config = state
            .risk_configurations
            .get(&key)
            .or_else(|| {
                if !client_id.is_empty() {
                    state.risk_configurations.get(&format!("{pool_id}:"))
                } else {
                    None
                }
            })
            .cloned()
            .unwrap_or_else(|| {
                let mut cfg = json!({
                    "UserPoolId": pool_id,
                });
                if !client_id.is_empty() {
                    cfg["ClientId"] = json!(client_id);
                }
                cfg
            });

        Ok(AwsResponse::ok_json(json!({
            "RiskConfiguration": config
        })))
    }

    pub(super) fn set_risk_configuration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = body["ClientId"].as_str().unwrap_or("");

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let key = format!("{pool_id}:{client_id}");

        let mut config = json!({
            "UserPoolId": pool_id,
        });

        if !client_id.is_empty() {
            config["ClientId"] = json!(client_id);
        }

        // Copy through the risk config fields
        if !body["CompromisedCredentialsRiskConfiguration"].is_null() {
            config["CompromisedCredentialsRiskConfiguration"] =
                body["CompromisedCredentialsRiskConfiguration"].clone();
        }
        if !body["AccountTakeoverRiskConfiguration"].is_null() {
            config["AccountTakeoverRiskConfiguration"] =
                body["AccountTakeoverRiskConfiguration"].clone();
        }
        if !body["RiskExceptionConfiguration"].is_null() {
            config["RiskExceptionConfiguration"] = body["RiskExceptionConfiguration"].clone();
        }

        state.risk_configurations.insert(key, config.clone());

        Ok(AwsResponse::ok_json(json!({
            "RiskConfiguration": config
        })))
    }
}
