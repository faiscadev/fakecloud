use chrono::Utc;
use http::StatusCode;
use serde_json::json;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{require_str, CognitoService};

impl CognitoService {
    // ── UI Customization ───────────────────────────────────────────────

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
        let ui = state
            .ui_customizations
            .get(&key)
            .cloned()
            .unwrap_or_else(|| {
                json!({
                    "UserPoolId": pool_id,
                    "CreationDate": Utc::now().timestamp() as f64,
                    "LastModifiedDate": Utc::now().timestamp() as f64,
                })
            });

        Ok(AwsResponse::ok_json(json!({
            "UICustomization": ui
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
        let _image_file = body["ImageFile"].as_str(); // base64 blob, store but don't process

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let now = Utc::now();
        let mut ui = json!({
            "UserPoolId": pool_id,
            "CSS": css,
            "CSSVersion": "20190128172214",
            "CreationDate": now.timestamp() as f64,
            "LastModifiedDate": now.timestamp() as f64,
        });
        if !client_id.is_empty() {
            ui["ClientId"] = json!(client_id);
        }

        let key = format!("{pool_id}:{client_id}");
        state.ui_customizations.insert(key, ui.clone());

        Ok(AwsResponse::ok_json(json!({
            "UICustomization": ui
        })))
    }

    // ── Log Delivery Configuration ─────────────────────────────────────

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
                    "LogConfigurations": [],
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
            "LogConfigurations": log_configs,
        });

        state
            .log_delivery_configs
            .insert(pool_id.to_string(), config.clone());

        Ok(AwsResponse::ok_json(json!({
            "LogDeliveryConfiguration": config
        })))
    }

    // ── Risk Configuration ─────────────────────────────────────────────

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
            .cloned()
            .unwrap_or_else(|| {
                let mut c = json!({
                    "UserPoolId": pool_id,
                });
                if !client_id.is_empty() {
                    c["ClientId"] = json!(client_id);
                }
                c
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

        let mut config = json!({
            "UserPoolId": pool_id,
        });
        if !client_id.is_empty() {
            config["ClientId"] = json!(client_id);
        }
        if body["CompromisedCredentialsRiskConfiguration"].is_object() {
            config["CompromisedCredentialsRiskConfiguration"] =
                body["CompromisedCredentialsRiskConfiguration"].clone();
        }
        if body["AccountTakeoverRiskConfiguration"].is_object() {
            config["AccountTakeoverRiskConfiguration"] =
                body["AccountTakeoverRiskConfiguration"].clone();
        }
        if body["RiskExceptionConfiguration"].is_object() {
            config["RiskExceptionConfiguration"] = body["RiskExceptionConfiguration"].clone();
        }

        let key = format!("{pool_id}:{client_id}");
        state.risk_configurations.insert(key, config.clone());

        Ok(AwsResponse::ok_json(json!({
            "RiskConfiguration": config
        })))
    }
}
