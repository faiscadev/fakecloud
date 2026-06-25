// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl ApiGatewayService {
    pub(super) fn create_request_validator(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let body = req.json_body();
        let id = make_id();
        let mut value = body.clone();
        if let Some(o) = value.as_object_mut() {
            o.insert("id".to_string(), Value::String(id.clone()));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state
            .request_validators
            .entry(api_id)
            .or_default()
            .insert(id, value.clone());
        ok_status(StatusCode::CREATED, value)
    }

    pub(super) fn get_request_validator(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params
            .get("requestValidatorId")
            .cloned()
            .unwrap_or_default();
        let accounts = self.state.read();
        let v = accounts
            .get(&request_account(req))
            .and_then(|s| s.request_validators.get(&api_id))
            .and_then(|m| m.get(&id))
            .cloned()
            .ok_or_else(|| not_found("RequestValidator not found"))?;
        ok(v)
    }

    pub(super) fn get_request_validators(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .and_then(|s| s.request_validators.get(&api_id))
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_request_validator(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params
            .get("requestValidatorId")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .request_validators
            .get_mut(&api_id)
            .ok_or_else(|| not_found("RequestValidator not found"))?;
        if map.remove(&id).is_none() {
            return Err(not_found("RequestValidator not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_request_validator(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params
            .get("requestValidatorId")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .request_validators
            .get_mut(&api_id)
            .ok_or_else(|| not_found("RequestValidator not found"))?;
        let v = map
            .get_mut(&id)
            .ok_or_else(|| not_found("RequestValidator not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                let key = path.trim_start_matches('/').to_string();
                // JSON Patch sends scalar values as strings; the boolean
                // validator flags must be stored as real booleans so
                // GetRequestValidator returns the type the SDK expects
                // ("expected Boolean ... got string instead").
                let coerced = if matches!(
                    key.as_str(),
                    "validateRequestBody" | "validateRequestParameters"
                ) {
                    match value.as_str() {
                        Some("true") => serde_json::Value::Bool(true),
                        Some("false") => serde_json::Value::Bool(false),
                        _ => value.clone(),
                    }
                } else {
                    value.clone()
                };
                o.insert(key, coerced);
            }
        });
        ok(v.clone())
    }
}
