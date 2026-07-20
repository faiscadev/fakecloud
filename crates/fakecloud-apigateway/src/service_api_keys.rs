// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl ApiGatewayService {
    pub(super) fn create_api_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = make_id();
        let value = body
            .get("value")
            .and_then(Value::as_str)
            .map(String::from)
            .unwrap_or_else(|| {
                // AWS API keys are 40-char alphanumeric strings.
                uuid::Uuid::new_v4().simple().to_string()
            });
        let now = chrono::Utc::now();
        let key = ApiKey {
            id: id.clone(),
            value,
            name: body
                .get("name")
                .and_then(Value::as_str)
                .map(String::from)
                .unwrap_or_else(|| format!("key-{id}")),
            description: body
                .get("description")
                .and_then(Value::as_str)
                .map(String::from),
            enabled: body.get("enabled").and_then(Value::as_bool).unwrap_or(true),
            created_date: now,
            last_updated_date: now,
            stage_keys: body
                .get("stageKeys")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            tags: tags_from(&body),
            customer_id: body
                .get("customerId")
                .and_then(Value::as_str)
                .map(String::from),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.api_keys.insert(id, key.clone());
        ok_status(StatusCode::CREATED, api_key_to_json(&key, true))
    }

    pub(super) fn get_api_key(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("apiKeyId").cloned().unwrap_or_default();
        let include_value = req
            .query_params
            .get("includeValue")
            .map(|s| s == "true")
            .unwrap_or(false);
        let accounts = self.state.read();
        let k = accounts
            .get(&request_account(req))
            .and_then(|s| s.api_keys.get(&id))
            .cloned()
            .ok_or_else(|| not_found("ApiKey not found"))?;
        ok(api_key_to_json(&k, include_value))
    }

    pub(super) fn get_api_keys(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let include_value = req
            .query_params
            .get("includeValues")
            .map(|s| s == "true")
            .unwrap_or(false);
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .map(|s| {
                s.api_keys
                    .values()
                    .map(|k| api_key_to_json(k, include_value))
                    .collect()
            })
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_api_key(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("apiKeyId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.api_keys.remove(&id).is_none() {
            return Err(not_found("ApiKey not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_api_key(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("apiKeyId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let k = state
            .api_keys
            .get_mut(&id)
            .ok_or_else(|| not_found("ApiKey not found"))?;
        apply_patch_operations(req, |op, path, value| {
            if op != "replace" && op != "add" && op != "remove" {
                return;
            }
            match path {
                "/name" => {
                    if let Some(s) = value.as_str() {
                        k.name = s.to_string();
                    }
                }
                "/description" => k.description = value.as_str().map(String::from),
                "/enabled" => {
                    // PatchOperation.value is a STRING, so clients send
                    // `"false"` to disable a key; accept both forms.
                    if let Some(b) = patch_bool(value) {
                        k.enabled = b;
                    }
                }
                // Previously dropped (bug-hunt 2026-06-24, 1.11).
                "/customerId" => k.customer_id = value.as_str().map(String::from),
                "/stageKeys" if op == "add" => {
                    if let Some(s) = value.as_str() {
                        if !k.stage_keys.iter().any(|x| x == s) {
                            k.stage_keys.push(s.to_string());
                        }
                    }
                }
                _ if path.starts_with("/stageKeys/") && op == "remove" => {
                    let target = path.trim_start_matches("/stageKeys/");
                    k.stage_keys.retain(|x| x != target);
                }
                _ => {}
            }
        });
        k.last_updated_date = chrono::Utc::now();
        ok(api_key_to_json(k, false))
    }
}

#[cfg(test)]
mod patch_value_tests {
    use super::*;
    use crate::ApiGatewayService;

    fn svc() -> ApiGatewayService {
        let state =
            std::sync::Arc::new(
                parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::<
                    crate::state::ApiGatewayState,
                >::new("123456789012", "us-east-1", "")),
            );
        ApiGatewayService::new(state)
    }

    fn req(body: Value) -> AwsRequest {
        AwsRequest {
            service: "apigateway".into(),
            action: String::new(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(resp: &AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[test]
    fn update_api_key_disable_via_string_value() {
        // PatchOperation.value is a STRING, so the CLI/SDK/Terraform send
        // `"false"` to disable a key. Previously read via as_bool() and dropped,
        // leaving the key enabled and still usable.
        let svc = svc();
        let created = body_of(
            &svc.create_api_key(&req(json!({ "name": "k", "enabled": true })))
                .unwrap(),
        );
        let id = created["id"].as_str().unwrap().to_string();
        assert_eq!(created["enabled"], json!(true));
        let params: BTreeMap<String, String> = [("apiKeyId".to_string(), id)].into_iter().collect();
        svc.update_api_key(
            &req(json!({ "patchOperations": [
                { "op": "replace", "path": "/enabled", "value": "false" }
            ] })),
            &params,
        )
        .unwrap();
        let got = body_of(&svc.get_api_key(&req(json!({})), &params).unwrap());
        assert_eq!(got["enabled"], json!(false));
    }
}
