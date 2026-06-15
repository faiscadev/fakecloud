// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl ApiGatewayService {
    pub(super) fn create_usage_plan(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let plan = UsagePlan {
            id: make_id(),
            name: body
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| bad_request("name is required"))?
                .to_string(),
            description: body
                .get("description")
                .and_then(Value::as_str)
                .map(String::from),
            api_stages: body
                .get("apiStages")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default(),
            throttle: body.get("throttle").cloned(),
            quota: body.get("quota").cloned(),
            product_code: body
                .get("productCode")
                .and_then(Value::as_str)
                .map(String::from),
            tags: tags_from(&body),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.usage_plans.insert(plan.id.clone(), plan.clone());
        ok_status(StatusCode::CREATED, usage_plan_to_json(&plan))
    }

    pub(super) fn get_usage_plan(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("usagePlanId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let plan = accounts
            .get(&request_account(req))
            .and_then(|s| s.usage_plans.get(&id))
            .cloned()
            .ok_or_else(|| not_found("UsagePlan not found"))?;
        ok(usage_plan_to_json(&plan))
    }

    pub(super) fn get_usage_plans(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .map(|s| s.usage_plans.values().map(usage_plan_to_json).collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_usage_plan(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("usagePlanId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.usage_plans.remove(&id).is_none() {
            return Err(not_found("UsagePlan not found"));
        }
        state.usage_plan_keys.remove(&id);
        ok_no_content()
    }

    pub(super) fn update_usage_plan(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("usagePlanId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let plan = state
            .usage_plans
            .get_mut(&id)
            .ok_or_else(|| not_found("UsagePlan not found"))?;
        apply_patch_operations(req, |op, path, value| {
            if op != "replace" && op != "add" {
                return;
            }
            match path {
                "/name" => {
                    if let Some(s) = value.as_str() {
                        plan.name = s.to_string();
                    }
                }
                "/description" => plan.description = value.as_str().map(String::from),
                _ => {}
            }
        });
        ok(usage_plan_to_json(plan))
    }

    pub(super) fn create_usage_plan_key(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let plan_id = params.get("usagePlanId").cloned().unwrap_or_default();
        let body = req.json_body();
        let key_id = body
            .get("keyId")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("keyId is required"))?
            .to_string();
        let key_type = body
            .get("keyType")
            .and_then(Value::as_str)
            .unwrap_or("API_KEY")
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let key_value = state
            .api_keys
            .get(&key_id)
            .map(|k| k.value.clone())
            .ok_or_else(|| not_found("ApiKey not found"))?;
        if !state.usage_plans.contains_key(&plan_id) {
            return Err(not_found("UsagePlan not found"));
        }
        let entry = json!({"id": key_id, "type": key_type, "value": key_value});
        state
            .usage_plan_keys
            .entry(plan_id)
            .or_default()
            .insert(key_id, entry.clone());
        ok_status(StatusCode::CREATED, entry)
    }

    pub(super) fn get_usage_plan_key(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let plan_id = params.get("usagePlanId").cloned().unwrap_or_default();
        let key_id = params.get("keyId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let v = accounts
            .get(&request_account(req))
            .and_then(|s| s.usage_plan_keys.get(&plan_id))
            .and_then(|m| m.get(&key_id))
            .cloned()
            .ok_or_else(|| not_found("UsagePlanKey not found"))?;
        ok(v)
    }

    pub(super) fn get_usage_plan_keys(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let plan_id = params.get("usagePlanId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .and_then(|s| s.usage_plan_keys.get(&plan_id))
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_usage_plan_key(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let plan_id = params.get("usagePlanId").cloned().unwrap_or_default();
        let key_id = params.get("keyId").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .usage_plan_keys
            .get_mut(&plan_id)
            .ok_or_else(|| not_found("UsagePlanKey not found"))?;
        if map.remove(&key_id).is_none() {
            return Err(not_found("UsagePlanKey not found"));
        }
        ok_no_content()
    }

    pub(super) fn get_usage(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let plan_id = params.get("usagePlanId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let empty = crate::state::ApiGatewayState::new(&request_account(req), &req.region);
        let state = accounts.get(&request_account(req)).unwrap_or(&empty);

        let plan = state
            .usage_plans
            .get(&plan_id)
            .ok_or_else(|| not_found("Usage plan not found"))?;
        let quota_limit = quota_limit_for(plan);

        // Build `values: { keyId: [[used, remaining]] }` for each key on
        // the plan, falling back to the full quota when a key hasn't been
        // metered yet.
        let mut values = serde_json::Map::new();
        if let Some(keys) = state.usage_plan_keys.get(&plan_id) {
            for key_id in keys.keys() {
                let remaining = state
                    .usage_remaining
                    .get(&plan_id)
                    .and_then(|m| m.get(key_id))
                    .copied()
                    .unwrap_or(quota_limit);
                let used = (quota_limit - remaining).max(0);
                values.insert(key_id.clone(), json!([[used, remaining]]));
            }
        }

        ok(json!({
            "usagePlanId": plan_id,
            "startDate": params.get("startDate").cloned().unwrap_or_else(|| "1970-01-01".to_string()),
            "endDate": params
                .get("endDate")
                .cloned()
                .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string()),
            "values": serde_json::Value::Object(values),
        }))
    }

    /// UpdateUsage applies the supplied JSON-patch operations to the
    /// per-key remaining quota. AWS uses `op=replace` with
    /// `path=/<keyId>` and `value=<remaining>`.
    pub(super) fn update_usage(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let plan_id = params.get("usagePlanId").cloned().unwrap_or_default();
        let body = req.json_body();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if !state.usage_plans.contains_key(&plan_id) {
            return Err(not_found("Usage plan not found"));
        }

        if let Some(ops) = body["patchOperations"].as_array() {
            let entry = state.usage_remaining.entry(plan_id.clone()).or_default();
            for op in ops {
                let path = op["path"].as_str().unwrap_or_default();
                let key_id = path.trim_start_matches('/');
                if key_id.is_empty() {
                    continue;
                }
                // value may arrive as a number or a numeric string.
                let value = op["value"]
                    .as_i64()
                    .or_else(|| op["value"].as_str().and_then(|s| s.parse::<i64>().ok()));
                if let Some(v) = value {
                    entry.insert(key_id.to_string(), v);
                }
            }
        }

        // Echo the resulting usage view back (same shape as GetUsage).
        drop(accounts);
        self.get_usage(req, params)
    }
}

/// Resolve a usage plan's quota limit, defaulting to a large value when
/// the plan has no quota configured.
fn quota_limit_for(plan: &crate::state::UsagePlan) -> i64 {
    plan.quota
        .as_ref()
        .and_then(|q| q["limit"].as_i64())
        .unwrap_or(i64::MAX)
}

#[cfg(test)]
mod usage_tests {
    use super::*;
    use crate::service::ApiGatewayService;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn svc() -> ApiGatewayService {
        let state = Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        ApiGatewayService::new(state)
    }

    fn req(body: Value) -> AwsRequest {
        AwsRequest {
            service: "apigateway".to_string(),
            action: String::new(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query: String::new(),
            path_segments: vec![],
            query_params: HashMap::new(),
            headers: HeaderMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "rid".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    /// 1.24: UpdateUsage applies the patch and GetUsage reflects the
    /// resulting remaining quota; no longer canned.
    #[test]
    fn usage_update_and_get_round_trip() {
        let s = svc();
        // Create a plan with a quota limit of 1000.
        let resp = s
            .create_usage_plan(&req(
                json!({"name": "p", "quota": {"limit": 1000, "period": "DAY"}}),
            ))
            .unwrap();
        let pb: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let plan_id = pb["id"].as_str().unwrap().to_string();

        // Attach a key directly to the plan's key map.
        {
            let mut accts = s.state.write();
            let st = accts.get_or_create("123456789012");
            st.usage_plan_keys
                .entry(plan_id.clone())
                .or_default()
                .insert("key-1".to_string(), json!({"id": "key-1"}));
        }

        let params: BTreeMap<String, String> =
            BTreeMap::from([("usagePlanId".to_string(), plan_id.clone())]);

        // Initially remaining == limit.
        let resp = s.get_usage(&req(json!({})), &params).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["values"]["key-1"][0][1].as_i64(), Some(1000));
        assert_eq!(body["values"]["key-1"][0][0].as_i64(), Some(0));

        // UpdateUsage sets remaining to 250.
        let upd_body = json!({"patchOperations": [
            {"op": "replace", "path": "/key-1", "value": 250}
        ]});
        let resp = s.update_usage(&req(upd_body), &params).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["values"]["key-1"][0][1].as_i64(), Some(250));
        // used = limit - remaining
        assert_eq!(body["values"]["key-1"][0][0].as_i64(), Some(750));

        // GetUsage continues to reflect the persisted value.
        let resp = s.get_usage(&req(json!({})), &params).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["values"]["key-1"][0][1].as_i64(), Some(250));
    }

    /// 1.28: GetSdkType returns a real descriptor for a known id rather
    /// than the literal "Stub".
    #[test]
    fn get_sdk_type_returns_real_descriptor() {
        let s = svc();
        let params: BTreeMap<String, String> =
            BTreeMap::from([("id".to_string(), "java".to_string())]);
        let resp = s.get_sdk_type(&params).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["id"].as_str(), Some("java"));
        assert_eq!(body["friendlyName"].as_str(), Some("Java"));
        assert_ne!(body["friendlyName"].as_str(), Some("Stub"));

        // Unknown id -> NotFound.
        let params: BTreeMap<String, String> =
            BTreeMap::from([("id".to_string(), "cobol".to_string())]);
        assert!(s.get_sdk_type(&params).is_err());
    }
}
