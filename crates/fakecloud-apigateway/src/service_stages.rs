// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl ApiGatewayService {
    pub(super) fn create_stage(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let body = req.json_body();
        let stage_name = body
            .get("stageName")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("stageName is required"))?
            .to_string();
        let deployment_id = body
            .get("deploymentId")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("deploymentId is required"))?
            .to_string();
        let now = chrono::Utc::now();
        let stage = Stage {
            stage_name: stage_name.clone(),
            deployment_id,
            description: body
                .get("description")
                .and_then(Value::as_str)
                .map(String::from),
            cache_cluster_enabled: body
                .get("cacheClusterEnabled")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            cache_cluster_size: body
                .get("cacheClusterSize")
                .and_then(Value::as_str)
                .map(String::from),
            variables: extract_string_map(&body, "variables"),
            method_settings: body
                .get("methodSettings")
                .and_then(Value::as_object)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<BTreeMap<String, Value>>()
                })
                .unwrap_or_default(),
            created_date: now,
            last_updated_date: now,
            tracing_enabled: body
                .get("tracingEnabled")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            web_acl_arn: body
                .get("webAclArn")
                .and_then(Value::as_str)
                .map(String::from),
            canary_settings: body.get("canarySettings").cloned(),
            access_log_settings: body.get("accessLogSettings").cloned(),
            tags: tags_from(&body),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if !state.apis.contains_key(&api_id) {
            return Err(not_found(format!("RestApi {api_id} not found")));
        }
        state
            .stages
            .entry(api_id)
            .or_default()
            .insert(stage_name, stage.clone());
        ok_status(StatusCode::CREATED, stage_to_json(&stage))
    }

    pub(super) fn get_stage(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let name = params.get("stageName").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found("Stage not found"))?;
        let map = state
            .stages
            .get(&api_id)
            .ok_or_else(|| not_found("Stage not found"))?;
        let s = map.get(&name).ok_or_else(|| not_found("Stage not found"))?;
        ok(stage_to_json(s))
    }

    pub(super) fn get_stages(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .and_then(|s| s.stages.get(&api_id))
            .map(|m| m.values().map(stage_to_json).collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_stage(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let name = params.get("stageName").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .stages
            .get_mut(&api_id)
            .ok_or_else(|| not_found("Stage not found"))?;
        if map.remove(&name).is_none() {
            return Err(not_found("Stage not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_stage(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let name = params.get("stageName").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .stages
            .get_mut(&api_id)
            .ok_or_else(|| not_found("Stage not found"))?;
        let s = map
            .get_mut(&name)
            .ok_or_else(|| not_found("Stage not found"))?;
        apply_patch_operations(req, |op, path, value| {
            if op != "replace" && op != "add" && op != "remove" {
                return;
            }
            match path {
                "/deploymentId" => {
                    if let Some(s_) = value.as_str() {
                        s.deployment_id = s_.to_string();
                    }
                }
                "/description" => s.description = value.as_str().map(String::from),
                "/tracingEnabled" => {
                    if let Some(b) = value.as_bool() {
                        s.tracing_enabled = b;
                    }
                }
                // Previously dropped (bug-audit 2026-06-20, 1.21).
                "/cacheClusterEnabled" => {
                    if let Some(b) = value.as_bool() {
                        s.cache_cluster_enabled = b;
                    } else if let Some(v) = value.as_str() {
                        s.cache_cluster_enabled = v == "true";
                    }
                }
                "/cacheClusterSize" => s.cache_cluster_size = value.as_str().map(String::from),
                "/webAclArn" => s.web_acl_arn = value.as_str().map(String::from),
                // Per-method settings: AWS PATCHes them at
                // `/{resourcePath}/{httpMethod}/{setting}` (e.g.
                // `/~1pets/GET/throttling/rateLimit`, or `/*/*/logging/loglevel`
                // for all methods). AWS's GetStage returns `methodSettings` as a
                // map keyed by `"{resourcePath}/{httpMethod}"` whose value is a
                // MethodSetting object (metricsEnabled / loggingLevel /
                // throttlingRateLimit / ...). Storing the raw patch path with the
                // raw string value produced the wrong shape and a perpetual
                // Terraform diff on aws_api_gateway_method_settings.
                _ if path.contains("/throttling/")
                    || path.contains("/logging/")
                    || path.contains("/metrics/")
                    || path.contains("/caching/") =>
                {
                    if let Some((map_key, field)) = method_setting_key_and_field(path) {
                        let entry = s
                            .method_settings
                            .entry(map_key)
                            .or_insert_with(|| serde_json::json!({}));
                        if let Some(m) = entry.as_object_mut() {
                            if op == "remove" {
                                m.remove(&field);
                            } else if let Some(coerced) = coerce_method_setting_value(&field, value)
                            {
                                m.insert(field, coerced);
                            }
                        }
                    }
                }
                _ if path.starts_with("/variables/") => {
                    let k = path.trim_start_matches("/variables/").to_string();
                    if op == "remove" {
                        s.variables.remove(&k);
                    } else if let Some(v) = value.as_str() {
                        s.variables.insert(k, v.to_string());
                    }
                }
                // Access-log + canary settings were dropped: their patch arms
                // fell through, so enabling access logging or a canary
                // deployment silently no-op'd (bug-hunt 2026-06-24, 1.11).
                _ if path.starts_with("/accessLogSettings/") => {
                    let key = path.trim_start_matches("/accessLogSettings/").to_string();
                    let obj = s
                        .access_log_settings
                        .get_or_insert_with(|| serde_json::json!({}));
                    if let Some(m) = obj.as_object_mut() {
                        if op == "remove" {
                            m.remove(&key);
                        } else {
                            m.insert(key, value.clone());
                        }
                    }
                }
                _ if path.starts_with("/canarySettings/") => {
                    let key = path.trim_start_matches("/canarySettings/").to_string();
                    let obj = s
                        .canary_settings
                        .get_or_insert_with(|| serde_json::json!({}));
                    if let Some(m) = obj.as_object_mut() {
                        if op == "remove" {
                            m.remove(&key);
                        } else {
                            m.insert(key, value.clone());
                        }
                    }
                }
                _ => {}
            }
        });
        s.last_updated_date = chrono::Utc::now();
        ok(stage_to_json(s))
    }
}

/// Map a method-settings PATCH setting sub-path to its AWS MethodSetting field
/// name. Returns `None` for unrecognized setting paths.
fn method_setting_field(setting: &str) -> Option<&'static str> {
    Some(match setting {
        "metrics/enabled" => "metricsEnabled",
        "logging/loglevel" => "loggingLevel",
        "logging/dataTrace" => "dataTraceEnabled",
        "throttling/burstLimit" => "throttlingBurstLimit",
        "throttling/rateLimit" => "throttlingRateLimit",
        "caching/enabled" => "cachingEnabled",
        "caching/ttlInSeconds" => "cacheTtlInSeconds",
        "caching/dataEncrypted" => "cacheDataEncrypted",
        "caching/requireAuthorizationForCacheControl" => "requireAuthorizationForCacheControl",
        "caching/unauthorizedCacheControlHeaderStrategy" => {
            "unauthorizedCacheControlHeaderStrategy"
        }
        _ => return None,
    })
}

/// Parse a stage method-settings PATCH path (e.g. `/~1pets/GET/throttling/rateLimit`
/// or `/*/*/logging/loglevel`) into the AWS `methodSettings` map key
/// (`pets/GET`, `*/*`) plus the MethodSetting field name. API Gateway escapes
/// `/` as `~1` and `~` as `~0` in the resource-path segment.
fn method_setting_key_and_field(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim_start_matches('/');
    let mut parts = trimmed.splitn(3, '/');
    let resource_token = parts.next()?;
    let http_method = parts.next()?;
    let setting = parts.next()?;
    let field = method_setting_field(setting)?;
    let resource_path = resource_token.replace("~1", "/").replace("~0", "~");
    let resource_path = resource_path.trim_start_matches('/');
    Some((format!("{resource_path}/{http_method}"), field.to_string()))
}

/// Coerce a PATCH string value into the JSON type AWS reports for the given
/// MethodSetting field. Patch operation values arrive as strings, so numeric
/// and boolean fields must be parsed.
fn coerce_method_setting_value(field: &str, value: &Value) -> Option<Value> {
    match field {
        "metricsEnabled"
        | "dataTraceEnabled"
        | "cachingEnabled"
        | "cacheDataEncrypted"
        | "requireAuthorizationForCacheControl" => {
            let b = value
                .as_bool()
                .or_else(|| value.as_str().and_then(|s| s.parse::<bool>().ok()))?;
            Some(Value::Bool(b))
        }
        "throttlingBurstLimit" | "cacheTtlInSeconds" => {
            let n = value
                .as_i64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))?;
            Some(serde_json::json!(n))
        }
        "throttlingRateLimit" => {
            let n = value
                .as_f64()
                .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))?;
            Some(serde_json::json!(n))
        }
        "loggingLevel" | "unauthorizedCacheControlHeaderStrategy" => {
            value.as_str().map(|s| Value::String(s.to_string()))
        }
        _ => None,
    }
}

#[cfg(test)]
mod patch_tests {
    use super::*;
    use crate::state::Stage;
    use crate::ApiGatewayService;

    fn patch_req(ops: Value) -> AwsRequest {
        AwsRequest {
            service: "apigateway".into(),
            action: "UpdateStage".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_vec(&json!({ "patchOperations": ops })).unwrap(),
            ),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::PATCH,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn update_stage_applies_cache_and_webacl_patches() {
        // cacheClusterEnabled/Size, webAclArn, and per-method settings were
        // dropped (bug-audit 2026-06-20, 1.21).
        let state =
            std::sync::Arc::new(
                parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::<
                    crate::state::ApiGatewayState,
                >::new("123456789012", "us-east-1", "")),
            );
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create("123456789012");
            st.stages.entry("api1".to_string()).or_default().insert(
                "prod".to_string(),
                Stage {
                    stage_name: "prod".into(),
                    deployment_id: "d1".into(),
                    description: None,
                    cache_cluster_enabled: false,
                    cache_cluster_size: None,
                    variables: Default::default(),
                    method_settings: Default::default(),
                    created_date: chrono::Utc::now(),
                    last_updated_date: chrono::Utc::now(),
                    tracing_enabled: false,
                    web_acl_arn: None,
                    canary_settings: None,
                    access_log_settings: None,
                    tags: Default::default(),
                },
            );
        }
        let svc = ApiGatewayService::new(state.clone());
        let params: BTreeMap<String, String> = [
            ("restApiId".to_string(), "api1".to_string()),
            ("stageName".to_string(), "prod".to_string()),
        ]
        .into_iter()
        .collect();

        svc.update_stage(
            &patch_req(json!([
                { "op": "replace", "path": "/cacheClusterEnabled", "value": "true" },
                { "op": "replace", "path": "/cacheClusterSize", "value": "0.5" },
                { "op": "replace", "path": "/webAclArn", "value": "arn:aws:wafv2:::webacl/x" },
                { "op": "replace", "path": "/~1pets/GET/throttling/rateLimit", "value": "10" },
            ])),
            &params,
        )
        .unwrap();

        let accounts = state.read();
        let s = &accounts.get("123456789012").unwrap().stages["api1"]["prod"];
        assert!(s.cache_cluster_enabled);
        assert_eq!(s.cache_cluster_size.as_deref(), Some("0.5"));
        assert_eq!(s.web_acl_arn.as_deref(), Some("arn:aws:wafv2:::webacl/x"));
        // Method settings are keyed by "{resourcePath}/{httpMethod}" with a
        // MethodSetting object value (not the raw patch path / raw string).
        let ms = s.method_settings.get("pets/GET").expect("pets/GET key");
        assert_eq!(ms["throttlingRateLimit"], serde_json::json!(10.0));
    }

    #[test]
    fn update_stage_method_settings_use_aws_shape() {
        let state =
            std::sync::Arc::new(
                parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::<
                    crate::state::ApiGatewayState,
                >::new("123456789012", "us-east-1", "")),
            );
        {
            let mut accounts = state.write();
            let st = accounts.get_or_create("123456789012");
            st.stages.entry("api1".to_string()).or_default().insert(
                "prod".to_string(),
                Stage {
                    stage_name: "prod".into(),
                    deployment_id: "d1".into(),
                    description: None,
                    cache_cluster_enabled: false,
                    cache_cluster_size: None,
                    variables: Default::default(),
                    method_settings: Default::default(),
                    created_date: chrono::Utc::now(),
                    last_updated_date: chrono::Utc::now(),
                    tracing_enabled: false,
                    web_acl_arn: None,
                    canary_settings: None,
                    access_log_settings: None,
                    tags: Default::default(),
                },
            );
        }
        let svc = ApiGatewayService::new(state.clone());
        let params: BTreeMap<String, String> = [
            ("restApiId".to_string(), "api1".to_string()),
            ("stageName".to_string(), "prod".to_string()),
        ]
        .into_iter()
        .collect();

        // Apply a full set of `*/*` (all-methods) settings, the exact shape
        // Terraform's aws_api_gateway_method_settings sends.
        let resp = svc
            .update_stage(
                &patch_req(json!([
                    { "op": "replace", "path": "/*/*/metrics/enabled", "value": "true" },
                    { "op": "replace", "path": "/*/*/logging/loglevel", "value": "INFO" },
                    { "op": "replace", "path": "/*/*/logging/dataTrace", "value": "true" },
                    { "op": "replace", "path": "/*/*/throttling/burstLimit", "value": "5000" },
                    { "op": "replace", "path": "/*/*/throttling/rateLimit", "value": "10000" },
                    { "op": "replace", "path": "/*/*/caching/ttlInSeconds", "value": "300" },
                ])),
                &params,
            )
            .unwrap();

        // GetStage / UpdateStage round-trip the correct MethodSetting shape.
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let ms = &body["methodSettings"]["*/*"];
        assert_eq!(ms["metricsEnabled"], json!(true));
        assert_eq!(ms["loggingLevel"], json!("INFO"));
        assert_eq!(ms["dataTraceEnabled"], json!(true));
        assert_eq!(ms["throttlingBurstLimit"], json!(5000));
        assert_eq!(ms["throttlingRateLimit"], json!(10000.0));
        assert_eq!(ms["cacheTtlInSeconds"], json!(300));

        // A subsequent remove drops just that field, keeping the rest.
        svc.update_stage(
            &patch_req(json!([
                { "op": "remove", "path": "/*/*/logging/loglevel" },
            ])),
            &params,
        )
        .unwrap();
        let accounts = state.read();
        let s = &accounts.get("123456789012").unwrap().stages["api1"]["prod"];
        let ms = s.method_settings.get("*/*").unwrap();
        assert!(ms.get("loggingLevel").is_none());
        assert_eq!(ms["metricsEnabled"], json!(true));
    }
}
