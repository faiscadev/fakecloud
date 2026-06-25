// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl ApiGatewayService {
    pub(super) fn put_integration(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let res_id = params.get("resourceId").cloned().unwrap_or_default();
        let http_method = params.get("httpMethod").cloned().unwrap_or_default();
        let body = req.json_body();
        let integration_type = body
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("type is required"))?
            .to_string();
        let integration = Integration {
            rest_api_id: api_id.clone(),
            resource_id: res_id.clone(),
            http_method: http_method.to_uppercase(),
            integration_type,
            integration_http_method: body
                .get("httpMethod")
                .and_then(Value::as_str)
                .map(String::from),
            uri: body.get("uri").and_then(Value::as_str).map(String::from),
            credentials: body
                .get("credentials")
                .and_then(Value::as_str)
                .map(String::from),
            request_parameters: extract_string_map(&body, "requestParameters"),
            request_templates: extract_string_map(&body, "requestTemplates"),
            passthrough_behavior: body
                .get("passthroughBehavior")
                .and_then(Value::as_str)
                .unwrap_or("WHEN_NO_MATCH")
                .to_string(),
            timeout_in_millis: body
                .get("timeoutInMillis")
                .and_then(Value::as_i64)
                .map(|v| v as i32),
            cache_namespace: body
                .get("cacheNamespace")
                .and_then(Value::as_str)
                .map(String::from),
            cache_key_parameters: body
                .get("cacheKeyParameters")
                .and_then(Value::as_array)
                .map(|arr| {
                    arr.iter()
                        .filter_map(Value::as_str)
                        .map(String::from)
                        .collect()
                })
                .unwrap_or_default(),
            content_handling: body
                .get("contentHandling")
                .and_then(Value::as_str)
                .map(String::from),
            connection_type: body
                .get("connectionType")
                .and_then(Value::as_str)
                .map(String::from),
            connection_id: body
                .get("connectionId")
                .and_then(Value::as_str)
                .map(String::from),
            tls_config: body.get("tlsConfig").cloned(),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.integrations.insert(
            method_key(&api_id, &res_id, &http_method),
            integration.clone(),
        );
        ok_status(StatusCode::CREATED, integration_to_json(&integration))
    }

    pub(super) fn get_integration(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = method_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
        );
        let accounts = self.state.read();
        let state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found("Integration not found"))?;
        let i = state
            .integrations
            .get(&key)
            .ok_or_else(|| not_found("Integration not found"))?;
        ok(integration_to_json(i))
    }

    pub(super) fn delete_integration(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = method_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.integrations.remove(&key).is_none() {
            return Err(not_found("Integration not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_integration(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = method_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let integration = state
            .integrations
            .get_mut(&key)
            .ok_or_else(|| not_found("Integration not found"))?;
        apply_patch_operations(req, |op, path, value| {
            if op != "replace" && op != "add" && op != "remove" {
                return;
            }
            match path {
                "/uri" => integration.uri = value.as_str().map(String::from),
                "/type" => {
                    if let Some(s) = value.as_str() {
                        integration.integration_type = s.to_string();
                    }
                }
                "/timeoutInMillis" => {
                    integration.timeout_in_millis = value.as_i64().map(|v| v as i32);
                }
                // Previously dropped (bug-audit 2026-06-20, 1.21).
                "/credentials" => integration.credentials = value.as_str().map(String::from),
                "/connectionType" => integration.connection_type = value.as_str().map(String::from),
                "/contentHandling" => {
                    integration.content_handling = value.as_str().map(String::from)
                }
                "/passthroughBehavior" => {
                    if let Some(s) = value.as_str() {
                        integration.passthrough_behavior = s.to_string();
                    }
                }
                _ if path.starts_with("/requestParameters/") => {
                    let k = path.trim_start_matches("/requestParameters/").to_string();
                    if op == "remove" {
                        integration.request_parameters.remove(&k);
                    } else if let Some(v) = value.as_str() {
                        integration.request_parameters.insert(k, v.to_string());
                    }
                }
                _ if path.starts_with("/requestTemplates/") => {
                    let k = path.trim_start_matches("/requestTemplates/").to_string();
                    if op == "remove" {
                        integration.request_templates.remove(&k);
                    } else if let Some(v) = value.as_str() {
                        integration.request_templates.insert(k, v.to_string());
                    }
                }
                // Previously dropped (bug-hunt 2026-06-24, 1.11): VPC-link
                // connection id, cache namespace/keys, and mTLS backend config.
                "/connectionId" => integration.connection_id = value.as_str().map(String::from),
                "/cacheNamespace" => integration.cache_namespace = value.as_str().map(String::from),
                "/tlsConfig/insecureSkipVerification" => {
                    let obj = integration
                        .tls_config
                        .get_or_insert_with(|| serde_json::json!({}));
                    if let Some(m) = obj.as_object_mut() {
                        m.insert("insecureSkipVerification".to_string(), value.clone());
                    }
                }
                "/cacheKeyParameters" if op == "add" => {
                    if let Some(s) = value.as_str() {
                        if !integration.cache_key_parameters.iter().any(|x| x == s) {
                            integration.cache_key_parameters.push(s.to_string());
                        }
                    }
                }
                _ if path.starts_with("/cacheKeyParameters/") && op == "remove" => {
                    let target = path.trim_start_matches("/cacheKeyParameters/");
                    integration.cache_key_parameters.retain(|x| x != target);
                }
                _ => {}
            }
        });
        ok(integration_to_json(integration))
    }

    pub(super) fn put_integration_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = response_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
            params.get("statusCode").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut payload = req.json_body();
        if let Some(o) = payload.as_object_mut() {
            o.insert(
                "statusCode".to_string(),
                json!(params.get("statusCode").cloned().unwrap_or_default()),
            );
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state.integration_responses.insert(key, payload.clone());
        ok_status(StatusCode::CREATED, payload)
    }

    pub(super) fn get_integration_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = response_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
            params.get("statusCode").map(|s| s.as_str()).unwrap_or(""),
        );
        let accounts = self.state.read();
        let state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found("IntegrationResponse not found"))?;
        let v = state
            .integration_responses
            .get(&key)
            .cloned()
            .ok_or_else(|| not_found("IntegrationResponse not found"))?;
        ok(v)
    }

    pub(super) fn delete_integration_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = response_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
            params.get("statusCode").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if state.integration_responses.remove(&key).is_none() {
            return Err(not_found("IntegrationResponse not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_integration_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = response_key(
            params.get("restApiId").map(|s| s.as_str()).unwrap_or(""),
            params.get("resourceId").map(|s| s.as_str()).unwrap_or(""),
            params.get("httpMethod").map(|s| s.as_str()).unwrap_or(""),
            params.get("statusCode").map(|s| s.as_str()).unwrap_or(""),
        );
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let v = state
            .integration_responses
            .get_mut(&key)
            .ok_or_else(|| not_found("IntegrationResponse not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), value.clone());
            }
        });
        ok(v.clone())
    }

    pub(super) async fn test_invoke_method(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        // The TestInvokeMethod operation drives the data plane through
        // an in-process call instead of an HTTP one. We mock the
        // request the way AWS does: read the body's `pathWithQueryString`,
        // build a synthetic AwsRequest, and run it through the data
        // plane handler.
        let body = req.json_body();
        let path_with_query = body
            .get("pathWithQueryString")
            .and_then(Value::as_str)
            .unwrap_or("/")
            .to_string();
        let test_method = params
            .get("httpMethod")
            .cloned()
            .unwrap_or_else(|| "GET".to_string());
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let stage = body
            .get("stageVariables")
            .and_then(Value::as_object)
            .and_then(|m| m.get("stage"))
            .and_then(Value::as_str)
            .unwrap_or("test")
            .to_string();
        let body_str = body
            .get("body")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();

        let started = std::time::Instant::now();
        // The synthetic request must use the method passed in `httpMethod`,
        // not POST (which is the wire method of TestInvokeMethod itself).
        let synthetic_method = test_method.parse::<Method>().unwrap_or(Method::GET);
        let synthetic = AwsRequest {
            service: "apigateway".to_string(),
            action: String::new(),
            method: synthetic_method,
            raw_path: format!("/{stage}{path_with_query}"),
            raw_query: String::new(),
            path_segments: build_path_segments(&stage, &path_with_query),
            query_params: HashMap::new(),
            headers: req.headers.clone(),
            body: bytes::Bytes::from(body_str.into_bytes()),
            body_stream: parking_lot::Mutex::new(None),
            account_id: req.account_id.clone(),
            region: req.region.clone(),
            request_id: req.request_id.clone(),
            is_query_protocol: false,
            access_key_id: req.access_key_id.clone(),
            principal: req.principal.clone(),
        };
        let _ = api_id;
        let response = match crate::data_plane::handle(self, &synthetic).await {
            Ok(r) => r,
            Err(e) => {
                return Ok(AwsResponse::ok_json(json!({
                    "status": e.status().as_u16(),
                    "body": e.to_string(),
                    "log": "TestInvokeMethod failed",
                    "latency": started.elapsed().as_millis() as i64,
                })));
            }
        };
        let body_bytes = match &response.body {
            fakecloud_core::service::ResponseBody::Bytes(b) => b.to_vec(),
            _ => Vec::new(),
        };
        ok(json!({
            "status": response.status.as_u16(),
            "body": String::from_utf8_lossy(&body_bytes).to_string(),
            "headers": serializable_headers(&response.headers),
            "log": "TestInvokeMethod ok",
            "latency": started.elapsed().as_millis() as i64,
        }))
    }

    pub(super) async fn test_invoke_authorizer(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Evaluate the *actual* authorizer rather than returning a canned
        // Allow (bug-audit 2026-06-13, 1.7). For a Lambda authorizer
        // (TOKEN/REQUEST/CUSTOM) this invokes the configured function and
        // reflects its policy + principalId + context; for a Cognito
        // authorizer it verifies the supplied JWT. A misconfigured
        // authorizer now surfaces a 401/403 with the real verdict instead
        // of an unconditional pass.
        let rest_api_id = params.get("restApiId").cloned().unwrap_or_default();
        let authorizer_id = params.get("authorizerId").cloned().unwrap_or_default();
        let body = req.json_body();

        let authorizer = {
            let accounts = self.state.read();
            let state = accounts
                .get(&request_account(req))
                .ok_or_else(|| not_found("Authorizer not found"))?;
            state
                .authorizers
                .get(&rest_api_id)
                .and_then(|m| m.get(&authorizer_id))
                .cloned()
                .ok_or_else(|| not_found("Authorizer not found"))?
        };

        // Build a synthetic request carrying the headers supplied in the
        // TestInvokeAuthorizer body so the shared identity-source and
        // Lambda-event helpers operate on the test's headers, not the
        // wire request's.
        let mut headers = http::HeaderMap::new();
        if let Some(map) = body.get("headers").and_then(Value::as_object) {
            for (k, v) in map {
                if let Some(s) = v.as_str() {
                    if let (Ok(name), Ok(val)) = (
                        http::HeaderName::try_from(k.as_str()),
                        http::HeaderValue::from_str(s),
                    ) {
                        headers.insert(name, val);
                    }
                }
            }
        }
        let synthetic = AwsRequest {
            service: "apigateway".to_string(),
            action: String::new(),
            method: Method::GET,
            raw_path: String::new(),
            raw_query: String::new(),
            path_segments: Vec::new(),
            query_params: HashMap::new(),
            headers,
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: req.account_id.clone(),
            region: req.region.clone(),
            request_id: req.request_id.clone(),
            is_query_protocol: false,
            access_key_id: req.access_key_id.clone(),
            principal: req.principal.clone(),
        };

        let result = crate::data_plane::test_invoke_authorizer_eval(
            self,
            &synthetic,
            &rest_api_id,
            &authorizer,
        )
        .await?;
        ok(result)
    }
}
