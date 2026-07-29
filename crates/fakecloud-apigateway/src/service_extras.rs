// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl ApiGatewayService {
    pub(super) fn create_doc_part(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = make_id();
        let mut value = req.json_body();
        if let Some(o) = value.as_object_mut() {
            o.insert("id".to_string(), Value::String(id.clone()));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state
            .documentation_parts
            .entry(api_id)
            .or_default()
            .insert(id, value.clone());
        ok_status(StatusCode::CREATED, value)
    }

    pub(super) fn get_doc_part(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params
            .get("documentationPartId")
            .cloned()
            .unwrap_or_default();
        let accounts = self.state.read();
        let v = accounts
            .get(&request_account(req))
            .and_then(|s| s.documentation_parts.get(&api_id))
            .and_then(|m| m.get(&id))
            .cloned()
            .ok_or_else(|| not_found("DocumentationPart not found"))?;
        ok(v)
    }

    pub(super) fn get_doc_parts(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .and_then(|s| s.documentation_parts.get(&api_id))
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_doc_part(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params
            .get("documentationPartId")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .documentation_parts
            .get_mut(&api_id)
            .ok_or_else(|| not_found("DocumentationPart not found"))?;
        if map.remove(&id).is_none() {
            return Err(not_found("DocumentationPart not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_doc_part(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let id = params
            .get("documentationPartId")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .documentation_parts
            .get_mut(&api_id)
            .ok_or_else(|| not_found("DocumentationPart not found"))?;
        let v = map
            .get_mut(&id)
            .ok_or_else(|| not_found("DocumentationPart not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), value.clone());
            }
        });
        ok(v.clone())
    }

    pub(super) fn create_doc_version(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let body = req.json_body();
        let version = body
            .get("documentationVersion")
            .and_then(Value::as_str)
            .ok_or_else(|| bad_request("documentationVersion is required"))?
            .to_string();
        // DocumentationVersion output shape: { version, createdDate, description }.
        // Input has documentationVersion (-> version) + stageName (input-only)
        // + description; remap so list/get/create responses validate.
        let mut value = serde_json::Map::new();
        value.insert("version".to_string(), Value::String(version.clone()));
        if let Some(desc) = body.get("description").and_then(Value::as_str) {
            value.insert("description".to_string(), Value::String(desc.to_string()));
        }
        value.insert(
            "createdDate".to_string(),
            Value::Number(serde_json::Number::from(chrono::Utc::now().timestamp())),
        );
        let value = Value::Object(value);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state
            .documentation_versions
            .entry(api_id)
            .or_default()
            .insert(version, value.clone());
        ok_status(StatusCode::CREATED, value)
    }

    pub(super) fn get_doc_version(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let v = params
            .get("documentationVersion")
            .cloned()
            .unwrap_or_default();
        let accounts = self.state.read();
        let value = accounts
            .get(&request_account(req))
            .and_then(|s| s.documentation_versions.get(&api_id))
            .and_then(|m| m.get(&v))
            .cloned()
            .ok_or_else(|| not_found("DocumentationVersion not found"))?;
        ok(value)
    }

    pub(super) fn get_doc_versions(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .and_then(|s| s.documentation_versions.get(&api_id))
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_doc_version(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let v = params
            .get("documentationVersion")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .documentation_versions
            .get_mut(&api_id)
            .ok_or_else(|| not_found("DocumentationVersion not found"))?;
        if map.remove(&v).is_none() {
            return Err(not_found("DocumentationVersion not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_doc_version(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let v = params
            .get("documentationVersion")
            .cloned()
            .unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .documentation_versions
            .get_mut(&api_id)
            .ok_or_else(|| not_found("DocumentationVersion not found"))?;
        let value = map
            .get_mut(&v)
            .ok_or_else(|| not_found("DocumentationVersion not found"))?;
        apply_patch_operations(req, |_op, path, val| {
            if let Some(o) = value.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), val.clone());
            }
        });
        ok(value.clone())
    }

    pub(super) fn put_gateway_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let response_type = params.get("responseType").cloned().unwrap_or_default();
        let mut value = req.json_body();
        if let Some(o) = value.as_object_mut() {
            o.insert(
                "responseType".to_string(),
                Value::String(response_type.clone()),
            );
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        state
            .gateway_responses
            .entry(api_id)
            .or_default()
            .insert(response_type, value.clone());
        ok_status(StatusCode::CREATED, value)
    }

    pub(super) fn get_gateway_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let t = params.get("responseType").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let v = accounts
            .get(&request_account(req))
            .and_then(|s| s.gateway_responses.get(&api_id))
            .and_then(|m| m.get(&t))
            .cloned()
            .ok_or_else(|| not_found("GatewayResponse not found"))?;
        ok(v)
    }

    pub(super) fn get_gateway_responses(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let items: Vec<Value> = accounts
            .get(&request_account(req))
            .and_then(|s| s.gateway_responses.get(&api_id))
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        ok(json!({"item": items}))
    }

    pub(super) fn delete_gateway_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let t = params.get("responseType").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .gateway_responses
            .get_mut(&api_id)
            .ok_or_else(|| not_found("GatewayResponse not found"))?;
        if map.remove(&t).is_none() {
            return Err(not_found("GatewayResponse not found"));
        }
        ok_no_content()
    }

    pub(super) fn update_gateway_response(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let t = params.get("responseType").cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let map = state
            .gateway_responses
            .get_mut(&api_id)
            .ok_or_else(|| not_found("GatewayResponse not found"))?;
        let v = map
            .get_mut(&t)
            .ok_or_else(|| not_found("GatewayResponse not found"))?;
        apply_patch_operations(req, |_op, path, value| {
            if let Some(o) = v.as_object_mut() {
                o.insert(path.trim_start_matches('/').to_string(), value.clone());
            }
        });
        ok(v.clone())
    }

    pub(super) fn get_export(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api_id = params.get("restApiId").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let account_state = accounts
            .get(&request_account(req))
            .ok_or_else(|| not_found("RestApi not found"))?;
        let api = account_state
            .apis
            .get(&api_id)
            .cloned()
            .ok_or_else(|| not_found("RestApi not found"))?;
        // Round-trip the imported source verbatim if the API was created
        // via `ImportRestApi`; otherwise build OpenAPI 3.0 from the
        // current resources + methods.
        let body = if let Some(src) = api.import_source.clone() {
            src
        } else {
            let mut paths = serde_json::Map::new();
            if let Some(api_resources) = account_state.resources.get(&api_id) {
                for resource in api_resources.values() {
                    let mut path_item = serde_json::Map::new();
                    for method in account_state
                        .methods
                        .values()
                        .filter(|m| m.rest_api_id == api_id && m.resource_id == resource.id)
                    {
                        if method.http_method.eq_ignore_ascii_case("ANY") {
                            for verb in ["get", "post", "put", "delete", "patch", "head", "options"]
                            {
                                path_item.insert(verb.to_string(), method_to_openapi_op(method));
                            }
                        } else {
                            path_item.insert(
                                method.http_method.to_lowercase(),
                                method_to_openapi_op(method),
                            );
                        }
                    }
                    if !path_item.is_empty() {
                        paths.insert(resource.path.clone(), serde_json::Value::Object(path_item));
                    }
                }
            }
            json!({
                "openapi": "3.0.1",
                "info": {
                    "title": api.name,
                    "version": api.version.unwrap_or_default(),
                },
                "paths": serde_json::Value::Object(paths),
            })
            .to_string()
        };
        // AWS returns the export as a downloadable attachment; the
        // Content-Disposition header is surfaced by the `aws_api_gateway_export`
        // data source as `content_disposition`.
        let export_type = params
            .get("exportType")
            .cloned()
            .unwrap_or_else(|| "oas30".to_string());
        let mut headers = http::HeaderMap::new();
        if let Ok(v) = http::HeaderValue::from_str(&format!(
            "attachment; filename=\"{api_id}-{export_type}.json\""
        )) {
            headers.insert(http::header::CONTENT_DISPOSITION, v);
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/json".to_string(),
            body: bytes::Bytes::from(body.into_bytes()).into(),
            headers,
        })
    }

    pub(super) fn get_sdk(
        &self,
        _req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let sdk_type = params.get("sdkType").cloned().unwrap_or_default();
        // AWS returns a binary blob (a zip archive) for GetSdk. fakecloud
        // returns a deterministic dummy zip header — enough for SDK
        // tests that just want to verify the endpoint exists.
        let body = format!("PK\x03\x04fakecloud-{sdk_type}-stub-zip\x00\x00\x00",);
        // The `aws_api_gateway_sdk` data source reads `content_disposition` from
        // the attachment header AWS returns.
        let mut headers = http::HeaderMap::new();
        if let Ok(v) =
            http::HeaderValue::from_str(&format!("attachment; filename=\"{sdk_type}.zip\""))
        {
            headers.insert(http::header::CONTENT_DISPOSITION, v);
        }
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/octet-stream".to_string(),
            body: bytes::Bytes::from(body.into_bytes()).into(),
            headers,
        })
    }

    /// GetSdkType: return the descriptor for a known SDK type id instead
    /// of a literal "Stub" friendlyName.
    pub(super) fn get_sdk_type(
        &self,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = params.get("id").cloned().unwrap_or_default();
        sdk_types()
            .into_iter()
            .find(|t| t["id"].as_str() == Some(id.as_str()))
            .map(ok)
            .unwrap_or_else(|| Err(not_found("SDK type not found")))
    }

    pub(super) fn tag_resource(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = decode_resource_arn(&params.get("resourceArn").cloned().unwrap_or_default());
        let body = req.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        // A REST API's tags live on the RestApi itself (the same store
        // CreateRestApi writes and GetRestApi reads); route there so a tag set
        // via TagResource is visible to GetRestApi and vice-versa. Other
        // taggable resources fall back to the generic ARN-keyed tag map.
        let entry = match rest_api_id_from_arn(&arn).and_then(|id| state.apis.get_mut(&id)) {
            Some(api) => &mut api.tags,
            None => state.tags.entry(arn).or_default(),
        };
        if let Some(map) = body.get("tags").and_then(Value::as_object) {
            for (k, v) in map {
                if let Some(s) = v.as_str() {
                    entry.insert(k.clone(), s.to_string());
                }
            }
        }
        ok_no_content()
    }

    pub(super) fn untag_resource(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = params.get("resourceArn").cloned().unwrap_or_default();
        // `tagKeys` is an `@httpQuery` list: real SDK / terraform clients send
        // it as repeated `?tagKeys=a&tagKeys=b` pairs. `query_params` collapses
        // repeats to the last value, so parse every occurrence out of the raw
        // query string, percent-decoding each. Fall back to a JSON body for
        // clients that (incorrectly) send the keys there.
        let mut keys: Vec<String> = req
            .raw_query
            .split('&')
            .filter_map(|pair| pair.strip_prefix("tagKeys="))
            .map(|v| {
                percent_encoding::percent_decode_str(v)
                    .decode_utf8_lossy()
                    .into_owned()
            })
            .collect();
        if keys.is_empty() {
            if let Some(arr) = req.json_body().get("tagKeys").and_then(Value::as_array) {
                keys = arr
                    .iter()
                    .filter_map(|k| k.as_str().map(str::to_string))
                    .collect();
            }
        }
        let arn = decode_resource_arn(&arn);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let entry = match rest_api_id_from_arn(&arn).and_then(|id| state.apis.get_mut(&id)) {
            Some(api) => Some(&mut api.tags),
            None => state.tags.get_mut(&arn),
        };
        if let Some(entry) = entry {
            for k in &keys {
                entry.remove(k);
            }
        }
        ok_no_content()
    }

    pub(super) fn get_tags(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = decode_resource_arn(&params.get("resourceArn").cloned().unwrap_or_default());
        let accounts = self.state.read();
        let map = accounts
            .get(&request_account(req))
            .and_then(
                |s| match rest_api_id_from_arn(&arn).and_then(|id| s.apis.get(&id)) {
                    Some(api) => Some(api.tags.clone()),
                    None => s.tags.get(&arn).cloned(),
                },
            )
            .unwrap_or_default();
        ok(json!({"tags": map}))
    }
}

/// Percent-decode the greedy `resourceArn` path suffix. The core dispatcher
/// captures `/tags/{arn+}` as raw path segments and re-joins them without
/// decoding, so the SDK's percent-encoded `:` (`%3A`) and `/` (`%2F`) arrive
/// literal; decode them back to a real ARN before matching.
fn decode_resource_arn(raw: &str) -> String {
    percent_encoding::percent_decode_str(raw)
        .decode_utf8_lossy()
        .into_owned()
}

/// When `arn` names a REST API itself (`arn:aws:apigateway:{region}::/restapis/{id}`
/// with no trailing sub-resource), return its id. Returns None for stage /
/// other sub-resource ARNs, which keep using the generic tag map.
fn rest_api_id_from_arn(arn: &str) -> Option<String> {
    let tail = arn.split("::/").nth(1)?;
    let mut parts = tail.split('/');
    if parts.next()? != "restapis" {
        return None;
    }
    let id = parts.next()?;
    if id.is_empty() || parts.next().is_some() {
        return None;
    }
    Some(id.to_string())
}

/// Build an OpenAPI 3.0 operation object from the stored Method.
fn method_to_openapi_op(method: &crate::state::Method) -> Value {
    let mut op = serde_json::Map::new();
    if let Some(name) = &method.operation_name {
        op.insert("operationId".to_string(), json!(name));
    }
    let mut params = Vec::new();
    for (key, required) in &method.request_parameters {
        // AWS request parameter keys look like
        // `method.request.querystring.foo`, `.header.foo`, `.path.foo`.
        let parts: Vec<&str> = key.split('.').collect();
        if parts.len() < 4 || parts[0] != "method" || parts[1] != "request" {
            continue;
        }
        let r#in = match parts[2] {
            "querystring" => "query",
            "header" => "header",
            "path" => "path",
            _ => continue,
        };
        let name = parts[3..].join(".");
        params.push(json!({
            "name": name,
            "in": r#in,
            "required": *required || r#in == "path",
            "schema": {"type": "string"},
        }));
    }
    if !params.is_empty() {
        op.insert("parameters".to_string(), json!(params));
    }
    if !method.request_models.is_empty() {
        let mut content = serde_json::Map::new();
        for (ct, model) in &method.request_models {
            content.insert(
                ct.clone(),
                json!({"schema": {"$ref": format!("#/components/schemas/{model}")}}),
            );
        }
        op.insert(
            "requestBody".to_string(),
            json!({"required": true, "content": content}),
        );
    }
    op.insert(
        "responses".to_string(),
        json!({"200": {"description": "OK"}}),
    );
    Value::Object(op)
}

#[cfg(test)]
mod tag_store_tests {
    use super::*;
    use fakecloud_core::service::ResponseBody;
    use std::collections::HashMap;

    fn svc() -> ApiGatewayService {
        let state =
            std::sync::Arc::new(
                parking_lot::RwLock::new(fakecloud_core::multi_account::MultiAccountState::<
                    crate::state::ApiGatewayState,
                >::new("123456789012", "us-east-1", "")),
            );
        ApiGatewayService::new(state)
    }

    fn req_body(body: Value) -> AwsRequest {
        AwsRequest {
            service: "apigateway".into(),
            action: "x".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
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

    fn body_json(resp: &AwsResponse) -> Value {
        match &resp.body {
            ResponseBody::Bytes(b) => serde_json::from_slice(b).unwrap(),
            _ => panic!("expected bytes body"),
        }
    }

    fn params(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn rest_api_id_from_arn_parses_only_the_api_itself() {
        assert_eq!(
            rest_api_id_from_arn("arn:aws:apigateway:us-east-1::/restapis/abc123").as_deref(),
            Some("abc123")
        );
        // sub-resource ARN (a stage) -> not the api itself.
        assert_eq!(
            rest_api_id_from_arn("arn:aws:apigateway:us-east-1::/restapis/abc123/stages/prod"),
            None
        );
    }

    #[test]
    fn tag_resource_and_get_rest_api_share_one_store() {
        // bug-audit 2026-07-29 (cycle 8) CO-1: TagResource/GetTags used a
        // separate ARN-keyed map from CreateRestApi's api.tags, so a tag added
        // via TagResource never appeared in GetRestApi (terraform tag drift).
        let s = svc();
        let created = body_json(
            &s.create_rest_api(&req_body(json!({"name": "api", "tags": {"env": "prod"}})))
                .unwrap(),
        );
        let id = created["id"].as_str().unwrap().to_string();
        // resourceArn arrives percent-encoded (as the dispatcher hands it over).
        let arn_enc = format!("arn%3Aaws%3Aapigateway%3Aus-east-1%3A%3A%2Frestapis%2F{id}");

        // Add a tag via the generic TagResource path.
        s.tag_resource(
            &req_body(json!({"tags": {"team": "core"}})),
            &params(&[("resourceArn", &arn_enc)]),
        )
        .unwrap();

        // GetRestApi now reflects BOTH the create-time and TagResource tag.
        let api = body_json(
            &s.get_rest_api(&req_body(json!({})), &params(&[("restApiId", &id)]))
                .unwrap(),
        );
        assert_eq!(api["tags"]["env"], json!("prod"));
        assert_eq!(api["tags"]["team"], json!("core"));

        // GetTags returns the same unified set (incl. the create-time tag).
        let tags = body_json(
            &s.get_tags(&req_body(json!({})), &params(&[("resourceArn", &arn_enc)]))
                .unwrap(),
        );
        assert_eq!(tags["tags"]["env"], json!("prod"));
        assert_eq!(tags["tags"]["team"], json!("core"));

        // UntagResource removes from the same store.
        s.untag_resource(
            &AwsRequest {
                raw_query: "tagKeys=team".into(),
                ..req_body(json!({}))
            },
            &params(&[("resourceArn", &arn_enc)]),
        )
        .unwrap();
        let api2 = body_json(
            &s.get_rest_api(&req_body(json!({})), &params(&[("restApiId", &id)]))
                .unwrap(),
        );
        assert!(api2["tags"].get("team").is_none(), "{api2}");
        assert_eq!(api2["tags"]["env"], json!("prod"));
    }
}
