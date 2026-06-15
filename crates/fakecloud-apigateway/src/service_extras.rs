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
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/json".to_string(),
            body: bytes::Bytes::from(body.into_bytes()).into(),
            headers: http::HeaderMap::new(),
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
        Ok(AwsResponse {
            status: StatusCode::OK,
            content_type: "application/octet-stream".to_string(),
            body: bytes::Bytes::from(body.into_bytes()).into(),
            headers: http::HeaderMap::new(),
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
        let arn = params.get("resourceArn").cloned().unwrap_or_default();
        let body = req.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        let entry = state.tags.entry(arn).or_default();
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
        let body = req.json_body();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request_account(req));
        if let Some(entry) = state.tags.get_mut(&arn) {
            if let Some(arr) = body.get("tagKeys").and_then(Value::as_array) {
                for k in arr {
                    if let Some(s) = k.as_str() {
                        entry.remove(s);
                    }
                }
            }
        }
        ok_no_content()
    }

    pub(super) fn get_tags(
        &self,
        req: &AwsRequest,
        params: &BTreeMap<String, String>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = params.get("resourceArn").cloned().unwrap_or_default();
        let accounts = self.state.read();
        let map = accounts
            .get(&request_account(req))
            .and_then(|s| s.tags.get(&arn))
            .cloned()
            .unwrap_or_default();
        ok(json!({"tags": map}))
    }
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
