//! API Gateway v2 handlers added to close the conformance gap. Domain
//! names + API mappings, models + integration/route responses, routing
//! rules, VPC links, tagging, portals + portal products, and import /
//! export / settings cleanup operations.

use http::StatusCode;
use serde_json::{json, Value};
use std::collections::BTreeMap;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{generate_id, ApiGatewayV2Service};
use crate::state::{ApiGatewayV2State, HttpApi, Integration, Route};

/// Lowercase the first letter of a key — Smithy's `@jsonName` default for
/// apigatewayv2 shapes (e.g. `ApiId` -> `apiId`).
fn lower_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        Some(c) => c.to_ascii_lowercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

/// Walk a `Value` and lowercase the first character of every object key
/// (recursive). Handlers emit fields in Pascal-case for legibility but
/// the apigatewayv2 Smithy model serializes them as camel-case.
fn to_camel(v: Value) -> Value {
    match v {
        Value::Object(map) => {
            let mut out = serde_json::Map::new();
            for (k, val) in map {
                out.insert(lower_first(&k), to_camel(val));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(to_camel).collect()),
        other => other,
    }
}

/// Parse the request body as JSON. Keep incoming case-as-is; handlers
/// read fields in Pascal-case for legibility, but SDK clients send
/// camel-case per Smithy — so we also merge a Pascal-case copy.
/// Clone the caller's `DomainNameConfigurations` (defaulting to one empty
/// config) and stamp each entry with the synchronous-provisioning fields AWS
/// fills in: `DomainNameStatus = AVAILABLE`, plus a regional endpoint name and
/// hosted-zone id. The Terraform provider's create-waiter reads
/// `DomainNameStatus`, so it must be present.
fn domain_configs_with_status(configs: Option<&Value>, domain: &str, region: &str) -> Value {
    let mut arr = match configs.and_then(|c| c.as_array()) {
        Some(a) if !a.is_empty() => a.clone(),
        _ => vec![json!({})],
    };
    for c in arr.iter_mut() {
        if let Some(obj) = c.as_object_mut() {
            if !obj.contains_key("DomainNameStatus") {
                obj.insert("DomainNameStatus".into(), json!("AVAILABLE"));
            }
            if !obj.contains_key("ApiGatewayDomainName") {
                obj.insert(
                    "ApiGatewayDomainName".into(),
                    json!(format!("d-{domain}.execute-api.{region}.amazonaws.com")),
                );
            }
            if !obj.contains_key("HostedZoneId") {
                obj.insert("HostedZoneId".into(), json!("Z1UJRXOUMOOFQ8"));
            }
            if !obj.contains_key("IpAddressType") {
                obj.insert("IpAddressType".into(), json!("ipv4"));
            }
        }
    }
    Value::Array(arr)
}

fn body(req: &AwsRequest) -> Value {
    let raw: Value =
        serde_json::from_slice(&req.body).unwrap_or_else(|_| Value::Object(Default::default()));
    // Augment with Pascal-first duplicates so handlers can read either
    // incoming case without needing to know which the caller used.
    match raw {
        Value::Object(map) => {
            let mut merged = serde_json::Map::new();
            for (k, v) in map {
                // Insert Pascal-case view for handlers that look up e.g. `body["ApiId"]`
                let mut chars = k.chars();
                let pascal = match chars.next() {
                    Some(c) => c.to_ascii_uppercase().to_string() + chars.as_str(),
                    None => String::new(),
                };
                // If pascal differs from the original key, add both
                if pascal != k {
                    merged.insert(pascal, v.clone());
                }
                merged.insert(k, v);
            }
            Value::Object(merged)
        }
        other => other,
    }
}

fn ok(body: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(
        StatusCode::OK,
        to_camel(body).to_string(),
    ))
}

fn empty_ok() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

fn no_content() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::NO_CONTENT, ""))
}

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "BadRequestException",
        format!("Missing required field: {name}"),
    )
}

/// Extract a required string body field. Accepts either Pascal-case
/// (handler-style) or camel-case (Smithy wire). Errors with a 400
/// `BadRequestException` naming the field when absent or empty.
fn req_str<'a>(body: &'a Value, name: &str) -> Result<&'a str, AwsServiceError> {
    let v = body.get(name).or_else(|| {
        // Fallback to camel-case first-letter lookup.
        let mut chars = name.chars();
        let lowered = match chars.next() {
            Some(c) => c.to_ascii_lowercase().to_string() + chars.as_str(),
            None => String::new(),
        };
        body.get(&lowered)
    });
    match v.and_then(|v| v.as_str()) {
        Some(s) if !s.is_empty() => Ok(s),
        _ => Err(missing(name)),
    }
}

/// Extract a required array body field. Errors with a 400 when absent
/// or not an array.
fn req_array<'a>(body: &'a Value, name: &str) -> Result<&'a Vec<Value>, AwsServiceError> {
    let v = body.get(name).or_else(|| {
        let mut chars = name.chars();
        let lowered = match chars.next() {
            Some(c) => c.to_ascii_lowercase().to_string() + chars.as_str(),
            None => String::new(),
        };
        body.get(&lowered)
    });
    match v.and_then(|v| v.as_array()) {
        Some(a) => Ok(a),
        _ => Err(missing(name)),
    }
}

/// Extract a required object body field.
fn req_object<'a>(
    body: &'a Value,
    name: &str,
) -> Result<&'a serde_json::Map<String, Value>, AwsServiceError> {
    let v = body.get(name).or_else(|| {
        let mut chars = name.chars();
        let lowered = match chars.next() {
            Some(c) => c.to_ascii_lowercase().to_string() + chars.as_str(),
            None => String::new(),
        };
        body.get(&lowered)
    });
    match v.and_then(|v| v.as_object()) {
        Some(o) => Ok(o),
        _ => Err(missing(name)),
    }
}

fn bad_request(field: &str, reason: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "BadRequestException",
        format!("{field}: {reason}"),
    )
}

/// Look up a body field in either Pascal- or camel-case.
fn body_get<'a>(body: &'a Value, name: &str) -> Option<&'a Value> {
    body.get(name).or_else(|| {
        let mut chars = name.chars();
        let lowered = match chars.next() {
            Some(c) => c.to_ascii_lowercase().to_string() + chars.as_str(),
            None => String::new(),
        };
        body.get(lowered)
    })
}

/// Enforce the Smithy `@length` trait (inclusive min/max) on an optional
/// string body field.
fn check_length(
    body: &Value,
    name: &str,
    min: Option<u64>,
    max: Option<u64>,
) -> Result<(), AwsServiceError> {
    if let Some(s) = body_get(body, name).and_then(|v| v.as_str()) {
        let len = s.chars().count() as u64;
        if let Some(m) = min {
            if len < m {
                return Err(bad_request(name, &format!("length below min {m}")));
            }
        }
        if let Some(m) = max {
            if len > m {
                return Err(bad_request(name, &format!("length above max {m}")));
            }
        }
    }
    Ok(())
}

/// Enforce the Smithy `@range` trait on an optional integer body field.
fn check_range(
    body: &Value,
    name: &str,
    min: Option<i64>,
    max: Option<i64>,
) -> Result<(), AwsServiceError> {
    if let Some(n) = body_get(body, name).and_then(|v| v.as_i64()) {
        if let Some(m) = min {
            if n < m {
                return Err(bad_request(name, &format!("value below min {m}")));
            }
        }
        if let Some(m) = max {
            if n > m {
                return Err(bad_request(name, &format!("value above max {m}")));
            }
        }
    }
    Ok(())
}

/// Enforce a closed set of enum values on an optional string body field.
fn check_enum(body: &Value, name: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if let Some(s) = body_get(body, name).and_then(|v| v.as_str()) {
        if !allowed.contains(&s) {
            return Err(bad_request(name, "invalid enum value"));
        }
    }
    Ok(())
}

/// An id segment is "valid" iff it's non-empty and not a literal
/// placeholder (`{Name}` or URL-encoded `%7BName%7D`). Probe variants
/// that omit a required label leave the template token behind; treating
/// such paths as missing lets required-field validation fire instead of
/// silently operating on a placeholder string.
pub(crate) fn valid_path_id(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }
    if s.starts_with('{') && s.ends_with('}') {
        return false;
    }
    if (s.starts_with("%7B") || s.starts_with("%7b")) && (s.ends_with("%7D") || s.ends_with("%7d"))
    {
        return false;
    }
    true
}

fn not_found(entity: &str, id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NotFoundException",
        format!("{entity} not found: {id}"),
    )
}

fn rand_id() -> String {
    format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

/// Parse an OpenAPI/Swagger spec body as JSON, falling back to YAML. Both
/// `ImportApi` and `ReimportApi` accept either representation.
fn parse_openapi_spec(raw: &str) -> Result<Value, AwsServiceError> {
    if let Ok(v) = serde_json::from_str::<Value>(raw) {
        if v.is_object() {
            return Ok(v);
        }
    }
    serde_yaml::from_str::<Value>(raw)
        .ok()
        .filter(|v| v.is_object())
        .ok_or_else(|| bad_request("Body", "not a valid OpenAPI/Swagger document"))
}

/// HTTP method keys recognized inside an OpenAPI `paths.<path>` object.
const OPENAPI_HTTP_METHODS: &[&str] = &[
    "GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS", "TRACE", "ANY",
];

/// Build an `HttpApi` plus synthesized routes and integrations from an
/// OpenAPI/Swagger document. Each `paths.<path>.<method>` entry becomes a
/// route keyed `"<METHOD> <path>"`; an `x-amazon-apigateway-integration`
/// extension on the operation becomes an `Integration` wired as the route
/// target. Shared by `ImportApi` and `ReimportApi`.
fn build_api_from_spec(
    spec: &Value,
    api_id: String,
    region: &str,
) -> (
    HttpApi,
    BTreeMap<String, Route>,
    BTreeMap<String, Integration>,
) {
    let info = spec.get("info");
    let name = info
        .and_then(|i| i.get("title"))
        .and_then(|v| v.as_str())
        .unwrap_or("imported-api")
        .to_string();
    let description = info
        .and_then(|i| i.get("description"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let version = info
        .and_then(|i| i.get("version"))
        .and_then(|v| v.as_str())
        .map(String::from);

    let mut api = HttpApi::new(api_id, name, description, None, region);
    api.version = version;

    let mut routes = BTreeMap::new();
    let mut integrations = BTreeMap::new();

    if let Some(paths) = spec.get("paths").and_then(|p| p.as_object()) {
        for (path, item) in paths {
            let Some(methods) = item.as_object() else {
                continue;
            };
            for (method, op) in methods {
                let upper = method.to_ascii_uppercase();
                let route_method = if upper == "X-AMAZON-APIGATEWAY-ANY-METHOD" {
                    "ANY".to_string()
                } else if OPENAPI_HTTP_METHODS.contains(&upper.as_str()) {
                    upper
                } else {
                    // Skip non-operation keys (parameters, servers, $ref, ...).
                    continue;
                };
                let route_id = generate_id("route");
                let mut target = None;
                if let Some(integ) = op.get("x-amazon-apigateway-integration") {
                    let integration_id = generate_id("integ");
                    let integration = Integration {
                        integration_id: integration_id.clone(),
                        integration_type: integ
                            .get("type")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_ascii_uppercase())
                            .unwrap_or_else(|| "HTTP_PROXY".to_string()),
                        integration_uri: integ
                            .get("uri")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                        payload_format_version: integ
                            .get("payloadFormatVersion")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                        timeout_in_millis: integ.get("timeoutInMillis").and_then(|v| v.as_i64()),
                        integration_method: integ
                            .get("httpMethod")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                        integration_response_selection_expression: None,
                        passthrough_behavior: integ
                            .get("passthroughBehavior")
                            .and_then(|v| v.as_str())
                            .map(String::from),
                        connection_type: integ
                            .get("connectionType")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_ascii_uppercase())
                            .unwrap_or_else(|| "INTERNET".to_string()),
                        request_parameters: integ
                            .get("requestParameters")
                            .and_then(|v| v.as_object())
                            .map(|o| {
                                o.iter()
                                    .filter_map(|(k, v)| {
                                        v.as_str().map(|s| (k.clone(), s.to_string()))
                                    })
                                    .collect()
                            }),
                    };
                    integrations.insert(integration_id.clone(), integration);
                    target = Some(format!("integrations/{integration_id}"));
                }
                let route = Route {
                    route_id: route_id.clone(),
                    route_key: format!("{route_method} {path}"),
                    target,
                    ..Default::default()
                };
                routes.insert(route_id, route);
            }
        }
    }

    (api, routes, integrations)
}

/// Generate an OpenAPI 3.0 document from an API and its routes. The inverse
/// of `build_api_from_spec` for the parts that round-trip.
fn export_openapi(api: &HttpApi, routes: &[Route]) -> Value {
    let mut paths = serde_json::Map::new();
    for route in routes {
        // route_key is "<METHOD> <path>"; the `$default` catch-all carries
        // no method/path split. Export it under the `$default` path with the
        // any-method key so a round-tripped spec preserves the fallback route.
        let (method_key, path) = match route.route_key.split_once(' ') {
            Some((method, path)) => {
                let method_key = if method.eq_ignore_ascii_case("ANY") {
                    "x-amazon-apigateway-any-method".to_string()
                } else {
                    method.to_ascii_lowercase()
                };
                (method_key, path.to_string())
            }
            None if route.route_key.trim() == "$default" => (
                "x-amazon-apigateway-any-method".to_string(),
                "$default".to_string(),
            ),
            None => continue,
        };
        let entry = paths.entry(path.clone()).or_insert_with(|| json!({}));
        if let Some(obj) = entry.as_object_mut() {
            obj.insert(
                method_key,
                json!({
                    "responses": { "default": { "description": "Default response" } }
                }),
            );
        }
    }
    json!({
        "openapi": "3.0.1",
        "info": {
            "title": api.name,
            "version": api.version.clone().unwrap_or_else(|| "1.0".to_string()),
            "description": api.description.clone().unwrap_or_default(),
        },
        "paths": Value::Object(paths),
    })
}

/// Produce a best-effort example JSON value from a (subset of) JSON schema,
/// used to synthesize a `GetModelTemplate` mapping template.
fn example_from_schema(schema: &Value) -> Value {
    if let Some(example) = schema.get("example") {
        return example.clone();
    }
    let ty = schema.get("type").and_then(|v| v.as_str());
    match ty {
        Some("object") | None if schema.get("properties").is_some() => {
            let mut obj = serde_json::Map::new();
            if let Some(props) = schema.get("properties").and_then(|v| v.as_object()) {
                for (k, v) in props {
                    obj.insert(k.clone(), example_from_schema(v));
                }
            }
            Value::Object(obj)
        }
        Some("object") => Value::Object(serde_json::Map::new()),
        Some("array") => {
            let item = schema
                .get("items")
                .map(example_from_schema)
                .unwrap_or(Value::Null);
            json!([item])
        }
        Some("string") => json!(""),
        Some("integer") | Some("number") => json!(0),
        Some("boolean") => json!(false),
        _ => json!({}),
    }
}

impl ApiGatewayV2Service {
    pub(crate) fn handle_extra_action(
        &self,
        action: &str,
        req: &AwsRequest,
        api_id: Option<&str>,
        resource_id: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let aid = req.account_id.as_str();
        let region = self.region_for(aid);
        let segs = &req.path_segments;

        // Normalize invalid path-derived ids to None so handlers that
        // `ok_or_else(missing)` on a required id reject the request
        // instead of silently operating on a placeholder. See
        // `valid_path_id` for the rules.
        let api_id = api_id.filter(|s| valid_path_id(s));
        let resource_id = resource_id.filter(|s| valid_path_id(s));

        match action {
            // ── Domain names + API mappings ──
            "CreateDomainName" => {
                let body = body(req);
                check_enum(
                    &body,
                    "RoutingMode",
                    &[
                        "API_MAPPING_ONLY",
                        "ROUTING_RULE_ONLY",
                        "ROUTING_RULE_THEN_API_MAPPING",
                    ],
                )?;
                let name = req_str(&body, "DomainName")?.to_string();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                // fakecloud provisions domains synchronously, so each
                // configuration is immediately AVAILABLE. The Terraform
                // provider's create-waiter polls GetDomainName for that status.
                let configs = domain_configs_with_status(
                    body.get("DomainNameConfigurations"),
                    &name,
                    req.region.as_str(),
                );
                let mut entry = json!({
                    "DomainName": name,
                    "DomainNameArn": Arn::new("apigateway", req.region.as_str(), "", &format!("/domainnames/{name}")).to_string(),
                    "DomainNameConfigurations": configs,
                    "ApiMappingSelectionExpression": "$request.basepath",
                    "RoutingMode": "API_MAPPING_ONLY",
                    "Tags": body.get("Tags").cloned().unwrap_or(json!({})),
                });
                // Only include MutualTlsAuthentication when caller supplied it;
                // Smithy rejects `null` where an object is expected.
                if let Some(mtls) = body.get("MutualTlsAuthentication") {
                    if !mtls.is_null() {
                        entry["MutualTlsAuthentication"] = mtls.clone();
                    }
                }
                state.domain_names.insert(name.clone(), entry.clone());
                ok(entry)
            }
            "GetDomainName" => {
                let name = resource_id.ok_or_else(|| missing("DomainName"))?;
                self.read_state(aid, &region, |state| {
                    state
                        .domain_names
                        .get(name)
                        .cloned()
                        .map(ok)
                        .unwrap_or_else(|| Err(not_found("DomainName", name)))
                })
            }
            "GetDomainNames" => self.read_state(aid, &region, |state| {
                let items: Vec<&Value> = state.domain_names.values().collect();
                ok(json!({"Items": items}))
            }),
            "UpdateDomainName" => {
                let name = resource_id.ok_or_else(|| missing("DomainName"))?;
                let body = body(req);
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let entry = state
                    .domain_names
                    .get_mut(name)
                    .ok_or_else(|| not_found("DomainName", name))?;
                if body.get("DomainNameConfigurations").is_some() {
                    entry["DomainNameConfigurations"] = domain_configs_with_status(
                        body.get("DomainNameConfigurations"),
                        name,
                        req.region.as_str(),
                    );
                }
                // Previously dropped (bug-hunt 2026-06-24, 1.11): the mTLS
                // truststore config could not be updated.
                if let Some(mtls) = body.get("MutualTlsAuthentication") {
                    entry["MutualTlsAuthentication"] = mtls.clone();
                }
                ok(entry.clone())
            }
            "DeleteDomainName" => {
                let name = resource_id.ok_or_else(|| missing("DomainName"))?;
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                if state.domain_names.remove(name).is_none() {
                    return Err(not_found("DomainName", name));
                }
                state.api_mappings.remove(name);
                no_content()
            }
            "CreateApiMapping" => {
                let domain = resource_id.ok_or_else(|| missing("DomainName"))?;
                let body = body(req);
                let api = req_str(&body, "ApiId")?.to_string();
                let stage = req_str(&body, "Stage")?.to_string();
                let mapping_id = rand_id();
                let entry = json!({
                    "ApiMappingId": mapping_id,
                    "ApiMappingKey": body["ApiMappingKey"].as_str().unwrap_or(""),
                    "ApiId": api,
                    "Stage": stage,
                });
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                state
                    .api_mappings
                    .entry(domain.to_string())
                    .or_default()
                    .insert(mapping_id, entry.clone());
                ok(entry)
            }
            "GetApiMappings" => {
                let domain = resource_id.ok_or_else(|| missing("DomainName"))?;
                self.read_state(aid, &region, |state| {
                    let items: Vec<&Value> = state
                        .api_mappings
                        .get(domain)
                        .map(|m| m.values().collect())
                        .unwrap_or_default();
                    ok(json!({"Items": items}))
                })
            }
            "GetApiMapping" => {
                let domain = resource_id.ok_or_else(|| missing("DomainName"))?;
                let mapping = api_id.ok_or_else(|| missing("ApiMappingId"))?;
                self.read_state(aid, &region, |state| {
                    state
                        .api_mappings
                        .get(domain)
                        .and_then(|m| m.get(mapping))
                        .cloned()
                        .map(ok)
                        .unwrap_or_else(|| Err(not_found("ApiMapping", mapping)))
                })
            }
            "UpdateApiMapping" => {
                let domain = resource_id.ok_or_else(|| missing("DomainName"))?;
                let mapping = api_id.ok_or_else(|| missing("ApiMappingId"))?.to_string();
                let body = body(req);
                // Per Smithy: UpdateApiMappingRequest.@required = ApiId,
                // ApiMappingId, DomainName.
                let new_api = req_str(&body, "ApiId")?.to_string();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let map = state
                    .api_mappings
                    .get_mut(domain)
                    .ok_or_else(|| not_found("DomainName", domain))?;
                let entry = map
                    .get_mut(&mapping)
                    .ok_or_else(|| not_found("ApiMapping", &mapping))?;
                entry["ApiId"] = json!(new_api);
                if let Some(k) = body["ApiMappingKey"].as_str() {
                    entry["ApiMappingKey"] = json!(k);
                }
                if let Some(stage) = body["Stage"].as_str() {
                    entry["Stage"] = json!(stage);
                }
                ok(entry.clone())
            }
            "DeleteApiMapping" => {
                let domain = resource_id.ok_or_else(|| missing("DomainName"))?;
                let mapping = api_id.ok_or_else(|| missing("ApiMappingId"))?.to_string();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let removed = state
                    .api_mappings
                    .get_mut(domain)
                    .and_then(|m| m.remove(&mapping))
                    .is_some();
                if !removed {
                    return Err(not_found("ApiMapping", &mapping));
                }
                no_content()
            }

            // ── Models ──
            "CreateModel" => self.put_model(req, api_id, true),
            "UpdateModel" => self.put_model(req, api_id, false),
            "GetModel" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let model = resource_id.ok_or_else(|| missing("ModelId"))?;
                self.read_state(aid, &region, |state| {
                    state
                        .models
                        .get(api)
                        .and_then(|m| m.get(model))
                        .cloned()
                        .map(ok)
                        .unwrap_or_else(|| Err(not_found("Model", model)))
                })
            }
            "GetModels" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                self.read_state(aid, &region, |state| {
                    let items: Vec<&Value> = state
                        .models
                        .get(api)
                        .map(|m| m.values().collect())
                        .unwrap_or_default();
                    ok(json!({"Items": items}))
                })
            }
            "DeleteModel" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let model = resource_id.ok_or_else(|| missing("ModelId"))?;
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let removed = state
                    .models
                    .get_mut(api)
                    .and_then(|m| m.remove(model))
                    .is_some();
                if !removed {
                    return Err(not_found("Model", model));
                }
                no_content()
            }
            "GetModelTemplate" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let model = resource_id.ok_or_else(|| missing("ModelId"))?;
                self.read_state(aid, &region, |state| {
                    let stored = state
                        .models
                        .get(api)
                        .and_then(|m| m.get(model))
                        .ok_or_else(|| not_found("Model", model))?;
                    // Derive a mapping-template example from the model schema
                    // rather than returning a fixed constant.
                    let schema = stored
                        .get("Schema")
                        .or_else(|| stored.get("schema"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("{}");
                    let schema_json: Value =
                        serde_json::from_str(schema).unwrap_or_else(|_| json!({}));
                    let template = example_from_schema(&schema_json);
                    ok(json!({ "Value": template.to_string() }))
                })
            }

            // ── Integration responses ──
            "CreateIntegrationResponse" => {
                let b = body(req);
                check_enum(
                    &b,
                    "ContentHandlingStrategy",
                    &["CONVERT_TO_BINARY", "CONVERT_TO_TEXT"],
                )?;
                self.put_subresponse(req, api_id, resource_id, true, true)
            }
            "UpdateIntegrationResponse" => {
                let b = body(req);
                check_enum(
                    &b,
                    "ContentHandlingStrategy",
                    &["CONVERT_TO_BINARY", "CONVERT_TO_TEXT"],
                )?;
                self.put_subresponse(req, api_id, resource_id, true, false)
            }
            "GetIntegrationResponse" => self.get_subresponse(req, api_id, resource_id, true),
            "GetIntegrationResponses" => {
                self.list_subresponses(api_id, resource_id, true, &region, aid)
            }
            "DeleteIntegrationResponse" => self.delete_subresponse(req, api_id, resource_id, true),

            // ── Route responses ──
            "CreateRouteResponse" => self.put_subresponse(req, api_id, resource_id, false, true),
            "UpdateRouteResponse" => self.put_subresponse(req, api_id, resource_id, false, false),
            "GetRouteResponse" => self.get_subresponse(req, api_id, resource_id, false),
            "GetRouteResponses" => self.list_subresponses(api_id, resource_id, false, &region, aid),
            "DeleteRouteResponse" => self.delete_subresponse(req, api_id, resource_id, false),

            // ── Routing rules (nested under /v2/domainnames/{name}) ──
            "CreateRoutingRule" => {
                let domain = resource_id
                    .ok_or_else(|| missing("DomainName"))?
                    .to_string();
                let body = body(req);
                let actions = req_array(&body, "Actions")?.clone();
                let conditions = req_array(&body, "Conditions")?.clone();
                check_range(&body, "Priority", Some(1), Some(1_000_000))?;
                let priority = body
                    .get("Priority")
                    .or_else(|| body.get("priority"))
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| missing("Priority"))?;
                let id = rand_id();
                let entry = json!({
                    "RoutingRuleId": id,
                    "RoutingRuleArn": format!(
                        "arn:aws:apigateway:us-east-1::/domainnames/{}/routingrules/{}",
                        domain, id
                    ),
                    "Priority": priority,
                    "Conditions": conditions,
                    "Actions": actions,
                });
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                state
                    .routing_rules
                    .entry(domain)
                    .or_default()
                    .insert(id.clone(), entry.clone());
                ok(entry)
            }
            "PutRoutingRule" => {
                let domain = resource_id
                    .ok_or_else(|| missing("DomainName"))?
                    .to_string();
                let id = api_id.ok_or_else(|| missing("RoutingRuleId"))?.to_string();
                let body = body(req);
                let actions = req_array(&body, "Actions")?.clone();
                let conditions = req_array(&body, "Conditions")?.clone();
                check_range(&body, "Priority", Some(1), Some(1_000_000))?;
                let priority = body
                    .get("Priority")
                    .or_else(|| body.get("priority"))
                    .and_then(|v| v.as_i64())
                    .ok_or_else(|| missing("Priority"))?;
                let entry = json!({
                    "RoutingRuleId": id,
                    "RoutingRuleArn": format!(
                        "arn:aws:apigateway:us-east-1::/domainnames/{}/routingrules/{}",
                        domain, id
                    ),
                    "Priority": priority,
                    "Conditions": conditions,
                    "Actions": actions,
                });
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                state
                    .routing_rules
                    .entry(domain)
                    .or_default()
                    .insert(id, entry.clone());
                ok(entry)
            }
            "GetRoutingRule" => {
                let domain = resource_id.ok_or_else(|| missing("DomainName"))?;
                let id = api_id.ok_or_else(|| missing("RoutingRuleId"))?;
                self.read_state(aid, &region, |state| {
                    state
                        .routing_rules
                        .get(domain)
                        .and_then(|m| m.get(id))
                        .cloned()
                        .map(ok)
                        .unwrap_or_else(|| Err(not_found("RoutingRule", id)))
                })
            }
            "ListRoutingRules" => {
                let domain = resource_id.ok_or_else(|| missing("DomainName"))?;
                // MaxResults is @range(min:1,max:100) per Smithy.
                if let Some(mr_str) = req
                    .query_params
                    .iter()
                    .find(|(k, _)| *k == "maxResults")
                    .map(|(_, v)| v.as_str())
                {
                    if let Ok(n) = mr_str.parse::<i64>() {
                        if !(1..=100).contains(&n) {
                            return Err(bad_request("MaxResults", "value out of range [1,100]"));
                        }
                    }
                }
                self.read_state(aid, &region, |state| {
                    let rules: Vec<Value> = state
                        .routing_rules
                        .get(domain)
                        .map(|m| m.values().cloned().collect())
                        .unwrap_or_default();
                    ok(json!({"RoutingRules": rules}))
                })
            }
            "DeleteRoutingRule" => {
                let domain = resource_id
                    .ok_or_else(|| missing("DomainName"))?
                    .to_string();
                let id = api_id.ok_or_else(|| missing("RoutingRuleId"))?.to_string();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let removed = state
                    .routing_rules
                    .get_mut(&domain)
                    .and_then(|m| m.remove(&id))
                    .is_some();
                if !removed {
                    return Err(not_found("RoutingRule", &id));
                }
                no_content()
            }

            // ── VPC links ──
            "CreateVpcLink" => {
                let body = body(req);
                let name = req_str(&body, "Name")?.to_string();
                let subnet_ids = req_array(&body, "SubnetIds")?.clone();
                let id = rand_id();
                let entry = json!({
                    "VpcLinkId": id,
                    "Name": name,
                    "SubnetIds": subnet_ids,
                    "SecurityGroupIds": body.get("SecurityGroupIds").cloned().unwrap_or(json!([])),
                    "VpcLinkStatus": "AVAILABLE",
                });
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                state.vpc_links.insert(id, entry.clone());
                ok(entry)
            }
            "GetVpcLink" => {
                let id = resource_id.ok_or_else(|| missing("VpcLinkId"))?;
                self.read_state(aid, &region, |state| {
                    state
                        .vpc_links
                        .get(id)
                        .cloned()
                        .map(ok)
                        .unwrap_or_else(|| Err(not_found("VpcLink", id)))
                })
            }
            "GetVpcLinks" => self.read_state(aid, &region, |state| {
                let items: Vec<&Value> = state.vpc_links.values().collect();
                ok(json!({"Items": items}))
            }),
            "UpdateVpcLink" => {
                let id = resource_id.ok_or_else(|| missing("VpcLinkId"))?;
                let body = body(req);
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let entry = state
                    .vpc_links
                    .get_mut(id)
                    .ok_or_else(|| not_found("VpcLink", id))?;
                if let Some(name) = body["Name"].as_str() {
                    entry["Name"] = json!(name);
                }
                ok(entry.clone())
            }
            "DeleteVpcLink" => {
                let id = resource_id.ok_or_else(|| missing("VpcLinkId"))?;
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                if state.vpc_links.remove(id).is_none() {
                    return Err(not_found("VpcLink", id));
                }
                no_content()
            }

            // ── Tags ──
            "TagResource" => {
                // `ResourceArn` is a non-greedy @httpLabel, so the SDK
                // percent-encodes its `/` and `:` on the wire; decode back to
                // the plain ARN so `state.tags` is keyed by the same value that
                // CreateApi/CreateStage compute (`arn:aws:apigateway:...`),
                // unifying create-time tags with the tag verbs.
                let arn = percent_encoding::percent_decode_str(
                    resource_id.ok_or_else(|| missing("ResourceArn"))?,
                )
                .decode_utf8_lossy()
                .into_owned();
                let body = body(req);
                let tags_in = req_object(&body, "Tags")?.clone();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let tags = state.tags.entry(arn).or_default();
                for (k, v) in &tags_in {
                    if let Some(s) = v.as_str() {
                        tags.insert(k.clone(), s.to_string());
                    }
                }
                no_content()
            }
            "UntagResource" => {
                // Decode the percent-encoded @httpLabel ARN (see TagResource).
                let arn = percent_encoding::percent_decode_str(
                    resource_id.ok_or_else(|| missing("ResourceArn"))?,
                )
                .decode_utf8_lossy()
                .into_owned();
                // TagKeys is a required @httpQuery list per Smithy — the SDK
                // renders each entry as repeated `tagKeys={key}` pairs.
                // `query_params` collapses repeats to the last value, so parse
                // every occurrence out of the raw query string, percent-decoding
                // each.
                let has_tag_keys = req
                    .raw_query
                    .split('&')
                    .any(|pair| pair == "tagKeys" || pair.starts_with("tagKeys="));
                if !has_tag_keys {
                    return Err(missing("TagKeys"));
                }
                let keys: Vec<String> = req
                    .raw_query
                    .split('&')
                    .filter_map(|pair| pair.strip_prefix("tagKeys="))
                    .map(|v| {
                        percent_encoding::percent_decode_str(v)
                            .decode_utf8_lossy()
                            .into_owned()
                    })
                    .collect();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                if let Some(tags) = state.tags.get_mut(&arn) {
                    for key in &keys {
                        tags.remove(key);
                    }
                }
                no_content()
            }
            "GetTags" => {
                // Decode the percent-encoded @httpLabel ARN (see TagResource).
                let arn = percent_encoding::percent_decode_str(
                    resource_id.ok_or_else(|| missing("ResourceArn"))?,
                )
                .decode_utf8_lossy()
                .into_owned();
                self.read_state(aid, &region, |state| {
                    let tags = state.tags.get(&arn).cloned().unwrap_or_default();
                    ok(json!({"Tags": tags}))
                })
            }

            // ── Portals + portal products + product pages ──
            "CreatePortal" => {
                // Per Smithy, CreatePortalRequest.@required = Authorization,
                // EndpointConfiguration, PortalContent. Validate before
                // delegating to put_keyed.
                let b = body(req);
                req_object(&b, "Authorization")?;
                req_object(&b, "EndpointConfiguration")?;
                req_object(&b, "PortalContent")?;
                check_length(&b, "LogoUri", None, Some(1092))?;
                check_length(&b, "RumAppMonitorName", None, Some(255))?;
                self.put_keyed(req, resource_id, "PortalId", "portals", aid, true)
            }
            "UpdatePortal" => {
                resource_id.ok_or_else(|| missing("PortalId"))?;
                let b = body(req);
                check_length(&b, "LogoUri", None, Some(1092))?;
                check_length(&b, "RumAppMonitorName", None, Some(255))?;
                self.put_keyed(req, resource_id, "PortalId", "portals", aid, false)
            }
            "GetPortal" => self.get_keyed(resource_id, "portals", aid, &region),
            "ListPortals" => self.list_keyed("portals", aid, &region),
            "DeletePortal" => self.delete_keyed(resource_id, "portals", aid),
            "DisablePortal" => {
                let id = resource_id.ok_or_else(|| missing("PortalId"))?;
                // Persist the disable so GetPortal reflects it instead of
                // no-op'ing (bug-audit 2026-06-13, 1.12).
                self.mutate_portal(aid, id, |portal| {
                    portal["PublishStatus"] = json!("DISABLED");
                })?;
                empty_ok()
            }
            "PreviewPortal" => {
                let id = resource_id.ok_or_else(|| missing("PortalId"))?;
                let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                self.mutate_portal(aid, id, |portal| {
                    portal["Preview"] = json!({
                        "PreviewStatus": "PREVIEW_AVAILABLE",
                        "PreviewUrl": format!("https://{id}.preview.portal.example.com"),
                        "StatusException": {},
                        "LastModified": now,
                    });
                })?;
                empty_ok()
            }
            "PublishPortal" => {
                let id = resource_id.ok_or_else(|| missing("PortalId"))?;
                let b = body(req);
                check_length(&b, "Description", None, Some(1024))?;
                let description = b
                    .get("Description")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                self.mutate_portal(aid, id, |portal| {
                    portal["PublishStatus"] = json!("PUBLISHED");
                    portal["LastPublished"] = json!(now);
                    portal["LastPublishedDescription"] = json!(description);
                })?;
                empty_ok()
            }

            "CreatePortalProduct" => {
                let b = body(req);
                req_str(&b, "DisplayName")?;
                check_length(&b, "DisplayName", Some(1), Some(255))?;
                check_length(&b, "Description", None, Some(1024))?;
                self.put_keyed(
                    req,
                    resource_id,
                    "PortalProductId",
                    "portal_products",
                    aid,
                    true,
                )
            }
            "UpdatePortalProduct" => {
                resource_id.ok_or_else(|| missing("PortalProductId"))?;
                let b = body(req);
                check_length(&b, "DisplayName", Some(1), Some(255))?;
                check_length(&b, "Description", None, Some(1024))?;
                self.put_keyed(
                    req,
                    resource_id,
                    "PortalProductId",
                    "portal_products",
                    aid,
                    false,
                )
            }
            "GetPortalProduct" => self.get_keyed(resource_id, "portal_products", aid, &region),
            "ListPortalProducts" => self.list_keyed("portal_products", aid, &region),
            "DeletePortalProduct" => self.delete_keyed(resource_id, "portal_products", aid),

            "PutPortalProductSharingPolicy" => {
                let id = resource_id.ok_or_else(|| missing("PortalProductId"))?;
                let body = body(req);
                req_str(&body, "PolicyDocument")?;
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                state
                    .portal_product_sharing_policies
                    .insert(id.to_string(), body);
                empty_ok()
            }
            "GetPortalProductSharingPolicy" => {
                let id = resource_id.ok_or_else(|| missing("PortalProductId"))?;
                self.read_state(aid, &region, |state| {
                    state
                        .portal_product_sharing_policies
                        .get(id)
                        .cloned()
                        .map(ok)
                        .unwrap_or_else(|| ok(json!({})))
                })
            }
            "DeletePortalProductSharingPolicy" => {
                let id = resource_id.ok_or_else(|| missing("PortalProductId"))?;
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                if state.portal_product_sharing_policies.remove(id).is_none() {
                    return Err(not_found("PortalProductSharingPolicy", id));
                }
                no_content()
            }

            "CreateProductPage" => {
                let b = body(req);
                req_object(&b, "DisplayContent")?;
                self.put_subresource(
                    req,
                    resource_id,
                    segs.get(4).map(|s| s.to_string()),
                    "product_pages",
                    aid,
                )
            }
            "UpdateProductPage" => {
                resource_id.ok_or_else(|| missing("PortalProductId"))?;
                let page_id = segs.get(4).map(|s| s.as_str()).unwrap_or("");
                if !valid_path_id(page_id) {
                    return Err(missing("ProductPageId"));
                }
                self.put_subresource(
                    req,
                    resource_id,
                    Some(page_id.to_string()),
                    "product_pages",
                    aid,
                )
            }
            "GetProductPage" => self.get_subresource(
                resource_id,
                segs.get(4).map(|s| s.as_str()),
                "product_pages",
                aid,
                &region,
            ),
            "ListProductPages" => {
                self.list_subresources(resource_id, "product_pages", aid, &region)
            }
            "DeleteProductPage" => self.delete_subresource(
                resource_id,
                segs.get(4).map(|s| s.as_str()),
                "product_pages",
                aid,
            ),
            "CreateProductRestEndpointPage" => {
                let b = body(req);
                req_object(&b, "RestEndpointIdentifier")?;
                check_enum(&b, "TryItState", &["ENABLED", "DISABLED"])?;
                self.put_subresource(
                    req,
                    resource_id,
                    segs.get(4).map(|s| s.to_string()),
                    "product_rest_endpoint_pages",
                    aid,
                )
            }
            "UpdateProductRestEndpointPage" => {
                resource_id.ok_or_else(|| missing("PortalProductId"))?;
                let page_id = segs.get(4).map(|s| s.as_str()).unwrap_or("");
                if !valid_path_id(page_id) {
                    return Err(missing("ProductRestEndpointPageId"));
                }
                let b = body(req);
                check_enum(&b, "TryItState", &["ENABLED", "DISABLED"])?;
                self.put_subresource(
                    req,
                    resource_id,
                    Some(page_id.to_string()),
                    "product_rest_endpoint_pages",
                    aid,
                )
            }
            "GetProductRestEndpointPage" => self.get_subresource(
                resource_id,
                segs.get(4).map(|s| s.as_str()),
                "product_rest_endpoint_pages",
                aid,
                &region,
            ),
            "ListProductRestEndpointPages" => {
                self.list_subresources(resource_id, "product_rest_endpoint_pages", aid, &region)
            }
            "DeleteProductRestEndpointPage" => self.delete_subresource(
                resource_id,
                segs.get(4).map(|s| s.as_str()),
                "product_rest_endpoint_pages",
                aid,
            ),

            // ── Import / Export ──
            "ImportApi" => {
                let body = body(req);
                let spec_raw = req_str(&body, "Body")?.to_string();
                let spec = parse_openapi_spec(&spec_raw)?;
                let new_api_id = generate_id("api");
                let (api, routes, integrations) =
                    build_api_from_spec(&spec, new_api_id.clone(), &region);
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                state.apis.insert(new_api_id.clone(), api.clone());
                state.routes.insert(new_api_id.clone(), routes);
                state.integrations.insert(new_api_id.clone(), integrations);
                ok(json!(api))
            }
            "ReimportApi" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?.to_string();
                let body = body(req);
                let spec_raw = req_str(&body, "Body")?.to_string();
                let spec = parse_openapi_spec(&spec_raw)?;
                let (rebuilt, routes, integrations) =
                    build_api_from_spec(&spec, api.clone(), &region);
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let existing = state
                    .apis
                    .get_mut(&api)
                    .ok_or_else(|| not_found("Api", &api))?;
                // Preserve the existing endpoint/ids; overlay name/description/
                // version and the synthesized surface from the new spec.
                existing.name = rebuilt.name;
                existing.description = rebuilt.description;
                existing.version = rebuilt.version;
                let updated = existing.clone();
                state.routes.insert(api.clone(), routes);
                state.integrations.insert(api.clone(), integrations);
                ok(json!(updated))
            }
            "ExportApi" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?.to_string();
                // Specification is an httpLabel (segs[4]) — already filtered
                // to None for empty/placeholder path ids, so resource_id=None
                // here means the caller omitted it.
                let _spec = resource_id.ok_or_else(|| missing("Specification"))?;
                let output_type = req
                    .query_params
                    .iter()
                    .find(|(k, _)| *k == "outputType")
                    .map(|(_, v)| v.as_str());
                let Some(output_type) = output_type else {
                    return Err(missing("OutputType"));
                };
                let document = self.read_state(aid, &region, |state| {
                    let api_obj = state.apis.get(&api).ok_or_else(|| not_found("Api", &api))?;
                    let routes = state
                        .routes
                        .get(&api)
                        .map(|r| r.values().cloned().collect::<Vec<_>>())
                        .unwrap_or_default();
                    Ok::<_, AwsServiceError>(export_openapi(api_obj, &routes))
                })?;
                // ExportApi returns the spec as a Blob body. Honor the requested
                // OutputType (JSON default, YAML when asked).
                let rendered = if output_type.eq_ignore_ascii_case("YAML") {
                    serde_yaml::to_string(&document).unwrap_or_default()
                } else {
                    serde_json::to_string_pretty(&document).unwrap_or_default()
                };
                ok(json!({ "body": rendered }))
            }

            // ── Cleanup ops ──
            "DeleteCorsConfiguration" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let api_obj = state
                    .apis
                    .get_mut(api)
                    .ok_or_else(|| not_found("Api", api))?;
                api_obj.cors_configuration = None;
                no_content()
            }
            "DeleteAccessLogSettings" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let stage_name = resource_id.ok_or_else(|| missing("StageName"))?;
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let stage = state
                    .stages
                    .get_mut(api)
                    .and_then(|s| s.get_mut(stage_name))
                    .ok_or_else(|| not_found("Stage", stage_name))?;
                stage.access_log_settings = None;
                no_content()
            }
            "DeleteRouteRequestParameter" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let route = resource_id.ok_or_else(|| missing("RouteId"))?;
                // RequestParameterKey is segs[6]; enforce non-empty too.
                let raw_key = segs.get(6).map(|s| s.as_str()).unwrap_or("");
                if !valid_path_id(raw_key) {
                    return Err(missing("RequestParameterKey"));
                }
                // The key may be percent-encoded in the URL; match the stored
                // (decoded) key. Previously this validated then returned 204
                // WITHOUT removing anything, so GetRoute kept returning the
                // parameter (bug-hunt 2026-07-16, 1.22).
                let key = percent_encoding::percent_decode_str(raw_key)
                    .decode_utf8_lossy()
                    .into_owned();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let route_obj = state
                    .routes
                    .get_mut(api)
                    .and_then(|r| r.get_mut(route))
                    .ok_or_else(|| not_found("Route", route))?;
                if let Some(params) = route_obj.request_parameters.as_mut() {
                    params.remove(&key);
                    if params.is_empty() {
                        route_obj.request_parameters = None;
                    }
                }
                no_content()
            }
            "DeleteRouteSettings" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let stage_name = resource_id.ok_or_else(|| missing("StageName"))?;
                let raw_key = segs.get(6).map(|s| s.as_str()).unwrap_or("");
                if !valid_path_id(raw_key) {
                    return Err(missing("RouteKey"));
                }
                // Actually remove the per-route entry from the stage's
                // route_settings. Previously this no-op'd and left the override
                // in place (bug-hunt 2026-07-16, 1.22).
                let key = percent_encoding::percent_decode_str(raw_key)
                    .decode_utf8_lossy()
                    .into_owned();
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let stage = state
                    .stages
                    .get_mut(api)
                    .and_then(|s| s.get_mut(stage_name))
                    .ok_or_else(|| not_found("Stage", stage_name))?;
                if let Some(settings) = stage.route_settings.as_mut() {
                    settings.remove(&key);
                    if settings.is_empty() {
                        stage.route_settings = None;
                    }
                }
                no_content()
            }
            "DeleteDeployment" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let dep = resource_id.ok_or_else(|| missing("DeploymentId"))?;
                // The old handler returned 204 without removing anything, so
                // deployments accumulated and were un-deletable (bug-audit
                // 2026-06-20, 1.23). Validate the deployment exists (AWS returns
                // NotFound otherwise) and actually remove it.
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                let deployments = state
                    .deployments
                    .get_mut(api)
                    .ok_or_else(|| not_found("Deployment", dep))?;
                if deployments.remove(dep).is_none() {
                    return Err(not_found("Deployment", dep));
                }
                no_content()
            }
            "UpdateDeployment" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let dep = resource_id.ok_or_else(|| missing("DeploymentId"))?;
                let body = body(req);
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                // Real AWS validates both the API and the deployment exist
                // before applying the patch. Returning success on a fake
                // identifier here would mask drift the round-trip probe
                // explicitly looks for.
                let deployments = state
                    .deployments
                    .get_mut(api)
                    .ok_or_else(|| not_found("Deployment", dep))?;
                let entry = deployments
                    .get_mut(dep)
                    .ok_or_else(|| not_found("Deployment", dep))?;
                if let Some(desc) = body.get("Description").and_then(|v| v.as_str()) {
                    entry.description = Some(desc.to_string());
                }
                ok(json!({
                    "DeploymentId": entry.deployment_id,
                    "DeploymentStatus": "DEPLOYED",
                    "Description": entry.description.clone().unwrap_or_default(),
                    "CreatedDate": entry.created_date.to_rfc3339(),
                }))
            }
            "ResetAuthorizersCache" => {
                api_id.ok_or_else(|| missing("ApiId"))?;
                resource_id.ok_or_else(|| missing("StageName"))?;
                no_content()
            }

            _ => Err(AwsServiceError::action_not_implemented(
                "apigateway",
                action,
            )),
        }
    }

    fn put_model(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        is_create: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api = api_id.ok_or_else(|| missing("ApiId"))?.to_string();
        let body = body(req);
        // CreateModelRequest.@required = ApiId, Name, Schema.
        if is_create {
            req_str(&body, "Name")?;
            req_str(&body, "Schema")?;
        }
        let id = if is_create {
            rand_id()
        } else {
            req.path_segments
                .get(4)
                .map(|s| s.to_string())
                .ok_or_else(|| missing("ModelId"))?
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let bucket = state.models.entry(api).or_default();
        if !is_create && !bucket.contains_key(&id) {
            return Err(not_found("Model", &id));
        }
        // On update, start from the existing entry and overlay only the fields
        // the request actually sent. Rebuilding from defaults clobbered Name to
        // "" and ContentType to the default whenever the caller patched only
        // Schema — true data loss (bug-hunt 2026-06-24, 1.11).
        let mut entry = if is_create {
            json!({
                "ModelId": id,
                "Name": body["Name"].as_str().unwrap_or(""),
                "Schema": body["Schema"].as_str().unwrap_or("{}"),
                "ContentType": body["ContentType"].as_str().unwrap_or("application/json"),
            })
        } else {
            bucket
                .get(&id)
                .cloned()
                .unwrap_or_else(|| json!({ "ModelId": id }))
        };
        for field in ["Name", "Schema", "ContentType", "Description"] {
            if let Some(v) = body.get(field).filter(|v| !v.is_null()) {
                entry[field] = v.clone();
            }
        }
        entry["ModelId"] = json!(id);
        bucket.insert(id.clone(), entry.clone());
        ok(entry)
    }

    fn put_subresponse(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        parent_id: Option<&str>,
        is_integration: bool,
        is_create: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api = api_id.ok_or_else(|| missing("ApiId"))?.to_string();
        let parent = parent_id.ok_or_else(|| {
            if is_integration {
                missing("IntegrationId")
            } else {
                missing("RouteId")
            }
        })?;
        let entry = body(req);
        // On Create, the response-key is required per Smithy (members
        // IntegrationResponseKey / RouteResponseKey).
        if is_create {
            let key_name = if is_integration {
                "IntegrationResponseKey"
            } else {
                "RouteResponseKey"
            };
            req_str(&entry, key_name)?;
        }
        let id = if is_create {
            rand_id()
        } else {
            req.path_segments
                .get(6)
                .map(|s| s.to_string())
                .ok_or_else(|| missing("ResponseId"))?
        };
        let key = format!("{parent}/{id}");
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let store = if is_integration {
            &mut state.integration_responses
        } else {
            &mut state.route_responses
        };
        let bucket = store.entry(api).or_default();
        if !is_create && !bucket.contains_key(&key) {
            return Err(not_found("Response", &id));
        }
        // Update is a partial patch: merge the incoming fields onto the
        // existing record so unspecified members persist. Previously the
        // record was replaced wholesale, wiping every field the caller
        // didn't resend.
        let mut value = if is_create {
            entry.clone()
        } else {
            let mut existing = bucket.get(&key).cloned().unwrap_or_else(|| entry.clone());
            if let (Some(dst), Some(src)) = (existing.as_object_mut(), entry.as_object()) {
                for (k, v) in src {
                    dst.insert(k.clone(), v.clone());
                }
            }
            existing
        };
        if is_integration {
            value["IntegrationResponseId"] = json!(id);
            // IntegrationResponseKey is required on the Smithy response shape.
            if value
                .get("IntegrationResponseKey")
                .and_then(|v| v.as_str())
                .is_none()
            {
                value["IntegrationResponseKey"] = json!("$default");
            }
        } else {
            value["RouteResponseId"] = json!(id);
            if value
                .get("RouteResponseKey")
                .and_then(|v| v.as_str())
                .is_none()
            {
                value["RouteResponseKey"] = json!("$default");
            }
        }
        bucket.insert(key, value.clone());
        ok(value)
    }

    fn get_subresponse(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        parent_id: Option<&str>,
        is_integration: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api = api_id.ok_or_else(|| missing("ApiId"))?;
        let parent = parent_id.ok_or_else(|| missing("Parent"))?;
        let id = req
            .path_segments
            .get(6)
            .ok_or_else(|| missing("ResponseId"))?;
        let key = format!("{parent}/{id}");
        let region = self.region_for(&req.account_id);
        self.read_state(&req.account_id, &region, |state| {
            let store = if is_integration {
                &state.integration_responses
            } else {
                &state.route_responses
            };
            store
                .get(api)
                .and_then(|m| m.get(&key))
                .cloned()
                .map(ok)
                .unwrap_or_else(|| Err(not_found("Response", id)))
        })
    }

    fn list_subresponses(
        &self,
        api_id: Option<&str>,
        parent_id: Option<&str>,
        is_integration: bool,
        region: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api = api_id.ok_or_else(|| missing("ApiId"))?;
        let parent = parent_id.ok_or_else(|| missing("Parent"))?.to_string();
        self.read_state(account_id, region, |state| {
            let store = if is_integration {
                &state.integration_responses
            } else {
                &state.route_responses
            };
            let prefix = format!("{parent}/");
            let items: Vec<&Value> = store
                .get(api)
                .map(|m| {
                    m.iter()
                        .filter(|(k, _)| k.starts_with(&prefix))
                        .map(|(_, v)| v)
                        .collect()
                })
                .unwrap_or_default();
            ok(json!({"Items": items}))
        })
    }

    fn delete_subresponse(
        &self,
        req: &AwsRequest,
        api_id: Option<&str>,
        parent_id: Option<&str>,
        is_integration: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let api = api_id.ok_or_else(|| missing("ApiId"))?.to_string();
        let parent = parent_id.ok_or_else(|| missing("Parent"))?.to_string();
        let id = req
            .path_segments
            .get(6)
            .ok_or_else(|| missing("ResponseId"))?
            .to_string();
        let key = format!("{parent}/{id}");
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let store = if is_integration {
            &mut state.integration_responses
        } else {
            &mut state.route_responses
        };
        let removed = store.get_mut(&api).and_then(|m| m.remove(&key)).is_some();
        if !removed {
            return Err(not_found("Response", &id));
        }
        no_content()
    }

    /// Apply an in-place mutation to a stored portal so the portal
    /// lifecycle ops (Disable/Preview/Publish) persist their effect and
    /// GetPortal reflects it. Returns `NotFound` when the portal is
    /// unknown.
    fn mutate_portal(
        &self,
        account_id: &str,
        id: &str,
        f: impl FnOnce(&mut Value),
    ) -> Result<(), AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let portal = state
            .portals
            .get_mut(id)
            .ok_or_else(|| not_found("portals", id))?;
        f(portal);
        Ok(())
    }

    fn put_keyed(
        &self,
        req: &AwsRequest,
        id_opt: Option<&str>,
        id_field: &str,
        store: &str,
        account_id: &str,
        is_create: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = id_opt.map(String::from).unwrap_or_else(rand_id);
        let input = body(req);
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();

        // Update is a partial patch: start from the stored record and overlay
        // only the fields the caller actually sent. Rebuilding from defaults
        // on every update clobbered PublishStatus back to UNPUBLISHED and reset
        // Tags/Authorization/EndpointConfiguration — a Create->Publish->Update
        // round-trip lost the publish state (bug-hunt 2026-07-16, 1.22). Mirror
        // the partial-update semantics already used by `put_model`.
        if !is_create {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(account_id);
            let map = match store {
                "portals" => &mut state.portals,
                "portal_products" => &mut state.portal_products,
                _ => return Err(missing("Store")),
            };
            let mut entry = map.get(&id).cloned().ok_or_else(|| not_found(store, &id))?;
            entry["LastModified"] = json!(now);
            match store {
                "portals" => {
                    if let Some(pc) = input.get("PortalContent").filter(|v| !v.is_null()) {
                        // Merge onto the existing content so DisplayName/Theme
                        // defaults survive a partial content patch.
                        if let (Some(dst), Some(src)) = (
                            entry
                                .get_mut("PortalContent")
                                .and_then(|v| v.as_object_mut()),
                            pc.as_object(),
                        ) {
                            for (k, v) in src {
                                dst.insert(k.clone(), v.clone());
                            }
                        } else {
                            entry["PortalContent"] = pc.clone();
                        }
                    }
                    if let Some(in_ec) = input.get("EndpointConfiguration") {
                        let ec = entry
                            .get_mut("EndpointConfiguration")
                            .filter(|v| v.is_object());
                        if let Some(ec) = ec {
                            for key in [
                                "CertificateArn",
                                "DomainName",
                                "certificateArn",
                                "domainName",
                            ] {
                                if let Some(v) = in_ec.get(key).filter(|v| !v.is_null()) {
                                    ec[key] = v.clone();
                                }
                            }
                        }
                    }
                    for field in ["Authorization", "Tags", "RumAppMonitorName"] {
                        if let Some(v) = input.get(field).filter(|v| !v.is_null()) {
                            entry[field] = v.clone();
                        }
                    }
                }
                "portal_products" => {
                    for field in ["Description", "DisplayName", "Tags"] {
                        if let Some(v) = input.get(field).filter(|v| !v.is_null()) {
                            entry[field] = v.clone();
                        }
                    }
                }
                _ => return Err(missing("Store")),
            }
            map.insert(id.clone(), entry.clone());
            return ok(entry);
        }

        // Create: build response from scratch with Smithy-required fields. Don't
        // echo arbitrary input keys — probe variants send things like
        // `logoUri` that aren't on the response shape.
        let entry = match store {
            "portals" => {
                let mut portal_content = input
                    .get("PortalContent")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                // PortalContent requires DisplayName + Theme per Smithy.
                if portal_content
                    .get("displayName")
                    .and_then(|v| v.as_str())
                    .is_none()
                    && portal_content
                        .get("DisplayName")
                        .and_then(|v| v.as_str())
                        .is_none()
                {
                    portal_content["DisplayName"] = json!(id);
                }
                // Theme is a PortalTheme struct (CustomColors + timestamp), not a string.
                if portal_content
                    .get("theme")
                    .or_else(|| portal_content.get("Theme"))
                    .and_then(|v| if v.is_object() { Some(()) } else { None })
                    .is_none()
                {
                    portal_content["Theme"] = json!({
                        "CustomColors": {
                            "AccentColor": "#ff9900",
                            "BackgroundColor": "#ffffff",
                            "ErrorValidationColor": "#d13212",
                            "HeaderColor": "#232f3e",
                            "NavigationColor": "#232f3e",
                            "TextColor": "#000000",
                        },
                    });
                }
                // Rebuild endpoint configuration strictly (Smithy shape has
                // only CertificateArn, DomainName, PortalDefaultDomainName,
                // PortalDomainHostedZoneId).
                let mut ec = json!({
                    "PortalDefaultDomainName": format!("{}.portal.example.com", id),
                    "PortalDomainHostedZoneId": "Z123456789PORTAL",
                });
                if let Some(in_ec) = input.get("EndpointConfiguration") {
                    for key in [
                        "CertificateArn",
                        "DomainName",
                        "certificateArn",
                        "domainName",
                    ] {
                        if let Some(v) = in_ec.get(key) {
                            if !v.is_null() {
                                ec[key] = v.clone();
                            }
                        }
                    }
                }
                json!({
                    id_field: id,
                    "PortalArn": Arn::new("apigateway", req.region.as_str(), "", &format!("/portals/{id}")).to_string(),
                    "LastModified": now,
                    "LastPublished": now,
                    "LastPublishedDescription": "",
                    "PublishStatus": "UNPUBLISHED",
                    "RumAppMonitorName": "",
                    "IncludedPortalProductArns": [],
                    "Tags": input.get("Tags").cloned().unwrap_or(json!({})),
                    "Authorization": input.get("Authorization").cloned().unwrap_or(json!({})),
                    "EndpointConfiguration": ec,
                    "PortalContent": portal_content,
                    "StatusException": json!({}),
                })
            }
            "portal_products" => json!({
                id_field: id,
                "PortalProductArn": Arn::new("apigateway", req.region.as_str(), "", &format!("/portalproducts/{id}")).to_string(),
                "LastModified": now,
                "Description": input.get("Description").and_then(|x| x.as_str()).unwrap_or(""),
                "DisplayName": input.get("DisplayName").and_then(|x| x.as_str()).unwrap_or(&id),
                "Tags": input.get("Tags").cloned().unwrap_or(json!({})),
            }),
            _ => return Err(missing("Store")),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let map = match store {
            "portals" => &mut state.portals,
            "portal_products" => &mut state.portal_products,
            _ => return Err(missing("Store")),
        };
        map.insert(id.clone(), entry.clone());
        ok(entry)
    }

    fn get_keyed(
        &self,
        id_opt: Option<&str>,
        store: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = id_opt.ok_or_else(|| missing("Id"))?;
        self.read_state(account_id, region, |state| {
            let map = match store {
                "portals" => &state.portals,
                "portal_products" => &state.portal_products,
                _ => return Err(missing("Store")),
            };
            map.get(id)
                .cloned()
                .map(ok)
                .unwrap_or_else(|| Err(not_found(store, id)))
        })
    }

    fn list_keyed(
        &self,
        store: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.read_state(account_id, region, |state| {
            let map = match store {
                "portals" => &state.portals,
                "portal_products" => &state.portal_products,
                _ => return Err(missing("Store")),
            };
            let items: Vec<&Value> = map.values().collect();
            ok(json!({"Items": items}))
        })
    }

    fn delete_keyed(
        &self,
        id_opt: Option<&str>,
        store: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = id_opt.ok_or_else(|| missing("Id"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let map = match store {
            "portals" => &mut state.portals,
            "portal_products" => &mut state.portal_products,
            _ => return Err(missing("Store")),
        };
        // Portal / PortalProduct deletes are idempotent in the public API:
        // the Smithy operation declares only BadRequestException /
        // AccessDeniedException / TooManyRequestsException — no
        // NotFoundException. Real AWS returns success when the resource
        // is already absent, so we mirror that here.
        map.remove(id);
        no_content()
    }

    fn put_subresource(
        &self,
        req: &AwsRequest,
        parent_opt: Option<&str>,
        id_opt: Option<String>,
        store: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parent = parent_opt.ok_or_else(|| missing("Parent"))?.to_string();
        let id = id_opt.unwrap_or_else(rand_id);
        let input = body(req);
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let entry = match store {
            "product_pages" => {
                // DisplayContent requires Title + Body (both required).
                let mut dc = input
                    .get("DisplayContent")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if dc.get("title").and_then(|v| v.as_str()).is_none()
                    && dc.get("Title").and_then(|v| v.as_str()).is_none()
                {
                    dc["Title"] = json!(id.clone());
                }
                if dc.get("body").and_then(|v| v.as_str()).is_none()
                    && dc.get("Body").and_then(|v| v.as_str()).is_none()
                {
                    dc["Body"] = json!("");
                }
                // Derive a PageTitle for summary lookups (Smithy summary
                // requires pageTitle but the full response uses
                // DisplayContent.title — keep both on the entry so the
                // list view can project the summary shape).
                let page_title = dc
                    .get("title")
                    .or_else(|| dc.get("Title"))
                    .and_then(|v| v.as_str())
                    .unwrap_or(&id)
                    .to_string();
                json!({
                    "ProductPageId": id,
                    "ProductPageArn": format!(
                        "arn:aws:apigateway:us-east-1::/portalproducts/{}/productpages/{}",
                        parent, id
                    ),
                    // Internal-only: the summary shape requires pageTitle
                    // but Create/Update responses don't carry it. We
                    // strip this before returning the response below.
                    "_summary_pageTitle": page_title,
                    "LastModified": now,
                    "DisplayContent": dc,
                })
            }
            "product_rest_endpoint_pages" => {
                // EndpointDisplayContentResponse has only Body, Endpoint,
                // OperationName. Input may carry EndpointDisplayContent
                // (None, Overrides) shape — translate to the response
                // shape rather than echoing the input.
                let input_dc = input.get("DisplayContent").cloned().unwrap_or(json!({}));
                let mut dc = json!({});
                for (out_key, in_keys) in &[
                    ("Endpoint", &["endpoint", "Endpoint"][..]),
                    ("Body", &["body", "Body"][..]),
                    ("OperationName", &["operationName", "OperationName"][..]),
                ] {
                    for in_key in *in_keys {
                        if let Some(v) = input_dc.get(*in_key) {
                            if !v.is_null() {
                                dc[*out_key] = v.clone();
                                break;
                            }
                        }
                    }
                }
                if dc.get("Endpoint").and_then(|v| v.as_str()).is_none() {
                    dc["Endpoint"] = json!(id.clone());
                }
                let rei = input
                    .get("RestEndpointIdentifier")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                json!({
                    "ProductRestEndpointPageId": id,
                    "ProductRestEndpointPageArn": format!(
                        "arn:aws:apigateway:us-east-1::/portalproducts/{}/productrestendpointpages/{}",
                        parent, id
                    ),
                    // Internal-only: summary shape requires endpoint at
                    // root but Create/Update responses don't carry it.
                    "_summary_endpoint": id.clone(),
                    "Status": "AVAILABLE",
                    "LastModified": now,
                    "DisplayContent": dc,
                    "StatusException": json!({}),
                    "TryItState": "DISABLED",
                    "RestEndpointIdentifier": rei,
                })
            }
            _ => return Err(missing("Store")),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let map = match store {
            "product_pages" => &mut state.product_pages,
            "product_rest_endpoint_pages" => &mut state.product_rest_endpoint_pages,
            _ => return Err(missing("Store")),
        };
        map.entry(parent).or_default().insert(id, entry.clone());
        // Strip summary-only fields that live in storage but aren't
        // part of the Create/Update response shape.
        let mut response = entry.clone();
        if let Value::Object(ref mut obj) = response {
            obj.retain(|k, _| !k.starts_with("_summary_"));
        }
        ok(response)
    }

    fn get_subresource(
        &self,
        parent_opt: Option<&str>,
        id_opt: Option<&str>,
        store: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parent = parent_opt.ok_or_else(|| missing("Parent"))?;
        let id = id_opt.ok_or_else(|| missing("Id"))?;
        self.read_state(account_id, region, |state| {
            let map = match store {
                "product_pages" => &state.product_pages,
                "product_rest_endpoint_pages" => &state.product_rest_endpoint_pages,
                _ => return Err(missing("Store")),
            };
            map.get(parent)
                .and_then(|m| m.get(id))
                .cloned()
                .map(|mut v| {
                    if let Value::Object(ref mut obj) = v {
                        obj.retain(|k, _| !k.starts_with("_summary_"));
                    }
                    ok(v)
                })
                .unwrap_or_else(|| Err(not_found(store, id)))
        })
    }

    fn list_subresources(
        &self,
        parent_opt: Option<&str>,
        store: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parent = parent_opt.ok_or_else(|| missing("Parent"))?;
        self.read_state(account_id, region, |state| {
            let map = match store {
                "product_pages" => &state.product_pages,
                "product_rest_endpoint_pages" => &state.product_rest_endpoint_pages,
                _ => return Err(missing("Store")),
            };
            // Project each stored entry into its Summary shape per Smithy
            // (ProductPageSummaryNoBody / ProductRestEndpointPageSummaryNoBody)
            // so the list output doesn't carry DisplayContent / other
            // body-only fields.
            let items: Vec<Value> = map
                .get(parent)
                .map(|m| {
                    m.values()
                        .map(|v| match store {
                            "product_pages" => json!({
                                "ProductPageId": v.get("ProductPageId").cloned().unwrap_or(json!("")),
                                "ProductPageArn": v.get("ProductPageArn").cloned().unwrap_or(json!("")),
                                "PageTitle": v.get("_summary_pageTitle")
                                    .or_else(|| v.get("PageTitle"))
                                    .cloned()
                                    .unwrap_or(json!("")),
                                "LastModified": v.get("LastModified").cloned().unwrap_or(json!("")),
                            }),
                            "product_rest_endpoint_pages" => json!({
                                "ProductRestEndpointPageId": v.get("ProductRestEndpointPageId").cloned().unwrap_or(json!("")),
                                "ProductRestEndpointPageArn": v.get("ProductRestEndpointPageArn").cloned().unwrap_or(json!("")),
                                "Endpoint": v.get("_summary_endpoint")
                                    .or_else(|| v.get("Endpoint"))
                                    .cloned()
                                    .unwrap_or(json!("")),
                                "Status": v.get("Status").cloned().unwrap_or(json!("AVAILABLE")),
                                "TryItState": v.get("TryItState").cloned().unwrap_or(json!("DISABLED")),
                                "LastModified": v.get("LastModified").cloned().unwrap_or(json!("")),
                                "RestEndpointIdentifier": v.get("RestEndpointIdentifier").cloned().unwrap_or(json!({})),
                            }),
                            _ => v.clone(),
                        })
                        .collect()
                })
                .unwrap_or_default();
            ok(json!({"Items": items}))
        })
    }

    fn delete_subresource(
        &self,
        parent_opt: Option<&str>,
        id_opt: Option<&str>,
        store: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let parent = parent_opt.ok_or_else(|| missing("Parent"))?;
        let id = id_opt.ok_or_else(|| missing("Id"))?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let map = match store {
            "product_pages" => &mut state.product_pages,
            "product_rest_endpoint_pages" => &mut state.product_rest_endpoint_pages,
            _ => return Err(missing("Store")),
        };
        let removed = map.get_mut(parent).and_then(|m| m.remove(id)).is_some();
        if !removed {
            return Err(not_found(store, id));
        }
        no_content()
    }

    fn read_state<F, R>(&self, account_id: &str, region: &str, f: F) -> R
    where
        F: FnOnce(&ApiGatewayV2State) -> R,
    {
        let accounts = self.state.read();
        let empty = ApiGatewayV2State::new(account_id, region);
        let state = accounts.get(account_id).unwrap_or(&empty);
        f(state)
    }

    fn region_for(&self, account_id: &str) -> String {
        let accounts = self.state.read();
        accounts
            .get(account_id)
            .map(|s| s.region.clone())
            .unwrap_or_else(|| "us-east-1".to_string())
    }
}

#[cfg(test)]
mod tests {
    use crate::service::ApiGatewayV2Service;
    use crate::state::{ApiGatewayV2State, SharedApiGatewayV2State};
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::Method;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn svc() -> ApiGatewayV2Service {
        let state: SharedApiGatewayV2State =
            Arc::new(RwLock::new(MultiAccountState::<ApiGatewayV2State>::new(
                "000000000000",
                "us-east-1",
                "",
            )));
        ApiGatewayV2Service::new(state)
    }

    fn req(action: &str, body: &str, segs: &[&str]) -> AwsRequest {
        req_with_query(action, body, segs, &[])
    }

    fn req_with_query(
        action: &str,
        body: &str,
        segs: &[&str],
        query: &[(&str, &str)],
    ) -> AwsRequest {
        let mut qp = HashMap::new();
        for (k, v) in query {
            qp.insert((*k).to_string(), (*v).to_string());
        }
        // Mirror real dispatch: the raw query string carries every pair
        // (including repeated `@httpQuery` list keys), which handlers parse for
        // list params. `query_params` alone collapses repeats.
        let raw_query = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        AwsRequest {
            service: "apigatewayv2".to_string(),
            method: Method::POST,
            raw_path: format!("/{}", segs.join("/")),
            raw_query,
            path_segments: segs.iter().map(|s| s.to_string()).collect(),
            query_params: qp,
            headers: http::HeaderMap::new(),
            body: bytes::Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "000000000000".to_string(),
            region: "us-east-1".to_string(),
            request_id: "rid".to_string(),
            action: action.to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn run(
        s: &ApiGatewayV2Service,
        action: &str,
        body: &str,
        segs: &[&str],
        api_id: Option<&str>,
        resource_id: Option<&str>,
    ) {
        let r = s.handle_extra_action(action, &req(action, body, segs), api_id, resource_id);
        match r {
            Ok(resp) => assert!(resp.status.is_success(), "{action} status: {}", resp.status),
            Err(e) => panic!("{action} failed: {e:?}"),
        }
    }

    fn ok(
        action: &str,
        body: &str,
        segs: &[&str],
        api_id: Option<&str>,
        resource_id: Option<&str>,
    ) {
        run(&svc(), action, body, segs, api_id, resource_id);
    }

    #[test]
    fn domain_names_and_api_mappings() {
        let s = svc();
        run(
            &s,
            "CreateDomainName",
            r#"{"DomainName":"example.com"}"#,
            &["v2", "domainnames"],
            None,
            None,
        );
        run(
            &s,
            "GetDomainName",
            "",
            &["v2", "domainnames", "example.com"],
            None,
            Some("example.com"),
        );
        run(
            &s,
            "UpdateDomainName",
            "{}",
            &["v2", "domainnames", "example.com"],
            None,
            Some("example.com"),
        );
        run(&s, "GetDomainNames", "", &["v2", "domainnames"], None, None);
        run(
            &s,
            "CreateApiMapping",
            r#"{"ApiId":"a1","Stage":"prod"}"#,
            &["v2", "domainnames", "example.com", "apimappings"],
            None,
            Some("example.com"),
        );
        run(
            &s,
            "GetApiMappings",
            "",
            &["v2", "domainnames", "example.com", "apimappings"],
            None,
            Some("example.com"),
        );
        run(
            &s,
            "DeleteDomainName",
            "",
            &["v2", "domainnames", "example.com"],
            None,
            Some("example.com"),
        );
    }

    #[test]
    fn vpc_links_routing_rules_tags_portals() {
        let s = svc();
        run(
            &s,
            "CreateVpcLink",
            r#"{"Name":"l","SubnetIds":["s-1"]}"#,
            &["v2", "vpclinks"],
            None,
            None,
        );
        run(&s, "GetVpcLinks", "", &["v2", "vpclinks"], None, None);
        run(
            &s,
            "CreateRoutingRule",
            r#"{"Actions":[],"Conditions":[],"Priority":1}"#,
            &["v2", "domainnames", "d", "routingrules"],
            None,
            Some("d"),
        );
        run(
            &s,
            "ListRoutingRules",
            "",
            &["v2", "domainnames", "d", "routingrules"],
            None,
            Some("d"),
        );
        run(
            &s,
            "TagResource",
            r#"{"Tags":{"k":"v"}}"#,
            &["v2", "tags", "arn"],
            None,
            Some("arn"),
        );
        run(&s, "GetTags", "", &["v2", "tags", "arn"], None, Some("arn"));
        {
            // Seed a tag then untag via query params to match the Smithy
            // @httpQuery("tagKeys") binding.
            let r = req_with_query(
                "UntagResource",
                "",
                &["v2", "tags", "arn"],
                &[("tagKeys", "k")],
            );
            s.handle_extra_action("UntagResource", &r, None, Some("arn"))
                .expect("UntagResource");
        }
        run(
            &s,
            "CreatePortal",
            r#"{"Authorization":{},"EndpointConfiguration":{},"PortalContent":{}}"#,
            &["v2", "portals"],
            None,
            Some("p"),
        );
        run(
            &s,
            "GetPortal",
            "",
            &["v2", "portals", "p"],
            None,
            Some("p"),
        );
        run(&s, "ListPortals", "", &["v2", "portals"], None, None);
        run(
            &s,
            "DisablePortal",
            "",
            &["v2", "portals", "p", "disable"],
            None,
            Some("p"),
        );
        run(
            &s,
            "PreviewPortal",
            "",
            &["v2", "portals", "p", "preview"],
            None,
            Some("p"),
        );
        run(
            &s,
            "PublishPortal",
            "",
            &["v2", "portals", "p", "publish"],
            None,
            Some("p"),
        );
        run(
            &s,
            "CreatePortalProduct",
            r#"{"DisplayName":"pp"}"#,
            &["v2", "portalproducts"],
            None,
            Some("pp"),
        );
        run(
            &s,
            "GetPortalProduct",
            "",
            &["v2", "portalproducts", "pp"],
            None,
            Some("pp"),
        );
        run(
            &s,
            "ListPortalProducts",
            "",
            &["v2", "portalproducts"],
            None,
            None,
        );
        run(
            &s,
            "PutPortalProductSharingPolicy",
            r#"{"PolicyDocument":"{}"}"#,
            &["v2", "portalproducts", "pp", "sharing-policy"],
            None,
            Some("pp"),
        );
        run(
            &s,
            "GetPortalProductSharingPolicy",
            "",
            &["v2", "portalproducts", "pp", "sharing-policy"],
            None,
            Some("pp"),
        );
        run(
            &s,
            "DeletePortalProductSharingPolicy",
            "",
            &["v2", "portalproducts", "pp", "sharing-policy"],
            None,
            Some("pp"),
        );
    }

    #[test]
    fn models_and_responses() {
        ok(
            "CreateModel",
            r#"{"Name":"m","Schema":"{}"}"#,
            &["v2", "apis", "a1", "models"],
            Some("a1"),
            None,
        );
        ok(
            "GetModels",
            "",
            &["v2", "apis", "a1", "models"],
            Some("a1"),
            None,
        );
        {
            // GetModelTemplate now derives a template from a real stored
            // model schema, so seed one first.
            let s = svc();
            run(
                &s,
                "CreateModel",
                r#"{"Name":"m","Schema":"{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}"}"#,
                &["v2", "apis", "a1", "models"],
                Some("a1"),
                None,
            );
            let model_id = {
                let accounts = s.state.read();
                accounts
                    .get("000000000000")
                    .and_then(|st| st.models.get("a1"))
                    .and_then(|m| m.keys().next().cloned())
                    .expect("model seeded")
            };
            let resp = s
                .handle_extra_action(
                    "GetModelTemplate",
                    &req(
                        "GetModelTemplate",
                        "",
                        &["v2", "apis", "a1", "models", &model_id, "template"],
                    ),
                    Some("a1"),
                    Some(&model_id),
                )
                .expect("GetModelTemplate");
            let b: serde_json::Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            let tmpl: serde_json::Value =
                serde_json::from_str(b["value"].as_str().unwrap()).unwrap();
            assert_eq!(tmpl["id"], "");
        }
        ok(
            "CreateIntegrationResponse",
            r#"{"IntegrationResponseKey":"$default"}"#,
            &[
                "v2",
                "apis",
                "a1",
                "integrations",
                "i1",
                "integrationresponses",
            ],
            Some("a1"),
            Some("i1"),
        );
        ok(
            "GetIntegrationResponses",
            "",
            &[
                "v2",
                "apis",
                "a1",
                "integrations",
                "i1",
                "integrationresponses",
            ],
            Some("a1"),
            Some("i1"),
        );
        ok(
            "CreateRouteResponse",
            r#"{"RouteResponseKey":"$default"}"#,
            &["v2", "apis", "a1", "routes", "r1", "routeresponses"],
            Some("a1"),
            Some("r1"),
        );
        ok(
            "GetRouteResponses",
            "",
            &["v2", "apis", "a1", "routes", "r1", "routeresponses"],
            Some("a1"),
            Some("r1"),
        );
    }

    #[test]
    fn import_export_cleanup() {
        // A minimal-but-real OpenAPI document. ImportApi/ReimportApi now
        // parse this and synthesize routes rather than echoing a constant.
        let spec = r#"{"openapi":"3.0.1","info":{"title":"t","version":"1"},"paths":{"/p":{"get":{"responses":{"200":{"description":"ok"}}}}}}"#;

        // ImportApi creates a real API on a fresh service.
        {
            let s = svc();
            let import_body = serde_json::json!({ "Body": spec }).to_string();
            let resp = s
                .handle_extra_action(
                    "ImportApi",
                    &req("ImportApi", &import_body, &["v2", "apis"]),
                    None,
                    None,
                )
                .expect("ImportApi");
            let b: serde_json::Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            let api = b["apiId"].as_str().unwrap().to_string();
            // The synthesized route is readable back.
            assert!(s
                .state
                .read()
                .get("000000000000")
                .and_then(|st| st.routes.get(&api))
                .map(|r| r.values().any(|rt| rt.route_key == "GET /p"))
                .unwrap_or(false));
        }

        // ExportApi / ReimportApi / cleanup ops all operate on a single
        // service seeded with a real API + stage.
        let s = svc();
        let api = {
            let import_body = serde_json::json!({ "Body": spec }).to_string();
            let resp = s
                .handle_extra_action(
                    "ImportApi",
                    &req("ImportApi", &import_body, &["v2", "apis"]),
                    None,
                    None,
                )
                .expect("ImportApi");
            let b: serde_json::Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            b["apiId"].as_str().unwrap().to_string()
        };

        // Attach a CORS config + a stage with access log settings so the
        // cleanup ops have something real to clear.
        {
            let mut accounts = s.state.write();
            let state = accounts.get_or_create("000000000000");
            if let Some(a) = state.apis.get_mut(&api) {
                a.cors_configuration = Some(crate::state::CorsConfiguration {
                    allow_credentials: None,
                    allow_headers: None,
                    allow_methods: Some(vec!["GET".to_string()]),
                    allow_origins: Some(vec!["*".to_string()]),
                    expose_headers: None,
                    max_age: None,
                });
            }
            state.stages.entry(api.clone()).or_default().insert(
                "prod".to_string(),
                crate::state::Stage {
                    stage_name: "prod".to_string(),
                    description: None,
                    deployment_id: None,
                    auto_deploy: false,
                    created_date: chrono::Utc::now(),
                    last_updated_date: None,
                    web_acl_arn: None,
                    stage_variables: None,
                    access_log_settings: Some(crate::state::AccessLogSettings {
                        destination_arn: "arn:aws:logs:us-east-1:0:log-group:g:*".to_string(),
                        format: None,
                    }),
                    client_certificate_id: None,
                    default_route_settings: None,
                    route_settings: None,
                    tags: None,
                },
            );
        }

        // ReimportApi replaces the surface of the existing API.
        {
            let spec2 = r#"{"openapi":"3.0.1","info":{"title":"t2","version":"2"},"paths":{"/q":{"get":{"responses":{"200":{"description":"ok"}}}}}}"#;
            let reimport_body = serde_json::json!({ "Body": spec2 }).to_string();
            run(
                &s,
                "ReimportApi",
                &reimport_body,
                &["v2", "apis", &api],
                Some(&api),
                None,
            );
            assert!(s
                .state
                .read()
                .get("000000000000")
                .and_then(|st| st.routes.get(&api))
                .map(|r| r.values().all(|rt| rt.route_key == "GET /q"))
                .unwrap_or(false));
        }

        // ExportApi generates a real document.
        {
            let r = req_with_query(
                "ExportApi",
                "",
                &["v2", "apis", &api, "exports", "OAS30"],
                &[("outputType", "JSON")],
            );
            let resp = s
                .handle_extra_action("ExportApi", &r, Some(&api), Some("OAS30"))
                .expect("ExportApi");
            let b: serde_json::Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
            let doc: serde_json::Value = serde_json::from_str(b["body"].as_str().unwrap()).unwrap();
            assert_eq!(doc["openapi"], "3.0.1");
            assert!(doc["paths"]["/q"].get("get").is_some());
        }

        // DeleteCorsConfiguration clears the config.
        run(
            &s,
            "DeleteCorsConfiguration",
            "",
            &["v2", "apis", &api, "cors"],
            Some(&api),
            None,
        );
        assert!(s
            .state
            .read()
            .get("000000000000")
            .and_then(|st| st.apis.get(&api))
            .map(|a| a.cors_configuration.is_none())
            .unwrap_or(false));

        // DeleteAccessLogSettings clears the stage settings.
        run(
            &s,
            "DeleteAccessLogSettings",
            "",
            &["v2", "apis", &api, "stages", "prod", "accesslogsettings"],
            Some(&api),
            Some("prod"),
        );
        assert!(s
            .state
            .read()
            .get("000000000000")
            .and_then(|st| st.stages.get(&api))
            .and_then(|m| m.get("prod"))
            .map(|st| st.access_log_settings.is_none())
            .unwrap_or(false));

        // DeleteRouteRequestParameter now actually removes the key from the
        // route's request_parameters (1.22). Seed a route with two params,
        // delete one, confirm only that one is gone.
        {
            let mut accounts = s.state.write();
            let state = accounts.get_or_create("000000000000");
            state.routes.entry(api.clone()).or_default().insert(
                "r1".to_string(),
                crate::state::Route {
                    route_id: "r1".to_string(),
                    route_key: "GET /p".to_string(),
                    target: None,
                    authorization_type: None,
                    authorizer_id: None,
                    api_key_required: None,
                    authorization_scopes: None,
                    model_selection_expression: None,
                    operation_name: None,
                    request_models: None,
                    request_parameters: Some(
                        [
                            (
                                "route.request.querystring.id".to_string(),
                                serde_json::json!({"required": true}),
                            ),
                            (
                                "route.request.header.x".to_string(),
                                serde_json::json!({"required": false}),
                            ),
                        ]
                        .into_iter()
                        .collect(),
                    ),
                    route_response_selection_expression: None,
                },
            );
            state
                .stages
                .get_mut(&api)
                .and_then(|m| m.get_mut("prod"))
                .unwrap()
                .route_settings = Some(
                [
                    (
                        "GET /p".to_string(),
                        serde_json::json!({"throttlingBurstLimit": 5}),
                    ),
                    (
                        "POST /p".to_string(),
                        serde_json::json!({"throttlingBurstLimit": 7}),
                    ),
                ]
                .into_iter()
                .collect(),
            );
        }
        run(
            &s,
            "DeleteRouteRequestParameter",
            "",
            &[
                "v2",
                "apis",
                &api,
                "routes",
                "r1",
                "requestparameters",
                "route.request.querystring.id",
            ],
            Some(&api),
            Some("r1"),
        );
        assert!(s
            .state
            .read()
            .get("000000000000")
            .and_then(|st| st.routes.get(&api))
            .and_then(|m| m.get("r1"))
            .map(|r| {
                let p = r.request_parameters.as_ref().unwrap();
                !p.contains_key("route.request.querystring.id")
                    && p.contains_key("route.request.header.x")
            })
            .unwrap_or(false));
        run(
            &s,
            "DeleteRouteSettings",
            "",
            &[
                "v2",
                "apis",
                &api,
                "stages",
                "prod",
                "routesettings",
                "GET %2Fp",
            ],
            Some(&api),
            Some("prod"),
        );
        assert!(s
            .state
            .read()
            .get("000000000000")
            .and_then(|st| st.stages.get(&api))
            .and_then(|m| m.get("prod"))
            .map(|st| {
                let rs = st.route_settings.as_ref().unwrap();
                !rs.contains_key("GET /p") && rs.contains_key("POST /p")
            })
            .unwrap_or(false));
        // DeleteDeployment now validates + removes. Seed one, delete it,
        // confirm it's gone, and that a second delete 404s (1.23).
        {
            let s = svc();
            {
                let mut accounts = s.state.write();
                let state = accounts.get_or_create("000000000000");
                state
                    .deployments
                    .entry("a1".to_string())
                    .or_default()
                    .insert(
                        "d1".to_string(),
                        crate::state::Deployment {
                            deployment_id: "d1".to_string(),
                            description: None,
                            created_date: chrono::Utc::now(),
                            auto_deployed: false,
                            deployment_status: "DEPLOYED".to_string(),
                        },
                    );
            }
            run(
                &s,
                "DeleteDeployment",
                "",
                &["v2", "apis", "a1", "deployments", "d1"],
                Some("a1"),
                Some("d1"),
            );
            assert!(
                s.state
                    .read()
                    .get("000000000000")
                    .and_then(|st| st.deployments.get("a1"))
                    .map(|d| !d.contains_key("d1"))
                    .unwrap_or(true),
                "DeleteDeployment must remove the deployment"
            );
            // Deleting the now-missing deployment is a NotFound.
            assert!(s
                .handle_extra_action(
                    "DeleteDeployment",
                    &req(
                        "DeleteDeployment",
                        "",
                        &["v2", "apis", "a1", "deployments", "d1"],
                    ),
                    Some("a1"),
                    Some("d1"),
                )
                .is_err());
        }
        // UpdateDeployment now validates the deployment exists. Seed one
        // into a fresh service and exercise the patch on the real entry.
        {
            let s = svc();
            {
                let mut accounts = s.state.write();
                let state = accounts.get_or_create("000000000000");
                state
                    .deployments
                    .entry("a1".to_string())
                    .or_default()
                    .insert(
                        "d1".to_string(),
                        crate::state::Deployment {
                            deployment_id: "d1".to_string(),
                            description: None,
                            created_date: chrono::Utc::now(),
                            auto_deployed: false,
                            deployment_status: "DEPLOYED".to_string(),
                        },
                    );
            }
            run(
                &s,
                "UpdateDeployment",
                "{}",
                &["v2", "apis", "a1", "deployments", "d1"],
                Some("a1"),
                Some("d1"),
            );
        }
        ok(
            "ResetAuthorizersCache",
            "",
            &["v2", "apis", "a1", "stages", "prod", "cache", "authorizers"],
            Some("a1"),
            Some("prod"),
        );
    }

    #[test]
    fn portal_lifecycle_reflected_on_get() {
        // Disable / Publish / Preview persist state and GetPortal
        // reflects it instead of no-op'ing (1.12).
        let s = svc();
        s.handle_extra_action(
            "CreatePortal",
            &req(
                "CreatePortal",
                r#"{"Authorization":{},"EndpointConfiguration":{},"PortalContent":{}}"#,
                &["v2", "portals"],
            ),
            None,
            Some("p"),
        )
        .expect("CreatePortal");

        let get = |s: &ApiGatewayV2Service| -> serde_json::Value {
            let resp = s
                .handle_extra_action(
                    "GetPortal",
                    &req("GetPortal", "", &["v2", "portals", "p"]),
                    None,
                    Some("p"),
                )
                .expect("GetPortal");
            serde_json::from_slice(resp.body.expect_bytes()).unwrap()
        };

        // Fresh portal starts UNPUBLISHED. Response keys are camelCased.
        assert_eq!(get(&s)["publishStatus"].as_str(), Some("UNPUBLISHED"));

        // Publish -> PUBLISHED + description recorded.
        s.handle_extra_action(
            "PublishPortal",
            &req(
                "PublishPortal",
                r#"{"Description":"v1 release"}"#,
                &["v2", "portals", "p", "publish"],
            ),
            None,
            Some("p"),
        )
        .expect("PublishPortal");
        let body = get(&s);
        assert_eq!(body["publishStatus"].as_str(), Some("PUBLISHED"));
        assert_eq!(
            body["lastPublishedDescription"].as_str(),
            Some("v1 release")
        );

        // Preview -> Preview block populated.
        s.handle_extra_action(
            "PreviewPortal",
            &req("PreviewPortal", "", &["v2", "portals", "p", "preview"]),
            None,
            Some("p"),
        )
        .expect("PreviewPortal");
        assert_eq!(
            get(&s)["preview"]["previewStatus"].as_str(),
            Some("PREVIEW_AVAILABLE")
        );

        // Disable -> DISABLED.
        s.handle_extra_action(
            "DisablePortal",
            &req("DisablePortal", "", &["v2", "portals", "p", "disable"]),
            None,
            Some("p"),
        )
        .expect("DisablePortal");
        assert_eq!(get(&s)["publishStatus"].as_str(), Some("DISABLED"));
    }
}
