//! API Gateway v2 handlers added to close the conformance gap. Domain
//! names + API mappings, models + integration/route responses, routing
//! rules, VPC links, tagging, portals + portal products, and import /
//! export / settings cleanup operations.

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::ApiGatewayV2Service;
use crate::state::ApiGatewayV2State;

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
                let mut entry = json!({
                    "DomainName": name,
                    "DomainNameArn": Arn::new("apigateway", "us-east-1", "", &format!("/domainnames/{name}")).to_string(),
                    "DomainNameConfigurations": body.get("DomainNameConfigurations").cloned().unwrap_or(json!([])),
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
                if let Some(cfgs) = body.get("DomainNameConfigurations") {
                    entry["DomainNameConfigurations"] = cfgs.clone();
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
                api_id.ok_or_else(|| missing("ApiId"))?;
                resource_id.ok_or_else(|| missing("ModelId"))?;
                ok(json!({"Value": "{}"}))
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
                let arn = resource_id
                    .ok_or_else(|| missing("ResourceArn"))?
                    .to_string();
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
                let arn = resource_id.ok_or_else(|| missing("ResourceArn"))?;
                // TagKeys is a required @httpQuery param per Smithy — the
                // SDK renders each entry as `tagKeys={key}`.
                let has_tag_keys = req
                    .query_params
                    .iter()
                    .any(|(k, _)| k == "tagKeys" || k.starts_with("tagKeys"));
                if !has_tag_keys {
                    return Err(missing("TagKeys"));
                }
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(aid);
                if let Some(tags) = state.tags.get_mut(arn) {
                    for (key, val) in &req.query_params {
                        if key.starts_with("tagKeys") {
                            tags.remove(val);
                        }
                    }
                }
                no_content()
            }
            "GetTags" => {
                let arn = resource_id.ok_or_else(|| missing("ResourceArn"))?;
                self.read_state(aid, &region, |state| {
                    let tags = state.tags.get(arn).cloned().unwrap_or_default();
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
                self.put_keyed(req, resource_id, "PortalId", "portals", aid)
            }
            "UpdatePortal" => {
                resource_id.ok_or_else(|| missing("PortalId"))?;
                let b = body(req);
                check_length(&b, "LogoUri", None, Some(1092))?;
                check_length(&b, "RumAppMonitorName", None, Some(255))?;
                self.put_keyed(req, resource_id, "PortalId", "portals", aid)
            }
            "GetPortal" => self.get_keyed(resource_id, "portals", aid, &region),
            "ListPortals" => self.list_keyed("portals", aid, &region),
            "DeletePortal" => self.delete_keyed(resource_id, "portals", aid),
            "DisablePortal" | "PreviewPortal" => {
                resource_id.ok_or_else(|| missing("PortalId"))?;
                empty_ok()
            }
            "PublishPortal" => {
                resource_id.ok_or_else(|| missing("PortalId"))?;
                let b = body(req);
                check_length(&b, "Description", None, Some(1024))?;
                empty_ok()
            }

            "CreatePortalProduct" => {
                let b = body(req);
                req_str(&b, "DisplayName")?;
                check_length(&b, "DisplayName", Some(1), Some(255))?;
                check_length(&b, "Description", None, Some(1024))?;
                self.put_keyed(req, resource_id, "PortalProductId", "portal_products", aid)
            }
            "UpdatePortalProduct" => {
                resource_id.ok_or_else(|| missing("PortalProductId"))?;
                let b = body(req);
                check_length(&b, "DisplayName", Some(1), Some(255))?;
                check_length(&b, "Description", None, Some(1024))?;
                self.put_keyed(req, resource_id, "PortalProductId", "portal_products", aid)
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
                req_str(&body, "Body")?;
                let api_id = format!("imported-{}", rand_id());
                ok(json!({
                    "ApiId": api_id,
                    "Name": "imported-api",
                    "ProtocolType": "HTTP",
                }))
            }
            "ReimportApi" => {
                let api = api_id.ok_or_else(|| missing("ApiId"))?;
                let body = body(req);
                req_str(&body, "Body")?;
                ok(json!({"ApiId": api, "Name": "reimported"}))
            }
            "ExportApi" => {
                let _api = api_id.ok_or_else(|| missing("ApiId"))?;
                // Specification is an httpLabel (segs[4]) — already filtered
                // to None for empty/placeholder path ids, so resource_id=None
                // here means the caller omitted it.
                let _spec = resource_id.ok_or_else(|| missing("Specification"))?;
                let output_type = req
                    .query_params
                    .iter()
                    .find(|(k, _)| *k == "outputType")
                    .map(|(_, v)| v.as_str());
                if output_type.is_none() {
                    return Err(missing("OutputType"));
                }
                ok(json!({"body": "openapi: 3.0.1\n"}))
            }

            // ── Cleanup ops ──
            "DeleteCorsConfiguration" => {
                api_id.ok_or_else(|| missing("ApiId"))?;
                no_content()
            }
            "DeleteAccessLogSettings" => {
                api_id.ok_or_else(|| missing("ApiId"))?;
                resource_id.ok_or_else(|| missing("StageName"))?;
                no_content()
            }
            "DeleteRouteRequestParameter" => {
                api_id.ok_or_else(|| missing("ApiId"))?;
                resource_id.ok_or_else(|| missing("RouteId"))?;
                // RequestParameterKey is segs[6]; enforce non-empty too.
                let key = segs.get(6).map(|s| s.as_str()).unwrap_or("");
                if !valid_path_id(key) {
                    return Err(missing("RequestParameterKey"));
                }
                no_content()
            }
            "DeleteRouteSettings" => {
                api_id.ok_or_else(|| missing("ApiId"))?;
                resource_id.ok_or_else(|| missing("StageName"))?;
                let key = segs.get(6).map(|s| s.as_str()).unwrap_or("");
                if !valid_path_id(key) {
                    return Err(missing("RouteKey"));
                }
                no_content()
            }
            "DeleteDeployment" => {
                api_id.ok_or_else(|| missing("ApiId"))?;
                resource_id.ok_or_else(|| missing("DeploymentId"))?;
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
        let mut entry = json!({
            "ModelId": id,
            "Name": body["Name"].as_str().unwrap_or(""),
            "Schema": body["Schema"].as_str().unwrap_or("{}"),
            "ContentType": body["ContentType"].as_str().unwrap_or("application/json"),
        });
        if let Some(desc) = body["Description"].as_str() {
            entry["Description"] = json!(desc);
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let bucket = state.models.entry(api).or_default();
        if !is_create && !bucket.contains_key(&id) {
            return Err(not_found("Model", &id));
        }
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
        let mut value = entry.clone();
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
        let bucket = store.entry(api).or_default();
        if !is_create && !bucket.contains_key(&key) {
            return Err(not_found("Response", &id));
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

    fn put_keyed(
        &self,
        req: &AwsRequest,
        id_opt: Option<&str>,
        id_field: &str,
        store: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = id_opt.map(String::from).unwrap_or_else(rand_id);
        let input = body(req);
        // Build response from scratch with Smithy-required fields. Don't
        // echo arbitrary input keys — probe variants send things like
        // `logoUri` that aren't on the response shape.
        let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
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
                    "PortalArn": Arn::new("apigateway", "us-east-1", "", &format!("/portals/{id}")).to_string(),
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
                "PortalProductArn": Arn::new("apigateway", "us-east-1", "", &format!("/portalproducts/{id}")).to_string(),
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
        AwsRequest {
            service: "apigatewayv2".to_string(),
            method: Method::POST,
            raw_path: format!("/{}", segs.join("/")),
            raw_query: String::new(),
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
        ok(
            "GetModelTemplate",
            "",
            &["v2", "apis", "a1", "models", "m1", "template"],
            Some("a1"),
            Some("m1"),
        );
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
        ok(
            "ImportApi",
            r#"{"Body":"openapi"}"#,
            &["v2", "apis"],
            None,
            None,
        );
        ok(
            "ReimportApi",
            r#"{"Body":"openapi"}"#,
            &["v2", "apis", "a1"],
            Some("a1"),
            None,
        );
        {
            // ExportApi requires @httpQuery("outputType") + path Specification.
            let s = svc();
            let r = req_with_query(
                "ExportApi",
                "",
                &["v2", "apis", "a1", "exports", "OAS30"],
                &[("outputType", "JSON")],
            );
            s.handle_extra_action("ExportApi", &r, Some("a1"), Some("OAS30"))
                .expect("ExportApi");
        }
        ok(
            "DeleteCorsConfiguration",
            "",
            &["v2", "apis", "a1", "cors"],
            Some("a1"),
            None,
        );
        ok(
            "DeleteAccessLogSettings",
            "",
            &["v2", "apis", "a1", "stages", "prod", "accesslogsettings"],
            Some("a1"),
            Some("prod"),
        );
        ok(
            "DeleteRouteRequestParameter",
            "",
            &["v2", "apis", "a1", "routes", "r1", "requestparameters", "p"],
            Some("a1"),
            Some("r1"),
        );
        ok(
            "DeleteRouteSettings",
            "",
            &["v2", "apis", "a1", "stages", "prod", "routesettings", "X"],
            Some("a1"),
            Some("prod"),
        );
        ok(
            "DeleteDeployment",
            "",
            &["v2", "apis", "a1", "deployments", "d1"],
            Some("a1"),
            Some("d1"),
        );
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
}
