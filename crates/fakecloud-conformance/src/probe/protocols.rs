//! probe `protocols` (audit-2026-05-19).

use super::*;

pub(super) fn probe_query(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
    model: Option<&crate::smithy::ServiceModel>,
) -> Result<(u16, String), String> {
    // Build form-encoded body with Action parameter
    let mut params = vec![("Action".to_string(), operation_name.to_string())];

    // EC2 speaks `ec2Query`, whose request encoding differs from awsQuery:
    // lists flatten as `Name.N` (not `Name.member.N`) and member wire names
    // come from `ec2QueryName`/`xmlName` (e.g. CreateTags's `Resources` member
    // is sent as `ResourceId.N`). This needs the model to resolve names, so we
    // walk the input shape tree when it is available; otherwise we fall back to
    // the generic awsQuery flattener.
    let ec2_encoded = if service_name == "ec2" {
        model
            .and_then(|m| m.operations.iter().find(|o| o.name == operation_name))
            .and_then(|op| op.input_shape.as_deref())
            .map(|input_shape| {
                if let Value::Object(_) = variant.input {
                    encode_ec2_query(&variant.input, input_shape, "", model.unwrap(), &mut params);
                }
            })
            .is_some()
    } else {
        false
    };

    // Flatten JSON input into form params (awsQuery default path)
    if !ec2_encoded {
        if let Value::Object(ref map) = variant.input {
            flatten_to_form_params(map, "", &mut params);
        }
    }

    let body = params
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoded(k), urlencoded(v)))
        .collect::<Vec<_>>()
        .join("&");

    let resp = client
        .post(endpoint)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Authorization", sigv4_auth_header(service_name))
        .body(body)
        .send()
        .map_err(|e| e.to_string())?;

    let status = resp.status().as_u16();
    let body = resp.text().map_err(|e| e.to_string())?;
    Ok((status, body))
}

/// Encode a value into `ec2Query` form params by walking the Smithy input shape
/// alongside the generated JSON. Lists flatten as `{prefix}.{N}` (1-based, no
/// `.member`); structure members use their `ec2QueryName` / `xmlName` / capitalized
/// member name as the wire segment. Scalars push `(prefix, value)`.
fn encode_ec2_query(
    value: &Value,
    shape_id: &str,
    prefix: &str,
    model: &crate::smithy::ServiceModel,
    params: &mut Vec<(String, String)>,
) {
    use crate::smithy::ShapeType;

    match model.shapes.get(shape_id).map(|s| &s.shape_type) {
        Some(ShapeType::Structure { members }) | Some(ShapeType::Union { members }) => {
            let Value::Object(map) = value else { return };
            for (key, child) in map {
                let Some(member) = members.iter().find(|m| &m.name == key) else {
                    continue;
                };
                let wire = ec2_member_wire_name(member);
                let child_prefix = if prefix.is_empty() {
                    wire
                } else {
                    format!("{prefix}.{wire}")
                };
                encode_ec2_query(child, &member.target, &child_prefix, model, params);
            }
        }
        Some(ShapeType::List { member_target }) => {
            let Value::Array(items) = value else { return };
            for (i, item) in items.iter().enumerate() {
                let child_prefix = format!("{prefix}.{}", i + 1);
                encode_ec2_query(item, member_target, &child_prefix, model, params);
            }
        }
        Some(ShapeType::Map {
            key_target,
            value_target,
        }) => {
            // EC2 maps serialize as `{prefix}.N.key` / `{prefix}.N.value`.
            let Value::Object(map) = value else { return };
            for (i, (k, v)) in map.iter().enumerate() {
                let entry = format!("{prefix}.{}", i + 1);
                params.push((format!("{entry}.key"), k.clone()));
                let _ = key_target;
                encode_ec2_query(v, value_target, &format!("{entry}.value"), model, params);
            }
        }
        // Scalars, prelude primitives (not in the shapes map), and enums all
        // serialize as a single leaf param.
        _ => {
            if let Some(s) = scalar_to_string(value) {
                params.push((prefix.to_string(), s));
            }
        }
    }
}

/// Resolve a member's `ec2Query` request wire name: `ec2QueryName`, else
/// `xmlName`, else the member name with its first letter capitalized.
fn ec2_member_wire_name(member: &crate::smithy::Member) -> String {
    if let Some(n) = &member.traits.ec2_query_name {
        return n.clone();
    }
    if let Some(n) = &member.traits.xml_name {
        return n.clone();
    }
    let mut chars = member.name.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

/// Render a JSON scalar as its form-param string. Returns `None` for
/// objects/arrays/null (handled structurally elsewhere).
fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Build a minimally-well-formed SigV4 Authorization header for probing.
///
/// fakecloud's service-routing layer parses this header to extract the
/// service name (region/service/aws4_request). The parser at
/// `fakecloud-aws::sigv4::parse_sigv4` requires `Credential=...` to be
/// terminated by a comma, which means the header must also carry
/// `SignedHeaders` and `Signature` — otherwise the parse returns `None`,
/// service detection fails, and the request falls through to API Gateway's
/// execute-api fallback (returning `404 NotFoundException "Stage not
/// specified"`). The signature value is irrelevant — fakecloud does not
/// verify SigV4 signatures, only parses the credential scope.
pub(super) fn sigv4_auth_header(service_name: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/{}/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=00",
        service_name
    )
}

pub(super) fn probe_json(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    target_prefix: &str,
    operation_name: &str,
    variant: &TestVariant,
) -> Result<(u16, String), String> {
    let target = format!("{}.{}", target_prefix, operation_name);
    let body = serde_json::to_string(&variant.input).unwrap_or_else(|_| "{}".to_string());

    let resp = client
        .post(endpoint)
        .header("Content-Type", "application/x-amz-json-1.1")
        .header("X-Amz-Target", &target)
        .header("Authorization", sigv4_auth_header("service"))
        .body(body)
        .send()
        .map_err(|e| e.to_string())?;

    let status = resp.status().as_u16();
    let body = resp.text().map_err(|e| e.to_string())?;
    Ok((status, body))
}

pub(super) fn probe_rest(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
    model: Option<&ServiceModel>,
) -> Result<(u16, String), String> {
    let (method, url, headers, body) = match model {
        Some(model) if !SERVICES_WITH_HARDCODED_REST.contains(&service_name) => {
            let op = model.operations.iter().find(|o| o.name == operation_name);
            match op.and_then(|op| build_http_request_from_model(op, model, &variant.input)) {
                Some((m, path_and_query, hdrs, body)) => {
                    let url = format!("{}{}", endpoint, path_and_query);
                    (m, url, hdrs, body)
                }
                None => legacy_rest_request(endpoint, service_name, operation_name, variant),
            }
        }
        _ => legacy_rest_request(endpoint, service_name, operation_name, variant),
    };

    // For services with a hand-curated route table (Lambda / S3) we
    // still want to honour the Smithy model's `@httpQuery` traits so
    // negative/boundary variants targeting query members reach the
    // server. The legacy builder only knows the path; bolt the query
    // string on here when the model is available.
    let url = if let Some(model) = model {
        if SERVICES_WITH_HARDCODED_REST.contains(&service_name) {
            append_http_query_from_model(&url, model, operation_name, &variant.input)
        } else {
            url
        }
    } else {
        url
    };

    let mut req = client
        .request(method.clone(), &url)
        .header("Authorization", sigv4_auth_header(service_name));

    for (name, value) in &headers {
        req = req.header(name.as_str(), value.as_str());
    }

    // Trust the builder to decide whether to emit a body: both
    // `build_http_request_from_model` and `legacy_rest_request` only return
    // `Some(body)` when a body is appropriate for this op + method (including
    // DELETE/GET with an explicit `@httpPayload` member, a case AWS models do
    // use — e.g. `DeleteObjects` via POST-with-payload, plus streaming
    // ingest-style APIs with payloads on non-POST methods).
    if let Some(body) = body {
        if !headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        {
            req = req.header("Content-Type", "application/json");
        }
        req = req.body(body);
    }

    let resp = req.send().map_err(|e| e.to_string())?;

    let status = resp.status().as_u16();
    // S3 (and other REST services) return error codes on HEAD responses via
    // headers because HTTP forbids a body on HEAD. The classifier only looks
    // at the body, so for HEAD requests we synthesize a minimal XML error
    // body from the `x-amz-error-code` header if present. This is what AWS
    // SDKs do internally — they reconstruct an Error shape from the headers
    // when the body is empty.
    let head_error_code = if method == reqwest::Method::HEAD {
        resp.headers()
            .get("x-amz-error-code")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string())
    } else {
        None
    };
    let body = resp.text().map_err(|e| e.to_string())?;
    let body = if body.is_empty() {
        if let Some(code) = head_error_code {
            format!("<Error><Code>{}</Code></Error>", code)
        } else {
            body
        }
    } else {
        body
    };
    Ok((status, body))
}
