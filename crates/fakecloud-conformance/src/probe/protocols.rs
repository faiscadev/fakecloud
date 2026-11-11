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
    let op = model.and_then(|m| {
        m.operations
            .iter()
            .find(|o| o.name == operation_name)
            .map(|op| (m, op))
    });
    let (method, url, headers, body) = match op.and_then(|(m, op)| {
        let input = with_omitted_label_sentinels(service_name, op, m, &variant.input);
        build_http_request_from_model(op, m, &input)
    }) {
        Some((m, path_and_query, hdrs, body)) => {
            (m, format!("{}{}", endpoint, path_and_query), hdrs, body)
        }
        // No model, or an operation without `@http` metadata: there is no
        // route to build, so send the input to the service root and let the
        // classifier judge whatever comes back.
        None => (
            reqwest::Method::POST,
            format!("{}/", endpoint),
            Vec::new(),
            Some(serde_json::to_vec(&variant.input).unwrap_or_else(|_| b"{}".to_vec())),
        ),
    };

    let mut req = client
        .request(method.clone(), &url)
        .header("Authorization", sigv4_auth_header(service_name));

    for (name, value) in &headers {
        req = req.header(name.as_str(), value.as_str());
    }

    // Trust the builder to decide whether to emit a body: both
    // `build_http_request_from_model` only returns `Some(body)` when a body
    // is appropriate for this op + method (including
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
    // restJson1 names the error in `x-amzn-ErrorType`, which the SDKs read
    // before the body; Lambda's error bodies carry no code at all.
    let rest_json_error_type = resp
        .headers()
        .get("x-amzn-errortype")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
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
    let body = match rest_json_error_type {
        Some(error_type) if status >= 400 => with_rest_json_error_type(body, &error_type),
        _ => body,
    };
    Ok((status, body))
}

/// The classifier reads the error code from the body, so a restJson1 error
/// whose body has no `__type` / `code` gets the `x-amzn-ErrorType` header's
/// code (the part before any `:`) as `__type`, which is what the SDKs
/// resolve for it.
pub(super) fn with_rest_json_error_type(body: String, error_type: &str) -> String {
    let code = error_type.split(':').next().unwrap_or(error_type).trim();
    if code.is_empty() {
        return body;
    }
    let mut value = if body.trim().is_empty() {
        serde_json::Value::Object(serde_json::Map::new())
    } else {
        match serde_json::from_str::<serde_json::Value>(&body) {
            Ok(v @ serde_json::Value::Object(_)) => v,
            _ => return body,
        }
    };
    let obj = value.as_object_mut().expect("object checked above");
    if ["__type", "code", "Code"]
        .iter()
        .any(|k| obj.contains_key(*k))
    {
        return body;
    }
    obj.insert("__type".to_string(), serde_json::json!(code));
    value.to_string()
}

#[cfg(test)]
mod rest_json_error_type_tests {
    use super::with_rest_json_error_type;

    #[test]
    fn header_code_fills_a_body_without_one() {
        let body = r#"{"Type":"User","Message":"Function not found: f"}"#.to_string();
        let out: serde_json::Value = serde_json::from_str(&with_rest_json_error_type(
            body,
            "ResourceNotFoundException:http://internal.amazon.com/coral/",
        ))
        .unwrap();
        assert_eq!(out["__type"], "ResourceNotFoundException");
        assert_eq!(out["Message"], "Function not found: f");
        // A body that already names its code is left as it is.
        let named = r#"{"__type":"X","message":"m"}"#.to_string();
        assert_eq!(with_rest_json_error_type(named.clone(), "Y"), named);
    }
}
