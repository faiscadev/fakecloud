//! probe `protocols` (audit-2026-05-19).

use super::*;

pub(super) fn probe_query(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
) -> Result<(u16, String), String> {
    // Build form-encoded body with Action parameter
    let mut params = vec![("Action".to_string(), operation_name.to_string())];

    // Flatten JSON input into form params
    if let Value::Object(ref map) = variant.input {
        flatten_to_form_params(map, "", &mut params);
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
