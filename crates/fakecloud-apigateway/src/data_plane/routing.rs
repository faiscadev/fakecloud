//! apigateway data_plane `routing` concerns (audit-2026-05-19).

use super::*;

/// Resolve the header name an authorizer's `identitySource` points at.
/// AWS uses `method.request.header.<Name>` (e.g.
/// `method.request.header.Authorization`). Bare names also work for
/// callers that store just `Authorization`. Defaults to `Authorization`
/// when nothing was configured.
pub(super) fn header_name_from_identity_source(source: Option<&str>) -> String {
    let raw = source.unwrap_or("Authorization").trim();
    // `identitySource` may contain comma-separated entries; use the
    // first one (matches AWS's behaviour for primary-key caching).
    let first = raw.split(',').next().unwrap_or(raw).trim();
    if let Some(stripped) = first.strip_prefix("method.request.header.") {
        stripped.to_string()
    } else if first.is_empty() {
        "Authorization".to_string()
    } else {
        first.to_string()
    }
}

pub(super) fn extract_header_value(req: &AwsRequest, header_name: &str) -> Option<String> {
    req.headers
        .get(header_name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// Strip an optional `Bearer ` prefix from a TOKEN-authorizer header
/// value before forwarding the raw token to the Lambda. AWS leaves the
/// prefix in place, but Lambdas commonly receive it stripped too;
/// preserve verbatim to match real behaviour.
pub(super) fn raw_token(value: &str) -> &str {
    value
}

/// Match `template` (e.g. `/items/{id}/parts`) against `path_segments`
/// (e.g. `["items", "42", "parts"]`). Returns the path-parameter map on
/// match, or `None` if the path doesn't fit the template.
pub(super) fn match_resource_path(
    template: &str,
    path_segments: &[String],
) -> Option<BTreeMap<String, String>> {
    // Root resource: only an empty (or missing) remaining path matches.
    if template == "/" {
        return if path_segments.is_empty() {
            Some(BTreeMap::new())
        } else {
            None
        };
    }
    let template_segments: Vec<&str> = template.split('/').filter(|s| !s.is_empty()).collect();
    let mut params = BTreeMap::new();
    let mut t = 0;
    let mut p = 0;
    while t < template_segments.len() {
        let seg = template_segments[t];
        if seg.starts_with('{') && seg.ends_with('}') {
            let inner = seg.trim_start_matches('{').trim_end_matches('}');
            if let Some(name) = inner.strip_suffix('+') {
                // Greedy match — consume the remainder.
                if p >= path_segments.len() {
                    return None;
                }
                params.insert(name.to_string(), path_segments[p..].join("/"));
                return Some(params);
            }
            if p >= path_segments.len() {
                return None;
            }
            params.insert(inner.to_string(), path_segments[p].clone());
            p += 1;
        } else {
            if p >= path_segments.len() || path_segments[p] != seg {
                return None;
            }
            p += 1;
        }
        t += 1;
    }
    if p == path_segments.len() {
        Some(params)
    } else {
        None
    }
}

/// Pull the function ARN out of an AWS_PROXY integration URI of the
/// shape `arn:aws:apigateway:<region>:lambda:path/<api-version>/functions/<arn>/invocations`.
pub(super) fn extract_lambda_arn(uri: &str) -> Option<String> {
    if !uri.contains(":lambda:path/") {
        return None;
    }
    let prefix = uri.split("/functions/").nth(1)?;
    let arn = prefix.trim_end_matches("/invocations");
    Some(arn.to_string())
}

// ─── WAFv2 inspection ──────────────────────────────────────────────

/// Build the resource ARN that callers use when associating a WebACL
/// with an API Gateway v1 stage:
/// `arn:aws:apigateway:<region>::/restapis/<api>/stages/<stage>`.
pub(super) fn stage_resource_arn(region: &str, api_id: &str, stage_name: &str) -> String {
    format!("arn:aws:apigateway:{region}::/restapis/{api_id}/stages/{stage_name}",)
}
