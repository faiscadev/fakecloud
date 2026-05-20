//! apigateway data_plane `integrations` concerns (audit-2026-05-19).

use super::*;

pub(super) async fn http_proxy(
    req: &AwsRequest,
    integration: &Integration,
    body_override: Option<bytes::Bytes>,
) -> Result<AwsResponse, AwsServiceError> {
    let url = integration
        .uri
        .as_ref()
        .ok_or_else(|| bad_gateway("HTTP integration missing uri"))?;
    let method = match req.method {
        Method::GET => reqwest::Method::GET,
        Method::POST => reqwest::Method::POST,
        Method::PUT => reqwest::Method::PUT,
        Method::DELETE => reqwest::Method::DELETE,
        Method::PATCH => reqwest::Method::PATCH,
        Method::HEAD => reqwest::Method::HEAD,
        Method::OPTIONS => reqwest::Method::OPTIONS,
        _ => reqwest::Method::GET,
    };
    let client = reqwest::Client::new();
    let mut builder = client.request(method, url);
    for (k, v) in req.headers.iter() {
        if let Ok(s) = v.to_str() {
            builder = builder.header(k.as_str(), s);
        }
    }
    let body = body_override.as_ref().unwrap_or(&req.body);
    if !body.is_empty() {
        builder = builder.body(body.clone().to_vec());
    }
    let resp = builder
        .send()
        .await
        .map_err(|e| bad_gateway(format!("backend HTTP failure: {e}")))?;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let mut headers = http::HeaderMap::new();
    for (k, v) in resp.headers().iter() {
        if let (Ok(name), Ok(val)) = (
            http::HeaderName::from_bytes(k.as_str().as_bytes()),
            http::HeaderValue::from_bytes(v.as_bytes()),
        ) {
            // `append` preserves multi-value headers like multiple
            // `Set-Cookie` lines that the backend may emit.
            headers.append(name, val);
        }
    }
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let body = resp
        .bytes()
        .await
        .map_err(|e| bad_gateway(format!("backend body read failure: {e}")))?;
    Ok(AwsResponse {
        status,
        content_type,
        headers,
        body: bytes::Bytes::from(body.to_vec()).into(),
    })
}

/// Resolve a VPC_LINK integration by looking up the VpcLink's
/// `targetArns`, finding the first NLB/ALB that has a bound port in
/// the ELBv2 dataplane, and forwarding the request there.
pub(super) async fn vpc_link_proxy(
    req: &AwsRequest,
    integration: &Integration,
    service: &ApiGatewayService,
) -> Result<AwsResponse, AwsServiceError> {
    let connection_id = integration
        .connection_id
        .as_deref()
        .ok_or_else(|| bad_gateway("VPC_LINK integration missing connectionId"))?;

    let target_arns: Vec<String> = {
        let accounts = service.state_handle().read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| bad_gateway("VPC_LINK: account not found in API Gateway state"))?;
        let vpc_link = state
            .vpc_links
            .get(connection_id)
            .ok_or_else(|| bad_gateway(format!("VPC_LINK not found: {connection_id}")))?;
        let target_arns = vpc_link
            .get("targetArns")
            .and_then(|v| v.as_array())
            .ok_or_else(|| bad_gateway("VPC_LINK missing targetArns"))?;
        if target_arns.is_empty() {
            return Err(bad_gateway("VPC_LINK targetArns is empty"));
        }
        target_arns
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    };

    let elbv2 = service
        .elbv2_state
        .as_ref()
        .ok_or_else(|| bad_gateway("VPC_LINK not available: ELBv2 state not wired"))?;
    let port = {
        let elbv2_read = elbv2.read();
        let elbv2_account = elbv2_read
            .get(&req.account_id)
            .ok_or_else(|| bad_gateway("VPC_LINK: account not found in ELBv2 state"))?;
        let mut bound_port = None;
        for arn in &target_arns {
            if let Some(lb) = elbv2_account.load_balancers.get(arn) {
                if let Some(port) = lb.bound_port {
                    bound_port = Some(port);
                    break;
                }
            }
        }
        bound_port.ok_or_else(|| {
            bad_gateway("VPC_LINK: none of the target NLBs/ALBs have an active dataplane port")
        })?
    };

    // Build the backend URL from the integration URI, replacing the
    // host with the local ELBv2 dataplane endpoint.
    let original_url = integration.uri.as_deref().unwrap_or("http://localhost/");
    let path_and_query = http::Uri::try_from(original_url)
        .ok()
        .and_then(|u| u.path_and_query().map(|p| p.as_str().to_string()))
        .unwrap_or_else(|| req.raw_path.clone());
    let backend_url = format!("http://127.0.0.1:{port}{path_and_query}");

    let mut proxy_integration = integration.clone();
    proxy_integration.uri = Some(backend_url);
    http_proxy(req, &proxy_integration, None).await
}

/// Apply the integration's `requestTemplates` to the request body.
/// Returns `Some(transformed_body)` when a template matched the request
/// content type, or `None` to leave the body unchanged.
pub(super) fn apply_request_template(
    req: &AwsRequest,
    integration: &Integration,
    vtl_ctx: &mut crate::vtl::Context,
) -> Option<bytes::Bytes> {
    let content_type = req
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json");
    let normalized = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim();
    let template = integration.request_templates.get(normalized)?;
    let rendered = crate::vtl::render(template, vtl_ctx);
    Some(bytes::Bytes::from(rendered))
}

/// Apply the integration response's `responseTemplates` to the backend
/// response body. Returns a new `AwsResponse` with the rendered template.
#[allow(clippy::too_many_arguments)]
pub(super) async fn apply_response_template(
    backend_resp: AwsResponse,
    integration: &Integration,
    req: &AwsRequest,
    vtl_ctx: &mut crate::vtl::Context,
    api_id: &str,
    resource_path: &str,
    _stage_name: &str,
    _path_params: &BTreeMap<String, String>,
    _stage_vars: &BTreeMap<String, String>,
    service: &ApiGatewayService,
) -> Result<AwsResponse, AwsServiceError> {
    let status_code = backend_resp.status.as_u16().to_string();
    let key = response_key(
        api_id,
        resource_path,
        &integration.http_method,
        &status_code,
    );
    let accounts = service.state_handle().read();
    let state = accounts.get(&req.account_id);
    let resp_template = state.and_then(|st| {
        st.integration_responses
            .get(&key)
            .and_then(|v| v.get("responseTemplates"))
            .and_then(|t| t.as_object())
    });
    let content_type = backend_resp
        .content_type
        .as_str()
        .split(';')
        .next()
        .unwrap_or(&backend_resp.content_type)
        .trim();
    let template = resp_template
        .and_then(|t| t.get(content_type))
        .and_then(|v| v.as_str());
    let body = if let Some(template) = template {
        // Inject $input with the backend response body so the template
        // can reference it.
        let body_str = String::from_utf8_lossy(backend_resp.body.expect_bytes()).to_string();
        let body_json: Value = serde_json::from_str(&body_str).unwrap_or(Value::Null);
        vtl_ctx.set("input", json!({"body": body_str, "json": body_json}));
        crate::vtl::render(template, vtl_ctx).into_bytes()
    } else {
        backend_resp.body.expect_bytes().to_vec()
    };
    Ok(AwsResponse {
        status: backend_resp.status,
        content_type: backend_resp.content_type,
        headers: backend_resp.headers,
        body: bytes::Bytes::from(body).into(),
    })
}

/// Build a MOCK integration response by looking up the integration
/// response for the method and rendering its `responseTemplates`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn mock_response(
    req: &AwsRequest,
    integration: &Integration,
    vtl_ctx: &mut crate::vtl::Context,
    api_id: &str,
    resource_path: &str,
    _stage_name: &str,
    service: &ApiGatewayService,
) -> Result<AwsResponse, AwsServiceError> {
    let method = integration.http_method.as_str();
    // Default to 200 when no explicit status code is configured.
    let default_status = "200";
    let key = response_key(api_id, resource_path, method, default_status);
    let accounts = service.state_handle().read();
    let state = accounts.get(&req.account_id);
    // Try the 200 response first; if absent scan for any integration
    // response registered for this method and use its configured status.
    let resp_record = state.and_then(|st| {
        st.integration_responses.get(&key).or_else(|| {
            let prefix = format!("{api_id}/{resource_path}/{method}/");
            st.integration_responses
                .iter()
                .find(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v)
        })
    });
    let (status, resp_templates) = if let Some(record) = resp_record {
        let status = record
            .get("statusCode")
            .and_then(|v| v.as_str())
            .unwrap_or(default_status);
        let templates = record.get("responseTemplates").and_then(|v| v.as_object());
        (status, templates)
    } else {
        (default_status, None)
    };
    let status = status
        .parse::<u16>()
        .ok()
        .and_then(|n| StatusCode::from_u16(n).ok())
        .unwrap_or(StatusCode::OK);
    let content_type = "application/json";
    let body = if let Some(templates) = resp_templates {
        templates
            .get(content_type)
            .and_then(|v| v.as_str())
            .map(|t| crate::vtl::render(t, vtl_ctx))
            .unwrap_or_default()
    } else {
        String::new()
    };
    Ok(AwsResponse {
        status,
        content_type: content_type.to_string(),
        headers: http::HeaderMap::new(),
        body: bytes::Bytes::from(body).into(),
    })
}

/// Dispatch an `AWS` direct service integration to the corresponding
/// fakecloud service handler. The integration URI follows the API Gateway
/// format: `arn:aws:apigateway:{region}:{service}:action/{Action}` or
/// `arn:aws:apigateway:{region}:{service}:path/{path}`.
pub(super) async fn aws_direct_integration(
    req: &AwsRequest,
    uri: &str,
    service: &ApiGatewayService,
) -> Result<AwsResponse, AwsServiceError> {
    let registry = service.registry().ok_or_else(|| {
        bad_gateway("AWS direct integration not available: service registry not wired")
    })?;
    let registry = registry.get().ok_or_else(|| {
        bad_gateway("AWS direct integration not available: service registry not yet populated")
    })?;

    // Split only the first 6 segments — the trailing one is `action/...`
    // or `path/...` and can carry embedded `:` (e.g. Lambda ARNs). Naive
    // `split(':').collect()` truncated those at the first colon and
    // mis-routed the integration.
    let parts: Vec<&str> = uri.splitn(6, ':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "apigateway" {
        return Err(bad_gateway(format!(
            "AWS integration uri not in expected ARN format: {uri}"
        )));
    }
    let target_service = parts[4];
    let action_or_path = parts[5];

    let target = registry.get(target_service).ok_or_else(|| {
        bad_gateway(format!(
            "AWS integration target service '{target_service}' not registered"
        ))
    })?;

    let mut dispatch_req = AwsRequest {
        service: target_service.to_string(),
        action: req.action.clone(),
        region: req.region.clone(),
        account_id: req.account_id.clone(),
        request_id: uuid::Uuid::new_v4().to_string(),
        headers: req.headers.clone(),
        query_params: req.query_params.clone(),
        body: req.body.clone(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: req.path_segments.clone(),
        raw_path: req.raw_path.clone(),
        raw_query: req.raw_query.clone(),
        method: req.method.clone(),
        is_query_protocol: false,
        access_key_id: req.access_key_id.clone(),
        principal: req.principal.clone(),
    };

    if let Some(action) = action_or_path.strip_prefix("action/") {
        dispatch_req.action = action.to_string();
    } else if let Some(path) = action_or_path.strip_prefix("path/") {
        dispatch_req.raw_path = format!("/{path}");
        dispatch_req.path_segments = path.split('/').map(|s| s.to_string()).collect();
    } else {
        return Err(bad_gateway(format!(
            "AWS integration uri must contain action/ or path/ segment: {uri}"
        )));
    }

    target.handle(dispatch_req).await
}
