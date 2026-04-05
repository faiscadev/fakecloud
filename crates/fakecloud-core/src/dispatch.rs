use axum::body::Body;
use axum::extract::{Extension, Query};
use axum::http::{Request, StatusCode};
use axum::response::Response;
use std::collections::HashMap;
use std::sync::Arc;

use crate::protocol::{self, AwsProtocol};
use crate::registry::ServiceRegistry;
use crate::service::AwsRequest;

/// The main dispatch handler. All HTTP requests come through here.
pub async fn dispatch(
    Extension(registry): Extension<Arc<ServiceRegistry>>,
    Extension(config): Extension<Arc<DispatchConfig>>,
    Query(query_params): Query<HashMap<String, String>>,
    request: Request<Body>,
) -> Response<Body> {
    let request_id = uuid::Uuid::new_v4().to_string();

    let (parts, body) = request.into_parts();
    let body_bytes = match axum::body::to_bytes(body, 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(_) => {
            return build_error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "RequestEntityTooLarge",
                "Request body too large",
                &request_id,
                AwsProtocol::Query,
            );
        }
    };

    // Detect service and action
    let detected = match protocol::detect_service(&parts.headers, &query_params, &body_bytes) {
        Some(d) => d,
        None => {
            return build_error_response(
                StatusCode::BAD_REQUEST,
                "MissingAction",
                "Could not determine target service or action from request",
                &request_id,
                AwsProtocol::Query,
            );
        }
    };

    // Look up service
    let service = match registry.get(&detected.service) {
        Some(s) => s,
        None => {
            return build_error_response(
                detected.protocol.error_status(),
                "UnknownService",
                &format!("Service '{}' is not available", detected.service),
                &request_id,
                detected.protocol,
            );
        }
    };

    // Extract region from auth header, User-Agent, or use default
    let region = fakecloud_aws::sigv4::parse_sigv4(
        parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or(""),
    )
    .map(|info| info.region)
    .or_else(|| extract_region_from_user_agent(&parts.headers))
    .unwrap_or_else(|| config.region.clone());

    // Build path segments
    let path = parts.uri.path().to_string();
    let path_segments: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    // For JSON protocol, validate that non-empty bodies are valid JSON
    if detected.protocol == AwsProtocol::Json
        && !body_bytes.is_empty()
        && serde_json::from_slice::<serde_json::Value>(&body_bytes).is_err()
    {
        return build_error_response(
            StatusCode::BAD_REQUEST,
            "SerializationException",
            "Start of structure or map found where not expected",
            &request_id,
            AwsProtocol::Json,
        );
    }

    // Merge query params with form body params for Query protocol
    let mut all_params = query_params;
    if detected.protocol == AwsProtocol::Query {
        let body_params = protocol::parse_query_body(&body_bytes);
        for (k, v) in body_params {
            all_params.entry(k).or_insert(v);
        }
    }

    let aws_request = AwsRequest {
        service: detected.service.clone(),
        action: detected.action.clone(),
        region,
        account_id: config.account_id.clone(),
        request_id: request_id.clone(),
        headers: parts.headers,
        query_params: all_params,
        body: body_bytes,
        path_segments,
        method: parts.method,
        is_query_protocol: detected.protocol == AwsProtocol::Query,
    };

    tracing::info!(
        service = %aws_request.service,
        action = %aws_request.action,
        request_id = %aws_request.request_id,
        "handling request"
    );

    match service.handle(aws_request).await {
        Ok(resp) => {
            let mut builder = Response::builder()
                .status(resp.status)
                .header("content-type", &resp.content_type)
                .header("x-amzn-requestid", &request_id)
                .header("x-amz-request-id", &request_id);

            for (k, v) in &resp.headers {
                builder = builder.header(k, v);
            }

            builder.body(Body::from(resp.body)).unwrap()
        }
        Err(err) => {
            tracing::warn!(
                service = %detected.service,
                action = %detected.action,
                error = %err,
                "request failed"
            );
            build_error_response(
                err.status(),
                err.code(),
                &err.message(),
                &request_id,
                detected.protocol,
            )
        }
    }
}

/// Configuration passed to the dispatch handler.
#[derive(Debug, Clone)]
pub struct DispatchConfig {
    pub region: String,
    pub account_id: String,
}

/// Extract region from User-Agent header suffix `region/<region>`.
fn extract_region_from_user_agent(headers: &http::HeaderMap) -> Option<String> {
    let ua = headers.get("user-agent")?.to_str().ok()?;
    for part in ua.split_whitespace() {
        if let Some(region) = part.strip_prefix("region/") {
            if !region.is_empty() {
                return Some(region.to_string());
            }
        }
    }
    None
}

fn build_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    request_id: &str,
    protocol: AwsProtocol,
) -> Response<Body> {
    let (status, content_type, body) = match protocol {
        AwsProtocol::Query | AwsProtocol::Rest => {
            fakecloud_aws::error::xml_error_response(status, code, message, request_id)
        }
        AwsProtocol::Json => fakecloud_aws::error::json_error_response(status, code, message),
    };

    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .header("x-amzn-requestid", request_id)
        .header("x-amz-request-id", request_id)
        .body(Body::from(body))
        .unwrap()
}

trait ProtocolExt {
    fn error_status(&self) -> StatusCode;
}

impl ProtocolExt for AwsProtocol {
    fn error_status(&self) -> StatusCode {
        StatusCode::BAD_REQUEST
    }
}
