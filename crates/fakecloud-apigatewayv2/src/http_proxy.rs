use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use std::time::Duration;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

/// Forwards an HTTP request to an external endpoint
pub async fn forward_request(
    target_url: &str,
    req: &AwsRequest,
    timeout_millis: Option<i64>,
) -> Result<AwsResponse, AwsServiceError> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(timeout_millis.unwrap_or(30000).max(0) as u64))
        .build()
        .map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("Failed to create HTTP client: {}", e),
            )
        })?;

    // Build request URL with query parameters
    let url = if req.raw_query.is_empty() {
        target_url.to_string()
    } else {
        format!("{}?{}", target_url, req.raw_query)
    };

    // Build the request
    let mut request_builder = client.request(req.method.clone(), &url);

    // Copy headers (skip host and authorization)
    for (key, value) in &req.headers {
        let key_str = key.as_str();
        if key_str != "host" && key_str != "authorization" {
            request_builder = request_builder.header(key, value);
        }
    }

    // Add body
    if !req.body.is_empty() {
        request_builder = request_builder.body(req.body.clone());
    }

    // Execute request
    let response = request_builder.send().await.map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Failed to forward request: {}", e),
        )
    })?;

    // Extract status
    let status = response.status();

    // Extract headers
    let mut headers = HeaderMap::new();
    for (key, value) in response.headers() {
        headers.insert(key.clone(), value.clone());
    }

    // Extract body
    let body_bytes = response.bytes().await.map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Failed to read response body: {}", e),
        )
    })?;

    // Determine content type
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();

    Ok(AwsResponse {
        status,
        content_type,
        headers,
        body: Bytes::from(body_bytes.to_vec()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use std::collections::HashMap;

    fn create_test_request() -> AwsRequest {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());

        AwsRequest {
            service: "apigateway".to_string(),
            action: "Execute".to_string(),
            method: Method::POST,
            raw_path: "/prod/test".to_string(),
            raw_query: "param=value".to_string(),
            path_segments: vec!["prod".to_string(), "test".to_string()],
            query_params: HashMap::from([("param".to_string(), "value".to_string())]),
            headers,
            body: Bytes::from(br#"{"test":"data"}"#.to_vec()),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "request-id".to_string(),
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    // Note: Real HTTP proxy tests would require a mock HTTP server
    // For now, we test the function signature and error handling

    #[tokio::test]
    async fn test_forward_request_invalid_url() {
        let req = create_test_request();
        let result = forward_request("not-a-valid-url", &req, Some(5000)).await;
        assert!(result.is_err());
    }
}
