use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};

use crate::state::CorsConfiguration;
use fakecloud_core::service::{AwsRequest, AwsResponse};

/// Handle CORS preflight OPTIONS request
pub fn handle_preflight(cors_config: &CorsConfiguration, _req: &AwsRequest) -> AwsResponse {
    let mut headers = HeaderMap::new();

    // Add Access-Control-Allow-Origin
    if let Some(ref origins) = cors_config.allow_origins {
        let origin_value = if origins.contains(&"*".to_string()) {
            "*"
        } else {
            origins.first().map(|s| s.as_str()).unwrap_or("*")
        };
        headers.insert("access-control-allow-origin", origin_value.parse().unwrap());
    }

    // Add Access-Control-Allow-Methods
    if let Some(ref methods) = cors_config.allow_methods {
        let methods_value = methods.join(",");
        headers.insert(
            "access-control-allow-methods",
            methods_value.parse().unwrap(),
        );
    }

    // Add Access-Control-Allow-Headers
    if let Some(ref allow_headers) = cors_config.allow_headers {
        let headers_value = allow_headers.join(",");
        headers.insert(
            "access-control-allow-headers",
            headers_value.parse().unwrap(),
        );
    }

    // Add Access-Control-Max-Age
    if let Some(max_age) = cors_config.max_age {
        headers.insert(
            "access-control-max-age",
            max_age.to_string().parse().unwrap(),
        );
    }

    // Add Access-Control-Allow-Credentials
    if let Some(true) = cors_config.allow_credentials {
        headers.insert("access-control-allow-credentials", "true".parse().unwrap());
    }

    AwsResponse {
        status: StatusCode::NO_CONTENT,
        content_type: String::new(),
        headers,
        body: Bytes::new(),
    }
}

/// Add CORS headers to an existing response
pub fn add_cors_headers(mut response: AwsResponse, cors_config: &CorsConfiguration) -> AwsResponse {
    // Add Access-Control-Allow-Origin
    if let Some(ref origins) = cors_config.allow_origins {
        let origin_value = if origins.contains(&"*".to_string()) {
            "*"
        } else {
            origins.first().map(|s| s.as_str()).unwrap_or("*")
        };
        response
            .headers
            .insert("access-control-allow-origin", origin_value.parse().unwrap());
    }

    // Add Access-Control-Expose-Headers
    if let Some(ref expose_headers) = cors_config.expose_headers {
        let headers_value = expose_headers.join(",");
        response.headers.insert(
            "access-control-expose-headers",
            headers_value.parse().unwrap(),
        );
    }

    // Add Access-Control-Allow-Credentials
    if let Some(true) = cors_config.allow_credentials {
        response
            .headers
            .insert("access-control-allow-credentials", "true".parse().unwrap());
    }

    response
}

/// Check if the request is a CORS preflight request
pub fn is_preflight_request(req: &AwsRequest) -> bool {
    req.method == Method::OPTIONS && req.headers.contains_key("access-control-request-method")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn create_test_request(method: Method, has_preflight_header: bool) -> AwsRequest {
        let mut headers = HeaderMap::new();
        if has_preflight_header {
            headers.insert("access-control-request-method", "POST".parse().unwrap());
        }

        AwsRequest {
            service: "apigateway".to_string(),
            action: "Execute".to_string(),
            method,
            raw_path: "/prod/test".to_string(),
            raw_query: String::new(),
            path_segments: vec!["prod".to_string(), "test".to_string()],
            query_params: HashMap::new(),
            headers,
            body: Bytes::new(),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "request-id".to_string(),
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    #[test]
    fn test_is_preflight_request() {
        let preflight = create_test_request(Method::OPTIONS, true);
        assert!(is_preflight_request(&preflight));

        let not_preflight_1 = create_test_request(Method::GET, false);
        assert!(!is_preflight_request(&not_preflight_1));

        let not_preflight_2 = create_test_request(Method::OPTIONS, false);
        assert!(!is_preflight_request(&not_preflight_2));
    }

    #[test]
    fn test_handle_preflight() {
        let cors_config = CorsConfiguration {
            allow_credentials: Some(true),
            allow_headers: Some(vec![
                "Content-Type".to_string(),
                "Authorization".to_string(),
            ]),
            allow_methods: Some(vec!["GET".to_string(), "POST".to_string()]),
            allow_origins: Some(vec!["*".to_string()]),
            expose_headers: None,
            max_age: Some(3600),
        };

        let req = create_test_request(Method::OPTIONS, true);
        let response = handle_preflight(&cors_config, &req);

        assert_eq!(response.status, StatusCode::NO_CONTENT);
        assert_eq!(
            response.headers.get("access-control-allow-origin").unwrap(),
            "*"
        );
        assert_eq!(
            response
                .headers
                .get("access-control-allow-methods")
                .unwrap(),
            "GET,POST"
        );
        assert_eq!(
            response
                .headers
                .get("access-control-allow-headers")
                .unwrap(),
            "Content-Type,Authorization"
        );
        assert_eq!(
            response.headers.get("access-control-max-age").unwrap(),
            "3600"
        );
        assert_eq!(
            response
                .headers
                .get("access-control-allow-credentials")
                .unwrap(),
            "true"
        );
    }

    #[test]
    fn test_add_cors_headers() {
        let cors_config = CorsConfiguration {
            allow_credentials: Some(true),
            allow_headers: None,
            allow_methods: None,
            allow_origins: Some(vec!["https://example.com".to_string()]),
            expose_headers: Some(vec!["X-Custom-Header".to_string()]),
            max_age: None,
        };

        let response = AwsResponse {
            status: StatusCode::OK,
            content_type: "application/json".to_string(),
            headers: HeaderMap::new(),
            body: Bytes::from(b"test".to_vec()),
        };

        let response_with_cors = add_cors_headers(response, &cors_config);

        assert_eq!(
            response_with_cors
                .headers
                .get("access-control-allow-origin")
                .unwrap(),
            "https://example.com"
        );
        assert_eq!(
            response_with_cors
                .headers
                .get("access-control-expose-headers")
                .unwrap(),
            "X-Custom-Header"
        );
        assert_eq!(
            response_with_cors
                .headers
                .get("access-control-allow-credentials")
                .unwrap(),
            "true"
        );
    }
}
