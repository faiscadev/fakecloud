use base64::prelude::*;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use serde_json::json;
use std::collections::HashMap;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

/// Constructs a Lambda proxy integration event in v2.0 format.
/// https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
pub fn construct_event(
    req: &AwsRequest,
    route_key: &str,
    stage: &str,
    path_parameters: HashMap<String, String>,
) -> serde_json::Value {
    let (is_base64_encoded, body) = encode_body(req);

    let query_string_parameters = if req.query_params.is_empty() {
        None
    } else {
        Some(req.query_params.clone())
    };

    let raw_query_string = &req.raw_query;

    let path_parameters = if path_parameters.is_empty() {
        None
    } else {
        Some(path_parameters)
    };

    // Convert HeaderMap to HashMap<String, String> for JSON serialization
    let headers: HashMap<String, String> = req
        .headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|v_str| (k.as_str().to_string(), v_str.to_string()))
        })
        .collect();

    json!({
        "version": "2.0",
        "routeKey": route_key,
        "rawPath": req.raw_path,
        "rawQueryString": raw_query_string,
        "headers": headers,
        "requestContext": {
            "http": {
                "method": req.method.as_str(),
                "path": req.raw_path,
                "sourceIp": "127.0.0.1"
            },
            "routeKey": route_key,
            "stage": stage,
            "requestId": &req.request_id,
            "accountId": &req.account_id,
            "domainName": "localhost",
            "time": chrono::Utc::now().to_rfc3339(),
            "timeEpoch": chrono::Utc::now().timestamp_millis()
        },
        "pathParameters": path_parameters,
        "queryStringParameters": query_string_parameters,
        "body": body,
        "isBase64Encoded": is_base64_encoded
    })
}

fn encode_body(req: &AwsRequest) -> (bool, Option<String>) {
    if req.body.is_empty() {
        return (false, None);
    }

    // Check if body is binary by looking at content-type header
    let is_binary = req
        .headers
        .get("content-type")
        .and_then(|ct| ct.to_str().ok())
        .map(|ct_str| {
            let ct_lower = ct_str.to_lowercase();
            ct_lower.contains("octet-stream")
                || ct_lower.contains("image/")
                || ct_lower.contains("video/")
                || ct_lower.contains("audio/")
        })
        .unwrap_or(false);

    if is_binary {
        (true, Some(BASE64_STANDARD.encode(&req.body)))
    } else {
        // Try to interpret as UTF-8 string
        match String::from_utf8(req.body.to_vec()) {
            Ok(s) => (false, Some(s)),
            Err(_) => (true, Some(BASE64_STANDARD.encode(&req.body))),
        }
    }
}

/// Invokes a Lambda function via the delivery bus and parses the response.
pub async fn invoke_lambda(
    delivery: &DeliveryBus,
    function_arn: &str,
    event: serde_json::Value,
) -> Result<AwsResponse, AwsServiceError> {
    let event_json = serde_json::to_string(&event).map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("Failed to serialize event: {}", e),
        )
    })?;

    // Invoke Lambda via delivery bus
    let response_result = delivery
        .invoke_lambda(function_arn, &event_json)
        .await
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Lambda delivery not configured",
            )
        })?;

    let response_bytes = response_result.map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Lambda invocation failed: {}", e),
        )
    })?;

    // Parse Lambda response
    let response_json: serde_json::Value =
        serde_json::from_slice(&response_bytes).map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::BAD_GATEWAY,
                "BadGatewayException",
                format!("Failed to parse Lambda response: {}", e),
            )
        })?;

    parse_lambda_response(response_json)
}

/// Parses a Lambda proxy integration response in v2.0 format.
fn parse_lambda_response(response: serde_json::Value) -> Result<AwsResponse, AwsServiceError> {
    let status_code = match response.get("statusCode") {
        Some(v) => v.as_i64().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_GATEWAY,
                "BadGatewayException",
                "Lambda response has invalid statusCode",
            )
        })?,
        None => 200,
    };
    let status_code: u16 = status_code.try_into().map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Lambda response has invalid statusCode: {}", status_code),
        )
    })?;
    let status_code = StatusCode::from_u16(status_code).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Lambda response has invalid statusCode: {}", status_code),
        )
    })?;

    let mut headers = HeaderMap::new();
    if let Some(response_headers) = response["headers"].as_object() {
        for (k, v) in response_headers {
            if let Some(v_str) = v.as_str() {
                if let Ok(header_value) = http::HeaderValue::from_str(v_str) {
                    if let Ok(header_name) = http::HeaderName::from_bytes(k.as_bytes()) {
                        headers.insert(header_name, header_value);
                    }
                }
            }
        }
    }

    let is_base64 = response["isBase64Encoded"].as_bool().unwrap_or(false);
    let body = if let Some(body_str) = response["body"].as_str() {
        if is_base64 {
            Bytes::from(BASE64_STANDARD.decode(body_str).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_GATEWAY,
                    "BadGatewayException",
                    format!("Lambda response has invalid base64 body: {}", e),
                )
            })?)
        } else {
            Bytes::from(body_str.as_bytes().to_vec())
        }
    } else {
        Bytes::new()
    };

    // Determine content type from headers or default to application/json
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    Ok(AwsResponse {
        status: status_code,
        content_type,
        headers,
        body,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;

    fn create_test_request() -> AwsRequest {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());

        AwsRequest {
            service: "apigateway".to_string(),
            action: "Execute".to_string(),
            method: Method::POST,
            raw_path: "/prod/pets".to_string(),
            raw_query: "filter=available".to_string(),
            path_segments: vec!["prod".to_string(), "pets".to_string()],
            query_params: HashMap::from([("filter".to_string(), "available".to_string())]),
            headers,
            body: Bytes::from(br#"{"name":"Fluffy"}"#.to_vec()),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "request-id".to_string(),
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    #[test]
    fn test_construct_event() {
        let req = create_test_request();
        let path_params = HashMap::from([("id".to_string(), "123".to_string())]);

        let event = construct_event(&req, "POST /pets/{id}", "prod", path_params);

        assert_eq!(event["version"], "2.0");
        assert_eq!(event["routeKey"], "POST /pets/{id}");
        assert_eq!(event["rawPath"], "/prod/pets");
        assert_eq!(event["rawQueryString"], "filter=available");
        assert_eq!(event["requestContext"]["stage"], "prod");
        assert_eq!(event["pathParameters"]["id"], "123");
        assert_eq!(event["queryStringParameters"]["filter"], "available");
        assert_eq!(event["body"], r#"{"name":"Fluffy"}"#);
        assert_eq!(event["isBase64Encoded"], false);
    }

    #[test]
    fn test_parse_lambda_response() {
        let response = json!({
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": r#"{"message":"success"}"#,
            "isBase64Encoded": false
        });

        let result = parse_lambda_response(response).unwrap();

        assert_eq!(result.status, StatusCode::OK);
        assert_eq!(result.content_type, "application/json");
        assert_eq!(
            result.body,
            Bytes::from(br#"{"message":"success"}"#.to_vec())
        );
    }

    #[test]
    fn test_parse_lambda_response_base64() {
        let base64_body = BASE64_STANDARD.encode(b"binary data");
        let response = json!({
            "statusCode": 200,
            "body": base64_body,
            "isBase64Encoded": true
        });

        let result = parse_lambda_response(response).unwrap();

        assert_eq!(result.status, StatusCode::OK);
        assert_eq!(result.body, Bytes::from(b"binary data".to_vec()));
    }
}
