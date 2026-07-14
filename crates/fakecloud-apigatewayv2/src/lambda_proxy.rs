use base64::prelude::*;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use serde_json::json;
use std::collections::HashMap;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

/// Authorizer result to inject into the v2.0 request context.
pub enum AuthorizerInfo {
    /// JWT claims returned by a Cognito/JWT authorizer.
    Jwt { claims: serde_json::Value },
    /// Lambda authorizer context (already shaped as the `authorizer`
    /// object that should appear under `requestContext`).
    Lambda { context: serde_json::Value },
}

/// Constructs a Lambda proxy integration event in v2.0 format.
/// https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
pub fn construct_event(
    req: &AwsRequest,
    route_key: &str,
    stage: &str,
    path_parameters: HashMap<String, String>,
    authorizer: Option<AuthorizerInfo>,
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

    let mut request_context = serde_json::Map::new();
    request_context.insert(
        "http".to_string(),
        json!({
            "method": req.method.as_str(),
            "path": req.raw_path,
            "sourceIp": "127.0.0.1"
        }),
    );
    request_context.insert("routeKey".to_string(), json!(route_key));
    request_context.insert("stage".to_string(), json!(stage));
    request_context.insert("requestId".to_string(), json!(&req.request_id));
    request_context.insert("accountId".to_string(), json!(&req.account_id));
    request_context.insert("domainName".to_string(), json!("localhost"));
    request_context.insert("time".to_string(), json!(chrono::Utc::now().to_rfc3339()));
    request_context.insert(
        "timeEpoch".to_string(),
        json!(chrono::Utc::now().timestamp_millis()),
    );

    if let Some(auth) = authorizer {
        match auth {
            AuthorizerInfo::Jwt { claims } => {
                let mut jwt = serde_json::Map::new();
                jwt.insert("claims".to_string(), claims);
                let mut authorizer = serde_json::Map::new();
                authorizer.insert("jwt".to_string(), serde_json::Value::Object(jwt));
                request_context.insert(
                    "authorizer".to_string(),
                    serde_json::Value::Object(authorizer),
                );
            }
            AuthorizerInfo::Lambda { context } => {
                request_context.insert("authorizer".to_string(), context);
            }
        }
    }

    json!({
        "version": "2.0",
        "routeKey": route_key,
        "rawPath": req.raw_path,
        "rawQueryString": raw_query_string,
        "headers": headers,
        "requestContext": serde_json::Value::Object(request_context),
        "pathParameters": path_parameters,
        "queryStringParameters": query_string_parameters,
        "body": body,
        "isBase64Encoded": is_base64_encoded
    })
}

/// Constructs a Lambda proxy integration event in the **1.0** payload
/// format for an HTTP API whose integration set
/// `payloadFormatVersion = "1.0"`. This is the same envelope REST APIs
/// (API Gateway v1) send, so a Lambda written for a REST proxy keeps
/// working behind an HTTP API. See the payload-format-version docs:
/// https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-lambda.html
pub fn construct_event_v1(
    req: &AwsRequest,
    route_key: &str,
    stage: &str,
    path_parameters: HashMap<String, String>,
    authorizer: Option<AuthorizerInfo>,
) -> serde_json::Value {
    let (is_base64_encoded, body) = encode_body(req);

    // The `resource` field is the route's path template (the part of the
    // route key after the method), defaulting to the raw path.
    let resource = route_key
        .split_once(' ')
        .map(|(_, path)| path.to_string())
        .unwrap_or_else(|| req.raw_path.clone());

    // Single-value + multi-value header maps.
    let mut headers = serde_json::Map::new();
    let mut multi_value_headers = serde_json::Map::new();
    for (k, v) in req.headers.iter() {
        if let Ok(v_str) = v.to_str() {
            headers.insert(k.as_str().to_string(), json!(v_str));
            multi_value_headers
                .entry(k.as_str().to_string())
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap()
                .push(json!(v_str));
        }
    }

    let query_string_parameters = if req.query_params.is_empty() {
        serde_json::Value::Null
    } else {
        json!(req.query_params)
    };
    let multi_value_query_string_parameters = if req.query_params.is_empty() {
        serde_json::Value::Null
    } else {
        let mut m = serde_json::Map::new();
        for (k, v) in &req.query_params {
            m.insert(k.clone(), json!([v]));
        }
        serde_json::Value::Object(m)
    };

    let path_parameters = if path_parameters.is_empty() {
        serde_json::Value::Null
    } else {
        json!(path_parameters)
    };

    let mut request_context = serde_json::Map::new();
    request_context.insert("accountId".to_string(), json!(&req.account_id));
    request_context.insert("apiId".to_string(), json!(host_api_id(req)));
    request_context.insert("domainName".to_string(), json!("localhost"));
    request_context.insert("httpMethod".to_string(), json!(req.method.as_str()));
    request_context.insert("path".to_string(), json!(req.raw_path));
    request_context.insert("protocol".to_string(), json!("HTTP/1.1"));
    request_context.insert("requestId".to_string(), json!(&req.request_id));
    request_context.insert("resourcePath".to_string(), json!(resource));
    request_context.insert("stage".to_string(), json!(stage));
    request_context.insert("identity".to_string(), json!({ "sourceIp": "127.0.0.1" }));
    request_context.insert(
        "requestTimeEpoch".to_string(),
        json!(chrono::Utc::now().timestamp_millis()),
    );

    if let Some(auth) = authorizer {
        // 1.0 exposes the authorizer object directly under
        // `requestContext.authorizer` (JWT claims land under a `claims`
        // key, matching REST Cognito behaviour).
        let value = match auth {
            AuthorizerInfo::Jwt { claims } => json!({ "claims": claims }),
            AuthorizerInfo::Lambda { context } => context,
        };
        request_context.insert("authorizer".to_string(), value);
    }

    json!({
        "version": "1.0",
        "resource": resource,
        "path": req.raw_path,
        "httpMethod": req.method.as_str(),
        "headers": serde_json::Value::Object(headers),
        "multiValueHeaders": serde_json::Value::Object(multi_value_headers),
        "queryStringParameters": query_string_parameters,
        "multiValueQueryStringParameters": multi_value_query_string_parameters,
        "pathParameters": path_parameters,
        "stageVariables": serde_json::Value::Null,
        "requestContext": serde_json::Value::Object(request_context),
        "body": body,
        "isBase64Encoded": is_base64_encoded,
    })
}

/// Best-effort extraction of the API id from the execute-api Host header
/// (`{api-id}.execute-api.<region>.amazonaws.com`). Falls back to an
/// empty string when the header is absent.
fn host_api_id(req: &AwsRequest) -> String {
    req.headers
        .get("host")
        .and_then(|v| v.to_str().ok())
        .and_then(|h| h.split('.').next())
        .unwrap_or("")
        .to_string()
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
        body: body.into(),
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
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "request-id".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn test_construct_event() {
        let req = create_test_request();
        let path_params = HashMap::from([("id".to_string(), "123".to_string())]);

        let event = construct_event(&req, "POST /pets/{id}", "prod", path_params, None);

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
    fn test_construct_event_v1_shape() {
        // payloadFormatVersion "1.0" produces the REST-style envelope:
        // httpMethod / multiValueHeaders / requestContext.identity etc.
        let req = create_test_request();
        let path_params = HashMap::from([("id".to_string(), "123".to_string())]);
        let event = construct_event_v1(&req, "POST /pets/{id}", "prod", path_params, None);

        assert_eq!(event["version"], "1.0");
        assert_eq!(event["httpMethod"], "POST");
        assert_eq!(event["resource"], "/pets/{id}");
        assert_eq!(event["path"], "/prod/pets");
        // 2.0-only fields must be absent.
        assert!(event["routeKey"].is_null());
        assert!(event["rawPath"].is_null());
        // Multi-value maps + identity present.
        assert_eq!(
            event["multiValueHeaders"]["content-type"][0],
            "application/json"
        );
        assert_eq!(event["requestContext"]["identity"]["sourceIp"], "127.0.0.1");
        assert_eq!(event["requestContext"]["httpMethod"], "POST");
        assert_eq!(event["requestContext"]["stage"], "prod");
        assert_eq!(event["pathParameters"]["id"], "123");
        assert_eq!(
            event["multiValueQueryStringParameters"]["filter"][0],
            "available"
        );
    }

    #[test]
    fn test_construct_event_v1_lambda_authorizer_context() {
        let req = create_test_request();
        let ctx = json!({"principalId": "u", "role": "admin"});
        let event = construct_event_v1(
            &req,
            "GET /pets",
            "prod",
            HashMap::new(),
            Some(AuthorizerInfo::Lambda { context: ctx }),
        );
        assert_eq!(event["requestContext"]["authorizer"]["role"], "admin");
    }

    #[test]
    fn test_construct_event_with_authorizer_claims() {
        let req = create_test_request();
        let path_params = HashMap::new();
        let claims = json!({"sub": "user-123", "aud": "my-client"});

        let event = construct_event(
            &req,
            "POST /pets",
            "prod",
            path_params,
            Some(AuthorizerInfo::Jwt {
                claims: claims.clone(),
            }),
        );

        assert_eq!(
            event["requestContext"]["authorizer"]["jwt"]["claims"],
            claims
        );
    }

    #[test]
    fn test_construct_event_with_lambda_authorizer_context() {
        let req = create_test_request();
        let path_params = HashMap::new();
        let ctx = json!({"principalId": "lambda-user", "role": "admin"});

        let event = construct_event(
            &req,
            "POST /pets",
            "prod",
            path_params,
            Some(AuthorizerInfo::Lambda {
                context: ctx.clone(),
            }),
        );

        assert_eq!(
            event["requestContext"]["authorizer"]["principalId"],
            "lambda-user"
        );
        assert_eq!(event["requestContext"]["authorizer"]["role"], "admin");
        assert!(
            event["requestContext"]["authorizer"]["jwt"].is_null(),
            "jwt should be absent for Lambda authorizer"
        );
    }

    #[test]
    fn test_construct_event_without_authorizer_omits_field() {
        let req = create_test_request();
        let event = construct_event(&req, "POST /pets", "prod", HashMap::new(), None);
        assert!(
            event["requestContext"]["authorizer"].is_null(),
            "authorizer should be absent when None"
        );
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
            result.body.expect_bytes(),
            br#"{"message":"success"}"#.as_slice()
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
        assert_eq!(result.body.expect_bytes(), b"binary data".as_slice());
    }

    #[test]
    fn test_parse_lambda_response_no_body_defaults_empty() {
        let response = json!({"statusCode": 204});
        let result = parse_lambda_response(response).unwrap();
        assert_eq!(result.status, StatusCode::NO_CONTENT);
        assert!(result.body.expect_bytes().is_empty());
    }

    #[test]
    fn test_parse_lambda_response_invalid_status_errors() {
        let response = json!({"statusCode": 9999, "body": ""});
        assert!(parse_lambda_response(response).is_err());
    }

    #[test]
    fn test_parse_lambda_response_invalid_base64_errors() {
        let response = json!({
            "statusCode": 200,
            "body": "not-base64!!!",
            "isBase64Encoded": true
        });
        assert!(parse_lambda_response(response).is_err());
    }

    #[test]
    fn test_construct_event_with_binary_body() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/octet-stream".parse().unwrap());
        let req = AwsRequest {
            service: "apigateway".to_string(),
            action: "Execute".to_string(),
            method: Method::POST,
            raw_path: "/prod/img".to_string(),
            raw_query: String::new(),
            path_segments: vec!["prod".to_string(), "img".to_string()],
            query_params: HashMap::new(),
            headers,
            body: Bytes::from(vec![0xFF, 0xFE, 0xFD]),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "request-id".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let event = construct_event(&req, "POST /img", "prod", HashMap::new(), None);
        assert_eq!(event["isBase64Encoded"], true);
    }

    #[test]
    fn test_construct_event_empty_body() {
        let req = AwsRequest {
            service: "apigateway".to_string(),
            action: "Execute".to_string(),
            method: Method::GET,
            raw_path: "/prod/noop".to_string(),
            raw_query: String::new(),
            path_segments: vec!["prod".to_string(), "noop".to_string()],
            query_params: HashMap::new(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "request-id".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        let event = construct_event(&req, "GET /noop", "prod", HashMap::new(), None);
        assert_eq!(event["isBase64Encoded"], false);
        assert!(event["body"].is_null());
    }
}
