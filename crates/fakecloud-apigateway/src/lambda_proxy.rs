//! Lambda proxy integration for API Gateway v1.
//!
//! Builds the v1.0-shape event that AWS sends to AWS_PROXY-typed Lambda
//! integrations and parses the function's response back into an HTTP
//! reply. The shape differs from v2 (`fakecloud-apigatewayv2::lambda_proxy`)
//! in a few key fields: `multiValueHeaders`, `multiValueQueryStringParameters`,
//! `requestContext.identity`, and the absence of a `routeKey` /
//! `rawPath`. Keep them in sync with AWS docs:
//! https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html

use base64::prelude::*;
use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use serde_json::json;
use std::collections::BTreeMap;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

pub fn construct_event(
    req: &AwsRequest,
    rest_api_id: &str,
    stage: &str,
    resource_path: &str,
    path_parameters: BTreeMap<String, String>,
    stage_variables: BTreeMap<String, String>,
    binary_media_types: &[String],
) -> serde_json::Value {
    let (is_base64, body) = encode_body(req, binary_media_types);

    // Headers as both single-value and multi-value maps. AWS sends both
    // shapes; agents that only check one or the other should still work.
    let mut headers: BTreeMap<String, String> = BTreeMap::new();
    let mut multi_value_headers: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in req.headers.iter() {
        if let Ok(v_str) = v.to_str() {
            headers.insert(k.as_str().to_string(), v_str.to_string());
            multi_value_headers
                .entry(k.as_str().to_string())
                .or_default()
                .push(v_str.to_string());
        }
    }

    let mut query_string_parameters: BTreeMap<String, String> = BTreeMap::new();
    let mut multi_value_query_string_parameters: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (k, v) in &req.query_params {
        query_string_parameters.insert(k.clone(), v.clone());
        multi_value_query_string_parameters
            .entry(k.clone())
            .or_default()
            .push(v.clone());
    }

    json!({
        "resource": resource_path,
        "path": req.raw_path,
        "httpMethod": req.method.as_str(),
        "headers": if headers.is_empty() { serde_json::Value::Null } else { json!(headers) },
        "multiValueHeaders": if multi_value_headers.is_empty() {
            serde_json::Value::Null
        } else {
            json!(multi_value_headers)
        },
        "queryStringParameters": if query_string_parameters.is_empty() {
            serde_json::Value::Null
        } else {
            json!(query_string_parameters)
        },
        "multiValueQueryStringParameters": if multi_value_query_string_parameters.is_empty() {
            serde_json::Value::Null
        } else {
            json!(multi_value_query_string_parameters)
        },
        "pathParameters": if path_parameters.is_empty() {
            serde_json::Value::Null
        } else {
            json!(path_parameters)
        },
        "stageVariables": if stage_variables.is_empty() {
            serde_json::Value::Null
        } else {
            json!(stage_variables)
        },
        "requestContext": {
            "resourceId": "",
            "resourcePath": resource_path,
            "httpMethod": req.method.as_str(),
            "extendedRequestId": &req.request_id,
            "requestTime": chrono::Utc::now().to_rfc2822(),
            "path": req.raw_path,
            "accountId": &req.account_id,
            "protocol": "HTTP/1.1",
            "stage": stage,
            "domainPrefix": rest_api_id,
            "requestTimeEpoch": chrono::Utc::now().timestamp_millis(),
            "requestId": &req.request_id,
            "identity": {
                "cognitoIdentityPoolId": null,
                "accountId": null,
                "cognitoIdentityId": null,
                "caller": null,
                "sourceIp": "127.0.0.1",
                "principalOrgId": null,
                "accessKey": null,
                "cognitoAuthenticationType": null,
                "cognitoAuthenticationProvider": null,
                "userArn": null,
                "userAgent": req.headers
                    .get("user-agent")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or(""),
                "user": null
            },
            "domainName": "localhost",
            "apiId": rest_api_id
        },
        "body": body,
        "isBase64Encoded": is_base64
    })
}

fn encode_body(req: &AwsRequest, binary_media_types: &[String]) -> (bool, Option<String>) {
    if req.body.is_empty() {
        return (false, None);
    }
    let ct = req
        .headers
        .get("content-type")
        .and_then(|ct| ct.to_str().ok());
    let is_binary = ct.is_some_and(|ct| is_binary_media_type(ct, binary_media_types));
    if is_binary {
        (true, Some(BASE64_STANDARD.encode(&req.body)))
    } else {
        match String::from_utf8(req.body.to_vec()) {
            Ok(s) => (false, Some(s)),
            Err(_) => (true, Some(BASE64_STANDARD.encode(&req.body))),
        }
    }
}

/// Return true when `content_type` matches one of the configured binary
/// media types. AWS supports suffix wildcards (`image/*`) and exact
/// matches (`application/octet-stream`).
fn is_binary_media_type(content_type: &str, binary_media_types: &[String]) -> bool {
    let ct = content_type.to_lowercase();
    for pattern in binary_media_types {
        let p = pattern.to_lowercase();
        if p == ct || p == "*/*" {
            return true;
        }
        if p.ends_with("/*") {
            let prefix = &p[..p.len() - 1];
            if ct.starts_with(prefix) {
                return true;
            }
        }
    }
    false
}

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
    let response = delivery
        .invoke_lambda(function_arn, &event_json)
        .await
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "Lambda delivery not configured",
            )
        })?;
    let bytes = response.map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Lambda invocation failed: {}", e),
        )
    })?;
    let resp_json: serde_json::Value = serde_json::from_slice(&bytes).map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Failed to parse Lambda response: {}", e),
        )
    })?;
    parse_lambda_response(resp_json)
}

/// True when a proxied Lambda response is actually a **function-error**
/// envelope rather than a proxy response. A handler that throws makes the
/// Lambda runtime reply HTTP 200 with a body shaped like
/// `{"errorMessage": "...", "errorType": "...", "stackTrace": [...]}` and
/// no `statusCode`.
///
/// The guard requires BOTH `errorType` AND `errorMessage` as strings AND
/// the absence of `statusCode` — exactly the check the Lambda `Invoke`
/// handler and StepFunctions interpreter use. That avoids over-rejecting a
/// SUCCESSFUL Lambda whose 200 body legitimately carries an `errorMessage`
/// field: such a response either sets `statusCode` (a proxy response) or is
/// a plain object, so it will not trip all three conditions.
fn is_function_error_envelope(response: &serde_json::Value) -> bool {
    let Some(obj) = response.as_object() else {
        return false;
    };
    obj.get("errorType").and_then(|v| v.as_str()).is_some()
        && obj.get("errorMessage").and_then(|v| v.as_str()).is_some()
        && !obj.contains_key("statusCode")
}

/// AWS's response for a malformed proxy response or a Lambda function
/// error: HTTP 502 with `{"message":"Internal server error"}` and
/// `Content-Type: application/json`. Real API Gateway also logs
/// "Execution failed ... malformed Lambda proxy response".
fn malformed_proxy_response() -> AwsResponse {
    AwsResponse {
        status: StatusCode::BAD_GATEWAY,
        content_type: "application/json".to_string(),
        headers: HeaderMap::new(),
        body: Bytes::from_static(br#"{"message":"Internal server error"}"#).into(),
    }
}

fn parse_lambda_response(response: serde_json::Value) -> Result<AwsResponse, AwsServiceError> {
    // A thrown handler surfaces as a 200 error envelope with no statusCode.
    // AWS maps a Lambda function error to HTTP 502. A REST (v1) proxy
    // integration also REQUIRES statusCode on every response, so any
    // response lacking it — function error or otherwise malformed — is a
    // 502. (v2 payload format 2.0's statusCode-less "simple response" is
    // handled separately in the apigatewayv2 parser.)
    if is_function_error_envelope(&response) || response.get("statusCode").is_none() {
        return Ok(malformed_proxy_response());
    }

    let status_code = response
        .get("statusCode")
        .and_then(|v| v.as_i64())
        .unwrap_or(200);
    let status_code: u16 = status_code.try_into().map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Lambda response has invalid statusCode: {}", status_code),
        )
    })?;
    let status = StatusCode::from_u16(status_code).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_GATEWAY,
            "BadGatewayException",
            format!("Lambda response has invalid statusCode: {}", status_code),
        )
    })?;

    // AWS docs: when both `headers` and `multiValueHeaders` contain the
    // same key, `multiValueHeaders` wins. Process multi-value first and
    // skip duplicates from `headers` so we never emit the same key/value
    // pair twice.
    let mut headers = HeaderMap::new();
    let mut multi_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    if let Some(mvh) = response["multiValueHeaders"].as_object() {
        for (k, v_arr) in mvh {
            let Ok(name) = http::HeaderName::from_bytes(k.as_bytes()) else {
                continue;
            };
            let Some(arr) = v_arr.as_array() else {
                continue;
            };
            // Only treat the key as overridden once we successfully
            // accept at least one valid value — otherwise an empty or
            // entirely-invalid multi-value list would silently swallow
            // the corresponding `headers` entry.
            let mut accepted_any = false;
            for v in arr {
                if let Some(s) = v.as_str() {
                    if let Ok(val) = http::HeaderValue::from_str(s) {
                        headers.append(name.clone(), val);
                        accepted_any = true;
                    }
                }
            }
            if accepted_any {
                multi_keys.insert(name.as_str().to_lowercase());
            }
        }
    }
    if let Some(h) = response["headers"].as_object() {
        for (k, v) in h {
            if let (Ok(name), Some(s)) = (http::HeaderName::from_bytes(k.as_bytes()), v.as_str()) {
                if multi_keys.contains(&name.as_str().to_lowercase()) {
                    continue;
                }
                if let Ok(val) = http::HeaderValue::from_str(s) {
                    headers.insert(name, val);
                }
            }
        }
    }

    let is_base64 = response["isBase64Encoded"].as_bool().unwrap_or(false);
    let body = if let Some(s) = response["body"].as_str() {
        if is_base64 {
            Bytes::from(BASE64_STANDARD.decode(s).map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_GATEWAY,
                    "BadGatewayException",
                    format!("Lambda response has invalid base64 body: {}", e),
                )
            })?)
        } else {
            Bytes::from(s.as_bytes().to_vec())
        }
    } else {
        Bytes::new()
    };

    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/json")
        .to_string();

    Ok(AwsResponse {
        status,
        content_type,
        headers,
        body: body.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Method;
    use std::collections::HashMap;

    fn req() -> AwsRequest {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        AwsRequest {
            service: "apigateway".to_string(),
            action: String::new(),
            method: Method::POST,
            raw_path: "/prod/pets".to_string(),
            raw_query: "filter=available".to_string(),
            path_segments: vec!["prod".to_string(), "pets".to_string()],
            query_params: HashMap::from([("filter".to_string(), "available".to_string())]),
            headers,
            body: Bytes::from(br#"{"name":"Fluffy"}"#.to_vec()),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "000000000000".to_string(),
            region: "us-east-1".to_string(),
            request_id: "rid".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn construct_event_emits_v1_shape() {
        let event = construct_event(
            &req(),
            "abc123",
            "prod",
            "/pets",
            BTreeMap::new(),
            BTreeMap::new(),
            &[],
        );
        assert_eq!(event["resource"], "/pets");
        assert_eq!(event["httpMethod"], "POST");
        assert_eq!(event["requestContext"]["stage"], "prod");
        assert_eq!(event["requestContext"]["apiId"], "abc123");
        assert_eq!(event["body"], r#"{"name":"Fluffy"}"#);
        assert_eq!(event["isBase64Encoded"], false);
    }

    #[test]
    fn parse_lambda_response_minimum_shape() {
        let r = parse_lambda_response(json!({
            "statusCode": 201,
            "headers": {"x-test": "yes"},
            "body": "ok",
        }))
        .unwrap();
        assert_eq!(r.status, StatusCode::CREATED);
        assert_eq!(r.body.expect_bytes(), b"ok".as_slice());
    }

    #[test]
    fn parse_lambda_response_function_error_is_502() {
        // A thrown handler: 200 body with errorType+errorMessage, no
        // statusCode. REST maps it to 502 {"message":"Internal server error"}.
        let r = parse_lambda_response(json!({
            "errorMessage": "boom",
            "errorType": "RuntimeError",
            "stackTrace": ["line 1"],
        }))
        .unwrap();
        assert_eq!(r.status, StatusCode::BAD_GATEWAY);
        assert_eq!(r.content_type, "application/json");
        assert_eq!(
            r.body.expect_bytes(),
            br#"{"message":"Internal server error"}"#.as_slice()
        );
    }

    #[test]
    fn parse_lambda_response_no_status_code_is_502() {
        // A proxy response missing statusCode is malformed -> 502.
        let r = parse_lambda_response(json!({"body": "ok"})).unwrap();
        assert_eq!(r.status, StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn parse_lambda_response_success_with_error_message_in_body_not_502() {
        // A legit success whose body string mentions errorMessage but which
        // sets statusCode must NOT be turned into a 502.
        let r = parse_lambda_response(json!({
            "statusCode": 200,
            "body": r#"{"errorMessage":"x"}"#,
        }))
        .unwrap();
        assert_eq!(r.status, StatusCode::OK);
        assert_eq!(r.body.expect_bytes(), br#"{"errorMessage":"x"}"#.as_slice());
    }

    #[test]
    fn parse_lambda_response_base64() {
        let r = parse_lambda_response(json!({
            "statusCode": 200,
            "body": BASE64_STANDARD.encode(b"binary"),
            "isBase64Encoded": true,
        }))
        .unwrap();
        assert_eq!(r.body.expect_bytes(), b"binary".as_slice());
    }

    #[test]
    fn encode_body_respects_binary_media_types() {
        let mut req = req();
        req.headers
            .insert("content-type", "image/png".parse().unwrap());
        let (is_base64, body) = encode_body(
            &req,
            &[
                "image/png".to_string(),
                "application/octet-stream".to_string(),
            ],
        );
        assert!(is_base64);
        assert_eq!(body, Some(BASE64_STANDARD.encode(&req.body)));
    }

    #[test]
    fn encode_body_wildcard_match() {
        let mut req = req();
        req.headers
            .insert("content-type", "image/jpeg".parse().unwrap());
        let (is_base64, body) = encode_body(&req, &["image/*".to_string()]);
        assert!(is_base64);
        assert_eq!(body, Some(BASE64_STANDARD.encode(&req.body)));
    }

    #[test]
    fn encode_body_no_match_is_text() {
        let mut req = req();
        req.headers
            .insert("content-type", "application/json".parse().unwrap());
        let (is_base64, body) = encode_body(&req, &["image/*".to_string()]);
        assert!(!is_base64);
        assert_eq!(body, Some(r#"{"name":"Fluffy"}"#.to_string()));
    }

    #[test]
    fn construct_event_with_binary_media_types() {
        let mut req = req();
        req.headers
            .insert("content-type", "image/png".parse().unwrap());
        let event = construct_event(
            &req,
            "abc123",
            "prod",
            "/pets",
            BTreeMap::new(),
            BTreeMap::new(),
            &["image/png".to_string()],
        );
        assert_eq!(event["isBase64Encoded"], true);
        assert_eq!(event["body"], BASE64_STANDARD.encode(&req.body));
    }
}
