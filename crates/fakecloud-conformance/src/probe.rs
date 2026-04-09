//! Level 1 probing: send generated requests to fakecloud and classify responses.

use serde_json::Value;

use crate::generators::{Expectation, TestVariant};
use crate::shape_validator;
use crate::smithy::ServiceModel;

/// Protocol used by a service for request/response encoding.
#[derive(Debug, Clone, Copy)]
pub enum Protocol {
    /// Query protocol: form-encoded body with `Action` param, XML responses.
    /// Used by: SQS, SNS, IAM, STS, CloudFormation.
    Query,
    /// JSON protocol: JSON body with `X-Amz-Target` header.
    /// Used by: SSM, EventBridge, DynamoDB, Secrets Manager, CloudWatch Logs, KMS.
    Json { target_prefix: &'static str },
    /// REST protocol: HTTP method + path routing.
    /// Used by: S3, Lambda.
    Rest,
}

/// Result of probing a single test variant.
#[derive(Debug)]
pub struct ProbeResult {
    pub variant_name: String,
    pub status: ProbeStatus,
    pub http_status: u16,
    pub response_body: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeStatus {
    /// Response looks correct (shape matches, or expected error received).
    Pass,
    /// Response shape doesn't match the model.
    ShapeMismatch(String),
    /// Action is not implemented in fakecloud.
    NotImplemented,
    /// Unexpected server error (500, panic, etc.).
    Crash(String),
    /// Expected an error but got success, or vice versa.
    UnexpectedResult(String),
}

impl std::fmt::Display for ProbeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProbeStatus::Pass => write!(f, "PASS"),
            ProbeStatus::ShapeMismatch(msg) => write!(f, "SHAPE_MISMATCH: {}", msg),
            ProbeStatus::NotImplemented => write!(f, "NOT_IMPLEMENTED"),
            ProbeStatus::Crash(msg) => write!(f, "CRASH: {}", msg),
            ProbeStatus::UnexpectedResult(msg) => write!(f, "UNEXPECTED: {}", msg),
        }
    }
}

/// Map service names to their protocol.
pub fn service_protocol(service_name: &str) -> Protocol {
    match service_name {
        "sqs" => Protocol::Query,
        "sns" => Protocol::Query,
        "iam" => Protocol::Query,
        "sts" => Protocol::Query,
        "cloudformation" => Protocol::Query,
        "ssm" => Protocol::Json {
            target_prefix: "AmazonSSM",
        },
        "events" => Protocol::Json {
            target_prefix: "AWSEvents",
        },
        "dynamodb" => Protocol::Json {
            target_prefix: "DynamoDB_20120810",
        },
        "secretsmanager" => Protocol::Json {
            target_prefix: "secretsmanager",
        },
        "logs" => Protocol::Json {
            target_prefix: "Logs_20140328",
        },
        "kms" => Protocol::Json {
            target_prefix: "TrentService",
        },
        "cognito-idp" => Protocol::Json {
            target_prefix: "AWSCognitoIdentityProviderService",
        },
        "kinesis" => Protocol::Json {
            target_prefix: "Kinesis_20131202",
        },
        "s3" => Protocol::Rest,
        "lambda" => Protocol::Rest,
        _ => Protocol::Query,
    }
}

/// Probe a single test variant against a running fakecloud server.
/// If `model` and `output_shape_id` are provided, also validates the response shape.
pub fn probe_variant(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
) -> ProbeResult {
    probe_variant_with_model(
        client,
        endpoint,
        service_name,
        operation_name,
        variant,
        None,
    )
}

/// Probe a variant with optional shape validation against the Smithy model.
pub fn probe_variant_with_model(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
    model_info: Option<(&ServiceModel, &str)>,
) -> ProbeResult {
    let protocol = service_protocol(service_name);
    let start = std::time::Instant::now();

    let result = match protocol {
        Protocol::Query => probe_query(client, endpoint, service_name, operation_name, variant),
        Protocol::Json { target_prefix } => {
            probe_json(client, endpoint, target_prefix, operation_name, variant)
        }
        Protocol::Rest => probe_rest(client, endpoint, service_name, operation_name, variant),
    };

    let duration_ms = start.elapsed().as_millis() as u64;

    match result {
        Ok((status_code, body)) => {
            let mut probe_result = classify_response(
                &variant.name,
                status_code,
                &body,
                &variant.expectation,
                duration_ms,
            );

            // Run shape validation on successful responses
            if probe_result.status == ProbeStatus::Pass
                && (200..300).contains(&status_code)
                && !body.is_empty()
            {
                if let Some((model, output_shape_id)) = model_info {
                    let violations =
                        shape_validator::validate_response(model, output_shape_id, &body, protocol);
                    if !violations.is_empty() {
                        let msg = violations
                            .iter()
                            .take(5)
                            .map(|v| v.to_string())
                            .collect::<Vec<_>>()
                            .join("; ");
                        probe_result.status = ProbeStatus::ShapeMismatch(msg);
                    }
                }
            }

            probe_result
        }
        Err(e) => {
            let msg = if e.contains("timed out") || e.contains("timeout") {
                format!("Request timed out (>30s): {}", e)
            } else {
                format!("Request failed: {}", e)
            };
            ProbeResult {
                variant_name: variant.name.clone(),
                status: ProbeStatus::Crash(msg),
                http_status: 0,
                response_body: String::new(),
                duration_ms,
            }
        }
    }
}

fn probe_query(
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
        .header(
            "Authorization",
            format!(
                "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/{}/aws4_request",
                service_name
            ),
        )
        .body(body)
        .send()
        .map_err(|e| e.to_string())?;

    let status = resp.status().as_u16();
    let body = resp.text().map_err(|e| e.to_string())?;
    Ok((status, body))
}

fn probe_json(
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
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/service/aws4_request",
        )
        .body(body)
        .send()
        .map_err(|e| e.to_string())?;

    let status = resp.status().as_u16();
    let body = resp.text().map_err(|e| e.to_string())?;
    Ok((status, body))
}

/// Returns (HTTP method, path, optional query string) for a known REST API operation.
///
/// For S3, uses "test-conformance-bucket" as the bucket and "test-key" as the object key.
/// For Lambda, uses "test-conformance-function" as the function name.
fn rest_request_config(
    service_name: &str,
    operation_name: &str,
) -> (reqwest::Method, String, Option<String>) {
    const BUCKET: &str = "test-conformance-bucket";
    const KEY: &str = "test-key";
    const FUNC: &str = "test-conformance-function";
    const LAMBDA_PREFIX: &str = "/2015-03-31";

    match service_name {
        "lambda" => match operation_name {
            // Function CRUD
            "CreateFunction" => (
                reqwest::Method::POST,
                format!("{}/functions", LAMBDA_PREFIX),
                None,
            ),
            "ListFunctions" => (
                reqwest::Method::GET,
                format!("{}/functions", LAMBDA_PREFIX),
                None,
            ),
            "GetFunction" => (
                reqwest::Method::GET,
                format!("{}/functions/{}", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "DeleteFunction" => (
                reqwest::Method::DELETE,
                format!("{}/functions/{}", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "UpdateFunctionCode" => (
                reqwest::Method::PUT,
                format!("{}/functions/{}/code", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "UpdateFunctionConfiguration" => (
                reqwest::Method::PUT,
                format!("{}/functions/{}/configuration", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "GetFunctionConfiguration" => (
                reqwest::Method::GET,
                format!("{}/functions/{}/configuration", LAMBDA_PREFIX, FUNC),
                None,
            ),
            // Invocation
            "Invoke" => (
                reqwest::Method::POST,
                format!("{}/functions/{}/invocations", LAMBDA_PREFIX, FUNC),
                None,
            ),
            // Aliases
            "CreateAlias" => (
                reqwest::Method::POST,
                format!("{}/functions/{}/aliases", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "ListAliases" => (
                reqwest::Method::GET,
                format!("{}/functions/{}/aliases", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "GetAlias" => (
                reqwest::Method::GET,
                format!("{}/functions/{}/aliases/LATEST", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "DeleteAlias" => (
                reqwest::Method::DELETE,
                format!("{}/functions/{}/aliases/LATEST", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "UpdateAlias" => (
                reqwest::Method::PUT,
                format!("{}/functions/{}/aliases/LATEST", LAMBDA_PREFIX, FUNC),
                None,
            ),
            // Versions
            "PublishVersion" => (
                reqwest::Method::POST,
                format!("{}/functions/{}/versions", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "ListVersionsByFunction" => (
                reqwest::Method::GET,
                format!("{}/functions/{}/versions", LAMBDA_PREFIX, FUNC),
                None,
            ),
            // Event source mappings
            "CreateEventSourceMapping" => (
                reqwest::Method::POST,
                format!("{}/event-source-mappings", LAMBDA_PREFIX),
                None,
            ),
            "ListEventSourceMappings" => (
                reqwest::Method::GET,
                format!("{}/event-source-mappings", LAMBDA_PREFIX),
                None,
            ),
            "GetEventSourceMapping" => (
                reqwest::Method::GET,
                format!(
                    "{}/event-source-mappings/00000000-0000-0000-0000-000000000000",
                    LAMBDA_PREFIX
                ),
                None,
            ),
            "DeleteEventSourceMapping" => (
                reqwest::Method::DELETE,
                format!(
                    "{}/event-source-mappings/00000000-0000-0000-0000-000000000000",
                    LAMBDA_PREFIX
                ),
                None,
            ),
            "UpdateEventSourceMapping" => (
                reqwest::Method::PUT,
                format!(
                    "{}/event-source-mappings/00000000-0000-0000-0000-000000000000",
                    LAMBDA_PREFIX
                ),
                None,
            ),
            // Layers
            "PublishLayerVersion" => (
                reqwest::Method::POST,
                format!("{}/layers/test-layer/versions", LAMBDA_PREFIX),
                None,
            ),
            "ListLayers" => (
                reqwest::Method::GET,
                format!("{}/layers", LAMBDA_PREFIX),
                None,
            ),
            "ListLayerVersions" => (
                reqwest::Method::GET,
                format!("{}/layers/test-layer/versions", LAMBDA_PREFIX),
                None,
            ),
            "GetLayerVersion" => (
                reqwest::Method::GET,
                format!("{}/layers/test-layer/versions/1", LAMBDA_PREFIX),
                None,
            ),
            "DeleteLayerVersion" => (
                reqwest::Method::DELETE,
                format!("{}/layers/test-layer/versions/1", LAMBDA_PREFIX),
                None,
            ),
            // Concurrency
            "PutFunctionConcurrency" => (
                reqwest::Method::PUT,
                format!("{}/functions/{}/concurrency", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "GetFunctionConcurrency" => (
                reqwest::Method::GET,
                format!("{}/functions/{}/concurrency", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "DeleteFunctionConcurrency" => (
                reqwest::Method::DELETE,
                format!("{}/functions/{}/concurrency", LAMBDA_PREFIX, FUNC),
                None,
            ),
            // Tags
            "TagResource" => (
                reqwest::Method::POST,
                format!(
                    "{}/tags/arn:aws:lambda:us-east-1:000000000000:function:{}",
                    LAMBDA_PREFIX, FUNC
                ),
                None,
            ),
            "UntagResource" => (
                reqwest::Method::DELETE,
                format!(
                    "{}/tags/arn:aws:lambda:us-east-1:000000000000:function:{}",
                    LAMBDA_PREFIX, FUNC
                ),
                Some("tagKeys=key1".to_string()),
            ),
            "ListTags" => (
                reqwest::Method::GET,
                format!(
                    "{}/tags/arn:aws:lambda:us-east-1:000000000000:function:{}",
                    LAMBDA_PREFIX, FUNC
                ),
                None,
            ),
            // Policy
            "GetPolicy" => (
                reqwest::Method::GET,
                format!("{}/functions/{}/policy", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "AddPermission" => (
                reqwest::Method::POST,
                format!("{}/functions/{}/policy", LAMBDA_PREFIX, FUNC),
                None,
            ),
            "RemovePermission" => (
                reqwest::Method::DELETE,
                format!("{}/functions/{}/policy/test-statement", LAMBDA_PREFIX, FUNC),
                None,
            ),
            // Account settings
            "GetAccountSettings" => (
                reqwest::Method::GET,
                format!("{}/account-settings", LAMBDA_PREFIX),
                None,
            ),
            // Default: POST to functions path
            _ => (
                reqwest::Method::POST,
                format!("{}/functions", LAMBDA_PREFIX),
                None,
            ),
        },
        "s3" => match operation_name {
            // Service-level
            "ListBuckets" => (reqwest::Method::GET, "/".to_string(), None),
            // Bucket operations
            "CreateBucket" => (reqwest::Method::PUT, format!("/{}", BUCKET), None),
            "DeleteBucket" => (reqwest::Method::DELETE, format!("/{}", BUCKET), None),
            "HeadBucket" => (reqwest::Method::HEAD, format!("/{}", BUCKET), None),
            "ListObjects" | "ListObjectsV2" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("list-type=2".to_string()),
            ),
            "ListObjectVersions" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("versions".to_string()),
            ),
            // Bucket sub-resources
            "GetBucketTagging" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("tagging".to_string()),
            ),
            "PutBucketTagging" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("tagging".to_string()),
            ),
            "DeleteBucketTagging" => (
                reqwest::Method::DELETE,
                format!("/{}", BUCKET),
                Some("tagging".to_string()),
            ),
            "GetBucketVersioning" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("versioning".to_string()),
            ),
            "PutBucketVersioning" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("versioning".to_string()),
            ),
            "GetBucketLocation" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("location".to_string()),
            ),
            "GetBucketAcl" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("acl".to_string()),
            ),
            "PutBucketAcl" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("acl".to_string()),
            ),
            "GetBucketPolicy" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("policy".to_string()),
            ),
            "PutBucketPolicy" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("policy".to_string()),
            ),
            "DeleteBucketPolicy" => (
                reqwest::Method::DELETE,
                format!("/{}", BUCKET),
                Some("policy".to_string()),
            ),
            "GetBucketCors" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("cors".to_string()),
            ),
            "PutBucketCors" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("cors".to_string()),
            ),
            "DeleteBucketCors" => (
                reqwest::Method::DELETE,
                format!("/{}", BUCKET),
                Some("cors".to_string()),
            ),
            "GetBucketLifecycleConfiguration" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("lifecycle".to_string()),
            ),
            "PutBucketLifecycleConfiguration" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("lifecycle".to_string()),
            ),
            "DeleteBucketLifecycle" => (
                reqwest::Method::DELETE,
                format!("/{}", BUCKET),
                Some("lifecycle".to_string()),
            ),
            "GetBucketNotificationConfiguration" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("notification".to_string()),
            ),
            "PutBucketNotificationConfiguration" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("notification".to_string()),
            ),
            "GetBucketEncryption" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("encryption".to_string()),
            ),
            "PutBucketEncryption" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("encryption".to_string()),
            ),
            "DeleteBucketEncryption" => (
                reqwest::Method::DELETE,
                format!("/{}", BUCKET),
                Some("encryption".to_string()),
            ),
            // Object operations
            "PutObject" => (reqwest::Method::PUT, format!("/{}/{}", BUCKET, KEY), None),
            "GetObject" => (reqwest::Method::GET, format!("/{}/{}", BUCKET, KEY), None),
            "HeadObject" => (reqwest::Method::HEAD, format!("/{}/{}", BUCKET, KEY), None),
            "DeleteObject" => (
                reqwest::Method::DELETE,
                format!("/{}/{}", BUCKET, KEY),
                None,
            ),
            "CopyObject" => (reqwest::Method::PUT, format!("/{}/{}", BUCKET, KEY), None),
            "GetObjectTagging" => (
                reqwest::Method::GET,
                format!("/{}/{}", BUCKET, KEY),
                Some("tagging".to_string()),
            ),
            "PutObjectTagging" => (
                reqwest::Method::PUT,
                format!("/{}/{}", BUCKET, KEY),
                Some("tagging".to_string()),
            ),
            "DeleteObjectTagging" => (
                reqwest::Method::DELETE,
                format!("/{}/{}", BUCKET, KEY),
                Some("tagging".to_string()),
            ),
            "GetObjectAcl" => (
                reqwest::Method::GET,
                format!("/{}/{}", BUCKET, KEY),
                Some("acl".to_string()),
            ),
            "PutObjectAcl" => (
                reqwest::Method::PUT,
                format!("/{}/{}", BUCKET, KEY),
                Some("acl".to_string()),
            ),
            // Multipart upload
            "CreateMultipartUpload" => (
                reqwest::Method::POST,
                format!("/{}/{}", BUCKET, KEY),
                Some("uploads".to_string()),
            ),
            "CompleteMultipartUpload" => (
                reqwest::Method::POST,
                format!("/{}/{}", BUCKET, KEY),
                Some("uploadId=test-upload-id".to_string()),
            ),
            "AbortMultipartUpload" => (
                reqwest::Method::DELETE,
                format!("/{}/{}", BUCKET, KEY),
                Some("uploadId=test-upload-id".to_string()),
            ),
            "UploadPart" => (
                reqwest::Method::PUT,
                format!("/{}/{}", BUCKET, KEY),
                Some("partNumber=1&uploadId=test-upload-id".to_string()),
            ),
            "ListMultipartUploads" => (
                reqwest::Method::GET,
                format!("/{}", BUCKET),
                Some("uploads".to_string()),
            ),
            "ListParts" => (
                reqwest::Method::GET,
                format!("/{}/{}", BUCKET, KEY),
                Some("uploadId=test-upload-id".to_string()),
            ),
            // Batch delete
            "DeleteObjects" => (
                reqwest::Method::POST,
                format!("/{}", BUCKET),
                Some("delete".to_string()),
            ),
            // Default: GET on the bucket
            _ => (reqwest::Method::GET, format!("/{}", BUCKET), None),
        },
        // Unknown REST service: fall back to POST at root
        _ => (reqwest::Method::POST, "/".to_string(), None),
    }
}

fn probe_rest(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
) -> Result<(u16, String), String> {
    let (method, path, query) = rest_request_config(service_name, operation_name);

    let url = match query {
        Some(qs) => format!("{}{}?{}", endpoint, path, qs),
        None => format!("{}{}", endpoint, path),
    };

    let has_body = matches!(method, reqwest::Method::POST | reqwest::Method::PUT);
    let body = if has_body {
        serde_json::to_string(&variant.input).unwrap_or_else(|_| "{}".to_string())
    } else {
        String::new()
    };

    let mut req = client.request(method, &url).header(
        "Authorization",
        format!(
            "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/{}/aws4_request",
            service_name
        ),
    );

    if has_body {
        req = req.header("Content-Type", "application/json").body(body);
    }

    let resp = req.send().map_err(|e| e.to_string())?;

    let status = resp.status().as_u16();
    let body = resp.text().map_err(|e| e.to_string())?;
    Ok((status, body))
}

fn classify_response(
    variant_name: &str,
    http_status: u16,
    body: &str,
    expectation: &Expectation,
    duration_ms: u64,
) -> ProbeResult {
    let is_not_implemented = body.contains("not implemented")
        || body.contains("NotImplemented")
        || body.contains("UnknownAction")
        || body.contains("InvalidAction");

    if is_not_implemented {
        return ProbeResult {
            variant_name: variant_name.to_string(),
            status: ProbeStatus::NotImplemented,
            http_status,
            response_body: body.to_string(),
            duration_ms,
        };
    }

    if http_status == 500 {
        return ProbeResult {
            variant_name: variant_name.to_string(),
            status: ProbeStatus::Crash(format!("HTTP 500: {}", truncate(body, 200))),
            http_status,
            response_body: body.to_string(),
            duration_ms,
        };
    }

    let status = match expectation {
        Expectation::Success => {
            if (200..500).contains(&http_status) {
                // 2xx = genuine success.
                // 4xx = also treated as Pass because most "success" probes send synthetic
                // placeholder data (no real fixtures), which triggers AWS validation errors
                // (e.g. ResourceNotFoundException, ValidationException). The important signal
                // is that the action was routed and processed — not that the dummy data was
                // accepted. Once fixture support is added, this should be tightened to
                // distinguish real validation errors from routing failures.
                ProbeStatus::Pass
            } else {
                ProbeStatus::UnexpectedResult(format!("Expected success, got HTTP {}", http_status))
            }
        }
        Expectation::AnyError => {
            if http_status >= 400 {
                ProbeStatus::Pass
            } else {
                ProbeStatus::UnexpectedResult(format!("Expected error, got HTTP {}", http_status))
            }
        }
        Expectation::Error(expected_code) => {
            if body.contains(expected_code) {
                ProbeStatus::Pass
            } else if http_status >= 400 {
                ProbeStatus::UnexpectedResult(format!(
                    "Expected error '{}', got HTTP {} with different error",
                    expected_code, http_status
                ))
            } else {
                ProbeStatus::UnexpectedResult(format!(
                    "Expected error '{}', got HTTP {}",
                    expected_code, http_status
                ))
            }
        }
    };

    ProbeResult {
        variant_name: variant_name.to_string(),
        status,
        http_status,
        response_body: body.to_string(),
        duration_ms,
    }
}

fn flatten_to_form_params(
    map: &serde_json::Map<String, Value>,
    prefix: &str,
    params: &mut Vec<(String, String)>,
) {
    for (key, value) in map {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        match value {
            Value::String(s) => params.push((full_key, s.clone())),
            Value::Number(n) => params.push((full_key, n.to_string())),
            Value::Bool(b) => params.push((full_key, b.to_string())),
            Value::Object(nested) => flatten_to_form_params(nested, &full_key, params),
            Value::Array(arr) => {
                for (i, item) in arr.iter().enumerate() {
                    let item_key = format!("{}.member.{}", full_key, i + 1);
                    match item {
                        Value::String(s) => params.push((item_key, s.clone())),
                        Value::Number(n) => params.push((item_key, n.to_string())),
                        Value::Bool(b) => params.push((item_key, b.to_string())),
                        Value::Object(nested) => flatten_to_form_params(nested, &item_key, params),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}

fn urlencoded(s: &str) -> String {
    s.replace('%', "%25")
        .replace('&', "%26")
        .replace('=', "%3D")
        .replace('+', "%2B")
        .replace(' ', "%20")
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() <= max {
        s
    } else {
        // Find a char boundary at or before `max` to avoid panicking on multi-byte chars.
        let boundary = s
            .char_indices()
            .map(|(i, _)| i)
            .take_while(|&i| i <= max)
            .last()
            .unwrap_or(0);
        &s[..boundary]
    }
}
