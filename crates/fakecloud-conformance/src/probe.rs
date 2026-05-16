//! Level 1 probing: send generated requests to fakecloud and classify responses.

use std::collections::HashMap;

use serde_json::Value;

use crate::generators::{Expectation, TestVariant};
use crate::shape_validator;
use crate::smithy::ServiceModel;

/// Protocol used by a service for request/response encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
        "dynamodbstreams" => Protocol::Json {
            target_prefix: "DynamoDBStreams_20120810",
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
        "cognito-identity" => Protocol::Json {
            target_prefix: "AWSCognitoIdentityService",
        },
        "kinesis" => Protocol::Json {
            target_prefix: "Kinesis_20131202",
        },
        "ecr" => Protocol::Json {
            target_prefix: "AmazonEC2ContainerRegistry_V20150921",
        },
        "ecs" => Protocol::Json {
            target_prefix: "AmazonEC2ContainerServiceV20141113",
        },
        // Smithy service_name for Step Functions is `states`; SDK calls it SFN.
        "states" => Protocol::Json {
            target_prefix: "AWSStepFunctions",
        },
        "organizations" => Protocol::Json {
            target_prefix: "AWSOrganizationsV20161128",
        },
        "acm" => Protocol::Json {
            target_prefix: "CertificateManager",
        },
        "application-autoscaling" => Protocol::Json {
            target_prefix: "AnyScaleFrontendService",
        },
        "wafv2" => Protocol::Json {
            target_prefix: "AWSWAF_20190729",
        },
        "athena" => Protocol::Json {
            target_prefix: "AmazonAthena",
        },
        "firehose" => Protocol::Json {
            target_prefix: "Firehose_20150804",
        },
        "glue" => Protocol::Json {
            target_prefix: "AWSGlue",
        },
        "s3" => Protocol::Rest,
        "lambda" => Protocol::Rest,
        // API Gateway v1 (REST APIs) and v2 (HTTP APIs) are separate
        // Smithy models with distinct `service_name` entries in
        // `service-map.json`. fakecloud's facade routes both behind the
        // single SigV4 service identifier `apigateway`, but probing
        // keeps them separate. restJson1 wire format for both.
        "apigateway" | "apigatewayv1" | "apigatewayv2" => Protocol::Rest,
        // restJson1 services — REST routing with @http traits + JSON bodies.
        "ses" => Protocol::Rest,
        "bedrock" => Protocol::Rest,
        "bedrock-runtime" => Protocol::Rest,
        "bedrock-agent" => Protocol::Rest,
        "bedrock-agent-runtime" => Protocol::Rest,
        "scheduler" => Protocol::Rest,
        // REST-XML services — distinct wire format from restJson1 but the
        // probe uses the same `@http` trait-driven URL builder for both
        // and reads response bodies as opaque text.
        "route53" => Protocol::Rest,
        "cloudfront" => Protocol::Rest,
        // awsQuery services — RDS, ElastiCache, ELBv2 — explicitly listed
        // for clarity instead of relying on the default fall-through.
        "rds" => Protocol::Query,
        "elasticache" => Protocol::Query,
        "elasticloadbalancing" => Protocol::Query,
        _ => Protocol::Query,
    }
}

/// Services whose wire format is awsJson1.x but which carry the
/// `aws.protocols#awsQueryCompatible` trait, so their `__type` values
/// follow the awsQuery `<Code>` convention (preserved for legacy
/// SDK compatibility). The shape-level `awsQueryError` trait is the
/// authoritative wire code for these services even though they speak
/// JSON. Currently SQS is the only such service in the vendored models.
fn is_aws_query_compatible_service(service_name: &str) -> bool {
    matches!(service_name, "sqs")
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
        Protocol::Rest => probe_rest(
            client,
            endpoint,
            service_name,
            operation_name,
            variant,
            model_info.map(|(m, _)| m),
        ),
    };

    let duration_ms = start.elapsed().as_millis() as u64;

    // Resolve the operation's declared error wire codes once so the
    // classifier can distinguish real handler-emitted exceptions from
    // fakecloud's own routing-miss 4xxs.
    //
    // Strict mode: only the op's own `errors:` list counts. We do NOT union
    // every @error-tagged shape from the service model (that was the #1342
    // loosening) — if a service genuinely emits an error AWS Smithy doesn't
    // declare, we want it surfaced as UNDECLARED_ERROR so it gets reported
    // against the right service, not silently accepted.
    //
    // For each declared shape we derive the wire code: the
    // `aws.protocols#awsQueryError.code` trait if present AND the service
    // wire format is awsQuery or awsQueryCompatible (covers IAM/RDS/etc.
    // where the shape name and `<Code>` differ, and SQS which is awsJson1.0
    // + awsQueryCompatible), else the shape's short name (after `#`).
    //
    // Why the protocol gate: a handful of awsJson1.x services (KMS, ACM,
    // DynamoDB, SSM, application-autoscaling) carry `awsQueryError` traits
    // on their error shapes for historical reasons (the same shape is
    // shared with sibling awsQuery models). For awsJson1.x wire encoding
    // the trait is inert — the wire `__type` is the shape's short name.
    // Using the trait there mis-resolves codes like `NotFoundException`
    // to `NotFound` and flags every real handler error as undeclared.
    let honor_query_error_trait =
        matches!(protocol, Protocol::Query) || is_aws_query_compatible_service(service_name);
    let op_error_shapes: Option<Vec<String>> = model_info.map(|(m, _)| {
        let declared_shape_ids: Vec<String> = m
            .operations
            .iter()
            .find(|o| o.name == operation_name)
            .map(|op| op.error_shapes.clone())
            .unwrap_or_default();
        declared_shape_ids
            .iter()
            .map(|shape_id| {
                if honor_query_error_trait {
                    if let Some(shape) = m.shapes.get(shape_id) {
                        if let Some(code) = &shape.traits.aws_query_error_code {
                            return code.clone();
                        }
                    }
                }
                shape_id.rsplit('#').next().unwrap_or(shape_id).to_string()
            })
            .collect()
    });

    match result {
        Ok((status_code, body)) => {
            let mut probe_result = classify_response(
                &variant.name,
                status_code,
                &body,
                &variant.expectation,
                duration_ms,
                op_error_shapes.as_deref(),
                service_name,
            );

            // Run shape validation on successful responses
            if probe_result.status == ProbeStatus::Pass
                && (200..300).contains(&status_code)
                && !body.is_empty()
            {
                let mut all_violations = Vec::new();
                if let Some((model, output_shape_id)) = model_info {
                    all_violations.extend(shape_validator::validate_response(
                        model,
                        output_shape_id,
                        &body,
                        protocol,
                    ));
                }
                // Strategy 7 (`examples_diff`): the variant carries a documented
                // response from the operation's `@examples` trait. Deep-diff
                // it against the live response — every leaf in the documented
                // output must exist (with matching JSON type) in actual. Catches
                // optional-but-always-present fields that shape_validator can't
                // see (#816).
                if let Some(documented) = variant.expected_output.as_ref() {
                    if let Ok(actual) = serde_json::from_str::<serde_json::Value>(&body) {
                        all_violations
                            .extend(shape_validator::diff_against_example(&actual, documented));
                    }
                }
                // Strategy 8 (`round_trip`): chase the Create with the
                // discovered Get/Describe, assert each input field echoed.
                // Only meaningful when we have a model to find the followup
                // operation in.
                if let (Some(followup), Some((model, _))) = (variant.followup.as_ref(), model_info)
                {
                    let response_body: Option<serde_json::Value> = serde_json::from_str(&body).ok();
                    all_violations.extend(run_round_trip_followup(
                        client,
                        endpoint,
                        service_name,
                        variant,
                        response_body.as_ref(),
                        followup,
                        model,
                    ));
                }
                if !all_violations.is_empty() {
                    let msg = all_violations
                        .iter()
                        .take(5)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join("; ");
                    probe_result.status = ProbeStatus::ShapeMismatch(msg);
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
fn sigv4_auth_header(service_name: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/{}/aws4_request, \
         SignedHeaders=host;x-amz-date, Signature=00",
        service_name
    )
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
        .header("Authorization", sigv4_auth_header("service"))
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
            // Layers — AWS uses the `2018-10-31` prefix, not `LAMBDA_PREFIX`
            // (`2015-03-31`). The Lambda service router requires the
            // 2018-10-31 date for layer paths.
            "PublishLayerVersion" => (
                reqwest::Method::POST,
                "/2018-10-31/layers/test-layer/versions".to_string(),
                None,
            ),
            "ListLayers" => (reqwest::Method::GET, "/2018-10-31/layers".to_string(), None),
            "ListLayerVersions" => (
                reqwest::Method::GET,
                "/2018-10-31/layers/test-layer/versions".to_string(),
                None,
            ),
            "GetLayerVersion" => (
                reqwest::Method::GET,
                "/2018-10-31/layers/test-layer/versions/__VERSION__".to_string(),
                None,
            ),
            "DeleteLayerVersion" => (
                reqwest::Method::DELETE,
                "/2018-10-31/layers/test-layer/versions/__VERSION__".to_string(),
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
            // Function URL config (uses 2021-10-31 prefix in AWS but the
            // route is prefix-agnostic in fakecloud).
            "CreateFunctionUrlConfig" => (
                reqwest::Method::POST,
                format!("/2021-10-31/functions/{}/url", FUNC),
                None,
            ),
            "GetFunctionUrlConfig" => (
                reqwest::Method::GET,
                format!("/2021-10-31/functions/{}/url", FUNC),
                None,
            ),
            "UpdateFunctionUrlConfig" => (
                reqwest::Method::PUT,
                format!("/2021-10-31/functions/{}/url", FUNC),
                None,
            ),
            "DeleteFunctionUrlConfig" => (
                reqwest::Method::DELETE,
                format!("/2021-10-31/functions/{}/url", FUNC),
                None,
            ),
            "ListFunctionUrlConfigs" => (
                reqwest::Method::GET,
                format!("/2021-10-31/functions/{}/urls", FUNC),
                None,
            ),
            // Per-function code-signing config (2020-06-30 in AWS).
            "PutFunctionCodeSigningConfig" => (
                reqwest::Method::PUT,
                format!("/2020-06-30/functions/{}/code-signing-config", FUNC),
                None,
            ),
            "GetFunctionCodeSigningConfig" => (
                reqwest::Method::GET,
                format!("/2020-06-30/functions/{}/code-signing-config", FUNC),
                None,
            ),
            "DeleteFunctionCodeSigningConfig" => (
                reqwest::Method::DELETE,
                format!("/2020-06-30/functions/{}/code-signing-config", FUNC),
                None,
            ),
            // Runtime management config (2021-07-20 in AWS).
            "PutRuntimeManagementConfig" => (
                reqwest::Method::PUT,
                format!("/2021-07-20/functions/{}/runtime-management-config", FUNC),
                None,
            ),
            "GetRuntimeManagementConfig" => (
                reqwest::Method::GET,
                format!("/2021-07-20/functions/{}/runtime-management-config", FUNC),
                None,
            ),
            // Async invocation (2014-11-13 in AWS, but Lambda's router
            // accepts any well-formed date prefix).
            "InvokeAsync" => (
                reqwest::Method::POST,
                format!("/2014-11-13/functions/{}/invoke-async", FUNC),
                None,
            ),
            // Response-streaming invocation (2021-11-15).
            "InvokeWithResponseStream" => (
                reqwest::Method::POST,
                format!(
                    "/2021-11-15/functions/{}/response-streaming-invocations",
                    FUNC
                ),
                None,
            ),
            // Event invoke configuration (2019-09-25).
            "PutFunctionEventInvokeConfig" => (
                reqwest::Method::PUT,
                format!("/2019-09-25/functions/{}/event-invoke-config", FUNC),
                None,
            ),
            "UpdateFunctionEventInvokeConfig" => (
                reqwest::Method::POST,
                format!("/2019-09-25/functions/{}/event-invoke-config", FUNC),
                None,
            ),
            "GetFunctionEventInvokeConfig" => (
                reqwest::Method::GET,
                format!("/2019-09-25/functions/{}/event-invoke-config", FUNC),
                None,
            ),
            "DeleteFunctionEventInvokeConfig" => (
                reqwest::Method::DELETE,
                format!("/2019-09-25/functions/{}/event-invoke-config", FUNC),
                None,
            ),
            "ListFunctionEventInvokeConfigs" => (
                reqwest::Method::GET,
                format!("/2019-09-25/functions/{}/event-invoke-config/list", FUNC),
                None,
            ),
            // Provisioned concurrency (2019-09-30).
            "PutProvisionedConcurrencyConfig" => (
                reqwest::Method::PUT,
                format!("/2019-09-30/functions/{}/provisioned-concurrency", FUNC),
                None,
            ),
            "GetProvisionedConcurrencyConfig" => (
                reqwest::Method::GET,
                format!("/2019-09-30/functions/{}/provisioned-concurrency", FUNC),
                None,
            ),
            "DeleteProvisionedConcurrencyConfig" => (
                reqwest::Method::DELETE,
                format!("/2019-09-30/functions/{}/provisioned-concurrency", FUNC),
                None,
            ),
            "ListProvisionedConcurrencyConfigs" => (
                reqwest::Method::GET,
                format!("/2019-09-30/functions/{}/provisioned-concurrency", FUNC),
                Some("List=ALL".to_string()),
            ),
            // Recursion config (2024-08-31).
            "PutFunctionRecursionConfig" => (
                reqwest::Method::PUT,
                format!("/2024-08-31/functions/{}/recursion-config", FUNC),
                None,
            ),
            "GetFunctionRecursionConfig" => (
                reqwest::Method::GET,
                format!("/2024-08-31/functions/{}/recursion-config", FUNC),
                None,
            ),
            // Scaling config (2025-11-30).
            "PutFunctionScalingConfig" => (
                reqwest::Method::PUT,
                format!("/2025-11-30/functions/{}/function-scaling-config", FUNC),
                None,
            ),
            "GetFunctionScalingConfig" => (
                reqwest::Method::GET,
                format!("/2025-11-30/functions/{}/function-scaling-config", FUNC),
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

/// REST services with a hand-curated `rest_request_config` table. These keep
/// their hardcoded entries; everything else falls back to the generic
/// `@http`-trait-driven request builder.
const SERVICES_WITH_HARDCODED_REST: &[&str] = &["lambda", "s3"];

fn probe_rest(
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

/// `(method, url, headers, body)` tuple produced by the REST request builders.
type RestRequestParts = (
    reqwest::Method,
    String,
    Vec<(String, String)>,
    Option<String>,
);

/// Append the `@httpQuery`-bound members of an operation's input shape
/// to a legacy-routed URL. Used for Lambda / S3 where the path is
/// hand-curated but query parameters still need to be honored so
/// negative/boundary variants can target them.
fn append_http_query_from_model(
    base_url: &str,
    model: &ServiceModel,
    operation_name: &str,
    input: &serde_json::Value,
) -> String {
    use crate::smithy::ShapeType;

    let obj = match input.as_object() {
        Some(o) => o,
        None => return base_url.to_string(),
    };
    let op = match model.operations.iter().find(|o| o.name == operation_name) {
        Some(op) => op,
        None => return base_url.to_string(),
    };
    let members: Vec<crate::smithy::Member> = op
        .input_shape
        .as_ref()
        .and_then(|id| model.shapes.get(id))
        .and_then(|shape| match &shape.shape_type {
            ShapeType::Structure { members } => Some(members.clone()),
            _ => None,
        })
        .unwrap_or_default();

    let mut additions: Vec<String> = Vec::new();
    for member in &members {
        let target_traits = model.shapes.get(&member.target).map(|s| &s.traits);
        let query_name = member
            .traits
            .http_query
            .clone()
            .or_else(|| target_traits.and_then(|t| t.http_query.clone()));
        if let Some(qk) = query_name {
            if let Some(val) = obj.get(&member.name) {
                let mut parts: Vec<String> = Vec::new();
                append_query(&mut parts, &qk, val);
                additions.extend(parts);
            }
        }
    }
    if additions.is_empty() {
        return base_url.to_string();
    }
    let joiner = if base_url.contains('?') { '&' } else { '?' };
    format!("{}{}{}", base_url, joiner, additions.join("&"))
}

/// Preserve the pre-existing hardcoded-table behavior for Lambda / S3.
fn legacy_rest_request(
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
) -> RestRequestParts {
    let (method, path, query) = rest_request_config(service_name, operation_name);
    // Hardcoded paths in `rest_request_config` use placeholder identifiers
    // (`test-conformance-function`, `test-conformance-bucket`, `test-key`).
    // Strategies that need to exercise alternative identifier forms
    // (id_forms — bare/ARN/partial) inject the desired value into
    // `variant.input`; substitute it in here so the URL actually carries
    // the form being tested. Without this swap, the probe would silently
    // send the placeholder name and the new forms would be invisible at
    // the wire layer.
    let path = legacy_substitute_identifiers(&path, service_name, &variant.input);
    let url = match query {
        Some(qs) => format!("{}{}?{}", endpoint, path, qs),
        None => format!("{}{}", endpoint, path),
    };
    let has_body = matches!(method, reqwest::Method::POST | reqwest::Method::PUT);
    let body = if has_body {
        Some(serde_json::to_string(&variant.input).unwrap_or_else(|_| "{}".to_string()))
    } else {
        None
    };
    (method, url, Vec::new(), body)
}

/// Replace the hardcoded placeholder identifiers in legacy REST paths
/// with the value the variant put in its input, when present. Mirrors
/// the substitution `build_http_request_from_model` does via
/// `@httpLabel`, but for the hand-curated Lambda/S3 routing tables.
fn legacy_substitute_identifiers(
    path: &str,
    service_name: &str,
    input: &serde_json::Value,
) -> String {
    let obj = match input.as_object() {
        Some(o) => o,
        None => return path.to_string(),
    };
    let mut out = path.to_string();
    let subs: &[(&str, &str)] = match service_name {
        "lambda" => &[
            ("test-conformance-function", "FunctionName"),
            // Layer ops route on `LayerName` (and an optional
            // numeric `VersionNumber`). Substitute from the variant
            // so negative/boundary variants reach those routes too.
            ("test-layer", "LayerName"),
            ("__VERSION__", "VersionNumber"),
            // Alias ops use `LATEST` as the path placeholder. Drive
            // negative variants through the same slot.
            ("LATEST", "Name"),
        ],
        "s3" => &[("test-conformance-bucket", "Bucket"), ("test-key", "Key")],
        _ => &[],
    };
    for (placeholder, member) in subs {
        match obj.get(*member) {
            Some(serde_json::Value::String(value)) => {
                // The legacy path is unencoded; the variant value may be raw
                // ARN or already URL-encoded. Trust the variant — the
                // id_forms strategy decides whether to URL-encode.
                out = out.replace(placeholder, value);
            }
            Some(serde_json::Value::Number(value)) => {
                // Numeric httpLabel members (e.g. Lambda `VersionNumber`)
                // need to be stringified before the path substitution.
                out = out.replace(placeholder, &value.to_string());
            }
            None => {
                // The variant omitted this httpLabel member entirely
                // (e.g. `negative_omit_FunctionName`). Real AWS would
                // never see such a request — the SDK refuses to build
                // one — but synthetic probes can. Substitute a
                // clearly-invalid sentinel for S3 so a path like
                // `GET /{Bucket}?prefix=...` doesn't collapse to `/` and
                // hit ListBuckets (a 200 happy-path response). Uppercase
                // + underscores violate S3's bucket naming rules, so the
                // server reliably returns InvalidBucketName. Other
                // services keep the empty-string collapse — their
                // dispatchers reject empty path labels cleanly.
                let sentinel = if service_name == "s3" {
                    "INVALID_OMITTED_LABEL"
                } else {
                    ""
                };
                out = out.replace(placeholder, sentinel);
            }
            _ => {}
        }
    }
    out
}

/// Build an HTTP request for an operation from the `@http` / `@httpLabel` /
/// `@httpQuery` / `@httpHeader` / `@httpPayload` Smithy traits on its input
/// shape. Returns `None` if the operation is missing `@http` metadata.
///
/// Returned tuple: `(method, path_and_query_string, headers, body)`.
fn build_http_request_from_model(
    op: &crate::smithy::Operation,
    model: &ServiceModel,
    input: &serde_json::Value,
) -> Option<RestRequestParts> {
    use crate::smithy::ShapeType;

    let method_str = op.http_method.as_ref()?;
    let uri_template = op.http_uri.as_ref()?;

    let method = reqwest::Method::from_bytes(method_str.as_bytes()).ok()?;

    // Clone input so we can progressively drain label/query/header/payload members.
    let mut remaining = match input {
        serde_json::Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };

    let mut headers: Vec<(String, String)> = Vec::new();
    let mut query_parts: Vec<String> = Vec::new();
    let mut payload_value: Option<serde_json::Value> = None;

    // Walk the input shape's members to discover http bindings. Label / query /
    // header / payload traits can live on either the member or the referenced
    // target shape — check both.
    let members: Vec<crate::smithy::Member> = op
        .input_shape
        .as_ref()
        .and_then(|id| model.shapes.get(id))
        .and_then(|shape| match &shape.shape_type {
            ShapeType::Structure { members } => Some(members.clone()),
            _ => None,
        })
        .unwrap_or_default();

    for member in &members {
        let target_traits = model.shapes.get(&member.target).map(|s| &s.traits);
        let member_traits = &member.traits;
        let is_label =
            member_traits.http_label || target_traits.map(|t| t.http_label).unwrap_or(false);
        let query_name = member_traits
            .http_query
            .clone()
            .or_else(|| target_traits.and_then(|t| t.http_query.clone()));
        let header_name = member_traits
            .http_header
            .clone()
            .or_else(|| target_traits.and_then(|t| t.http_header.clone()));
        let is_payload =
            member_traits.http_payload || target_traits.map(|t| t.http_payload).unwrap_or(false);

        if is_label || query_name.is_some() || header_name.is_some() || is_payload {
            if let Some(val) = remaining.remove(&member.name) {
                if let Some(qk) = query_name {
                    append_query(&mut query_parts, &qk, &val);
                } else if let Some(hk) = header_name {
                    if let Some(hv) = value_to_header_string(&val) {
                        headers.push((hk, hv));
                    }
                } else if is_payload {
                    payload_value = Some(val);
                }
                // is_label handled below from `remaining` ∪ `members` (we already
                // removed; re-use the popped value via a side map)
                if is_label {
                    // Re-insert for label substitution below. Simpler: keep a
                    // separate label map rather than re-reading `remaining`.
                }
            }
        }
    }

    // Second pass: collect label values fresh from the original input, since we
    // may have removed them above.
    let mut label_values: HashMap<String, serde_json::Value> = HashMap::new();
    for member in &members {
        let target_traits = model.shapes.get(&member.target).map(|s| &s.traits);
        let is_label =
            member.traits.http_label || target_traits.map(|t| t.http_label).unwrap_or(false);
        if is_label {
            if let Some(val) = input.get(&member.name) {
                label_values.insert(member.name.clone(), val.clone());
            }
        }
    }

    let path = substitute_uri_labels(uri_template, &label_values);

    // Merge literal query (from URI template after `?`) with computed params.
    let (path_only, literal_query) = match path.split_once('?') {
        Some((p, q)) => (p.to_string(), Some(q.to_string())),
        None => (path, None),
    };

    let mut all_query: Vec<String> = Vec::new();
    if let Some(lq) = literal_query {
        if !lq.is_empty() {
            all_query.push(lq);
        }
    }
    all_query.extend(query_parts);
    let path_and_query = if all_query.is_empty() {
        path_only
    } else {
        format!("{}?{}", path_only, all_query.join("&"))
    };

    // Body: @httpPayload member wins; else whatever remains in the input object
    // (minus labels, query, headers, payload). Omit body entirely on
    // GET/HEAD/DELETE unless an explicit @httpPayload member was present.
    let is_bodyless_method = matches!(
        method,
        reqwest::Method::GET | reqwest::Method::HEAD | reqwest::Method::DELETE
    );
    let body = if let Some(v) = payload_value {
        Some(value_to_body(&v))
    } else if is_bodyless_method {
        None
    } else {
        // Drop label members from `remaining` (we re-added none, but be defensive).
        for name in label_values.keys() {
            remaining.remove(name);
        }
        if remaining.is_empty() {
            Some("{}".to_string())
        } else {
            Some(serde_json::to_string(&serde_json::Value::Object(remaining)).unwrap_or_default())
        }
    };

    Some((method, path_and_query, headers, body))
}

fn substitute_uri_labels(template: &str, labels: &HashMap<String, serde_json::Value>) -> String {
    // URI templates use `{Name}` and `{Name+}` (greedy, keeps `/` literal).
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            if let Some(end) = template[i + 1..].find('}') {
                let inner = &template[i + 1..i + 1 + end];
                let (name, greedy) = if let Some(n) = inner.strip_suffix('+') {
                    (n, true)
                } else {
                    (inner, false)
                };
                let raw = labels.get(name).and_then(|v| match v {
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    serde_json::Value::Bool(b) => Some(b.to_string()),
                    _ => None,
                });
                if let Some(raw) = raw {
                    let encoded = if greedy {
                        percent_encode_greedy(&raw)
                    } else {
                        percent_encode_label(&raw)
                    };
                    out.push_str(&encoded);
                } else {
                    // No value: leave the literal {Name} in place so the
                    // server-side failure surfaces as a mismatch rather than a
                    // silent 500.
                    out.push_str(&template[i..i + 1 + end + 1]);
                }
                i += 1 + end + 1;
                continue;
            }
        }
        out.push(template[i..].chars().next().unwrap());
        i += template[i..].chars().next().unwrap().len_utf8();
    }
    out
}

/// Percent-encode a path segment label. Keeps `-._~` unencoded (RFC 3986
/// unreserved) and encodes `/` so segment boundaries are preserved.
fn percent_encode_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char);
            }
            other => {
                out.push_str(&format!("%{:02X}", other));
            }
        }
    }
    out
}

/// Greedy-label encoding (`{Name+}`): same as label encoding but keeps `/`.
fn percent_encode_greedy(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(*b as char);
            }
            other => {
                out.push_str(&format!("%{:02X}", other));
            }
        }
    }
    out
}

fn append_query(out: &mut Vec<String>, key: &str, v: &serde_json::Value) {
    match v {
        serde_json::Value::String(s) => out.push(format!("{}={}", key, percent_encode_label(s))),
        serde_json::Value::Number(n) => out.push(format!("{}={}", key, n)),
        serde_json::Value::Bool(b) => out.push(format!("{}={}", key, b)),
        serde_json::Value::Array(items) => {
            for item in items {
                append_query(out, key, item);
            }
        }
        serde_json::Value::Null | serde_json::Value::Object(_) => {}
    }
}

fn value_to_header_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// For `@httpPayload` members: structures/lists/objects become JSON; strings
/// become the raw string; blobs (JSON-encoded as strings here) likewise.
fn value_to_body(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        _ => serde_json::to_string(v).unwrap_or_default(),
    }
}

/// Classify the response when the variant expected a successful outcome.
///
/// Pre-`Expectation::Success` policy was: any 2xx-or-4xx == Pass. Reasoning
/// at the time was that synthetic placeholder inputs make a 4xx
/// (ResourceNotFoundException, ValidationException) the *expected* shape
/// for an implemented handler. The trade-off was that fakecloud's *own*
/// routing-miss 4xxs (returning 404 for a URL form the router didn't
/// know how to dispatch — exactly #817) were indistinguishable from
/// real handler-emitted 4xxs and slipped through as Pass.
///
/// New policy splits 4xx by what the body looks like:
/// - 4xx with an AWS-shaped error code in the body (`__type` JSON field
///   or `<Code>` XML element) — handler ran, returned a real exception.
///   If the op declares `error_shapes`, the code must short-name-match
///   one of them (Smithy declares which exceptions an op can raise).
///   Pass.
/// - 4xx with no recognisable AWS error code, OR with a code that's
///   absent from the op's declared `error_shapes` — likely fakecloud's
///   own routing-miss / unhandled-form response. Fail.
///
/// Net effect: signed routing reaches a handler -> Pass. Routing
/// silently misses (404 with body that doesn't match Smithy) -> Fail.
fn classify_success_expectation(
    http_status: u16,
    body: &str,
    op_error_shapes: Option<&[String]>,
    service_name: &str,
) -> ProbeStatus {
    if (200..300).contains(&http_status) {
        return ProbeStatus::Pass;
    }
    if !(400..500).contains(&http_status) {
        return ProbeStatus::UnexpectedResult(format!(
            "Expected success, got HTTP {}",
            http_status
        ));
    }
    let code = match extract_aws_error_code(body) {
        Some(c) => c,
        None => {
            return ProbeStatus::UnexpectedResult(format!(
                "HTTP {} with no AWS error code in body (likely routing miss): {}",
                http_status,
                truncate(body, 200)
            ));
        }
    };
    // Some services publish "shared error responses" outside per-operation
    // Smithy `errors:` lists — codes the model implicitly inherits across
    // every op that touches the same resource family. S3 is the canonical
    // example: <https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html>
    // documents `NoSuchBucket`, `NoSuchKey`, `InvalidBucketName`, and
    // `AccessDenied` as universal responses any bucket/object-taking op can
    // return, but the AWS Smithy file only enumerates them on a handful of
    // operations (`HeadBucket` -> NotFound, `CreateSession` -> NoSuchBucket,
    // etc.). Accepting these as handler-emitted errors here keeps the
    // classifier aligned with the published API contract rather than the
    // incomplete Smithy enumeration.
    if service_common_errors(service_name)
        .iter()
        .any(|c| *c == code)
    {
        return ProbeStatus::Pass;
    }
    // Op model available -> require the code to be in its declared errors.
    // Op model unavailable (no model for this service or unknown op) -> any
    // AWS-shaped error counts as a handler response.
    if let Some(declared) = op_error_shapes {
        if declared.is_empty() || matches_declared_error(&code, declared) {
            ProbeStatus::Pass
        } else {
            ProbeStatus::UnexpectedResult(format!(
                "HTTP {} with undeclared error '{}' (not in op's Smithy error_shapes)",
                http_status, code
            ))
        }
    } else {
        ProbeStatus::Pass
    }
}

/// Pull the AWS error code out of a response body. Handles all four
/// AWS wire forms:
///   - JSON: `{"__type":"X"}` or `{"__type":"com.amazonaws.svc#X"}`
///     (restJson1 / awsJson1.1)
///   - JSON: `{"code":"X"}` / `{"Code":"X"}` (Smithy fallbacks some
///     services use)
///   - XML: `<Error><Code>X</Code></Error>` (restXml — S3, CloudFront)
///   - XML: `<ErrorResponse><Error><Code>X</Code></Error></ErrorResponse>`
///     (awsQuery — IAM, RDS, SNS, ELB, CFN, STS)
///
/// Returns the short name (after `#` if present). Returns `None` when
/// the body has no recognisable AWS error code — the signal we care
/// about for distinguishing real handler responses from routing misses.
fn extract_aws_error_code(body: &str) -> Option<String> {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
        for key in ["__type", "Code", "code"] {
            if let Some(s) = v.get(key).and_then(|x| x.as_str()) {
                return Some(short_error_name(s));
            }
        }
        // Some restJson1 services nest under `Error` or `error`.
        for outer in ["Error", "error"] {
            if let Some(inner) = v.get(outer) {
                for key in ["Code", "code", "__type"] {
                    if let Some(s) = inner.get(key).and_then(|x| x.as_str()) {
                        return Some(short_error_name(s));
                    }
                }
            }
        }
    }
    // XML <Code>X</Code> — first occurrence wins.
    if let Some(start) = body.find("<Code>") {
        let after = &body[start + "<Code>".len()..];
        if let Some(end) = after.find("</Code>") {
            return Some(after[..end].trim().to_string());
        }
    }
    None
}

/// Strip Smithy namespace from an error type. `com.amazonaws.lambda#X` -> `X`.
fn short_error_name(s: &str) -> String {
    let after_hash = s.rsplit('#').next().unwrap_or(s);
    // Some services prefix with shape namespace via colon syntax.
    let after_colon = after_hash.rsplit(':').next().unwrap_or(after_hash);
    after_colon.trim().to_string()
}

/// Service-wide error codes that AWS documents as "shared error responses"
/// applicable to any operation in the service, even when the per-op Smithy
/// `errors:` list omits them.
///
/// The list comes from each service's published API reference, not from
/// fakecloud's own behaviour. For services with no shared-error
/// documentation this returns an empty slice and the strict per-op rule
/// applies verbatim.
fn service_common_errors(service_name: &str) -> &'static [&'static str] {
    match service_name {
        // S3 "Common Error Responses": every operation can return these on
        // a missing/forbidden bucket or object. Source:
        // https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
        // Anything else (validation errors, conditional-write violations,
        // op-specific failures) still has to match the per-op `errors:` list.
        "s3" => &[
            "NoSuchBucket",
            "NoSuchKey",
            "InvalidBucketName",
            "AccessDenied",
            "NotFound",
        ],
        _ => &[],
    }
}

/// Strict per-op error matcher.
///
/// `declared` is the operation's directly-declared error wire codes — already
/// resolved upstream in `probe_variant_with_model` so each entry is what AWS
/// actually puts on the wire (`aws.protocols#awsQueryError.code` if present,
/// otherwise the shape's short name). An observed wire code passes only if
/// it appears verbatim in that list.
///
/// We deliberately do NOT accept:
/// - A universal "framework error" allowlist (ValidationException, Throttling,
///   AccessDenied, etc. — #1344). If the op declares those, they're already
///   in `declared`; if it doesn't, AWS doesn't return them for that op and
///   we shouldn't either.
/// - A `NoSuch*` / `*NotFound` blanket accept (#1347). Those codes belong to
///   specific shapes on specific ops; the strict matcher demands the model
///   actually declare them.
/// - Suffix stripping of `Exception` / `Fault` / `Exists` from shape names
///   (#1336). That was a heuristic substitute for parsing the awsQueryError
///   trait — now that we parse the trait directly, the heuristic is gone.
fn matches_declared_error(code: &str, declared: &[String]) -> bool {
    declared.iter().any(|wire| wire == code)
}

fn classify_response(
    variant_name: &str,
    http_status: u16,
    body: &str,
    expectation: &Expectation,
    duration_ms: u64,
    op_error_shapes: Option<&[String]>,
    service_name: &str,
) -> ProbeResult {
    // Classify as NotImplemented when fakecloud signals "we did not find a
    // handler for this action" — as opposed to AWS-shaped errors that mean
    // "handler found, rejected synthetic input" (e.g. ValidationException,
    // ResourceNotFoundException for a non-existent resource id).
    //
    // The error-body patterns below cover every way fakecloud services
    // today express an unrouted action:
    //   - `not implemented` / `NotImplemented` — `ActionNotImplemented`
    //     emitted by the generic service dispatcher.
    //   - `UnknownAction` / `InvalidAction` — Query-protocol services
    //     for unknown `Action=…` form params.
    //   - `UnknownOperationException` — Lambda for unrouted URL paths.
    //   - `Unknown path:` — API Gateway v2, EventBridge Scheduler, and
    //     a few other REST-routed services return this string in the
    //     error body when `resolve_action` yields None.
    //   - `Unknown operation:` — also emitted by some Query services.
    //
    // Important: these substrings must NOT appear in legitimate AWS-shaped
    // error responses for implemented actions. `NotFoundException` alone is
    // not listed here because it's also what implemented handlers return
    // for a missing resource id.
    let is_not_implemented = body.contains("not implemented")
        || body.contains("NotImplemented")
        || body.contains("UnknownAction")
        || body.contains("InvalidAction")
        || body.contains("UnknownOperationException")
        || body.contains("Unknown path:")
        || body.contains("Unknown operation:");

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
            classify_success_expectation(http_status, body, op_error_shapes, service_name)
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

/// After a Create/Put/Update variant succeeds, fire its discovered
/// Get/Describe followup and assert each input field that was set on the
/// Create echoes through the Get response. Returns a list of violations
/// (empty if everything echoed cleanly or the followup couldn't be
/// reached for benign reasons; reports HTTP errors as a single violation
/// so harness operators can spot pairs that need bespoke handling).
/// Return the names of all `@required` members on the operation's input
/// shape. Used by the round-trip followup to discover which fields the
/// reader needs that aren't shared by name with the writer's input.
fn reader_required_inputs(model: &ServiceModel, operation_name: &str) -> Vec<String> {
    use crate::smithy::ShapeType;
    let op = match model.operations.iter().find(|o| o.name == operation_name) {
        Some(op) => op,
        None => return Vec::new(),
    };
    let input_id = match op.input_shape.as_deref() {
        Some(id) => id,
        None => return Vec::new(),
    };
    let input_shape = match model.shapes.get(input_id) {
        Some(s) => s,
        None => return Vec::new(),
    };
    match &input_shape.shape_type {
        ShapeType::Structure { members } => members
            .iter()
            .filter(|m| m.required)
            .map(|m| m.name.clone())
            .collect(),
        _ => Vec::new(),
    }
}

/// Look up a JSON field tolerating PascalCase vs camelCase first-letter
/// differences. restJson1 services with `@jsonName` (apigatewayv2) declare
/// PascalCase member names but serialise camelCase on the wire — so the
/// probe needs to find values under either casing when crossing between
/// model-derived names and observed wire keys.
pub(crate) fn lookup_field_any_case<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    member: &str,
) -> Option<&'a serde_json::Value> {
    if let Some(v) = obj.get(member) {
        return Some(v);
    }
    let mut chars = member.chars();
    let first = chars.next()?;
    let alt = if first.is_ascii_uppercase() {
        let mut s = String::with_capacity(member.len());
        s.push(first.to_ascii_lowercase());
        s.extend(chars);
        s
    } else if first.is_ascii_lowercase() {
        let mut s = String::with_capacity(member.len());
        s.push(first.to_ascii_uppercase());
        s.extend(chars);
        s
    } else {
        return None;
    };
    obj.get(&alt)
}

fn run_round_trip_followup(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    create_variant: &TestVariant,
    create_response: Option<&serde_json::Value>,
    followup: &crate::generators::RoundTripFollowup,
    model: &ServiceModel,
) -> Vec<shape_validator::ShapeViolation> {
    let mut violations = Vec::new();

    // The resource identifier we used on Create is the same value we
    // need to feed into Get/Describe. Read it straight off the variant's
    // own input — no need to parse the Create response, which avoids
    // false negatives on services whose Create output wraps the
    // identifier in a sub-structure.
    let create_obj = match create_variant.input.as_object() {
        Some(o) => o,
        None => return violations,
    };
    let id_value = match create_obj.get(&followup.id_field) {
        Some(v) if !v.is_null() => v.clone(),
        _ => return violations,
    };

    let mut get_input = serde_json::Map::new();
    get_input.insert(followup.id_field.clone(), id_value);
    // Forward every shared identifier so the follow-up Get can resolve a
    // multi-segment resource (e.g. `restApiId` + `resourceId` +
    // `httpMethod` on API Gateway v1's `GetMethod`). Skip any that the
    // writer didn't actually supply — keeps the legacy single-id flow
    // intact for services where the writer's input only has one shared
    // identifier member.
    for extra in &followup.id_fields {
        if extra == &followup.id_field {
            continue;
        }
        if let Some(v) = create_obj.get(extra) {
            if !v.is_null() {
                get_input.insert(extra.clone(), v.clone());
            }
        }
    }

    // Many APIs name the new resource's identifier differently on the
    // writer's input vs. the reader's input (e.g. `CreateModel.name` ->
    // `GetModel.modelName`, `CreateAuthorizer` returns `id` ->
    // `GetAuthorizer.authorizerId`). When the Create response carries
    // that identifier as a field, look up each of the reader's required
    // members in the response and fill from there. Conservative match:
    // exact match first, then `<reader_member>` resolves to either
    // `id` (the canonical generated identifier slot) or the reader's
    // shape suffix (`modelName` -> `name`).
    if let Some(create_response_obj) = create_response.and_then(|v| v.as_object()) {
        let reader_required = reader_required_inputs(model, &followup.get_operation);
        for member in reader_required {
            if get_input.contains_key(&member) {
                continue;
            }
            // Look up the reader-input member name in the writer's response.
            // restJson1 services with `@jsonName` (apigatewayv2 in particular)
            // declare PascalCase member names but serialise camelCase on the
            // wire. Try the raw member name first, then the lowercase-first
            // alias, so the followup id-fill works under either casing.
            if let Some(v) = lookup_field_any_case(create_response_obj, &member) {
                if !v.is_null() {
                    get_input.insert(member.clone(), v.clone());
                    continue;
                }
            }
            // `modelName` -> Create response `name`; same for other
            // <Resource>Name patterns where the response carries `name`.
            if member.ends_with("Name") {
                if let Some(v) = lookup_field_any_case(create_response_obj, "Name") {
                    if !v.is_null() {
                        get_input.insert(member.clone(), v.clone());
                        continue;
                    }
                }
            }
            // `authorizerId`, `vpcLinkId`, ... -> Create response `id`.
            if member.ends_with("Id") {
                if let Some(v) = lookup_field_any_case(create_response_obj, "Id") {
                    if !v.is_null() {
                        get_input.insert(member.clone(), v.clone());
                    }
                }
            }
        }
    }
    let get_variant = TestVariant {
        name: format!("{}__followup_get", create_variant.name),
        strategy: crate::generators::Strategy::RoundTrip,
        input: serde_json::Value::Object(get_input),
        expectation: crate::generators::Expectation::Success,
        expected_output: None,
        followup: None,
    };

    // Resolve the Get op's output shape so the recursive probe can keep
    // shape validation on. Without it the followup is still useful (we
    // still echo-check) but skipped if the op isn't in the model.
    let get_op = match model
        .operations
        .iter()
        .find(|o| o.name == followup.get_operation)
    {
        Some(op) => op,
        None => return violations,
    };
    let get_output_shape = match get_op.output_shape.as_deref() {
        Some(s) => s,
        None => return violations,
    };

    // Recurse via the public probe entry. The Get variant has no
    // `followup`, so this terminates after one extra hop.
    let get_result = probe_variant_with_model(
        client,
        endpoint,
        service_name,
        &followup.get_operation,
        &get_variant,
        Some((model, get_output_shape)),
    );

    // Only echo-check on a clean 2xx with a parseable body. A 4xx/5xx on
    // the followup is its own signal — surface as a single violation
    // rather than fabricate a misleading echo failure.
    if !(200..300).contains(&get_result.http_status) {
        violations.push(shape_validator::ShapeViolation::RoundTripFieldNotEchoed {
            field: format!("(followup {})", followup.get_operation),
            sent: serde_json::Value::String(format!("HTTP {}", get_result.http_status)),
            received: None,
        });
        return violations;
    }
    let get_body: serde_json::Value = match serde_json::from_str(&get_result.response_body) {
        Ok(v) => v,
        Err(_) => return violations,
    };

    // Pull each echo field from the Create variant's input and compare
    // against the Get output.
    for (input_field, output_field) in &followup.echo_fields {
        let sent = match create_obj.get(input_field) {
            Some(v) => v,
            None => continue,
        };
        if let Some(v) = shape_validator::echo_check(output_field, sent, &get_body) {
            violations.push(v);
        }
    }
    violations
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smithy::{Member, Operation, Shape, ShapeTraits, ShapeType};

    #[test]
    fn service_protocol_covers_every_shipped_service() {
        // Every Smithy service_name fakecloud ships must have an explicit
        // protocol mapping. Falling through to the `_ => Query` default
        // misroutes JSON/REST services as form-encoded `Action=` requests
        // that get caught by APIGW's catch-all and silently classified as
        // Pass — masking real conformance gaps. See issue surfaced when
        // ECS reported 76/76 pass while only 60 ops were actually routed.
        let cases = [
            ("sqs", Protocol::Query),
            ("sns", Protocol::Query),
            ("iam", Protocol::Query),
            ("sts", Protocol::Query),
            ("cloudformation", Protocol::Query),
            ("rds", Protocol::Query),
            ("elasticache", Protocol::Query),
            ("elasticloadbalancing", Protocol::Query),
            (
                "ssm",
                Protocol::Json {
                    target_prefix: "AmazonSSM",
                },
            ),
            (
                "events",
                Protocol::Json {
                    target_prefix: "AWSEvents",
                },
            ),
            (
                "dynamodb",
                Protocol::Json {
                    target_prefix: "DynamoDB_20120810",
                },
            ),
            (
                "secretsmanager",
                Protocol::Json {
                    target_prefix: "secretsmanager",
                },
            ),
            (
                "logs",
                Protocol::Json {
                    target_prefix: "Logs_20140328",
                },
            ),
            (
                "kms",
                Protocol::Json {
                    target_prefix: "TrentService",
                },
            ),
            (
                "cognito-idp",
                Protocol::Json {
                    target_prefix: "AWSCognitoIdentityProviderService",
                },
            ),
            (
                "cognito-identity",
                Protocol::Json {
                    target_prefix: "AWSCognitoIdentityService",
                },
            ),
            (
                "kinesis",
                Protocol::Json {
                    target_prefix: "Kinesis_20131202",
                },
            ),
            (
                "ecr",
                Protocol::Json {
                    target_prefix: "AmazonEC2ContainerRegistry_V20150921",
                },
            ),
            (
                "ecs",
                Protocol::Json {
                    target_prefix: "AmazonEC2ContainerServiceV20141113",
                },
            ),
            (
                "states",
                Protocol::Json {
                    target_prefix: "AWSStepFunctions",
                },
            ),
            ("s3", Protocol::Rest),
            ("lambda", Protocol::Rest),
            ("apigateway", Protocol::Rest),
            ("ses", Protocol::Rest),
            ("bedrock", Protocol::Rest),
            ("bedrock-runtime", Protocol::Rest),
            ("scheduler", Protocol::Rest),
        ];
        for (svc, expected) in cases {
            let got = service_protocol(svc);
            assert_eq!(got, expected, "wrong protocol for {svc}");
        }
    }

    fn op_with_http(name: &str, method: &str, uri: &str, input_shape_id: &str) -> Operation {
        Operation {
            name: name.to_string(),
            input_shape: Some(input_shape_id.to_string()),
            output_shape: None,
            error_shapes: Vec::new(),
            http_method: Some(method.to_string()),
            http_uri: Some(uri.to_string()),
            http_code: Some(200),
        }
    }

    fn member(name: &str, target: &str, traits: ShapeTraits) -> Member {
        Member {
            name: name.to_string(),
            target: target.to_string(),
            required: false,
            traits,
        }
    }

    fn structure_shape(id: &str, members: Vec<Member>) -> Shape {
        Shape {
            shape_id: id.to_string(),
            shape_type: ShapeType::Structure { members },
            traits: ShapeTraits::default(),
        }
    }

    fn string_shape(id: &str, traits: ShapeTraits) -> Shape {
        Shape {
            shape_id: id.to_string(),
            shape_type: ShapeType::String { enum_values: None },
            traits,
        }
    }

    fn model_with(op: Operation, shapes: Vec<Shape>) -> ServiceModel {
        let mut map = HashMap::new();
        for s in shapes {
            map.insert(s.shape_id.clone(), s);
        }
        ServiceModel {
            service_name: "test".to_string(),
            operations: vec![op],
            shapes: map,
        }
    }

    fn label_traits() -> ShapeTraits {
        ShapeTraits {
            http_label: true,
            ..ShapeTraits::default()
        }
    }

    fn query_traits(name: &str) -> ShapeTraits {
        ShapeTraits {
            http_query: Some(name.to_string()),
            ..ShapeTraits::default()
        }
    }

    fn header_traits(name: &str) -> ShapeTraits {
        ShapeTraits {
            http_header: Some(name.to_string()),
            ..ShapeTraits::default()
        }
    }

    fn payload_traits() -> ShapeTraits {
        ShapeTraits {
            http_payload: true,
            ..ShapeTraits::default()
        }
    }

    #[test]
    fn label_substitution_basic() {
        let op = op_with_http("GetApi", "GET", "/v2/apis/{ApiId}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("ApiId", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"ApiId": "abc123"});
        let (method, url, headers, body) =
            build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(method, reqwest::Method::GET);
        assert_eq!(url, "/v2/apis/abc123");
        assert!(headers.is_empty());
        assert!(body.is_none(), "GET has no body");
    }

    #[test]
    fn greedy_label_preserves_slashes() {
        let op = op_with_http("X", "GET", "/foo/{Path+}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Path", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Path": "a/b/c"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo/a/b/c");
    }

    #[test]
    fn non_greedy_label_encodes_slashes() {
        let op = op_with_http("X", "GET", "/foo/{Name}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Name", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Name": "a/b"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo/a%2Fb");
    }

    #[test]
    fn query_optional_omitted() {
        let op = op_with_http("X", "GET", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![member("BasePath", "#String", query_traits("basepath"))],
                ),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({}); // BasePath absent
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo");
    }

    #[test]
    fn query_present_emitted() {
        let op = op_with_http("X", "GET", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![member("BasePath", "#String", query_traits("basepath"))],
                ),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"BasePath": "hello"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo?basepath=hello");
    }

    #[test]
    fn header_extracted_out_of_body() {
        let op = op_with_http("X", "POST", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![
                        member("Idempotency", "#String", header_traits("x-idem")),
                        member("Other", "#String", ShapeTraits::default()),
                    ],
                ),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Idempotency": "abc", "Other": "keep"});
        let (_, _, headers, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(headers, vec![("x-idem".to_string(), "abc".to_string())]);
        let body: serde_json::Value = serde_json::from_str(&body.unwrap()).unwrap();
        assert_eq!(body, serde_json::json!({"Other": "keep"}));
    }

    #[test]
    fn payload_member_only_body() {
        let op = op_with_http("X", "PUT", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Body", "#String", payload_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Body": "raw-openapi-doc"});
        let (_, _, _, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(body.unwrap(), "raw-openapi-doc");
    }

    #[test]
    fn delete_without_payload_has_no_body() {
        let op = op_with_http("X", "DELETE", "/foo/{Id}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Id", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Id": "abc"});
        let (method, _, _, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(method, reqwest::Method::DELETE);
        assert!(body.is_none());
    }

    #[test]
    fn missing_label_leaves_placeholder() {
        // When a required label is absent, keep the literal {Name} so the server
        // returns a routing failure (not a silent 500). Ensures the variant
        // surfaces as a SHAPE_MISMATCH/UNEXPECTED rather than being hidden.
        let op = op_with_http("X", "GET", "/foo/{ApiId}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("ApiId", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({}); // ApiId missing
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo/{ApiId}");
    }

    #[test]
    fn literal_query_in_template_merged_with_computed() {
        let op = op_with_http("X", "GET", "/x?action=foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("P", "#String", query_traits("p"))]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"P": "v"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/x?action=foo&p=v");
    }

    #[test]
    fn list_valued_query_repeats() {
        let op = op_with_http("X", "GET", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![member("Tags", "#StringList", query_traits("tag"))],
                ),
                Shape {
                    shape_id: "#StringList".to_string(),
                    shape_type: ShapeType::List {
                        member_target: "#String".to_string(),
                    },
                    traits: ShapeTraits::default(),
                },
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Tags": ["a", "b"]});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo?tag=a&tag=b");
    }

    #[test]
    fn delete_with_payload_keeps_body() {
        // `@httpPayload` overrides the default "DELETE = bodyless" rule. Rare
        // in practice but AWS models do use it for some delete-with-filter-doc
        // shapes; ensure the builder emits a body in that case.
        let op = op_with_http("X", "DELETE", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Body", "#String", payload_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Body": "delete-me"});
        let (method, _, _, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(method, reqwest::Method::DELETE);
        assert_eq!(body.unwrap(), "delete-me");
    }

    #[test]
    fn patch_method_keeps_body() {
        // APIGWv2 Update* ops use PATCH. Regression guard for the
        // `has_body_method` check: don't treat PATCH as bodyless.
        let op = op_with_http("X", "PATCH", "/foo/{Id}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![
                        member("Id", "#String", label_traits()),
                        member("Description", "#String", ShapeTraits::default()),
                    ],
                ),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Id": "abc", "Description": "updated"});
        let (method, _, _, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(method, reqwest::Method::PATCH);
        let body: serde_json::Value = serde_json::from_str(&body.unwrap()).unwrap();
        assert_eq!(body, serde_json::json!({"Description": "updated"}));
    }

    #[test]
    fn percent_encoding_in_label() {
        let op = op_with_http("X", "GET", "/foo/{Id}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Id", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Id": "a b#c"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo/a%20b%23c");
    }

    #[test]
    fn classify_unknown_path_is_not_implemented() {
        // API Gateway v2 emits `Unknown path: ...` when resolve_action
        // can't match a URL. Must classify as NotImplemented, not Pass.
        let body = r#"{"__type":"NotFoundException","message":"Unknown path: /v2/domainnames"}"#;
        let result = classify_response("v1", 404, body, &Expectation::Success, 0, None, "");
        assert_eq!(result.status, ProbeStatus::NotImplemented);
    }

    #[test]
    fn classify_unknown_operation_is_not_implemented() {
        // Lambda emits `UnknownOperationException` for URLs its
        // resolve_action doesn't recognize.
        let body = r#"{"__type":"UnknownOperationException","message":"Unknown operation: /foo"}"#;
        let result = classify_response("v1", 404, body, &Expectation::Success, 0, None, "");
        assert_eq!(result.status, ProbeStatus::NotImplemented);
    }

    #[test]
    fn classify_action_not_implemented_string() {
        // `ActionNotImplemented` error maps to the substring "not implemented"
        // in the response body.
        let body =
            r#"{"__type":"InvalidAction","message":"action Foo not implemented for service bar"}"#;
        let result = classify_response("v1", 501, body, &Expectation::Success, 0, None, "");
        assert_eq!(result.status, ProbeStatus::NotImplemented);
    }

    #[test]
    fn classify_legit_resource_not_found_is_pass() {
        // AWS-shaped `ResourceNotFoundException` for a synthetic id is a
        // legitimate response from an implemented handler; must not be
        // confused with NotImplemented. Strict mode: `declared` carries
        // wire codes (not shape IDs), pre-resolved by the caller.
        let body =
            r#"{"__type":"ResourceNotFoundException","message":"Function not found: test-fn"}"#;
        let declared = vec!["ResourceNotFoundException".to_string()];
        let result = classify_response(
            "v1",
            404,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    // -- error-shape-driven 4xx classification --

    #[test]
    fn classify_404_with_no_aws_error_shape_fails() {
        // Mirrors #817: routing miss returns 404 with a body that has no
        // AWS error code. Must NOT pass — that's the gaming we're closing.
        let body = r#"{"message":"Function not found"}"#;
        let result = classify_response("v1", 404, body, &Expectation::Success, 0, None, "");
        assert!(matches!(result.status, ProbeStatus::UnexpectedResult(_)));
    }

    #[test]
    fn classify_404_with_undeclared_error_fails() {
        // Handler-emitted error that doesn't appear in the op's Smithy
        // error_shapes list — could be a stray fakecloud error type that
        // AWS would never return. Flag it.
        let body = r#"{"__type":"WeirdInternalException","message":"oops"}"#;
        let declared = vec![
            "ResourceNotFoundException".to_string(),
            "ValidationException".to_string(),
        ];
        let result = classify_response(
            "v1",
            404,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert!(
            matches!(result.status, ProbeStatus::UnexpectedResult(_)),
            "got {:?}",
            result.status
        );
    }

    #[test]
    fn classify_400_with_xml_error_code_passes() {
        // restXml + awsQuery both encode the error code in <Code>X</Code>.
        let body =
            r#"<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>x</Message></Error>"#;
        let declared = vec!["NoSuchBucket".to_string()];
        let result = classify_response(
            "v1",
            404,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    #[test]
    fn classify_400_query_protocol_error_passes() {
        // awsQuery (IAM, RDS, …) wraps the error in <ErrorResponse><Error>...
        let body = r#"<ErrorResponse><Error><Code>InvalidParameterValue</Code><Message>x</Message></Error></ErrorResponse>"#;
        let declared = vec!["InvalidParameterValue".to_string()];
        let result = classify_response(
            "v1",
            400,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    #[test]
    fn classify_4xx_no_op_model_lenient() {
        // Op model unavailable (caller didn't pass declared errors): any
        // AWS-shaped error counts as a real handler response.
        let body = r#"{"__type":"SomeException"}"#;
        let result = classify_response("v1", 400, body, &Expectation::Success, 0, None, "");
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    #[test]
    fn classify_4xx_empty_error_shapes_lenient() {
        // Op declares no errors (rare). Treat any AWS-shaped error as Pass.
        let body = r#"{"__type":"SomeException"}"#;
        let declared: Vec<String> = Vec::new();
        let result = classify_response(
            "v1",
            400,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    #[test]
    fn extract_error_code_from_namespaced_type() {
        let body = r#"{"__type":"com.amazonaws.lambda#ResourceNotFoundException"}"#;
        assert_eq!(
            extract_aws_error_code(body),
            Some("ResourceNotFoundException".to_string())
        );
    }

    #[test]
    fn extract_error_code_from_xml() {
        let body = r#"<Error><Code>NoSuchBucket</Code></Error>"#;
        assert_eq!(
            extract_aws_error_code(body),
            Some("NoSuchBucket".to_string())
        );
    }

    #[test]
    fn extract_error_code_returns_none_for_plain_message() {
        // Routing-miss body shape — no recognisable AWS error code.
        let body = r#"{"message":"Unknown URL"}"#;
        assert_eq!(extract_aws_error_code(body), None);
    }

    #[test]
    fn aws_query_error_trait_gated_by_protocol() {
        // KMS is awsJson1.1; its `NotFoundException` shape carries a
        // legacy `awsQueryError.code = "NotFound"` trait that is inert
        // for JSON wire encoding. The probe must NOT honor the trait
        // for non-query services — the wire `__type` is the shape's
        // short name. SQS (awsJson1.0 + awsQueryCompatible) is the
        // only awsJson service where the trait does apply.
        assert!(matches!(service_protocol("kms"), Protocol::Json { .. }));
        assert!(!is_aws_query_compatible_service("kms"));
        assert!(is_aws_query_compatible_service("sqs"));
        assert!(matches!(service_protocol("iam"), Protocol::Query));
    }
}
