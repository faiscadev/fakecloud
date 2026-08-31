//! probe `rest_request` (audit-2026-05-19).

use super::*;

/// Returns (HTTP method, path, optional query string) for a known REST API operation.
///
/// For S3, uses "test-conformance-bucket" as the bucket and "test-key" as the object key.
/// For Lambda, uses "test-conformance-function" as the function name.
pub(super) fn rest_request_config(
    service_name: &str,
    operation_name: &str,
) -> (reqwest::Method, String, Option<String>) {
    const BUCKET: &str = "test-conformance-bucket";
    const KEY: &str = "test-key";
    const FUNC: &str = "test-conformance-function";
    const LAMBDA_PREFIX: &str = "/2015-03-31";
    const RBP_ARN: &str = "arn:aws:lambda:us-east-1:000000000000:function:test-rbp-function";

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
            // Lambda Workflows: capacity providers (2025-11-30 API).
            "CreateCapacityProvider" => (
                reqwest::Method::POST,
                "/2025-11-30/capacity-providers".to_string(),
                None,
            ),
            "ListCapacityProviders" => (
                reqwest::Method::GET,
                "/2025-11-30/capacity-providers".to_string(),
                None,
            ),
            "GetCapacityProvider" => (
                reqwest::Method::GET,
                "/2025-11-30/capacity-providers/test-capacity-provider".to_string(),
                None,
            ),
            "UpdateCapacityProvider" => (
                reqwest::Method::PUT,
                "/2025-11-30/capacity-providers/test-capacity-provider".to_string(),
                None,
            ),
            "DeleteCapacityProvider" => (
                reqwest::Method::DELETE,
                "/2025-11-30/capacity-providers/test-capacity-provider".to_string(),
                None,
            ),
            "ListFunctionVersionsByCapacityProvider" => (
                reqwest::Method::GET,
                "/2025-11-30/capacity-providers/test-capacity-provider/function-versions"
                    .to_string(),
                None,
            ),
            // Lambda Workflows: durable executions (2025-12-01 API).
            "GetDurableExecution" => (
                reqwest::Method::GET,
                "/2025-12-01/durable-executions/test-durable-execution".to_string(),
                None,
            ),
            "GetDurableExecutionHistory" => (
                reqwest::Method::GET,
                "/2025-12-01/durable-executions/test-durable-execution/history".to_string(),
                None,
            ),
            "GetDurableExecutionState" => (
                reqwest::Method::GET,
                "/2025-12-01/durable-executions/test-durable-execution/state".to_string(),
                None,
            ),
            "CheckpointDurableExecution" => (
                reqwest::Method::POST,
                "/2025-12-01/durable-executions/test-durable-execution/checkpoint".to_string(),
                None,
            ),
            "StopDurableExecution" => (
                reqwest::Method::POST,
                "/2025-12-01/durable-executions/test-durable-execution/stop".to_string(),
                None,
            ),
            "ListDurableExecutionsByFunction" => (
                reqwest::Method::GET,
                format!("/2025-12-01/functions/{}/durable-executions", FUNC),
                None,
            ),
            "SendDurableExecutionCallbackSuccess" => (
                reqwest::Method::POST,
                "/2025-12-01/durable-execution-callbacks/test-callback/succeed".to_string(),
                None,
            ),
            "SendDurableExecutionCallbackFailure" => (
                reqwest::Method::POST,
                "/2025-12-01/durable-execution-callbacks/test-callback/fail".to_string(),
                None,
            ),
            "SendDurableExecutionCallbackHeartbeat" => (
                reqwest::Method::POST,
                "/2025-12-01/durable-execution-callbacks/test-callback/heartbeat".to_string(),
                None,
            ),
            // Resource-policy document API (2026-07-09). `ResourceArn` is a
            // full function ARN in the path, so keep the placeholder distinct
            // from `FUNC` — the `FunctionName` substitution below would
            // otherwise rewrite the middle of the ARN.
            "PutResourcePolicy" => (
                reqwest::Method::PUT,
                format!("/2026-07-09/resource-policy/{}", RBP_ARN),
                None,
            ),
            "GetResourcePolicy" => (
                reqwest::Method::GET,
                format!("/2026-07-09/resource-policy/{}", RBP_ARN),
                None,
            ),
            "DeleteResourcePolicy" => (
                reqwest::Method::DELETE,
                format!("/2026-07-09/resource-policy/{}", RBP_ARN),
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
            "ListObjects" => (reqwest::Method::GET, format!("/{}", BUCKET), None),
            "ListObjectsV2" => (
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
            // Object annotations. `AnnotationName` is an `@httpQuery` member,
            // so `append_http_query_from_model` supplies it from the variant —
            // which is also what separates GetObjectAnnotation (name present)
            // from ListObjectAnnotations (name absent) on the shared URI.
            "PutObjectAnnotation" => (
                reqwest::Method::PUT,
                format!("/{}/{}", BUCKET, KEY),
                Some("annotation".to_string()),
            ),
            "GetObjectAnnotation" => (
                reqwest::Method::GET,
                format!("/{}/{}", BUCKET, KEY),
                Some("annotation".to_string()),
            ),
            "DeleteObjectAnnotation" => (
                reqwest::Method::DELETE,
                format!("/{}/{}", BUCKET, KEY),
                Some("annotation".to_string()),
            ),
            "ListObjectAnnotations" => (
                reqwest::Method::GET,
                format!("/{}/{}", BUCKET, KEY),
                Some("annotation".to_string()),
            ),
            "UpdateBucketMetadataAnnotationTableConfiguration" => (
                reqwest::Method::PUT,
                format!("/{}", BUCKET),
                Some("metadataAnnotationTable".to_string()),
            ),
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

/// Append the `@httpQuery`-bound members of an operation's input shape
/// to a legacy-routed URL. Used for Lambda / S3 where the path is
/// hand-curated but query parameters still need to be honored so
/// negative/boundary variants can target them.
pub(super) fn append_http_query_from_model(
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
pub(super) fn legacy_rest_request(
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
pub(super) fn legacy_substitute_identifiers(
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
            // `ResourceArn` first: it spans a whole ARN, and the
            // `FunctionName` entry below would otherwise rewrite part of it.
            (
                "arn:aws:lambda:us-east-1:000000000000:function:test-rbp-function",
                "ResourceArn",
            ),
            ("test-conformance-function", "FunctionName"),
            // Layer ops route on `LayerName` (and an optional
            // numeric `VersionNumber`). Substitute from the variant
            // so negative/boundary variants reach those routes too.
            ("test-layer", "LayerName"),
            ("__VERSION__", "VersionNumber"),
            // Alias ops use `LATEST` as the path placeholder. Drive
            // negative variants through the same slot.
            ("LATEST", "Name"),
            // Lambda Workflows path labels (2025-11-30 + 2025-12-01).
            ("test-capacity-provider", "CapacityProviderName"),
            ("test-durable-execution", "DurableExecutionArn"),
            ("test-callback", "CallbackId"),
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
                // server reliably returns InvalidBucketName.
                //
                // Lambda needs the same treatment: omitting `FunctionName`
                // from `GET /functions/{FunctionName}` collapses to
                // `GET /functions/`, which real AWS — and now fakecloud
                // (issue #1645) — routes to `ListFunctions` (200), not a
                // client error. A non-empty sentinel keeps the probe on
                // the `GetFunction` route, where a nonexistent name yields
                // the expected 4xx. Other services keep the empty-string
                // collapse — their dispatchers reject empty path labels
                // cleanly.
                let sentinel = match service_name {
                    "s3" | "lambda" => "INVALID_OMITTED_LABEL",
                    _ => "",
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
pub(super) fn build_http_request_from_model(
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

pub(super) fn substitute_uri_labels(
    template: &str,
    labels: &HashMap<String, serde_json::Value>,
) -> String {
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
pub(super) fn percent_encode_label(s: &str) -> String {
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
pub(super) fn percent_encode_greedy(s: &str) -> String {
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

pub(super) fn append_query(out: &mut Vec<String>, key: &str, v: &serde_json::Value) {
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

pub(super) fn value_to_header_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// For `@httpPayload` members: structures/lists/objects become JSON; strings
/// become the raw string; blobs (JSON-encoded as strings here) likewise.
pub(super) fn value_to_body(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        _ => serde_json::to_string(v).unwrap_or_default(),
    }
}

pub(super) fn flatten_to_form_params(
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

pub(super) fn urlencoded(s: &str) -> String {
    s.replace('%', "%25")
        .replace('&', "%26")
        .replace('=', "%3D")
        .replace('+', "%2B")
        .replace(' ', "%20")
}
