use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::runtime::ContainerRuntime;
use crate::state::{
    EventSourceMapping, LambdaFunction, LambdaSnapshot, LambdaState, SharedLambdaState,
    LAMBDA_SNAPSHOT_SCHEMA_VERSION,
};

fn invalid_param(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        msg,
    )
}

fn check_len(field: &str, v: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    if v.len() < min || v.len() > max {
        return Err(invalid_param(format!(
            "{field} length must be in [{min},{max}], got {}",
            v.len()
        )));
    }
    Ok(())
}

fn check_optional_len(
    field: &str,
    v: Option<&str>,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    if let Some(s) = v {
        check_len(field, s, min, max)?;
    }
    Ok(())
}

fn check_optional_int_range(
    field: &str,
    v: Option<i64>,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    if let Some(n) = v {
        if n < min || n > max {
            return Err(invalid_param(format!(
                "{field} must be in [{min},{max}], got {n}"
            )));
        }
    }
    Ok(())
}

const LAMBDA_PUBLISH_TO_VALUES: &[&str] = &["LATEST_PUBLISHED"];

// Trimmed to runtimes the SDK still mints; the full Smithy enum has 46
// entries but only these are emitted by `aws-sdk-lambda` since the
// older ones are deprecation-only and never surfaced via CreateFunction
// in practice. Conformance probes use the model enum exhaustively, so
// keep this list in sync with the Smithy model.
const LAMBDA_RUNTIMES: &[&str] = &[
    "nodejs",
    "nodejs4.3",
    "nodejs4.3-edge",
    "nodejs6.10",
    "nodejs8.10",
    "nodejs10.x",
    "nodejs12.x",
    "nodejs14.x",
    "nodejs16.x",
    "nodejs18.x",
    "nodejs20.x",
    "nodejs22.x",
    "nodejs24.x",
    "java8",
    "java8.al2",
    "java11",
    "java17",
    "java21",
    "java25",
    "python2.7",
    "python3.6",
    "python3.7",
    "python3.8",
    "python3.9",
    "python3.10",
    "python3.11",
    "python3.12",
    "python3.13",
    "python3.14",
    "dotnetcore1.0",
    "dotnetcore2.0",
    "dotnetcore2.1",
    "dotnetcore3.1",
    "dotnet6",
    "dotnet8",
    "dotnet10",
    "go1.x",
    "ruby2.5",
    "ruby2.7",
    "ruby3.2",
    "ruby3.3",
    "ruby3.4",
    "provided",
    "provided.al2",
    "provided.al2023",
];

fn check_optional_enum(
    field: &str,
    v: Option<&str>,
    allowed: &[&str],
) -> Result<(), AwsServiceError> {
    if let Some(s) = v {
        if !allowed.contains(&s) {
            return Err(invalid_param(format!(
                "{field} must be one of the enum values, got '{s}'"
            )));
        }
    }
    Ok(())
}

fn prevalidate_lambda(action: &str, req: &AwsRequest) -> Result<(), AwsServiceError> {
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
    match action {
        "CreateFunction" => {
            check_optional_len("Description", body["Description"].as_str(), 0, 256)?;
            check_optional_len("Handler", body["Handler"].as_str(), 0, 128)?;
            check_optional_int_range("MemorySize", body["MemorySize"].as_i64(), 128, 32768)?;
            check_optional_int_range("Timeout", body["Timeout"].as_i64(), 1, 5400)?;
            check_optional_enum("Runtime", body["Runtime"].as_str(), LAMBDA_RUNTIMES)?;
        }
        "PublishVersion" => {
            check_optional_len("Description", body["Description"].as_str(), 0, 256)?;
            check_optional_enum(
                "PublishTo",
                body["PublishTo"].as_str(),
                LAMBDA_PUBLISH_TO_VALUES,
            )?;
        }
        "UpdateFunctionCode" => {
            check_optional_enum(
                "PublishTo",
                body["PublishTo"].as_str(),
                LAMBDA_PUBLISH_TO_VALUES,
            )?;
            check_optional_len("S3Bucket", body["S3Bucket"].as_str(), 3, 63)?;
            check_optional_len("S3Key", body["S3Key"].as_str(), 1, 1024)?;
            check_optional_len("S3ObjectVersion", body["S3ObjectVersion"].as_str(), 1, 1024)?;
        }
        "UpdateFunctionConfiguration" => {
            check_optional_len("Description", body["Description"].as_str(), 0, 256)?;
            check_optional_len("Handler", body["Handler"].as_str(), 0, 128)?;
            check_optional_int_range("MemorySize", body["MemorySize"].as_i64(), 128, 32768)?;
            check_optional_int_range("Timeout", body["Timeout"].as_i64(), 1, 5400)?;
            check_optional_enum("Runtime", body["Runtime"].as_str(), LAMBDA_RUNTIMES)?;
        }
        _ => {}
    }
    Ok(())
}

/// Lambda actions whose URL `resource_name` slot is a `FunctionName`
/// (and therefore accepts ARN / partial ARN / `name:qualifier` forms).
/// Layer / event-source-mapping / code-signing-config actions key off
/// other resource identifiers and are excluded.
pub(crate) fn action_takes_function_name(action: &str) -> bool {
    matches!(
        action,
        "GetFunction"
            | "DeleteFunction"
            | "Invoke"
            | "InvokeAsync"
            | "InvokeWithResponseStream"
            | "PublishVersion"
            | "ListVersionsByFunction"
            | "AddPermission"
            | "RemovePermission"
            | "GetPolicy"
            | "GetFunctionConfiguration"
            | "UpdateFunctionConfiguration"
            | "UpdateFunctionCode"
            | "GetFunctionConcurrency"
            | "PutFunctionConcurrency"
            | "DeleteFunctionConcurrency"
            | "PutProvisionedConcurrencyConfig"
            | "GetProvisionedConcurrencyConfig"
            | "DeleteProvisionedConcurrencyConfig"
            | "ListProvisionedConcurrencyConfigs"
            | "PutFunctionEventInvokeConfig"
            | "UpdateFunctionEventInvokeConfig"
            | "GetFunctionEventInvokeConfig"
            | "DeleteFunctionEventInvokeConfig"
            | "ListFunctionEventInvokeConfigs"
            | "CreateFunctionUrlConfig"
            | "UpdateFunctionUrlConfig"
            | "GetFunctionUrlConfig"
            | "DeleteFunctionUrlConfig"
            | "ListFunctionUrlConfigs"
            | "PutFunctionCodeSigningConfig"
            | "GetFunctionCodeSigningConfig"
            | "DeleteFunctionCodeSigningConfig"
            | "GetFunctionScalingConfig"
            | "PutFunctionScalingConfig"
            | "PutFunctionRecursionConfig"
            | "GetFunctionRecursionConfig"
            | "CreateAlias"
            | "GetAlias"
            | "ListAliases"
            | "UpdateAlias"
            | "DeleteAlias"
            | "PutRuntimeManagementConfig"
            | "GetRuntimeManagementConfig"
    )
}

/// Strip an ARN, partial ARN, or trailing `:qualifier` from a Lambda
/// `FunctionName` input down to the bare function name used as the
/// state map key. AWS Lambda accepts four forms in URL path slots and
/// API params:
///
///   - `MyFunction`
///   - `MyFunction:Qualifier`
///   - `123456789012:function:MyFunction[:Qualifier]`           (partial ARN)
///   - `arn:aws:lambda:REGION:ACCOUNT:function:MyFunction[:Qualifier]`
///
/// Inputs that don't match any of those structures are returned
/// unchanged. The qualifier (version or alias) is dropped because most
/// callers look up the function by name and resolve qualifier
/// separately.
/// Map a dispatched Lambda operation name (as returned by
/// [`LambdaService::resolve_action`]) to its AWS IAM action string.
///
/// Lambda is `iam_enforceable`, so every op the dispatcher can route MUST
/// resolve here — an op that returned `None` would run with no policy
/// evaluation at all (a silent auth bypass). The mapping is driven from
/// the same op-name strings the dispatcher uses so the two can never
/// drift; `iam_action_name_for_exhaustiveness` (see tests) asserts every
/// dispatchable op resolves to `Some`.
///
/// Almost all IAM actions equal the operation name. The exceptions come
/// straight from the Smithy model's `aws.iam#iamAction.name` overrides:
///   - `Invoke`                     -> `lambda:InvokeFunction`
///   - `InvokeWithResponseStream`   -> `lambda:InvokeFunction`
///   - `GetLayerVersionByArn`       -> `lambda:GetLayerVersion`
pub(crate) fn iam_action_name_for(op: &str) -> Option<&'static str> {
    let action = match op {
        // --- Smithy IAM-action name overrides ---
        "Invoke" => "InvokeFunction",
        "InvokeWithResponseStream" => "InvokeFunction",
        "GetLayerVersionByArn" => "GetLayerVersion",

        // --- functions ---
        "CreateFunction" => "CreateFunction",
        "ListFunctions" => "ListFunctions",
        "GetFunction" => "GetFunction",
        "DeleteFunction" => "DeleteFunction",
        "InvokeAsync" => "InvokeAsync",
        "UpdateFunctionCode" => "UpdateFunctionCode",
        "UpdateFunctionConfiguration" => "UpdateFunctionConfiguration",
        "GetFunctionConfiguration" => "GetFunctionConfiguration",
        "PublishVersion" => "PublishVersion",
        "ListVersionsByFunction" => "ListVersionsByFunction",
        "GetAccountSettings" => "GetAccountSettings",

        // --- resource policy / permissions ---
        "AddPermission" => "AddPermission",
        "RemovePermission" => "RemovePermission",
        "GetPolicy" => "GetPolicy",

        // --- aliases ---
        "CreateAlias" => "CreateAlias",
        "GetAlias" => "GetAlias",
        "UpdateAlias" => "UpdateAlias",
        "DeleteAlias" => "DeleteAlias",
        "ListAliases" => "ListAliases",

        // --- concurrency ---
        "PutFunctionConcurrency" => "PutFunctionConcurrency",
        "GetFunctionConcurrency" => "GetFunctionConcurrency",
        "DeleteFunctionConcurrency" => "DeleteFunctionConcurrency",
        "PutProvisionedConcurrencyConfig" => "PutProvisionedConcurrencyConfig",
        "GetProvisionedConcurrencyConfig" => "GetProvisionedConcurrencyConfig",
        "DeleteProvisionedConcurrencyConfig" => "DeleteProvisionedConcurrencyConfig",
        "ListProvisionedConcurrencyConfigs" => "ListProvisionedConcurrencyConfigs",

        // --- event invoke config ---
        "PutFunctionEventInvokeConfig" => "PutFunctionEventInvokeConfig",
        "GetFunctionEventInvokeConfig" => "GetFunctionEventInvokeConfig",
        "UpdateFunctionEventInvokeConfig" => "UpdateFunctionEventInvokeConfig",
        "DeleteFunctionEventInvokeConfig" => "DeleteFunctionEventInvokeConfig",
        "ListFunctionEventInvokeConfigs" => "ListFunctionEventInvokeConfigs",

        // --- runtime / scaling / recursion config ---
        "PutRuntimeManagementConfig" => "PutRuntimeManagementConfig",
        "GetRuntimeManagementConfig" => "GetRuntimeManagementConfig",
        "PutFunctionScalingConfig" => "PutFunctionScalingConfig",
        "GetFunctionScalingConfig" => "GetFunctionScalingConfig",
        "PutFunctionRecursionConfig" => "PutFunctionRecursionConfig",
        "GetFunctionRecursionConfig" => "GetFunctionRecursionConfig",

        // --- function URL config ---
        "CreateFunctionUrlConfig" => "CreateFunctionUrlConfig",
        "GetFunctionUrlConfig" => "GetFunctionUrlConfig",
        "UpdateFunctionUrlConfig" => "UpdateFunctionUrlConfig",
        "DeleteFunctionUrlConfig" => "DeleteFunctionUrlConfig",
        "ListFunctionUrlConfigs" => "ListFunctionUrlConfigs",

        // --- event source mappings ---
        "CreateEventSourceMapping" => "CreateEventSourceMapping",
        "ListEventSourceMappings" => "ListEventSourceMappings",
        "GetEventSourceMapping" => "GetEventSourceMapping",
        "UpdateEventSourceMapping" => "UpdateEventSourceMapping",
        "DeleteEventSourceMapping" => "DeleteEventSourceMapping",

        // --- layers ---
        "PublishLayerVersion" => "PublishLayerVersion",
        "ListLayers" => "ListLayers",
        "ListLayerVersions" => "ListLayerVersions",
        "GetLayerVersion" => "GetLayerVersion",
        "DeleteLayerVersion" => "DeleteLayerVersion",
        "GetLayerVersionPolicy" => "GetLayerVersionPolicy",
        "AddLayerVersionPermission" => "AddLayerVersionPermission",
        "RemoveLayerVersionPermission" => "RemoveLayerVersionPermission",

        // --- code signing config ---
        "CreateCodeSigningConfig" => "CreateCodeSigningConfig",
        "GetCodeSigningConfig" => "GetCodeSigningConfig",
        "UpdateCodeSigningConfig" => "UpdateCodeSigningConfig",
        "DeleteCodeSigningConfig" => "DeleteCodeSigningConfig",
        "ListCodeSigningConfigs" => "ListCodeSigningConfigs",
        "PutFunctionCodeSigningConfig" => "PutFunctionCodeSigningConfig",
        "GetFunctionCodeSigningConfig" => "GetFunctionCodeSigningConfig",
        "DeleteFunctionCodeSigningConfig" => "DeleteFunctionCodeSigningConfig",
        "ListFunctionsByCodeSigningConfig" => "ListFunctionsByCodeSigningConfig",

        // --- tags ---
        "TagResource" => "TagResource",
        "UntagResource" => "UntagResource",
        "ListTags" => "ListTags",

        // --- capacity providers (Lambda Workflows) ---
        "CreateCapacityProvider" => "CreateCapacityProvider",
        "GetCapacityProvider" => "GetCapacityProvider",
        "ListCapacityProviders" => "ListCapacityProviders",
        "UpdateCapacityProvider" => "UpdateCapacityProvider",
        "DeleteCapacityProvider" => "DeleteCapacityProvider",
        "ListFunctionVersionsByCapacityProvider" => "ListFunctionVersionsByCapacityProvider",

        // --- durable executions ---
        // The Smithy model carries no `aws.iam#iamAction` annotation for
        // these preview ops, so the IAM action equals the op name (AWS's
        // default convention when no override is present).
        "GetDurableExecution" => "GetDurableExecution",
        "GetDurableExecutionHistory" => "GetDurableExecutionHistory",
        "GetDurableExecutionState" => "GetDurableExecutionState",
        "ListDurableExecutionsByFunction" => "ListDurableExecutionsByFunction",
        "CheckpointDurableExecution" => "CheckpointDurableExecution",
        "StopDurableExecution" => "StopDurableExecution",
        "SendDurableExecutionCallbackSuccess" => "SendDurableExecutionCallbackSuccess",
        "SendDurableExecutionCallbackFailure" => "SendDurableExecutionCallbackFailure",
        "SendDurableExecutionCallbackHeartbeat" => "SendDurableExecutionCallbackHeartbeat",

        _ => return None,
    };
    Some(action)
}

pub(crate) fn normalize_function_name(input: &str) -> String {
    if input.is_empty() {
        return String::new();
    }

    // SDKs URL-encode `:` in path segments, so `arn:aws:lambda:...`
    // arrives as `arn%3Aaws%3Alambda%3A...`. Decode first; legitimate
    // function names contain no percent-encoded characters, so this is
    // safe for the bare-name path too.
    let decoded = percent_encoding::percent_decode_str(input)
        .decode_utf8_lossy()
        .into_owned();
    let input = decoded.as_str();

    // Full ARN: arn:aws:lambda:REGION:ACCOUNT:function:NAME[:QUALIFIER]
    if let Some(rest) = input.strip_prefix("arn:aws:lambda:") {
        let parts: Vec<&str> = rest.splitn(5, ':').collect();
        // parts: [region, account, "function", name, qualifier?]
        if parts.len() >= 4 && parts[2] == "function" && !parts[3].is_empty() {
            return parts[3].to_string();
        }
        return input.to_string();
    }

    // Partial ARN: ACCOUNT:function:NAME[:QUALIFIER]
    let parts: Vec<&str> = input.splitn(4, ':').collect();
    if parts.len() >= 3 && parts[1] == "function" && parts[0].chars().all(|c| c.is_ascii_digit()) {
        if !parts[2].is_empty() {
            return parts[2].to_string();
        }
        return input.to_string();
    }

    // Bare name with qualifier: NAME:QUALIFIER. Only apply when the
    // input contains exactly one colon and the name part is a valid
    // Lambda function-name token, so malformed ARNs (e.g. wrong service
    // or wrong format) fall through unchanged rather than getting their
    // first colon-segment returned.
    if input.matches(':').count() == 1 {
        if let Some((name, _qualifier)) = input.split_once(':') {
            if !name.is_empty() && name.chars().all(is_function_name_char) {
                return name.to_string();
            }
        }
    }

    input.to_string()
}

fn is_function_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

/// Lambda's `Marker`/`MaxItems` list pagination. The items are sorted by their
/// opaque per-item key (`marker_of`) for a stable order, then sliced: a
/// non-empty `Marker` resumes after the item whose key equals it, and
/// `MaxItems` bounds the page. Returns `(page, next_marker)` where the marker
/// is the last returned item's key when more remain, else `""` (AWS emits an
/// empty-string `NextMarker` on the final page rather than omitting it).
///
/// An unrecognised marker (its item was deleted between calls) yields an empty
/// final page, matching AWS's "resume past the end" behaviour.
pub(crate) fn paginate_marker<T, F>(
    mut items: Vec<T>,
    marker: Option<&str>,
    max_items: Option<usize>,
    marker_of: F,
) -> (Vec<T>, String)
where
    F: Fn(&T) -> String,
{
    items.sort_by_key(&marker_of);

    let start = match marker {
        Some(m) if !m.is_empty() => items
            .iter()
            .position(|it| marker_of(it) == m)
            .map(|p| p + 1)
            .unwrap_or(items.len()),
        _ => 0,
    };
    if start >= items.len() {
        return (Vec::new(), String::new());
    }

    let limit = max_items.filter(|&n| n > 0).unwrap_or(usize::MAX);
    let end = start.saturating_add(limit).min(items.len());
    let next_marker = if end < items.len() {
        marker_of(&items[end - 1])
    } else {
        String::new()
    };
    let page: Vec<T> = items.drain(start..end).collect();
    (page, next_marker)
}

/// Parse `MaxItems` from the query (already range-validated upstream) into a
/// page size, returning `None` when absent.
pub(crate) fn marker_page_size(req: &AwsRequest) -> Option<usize> {
    req.query_params
        .get("MaxItems")
        .and_then(|s| s.parse::<usize>().ok())
}

/// Extract a version/alias qualifier embedded in a function reference, the
/// mirror of [`normalize_function_name`] (which strips it). AWS accepts the
/// qualifier inline -- `...:function:MyFn:PROD`, `123:function:MyFn:PROD`,
/// `MyFn:PROD` -- and Invoke must honor it when no `?Qualifier=` is supplied;
/// previously it was dropped and the invoke silently ran `$LATEST`
/// (bug-audit 2026-06-20, 1.3). Returns `None` when no qualifier is present.
pub(crate) fn qualifier_from_function_ref(input: &str) -> Option<String> {
    if input.is_empty() {
        return None;
    }
    let decoded = percent_encoding::percent_decode_str(input)
        .decode_utf8_lossy()
        .into_owned();
    let input = decoded.as_str();

    if let Some(rest) = input.strip_prefix("arn:aws:lambda:") {
        // [region, account, "function", name, qualifier?]
        let parts: Vec<&str> = rest.splitn(5, ':').collect();
        if parts.len() == 5 && parts[2] == "function" && !parts[4].is_empty() {
            return Some(parts[4].to_string());
        }
        return None;
    }
    // Partial ARN: ACCOUNT:function:NAME[:QUALIFIER]
    let parts: Vec<&str> = input.splitn(4, ':').collect();
    if parts.len() == 4
        && parts[1] == "function"
        && parts[0].chars().all(|c| c.is_ascii_digit())
        && !parts[3].is_empty()
    {
        return Some(parts[3].to_string());
    }
    // Bare name with qualifier: NAME:QUALIFIER (exactly one colon, valid name).
    if input.matches(':').count() == 1 {
        if let Some((name, qualifier)) = input.split_once(':') {
            if !name.is_empty() && name.chars().all(is_function_name_char) && !qualifier.is_empty()
            {
                return Some(qualifier.to_string());
            }
        }
    }
    None
}

/// AWS bounds `EphemeralStorage.Size` to `[512, 10240]` MiB. Anything
/// outside that range is rejected at the API edge with
/// `InvalidParameterValueException`, matching the real Lambda control
/// plane. Returns the validated size unchanged on success.
pub(crate) fn validate_ephemeral_storage(size: i64) -> Result<i64, AwsServiceError> {
    if !(512..=10240).contains(&size) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValueException",
            format!(
                "Value {size} at 'ephemeralStorage.size' failed to satisfy constraint: \
                 Member must satisfy constraint: [Member must have value less than or equal to 10240, \
                 Member must have value greater than or equal to 512]"
            ),
        ));
    }
    Ok(size)
}

/// All fields of a `CreateFunction` request, already parsed and
/// defaulted. The code zip (if any) is eagerly base64-decoded so the
/// caller can hash it without doing the decode again.
struct CreateFunctionInput {
    function_name: String,
    runtime: String,
    role: String,
    handler: String,
    description: String,
    timeout: i64,
    memory_size: i64,
    package_type: String,
    tags: BTreeMap<String, String>,
    environment: BTreeMap<String, String>,
    architectures: Vec<String>,
    code_zip: Option<Vec<u8>>,
    code_fallback: Vec<u8>,
    image_uri: Option<String>,
    layer_arns: Vec<String>,
    tracing_mode: Option<String>,
    kms_key_arn: Option<String>,
    ephemeral_storage_size: Option<i64>,
    vpc_config: Option<serde_json::Value>,
    snap_start: Option<serde_json::Value>,
    dead_letter_config_arn: Option<String>,
    file_system_configs: Vec<serde_json::Value>,
    logging_config: Option<serde_json::Value>,
    image_config: Option<serde_json::Value>,
    durable_config: Option<serde_json::Value>,
}

impl CreateFunctionInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
        let function_name = body["FunctionName"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "FunctionName is required",
                )
            })?
            .to_string();

        let tags: BTreeMap<String, String> = body["Tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let environment: BTreeMap<String, String> = body["Environment"]["Variables"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let architectures = body["Architectures"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_else(|| vec!["x86_64".to_string()]);

        let code_zip: Option<Vec<u8>> = match body["Code"]["ZipFile"].as_str() {
            Some(b64) => Some(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).map_err(
                    |_| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValueException",
                            "Could not decode Code.ZipFile: invalid base64",
                        )
                    },
                )?,
            ),
            None => None,
        };

        let code_fallback = serde_json::to_vec(&body["Code"]).unwrap_or_default();

        let package_type = body["PackageType"].as_str().unwrap_or("Zip").to_string();
        // ImageUri belongs to `PackageType=Image` functions. Silently
        // dropping it on `Zip` functions avoids GetFunction returning
        // ECR code metadata for a Zip-based function (AWS ignores the
        // field entirely in that case too).
        let image_uri = if package_type == "Image" {
            body["Code"]["ImageUri"].as_str().map(String::from)
        } else {
            None
        };

        // PackageType=Image requires Code.ImageUri; PackageType=Zip requires
        // code content. Reject inconsistent shapes with AWS's error code so
        // SDK-level validation tests see matching behaviour.
        if package_type == "Image" && image_uri.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Code.ImageUri is required when PackageType is Image",
            ));
        }

        let layer_arns: Vec<String> = body["Layers"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let tracing_mode = body["TracingConfig"]["Mode"].as_str().map(String::from);
        let kms_key_arn = body["KMSKeyArn"].as_str().map(String::from);
        let ephemeral_storage_size = match body["EphemeralStorage"]["Size"].as_i64() {
            Some(size) => Some(validate_ephemeral_storage(size)?),
            None => None,
        };
        let vpc_config = body["VpcConfig"]
            .is_object()
            .then(|| body["VpcConfig"].clone());
        let snap_start = body["SnapStart"]
            .is_object()
            .then(|| body["SnapStart"].clone());
        let dead_letter_config_arn = body["DeadLetterConfig"]["TargetArn"]
            .as_str()
            .map(String::from);
        let file_system_configs = body["FileSystemConfigs"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let logging_config = body["LoggingConfig"]
            .is_object()
            .then(|| body["LoggingConfig"].clone());
        let image_config = body["ImageConfig"]
            .is_object()
            .then(|| body["ImageConfig"].clone());
        let durable_config = body["DurableConfig"]
            .is_object()
            .then(|| body["DurableConfig"].clone());

        Ok(Self {
            function_name,
            runtime: body["Runtime"].as_str().unwrap_or("python3.12").to_string(),
            role: body["Role"].as_str().unwrap_or("").to_string(),
            handler: body["Handler"]
                .as_str()
                .unwrap_or("index.handler")
                .to_string(),
            description: body["Description"].as_str().unwrap_or("").to_string(),
            timeout: body["Timeout"].as_i64().unwrap_or(3),
            memory_size: body["MemorySize"].as_i64().unwrap_or(128),
            package_type,
            tags,
            environment,
            architectures,
            code_zip,
            code_fallback,
            image_uri,
            layer_arns,
            tracing_mode,
            kms_key_arn,
            ephemeral_storage_size,
            vpc_config,
            snap_start,
            dead_letter_config_arn,
            file_system_configs,
            logging_config,
            image_config,
            durable_config,
        })
    }
}

/// AWS Lambda's InvocationType: synchronous, async (event), or dry-run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvocationType {
    RequestResponse,
    Event,
    DryRun,
}

impl InvocationType {
    pub fn from_header(value: Option<&str>) -> Self {
        match value {
            Some("Event") => Self::Event,
            Some("DryRun") => Self::DryRun,
            _ => Self::RequestResponse,
        }
    }
}

/// Route an async-invoke result to the configured OnSuccess / OnFailure
/// destination. Destination is matched by ARN scheme: SQS, SNS, EventBridge,
/// or another Lambda. Mirrors the AWS Lambda destinations record schema.
fn route_to_destination(
    bus: Arc<fakecloud_core::delivery::DeliveryBus>,
    function_arn: &str,
    request_payload: &[u8],
    result: &Result<Vec<u8>, String>,
    destination_config: Option<&serde_json::Value>,
) {
    let Some(cfg) = destination_config else {
        return;
    };
    let (key, condition, response_value): (&str, &str, serde_json::Value) = match result {
        Ok(bytes) => (
            "OnSuccess",
            "Success",
            serde_json::from_slice(bytes).unwrap_or(serde_json::Value::Null),
        ),
        Err(err) => (
            "OnFailure",
            "RetriesExhausted",
            serde_json::json!({ "errorMessage": err }),
        ),
    };
    let Some(dest) = cfg
        .get(key)
        .and_then(|v| v.get("Destination"))
        .and_then(|v| v.as_str())
    else {
        return;
    };
    let request_payload_v: serde_json::Value =
        serde_json::from_slice(request_payload).unwrap_or(serde_json::Value::Null);
    let record = serde_json::json!({
        "version": "1.0",
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "requestContext": {
            "requestId": uuid::Uuid::new_v4().to_string(),
            "functionArn": format!("{function_arn}:$LATEST"),
            "condition": condition,
            "approximateInvokeCount": 1,
        },
        "requestPayload": request_payload_v,
        "responseContext": {
            "statusCode": 200,
            "executedVersion": "$LATEST",
        },
        "responsePayload": response_value,
    });
    let body = record.to_string();
    if dest.contains(":sqs:") {
        bus.send_to_sqs(dest, &body, &std::collections::HashMap::new());
    } else if dest.contains(":sns:") {
        bus.publish_to_sns(dest, &body, None);
    } else if dest.contains(":lambda:") {
        let dest = dest.to_string();
        let payload = body.clone();
        tokio::spawn(async move {
            let _ = bus.invoke_lambda(&dest, &payload).await;
        });
    } else if dest.contains(":events:") || dest.contains(":eventbridge:") {
        let detail_type = if result.is_ok() {
            "Lambda Function Invocation Result - Success"
        } else {
            "Lambda Function Invocation Result - Failure"
        };
        bus.put_event_to_eventbridge("lambda", detail_type, &body, "default");
    }
}

/// Decrements the per-function in-flight counter on drop. Lives as
/// long as the invocation it gates — for synchronous invokes that's
/// the function call's stack frame; for `Event` invokes the guard is
/// moved into the spawned task so the counter drops only when the
/// async work finishes.
pub(crate) struct ConcurrencyGuard {
    pub(crate) map: Arc<parking_lot::RwLock<BTreeMap<String, i64>>>,
    pub(crate) key: String,
}

impl Drop for ConcurrencyGuard {
    fn drop(&mut self) {
        let mut m = self.map.write();
        let n = m.get(&self.key).copied().unwrap_or(0);
        if n <= 1 {
            m.remove(&self.key);
        } else {
            m.insert(self.key.clone(), n - 1);
        }
    }
}

/// Map an Invoke `Qualifier` (alias name, numeric version, or
/// `$LATEST`) to a concrete numeric version string. Aliases with a
/// `RoutingConfig.AdditionalVersionWeights` table do a weighted pick
/// across the alias's primary `function_version` plus the additional
/// True when `prev` is byte-equivalent to `live` for every field
/// that `PublishVersion` would otherwise capture into a new snapshot.
/// Used to short-circuit a no-op publish (AWS-style idempotency:
/// re-publishing without any change returns the previous version
/// unchanged). The comparison spans code identity (sha + size),
/// configuration (runtime/handler/role/timeout/memory/env/layers/...)
/// and every advanced field round-tripped through
/// `function_config_json`. The caller is responsible for resolving
/// the `effective_description` (caller-supplied override wins over
/// the live `$LATEST` description, matching real PublishVersion
/// semantics).
fn function_config_unchanged_for_publish(
    prev: &LambdaFunction,
    live: &LambdaFunction,
    effective_description: &str,
) -> bool {
    prev.code_sha256 == live.code_sha256
        && prev.code_size == live.code_size
        && prev.image_uri == live.image_uri
        && prev.package_type == live.package_type
        && prev.runtime == live.runtime
        && prev.role == live.role
        && prev.handler == live.handler
        && prev.description == effective_description
        && prev.timeout == live.timeout
        && prev.memory_size == live.memory_size
        && prev.environment == live.environment
        && prev.architectures == live.architectures
        && prev.layers.len() == live.layers.len()
        && prev
            .layers
            .iter()
            .zip(live.layers.iter())
            .all(|(a, b)| a.arn == b.arn && a.code_size == b.code_size)
        && prev.tracing_mode == live.tracing_mode
        && prev.kms_key_arn == live.kms_key_arn
        && prev.ephemeral_storage_size == live.ephemeral_storage_size
        && prev.vpc_config == live.vpc_config
        && prev.dead_letter_config_arn == live.dead_letter_config_arn
        && prev.file_system_configs == live.file_system_configs
        && prev.logging_config == live.logging_config
        && prev.image_config == live.image_config
        && prev.signing_profile_version_arn == live.signing_profile_version_arn
        && prev.signing_job_arn == live.signing_job_arn
        && prev.runtime_version_config == live.runtime_version_config
        && snap_start_apply_on_eq(prev.snap_start.as_ref(), live.snap_start.as_ref())
}

/// Compare two `SnapStart` configs by `ApplyOn` only — that's the
/// caller-supplied knob. `OptimizationStatus` is server-side state
/// that PublishVersion mutates on snapshots (flipping to "On" when
/// ApplyOn=PublishedVersions) while $LATEST stays "Off", so a deep
/// equality check here would never match on a SnapStart-enabled
/// function and PublishVersion would never be idempotent. Treating
/// `None` and `{ApplyOn:"None"}` as equivalent matches AWS, which
/// emits the latter when the field is unset.
fn snap_start_apply_on_eq(prev: Option<&Value>, live: Option<&Value>) -> bool {
    let prev_apply = prev
        .and_then(|v| v.get("ApplyOn"))
        .and_then(|v| v.as_str())
        .unwrap_or("None");
    let live_apply = live
        .and_then(|v| v.get("ApplyOn"))
        .and_then(|v| v.as_str())
        .unwrap_or("None");
    prev_apply == live_apply
}

/// versions in the weight map. Returns `None` for `$LATEST` /
/// unqualified invokes (caller uses the live `$LATEST` config).
pub(crate) fn resolve_qualifier_to_version(
    state: &LambdaState,
    function_name: &str,
    qualifier: Option<&str>,
) -> Option<String> {
    let q = qualifier?;
    if q == "$LATEST" {
        return None;
    }
    if q.chars().all(|c| c.is_ascii_digit()) {
        return Some(q.to_string());
    }
    let alias_key = format!("{function_name}:{q}");
    let alias = state.aliases.get(&alias_key)?;
    let primary = alias.function_version.clone();
    let routing = alias
        .routing_config
        .as_ref()
        .and_then(|rc| rc.get("AdditionalVersionWeights"))
        .and_then(|m| m.as_object());
    let Some(weights) = routing else {
        return Some(primary);
    };
    // Sum of additional weights ∈ [0,1]; primary gets 1 - sum. Pick
    // uniformly in [0,1) and walk the cumulative weight axis.
    let mut additional: Vec<(String, f64)> = Vec::with_capacity(weights.len());
    let mut sum: f64 = 0.0;
    for (ver, w) in weights {
        let weight = w.as_f64().unwrap_or(0.0).clamp(0.0, 1.0);
        sum += weight;
        additional.push((ver.clone(), weight));
    }
    let primary_weight = (1.0 - sum).max(0.0);
    let pick: f64 = {
        // Mix a thread-local LCG state with wall-clock nanos so
        // back-to-back calls within a single process tick still
        // produce distinct picks. Invoke routing only needs fairness
        // over many invokes, not crypto randomness.
        use std::cell::Cell;
        thread_local! {
            static RNG: Cell<u64> = const { Cell::new(0x9E37_79B9_7F4A_7C15) };
        }
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        RNG.with(|cell| {
            let mut s = cell.get() ^ now_nanos;
            // splitmix64 step
            s = s.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = s;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            cell.set(s);
            (z >> 11) as f64 / ((1u64 << 53) as f64)
        })
    };
    let mut acc = primary_weight;
    if pick < acc {
        return Some(primary);
    }
    for (ver, w) in &additional {
        acc += w;
        if pick < acc {
            return Some(ver.clone());
        }
    }
    Some(primary)
}

pub struct LambdaService {
    pub(crate) state: SharedLambdaState,
    pub(crate) runtime: Option<Arc<ContainerRuntime>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    pub(crate) delivery_bus: Option<Arc<fakecloud_core::delivery::DeliveryBus>>,
    pub(crate) role_trust_validator: Option<Arc<dyn fakecloud_core::auth::RoleTrustValidator>>,
    pub(crate) s3_delivery: Option<Arc<dyn fakecloud_core::delivery::S3Delivery>>,
    /// Per-account-per-function in-flight invocation count, used to
    /// gate `Invoke` against `PutFunctionConcurrency`'s
    /// `ReservedConcurrentExecutions` ceiling. Keyed by
    /// `{account_id}:{function_name}`. Live counter — incremented at
    /// invoke entry, decremented when the invocation completes (or
    /// when the spawned async task finishes for `Event` invokes).
    pub(crate) inflight_invocations: Arc<parking_lot::RwLock<BTreeMap<String, i64>>>,
}

mod functions;
mod init;
mod invoke;
mod publish;

impl LambdaService {
    pub fn new(state: SharedLambdaState) -> Self {
        Self {
            state,
            runtime: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
            delivery_bus: None,
            role_trust_validator: None,
            s3_delivery: None,
            inflight_invocations: Arc::new(parking_lot::RwLock::new(BTreeMap::new())),
        }
    }

    pub fn with_s3_delivery(mut self, s3: Arc<dyn fakecloud_core::delivery::S3Delivery>) -> Self {
        self.s3_delivery = Some(s3);
        self
    }

    pub fn with_runtime(mut self, runtime: Arc<ContainerRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_delivery_bus(mut self, bus: Arc<fakecloud_core::delivery::DeliveryBus>) -> Self {
        self.delivery_bus = Some(bus);
        self
    }

    pub fn with_role_trust_validator(
        mut self,
        validator: Arc<dyn fakecloud_core::auth::RoleTrustValidator>,
    ) -> Self {
        self.role_trust_validator = Some(validator);
        self
    }

    async fn save_snapshot(&self) {
        save_lambda_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Lambda state when invoked, or
    /// `None` in memory mode (no snapshot store). The CloudFormation provisioner
    /// mutates `state` directly and uses this to write a CFN-provisioned
    /// function through to disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_lambda_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current Lambda state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `LambdaService::save_snapshot` and the
/// CloudFormation provisioner's post-provision persist hook so both route
/// through the same serialize-and-write path.
pub async fn save_lambda_snapshot(
    state: &SharedLambdaState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = LambdaSnapshot {
        schema_version: LAMBDA_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
        state: None,
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write lambda snapshot"),
        Err(err) => tracing::error!(%err, "lambda snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for LambdaService {
    fn service_name(&self) -> &str {
        "lambda"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, resource_name) = Self::resolve_action(&req).ok_or_else(|| {
            // Distinguish a genuinely unknown URL path from one that
            // hit a known Lambda collection (`/functions`, `/layers`,
            // `/event-source-mappings`, `/tags`, `/code-signing-configs`,
            // `/account-settings`, `/layers-by-arn`) but couldn't be
            // routed because a required identifier was empty or the
            // method was wrong. The latter is a client-side validation
            // error (`InvalidParameterValueException`), not a
            // "service doesn't implement this" signal — collapsing the
            // two confuses conformance probes whose synthetic too-short
            // identifiers collapse path segments at the URL level.
            const KNOWN_COLLECTIONS: &[&str] = &[
                "functions",
                "layers",
                "layers-by-arn",
                "event-source-mappings",
                "tags",
                "account-settings",
                "code-signing-configs",
            ];
            let is_known_collection = req
                .path_segments
                .get(1)
                .map(|s| KNOWN_COLLECTIONS.contains(&s.as_str()))
                .unwrap_or(false);
            if is_known_collection {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!(
                        "Could not route request {} {} — missing or invalid identifier",
                        req.method, req.raw_path
                    ),
                )
            } else {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "UnknownOperationException",
                    format!("Unknown operation: {} {}", req.method, req.raw_path),
                )
            }
        })?;

        // Normalize FunctionName-bearing resource slots: AWS Lambda accepts
        // bare name, name:qualifier, partial ARN, and full ARN in any URL
        // slot that names a function. Layer / event-source-mapping resource
        // names go through different routes and are left as-is.
        // Capture a qualifier embedded in the raw function reference (e.g.
        // `...:function:MyFn:PROD`) before normalization strips it, so Invoke
        // can fall back to it when no `?Qualifier=` is supplied (1.3).
        let arn_embedded_qualifier = resource_name
            .as_deref()
            .and_then(qualifier_from_function_ref);
        let resource_name = if action_takes_function_name(action) {
            // Enforce the Smithy length bound (`FunctionName.length 1..140`)
            // before normalization. Synthetic conformance variants drive
            // 141-character strings through these paths; without an early
            // reject we'd happily serve `GetFunction` against a name that
            // could never have been created. The 170-char ceiling tracks
            // the documented ARN-form upper bound.
            if let Some(raw) = resource_name.as_ref() {
                // Percent-decode the path label before length-checking;
                // SDK clients escape `:` to `%3A` for ARN-form names, so
                // the raw count overruns the 200-char ARN ceiling on
                // valid inputs.
                let decoded = crate::extras::percent_decode_for_length(raw);
                let len = decoded.chars().count();
                // Bare-name form caps at 140. ARN form
                // (`arn:aws:lambda:<region>:<acct>:function:<name>`)
                // adds ~60 chars of prefix → up to ~200 total. Reject
                // anything longer outright so synthetic 141-char names
                // can't bypass the constraint. `InvokeAsync`'s Smithy
                // error envelope doesn't declare
                // `InvalidParameterValueException`, so route its
                // too-long inputs through `ResourceNotFoundException`
                // instead — which is declared, and also reflects
                // the practical outcome of looking up a 141-char name.
                let limit = if decoded.starts_with("arn:") {
                    200
                } else {
                    140
                };
                if decoded.is_empty() || len > limit {
                    let (code, msg) = if action == "InvokeAsync" {
                        (
                            "ResourceNotFoundException",
                            format!("Function not found: {}", raw),
                        )
                    } else {
                        (
                            "InvalidParameterValueException",
                            format!(
                                "1 validation error detected: Value '{}' at 'functionName' failed to \
                                 satisfy constraint: Member must have length less than or equal to 140",
                                raw
                            ),
                        )
                    };
                    return Err(AwsServiceError::aws_error(
                        if action == "InvokeAsync" {
                            StatusCode::NOT_FOUND
                        } else {
                            StatusCode::BAD_REQUEST
                        },
                        code,
                        msg,
                    ));
                }
            }
            resource_name.map(|s| normalize_function_name(&s))
        } else {
            resource_name
        };

        // Generic MaxItems range guard. The query is bound to different
        // Smithy integer shapes per operation (general `MaxListItems`
        // is 1..10000; layer/url/event-invoke/provisioned-concurrency
        // listings cap at 50). Pick the right ceiling for the routed
        // action so above-max variants trip the validation reliably.
        if let Some(raw) = req.query_params.get("MaxItems") {
            // Non-numeric MaxItems is a malformed request, not "use the
            // default". AWS responds 400 — reject before falling through
            // to range-check on the parsed value.
            let n = raw.parse::<i64>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!("MaxItems must be a number (got '{raw}')"),
                )
            })?;
            // ListAliases' MaxItems range is 1..10000 in the Smithy model (only
            // the layer / url-config / provisioned-concurrency / event-invoke
            // lists cap at 50), so it must not be grouped with the 50-cap ops.
            let max = match action {
                "ListLayers"
                | "ListLayerVersions"
                | "ListFunctionUrlConfigs"
                | "ListProvisionedConcurrencyConfigs"
                | "ListFunctionEventInvokeConfigs" => 50,
                _ => 10000,
            };
            if !(1..=max).contains(&n) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!("MaxItems must be between 1 and {} (got {})", max, n),
                ));
            }
        }

        // Smithy `Qualifier` shape is `length 1..128`. Probe variants
        // exercise the lower boundary by sending the empty string;
        // reject pre-dispatch so every per-handler `parse_qualifier`
        // call doesn't need its own check.
        if let Some(q) = req.query_params.get("Qualifier") {
            let len = q.chars().count();
            if q.is_empty() || len > 128 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!("Qualifier must be 1..128 characters (got length {})", len),
                ));
            }
        }
        // Same guard for the `FunctionVersion` query member used by
        // `ListAliases` (`length 1..1024` / pattern `(\\$LATEST|[0-9]+)`).
        if let Some(fv) = req.query_params.get("FunctionVersion") {
            let len = fv.chars().count();
            if fv.is_empty() || len > 1024 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!(
                        "FunctionVersion must be 1..1024 characters (got length {})",
                        len
                    ),
                ));
            }
        }

        let mutates = matches!(
            action,
            "CreateFunction"
                | "DeleteFunction"
                | "PublishVersion"
                | "AddPermission"
                | "RemovePermission"
                | "CreateEventSourceMapping"
                | "DeleteEventSourceMapping"
                | "UpdateEventSourceMapping"
                | "UpdateFunctionCode"
                | "UpdateFunctionConfiguration"
                | "CreateAlias"
                | "DeleteAlias"
                | "UpdateAlias"
                | "PublishLayerVersion"
                | "DeleteLayerVersion"
                | "AddLayerVersionPermission"
                | "RemoveLayerVersionPermission"
                | "CreateFunctionUrlConfig"
                | "DeleteFunctionUrlConfig"
                | "UpdateFunctionUrlConfig"
                | "PutFunctionConcurrency"
                | "DeleteFunctionConcurrency"
                | "PutProvisionedConcurrencyConfig"
                | "DeleteProvisionedConcurrencyConfig"
                | "CreateCodeSigningConfig"
                | "UpdateCodeSigningConfig"
                | "DeleteCodeSigningConfig"
                | "PutFunctionCodeSigningConfig"
                | "DeleteFunctionCodeSigningConfig"
                | "PutFunctionEventInvokeConfig"
                | "UpdateFunctionEventInvokeConfig"
                | "DeleteFunctionEventInvokeConfig"
                | "PutRuntimeManagementConfig"
                | "PutFunctionScalingConfig"
                | "PutFunctionRecursionConfig"
                | "TagResource"
                | "UntagResource"
                | "InvokeAsync"
                | "InvokeWithResponseStream"
        );

        let aid = &req.account_id;
        // Smithy-aligned validation for the handful of input fields whose
        // refreshed @length / @range / enum constraints surface as new
        // conformance variants. Centralised here so the body parser in each
        // handler stays focused on shape transforms.
        prevalidate_lambda(action, &req)?;
        let result = match action {
            "CreateFunction" => self.create_function(&req),
            "ListFunctions" => self.list_functions(
                aid,
                req.query_params.get("FunctionVersion").map(String::as_str),
                req.query_params.get("Marker").map(String::as_str),
                marker_page_size(&req),
            ),
            "GetFunction" => self.get_function(
                &req,
                resource_name.as_deref().unwrap_or(""),
                aid,
                req.region.as_str(),
                req.query_params.get("Qualifier").map(String::as_str),
            ),
            "DeleteFunction" => self.delete_function(
                resource_name.as_deref().unwrap_or(""),
                aid,
                req.query_params.get("Qualifier").map(String::as_str),
            ),
            "Invoke" => {
                let invocation_type = InvocationType::from_header(
                    req.headers
                        .get("x-amz-invocation-type")
                        .and_then(|v| v.to_str().ok()),
                );
                let log_tail = req
                    .headers
                    .get("x-amz-log-type")
                    .and_then(|v| v.to_str().ok())
                    == Some("Tail");
                // `?Qualifier=` wins; otherwise honor a qualifier embedded in
                // the function ARN/ref (1.3).
                let qualifier = req
                    .query_params
                    .get("Qualifier")
                    .map(String::as_str)
                    .or(arn_embedded_qualifier.as_deref());
                self.invoke(
                    resource_name.as_deref().unwrap_or(""),
                    &req.body,
                    aid,
                    invocation_type,
                    qualifier,
                    log_tail,
                )
                .await
            }
            "InvokeAsync" => {
                // `InvokeAsync` is deprecated. AWS returns 202 with a
                // `Status` body and never surfaces synchronous-invoke
                // errors (`InvalidParameterValueException` isn't in
                // the op's declared error envelope). Validate the
                // function exists, then enqueue is a no-op.
                let name = resource_name.as_deref().unwrap_or("");
                let accounts = self.state.read();
                let exists = accounts
                    .get(aid)
                    .map(|s| s.functions.contains_key(name))
                    .unwrap_or(false);
                if !exists {
                    Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ResourceNotFoundException",
                        format!("Function not found: {}", name),
                    ))
                } else {
                    Ok(AwsResponse::json(
                        StatusCode::ACCEPTED,
                        json!({ "Status": 202 }).to_string(),
                    ))
                }
            }
            "PublishVersion" => {
                self.publish_version(resource_name.as_deref().unwrap_or(""), aid, &req)
            }
            "AddPermission" => self.add_permission(resource_name.as_deref().unwrap_or(""), &req),
            "GetPolicy" => self.get_policy(
                resource_name.as_deref().unwrap_or(""),
                aid,
                req.query_params.get("Qualifier").map(String::as_str),
            ),
            "RemovePermission" => {
                // Path: /2015-03-31/functions/{name}/policy/{sid}
                let sid = req.path_segments.get(4).cloned().unwrap_or_default();
                self.remove_permission(
                    resource_name.as_deref().unwrap_or(""),
                    &sid,
                    aid,
                    req.query_params.get("Qualifier").map(String::as_str),
                )
            }
            "CreateEventSourceMapping" => self.create_event_source_mapping(&req),
            "ListEventSourceMappings" => {
                // `FunctionName` is an optional httpQuery member, but
                // when present it must satisfy `length 1..140` like
                // every other `FunctionName` slot in the API.
                if let Some(fn_name) = req.query_params.get("FunctionName") {
                    let len = fn_name.chars().count();
                    if fn_name.is_empty() || len > 140 {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValueException",
                            "FunctionName must be 1..140 characters",
                        ));
                    }
                }
                self.list_event_source_mappings(aid, &req)
            }
            "GetEventSourceMapping" => {
                self.get_event_source_mapping(resource_name.as_deref().unwrap_or(""), aid)
            }
            "DeleteEventSourceMapping" => {
                self.delete_event_source_mapping(resource_name.as_deref().unwrap_or(""), aid)
            }
            "CreateCapacityProvider" => {
                crate::workflows::create_capacity_provider(&self.state, &req, &req.json_body())
            }
            "GetCapacityProvider" => crate::workflows::get_capacity_provider(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            "ListCapacityProviders" => crate::workflows::list_capacity_providers(&self.state, &req),
            "UpdateCapacityProvider" => crate::workflows::update_capacity_provider(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
                &req.json_body(),
            ),
            "DeleteCapacityProvider" => crate::workflows::delete_capacity_provider(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            "ListFunctionVersionsByCapacityProvider" => {
                crate::workflows::list_function_versions_by_capacity_provider(
                    &self.state,
                    &req,
                    resource_name.as_deref().unwrap_or(""),
                )
            }
            "GetDurableExecution" => crate::workflows::get_durable_execution(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            "GetDurableExecutionHistory" => crate::workflows::get_durable_execution_history(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            "GetDurableExecutionState" => crate::workflows::get_durable_execution_state(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            "ListDurableExecutionsByFunction" => {
                crate::workflows::list_durable_executions_by_function(
                    &self.state,
                    &req,
                    resource_name.as_deref().unwrap_or(""),
                )
            }
            "CheckpointDurableExecution" => crate::workflows::checkpoint_durable_execution(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
                &req.json_body(),
            ),
            "StopDurableExecution" => crate::workflows::stop_durable_execution(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            "SendDurableExecutionCallbackSuccess" => crate::workflows::send_callback_success(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            "SendDurableExecutionCallbackFailure" => crate::workflows::send_callback_failure(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            "SendDurableExecutionCallbackHeartbeat" => crate::workflows::send_callback_heartbeat(
                &self.state,
                &req,
                resource_name.as_deref().unwrap_or(""),
            ),
            other => {
                self.handle_extra(other, resource_name.as_deref(), &req)
                    .await
            }
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateFunction",
            "GetFunction",
            "DeleteFunction",
            "ListFunctions",
            "Invoke",
            "InvokeAsync",
            "InvokeWithResponseStream",
            "PublishVersion",
            "ListVersionsByFunction",
            "AddPermission",
            "RemovePermission",
            "GetPolicy",
            "CreateEventSourceMapping",
            "ListEventSourceMappings",
            "GetEventSourceMapping",
            "UpdateEventSourceMapping",
            "DeleteEventSourceMapping",
            "GetFunctionConfiguration",
            "UpdateFunctionConfiguration",
            "UpdateFunctionCode",
            "GetAccountSettings",
            "CreateAlias",
            "GetAlias",
            "ListAliases",
            "UpdateAlias",
            "DeleteAlias",
            "PublishLayerVersion",
            "GetLayerVersion",
            "GetLayerVersionByArn",
            "DeleteLayerVersion",
            "ListLayerVersions",
            "ListLayers",
            "GetLayerVersionPolicy",
            "AddLayerVersionPermission",
            "RemoveLayerVersionPermission",
            "CreateFunctionUrlConfig",
            "GetFunctionUrlConfig",
            "UpdateFunctionUrlConfig",
            "DeleteFunctionUrlConfig",
            "ListFunctionUrlConfigs",
            "PutFunctionConcurrency",
            "GetFunctionConcurrency",
            "DeleteFunctionConcurrency",
            "PutProvisionedConcurrencyConfig",
            "GetProvisionedConcurrencyConfig",
            "DeleteProvisionedConcurrencyConfig",
            "ListProvisionedConcurrencyConfigs",
            "CreateCodeSigningConfig",
            "GetCodeSigningConfig",
            "UpdateCodeSigningConfig",
            "DeleteCodeSigningConfig",
            "ListCodeSigningConfigs",
            "PutFunctionCodeSigningConfig",
            "GetFunctionCodeSigningConfig",
            "DeleteFunctionCodeSigningConfig",
            "ListFunctionsByCodeSigningConfig",
            "PutFunctionEventInvokeConfig",
            "GetFunctionEventInvokeConfig",
            "UpdateFunctionEventInvokeConfig",
            "DeleteFunctionEventInvokeConfig",
            "ListFunctionEventInvokeConfigs",
            "PutRuntimeManagementConfig",
            "GetRuntimeManagementConfig",
            "PutFunctionScalingConfig",
            "GetFunctionScalingConfig",
            "PutFunctionRecursionConfig",
            "GetFunctionRecursionConfig",
            "TagResource",
            "UntagResource",
            "ListTags",
            "CreateCapacityProvider",
            "GetCapacityProvider",
            "ListCapacityProviders",
            "UpdateCapacityProvider",
            "DeleteCapacityProvider",
            "ListFunctionVersionsByCapacityProvider",
            "GetDurableExecution",
            "GetDurableExecutionHistory",
            "GetDurableExecutionState",
            "ListDurableExecutionsByFunction",
            "CheckpointDurableExecution",
            "StopDurableExecution",
            "SendDurableExecutionCallbackSuccess",
            "SendDurableExecutionCallbackFailure",
            "SendDurableExecutionCallbackHeartbeat",
        ]
    }

    fn iam_enforceable(&self) -> bool {
        true
    }

    /// Lambda resources are function ARNs. Function-scoped ops
    /// resolve the target ARN from the path; list ops target `*`
    /// (the whole service), matching how AWS models them.
    fn iam_action_for(&self, request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
        // REST-JSON services don't have `request.action` populated at
        // dispatch time — it's derived from method+path inside
        // `handle()`. Reuse the same resolver so the two can never
        // drift.
        let (action_str, resource_name) = Self::resolve_action(request)?;
        // Every op that `resolve_action` (and therefore `handle`) can
        // dispatch MUST map to an IAM action here. Lambda is
        // `iam_enforceable`, so an unmapped op returning `None` would run
        // with zero policy evaluation — a silent auth bypass. We drive
        // this from the same op-name strings the dispatcher uses so the
        // two can't drift; the exhaustiveness test asserts every
        // dispatchable op resolves to `Some`.
        let action = iam_action_name_for(action_str)?;
        let accounts = self.state.read();
        let empty = LambdaState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let resource = match action_str {
            // Function-scoped ops: the path identifier is a function name
            // (or ARN/partial-ARN). Normalize it to a bare name and build
            // the canonical function ARN so policy evaluation matches the
            // real function regardless of how the caller spelled it.
            "GetFunction"
            | "DeleteFunction"
            | "Invoke"
            | "InvokeAsync"
            | "InvokeWithResponseStream"
            | "PublishVersion"
            | "ListVersionsByFunction"
            | "AddPermission"
            | "RemovePermission"
            | "GetPolicy"
            | "GetFunctionConfiguration"
            | "UpdateFunctionConfiguration"
            | "UpdateFunctionCode"
            | "CreateAlias"
            | "GetAlias"
            | "UpdateAlias"
            | "DeleteAlias"
            | "ListAliases"
            | "PutFunctionConcurrency"
            | "GetFunctionConcurrency"
            | "DeleteFunctionConcurrency"
            | "PutProvisionedConcurrencyConfig"
            | "GetProvisionedConcurrencyConfig"
            | "DeleteProvisionedConcurrencyConfig"
            | "ListProvisionedConcurrencyConfigs"
            | "PutFunctionEventInvokeConfig"
            | "GetFunctionEventInvokeConfig"
            | "UpdateFunctionEventInvokeConfig"
            | "DeleteFunctionEventInvokeConfig"
            | "ListFunctionEventInvokeConfigs"
            | "PutRuntimeManagementConfig"
            | "GetRuntimeManagementConfig"
            | "PutFunctionScalingConfig"
            | "GetFunctionScalingConfig"
            | "PutFunctionRecursionConfig"
            | "GetFunctionRecursionConfig"
            | "PutFunctionCodeSigningConfig"
            | "GetFunctionCodeSigningConfig"
            | "DeleteFunctionCodeSigningConfig"
            | "CreateFunctionUrlConfig"
            | "GetFunctionUrlConfig"
            | "UpdateFunctionUrlConfig"
            | "DeleteFunctionUrlConfig"
            | "ListFunctionUrlConfigs"
            | "ListDurableExecutionsByFunction" => {
                let raw = resource_name.unwrap_or_default();
                if raw.is_empty() {
                    "*".to_string()
                } else {
                    // Normalize ARN / `function:Name` / partial-ARN
                    // inputs to bare names — IAM resource derivation
                    // must produce the same ARN regardless of how the
                    // caller spelled FunctionName, or policy evaluation
                    // mismatches the actual function.
                    let name = normalize_function_name(&raw);
                    format!(
                        "arn:aws:lambda:{}:{}:function:{}",
                        state.region, state.account_id, name
                    )
                }
            }
            "CreateFunction" => {
                // Best-effort: parse the FunctionName from the body so
                // CreateFunction can be resource-scoped against the
                // to-be-created ARN. Falls back to `*` when the body
                // isn't JSON yet (e.g. soft-mode observability).
                serde_json::from_slice::<Value>(&request.body)
                    .ok()
                    .and_then(|v| {
                        v.get("FunctionName").and_then(|f| f.as_str()).map(|n| {
                            format!(
                                "arn:aws:lambda:{}:{}:function:{}",
                                state.region, state.account_id, n
                            )
                        })
                    })
                    .unwrap_or_else(|| "*".to_string())
            }
            _ => "*".to_string(),
        };
        Some(fakecloud_core::auth::IamAction {
            service: "lambda",
            action,
            resource,
        })
    }

    fn iam_condition_keys_for(
        &self,
        request: &AwsRequest,
        action: &fakecloud_core::auth::IamAction,
    ) -> std::collections::BTreeMap<String, Vec<String>> {
        let mut out = std::collections::BTreeMap::new();
        if action.action == "AddPermission" {
            if action.resource != "*" {
                out.insert(
                    "lambda:functionarn".to_string(),
                    vec![action.resource.clone()],
                );
            }
            if let Ok(body) = serde_json::from_slice::<Value>(&request.body) {
                if let Some(principal) = body.get("Principal").and_then(|p| p.as_str()) {
                    out.insert("lambda:principal".to_string(), vec![principal.to_string()]);
                }
            }
        }
        out
    }
}

#[path = "../service_event_sources.rs"]
mod service_event_sources;
#[path = "../service_permissions.rs"]
mod service_permissions;

#[cfg(test)]
#[path = "../service_tests.rs"]
mod tests;
