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
            check_optional_int_range("Timeout", body["Timeout"].as_i64(), 1, i64::MAX)?;
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
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let snapshot = LambdaSnapshot {
            schema_version: LAMBDA_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(self.state.read().clone()),
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

    /// Determine the action from the HTTP method and path segments.
    /// Lambda uses REST-style routing:
    ///   POST   /2015-03-31/functions                         -> CreateFunction
    ///   GET    /2015-03-31/functions                         -> ListFunctions
    ///   GET    /2015-03-31/functions/{name}                  -> GetFunction
    ///   DELETE /2015-03-31/functions/{name}                  -> DeleteFunction
    ///   POST   /2015-03-31/functions/{name}/invocations      -> Invoke
    ///   POST   /2015-03-31/functions/{name}/versions         -> PublishVersion
    ///   POST   /2015-03-31/event-source-mappings             -> CreateEventSourceMapping
    ///   GET    /2015-03-31/event-source-mappings             -> ListEventSourceMappings
    ///   GET    /2015-03-31/event-source-mappings/{uuid}      -> GetEventSourceMapping
    ///   DELETE /2015-03-31/event-source-mappings/{uuid}      -> DeleteEventSourceMapping
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Option<String>)> {
        let segs = &req.path_segments;
        if segs.is_empty() {
            return None;
        }
        // The Lambda data API uses many date prefixes (one per
        // operation family). Recognise any well-formed YYYY-MM-DD
        // prefix and route based on the path structure that follows.
        let prefix = segs[0].as_str();

        // Account settings + InvokeAsync — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("account-settings") && req.method == Method::GET
        {
            return Some(("GetAccountSettings", None));
        }
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("invoke-async")
            && req.method == Method::POST
        {
            return Some(("InvokeAsync", segs.get(2).map(|s| s.to_string())));
        }
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("response-streaming-invocations")
            && req.method == Method::POST
        {
            return Some((
                "InvokeWithResponseStream",
                segs.get(2).map(|s| s.to_string()),
            ));
        }

        // Concurrency (reserved + provisioned) — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("concurrency")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionConcurrency", res)),
                Method::GET => Some(("GetFunctionConcurrency", res)),
                Method::DELETE => Some(("DeleteFunctionConcurrency", res)),
                _ => None,
            };
        }

        // Provisioned concurrency at any prefix. AWS overloads
        // `GET /functions/{name}/provisioned-concurrency` on the
        // `List=ALL` query parameter — with it, the op is
        // `ListProvisionedConcurrencyConfigs`; without, it's
        // `GetProvisionedConcurrencyConfig` (which needs a
        // `Qualifier`). Disambiguate before falling into the
        // per-method match.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("provisioned-concurrency")
        {
            let res = segs.get(2).map(|s| s.to_string());
            if req.method == Method::GET && req.query_params.contains_key("List") {
                return Some(("ListProvisionedConcurrencyConfigs", res));
            }
            return match req.method {
                Method::PUT => Some(("PutProvisionedConcurrencyConfig", res)),
                Method::GET => Some(("GetProvisionedConcurrencyConfig", res)),
                Method::DELETE => Some(("DeleteProvisionedConcurrencyConfig", res)),
                _ => None,
            };
        }
        // Legacy alias path (`provisioned-concurrency-configs`) — kept
        // for backward compatibility with conformance fixtures that
        // hand-craft the URL.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("provisioned-concurrency-configs")
            && req.method == Method::GET
        {
            return Some((
                "ListProvisionedConcurrencyConfigs",
                segs.get(2).map(|s| s.to_string()),
            ));
        }

        // Event invoke config — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("event-invoke-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::POST => Some(("PutFunctionEventInvokeConfig", res)),
                Method::PUT => Some(("UpdateFunctionEventInvokeConfig", res)),
                Method::GET => Some(("GetFunctionEventInvokeConfig", res)),
                Method::DELETE => Some(("DeleteFunctionEventInvokeConfig", res)),
                _ => None,
            };
        }
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && (segs.get(3).map(|s| s.as_str()) == Some("event-invoke-config-list")
                || (segs.get(3).map(|s| s.as_str()) == Some("event-invoke-config")
                    && segs.get(4).map(|s| s.as_str()) == Some("list")))
            && req.method == Method::GET
        {
            return Some((
                "ListFunctionEventInvokeConfigs",
                segs.get(2).map(|s| s.to_string()),
            ));
        }

        // Recursion config — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("recursion-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionRecursionConfig", res)),
                Method::GET => Some(("GetFunctionRecursionConfig", res)),
                _ => None,
            };
        }

        // Runtime management config — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("runtime-management-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutRuntimeManagementConfig", res)),
                Method::GET => Some(("GetRuntimeManagementConfig", res)),
                _ => None,
            };
        }

        // Code signing config (function and global) — any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("code-signing-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionCodeSigningConfig", res)),
                Method::GET => Some(("GetFunctionCodeSigningConfig", res)),
                Method::DELETE => Some(("DeleteFunctionCodeSigningConfig", res)),
                _ => None,
            };
        }
        if segs.get(1).map(|s| s.as_str()) == Some("code-signing-configs") {
            let res = segs.get(2).map(|s| s.to_string());
            return match (
                req.method.clone(),
                segs.len(),
                segs.get(3).map(|s| s.as_str()),
            ) {
                (Method::POST, 2, _) => Some(("CreateCodeSigningConfig", None)),
                (Method::GET, 2, _) => Some(("ListCodeSigningConfigs", None)),
                (Method::GET, 3, _) => Some(("GetCodeSigningConfig", res)),
                (Method::PUT, 3, _) => Some(("UpdateCodeSigningConfig", res)),
                (Method::DELETE, 3, _) => Some(("DeleteCodeSigningConfig", res)),
                (Method::GET, 4, Some("functions")) => {
                    Some(("ListFunctionsByCodeSigningConfig", res))
                }
                _ => None,
            };
        }

        // Tags resource ARN at any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("tags") && segs.len() >= 3 {
            let res = segs[2..].join("/");
            return match req.method {
                Method::POST => Some(("TagResource", Some(res))),
                Method::DELETE => Some(("UntagResource", Some(res))),
                Method::GET => Some(("ListTags", Some(res))),
                _ => None,
            };
        }

        // Function URL config + scaling config (any prefix).
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("url")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::POST => Some(("CreateFunctionUrlConfig", res)),
                Method::GET => Some(("GetFunctionUrlConfig", res)),
                Method::PUT => Some(("UpdateFunctionUrlConfig", res)),
                Method::DELETE => Some(("DeleteFunctionUrlConfig", res)),
                _ => None,
            };
        }
        if segs.get(1).map(|s| s.as_str()) == Some("function-urls") && req.method == Method::GET {
            return Some(("ListFunctionUrlConfigs", None));
        }
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("urls")
            && req.method == Method::GET
        {
            return Some(("ListFunctionUrlConfigs", segs.get(2).map(|s| s.to_string())));
        }
        // Function scaling config — AWS uses
        // `/2025-11-30/functions/{name}/function-scaling-config` (and the
        // earlier `scaling-config` path on event-source-mappings was
        // a different operation that no longer exists in the SDK).
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("function-scaling-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionScalingConfig", res)),
                Method::GET => Some(("GetFunctionScalingConfig", res)),
                _ => None,
            };
        }

        // NOTE: concurrency, event-invoke-config, recursion-config, and
        // code-signing-configs routes are all handled by the prefix-agnostic
        // blocks above. The previously-present date-specific blocks were
        // dead code.

        // /2018-10-31/layers
        if prefix == "2018-10-31" && segs.get(1).map(|s| s.as_str()) == Some("layers") {
            let layer = segs.get(2).map(|s| s.to_string());
            let third = segs.get(3).map(|s| s.as_str());
            let version = segs.get(4).map(|s| s.to_string());
            return match (&req.method, segs.len(), third, version.is_some()) {
                (&Method::GET, 2, _, _) => Some(("ListLayers", None)),
                (&Method::POST, 4, Some("versions"), false) => Some(("PublishLayerVersion", layer)),
                (&Method::GET, 4, Some("versions"), false) => {
                    // `GET .../versions` is `ListLayerVersions`; the same
                    // path with a trailing slash is a malformed
                    // `GetLayerVersion` call whose `VersionNumber` httpLabel
                    // was elided. Route there so the handler can reject.
                    if req.raw_path.ends_with("/versions/") {
                        Some(("GetLayerVersion", layer))
                    } else {
                        Some(("ListLayerVersions", layer))
                    }
                }
                (&Method::GET, 5, Some("versions"), true) => Some(("GetLayerVersion", version)),
                (&Method::DELETE, 5, Some("versions"), true) => {
                    Some(("DeleteLayerVersion", version))
                }
                // `DELETE .../versions/` (trailing slash, no VersionNumber
                // segment) is a malformed `DeleteLayerVersion` call — route
                // there so the handler can reject the missing path label.
                (&Method::DELETE, 4, Some("versions"), false)
                    if req.raw_path.ends_with("/versions/") =>
                {
                    Some(("DeleteLayerVersion", layer))
                }
                (&Method::GET, 6, Some("versions"), true)
                    if segs.get(5).map(|s| s.as_str()) == Some("policy") =>
                {
                    Some(("GetLayerVersionPolicy", version))
                }
                (&Method::POST, 6, Some("versions"), true)
                    if segs.get(5).map(|s| s.as_str()) == Some("policy") =>
                {
                    Some(("AddLayerVersionPermission", version))
                }
                (&Method::DELETE, 7, Some("versions"), true)
                    if segs.get(5).map(|s| s.as_str()) == Some("policy") =>
                {
                    Some(("RemoveLayerVersionPermission", version))
                }
                _ => None,
            };
        }

        // /2018-10-31/layers-by-arn
        if prefix == "2018-10-31"
            && segs.get(1).map(|s| s.as_str()) == Some("layers-by-arn")
            && req.method == Method::GET
        {
            return Some(("GetLayerVersionByArn", None));
        }

        // NOTE: 2021-10-31/functions/{name}/url and ListFunctionUrlConfigs
        // are handled by the prefix-agnostic blocks above.

        // /2025-11-30/capacity-providers — Lambda Workflows.
        if prefix == "2025-11-30" && segs.get(1).map(|s| s.as_str()) == Some("capacity-providers") {
            let name = segs.get(2).map(|s| s.to_string());
            return match (
                req.method.clone(),
                segs.len(),
                segs.get(3).map(|s| s.as_str()),
            ) {
                (Method::POST, 2, _) => Some(("CreateCapacityProvider", None)),
                (Method::GET, 2, _) => Some(("ListCapacityProviders", None)),
                (Method::GET, 3, _) => Some(("GetCapacityProvider", name)),
                (Method::PUT, 3, _) => Some(("UpdateCapacityProvider", name)),
                (Method::DELETE, 3, _) => Some(("DeleteCapacityProvider", name)),
                (Method::GET, 4, Some("function-versions")) => {
                    Some(("ListFunctionVersionsByCapacityProvider", name))
                }
                _ => None,
            };
        }

        // /2025-12-01/durable-executions and durable-execution-callbacks.
        if prefix == "2025-12-01" {
            let collection = segs.get(1).map(|s| s.as_str());
            let arn = segs.get(2).map(|s| s.to_string());
            let action = segs.get(3).map(|s| s.as_str());
            match (req.method.clone(), collection, segs.len(), action) {
                (Method::GET, Some("durable-executions"), 3, _) => {
                    return Some(("GetDurableExecution", arn));
                }
                (Method::GET, Some("durable-executions"), 4, Some("history")) => {
                    return Some(("GetDurableExecutionHistory", arn));
                }
                (Method::GET, Some("durable-executions"), 4, Some("state")) => {
                    return Some(("GetDurableExecutionState", arn));
                }
                (Method::POST, Some("durable-executions"), 4, Some("checkpoint")) => {
                    return Some(("CheckpointDurableExecution", arn));
                }
                (Method::POST, Some("durable-executions"), 4, Some("stop")) => {
                    return Some(("StopDurableExecution", arn));
                }
                (Method::POST, Some("durable-execution-callbacks"), 4, Some("succeed")) => {
                    return Some(("SendDurableExecutionCallbackSuccess", arn));
                }
                (Method::POST, Some("durable-execution-callbacks"), 4, Some("fail")) => {
                    return Some(("SendDurableExecutionCallbackFailure", arn));
                }
                (Method::POST, Some("durable-execution-callbacks"), 4, Some("heartbeat")) => {
                    return Some(("SendDurableExecutionCallbackHeartbeat", arn));
                }
                (Method::GET, Some("functions"), 4, Some("durable-executions")) => {
                    return Some(("ListDurableExecutionsByFunction", arn));
                }
                _ => {}
            }
        }

        if prefix != "2015-03-31" {
            return None;
        }

        let collection = segs.get(1).map(|s| s.as_str());
        let resource = segs.get(2).map(|s| s.to_string());
        let third = segs.get(3).map(|s| s.as_str());
        let fourth = segs.get(4).map(|s| s.as_str());

        // NOTE: We intentionally do not try to disambiguate URLs that
        // collapsed because an httpLabel was elided (e.g.
        // `/functions/` vs `/functions`). Real AWS treats those as
        // the same path — both hit `ListFunctions` — and we have
        // e2e coverage that relies on the trailing-slash behaviour
        // (`host_routing::unsigned_lambda_list_functions_via_host_header`).
        // Synthetic conformance probes that elide a required path
        // identifier are inherently un-faithful to the SDK; accept
        // the small false-positive count rather than break a real
        // SDK convention.

        let action = match (&req.method, segs.len(), collection, third) {
            (&Method::POST, 2, Some("functions"), _) => "CreateFunction",
            (&Method::GET, 2, Some("functions"), _) => {
                // `GET .../functions` is `ListFunctions`. `GET .../functions/`
                // (trailing slash) signals a `GetFunction` call whose
                // `FunctionName` httpLabel was elided — route there so it
                // can reject the missing identifier.
                if req.raw_path.ends_with("/functions/") {
                    "GetFunction"
                } else {
                    "ListFunctions"
                }
            }
            (&Method::GET, 3, Some("functions"), _) => "GetFunction",
            (&Method::DELETE, 3, Some("functions"), _) => "DeleteFunction",
            (&Method::POST, 4, Some("functions"), Some("invocations")) => "Invoke",
            (&Method::POST, 4, Some("functions"), Some("invoke-async")) => "InvokeAsync",
            (&Method::POST, 4, Some("functions"), Some("response-streaming-invocations")) => {
                "InvokeWithResponseStream"
            }
            (&Method::POST, 4, Some("functions"), Some("versions")) => "PublishVersion",
            (&Method::GET, 4, Some("functions"), Some("versions")) => "ListVersionsByFunction",
            (&Method::POST, 4, Some("functions"), Some("policy")) => "AddPermission",
            (&Method::GET, 4, Some("functions"), Some("policy")) => "GetPolicy",
            (&Method::DELETE, 5, Some("functions"), Some("policy")) => "RemovePermission",
            (&Method::POST, 4, Some("functions"), Some("aliases")) => "CreateAlias",
            (&Method::GET, 4, Some("functions"), Some("aliases")) => {
                // `GET .../aliases` (no trailing slash) is `ListAliases`.
                // `GET .../aliases/` (trailing slash) is `GetAlias` with an
                // empty `Name` — route there so it can reject the missing
                // path label rather than silently degrading to a list.
                if req.raw_path.ends_with("/aliases/") {
                    "GetAlias"
                } else {
                    "ListAliases"
                }
            }
            (&Method::GET, 5, Some("functions"), Some("aliases")) => "GetAlias",
            (&Method::PUT, 5, Some("functions"), Some("aliases")) => "UpdateAlias",
            (&Method::DELETE, 5, Some("functions"), Some("aliases")) => "DeleteAlias",
            (&Method::GET, 4, Some("functions"), Some("configuration")) => {
                "GetFunctionConfiguration"
            }
            (&Method::PUT, 4, Some("functions"), Some("configuration")) => {
                "UpdateFunctionConfiguration"
            }
            (&Method::PUT, 4, Some("functions"), Some("code")) => "UpdateFunctionCode",
            (&Method::PUT, 4, Some("functions"), Some("concurrency")) => "PutFunctionConcurrency",
            (&Method::GET, 4, Some("functions"), Some("concurrency")) => "GetFunctionConcurrency",
            (&Method::DELETE, 4, Some("functions"), Some("concurrency")) => {
                "DeleteFunctionConcurrency"
            }
            (&Method::PUT, 4, Some("functions"), Some("provisioned-concurrency")) => {
                "PutProvisionedConcurrencyConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("provisioned-concurrency")) => {
                "GetProvisionedConcurrencyConfig"
            }
            (&Method::DELETE, 4, Some("functions"), Some("provisioned-concurrency")) => {
                "DeleteProvisionedConcurrencyConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("provisioned-concurrency-configs")) => {
                "ListProvisionedConcurrencyConfigs"
            }
            (&Method::PUT, 4, Some("functions"), Some("event-invoke-config")) => {
                "UpdateFunctionEventInvokeConfig"
            }
            (&Method::POST, 4, Some("functions"), Some("event-invoke-config")) => {
                "PutFunctionEventInvokeConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("event-invoke-config")) => {
                "GetFunctionEventInvokeConfig"
            }
            (&Method::DELETE, 4, Some("functions"), Some("event-invoke-config")) => {
                "DeleteFunctionEventInvokeConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("event-invoke-config-list")) => {
                "ListFunctionEventInvokeConfigs"
            }
            (&Method::PUT, 4, Some("functions"), Some("code-signing-config")) => {
                "PutFunctionCodeSigningConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("code-signing-config")) => {
                "GetFunctionCodeSigningConfig"
            }
            (&Method::DELETE, 4, Some("functions"), Some("code-signing-config")) => {
                "DeleteFunctionCodeSigningConfig"
            }
            (&Method::PUT, 4, Some("functions"), Some("runtime-management-config")) => {
                "PutRuntimeManagementConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("runtime-management-config")) => {
                "GetRuntimeManagementConfig"
            }
            (&Method::PUT, 4, Some("functions"), Some("function-scaling-config")) => {
                "PutFunctionScalingConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("function-scaling-config")) => {
                "GetFunctionScalingConfig"
            }
            (&Method::PUT, 4, Some("functions"), Some("recursion-config")) => {
                "PutFunctionRecursionConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("recursion-config")) => {
                "GetFunctionRecursionConfig"
            }
            (&Method::POST, 2, Some("event-source-mappings"), _) => "CreateEventSourceMapping",
            (&Method::GET, 2, Some("event-source-mappings"), _) => "ListEventSourceMappings",
            (&Method::GET, 3, Some("event-source-mappings"), _) => "GetEventSourceMapping",
            (&Method::PUT, 3, Some("event-source-mappings"), _) => "UpdateEventSourceMapping",
            (&Method::DELETE, 3, Some("event-source-mappings"), _) => "DeleteEventSourceMapping",
            (&Method::POST, 3, Some("tags"), _) => "TagResource",
            (&Method::DELETE, 3, Some("tags"), _) => "UntagResource",
            (&Method::GET, 3, Some("tags"), _) => "ListTags",
            _ => return None,
        };
        let _ = fourth;

        Some((action, resource))
    }

    fn create_function(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let input = CreateFunctionInput::from_body(&body)?;

        // Enforce the Smithy length bounds on `FunctionName` (1..=140
        // characters; AWS accepts the bare name or any ARN form that
        // resolves to <= 140 chars including the ARN prefix). Synthetic
        // negative-conformance variants drive empty / 141-char inputs
        // through this path, so reject up front rather than persisting
        // an invalid record.
        let raw = input.function_name.as_str();
        if raw.is_empty() || raw.chars().count() > 140 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "1 validation error detected: Value '{}' at 'functionName' failed to \
                     satisfy constraint: Member must have length less than or equal to 140",
                    raw
                ),
            ));
        }

        // PassRole trust-policy check: the supplied execution role must
        // have a trust policy that allows lambda.amazonaws.com to call
        // sts:AssumeRole. Real AWS rejects with InvalidParameterValueException
        // when the trust policy doesn't include the service principal.
        if let Some(ref validator) = self.role_trust_validator {
            if let Err(err) =
                validator.validate(&req.account_id, &input.role, "lambda.amazonaws.com")
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    err.to_string(),
                ));
            }
        }

        let mut accounts = self.state.write();
        // Pre-resolve layer attachments before re-borrowing accounts mutably.
        // Layer ARNs may live in sibling accounts.
        let layer_attachments =
            crate::extras::resolve_layer_attachments(&accounts, input.layer_arns.clone());
        let state = accounts.get_or_create(&req.account_id);

        if state.functions.contains_key(&input.function_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceConflictException",
                format!("Function already exist: {}", input.function_name),
            ));
        }

        // Hash the actual ZIP bytes when available, falling back to the
        // raw Code JSON so image-based functions still get a stable id.
        let code_bytes = input.code_zip.as_deref().unwrap_or(&input.code_fallback);
        let mut hasher = Sha256::new();
        hasher.update(code_bytes);
        let hash = hasher.finalize();
        let code_sha256 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hash);
        let code_size = code_bytes.len() as i64;

        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            state.region, state.account_id, input.function_name
        );
        let now = Utc::now();

        let func = LambdaFunction {
            function_name: input.function_name.clone(),
            function_arn,
            runtime: input.runtime,
            role: input.role,
            handler: input.handler,
            description: input.description,
            timeout: input.timeout,
            memory_size: input.memory_size,
            code_sha256,
            code_size,
            version: "$LATEST".to_string(),
            last_modified: now,
            tags: input.tags,
            environment: input.environment,
            architectures: input.architectures,
            package_type: input.package_type,
            code_zip: input.code_zip,
            image_uri: input.image_uri,
            policy: None,
            layers: layer_attachments,
            revision_id: uuid::Uuid::new_v4().to_string(),
            tracing_mode: input.tracing_mode,
            kms_key_arn: input.kms_key_arn,
            ephemeral_storage_size: input.ephemeral_storage_size,
            vpc_config: input.vpc_config,
            snap_start: input.snap_start,
            dead_letter_config_arn: input.dead_letter_config_arn,
            file_system_configs: input.file_system_configs,
            logging_config: input.logging_config,
            image_config: input.image_config,
            durable_config: input.durable_config,
            signing_profile_version_arn: None,
            signing_job_arn: None,
            runtime_version_config: None,
            master_arn: None,
            state_reason: None,
            state_reason_code: None,
            last_update_status_reason: None,
            last_update_status_reason_code: None,
        };

        let response = self.function_config_json(&func);

        state.functions.insert(input.function_name, func);

        Ok(AwsResponse::json(StatusCode::CREATED, response.to_string()))
    }

    fn get_function(
        &self,
        req: &AwsRequest,
        function_name: &str,
        account_id: &str,
        region: &str,
        qualifier: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        if function_name.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "FunctionName is required",
            ));
        }
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, region);
        let state = accounts.get(account_id).unwrap_or(&empty);
        let live = state.functions.get(function_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Function not found: arn:aws:lambda:{}:{}:function:{}",
                    state.region, state.account_id, function_name
                ),
            )
        })?;

        // Resolve the qualifier to either $LATEST (live config) or a
        // numbered immutable snapshot. Aliases route through
        // `resolve_qualifier_to_version` so weighted aliases still pick
        // between the underlying numbered versions.
        let resolved_version = resolve_qualifier_to_version(state, function_name, qualifier);
        let (func, version_label) = match resolved_version {
            None => (live, "$LATEST".to_string()),
            Some(v) => {
                let snap = state
                    .function_version_snapshots
                    .get(function_name)
                    .and_then(|m| m.get(&v))
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "ResourceNotFoundException",
                            format!(
                                "Function not found: arn:aws:lambda:{}:{}:function:{}:{v}",
                                state.region, state.account_id, function_name
                            ),
                        )
                    })?;
                (snap, v)
            }
        };

        let mut config = self.function_config_json(func);
        config["Version"] = json!(version_label);
        if version_label != "$LATEST" {
            config["FunctionArn"] = json!(format!("{}:{version_label}", live.function_arn));
            config["MasterArn"] = json!(live.function_arn);
        }
        let code = if let Some(ref uri) = func.image_uri {
            json!({
                "ImageUri": uri,
                "ResolvedImageUri": uri,
                "RepositoryType": "ECR",
            })
        } else {
            // Serve the function's stored ZIP from a fakecloud-hosted route on
            // the same authority the SDK used, so AWS Toolkit / `aws lambda
            // get-function --query 'Code.Location'` can actually download it.
            json!({
                "Location": crate::extras::function_code_url(
                    req,
                    &state.account_id,
                    function_name,
                    &version_label,
                ),
                "RepositoryType": "S3",
            })
        };
        let response = json!({
            "Code": code,
            "Configuration": config,
            "Tags": live.tags,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_function(
        &self,
        function_name: &str,
        account_id: &str,
        qualifier: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let region = state.region.clone();
        let account_id_owned = state.account_id.clone();

        // Qualifier=N targets a single immutable version snapshot; the
        // live $LATEST function and other versions stay put. AWS only
        // accepts numeric qualifiers here — alias targets are deleted
        // via DeleteAlias, and `$LATEST` is rejected as
        // InvalidParameterValueException.
        if let Some(q) = qualifier {
            if q == "$LATEST" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "$LATEST version cannot be deleted without deleting the function.",
                ));
            }
            if !q.chars().all(|c| c.is_ascii_digit()) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!(
                        "Value '{q}' at 'qualifier' failed to satisfy constraint: Member must satisfy regular expression pattern: (|[a-zA-Z0-9$_-]+)"
                    ),
                ));
            }
            // Live function must exist or AWS 404s before checking the version.
            if !state.functions.contains_key(function_name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{region}:{account_id_owned}:function:{function_name}:{q}"
                    ),
                ));
            }
            let snap_existed = state
                .function_version_snapshots
                .get_mut(function_name)
                .map(|m| m.remove(q).is_some())
                .unwrap_or(false);
            if !snap_existed {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{region}:{account_id_owned}:function:{function_name}:{q}"
                    ),
                ));
            }
            // Drop the version from the ordered list too so
            // ListVersionsByFunction reflects the deletion.
            if let Some(list) = state.function_versions.get_mut(function_name) {
                list.retain(|v| v != q);
            }
            return Ok(AwsResponse::json(StatusCode::NO_CONTENT, ""));
        }

        if state.functions.remove(function_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Function not found: arn:aws:lambda:{region}:{account_id_owned}:function:{function_name}"
                ),
            ));
        }
        // Drop all numbered versions + their snapshots so the function
        // is gone end-to-end (AWS deletes everything when no Qualifier
        // is supplied).
        state.function_versions.remove(function_name);
        state.function_version_snapshots.remove(function_name);
        // Aliases on this function disappear too.
        let prefix = format!("{function_name}:");
        state.aliases.retain(|k, _| !k.starts_with(&prefix));

        // Clean up any running container for this function
        if let Some(ref runtime) = self.runtime {
            let rt = runtime.clone();
            let name = function_name.to_string();
            tokio::spawn(async move { rt.stop_container(&name).await });
        }

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, ""))
    }

    fn list_functions(
        &self,
        account_id: &str,
        function_version: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        // `FunctionVersion` is an enum with the single member `ALL`; reject
        // any other value rather than silently ignoring it.
        if let Some(fv) = function_version {
            if fv != "ALL" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    format!("Invalid FunctionVersion value '{}'; expected 'ALL'", fv),
                ));
            }
        }
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.get(account_id).unwrap_or(&empty);
        let functions: Vec<Value> = state
            .functions
            .values()
            .map(|f| self.function_config_json(f))
            .collect();

        // AWS's documented example carries `NextMarker` as a string even
        // on the final page. We don't paginate yet, so emit an empty
        // string rather than a true sentinel — closer to AWS's
        // observed behavior than omitting the field, and string-typed
        // so strict shape validators don't trip.
        let response = json!({
            "Functions": functions,
            "NextMarker": "",
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    async fn invoke(
        &self,
        function_name: &str,
        payload: &[u8],
        account_id: &str,
        invocation_type: InvocationType,
        qualifier: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Resolve qualifier (alias / numeric version / $LATEST) to a
        // concrete version string. Aliases with a
        // `RoutingConfig.AdditionalVersionWeights` map do a weighted
        // pick across `function_version` + the additional versions,
        // mirroring AWS canary routing. The resolved version is
        // surfaced via `x-amz-executed-version` so callers can verify
        // which version actually ran. We always invoke against the
        // live `$LATEST` function record for now — version-snapshot
        // execution is wired separately once
        // `function_version_snapshots` lands.
        let resolved_version: Option<String> = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, "");
            let state = accounts.get(account_id).unwrap_or(&empty);
            resolve_qualifier_to_version(state, function_name, qualifier)
        };
        let executed_version = resolved_version
            .clone()
            .unwrap_or_else(|| "$LATEST".to_string());
        let (func, layer_zips) = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, "");
            let state = accounts.get(account_id).unwrap_or(&empty);
            // Resolve numbered versions to the immutable snapshot stored
            // by PublishVersion so an alias pinned to v1 runs the v1 code
            // even after $LATEST is mutated. Falls back to $LATEST when
            // the snapshot is missing (legacy state) so we never 404 a
            // routable invoke.
            let func = match resolved_version.as_deref() {
                Some(v) => state
                    .function_version_snapshots
                    .get(function_name)
                    .and_then(|m| m.get(v))
                    .cloned()
                    .or_else(|| state.functions.get(function_name).cloned()),
                None => state.functions.get(function_name).cloned(),
            }
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{}:{}:function:{}",
                        state.region, state.account_id, function_name
                    ),
                )
            })?;
            // Resolve attached layer ARNs to ZIP bytes under the same read
            // lock. Layers may live in sibling accounts (cross-account
            // attach is legal in AWS); fall back to no bytes for unknown
            // ARNs and warn — invoke proceeds without that layer.
            let mut layer_zips: Vec<Vec<u8>> = Vec::with_capacity(func.layers.len());
            for attached in &func.layers {
                let bytes = crate::extras::parse_layer_version_arn(&attached.arn).and_then(
                    |(acct, name, ver)| {
                        accounts
                            .get(&acct)
                            .and_then(|s| s.layers.get(&name))
                            .and_then(|l| l.versions.iter().find(|v| v.version == ver))
                            .and_then(|v| v.code_zip.clone())
                    },
                );
                match bytes {
                    Some(b) => layer_zips.push(b),
                    None => tracing::warn!(
                        function = %function_name,
                        layer_arn = %attached.arn,
                        "attached layer not resolvable; skipping /opt mount for this layer"
                    ),
                }
            }
            (func, layer_zips)
        };

        // Reserved-concurrency gate runs before code-zip / DryRun
        // checks: AWS returns 429 even for functions without a code
        // package as long as the cap is already hit, since throttling
        // is request-rate, not deployment-state.
        let concurrency_key = format!("{account_id}:{function_name}");
        let _concurrency_guard = {
            let cap = {
                let accounts = self.state.read();
                accounts
                    .get(account_id)
                    .and_then(|s| s.function_concurrency.get(function_name).copied())
            };
            let mut map = self.inflight_invocations.write();
            let current = map.get(&concurrency_key).copied().unwrap_or(0);
            if let Some(limit) = cap {
                if current >= limit {
                    // AWS returns the throttle Reason in the body so SDKs
                    // can branch on `ReservedFunctionConcurrentInvocationLimitExceeded`
                    // versus account-pool limits.
                    return Err(AwsServiceError::aws_error_with_fields(
                        StatusCode::TOO_MANY_REQUESTS,
                        "TooManyRequestsException",
                        "Rate Exceeded.",
                        vec![(
                            "Reason".to_string(),
                            "ReservedFunctionConcurrentInvocationLimitExceeded".to_string(),
                        )],
                    ));
                }
            }
            map.insert(concurrency_key.clone(), current + 1);
            ConcurrencyGuard {
                map: self.inflight_invocations.clone(),
                key: concurrency_key.clone(),
            }
        };

        if func.code_zip.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Function has no deployment package",
            ));
        }

        let invoke_start = std::time::Instant::now();
        let dry_run_response = if matches!(invocation_type, InvocationType::DryRun) {
            let mut resp = AwsResponse::json(StatusCode::NO_CONTENT, "");
            if let Ok(v) = http::header::HeaderValue::from_str(&executed_version) {
                resp.headers.insert(
                    http::header::HeaderName::from_static("x-amz-executed-version"),
                    v,
                );
            }
            Some(resp)
        } else {
            None
        };

        let runtime_for_invoke = if dry_run_response.is_some() {
            None
        } else {
            self.runtime.clone()
        };

        let result: Result<AwsResponse, AwsServiceError> = if let Some(resp) = dry_run_response {
            Ok(resp)
        } else if let Some(runtime) = runtime_for_invoke {
            match invocation_type {
                InvocationType::Event => {
                    // Fire-and-forget. AWS returns 202 with no body. Move
                    // the concurrency guard into the spawned task so the
                    // counter only drops once the async invocation
                    // actually finishes.
                    let runtime = runtime.clone();
                    let func_clone = func.clone();
                    let payload_vec = payload.to_vec();
                    let bus = self.delivery_bus.clone();
                    let destination_config = self.lookup_destination_config(&func, account_id);
                    let function_arn = func.function_arn.clone();
                    let layer_zips_async = layer_zips.clone();
                    let async_guard = _concurrency_guard;
                    tokio::spawn(async move {
                        let _g = async_guard;
                        let result = match runtime
                            .invoke(&func_clone, &payload_vec, &layer_zips_async)
                            .await
                        {
                            Ok(bytes) => {
                                // Lambda runtime returns 200 even on uncaught
                                // function errors; the body has errorMessage /
                                // errorType. Treat that as failure for routing.
                                let parsed: Option<serde_json::Value> =
                                    serde_json::from_slice(&bytes).ok();
                                let is_error = parsed
                                    .as_ref()
                                    .and_then(|v| v.as_object())
                                    .map(|m| {
                                        m.contains_key("errorMessage")
                                            || m.contains_key("errorType")
                                    })
                                    .unwrap_or(false);
                                if is_error {
                                    let msg = parsed
                                        .as_ref()
                                        .and_then(|v| v.get("errorMessage"))
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("function error")
                                        .to_string();
                                    Err(msg)
                                } else {
                                    Ok(bytes)
                                }
                            }
                            Err(e) => Err(e.to_string()),
                        };
                        if let Some(bus) = bus {
                            route_to_destination(
                                bus,
                                &function_arn,
                                &payload_vec,
                                &result,
                                destination_config.as_ref(),
                            );
                        }
                    });
                    let mut resp = AwsResponse::json(StatusCode::ACCEPTED, "");
                    if let Ok(v) = http::header::HeaderValue::from_str(&executed_version) {
                        resp.headers.insert(
                            http::header::HeaderName::from_static("x-amz-executed-version"),
                            v,
                        );
                    }
                    Ok(resp)
                }
                InvocationType::RequestResponse | InvocationType::DryRun => {
                    match runtime.invoke(&func, payload, &layer_zips).await {
                        Ok(response_bytes) => {
                            let mut resp = AwsResponse::json(StatusCode::OK, response_bytes);
                            if let Ok(v) = http::header::HeaderValue::from_str(&executed_version) {
                                resp.headers.insert(
                                    http::header::HeaderName::from_static("x-amz-executed-version"),
                                    v,
                                );
                            }
                            Ok(resp)
                        }
                        Err(e) => {
                            tracing::error!(function = %function_name, error = %e, "Lambda invocation failed");
                            Err(AwsServiceError::aws_error(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "ServiceException",
                                format!("Lambda execution failed: {e}"),
                            ))
                        }
                    }
                }
            }
        } else {
            Err(AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServiceException",
                "Docker/Podman is required for Lambda execution but is not available",
            ))
        };

        // Publish standard AWS/Lambda metrics: Invocations always, Errors
        // when the runtime returned a failure, Duration in milliseconds.
        // Mirrors what real Lambda emits and lets fakecloud users wire
        // CloudWatch alarms on Lambda errors / latency in tests.
        if let Some(bus) = &self.delivery_bus {
            let dims: std::collections::BTreeMap<String, String> =
                [("FunctionName".to_string(), function_name.to_string())]
                    .into_iter()
                    .collect();
            let now_ms = chrono::Utc::now().timestamp_millis();
            let region = {
                let accounts = self.state.read();
                let empty = LambdaState::new(account_id, "");
                accounts
                    .get(account_id)
                    .map(|s| s.region.clone())
                    .unwrap_or_else(|| empty.region)
            };
            bus.put_cloudwatch_metric(
                account_id,
                &region,
                "AWS/Lambda",
                "Invocations",
                1.0,
                Some("Count"),
                dims.clone(),
                now_ms,
            );
            bus.put_cloudwatch_metric(
                account_id,
                &region,
                "AWS/Lambda",
                "Duration",
                invoke_start.elapsed().as_millis() as f64,
                Some("Milliseconds"),
                dims.clone(),
                now_ms,
            );
            if result.is_err() {
                bus.put_cloudwatch_metric(
                    account_id,
                    &region,
                    "AWS/Lambda",
                    "Errors",
                    1.0,
                    Some("Count"),
                    dims,
                    now_ms,
                );
            }
        }

        result
    }

    /// Pull EventInvokeConfig.DestinationConfig for the function. The
    /// stored key is `<function_name>:<qualifier>`; treat unqualified
    /// invokes as the empty qualifier (matches `parse_qualifier` in
    /// `extras.rs` when no `Qualifier` is supplied).
    fn lookup_destination_config(
        &self,
        func: &crate::state::LambdaFunction,
        account_id: &str,
    ) -> Option<serde_json::Value> {
        let accounts = self.state.read();
        let state = accounts.get(account_id)?;
        let key = format!("{}:$LATEST", func.function_name);
        state
            .event_invoke_configs
            .get(&key)
            .and_then(|cfg| cfg.destination_config.clone())
            .filter(|v| !v.is_null() && !v.as_object().map(|o| o.is_empty()).unwrap_or(false))
    }

    pub(crate) fn publish_version(
        &self,
        function_name: &str,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Optional preconditions from the body. Both compare the supplied
        // value against the live `$LATEST` state; mismatch yields 412
        // PreconditionFailedException, matching AWS optimistic-concurrency.
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let supplied_revision = body["RevisionId"].as_str().map(String::from);
        let supplied_sha = body["CodeSha256"].as_str().map(String::from);
        let description_override = body["Description"].as_str().map(String::from);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let func = state.functions.get(function_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Function not found: arn:aws:lambda:{}:{}:function:{}",
                    state.region, state.account_id, function_name
                ),
            )
        })?;

        if let Some(ref rev) = supplied_revision {
            if rev != &func.revision_id {
                return Err(AwsServiceError::aws_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailedException",
                    "The RevisionId provided does not match the latest RevisionId for the Lambda function. Call the GetFunction or the GetAlias API to retrieve the latest RevisionId for your resource.",
                ));
            }
        }
        if let Some(ref sha) = supplied_sha {
            if sha != &func.code_sha256 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailedException",
                    "CodeSha256 does not match the SHA-256 of the function's deployment package.",
                ));
            }
        }

        // Pick the next version number per function, monotonic per
        // function arn, never reused. AWS uses sequential decimal
        // strings starting at 1.
        let existing = state
            .function_versions
            .get(function_name)
            .cloned()
            .unwrap_or_default();
        let latest_version = existing.iter().filter_map(|v| v.parse::<u64>().ok()).max();

        // PublishVersion is idempotent on AWS: if `$LATEST` hasn't
        // changed since the most recent published version, return that
        // existing snapshot instead of bumping the counter. We compare
        // every field that PublishVersion would otherwise carry into
        // the new snapshot — code identity, description, runtime,
        // handler, role, env, memory/timeout, layers, image config,
        // VPC, EFS, logging, tracing, kms, ephemeral storage — so a
        // config-only change (e.g. UpdateFunctionConfiguration bumping
        // memory) still produces a fresh version. This keeps deploy
        // pipelines that re-publish on every CI run from leaking a
        // fresh numbered version per build when the underlying
        // artifact + config are identical.
        if let Some(latest_num) = latest_version {
            let latest_str = latest_num.to_string();
            if let Some(prev_snap) = state
                .function_version_snapshots
                .get(function_name)
                .and_then(|m| m.get(&latest_str))
                .cloned()
            {
                let effective_desc = description_override
                    .clone()
                    .unwrap_or_else(|| func.description.clone());
                if function_config_unchanged_for_publish(&prev_snap, func, &effective_desc) {
                    let mut config = self.function_config_json(&prev_snap);
                    config["Version"] = json!(latest_str);
                    config["FunctionArn"] = json!(format!("{}:{latest_str}", func.function_arn));
                    config["MasterArn"] = json!(func.function_arn);
                    return Ok(AwsResponse::json(StatusCode::CREATED, config.to_string()));
                }
            }
        }

        let next: u64 = latest_version.unwrap_or(0) + 1;
        let next_str = next.to_string();

        // Snapshot the function config + code for the new immutable version.
        let mut snapshot = func.clone();
        snapshot.version = next_str.clone();
        snapshot.master_arn = Some(func.function_arn.clone());
        if let Some(desc) = description_override {
            snapshot.description = desc;
        }
        // Each numbered version gets its own RevisionId, decoupled from $LATEST.
        snapshot.revision_id = uuid::Uuid::new_v4().to_string();

        // SnapStart optimization completes asynchronously on real Lambda
        // when ApplyOn=PublishedVersions. fakecloud has no actual
        // optimization step, so we flip OptimizationStatus to "On"
        // eagerly on the published-version snapshot so clients that
        // wait on this transition see the steady state immediately.
        if let Some(snap) = snapshot.snap_start.as_mut() {
            if snap.get("ApplyOn").and_then(|v| v.as_str()) == Some("PublishedVersions") {
                snap["OptimizationStatus"] = json!("On");
            }
        }

        // Append to numbered list and store the snapshot.
        state
            .function_versions
            .entry(function_name.to_string())
            .or_default()
            .push(next_str.clone());
        state
            .function_version_snapshots
            .entry(function_name.to_string())
            .or_default()
            .insert(next_str.clone(), snapshot.clone());

        let mut config = self.function_config_json(&snapshot);
        config["Version"] = json!(next_str);
        config["FunctionArn"] = json!(format!("{}:{next_str}", func.function_arn));
        config["MasterArn"] = json!(func.function_arn);

        Ok(AwsResponse::json(StatusCode::CREATED, config.to_string()))
    }

    pub(crate) fn function_config_json(&self, func: &LambdaFunction) -> Value {
        // AWS always emits Environment with at least an empty Variables map.
        let env_vars = if func.environment.is_empty() {
            json!({ "Variables": {} })
        } else {
            json!({ "Variables": func.environment })
        };

        let tracing_mode = func.tracing_mode.as_deref().unwrap_or("PassThrough");
        let ephemeral_size = func.ephemeral_storage_size.unwrap_or(512);

        let mut config = json!({
            "FunctionName": func.function_name,
            "FunctionArn": func.function_arn,
            "Runtime": func.runtime,
            "Role": func.role,
            "Handler": func.handler,
            "Description": func.description,
            "Timeout": func.timeout,
            "MemorySize": func.memory_size,
            "CodeSha256": func.code_sha256,
            "CodeSize": func.code_size,
            "Version": func.version,
            "LastModified": func.last_modified.format("%Y-%m-%dT%H:%M:%S%.3f+0000").to_string(),
            "PackageType": func.package_type,
            "Architectures": func.architectures,
            "Environment": env_vars,
            "State": "Active",
            "LastUpdateStatus": "Successful",
            "TracingConfig": { "Mode": tracing_mode },
            "RevisionId": func.revision_id,
            "EphemeralStorage": { "Size": ephemeral_size },
            "SnapStart": func.snap_start.clone().unwrap_or_else(|| json!({
                "ApplyOn": "None",
                "OptimizationStatus": "Off",
            })),
        });
        if let Some(ref kms) = func.kms_key_arn {
            config["KMSKeyArn"] = json!(kms);
        }
        if let Some(ref vpc) = func.vpc_config {
            config["VpcConfig"] = vpc.clone();
        }
        if let Some(ref dlq) = func.dead_letter_config_arn {
            config["DeadLetterConfig"] = json!({ "TargetArn": dlq });
        }
        if !func.file_system_configs.is_empty() {
            config["FileSystemConfigs"] = json!(func.file_system_configs);
        }
        if let Some(ref lg) = func.logging_config {
            config["LoggingConfig"] = lg.clone();
        }
        if let Some(ref ic) = func.image_config {
            config["ImageConfigResponse"] = json!({ "ImageConfig": ic });
        }
        if let Some(ref dc) = func.durable_config {
            config["DurableConfig"] = dc.clone();
        }
        if let Some(ref s) = func.signing_profile_version_arn {
            config["SigningProfileVersionArn"] = json!(s);
        }
        if let Some(ref s) = func.signing_job_arn {
            config["SigningJobArn"] = json!(s);
        }
        if let Some(ref rv) = func.runtime_version_config {
            config["RuntimeVersionConfig"] = rv.clone();
        }
        if let Some(ref m) = func.master_arn {
            config["MasterArn"] = json!(m);
        }
        // AWS's `FunctionConfiguration` shape has no `Code` member —
        // `FunctionCodeLocation` only appears on `GetFunction`'s response
        // wrapper. Image-based functions surface their URI via the
        // wrapper at `Code.ImageUri`, set by `get_function`.
        if !func.layers.is_empty() {
            config["Layers"] = json!(func
                .layers
                .iter()
                .map(|l| json!({"Arn": l.arn, "CodeSize": l.code_size}))
                .collect::<Vec<_>>());
        }
        if let Some(ref r) = func.state_reason {
            config["StateReason"] = json!(r);
        }
        if let Some(ref c) = func.state_reason_code {
            config["StateReasonCode"] = json!(c);
        }
        if let Some(ref r) = func.last_update_status_reason {
            config["LastUpdateStatusReason"] = json!(r);
        }
        if let Some(ref c) = func.last_update_status_reason_code {
            config["LastUpdateStatusReasonCode"] = json!(c);
        }
        config
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
        let resource_name = if action_takes_function_name(action) {
            // Enforce the Smithy length bound (`FunctionName.length 1..140`)
            // before normalization. Synthetic conformance variants drive
            // 141-character strings through these paths; without an early
            // reject we'd happily serve `GetFunction` against a name that
            // could never have been created. The 170-char ceiling tracks
            // the documented ARN-form upper bound.
            if let Some(raw) = resource_name.as_ref() {
                let len = raw.chars().count();
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
                let limit = if raw.starts_with("arn:") { 200 } else { 140 };
                if raw.is_empty() || len > limit {
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
            if let Ok(n) = raw.parse::<i64>() {
                let max = match action {
                    "ListLayers"
                    | "ListLayerVersions"
                    | "ListFunctionUrlConfigs"
                    | "ListProvisionedConcurrencyConfigs"
                    | "ListFunctionEventInvokeConfigs"
                    | "ListAliases" => 50,
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
                let qualifier = req.query_params.get("Qualifier").map(String::as_str);
                self.invoke(
                    resource_name.as_deref().unwrap_or(""),
                    &req.body,
                    aid,
                    invocation_type,
                    qualifier,
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
                self.list_event_source_mappings(aid)
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
        let action: &'static str = match action_str {
            "CreateFunction" => "CreateFunction",
            "ListFunctions" => "ListFunctions",
            "GetFunction" => "GetFunction",
            "DeleteFunction" => "DeleteFunction",
            "Invoke" => "InvokeFunction",
            "InvokeWithResponseStream" => "InvokeFunctionWithResponseStream",
            "PublishVersion" => "PublishVersion",
            "AddPermission" => "AddPermission",
            "RemovePermission" => "RemovePermission",
            "GetPolicy" => "GetPolicy",
            "CreateEventSourceMapping" => "CreateEventSourceMapping",
            "ListEventSourceMappings" => "ListEventSourceMappings",
            "GetEventSourceMapping" => "GetEventSourceMapping",
            "DeleteEventSourceMapping" => "DeleteEventSourceMapping",
            _ => return None,
        };
        let accounts = self.state.read();
        let empty = LambdaState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let resource = match action {
            "GetFunction"
            | "DeleteFunction"
            | "InvokeFunction"
            | "InvokeFunctionWithResponseStream"
            | "PublishVersion"
            | "AddPermission"
            | "RemovePermission"
            | "GetPolicy" => {
                let name = resource_name.unwrap_or_default();
                if name.is_empty() {
                    "*".to_string()
                } else {
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

#[path = "service_event_sources.rs"]
mod service_event_sources;
#[path = "service_permissions.rs"]
mod service_permissions;

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;
