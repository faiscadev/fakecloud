use std::collections::HashMap;
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
    tags: HashMap<String, String>,
    environment: HashMap<String, String>,
    architectures: Vec<String>,
    code_zip: Option<Vec<u8>>,
    code_fallback: Vec<u8>,
    image_uri: Option<String>,
}

impl CreateFunctionInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
        let raw_function_name = body["FunctionName"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "FunctionName is required",
            )
        })?;
        let function_name = resolve_function_name(raw_function_name);

        let tags: HashMap<String, String> = body["Tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let environment: HashMap<String, String> = body["Environment"]["Variables"]
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
        })
    }
}

/// Resolve a Lambda `FunctionName` parameter to the bare function name
/// used as the `state.functions` map key.
///
/// AWS Lambda accepts four equivalent forms wherever `FunctionName` is
/// taken:
///   - plain name: `my-fn`
///   - plain name with qualifier: `my-fn:PROD` or `my-fn:7`
///   - full ARN: `arn:aws:lambda:REGION:ACCOUNT:function:my-fn[:QUALIFIER]`
///   - partial ARN: `ACCOUNT:function:my-fn[:QUALIFIER]`
///
/// AWS SDKs URL-encode the colons in ARN-form path components
/// (`arn%3Aaws%3Alambda%3A...`), so the input is percent-decoded first.
///
/// Internally fakecloud only ever stores the unqualified name, so each
/// caller must collapse these forms before doing a map lookup. Inputs
/// that don't match any known shape pass through unchanged so
/// downstream `not found` errors keep their original identifier.
pub(crate) fn resolve_function_name(input: &str) -> String {
    let decoded: std::borrow::Cow<'_, str> = if input.contains('%') {
        percent_encoding::percent_decode_str(input)
            .decode_utf8()
            .unwrap_or(std::borrow::Cow::Borrowed(input))
    } else {
        std::borrow::Cow::Borrowed(input)
    };
    let s: &str = decoded.as_ref();

    // Full ARN: arn:aws:lambda:REGION:ACCOUNT:function:NAME[:QUALIFIER]
    if let Some(rest) = s.strip_prefix("arn:aws:lambda:") {
        let parts: Vec<&str> = rest.splitn(5, ':').collect();
        if parts.len() >= 4
            && !parts[0].is_empty()
            && !parts[1].is_empty()
            && parts[2] == "function"
            && !parts[3].is_empty()
        {
            return parts[3].to_string();
        }
        return s.to_string();
    }
    // Partial ARN: ACCOUNT:function:NAME[:QUALIFIER] (account is 12 digits).
    let head: Vec<&str> = s.splitn(4, ':').collect();
    if head.len() >= 3
        && head[0].len() == 12
        && head[0].bytes().all(|b| b.is_ascii_digit())
        && head[1] == "function"
        && !head[2].is_empty()
    {
        return head[2].to_string();
    }
    // Plain name with optional qualifier. AWS function names are
    // `[a-zA-Z0-9-_]+`, so any colon means a qualifier suffix.
    if let Some((name, _qualifier)) = s.split_once(':') {
        if !name.is_empty() {
            return name.to_string();
        }
    }
    s.to_string()
}

/// True when the action's path-derived resource is a Lambda
/// `FunctionName` (versus a layer name, code-signing-config id, event-
/// source-mapping uuid, capacity-provider name, durable-execution id,
/// or a tag-resource ARN). Drives whether `handle()` collapses ARN /
/// partial-ARN / qualified forms to the bare function name.
fn is_function_name_action(action: &str) -> bool {
    matches!(
        action,
        "GetFunction"
            | "DeleteFunction"
            | "Invoke"
            | "InvokeAsync"
            | "InvokeWithResponseStream"
            | "PublishVersion"
            | "AddPermission"
            | "GetPolicy"
            | "RemovePermission"
            | "GetFunctionConfiguration"
            | "UpdateFunctionConfiguration"
            | "UpdateFunctionCode"
            | "ListVersionsByFunction"
            | "CreateAlias"
            | "GetAlias"
            | "ListAliases"
            | "UpdateAlias"
            | "DeleteAlias"
            | "CreateFunctionUrlConfig"
            | "GetFunctionUrlConfig"
            | "UpdateFunctionUrlConfig"
            | "DeleteFunctionUrlConfig"
            | "PutFunctionConcurrency"
            | "GetFunctionConcurrency"
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
            | "PutRuntimeManagementConfig"
            | "GetRuntimeManagementConfig"
            | "PutFunctionScalingConfig"
            | "GetFunctionScalingConfig"
            | "PutFunctionRecursionConfig"
            | "GetFunctionRecursionConfig"
            | "PutFunctionCodeSigningConfig"
            | "GetFunctionCodeSigningConfig"
            | "DeleteFunctionCodeSigningConfig"
    )
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

pub struct LambdaService {
    pub(crate) state: SharedLambdaState,
    runtime: Option<Arc<ContainerRuntime>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
    pub(crate) delivery_bus: Option<Arc<fakecloud_core::delivery::DeliveryBus>>,
    pub(crate) role_trust_validator: Option<Arc<dyn fakecloud_core::auth::RoleTrustValidator>>,
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
        }
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

        // Provisioned concurrency at any prefix.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("provisioned-concurrency")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutProvisionedConcurrencyConfig", res)),
                Method::GET => Some(("GetProvisionedConcurrencyConfig", res)),
                Method::DELETE => Some(("DeleteProvisionedConcurrencyConfig", res)),
                _ => None,
            };
        }
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
        if segs.get(1).map(|s| s.as_str()) == Some("event-source-mappings")
            && segs.get(3).map(|s| s.as_str()) == Some("scaling-config")
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match req.method {
                Method::PUT => Some(("PutFunctionScalingConfig", res)),
                Method::GET => Some(("GetFunctionScalingConfig", res)),
                _ => None,
            };
        }

        // Capacity providers (any prefix).
        if segs.get(1).map(|s| s.as_str()) == Some("capacity-providers") {
            let res = segs.get(2).map(|s| s.to_string());
            return match (
                req.method.clone(),
                segs.len(),
                segs.get(3).map(|s| s.as_str()),
            ) {
                (Method::POST, 2, _) => Some(("CreateCapacityProvider", None)),
                (Method::GET, 2, _) => Some(("ListCapacityProviders", None)),
                (Method::GET, 3, _) => Some(("GetCapacityProvider", res)),
                (Method::PUT, 3, _) => Some(("UpdateCapacityProvider", res)),
                (Method::DELETE, 3, _) => Some(("DeleteCapacityProvider", res)),
                (Method::GET, 4, Some("function-versions")) => {
                    Some(("ListFunctionVersionsByCapacityProvider", res))
                }
                _ => None,
            };
        }

        // ListDurableExecutionsByFunction lives under functions/{name}.
        if segs.get(1).map(|s| s.as_str()) == Some("functions")
            && segs.get(3).map(|s| s.as_str()) == Some("durable-executions")
            && req.method == Method::GET
        {
            return Some((
                "ListDurableExecutionsByFunction",
                segs.get(2).map(|s| s.to_string()),
            ));
        }

        // Durable execution callbacks at /durable-execution-callbacks/{id}/{kind}
        if segs.get(1).map(|s| s.as_str()) == Some("durable-execution-callbacks")
            && req.method == Method::POST
        {
            let res = segs.get(2).map(|s| s.to_string());
            return match segs.get(3).map(|s| s.as_str()) {
                Some("success") | Some("succeed") => {
                    Some(("SendDurableExecutionCallbackSuccess", res))
                }
                Some("failure") | Some("fail") => {
                    Some(("SendDurableExecutionCallbackFailure", res))
                }
                Some("heartbeat") => Some(("SendDurableExecutionCallbackHeartbeat", res)),
                _ => None,
            };
        }

        // Durable executions (any prefix).
        if segs.get(1).map(|s| s.as_str()) == Some("durable-executions") {
            let res = segs.get(2).map(|s| s.to_string());
            return match (
                req.method.clone(),
                segs.len(),
                segs.get(3).map(|s| s.as_str()),
                segs.get(4).map(|s| s.as_str()),
            ) {
                (Method::GET, 3, _, _) => Some(("GetDurableExecution", res)),
                (Method::GET, 4, Some("history"), _) => Some(("GetDurableExecutionHistory", res)),
                (Method::GET, 4, Some("state"), _) => Some(("GetDurableExecutionState", res)),
                (Method::POST, 4, Some("checkpoint"), _) => {
                    Some(("CheckpointDurableExecution", res))
                }
                (Method::POST, 4, Some("stop"), _) => Some(("StopDurableExecution", res)),
                (Method::POST, 5, Some("callback"), Some("success")) => {
                    Some(("SendDurableExecutionCallbackSuccess", res))
                }
                (Method::POST, 5, Some("callback"), Some("failure")) => {
                    Some(("SendDurableExecutionCallbackFailure", res))
                }
                (Method::POST, 5, Some("callback"), Some("heartbeat")) => {
                    Some(("SendDurableExecutionCallbackHeartbeat", res))
                }
                _ => None,
            };
        }

        // NOTE: concurrency, event-invoke-config, recursion-config,
        // capacity-providers, durable-executions, and code-signing-configs
        // routes are all handled by the prefix-agnostic blocks above.
        // The previously-present date-specific blocks were dead code.

        // /2018-10-31/layers
        if prefix == "2018-10-31" && segs.get(1).map(|s| s.as_str()) == Some("layers") {
            let layer = segs.get(2).map(|s| s.to_string());
            let third = segs.get(3).map(|s| s.as_str());
            let version = segs.get(4).map(|s| s.to_string());
            return match (&req.method, segs.len(), third, version.is_some()) {
                (&Method::GET, 2, _, _) => Some(("ListLayers", None)),
                (&Method::POST, 4, Some("versions"), false) => Some(("PublishLayerVersion", layer)),
                (&Method::GET, 4, Some("versions"), false) => Some(("ListLayerVersions", layer)),
                (&Method::GET, 5, Some("versions"), true) => Some(("GetLayerVersion", version)),
                (&Method::DELETE, 5, Some("versions"), true) => {
                    Some(("DeleteLayerVersion", version))
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

        if prefix != "2015-03-31" {
            return None;
        }

        let collection = segs.get(1).map(|s| s.as_str());
        let resource = segs.get(2).map(|s| s.to_string());
        let third = segs.get(3).map(|s| s.as_str());
        let fourth = segs.get(4).map(|s| s.as_str());

        let action = match (&req.method, segs.len(), collection, third) {
            (&Method::POST, 2, Some("functions"), _) => "CreateFunction",
            (&Method::GET, 2, Some("functions"), _) => "ListFunctions",
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
            (&Method::GET, 4, Some("functions"), Some("aliases")) => "ListAliases",
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
            (&Method::PUT, 4, Some("functions"), Some("scaling-config")) => {
                "PutFunctionScalingConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("scaling-config")) => {
                "GetFunctionScalingConfig"
            }
            (&Method::PUT, 4, Some("functions"), Some("recursion-config")) => {
                "PutFunctionRecursionConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("recursion-config")) => {
                "GetFunctionRecursionConfig"
            }
            (&Method::GET, 4, Some("functions"), Some("durable-executions")) => {
                "ListDurableExecutionsByFunction"
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
        };

        let response = self.function_config_json(&func);

        state.functions.insert(input.function_name, func);

        Ok(AwsResponse::json(StatusCode::CREATED, response.to_string()))
    }

    fn get_function(
        &self,
        function_name: &str,
        account_id: &str,
        region: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, region);
        let state = accounts.get(account_id).unwrap_or(&empty);
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

        let config = self.function_config_json(func);
        let code = if let Some(ref uri) = func.image_uri {
            json!({
                "ImageUri": uri,
                "ResolvedImageUri": uri,
                "RepositoryType": "ECR",
            })
        } else {
            json!({
                "Location": format!(
                    "https://awslambda-{}-tasks.s3.{}.amazonaws.com/stub",
                    func.function_arn.split(':').nth(3).unwrap_or("us-east-1"),
                    func.function_arn.split(':').nth(3).unwrap_or("us-east-1")
                ),
                "RepositoryType": "S3",
            })
        };
        let response = json!({
            "Code": code,
            "Configuration": config,
            "Tags": func.tags,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_function(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let region = state.region.clone();
        let account_id = state.account_id.clone();
        if state.functions.remove(function_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Function not found: arn:aws:lambda:{}:{}:function:{}",
                    region, account_id, function_name
                ),
            ));
        }

        // Clean up any running container for this function
        if let Some(ref runtime) = self.runtime {
            let rt = runtime.clone();
            let name = function_name.to_string();
            tokio::spawn(async move { rt.stop_container(&name).await });
        }

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, ""))
    }

    fn list_functions(&self, account_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.get(account_id).unwrap_or(&empty);
        let functions: Vec<Value> = state
            .functions
            .values()
            .map(|f| self.function_config_json(f))
            .collect();

        let response = json!({
            "Functions": functions,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    async fn invoke(
        &self,
        function_name: &str,
        payload: &[u8],
        account_id: &str,
        invocation_type: InvocationType,
    ) -> Result<AwsResponse, AwsServiceError> {
        let func = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, "");
            let state = accounts.get(account_id).unwrap_or(&empty);
            state.functions.get(function_name).cloned().ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{}:{}:function:{}",
                        state.region, state.account_id, function_name
                    ),
                )
            })?
        };

        if func.code_zip.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Function has no deployment package",
            ));
        }

        if matches!(invocation_type, InvocationType::DryRun) {
            let mut resp = AwsResponse::json(StatusCode::NO_CONTENT, "");
            resp.headers.insert(
                http::header::HeaderName::from_static("x-amz-executed-version"),
                http::header::HeaderValue::from_static("$LATEST"),
            );
            return Ok(resp);
        }

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServiceException",
                "Docker/Podman is required for Lambda execution but is not available",
            )
        })?;

        match invocation_type {
            InvocationType::Event => {
                // Fire-and-forget. AWS returns 202 with no body.
                let runtime = runtime.clone();
                let func_clone = func.clone();
                let payload_vec = payload.to_vec();
                let bus = self.delivery_bus.clone();
                let destination_config = self.lookup_destination_config(&func, account_id);
                let function_arn = func.function_arn.clone();
                tokio::spawn(async move {
                    let result = match runtime.invoke(&func_clone, &payload_vec).await {
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
                                    m.contains_key("errorMessage") || m.contains_key("errorType")
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
                resp.headers.insert(
                    http::header::HeaderName::from_static("x-amz-executed-version"),
                    http::header::HeaderValue::from_static("$LATEST"),
                );
                Ok(resp)
            }
            InvocationType::RequestResponse | InvocationType::DryRun => {
                match runtime.invoke(&func, payload).await {
                    Ok(response_bytes) => {
                        let mut resp = AwsResponse::json(StatusCode::OK, response_bytes);
                        resp.headers.insert(
                            http::header::HeaderName::from_static("x-amz-executed-version"),
                            http::header::HeaderValue::from_static("$LATEST"),
                        );
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
            .map(|cfg| cfg.destination_config.clone())
            .filter(|v| !v.is_null() && !v.as_object().map(|o| o.is_empty()).unwrap_or(false))
    }

    fn publish_version(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.get(account_id).unwrap_or(&empty);
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

        let mut config = self.function_config_json(func);
        // Stub: always return version "1"
        config["Version"] = json!("1");
        config["FunctionArn"] = json!(format!("{}:1", func.function_arn));

        Ok(AwsResponse::json(StatusCode::CREATED, config.to_string()))
    }

    fn create_event_source_mapping(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "EventSourceArn is required",
                )
            })?
            .to_string();

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Resolve to a canonical function ARN. Accept name, qualified
        // name, partial ARN, or full ARN — collapse to the bare name,
        // then look up the stored ARN. Mirrors AWS's flexibility.
        let bare_name = resolve_function_name(&function_name);
        let function_arn = {
            let func = state.functions.get(&bare_name).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{}:{}:function:{}",
                        state.region, state.account_id, bare_name
                    ),
                )
            })?;
            func.function_arn.clone()
        };

        let batch_size = body["BatchSize"].as_i64().unwrap_or(10);
        let enabled = body["Enabled"].as_bool().unwrap_or(true);
        let mapping_uuid = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        // Extract Filters[].Pattern strictly: any entry where
        // `Pattern` is missing or not a string is a hard error,
        // matching AWS. Doing this before `validate` keeps malformed
        // values from being silently dropped by the lossy serializer.
        // FilterCriteria itself must be an object (or absent) — non-
        // object values would otherwise be silently dropped by
        // `Value::get`, masking client bugs.
        let filter_patterns: Vec<String> = match body.get("FilterCriteria") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Object(_)) => {
                match body.get("FilterCriteria").and_then(|v| v.get("Filters")) {
                    None => Vec::new(),
                    Some(Value::Array(arr)) => {
                        let mut out = Vec::with_capacity(arr.len());
                        for f in arr {
                            match f.get("Pattern") {
                                Some(Value::String(s)) => out.push(s.clone()),
                                _ => {
                                    return Err(AwsServiceError::aws_error(
                                        StatusCode::BAD_REQUEST,
                                        "InvalidParameterValueException",
                                        "FilterCriteria.Filters[].Pattern must be a string",
                                    ));
                                }
                            }
                        }
                        out
                    }
                    Some(_) => {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValueException",
                            "FilterCriteria.Filters must be an array",
                        ));
                    }
                }
            }
            Some(_) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "FilterCriteria must be an object",
                ));
            }
        };
        // AWS rejects malformed FilterCriteria at create time.
        if let Err(err) = crate::filter::FilterSet::validate(filter_patterns.iter()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                err,
            ));
        }
        let function_response_types: Vec<String> = body
            .get("FunctionResponseTypes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let starting_position = body
            .get("StartingPosition")
            .and_then(|v| v.as_str())
            .map(String::from);
        let starting_position_timestamp = body
            .get("StartingPositionTimestamp")
            .and_then(|v| v.as_f64());
        let parallelization_factor = body.get("ParallelizationFactor").and_then(|v| v.as_i64());
        let maximum_batching_window_in_seconds = body
            .get("MaximumBatchingWindowInSeconds")
            .and_then(|v| v.as_i64());

        let mapping = EventSourceMapping {
            uuid: mapping_uuid.clone(),
            function_arn: function_arn.clone(),
            event_source_arn: event_source_arn.clone(),
            batch_size,
            enabled,
            state: if enabled {
                "Enabled".to_string()
            } else {
                "Disabled".to_string()
            },
            last_modified: now,
            filter_patterns,
            maximum_batching_window_in_seconds,
            starting_position,
            starting_position_timestamp,
            parallelization_factor,
            function_response_types,
        };

        let response = self.event_source_mapping_json(&mapping);
        state.event_source_mappings.insert(mapping_uuid, mapping);

        Ok(AwsResponse::json(
            StatusCode::ACCEPTED,
            response.to_string(),
        ))
    }

    fn list_event_source_mappings(&self, account_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.get(account_id).unwrap_or(&empty);
        let mappings: Vec<Value> = state
            .event_source_mappings
            .values()
            .map(|m| self.event_source_mapping_json(m))
            .collect();

        let response = json!({
            "EventSourceMappings": mappings,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn get_event_source_mapping(
        &self,
        uuid: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.get(account_id).unwrap_or(&empty);
        let mapping = state.event_source_mappings.get(uuid).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("The resource you requested does not exist. (Service: Lambda, Status Code: 404, Request ID: {uuid})"),
            )
        })?;

        let response = self.event_source_mapping_json(mapping);
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    fn delete_event_source_mapping(
        &self,
        uuid: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let mapping = state.event_source_mappings.remove(uuid).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("The resource you requested does not exist. (Service: Lambda, Status Code: 404, Request ID: {uuid})"),
            )
        })?;

        let mut response = self.event_source_mapping_json(&mapping);
        response["State"] = json!("Deleting");
        Ok(AwsResponse::json(
            StatusCode::ACCEPTED,
            response.to_string(),
        ))
    }

    pub(crate) fn function_config_json(&self, func: &LambdaFunction) -> Value {
        let mut env_vars = json!({});
        if !func.environment.is_empty() {
            env_vars = json!({ "Variables": func.environment });
        }

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
            "TracingConfig": { "Mode": "PassThrough" },
            "RevisionId": uuid::Uuid::new_v4().to_string(),
        });
        if let Some(ref uri) = func.image_uri {
            config["Code"] = json!({
                "ImageUri": uri,
                "ResolvedImageUri": uri,
            });
        }
        config
    }

    fn event_source_mapping_json(&self, mapping: &EventSourceMapping) -> Value {
        let mut out = json!({
            "UUID": mapping.uuid,
            "FunctionArn": mapping.function_arn,
            "EventSourceArn": mapping.event_source_arn,
            "BatchSize": mapping.batch_size,
            "State": mapping.state,
            "LastModified": mapping.last_modified.timestamp_millis() as f64 / 1000.0,
        });
        let obj = out.as_object_mut().expect("json! built object");
        if !mapping.filter_patterns.is_empty() {
            obj.insert(
                "FilterCriteria".into(),
                json!({
                    "Filters": mapping.filter_patterns.iter().map(|p| json!({"Pattern": p})).collect::<Vec<_>>(),
                }),
            );
        }
        if !mapping.function_response_types.is_empty() {
            obj.insert(
                "FunctionResponseTypes".into(),
                json!(mapping.function_response_types),
            );
        }
        if let Some(sp) = &mapping.starting_position {
            obj.insert("StartingPosition".into(), json!(sp));
        }
        if let Some(ts) = mapping.starting_position_timestamp {
            obj.insert("StartingPositionTimestamp".into(), json!(ts));
        }
        if let Some(pf) = mapping.parallelization_factor {
            obj.insert("ParallelizationFactor".into(), json!(pf));
        }
        if let Some(w) = mapping.maximum_batching_window_in_seconds {
            obj.insert("MaximumBatchingWindowInSeconds".into(), json!(w));
        }
        out
    }

    /// Grant a permission on a Lambda function by appending a
    /// statement to its resource-based policy.
    ///
    /// Mirrors AWS: the caller passes `(StatementId, Action,
    /// Principal, SourceArn?, SourceAccount?)` and the service
    /// composes a canonical policy document so that the existing
    /// evaluator can read it without a Lambda-specific fork. Per the
    /// S3 rollout's #427 evaluator, `SourceArn` becomes an `ArnLike`
    /// Condition and `SourceAccount` becomes a `StringEquals`
    /// Condition — both are already supported by the Phase 2 operator
    /// set, so the permission gate behaves end-to-end without any new
    /// evaluator code.
    fn add_permission(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let statement_id = body
            .get("StatementId")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "StatementId is required",
                )
            })?
            .to_string();
        let action = body
            .get("Action")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "Action is required",
                )
            })?
            .to_string();
        let principal_raw = body
            .get("Principal")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "Principal is required",
                )
            })?
            .to_string();
        let source_arn = body
            .get("SourceArn")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let source_account = body
            .get("SourceAccount")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let func = state.functions.get_mut(function_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Function not found: {function_name}"),
            )
        })?;

        // Load current policy or seed a fresh canonical doc. Any
        // stored blob that doesn't parse as a JSON object is treated
        // as corrupt and replaced — `AddPermission` is the only
        // mutation path for this field and it always writes valid
        // JSON, so seeing a non-object here means something else
        // wrote garbage, and silently propagating it would make
        // later reads harder to debug.
        let mut doc: Value = func
            .policy
            .as_deref()
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .filter(|v| v.is_object())
            .unwrap_or_else(|| json!({"Version": "2012-10-17", "Statement": []}));

        // Ensure Statement is an array so we can push into it.
        if !doc.get("Statement").map(|s| s.is_array()).unwrap_or(false) {
            doc["Statement"] = json!([]);
        }
        let statements = doc["Statement"].as_array_mut().unwrap();

        // Reject duplicate StatementId — matches AWS's
        // ResourceConflictException.
        if statements
            .iter()
            .any(|s| s.get("Sid").and_then(|v| v.as_str()) == Some(statement_id.as_str()))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceConflictException",
                format!("The statement id ({statement_id}) provided already exists"),
            ));
        }

        // Canonicalize Principal: a service host string becomes
        // `{"Service": "<host>"}`, an account-id or ARN becomes
        // `{"AWS": "<raw>"}`. AWS accepts both shapes on the wire;
        // storing the object form uniformly means the existing
        // evaluator path handles everything without reading back the
        // raw input.
        let principal_value =
            if principal_raw.ends_with(".amazonaws.com") || principal_raw.contains(".amazon") {
                json!({ "Service": principal_raw })
            } else {
                json!({ "AWS": principal_raw })
            };

        // Emit SourceArn / SourceAccount as Condition keys so the
        // existing Phase 2 ArnLike / StringEquals operators gate the
        // grant without new evaluator code.
        let mut condition = serde_json::Map::new();
        if let Some(arn) = source_arn.as_ref() {
            condition.insert("ArnLike".to_string(), json!({ "aws:SourceArn": arn }));
        }
        if let Some(acct) = source_account.as_ref() {
            condition.insert(
                "StringEquals".to_string(),
                json!({ "aws:SourceAccount": acct }),
            );
        }

        let mut new_statement = serde_json::Map::new();
        new_statement.insert("Sid".to_string(), json!(statement_id));
        new_statement.insert("Effect".to_string(), json!("Allow"));
        new_statement.insert("Principal".to_string(), principal_value);
        new_statement.insert("Action".to_string(), json!(format!("lambda:{action}")));
        new_statement.insert("Resource".to_string(), json!(func.function_arn));
        if !condition.is_empty() {
            new_statement.insert("Condition".to_string(), Value::Object(condition));
        }
        let statement_json = Value::Object(new_statement);
        statements.push(statement_json.clone());

        func.policy = Some(serde_json::to_string(&doc).unwrap());

        Ok(AwsResponse::json(
            StatusCode::CREATED,
            json!({ "Statement": serde_json::to_string(&statement_json).unwrap() }).to_string(),
        ))
    }

    fn remove_permission(
        &self,
        function_name: &str,
        statement_id: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let func = state.functions.get_mut(function_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Function not found: {function_name}"),
            )
        })?;
        let policy_str = func.policy.as_deref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("No policy is associated with function {function_name}"),
            )
        })?;
        let mut doc: Value = serde_json::from_str(policy_str).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                "stored resource policy is not valid JSON",
            )
        })?;
        let statements = doc
            .get_mut("Statement")
            .and_then(|s| s.as_array_mut())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalError",
                    "stored resource policy has no Statement array",
                )
            })?;
        let before = statements.len();
        statements.retain(|s| s.get("Sid").and_then(|v| v.as_str()) != Some(statement_id));
        if statements.len() == before {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Statement {statement_id} is not found in resource policy"),
            ));
        }
        // Leave an empty {"Statement":[]} behind rather than clearing
        // the field to None — AWS's GetPolicy keeps returning the
        // (empty) doc until the function itself is deleted.
        func.policy = Some(serde_json::to_string(&doc).unwrap());
        Ok(AwsResponse::json(StatusCode::NO_CONTENT, String::new()))
    }

    fn get_policy(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, "");
        let state = accounts.get(account_id).unwrap_or(&empty);
        let func = state.functions.get(function_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Function not found: {function_name}"),
            )
        })?;
        let policy = func.policy.as_deref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("No policy is associated with function {function_name}"),
            )
        })?;
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({
                "Policy": policy,
                "RevisionId": uuid::Uuid::new_v4().to_string(),
            })
            .to_string(),
        ))
    }
}

#[async_trait]
impl AwsService for LambdaService {
    fn service_name(&self) -> &str {
        "lambda"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, resource_name) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        // For actions where path[2] is a FunctionName, accept all four
        // AWS-supported forms (plain name, qualified name, full ARN,
        // partial ARN) by collapsing to the bare function name before
        // hitting any state.functions lookup. Tag/EventSourceMapping/
        // Layer/CodeSigningConfig/CapacityProvider/DurableExecution
        // resources keep their original identifier — they're not
        // FunctionName-keyed.
        let resource_name = if is_function_name_action(action) {
            resource_name.map(|s| resolve_function_name(&s))
        } else {
            resource_name
        };

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
                | "CreateCapacityProvider"
                | "UpdateCapacityProvider"
                | "DeleteCapacityProvider"
                | "CheckpointDurableExecution"
                | "StopDurableExecution"
                | "SendDurableExecutionCallbackSuccess"
                | "SendDurableExecutionCallbackFailure"
                | "SendDurableExecutionCallbackHeartbeat"
                | "InvokeAsync"
                | "InvokeWithResponseStream"
        );

        let aid = &req.account_id;
        let result = match action {
            "CreateFunction" => self.create_function(&req),
            "ListFunctions" => self.list_functions(aid),
            "GetFunction" => self.get_function(
                resource_name.as_deref().unwrap_or(""),
                aid,
                req.region.as_str(),
            ),
            "DeleteFunction" => self.delete_function(resource_name.as_deref().unwrap_or(""), aid),
            "Invoke" => {
                let invocation_type = InvocationType::from_header(
                    req.headers
                        .get("x-amz-invocation-type")
                        .and_then(|v| v.to_str().ok()),
                );
                self.invoke(
                    resource_name.as_deref().unwrap_or(""),
                    &req.body,
                    aid,
                    invocation_type,
                )
                .await
            }
            "InvokeAsync" => {
                self.invoke(
                    resource_name.as_deref().unwrap_or(""),
                    &req.body,
                    aid,
                    InvocationType::Event,
                )
                .await
            }
            "PublishVersion" => self.publish_version(resource_name.as_deref().unwrap_or(""), aid),
            "AddPermission" => self.add_permission(resource_name.as_deref().unwrap_or(""), &req),
            "GetPolicy" => self.get_policy(resource_name.as_deref().unwrap_or(""), aid),
            "RemovePermission" => {
                // Path: /2015-03-31/functions/{name}/policy/{sid}
                let sid = req.path_segments.get(4).cloned().unwrap_or_default();
                self.remove_permission(resource_name.as_deref().unwrap_or(""), &sid, aid)
            }
            "CreateEventSourceMapping" => self.create_event_source_mapping(&req),
            "ListEventSourceMappings" => self.list_event_source_mappings(aid),
            "GetEventSourceMapping" => {
                self.get_event_source_mapping(resource_name.as_deref().unwrap_or(""), aid)
            }
            "DeleteEventSourceMapping" => {
                self.delete_event_source_mapping(resource_name.as_deref().unwrap_or(""), aid)
            }
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
            "UpdateCapacityProvider",
            "DeleteCapacityProvider",
            "ListCapacityProviders",
            "ListFunctionVersionsByCapacityProvider",
            "CheckpointDurableExecution",
            "GetDurableExecution",
            "GetDurableExecutionHistory",
            "GetDurableExecutionState",
            "ListDurableExecutionsByFunction",
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
            "GetFunction" | "DeleteFunction" | "InvokeFunction" | "PublishVersion"
            | "AddPermission" | "RemovePermission" | "GetPolicy" => {
                let raw = resource_name.unwrap_or_default();
                let name = resolve_function_name(&raw);
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
                                state.region,
                                state.account_id,
                                resolve_function_name(n)
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedLambdaState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
        let path_segments: Vec<String> = path
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        AwsRequest {
            service: "lambda".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments,
            raw_path: path.to_string(),
            raw_query: String::new(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn iam_condition_keys_for_add_permission_populates_arn_and_principal() {
        let svc = LambdaService::new(make_state());
        let body = json!({
            "StatementId": "stmt",
            "Action": "lambda:InvokeFunction",
            "Principal": "s3.amazonaws.com",
        })
        .to_string();
        let req = make_request(Method::POST, "/2015-03-31/functions/my-func/policy", &body);
        let action = fakecloud_core::auth::IamAction {
            service: "lambda",
            action: "AddPermission",
            resource: "arn:aws:lambda:us-east-1:123456789012:function:my-func".to_string(),
        };
        let keys = svc.iam_condition_keys_for(&req, &action);
        assert_eq!(
            keys.get("lambda:functionarn"),
            Some(&vec![
                "arn:aws:lambda:us-east-1:123456789012:function:my-func".to_string()
            ])
        );
        assert_eq!(
            keys.get("lambda:principal"),
            Some(&vec!["s3.amazonaws.com".to_string()])
        );
    }

    #[test]
    fn iam_condition_keys_for_add_permission_omits_missing_principal() {
        let svc = LambdaService::new(make_state());
        let body = json!({"StatementId": "stmt", "Action": "lambda:InvokeFunction"}).to_string();
        let req = make_request(Method::POST, "/2015-03-31/functions/my-func/policy", &body);
        let action = fakecloud_core::auth::IamAction {
            service: "lambda",
            action: "AddPermission",
            resource: "arn:aws:lambda:us-east-1:123456789012:function:my-func".to_string(),
        };
        let keys = svc.iam_condition_keys_for(&req, &action);
        assert!(!keys.contains_key("lambda:principal"));
        assert!(keys.contains_key("lambda:functionarn"));
    }

    #[test]
    fn iam_condition_keys_for_non_add_permission_is_empty() {
        let svc = LambdaService::new(make_state());
        let req = make_request(Method::GET, "/2015-03-31/functions/my-func", "");
        let action = fakecloud_core::auth::IamAction {
            service: "lambda",
            action: "GetFunction",
            resource: "arn:aws:lambda:us-east-1:123456789012:function:my-func".to_string(),
        };
        assert!(svc.iam_condition_keys_for(&req, &action).is_empty());
    }

    #[tokio::test]
    async fn test_create_and_get_function() {
        let state = make_state();
        let svc = LambdaService::new(state);

        let create_body = json!({
            "FunctionName": "my-func",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/test-role",
            "Handler": "index.handler",
            "Code": { "ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAA=" }
        });

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);

        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["FunctionName"], "my-func");
        assert_eq!(body["Runtime"], "python3.12");

        // Get
        let req = make_request(Method::GET, "/2015-03-31/functions/my-func", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["Configuration"]["FunctionName"], "my-func");
    }

    #[tokio::test]
    async fn test_delete_function() {
        let state = make_state();
        let svc = LambdaService::new(state);

        let create_body = json!({
            "FunctionName": "to-delete",
            "Runtime": "nodejs20.x",
            "Role": "arn:aws:iam::123456789012:role/test",
            "Handler": "index.handler",
            "Code": {}
        });

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        svc.handle(req).await.unwrap();

        let req = make_request(Method::DELETE, "/2015-03-31/functions/to-delete", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NO_CONTENT);

        // Verify deleted
        let req = make_request(Method::GET, "/2015-03-31/functions/to-delete", "");
        let resp = svc.handle(req).await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn test_invoke_without_runtime_returns_error() {
        let state = make_state();
        let svc = LambdaService::new(state);

        let create_body = json!({
            "FunctionName": "invoke-me",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/test",
            "Handler": "index.handler",
            "Code": {}
        });

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/invoke-me/invocations",
            r#"{"key": "value"}"#,
        );
        let resp = svc.handle(req).await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn test_invoke_nonexistent_function() {
        let state = make_state();
        let svc = LambdaService::new(state);

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/does-not-exist/invocations",
            "{}",
        );
        let resp = svc.handle(req).await;
        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn test_list_functions() {
        let state = make_state();
        let svc = LambdaService::new(state);

        for name in &["func-a", "func-b"] {
            let create_body = json!({
                "FunctionName": name,
                "Runtime": "python3.12",
                "Role": "arn:aws:iam::123456789012:role/test",
                "Handler": "index.handler",
                "Code": {}
            });
            let req = make_request(
                Method::POST,
                "/2015-03-31/functions",
                &create_body.to_string(),
            );
            svc.handle(req).await.unwrap();
        }

        let req = make_request(Method::GET, "/2015-03-31/functions", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["Functions"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_event_source_mapping() {
        let state = make_state();
        let svc = LambdaService::new(state);

        // Create function first
        let create_body = json!({
            "FunctionName": "esm-func",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/test",
            "Handler": "index.handler",
            "Code": {}
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions",
            &create_body.to_string(),
        );
        svc.handle(req).await.unwrap();

        // Create mapping
        let mapping_body = json!({
            "FunctionName": "esm-func",
            "EventSourceArn": "arn:aws:sqs:us-east-1:123456789012:my-queue",
            "BatchSize": 5
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/event-source-mappings",
            &mapping_body.to_string(),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::ACCEPTED);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let uuid = body["UUID"].as_str().unwrap().to_string();

        // List mappings
        let req = make_request(Method::GET, "/2015-03-31/event-source-mappings", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["EventSourceMappings"].as_array().unwrap().len(), 1);

        // Delete mapping
        let req = make_request(
            Method::DELETE,
            &format!("/2015-03-31/event-source-mappings/{uuid}"),
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::ACCEPTED);
    }

    async fn seed_function(svc: &LambdaService, name: &str) {
        let body = json!({
            "FunctionName": name,
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Handler": "index.handler",
            "Code": {}
        });
        let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
        svc.handle(req).await.unwrap();
    }

    #[tokio::test]
    async fn add_permission_builds_canonical_statement() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "f").await;

        let body = json!({
            "StatementId": "s3-invoke",
            "Action": "InvokeFunction",
            "Principal": "s3.amazonaws.com",
            "SourceArn": "arn:aws:s3:::my-bucket",
            "SourceAccount": "123456789012",
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/f/policy",
            &body.to_string(),
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);

        let out: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let statement: Value = serde_json::from_str(out["Statement"].as_str().unwrap()).unwrap();
        assert_eq!(statement["Sid"], "s3-invoke");
        assert_eq!(statement["Effect"], "Allow");
        assert_eq!(statement["Principal"]["Service"], "s3.amazonaws.com");
        assert_eq!(statement["Action"], "lambda:InvokeFunction");
        assert_eq!(
            statement["Resource"],
            "arn:aws:lambda:us-east-1:123456789012:function:f"
        );
        assert_eq!(
            statement["Condition"]["ArnLike"]["aws:SourceArn"],
            "arn:aws:s3:::my-bucket"
        );
        assert_eq!(
            statement["Condition"]["StringEquals"]["aws:SourceAccount"],
            "123456789012"
        );
    }

    #[tokio::test]
    async fn add_permission_aws_principal_emits_aws_key() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "f").await;

        let body = json!({
            "StatementId": "user-invoke",
            "Action": "InvokeFunction",
            "Principal": "arn:aws:iam::123456789012:user/alice",
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/f/policy",
            &body.to_string(),
        );
        svc.handle(req).await.unwrap();

        // Fetch via GetPolicy and inspect the stored doc.
        let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let doc: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
        let statements = doc["Statement"].as_array().unwrap();
        assert_eq!(statements.len(), 1);
        assert_eq!(
            statements[0]["Principal"]["AWS"],
            "arn:aws:iam::123456789012:user/alice"
        );
        assert!(statements[0].get("Condition").is_none());
    }

    #[tokio::test]
    async fn add_permission_rejects_duplicate_statement_id() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "f").await;

        let body = json!({
            "StatementId": "dup",
            "Action": "InvokeFunction",
            "Principal": "arn:aws:iam::123456789012:user/a",
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/f/policy",
            &body.to_string(),
        );
        svc.handle(req).await.unwrap();

        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/f/policy",
            &body.to_string(),
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn get_policy_returns_404_when_no_policy_attached() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "f").await;

        let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn remove_permission_strips_matching_sid_and_leaves_empty_doc() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "f").await;

        for sid in ["a", "b"] {
            let body = json!({
                "StatementId": sid,
                "Action": "InvokeFunction",
                "Principal": "arn:aws:iam::123456789012:user/u",
            });
            let req = make_request(
                Method::POST,
                "/2015-03-31/functions/f/policy",
                &body.to_string(),
            );
            svc.handle(req).await.unwrap();
        }

        // Remove "a"
        let req = make_request(Method::DELETE, "/2015-03-31/functions/f/policy/a", "");
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NO_CONTENT);

        // GetPolicy still returns the doc with just "b".
        let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let doc: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
        let stmts = doc["Statement"].as_array().unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(stmts[0]["Sid"], "b");

        // Remove the last one — doc stays (empty Statement array).
        let req = make_request(Method::DELETE, "/2015-03-31/functions/f/policy/b", "");
        svc.handle(req).await.unwrap();

        let req = make_request(Method::GET, "/2015-03-31/functions/f/policy", "");
        let resp = svc.handle(req).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let doc: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
        assert_eq!(doc["Statement"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn remove_permission_unknown_sid_is_404() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "f").await;

        let body = json!({
            "StatementId": "known",
            "Action": "InvokeFunction",
            "Principal": "arn:aws:iam::123456789012:user/u",
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/f/policy",
            &body.to_string(),
        );
        svc.handle(req).await.unwrap();

        let req = make_request(Method::DELETE, "/2015-03-31/functions/f/policy/other", "");
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn add_permission_on_missing_function_is_404() {
        let svc = LambdaService::new(make_state());
        let body = json!({
            "StatementId": "s",
            "Action": "InvokeFunction",
            "Principal": "arn:aws:iam::123456789012:user/u",
        });
        let req = make_request(
            Method::POST,
            "/2015-03-31/functions/missing/policy",
            &body.to_string(),
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn iam_action_for_maps_invoke_to_function_arn() {
        let svc = LambdaService::new(make_state());
        let req = make_request(Method::POST, "/2015-03-31/functions/f/invocations", "");
        let action = svc.iam_action_for(&req).unwrap();
        assert_eq!(action.service, "lambda");
        assert_eq!(action.action, "InvokeFunction");
        assert_eq!(
            action.resource,
            "arn:aws:lambda:us-east-1:123456789012:function:f"
        );
    }

    #[test]
    fn iam_action_for_maps_list_to_star() {
        let svc = LambdaService::new(make_state());
        let req = make_request(Method::GET, "/2015-03-31/functions", "");
        let action = svc.iam_action_for(&req).unwrap();
        assert_eq!(action.action, "ListFunctions");
        assert_eq!(action.resource, "*");
    }

    #[test]
    fn iam_action_for_create_reads_function_name_from_body() {
        let svc = LambdaService::new(make_state());
        let body = json!({
            "FunctionName": "newfn",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Handler": "index.handler",
            "Code": {}
        });
        let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
        let action = svc.iam_action_for(&req).unwrap();
        assert_eq!(action.action, "CreateFunction");
        assert_eq!(
            action.resource,
            "arn:aws:lambda:us-east-1:123456789012:function:newfn"
        );
    }

    // ── Error branch tests ──

    #[tokio::test]
    async fn create_function_duplicate_returns_conflict() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "dup-fn").await;

        let body = json!({
            "FunctionName": "dup-fn",
            "Runtime": "python3.12",
            "Role": "arn:aws:iam::123456789012:role/r",
            "Handler": "index.handler",
            "Code": {"ZipFile": "UEsDBBQ="},
        });
        let req = make_request(Method::POST, "/2015-03-31/functions", &body.to_string());
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected ResourceConflictException"),
        };
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn get_function_not_found() {
        let svc = LambdaService::new(make_state());
        let req = make_request(Method::GET, "/2015-03-31/functions/nope", "");
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_function_not_found() {
        let svc = LambdaService::new(make_state());
        let req = make_request(Method::DELETE, "/2015-03-31/functions/nope", "");
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_event_source_mapping_not_found() {
        let svc = LambdaService::new(make_state());
        let req = make_request(
            Method::GET,
            "/2015-03-31/event-source-mappings/nonexistent",
            "",
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn delete_event_source_mapping_not_found() {
        let svc = LambdaService::new(make_state());
        let req = make_request(
            Method::DELETE,
            "/2015-03-31/event-source-mappings/nonexistent",
            "",
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_policy_on_missing_function() {
        let svc = LambdaService::new(make_state());
        let req = make_request(Method::GET, "/2015-03-31/functions/nope/policy", "");
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn remove_permission_on_missing_function() {
        let svc = LambdaService::new(make_state());
        let req = make_request(
            Method::DELETE,
            "/2015-03-31/functions/nope/policy/stmt1",
            "",
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn publish_version_on_missing_function() {
        let svc = LambdaService::new(make_state());
        let req = make_request(Method::POST, "/2015-03-31/functions/nope/versions", "{}");
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unknown_route_returns_error() {
        let svc = LambdaService::new(make_state());
        let req = make_request(Method::POST, "/unknown/route", "{}");
        assert!(svc.handle(req).await.is_err());
    }

    #[tokio::test]
    async fn publish_version_unknown_function_errors() {
        let svc = LambdaService::new(make_state());
        assert!(svc.publish_version("ghost", "123456789012").is_err());
    }

    #[tokio::test]
    async fn get_function_unknown_errors() {
        let svc = LambdaService::new(make_state());
        assert!(svc
            .get_function("ghost", "123456789012", "us-east-1")
            .is_err());
    }

    #[tokio::test]
    async fn delete_function_unknown_errors() {
        let svc = LambdaService::new(make_state());
        assert!(svc.delete_function("ghost", "123456789012").is_err());
    }

    #[tokio::test]
    async fn get_event_source_mapping_unknown_errors() {
        let svc = LambdaService::new(make_state());
        assert!(svc
            .get_event_source_mapping("ghost", "123456789012")
            .is_err());
    }

    #[tokio::test]
    async fn delete_event_source_mapping_unknown_errors() {
        let svc = LambdaService::new(make_state());
        assert!(svc
            .delete_event_source_mapping("ghost", "123456789012")
            .is_err());
    }

    #[tokio::test]
    async fn list_functions_empty_ok() {
        let svc = LambdaService::new(make_state());
        let resp = svc.list_functions("123456789012").unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
    }

    #[tokio::test]
    async fn list_event_source_mappings_empty_ok() {
        let svc = LambdaService::new(make_state());
        let resp = svc.list_event_source_mappings("123456789012").unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
    }

    #[test]
    fn resolve_function_name_plain() {
        assert_eq!(resolve_function_name("my-fn"), "my-fn");
    }

    #[test]
    fn resolve_function_name_qualified_plain() {
        assert_eq!(resolve_function_name("my-fn:PROD"), "my-fn");
        assert_eq!(resolve_function_name("my-fn:7"), "my-fn");
        assert_eq!(resolve_function_name("my-fn:$LATEST"), "my-fn");
    }

    #[test]
    fn resolve_function_name_full_arn() {
        assert_eq!(
            resolve_function_name("arn:aws:lambda:us-east-1:123456789012:function:my-fn"),
            "my-fn"
        );
    }

    #[test]
    fn resolve_function_name_qualified_full_arn() {
        assert_eq!(
            resolve_function_name("arn:aws:lambda:us-east-1:123456789012:function:my-fn:PROD"),
            "my-fn"
        );
        assert_eq!(
            resolve_function_name("arn:aws:lambda:us-east-1:123456789012:function:my-fn:7"),
            "my-fn"
        );
        assert_eq!(
            resolve_function_name("arn:aws:lambda:us-east-1:123456789012:function:my-fn:$LATEST"),
            "my-fn"
        );
    }

    #[test]
    fn resolve_function_name_partial_arn() {
        assert_eq!(
            resolve_function_name("123456789012:function:my-fn"),
            "my-fn"
        );
    }

    #[test]
    fn resolve_function_name_qualified_partial_arn() {
        assert_eq!(
            resolve_function_name("123456789012:function:my-fn:7"),
            "my-fn"
        );
        assert_eq!(
            resolve_function_name("123456789012:function:my-fn:PROD"),
            "my-fn"
        );
    }

    #[test]
    fn resolve_function_name_empty() {
        assert_eq!(resolve_function_name(""), "");
    }

    #[test]
    fn resolve_function_name_partial_arn_requires_12_digit_account() {
        // Not a real partial ARN — first segment isn't 12 digits, so the
        // colon is treated as a qualifier separator on a plain name.
        assert_eq!(resolve_function_name("acct:function:my-fn"), "acct");
    }

    #[test]
    fn resolve_function_name_percent_encoded_arn() {
        // AWS SDKs URL-encode the colons in ARN-form path segments;
        // `arn%3Aaws%3Alambda%3A...` must collapse to the bare name.
        assert_eq!(
            resolve_function_name(
                "arn%3Aaws%3Alambda%3Aus-east-1%3A123456789012%3Afunction%3Atoolkit-fn"
            ),
            "toolkit-fn"
        );
        assert_eq!(
            resolve_function_name(
                "arn%3aaws%3alambda%3aus-east-1%3a123456789012%3afunction%3atoolkit-fn%3a%24LATEST"
            ),
            "toolkit-fn"
        );
    }

    #[test]
    fn resolve_function_name_unknown_arn_passthrough() {
        // An ARN we don't recognise (wrong service / wrong shape) is
        // returned unchanged so the caller surfaces a 404 with the
        // original identifier instead of silently truncating it.
        assert_eq!(
            resolve_function_name("arn:aws:lambda:us-east-1:123456789012:layer:my-layer"),
            "arn:aws:lambda:us-east-1:123456789012:layer:my-layer"
        );
        assert_eq!(
            resolve_function_name("arn:aws:lambda:::function:my-fn"),
            "arn:aws:lambda:::function:my-fn"
        );
    }

    #[tokio::test]
    async fn get_function_accepts_full_arn_in_path() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "toolkit-fn").await;

        let req = make_request(
            Method::GET,
            "/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:toolkit-fn",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["Configuration"]["FunctionName"], "toolkit-fn");
    }

    #[tokio::test]
    async fn get_function_accepts_partial_arn_in_path() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "toolkit-fn").await;

        let req = make_request(
            Method::GET,
            "/2015-03-31/functions/123456789012:function:toolkit-fn",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["Configuration"]["FunctionName"], "toolkit-fn");
    }

    #[tokio::test]
    async fn get_function_accepts_qualified_arn_in_path() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "toolkit-fn").await;

        let req = make_request(
            Method::GET,
            "/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:toolkit-fn:$LATEST",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }

    #[tokio::test]
    async fn delete_function_accepts_full_arn_in_path() {
        let svc = LambdaService::new(make_state());
        seed_function(&svc, "toolkit-fn").await;

        let req = make_request(
            Method::DELETE,
            "/2015-03-31/functions/arn:aws:lambda:us-east-1:123456789012:function:toolkit-fn",
            "",
        );
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(resp.status, StatusCode::NO_CONTENT);

        // Confirm the function is actually gone.
        let req = make_request(Method::GET, "/2015-03-31/functions/toolkit-fn", "");
        let resp = svc.handle(req).await;
        assert!(resp.is_err());
    }
}
