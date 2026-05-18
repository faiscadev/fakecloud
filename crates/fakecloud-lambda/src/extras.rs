//! Lambda handlers added to close the conformance gap. Aliases, layers,
//! function URL configs, concurrency, code signing, event invoke, runtime
//! management, scaling, recursion, tagging, and account settings.

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::LambdaService;
use crate::state::{
    AccountSettings, AttachedLayer, CodeSigningConfig, EventInvokeConfig, FunctionAlias,
    FunctionScalingConfig, FunctionUrlConfig, LambdaState, Layer, LayerVersion,
    ProvisionedConcurrencyConfig, RuntimeManagementConfig,
};

/// Resolve a layer-version ARN to its current `CodeSize` from the
/// multi-account state. Returns 0 when the ARN is unparseable, when the
/// referenced account/layer/version is unknown, or when the version was
/// published without ZIP content (legacy snapshots).
pub(crate) fn resolve_layer_attachments(
    accounts: &fakecloud_core::multi_account::MultiAccountState<LambdaState>,
    arns: Vec<String>,
) -> Vec<AttachedLayer> {
    arns.into_iter()
        .map(|arn| {
            let code_size = parse_layer_version_arn(&arn)
                .and_then(|(acct, name, ver)| {
                    accounts
                        .get(&acct)
                        .and_then(|s| s.layers.get(&name))
                        .and_then(|l| l.versions.iter().find(|v| v.version == ver))
                        .map(|v| v.code_size)
                })
                .unwrap_or(0);
            AttachedLayer { arn, code_size }
        })
        .collect()
}

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        format!("Missing required field: {name}"),
    )
}

fn not_found(entity: &str, name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("{entity} not found: {name}"),
    )
}

fn ok(body: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
}

fn empty() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
}

fn body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or_else(|_| Value::Object(Default::default()))
}

/// Extract the function name from a Lambda function ARN, ignoring any
/// trailing `:version` / `:alias` qualifier. Returns `None` for ARNs
/// that name a different resource type (event-source mapping,
/// code-signing config, layer, …) — Lambda only supports tags on
/// function ARNs in this implementation, so non-function ARNs are
/// rejected by callers as `InvalidParameterValueException`.
fn function_name_from_arn(arn: &str) -> Option<String> {
    let rest = arn.strip_prefix("arn:aws:lambda:")?;
    let mut parts = rest.splitn(5, ':');
    let _region = parts.next()?;
    let _account = parts.next()?;
    let resource_kind = parts.next()?;
    if resource_kind != "function" {
        return None;
    }
    let name_with_qualifier = parts.next()?;
    Some(
        name_with_qualifier
            .split(':')
            .next()
            .unwrap_or(name_with_qualifier)
            .to_string(),
    )
}

/// Parse a raw query string into key/value pairs preserving repeats.
/// `req.query_params` is a `HashMap<String, String>` and so collapses
/// `tagKeys=A&tagKeys=B` to a single entry; this lets the
/// `UntagResource` handler see every value the caller actually sent.
/// Percent-decodes both key and value with the same lossy fallback the
/// rest of the dispatch path uses.
fn parse_query_pairs(raw_query: &str) -> Vec<(String, String)> {
    raw_query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let mut it = pair.splitn(2, '=');
            let k = it.next().unwrap_or("");
            let v = it.next().unwrap_or("");
            (decode_query_segment(k), decode_query_segment(v))
        })
        .collect()
}

fn decode_query_segment(s: &str) -> String {
    // Replace `+` with space to match `application/x-www-form-urlencoded`,
    // then percent-decode. SDKs hit both shapes for path/query data.
    let plus_decoded = s.replace('+', " ");
    percent_encoding::percent_decode_str(&plus_decoded)
        .decode_utf8_lossy()
        .into_owned()
}

/// Build a fakecloud-hosted download URL for a layer version's ZIP. The URL
/// is reachable on the same authority the SDK used for the original
/// request, so test harnesses get a working `Location` they can `GET`
/// directly instead of the placeholder AWS clients otherwise see.
fn layer_content_url(req: &AwsRequest, account_id: &str, layer_name: &str, version: i64) -> String {
    let host = req
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost");
    let scheme = req
        .headers
        .get("x-forwarded-proto")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("http");
    format!(
        "{scheme}://{host}/_fakecloud/lambda/layer-content/{account_id}/{layer_name}/{version}.zip"
    )
}

/// Build a fakecloud-hosted download URL for a function version's ZIP. AWS
/// Toolkit (and `aws lambda get-function --query 'Code.Location'`) expects
/// this to resolve to an actual ZIP body, so the URL points back at the
/// running fakecloud instance on the same authority the SDK used.
pub(crate) fn function_code_url(
    req: &AwsRequest,
    account_id: &str,
    function_name: &str,
    version_label: &str,
) -> String {
    let host = req
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost");
    let scheme = req
        .headers
        .get("x-forwarded-proto")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("http");
    let file = if version_label == "$LATEST" {
        "latest.zip".to_string()
    } else {
        format!("{version_label}.zip")
    };
    format!("{scheme}://{host}/_fakecloud/lambda/function-code/{account_id}/{function_name}/{file}")
}

/// AWS layer-version ARN: `arn:aws:lambda:<region>:<account>:layer:<name>:<version>`.
/// Returns `(account_id, layer_name, version)`. Used to resolve cross-account
/// layer references attached to a function.
pub fn parse_layer_version_arn(arn: &str) -> Option<(String, String, i64)> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() != 8 || parts[0] != "arn" || parts[2] != "lambda" || parts[5] != "layer" {
        return None;
    }
    let account = parts[4].to_string();
    let name = parts[6].to_string();
    let version: i64 = parts[7].parse().ok()?;
    Some((account, name, version))
}

/// Enum members of `com.amazonaws.lambda#Runtime`. Used by layer-listing
/// ops to validate the `CompatibleRuntime` query filter without
/// teaching every handler the full enum.
const LAMBDA_RUNTIMES: &[&str] = &[
    "nodejs",
    "nodejs4.3",
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
    "nodejs4.3-edge",
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

/// Validate the `CompatibleArchitecture` and `CompatibleRuntime` query
/// filters shared by `ListLayers` and `ListLayerVersions`.
fn validate_layer_filters(req: &AwsRequest) -> Result<(), AwsServiceError> {
    if let Some(arch) = req.query_params.get("CompatibleArchitecture") {
        if arch != "x86_64" && arch != "arm64" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "Invalid CompatibleArchitecture value '{}'; expected 'x86_64' or 'arm64'",
                    arch
                ),
            ));
        }
    }
    if let Some(rt) = req.query_params.get("CompatibleRuntime") {
        if !LAMBDA_RUNTIMES.contains(&rt.as_str()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("Invalid CompatibleRuntime value '{}'", rt),
            ));
        }
    }
    Ok(())
}

fn parse_qualifier(req: &AwsRequest) -> String {
    req.query_params
        .get("Qualifier")
        .cloned()
        .unwrap_or_else(|| "$LATEST".to_string())
}

/// Strict variant for operations whose Smithy model marks `Qualifier`
/// `@required` (provisioned-concurrency, scaling-config). Returns
/// `InvalidParameterValueException` when the query parameter is
/// missing, matching AWS's wire response.
fn require_qualifier(req: &AwsRequest) -> Result<String, AwsServiceError> {
    req.query_params.get("Qualifier").cloned().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValueException",
            "Qualifier is required for this operation",
        )
    })
}

fn id_from_time(prefix: &str) -> String {
    format!(
        "{}{}",
        prefix,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

impl LambdaService {
    pub(crate) async fn handle_extra(
        &self,
        action: &str,
        resource: Option<&str>,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let aid = req.account_id.as_str();
        let res = resource.unwrap_or("");
        match action {
            // Function lifecycle extras
            "GetFunctionConfiguration" => self.get_function_configuration(res, aid, req),
            "UpdateFunctionConfiguration" => self.update_function_configuration(res, req),
            "UpdateFunctionCode" => self.update_function_code(res, req),
            "UpdateEventSourceMapping" => self.update_event_source_mapping_handler(res, req),
            "GetAccountSettings" => self.get_account_settings(aid),
            "InvokeAsync" => Ok(AwsResponse::json(StatusCode::ACCEPTED, "{}".to_string())),
            "InvokeWithResponseStream" => self.invoke_with_response_stream(res, aid, req).await,

            // Versions
            "ListVersionsByFunction" => self.list_versions_by_function(res, aid, req),

            // Aliases
            "CreateAlias" => self.create_alias(res, req),
            "GetAlias" => self.get_alias(res, req),
            "ListAliases" => self.list_aliases(res, aid),
            "UpdateAlias" => self.update_alias(res, req),
            "DeleteAlias" => self.delete_alias(res, req),

            // Layers
            "PublishLayerVersion" => self.publish_layer_version(res, req),
            "GetLayerVersion" => self.get_layer_version(req),
            "GetLayerVersionByArn" => self.get_layer_version_by_arn(req),
            "ListLayers" => {
                validate_layer_filters(req)?;
                self.list_layers(aid)
            }
            "ListLayerVersions" => {
                validate_layer_filters(req)?;
                if res.is_empty() {
                    return Err(missing("LayerName"));
                }
                // Smithy `LayerName.length 1..140`; ARN form is longer
                // (~200) but the probe drives the bare-name path.
                let limit = if res.starts_with("arn:") { 200 } else { 140 };
                if res.chars().count() > limit {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        "LayerName exceeds the 140-character maximum",
                    ));
                }
                self.list_layer_versions(res, aid)
            }
            "DeleteLayerVersion" => self.delete_layer_version(req),
            "GetLayerVersionPolicy" => self.get_layer_version_policy(req),
            "AddLayerVersionPermission" => self.add_layer_version_permission(req),
            "RemoveLayerVersionPermission" => self.remove_layer_version_permission(req),

            // Function URL
            "CreateFunctionUrlConfig" => self.create_function_url_config(res, req),
            "GetFunctionUrlConfig" => self.get_function_url_config(res, aid),
            "UpdateFunctionUrlConfig" => self.update_function_url_config(res, req),
            "DeleteFunctionUrlConfig" => self.delete_function_url_config(res, aid),
            "ListFunctionUrlConfigs" => self.list_function_url_configs(aid),

            // Concurrency
            "PutFunctionConcurrency" => self.put_function_concurrency(res, req),
            "GetFunctionConcurrency" => self.get_function_concurrency(res, aid),
            "DeleteFunctionConcurrency" => self.delete_function_concurrency(res, aid),
            "PutProvisionedConcurrencyConfig" => self.put_provisioned_concurrency(res, req),
            "GetProvisionedConcurrencyConfig" => self.get_provisioned_concurrency(res, req),
            "DeleteProvisionedConcurrencyConfig" => self.delete_provisioned_concurrency(res, req),
            "ListProvisionedConcurrencyConfigs" => self.list_provisioned_concurrency(res, aid),

            // Code signing
            "CreateCodeSigningConfig" => self.create_code_signing_config(req),
            "GetCodeSigningConfig" => self.get_code_signing_config(res, aid),
            "UpdateCodeSigningConfig" => self.update_code_signing_config(res, req),
            "DeleteCodeSigningConfig" => self.delete_code_signing_config(res, aid),
            "ListCodeSigningConfigs" => self.list_code_signing_configs(aid),
            "PutFunctionCodeSigningConfig" => self.put_function_code_signing(res, req),
            "GetFunctionCodeSigningConfig" => self.get_function_code_signing(res, aid),
            "DeleteFunctionCodeSigningConfig" => self.delete_function_code_signing(res, aid),
            "ListFunctionsByCodeSigningConfig" => self.list_functions_by_code_signing(res, aid),

            // Event invoke
            "PutFunctionEventInvokeConfig" | "UpdateFunctionEventInvokeConfig" => {
                self.put_function_event_invoke(res, req)
            }
            "GetFunctionEventInvokeConfig" => self.get_function_event_invoke(res, req),
            "DeleteFunctionEventInvokeConfig" => self.delete_function_event_invoke(res, req),
            "ListFunctionEventInvokeConfigs" => self.list_function_event_invoke(res, aid),

            // Runtime management
            "PutRuntimeManagementConfig" => self.put_runtime_management(res, req),
            "GetRuntimeManagementConfig" => self.get_runtime_management(res, req),

            // Scaling
            "PutFunctionScalingConfig" => self.put_scaling_config(res, req),
            "GetFunctionScalingConfig" => {
                require_qualifier(req)?;
                self.get_scaling_config(res, aid)
            }

            // Recursion
            "PutFunctionRecursionConfig" => self.put_recursion_config(res, req),
            "GetFunctionRecursionConfig" => self.get_recursion_config(res, aid),

            // Tags
            "TagResource" => self.tag_resource(res, req),
            "UntagResource" => self.untag_resource(res, req),
            "ListTags" => self.list_tags(res, aid),

            _ => Err(AwsServiceError::action_not_implemented("lambda", action)),
        }
    }

    fn with_state_read<F, R>(&self, account_id: &str, region: &str, f: F) -> R
    where
        F: FnOnce(&LambdaState) -> R,
    {
        let accounts = self.state.read();
        let empty = LambdaState::new(account_id, region);
        let state = accounts.get(account_id).unwrap_or(&empty);
        f(state)
    }

    // ── Function lifecycle extras ──

    fn get_function_configuration(
        &self,
        function_name: &str,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        let qualifier = req.query_params.get("Qualifier").cloned();
        self.with_state_read(account_id, &region, |state| {
            let live = state
                .functions
                .get(function_name)
                .ok_or_else(|| not_found("Function", function_name))?;
            // Qualifier resolution mirrors GetFunction: $LATEST or omitted
            // returns the live config; numeric / alias qualifiers resolve
            // to a numbered snapshot.
            let resolved = crate::service::resolve_qualifier_to_version(
                state,
                function_name,
                qualifier.as_deref(),
            );
            let (func, version_label) = match resolved {
                None => (live, "$LATEST".to_string()),
                Some(v) => {
                    let snap = state
                        .function_version_snapshots
                        .get(function_name)
                        .and_then(|m| m.get(&v))
                        .ok_or_else(|| not_found("Function", function_name))?;
                    (snap, v)
                }
            };
            let mut config = self.function_config_json(func);
            config["Version"] = json!(version_label);
            if version_label != "$LATEST" {
                config["FunctionArn"] = json!(format!("{}:{version_label}", live.function_arn));
                config["MasterArn"] = json!(live.function_arn);
            }
            ok(config)
        })
    }

    fn update_function_configuration(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        // Validate before taking the write lock and before any mutation:
        // an invalid EphemeralStorage.Size on an otherwise valid request
        // must not silently apply the surrounding fields.
        let validated_ephemeral = match body["EphemeralStorage"]["Size"].as_i64() {
            Some(size) => Some(crate::service::validate_ephemeral_storage(size)?),
            None => None,
        };
        let mut accounts = self.state.write();
        // Pre-resolve layer attachments before re-borrowing accounts mutably
        // for the function. Layer ARNs may live in sibling accounts.
        let layer_attachments: Option<Vec<AttachedLayer>> = body["Layers"].as_array().map(|arr| {
            let arns: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            resolve_layer_attachments(&accounts, arns)
        });
        let state = accounts.get_or_create(&req.account_id);
        let func = state
            .functions
            .get_mut(function_name)
            .ok_or_else(|| not_found("Function", function_name))?;
        if let Some(handler) = body["Handler"].as_str() {
            func.handler = handler.to_string();
        }
        if let Some(t) = body["Timeout"].as_i64() {
            func.timeout = t;
        }
        if let Some(m) = body["MemorySize"].as_i64() {
            func.memory_size = m;
        }
        if let Some(role) = body["Role"].as_str() {
            func.role = role.to_string();
        }
        if let Some(desc) = body["Description"].as_str() {
            func.description = desc.to_string();
        }
        if let Some(rt) = body["Runtime"].as_str() {
            func.runtime = rt.to_string();
        }
        if let Some(env) = body["Environment"]["Variables"].as_object() {
            func.environment = env
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect();
        }
        if let Some(mode) = body["TracingConfig"]["Mode"].as_str() {
            func.tracing_mode = Some(mode.to_string());
        }
        if let Some(arn) = body["KMSKeyArn"].as_str() {
            func.kms_key_arn = if arn.is_empty() {
                None
            } else {
                Some(arn.to_string())
            };
        }
        if let Some(size) = validated_ephemeral {
            func.ephemeral_storage_size = Some(size);
        }
        if body["VpcConfig"].is_object() {
            func.vpc_config = Some(body["VpcConfig"].clone());
        }
        if body["SnapStart"].is_object() {
            func.snap_start = Some(body["SnapStart"].clone());
        }
        if let Some(arn) = body["DeadLetterConfig"]["TargetArn"].as_str() {
            func.dead_letter_config_arn = if arn.is_empty() {
                None
            } else {
                Some(arn.to_string())
            };
        }
        if let Some(fsc) = body["FileSystemConfigs"].as_array() {
            func.file_system_configs = fsc.clone();
        }
        if body["LoggingConfig"].is_object() {
            func.logging_config = Some(body["LoggingConfig"].clone());
        }
        if body["ImageConfig"].is_object() {
            func.image_config = Some(body["ImageConfig"].clone());
        }
        if body["DurableConfig"].is_object() {
            func.durable_config = Some(body["DurableConfig"].clone());
        }
        if let Some(attachments) = layer_attachments {
            func.layers = attachments;
        }
        // RevisionId rotates only on real config changes — clients
        // round-trip it through optimistic-concurrency calls, so we
        // mint a fresh one here to signal "config changed".
        func.revision_id = uuid::Uuid::new_v4().to_string();
        func.last_modified = Utc::now();
        ok(self.function_config_json(func))
    }

    fn update_function_code(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap_or_default();

        // ZipFile / ImageUri / S3Bucket+S3Key are mutually exclusive; AWS
        // rejects the request when more than one is present. The handler
        // picks one with a defined precedence: ZipFile, S3 descriptor,
        // ImageUri.
        let new_zip: Option<Vec<u8>> = match body["ZipFile"].as_str() {
            Some(b64) => Some(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).map_err(
                    |_| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValueException",
                            "Could not decode ZipFile: invalid base64",
                        )
                    },
                )?,
            ),
            None => None,
        };
        let new_image_uri = body["ImageUri"].as_str().map(String::from);
        // S3 source descriptor: when the caller didn't supply ZipFile or
        // ImageUri, AWS expects S3Bucket+S3Key (S3ObjectVersion is
        // optional). fakecloud doesn't fetch the object — CreateFunction
        // takes the same shortcut — so we synthesize a fingerprint from
        // the descriptor and use that as the new code identity. The hash
        // and size still rotate when the descriptor differs, so
        // optimistic-concurrency callers see RevisionId bump on real
        // changes.
        // S3-sourced code: if an S3Delivery hook is wired, fetch the
        // actual object bytes and treat them as a ZIP upload. This
        // matches real Lambda's S3-pull semantics. Fall back to the
        // descriptor-hash shortcut when no hook is available.
        let s3_fetched_zip: Option<Vec<u8>> = match (
            body["S3Bucket"].as_str(),
            body["S3Key"].as_str(),
        ) {
            (Some(bucket), Some(key)) if new_zip.is_none() && new_image_uri.is_none() => {
                if let Some(s3) = &self.s3_delivery {
                    match s3.get_object(&req.account_id, bucket, key) {
                        Ok(bytes) => Some(bytes),
                        Err(e) => {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "InvalidParameterValueException",
                                format!("Error occurred while GetObject. S3 Error Code: NoSuchKey. S3 Error Message: {e}"),
                            ));
                        }
                    }
                } else {
                    None
                }
            }
            _ => None,
        };

        let new_s3_descriptor: Option<Vec<u8>> =
            match (body["S3Bucket"].as_str(), body["S3Key"].as_str()) {
                (Some(bucket), Some(key))
                    if new_zip.is_none() && new_image_uri.is_none() && s3_fetched_zip.is_none() =>
                {
                    let mut descriptor = serde_json::Map::new();
                    descriptor.insert("S3Bucket".to_string(), Value::String(bucket.to_string()));
                    descriptor.insert("S3Key".to_string(), Value::String(key.to_string()));
                    if let Some(ver) = body["S3ObjectVersion"].as_str() {
                        descriptor.insert(
                            "S3ObjectVersion".to_string(),
                            Value::String(ver.to_string()),
                        );
                    }
                    Some(serde_json::to_vec(&Value::Object(descriptor)).unwrap_or_default())
                }
                _ => None,
            };
        let new_zip = new_zip.or(s3_fetched_zip);
        let supplied_signing_profile = body["SigningProfileVersionArn"].as_str().map(String::from);
        let supplied_revision_id = body["RevisionId"].as_str().map(String::from);
        let new_architectures: Option<Vec<String>> = body["Architectures"].as_array().map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        });
        let dry_run = body["DryRun"].as_bool().unwrap_or(false);
        let publish = body["Publish"].as_bool().unwrap_or(false);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Function existence is the first check so callers always see
        // ResourceNotFoundException 404 even when CSC / sig-profile
        // fields would otherwise reject the request.
        if !state.functions.contains_key(function_name) {
            return Err(not_found("Function", function_name));
        }

        // Code-signing gate: if a CSC is bound to this function and at
        // least one allowed publisher is registered, the caller must
        // supply a SigningProfileVersionArn from that allow-list when
        // the policy is Enforce. Warn just lets the upload through.
        if let Some(csc_arn) = state.function_code_signing.get(function_name).cloned() {
            let csc_id = extract_csc_id(&csc_arn);
            if let Some(csc) = state.code_signing_configs.get(&csc_id).cloned() {
                if !csc.allowed_publishers.is_empty()
                    && csc
                        .untrusted_artifact_action
                        .eq_ignore_ascii_case("Enforce")
                {
                    let allowed = match supplied_signing_profile.as_deref() {
                        Some(arn) => csc.allowed_publishers.iter().any(|p| p == arn),
                        None => false,
                    };
                    if !allowed {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "CodeVerificationFailedException",
                            "The code signature failed the integrity check or the signing profile is not in the allowed publishers list.",
                        ));
                    }
                }
            }
        }

        let func = state
            .functions
            .get_mut(function_name)
            .ok_or_else(|| not_found("Function", function_name))?;

        // Optimistic-concurrency precondition: when the caller supplies
        // a RevisionId, it must match the function's current revision
        // or AWS rejects with PreconditionFailedException 412.
        if let Some(ref rev) = supplied_revision_id {
            if rev != &func.revision_id {
                return Err(AwsServiceError::aws_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailedException",
                    format!(
                        "The Revision Id provided: {rev} does not match the latest Revision Id of function: {function_name}. Call the GetFunction/GetAlias API to retrieve the latest Revision Id"
                    ),
                ));
            }
        }

        // DryRun validates the request shape but never mutates state.
        if dry_run {
            return ok(self.function_config_json(func));
        }

        let mut changed = false;
        if let Some(bytes) = new_zip {
            // SHA256(base64) of the new code, matching CreateFunction's
            // hash so GetFunction returns identical CodeSha256 round-trip.
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            let hash = hasher.finalize();
            let code_sha256 =
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hash);
            if code_sha256 != func.code_sha256 {
                changed = true;
            }
            func.code_size = bytes.len() as i64;
            func.code_zip = Some(bytes);
            func.code_sha256 = code_sha256;
            func.image_uri = None;
            func.package_type = "Zip".to_string();
        } else if let Some(descriptor_bytes) = new_s3_descriptor {
            // Hash the S3 descriptor JSON (S3Bucket+S3Key+optional
            // S3ObjectVersion) so the same descriptor produces a stable
            // sha and a different descriptor rotates RevisionId. This
            // mirrors CreateFunction's behavior for S3-sourced code,
            // which also fingerprints the descriptor rather than fetching
            // S3 (real Lambda fetches asynchronously).
            let mut hasher = Sha256::new();
            hasher.update(&descriptor_bytes);
            let hash = hasher.finalize();
            let code_sha256 =
                base64::Engine::encode(&base64::engine::general_purpose::STANDARD, hash);
            if code_sha256 != func.code_sha256 {
                changed = true;
            }
            func.code_size = descriptor_bytes.len() as i64;
            // We don't have the object bytes — clear the cached zip so
            // the runtime falls back to whatever it had previously cached
            // rather than serving stale bytes for the new descriptor.
            func.code_zip = None;
            func.code_sha256 = code_sha256;
            func.image_uri = None;
            func.package_type = "Zip".to_string();
        } else if let Some(uri) = new_image_uri {
            if func.image_uri.as_deref() != Some(uri.as_str()) {
                changed = true;
            }
            func.image_uri = Some(uri);
            func.code_zip = None;
            func.package_type = "Image".to_string();
            // AWS reports CodeSize=0 and an empty CodeSha256 for
            // image-package functions — the actual digest lives on the
            // ECR side, not in the Lambda response.
            func.code_size = 0;
            func.code_sha256 = String::new();
        }

        if let Some(arns) = new_architectures {
            if !arns.is_empty() && arns != func.architectures {
                changed = true;
                func.architectures = arns;
            }
        }

        if let Some(arn) = supplied_signing_profile {
            if func.signing_profile_version_arn.as_deref() != Some(arn.as_str()) {
                changed = true;
            }
            func.signing_profile_version_arn = Some(arn);
        }

        // last_modified is bumped on every call (matches AWS), but
        // revision_id only rotates when code or signing fields actually
        // change so optimistic-concurrency callers don't see spurious
        // updates from no-op pings.
        func.last_modified = Utc::now();
        if changed {
            func.revision_id = uuid::Uuid::new_v4().to_string();
        }
        // A successful UpdateFunctionCode clears any prior failure
        // reason — function_config_json elides the field when None,
        // matching AWS's "no LastUpdateStatusReason on success" shape.
        func.last_update_status_reason = None;
        func.last_update_status_reason_code = None;

        // Publish=true mints a new immutable version snapshot off the
        // freshly updated $LATEST and returns that version's config.
        if publish {
            drop(accounts);
            return self.publish_version(function_name, &req.account_id, req);
        }

        ok(self.function_config_json(func))
    }

    fn get_account_settings(&self, account_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        let settings = state.account_settings.clone().unwrap_or(AccountSettings {
            concurrent_executions: 1000,
            code_size_zipped: 52_428_800,
            code_size_unzipped: 262_144_000,
            total_code_size: 80_530_636_800,
        });
        if state.account_settings.is_none() {
            state.account_settings = Some(settings.clone());
        }
        // Real AccountUsage so clients monitoring deployment quotas see
        // accurate numbers. AWS sums total code size across all functions.
        let function_count = state.functions.len() as i64;
        let total_code_size: i64 = state.functions.values().map(|f| f.code_size).sum();
        ok(json!({
            "AccountLimit": {
                "ConcurrentExecutions": settings.concurrent_executions,
                "CodeSizeZipped": settings.code_size_zipped,
                "CodeSizeUnzipped": settings.code_size_unzipped,
                "TotalCodeSize": settings.total_code_size,
                "UnreservedConcurrentExecutions": settings.concurrent_executions,
            },
            "AccountUsage": {
                "TotalCodeSize": total_code_size,
                "FunctionCount": function_count,
            },
        }))
    }

    // ── Versions ──

    fn list_versions_by_function(
        &self,
        function_name: &str,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        let max_items: usize = req
            .query_params
            .get("MaxItems")
            .and_then(|v| v.parse::<usize>().ok())
            .map(|n| n.clamp(1, 50))
            .unwrap_or(50);
        let marker = req.query_params.get("Marker").cloned();
        self.with_state_read(account_id, &region, |state| {
            let func = state
                .functions
                .get(function_name)
                .ok_or_else(|| not_found("Function", function_name))?;
            // AWS returns $LATEST first, then numbered versions in
            // ascending order. Each numbered version is an immutable
            // snapshot of the function at publish time.
            let mut all: Vec<serde_json::Value> = Vec::new();
            let mut latest = self.function_config_json(func);
            latest["Version"] = json!("$LATEST");
            all.push(latest);
            let snapshots = state.function_version_snapshots.get(function_name);
            if let Some(numbered) = state.function_versions.get(function_name) {
                for v in numbered {
                    let snap = snapshots.and_then(|m| m.get(v)).unwrap_or(func);
                    let mut cfg = self.function_config_json(snap);
                    cfg["Version"] = json!(v);
                    cfg["FunctionArn"] = json!(format!("{}:{v}", func.function_arn));
                    cfg["MasterArn"] = json!(func.function_arn);
                    all.push(cfg);
                }
            }
            // Pagination: skip past Marker if supplied (Marker is the
            // Version string of the entry to start *after*), then take
            // up to MaxItems. Emit a NextMarker when truncated.
            let start = match marker.as_deref() {
                Some(m) => all
                    .iter()
                    .position(|v| v["Version"].as_str() == Some(m))
                    .map(|i| i + 1)
                    .unwrap_or(0),
                None => 0,
            };
            let end = (start + max_items).min(all.len());
            let page: Vec<serde_json::Value> = all[start..end].to_vec();
            let mut body = json!({ "Versions": page });
            if end < all.len() {
                if let Some(last) = all[end - 1]["Version"].as_str() {
                    body["NextMarker"] = json!(last);
                }
            }
            ok(body)
        })
    }

    // ── Aliases ──

    fn alias_key(function: &str, alias: &str) -> String {
        format!("{function}:{alias}")
    }

    fn create_alias(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let version = body["FunctionVersion"]
            .as_str()
            .unwrap_or("$LATEST")
            .to_string();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.functions.contains_key(function_name) {
            return Err(not_found("Function", function_name));
        }
        let alias_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}:{}",
            state.region, state.account_id, function_name, name
        );
        let alias = FunctionAlias {
            alias_arn: alias_arn.clone(),
            name: name.clone(),
            function_version: version,
            description: body["Description"].as_str().unwrap_or("").to_string(),
            revision_id: id_from_time("rev-"),
            routing_config: body.get("RoutingConfig").cloned(),
        };
        state
            .aliases
            .insert(Self::alias_key(function_name, &name), alias.clone());
        ok(serde_json::to_value(alias).unwrap_or_default())
    }

    fn get_alias(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let alias_name = req.path_segments.get(4).cloned().unwrap_or_default();
        if alias_name.is_empty() {
            return Err(missing("Name"));
        }
        if alias_name.chars().count() > 128 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Alias name exceeds the 128-character maximum",
            ));
        }
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            state
                .aliases
                .get(&Self::alias_key(function_name, &alias_name))
                .map(|a| ok(serde_json::to_value(a).unwrap_or_default()))
                .unwrap_or_else(|| Err(not_found("Alias", &alias_name)))
        })
    }

    fn list_aliases(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let prefix = format!("{function_name}:");
            let aliases: Vec<&FunctionAlias> = state
                .aliases
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, v)| v)
                .collect();
            ok(json!({"Aliases": aliases}))
        })
    }

    fn update_alias(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let alias_name = req.path_segments.get(4).cloned().unwrap_or_default();
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = Self::alias_key(function_name, &alias_name);
        let alias = state
            .aliases
            .get_mut(&key)
            .ok_or_else(|| not_found("Alias", &alias_name))?;
        if let Some(v) = body["FunctionVersion"].as_str() {
            alias.function_version = v.to_string();
        }
        if let Some(d) = body["Description"].as_str() {
            alias.description = d.to_string();
        }
        if let Some(rc) = body.get("RoutingConfig") {
            alias.routing_config = Some(rc.clone());
        }
        alias.revision_id = id_from_time("rev-");
        ok(serde_json::to_value(alias).unwrap_or_default())
    }

    fn delete_alias(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let alias_name = req.path_segments.get(4).cloned().unwrap_or_default();
        if alias_name.is_empty() {
            return Err(missing("Name"));
        }
        // Smithy `Alias.length 1..128`.
        if alias_name.chars().count() > 128 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Alias name exceeds the 128-character maximum",
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // `DeleteAlias` is idempotent on AWS — no `ResourceNotFoundException`
        // is declared on the operation. Removing without error matches
        // the live API.
        state
            .aliases
            .remove(&Self::alias_key(function_name, &alias_name));
        empty()
    }

    // ── Layers ──

    fn publish_layer_version(
        &self,
        layer_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        if layer_name.is_empty() {
            return Err(missing("LayerName"));
        }
        let limit = if layer_name.starts_with("arn:") {
            200
        } else {
            140
        };
        if layer_name.chars().count() > limit {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "LayerName exceeds the 140-character maximum",
            ));
        }
        let body = body(req);
        // `Content` is `@required` on `PublishLayerVersionRequest` —
        // reject when missing rather than silently publishing a
        // zero-byte layer.
        if body.get("Content").is_none() || body["Content"].is_null() {
            return Err(missing("Content"));
        }
        // `Description` is bound to Smithy's `Description` shape
        // (`length 0..256`). Reject overlong values up front.
        if let Some(desc) = body["Description"].as_str() {
            if desc.chars().count() > 256 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "Description exceeds the 256-character maximum",
                ));
            }
        }
        // `LicenseInfo` Smithy shape: `length 0..512`.
        if let Some(li) = body["LicenseInfo"].as_str() {
            if li.chars().count() > 512 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "LicenseInfo exceeds the 512-character maximum",
                ));
            }
        }
        let zip_bytes: Option<Vec<u8>> = match body["Content"]["ZipFile"].as_str() {
            Some(b64) => Some(
                base64::Engine::decode(&base64::engine::general_purpose::STANDARD, b64).map_err(
                    |_| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValueException",
                            "Could not decode Content.ZipFile: invalid base64",
                        )
                    },
                )?,
            ),
            None => None,
        };
        let (code_sha256, code_size) = match zip_bytes.as_deref() {
            Some(bytes) => {
                let mut hasher = Sha256::new();
                hasher.update(bytes);
                let digest = hasher.finalize();
                (
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, digest),
                    bytes.len() as i64,
                )
            }
            None => (String::new(), 0),
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let account_id = state.account_id.clone();
        let layer = state
            .layers
            .entry(layer_name.to_string())
            .or_insert_with(|| Layer {
                layer_name: layer_name.to_string(),
                layer_arn: format!(
                    "arn:aws:lambda:{}:{}:layer:{}",
                    state.region, state.account_id, layer_name
                ),
                versions: Vec::new(),
            });
        let next_version = (layer.versions.len() as i64) + 1;
        let version_arn = format!("{}:{}", layer.layer_arn, next_version);
        let runtimes: Vec<String> = body["CompatibleRuntimes"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let architectures: Vec<String> = body["CompatibleArchitectures"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let layer_arn = layer.layer_arn.clone();
        let lv = LayerVersion {
            version: next_version,
            layer_version_arn: version_arn.clone(),
            description: body["Description"].as_str().unwrap_or("").to_string(),
            created_date: Utc::now(),
            compatible_runtimes: runtimes,
            license_info: body["LicenseInfo"].as_str().unwrap_or("").to_string(),
            policy: None,
            code_zip: zip_bytes,
            code_sha256: code_sha256.clone(),
            code_size,
            compatible_architectures: architectures,
        };
        layer.versions.push(lv.clone());
        let location = layer_content_url(req, &account_id, layer_name, next_version);
        ok(json!({
            "LayerArn": layer_arn,
            "LayerVersionArn": version_arn,
            "Version": next_version,
            "Description": lv.description,
            "CreatedDate": lv.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
            "CompatibleRuntimes": lv.compatible_runtimes,
            "CompatibleArchitectures": lv.compatible_architectures,
            "LicenseInfo": lv.license_info,
            "Content": {
                "Location": location,
                "CodeSha256": code_sha256,
                "CodeSize": code_size,
            },
        }))
    }

    fn list_layers(&self, account_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let layers: Vec<Value> = state
                .layers
                .values()
                .map(|l| {
                    json!({
                        "LayerName": l.layer_name,
                        "LayerArn": l.layer_arn,
                        "LatestMatchingVersion": l.versions.last().map(|v| json!({
                            "LayerVersionArn": v.layer_version_arn,
                            "Version": v.version,
                            "Description": v.description,
                            "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                            "CompatibleRuntimes": v.compatible_runtimes,
                            "CompatibleArchitectures": v.compatible_architectures,
                        })),
                    })
                })
                .collect();
            ok(json!({"Layers": layers}))
        })
    }

    fn list_layer_versions(
        &self,
        layer_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let versions: Vec<Value> = state
                .layers
                .get(layer_name)
                .map(|l| {
                    l.versions
                        .iter()
                        .map(|v| {
                            json!({
                                "LayerVersionArn": v.layer_version_arn,
                                "Version": v.version,
                                "Description": v.description,
                                "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                                "CompatibleRuntimes": v.compatible_runtimes,
                                "CompatibleArchitectures": v.compatible_architectures,
                                "LicenseInfo": v.license_info,
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            ok(json!({"LayerVersions": versions}))
        })
    }

    fn get_layer_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        let version: i64 = req
            .path_segments
            .get(4)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| missing("VersionNumber"))?;
        let region = self.region_for(&req.account_id);
        let location = layer_content_url(req, &req.account_id, &layer_name, version);
        self.with_state_read(&req.account_id, &region, |state| {
            state
                .layers
                .get(&layer_name)
                .and_then(|l| l.versions.iter().find(|v| v.version == version))
                .map(|v| {
                    ok(json!({
                        "LayerVersionArn": v.layer_version_arn,
                        "Version": v.version,
                        "Description": v.description,
                        "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                        "CompatibleRuntimes": v.compatible_runtimes,
                        "CompatibleArchitectures": v.compatible_architectures,
                        "LicenseInfo": v.license_info,
                        "Content": {
                            "Location": location,
                            "CodeSha256": v.code_sha256,
                            "CodeSize": v.code_size,
                        },
                    }))
                })
                .unwrap_or_else(|| Err(not_found("LayerVersion", &layer_name)))
        })
    }

    fn get_layer_version_by_arn(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = req
            .query_params
            .get("Arn")
            .or_else(|| req.query_params.get("find"))
            .cloned()
            .unwrap_or_default();
        let (account_id, layer_name, version) =
            parse_layer_version_arn(&arn).ok_or_else(|| missing("Arn"))?;
        let region = self.region_for(&account_id);
        let location = layer_content_url(req, &account_id, &layer_name, version);
        self.with_state_read(&account_id, &region, |state| {
            state
                .layers
                .get(&layer_name)
                .and_then(|l| l.versions.iter().find(|v| v.version == version))
                .map(|v| {
                    ok(json!({
                        "LayerVersionArn": v.layer_version_arn,
                        "Version": v.version,
                        "Description": v.description,
                        "CreatedDate": v.created_date.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                        "CompatibleRuntimes": v.compatible_runtimes,
                        "CompatibleArchitectures": v.compatible_architectures,
                        "LicenseInfo": v.license_info,
                        "Content": {
                            "Location": location,
                            "CodeSha256": v.code_sha256,
                            "CodeSize": v.code_size,
                        },
                    }))
                })
                .unwrap_or_else(|| Err(not_found("LayerVersion", &arn)))
        })
    }

    fn delete_layer_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        if layer_name.is_empty() {
            return Err(missing("LayerName"));
        }
        let limit = if layer_name.starts_with("arn:") {
            200
        } else {
            140
        };
        if layer_name.chars().count() > limit {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "LayerName exceeds the 140-character maximum",
            ));
        }
        let version_raw = req.path_segments.get(4).map(|s| s.as_str()).unwrap_or("");
        if version_raw.is_empty() {
            return Err(missing("VersionNumber"));
        }
        let version: i64 = version_raw.parse().map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "VersionNumber must be an integer",
            )
        })?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(layer) = state.layers.get_mut(&layer_name) {
            layer.versions.retain(|v| v.version != version);
        }
        empty()
    }

    fn get_layer_version_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        let version: i64 = req
            .path_segments
            .get(4)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            let policy = state
                .layers
                .get(&layer_name)
                .and_then(|l| l.versions.iter().find(|v| v.version == version))
                .and_then(|v| v.policy.clone())
                .unwrap_or_else(|| "{}".to_string());
            ok(json!({"Policy": policy, "RevisionId": id_from_time("rev-")}))
        })
    }

    fn add_layer_version_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        let version: i64 = req
            .path_segments
            .get(4)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(layer) = state.layers.get_mut(&layer_name) {
            if let Some(v) = layer.versions.iter_mut().find(|v| v.version == version) {
                let policy = v.policy.clone().unwrap_or_else(|| "{}".to_string());
                let mut policy_doc: Value = serde_json::from_str(&policy).unwrap_or(json!({}));
                let statements = policy_doc["Statement"].as_array_mut();
                let new_stmt = json!({
                    "Sid": body["StatementId"].as_str().unwrap_or("default"),
                    "Effect": "Allow",
                    "Principal": body["Principal"].clone(),
                    "Action": body["Action"].clone(),
                    "Resource": v.layer_version_arn.clone(),
                });
                if let Some(s) = statements {
                    s.push(new_stmt);
                } else {
                    policy_doc = json!({"Version": "2012-10-17", "Statement": [new_stmt]});
                }
                v.policy = Some(policy_doc.to_string());
            }
        }
        ok(json!({
            "Statement": body["StatementId"],
            "RevisionId": id_from_time("rev-"),
        }))
    }

    fn remove_layer_version_permission(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let layer_name = req.path_segments.get(2).cloned().unwrap_or_default();
        let version: i64 = req
            .path_segments
            .get(4)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let sid = req.path_segments.get(6).cloned().unwrap_or_default();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(layer) = state.layers.get_mut(&layer_name) {
            if let Some(v) = layer.versions.iter_mut().find(|v| v.version == version) {
                if let Some(policy) = v.policy.clone() {
                    let mut policy_doc: Value = serde_json::from_str(&policy).unwrap_or(json!({}));
                    if let Some(stmts) = policy_doc["Statement"].as_array_mut() {
                        stmts.retain(|s| s["Sid"].as_str() != Some(&sid));
                    }
                    v.policy = Some(policy_doc.to_string());
                }
            }
        }
        empty()
    }

    // ── Function URL ──

    /// Render a `FunctionUrlConfig` into the AWS-shaped JSON the Lambda
    /// SDK expects (PascalCase keys, ISO-8601 timestamps). Direct
    /// `serde_json::to_value` would emit the struct's snake_case field
    /// names, which the SDK silently treats as missing fields — leaving
    /// `function_url()` returning an empty string.
    fn function_url_config_json(cfg: &FunctionUrlConfig) -> Value {
        let mut out = json!({
            "FunctionArn": cfg.function_arn,
            "FunctionUrl": cfg.function_url,
            "AuthType": cfg.auth_type,
            "InvokeMode": cfg.invoke_mode,
            "CreationTime": cfg.creation_time.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
            "LastModifiedTime": cfg.last_modified_time.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
        });
        if let Some(cors) = &cfg.cors {
            out["Cors"] = cors.clone();
        }
        out
    }

    fn create_function_url_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let auth_type = body["AuthType"]
            .as_str()
            .ok_or_else(|| missing("AuthType"))?
            .to_string();
        // `FunctionUrlAuthType` enum: `NONE` | `AWS_IAM`. Reject any
        // other value rather than persisting an unrecognised auth type.
        if auth_type != "NONE" && auth_type != "AWS_IAM" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "Invalid AuthType value '{}'; expected 'NONE' or 'AWS_IAM'",
                    auth_type
                ),
            ));
        }
        let now = Utc::now();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if !state.functions.contains_key(function_name) {
            return Err(not_found("Function", function_name));
        }
        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            state.region, state.account_id, function_name
        );
        let cfg = FunctionUrlConfig {
            function_arn: function_arn.clone(),
            function_url: format!(
                "https://{function_name}.lambda-url.{}.on.aws/",
                state.region
            ),
            auth_type: auth_type.clone(),
            cors: body.get("Cors").cloned(),
            creation_time: now,
            last_modified_time: now,
            invoke_mode: {
                let m = body["InvokeMode"]
                    .as_str()
                    .unwrap_or("BUFFERED")
                    .to_string();
                if m != "BUFFERED" && m != "RESPONSE_STREAM" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValueException",
                        format!(
                            "Invalid InvokeMode value '{}'; expected 'BUFFERED' or 'RESPONSE_STREAM'",
                            m
                        ),
                    ));
                }
                m
            },
        };
        state
            .function_url_configs
            .insert(function_name.to_string(), cfg.clone());
        // `CreateFunctionUrlConfigResponse` lacks `LastModifiedTime` —
        // that member only appears on `Get`/`Update` responses. Strip it
        // before returning so strict shape validators don't reject it.
        let mut created = Self::function_url_config_json(&cfg);
        if let Some(obj) = created.as_object_mut() {
            obj.remove("LastModifiedTime");
        }
        ok(created)
    }

    fn get_function_url_config(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            state
                .function_url_configs
                .get(function_name)
                .map(|c| ok(Self::function_url_config_json(c)))
                .unwrap_or_else(|| Err(not_found("FunctionUrlConfig", function_name)))
        })
    }

    fn update_function_url_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cfg = state
            .function_url_configs
            .get_mut(function_name)
            .ok_or_else(|| not_found("FunctionUrlConfig", function_name))?;
        if let Some(a) = body["AuthType"].as_str() {
            cfg.auth_type = a.to_string();
        }
        if let Some(c) = body.get("Cors") {
            cfg.cors = Some(c.clone());
        }
        if let Some(m) = body["InvokeMode"].as_str() {
            cfg.invoke_mode = m.to_string();
        }
        cfg.last_modified_time = Utc::now();
        let snapshot = cfg.clone();
        ok(Self::function_url_config_json(&snapshot))
    }

    fn delete_function_url_config(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.function_url_configs.remove(function_name);
        empty()
    }

    fn list_function_url_configs(&self, account_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let configs: Vec<Value> = state
                .function_url_configs
                .values()
                .map(Self::function_url_config_json)
                .collect();
            ok(json!({"FunctionUrlConfigs": configs}))
        })
    }

    // ── Concurrency ──

    fn put_function_concurrency(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let n = body["ReservedConcurrentExecutions"]
            .as_i64()
            .ok_or_else(|| missing("ReservedConcurrentExecutions"))?;
        // Smithy `range(min=0)` — negative values are invalid.
        if n < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("ReservedConcurrentExecutions must be >= 0 (got {})", n),
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .function_concurrency
            .insert(function_name.to_string(), n);
        ok(json!({"ReservedConcurrentExecutions": n}))
    }

    fn get_function_concurrency(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let n = state
                .function_concurrency
                .get(function_name)
                .copied()
                .unwrap_or(0);
            ok(json!({"ReservedConcurrentExecutions": n}))
        })
    }

    fn delete_function_concurrency(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.function_concurrency.remove(function_name);
        empty()
    }

    fn pc_key(function: &str, qualifier: &str) -> String {
        format!("{function}:{qualifier}")
    }

    fn put_provisioned_concurrency(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let qualifier = require_qualifier(req)?;
        let requested = body["ProvisionedConcurrentExecutions"]
            .as_i64()
            .ok_or_else(|| missing("ProvisionedConcurrentExecutions"))?;
        // Smithy `range(min=1)` — zero and negatives are invalid.
        if requested < 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "ProvisionedConcurrentExecutions must be >= 1 (got {})",
                    requested
                ),
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let cfg = ProvisionedConcurrencyConfig {
            requested,
            allocated: requested,
            status: "READY".to_string(),
            last_modified: Utc::now(),
        };
        state
            .provisioned_concurrency
            .insert(Self::pc_key(function_name, &qualifier), cfg.clone());
        ok(json!({
            "RequestedProvisionedConcurrentExecutions": cfg.requested,
            "AvailableProvisionedConcurrentExecutions": cfg.allocated,
            "AllocatedProvisionedConcurrentExecutions": cfg.allocated,
            "Status": cfg.status,
            "LastModified": cfg.last_modified.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
        }))
    }

    fn get_provisioned_concurrency(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = require_qualifier(req)?;
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            state
                .provisioned_concurrency
                .get(&Self::pc_key(function_name, &qualifier))
                .map(|cfg| ok(json!({
                    "RequestedProvisionedConcurrentExecutions": cfg.requested,
                    "AvailableProvisionedConcurrentExecutions": cfg.allocated,
                    "AllocatedProvisionedConcurrentExecutions": cfg.allocated,
                    "Status": cfg.status,
                    "LastModified": cfg.last_modified.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                })))
                .unwrap_or_else(|| Err(not_found("ProvisionedConcurrencyConfig", function_name)))
        })
    }

    fn delete_provisioned_concurrency(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = require_qualifier(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .provisioned_concurrency
            .remove(&Self::pc_key(function_name, &qualifier));
        empty()
    }

    fn list_provisioned_concurrency(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let prefix = format!("{function_name}:");
            let configs: Vec<Value> = state
                .provisioned_concurrency
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(k, cfg)| {
                    let qualifier = k.split(':').next_back().unwrap_or("$LATEST");
                    json!({
                        "FunctionArn": format!(
                            "arn:aws:lambda:{}:{}:function:{}:{}",
                            state.region, state.account_id, function_name, qualifier
                        ),
                        "Status": cfg.status,
                        "RequestedProvisionedConcurrentExecutions": cfg.requested,
                        "AvailableProvisionedConcurrentExecutions": cfg.allocated,
                        "AllocatedProvisionedConcurrentExecutions": cfg.allocated,
                        "LastModified": cfg.last_modified.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
                    })
                })
                .collect();
            ok(json!({"ProvisionedConcurrencyConfigs": configs}))
        })
    }

    // ── Code signing ──

    fn create_code_signing_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let id = id_from_time("csc-");
        let arn = format!(
            "arn:aws:lambda:{}:{}:code-signing-config:{}",
            state.region, state.account_id, id
        );
        let publishers: Vec<String> = body
            .get("AllowedPublishers")
            .and_then(|v| v.get("SigningProfileVersionArns"))
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let csc = CodeSigningConfig {
            csc_id: id.clone(),
            csc_arn: arn,
            description: body["Description"].as_str().unwrap_or("").to_string(),
            allowed_publishers: publishers,
            untrusted_artifact_action: body["CodeSigningPolicies"]["UntrustedArtifactOnDeployment"]
                .as_str()
                .unwrap_or("Warn")
                .to_string(),
            last_modified: Utc::now(),
        };
        state.code_signing_configs.insert(id, csc.clone());
        ok(json!({"CodeSigningConfig": code_signing_json(&csc)}))
    }

    fn get_code_signing_config(
        &self,
        csc_id: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = extract_csc_id(csc_id);
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            state
                .code_signing_configs
                .get(&id)
                .map(|c| ok(json!({"CodeSigningConfig": code_signing_json(c)})))
                .unwrap_or_else(|| Err(not_found("CodeSigningConfig", &id)))
        })
    }

    fn update_code_signing_config(
        &self,
        csc_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let id = extract_csc_id(csc_id);
        let csc = state
            .code_signing_configs
            .get_mut(&id)
            .ok_or_else(|| not_found("CodeSigningConfig", &id))?;
        if let Some(d) = body["Description"].as_str() {
            csc.description = d.to_string();
        }
        if let Some(action) = body["CodeSigningPolicies"]["UntrustedArtifactOnDeployment"].as_str()
        {
            csc.untrusted_artifact_action = action.to_string();
        }
        csc.last_modified = Utc::now();
        ok(json!({"CodeSigningConfig": code_signing_json(csc)}))
    }

    fn delete_code_signing_config(
        &self,
        csc_id: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = extract_csc_id(csc_id);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.code_signing_configs.remove(&id);
        empty()
    }

    fn list_code_signing_configs(&self, account_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let cfgs: Vec<Value> = state
                .code_signing_configs
                .values()
                .map(code_signing_json)
                .collect();
            ok(json!({"CodeSigningConfigs": cfgs}))
        })
    }

    fn put_function_code_signing(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let csc_arn = body["CodeSigningConfigArn"]
            .as_str()
            .ok_or_else(|| missing("CodeSigningConfigArn"))?
            .to_string();
        // Smithy length bound: max 200. Reject overlong inputs rather
        // than persisting a malformed ARN.
        if csc_arn.chars().count() > 200 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "CodeSigningConfigArn exceeds the 200-character maximum",
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .function_code_signing
            .insert(function_name.to_string(), csc_arn.clone());
        ok(json!({
            "CodeSigningConfigArn": csc_arn,
            "FunctionName": function_name,
        }))
    }

    fn get_function_code_signing(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let arn = state
                .function_code_signing
                .get(function_name)
                .cloned()
                .unwrap_or_default();
            ok(json!({
                "CodeSigningConfigArn": arn,
                "FunctionName": function_name,
            }))
        })
    }

    fn delete_function_code_signing(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(account_id);
        state.function_code_signing.remove(function_name);
        empty()
    }

    fn list_functions_by_code_signing(
        &self,
        csc_id: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = extract_csc_id(csc_id);
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let funcs: Vec<&String> = state
                .function_code_signing
                .iter()
                .filter(|(_, v)| v.contains(&id))
                .map(|(k, _)| k)
                .collect();
            ok(json!({"FunctionArns": funcs}))
        })
    }

    // ── Event invoke ──

    fn ev_key(function: &str, qualifier: &str) -> String {
        format!("{function}:{qualifier}")
    }

    fn put_function_event_invoke(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let qualifier = parse_qualifier(req);
        let function_arn = format!(
            "arn:aws:lambda:{}:{}:function:{}",
            self.region_for(&req.account_id),
            req.account_id,
            function_name
        );
        // Validate Smithy ranges before persisting:
        //   MaximumEventAgeInSeconds: 60..=21600
        //   MaximumRetryAttempts:     0..=2
        let event_age = body["MaximumEventAgeInSeconds"].as_i64().unwrap_or(21600);
        if !(60..=21600).contains(&event_age) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "MaximumEventAgeInSeconds must be 60..21600 (got {})",
                    event_age
                ),
            ));
        }
        let retries = body["MaximumRetryAttempts"].as_i64().unwrap_or(2);
        if !(0..=2).contains(&retries) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("MaximumRetryAttempts must be 0..2 (got {})", retries),
            ));
        }
        let cfg = EventInvokeConfig {
            function_arn: function_arn.clone(),
            maximum_event_age: event_age,
            maximum_retry_attempts: retries,
            destination_config: body.get("DestinationConfig").cloned(),
            last_modified: Utc::now(),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .event_invoke_configs
            .insert(Self::ev_key(function_name, &qualifier), cfg.clone());
        ok(event_invoke_json(&cfg))
    }

    fn get_function_event_invoke(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = parse_qualifier(req);
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            state
                .event_invoke_configs
                .get(&Self::ev_key(function_name, &qualifier))
                .map(|c| ok(event_invoke_json(c)))
                .unwrap_or_else(|| Err(not_found("EventInvokeConfig", function_name)))
        })
    }

    fn delete_function_event_invoke(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = parse_qualifier(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .event_invoke_configs
            .remove(&Self::ev_key(function_name, &qualifier));
        empty()
    }

    fn list_function_event_invoke(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let prefix = format!("{function_name}:");
            let configs: Vec<Value> = state
                .event_invoke_configs
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, c)| event_invoke_json(c))
                .collect();
            ok(json!({"FunctionEventInvokeConfigs": configs}))
        })
    }

    // ── Runtime management ──

    fn put_runtime_management(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let qualifier = parse_qualifier(req);
        // `UpdateRuntimeOn` is `@required` in the model; reject the
        // request rather than silently defaulting to `Auto`.
        let update_runtime_on = body["UpdateRuntimeOn"]
            .as_str()
            .ok_or_else(|| missing("UpdateRuntimeOn"))?
            .to_string();
        // `UpdateRuntimeOn` enum: Auto | Manual | FunctionUpdate.
        if !matches!(
            update_runtime_on.as_str(),
            "Auto" | "Manual" | "FunctionUpdate"
        ) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "Invalid UpdateRuntimeOn value '{}'; expected 'Auto', 'Manual', or 'FunctionUpdate'",
                    update_runtime_on
                ),
            ));
        }
        let runtime_version_arn = body["RuntimeVersionArn"].as_str().unwrap_or("").to_string();
        // `RuntimeVersionArn` Smithy shape: length 26..2048. Empty
        // means "unset" (valid); any non-empty value must satisfy the
        // minimum.
        if !runtime_version_arn.is_empty()
            && (runtime_version_arn.chars().count() < 26
                || runtime_version_arn.chars().count() > 2048)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "RuntimeVersionArn must be 26..2048 characters",
            ));
        }
        let cfg = RuntimeManagementConfig {
            update_runtime_on,
            runtime_version_arn,
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .runtime_management
            .insert(format!("{function_name}:{qualifier}"), cfg.clone());
        ok(json!({
            "FunctionArn": Arn::new("lambda", &state.region, &state.account_id, &format!("function:{function_name}:{qualifier}")).to_string(),
            "UpdateRuntimeOn": cfg.update_runtime_on,
            "RuntimeVersionArn": cfg.runtime_version_arn,
        }))
    }

    fn get_runtime_management(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let qualifier = parse_qualifier(req);
        let region = self.region_for(&req.account_id);
        self.with_state_read(&req.account_id, &region, |state| {
            let cfg = state
                .runtime_management
                .get(&format!("{function_name}:{qualifier}"))
                .cloned()
                .unwrap_or(RuntimeManagementConfig {
                    update_runtime_on: "Auto".to_string(),
                    runtime_version_arn: String::new(),
                });
            ok(json!({
                "FunctionArn": format!(
                    "arn:aws:lambda:{}:{}:function:{}:{}",
                    state.region, state.account_id, function_name, qualifier
                ),
                "UpdateRuntimeOn": cfg.update_runtime_on,
                "RuntimeVersionArn": cfg.runtime_version_arn,
            }))
        })
    }

    // ── Scaling ──

    fn put_scaling_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _qualifier = require_qualifier(req)?;
        let body = body(req);
        let inner = body
            .get("FunctionScalingConfig")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let cfg = FunctionScalingConfig {
            min_execution_environments: inner["MinExecutionEnvironments"].as_i64(),
            max_execution_environments: inner["MaxExecutionEnvironments"].as_i64(),
        };
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.scaling_configs.insert(function_name.to_string(), cfg);
        // `PutFunctionScalingConfigResponse` only carries `FunctionState`
        // (the post-update steady state). Pending → ready is instant in
        // fakecloud since there's no real fleet to scale.
        ok(json!({ "FunctionState": "Ready" }))
    }

    fn get_scaling_config(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Caller validates `Qualifier` via `require_qualifier` before
        // delegating here; reads don't need it post-validation since
        // scaling config is per-function in fakecloud.
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let cfg = state
                .scaling_configs
                .get(function_name)
                .cloned()
                .unwrap_or_default();
            let mut applied = serde_json::Map::new();
            if let Some(v) = cfg.min_execution_environments {
                applied.insert("MinExecutionEnvironments".into(), json!(v));
            }
            if let Some(v) = cfg.max_execution_environments {
                applied.insert("MaxExecutionEnvironments".into(), json!(v));
            }
            let function_arn = format!(
                "arn:aws:lambda:{}:{}:function:{}",
                state.region, state.account_id, function_name
            );
            ok(json!({
                "FunctionArn": function_arn,
                "AppliedFunctionScalingConfig": Value::Object(applied.clone()),
                "RequestedFunctionScalingConfig": Value::Object(applied),
            }))
        })
    }

    // ── Recursion ──

    fn put_recursion_config(
        &self,
        function_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        // `RecursiveLoop` is `@required` on the model — reject missing
        // values instead of defaulting silently to `Terminate`. The
        // enum admits only `Allow` and `Terminate`.
        let mode = body["RecursiveLoop"]
            .as_str()
            .ok_or_else(|| missing("RecursiveLoop"))?
            .to_string();
        if mode != "Allow" && mode != "Terminate" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!(
                    "Invalid RecursiveLoop value '{}'; expected 'Allow' or 'Terminate'",
                    mode
                ),
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .recursion_configs
            .insert(function_name.to_string(), mode.clone());
        ok(json!({"RecursiveLoop": mode}))
    }

    fn get_recursion_config(
        &self,
        function_name: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let mode = state
                .recursion_configs
                .get(function_name)
                .cloned()
                .unwrap_or_else(|| "Terminate".to_string());
            ok(json!({"RecursiveLoop": mode}))
        })
    }

    // ── Tags ──

    fn tag_resource(
        &self,
        resource_arn: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let new_tags: Vec<(String, String)> = body
            .get("Tags")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        // SDKs URL-encode `:` in the path so the ARN arrives as
        // `arn%3Aaws%3Alambda%3A...`; decode before parsing.
        let resource_arn_decoded = decode_query_segment(resource_arn);
        let name = function_name_from_arn(&resource_arn_decoded).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("Resource ARN is not a Lambda function: {resource_arn_decoded}"),
            )
        })?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let func = state.functions.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Function not found: {name}"),
            )
        })?;
        // Single source of truth: per-function `tags`. `GetFunction`,
        // `ListTagsForResource`, and `UntagResource` all read here.
        for (k, v) in new_tags {
            func.tags.insert(k, v);
        }
        empty()
    }

    fn untag_resource(
        &self,
        resource_arn: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // AWS sends keys as repeated `tagKeys=K1&tagKeys=K2` query
        // params per the Smithy model (`httpQuery: "tagKeys"`). The
        // dispatcher's deduplicated `query_params` HashMap collapses
        // repeats, so parse the raw query string for every occurrence.
        // Also accept `tagKeys.1=K1` / `tagKeys.member.1=K1` for SDKs
        // that serialize list params indexed-style.
        //
        // As a defensive fallback we also accept a JSON body of the
        // form `{"TagKeys": [...]}` / `{"tagKeys": [...]}` for clients
        // that mistakenly send the tag keys in the body. Query
        // parameters win when both are present, since query is the
        // AWS-canonical wire format.
        let mut keys: Vec<String> = Vec::new();
        for (k, v) in parse_query_pairs(&req.raw_query) {
            if k == "tagKeys" || k.starts_with("tagKeys.") {
                keys.push(v);
            }
        }
        if keys.is_empty() {
            let parsed = body(req);
            for field in ["TagKeys", "tagKeys"] {
                if let Some(arr) = parsed.get(field).and_then(|v| v.as_array()) {
                    for v in arr {
                        if let Some(s) = v.as_str() {
                            keys.push(s.to_string());
                        }
                    }
                    if !keys.is_empty() {
                        break;
                    }
                }
            }
        }
        let resource_arn_decoded = decode_query_segment(resource_arn);
        let name = function_name_from_arn(&resource_arn_decoded).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("Resource ARN is not a Lambda function: {resource_arn_decoded}"),
            )
        })?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let func = state.functions.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Function not found: {name}"),
            )
        })?;
        for k in &keys {
            func.tags.remove(k);
        }
        empty()
    }

    fn list_tags(
        &self,
        resource_arn: &str,
        account_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_arn_decoded = decode_query_segment(resource_arn);
        let name = function_name_from_arn(&resource_arn_decoded).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                format!("Resource ARN is not a Lambda function: {resource_arn_decoded}"),
            )
        })?;
        let region = self.region_for(account_id);
        self.with_state_read(account_id, &region, |state| {
            let func = state.functions.get(&name).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!("Function not found: {name}"),
                )
            })?;
            let tags: serde_json::Map<String, Value> = func
                .tags
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect();
            ok(json!({"Tags": tags}))
        })
    }

    // ── Capacity providers ──

    fn update_event_source_mapping_handler(
        &self,
        uuid: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = body(req);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let esm = state
            .event_source_mappings
            .get_mut(uuid)
            .ok_or_else(|| not_found("EventSourceMapping", uuid))?;
        if let Some(b) = body["BatchSize"].as_i64() {
            esm.batch_size = b;
        }
        if let Some(name) = body["FunctionName"].as_str() {
            esm.function_arn = format!(
                "arn:aws:lambda:{}:{}:function:{}",
                state.region, state.account_id, name
            );
        }
        if let Some(filters) = body
            .get("FilterCriteria")
            .and_then(|v| v.get("Filters"))
            .and_then(|v| v.as_array())
        {
            esm.filter_patterns = filters
                .iter()
                .filter_map(|f| f.get("Pattern").and_then(|p| p.as_str()).map(String::from))
                .collect();
        }
        if let Some(types) = body.get("FunctionResponseTypes").and_then(|v| v.as_array()) {
            esm.function_response_types = types
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(w) = body
            .get("MaximumBatchingWindowInSeconds")
            .and_then(|v| v.as_i64())
        {
            esm.maximum_batching_window_in_seconds = Some(w);
        }
        if let Some(p) = body.get("ParallelizationFactor").and_then(|v| v.as_i64()) {
            esm.parallelization_factor = Some(p);
        }
        if let Some(s) = body.get("KMSKeyArn").and_then(|v| v.as_str()) {
            esm.kms_key_arn = Some(s.to_string());
        }
        if let Some(mc) = body.get("MetricsConfig") {
            esm.metrics_config = Some(mc.clone());
        }
        if let Some(dc) = body.get("DestinationConfig") {
            esm.destination_config = Some(dc.clone());
        }
        if let Some(n) = body.get("MaximumRetryAttempts").and_then(|v| v.as_i64()) {
            esm.maximum_retry_attempts = Some(n);
        }
        if let Some(n) = body
            .get("MaximumRecordAgeInSeconds")
            .and_then(|v| v.as_i64())
        {
            esm.maximum_record_age_in_seconds = Some(n);
        }
        if let Some(b) = body
            .get("BisectBatchOnFunctionError")
            .and_then(|v| v.as_bool())
        {
            esm.bisect_batch_on_function_error = Some(b);
        }
        if let Some(n) = body.get("TumblingWindowInSeconds").and_then(|v| v.as_i64()) {
            esm.tumbling_window_in_seconds = Some(n);
        }
        let mut body_json = json!({
            "UUID": esm.uuid,
            "FunctionArn": esm.function_arn,
            "EventSourceArn": esm.event_source_arn,
            "BatchSize": esm.batch_size,
            "State": "Enabled",
            "StateTransitionReason": "USER_INITIATED",
            "LastModified": chrono::Utc::now().timestamp() as f64,
        });
        let obj = body_json.as_object_mut().expect("json! built object");
        if !esm.filter_patterns.is_empty() {
            obj.insert(
                "FilterCriteria".into(),
                json!({
                    "Filters": esm
                        .filter_patterns
                        .iter()
                        .map(|p| json!({"Pattern": p}))
                        .collect::<Vec<_>>(),
                }),
            );
        }
        if !esm.function_response_types.is_empty() {
            obj.insert(
                "FunctionResponseTypes".into(),
                json!(esm.function_response_types),
            );
        }
        if let Some(w) = esm.maximum_batching_window_in_seconds {
            obj.insert("MaximumBatchingWindowInSeconds".into(), json!(w));
        }
        if let Some(p) = esm.parallelization_factor {
            obj.insert("ParallelizationFactor".into(), json!(p));
        }
        ok(body_json)
    }

    fn region_for(&self, account_id: &str) -> String {
        let accounts = self.state.read();
        accounts
            .get(account_id)
            .map(|s| s.region.clone())
            .unwrap_or_else(|| "us-east-1".to_string())
    }

    /// `InvokeWithResponseStream` — invoke the function and serialize
    /// its response as a sequence of `application/vnd.amazon.eventstream`
    /// frames. AWS uses this protocol for response-streaming Lambda
    /// invocations (Node.js `awslambda.streamifyResponse`, Python
    /// streaming handlers, custom runtimes that flush mid-handler).
    ///
    /// On success: zero or more `PayloadChunk` events (one per chunk
    /// the RIE flushed) followed by an `InvokeComplete` event with
    /// `ErrorCode = null`. On a function error (uncaught exception in
    /// the handler) or an infrastructure error (timeout, container
    /// crash): an `InvokeComplete` with non-null `ErrorCode`/
    /// `ErrorDetails`. The HTTP status itself is always 200 — failures
    /// surface inside the trailing event, matching AWS.
    pub(crate) async fn invoke_with_response_stream(
        &self,
        function_name: &str,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Resolve the function under the same rules as buffered Invoke
        // — qualifier, version snapshots, attached layers, code-zip
        // presence — but without the InvocationType branch (streaming
        // is always synchronous).
        let qualifier = req.query_params.get("Qualifier").map(String::as_str);

        let resolved_version: Option<String> = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, "");
            let state = accounts.get(account_id).unwrap_or(&empty);
            crate::service::resolve_qualifier_to_version(state, function_name, qualifier)
        };
        let executed_version = resolved_version
            .clone()
            .unwrap_or_else(|| "$LATEST".to_string());

        let (func, layer_zips) = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, "");
            let state = accounts.get(account_id).unwrap_or(&empty);
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
            let mut zips: Vec<Vec<u8>> = Vec::with_capacity(func.layers.len());
            for attached in &func.layers {
                if let Some(b) =
                    parse_layer_version_arn(&attached.arn).and_then(|(acct, name, ver)| {
                        accounts
                            .get(&acct)
                            .and_then(|s| s.layers.get(&name))
                            .and_then(|l| l.versions.iter().find(|v| v.version == ver))
                            .and_then(|v| v.code_zip.clone())
                    })
                {
                    zips.push(b);
                }
            }
            (func, zips)
        };

        if func.code_zip.is_none() && func.package_type != "Image" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValueException",
                "Function has no deployment package",
            ));
        }

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "ServiceException",
                "Docker/Podman is required for Lambda execution but is not available",
            )
        })?;

        // Drive the streaming RIE call and assemble the eventstream
        // body. We buffer all frames before returning — `AwsResponse`
        // is byte-buffered today — but the chunk boundaries the RIE
        // flushed are preserved as separate `PayloadChunk` events, so
        // SDK parsers see exactly the streaming structure they expect.
        let mut frames: Vec<u8> = Vec::new();
        let invoke_result = runtime
            .invoke_streaming(&func, &req.body, &layer_zips)
            .await;

        let (error_code, error_details) = match invoke_result {
            Ok(mut stream) => {
                let mut last_chunk: Option<bytes::Bytes> = None;
                let mut had_chunks = false;
                loop {
                    match stream.next_chunk().await {
                        Ok(Some(chunk)) => {
                            had_chunks = true;
                            frames.extend_from_slice(&crate::eventstream::payload_chunk_frame(
                                &chunk,
                            ));
                            last_chunk = Some(chunk);
                        }
                        Ok(None) => break,
                        Err(e) => {
                            tracing::error!(function = %function_name, error = %e, "Lambda streaming chunk read failed");
                            return Err(AwsServiceError::aws_error(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "ServiceException",
                                format!("Lambda streaming read failed: {e}"),
                            ));
                        }
                    }
                }

                // The Lambda runtime returns 200 even when the user
                // handler threw, packing `errorMessage`/`errorType`
                // into the buffered body. Streaming handlers do the
                // same on the final chunk. Inspect the last chunk we
                // saw and surface that as a function error in the
                // terminal `InvokeComplete` event.
                let mut error: Option<(String, String)> = None;
                if had_chunks {
                    if let Some(bytes) = last_chunk {
                        if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                            if let Some(obj) = v.as_object() {
                                if obj.contains_key("errorMessage") || obj.contains_key("errorType")
                                {
                                    let etype = obj
                                        .get("errorType")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("Runtime.Unknown")
                                        .to_string();
                                    let emsg = obj
                                        .get("errorMessage")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("function error")
                                        .to_string();
                                    error = Some((etype, emsg));
                                }
                            }
                        }
                    }
                }
                match error {
                    Some((code, details)) => (Some(code), Some(details)),
                    None => (None, None),
                }
            }
            Err(e) => {
                tracing::error!(function = %function_name, error = %e, "Lambda streaming invocation failed");
                (
                    Some("Runtime.InvocationFailure".to_string()),
                    Some(e.to_string()),
                )
            }
        };

        frames.extend_from_slice(&crate::eventstream::invoke_complete_frame(
            error_code.as_deref(),
            error_details.as_deref(),
            "",
        ));

        let mut resp = AwsResponse {
            status: StatusCode::OK,
            content_type: "application/vnd.amazon.eventstream".to_string(),
            body: fakecloud_core::service::ResponseBody::Bytes(bytes::Bytes::from(frames)),
            headers: http::HeaderMap::new(),
        };
        if let Ok(v) = http::HeaderValue::from_str(&executed_version) {
            resp.headers
                .insert(http::HeaderName::from_static("x-amz-executed-version"), v);
        }
        Ok(resp)
    }
}

fn extract_csc_id(input: &str) -> String {
    // Decode percent encoding then take the segment after the last colon
    // (csc id), or treat as id if no colon present.
    let decoded = percent_decode(input);
    decoded.rsplit(':').next().unwrap_or(&decoded).to_string()
}

fn percent_decode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push(((h * 16 + l) as u8) as char);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i] as char);
        i += 1;
    }
    out
}

fn code_signing_json(c: &CodeSigningConfig) -> Value {
    json!({
        "CodeSigningConfigId": c.csc_id,
        "CodeSigningConfigArn": c.csc_arn,
        "Description": c.description,
        "AllowedPublishers": {
            "SigningProfileVersionArns": c.allowed_publishers,
        },
        "CodeSigningPolicies": {
            "UntrustedArtifactOnDeployment": c.untrusted_artifact_action,
        },
        "LastModified": c.last_modified.format("%Y-%m-%dT%H:%M:%S.%3fZ").to_string(),
    })
}

fn event_invoke_json(c: &EventInvokeConfig) -> Value {
    // `DestinationConfig` serialization mirrors the three AWS behaviours
    // documented in the Smithy `@examples` for Put/UpdateFunctionEventInvokeConfig
    // and asserted by the conformance round-trip strategy:
    //
    //   stored `None`          (input omitted entirely)
    //       -> emit `{OnSuccess:{}, OnFailure:{}}`  (Put example)
    //   stored `Some({})`      (caller sent `{}`)
    //       -> echo `{}` verbatim                    (round-trip strategy)
    //   stored `Some({half})`  (one side configured)
    //       -> backfill the other half as `{}`       (Update example)
    let destination = match &c.destination_config {
        None => json!({"OnSuccess": {}, "OnFailure": {}}),
        Some(v) if !v.is_object() => json!({}),
        Some(v) => {
            let mut map = v.as_object().cloned().unwrap_or_default();
            if !map.is_empty() {
                map.entry("OnSuccess".to_string()).or_insert(json!({}));
                map.entry("OnFailure".to_string()).or_insert(json!({}));
            }
            Value::Object(map)
        }
    };
    json!({
        "FunctionArn": c.function_arn,
        "MaximumEventAgeInSeconds": c.maximum_event_age,
        "MaximumRetryAttempts": c.maximum_retry_attempts,
        "DestinationConfig": destination,
        // `LastModified` is bound to Smithy's `Date` shape
        // (`type: timestamp`). The default REST-JSON serialization
        // for `timestamp` is an epoch-seconds float, which is what
        // `aws-sdk-lambda` deserializes; emitting an ISO string here
        // makes the SDK panic on `f64::from_str("2026-...")`.
        "LastModified": c
            .last_modified
            .timestamp_millis() as f64
            / 1000.0,
    })
}

#[cfg(test)]
mod tests {
    use crate::service::LambdaService;
    use crate::state::{LambdaState, SharedLambdaState};
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::Method;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn svc() -> LambdaService {
        let state: SharedLambdaState = Arc::new(RwLock::new(
            MultiAccountState::<LambdaState>::new("000000000000", "us-east-1", ""),
        ));
        LambdaService::new(state)
    }

    fn req(action: &str, body: &str, segs: &[&str]) -> AwsRequest {
        AwsRequest {
            service: "lambda".to_string(),
            method: Method::POST,
            raw_path: format!("/{}", segs.join("/")),
            raw_query: String::new(),
            path_segments: segs.iter().map(|s| s.to_string()).collect(),
            query_params: HashMap::new(),
            headers: http::HeaderMap::new(),
            body: bytes::Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "000000000000".to_string(),
            region: "us-east-1".to_string(),
            request_id: "rid".to_string(),
            action: action.to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn run(s: &LambdaService, action: &str, body: &str, res: Option<&str>, segs: &[&str]) {
        let r = s.handle_extra(action, res, &req(action, body, segs)).await;
        match r {
            Ok(resp) => assert!(resp.status.is_success(), "{action} status: {}", resp.status),
            Err(e) => panic!("{action} failed: {e:?}"),
        }
    }

    #[tokio::test]
    async fn read_only_listings_succeed_without_state() {
        let s = svc();
        run(&s, "GetAccountSettings", "", None, &[]).await;
        run(&s, "InvokeAsync", r#"{}"#, Some("fn"), &[]).await;
        run(&s, "ListLayers", "", None, &[]).await;
        run(&s, "ListLayerVersions", "", Some("layer"), &[]).await;
    }

    #[tokio::test]
    async fn layers_lifecycle() {
        let s = svc();
        run(
            &s,
            "PublishLayerVersion",
            r#"{"Content":{"ZipFile":""}}"#,
            Some("layer1"),
            &["2018-10-31", "layers", "layer1", "versions"],
        )
        .await;
        run(&s, "ListLayers", "", None, &[]).await;
        run(&s, "ListLayerVersions", "", Some("layer1"), &[]).await;
    }

    #[tokio::test]
    async fn code_signing_lifecycle() {
        let s = svc();
        run(
            &s,
            "CreateCodeSigningConfig",
            r#"{"AllowedPublishers":{"SigningProfileVersionArns":[]}}"#,
            None,
            &[],
        )
        .await;
        run(&s, "ListCodeSigningConfigs", "", None, &[]).await;
    }
}
