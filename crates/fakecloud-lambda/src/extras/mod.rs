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

mod account;
mod aliases;
mod code_signing;
mod concurrency;
mod event_invoke;
mod function_url;
mod layers;
mod recursion;
mod runtime;
mod stream;

impl LambdaService {
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

    // ── Versions ──

    fn list_versions_by_function(
        &self,
        function_name: &str,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let region = self.region_for(account_id);
        // ListVersionsByFunction MaxItems range is 1..10000 in the Smithy model
        // (default 50), not capped at 50.
        let max_items: usize = req
            .query_params
            .get("MaxItems")
            .and_then(|v| v.parse::<usize>().ok())
            .map(|n| n.clamp(1, 10000))
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
            // In ListVersionsByFunction, AWS qualifies the $LATEST entry's
            // FunctionArn with `:$LATEST` (unlike a bare GetFunction). The
            // Terraform `aws_lambda_function` resource derives `qualified_arn`
            // from this entry, so it must carry the qualifier.
            latest["FunctionArn"] = json!(format!("{}:$LATEST", func.function_arn));
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

    fn pc_key(function: &str, qualifier: &str) -> String {
        format!("{function}:{qualifier}")
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
        // Enabled toggles the ESM State between Enabled/Disabled. AWS
        // momentarily returns Enabling/Disabling but settles to the terminal
        // state; we report the terminal state directly since there's no poller
        // lag to model.
        if let Some(enabled) = body.get("Enabled").and_then(|v| v.as_bool()) {
            esm.enabled = enabled;
            esm.state = if enabled { "Enabled" } else { "Disabled" }.to_string();
        }
        // SourceAccessConfigurations (Kafka/MQ/MSK VPC + auth config). Replace
        // wholesale when provided so credential/subnet updates actually persist
        // rather than vanishing silently (1.1).
        if let Some(sac) = body
            .get("SourceAccessConfigurations")
            .and_then(|v| v.as_array())
        {
            esm.source_access_configurations = sac.clone();
        }
        esm.last_modified = chrono::Utc::now();
        // Reuse the shared serializer so Update echoes the same full field set
        // (State, SourceAccessConfigurations, KMSKeyArn, DestinationConfig,
        // MetricsConfig, MaximumRetryAttempts, ...) that Create/Get/List emit.
        let response = self.event_source_mapping_json(esm);
        ok(response)
    }

    fn region_for(&self, account_id: &str) -> String {
        let accounts = self.state.read();
        accounts
            .get(account_id)
            .map(|s| s.region.clone())
            .unwrap_or_else(|| "us-east-1".to_string())
    }
}

fn extract_csc_id(input: &str) -> String {
    // Decode percent encoding then take the segment after the last colon
    // (csc id), or treat as id if no colon present.
    let decoded = percent_decode(input);
    decoded.rsplit(':').next().unwrap_or(&decoded).to_string()
}

/// Re-export of `percent_decode` for the service-mod length check.
/// Wraps the private helper without changing its signature.
pub(crate) fn percent_decode_for_length(input: &str) -> String {
    percent_decode(input)
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
    let mut out = json!({
        "FunctionArn": c.function_arn,
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
    });
    // AWS only reports MaximumEventAgeInSeconds once the caller set it.
    if c.maximum_event_age != 0 {
        out["MaximumEventAgeInSeconds"] = json!(c.maximum_event_age);
    }
    out
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
