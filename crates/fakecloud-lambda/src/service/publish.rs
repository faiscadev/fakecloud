//! `LambdaService` `publish` family — extracted from service.rs by audit-2026-05-19.

use super::*;
use crate::state::LATEST_PUBLISHED;

impl LambdaService {
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
        let publish_to_latest_published = body["PublishTo"].as_str() == Some("LATEST_PUBLISHED");
        let storage_limit = self.code_storage_limit(account_id, &req.region);

        let mut accounts = self.state.write();
        let state = accounts.regional_mut(account_id, &req.region);
        let func = state.functions.get(function_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!(
                    "Function not found: {}",
                    function_arn(&req.region, &state.account_id, function_name)
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

        if publish_to_latest_published {
            // `$LATEST.PUBLISHED` exists only for Lambda Managed Instances
            // functions; a Lambda (default) function refuses the value.
            if func.capacity_provider_config.is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValueException",
                    "LATEST_PUBLISHED' isn't a valid value for this field",
                ));
            }
            return self.publish_latest_published_locked(
                state,
                function_name,
                description_override.as_deref(),
            );
        }

        self.publish_locked(
            state,
            function_name,
            description_override.as_deref(),
            storage_limit,
        )
    }

    /// Create or republish the `$LATEST.PUBLISHED` version of the Lambda
    /// Managed Instances function `function_name` under the caller's Lambda
    /// lock, answering with its configuration.
    pub(crate) fn publish_latest_published_locked(
        &self,
        state: &mut LambdaState,
        function_name: &str,
        description_override: Option<&str>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let snapshot = publish_latest_published(state, function_name, description_override)?;
        let master = snapshot.function_arn.clone();
        let mut config = self.function_config_json(&snapshot);
        config["Version"] = json!(LATEST_PUBLISHED);
        config["FunctionArn"] = json!(format!("{master}:{LATEST_PUBLISHED}"));
        config["MasterArn"] = json!(master);
        Ok(AwsResponse::json(StatusCode::CREATED, config.to_string()))
    }

    /// Publish `$LATEST` of `function_name` as a new version, under the
    /// caller's Lambda lock. The request's preconditions are the caller's to
    /// check. `storage_limit` is the enforced code storage limit the new
    /// version's copy of the code is checked against; `UpdateFunctionCode`
    /// with `Publish` passes `None`, having checked both copies before it
    /// replaced `$LATEST`.
    pub(crate) fn publish_locked(
        &self,
        state: &mut LambdaState,
        function_name: &str,
        description_override: Option<&str>,
        storage_limit: Option<i64>,
    ) -> Result<AwsResponse, AwsServiceError> {
        let func = state
            .functions
            .get(function_name)
            .ok_or_else(|| not_found_function(state, function_name))?;
        // Pick the next version number per function, monotonic per
        // function arn, never reused. AWS uses sequential decimal
        // strings starting at 1.
        let latest_version = state
            .function_versions
            .get(function_name)
            .and_then(|versions| versions.iter().filter_map(|v| v.parse::<u64>().ok()).max());

        // PublishVersion is idempotent on AWS: if `$LATEST` hasn't changed
        // since the most recent published version, return that existing
        // snapshot instead of bumping the counter (see
        // `publish_creates_version`), so deploy pipelines that re-publish on
        // every CI run don't leak a numbered version per build.
        if !publish_creates_version(state, function_name, func, description_override) {
            if let Some(latest_str) = latest_version.map(|v| v.to_string()) {
                if let Some(prev_snap) = state
                    .function_version_snapshots
                    .get(function_name)
                    .and_then(|m| m.get(&latest_str))
                {
                    let mut config = self.function_config_json(prev_snap);
                    config["Version"] = json!(latest_str);
                    config["FunctionArn"] = json!(format!("{}:{latest_str}", func.function_arn));
                    config["MasterArn"] = json!(func.function_arn);
                    return Ok(AwsResponse::json(StatusCode::CREATED, config.to_string()));
                }
            }
        }

        let next: u64 = latest_version.unwrap_or(0) + 1;
        let next_str = next.to_string();

        // The new version stores its own copy of the code.
        crate::quota::check_new_code(state, storage_limit, crate::quota::stored_code_size(func))?;
        // A Lambda Managed Instances version takes a slot on its capacity
        // provider.
        if let Some(cpc) = &func.capacity_provider_config {
            crate::capacity::check_version_quota(
                crate::capacity::versions_in_state(state, &cpc.capacity_provider_arn).len(),
                1,
                &cpc.capacity_provider_arn,
            )?;
        }

        // Snapshot the function config + code for the new immutable version.
        let mut snapshot = func.clone();
        snapshot.version = next_str.clone();
        snapshot.master_arn = Some(func.function_arn.clone());
        if let Some(desc) = description_override {
            snapshot.description = desc.to_string();
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
        let function_arn = func.function_arn.clone();

        // Append to numbered list and store the snapshot.
        state
            .function_versions
            .entry(function_name.to_string())
            .or_default()
            .push(next_str.clone());
        let mut config = self.function_config_json(&snapshot);
        state
            .function_version_snapshots
            .entry(function_name.to_string())
            .or_default()
            .insert(next_str.clone(), snapshot);

        config["Version"] = json!(next_str);
        config["FunctionArn"] = json!(format!("{function_arn}:{next_str}"));
        config["MasterArn"] = json!(function_arn);

        Ok(AwsResponse::json(StatusCode::CREATED, config.to_string()))
    }

    pub(crate) fn function_config_json(&self, func: &LambdaFunction) -> Value {
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
        // Only emit Environment when the function actually has variables. AWS
        // omits the Environment block entirely for a function created without
        // one; returning `{"Variables":{}}` made the Terraform provider see a
        // perpetual `variables = {} -> null` diff (surfaced by the CloudWatch
        // Logs subscription-filter test, which provisions a Lambda destination).
        if !func.environment.is_empty() {
            config["Environment"] = json!({ "Variables": func.environment });
        }
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
        // Every function has a LoggingConfig: AWS defaults it to Text format
        // delivering to `/aws/lambda/<name>`. The Terraform resource asserts a
        // populated `logging_config` block even when the caller set none.
        config["LoggingConfig"] = func.logging_config.clone().unwrap_or_else(|| {
            json!({
                "LogFormat": "Text",
                "LogGroup": format!("/aws/lambda/{}", func.function_name),
            })
        });
        if let Some(ref ic) = func.image_config {
            config["ImageConfigResponse"] = json!({ "ImageConfig": ic });
        }
        if let Some(ref dc) = func.durable_config {
            config["DurableConfig"] = dc.clone();
        }
        if let Some(ref cpc) = func.capacity_provider_config {
            config["CapacityProviderConfig"] = crate::capacity::config_json(cpc);
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

/// Create or republish the `$LATEST.PUBLISHED` version of the Lambda
/// Managed Instances function `function_name` from its `$LATEST`. Each
/// republish replaces the previous `$LATEST.PUBLISHED` (no numbered version
/// is created); the first one takes a slot on the function's capacity
/// provider. Returns the stored snapshot. Shared with CloudFormation's
/// `PublishToLatestPublished`.
pub fn publish_latest_published(
    state: &mut LambdaState,
    function_name: &str,
    description_override: Option<&str>,
) -> Result<LambdaFunction, AwsServiceError> {
    let func = state
        .functions
        .get(function_name)
        .ok_or_else(|| not_found_function(state, function_name))?;
    if let Some(cpc) = &func.capacity_provider_config {
        if !state.latest_published.contains_key(function_name) {
            crate::capacity::check_version_quota(
                crate::capacity::versions_in_state(state, &cpc.capacity_provider_arn).len(),
                1,
                &cpc.capacity_provider_arn,
            )?;
        }
    }
    let mut snapshot = func.clone();
    snapshot.version = LATEST_PUBLISHED.to_string();
    snapshot.master_arn = Some(func.function_arn.clone());
    if let Some(desc) = description_override {
        snapshot.description = desc.to_string();
    }
    snapshot.revision_id = uuid::Uuid::new_v4().to_string();
    snapshot.last_modified = Utc::now();
    state
        .latest_published
        .insert(function_name.to_string(), snapshot.clone());
    Ok(snapshot)
}

/// `ResourceNotFoundException` for a function missing from `state`.
fn not_found_function(state: &LambdaState, function_name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!(
            "Function not found: {}",
            function_arn(&state.region, &state.account_id, function_name)
        ),
    )
}
