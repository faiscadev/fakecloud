//! `LambdaService` `publish` family — extracted from service.rs by audit-2026-05-19.

use super::*;

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
