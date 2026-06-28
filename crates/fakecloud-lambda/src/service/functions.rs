//! `LambdaService` `functions` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    pub(crate) fn create_function(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        // Pre-pull the runtime image in the background so the first
        // Invoke doesn't pay the cold-pull cost. Cold pulls of AWS base
        // images (~700 MB) routinely exceed the AWS CLI default 60s read
        // timeout, surfacing to users as `Connection was closed`
        // (issue #1539). Invoke still re-pulls as a fallback if this
        // task lost the race or failed, so a pre-pull error is not fatal.
        if let Some(runtime) = self.runtime.clone() {
            let func_for_prepull = func.clone();
            let name = func.function_name.clone();
            tokio::spawn(async move {
                match runtime.prepull_for_function(&func_for_prepull).await {
                    Some(Ok(())) => {
                        tracing::info!(function = %name, "pre-pulled Lambda runtime image");
                    }
                    Some(Err(e)) => {
                        tracing::warn!(
                            function = %name,
                            error = %e,
                            "Lambda runtime image pre-pull failed; Invoke will retry on cold path"
                        );
                    }
                    None => {} // no resolvable image (e.g. unsupported runtime)
                }
            });
        }

        state.functions.insert(input.function_name, func);

        Ok(AwsResponse::json(StatusCode::CREATED, response.to_string()))
    }

    pub(crate) fn get_function(
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
        // When the caller passed an explicit `Qualifier` (including `$LATEST`),
        // AWS qualifies the returned FunctionArn with it; an unqualified
        // GetFunction returns the bare ARN. The Terraform function data source
        // relies on this to derive `qualified_arn`.
        if qualifier.is_some() {
            config["FunctionArn"] = json!(format!("{}:{version_label}", live.function_arn));
        }
        if version_label != "$LATEST" {
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

    pub(crate) fn delete_function(
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
            // AWS rejects DeleteFunction on a version still referenced
            // by an alias; otherwise the alias dangles. Mirror that
            // before mutating any snapshot maps.
            let alias_targets: Vec<String> = state
                .aliases
                .iter()
                .filter_map(|(k, a)| {
                    let prefix = format!("{function_name}:");
                    if k.starts_with(&prefix) && a.function_version == *q {
                        Some(a.name.clone())
                    } else {
                        None
                    }
                })
                .collect();
            if !alias_targets.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "ResourceConflictException",
                    format!(
                        "Cannot delete version {q} of function {function_name}: alias(es) reference it ({})",
                        alias_targets.join(", ")
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

        // Release the state lock before touching the runtime's warm-instance
        // map. Holding `state` while acquiring the facade `instances` lock
        // would nest the two in the opposite order from the invoke path and
        // risk a deadlock.
        drop(accounts);

        // Clean up any running container for this function. Snapshot the
        // warm pool *synchronously* (the map removal is atomic and runs with
        // no await in between), then terminate those exact instances
        // off-thread. Previously the whole stop ran in the spawned task, so
        // a CreateFunction + warm-up of the same name racing ahead of the
        // task had its fresh container reaped by this delete (bug-hunt
        // 2026-06-13, finding 4.2). Terminating the snapshot — not whatever
        // pool exists when the task eventually runs — means a recreated
        // function keeps its new container.
        if let Some(ref runtime) = self.runtime {
            let rt = runtime.clone();
            let pool = rt.take_warm_instances(function_name);
            tokio::spawn(async move { rt.terminate_instances(pool).await });
        }

        Ok(AwsResponse::json(StatusCode::NO_CONTENT, ""))
    }

    pub(crate) fn list_functions(
        &self,
        account_id: &str,
        function_version: Option<&str>,
        marker: Option<&str>,
        max_items: Option<usize>,
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
        let all_versions = function_version == Some("ALL");
        let mut functions: Vec<Value> = state
            .functions
            .values()
            .map(|f| self.function_config_json(f))
            .collect();
        if all_versions {
            // FunctionVersion=ALL also lists every published numbered version,
            // not just $LATEST.
            for snaps in state.function_version_snapshots.values() {
                for snap in snaps.values() {
                    functions.push(self.function_config_json(snap));
                }
            }
            // Order by function name, then $LATEST ahead of ascending versions.
            functions.sort_by(|a, b| {
                let an = a["FunctionName"].as_str().unwrap_or_default();
                let bn = b["FunctionName"].as_str().unwrap_or_default();
                an.cmp(bn)
                    .then_with(|| version_sort_key(a).cmp(&version_sort_key(b)))
            });
        }

        // Honor Marker/MaxItems. AWS orders ListFunctions by FunctionName and
        // carries NextMarker as a string even on the final page (empty there).
        // With ALL versions, key on the ARN (unique per version) so duplicate
        // function names don't collide in the pagination marker.
        let key: fn(&Value) -> String = if all_versions { farn_key } else { fname_key };
        let (page, next_marker) = super::paginate_marker(functions, marker, max_items, key);
        let response = json!({
            "Functions": page,
            "NextMarker": next_marker,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }
}

fn fname_key(f: &Value) -> String {
    f["FunctionName"].as_str().unwrap_or_default().to_string()
}

fn farn_key(f: &Value) -> String {
    f["FunctionArn"].as_str().unwrap_or_default().to_string()
}

/// Sort key for ListFunctions(ALL): `$LATEST` first (0), then numbered versions
/// in ascending numeric order.
fn version_sort_key(f: &Value) -> (u8, u64) {
    match f["Version"].as_str() {
        Some("$LATEST") | None => (0, 0),
        Some(v) => (1, v.parse::<u64>().unwrap_or(u64::MAX)),
    }
}
