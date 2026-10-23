//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `lambda`.

use super::*;

impl ResourceProvisioner {
    /// A function's `Layers`, resolved as CreateFunction resolves them: each
    /// version's `CodeSize`, and a refusal for a version another account has
    /// not shared with the stack's account (or its organization).
    fn resolve_lambda_layers(&self, arns: &[String]) -> Result<Vec<AttachedLayer>, String> {
        let caller = fakecloud_lambda::layer_access::LayerCaller::for_account(
            &self.account_id,
            &self.region,
            None,
            Some(
                fakecloud_organizations::resolver::OrganizationsScpResolver::shared(
                    self.organizations_state.clone(),
                ),
            ),
        );
        let accounts = self.lambda_state.read();
        fakecloud_lambda::layer_access::resolve_layer_attachments(&accounts, arns.to_vec(), &caller)
            .map_err(|e| e.message())
    }

    pub(super) fn get_att_lambda_function(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let function_name = parse_lambda_function_name(physical_id);
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        match attribute {
            "Arn" => state
                .functions
                .get(&function_name)
                .map(|f| f.function_arn.clone()),
            "FunctionUrl" => state
                .function_url_configs
                .get(&function_name)
                .map(|u| u.function_url.clone()),
            "Version" => state
                .functions
                .get(&function_name)
                .map(|f| f.version.clone()),
            _ => None,
        }
    }

    pub(super) fn create_lambda_function(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = props
            .get("FunctionName")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| self.physical_name(resource));
        // Checked up front (before reading code from S3 or warming the
        // runtime image) and again at insert time below.
        if self
            .lambda_state
            .read()
            .regional(&self.account_id, &self.region)
            .is_some_and(|s| s.functions.contains_key(&function_name))
        {
            return Err(resource_already_exists(
                "AWS::Lambda::Function",
                &function_name,
            ));
        }

        let cfg = parse_lambda_function_props(props)?;
        self.validate_lambda_execution_role(&cfg.role)?;
        let function_arn =
            fakecloud_lambda::function_arn(&self.region, &self.account_id, &function_name);

        // Resolve `Code.S3Bucket` + `Code.S3Key` against the in-process S3 state
        // so a stack that uploads code via `AWS::S3::Bucket` + `AWS::S3::Object`
        // (or any S3 PutObject before CreateStack) hydrates `code_zip` from the
        // real bytes. ZipFile (already decoded by parse_lambda_function_props)
        // wins over S3 if both are present, mirroring AWS validation order.
        let code_zip = if cfg.code_zip.is_some() {
            cfg.code_zip.clone()
        } else if let (Some(bucket_name), Some(key)) = (&cfg.s3_bucket, &cfg.s3_key) {
            Some(self.read_s3_object_bytes(bucket_name, key).map_err(|e| {
                format!("Failed to read Code.S3Bucket={bucket_name} Code.S3Key={key}: {e}")
            })?)
        } else {
            None
        };

        // Hash the actual ZIP bytes when available; image-based functions
        // leave the sha empty (matching the lambda CreateFunction code path
        // when neither ZipFile nor S3 source is supplied).
        let (code_sha256, code_size) = match &code_zip {
            Some(bytes) => (sha256_b64(bytes), bytes.len() as i64),
            None => (String::new(), 0),
        };

        // Resolve attached layer ARNs to their current code_size so
        // GetFunctionConfiguration echoes the right size without a
        // second state lookup.
        let layers: Vec<AttachedLayer> = self.resolve_lambda_layers(&cfg.layers)?;

        let func = fakecloud_lambda::LambdaFunction {
            function_name: function_name.clone(),
            function_arn: function_arn.clone(),
            runtime: cfg.runtime,
            role: cfg.role,
            handler: cfg.handler,
            description: cfg.description,
            timeout: cfg.timeout,
            memory_size: cfg.memory_size,
            code_sha256,
            code_size,
            version: "$LATEST".to_string(),
            last_modified: Utc::now(),
            tags: cfg.tags,
            environment: cfg.environment,
            architectures: cfg.architectures,
            package_type: cfg.package_type,
            code_zip,
            image_uri: cfg.image_uri,
            policy: None,
            layers,
            revision_id: Uuid::new_v4().to_string(),
            tracing_mode: cfg.tracing_mode,
            kms_key_arn: cfg.kms_key_arn,
            ephemeral_storage_size: cfg.ephemeral_storage_size,
            vpc_config: cfg
                .vpc_config
                .map(|c| {
                    fakecloud_lambda::vpc::resolve_vpc_config(
                        &self.ec2_state,
                        &self.account_id,
                        &self.region,
                        &c,
                    )
                })
                .transpose()?,
            snap_start: cfg.snap_start,
            dead_letter_config_arn: cfg.dead_letter_config_arn,
            file_system_configs: cfg.file_system_configs,
            logging_config: cfg.logging_config,
            image_config: cfg.image_config,
            durable_config: None,
            signing_profile_version_arn: None,
            signing_job_arn: None,
            runtime_version_config: None,
            master_arn: None,
            state_reason: None,
            state_reason_code: None,
            last_update_status_reason: None,
            last_update_status_reason_code: None,
        };

        // Pre-pull the runtime image in the background so the first Invoke of
        // this CFN-provisioned function doesn't pay the cold-pull cost (the
        // #1539 timeout, through the CloudFormation door — mirrors the
        // CreateFunction path in the lambda service). Best-effort: Invoke still
        // re-pulls as a fallback, so a pre-pull error is not fatal, and we only
        // spawn when a Tokio runtime is active (provisioning runs inside the
        // async request handler; unit tests that call the provisioner directly
        // have no runtime and simply skip the warm-up).
        if let Some(runtime) = self.lambda_runtime.clone() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let func_for_prepull = func.clone();
                let name = function_name.clone();
                handle.spawn(async move {
                    match runtime.prepull_for_function(&func_for_prepull).await {
                        Some(Ok(())) => {
                            tracing::info!(function = %name, "pre-pulled CFN Lambda runtime image");
                        }
                        Some(Err(e)) => {
                            tracing::warn!(
                                function = %name,
                                error = %e,
                                "CFN Lambda runtime image pre-pull failed; Invoke will retry on cold path"
                            );
                        }
                        None => {} // no resolvable image (e.g. unsupported runtime)
                    }
                });
            }
        }

        let storage_limit = self.lambda_storage_limit();
        let concurrency_quota = self.lambda_concurrency_quota();
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        // An existing function under this name (e.g. one a deleted stack
        // retained) fails the create, as Lambda's ResourceConflictException
        // does, instead of being overwritten.
        if state.functions.contains_key(&function_name) {
            return Err(resource_already_exists(
                "AWS::Lambda::Function",
                &function_name,
            ));
        }
        fakecloud_lambda::quota::check_new_code(
            state,
            storage_limit,
            fakecloud_lambda::quota::stored_code_size(&func),
        )
        .map_err(|e| super::quota::refusal("Lambda", e))?;
        if let Some(reserved) = cfg.reserved_concurrent_executions {
            let limit = fakecloud_lambda::quota::concurrency_limit(state, concurrency_quota);
            fakecloud_lambda::quota::check_reservation(state, &function_name, reserved, limit)
                .map_err(|e| super::quota::refusal("Lambda", e))?;
        }
        state.functions.insert(function_name.clone(), func);
        if let Some(reserved) = cfg.reserved_concurrent_executions {
            state
                .function_concurrency
                .insert(function_name.clone(), reserved);
        }

        // Capture every GetAtt-resolvable attribute eagerly. Output
        // resolution happens after `provision_stack_resources` returns
        // and only sees `StackResource.attributes`, so any attribute the
        // template's `Outputs` need must be in this map.
        Ok(ProvisionResult::new(function_name.clone())
            .with("Arn", function_arn)
            .with("FunctionName", function_name)
            .with("Version", "$LATEST"))
    }

    /// The same execution-role checks `CreateFunction` runs: a role whose
    /// trust policy lets Lambda assume it, in the stack's account under IAM
    /// enforcement.
    fn validate_lambda_execution_role(&self, role_arn: &str) -> Result<(), String> {
        let validator =
            fakecloud_iam::pass_role::IamRoleTrustValidator::new(self.iam_state.clone());
        fakecloud_lambda::validate_execution_role(
            &self.account_id,
            role_arn,
            Some(&validator),
            self.iam_mode,
        )
        .map_err(|e| e.to_string())
    }

    /// Apply a CFN template-driven update to an existing Lambda function.
    /// Mirrors `UpdateFunctionConfiguration` + `UpdateFunctionCode`:
    /// rewrite mutable configuration fields from the new template, re-hash
    /// any new code bytes, bump `revision_id` and `last_modified`. The
    /// function's ARN, name, and `$LATEST` version stay put — those are
    /// the immutable identity of the resource as far as CloudFormation
    /// is concerned.
    pub(super) fn update_lambda_function(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = existing.physical_id.clone();
        let cfg = parse_lambda_function_props(props)?;
        self.validate_lambda_execution_role(&cfg.role)?;

        let new_code_zip = if cfg.code_zip.is_some() {
            cfg.code_zip.clone()
        } else if let (Some(bucket_name), Some(key)) = (&cfg.s3_bucket, &cfg.s3_key) {
            Some(self.read_s3_object_bytes(bucket_name, key).map_err(|e| {
                format!("Failed to read Code.S3Bucket={bucket_name} Code.S3Key={key}: {e}")
            })?)
        } else {
            None
        };

        let resolved_layers: Vec<AttachedLayer> = self.resolve_lambda_layers(&cfg.layers)?;

        let vpc_config = cfg
            .vpc_config
            .clone()
            .map(|c| {
                fakecloud_lambda::vpc::resolve_vpc_config(
                    &self.ec2_state,
                    &self.account_id,
                    &self.region,
                    &c,
                )
            })
            .transpose()?;
        let storage_limit = self.lambda_storage_limit();
        let concurrency_quota = self.lambda_concurrency_quota();
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        // The update is built on a copy and checked against the quotas
        // before it replaces `$LATEST`, so a refused update changes nothing.
        let mut func = state
            .functions
            .get(&function_name)
            .cloned()
            .ok_or_else(|| {
                format!("Cannot update {function_name}: function does not exist in lambda state")
            })?;
        let old_size = fakecloud_lambda::quota::stored_code_size(&func);

        func.runtime = cfg.runtime;
        func.role = cfg.role;
        func.handler = cfg.handler;
        func.description = cfg.description;
        func.timeout = cfg.timeout;
        func.memory_size = cfg.memory_size;
        func.environment = cfg.environment;
        func.architectures = cfg.architectures;
        func.package_type = cfg.package_type;
        func.tracing_mode = cfg.tracing_mode;
        func.kms_key_arn = cfg.kms_key_arn;
        func.ephemeral_storage_size = cfg.ephemeral_storage_size;
        func.vpc_config = vpc_config;
        func.snap_start = cfg.snap_start;
        func.dead_letter_config_arn = cfg.dead_letter_config_arn;
        func.file_system_configs = cfg.file_system_configs;
        func.logging_config = cfg.logging_config;
        func.layers = resolved_layers;
        if cfg.image_uri.is_some() {
            func.image_uri = cfg.image_uri;
        }
        func.image_config = cfg.image_config;
        // Tags are desired state: an empty or absent Tags property clears
        // them, as a stack update does in AWS.
        func.tags = cfg.tags;
        if let Some(bytes) = new_code_zip {
            func.code_sha256 = sha256_b64(&bytes);
            func.code_size = bytes.len() as i64;
            func.code_zip = Some(bytes);
        }
        func.last_modified = Utc::now();
        func.revision_id = Uuid::new_v4().to_string();

        // The same check `UpdateFunctionCode` runs on its updated record, so
        // S3-sourced code and container images count alike.
        fakecloud_lambda::quota::check_replaced_code(state, storage_limit, old_size, &func, false)
            .map_err(|e| super::quota::refusal("Lambda", e))?;
        if let Some(reserved) = cfg.reserved_concurrent_executions {
            let limit = fakecloud_lambda::quota::concurrency_limit(state, concurrency_quota);
            fakecloud_lambda::quota::check_reservation(state, &function_name, reserved, limit)
                .map_err(|e| super::quota::refusal("Lambda", e))?;
        }
        let function_arn = func.function_arn.clone();
        state.functions.insert(function_name.clone(), func);
        // Reserved concurrency follows the template: set it when present,
        // remove it (DeleteFunctionConcurrency) when the property is dropped.
        match cfg.reserved_concurrent_executions {
            Some(reserved) => {
                state
                    .function_concurrency
                    .insert(function_name.clone(), reserved);
            }
            None => {
                state.function_concurrency.remove(&function_name);
            }
        }
        Ok(ProvisionResult::new(function_name.clone())
            .with("Arn", function_arn)
            .with("FunctionName", function_name)
            .with("Version", "$LATEST"))
    }

    pub(super) fn delete_lambda_function(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        state.functions.remove(physical_id);
        state.function_concurrency.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_lambda_permission(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "FunctionName is required".to_string())?,
        );
        // CFN does not surface a StatementId knob; synthesize one from
        // the logical id so subsequent updates / deletes can find the
        // statement again.
        let statement_id = format!(
            "cfn-{}-{}",
            resource.logical_id,
            fakecloud_core::ids::short_id(8)
        );
        self.append_lambda_permission_statement(&function_name, &statement_id, props)?;

        // Encode `{function}|{sid}` so delete can target a single statement.
        let physical_id = format!("{function_name}|{statement_id}");
        Ok(ProvisionResult::new(physical_id).with("Id", statement_id))
    }

    /// Replace the policy statement for an existing CFN-managed
    /// `AWS::Lambda::Permission`. CFN treats most property changes as
    /// in-place updates: the statement id stays put, the body is
    /// rewritten to reflect the new properties.
    pub(super) fn update_lambda_permission(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let Some((function_name, statement_id)) = existing.physical_id.split_once('|') else {
            return Err(format!(
                "Permission physical id `{}` is malformed; expected `{{function}}|{{sid}}`",
                existing.physical_id
            ));
        };
        // First strip the prior statement so we don't end up with a duplicate
        // sid in the policy doc.
        {
            let mut accounts = self.lambda_state.write();
            let state = accounts.regional_mut(&self.account_id, &self.region);
            if let Some(func) = state.functions.get_mut(function_name) {
                if let Some(policy_str) = func.policy.as_deref() {
                    if let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(policy_str) {
                        if let Some(arr) = doc.get_mut("Statement").and_then(|v| v.as_array_mut()) {
                            arr.retain(|s| {
                                s.get("Sid").and_then(|v| v.as_str()) != Some(statement_id)
                            });
                            func.policy = Some(doc.to_string());
                        }
                    }
                }
            }
        }
        self.append_lambda_permission_statement(function_name, statement_id, &resource.properties)?;
        Ok(ProvisionResult::new(existing.physical_id.clone()).with("Id", statement_id.to_string()))
    }

    pub(super) fn delete_lambda_permission(&self, physical_id: &str) -> Result<(), String> {
        let Some((function_name, sid)) = physical_id.split_once('|') else {
            return Ok(());
        };
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if let Some(func) = state.functions.get_mut(function_name) {
            if let Some(policy_str) = func.policy.as_deref() {
                if let Ok(mut doc) = serde_json::from_str::<serde_json::Value>(policy_str) {
                    if let Some(arr) = doc.get_mut("Statement").and_then(|v| v.as_array_mut()) {
                        arr.retain(|s| s.get("Sid").and_then(|v| v.as_str()) != Some(sid));
                        func.policy = Some(doc.to_string());
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn create_lambda_event_source_mapping(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "FunctionName is required".to_string())?,
        );
        let cfg = parse_lambda_event_source_mapping_props(props)?;

        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if !state.functions.contains_key(&function_name) {
            return Err(format!(
                "Function {function_name} does not exist yet — retry once it has been provisioned"
            ));
        }
        // An ARN (e.g. an alias's, from `Ref` on an AWS::Lambda::Alias) keeps
        // its qualifier, as CreateEventSourceMapping keeps it, so the mapping
        // invokes that alias.
        let function_arn = match props.get("FunctionName").and_then(|v| v.as_str()) {
            Some(arn) if arn.starts_with("arn:") => arn.to_string(),
            _ => fakecloud_lambda::function_arn(&self.region, &self.account_id, &function_name),
        };
        let uuid = Uuid::new_v4().to_string();
        let esm = EventSourceMapping {
            uuid: uuid.clone(),
            function_arn,
            event_source_arn: cfg.event_source_arn,
            batch_size: cfg.batch_size,
            enabled: cfg.enabled,
            state: if cfg.enabled {
                "Enabled".to_string()
            } else {
                "Disabled".to_string()
            },
            last_modified: Utc::now(),
            filter_patterns: cfg.filter_patterns,
            maximum_batching_window_in_seconds: cfg.maximum_batching_window_in_seconds,
            starting_position: cfg.starting_position,
            starting_position_timestamp: cfg.starting_position_timestamp,
            parallelization_factor: cfg.parallelization_factor,
            function_response_types: cfg.function_response_types,
            kms_key_arn: cfg.kms_key_arn,
            metrics_config: cfg.metrics_config,
            destination_config: cfg.destination_config,
            maximum_retry_attempts: cfg.maximum_retry_attempts,
            maximum_record_age_in_seconds: cfg.maximum_record_age_in_seconds,
            bisect_batch_on_function_error: cfg.bisect_batch_on_function_error,
            tumbling_window_in_seconds: cfg.tumbling_window_in_seconds,
            topics: cfg.topics,
            queues: cfg.queues,
            source_access_configurations: cfg.source_access_configurations,
            self_managed_event_source: cfg.self_managed_event_source,
            self_managed_kafka_event_source_config: cfg.self_managed_kafka_event_source_config,
            document_db_event_source_config: cfg.document_db_event_source_config,
        };
        state.event_source_mappings.insert(uuid.clone(), esm);
        Ok(ProvisionResult::new(uuid.clone()).with("Id", uuid))
    }

    /// In-place update of an existing EventSourceMapping. CFN treats
    /// every mutable property as in-place; the EventSourceArn /
    /// FunctionName pair stays put (those are immutable identity).
    pub(super) fn update_lambda_event_source_mapping(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let cfg = parse_lambda_event_source_mapping_props(&resource.properties)?;
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let esm = state
            .event_source_mappings
            .get_mut(&existing.physical_id)
            .ok_or_else(|| {
                format!(
                    "EventSourceMapping {} does not exist in lambda state",
                    existing.physical_id
                )
            })?;
        esm.batch_size = cfg.batch_size;
        esm.enabled = cfg.enabled;
        esm.state = if cfg.enabled {
            "Enabled".to_string()
        } else {
            "Disabled".to_string()
        };
        esm.last_modified = Utc::now();
        esm.filter_patterns = cfg.filter_patterns;
        esm.maximum_batching_window_in_seconds = cfg.maximum_batching_window_in_seconds;
        esm.parallelization_factor = cfg.parallelization_factor;
        esm.function_response_types = cfg.function_response_types;
        esm.kms_key_arn = cfg.kms_key_arn;
        esm.metrics_config = cfg.metrics_config;
        esm.destination_config = cfg.destination_config;
        esm.maximum_retry_attempts = cfg.maximum_retry_attempts;
        esm.maximum_record_age_in_seconds = cfg.maximum_record_age_in_seconds;
        esm.bisect_batch_on_function_error = cfg.bisect_batch_on_function_error;
        esm.tumbling_window_in_seconds = cfg.tumbling_window_in_seconds;
        esm.source_access_configurations = cfg.source_access_configurations;
        esm.document_db_event_source_config = cfg.document_db_event_source_config;
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("Id", existing.physical_id.clone()))
    }

    pub(super) fn delete_lambda_event_source_mapping(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        state.event_source_mappings.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_lambda_layer_version(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let layer_name = props
            .get("LayerName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let license_info = props
            .get("LicenseInfo")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let compatible_runtimes: Vec<String> = props
            .get("CompatibleRuntimes")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let compatible_architectures: Vec<String> = props
            .get("CompatibleArchitectures")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        // CFN's `Content.ZipFile` rides as base64 (per the user guide).
        // `Content.S3Bucket` + `Content.S3Key` hydrate the same way the
        // Lambda function provisioner pulls Code.S3Bucket. Either path
        // is optional — fakecloud accepts a layer with zero-byte content
        // so templates that reference an upstream-managed layer ARN
        // still work.
        let content = props.get("Content");
        let zip_bytes = if let Some(b64) = content
            .and_then(|v| v.get("ZipFile"))
            .and_then(|v| v.as_str())
        {
            use base64::Engine;
            Some(
                base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| format!("Content.ZipFile is not valid base64: {e}"))?,
            )
        } else if let (Some(bucket), Some(key)) = (
            content
                .and_then(|c| c.get("S3Bucket"))
                .and_then(|v| v.as_str()),
            content
                .and_then(|c| c.get("S3Key"))
                .and_then(|v| v.as_str()),
        ) {
            Some(self.read_s3_object_bytes(bucket, key).map_err(|e| {
                format!("Failed to read Content.S3Bucket={bucket} Content.S3Key={key}: {e}")
            })?)
        } else {
            None
        };

        let (code_sha256, code_size) = match zip_bytes.as_deref() {
            Some(bytes) => (sha256_b64(bytes), bytes.len() as i64),
            None => (String::new(), 0),
        };
        let storage_limit = self.lambda_storage_limit();

        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        fakecloud_lambda::quota::check_new_code(state, storage_limit, code_size)
            .map_err(|e| super::quota::refusal("Lambda", e))?;
        let layer_arn = fakecloud_lambda::layer_arn(&self.region, &self.account_id, &layer_name);
        let layer = state
            .layers
            .entry(layer_name.clone())
            .or_insert_with(|| Layer::new(&layer_name, layer_arn.clone()));
        let next_version = layer.next_version();
        let version_arn = format!("{}:{}", layer.layer_arn, next_version);
        layer.versions.push(LayerVersion {
            version: next_version,
            layer_version_arn: version_arn.clone(),
            description: description.clone(),
            created_date: Utc::now(),
            compatible_runtimes,
            license_info,
            policy: None,
            policy_revision_id: None,
            code_zip: zip_bytes,
            code_sha256,
            code_size,
            compatible_architectures,
        });
        Ok(ProvisionResult::new(version_arn.clone())
            .with("LayerVersionArn", version_arn)
            .with("LayerArn", layer_arn))
    }

    pub(super) fn delete_lambda_layer_version(&self, physical_id: &str) -> Result<(), String> {
        // physical_id is the layer version ARN, which names the account and
        // region the version lives in.
        let Some(arn) = fakecloud_lambda::layer_access::LayerArn::parse(physical_id) else {
            return Ok(());
        };
        let Some(version) = arn.version else {
            return Ok(());
        };
        let mut accounts = self.lambda_state.write();
        if let Some(layer) = accounts
            .regional_get_mut(&arn.account_id, &arn.region)
            .and_then(|s| s.layers.get_mut(&arn.layer_name))
        {
            layer.versions.retain(|v| v.version != version);
        }
        Ok(())
    }

    pub(super) fn create_lambda_url(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("TargetFunctionArn")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "TargetFunctionArn is required".to_string())?,
        );
        // Optional qualifier — when set the URL points at an alias
        // (`{function_name}:{qualifier}`) rather than `$LATEST`. AWS
        // refuses to mint a URL for a numeric version, but the CFN
        // schema allows the property so we accept and round-trip it.
        let qualifier = props
            .get("Qualifier")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string());
        let auth_type = props
            .get("AuthType")
            .and_then(|v| v.as_str())
            .unwrap_or("NONE")
            .to_string();
        let invoke_mode = props
            .get("InvokeMode")
            .and_then(|v| v.as_str())
            .unwrap_or("BUFFERED")
            .to_string();
        let cors = props.get("Cors").cloned();

        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if !state.functions.contains_key(&function_name) {
            return Err(format!(
                "Function {function_name} does not exist yet — retry once it has been provisioned"
            ));
        }
        let function_arn = match &qualifier {
            Some(q) => fakecloud_lambda::qualified_function_arn(
                &self.region,
                &self.account_id,
                &function_name,
                q,
            ),
            None => fakecloud_lambda::function_arn(&self.region, &self.account_id, &function_name),
        };
        let function_url = format!("https://{function_name}.lambda-url.{}.on.aws/", self.region);
        let now = Utc::now();
        let cfg = FunctionUrlConfig {
            function_arn: function_arn.clone(),
            function_url: function_url.clone(),
            auth_type,
            cors,
            creation_time: now,
            last_modified_time: now,
            invoke_mode,
        };
        // Key by `{function}:{qualifier}` when qualifier is set so
        // delete and update can round-trip the same shape, while leaving
        // the bare-function-name URL alone for $LATEST.
        let key = match &qualifier {
            Some(q) => format!("{function_name}:{q}"),
            None => function_name.clone(),
        };
        state.function_url_configs.insert(key.clone(), cfg);

        Ok(ProvisionResult::new(key)
            .with("FunctionArn", function_arn)
            .with("FunctionUrl", function_url))
    }

    /// Update an existing Lambda Url config in place. AuthType, Cors,
    /// and InvokeMode are mutable; the target function and qualifier
    /// (which collectively form the key) are not — CFN replaces the
    /// resource if those change, so we only touch the body here.
    pub(super) fn update_lambda_url(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let auth_type = props
            .get("AuthType")
            .and_then(|v| v.as_str())
            .unwrap_or("NONE")
            .to_string();
        let invoke_mode = props
            .get("InvokeMode")
            .and_then(|v| v.as_str())
            .unwrap_or("BUFFERED")
            .to_string();
        let cors = props.get("Cors").cloned();

        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let cfg = state
            .function_url_configs
            .get_mut(&existing.physical_id)
            .ok_or_else(|| {
                format!(
                    "FunctionUrlConfig {} does not exist in lambda state",
                    existing.physical_id
                )
            })?;
        cfg.auth_type = auth_type;
        cfg.invoke_mode = invoke_mode;
        cfg.cors = cors;
        cfg.last_modified_time = Utc::now();
        let function_arn = cfg.function_arn.clone();
        let function_url = cfg.function_url.clone();
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("FunctionArn", function_arn)
            .with("FunctionUrl", function_url))
    }

    pub(super) fn delete_lambda_url(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        state.function_url_configs.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_lambda_alias(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "FunctionName is required".to_string())?,
        );
        let alias_name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Name is required".to_string())?
            .to_string();
        let function_version = props
            .get("FunctionVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("$LATEST")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let routing_config = props.get("RoutingConfig").cloned();

        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if !state.functions.contains_key(&function_name) {
            return Err(format!(
                "Function {function_name} does not exist yet — retry once it has been provisioned"
            ));
        }
        let alias_arn = fakecloud_lambda::qualified_function_arn(
            &self.region,
            &self.account_id,
            &function_name,
            &alias_name,
        );
        let key = format!("{function_name}:{alias_name}");
        state.aliases.insert(
            key.clone(),
            FunctionAlias {
                alias_arn: alias_arn.clone(),
                name: alias_name,
                function_version,
                description,
                revision_id: Uuid::new_v4().to_string(),
                routing_config,
            },
        );
        // ProvisionedConcurrencyConfig — CFN allows attaching a target
        // concurrency at the same time as the alias is created. Mirror
        // the runtime PutProvisionedConcurrencyConfig path: stamp a
        // `ProvisionedConcurrencyConfig` entry keyed by `{function}:{alias}`
        // with a `READY` status and the requested count fully allocated.
        if let Some(cnt) = props
            .get("ProvisionedConcurrencyConfig")
            .and_then(|v| v.get("ProvisionedConcurrentExecutions"))
            .and_then(|v| v.as_i64())
        {
            state.provisioned_concurrency.insert(
                key.clone(),
                fakecloud_lambda::ProvisionedConcurrencyConfig {
                    requested: cnt,
                    allocated: cnt,
                    status: "READY".to_string(),
                    last_modified: Utc::now(),
                },
            );
        }
        // Expose the alias ARN as the physical id so `Ref` on the alias
        // resolves to the ARN, like real CloudFormation. The internal
        // `aliases` / `provisioned_concurrency` maps stay keyed by
        // `{function}:{alias}` (`key`); update/delete recover that key
        // from the ARN physical id via `alias_state_key`.
        Ok(ProvisionResult::new(alias_arn.clone()).with("AliasArn", alias_arn))
    }

    /// Update an existing Lambda Alias in place. FunctionVersion,
    /// Description, RoutingConfig, and ProvisionedConcurrencyConfig are
    /// the mutable fields — Name and FunctionName are immutable, so a
    /// change there is a replacement (the parent CFN engine handles
    /// that path; this fn only sees the in-place case).
    pub(super) fn update_lambda_alias(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_version = props
            .get("FunctionVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("$LATEST")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let routing_config = props.get("RoutingConfig").cloned();

        // `existing.physical_id` is the alias ARN; the maps are keyed by
        // `{function}:{alias}`.
        let key = alias_state_key(&existing.physical_id);

        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let alias = state.aliases.get_mut(&key).ok_or_else(|| {
            format!(
                "Alias {} does not exist in lambda state",
                existing.physical_id
            )
        })?;
        alias.function_version = function_version;
        alias.description = description;
        alias.routing_config = routing_config;
        alias.revision_id = Uuid::new_v4().to_string();
        let alias_arn = alias.alias_arn.clone();

        // Re-stamp provisioned concurrency to match the new shape; remove
        // when the property is dropped from the template.
        match props
            .get("ProvisionedConcurrencyConfig")
            .and_then(|v| v.get("ProvisionedConcurrentExecutions"))
            .and_then(|v| v.as_i64())
        {
            Some(cnt) => {
                state.provisioned_concurrency.insert(
                    key.clone(),
                    fakecloud_lambda::ProvisionedConcurrencyConfig {
                        requested: cnt,
                        allocated: cnt,
                        status: "READY".to_string(),
                        last_modified: Utc::now(),
                    },
                );
            }
            None => {
                state.provisioned_concurrency.remove(&key);
            }
        }
        Ok(ProvisionResult::new(existing.physical_id.clone()).with("AliasArn", alias_arn))
    }

    pub(super) fn delete_lambda_alias(&self, physical_id: &str) -> Result<(), String> {
        // `physical_id` is the alias ARN; the maps are keyed by
        // `{function}:{alias}`.
        let key = alias_state_key(physical_id);
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        state.aliases.remove(&key);
        state.provisioned_concurrency.remove(&key);
        Ok(())
    }

    pub(super) fn create_lambda_version(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "FunctionName is required".to_string())?,
        );
        // CFN's `Description` overrides the parent function's
        // description on this immutable snapshot; AWS does the same.
        // `CodeSha256`, when set, gates publish — AWS rejects publish if
        // the current `$LATEST` sha doesn't match the supplied value.
        let description_override = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let expected_sha = props
            .get("CodeSha256")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let storage_limit = self.lambda_storage_limit();

        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let func = state
            .functions
            .get(&function_name)
            .ok_or_else(|| format!("Function {function_name} does not exist yet — retry once it has been provisioned"))?
            .clone();
        // The version stores its own copy of the code.
        fakecloud_lambda::quota::check_new_code(
            state,
            storage_limit,
            fakecloud_lambda::quota::stored_code_size(&func),
        )
        .map_err(|e| super::quota::refusal("Lambda", e))?;
        if let Some(expected) = &expected_sha {
            if !expected.is_empty() && expected != &func.code_sha256 {
                return Err(format!(
                    "PreconditionFailed: CodeSha256 mismatch on {function_name} — expected {expected}, $LATEST is {actual}",
                    actual = func.code_sha256,
                ));
            }
        }
        let versions = state
            .function_versions
            .entry(function_name.clone())
            .or_default();
        let next_version = (versions.len() as i64 + 1).to_string();
        versions.push(next_version.clone());
        // Snapshot current function config under this version with the
        // CFN-supplied description override (if any).
        let mut snapshot = func.clone();
        snapshot.version = next_version.clone();
        if let Some(desc) = description_override {
            snapshot.description = desc;
        }
        state
            .function_version_snapshots
            .entry(function_name.clone())
            .or_default()
            .insert(next_version.clone(), snapshot);
        let version_arn = fakecloud_lambda::qualified_function_arn(
            &self.region,
            &self.account_id,
            &function_name,
            &next_version,
        );
        let physical_id = format!("{function_name}:{next_version}");
        Ok(ProvisionResult::new(physical_id)
            .with("Version", next_version)
            .with("FunctionArn", version_arn))
    }

    /// Update an existing Lambda Version. CFN treats numbered versions
    /// as immutable — any property change forces replacement (a new
    /// version number) at the engine level, so this fn just no-ops and
    /// echoes the existing physical id back. Keeping it wired keeps the
    /// update dispatch table consistent with the other 5 aux types.
    pub(super) fn update_lambda_version(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        let Some((function_name, version)) = existing.physical_id.split_once(':') else {
            return Err(format!(
                "Version physical id `{}` is malformed; expected `{{function}}:{{version}}`",
                existing.physical_id
            ));
        };
        let exists = state
            .function_version_snapshots
            .get(function_name)
            .map(|m| m.contains_key(version))
            .unwrap_or(false);
        if !exists {
            return Err(format!(
                "Version {version} for function {function_name} no longer exists in lambda state"
            ));
        }
        let version_arn = fakecloud_lambda::qualified_function_arn(
            &self.region,
            &self.account_id,
            function_name,
            version,
        );
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("Version", version.to_string())
            .with("FunctionArn", version_arn))
    }

    /// Update an existing LayerVersion. CFN treats LayerVersion as
    /// immutable (Content / CompatibleRuntimes / etc are all fixed once
    /// published), so a property change forces a new version. This fn
    /// no-ops to keep the dispatch table symmetric with the other aux
    /// types.
    pub(super) fn update_lambda_layer_version(
        &self,
        existing: &StackResource,
        _resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let arn = existing.physical_id.clone();
        let layer_arn_only = fakecloud_lambda::layer_access::LayerArn::parse(&arn)
            .map(|a| a.layer_arn())
            .unwrap_or_else(|| arn.clone());
        Ok(ProvisionResult::new(existing.physical_id.clone())
            .with("LayerVersionArn", arn)
            .with("LayerArn", layer_arn_only))
    }

    pub(super) fn delete_lambda_version(&self, physical_id: &str) -> Result<(), String> {
        let Some((function_name, version)) = physical_id.split_once(':') else {
            return Ok(());
        };
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if let Some(versions) = state.function_versions.get_mut(function_name) {
            versions.retain(|v| v != version);
        }
        if let Some(snapshots) = state.function_version_snapshots.get_mut(function_name) {
            snapshots.remove(version);
        }
        Ok(())
    }
}

impl ResourceProvisioner {
    /// `AWS::Lambda::EventInvokeConfig`: the asynchronous-invocation settings
    /// (max event age, retries, destinations) for a function qualifier, as
    /// PutFunctionEventInvokeConfig stores them -- the async invoke path reads
    /// them to route results to OnSuccess / OnFailure destinations. The
    /// physical id is `<function>:<qualifier>`.
    pub(super) fn create_lambda_event_invoke_config(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let function_name = parse_lambda_function_name(
            props
                .get("FunctionName")
                .and_then(|v| v.as_str())
                .ok_or("EventInvokeConfig requires FunctionName")?,
        );
        let qualifier = props
            .get("Qualifier")
            .and_then(|v| v.as_str())
            .ok_or("EventInvokeConfig requires Qualifier")?
            .to_string();
        let event_age = match props.get("MaximumEventAgeInSeconds") {
            None | Some(serde_json::Value::Null) => 0,
            Some(v) => {
                let n = v
                    .as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(-1);
                if !(60..=21600).contains(&n) {
                    return Err(format!(
                        "InvalidParameterValueException: MaximumEventAgeInSeconds must be 60..21600 (got {v})"
                    ));
                }
                n
            }
        };
        let retries = match props.get("MaximumRetryAttempts") {
            None | Some(serde_json::Value::Null) => 2,
            Some(v) => {
                let n = v
                    .as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                    .unwrap_or(-1);
                if !(0..=2).contains(&n) {
                    return Err(format!(
                        "InvalidParameterValueException: MaximumRetryAttempts must be 0..2 (got {v})"
                    ));
                }
                n
            }
        };
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        if !state.functions.contains_key(&function_name) {
            return Err(format!(
                "ResourceNotFoundException: Function not found: {function_name}"
            ));
        }
        let cfg = fakecloud_lambda::EventInvokeConfig {
            function_arn: fakecloud_lambda::function_arn(
                &self.region,
                &self.account_id,
                &function_name,
            ),
            maximum_event_age: event_age,
            maximum_retry_attempts: retries,
            destination_config: props
                .get("DestinationConfig")
                .filter(|v| v.is_object())
                .cloned(),
            last_modified: Utc::now(),
        };
        let key = format!("{function_name}:{qualifier}");
        state.event_invoke_configs.insert(key.clone(), cfg);
        Ok(ProvisionResult::new(key))
    }

    /// Put again on update: the template is the whole config (Put
    /// semantics, so dropped settings return to their defaults).
    pub(super) fn update_lambda_event_invoke_config(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let result = self.create_lambda_event_invoke_config(resource)?;
        if result.physical_id != existing.physical_id {
            self.delete_lambda_event_invoke_config(&existing.physical_id)?;
        }
        Ok(result)
    }

    pub(super) fn delete_lambda_event_invoke_config(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.lambda_state.write();
        let state = accounts.regional_mut(&self.account_id, &self.region);
        state.event_invoke_configs.remove(physical_id);
        Ok(())
    }
}
