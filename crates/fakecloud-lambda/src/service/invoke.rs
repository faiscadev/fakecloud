//! `LambdaService` `invoke` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    pub(crate) async fn invoke(
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
    pub(crate) fn lookup_destination_config(
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
}
