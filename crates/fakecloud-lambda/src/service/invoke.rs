//! `LambdaService` `invoke` family — extracted from service.rs by audit-2026-05-19.

use base64::Engine as _;

use super::*;

impl LambdaService {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn invoke(
        &self,
        function_name: &str,
        payload: &[u8],
        account_id: &str,
        region: &str,
        invocation_type: InvocationType,
        qualifier: Option<&str>,
        log_tail: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        // An unknown alias must 404 like GetFunction does, not silently fall
        // through to $LATEST. resolve_qualifier_to_version returns None for both
        // "$LATEST" and a non-existent alias, conflating "run live" with "not
        // found" -- so a typo'd alias quietly invoked prod $LATEST. Reject an
        // alias qualifier that doesn't exist here (bug-audit 2026-06-20, 1.9).
        // Numeric versions keep the existing lenient fallback (a missing version
        // snapshot is legacy state, handled below).
        if let Some(q) = qualifier {
            if q != "$LATEST" && !q.chars().all(|c| c.is_ascii_digit()) {
                let accounts = self.state.read();
                let empty = LambdaState::new(account_id, "");
                let state = accounts.get(account_id).unwrap_or(&empty);
                if !state.aliases.contains_key(&format!("{function_name}:{q}")) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ResourceNotFoundException",
                        format!(
                            "Function not found: arn:aws:lambda:{}:{}:function:{function_name}:{q}",
                            region, state.account_id
                        ),
                    ));
                }
            }
        }
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
        // Tracks the version actually executed — diverges from the
        // requested version when the version snapshot is missing and
        // we fall back to $LATEST code below.
        let mut executed_version = resolved_version
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
                Some(v) => {
                    let snap = state
                        .function_version_snapshots
                        .get(function_name)
                        .and_then(|m| m.get(v))
                        .cloned();
                    if snap.is_none() {
                        // Snapshot missing: code is whatever is in
                        // `functions` ($LATEST). Reflect that in the
                        // response header so callers can't be misled
                        // into thinking they ran v=`v`.
                        executed_version = "$LATEST".to_string();
                    }
                    snap.or_else(|| state.functions.get(function_name).cloned())
                }
                None => state.functions.get(function_name).cloned(),
            }
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: arn:aws:lambda:{}:{}:function:{}",
                        region, state.account_id, function_name
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

        if func.code_zip.is_none() && func.package_type != "Image" {
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
                    let destination_config =
                        self.lookup_destination_config(&func, account_id, qualifier);
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
                    // With `LogType=Tail` AWS returns the base64 of the last
                    // 4 KiB of the invocation's logs in `X-Amz-Log-Result`; the
                    // buffered path previously never set it (bug-hunt
                    // 2026-06-24, 1.20).
                    let invoke_result = if log_tail {
                        runtime
                            .invoke_with_log_tail(&func, payload, &layer_zips)
                            .await
                    } else {
                        runtime
                            .invoke(&func, payload, &layer_zips)
                            .await
                            .map(|b| (b, None))
                    };
                    match invoke_result.map(|(bytes, logs)| (bytes, logs.filter(|_| log_tail))) {
                        Ok((response_bytes, log_tail_text)) => {
                            // A handler that throws still returns HTTP 200 with
                            // the error payload in the body; AWS signals it with
                            // the `X-Amz-Function-Error` header (surfaced as
                            // `FunctionError` by the SDKs). Without it, boto3/JS
                            // callers treat a failed invocation as success. The
                            // async Event branch already detects this envelope;
                            // mirror it here (bug-audit 2026-06-20, 1.8).
                            let is_function_error = is_function_error_payload(&response_bytes);
                            let mut resp = AwsResponse::json(StatusCode::OK, response_bytes);
                            if let Ok(v) = http::header::HeaderValue::from_str(&executed_version) {
                                resp.headers.insert(
                                    http::header::HeaderName::from_static("x-amz-executed-version"),
                                    v,
                                );
                            }
                            if is_function_error {
                                // The error envelope (errorMessage/errorType)
                                // means the handler raised an exception the
                                // runtime caught and serialized — AWS labels
                                // that "Handled". "Unhandled" is for runtime
                                // crashes (OOM/timeout/init) that never produce
                                // an envelope.
                                resp.headers.insert(
                                    http::header::HeaderName::from_static("x-amz-function-error"),
                                    http::header::HeaderValue::from_static("Handled"),
                                );
                            }
                            // LogType=Tail: base64 of the last 4 KiB of logs.
                            if let Some(text) = log_tail_text {
                                let bytes = text.as_bytes();
                                let tail = &bytes[bytes.len().saturating_sub(4096)..];
                                let encoded =
                                    base64::engine::general_purpose::STANDARD.encode(tail);
                                if let Ok(v) = http::header::HeaderValue::from_str(&encoded) {
                                    resp.headers.insert(
                                        http::header::HeaderName::from_static("x-amz-log-result"),
                                        v,
                                    );
                                }
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
                format!(
                    "Docker/Podman is required for Lambda execution but is not available. {}",
                    fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                ),
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
        qualifier: Option<&str>,
    ) -> Option<serde_json::Value> {
        let accounts = self.state.read();
        let state = accounts.get(account_id)?;
        // EventInvokeConfig is keyed per-qualifier on AWS — alias/version
        // can carry its own DestinationConfig. Walk qualifier-specific
        // first, then fall back to $LATEST so unqualified invokes still
        // see function-level config.
        let candidates = [qualifier.unwrap_or("$LATEST"), "$LATEST"];
        for q in candidates {
            let key = format!("{}:{}", func.function_name, q);
            if let Some(cfg) = state.event_invoke_configs.get(&key) {
                if let Some(dc) = cfg.destination_config.clone() {
                    if !dc.is_null() && !dc.as_object().map(|o| o.is_empty()).unwrap_or(false) {
                        return Some(dc);
                    }
                }
            }
        }
        None
    }
}

/// True when a synchronous invoke's response body is a Lambda function-error
/// envelope (`{"errorMessage": ..., "errorType": ...}`). The runtime returns
/// HTTP 200 with this body when the handler throws; the caller sets
/// `X-Amz-Function-Error` so SDKs surface it as `FunctionError`
/// (bug-audit 2026-06-20, 1.8).
pub(crate) fn is_function_error_payload(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .as_ref()
        .and_then(|v| v.as_object())
        .map(|m| m.contains_key("errorMessage") || m.contains_key("errorType"))
        .unwrap_or(false)
}

#[cfg(test)]
mod invoke_error_tests {
    use super::is_function_error_payload;

    #[test]
    fn detects_function_error_envelope() {
        assert!(is_function_error_payload(
            br#"{"errorMessage":"boom","errorType":"Error"}"#
        ));
        assert!(is_function_error_payload(
            br#"{"errorType":"Runtime.Error"}"#
        ));
    }

    #[test]
    fn plain_result_is_not_a_function_error() {
        assert!(!is_function_error_payload(br#"{"statusCode":200}"#));
        assert!(!is_function_error_payload(br#""just a string""#));
        assert!(!is_function_error_payload(b"not json"));
    }
}
