//! `LambdaService` `invoke` family — extracted from service.rs by audit-2026-05-19.

use base64::Engine as _;

use super::*;

/// The ARN a caller invoked a function through: its ARN, qualified when the
/// caller named a version or alias (`Lambda-Runtime-Invoked-Function-Arn`).
pub(crate) fn invoked_function_arn(function_arn: &str, qualifier: Option<&str>) -> String {
    match qualifier.filter(|q| !q.is_empty()) {
        Some(q) => format!("{function_arn}:{q}"),
        None => function_arn.to_string(),
    }
}

/// The caller's X-Ray trace header, passed on to the runtime.
pub(crate) fn trace_header(req: &AwsRequest) -> Option<String> {
    req.headers
        .get("x-amzn-trace-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

/// The client context the caller sent (`X-Amz-Client-Context`, base64 JSON),
/// decoded as the runtime receives it.
pub(crate) fn client_context(req: &AwsRequest) -> Option<String> {
    let raw = req.headers.get("x-amz-client-context")?.to_str().ok()?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw.trim())
        .ok()?;
    String::from_utf8(decoded).ok()
}

/// The invocation details a synchronous or asynchronous `Invoke` hands the
/// runtime.
pub(crate) fn invocation_context(
    req: &AwsRequest,
    function_arn: &str,
    qualifier: Option<&str>,
) -> crate::runtime::InvocationContext {
    crate::runtime::InvocationContext {
        request_id: Some(req.request_id.clone()),
        invoked_function_arn: Some(invoked_function_arn(function_arn, qualifier)),
        trace_id: trace_header(req),
        client_context: client_context(req),
        cognito_identity: None,
    }
}

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
        request_id: &str,
        ctx: &crate::runtime::InvocationContext,
    ) -> Result<AwsResponse, AwsServiceError> {
        // An unqualified invoke of a Lambda Managed Instances function runs
        // its `$LATEST.PUBLISHED` version rather than the unpublished
        // `$LATEST`.
        let default_qualifier = {
            let accounts = self.state.read();
            accounts
                .regional(account_id, region)
                .and_then(|s| crate::service::default_invoke_qualifier(s, function_name))
        };
        let qualifier = qualifier.or(default_qualifier);
        // The version that runs, picked once: an alias's (a weighted alias's
        // routing config picks between its versions, surfaced through
        // `x-amz-executed-version`), the number named, `$LATEST.PUBLISHED`,
        // or `$LATEST`. A numbered version runs the immutable snapshot
        // PublishVersion stored, so an alias pinned to v1 runs the v1 code
        // even after $LATEST is mutated. An unknown alias or version is not
        // found: it never runs `$LATEST` instead.
        let (func, layer_zips, executed_version, alias) = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, region);
            let state = accounts.regional(account_id, region).unwrap_or(&empty);
            let resolved = resolve_invoke(state, function_name, qualifier)
                .ok_or_else(|| invoke_not_found(region, account_id, function_name, qualifier))?;
            let func = resolved.func.clone();
            // Resolve attached layer ARNs to ZIP bytes under the same read
            // lock. Layers may live in sibling accounts (cross-account
            // attach is legal in AWS); an unresolvable layer is skipped with
            // a warning and invoke proceeds without it.
            let layer_zips = crate::state::attached_layer_zips(&accounts, &func);
            (func, layer_zips, resolved.executed_version, resolved.alias)
        };

        // Reserved-concurrency gate runs before code-zip / DryRun
        // checks: AWS returns 429 even for functions without a code
        // package as long as the cap is already hit, since throttling
        // is request-rate, not deployment-state.
        let concurrency_key = format!("{account_id}:{region}:{function_name}");
        let _concurrency_guard = {
            let cap = {
                let accounts = self.state.read();
                accounts
                    .regional(account_id, region)
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

        let mut function_errored = false;
        let result: Result<AwsResponse, AwsServiceError> = if let Some(resp) = dry_run_response {
            Ok(resp)
        } else if let Some(runtime) = runtime_for_invoke {
            match invocation_type {
                InvocationType::Event => {
                    // Lambda queues the event and answers 202; the queued
                    // invocation runs with the function's async error
                    // handling (retries, event age, destinations, DLQ). The
                    // concurrency guard travels with it so the counter only
                    // drops once the async work finishes.
                    super::async_invoke::spawn(super::async_invoke::AsyncInvocation {
                        runtime: runtime.clone(),
                        bus: self.delivery_bus.clone(),
                        settings: self.async_settings(&func, account_id, region, qualifier),
                        func: func.clone(),
                        resolve: super::async_invoke::invoked_version_resolver(
                            self.state.clone(),
                            super::async_invoke::InvokedVersion::new(
                                account_id,
                                region,
                                function_name,
                                alias.clone(),
                                &executed_version,
                                &func,
                            ),
                        ),
                        payload: payload.to_vec(),
                        executed_version: executed_version.clone(),
                        request_id: request_id.to_string(),
                        context: ctx.clone(),
                        guard: Some(Box::new(_concurrency_guard)),
                        on_unrunnable: None,
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
                            .invoke_with_log_tail(&func, payload, &layer_zips, ctx)
                            .await
                    } else {
                        runtime.invoke_with(&func, payload, &layer_zips, ctx).await
                    };
                    match invoke_result.map(|(output, logs)| (output, logs.filter(|_| log_tail))) {
                        Ok((output, log_tail_text)) => {
                            // A function error still answers HTTP 200; AWS
                            // signals it with `X-Amz-Function-Error` (surfaced
                            // as `FunctionError` by the SDKs) and the error
                            // object as the payload. The runtime layer decides
                            // which invocations failed.
                            function_errored = output.is_function_error();
                            let mut resp = AwsResponse::json(StatusCode::OK, output.payload);
                            if let Ok(v) = http::header::HeaderValue::from_str(&executed_version) {
                                resp.headers.insert(
                                    http::header::HeaderName::from_static("x-amz-executed-version"),
                                    v,
                                );
                            }
                            if let Some(function_error) = output.function_error.as_deref() {
                                if let Ok(v) = http::header::HeaderValue::from_str(function_error) {
                                    resp.headers.insert(
                                        http::header::HeaderName::from_static(
                                            "x-amz-function-error",
                                        ),
                                        v,
                                    );
                                }
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
            bus.put_cloudwatch_metric(
                account_id,
                region,
                "AWS/Lambda",
                "Invocations",
                1.0,
                Some("Count"),
                dims.clone(),
                now_ms,
            );
            bus.put_cloudwatch_metric(
                account_id,
                region,
                "AWS/Lambda",
                "Duration",
                invoke_start.elapsed().as_millis() as f64,
                Some("Milliseconds"),
                dims.clone(),
                now_ms,
            );
            // `Errors` counts invocations that resulted in a function
            // error as well as ones Lambda could not run.
            if result.is_err() || function_errored {
                bus.put_cloudwatch_metric(
                    account_id,
                    region,
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

    /// The function's async error-handling settings for `qualifier`.
    pub(crate) fn async_settings(
        &self,
        func: &crate::state::LambdaFunction,
        account_id: &str,
        region: &str,
        qualifier: Option<&str>,
    ) -> super::async_invoke::AsyncSettings {
        let accounts = self.state.read();
        super::async_invoke::resolve_settings(
            &accounts,
            account_id,
            region,
            &func.function_name,
            qualifier,
        )
    }
}
