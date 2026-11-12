//! `LambdaService` `stream` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl LambdaService {
    /// `InvokeWithResponseStream` — invoke the function and serialize
    /// its response as a sequence of `application/vnd.amazon.eventstream`
    /// frames. AWS uses this protocol for response-streaming Lambda
    /// invocations (Node.js `awslambda.streamifyResponse`, Python
    /// streaming handlers, custom runtimes that flush mid-handler).
    ///
    /// On success: zero or more `PayloadChunk` events (one per chunk
    /// the runtime sent) followed by an `InvokeComplete` event with
    /// `ErrorCode = null`. On a function error (uncaught exception in
    /// the handler) or an infrastructure error (timeout, container
    /// crash): an `InvokeComplete` with non-null `ErrorCode`/
    /// `ErrorDetails`. The HTTP status itself is always 200 — failures
    /// surface inside the trailing event, matching AWS.
    pub(super) async fn invoke_with_response_stream(
        &self,
        function_name: &str,
        account_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Resolve the function under the same rules as buffered Invoke
        // — qualifier, version snapshots, attached layers, code-zip
        // presence — but without the InvocationType branch (streaming
        // is always synchronous).
        // `?Qualifier=` wins; otherwise a qualifier embedded in the function
        // reference in the path (`name:alias`, a qualified ARN), as on Invoke.
        let requested_qualifier: Option<String> =
            req.query_params.get("Qualifier").cloned().or_else(|| {
                req.path_segments
                    .get(2)
                    .and_then(|raw| crate::service::qualifier_from_function_ref(raw))
            });
        let (func, layer_zips, executed_version) = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, &req.region);
            let state = accounts.regional(account_id, &req.region).unwrap_or(&empty);
            let qualifier = requested_qualifier
                .as_deref()
                .or_else(|| crate::service::default_invoke_qualifier(state, function_name));
            // Resolved exactly as Invoke resolves it: an unknown alias or
            // version is not found (it never runs `$LATEST` instead), and an
            // alias at `$LATEST` runs the live function.
            let resolved = crate::service::resolve_invoke(state, function_name, qualifier)
                .ok_or_else(|| {
                    crate::service::invoke_not_found(
                        &req.region,
                        account_id,
                        function_name,
                        qualifier,
                    )
                })?;
            let func = resolved.func.clone();
            let zips = crate::state::attached_layer_zips(&accounts, &func);
            (func, zips, resolved.executed_version)
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
                format!(
                    "Docker/Podman is required for Lambda execution but is not available. {}",
                    fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                ),
            )
        })?;

        // Relay the runtime's response as it arrives and assemble the
        // eventstream body. We buffer all frames before returning
        // (`AwsResponse` is byte-buffered today), but each piece the runtime
        // sent (one per `responseStream.write`) stays its own
        // `PayloadChunk` event, so SDK parsers see exactly the streaming
        // structure they expect.
        let mut frames: Vec<u8> = Vec::new();
        let ctx = crate::runtime::InvocationContext {
            request_id: Some(req.request_id.clone()),
            invoked_function_arn: Some(crate::service::invoke::invoked_function_arn(
                &func.function_arn,
                requested_qualifier.as_deref(),
            )),
            trace_id: crate::service::invoke::trace_header(req),
            client_context: crate::service::invoke::client_context(req),
            cognito_identity: None,
        };
        let invoke_result = runtime
            .invoke_streaming(&func, &req.body, &layer_zips, &ctx, &mut |chunk| {
                frames.extend_from_slice(&crate::eventstream::payload_chunk_frame(&chunk));
            })
            .await;

        let (error_code, error_details) = match invoke_result {
            Ok(crate::runtime::RunOutcome::Success) => (None, None),
            // A function error ends the stream with an `InvokeComplete`
            // naming the error object's type and message.
            Ok(crate::runtime::RunOutcome::Error(payload)) => {
                let output = fakecloud_core::delivery::LambdaInvocationOutput::unhandled(payload);
                (
                    Some(
                        output
                            .error_type()
                            .unwrap_or_else(|| "Unhandled".to_string()),
                    ),
                    Some(output.error_message().unwrap_or_default()),
                )
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
