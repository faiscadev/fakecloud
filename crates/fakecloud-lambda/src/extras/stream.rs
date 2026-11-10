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
    /// the RIE flushed) followed by an `InvokeComplete` event with
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
        let default_qualifier = {
            let accounts = self.state.read();
            accounts
                .regional(account_id, &req.region)
                .and_then(|s| crate::service::default_invoke_qualifier(s, function_name))
        };
        let qualifier = req
            .query_params
            .get("Qualifier")
            .map(String::as_str)
            .or(default_qualifier);

        let resolved_version: Option<String> = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, &req.region);
            let state = accounts.regional(account_id, &req.region).unwrap_or(&empty);
            crate::service::resolve_qualifier_to_version(state, function_name, qualifier)
        };
        let executed_version = resolved_version
            .clone()
            .unwrap_or_else(|| "$LATEST".to_string());

        let (func, layer_zips) = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, &req.region);
            let state = accounts.regional(account_id, &req.region).unwrap_or(&empty);
            let func = match resolved_version.as_deref() {
                Some(v) => state
                    .version_snapshot(function_name, v)
                    .cloned()
                    // A numeric version without a snapshot is legacy state
                    // and runs `$LATEST`, as Invoke does; an alias that does
                    // not exist is not found.
                    .or_else(|| {
                        v.chars()
                            .all(|c| c.is_ascii_digit())
                            .then(|| state.functions.get(function_name).cloned())
                            .flatten()
                    }),
                None => state.functions.get(function_name).cloned(),
            }
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!(
                        "Function not found: {}{}",
                        function_arn(&req.region, &state.account_id, function_name),
                        qualifier.map(|q| format!(":{q}")).unwrap_or_default()
                    ),
                )
            })?;
            let zips = crate::state::attached_layer_zips(&accounts, &func);
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
                format!(
                    "Docker/Podman is required for Lambda execution but is not available. {}",
                    fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                ),
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
                let status = stream.status();
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
                            // Stream-invoke contract: errors are encoded
                            // in-stream as a terminal InvokeComplete frame
                            // with HTTP 200. Returning HTTP 500 here breaks
                            // SDK parsers expecting that envelope.
                            tracing::error!(function = %function_name, error = %e, "Lambda streaming chunk read failed");
                            let detail = format!("Lambda streaming read failed: {e}");
                            frames.extend_from_slice(&crate::eventstream::invoke_complete_frame(
                                Some("Runtime.StreamReadFailure"),
                                Some(&detail),
                                "",
                            ));
                            return Ok(AwsResponse {
                                status: StatusCode::OK,
                                headers: http::HeaderMap::new(),
                                body: bytes::Bytes::from(frames).into(),
                                content_type: "application/vnd.amazon.eventstream".to_string(),
                            });
                        }
                    }
                }

                // A function error ends the stream with the runtime's error
                // object as the final chunk (or the whole body, for a
                // runtime that exited / timed out). The runtime layer's
                // classification decides, and its error object names the
                // terminal `InvokeComplete` event's code and details.
                let mut error: Option<(String, String)> = None;
                let tail = last_chunk.unwrap_or_default();
                if had_chunks || status != 200 {
                    let output = crate::runtime::classify_invocation_response(status, &tail);
                    if output.is_function_error() {
                        error = Some((
                            output
                                .error_type()
                                .unwrap_or_else(|| "Runtime.Unknown".to_string()),
                            output
                                .error_message()
                                .unwrap_or_else(|| "function error".to_string()),
                        ));
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
