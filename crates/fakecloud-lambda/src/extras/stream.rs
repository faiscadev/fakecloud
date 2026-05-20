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
        let qualifier = req.query_params.get("Qualifier").map(String::as_str);

        let resolved_version: Option<String> = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, "");
            let state = accounts.get(account_id).unwrap_or(&empty);
            crate::service::resolve_qualifier_to_version(state, function_name, qualifier)
        };
        let executed_version = resolved_version
            .clone()
            .unwrap_or_else(|| "$LATEST".to_string());

        let (func, layer_zips) = {
            let accounts = self.state.read();
            let empty = LambdaState::new(account_id, "");
            let state = accounts.get(account_id).unwrap_or(&empty);
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
            let mut zips: Vec<Vec<u8>> = Vec::with_capacity(func.layers.len());
            for attached in &func.layers {
                if let Some(b) =
                    parse_layer_version_arn(&attached.arn).and_then(|(acct, name, ver)| {
                        accounts
                            .get(&acct)
                            .and_then(|s| s.layers.get(&name))
                            .and_then(|l| l.versions.iter().find(|v| v.version == ver))
                            .and_then(|v| v.code_zip.clone())
                    })
                {
                    zips.push(b);
                }
            }
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
                "Docker/Podman is required for Lambda execution but is not available",
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
                            tracing::error!(function = %function_name, error = %e, "Lambda streaming chunk read failed");
                            return Err(AwsServiceError::aws_error(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "ServiceException",
                                format!("Lambda streaming read failed: {e}"),
                            ));
                        }
                    }
                }

                // The Lambda runtime returns 200 even when the user
                // handler threw, packing `errorMessage`/`errorType`
                // into the buffered body. Streaming handlers do the
                // same on the final chunk. Inspect the last chunk we
                // saw and surface that as a function error in the
                // terminal `InvokeComplete` event.
                let mut error: Option<(String, String)> = None;
                if had_chunks {
                    if let Some(bytes) = last_chunk {
                        if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
                            if let Some(obj) = v.as_object() {
                                if obj.contains_key("errorMessage") || obj.contains_key("errorType")
                                {
                                    let etype = obj
                                        .get("errorType")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("Runtime.Unknown")
                                        .to_string();
                                    let emsg = obj
                                        .get("errorMessage")
                                        .and_then(|x| x.as_str())
                                        .unwrap_or("function error")
                                        .to_string();
                                    error = Some((etype, emsg));
                                }
                            }
                        }
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
