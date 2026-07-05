use serde_json::{json, Value};

/// Encode data as an AWS event-stream message.
/// The event stream binary format:
///   [total_byte_length:4] [headers_byte_length:4] [prelude_crc:4]
///   [headers:*] [payload:*] [message_crc:4]
pub(crate) fn encode_event(event_type: &str, content_type: &str, payload: &[u8]) -> Vec<u8> {
    let headers = encode_headers(event_type, content_type);
    let headers_len = headers.len() as u32;

    // Total = 4 (total len) + 4 (headers len) + 4 (prelude CRC) + headers + payload + 4 (msg CRC)
    let total_len = 12 + headers_len + payload.len() as u32 + 4;

    let mut buf = Vec::with_capacity(total_len as usize);

    // Prelude
    buf.extend_from_slice(&total_len.to_be_bytes());
    buf.extend_from_slice(&headers_len.to_be_bytes());

    // Prelude CRC
    let prelude_crc = crc32(&buf[..8]);
    buf.extend_from_slice(&prelude_crc.to_be_bytes());

    // Headers
    buf.extend_from_slice(&headers);

    // Payload
    buf.extend_from_slice(payload);

    // Message CRC
    let msg_crc = crc32(&buf);
    buf.extend_from_slice(&msg_crc.to_be_bytes());

    buf
}

fn encode_headers(event_type: &str, content_type: &str) -> Vec<u8> {
    let mut headers = Vec::new();

    // :event-type header
    encode_string_header(&mut headers, ":event-type", event_type);

    // :content-type header
    encode_string_header(&mut headers, ":content-type", content_type);

    // :message-type header
    encode_string_header(&mut headers, ":message-type", "event");

    headers
}

fn encode_string_header(buf: &mut Vec<u8>, name: &str, value: &str) {
    // Header name: 1 byte length + name bytes
    buf.push(name.len() as u8);
    buf.extend_from_slice(name.as_bytes());

    // Header value type: 7 = string
    buf.push(7);

    // String value: 2 byte length + value bytes
    let value_len = value.len() as u16;
    buf.extend_from_slice(&value_len.to_be_bytes());
    buf.extend_from_slice(value.as_bytes());
}

/// CRC-32 (IEEE/CRC-32C is used by AWS but standard CRC-32 works for compatibility)
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

/// Build the complete event stream body for InvokeModelWithResponseStream.
/// Returns the full body as a single chunk containing all events.
pub(crate) fn build_invoke_stream_response(model_id: &str, response_text: &str) -> Vec<u8> {
    let mut body = Vec::new();

    // For Anthropic models, emit message_start, content_block_start, content_block_delta,
    // content_block_stop, message_delta, message_stop
    if model_id.starts_with("anthropic.") {
        // chunk event with the response
        let chunk = json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {
                "type": "text_delta",
                "text": response_text
            }
        });
        let payload = serde_json::to_vec(
            &json!({ "bytes": base64_encode(&serde_json::to_vec(&chunk).expect("serde_json::Value serialization is infallible")) }),
        )
        .expect("serde_json::Value serialization is infallible");
        body.extend(encode_event("chunk", "application/json", &payload));
    } else {
        // Generic: single chunk with the full response
        let chunk = json!({
            "outputText": response_text
        });
        let payload = serde_json::to_vec(
            &json!({ "bytes": base64_encode(&serde_json::to_vec(&chunk).expect("serde_json::Value serialization is infallible")) }),
        )
        .expect("serde_json::Value serialization is infallible");
        body.extend(encode_event("chunk", "application/json", &payload));
    }

    body
}

/// Build the complete event stream body for ConverseStream.
///
/// `input_tokens` and `output_tokens` are surfaced verbatim in the
/// terminating `metadata` event so callers see real, prompt-derived
/// usage figures instead of the historical 10/20 placeholder.
pub(crate) fn build_converse_stream_response(
    response_text: &str,
    input_tokens: u64,
    output_tokens: u64,
) -> Vec<u8> {
    let mut body = Vec::new();

    // messageStart event
    let start = json!({ "role": "assistant" });
    let payload = serde_json::to_vec(&json!({ "messageStart": start }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event("messageStart", "application/json", &payload));

    // contentBlockStart event
    let block_start = json!({ "contentBlockIndex": 0, "start": {} });
    let payload = serde_json::to_vec(&json!({ "contentBlockStart": block_start }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event(
        "contentBlockStart",
        "application/json",
        &payload,
    ));

    // contentBlockDelta event with the text
    let delta = json!({
        "contentBlockIndex": 0,
        "delta": {
            "text": response_text
        }
    });
    let payload = serde_json::to_vec(&json!({ "contentBlockDelta": delta }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event(
        "contentBlockDelta",
        "application/json",
        &payload,
    ));

    // contentBlockStop event
    let block_stop = json!({ "contentBlockIndex": 0 });
    let payload = serde_json::to_vec(&json!({ "contentBlockStop": block_stop }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event(
        "contentBlockStop",
        "application/json",
        &payload,
    ));

    // messageStop event
    let stop = json!({
        "stopReason": "end_turn"
    });
    let payload = serde_json::to_vec(&json!({ "messageStop": stop }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event("messageStop", "application/json", &payload));

    // metadata event
    let metadata = json!({
        "usage": {
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "totalTokens": input_tokens + output_tokens
        },
        "metrics": {
            "latencyMs": 100
        }
    });
    let payload = serde_json::to_vec(&json!({ "metadata": metadata }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event("metadata", "application/json", &payload));

    body
}

/// Re-encode a sequence of real upstream token deltas as a Bedrock
/// `InvokeModelWithResponseStream` event stream. Each `deltas` entry becomes
/// its own frame, so the caller streams genuine incremental tokens rather
/// than a single canned block.
///
/// Every provider is re-encoded into *its own* provider-native streaming
/// shape (matching the non-streaming `build_provider_response`) so the
/// aws-sdk decoder for each provider finds the field it expects: Anthropic
/// `content_block_delta.text`, Titan/Amazon `outputText`, Llama
/// `generation`, Cohere `text`, Mistral `outputs[].text`.
pub(crate) fn build_invoke_stream_from_deltas(
    model_id: &str,
    deltas: &[String],
    stop: crate::upstream::StopKind,
    input_tokens: u64,
    output_tokens: u64,
) -> Vec<u8> {
    use crate::upstream::{stop_anthropic, stop_cohere, stop_meta_mistral, stop_titan};
    let mut body = Vec::new();
    let family = crate::upstream::strip_region_prefix(model_id);

    if family.starts_with("anthropic.") {
        push_bytes_chunk(
            &mut body,
            &json!({
                "type": "message_start",
                "message": {
                    "id": "msg_fakecloudstream01",
                    "type": "message",
                    "role": "assistant",
                    "model": model_id,
                    "content": [],
                    "stop_reason": null,
                    "usage": { "input_tokens": input_tokens, "output_tokens": 0 }
                }
            }),
        );
        push_bytes_chunk(
            &mut body,
            &json!({
                "type": "content_block_start",
                "index": 0,
                "content_block": { "type": "text", "text": "" }
            }),
        );
        for delta in deltas {
            push_bytes_chunk(
                &mut body,
                &json!({
                    "type": "content_block_delta",
                    "index": 0,
                    "delta": { "type": "text_delta", "text": delta }
                }),
            );
        }
        push_bytes_chunk(
            &mut body,
            &json!({ "type": "content_block_stop", "index": 0 }),
        );
        push_bytes_chunk(
            &mut body,
            &json!({
                "type": "message_delta",
                "delta": { "stop_reason": stop_anthropic(stop), "stop_sequence": null },
                "usage": { "output_tokens": output_tokens }
            }),
        );
        push_bytes_chunk(&mut body, &json!({ "type": "message_stop" }));
    } else if family.starts_with("amazon.") {
        for (i, delta) in deltas.iter().enumerate() {
            push_bytes_chunk(
                &mut body,
                &json!({
                    "outputText": delta,
                    "index": 0,
                    "completionReason": Value::Null,
                    "inputTextTokenCount": if i == 0 { json!(input_tokens) } else { Value::Null }
                }),
            );
        }
        push_bytes_chunk(
            &mut body,
            &json!({
                "outputText": "",
                "index": 0,
                "totalOutputTextTokenCount": output_tokens,
                "completionReason": stop_titan(stop),
                "inputTextTokenCount": input_tokens
            }),
        );
    } else if family.starts_with("meta.") {
        for delta in deltas {
            push_bytes_chunk(
                &mut body,
                &json!({
                    "generation": delta,
                    "prompt_token_count": Value::Null,
                    "generation_token_count": Value::Null,
                    "stop_reason": Value::Null
                }),
            );
        }
        push_bytes_chunk(
            &mut body,
            &json!({
                "generation": "",
                "prompt_token_count": input_tokens,
                "generation_token_count": output_tokens,
                "stop_reason": stop_meta_mistral(stop)
            }),
        );
    } else if family.starts_with("cohere.") {
        for delta in deltas {
            push_bytes_chunk(
                &mut body,
                &json!({ "text": delta, "is_finished": false, "index": 0 }),
            );
        }
        push_bytes_chunk(
            &mut body,
            &json!({ "is_finished": true, "finish_reason": stop_cohere(stop) }),
        );
    } else if family.starts_with("mistral.") {
        for delta in deltas {
            push_bytes_chunk(
                &mut body,
                &json!({ "outputs": [{ "text": delta, "stop_reason": Value::Null }] }),
            );
        }
        push_bytes_chunk(
            &mut body,
            &json!({ "outputs": [{ "text": "", "stop_reason": stop_meta_mistral(stop) }] }),
        );
    } else {
        for delta in deltas {
            push_bytes_chunk(&mut body, &json!({ "outputText": delta }));
        }
    }

    body
}

/// Encode a single `chunk` event whose payload is the base64-wrapped JSON
/// value, matching how Bedrock frames provider-native streaming bodies.
fn push_bytes_chunk(body: &mut Vec<u8>, chunk: &Value) {
    let payload = serde_json::to_vec(&json!({
        "bytes": base64_encode(&serde_json::to_vec(chunk)
            .expect("serde_json::Value serialization is infallible"))
    }))
    .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event("chunk", "application/json", &payload));
}

/// Re-encode a sequence of real upstream token deltas as a Bedrock
/// `ConverseStream` event stream: one `contentBlockDelta` event per token,
/// terminated with real usage figures in the `metadata` event.
pub(crate) fn build_converse_stream_from_deltas(
    deltas: &[String],
    stop: crate::upstream::StopKind,
    input_tokens: u64,
    output_tokens: u64,
) -> Vec<u8> {
    let mut body = Vec::new();

    let start = json!({ "role": "assistant" });
    let payload = serde_json::to_vec(&json!({ "messageStart": start }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event("messageStart", "application/json", &payload));

    let block_start = json!({ "contentBlockIndex": 0, "start": {} });
    let payload = serde_json::to_vec(&json!({ "contentBlockStart": block_start }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event(
        "contentBlockStart",
        "application/json",
        &payload,
    ));

    for delta in deltas {
        let block_delta = json!({
            "contentBlockIndex": 0,
            "delta": { "text": delta }
        });
        let payload = serde_json::to_vec(&json!({ "contentBlockDelta": block_delta }))
            .expect("serde_json::Value serialization is infallible");
        body.extend(encode_event(
            "contentBlockDelta",
            "application/json",
            &payload,
        ));
    }

    let block_stop = json!({ "contentBlockIndex": 0 });
    let payload = serde_json::to_vec(&json!({ "contentBlockStop": block_stop }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event(
        "contentBlockStop",
        "application/json",
        &payload,
    ));

    let stop_event = json!({ "stopReason": crate::upstream::stop_converse(stop) });
    let payload = serde_json::to_vec(&json!({ "messageStop": stop_event }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event("messageStop", "application/json", &payload));

    let metadata = json!({
        "usage": {
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "totalTokens": input_tokens + output_tokens
        },
        "metrics": { "latencyMs": 100 }
    });
    let payload = serde_json::to_vec(&json!({ "metadata": metadata }))
        .expect("serde_json::Value serialization is infallible");
    body.extend(encode_event("metadata", "application/json", &payload));

    body
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Wrap response text for non-streaming provider fallback
pub(crate) fn default_stream_text() -> &'static str {
    "This is a test response from the emulated model."
}

/// Generate the canned response text, checking for prompt-conditional and
/// legacy custom overrides for the given call. When neither is set and
/// `FAKECLOUD_BEDROCK_ECHO=1` is exported, the caller's prompt is
/// reflected back so streaming tests can assert against their own input.
pub(crate) fn get_response_text(
    state: &crate::state::SharedBedrockState,
    req: &fakecloud_core::service::AwsRequest,
    model_id: &str,
    body: &[u8],
) -> String {
    let Some(custom) = crate::prompt::resolve_override(state, req, model_id, body) else {
        if crate::prompt::echo_enabled() {
            let echoed = crate::prompt::extract_prompt_text(model_id, body);
            return if echoed.is_empty() {
                "(empty prompt)".to_string()
            } else {
                echoed
            };
        }
        return default_stream_text().to_string();
    };
    override_text(model_id, custom)
}

/// Extract the assistant text from a configured simulation override. The
/// override may be a provider-native JSON body (Anthropic `content[].text`
/// or Converse `output.message.content[].text`) or an opaque raw string, in
/// which case it is returned verbatim. `model_id` is currently unused but
/// kept for symmetry with the provider-shaped extractors.
pub(crate) fn override_text(_model_id: &str, custom: String) -> String {
    if let Ok(parsed) = serde_json::from_str::<Value>(&custom) {
        if let Some(text) = parsed["content"][0]["text"].as_str() {
            return text.to_string();
        }
        if let Some(text) = parsed["output"]["message"]["content"][0]["text"].as_str() {
            return text.to_string();
        }
    }
    custom
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::BedrockState;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::AwsRequest;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn shared() -> crate::state::SharedBedrockState {
        let multi: MultiAccountState<BedrockState> =
            MultiAccountState::new("123456789012", "us-east-1", "http://x");
        Arc::new(RwLock::new(multi))
    }

    fn req() -> AwsRequest {
        AwsRequest {
            service: "bedrock".to_string(),
            action: "a".to_string(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query: String::new(),
            path_segments: vec![],
            query_params: HashMap::new(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "r".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    /// Parse prelude to verify framing and sizes.
    fn parse_prelude(buf: &[u8]) -> (u32, u32) {
        let total = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let headers = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
        (total, headers)
    }

    #[test]
    fn encode_event_produces_well_formed_frame() {
        let payload = b"{\"x\":1}";
        let buf = encode_event("chunk", "application/json", payload);
        let (total, headers_len) = parse_prelude(&buf);
        assert_eq!(total as usize, buf.len());
        let expected_headers = 1
            + ":event-type".len()
            + 1
            + 2
            + "chunk".len()
            + 1
            + ":content-type".len()
            + 1
            + 2
            + "application/json".len()
            + 1
            + ":message-type".len()
            + 1
            + 2
            + "event".len();
        assert_eq!(headers_len as usize, expected_headers);
        let start = 12 + headers_len as usize;
        assert_eq!(&buf[start..start + payload.len()], payload);
    }

    fn decode_inner_chunk(frame: &[u8]) -> Value {
        use base64::Engine;
        let s = String::from_utf8_lossy(frame);
        let bytes_start = s.find("\"bytes\":").unwrap() + "\"bytes\":".len();
        let after = &s[bytes_start..];
        let quote = after.find('"').unwrap();
        let end = after[quote + 1..].find('"').unwrap();
        let b64 = &after[quote + 1..quote + 1 + end];
        let raw = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        serde_json::from_slice(&raw).unwrap()
    }

    #[test]
    fn build_invoke_stream_response_anthropic_uses_content_block_delta() {
        let out = build_invoke_stream_response("anthropic.claude-3", "hello");
        let inner = decode_inner_chunk(&out);
        assert_eq!(inner["type"], "content_block_delta");
        assert_eq!(inner["delta"]["text"], "hello");
    }

    #[test]
    fn build_invoke_stream_response_generic_uses_output_text() {
        let out = build_invoke_stream_response("amazon.titan-text-lite", "hi");
        let inner = decode_inner_chunk(&out);
        assert_eq!(inner["outputText"], "hi");
    }

    #[test]
    fn build_converse_stream_emits_all_events() {
        let out = build_converse_stream_response("hello world", 5, 7);
        let s = String::from_utf8_lossy(&out);
        for marker in [
            "messageStart",
            "contentBlockStart",
            "contentBlockDelta",
            "contentBlockStop",
            "messageStop",
            "metadata",
            "end_turn",
        ] {
            assert!(s.contains(marker), "missing marker {marker}");
        }
        // Token counts surface via metadata, not a hardcoded 10/20.
        assert!(s.contains("\"inputTokens\":5"));
        assert!(s.contains("\"outputTokens\":7"));
        assert!(s.contains("\"totalTokens\":12"));
    }

    #[test]
    fn default_stream_text_is_non_empty() {
        assert!(!default_stream_text().is_empty());
    }

    #[test]
    fn get_response_text_returns_default_without_override() {
        // Serialize with the other echo-env tests and force echo OFF so the
        // expectation is deterministic under threaded `cargo test`.
        let _env = crate::prompt::ECHO_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var("FAKECLOUD_BEDROCK_ECHO").ok();
        let prev_legacy = std::env::var("BEDROCK_ECHO").ok();
        // SAFETY: env is process-global; restored below, under the lock.
        unsafe {
            std::env::remove_var("FAKECLOUD_BEDROCK_ECHO");
            std::env::remove_var("BEDROCK_ECHO");
        }

        let s = shared();
        let out = get_response_text(&s, &req(), "anthropic.claude", b"{}");
        assert_eq!(out, default_stream_text());

        // SAFETY: see above.
        unsafe {
            match prev {
                Some(p) => std::env::set_var("FAKECLOUD_BEDROCK_ECHO", p),
                None => std::env::remove_var("FAKECLOUD_BEDROCK_ECHO"),
            }
            match prev_legacy {
                Some(p) => std::env::set_var("BEDROCK_ECHO", p),
                None => std::env::remove_var("BEDROCK_ECHO"),
            }
        }
    }

    fn install_rule(state: &crate::state::SharedBedrockState, model: &str, response: &str) {
        let mut st = state.write();
        let acct = st.get_or_create("123456789012");
        acct.response_rules.insert(
            model.to_string(),
            vec![crate::state::ResponseRule {
                prompt_contains: None,
                response: response.to_string(),
            }],
        );
    }

    #[test]
    fn get_response_text_extracts_anthropic_content_when_override_present() {
        let s = shared();
        install_rule(
            &s,
            "anthropic.claude",
            &json!({ "content": [{"text": "FROM-RULE"}] }).to_string(),
        );
        let out = get_response_text(&s, &req(), "anthropic.claude", b"{}");
        assert_eq!(out, "FROM-RULE");
    }

    #[test]
    fn get_response_text_extracts_converse_output_when_override_present() {
        let s = shared();
        install_rule(
            &s,
            "anthropic.claude",
            &json!({
                "output": {"message": {"content": [{"text": "CONV-TEXT"}]}}
            })
            .to_string(),
        );
        let out = get_response_text(&s, &req(), "anthropic.claude", b"{}");
        assert_eq!(out, "CONV-TEXT");
    }

    #[test]
    fn get_response_text_returns_raw_string_when_not_parseable() {
        let s = shared();
        install_rule(&s, "anthropic.claude", "plain raw");
        let out = get_response_text(&s, &req(), "anthropic.claude", b"{}");
        assert_eq!(out, "plain raw");
    }

    /// Echo mode test mutates the process-global env var; we don't
    /// share a lock with sibling test files (the `cargo test` binary
    /// is per-crate and these are different modules), so we restore
    /// the previous value as a defensive cleanup.
    #[test]
    fn get_response_text_echo_mode_reflects_prompt() {
        let _env = crate::prompt::ECHO_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var("FAKECLOUD_BEDROCK_ECHO").ok();
        // SAFETY: scoped mutation below restores the previous value.
        unsafe { std::env::set_var("FAKECLOUD_BEDROCK_ECHO", "1") };

        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":"hello stream"}]}"#;
        let out = get_response_text(&s, &req(), "anthropic.claude-3", body);
        assert!(out.contains("hello stream"), "echo missing prompt: {out}");

        // SAFETY: see above.
        unsafe {
            match prev {
                Some(p) => std::env::set_var("FAKECLOUD_BEDROCK_ECHO", p),
                None => std::env::remove_var("FAKECLOUD_BEDROCK_ECHO"),
            }
        }
    }

    #[test]
    fn build_converse_stream_response_emits_dynamic_token_counts() {
        let out = build_converse_stream_response("text", 42, 7);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\"inputTokens\":42"));
        assert!(s.contains("\"outputTokens\":7"));
        assert!(s.contains("\"totalTokens\":49"));
    }
}
