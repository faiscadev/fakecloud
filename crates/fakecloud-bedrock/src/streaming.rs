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

/// Append one event to a stream. An event-stream message's payload holds the
/// event's own members; its type travels only in the `:event-type` header, so
/// wrapping the members in the event name leaves a decoder with an empty event.
fn push_event(body: &mut Vec<u8>, event_type: &str, members: &Value) {
    let payload =
        serde_json::to_vec(members).expect("serde_json::Value serialization is infallible");
    body.extend(encode_event(event_type, "application/json", &payload));
}

/// The `metadata` event closing a ConverseStream.
fn push_converse_metadata(body: &mut Vec<u8>, input_tokens: u64, output_tokens: u64) {
    push_event(
        body,
        "metadata",
        &json!({
            "usage": {
                "inputTokens": input_tokens,
                "outputTokens": output_tokens,
                "totalTokens": input_tokens + output_tokens
            },
            "metrics": { "latencyMs": 100 }
        }),
    );
}

/// Build the complete event stream body for ConverseStream.
///
/// `input_tokens` and `output_tokens` are surfaced verbatim in the
/// terminating `metadata` event so callers see real, prompt-derived
/// usage figures instead of the historical 10/20 placeholder.
///
/// `stop_reason` and `forced_tool` are computed by the handler from the same
/// inference config / `toolChoice` the non-streaming `Converse` path reads, so
/// both operations return a consistent shape for identical input. When
/// `forced_tool` is set, a second `toolUse` content block is streamed after
/// the text block and `stop_reason` is `tool_use`.
pub(crate) fn build_converse_stream_response(
    response_text: &str,
    stop_reason: &str,
    forced_tool: Option<&str>,
    input_tokens: u64,
    output_tokens: u64,
) -> Vec<u8> {
    let mut body = Vec::new();

    push_event(&mut body, "messageStart", &json!({ "role": "assistant" }));

    // A text block opens with its first delta: `ContentBlockStart` is a union
    // with no text member, so there is nothing a start event could say.
    push_event(
        &mut body,
        "contentBlockDelta",
        &json!({ "contentBlockIndex": 0, "delta": { "text": response_text } }),
    );
    push_event(
        &mut body,
        "contentBlockStop",
        &json!({ "contentBlockIndex": 0 }),
    );

    // Optional forced toolUse content block (index 1), mirroring Converse.
    if let Some(tool_name) = forced_tool {
        push_event(
            &mut body,
            "contentBlockStart",
            &json!({
                "contentBlockIndex": 1,
                "start": { "toolUse": { "toolUseId": "tooluse_fakecloud_01", "name": tool_name } }
            }),
        );
        push_event(
            &mut body,
            "contentBlockDelta",
            &json!({ "contentBlockIndex": 1, "delta": { "toolUse": { "input": "{}" } } }),
        );
        push_event(
            &mut body,
            "contentBlockStop",
            &json!({ "contentBlockIndex": 1 }),
        );
    }

    push_event(
        &mut body,
        "messageStop",
        &json!({ "stopReason": stop_reason }),
    );
    push_converse_metadata(&mut body, input_tokens, output_tokens);

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

/// A tool call to stream as a `toolUse` content block; `input` is its JSON text.
pub(crate) struct StreamedToolUse<'a> {
    pub(crate) id: &'a str,
    pub(crate) name: &'a str,
    pub(crate) input: String,
}

/// Re-encode a sequence of real upstream token deltas as a Bedrock
/// `ConverseStream` event stream: one `contentBlockDelta` event per token, a
/// `toolUse` block per tool the model called, terminated with real usage figures
/// in the `metadata` event.
pub(crate) fn build_converse_stream_from_deltas(
    deltas: &[String],
    tools: &[StreamedToolUse<'_>],
    stop: crate::upstream::StopKind,
    input_tokens: u64,
    output_tokens: u64,
) -> Vec<u8> {
    let mut body = Vec::new();

    push_event(&mut body, "messageStart", &json!({ "role": "assistant" }));
    // A delta is what opens a text block, so an empty reply with no tool calls
    // still needs one for its `contentBlockStop` to close something.
    let empty = [String::new()];
    let deltas = if deltas.is_empty() && tools.is_empty() {
        &empty[..]
    } else {
        deltas
    };
    for delta in deltas {
        push_event(
            &mut body,
            "contentBlockDelta",
            &json!({ "contentBlockIndex": 0, "delta": { "text": delta } }),
        );
    }
    let mut next_index = 0;
    if !deltas.is_empty() {
        push_event(
            &mut body,
            "contentBlockStop",
            &json!({ "contentBlockIndex": 0 }),
        );
        next_index = 1;
    }
    for (offset, tool) in tools.iter().enumerate() {
        let index = next_index + offset;
        push_event(
            &mut body,
            "contentBlockStart",
            &json!({
                "contentBlockIndex": index,
                "start": { "toolUse": { "toolUseId": tool.id, "name": tool.name } }
            }),
        );
        push_event(
            &mut body,
            "contentBlockDelta",
            &json!({ "contentBlockIndex": index, "delta": { "toolUse": { "input": tool.input } } }),
        );
        push_event(
            &mut body,
            "contentBlockStop",
            &json!({ "contentBlockIndex": index }),
        );
    }
    push_event(
        &mut body,
        "messageStop",
        &json!({ "stopReason": crate::upstream::stop_converse(stop) }),
    );
    push_converse_metadata(&mut body, input_tokens, output_tokens);

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
    match crate::prompt::resolve_override(state, req, model_id, body) {
        Some(custom) => override_text(model_id, custom),
        None => fallback_text(model_id, body),
    }
}

/// The reply text when no override is configured: the caller's prompt in
/// echo mode, else the canned phrase.
pub(crate) fn fallback_text(model_id: &str, body: &[u8]) -> String {
    if crate::prompt::echo_enabled() {
        let echoed = crate::prompt::extract_prompt_text(model_id, body);
        return if echoed.is_empty() {
            "(empty prompt)".to_string()
        } else {
            echoed
        };
    }
    default_stream_text().to_string()
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

    /// Each frame's `:event-type` header and JSON payload.
    fn frames(mut buf: &[u8]) -> Vec<(String, Value)> {
        let mut out = Vec::new();
        while !buf.is_empty() {
            let (total, headers_len) = parse_prelude(buf);
            let (total, headers_len) = (total as usize, headers_len as usize);
            let mut headers = &buf[12..12 + headers_len];
            let mut event_type = None;
            while !headers.is_empty() {
                let name_len = headers[0] as usize;
                let name = &headers[1..1 + name_len];
                let value_start = 1 + name_len + 1;
                let value_len =
                    u16::from_be_bytes([headers[value_start], headers[value_start + 1]]) as usize;
                let value = &headers[value_start + 2..value_start + 2 + value_len];
                if name == b":event-type" {
                    event_type = Some(String::from_utf8(value.to_vec()).unwrap());
                }
                headers = &headers[value_start + 2 + value_len..];
            }
            let payload = &buf[12 + headers_len..total - 4];
            out.push((
                event_type.expect(":event-type header"),
                serde_json::from_slice(payload).unwrap(),
            ));
            buf = &buf[total..];
        }
        out
    }

    #[test]
    fn converse_stream_payloads_are_the_events_own_members() {
        let out = build_converse_stream_response("hello world", "end_turn", None, 5, 7);
        assert_eq!(
            frames(&out),
            vec![
                ("messageStart".to_string(), json!({ "role": "assistant" })),
                (
                    "contentBlockDelta".to_string(),
                    json!({ "contentBlockIndex": 0, "delta": { "text": "hello world" } })
                ),
                (
                    "contentBlockStop".to_string(),
                    json!({ "contentBlockIndex": 0 })
                ),
                (
                    "messageStop".to_string(),
                    json!({ "stopReason": "end_turn" })
                ),
                (
                    "metadata".to_string(),
                    json!({
                        "usage": { "inputTokens": 5, "outputTokens": 7, "totalTokens": 12 },
                        "metrics": { "latencyMs": 100 }
                    })
                ),
            ]
        );
    }

    #[test]
    fn converse_stream_forced_tool_block_has_the_modeled_shape() {
        let out = build_converse_stream_response("", "tool_use", Some("calculator"), 3, 0);
        let frames = frames(&out);
        let start = frames
            .iter()
            .find(|(event, _)| event == "contentBlockStart")
            .expect("toolUse block start");
        assert_eq!(
            start.1,
            json!({
                "contentBlockIndex": 1,
                "start": { "toolUse": { "toolUseId": "tooluse_fakecloud_01", "name": "calculator" } }
            })
        );
        assert!(frames.contains(&(
            "contentBlockDelta".to_string(),
            json!({ "contentBlockIndex": 1, "delta": { "toolUse": { "input": "{}" } } })
        )));
        assert!(frames.contains(&(
            "messageStop".to_string(),
            json!({ "stopReason": "tool_use" })
        )));
    }

    #[test]
    fn converse_stream_from_deltas_emits_one_event_per_delta() {
        let deltas = vec!["foo".to_string(), "bar".to_string()];
        let out = build_converse_stream_from_deltas(
            &deltas,
            &[],
            crate::upstream::StopKind::EndTurn,
            5,
            2,
        );
        let frames = frames(&out);
        let texts: Vec<&Value> = frames
            .iter()
            .filter(|(event, _)| event == "contentBlockDelta")
            .map(|(_, members)| &members["delta"]["text"])
            .collect();
        assert_eq!(texts, [&json!("foo"), &json!("bar")]);
        let (event, metadata) = frames.last().unwrap();
        assert_eq!(event, "metadata");
        assert_eq!(metadata["usage"]["inputTokens"], 5);
        assert_eq!(metadata["usage"]["outputTokens"], 2);
    }

    #[test]
    fn converse_stream_from_no_deltas_still_opens_the_block_it_stops() {
        let out =
            build_converse_stream_from_deltas(&[], &[], crate::upstream::StopKind::EndTurn, 1, 0);
        let events: Vec<(String, Value)> = frames(&out);
        assert_eq!(events[1].0, "contentBlockDelta");
        assert_eq!(events[1].1["delta"]["text"], "");
        assert_eq!(events[2].0, "contentBlockStop");
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
        let out = build_converse_stream_response("text", "end_turn", None, 42, 7);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\"inputTokens\":42"));
        assert!(s.contains("\"outputTokens\":7"));
        assert!(s.contains("\"totalTokens\":49"));
    }
}
