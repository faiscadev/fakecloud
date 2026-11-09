use serde_json::{json, Value};

use crate::upstream::StopKind;

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

/// A `toolUse` content block: its start, its input as one delta, its stop.
fn push_tool_use_block(body: &mut Vec<u8>, index: usize, id: &str, name: &str, input: &str) {
    push_event(
        body,
        "contentBlockStart",
        &json!({
            "contentBlockIndex": index,
            "start": { "toolUse": { "toolUseId": id, "name": name } }
        }),
    );
    push_event(
        body,
        "contentBlockDelta",
        &json!({ "contentBlockIndex": index, "delta": { "toolUse": { "input": input } } }),
    );
    push_event(
        body,
        "contentBlockStop",
        &json!({ "contentBlockIndex": index }),
    );
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
    tools: &[crate::upstream::ToolCall],
    input_tokens: u64,
    output_tokens: u64,
) -> Vec<u8> {
    let mut body = Vec::new();

    push_event(&mut body, "messageStart", &json!({ "role": "assistant" }));

    // A text block opens with its first delta: `ContentBlockStart` is a union
    // with no text member, so there is nothing a start event could say. A reply
    // of only tool calls has no text block.
    let has_text = !response_text.is_empty() || tools.is_empty();
    if has_text {
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
    }

    // Optional forced toolUse content block (index 1), mirroring Converse.
    if let Some(tool_name) = forced_tool {
        push_tool_use_block(&mut body, 1, "tooluse_fakecloud_01", tool_name, "{}");
    }
    for (offset, tool) in tools.iter().enumerate() {
        push_tool_use_block(
            &mut body,
            offset + usize::from(has_text),
            &tool.id,
            &tool.name,
            &tool.input.to_string(),
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
    tool_calls: &[crate::upstream::ToolCall],
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
        // A reply of only tool calls has no text block.
        let has_text = !deltas.is_empty() || tool_calls.is_empty();
        if has_text {
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
        }
        for (offset, tool) in tool_calls.iter().enumerate() {
            let index = offset + usize::from(has_text);
            push_bytes_chunk(
                &mut body,
                &json!({
                    "type": "content_block_start",
                    "index": index,
                    "content_block": { "type": "tool_use", "id": tool.id, "name": tool.name, "input": {} }
                }),
            );
            push_bytes_chunk(
                &mut body,
                &json!({
                    "type": "content_block_delta",
                    "index": index,
                    "delta": { "type": "input_json_delta", "partial_json": tool.input.to_string() }
                }),
            );
            push_bytes_chunk(
                &mut body,
                &json!({ "type": "content_block_stop", "index": index }),
            );
        }
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
/// `ConverseStream` event stream: one `contentBlockDelta` event per token, a
/// `toolUse` block per tool the model called, terminated with real usage figures
/// in the `metadata` event.
pub(crate) fn build_converse_stream_from_deltas(
    deltas: &[String],
    tools: &[crate::upstream::ToolCall],
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
        push_tool_use_block(
            &mut body,
            index,
            &tool.id,
            &tool.name,
            &tool.input.to_string(),
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

/// What a configured override says the model replied: its text, tool calls and,
/// when it says, why it stopped. `stop` is `None` for an opaque string, or a body
/// naming neither a stop reason nor a tool.
pub(crate) struct OverrideReply {
    pub(crate) text: String,
    pub(crate) tools: Vec<crate::upstream::ToolCall>,
    pub(crate) stop: Option<StopKind>,
}

/// Read an override, which is the body `InvokeModel` or `Converse` would return:
/// any provider's native shape or Converse's, or an opaque string that is the
/// text verbatim.
pub(crate) fn parse_override(custom: &str) -> OverrideReply {
    use crate::upstream::ToolCall;

    let opaque = || OverrideReply {
        text: custom.to_string(),
        tools: Vec::new(),
        stop: None,
    };
    let Ok(parsed) = serde_json::from_str::<Value>(custom) else {
        return opaque();
    };
    let tool = |id: &Value, name: &Value, input: &Value, ordinal: usize| ToolCall {
        id: id
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| format!("tooluse_fakecloud_{ordinal:02}")),
        name: name.as_str().unwrap_or_default().to_string(),
        input: if input.is_null() {
            json!({})
        } else {
            input.clone()
        },
    };

    let mut text = String::new();
    let mut tools = Vec::new();
    let reason;
    if let Some(blocks) = parsed["content"].as_array() {
        for block in blocks {
            text.push_str(block["text"].as_str().unwrap_or_default());
            if block["type"] == "tool_use" {
                tools.push(tool(
                    &block["id"],
                    &block["name"],
                    &block["input"],
                    tools.len(),
                ));
            }
        }
        reason = &parsed["stop_reason"];
    } else if let Some(blocks) = parsed["output"]["message"]["content"].as_array() {
        for block in blocks {
            text.push_str(block["text"].as_str().unwrap_or_default());
            let call = &block["toolUse"];
            if call.is_object() {
                tools.push(tool(
                    &call["toolUseId"],
                    &call["name"],
                    &call["input"],
                    tools.len(),
                ));
            }
        }
        reason = &parsed["stopReason"];
    } else if let Some(output) = parsed["results"][0]["outputText"].as_str() {
        text.push_str(output);
        reason = &parsed["results"][0]["completionReason"];
    } else if let Some(output) = parsed["generation"].as_str() {
        text.push_str(output);
        reason = &parsed["stop_reason"];
    } else if let Some(output) = parsed["generations"][0]["text"].as_str() {
        text.push_str(output);
        reason = &parsed["generations"][0]["finish_reason"];
    } else if let Some(output) = parsed["outputs"][0]["text"].as_str() {
        text.push_str(output);
        reason = &parsed["outputs"][0]["stop_reason"];
    } else {
        return opaque();
    }
    // Content that holds nothing readable (an image block, say) isn't a reply
    // this can stream, so the body itself stays the text.
    if text.is_empty() && tools.is_empty() && !reason.is_string() {
        return opaque();
    }
    let reason = reason.as_str();
    let stop = (reason.is_some() || !tools.is_empty())
        .then(|| crate::upstream::normalize_stop(reason, !tools.is_empty()));
    OverrideReply { text, tools, stop }
}

/// `InvokeModelWithResponseStream` frames for a configured override.
pub(crate) fn build_override_stream(model_id: &str, custom: &str, input_tokens: u64) -> Vec<u8> {
    let reply = parse_override(custom);
    let output_tokens = crate::prompt::count_tokens(&reply.text);
    let deltas = if reply.text.is_empty() {
        Vec::new()
    } else {
        vec![reply.text]
    };
    build_invoke_stream_from_deltas(
        model_id,
        &deltas,
        &reply.tools,
        reply.stop.unwrap_or(StopKind::EndTurn),
        input_tokens,
        output_tokens,
    )
}

/// Offline `InvokeModelWithResponseStream`: a configured override, else the same
/// fallback reply as `InvokeModel`, with usage counted as `count_tokens` counts it.
pub(crate) fn build_offline_invoke_stream(
    model_id: &str,
    body: &[u8],
    custom: Option<&str>,
) -> Vec<u8> {
    let input_tokens =
        crate::prompt::count_tokens(&crate::prompt::extract_prompt_text(model_id, body));
    if let Some(custom) = custom {
        return build_override_stream(model_id, custom, input_tokens);
    }
    let text = fallback_text(model_id, body);
    let output_tokens = crate::prompt::count_tokens(&text);
    build_invoke_stream_from_deltas(
        model_id,
        std::slice::from_ref(&text),
        &[],
        StopKind::EndTurn,
        input_tokens,
        output_tokens,
    )
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

    /// The text a call replies with.
    fn get_response_text(
        state: &crate::state::SharedBedrockState,
        req: &fakecloud_core::service::AwsRequest,
        model_id: &str,
        body: &[u8],
    ) -> String {
        match crate::prompt::resolve_override(state, req, model_id, body) {
            Some(custom) => parse_override(&custom).text,
            None => fallback_text(model_id, body),
        }
    }

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

    /// The chunk payloads of an `InvokeModelWithResponseStream`.
    fn chunks(stream: &[u8]) -> Vec<Value> {
        frames(stream)
            .into_iter()
            .map(|(_, payload)| {
                use base64::Engine;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(payload["bytes"].as_str().unwrap())
                    .unwrap();
                serde_json::from_slice(&bytes).unwrap()
            })
            .collect()
    }

    #[test]
    fn a_tool_use_override_streams_its_tool_and_stop_reason() {
        let custom = json!({
            "content": [
                {"type": "text", "text": "calling"},
                {"type": "tool_use", "id": "t1", "name": "calc", "input": {"a": 1}}
            ],
            "stop_reason": "tool_use"
        })
        .to_string();
        let got = chunks(&build_override_stream("anthropic.claude-3", &custom, 2));
        let text: Vec<&Value> = got
            .iter()
            .filter(|chunk| chunk["delta"]["type"] == "text_delta")
            .collect();
        assert_eq!(text.len(), 1);
        assert_eq!(text[0]["delta"]["text"], "calling");
        assert!(got
            .iter()
            .any(|chunk| chunk["content_block"]["type"] == "tool_use"
                && chunk["content_block"]["id"] == "t1"));
        assert!(got
            .iter()
            .any(|chunk| chunk["delta"]["partial_json"] == "{\"a\":1}"));
        assert!(got
            .iter()
            .any(|chunk| chunk["delta"]["stop_reason"] == "tool_use"));
    }

    #[test]
    fn a_stop_reason_in_an_override_is_honoured_for_each_family() {
        let cases = [
            (
                "anthropic.claude-3",
                r#"{"content":[{"type":"text","text":"x"}],"stop_reason":"max_tokens"}"#,
            ),
            (
                "amazon.titan-text",
                r#"{"results":[{"outputText":"x","completionReason":"LENGTH"}]}"#,
            ),
            (
                "meta.llama3",
                r#"{"generation":"x","stop_reason":"length"}"#,
            ),
            (
                "cohere.command",
                r#"{"generations":[{"text":"x","finish_reason":"MAX_TOKENS"}]}"#,
            ),
            (
                "mistral.mistral",
                r#"{"outputs":[{"text":"x","stop_reason":"length"}]}"#,
            ),
        ];
        for (model, custom) in cases {
            let reply = parse_override(custom);
            assert_eq!(reply.text, "x", "{custom}");
            assert_eq!(reply.stop, Some(StopKind::MaxTokens), "{custom}");
            let stream = build_override_stream(model, custom, 1);
            assert!(!chunks(&stream).is_empty(), "{model}");
        }
    }

    #[test]
    fn a_converse_shaped_override_carries_its_tools() {
        let reply = parse_override(
            &json!({
                "output": {"message": {"content": [
                    {"toolUse": {"toolUseId": "t", "name": "calc", "input": {}}}
                ]}},
                "stopReason": "tool_use"
            })
            .to_string(),
        );
        assert!(reply.text.is_empty());
        assert_eq!(reply.tools[0].id, "t");
        assert_eq!(reply.stop, Some(StopKind::ToolUse));
    }

    #[test]
    fn an_override_without_a_readable_reply_is_opaque_text() {
        for custom in [
            "plain words",
            r#"{"label":"ham"}"#,
            r#"{"content":[{"type":"image"}]}"#,
        ] {
            let reply = parse_override(custom);
            assert_eq!(reply.text, custom);
            assert!(reply.tools.is_empty());
            assert_eq!(reply.stop, None);
        }
    }

    #[test]
    fn families_without_tool_calls_end_normally_rather_than_on_a_tool_stop() {
        let tool = crate::upstream::ToolCall {
            id: "t".to_string(),
            name: "calc".to_string(),
            input: json!({}),
        };
        for (model, field) in [
            ("amazon.titan-text", "completionReason"),
            ("meta.llama3", "stop_reason"),
            ("cohere.command", "finish_reason"),
        ] {
            let stream = build_invoke_stream_from_deltas(
                model,
                &[],
                std::slice::from_ref(&tool),
                StopKind::ToolUse,
                1,
                1,
            );
            let last = chunks(&stream).pop().unwrap();
            let stop = last[field].as_str().unwrap();
            assert!(
                matches!(stop, "FINISH" | "stop" | "COMPLETE"),
                "{model}: {stop}"
            );
        }
    }

    #[test]
    fn empty_text_counts_no_tokens_in_a_stream() {
        let stream = build_override_stream("anthropic.claude-3", "", 0);
        let got = chunks(&stream);
        assert_eq!(got[0]["message"]["usage"]["input_tokens"], 0);
        let delta = got.iter().find(|c| c["type"] == "message_delta").unwrap();
        assert_eq!(delta["usage"]["output_tokens"], 0);

        let offline = build_offline_invoke_stream("anthropic.claude-3", b"{}", Some(""));
        assert_eq!(chunks(&offline)[0]["message"]["usage"]["input_tokens"], 0);
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
        let out = build_converse_stream_response("hello world", "end_turn", None, &[], 5, 7);
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
        let out = build_converse_stream_response("", "tool_use", Some("calculator"), &[], 3, 0);
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
        let out = build_converse_stream_response("text", "end_turn", None, &[], 42, 7);
        let s = String::from_utf8_lossy(&out);
        assert!(s.contains("\"inputTokens\":42"));
        assert!(s.contains("\"outputTokens\":7"));
        assert!(s.contains("\"totalTokens\":49"));
    }
}
