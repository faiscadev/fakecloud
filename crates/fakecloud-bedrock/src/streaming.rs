use serde_json::{json, Value};

/// Encode data as an AWS event-stream message.
/// The event stream binary format:
///   [total_byte_length:4] [headers_byte_length:4] [prelude_crc:4]
///   [headers:*] [payload:*] [message_crc:4]
pub fn encode_event(event_type: &str, content_type: &str, payload: &[u8]) -> Vec<u8> {
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
pub fn build_invoke_stream_response(model_id: &str, response_text: &str) -> Vec<u8> {
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
            &json!({ "bytes": base64_encode(&serde_json::to_vec(&chunk).unwrap()) }),
        )
        .unwrap();
        body.extend(encode_event("chunk", "application/json", &payload));
    } else {
        // Generic: single chunk with the full response
        let chunk = json!({
            "outputText": response_text
        });
        let payload = serde_json::to_vec(
            &json!({ "bytes": base64_encode(&serde_json::to_vec(&chunk).unwrap()) }),
        )
        .unwrap();
        body.extend(encode_event("chunk", "application/json", &payload));
    }

    body
}

/// Build the complete event stream body for ConverseStream.
pub fn build_converse_stream_response(response_text: &str) -> Vec<u8> {
    let mut body = Vec::new();

    // messageStart event
    let start = json!({ "role": "assistant" });
    let payload = serde_json::to_vec(&json!({ "messageStart": start })).unwrap();
    body.extend(encode_event("messageStart", "application/json", &payload));

    // contentBlockStart event
    let block_start = json!({ "contentBlockIndex": 0, "start": {} });
    let payload = serde_json::to_vec(&json!({ "contentBlockStart": block_start })).unwrap();
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
    let payload = serde_json::to_vec(&json!({ "contentBlockDelta": delta })).unwrap();
    body.extend(encode_event(
        "contentBlockDelta",
        "application/json",
        &payload,
    ));

    // contentBlockStop event
    let block_stop = json!({ "contentBlockIndex": 0 });
    let payload = serde_json::to_vec(&json!({ "contentBlockStop": block_stop })).unwrap();
    body.extend(encode_event(
        "contentBlockStop",
        "application/json",
        &payload,
    ));

    // messageStop event
    let stop = json!({
        "stopReason": "end_turn"
    });
    let payload = serde_json::to_vec(&json!({ "messageStop": stop })).unwrap();
    body.extend(encode_event("messageStop", "application/json", &payload));

    // metadata event
    let metadata = json!({
        "usage": {
            "inputTokens": 10,
            "outputTokens": 20,
            "totalTokens": 30
        },
        "metrics": {
            "latencyMs": 100
        }
    });
    let payload = serde_json::to_vec(&json!({ "metadata": metadata })).unwrap();
    body.extend(encode_event("metadata", "application/json", &payload));

    body
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Wrap response text for non-streaming provider fallback
pub fn default_stream_text() -> &'static str {
    "This is a test response from the emulated model."
}

/// Generate the canned response text, checking for custom overrides
pub fn get_response_text(state: &crate::state::SharedBedrockState, model_id: &str) -> String {
    let s = state.read();
    if let Some(custom) = s.custom_responses.get(model_id) {
        // Try to extract text from a JSON response
        if let Ok(parsed) = serde_json::from_str::<Value>(custom) {
            // Anthropic format
            if let Some(text) = parsed["content"][0]["text"].as_str() {
                return text.to_string();
            }
            // Converse format
            if let Some(text) = parsed["output"]["message"]["content"][0]["text"].as_str() {
                return text.to_string();
            }
        }
        custom.clone()
    } else {
        default_stream_text().to_string()
    }
}
