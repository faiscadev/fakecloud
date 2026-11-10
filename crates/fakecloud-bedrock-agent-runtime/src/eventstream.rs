//! Minimal `application/vnd.amazon.eventstream` frame encoder used by
//! `InvokeAgent` and `InvokeFlow` to ship streamed responses to AWS SDK
//! clients.
//!
//! Frame layout (network byte order):
//!
//! ```text
//! +-----------------------------------+
//! | total length        (u32 BE)      |
//! | headers length      (u32 BE)      |
//! | prelude CRC32       (u32 BE)      |  CRC of the two preceding u32s
//! | headers bytes       (raw)         |
//! | payload bytes       (raw)         |
//! | message CRC32       (u32 BE)      |  CRC of the whole frame so far
//! +-----------------------------------+
//! ```
//!
//! Each header: `name_len (u8) | name | type (u8 = 7) | value_len (u16
//! BE) | value`. Only the string header type (7) is needed for Bedrock
//! Agent — `:event-type`, `:content-type`, `:message-type` are all
//! short ASCII strings.

const HEADER_TYPE_STRING: u8 = 7;

pub fn encode_frame(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
    let headers_bytes = encode_headers(headers);
    let headers_len = headers_bytes.len() as u32;
    let total_len = 12u32 + headers_len + payload.len() as u32 + 4;

    let mut out = Vec::with_capacity(total_len as usize);
    out.extend_from_slice(&total_len.to_be_bytes());
    out.extend_from_slice(&headers_len.to_be_bytes());

    let prelude_crc = crc32fast::hash(&out[..8]);
    out.extend_from_slice(&prelude_crc.to_be_bytes());

    out.extend_from_slice(&headers_bytes);
    out.extend_from_slice(payload);

    let msg_crc = crc32fast::hash(&out);
    out.extend_from_slice(&msg_crc.to_be_bytes());

    out
}

fn encode_headers(headers: &[(&str, &str)]) -> Vec<u8> {
    let mut buf = Vec::new();
    for (name, value) in headers {
        let name_bytes = name.as_bytes();
        let value_bytes = value.as_bytes();
        debug_assert!(name_bytes.len() <= u8::MAX as usize, "header name too long");
        debug_assert!(
            value_bytes.len() <= u16::MAX as usize,
            "header value too long"
        );
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.push(HEADER_TYPE_STRING);
        buf.extend_from_slice(&(value_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(value_bytes);
    }
    buf
}

/// Build a `chunk` event frame for `InvokeAgent`. Carries a JSON
/// payload `{"bytes": "<base64 of model output>"}`.
pub fn chunk_frame(text: &str) -> Vec<u8> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(text.as_bytes());
    let body = serde_json::to_vec(&serde_json::json!({ "bytes": b64 }))
        .expect("static JSON shape never fails");
    encode_frame(
        &[
            (":event-type", "chunk"),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        &body,
    )
}

/// Build a `flowOutputEvent` frame for `InvokeFlow`: `nodeName` and the
/// `content.document` envelope. The model marks `nodeType` required, but AWS was
/// recorded not sending it.
pub fn flow_output_frame(node_name: &str, document: &serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(&serde_json::json!({
        "nodeName": node_name,
        "content": { "document": document },
    }))
    .expect("static JSON shape never fails");
    encode_frame(
        &[
            (":event-type", "flowOutputEvent"),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        &body,
    )
}

/// Build a `flowTraceEvent` frame carrying one `FlowTrace` union value.
pub fn flow_trace_frame(trace: serde_json::Value) -> Vec<u8> {
    let body = serde_json::to_vec(&serde_json::json!({ "trace": trace }))
        .expect("static JSON shape never fails");
    encode_frame(
        &[
            (":event-type", "flowTraceEvent"),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        &body,
    )
}

/// Build the `flowCompletionEvent` frame that ends a finished `InvokeFlow`.
pub fn flow_completion_frame() -> Vec<u8> {
    let body = serde_json::to_vec(&serde_json::json!({ "completionReason": "SUCCESS" }))
        .expect("static JSON shape never fails");
    encode_frame(
        &[
            (":event-type", "flowCompletionEvent"),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        &body,
    )
}

/// Build an in-stream exception frame, how a stream already under way reports
/// an error: `:message-type` `exception`, the union member's name as
/// `:exception-type`, and the error's members as the payload.
pub fn exception_frame(exception_type: &str, message: &str) -> Vec<u8> {
    let body = serde_json::to_vec(&serde_json::json!({ "message": message }))
        .expect("static JSON shape never fails");
    encode_frame(
        &[
            (":exception-type", exception_type),
            (":content-type", "application/json"),
            (":message-type", "exception"),
        ],
        &body,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_frame(buf: &[u8]) -> (Vec<(String, String)>, Vec<u8>) {
        assert!(buf.len() >= 16);
        let total_len = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
        assert_eq!(total_len, buf.len());
        let headers_len = u32::from_be_bytes(buf[4..8].try_into().unwrap()) as usize;
        assert_eq!(
            u32::from_be_bytes(buf[8..12].try_into().unwrap()),
            crc32fast::hash(&buf[0..8])
        );
        let headers_start = 12;
        let headers_end = headers_start + headers_len;
        let payload_end = total_len - 4;
        assert_eq!(
            u32::from_be_bytes(buf[payload_end..total_len].try_into().unwrap()),
            crc32fast::hash(&buf[..payload_end])
        );
        let mut headers = Vec::new();
        let hbuf = &buf[headers_start..headers_end];
        let mut i = 0;
        while i < hbuf.len() {
            let nl = hbuf[i] as usize;
            let name = String::from_utf8(hbuf[i + 1..i + 1 + nl].to_vec()).unwrap();
            i += 1 + nl;
            assert_eq!(hbuf[i], HEADER_TYPE_STRING);
            i += 1;
            let vl = u16::from_be_bytes(hbuf[i..i + 2].try_into().unwrap()) as usize;
            i += 2;
            let value = String::from_utf8(hbuf[i..i + vl].to_vec()).unwrap();
            i += vl;
            headers.push((name, value));
        }
        (headers, buf[headers_end..payload_end].to_vec())
    }

    #[test]
    fn chunk_frame_roundtrips() {
        let frame = chunk_frame("hello agent");
        let (headers, payload) = decode_frame(&frame);
        let event_type = headers.iter().find(|(k, _)| k == ":event-type").unwrap();
        assert_eq!(event_type.1, "chunk");
        let body: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        let b64 = body["bytes"].as_str().unwrap();
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        assert_eq!(decoded, b"hello agent");
    }

    #[test]
    fn flow_output_frame_roundtrips() {
        let frame = flow_output_frame("StartNode", &serde_json::json!("document text"));
        let (headers, payload) = decode_frame(&frame);
        let event_type = headers.iter().find(|(k, _)| k == ":event-type").unwrap();
        assert_eq!(event_type.1, "flowOutputEvent");
        let body: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(body["nodeName"], "StartNode");
        assert!(body.get("nodeType").is_none());
        assert_eq!(body["content"]["document"], "document text");
    }

    #[test]
    fn exception_frame_names_its_exception() {
        let (headers, body) = decode_frame(&exception_frame("validationException", "bad"));
        assert!(headers.contains(&(":message-type".to_string(), "exception".to_string())));
        assert!(headers.contains(&(
            ":exception-type".to_string(),
            "validationException".to_string()
        )));
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body, serde_json::json!({"message": "bad"}));
    }

    #[test]
    fn flow_completion_frame_reports_success() {
        let (headers, payload) = decode_frame(&flow_completion_frame());
        let event_type = headers.iter().find(|(k, _)| k == ":event-type").unwrap();
        assert_eq!(event_type.1, "flowCompletionEvent");
        let body: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(body, serde_json::json!({"completionReason": "SUCCESS"}));
    }
}
