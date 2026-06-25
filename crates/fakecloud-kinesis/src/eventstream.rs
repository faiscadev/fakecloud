//! Minimal `application/vnd.amazon.eventstream` frame encoder for Kinesis
//! `SubscribeToShard`. Same wire format as Lambda's response-streaming and S3
//! Select (prelude + headers + payload + CRCs); the Kinesis SDK's eventstream
//! unmarshaller parses these frames into `SubscribeToShardEventStream` items.

const HEADER_TYPE_STRING: u8 = 7;

/// Encode one eventstream message: 4-byte total length, 4-byte headers length,
/// prelude CRC32, headers, payload, message CRC32 (all big-endian).
fn encode_frame(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
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
        buf.push(name_bytes.len() as u8);
        buf.extend_from_slice(name_bytes);
        buf.push(HEADER_TYPE_STRING);
        buf.extend_from_slice(&(value_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(value_bytes);
    }
    buf
}

/// Build a `SubscribeToShardEvent` frame from its JSON payload.
pub(crate) fn subscribe_to_shard_event_frame(payload: &[u8]) -> Vec<u8> {
    encode_frame(
        &[
            (":event-type", "SubscribeToShardEvent"),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        payload,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_has_prelude_and_crcs() {
        let frame = subscribe_to_shard_event_frame(b"{}");
        // total_len is the first 4 bytes and must equal the frame length.
        let total = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]);
        assert_eq!(total as usize, frame.len());
        // prelude CRC over the first 8 bytes.
        let prelude_crc = u32::from_be_bytes([frame[8], frame[9], frame[10], frame[11]]);
        assert_eq!(prelude_crc, crc32fast::hash(&frame[..8]));
        // message CRC over everything before the trailing 4 bytes.
        let end = frame.len();
        let msg_crc = u32::from_be_bytes([
            frame[end - 4],
            frame[end - 3],
            frame[end - 2],
            frame[end - 1],
        ]);
        assert_eq!(msg_crc, crc32fast::hash(&frame[..end - 4]));
    }
}
