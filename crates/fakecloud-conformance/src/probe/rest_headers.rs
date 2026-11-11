//! HTTP header bindings for the hand-routed restXml path (S3).
//!
//! S3 keeps its curated URL table, so the model-driven builder never binds
//! its `@httpHeader` / `@httpPrefixHeaders` members. Without them every
//! header-driven behaviour (ACL grants, SSE, conditional writes, checksums,
//! object metadata) goes unexercised. This module binds them the way an AWS
//! SDK serializes them:
//!
//! - Values follow the HTTP binding rules: timestamps are `http-date` unless
//!   `@timestampFormat` says otherwise, lists are comma-joined (items that
//!   contain a comma or quote are quoted), strings with `@mediaType` are
//!   base64, booleans and numbers are their literal text.
//! - Framing and integrity headers are computed, never taken from the
//!   generated input: `Content-Length` is left to the HTTP client,
//!   `x-amz-content-sha256` to the signer, `Content-MD5` is the MD5 of the
//!   body actually sent, and at most one `x-amz-checksum-<algorithm>` header
//!   is sent, carrying that algorithm's checksum of the body: the algorithm
//!   the request names in `x-amz-sdk-checksum-algorithm`, else the first
//!   checksum member the variant sets. SDKs never send more than one, and S3
//!   refuses a request that does before any handler runs.
//! - An operation whose `aws.protocols#httpChecksum` sets
//!   `requestChecksumRequired` always carries an integrity header: when the
//!   variant supplies none, the SDK default (CRC32, with the algorithm member
//!   set to match) is added, as current SDKs do.
//! - A value that cannot be a header (bytes outside visible ASCII, which no
//!   SDK would put on the wire) is not sent.

use base64::Engine as _;
use serde_json::Value;

use crate::smithy::{Member, Operation, ServiceModel, ShapeTraits, ShapeType};

const BASE64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

/// The request-digest checksum headers S3 understands, with the
/// `ChecksumAlgorithm` enum value that names each.
const CHECKSUM_HEADERS: &[(&str, &str)] = &[
    ("x-amz-checksum-crc32", "CRC32"),
    ("x-amz-checksum-crc32c", "CRC32C"),
    ("x-amz-checksum-crc64nvme", "CRC64NVME"),
    ("x-amz-checksum-sha1", "SHA1"),
    ("x-amz-checksum-sha256", "SHA256"),
    ("x-amz-checksum-sha512", "SHA512"),
    ("x-amz-checksum-md5", "MD5"),
    ("x-amz-checksum-xxhash64", "XXHASH64"),
    ("x-amz-checksum-xxhash3", "XXHASH3"),
    ("x-amz-checksum-xxhash128", "XXHASH128"),
];

const SDK_CHECKSUM_ALGORITHM: &str = "x-amz-sdk-checksum-algorithm";

/// The headers `input` binds for `op`, with framing/integrity headers
/// computed over `body` (the exact bytes that will be sent; empty for a
/// bodyless request).
pub(super) fn bind_headers(
    op: &Operation,
    model: &ServiceModel,
    input: &Value,
    body: &[u8],
) -> Vec<(String, String)> {
    let Some(input_shape) = op.input_shape.as_deref() else {
        return Vec::new();
    };
    let Some(members) = model
        .shapes
        .get(input_shape)
        .and_then(|s| match &s.shape_type {
            ShapeType::Structure { members } => Some(members),
            _ => None,
        })
    else {
        return Vec::new();
    };
    let empty = serde_json::Map::new();
    let obj = input.as_object().unwrap_or(&empty);

    let mut headers: Vec<(String, String)> = Vec::new();
    for member in super::rest_xml::declared_order(model, input_shape, members) {
        let Some(value) = obj.get(&member.name).filter(|v| !v.is_null()) else {
            continue;
        };
        if let Some(name) = header_name(model, member) {
            if let Some(v) = header_value(model, member, value) {
                headers.push((name, v));
            }
        } else if let Some(prefix) = member_or_target(model, member, |t| &t.http_prefix_headers) {
            if let Some(map) = value.as_object() {
                for (k, v) in map {
                    if let Some(text) = scalar(v) {
                        headers.push((format!("{prefix}{k}"), text));
                    }
                }
            }
        }
    }

    let required = model.request_checksum_required.get(&op.name).map(|c| {
        // The header the algorithm member binds to.
        c.algorithm_member
            .as_deref()
            .and_then(|name| members.iter().find(|m| m.name == name))
            .and_then(|m| header_name(model, m))
    });
    finalize_integrity_headers(headers, body, required)
        .into_iter()
        .filter(|(k, v)| {
            reqwest::header::HeaderName::from_bytes(k.as_bytes()).is_ok()
                && reqwest::header::HeaderValue::from_str(v).is_ok()
        })
        .collect()
}

/// A string trait read from the member, else from its target shape (binding
/// traits may sit on either).
fn member_or_target(
    model: &ServiceModel,
    member: &Member,
    field: impl Fn(&ShapeTraits) -> &Option<String>,
) -> Option<String> {
    field(&member.traits).clone().or_else(|| {
        model
            .shapes
            .get(&member.target)
            .and_then(|s| field(&s.traits).clone())
    })
}

/// The `@httpHeader` name `member` binds to.
fn header_name(model: &ServiceModel, member: &Member) -> Option<String> {
    member_or_target(model, member, |t| &t.http_header)
}

/// Drop the headers the client and signer own, and compute the digests.
///
/// `required` is `Some` when the operation requires a request checksum; it
/// carries the header the operation's algorithm member binds to, if any.
fn finalize_integrity_headers(
    headers: Vec<(String, String)>,
    body: &[u8],
    required: Option<Option<String>>,
) -> Vec<(String, String)> {
    let is = |k: &str, name: &str| k.eq_ignore_ascii_case(name);
    let named_algorithm = headers
        .iter()
        .find(|(k, _)| is(k, SDK_CHECKSUM_ALGORITHM))
        .map(|(_, v)| v.clone());
    let named = named_algorithm.as_deref().and_then(|v| {
        CHECKSUM_HEADERS
            .iter()
            .find(|(_, alg)| alg.eq_ignore_ascii_case(v))
    });
    // The single checksum header to send: the named algorithm's, else the
    // first checksum member present.
    let mut selected = named.or_else(|| {
        headers
            .iter()
            .find_map(|(k, _)| CHECKSUM_HEADERS.iter().find(|(h, _)| is(k, h)))
    });

    let mut out = Vec::with_capacity(headers.len() + 2);
    let mut has_md5 = false;
    for (k, v) in headers {
        if is(&k, "content-length") || is(&k, "x-amz-content-sha256") {
            continue;
        }
        if is(&k, "content-md5") {
            has_md5 = true;
            out.push((k, BASE64.encode(digest("MD5", body))));
            continue;
        }
        if CHECKSUM_HEADERS.iter().any(|(h, _)| is(&k, h)) {
            continue;
        }
        out.push((k, v));
    }

    // A required checksum the variant did not supply: the SDK fills in its
    // default algorithm, CRC32, and sets the algorithm member to match. An
    // algorithm member naming an unknown algorithm is left for the server to
    // reject; an SDK would refuse it client side.
    if let Some(algorithm_header) = required {
        if selected.is_none() && !has_md5 && named_algorithm.is_none() {
            selected = CHECKSUM_HEADERS.iter().find(|(_, alg)| *alg == "CRC32");
            if let Some(h) = algorithm_header {
                out.push((h, "CRC32".to_string()));
            }
        }
    }
    if let Some((header, alg)) = selected {
        out.push((header.to_string(), BASE64.encode(digest(alg, body))));
    }
    out
}

/// The raw digest of `data` under the `ChecksumAlgorithm` enum value `alg`.
fn digest(alg: &str, data: &[u8]) -> Vec<u8> {
    use sha1::Digest as _;
    match alg {
        "CRC32" => crc32fast::hash(data).to_be_bytes().to_vec(),
        "CRC32C" => crc32c::crc32c(data).to_be_bytes().to_vec(),
        "CRC64NVME" => {
            let mut d = crc64fast_nvme::Digest::new();
            d.write(data);
            d.sum64().to_be_bytes().to_vec()
        }
        "SHA1" => sha1::Sha1::digest(data).to_vec(),
        "SHA256" => sha2::Sha256::digest(data).to_vec(),
        "SHA512" => sha2::Sha512::digest(data).to_vec(),
        "MD5" => md5::Md5::digest(data).to_vec(),
        "XXHASH64" => xxhash_rust::xxh64::xxh64(data, 0).to_be_bytes().to_vec(),
        "XXHASH3" => xxhash_rust::xxh3::xxh3_64(data).to_be_bytes().to_vec(),
        "XXHASH128" => xxhash_rust::xxh3::xxh3_128(data).to_be_bytes().to_vec(),
        other => unreachable!("not a checksum algorithm: {other}"),
    }
}

/// Render `value` for the header bound by `member`.
fn header_value(model: &ServiceModel, member: &Member, value: &Value) -> Option<String> {
    let target = model.shapes.get(&member.target);
    match target.map(|s| &s.shape_type) {
        Some(ShapeType::List { member_target }) => {
            let items = value.as_array()?;
            // `@timestampFormat` on the list's member applies to its items.
            let item_traits = ShapeTraits {
                timestamp_format: target
                    .and_then(|t| t.traits.list_member_timestamp_format.clone()),
                ..ShapeTraits::default()
            };
            let item_is_timestamp = crate::smithy::is_timestamp(model, member_target);
            let rendered: Vec<String> = items
                .iter()
                .filter_map(|item| scalar_header(model, member_target, Some(&item_traits), item))
                .map(|s| {
                    // Strings with a comma or quote are quoted; http-date
                    // timestamps contain a comma but are never quoted.
                    if !item_is_timestamp && (s.contains(',') || s.contains('"')) {
                        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
                    } else {
                        s
                    }
                })
                .collect();
            Some(rendered.join(", "))
        }
        _ => scalar_header(model, &member.target, Some(&member.traits), value),
    }
}

fn scalar_header(
    model: &ServiceModel,
    shape_id: &str,
    member_traits: Option<&ShapeTraits>,
    value: &Value,
) -> Option<String> {
    let shape = model.shapes.get(shape_id);
    if crate::smithy::is_timestamp(model, shape_id) {
        let format = member_traits
            .and_then(|t| t.timestamp_format.clone())
            .or_else(|| shape.and_then(|s| s.traits.timestamp_format.clone()))
            .unwrap_or_else(|| "http-date".to_string());
        return value
            .as_str()
            .map(|s| super::rest_xml::format_timestamp(s, Some(&format)));
    }
    let text = scalar(value)?;
    if value.is_string() && shape.is_some_and(|s| s.traits.media_type.is_some()) {
        return Some(BASE64.encode(text.as_bytes()));
    }
    Some(text)
}

fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smithy::parse_model_str;
    use serde_json::json;

    fn model() -> ServiceModel {
        parse_model_str(
            r##"{
                "smithy": "2.0",
                "shapes": {
                    "t#Svc": {
                        "type": "service",
                        "operations": [{"target": "t#Put"}, {"target": "t#PutRequired"}],
                        "traits": {"aws.protocols#restXml": {}}
                    },
                    "t#Put": {
                        "type": "operation",
                        "input": {"target": "t#PutRequest"},
                        "traits": {"smithy.api#http": {"method": "PUT", "uri": "/{Key}"}}
                    },
                    "t#PutRequest": {
                        "type": "structure",
                        "members": {
                            "Key": {"target": "t#Str", "traits": {"smithy.api#httpLabel": {}}},
                            "Acl": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-amz-acl"}},
                            "Expires": {"target": "t#Time", "traits": {"smithy.api#httpHeader": "Expires"}},
                            "RetainUntil": {
                                "target": "t#Time",
                                "traits": {"smithy.api#httpHeader": "x-amz-retain", "smithy.api#timestampFormat": "date-time"}
                            },
                            "Attrs": {"target": "t#StrList", "traits": {"smithy.api#httpHeader": "x-amz-attrs"}},
                            "Context": {"target": "t#Json", "traits": {"smithy.api#httpHeader": "x-amz-context"}},
                            "Enabled": {"target": "t#Bool", "traits": {"smithy.api#httpHeader": "x-amz-enabled"}},
                            "Size": {"target": "t#Long", "traits": {"smithy.api#httpHeader": "x-amz-size"}},
                            "Metadata": {"target": "t#Meta", "traits": {"smithy.api#httpPrefixHeaders": "x-amz-meta-"}},
                            "ContentLength": {"target": "t#Long", "traits": {"smithy.api#httpHeader": "Content-Length"}},
                            "ContentMD5": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "Content-MD5"}},
                            "Sha": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-amz-content-sha256"}},
                            "ChecksumAlgorithm": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-amz-sdk-checksum-algorithm"}},
                            "ChecksumCRC32": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-amz-checksum-crc32"}},
                            "ChecksumSHA256": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-amz-checksum-sha256"}},
                            "Bad": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-amz-bad"}},
                            "Dates": {"target": "t#Dates", "traits": {"smithy.api#httpHeader": "x-amz-dates"}},
                            "EpochDates": {"target": "t#EpochDates", "traits": {"smithy.api#httpHeader": "x-amz-epoch-dates"}},
                            "Body": {"target": "t#Blob", "traits": {"smithy.api#httpPayload": {}}}
                        }
                    },
                    "t#PutRequired": {
                        "type": "operation",
                        "input": {"target": "t#PutRequiredRequest"},
                        "traits": {
                            "smithy.api#http": {"method": "PUT", "uri": "/r"},
                            "aws.protocols#httpChecksum": {
                                "requestAlgorithmMember": "ChecksumAlgorithm",
                                "requestChecksumRequired": true
                            }
                        }
                    },
                    "t#PutRequiredRequest": {
                        "type": "structure",
                        "members": {
                            "ContentMD5": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "Content-MD5"}},
                            "ChecksumAlgorithm": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-amz-sdk-checksum-algorithm"}},
                            "ChecksumSHA256": {"target": "t#Str", "traits": {"smithy.api#httpHeader": "x-amz-checksum-sha256"}}
                        }
                    },
                    "t#Dates": {"type": "list", "member": {"target": "t#Time"}},
                    "t#EpochDates": {
                        "type": "list",
                        "member": {"target": "t#Time", "traits": {"smithy.api#timestampFormat": "epoch-seconds"}}
                    },
                    "t#Str": {"type": "string"},
                    "t#Json": {"type": "string", "traits": {"smithy.api#mediaType": "application/json"}},
                    "t#StrList": {"type": "list", "member": {"target": "t#Str"}},
                    "t#Meta": {"type": "map", "key": {"target": "t#Str"}, "value": {"target": "t#Str"}},
                    "t#Time": {"type": "timestamp"},
                    "t#Bool": {"type": "boolean"},
                    "t#Long": {"type": "long"},
                    "t#Blob": {"type": "blob"}
                }
            }"##,
        )
        .unwrap()
    }

    fn headers(input: Value, body: &[u8]) -> Vec<(String, String)> {
        headers_for("Put", input, body)
    }

    fn headers_for(op: &str, input: Value, body: &[u8]) -> Vec<(String, String)> {
        let m = model();
        let op = m.operations.iter().find(|o| o.name == op).unwrap();
        bind_headers(op, &m, &input, body)
    }

    fn get<'a>(h: &'a [(String, String)], name: &str) -> Option<&'a str> {
        h.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    #[test]
    fn values_follow_the_header_binding_rules() {
        let h = headers(
            json!({
                "Key": "k",
                "Acl": "private",
                "Expires": "2024-01-01T00:00:00Z",
                "RetainUntil": "2024-01-01T00:00:00Z",
                "Attrs": ["ETag", "a,b", "say \"hi\""],
                "Context": "{\"k\":\"v\"}",
                "Enabled": true,
                "Size": 42,
                "Metadata": {"color": "blue"}
            }),
            b"",
        );
        assert_eq!(get(&h, "x-amz-acl"), Some("private"));
        assert_eq!(get(&h, "Expires"), Some("Mon, 01 Jan 2024 00:00:00 GMT"));
        assert_eq!(get(&h, "x-amz-retain"), Some("2024-01-01T00:00:00Z"));
        assert_eq!(get(&h, "x-amz-attrs"), Some(r#"ETag, "a,b", "say \"hi\"""#));
        assert_eq!(get(&h, "x-amz-context"), Some("eyJrIjoidiJ9"));
        assert_eq!(get(&h, "x-amz-enabled"), Some("true"));
        assert_eq!(get(&h, "x-amz-size"), Some("42"));
        assert_eq!(get(&h, "x-amz-meta-color"), Some("blue"));
        // Labels are not headers.
        assert!(h.iter().all(|(_, v)| v != "k"));
    }

    #[test]
    fn framing_headers_are_computed_or_left_to_the_client() {
        let h = headers(
            json!({"ContentLength": 999, "ContentMD5": "bogus", "Sha": "bogus"}),
            b"",
        );
        assert_eq!(get(&h, "Content-Length"), None);
        assert_eq!(get(&h, "x-amz-content-sha256"), None);
        assert_eq!(get(&h, "Content-MD5"), Some("1B2M2Y8AsgTpgAmY7PhCfg=="));
    }

    #[test]
    fn only_the_first_checksum_member_is_sent_with_the_body_checksum() {
        // SDKs send at most one x-amz-checksum-* header; S3 refuses more.
        let h = headers(
            json!({"ChecksumSHA256": "bogus", "ChecksumCRC32": "bogus"}),
            b"abc",
        );
        assert_eq!(get(&h, "x-amz-checksum-crc32"), Some("NSRBwg=="));
        assert_eq!(get(&h, "x-amz-checksum-sha256"), None);
        let h = headers(json!({"ChecksumSHA256": "bogus"}), b"abc");
        assert_eq!(
            get(&h, "x-amz-checksum-sha256"),
            Some("ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=")
        );
        let count = h
            .iter()
            .filter(|(k, _)| k.starts_with("x-amz-checksum-"))
            .count();
        assert_eq!(count, 1);
    }

    #[test]
    fn a_required_checksum_defaults_to_crc32() {
        let h = headers_for("PutRequired", json!({}), b"abc");
        assert_eq!(get(&h, "x-amz-checksum-crc32"), Some("NSRBwg=="));
        assert_eq!(get(&h, "x-amz-sdk-checksum-algorithm"), Some("CRC32"));
        // A supplied integrity header satisfies the requirement.
        let h = headers_for("PutRequired", json!({"ContentMD5": "x"}), b"");
        assert_eq!(get(&h, "Content-MD5"), Some("1B2M2Y8AsgTpgAmY7PhCfg=="));
        assert_eq!(get(&h, "x-amz-checksum-crc32"), None);
        // So does a named algorithm, which gets its own checksum.
        let h = headers_for(
            "PutRequired",
            json!({"ChecksumAlgorithm": "SHA256"}),
            b"abc",
        );
        assert_eq!(
            get(&h, "x-amz-checksum-sha256"),
            Some("ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=")
        );
        assert_eq!(get(&h, "x-amz-checksum-crc32"), None);
        // An optional-checksum operation gets nothing added.
        let h = headers(json!({}), b"abc");
        assert!(h.iter().all(|(k, _)| !k.starts_with("x-amz-checksum-")));
    }

    #[test]
    fn algorithm_member_header_falls_back_to_the_target_shape() {
        // Binding traits may sit on the target shape; the required-checksum
        // default must find the algorithm member's header the same way the
        // binding loop does.
        let mut m = model();
        let req = m.shapes.get_mut("t#PutRequiredRequest").unwrap();
        if let ShapeType::Structure { members } = &mut req.shape_type {
            let alg = members
                .iter_mut()
                .find(|m| m.name == "ChecksumAlgorithm")
                .unwrap();
            alg.traits.http_header = None;
            alg.target = "t#AlgHeader".to_string();
        }
        let mut target = m.shapes["t#Str"].clone();
        target.shape_id = "t#AlgHeader".to_string();
        target.traits.http_header = Some("x-amz-sdk-checksum-algorithm".to_string());
        m.shapes.insert("t#AlgHeader".to_string(), target);
        let op = m
            .operations
            .iter()
            .find(|o| o.name == "PutRequired")
            .unwrap();
        let h = bind_headers(op, &m, &json!({}), b"abc");
        assert_eq!(get(&h, "x-amz-sdk-checksum-algorithm"), Some("CRC32"));
        assert_eq!(get(&h, "x-amz-checksum-crc32"), Some("NSRBwg=="));
    }

    #[test]
    fn timestamp_lists_are_not_quoted_and_honor_member_format() {
        let h = headers(
            json!({
                "Dates": ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"],
                "EpochDates": ["2024-01-01T00:00:00.5Z", "1970-01-01T01:00:00+01:00"]
            }),
            b"",
        );
        assert_eq!(
            get(&h, "x-amz-dates"),
            Some("Mon, 01 Jan 2024 00:00:00 GMT, Tue, 02 Jan 2024 00:00:00 GMT")
        );
        assert_eq!(get(&h, "x-amz-epoch-dates"), Some("1704067200.5, 0"));
    }

    #[test]
    fn a_named_algorithm_sends_exactly_that_checksum() {
        let h = headers(
            json!({"ChecksumAlgorithm": "SHA256", "ChecksumCRC32": "bogus"}),
            b"abc",
        );
        assert_eq!(get(&h, "x-amz-sdk-checksum-algorithm"), Some("SHA256"));
        assert_eq!(get(&h, "x-amz-checksum-crc32"), None);
        assert_eq!(
            get(&h, "x-amz-checksum-sha256"),
            Some("ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=")
        );
        // An unknown algorithm is passed through for the server to reject,
        // and the per-algorithm members keep their computed values.
        let h = headers(
            json!({"ChecksumAlgorithm": "NOPE", "ChecksumCRC32": "bogus"}),
            b"abc",
        );
        assert_eq!(get(&h, "x-amz-sdk-checksum-algorithm"), Some("NOPE"));
        assert_eq!(get(&h, "x-amz-checksum-crc32"), Some("NSRBwg=="));
    }

    #[test]
    fn values_that_cannot_be_headers_are_not_sent() {
        let h = headers(json!({"Bad": "line\nbreak", "Acl": "private"}), b"");
        assert_eq!(get(&h, "x-amz-bad"), None);
        assert_eq!(get(&h, "x-amz-acl"), Some("private"));
    }
}
