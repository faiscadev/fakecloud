//! Smithy RPC v2 CBOR (`smithy.protocols#rpcv2Cbor`) wire support.
//!
//! An rpcv2Cbor request is `POST /service/{ServiceName}/operation/{Operation}`
//! with the `smithy-protocol: rpc-v2-cbor` header and a CBOR-encoded input
//! structure as the body. Responses carry the same header, `Content-Type:
//! application/cbor`, and a CBOR body; errors are a CBOR map holding `__type`
//! plus the error structure's members.
//!
//! fakecloud's JSON-protocol handlers already take a JSON input document, so
//! the dispatcher decodes the CBOR body into the equivalent awsJson document
//! ([`decode_to_json`]: tag-1 timestamps become epoch seconds, byte strings
//! become base64 strings) and routes the request through the service's JSON
//! path. A service that knows its output schema returns a ready CBOR body
//! (content type [`CBOR_CONTENT_TYPE`]); any JSON response is transcoded
//! generically with [`json_to_cbor`].

use base64::Engine;
use serde_json::{Map, Number, Value};

pub use ciborium::Value as CborValue;

/// `Content-Type` of every rpcv2Cbor request and response body.
pub const CBOR_CONTENT_TYPE: &str = "application/cbor";
/// Header naming the Smithy protocol on rpcv2Cbor requests and responses.
pub const SMITHY_PROTOCOL_HEADER: &str = "smithy-protocol";
/// Value of [`SMITHY_PROTOCOL_HEADER`] for rpcv2Cbor.
pub const RPC_V2_CBOR: &str = "rpc-v2-cbor";

/// CBOR tag for an epoch-seconds date/time (RFC 8949 section 3.4.2), the only
/// timestamp encoding rpcv2Cbor allows.
const EPOCH_TIMESTAMP_TAG: u64 = 1;

/// Split an rpcv2Cbor request path `/service/{ServiceName}/operation/{Operation}`
/// into `(ServiceName, Operation)`. Smithy allows a path prefix before
/// `/service/`, so the match is anchored on the trailing four segments.
pub fn parse_rpc_v2_path(path: &str) -> Option<(&str, &str)> {
    let segs: Vec<&str> = path.trim_end_matches('/').split('/').collect();
    match segs.as_slice() {
        [.., "service", service, "operation", operation]
            if !service.is_empty() && !operation.is_empty() =>
        {
            Some((service, operation))
        }
        _ => None,
    }
}

/// Decode an rpcv2Cbor request body into the awsJson document a JSON handler
/// reads. An empty body is an empty input structure.
pub fn decode_to_json(body: &[u8]) -> Result<Value, String> {
    if body.is_empty() {
        return Ok(Value::Object(Map::new()));
    }
    let value: CborValue = ciborium::from_reader(body).map_err(|e| e.to_string())?;
    cbor_to_json(value)
}

fn cbor_to_json(value: CborValue) -> Result<Value, String> {
    Ok(match value {
        CborValue::Null => Value::Null,
        CborValue::Bool(b) => Value::Bool(b),
        CborValue::Integer(i) => {
            let i = i128::from(i);
            if let Ok(v) = i64::try_from(i) {
                Value::Number(v.into())
            } else if let Ok(v) = u64::try_from(i) {
                Value::Number(v.into())
            } else {
                return Err(format!("integer {i} out of range"));
            }
        }
        CborValue::Float(f) => float_to_json(f),
        CborValue::Text(s) => Value::String(s),
        // awsJson carries blobs as base64 strings.
        CborValue::Bytes(b) => Value::String(base64::engine::general_purpose::STANDARD.encode(b)),
        CborValue::Array(items) => Value::Array(
            items
                .into_iter()
                .map(cbor_to_json)
                .collect::<Result<_, _>>()?,
        ),
        CborValue::Map(entries) => {
            let mut obj = Map::new();
            for (k, v) in entries {
                let CborValue::Text(key) = k else {
                    return Err("map keys must be text strings".to_string());
                };
                // rpcv2Cbor serializes an absent member by omitting it; a
                // null is treated the same way, as awsJson handlers expect.
                if v.is_null() {
                    continue;
                }
                obj.insert(key, cbor_to_json(v)?);
            }
            Value::Object(obj)
        }
        // A tag-1 timestamp is epoch seconds, which is exactly the awsJson
        // timestamp representation. Other tags carry no meaning to the
        // handlers, so their content is used as-is.
        CborValue::Tag(_, inner) => cbor_to_json(*inner)?,
        other => return Err(format!("unsupported CBOR value {other:?}")),
    })
}

/// awsJson renders non-finite doubles as the strings `NaN`, `Infinity` and
/// `-Infinity`.
fn float_to_json(f: f64) -> Value {
    match Number::from_f64(f) {
        Some(n) => Value::Number(n),
        None if f.is_nan() => Value::String("NaN".to_string()),
        None if f > 0.0 => Value::String("Infinity".to_string()),
        None => Value::String("-Infinity".to_string()),
    }
}

/// An epoch-seconds timestamp as rpcv2Cbor encodes it: tag 1 over a double.
/// A float (rather than an integer for whole seconds) keeps millisecond
/// precision and is what every SDK's decoder accepts.
pub fn timestamp(epoch_seconds: f64) -> CborValue {
    CborValue::Tag(
        EPOCH_TIMESTAMP_TAG,
        Box::new(CborValue::Float(epoch_seconds)),
    )
}

/// Encode a CBOR value to bytes, with definite lengths and every float as a
/// 64-bit double. (A generic encoder shrinks floats to half/single precision
/// when lossless, but SDK decoders built without half-float support reject a
/// 16-bit float where the schema says `double`.)
pub fn encode(value: &CborValue) -> Vec<u8> {
    let mut out = Vec::new();
    encode_into(value, &mut out);
    out
}

/// Write a CBOR item head: major type plus argument, shortest form.
fn write_head(major: u8, arg: u64, out: &mut Vec<u8>) {
    let m = major << 5;
    if arg < 24 {
        out.push(m | arg as u8);
    } else if arg <= u8::MAX as u64 {
        out.push(m | 24);
        out.push(arg as u8);
    } else if arg <= u16::MAX as u64 {
        out.push(m | 25);
        out.extend_from_slice(&(arg as u16).to_be_bytes());
    } else if arg <= u32::MAX as u64 {
        out.push(m | 26);
        out.extend_from_slice(&(arg as u32).to_be_bytes());
    } else {
        out.push(m | 27);
        out.extend_from_slice(&arg.to_be_bytes());
    }
}

fn encode_into(value: &CborValue, out: &mut Vec<u8>) {
    match value {
        CborValue::Integer(i) => {
            let i = i128::from(*i);
            if i >= 0 {
                write_head(0, i as u64, out);
            } else {
                write_head(1, (-1 - i) as u64, out);
            }
        }
        CborValue::Bytes(b) => {
            write_head(2, b.len() as u64, out);
            out.extend_from_slice(b);
        }
        CborValue::Text(t) => {
            write_head(3, t.len() as u64, out);
            out.extend_from_slice(t.as_bytes());
        }
        CborValue::Array(items) => {
            write_head(4, items.len() as u64, out);
            for item in items {
                encode_into(item, out);
            }
        }
        CborValue::Map(entries) => {
            write_head(5, entries.len() as u64, out);
            for (k, v) in entries {
                encode_into(k, out);
                encode_into(v, out);
            }
        }
        CborValue::Tag(tag, inner) => {
            write_head(6, *tag, out);
            encode_into(inner, out);
        }
        CborValue::Bool(false) => out.push(0xf4),
        CborValue::Bool(true) => out.push(0xf5),
        CborValue::Null => out.push(0xf6),
        CborValue::Float(f) => {
            out.push(0xfb);
            out.extend_from_slice(&f.to_be_bytes());
        }
        // Not produced by fakecloud; encode as null rather than panic.
        _ => out.push(0xf6),
    }
}

/// Schema-less transcoding of an awsJson document into CBOR, for services that
/// do not render CBOR themselves: integers stay integers, other numbers become
/// floats, strings stay text. Timestamps and blobs keep their awsJson form
/// (number / base64 text) since their shape type is not known here.
pub fn json_to_cbor(value: &Value) -> CborValue {
    match value {
        Value::Null => CborValue::Null,
        Value::Bool(b) => CborValue::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                CborValue::Integer(i.into())
            } else if let Some(u) = n.as_u64() {
                CborValue::Integer(u.into())
            } else {
                CborValue::Float(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => CborValue::Text(s.clone()),
        Value::Array(items) => CborValue::Array(items.iter().map(json_to_cbor).collect()),
        Value::Object(obj) => CborValue::Map(
            obj.iter()
                .map(|(k, v)| (CborValue::Text(k.clone()), json_to_cbor(v)))
                .collect(),
        ),
    }
}

/// The CBOR body of an rpcv2Cbor error: `__type` names the error, `message`
/// carries its text, and any extra members are added alongside (parsed as
/// JSON when they hold a JSON document, as for the awsJson error body).
pub fn error_body(code: &str, message: &str, extra_fields: &[(String, String)]) -> Vec<u8> {
    let mut entries = vec![
        (
            CborValue::Text("__type".to_string()),
            CborValue::Text(code.to_string()),
        ),
        (
            CborValue::Text("message".to_string()),
            CborValue::Text(message.to_string()),
        ),
    ];
    for (key, value) in extra_fields {
        let parsed = serde_json::from_str::<Value>(value)
            .map(|v| json_to_cbor(&v))
            .unwrap_or_else(|_| CborValue::Text(value.clone()));
        entries.push((CborValue::Text(key.clone()), parsed));
    }
    encode(&CborValue::Map(entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cbor(v: &CborValue) -> Vec<u8> {
        encode(v)
    }

    fn text(s: &str) -> CborValue {
        CborValue::Text(s.to_string())
    }

    #[test]
    fn parses_rpc_v2_path() {
        assert_eq!(
            parse_rpc_v2_path("/service/GraniteServiceVersion20100801/operation/GetMetricData"),
            Some(("GraniteServiceVersion20100801", "GetMetricData"))
        );
        assert_eq!(
            parse_rpc_v2_path("/prefix/service/Svc/operation/Op"),
            Some(("Svc", "Op"))
        );
        assert_eq!(parse_rpc_v2_path("/service/Svc"), None);
        assert_eq!(parse_rpc_v2_path("/service//operation/Op"), None);
        assert_eq!(parse_rpc_v2_path("/"), None);
    }

    #[test]
    fn decodes_empty_body_as_empty_object() {
        assert_eq!(decode_to_json(b"").unwrap(), serde_json::json!({}));
    }

    #[test]
    fn decodes_typed_members_to_aws_json() {
        let body = cbor(&CborValue::Map(vec![
            (text("Namespace"), text("App")),
            (text("Period"), CborValue::Integer(60.into())),
            (text("Value"), CborValue::Float(1.5)),
            (text("Enabled"), CborValue::Bool(true)),
            (text("StartTime"), timestamp(1_577_836_800.0)),
            (text("EndTime"), timestamp(1_577_836_800.5)),
            (text("Blob"), CborValue::Bytes(b"hi".to_vec())),
            (text("Absent"), CborValue::Null),
            (
                text("Values"),
                CborValue::Array(vec![CborValue::Float(1.0), CborValue::Float(f64::NAN)]),
            ),
        ]));
        let json = decode_to_json(&body).unwrap();
        assert_eq!(json["Namespace"], "App");
        assert_eq!(json["Period"], 60);
        assert_eq!(json["Value"], 1.5);
        assert_eq!(json["Enabled"], true);
        assert_eq!(json["StartTime"], 1_577_836_800.0);
        assert_eq!(json["EndTime"], 1_577_836_800.5);
        assert_eq!(json["Blob"], "aGk=");
        assert!(json.get("Absent").is_none());
        assert_eq!(json["Values"][0], 1.0);
        assert_eq!(json["Values"][1], "NaN");
    }

    #[test]
    fn rejects_malformed_cbor() {
        assert!(decode_to_json(&[0xff, 0x00]).is_err());
        // Non-text map key.
        let body = cbor(&CborValue::Map(vec![(
            CborValue::Integer(1.into()),
            text("x"),
        )]));
        assert!(decode_to_json(&body).is_err());
    }

    #[test]
    fn timestamp_is_tag_one_over_a_double() {
        assert_eq!(
            timestamp(10.0),
            CborValue::Tag(1, Box::new(CborValue::Float(10.0)))
        );
        assert_eq!(
            encode(&timestamp(10.0)),
            [&[0xc1, 0xfb][..], &10.0f64.to_be_bytes()].concat()
        );
    }

    #[test]
    fn encoder_round_trips_and_keeps_doubles_wide() {
        assert_eq!(
            encode(&CborValue::Float(1.0)),
            [&[0xfb][..], &1.0f64.to_be_bytes()].concat()
        );
        assert_eq!(
            encode(&CborValue::Integer(500.into())),
            vec![0x19, 0x01, 0xf4]
        );
        assert_eq!(encode(&CborValue::Integer((-1).into())), vec![0x20]);
        let value = CborValue::Map(vec![
            (text("n"), CborValue::Integer(u64::MAX.into())),
            (text("neg"), CborValue::Integer(i64::MIN.into())),
            (text("b"), CborValue::Bytes(vec![1, 2, 3])),
            (
                text("l"),
                CborValue::Array(vec![CborValue::Bool(true), CborValue::Null]),
            ),
            (text("s"), text(&"x".repeat(300))),
            (text("t"), timestamp(1.5)),
        ]);
        let back: CborValue = ciborium::from_reader(encode(&value).as_slice()).unwrap();
        assert_eq!(back, value);
    }

    #[test]
    fn json_to_cbor_keeps_number_kinds() {
        let v = json_to_cbor(&serde_json::json!({"a": 1, "b": 1.5, "c": "s", "d": [true]}));
        let back: CborValue = ciborium::from_reader(encode(&v).as_slice()).unwrap();
        let CborValue::Map(entries) = back else {
            panic!("expected map");
        };
        let get = |k: &str| {
            entries
                .iter()
                .find(|(key, _)| key == &text(k))
                .map(|(_, v)| v.clone())
                .unwrap()
        };
        assert_eq!(get("a"), CborValue::Integer(1.into()));
        assert_eq!(get("b"), CborValue::Float(1.5));
        assert_eq!(get("c"), text("s"));
        assert_eq!(get("d"), CborValue::Array(vec![CborValue::Bool(true)]));
    }

    #[test]
    fn error_body_has_type_and_message() {
        let body = error_body(
            "InvalidParameterValue",
            "bad",
            &[("Extra".to_string(), "{\"k\":1}".to_string())],
        );
        let json = decode_to_json(&body).unwrap();
        assert_eq!(json["__type"], "InvalidParameterValue");
        assert_eq!(json["message"], "bad");
        assert_eq!(json["Extra"]["k"], 1);
    }
}
