//! probe `rest_request` (audit-2026-05-19).

use super::*;
/// S3's object-key limit: a key longer than this many UTF-8 bytes is refused
/// with `KeyTooLongError`.
const S3_MAX_KEY_BYTES: usize = 1024;

/// The input to serialize for `op`, with a stand-in for an omitted (or empty)
/// required `@httpLabel` where leaving the label out would make the request
/// valid.
///
/// No Smithy client sends such a request (smithy-rs refuses to build one), so
/// a `negative_omit_*` / `negative_too_short_*` variant can only be judged on a
/// value that keeps the request on the operation's own route and that AWS
/// answers with a client error. `build_http_request_from_model` leaves the
/// literal `{Name}` in the path, which does that for almost every label: the
/// braces violate the label's `@pattern` (Lambda names, layer names) or S3's
/// bucket-naming rules. S3's greedy `{Key+}` is the exception: `{Key+}` is a
/// perfectly good object key, so `PUT /bucket/{Key+}` would create an object.
/// Its stand-in is one byte past S3's key-length limit instead, which S3
/// refuses with `KeyTooLongError` before touching any object.
pub(super) fn with_omitted_label_sentinels(
    service_name: &str,
    op: &crate::smithy::Operation,
    model: &ServiceModel,
    input: &serde_json::Value,
) -> serde_json::Value {
    let mut input = input.clone();
    if service_name != "s3" || !op.http_uri.as_deref().is_some_and(|u| u.contains("{Key+}")) {
        return input;
    }
    let Some(obj) = input.as_object_mut() else {
        return input;
    };
    let is_label =
        crate::generators::get_members(model, op.input_shape.as_deref().unwrap_or_default())
            .iter()
            .any(|m| m.name == "Key" && m.traits.http_label);
    let omitted = !matches!(obj.get("Key"), Some(serde_json::Value::String(k)) if !k.is_empty());
    if is_label && omitted {
        obj.insert(
            "Key".to_string(),
            serde_json::Value::String("k".repeat(S3_MAX_KEY_BYTES + 1)),
        );
    }
    input
}

/// Build an HTTP request for an operation from the `@http` / `@httpLabel` /
/// `@httpQuery` / `@httpHeader` / `@httpPayload` Smithy traits on its input
/// shape. Returns `None` if the operation is missing `@http` metadata.
///
/// Returned tuple: `(method, path_and_query_string, headers, body)`.
pub(super) fn build_http_request_from_model(
    op: &crate::smithy::Operation,
    model: &ServiceModel,
    input: &serde_json::Value,
) -> Option<RestRequestParts> {
    use crate::smithy::ShapeType;

    let method_str = op.http_method.as_ref()?;
    let uri_template = op.http_uri.as_ref()?;

    let method = reqwest::Method::from_bytes(method_str.as_bytes()).ok()?;

    // Clone input so we can progressively drain label/query/header/payload members.
    let mut remaining = match input {
        serde_json::Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };

    let mut query_parts: Vec<String> = Vec::new();
    let mut payload_value: Option<serde_json::Value> = None;
    // Whether the input shape binds an `@httpPayload` member at all: the
    // body is then that member alone, and empty when it is absent.
    let mut has_payload_member = false;

    // Walk the input shape's members to discover http bindings. Label / query /
    // header / payload traits can live on either the member or the referenced
    // target shape — check both.
    let members: Vec<crate::smithy::Member> = op
        .input_shape
        .as_ref()
        .and_then(|id| model.shapes.get(id))
        .and_then(|shape| match &shape.shape_type {
            ShapeType::Structure { members } => Some(members.clone()),
            _ => None,
        })
        .unwrap_or_default();

    // Header (`@httpHeader` / `@httpPrefixHeaders`) members are bound once,
    // after the body is built, by the shared binder (`rest_headers`): the HTTP
    // binding rules are protocol-independent (http-date timestamps,
    // comma-joined lists, base64 `@mediaType` strings), and integrity headers
    // are computed over the body actually sent. This loop only drains them so
    // they do not also travel in a restJson1 body.
    let rest_xml = model.is_rest_xml();
    for member in &members {
        let target_traits = model.shapes.get(&member.target).map(|s| &s.traits);
        let member_traits = &member.traits;
        let is_label =
            member_traits.http_label || target_traits.map(|t| t.http_label).unwrap_or(false);
        let query_name = member_traits
            .http_query
            .clone()
            .or_else(|| target_traits.and_then(|t| t.http_query.clone()));
        let is_header = member_traits.http_header.is_some()
            || member_traits.http_prefix_headers.is_some()
            || target_traits
                .map(|t| t.http_header.is_some() || t.http_prefix_headers.is_some())
                .unwrap_or(false);
        let is_payload =
            member_traits.http_payload || target_traits.map(|t| t.http_payload).unwrap_or(false);
        has_payload_member |= is_payload;

        if is_label || query_name.is_some() || is_header || is_payload {
            if let Some(val) = remaining.remove(&member.name) {
                if let Some(qk) = query_name {
                    append_query(&mut query_parts, &qk, &val);
                } else if is_payload {
                    payload_value = Some(val);
                }
            }
        }
    }

    // Second pass: collect label values fresh from the original input, since we
    // may have removed them above.
    let mut label_values: HashMap<String, serde_json::Value> = HashMap::new();
    for member in &members {
        let target_traits = model.shapes.get(&member.target).map(|s| &s.traits);
        let is_label =
            member.traits.http_label || target_traits.map(|t| t.http_label).unwrap_or(false);
        if is_label {
            if let Some(val) = input.get(&member.name) {
                label_values.insert(member.name.clone(), val.clone());
            }
        }
    }

    let path = substitute_uri_labels(uri_template, &label_values);

    // Merge literal query (from URI template after `?`) with computed params.
    let (path_only, literal_query) = match path.split_once('?') {
        Some((p, q)) => (p.to_string(), Some(q.to_string())),
        None => (path, None),
    };

    let mut all_query: Vec<String> = Vec::new();
    if let Some(lq) = literal_query {
        if !lq.is_empty() {
            all_query.push(lq);
        }
    }
    all_query.extend(query_parts);
    let path_and_query = if all_query.is_empty() {
        path_only
    } else {
        format!("{}?{}", path_only, all_query.join("&"))
    };

    // Body: @httpPayload member wins; else whatever remains in the input object
    // (minus labels, query, headers, payload). Omit body entirely on
    // GET/HEAD/DELETE unless an explicit @httpPayload member was present.
    let is_bodyless_method = matches!(
        method,
        reqwest::Method::GET | reqwest::Method::HEAD | reqwest::Method::DELETE
    );
    let mut content_type: Option<&str> = None;
    let body = if rest_xml {
        // restXml bodies are XML documents built from the binding traits,
        // not the JSON the generators produce.
        let (body, ct) = super::rest_xml::rest_xml_body(op, model, input);
        content_type = ct;
        body
    } else if let Some(v) = payload_value {
        Some(value_to_body(&v).into_bytes())
    } else if has_payload_member {
        // An absent `@httpPayload` member is no body, as an SDK sends it
        // (not `{}`, which is a payload: Lambda InvokeAsync accepts `{}` as
        // an event and refuses an empty body).
        None
    } else if is_bodyless_method {
        None
    } else {
        // Drop label members from `remaining` (we re-added none, but be defensive).
        for name in label_values.keys() {
            remaining.remove(name);
        }
        if remaining.is_empty() {
            Some(b"{}".to_vec())
        } else {
            Some(serde_json::to_vec(&serde_json::Value::Object(remaining)).unwrap_or_default())
        }
    };
    let mut headers =
        super::rest_headers::bind_headers(op, model, input, body.as_deref().unwrap_or_default());
    if let Some(ct) = content_type {
        if !headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        {
            headers.push(("Content-Type".to_string(), ct.to_string()));
        }
    }

    Some((method, path_and_query, headers, body))
}

pub(super) fn substitute_uri_labels(
    template: &str,
    labels: &HashMap<String, serde_json::Value>,
) -> String {
    // URI templates use `{Name}` and `{Name+}` (greedy, keeps `/` literal).
    let mut out = String::with_capacity(template.len());
    let bytes = template.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            if let Some(end) = template[i + 1..].find('}') {
                let inner = &template[i + 1..i + 1 + end];
                let (name, greedy) = if let Some(n) = inner.strip_suffix('+') {
                    (n, true)
                } else {
                    (inner, false)
                };
                // An empty-string label is unsendable: Smithy clients refuse to
                // serialize an empty `@httpLabel` (smithy-rs fails the build
                // with "cannot be empty or unset"), and the URL it would
                // produce collapses the segment -- `GET /queryloggingconfig/{Id}`
                // with `Id = ""` is byte-for-byte the trailing-slash form of
                // `GET /queryloggingconfig` (ListQueryLoggingConfigs), which
                // real AWS answers as the List op (#1645). Serialize it like an
                // omitted label instead, so the variant still sends a request
                // that targets this operation and is judged on the response.
                let raw = labels.get(name).and_then(|v| match v {
                    serde_json::Value::String(s) if s.is_empty() => None,
                    serde_json::Value::String(s) => Some(s.clone()),
                    serde_json::Value::Number(n) => Some(n.to_string()),
                    serde_json::Value::Bool(b) => Some(b.to_string()),
                    _ => None,
                });
                if let Some(raw) = raw {
                    let encoded = if greedy {
                        percent_encode_greedy(&raw)
                    } else {
                        percent_encode_label(&raw)
                    };
                    out.push_str(&encoded);
                } else {
                    // No value: leave the literal {Name} in place so the
                    // server-side failure surfaces as a mismatch rather than a
                    // silent 500.
                    out.push_str(&template[i..i + 1 + end + 1]);
                }
                i += 1 + end + 1;
                continue;
            }
        }
        out.push(template[i..].chars().next().unwrap());
        i += template[i..].chars().next().unwrap().len_utf8();
    }
    out
}

/// Percent-encode a path segment label. Keeps `-._~` unencoded (RFC 3986
/// unreserved) and encodes `/` so segment boundaries are preserved.
pub(super) fn percent_encode_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*b as char);
            }
            other => {
                out.push_str(&format!("%{:02X}", other));
            }
        }
    }
    out
}

/// Greedy-label encoding (`{Name+}`): same as label encoding but keeps `/`.
pub(super) fn percent_encode_greedy(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' => {
                out.push(*b as char);
            }
            other => {
                out.push_str(&format!("%{:02X}", other));
            }
        }
    }
    out
}

pub(super) fn append_query(out: &mut Vec<String>, key: &str, v: &serde_json::Value) {
    match v {
        serde_json::Value::String(s) => out.push(format!("{}={}", key, percent_encode_label(s))),
        serde_json::Value::Number(n) => out.push(format!("{}={}", key, n)),
        serde_json::Value::Bool(b) => out.push(format!("{}={}", key, b)),
        serde_json::Value::Array(items) => {
            for item in items {
                append_query(out, key, item);
            }
        }
        serde_json::Value::Null | serde_json::Value::Object(_) => {}
    }
}

/// For `@httpPayload` members: structures/lists/objects become JSON; strings
/// become the raw string; blobs (JSON-encoded as strings here) likewise.
pub(super) fn value_to_body(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        _ => serde_json::to_string(v).unwrap_or_default(),
    }
}

pub(super) fn flatten_to_form_params(
    map: &serde_json::Map<String, Value>,
    prefix: &str,
    params: &mut Vec<(String, String)>,
) {
    for (key, value) in map {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        match value {
            Value::String(s) => params.push((full_key, s.clone())),
            Value::Number(n) => params.push((full_key, n.to_string())),
            Value::Bool(b) => params.push((full_key, b.to_string())),
            Value::Object(nested) => flatten_to_form_params(nested, &full_key, params),
            Value::Array(arr) => {
                for (i, item) in arr.iter().enumerate() {
                    let item_key = format!("{}.member.{}", full_key, i + 1);
                    match item {
                        Value::String(s) => params.push((item_key, s.clone())),
                        Value::Number(n) => params.push((item_key, n.to_string())),
                        Value::Bool(b) => params.push((item_key, b.to_string())),
                        Value::Object(nested) => flatten_to_form_params(nested, &item_key, params),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}

pub(super) fn urlencoded(s: &str) -> String {
    s.replace('%', "%25")
        .replace('&', "%26")
        .replace('=', "%3D")
        .replace('+', "%2B")
        .replace(' ', "%20")
}
