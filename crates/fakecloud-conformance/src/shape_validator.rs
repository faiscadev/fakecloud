//! Response shape validation against Smithy models.
//!
//! Validates that HTTP response bodies from fakecloud match the expected output
//! shape structure defined in the Smithy model.

use serde_json::Value;

use crate::probe::Protocol;
use crate::smithy::{effective_shape_type, ServiceModel, ShapeType};

const MAX_DEPTH: usize = 10;

/// A violation found when validating a response body against the expected shape.
#[derive(Debug, Clone)]
pub enum ShapeViolation {
    /// A required field defined in the model is missing from the response.
    MissingField { path: String, field: String },
    /// A field is present but has the wrong JSON type.
    WrongType {
        path: String,
        expected: String,
        got: String,
    },
    /// A field is present in the response but not defined in the model.
    UnexpectedField { path: String, field: String },
    /// The response body could not be parsed.
    ParseError { message: String },
    /// A field documented in the operation's `@examples` output is absent
    /// from the live response (or has the wrong JSON type). Catches
    /// "optional in Smithy but AWS always emits" bugs (#816).
    ExamplesOutputDivergence {
        path: String,
        reason: ExamplesDivergenceReason,
    },
    /// An input field on a Create/Put/Update operation did not echo into
    /// the corresponding Get/Describe response. Catches silent-input-drop
    /// bugs (#853 Lambda Layers).
    RoundTripFieldNotEchoed {
        field: String,
        sent: serde_json::Value,
        received: Option<serde_json::Value>,
    },
}

/// What went wrong when diffing a response against a documented `@examples` output.
#[derive(Debug, Clone)]
pub enum ExamplesDivergenceReason {
    /// The path exists in the documented output but not in the live response.
    MissingField,
    /// The path exists in both but the JSON value types differ.
    WrongType { expected: String, got: String },
}

impl std::fmt::Display for ShapeViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShapeViolation::MissingField { path, field } => {
                write!(f, "missing required field '{}' at {}", field, path)
            }
            ShapeViolation::WrongType {
                path,
                expected,
                got,
            } => {
                write!(
                    f,
                    "wrong type at {}: expected {}, got {}",
                    path, expected, got
                )
            }
            ShapeViolation::UnexpectedField { path, field } => {
                write!(f, "unexpected field '{}' at {}", field, path)
            }
            ShapeViolation::ParseError { message } => {
                write!(f, "parse error: {}", message)
            }
            ShapeViolation::ExamplesOutputDivergence { path, reason } => match reason {
                ExamplesDivergenceReason::MissingField => write!(
                    f,
                    "@examples output diverges at {}: documented field absent from live response",
                    path
                ),
                ExamplesDivergenceReason::WrongType { expected, got } => write!(
                    f,
                    "@examples output diverges at {}: expected JSON {}, got {}",
                    path, expected, got
                ),
            },
            ShapeViolation::RoundTripFieldNotEchoed {
                field,
                sent,
                received,
            } => match received {
                Some(r) => write!(
                    f,
                    "round-trip mismatch for '{}': sent {}, got {}",
                    field, sent, r
                ),
                None => write!(
                    f,
                    "round-trip drop for '{}': sent {}, got nothing back",
                    field, sent
                ),
            },
        }
    }
}

/// Compare a value sent on a Create/Put/Update input against the value
/// returned for the same-named field on the corresponding Get/Describe
/// output. Used by the `round_trip` strategy.
///
/// Equality is `serde_json::Value` deep-equality. The intent is to catch
/// fakecloud silently dropping a field at the input layer or at the
/// storage layer; the diff is binary. Treat `null` and "field absent"
/// the same on the output side because some protocols omit nulls.
pub fn echo_check(
    field: &str,
    sent: &serde_json::Value,
    output: &serde_json::Value,
) -> Option<ShapeViolation> {
    let received = match output {
        serde_json::Value::Object(map) => crate::probe::lookup_field_any_case(map, field),
        _ => None,
    };
    match received {
        Some(serde_json::Value::Null) | None => Some(ShapeViolation::RoundTripFieldNotEchoed {
            field: field.to_string(),
            sent: sent.clone(),
            received: None,
        }),
        Some(v) if values_match_ignoring_key_case(v, sent) => None,
        Some(v) => Some(ShapeViolation::RoundTripFieldNotEchoed {
            field: field.to_string(),
            sent: sent.clone(),
            received: Some(v.clone()),
        }),
    }
}

/// Compare two JSON values for equality while tolerating first-letter
/// case differences on object keys. restJson1 services with `@jsonName`
/// (apigatewayv2) emit camelCase keys on the wire while the probe input
/// is keyed under the Smithy member names (PascalCase). The structural
/// payload is identical — only the first letter of each key differs.
fn values_match_ignoring_key_case(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Object(am), Value::Object(bm)) => {
            if am.len() != bm.len() {
                return false;
            }
            for (k, v) in am {
                match crate::probe::lookup_field_any_case(bm, k) {
                    Some(bv) if values_match_ignoring_key_case(v, bv) => continue,
                    _ => return false,
                }
            }
            true
        }
        (Value::Array(aa), Value::Array(bb)) => {
            aa.len() == bb.len()
                && aa
                    .iter()
                    .zip(bb.iter())
                    .all(|(x, y)| values_match_ignoring_key_case(x, y))
        }
        _ => a == b,
    }
}

/// Deep-diff a live response against the operation's documented `@examples`
/// output. Every leaf path present in `documented` must also exist in
/// `actual` with the same JSON type. Values are intentionally not compared
/// — many `@examples` use placeholders (`"examplebucket"`, `"123456789012"`)
/// and string-equality would be noise.
///
/// Object members are recursed into. Arrays compare the first element of
/// `documented` against the first element of `actual` if both are non-empty
/// (the AWS examples always show a one-element array as a representative
/// shape; comparing element 0 keeps the assertion structural).
pub fn diff_against_example(actual: &Value, documented: &Value) -> Vec<ShapeViolation> {
    let mut violations = Vec::new();
    diff_at(actual, documented, "$", &mut violations);
    violations
}

fn diff_at(actual: &Value, documented: &Value, path: &str, out: &mut Vec<ShapeViolation>) {
    match documented {
        Value::Object(doc_map) => {
            let actual_map = match actual {
                Value::Object(m) => m,
                _ => {
                    out.push(ShapeViolation::ExamplesOutputDivergence {
                        path: path.to_string(),
                        reason: ExamplesDivergenceReason::WrongType {
                            expected: "object".to_string(),
                            got: json_type_name(actual).to_string(),
                        },
                    });
                    return;
                }
            };
            // If the documented output advertises a pagination token but the
            // live response holds no items to paginate, accept the absence:
            // AWS only emits the token when truncating a non-empty page.
            let live_pagination_empty = actual_pagination_empty(actual_map);
            for (key, doc_val) in doc_map {
                let child_path = format!("{}.{}", path, key);
                match actual_map.get(key) {
                    Some(act_val) => diff_at(act_val, doc_val, &child_path, out),
                    None => {
                        if live_pagination_empty && is_pagination_token_name(key) {
                            continue;
                        }
                        out.push(ShapeViolation::ExamplesOutputDivergence {
                            path: child_path,
                            reason: ExamplesDivergenceReason::MissingField,
                        })
                    }
                }
            }
        }
        Value::Array(doc_arr) => {
            let actual_arr = match actual {
                Value::Array(a) => a,
                _ => {
                    out.push(ShapeViolation::ExamplesOutputDivergence {
                        path: path.to_string(),
                        reason: ExamplesDivergenceReason::WrongType {
                            expected: "array".to_string(),
                            got: json_type_name(actual).to_string(),
                        },
                    });
                    return;
                }
            };
            // Documented arrays are representative — recurse into element 0
            // when both sides have a first element. Empty actual is allowed
            // (AWS examples show shape, not a guarantee of cardinality).
            if let (Some(doc_first), Some(act_first)) = (doc_arr.first(), actual_arr.first()) {
                let child_path = format!("{}[0]", path);
                diff_at(act_first, doc_first, &child_path, out);
            }
        }
        // For leaves we only assert JSON-type parity; values are placeholders.
        _ => {
            let actual_kind = json_type_name(actual);
            let doc_kind = json_type_name(documented);
            // null in the documented output means "present but value omitted";
            // accept anything in the live response at that path.
            if doc_kind != actual_kind && doc_kind != "null" {
                // Smithy timestamps wire-encode as ISO-8601 strings or epoch
                // numbers depending on protocol. Tolerate the cross-form only
                // for paths whose member name looks like a timestamp.
                let is_timestamp_name = path
                    .rsplit(['.', '['])
                    .next()
                    .map(looks_like_timestamp_name)
                    .unwrap_or(false);
                let timestamp_ok = is_timestamp_name
                    && ((doc_kind == "string" && actual_kind == "number")
                        || (doc_kind == "number" && actual_kind == "string"));
                if !timestamp_ok {
                    out.push(ShapeViolation::ExamplesOutputDivergence {
                        path: path.to_string(),
                        reason: ExamplesDivergenceReason::WrongType {
                            expected: doc_kind.to_string(),
                            got: actual_kind.to_string(),
                        },
                    });
                }
            }
        }
    }
}

/// Members bound via REST HTTP traits (`@httpHeader`, `@httpQuery`,
/// `@httpLabel`, `@httpPayload`) are serialised somewhere other than the
/// JSON document, so they must be filtered out before checking which
/// structure members must appear in the body. We keep `@httpPayload` here
/// because the body validator routes payload members separately via
/// `validate_json_response`.
fn is_json_body_member(m: &crate::smithy::Member) -> bool {
    !(m.traits.http_header.is_some()
        || m.traits.http_query.is_some()
        || m.traits.http_label
        || m.traits.http_payload)
}

/// Validate a response body against the expected output shape from the Smithy model.
///
/// Returns a list of violations. An empty list means the response conforms to the model.
pub fn validate_response(
    model: &ServiceModel,
    output_shape_id: &str,
    response_body: &str,
    protocol: Protocol,
) -> Vec<ShapeViolation> {
    if response_body.is_empty() {
        // An empty body is valid if the output shape is Unit or has no
        // required members. Members bound to HTTP headers / query / labels
        // are not expected to appear in the JSON body, so they don't count
        // as "missing" here. A required `@httpPayload` member is a special
        // case: when its target is Blob or String the wire body can
        // legitimately be empty (an empty blob, an empty string), so it's
        // not a violation; but when the target is a structure / list /
        // map, an empty body means the required payload is genuinely
        // absent and must be reported.
        if let Some(ShapeType::Structure { members }) = effective_shape_type(model, output_shape_id)
        {
            let required: Vec<_> = members
                .iter()
                .filter(|m| {
                    if !m.required {
                        return false;
                    }
                    if m.traits.http_payload {
                        return !matches!(
                            effective_shape_type(model, &m.target),
                            Some(ShapeType::Blob) | Some(ShapeType::String { .. })
                        );
                    }
                    is_json_body_member(m)
                })
                .collect();
            if !required.is_empty() {
                return required
                    .iter()
                    .map(|m| ShapeViolation::MissingField {
                        path: "$".to_string(),
                        field: m.name.clone(),
                    })
                    .collect();
            }
        }
        return Vec::new();
    }

    match protocol {
        Protocol::Json { .. } | Protocol::Rest => {
            validate_json_response(model, output_shape_id, response_body)
        }
        Protocol::Query => validate_query_response(model, output_shape_id, response_body),
    }
}

/// Validate a JSON protocol response body.
fn validate_json_response(
    model: &ServiceModel,
    output_shape_id: &str,
    response_body: &str,
) -> Vec<ShapeViolation> {
    // Some operations return non-JSON bodies on success — e.g. Bedrock
    // `InvokeModel` and `InvokeModelWithResponseStream` return raw or
    // event-stream-framed binary payloads, S3 GetObject returns the object
    // bytes directly, etc. Smithy marks those with `@streaming` on a member
    // bound via `@httpPayload`, but we don't thread that flag through here
    // yet. As a pragmatic guard, treat any response whose first
    // non-whitespace character isn't `{` or `[` as an opaque streaming body
    // and trust the HTTP status code that classify_response already saw.
    let trimmed = response_body.trim_start();
    if trimmed.is_empty() || !(trimmed.starts_with('{') || trimmed.starts_with('[')) {
        return Vec::new();
    }

    // `@httpPayload` redirects body validation to the payload member's target
    // shape, not the parent structure. Bedrock `InvokeModel` is the canonical
    // example: the response body IS the provider-specific blob declared on
    // member `body`, while `contentType` rides on the Content-Type header
    // and `performanceConfigLatency` / `serviceTier` ride on response
    // headers. Without this redirect we'd flag a perfectly-formed Bedrock
    // payload as "missing required field 'body'" because no JSON envelope
    // wraps it.
    //
    // When the payload's target is a Blob or String, the body is raw bytes
    // (e.g. a provider-specific JSON blob, a binary upload, an arbitrary
    // string). We can't validate it structurally — AWS returns whatever
    // the underlying model/object produced — so accept it as opaque.
    let effective_shape_id = match effective_shape_type(model, output_shape_id) {
        Some(ShapeType::Structure { members }) => {
            if let Some(payload) = members.iter().find(|m| m.traits.http_payload) {
                match effective_shape_type(model, &payload.target) {
                    Some(ShapeType::Blob) | Some(ShapeType::String { .. }) => return Vec::new(),
                    _ => payload.target.clone(),
                }
            } else {
                output_shape_id.to_string()
            }
        }
        _ => output_shape_id.to_string(),
    };

    let value: Value = match serde_json::from_str(response_body) {
        Ok(v) => v,
        Err(e) => {
            return vec![ShapeViolation::ParseError {
                message: format!("invalid JSON: {}", e),
            }];
        }
    };

    let mut violations = Vec::new();
    validate_shape(model, &effective_shape_id, &value, "$", 0, &mut violations);
    violations
}

/// Validate a Query protocol (XML) response body.
///
/// Query protocol responses wrap their result in a `<ActionNameResponse><ActionNameResult>` element.
/// We extract field names from the XML and check required fields are present.
fn validate_query_response(
    model: &ServiceModel,
    output_shape_id: &str,
    response_body: &str,
) -> Vec<ShapeViolation> {
    let shape_type = match effective_shape_type(model, output_shape_id) {
        Some(st) => st,
        None => return Vec::new(),
    };

    let members = match &shape_type {
        ShapeType::Structure { members } => members,
        _ => return Vec::new(),
    };

    // Extract XML element names present in the response body using simple text scanning.
    // This avoids pulling in an XML parser dependency for what is essentially a field-presence check.
    let xml_fields = extract_xml_field_names(response_body);

    let mut violations = Vec::new();

    for member in members {
        if member.required && !xml_fields.contains(&member.name.as_str()) {
            violations.push(ShapeViolation::MissingField {
                path: "$".to_string(),
                field: member.name.clone(),
            });
        }
    }

    violations
}

/// Extract top-level-ish XML element names from a response body.
/// This is a best-effort extraction — it finds all `<ElementName>` opening tags.
fn extract_xml_field_names(xml: &str) -> Vec<&str> {
    let mut names = Vec::new();
    let mut rest = xml;
    while let Some(start) = rest.find('<') {
        rest = &rest[start + 1..];
        // Skip closing tags, processing instructions, comments
        if rest.starts_with('/') || rest.starts_with('?') || rest.starts_with('!') {
            continue;
        }
        // Find end of tag name
        if let Some(end) = rest.find(['>', ' ', '/']) {
            let tag_name = &rest[..end];
            if !tag_name.is_empty() {
                names.push(tag_name);
            }
        }
    }
    names
}

/// Recursively validate a JSON value against a shape definition.
fn validate_shape(
    model: &ServiceModel,
    shape_id: &str,
    value: &Value,
    path: &str,
    depth: usize,
    violations: &mut Vec<ShapeViolation>,
) {
    if depth >= MAX_DEPTH {
        return;
    }

    // JSON `null` represents an absent optional member. Required members
    // are already enforced by `validate_structure` via the missing-key
    // check on `obj.contains_key(...)` — but `null` is technically a
    // present key, so we'd previously fall through to a string-vs-null
    // WrongType error. That's wrong: AWS services routinely emit
    // `"NextToken": null` to indicate no further pages, and the SDK
    // treats it identically to the field being absent. Skip primitive
    // type validation when the value is null.
    if value.is_null() {
        return;
    }

    let shape_type = match effective_shape_type(model, shape_id) {
        Some(st) => st,
        None => return, // Unknown shape; skip validation
    };

    match &shape_type {
        ShapeType::Structure { members } => {
            validate_structure(model, members, value, path, depth, violations);
        }
        ShapeType::List { member_target } => {
            validate_list(model, member_target, value, path, depth, violations);
        }
        ShapeType::Map { value_target, .. } => {
            validate_map(model, value_target, value, path, depth, violations);
        }
        ShapeType::Union { members } => {
            // A union should be an object with exactly one key matching a member name.
            if let Value::Object(obj) = value {
                if obj.len() != 1 {
                    violations.push(ShapeViolation::WrongType {
                        path: path.to_string(),
                        expected: "union (object with exactly 1 member)".to_string(),
                        got: format!("object with {} members", obj.len()),
                    });
                }
                for (key, val) in obj {
                    let member = members.iter().find(|m| {
                        let member_key = m.traits.json_name.as_deref().unwrap_or(&m.name);
                        member_key == key.as_str()
                    });
                    if let Some(member) = member {
                        let child_path = format!("{}.{}", path, key);
                        validate_shape(
                            model,
                            &member.target,
                            val,
                            &child_path,
                            depth + 1,
                            violations,
                        );
                    } else {
                        violations.push(ShapeViolation::UnexpectedField {
                            path: path.to_string(),
                            field: key.clone(),
                        });
                    }
                }
            } else {
                violations.push(ShapeViolation::WrongType {
                    path: path.to_string(),
                    expected: "object (union)".to_string(),
                    got: json_type_name(value).to_string(),
                });
            }
        }
        ShapeType::String { .. } | ShapeType::Enum { .. } => {
            if !value.is_string() {
                violations.push(ShapeViolation::WrongType {
                    path: path.to_string(),
                    expected: "string".to_string(),
                    got: json_type_name(value).to_string(),
                });
            }
        }
        ShapeType::Integer | ShapeType::Long | ShapeType::IntEnum { .. } => {
            if !(value.is_i64() || value.is_u64()) {
                violations.push(ShapeViolation::WrongType {
                    path: path.to_string(),
                    expected: "integer".to_string(),
                    got: json_type_name(value).to_string(),
                });
            }
        }
        ShapeType::Float | ShapeType::Double => {
            if !value.is_number() {
                violations.push(ShapeViolation::WrongType {
                    path: path.to_string(),
                    expected: "number".to_string(),
                    got: json_type_name(value).to_string(),
                });
            }
        }
        ShapeType::Boolean => {
            if !value.is_boolean() {
                violations.push(ShapeViolation::WrongType {
                    path: path.to_string(),
                    expected: "boolean".to_string(),
                    got: json_type_name(value).to_string(),
                });
            }
        }
        ShapeType::Blob => {
            // Blobs are base64-encoded strings in JSON
            if !value.is_string() {
                violations.push(ShapeViolation::WrongType {
                    path: path.to_string(),
                    expected: "string (base64 blob)".to_string(),
                    got: json_type_name(value).to_string(),
                });
            }
        }
        ShapeType::Timestamp => {
            // Timestamps can be strings or numbers depending on format
            if !value.is_string() && !value.is_number() {
                violations.push(ShapeViolation::WrongType {
                    path: path.to_string(),
                    expected: "string or number (timestamp)".to_string(),
                    got: json_type_name(value).to_string(),
                });
            }
        }
        // `smithy.api#Document` is an arbitrary JSON value — any type
        // (object, array, string, number, bool, null) is acceptable.
        ShapeType::Document => {}
        // Service, Operation, Resource shapes are not value types
        ShapeType::Service | ShapeType::Operation | ShapeType::Resource => {}
    }
}

/// Validate a JSON value against a structure shape.
fn validate_structure(
    model: &ServiceModel,
    members: &[crate::smithy::Member],
    value: &Value,
    path: &str,
    depth: usize,
    violations: &mut Vec<ShapeViolation>,
) {
    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            violations.push(ShapeViolation::WrongType {
                path: path.to_string(),
                expected: "object".to_string(),
                got: json_type_name(value).to_string(),
            });
            return;
        }
    };

    // Per-member key: `@jsonName` trait on the member (or its target shape)
    // wins; otherwise the member's own name. AWS restJson1 services commonly
    // override with camelCase (e.g. `Items` -> `items`).
    let member_key = |m: &crate::smithy::Member| -> String {
        if let Some(name) = &m.traits.json_name {
            return name.clone();
        }
        if let Some(shape) = model.shapes.get(&m.target) {
            if let Some(name) = &shape.traits.json_name {
                return name.clone();
            }
        }
        m.name.clone()
    };

    // Check required fields are present. Members bound to HTTP headers,
    // query strings, labels, or the response status code don't appear in
    // the JSON body, so skip them here — the probe asserts those bindings
    // elsewhere (header presence, status code classification).
    for member in members {
        if !is_json_body_member(member) {
            continue;
        }
        let key = member_key(member);
        if member.required && !obj.contains_key(&key) {
            violations.push(ShapeViolation::MissingField {
                path: path.to_string(),
                field: key,
            });
        }
    }

    // Validate present fields
    for (key, val) in obj {
        if let Some(member) = members.iter().find(|m| member_key(m) == *key) {
            let child_path = format!("{}.{}", path, key);
            validate_shape(
                model,
                &member.target,
                val,
                &child_path,
                depth + 1,
                violations,
            );
        } else {
            violations.push(ShapeViolation::UnexpectedField {
                path: path.to_string(),
                field: key.clone(),
            });
        }
    }
}

/// Validate a JSON value against a list shape.
fn validate_list(
    model: &ServiceModel,
    member_target: &str,
    value: &Value,
    path: &str,
    depth: usize,
    violations: &mut Vec<ShapeViolation>,
) {
    let arr = match value.as_array() {
        Some(a) => a,
        None => {
            violations.push(ShapeViolation::WrongType {
                path: path.to_string(),
                expected: "array".to_string(),
                got: json_type_name(value).to_string(),
            });
            return;
        }
    };

    for (i, item) in arr.iter().enumerate() {
        let child_path = format!("{}[{}]", path, i);
        validate_shape(
            model,
            member_target,
            item,
            &child_path,
            depth + 1,
            violations,
        );
    }
}

/// Validate a JSON value against a map shape.
fn validate_map(
    model: &ServiceModel,
    value_target: &str,
    value: &Value,
    path: &str,
    depth: usize,
    violations: &mut Vec<ShapeViolation>,
) {
    let obj = match value.as_object() {
        Some(o) => o,
        None => {
            violations.push(ShapeViolation::WrongType {
                path: path.to_string(),
                expected: "object (map)".to_string(),
                got: json_type_name(value).to_string(),
            });
            return;
        }
    };

    for (key, val) in obj {
        let child_path = format!("{}[{:?}]", path, key);
        validate_shape(model, value_target, val, &child_path, depth + 1, violations);
    }
}

/// Heuristic: does the trailing member name look like a Smithy timestamp?
/// Used to scope the string-vs-number tolerance in `diff_against_example`.
/// AWS pagination tokens use a small set of conventional member names.
/// Recognise them so we can skip "documented but missing" checks when the
/// live response has nothing to paginate.
fn is_pagination_token_name(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    matches!(
        n.as_str(),
        "nexttoken"
            | "nextmarker"
            | "marker"
            | "continuationtoken"
            | "nextcontinuationtoken"
            | "nextpagetoken"
            | "pagetoken"
            | "nextforwardtoken"
            | "nextbackwardtoken"
    )
}

/// True when every value in the live response that looks like a paginated
/// collection (array or list-typed map) is empty. Used to relax the
/// pagination-token presence check.
fn actual_pagination_empty(actual_map: &serde_json::Map<String, Value>) -> bool {
    let mut saw_collection = false;
    for value in actual_map.values() {
        if let Value::Array(arr) = value {
            saw_collection = true;
            if !arr.is_empty() {
                return false;
            }
        }
    }
    saw_collection
}

fn looks_like_timestamp_name(name: &str) -> bool {
    let n = name.to_ascii_lowercase();
    n.ends_with("time")
        || n.ends_with("date")
        || n.ends_with("timestamp")
        || n.ends_with("at")
        || n.ends_with("expires")
        || n.ends_with("modified")
        || n == "createdat"
        || n == "modifiedat"
}

/// Return a human-readable name for a JSON value type.
fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smithy::{Member, Shape, ShapeTraits};
    use std::collections::HashMap;

    /// Build a minimal ServiceModel for testing.
    fn test_model() -> ServiceModel {
        let mut shapes = HashMap::new();

        // A simple output structure: { "QueueUrl": string (required), "Tags": map }
        shapes.insert(
            "test#CreateQueueOutput".to_string(),
            Shape {
                shape_id: "test#CreateQueueOutput".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![
                        Member {
                            name: "QueueUrl".to_string(),
                            target: "smithy.api#String".to_string(),
                            required: true,
                            traits: ShapeTraits::default(),
                        },
                        Member {
                            name: "Tags".to_string(),
                            target: "test#TagMap".to_string(),
                            required: false,
                            traits: ShapeTraits::default(),
                        },
                    ],
                },
                traits: ShapeTraits::default(),
            },
        );

        // TagMap: map<String, String>
        shapes.insert(
            "test#TagMap".to_string(),
            Shape {
                shape_id: "test#TagMap".to_string(),
                shape_type: ShapeType::Map {
                    key_target: "smithy.api#String".to_string(),
                    value_target: "smithy.api#String".to_string(),
                },
                traits: ShapeTraits::default(),
            },
        );

        // A structure with a list member
        shapes.insert(
            "test#ListOutput".to_string(),
            Shape {
                shape_id: "test#ListOutput".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![Member {
                        name: "Items".to_string(),
                        target: "test#ItemList".to_string(),
                        required: true,
                        traits: ShapeTraits::default(),
                    }],
                },
                traits: ShapeTraits::default(),
            },
        );

        shapes.insert(
            "test#ItemList".to_string(),
            Shape {
                shape_id: "test#ItemList".to_string(),
                shape_type: ShapeType::List {
                    member_target: "test#Item".to_string(),
                },
                traits: ShapeTraits::default(),
            },
        );

        shapes.insert(
            "test#Item".to_string(),
            Shape {
                shape_id: "test#Item".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![
                        Member {
                            name: "Id".to_string(),
                            target: "smithy.api#Integer".to_string(),
                            required: true,
                            traits: ShapeTraits::default(),
                        },
                        Member {
                            name: "Name".to_string(),
                            target: "smithy.api#String".to_string(),
                            required: true,
                            traits: ShapeTraits::default(),
                        },
                    ],
                },
                traits: ShapeTraits::default(),
            },
        );

        ServiceModel {
            service_name: "test".to_string(),
            operations: Vec::new(),
            shapes,
        }
    }

    #[test]
    fn valid_json_response() {
        let model = test_model();
        let body = r#"{"QueueUrl": "http://localhost:4566/queue/test"}"#;
        let violations = validate_response(
            &model,
            "test#CreateQueueOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert!(
            violations.is_empty(),
            "expected no violations: {:?}",
            violations
        );
    }

    #[test]
    fn missing_required_field() {
        let model = test_model();
        let body = r#"{"Tags": {}}"#;
        let violations = validate_response(
            &model,
            "test#CreateQueueOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            ShapeViolation::MissingField { field, .. } if field == "QueueUrl"
        ));
    }

    #[test]
    fn wrong_type_field() {
        let model = test_model();
        let body = r#"{"QueueUrl": 123}"#;
        let violations = validate_response(
            &model,
            "test#CreateQueueOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            ShapeViolation::WrongType { expected, got, .. } if expected == "string" && got == "number"
        ));
    }

    #[test]
    fn unexpected_field() {
        let model = test_model();
        let body = r#"{"QueueUrl": "http://localhost/q", "Bogus": true}"#;
        let violations = validate_response(
            &model,
            "test#CreateQueueOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            ShapeViolation::UnexpectedField { field, .. } if field == "Bogus"
        ));
    }

    #[test]
    fn nested_list_validation() {
        let model = test_model();
        let body = r#"{"Items": [{"Id": 1, "Name": "a"}, {"Id": "bad", "Name": "b"}]}"#;
        let violations = validate_response(
            &model,
            "test#ListOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            ShapeViolation::WrongType { path, expected, got } if path == "$.Items[1].Id" && expected == "integer" && got == "string"
        ));
    }

    #[test]
    fn map_validation() {
        let model = test_model();
        let body = r#"{"QueueUrl": "http://localhost/q", "Tags": {"env": "dev", "bad": 42}}"#;
        let violations = validate_response(
            &model,
            "test#CreateQueueOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            ShapeViolation::WrongType { expected, got, .. } if expected == "string" && got == "number"
        ));
    }

    #[test]
    fn invalid_json_body() {
        let model = test_model();
        // Malformed JSON that *looks* like JSON (starts with `{`) — parsing
        // still fails and must surface as a ParseError. Bodies that don't
        // look like JSON at all are treated as opaque streaming payloads
        // and are not validated here; see `non_json_body_is_opaque`.
        let body = "{not valid";
        let violations = validate_response(
            &model,
            "test#CreateQueueOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert_eq!(violations.len(), 1);
        assert!(matches!(&violations[0], ShapeViolation::ParseError { .. }));
    }

    #[test]
    fn non_json_body_is_opaque() {
        // Some operations (Bedrock InvokeModel, S3 GetObject, ...) return
        // binary or event-stream-framed bodies. Those don't look like JSON
        // and must not be flagged as parse errors.
        let model = test_model();
        let body = "not json at all";
        let violations = validate_response(
            &model,
            "test#CreateQueueOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert!(violations.is_empty(), "got {:?}", violations);
    }

    #[test]
    fn empty_body_with_required_fields() {
        let model = test_model();
        let violations = validate_response(
            &model,
            "test#CreateQueueOutput",
            "",
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            ShapeViolation::MissingField { field, .. } if field == "QueueUrl"
        ));
    }

    #[test]
    fn query_protocol_checks_required_fields() {
        let model = test_model();
        let xml = r#"<CreateQueueResponse><CreateQueueResult><QueueUrl>http://localhost/q</QueueUrl></CreateQueueResult></CreateQueueResponse>"#;
        let violations = validate_response(&model, "test#CreateQueueOutput", xml, Protocol::Query);
        assert!(
            violations.is_empty(),
            "expected no violations: {:?}",
            violations
        );
    }

    #[test]
    fn query_protocol_missing_required_field() {
        let model = test_model();
        let xml = r#"<CreateQueueResponse><CreateQueueResult><Tags></Tags></CreateQueueResult></CreateQueueResponse>"#;
        let violations = validate_response(&model, "test#CreateQueueOutput", xml, Protocol::Query);
        assert_eq!(violations.len(), 1);
        assert!(matches!(
            &violations[0],
            ShapeViolation::MissingField { field, .. } if field == "QueueUrl"
        ));
    }

    #[test]
    fn rest_protocol_validates_as_json() {
        let model = test_model();
        let body = r#"{"QueueUrl": "http://localhost/q"}"#;
        let violations = validate_response(&model, "test#CreateQueueOutput", body, Protocol::Rest);
        assert!(
            violations.is_empty(),
            "expected no violations: {:?}",
            violations
        );
    }

    #[test]
    fn unknown_shape_is_lenient() {
        let model = test_model();
        let body = r#"{"anything": "goes"}"#;
        let violations = validate_response(
            &model,
            "test#DoesNotExist",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert!(violations.is_empty());
    }

    /// Build a model where the output member has a `@jsonName` override
    /// (`Items` -> `items`), matching the restJson1 pattern used by
    /// API Gateway v2 and most AWS REST services.
    fn json_name_model() -> ServiceModel {
        let mut shapes = HashMap::new();

        let items_traits = ShapeTraits {
            json_name: Some("items".to_string()),
            ..ShapeTraits::default()
        };

        shapes.insert(
            "test#GetApisOutput".to_string(),
            Shape {
                shape_id: "test#GetApisOutput".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![Member {
                        name: "Items".to_string(),
                        target: "test#ApiList".to_string(),
                        required: false,
                        traits: items_traits,
                    }],
                },
                traits: ShapeTraits::default(),
            },
        );

        shapes.insert(
            "test#ApiList".to_string(),
            Shape {
                shape_id: "test#ApiList".to_string(),
                shape_type: ShapeType::List {
                    member_target: "smithy.api#String".to_string(),
                },
                traits: ShapeTraits::default(),
            },
        );

        ServiceModel {
            service_name: "test".to_string(),
            operations: Vec::new(),
            shapes,
        }
    }

    #[test]
    fn json_name_override_accepts_lowercase_key() {
        let model = json_name_model();
        // Response uses the @jsonName ("items"), not the Smithy PascalCase
        // member name ("Items"). Must validate cleanly.
        let body = r#"{"items": ["a", "b"]}"#;
        let violations = validate_response(
            &model,
            "test#GetApisOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert!(
            violations.is_empty(),
            "expected no violations, got {:?}",
            violations
        );
    }

    #[test]
    fn json_name_override_rejects_pascal_key() {
        let model = json_name_model();
        // Member has @jsonName("items"), so a response using the PascalCase
        // Smithy name is NOT a valid restJson1 payload — flag it.
        let body = r#"{"Items": ["a"]}"#;
        let violations = validate_response(
            &model,
            "test#GetApisOutput",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert!(
            violations.iter().any(
                |v| matches!(v, ShapeViolation::UnexpectedField { field, .. } if field == "Items")
            ),
            "expected an UnexpectedField(Items) violation, got {:?}",
            violations
        );
    }

    /// Model where the output has a `@httpPayload` member targeting a
    /// blob (Bedrock `InvokeModelResponse.body` pattern) plus a header
    /// member (`contentType`) that must NOT be required-in-body.
    fn http_payload_blob_model() -> ServiceModel {
        let mut shapes = HashMap::new();

        shapes.insert(
            "test#InvokeOutput".to_string(),
            Shape {
                shape_id: "test#InvokeOutput".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![
                        Member {
                            name: "body".to_string(),
                            target: "test#Body".to_string(),
                            required: true,
                            traits: ShapeTraits {
                                http_payload: true,
                                ..ShapeTraits::default()
                            },
                        },
                        Member {
                            name: "contentType".to_string(),
                            target: "smithy.api#String".to_string(),
                            required: true,
                            traits: ShapeTraits {
                                http_header: Some("Content-Type".to_string()),
                                ..ShapeTraits::default()
                            },
                        },
                    ],
                },
                traits: ShapeTraits::default(),
            },
        );
        shapes.insert(
            "test#Body".to_string(),
            Shape {
                shape_id: "test#Body".to_string(),
                shape_type: ShapeType::Blob,
                traits: ShapeTraits::default(),
            },
        );

        ServiceModel {
            service_name: "test".to_string(),
            operations: Vec::new(),
            shapes,
        }
    }

    #[test]
    fn http_payload_blob_body_is_opaque() {
        // Bedrock InvokeModel: the wire body IS the blob (a provider JSON
        // payload). The validator must not look up `body` / `contentType`
        // as structure fields and must not flag unknown keys inside the
        // provider JSON.
        let model = http_payload_blob_model();
        let body = r#"{"completion": "hello", "stop_reason": "end_turn"}"#;
        let violations = validate_response(&model, "test#InvokeOutput", body, Protocol::Rest);
        assert!(
            violations.is_empty(),
            "expected no violations (httpPayload Blob is opaque), got {:?}",
            violations
        );
    }

    /// Model where the output has a `@httpPayload` member targeting a
    /// structure (not Blob/String). Empty wire body must still report
    /// the payload as missing.
    fn http_payload_struct_model() -> ServiceModel {
        let mut shapes = HashMap::new();

        shapes.insert(
            "test#GetThingOutput".to_string(),
            Shape {
                shape_id: "test#GetThingOutput".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![Member {
                        name: "thing".to_string(),
                        target: "test#Thing".to_string(),
                        required: true,
                        traits: ShapeTraits {
                            http_payload: true,
                            ..ShapeTraits::default()
                        },
                    }],
                },
                traits: ShapeTraits::default(),
            },
        );
        shapes.insert(
            "test#Thing".to_string(),
            Shape {
                shape_id: "test#Thing".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![Member {
                        name: "id".to_string(),
                        target: "smithy.api#String".to_string(),
                        required: true,
                        traits: ShapeTraits::default(),
                    }],
                },
                traits: ShapeTraits::default(),
            },
        );

        ServiceModel {
            service_name: "test".to_string(),
            operations: Vec::new(),
            shapes,
        }
    }

    #[test]
    fn empty_body_flags_required_struct_payload() {
        // Required `@httpPayload` targeting a structure: empty wire body
        // genuinely means the payload is missing — must report.
        let model = http_payload_struct_model();
        let v = validate_response(&model, "test#GetThingOutput", "", Protocol::Rest);
        assert!(
            v.iter().any(
                |x| matches!(x, ShapeViolation::MissingField { field, .. } if field == "thing")
            ),
            "expected MissingField(thing) for empty body, got {:?}",
            v
        );
    }

    #[test]
    fn empty_body_skips_http_bound_required_members() {
        // An empty body must not be reported as missing `contentType` —
        // that member rides on a response header. Same for `body` because
        // it's `@httpPayload` (we drop both via `is_json_body_member`).
        let model = http_payload_blob_model();
        let violations = validate_response(&model, "test#InvokeOutput", "", Protocol::Rest);
        assert!(
            violations.is_empty(),
            "empty body shouldn't flag header-bound required members, got {:?}",
            violations
        );
    }

    /// Model where a structure member targets `smithy.api#Document`. A
    /// Document accepts any JSON value, not just strings.
    fn document_model() -> ServiceModel {
        let mut shapes = HashMap::new();
        shapes.insert(
            "test#ToolUseBlock".to_string(),
            Shape {
                shape_id: "test#ToolUseBlock".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![Member {
                        name: "input".to_string(),
                        target: "smithy.api#Document".to_string(),
                        required: true,
                        traits: ShapeTraits::default(),
                    }],
                },
                traits: ShapeTraits::default(),
            },
        );
        ServiceModel {
            service_name: "test".to_string(),
            operations: Vec::new(),
            shapes,
        }
    }

    #[test]
    fn document_member_accepts_arbitrary_json() {
        let model = document_model();
        // Object Document.
        let body = r#"{"input": {"location": "Seattle"}}"#;
        let v = validate_response(
            &model,
            "test#ToolUseBlock",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert!(v.is_empty(), "object Document should pass, got {:?}", v);

        // String Document.
        let body = r#"{"input": "raw"}"#;
        let v = validate_response(
            &model,
            "test#ToolUseBlock",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert!(v.is_empty(), "string Document should pass, got {:?}", v);

        // Array Document.
        let body = r#"{"input": [1, 2, 3]}"#;
        let v = validate_response(
            &model,
            "test#ToolUseBlock",
            body,
            Protocol::Json {
                target_prefix: "Test",
            },
        );
        assert!(v.is_empty(), "array Document should pass, got {:?}", v);
    }

    // ----- diff_against_example -----

    #[test]
    fn diff_flags_field_documented_but_missing() {
        // Mirrors #816: documented output has BucketRegion, response doesn't.
        let documented = serde_json::json!({
            "Buckets": [{"Name": "x", "BucketRegion": "us-east-1"}]
        });
        let actual = serde_json::json!({
            "Buckets": [{"Name": "x"}]
        });
        let v = diff_against_example(&actual, &documented);
        assert!(
            v.iter().any(|x| matches!(
                x,
                ShapeViolation::ExamplesOutputDivergence { path, reason: ExamplesDivergenceReason::MissingField }
                    if path == "$.Buckets[0].BucketRegion"
            )),
            "expected MissingField at $.Buckets[0].BucketRegion, got {:?}",
            v
        );
    }

    #[test]
    fn diff_flags_wrong_json_type() {
        let documented = serde_json::json!({"Count": 3});
        let actual = serde_json::json!({"Count": "three"});
        let v = diff_against_example(&actual, &documented);
        assert!(
            v.iter().any(|x| matches!(
                x,
                ShapeViolation::ExamplesOutputDivergence { reason: ExamplesDivergenceReason::WrongType { expected, got }, .. }
                    if expected == "number" && got == "string"
            )),
            "expected WrongType(number,string), got {:?}",
            v
        );
    }

    #[test]
    fn diff_ignores_string_value_differences() {
        // Documented uses placeholder, actual uses real value — both strings,
        // so no violation. Value identity is not asserted.
        let documented = serde_json::json!({"Name": "examplebucket"});
        let actual = serde_json::json!({"Name": "production-logs"});
        assert!(diff_against_example(&actual, &documented).is_empty());
    }

    #[test]
    fn diff_ignores_extra_fields_in_response() {
        // Live response superset of documented: not a violation.
        let documented = serde_json::json!({"A": "x"});
        let actual = serde_json::json!({"A": "y", "B": 42, "C": null});
        assert!(diff_against_example(&actual, &documented).is_empty());
    }

    #[test]
    fn diff_recurses_into_objects() {
        let documented = serde_json::json!({"Owner": {"ID": "id", "DisplayName": "n"}});
        let actual = serde_json::json!({"Owner": {"ID": "z"}});
        let v = diff_against_example(&actual, &documented);
        assert_eq!(v.len(), 1);
        assert!(matches!(
            &v[0],
            ShapeViolation::ExamplesOutputDivergence { path, reason: ExamplesDivergenceReason::MissingField }
                if path == "$.Owner.DisplayName"
        ));
    }

    #[test]
    fn diff_compares_first_array_element_when_both_present() {
        let documented = serde_json::json!({"Items": [{"K": "v", "Region": "us-east-1"}]});
        let actual = serde_json::json!({"Items": [{"K": "v"}]});
        let v = diff_against_example(&actual, &documented);
        assert!(v.iter().any(|x| matches!(
            x,
            ShapeViolation::ExamplesOutputDivergence { path, .. } if path == "$.Items[0].Region"
        )));
    }

    #[test]
    fn diff_allows_empty_actual_array() {
        // AWS examples show shape with 1 element; live response with 0
        // elements is fine — pagination, filtering, or just no resources.
        let documented = serde_json::json!({"Items": [{"Name": "x"}]});
        let actual = serde_json::json!({"Items": []});
        assert!(diff_against_example(&actual, &documented).is_empty());
    }

    #[test]
    fn diff_treats_documented_null_as_wildcard() {
        // Documented `null` is a value placeholder — anything in actual is OK.
        let documented = serde_json::json!({"Field": null});
        let actual = serde_json::json!({"Field": "anything"});
        assert!(diff_against_example(&actual, &documented).is_empty());
    }

    #[test]
    fn diff_tolerates_timestamp_string_vs_number() {
        // Smithy timestamps wire-encode as string OR number across protocols.
        // Don't flag either form when the documented example shows the other.
        let documented = serde_json::json!({"CreatedAt": "2024-01-01T00:00:00Z"});
        let actual = serde_json::json!({"CreatedAt": 1704067200});
        assert!(diff_against_example(&actual, &documented).is_empty());
    }

    #[test]
    fn diff_handles_top_level_object_mismatch() {
        let documented = serde_json::json!({"A": 1});
        let actual = serde_json::json!([1, 2, 3]);
        let v = diff_against_example(&actual, &documented);
        assert!(v
            .iter()
            .any(|x| matches!(x, ShapeViolation::ExamplesOutputDivergence { .. })));
    }
}
