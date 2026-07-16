//! Model-driven input validation for Amazon Timestream.
//!
//! AWS validates request members against the Smithy model's `required`,
//! `length`, `range` and `enum` constraints and rejects a violating request
//! before any business logic runs. This module reproduces that: the combined
//! Timestream (write + query) Smithy model is embedded and parsed once, and
//! [`validate_input`] checks a request body's top-level members against the
//! operation's input shape, returning the message for the first violation. The
//! message is surfaced as a `ValidationException` by the caller.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde_json::Value;

/// The vendored combined Timestream Smithy model (write + query merged under a
/// single service shape), embedded so validation needs no runtime file access.
const MODEL_JSON: &str = include_str!("../model.json");

/// The embedded model parses to exactly this many distinct operations (keyed by
/// short name; the four ops shared between write and query dedup to one). A
/// mismatch means the vendored model drifted from the implemented surface.
#[cfg(test)]
pub const OPERATION_COUNT: usize = 30;

#[derive(Debug, Default)]
struct MemberConstraint {
    name: String,
    required: bool,
    min_len: Option<u64>,
    max_len: Option<u64>,
    min_val: Option<f64>,
    max_val: Option<f64>,
    enum_values: Option<Vec<String>>,
}

type OpConstraints = HashMap<String, Vec<MemberConstraint>>;

fn constraints() -> &'static OpConstraints {
    static CELL: OnceLock<OpConstraints> = OnceLock::new();
    CELL.get_or_init(build_constraints)
}

fn short(id: &str) -> &str {
    id.rsplit('#').next().unwrap_or(id)
}

fn build_constraints() -> OpConstraints {
    let model: Value = serde_json::from_str(MODEL_JSON).expect("embedded timestream model parses");
    let shapes = model
        .get("shapes")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    // Follow the SERVICE shape's operations list rather than every operation
    // shape in the model. The four ops shared between the write and query
    // namespaces (`TagResource`, `UntagResource`, `ListTagsForResource`,
    // `DescribeEndpoints`) exist twice with the same short name but subtly
    // different constraints (e.g. `AmazonResourceName` max length is 1011 in
    // write, 2048 in query). The combined model's single service shape lists
    // the write-namespace variants, and the conformance probe generates its
    // boundary cases from exactly those targets, so validation must resolve to
    // the same targets or a too-long ARN slips through as a false 200.
    let op_targets: Vec<String> = shapes
        .values()
        .find(|s| s.get("type").and_then(Value::as_str) == Some("service"))
        .and_then(|svc| svc.get("operations").and_then(Value::as_array))
        .map(|ops| {
            ops.iter()
                .filter_map(|o| o.get("target").and_then(Value::as_str).map(str::to_string))
                .collect()
        })
        .unwrap_or_default();

    let mut out = OpConstraints::new();
    for sid in &op_targets {
        let Some(shape) = shapes.get(sid) else {
            continue;
        };
        if shape.get("type").and_then(Value::as_str) != Some("operation") {
            continue;
        }
        let op_name = short(sid).to_string();
        let Some(input_id) = shape.pointer("/input/target").and_then(Value::as_str) else {
            out.entry(op_name).or_default();
            continue;
        };
        let Some(input_shape) = shapes.get(input_id) else {
            out.entry(op_name).or_default();
            continue;
        };
        let mut members = Vec::new();
        if let Some(map) = input_shape.get("members").and_then(Value::as_object) {
            for (mname, member) in map {
                let mut c = MemberConstraint {
                    name: mname.clone(),
                    ..Default::default()
                };
                if let Some(t) = member.get("traits").and_then(Value::as_object) {
                    if t.contains_key("smithy.api#required") {
                        c.required = true;
                    }
                    apply_traits(t, &mut c);
                }
                if let Some(target_id) = member.get("target").and_then(Value::as_str) {
                    apply_shape_constraints(&shapes, target_id, &mut c);
                }
                members.push(c);
            }
        }
        out.insert(op_name, members);
    }
    out
}

fn apply_shape_constraints(
    shapes: &serde_json::Map<String, Value>,
    target_id: &str,
    c: &mut MemberConstraint,
) {
    let Some(shape) = shapes.get(target_id) else {
        return;
    };
    if let Some(t) = shape.get("traits").and_then(Value::as_object) {
        apply_traits(t, c);
    }
    if shape.get("type").and_then(Value::as_str) == Some("enum") {
        if let Some(map) = shape.get("members").and_then(Value::as_object) {
            let vals: Vec<String> = map
                .iter()
                .map(|(k, m)| {
                    m.pointer("/traits/smithy.api#enumValue")
                        .and_then(Value::as_str)
                        .map(String::from)
                        .unwrap_or_else(|| k.clone())
                })
                .collect();
            if !vals.is_empty() {
                c.enum_values = Some(vals);
            }
        }
    }
}

fn apply_traits(t: &serde_json::Map<String, Value>, c: &mut MemberConstraint) {
    if let Some(len) = t.get("smithy.api#length") {
        if let Some(min) = len.get("min").and_then(Value::as_u64) {
            c.min_len = Some(min);
        }
        if let Some(max) = len.get("max").and_then(Value::as_u64) {
            c.max_len = Some(max);
        }
    }
    if let Some(range) = t.get("smithy.api#range") {
        if let Some(min) = range.get("min").and_then(Value::as_f64) {
            c.min_val = Some(min);
        }
        if let Some(max) = range.get("max").and_then(Value::as_f64) {
            c.max_val = Some(max);
        }
    }
    if let Some(items) = t.get("smithy.api#enum").and_then(Value::as_array) {
        let vals: Vec<String> = items
            .iter()
            .filter_map(|i| i.get("value").and_then(Value::as_str).map(String::from))
            .collect();
        if !vals.is_empty() {
            c.enum_values = Some(vals);
        }
    }
}

/// Validate a request body against the operation's input-shape constraints.
/// Returns `Err(message)` for the first violation, or `Ok(())` when every
/// top-level member is in bounds.
pub fn validate_input(op: &str, body: &Value) -> Result<(), String> {
    let Some(members) = constraints().get(op) else {
        return Ok(());
    };
    for c in members {
        let present = body.get(&c.name).filter(|v| !v.is_null());
        match present {
            None => {
                if c.required {
                    return Err(format!(
                        "1 validation error detected: Value null at '{}' failed to satisfy constraint: Member must not be null",
                        c.name
                    ));
                }
            }
            Some(v) => {
                if let Some(s) = v.as_str() {
                    let len = s.chars().count() as u64;
                    if let Some(min) = c.min_len {
                        if len < min {
                            return Err(len_msg(&c.name, "greater than or equal to", min));
                        }
                    }
                    if let Some(max) = c.max_len {
                        if len > max {
                            return Err(len_msg(&c.name, "less than or equal to", max));
                        }
                    }
                    if let Some(vals) = &c.enum_values {
                        if !vals.iter().any(|x| x == s) {
                            return Err(format!(
                                "1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must satisfy enum value set: {:?}",
                                s, c.name, vals
                            ));
                        }
                    }
                }
                if let Some(arr) = v.as_array() {
                    let len = arr.len() as u64;
                    if let Some(min) = c.min_len {
                        if len < min {
                            return Err(len_msg(&c.name, "greater than or equal to", min));
                        }
                    }
                    if let Some(max) = c.max_len {
                        if len > max {
                            return Err(len_msg(&c.name, "less than or equal to", max));
                        }
                    }
                }
                if let Some(n) = v.as_f64() {
                    if let Some(min) = c.min_val {
                        if n < min {
                            return Err(format!(
                                "1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must have value greater than or equal to {}",
                                n, c.name, min
                            ));
                        }
                    }
                    if let Some(max) = c.max_val {
                        if n > max {
                            return Err(format!(
                                "1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must have value less than or equal to {}",
                                n, c.name, max
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn len_msg(name: &str, cmp: &str, bound: u64) -> String {
    format!(
        "1 validation error detected: Value at '{name}' failed to satisfy constraint: Member must have length {cmp} {bound}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn missing_required_member_is_rejected() {
        let err = validate_input("CreateDatabase", &json!({})).unwrap_err();
        assert!(err.contains("DatabaseName"));
    }

    #[test]
    fn valid_input_passes() {
        assert!(validate_input("CreateDatabase", &json!({ "DatabaseName": "metrics" })).is_ok());
    }

    #[test]
    fn query_requires_query_string() {
        let err = validate_input("Query", &json!({})).unwrap_err();
        assert!(err.contains("QueryString"));
    }

    #[test]
    fn all_operations_have_constraint_entries() {
        assert!(constraints().contains_key("CreateTable"));
        assert!(constraints().contains_key("WriteRecords"));
        assert!(constraints().contains_key("Query"));
        assert!(constraints().contains_key("DescribeEndpoints"));
        assert_eq!(constraints().len(), OPERATION_COUNT);
    }
}
