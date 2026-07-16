//! Model-driven input validation for AWS Config.
//!
//! AWS validates request members against the Smithy model's `required`,
//! `length`, `range` and `enum` constraints and returns a client error before
//! any business logic runs. This module reproduces that: the Config Smithy
//! model is embedded and parsed once, and [`validate_input`] checks a request
//! body's top-level members against the operation's input shape, returning the
//! error AWS returns (`ValidationException`) for the first violation.

use std::collections::HashMap;
use std::sync::OnceLock;

use serde_json::Value;

/// The vendored Config Smithy model (byte-for-byte from aws/api-models-aws),
/// embedded so validation needs no runtime file access.
const MODEL_JSON: &str = include_str!("../model.json");

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
    let model: Value = serde_json::from_str(MODEL_JSON).expect("embedded config model parses");
    let shapes = model
        .get("shapes")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut out = OpConstraints::new();
    for (sid, shape) in &shapes {
        if shape.get("type").and_then(Value::as_str) != Some("operation") {
            continue;
        }
        let op_name = short(sid).to_string();
        let Some(input_id) = shape.pointer("/input/target").and_then(Value::as_str) else {
            out.insert(op_name, Vec::new());
            continue;
        };
        let Some(input_shape) = shapes.get(input_id) else {
            out.insert(op_name, Vec::new());
            continue;
        };
        let mut members = Vec::new();
        if let Some(map) = input_shape.get("members").and_then(Value::as_object) {
            for (mname, member) in map {
                let mut c = MemberConstraint {
                    name: mname.clone(),
                    ..Default::default()
                };
                let member_traits = member.get("traits").and_then(Value::as_object);
                if let Some(t) = member_traits {
                    if t.contains_key("smithy.api#required") {
                        c.required = true;
                    }
                }
                // Constraints usually live on the target shape.
                if let Some(target_id) = member.get("target").and_then(Value::as_str) {
                    apply_shape_constraints(&shapes, target_id, &mut c);
                }
                // Length/range can also be reasserted on the member itself.
                if let Some(t) = member_traits {
                    apply_traits(t, &mut c);
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
    // Smithy 2.0 enum shape: collect its enum values.
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
    // Legacy `smithy.api#enum` trait: list of { value: ... }.
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
/// Returns `Err(message)` for the first violation (surfaced as
/// `ValidationException`), or `Ok(())` when every top-level member is in bounds.
pub fn validate_input(op: &str, body: &Value) -> Result<(), String> {
    let Some(members) = constraints().get(op) else {
        return Ok(());
    };
    for c in members {
        let present = body.get(&c.name).filter(|v| !v.is_null());
        match present {
            None => {
                if c.required {
                    return Err(format!("1 validation error detected: Value null at '{}' failed to satisfy constraint: Member must not be null", c.name));
                }
            }
            Some(v) => {
                if let Some(s) = v.as_str() {
                    let len = s.chars().count() as u64;
                    if let Some(min) = c.min_len {
                        if len < min {
                            return Err(len_msg(&c.name, s, "greater than or equal to", min));
                        }
                    }
                    if let Some(max) = c.max_len {
                        if len > max {
                            return Err(len_msg(&c.name, s, "less than or equal to", max));
                        }
                    }
                    if let Some(vals) = &c.enum_values {
                        if !vals.iter().any(|x| x == s) {
                            return Err(format!("1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must satisfy enum value set: {:?}", s, c.name, vals));
                        }
                    }
                }
                if let Some(n) = v.as_f64() {
                    if let Some(min) = c.min_val {
                        if n < min {
                            return Err(format!("1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must have value greater than or equal to {}", n, c.name, min));
                        }
                    }
                    if let Some(max) = c.max_val {
                        if n > max {
                            return Err(format!("1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must have value less than or equal to {}", n, c.name, max));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn len_msg(name: &str, val: &str, cmp: &str, bound: u64) -> String {
    format!(
        "1 validation error detected: Value '{val}' at '{name}' failed to satisfy constraint: Member must have length {cmp} {bound}"
    )
}
