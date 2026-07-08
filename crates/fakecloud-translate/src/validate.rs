//! Model-driven input validation for Amazon Translate.
//!
//! AWS validates request members against the Smithy model's `required`,
//! `length`, `range`, `enum` and `pattern` constraints and returns a
//! validation exception before any business logic runs. This module reproduces
//! that: the Translate Smithy model is embedded and parsed once, and
//! [`validate_input`] checks a request body's top-level members against the
//! operation's input shape, returning the message for the first violation.
//!
//! The wire error *code* a violation is surfaced as differs per operation:
//! Translate declares `InvalidParameterValueException` on most control-plane
//! ops but only `InvalidRequestException` on the translate/document ops.
//! [`validation_error_code`] resolves the code from each operation's declared
//! `errors:` list so the returned exception is always one the operation's
//! Smithy contract admits.

use std::collections::HashMap;
use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;

/// The vendored Translate Smithy model (byte-for-byte from aws/api-models-aws),
/// embedded so validation needs no runtime file access.
const MODEL_JSON: &str = include_str!("../../../aws-models/translate.json");

#[derive(Debug, Default)]
struct MemberConstraint {
    name: String,
    required: bool,
    min_len: Option<u64>,
    max_len: Option<u64>,
    min_val: Option<f64>,
    max_val: Option<f64>,
    enum_values: Option<Vec<String>>,
    pattern: Option<Regex>,
}

struct OpInfo {
    members: Vec<MemberConstraint>,
    /// The wire error code to surface a validation failure as, chosen from the
    /// operation's declared client errors.
    validation_code: String,
}

type OpMap = HashMap<String, OpInfo>;

fn ops() -> &'static OpMap {
    static CELL: OnceLock<OpMap> = OnceLock::new();
    CELL.get_or_init(build_ops)
}

fn short(id: &str) -> &str {
    id.rsplit('#').next().unwrap_or(id)
}

/// Client validation error codes, most-preferred first. The first one an
/// operation declares is used to surface its constraint violations.
const VALIDATION_CODE_PREFERENCE: &[&str] = &[
    "InvalidParameterValueException",
    "InvalidRequestException",
    "InvalidFilterException",
    "UnsupportedDisplayLanguageCodeException",
];

fn build_ops() -> OpMap {
    let model: Value = serde_json::from_str(MODEL_JSON).expect("embedded translate model parses");
    let shapes = model
        .get("shapes")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut out = OpMap::new();
    for (sid, shape) in &shapes {
        if shape.get("type").and_then(Value::as_str) != Some("operation") {
            continue;
        }
        let op_name = short(sid).to_string();

        // Resolve the validation error code from the op's declared errors.
        let declared: Vec<String> = shape
            .get("errors")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| e.get("target").and_then(Value::as_str))
                    .map(|t| short(t).to_string())
                    .collect()
            })
            .unwrap_or_default();
        let validation_code = VALIDATION_CODE_PREFERENCE
            .iter()
            .find(|pref| declared.iter().any(|d| d == *pref))
            .map(|s| s.to_string())
            .unwrap_or_else(|| "InvalidRequestException".to_string());

        let members = shape
            .pointer("/input/target")
            .and_then(Value::as_str)
            .and_then(|input_id| shapes.get(input_id))
            .map(|input_shape| build_members(&shapes, input_shape))
            .unwrap_or_default();

        out.insert(
            op_name,
            OpInfo {
                members,
                validation_code,
            },
        );
    }
    out
}

fn build_members(
    shapes: &serde_json::Map<String, Value>,
    input_shape: &Value,
) -> Vec<MemberConstraint> {
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
                apply_shape_constraints(shapes, target_id, &mut c);
            }
            // Length/range/pattern can also be reasserted on the member.
            if let Some(t) = member_traits {
                apply_traits(t, &mut c);
            }
            members.push(c);
        }
    }
    members
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
    if let Some(items) = t.get("smithy.api#enum").and_then(Value::as_array) {
        let vals: Vec<String> = items
            .iter()
            .filter_map(|i| i.get("value").and_then(Value::as_str).map(String::from))
            .collect();
        if !vals.is_empty() {
            c.enum_values = Some(vals);
        }
    }
    if let Some(p) = t.get("smithy.api#pattern").and_then(Value::as_str) {
        // Only patterns the Rust regex engine accepts are enforced; a pattern
        // it can't compile is skipped rather than panicking at startup.
        if c.pattern.is_none() {
            if let Ok(re) = Regex::new(p) {
                c.pattern = Some(re);
            }
        }
    }
}

/// The wire error code a validation failure for `op` should be surfaced as,
/// resolved from the operation's declared client errors.
pub fn validation_error_code(op: &str) -> &'static str {
    match ops().get(op).map(|o| o.validation_code.as_str()) {
        Some("InvalidParameterValueException") => "InvalidParameterValueException",
        Some("InvalidFilterException") => "InvalidFilterException",
        Some("UnsupportedDisplayLanguageCodeException") => {
            "UnsupportedDisplayLanguageCodeException"
        }
        _ => "InvalidRequestException",
    }
}

/// Validate a request body against the operation's input-shape constraints.
/// Returns `Err(message)` for the first violation, or `Ok(())` when every
/// top-level member is in bounds.
pub fn validate_input(op: &str, body: &Value) -> Result<(), String> {
    let Some(info) = ops().get(op) else {
        return Ok(());
    };
    for c in &info.members {
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
                            return Err(format!(
                                "1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must satisfy enum value set: {:?}",
                                s, c.name, vals
                            ));
                        }
                    }
                    if let Some(re) = &c.pattern {
                        if !re.is_match(s) {
                            return Err(format!(
                                "1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must satisfy regular expression pattern: {}",
                                s, c.name, re.as_str()
                            ));
                        }
                    }
                }
                if let Some(arr) = v.as_array() {
                    let len = arr.len() as u64;
                    if let Some(min) = c.min_len {
                        if len < min {
                            return Err(len_list_msg(&c.name, "greater than or equal to", min));
                        }
                    }
                    if let Some(max) = c.max_len {
                        if len > max {
                            return Err(len_list_msg(&c.name, "less than or equal to", max));
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

fn len_msg(name: &str, val: &str, cmp: &str, bound: u64) -> String {
    format!(
        "1 validation error detected: Value '{val}' at '{name}' failed to satisfy constraint: Member must have length {cmp} {bound}"
    )
}

fn len_list_msg(name: &str, cmp: &str, bound: u64) -> String {
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
        let err = validate_input("DescribeTextTranslationJob", &json!({})).unwrap_err();
        assert!(err.contains("JobId"));
    }

    #[test]
    fn valid_input_passes() {
        assert!(
            validate_input("DescribeTextTranslationJob", &json!({ "JobId": "abc123" })).is_ok()
        );
    }

    #[test]
    fn bad_enum_is_rejected() {
        let err = validate_input(
            "ImportTerminology",
            &json!({ "Name": "t", "MergeStrategy": "NOPE", "TerminologyData": {} }),
        )
        .unwrap_err();
        assert!(err.contains("MergeStrategy"));
    }

    #[test]
    fn bad_pattern_is_rejected() {
        // A name with a space violates the ResourceName pattern.
        let err = validate_input("DeleteTerminology", &json!({ "Name": "has space" })).unwrap_err();
        assert!(err.contains("regular expression"));
    }

    #[test]
    fn validation_code_is_per_operation() {
        // Translate ops carry InvalidParameterValueException.
        assert_eq!(
            validation_error_code("CreateParallelData"),
            "InvalidParameterValueException"
        );
        // TranslateText declares only InvalidRequestException among validation
        // errors (no InvalidParameterValueException).
        assert_eq!(
            validation_error_code("TranslateText"),
            "InvalidRequestException"
        );
        // ListTextTranslationJobs uses InvalidFilterException for bad filters,
        // but InvalidRequestException ranks... actually it prefers the first
        // in preference order it declares.
        assert_eq!(
            validation_error_code("ListTextTranslationJobs"),
            "InvalidRequestException"
        );
    }

    #[test]
    fn all_operations_are_represented() {
        assert!(ops().contains_key("TranslateText"));
        assert!(ops().contains_key("ImportTerminology"));
        assert_eq!(ops().len(), 19);
    }
}
