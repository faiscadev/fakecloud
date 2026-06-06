//! Strategy 6: Negative testing.
//!
//! For each action:
//! - Omit each required field one at a time → expect validation error
//! - Out-of-range values for constrained fields → expect error
//! - Invalid enum values → expect error

use serde_json::Value;
use std::collections::HashMap;

use super::{build_required_input, default_value_for_shape, Expectation, Strategy, TestVariant};
use crate::smithy::{is_prelude_shape, ServiceModel, ShapeType};

/// `negative_omit_<field>` for required list/map members is wire-equivalent
/// to submitting the field with an empty collection: AWS query and REST
/// protocols serialise empty arrays/maps as zero wire params. The server
/// can't tell the difference, so the negative case can't observably fail.
/// Skip emitting the variant rather than asking the server to do something
/// impossible.
///
/// The same is true for a required *structure* whose members are all optional:
/// an empty struct serialises to zero wire params, indistinguishable from
/// omission. (A struct with at least one required member IS observable — the
/// required sub-fields appear on the wire when present, vanish when omitted.)
fn omission_is_wire_observable(model: &ServiceModel, member_target: &str) -> bool {
    if is_prelude_shape(member_target) {
        return true;
    }
    match model.shapes.get(member_target) {
        Some(shape) => match &shape.shape_type {
            ShapeType::List { .. } | ShapeType::Map { .. } => false,
            ShapeType::Structure { members } | ShapeType::Union { members } => {
                members.iter().any(|m| m.required)
            }
            _ => true,
        },
        None => true,
    }
}

pub fn generate(
    model: &ServiceModel,
    input_shape_id: &str,
    overrides: &HashMap<String, Value>,
) -> Vec<TestVariant> {
    let mut variants = Vec::new();

    let members = super::get_members(model, input_shape_id);

    // Omit each required field one at a time
    let required_members: Vec<_> = members.iter().filter(|m| m.required).collect();
    for omit_member in &required_members {
        if !omission_is_wire_observable(model, &omit_member.target) {
            continue;
        }
        let mut obj = serde_json::Map::new();
        for member in &required_members {
            if member.name == omit_member.name {
                continue; // Skip this one
            }
            if let Some(override_val) = overrides.get(&member.name) {
                obj.insert(member.name.clone(), override_val.clone());
            } else {
                let val = default_value_for_shape(model, &member.target, 0);
                obj.insert(member.name.clone(), val);
            }
        }
        variants.push(TestVariant {
            name: format!("negative_omit_{}", omit_member.name),
            strategy: Strategy::Negative,
            input: Value::Object(obj),
            expectation: Expectation::AnyError,
            expected_output: None,
            followup: None,
        });
    }

    // Out-of-range values for constrained fields
    for member in members {
        let shape = match model.shapes.get(&member.target) {
            Some(s) => s,
            None => continue,
        };

        let traits = &shape.traits;

        // String too short (below min length).
        //
        // `default_value_for_shape` caps positive-variant string fillers at
        // 20 chars. When `length_min` exceeds 20 the positive default is
        // already shorter than `min - 1`, so the server can't validate the
        // negative case without also rejecting every legitimate positive
        // call. Skip the variant rather than emit one that's unreachable
        // by construction.
        if let Some(min) = traits.length_min {
            if min > 0 && (min as usize) <= 20 {
                if let ShapeType::String { .. } = &shape.shape_type {
                    let mut input = build_required_input(model, input_shape_id, overrides);
                    if let Value::Object(ref mut obj) = input {
                        // Use a string shorter than min
                        let short = "a".repeat((min as usize).saturating_sub(1));
                        obj.insert(member.name.clone(), Value::String(short));
                    }
                    variants.push(TestVariant {
                        name: format!("negative_too_short_{}", member.name),
                        strategy: Strategy::Negative,
                        input,
                        expectation: Expectation::AnyError,
                        expected_output: None,
                        followup: None,
                    });
                }
            }
        }

        // String too long (above max length)
        if let Some(max) = traits.length_max {
            if max < 100000 {
                // Don't generate absurdly large strings
                if let ShapeType::String { .. } = &shape.shape_type {
                    let mut input = build_required_input(model, input_shape_id, overrides);
                    if let Value::Object(ref mut obj) = input {
                        let long = "a".repeat(max as usize + 1);
                        obj.insert(member.name.clone(), Value::String(long));
                    }
                    variants.push(TestVariant {
                        name: format!("negative_too_long_{}", member.name),
                        strategy: Strategy::Negative,
                        input,
                        expectation: Expectation::AnyError,
                        expected_output: None,
                        followup: None,
                    });
                }
            }
        }

        // Integer below min range
        if let Some(min) = traits.range_min {
            match &shape.shape_type {
                ShapeType::Integer | ShapeType::Long => {
                    if let Some(below) = (min as i64).checked_sub(1) {
                        let mut input = build_required_input(model, input_shape_id, overrides);
                        if let Value::Object(ref mut obj) = input {
                            obj.insert(member.name.clone(), Value::Number(below.into()));
                        }
                        variants.push(TestVariant {
                            name: format!("negative_below_min_{}", member.name),
                            strategy: Strategy::Negative,
                            input,
                            expectation: Expectation::AnyError,
                            expected_output: None,
                            followup: None,
                        });
                    }
                }
                _ => {}
            }
        }

        // Integer above max range
        if let Some(max) = traits.range_max {
            match &shape.shape_type {
                ShapeType::Integer | ShapeType::Long => {
                    if let Some(above) = (max as i64).checked_add(1) {
                        let mut input = build_required_input(model, input_shape_id, overrides);
                        if let Value::Object(ref mut obj) = input {
                            obj.insert(member.name.clone(), Value::Number(above.into()));
                        }
                        variants.push(TestVariant {
                            name: format!("negative_above_max_{}", member.name),
                            strategy: Strategy::Negative,
                            input,
                            expectation: Expectation::AnyError,
                            expected_output: None,
                            followup: None,
                        });
                    }
                }
                _ => {}
            }
        }

        // Invalid enum value
        let has_enum = matches!(
            &shape.shape_type,
            ShapeType::String {
                enum_values: Some(_)
            } | ShapeType::Enum { .. }
        );
        if has_enum {
            let mut input = build_required_input(model, input_shape_id, overrides);
            if let Value::Object(ref mut obj) = input {
                obj.insert(
                    member.name.clone(),
                    Value::String("__INVALID_ENUM_VALUE__".to_string()),
                );
            }
            variants.push(TestVariant {
                name: format!("negative_invalid_enum_{}", member.name),
                strategy: Strategy::Negative,
                input,
                expectation: Expectation::AnyError,
                expected_output: None,
                followup: None,
            });
        }
    }

    variants
}
