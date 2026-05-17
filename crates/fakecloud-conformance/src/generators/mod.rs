pub mod boundary;
pub mod enum_exhaust;
pub mod examples;
pub mod examples_diff;
pub mod id_forms;
pub mod negative;
pub mod optionals;
pub mod proptest_gen;
pub mod round_trip;

use serde_json::Value;
use std::collections::HashMap;

use crate::smithy::{self, Member, ServiceModel, Shape, ShapeType};

/// A generated test variant for an operation.
#[derive(Debug, Clone)]
pub struct TestVariant {
    /// Human-readable name for this variant (e.g., "required_only", "boundary_min_QueueName").
    pub name: String,
    /// The strategy that generated this variant.
    pub strategy: Strategy,
    /// The JSON body to send as the request.
    pub input: Value,
    /// Whether this variant is expected to succeed or return a specific error.
    pub expectation: Expectation,
    /// Documented response body from the operation's `@examples` trait.
    /// When `Some`, the harness deep-diffs the live response against this
    /// value: every leaf field present here must also be present (with
    /// matching JSON type) in the actual response. Catches "optional in
    /// Smithy but AWS always emits" bugs (see #816).
    pub expected_output: Option<Value>,
    /// Optional second-step probe instructions (e.g. follow Create with Get
    /// and assert each input field echoes in the output). Set by the
    /// `round_trip` strategy. Catches silent-input-drop bugs (#853).
    pub followup: Option<RoundTripFollowup>,
}

/// Instructs the probe layer to fire a second request after the variant's
/// own Create/Put/Update succeeds, then verify that selected input fields
/// were preserved in the resource's GET response.
#[derive(Debug, Clone)]
pub struct RoundTripFollowup {
    /// Operation to invoke against fakecloud after the variant succeeds.
    pub get_operation: String,
    /// Member name shared between the writer's input and the reader's
    /// input. The probe pulls the identifier value out of the variant's
    /// own input by this name and re-uses it for the reader's input.
    pub id_field: String,
    /// All shared identifier members. Services with multi-segment URIs
    /// (e.g. API Gateway v1's
    /// `/restapis/{r}/resources/{x}/methods/{m}`) need the entire
    /// composite key in the follow-up Get — sending only `id_field`
    /// leaves the Get with a half-specified path and a guaranteed 404.
    pub id_fields: Vec<String>,
    /// (input_field, output_field) pairs that must echo verbatim from the
    /// Create input into the Get output.
    pub echo_fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    /// Strategy 1: Constraint-aware boundary values
    Boundary,
    /// Strategy 2: Enum exhaustion
    EnumExhaust,
    /// Strategy 3: Optionals permutation
    Optionals,
    /// Strategy 4: Property-based random value generation
    Proptest,
    /// Strategy 5: Real-world examples from model
    Examples,
    /// Strategy 6: Negative testing
    Negative,
    /// Strategy 7: Documented `@examples` output diff against live response
    ExamplesDiff,
    /// Strategy 8: Auto-discovered Create->Get round-trip echo
    RoundTrip,
    /// Strategy 9: Identifier-form fanout for httpLabel-bound members whose
    /// `@pattern` admits an ARN. Generates one variant per accepted form:
    /// bare name, `name:qualifier`, partial ARN, full ARN, URL-encoded
    /// full ARN. Catches "GetFunction returns 404 for ARN form" (#817).
    IdForms,
}

impl std::fmt::Display for Strategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Strategy::Boundary => write!(f, "boundary"),
            Strategy::EnumExhaust => write!(f, "enum_exhaust"),
            Strategy::Optionals => write!(f, "optionals"),
            Strategy::Proptest => write!(f, "proptest"),
            Strategy::Examples => write!(f, "examples"),
            Strategy::Negative => write!(f, "negative"),
            Strategy::ExamplesDiff => write!(f, "examples_diff"),
            Strategy::RoundTrip => write!(f, "round_trip"),
            Strategy::IdForms => write!(f, "id_forms"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum Expectation {
    /// Expect a successful response (2xx).
    Success,
    /// Expect an error response with a specific error code.
    Error(String),
    /// Expect any error (validation, etc.) but not a crash/500.
    AnyError,
}

/// Generate a default value for a shape, populating required fields recursively.
pub fn default_value_for_shape(model: &ServiceModel, shape_id: &str, depth: usize) -> Value {
    if depth > 10 {
        return Value::Null;
    }

    // Handle prelude types
    if smithy::is_prelude_shape(shape_id) {
        return default_for_prelude(shape_id);
    }

    let shape = match model.shapes.get(shape_id) {
        Some(s) => s,
        None => return default_for_prelude(shape_id),
    };

    default_value_for_shape_def(model, shape, depth)
}

fn default_value_for_shape_def(model: &ServiceModel, shape: &Shape, depth: usize) -> Value {
    match &shape.shape_type {
        ShapeType::Structure { members } => {
            let mut obj = serde_json::Map::new();
            for member in members {
                if member.required {
                    let val = default_value_for_shape(model, &member.target, depth + 1);
                    obj.insert(member.name.clone(), val);
                }
            }
            Value::Object(obj)
        }
        ShapeType::List { member_target } => {
            // Populate at least one element when the list carries a
            // `@length` min>=1 trait so that services which honour the
            // bound do not reject auto-built positive variants as a
            // ValidationException. Lists with min=0 (or no length trait)
            // still default to empty.
            let min = shape.traits.length_min.unwrap_or(0) as usize;
            if min == 0 {
                Value::Array(vec![])
            } else {
                let elem = default_value_for_shape(model, member_target, depth + 1);
                Value::Array(vec![elem])
            }
        }
        ShapeType::Map { .. } => Value::Object(serde_json::Map::new()),
        ShapeType::Union { members } => {
            // Use first member
            if let Some(first) = members.first() {
                let mut obj = serde_json::Map::new();
                let val = default_value_for_shape(model, &first.target, depth + 1);
                obj.insert(first.name.clone(), val);
                Value::Object(obj)
            } else {
                Value::Object(serde_json::Map::new())
            }
        }
        ShapeType::String { enum_values } => {
            if let Some(values) = enum_values {
                if let Some(first) = values.first() {
                    return Value::String(first.value.clone());
                }
            }
            // Use constraints if available
            let len = shape.traits.length_min.unwrap_or(1).max(1) as usize;
            Value::String("t".repeat(len.min(20)))
        }
        ShapeType::Enum { values } => {
            if let Some(first) = values.first() {
                Value::String(first.value.clone())
            } else {
                Value::String("test".to_string())
            }
        }
        ShapeType::IntEnum { values } => {
            if let Some(first) = values.first() {
                Value::Number(first.1.into())
            } else {
                Value::Number(0.into())
            }
        }
        ShapeType::Integer | ShapeType::Long => {
            let val = shape.traits.range_min.map(|v| v as i64).unwrap_or(1);
            Value::Number(val.into())
        }
        ShapeType::Float | ShapeType::Double => {
            let val = shape.traits.range_min.unwrap_or(1.0);
            Value::Number(serde_json::Number::from_f64(val).unwrap_or(1.into()))
        }
        ShapeType::Boolean => Value::Bool(true),
        ShapeType::Blob => Value::String("dGVzdA==".to_string()), // base64("test")
        ShapeType::Timestamp => Value::String("2024-01-01T00:00:00Z".to_string()),
        ShapeType::Document => Value::Object(serde_json::Map::new()),
        _ => Value::Null,
    }
}

fn default_for_prelude(shape_id: &str) -> Value {
    match shape_id {
        "smithy.api#String" | "smithy.api#Document" => Value::String("test".to_string()),
        "smithy.api#Integer" | "smithy.api#Short" | "smithy.api#Byte" => Value::Number(1.into()),
        "smithy.api#Long" | "smithy.api#BigInteger" => Value::Number(1.into()),
        "smithy.api#Float" | "smithy.api#Double" | "smithy.api#BigDecimal" => {
            Value::Number(serde_json::Number::from_f64(1.0).unwrap())
        }
        "smithy.api#Boolean" | "smithy.api#PrimitiveBoolean" => Value::Bool(true),
        "smithy.api#Blob" => Value::String("dGVzdA==".to_string()),
        "smithy.api#Timestamp" => Value::String("2024-01-01T00:00:00Z".to_string()),
        "smithy.api#PrimitiveInteger"
        | "smithy.api#PrimitiveShort"
        | "smithy.api#PrimitiveByte" => Value::Number(1.into()),
        "smithy.api#PrimitiveLong" => Value::Number(1.into()),
        "smithy.api#PrimitiveFloat" | "smithy.api#PrimitiveDouble" => {
            Value::Number(serde_json::Number::from_f64(1.0).unwrap())
        }
        "smithy.api#Unit" => Value::Object(serde_json::Map::new()),
        _ => Value::String("test".to_string()),
    }
}

/// Build a base input object with all required fields populated.
pub fn build_required_input(
    model: &ServiceModel,
    input_shape_id: &str,
    overrides: &HashMap<String, Value>,
) -> Value {
    let shape = match model.shapes.get(input_shape_id) {
        Some(s) => s,
        None => return Value::Object(serde_json::Map::new()),
    };

    match &shape.shape_type {
        ShapeType::Structure { members } => {
            let mut obj = serde_json::Map::new();
            for member in members {
                if member.required {
                    if let Some(override_val) = overrides.get(&member.name) {
                        obj.insert(member.name.clone(), override_val.clone());
                    } else {
                        let val = default_value_for_shape(model, &member.target, 0);
                        obj.insert(member.name.clone(), val);
                    }
                }
            }
            Value::Object(obj)
        }
        _ => Value::Object(serde_json::Map::new()),
    }
}

/// Build a full input object with all fields (required + optional) populated.
pub fn build_full_input(
    model: &ServiceModel,
    input_shape_id: &str,
    overrides: &HashMap<String, Value>,
) -> Value {
    let shape = match model.shapes.get(input_shape_id) {
        Some(s) => s,
        None => return Value::Object(serde_json::Map::new()),
    };

    match &shape.shape_type {
        ShapeType::Structure { members } => {
            let mut obj = serde_json::Map::new();
            for member in members {
                if let Some(override_val) = overrides.get(&member.name) {
                    obj.insert(member.name.clone(), override_val.clone());
                } else {
                    let val = default_value_for_shape(model, &member.target, 0);
                    obj.insert(member.name.clone(), val);
                }
            }
            Value::Object(obj)
        }
        _ => Value::Object(serde_json::Map::new()),
    }
}

/// Get the members of a structure shape.
pub fn get_members<'a>(model: &'a ServiceModel, shape_id: &str) -> &'a [Member] {
    model
        .shapes
        .get(shape_id)
        .map(|s| match &s.shape_type {
            ShapeType::Structure { members } => members.as_slice(),
            _ => &[],
        })
        .unwrap_or(&[])
}

/// Generate all test variants for an operation across all strategies.
pub fn generate_all_variants(
    model: &ServiceModel,
    operation_name: &str,
    overrides: &HashMap<String, Value>,
) -> Vec<TestVariant> {
    let op = match model.operations.iter().find(|o| o.name == operation_name) {
        Some(o) => o,
        None => return Vec::new(),
    };

    let input_shape_id = match &op.input_shape {
        Some(id) => id.as_str(),
        None => {
            return vec![TestVariant {
                name: "no_input".to_string(),
                strategy: Strategy::Optionals,
                input: Value::Object(serde_json::Map::new()),
                expectation: Expectation::Success,
                expected_output: None,
                followup: None,
            }]
        }
    };

    let mut variants = Vec::new();

    // Strategy 1: Boundary values
    variants.extend(boundary::generate(model, input_shape_id, overrides));

    // Strategy 2: Enum exhaustion
    variants.extend(enum_exhaust::generate(model, input_shape_id, overrides));

    // Strategy 3: Optionals permutation
    variants.extend(optionals::generate(model, input_shape_id, overrides));

    // Strategy 4: Property-based random value generation (20 variants)
    variants.extend(proptest_gen::generate(model, input_shape_id, overrides, 20));

    // Strategy 5 + 7: Examples from model. Both strategies want the same
    // `ShapeTraits` (operation-level `@examples`); resolve once.
    let op_traits = find_operation_traits(model, operation_name);
    if let Some(traits) = op_traits {
        variants.extend(examples::generate(traits));
        variants.extend(examples_diff::generate(traits));
    }

    // Strategy 6: Negative testing
    variants.extend(negative::generate(model, input_shape_id, overrides));

    // Strategy 8: auto-discovered Create/Put/Update -> Get/Describe round-trip echo
    variants.extend(round_trip::generate(model, operation_name, overrides));

    // Strategy 9: Identifier-form fanout for httpLabel-bound members whose
    // `@pattern` admits an ARN. Catches #817-class wire-form 404s.
    variants.extend(id_forms::generate(model, operation_name, overrides));

    variants
}

/// Locate the `ShapeTraits` for an operation, looking up by canonical shape
/// ID first, then falling back to a scan. Used by the `examples` and
/// `examples_diff` strategies, which both consume operation-level traits.
fn find_operation_traits<'a>(
    model: &'a ServiceModel,
    operation_name: &str,
) -> Option<&'a crate::smithy::ShapeTraits> {
    let canonical_id = format!("{}#{}", model.service_name, operation_name);
    if let Some(op_shape) = model.shapes.get(&canonical_id) {
        return Some(&op_shape.traits);
    }
    let suffix = format!("#{}", operation_name);
    for (shape_id, shape) in &model.shapes {
        if shape_id.ends_with(&suffix) && matches!(shape.shape_type, ShapeType::Operation) {
            return Some(&shape.traits);
        }
    }
    None
}
