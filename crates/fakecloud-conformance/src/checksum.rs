use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashSet};

use crate::smithy::{self, ServiceModel, ShapeType};

/// Compute a deterministic checksum for an operation's full shape signature.
/// This includes the input shape, output shape, and error shapes, recursively
/// resolved to capture all referenced sub-shapes.
///
/// Returns the first 8 hex characters of a SHA-256 hash.
pub fn operation_checksum(model: &ServiceModel, operation_name: &str) -> Option<String> {
    let op = model.operations.iter().find(|o| o.name == operation_name)?;

    let mut collected_shapes = BTreeMap::new();
    let mut visited = HashSet::new();

    // Collect input shape tree
    if let Some(ref input_id) = op.input_shape {
        collect_shape_tree(model, input_id, &mut collected_shapes, &mut visited);
    }
    // Collect output shape tree
    if let Some(ref output_id) = op.output_shape {
        collect_shape_tree(model, output_id, &mut collected_shapes, &mut visited);
    }
    // Collect error shape trees
    for error_id in &op.error_shapes {
        collect_shape_tree(model, error_id, &mut collected_shapes, &mut visited);
    }

    // Build canonical representation
    let canonical = build_canonical(&op.name, op, &collected_shapes);

    // Hash
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let result = hasher.finalize();
    Some(hex::encode(&result[..4]))
}

/// Recursively collect all shapes referenced by a shape ID.
fn collect_shape_tree(
    model: &ServiceModel,
    shape_id: &str,
    collected: &mut BTreeMap<String, ShapeCanonical>,
    visited: &mut HashSet<String>,
) {
    if visited.contains(shape_id) {
        return;
    }
    visited.insert(shape_id.to_string());

    // Handle prelude types
    if smithy::is_prelude_shape(shape_id) {
        collected.insert(
            shape_id.to_string(),
            ShapeCanonical {
                shape_type: smithy::short_name(shape_id).to_string(),
                members: BTreeMap::new(),
                constraints: String::new(),
            },
        );
        return;
    }

    let shape = match model.shapes.get(shape_id) {
        Some(s) => s,
        None => return,
    };

    let mut canonical = ShapeCanonical {
        shape_type: shape_type_name(&shape.shape_type),
        members: BTreeMap::new(),
        constraints: format_constraints(&shape.traits),
    };

    match &shape.shape_type {
        ShapeType::Structure { members } | ShapeType::Union { members } => {
            for member in members {
                canonical.members.insert(
                    member.name.clone(),
                    MemberCanonical {
                        target: member.target.clone(),
                        required: member.required,
                    },
                );
                collect_shape_tree(model, &member.target, collected, visited);
            }
        }
        ShapeType::List { member_target } => {
            collect_shape_tree(model, member_target, collected, visited);
        }
        ShapeType::Map {
            key_target,
            value_target,
        } => {
            collect_shape_tree(model, key_target, collected, visited);
            collect_shape_tree(model, value_target, collected, visited);
        }
        ShapeType::String {
            enum_values: Some(values),
        } => {
            let enum_constraint = format!(
                "enum:{}",
                values
                    .iter()
                    .map(|v| v.value.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            if canonical.constraints.is_empty() {
                canonical.constraints = enum_constraint;
            } else {
                canonical.constraints = format!("{};{}", canonical.constraints, enum_constraint);
            }
        }
        ShapeType::String { enum_values: None } => {}
        ShapeType::Enum { values } => {
            let enum_constraint = format!(
                "enum:{}",
                values
                    .iter()
                    .map(|v| v.value.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            if canonical.constraints.is_empty() {
                canonical.constraints = enum_constraint;
            } else {
                canonical.constraints = format!("{};{}", canonical.constraints, enum_constraint);
            }
        }
        _ => {}
    }

    collected.insert(shape_id.to_string(), canonical);
}

#[derive(Debug)]
struct ShapeCanonical {
    shape_type: String,
    members: BTreeMap<String, MemberCanonical>,
    constraints: String,
}

#[derive(Debug)]
struct MemberCanonical {
    target: String,
    required: bool,
}

fn build_canonical(
    op_name: &str,
    op: &crate::smithy::Operation,
    shapes: &BTreeMap<String, ShapeCanonical>,
) -> String {
    let mut parts = Vec::new();
    parts.push(format!("op:{}", op_name));

    if let Some(ref input) = op.input_shape {
        parts.push(format!("in:{}", input));
    }
    if let Some(ref output) = op.output_shape {
        parts.push(format!("out:{}", output));
    }
    for error in &op.error_shapes {
        parts.push(format!("err:{}", error));
    }

    // Add all shapes in deterministic order
    for (id, shape) in shapes {
        let mut shape_str = format!("shape:{}:type:{}", id, shape.shape_type);
        if !shape.constraints.is_empty() {
            shape_str.push_str(&format!(":constraints:{}", shape.constraints));
        }
        for (name, member) in &shape.members {
            shape_str.push_str(&format!(
                ":member:{}:{}:req:{}",
                name, member.target, member.required
            ));
        }
        parts.push(shape_str);
    }

    parts.join("\n")
}

fn shape_type_name(st: &ShapeType) -> String {
    match st {
        ShapeType::Structure { .. } => "structure".to_string(),
        ShapeType::List { member_target } => format!("list<{}>", member_target),
        ShapeType::Map {
            key_target,
            value_target,
        } => format!("map<{},{}>", key_target, value_target),
        ShapeType::Union { .. } => "union".to_string(),
        ShapeType::String { .. } => "string".to_string(),
        ShapeType::Enum { .. } => "enum".to_string(),
        ShapeType::IntEnum { .. } => "intEnum".to_string(),
        ShapeType::Integer => "integer".to_string(),
        ShapeType::Long => "long".to_string(),
        ShapeType::Float => "float".to_string(),
        ShapeType::Double => "double".to_string(),
        ShapeType::Boolean => "boolean".to_string(),
        ShapeType::Blob => "blob".to_string(),
        ShapeType::Timestamp => "timestamp".to_string(),
        ShapeType::Document => "document".to_string(),
        ShapeType::Service => "service".to_string(),
        ShapeType::Operation => "operation".to_string(),
        ShapeType::Resource => "resource".to_string(),
    }
}

fn format_constraints(traits: &crate::smithy::ShapeTraits) -> String {
    let mut parts = Vec::new();
    if let Some(min) = traits.length_min {
        parts.push(format!("len_min:{}", min));
    }
    if let Some(max) = traits.length_max {
        parts.push(format!("len_max:{}", max));
    }
    if let Some(min) = traits.range_min {
        parts.push(format!("range_min:{}", min));
    }
    if let Some(max) = traits.range_max {
        parts.push(format!("range_max:{}", max));
    }
    if let Some(ref pat) = traits.pattern {
        parts.push(format!("pattern:{}", pat));
    }
    parts.join(";")
}

/// Helper to encode bytes as hex (avoid adding a `hex` crate dependency).
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn models_dir() -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("aws-models")
    }

    #[test]
    fn checksum_is_deterministic() {
        let model = crate::smithy::parse_model(&models_dir().join("sqs.json")).unwrap();
        let c1 = operation_checksum(&model, "CreateQueue").unwrap();
        let c2 = operation_checksum(&model, "CreateQueue").unwrap();
        assert_eq!(c1, c2);
        assert_eq!(c1.len(), 8);
    }

    #[test]
    fn different_operations_have_different_checksums() {
        let model = crate::smithy::parse_model(&models_dir().join("sqs.json")).unwrap();
        let c1 = operation_checksum(&model, "CreateQueue").unwrap();
        let c2 = operation_checksum(&model, "SendMessage").unwrap();
        assert_ne!(c1, c2);
    }
}
