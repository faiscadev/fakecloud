use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

/// A parsed Smithy service model.
#[derive(Debug)]
pub struct ServiceModel {
    pub service_name: String,
    pub operations: Vec<Operation>,
    pub shapes: HashMap<String, Shape>,
}

/// A parsed operation from the model.
#[derive(Debug, Clone)]
pub struct Operation {
    pub name: String,
    pub input_shape: Option<String>,
    pub output_shape: Option<String>,
    pub error_shapes: Vec<String>,
}

/// A parsed shape definition.
#[derive(Debug, Clone)]
pub struct Shape {
    pub shape_id: String,
    pub shape_type: ShapeType,
    pub traits: ShapeTraits,
}

#[derive(Debug, Clone)]
pub enum ShapeType {
    /// A structure with named members.
    Structure {
        members: Vec<Member>,
    },
    /// A list with a member type.
    List {
        member_target: String,
    },
    /// A map with key and value types.
    Map {
        key_target: String,
        value_target: String,
    },
    /// A union (tagged union / oneOf).
    Union {
        members: Vec<Member>,
    },
    /// A string, optionally an enum.
    String {
        enum_values: Option<Vec<EnumValue>>,
    },
    /// An enum defined via the `enum` shape type (Smithy 2.0).
    Enum {
        values: Vec<EnumValue>,
    },
    /// An integer enum.
    IntEnum {
        values: Vec<(String, i64)>,
    },
    Integer,
    Long,
    Float,
    Double,
    Boolean,
    Blob,
    Timestamp,
    /// Service, operation, resource — not directly useful for value generation.
    Service,
    Operation,
    Resource,
}

#[derive(Debug, Clone)]
pub struct Member {
    pub name: String,
    pub target: String,
    pub required: bool,
    pub traits: ShapeTraits,
}

#[derive(Debug, Clone)]
pub struct EnumValue {
    pub name: String,
    pub value: String,
}

/// Traits extracted from a shape or member that are relevant for conformance testing.
#[derive(Debug, Clone, Default)]
pub struct ShapeTraits {
    pub documentation: Option<String>,
    pub length_min: Option<u64>,
    pub length_max: Option<u64>,
    pub range_min: Option<f64>,
    pub range_max: Option<f64>,
    pub pattern: Option<String>,
    pub deprecated: bool,
    pub sensitive: bool,
    pub error: Option<String>,
    pub http_error: Option<u16>,
    pub default_value: Option<Value>,
    pub examples: Vec<OperationExample>,
}

/// An example from `smithy.api#examples` trait on operations.
#[derive(Debug, Clone)]
pub struct OperationExample {
    pub title: String,
    pub input: Value,
    pub output: Value,
}

/// Parse a Smithy JSON AST model file.
pub fn parse_model(path: &Path) -> Result<ServiceModel, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
    let root: Value = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse {}: {}", path.display(), e))?;

    let smithy_version = root
        .get("smithy")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    if !smithy_version.starts_with("2.") {
        return Err(format!("Unsupported Smithy version: {}", smithy_version));
    }

    let raw_shapes = root
        .get("shapes")
        .and_then(|v| v.as_object())
        .ok_or("Missing 'shapes' in model")?;

    // Find the service shape and extract operations
    let mut service_name = String::new();
    let mut operation_targets: Vec<String> = Vec::new();

    for (shape_id, shape_def) in raw_shapes {
        if shape_def.get("type").and_then(|v| v.as_str()) == Some("service") {
            service_name = shape_id.split('#').next().unwrap_or(shape_id).to_string();
            if let Some(ops) = shape_def.get("operations").and_then(|v| v.as_array()) {
                for op in ops {
                    if let Some(target) = op.get("target").and_then(|v| v.as_str()) {
                        operation_targets.push(target.to_string());
                    }
                }
            }
            // Also collect operations from resources
            if let Some(resources) = shape_def.get("resources").and_then(|v| v.as_array()) {
                for res in resources {
                    if let Some(target) = res.get("target").and_then(|v| v.as_str()) {
                        collect_resource_operations(raw_shapes, target, &mut operation_targets);
                    }
                }
            }
            break;
        }
    }

    // Parse all shapes
    let mut shapes = HashMap::new();
    for (shape_id, shape_def) in raw_shapes {
        if let Some(shape) = parse_shape(shape_id, shape_def) {
            shapes.insert(shape_id.clone(), shape);
        }
    }

    // Build operation list
    let mut operations = Vec::new();
    for target in &operation_targets {
        if let Some(shape_def) = raw_shapes.get(target.as_str()) {
            let name = target.rsplit('#').next().unwrap_or(target).to_string();
            let input_shape = shape_def
                .get("input")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let output_shape = shape_def
                .get("output")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let error_shapes = shape_def
                .get("errors")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|e| {
                            e.get("target")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_string())
                        })
                        .collect()
                })
                .unwrap_or_default();

            let op = Operation {
                name,
                input_shape,
                output_shape,
                error_shapes,
            };

            // Extract examples from operation traits
            if let Some(traits) = shape_def.get("traits").and_then(|v| v.as_object()) {
                if let Some(examples) = traits.get("smithy.api#examples").and_then(|v| v.as_array())
                {
                    // Store examples on the operation (we'll reference them from shapes too)
                    let _ = examples; // examples are stored in the shape's traits
                }
            }

            operations.push(op);
        }
    }

    operations.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(ServiceModel {
        service_name,
        operations,
        shapes,
    })
}

fn collect_resource_operations(
    raw_shapes: &serde_json::Map<String, Value>,
    resource_target: &str,
    targets: &mut Vec<String>,
) {
    if let Some(resource_def) = raw_shapes.get(resource_target) {
        // Collect direct operations
        for key in &["create", "read", "update", "delete", "list", "put"] {
            if let Some(op) = resource_def
                .get(*key)
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
            {
                targets.push(op.to_string());
            }
        }
        if let Some(ops) = resource_def.get("operations").and_then(|v| v.as_array()) {
            for op in ops {
                if let Some(target) = op.get("target").and_then(|v| v.as_str()) {
                    targets.push(target.to_string());
                }
            }
        }
        if let Some(coll_ops) = resource_def
            .get("collectionOperations")
            .and_then(|v| v.as_array())
        {
            for op in coll_ops {
                if let Some(target) = op.get("target").and_then(|v| v.as_str()) {
                    targets.push(target.to_string());
                }
            }
        }
        // Recurse into sub-resources
        if let Some(resources) = resource_def.get("resources").and_then(|v| v.as_array()) {
            for res in resources {
                if let Some(target) = res.get("target").and_then(|v| v.as_str()) {
                    collect_resource_operations(raw_shapes, target, targets);
                }
            }
        }
    }
}

fn parse_shape(shape_id: &str, def: &Value) -> Option<Shape> {
    let type_str = def.get("type").and_then(|v| v.as_str())?;
    let raw_traits = def.get("traits").and_then(|v| v.as_object());
    let traits = parse_traits(raw_traits);

    let shape_type = match type_str {
        "structure" => {
            let members = parse_members(def);
            ShapeType::Structure { members }
        }
        "union" => {
            let members = parse_members(def);
            ShapeType::Union { members }
        }
        "list" => {
            let member_target = def
                .get("member")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String")
                .to_string();
            ShapeType::List { member_target }
        }
        "map" => {
            let key_target = def
                .get("key")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String")
                .to_string();
            let value_target = def
                .get("value")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String")
                .to_string();
            ShapeType::Map {
                key_target,
                value_target,
            }
        }
        "string" => {
            // Check for @enum trait (Smithy 1.0 style enum on string)
            let enum_values = raw_traits
                .and_then(|t| t.get("smithy.api#enum"))
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|e| {
                            let value = e.get("value").and_then(|v| v.as_str())?.to_string();
                            let name = e
                                .get("name")
                                .and_then(|v| v.as_str())
                                .unwrap_or(&value)
                                .to_string();
                            Some(EnumValue { name, value })
                        })
                        .collect()
                });
            ShapeType::String { enum_values }
        }
        "enum" => {
            // Smithy 2.0 enum shape
            let values = def
                .get("members")
                .and_then(|v| v.as_object())
                .map(|members| {
                    members
                        .iter()
                        .map(|(name, member_def)| {
                            let value = member_def
                                .get("traits")
                                .and_then(|t| t.get("smithy.api#enumValue"))
                                .and_then(|v| v.as_str())
                                .unwrap_or(name)
                                .to_string();
                            EnumValue {
                                name: name.clone(),
                                value,
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();
            ShapeType::Enum { values }
        }
        "intEnum" => {
            let values = def
                .get("members")
                .and_then(|v| v.as_object())
                .map(|members| {
                    members
                        .iter()
                        .filter_map(|(name, member_def)| {
                            let value = member_def
                                .get("traits")
                                .and_then(|t| t.get("smithy.api#enumValue"))
                                .and_then(|v| v.as_i64())?;
                            Some((name.clone(), value))
                        })
                        .collect()
                })
                .unwrap_or_default();
            ShapeType::IntEnum { values }
        }
        "integer" => ShapeType::Integer,
        "long" => ShapeType::Long,
        "float" => ShapeType::Float,
        "double" => ShapeType::Double,
        "boolean" => ShapeType::Boolean,
        "blob" => ShapeType::Blob,
        "timestamp" => ShapeType::Timestamp,
        "service" => ShapeType::Service,
        "operation" => ShapeType::Operation,
        "resource" => ShapeType::Resource,
        _ => return None,
    };

    Some(Shape {
        shape_id: shape_id.to_string(),
        shape_type,
        traits,
    })
}

fn parse_members(def: &Value) -> Vec<Member> {
    let members_obj = match def.get("members").and_then(|v| v.as_object()) {
        Some(m) => m,
        None => return Vec::new(),
    };

    members_obj
        .iter()
        .map(|(name, member_def)| {
            let target = member_def
                .get("target")
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String")
                .to_string();

            let member_traits = member_def.get("traits").and_then(|v| v.as_object());
            let required = member_traits
                .map(|t| t.contains_key("smithy.api#required"))
                .unwrap_or(false);
            let traits = parse_traits(member_traits);

            Member {
                name: name.clone(),
                target,
                required,
                traits,
            }
        })
        .collect()
}

fn parse_traits(raw: Option<&serde_json::Map<String, Value>>) -> ShapeTraits {
    let raw = match raw {
        Some(r) => r,
        None => return ShapeTraits::default(),
    };

    let mut traits = ShapeTraits::default();

    if let Some(doc) = raw.get("smithy.api#documentation").and_then(|v| v.as_str()) {
        traits.documentation = Some(doc.to_string());
    }

    if let Some(length) = raw.get("smithy.api#length") {
        traits.length_min = length.get("min").and_then(|v| v.as_u64());
        traits.length_max = length.get("max").and_then(|v| v.as_u64());
    }

    if let Some(range) = raw.get("smithy.api#range") {
        traits.range_min = range.get("min").and_then(|v| v.as_f64());
        traits.range_max = range.get("max").and_then(|v| v.as_f64());
    }

    if let Some(pattern) = raw.get("smithy.api#pattern").and_then(|v| v.as_str()) {
        traits.pattern = Some(pattern.to_string());
    }

    if raw.contains_key("smithy.api#deprecated") {
        traits.deprecated = true;
    }

    if raw.contains_key("smithy.api#sensitive") {
        traits.sensitive = true;
    }

    if let Some(error) = raw.get("smithy.api#error").and_then(|v| v.as_str()) {
        traits.error = Some(error.to_string());
    }

    if let Some(http_error) = raw.get("smithy.api#httpError").and_then(|v| v.as_u64()) {
        traits.http_error = Some(http_error as u16);
    }

    if let Some(default) = raw.get("smithy.api#default") {
        traits.default_value = Some(default.clone());
    }

    if let Some(examples) = raw.get("smithy.api#examples").and_then(|v| v.as_array()) {
        traits.examples = examples
            .iter()
            .filter_map(|ex| {
                let title = ex.get("title").and_then(|v| v.as_str())?.to_string();
                let input = ex
                    .get("input")
                    .cloned()
                    .unwrap_or(Value::Object(Default::default()));
                let output = ex
                    .get("output")
                    .cloned()
                    .unwrap_or(Value::Object(Default::default()));
                Some(OperationExample {
                    title,
                    input,
                    output,
                })
            })
            .collect();
    }

    traits
}

/// Resolve a shape ID to its short name (after the `#`).
pub fn short_name(shape_id: &str) -> &str {
    shape_id.rsplit('#').next().unwrap_or(shape_id)
}

/// Check if a shape ID is a Smithy prelude type (e.g., `smithy.api#String`).
pub fn is_prelude_shape(shape_id: &str) -> bool {
    shape_id.starts_with("smithy.api#")
}

/// Get the shape type for a Smithy prelude shape ID.
pub fn prelude_shape_type(shape_id: &str) -> Option<ShapeType> {
    match shape_id {
        "smithy.api#String" => Some(ShapeType::String { enum_values: None }),
        "smithy.api#Integer" => Some(ShapeType::Integer),
        "smithy.api#Long" => Some(ShapeType::Long),
        "smithy.api#Short" => Some(ShapeType::Integer),
        "smithy.api#Byte" => Some(ShapeType::Integer),
        "smithy.api#Float" => Some(ShapeType::Float),
        "smithy.api#Double" => Some(ShapeType::Double),
        "smithy.api#Boolean" => Some(ShapeType::Boolean),
        "smithy.api#Blob" => Some(ShapeType::Blob),
        "smithy.api#Timestamp" => Some(ShapeType::Timestamp),
        "smithy.api#BigInteger" => Some(ShapeType::Long),
        "smithy.api#BigDecimal" => Some(ShapeType::Double),
        "smithy.api#Document" => Some(ShapeType::String { enum_values: None }),
        "smithy.api#Unit" => Some(ShapeType::Structure {
            members: Vec::new(),
        }),
        "smithy.api#PrimitiveBoolean" => Some(ShapeType::Boolean),
        "smithy.api#PrimitiveInteger" => Some(ShapeType::Integer),
        "smithy.api#PrimitiveLong" => Some(ShapeType::Long),
        "smithy.api#PrimitiveFloat" => Some(ShapeType::Float),
        "smithy.api#PrimitiveDouble" => Some(ShapeType::Double),
        "smithy.api#PrimitiveShort" => Some(ShapeType::Integer),
        "smithy.api#PrimitiveByte" => Some(ShapeType::Integer),
        _ => None,
    }
}

/// Resolve a shape ID to its Shape, handling prelude types.
pub fn resolve_shape<'a>(model: &'a ServiceModel, shape_id: &str) -> Option<&'a Shape> {
    model.shapes.get(shape_id)
}

/// Get the effective shape type for a shape ID, handling prelude types.
pub fn effective_shape_type(model: &ServiceModel, shape_id: &str) -> Option<ShapeType> {
    if let Some(shape) = model.shapes.get(shape_id) {
        Some(shape.shape_type.clone())
    } else {
        prelude_shape_type(shape_id)
    }
}

/// Load the service map from service-map.json.
pub fn load_service_map(models_dir: &Path) -> Result<HashMap<String, ServiceMapEntry>, String> {
    let path = models_dir.join("service-map.json");
    let content = std::fs::read_to_string(&path)
        .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
    let map: HashMap<String, ServiceMapEntry> = serde_json::from_str(&content)
        .map_err(|e| format!("Failed to parse {}: {}", path.display(), e))?;
    Ok(map)
}

#[derive(Debug, Deserialize)]
pub struct ServiceMapEntry {
    pub repo_dir: String,
    pub service_name: String,
}

/// Load all service models from the aws-models directory.
pub fn load_all_models(models_dir: &Path) -> Result<Vec<(String, ServiceModel)>, String> {
    let service_map = load_service_map(models_dir)?;
    let mut models = Vec::new();

    for (model_key, entry) in &service_map {
        let model_path = models_dir.join(format!("{}.json", model_key));
        if !model_path.exists() {
            eprintln!(
                "Warning: Model file not found for {}: {}",
                model_key,
                model_path.display()
            );
            continue;
        }
        let model = parse_model(&model_path)?;
        models.push((entry.service_name.clone(), model));
    }

    models.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(models)
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
    fn parse_sqs_model() {
        let path = models_dir().join("sqs.json");
        let model = parse_model(&path).unwrap();

        assert!(!model.operations.is_empty());
        assert!(model.operations.iter().any(|op| op.name == "CreateQueue"));
        assert!(model.operations.iter().any(|op| op.name == "SendMessage"));
        assert!(model
            .operations
            .iter()
            .any(|op| op.name == "ReceiveMessage"));

        // Check that CreateQueue has input/output shapes
        let create_queue = model
            .operations
            .iter()
            .find(|op| op.name == "CreateQueue")
            .unwrap();
        assert!(create_queue.input_shape.is_some());
        assert!(create_queue.output_shape.is_some());
        assert!(!create_queue.error_shapes.is_empty());

        // Check input shape has members
        let input_id = create_queue.input_shape.as_ref().unwrap();
        let input_shape = model.shapes.get(input_id).unwrap();
        match &input_shape.shape_type {
            ShapeType::Structure { members } => {
                assert!(members.iter().any(|m| m.name == "QueueName" && m.required));
            }
            _ => panic!("Expected structure"),
        }
    }

    #[test]
    fn parse_dynamodb_model_with_constraints() {
        let path = models_dir().join("dynamodb.json");
        let model = parse_model(&path).unwrap();

        // DynamoDB should have operation shapes with examples
        let op_shapes_with_examples: Vec<_> = model
            .shapes
            .iter()
            .filter(|(_, s)| !s.traits.examples.is_empty())
            .collect();
        assert!(
            !op_shapes_with_examples.is_empty(),
            "DynamoDB should have operation examples"
        );

        // Check for length constraints
        let shapes_with_length: Vec<_> = model
            .shapes
            .iter()
            .filter(|(_, s)| s.traits.length_min.is_some() || s.traits.length_max.is_some())
            .collect();
        assert!(
            !shapes_with_length.is_empty(),
            "DynamoDB should have shapes with length constraints"
        );
    }

    #[test]
    fn load_all_models_works() {
        let dir = models_dir();
        let models = load_all_models(&dir).unwrap();
        assert!(models.len() >= 13, "Should load at least 13 service models");

        // Check SQS is present
        assert!(models.iter().any(|(name, _)| name == "sqs"));
    }
}
