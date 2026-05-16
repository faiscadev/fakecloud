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
    /// HTTP method from `smithy.api#http` if present (`GET`, `POST`, …).
    pub http_method: Option<String>,
    /// URI template from `smithy.api#http`, e.g. `/v2/apis/{ApiId}/routes/{RouteId}`.
    pub http_uri: Option<String>,
    /// Success HTTP status code from `smithy.api#http` (`code`).
    pub http_code: Option<u16>,
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
    /// `smithy.api#Document` — an arbitrary JSON value (object, array,
    /// string, number, bool, null). Used by Bedrock `ToolUseBlock.input`
    /// and similar dynamic payloads.
    Document,
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
    /// `smithy.api#httpLabel` — member value substitutes into the URI template.
    pub http_label: bool,
    /// `smithy.api#httpQuery("name")` — query parameter name.
    pub http_query: Option<String>,
    /// `smithy.api#httpHeader("name")` — HTTP request header name.
    pub http_header: Option<String>,
    /// `smithy.api#httpPayload` — member is sent as the raw request/response body.
    pub http_payload: bool,
    /// `smithy.api#jsonName("name")` — override the key used when serializing
    /// this member in JSON bodies. AWS restJson1 services use this to map
    /// camelCase/kebab-case JSON keys to PascalCase Smithy member names.
    pub json_name: Option<String>,
    /// `aws.protocols#awsQueryError.code` — explicit wire code for awsQuery /
    /// awsQueryCompat services where the Smithy shape name differs from the
    /// `<Code>` value AWS actually returns on the wire. E.g. IAM declares the
    /// shape `NoSuchEntityException` but wires `__type: "NoSuchEntity"`; RDS
    /// declares `DBInstanceNotFoundFault` but wires `DBInstanceNotFound`. The
    /// strict matcher reads this when deriving the per-shape wire code so we
    /// don't need a suffix-stripping heuristic.
    pub aws_query_error_code: Option<String>,
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

    // Find the service shape and extract operations.
    //
    // Strict mode (post-#1342 revert): we deliberately do NOT collect the
    // service-shape `errors:` list and union it into every op. That was a
    // lenient acceptance hack; the strict probe matches only the op's own
    // directly-declared error shapes.
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
            let error_shapes: Vec<String> = shape_def
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

            let (http_method, http_uri, http_code) = shape_def
                .get("traits")
                .and_then(|v| v.as_object())
                .and_then(|t| t.get("smithy.api#http"))
                .map(|h| {
                    let method = h
                        .get("method")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let uri = h.get("uri").and_then(|v| v.as_str()).map(|s| s.to_string());
                    let code = h.get("code").and_then(|v| v.as_u64()).map(|c| c as u16);
                    (method, uri, code)
                })
                .unwrap_or((None, None, None));

            let op = Operation {
                name,
                input_shape,
                output_shape,
                error_shapes,
                http_method,
                http_uri,
                http_code,
            };

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
        "document" => ShapeType::Document,
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

    if raw.contains_key("smithy.api#httpLabel") {
        traits.http_label = true;
    }

    if let Some(name) = raw.get("smithy.api#httpQuery").and_then(|v| v.as_str()) {
        traits.http_query = Some(name.to_string());
    }

    if let Some(name) = raw.get("smithy.api#httpHeader").and_then(|v| v.as_str()) {
        traits.http_header = Some(name.to_string());
    }

    if raw.contains_key("smithy.api#httpPayload") {
        traits.http_payload = true;
    }

    if let Some(name) = raw.get("smithy.api#jsonName").and_then(|v| v.as_str()) {
        traits.json_name = Some(name.to_string());
    }

    if let Some(code) = raw
        .get("aws.protocols#awsQueryError")
        .and_then(|v| v.get("code"))
        .and_then(|v| v.as_str())
    {
        traits.aws_query_error_code = Some(code.to_string());
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
        "smithy.api#Document" => Some(ShapeType::Document),
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

/// A round-trip pair: a mutating op (`Create*`/`Put*`/`Update*`) and a
/// matching read op (`Get*`/`Describe*`) that retrieves the same resource,
/// linked by the identifier the user supplies on both inputs.
#[derive(Debug, Clone)]
pub struct RoundTripPair {
    /// The mutating operation (`Create*`/`Put*`/`Update*`).
    pub writer: Operation,
    /// The corresponding read operation (`Get*`/`Describe*`).
    pub reader: Operation,
    /// Member name shared between writer's *input* and reader's *input* —
    /// the resource identifier. The same value flows into both requests
    /// without the round-trip strategy needing to parse the writer's
    /// response.
    pub id_source_field: String,
}

/// Discover Create/Put/Update -> Get/Describe pairs by walking the service
/// model. The match is structural with a name hint:
///
/// 1. The writer's name starts with `Create`, `Put`, or `Update`.
/// 2. The reader's name starts with `Get` or `Describe` AND its remaining
///    suffix overlaps with the writer's suffix (the noun the writer
///    creates appears in the reader's name — e.g. `CreateFunction` <->
///    `GetFunctionConfiguration`).
/// 3. There exists at least one structure member that appears on both
///    writer.input AND reader.input by the same name and primitive-
///    compatible target shape — this is the resource identifier the
///    user supplies on both calls.
///
/// When multiple readers qualify, prefer the one whose suffix is shortest
/// (closest to the bare resource — `GetFunction` over
/// `GetFunctionConfiguration` when both exist). Pairs that fail #3 are
/// skipped: they typically need cross-resource state (e.g. `CreateAlias`
/// needs a function) and the strategy can't drive them from schema alone.
pub fn find_round_trip_pairs(model: &ServiceModel) -> Vec<RoundTripPair> {
    let mut pairs = Vec::new();
    for writer in &model.operations {
        let writer_suffix = match strip_writer_prefix(&writer.name) {
            Some(s) => s,
            None => continue,
        };
        let writer_input_members =
            structure_members_for(model, writer.input_shape.as_deref().unwrap_or(""));
        if writer_input_members.is_empty() {
            continue;
        }

        let mut candidates: Vec<(&Operation, &str)> = Vec::new();
        for reader in &model.operations {
            let reader_suffix = match strip_reader_prefix(&reader.name) {
                Some(s) => s,
                None => continue,
            };
            let name_match = reader_suffix.starts_with(writer_suffix)
                || writer_suffix.starts_with(reader_suffix);
            if !name_match {
                continue;
            }
            candidates.push((reader, reader_suffix));
        }
        // Prefer an exact-suffix match (writer_suffix == reader_suffix) over
        // a partial one. Without this, `PutIntegrationResponse` (writer
        // suffix `IntegrationResponse`) pairs with `GetIntegration` (reader
        // suffix `Integration`) because the latter has a shorter suffix
        // and `IntegrationResponse.starts_with("Integration")` is true.
        // Sort exact-matches first, then ascending suffix length, so the
        // most specific reader wins.
        candidates.sort_by_key(|(_, suf)| (*suf != writer_suffix, suf.len()));

        for (reader, _) in candidates {
            let reader_input_members =
                structure_members_for(model, reader.input_shape.as_deref().unwrap_or(""));
            let binding = writer_input_members.iter().find(|wm| {
                reader_input_members.iter().any(|rm| {
                    rm.name == wm.name && primitive_compatible(model, &rm.target, &wm.target)
                })
            });
            if let Some(bind) = binding {
                pairs.push(RoundTripPair {
                    writer: writer.clone(),
                    reader: reader.clone(),
                    id_source_field: bind.name.clone(),
                });
                break;
            }
        }
    }
    pairs
}

fn strip_writer_prefix(name: &str) -> Option<&str> {
    for prefix in ["Create", "Put", "Update"] {
        if let Some(suffix) = name.strip_prefix(prefix) {
            if !suffix.is_empty() {
                return Some(suffix);
            }
        }
    }
    None
}

fn strip_reader_prefix(name: &str) -> Option<&str> {
    for prefix in ["Get", "Describe"] {
        if let Some(suffix) = name.strip_prefix(prefix) {
            if !suffix.is_empty() {
                return Some(suffix);
            }
        }
    }
    None
}

fn structure_members_for(model: &ServiceModel, shape_id: &str) -> Vec<Member> {
    if shape_id.is_empty() {
        return Vec::new();
    }
    match effective_shape_type(model, shape_id) {
        Some(ShapeType::Structure { members }) => members,
        _ => Vec::new(),
    }
}

/// Two shapes are "primitive compatible" if they resolve to the same
/// JSON-level type bucket. Used to bind identifier members across writer
/// and reader inputs even when the shape IDs differ — Lambda's
/// `CreateFunctionRequest.FunctionName` (`FunctionName` shape) and
/// `GetFunctionRequest.FunctionName` (`NamespacedFunctionName` shape)
/// are both strings, so the round-trip identifier round-trips fine even
/// though Smithy declares them as distinct typedefs.
pub fn primitive_compatible(model: &ServiceModel, a: &str, b: &str) -> bool {
    fn bucket(t: &ShapeType) -> u8 {
        match t {
            ShapeType::String { .. } | ShapeType::Enum { .. } => 1,
            ShapeType::Integer | ShapeType::Long | ShapeType::IntEnum { .. } => 2,
            ShapeType::Float | ShapeType::Double => 3,
            ShapeType::Boolean => 4,
            ShapeType::Blob => 5,
            ShapeType::Timestamp => 6,
            ShapeType::List { .. } => 7,
            ShapeType::Map { .. } => 8,
            ShapeType::Structure { .. } => 9,
            ShapeType::Union { .. } => 10,
            _ => 0,
        }
    }
    match (
        effective_shape_type(model, a),
        effective_shape_type(model, b),
    ) {
        (Some(ta), Some(tb)) => bucket(&ta) != 0 && bucket(&ta) == bucket(&tb),
        _ => false,
    }
}

/// Best-effort heuristic: does this `@pattern` regex admit ARN-form input?
///
/// AWS Smithy patterns that allow both bare names and ARNs typically
/// contain a literal `arn:` in an alternation. We accept anything that
/// mentions `arn:` literally — that covers every ARN-tolerant shape in
/// the vendored models (verified against `aws-models/lambda.json`,
/// `aws-models/iam.json`, `aws-models/s3.json`).
pub fn pattern_admits_arn(pattern: &str) -> bool {
    pattern.contains("arn:")
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

    #[test]
    fn parse_http_trait_on_operation() {
        // Lambda GetFunction has `@http(method: GET, uri: "/2015-03-31/functions/{FunctionName}")`.
        let dir = models_dir();
        let path = dir.join("lambda.json");
        let model = parse_model(&path).unwrap();
        let op = model
            .operations
            .iter()
            .find(|op| op.name == "GetFunction")
            .expect("GetFunction present");
        assert_eq!(op.http_method.as_deref(), Some("GET"));
        let uri = op.http_uri.as_deref().unwrap();
        assert!(uri.contains("{FunctionName}"), "uri contains label: {uri}");
    }

    #[test]
    fn parse_http_label_on_member() {
        // Lambda GetFunction's input has `FunctionName` marked `@httpLabel`.
        let dir = models_dir();
        let path = dir.join("lambda.json");
        let model = parse_model(&path).unwrap();
        let op = model
            .operations
            .iter()
            .find(|op| op.name == "GetFunction")
            .unwrap();
        let input_id = op.input_shape.as_deref().unwrap();
        let input = model.shapes.get(input_id).unwrap();
        let members = match &input.shape_type {
            ShapeType::Structure { members } => members,
            other => panic!("expected structure, got {other:?}"),
        };
        let fn_name = members.iter().find(|m| m.name == "FunctionName").unwrap();
        let target_traits = &model.shapes[&fn_name.target].traits;
        // http_label can sit on either the member or the target shape. Pragma:
        // we parse it onto whichever shape the trait lives on — check both.
        assert!(
            fn_name.traits.http_label || target_traits.http_label,
            "FunctionName carries @httpLabel"
        );
    }

    #[test]
    fn round_trip_pairs_discover_lambda_create_get() {
        // The whole point of #853: CreateFunction <-> GetFunction(Configuration)
        // must be discovered automatically from the Smithy graph.
        let dir = models_dir();
        let model = parse_model(&dir.join("lambda.json")).unwrap();
        let pairs = find_round_trip_pairs(&model);
        let lambda_pair = pairs
            .iter()
            .find(|p| p.writer.name == "CreateFunction")
            .expect("CreateFunction must pair with a reader");
        assert!(
            matches!(
                lambda_pair.reader.name.as_str(),
                "GetFunction" | "GetFunctionConfiguration"
            ),
            "got reader {}",
            lambda_pair.reader.name
        );
        assert_eq!(lambda_pair.id_source_field, "FunctionName");
    }

    #[test]
    fn round_trip_pairs_discover_dynamodb_create_describe() {
        let dir = models_dir();
        let model = parse_model(&dir.join("dynamodb.json")).unwrap();
        let pairs = find_round_trip_pairs(&model);
        let table_pair = pairs
            .iter()
            .find(|p| p.writer.name == "CreateTable")
            .expect("CreateTable must pair with DescribeTable");
        assert_eq!(table_pair.reader.name, "DescribeTable");
        assert_eq!(table_pair.id_source_field, "TableName");
    }

    #[test]
    fn pattern_admits_arn_recognises_lambda_function_name() {
        // Real Lambda FunctionName pattern admits both bare names and ARNs.
        let p = "(arn:(aws[a-zA-Z-]*)?:lambda:)?([a-z]{2}(-gov)?-[a-z]+-\\d{1}:)?(\\d{12}:)?(function:)?([a-zA-Z0-9-_\\.]+)(:(\\$LATEST|[a-zA-Z0-9-_]+))?";
        assert!(pattern_admits_arn(p));
    }

    #[test]
    fn pattern_admits_arn_rejects_bare_name_only() {
        let p = "[a-zA-Z0-9_-]+";
        assert!(!pattern_admits_arn(p));
    }

    #[test]
    fn pattern_admits_arn_recognises_iam_role_pattern() {
        let p = "^arn:[\\w+=/,.@-]+:iam::\\d+:role/[\\w+=,.@-]+$";
        assert!(pattern_admits_arn(p));
    }
}
