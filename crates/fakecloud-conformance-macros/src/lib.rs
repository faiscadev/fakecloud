use proc_macro::TokenStream;
use quote::quote;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, ItemFn, LitStr, Token};

// ---------------------------------------------------------------------------
// Macro argument parsing
// ---------------------------------------------------------------------------

struct TestActionArgs {
    service: String,
    action: String,
    checksum: String,
}

impl Parse for TestActionArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let service: LitStr = input.parse()?;
        input.parse::<Token![,]>()?;
        let action: LitStr = input.parse()?;
        input.parse::<Token![,]>()?;

        // Parse `checksum = "..."`
        let ident: syn::Ident = input.parse()?;
        if ident != "checksum" {
            return Err(syn::Error::new(ident.span(), "expected `checksum`"));
        }
        input.parse::<Token![=]>()?;
        let checksum: LitStr = input.parse()?;

        Ok(TestActionArgs {
            service: service.value(),
            action: action.value(),
            checksum: checksum.value(),
        })
    }
}

// ---------------------------------------------------------------------------
// Cached model store (thread-local to avoid re-parsing per invocation)
// ---------------------------------------------------------------------------

thread_local! {
    #[allow(clippy::missing_const_for_thread_local)]
    static MODEL_CACHE: RefCell<HashMap<String, serde_json::Value>> = RefCell::new(HashMap::new());
    static SERVICE_MAP_CACHE: RefCell<Option<HashMap<String, ServiceMapEntry>>> = const { RefCell::new(None) };
}

#[derive(Clone)]
struct ServiceMapEntry {
    service_name: String,
}

fn aws_models_dir() -> PathBuf {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR not set");
    PathBuf::from(manifest_dir)
        .join("..")
        .join("..")
        .join("aws-models")
}

fn load_service_map() -> HashMap<String, ServiceMapEntry> {
    SERVICE_MAP_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(ref map) = *cache {
            return map.clone();
        }
        let path = aws_models_dir().join("service-map.json");
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e));
        let raw: serde_json::Value = serde_json::from_str(&content)
            .unwrap_or_else(|e| panic!("Failed to parse {}: {}", path.display(), e));
        let obj = raw.as_object().expect("service-map.json must be an object");
        let mut map = HashMap::new();
        for (key, val) in obj {
            let service_name = val
                .get("service_name")
                .and_then(|v| v.as_str())
                .unwrap_or(key)
                .to_string();
            map.insert(key.clone(), ServiceMapEntry { service_name });
        }
        *cache = Some(map.clone());
        map
    })
}

fn load_model(model_key: &str) -> serde_json::Value {
    MODEL_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(val) = cache.get(model_key) {
            return val.clone();
        }
        let path = aws_models_dir().join(format!("{}.json", model_key));
        let content = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("Failed to read {}: {}", path.display(), e));
        let val: serde_json::Value = serde_json::from_str(&content)
            .unwrap_or_else(|e| panic!("Failed to parse {}: {}", path.display(), e));
        cache.insert(model_key.to_string(), val.clone());
        val
    })
}

// ---------------------------------------------------------------------------
// Resolve model key from service name (e.g. "sqs" -> "sqs", "logs" -> "cloudwatch-logs")
// ---------------------------------------------------------------------------

fn resolve_model_key(service: &str) -> Option<String> {
    let service_map = load_service_map();
    // First: direct key match
    if service_map.contains_key(service) {
        return Some(service.to_string());
    }
    // Second: match by service_name
    for (key, entry) in &service_map {
        if entry.service_name == service {
            return Some(key.clone());
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Minimal Smithy model querying (enough for operation lookup + checksum)
// ---------------------------------------------------------------------------

struct OperationInfo {
    name: String,
    input_shape: Option<String>,
    output_shape: Option<String>,
    error_shapes: Vec<String>,
}

fn find_operation(root: &serde_json::Value, action: &str) -> Option<OperationInfo> {
    let shapes = root.get("shapes")?.as_object()?;

    // Collect all operation targets from the service shape (including resources)
    let mut op_targets = Vec::new();
    for (_id, def) in shapes {
        if def.get("type").and_then(|v| v.as_str()) == Some("service") {
            if let Some(ops) = def.get("operations").and_then(|v| v.as_array()) {
                for op in ops {
                    if let Some(t) = op.get("target").and_then(|v| v.as_str()) {
                        op_targets.push(t.to_string());
                    }
                }
            }
            if let Some(resources) = def.get("resources").and_then(|v| v.as_array()) {
                for res in resources {
                    if let Some(t) = res.get("target").and_then(|v| v.as_str()) {
                        collect_resource_operations(shapes, t, &mut op_targets);
                    }
                }
            }
            break;
        }
    }

    // Find the matching operation
    for target in &op_targets {
        let short = target.rsplit('#').next().unwrap_or(target);
        if short == action {
            let op_def = shapes.get(target.as_str())?;
            let input_shape = op_def
                .get("input")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let output_shape = op_def
                .get("output")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let error_shapes = op_def
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
            return Some(OperationInfo {
                name: short.to_string(),
                input_shape,
                output_shape,
                error_shapes,
            });
        }
    }
    None
}

fn collect_resource_operations(
    shapes: &serde_json::Map<String, serde_json::Value>,
    resource_target: &str,
    targets: &mut Vec<String>,
) {
    let resource_def = match shapes.get(resource_target) {
        Some(d) => d,
        None => return,
    };
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
            if let Some(t) = op.get("target").and_then(|v| v.as_str()) {
                targets.push(t.to_string());
            }
        }
    }
    if let Some(ops) = resource_def
        .get("collectionOperations")
        .and_then(|v| v.as_array())
    {
        for op in ops {
            if let Some(t) = op.get("target").and_then(|v| v.as_str()) {
                targets.push(t.to_string());
            }
        }
    }
    if let Some(resources) = resource_def.get("resources").and_then(|v| v.as_array()) {
        for res in resources {
            if let Some(t) = res.get("target").and_then(|v| v.as_str()) {
                collect_resource_operations(shapes, t, targets);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Checksum computation (mirrors fakecloud-conformance/src/checksum.rs exactly)
// ---------------------------------------------------------------------------

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

fn compute_checksum(root: &serde_json::Value, op: &OperationInfo) -> String {
    let shapes = root.get("shapes").and_then(|v| v.as_object()).unwrap();
    let mut collected = BTreeMap::new();
    let mut visited = HashSet::new();

    if let Some(ref input_id) = op.input_shape {
        collect_shape_tree(shapes, input_id, &mut collected, &mut visited);
    }
    if let Some(ref output_id) = op.output_shape {
        collect_shape_tree(shapes, output_id, &mut collected, &mut visited);
    }
    for error_id in &op.error_shapes {
        collect_shape_tree(shapes, error_id, &mut collected, &mut visited);
    }

    let canonical = build_canonical(&op.name, op, &collected);

    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let result = hasher.finalize();
    hex_encode(&result[..4])
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn collect_shape_tree(
    shapes: &serde_json::Map<String, serde_json::Value>,
    shape_id: &str,
    collected: &mut BTreeMap<String, ShapeCanonical>,
    visited: &mut HashSet<String>,
) {
    if visited.contains(shape_id) {
        return;
    }
    visited.insert(shape_id.to_string());

    // Handle prelude types
    if shape_id.starts_with("smithy.api#") {
        let short = shape_id.rsplit('#').next().unwrap_or(shape_id);
        collected.insert(
            shape_id.to_string(),
            ShapeCanonical {
                shape_type: short.to_string(),
                members: BTreeMap::new(),
                constraints: String::new(),
            },
        );
        return;
    }

    let shape_def = match shapes.get(shape_id) {
        Some(s) => s,
        None => return,
    };

    let type_str = shape_def
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let raw_traits = shape_def.get("traits").and_then(|v| v.as_object());

    let mut canonical = ShapeCanonical {
        shape_type: shape_type_name(type_str, shape_def),
        members: BTreeMap::new(),
        constraints: format_constraints(raw_traits),
    };

    match type_str {
        "structure" | "union" => {
            if let Some(members) = shape_def.get("members").and_then(|v| v.as_object()) {
                for (name, member_def) in members {
                    let target = member_def
                        .get("target")
                        .and_then(|v| v.as_str())
                        .unwrap_or("smithy.api#String")
                        .to_string();
                    let required = member_def
                        .get("traits")
                        .and_then(|v| v.as_object())
                        .map(|t| t.contains_key("smithy.api#required"))
                        .unwrap_or(false);
                    canonical.members.insert(
                        name.clone(),
                        MemberCanonical {
                            target: target.clone(),
                            required,
                        },
                    );
                    collect_shape_tree(shapes, &target, collected, visited);
                }
            }
        }
        "list" => {
            let member_target = shape_def
                .get("member")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String");
            collect_shape_tree(shapes, member_target, collected, visited);
        }
        "map" => {
            let key_target = shape_def
                .get("key")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String");
            let value_target = shape_def
                .get("value")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String");
            collect_shape_tree(shapes, key_target, collected, visited);
            collect_shape_tree(shapes, value_target, collected, visited);
        }
        "string" => {
            // Check for @enum trait (Smithy 1.0 style)
            if let Some(enum_vals) = raw_traits
                .and_then(|t| t.get("smithy.api#enum"))
                .and_then(|v| v.as_array())
            {
                let vals: Vec<&str> = enum_vals
                    .iter()
                    .filter_map(|e| e.get("value").and_then(|v| v.as_str()))
                    .collect();
                canonical.constraints = format!("enum:{}", vals.join(","));
            }
        }
        "enum" => {
            // Smithy 2.0 enum shape
            if let Some(members) = shape_def.get("members").and_then(|v| v.as_object()) {
                let vals: Vec<String> = members
                    .iter()
                    .map(|(name, member_def)| {
                        member_def
                            .get("traits")
                            .and_then(|t| t.get("smithy.api#enumValue"))
                            .and_then(|v| v.as_str())
                            .unwrap_or(name)
                            .to_string()
                    })
                    .collect();
                canonical.constraints = format!("enum:{}", vals.join(","));
            }
        }
        _ => {}
    }

    collected.insert(shape_id.to_string(), canonical);
}

fn shape_type_name(type_str: &str, shape_def: &serde_json::Value) -> String {
    match type_str {
        "structure" => "structure".to_string(),
        "union" => "union".to_string(),
        "list" => {
            let member_target = shape_def
                .get("member")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String");
            format!("list<{}>", member_target)
        }
        "map" => {
            let key_target = shape_def
                .get("key")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String");
            let value_target = shape_def
                .get("value")
                .and_then(|v| v.get("target"))
                .and_then(|v| v.as_str())
                .unwrap_or("smithy.api#String");
            format!("map<{},{}>", key_target, value_target)
        }
        "string" => "string".to_string(),
        "enum" => "enum".to_string(),
        "intEnum" => "intEnum".to_string(),
        "integer" => "integer".to_string(),
        "long" => "long".to_string(),
        "float" => "float".to_string(),
        "double" => "double".to_string(),
        "boolean" => "boolean".to_string(),
        "blob" => "blob".to_string(),
        "timestamp" => "timestamp".to_string(),
        "service" => "service".to_string(),
        "operation" => "operation".to_string(),
        "resource" => "resource".to_string(),
        other => other.to_string(),
    }
}

fn format_constraints(raw_traits: Option<&serde_json::Map<String, serde_json::Value>>) -> String {
    let raw = match raw_traits {
        Some(t) => t,
        None => return String::new(),
    };
    let mut parts = Vec::new();
    if let Some(length) = raw.get("smithy.api#length") {
        if let Some(min) = length.get("min").and_then(|v| v.as_u64()) {
            parts.push(format!("len_min:{}", min));
        }
        if let Some(max) = length.get("max").and_then(|v| v.as_u64()) {
            parts.push(format!("len_max:{}", max));
        }
    }
    if let Some(range) = raw.get("smithy.api#range") {
        if let Some(min) = range.get("min").and_then(|v| v.as_f64()) {
            parts.push(format!("range_min:{}", min));
        }
        if let Some(max) = range.get("max").and_then(|v| v.as_f64()) {
            parts.push(format!("range_max:{}", max));
        }
    }
    if let Some(pattern) = raw.get("smithy.api#pattern").and_then(|v| v.as_str()) {
        parts.push(format!("pattern:{}", pattern));
    }
    parts.join(";")
}

fn build_canonical(
    op_name: &str,
    op: &OperationInfo,
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

// ---------------------------------------------------------------------------
// The proc macro itself
// ---------------------------------------------------------------------------

/// Attribute macro for Level 2 conformance tests.
///
/// Usage:
///
/// ```text
/// #[test_action("sqs", "CreateQueue", checksum = "a3f8b2c1")]
/// fn test_create_queue() { ... }
/// ```
///
/// At compile time this validates:
/// 1. The action exists in the Smithy model for the given service.
/// 2. The checksum matches the current model's operation signature.
///
/// The function is passed through unchanged.
#[proc_macro_attribute]
pub fn test_action(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as TestActionArgs);
    let input_fn = parse_macro_input!(item as ItemFn);

    // Resolve the model key
    let model_key = match resolve_model_key(&args.service) {
        Some(k) => k,
        None => {
            let msg = format!(
                "Unknown service '{}': not found in aws-models/service-map.json",
                args.service
            );
            return syn::Error::new(proc_macro2::Span::call_site(), msg)
                .to_compile_error()
                .into();
        }
    };

    // Load the model
    let root = load_model(&model_key);

    // Find the operation
    let op = match find_operation(&root, &args.action) {
        Some(o) => o,
        None => {
            let msg = format!(
                "Action '{}' not found in Smithy model for service '{}'",
                args.action, args.service
            );
            return syn::Error::new(proc_macro2::Span::call_site(), msg)
                .to_compile_error()
                .into();
        }
    };

    // Compute and validate checksum
    let actual_checksum = compute_checksum(&root, &op);
    if actual_checksum != args.checksum {
        let msg = format!(
            "Checksum mismatch for {}.{}: expected '{}', got '{}'. \
             The Smithy model has changed — update the checksum or the conformance test.",
            args.service, args.action, args.checksum, actual_checksum
        );
        return syn::Error::new(proc_macro2::Span::call_site(), msg)
            .to_compile_error()
            .into();
    }

    // Pass through the original function unchanged
    let output = quote! {
        #input_fn
    };
    output.into()
}
