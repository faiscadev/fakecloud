//! `for_each` concerns from template.rs (audit-2026-05-19).

use super::*;

/// Expand `Fn::ForEach::<UniqueLoopName>` macros in `value` recursively.
///
/// Syntax (from the AWS docs / sample):
/// ```text
/// "Fn::ForEach::TopicLoop": [
///   "LoopVar",
///   ["a", "b", "c"],
///   { "${LoopVar}Topic": { "Type": "AWS::SNS::Topic", ... } }
/// ]
/// ```
/// becomes three siblings (`aTopic`, `bTopic`, `cTopic`) in the parent
/// object. `${LoopVar}` substitutes inside both keys and values, so the
/// emitted body can reference the iteration value the same way `Fn::Sub`
/// does.
///
/// Macros nest: an outer ForEach's bindings flow into inner ForEach
/// bodies via `bindings`, so `${OuterVar}` resolves inside an inner
/// loop's body. Each call resolves its own loop variable's iterations
/// before recursing into the emitted entries.
pub(super) fn expand_for_each(
    value: &Value,
    bindings: &BTreeMap<String, String>,
    parameters: &BTreeMap<String, String>,
) -> Result<Value, String> {
    match value {
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                if let Some(loop_name) = k.strip_prefix("Fn::ForEach::") {
                    let arr = v.as_array().ok_or_else(|| {
                        format!("Fn::ForEach::{loop_name} requires an array argument")
                    })?;
                    if arr.len() != 3 {
                        return Err(format!(
                            "Fn::ForEach::{loop_name} requires 3 arguments (loopVar, list, template), got {}",
                            arr.len()
                        ));
                    }
                    let loop_var = arr[0].as_str().ok_or_else(|| {
                        format!("Fn::ForEach::{loop_name} loop variable must be a string")
                    })?;
                    // The items list may be a literal array OR a `Ref`
                    // to a CommaDelimitedList parameter (AWS-supported).
                    // Resolve the latter against `parameters` by
                    // splitting on `,` so the loop iterates the same
                    // values the template author wrote.
                    let items_owned: Vec<Value> =
                        resolve_for_each_items(&arr[1], parameters).ok_or_else(|| {
                            format!(
                                "Fn::ForEach::{loop_name} second argument must be an array or a Ref to a CommaDelimitedList parameter"
                            )
                        })?;
                    let body = arr[2].as_object().ok_or_else(|| {
                        format!("Fn::ForEach::{loop_name} third argument must be an object")
                    })?;
                    for item in &items_owned {
                        let item_str = match item {
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        let mut next = bindings.clone();
                        next.insert(loop_var.to_string(), item_str.clone());
                        // Substitute loop vars across the whole body
                        // first, then recurse via `expand_for_each` so
                        // any nested `Fn::ForEach::*` keys land inline
                        // as sibling entries of `out` (instead of
                        // wrapping them under the unresolved macro key).
                        let body_value = Value::Object(body.clone());
                        let substituted = substitute_loop_vars_in_value(&body_value, &next);
                        let expanded = expand_for_each(&substituted, &next, parameters)?;
                        if let Value::Object(emitted) = expanded {
                            for (ek, ev) in emitted {
                                out.insert(ek, ev);
                            }
                        }
                    }
                    continue;
                }
                out.insert(k.clone(), expand_for_each(v, bindings, parameters)?);
            }
            Ok(Value::Object(out))
        }
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(expand_for_each(v, bindings, parameters)?);
            }
            Ok(Value::Array(out))
        }
        other => Ok(other.clone()),
    }
}

/// Expand AWS::Serverless-2016-10-31 SAM resources into native
/// CloudFormation resources so the provisioner can handle them.
pub(super) fn expand_sam(value: &Value) -> Value {
    let transform = value.get("Transform");
    let has_sam = match transform {
        Some(Value::String(s)) => s == "AWS::Serverless-2016-10-31",
        Some(Value::Array(arr)) => arr
            .iter()
            .any(|v| v.as_str() == Some("AWS::Serverless-2016-10-31")),
        _ => false,
    };
    if !has_sam {
        return value.clone();
    }

    let mut value = value.clone();
    let Some(resources) = value.get_mut("Resources") else {
        return value;
    };
    let Some(resources_map) = resources.as_object_mut() else {
        return value;
    };

    let mut new_resources = serde_json::Map::new();
    for (logical_id, resource) in resources_map.iter() {
        let Some(resource_obj) = resource.as_object() else {
            new_resources.insert(logical_id.clone(), resource.clone());
            continue;
        };
        let Some(ty) = resource_obj.get("Type").and_then(|v| v.as_str()) else {
            new_resources.insert(logical_id.clone(), resource.clone());
            continue;
        };
        let properties = resource_obj
            .get("Properties")
            .cloned()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()));

        match ty {
            "AWS::Serverless::Function" => {
                let mut lambda_props = if let Some(p) = properties.as_object() {
                    p.clone()
                } else {
                    serde_json::Map::new()
                };
                // Map CodeUri / InlineCode to Code
                if let Some(code_uri) = lambda_props.get("CodeUri").cloned() {
                    lambda_props.remove("CodeUri");
                    let code = if let Some(s) = code_uri.as_str() {
                        if let Some(stripped) = s.strip_prefix("s3://") {
                            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
                            if parts.len() == 2 {
                                json!({"S3Bucket": parts[0], "S3Key": parts[1]})
                            } else {
                                json!({"S3Bucket": "sam", "S3Key": s})
                            }
                        } else {
                            json!({"S3Bucket": "sam", "S3Key": s})
                        }
                    } else {
                        code_uri
                    };
                    lambda_props.insert("Code".to_string(), code);
                } else if let Some(inline) = lambda_props.get("InlineCode").cloned() {
                    lambda_props.remove("InlineCode");
                    lambda_props.insert("Code".to_string(), json!({"ZipFile": inline}));
                }
                let mut lambda_resource = serde_json::Map::new();
                lambda_resource.insert("Type".to_string(), json!("AWS::Lambda::Function"));
                lambda_resource.insert("Properties".to_string(), Value::Object(lambda_props));
                for (k, v) in resource_obj {
                    if k != "Type" && k != "Properties" {
                        lambda_resource.insert(k.clone(), v.clone());
                    }
                }
                new_resources.insert(logical_id.clone(), Value::Object(lambda_resource));
            }
            "AWS::Serverless::Api" => {
                let mut api_props = if let Some(p) = properties.as_object() {
                    p.clone()
                } else {
                    serde_json::Map::new()
                };
                if let Some(def) = api_props.get("DefinitionBody").cloned() {
                    api_props.remove("DefinitionBody");
                    api_props.insert("Body".to_string(), def);
                }
                let mut api_resource = serde_json::Map::new();
                api_resource.insert("Type".to_string(), json!("AWS::ApiGateway::RestApi"));
                api_resource.insert("Properties".to_string(), Value::Object(api_props));
                for (k, v) in resource_obj {
                    if k != "Type" && k != "Properties" {
                        api_resource.insert(k.clone(), v.clone());
                    }
                }
                new_resources.insert(logical_id.clone(), Value::Object(api_resource));
            }
            "AWS::Serverless::HttpApi" => {
                let mut httpapi_resource = serde_json::Map::new();
                httpapi_resource.insert("Type".to_string(), json!("AWS::ApiGatewayV2::Api"));
                httpapi_resource.insert("Properties".to_string(), properties);
                for (k, v) in resource_obj {
                    if k != "Type" && k != "Properties" {
                        httpapi_resource.insert(k.clone(), v.clone());
                    }
                }
                new_resources.insert(logical_id.clone(), Value::Object(httpapi_resource));
            }
            "AWS::Serverless::SimpleTable" => {
                let mut table_props = if let Some(p) = properties.as_object() {
                    p.clone()
                } else {
                    serde_json::Map::new()
                };
                if let Some(pk) = table_props.get("PrimaryKey") {
                    if let Some(pk_obj) = pk.as_object() {
                        let name = pk_obj.get("Name").cloned().unwrap_or_else(|| json!("id"));
                        let ty = match pk_obj.get("Type").and_then(|v| v.as_str()) {
                            Some("String") => json!("S"),
                            Some("Number") => json!("N"),
                            Some("Binary") => json!("B"),
                            Some(other) => json!(other),
                            None => json!("S"),
                        };
                        table_props.remove("PrimaryKey");
                        table_props.insert(
                            "KeySchema".to_string(),
                            json!([{"AttributeName": name.clone(), "KeyType": "HASH"}]),
                        );
                        table_props.insert(
                            "AttributeDefinitions".to_string(),
                            json!([{"AttributeName": name, "AttributeType": ty}]),
                        );
                    }
                }
                if !table_props.contains_key("BillingMode") {
                    table_props.insert("BillingMode".to_string(), json!("PAY_PER_REQUEST"));
                }
                let mut table_resource = serde_json::Map::new();
                table_resource.insert("Type".to_string(), json!("AWS::DynamoDB::Table"));
                table_resource.insert("Properties".to_string(), Value::Object(table_props));
                for (k, v) in resource_obj {
                    if k != "Type" && k != "Properties" {
                        table_resource.insert(k.clone(), v.clone());
                    }
                }
                new_resources.insert(logical_id.clone(), Value::Object(table_resource));
            }
            "AWS::Serverless::LayerVersion" => {
                let mut layer_props = if let Some(p) = properties.as_object() {
                    p.clone()
                } else {
                    serde_json::Map::new()
                };
                if let Some(uri) = layer_props.get("ContentUri").cloned() {
                    layer_props.remove("ContentUri");
                    let content = if let Some(s) = uri.as_str() {
                        if let Some(stripped) = s.strip_prefix("s3://") {
                            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
                            if parts.len() == 2 {
                                json!({"S3Bucket": parts[0], "S3Key": parts[1]})
                            } else {
                                json!({"S3Bucket": "sam", "S3Key": s})
                            }
                        } else {
                            json!({"S3Bucket": "sam", "S3Key": s})
                        }
                    } else {
                        uri
                    };
                    layer_props.insert("Content".to_string(), content);
                }
                let mut layer_resource = serde_json::Map::new();
                layer_resource.insert("Type".to_string(), json!("AWS::Lambda::LayerVersion"));
                layer_resource.insert("Properties".to_string(), Value::Object(layer_props));
                for (k, v) in resource_obj {
                    if k != "Type" && k != "Properties" {
                        layer_resource.insert(k.clone(), v.clone());
                    }
                }
                new_resources.insert(logical_id.clone(), Value::Object(layer_resource));
            }
            _ => {
                new_resources.insert(logical_id.clone(), resource.clone());
            }
        }
    }

    resources_map.clear();
    for (k, v) in new_resources {
        resources_map.insert(k, v);
    }
    value
}

/// Resolve the `items` argument of an `Fn::ForEach` macro. Accepts:
/// - A literal JSON array — returned as-is.
/// - `{ "Ref": "<name>" }` against a parameter holding either a comma
///   delimited list (`CommaDelimitedList` / `List<*>`) or a single
///   value. Splits on `,` and trims whitespace so parameters set as
///   `"a, b, c"` iterate cleanly.
///
/// Returns `None` for any other shape (e.g. an object that isn't a
/// `Ref`, or a `Ref` to an undefined parameter), letting the caller
/// surface a precise error.
pub(super) fn resolve_for_each_items(
    value: &Value,
    parameters: &BTreeMap<String, String>,
) -> Option<Vec<Value>> {
    if let Some(arr) = value.as_array() {
        return Some(arr.clone());
    }
    if let Some(map) = value.as_object() {
        if let Some(name) = map.get("Ref").and_then(|v| v.as_str()) {
            let raw = parameters.get(name)?;
            return Some(
                raw.split(',')
                    .map(|p| Value::String(p.trim().to_string()))
                    .collect(),
            );
        }
    }
    None
}

/// Substitute every `${var}` and `&{var}` token in a string against
/// `bindings`. Both forms are AWS-supported for `Fn::ForEach` loop
/// variables — `&{}` exists so identifiers with non-alphanumeric
/// characters can interpolate into resource logical IDs without
/// colliding with Fn::Sub's `${}` syntax. Unknown vars stay verbatim
/// so non-loop substitutions (Fn::Sub, resource physical IDs) handle
/// them later.
pub(super) fn substitute_loop_vars(s: &str, bindings: &BTreeMap<String, String>) -> String {
    let mut result = s.to_string();
    for (k, v) in bindings {
        result = result.replace(&format!("${{{k}}}"), v);
        result = result.replace(&format!("&{{{k}}}"), v);
    }
    result
}

/// Walk `value` and apply `substitute_loop_vars` to every string leaf.
/// Object keys are also rewritten so resource logical IDs and property
/// names parameterized by the loop variable land correctly.
pub(super) fn substitute_loop_vars_in_value(
    value: &Value,
    bindings: &BTreeMap<String, String>,
) -> Value {
    match value {
        Value::String(s) => Value::String(substitute_loop_vars(s, bindings)),
        Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                let new_key = substitute_loop_vars(k, bindings);
                out.insert(new_key, substitute_loop_vars_in_value(v, bindings));
            }
            Value::Object(out)
        }
        Value::Array(arr) => Value::Array(
            arr.iter()
                .map(|v| substitute_loop_vars_in_value(v, bindings))
                .collect(),
        ),
        other => other.clone(),
    }
}
