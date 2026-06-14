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

    // SAM `Globals` supply default properties for every resource of a given
    // type; per-resource Properties override them per key. Real `sam`/
    // samtranslator applies these during the transform, so without merging them
    // here a function that sets Handler/Runtime only in `Globals.Function`
    // deploys with the bare Lambda defaults (index.handler / python3.12) and
    // fails every invoke with `Runtime.HandlerNotFound`.
    let global = |section: &str| {
        value
            .get("Globals")
            .and_then(|g| g.get(section))
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default()
    };
    let global_function = global("Function");
    let global_api = global("Api");
    let global_http_api = global("HttpApi");
    let global_simple_table = global("SimpleTable");
    let global_state_machine = global("StateMachine");

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
                let mut lambda_props = merge_global_properties(&global_function, &properties);
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
                let mut api_props = merge_global_properties(&global_api, &properties);
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
                let httpapi_props = merge_global_properties(&global_http_api, &properties);
                let mut httpapi_resource = serde_json::Map::new();
                httpapi_resource.insert("Type".to_string(), json!("AWS::ApiGatewayV2::Api"));
                httpapi_resource.insert("Properties".to_string(), Value::Object(httpapi_props));
                for (k, v) in resource_obj {
                    if k != "Type" && k != "Properties" {
                        httpapi_resource.insert(k.clone(), v.clone());
                    }
                }
                new_resources.insert(logical_id.clone(), Value::Object(httpapi_resource));
            }
            "AWS::Serverless::SimpleTable" => {
                let mut table_props = merge_global_properties(&global_simple_table, &properties);
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
            "AWS::Serverless::StateMachine" => {
                let mut sfn_props = merge_global_properties(&global_state_machine, &properties);
                // `Definition` (inline ASL object) passes through unchanged —
                // the native provisioner's resolve_sfn_definition reads it
                // directly. `DefinitionSubstitutions` likewise passes through.
                // Map `DefinitionUri` onto `DefinitionS3Location`, reusing the
                // same `s3://bucket/key` parsing the Function arm uses for
                // CodeUri. SAM also allows the object form
                // `{Bucket, Key, Version}`, which maps over verbatim.
                if let Some(uri) = sfn_props.get("DefinitionUri").cloned() {
                    sfn_props.remove("DefinitionUri");
                    let location = if let Some(s) = uri.as_str() {
                        if let Some(stripped) = s.strip_prefix("s3://") {
                            let parts: Vec<&str> = stripped.splitn(2, '/').collect();
                            if parts.len() == 2 {
                                json!({"Bucket": parts[0], "Key": parts[1]})
                            } else {
                                json!({"Bucket": "sam", "Key": s})
                            }
                        } else {
                            json!({"Bucket": "sam", "Key": s})
                        }
                    } else {
                        // Already an object form: {Bucket, Key, Version}.
                        uri
                    };
                    sfn_props.insert("DefinitionS3Location".to_string(), location);
                }
                // `Role` (ARN) → `RoleArn`.
                if let Some(role) = sfn_props.remove("Role") {
                    sfn_props.insert("RoleArn".to_string(), role);
                }
                // `Name` → `StateMachineName`.
                if let Some(name) = sfn_props.remove("Name") {
                    sfn_props.insert("StateMachineName".to_string(), name);
                }
                // SAM `Type` (STANDARD|EXPRESS) → native `StateMachineType`.
                if let Some(machine_type) = sfn_props.remove("Type") {
                    sfn_props.insert("StateMachineType".to_string(), machine_type);
                }
                // TODO: expand `Events` (Api/Schedule/EventBridge/SQS/etc.)
                // into the corresponding trigger resources. Deferred — the
                // state machine itself expands and provisions without it.
                sfn_props.remove("Events");

                let mut sfn_resource = serde_json::Map::new();
                sfn_resource.insert(
                    "Type".to_string(),
                    json!("AWS::StepFunctions::StateMachine"),
                );
                sfn_resource.insert("Properties".to_string(), Value::Object(sfn_props));
                for (k, v) in resource_obj {
                    if k != "Type" && k != "Properties" {
                        sfn_resource.insert(k.clone(), v.clone());
                    }
                }
                new_resources.insert(logical_id.clone(), Value::Object(sfn_resource));
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

/// Merge SAM `Globals.<Section>` defaults under a resource's own `Properties`.
/// Per-resource keys win (plain per-key override). AWS appends a few list-typed
/// Globals (e.g. `Layers`, `Policies`) rather than replacing; per-key override
/// is sufficient for the templates we target and matches the common case where
/// the relevant keys (Handler, Runtime, Timeout, MemorySize, Environment) are
/// set in exactly one place.
fn merge_global_properties(
    globals: &serde_json::Map<String, Value>,
    properties: &Value,
) -> serde_json::Map<String, Value> {
    let mut merged = globals.clone();
    if let Some(p) = properties.as_object() {
        for (k, v) in p {
            merged.insert(k.clone(), v.clone());
        }
    }
    merged
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

#[cfg(test)]
mod tests {
    use super::*;

    // Gap #4: Handler/Runtime set only in `Globals.Function` must land on the
    // expanded Lambda; otherwise the function deploys with the bare defaults
    // (index.handler / python3.12) and every invoke raises Runtime.HandlerNotFound.
    #[test]
    fn expand_sam_applies_function_globals() {
        let template = json!({
            "Transform": "AWS::Serverless-2016-10-31",
            "Globals": {
                "Function": {
                    "Handler": "index.lambda_handler",
                    "Runtime": "python3.13",
                    "Timeout": 300,
                    "MemorySize": 256
                }
            },
            "Resources": {
                "Dispatcher": {
                    "Type": "AWS::Serverless::Function",
                    "Properties": {
                        "FunctionName": "workflow_dispatcher_v2",
                        "InlineCode": "def lambda_handler(e, c): return e"
                    }
                }
            }
        });

        let props = &expand_sam(&template)["Resources"]["Dispatcher"]["Properties"];
        assert_eq!(props["Handler"], json!("index.lambda_handler"));
        assert_eq!(props["Runtime"], json!("python3.13"));
        assert_eq!(props["Timeout"], json!(300));
        assert_eq!(props["MemorySize"], json!(256));
        // Per-function properties are preserved alongside the merged globals.
        assert_eq!(props["FunctionName"], json!("workflow_dispatcher_v2"));
    }

    // Per-function Properties override Globals per key.
    #[test]
    fn expand_sam_function_overrides_globals() {
        let template = json!({
            "Transform": "AWS::Serverless-2016-10-31",
            "Globals": {"Function": {"Handler": "index.lambda_handler", "Runtime": "python3.13"}},
            "Resources": {
                "F": {
                    "Type": "AWS::Serverless::Function",
                    "Properties": {"Handler": "app.main", "InlineCode": "x"}
                }
            }
        });

        let props = &expand_sam(&template)["Resources"]["F"]["Properties"];
        assert_eq!(
            props["Handler"],
            json!("app.main"),
            "per-function Handler wins"
        );
        assert_eq!(
            props["Runtime"],
            json!("python3.13"),
            "global Runtime still applies"
        );
    }

    #[test]
    fn expand_sam_statemachine_inline_definition() {
        let template = json!({
            "Transform": "AWS::Serverless-2016-10-31",
            "Resources": {
                "MySM": {
                    "Type": "AWS::Serverless::StateMachine",
                    "DependsOn": "SomeOtherResource",
                    "Properties": {
                        "Definition": {
                            "StartAt": "Done",
                            "States": {"Done": {"Type": "Succeed"}}
                        },
                        "DefinitionSubstitutions": {"fn": "my-fn"},
                        "Role": "arn:aws:iam::123456789012:role/sfn-role",
                        "Name": "my-state-machine",
                        "Type": "EXPRESS"
                    }
                }
            }
        });

        let expanded = expand_sam(&template);
        let resource = &expanded["Resources"]["MySM"];

        assert_eq!(resource["Type"], json!("AWS::StepFunctions::StateMachine"));

        let props = &resource["Properties"];
        assert_eq!(
            props["RoleArn"],
            json!("arn:aws:iam::123456789012:role/sfn-role")
        );
        assert_eq!(props["StateMachineName"], json!("my-state-machine"));
        assert_eq!(props["StateMachineType"], json!("EXPRESS"));
        // Inline definition carries over unchanged for the provisioner to read.
        assert_eq!(
            props["Definition"],
            json!({"StartAt": "Done", "States": {"Done": {"Type": "Succeed"}}})
        );
        // DefinitionSubstitutions passes through.
        assert_eq!(props["DefinitionSubstitutions"], json!({"fn": "my-fn"}));
        // SAM-only keys are gone.
        assert!(props.get("Role").is_none());
        assert!(props.get("Name").is_none());
        // Resource-level metadata is preserved.
        assert_eq!(resource["DependsOn"], json!("SomeOtherResource"));
    }

    #[test]
    fn expand_sam_statemachine_definition_uri() {
        let template = json!({
            "Transform": "AWS::Serverless-2016-10-31",
            "Resources": {
                "MySM": {
                    "Type": "AWS::Serverless::StateMachine",
                    "Properties": {
                        "DefinitionUri": "s3://my-bucket/path/to/def.asl.json",
                        "Role": "arn:aws:iam::123456789012:role/sfn-role"
                    }
                }
            }
        });

        let expanded = expand_sam(&template);
        let props = &expanded["Resources"]["MySM"]["Properties"];

        assert_eq!(
            props["DefinitionS3Location"],
            json!({"Bucket": "my-bucket", "Key": "path/to/def.asl.json"})
        );
        assert!(props.get("DefinitionUri").is_none());
        assert_eq!(
            props["RoleArn"],
            json!("arn:aws:iam::123456789012:role/sfn-role")
        );
    }
}
