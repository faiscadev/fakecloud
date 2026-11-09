//! `parser` concerns from template.rs (audit-2026-05-19).

use super::*;

/// Parse a CloudFormation template from a string (JSON or YAML).
pub fn parse_template(
    template_body: &str,
    parameters: &BTreeMap<String, String>,
) -> Result<ParsedTemplate, String> {
    parse_template_with_physical_ids(template_body, parameters, &BTreeMap::new())
}

/// Parse a CloudFormation template, resolving Refs using known physical resource IDs.
pub fn parse_template_with_physical_ids(
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    resource_physical_ids: &BTreeMap<String, String>,
) -> Result<ParsedTemplate, String> {
    parse_template_with_resolution(
        template_body,
        parameters,
        resource_physical_ids,
        &BTreeMap::new(),
    )
}

/// Parse a CloudFormation template, resolving `Ref` via `resource_physical_ids`
/// and `Fn::GetAtt` via `resource_attributes` (keyed by logical id, then
/// attribute name).
pub fn parse_template_with_resolution(
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    resource_physical_ids: &BTreeMap<String, String>,
    resource_attributes: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<ParsedTemplate, String> {
    let value: Value = fakecloud_core::cfn_template::parse_template_body(template_body)?;

    // Expand `Fn::ForEach::*` macros (template transform). New resources
    // and properties land in place before the rest of resolution sees the
    // template, so a ForEach-emitted resource works exactly like a
    // hand-authored one. Parameters flow in so the items list can be a
    // `Ref` to a CommaDelimitedList parameter.
    let value = expand_for_each(&value, &BTreeMap::new(), parameters)?;
    let value = expand_sam(&value)?;

    let description = value
        .get("Description")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let conditions = evaluate_conditions(&value, parameters)?;
    let mappings = parse_mappings(&value);

    let resources_obj = value
        .get("Resources")
        .and_then(|v| v.as_object())
        .ok_or("Template must contain a Resources section")?;

    let mut resources = Vec::new();
    for (logical_id, resource) in resources_obj {
        // Skip resources whose Condition evaluates to false. Real CFN
        // simply omits these resources from the stack.
        if let Some(cond_name) = resource.get("Condition").and_then(|v| v.as_str()) {
            if !conditions.get(cond_name).copied().unwrap_or(false) {
                continue;
            }
        }
        let resource_type = resource
            .get("Type")
            .and_then(|v| v.as_str())
            .ok_or(format!("Resource {logical_id} must have a Type property"))?
            .to_string();

        let properties = resource
            .get("Properties")
            .cloned()
            .unwrap_or(Value::Object(serde_json::Map::new()));

        // Pre-resolve Fn::FindInMap before the main intrinsics pass so the
        // existing resolver doesn't need to thread mappings through. We
        // pass `conditions` so a FindInMap sitting in an unused Fn::If
        // branch is skipped (CFN never executes the dropped branch).
        let properties = apply_mappings(&properties, parameters, &mappings, &conditions)?;

        // Resolve Ref and parameter substitutions in properties
        let resolved = resolve_refs_full(
            &properties,
            parameters,
            resources_obj,
            resource_physical_ids,
            resource_attributes,
            &BTreeMap::new(),
            &conditions,
        );
        let resolved = strip_no_value(resolved);

        // DeletionPolicy / UpdateReplacePolicy are resource-level attributes
        // (siblings of Type/Properties), not properties. Capture them verbatim
        // so the delete/replace paths can honor Retain/Snapshot.
        let deletion_policy = resource
            .get("DeletionPolicy")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        let update_replace_policy = resource
            .get("UpdateReplacePolicy")
            .and_then(|v| v.as_str())
            .map(str::to_string);

        resources.push(ResourceDefinition {
            logical_id: logical_id.clone(),
            resource_type,
            properties: resolved,
            deletion_policy,
            update_replace_policy,
        });
    }

    let outputs = parse_outputs(
        &value,
        parameters,
        resources_obj,
        resource_physical_ids,
        resource_attributes,
        &BTreeMap::new(),
    )?;

    Ok(ParsedTemplate {
        description,
        resources,
        outputs,
    })
}

/// Walk every `Fn::ImportValue` site in the parsed template (Resources +
/// Outputs) and collect the static export names it references. Names that
/// can only be resolved at runtime (e.g. `{ "Fn::Sub": "${Env}-arn" }`)
/// resolve against `parameters` first; if they still aren't strings,
/// they're skipped — the runtime resolver will surface the gap then.
pub fn collect_import_value_names(
    template: &Value,
    parameters: &BTreeMap<String, String>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    collect_imports_walk(template, parameters, &mut out);
    out.sort();
    out.dedup();
    out
}

pub(super) fn collect_imports_walk(
    value: &Value,
    parameters: &BTreeMap<String, String>,
    out: &mut Vec<String>,
) {
    match value {
        Value::Object(map) => {
            if let Some(arg) = map.get("Fn::ImportValue") {
                if let Some(name) = static_import_name(arg, parameters) {
                    out.push(name);
                } else {
                    // Recurse into the arg in case it contains nested ImportValues.
                    collect_imports_walk(arg, parameters, out);
                }
            }
            for (k, v) in map {
                if k == "Fn::ImportValue" {
                    continue;
                }
                collect_imports_walk(v, parameters, out);
            }
        }
        Value::Array(arr) => {
            for v in arr {
                collect_imports_walk(v, parameters, out);
            }
        }
        _ => {}
    }
}

pub(super) fn static_import_name(
    value: &Value,
    parameters: &BTreeMap<String, String>,
) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Object(m) => {
            if let Some(name) = m.get("Ref").and_then(|v| v.as_str()) {
                return parameters.get(name).cloned();
            }
            if let Some(s) = m.get("Fn::Sub").and_then(|v| v.as_str()) {
                let mut result = s.to_string();
                for (k, v) in parameters {
                    result = result.replace(&format!("${{{k}}}"), v);
                }
                if !result.contains("${") {
                    return Some(result);
                }
            }
            None
        }
        _ => None,
    }
}

/// The template after the macro expansions provisioning sees, `None` when the
/// body is not a template this crate can read.
fn expanded_template(template_body: &str, parameters: &BTreeMap<String, String>) -> Option<Value> {
    let value = fakecloud_core::cfn_template::parse_template_body(template_body).ok()?;
    let value = expand_for_each(&value, &BTreeMap::new(), parameters).ok()?;
    expand_sam(&value).ok()
}

/// The names this template imports with `Fn::ImportValue`.
pub fn imported_names(template_body: &str, parameters: &BTreeMap<String, String>) -> Vec<String> {
    fakecloud_core::cfn_template::parse_template_body(template_body)
        .map(|value| collect_import_value_names(&value, parameters))
        .unwrap_or_default()
}

/// The export names this template's `Outputs` declare, skipping an output whose
/// `Condition` is false. CloudFormation requires a name to be known before any
/// resource exists (see [`validate_export_names`]), so nothing but parameters
/// goes into resolving it.
pub fn export_names(template_body: &str, parameters: &BTreeMap<String, String>) -> Vec<String> {
    let Some(template) = expanded_template(template_body, parameters) else {
        return Vec::new();
    };
    let Some(outputs) = template.get("Outputs").and_then(|v| v.as_object()) else {
        return Vec::new();
    };
    let Ok(conditions) = evaluate_conditions(&template, parameters) else {
        return Vec::new();
    };
    let no_resources = serde_json::Map::new();
    let (no_ids, no_attributes, no_imports) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
    outputs
        .values()
        .filter(
            |body| match body.get("Condition").and_then(|v| v.as_str()) {
                Some(name) => conditions.get(name).copied().unwrap_or(false),
                None => true,
            },
        )
        .filter_map(|body| body.get("Export")?.get("Name"))
        .map(|name| {
            match resolve_refs_full(
                name,
                parameters,
                &no_resources,
                &no_ids,
                &no_attributes,
                &no_imports,
                &conditions,
            ) {
                Value::String(text) => text,
                other => other.to_string(),
            }
        })
        .collect()
}

/// `Fn::Sub`'s `${Name}` / `${Name.Attr}` references, without `${!Literal}`.
fn sub_references(text: &str) -> Vec<&str> {
    text.split("${")
        .skip(1)
        .filter_map(|tail| tail.split_once('}').map(|(reference, _)| reference))
        .filter(|reference| !reference.starts_with('!'))
        .map(|reference| reference.split('.').next().unwrap_or(reference))
        .collect()
}

fn depends_on_resources(value: &Value, resources: &serde_json::Map<String, Value>) -> bool {
    match value {
        Value::Object(map) => {
            if map.contains_key("Fn::GetAtt")
                || map.contains_key("Fn::ImportValue")
                || map.contains_key("Fn::GetAZs")
            {
                return true;
            }
            if map
                .get("Ref")
                .and_then(|v| v.as_str())
                .is_some_and(|name| resources.contains_key(name))
            {
                return true;
            }
            if let Some(sub) = map.get("Fn::Sub") {
                let (text, variables) = match sub {
                    Value::String(text) => (text.as_str(), None),
                    Value::Array(items) => (
                        items.first().and_then(|v| v.as_str()).unwrap_or(""),
                        items.get(1).and_then(|v| v.as_object()),
                    ),
                    _ => ("", None),
                };
                let names_resource = sub_references(text).into_iter().any(|name| {
                    resources.contains_key(name)
                        && !variables.is_some_and(|vars| vars.contains_key(name))
                });
                if names_resource {
                    return true;
                }
                return variables
                    .is_some_and(|vars| vars.values().any(|v| depends_on_resources(v, resources)));
            }
            map.values().any(|v| depends_on_resources(v, resources))
        }
        Value::Array(items) => items.iter().any(|v| depends_on_resources(v, resources)),
        _ => false,
    }
}

/// Refuse an output whose `Export.Name` depends on a resource, an imported value
/// or `Fn::GetAZs`, which CloudFormation rejects up front. It is what makes an
/// export collision knowable before anything is provisioned.
pub fn validate_export_names(
    template_body: &str,
    parameters: &BTreeMap<String, String>,
) -> Result<(), String> {
    let Some(template) = expanded_template(template_body, parameters) else {
        return Ok(());
    };
    let Some(outputs) = template.get("Outputs").and_then(|v| v.as_object()) else {
        return Ok(());
    };
    let no_resources = serde_json::Map::new();
    let resources = template
        .get("Resources")
        .and_then(|v| v.as_object())
        .unwrap_or(&no_resources);
    for (logical_id, body) in outputs {
        let name = body.get("Export").and_then(|export| export.get("Name"));
        if name.is_some_and(|name| depends_on_resources(name, resources)) {
            // Two spaces after "malformed." as CloudFormation prints it.
            return Err(format!(
                "Template format error: Output {logical_id} is malformed.  The Name field of Export must not depend on any resources, imported values, or Fn::GetAZs."
            ));
        }
    }
    Ok(())
}

/// Parse the template's `Outputs` block into resolved entries. Each
/// `Value` is fully resolved (Ref / GetAtt / Sub / Join / Fn::ImportValue)
/// to a string. Imports use `imports` for cross-stack lookups.
pub fn parse_outputs(
    template: &Value,
    parameters: &BTreeMap<String, String>,
    resources: &serde_json::Map<String, Value>,
    resource_physical_ids: &BTreeMap<String, String>,
    resource_attributes: &BTreeMap<String, BTreeMap<String, String>>,
    imports: &BTreeMap<String, String>,
) -> Result<Vec<TemplateOutput>, String> {
    // Expand Fn::ForEach in Outputs so resolve picks up macro-emitted
    // entries. Callers pass the raw template value, which may still
    // contain unexpanded ForEach macros.
    let template_owned = expand_for_each(template, &BTreeMap::new(), parameters)?;
    // The SAM transform resolves `Ref: MyFunction.Alias` / `MyApi.Stage`
    // style references to the resources it generates.
    let template_owned = expand_sam(&template_owned)?;
    let template = &template_owned;
    let outputs_obj = match template.get("Outputs").and_then(|v| v.as_object()) {
        Some(o) => o,
        None => return Ok(Vec::new()),
    };

    let conditions = evaluate_conditions(template, parameters)?;
    let mut out = Vec::new();
    for (logical_id, body) in outputs_obj {
        // Skip outputs gated on a Condition that resolves false. CFN
        // simply omits these from the resolved Outputs set.
        if let Some(cond_name) = body.get("Condition").and_then(|v| v.as_str()) {
            if !conditions.get(cond_name).copied().unwrap_or(false) {
                continue;
            }
        }
        let raw_value = match body.get("Value") {
            Some(v) => v,
            None => continue,
        };
        let resolved = resolve_refs_full(
            raw_value,
            parameters,
            resources,
            resource_physical_ids,
            resource_attributes,
            imports,
            &conditions,
        );
        let resolved = strip_no_value(resolved);
        let value = match resolved {
            Value::String(s) => s,
            other => other.to_string(),
        };
        let description = body
            .get("Description")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let export_name = body.get("Export").and_then(|e| e.get("Name")).map(|n| {
            let resolved = resolve_refs_full(
                n,
                parameters,
                resources,
                resource_physical_ids,
                resource_attributes,
                imports,
                &conditions,
            );
            match resolved {
                Value::String(s) => s,
                other => other.to_string(),
            }
        });
        out.push(TemplateOutput {
            logical_id: logical_id.clone(),
            value,
            description,
            export_name,
        });
    }
    Ok(out)
}
