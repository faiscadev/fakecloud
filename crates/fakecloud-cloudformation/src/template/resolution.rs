//! `resolution` concerns from template.rs (audit-2026-05-19).

use super::*;

/// Re-resolve a single resource definition's properties with updated physical IDs.
pub fn resolve_resource_properties(
    resource: &ResourceDefinition,
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    resource_physical_ids: &BTreeMap<String, String>,
) -> Result<ResourceDefinition, String> {
    resolve_resource_properties_with_attrs(
        resource,
        template_body,
        parameters,
        resource_physical_ids,
        &BTreeMap::new(),
    )
}

/// Re-resolve a single resource definition's properties with updated physical
/// IDs and attribute values for `Fn::GetAtt`.
pub fn resolve_resource_properties_with_attrs(
    resource: &ResourceDefinition,
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    resource_physical_ids: &BTreeMap<String, String>,
    resource_attributes: &BTreeMap<String, BTreeMap<String, String>>,
) -> Result<ResourceDefinition, String> {
    let value: Value = if template_body.trim_start().starts_with('{') {
        serde_json::from_str(template_body).map_err(|e| format!("Invalid JSON template: {e}"))?
    } else {
        serde_yaml::from_str(template_body).map_err(|e| format!("Invalid YAML template: {e}"))?
    };
    // Re-expand ForEach so the resource we look up matches the post-
    // expansion logical IDs from the original parse.
    let value = expand_for_each(&value, &BTreeMap::new(), parameters)?;
    // Re-apply the SAM transform too: the original parse rewrote SAM
    // resources (e.g. AWS::Serverless::StateMachine -> the native
    // AWS::StepFunctions::StateMachine, Role -> RoleArn). Without this the
    // properties get re-read from the raw SAM resource and the native
    // provisioner sees SAM property names it doesn't understand.
    let value = expand_sam(&value);

    let resources_obj = value
        .get("Resources")
        .and_then(|v| v.as_object())
        .ok_or("Template must contain a Resources section")?;

    let raw_props = resources_obj
        .get(&resource.logical_id)
        .and_then(|r| r.get("Properties"))
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()));

    // Re-evaluate Conditions / Mappings on every resolve so Fn::If picks
    // the right branch and AWS::NoValue still strips at incremental
    // provisioning time. Without this, the sentinel would leak into the
    // provisioned property map.
    let conditions = evaluate_conditions(&value, parameters)?;
    let mappings = parse_mappings(&value);
    let raw_props = apply_mappings(&raw_props, parameters, &mappings, &conditions)?;

    let resolved = resolve_refs_full(
        &raw_props,
        parameters,
        resources_obj,
        resource_physical_ids,
        resource_attributes,
        &BTreeMap::new(),
        &conditions,
    );
    let resolved = strip_no_value(resolved);

    Ok(ResourceDefinition {
        logical_id: resource.logical_id.clone(),
        resource_type: resource.resource_type.clone(),
        properties: resolved,
    })
}
