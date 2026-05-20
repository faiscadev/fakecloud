//! probe `followup` (audit-2026-05-19).

use super::*;

/// After a Create/Put/Update variant succeeds, fire its discovered
/// Get/Describe followup and assert each input field that was set on the
/// Create echoes through the Get response. Returns a list of violations
/// (empty if everything echoed cleanly or the followup couldn't be
/// reached for benign reasons; reports HTTP errors as a single violation
/// so harness operators can spot pairs that need bespoke handling).
/// Return the names of all `@required` members on the operation's input
/// shape. Used by the round-trip followup to discover which fields the
/// reader needs that aren't shared by name with the writer's input.
pub(super) fn reader_required_inputs(model: &ServiceModel, operation_name: &str) -> Vec<String> {
    use crate::smithy::ShapeType;
    let op = match model.operations.iter().find(|o| o.name == operation_name) {
        Some(op) => op,
        None => return Vec::new(),
    };
    let input_id = match op.input_shape.as_deref() {
        Some(id) => id,
        None => return Vec::new(),
    };
    let input_shape = match model.shapes.get(input_id) {
        Some(s) => s,
        None => return Vec::new(),
    };
    match &input_shape.shape_type {
        ShapeType::Structure { members } => members
            .iter()
            .filter(|m| m.required)
            .map(|m| m.name.clone())
            .collect(),
        _ => Vec::new(),
    }
}

pub(super) fn run_round_trip_followup(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    create_variant: &TestVariant,
    create_response: Option<&serde_json::Value>,
    followup: &crate::generators::RoundTripFollowup,
    model: &ServiceModel,
) -> Vec<shape_validator::ShapeViolation> {
    let mut violations = Vec::new();

    // The resource identifier we used on Create is the same value we
    // need to feed into Get/Describe. Read it straight off the variant's
    // own input — no need to parse the Create response, which avoids
    // false negatives on services whose Create output wraps the
    // identifier in a sub-structure.
    let create_obj = match create_variant.input.as_object() {
        Some(o) => o,
        None => return violations,
    };
    let id_value = match create_obj.get(&followup.id_field) {
        Some(v) if !v.is_null() => v.clone(),
        _ => return violations,
    };

    let mut get_input = serde_json::Map::new();
    get_input.insert(followup.id_field.clone(), id_value);
    // Forward every shared identifier so the follow-up Get can resolve a
    // multi-segment resource (e.g. `restApiId` + `resourceId` +
    // `httpMethod` on API Gateway v1's `GetMethod`). Skip any that the
    // writer didn't actually supply — keeps the legacy single-id flow
    // intact for services where the writer's input only has one shared
    // identifier member.
    for extra in &followup.id_fields {
        if extra == &followup.id_field {
            continue;
        }
        if let Some(v) = create_obj.get(extra) {
            if !v.is_null() {
                get_input.insert(extra.clone(), v.clone());
            }
        }
    }

    // Many APIs name the new resource's identifier differently on the
    // writer's input vs. the reader's input (e.g. `CreateModel.name` ->
    // `GetModel.modelName`, `CreateAuthorizer` returns `id` ->
    // `GetAuthorizer.authorizerId`). When the Create response carries
    // that identifier as a field, look up each of the reader's required
    // members in the response and fill from there. Conservative match:
    // exact match first, then `<reader_member>` resolves to either
    // `id` (the canonical generated identifier slot) or the reader's
    // shape suffix (`modelName` -> `name`).
    if let Some(create_response_obj) = create_response.and_then(|v| v.as_object()) {
        let reader_required = reader_required_inputs(model, &followup.get_operation);
        for member in reader_required {
            if get_input.contains_key(&member) {
                continue;
            }
            // Look up the reader-input member name in the writer's response.
            // restJson1 services with `@jsonName` (apigatewayv2 in particular)
            // declare PascalCase member names but serialise camelCase on the
            // wire. Try the raw member name first, then the lowercase-first
            // alias, so the followup id-fill works under either casing.
            if let Some(v) = lookup_field_any_case(create_response_obj, &member) {
                if !v.is_null() {
                    get_input.insert(member.clone(), v.clone());
                    continue;
                }
            }
            // `modelName` -> Create response `name`; same for other
            // <Resource>Name patterns where the response carries `name`.
            if member.ends_with("Name") {
                if let Some(v) = lookup_field_any_case(create_response_obj, "Name") {
                    if !v.is_null() {
                        get_input.insert(member.clone(), v.clone());
                        continue;
                    }
                }
            }
            // `authorizerId`, `vpcLinkId`, ... -> Create response `id`.
            if member.ends_with("Id") {
                if let Some(v) = lookup_field_any_case(create_response_obj, "Id") {
                    if !v.is_null() {
                        get_input.insert(member.clone(), v.clone());
                    }
                }
            }
        }
    }
    let get_variant = TestVariant {
        name: format!("{}__followup_get", create_variant.name),
        strategy: crate::generators::Strategy::RoundTrip,
        input: serde_json::Value::Object(get_input),
        expectation: crate::generators::Expectation::Success,
        expected_output: None,
        followup: None,
    };

    // Resolve the Get op's output shape so the recursive probe can keep
    // shape validation on. Without it the followup is still useful (we
    // still echo-check) but skipped if the op isn't in the model.
    let get_op = match model
        .operations
        .iter()
        .find(|o| o.name == followup.get_operation)
    {
        Some(op) => op,
        None => return violations,
    };
    let get_output_shape = match get_op.output_shape.as_deref() {
        Some(s) => s,
        None => return violations,
    };

    // Recurse via the public probe entry. The Get variant has no
    // `followup`, so this terminates after one extra hop.
    let get_result = probe_variant_with_model(
        client,
        endpoint,
        service_name,
        &followup.get_operation,
        &get_variant,
        Some((model, get_output_shape)),
    );

    // Only echo-check on a clean 2xx with a parseable body. A 4xx/5xx on
    // the followup is its own signal — surface as a single violation
    // rather than fabricate a misleading echo failure.
    if !(200..300).contains(&get_result.http_status) {
        violations.push(shape_validator::ShapeViolation::RoundTripFieldNotEchoed {
            field: format!("(followup {})", followup.get_operation),
            sent: serde_json::Value::String(format!("HTTP {}", get_result.http_status)),
            received: None,
        });
        return violations;
    }
    let get_body: serde_json::Value = match serde_json::from_str(&get_result.response_body) {
        Ok(v) => v,
        Err(_) => return violations,
    };

    // Pull each echo field from the Create variant's input and compare
    // against the Get output.
    for (input_field, output_field) in &followup.echo_fields {
        let sent = match create_obj.get(input_field) {
            Some(v) => v,
            None => continue,
        };
        if let Some(v) = shape_validator::echo_check(output_field, sent, &get_body) {
            violations.push(v);
        }
    }
    violations
}
