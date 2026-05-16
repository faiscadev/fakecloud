//! Strategy 7: auto-discovered Create/Put/Update -> Get/Describe round-trip echo.
//!
//! Walks the Smithy graph at variant-generation time to pair every
//! mutating operation with its matching read operation (by structural
//! identifier match — see `smithy::find_round_trip_pairs`). For each pair
//! the strategy picks one optional input field on the writer whose name
//! also appears on the reader's output, and emits a `TestVariant` that
//! sets that field. The probe layer then chases the variant by sending
//! Get/Describe with the writer's returned identifier and asserts the
//! optional value made the round-trip — catches silent-input-drop bugs
//! like #853 (Lambda `Layers` dropped on CreateFunction).

use serde_json::Value;
use std::collections::HashMap;

use super::{
    build_required_input, default_value_for_shape, Expectation, RoundTripFollowup, Strategy,
    TestVariant,
};
use crate::smithy::{
    effective_shape_type, find_round_trip_pairs, primitive_compatible, Member, RoundTripPair,
    ServiceModel, ShapeType,
};

pub fn generate(
    model: &ServiceModel,
    operation_name: &str,
    overrides: &HashMap<String, Value>,
) -> Vec<TestVariant> {
    // The strategy is keyed on the *writer* op (the variant is for Create);
    // the followup runs Get inside `probe::probe_variant_with_model`.
    let pair = match find_pair_for_writer(model, operation_name) {
        Some(p) => p,
        None => return Vec::new(),
    };

    let writer_input_id = match pair.writer.input_shape.as_deref() {
        Some(id) => id,
        None => return Vec::new(),
    };
    let reader_output_id = match pair.reader.output_shape.as_deref() {
        Some(id) => id,
        None => return Vec::new(),
    };

    let writer_input_members = structure_members(model, writer_input_id);
    let reader_output_members = structure_members(model, reader_output_id);

    // Find an optional input field whose name + target shape also appears
    // on the reader's output. That's the field whose round-trip we'll
    // assert. Skip the identifier itself — it's already being validated
    // by virtue of the followup Get succeeding.
    let (echo_field, echo_output_member) = match writer_input_members.iter().find_map(|wm| {
        if wm.required || wm.name == pair.id_source_field {
            return None;
        }
        let rm = reader_output_members
            .iter()
            .find(|rm| rm.name == wm.name && primitive_compatible(model, &rm.target, &wm.target))?;
        // Structure / union echo only makes sense when the writer's input
        // shape is identical to the reader's output shape — services often
        // declare distinct request/response shapes for the same field
        // (e.g. apigatewayv2's `EndpointDisplayContent` input vs.
        // `EndpointDisplayContentResponse` output). Skip those: the
        // strategy can't honestly assert echo across shape transforms.
        let same_struct = matches!(
            effective_shape_type(model, &wm.target),
            Some(
                crate::smithy::ShapeType::Structure { .. } | crate::smithy::ShapeType::Union { .. }
            )
        );
        if same_struct && wm.target != rm.target {
            return None;
        }
        Some((wm, rm))
    }) {
        Some(pair) => pair,
        None => return Vec::new(),
    };
    // Reader output's wire key. restJson1 services (notably apigatewayv2)
    // declare PascalCase member names with `@jsonName("camelCase")` so the
    // actual response key on the wire is camelCase. Use the resolved wire
    // name when fetching the echoed value out of the Get response.
    let echo_output_wire = echo_output_member
        .traits
        .json_name
        .clone()
        .unwrap_or_else(|| echo_output_member.name.clone());

    let mut input = build_required_input(model, writer_input_id, overrides);
    if let Value::Object(ref mut obj) = input {
        let echo_value = optional_field_value(model, echo_field);
        obj.insert(echo_field.name.clone(), echo_value);
        // Concurrency-isolate this variant: every required string input
        // gets a unique identifier so parallel variants targeting the
        // same writer can't clobber each other's stored state. Without
        // this, the round_trip's GET could read a sibling variant's
        // PUT and report a spurious echo drop. Only rewrite string-
        // typed required members so we don't trample fields with
        // tighter formats (numbers, enums, etc.).
        let isolation_tag = format!("rt-{}", echo_field.name);
        for wm in writer_input_members.iter().filter(|m| m.required) {
            let is_plain_string = matches!(
                effective_shape_type(model, &wm.target),
                Some(crate::smithy::ShapeType::String { enum_values: None }),
            );
            if !is_plain_string {
                continue;
            }
            obj.insert(wm.name.clone(), Value::String(isolation_tag.clone()));
        }
    }

    // Every input member that also appears on the reader's input is part
    // of the resource's composite identifier — REST APIs in particular
    // model multi-segment URIs (`/restapis/{r}/resources/{x}/methods/{m}`)
    // and the follow-up Get cannot succeed without all of them. Forward
    // every shared identifier from the writer's input so the probe drives
    // the reader the way a real SDK would.
    let reader_input_members =
        structure_members(model, pair.reader.input_shape.as_deref().unwrap_or(""));
    let mut id_fields: Vec<String> = writer_input_members
        .iter()
        .filter(|wm| {
            reader_input_members
                .iter()
                .any(|rm| rm.name == wm.name && primitive_compatible(model, &rm.target, &wm.target))
        })
        .map(|m| m.name.clone())
        .collect();
    // Always include the canonical id_source_field even when the model
    // ordering puts it last so the legacy single-id consumers keep
    // working.
    if !id_fields.contains(&pair.id_source_field) {
        id_fields.push(pair.id_source_field.clone());
    }

    let followup = RoundTripFollowup {
        get_operation: pair.reader.name.clone(),
        id_field: pair.id_source_field.clone(),
        id_fields,
        echo_fields: vec![(echo_field.name.clone(), echo_output_wire)],
    };

    vec![TestVariant {
        name: format!("round_trip_{}", echo_field.name),
        strategy: Strategy::RoundTrip,
        input,
        expectation: Expectation::Success,
        expected_output: None,
        followup: Some(followup),
    }]
}

fn find_pair_for_writer(model: &ServiceModel, writer_name: &str) -> Option<RoundTripPair> {
    find_round_trip_pairs(model)
        .into_iter()
        .find(|p| p.writer.name == writer_name)
}

fn structure_members(model: &ServiceModel, shape_id: &str) -> Vec<Member> {
    match effective_shape_type(model, shape_id) {
        Some(ShapeType::Structure { members }) => members,
        _ => Vec::new(),
    }
}

/// Pick a non-default value for an optional field. Use the schema's
/// `default_value_for_shape`; for collections, populate one element so the
/// round-trip assertion has something to compare. A bare `[]` would echo
/// `[]` even on a service that drops the field on the floor (#853 was
/// invisible precisely because a missing optional looks like an empty
/// optional in JSON).
fn optional_field_value(model: &ServiceModel, member: &Member) -> Value {
    let target_type = effective_shape_type(model, &member.target);
    match target_type {
        Some(ShapeType::List { member_target }) => {
            Value::Array(vec![default_value_for_shape(model, &member_target, 0)])
        }
        Some(ShapeType::Map {
            key_target: _,
            value_target,
        }) => {
            let mut obj = serde_json::Map::new();
            obj.insert(
                "k".to_string(),
                default_value_for_shape(model, &value_target, 0),
            );
            Value::Object(obj)
        }
        _ => default_value_for_shape(model, &member.target, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smithy::{Operation, Shape, ShapeTraits};

    fn op(
        name: &str,
        input: Option<&str>,
        output: Option<&str>,
        method: Option<&str>,
        uri: Option<&str>,
    ) -> Operation {
        Operation {
            name: name.to_string(),
            input_shape: input.map(String::from),
            output_shape: output.map(String::from),
            error_shapes: Vec::new(),
            http_method: method.map(String::from),
            http_uri: uri.map(String::from),
            http_code: None,
        }
    }

    fn structure(id: &str, members: Vec<Member>) -> Shape {
        Shape {
            shape_id: id.to_string(),
            shape_type: ShapeType::Structure { members },
            traits: ShapeTraits::default(),
        }
    }

    fn member(name: &str, target: &str, required: bool) -> Member {
        Member {
            name: name.to_string(),
            target: target.to_string(),
            required,
            traits: ShapeTraits::default(),
        }
    }

    fn lambda_layers_model() -> ServiceModel {
        // Mirrors the #853 scenario in miniature:
        // CreateFunction(FunctionName, Layers?) -> { FunctionArn }
        // GetFunctionConfiguration(FunctionName) -> { FunctionArn, FunctionName, Layers? }
        let mut shapes = std::collections::HashMap::new();

        shapes.insert(
            "test#StringList".to_string(),
            Shape {
                shape_id: "test#StringList".to_string(),
                shape_type: ShapeType::List {
                    member_target: "smithy.api#String".to_string(),
                },
                traits: ShapeTraits::default(),
            },
        );

        shapes.insert(
            "test#CreateFunctionRequest".to_string(),
            structure(
                "test#CreateFunctionRequest",
                vec![
                    member("FunctionName", "smithy.api#String", true),
                    member("Layers", "test#StringList", false),
                ],
            ),
        );
        shapes.insert(
            "test#CreateFunctionResponse".to_string(),
            structure(
                "test#CreateFunctionResponse",
                vec![
                    member("FunctionName", "smithy.api#String", false),
                    member("FunctionArn", "smithy.api#String", false),
                ],
            ),
        );
        shapes.insert(
            "test#GetFunctionConfigurationRequest".to_string(),
            structure(
                "test#GetFunctionConfigurationRequest",
                vec![member("FunctionName", "smithy.api#String", true)],
            ),
        );
        shapes.insert(
            "test#GetFunctionConfigurationResponse".to_string(),
            structure(
                "test#GetFunctionConfigurationResponse",
                vec![
                    member("FunctionName", "smithy.api#String", false),
                    member("Layers", "test#StringList", false),
                ],
            ),
        );

        ServiceModel {
            service_name: "test".to_string(),
            operations: vec![
                op(
                    "CreateFunction",
                    Some("test#CreateFunctionRequest"),
                    Some("test#CreateFunctionResponse"),
                    Some("POST"),
                    Some("/functions"),
                ),
                op(
                    "GetFunctionConfiguration",
                    Some("test#GetFunctionConfigurationRequest"),
                    Some("test#GetFunctionConfigurationResponse"),
                    Some("GET"),
                    Some("/functions/{FunctionName}/configuration"),
                ),
            ],
            shapes,
        }
    }

    #[test]
    fn discovers_create_get_pair_and_emits_round_trip_variant() {
        let model = lambda_layers_model();
        let variants = generate(&model, "CreateFunction", &HashMap::new());
        assert_eq!(variants.len(), 1, "expected one round-trip variant");
        let v = &variants[0];
        assert_eq!(v.strategy, Strategy::RoundTrip);
        let f = v.followup.as_ref().expect("followup must be set");
        assert_eq!(f.get_operation, "GetFunctionConfiguration");
        assert_eq!(f.id_field, "FunctionName");
        assert_eq!(
            f.echo_fields,
            vec![("Layers".to_string(), "Layers".to_string())]
        );
        // input has the required FunctionName + the optional Layers populated
        // with one element so the echo check has something to compare.
        let obj = v.input.as_object().expect("object input");
        assert!(obj.contains_key("FunctionName"));
        let layers = obj
            .get("Layers")
            .expect("Layers populated")
            .as_array()
            .unwrap();
        assert_eq!(layers.len(), 1);
    }

    #[test]
    fn skips_when_no_echoable_optional_exists() {
        // Writer has only required fields → no optional to round-trip on.
        let mut shapes = std::collections::HashMap::new();
        shapes.insert(
            "test#PutThingRequest".to_string(),
            structure(
                "test#PutThingRequest",
                vec![member("Id", "smithy.api#String", true)],
            ),
        );
        shapes.insert(
            "test#PutThingResponse".to_string(),
            structure(
                "test#PutThingResponse",
                vec![member("Id", "smithy.api#String", false)],
            ),
        );
        shapes.insert(
            "test#GetThingRequest".to_string(),
            structure(
                "test#GetThingRequest",
                vec![member("Id", "smithy.api#String", true)],
            ),
        );
        shapes.insert(
            "test#GetThingResponse".to_string(),
            structure(
                "test#GetThingResponse",
                vec![member("Id", "smithy.api#String", false)],
            ),
        );
        let model = ServiceModel {
            service_name: "test".to_string(),
            operations: vec![
                op(
                    "PutThing",
                    Some("test#PutThingRequest"),
                    Some("test#PutThingResponse"),
                    None,
                    None,
                ),
                op(
                    "GetThing",
                    Some("test#GetThingRequest"),
                    Some("test#GetThingResponse"),
                    None,
                    None,
                ),
            ],
            shapes,
        };
        let variants = generate(&model, "PutThing", &HashMap::new());
        assert!(variants.is_empty());
    }

    #[test]
    fn skips_when_no_reader_exists() {
        let mut shapes = std::collections::HashMap::new();
        shapes.insert(
            "test#CreateOrphanRequest".to_string(),
            structure(
                "test#CreateOrphanRequest",
                vec![member("Name", "smithy.api#String", true)],
            ),
        );
        shapes.insert(
            "test#CreateOrphanResponse".to_string(),
            structure(
                "test#CreateOrphanResponse",
                vec![member("Name", "smithy.api#String", false)],
            ),
        );
        let model = ServiceModel {
            service_name: "test".to_string(),
            operations: vec![op(
                "CreateOrphan",
                Some("test#CreateOrphanRequest"),
                Some("test#CreateOrphanResponse"),
                None,
                None,
            )],
            shapes,
        };
        assert!(generate(&model, "CreateOrphan", &HashMap::new()).is_empty());
    }
}
