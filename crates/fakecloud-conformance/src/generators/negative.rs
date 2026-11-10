//! Strategy 6: Negative testing.
//!
//! For each action:
//! - Omit each required field one at a time → expect validation error
//! - Out-of-range values for constrained fields → expect error
//! - Invalid enum values → expect error

use serde_json::Value;
use std::collections::HashMap;

use super::{build_required_input, default_value_for_shape, Expectation, Strategy, TestVariant};
use crate::smithy::{is_prelude_shape, ServiceModel, ShapeType};

/// `negative_omit_<field>` for required list/map members is wire-equivalent
/// to submitting the field with an empty collection: AWS query and REST
/// protocols serialise empty arrays/maps as zero wire params. The server
/// can't tell the difference, so the negative case can't observably fail.
/// Skip emitting the variant rather than asking the server to do something
/// impossible.
///
/// The same is true for a required *structure* whose members are all optional:
/// an empty struct serialises to zero wire params, indistinguishable from
/// omission. (A struct with at least one required member IS observable — the
/// required sub-fields appear on the wire when present, vanish when omitted.)
fn omission_is_wire_observable(model: &ServiceModel, member_target: &str) -> bool {
    if is_prelude_shape(member_target) {
        return true;
    }
    match model.shapes.get(member_target) {
        Some(shape) => match &shape.shape_type {
            ShapeType::List { .. } | ShapeType::Map { .. } => false,
            ShapeType::Structure { members } | ShapeType::Union { members } => {
                members.iter().any(|m| m.required)
            }
            _ => true,
        },
        None => true,
    }
}

/// Whether an input member carries an HTTP wire binding (`@httpQuery`,
/// `@httpLabel`, `@httpHeader`, or `@httpPayload`) on either the member or its
/// target shape. Constraint-violation negatives on a member with no binding are
/// only reachable when the operation serialises a request body; on a bodyless
/// method (`GET`/`HEAD`/`DELETE`) neither an AWS SDK nor the probe can place the
/// value on the wire, so the negative is unobservable by construction.
fn member_is_wire_bound(model: &ServiceModel, member: &crate::smithy::Member) -> bool {
    let mt = &member.traits;
    if mt.http_label || mt.http_query.is_some() || mt.http_header.is_some() || mt.http_payload {
        return true;
    }
    match model.shapes.get(&member.target).map(|s| &s.traits) {
        Some(tt) => {
            tt.http_label || tt.http_query.is_some() || tt.http_header.is_some() || tt.http_payload
        }
        None => false,
    }
}

/// Whether an empty value for the `@httpLabel` member `label` collapses the
/// request onto a *different* modeled operation. True when `label` is the
/// final segment of `http_uri` and the URI with that segment removed is bound
/// to another operation of the same model with the same HTTP method (e.g.
/// `GET /foundation-models/{modelIdentifier}` with `""` is the wire request
/// `GET /foundation-models/`, which is ListFoundationModels).
///
/// Such a too-short negative is unsendable by construction: Smithy clients
/// reject an empty `@httpLabel` before serializing, and the bytes the probe
/// would put on the wire ARE the other operation's request, so the server
/// can only answer as that operation.
fn empty_label_collapses_to_other_op(
    model: &ServiceModel,
    http_method: Option<&str>,
    http_uri: Option<&str>,
    label: &str,
) -> bool {
    let (Some(method), Some(uri)) = (http_method, http_uri) else {
        return false;
    };
    let (path, query) = split_uri_template(uri);
    let Some(parent) = path
        .strip_suffix(&format!("/{{{label}}}"))
        .or_else(|| path.strip_suffix(&format!("/{{{label}+}}")))
    else {
        return false;
    };
    // The collapsed request must be byte-identical to the other operation's:
    // same method, same path, and the same constant query literals (S3's
    // `POST /{Bucket}/{Key+}?uploads` with an empty key is NOT
    // `POST /{Bucket}?delete`).
    model.operations.iter().any(|op| {
        let same_method = op
            .http_method
            .as_deref()
            .is_some_and(|m| m.eq_ignore_ascii_case(method));
        same_method
            && op.http_uri.as_deref().is_some_and(|u| {
                let (op_path, op_query) = split_uri_template(u);
                op_path.trim_end_matches('/') == parent.trim_end_matches('/') && op_query == query
            })
    })
}

/// Split a Smithy `@http` URI template into its path and its sorted constant
/// query literals (`?uploads&x-id=GetObject` -> `["uploads", "x-id=GetObject"]`).
fn split_uri_template(uri: &str) -> (&str, Vec<&str>) {
    match uri.split_once('?') {
        Some((path, query)) => {
            let mut literals: Vec<&str> = query.split('&').filter(|q| !q.is_empty()).collect();
            literals.sort_unstable();
            (path, literals)
        }
        None => (uri, Vec::new()),
    }
}

pub fn generate(
    model: &ServiceModel,
    input_shape_id: &str,
    overrides: &HashMap<String, Value>,
    http_method: Option<&str>,
    http_uri: Option<&str>,
) -> Vec<TestVariant> {
    let mut variants = Vec::new();

    // A `GET`/`HEAD`/`DELETE` operation carries no request body, so an unbound
    // constrained member (no `@httpQuery`/`@httpLabel`/`@httpHeader`/
    // `@httpPayload`) has nowhere to travel — its constraint-violation negatives
    // are unreachable and must not be emitted (mirroring the
    // `omission_is_wire_observable` skip above).
    let bodyless_method = matches!(
        http_method.map(str::to_ascii_uppercase).as_deref(),
        Some("GET") | Some("HEAD") | Some("DELETE")
    );

    let members = super::get_members(model, input_shape_id);

    // Omit each required field one at a time
    let required_members: Vec<_> = members.iter().filter(|m| m.required).collect();
    for omit_member in &required_members {
        if !omission_is_wire_observable(model, &omit_member.target) {
            continue;
        }
        let mut obj = serde_json::Map::new();
        for member in &required_members {
            if member.name == omit_member.name {
                continue; // Skip this one
            }
            if let Some(override_val) = overrides.get(&member.name) {
                obj.insert(member.name.clone(), override_val.clone());
            } else {
                let val = default_value_for_shape(model, &member.target, 0);
                obj.insert(member.name.clone(), val);
            }
        }
        variants.push(TestVariant {
            name: format!("negative_omit_{}", omit_member.name),
            strategy: Strategy::Negative,
            input: Value::Object(obj),
            expectation: Expectation::AnyError,
            expected_output: None,
            followup: None,
        });
    }

    // Out-of-range values for constrained fields
    for member in members {
        let shape = match model.shapes.get(&member.target) {
            Some(s) => s,
            None => continue,
        };

        // On a bodyless method, an unbound member's constraint negatives are
        // unreachable (see `bodyless_method` above) — skip them.
        if bodyless_method && !member_is_wire_bound(model, member) {
            continue;
        }

        let traits = &shape.traits;

        // String too short (below min length).
        //
        // `default_value_for_shape` caps positive-variant string fillers at
        // 20 chars. When `length_min` exceeds 20 the positive default is
        // already shorter than `min - 1`, so the server can't validate the
        // negative case without also rejecting every legitimate positive
        // call. Skip the variant rather than emit one that's unreachable
        // by construction.
        // An empty (`min == 1`) value for a trailing `@httpLabel` whose
        // collapsed URI is another operation's route is that operation's
        // request on the wire; see `empty_label_collapses_to_other_op`.
        let is_label = member.traits.http_label || traits.http_label;
        let collapses = traits.length_min == Some(1)
            && is_label
            && empty_label_collapses_to_other_op(model, http_method, http_uri, &member.name);
        if let Some(min) = traits.length_min.filter(|_| !collapses) {
            if min > 0 && (min as usize) <= 20 {
                if let ShapeType::String { .. } = &shape.shape_type {
                    let mut input = build_required_input(model, input_shape_id, overrides);
                    if let Value::Object(ref mut obj) = input {
                        // Use a string shorter than min
                        let short = "a".repeat((min as usize).saturating_sub(1));
                        obj.insert(member.name.clone(), Value::String(short));
                    }
                    variants.push(TestVariant {
                        name: format!("negative_too_short_{}", member.name),
                        strategy: Strategy::Negative,
                        input,
                        expectation: Expectation::AnyError,
                        expected_output: None,
                        followup: None,
                    });
                }
            }
        }

        // String too long (above max length)
        if let Some(max) = traits.length_max {
            // A member bound to the URL (`@httpLabel` / `@httpQuery`) carries its
            // too-long value in the request URI. `http::Uri` caps the whole URI
            // at ~64 KiB, so a `max + 1` string for a member whose `@length` max
            // approaches that limit produces a URI no HTTP client can build --
            // the request is unsendable by construction (mirroring the
            // `omission_is_wire_observable` / `bodyless_method` skips above).
            // Skip it rather than emit a variant that can only ever CRASH on the
            // client side.
            let url_bound = member.traits.http_label || member.traits.http_query.is_some() || {
                model
                    .shapes
                    .get(&member.target)
                    .map(|s| s.traits.http_label || s.traits.http_query.is_some())
                    .unwrap_or(false)
            };
            let uri_safe = !url_bound || (max as usize) < 65_000;
            if max < 100000 && uri_safe {
                // Don't generate absurdly large strings
                if let ShapeType::String { .. } = &shape.shape_type {
                    let mut input = build_required_input(model, input_shape_id, overrides);
                    if let Value::Object(ref mut obj) = input {
                        let long = "a".repeat(max as usize + 1);
                        obj.insert(member.name.clone(), Value::String(long));
                    }
                    variants.push(TestVariant {
                        name: format!("negative_too_long_{}", member.name),
                        strategy: Strategy::Negative,
                        input,
                        expectation: Expectation::AnyError,
                        expected_output: None,
                        followup: None,
                    });
                }
            }
        }

        // Integer below min range
        if let Some(min) = traits.range_min {
            match &shape.shape_type {
                ShapeType::Integer | ShapeType::Long => {
                    if let Some(below) = (min as i64).checked_sub(1) {
                        let mut input = build_required_input(model, input_shape_id, overrides);
                        if let Value::Object(ref mut obj) = input {
                            obj.insert(member.name.clone(), Value::Number(below.into()));
                        }
                        variants.push(TestVariant {
                            name: format!("negative_below_min_{}", member.name),
                            strategy: Strategy::Negative,
                            input,
                            expectation: Expectation::AnyError,
                            expected_output: None,
                            followup: None,
                        });
                    }
                }
                _ => {}
            }
        }

        // Integer above max range
        if let Some(max) = traits.range_max {
            match &shape.shape_type {
                ShapeType::Integer | ShapeType::Long => {
                    if let Some(above) = (max as i64).checked_add(1) {
                        let mut input = build_required_input(model, input_shape_id, overrides);
                        if let Value::Object(ref mut obj) = input {
                            obj.insert(member.name.clone(), Value::Number(above.into()));
                        }
                        variants.push(TestVariant {
                            name: format!("negative_above_max_{}", member.name),
                            strategy: Strategy::Negative,
                            input,
                            expectation: Expectation::AnyError,
                            expected_output: None,
                            followup: None,
                        });
                    }
                }
                _ => {}
            }
        }

        // Invalid enum value
        let has_enum = matches!(
            &shape.shape_type,
            ShapeType::String {
                enum_values: Some(_)
            } | ShapeType::Enum { .. }
        );
        if has_enum {
            let mut input = build_required_input(model, input_shape_id, overrides);
            if let Value::Object(ref mut obj) = input {
                obj.insert(
                    member.name.clone(),
                    Value::String("__INVALID_ENUM_VALUE__".to_string()),
                );
            }
            variants.push(TestVariant {
                name: format!("negative_invalid_enum_{}", member.name),
                strategy: Strategy::Negative,
                input,
                expectation: Expectation::AnyError,
                expected_output: None,
                followup: None,
            });
        }
    }

    variants
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smithy::{Member, Operation, Shape, ShapeTraits};

    fn op(name: &str, method: &str, uri: &str, input: &str) -> Operation {
        Operation {
            name: name.to_string(),
            input_shape: Some(input.to_string()),
            output_shape: None,
            error_shapes: Vec::new(),
            http_method: Some(method.to_string()),
            http_uri: Some(uri.to_string()),
            http_code: None,
        }
    }

    /// Model with a `{Id}`-labelled input (`@length(min)` on the label) and
    /// the given operations.
    fn model(min: u64, ops: Vec<Operation>) -> ServiceModel {
        let mut shapes = HashMap::new();
        shapes.insert(
            "t#Input".to_string(),
            Shape {
                shape_id: "t#Input".to_string(),
                shape_type: ShapeType::Structure {
                    members: vec![
                        Member {
                            name: "Id".to_string(),
                            target: "t#Id".to_string(),
                            required: true,
                            traits: ShapeTraits {
                                http_label: true,
                                ..ShapeTraits::default()
                            },
                        },
                        Member {
                            name: "Sub".to_string(),
                            target: "t#Id".to_string(),
                            required: true,
                            traits: ShapeTraits {
                                http_label: true,
                                ..ShapeTraits::default()
                            },
                        },
                    ],
                },
                traits: ShapeTraits::default(),
            },
        );
        shapes.insert(
            "t#Id".to_string(),
            Shape {
                shape_id: "t#Id".to_string(),
                shape_type: ShapeType::String { enum_values: None },
                traits: ShapeTraits {
                    length_min: Some(min),
                    length_max: Some(64),
                    ..ShapeTraits::default()
                },
            },
        );
        ServiceModel {
            service_name: "t".to_string(),
            operations: ops,
            shapes,
            ..Default::default()
        }
    }

    fn names(m: &ServiceModel, method: &str, uri: &str) -> Vec<String> {
        generate(m, "t#Input", &HashMap::new(), Some(method), Some(uri))
            .into_iter()
            .map(|v| v.name)
            .collect()
    }

    #[test]
    fn empty_trailing_label_collapsing_onto_another_op_is_skipped() {
        let uri = "/things/{Sub}/items/{Id}";
        let m = model(
            1,
            vec![
                op("GetItem", "GET", uri, "t#Input"),
                op("ListItems", "GET", "/things/{Sub}/items", "t#Input"),
            ],
        );
        let got = names(&m, "GET", uri);
        // Trailing label `Id`: `GET /things/x/items/` is ListItems.
        assert!(
            !got.contains(&"negative_too_short_Id".to_string()),
            "{got:?}"
        );
        // Interior label `Sub` keeps its variant: `//` reaches GetItem.
        assert!(
            got.contains(&"negative_too_short_Sub".to_string()),
            "{got:?}"
        );
        // Other negatives for the trailing label are unaffected.
        assert!(got.contains(&"negative_too_long_Id".to_string()), "{got:?}");
    }

    #[test]
    fn empty_trailing_label_without_same_method_parent_route_is_kept() {
        let uri = "/things/{Sub}/items/{Id}";
        // The collapsed URI is only bound to a POST, not a GET.
        let m = model(
            1,
            vec![
                op("GetItem", "GET", uri, "t#Input"),
                op("CreateItem", "POST", "/things/{Sub}/items", "t#Input"),
            ],
        );
        let got = names(&m, "GET", uri);
        assert!(
            got.contains(&"negative_too_short_Id".to_string()),
            "{got:?}"
        );
    }

    #[test]
    fn collapse_requires_equal_query_literals() {
        let uri = "/things/{Sub}/items/{Id}?mode=a&flag";
        // Same path and the same literals in another order -> identical
        // request -> skipped.
        let m = model(
            1,
            vec![
                op("GetItem", "GET", uri, "t#Input"),
                op(
                    "ListItems",
                    "GET",
                    "/things/{Sub}/items?flag&mode=a",
                    "t#Input",
                ),
            ],
        );
        let got = names(&m, "GET", uri);
        assert!(
            !got.contains(&"negative_too_short_Id".to_string()),
            "{got:?}"
        );

        // Different literals (`?uploads` vs `?delete`), a differing value, or
        // a literal on only one side -> a distinct request -> kept.
        for other in [
            "/things/{Sub}/items?delete",
            "/things/{Sub}/items?mode=b&flag",
            "/things/{Sub}/items",
        ] {
            let m = model(
                1,
                vec![
                    op("GetItem", "GET", uri, "t#Input"),
                    op("Other", "GET", other, "t#Input"),
                ],
            );
            let got = names(&m, "GET", uri);
            assert!(
                got.contains(&"negative_too_short_Id".to_string()),
                "{other}: {got:?}"
            );
        }
    }

    #[test]
    fn non_empty_too_short_trailing_label_is_kept() {
        let uri = "/things/{Sub}/items/{Id}";
        // min 3 -> the too-short value is "aa", which does not collapse.
        let m = model(
            3,
            vec![
                op("GetItem", "GET", uri, "t#Input"),
                op("ListItems", "GET", "/things/{Sub}/items", "t#Input"),
            ],
        );
        let got = names(&m, "GET", uri);
        assert!(
            got.contains(&"negative_too_short_Id".to_string()),
            "{got:?}"
        );
    }
}
