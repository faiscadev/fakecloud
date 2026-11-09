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

/// The Smithy namespace of the S3 model.
const S3_NAMESPACE: &str = "com.amazonaws.s3";

/// The query key or header name a member is bound to, if either.
fn query_or_header_binding(
    model: &ServiceModel,
    member: &crate::smithy::Member,
) -> Option<(&'static str, String)> {
    let target = model.shapes.get(&member.target).map(|s| &s.traits);
    let query = member
        .traits
        .http_query
        .clone()
        .or_else(|| target.and_then(|t| t.http_query.clone()));
    if let Some(q) = query {
        return Some(("query", q));
    }
    member
        .traits
        .http_header
        .clone()
        .or_else(|| target.and_then(|t| t.http_header.clone()))
        .map(|h| ("header", h.to_ascii_lowercase()))
}

/// The constant query literals of a URI template that select the operation.
/// `x-id=<Operation>` is left out: it is a routing hint SDKs add for logging,
/// which S3 does not use to pick the operation (a `DELETE /{Bucket}/{Key+}`
/// is DeleteObject or AbortMultipartUpload by its `uploadId`, whatever
/// `x-id` says).
fn selecting_literals(uri: &str) -> (&str, Vec<&str>) {
    let (path, mut literals) = split_uri_template(uri);
    literals.retain(|q| !q.starts_with("x-id="));
    (path, literals)
}

/// Whether omitting the required `@httpQuery` / `@httpHeader` member
/// `omitted` turns the request into another modeled operation's request.
///
/// The request without it has the same method, path and selecting query
/// literals as another operation, every other required query/header member
/// it still carries is one that operation binds too, and it carries every
/// query/header member that operation requires, so the server can only
/// answer as that operation (the `empty_label_collapses_to_other_op`
/// rule, for query and header members). Recorded against S3: CopyObject
/// without `x-amz-copy-source` is PutObject, ListParts without `uploadId` is
/// GetObject, AbortMultipartUpload without it is DeleteObject (204), and
/// `GET /{Bucket}?analytics` without `id` is ListBucketAnalyticsConfigurations
/// (200). UploadPart without `uploadId` still carries `partNumber`, which
/// PutObject does not bind, so it stays a real negative (S3: 400
/// InvalidArgument "This operation does not accept partNumber without
/// uploadId").
fn omission_collapses_to_other_op(
    model: &ServiceModel,
    input_shape_id: &str,
    http_method: Option<&str>,
    http_uri: Option<&str>,
    omitted: &crate::smithy::Member,
) -> bool {
    let (Some(method), Some(uri)) = (http_method, http_uri) else {
        return false;
    };
    if query_or_header_binding(model, omitted).is_none() {
        return false;
    }
    let remaining: Vec<(&'static str, String)> = super::get_members(model, input_shape_id)
        .iter()
        .filter(|m| m.required && m.name != omitted.name)
        .filter_map(|m| query_or_header_binding(model, m))
        .collect();
    let own = selecting_literals(uri);
    model.operations.iter().any(|op| {
        let (Some(op_method), Some(op_uri)) = (op.http_method.as_deref(), op.http_uri.as_deref())
        else {
            return false;
        };
        if !op_method.eq_ignore_ascii_case(method)
            || op.input_shape.as_deref() == Some(input_shape_id)
            || selecting_literals(op_uri) != own
        {
            return false;
        }
        let members = op
            .input_shape
            .as_deref()
            .map(|id| super::get_members(model, id))
            .unwrap_or_default();
        let bound: Vec<(&'static str, String)> = members
            .iter()
            .filter_map(|m| query_or_header_binding(model, m))
            .collect();
        // ...and the request still carries everything that operation
        // requires (UploadPart without `uploadId` is not UploadPartCopy,
        // which needs `x-amz-copy-source`).
        let satisfied = members
            .iter()
            .filter(|m| m.required)
            .filter_map(|m| query_or_header_binding(model, m))
            .all(|b| remaining.contains(&b));
        satisfied && remaining.iter().all(|b| bound.contains(b))
    })
}

/// What a `negative_invalid_enum_<member>` variant expects, or `None` when
/// the variant cannot be judged.
///
/// Most services reject a value outside a member's enum. S3 does not for
/// every header; each entry below was recorded against real S3 (us-east-1,
/// 2026-11-09, raw signed requests with `__INVALID_ENUM_VALUE__`):
///
/// - `RequestPayer` (`x-amz-request-payer`): ignored. PutObject, GetObject,
///   HeadObject, ListObjectsV2, DeleteObject and GetObjectTagging all
///   answered 2xx, as without the header. S3 only acts on `requester`.
/// - CreateSession's `SessionMode` (`x-amz-create-session-mode`): ignored;
///   the request answered 200.
///
/// Such a variant still runs: it expects the operation's normal success
/// path, so it checks that the handler ignores the value rather than
/// failing on it.
///
/// WriteGetObjectResponse's members are the `x-amz-fwd-header-*` response
/// headers an S3 Object Lambda function hands back for the original GetObject
/// caller. That call needs an Object Lambda access point's request route and
/// token, so it cannot be recorded, and the API reference documents the
/// headers as pass-through with no validation; its invalid-enum variants are
/// not generated.
fn invalid_enum_expectation(
    model: &ServiceModel,
    member: &str,
    http_uri: Option<&str>,
) -> Option<Expectation> {
    if model.service_name != S3_NAMESPACE {
        return Some(Expectation::AnyError);
    }
    match (member, http_uri) {
        (_, Some("/WriteGetObjectResponse")) => None,
        ("RequestPayer", _) => Some(Expectation::Success),
        ("SessionMode", Some("/{Bucket}?session")) => Some(Expectation::Success),
        _ => Some(Expectation::AnyError),
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
        if !omission_is_wire_observable(model, &omit_member.target)
            || omission_collapses_to_other_op(
                model,
                input_shape_id,
                http_method,
                http_uri,
                omit_member,
            )
        {
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
        let enum_expectation = if has_enum {
            invalid_enum_expectation(model, &member.name, http_uri)
        } else {
            None
        };
        if let Some(expectation) = enum_expectation {
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
                expectation,
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

    fn real_model(file: &str) -> ServiceModel {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../aws-models")
            .join(file);
        crate::smithy::parse_model(&path).expect("parse model")
    }

    fn variants_for(model: &ServiceModel, op_name: &str) -> Vec<TestVariant> {
        let op = model.operations.iter().find(|o| o.name == op_name).unwrap();
        generate(
            model,
            op.input_shape.as_deref().unwrap(),
            &HashMap::new(),
            op.http_method.as_deref(),
            op.http_uri.as_deref(),
        )
    }

    fn variant_names(vs: &[TestVariant]) -> Vec<&str> {
        vs.iter().map(|v| v.name.as_str()).collect()
    }

    #[test]
    fn omissions_that_are_another_operation_are_not_generated() {
        let s3 = real_model("s3.json");
        // The request without the member IS another operation's request.
        for (op, member) in [
            ("CopyObject", "negative_omit_CopySource"),
            ("UploadPartCopy", "negative_omit_CopySource"),
            ("ListParts", "negative_omit_UploadId"),
            ("AbortMultipartUpload", "negative_omit_UploadId"),
            ("GetBucketAnalyticsConfiguration", "negative_omit_Id"),
            ("GetObjectAnnotation", "negative_omit_AnnotationName"),
        ] {
            assert!(
                !variant_names(&variants_for(&s3, op)).contains(&member),
                "{op} still emits {member}"
            );
        }
        // UploadPart without uploadId still carries partNumber, which no
        // other operation binds: a real negative (400 InvalidArgument).
        let up = variants_for(&s3, "UploadPart");
        assert!(variant_names(&up).contains(&"negative_omit_UploadId"));
        assert!(variant_names(&up).contains(&"negative_omit_PartNumber"));
        // A delete without its id has no List twin.
        assert!(
            variant_names(&variants_for(&s3, "DeleteBucketAnalyticsConfiguration"))
                .contains(&"negative_omit_Id")
        );
    }

    #[test]
    fn s3_headers_it_ignores_expect_the_success_path() {
        let s3 = real_model("s3.json");
        let get = variants_for(&s3, "GetObject");
        let rp = get
            .iter()
            .find(|v| v.name == "negative_invalid_enum_RequestPayer")
            .unwrap();
        assert!(matches!(rp.expectation, Expectation::Success));
        let cm = get
            .iter()
            .find(|v| v.name == "negative_invalid_enum_ChecksumMode")
            .unwrap();
        assert!(matches!(cm.expectation, Expectation::AnyError));
        let session = variants_for(&s3, "CreateSession");
        let sm = session
            .iter()
            .find(|v| v.name == "negative_invalid_enum_SessionMode")
            .unwrap();
        assert!(matches!(sm.expectation, Expectation::Success));
        // WriteGetObjectResponse's forwarded headers are not judged.
        assert!(!variant_names(&variants_for(&s3, "WriteGetObjectResponse"))
            .iter()
            .any(|n| n.starts_with("negative_invalid_enum_")));
        // Other services keep the strict expectation.
        let lambda = real_model("lambda.json");
        let inv = variants_for(&lambda, "Invoke");
        let it = inv
            .iter()
            .find(|v| v.name == "negative_invalid_enum_InvocationType")
            .unwrap();
        assert!(matches!(it.expectation, Expectation::AnyError));
    }
}
