//! Strategy 8: identifier-form fanout.
//!
//! AWS REST APIs that take an identifier in the URL path (e.g.
//! `GET /2015-03-31/functions/{FunctionName}` for Lambda) typically
//! accept the identifier in multiple forms: bare name, `name:qualifier`,
//! partial ARN, full ARN. The conformance harness historically only
//! generated the bare name -- #817 was missed because the ARN form
//! returned 404 from fakecloud and the negative-strategy classifier waved
//! it through.
//!
//! This strategy walks each REST operation's URL labels (members marked
//! `@httpLabel`) and inspects the `@pattern` trait on the member's target
//! shape. If the pattern admits ARN input (`smithy::pattern_admits_arn`),
//! we emit one variant per accepted form. Each variant overrides the
//! identifier value in the variant's input -- the probe layer's
//! Smithy-driven URL builder substitutes it into the path automatically,
//! percent-encoding it once (`:` -> `%3A`), exactly as an AWS SDK
//! serializes a label. That is also why there is no separate "URL-encoded
//! ARN" form: a full ARN already reaches the server URL-encoded, and a
//! value encoded before the SDK sees it is encoded again on the wire
//! (`%253A`), which names no resource.
//!
//! A form the member's `@pattern` rejects is not generated: AWS refuses it
//! before looking anything up, so a success-path variant built on it can
//! only ever be answered with an error (a layer has no `name:qualifier`
//! form, a scheduler ARN no `function:` resource).

use regex::Regex;
use serde_json::Value;
use std::collections::HashMap;

use super::{build_required_input, Expectation, Strategy, TestVariant};
use crate::smithy::{
    effective_shape_type, pattern_admits_arn, ServiceModel, ShapeTraits, ShapeType,
};

const FAKE_ACCOUNT: &str = "123456789012";
const FAKE_REGION: &str = "us-east-1";

/// The service segment of the synthesized ARNs: the model's ARN namespace,
/// else the last segment of its Smithy namespace (`com.amazonaws.lambda`).
fn arn_service(model: &ServiceModel) -> &str {
    model.arn_namespace.as_deref().unwrap_or_else(|| {
        model
            .service_name
            .rsplit('.')
            .next()
            .unwrap_or(&model.service_name)
    })
}

/// The ARN resource type a label's pattern names (`layer` for a Lambda layer
/// name), defaulting to Lambda's `function`.
fn resource_type(pattern: &str) -> &'static str {
    ["layer", "function"]
        .into_iter()
        .find(|t| pattern.contains(&format!("{t}:")))
        .unwrap_or("function")
}

/// Whether `value` satisfies the whole `pattern`, as AWS validates a member
/// (Smithy patterns are not anchored; AWS matches the full value). A
/// pattern the `regex` crate cannot compile admits everything.
fn satisfies(pattern: &str, value: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    match Regex::new(&format!("^(?:{pattern})$")) {
        Ok(re) => re.is_match(value),
        Err(_) => true,
    }
}

/// The identifier forms of `basis` (a seeded identifier or the schema's
/// default) that `pattern` admits, as `(form, value)`.
fn forms(
    basis: &str,
    pattern: &str,
    service: &str,
    qualifier: Option<&str>,
) -> Vec<(&'static str, String)> {
    let base = strip_arn(basis);
    let rtype = resource_type(pattern);
    // A seeded ARN is the real one; synthesize the ARN only from a bare name.
    let (full_arn, account) = if basis.starts_with("arn:") {
        let account = basis.split(':').nth(4).unwrap_or(FAKE_ACCOUNT).to_string();
        (basis.to_string(), account)
    } else {
        (
            format!("arn:aws:{service}:{FAKE_REGION}:{FAKE_ACCOUNT}:{rtype}:{base}"),
            FAKE_ACCOUNT.to_string(),
        )
    };
    let candidates = [
        ("bare", base.clone()),
        // The seeded alias when there is one: `$LATEST` is the only
        // qualifier that exists without a seed.
        (
            "qualifier",
            format!("{base}:{}", qualifier.unwrap_or("$LATEST")),
        ),
        ("partial_arn", format!("{account}:{rtype}:{base}")),
        ("full_arn", full_arn),
    ];
    let mut out: Vec<(&'static str, String)> = Vec::new();
    for (form, value) in candidates {
        if satisfies(pattern, &value) && !out.iter().any(|(_, v)| *v == value) {
            out.push((form, value));
        }
    }
    out
}

pub fn generate(
    model: &ServiceModel,
    operation_name: &str,
    overrides: &HashMap<String, Value>,
) -> Vec<TestVariant> {
    let op = match model.operations.iter().find(|o| o.name == operation_name) {
        Some(o) => o,
        None => return Vec::new(),
    };
    // Op must be REST-bound (have `@http`) and have at least one
    // httpLabel-marked input member; everything else falls through to
    // the existing strategies.
    if op.http_uri.is_none() {
        return Vec::new();
    }
    let input_shape_id = match op.input_shape.as_deref() {
        Some(id) => id,
        None => return Vec::new(),
    };

    let labels = httplabel_members_with_arn_pattern(model, input_shape_id);
    if labels.is_empty() {
        return Vec::new();
    }

    let mut variants = Vec::new();
    let service = arn_service(model);
    let qualifier = overrides.get("Qualifier").and_then(Value::as_str);
    for (member_name, default_value, pattern) in labels {
        // Build on the seeded identifier when the caller pinned one (so
        // every form names a resource that exists and the variant tests the
        // form, not a not-found), else the schema's default.
        let basis = overrides
            .get(&member_name)
            .and_then(Value::as_str)
            .map(str::to_string)
            .unwrap_or(default_value);
        for (form_name, value) in forms(&basis, &pattern, service, qualifier) {
            let mut local_overrides = overrides.clone();
            local_overrides.insert(member_name.clone(), Value::String(value));
            let input = build_required_input(model, input_shape_id, &local_overrides);
            variants.push(TestVariant {
                name: format!("id_form_{}_{}", member_name, form_name),
                strategy: Strategy::IdForms,
                input,
                expectation: Expectation::Success,
                expected_output: None,
                followup: None,
            });
        }
    }
    variants
}

/// Find input members carrying `@httpLabel` whose target string shape's
/// `@pattern` trait admits an ARN. Returns `(member_name, default_value,
/// pattern)` where the default is the schema-driven bare value used as the
/// basis for ARN synthesis.
fn httplabel_members_with_arn_pattern(
    model: &ServiceModel,
    input_shape_id: &str,
) -> Vec<(String, String, String)> {
    let members = match effective_shape_type(model, input_shape_id) {
        Some(ShapeType::Structure { members }) => members,
        _ => return Vec::new(),
    };
    let mut out = Vec::new();
    for member in members {
        let traits = effective_traits(model, &member.target, &member.traits);
        if !traits.http_label && !member.traits.http_label {
            continue;
        }
        let pattern = traits.pattern.as_deref().unwrap_or("");
        if !pattern_admits_arn(pattern) {
            continue;
        }
        let default = super::default_value_for_shape(model, &member.target, 0);
        let default_str = match default {
            Value::String(s) => s,
            _ => continue,
        };
        out.push((member.name.clone(), default_str, pattern.to_string()));
    }
    out
}

/// Pull the most specific traits available: target shape's traits beat
/// the member's traits when both carry a value (matches Smithy lookup
/// semantics for `@pattern` and `@httpLabel`).
fn effective_traits<'a>(
    model: &'a ServiceModel,
    target_id: &str,
    member_traits: &'a ShapeTraits,
) -> std::borrow::Cow<'a, ShapeTraits> {
    match model.shapes.get(target_id) {
        Some(shape) => std::borrow::Cow::Owned(merged_traits(&shape.traits, member_traits)),
        None => std::borrow::Cow::Borrowed(member_traits),
    }
}

fn merged_traits(target: &ShapeTraits, member: &ShapeTraits) -> ShapeTraits {
    ShapeTraits {
        documentation: member
            .documentation
            .clone()
            .or_else(|| target.documentation.clone()),
        length_min: member.length_min.or(target.length_min),
        length_max: member.length_max.or(target.length_max),
        range_min: member.range_min.or(target.range_min),
        range_max: member.range_max.or(target.range_max),
        pattern: member.pattern.clone().or_else(|| target.pattern.clone()),
        deprecated: member.deprecated || target.deprecated,
        sensitive: member.sensitive || target.sensitive,
        error: member.error.clone().or_else(|| target.error.clone()),
        http_error: member.http_error.or(target.http_error),
        default_value: member
            .default_value
            .clone()
            .or_else(|| target.default_value.clone()),
        examples: if member.examples.is_empty() {
            target.examples.clone()
        } else {
            member.examples.clone()
        },
        http_label: member.http_label || target.http_label,
        http_query: member
            .http_query
            .clone()
            .or_else(|| target.http_query.clone()),
        http_header: member
            .http_header
            .clone()
            .or_else(|| target.http_header.clone()),
        http_payload: member.http_payload || target.http_payload,
        json_name: member
            .json_name
            .clone()
            .or_else(|| target.json_name.clone()),
        aws_query_error_code: member
            .aws_query_error_code
            .clone()
            .or_else(|| target.aws_query_error_code.clone()),
        ec2_query_name: member
            .ec2_query_name
            .clone()
            .or_else(|| target.ec2_query_name.clone()),
        xml_name: member.xml_name.clone().or_else(|| target.xml_name.clone()),
        xml_flattened: member.xml_flattened || target.xml_flattened,
        xml_namespace: member
            .xml_namespace
            .clone()
            .or_else(|| target.xml_namespace.clone()),
        xml_attribute: member.xml_attribute || target.xml_attribute,
        timestamp_format: member
            .timestamp_format
            .clone()
            .or_else(|| target.timestamp_format.clone()),
        http_prefix_headers: member
            .http_prefix_headers
            .clone()
            .or_else(|| target.http_prefix_headers.clone()),
        http_query_params: member.http_query_params || target.http_query_params,
        media_type: member
            .media_type
            .clone()
            .or_else(|| target.media_type.clone()),
        xml_list_member_name: target.xml_list_member_name.clone(),
        xml_map_key_name: target.xml_map_key_name.clone(),
        xml_map_value_name: target.xml_map_value_name.clone(),
        list_member_timestamp_format: target.list_member_timestamp_format.clone(),
        map_value_timestamp_format: target.map_value_timestamp_format.clone(),
    }
}

fn strip_arn(value: &str) -> String {
    if let Some(rest) = value.strip_prefix("arn:") {
        // Keep only the resource segment: arn:aws:svc:region:account:resource[/X]
        rest.rsplit(':').next().unwrap_or(value).to_string()
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smithy::{Member, Operation, Shape, ShapeTraits};

    fn op_with_uri(name: &str, input: &str) -> Operation {
        Operation {
            name: name.to_string(),
            input_shape: Some(input.to_string()),
            output_shape: None,
            error_shapes: Vec::new(),
            http_method: Some("GET".to_string()),
            http_uri: Some("/2015-03-31/functions/{FunctionName}".to_string()),
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

    fn lambda_function_name_shape() -> Shape {
        Shape {
            shape_id: "test#FunctionName".to_string(),
            shape_type: ShapeType::String { enum_values: None },
            traits: ShapeTraits {
                pattern: Some(
                    "(arn:(aws[a-zA-Z-]*)?:lambda:)?([a-z]{2}(-gov)?-[a-z]+-\\d{1}:)?(\\d{12}:)?(function:)?([a-zA-Z0-9-_\\.]+)(:(\\$LATEST|[a-zA-Z0-9-_]+))?".to_string(),
                ),
                ..ShapeTraits::default()
            },
        }
    }

    fn get_function_model() -> ServiceModel {
        let mut shapes = std::collections::HashMap::new();
        shapes.insert(
            "test#GetFunctionRequest".to_string(),
            structure(
                "test#GetFunctionRequest",
                vec![Member {
                    name: "FunctionName".to_string(),
                    target: "test#FunctionName".to_string(),
                    required: true,
                    traits: ShapeTraits {
                        http_label: true,
                        ..ShapeTraits::default()
                    },
                }],
            ),
        );
        shapes.insert(
            "test#FunctionName".to_string(),
            lambda_function_name_shape(),
        );
        ServiceModel {
            service_name: "com.amazonaws.lambda".to_string(),
            arn_namespace: Some("lambda".to_string()),
            operations: vec![op_with_uri("GetFunction", "test#GetFunctionRequest")],
            shapes,
            ..Default::default()
        }
    }

    fn form<'v>(variants: &'v [TestVariant], name: &str) -> Option<&'v str> {
        variants
            .iter()
            .find(|v| v.name == format!("id_form_FunctionName_{name}"))
            .map(|v| v.input["FunctionName"].as_str().unwrap())
    }

    #[test]
    fn emits_one_variant_per_admitted_form() {
        let model = get_function_model();
        let variants = generate(&model, "GetFunction", &HashMap::new());
        let names: Vec<&str> = variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "id_form_FunctionName_bare",
                "id_form_FunctionName_qualifier",
                "id_form_FunctionName_partial_arn",
                "id_form_FunctionName_full_arn",
            ]
        );
        for v in &variants {
            assert_eq!(v.strategy, Strategy::IdForms);
            // One level of encoding is the request builder's job: no form
            // arrives pre-encoded.
            assert!(!v.input["FunctionName"].as_str().unwrap().contains('%'));
        }
        let full = form(&variants, "full_arn").unwrap();
        assert!(
            full.starts_with("arn:aws:lambda:us-east-1:123456789012:function:"),
            "got {full}"
        );
    }

    #[test]
    fn arn_service_comes_from_the_arn_namespace() {
        let mut model = get_function_model();
        model.arn_namespace = Some("states".to_string());
        assert_eq!(arn_service(&model), "states");
        model.arn_namespace = None;
        assert_eq!(arn_service(&model), "lambda");
    }

    #[test]
    fn the_qualifier_form_uses_the_seeded_qualifier_else_latest() {
        let model = get_function_model();
        let overrides = HashMap::from([
            ("FunctionName".to_string(), Value::String("fn".to_string())),
            ("Qualifier".to_string(), Value::String("live".to_string())),
        ]);
        let seeded = generate(&model, "GetFunction", &overrides);
        assert_eq!(form(&seeded, "qualifier"), Some("fn:live"));
        let unseeded = generate(&model, "GetFunction", &HashMap::new());
        assert!(form(&unseeded, "qualifier").unwrap().ends_with(":$LATEST"));
    }

    #[test]
    fn a_seeded_arn_is_its_own_full_arn_form() {
        let arn = "arn:aws:lambda:eu-west-1:210987654321:function:seeded";
        let forms = forms(arn, "", "lambda", None);
        assert!(forms.contains(&("full_arn", arn.to_string())));
        assert!(forms.contains(&("bare", "seeded".to_string())));
        assert!(forms.contains(&("partial_arn", "210987654321:function:seeded".to_string())));
    }

    #[test]
    fn forms_the_pattern_rejects_are_not_generated() {
        // Lambda's LayerName: a bare name or a layer ARN, nothing else.
        let layer =
            "(arn:[a-zA-Z0-9-]+:lambda:[a-zA-Z0-9-]+:\\d{12}:layer:[a-zA-Z0-9-_]+)|[a-zA-Z0-9-_]+";
        let got = forms("my-layer", layer, "lambda", None);
        assert_eq!(
            got,
            [
                ("bare", "my-layer".to_string()),
                (
                    "full_arn",
                    "arn:aws:lambda:us-east-1:123456789012:layer:my-layer".to_string()
                ),
            ]
        );
        // An ARN-only pattern of another resource type keeps no synthesized
        // `function:` form.
        let schedule =
            "arn:aws(-[a-z]+)?:scheduler:[a-z0-9\\-]+:\\d{12}:schedule-group\\/[0-9a-zA-Z-_.]+";
        assert!(forms("t", schedule, "scheduler", None).is_empty());
    }

    #[test]
    fn patterns_match_the_whole_value() {
        assert!(satisfies("[a-z]+", "abc"));
        assert!(!satisfies("[a-z]+", "abc:1"));
        // Unsupported syntax (lookahead) admits the value.
        assert!(satisfies("(?!x)[a-z]+", "abc:1"));
    }

    #[test]
    fn forms_are_built_on_the_seeded_identifier() {
        let mut shapes = std::collections::HashMap::new();
        shapes.insert(
            "test#GetFunctionRequest".to_string(),
            structure(
                "test#GetFunctionRequest",
                vec![Member {
                    name: "FunctionName".to_string(),
                    target: "test#FunctionName".to_string(),
                    required: true,
                    traits: ShapeTraits {
                        http_label: true,
                        ..ShapeTraits::default()
                    },
                }],
            ),
        );
        shapes.insert(
            "test#FunctionName".to_string(),
            lambda_function_name_shape(),
        );
        let model = ServiceModel {
            service_name: "lambda".to_string(),
            operations: vec![op_with_uri("GetFunction", "test#GetFunctionRequest")],
            shapes,
            ..Default::default()
        };
        let overrides = HashMap::from([(
            "FunctionName".to_string(),
            Value::String("seeded-fn".to_string()),
        )]);
        let variants = generate(&model, "GetFunction", &overrides);
        let full = variants
            .iter()
            .find(|v| v.name == "id_form_FunctionName_full_arn")
            .unwrap();
        assert_eq!(
            full.input["FunctionName"],
            "arn:aws:lambda:us-east-1:123456789012:function:seeded-fn"
        );
        let bare = variants
            .iter()
            .find(|v| v.name == "id_form_FunctionName_bare")
            .unwrap();
        assert_eq!(bare.input["FunctionName"], "seeded-fn");
    }

    #[test]
    fn skips_when_pattern_does_not_admit_arn() {
        let mut shapes = std::collections::HashMap::new();
        shapes.insert(
            "test#NamedRequest".to_string(),
            structure(
                "test#NamedRequest",
                vec![Member {
                    name: "Name".to_string(),
                    target: "test#NameOnly".to_string(),
                    required: true,
                    traits: ShapeTraits {
                        http_label: true,
                        ..ShapeTraits::default()
                    },
                }],
            ),
        );
        shapes.insert(
            "test#NameOnly".to_string(),
            Shape {
                shape_id: "test#NameOnly".to_string(),
                shape_type: ShapeType::String { enum_values: None },
                traits: ShapeTraits {
                    pattern: Some("[a-zA-Z0-9_-]+".to_string()),
                    ..ShapeTraits::default()
                },
            },
        );
        let model = ServiceModel {
            service_name: "iam".to_string(),
            operations: vec![op_with_uri("GetThing", "test#NamedRequest")],
            shapes,
            ..Default::default()
        };
        assert!(generate(&model, "GetThing", &HashMap::new()).is_empty());
    }

    #[test]
    fn skips_when_no_http_uri() {
        let mut shapes = std::collections::HashMap::new();
        shapes.insert(
            "test#Req".to_string(),
            structure(
                "test#Req",
                vec![Member {
                    name: "X".to_string(),
                    target: "smithy.api#String".to_string(),
                    required: true,
                    traits: ShapeTraits::default(),
                }],
            ),
        );
        let mut op = op_with_uri("Foo", "test#Req");
        op.http_uri = None;
        let model = ServiceModel {
            service_name: "lambda".to_string(),
            operations: vec![op],
            shapes,
            ..Default::default()
        };
        assert!(generate(&model, "Foo", &HashMap::new()).is_empty());
    }
}
