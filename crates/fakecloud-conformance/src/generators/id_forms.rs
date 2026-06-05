//! Strategy 8: identifier-form fanout.
//!
//! AWS REST APIs that take an identifier in the URL path (e.g.
//! `GET /2015-03-31/functions/{FunctionName}` for Lambda) typically
//! accept the identifier in multiple forms: bare name, `name:qualifier`,
//! partial ARN, full ARN, URL-encoded full ARN. The conformance harness
//! historically only generated the bare name — #817 was missed because
//! the ARN form returned 404 from fakecloud and the negative-strategy
//! classifier waved it through.
//!
//! This strategy walks each REST operation's URL labels (members marked
//! `@httpLabel`) and inspects the `@pattern` trait on the member's target
//! shape. If the pattern admits ARN input (`smithy::pattern_admits_arn`),
//! we emit one variant per accepted form. Each variant overrides the
//! identifier value in the variant's input — the probe layer's
//! Smithy-driven URL builder substitutes it into the path automatically.
//! For Lambda/S3 (still on the legacy hardcoded routing table), the
//! probe layer also consults the variant input for known label names.

use serde_json::Value;
use std::collections::HashMap;

use super::{build_required_input, Expectation, Strategy, TestVariant};
use crate::smithy::{
    effective_shape_type, pattern_admits_arn, ServiceModel, ShapeTraits, ShapeType,
};

const FAKE_ACCOUNT: &str = "123456789012";

/// Per-service ARN service prefix used to synthesise `arn:aws:<svc>:...` forms.
/// Mirrors the SigV4 service identifier most services use.
fn arn_service_prefix(service_name: &str) -> &'static str {
    match service_name {
        "lambda" => "lambda",
        "s3" => "s3",
        "iam" => "iam",
        "ses" | "sesv2" => "ses",
        "states" => "states",
        "ecs" => "ecs",
        "ecr" => "ecr",
        _ => "service",
    }
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
    let svc = arn_service_prefix(&model.service_name);
    for (member_name, default_value) in labels {
        // Derive the bare-name basis from the schema's default. Strip
        // any `arn:` prefix the default happens to carry so the synthetic
        // forms below are always layered on a clean identifier.
        let base = strip_arn(&default_value);
        let forms = vec![
            ("bare", base.clone()),
            ("qualifier", format!("{}:LATEST", base)),
            ("partial_arn", format!("{}:function:{}", FAKE_ACCOUNT, base)),
            (
                "full_arn",
                format!(
                    "arn:aws:{}:us-east-1:{}:function:{}",
                    svc, FAKE_ACCOUNT, base
                ),
            ),
            (
                "encoded_full_arn",
                url_encode_colons(&format!(
                    "arn:aws:{}:us-east-1:{}:function:{}",
                    svc, FAKE_ACCOUNT, base
                )),
            ),
        ];
        for (form_name, value) in forms {
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
/// `@pattern` trait admits an ARN. Returns `(member_name, default_value)`
/// where the default is the schema-driven bare value used as the basis
/// for ARN synthesis.
fn httplabel_members_with_arn_pattern(
    model: &ServiceModel,
    input_shape_id: &str,
) -> Vec<(String, String)> {
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
        out.push((member.name.clone(), default_str));
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

fn url_encode_colons(s: &str) -> String {
    s.replace(':', "%3A")
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

    #[test]
    fn emits_five_variants_for_arn_admitting_label() {
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
        };

        let variants = generate(&model, "GetFunction", &HashMap::new());
        let names: Vec<&str> = variants.iter().map(|v| v.name.as_str()).collect();
        assert_eq!(variants.len(), 5, "expected 5 forms, got {:?}", names);
        // Every variant carries the FunctionName override.
        for v in &variants {
            let obj = v.input.as_object().expect("object");
            assert!(obj.contains_key("FunctionName"));
            assert_eq!(v.strategy, Strategy::IdForms);
        }
        // Spot-check the full-ARN form.
        let full = variants
            .iter()
            .find(|v| v.name.contains("full_arn") && !v.name.contains("encoded"))
            .expect("full_arn variant present");
        let val = full
            .input
            .as_object()
            .unwrap()
            .get("FunctionName")
            .unwrap()
            .as_str()
            .unwrap();
        assert!(val.starts_with("arn:aws:lambda:"), "got {val}");
        // URL-encoded form.
        let enc = variants
            .iter()
            .find(|v| v.name.contains("encoded_full_arn"))
            .unwrap();
        let enc_val = enc
            .input
            .as_object()
            .unwrap()
            .get("FunctionName")
            .unwrap()
            .as_str()
            .unwrap();
        assert!(enc_val.contains("%3A"), "got {enc_val}");
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
        };
        assert!(generate(&model, "Foo", &HashMap::new()).is_empty());
    }
}
