//! Level 1 probing: send generated requests to fakecloud and classify responses.

use std::collections::HashMap;

use serde_json::Value;

use crate::generators::{Expectation, TestVariant};
use crate::shape_validator;
use crate::smithy::ServiceModel;

/// Protocol used by a service for request/response encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// Query protocol: form-encoded body with `Action` param, XML responses.
    /// Used by: SQS, SNS, IAM, STS, CloudFormation.
    Query,
    /// JSON protocol: JSON body with `X-Amz-Target` header.
    /// Used by: SSM, EventBridge, DynamoDB, Secrets Manager, CloudWatch Logs, KMS.
    Json { target_prefix: &'static str },
    /// REST protocol: HTTP method + path routing.
    /// Used by: S3, Lambda.
    Rest,
}

/// Result of probing a single test variant.
#[derive(Debug)]
pub struct ProbeResult {
    pub variant_name: String,
    pub status: ProbeStatus,
    pub http_status: u16,
    pub response_body: String,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeStatus {
    /// Response looks correct (shape matches, or expected error received).
    Pass,
    /// Response shape doesn't match the model.
    ShapeMismatch(String),
    /// Action is not implemented in fakecloud.
    NotImplemented,
    /// Unexpected server error (500, panic, etc.).
    Crash(String),
    /// Expected an error but got success, or vice versa.
    UnexpectedResult(String),
}

impl std::fmt::Display for ProbeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProbeStatus::Pass => write!(f, "PASS"),
            ProbeStatus::ShapeMismatch(msg) => write!(f, "SHAPE_MISMATCH: {}", msg),
            ProbeStatus::NotImplemented => write!(f, "NOT_IMPLEMENTED"),
            ProbeStatus::Crash(msg) => write!(f, "CRASH: {}", msg),
            ProbeStatus::UnexpectedResult(msg) => write!(f, "UNEXPECTED: {}", msg),
        }
    }
}

/// Probe a single test variant against a running fakecloud server.
/// If `model` and `output_shape_id` are provided, also validates the response shape.
pub fn probe_variant(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
) -> ProbeResult {
    probe_variant_with_model(
        client,
        endpoint,
        service_name,
        operation_name,
        variant,
        None,
    )
}

/// Probe a variant with optional shape validation against the Smithy model.
pub fn probe_variant_with_model(
    client: &reqwest::blocking::Client,
    endpoint: &str,
    service_name: &str,
    operation_name: &str,
    variant: &TestVariant,
    model_info: Option<(&ServiceModel, &str)>,
) -> ProbeResult {
    let protocol = service_protocol(service_name);
    let start = std::time::Instant::now();

    let result = match protocol {
        Protocol::Query => probe_query(
            client,
            endpoint,
            service_name,
            operation_name,
            variant,
            model_info.map(|(m, _)| m),
        ),
        Protocol::Json { target_prefix } => {
            probe_json(client, endpoint, target_prefix, operation_name, variant)
        }
        Protocol::Rest => probe_rest(
            client,
            endpoint,
            service_name,
            operation_name,
            variant,
            model_info.map(|(m, _)| m),
        ),
    };

    let duration_ms = start.elapsed().as_millis() as u64;

    // Resolve the operation's declared error wire codes once so the
    // classifier can distinguish real handler-emitted exceptions from
    // fakecloud's own routing-miss 4xxs.
    //
    // Strict mode: only the op's own `errors:` list counts. We do NOT union
    // every @error-tagged shape from the service model (that was the #1342
    // loosening) — if a service genuinely emits an error AWS Smithy doesn't
    // declare, we want it surfaced as UNDECLARED_ERROR so it gets reported
    // against the right service, not silently accepted.
    //
    // For each declared shape we derive the wire code: the
    // `aws.protocols#awsQueryError.code` trait if present AND the service
    // wire format is awsQuery or awsQueryCompatible (covers IAM/RDS/etc.
    // where the shape name and `<Code>` differ, and SQS which is awsJson1.0
    // + awsQueryCompatible), else the shape's short name (after `#`).
    //
    // Why the protocol gate: a handful of awsJson1.x services (KMS, ACM,
    // DynamoDB, SSM, application-autoscaling) carry `awsQueryError` traits
    // on their error shapes for historical reasons (the same shape is
    // shared with sibling awsQuery models). For awsJson1.x wire encoding
    // the trait is inert — the wire `__type` is the shape's short name.
    // Using the trait there mis-resolves codes like `NotFoundException`
    // to `NotFound` and flags every real handler error as undeclared.
    let honor_query_error_trait =
        matches!(protocol, Protocol::Query) || is_aws_query_compatible_service(service_name);
    let op_error_shapes: Option<Vec<String>> = model_info.map(|(m, _)| {
        let declared_shape_ids: Vec<String> = m
            .operations
            .iter()
            .find(|o| o.name == operation_name)
            .map(|op| op.error_shapes.clone())
            .unwrap_or_default();
        declared_shape_ids
            .iter()
            .map(|shape_id| {
                if honor_query_error_trait {
                    if let Some(shape) = m.shapes.get(shape_id) {
                        if let Some(code) = &shape.traits.aws_query_error_code {
                            return code.clone();
                        }
                    }
                }
                shape_id.rsplit('#').next().unwrap_or(shape_id).to_string()
            })
            .collect()
    });

    match result {
        Ok((status_code, body)) => {
            let mut probe_result = classify_response(
                &variant.name,
                status_code,
                &body,
                &variant.expectation,
                duration_ms,
                op_error_shapes.as_deref(),
                service_name,
            );

            // Run shape validation on successful responses
            if probe_result.status == ProbeStatus::Pass
                && (200..300).contains(&status_code)
                && !body.is_empty()
            {
                let mut all_violations = Vec::new();
                if let Some((model, output_shape_id)) = model_info {
                    all_violations.extend(shape_validator::validate_response(
                        model,
                        output_shape_id,
                        &body,
                        protocol,
                    ));
                }
                // Strategy 7 (`examples_diff`): the variant carries a documented
                // response from the operation's `@examples` trait. Deep-diff
                // it against the live response — every leaf in the documented
                // output must exist (with matching JSON type) in actual. Catches
                // optional-but-always-present fields that shape_validator can't
                // see (#816).
                if let Some(documented) = variant.expected_output.as_ref() {
                    if let Ok(actual) = serde_json::from_str::<serde_json::Value>(&body) {
                        if let Some((model, output_shape_id)) = model_info {
                            all_violations.extend(
                                shape_validator::diff_against_example_with_model(
                                    &actual,
                                    documented,
                                    model,
                                    output_shape_id,
                                ),
                            );
                        } else {
                            all_violations
                                .extend(shape_validator::diff_against_example(&actual, documented));
                        }
                    }
                }
                // Strategy 8 (`round_trip`): chase the Create with the
                // discovered Get/Describe, assert each input field echoed.
                // Only meaningful when we have a model to find the followup
                // operation in.
                if let (Some(followup), Some((model, _))) = (variant.followup.as_ref(), model_info)
                {
                    let response_body: Option<serde_json::Value> = serde_json::from_str(&body).ok();
                    all_violations.extend(run_round_trip_followup(
                        client,
                        endpoint,
                        service_name,
                        variant,
                        response_body.as_ref(),
                        followup,
                        model,
                    ));
                }
                if !all_violations.is_empty() {
                    let msg = all_violations
                        .iter()
                        .take(5)
                        .map(|v| v.to_string())
                        .collect::<Vec<_>>()
                        .join("; ");
                    probe_result.status = ProbeStatus::ShapeMismatch(msg);
                }
            }

            probe_result
        }
        Err(e) => {
            let msg = if e.contains("timed out") || e.contains("timeout") {
                format!("Request timed out (>30s): {}", e)
            } else {
                format!("Request failed: {}", e)
            };
            ProbeResult {
                variant_name: variant.name.clone(),
                status: ProbeStatus::Crash(msg),
                http_status: 0,
                response_body: String::new(),
                duration_ms,
            }
        }
    }
}

/// REST services with a hand-curated `rest_request_config` table. These keep
/// their hardcoded entries; everything else falls back to the generic
/// `@http`-trait-driven request builder.
const SERVICES_WITH_HARDCODED_REST: &[&str] = &["lambda", "s3"];

/// `(method, url, headers, body)` tuple produced by the REST request builders.
type RestRequestParts = (
    reqwest::Method,
    String,
    Vec<(String, String)>,
    Option<String>,
);

/// Look up a JSON field tolerating PascalCase vs camelCase first-letter
/// differences. restJson1 services with `@jsonName` (apigatewayv2) declare
/// PascalCase member names but serialise camelCase on the wire — so the
/// probe needs to find values under either casing when crossing between
/// model-derived names and observed wire keys.
pub(crate) fn lookup_field_any_case<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    member: &str,
) -> Option<&'a serde_json::Value> {
    if let Some(v) = obj.get(member) {
        return Some(v);
    }
    let mut chars = member.chars();
    let first = chars.next()?;
    let alt = if first.is_ascii_uppercase() {
        let mut s = String::with_capacity(member.len());
        s.push(first.to_ascii_lowercase());
        s.extend(chars);
        s
    } else if first.is_ascii_lowercase() {
        let mut s = String::with_capacity(member.len());
        s.push(first.to_ascii_uppercase());
        s.extend(chars);
        s
    } else {
        return None;
    };
    obj.get(&alt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smithy::{Member, Operation, Shape, ShapeTraits, ShapeType};

    #[test]
    fn service_protocol_covers_every_shipped_service() {
        // Every Smithy service_name fakecloud ships must have an explicit
        // protocol mapping. Falling through to the `_ => Query` default
        // misroutes JSON/REST services as form-encoded `Action=` requests
        // that get caught by APIGW's catch-all and silently classified as
        // Pass — masking real conformance gaps. See issue surfaced when
        // ECS reported 76/76 pass while only 60 ops were actually routed.
        let cases = [
            ("sqs", Protocol::Query),
            ("sns", Protocol::Query),
            ("iam", Protocol::Query),
            ("sts", Protocol::Query),
            ("cloudformation", Protocol::Query),
            ("rds", Protocol::Query),
            ("ec2", Protocol::Query),
            ("elasticache", Protocol::Query),
            ("elasticbeanstalk", Protocol::Query),
            ("elasticloadbalancing", Protocol::Query),
            (
                "ssm",
                Protocol::Json {
                    target_prefix: "AmazonSSM",
                },
            ),
            (
                "events",
                Protocol::Json {
                    target_prefix: "AWSEvents",
                },
            ),
            (
                "dynamodb",
                Protocol::Json {
                    target_prefix: "DynamoDB_20120810",
                },
            ),
            (
                "secretsmanager",
                Protocol::Json {
                    target_prefix: "secretsmanager",
                },
            ),
            (
                "logs",
                Protocol::Json {
                    target_prefix: "Logs_20140328",
                },
            ),
            (
                "kms",
                Protocol::Json {
                    target_prefix: "TrentService",
                },
            ),
            (
                "cognito-idp",
                Protocol::Json {
                    target_prefix: "AWSCognitoIdentityProviderService",
                },
            ),
            (
                "cognito-identity",
                Protocol::Json {
                    target_prefix: "AWSCognitoIdentityService",
                },
            ),
            (
                "kinesis",
                Protocol::Json {
                    target_prefix: "Kinesis_20131202",
                },
            ),
            (
                "ecr",
                Protocol::Json {
                    target_prefix: "AmazonEC2ContainerRegistry_V20150921",
                },
            ),
            (
                "ecs",
                Protocol::Json {
                    target_prefix: "AmazonEC2ContainerServiceV20141113",
                },
            ),
            (
                "states",
                Protocol::Json {
                    target_prefix: "AWSStepFunctions",
                },
            ),
            ("s3", Protocol::Rest),
            ("lambda", Protocol::Rest),
            ("apigateway", Protocol::Rest),
            ("ses", Protocol::Rest),
            ("bedrock", Protocol::Rest),
            ("bedrock-runtime", Protocol::Rest),
            ("scheduler", Protocol::Rest),
            ("pipes", Protocol::Rest),
        ];
        for (svc, expected) in cases {
            let got = service_protocol(svc);
            assert_eq!(got, expected, "wrong protocol for {svc}");
        }
    }

    fn op_with_http(name: &str, method: &str, uri: &str, input_shape_id: &str) -> Operation {
        Operation {
            name: name.to_string(),
            input_shape: Some(input_shape_id.to_string()),
            output_shape: None,
            error_shapes: Vec::new(),
            http_method: Some(method.to_string()),
            http_uri: Some(uri.to_string()),
            http_code: Some(200),
        }
    }

    fn member(name: &str, target: &str, traits: ShapeTraits) -> Member {
        Member {
            name: name.to_string(),
            target: target.to_string(),
            required: false,
            traits,
        }
    }

    fn structure_shape(id: &str, members: Vec<Member>) -> Shape {
        Shape {
            shape_id: id.to_string(),
            shape_type: ShapeType::Structure { members },
            traits: ShapeTraits::default(),
        }
    }

    fn string_shape(id: &str, traits: ShapeTraits) -> Shape {
        Shape {
            shape_id: id.to_string(),
            shape_type: ShapeType::String { enum_values: None },
            traits,
        }
    }

    fn model_with(op: Operation, shapes: Vec<Shape>) -> ServiceModel {
        let mut map = HashMap::new();
        for s in shapes {
            map.insert(s.shape_id.clone(), s);
        }
        ServiceModel {
            service_name: "test".to_string(),
            operations: vec![op],
            shapes: map,
        }
    }

    fn label_traits() -> ShapeTraits {
        ShapeTraits {
            http_label: true,
            ..ShapeTraits::default()
        }
    }

    fn query_traits(name: &str) -> ShapeTraits {
        ShapeTraits {
            http_query: Some(name.to_string()),
            ..ShapeTraits::default()
        }
    }

    fn header_traits(name: &str) -> ShapeTraits {
        ShapeTraits {
            http_header: Some(name.to_string()),
            ..ShapeTraits::default()
        }
    }

    fn payload_traits() -> ShapeTraits {
        ShapeTraits {
            http_payload: true,
            ..ShapeTraits::default()
        }
    }

    #[test]
    fn label_substitution_basic() {
        let op = op_with_http("GetApi", "GET", "/v2/apis/{ApiId}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("ApiId", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"ApiId": "abc123"});
        let (method, url, headers, body) =
            build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(method, reqwest::Method::GET);
        assert_eq!(url, "/v2/apis/abc123");
        assert!(headers.is_empty());
        assert!(body.is_none(), "GET has no body");
    }

    #[test]
    fn greedy_label_preserves_slashes() {
        let op = op_with_http("X", "GET", "/foo/{Path+}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Path", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Path": "a/b/c"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo/a/b/c");
    }

    #[test]
    fn non_greedy_label_encodes_slashes() {
        let op = op_with_http("X", "GET", "/foo/{Name}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Name", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Name": "a/b"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo/a%2Fb");
    }

    #[test]
    fn query_optional_omitted() {
        let op = op_with_http("X", "GET", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![member("BasePath", "#String", query_traits("basepath"))],
                ),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({}); // BasePath absent
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo");
    }

    #[test]
    fn query_present_emitted() {
        let op = op_with_http("X", "GET", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![member("BasePath", "#String", query_traits("basepath"))],
                ),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"BasePath": "hello"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo?basepath=hello");
    }

    #[test]
    fn header_extracted_out_of_body() {
        let op = op_with_http("X", "POST", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![
                        member("Idempotency", "#String", header_traits("x-idem")),
                        member("Other", "#String", ShapeTraits::default()),
                    ],
                ),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Idempotency": "abc", "Other": "keep"});
        let (_, _, headers, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(headers, vec![("x-idem".to_string(), "abc".to_string())]);
        let body: serde_json::Value = serde_json::from_str(&body.unwrap()).unwrap();
        assert_eq!(body, serde_json::json!({"Other": "keep"}));
    }

    #[test]
    fn payload_member_only_body() {
        let op = op_with_http("X", "PUT", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Body", "#String", payload_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Body": "raw-openapi-doc"});
        let (_, _, _, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(body.unwrap(), "raw-openapi-doc");
    }

    #[test]
    fn delete_without_payload_has_no_body() {
        let op = op_with_http("X", "DELETE", "/foo/{Id}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Id", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Id": "abc"});
        let (method, _, _, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(method, reqwest::Method::DELETE);
        assert!(body.is_none());
    }

    #[test]
    fn missing_label_leaves_placeholder() {
        // When a required label is absent, keep the literal {Name} so the server
        // returns a routing failure (not a silent 500). Ensures the variant
        // surfaces as a SHAPE_MISMATCH/UNEXPECTED rather than being hidden.
        let op = op_with_http("X", "GET", "/foo/{ApiId}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("ApiId", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({}); // ApiId missing
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo/{ApiId}");
    }

    #[test]
    fn literal_query_in_template_merged_with_computed() {
        let op = op_with_http("X", "GET", "/x?action=foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("P", "#String", query_traits("p"))]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"P": "v"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/x?action=foo&p=v");
    }

    #[test]
    fn list_valued_query_repeats() {
        let op = op_with_http("X", "GET", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![member("Tags", "#StringList", query_traits("tag"))],
                ),
                Shape {
                    shape_id: "#StringList".to_string(),
                    shape_type: ShapeType::List {
                        member_target: "#String".to_string(),
                    },
                    traits: ShapeTraits::default(),
                },
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Tags": ["a", "b"]});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo?tag=a&tag=b");
    }

    #[test]
    fn delete_with_payload_keeps_body() {
        // `@httpPayload` overrides the default "DELETE = bodyless" rule. Rare
        // in practice but AWS models do use it for some delete-with-filter-doc
        // shapes; ensure the builder emits a body in that case.
        let op = op_with_http("X", "DELETE", "/foo", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Body", "#String", payload_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Body": "delete-me"});
        let (method, _, _, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(method, reqwest::Method::DELETE);
        assert_eq!(body.unwrap(), "delete-me");
    }

    #[test]
    fn patch_method_keeps_body() {
        // APIGWv2 Update* ops use PATCH. Regression guard for the
        // `has_body_method` check: don't treat PATCH as bodyless.
        let op = op_with_http("X", "PATCH", "/foo/{Id}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape(
                    "#Input",
                    vec![
                        member("Id", "#String", label_traits()),
                        member("Description", "#String", ShapeTraits::default()),
                    ],
                ),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Id": "abc", "Description": "updated"});
        let (method, _, _, body) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(method, reqwest::Method::PATCH);
        let body: serde_json::Value = serde_json::from_str(&body.unwrap()).unwrap();
        assert_eq!(body, serde_json::json!({"Description": "updated"}));
    }

    #[test]
    fn percent_encoding_in_label() {
        let op = op_with_http("X", "GET", "/foo/{Id}", "#Input");
        let model = model_with(
            op.clone(),
            vec![
                structure_shape("#Input", vec![member("Id", "#String", label_traits())]),
                string_shape("#String", ShapeTraits::default()),
            ],
        );
        let input = serde_json::json!({"Id": "a b#c"});
        let (_, url, _, _) = build_http_request_from_model(&op, &model, &input).unwrap();
        assert_eq!(url, "/foo/a%20b%23c");
    }

    #[test]
    fn classify_unknown_path_is_not_implemented() {
        // API Gateway v2 emits `Unknown path: ...` when resolve_action
        // can't match a URL. Must classify as NotImplemented, not Pass.
        let body = r#"{"__type":"NotFoundException","message":"Unknown path: /v2/domainnames"}"#;
        let result = classify_response("v1", 404, body, &Expectation::Success, 0, None, "");
        assert_eq!(result.status, ProbeStatus::NotImplemented);
    }

    #[test]
    fn classify_unknown_operation_is_not_implemented() {
        // Lambda emits `UnknownOperationException` for URLs its
        // resolve_action doesn't recognize.
        let body = r#"{"__type":"UnknownOperationException","message":"Unknown operation: /foo"}"#;
        let result = classify_response("v1", 404, body, &Expectation::Success, 0, None, "");
        assert_eq!(result.status, ProbeStatus::NotImplemented);
    }

    #[test]
    fn classify_action_not_implemented_string() {
        // `ActionNotImplemented` error maps to the substring "not implemented"
        // in the response body.
        let body =
            r#"{"__type":"InvalidAction","message":"action Foo not implemented for service bar"}"#;
        let result = classify_response("v1", 501, body, &Expectation::Success, 0, None, "");
        assert_eq!(result.status, ProbeStatus::NotImplemented);
    }

    #[test]
    fn classify_legit_resource_not_found_is_pass() {
        // AWS-shaped `ResourceNotFoundException` for a synthetic id is a
        // legitimate response from an implemented handler; must not be
        // confused with NotImplemented. Strict mode: `declared` carries
        // wire codes (not shape IDs), pre-resolved by the caller.
        let body =
            r#"{"__type":"ResourceNotFoundException","message":"Function not found: test-fn"}"#;
        let declared = vec!["ResourceNotFoundException".to_string()];
        let result = classify_response(
            "v1",
            404,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    // -- error-shape-driven 4xx classification --

    #[test]
    fn classify_s3_common_error_codes_pass_on_any_op() {
        // S3's Smithy file enumerates almost no per-operation errors, but the
        // published "Error Responses" reference documents these as responses
        // any op can give. A handler returning one ran correctly, so the
        // classifier must not read it as an undeclared error.
        for (status, code) in [
            (400, "MalformedXML"),
            (400, "InvalidArgument"),
            (404, "NoSuchBucketPolicy"),
            (404, "NoSuchCORSConfiguration"),
            (404, "NoSuchLifecycleConfiguration"),
            (404, "NoSuchTagSet"),
            (404, "NoSuchUpload"),
        ] {
            let body = format!("<Error><Code>{code}</Code><Message>m</Message></Error>");
            let result = classify_response(
                "v1",
                status,
                &body,
                &Expectation::Success,
                0,
                Some(&["SomethingElse".to_string()]),
                "s3",
            );
            assert_eq!(
                result.status,
                ProbeStatus::Pass,
                "{code} should pass for s3"
            );

            // The same code from a service with no shared-error list stays
            // undeclared — the allowlist is per service, not global.
            let result = classify_response(
                "v1",
                status,
                &body,
                &Expectation::Success,
                0,
                Some(&["SomethingElse".to_string()]),
                "dynamodb",
            );
            assert!(
                matches!(result.status, ProbeStatus::UnexpectedResult(_)),
                "{code} must not pass for a service without it in its list"
            );
        }
    }

    #[test]
    fn classify_ec2_notfound_codes_pass_on_any_op() {
        // EC2's Smithy model declares no per-operation `errors:` at all, so
        // every `.NotFound` a handler returns for a synthetic id has to come
        // from the service-wide list or it reads as an undeclared error.
        for code in [
            "InvalidVpcEndpointServiceId.NotFound",
            "InvalidVerifiedAccessEndpointId.NotFound",
            "InvalidPublicIpv4PoolID.NotFound",
            "InvalidVpcID.NotFound",
            "InvalidSubnetID.NotFound",
        ] {
            let body =
                format!("<Response><Errors><Error><Code>{code}</Code></Error></Errors></Response>");
            let result = classify_response(
                "v1",
                400,
                &body,
                &Expectation::Success,
                0,
                Some(&["SomethingElse".to_string()]),
                "ec2",
            );
            assert_eq!(
                result.status,
                ProbeStatus::Pass,
                "{code} should pass for ec2"
            );
        }
    }

    #[test]
    fn classify_404_with_no_aws_error_shape_fails() {
        // Mirrors #817: routing miss returns 404 with a body that has no
        // AWS error code. Must NOT pass — that's the gaming we're closing.
        let body = r#"{"message":"Function not found"}"#;
        let result = classify_response("v1", 404, body, &Expectation::Success, 0, None, "");
        assert!(matches!(result.status, ProbeStatus::UnexpectedResult(_)));
    }

    #[test]
    fn classify_404_with_undeclared_error_fails() {
        // Handler-emitted error that doesn't appear in the op's Smithy
        // error_shapes list — could be a stray fakecloud error type that
        // AWS would never return. Flag it.
        let body = r#"{"__type":"WeirdInternalException","message":"oops"}"#;
        let declared = vec![
            "ResourceNotFoundException".to_string(),
            "ValidationException".to_string(),
        ];
        let result = classify_response(
            "v1",
            404,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert!(
            matches!(result.status, ProbeStatus::UnexpectedResult(_)),
            "got {:?}",
            result.status
        );
    }

    #[test]
    fn classify_400_with_xml_error_code_passes() {
        // restXml + awsQuery both encode the error code in <Code>X</Code>.
        let body =
            r#"<?xml version="1.0"?><Error><Code>NoSuchBucket</Code><Message>x</Message></Error>"#;
        let declared = vec!["NoSuchBucket".to_string()];
        let result = classify_response(
            "v1",
            404,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    #[test]
    fn classify_400_query_protocol_error_passes() {
        // awsQuery (IAM, RDS, …) wraps the error in <ErrorResponse><Error>...
        let body = r#"<ErrorResponse><Error><Code>InvalidParameterValue</Code><Message>x</Message></Error></ErrorResponse>"#;
        let declared = vec!["InvalidParameterValue".to_string()];
        let result = classify_response(
            "v1",
            400,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    #[test]
    fn classify_4xx_no_op_model_lenient() {
        // Op model unavailable (caller didn't pass declared errors): any
        // AWS-shaped error counts as a real handler response.
        let body = r#"{"__type":"SomeException"}"#;
        let result = classify_response("v1", 400, body, &Expectation::Success, 0, None, "");
        assert_eq!(result.status, ProbeStatus::Pass);
    }

    #[test]
    fn classify_4xx_empty_error_shapes_strict() {
        // Op declares NO errors. Any 4xx is undeclared, so surface it
        // rather than silently passing.
        let body = r#"{"__type":"SomeException"}"#;
        let declared: Vec<String> = Vec::new();
        let result = classify_response(
            "v1",
            400,
            body,
            &Expectation::Success,
            0,
            Some(&declared),
            "",
        );
        assert!(
            matches!(result.status, ProbeStatus::UnexpectedResult(_)),
            "expected UnexpectedResult, got {:?}",
            result.status
        );
    }

    #[test]
    fn extract_error_code_from_namespaced_type() {
        let body = r#"{"__type":"com.amazonaws.lambda#ResourceNotFoundException"}"#;
        assert_eq!(
            extract_aws_error_code(body),
            Some("ResourceNotFoundException".to_string())
        );
    }

    #[test]
    fn extract_error_code_from_xml() {
        let body = r#"<Error><Code>NoSuchBucket</Code></Error>"#;
        assert_eq!(
            extract_aws_error_code(body),
            Some("NoSuchBucket".to_string())
        );
    }

    #[test]
    fn extract_error_code_returns_none_for_plain_message() {
        // Routing-miss body shape — no recognisable AWS error code.
        let body = r#"{"message":"Unknown URL"}"#;
        assert_eq!(extract_aws_error_code(body), None);
    }

    #[test]
    fn aws_query_error_trait_gated_by_protocol() {
        // KMS is awsJson1.1; its `NotFoundException` shape carries a
        // legacy `awsQueryError.code = "NotFound"` trait that is inert
        // for JSON wire encoding. The probe must NOT honor the trait
        // for non-query services — the wire `__type` is the shape's
        // short name. SQS (awsJson1.0 + awsQueryCompatible) is the
        // only awsJson service where the trait does apply.
        assert!(matches!(service_protocol("kms"), Protocol::Json { .. }));
        assert!(!is_aws_query_compatible_service("kms"));
        assert!(is_aws_query_compatible_service("sqs"));
        assert!(matches!(service_protocol("iam"), Protocol::Query));
    }
}

mod followup;
mod protocols;
mod response;
mod rest_request;
mod service_detection;
use followup::*;
use protocols::*;
use response::*;
use rest_request::*;
use service_detection::*;
