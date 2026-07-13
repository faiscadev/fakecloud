use super::*;
use crate::generated::{Field, OpMeta, Verb, K, OPS};
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::AwsRequest;
use parking_lot::RwLock;
use serde_json::{json, Map, Value};

fn svc() -> SageMakerService {
    let state: SharedSageMakerState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    SageMakerService::new(state)
}

fn mk_req(action: &str, body: Value) -> AwsRequest {
    let body_bytes = if body.is_null() {
        bytes::Bytes::new()
    } else {
        bytes::Bytes::from(serde_json::to_vec(&body).unwrap())
    };
    AwsRequest {
        service: "sagemaker".into(),
        action: action.into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        request_id: "req".into(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: body_bytes,
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".into(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn run(s: &SageMakerService, action: &str, body: Value) -> Result<AwsResponse, AwsServiceError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(s.handle(mk_req(action, body)))
}

fn expect_err(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match r {
        Ok(_) => panic!("expected an error, got Ok"),
        Err(e) => e,
    }
}

fn resp_json(resp: &AwsResponse) -> Value {
    match &resp.body {
        fakecloud_core::service::ResponseBody::Bytes(b) => {
            serde_json::from_slice(b).unwrap_or(Value::Null)
        }
        _ => Value::Null,
    }
}

/// Build a model-valid request body from an operation's required rules.
fn build_success_body(meta: &OpMeta) -> Value {
    let mut body = Map::new();
    for rule in meta.rules {
        if !rule.req {
            continue;
        }
        let v = match rule.kind {
            K::Str | K::Blob => {
                if let Some(first) = rule.enums.first() {
                    Value::String((*first).to_string())
                } else {
                    let min = rule.min_len.unwrap_or(1).max(1) as usize;
                    let max = rule.max_len.map(|m| m as usize).unwrap_or(min.max(3));
                    Value::String("a".repeat(min.min(max.max(1)).max(1)))
                }
            }
            K::Ts => Value::from(1_752_324_947.041_f64),
            K::Int | K::Num => Value::Number(rule.min_val.unwrap_or(1).into()),
            K::Bool => Value::Bool(true),
            K::List => Value::Array(vec![]),
            K::Map | K::Struct => Value::Object(Map::new()),
        };
        body.insert(rule.wire.to_string(), v);
    }
    if body.is_empty() {
        Value::Null
    } else {
        Value::Object(body)
    }
}

fn accepted_error(meta: &OpMeta, code: &str) -> bool {
    meta.errors.contains(&code) || COMMON_ERRORS.contains(&code)
}

/// Whether a JSON value's type matches a modelled member kind (mirrors the
/// engine / CI shape validator: timestamps wire as numbers).
fn json_kind_ok(kind: K, v: &Value) -> bool {
    match kind {
        K::Str | K::Blob => v.is_string(),
        K::Ts => v.is_number() || v.is_string(),
        K::Int | K::Num => v.is_number(),
        K::Bool => v.is_boolean(),
        K::List => v.is_array(),
        K::Map | K::Struct => v.is_object(),
    }
}

/// Assert that every `@required` member in `fields` is present in `obj` with the
/// right JSON type, recursing into required structures and into the required
/// members of every present list element. This mirrors what the CI conformance
/// shape-validator checks and is what previously shipped broken.
fn check_required(
    op: &str,
    prefix: &str,
    obj: &Value,
    fields: &[Field],
    failures: &mut Vec<String>,
) {
    let Some(map) = obj.as_object() else {
        failures.push(format!("{op}: {prefix} is not an object"));
        return;
    };
    for f in fields {
        match map.get(f.wire) {
            None | Some(Value::Null) => {
                failures.push(format!("{op}: missing required '{}' at {prefix}", f.wire));
            }
            Some(v) => {
                if !json_kind_ok(f.kind, v) {
                    failures.push(format!(
                        "{op}: required '{}' at {prefix} has wrong type ({v})",
                        f.wire
                    ));
                    continue;
                }
                match f.kind {
                    // A union has no @required members; presence + object type is
                    // all the shape validator checks (mirrors CI).
                    K::Struct if f.is_union => {}
                    K::Struct => {
                        check_required(op, &format!("{prefix}.{}", f.wire), v, f.children, failures)
                    }
                    K::List if f.elem_kind == K::Struct => {
                        if let Some(arr) = v.as_array() {
                            for (i, el) in arr.iter().enumerate() {
                                check_required(
                                    op,
                                    &format!("{prefix}.{}[{i}]", f.wire),
                                    el,
                                    f.children,
                                    failures,
                                );
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

// ---------- in-process conformance proxy ----------
//
// The live-server conformance probe cannot run in the local sandbox, so this
// test reproduces the probe's core pass criteria in-process: for every one of
// the ~403 operations, a model-valid request must NOT crash (HTTP 500) and must
// return either a 2xx response or a 4xx whose error code is declared in the
// operation's Smithy `errors` list or in SageMaker's service-wide common errors.
//
// It ALSO validates every success response against the model's output shape:
// every `@required` output member (recursively, including nested structures and
// list elements) must be present and of the right JSON type. This mirrors the CI
// shape-validator, whose `SHAPE_MISMATCH: missing required field` failures this
// suite must now catch (they previously shipped because the test only checked
// the status code).
#[test]
fn every_operation_passes_success_criteria() {
    let s = svc();
    let mut failures: Vec<String> = Vec::new();
    for meta in OPS {
        let body = build_success_body(meta);
        match run(&s, meta.op, body) {
            Ok(resp) => {
                if !resp.status.is_success() {
                    failures.push(format!("{}: unexpected status {}", meta.op, resp.status));
                } else {
                    let json = resp_json(&resp);
                    check_required(meta.op, "$", &json, meta.req_out, &mut failures);
                }
            }
            Err(AwsServiceError::AwsError { status, code, .. }) => {
                let sc = status.as_u16();
                if sc == 500 {
                    failures.push(format!("{}: HTTP 500 crash ({code})", meta.op));
                } else if (400..500).contains(&sc) {
                    if !accepted_error(meta, &code) {
                        failures.push(format!(
                            "{}: undeclared error '{}' (not in {:?} or common)",
                            meta.op, code, meta.errors
                        ));
                    }
                } else {
                    failures.push(format!("{}: unexpected status {}", meta.op, sc));
                }
            }
            Err(other) => failures.push(format!("{}: non-AWS error {other:?}", meta.op)),
        }
    }
    assert!(
        failures.is_empty(),
        "{} operations failed the success criteria:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

// Every non-scalar List operation must, when it has elements, emit elements
// carrying their required fields (the `ListAI*` `$.X[0].Status` failure class).
// Seed one bare record per family so the list is non-empty, then validate the
// element against the model's element required-member tree.
#[test]
fn list_elements_carry_required_fields() {
    let mut failures: Vec<String> = Vec::new();
    for meta in OPS {
        if !matches!(meta.verb, Verb::List) || meta.list_scalar || meta.req_elem.is_empty() {
            continue;
        }
        // `ListTags` is served by the resource-specific tag handler (its elements
        // come from the ARN-keyed tag store, not the generic resource engine), so
        // seeding a resource family does not populate it.
        if meta.op == "ListTags" {
            continue;
        }
        let s = svc();
        {
            let mut g = s.state.write();
            let data = g.get_or_create("000000000000");
            data.put_resource(meta.family, "seed", Value::Object(Map::new()));
        }
        let listed = resp_json(&run(&s, meta.op, build_success_body(meta)).unwrap());
        let field = meta
            .list_field
            .expect("non-scalar list op has a list field");
        let arr = listed[field]
            .as_array()
            .unwrap_or_else(|| panic!("{}: {field} must be an array", meta.op));
        assert!(
            !arr.is_empty(),
            "{}: seeded list must have an element",
            meta.op
        );
        check_required(
            meta.op,
            &format!("$.{field}[0]"),
            &arr[0],
            meta.req_elem,
            &mut failures,
        );
    }
    assert!(
        failures.is_empty(),
        "{} list elements missing required fields:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn create_describe_list_round_trip_model() {
    let s = svc();
    // Create a model.
    let created = run(
        &s,
        "CreateModel",
        json!({"ModelName": "my-model", "ExecutionRoleArn": "arn:aws:iam::000000000000:role/r"}),
    )
    .unwrap();
    let body = resp_json(&created);
    let arn = body["ModelArn"].as_str().unwrap();
    assert!(arn.starts_with("arn:aws:sagemaker:us-east-1:000000000000:model/my-model"));

    // Describe echoes the input + a numeric CreationTime.
    let described = resp_json(&run(&s, "DescribeModel", json!({"ModelName": "my-model"})).unwrap());
    assert_eq!(described["ModelName"], "my-model");
    assert_eq!(
        described["ExecutionRoleArn"],
        "arn:aws:iam::000000000000:role/r"
    );
    assert!(
        described["CreationTime"].is_number(),
        "CreationTime must be a numeric epoch timestamp, got {:?}",
        described["CreationTime"]
    );
    assert_eq!(described["ModelArn"], arn);

    // List returns a Models array whose summary carries the required subfields.
    let listed = resp_json(&run(&s, "ListModels", Value::Null).unwrap());
    let models = listed["Models"].as_array().unwrap();
    assert_eq!(models.len(), 1);
    let m = &models[0];
    assert_eq!(m["ModelName"], "my-model");
    assert!(m["ModelArn"].is_string());
    assert!(m["CreationTime"].is_number());

    // Delete then Describe -> ResourceNotFound.
    run(&s, "DeleteModel", json!({"ModelName": "my-model"})).unwrap();
    let err = expect_err(run(&s, "DescribeModel", json!({"ModelName": "my-model"})));
    match err {
        AwsServiceError::AwsError { code, .. } => assert_eq!(code, "ResourceNotFound"),
        other => panic!("expected ResourceNotFound, got {other:?}"),
    }
}

#[test]
fn describe_missing_resource_returns_not_found() {
    let s = svc();
    let err = expect_err(run(
        &s,
        "DescribeEndpointConfig",
        json!({"EndpointConfigName": "nope"}),
    ));
    match err {
        AwsServiceError::AwsError { code, status, .. } => {
            assert_eq!(code, "ResourceNotFound");
            assert_eq!(status.as_u16(), 404);
        }
        other => panic!("expected ResourceNotFound, got {other:?}"),
    }
}

#[test]
fn missing_required_member_is_validation_error() {
    let s = svc();
    // CreateEndpointConfig requires EndpointConfigName + ProductionVariants.
    let err = expect_err(run(&s, "CreateEndpointConfig", json!({})));
    match err {
        AwsServiceError::AwsError { code, status, .. } => {
            assert_eq!(code, "ValidationException");
            assert_eq!(status.as_u16(), 400);
        }
        other => panic!("expected ValidationException, got {other:?}"),
    }
}

#[test]
fn tags_round_trip() {
    let s = svc();
    let arn = "arn:aws:sagemaker:us-east-1:000000000000:model/tagged";
    run(
        &s,
        "AddTags",
        json!({"ResourceArn": arn, "Tags": [{"Key": "env", "Value": "prod"}]}),
    )
    .unwrap();
    let listed = resp_json(&run(&s, "ListTags", json!({"ResourceArn": arn})).unwrap());
    let tags = listed["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Key"], "env");
    assert_eq!(tags[0]["Value"], "prod");

    run(
        &s,
        "DeleteTags",
        json!({"ResourceArn": arn, "TagKeys": ["env"]}),
    )
    .unwrap();
    let listed = resp_json(&run(&s, "ListTags", json!({"ResourceArn": arn})).unwrap());
    assert!(listed["Tags"].as_array().unwrap().is_empty());
}

#[test]
fn list_scalar_serialises_as_string_array() {
    // ListImages returns Images (structs); ListCandidatesForAutoMLJob etc. vary.
    // Use a scalar list op: pick one whose element is a plain string.
    let scalar_op = OPS
        .iter()
        .find(|m| matches!(m.verb, crate::generated::Verb::List) && m.list_scalar);
    if let Some(meta) = scalar_op {
        let s = svc();
        let listed = resp_json(&run(&s, meta.op, build_success_body(meta)).unwrap());
        if let Some(field) = meta.list_field {
            assert!(
                listed[field].is_array(),
                "{} list field {} must be an array",
                meta.op,
                field
            );
        }
    }
}

#[test]
fn unknown_action_is_not_implemented() {
    let s = svc();
    let err = expect_err(run(&s, "NotARealSageMakerOp", Value::Null));
    assert!(matches!(err, AwsServiceError::ActionNotImplemented { .. }));
}

// StartPipelineExecution is the only creation path for a pipeline execution, so
// it must persist a record the Describe / List siblings can resolve.
#[test]
fn pipeline_execution_action_persists() {
    let s = svc();
    let started = resp_json(
        &run(
            &s,
            "StartPipelineExecution",
            json!({"PipelineName": "p1", "ClientRequestToken": "a".repeat(32)}),
        )
        .unwrap(),
    );
    let arn = started["PipelineExecutionArn"].as_str().unwrap().to_string();
    assert!(arn.contains(":pipeline/p1/execution/"), "arn: {arn}");

    let described = resp_json(
        &run(
            &s,
            "DescribePipelineExecution",
            json!({"PipelineExecutionArn": arn}),
        )
        .unwrap(),
    );
    assert_eq!(described["PipelineExecutionArn"], arn);
    assert_eq!(described["PipelineExecutionStatus"], "Executing");

    let listed = resp_json(
        &run(&s, "ListPipelineExecutions", json!({"PipelineName": "p1"})).unwrap(),
    );
    let sums = listed["PipelineExecutionSummaries"].as_array().unwrap();
    assert!(sums.iter().any(|x| x["PipelineExecutionArn"] == arn));
}

// ImportHubContent is the only creation path for hub content.
#[test]
fn import_hub_content_action_persists() {
    let s = svc();
    let imported = resp_json(
        &run(
            &s,
            "ImportHubContent",
            json!({
                "HubName": "h1",
                "HubContentName": "c1",
                "HubContentType": "Model",
                "DocumentSchemaVersion": "1.0.0",
                "HubContentDocument": "{}"
            }),
        )
        .unwrap(),
    );
    assert!(imported["HubContentArn"].as_str().unwrap().contains("c1"));
    assert!(imported["HubArn"].as_str().unwrap().contains("h1"));

    let described = resp_json(
        &run(
            &s,
            "DescribeHubContent",
            json!({"HubName": "h1", "HubContentType": "Model", "HubContentName": "c1"}),
        )
        .unwrap(),
    );
    assert_eq!(described["HubContentName"], "c1");
    assert_eq!(described["HubName"], "h1");
}

// AddAssociation persists the edge; DeleteAssociation removes exactly it.
#[test]
fn association_action_persists_and_deletes() {
    let s = svc();
    let src = "arn:aws:sagemaker:us-east-1:000000000000:experiment/e";
    let dst = "arn:aws:sagemaker:us-east-1:000000000000:artifact/a";
    run(
        &s,
        "AddAssociation",
        json!({"SourceArn": src, "DestinationArn": dst}),
    )
    .unwrap();
    let listed = resp_json(&run(&s, "ListAssociations", Value::Null).unwrap());
    let sums = listed["AssociationSummaries"].as_array().unwrap();
    assert!(sums
        .iter()
        .any(|x| x["SourceArn"] == src && x["DestinationArn"] == dst));

    run(
        &s,
        "DeleteAssociation",
        json!({"SourceArn": src, "DestinationArn": dst}),
    )
    .unwrap();
    let listed = resp_json(&run(&s, "ListAssociations", Value::Null).unwrap());
    assert!(listed["AssociationSummaries"].as_array().unwrap().is_empty());
}

// NameContains must narrow a List result to matching records.
#[test]
fn list_name_contains_filters_results() {
    let s = svc();
    for name in ["alpha-model", "beta-model"] {
        run(&s, "CreateModel", json!({ "ModelName": name })).unwrap();
    }
    let listed = resp_json(&run(&s, "ListModels", json!({"NameContains": "alpha"})).unwrap());
    let models = listed["Models"].as_array().unwrap();
    assert_eq!(models.len(), 1);
    assert_eq!(models[0]["ModelName"], "alpha-model");
}

// A stored string timestamp must project to a numeric epoch on read, never an
// SDK-rejecting string.
#[test]
fn string_timestamp_coerced_to_number_on_read() {
    let s = svc();
    {
        let mut g = s.state.write();
        let data = g.get_or_create("000000000000");
        data.put_resource(
            "Model",
            "m",
            json!({"ModelName": "m", "CreationTime": "1752324947.041"}),
        );
    }
    let described = resp_json(&run(&s, "DescribeModel", json!({"ModelName": "m"})).unwrap());
    assert!(
        described["CreationTime"].is_number(),
        "CreationTime must coerce to a number, got {:?}",
        described["CreationTime"]
    );
}
