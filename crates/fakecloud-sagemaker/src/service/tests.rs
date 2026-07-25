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
        // Seed the record carrying the operation's required request members, so
        // resource-scoped list handlers (e.g. ListClusterNodes filtered by the
        // required ClusterName, ListPipelineExecutions by PipelineName) still
        // resolve the seeded record instead of filtering it out.
        let seed = match build_success_body(meta) {
            Value::Object(m) => Value::Object(m),
            _ => Value::Object(Map::new()),
        };
        {
            let mut g = s.state.write();
            let data = g.get_or_create("000000000000");
            data.put_resource(meta.family, "seed", seed);
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
    let arn = started["PipelineExecutionArn"]
        .as_str()
        .unwrap()
        .to_string();
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

    let listed =
        resp_json(&run(&s, "ListPipelineExecutions", json!({"PipelineName": "p1"})).unwrap());
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
    assert!(listed["AssociationSummaries"]
        .as_array()
        .unwrap()
        .is_empty());
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

// PutModelPackageGroupPolicy persists the ResourcePolicy; Get returns the
// stored value (not the placeholder "a"); Delete removes it (bug-hunt 1.24).
#[test]
fn model_package_group_policy_round_trips() {
    let s = svc();
    let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
    run(
        &s,
        "PutModelPackageGroupPolicy",
        json!({"ModelPackageGroupName": "grp", "ResourcePolicy": policy}),
    )
    .unwrap();

    let got = resp_json(
        &run(
            &s,
            "GetModelPackageGroupPolicy",
            json!({"ModelPackageGroupName": "grp"}),
        )
        .unwrap(),
    );
    assert_eq!(got["ResourcePolicy"], policy);

    // Delete removes it; a subsequent Get no longer returns the real policy.
    run(
        &s,
        "DeleteModelPackageGroupPolicy",
        json!({"ModelPackageGroupName": "grp"}),
    )
    .unwrap();
    let after = resp_json(
        &run(
            &s,
            "GetModelPackageGroupPolicy",
            json!({"ModelPackageGroupName": "grp"}),
        )
        .unwrap(),
    );
    assert_ne!(after["ResourcePolicy"], policy);
}

// RegisterDevices persists devices so DescribeDevice / ListDevices resolve
// them; UpdateDevices merges; DeregisterDevices removes (bug-hunt 1.24).
#[test]
fn register_devices_visible_to_read_siblings() {
    let s = svc();
    run(
        &s,
        "RegisterDevices",
        json!({
            "DeviceFleetName": "fleet1",
            "Devices": [
                {"DeviceName": "dev-a", "Description": "first", "IotThingName": "thing-a"},
                {"DeviceName": "dev-b", "Description": "second"},
            ],
        }),
    )
    .unwrap();

    // ListDevices sees both.
    let listed = resp_json(&run(&s, "ListDevices", json!({})).unwrap());
    let summaries = listed["DeviceSummaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 2);

    // DescribeDevice resolves a registered device with its fleet + description.
    let described = resp_json(
        &run(
            &s,
            "DescribeDevice",
            json!({"DeviceName": "dev-a", "DeviceFleetName": "fleet1"}),
        )
        .unwrap(),
    );
    assert_eq!(described["DeviceName"], "dev-a");
    assert_eq!(described["DeviceFleetName"], "fleet1");
    assert_eq!(described["Description"], "first");
    assert!(described["DeviceArn"]
        .as_str()
        .unwrap()
        .contains(":device/"));

    // UpdateDevices merges a new description while keeping the fleet.
    run(
        &s,
        "UpdateDevices",
        json!({
            "DeviceFleetName": "fleet1",
            "Devices": [{"DeviceName": "dev-a", "Description": "updated"}],
        }),
    )
    .unwrap();
    let after = resp_json(
        &run(
            &s,
            "DescribeDevice",
            json!({"DeviceName": "dev-a", "DeviceFleetName": "fleet1"}),
        )
        .unwrap(),
    );
    assert_eq!(after["Description"], "updated");
    assert_eq!(after["DeviceFleetName"], "fleet1");

    // DeregisterDevices removes one; the other remains.
    run(
        &s,
        "DeregisterDevices",
        json!({"DeviceFleetName": "fleet1", "DeviceNames": ["dev-a"]}),
    )
    .unwrap();
    let listed = resp_json(&run(&s, "ListDevices", json!({})).unwrap());
    assert_eq!(listed["DeviceSummaries"].as_array().unwrap().len(), 1);
    let err = expect_err(run(
        &s,
        "DescribeDevice",
        json!({"DeviceName": "dev-a", "DeviceFleetName": "fleet1"}),
    ));
    assert_eq!(err.code(), "ResourceNotFound");
}

// Enable/Disable persist the Service Catalog portfolio status; Get reads it back
// (default Disabled before any Enable) (bug-hunt 2026-07-19).
#[test]
fn servicecatalog_portfolio_status_round_trips() {
    let s = svc();
    // Default before any Enable/Disable is Disabled.
    let got = resp_json(&run(&s, "GetSagemakerServicecatalogPortfolioStatus", json!({})).unwrap());
    assert_eq!(got["Status"], "Disabled");

    run(&s, "EnableSagemakerServicecatalogPortfolio", json!({})).unwrap();
    let got = resp_json(&run(&s, "GetSagemakerServicecatalogPortfolioStatus", json!({})).unwrap());
    assert_eq!(got["Status"], "Enabled");

    run(&s, "DisableSagemakerServicecatalogPortfolio", json!({})).unwrap();
    let got = resp_json(&run(&s, "GetSagemakerServicecatalogPortfolioStatus", json!({})).unwrap());
    assert_eq!(got["Status"], "Disabled");
}

// RetryPipelineExecution transitions a stopped execution's status back to
// Executing, visible via DescribePipelineExecution (bug-hunt 2026-07-19).
#[test]
fn retry_pipeline_execution_transitions_status() {
    let s = svc();
    let started = resp_json(
        &run(
            &s,
            "StartPipelineExecution",
            json!({"PipelineName": "p1", "ClientRequestToken": "a".repeat(32)}),
        )
        .unwrap(),
    );
    let arn = started["PipelineExecutionArn"]
        .as_str()
        .unwrap()
        .to_string();

    // Stop it so it leaves the Executing state.
    run(
        &s,
        "StopPipelineExecution",
        json!({"PipelineExecutionArn": arn, "ClientRequestToken": "b".repeat(32)}),
    )
    .unwrap();
    let described = resp_json(
        &run(
            &s,
            "DescribePipelineExecution",
            json!({"PipelineExecutionArn": arn}),
        )
        .unwrap(),
    );
    assert_ne!(described["PipelineExecutionStatus"], "Executing");

    // Retry brings it back to Executing.
    run(
        &s,
        "RetryPipelineExecution",
        json!({"PipelineExecutionArn": arn, "ClientRequestToken": "c".repeat(32)}),
    )
    .unwrap();
    let described = resp_json(
        &run(
            &s,
            "DescribePipelineExecution",
            json!({"PipelineExecutionArn": arn}),
        )
        .unwrap(),
    );
    assert_eq!(described["PipelineExecutionStatus"], "Executing");
}

// BatchAddClusterNodes persists nodes under the ClusterNode family so
// ListClusterNodes reflects the added set; BatchDeleteClusterNodes removes them.
#[test]
fn batch_cluster_nodes_add_and_delete_round_trip() {
    let s = svc();
    let added = resp_json(
        &run(
            &s,
            "BatchAddClusterNodes",
            json!({
                "ClusterName": "c1",
                "NodesToAdd": [{"InstanceGroupName": "g1", "IncrementTargetCountBy": 2}],
            }),
        )
        .unwrap(),
    );
    // The action output reflects the two real minted nodes.
    let successful = added["Successful"].as_array().unwrap();
    assert_eq!(successful.len(), 2, "add output: {added}");
    assert!(successful
        .iter()
        .all(|n| n["NodeLogicalId"].is_string() && n["InstanceGroupName"] == "g1"));

    // ListClusterNodes now surfaces both nodes.
    let listed = resp_json(&run(&s, "ListClusterNodes", json!({"ClusterName": "c1"})).unwrap());
    let nodes = listed["ClusterNodeSummaries"].as_array().unwrap();
    assert_eq!(nodes.len(), 2, "list after add: {listed}");
    let node_id = nodes[0]["NodeLogicalId"].as_str().unwrap().to_string();

    // Delete one by its logical id; the set shrinks to one.
    run(
        &s,
        "BatchDeleteClusterNodes",
        json!({"ClusterName": "c1", "NodeLogicalIds": [node_id]}),
    )
    .unwrap();
    let listed = resp_json(&run(&s, "ListClusterNodes", json!({"ClusterName": "c1"})).unwrap());
    assert_eq!(listed["ClusterNodeSummaries"].as_array().unwrap().len(), 1);
}

// BatchRebootClusterNodes reports in Successful exactly the requested ids that
// resolve to a node in the cluster (reading real persisted state).
#[test]
fn batch_reboot_cluster_nodes_reflects_membership() {
    let s = svc();
    run(
        &s,
        "BatchAddClusterNodes",
        json!({"ClusterName": "c1", "NodesToAdd": [{"InstanceGroupName": "g1"}]}),
    )
    .unwrap();
    let listed = resp_json(&run(&s, "ListClusterNodes", json!({"ClusterName": "c1"})).unwrap());
    let node_id = listed["ClusterNodeSummaries"][0]["NodeLogicalId"]
        .as_str()
        .unwrap()
        .to_string();

    let rebooted = resp_json(
        &run(
            &s,
            "BatchRebootClusterNodes",
            json!({"ClusterName": "c1", "NodeLogicalIds": [node_id.clone(), "does-not-exist"]}),
        )
        .unwrap(),
    );
    let ok = rebooted["Successful"].as_array().unwrap();
    assert_eq!(ok.len(), 1, "only the real node reboots: {rebooted}");
    assert_eq!(ok[0], node_id);
}

// BatchReplaceClusterNodes swaps a node's underlying instance (new InstanceId),
// keeping its logical id, visible via ListClusterNodes.
#[test]
fn batch_replace_cluster_nodes_swaps_instance() {
    let s = svc();
    run(
        &s,
        "BatchAddClusterNodes",
        json!({"ClusterName": "c1", "NodesToAdd": [{"InstanceGroupName": "g1"}]}),
    )
    .unwrap();
    let listed = resp_json(&run(&s, "ListClusterNodes", json!({"ClusterName": "c1"})).unwrap());
    let node_id = listed["ClusterNodeSummaries"][0]["NodeLogicalId"]
        .as_str()
        .unwrap()
        .to_string();
    let old_instance = listed["ClusterNodeSummaries"][0]["InstanceId"]
        .as_str()
        .unwrap()
        .to_string();

    let replaced = resp_json(
        &run(
            &s,
            "BatchReplaceClusterNodes",
            json!({"ClusterName": "c1", "NodeLogicalIds": [node_id.clone()]}),
        )
        .unwrap(),
    );
    assert_eq!(
        replaced["Successful"].as_array().unwrap(),
        &vec![json!(node_id)]
    );

    let listed = resp_json(&run(&s, "ListClusterNodes", json!({"ClusterName": "c1"})).unwrap());
    let summary = &listed["ClusterNodeSummaries"][0];
    assert_eq!(summary["NodeLogicalId"], node_id, "logical id is stable");
    assert_ne!(
        summary["InstanceId"].as_str().unwrap(),
        old_instance,
        "the underlying instance was replaced"
    );
}

// AssociateTrialComponent records the trial<->component edge so a scoped
// ListTrialComponents(TrialName=…) returns it; DisassociateTrialComponent removes it.
#[test]
fn trial_component_association_round_trip() {
    let s = svc();
    run(
        &s,
        "CreateTrial",
        json!({"TrialName": "t1", "ExperimentName": "e1"}),
    )
    .unwrap();
    run(
        &s,
        "CreateTrialComponent",
        json!({"TrialComponentName": "tc1"}),
    )
    .unwrap();

    // Before association, the trial has no components.
    let scoped = resp_json(&run(&s, "ListTrialComponents", json!({"TrialName": "t1"})).unwrap());
    assert!(scoped["TrialComponentSummaries"]
        .as_array()
        .unwrap()
        .is_empty());

    run(
        &s,
        "AssociateTrialComponent",
        json!({"TrialComponentName": "tc1", "TrialName": "t1"}),
    )
    .unwrap();

    // The scoped list now returns the associated component.
    let scoped = resp_json(&run(&s, "ListTrialComponents", json!({"TrialName": "t1"})).unwrap());
    let sums = scoped["TrialComponentSummaries"].as_array().unwrap();
    assert_eq!(sums.len(), 1, "scoped list after associate: {scoped}");
    assert_eq!(sums[0]["TrialComponentName"], "tc1");

    // An unrelated trial sees nothing.
    let other =
        resp_json(&run(&s, "ListTrialComponents", json!({"TrialName": "t-other"})).unwrap());
    assert!(other["TrialComponentSummaries"]
        .as_array()
        .unwrap()
        .is_empty());

    // Disassociate empties the scoped list again.
    run(
        &s,
        "DisassociateTrialComponent",
        json!({"TrialComponentName": "tc1", "TrialName": "t1"}),
    )
    .unwrap();
    let scoped = resp_json(&run(&s, "ListTrialComponents", json!({"TrialName": "t1"})).unwrap());
    assert!(scoped["TrialComponentSummaries"]
        .as_array()
        .unwrap()
        .is_empty());
}

// ListTrialComponents without a TrialName filter keeps the generic, unscoped
// behaviour (returns every component regardless of association).
#[test]
fn list_trial_components_unscoped_returns_all() {
    let s = svc();
    run(
        &s,
        "CreateTrialComponent",
        json!({"TrialComponentName": "tc1"}),
    )
    .unwrap();
    run(
        &s,
        "CreateTrialComponent",
        json!({"TrialComponentName": "tc2"}),
    )
    .unwrap();
    let all = resp_json(&run(&s, "ListTrialComponents", Value::Null).unwrap());
    assert_eq!(all["TrialComponentSummaries"].as_array().unwrap().len(), 2);
}

// ListClusterNodes must scope to the request's required ClusterName: nodes added
// to cluster A must not leak into a ListClusterNodes(B) call (regression 0.2).
#[test]
fn list_cluster_nodes_scoped_by_cluster_name() {
    let s = svc();
    run(
        &s,
        "BatchAddClusterNodes",
        json!({"ClusterName": "cluster-a", "NodesToAdd": [{"InstanceGroupName": "g", "IncrementTargetCountBy": 2}]}),
    )
    .unwrap();
    run(
        &s,
        "BatchAddClusterNodes",
        json!({"ClusterName": "cluster-b", "NodesToAdd": [{"InstanceGroupName": "g", "IncrementTargetCountBy": 3}]}),
    )
    .unwrap();

    let a = resp_json(&run(&s, "ListClusterNodes", json!({"ClusterName": "cluster-a"})).unwrap());
    assert_eq!(
        a["ClusterNodeSummaries"].as_array().unwrap().len(),
        2,
        "cluster-a sees only its own nodes: {a}"
    );
    let b = resp_json(&run(&s, "ListClusterNodes", json!({"ClusterName": "cluster-b"})).unwrap());
    assert_eq!(
        b["ClusterNodeSummaries"].as_array().unwrap().len(),
        3,
        "cluster-b sees only its own nodes: {b}"
    );
    // An unknown cluster sees nothing.
    let none =
        resp_json(&run(&s, "ListClusterNodes", json!({"ClusterName": "cluster-x"})).unwrap());
    assert!(none["ClusterNodeSummaries"].as_array().unwrap().is_empty());
}

// ListPipelineExecutionSteps must scope to the request's PipelineExecutionArn:
// callback steps sent for execution X must not leak into a query for Y (0.3).
#[test]
fn list_pipeline_execution_steps_scoped_by_execution_arn() {
    let s = svc();
    // Two distinct callback tokens -> two distinct derived execution arns.
    let sent_x = resp_json(
        &run(
            &s,
            "SendPipelineExecutionStepSuccess",
            json!({"CallbackToken": "tokenaaaa1"}),
        )
        .unwrap(),
    );
    let arn_x = sent_x["PipelineExecutionArn"].as_str().unwrap().to_string();
    run(
        &s,
        "SendPipelineExecutionStepSuccess",
        json!({"CallbackToken": "tokenaaaa2"}),
    )
    .unwrap();

    // Scoped to X returns exactly the one step for X.
    let scoped = resp_json(
        &run(
            &s,
            "ListPipelineExecutionSteps",
            json!({"PipelineExecutionArn": arn_x}),
        )
        .unwrap(),
    );
    let steps = scoped["PipelineExecutionSteps"].as_array().unwrap();
    assert_eq!(steps.len(), 1, "only X's step: {scoped}");
    assert_eq!(steps[0]["StepStatus"], "Succeeded");
    // The internal scoping member is not projected onto the summary.
    assert!(steps[0].get("PipelineExecutionArn").is_none());

    // Unscoped (no filter) keeps the generic behaviour: both steps.
    let all = resp_json(&run(&s, "ListPipelineExecutionSteps", json!({})).unwrap());
    assert_eq!(all["PipelineExecutionSteps"].as_array().unwrap().len(), 2);
}

// ListPipelineExecutions must scope to the request's required PipelineName:
// executions of pipeline p1 must not leak into a query for p2 (0.3 LOW).
#[test]
fn list_pipeline_executions_scoped_by_pipeline_name() {
    let s = svc();
    for (name, count) in [("p1", 2), ("p2", 1)] {
        for i in 0..count {
            run(
                &s,
                "StartPipelineExecution",
                json!({"PipelineName": name, "ClientRequestToken": format!("{name}-{i}-{}", "a".repeat(32))}),
            )
            .unwrap();
        }
    }
    let p1 = resp_json(&run(&s, "ListPipelineExecutions", json!({"PipelineName": "p1"})).unwrap());
    let sums = p1["PipelineExecutionSummaries"].as_array().unwrap();
    assert_eq!(sums.len(), 2, "p1 sees only its own executions: {p1}");
    assert!(sums
        .iter()
        .all(|x| x["PipelineExecutionArn"].as_str().unwrap().contains("/p1/")));

    let p2 = resp_json(&run(&s, "ListPipelineExecutions", json!({"PipelineName": "p2"})).unwrap());
    assert_eq!(
        p2["PipelineExecutionSummaries"].as_array().unwrap().len(),
        1,
        "p2 sees only its own executions: {p2}"
    );
}

// SendPipelineExecutionStepSuccess advances a waiting callback step to Succeeded;
// SendPipelineExecutionStepFailure to Failed (visible via ListPipelineExecutionSteps).
#[test]
fn send_pipeline_execution_step_advances_callback() {
    let s = svc();
    let token = "cbtoken001"; // exactly 10 chars (model @length)
                              // Seed a waiting callback step keyed by the token.
    {
        let mut g = s.state.write();
        let data = g.get_or_create("000000000000");
        data.put_resource(
            "PipelineExecutionStep",
            token,
            json!({"StepName": "Callback", "StepStatus": "Executing"}),
        );
    }

    let sent = resp_json(
        &run(
            &s,
            "SendPipelineExecutionStepSuccess",
            json!({"CallbackToken": token}),
        )
        .unwrap(),
    );
    assert!(sent["PipelineExecutionArn"].is_string());

    // The step advanced from Executing to Succeeded.
    {
        let g = s.state.read();
        let rec = g
            .get("000000000000")
            .unwrap()
            .get_resource("PipelineExecutionStep", token)
            .unwrap();
        assert_eq!(rec["StepStatus"], "Succeeded");
    }
    let listed = resp_json(&run(&s, "ListPipelineExecutionSteps", json!({})).unwrap());
    let steps = listed["PipelineExecutionSteps"].as_array().unwrap();
    assert!(steps.iter().any(|st| st["StepStatus"] == "Succeeded"));

    // A failure send transitions the step to Failed and records the reason.
    run(
        &s,
        "SendPipelineExecutionStepFailure",
        json!({"CallbackToken": token, "FailureReason": "boom"}),
    )
    .unwrap();
    {
        let g = s.state.read();
        let rec = g
            .get("000000000000")
            .unwrap()
            .get_resource("PipelineExecutionStep", token)
            .unwrap();
        assert_eq!(rec["StepStatus"], "Failed");
        assert_eq!(rec["FailureReason"], "boom");
    }
}

// ── Ops mis-mapped to a synthetic child family resolve against their parent ──
//
// Each of these six ops was auto-generated under a `family` slug taken from its
// own shape name, which no `Create*` populates, so the generic engine always
// 404'd even against an existing parent. They now resolve the real parent family
// and project the modelled output shape (bug-hunt 2026-07-25 Tier-1).

// UpdateEndpointWeightsAndCapacities updates the parent endpoint's production
// variant weights and is visible on DescribeEndpoint.
#[test]
fn update_endpoint_weights_resolves_endpoint_and_round_trips() {
    let s = svc();
    // Absent endpoint -> 404 ResourceNotFound (a conformance common-error PASS).
    match expect_err(run(
        &s,
        "UpdateEndpointWeightsAndCapacities",
        json!({
            "EndpointName": "ep1",
            "DesiredWeightsAndCapacities": [{"VariantName": "v1", "DesiredWeight": 2.0}]
        }),
    )) {
        AwsServiceError::AwsError { code, status, .. } => {
            assert_eq!(code, "ResourceNotFound");
            assert_eq!(status.as_u16(), 404);
        }
        other => panic!("expected AwsError, got {other:?}"),
    }

    run(
        &s,
        "CreateEndpoint",
        json!({"EndpointName": "ep1", "EndpointConfigName": "cfg1"}),
    )
    .unwrap();

    let updated = resp_json(
        &run(
            &s,
            "UpdateEndpointWeightsAndCapacities",
            json!({
                "EndpointName": "ep1",
                "DesiredWeightsAndCapacities": [
                    {"VariantName": "v1", "DesiredWeight": 5.0, "DesiredInstanceCount": 3}
                ]
            }),
        )
        .unwrap(),
    );
    // Modelled output shape: { EndpointArn }.
    assert!(updated["EndpointArn"]
        .as_str()
        .unwrap()
        .contains("endpoint/"));

    // The new desired weight/capacity is visible on DescribeEndpoint.
    let described =
        resp_json(&run(&s, "DescribeEndpoint", json!({"EndpointName": "ep1"})).unwrap());
    let variants = described["ProductionVariants"].as_array().unwrap();
    let v1 = variants
        .iter()
        .find(|v| v["VariantName"] == "v1")
        .expect("v1 variant present");
    assert_eq!(v1["DesiredWeight"], 5.0);
    assert_eq!(v1["DesiredInstanceCount"], 3);
}

// UpdateFeatureMetadata writes description/parameters; DescribeFeatureMetadata
// reads them back, both scoped to the parent FeatureGroup.
#[test]
fn feature_metadata_round_trips_against_feature_group() {
    let s = svc();
    // Absent feature group -> 404 on both Describe and Update.
    for op in ["DescribeFeatureMetadata", "UpdateFeatureMetadata"] {
        match expect_err(run(
            &s,
            op,
            json!({"FeatureGroupName": "fg1", "FeatureName": "age"}),
        )) {
            AwsServiceError::AwsError { code, status, .. } => {
                assert_eq!(code, "ResourceNotFound", "{op}");
                assert_eq!(status.as_u16(), 404, "{op}");
            }
            other => panic!("{op}: expected AwsError, got {other:?}"),
        }
    }

    run(
        &s,
        "CreateFeatureGroup",
        json!({
            "FeatureGroupName": "fg1",
            "RecordIdentifierFeatureName": "id",
            "EventTimeFeatureName": "ts",
            "FeatureDefinitions": [
                {"FeatureName": "age", "FeatureType": "Integral"},
                {"FeatureName": "id", "FeatureType": "String"}
            ]
        }),
    )
    .unwrap();

    // Describe before any update: FeatureType comes from the group definitions.
    let before = resp_json(
        &run(
            &s,
            "DescribeFeatureMetadata",
            json!({"FeatureGroupName": "fg1", "FeatureName": "age"}),
        )
        .unwrap(),
    );
    assert_eq!(before["FeatureName"], "age");
    assert_eq!(before["FeatureType"], "Integral");
    assert!(before["FeatureGroupArn"]
        .as_str()
        .unwrap()
        .contains("feature-group/"));

    // Update: set a description and add a parameter (empty modelled output shape).
    let updated = resp_json(
        &run(
            &s,
            "UpdateFeatureMetadata",
            json!({
                "FeatureGroupName": "fg1",
                "FeatureName": "age",
                "Description": "years old",
                "ParameterAdditions": [{"Key": "unit", "Value": "year"}]
            }),
        )
        .unwrap(),
    );
    assert_eq!(updated, json!({}));

    // Describe reflects the written metadata.
    let after = resp_json(
        &run(
            &s,
            "DescribeFeatureMetadata",
            json!({"FeatureGroupName": "fg1", "FeatureName": "age"}),
        )
        .unwrap(),
    );
    assert_eq!(after["Description"], "years old");
    let params = after["Parameters"].as_array().unwrap();
    assert!(params
        .iter()
        .any(|p| p["Key"] == "unit" && p["Value"] == "year"));

    // A removal drops the parameter again.
    run(
        &s,
        "UpdateFeatureMetadata",
        json!({
            "FeatureGroupName": "fg1",
            "FeatureName": "age",
            "ParameterRemovals": ["unit"]
        }),
    )
    .unwrap();
    let after2 = resp_json(
        &run(
            &s,
            "DescribeFeatureMetadata",
            json!({"FeatureGroupName": "fg1", "FeatureName": "age"}),
        )
        .unwrap(),
    );
    assert!(after2["Parameters"].as_array().unwrap().is_empty());
}

// The remaining four ops resolve their parent and return the modelled output.
#[test]
fn cluster_software_inference_runtime_alert_and_pipeline_version_resolve_parent() {
    let s = svc();

    // UpdateClusterSoftware -> Cluster (ClusterArn).
    run(
        &s,
        "CreateCluster",
        json!({"ClusterName": "c1", "InstanceGroups": []}),
    )
    .unwrap();
    let cs = resp_json(&run(&s, "UpdateClusterSoftware", json!({"ClusterName": "c1"})).unwrap());
    assert!(cs["ClusterArn"].as_str().unwrap().contains("cluster/"));

    // UpdateInferenceComponentRuntimeConfig -> InferenceComponent.
    run(
        &s,
        "CreateInferenceComponent",
        json!({
            "InferenceComponentName": "ic1",
            "EndpointName": "ep",
            "VariantName": "v",
            "Specification": {}
        }),
    )
    .unwrap();
    let ic = resp_json(
        &run(
            &s,
            "UpdateInferenceComponentRuntimeConfig",
            json!({"InferenceComponentName": "ic1", "DesiredRuntimeConfig": {"CopyCount": 4}}),
        )
        .unwrap(),
    );
    assert!(ic["InferenceComponentArn"]
        .as_str()
        .unwrap()
        .contains("inference-component/"));
    let ic_desc = resp_json(
        &run(
            &s,
            "DescribeInferenceComponent",
            json!({"InferenceComponentName": "ic1"}),
        )
        .unwrap(),
    );
    assert_eq!(ic_desc["RuntimeConfig"]["DesiredCopyCount"], 4);

    // UpdateMonitoringAlert -> MonitoringSchedule (arn + echoed alert name).
    run(
        &s,
        "CreateMonitoringSchedule",
        json!({"MonitoringScheduleName": "ms1", "MonitoringScheduleConfig": {}}),
    )
    .unwrap();
    let ma = resp_json(
        &run(
            &s,
            "UpdateMonitoringAlert",
            json!({
                "MonitoringScheduleName": "ms1",
                "MonitoringAlertName": "a1",
                "DatapointsToAlert": 2,
                "EvaluationPeriod": 5
            }),
        )
        .unwrap(),
    );
    assert!(ma["MonitoringScheduleArn"]
        .as_str()
        .unwrap()
        .contains("monitoring-schedule/"));
    assert_eq!(ma["MonitoringAlertName"], "a1");

    // UpdatePipelineVersion -> Pipeline, resolved by the minted PipelineArn.
    let pipeline = resp_json(
        &run(
            &s,
            "CreatePipeline",
            json!({
                "PipelineName": "p1",
                "RoleArn": "arn:aws:iam::0:role/r",
                "ClientRequestToken": "0123456789abcdef0123456789abcdef"
            }),
        )
        .unwrap(),
    );
    let pipeline_arn = pipeline["PipelineArn"].as_str().unwrap().to_string();
    let pv = resp_json(
        &run(
            &s,
            "UpdatePipelineVersion",
            json!({
                "PipelineArn": pipeline_arn,
                "PipelineVersionId": 1,
                "PipelineVersionDisplayName": "v1"
            }),
        )
        .unwrap(),
    );
    assert_eq!(pv["PipelineVersionId"], 1);
    assert!(pv["PipelineArn"].as_str().unwrap().contains("pipeline/"));
}
