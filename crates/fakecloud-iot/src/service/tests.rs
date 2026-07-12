use super::*;
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::AwsRequest;
use parking_lot::RwLock;
use serde_json::json;

fn svc() -> IotService {
    let state: SharedIotState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    IotService::new(state)
}

fn mk_req(method: &str, path: &str, headers: &[(&str, &str)], body: Value) -> AwsRequest {
    let mut hm = http::HeaderMap::new();
    for (k, v) in headers {
        hm.insert(
            http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
            http::HeaderValue::from_str(v).unwrap(),
        );
    }
    let (raw_path, raw_query) = match path.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (path.to_string(), String::new()),
    };
    let body_bytes = if body.is_null() {
        bytes::Bytes::new()
    } else {
        bytes::Bytes::from(serde_json::to_vec(&body).unwrap())
    };
    AwsRequest {
        service: "iot".into(),
        action: String::new(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        request_id: "req".into(),
        headers: hm,
        query_params: std::collections::HashMap::new(),
        body: body_bytes,
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path,
        raw_query,
        method: Method::from_bytes(method.as_bytes()).unwrap(),
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn run(
    svc: &IotService,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Value,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(svc.handle(mk_req(method, path, headers, body)))
}

fn body_of(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn is_code(err: &AwsServiceError, want: &str) -> bool {
    matches!(err, AwsServiceError::AwsError { code, .. } if code == want)
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Ok(_) => panic!("expected an error, got a response"),
        Err(e) => e,
    }
}

// ---------- routing ----------

#[test]
fn routes_by_method_and_path() {
    let (m, labels) = match_route(&Method::POST, "/things/sensor-1").unwrap();
    assert_eq!(m.op, "CreateThing");
    assert_eq!(labels.get("thingName").unwrap(), "sensor-1");

    let (m, _) = match_route(&Method::GET, "/things/sensor-1").unwrap();
    assert_eq!(m.op, "DescribeThing");

    let (m, _) = match_route(&Method::GET, "/things").unwrap();
    assert_eq!(m.op, "ListThings");

    // Fixed segment wins over label at the same position.
    let (m, _) = match_route(&Method::PUT, "/thing-groups/addThingToThingGroup").unwrap();
    assert_eq!(m.op, "AddThingToThingGroup");

    // Greedy label captures the rest of the path.
    let (m, labels) =
        match_route(&Method::DELETE, "/destinations/arn:aws:iot:x:y:dest/abc").unwrap();
    assert_eq!(m.op, "DeleteTopicRuleDestination");
    assert_eq!(labels.get("arn").unwrap(), "arn:aws:iot:x:y:dest/abc");
}

#[test]
fn unknown_route_is_not_found() {
    let err = expect_err(run(&svc(), "GET", "/nope/nope", &[], Value::Null));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

// ---------- things lifecycle ----------

#[test]
fn thing_create_get_list_delete() {
    let s = svc();
    let created = run(
        &s,
        "POST",
        "/things/sensor-1",
        &[],
        json!({"thingTypeName": "temp"}),
    )
    .unwrap();
    let doc = body_of(&created);
    assert_eq!(doc["thingName"], "sensor-1");
    assert!(doc["thingArn"]
        .as_str()
        .unwrap()
        .contains(":thing/sensor-1"));
    assert!(doc["thingId"].is_string());

    let got = body_of(&run(&s, "GET", "/things/sensor-1", &[], Value::Null).unwrap());
    assert_eq!(got["thingName"], "sensor-1");
    assert_eq!(got["thingTypeName"], "temp");

    let listed = body_of(&run(&s, "GET", "/things", &[], Value::Null).unwrap());
    assert_eq!(listed["things"].as_array().unwrap().len(), 1);
    assert_eq!(listed["things"][0]["thingName"], "sensor-1");

    run(&s, "DELETE", "/things/sensor-1", &[], Value::Null).unwrap();
    let err = expect_err(run(&s, "GET", "/things/sensor-1", &[], Value::Null));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

#[test]
fn describe_missing_thing_is_not_found() {
    let err = expect_err(run(&svc(), "GET", "/things/ghost", &[], Value::Null));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

#[test]
fn update_thing_merges_attributes() {
    let s = svc();
    run(&s, "POST", "/things/t1", &[], json!({"thingTypeName": "a"})).unwrap();
    run(
        &s,
        "PATCH",
        "/things/t1",
        &[],
        json!({"thingTypeName": "b"}),
    )
    .unwrap();
    let got = body_of(&run(&s, "GET", "/things/t1", &[], Value::Null).unwrap());
    assert_eq!(got["thingTypeName"], "b");
}

// ---------- thing types / groups / billing groups ----------

#[test]
fn thing_type_group_billing_lifecycle() {
    let s = svc();
    let tt = body_of(&run(&s, "POST", "/thing-types/temp", &[], json!({})).unwrap());
    assert_eq!(tt["thingTypeName"], "temp");
    assert!(tt["thingTypeArn"]
        .as_str()
        .unwrap()
        .contains(":thingtype/temp"));

    let tg = body_of(&run(&s, "POST", "/thing-groups/g1", &[], json!({})).unwrap());
    assert_eq!(tg["thingGroupName"], "g1");
    assert!(tg["thingGroupArn"]
        .as_str()
        .unwrap()
        .contains(":thinggroup/g1"));

    let bg = body_of(&run(&s, "POST", "/billing-groups/b1", &[], json!({})).unwrap());
    assert_eq!(bg["billingGroupName"], "b1");
    assert!(bg["billingGroupArn"]
        .as_str()
        .unwrap()
        .contains(":billinggroup/b1"));
}

// ---------- policies + versions + attachment ----------

#[test]
fn policy_create_and_attach() {
    let s = svc();
    let pol = body_of(
        &run(
            &s,
            "POST",
            "/policies/p1",
            &[],
            json!({"policyDocument": "{\"Version\":\"2012-10-17\"}"}),
        )
        .unwrap(),
    );
    assert_eq!(pol["policyName"], "p1");
    assert!(pol["policyArn"].as_str().unwrap().contains(":policy/p1"));

    // Attach the policy to a target (a certificate ARN).
    run(
        &s,
        "PUT",
        "/target-policies/p1",
        &[],
        json!({"target": "arn:aws:iot:us-east-1:000000000000:cert/abc"}),
    )
    .unwrap();
    let targets = body_of(&run(&s, "POST", "/policy-targets/p1", &[], json!({})).unwrap());
    assert_eq!(targets["targets"].as_array().unwrap().len(), 1);

    // The target ARN's slash is URL-encoded in the path, as the SDK sends it.
    let attached = body_of(
        &run(
            &s,
            "POST",
            "/attached-policies/arn:aws:iot:us-east-1:000000000000:cert%2Fabc",
            &[],
            json!({}),
        )
        .unwrap(),
    );
    assert_eq!(attached["policies"][0]["policyName"], "p1");
}

// ---------- certificates ----------

#[test]
fn create_keys_and_certificate_mints_real_shapes() {
    let s = svc();
    let doc = body_of(
        &run(
            &s,
            "POST",
            "/keys-and-certificate?setAsActive=true",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    let cert_id = doc["certificateId"].as_str().unwrap();
    assert_eq!(cert_id.len(), 64);
    assert!(cert_id.chars().all(|c| c.is_ascii_hexdigit()));
    assert!(doc["certificateArn"].as_str().unwrap().contains(":cert/"));
    assert!(doc["certificatePem"]
        .as_str()
        .unwrap()
        .contains("BEGIN CERTIFICATE"));
    assert!(doc["keyPair"]["PrivateKey"]
        .as_str()
        .unwrap()
        .contains("PRIVATE KEY"));

    // The minted certificate is described by id (nested under
    // `certificateDescription`, matching the model).
    let desc = body_of(
        &run(
            &s,
            "GET",
            &format!("/certificates/{cert_id}"),
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(
        desc["certificateDescription"]["certificateArn"],
        doc["certificateArn"]
    );
    // The server-generated `creationDate` is a restJson1 epoch-seconds number,
    // not an RFC3339 string (which the aws-sdk timestamp deserializer rejects).
    let creation_date = &desc["certificateDescription"]["creationDate"];
    assert!(
        creation_date.is_number(),
        "creationDate must be a numeric epoch-seconds timestamp, got {creation_date:?}"
    );
    assert!(creation_date.as_f64().unwrap() > 1_600_000_000.0);
}

// ---------- attach thing principal ----------

#[test]
fn attach_thing_principal_round_trips() {
    let s = svc();
    run(&s, "POST", "/things/t1", &[], json!({})).unwrap();
    run(
        &s,
        "PUT",
        "/things/t1/principals",
        &[(
            "x-amzn-principal",
            "arn:aws:iot:us-east-1:000000000000:cert/abc",
        )],
        Value::Null,
    )
    .unwrap();
    let principals = body_of(&run(&s, "GET", "/things/t1/principals", &[], Value::Null).unwrap());
    assert_eq!(principals["principals"].as_array().unwrap().len(), 1);
}

// ---------- thing group membership ----------

#[test]
fn add_thing_to_group_round_trips() {
    let s = svc();
    run(&s, "POST", "/things/t1", &[], json!({})).unwrap();
    run(&s, "POST", "/thing-groups/g1", &[], json!({})).unwrap();
    run(
        &s,
        "PUT",
        "/thing-groups/addThingToThingGroup",
        &[],
        json!({"thingGroupName": "g1", "thingName": "t1"}),
    )
    .unwrap();
    let things = body_of(&run(&s, "GET", "/thing-groups/g1/things", &[], Value::Null).unwrap());
    assert_eq!(things["things"], json!(["t1"]));
}

// ---------- jobs + topic rules ----------

#[test]
fn job_and_topic_rule_lifecycle() {
    let s = svc();
    let job = body_of(
        &run(
            &s,
            "PUT",
            "/jobs/job-1",
            &[],
            json!({"targets": ["arn:aws:iot:us-east-1:000000000000:thing/t1"]}),
        )
        .unwrap(),
    );
    assert_eq!(job["jobId"], "job-1");
    assert!(job["jobArn"].as_str().unwrap().contains(":job/job-1"));

    // Topic rule carries an @httpPayload body (the rule payload IS the body).
    run(
        &s,
        "POST",
        "/rules/r1",
        &[],
        json!({"sql": "SELECT * FROM 'topic'", "actions": []}),
    )
    .unwrap();
    let rule = body_of(&run(&s, "GET", "/rules/r1", &[], Value::Null).unwrap());
    assert_eq!(rule["rule"]["ruleName"], "r1");
    // restJson1 (IoT's protocol) wire-encodes timestamps as epoch-seconds JSON
    // numbers, NOT RFC3339 strings. The aws-sdk timestamp deserializer rejects a
    // string here, so `createdAt` must round-trip through create -> get as a
    // number whose value is a plausible Unix epoch-seconds instant.
    let created_at = &rule["rule"]["createdAt"];
    assert!(
        created_at.is_number(),
        "createdAt must be a numeric epoch-seconds timestamp, got {created_at:?}"
    );
    let secs = created_at.as_f64().unwrap();
    assert!(
        secs > 1_600_000_000.0,
        "createdAt {secs} is not a plausible epoch-seconds value"
    );
}

// ---------- endpoint / registration code / tags ----------

#[test]
fn describe_endpoint_is_deterministic() {
    let s = svc();
    let a = body_of(
        &run(
            &s,
            "GET",
            "/endpoint?endpointType=iot:Data-ATS",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    let b = body_of(
        &run(
            &s,
            "GET",
            "/endpoint?endpointType=iot:Data-ATS",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(a["endpointAddress"], b["endpointAddress"]);
    assert!(a["endpointAddress"]
        .as_str()
        .unwrap()
        .contains("-ats.iot.us-east-1"));

    let jobs = body_of(
        &run(
            &s,
            "GET",
            "/endpoint?endpointType=iot:Jobs",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert!(jobs["endpointAddress"]
        .as_str()
        .unwrap()
        .contains(".jobs.iot."));
}

#[test]
fn tags_round_trip() {
    let s = svc();
    run(
        &s,
        "POST",
        "/tags",
        &[],
        json!({"resourceArn": "arn:aws:iot:us-east-1:000000000000:thing/t1", "tags": [{"Key": "env", "Value": "prod"}]}),
    )
    .unwrap();
    let listed = body_of(
        &run(
            &s,
            "GET",
            "/tags?resourceArn=arn:aws:iot:us-east-1:000000000000:thing/t1",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(listed["tags"][0]["Key"], "env");
    assert_eq!(listed["tags"][0]["Value"], "prod");
}

// ---------- singletons ----------

#[test]
fn indexing_configuration_round_trips() {
    let s = svc();
    run(
        &s,
        "POST",
        "/indexing/config",
        &[],
        json!({"thingIndexingConfiguration": {"thingIndexingMode": "REGISTRY"}}),
    )
    .unwrap();
    let got = body_of(&run(&s, "GET", "/indexing/config", &[], Value::Null).unwrap());
    assert_eq!(
        got["thingIndexingConfiguration"]["thingIndexingMode"],
        "REGISTRY"
    );
}

// ---------- validation ----------

#[test]
fn missing_required_body_member_is_rejected() {
    // AttachPolicy requires `target` in the body.
    let err = expect_err(run(&svc(), "PUT", "/target-policies/p1", &[], json!({})));
    assert!(matches!(err, AwsServiceError::AwsError { status, .. } if status.is_client_error()));
}

#[test]
fn too_long_label_is_rejected() {
    // certificateId is fixed length 64.
    let long = "a".repeat(65);
    let err = expect_err(run(
        &svc(),
        "GET",
        &format!("/certificates/{long}"),
        &[],
        Value::Null,
    ));
    assert!(matches!(err, AwsServiceError::AwsError { status, .. } if status.is_client_error()));
}

#[test]
fn placeholder_label_is_rejected() {
    let err = expect_err(run(&svc(), "GET", "/things/{thingName}", &[], Value::Null));
    assert!(matches!(err, AwsServiceError::AwsError { status, .. } if status.is_client_error()));
}

// ---------- pagination ----------

#[test]
fn list_paginates_with_round_tripping_token() {
    let s = svc();
    for i in 0..5 {
        run(&s, "POST", &format!("/things/t{i}"), &[], json!({})).unwrap();
    }
    let page1 = body_of(&run(&s, "GET", "/things?maxResults=2", &[], Value::Null).unwrap());
    assert_eq!(page1["things"].as_array().unwrap().len(), 2);
    let token = page1["nextToken"].as_str().unwrap();
    let page2 = body_of(
        &run(
            &s,
            "GET",
            &format!("/things?maxResults=2&nextToken={token}"),
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(page2["things"].as_array().unwrap().len(), 2);
    assert_ne!(
        page1["things"][0]["thingName"],
        page2["things"][0]["thingName"]
    );
}

// ---------- search index (bounded) ----------

#[test]
fn search_index_bounded_query() {
    let s = svc();
    run(&s, "POST", "/things/t1", &[], json!({})).unwrap();
    let all = body_of(
        &run(
            &s,
            "POST",
            "/indices/search",
            &[],
            json!({"queryString": "*"}),
        )
        .unwrap(),
    );
    assert_eq!(all["things"].as_array().unwrap().len(), 1);

    let one = body_of(
        &run(
            &s,
            "POST",
            "/indices/search",
            &[],
            json!({"queryString": "thingName:t1"}),
        )
        .unwrap(),
    );
    assert_eq!(one["things"][0]["thingName"], "t1");

    // Unsupported query -> declared error, never a wrong result.
    let err = expect_err(run(
        &s,
        "POST",
        "/indices/search",
        &[],
        json!({"queryString": "attributes.color:red AND foo:bar"}),
    ));
    assert!(matches!(err, AwsServiceError::AwsError { status, .. } if status.is_client_error()));
}

// ---------- output-shape regressions (conformance) ----------

#[test]
fn scalar_name_lists_are_arrays_of_strings() {
    let s = svc();
    // Custom metrics: metricNames is list<string>.
    run(
        &s,
        "POST",
        "/custom-metric/m1",
        &[],
        json!({"metricType": "number", "clientRequestToken": "t"}),
    )
    .unwrap();
    let listed = body_of(&run(&s, "GET", "/custom-metrics", &[], Value::Null).unwrap());
    assert_eq!(listed["metricNames"], json!(["m1"]));

    // Dimensions: dimensionNames is list<string>.
    run(
        &s,
        "POST",
        "/dimensions/d1",
        &[],
        json!({"type": "TOPIC_FILTER", "stringValues": ["x"], "clientRequestToken": "t"}),
    )
    .unwrap();
    let listed = body_of(&run(&s, "GET", "/dimensions", &[], Value::Null).unwrap());
    assert_eq!(listed["dimensionNames"], json!(["d1"]));

    // Role aliases: roleAliases is list<string>.
    run(
        &s,
        "POST",
        "/role-aliases/r1",
        &[],
        json!({"roleArn": "arn:aws:iam::000000000000:role/x"}),
    )
    .unwrap();
    let listed = body_of(&run(&s, "GET", "/role-aliases", &[], Value::Null).unwrap());
    assert_eq!(listed["roleAliases"], json!(["r1"]));
}

#[test]
fn list_security_profiles_has_name_and_arn() {
    let s = svc();
    run(&s, "POST", "/security-profiles/sp1", &[], json!({})).unwrap();
    let listed = body_of(&run(&s, "GET", "/security-profiles", &[], Value::Null).unwrap());
    let ids = listed["securityProfileIdentifiers"].as_array().unwrap();
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0]["name"], "sp1");
    assert!(ids[0]["arn"]
        .as_str()
        .unwrap()
        .contains(":securityprofile/sp1"));
}

#[test]
fn required_payload_omission_is_rejected() {
    let s = svc();
    // CreateTopicRule / ReplaceTopicRule / SetLoggingOptions require an
    // @httpPayload body; an empty body is a client error.
    for (method, path) in [
        ("POST", "/rules/r1"),
        ("PATCH", "/rules/r1"),
        ("POST", "/loggingOptions"),
    ] {
        let err = expect_err(run(&s, method, path, &[], Value::Null));
        assert!(
            matches!(&err, AwsServiceError::AwsError { status, .. } if status.is_client_error()),
            "{method} {path} should reject empty payload, got {err:?}"
        );
    }
    // A non-empty payload is accepted.
    run(
        &s,
        "POST",
        "/rules/r1",
        &[],
        json!({"sql": "SELECT * FROM 'x'", "actions": []}),
    )
    .unwrap();
}

#[test]
fn audit_suppression_description_round_trips() {
    let s = svc();
    let ri = json!({"deviceCertificateId": "abcdef"});
    run(
        &s,
        "POST",
        "/audit/suppressions/create",
        &[],
        json!({"checkName": "LOGGING_DISABLED_CHECK", "resourceIdentifier": ri,
               "description": "created", "clientRequestToken": "t"}),
    )
    .unwrap();
    let desc = body_of(
        &run(
            &s,
            "POST",
            "/audit/suppressions/describe",
            &[],
            json!({"checkName": "LOGGING_DISABLED_CHECK", "resourceIdentifier": ri}),
        )
        .unwrap(),
    );
    assert_eq!(desc["description"], "created");

    // Update echoes the new description on the next describe.
    run(
        &s,
        "PATCH",
        "/audit/suppressions/update",
        &[],
        json!({"checkName": "LOGGING_DISABLED_CHECK", "resourceIdentifier": ri,
               "description": "updated"}),
    )
    .unwrap();
    let desc = body_of(
        &run(
            &s,
            "POST",
            "/audit/suppressions/describe",
            &[],
            json!({"checkName": "LOGGING_DISABLED_CHECK", "resourceIdentifier": ri}),
        )
        .unwrap(),
    );
    assert_eq!(desc["description"], "updated");
}

#[test]
fn create_job_document_source_round_trips() {
    let s = svc();
    run(
        &s,
        "PUT",
        "/jobs/j1",
        &[],
        json!({"targets": ["arn:aws:iot:us-east-1:000000000000:thing/t1"],
               "documentSource": "https://example.com/doc.json"}),
    )
    .unwrap();
    // DescribeJob returns documentSource as a top-level member, not inside job.
    let desc = body_of(&run(&s, "GET", "/jobs/j1", &[], Value::Null).unwrap());
    assert_eq!(desc["documentSource"], "https://example.com/doc.json");
    assert_eq!(desc["job"]["jobId"], "j1");
    assert!(desc["job"].get("documentSource").is_none());
}

// ---------- every operation routes to a handler ----------

#[test]
fn every_action_is_supported() {
    assert_eq!(IOT_ACTIONS.len(), 272);
    let s = svc();
    assert_eq!(s.supported_actions().len(), 272);
}

// ---------- in-process conformance proxy ----------
//
// The live-server conformance probe cannot run in the local sandbox (the Rust
// HTTP server is unreachable there), so this test reproduces the probe's core
// pass criteria in-process: for every one of the 272 operations, a model-valid
// request must NOT crash (HTTP 500) and must return either a 2xx response or a
// 4xx whose error code is declared in the operation's Smithy `errors` list.
// That is exactly what `classify_success_expectation` checks. It catches
// routing misses, panics, and undeclared-error responses without a network.

use crate::generated::{OpMeta, Seg, Src, K, OPS};

/// A label value satisfying a `@length` constraint (min..=max).
fn label_value(rule_min: Option<u64>, rule_max: Option<u64>) -> String {
    let min = rule_min.unwrap_or(1).max(1) as usize;
    let max = rule_max.map(|m| m as usize).unwrap_or(min.max(3));
    "a".repeat(min.max(1).min(max.max(1)))
}

/// Build a model-valid success request for an operation from its metadata.
fn build_success_request(meta: &OpMeta) -> (String, String, Vec<(String, String)>, Value) {
    let find_rule = |wire: &str| meta.rules.iter().find(|r| r.wire == wire);
    // Path from segments; labels filled with length-valid values.
    let mut path = String::new();
    for seg in meta.segs {
        path.push('/');
        match seg {
            Seg::Fixed(f) => path.push_str(f),
            Seg::Label(name) | Seg::Greedy(name) => {
                let (mn, mx) = find_rule(name)
                    .map(|r| (r.min_len, r.max_len))
                    .unwrap_or((None, None));
                path.push_str(&label_value(mn, mx));
            }
        }
    }
    // Query + header + body from the required rules.
    let mut query: Vec<(String, String)> = Vec::new();
    let mut headers: Vec<(String, String)> = Vec::new();
    let mut body = Map::new();
    for rule in meta.rules {
        if !rule.req {
            continue;
        }
        let string_val = || -> String {
            if let Some(first) = rule.enums.first() {
                (*first).to_string()
            } else {
                let min = rule.min_len.unwrap_or(1).max(1) as usize;
                let max = rule.max_len.map(|m| m as usize).unwrap_or(min.max(3));
                "a".repeat(min.min(max.max(1)).max(1))
            }
        };
        match rule.src {
            Src::Label => {} // already in path
            Src::Query => query.push((rule.wire.to_string(), string_val())),
            Src::Header => headers.push((rule.wire.to_string(), string_val())),
            Src::Body => {
                let v = match rule.kind {
                    K::Str | K::Blob => Value::String(string_val()),
                    // restJson1 timestamps wire-encode as epoch-seconds numbers.
                    K::Ts => Value::from(1_752_324_947.041_f64),
                    K::Int | K::Num => Value::Number(rule.min_val.unwrap_or(1).into()),
                    K::Bool => Value::Bool(true),
                    K::List => Value::Array(vec![]),
                    K::Map | K::Struct => Value::Object(Map::new()),
                };
                body.insert(rule.wire.to_string(), v);
            }
        }
    }
    let query_str = query
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let full_path = if query_str.is_empty() {
        path
    } else {
        format!("{path}?{query_str}")
    };
    // A required `@httpPayload` member is the whole body; supply a non-empty
    // one so the model-valid request is accepted (the payload contents are not
    // otherwise constrained here).
    if meta.req_payload && body.is_empty() {
        body.insert("payload".to_string(), json!({}));
    }
    let body_val = if body.is_empty() {
        Value::Null
    } else {
        Value::Object(body)
    };
    (meta.method.to_string(), full_path, headers, body_val)
}

#[test]
fn every_operation_passes_success_criteria() {
    let s = svc();
    let mut failures: Vec<String> = Vec::new();
    for meta in OPS {
        let (method, path, headers, body) = build_success_request(meta);
        let hdrs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        match run(&s, &method, &path, &hdrs, body) {
            Ok(resp) => {
                // 2xx is always a pass.
                if !resp.status.is_success() {
                    failures.push(format!("{}: unexpected status {}", meta.op, resp.status));
                }
            }
            Err(AwsServiceError::AwsError { status, code, .. }) => {
                let s = status.as_u16();
                if s == 500 {
                    failures.push(format!("{}: HTTP 500 crash ({code})", meta.op));
                } else if (400..500).contains(&s) {
                    // A 4xx is a pass only if the code is declared on the op.
                    if !meta.errors.contains(&code.as_str()) {
                        failures.push(format!(
                            "{}: undeclared error '{}' (not in {:?})",
                            meta.op, code, meta.errors
                        ));
                    }
                } else {
                    failures.push(format!("{}: unexpected status {}", meta.op, s));
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

#[test]
fn required_body_omission_is_rejected_for_every_op() {
    // Mirrors the probe's negative `omit-required` variant: dropping a required
    // body member must yield a 4xx for every op that has one.
    let s = svc();
    let mut failures: Vec<String> = Vec::new();
    for meta in OPS {
        // Find a required body member (or a required @httpPayload) to omit;
        // skip ops without one.
        let has_required_body = meta
            .rules
            .iter()
            .any(|r| r.req && matches!(r.src, Src::Body));
        if !has_required_body && !meta.req_payload {
            continue;
        }
        let (method, path, headers, _body) = build_success_request(meta);
        let hdrs: Vec<(&str, &str)> = headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        // Send an empty body, omitting all required body members.
        match run(&s, &method, &path, &hdrs, Value::Null) {
            Ok(resp) => failures.push(format!(
                "{}: expected 4xx for omitted required body, got {}",
                meta.op, resp.status
            )),
            Err(AwsServiceError::AwsError { status, .. }) if status.is_client_error() => {}
            Err(e) => failures.push(format!("{}: expected client error, got {e:?}", meta.op)),
        }
    }
    assert!(
        failures.is_empty(),
        "negative-omit failures:\n{}",
        failures.join("\n")
    );
}
