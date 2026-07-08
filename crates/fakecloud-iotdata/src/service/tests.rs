use super::*;
use fakecloud_core::multi_account::MultiAccountState;
use parking_lot::RwLock;

fn service() -> IotDataService {
    let state: SharedIotDataState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    IotDataService::new(state)
}

fn ctx() -> Ctx {
    Ctx {
        account: "000000000000".to_string(),
    }
}

fn q(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn body_of(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Ok(_) => panic!("expected an error, got a response"),
        Err(e) => e,
    }
}

fn is_code(err: &AwsServiceError, want: &str) -> bool {
    matches!(err, AwsServiceError::AwsError { code, .. } if code == want)
}

#[test]
fn update_then_get_merges_and_computes_delta() {
    let svc = service();
    let ctx = ctx();

    // First update: reported color=red, desired color=green.
    let upd = svc
        .update_thing_shadow(
            &ctx,
            "sensor-1",
            &[],
            br#"{"state":{"reported":{"color":"red"},"desired":{"color":"green"}}}"#,
        )
        .unwrap();
    assert_eq!(upd.status, StatusCode::OK);
    let doc = body_of(&upd);
    assert_eq!(doc["version"], 1);
    assert!(doc["timestamp"].is_number());
    assert_eq!(doc["state"]["reported"]["color"], "red");
    assert_eq!(doc["state"]["desired"]["color"], "green");
    // delta = desired minus reported = {"color":"green"}.
    assert_eq!(doc["state"]["delta"]["color"], "green");
    // metadata stamped at leaves.
    assert!(doc["metadata"]["reported"]["color"]["timestamp"].is_number());

    // GetThingShadow returns the merged doc.
    let got = body_of(&svc.get_thing_shadow(&ctx, "sensor-1", &[]).unwrap());
    assert_eq!(got["version"], 1);
    assert_eq!(got["state"]["delta"]["color"], "green");

    // Second update: reported color=green -> delta disappears, version bumps.
    let upd2 = body_of(
        &svc.update_thing_shadow(
            &ctx,
            "sensor-1",
            &[],
            br#"{"state":{"reported":{"color":"green"}}}"#,
        )
        .unwrap(),
    );
    assert_eq!(upd2["version"], 2);
    assert!(upd2["state"].get("delta").is_none());
}

#[test]
fn merge_null_deletes_reported_key() {
    let svc = service();
    let ctx = ctx();
    svc.update_thing_shadow(&ctx, "t", &[], br#"{"state":{"reported":{"a":1,"b":2}}}"#)
        .unwrap();
    let got = body_of(
        &svc.update_thing_shadow(&ctx, "t", &[], br#"{"state":{"reported":{"b":null}}}"#)
            .unwrap(),
    );
    assert_eq!(got["state"]["reported"]["a"], 1);
    assert!(got["state"]["reported"].get("b").is_none());
}

#[test]
fn get_missing_shadow_is_not_found() {
    let svc = service();
    let err = expect_err(svc.get_thing_shadow(&ctx(), "nope", &[]));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

#[test]
fn version_conflict_returns_conflict() {
    let svc = service();
    let ctx = ctx();
    svc.update_thing_shadow(&ctx, "t", &[], br#"{"state":{"reported":{"a":1}}}"#)
        .unwrap();
    // Stored version is now 1; supplying version 5 conflicts.
    let err = expect_err(svc.update_thing_shadow(
        &ctx,
        "t",
        &[],
        br#"{"version":5,"state":{"reported":{"a":2}}}"#,
    ));
    assert!(is_code(&err, "ConflictException"));
    // Supplying the correct version succeeds.
    let ok = svc
        .update_thing_shadow(
            &ctx,
            "t",
            &[],
            br#"{"version":1,"state":{"reported":{"a":2}}}"#,
        )
        .unwrap();
    assert_eq!(body_of(&ok)["version"], 2);
}

#[test]
fn named_shadows_are_isolated_and_listed() {
    let svc = service();
    let ctx = ctx();
    svc.update_thing_shadow(&ctx, "t", &[], br#"{"state":{"reported":{"c":0}}}"#)
        .unwrap();
    svc.update_thing_shadow(
        &ctx,
        "t",
        &q(&[("name", "alpha")]),
        br#"{"state":{"reported":{"c":1}}}"#,
    )
    .unwrap();
    svc.update_thing_shadow(
        &ctx,
        "t",
        &q(&[("name", "beta")]),
        br#"{"state":{"reported":{"c":2}}}"#,
    )
    .unwrap();

    // Named shadow is a distinct document.
    let alpha = body_of(
        &svc.get_thing_shadow(&ctx, "t", &q(&[("name", "alpha")]))
            .unwrap(),
    );
    assert_eq!(alpha["state"]["reported"]["c"], 1);

    // ListNamedShadowsForThing returns only named shadows, not the classic one.
    let listed = body_of(&svc.list_named_shadows(&ctx, "t", &[]).unwrap());
    let names: Vec<&str> = listed["results"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["alpha", "beta"]);
    assert!(listed["timestamp"].is_number());
}

#[test]
fn named_shadows_pagination_round_trips() {
    let svc = service();
    let ctx = ctx();
    for n in ["a", "b", "c"] {
        svc.update_thing_shadow(
            &ctx,
            "t",
            &q(&[("name", n)]),
            br#"{"state":{"reported":{}}}"#,
        )
        .unwrap();
    }
    let page1 = body_of(
        &svc.list_named_shadows(&ctx, "t", &q(&[("pageSize", "2")]))
            .unwrap(),
    );
    assert_eq!(page1["results"].as_array().unwrap().len(), 2);
    let token = page1["nextToken"].as_str().unwrap().to_string();
    let page2 = body_of(
        &svc.list_named_shadows(&ctx, "t", &q(&[("pageSize", "2"), ("nextToken", &token)]))
            .unwrap(),
    );
    assert_eq!(page2["results"].as_array().unwrap().len(), 1);
    assert!(page2.get("nextToken").is_none());
}

#[test]
fn delete_shadow_returns_payload_and_removes() {
    let svc = service();
    let ctx = ctx();
    svc.update_thing_shadow(&ctx, "t", &[], br#"{"state":{"reported":{"a":1}}}"#)
        .unwrap();
    let del = body_of(&svc.delete_thing_shadow(&ctx, "t", &[]).unwrap());
    assert_eq!(del["version"], 1);
    assert!(del["timestamp"].is_number());
    // Second delete is not found.
    let err = expect_err(svc.delete_thing_shadow(&ctx, "t", &[]));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

#[test]
fn update_rejects_malformed_document() {
    let svc = service();
    let err = expect_err(svc.update_thing_shadow(&ctx(), "t", &[], b"not json"));
    assert!(is_code(&err, "InvalidRequestException"));
}

#[test]
fn publish_retained_then_get_and_list() {
    let svc = service();
    let ctx = ctx();
    let pubresp = svc
        .publish(
            &ctx,
            "sensors/temp",
            &q(&[("qos", "1"), ("retain", "true")]),
            None,
            b"hello",
        )
        .unwrap();
    assert_eq!(pubresp.status, StatusCode::OK);

    let got = body_of(&svc.get_retained_message(&ctx, "sensors/temp").unwrap());
    assert_eq!(got["topic"], "sensors/temp");
    assert_eq!(got["qos"], 1);
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(got["payload"].as_str().unwrap())
        .unwrap();
    assert_eq!(decoded, b"hello");
    assert!(got["lastModifiedTime"].is_number());

    let list = body_of(&svc.list_retained_messages(&ctx, &[]).unwrap());
    let topics = list["retainedTopics"].as_array().unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0]["topic"], "sensors/temp");
    assert_eq!(topics[0]["payloadSize"], 5);
    // Summary carries no payload.
    assert!(topics[0].get("payload").is_none());
}

#[test]
fn non_retained_publish_is_noop() {
    let svc = service();
    let ctx = ctx();
    svc.publish(&ctx, "t/x", &q(&[("qos", "0")]), None, b"data")
        .unwrap();
    let err = expect_err(svc.get_retained_message(&ctx, "t/x"));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

#[test]
fn empty_retained_payload_clears_topic() {
    let svc = service();
    let ctx = ctx();
    svc.publish(&ctx, "t/x", &q(&[("retain", "true")]), None, b"v")
        .unwrap();
    assert!(svc.get_retained_message(&ctx, "t/x").is_ok());
    // Retained publish with empty body clears the topic.
    svc.publish(&ctx, "t/x", &q(&[("retain", "true")]), None, b"")
        .unwrap();
    assert!(svc.get_retained_message(&ctx, "t/x").is_err());
}

#[test]
fn publish_rejects_out_of_range_qos() {
    let svc = service();
    let err = expect_err(svc.publish(&ctx(), "t", &q(&[("qos", "5")]), None, b"x"));
    assert!(is_code(&err, "InvalidRequestException"));
}

#[test]
fn rejects_over_long_thing_name() {
    let svc = service();
    let long = "a".repeat(129);
    let err = expect_err(svc.list_named_shadows(&ctx(), &long, &[]));
    assert!(is_code(&err, "InvalidRequestException"));
}

#[test]
fn publish_rejects_invalid_payload_format_indicator() {
    let svc = service();
    let err = expect_err(svc.publish(&ctx(), "t", &[], Some("BOGUS"), b"x"));
    assert!(is_code(&err, "InvalidRequestException"));
    // A valid enum value is accepted.
    assert!(svc
        .publish(&ctx(), "t", &[], Some("UTF8_DATA"), b"x")
        .is_ok());
}

#[test]
fn connection_ops_report_no_connection() {
    let svc = service();
    for action in [
        "GetConnection",
        "DeleteConnection",
        "ListSubscriptions",
        "SendDirectMessage",
    ] {
        let err = expect_err(svc.dispatch(action, &["client-1".to_string()], &empty_req(action)));
        assert!(
            is_code(&err, "ResourceNotFoundException"),
            "{action} should report no connection"
        );
    }
}

/// Build a minimal `AwsRequest` for dispatch-level tests.
fn empty_req(_action: &str) -> AwsRequest {
    AwsRequest {
        service: "iotdata".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "000000000000".to_string(),
        request_id: "rid".to_string(),
        headers: http::HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/connections/client-1".to_string(),
        raw_query: String::new(),
        method: Method::GET,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

#[test]
fn routing_maps_methods_and_paths() {
    let cases = [
        ("GET", "/things/t/shadow", "GetThingShadow"),
        ("POST", "/things/t/shadow", "UpdateThingShadow"),
        ("DELETE", "/things/t/shadow", "DeleteThingShadow"),
        (
            "GET",
            "/api/things/shadow/ListNamedShadowsForThing/t",
            "ListNamedShadowsForThing",
        ),
        ("POST", "/topics/a", "Publish"),
        ("GET", "/retainedMessage", "ListRetainedMessages"),
        ("GET", "/retainedMessage/a", "GetRetainedMessage"),
        ("GET", "/connections/c", "GetConnection"),
        ("DELETE", "/connections/c", "DeleteConnection"),
        ("GET", "/connections/c/subscriptions", "ListSubscriptions"),
        ("POST", "/connections/c/messages", "SendDirectMessage"),
    ];
    for (method, path, expected) in cases {
        let mut req = empty_req(expected);
        req.method = Method::from_bytes(method.as_bytes()).unwrap();
        req.raw_path = path.to_string();
        let (action, _) = IotDataService::resolve_action(&req).expect("route");
        assert_eq!(action, expected, "{method} {path}");
    }
}
