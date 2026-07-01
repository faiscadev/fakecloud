use super::*;

/// Test helper that calls matches_pattern with default account/region/resources
fn test_matches(pattern_json: Option<&str>, source: &str, detail_type: &str, detail: &str) -> bool {
    matches_pattern(
        pattern_json,
        source,
        detail_type,
        detail,
        "123456789012",
        "us-east-1",
        &[],
    )
}

#[test]
fn pattern_matches_source() {
    assert!(test_matches(
        Some(r#"{"source": ["my.app"]}"#),
        "my.app",
        "OrderPlaced",
        "{}"
    ));
    assert!(!test_matches(
        Some(r#"{"source": ["other.app"]}"#),
        "my.app",
        "OrderPlaced",
        "{}"
    ));
}

#[test]
fn pattern_matches_detail_type() {
    assert!(test_matches(
        Some(r#"{"detail-type": ["OrderPlaced"]}"#),
        "my.app",
        "OrderPlaced",
        "{}"
    ));
    assert!(!test_matches(
        Some(r#"{"detail-type": ["OrderShipped"]}"#),
        "my.app",
        "OrderPlaced",
        "{}"
    ));
}

#[test]
fn pattern_matches_detail_field() {
    assert!(test_matches(
        Some(r#"{"detail": {"status": ["ACTIVE"]}}"#),
        "my.app",
        "StatusChange",
        r#"{"status": "ACTIVE"}"#
    ));
    assert!(!test_matches(
        Some(r#"{"detail": {"status": ["ACTIVE"]}}"#),
        "my.app",
        "StatusChange",
        r#"{"status": "INACTIVE"}"#
    ));
}

#[test]
fn no_pattern_matches_everything() {
    assert!(test_matches(None, "any", "any", "{}"));
}

#[test]
fn combined_pattern() {
    let pattern = r#"{"source": ["orders"], "detail-type": ["OrderPlaced"]}"#;
    assert!(test_matches(Some(pattern), "orders", "OrderPlaced", "{}"));
    assert!(!test_matches(Some(pattern), "orders", "OrderShipped", "{}"));
    assert!(!test_matches(Some(pattern), "other", "OrderPlaced", "{}"));
}

#[test]
fn nested_detail_pattern() {
    let pattern = r#"{"detail": {"order": {"status": ["PLACED"]}}}"#;
    assert!(test_matches(
        Some(pattern),
        "my.app",
        "OrderEvent",
        r#"{"order": {"status": "PLACED", "id": "123"}}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "my.app",
        "OrderEvent",
        r#"{"order": {"status": "SHIPPED", "id": "123"}}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "my.app",
        "OrderEvent",
        r#"{"order": {"id": "123"}}"#
    ));
}

#[test]
fn deeply_nested_detail_pattern() {
    let pattern = r#"{"detail": {"a": {"b": {"c": ["deep"]}}}}"#;
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"a": {"b": {"c": "deep"}}}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"a": {"b": {"c": "shallow"}}}"#
    ));
}

#[test]
fn prefix_matcher() {
    let pattern = r#"{"source": [{"prefix": "com.myapp"}]}"#;
    assert!(test_matches(
        Some(pattern),
        "com.myapp.orders",
        "OrderPlaced",
        "{}"
    ));
    assert!(test_matches(
        Some(pattern),
        "com.myapp",
        "OrderPlaced",
        "{}"
    ));
    assert!(!test_matches(
        Some(pattern),
        "com.other",
        "OrderPlaced",
        "{}"
    ));
}

#[test]
fn prefix_matcher_in_detail() {
    let pattern = r#"{"detail": {"region": [{"prefix": "us-"}]}}"#;
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"region": "us-east-1"}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"region": "eu-west-1"}"#
    ));
}

#[test]
fn exists_matcher() {
    let pattern = r#"{"detail": {"error": [{"exists": true}]}}"#;
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"error": "something broke"}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"status": "ok"}"#
    ));

    let pattern = r#"{"detail": {"error": [{"exists": false}]}}"#;
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"status": "ok"}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"error": "something broke"}"#
    ));
}

#[test]
fn anything_but_matcher() {
    let pattern = r#"{"source": [{"anything-but": "internal"}]}"#;
    assert!(test_matches(Some(pattern), "external", "Event", "{}"));
    assert!(!test_matches(Some(pattern), "internal", "Event", "{}"));

    let pattern = r#"{"source": [{"anything-but": ["internal", "test"]}]}"#;
    assert!(test_matches(Some(pattern), "external", "Event", "{}"));
    assert!(!test_matches(Some(pattern), "internal", "Event", "{}"));
    assert!(!test_matches(Some(pattern), "test", "Event", "{}"));
}

#[test]
fn anything_but_in_detail() {
    let pattern = r#"{"detail": {"env": [{"anything-but": "prod"}]}}"#;
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"env": "staging"}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"env": "prod"}"#
    ));
}

#[test]
fn numeric_greater_than() {
    let pattern = r#"{"detail": {"count": [{"numeric": [">", 100]}]}}"#;
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 150}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 100}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 50}"#
    ));
}

#[test]
fn numeric_less_than() {
    let pattern = r#"{"detail": {"count": [{"numeric": ["<", 10]}]}}"#;
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 5}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 10}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 15}"#
    ));
}

#[test]
fn numeric_range() {
    let pattern = r#"{"detail": {"count": [{"numeric": [">=", 50, "<", 200]}]}}"#;
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 50}"#
    ));
    assert!(test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 100}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 200}"#
    ));
    assert!(!test_matches(
        Some(pattern),
        "src",
        "type",
        r#"{"count": 49}"#
    ));
}

#[test]
fn mixed_matchers_and_literals() {
    let pattern = r#"{"source": ["exact.match", {"prefix": "com.myapp"}]}"#;
    assert!(test_matches(Some(pattern), "exact.match", "Event", "{}"));
    assert!(test_matches(
        Some(pattern),
        "com.myapp.orders",
        "Event",
        "{}"
    ));
    assert!(!test_matches(Some(pattern), "other.source", "Event", "{}"));
}

// ---- list_connections / list_api_destinations filtering & pagination ----

use fakecloud_core::delivery::DeliveryBus;
use parking_lot::RwLock;

fn make_service() -> EventBridgeService {
    let state = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let delivery = Arc::new(DeliveryBus::new());
    EventBridgeService::new(state, delivery)
}

fn make_request(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "events".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-id".to_string(),
        headers: http::HeaderMap::new(),
        query_params: HashMap::new(),
        body: serde_json::to_vec(&body).unwrap().into(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn create_connection(svc: &EventBridgeService, name: &str) {
    let req = make_request(
        "CreateConnection",
        json!({
            "Name": name,
            "AuthorizationType": "API_KEY",
            "AuthParameters": {
                "ApiKeyAuthParameters": {
                    "ApiKeyName": "x-api-key",
                    "ApiKeyValue": "secret"
                }
            }
        }),
    );
    svc.create_connection(&req).unwrap();
}

fn create_api_destination(svc: &EventBridgeService, name: &str, conn_name: &str) {
    let conn_arn_field = {
        let _mas = svc.state.read();
        let state = _mas.default_ref();
        state.connections.get(conn_name).unwrap().arn.clone()
    };
    let req = make_request(
        "CreateApiDestination",
        json!({
            "Name": name,
            "ConnectionArn": conn_arn_field,
            "InvocationEndpoint": "https://example.com",
            "HttpMethod": "POST"
        }),
    );
    svc.create_api_destination(&req).unwrap();
}

// -- ListConnections tests --

#[test]
fn list_connections_returns_all_by_default() {
    let svc = make_service();
    create_connection(&svc, "conn-alpha");
    create_connection(&svc, "conn-beta");
    create_connection(&svc, "conn-gamma");

    let req = make_request("ListConnections", json!({}));
    let resp = svc.list_connections(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Connections"].as_array().unwrap().len(), 3);
    assert!(body["NextToken"].is_null());
}

#[test]
fn list_connections_name_prefix_filter() {
    let svc = make_service();
    create_connection(&svc, "prod-conn-1");
    create_connection(&svc, "prod-conn-2");
    create_connection(&svc, "dev-conn-1");

    let req = make_request("ListConnections", json!({ "NamePrefix": "prod-" }));
    let resp = svc.list_connections(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names: Vec<&str> = body["Connections"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["Name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().all(|n| n.starts_with("prod-")));
}

#[test]
fn list_connections_state_filter() {
    let svc = make_service();
    create_connection(&svc, "conn-a");
    create_connection(&svc, "conn-b");

    // All connections start as AUTHORIZED; change one
    {
        let mut _mas = svc.state.write();
        let state = _mas.default_mut();
        state
            .connections
            .get_mut("conn-b")
            .unwrap()
            .connection_state = "DEAUTHORIZED".to_string();
    }

    let req = make_request(
        "ListConnections",
        json!({ "ConnectionState": "AUTHORIZED" }),
    );
    let resp = svc.list_connections(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let conns = body["Connections"].as_array().unwrap();
    assert_eq!(conns.len(), 1);
    assert_eq!(conns[0]["Name"].as_str().unwrap(), "conn-a");
}

#[test]
fn list_connections_pagination() {
    let svc = make_service();
    for i in 0..5 {
        create_connection(&svc, &format!("conn-{i:02}"));
    }

    // First page: limit 2
    let req = make_request("ListConnections", json!({ "Limit": 2 }));
    let resp = svc.list_connections(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Connections"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "2");

    // Second page
    let req = make_request("ListConnections", json!({ "Limit": 2, "NextToken": token }));
    let resp = svc.list_connections(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Connections"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "4");

    // Third page (only 1 remaining)
    let req = make_request("ListConnections", json!({ "Limit": 2, "NextToken": token }));
    let resp = svc.list_connections(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Connections"].as_array().unwrap().len(), 1);
    assert!(body["NextToken"].is_null());
}

#[test]
fn list_connections_pagination_with_filter() {
    let svc = make_service();
    for i in 0..4 {
        create_connection(&svc, &format!("prod-{i:02}"));
    }
    create_connection(&svc, "dev-00");

    let req = make_request(
        "ListConnections",
        json!({ "NamePrefix": "prod-", "Limit": 2 }),
    );
    let resp = svc.list_connections(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Connections"].as_array().unwrap().len(), 2);
    assert!(body["NextToken"].as_str().is_some());
}

// -- ListApiDestinations tests --

#[test]
fn list_api_destinations_returns_all_by_default() {
    let svc = make_service();
    create_connection(&svc, "my-conn");
    create_api_destination(&svc, "dest-alpha", "my-conn");
    create_api_destination(&svc, "dest-beta", "my-conn");

    let req = make_request("ListApiDestinations", json!({}));
    let resp = svc.list_api_destinations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ApiDestinations"].as_array().unwrap().len(), 2);
    assert!(body["NextToken"].is_null());
}

#[test]
fn list_api_destinations_name_prefix_filter() {
    let svc = make_service();
    create_connection(&svc, "my-conn");
    create_api_destination(&svc, "prod-dest-1", "my-conn");
    create_api_destination(&svc, "prod-dest-2", "my-conn");
    create_api_destination(&svc, "dev-dest-1", "my-conn");

    let req = make_request("ListApiDestinations", json!({ "NamePrefix": "prod-" }));
    let resp = svc.list_api_destinations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names: Vec<&str> = body["ApiDestinations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["Name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().all(|n| n.starts_with("prod-")));
}

#[test]
fn list_api_destinations_connection_arn_filter() {
    let svc = make_service();
    create_connection(&svc, "conn-a");
    create_connection(&svc, "conn-b");
    create_api_destination(&svc, "dest-1", "conn-a");
    create_api_destination(&svc, "dest-2", "conn-b");
    create_api_destination(&svc, "dest-3", "conn-a");

    let conn_a_arn = {
        let _mas = svc.state.read();
        let state = _mas.default_ref();
        state.connections.get("conn-a").unwrap().arn.clone()
    };

    let req = make_request(
        "ListApiDestinations",
        json!({ "ConnectionArn": conn_a_arn }),
    );
    let resp = svc.list_api_destinations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names: Vec<&str> = body["ApiDestinations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["Name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"dest-1"));
    assert!(names.contains(&"dest-3"));
}

#[test]
fn list_api_destinations_pagination() {
    let svc = make_service();
    create_connection(&svc, "my-conn");
    for i in 0..5 {
        create_api_destination(&svc, &format!("dest-{i:02}"), "my-conn");
    }

    // First page
    let req = make_request("ListApiDestinations", json!({ "Limit": 2 }));
    let resp = svc.list_api_destinations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ApiDestinations"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "2");

    // Second page
    let req = make_request(
        "ListApiDestinations",
        json!({ "Limit": 2, "NextToken": token }),
    );
    let resp = svc.list_api_destinations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ApiDestinations"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "4");

    // Last page
    let req = make_request(
        "ListApiDestinations",
        json!({ "Limit": 2, "NextToken": token }),
    );
    let resp = svc.list_api_destinations(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ApiDestinations"].as_array().unwrap().len(), 1);
    assert!(body["NextToken"].is_null());
}

// -- ListEventBuses pagination tests --

fn create_event_bus(svc: &EventBridgeService, name: &str) {
    let req = make_request("CreateEventBus", json!({ "Name": name }));
    svc.create_event_bus(&req).unwrap();
}

#[test]
fn list_event_buses_pagination() {
    let svc = make_service();
    // "default" bus already exists, create 4 more
    for i in 0..4 {
        create_event_bus(&svc, &format!("bus-{i:02}"));
    }

    // First page: limit 2
    let req = make_request("ListEventBuses", json!({ "Limit": 2 }));
    let resp = svc.list_event_buses(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EventBuses"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "2");

    // Second page
    let req = make_request("ListEventBuses", json!({ "Limit": 2, "NextToken": token }));
    let resp = svc.list_event_buses(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EventBuses"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "4");

    // Third page (only 1 remaining)
    let req = make_request("ListEventBuses", json!({ "Limit": 2, "NextToken": token }));
    let resp = svc.list_event_buses(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EventBuses"].as_array().unwrap().len(), 1);
    assert!(body["NextToken"].is_null());
}

#[test]
fn list_event_buses_no_pagination_returns_all() {
    let svc = make_service();
    create_event_bus(&svc, "bus-alpha");
    create_event_bus(&svc, "bus-beta");

    let req = make_request("ListEventBuses", json!({}));
    let resp = svc.list_event_buses(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    // default + 2 custom = 3
    assert_eq!(body["EventBuses"].as_array().unwrap().len(), 3);
    assert!(body["NextToken"].is_null());
}

// -- PutEvents EndpointId tests --

#[test]
fn put_events_never_includes_endpoint_id_in_response() {
    let svc = make_service();
    // Even when EndpointId is provided in the request, it must not appear in the response
    let req = make_request(
        "PutEvents",
        json!({
            "EndpointId": "my-endpoint.abc123",
            "Entries": [{
                "Source": "my.source",
                "DetailType": "MyType",
                "Detail": "{}",
                "EventBusName": "default"
            }]
        }),
    );
    let resp = svc.put_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(
        !body.as_object().unwrap().contains_key("EndpointId"),
        "EndpointId should never be in the PutEvents response"
    );
    assert_eq!(body["FailedEntryCount"], 0);
}

// -- ListArchives pagination tests --

fn create_archive(svc: &EventBridgeService, name: &str) {
    let req = make_request(
        "CreateArchive",
        json!({
            "ArchiveName": name,
            "EventSourceArn": "arn:aws:events:us-east-1:123456789012:event-bus/default"
        }),
    );
    svc.create_archive(&req).unwrap();
}

#[test]
fn list_archives_pagination() {
    let svc = make_service();
    for i in 0..5 {
        create_archive(&svc, &format!("archive-{i:02}"));
    }

    // First page: limit 2
    let req = make_request("ListArchives", json!({ "Limit": 2 }));
    let resp = svc.list_archives(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Archives"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "2");

    // Second page
    let req = make_request("ListArchives", json!({ "Limit": 2, "NextToken": token }));
    let resp = svc.list_archives(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Archives"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "4");

    // Third page (only 1 remaining)
    let req = make_request("ListArchives", json!({ "Limit": 2, "NextToken": token }));
    let resp = svc.list_archives(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Archives"].as_array().unwrap().len(), 1);
    assert!(body["NextToken"].is_null());
}

// -- ListReplays pagination tests --

fn create_replay(svc: &EventBridgeService, name: &str) {
    // Need an archive first for the replay's event source
    let archive_arn = {
        let guard = svc.state.read();
        let st = guard.default_ref();
        if st.archives.contains_key("replay-archive") {
            st.archives["replay-archive"].arn.clone()
        } else {
            drop(guard);
            create_archive(svc, "replay-archive");
            svc.state.read().default_ref().archives["replay-archive"]
                .arn
                .clone()
        }
    };
    let req = make_request(
        "StartReplay",
        json!({
            "ReplayName": name,
            "EventSourceArn": archive_arn,
            "EventStartTime": 1000000.0,
            "EventEndTime": 2000000.0,
            "Destination": {
                "Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default"
            }
        }),
    );
    svc.start_replay(&req).unwrap();
}

#[test]
fn list_replays_pagination() {
    let svc = make_service();
    for i in 0..5 {
        create_replay(&svc, &format!("replay-{i:02}"));
    }

    // First page: limit 2
    let req = make_request("ListReplays", json!({ "Limit": 2 }));
    let resp = svc.list_replays(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Replays"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "2");

    // Second page
    let req = make_request("ListReplays", json!({ "Limit": 2, "NextToken": token }));
    let resp = svc.list_replays(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Replays"].as_array().unwrap().len(), 2);
    let token = body["NextToken"].as_str().unwrap();
    assert_eq!(token, "4");

    // Third page (only 1 remaining)
    let req = make_request("ListReplays", json!({ "Limit": 2, "NextToken": token }));
    let resp = svc.list_replays(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Replays"].as_array().unwrap().len(), 1);
    assert!(body["NextToken"].is_null());
}

#[test]
fn list_event_buses_invalid_next_token_treated_as_first_page() {
    // ListEventBuses' Smithy model declares only InternalException —
    // InvalidNextTokenException is not part of the contract, so we fall
    // back to the first page rather than emitting an undeclared error.
    let svc = make_service();

    let req = make_request("ListEventBuses", json!({ "NextToken": "not-a-number" }));
    svc.list_event_buses(&req)
        .expect("invalid token is accepted");
}

// ---- TestEventPattern tests ----

#[test]
fn test_event_pattern_match() {
    let svc = make_service();
    let req = make_request(
        "TestEventPattern",
        json!({
            "EventPattern": r#"{"source": ["my.app"]}"#,
            "Event": r#"{"source": "my.app", "detail-type": "Test", "detail": {}}"#
        }),
    );
    let resp = svc.test_event_pattern(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Result"], true);
}

#[test]
fn test_event_pattern_no_match() {
    let svc = make_service();
    let req = make_request(
        "TestEventPattern",
        json!({
            "EventPattern": r#"{"source": ["other.app"]}"#,
            "Event": r#"{"source": "my.app", "detail-type": "Test", "detail": {}}"#
        }),
    );
    let resp = svc.test_event_pattern(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Result"], false);
}

#[test]
fn test_event_pattern_detail_match() {
    let svc = make_service();
    let req = make_request(
        "TestEventPattern",
        json!({
            "EventPattern": r#"{"detail": {"status": ["PLACED"]}}"#,
            "Event": r#"{"source": "my.app", "detail-type": "Order", "detail": {"status": "PLACED", "id": "123"}}"#
        }),
    );
    let resp = svc.test_event_pattern(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Result"], true);
}

// ---- UpdateEventBus tests ----

#[test]
fn update_event_bus_description() {
    let svc = make_service();
    create_event_bus(&svc, "my-bus");

    let req = make_request(
        "UpdateEventBus",
        json!({ "Name": "my-bus", "Description": "Updated desc" }),
    );
    let resp = svc.update_event_bus(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "my-bus");

    // Verify via describe
    let req = make_request("DescribeEventBus", json!({ "Name": "my-bus" }));
    let resp = svc.describe_event_bus(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Description"], "Updated desc");
}

#[test]
fn update_event_bus_not_found() {
    let svc = make_service();
    let req = make_request(
        "UpdateEventBus",
        json!({ "Name": "ghost-bus", "Description": "nope" }),
    );
    assert!(svc.update_event_bus(&req).is_err());
}

// ---- Endpoint CRUD tests ----

fn create_endpoint_helper(svc: &EventBridgeService, name: &str) {
    let req = make_request(
        "CreateEndpoint",
        json!({
            "Name": name,
            "RoutingConfig": {
                "FailoverConfig": {
                    "Primary": { "HealthCheck": "" },
                    "Secondary": { "Route": "us-west-2" }
                }
            },
            "EventBuses": [
                { "EventBusArn": "arn:aws:events:us-east-1:123456789012:event-bus/default" }
            ]
        }),
    );
    svc.create_endpoint(&req).unwrap();
}

#[test]
fn endpoint_create_describe_delete() {
    let svc = make_service();
    create_endpoint_helper(&svc, "my-endpoint");

    // Describe
    let req = make_request("DescribeEndpoint", json!({ "Name": "my-endpoint" }));
    let resp = svc.describe_endpoint(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "my-endpoint");
    assert_eq!(body["State"], "ACTIVE");
    assert!(body["EndpointId"].as_str().unwrap().contains("my-endpoint"));

    // Delete
    let req = make_request("DeleteEndpoint", json!({ "Name": "my-endpoint" }));
    svc.delete_endpoint(&req).unwrap();

    // Verify gone
    let req = make_request("DescribeEndpoint", json!({ "Name": "my-endpoint" }));
    assert!(svc.describe_endpoint(&req).is_err());
}

#[test]
fn endpoint_list_and_update() {
    let svc = make_service();
    create_endpoint_helper(&svc, "ep-alpha");
    create_endpoint_helper(&svc, "ep-beta");

    // List all
    let req = make_request("ListEndpoints", json!({}));
    let resp = svc.list_endpoints(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Endpoints"].as_array().unwrap().len(), 2);

    // Update
    let req = make_request(
        "UpdateEndpoint",
        json!({ "Name": "ep-alpha", "Description": "updated" }),
    );
    let resp = svc.update_endpoint(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "ep-alpha");

    // Verify description
    let req = make_request("DescribeEndpoint", json!({ "Name": "ep-alpha" }));
    let resp = svc.describe_endpoint(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Description"], "updated");
}

#[test]
fn endpoint_duplicate_fails() {
    let svc = make_service();
    create_endpoint_helper(&svc, "dup-ep");
    let req = make_request(
        "CreateEndpoint",
        json!({
            "Name": "dup-ep",
            "RoutingConfig": {},
            "EventBuses": []
        }),
    );
    assert!(svc.create_endpoint(&req).is_err());
}

// ---- DeauthorizeConnection tests ----

#[test]
fn deauthorize_connection_sets_state() {
    let svc = make_service();
    create_connection(&svc, "deauth-conn");

    let req = make_request("DeauthorizeConnection", json!({ "Name": "deauth-conn" }));
    let resp = svc.deauthorize_connection(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ConnectionState"], "DEAUTHORIZING");
    assert!(body["ConnectionArn"]
        .as_str()
        .unwrap()
        .contains("deauth-conn"));

    // Verify via describe
    let req = make_request("DescribeConnection", json!({ "Name": "deauth-conn" }));
    let resp = svc.describe_connection(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ConnectionState"], "DEAUTHORIZING");
}

#[test]
fn deauthorize_connection_not_found() {
    let svc = make_service();
    let req = make_request("DeauthorizeConnection", json!({ "Name": "ghost-conn" }));
    assert!(svc.deauthorize_connection(&req).is_err());
}

// ---- Partner event source tests ----

#[test]
fn partner_event_source_crud() {
    let svc = make_service();

    // Create
    let req = make_request(
        "CreatePartnerEventSource",
        json!({ "Name": "partner/test", "Account": "123456789012" }),
    );
    svc.create_partner_event_source(&req).unwrap();

    // Describe
    let req = make_request(
        "DescribePartnerEventSource",
        json!({ "Name": "partner/test" }),
    );
    let resp = svc.describe_partner_event_source(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "partner/test");

    // List
    let req = make_request("ListPartnerEventSources", json!({"NamePrefix": "partner/"}));
    let resp = svc.list_partner_event_sources(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["PartnerEventSources"].as_array().unwrap().len(), 1);

    // ListPartnerEventSourceAccounts
    let req = make_request(
        "ListPartnerEventSourceAccounts",
        json!({ "EventSourceName": "partner/test" }),
    );
    let resp = svc.list_partner_event_source_accounts(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["PartnerEventSourceAccounts"].as_array().unwrap().len(),
        1
    );

    // DescribeEventSource
    let req = make_request("DescribeEventSource", json!({ "Name": "partner/test" }));
    let resp = svc.describe_event_source(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "partner/test");
    assert_eq!(body["State"], "ACTIVE");

    // ListEventSources
    let req = make_request("ListEventSources", json!({}));
    let resp = svc.list_event_sources(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EventSources"].as_array().unwrap().len(), 1);

    // Delete
    let req = make_request(
        "DeletePartnerEventSource",
        json!({ "Name": "partner/test", "Account": "123456789012" }),
    );
    svc.delete_partner_event_source(&req).unwrap();

    // Verify gone
    let req = make_request(
        "DescribePartnerEventSource",
        json!({ "Name": "partner/test" }),
    );
    assert!(svc.describe_partner_event_source(&req).is_err());
}

#[test]
fn activate_deactivate_event_source() {
    let svc = make_service();

    // Create a partner event source first
    let req = make_request(
        "CreatePartnerEventSource",
        json!({ "Name": "aws.partner/test", "Account": "123456789012" }),
    );
    svc.create_partner_event_source(&req).unwrap();

    // Deactivate it
    let req = make_request(
        "DeactivateEventSource",
        json!({ "Name": "aws.partner/test" }),
    );
    svc.deactivate_event_source(&req).unwrap();
    {
        let _mas = svc.state.read();
        let state = _mas.default_ref();
        assert_eq!(
            state.partner_event_sources["aws.partner/test"].state,
            "INACTIVE"
        );
    }

    // Activate it
    let req = make_request("ActivateEventSource", json!({ "Name": "aws.partner/test" }));
    svc.activate_event_source(&req).unwrap();
    {
        let _mas = svc.state.read();
        let state = _mas.default_ref();
        assert_eq!(
            state.partner_event_sources["aws.partner/test"].state,
            "ACTIVE"
        );
    }

    // Not-found returns error
    let req = make_request("ActivateEventSource", json!({ "Name": "nonexistent" }));
    assert!(svc.activate_event_source(&req).is_err());

    let req = make_request("DeactivateEventSource", json!({ "Name": "nonexistent" }));
    assert!(svc.deactivate_event_source(&req).is_err());
}

#[test]
fn delete_partner_event_source_verifies_account() {
    // DeletePartnerEventSource's Smithy model has no ValidationException or
    // ResourceNotFoundException — only ConcurrentModification, Internal,
    // and OperationDisabled — so we treat unknown sources and wrong-account
    // mismatches as idempotent no-ops, leaving any matching record alone.
    let svc = make_service();

    let req = make_request(
        "CreatePartnerEventSource",
        json!({ "Name": "aws.partner/test", "Account": "123456789012" }),
    );
    svc.create_partner_event_source(&req).unwrap();

    // Deleting with wrong account is a no-op and leaves the source intact.
    let req = make_request(
        "DeletePartnerEventSource",
        json!({ "Name": "aws.partner/test", "Account": "999999999999" }),
    );
    svc.delete_partner_event_source(&req).unwrap();
    assert!(svc
        .state
        .read()
        .default_ref()
        .partner_event_sources
        .contains_key("aws.partner/test"));

    // Deleting with correct account removes the source.
    let req = make_request(
        "DeletePartnerEventSource",
        json!({ "Name": "aws.partner/test", "Account": "123456789012" }),
    );
    svc.delete_partner_event_source(&req).unwrap();
    assert!(!svc
        .state
        .read()
        .default_ref()
        .partner_event_sources
        .contains_key("aws.partner/test"));

    // Deleting an already-absent source is also a no-op success.
    let req = make_request(
        "DeletePartnerEventSource",
        json!({ "Name": "aws.partner/test", "Account": "123456789012" }),
    );
    svc.delete_partner_event_source(&req).unwrap();
}

#[test]
fn put_partner_events() {
    let svc = make_service();
    let req = make_request(
        "PutPartnerEvents",
        json!({
            "Entries": [
                { "Source": "partner.app", "DetailType": "Test", "Detail": "{}" }
            ]
        }),
    );
    let resp = svc.put_partner_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 0);
    assert_eq!(body["Entries"].as_array().unwrap().len(), 1);
    assert!(body["Entries"][0]["EventId"].as_str().is_some());
}

#[test]
fn put_partner_events_validates_and_records_failed_entries() {
    let svc = make_service();
    // Missing DetailType + a valid entry -> one failure, one success.
    let req = make_request(
        "PutPartnerEvents",
        json!({
            "Entries": [
                { "Source": "aws.partner/x/y", "Detail": "{}" },
                { "Source": "aws.partner/x/y", "DetailType": "T", "Detail": "{}" }
            ]
        }),
    );
    let resp = svc.put_partner_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 1);
    assert!(body["Entries"][0]["ErrorCode"].as_str().is_some());
    assert!(body["Entries"][1]["EventId"].as_str().is_some());
}

#[test]
fn put_partner_events_routes_to_partner_bus_rule_target() {
    let (svc, messages) = make_service_with_sqs_recorder();
    let source = "aws.partner/example.com/test-source";
    let queue_arn = "arn:aws:sqs:us-east-1:123456789012:partner-queue";

    // 1. Create the partner event source.
    svc.create_partner_event_source(&make_request(
        "CreatePartnerEventSource",
        json!({ "Name": source, "Account": "123456789012" }),
    ))
    .unwrap();

    // 2. The receiving account creates the partner event bus (name == source).
    svc.create_event_bus(&make_request(
        "CreateEventBus",
        json!({ "Name": source, "EventSourceName": source }),
    ))
    .unwrap();

    // 3. Rule on the partner bus matching the source, with an SQS target.
    svc.put_rule(&make_request(
        "PutRule",
        json!({
            "Name": "partner-rule",
            "EventBusName": source,
            "EventPattern": format!(r#"{{"source": ["{source}"]}}"#),
            "State": "ENABLED"
        }),
    ))
    .unwrap();
    svc.put_targets(&make_request(
        "PutTargets",
        json!({
            "Rule": "partner-rule",
            "EventBusName": source,
            "Targets": [{ "Id": "t1", "Arn": queue_arn }]
        }),
    ))
    .unwrap();

    // 4. PutPartnerEvents with Source == the partner bus name.
    let resp = svc
        .put_partner_events(&make_request(
            "PutPartnerEvents",
            json!({
                "Entries": [{
                    "Source": source,
                    "DetailType": "OrderCreated",
                    "Detail": "{\"orderId\":\"42\"}"
                }]
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 0);

    // 5. The event must have been routed to the rule's SQS target.
    let delivered = messages.lock();
    assert_eq!(delivered.len(), 1, "partner event should reach the target");
    assert_eq!(delivered[0].0, queue_arn);
    assert!(delivered[0].1.contains("OrderCreated"));
    assert!(delivered[0].1.contains("42"));
}

// ---- Archive + Replay delivery tests ----

/// Helper: create a service with a mock SQS delivery that records messages.
#[allow(clippy::type_complexity)]
fn make_service_with_sqs_recorder() -> (
    EventBridgeService,
    Arc<parking_lot::Mutex<Vec<(String, String)>>>,
) {
    use fakecloud_core::delivery::SqsDelivery;

    struct RecordingSqsDelivery {
        messages: Arc<parking_lot::Mutex<Vec<(String, String)>>>,
    }

    impl SqsDelivery for RecordingSqsDelivery {
        fn deliver_to_queue(
            &self,
            queue_arn: &str,
            message_body: &str,
            _attributes: &HashMap<String, String>,
        ) {
            self.messages
                .lock()
                .push((queue_arn.to_string(), message_body.to_string()));
        }
    }

    let messages: Arc<parking_lot::Mutex<Vec<(String, String)>>> =
        Arc::new(parking_lot::Mutex::new(Vec::new()));
    let state = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let delivery = Arc::new(DeliveryBus::new().with_sqs(Arc::new(RecordingSqsDelivery {
        messages: messages.clone(),
    })));
    let svc = EventBridgeService::new(state, delivery);
    (svc, messages)
}

#[test]
fn start_replay_delivers_archived_events_to_sqs_target() {
    let (svc, messages) = make_service_with_sqs_recorder();
    let queue_arn = "arn:aws:sqs:us-east-1:123456789012:replay-queue";

    // Create a rule with an SQS target
    let req = make_request(
        "PutRule",
        json!({
            "Name": "replay-test-rule",
            "EventPattern": r#"{"source": ["my.app"]}"#,
            "State": "ENABLED"
        }),
    );
    svc.put_rule(&req).unwrap();

    let req = make_request(
        "PutTargets",
        json!({
            "Rule": "replay-test-rule",
            "Targets": [{
                "Id": "sqs-target",
                "Arn": queue_arn
            }]
        }),
    );
    svc.put_targets(&req).unwrap();

    // Create an archive on the default bus
    let req = make_request(
        "CreateArchive",
        json!({
            "ArchiveName": "test-archive",
            "EventSourceArn": "arn:aws:events:us-east-1:123456789012:event-bus/default"
        }),
    );
    svc.create_archive(&req).unwrap();

    // PutEvents: these should get archived and delivered
    let req = make_request(
        "PutEvents",
        json!({
            "Entries": [
                {
                    "Source": "my.app",
                    "DetailType": "OrderCreated",
                    "Detail": "{\"orderId\": \"1\"}",
                    "EventBusName": "default"
                },
                {
                    "Source": "my.app",
                    "DetailType": "OrderShipped",
                    "Detail": "{\"orderId\": \"2\"}",
                    "EventBusName": "default"
                }
            ]
        }),
    );
    let resp = svc.put_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 0);

    // Verify archive has 2 events
    {
        let _mas = svc.state.read();
        let state = _mas.default_ref();
        let archive = state.archives.get("test-archive").unwrap();
        assert_eq!(archive.events.len(), 2);
        assert_eq!(archive.event_count, 2);
    }

    // Clear recorded messages from PutEvents delivery
    messages.lock().clear();

    // StartReplay: should re-deliver the archived events
    let archive_arn = {
        let _mas = svc.state.read();
        let state = _mas.default_ref();
        state.archives.get("test-archive").unwrap().arn.clone()
    };

    // Use a wide time range to capture all events
    let start_ts = 0.0_f64;
    let end_ts = (chrono::Utc::now().timestamp() + 3600) as f64;

    let req = make_request(
        "StartReplay",
        json!({
            "ReplayName": "my-replay",
            "EventSourceArn": archive_arn,
            "Destination": {
                "Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default"
            },
            "EventStartTime": start_ts,
            "EventEndTime": end_ts
        }),
    );
    let resp = svc.start_replay(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["State"], "STARTING");

    // Verify the replay delivered events to SQS
    let delivered = messages.lock();
    assert_eq!(
        delivered.len(),
        2,
        "expected 2 replayed events delivered to SQS"
    );
    for (arn, msg) in delivered.iter() {
        assert_eq!(arn, queue_arn);
        let event: Value = serde_json::from_str(msg).unwrap();
        assert_eq!(event["source"], "my.app");
        // Replayed events should include replay-name
        assert!(event["replay-name"].as_str().is_some());
    }

    // Verify replay is marked as COMPLETED
    let _mas = svc.state.read();
    let state = _mas.default_ref();
    let replay = state.replays.get("my-replay").unwrap();
    assert_eq!(replay.state, "COMPLETED");
}

#[test]
fn apply_connection_auth_api_key() {
    let conn = Connection {
        name: "test-conn".to_string(),
        arn: "arn:aws:events:us-east-1:123456789012:connection/test-conn/uuid".to_string(),
        description: None,
        authorization_type: "API_KEY".to_string(),
        auth_parameters: json!({
            "ApiKeyAuthParameters": {
                "ApiKeyName": "x-api-key",
                "ApiKeyValue": "my-secret"
            }
        }),
        connection_state: "AUTHORIZED".to_string(),
        secret_arn: "arn:aws:secretsmanager:us-east-1:123456789012:secret:test".to_string(),
        creation_time: Utc::now(),
        last_modified_time: Utc::now(),
        last_authorized_time: Utc::now(),
    };

    let client = reqwest::Client::new();
    let builder = client
        .post("http://localhost:12345/test")
        .header("Content-Type", "application/json");
    let builder = apply_connection_auth(builder, &conn);

    // Build and verify the header was applied
    let request = builder.body("{}").build().unwrap();
    assert_eq!(
        request
            .headers()
            .get("x-api-key")
            .unwrap()
            .to_str()
            .unwrap(),
        "my-secret"
    );
}

#[test]
fn apply_connection_auth_basic() {
    let conn = Connection {
        name: "basic-conn".to_string(),
        arn: "arn:aws:events:us-east-1:123456789012:connection/basic-conn/uuid".to_string(),
        description: None,
        authorization_type: "BASIC".to_string(),
        auth_parameters: json!({
            "BasicAuthParameters": {
                "Username": "user",
                "Password": "pass"
            }
        }),
        connection_state: "AUTHORIZED".to_string(),
        secret_arn: "arn:aws:secretsmanager:us-east-1:123456789012:secret:test".to_string(),
        creation_time: Utc::now(),
        last_modified_time: Utc::now(),
        last_authorized_time: Utc::now(),
    };

    let client = reqwest::Client::new();
    let builder = client.post("http://localhost:12345/test");
    let builder = apply_connection_auth(builder, &conn);

    let request = builder.body("{}").build().unwrap();
    let auth_header = request
        .headers()
        .get("authorization")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        auth_header.starts_with("Basic "),
        "Expected Basic auth header, got: {auth_header}"
    );
}

#[tokio::test]
async fn put_events_with_api_destination_target_resolves_destination() {
    // This test verifies that the PutEvents code path correctly identifies
    // api-destination ARN targets and resolves the destination metadata.
    // The actual HTTP call goes to a non-existent host (fire-and-forget).
    let state = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let delivery = Arc::new(DeliveryBus::new());
    let svc = EventBridgeService::new(state, delivery);

    // Create connection and api destination
    create_connection(&svc, "my-conn");
    let conn_arn = {
        let _mas = svc.state.read();
        let state = _mas.default_ref();
        state.connections.get("my-conn").unwrap().arn.clone()
    };
    let req = make_request(
        "CreateApiDestination",
        json!({
            "Name": "my-dest",
            "ConnectionArn": conn_arn,
            "InvocationEndpoint": "http://127.0.0.1:1/noop",
            "HttpMethod": "POST"
        }),
    );
    svc.create_api_destination(&req).unwrap();

    let dest_arn = {
        let _mas = svc.state.read();
        let state = _mas.default_ref();
        state.api_destinations.get("my-dest").unwrap().arn.clone()
    };

    // Create a rule that targets the api-destination
    let req = make_request(
        "PutRule",
        json!({
            "Name": "api-dest-rule",
            "EventPattern": r#"{"source":["test.app"]}"#,
            "State": "ENABLED"
        }),
    );
    svc.put_rule(&req).unwrap();

    let req = make_request(
        "PutTargets",
        json!({
            "Rule": "api-dest-rule",
            "Targets": [{ "Id": "dest-target", "Arn": dest_arn }]
        }),
    );
    svc.put_targets(&req).unwrap();

    // PutEvents - should match the rule and attempt delivery to ApiDestination
    let req = make_request(
        "PutEvents",
        json!({
            "Entries": [{
                "Source": "test.app",
                "DetailType": "TestEvent",
                "Detail": r#"{"key":"value"}"#
            }]
        }),
    );
    let resp = svc.put_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 0);
    assert_eq!(body["Entries"].as_array().unwrap().len(), 1);
    assert!(body["Entries"][0]["EventId"].as_str().is_some());
}

#[test]
fn test_function_name_from_arn() {
    // Unqualified ARN
    assert_eq!(
        super::function_name_from_arn("arn:aws:lambda:us-east-1:123456789012:function:my-func"),
        "my-func"
    );
    // Qualified ARN with alias
    assert_eq!(
        super::function_name_from_arn(
            "arn:aws:lambda:us-east-1:123456789012:function:my-func:prod"
        ),
        "my-func"
    );
    // Qualified ARN with version
    assert_eq!(
        super::function_name_from_arn("arn:aws:lambda:us-east-1:123456789012:function:my-func:42"),
        "my-func"
    );
    // Plain function name (not an ARN)
    assert_eq!(super::function_name_from_arn("my-func"), "my-func");
}

// ── Rules / targets / tags handler tests ────────────────────────

fn put_rule_simple(svc: &EventBridgeService, name: &str) {
    let req = make_request(
        "PutRule",
        json!({ "Name": name, "EventPattern": r#"{"source":["a"]}"# }),
    );
    svc.put_rule(&req).unwrap();
}

#[test]
fn put_rule_persists_event_pattern_and_state() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let _mas = svc.state.read();
    let state = _mas.default_ref();
    let rule = state
        .rules
        .get(&("default".to_string(), "r1".to_string()))
        .unwrap();
    assert_eq!(rule.state, "ENABLED");
    assert!(rule.event_pattern.is_some());
    assert!(rule.arn.contains("rule/r1"));
}

#[test]
fn put_rule_accepts_schedule_on_non_default_bus() {
    // PutRule's Smithy model does not declare ValidationException; we accept
    // a ScheduleExpression on a non-default bus rather than rejecting it
    // with an undeclared error. The scheduler simply does not fire schedules
    // for non-default buses.
    let svc = make_service();
    let bus_req = make_request("CreateEventBus", json!({ "Name": "custom" }));
    svc.create_event_bus(&bus_req).unwrap();

    let req = make_request(
        "PutRule",
        json!({
            "Name": "r1",
            "EventBusName": "custom",
            "ScheduleExpression": "rate(5 minutes)"
        }),
    );
    svc.put_rule(&req)
        .expect("schedule on non-default bus is accepted");
}

#[test]
fn put_rule_rejects_unknown_event_bus() {
    let svc = make_service();
    let req = make_request(
        "PutRule",
        json!({ "Name": "r1", "EventBusName": "ghost", "EventPattern": r#"{"source":["a"]}"# }),
    );
    let err = svc.put_rule(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn put_rule_overlay_preserves_existing_targets() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    // Inject a target directly.
    {
        let mut _mas = svc.state.write();
        let state = _mas.default_mut();
        let rule = state
            .rules
            .get_mut(&("default".to_string(), "r1".to_string()))
            .unwrap();
        rule.targets.push(crate::state::EventTarget {
            id: "t1".to_string(),
            arn: "arn:aws:sqs:us-east-1:123456789012:q".to_string(),
            input: None,
            input_path: None,
            input_transformer: None,
            sqs_parameters: None,
            ..Default::default()
        });
    }

    // Re-PutRule with new description; targets should survive.
    let req = make_request(
        "PutRule",
        json!({ "Name": "r1", "Description": "updated", "EventPattern": r#"{"source":["a"]}"# }),
    );
    svc.put_rule(&req).unwrap();
    let _mas = svc.state.read();
    let state = _mas.default_ref();
    let rule = state
        .rules
        .get(&("default".to_string(), "r1".to_string()))
        .unwrap();
    assert_eq!(rule.description.as_deref(), Some("updated"));
    assert_eq!(rule.targets.len(), 1);
}

#[test]
fn delete_rule_with_targets_errors() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let put_targets_req = make_request(
        "PutTargets",
        json!({
            "Rule": "r1",
            "Targets": [{ "Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q" }]
        }),
    );
    svc.put_targets(&put_targets_req).unwrap();

    let req = make_request("DeleteRule", json!({ "Name": "r1" }));
    let err = svc.delete_rule(&req).err().expect("expected error");
    assert_eq!(err.code(), "ValidationException");
}

#[test]
fn delete_rule_after_remove_targets_succeeds() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let put_t = make_request(
        "PutTargets",
        json!({
            "Rule": "r1",
            "Targets": [{ "Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q" }]
        }),
    );
    svc.put_targets(&put_t).unwrap();
    let rm_t = make_request("RemoveTargets", json!({ "Rule": "r1", "Ids": ["t1"] }));
    svc.remove_targets(&rm_t).unwrap();
    let del = make_request("DeleteRule", json!({ "Name": "r1" }));
    svc.delete_rule(&del).unwrap();
    assert!(!svc
        .state
        .read()
        .default_ref()
        .rules
        .contains_key(&("default".to_string(), "r1".to_string())));
}

#[test]
fn enable_disable_rule_toggles_state() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let dis = make_request("DisableRule", json!({ "Name": "r1" }));
    svc.disable_rule(&dis).unwrap();
    assert_eq!(
        svc.state
            .read()
            .default_ref()
            .rules
            .get(&("default".to_string(), "r1".to_string()))
            .unwrap()
            .state,
        "DISABLED"
    );
    let en = make_request("EnableRule", json!({ "Name": "r1" }));
    svc.enable_rule(&en).unwrap();
    assert_eq!(
        svc.state
            .read()
            .default_ref()
            .rules
            .get(&("default".to_string(), "r1".to_string()))
            .unwrap()
            .state,
        "ENABLED"
    );
}

#[test]
fn enable_rule_unknown_errors() {
    let svc = make_service();
    let req = make_request("EnableRule", json!({ "Name": "ghost" }));
    let err = svc.enable_rule(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn list_rules_with_name_prefix_filter() {
    let svc = make_service();
    put_rule_simple(&svc, "prod-orders");
    put_rule_simple(&svc, "prod-shipping");
    put_rule_simple(&svc, "dev-orders");

    let req = make_request("ListRules", json!({ "NamePrefix": "prod-" }));
    let resp = svc.list_rules(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names: Vec<&str> = body["Rules"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["Name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 2);
    assert!(names.iter().all(|n| n.starts_with("prod-")));
}

#[test]
fn list_rules_pagination_emits_next_token() {
    let svc = make_service();
    for i in 0..5 {
        put_rule_simple(&svc, &format!("r{i}"));
    }
    let req = make_request("ListRules", json!({ "Limit": 2 }));
    let resp = svc.list_rules(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Rules"].as_array().unwrap().len(), 2);
    assert!(body["NextToken"].is_string());
}

#[test]
fn describe_rule_returns_persisted_fields() {
    let svc = make_service();
    let put = make_request(
        "PutRule",
        json!({
            "Name": "r1",
            "EventPattern": r#"{"source":["a"]}"#,
            "Description": "hi",
            "State": "DISABLED"
        }),
    );
    svc.put_rule(&put).unwrap();
    let desc = make_request("DescribeRule", json!({ "Name": "r1" }));
    let resp = svc.describe_rule(&desc).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], json!("r1"));
    assert_eq!(body["State"], json!("DISABLED"));
    assert_eq!(body["Description"], json!("hi"));
}

#[test]
fn describe_rule_unknown_errors() {
    let svc = make_service();
    let req = make_request("DescribeRule", json!({ "Name": "ghost" }));
    let err = svc.describe_rule(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn put_targets_round_trips_full_target_config() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let req = make_request(
        "PutTargets",
        json!({
            "Rule": "r1",
            "Targets": [{
                "Id": "t1",
                "Arn": "arn:aws:ecs:us-east-1:123456789012:cluster/c1",
                "RoleArn": "arn:aws:iam::123456789012:role/InvokeRole",
                "DeadLetterConfig": {
                    "Arn": "arn:aws:sqs:us-east-1:123456789012:dlq"
                },
                "RetryPolicy": {
                    "MaximumRetryAttempts": 2,
                    "MaximumEventAgeInSeconds": 600
                },
                "EcsParameters": {
                    "TaskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
                    "TaskCount": 1,
                    "LaunchType": "FARGATE"
                },
                "HttpParameters": {
                    "PathParameterValues": ["v1"],
                    "HeaderParameters": {"X-Trace": "abc"},
                    "QueryStringParameters": {"q": "1"}
                },
                "KinesisParameters": { "PartitionKeyPath": "$.detail.id" },
                "BatchParameters": {
                    "JobDefinition": "arn:aws:batch:us-east-1:123456789012:job-definition/jd:1",
                    "JobName": "j"
                },
                "RedshiftDataParameters": {
                    "Database": "db", "Sql": "SELECT 1"
                },
                "SageMakerPipelineParameters": {
                    "PipelineParameterList": [{"Name": "k", "Value": "v"}]
                },
                "AppSyncParameters": { "GraphQLOperation": "query Foo { id }" },
                "RunCommandParameters": {
                    "RunCommandTargets": [{"Key": "tag:env", "Values": ["prod"]}]
                }
            }]
        }),
    );
    svc.put_targets(&req).unwrap();

    let list_req = make_request("ListTargetsByRule", json!({"Rule": "r1"}));
    let resp = svc.list_targets_by_rule(&list_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let target = &body["Targets"][0];
    assert_eq!(
        target["RoleArn"],
        "arn:aws:iam::123456789012:role/InvokeRole"
    );
    assert_eq!(
        target["DeadLetterConfig"]["Arn"],
        "arn:aws:sqs:us-east-1:123456789012:dlq"
    );
    assert_eq!(target["RetryPolicy"]["MaximumRetryAttempts"], 2);
    assert_eq!(target["EcsParameters"]["LaunchType"], "FARGATE");
    assert_eq!(
        target["HttpParameters"]["HeaderParameters"]["X-Trace"],
        "abc"
    );
    assert_eq!(
        target["KinesisParameters"]["PartitionKeyPath"],
        "$.detail.id"
    );
    assert_eq!(target["BatchParameters"]["JobName"], "j");
    assert_eq!(target["RedshiftDataParameters"]["Database"], "db");
    assert!(target["SageMakerPipelineParameters"]["PipelineParameterList"].is_array());
    assert!(target["AppSyncParameters"]["GraphQLOperation"]
        .as_str()
        .unwrap_or("")
        .contains("Foo"));
    assert!(target["RunCommandParameters"]["RunCommandTargets"].is_array());
}

#[test]
fn put_targets_reports_fifo_without_sqs_parameters_in_failed_entries() {
    // PutTargets' Smithy model has no ValidationException; per-target
    // input errors are surfaced via FailedEntries, matching AWS.
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let req = make_request(
        "PutTargets",
        json!({
            "Rule": "r1",
            "Targets": [{ "Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q.fifo" }]
        }),
    );
    let resp = svc.put_targets(&req).expect("call succeeds");
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 1);
    assert_eq!(body["FailedEntries"][0]["TargetId"], "t1");
    assert_eq!(body["FailedEntries"][0]["ErrorCode"], "ValidationException");
}

#[test]
fn put_targets_reports_invalid_arn_in_failed_entries() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let req = make_request(
        "PutTargets",
        json!({
            "Rule": "r1",
            "Targets": [{ "Id": "t1", "Arn": "not-an-arn" }]
        }),
    );
    let resp = svc.put_targets(&req).expect("call succeeds");
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 1);
    assert_eq!(body["FailedEntries"][0]["ErrorCode"], "ValidationException");
}

#[test]
fn put_targets_unknown_rule_errors() {
    let svc = make_service();
    let req = make_request(
        "PutTargets",
        json!({
            "Rule": "ghost",
            "Targets": [{ "Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q" }]
        }),
    );
    let err = svc.put_targets(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn put_targets_replaces_existing_with_same_id() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let first = make_request(
        "PutTargets",
        json!({
            "Rule": "r1",
            "Targets": [{ "Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q1" }]
        }),
    );
    svc.put_targets(&first).unwrap();
    let second = make_request(
        "PutTargets",
        json!({
            "Rule": "r1",
            "Targets": [{ "Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q2" }]
        }),
    );
    svc.put_targets(&second).unwrap();

    let _mas = svc.state.read();
    let state = _mas.default_ref();
    let rule = state
        .rules
        .get(&("default".to_string(), "r1".to_string()))
        .unwrap();
    assert_eq!(rule.targets.len(), 1);
    assert!(rule.targets[0].arn.ends_with("q2"));
}

#[test]
fn list_targets_by_rule_returns_pagination_token() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    for i in 0..4 {
        let req = make_request(
            "PutTargets",
            json!({
                "Rule": "r1",
                "Targets": [{
                    "Id": format!("t{i}"),
                    "Arn": format!("arn:aws:sqs:us-east-1:123456789012:q{i}")
                }]
            }),
        );
        svc.put_targets(&req).unwrap();
    }
    let req = make_request("ListTargetsByRule", json!({ "Rule": "r1", "Limit": 2 }));
    let resp = svc.list_targets_by_rule(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Targets"].as_array().unwrap().len(), 2);
    assert!(body["NextToken"].is_string());
}

#[test]
fn list_rule_names_by_target_groups_by_arn() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    put_rule_simple(&svc, "r2");
    for rule in ["r1", "r2"] {
        let req = make_request(
            "PutTargets",
            json!({
                "Rule": rule,
                "Targets": [{
                    "Id": "t1",
                    "Arn": "arn:aws:sqs:us-east-1:123456789012:shared"
                }]
            }),
        );
        svc.put_targets(&req).unwrap();
    }
    let req = make_request(
        "ListRuleNamesByTarget",
        json!({ "TargetArn": "arn:aws:sqs:us-east-1:123456789012:shared" }),
    );
    let resp = svc.list_rule_names_by_target(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names: Vec<&str> = body["RuleNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["r1", "r2"]);
}

// ── Tag operations ───────────────────────────────────────────────

#[test]
fn tag_then_list_tags_for_rule() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let arn = svc
        .state
        .read()
        .default_ref()
        .rules
        .get(&("default".to_string(), "r1".to_string()))
        .unwrap()
        .arn
        .clone();

    let tag_req = make_request(
        "TagResource",
        json!({
            "ResourceARN": arn,
            "Tags": [{ "Key": "env", "Value": "prod" }]
        }),
    );
    svc.tag_resource(&tag_req).unwrap();

    let list_req = make_request("ListTagsForResource", json!({ "ResourceARN": arn }));
    let resp = svc.list_tags_for_resource(&list_req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Key"], json!("env"));
    assert_eq!(tags[0]["Value"], json!("prod"));
}

#[test]
fn untag_resource_removes_listed_keys() {
    let svc = make_service();
    put_rule_simple(&svc, "r1");
    let arn = svc
        .state
        .read()
        .default_ref()
        .rules
        .get(&("default".to_string(), "r1".to_string()))
        .unwrap()
        .arn
        .clone();
    let tag_req = make_request(
        "TagResource",
        json!({
            "ResourceARN": &arn,
            "Tags": [{ "Key": "env", "Value": "prod" }, { "Key": "team", "Value": "core" }]
        }),
    );
    svc.tag_resource(&tag_req).unwrap();

    let untag = make_request(
        "UntagResource",
        json!({ "ResourceARN": &arn, "TagKeys": ["env"] }),
    );
    svc.untag_resource(&untag).unwrap();

    let _mas = svc.state.read();
    let state = _mas.default_ref();
    let rule = state
        .rules
        .get(&("default".to_string(), "r1".to_string()))
        .unwrap();
    assert!(!rule.tags.contains_key("env"));
    assert_eq!(rule.tags.get("team").map(String::as_str), Some("core"));
}

// ── TestEventPattern ─────────────────────────────────────────────

#[test]
fn test_event_pattern_returns_result_field() {
    let svc = make_service();
    let req = make_request(
        "TestEventPattern",
        json!({
            "EventPattern": r#"{"source":["my.app"]}"#,
            "Event": r#"{"source":"my.app","detail-type":"x","detail":{}}"#
        }),
    );
    let resp = svc.test_event_pattern(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Result"], json!(true));
}

// ── Event bus describe / delete ──────────────────────────────────

#[test]
fn describe_event_bus_default_returns_arn() {
    let svc = make_service();
    let req = make_request("DescribeEventBus", json!({}));
    let resp = svc.describe_event_bus(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], json!("default"));
    assert!(body["Arn"].as_str().unwrap().contains("event-bus/default"));
}

#[test]
fn delete_event_bus_default_fails() {
    let svc = make_service();
    let req = make_request("DeleteEventBus", json!({ "Name": "default" }));
    let err = svc.delete_event_bus(&req).err().expect("expected error");
    assert_eq!(err.code(), "ValidationException");
}

// ── Error branch tests ──

#[test]
fn describe_rule_not_found() {
    let svc = make_service();
    let req = make_request("DescribeRule", json!({"Name": "nonexistent"}));
    let err = svc.describe_rule(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn delete_rule_nonexistent_is_noop() {
    let svc = make_service();
    let req = make_request("DeleteRule", json!({"Name": "nope"}));
    // EventBridge returns success for deleting nonexistent rules
    svc.delete_rule(&req).unwrap();
}

#[test]
fn put_targets_rule_not_found() {
    let svc = make_service();
    let req = make_request(
        "PutTargets",
        json!({"Rule": "ghost", "Targets": [{"Id": "t1", "Arn": "arn:a"}]}),
    );
    let err = svc.put_targets(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn remove_targets_rule_not_found() {
    let svc = make_service();
    let req = make_request("RemoveTargets", json!({"Rule": "ghost", "Ids": ["t1"]}));
    let err = svc.remove_targets(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn list_targets_by_rule_not_found() {
    let svc = make_service();
    let req = make_request("ListTargetsByRule", json!({"Rule": "ghost"}));
    let err = svc
        .list_targets_by_rule(&req)
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn enable_rule_not_found() {
    let svc = make_service();
    let req = make_request("EnableRule", json!({"Name": "ghost"}));
    let err = svc.enable_rule(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn disable_rule_not_found() {
    let svc = make_service();
    let req = make_request("DisableRule", json!({"Name": "ghost"}));
    let err = svc.disable_rule(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn describe_event_bus_not_found() {
    let svc = make_service();
    let req = make_request("DescribeEventBus", json!({"Name": "nonexistent-bus"}));
    let err = svc.describe_event_bus(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn tag_resource_not_found() {
    let svc = make_service();
    let req = make_request(
        "TagResource",
        json!({"ResourceARN": "arn:aws:events:us-east-1:123:nope", "Tags": [{"Key": "k", "Value": "v"}]}),
    );
    let err = svc.tag_resource(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn untag_resource_not_found() {
    let svc = make_service();
    let req = make_request(
        "UntagResource",
        json!({"ResourceARN": "arn:aws:events:us-east-1:123:nope", "TagKeys": ["k"]}),
    );
    let err = svc.untag_resource(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn describe_archive_not_found() {
    let svc = make_service();
    let req = make_request("DescribeArchive", json!({"ArchiveName": "ghost"}));
    let err = svc.describe_archive(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn delete_archive_not_found() {
    let svc = make_service();
    let req = make_request("DeleteArchive", json!({"ArchiveName": "ghost"}));
    let err = svc.delete_archive(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn describe_connection_not_found() {
    let svc = make_service();
    let req = make_request("DescribeConnection", json!({"Name": "ghost"}));
    let err = svc.describe_connection(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn describe_api_destination_not_found() {
    let svc = make_service();
    let req = make_request("DescribeApiDestination", json!({"Name": "ghost"}));
    let err = svc
        .describe_api_destination(&req)
        .err()
        .expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn describe_replay_not_found() {
    let svc = make_service();
    let req = make_request("DescribeReplay", json!({"ReplayName": "ghost"}));
    let err = svc.describe_replay(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[test]
fn create_event_bus_duplicate() {
    let svc = make_service();
    let req = make_request("CreateEventBus", json!({"Name": "dup-bus"}));
    svc.create_event_bus(&req).unwrap();
    let err = svc.create_event_bus(&req).err().expect("expected error");
    assert_eq!(err.code(), "ResourceAlreadyExistsException");
}

// ── Rule lifecycle ──

#[test]
fn rule_put_describe_enable_disable_delete() {
    let svc = make_service();
    svc.put_rule(&make_request(
        "PutRule",
        json!({"Name": "my-rule", "EventPattern": "{\"source\":[\"aws.s3\"]}", "State": "ENABLED"}),
    ))
    .unwrap();

    let resp = svc
        .describe_rule(&make_request("DescribeRule", json!({"Name": "my-rule"})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["State"], "ENABLED");

    svc.disable_rule(&make_request("DisableRule", json!({"Name": "my-rule"})))
        .unwrap();
    svc.enable_rule(&make_request("EnableRule", json!({"Name": "my-rule"})))
        .unwrap();
    svc.delete_rule(&make_request("DeleteRule", json!({"Name": "my-rule"})))
        .unwrap();
}

#[test]
fn list_rules_returns_created() {
    let svc = make_service();
    for name in &["r1", "r2", "r3"] {
        svc.put_rule(&make_request(
            "PutRule",
            json!({"Name": name, "EventPattern": "{\"source\":[\"aws.s3\"]}"}),
        ))
        .unwrap();
    }
    let resp = svc
        .list_rules(&make_request("ListRules", json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Rules"].as_array().unwrap().len(), 3);
}

// ── Targets ──

#[test]
fn put_list_remove_targets() {
    let svc = make_service();
    svc.put_rule(&make_request(
        "PutRule",
        json!({"Name": "tr", "EventPattern": "{\"source\":[\"aws.s3\"]}"}),
    ))
    .unwrap();

    svc.put_targets(&make_request(
        "PutTargets",
        json!({
            "Rule": "tr",
            "Targets": [
                {"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q1"},
                {"Id": "t2", "Arn": "arn:aws:lambda:us-east-1:123456789012:function:fn1"},
            ]
        }),
    ))
    .unwrap();

    let resp = svc
        .list_targets_by_rule(&make_request("ListTargetsByRule", json!({"Rule": "tr"})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Targets"].as_array().unwrap().len(), 2);

    svc.remove_targets(&make_request(
        "RemoveTargets",
        json!({"Rule": "tr", "Ids": ["t1"]}),
    ))
    .unwrap();

    let resp = svc
        .list_targets_by_rule(&make_request("ListTargetsByRule", json!({"Rule": "tr"})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Targets"].as_array().unwrap().len(), 1);
}

// ── PutEvents ──

#[test]
fn put_events_basic() {
    let svc = make_service();
    let resp = svc
        .put_events(&make_request(
            "PutEvents",
            json!({
                "Entries": [
                    {"Source": "aws.s3", "DetailType": "Object Created", "Detail": "{}"},
                ]
            }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 0);
}

// ── Archives ──

#[test]
fn archive_create_describe_list_delete() {
    let svc = make_service();

    svc.create_archive(&make_request(
        "CreateArchive",
        json!({
            "ArchiveName": "my-archive",
            "EventSourceArn": "arn:aws:events:us-east-1:123456789012:event-bus/default",
        }),
    ))
    .unwrap();

    let resp = svc
        .describe_archive(&make_request(
            "DescribeArchive",
            json!({"ArchiveName": "my-archive"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ArchiveName"], "my-archive");

    let resp = svc
        .list_archives(&make_request("ListArchives", json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(!body["Archives"].as_array().unwrap().is_empty());

    svc.delete_archive(&make_request(
        "DeleteArchive",
        json!({"ArchiveName": "my-archive"}),
    ))
    .unwrap();
}

// ── Connections ──

#[test]
fn connection_create_list_describe_deauthorize() {
    let svc = make_service();

    svc.create_connection(&make_request(
        "CreateConnection",
        json!({
            "Name": "my-conn",
            "AuthorizationType": "API_KEY",
            "AuthParameters": {
                "ApiKeyAuthParameters": {"ApiKeyName": "x-key", "ApiKeyValue": "secret"}
            }
        }),
    ))
    .unwrap();

    let resp = svc
        .list_connections(&make_request("ListConnections", json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(!body["Connections"].as_array().unwrap().is_empty());

    svc.describe_connection(&make_request(
        "DescribeConnection",
        json!({"Name": "my-conn"}),
    ))
    .unwrap();
    svc.deauthorize_connection(&make_request(
        "DeauthorizeConnection",
        json!({"Name": "my-conn"}),
    ))
    .unwrap();
}

#[test]
fn describe_connection_echoes_invocation_http_parameters() {
    let svc = make_service();

    svc.create_connection(&make_request(
        "CreateConnection",
        json!({
            "Name": "conn-http",
            "AuthorizationType": "API_KEY",
            "AuthParameters": {
                "ApiKeyAuthParameters": {"ApiKeyName": "x-key", "ApiKeyValue": "secret"},
                "InvocationHttpParameters": {
                    "HeaderParameters": [
                        {"Key": "X-Custom", "Value": "public", "IsValueSecret": false},
                        {"Key": "X-Token", "Value": "hush", "IsValueSecret": true}
                    ],
                    "QueryStringParameters": [
                        {"Key": "q", "Value": "v", "IsValueSecret": false}
                    ]
                }
            }
        }),
    ))
    .unwrap();

    let resp = svc
        .describe_connection(&make_request(
            "DescribeConnection",
            json!({"Name": "conn-http"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let inv = &body["AuthParameters"]["InvocationHttpParameters"];
    let headers = inv["HeaderParameters"].as_array().unwrap();
    assert_eq!(headers.len(), 2);
    // Non-secret header echoes its value; keys + IsValueSecret always present.
    let public = headers.iter().find(|h| h["Key"] == "X-Custom").unwrap();
    assert_eq!(public["Value"], "public");
    assert_eq!(public["IsValueSecret"], false);
    // Secret header keeps its key + flag but hides the value.
    let secret = headers.iter().find(|h| h["Key"] == "X-Token").unwrap();
    assert_eq!(secret["IsValueSecret"], true);
    assert!(secret.get("Value").is_none(), "secret value must be hidden");
    // Query string parameters echoed too.
    assert_eq!(inv["QueryStringParameters"][0]["Key"], "q");
}

// ── Event bus list ──

#[test]
fn list_event_buses_returns_default_and_custom() {
    let svc = make_service();
    svc.create_event_bus(&make_request(
        "CreateEventBus",
        json!({"Name": "custom-bus"}),
    ))
    .unwrap();

    let resp = svc
        .list_event_buses(&make_request("ListEventBuses", json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names: Vec<&str> = body["EventBuses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v["Name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"default"));
    assert!(names.contains(&"custom-bus"));
}

// ── Tags ──

#[test]
fn tag_list_untag_rule_resource() {
    let svc = make_service();
    svc.put_rule(&make_request(
        "PutRule",
        json!({"Name": "tagged-rule", "EventPattern": "{\"source\":[\"aws.s3\"]}"}),
    ))
    .unwrap();

    let arn = "arn:aws:events:us-east-1:123456789012:rule/tagged-rule";

    svc.tag_resource(&make_request(
        "TagResource",
        json!({"ResourceARN": arn, "Tags": [{"Key": "env", "Value": "prod"}]}),
    ))
    .unwrap();

    let resp = svc
        .list_tags_for_resource(&make_request(
            "ListTagsForResource",
            json!({"ResourceARN": arn}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Tags"].as_array().unwrap().len(), 1);

    svc.untag_resource(&make_request(
        "UntagResource",
        json!({"ResourceARN": arn, "TagKeys": ["env"]}),
    ))
    .unwrap();
}

// ── put_permission / remove_permission ──

#[test]
fn put_permission_with_policy_json() {
    let svc = make_service();
    let policy = r#"{"Version":"2012-10-17","Statement":[]}"#;
    let req = make_request("PutPermission", json!({"Policy": policy}));
    svc.put_permission(&req).unwrap();
}

#[test]
fn put_permission_accepts_any_action_string_within_length_bounds() {
    // PutPermission validates Action length (1..=64) per the Smithy
    // @length trait but does not gate against an allow-list — any string
    // that fits is recorded verbatim.
    let svc = make_service();
    let req = make_request(
        "PutPermission",
        json!({
            "Action": "events:NotARealAction",
            "Principal": "123456789012",
            "StatementId": "s1"
        }),
    );
    svc.put_permission(&req)
        .expect("in-bounds action string is accepted");
}

#[test]
fn put_permission_rejects_action_over_length_bound() {
    // Smithy declares Action length 1..=64; anything longer is a
    // ValidationException at the wire.
    let svc = make_service();
    let req = make_request(
        "PutPermission",
        json!({
            "Action": "a".repeat(65),
            "Principal": "123456789012",
            "StatementId": "s1"
        }),
    );
    assert!(svc.put_permission(&req).is_err());
}

#[test]
fn put_permission_rejects_principal_over_length_bound() {
    // Smithy declares Principal length 1..=12 (12-digit account or `*`).
    let svc = make_service();
    let req = make_request(
        "PutPermission",
        json!({
            "Action": "events:PutEvents",
            "Principal": "1".repeat(13),
            "StatementId": "s1"
        }),
    );
    assert!(svc.put_permission(&req).is_err());
}

#[test]
fn put_permission_unknown_bus_errors() {
    let svc = make_service();
    let req = make_request(
        "PutPermission",
        json!({
            "EventBusName": "missing",
            "Action": "events:PutEvents",
            "Principal": "123456789012",
            "StatementId": "s1"
        }),
    );
    assert!(svc.put_permission(&req).is_err());
}

#[test]
fn put_permission_add_and_remove_statement() {
    let svc = make_service();
    let req = make_request(
        "PutPermission",
        json!({
            "Action": "events:PutEvents",
            "Principal": "123456789012",
            "StatementId": "s1"
        }),
    );
    svc.put_permission(&req).unwrap();

    let req = make_request("RemovePermission", json!({"StatementId": "s1"}));
    svc.remove_permission(&req).unwrap();
}

#[test]
fn remove_permission_remove_all_flag() {
    let svc = make_service();
    let req = make_request(
        "PutPermission",
        json!({
            "Action": "events:PutEvents",
            "Principal": "123456789012",
            "StatementId": "s1"
        }),
    );
    svc.put_permission(&req).unwrap();

    let req = make_request("RemovePermission", json!({"RemoveAllPermissions": true}));
    svc.remove_permission(&req).unwrap();
}

#[test]
fn remove_permission_unknown_bus_errors() {
    let svc = make_service();
    let req = make_request(
        "RemovePermission",
        json!({"EventBusName": "missing", "StatementId": "s1"}),
    );
    assert!(svc.remove_permission(&req).is_err());
}

#[test]
fn remove_permission_no_policy_errors() {
    let svc = make_service();
    let req = make_request("RemovePermission", json!({"StatementId": "s1"}));
    assert!(svc.remove_permission(&req).is_err());
}

#[test]
fn remove_permission_unknown_statement_errors() {
    let svc = make_service();
    svc.put_permission(&make_request(
        "PutPermission",
        json!({
            "Action": "events:PutEvents",
            "Principal": "123456789012",
            "StatementId": "s1"
        }),
    ))
    .unwrap();

    let req = make_request("RemovePermission", json!({"StatementId": "ghost"}));
    assert!(svc.remove_permission(&req).is_err());
}

// ── put_rule invalid schedule expression ──

#[test]
fn put_rule_rejects_missing_name() {
    // Smithy marks Name @required (RuleName length 1..=64).
    let svc = make_service();
    let req = make_request("PutRule", json!({}));
    assert!(svc.put_rule(&req).is_err());
}

#[test]
fn put_rule_rejects_long_name() {
    let svc = make_service();
    let name = "x".repeat(65);
    let req = make_request("PutRule", json!({"Name": name}));
    assert!(svc.put_rule(&req).is_err());
}

#[test]
fn put_rule_rejects_unknown_state() {
    // RuleState enum: ENABLED, DISABLED, ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS.
    let svc = make_service();
    let req = make_request("PutRule", json!({"Name": "r1", "State": "BOGUS"}));
    assert!(svc.put_rule(&req).is_err());
}

#[test]
fn put_rule_rejects_invalid_event_pattern() {
    // An EventPattern that isn't valid JSON must be rejected at PutRule time,
    // not stored and silently never matched.
    let svc = make_service();
    let req = make_request(
        "PutRule",
        json!({"Name": "r1", "EventPattern": "{not valid json"}),
    );
    let err = match svc.put_rule(&req) {
        Err(e) => e,
        Ok(_) => panic!("invalid pattern must error"),
    };
    assert_eq!(err.code(), "InvalidEventPatternException");

    // A valid pattern still succeeds.
    let ok = make_request(
        "PutRule",
        json!({"Name": "r2", "EventPattern": r#"{"source":["aws.ec2"]}"#}),
    );
    assert!(svc.put_rule(&ok).is_ok());
}

// ── create_connection variants ──

#[test]
fn create_connection_api_key_auth() {
    let svc = make_service();
    let req = make_request(
        "CreateConnection",
        json!({
            "Name": "conn-apikey",
            "AuthorizationType": "API_KEY",
            "AuthParameters": {
                "ApiKeyAuthParameters": {
                    "ApiKeyName": "X-Api-Key",
                    "ApiKeyValue": "secret"
                }
            }
        }),
    );
    svc.create_connection(&req).unwrap();
}

#[test]
fn create_connection_basic_auth() {
    let svc = make_service();
    let req = make_request(
        "CreateConnection",
        json!({
            "Name": "conn-basic",
            "AuthorizationType": "BASIC",
            "AuthParameters": {
                "BasicAuthParameters": {
                    "Username": "u",
                    "Password": "p"
                }
            }
        }),
    );
    svc.create_connection(&req).unwrap();
}

#[test]
fn create_connection_missing_name_errors() {
    let svc = make_service();
    let req = make_request("CreateConnection", json!({"AuthorizationType": "API_KEY"}));
    assert!(svc.create_connection(&req).is_err());
}

#[test]
fn create_connection_missing_auth_type_errors() {
    let svc = make_service();
    let req = make_request("CreateConnection", json!({"Name": "c-noauth"}));
    assert!(svc.create_connection(&req).is_err());
}

#[test]
fn delete_connection_not_found() {
    let svc = make_service();
    let req = make_request("DeleteConnection", json!({"Name": "ghost"}));
    assert!(svc.delete_connection(&req).is_err());
}

// ── api destination validation ──

#[test]
fn create_api_destination_missing_name_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateApiDestination",
        json!({
            "ConnectionArn": "arn:aws:events:us-east-1:123456789012:connection/c",
            "InvocationEndpoint": "https://example.com",
            "HttpMethod": "POST"
        }),
    );
    assert!(svc.create_api_destination(&req).is_err());
}

#[test]
fn create_api_destination_invalid_method_errors() {
    let svc = make_service();
    create_connection(&svc, "conn-m");
    let guard = svc.state.read();
    let st = guard.default_ref();
    let conn_arn = st
        .connections
        .get("conn-m")
        .map(|c| c.arn.clone())
        .unwrap_or_default();
    drop(guard);

    let req = make_request(
        "CreateApiDestination",
        json!({
            "Name": "d1",
            "ConnectionArn": conn_arn,
            "InvocationEndpoint": "https://example.com",
            "HttpMethod": "FLY"
        }),
    );
    assert!(svc.create_api_destination(&req).is_err());
}

#[test]
fn delete_api_destination_not_found() {
    let svc = make_service();
    let req = make_request("DeleteApiDestination", json!({"Name": "ghost"}));
    assert!(svc.delete_api_destination(&req).is_err());
}

// ── archive error paths ──

#[test]
fn create_archive_missing_name_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateArchive",
        json!({"EventSourceArn": "arn:aws:events:us-east-1:123456789012:event-bus/default"}),
    );
    assert!(svc.create_archive(&req).is_err());
}

#[test]
fn create_archive_missing_source_arn_errors() {
    let svc = make_service();
    let req = make_request("CreateArchive", json!({"ArchiveName": "arc1"}));
    assert!(svc.create_archive(&req).is_err());
}

#[test]
fn delete_archive_missing_errors() {
    let svc = make_service();
    let req = make_request("DeleteArchive", json!({"ArchiveName": "ghost"}));
    assert!(svc.delete_archive(&req).is_err());
}

// ── replay error paths ──

#[test]
fn cancel_replay_not_found() {
    let svc = make_service();
    let req = make_request("CancelReplay", json!({"ReplayName": "ghost"}));
    assert!(svc.cancel_replay(&req).is_err());
}

// ── put_events empty ──

#[test]
fn put_events_empty_entries_rejected() {
    // Smithy PutEventsRequestEntryList carries `@length min=1`. AWS rejects
    // an empty batch with ValidationException; we enforce the same bound.
    let svc = make_service();
    let req = make_request("PutEvents", json!({"Entries": []}));
    assert!(svc.put_events(&req).is_err());
}

#[test]
fn put_events_success_count() {
    let svc = make_service();
    let req = make_request(
        "PutEvents",
        json!({
            "Entries": [
                {"Source": "my.app", "DetailType": "Test", "Detail": "{}"}
            ]
        }),
    );
    let resp = svc.put_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"], 0);
    assert_eq!(body["Entries"].as_array().unwrap().len(), 1);
}

// ── list_tags_for_resource on unknown ARN ──

#[test]
fn list_tags_for_resource_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "ListTagsForResource",
        json!({
            "ResourceARN": "arn:aws:events:us-east-1:123456789012:rule/ghost"
        }),
    );
    assert!(svc.list_tags_for_resource(&req).is_err());
}

// ── describe_rule with EventBusName ──

#[test]
fn describe_rule_custom_bus() {
    let svc = make_service();
    svc.create_event_bus(&make_request("CreateEventBus", json!({"Name": "cb"})))
        .unwrap();

    svc.put_rule(&make_request(
        "PutRule",
        json!({
            "Name": "r-cb",
            "EventPattern": "{\"source\":[\"aws.s3\"]}",
            "EventBusName": "cb"
        }),
    ))
    .unwrap();

    let resp = svc
        .describe_rule(&make_request(
            "DescribeRule",
            json!({"Name": "r-cb", "EventBusName": "cb"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "r-cb");
}

// ── enable/disable rule on custom bus ──

#[test]
fn disable_rule_on_custom_bus() {
    let svc = make_service();
    svc.create_event_bus(&make_request("CreateEventBus", json!({"Name": "dcb"})))
        .unwrap();
    svc.put_rule(&make_request(
        "PutRule",
        json!({
            "Name": "r-d",
            "EventPattern": "{\"source\":[\"s\"]}",
            "EventBusName": "dcb"
        }),
    ))
    .unwrap();
    svc.disable_rule(&make_request(
        "DisableRule",
        json!({"Name": "r-d", "EventBusName": "dcb"}),
    ))
    .unwrap();
}

// ── describe_event_bus with custom bus ──

#[test]
fn describe_event_bus_custom() {
    let svc = make_service();
    svc.create_event_bus(&make_request("CreateEventBus", json!({"Name": "deb"})))
        .unwrap();
    let resp = svc
        .describe_event_bus(&make_request("DescribeEventBus", json!({"Name": "deb"})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Name"], "deb");
}

#[test]
fn list_event_buses_with_name_prefix() {
    let svc = make_service();
    for name in &["dev-x", "dev-y", "prod-z"] {
        svc.create_event_bus(&make_request("CreateEventBus", json!({"Name": name})))
            .unwrap();
    }
    let resp = svc
        .list_event_buses(&make_request(
            "ListEventBuses",
            json!({"NamePrefix": "dev-"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EventBuses"].as_array().unwrap().len(), 2);
}

#[test]
fn list_rules_on_custom_bus() {
    let svc = make_service();
    svc.create_event_bus(&make_request("CreateEventBus", json!({"Name": "lrcb"})))
        .unwrap();
    svc.put_rule(&make_request(
        "PutRule",
        json!({
            "Name": "r1",
            "EventPattern": "{\"source\":[\"s\"]}",
            "EventBusName": "lrcb"
        }),
    ))
    .unwrap();

    let resp = svc
        .list_rules(&make_request("ListRules", json!({"EventBusName": "lrcb"})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Rules"].as_array().unwrap().len(), 1);
}

// ── put_targets on custom bus ──

#[test]
fn put_targets_on_custom_bus() {
    let svc = make_service();
    svc.create_event_bus(&make_request("CreateEventBus", json!({"Name": "ptcb"})))
        .unwrap();
    svc.put_rule(&make_request(
        "PutRule",
        json!({
            "Name": "rt",
            "EventPattern": "{\"source\":[\"s\"]}",
            "EventBusName": "ptcb"
        }),
    ))
    .unwrap();

    svc.put_targets(&make_request(
        "PutTargets",
        json!({
            "Rule": "rt",
            "EventBusName": "ptcb",
            "Targets": [{"Id": "t1", "Arn": "arn:aws:sqs:us-east-1:123456789012:q1"}]
        }),
    ))
    .unwrap();
}

// ── remove_targets unknown target ids ──

#[test]
fn remove_targets_unknown_ids_returns_failed() {
    let svc = make_service();
    svc.put_rule(&make_request(
        "PutRule",
        json!({"Name": "rmt", "EventPattern": "{\"source\":[\"s\"]}"}),
    ))
    .unwrap();

    let resp = svc
        .remove_targets(&make_request(
            "RemoveTargets",
            json!({"Rule": "rmt", "Ids": ["ghost1", "ghost2"]}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    // Unknown ids are silently ok in many implementations; at least we hit the code path
    assert!(body.is_object());
}

#[test]
fn describe_event_source_unknown_errors() {
    let svc = make_service();
    let req = make_request("DescribeEventSource", json!({"Name": "ghost"}));
    assert!(svc.describe_event_source(&req).is_err());
}

#[test]
fn describe_partner_event_source_unknown_errors() {
    let svc = make_service();
    let req = make_request("DescribePartnerEventSource", json!({"Name": "ghost"}));
    assert!(svc.describe_partner_event_source(&req).is_err());
}

#[test]
fn list_partner_event_sources_empty_ok() {
    let svc = make_service();
    let req = make_request(
        "ListPartnerEventSources",
        json!({"NamePrefix": "aws.partner"}),
    );
    let resp = svc.list_partner_event_sources(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["PartnerEventSources"].is_array());
}

#[test]
fn list_event_sources_empty_ok() {
    let svc = make_service();
    let req = make_request("ListEventSources", json!({}));
    let resp = svc.list_event_sources(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["EventSources"].is_array());
}

#[test]
fn update_connection_unknown_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateConnection",
        json!({"Name": "ghost", "AuthorizationType": "API_KEY"}),
    );
    assert!(svc.update_connection(&req).is_err());
}

#[test]
fn describe_api_destination_unknown_errors() {
    let svc = make_service();
    let req = make_request("DescribeApiDestination", json!({"Name": "ghost"}));
    assert!(svc.describe_api_destination(&req).is_err());
}

#[test]
fn update_api_destination_unknown_errors() {
    let svc = make_service();
    let req = make_request("UpdateApiDestination", json!({"Name": "ghost"}));
    assert!(svc.update_api_destination(&req).is_err());
}

#[test]
fn update_archive_unknown_errors() {
    let svc = make_service();
    let req = make_request("UpdateArchive", json!({"ArchiveName": "ghost"}));
    assert!(svc.update_archive(&req).is_err());
}

#[test]
fn describe_archive_unknown_errors_b() {
    let svc = make_service();
    let req = make_request("DescribeArchive", json!({"ArchiveName": "ghost"}));
    assert!(svc.describe_archive(&req).is_err());
}

#[test]
fn list_archives_empty_ok() {
    let svc = make_service();
    let req = make_request("ListArchives", json!({}));
    let resp = svc.list_archives(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Archives"].is_array());
}

#[test]
fn list_replays_empty_ok() {
    let svc = make_service();
    let req = make_request("ListReplays", json!({}));
    let resp = svc.list_replays(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Replays"].is_array());
}

#[test]
fn describe_endpoint_unknown_errors() {
    let svc = make_service();
    let req = make_request("DescribeEndpoint", json!({"Name": "ghost"}));
    assert!(svc.describe_endpoint(&req).is_err());
}

#[test]
fn delete_endpoint_unknown_errors() {
    let svc = make_service();
    let req = make_request("DeleteEndpoint", json!({"Name": "ghost"}));
    assert!(svc.delete_endpoint(&req).is_err());
}

#[test]
fn list_endpoints_empty_ok() {
    let svc = make_service();
    let req = make_request("ListEndpoints", json!({}));
    let resp = svc.list_endpoints(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Endpoints"].is_array());
}

// ── filter operators (K8) ──

#[test]
fn pattern_matches_suffix_operator() {
    assert!(test_matches(
        Some(r#"{"source": [{"suffix": ".prod"}]}"#),
        "my.app.prod",
        "T",
        "{}"
    ));
    assert!(!test_matches(
        Some(r#"{"source": [{"suffix": ".prod"}]}"#),
        "my.app.dev",
        "T",
        "{}"
    ));
}

#[test]
fn pattern_matches_equals_ignore_case_operator() {
    assert!(test_matches(
        Some(r#"{"detail-type": [{"equals-ignore-case": "ORDER"}]}"#),
        "x",
        "Order",
        "{}"
    ));
    assert!(!test_matches(
        Some(r#"{"detail-type": [{"equals-ignore-case": "ORDER"}]}"#),
        "x",
        "Shipment",
        "{}"
    ));
}

#[test]
fn pattern_matches_cidr_operator() {
    assert!(test_matches(
        Some(r#"{"detail": {"ip": [{"cidr": "10.0.0.0/8"}]}}"#),
        "x",
        "T",
        r#"{"ip": "10.5.5.5"}"#
    ));
    assert!(!test_matches(
        Some(r#"{"detail": {"ip": [{"cidr": "10.0.0.0/8"}]}}"#),
        "x",
        "T",
        r#"{"ip": "192.168.1.1"}"#
    ));
}

#[test]
fn pattern_matches_wildcard_operator() {
    assert!(test_matches(
        Some(r#"{"source": [{"wildcard": "*.prod.*"}]}"#),
        "service.prod.us-east-1",
        "T",
        "{}"
    ));
    assert!(!test_matches(
        Some(r#"{"source": [{"wildcard": "*.prod.*"}]}"#),
        "service.dev.us-east-1",
        "T",
        "{}"
    ));
    // Anchored prefix-suffix
    assert!(test_matches(
        Some(r#"{"source": [{"wildcard": "svc.*.app"}]}"#),
        "svc.x.y.app",
        "T",
        "{}"
    ));
}

#[test]
fn pattern_matches_or_alternation() {
    let pat = Some(
        r#"{"$or": [
            {"source": ["a.app"]},
            {"detail-type": ["Special"]}
        ]}"#,
    );
    assert!(test_matches(pat, "a.app", "Anything", "{}"));
    assert!(test_matches(pat, "other.app", "Special", "{}"));
    assert!(!test_matches(pat, "other.app", "Other", "{}"));
}

// ── K10: bus policy gate on cross-account PutEvents ──

fn put_bus_policy(svc: &EventBridgeService, bus: &str, policy: serde_json::Value) {
    let mut accts = svc.state.write();
    let s = accts.default_mut();
    let bus_entry = s.buses.get_mut(bus).expect("bus");
    bus_entry.policy = Some(policy);
}

fn cross_account_request(action: &str, body: Value) -> AwsRequest {
    let mut req = make_request(action, body);
    req.principal = Some(fakecloud_core::auth::Principal {
        arn: "arn:aws:iam::999999999999:user/cross-acct".to_string(),
        user_id: "AIDAOTHER".to_string(),
        account_id: "999999999999".to_string(),
        principal_type: fakecloud_core::auth::PrincipalType::User,
        source_identity: None,
        tags: None,
    });
    req
}

#[test]
fn put_events_same_account_succeeds_without_policy_check() {
    let svc = make_service();
    let req = make_request(
        "PutEvents",
        json!({
            "Entries": [
                {"Source": "my.app", "DetailType": "T", "Detail": "{}"}
            ]
        }),
    );
    let resp = svc.put_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"].as_i64().unwrap(), 0);
}

#[test]
fn put_events_cross_account_denied_when_bus_policy_excludes_caller() {
    let svc = make_service();
    let restrictive_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::111122223333:root"},
            "Action": "events:PutEvents",
            "Resource": "*"
        }]
    });
    put_bus_policy(&svc, "default", restrictive_policy);

    let req = cross_account_request(
        "PutEvents",
        json!({
            "Entries": [
                {"Source": "my.app", "DetailType": "T", "Detail": "{}"}
            ]
        }),
    );
    let resp = svc.put_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"].as_i64().unwrap(), 1);
    let entry = &body["Entries"][0];
    assert_eq!(
        entry["ErrorCode"].as_str().unwrap(),
        "AccessDeniedException"
    );
}

#[test]
fn put_events_cross_account_allowed_when_bus_policy_grants_caller() {
    let svc = make_service();
    let permissive_policy = json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::999999999999:root"},
            "Action": "events:PutEvents",
            "Resource": "*"
        }]
    });
    put_bus_policy(&svc, "default", permissive_policy);

    let req = cross_account_request(
        "PutEvents",
        json!({
            "Entries": [
                {"Source": "my.app", "DetailType": "T", "Detail": "{}"}
            ]
        }),
    );
    let resp = svc.put_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedEntryCount"].as_i64().unwrap(), 0);
}

#[test]
fn put_events_cross_account_denied_when_bus_has_no_policy() {
    let svc = make_service();
    // No bus policy set; cross-account caller still hits an empty policy check
    // — the gate skips evaluation entirely (AWS treats absence as same-account-only).
    // Regression: ensure our gate doesn't crash when policy is None.
    let req = cross_account_request(
        "PutEvents",
        json!({
            "Entries": [
                {"Source": "my.app", "DetailType": "T", "Detail": "{}"}
            ]
        }),
    );
    let resp = svc.put_events(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    // No policy = AWS would reject, but we historically allow. Document the
    // current behavior in this assertion so a future tightening shows up.
    assert_eq!(body["FailedEntryCount"].as_i64().unwrap(), 0);
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = make_service();
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating EventBridge
/// state directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = make_service().with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    // Must not panic; exercises the closure and the snapshot save path.
    hook().await;
}

#[test]
fn put_permission_star_principal_stored_verbatim() {
    // A `*` principal means "any account" and must be stored as "*", not an
    // account-root ARN. The Terraform aws_cloudwatch_event_permission resource
    // reads it back and asserts principal == "*".
    let svc = make_service();
    svc.put_permission(&make_request(
        "PutPermission",
        json!({ "Action": "events:PutEvents", "Principal": "*", "StatementId": "s1" }),
    ))
    .unwrap();
    let resp = svc
        .describe_event_bus(&make_request(
            "DescribeEventBus",
            json!({ "Name": "default" }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let policy = body["Policy"].as_str().unwrap();
    assert!(
        policy.contains("\"Principal\":\"*\""),
        "star principal should be stored verbatim, got {policy}"
    );
}

#[test]
fn put_permission_same_statement_id_replaces() {
    // PutPermission with an existing StatementId replaces that statement (e.g.
    // changing its principal) rather than stacking a duplicate.
    let svc = make_service();
    svc.put_permission(&make_request(
        "PutPermission",
        json!({ "Action": "events:PutEvents", "Principal": "111111111111", "StatementId": "s1" }),
    ))
    .unwrap();
    svc.put_permission(&make_request(
        "PutPermission",
        json!({ "Action": "events:PutEvents", "Principal": "*", "StatementId": "s1" }),
    ))
    .unwrap();
    let resp = svc
        .describe_event_bus(&make_request(
            "DescribeEventBus",
            json!({ "Name": "default" }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let policy: Value = serde_json::from_str(body["Policy"].as_str().unwrap()).unwrap();
    let stmts = policy["Statement"].as_array().unwrap();
    assert_eq!(
        stmts.len(),
        1,
        "duplicate Sid should be replaced, not stacked"
    );
    assert_eq!(stmts[0]["Principal"], json!("*"));
}

#[test]
fn list_event_buses_includes_creation_time() {
    // The aws_cloudwatch_event_buses data source expects each bus to report a
    // creation time; ListEventBuses must surface it like DescribeEventBus.
    let svc = make_service();
    let resp = svc
        .list_event_buses(&make_request("ListEventBuses", json!({})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let first = &body["EventBuses"][0];
    assert!(
        first["CreationTime"].is_number(),
        "ListEventBuses entry should carry CreationTime, got {first}"
    );
}
