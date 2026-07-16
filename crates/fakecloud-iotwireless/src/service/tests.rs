use super::*;
use fakecloud_core::multi_account::MultiAccountState;
use fakecloud_core::service::AwsRequest;
use parking_lot::RwLock;
use serde_json::json;

fn svc() -> IotWirelessService {
    let state: SharedIotWirelessState = Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    )));
    IotWirelessService::new(state)
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
        service: "iotwireless".into(),
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
    svc: &IotWirelessService,
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
    let (m, _) = match_route(&Method::POST, "/destinations").unwrap();
    assert_eq!(m.op, "CreateDestination");

    let (m, labels) = match_route(&Method::GET, "/destinations/dest-1").unwrap();
    assert_eq!(m.op, "GetDestination");
    assert_eq!(labels.get("Name").unwrap(), "dest-1");

    let (m, _) = match_route(&Method::GET, "/destinations").unwrap();
    assert_eq!(m.op, "ListDestinations");

    // Trailing fixed segment (sub-resource verb) wins over a bare label route.
    let (m, labels) = match_route(&Method::DELETE, "/multicast-groups/g1/session").unwrap();
    assert_eq!(m.op, "CancelMulticastGroupSession");
    assert_eq!(labels.get("Id").unwrap(), "g1");
}

#[test]
fn unknown_route_is_not_found() {
    let err = expect_err(run(&svc(), "GET", "/nope/nope", &[], Value::Null));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

// ---------- destination lifecycle (name-keyed) ----------

#[test]
fn destination_create_get_list_update_delete() {
    let s = svc();
    let created = body_of(
        &run(
            &s,
            "POST",
            "/destinations",
            &[],
            json!({"Name": "dest-1", "ExpressionType": "RuleName",
                   "Expression": "rule", "RoleArn": "arn:aws:iam::000000000000:role/x",
                   "Description": "first"}),
        )
        .unwrap(),
    );
    assert_eq!(created["Name"], "dest-1");
    assert!(created["Arn"]
        .as_str()
        .unwrap()
        .contains(":Destination/dest-1"));

    let got = body_of(&run(&s, "GET", "/destinations/dest-1", &[], Value::Null).unwrap());
    assert_eq!(got["Name"], "dest-1");
    assert_eq!(got["Expression"], "rule");
    assert_eq!(got["Description"], "first");

    let listed = body_of(&run(&s, "GET", "/destinations", &[], Value::Null).unwrap());
    assert_eq!(listed["DestinationList"].as_array().unwrap().len(), 1);
    assert_eq!(listed["DestinationList"][0]["Name"], "dest-1");

    // Update merges (round-trips the Description through GET).
    run(
        &s,
        "PATCH",
        "/destinations/dest-1",
        &[],
        json!({"Description": "updated"}),
    )
    .unwrap();
    let got = body_of(&run(&s, "GET", "/destinations/dest-1", &[], Value::Null).unwrap());
    assert_eq!(got["Description"], "updated");

    run(&s, "DELETE", "/destinations/dest-1", &[], Value::Null).unwrap();
    let err = expect_err(run(&s, "GET", "/destinations/dest-1", &[], Value::Null));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

// ---------- minted-id lifecycle ----------

#[test]
fn device_profile_mints_id_and_round_trips() {
    let s = svc();
    let created = body_of(
        &run(
            &s,
            "POST",
            "/device-profiles",
            &[],
            json!({"Name": "profile-1"}),
        )
        .unwrap(),
    );
    let id = created["Id"].as_str().unwrap().to_string();
    assert!(!id.is_empty());
    assert!(created["Arn"].as_str().unwrap().contains(":DeviceProfile/"));

    let got = body_of(
        &run(
            &s,
            "GET",
            &format!("/device-profiles/{id}"),
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(got["Id"], id);
    assert_eq!(got["Name"], "profile-1");

    let listed = body_of(&run(&s, "GET", "/device-profiles", &[], Value::Null).unwrap());
    assert_eq!(listed["DeviceProfileList"].as_array().unwrap().len(), 1);
}

#[test]
fn fuota_task_created_at_is_numeric_timestamp() {
    let s = svc();
    let created = body_of(
        &run(
            &s,
            "POST",
            "/fuota-tasks",
            &[],
            json!({"FirmwareUpdateImage": "img", "FirmwareUpdateRole": "arn:role"}),
        )
        .unwrap(),
    );
    let id = created["Id"].as_str().unwrap().to_string();
    let got = body_of(&run(&s, "GET", &format!("/fuota-tasks/{id}"), &[], Value::Null).unwrap());
    // restJson1 wire-encodes timestamps as epoch-seconds JSON numbers, never
    // RFC3339 strings (which the aws-sdk timestamp deserializer rejects).
    let created_at = &got["CreatedAt"];
    assert!(
        created_at.is_number(),
        "CreatedAt must be a numeric epoch-seconds timestamp, got {created_at:?}"
    );
    assert!(created_at.as_f64().unwrap() > 1_600_000_000.0);
}

// ---------- tags ----------

#[test]
fn tags_round_trip() {
    let s = svc();
    let arn = "arn:aws:iotwireless:us-east-1:000000000000:WirelessDevice/abc";
    run(
        &s,
        "POST",
        &format!("/tags?resourceArn={arn}"),
        &[],
        json!({"Tags": [{"Key": "env", "Value": "prod"}]}),
    )
    .unwrap();
    let listed = body_of(
        &run(
            &s,
            "GET",
            &format!("/tags?resourceArn={arn}"),
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(listed["Tags"][0]["Key"], "env");
    assert_eq!(listed["Tags"][0]["Value"], "prod");

    run(
        &s,
        "DELETE",
        &format!("/tags?resourceArn={arn}&tagKeys=env"),
        &[],
        Value::Null,
    )
    .unwrap();
    let listed = body_of(
        &run(
            &s,
            "GET",
            &format!("/tags?resourceArn={arn}"),
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(listed["Tags"].as_array().unwrap().len(), 0);
}

// ---------- position configuration (Put/Get pair) ----------

#[test]
fn put_get_position_configuration_round_trips() {
    let s = svc();
    run(
        &s,
        "PUT",
        "/position-configurations/dev1?resourceType=WirelessDevice",
        &[],
        json!({"Destination": "dest-x"}),
    )
    .unwrap();
    let got = body_of(
        &run(
            &s,
            "GET",
            "/position-configurations/dev1?resourceType=WirelessDevice",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(got["Destination"], "dest-x");
}

// ---------- resource position raw payload ----------

#[test]
fn update_get_resource_position_round_trips_blob() {
    let s = svc();
    // GeoJsonPayload is an @httpPayload blob; the request/response body IS the
    // raw payload, so send a raw (non-JSON-object) string.
    let req = mk_req(
        "PATCH",
        "/resource-positions/dev1?resourceType=WirelessDevice",
        &[],
        Value::Null,
    );
    // Manually attach a raw body.
    let mut req = req;
    req.body = bytes::Bytes::from_static(b"dGVzdA==");
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(s.handle(req)).unwrap();

    let got = run(
        &s,
        "GET",
        "/resource-positions/dev1?resourceType=WirelessDevice",
        &[],
        Value::Null,
    )
    .unwrap();
    assert_eq!(got.body.expect_bytes(), b"dGVzdA==");
}

#[test]
fn get_resource_position_404s_when_never_set() {
    let err = expect_err(run(
        &svc(),
        "GET",
        "/resource-positions/dev-x?resourceType=WirelessDevice",
        &[],
        Value::Null,
    ));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

// ---------- per-resource log level (finding 1) ----------

#[test]
fn put_get_reset_resource_log_level_round_trips() {
    let s = svc();
    // Never-set -> 404.
    let err = expect_err(run(
        &s,
        "GET",
        "/log-levels/dev1?resourceType=WirelessDevice",
        &[],
        Value::Null,
    ));
    assert!(is_code(&err, "ResourceNotFoundException"));

    run(
        &s,
        "PUT",
        "/log-levels/dev1?resourceType=WirelessDevice",
        &[],
        json!({"LogLevel": "ERROR"}),
    )
    .unwrap();
    let got = body_of(
        &run(
            &s,
            "GET",
            "/log-levels/dev1?resourceType=WirelessDevice",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(got["LogLevel"], "ERROR");

    run(
        &s,
        "DELETE",
        "/log-levels/dev1?resourceType=WirelessDevice",
        &[],
        Value::Null,
    )
    .unwrap();
    let err = expect_err(run(
        &s,
        "GET",
        "/log-levels/dev1?resourceType=WirelessDevice",
        &[],
        Value::Null,
    ));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

// ---------- singleton configurations (finding 2) ----------

#[test]
fn metric_configuration_update_get_round_trips() {
    let s = svc();
    // Default before any update.
    let got = body_of(&run(&s, "GET", "/metric-configuration", &[], Value::Null).unwrap());
    assert_eq!(got["SummaryMetric"]["Status"], "Disabled");

    run(
        &s,
        "PUT",
        "/metric-configuration",
        &[],
        json!({"SummaryMetric": {"Status": "Enabled"}}),
    )
    .unwrap();
    let got = body_of(&run(&s, "GET", "/metric-configuration", &[], Value::Null).unwrap());
    assert_eq!(got["SummaryMetric"]["Status"], "Enabled");
}

#[test]
fn log_levels_by_resource_types_update_get_round_trips() {
    let s = svc();
    let got = body_of(&run(&s, "GET", "/log-levels", &[], Value::Null).unwrap());
    assert_eq!(got["DefaultLogLevel"], "INFO");

    run(
        &s,
        "POST",
        "/log-levels",
        &[],
        json!({"DefaultLogLevel": "ERROR"}),
    )
    .unwrap();
    let got = body_of(&run(&s, "GET", "/log-levels", &[], Value::Null).unwrap());
    assert_eq!(got["DefaultLogLevel"], "ERROR");
}

// ---------- Action output identifiers (finding 3) ----------

#[test]
fn send_data_to_wireless_device_returns_message_id() {
    let out = body_of(
        &run(
            &svc(),
            "POST",
            "/wireless-devices/dev1/data",
            &[],
            json!({"TransmitMode": 1, "PayloadData": "aGk="}),
        )
        .unwrap(),
    );
    assert!(out["MessageId"].as_str().is_some_and(|m| !m.is_empty()));
}

#[test]
fn import_task_start_get_delete_round_trips() {
    let s = svc();
    let started = body_of(
        &run(
            &s,
            "POST",
            "/wireless_device_import_task",
            &[],
            json!({"DestinationName": "dest-x", "Sidewalk": {"DeviceCreationFile": "s3://b/f"}}),
        )
        .unwrap(),
    );
    let id = started["Id"].as_str().unwrap().to_string();
    assert!(!id.is_empty());
    assert!(started["Arn"].as_str().unwrap().contains(":ImportTask/"));

    let got = body_of(
        &run(
            &s,
            "GET",
            &format!("/wireless_device_import_task/{id}"),
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(got["Id"], id);
    assert_eq!(got["Status"], "INITIALIZING");
    assert_eq!(got["DestinationName"], "dest-x");

    run(
        &s,
        "DELETE",
        &format!("/wireless_device_import_task/{id}"),
        &[],
        Value::Null,
    )
    .unwrap();
    let err = expect_err(run(
        &s,
        "GET",
        &format!("/wireless_device_import_task/{id}"),
        &[],
        Value::Null,
    ));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

#[test]
fn wireless_gateway_task_create_get_delete_round_trips() {
    let s = svc();
    let created = body_of(
        &run(
            &s,
            "POST",
            "/wireless-gateways/gw1/tasks",
            &[],
            json!({"WirelessGatewayTaskDefinitionId": "def-1"}),
        )
        .unwrap(),
    );
    assert_eq!(created["WirelessGatewayTaskDefinitionId"], "def-1");
    assert_eq!(created["Status"], "QUEUED");

    let got = body_of(&run(&s, "GET", "/wireless-gateways/gw1/tasks", &[], Value::Null).unwrap());
    assert_eq!(got["WirelessGatewayTaskDefinitionId"], "def-1");
    assert_eq!(got["WirelessGatewayId"], "gw1");

    run(
        &s,
        "DELETE",
        "/wireless-gateways/gw1/tasks",
        &[],
        Value::Null,
    )
    .unwrap();
    let err = expect_err(run(
        &s,
        "GET",
        "/wireless-gateways/gw1/tasks",
        &[],
        Value::Null,
    ));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

#[test]
fn get_service_endpoint_returns_fixed_members() {
    let out = body_of(
        &run(
            &svc(),
            "GET",
            "/service-endpoint?serviceType=CUPS",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(out["ServiceType"], "CUPS");
    assert!(out["ServiceEndpoint"].as_str().unwrap().contains("cups"));
    assert!(out["ServerTrust"]
        .as_str()
        .unwrap()
        .contains("BEGIN CERTIFICATE"));
}

#[test]
fn get_position_estimate_returns_feature_collection() {
    let resp = run(&svc(), "POST", "/position-estimate", &[], json!({})).unwrap();
    let payload: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(payload["type"], "FeatureCollection");
    assert!(payload["features"].as_array().unwrap()[0]["geometry"]["coordinates"].is_array());
}

// ---------- association edges (finding 4) ----------

#[test]
fn associate_multicast_group_with_fuota_task_then_list() {
    let s = svc();
    run(
        &s,
        "PUT",
        "/fuota-tasks/task1/multicast-group",
        &[],
        json!({"MulticastGroupId": "mc-1"}),
    )
    .unwrap();
    run(
        &s,
        "PUT",
        "/fuota-tasks/task1/multicast-group",
        &[],
        json!({"MulticastGroupId": "mc-2"}),
    )
    .unwrap();
    let listed = body_of(
        &run(
            &s,
            "GET",
            "/fuota-tasks/task1/multicast-groups",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    let ids: Vec<&str> = listed["MulticastGroupList"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["Id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["mc-1", "mc-2"]);

    // Disassociate removes the edge.
    run(
        &s,
        "DELETE",
        "/fuota-tasks/task1/multicast-groups/mc-1",
        &[],
        Value::Null,
    )
    .unwrap();
    let listed = body_of(
        &run(
            &s,
            "GET",
            "/fuota-tasks/task1/multicast-groups",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    let ids: Vec<&str> = listed["MulticastGroupList"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["Id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["mc-2"]);
}

// ---------- validation ----------

#[test]
fn missing_required_body_member_is_rejected() {
    // CreateDestination requires Name/ExpressionType/Expression/RoleArn.
    let err = expect_err(run(&svc(), "POST", "/destinations", &[], json!({})));
    assert!(matches!(err, AwsServiceError::AwsError { status, .. } if status.is_client_error()));
}

#[test]
fn missing_required_query_is_rejected() {
    // GetPartnerAccount requires the PartnerType query parameter.
    let err = expect_err(run(
        &svc(),
        "GET",
        "/partner-accounts/acct1",
        &[],
        Value::Null,
    ));
    assert!(matches!(err, AwsServiceError::AwsError { status, .. } if status.is_client_error()));
}

#[test]
fn placeholder_label_is_rejected() {
    let err = expect_err(run(&svc(), "GET", "/destinations/{Name}", &[], Value::Null));
    assert!(matches!(err, AwsServiceError::AwsError { status, .. } if status.is_client_error()));
}

// ---------- pagination ----------

#[test]
fn list_paginates_with_round_tripping_token() {
    let s = svc();
    for i in 0..5 {
        run(
            &s,
            "POST",
            "/device-profiles",
            &[],
            json!({"Name": format!("p{i}")}),
        )
        .unwrap();
    }
    let page1 =
        body_of(&run(&s, "GET", "/device-profiles?maxResults=2", &[], Value::Null).unwrap());
    assert_eq!(page1["DeviceProfileList"].as_array().unwrap().len(), 2);
    let token = page1["NextToken"].as_str().unwrap();
    let page2 = body_of(
        &run(
            &s,
            "GET",
            &format!("/device-profiles?maxResults=2&NextToken={token}"),
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(page2["DeviceProfileList"].as_array().unwrap().len(), 2);
}

// ---------- multicast session / fuota / bulk / low-frequency mutators ----------

#[test]
fn multicast_group_session_start_get_cancel_round_trips() {
    let s = svc();
    // No session yet -> empty body.
    let empty = body_of(
        &run(
            &s,
            "GET",
            "/multicast-groups/mc-1/session",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert!(empty.get("LoRaWAN").is_none());

    // Start persists the LoRaWAN session.
    run(
        &s,
        "PUT",
        "/multicast-groups/mc-1/session",
        &[],
        json!({"LoRaWAN": {"SessionStartTime": 1_752_324_947.0, "SessionTimeout": 60}}),
    )
    .unwrap();
    let got = body_of(
        &run(
            &s,
            "GET",
            "/multicast-groups/mc-1/session",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert_eq!(got["LoRaWAN"]["SessionTimeout"], 60);

    // Cancel clears it.
    run(
        &s,
        "DELETE",
        "/multicast-groups/mc-1/session",
        &[],
        Value::Null,
    )
    .unwrap();
    let cleared = body_of(
        &run(
            &s,
            "GET",
            "/multicast-groups/mc-1/session",
            &[],
            Value::Null,
        )
        .unwrap(),
    );
    assert!(cleared.get("LoRaWAN").is_none());
}

#[test]
fn start_fuota_task_transitions_status() {
    let s = svc();
    let created = body_of(
        &run(
            &s,
            "POST",
            "/fuota-tasks",
            &[],
            json!({"FirmwareUpdateImage": "img", "FirmwareUpdateRole": "arn:role"}),
        )
        .unwrap(),
    );
    let id = created["Id"].as_str().unwrap().to_string();

    run(
        &s,
        "PUT",
        &format!("/fuota-tasks/{id}"),
        &[],
        json!({"LoRaWAN": {}, "DestinationName": "dest"}),
    )
    .unwrap();
    let got = body_of(&run(&s, "GET", &format!("/fuota-tasks/{id}"), &[], Value::Null).unwrap());
    assert_eq!(got["Status"], "In_FuotaSession");
}

#[test]
fn bulk_associate_wireless_devices_with_multicast_group_persists_edges() {
    let s = svc();
    // Register two wireless devices.
    let d1 = body_of(
        &run(
            &s,
            "POST",
            "/wireless-devices",
            &[],
            json!({"Type": "Sidewalk", "DestinationName": "d"}),
        )
        .unwrap(),
    );
    let id1 = d1["Id"].as_str().unwrap().to_string();
    let d2 = body_of(
        &run(
            &s,
            "POST",
            "/wireless-devices",
            &[],
            json!({"Type": "Sidewalk", "DestinationName": "d"}),
        )
        .unwrap(),
    );
    let id2 = d2["Id"].as_str().unwrap().to_string();

    run(
        &s,
        "PATCH",
        "/multicast-groups/mc-9/bulk",
        &[],
        json!({"QueryString": "thingName:*"}),
    )
    .unwrap();
    {
        let g = s.state.read();
        let members = g
            .get("000000000000")
            .unwrap()
            .list_relation("multicast-devices:mc-9");
        assert!(members.contains(&id1) && members.contains(&id2));
    }

    run(
        &s,
        "POST",
        "/multicast-groups/mc-9/bulk",
        &[],
        json!({"QueryString": "thingName:*"}),
    )
    .unwrap();
    {
        let g = s.state.read();
        let members = g
            .get("000000000000")
            .unwrap()
            .list_relation("multicast-devices:mc-9");
        assert!(members.is_empty());
    }
}

#[test]
fn reset_all_resource_log_levels_clears_store() {
    let s = svc();
    run(
        &s,
        "PUT",
        "/log-levels/res-1?resourceType=WirelessDevice",
        &[],
        json!({"LogLevel": "INFO"}),
    )
    .unwrap();
    // Present before reset.
    assert!(run(
        &s,
        "GET",
        "/log-levels/res-1?resourceType=WirelessDevice",
        &[],
        Value::Null
    )
    .is_ok());
    run(&s, "DELETE", "/log-levels", &[], Value::Null).unwrap();
    // Gone after reset (GetResourceLogLevel 404s).
    let err = expect_err(run(
        &s,
        "GET",
        "/log-levels/res-1?resourceType=WirelessDevice",
        &[],
        Value::Null,
    ));
    assert!(is_code(&err, "ResourceNotFoundException"));
}

#[test]
fn deregister_wireless_device_persists_state() {
    let s = svc();
    let d = body_of(
        &run(
            &s,
            "POST",
            "/wireless-devices",
            &[],
            json!({"Type": "Sidewalk", "DestinationName": "d"}),
        )
        .unwrap(),
    );
    let id = d["Id"].as_str().unwrap().to_string();
    run(
        &s,
        "PATCH",
        &format!("/wireless-devices/{id}/deregister?WirelessDeviceType=Sidewalk"),
        &[],
        Value::Null,
    )
    .unwrap();
    let g = s.state.read();
    let rec = g
        .get("000000000000")
        .unwrap()
        .get_resource("wireless-devices", &id)
        .cloned()
        .unwrap();
    assert_eq!(rec["DeviceRegistrationState"], "Deregistered");
}

#[test]
fn read_only_action_classifier_separates_reads_from_mutators() {
    let find = |op: &str| crate::generated::OPS.iter().find(|m| m.op == op).unwrap();
    // GET-shaped actions are accepted as empty no-ops when unclaimed.
    assert!(super::is_read_only_action(find(
        "GetWirelessGatewayFirmwareInformation"
    )));
    assert!(super::is_read_only_action(find("GetServiceEndpoint")));
    // Mutating actions must NOT be classified read-only (they fail loud if they
    // ever reach the fall-through).
    assert!(!super::is_read_only_action(find(
        "StartMulticastGroupSession"
    )));
    assert!(!super::is_read_only_action(find(
        "DeregisterWirelessDevice"
    )));
    assert!(!super::is_read_only_action(find(
        "AssociateWirelessDeviceWithThing"
    )));
}

// ---------- every operation routes to a handler ----------

#[test]
fn every_action_is_supported() {
    assert_eq!(IOTWIRELESS_ACTIONS.len(), 112);
    let s = svc();
    assert_eq!(s.supported_actions().len(), 112);
}

// ---------- in-process conformance proxy ----------
//
// The live-server conformance probe cannot run in the local sandbox, so this
// test reproduces the probe's core pass criteria in-process: for every one of
// the 112 operations, a model-valid request must NOT crash (HTTP 500) and must
// return either a 2xx response or a 4xx whose error code is declared in the
// operation's Smithy `errors` list.

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
            Src::Label => {}
            Src::Query => query.push((rule.wire.to_string(), string_val())),
            Src::Header => headers.push((rule.wire.to_string(), string_val())),
            Src::Body => {
                let v = match rule.kind {
                    K::Str | K::Blob => Value::String(string_val()),
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
                if !resp.status.is_success() {
                    failures.push(format!("{}: unexpected status {}", meta.op, resp.status));
                }
            }
            Err(AwsServiceError::AwsError { status, code, .. }) => {
                let s = status.as_u16();
                if s == 500 {
                    failures.push(format!("{}: HTTP 500 crash ({code})", meta.op));
                } else if (400..500).contains(&s) {
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
    let s = svc();
    let mut failures: Vec<String> = Vec::new();
    for meta in OPS {
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
