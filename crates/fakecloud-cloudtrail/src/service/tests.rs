use super::*;
use bytes::Bytes;
use fakecloud_core::multi_account::MultiAccountState;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;

fn svc() -> CloudTrailService {
    CloudTrailService::new(Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    ))))
}

fn req(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "cloudtrail".into(),
        action: action.into(),
        region: "us-east-1".into(),
        account_id: "000000000000".into(),
        request_id: "req".into(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: Mutex::new(None),
        path_segments: vec![],
        raw_path: String::new(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn call(s: &CloudTrailService, action: &str, body: Value) -> Value {
    let resp = dispatch(s, &req(action, body)).expect("op ok");
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn err_code(s: &CloudTrailService, action: &str, body: Value) -> String {
    let e = dispatch(s, &req(action, body))
        .err()
        .expect("expected error");
    e.code().to_string()
}

fn new_trail(s: &CloudTrailService, name: &str) -> Value {
    call(
        s,
        "CreateTrail",
        json!({ "Name": name, "S3BucketName": "my-bucket" }),
    )
}

fn new_eds(s: &CloudTrailService, name: &str) -> String {
    call(s, "CreateEventDataStore", json!({ "Name": name }))["EventDataStoreArn"]
        .as_str()
        .unwrap()
        .to_string()
}

#[test]
fn trail_crud_round_trip() {
    let s = svc();
    let created = new_trail(&s, "t1");
    assert_eq!(created["Name"], json!("t1"));
    assert!(created["TrailARN"].as_str().unwrap().ends_with(":trail/t1"));
    assert_eq!(created["S3BucketName"], json!("my-bucket"));

    let got = call(&s, "GetTrail", json!({ "Name": "t1" }));
    assert_eq!(got["Trail"]["Name"], json!("t1"));
    assert_eq!(got["Trail"]["HomeRegion"], json!("us-east-1"));

    // Lookup by ARN works too.
    let arn = created["TrailARN"].as_str().unwrap();
    let by_arn = call(&s, "GetTrail", json!({ "Name": arn }));
    assert_eq!(by_arn["Trail"]["Name"], json!("t1"));

    let desc = call(&s, "DescribeTrails", json!({}));
    assert_eq!(desc["trailList"].as_array().unwrap().len(), 1);

    let list = call(&s, "ListTrails", json!({}));
    assert_eq!(list["Trails"].as_array().unwrap().len(), 1);

    // Update persists.
    let updated = call(
        &s,
        "UpdateTrail",
        json!({ "Name": "t1", "S3BucketName": "other-bucket", "IsMultiRegionTrail": true }),
    );
    assert_eq!(updated["S3BucketName"], json!("other-bucket"));
    assert_eq!(updated["IsMultiRegionTrail"], json!(true));

    // Delete removes.
    call(&s, "DeleteTrail", json!({ "Name": "t1" }));
    assert_eq!(
        err_code(&s, "GetTrail", json!({ "Name": "t1" })),
        "TrailNotFoundException"
    );
}

#[test]
fn duplicate_trail_rejected() {
    let s = svc();
    new_trail(&s, "dup");
    assert_eq!(
        err_code(
            &s,
            "CreateTrail",
            json!({ "Name": "dup", "S3BucketName": "b" })
        ),
        "TrailAlreadyExistsException"
    );
}

#[test]
fn get_trail_status_toggles_logging() {
    let s = svc();
    new_trail(&s, "t");
    // CreateTrail leaves logging OFF.
    let status = call(&s, "GetTrailStatus", json!({ "Name": "t" }));
    assert_eq!(status["IsLogging"], json!(false));

    call(&s, "StartLogging", json!({ "Name": "t" }));
    let status = call(&s, "GetTrailStatus", json!({ "Name": "t" }));
    assert_eq!(status["IsLogging"], json!(true));
    assert!(status.get("StartLoggingTime").is_some());

    call(&s, "StopLogging", json!({ "Name": "t" }));
    let status = call(&s, "GetTrailStatus", json!({ "Name": "t" }));
    assert_eq!(status["IsLogging"], json!(false));
    assert!(status.get("StopLoggingTime").is_some());

    assert_eq!(
        err_code(&s, "GetTrailStatus", json!({ "Name": "nope" })),
        "TrailNotFoundException"
    );
}

#[test]
fn event_selectors_round_trip() {
    let s = svc();
    new_trail(&s, "t");
    let selectors = json!([{ "ReadWriteType": "WriteOnly", "IncludeManagementEvents": true }]);
    let put = call(
        &s,
        "PutEventSelectors",
        json!({ "TrailName": "t", "EventSelectors": selectors }),
    );
    assert_eq!(put["EventSelectors"], selectors);

    let got = call(&s, "GetEventSelectors", json!({ "TrailName": "t" }));
    assert_eq!(got["EventSelectors"], selectors);
    assert!(got["TrailARN"].as_str().unwrap().ends_with(":trail/t"));

    assert_eq!(
        err_code(
            &s,
            "PutEventSelectors",
            json!({ "TrailName": "nope", "EventSelectors": [] })
        ),
        "TrailNotFoundException"
    );
}

#[test]
fn insight_selectors_round_trip() {
    let s = svc();
    new_trail(&s, "t");
    let sel = json!([{ "InsightType": "ApiCallRateInsight" }]);
    call(
        &s,
        "PutInsightSelectors",
        json!({ "TrailName": "t", "InsightSelectors": sel }),
    );
    let got = call(&s, "GetInsightSelectors", json!({ "TrailName": "t" }));
    assert_eq!(got["InsightSelectors"], sel);
}

#[test]
fn event_data_store_lifecycle() {
    let s = svc();
    let arn = new_eds(&s, "eds1");
    assert!(arn.contains(":eventdatastore/"));

    let got = call(&s, "GetEventDataStore", json!({ "EventDataStore": arn }));
    assert_eq!(got["Status"], json!("ENABLED"));
    assert_eq!(got["Name"], json!("eds1"));

    // With no selectors specified, AWS returns the default management-events
    // advanced selector (terraform's basic acceptance test asserts this).
    let aes = &got["AdvancedEventSelectors"];
    assert_eq!(aes.as_array().unwrap().len(), 1);
    assert_eq!(aes[0]["Name"], json!("Default management events"));
    assert_eq!(aes[0]["FieldSelectors"][0]["Field"], json!("eventCategory"));
    assert_eq!(
        aes[0]["FieldSelectors"][0]["Equals"][0],
        json!("Management")
    );

    let list = call(&s, "ListEventDataStores", json!({}));
    assert_eq!(list["EventDataStores"].as_array().unwrap().len(), 1);

    // Stop/Start ingestion toggles Status.
    call(
        &s,
        "StopEventDataStoreIngestion",
        json!({ "EventDataStore": arn }),
    );
    let got = call(&s, "GetEventDataStore", json!({ "EventDataStore": arn }));
    assert_eq!(got["Status"], json!("STOPPED_INGESTION"));
    call(
        &s,
        "StartEventDataStoreIngestion",
        json!({ "EventDataStore": arn }),
    );
    let got = call(&s, "GetEventDataStore", json!({ "EventDataStore": arn }));
    assert_eq!(got["Status"], json!("ENABLED"));

    // Update persists.
    let upd = call(
        &s,
        "UpdateEventDataStore",
        json!({ "EventDataStore": arn, "RetentionPeriod": 90 }),
    );
    assert_eq!(upd["RetentionPeriod"], json!(90));

    assert_eq!(
        err_code(
            &s,
            "GetEventDataStore",
            json!({ "EventDataStore": "arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/x" })
        ),
        "EventDataStoreNotFoundException"
    );
}

#[test]
fn event_data_store_delete_and_restore() {
    let s = svc();
    let arn = call(
        &s,
        "CreateEventDataStore",
        json!({ "Name": "eds", "TerminationProtectionEnabled": false }),
    )["EventDataStoreArn"]
        .as_str()
        .unwrap()
        .to_string();
    call(&s, "DeleteEventDataStore", json!({ "EventDataStore": arn }));
    let got = call(&s, "GetEventDataStore", json!({ "EventDataStore": arn }));
    assert_eq!(got["Status"], json!("PENDING_DELETION"));
    let restored = call(
        &s,
        "RestoreEventDataStore",
        json!({ "EventDataStore": arn }),
    );
    assert_eq!(restored["Status"], json!("ENABLED"));
}

#[test]
fn federation_toggle() {
    let s = svc();
    let arn = new_eds(&s, "eds");
    let en = call(
        &s,
        "EnableFederation",
        json!({ "EventDataStore": arn, "FederationRoleArn": "arn:aws:iam::000000000000:role/r" }),
    );
    assert_eq!(en["FederationStatus"], json!("ENABLED"));
    let dis = call(&s, "DisableFederation", json!({ "EventDataStore": arn }));
    assert_eq!(dis["FederationStatus"], json!("DISABLED"));
}

#[test]
fn channel_crud() {
    let s = svc();
    let created = call(
        &s,
        "CreateChannel",
        json!({ "Name": "chan1", "Source": "custom", "Destinations": [{ "Type": "EVENT_DATA_STORE", "Location": "arn" }] }),
    );
    let arn = created["ChannelArn"].as_str().unwrap().to_string();
    assert!(arn.contains(":channel/"));

    let got = call(&s, "GetChannel", json!({ "Channel": arn }));
    assert_eq!(got["Name"], json!("chan1"));
    assert!(got.get("SourceConfig").is_some());

    let upd = call(
        &s,
        "UpdateChannel",
        json!({ "Channel": arn, "Name": "chan2" }),
    );
    assert_eq!(upd["Name"], json!("chan2"));

    let list = call(&s, "ListChannels", json!({}));
    assert_eq!(list["Channels"].as_array().unwrap().len(), 1);

    call(&s, "DeleteChannel", json!({ "Channel": arn }));
    assert_eq!(
        err_code(&s, "GetChannel", json!({ "Channel": arn })),
        "ChannelNotFoundException"
    );
}

#[test]
fn import_crud() {
    let s = svc();
    let started = call(
        &s,
        "StartImport",
        json!({ "Destinations": ["arn:eds"], "ImportSource": { "S3": { "S3LocationUri": "s3://b", "S3BucketRegion": "us-east-1", "S3BucketAccessRoleArn": "arn:role" } } }),
    );
    let id = started["ImportId"].as_str().unwrap().to_string();
    let got = call(&s, "GetImport", json!({ "ImportId": id }));
    assert_eq!(got["ImportId"], json!(id));
    let list = call(&s, "ListImports", json!({}));
    assert_eq!(list["Imports"].as_array().unwrap().len(), 1);
    let stopped = call(&s, "StopImport", json!({ "ImportId": id }));
    assert_eq!(stopped["ImportStatus"], json!("STOPPED"));
    let failures = call(&s, "ListImportFailures", json!({ "ImportId": id }));
    assert_eq!(failures["Failures"].as_array().unwrap().len(), 0);
    assert_eq!(
        err_code(
            &s,
            "GetImport",
            json!({ "ImportId": "11111111-1111-1111-1111-111111111111" })
        ),
        "ImportNotFoundException"
    );
}

#[test]
fn query_lifecycle() {
    let s = svc();
    let started = call(
        &s,
        "StartQuery",
        json!({ "QueryStatement": "SELECT * FROM eds" }),
    );
    let id = started["QueryId"].as_str().unwrap().to_string();
    let desc = call(&s, "DescribeQuery", json!({ "QueryId": id }));
    assert_eq!(desc["QueryStatus"], json!("FINISHED"));
    let results = call(&s, "GetQueryResults", json!({ "QueryId": id }));
    assert_eq!(results["QueryStatus"], json!("FINISHED"));
    assert_eq!(results["QueryResultRows"].as_array().unwrap().len(), 0);
    let cancelled = call(&s, "CancelQuery", json!({ "QueryId": id }));
    assert_eq!(cancelled["QueryStatus"], json!("CANCELLED"));
    assert_eq!(
        err_code(
            &s,
            "DescribeQuery",
            json!({ "QueryId": "22222222-2222-2222-2222-222222222222" })
        ),
        "QueryIdNotFoundException"
    );
}

#[test]
fn list_queries_requires_existing_eds() {
    let s = svc();
    assert_eq!(
        err_code(
            &s,
            "ListQueries",
            json!({ "EventDataStore": "arn:aws:cloudtrail:us-east-1:000000000000:eventdatastore/x" })
        ),
        "EventDataStoreNotFoundException"
    );
    let arn = new_eds(&s, "eds");
    let list = call(&s, "ListQueries", json!({ "EventDataStore": arn }));
    assert!(list["Queries"].is_array());
}

#[test]
fn dashboard_crud() {
    let s = svc();
    let created = call(&s, "CreateDashboard", json!({ "Name": "dash1" }));
    let arn = created["DashboardArn"].as_str().unwrap().to_string();
    let got = call(&s, "GetDashboard", json!({ "DashboardId": arn }));
    assert_eq!(got["Type"], json!("CUSTOM"));
    let refresh = call(&s, "StartDashboardRefresh", json!({ "DashboardId": arn }));
    assert!(refresh.get("RefreshId").is_some());
    let list = call(&s, "ListDashboards", json!({}));
    assert_eq!(list["Dashboards"].as_array().unwrap().len(), 1);
    call(&s, "DeleteDashboard", json!({ "DashboardId": arn }));
    assert_eq!(
        err_code(&s, "GetDashboard", json!({ "DashboardId": arn })),
        "ResourceNotFoundException"
    );
}

#[test]
fn resource_policy_round_trip() {
    let s = svc();
    let arn = "arn:aws:cloudtrail:us-east-1:000000000000:channel/c";
    call(
        &s,
        "PutResourcePolicy",
        json!({ "ResourceArn": arn, "ResourcePolicy": "{\"Version\":\"2012-10-17\"}" }),
    );
    let got = call(&s, "GetResourcePolicy", json!({ "ResourceArn": arn }));
    assert!(got["ResourcePolicy"]
        .as_str()
        .unwrap()
        .contains("2012-10-17"));
    call(&s, "DeleteResourcePolicy", json!({ "ResourceArn": arn }));
    assert_eq!(
        err_code(&s, "GetResourcePolicy", json!({ "ResourceArn": arn })),
        "ResourcePolicyNotFoundException"
    );
}

#[test]
fn delegated_admin_register_deregister() {
    let s = svc();
    call(
        &s,
        "RegisterOrganizationDelegatedAdmin",
        json!({ "MemberAccountId": "111122223333" }),
    );
    assert_eq!(
        err_code(
            &s,
            "RegisterOrganizationDelegatedAdmin",
            json!({ "MemberAccountId": "111122223333" })
        ),
        "AccountRegisteredException"
    );
    call(
        &s,
        "DeregisterOrganizationDelegatedAdmin",
        json!({ "DelegatedAdminAccountId": "111122223333" }),
    );
    assert_eq!(
        err_code(
            &s,
            "DeregisterOrganizationDelegatedAdmin",
            json!({ "DelegatedAdminAccountId": "111122223333" })
        ),
        "AccountNotRegisteredException"
    );
}

#[test]
fn event_configuration_round_trip() {
    let s = svc();
    new_trail(&s, "t");
    let put = call(
        &s,
        "PutEventConfiguration",
        json!({ "TrailName": "t", "MaxEventSize": "Large" }),
    );
    assert_eq!(put["MaxEventSize"], json!("Large"));
    let got = call(&s, "GetEventConfiguration", json!({ "TrailName": "t" }));
    assert_eq!(got["MaxEventSize"], json!("Large"));
}

#[test]
fn tag_round_trip() {
    let s = svc();
    let arn = "arn:aws:cloudtrail:us-east-1:000000000000:trail/t";
    call(
        &s,
        "AddTags",
        json!({ "ResourceId": arn, "TagsList": [{ "Key": "env", "Value": "prod" }] }),
    );
    let listed = call(&s, "ListTags", json!({ "ResourceIdList": [arn] }));
    let tags = listed["ResourceTagList"][0]["TagsList"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    call(
        &s,
        "RemoveTags",
        json!({ "ResourceId": arn, "TagsList": [{ "Key": "env", "Value": "prod" }] }),
    );
    let listed = call(&s, "ListTags", json!({ "ResourceIdList": [arn] }));
    assert_eq!(
        listed["ResourceTagList"][0]["TagsList"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn lookup_events_returns_empty() {
    let s = svc();
    let out = call(&s, "LookupEvents", json!({}));
    assert_eq!(out["Events"].as_array().unwrap().len(), 0);
    assert!(out.get("NextToken").is_none());
}

#[test]
fn empty_result_ops() {
    let s = svc();
    assert_eq!(
        call(&s, "ListPublicKeys", json!({}))["PublicKeyList"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        call(
            &s,
            "SearchSampleQueries",
            json!({ "SearchPhrase": "errors" })
        )["SearchResults"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    let metrics = call(
        &s,
        "ListInsightsMetricData",
        json!({ "EventSource": "ec2.amazonaws.com", "EventName": "RunInstances", "InsightType": "ApiCallRateInsight" }),
    );
    assert_eq!(metrics["Values"].as_array().unwrap().len(), 0);
}

#[test]
fn validation_rejects_bad_enum() {
    let s = svc();
    assert_eq!(
        err_code(
            &s,
            "CreateEventDataStore",
            json!({ "Name": "eds", "BillingMode": "BOGUS" })
        ),
        "InvalidParameterException"
    );
}
