//! Unit tests for the Cost Explorer control plane.

use super::*;
use bytes::Bytes;
use fakecloud_core::multi_account::MultiAccountState;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;

fn svc() -> CeService {
    CeService::new(Arc::new(RwLock::new(MultiAccountState::new(
        "000000000000",
        "us-east-1",
        "",
    ))))
}

fn req(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "ce".into(),
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

fn call(s: &CeService, action: &str, body: Value) -> Value {
    let resp = dispatch(s, &req(action, body)).expect("op ok");
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn err_code(s: &CeService, action: &str, body: Value) -> String {
    let e = dispatch(s, &req(action, body))
        .err()
        .expect("expected error");
    e.code().to_string()
}

#[test]
fn anomaly_monitor_crud_round_trips() {
    let s = svc();
    let out = call(
        &s,
        "CreateAnomalyMonitor",
        json!({ "AnomalyMonitor": { "MonitorName": "m1", "MonitorType": "CUSTOM" } }),
    );
    let arn = out["MonitorArn"].as_str().unwrap().to_string();
    assert!(arn.contains(":anomalymonitor/"));

    let list = call(&s, "GetAnomalyMonitors", json!({}));
    assert_eq!(list["AnomalyMonitors"].as_array().unwrap().len(), 1);

    call(
        &s,
        "UpdateAnomalyMonitor",
        json!({ "MonitorArn": arn, "MonitorName": "m2" }),
    );
    call(&s, "DeleteAnomalyMonitor", json!({ "MonitorArn": arn }));
    let list = call(&s, "GetAnomalyMonitors", json!({}));
    assert_eq!(list["AnomalyMonitors"].as_array().unwrap().len(), 0);
}

#[test]
fn delete_unknown_monitor_errors() {
    let s = svc();
    assert_eq!(
        err_code(
            &s,
            "DeleteAnomalyMonitor",
            json!({ "MonitorArn": "arn:aws:ce::000000000000:anomalymonitor/x" })
        ),
        "UnknownMonitorException"
    );
}

#[test]
fn cost_category_round_trip_and_not_found() {
    let s = svc();
    let out = call(
        &s,
        "CreateCostCategoryDefinition",
        json!({
            "Name": "cc",
            "RuleVersion": "CostCategoryExpression.v1",
            "Rules": [{ "Value": "x", "Rule": {} }]
        }),
    );
    let arn = out["CostCategoryArn"].as_str().unwrap().to_string();

    let desc = call(
        &s,
        "DescribeCostCategoryDefinition",
        json!({ "CostCategoryArn": arn }),
    );
    assert_eq!(desc["CostCategory"]["Name"], "cc");

    assert_eq!(
        err_code(
            &s,
            "DescribeCostCategoryDefinition",
            json!({ "CostCategoryArn": "arn:aws:ce::000000000000:costcategory/none" })
        ),
        "ResourceNotFoundException"
    );
}

#[test]
fn get_cost_and_usage_returns_zero_buckets() {
    let s = svc();
    let out = call(
        &s,
        "GetCostAndUsage",
        json!({
            "TimePeriod": { "Start": "2024-01-01", "End": "2024-03-01" },
            "Granularity": "MONTHLY",
            "Metrics": ["UnblendedCost"]
        }),
    );
    let results = out["ResultsByTime"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["Estimated"], false);
    assert_eq!(results[0]["Total"]["UnblendedCost"]["Amount"], "0");
}

#[test]
fn savings_plans_utilization_has_required_total() {
    let s = svc();
    let out = call(
        &s,
        "GetSavingsPlansUtilization",
        json!({ "TimePeriod": { "Start": "2024-01-01", "End": "2024-02-01" } }),
    );
    assert!(out["Total"]["Utilization"].is_object());
}

#[test]
fn missing_required_field_is_rejected() {
    let s = svc();
    assert_eq!(
        err_code(&s, "GetCostAndUsage", json!({ "Granularity": "MONTHLY" })),
        "ValidationException"
    );
}

#[test]
fn invalid_enum_is_rejected() {
    let s = svc();
    assert_eq!(
        err_code(
            &s,
            "GetCostForecast",
            json!({
                "TimePeriod": { "Start": "2024-01-01", "End": "2024-02-01" },
                "Metric": "NOT_A_METRIC",
                "Granularity": "MONTHLY"
            })
        ),
        "ValidationException"
    );
}
