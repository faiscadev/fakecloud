//! Unit tests for the CloudWatch service handlers added in phase 4
//! (anomaly detectors, insight rules, metric streams, composite alarms,
//! mute rules, OTel, tagging). Each exercises a happy path plus a declared
//! error case where the op declares one.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::{Mutex, RwLock};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::service::CloudWatchService;
use crate::state::CloudWatchAccounts;

const ACCT: &str = "123456789012";
const REGION: &str = "us-east-1";

fn service() -> CloudWatchService {
    CloudWatchService::new(Arc::new(RwLock::new(CloudWatchAccounts::new())))
}

fn req(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut query_params = HashMap::new();
    for (k, v) in params {
        query_params.insert((*k).to_string(), (*v).to_string());
    }
    AwsRequest {
        service: "monitoring".to_string(),
        action: action.to_string(),
        region: REGION.to_string(),
        account_id: ACCT.to_string(),
        request_id: "test-req".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::new(),
        body_stream: Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    }
}

async fn call(svc: &CloudWatchService, action: &str, params: &[(&str, &str)]) -> AwsResponse {
    svc.handle(req(action, params)).await.expect("handler ok")
}

fn body_of(resp: &AwsResponse) -> String {
    String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
}

/// Call a handler expecting it to fail; return the error (AwsResponse isn't
/// `Debug`, so `unwrap_err` can't be used directly).
async fn call_err(
    svc: &CloudWatchService,
    action: &str,
    params: &[(&str, &str)],
) -> AwsServiceError {
    match svc.handle(req(action, params)).await {
        Ok(_) => panic!("expected {action} to fail"),
        Err(e) => e,
    }
}

#[tokio::test]
async fn anomaly_detector_lifecycle() {
    let svc = service();
    call(
        &svc,
        "PutAnomalyDetector",
        &[
            ("Namespace", "AWS/EC2"),
            ("MetricName", "CPU"),
            ("Stat", "Average"),
        ],
    )
    .await;

    let described = call(
        &svc,
        "DescribeAnomalyDetectors",
        &[("Namespace", "AWS/EC2")],
    )
    .await;
    let b = body_of(&described);
    assert!(b.contains("<MetricName>CPU</MetricName>"));
    assert!(b.contains("<SingleMetricAnomalyDetector>"));

    call(
        &svc,
        "DeleteAnomalyDetector",
        &[
            ("Namespace", "AWS/EC2"),
            ("MetricName", "CPU"),
            ("Stat", "Average"),
        ],
    )
    .await;
    let after = call(
        &svc,
        "DescribeAnomalyDetectors",
        &[("Namespace", "AWS/EC2")],
    )
    .await;
    assert!(!body_of(&after).contains("<MetricName>CPU</MetricName>"));
}

#[tokio::test]
async fn delete_anomaly_detector_missing_is_not_found() {
    let svc = service();
    let err = call_err(
        &svc,
        "DeleteAnomalyDetector",
        &[
            ("Namespace", "Nope"),
            ("MetricName", "Nope"),
            ("Stat", "Sum"),
        ],
    )
    .await;
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn insight_rule_lifecycle() {
    let svc = service();
    call(
        &svc,
        "PutInsightRule",
        &[("RuleName", "r1"), ("RuleDefinition", "{\"x\":1}")],
    )
    .await;
    let described = call(&svc, "DescribeInsightRules", &[]).await;
    assert!(body_of(&described).contains("<State>ENABLED</State>"));

    call(&svc, "DisableInsightRules", &[("RuleNames.member.1", "r1")]).await;
    let disabled = call(&svc, "DescribeInsightRules", &[]).await;
    assert!(body_of(&disabled).contains("<State>DISABLED</State>"));

    let report = call(
        &svc,
        "GetInsightRuleReport",
        &[
            ("RuleName", "r1"),
            ("StartTime", "2024-01-01T00:00:00Z"),
            ("EndTime", "2024-01-02T00:00:00Z"),
            ("Period", "60"),
        ],
    )
    .await;
    assert!(body_of(&report).contains("<Contributors/>"));

    call(&svc, "DeleteInsightRules", &[("RuleNames.member.1", "r1")]).await;
    let gone = call(&svc, "DescribeInsightRules", &[]).await;
    assert!(!body_of(&gone).contains("r1"));
}

#[tokio::test]
async fn insight_rule_report_missing_is_not_found() {
    let svc = service();
    let err = call_err(
        &svc,
        "GetInsightRuleReport",
        &[
            ("RuleName", "ghost"),
            ("StartTime", "2024-01-01T00:00:00Z"),
            ("EndTime", "2024-01-02T00:00:00Z"),
            ("Period", "60"),
        ],
    )
    .await;
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn managed_insight_rules_lifecycle() {
    let svc = service();
    let arn = "arn:aws:ecs:us-east-1:123456789012:cluster/c";
    call(
        &svc,
        "PutManagedInsightRules",
        &[
            ("ManagedRules.member.1.TemplateName", "ECS"),
            ("ManagedRules.member.1.ResourceARN", arn),
        ],
    )
    .await;
    let list = call(&svc, "ListManagedInsightRules", &[("ResourceARN", arn)]).await;
    assert!(body_of(&list).contains("<TemplateName>ECS</TemplateName>"));
}

#[tokio::test]
async fn metric_stream_lifecycle() {
    let svc = service();
    call(
        &svc,
        "PutMetricStream",
        &[
            ("Name", "s1"),
            (
                "FirehoseArn",
                "arn:aws:firehose:us-east-1:123456789012:deliverystream/x",
            ),
            ("RoleArn", "arn:aws:iam::123456789012:role/x"),
            ("OutputFormat", "JSON"),
        ],
    )
    .await;
    let got = call(&svc, "GetMetricStream", &[("Name", "s1")]).await;
    assert!(body_of(&got).contains("<State>running</State>"));

    call(&svc, "StopMetricStreams", &[("Names.member.1", "s1")]).await;
    let stopped = call(&svc, "GetMetricStream", &[("Name", "s1")]).await;
    assert!(body_of(&stopped).contains("<State>stopped</State>"));

    call(&svc, "StartMetricStreams", &[("Names.member.1", "s1")]).await;
    let list = call(&svc, "ListMetricStreams", &[]).await;
    assert!(body_of(&list).contains("<Name>s1</Name>"));

    call(&svc, "DeleteMetricStream", &[("Name", "s1")]).await;
    let err = call_err(&svc, "GetMetricStream", &[("Name", "s1")]).await;
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn composite_alarm_in_describe_alarms() {
    let svc = service();
    call(
        &svc,
        "PutCompositeAlarm",
        &[("AlarmName", "comp"), ("AlarmRule", "ALARM(x)")],
    )
    .await;
    let described = call(&svc, "DescribeAlarms", &[("AlarmNames.member.1", "comp")]).await;
    let b = body_of(&described);
    assert!(b.contains("<AlarmRule>ALARM(x)</AlarmRule>"));
    assert!(b.contains("<CompositeAlarms>"));

    call(&svc, "DeleteAlarms", &[("AlarmNames.member.1", "comp")]).await;
    let after = call(&svc, "DescribeAlarms", &[("AlarmNames.member.1", "comp")]).await;
    assert!(!body_of(&after).contains("ALARM(x)"));
}

#[tokio::test]
async fn mute_rule_lifecycle() {
    let svc = service();
    call(
        &svc,
        "PutAlarmMuteRule",
        &[
            ("Name", "m1"),
            ("Rule.Schedule.Expression", "cron(0 2 * * *)"),
            ("Rule.Schedule.Duration", "PT4H"),
        ],
    )
    .await;
    let got = call(&svc, "GetAlarmMuteRule", &[("AlarmMuteRuleName", "m1")]).await;
    let b = body_of(&got);
    assert!(b.contains("<Expression>cron(0 2 * * *)</Expression>"));
    assert!(b.contains("<Status>ACTIVE</Status>"));

    let list = call(&svc, "ListAlarmMuteRules", &[]).await;
    assert!(body_of(&list).contains("alarm-mute-rule/m1"));

    call(&svc, "DeleteAlarmMuteRule", &[("AlarmMuteRuleName", "m1")]).await;
    let err = call_err(&svc, "GetAlarmMuteRule", &[("AlarmMuteRuleName", "m1")]).await;
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn otel_enrichment_toggle() {
    let svc = service();
    let initial = call(&svc, "GetOTelEnrichment", &[]).await;
    assert!(body_of(&initial).contains("<Status>STOPPED</Status>"));

    call(&svc, "StartOTelEnrichment", &[]).await;
    let running = call(&svc, "GetOTelEnrichment", &[]).await;
    assert!(body_of(&running).contains("<Status>RUNNING</Status>"));

    call(&svc, "StopOTelEnrichment", &[]).await;
    let stopped = call(&svc, "GetOTelEnrichment", &[]).await;
    assert!(body_of(&stopped).contains("<Status>STOPPED</Status>"));
}

#[tokio::test]
async fn describe_alarm_contributors_unknown_is_not_found() {
    let svc = service();
    let err = call_err(&svc, "DescribeAlarmContributors", &[("AlarmName", "ghost")]).await;
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn metric_widget_image_returns_blob() {
    let svc = service();
    let resp = call(
        &svc,
        "GetMetricWidgetImage",
        &[("MetricWidget", "{\"metrics\":[]}")],
    )
    .await;
    assert!(body_of(&resp).contains("<MetricWidgetImage>"));
}

#[tokio::test]
async fn tagging_lifecycle() {
    let svc = service();
    let arn = "arn:aws:cloudwatch:us-east-1:123456789012:alarm:a";
    call(
        &svc,
        "TagResource",
        &[
            ("ResourceARN", arn),
            ("Tags.member.1.Key", "env"),
            ("Tags.member.1.Value", "prod"),
        ],
    )
    .await;
    let listed = call(&svc, "ListTagsForResource", &[("ResourceARN", arn)]).await;
    let b = body_of(&listed);
    assert!(b.contains("<Key>env</Key>"));
    assert!(b.contains("<Value>prod</Value>"));

    call(
        &svc,
        "UntagResource",
        &[("ResourceARN", arn), ("TagKeys.member.1", "env")],
    )
    .await;
    let after = call(&svc, "ListTagsForResource", &[("ResourceARN", arn)]).await;
    assert!(!body_of(&after).contains("<Key>env</Key>"));
}

#[tokio::test]
async fn list_tags_missing_arn_errors() {
    let svc = service();
    let err = call_err(&svc, "ListTagsForResource", &[]).await;
    assert_eq!(err.code(), "MissingParameter");
}
