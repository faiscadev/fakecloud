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
    // AWS returns only metric alarms by default; composite alarms must be
    // requested explicitly via AlarmTypes.
    let described = call(
        &svc,
        "DescribeAlarms",
        &[
            ("AlarmNames.member.1", "comp"),
            ("AlarmTypes.member.1", "CompositeAlarm"),
        ],
    )
    .await;
    let b = body_of(&described);
    assert!(b.contains("<AlarmRule>ALARM(x)</AlarmRule>"));
    assert!(b.contains("<CompositeAlarms>"));

    call(&svc, "DeleteAlarms", &[("AlarmNames.member.1", "comp")]).await;
    let after = call(
        &svc,
        "DescribeAlarms",
        &[
            ("AlarmNames.member.1", "comp"),
            ("AlarmTypes.member.1", "CompositeAlarm"),
        ],
    )
    .await;
    assert!(!body_of(&after).contains("ALARM(x)"));
}

// H5: metric alarms are evaluated against PutMetricData on DescribeAlarms.
#[tokio::test]
async fn metric_alarm_evaluates_to_alarm_above_threshold() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "cpu-alarm"),
            ("Namespace", "Test/App"),
            ("MetricName", "CPU"),
            ("ComparisonOperator", "GreaterThanThreshold"),
            ("Threshold", "50"),
            ("EvaluationPeriods", "1"),
            ("Period", "60"),
            ("Statistic", "Average"),
        ],
    )
    .await;
    // Publish a datapoint above the threshold.
    call(
        &svc,
        "PutMetricData",
        &[
            ("Namespace", "Test/App"),
            ("MetricData.member.1.MetricName", "CPU"),
            ("MetricData.member.1.Value", "100"),
        ],
    )
    .await;
    let desc = call(
        &svc,
        "DescribeAlarms",
        &[("AlarmNames.member.1", "cpu-alarm")],
    )
    .await;
    assert!(
        body_of(&desc).contains("<StateValue>ALARM</StateValue>"),
        "alarm above threshold should be ALARM: {}",
        body_of(&desc)
    );
}

#[tokio::test]
async fn metric_alarm_evaluates_to_ok_below_threshold() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "cpu-ok"),
            ("Namespace", "Test/App2"),
            ("MetricName", "CPU"),
            ("ComparisonOperator", "GreaterThanThreshold"),
            ("Threshold", "50"),
            ("EvaluationPeriods", "1"),
            ("Period", "60"),
            ("Statistic", "Average"),
        ],
    )
    .await;
    call(
        &svc,
        "PutMetricData",
        &[
            ("Namespace", "Test/App2"),
            ("MetricData.member.1.MetricName", "CPU"),
            ("MetricData.member.1.Value", "10"),
        ],
    )
    .await;
    let desc = call(&svc, "DescribeAlarms", &[("AlarmNames.member.1", "cpu-ok")]).await;
    assert!(
        body_of(&desc).contains("<StateValue>OK</StateValue>"),
        "alarm below threshold should be OK: {}",
        body_of(&desc)
    );
}

// DescribeAlarmsForMetric returns freshly-evaluated state (not a stale stored
// value) and filters by Dimensions.
#[tokio::test]
async fn describe_alarms_for_metric_returns_fresh_state_filtered_by_dimensions() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "dfm-alarm"),
            ("Namespace", "Test/DFM"),
            ("MetricName", "CPU"),
            ("Dimensions.member.1.Name", "Host"),
            ("Dimensions.member.1.Value", "a"),
            ("ComparisonOperator", "GreaterThanThreshold"),
            ("Threshold", "50"),
            ("EvaluationPeriods", "1"),
            ("Period", "60"),
            ("Statistic", "Average"),
        ],
    )
    .await;
    // Publish a breaching datapoint AFTER creating the alarm; the handler must
    // re-evaluate rather than return the initial INSUFFICIENT_DATA.
    call(
        &svc,
        "PutMetricData",
        &[
            ("Namespace", "Test/DFM"),
            ("MetricData.member.1.MetricName", "CPU"),
            ("MetricData.member.1.Value", "100"),
            ("MetricData.member.1.Dimensions.member.1.Name", "Host"),
            ("MetricData.member.1.Dimensions.member.1.Value", "a"),
        ],
    )
    .await;

    // Matching dimensions -> the alarm is returned with its fresh ALARM state.
    let matched = call(
        &svc,
        "DescribeAlarmsForMetric",
        &[
            ("Namespace", "Test/DFM"),
            ("MetricName", "CPU"),
            ("Dimensions.member.1.Name", "Host"),
            ("Dimensions.member.1.Value", "a"),
        ],
    )
    .await;
    let body = body_of(&matched);
    assert!(body.contains("dfm-alarm"), "alarm should match: {body}");
    assert!(
        body.contains("<StateValue>ALARM</StateValue>"),
        "fresh state should be ALARM: {body}"
    );

    // Non-matching dimensions -> filtered out.
    let unmatched = call(
        &svc,
        "DescribeAlarmsForMetric",
        &[
            ("Namespace", "Test/DFM"),
            ("MetricName", "CPU"),
            ("Dimensions.member.1.Name", "Host"),
            ("Dimensions.member.1.Value", "b"),
        ],
    )
    .await;
    assert!(
        !body_of(&unmatched).contains("dfm-alarm"),
        "different dimensions must not match: {}",
        body_of(&unmatched)
    );
}

// A manually-set state must survive DescribeAlarms re-evaluation when the
// watched metric has no datapoints (default treat-missing-data).
#[tokio::test]
async fn set_alarm_state_survives_evaluation_without_data() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "manual"),
            ("Namespace", "Test/None"),
            ("MetricName", "M"),
            ("ComparisonOperator", "GreaterThanThreshold"),
            ("Threshold", "1"),
            ("EvaluationPeriods", "1"),
            ("Statistic", "Sum"),
        ],
    )
    .await;
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "manual"),
            ("StateValue", "ALARM"),
            ("StateReason", "manual"),
        ],
    )
    .await;
    let desc = call(&svc, "DescribeAlarms", &[("AlarmNames.member.1", "manual")]).await;
    assert!(body_of(&desc).contains("<StateValue>ALARM</StateValue>"));
}

// H3: a composite alarm reflects ALARM when its rule is satisfied by children.
#[tokio::test]
async fn composite_alarm_reflects_children_states() {
    let svc = service();
    for name in ["child-a", "child-b"] {
        call(
            &svc,
            "PutMetricAlarm",
            &[
                ("AlarmName", name),
                ("Namespace", "Test/C"),
                ("MetricName", "M"),
                ("ComparisonOperator", "GreaterThanThreshold"),
                ("Threshold", "1"),
                ("EvaluationPeriods", "1"),
                ("Statistic", "Sum"),
            ],
        )
        .await;
        call(
            &svc,
            "SetAlarmState",
            &[
                ("AlarmName", name),
                ("StateValue", "ALARM"),
                ("StateReason", "x"),
            ],
        )
        .await;
    }
    call(
        &svc,
        "PutCompositeAlarm",
        &[
            ("AlarmName", "comp2"),
            ("AlarmRule", "ALARM(child-a) AND ALARM(child-b)"),
        ],
    )
    .await;
    let desc = call(
        &svc,
        "DescribeAlarms",
        &[
            ("AlarmNames.member.1", "comp2"),
            ("AlarmTypes.member.1", "CompositeAlarm"),
        ],
    )
    .await;
    let b = body_of(&desc);
    assert!(
        b.contains("<CompositeAlarms>") && b.contains("<StateValue>ALARM</StateValue>"),
        "composite should be ALARM when both children ALARM: {b}"
    );

    // Flip one child OK -> composite should evaluate back to OK.
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "child-a"),
            ("StateValue", "OK"),
            ("StateReason", "x"),
        ],
    )
    .await;
    let desc = call(
        &svc,
        "DescribeAlarms",
        &[
            ("AlarmNames.member.1", "comp2"),
            ("AlarmTypes.member.1", "CompositeAlarm"),
        ],
    )
    .await;
    assert!(body_of(&desc).contains("<StateValue>OK</StateValue>"));
}

// H4: SetAlarmState can target a composite alarm.
#[tokio::test]
async fn set_alarm_state_targets_composite_alarm() {
    let svc = service();
    call(
        &svc,
        "PutCompositeAlarm",
        &[("AlarmName", "comp3"), ("AlarmRule", "ALARM(x)")],
    )
    .await;
    // Previously this returned ResourceNotFound (only metric alarms checked).
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "comp3"),
            ("StateValue", "ALARM"),
            ("StateReason", "manual"),
        ],
    )
    .await;
    // Verify directly against state (DescribeAlarms would re-evaluate the rule).
    let state = svc.state.read();
    let sv = state
        .get(ACCT)
        .and_then(|a| a.composite_alarms_in(REGION))
        .and_then(|c| c.get("comp3"))
        .map(|c| c.state_value)
        .unwrap();
    assert_eq!(sv, crate::state::AlarmState::Alarm);
}

// H3: PutCompositeAlarm rejects a malformed AlarmRule.
#[tokio::test]
async fn put_composite_alarm_rejects_malformed_rule() {
    let svc = service();
    let err = call_err(
        &svc,
        "PutCompositeAlarm",
        &[("AlarmName", "bad"), ("AlarmRule", "ALARM(")],
    )
    .await;
    assert_eq!(err.code(), "ValidationError");
}

// M1: JSON-protocol epoch-second timestamps are accepted on PutMetricData.
#[tokio::test]
async fn put_metric_data_accepts_epoch_second_timestamp() {
    let svc = service();
    let now = chrono::Utc::now().timestamp();
    call(
        &svc,
        "PutMetricData",
        &[
            ("Namespace", "Test/Epoch"),
            ("MetricData.member.1.MetricName", "M"),
            ("MetricData.member.1.Value", "5"),
            ("MetricData.member.1.Timestamp", &now.to_string()),
        ],
    )
    .await;
    // Read it back over a window that includes `now`; an epoch-second timestamp
    // that was dropped would have defaulted to ingestion time (still now), so
    // instead assert the stored datum carries the exact epoch we sent.
    let state = svc.state.read();
    let datum_ts = state
        .get(ACCT)
        .and_then(|a| a.metrics_in(REGION))
        .and_then(|m| m.get("Test/Epoch"))
        .and_then(|v| v.first())
        .map(|d| d.timestamp.timestamp())
        .unwrap();
    assert_eq!(datum_ts, now);
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

#[tokio::test]
async fn put_metric_alarm_round_trips_threshold_metric_id() {
    // ThresholdMetricId (anomaly-detection band) was accepted then dropped
    // (bug-audit 2026-06-20, 1.24); DescribeAlarms must echo it.
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "anomaly-alarm"),
            ("Namespace", "AWS/EC2"),
            ("MetricName", "CPUUtilization"),
            ("ComparisonOperator", "GreaterThanUpperThreshold"),
            ("ThresholdMetricId", "ad1"),
            ("EvaluationPeriods", "1"),
        ],
    )
    .await;

    let described = call(
        &svc,
        "DescribeAlarms",
        &[("AlarmNames.member.1", "anomaly-alarm")],
    )
    .await;
    let body = body_of(&described);
    assert!(
        body.contains("<ThresholdMetricId>ad1</ThresholdMetricId>"),
        "DescribeAlarms must echo ThresholdMetricId: {body}"
    );
}

#[tokio::test]
async fn introspection_lists_metric_and_composite_alarms() {
    use crate::introspection::list_all_alarms;
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "cpu-high"),
            ("Namespace", "AWS/EC2"),
            ("MetricName", "CPUUtilization"),
            ("ComparisonOperator", "GreaterThanThreshold"),
            ("Threshold", "80"),
            ("EvaluationPeriods", "1"),
            (
                "AlarmActions.member.1",
                "arn:aws:sns:us-east-1:123456789012:topic",
            ),
        ],
    )
    .await;
    call(
        &svc,
        "PutCompositeAlarm",
        &[("AlarmName", "comp"), ("AlarmRule", "ALARM(cpu-high)")],
    )
    .await;

    let rows = list_all_alarms(&svc.state);
    assert_eq!(rows.len(), 2);
    // Sorted by name within account/region: "comp" < "cpu-high".
    assert_eq!(rows[0].name, "comp");
    assert_eq!(rows[0].kind, "composite");
    assert_eq!(rows[0].alarm_rule.as_deref(), Some("ALARM(cpu-high)"));
    assert!(rows[0].namespace.is_none());

    assert_eq!(rows[1].name, "cpu-high");
    assert_eq!(rows[1].kind, "metric");
    assert_eq!(rows[1].namespace.as_deref(), Some("AWS/EC2"));
    assert_eq!(rows[1].metric_name.as_deref(), Some("CPUUtilization"));
    assert_eq!(rows[1].threshold, Some(80.0));
    assert_eq!(
        rows[1].comparison_operator.as_deref(),
        Some("GreaterThanThreshold")
    );
    assert_eq!(rows[1].state, "INSUFFICIENT_DATA");
    assert_eq!(
        rows[1].alarm_actions,
        vec!["arn:aws:sns:us-east-1:123456789012:topic".to_string()]
    );
}

#[tokio::test]
async fn introspection_collapses_metric_series() {
    use crate::introspection::list_all_metrics;
    let svc = service();
    // Two datapoints for the same series.
    call(
        &svc,
        "PutMetricData",
        &[
            ("Namespace", "MyApp"),
            ("MetricData.member.1.MetricName", "Requests"),
            ("MetricData.member.1.Value", "1"),
            ("MetricData.member.1.Unit", "Count"),
        ],
    )
    .await;
    call(
        &svc,
        "PutMetricData",
        &[
            ("Namespace", "MyApp"),
            ("MetricData.member.1.MetricName", "Requests"),
            ("MetricData.member.1.Value", "5"),
            ("MetricData.member.1.Unit", "Count"),
        ],
    )
    .await;

    let rows = list_all_metrics(&svc.state);
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r.namespace, "MyApp");
    assert_eq!(r.metric_name, "Requests");
    assert_eq!(r.datapoint_count, 2);
    let latest = r.latest.as_ref().expect("latest present");
    assert_eq!(latest.value, Some(5.0));
    assert_eq!(latest.unit.as_deref(), Some("Count"));
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = service();
    assert!(svc.snapshot_hook().is_none());
}

#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = service().with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}

// ---------------------------------------------------------------------------
// awsJson1_0 protocol support (CloudWatch advertises awsJson1_0 alongside the
// legacy awsQuery protocol). A JSON body is flattened into the same flat-key
// param map the handlers consume, and the XML response is re-serialized as
// JSON for the JSON caller.
// ---------------------------------------------------------------------------

/// Build a JSON-protocol request: flatten the JSON body the way the central
/// dispatcher does, and set the `X-Amz-Target` header that identifies a JSON
/// caller.
fn json_req(action: &str, body: serde_json::Value) -> AwsRequest {
    let bytes = Bytes::from(serde_json::to_vec(&body).unwrap());
    let flat = fakecloud_core::protocol::flatten_json_to_query(&bytes);
    let mut query_params = HashMap::new();
    for (k, v) in flat {
        query_params.insert(k, v);
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-amz-target",
        format!("GraniteServiceVersion20100801.{action}")
            .parse()
            .unwrap(),
    );
    AwsRequest {
        service: "monitoring".to_string(),
        action: action.to_string(),
        region: REGION.to_string(),
        account_id: ACCT.to_string(),
        request_id: "test-req".to_string(),
        headers,
        query_params,
        body: bytes,
        body_stream: Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

async fn call_json(svc: &CloudWatchService, action: &str, body: serde_json::Value) -> AwsResponse {
    svc.handle(json_req(action, body))
        .await
        .expect("handler ok")
}

fn json_body(resp: &AwsResponse) -> serde_json::Value {
    serde_json::from_slice(resp.body.expect_bytes()).expect("valid json body")
}

#[tokio::test]
async fn put_metric_data_json_and_query_roundtrip() {
    let svc = service();
    // JSON protocol PutMetricData.
    let resp = call_json(
        &svc,
        "PutMetricData",
        serde_json::json!({
            "Namespace": "MyApp",
            "MetricData": [{
                "MetricName": "Latency",
                "Value": 12.5,
                "Dimensions": [{"Name": "Endpoint", "Value": "/api"}]
            }]
        }),
    )
    .await;
    assert_eq!(resp.content_type, "application/x-amz-json-1.0");
    assert_eq!(json_body(&resp), serde_json::json!({}));

    // The data is readable back over BOTH protocols.
    let json_list = call_json(
        &svc,
        "ListMetrics",
        serde_json::json!({"Namespace": "MyApp"}),
    )
    .await;
    let v = json_body(&json_list);
    assert_eq!(v["Metrics"][0]["Namespace"], "MyApp");
    assert_eq!(v["Metrics"][0]["MetricName"], "Latency");
    assert_eq!(v["Metrics"][0]["Dimensions"][0]["Name"], "Endpoint");
    assert_eq!(v["Metrics"][0]["Dimensions"][0]["Value"], "/api");

    // Query protocol still returns XML for the same data.
    let xml_list = call(&svc, "ListMetrics", &[("Namespace", "MyApp")]).await;
    assert_eq!(xml_list.content_type, "text/xml");
    assert!(body_of(&xml_list).contains("<MetricName>Latency</MetricName>"));
}

#[tokio::test]
async fn list_metrics_name_only_dimension_filter_narrows_results() {
    // Regression: a DimensionFilter with only a Name (no Value) must still
    // narrow the results. Before the fix such a filter was dropped and every
    // metric in the namespace was returned.
    let svc = service();
    call_json(
        &svc,
        "PutMetricData",
        serde_json::json!({
            "Namespace": "NS",
            "MetricData": [{
                "MetricName": "M1",
                "Value": 1.0,
                "Dimensions": [{"Name": "Host", "Value": "a"}]
            }]
        }),
    )
    .await;
    call_json(
        &svc,
        "PutMetricData",
        serde_json::json!({
            "Namespace": "NS",
            "MetricData": [{
                "MetricName": "M2",
                "Value": 2.0,
                "Dimensions": [{"Name": "Region", "Value": "us"}]
            }]
        }),
    )
    .await;

    let resp = call(
        &svc,
        "ListMetrics",
        &[("Namespace", "NS"), ("Dimensions.member.1.Name", "Host")],
    )
    .await;
    let body = body_of(&resp);
    assert!(
        body.contains("<MetricName>M1</MetricName>"),
        "M1 (has dimension Host) expected: {body}"
    );
    assert!(
        !body.contains("<MetricName>M2</MetricName>"),
        "M2 (no Host dimension) must be filtered out: {body}"
    );
}

#[tokio::test]
async fn get_metric_statistics_json_roundtrip() {
    let svc = service();
    // Publish a couple of points via JSON at a fixed timestamp so both land in
    // the same 60s bucket deterministically.
    for v in [10.0, 20.0] {
        call_json(
            &svc,
            "PutMetricData",
            serde_json::json!({
                "Namespace": "MyApp",
                "MetricData": [{
                    "MetricName": "Latency",
                    "Value": v,
                    "Timestamp": "2020-06-01T12:00:00Z"
                }]
            }),
        )
        .await;
    }
    let resp = call_json(
        &svc,
        "GetMetricStatistics",
        serde_json::json!({
            "Namespace": "MyApp",
            "MetricName": "Latency",
            "StartTime": "2000-01-01T00:00:00Z",
            "EndTime": "2100-01-01T00:00:00Z",
            "Period": 60,
            "Statistics": ["Average", "SampleCount"]
        }),
    )
    .await;
    assert_eq!(resp.content_type, "application/x-amz-json-1.0");
    let v = json_body(&resp);
    assert_eq!(v["Label"], "Latency");
    // Average and SampleCount come back as JSON numbers, timestamp as epoch.
    let dp = &v["Datapoints"][0];
    assert_eq!(dp["Average"].as_f64(), Some(15.0));
    assert_eq!(dp["SampleCount"].as_f64(), Some(2.0));
    assert!(dp["Timestamp"].is_number());
}

#[tokio::test]
async fn put_metric_alarm_json_roundtrip() {
    let svc = service();
    let resp = call_json(
        &svc,
        "PutMetricAlarm",
        serde_json::json!({
            "AlarmName": "cpu-high",
            "ComparisonOperator": "GreaterThanThreshold",
            "EvaluationPeriods": 2,
            "Threshold": 80.0,
            "MetricName": "CPUUtilization",
            "Namespace": "AWS/EC2",
            "Period": 60,
            "Statistic": "Average"
        }),
    )
    .await;
    assert_eq!(resp.content_type, "application/x-amz-json-1.0");
    assert_eq!(json_body(&resp), serde_json::json!({}));

    let desc = call_json(
        &svc,
        "DescribeAlarms",
        serde_json::json!({"AlarmNames": ["cpu-high"]}),
    )
    .await;
    let v = json_body(&desc);
    let alarm = &v["MetricAlarms"][0];
    assert_eq!(alarm["AlarmName"], "cpu-high");
    assert_eq!(alarm["Threshold"].as_f64(), Some(80.0));
    assert_eq!(alarm["EvaluationPeriods"], 2);
    assert_eq!(alarm["ActionsEnabled"], true);
}

#[tokio::test]
async fn put_metric_alarm_persists_metrics_and_tags() {
    let svc = service();
    // Metric-math alarm with a Metrics list and inline Tags.
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "math-alarm"),
            ("ComparisonOperator", "GreaterThanThreshold"),
            ("EvaluationPeriods", "1"),
            ("Threshold", "10"),
            ("Metrics.member.1.Id", "e1"),
            ("Metrics.member.1.Expression", "m1 + m2"),
            ("Metrics.member.1.Label", "sum"),
            ("Metrics.member.1.ReturnData", "true"),
            ("Metrics.member.2.Id", "m1"),
            ("Metrics.member.2.MetricStat.Metric.Namespace", "AWS/EC2"),
            (
                "Metrics.member.2.MetricStat.Metric.MetricName",
                "CPUUtilization",
            ),
            (
                "Metrics.member.2.MetricStat.Metric.Dimensions.member.1.Name",
                "InstanceId",
            ),
            (
                "Metrics.member.2.MetricStat.Metric.Dimensions.member.1.Value",
                "i-123",
            ),
            ("Metrics.member.2.MetricStat.Period", "300"),
            ("Metrics.member.2.MetricStat.Stat", "Average"),
            ("Metrics.member.2.ReturnData", "false"),
            ("Tags.member.1.Key", "team"),
            ("Tags.member.1.Value", "obs"),
        ],
    )
    .await;

    let desc = call(
        &svc,
        "DescribeAlarms",
        &[("AlarmNames.member.1", "math-alarm")],
    )
    .await;
    let xml = body_of(&desc);
    assert!(xml.contains("<Metrics>"), "Metrics echoed: {xml}");
    assert!(xml.contains("<Id>e1</Id>"));
    assert!(xml.contains("<Expression>m1 + m2</Expression>"));
    assert!(xml.contains("<MetricName>CPUUtilization</MetricName>"));
    assert!(xml.contains("<Stat>Average</Stat>"));
    assert!(xml.contains("<Value>i-123</Value>"));

    // Inline Tags are queryable via ListTagsForResource.
    let arn = format!("arn:aws:cloudwatch:{REGION}:{ACCT}:alarm:math-alarm");
    let tags = call(&svc, "ListTagsForResource", &[("ResourceARN", &arn)]).await;
    let tag_xml = body_of(&tags);
    assert!(tag_xml.contains("<Key>team</Key>"), "tags: {tag_xml}");
    assert!(tag_xml.contains("<Value>obs</Value>"));
}

/// AWS ignores the inline `Tags` param when PutMetricAlarm updates an existing
/// alarm; tags are only applied on create. TagResource/UntagResource manage
/// tags on an existing alarm.
#[tokio::test]
async fn put_metric_alarm_update_ignores_inline_tags() {
    let svc = service();
    let base = &[
        ("AlarmName", "cpu"),
        ("ComparisonOperator", "GreaterThanThreshold"),
        ("EvaluationPeriods", "1"),
        ("Threshold", "10"),
        ("MetricName", "CPUUtilization"),
        ("Namespace", "AWS/EC2"),
    ];
    // Create with an inline tag.
    let mut create = base.to_vec();
    create.push(("Tags.member.1.Key", "team"));
    create.push(("Tags.member.1.Value", "obs"));
    call(&svc, "PutMetricAlarm", &create).await;

    // Update the SAME alarm with different inline tags -> must be ignored.
    let mut update = base.to_vec();
    update.push(("Threshold", "20"));
    update.push(("Tags.member.1.Key", "team"));
    update.push(("Tags.member.1.Value", "changed"));
    update.push(("Tags.member.2.Key", "env"));
    update.push(("Tags.member.2.Value", "prod"));
    call(&svc, "PutMetricAlarm", &update).await;

    let arn = format!("arn:aws:cloudwatch:{REGION}:{ACCT}:alarm:cpu");
    let tags = call(&svc, "ListTagsForResource", &[("ResourceARN", &arn)]).await;
    let tag_xml = body_of(&tags);
    // Original create-time tag survives unchanged; update tags are ignored.
    assert!(tag_xml.contains("<Value>obs</Value>"), "tags: {tag_xml}");
    assert!(
        !tag_xml.contains("<Value>changed</Value>"),
        "update inline tag must be ignored: {tag_xml}"
    );
    assert!(
        !tag_xml.contains("<Key>env</Key>"),
        "update inline tag must be ignored: {tag_xml}"
    );
}

#[tokio::test]
async fn put_anomaly_detector_persists_configuration() {
    let svc = service();
    call(
        &svc,
        "PutAnomalyDetector",
        &[
            ("Namespace", "AWS/EC2"),
            ("MetricName", "CPU"),
            ("Stat", "Average"),
            ("Configuration.MetricTimezone", "America/New_York"),
            (
                "Configuration.ExcludedTimeRanges.member.1.StartTime",
                "2020-01-01T00:00:00Z",
            ),
            (
                "Configuration.ExcludedTimeRanges.member.1.EndTime",
                "2020-01-02T00:00:00Z",
            ),
            ("MetricCharacteristics.PeriodicSpikes", "true"),
        ],
    )
    .await;

    let desc = call(
        &svc,
        "DescribeAnomalyDetectors",
        &[("Namespace", "AWS/EC2")],
    )
    .await;
    let xml = body_of(&desc);
    assert!(
        xml.contains("<MetricTimezone>America/New_York</MetricTimezone>"),
        "config echoed: {xml}"
    );
    assert!(xml.contains("<ExcludedTimeRanges>"));
    assert!(xml.contains("<StartTime>2020-01-01T00:00:00Z</StartTime>"));
    assert!(xml.contains("<PeriodicSpikes>true</PeriodicSpikes>"));
}
