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
    assert!(body_of(&initial).contains("<Status>Stopped</Status>"));

    call(&svc, "StartOTelEnrichment", &[]).await;
    let running = body_of(&call(&svc, "GetOTelEnrichment", &[]).await);
    assert!(running.contains("<Status>Running</Status>"));
    assert!(running.contains("<CreatedAt>"));
    assert!(!running.contains("IncludeFilters"));

    call(&svc, "StopOTelEnrichment", &[]).await;
    let stopped = body_of(&call(&svc, "GetOTelEnrichment", &[]).await);
    assert!(stopped.contains("<Status>Stopped</Status>"));
    assert!(!stopped.contains("CreatedAt"));
}

#[tokio::test]
async fn otel_enrichment_filters_stored_and_replaced() {
    let svc = service();
    // Update before start is ResourceNotFound.
    let err = call_err(&svc, "UpdateOTelEnrichment", &[]).await;
    assert_eq!(err.code(), "ResourceNotFoundException");

    let started = body_of(
        &call(
            &svc,
            "StartOTelEnrichment",
            &[
                ("IncludeFilters.member.1.Namespace", "AWS/EC2"),
                (
                    "IncludeFilters.member.1.MetricNames.member.1",
                    "CPUUtilization",
                ),
                ("ExcludeFilters.member.1.Namespace", "AWS/RDS"),
            ],
        )
        .await,
    );
    assert!(started.contains("<IncludeFilters><member><Namespace>AWS/EC2</Namespace><MetricNames><member>CPUUtilization</member></MetricNames></member></IncludeFilters>"));
    assert!(started.contains(
        "<ExcludeFilters><member><Namespace>AWS/RDS</Namespace></member></ExcludeFilters>"
    ));

    // Starting again is a no-op that keeps the stored filters.
    let again = body_of(
        &call(
            &svc,
            "StartOTelEnrichment",
            &[("IncludeFilters.member.1.Namespace", "AWS/Lambda")],
        )
        .await,
    );
    assert!(again.contains("AWS/EC2"));
    assert!(!again.contains("AWS/Lambda"));

    // Update replaces the pair: specifying only IncludeFilters clears excludes.
    let updated = body_of(
        &call(
            &svc,
            "UpdateOTelEnrichment",
            &[("IncludeFilters.member.1.Namespace", "AWS/Lambda")],
        )
        .await,
    );
    assert!(updated.contains("AWS/Lambda"));
    assert!(!updated.contains("ExcludeFilters"));
    assert!(updated.contains("<UpdatedAt>"));
    let got = body_of(&call(&svc, "GetOTelEnrichment", &[]).await);
    assert!(got.contains("AWS/Lambda") && !got.contains("AWS/RDS"));
}

#[tokio::test]
async fn otel_enrichment_rejects_too_many_filters() {
    let svc = service();
    let keys: Vec<String> = (1..=101)
        .map(|i| format!("IncludeFilters.member.{i}.Namespace"))
        .collect();
    let params: Vec<(&str, &str)> = keys.iter().map(|k| (k.as_str(), "NS")).collect();
    let err = call_err(&svc, "StartOTelEnrichment", &params).await;
    assert_eq!(err.code(), "ValidationError");

    let name_keys: Vec<String> = (1..=101)
        .map(|i| format!("IncludeFilters.member.1.MetricNames.member.{i}"))
        .collect();
    let mut params: Vec<(&str, &str)> = name_keys.iter().map(|k| (k.as_str(), "m")).collect();
    params.push(("IncludeFilters.member.1.Namespace", "NS"));
    let err = call_err(&svc, "StartOTelEnrichment", &params).await;
    assert_eq!(err.code(), "ValidationError");
}

#[tokio::test]
async fn resource_metrics_configuration_lifecycle() {
    let svc = service();
    let arn = "arn:aws:ec2:us-east-1:123456789012:instance/i-0123456789abcdef0";
    let err = call_err(
        &svc,
        "GetResourceMetricsConfiguration",
        &[("ResourceArn", arn)],
    )
    .await;
    assert_eq!(err.code(), "ResourceNotFoundException");

    let created = body_of(
        &call(
            &svc,
            "CreateResourceMetricsConfiguration",
            &[
                ("ResourceArn", arn),
                (
                    "MetricSelections.member.1.IncludeMetrics.member.1",
                    "CPUUtilization",
                ),
                (
                    "MetricSelections.member.1.IncludeMetrics.member.2",
                    "NetworkIn",
                ),
            ],
        )
        .await,
    );
    assert!(created.contains(&format!("<ResourceArn>{arn}</ResourceArn>")));
    assert!(created.contains(
        "<MetricSelections><member><IncludeMetrics><member>CPUUtilization</member><member>NetworkIn</member></IncludeMetrics></member></MetricSelections>"
    ));

    let err = call_err(
        &svc,
        "CreateResourceMetricsConfiguration",
        &[("ResourceArn", arn)],
    )
    .await;
    assert_eq!(err.code(), "ConflictException");

    // Update without selections clears the filter (collect everything).
    let updated = body_of(
        &call(
            &svc,
            "UpdateResourceMetricsConfiguration",
            &[("ResourceArn", arn)],
        )
        .await,
    );
    assert!(!updated.contains("MetricSelections"));
    let got = body_of(
        &call(
            &svc,
            "GetResourceMetricsConfiguration",
            &[("ResourceArn", arn)],
        )
        .await,
    );
    assert!(!got.contains("MetricSelections") && got.contains("<UpdatedAt>"));

    call(
        &svc,
        "DeleteResourceMetricsConfiguration",
        &[("ResourceArn", arn)],
    )
    .await;
    let err = call_err(
        &svc,
        "DeleteResourceMetricsConfiguration",
        &[("ResourceArn", arn)],
    )
    .await;
    assert_eq!(err.code(), "ResourceNotFoundException");
    let err = call_err(
        &svc,
        "UpdateResourceMetricsConfiguration",
        &[("ResourceArn", arn)],
    )
    .await;
    assert_eq!(err.code(), "ResourceNotFoundException");
}

#[tokio::test]
async fn resource_metrics_configuration_validates_input() {
    let svc = service();
    let err = call_err(
        &svc,
        "CreateResourceMetricsConfiguration",
        &[("ResourceArn", "not-an-arn-but-long-enough")],
    )
    .await;
    assert_eq!(err.code(), "ValidationError");
    let err = call_err(&svc, "CreateResourceMetricsConfiguration", &[]).await;
    assert_eq!(err.code(), "MissingParameter");
    let arn = "arn:aws:ec2:us-east-1:123456789012:instance/i-1";
    let err = call_err(
        &svc,
        "CreateResourceMetricsConfiguration",
        &[
            ("ResourceArn", arn),
            ("MetricSelections.member.1.IncludeMetrics.member.1", "a"),
            ("MetricSelections.member.2.IncludeMetrics.member.1", "b"),
        ],
    )
    .await;
    assert_eq!(err.code(), "ValidationError");
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
async fn get_metric_statistics_rejects_start_ge_end() {
    // AWS rejects StartTime >= EndTime with InvalidParameterValue.
    let svc = service();
    let err = call_err(
        &svc,
        "GetMetricStatistics",
        &[
            ("Namespace", "MyApp"),
            ("MetricName", "Latency"),
            ("StartTime", "2020-06-01T12:00:00Z"),
            ("EndTime", "2020-06-01T12:00:00Z"),
            ("Period", "60"),
            ("Statistics.member.1", "Average"),
        ],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue");
}

#[tokio::test]
async fn get_metric_statistics_rejects_non_multiple_of_60_period() {
    // Period must be 1/5/10/30 or a positive multiple of 60; 7 is invalid.
    let svc = service();
    let err = call_err(
        &svc,
        "GetMetricStatistics",
        &[
            ("Namespace", "MyApp"),
            ("MetricName", "Latency"),
            ("StartTime", "2020-06-01T12:00:00Z"),
            ("EndTime", "2020-06-01T13:00:00Z"),
            ("Period", "7"),
            ("Statistics.member.1", "Average"),
        ],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue");
}

#[tokio::test]
async fn get_metric_statistics_accepts_valid_range_and_period() {
    // A valid StartTime<EndTime with a 60s Period (and a high-resolution 10s
    // value) succeeds.
    let svc = service();
    for period in ["60", "10"] {
        call(
            &svc,
            "GetMetricStatistics",
            &[
                ("Namespace", "MyApp"),
                ("MetricName", "Latency"),
                ("StartTime", "2020-06-01T12:00:00Z"),
                ("EndTime", "2020-06-01T13:00:00Z"),
                ("Period", period),
                ("Statistics.member.1", "Average"),
            ],
        )
        .await;
    }
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

// ---------------------------------------------------------------------
// Log alarms
// ---------------------------------------------------------------------

fn log_alarm_params<'a>(name: &'a str, extra: &[(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
    let mut p: Vec<(&str, &str)> = vec![
        ("AlarmName", name),
        (
            "ScheduledQueryConfiguration.QueryString",
            "fields @message | filter @message like /ERROR/",
        ),
        (
            "ScheduledQueryConfiguration.ScheduledQueryRoleARN",
            "arn:aws:iam::123456789012:role/cw-query",
        ),
        (
            "ScheduledQueryConfiguration.ScheduleConfiguration.ScheduleExpression",
            "rate(5 minutes)",
        ),
        (
            "ScheduledQueryConfiguration.AggregationExpression",
            "count(*)",
        ),
        ("QueryResultsToEvaluate", "3"),
        ("QueryResultsToAlarm", "2"),
        ("Threshold", "10"),
        ("ComparisonOperator", "GreaterThanThreshold"),
    ];
    p.extend_from_slice(extra);
    p
}

#[tokio::test]
async fn put_log_alarm_round_trips_through_describe_alarms() {
    let svc = service();
    call(
        &svc,
        "PutLogAlarm",
        &log_alarm_params(
            "errors-alarm",
            &[
                ("AlarmDescription", "too many errors"),
                (
                    "ScheduledQueryConfiguration.LogGroupIdentifiers.member.1",
                    "/aws/lambda/fn",
                ),
                (
                    "AlarmActions.member.1",
                    "arn:aws:sns:us-east-1:123456789012:ops",
                ),
                ("TreatMissingData", "notBreaching"),
            ],
        ),
    )
    .await;

    // A log alarm is only returned when AlarmTypes asks for it: the default is
    // metric alarms only.
    let default_body = body_of(&call(&svc, "DescribeAlarms", &[]).await);
    assert!(!default_body.contains("errors-alarm"), "{default_body}");

    let body = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmTypes.member.1", "LogAlarm")],
        )
        .await,
    );
    assert!(body.contains("<LogAlarms>"), "{body}");
    assert!(
        body.contains("<AlarmName>errors-alarm</AlarmName>"),
        "{body}"
    );
    assert!(body.contains("<AlarmDescription>too many errors</AlarmDescription>"));
    assert!(body.contains("<QueryResultsToEvaluate>3</QueryResultsToEvaluate>"));
    assert!(body.contains("<QueryResultsToAlarm>2</QueryResultsToAlarm>"));
    assert!(body.contains("<ComparisonOperator>GreaterThanThreshold</ComparisonOperator>"));
    assert!(body.contains("<TreatMissingData>notBreaching</TreatMissingData>"));
    assert!(body.contains("<ScheduleExpression>rate(5 minutes)</ScheduleExpression>"));
    assert!(body.contains("<member>/aws/lambda/fn</member>"));
    assert!(body.contains(
        "<AlarmArn>arn:aws:cloudwatch:us-east-1:123456789012:alarm:errors-alarm</AlarmArn>"
    ));
    // A fresh alarm has not been evaluated yet.
    assert!(body.contains("<StateValue>INSUFFICIENT_DATA</StateValue>"));

    // DeleteAlarms removes it alongside metric and composite alarms.
    call(
        &svc,
        "DeleteAlarms",
        &[("AlarmNames.member.1", "errors-alarm")],
    )
    .await;
    let body = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmTypes.member.1", "LogAlarm")],
        )
        .await,
    );
    assert!(!body.contains("errors-alarm"), "{body}");
}

#[tokio::test]
async fn put_log_alarm_update_preserves_alarm_state() {
    let svc = service();
    call(&svc, "PutLogAlarm", &log_alarm_params("stateful", &[])).await;
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "stateful"),
            ("StateValue", "ALARM"),
            ("StateReason", "manual"),
        ],
    )
    .await;

    // Re-putting the configuration must not reset the alarm's state.
    call(
        &svc,
        "PutLogAlarm",
        &log_alarm_params("stateful", &[("Threshold", "99")]),
    )
    .await;
    let body = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmTypes.member.1", "LogAlarm")],
        )
        .await,
    );
    assert!(body.contains("<Threshold>99</Threshold>"), "{body}");
}

#[tokio::test]
async fn put_log_alarm_validates_required_and_bounded_members() {
    let svc = service();

    // Each required member is enforced.
    for drop_key in [
        "AlarmName",
        "ScheduledQueryConfiguration.QueryString",
        "ScheduledQueryConfiguration.ScheduledQueryRoleARN",
        "ScheduledQueryConfiguration.ScheduleConfiguration.ScheduleExpression",
        "ScheduledQueryConfiguration.AggregationExpression",
        "QueryResultsToEvaluate",
        "QueryResultsToAlarm",
        "Threshold",
        "ComparisonOperator",
    ] {
        let params: Vec<(&str, &str)> = log_alarm_params("req-check", &[])
            .into_iter()
            .filter(|(k, _)| *k != drop_key)
            .collect();
        let err = call_err(&svc, "PutLogAlarm", &params).await;
        assert_eq!(
            err.status().as_u16(),
            400,
            "missing {drop_key} should be rejected"
        );
    }

    // QueryResultsToEvaluate / QueryResultsToAlarm are min 1, and an alarm
    // cannot need more results to alarm than it evaluates.
    for (k, v) in [
        ("QueryResultsToEvaluate", "0"),
        ("QueryResultsToAlarm", "0"),
    ] {
        let err = call_err(&svc, "PutLogAlarm", &log_alarm_params("bounds", &[(k, v)])).await;
        assert_eq!(err.code(), "ValidationError", "{k}={v}");
    }
    let err = call_err(
        &svc,
        "PutLogAlarm",
        &log_alarm_params("bounds", &[("QueryResultsToAlarm", "9")]),
    )
    .await;
    assert_eq!(err.code(), "ValidationError");

    // ComparisonOperator and TreatMissingData are enum-bound.
    let err = call_err(
        &svc,
        "PutLogAlarm",
        &log_alarm_params("enums", &[("ComparisonOperator", "NotAnOperator")]),
    )
    .await;
    assert_eq!(err.code(), "ValidationError");
    let err = call_err(
        &svc,
        "PutLogAlarm",
        &log_alarm_params("enums", &[("TreatMissingData", "nope")]),
    )
    .await;
    assert_eq!(err.code(), "ValidationError");
}

#[tokio::test]
async fn put_log_alarm_accepts_an_empty_query_string() {
    // `QueryString` is `@length(min: 0)`: an empty value is present, not
    // missing, and is stored as sent.
    let svc = service();
    let params: Vec<(&str, &str)> = log_alarm_params("empty-query", &[])
        .into_iter()
        .map(|(k, v)| {
            if k == "ScheduledQueryConfiguration.QueryString" {
                (k, "")
            } else {
                (k, v)
            }
        })
        .collect();
    call(&svc, "PutLogAlarm", &params).await;
    let body = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmTypes.member.1", "LogAlarm")],
        )
        .await,
    );
    assert!(
        body.contains("<AlarmName>empty-query</AlarmName>"),
        "{body}"
    );
    assert!(body.contains("<QueryString></QueryString>"), "{body}");
}

#[tokio::test]
async fn describe_alarms_accepts_the_log_alarm_type() {
    // `AlarmType` gained `LogAlarm`; DescribeAlarms used to reject it as an
    // invalid AlarmTypes value.
    let svc = service();
    call(
        &svc,
        "DescribeAlarms",
        &[("AlarmTypes.member.1", "LogAlarm")],
    )
    .await;
    let err = call_err(
        &svc,
        "DescribeAlarms",
        &[("AlarmTypes.member.1", "NotAnAlarmType")],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue");
}

#[tokio::test]
async fn china_region_alarm_and_dashboard_arns_use_the_aws_cn_partition() {
    let svc = service();
    let in_china = |action: &str, params: &[(&str, &str)]| AwsRequest {
        region: "cn-north-1".to_string(),
        ..req(action, params)
    };
    svc.handle(in_china(
        "PutMetricAlarm",
        &[
            ("AlarmName", "cn-alarm"),
            ("Namespace", "Test/App"),
            ("MetricName", "CPU"),
            ("ComparisonOperator", "GreaterThanThreshold"),
            ("Threshold", "50"),
            ("EvaluationPeriods", "1"),
            ("Period", "60"),
            ("Statistic", "Average"),
        ],
    ))
    .await
    .expect("put alarm");
    let desc = svc
        .handle(in_china(
            "DescribeAlarms",
            &[("AlarmNames.member.1", "cn-alarm")],
        ))
        .await
        .expect("describe alarms");
    let arn = "arn:aws-cn:cloudwatch:cn-north-1:123456789012:alarm:cn-alarm";
    assert!(body_of(&desc).contains(&format!("<AlarmArn>{arn}</AlarmArn>")));

    svc.handle(in_china(
        "PutDashboard",
        &[
            ("DashboardName", "cn-dash"),
            ("DashboardBody", "{\"widgets\":[]}"),
        ],
    ))
    .await
    .expect("put dashboard");
    let dash = svc
        .handle(in_china("GetDashboard", &[("DashboardName", "cn-dash")]))
        .await
        .expect("get dashboard");
    assert!(body_of(&dash).contains("arn:aws-cn:cloudwatch::123456789012:dashboard/cn-dash"));
}

#[tokio::test]
async fn associate_dataset_kms_key_rejects_short_arn() {
    let svc = service();
    // KmsKeyArn has a 20-character minimum in the model.
    let err = call_err(
        &svc,
        "AssociateDatasetKmsKey",
        &[("DatasetIdentifier", "ds-1"), ("KmsKeyArn", "arn:short")],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue");
}

// ---------------------------------------------------------------------------
// awsQueryCompatible error header + rpcv2Cbor responses.
// ---------------------------------------------------------------------------

fn query_error_header(err: &AwsServiceError) -> Option<String> {
    err.response_headers()
        .iter()
        .find(|(k, _)| k == "x-amzn-query-error")
        .map(|(_, v)| v.clone())
}

#[tokio::test]
async fn json_and_cbor_errors_carry_query_error_header() {
    let svc = service();
    let json_err = match svc
        .handle(json_req(
            "GetDashboard",
            serde_json::json!({"DashboardName": "ghost"}),
        ))
        .await
    {
        Ok(_) => panic!("expected GetDashboard to fail"),
        Err(e) => e,
    };
    assert_eq!(json_err.code(), "ResourceNotFound");
    assert_eq!(
        query_error_header(&json_err).as_deref(),
        Some("ResourceNotFound;Sender")
    );

    let mut cbor = json_req(
        "GetDashboard",
        serde_json::json!({"DashboardName": "ghost"}),
    );
    cbor.headers.remove("x-amz-target");
    cbor.headers
        .insert("smithy-protocol", "rpc-v2-cbor".parse().unwrap());
    let cbor_err = match svc.handle(cbor).await {
        Ok(_) => panic!("expected GetDashboard to fail"),
        Err(e) => e,
    };
    assert_eq!(
        query_error_header(&cbor_err).as_deref(),
        Some("ResourceNotFound;Sender")
    );

    // Query-protocol callers get the plain awsQuery error, no extra header.
    let query_err = call_err(&svc, "GetDashboard", &[("DashboardName", "ghost")]).await;
    assert_eq!(query_error_header(&query_err), None);
}

#[tokio::test]
async fn cbor_request_gets_cbor_response() {
    let svc = service();
    let mut put = json_req(
        "PutMetricData",
        serde_json::json!({
            "Namespace": "App",
            "MetricData": [{"MetricName": "Latency", "Value": 5.0}]
        }),
    );
    put.headers.remove("x-amz-target");
    put.headers
        .insert("smithy-protocol", "rpc-v2-cbor".parse().unwrap());
    let resp = svc.handle(put).await.expect("put ok");
    assert_eq!(resp.content_type, "application/cbor");

    let mut list = json_req("ListMetrics", serde_json::json!({"Namespace": "App"}));
    list.headers.remove("x-amz-target");
    list.headers
        .insert("smithy-protocol", "rpc-v2-cbor".parse().unwrap());
    let resp = svc.handle(list).await.expect("list ok");
    assert_eq!(resp.content_type, "application/cbor");
    let json = fakecloud_core::cbor::decode_to_json(resp.body.expect_bytes()).unwrap();
    assert_eq!(json["Metrics"][0]["MetricName"], "Latency");
}

/// Walk a paged CloudWatch list by `NextToken`, returning each page's body.
async fn walk_pages(
    svc: &CloudWatchService,
    action: &str,
    size_param: &str,
    size: &str,
    extra: &[(&str, &str)],
) -> Vec<String> {
    let mut pages = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut params: Vec<(&str, &str)> = extra.to_vec();
        params.push((size_param, size));
        if let Some(t) = &token {
            params.push(("NextToken", t.as_str()));
        }
        let body = body_of(&call(svc, action, &params).await);
        token = body
            .split("<NextToken>")
            .nth(1)
            .map(|s| s.split("</NextToken>").next().unwrap().to_string());
        pages.push(body);
        if token.is_none() {
            break;
        }
        assert!(pages.len() < 100, "{action} pagination does not terminate");
    }
    pages
}

#[tokio::test]
async fn list_operations_page_with_next_token() {
    let svc = service();
    for i in 0..5 {
        let name = format!("r{i}");
        call(
            &svc,
            "PutInsightRule",
            &[("RuleName", &name), ("RuleDefinition", "{\"x\":1}")],
        )
        .await;
        let stream = format!("s{i}");
        call(
            &svc,
            "PutMetricStream",
            &[
                ("Name", &stream),
                (
                    "FirehoseArn",
                    "arn:aws:firehose:us-east-1:123456789012:deliverystream/d",
                ),
                ("RoleArn", "arn:aws:iam::123456789012:role/r"),
                ("OutputFormat", "json"),
            ],
        )
        .await;
        let mute = format!("m{i}");
        call(
            &svc,
            "PutAlarmMuteRule",
            &[
                ("Name", &mute),
                ("Rule.Schedule.Expression", "cron(0 2 * * *)"),
                ("Rule.Schedule.Duration", "PT4H"),
                (
                    "MuteTargets.AlarmNames.member.1",
                    if i % 2 == 0 { "even" } else { "odd" },
                ),
            ],
        )
        .await;
        let metric = format!("Metric{i}");
        call(
            &svc,
            "PutAnomalyDetector",
            &[
                ("Namespace", "NS"),
                ("MetricName", &metric),
                ("Stat", "Average"),
            ],
        )
        .await;
    }
    for (action, size_param, tag) in [
        ("DescribeInsightRules", "MaxResults", "<Name>r"),
        ("ListMetricStreams", "MaxResults", "<Name>s"),
        ("ListAlarmMuteRules", "MaxRecords", "alarm-mute-rule/m"),
        (
            "DescribeAnomalyDetectors",
            "MaxResults",
            "<MetricName>Metric",
        ),
    ] {
        let pages = walk_pages(&svc, action, size_param, "2", &[]).await;
        assert_eq!(pages.len(), 3, "{action}: {pages:?}");
        // Some members render their identifying element more than once.
        let counts: Vec<usize> = pages.iter().map(|p| p.matches(tag).count()).collect();
        let per = counts[2];
        assert!(per > 0, "{action}: {counts:?}");
        assert_eq!(counts, vec![2 * per, 2 * per, per], "{action}");
        let err = call_err(&svc, action, &[("NextToken", "bogus")]).await;
        assert_eq!(err.code(), "InvalidNextToken", "{action}");
    }

    // `AlarmName` narrows mute rules to those that target the alarm.
    let even = body_of(&call(&svc, "ListAlarmMuteRules", &[("AlarmName", "even")]).await);
    assert_eq!(even.matches("alarm-mute-rule/m").count(), 3, "{even}");
}

// ---------------------------------------------------------------------
// ActionConfiguration / WarmUp / EvaluationCriteria / anomaly config
// ---------------------------------------------------------------------

#[tokio::test]
async fn metric_alarm_round_trips_nested_configuration_members() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "promql-alarm"),
            ("EvaluationCriteria.PromQLCriteria.Query", "up == 0"),
            ("EvaluationCriteria.PromQLCriteria.PendingPeriod", "60"),
            ("EvaluationInterval", "60"),
            ("WarmUpConfiguration.WarmUpPeriodDurationInMinutes", "15"),
            (
                "WarmUpConfiguration.OnlyStartEvaluatingAfterWarmUpPeriodEnds",
                "true",
            ),
            ("EvaluationWindow.WallClockWindow.Timezone", "Europe/Lisbon"),
            ("ActionConfiguration.AlarmTagsPropagation.Enabled", "true"),
            (
                "ActionConfiguration.AlarmTagsPropagation.IncludedTagKeys.member.1",
                "team",
            ),
            (
                "ActionConfiguration.AlarmTagsPropagation.IncludedTagKeys.member.2",
                "env",
            ),
        ],
    )
    .await;
    let body = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmNames.member.1", "promql-alarm")],
        )
        .await,
    );
    for frag in [
        "<EvaluationCriteria><PromQLCriteria><PendingPeriod>60</PendingPeriod><Query>up == 0</Query></PromQLCriteria></EvaluationCriteria>",
        "<EvaluationInterval>60</EvaluationInterval>",
        "<WarmUpConfiguration><OnlyStartEvaluatingAfterWarmUpPeriodEnds>true</OnlyStartEvaluatingAfterWarmUpPeriodEnds><WarmUpPeriodDurationInMinutes>15</WarmUpPeriodDurationInMinutes></WarmUpConfiguration>",
        "<EvaluationWindow><WallClockWindow><Timezone>Europe/Lisbon</Timezone></WallClockWindow></EvaluationWindow>",
        "<ActionConfiguration><AlarmTagsPropagation><Enabled>true</Enabled><IncludedTagKeys><member>team</member><member>env</member></IncludedTagKeys></AlarmTagsPropagation></ActionConfiguration>",
    ] {
        assert!(body.contains(frag), "missing {frag}: {body}");
    }

    // Over awsJson the nested members come back typed (bool, int, list).
    let v = json_body(
        &call_json(
            &svc,
            "DescribeAlarms",
            serde_json::json!({"AlarmNames": ["promql-alarm"]}),
        )
        .await,
    );
    let alarm = &v["MetricAlarms"][0];
    assert_eq!(
        alarm["ActionConfiguration"]["AlarmTagsPropagation"],
        serde_json::json!({"Enabled": true, "IncludedTagKeys": ["team", "env"]})
    );
    assert_eq!(
        alarm["WarmUpConfiguration"]["WarmUpPeriodDurationInMinutes"],
        15
    );
    assert_eq!(alarm["EvaluationInterval"], 60);
}

#[tokio::test]
async fn nested_configuration_members_are_validated() {
    let svc = service();
    let err = call_err(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "bad"),
            ("WarmUpConfiguration.WarmUpPeriodDurationInMinutes", "3000"),
        ],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue");
    let err = call_err(
        &svc,
        "PutCompositeAlarm",
        &[
            ("AlarmName", "bad"),
            ("AlarmRule", "TRUE"),
            (
                "ActionConfiguration.AlarmTagsPropagation.ExcludedTagKeys.member.1",
                "x",
            ),
        ],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue", "Enabled is required");
}

#[tokio::test]
async fn composite_and_log_alarms_round_trip_action_configuration() {
    let svc = service();
    call(
        &svc,
        "PutCompositeAlarm",
        &[
            ("AlarmName", "comp"),
            ("AlarmRule", "TRUE"),
            ("ActionConfiguration.AlarmTagsPropagation.Enabled", "false"),
        ],
    )
    .await;
    call(
        &svc,
        "PutLogAlarm",
        &log_alarm_params(
            "log",
            &[
                ("ActionConfiguration.AlarmTagsPropagation.Enabled", "true"),
                (
                    "ActionConfiguration.AlarmTagsPropagation.ExcludedTagKeys.member.1",
                    "secret",
                ),
                ("WarmUpConfiguration.WarmUpPeriodDurationInMinutes", "5"),
            ],
        ),
    )
    .await;
    let comp = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmTypes.member.1", "CompositeAlarm")],
        )
        .await,
    );
    assert!(
        comp.contains("<ActionConfiguration><AlarmTagsPropagation><Enabled>false</Enabled></AlarmTagsPropagation></ActionConfiguration>"),
        "{comp}"
    );
    let log = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmTypes.member.1", "LogAlarm")],
        )
        .await,
    );
    assert!(
        log.contains("<ExcludedTagKeys><member>secret</member></ExcludedTagKeys>"),
        "{log}"
    );
    assert!(
        log.contains("<WarmUpPeriodDurationInMinutes>5</WarmUpPeriodDurationInMinutes>"),
        "{log}"
    );
}

#[tokio::test]
async fn anomaly_detector_round_trips_adaptation_and_outlier_settings() {
    let svc = service();
    call(
        &svc,
        "PutAnomalyDetector",
        &[
            ("SingleMetricAnomalyDetector.Namespace", "AWS/EC2"),
            ("SingleMetricAnomalyDetector.MetricName", "CPUUtilization"),
            ("SingleMetricAnomalyDetector.Stat", "Average"),
            ("Configuration.AdaptationSpeed", "FAST"),
            ("Configuration.OutlierFilteringStrength", "STRICT"),
        ],
    )
    .await;
    let body = body_of(&call(&svc, "DescribeAnomalyDetectors", &[]).await);
    assert!(
        body.contains("<AdaptationSpeed>FAST</AdaptationSpeed>"),
        "{body}"
    );
    assert!(
        body.contains("<OutlierFilteringStrength>STRICT</OutlierFilteringStrength>"),
        "{body}"
    );
    let err = call_err(
        &svc,
        "PutAnomalyDetector",
        &[
            ("SingleMetricAnomalyDetector.Namespace", "AWS/EC2"),
            ("SingleMetricAnomalyDetector.MetricName", "CPUUtilization"),
            ("SingleMetricAnomalyDetector.Stat", "Average"),
            ("Configuration.AdaptationSpeed", "LUDICROUS"),
        ],
    )
    .await;
    assert_eq!(err.code(), "InvalidParameterValue");
}

fn metric_alarm(svc: &CloudWatchService, name: &str) -> crate::state::MetricAlarm {
    svc.state
        .read()
        .get(ACCT)
        .and_then(|a| a.alarms_in(REGION))
        .and_then(|a| a.get(name))
        .cloned()
        .unwrap()
}

// SetAlarmState stores StateReasonData as given; StateUpdatedTimestamp moves on
// every call, StateTransitionedTimestamp only when the value changes, and the
// history records old and new state with their reason data.
#[tokio::test]
async fn set_alarm_state_tracks_transitions_and_reason_data() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "t1"),
            ("Namespace", "Test/T"),
            ("MetricName", "M"),
            ("Threshold", "1"),
            ("Statistic", "Sum"),
        ],
    )
    .await;
    let created = metric_alarm(&svc, "t1");
    assert_eq!(
        created.state_transitioned_timestamp,
        Some(created.state_updated_timestamp)
    );
    let set = |value: &'static str, data: &'static str| {
        let svc = &svc;
        async move {
            call(
                svc,
                "SetAlarmState",
                &[
                    ("AlarmName", "t1"),
                    ("StateValue", value),
                    ("StateReason", "manual"),
                    ("StateReasonData", data),
                ],
            )
            .await;
        }
    };
    set("ALARM", "{\"k\":1}").await;
    let first = metric_alarm(&svc, "t1");
    assert_eq!(first.state_reason_data.as_deref(), Some("{\"k\":1}"));
    let transitioned = first.state_transitioned_timestamp.unwrap();
    assert_eq!(transitioned, first.state_updated_timestamp);
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    set("ALARM", "{\"k\":2}").await;
    let second = metric_alarm(&svc, "t1");
    assert!(second.state_updated_timestamp > first.state_updated_timestamp);
    assert_eq!(second.state_transitioned_timestamp, Some(transitioned));
    assert_eq!(second.state_reason_data.as_deref(), Some("{\"k\":2}"));

    // A re-put leaves the state and its computed outputs unchanged.
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "t1"),
            ("Namespace", "Test/T"),
            ("MetricName", "M"),
            ("Threshold", "2"),
            ("Statistic", "Sum"),
        ],
    )
    .await;
    let reput = metric_alarm(&svc, "t1");
    assert_eq!(reput.state_transitioned_timestamp, Some(transitioned));
    assert_eq!(reput.state_reason_data.as_deref(), Some("{\"k\":2}"));

    let body = body_of(&call(&svc, "DescribeAlarms", &[("AlarmNames.member.1", "t1")]).await);
    assert!(
        body.contains("<StateReasonData>{&quot;k&quot;:2}</StateReasonData>"),
        "{body}"
    );
    assert!(body.contains("<StateTransitionedTimestamp>"), "{body}");

    let hist = body_of(
        &call(
            &svc,
            "DescribeAlarmHistory",
            &[
                ("AlarmName", "t1"),
                ("HistoryItemType", "StateUpdate"),
                ("ScanBy", "TimestampAscending"),
            ],
        )
        .await,
    );
    assert!(
        hist.contains("Alarm updated from INSUFFICIENT_DATA to ALARM"),
        "{hist}"
    );
    assert!(
        hist.contains("&quot;stateReasonData&quot;:{&quot;k&quot;:2}"),
        "{hist}"
    );
}

// A metric alarm PutMetricAlarm accepts but can't evaluate reports
// EVALUATION_ERROR; fixing it clears the evaluation state.
#[tokio::test]
async fn unevaluable_metric_alarm_reports_evaluation_error() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "bad"),
            ("Namespace", "Test/T"),
            ("MetricName", "M"),
            ("Statistic", "Sum"),
        ],
    )
    .await;
    let body = body_of(&call(&svc, "DescribeAlarms", &[]).await);
    assert!(
        body.contains("<EvaluationState>EVALUATION_ERROR</EvaluationState>"),
        "{body}"
    );
    assert!(body.contains("no Threshold"), "{body}");
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "bad"),
            ("Namespace", "Test/T"),
            ("MetricName", "M"),
            ("Statistic", "Sum"),
            ("Threshold", "1"),
        ],
    )
    .await;
    let body = body_of(&call(&svc, "DescribeAlarms", &[]).await);
    assert!(!body.contains("<EvaluationState>"), "{body}");
    assert!(body.contains("<StateReasonData>"), "{body}");
}

// A composite alarm reports what suppresses its actions, and its rule
// transition carries the triggering children as reason data. Composite history
// is only returned when AlarmTypes asks for it.
#[tokio::test]
async fn composite_alarm_action_suppression_and_reason_data() {
    let svc = service();
    for name in ["child", "maint"] {
        call(
            &svc,
            "PutMetricAlarm",
            &[
                ("AlarmName", name),
                ("Namespace", "Test/C"),
                ("MetricName", name),
                ("Threshold", "1"),
                ("Statistic", "Sum"),
            ],
        )
        .await;
    }
    call(
        &svc,
        "PutCompositeAlarm",
        &[
            ("AlarmName", "comp"),
            ("AlarmRule", "ALARM(child)"),
            ("ActionsSuppressor", "maint"),
            ("ActionsSuppressorWaitPeriod", "600"),
            ("ActionsSuppressorExtensionPeriod", "600"),
        ],
    )
    .await;
    let set = |name: &'static str, value: &'static str| {
        let svc = &svc;
        async move {
            call(
                svc,
                "SetAlarmState",
                &[
                    ("AlarmName", name),
                    ("StateValue", value),
                    ("StateReason", "x"),
                ],
            )
            .await;
        }
    };
    let describe = || async {
        body_of(
            &call(
                &svc,
                "DescribeAlarms",
                &[
                    ("AlarmNames.member.1", "comp"),
                    ("AlarmTypes.member.1", "CompositeAlarm"),
                ],
            )
            .await,
        )
    };

    set("child", "ALARM").await;
    let b = describe().await;
    assert!(b.contains("<StateValue>ALARM</StateValue>"), "{b}");
    assert!(
        b.contains("<ActionsSuppressedBy>WaitPeriod</ActionsSuppressedBy>"),
        "{b}"
    );
    assert!(
        b.contains(
            "<ActionsSuppressedReason>Actions suppressed by WaitPeriod</ActionsSuppressedReason>"
        ),
        "{b}"
    );
    assert!(b.contains("triggeringAlarms"), "{b}");
    assert!(b.contains("alarm:child"), "{b}");

    set("maint", "ALARM").await;
    let b = describe().await;
    assert!(
        b.contains("<ActionsSuppressedBy>Alarm</ActionsSuppressedBy>"),
        "{b}"
    );

    set("maint", "OK").await;
    let b = describe().await;
    assert!(
        b.contains("<ActionsSuppressedBy>ExtensionPeriod</ActionsSuppressedBy>"),
        "{b}"
    );

    set("child", "OK").await;
    let b = describe().await;
    assert!(b.contains("<StateValue>OK</StateValue>"), "{b}");
    assert!(!b.contains("<ActionsSuppressedBy>"), "{b}");

    // Metric-alarm history only by default; composite history on request.
    let default_hist = body_of(&call(&svc, "DescribeAlarmHistory", &[("AlarmName", "comp")]).await);
    assert!(!default_hist.contains("<member>"), "{default_hist}");
    let comp_hist = body_of(
        &call(
            &svc,
            "DescribeAlarmHistory",
            &[
                ("AlarmName", "comp"),
                ("AlarmTypes.member.1", "CompositeAlarm"),
            ],
        )
        .await,
    );
    // Evaluated as soon as it is created (child not in ALARM yet), then
    // following the child.
    for summary in [
        "Alarm updated from INSUFFICIENT_DATA to OK",
        "Alarm updated from OK to ALARM",
        "Alarm updated from ALARM to OK",
    ] {
        assert!(comp_hist.contains(summary), "{summary}: {comp_hist}");
    }
    assert!(
        comp_hist.contains("Alarm &quot;comp&quot; created"),
        "{comp_hist}"
    );
}

// ---------------------------------------------------------------------------
// Alarm actions.
// ---------------------------------------------------------------------------

const TOPIC: &str = "arn:aws:sns:us-east-1:123456789012:alarms";

/// Records SNS publishes; topics other than [`TOPIC`] don't exist.
#[derive(Default)]
struct RecordingSns {
    published: parking_lot::Mutex<Vec<(String, String, Option<String>)>>,
}

impl fakecloud_core::delivery::SnsDelivery for RecordingSns {
    fn publish_to_topic(&self, topic_arn: &str, message: &str, subject: Option<&str>) {
        let _ = self.try_publish_to_topic(topic_arn, message, subject);
    }

    fn try_publish_to_topic(
        &self,
        topic_arn: &str,
        message: &str,
        subject: Option<&str>,
    ) -> Result<String, String> {
        if topic_arn != TOPIC {
            return Err(format!("Topic does not exist: {topic_arn}"));
        }
        self.published.lock().push((
            topic_arn.to_string(),
            message.to_string(),
            subject.map(str::to_string),
        ));
        Ok("msg-1".to_string())
    }
}

/// Records EC2 alarm actions.
#[derive(Default)]
struct RecordingEc2 {
    performed: Arc<parking_lot::Mutex<Vec<(String, crate::Ec2InstanceAction)>>>,
}

impl crate::Ec2AlarmActionHook for RecordingEc2 {
    fn perform(
        &self,
        _account_id: &str,
        _region: &str,
        instance_id: &str,
        action: crate::Ec2InstanceAction,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>> {
        self.performed
            .lock()
            .push((instance_id.to_string(), action));
        Box::pin(async { Ok(()) })
    }
}

type Performed = Arc<parking_lot::Mutex<Vec<(String, crate::Ec2InstanceAction)>>>;

fn acting_service() -> (CloudWatchService, Arc<RecordingSns>, Performed) {
    let sns = Arc::new(RecordingSns::default());
    let ec2 = RecordingEc2::default();
    let performed = ec2.performed.clone();
    let bus = fakecloud_core::delivery::DeliveryBus::new().with_sns(sns.clone());
    let svc = service()
        .with_delivery(Arc::new(bus))
        .with_ec2_actions(Arc::new(ec2));
    (svc, sns, performed)
}

async fn history_of(svc: &CloudWatchService, name: &str, alarm_type: &str) -> String {
    body_of(
        &call(
            svc,
            "DescribeAlarmHistory",
            &[
                ("AlarmName", name),
                ("AlarmTypes.member.1", alarm_type),
                ("HistoryItemType", "Action"),
            ],
        )
        .await,
    )
}

#[tokio::test]
async fn metric_alarm_publishes_notification_on_transition() {
    let (svc, sns, _) = acting_service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "cpu"),
            ("AlarmDescription", "CPU high"),
            ("Namespace", "AWS/EC2"),
            ("MetricName", "CPUUtilization"),
            ("Statistic", "Average"),
            ("Period", "60"),
            ("Threshold", "80"),
            ("Dimensions.member.1.Name", "InstanceId"),
            ("Dimensions.member.1.Value", "i-123"),
            ("AlarmActions.member.1", TOPIC),
            (
                "AlarmActions.member.2",
                "arn:aws:sns:us-east-1:123456789012:missing",
            ),
        ],
    )
    .await;
    call(
        &svc,
        "PutMetricData",
        &[
            ("Namespace", "AWS/EC2"),
            ("MetricData.member.1.MetricName", "CPUUtilization"),
            ("MetricData.member.1.Value", "95"),
            ("MetricData.member.1.Dimensions.member.1.Name", "InstanceId"),
            ("MetricData.member.1.Dimensions.member.1.Value", "i-123"),
        ],
    )
    .await;
    let published = sns.published.lock().clone();
    assert_eq!(published.len(), 1, "{published:?}");
    let (_, message, subject) = &published[0];
    assert_eq!(
        subject.as_deref(),
        Some("ALARM: \"cpu\" in US East (N. Virginia)")
    );
    let m: serde_json::Value = serde_json::from_str(message).unwrap();
    assert_eq!(m["AlarmName"], "cpu");
    assert_eq!(m["AlarmDescription"], "CPU high");
    assert_eq!(m["AWSAccountId"], ACCT);
    assert_eq!(m["NewStateValue"], "ALARM");
    assert_eq!(m["OldStateValue"], "INSUFFICIENT_DATA");
    assert_eq!(m["Region"], "US East (N. Virginia)");
    assert_eq!(
        m["AlarmArn"],
        "arn:aws:cloudwatch:us-east-1:123456789012:alarm:cpu"
    );
    assert_eq!(m["Trigger"]["MetricName"], "CPUUtilization");
    assert_eq!(m["Trigger"]["Statistic"], "AVERAGE");
    assert_eq!(m["Trigger"]["Threshold"], 80.0);
    assert_eq!(
        m["Trigger"]["Dimensions"],
        serde_json::json!([{"value": "i-123", "name": "InstanceId"}])
    );

    // Both actions are recorded; the missing topic as a failure.
    let hist = history_of(&svc, "cpu", "MetricAlarm").await;
    assert!(
        hist.contains(&format!("Successfully executed action {TOPIC}")),
        "{hist}"
    );
    assert!(
        hist.contains("Failed to execute action arn:aws:sns:us-east-1:123456789012:missing"),
        "{hist}"
    );

    // Same state again: no new actions.
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "cpu"),
            ("StateValue", "ALARM"),
            ("StateReason", "x"),
        ],
    )
    .await;
    assert_eq!(sns.published.lock().len(), 1);
}

#[tokio::test]
async fn disabled_actions_and_unlisted_states_do_not_fire() {
    let (svc, sns, _) = acting_service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "quiet"),
            ("Namespace", "N"),
            ("MetricName", "M"),
            ("Statistic", "Sum"),
            ("Threshold", "1"),
            ("ActionsEnabled", "false"),
            ("AlarmActions.member.1", TOPIC),
        ],
    )
    .await;
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "quiet"),
            ("StateValue", "ALARM"),
            ("StateReason", "x"),
        ],
    )
    .await;
    // OK has no actions listed.
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "loud"),
            ("Namespace", "N"),
            ("MetricName", "M"),
            ("Statistic", "Sum"),
            ("Threshold", "1"),
            ("AlarmActions.member.1", TOPIC),
        ],
    )
    .await;
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "loud"),
            ("StateValue", "OK"),
            ("StateReason", "x"),
        ],
    )
    .await;
    assert!(sns.published.lock().is_empty());
    let hist = history_of(&svc, "quiet", "MetricAlarm").await;
    assert!(!hist.contains("<member>"), "{hist}");
}

#[tokio::test]
async fn ec2_action_acts_on_the_instance_dimension() {
    let (svc, _, performed) = acting_service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "stop-it"),
            ("Namespace", "AWS/EC2"),
            ("MetricName", "CPUUtilization"),
            ("Statistic", "Average"),
            ("Threshold", "80"),
            ("Dimensions.member.1.Name", "InstanceId"),
            ("Dimensions.member.1.Value", "i-abc"),
            (
                "AlarmActions.member.1",
                "arn:aws:automate:us-east-1:ec2:stop",
            ),
            (
                "OKActions.member.1",
                "arn:aws:swf:us-east-1:123456789012:action/actions/AWS_EC2.InstanceId.Reboot/1.0",
            ),
        ],
    )
    .await;
    for value in ["ALARM", "OK"] {
        call(
            &svc,
            "SetAlarmState",
            &[
                ("AlarmName", "stop-it"),
                ("StateValue", value),
                ("StateReason", "x"),
            ],
        )
        .await;
    }
    assert_eq!(
        performed.lock().clone(),
        vec![
            ("i-abc".to_string(), crate::Ec2InstanceAction::Stop),
            ("i-abc".to_string(), crate::Ec2InstanceAction::Reboot),
        ]
    );
    let hist = history_of(&svc, "stop-it", "MetricAlarm").await;
    assert!(
        hist.contains("Successfully executed action arn:aws:automate:us-east-1:ec2:stop"),
        "{hist}"
    );
}

/// Records asynchronous Lambda invocations.
#[derive(Default)]
struct RecordingLambda {
    invoked: parking_lot::Mutex<Vec<(String, String)>>,
}

impl fakecloud_core::delivery::LambdaDelivery for RecordingLambda {
    fn invoke_lambda(
        &self,
        _function_arn: &str,
        _payload: &str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<fakecloud_core::delivery::LambdaInvocationOutput, String>,
                > + Send,
        >,
    > {
        Box::pin(async { Err("sync invoke not expected".to_string()) })
    }

    fn invoke_lambda_async(&self, function_arn: &str, payload: &str) -> Result<(), String> {
        self.invoked
            .lock()
            .push((function_arn.to_string(), payload.to_string()));
        Ok(())
    }
}

#[tokio::test]
async fn lambda_action_receives_the_alarm_event() {
    let lambda = Arc::new(RecordingLambda::default());
    let bus = fakecloud_core::delivery::DeliveryBus::new().with_lambda(lambda.clone());
    let svc = service().with_delivery(Arc::new(bus));
    let function = "arn:aws:lambda:us-east-1:123456789012:function:on-alarm";
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "fn"),
            ("Namespace", "N"),
            ("MetricName", "M"),
            ("Statistic", "Sum"),
            ("Period", "60"),
            ("Threshold", "1"),
            ("AlarmActions.member.1", function),
        ],
    )
    .await;
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "fn"),
            ("StateValue", "ALARM"),
            ("StateReason", "test"),
            ("StateReasonData", "{\"k\":1}"),
        ],
    )
    .await;
    let invoked = lambda.invoked.lock().clone();
    assert_eq!(invoked.len(), 1);
    assert_eq!(invoked[0].0, function);
    let e: serde_json::Value = serde_json::from_str(&invoked[0].1).unwrap();
    assert_eq!(e["source"], "aws.cloudwatch");
    assert_eq!(e["accountId"], ACCT);
    assert_eq!(e["region"], REGION);
    assert_eq!(e["alarmData"]["alarmName"], "fn");
    assert_eq!(e["alarmData"]["state"]["value"], "ALARM");
    assert_eq!(e["alarmData"]["state"]["reason"], "test");
    assert_eq!(e["alarmData"]["state"]["reasonData"], "{\"k\":1}");
    assert_eq!(
        e["alarmData"]["previousState"]["value"],
        "INSUFFICIENT_DATA"
    );
    assert_eq!(
        e["alarmData"]["configuration"]["metrics"][0]["metricStat"]["metric"]["name"],
        "M"
    );
    let hist = history_of(&svc, "fn", "MetricAlarm").await;
    assert!(
        hist.contains(&format!("Successfully executed action {function}")),
        "{hist}"
    );
}

// A composite whose ActionsSuppressor is in ALARM doesn't act; once the
// suppression ends while it is still in ALARM, it performs the owed actions.
#[tokio::test]
async fn suppressed_composite_acts_only_after_suppression_ends() {
    let (svc, sns, _) = acting_service();
    for name in ["child", "maint"] {
        call(
            &svc,
            "PutMetricAlarm",
            &[
                ("AlarmName", name),
                ("Namespace", "N"),
                ("MetricName", name),
                ("Statistic", "Sum"),
                ("Threshold", "1"),
            ],
        )
        .await;
    }
    call(
        &svc,
        "PutCompositeAlarm",
        &[
            ("AlarmName", "svc"),
            ("AlarmRule", "ALARM(child)"),
            ("AlarmActions.member.1", TOPIC),
            ("ActionsSuppressor", "maint"),
            ("ActionsSuppressorWaitPeriod", "0"),
            ("ActionsSuppressorExtensionPeriod", "0"),
        ],
    )
    .await;
    let set = |name: &'static str, value: &'static str| {
        let svc = &svc;
        async move {
            call(
                svc,
                "SetAlarmState",
                &[
                    ("AlarmName", name),
                    ("StateValue", value),
                    ("StateReason", "x"),
                ],
            )
            .await;
        }
    };
    set("maint", "ALARM").await;
    set("child", "ALARM").await;
    assert!(
        sns.published.lock().is_empty(),
        "suppressed composite acted"
    );
    // The suppressor leaves ALARM with no extension period: actions run.
    set("maint", "OK").await;
    let published = sns.published.lock().clone();
    assert_eq!(published.len(), 1, "{published:?}");
    let m: serde_json::Value = serde_json::from_str(&published[0].1).unwrap();
    assert_eq!(m["NewStateValue"], "ALARM");
    assert_eq!(m["AlarmRule"], "ALARM(child)");
    assert!(m["TriggeringChildren"][0]["Arn"]
        .as_str()
        .unwrap()
        .ends_with(":alarm:child"));
    let hist = history_of(&svc, "svc", "CompositeAlarm").await;
    assert!(hist.contains("Successfully executed action"), "{hist}");
    // Nothing more is owed.
    call(&svc, "DescribeAlarms", &[]).await;
    assert_eq!(sns.published.lock().len(), 1);
}

// The wait period holds the actions back until it runs out.
#[test]
fn composite_actions_run_when_the_wait_period_ends() {
    use crate::state::{AlarmState, CompositeAlarm};
    let mut st = crate::state::CloudWatchState::new(ACCT);
    let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let composite = CompositeAlarm {
        alarm_name: "svc".into(),
        alarm_arn: crate::state::alarm_arn(REGION, ACCT, "svc"),
        alarm_description: None,
        alarm_rule: "TRUE".into(),
        actions_enabled: true,
        ok_actions: vec![],
        alarm_actions: vec![TOPIC.into()],
        insufficient_data_actions: vec![],
        actions_suppressor: Some("maint".into()),
        actions_suppressor_wait_period: Some(60),
        actions_suppressor_extension_period: Some(60),
        state_value: AlarmState::InsufficientData,
        state_reason: String::new(),
        state_updated_timestamp: t0,
        alarm_configuration_updated_timestamp: t0,
        action_configuration: None,
        state_reason_data: None,
        state_transitioned_timestamp: Some(t0),
        actions_suppressed_by: None,
        actions_suppressed_reason: None,
        state_manually_set: false,
        pending_action_from: None,
        alarm_exit: crate::state::AlarmExit::new(),
    };
    st.composite_alarms_in_mut(REGION)
        .insert("svc".into(), composite);
    let first = crate::alarm_eval::evaluate_alarms(&mut st, REGION, t0);
    assert!(first.notifications.is_empty());
    let c = &st.composite_alarms_in(REGION).unwrap()["svc"];
    assert_eq!(c.state_value, AlarmState::Alarm);
    assert_eq!(
        c.actions_suppressed_by,
        Some(crate::state::ActionsSuppressedBy::WaitPeriod)
    );
    let during =
        crate::alarm_eval::evaluate_alarms(&mut st, REGION, t0 + chrono::Duration::seconds(30));
    assert!(during.notifications.is_empty());
    let after =
        crate::alarm_eval::evaluate_alarms(&mut st, REGION, t0 + chrono::Duration::seconds(61));
    assert_eq!(after.notifications.len(), 1);
    assert_eq!(after.notifications[0].actions, vec![TOPIC.to_string()]);
    let again =
        crate::alarm_eval::evaluate_alarms(&mut st, REGION, t0 + chrono::Duration::seconds(90));
    assert!(again.notifications.is_empty());
}

// ---------------------------------------------------------------------------
// Review regressions.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn suppressor_periods_must_fit_32_bits() {
    let svc = service();
    for (param, value) in [
        ("ActionsSuppressorWaitPeriod", "100000000000000"),
        ("ActionsSuppressorExtensionPeriod", "-100000000000000"),
        ("ActionsSuppressorWaitPeriod", "ten"),
    ] {
        let err = call_err(
            &svc,
            "PutCompositeAlarm",
            &[
                ("AlarmName", "c"),
                ("AlarmRule", "TRUE"),
                ("ActionsSuppressor", "s"),
                (param, value),
            ],
        )
        .await;
        assert_eq!(err.code(), "InvalidParameterValue", "{param}={value}");
    }
    // An in-range maximum evaluates without overflow.
    call(
        &svc,
        "PutCompositeAlarm",
        &[
            ("AlarmName", "c"),
            ("AlarmRule", "TRUE"),
            ("ActionsSuppressor", "s"),
            ("ActionsSuppressorWaitPeriod", "2147483647"),
            ("ActionsSuppressorExtensionPeriod", "2147483647"),
        ],
    )
    .await;
    let b = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmTypes.member.1", "CompositeAlarm")],
        )
        .await,
    );
    assert!(
        b.contains("<ActionsSuppressedBy>WaitPeriod</ActionsSuppressedBy>"),
        "{b}"
    );
}

#[test]
fn suppression_status_saturates_instead_of_overflowing() {
    use crate::alarm_eval::{suppression_status, SuppressionInput};
    use crate::state::AlarmState;
    let now = chrono::Utc::now();
    let by = suppression_status(&SuppressionInput {
        composite_state: AlarmState::Alarm,
        entered_state_at: now,
        suppressor_state: Some(AlarmState::Ok),
        suppressor_left_alarm_at: Some(now),
        wait_period: i64::MAX,
        extension_period: i64::MAX,
        now,
    });
    assert_eq!(by, Some(crate::state::ActionsSuppressedBy::ExtensionPeriod));
}

#[tokio::test]
async fn set_alarm_state_rejects_non_json_reason_data() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[("AlarmName", "j"), ("Namespace", "N"), ("MetricName", "M")],
    )
    .await;
    let err = call_err(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "j"),
            ("StateValue", "ALARM"),
            ("StateReason", "x"),
            ("StateReasonData", "not json"),
        ],
    )
    .await;
    assert_eq!(err.code(), "InvalidFormat");
    assert_eq!(err.status(), http::StatusCode::BAD_REQUEST);
    // Nothing changed.
    assert_eq!(
        metric_alarm(&svc, "j").state_value,
        crate::state::AlarmState::InsufficientData
    );
}

#[tokio::test]
async fn reput_to_unevaluated_config_clears_evaluation_error() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "e"),
            ("Namespace", "N"),
            ("MetricName", "M"),
            ("Statistic", "Sum"),
        ],
    )
    .await;
    assert_eq!(
        metric_alarm(&svc, "e").evaluation_state,
        Some(crate::state::EvaluationState::EvaluationError)
    );
    // Re-put as a metric-math alarm, which this evaluator doesn't evaluate.
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "e"),
            ("Threshold", "1"),
            ("Metrics.member.1.Id", "e1"),
            ("Metrics.member.1.Expression", "SUM(METRICS())"),
        ],
    )
    .await;
    assert_eq!(metric_alarm(&svc, "e").evaluation_state, None);
}

#[tokio::test]
async fn configuration_history_carries_alarm_configs() {
    let svc = service();
    for threshold in ["1", "2"] {
        call(
            &svc,
            "PutMetricAlarm",
            &[
                ("AlarmName", "cfg"),
                ("Namespace", "N"),
                ("MetricName", "M"),
                ("Statistic", "Sum"),
                ("Threshold", threshold),
            ],
        )
        .await;
    }
    let items = {
        let state = svc.state.read();
        state.get(ACCT).unwrap().alarm_history_in(REGION).unwrap()["cfg"]
            .iter()
            .filter(|i| i.history_item_type == "ConfigurationUpdate")
            .map(|i| serde_json::from_str::<serde_json::Value>(&i.history_data).unwrap())
            .collect::<Vec<_>>()
    };
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["type"], "Create");
    assert_eq!(items[0]["createdAlarm"]["threshold"], 1.0);
    assert_eq!(items[0]["createdAlarm"]["alarmName"], "cfg");
    assert_eq!(items[1]["type"], "Update");
    assert_eq!(items[1]["originalAlarm"]["threshold"], 1.0);
    assert_eq!(items[1]["updatedAlarm"]["threshold"], 2.0);

    call(
        &svc,
        "PutCompositeAlarm",
        &[("AlarmName", "cc"), ("AlarmRule", "ALARM(cfg)")],
    )
    .await;
    let state = svc.state.read();
    let comp = &state.get(ACCT).unwrap().alarm_history_in(REGION).unwrap()["cc"][0];
    let v: serde_json::Value = serde_json::from_str(&comp.history_data).unwrap();
    assert_eq!(v["createdAlarm"]["alarmRule"], "ALARM(cfg)");
}

// The suppressor's exit from ALARM is recorded on the alarm; an alarm from an
// older snapshot (untracked) falls back to its history.
#[tokio::test]
async fn suppressor_exit_is_tracked_on_the_alarm() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[("AlarmName", "s"), ("Namespace", "N"), ("MetricName", "M")],
    )
    .await;
    for value in ["ALARM", "OK"] {
        call(
            &svc,
            "SetAlarmState",
            &[
                ("AlarmName", "s"),
                ("StateValue", value),
                ("StateReason", "x"),
            ],
        )
        .await;
    }
    let alarm = metric_alarm(&svc, "s");
    assert!(alarm.alarm_exit.tracked);
    assert_eq!(alarm.alarm_exit.at, Some(alarm.state_updated_timestamp));

    // An untracked alarm reads it from history.
    svc.state
        .write()
        .get_or_create(ACCT)
        .alarms_in_mut(REGION)
        .get_mut("s")
        .unwrap()
        .alarm_exit = crate::state::AlarmExit::default();
    call(
        &svc,
        "PutCompositeAlarm",
        &[
            ("AlarmName", "c"),
            ("AlarmRule", "TRUE"),
            ("ActionsSuppressor", "s"),
            ("ActionsSuppressorWaitPeriod", "0"),
            ("ActionsSuppressorExtensionPeriod", "3600"),
        ],
    )
    .await;
    let b = body_of(
        &call(
            &svc,
            "DescribeAlarms",
            &[("AlarmTypes.member.1", "CompositeAlarm")],
        )
        .await,
    );
    assert!(
        b.contains("<ActionsSuppressedBy>ExtensionPeriod</ActionsSuppressedBy>"),
        "{b}"
    );
}

// ---------------------------------------------------------------------------
// Evaluation cost, holds, ordering, validation, authorization.
// ---------------------------------------------------------------------------

fn bare_alarm(name: &str, metric: &str) -> crate::state::MetricAlarm {
    let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap();
    crate::state::MetricAlarm {
        alarm_name: name.into(),
        alarm_arn: crate::state::alarm_arn(REGION, ACCT, name),
        alarm_description: None,
        actions_enabled: true,
        ok_actions: vec![],
        alarm_actions: vec![],
        insufficient_data_actions: vec![],
        state_value: crate::state::AlarmState::InsufficientData,
        state_reason: String::new(),
        state_updated_timestamp: t0,
        metric_name: Some(metric.into()),
        namespace: Some("NS".into()),
        statistic: Some("Sum".into()),
        extended_statistic: None,
        dimensions: Default::default(),
        period: Some(60),
        unit: None,
        evaluation_periods: 1,
        datapoints_to_alarm: None,
        threshold: Some(1.0),
        comparison_operator: "GreaterThanThreshold".into(),
        treat_missing_data: None,
        evaluate_low_sample_count_percentile: None,
        threshold_metric_id: None,
        configuration_updated_timestamp: t0,
        alarm_configuration_updated_timestamp: t0,
        metrics: vec![],
        state_manually_set: false,
        action_configuration: None,
        warm_up_configuration: None,
        evaluation_criteria: None,
        evaluation_window: None,
        evaluation_interval: None,
        state_reason_data: None,
        state_transitioned_timestamp: Some(t0),
        evaluation_state: None,
        alarm_exit: crate::state::AlarmExit::new(),
        manual_hold_until: None,
        next_evaluation_at: None,
    }
}

fn datum_at(metric: &str, secs: i64, value: f64) -> crate::state::MetricDatum {
    crate::state::MetricDatum {
        metric_name: metric.into(),
        dimensions: Default::default(),
        timestamp: chrono::DateTime::<chrono::Utc>::from_timestamp(secs, 0).unwrap(),
        value: Some(value),
        statistic_values: None,
        unit: None,
        storage_resolution: None,
    }
}

// A put to metric X evaluates the alarm on X only, and reads only the
// datapoints in its evaluation window however much history is stored.
#[test]
fn put_evaluation_is_targeted_and_window_bounded() {
    use crate::alarm_eval::{evaluate_alarms_in, Scope};
    let now = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_030, 0).unwrap();
    let mut st = crate::state::CloudWatchState::new(ACCT);
    for (name, metric) in [("on-x", "X"), ("on-y", "Y")] {
        st.alarms_in_mut(REGION)
            .insert(name.into(), bare_alarm(name, metric));
    }
    {
        let data = st.metrics_in_mut(REGION).entry("NS".into()).or_default();
        // 50,000 old datapoints of X and Y (outside any window).
        for i in 0..25_000 {
            data.push(datum_at("X", 1_600_000_000 + i, 5.0));
            data.push(datum_at("Y", 1_600_000_000 + i, 5.0));
        }
        // Y breaches now, but nobody wrote Y in this put.
        data.push(datum_at("Y", 1_700_000_000, 5.0));
        data.push(datum_at("X", 1_700_000_001, 5.0));
        data.push(datum_at("X", 1_700_000_002, 5.0));
    }
    let touched = vec![("X".to_string(), Default::default())];
    let out = evaluate_alarms_in(
        &mut st,
        REGION,
        now,
        Scope::Series {
            namespace: "NS",
            series: &touched,
        },
    );
    assert_eq!(out.alarms_evaluated, 1);
    assert_eq!(out.datums_inspected, 2, "only X's window datapoints");
    let alarms = st.alarms_in(REGION).unwrap();
    assert_eq!(alarms["on-x"].state_value, crate::state::AlarmState::Alarm);
    assert_eq!(
        alarms["on-y"].state_value,
        crate::state::AlarmState::InsufficientData,
        "the alarm on Y was evaluated by a put to X"
    );

    // The next put reads its window again, not the history.
    st.metrics_in_mut(REGION)
        .get_mut("NS")
        .unwrap()
        .push(datum_at("X", 1_700_000_003, 5.0));
    let out = evaluate_alarms_in(
        &mut st,
        REGION,
        now,
        Scope::Series {
            namespace: "NS",
            series: &touched,
        },
    );
    assert_eq!(out.datums_inspected, 3);
}

// The timer only evaluates alarms past their next period boundary.
#[test]
fn timer_evaluates_alarms_once_per_period() {
    use crate::alarm_eval::{evaluate_alarms_in, Scope};
    let now = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_030, 0).unwrap();
    let mut st = crate::state::CloudWatchState::new(ACCT);
    st.alarms_in_mut(REGION)
        .insert("a".into(), bare_alarm("a", "X"));
    assert_eq!(
        evaluate_alarms_in(&mut st, REGION, now, Scope::Due).alarms_evaluated,
        1
    );
    let boundary = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_040, 0).unwrap();
    assert_eq!(
        st.alarms_in(REGION).unwrap()["a"].next_evaluation_at,
        Some(boundary)
    );
    let before = boundary - chrono::Duration::seconds(1);
    assert_eq!(
        evaluate_alarms_in(&mut st, REGION, before, Scope::Due).alarms_evaluated,
        0
    );
    assert_eq!(
        evaluate_alarms_in(&mut st, REGION, boundary, Scope::Due).alarms_evaluated,
        1
    );
    // An account without alarms does no work.
    let mut empty = crate::state::CloudWatchState::new(ACCT);
    assert_eq!(
        evaluate_alarms_in(&mut empty, REGION, now, Scope::Due).alarms_evaluated,
        0
    );
}

/// Push a datapoint of NS/X with `value` at `at` and evaluate the alarms on
/// it at `at`, returning the actions they queued.
fn put_x_at(
    svc: &CloudWatchService,
    at: chrono::DateTime<chrono::Utc>,
    value: f64,
) -> Vec<crate::alarm_actions::AlarmNotification> {
    let mut state = svc.state.write();
    let acct = state.accounts.get_mut(ACCT).unwrap();
    acct.metrics_in_mut(REGION)
        .entry("NS".into())
        .or_default()
        .push(crate::state::MetricDatum {
            timestamp: at,
            ..datum_at("X", 0, value)
        });
    let series = [("X".to_string(), Default::default())];
    crate::alarm_eval::evaluate_alarms_in(
        acct,
        REGION,
        at,
        crate::alarm_eval::Scope::Series {
            namespace: "NS",
            series: &series,
        },
    )
    .notifications
}

// SetAlarmState holds the forced state until the alarm's next evaluation
// (each minute; the period for high-resolution alarms) even with live data
// saying otherwise: no revert in the same request, no back-to-back actions.
// Then the alarm is evaluated normally. Times are explicit so a minute
// boundary can't land mid-test.
#[tokio::test]
async fn set_alarm_state_holds_until_the_next_evaluation() {
    let svc = service();
    for (name, period) in [("minutely", "60"), ("daily", "86400"), ("fast", "10")] {
        call(
            &svc,
            "PutMetricAlarm",
            &[
                ("AlarmName", name),
                ("Namespace", "NS"),
                ("MetricName", "X"),
                ("Statistic", "Sum"),
                ("Period", period),
                ("Threshold", "1"),
                ("AlarmActions.member.1", TOPIC),
                ("OKActions.member.1", TOPIC),
            ],
        )
        .await;
        call(
            &svc,
            "SetAlarmState",
            &[
                ("AlarmName", name),
                ("StateValue", "OK"),
                ("StateReason", "forced"),
            ],
        )
        .await;
    }
    for (name, interval) in [("minutely", 60), ("daily", 60), ("fast", 10)] {
        let alarm = metric_alarm(&svc, name);
        assert_eq!(
            alarm.manual_hold_until,
            crate::alarm_eval::next_boundary(alarm.state_updated_timestamp, interval),
            "{name}"
        );
    }
    let set_at = metric_alarm(&svc, "minutely").state_updated_timestamp;
    let hold = metric_alarm(&svc, "minutely").manual_hold_until.unwrap();
    svc.state
        .write()
        .get_or_create(ACCT)
        .pending_actions
        .clear();

    // Breaching data within the hold: the forced OK stands, nothing acts.
    assert!(put_x_at(&svc, set_at, 5.0).is_empty());
    assert_eq!(
        metric_alarm(&svc, "minutely").state_value,
        crate::state::AlarmState::Ok
    );
    // At the next evaluation the data governs again.
    let acted = put_x_at(&svc, hold, 5.0);
    assert_eq!(
        metric_alarm(&svc, "minutely").state_value,
        crate::state::AlarmState::Alarm
    );
    assert!(acted.iter().any(|n| n.alarm_name == "minutely"));

    // A new configuration during a hold ends it and evaluates at once.
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "daily"),
            ("StateValue", "OK"),
            ("StateReason", "forced"),
        ],
    )
    .await;
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "daily"),
            ("Namespace", "NS"),
            ("MetricName", "X"),
            ("Statistic", "Sum"),
            ("Period", "86400"),
            ("Threshold", "1"),
        ],
    )
    .await;
    let daily = metric_alarm(&svc, "daily");
    assert_eq!(daily.manual_hold_until, None);
    assert_eq!(daily.state_value, crate::state::AlarmState::Alarm);
}

// Datapoints another service publishes (here through the cross-service
// metrics delivery, as Lambda/SNS/Logs metric filters do) evaluate the
// alarms on that metric like PutMetricData, and the next evaluation tick
// runs their actions. Nothing here describes the alarms.
#[tokio::test]
async fn delivered_metrics_evaluate_and_act_without_describe() {
    use fakecloud_core::delivery::CloudwatchDelivery;
    let (svc, sns, _) = acting_service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "fn-errors"),
            ("Namespace", "AWS/Lambda"),
            ("MetricName", "Errors"),
            ("Statistic", "Sum"),
            ("Period", "60"),
            ("EvaluationPeriods", "1"),
            ("Threshold", "0"),
            ("Dimensions.member.1.Name", "FunctionName"),
            ("Dimensions.member.1.Value", "f"),
            ("AlarmActions.member.1", TOPIC),
        ],
    )
    .await;
    let delivery = crate::CloudwatchDeliveryImpl::new(svc.state.clone());
    delivery.put_metric(
        ACCT,
        REGION,
        "AWS/Lambda",
        "Errors",
        1.0,
        Some("Count"),
        [("FunctionName".to_string(), "f".to_string())].into(),
        chrono::Utc::now().timestamp_millis(),
    );
    assert_eq!(
        metric_alarm(&svc, "fn-errors").state_value,
        crate::state::AlarmState::Alarm
    );
    // The timer tick dispatches the queued action.
    svc.evaluate_all_alarms().await;
    let published = sns.published.lock().clone();
    assert_eq!(published.len(), 1, "{published:?}");
    assert_eq!(
        published[0].2.as_deref(),
        Some("ALARM: \"fn-errors\" in US East (N. Virginia)")
    );
}

// Deleting an alarm a composite references re-evaluates the composite at
// once.
#[tokio::test]
async fn deleting_a_child_refreshes_its_composites() {
    let svc = service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "child"),
            ("Namespace", "N"),
            ("MetricName", "M"),
        ],
    )
    .await;
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "child"),
            ("StateValue", "ALARM"),
            ("StateReason", "x"),
        ],
    )
    .await;
    call(
        &svc,
        "PutCompositeAlarm",
        &[("AlarmName", "c"), ("AlarmRule", "ALARM(child)")],
    )
    .await;
    let composite_state = |svc: &CloudWatchService| {
        svc.state
            .read()
            .get(ACCT)
            .unwrap()
            .composite_alarms_in(REGION)
            .unwrap()["c"]
            .state_value
    };
    assert_eq!(composite_state(&svc), crate::state::AlarmState::Alarm);
    call(&svc, "DeleteAlarms", &[("AlarmNames.member.1", "child")]).await;
    assert_eq!(composite_state(&svc), crate::state::AlarmState::Ok);
}

// Actions are dispatched one at a time in transition order, even when two
// requests drain the queue concurrently; actions queued before a reset are
// dropped.
#[tokio::test]
async fn actions_dispatch_in_order_and_not_after_reset() {
    let (svc, sns, _) = acting_service();
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "o"),
            ("Namespace", "NS"),
            ("MetricName", "X"),
            ("Statistic", "Sum"),
            ("Threshold", "1"),
            ("AlarmActions.member.1", TOPIC),
            ("OKActions.member.1", TOPIC),
        ],
    )
    .await;
    let notification = |value: crate::state::AlarmState, prev: crate::state::AlarmState| {
        let state = svc.state.read();
        let acct = state.get(ACCT).unwrap();
        let mut alarm = acct.alarms_in(REGION).unwrap()["o"].clone();
        alarm.state_value = value;
        let mut n = crate::alarm_actions::metric_alarm_notification(
            ACCT,
            REGION,
            &alarm,
            &crate::state::PreviousState {
                value: prev,
                reason: String::new(),
                reason_data: None,
                timestamp: alarm.state_updated_timestamp,
            },
        )
        .unwrap();
        n.epoch = acct.epoch;
        n
    };
    use crate::state::AlarmState::{Alarm, InsufficientData, Ok as OkState};
    let first = notification(Alarm, InsufficientData);
    let second = notification(OkState, Alarm);
    svc.state
        .write()
        .get_or_create(ACCT)
        .pending_actions
        .extend([first, second]);
    tokio::join!(svc.run_pending_actions(), svc.run_pending_actions());
    let subjects: Vec<String> = sns
        .published
        .lock()
        .iter()
        .filter_map(|p| p.2.clone())
        .collect();
    assert!(subjects[0].starts_with("ALARM:"), "{subjects:?}");
    assert!(subjects[1].starts_with("OK:"), "{subjects:?}");

    // Queued, then the account is reset: nothing is dispatched.
    // A reset drops the queue with the state.
    let queued = notification(Alarm, OkState);
    svc.state
        .write()
        .get_or_create(ACCT)
        .pending_actions
        .push(queued.clone());
    *svc.state.write() = crate::state::CloudWatchAccounts::new();
    svc.state.write().get_or_create(ACCT);
    assert!(!svc.run_pending_actions().await);
    // One queued against an earlier incarnation of the account is dropped.
    svc.state
        .write()
        .get_or_create(ACCT)
        .pending_actions
        .push(queued);
    svc.run_pending_actions().await;
    assert_eq!(sns.published.lock().len(), 2);
}

/// A request that finds the queue empty still waits for a dispatch already
/// in flight: that dispatch may hold the request's own notification.
#[tokio::test]
async fn empty_queue_still_waits_for_an_in_flight_dispatch() {
    let (svc, _sns, _) = acting_service();
    let svc = Arc::new(svc);
    let held = svc.dispatch_lock.lock().await;
    let waiter = {
        let svc = Arc::clone(&svc);
        tokio::spawn(async move { svc.run_pending_actions().await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        !waiter.is_finished(),
        "returned while a dispatch was in flight"
    );
    drop(held);
    assert!(!waiter.await.unwrap());
    // With nothing queued and nobody dispatching, it returns at once.
    assert!(!svc.run_pending_actions().await);
}

#[tokio::test]
async fn put_alarm_rejects_invalid_action_arns() {
    let svc = service();
    let put = |actions: &'static str| {
        let svc = &svc;
        async move {
            svc.handle(req(
                "PutMetricAlarm",
                &[
                    ("AlarmName", "v"),
                    ("Namespace", "N"),
                    ("MetricName", "M"),
                    ("AlarmActions.member.1", actions),
                ],
            ))
            .await
            .err()
            .map(|e| (e.code().to_string(), e.message()))
        }
    };
    let (code, msg) = put("arn:aws:automate:us-west-2:ec2:stop").await.unwrap();
    assert_eq!(code, "ValidationError");
    assert_eq!(
        msg,
        "Invalid region us-west-2 specified. Only us-east-1 is supported."
    );
    assert!(put("not-an-arn").await.is_some());
    assert!(put("arn:aws:automate:us-east-1:ec2:explode")
        .await
        .is_some());
    for ok in [
        "arn:aws:automate:us-east-1:ec2:recover",
        "arn:aws:sns:us-east-1:123456789012:t",
        "arn:aws:lambda:us-east-1:123456789012:function:f:alias",
        "arn:aws:ssm:us-east-1:123456789012:opsitem:2#CATEGORY=Security",
        "arn:aws:ssm-incidents::123456789012:responseplan/plan",
        "arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:id:autoScalingGroupName/g:policyName/p",
    ] {
        assert_eq!(put(ok).await, None, "{ok}");
    }
    // Composite alarms take no EC2 actions.
    let err = call_err(
        &svc,
        "PutCompositeAlarm",
        &[
            ("AlarmName", "c"),
            ("AlarmRule", "TRUE"),
            (
                "AlarmActions.member.1",
                "arn:aws:automate:us-east-1:ec2:stop",
            ),
        ],
    )
    .await;
    assert_eq!(err.code(), "ValidationError");
}

/// Grants only what `allowed` lists.
struct StaticAuthorizer {
    allowed: Vec<(&'static str, String)>,
}

impl crate::alarm_actions::AlarmActionAuthorizer for StaticAuthorizer {
    fn allows(&self, grant: &crate::alarm_actions::ActionGrant<'_>) -> bool {
        let who = match grant.principal {
            crate::alarm_actions::GrantPrincipal::Service(s) => s.to_string(),
            crate::alarm_actions::GrantPrincipal::Account(a) => a.to_string(),
        };
        self.allowed
            .iter()
            .any(|(r, p)| *r == grant.resource_arn && *p == who)
    }
}

// Lambda actions need the function policy to grant the alarms principal;
// cross-account topics need the topic policy to allow CloudWatch.
#[tokio::test]
async fn actions_honor_target_resource_policies() {
    const DENIED_FN: &str = "arn:aws:lambda:us-east-1:123456789012:function:no-perm";
    const OTHER_TOPIC: &str = "arn:aws:sns:us-east-1:999999999999:theirs";
    let lambda = Arc::new(RecordingLambda::default());
    let sns = Arc::new(RecordingSns::default());
    let bus = fakecloud_core::delivery::DeliveryBus::new()
        .with_lambda(lambda.clone())
        .with_sns(sns.clone());
    let svc = service()
        .with_delivery(Arc::new(bus))
        .with_action_authorizer(Arc::new(StaticAuthorizer {
            allowed: vec![(
                "arn:aws:lambda:us-east-1:123456789012:function:ok",
                crate::alarm_actions::LAMBDA_ALARMS_PRINCIPAL.to_string(),
            )],
        }));
    call(
        &svc,
        "PutMetricAlarm",
        &[
            ("AlarmName", "p"),
            ("Namespace", "N"),
            ("MetricName", "M"),
            ("Statistic", "Sum"),
            ("Threshold", "1"),
            (
                "AlarmActions.member.1",
                "arn:aws:lambda:us-east-1:123456789012:function:ok",
            ),
            ("AlarmActions.member.2", DENIED_FN),
            ("AlarmActions.member.3", OTHER_TOPIC),
            ("AlarmActions.member.4", TOPIC),
        ],
    )
    .await;
    call(
        &svc,
        "SetAlarmState",
        &[
            ("AlarmName", "p"),
            ("StateValue", "ALARM"),
            ("StateReason", "x"),
        ],
    )
    .await;
    let invoked: Vec<String> = lambda.invoked.lock().iter().map(|i| i.0.clone()).collect();
    assert_eq!(
        invoked,
        vec!["arn:aws:lambda:us-east-1:123456789012:function:ok".to_string()]
    );
    // Same-account topic: no policy needed.
    assert_eq!(sns.published.lock().len(), 1);
    let hist = history_of(&svc, "p", "MetricAlarm").await;
    assert!(
        hist.contains(&format!("Failed to execute action {DENIED_FN}")),
        "{hist}"
    );
    assert!(
        hist.contains(&format!("Failed to execute action {OTHER_TOPIC}")),
        "{hist}"
    );
    assert!(
        hist.contains(crate::alarm_actions::LAMBDA_ALARMS_PRINCIPAL),
        "{hist}"
    );
    assert!(hist.contains("the topic policy"), "{hist}");
}

// A manually set composite over a composite thaws in the same pass its
// child composite changes.
#[test]
fn manual_composite_thaws_in_the_pass_its_child_composite_changes() {
    use crate::state::{AlarmState, CompositeAlarm};
    let t0 = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let now = t0 + chrono::Duration::seconds(10);
    let composite = |name: &str, rule: &str, state: AlarmState, manual: bool| CompositeAlarm {
        alarm_name: name.into(),
        alarm_arn: crate::state::alarm_arn(REGION, ACCT, name),
        alarm_description: None,
        alarm_rule: rule.into(),
        actions_enabled: true,
        ok_actions: vec![],
        alarm_actions: vec![],
        insufficient_data_actions: vec![],
        actions_suppressor: None,
        actions_suppressor_wait_period: None,
        actions_suppressor_extension_period: None,
        state_value: state,
        state_reason: String::new(),
        state_updated_timestamp: t0,
        alarm_configuration_updated_timestamp: t0,
        action_configuration: None,
        state_reason_data: None,
        state_transitioned_timestamp: Some(t0),
        actions_suppressed_by: None,
        actions_suppressed_reason: None,
        state_manually_set: manual,
        pending_action_from: None,
        alarm_exit: crate::state::AlarmExit::new(),
    };
    let mut st = crate::state::CloudWatchState::new(ACCT);
    let mut m = bare_alarm("m", "X");
    m.state_value = AlarmState::Alarm;
    m.state_transitioned_timestamp = Some(now);
    m.state_manually_set = true;
    m.manual_hold_until = Some(now + chrono::Duration::seconds(60));
    st.alarms_in_mut(REGION).insert("m".into(), m);
    let c = st.composite_alarms_in_mut(REGION);
    c.insert(
        "child".into(),
        composite("child", "ALARM(m)", AlarmState::Ok, false),
    );
    c.insert(
        "parent".into(),
        composite("parent", "ALARM(child)", AlarmState::Ok, true),
    );
    crate::alarm_eval::evaluate_alarms(&mut st, REGION, now);
    let c = st.composite_alarms_in(REGION).unwrap();
    assert_eq!(c["child"].state_value, AlarmState::Alarm);
    assert_eq!(c["parent"].state_value, AlarmState::Alarm);
    assert!(!c["parent"].state_manually_set);
}
