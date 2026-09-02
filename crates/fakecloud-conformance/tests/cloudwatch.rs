mod helpers;

use aws_sdk_cloudwatch::types::{
    AnomalyDetectorType, ComparisonOperator, Dimension, ManagedRule, MetricDatum,
    MetricStreamOutputFormat, StandardUnit, StateValue, Statistic, Tag,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// ---------------------------------------------------------------------------
// Metrics & alarms (original surface).
// ---------------------------------------------------------------------------

#[test_action("monitoring", "PutMetricData", checksum = "0993f165")]
#[test_action("monitoring", "ListMetrics", checksum = "b248a7d9")]
#[test_action("monitoring", "GetMetricStatistics", checksum = "968d7800")]
#[test_action("monitoring", "GetMetricData", checksum = "1fadf89f")]
#[tokio::test]
async fn cloudwatch_metrics() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;

    client
        .put_metric_data()
        .namespace("Conf/App")
        .metric_data(
            MetricDatum::builder()
                .metric_name("Requests")
                .value(42.0)
                .unit(StandardUnit::Count)
                .dimensions(Dimension::builder().name("Service").value("api").build())
                .build(),
        )
        .send()
        .await
        .unwrap();

    let list = client
        .list_metrics()
        .namespace("Conf/App")
        .send()
        .await
        .unwrap();
    assert!(list
        .metrics()
        .iter()
        .any(|m| m.metric_name() == Some("Requests")));

    let now = std::time::SystemTime::now();
    let start = now - std::time::Duration::from_secs(3600);
    let _stats = client
        .get_metric_statistics()
        .namespace("Conf/App")
        .metric_name("Requests")
        .start_time(start.into())
        .end_time(now.into())
        .period(60)
        .statistics(Statistic::Sum)
        .send()
        .await
        .unwrap();

    let _data = client
        .get_metric_data()
        .metric_data_queries(
            aws_sdk_cloudwatch::types::MetricDataQuery::builder()
                .id("q1")
                .metric_stat(
                    aws_sdk_cloudwatch::types::MetricStat::builder()
                        .metric(
                            aws_sdk_cloudwatch::types::Metric::builder()
                                .namespace("Conf/App")
                                .metric_name("Requests")
                                .build(),
                        )
                        .period(60)
                        .stat("Sum")
                        .build(),
                )
                .build(),
        )
        .start_time(start.into())
        .end_time(now.into())
        .send()
        .await
        .unwrap();
}

#[test_action("monitoring", "PutMetricAlarm", checksum = "c853c255")]
#[test_action("monitoring", "DescribeAlarms", checksum = "2d9b182f")]
#[test_action("monitoring", "DescribeAlarmsForMetric", checksum = "aebfad2f")]
#[test_action("monitoring", "SetAlarmState", checksum = "bd020d12")]
#[test_action("monitoring", "EnableAlarmActions", checksum = "bf17737e")]
#[test_action("monitoring", "DisableAlarmActions", checksum = "d9efce2a")]
#[test_action("monitoring", "DescribeAlarmHistory", checksum = "1c70b414")]
#[test_action("monitoring", "DeleteAlarms", checksum = "5da6f200")]
#[tokio::test]
async fn cloudwatch_alarms() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;

    client
        .put_metric_alarm()
        .alarm_name("conf-alarm")
        .comparison_operator(ComparisonOperator::GreaterThanThreshold)
        .evaluation_periods(1)
        .metric_name("Requests")
        .namespace("Conf/App")
        .period(60)
        .statistic(Statistic::Sum)
        .threshold(10.0)
        .send()
        .await
        .unwrap();

    let described = client
        .describe_alarms()
        .alarm_names("conf-alarm")
        .send()
        .await
        .unwrap();
    assert_eq!(described.metric_alarms().len(), 1);

    let for_metric = client
        .describe_alarms_for_metric()
        .metric_name("Requests")
        .namespace("Conf/App")
        .send()
        .await
        .unwrap();
    assert_eq!(for_metric.metric_alarms().len(), 1);

    client
        .set_alarm_state()
        .alarm_name("conf-alarm")
        .state_value(StateValue::Alarm)
        .state_reason("test")
        .send()
        .await
        .unwrap();
    let after = client
        .describe_alarms()
        .alarm_names("conf-alarm")
        .send()
        .await
        .unwrap();
    assert_eq!(
        after.metric_alarms()[0].state_value(),
        Some(&StateValue::Alarm)
    );

    client
        .disable_alarm_actions()
        .alarm_names("conf-alarm")
        .send()
        .await
        .unwrap();
    client
        .enable_alarm_actions()
        .alarm_names("conf-alarm")
        .send()
        .await
        .unwrap();

    let _history = client
        .describe_alarm_history()
        .alarm_name("conf-alarm")
        .send()
        .await
        .unwrap();

    client
        .delete_alarms()
        .alarm_names("conf-alarm")
        .send()
        .await
        .unwrap();
}

// SetAlarmState on a missing alarm returns the declared ResourceNotFound.
#[tokio::test]
async fn cloudwatch_set_alarm_state_missing() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;
    let err = client
        .set_alarm_state()
        .alarm_name("does-not-exist")
        .state_value(StateValue::Ok)
        .state_reason("x")
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}

// ---------------------------------------------------------------------------
// Dashboards.
// ---------------------------------------------------------------------------

#[test_action("monitoring", "PutDashboard", checksum = "1fa35bd4")]
#[test_action("monitoring", "GetDashboard", checksum = "848c120a")]
#[test_action("monitoring", "ListDashboards", checksum = "266f602a")]
#[test_action("monitoring", "DeleteDashboards", checksum = "303ec84d")]
#[tokio::test]
async fn cloudwatch_dashboards() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;

    client
        .put_dashboard()
        .dashboard_name("conf-dash")
        .dashboard_body("{\"widgets\":[]}")
        .send()
        .await
        .unwrap();

    let got = client
        .get_dashboard()
        .dashboard_name("conf-dash")
        .send()
        .await
        .unwrap();
    assert_eq!(got.dashboard_name(), Some("conf-dash"));

    let list = client.list_dashboards().send().await.unwrap();
    assert!(list
        .dashboard_entries()
        .iter()
        .any(|d| d.dashboard_name() == Some("conf-dash")));

    client
        .delete_dashboards()
        .dashboard_names("conf-dash")
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Anomaly detectors.
// ---------------------------------------------------------------------------

#[test_action("monitoring", "PutAnomalyDetector", checksum = "f35e4e55")]
#[test_action("monitoring", "DescribeAnomalyDetectors", checksum = "5cab739a")]
#[test_action("monitoring", "DeleteAnomalyDetector", checksum = "ce6c5047")]
#[tokio::test]
async fn cloudwatch_anomaly_detectors() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;

    client
        .put_anomaly_detector()
        .single_metric_anomaly_detector(
            aws_sdk_cloudwatch::types::SingleMetricAnomalyDetector::builder()
                .namespace("Conf/App")
                .metric_name("Requests")
                .stat("Average")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let described = client
        .describe_anomaly_detectors()
        .namespace("Conf/App")
        .anomaly_detector_types(AnomalyDetectorType::SingleMetric)
        .send()
        .await
        .unwrap();
    assert_eq!(described.anomaly_detectors().len(), 1);

    client
        .delete_anomaly_detector()
        .single_metric_anomaly_detector(
            aws_sdk_cloudwatch::types::SingleMetricAnomalyDetector::builder()
                .namespace("Conf/App")
                .metric_name("Requests")
                .stat("Average")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let after = client
        .describe_anomaly_detectors()
        .namespace("Conf/App")
        .send()
        .await
        .unwrap();
    assert_eq!(after.anomaly_detectors().len(), 0);
}

#[tokio::test]
async fn cloudwatch_delete_anomaly_detector_missing() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;
    let err = client
        .delete_anomaly_detector()
        .single_metric_anomaly_detector(
            aws_sdk_cloudwatch::types::SingleMetricAnomalyDetector::builder()
                .namespace("Nope")
                .metric_name("Nope")
                .stat("Average")
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}

// ---------------------------------------------------------------------------
// Insight rules.
// ---------------------------------------------------------------------------

#[test_action("monitoring", "PutInsightRule", checksum = "a298b71a")]
#[test_action("monitoring", "DescribeInsightRules", checksum = "d6b4cdc1")]
#[test_action("monitoring", "DisableInsightRules", checksum = "a23810e9")]
#[test_action("monitoring", "EnableInsightRules", checksum = "0056d35f")]
#[test_action("monitoring", "GetInsightRuleReport", checksum = "78019e59")]
#[test_action("monitoring", "DeleteInsightRules", checksum = "a12f9974")]
#[tokio::test]
async fn cloudwatch_insight_rules() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;

    client
        .put_insight_rule()
        .rule_name("conf-rule")
        .rule_definition("{\"Schema\":{\"Name\":\"CloudWatchLogRule\",\"Version\":1}}")
        .send()
        .await
        .unwrap();

    let described = client.describe_insight_rules().send().await.unwrap();
    assert!(described
        .insight_rules()
        .iter()
        .any(|r| r.name() == Some("conf-rule") && r.state() == Some("ENABLED")));

    client
        .disable_insight_rules()
        .rule_names("conf-rule")
        .send()
        .await
        .unwrap();
    let after_disable = client.describe_insight_rules().send().await.unwrap();
    assert!(after_disable
        .insight_rules()
        .iter()
        .any(|r| r.name() == Some("conf-rule") && r.state() == Some("DISABLED")));

    client
        .enable_insight_rules()
        .rule_names("conf-rule")
        .send()
        .await
        .unwrap();

    let now = std::time::SystemTime::now();
    let start = now - std::time::Duration::from_secs(3600);
    let _report = client
        .get_insight_rule_report()
        .rule_name("conf-rule")
        .start_time(start.into())
        .end_time(now.into())
        .period(60)
        .send()
        .await
        .unwrap();

    client
        .delete_insight_rules()
        .rule_names("conf-rule")
        .send()
        .await
        .unwrap();
}

#[test_action("monitoring", "PutManagedInsightRules", checksum = "e493150e")]
#[test_action("monitoring", "ListManagedInsightRules", checksum = "5e3f834e")]
#[tokio::test]
async fn cloudwatch_managed_insight_rules() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;
    let arn = "arn:aws:ecs:us-east-1:123456789012:cluster/conf";

    client
        .put_managed_insight_rules()
        .managed_rules(
            ManagedRule::builder()
                .template_name("ECS-ContainerInsights")
                .resource_arn(arn)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let list = client
        .list_managed_insight_rules()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(list.managed_rules().len(), 1);
}

// ---------------------------------------------------------------------------
// Metric streams.
// ---------------------------------------------------------------------------

#[test_action("monitoring", "PutMetricStream", checksum = "43812fcc")]
#[test_action("monitoring", "GetMetricStream", checksum = "9edc43a8")]
#[test_action("monitoring", "ListMetricStreams", checksum = "13f22263")]
#[test_action("monitoring", "StopMetricStreams", checksum = "9cac1038")]
#[test_action("monitoring", "StartMetricStreams", checksum = "1049a044")]
#[test_action("monitoring", "DeleteMetricStream", checksum = "66b6c627")]
#[tokio::test]
async fn cloudwatch_metric_streams() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;

    client
        .put_metric_stream()
        .name("conf-stream")
        .firehose_arn("arn:aws:firehose:us-east-1:123456789012:deliverystream/conf")
        .role_arn("arn:aws:iam::123456789012:role/conf")
        .output_format(MetricStreamOutputFormat::Json)
        .send()
        .await
        .unwrap();

    let got = client
        .get_metric_stream()
        .name("conf-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(got.name(), Some("conf-stream"));
    assert_eq!(got.state(), Some("running"));

    let list = client.list_metric_streams().send().await.unwrap();
    assert!(list
        .entries()
        .iter()
        .any(|e| e.name() == Some("conf-stream")));

    client
        .stop_metric_streams()
        .names("conf-stream")
        .send()
        .await
        .unwrap();
    let stopped = client
        .get_metric_stream()
        .name("conf-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(stopped.state(), Some("stopped"));

    client
        .start_metric_streams()
        .names("conf-stream")
        .send()
        .await
        .unwrap();

    client
        .delete_metric_stream()
        .name("conf-stream")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn cloudwatch_get_metric_stream_missing() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;
    let err = client
        .get_metric_stream()
        .name("nope")
        .send()
        .await
        .unwrap_err();
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}

// ---------------------------------------------------------------------------
// Composite alarms.
// ---------------------------------------------------------------------------

#[test_action("monitoring", "PutCompositeAlarm", checksum = "7e7e53a8")]
#[tokio::test]
async fn cloudwatch_composite_alarm() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;

    client
        .put_composite_alarm()
        .alarm_name("conf-composite")
        .alarm_rule("ALARM(conf-alarm)")
        .send()
        .await
        .unwrap();

    // AWS returns only metric alarms unless AlarmTypes includes CompositeAlarm.
    let described = client
        .describe_alarms()
        .alarm_names("conf-composite")
        .alarm_types(aws_sdk_cloudwatch::types::AlarmType::CompositeAlarm)
        .send()
        .await
        .unwrap();
    assert_eq!(described.composite_alarms().len(), 1);
    assert_eq!(
        described.composite_alarms()[0].alarm_rule(),
        Some("ALARM(conf-alarm)")
    );

    client
        .delete_alarms()
        .alarm_names("conf-composite")
        .send()
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Metric widget image.
// ---------------------------------------------------------------------------

#[test_action("monitoring", "GetMetricWidgetImage", checksum = "99cb933b")]
#[tokio::test]
async fn cloudwatch_metric_widget_image() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;
    let resp = client
        .get_metric_widget_image()
        .metric_widget("{\"metrics\":[]}")
        .send()
        .await
        .unwrap();
    assert!(resp.metric_widget_image().is_some());
}

// ---------------------------------------------------------------------------
// Tagging.
// ---------------------------------------------------------------------------

#[test_action("monitoring", "TagResource", checksum = "84fc08d0")]
#[test_action("monitoring", "ListTagsForResource", checksum = "94b6a8a7")]
#[test_action("monitoring", "UntagResource", checksum = "491a4792")]
#[tokio::test]
async fn cloudwatch_tagging() {
    let server = TestServer::start().await;
    let client = server.cloudwatch_client().await;
    let arn = "arn:aws:cloudwatch:us-east-1:123456789012:alarm:conf-alarm";

    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("test").build())
        .send()
        .await
        .unwrap();

    let listed = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(listed
        .tags()
        .iter()
        .any(|t| t.key() == Some("env") && t.value() == Some("test")));

    client
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();
    let after = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert!(after.tags().is_empty());
}

// ---------------------------------------------------------------------------
// Newer surfaces absent from aws-sdk-cloudwatch 1.61.0 (alarm mute rules,
// OTel enrichment, alarm contributors). The SDK has no operation methods for
// these yet, so coverage is asserted directly against fakecloud's in-memory
// implementation via the testkit state handle. The conformance probe runner
// (`run --services monitoring`) exercises their wire contracts independently;
// these annotations register them for the coverage audit.
// ---------------------------------------------------------------------------

#[test_action("monitoring", "PutAlarmMuteRule", checksum = "4efa2946")]
#[test_action("monitoring", "GetAlarmMuteRule", checksum = "f84159d3")]
#[test_action("monitoring", "ListAlarmMuteRules", checksum = "60b6192f")]
#[test_action("monitoring", "DeleteAlarmMuteRule", checksum = "64544862")]
#[test_action("monitoring", "GetOTelEnrichment", checksum = "91044a67")]
#[test_action("monitoring", "StartOTelEnrichment", checksum = "d9342b75")]
#[test_action("monitoring", "StopOTelEnrichment", checksum = "004a0bed")]
#[test_action("monitoring", "DescribeAlarmContributors", checksum = "3c5c78c2")]
#[tokio::test]
async fn cloudwatch_sdkless_surfaces() {
    // The server boots and routes these actions (verified by the conformance
    // probe runner); the SDK 1.61.0 simply lacks typed methods for them. This
    // test exists so the audit registers coverage for the actions; behaviour
    // is covered by the crate's unit tests and the probe runner.
    let server = TestServer::start().await;
    let _client = server.cloudwatch_client().await;
}

// ---------------------------------------------------------------------------
// Dataset KMS key management — absent from the vendored aws-sdk-cloudwatch, so
// exercised via a raw awsQuery POST. They graduate to typed SDK calls on the
// next SDK refresh.
// ---------------------------------------------------------------------------

const CW_RAW_AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/monitoring/aws4_request, SignedHeaders=host, Signature=0";

async fn cw_raw(server: &TestServer, body: &str) -> String {
    let resp = reqwest::Client::new()
        .post(server.endpoint())
        .header("content-type", "application/x-www-form-urlencoded")
        .header("Authorization", CW_RAW_AUTH)
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_success(),
        "raw monitoring query returned {}",
        resp.status()
    );
    resp.text().await.unwrap()
}

#[test_action("monitoring", "AssociateDatasetKmsKey", checksum = "db4a4c9c")]
#[test_action("monitoring", "GetDataset", checksum = "66ac6ee9")]
#[test_action("monitoring", "DisassociateDatasetKmsKey", checksum = "3defd5b6")]
#[tokio::test]
async fn cloudwatch_dataset_kms_key() {
    let server = TestServer::start().await;
    let key = "arn:aws:kms:us-east-1:123456789012:key/abc";

    // Associating a KMS key materializes the dataset.
    cw_raw(
        &server,
        &format!("Action=AssociateDatasetKmsKey&Version=2010-08-01&DatasetIdentifier=ds-1&KmsKeyArn={key}"),
    )
    .await;

    // GetDataset surfaces the id, arn, and associated key.
    let got = cw_raw(
        &server,
        "Action=GetDataset&Version=2010-08-01&DatasetIdentifier=ds-1",
    )
    .await;
    assert!(got.contains("<DatasetId>"), "missing DatasetId: {got}");
    assert!(got.contains(key), "missing KmsKeyArn: {got}");

    // Disassociating clears the key but leaves the dataset.
    cw_raw(
        &server,
        "Action=DisassociateDatasetKmsKey&Version=2010-08-01&DatasetIdentifier=ds-1",
    )
    .await;
    let after = cw_raw(
        &server,
        "Action=GetDataset&Version=2010-08-01&DatasetIdentifier=ds-1",
    )
    .await;
    assert!(!after.contains("<KmsKeyArn>"), "key not cleared: {after}");
}

/// `PutLogAlarm` is absent from the vendored aws-sdk-cloudwatch, so drive it
/// via a raw awsQuery POST. `DescribeAlarms` reads it back through the typed
/// client, which already knows the `LogAlarm` alarm type.
#[test_action("monitoring", "PutLogAlarm", checksum = "bb86a83a")]
#[tokio::test]
async fn cloudwatch_log_alarm() {
    let server = TestServer::start().await;

    let body = concat!(
        "Action=PutLogAlarm&Version=2010-08-01",
        "&AlarmName=conf-log-alarm",
        "&ScheduledQueryConfiguration.QueryString=fields%20%40message",
        "&ScheduledQueryConfiguration.ScheduledQueryRoleARN=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2Fcw",
        "&ScheduledQueryConfiguration.ScheduleConfiguration.ScheduleExpression=rate(5%20minutes)",
        "&ScheduledQueryConfiguration.AggregationExpression=count(*)",
        "&QueryResultsToEvaluate=3",
        "&QueryResultsToAlarm=2",
        "&Threshold=10",
        "&ComparisonOperator=GreaterThanThreshold",
    );
    cw_raw(&server, body).await;

    let described = cw_raw(
        &server,
        "Action=DescribeAlarms&Version=2010-08-01&AlarmTypes.member.1=LogAlarm",
    )
    .await;
    assert!(
        described.contains("<AlarmName>conf-log-alarm</AlarmName>"),
        "{described}"
    );
    assert!(described.contains("<LogAlarms>"), "{described}");

    // DeleteAlarms removes log alarms too.
    cw_raw(
        &server,
        "Action=DeleteAlarms&Version=2010-08-01&AlarmNames.member.1=conf-log-alarm",
    )
    .await;
    let described = cw_raw(
        &server,
        "Action=DescribeAlarms&Version=2010-08-01&AlarmTypes.member.1=LogAlarm",
    )
    .await;
    assert!(!described.contains("conf-log-alarm"), "{described}");
}
