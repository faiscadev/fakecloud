//! CloudWatch metrics + alarms E2E.

mod helpers;

use aws_sdk_cloudwatch::primitives::DateTime as AwsDateTime;
use aws_sdk_cloudwatch::types::Metric as CwMetric;
use aws_sdk_cloudwatch::types::{
    ComparisonOperator, Dimension, MetricDataQuery, MetricDatum, MetricStat, StandardUnit,
    StateValue, Statistic,
};
use helpers::TestServer;

#[tokio::test]
async fn put_and_list_metrics() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;

    cw.put_metric_data()
        .namespace("MyApp")
        .metric_data(
            MetricDatum::builder()
                .metric_name("Latency")
                .value(123.4)
                .unit(StandardUnit::Milliseconds)
                .dimensions(Dimension::builder().name("Service").value("api").build())
                .build(),
        )
        .send()
        .await
        .expect("put");

    let listed = cw
        .list_metrics()
        .namespace("MyApp")
        .send()
        .await
        .expect("list");
    let metrics = listed.metrics();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].metric_name(), Some("Latency"));
    assert_eq!(metrics[0].namespace(), Some("MyApp"));
}

// bug-audit 2026-06-27, T1.5: PutMetricData must accept the Values/Counts
// value-distribution publish path (previously 400'd), and the distribution must
// aggregate into the statistics.
#[tokio::test]
async fn put_metric_data_accepts_values_and_counts() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    cw.put_metric_data()
        .namespace("Dist")
        .metric_data(
            MetricDatum::builder()
                .metric_name("Latency")
                .values(10.0)
                .values(20.0)
                .counts(2.0)
                .counts(3.0)
                .timestamp(AwsDateTime::from_secs(now.timestamp()))
                .build(),
        )
        .send()
        .await
        .expect("put values/counts distribution");

    let stats = cw
        .get_metric_statistics()
        .namespace("Dist")
        .metric_name("Latency")
        .start_time(AwsDateTime::from_secs(now.timestamp() - 600))
        .end_time(AwsDateTime::from_secs(now.timestamp() + 600))
        .period(60)
        .statistics(Statistic::Sum)
        .statistics(Statistic::SampleCount)
        .statistics(Statistic::Maximum)
        .send()
        .await
        .expect("stats");

    let dp = &stats.datapoints()[0];
    // sum = 10*2 + 20*3 = 80, sample_count = 5, max = 20.
    assert!((dp.sum().unwrap() - 80.0).abs() < 1e-6);
    assert_eq!(dp.sample_count().unwrap(), 5.0);
    assert!((dp.maximum().unwrap() - 20.0).abs() < 1e-6);
}

#[tokio::test]
async fn get_metric_statistics_aggregates_by_period() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    for v in [10.0, 20.0, 30.0] {
        cw.put_metric_data()
            .namespace("Bench")
            .metric_data(
                MetricDatum::builder()
                    .metric_name("Throughput")
                    .value(v)
                    .timestamp(AwsDateTime::from_secs(now.timestamp()))
                    .build(),
            )
            .send()
            .await
            .expect("put");
    }

    let start = AwsDateTime::from_secs(now.timestamp() - 600);
    let end = AwsDateTime::from_secs(now.timestamp() + 600);
    let stats = cw
        .get_metric_statistics()
        .namespace("Bench")
        .metric_name("Throughput")
        .start_time(start)
        .end_time(end)
        .period(60)
        .statistics(Statistic::Sum)
        .statistics(Statistic::Average)
        .statistics(Statistic::SampleCount)
        .send()
        .await
        .expect("stats");

    let datapoints = stats.datapoints();
    assert_eq!(datapoints.len(), 1);
    let dp = &datapoints[0];
    assert!((dp.sum().unwrap() - 60.0).abs() < 1e-6);
    assert!((dp.average().unwrap() - 20.0).abs() < 1e-6);
    assert_eq!(dp.sample_count().unwrap(), 3.0);
}

#[tokio::test]
async fn get_metric_data_returns_per_query_results() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    for v in [1.0, 2.0, 3.0, 4.0] {
        cw.put_metric_data()
            .namespace("App")
            .metric_data(
                MetricDatum::builder()
                    .metric_name("Errors")
                    .value(v)
                    .timestamp(AwsDateTime::from_secs(now.timestamp()))
                    .build(),
            )
            .send()
            .await
            .expect("put");
    }

    let start = AwsDateTime::from_secs(now.timestamp() - 600);
    let end = AwsDateTime::from_secs(now.timestamp() + 600);
    let resp = cw
        .get_metric_data()
        .start_time(start)
        .end_time(end)
        .metric_data_queries(
            MetricDataQuery::builder()
                .id("q1")
                .label("error-sum")
                .metric_stat(
                    MetricStat::builder()
                        .metric(
                            CwMetric::builder()
                                .namespace("App")
                                .metric_name("Errors")
                                .build(),
                        )
                        .period(60)
                        .stat("Sum")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("metric data");

    let results = resp.metric_data_results();
    assert_eq!(results.len(), 1);
    let r = &results[0];
    assert_eq!(r.id(), Some("q1"));
    let values = r.values();
    assert_eq!(values.len(), 1);
    assert!((values[0] - 10.0).abs() < 1e-6);
}

#[tokio::test]
async fn get_metric_data_honors_scan_by() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    // Two datapoints, two periods apart -> two buckets: older=1.0, newer=2.0.
    for (offset, v) in [(-120i64, 1.0_f64), (0, 2.0)] {
        cw.put_metric_data()
            .namespace("Scan")
            .metric_data(
                MetricDatum::builder()
                    .metric_name("M")
                    .value(v)
                    .timestamp(AwsDateTime::from_secs(now.timestamp() + offset))
                    .build(),
            )
            .send()
            .await
            .expect("put");
    }

    let query = |scan: Option<aws_sdk_cloudwatch::types::ScanBy>| {
        let mut b = cw
            .get_metric_data()
            .start_time(AwsDateTime::from_secs(now.timestamp() - 600))
            .end_time(AwsDateTime::from_secs(now.timestamp() + 600))
            .metric_data_queries(
                MetricDataQuery::builder()
                    .id("q1")
                    .metric_stat(
                        MetricStat::builder()
                            .metric(
                                CwMetric::builder()
                                    .namespace("Scan")
                                    .metric_name("M")
                                    .build(),
                            )
                            .period(60)
                            .stat("Sum")
                            .build(),
                    )
                    .build(),
            );
        if let Some(s) = scan {
            b = b.scan_by(s);
        }
        b
    };

    // Default is TimestampDescending -> newest (2.0) first.
    let desc = query(None).send().await.unwrap();
    assert_eq!(desc.metric_data_results()[0].values(), &[2.0, 1.0]);

    // TimestampAscending -> oldest (1.0) first.
    let asc = query(Some(aws_sdk_cloudwatch::types::ScanBy::TimestampAscending))
        .send()
        .await
        .unwrap();
    assert_eq!(asc.metric_data_results()[0].values(), &[1.0, 2.0]);
}

#[tokio::test]
async fn alarm_lifecycle_and_set_state() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;

    cw.put_metric_alarm()
        .alarm_name("HighErrors")
        .alarm_description("alert when errors spike")
        .namespace("App")
        .metric_name("Errors")
        .statistic(Statistic::Sum)
        .period(60)
        .evaluation_periods(2)
        .threshold(10.0)
        .comparison_operator(ComparisonOperator::GreaterThanThreshold)
        .alarm_actions("arn:aws:sns:us-east-1:123456789012:ops")
        .send()
        .await
        .expect("put alarm");

    let listed = cw.describe_alarms().send().await.expect("describe");
    assert_eq!(listed.metric_alarms().len(), 1);
    let alarm = &listed.metric_alarms()[0];
    assert_eq!(alarm.alarm_name(), Some("HighErrors"));
    assert_eq!(alarm.state_value(), Some(&StateValue::InsufficientData));
    assert_eq!(alarm.threshold(), Some(10.0));
    assert_eq!(alarm.alarm_actions().len(), 1);

    cw.set_alarm_state()
        .alarm_name("HighErrors")
        .state_value(StateValue::Alarm)
        .state_reason("threshold breached")
        .send()
        .await
        .expect("set state");

    let after = cw.describe_alarms().send().await.expect("describe again");
    assert_eq!(
        after.metric_alarms()[0].state_value(),
        Some(&StateValue::Alarm)
    );

    cw.disable_alarm_actions()
        .alarm_names("HighErrors")
        .send()
        .await
        .expect("disable");
    let after_disable = cw.describe_alarms().send().await.expect("describe");
    assert_eq!(
        after_disable.metric_alarms()[0].actions_enabled(),
        Some(false)
    );

    cw.delete_alarms()
        .alarm_names("HighErrors")
        .send()
        .await
        .expect("delete");
    let final_list = cw.describe_alarms().send().await.expect("describe");
    assert!(final_list.metric_alarms().is_empty());
}

#[tokio::test]
async fn list_metrics_filters_by_dimension() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;

    cw.put_metric_data()
        .namespace("MultiDim")
        .metric_data(
            MetricDatum::builder()
                .metric_name("Hits")
                .value(1.0)
                .dimensions(Dimension::builder().name("Service").value("api").build())
                .build(),
        )
        .metric_data(
            MetricDatum::builder()
                .metric_name("Hits")
                .value(2.0)
                .dimensions(Dimension::builder().name("Service").value("worker").build())
                .build(),
        )
        .send()
        .await
        .expect("put");

    let listed = cw
        .list_metrics()
        .namespace("MultiDim")
        .dimensions(
            aws_sdk_cloudwatch::types::DimensionFilter::builder()
                .name("Service")
                .value("api")
                .build(),
        )
        .send()
        .await
        .expect("list filtered");
    assert_eq!(listed.metrics().len(), 1);
}

#[tokio::test]
async fn dashboard_put_get_delete_roundtrip() {
    // DeleteDashboards must return a parseable response (an empty
    // DeleteDashboardsResult element) — the SDK errors if that node is absent.
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;

    let body = r#"{"widgets":[{"type":"text","x":0,"y":0,"width":6,"height":3,"properties":{"markdown":"hi"}}]}"#;
    cw.put_dashboard()
        .dashboard_name("dash1")
        .dashboard_body(body)
        .send()
        .await
        .expect("put dashboard");

    let got = cw
        .get_dashboard()
        .dashboard_name("dash1")
        .send()
        .await
        .expect("get dashboard");
    assert!(got.dashboard_body().unwrap().contains("markdown"));

    // Delete must succeed (response deserializes thanks to the result node).
    cw.delete_dashboards()
        .dashboard_names("dash1")
        .send()
        .await
        .expect("delete dashboards");

    let listed = cw.list_dashboards().send().await.expect("list");
    assert!(listed
        .dashboard_entries()
        .iter()
        .all(|e| e.dashboard_name() != Some("dash1")));
}

// bug-audit 2026-06-27, T1.14: GetMetricStatistics must honor the Unit filter
// instead of mixing datapoints across units.
#[tokio::test]
async fn get_metric_statistics_filters_by_unit() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    cw.put_metric_data()
        .namespace("U")
        .metric_data(
            MetricDatum::builder()
                .metric_name("M")
                .value(10.0)
                .unit(StandardUnit::Count)
                .timestamp(AwsDateTime::from_secs(now.timestamp()))
                .build(),
        )
        .metric_data(
            MetricDatum::builder()
                .metric_name("M")
                .value(999.0)
                .unit(StandardUnit::Milliseconds)
                .timestamp(AwsDateTime::from_secs(now.timestamp()))
                .build(),
        )
        .send()
        .await
        .expect("put");

    let stats = cw
        .get_metric_statistics()
        .namespace("U")
        .metric_name("M")
        .start_time(AwsDateTime::from_secs(now.timestamp() - 600))
        .end_time(AwsDateTime::from_secs(now.timestamp() + 600))
        .period(60)
        .statistics(Statistic::Sum)
        .unit(StandardUnit::Count)
        .send()
        .await
        .expect("stats");
    assert!(
        (stats.datapoints()[0].sum().unwrap() - 10.0).abs() < 1e-6,
        "only the Count datapoint aggregated, not the Milliseconds one"
    );
}

// bug-cluster: GetMetricStatistics must match dimensions as an EXACT set. A
// dimensionless query must only see dimensionless data (not aggregate every
// dimension combination), and an exact-set query returns only that combination.
#[tokio::test]
async fn get_metric_statistics_exact_dimension_matching() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    // Two datapoints with DIFFERENT dimension sets, plus one dimensionless.
    cw.put_metric_data()
        .namespace("Dim")
        .metric_data(
            MetricDatum::builder()
                .metric_name("M")
                .value(5.0)
                .dimensions(Dimension::builder().name("Service").value("api").build())
                .timestamp(AwsDateTime::from_secs(now.timestamp()))
                .build(),
        )
        .metric_data(
            MetricDatum::builder()
                .metric_name("M")
                .value(7.0)
                .dimensions(Dimension::builder().name("Service").value("worker").build())
                .timestamp(AwsDateTime::from_secs(now.timestamp()))
                .build(),
        )
        .send()
        .await
        .expect("put");

    let start = AwsDateTime::from_secs(now.timestamp() - 600);
    let end = AwsDateTime::from_secs(now.timestamp() + 600);

    // Dimensionless query: must be EMPTY (no datapoint published without dims).
    let dimensionless = cw
        .get_metric_statistics()
        .namespace("Dim")
        .metric_name("M")
        .start_time(start)
        .end_time(end)
        .period(60)
        .statistics(Statistic::Sum)
        .send()
        .await
        .expect("stats");
    assert!(
        dimensionless.datapoints().is_empty(),
        "dimensionless query must not aggregate dimensioned data"
    );

    // Exact-set query: only the api datapoint (5.0), not the worker one.
    let api = cw
        .get_metric_statistics()
        .namespace("Dim")
        .metric_name("M")
        .dimensions(Dimension::builder().name("Service").value("api").build())
        .start_time(start)
        .end_time(end)
        .period(60)
        .statistics(Statistic::Sum)
        .send()
        .await
        .expect("stats");
    assert_eq!(api.datapoints().len(), 1);
    assert!((api.datapoints()[0].sum().unwrap() - 5.0).abs() < 1e-6);
}

// bug-cluster: GetMetricData must evaluate metric-math Expression queries.
#[tokio::test]
async fn get_metric_data_evaluates_expression() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    for (name, v) in [("A", 5.0_f64), ("B", 7.0)] {
        cw.put_metric_data()
            .namespace("ExprNs")
            .metric_data(
                MetricDatum::builder()
                    .metric_name(name)
                    .value(v)
                    .timestamp(AwsDateTime::from_secs(now.timestamp()))
                    .build(),
            )
            .send()
            .await
            .expect("put");
    }

    let mk_metric_stat = |name: &str| {
        MetricStat::builder()
            .metric(
                CwMetric::builder()
                    .namespace("ExprNs")
                    .metric_name(name)
                    .build(),
            )
            .period(60)
            .stat("Sum")
            .build()
    };

    let resp = cw
        .get_metric_data()
        .start_time(AwsDateTime::from_secs(now.timestamp() - 600))
        .end_time(AwsDateTime::from_secs(now.timestamp() + 600))
        .metric_data_queries(
            MetricDataQuery::builder()
                .id("m1")
                .metric_stat(mk_metric_stat("A"))
                .return_data(false)
                .build(),
        )
        .metric_data_queries(
            MetricDataQuery::builder()
                .id("m2")
                .metric_stat(mk_metric_stat("B"))
                .return_data(false)
                .build(),
        )
        .metric_data_queries(
            MetricDataQuery::builder()
                .id("e1")
                .expression("m1+m2")
                .build(),
        )
        .send()
        .await
        .expect("metric data");

    let results = resp.metric_data_results();
    // Only the expression result is returned (the metric inputs set
    // ReturnData=false).
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].id(), Some("e1"));
    assert_eq!(results[0].values(), &[12.0]);
}

// bug-cluster: ExtendedStatistics (percentiles) must be computed.
#[tokio::test]
async fn get_metric_statistics_computes_percentiles() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    for v in [10.0, 20.0, 30.0] {
        cw.put_metric_data()
            .namespace("Pct")
            .metric_data(
                MetricDatum::builder()
                    .metric_name("M")
                    .value(v)
                    .timestamp(AwsDateTime::from_secs(now.timestamp()))
                    .build(),
            )
            .send()
            .await
            .expect("put");
    }

    let stats = cw
        .get_metric_statistics()
        .namespace("Pct")
        .metric_name("M")
        .start_time(AwsDateTime::from_secs(now.timestamp() - 600))
        .end_time(AwsDateTime::from_secs(now.timestamp() + 600))
        .period(60)
        .extended_statistics("p50")
        .send()
        .await
        .expect("stats");

    let dp = &stats.datapoints()[0];
    let ext = dp
        .extended_statistics()
        .expect("extended statistics present");
    // Median of {10,20,30} via linear interpolation = 20.
    assert!((ext.get("p50").copied().unwrap() - 20.0).abs() < 1e-6);
}

// bug-cluster: DescribeAlarms must paginate via MaxRecords + NextToken.
#[tokio::test]
async fn describe_alarms_paginates() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;

    for name in ["alarm-a", "alarm-b"] {
        cw.put_metric_alarm()
            .alarm_name(name)
            .namespace("App")
            .metric_name("Errors")
            .statistic(Statistic::Sum)
            .period(60)
            .evaluation_periods(1)
            .threshold(1.0)
            .comparison_operator(ComparisonOperator::GreaterThanThreshold)
            .send()
            .await
            .expect("put alarm");
    }

    let page1 = cw
        .describe_alarms()
        .max_records(1)
        .send()
        .await
        .expect("page1");
    assert_eq!(page1.metric_alarms().len(), 1);
    let token = page1.next_token().expect("next token on first page");

    let page2 = cw
        .describe_alarms()
        .max_records(1)
        .next_token(token)
        .send()
        .await
        .expect("page2");
    assert_eq!(page2.metric_alarms().len(), 1);
    assert_ne!(
        page1.metric_alarms()[0].alarm_name(),
        page2.metric_alarms()[0].alarm_name()
    );
}

// bug-cluster: ListMetrics must cap each page (500) and round-trip a NextToken.
#[tokio::test]
async fn list_metrics_paginates() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;

    // 501 distinct metrics -> first page 500 + NextToken, second page 1.
    // Published in batches to keep each request body small.
    let mut next = 0;
    while next < 501 {
        let mut put = cw.put_metric_data().namespace("Many");
        for i in next..(next + 100).min(501) {
            put = put.metric_data(
                MetricDatum::builder()
                    .metric_name(format!("M{i}"))
                    .value(1.0)
                    .build(),
            );
        }
        put.send().await.expect("put many");
        next += 100;
    }

    let page1 = cw
        .list_metrics()
        .namespace("Many")
        .send()
        .await
        .expect("page1");
    assert_eq!(page1.metrics().len(), 500);
    let token = page1.next_token().expect("next token");

    let page2 = cw
        .list_metrics()
        .namespace("Many")
        .next_token(token)
        .send()
        .await
        .expect("page2");
    assert_eq!(page2.metrics().len(), 1);
}

// bug-cluster: DescribeAlarmHistory must reflect real state transitions.
#[tokio::test]
async fn describe_alarm_history_records_transitions() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;

    cw.put_metric_alarm()
        .alarm_name("HistAlarm")
        .namespace("App")
        .metric_name("Errors")
        .statistic(Statistic::Sum)
        .period(60)
        .evaluation_periods(1)
        .threshold(1.0)
        .comparison_operator(ComparisonOperator::GreaterThanThreshold)
        .send()
        .await
        .expect("put alarm");

    cw.set_alarm_state()
        .alarm_name("HistAlarm")
        .state_value(StateValue::Alarm)
        .state_reason("breached")
        .send()
        .await
        .expect("set state");

    let history = cw
        .describe_alarm_history()
        .alarm_name("HistAlarm")
        .send()
        .await
        .expect("history");
    let items = history.alarm_history_items();
    assert!(!items.is_empty(), "history must not be empty");
    assert!(
        items
            .iter()
            .any(|i| i.history_item_type().map(|t| t.as_str()) == Some("StateUpdate")),
        "a StateUpdate history item must be recorded"
    );
}

// bug-cluster: DescribeAlarms honors AlarmTypes. AWS returns only metric alarms
// by default; composite alarms appear only when AlarmTypes includes them.
#[tokio::test]
async fn describe_alarms_honors_alarm_types() {
    use aws_sdk_cloudwatch::types::AlarmType;
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;

    cw.put_metric_alarm()
        .alarm_name("m-alarm")
        .namespace("App")
        .metric_name("Errors")
        .statistic(Statistic::Sum)
        .period(60)
        .evaluation_periods(1)
        .threshold(1.0)
        .comparison_operator(ComparisonOperator::GreaterThanThreshold)
        .send()
        .await
        .expect("put metric alarm");
    cw.put_composite_alarm()
        .alarm_name("c-alarm")
        .alarm_rule("ALARM(m-alarm)")
        .send()
        .await
        .expect("put composite alarm");

    // Default: metric alarms only, no composite alarms.
    let default = cw.describe_alarms().send().await.expect("default");
    assert_eq!(default.metric_alarms().len(), 1);
    assert!(
        default.composite_alarms().is_empty(),
        "default DescribeAlarms must not return composite alarms"
    );

    // AlarmTypes = CompositeAlarm: only the composite alarm.
    let composite = cw
        .describe_alarms()
        .alarm_types(AlarmType::CompositeAlarm)
        .send()
        .await
        .expect("composite only");
    assert!(composite.metric_alarms().is_empty());
    assert_eq!(composite.composite_alarms().len(), 1);

    // Both types requested: both returned.
    let both = cw
        .describe_alarms()
        .alarm_types(AlarmType::MetricAlarm)
        .alarm_types(AlarmType::CompositeAlarm)
        .send()
        .await
        .expect("both");
    assert_eq!(both.metric_alarms().len(), 1);
    assert_eq!(both.composite_alarms().len(), 1);
}

// bug-cluster: a metric-math divide-by-zero yields NaN internally; AWS emits no
// datapoint for a NaN/infinite result, so no NaN must reach the wire.
#[tokio::test]
async fn get_metric_data_drops_nan_from_divide_by_zero() {
    let server = TestServer::start().await;
    let cw = server.cloudwatch_client().await;
    let now = chrono::Utc::now();

    // A=10 at t; B is never put (its series is empty), but even a present zero
    // divisor must not surface NaN. Put A and a zero-valued B at the same time.
    for (name, v) in [("A", 10.0_f64), ("B", 0.0)] {
        cw.put_metric_data()
            .namespace("NanNs")
            .metric_data(
                MetricDatum::builder()
                    .metric_name(name)
                    .value(v)
                    .timestamp(AwsDateTime::from_secs(now.timestamp()))
                    .build(),
            )
            .send()
            .await
            .expect("put");
    }

    let mk_stat = |name: &str| {
        MetricStat::builder()
            .metric(
                CwMetric::builder()
                    .namespace("NanNs")
                    .metric_name(name)
                    .build(),
            )
            .period(60)
            .stat("Sum")
            .build()
    };

    let resp = cw
        .get_metric_data()
        .start_time(AwsDateTime::from_secs(now.timestamp() - 600))
        .end_time(AwsDateTime::from_secs(now.timestamp() + 600))
        .metric_data_queries(
            MetricDataQuery::builder()
                .id("a")
                .metric_stat(mk_stat("A"))
                .return_data(false)
                .build(),
        )
        .metric_data_queries(
            MetricDataQuery::builder()
                .id("b")
                .metric_stat(mk_stat("B"))
                .return_data(false)
                .build(),
        )
        .metric_data_queries(MetricDataQuery::builder().id("e").expression("a/b").build())
        .send()
        .await
        .expect("metric data");

    let results = resp.metric_data_results();
    let e = results
        .iter()
        .find(|r| r.id() == Some("e"))
        .expect("e result");
    // The divide-by-zero datapoint is dropped entirely — no value, and nothing
    // that is NaN or infinite.
    assert!(
        e.values().iter().all(|v| v.is_finite()),
        "no NaN/infinite datapoint may reach the wire: {:?}",
        e.values()
    );
    assert!(
        e.values().is_empty(),
        "divide-by-zero must produce no datapoint: {:?}",
        e.values()
    );
}
