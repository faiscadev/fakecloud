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
