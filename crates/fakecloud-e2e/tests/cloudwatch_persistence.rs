mod helpers;

use aws_sdk_cloudwatch::types::{ComparisonOperator, StandardUnit, Statistic};
use helpers::TestServer;

/// Alarm + dashboard survive a restart. Audit 2026-05-10 flagged CW
/// state as in-memory only; this guards against regression.
#[tokio::test]
async fn persistence_round_trip_alarm_and_dashboard() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let cw = aws_sdk_cloudwatch::Client::new(&server.aws_config().await);

    cw.put_metric_alarm()
        .alarm_name("persist-alarm")
        .metric_name("CPUUtilization")
        .namespace("AWS/EC2")
        .statistic(Statistic::Average)
        .period(60)
        .evaluation_periods(1)
        .threshold(80.0)
        .comparison_operator(ComparisonOperator::GreaterThanThreshold)
        .unit(StandardUnit::Percent)
        .send()
        .await
        .unwrap();

    cw.put_dashboard()
        .dashboard_name("persist-dashboard")
        .dashboard_body(r#"{"widgets": []}"#)
        .send()
        .await
        .unwrap();

    drop(cw);
    server.restart().await;
    let cw = aws_sdk_cloudwatch::Client::new(&server.aws_config().await);

    let alarms = cw
        .describe_alarms()
        .alarm_names("persist-alarm")
        .send()
        .await
        .unwrap();
    assert_eq!(
        alarms.metric_alarms().len(),
        1,
        "alarm should survive restart"
    );
    assert_eq!(
        alarms.metric_alarms()[0].alarm_name(),
        Some("persist-alarm")
    );

    let dashboards = cw.list_dashboards().send().await.unwrap();
    let names: Vec<_> = dashboards
        .dashboard_entries()
        .iter()
        .filter_map(|d| d.dashboard_name())
        .collect();
    assert!(
        names.contains(&"persist-dashboard"),
        "dashboard should survive restart; got {names:?}"
    );
}
