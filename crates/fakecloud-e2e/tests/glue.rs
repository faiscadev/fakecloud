//! Glue Data Catalog E2E.

mod helpers;

use aws_sdk_glue::types::{Column, DatabaseInput, PartitionInput, StorageDescriptor, TableInput};
use helpers::TestServer;

fn table_input(name: &str) -> TableInput {
    TableInput::builder()
        .name(name)
        .description("test table")
        .table_type("EXTERNAL_TABLE")
        .partition_keys(
            Column::builder()
                .name("dt")
                .r#type("string")
                .build()
                .unwrap(),
        )
        .storage_descriptor(
            StorageDescriptor::builder()
                .columns(
                    Column::builder()
                        .name("id")
                        .r#type("string")
                        .build()
                        .unwrap(),
                )
                .columns(
                    Column::builder()
                        .name("amount")
                        .r#type("bigint")
                        .build()
                        .unwrap(),
                )
                .location("s3://example/test")
                .input_format("org.apache.hadoop.mapred.TextInputFormat")
                .output_format("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .build(),
        )
        .build()
        .expect("table input")
}

#[tokio::test]
async fn database_lifecycle() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(
            DatabaseInput::builder()
                .name("salesdb")
                .description("sales data")
                .build()
                .expect("db input"),
        )
        .send()
        .await
        .expect("create");

    let got = glue
        .get_database()
        .name("salesdb")
        .send()
        .await
        .expect("get");
    let db = got.database().expect("db");
    assert_eq!(db.name(), "salesdb");
    assert_eq!(db.description(), Some("sales data"));

    let listed = glue.get_databases().send().await.expect("list");
    assert!(listed.database_list().iter().any(|d| d.name() == "salesdb"));

    glue.update_database()
        .name("salesdb")
        .database_input(
            DatabaseInput::builder()
                .name("salesdb")
                .description("updated")
                .build()
                .expect("db input"),
        )
        .send()
        .await
        .expect("update");

    let after = glue
        .get_database()
        .name("salesdb")
        .send()
        .await
        .expect("get after update");
    assert_eq!(after.database().unwrap().description(), Some("updated"));

    glue.delete_database()
        .name("salesdb")
        .send()
        .await
        .expect("delete");

    let err = glue
        .get_database()
        .name("salesdb")
        .send()
        .await
        .expect_err("not found");
    assert!(err.into_service_error().is_entity_not_found_exception());
}

#[tokio::test]
async fn duplicate_database_returns_already_exists() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("dup").build().unwrap())
        .send()
        .await
        .expect("create");

    let err = glue
        .create_database()
        .database_input(DatabaseInput::builder().name("dup").build().unwrap())
        .send()
        .await
        .expect_err("dup");
    assert!(err.into_service_error().is_already_exists_exception());
}

#[tokio::test]
async fn table_lifecycle() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("warehouse").build().unwrap())
        .send()
        .await
        .expect("create db");

    glue.create_table()
        .database_name("warehouse")
        .table_input(table_input("orders"))
        .send()
        .await
        .expect("create table");

    let got = glue
        .get_table()
        .database_name("warehouse")
        .name("orders")
        .send()
        .await
        .expect("get table");
    let table = got.table().expect("table");
    assert_eq!(table.name(), "orders");
    assert_eq!(table.database_name(), Some("warehouse"));
    assert_eq!(table.partition_keys().len(), 1);
    assert_eq!(
        table
            .storage_descriptor()
            .and_then(|sd| sd.location())
            .unwrap_or_default(),
        "s3://example/test"
    );

    let listed = glue
        .get_tables()
        .database_name("warehouse")
        .send()
        .await
        .expect("list tables");
    assert_eq!(listed.table_list().len(), 1);

    glue.delete_table()
        .database_name("warehouse")
        .name("orders")
        .send()
        .await
        .expect("delete");

    let err = glue
        .get_table()
        .database_name("warehouse")
        .name("orders")
        .send()
        .await
        .expect_err("gone");
    assert!(err.into_service_error().is_entity_not_found_exception());
}

#[tokio::test]
async fn partition_lifecycle() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("dl").build().unwrap())
        .send()
        .await
        .expect("create db");

    glue.create_table()
        .database_name("dl")
        .table_input(table_input("events"))
        .send()
        .await
        .expect("create table");

    glue.create_partition()
        .database_name("dl")
        .table_name("events")
        .partition_input(
            PartitionInput::builder()
                .values("2026-04-30".to_string())
                .storage_descriptor(
                    StorageDescriptor::builder()
                        .location("s3://dl/events/dt=2026-04-30/")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("create partition");

    let got = glue
        .get_partition()
        .database_name("dl")
        .table_name("events")
        .partition_values("2026-04-30")
        .send()
        .await
        .expect("get partition");
    assert_eq!(got.partition().unwrap().values(), &["2026-04-30"]);

    let listed = glue
        .get_partitions()
        .database_name("dl")
        .table_name("events")
        .send()
        .await
        .expect("list partitions");
    assert_eq!(listed.partitions().len(), 1);

    glue.batch_create_partition()
        .database_name("dl")
        .table_name("events")
        .partition_input_list(
            PartitionInput::builder()
                .values("2026-05-01".to_string())
                .build(),
        )
        .partition_input_list(
            PartitionInput::builder()
                .values("2026-05-02".to_string())
                .build(),
        )
        .send()
        .await
        .expect("batch create");

    let after = glue
        .get_partitions()
        .database_name("dl")
        .table_name("events")
        .send()
        .await
        .expect("list after batch");
    assert_eq!(after.partitions().len(), 3);

    glue.delete_partition()
        .database_name("dl")
        .table_name("events")
        .partition_values("2026-04-30")
        .send()
        .await
        .expect("delete partition");

    let err = glue
        .get_partition()
        .database_name("dl")
        .table_name("events")
        .partition_values("2026-04-30")
        .send()
        .await
        .expect_err("gone");
    assert!(err.into_service_error().is_entity_not_found_exception());
}

#[tokio::test]
async fn jobs_and_job_runs_introspection() {
    use aws_sdk_glue::types::JobCommand;
    use serde_json::Value;

    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_job()
        .name("etl-job")
        .role("arn:aws:iam::123456789012:role/glue")
        .command(
            JobCommand::builder()
                .name("glueetl")
                .script_location("s3://example/script.py")
                .python_version("3")
                .build(),
        )
        .max_retries(2)
        .send()
        .await
        .expect("create job");

    let run = glue
        .start_job_run()
        .job_name("etl-job")
        .send()
        .await
        .expect("start run");
    let run_id = run.job_run_id().expect("run id").to_string();

    let client = reqwest::Client::new();
    let jobs: Value = client
        .get(format!("{}/_fakecloud/glue/jobs", server.endpoint()))
        .send()
        .await
        .expect("jobs request")
        .json()
        .await
        .expect("jobs json");
    let jobs_arr = jobs["jobs"].as_array().expect("jobs array");
    assert_eq!(jobs_arr.len(), 1);
    assert_eq!(jobs_arr[0]["name"], "etl-job");
    assert_eq!(jobs_arr[0]["role"], "arn:aws:iam::123456789012:role/glue");
    assert_eq!(jobs_arr[0]["maxRetries"], 2);
    assert_eq!(jobs_arr[0]["command"]["Name"], "glueetl");

    let runs: Value = client
        .get(format!("{}/_fakecloud/glue/job-runs", server.endpoint()))
        .send()
        .await
        .expect("runs request")
        .json()
        .await
        .expect("runs json");
    let runs_arr = runs["runs"].as_array().expect("runs array");
    assert_eq!(runs_arr.len(), 1);
    assert_eq!(runs_arr[0]["id"], run_id);
    assert_eq!(runs_arr[0]["jobName"], "etl-job");
    assert_eq!(runs_arr[0]["jobRunState"], "SUCCEEDED");

    // Filter by job_name
    let filtered: Value = client
        .get(format!(
            "{}/_fakecloud/glue/job-runs?job_name=etl-job",
            server.endpoint()
        ))
        .send()
        .await
        .expect("filter request")
        .json()
        .await
        .expect("filter json");
    assert_eq!(filtered["runs"].as_array().unwrap().len(), 1);

    let none: Value = client
        .get(format!(
            "{}/_fakecloud/glue/job-runs?job_name=missing",
            server.endpoint()
        ))
        .send()
        .await
        .expect("none request")
        .json()
        .await
        .expect("none json");
    assert!(none["runs"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn cloudwatch_and_crawler_introspection() {
    use aws_sdk_cloudwatch::types::{ComparisonOperator, Dimension, MetricDatum, Statistic};
    use aws_sdk_glue::types::{CrawlerTargets, JdbcTarget, S3Target};
    use serde_json::Value;

    let server = TestServer::start().await;
    let glue = server.glue_client().await;
    let cw = server.cloudwatch_client().await;

    // Metric alarm.
    cw.put_metric_alarm()
        .alarm_name("HighErrors")
        .namespace("MyApp")
        .metric_name("Errors")
        .statistic(Statistic::Sum)
        .period(60)
        .evaluation_periods(1)
        .threshold(10.0)
        .comparison_operator(ComparisonOperator::GreaterThanThreshold)
        .alarm_actions("arn:aws:sns:us-east-1:123456789012:ops")
        .send()
        .await
        .expect("put alarm");

    // Metric datapoint.
    cw.put_metric_data()
        .namespace("MyApp")
        .metric_data(
            MetricDatum::builder()
                .metric_name("Requests")
                .value(42.0)
                .unit(aws_sdk_cloudwatch::types::StandardUnit::Count)
                .dimensions(Dimension::builder().name("Service").value("api").build())
                .build(),
        )
        .send()
        .await
        .expect("put metric data");

    // Crawler.
    glue.create_crawler()
        .name("inventory")
        .role("arn:aws:iam::123456789012:role/glue")
        .database_name("analytics")
        .targets(
            CrawlerTargets::builder()
                .s3_targets(S3Target::builder().path("s3://b/a").build())
                .jdbc_targets(JdbcTarget::builder().connection_name("conn").build())
                .build(),
        )
        .send()
        .await
        .expect("create crawler");

    let client = reqwest::Client::new();

    // Alarms endpoint.
    let alarms: Value = client
        .get(format!(
            "{}/_fakecloud/cloudwatch/alarms",
            server.endpoint()
        ))
        .send()
        .await
        .expect("alarms request")
        .json()
        .await
        .expect("alarms json");
    let alarms_arr = alarms["alarms"].as_array().expect("alarms array");
    assert_eq!(alarms_arr.len(), 1);
    let a = &alarms_arr[0];
    assert_eq!(a["name"], "HighErrors");
    assert_eq!(a["type"], "metric");
    assert_eq!(a["namespace"], "MyApp");
    assert_eq!(a["metricName"], "Errors");
    assert_eq!(a["threshold"], 10.0);
    assert_eq!(a["comparisonOperator"], "GreaterThanThreshold");
    assert_eq!(a["state"], "INSUFFICIENT_DATA");
    assert_eq!(
        a["alarmActions"][0],
        "arn:aws:sns:us-east-1:123456789012:ops"
    );

    // Metrics endpoint.
    let metrics: Value = client
        .get(format!(
            "{}/_fakecloud/cloudwatch/metrics",
            server.endpoint()
        ))
        .send()
        .await
        .expect("metrics request")
        .json()
        .await
        .expect("metrics json");
    let metrics_arr = metrics["metrics"].as_array().expect("metrics array");
    assert_eq!(metrics_arr.len(), 1);
    let m = &metrics_arr[0];
    assert_eq!(m["namespace"], "MyApp");
    assert_eq!(m["metricName"], "Requests");
    assert_eq!(m["datapointCount"], 1);
    assert_eq!(m["dimensions"][0]["name"], "Service");
    assert_eq!(m["dimensions"][0]["value"], "api");
    assert_eq!(m["latest"]["value"], 42.0);
    assert_eq!(m["latest"]["unit"], "Count");

    // Crawlers endpoint.
    let crawlers: Value = client
        .get(format!("{}/_fakecloud/glue/crawlers", server.endpoint()))
        .send()
        .await
        .expect("crawlers request")
        .json()
        .await
        .expect("crawlers json");
    let crawlers_arr = crawlers["crawlers"].as_array().expect("crawlers array");
    assert_eq!(crawlers_arr.len(), 1);
    let c = &crawlers_arr[0];
    assert_eq!(c["name"], "inventory");
    assert_eq!(c["role"], "arn:aws:iam::123456789012:role/glue");
    assert_eq!(c["databaseName"], "analytics");
    assert_eq!(c["state"], "READY");
    assert_eq!(c["targetSummary"], "1 S3, 1 JDBC");
}

#[tokio::test]
async fn table_in_missing_database_returns_not_found() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    let err = glue
        .create_table()
        .database_name("ghost")
        .table_input(table_input("t"))
        .send()
        .await
        .expect_err("missing db");
    assert!(err.into_service_error().is_entity_not_found_exception());
}
