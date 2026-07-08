//! End-to-end tests for Amazon Timestream, driven through the real
//! `aws-sdk-timestreamwrite` and `aws-sdk-timestreamquery` clients against a
//! live fakecloud server.
//!
//! Timestream normally uses endpoint discovery: a client calls
//! `DescribeEndpoints` and routes follow-up calls to the returned `Address`.
//! The AWS SDK's opt-in auto-discovery mode forces `https://<address>`, which a
//! plain-HTTP fakecloud server can't serve, so these tests use the default
//! (non-discovery) clients pinned to the fakecloud endpoint and call
//! `DescribeEndpoints` explicitly to prove it echoes the fakecloud host back
//! (the address a real SDK would then dial).

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn timestream_write_then_query_round_trip() {
    let server = TestServer::start().await;
    let write = server.timestream_write_client().await;
    let query = server.timestream_query_client().await;

    // --- DescribeEndpoints echoes the fakecloud host (endpoint discovery) ---
    let endpoints = write
        .describe_endpoints()
        .send()
        .await
        .expect("write describe_endpoints")
        .endpoints;
    assert!(!endpoints.is_empty(), "endpoints must not be empty");
    let addr = endpoints[0].address();
    assert!(!addr.is_empty(), "endpoint address must not be empty");
    assert_eq!(endpoints[0].cache_period_in_minutes(), 1440);

    let q_endpoints = query
        .describe_endpoints()
        .send()
        .await
        .expect("query describe_endpoints")
        .endpoints;
    assert!(!q_endpoints.is_empty());

    // --- Create database ---
    let db = write
        .create_database()
        .database_name("e2e-metrics")
        .send()
        .await
        .expect("create_database")
        .database
        .expect("database present");
    assert_eq!(db.database_name(), Some("e2e-metrics"));
    assert!(db
        .arn()
        .unwrap_or_default()
        .ends_with(":database/e2e-metrics"));

    // --- Create table ---
    let table = write
        .create_table()
        .database_name("e2e-metrics")
        .table_name("cpu")
        .send()
        .await
        .expect("create_table")
        .table
        .expect("table present");
    assert_eq!(table.table_name(), Some("cpu"));
    assert_eq!(
        table.table_status(),
        Some(&aws_sdk_timestreamwrite::types::TableStatus::Active)
    );

    // --- Write records ---
    use aws_sdk_timestreamwrite::types::{Dimension, MeasureValueType, Record, TimeUnit};
    let rec = |host: &str, value: &str, t: &str| {
        Record::builder()
            .dimensions(
                Dimension::builder()
                    .name("host")
                    .value(host)
                    .build()
                    .expect("dimension"),
            )
            .measure_name("cpu_utilization")
            .measure_value(value)
            .measure_value_type(MeasureValueType::Double)
            .time(t)
            .time_unit(TimeUnit::Milliseconds)
            .build()
    };
    let ingested = write
        .write_records()
        .database_name("e2e-metrics")
        .table_name("cpu")
        .records(rec("host-a", "42.5", "1700000000000"))
        .records(rec("host-b", "13.25", "1700000001000"))
        .send()
        .await
        .expect("write_records")
        .records_ingested
        .expect("records ingested");
    assert_eq!(ingested.total(), 2);

    // --- Query the written rows back (query client -> shared store) ---
    let out = query
        .query()
        .query_string(r#"SELECT * FROM "e2e-metrics"."cpu" ORDER BY time ASC"#)
        .send()
        .await
        .expect("query select *");
    assert_eq!(out.rows().len(), 2, "expected the two written points back");
    assert!(!out.query_id().is_empty());
    // Column layout: host (dimension), measure_name, measure_value::double, time.
    let col_names: Vec<&str> = out.column_info().iter().filter_map(|c| c.name()).collect();
    assert!(col_names.contains(&"host"));
    assert!(col_names.contains(&"measure_name"));
    assert!(col_names.contains(&"time"));
    assert!(col_names.iter().any(|c| c.starts_with("measure_value::")));

    // First row's first datum is the "host" dimension value.
    let first = &out.rows()[0];
    assert_eq!(
        first.data()[0].scalar_value(),
        Some("host-a"),
        "first ordered row should be host-a"
    );

    // --- COUNT(*) ---
    let count = query
        .query()
        .query_string(r#"SELECT COUNT(*) FROM "e2e-metrics"."cpu""#)
        .send()
        .await
        .expect("query count");
    assert_eq!(count.rows().len(), 1);
    assert_eq!(count.rows()[0].data()[0].scalar_value(), Some("2"));

    // --- PrepareQuery returns column info ---
    let prep = query
        .prepare_query()
        .query_string(r#"SELECT * FROM "e2e-metrics"."cpu""#)
        .send()
        .await
        .expect("prepare_query");
    assert!(!prep.columns().is_empty());

    // --- Unsupported query shape -> ValidationException, not a wrong success ---
    let err = query
        .query()
        .query_string(r#"SELECT avg(measure_value::double) FROM "e2e-metrics"."cpu""#)
        .send()
        .await
        .expect_err("unsupported projection must be rejected");
    assert!(err.into_service_error().is_validation_exception());

    // --- List / describe / tag / delete ---
    let tables = write
        .list_tables()
        .database_name("e2e-metrics")
        .send()
        .await
        .expect("list_tables")
        .tables
        .unwrap_or_default();
    assert!(tables.iter().any(|t| t.table_name() == Some("cpu")));

    let db_arn = db.arn().unwrap().to_string();
    write
        .tag_resource()
        .resource_arn(&db_arn)
        .tags(
            aws_sdk_timestreamwrite::types::Tag::builder()
                .key("team")
                .value("obs")
                .build()
                .expect("tag"),
        )
        .send()
        .await
        .expect("tag_resource");
    let tags = write
        .list_tags_for_resource()
        .resource_arn(&db_arn)
        .send()
        .await
        .expect("list_tags")
        .tags
        .unwrap_or_default();
    assert!(tags.iter().any(|t| t.key() == "team" && t.value() == "obs"));

    // Deleting a non-empty database is rejected.
    let err = write
        .delete_database()
        .database_name("e2e-metrics")
        .send()
        .await
        .expect_err("delete non-empty database must fail");
    assert!(err.into_service_error().is_validation_exception());

    write
        .delete_table()
        .database_name("e2e-metrics")
        .table_name("cpu")
        .send()
        .await
        .expect("delete_table");
    write
        .delete_database()
        .database_name("e2e-metrics")
        .send()
        .await
        .expect("delete_database after table removed");
}

#[tokio::test]
async fn timestream_scheduled_query_and_account_settings() {
    let server = TestServer::start().await;
    let query = server.timestream_query_client().await;

    // Account settings round-trip.
    let settings = query
        .describe_account_settings()
        .send()
        .await
        .expect("describe_account_settings");
    assert!(settings.query_pricing_model().is_some());

    // Scheduled query CRUD.
    use aws_sdk_timestreamquery::types::{
        ErrorReportConfiguration, NotificationConfiguration, S3Configuration,
        ScheduleConfiguration, ScheduledQueryState, SnsConfiguration,
    };
    let arn = query
        .create_scheduled_query()
        .name("e2e-sq")
        .query_string(r#"SELECT COUNT(*) FROM "db"."tbl""#)
        .schedule_configuration(
            ScheduleConfiguration::builder()
                .schedule_expression("rate(1 hour)")
                .build()
                .expect("schedule"),
        )
        .notification_configuration(
            NotificationConfiguration::builder()
                .sns_configuration(
                    SnsConfiguration::builder()
                        .topic_arn("arn:aws:sns:us-east-1:000000000000:t")
                        .build()
                        .expect("sns"),
                )
                .build(),
        )
        .scheduled_query_execution_role_arn("arn:aws:iam::000000000000:role/r")
        .error_report_configuration(
            ErrorReportConfiguration::builder()
                .s3_configuration(
                    S3Configuration::builder()
                        .bucket_name("errbucket")
                        .build()
                        .expect("s3"),
                )
                .build(),
        )
        .send()
        .await
        .expect("create_scheduled_query")
        .arn;
    assert!(arn.contains(":scheduled-query/"));

    let described = query
        .describe_scheduled_query()
        .scheduled_query_arn(&arn)
        .send()
        .await
        .expect("describe_scheduled_query")
        .scheduled_query
        .expect("scheduled query present");
    assert_eq!(described.state(), &ScheduledQueryState::Enabled);

    query
        .update_scheduled_query()
        .scheduled_query_arn(&arn)
        .state(ScheduledQueryState::Disabled)
        .send()
        .await
        .expect("update_scheduled_query");

    let listed = query
        .list_scheduled_queries()
        .send()
        .await
        .expect("list_scheduled_queries")
        .scheduled_queries;
    assert!(listed.iter().any(|s| s.arn() == arn));

    query
        .delete_scheduled_query()
        .scheduled_query_arn(&arn)
        .send()
        .await
        .expect("delete_scheduled_query");
}
