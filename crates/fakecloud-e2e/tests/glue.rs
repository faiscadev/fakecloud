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
async fn table_versions_are_archived_on_create_and_update() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("versdb").build().unwrap())
        .send()
        .await
        .expect("create db");

    glue.create_table()
        .database_name("versdb")
        .table_input(table_input("t"))
        .send()
        .await
        .expect("create table");

    // A create archives version 1.
    let v1 = glue
        .get_table_versions()
        .database_name("versdb")
        .table_name("t")
        .send()
        .await
        .expect("get versions after create");
    assert_eq!(v1.table_versions().len(), 1);
    assert_eq!(v1.table_versions()[0].version_id(), Some("1"));

    // An update archives a second version with the mutated table.
    glue.update_table()
        .database_name("versdb")
        .table_input(
            TableInput::builder()
                .name("t")
                .description("updated description")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("update table");

    let v2 = glue
        .get_table_versions()
        .database_name("versdb")
        .table_name("t")
        .send()
        .await
        .expect("get versions after update");
    assert_eq!(
        v2.table_versions().len(),
        2,
        "update must archive a new version"
    );

    // GetTableVersion with no VersionId returns the latest (2), reflecting the
    // updated description rather than a synthesized default.
    let latest = glue
        .get_table_version()
        .database_name("versdb")
        .table_name("t")
        .send()
        .await
        .expect("get latest version");
    let tv = latest.table_version().expect("table version");
    assert_eq!(tv.version_id(), Some("2"));
    assert_eq!(
        tv.table().and_then(|t| t.description()),
        Some("updated description")
    );

    // Deleting the table purges its version archive.
    glue.delete_table()
        .database_name("versdb")
        .name("t")
        .send()
        .await
        .expect("delete");
    let after = glue
        .get_table_versions()
        .database_name("versdb")
        .table_name("t")
        .send()
        .await
        .expect("get versions after delete");
    assert!(after.table_versions().is_empty());
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
    // Partitions report their owning catalog id (the account) so the
    // Terraform `aws_glue_partition` resource sees a stable `catalog_id`.
    assert_eq!(got.partition().unwrap().catalog_id(), Some("123456789012"));

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

#[tokio::test]
async fn create_script_reflects_dag_nodes() {
    use aws_sdk_glue::types::{CodeGenEdge, CodeGenNode, CodeGenNodeArg};
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    let resp = glue
        .create_script()
        .dag_nodes(
            CodeGenNode::builder()
                .id("datasource0")
                .node_type("DataSource")
                .args(
                    CodeGenNodeArg::builder()
                        .name("database")
                        .value("warehouse")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .dag_nodes(
            CodeGenNode::builder()
                .id("datasink0")
                .node_type("DataSink")
                .args(
                    CodeGenNodeArg::builder()
                        .name("name")
                        .value("sink")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .dag_edges(
            CodeGenEdge::builder()
                .source("datasource0")
                .target("datasink0")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create script");

    let py = resp.python_script().unwrap_or_default();
    // Script must reference the actual nodes, not be a constant.
    assert!(py.contains("datasource0 = DataSource.apply"), "py=\n{py}");
    assert!(py.contains("datasink0 = DataSink.apply"), "py=\n{py}");
    assert!(py.contains("database = \"warehouse\""), "py=\n{py}");
    // The edge must wire the sink to its upstream source.
    assert!(py.contains("frame = datasource0"), "py=\n{py}");
    assert!(resp
        .scala_code()
        .unwrap_or_default()
        .contains("val datasink0"));
}

#[tokio::test]
async fn get_dataflow_graph_round_trips_script() {
    use aws_sdk_glue::types::{CodeGenNode, CodeGenNodeArg};
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    // Generate a script, then parse it back into a DAG.
    let script = glue
        .create_script()
        .dag_nodes(
            CodeGenNode::builder()
                .id("src")
                .node_type("DataSource")
                .args(
                    CodeGenNodeArg::builder()
                        .name("database")
                        .value("db")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .python_script()
        .unwrap()
        .to_string();

    let graph = glue
        .get_dataflow_graph()
        .python_script(script)
        .send()
        .await
        .expect("dataflow graph");
    let ids: Vec<&str> = graph.dag_nodes().iter().map(|n| n.id()).collect();
    assert!(ids.contains(&"src"), "ids={ids:?}");
}

#[tokio::test]
async fn get_mapping_derives_from_table_schema() {
    use aws_sdk_glue::types::CatalogEntry;
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("m_db").build().unwrap())
        .send()
        .await
        .unwrap();
    glue.create_table()
        .database_name("m_db")
        .table_input(table_input("orders"))
        .send()
        .await
        .unwrap();

    let resp = glue
        .get_mapping()
        .source(
            CatalogEntry::builder()
                .database_name("m_db")
                .table_name("orders")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("get mapping");
    let paths: Vec<&str> = resp
        .mapping()
        .iter()
        .filter_map(|m| m.source_path())
        .collect();
    // Mapping must reflect the table's columns, not be empty.
    assert!(paths.contains(&"id"), "paths={paths:?}");
    assert!(paths.contains(&"amount"), "paths={paths:?}");
}

#[tokio::test]
async fn list_and_describe_entity_reflect_catalog() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("e_db").build().unwrap())
        .send()
        .await
        .unwrap();
    glue.create_table()
        .database_name("e_db")
        .table_input(table_input("orders"))
        .send()
        .await
        .unwrap();

    // ListEntities reflects catalog tables.
    let listed = glue.list_entities().send().await.expect("list entities");
    let names: Vec<&str> = listed
        .entities()
        .iter()
        .filter_map(|e| e.entity_name())
        .collect();
    assert!(names.contains(&"orders"), "names={names:?}");

    // DescribeEntity returns the table's columns as fields.
    let described = glue
        .describe_entity()
        .connection_name("conn")
        .entity_name("orders")
        .send()
        .await
        .expect("describe entity");
    let field_names: Vec<&str> = described
        .fields()
        .iter()
        .filter_map(|f| f.field_name())
        .collect();
    assert!(field_names.contains(&"id"), "fields={field_names:?}");
    assert!(field_names.contains(&"amount"), "fields={field_names:?}");
}

#[tokio::test]
async fn test_connection_named_must_exist() {
    use aws_sdk_glue::types::ConnectionInput;
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    // A missing connection is a real error, not a fake empty success.
    let err = glue
        .test_connection()
        .connection_name("nope")
        .send()
        .await
        .expect_err("missing connection");
    assert!(err.into_service_error().is_entity_not_found_exception());

    // After creating it, the test succeeds.
    glue.create_connection()
        .connection_input(
            ConnectionInput::builder()
                .name("real-conn")
                .connection_type(aws_sdk_glue::types::ConnectionType::Jdbc)
                .connection_properties(
                    aws_sdk_glue::types::ConnectionPropertyKey::JdbcConnectionUrl,
                    "jdbc:postgresql://host:5432/db",
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    glue.test_connection()
        .connection_name("real-conn")
        .send()
        .await
        .expect("test existing connection");
}

#[tokio::test]
async fn table_storage_descriptor_round_trips_full_shape() {
    use aws_sdk_glue::types::{Order, SkewedInfo};

    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("sd").build().unwrap())
        .send()
        .await
        .expect("create db");

    let sd = StorageDescriptor::builder()
        .columns(
            Column::builder()
                .name("c1")
                .r#type("int")
                .parameters("p", "v")
                .build()
                .unwrap(),
        )
        .location("s3://sd/t/")
        .bucket_columns("c1")
        .number_of_buckets(4)
        .stored_as_sub_directories(false)
        .sort_columns(Order::builder().column("c1").sort_order(1).build().unwrap())
        .skewed_info(
            SkewedInfo::builder()
                .skewed_column_names("c1")
                .skewed_column_values("v1")
                .skewed_column_value_location_maps("v1", "loc")
                .build(),
        )
        .build();

    glue.create_table()
        .database_name("sd")
        .table_input(
            TableInput::builder()
                .name("t")
                .storage_descriptor(sd)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create table");

    let got = glue
        .get_table()
        .database_name("sd")
        .name("t")
        .send()
        .await
        .expect("get table");
    let rsd = got.table().unwrap().storage_descriptor().unwrap();
    assert_eq!(rsd.bucket_columns(), &["c1"]);
    assert_eq!(rsd.number_of_buckets(), 4);
    assert_eq!(rsd.sort_columns().len(), 1);
    assert_eq!(rsd.sort_columns()[0].column(), "c1");
    assert_eq!(
        rsd.columns()[0].parameters().unwrap().get("p"),
        Some(&"v".to_string())
    );
    let skew = rsd.skewed_info().unwrap();
    assert_eq!(skew.skewed_column_names(), &["c1"]);
}

#[tokio::test]
async fn get_partition_indexes_not_found_after_table_delete() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("pi").build().unwrap())
        .send()
        .await
        .expect("create db");
    glue.create_table()
        .database_name("pi")
        .table_input(table_input("t"))
        .send()
        .await
        .expect("create table");

    // Index present while the table exists.
    glue.get_partition_indexes()
        .database_name("pi")
        .table_name("t")
        .send()
        .await
        .expect("get indexes on live table");

    glue.delete_table()
        .database_name("pi")
        .name("t")
        .send()
        .await
        .expect("delete table");

    // Real AWS raises EntityNotFoundException once the table is gone — the
    // Terraform partition-index destroy check depends on this.
    let err = glue
        .get_partition_indexes()
        .database_name("pi")
        .table_name("t")
        .send()
        .await
        .expect_err("table gone");
    assert!(err.into_service_error().is_entity_not_found_exception());
}

#[tokio::test]
async fn schema_resolves_by_arn() {
    // GetSchema must resolve a schema by its ARN, parsing both the registry and
    // schema name out of the `schema/<registry>/<schema>` resource path.
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    let registry_id = aws_sdk_glue::types::RegistryId::builder()
        .registry_name("my-registry")
        .build();
    glue.create_registry()
        .registry_name("my-registry")
        .send()
        .await
        .expect("create registry");

    let created = glue
        .create_schema()
        .registry_id(registry_id)
        .schema_name("my-schema")
        .data_format(aws_sdk_glue::types::DataFormat::Avro)
        .compatibility(aws_sdk_glue::types::Compatibility::Backward)
        .schema_definition(r#"{"type":"record","name":"r","fields":[]}"#)
        .send()
        .await
        .expect("create schema");
    let arn = created.schema_arn().expect("schema arn").to_string();
    assert!(arn.ends_with(":schema/my-registry/my-schema"), "arn={arn}");

    // Look it up purely by ARN (no registry/schema name in the SchemaId).
    let by_arn = glue
        .get_schema()
        .schema_id(
            aws_sdk_glue::types::SchemaId::builder()
                .schema_arn(&arn)
                .build(),
        )
        .send()
        .await
        .expect("get schema by arn");
    assert_eq!(by_arn.schema_name(), Some("my-schema"));
    assert_eq!(by_arn.registry_name(), Some("my-registry"));
}

#[tokio::test]
async fn ml_transform_id_prefix_and_computed_schema() {
    // An ML transform gets a `tfm-` id and reports the schema derived from its
    // input record table's columns.
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("ml_db").build().unwrap())
        .send()
        .await
        .expect("create db");
    glue.create_table()
        .database_name("ml_db")
        .table_input(table_input("ml_table"))
        .send()
        .await
        .expect("create table");

    let created = glue
        .create_ml_transform()
        .name("my-transform")
        .role("arn:aws:iam::123456789012:role/glue")
        .input_record_tables(
            aws_sdk_glue::types::GlueTable::builder()
                .database_name("ml_db")
                .table_name("ml_table")
                .build()
                .unwrap(),
        )
        .parameters(
            aws_sdk_glue::types::TransformParameters::builder()
                .transform_type(aws_sdk_glue::types::TransformType::FindMatches)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create ml transform");
    let id = created.transform_id().expect("transform id").to_string();
    assert!(id.starts_with("tfm-"), "id={id}");

    let got = glue
        .get_ml_transform()
        .transform_id(&id)
        .send()
        .await
        .expect("get ml transform");
    // Input table has two columns (id, amount), so the schema has two entries.
    let schema = got.schema();
    assert_eq!(schema.len(), 2);
    assert_eq!(schema[0].name(), Some("id"));
    assert_eq!(schema[1].name(), Some("amount"));
}

#[tokio::test]
async fn dev_endpoint_is_ready_with_default_nodes() {
    // A dev endpoint is READY at once (nothing to provision) and defaults to 5
    // nodes when no worker type is given.
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_dev_endpoint()
        .endpoint_name("my-endpoint")
        .role_arn("arn:aws:iam::123456789012:role/glue")
        .send()
        .await
        .expect("create dev endpoint");

    let got = glue
        .get_dev_endpoint()
        .endpoint_name("my-endpoint")
        .send()
        .await
        .expect("get dev endpoint");
    let ep = got.dev_endpoint().expect("dev endpoint");
    assert_eq!(ep.status(), Some("READY"));
    assert_eq!(ep.number_of_nodes(), 5);
}

#[tokio::test]
async fn delete_schema_versions_actually_deletes() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_registry()
        .registry_name("dsv-registry")
        .send()
        .await
        .expect("create registry");
    glue.create_schema()
        .registry_id(
            aws_sdk_glue::types::RegistryId::builder()
                .registry_name("dsv-registry")
                .build(),
        )
        .schema_name("dsv-schema")
        .data_format(aws_sdk_glue::types::DataFormat::Avro)
        .compatibility(aws_sdk_glue::types::Compatibility::None)
        .schema_definition(r#"{"type":"record","name":"r","fields":[]}"#)
        .send()
        .await
        .expect("create schema (v1)");

    let schema_id = aws_sdk_glue::types::SchemaId::builder()
        .registry_name("dsv-registry")
        .schema_name("dsv-schema")
        .build();

    // Register a second version.
    glue.register_schema_version()
        .schema_id(schema_id.clone())
        .schema_definition(r#"{"type":"record","name":"r","fields":[{"name":"x","type":"int"}]}"#)
        .send()
        .await
        .expect("register v2");

    // Delete version 2 — must actually remove it.
    glue.delete_schema_versions()
        .schema_id(schema_id.clone())
        .versions("2")
        .send()
        .await
        .expect("delete_schema_versions");

    let v2 = glue
        .get_schema_version()
        .schema_id(schema_id)
        .schema_version_number(
            aws_sdk_glue::types::SchemaVersionNumber::builder()
                .version_number(2)
                .build(),
        )
        .send()
        .await;
    assert!(
        v2.is_err(),
        "version 2 must be gone after delete_schema_versions"
    );
}

#[tokio::test]
async fn update_crawler_schedule_persists_expression() {
    use aws_sdk_glue::types::{CrawlerTargets, S3Target};
    let server = TestServer::start().await;
    let glue = server.glue_client().await;

    glue.create_crawler()
        .name("sched-crawler")
        .role("arn:aws:iam::123456789012:role/glue")
        .database_name("analytics")
        .targets(
            CrawlerTargets::builder()
                .s3_targets(S3Target::builder().path("s3://b/a").build())
                .build(),
        )
        .send()
        .await
        .expect("create crawler");

    // The Schedule cron expression was previously dropped; GetCrawler must
    // reflect it after UpdateCrawlerSchedule.
    glue.update_crawler_schedule()
        .crawler_name("sched-crawler")
        .schedule("cron(0 12 * * ? *)")
        .send()
        .await
        .expect("update schedule");

    let got = glue
        .get_crawler()
        .name("sched-crawler")
        .send()
        .await
        .expect("get crawler");
    let sched = got.crawler().and_then(|c| c.schedule()).expect("schedule");
    assert_eq!(sched.schedule_expression(), Some("cron(0 12 * * ? *)"));
}

#[tokio::test]
async fn get_tables_filters_by_expression() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;
    glue.create_database()
        .database_input(DatabaseInput::builder().name("db").build().unwrap())
        .send()
        .await
        .expect("create db");
    for t in ["orders", "order_items", "customers"] {
        glue.create_table()
            .database_name("db")
            .table_input(table_input(t))
            .send()
            .await
            .expect("create table");
    }

    // Expression is a regex on the table name.
    let listed = glue
        .get_tables()
        .database_name("db")
        .expression("^order")
        .send()
        .await
        .expect("get tables");
    let mut names: Vec<&str> = listed.table_list().iter().map(|t| t.name()).collect();
    names.sort();
    assert_eq!(names, vec!["order_items", "orders"]);

    // No expression returns all three.
    let all = glue.get_tables().database_name("db").send().await.unwrap();
    assert_eq!(all.table_list().len(), 3);
}

#[tokio::test]
async fn get_job_runs_orders_most_recent_first() {
    let server = TestServer::start().await;
    let glue = server.glue_client().await;
    glue.create_job()
        .name("ordered-job")
        .role("arn:aws:iam::123456789012:role/glue")
        .command(
            aws_sdk_glue::types::JobCommand::builder()
                .name("glueetl")
                .build(),
        )
        .send()
        .await
        .expect("create job");

    // Three runs, spaced so their StartedOn timestamps are distinct.
    let mut ids = Vec::new();
    for _ in 0..3 {
        let r = glue
            .start_job_run()
            .job_name("ordered-job")
            .send()
            .await
            .expect("start run");
        ids.push(r.job_run_id().unwrap().to_string());
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    let runs = glue
        .get_job_runs()
        .job_name("ordered-job")
        .send()
        .await
        .expect("get job runs");
    let starts: Vec<i64> = runs
        .job_runs()
        .iter()
        .filter_map(|r| r.started_on())
        .map(|t| t.as_secs_f64() as i64)
        .collect();
    assert_eq!(runs.job_runs().len(), 3);
    // The newest run (last started) must be JobRuns[0].
    assert_eq!(
        runs.job_runs()[0].id(),
        Some(ids.last().unwrap().as_str()),
        "GetJobRuns must return the most recent run first"
    );
    // Non-increasing StartedOn across the list.
    assert!(
        starts.windows(2).all(|w| w[0] >= w[1]),
        "job runs must be ordered by StartedOn descending, got {starts:?}"
    );
}
