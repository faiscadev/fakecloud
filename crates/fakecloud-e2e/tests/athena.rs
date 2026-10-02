//! Athena service E2E.

mod helpers;

use aws_sdk_athena::types::{
    DataCatalogType, QueryExecutionContext, ResultConfiguration, Tag, WorkGroupConfiguration,
};
use aws_sdk_glue::types as glue_types;
use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

/// Poll GetQueryExecution until the query leaves QUEUED/RUNNING, the way
/// Athena clients wait for a query: StartQueryExecution only accepts it.
async fn wait_for_query(
    athena: &aws_sdk_athena::Client,
    qid: &str,
) -> aws_sdk_athena::types::QueryExecution {
    for _ in 0..200 {
        let got = athena
            .get_query_execution()
            .query_execution_id(qid)
            .send()
            .await
            .expect("get query execution");
        let qe = got.query_execution().expect("qe").clone();
        let state = qe
            .status()
            .and_then(|s| s.state())
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        if state != "QUEUED" && state != "RUNNING" {
            return qe;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("query {qid} did not finish");
}

#[tokio::test]
async fn primary_workgroup_is_seeded_on_first_call() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let listed = athena.list_work_groups().send().await.expect("list");
    let names: Vec<String> = listed
        .work_groups()
        .iter()
        .map(|w| w.name().unwrap_or_default().to_owned())
        .collect();
    assert!(
        names.iter().any(|n| n == "primary"),
        "expected primary workgroup to be seeded by default, got {names:?}"
    );
}

#[tokio::test]
async fn default_data_catalog_is_seeded() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let listed = athena
        .list_data_catalogs()
        .send()
        .await
        .expect("list catalogs");
    let names: Vec<String> = listed
        .data_catalogs_summary()
        .iter()
        .map(|c| c.catalog_name().unwrap_or_default().to_owned())
        .collect();
    assert!(
        names.iter().any(|n| n == "AwsDataCatalog"),
        "expected AwsDataCatalog seeded by default, got {names:?}"
    );
}

#[tokio::test]
async fn workgroup_create_get_update_delete_lifecycle() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let cfg = WorkGroupConfiguration::builder()
        .enforce_work_group_configuration(true)
        .publish_cloud_watch_metrics_enabled(false)
        .build();
    athena
        .create_work_group()
        .name("e2e-wg")
        .description("e2e workgroup")
        .configuration(cfg)
        .send()
        .await
        .expect("create");

    let got = athena
        .get_work_group()
        .work_group("e2e-wg")
        .send()
        .await
        .expect("get");
    let wg = got.work_group().expect("workgroup");
    assert_eq!(wg.name(), "e2e-wg");

    athena
        .update_work_group()
        .work_group("e2e-wg")
        .description("updated")
        .send()
        .await
        .expect("update");

    athena
        .delete_work_group()
        .work_group("e2e-wg")
        .send()
        .await
        .expect("delete");

    let err = athena
        .get_work_group()
        .work_group("e2e-wg")
        .send()
        .await
        .expect_err("expected not found after delete");
    let msg = format!("{err:?}");
    assert!(msg.contains("not found"), "unexpected err: {msg}");
}

#[tokio::test]
async fn primary_workgroup_cannot_be_deleted() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let err = athena
        .delete_work_group()
        .work_group("primary")
        .send()
        .await
        .expect_err("primary workgroup should not be deletable");
    let msg = format!("{err:?}");
    assert!(msg.contains("primary"), "unexpected err: {msg}");
}

#[tokio::test]
async fn workgroup_with_prepared_statements_blocks_non_recursive_delete() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    athena
        .create_work_group()
        .name("wg-with-ps")
        .send()
        .await
        .expect("create wg");
    athena
        .create_prepared_statement()
        .statement_name("ps1")
        .work_group("wg-with-ps")
        .query_statement("SELECT ?")
        .send()
        .await
        .expect("create prepared statement");
    let err = athena
        .delete_work_group()
        .work_group("wg-with-ps")
        .send()
        .await
        .expect_err("non-recursive delete should refuse non-empty workgroup");
    let msg = format!("{err:?}");
    assert!(msg.contains("still has resources"), "unexpected err: {msg}");
    athena
        .delete_work_group()
        .work_group("wg-with-ps")
        .recursive_delete_option(true)
        .send()
        .await
        .expect("recursive delete should succeed");
}

#[tokio::test]
async fn data_catalog_create_get_delete_lifecycle() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    athena
        .create_data_catalog()
        .name("custom_cat")
        .r#type(DataCatalogType::Lambda)
        .description("custom lambda catalog")
        .send()
        .await
        .expect("create");

    let got = athena
        .get_data_catalog()
        .name("custom_cat")
        .send()
        .await
        .expect("get");
    let cat = got.data_catalog().expect("catalog");
    assert_eq!(cat.name(), "custom_cat");
    assert_eq!(cat.r#type(), &DataCatalogType::Lambda);

    athena
        .delete_data_catalog()
        .name("custom_cat")
        .send()
        .await
        .expect("delete");
}

#[tokio::test]
async fn aws_data_catalog_cannot_be_deleted() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let err = athena
        .delete_data_catalog()
        .name("AwsDataCatalog")
        .send()
        .await
        .expect_err("AwsDataCatalog should not be deletable");
    let msg = format!("{err:?}");
    assert!(msg.contains("AwsDataCatalog"), "unexpected err: {msg}");
}

#[tokio::test]
async fn start_query_execution_returns_succeeded_result() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket("example-bucket")
        .send()
        .await
        .expect("create results bucket");
    let started = athena
        .start_query_execution()
        .query_string("SELECT 1")
        .work_group("primary")
        .query_execution_context(QueryExecutionContext::builder().database("default").build())
        .result_configuration(
            ResultConfiguration::builder()
                .output_location("s3://example-bucket/results/")
                .build(),
        )
        .send()
        .await
        .expect("start");
    let qid = started.query_execution_id().expect("qid").to_owned();

    let qe = wait_for_query(&athena, &qid).await;
    let state = qe.status().and_then(|s| s.state()).expect("status state");
    assert_eq!(state.as_str(), "SUCCEEDED");
    assert!(qe.status().and_then(|s| s.completion_date_time()).is_some());

    let results = athena
        .get_query_results()
        .query_execution_id(&qid)
        .send()
        .await
        .expect("results");
    let rs = results.result_set().expect("result set");
    assert!(!rs.rows().is_empty(), "expected at least one row");
}

#[tokio::test]
async fn named_query_create_get_list_delete() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let id = athena
        .create_named_query()
        .name("greet")
        .description("hello world query")
        .database("default")
        .query_string("SELECT 'hello'")
        .work_group("primary")
        .send()
        .await
        .expect("create")
        .named_query_id()
        .expect("id")
        .to_owned();

    let got = athena
        .get_named_query()
        .named_query_id(&id)
        .send()
        .await
        .expect("get");
    let nq = got.named_query().expect("nq");
    assert_eq!(nq.name(), "greet");

    let listed = athena
        .list_named_queries()
        .work_group("primary")
        .send()
        .await
        .expect("list");
    assert!(listed.named_query_ids().iter().any(|i| i == &id));

    athena
        .delete_named_query()
        .named_query_id(&id)
        .send()
        .await
        .expect("delete");
}

#[tokio::test]
async fn prepared_statement_create_get_list_delete() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    athena
        .create_prepared_statement()
        .statement_name("p1")
        .work_group("primary")
        .query_statement("SELECT ?")
        .send()
        .await
        .expect("create");

    let got = athena
        .get_prepared_statement()
        .statement_name("p1")
        .work_group("primary")
        .send()
        .await
        .expect("get");
    let ps = got.prepared_statement().expect("ps");
    assert_eq!(ps.statement_name(), Some("p1"));

    let listed = athena
        .list_prepared_statements()
        .work_group("primary")
        .send()
        .await
        .expect("list");
    assert!(listed
        .prepared_statements()
        .iter()
        .any(|p| p.statement_name() == Some("p1")));

    athena
        .delete_prepared_statement()
        .statement_name("p1")
        .work_group("primary")
        .send()
        .await
        .expect("delete");
}

#[tokio::test]
async fn tag_lifecycle_on_workgroup() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    athena
        .create_work_group()
        .name("tagged")
        .send()
        .await
        .expect("create");
    let arn = "arn:aws:athena:us-east-1:123456789012:workgroup/tagged";
    athena
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("test").build())
        .send()
        .await
        .expect("tag");
    let listed = athena
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .expect("list tags");
    let tags = listed.tags();
    assert!(tags
        .iter()
        .any(|t| t.key() == Some("env") && t.value() == Some("test")));

    athena
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("env")
        .send()
        .await
        .expect("untag");
    let listed = athena
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .expect("list tags");
    assert!(listed.tags().is_empty());
}

#[tokio::test]
async fn list_engine_versions_returns_known_versions() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let listed = athena
        .list_engine_versions()
        .send()
        .await
        .expect("list versions");
    let names: Vec<String> = listed
        .engine_versions()
        .iter()
        .filter_map(|v| v.effective_engine_version().map(str::to_owned))
        .collect();
    assert!(
        names.iter().any(|n| n.contains("Athena engine")),
        "expected at least one Athena engine version, got {names:?}"
    );
}

/// End-to-end: pre-seed a Glue table that points at CSV objects in S3,
/// `StartQueryExecution` with a `SELECT col FROM db.t WHERE col='x'`, and
/// confirm `GetQueryResults` returns the matched rows from the underlying
/// CSV — exercising the SQL parser + Glue catalog read + S3 read + result
/// CSV write path.
#[tokio::test]
async fn select_with_filter_reads_csv_via_glue_catalog() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let glue = server.glue_client().await;
    let s3 = server.s3_client().await;

    // 1. Seed S3 with a CSV file under the table's storage prefix.
    s3.create_bucket()
        .bucket("dl-bucket")
        .send()
        .await
        .expect("create bucket");
    let csv = "1,alice,active\n2,bob,inactive\n3,carol,active\n";
    s3.put_object()
        .bucket("dl-bucket")
        .key("users/data.csv")
        .body(ByteStream::from_static(csv.as_bytes()))
        .send()
        .await
        .expect("put csv");
    // A gzip-compressed part is decoded by extension, and Hadoop marker
    // files (`_SUCCESS`) are skipped, as on Athena.
    let gz = {
        use std::io::Write;
        let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(b"4,dave,active\n").unwrap();
        enc.finish().unwrap()
    };
    s3.put_object()
        .bucket("dl-bucket")
        .key("users/part-2.csv.gz")
        .body(ByteStream::from(gz))
        .send()
        .await
        .expect("put gz csv");
    s3.put_object()
        .bucket("dl-bucket")
        .key("users/_SUCCESS")
        .body(ByteStream::from_static(b"9,marker,active\n"))
        .send()
        .await
        .expect("put marker");

    // 2. Register a Glue database + CSV-backed table over that prefix.
    glue.create_database()
        .database_input(
            glue_types::DatabaseInput::builder()
                .name("warehouse")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create db");
    let table_input = glue_types::TableInput::builder()
        .name("users")
        .table_type("EXTERNAL_TABLE")
        .storage_descriptor(
            glue_types::StorageDescriptor::builder()
                .columns(
                    glue_types::Column::builder()
                        .name("id")
                        .r#type("bigint")
                        .build()
                        .unwrap(),
                )
                .columns(
                    glue_types::Column::builder()
                        .name("name")
                        .r#type("string")
                        .build()
                        .unwrap(),
                )
                .columns(
                    glue_types::Column::builder()
                        .name("status")
                        .r#type("string")
                        .build()
                        .unwrap(),
                )
                .location("s3://dl-bucket/users/")
                .input_format("org.apache.hadoop.mapred.TextInputFormat")
                .output_format("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat")
                .serde_info(
                    glue_types::SerDeInfo::builder()
                        .serialization_library("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
                        .build(),
                )
                .build(),
        )
        .build()
        .expect("table input");
    glue.create_table()
        .database_name("warehouse")
        .table_input(table_input)
        .send()
        .await
        .expect("create table");

    // 3. Result location bucket so Athena can write the CSV result back.
    s3.create_bucket()
        .bucket("athena-results")
        .send()
        .await
        .expect("create results bucket");

    // 4. Run the query.
    let started = athena
        .start_query_execution()
        .query_string("SELECT id, name FROM warehouse.users WHERE status = 'active' LIMIT 10")
        .work_group("primary")
        .query_execution_context(
            QueryExecutionContext::builder()
                .database("warehouse")
                .build(),
        )
        .result_configuration(
            ResultConfiguration::builder()
                .output_location("s3://athena-results/queries/")
                .build(),
        )
        .send()
        .await
        .expect("start");
    let qid = started.query_execution_id().expect("qid").to_owned();

    // 5. Verify the execution completed successfully.
    let qe = &wait_for_query(&athena, &qid).await;
    let state = qe
        .status()
        .and_then(|s| s.state())
        .expect("status state")
        .as_str()
        .to_string();
    assert_eq!(state, "SUCCEEDED", "qe failed: {qe:?}");
    let resolved_output = qe
        .result_configuration()
        .and_then(|rc| rc.output_location())
        .unwrap_or_default()
        .to_string();
    assert_eq!(
        resolved_output,
        format!("s3://athena-results/queries/{qid}.csv"),
        "Athena names the result file after the query execution id"
    );

    // 6. GetQueryResults: header row + matched rows, projected to (id, name).
    let results = athena
        .get_query_results()
        .query_execution_id(&qid)
        .send()
        .await
        .expect("results");
    let rs = results.result_set().expect("rs");
    let rows: Vec<Vec<String>> = rs
        .rows()
        .iter()
        .map(|row| {
            row.data()
                .iter()
                .map(|d| d.var_char_value().unwrap_or("").to_string())
                .collect()
        })
        .collect();
    assert_eq!(rows.len(), 4, "header + 3 matches; got {rows:?}");
    assert_eq!(rows[0], vec!["id".to_string(), "name".to_string()]);
    assert_eq!(rows[1], vec!["1".to_string(), "alice".to_string()]);
    assert_eq!(rows[2], vec!["3".to_string(), "carol".to_string()]);
    assert_eq!(rows[3], vec!["4".to_string(), "dave".to_string()]);

    // 7. The result CSV was written back to S3 — fetch and verify.
    let result_key = resolved_output
        .strip_prefix("s3://athena-results/")
        .expect("strip");
    let got = s3
        .get_object()
        .bucket("athena-results")
        .key(result_key)
        .send()
        .await
        .expect("get result csv");
    let body = got.body.collect().await.expect("body").into_bytes();
    let body_str = String::from_utf8(body.to_vec()).expect("utf8");
    assert!(
        body_str.contains("id,name\n1,alice\n3,carol\n4,dave"),
        "result CSV missing expected payload: {body_str:?}"
    );
}

/// SELECT against a non-existent table: Athena marks the execution `FAILED`
/// with a structured reason rather than synthesizing a fake row.
#[tokio::test]
async fn select_failing_against_unknown_table_marks_query_failed() {
    let server = TestServer::start().await;
    let athena = server.athena_client().await;
    let started = athena
        .start_query_execution()
        .query_string("SELECT * FROM nodb.notable")
        .work_group("primary")
        .send()
        .await
        .expect("start");
    let qid = started.query_execution_id().expect("qid").to_owned();
    let qe = &wait_for_query(&athena, &qid).await;
    let state = qe
        .status()
        .and_then(|s| s.state())
        .expect("state")
        .as_str()
        .to_string();
    assert_eq!(state, "FAILED");
    let reason = qe
        .status()
        .and_then(|s| s.state_change_reason())
        .unwrap_or_default()
        .to_string();
    assert!(
        reason.to_lowercase().contains("not found"),
        "expected `not found` in reason, got `{reason}`"
    );
}
