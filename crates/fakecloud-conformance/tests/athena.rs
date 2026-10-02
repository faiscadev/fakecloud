//! Athena conformance tests.

mod helpers;

use aws_sdk_athena::types::{DataCatalogType, QueryExecutionContext, ResultConfiguration, Tag};
use aws_sdk_glue::types::{DatabaseInput, TableInput};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

async fn make_workgroup(server: &TestServer, name: &str) {
    server
        .athena_client()
        .await
        .create_work_group()
        .name(name)
        .send()
        .await
        .unwrap();
}

async fn make_data_catalog(server: &TestServer, name: &str) {
    server
        .athena_client()
        .await
        .create_data_catalog()
        .name(name)
        .r#type(DataCatalogType::Lambda)
        .send()
        .await
        .unwrap();
}

async fn make_glue_database(server: &TestServer, name: &str) {
    server
        .glue_client()
        .await
        .create_database()
        .database_input(DatabaseInput::builder().name(name).build().unwrap())
        .send()
        .await
        .unwrap();
}

async fn make_glue_table(server: &TestServer, db_name: &str, table_name: &str) {
    server
        .glue_client()
        .await
        .create_table()
        .database_name(db_name)
        .table_input(TableInput::builder().name(table_name).build().unwrap())
        .send()
        .await
        .unwrap();
}

async fn make_named_query(server: &TestServer, name: &str) -> String {
    server
        .athena_client()
        .await
        .create_named_query()
        .name(name)
        .database("default")
        .query_string("SELECT 1")
        .work_group("primary")
        .send()
        .await
        .unwrap()
        .named_query_id()
        .unwrap()
        .to_owned()
}

async fn make_prepared_statement(server: &TestServer, name: &str) {
    server
        .athena_client()
        .await
        .create_prepared_statement()
        .statement_name(name)
        .work_group("primary")
        .query_statement("SELECT ?")
        .send()
        .await
        .unwrap();
}

/// Start a query that writes its result to an existing bucket and wait for
/// it to finish: StartQueryExecution only accepts the query, and a missing
/// output bucket fails it, as on Athena.
async fn make_query_execution(server: &TestServer) -> String {
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket("athena-conformance-results")
        .send()
        .await
        .unwrap();
    let athena = server.athena_client().await;
    let qid = athena
        .start_query_execution()
        .query_string("SELECT 1")
        .work_group("primary")
        .query_execution_context(QueryExecutionContext::builder().database("default").build())
        .result_configuration(
            ResultConfiguration::builder()
                .output_location("s3://athena-conformance-results/out/")
                .build(),
        )
        .send()
        .await
        .unwrap()
        .query_execution_id()
        .unwrap()
        .to_owned();
    for _ in 0..200 {
        let state = athena
            .get_query_execution()
            .query_execution_id(&qid)
            .send()
            .await
            .unwrap()
            .query_execution()
            .and_then(|q| q.status())
            .and_then(|s| s.state())
            .map(|s| s.as_str().to_string())
            .unwrap_or_default();
        if state != "QUEUED" && state != "RUNNING" {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    qid
}

async fn make_notebook(server: &TestServer, name: &str) -> String {
    server
        .athena_client()
        .await
        .create_notebook()
        .work_group("primary")
        .name(name)
        .send()
        .await
        .unwrap()
        .notebook_id()
        .unwrap()
        .to_owned()
}

async fn make_session(server: &TestServer) -> String {
    server
        .athena_client()
        .await
        .start_session()
        .work_group("primary")
        .engine_configuration(
            aws_sdk_athena::types::EngineConfiguration::builder()
                .max_concurrent_dpus(1)
                .build(),
        )
        .send()
        .await
        .unwrap()
        .session_id()
        .unwrap()
        .to_owned()
}

async fn make_calculation(server: &TestServer, session_id: &str) -> String {
    server
        .athena_client()
        .await
        .start_calculation_execution()
        .session_id(session_id)
        .code_block("print(1)")
        .send()
        .await
        .unwrap()
        .calculation_execution_id()
        .unwrap()
        .to_owned()
}

async fn make_capacity_reservation(server: &TestServer, name: &str) {
    server
        .athena_client()
        .await
        .create_capacity_reservation()
        .name(name)
        .target_dpus(4)
        .send()
        .await
        .unwrap();
}

// ─── Workgroups ────────────────────────────────────────────────────

#[test_action("athena", "CreateWorkGroup", checksum = "a1421529")]
#[tokio::test]
async fn athena_create_work_group() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .create_work_group()
        .name("conf-cwg")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetWorkGroup", checksum = "b4b7f16c")]
#[tokio::test]
async fn athena_get_work_group() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .get_work_group()
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListWorkGroups", checksum = "582adeb2")]
#[tokio::test]
async fn athena_list_work_groups() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_work_groups()
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "UpdateWorkGroup", checksum = "da75ec69")]
#[tokio::test]
async fn athena_update_work_group() {
    let server = TestServer::start().await;
    make_workgroup(&server, "conf-uwg").await;
    server
        .athena_client()
        .await
        .update_work_group()
        .work_group("conf-uwg")
        .description("updated")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "DeleteWorkGroup", checksum = "e9ab5986")]
#[tokio::test]
async fn athena_delete_work_group() {
    let server = TestServer::start().await;
    make_workgroup(&server, "conf-dwg").await;
    server
        .athena_client()
        .await
        .delete_work_group()
        .work_group("conf-dwg")
        .send()
        .await
        .unwrap();
}

// ─── Data catalogs ─────────────────────────────────────────────────

#[test_action("athena", "CreateDataCatalog", checksum = "2cfbe226")]
#[tokio::test]
async fn athena_create_data_catalog() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .create_data_catalog()
        .name("conf_create_dc")
        .r#type(DataCatalogType::Lambda)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetDataCatalog", checksum = "b6928b9e")]
#[tokio::test]
async fn athena_get_data_catalog() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .get_data_catalog()
        .name("AwsDataCatalog")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListDataCatalogs", checksum = "45278385")]
#[tokio::test]
async fn athena_list_data_catalogs() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_data_catalogs()
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "UpdateDataCatalog", checksum = "1d6b91f5")]
#[tokio::test]
async fn athena_update_data_catalog() {
    let server = TestServer::start().await;
    make_data_catalog(&server, "conf_upd_dc").await;
    server
        .athena_client()
        .await
        .update_data_catalog()
        .name("conf_upd_dc")
        .r#type(DataCatalogType::Lambda)
        .description("upd")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "DeleteDataCatalog", checksum = "ab6d1b3c")]
#[tokio::test]
async fn athena_delete_data_catalog() {
    let server = TestServer::start().await;
    make_data_catalog(&server, "conf_del_dc").await;
    server
        .athena_client()
        .await
        .delete_data_catalog()
        .name("conf_del_dc")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetDatabase", checksum = "a54b77ce")]
#[tokio::test]
async fn athena_get_database() {
    let server = TestServer::start().await;
    make_glue_database(&server, "default").await;
    server
        .athena_client()
        .await
        .get_database()
        .catalog_name("AwsDataCatalog")
        .database_name("default")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListDatabases", checksum = "792ebfff")]
#[tokio::test]
async fn athena_list_databases() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_databases()
        .catalog_name("AwsDataCatalog")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetTableMetadata", checksum = "49a0adc3")]
#[tokio::test]
async fn athena_get_table_metadata() {
    let server = TestServer::start().await;
    make_glue_database(&server, "default").await;
    make_glue_table(&server, "default", "t").await;
    server
        .athena_client()
        .await
        .get_table_metadata()
        .catalog_name("AwsDataCatalog")
        .database_name("default")
        .table_name("t")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListTableMetadata", checksum = "3e177af3")]
#[tokio::test]
async fn athena_list_table_metadata() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_table_metadata()
        .catalog_name("AwsDataCatalog")
        .database_name("default")
        .send()
        .await
        .unwrap();
}

// ─── Named queries ─────────────────────────────────────────────────

#[test_action("athena", "CreateNamedQuery", checksum = "8f9d34e9")]
#[tokio::test]
async fn athena_create_named_query() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .create_named_query()
        .name("conf-cnq")
        .database("default")
        .query_string("SELECT 1")
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetNamedQuery", checksum = "47e3616c")]
#[tokio::test]
async fn athena_get_named_query() {
    let server = TestServer::start().await;
    let id = make_named_query(&server, "conf-gnq").await;
    server
        .athena_client()
        .await
        .get_named_query()
        .named_query_id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListNamedQueries", checksum = "c09872f1")]
#[tokio::test]
async fn athena_list_named_queries() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_named_queries()
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "BatchGetNamedQuery", checksum = "17b69458")]
#[tokio::test]
async fn athena_batch_get_named_query() {
    let server = TestServer::start().await;
    let id = make_named_query(&server, "conf-bgnq").await;
    server
        .athena_client()
        .await
        .batch_get_named_query()
        .named_query_ids(id)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "UpdateNamedQuery", checksum = "1e1dd4bb")]
#[tokio::test]
async fn athena_update_named_query() {
    let server = TestServer::start().await;
    let id = make_named_query(&server, "conf-unq").await;
    server
        .athena_client()
        .await
        .update_named_query()
        .named_query_id(id)
        .name("renamed")
        .query_string("SELECT 2")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "DeleteNamedQuery", checksum = "e1712ce7")]
#[tokio::test]
async fn athena_delete_named_query() {
    let server = TestServer::start().await;
    let id = make_named_query(&server, "conf-dnq").await;
    server
        .athena_client()
        .await
        .delete_named_query()
        .named_query_id(id)
        .send()
        .await
        .unwrap();
}

// ─── Prepared statements ───────────────────────────────────────────

#[test_action("athena", "CreatePreparedStatement", checksum = "691bbc30")]
#[tokio::test]
async fn athena_create_prepared_statement() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .create_prepared_statement()
        .statement_name("conf-cps")
        .work_group("primary")
        .query_statement("SELECT ?")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetPreparedStatement", checksum = "6755681f")]
#[tokio::test]
async fn athena_get_prepared_statement() {
    let server = TestServer::start().await;
    make_prepared_statement(&server, "conf-gps").await;
    server
        .athena_client()
        .await
        .get_prepared_statement()
        .statement_name("conf-gps")
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListPreparedStatements", checksum = "586c4471")]
#[tokio::test]
async fn athena_list_prepared_statements() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_prepared_statements()
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "BatchGetPreparedStatement", checksum = "4a7980b9")]
#[tokio::test]
async fn athena_batch_get_prepared_statement() {
    let server = TestServer::start().await;
    make_prepared_statement(&server, "conf-bgps").await;
    server
        .athena_client()
        .await
        .batch_get_prepared_statement()
        .prepared_statement_names("conf-bgps")
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "UpdatePreparedStatement", checksum = "5eab0f3d")]
#[tokio::test]
async fn athena_update_prepared_statement() {
    let server = TestServer::start().await;
    make_prepared_statement(&server, "conf-ups").await;
    server
        .athena_client()
        .await
        .update_prepared_statement()
        .statement_name("conf-ups")
        .work_group("primary")
        .query_statement("SELECT ?, ?")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "DeletePreparedStatement", checksum = "8dfd9a4c")]
#[tokio::test]
async fn athena_delete_prepared_statement() {
    let server = TestServer::start().await;
    make_prepared_statement(&server, "conf-dps").await;
    server
        .athena_client()
        .await
        .delete_prepared_statement()
        .statement_name("conf-dps")
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

// ─── Query executions ──────────────────────────────────────────────

#[test_action("athena", "StartQueryExecution", checksum = "1834e23b")]
#[tokio::test]
async fn athena_start_query_execution() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .start_query_execution()
        .query_string("SELECT 1")
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "StopQueryExecution", checksum = "b29709f0")]
#[tokio::test]
async fn athena_stop_query_execution() {
    let server = TestServer::start().await;
    let qid = make_query_execution(&server).await;
    server
        .athena_client()
        .await
        .stop_query_execution()
        .query_execution_id(qid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetQueryExecution", checksum = "bc94fa2c")]
#[tokio::test]
async fn athena_get_query_execution() {
    let server = TestServer::start().await;
    let qid = make_query_execution(&server).await;
    server
        .athena_client()
        .await
        .get_query_execution()
        .query_execution_id(qid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListQueryExecutions", checksum = "1e91ebe8")]
#[tokio::test]
async fn athena_list_query_executions() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_query_executions()
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "BatchGetQueryExecution", checksum = "effdf281")]
#[tokio::test]
async fn athena_batch_get_query_execution() {
    let server = TestServer::start().await;
    let qid = make_query_execution(&server).await;
    server
        .athena_client()
        .await
        .batch_get_query_execution()
        .query_execution_ids(qid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetQueryResults", checksum = "09668f88")]
#[tokio::test]
async fn athena_get_query_results() {
    let server = TestServer::start().await;
    let qid = make_query_execution(&server).await;
    server
        .athena_client()
        .await
        .get_query_results()
        .query_execution_id(qid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetQueryRuntimeStatistics", checksum = "e0fca0fa")]
#[tokio::test]
async fn athena_get_query_runtime_statistics() {
    let server = TestServer::start().await;
    let qid = make_query_execution(&server).await;
    server
        .athena_client()
        .await
        .get_query_runtime_statistics()
        .query_execution_id(qid)
        .send()
        .await
        .unwrap();
}

// ─── Notebooks ─────────────────────────────────────────────────────

#[test_action("athena", "CreateNotebook", checksum = "f8f13ede")]
#[tokio::test]
async fn athena_create_notebook() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .create_notebook()
        .work_group("primary")
        .name("conf-nb")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ImportNotebook", checksum = "cf6db24b")]
#[tokio::test]
async fn athena_import_notebook() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .import_notebook()
        .work_group("primary")
        .name("conf-imp-nb")
        .r#type(aws_sdk_athena::types::NotebookType::Ipynb)
        .payload("{}")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ExportNotebook", checksum = "58bd2630")]
#[tokio::test]
async fn athena_export_notebook() {
    let server = TestServer::start().await;
    let id = make_notebook(&server, "conf-exp-nb").await;
    server
        .athena_client()
        .await
        .export_notebook()
        .notebook_id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetNotebookMetadata", checksum = "08398bb1")]
#[tokio::test]
async fn athena_get_notebook_metadata() {
    let server = TestServer::start().await;
    let id = make_notebook(&server, "conf-gnb").await;
    server
        .athena_client()
        .await
        .get_notebook_metadata()
        .notebook_id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListNotebookMetadata", checksum = "afe7fe47")]
#[tokio::test]
async fn athena_list_notebook_metadata() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_notebook_metadata()
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "UpdateNotebook", checksum = "829ab6cb")]
#[tokio::test]
async fn athena_update_notebook() {
    let server = TestServer::start().await;
    let id = make_notebook(&server, "conf-unb").await;
    server
        .athena_client()
        .await
        .update_notebook()
        .notebook_id(id)
        .payload("{\"v\":1}")
        .r#type(aws_sdk_athena::types::NotebookType::Ipynb)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "UpdateNotebookMetadata", checksum = "a61e65ab")]
#[tokio::test]
async fn athena_update_notebook_metadata() {
    let server = TestServer::start().await;
    let id = make_notebook(&server, "conf-unbm").await;
    server
        .athena_client()
        .await
        .update_notebook_metadata()
        .notebook_id(id)
        .name("renamed")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "DeleteNotebook", checksum = "6c3cfc38")]
#[tokio::test]
async fn athena_delete_notebook() {
    let server = TestServer::start().await;
    let id = make_notebook(&server, "conf-dnb").await;
    server
        .athena_client()
        .await
        .delete_notebook()
        .notebook_id(id)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "CreatePresignedNotebookUrl", checksum = "becb1102")]
#[tokio::test]
async fn athena_create_presigned_notebook_url() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    server
        .athena_client()
        .await
        .create_presigned_notebook_url()
        .session_id(sid)
        .send()
        .await
        .unwrap();
}

// ─── Sessions / calculations ───────────────────────────────────────

#[test_action("athena", "StartSession", checksum = "7782ed88")]
#[tokio::test]
async fn athena_start_session() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .start_session()
        .work_group("primary")
        .engine_configuration(
            aws_sdk_athena::types::EngineConfiguration::builder()
                .max_concurrent_dpus(1)
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetSession", checksum = "9b1d0a32")]
#[tokio::test]
async fn athena_get_session() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    server
        .athena_client()
        .await
        .get_session()
        .session_id(sid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetSessionStatus", checksum = "b2a8eb1a")]
#[tokio::test]
async fn athena_get_session_status() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    server
        .athena_client()
        .await
        .get_session_status()
        .session_id(sid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetSessionEndpoint", checksum = "a29049dc")]
#[tokio::test]
async fn athena_get_session_endpoint() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    server
        .athena_client()
        .await
        .get_session_endpoint()
        .session_id(sid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListSessions", checksum = "93f9c7b4")]
#[tokio::test]
async fn athena_list_sessions() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_sessions()
        .work_group("primary")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListNotebookSessions", checksum = "aa397dee")]
#[tokio::test]
async fn athena_list_notebook_sessions() {
    let server = TestServer::start().await;
    let nid = make_notebook(&server, "conf-lns-nb").await;
    server
        .athena_client()
        .await
        .list_notebook_sessions()
        .notebook_id(nid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "TerminateSession", checksum = "87d39028")]
#[tokio::test]
async fn athena_terminate_session() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    server
        .athena_client()
        .await
        .terminate_session()
        .session_id(sid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "StartCalculationExecution", checksum = "d36b49f1")]
#[tokio::test]
async fn athena_start_calculation_execution() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    server
        .athena_client()
        .await
        .start_calculation_execution()
        .session_id(sid)
        .code_block("print(1)")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "StopCalculationExecution", checksum = "b36eaaf5")]
#[tokio::test]
async fn athena_stop_calculation_execution() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    let cid = make_calculation(&server, &sid).await;
    server
        .athena_client()
        .await
        .stop_calculation_execution()
        .calculation_execution_id(cid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetCalculationExecution", checksum = "f3991669")]
#[tokio::test]
async fn athena_get_calculation_execution() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    let cid = make_calculation(&server, &sid).await;
    server
        .athena_client()
        .await
        .get_calculation_execution()
        .calculation_execution_id(cid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetCalculationExecutionCode", checksum = "19c95425")]
#[tokio::test]
async fn athena_get_calculation_execution_code() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    let cid = make_calculation(&server, &sid).await;
    server
        .athena_client()
        .await
        .get_calculation_execution_code()
        .calculation_execution_id(cid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetCalculationExecutionStatus", checksum = "7535110f")]
#[tokio::test]
async fn athena_get_calculation_execution_status() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    let cid = make_calculation(&server, &sid).await;
    server
        .athena_client()
        .await
        .get_calculation_execution_status()
        .calculation_execution_id(cid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListCalculationExecutions", checksum = "359fa57c")]
#[tokio::test]
async fn athena_list_calculation_executions() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    server
        .athena_client()
        .await
        .list_calculation_executions()
        .session_id(sid)
        .send()
        .await
        .unwrap();
}

// ─── Capacity reservations ─────────────────────────────────────────

#[test_action("athena", "CreateCapacityReservation", checksum = "5a229bad")]
#[tokio::test]
async fn athena_create_capacity_reservation() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .create_capacity_reservation()
        .name("conf-ccr")
        .target_dpus(4)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetCapacityReservation", checksum = "7df69b16")]
#[tokio::test]
async fn athena_get_capacity_reservation() {
    let server = TestServer::start().await;
    make_capacity_reservation(&server, "conf-gcr").await;
    server
        .athena_client()
        .await
        .get_capacity_reservation()
        .name("conf-gcr")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListCapacityReservations", checksum = "1e312f18")]
#[tokio::test]
async fn athena_list_capacity_reservations() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_capacity_reservations()
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "UpdateCapacityReservation", checksum = "5f1ff727")]
#[tokio::test]
async fn athena_update_capacity_reservation() {
    let server = TestServer::start().await;
    make_capacity_reservation(&server, "conf-ucr").await;
    server
        .athena_client()
        .await
        .update_capacity_reservation()
        .name("conf-ucr")
        .target_dpus(8)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "CancelCapacityReservation", checksum = "0f3c1e4a")]
#[tokio::test]
async fn athena_cancel_capacity_reservation() {
    let server = TestServer::start().await;
    make_capacity_reservation(&server, "conf-canc").await;
    server
        .athena_client()
        .await
        .cancel_capacity_reservation()
        .name("conf-canc")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "DeleteCapacityReservation", checksum = "b890b51b")]
#[tokio::test]
async fn athena_delete_capacity_reservation() {
    let server = TestServer::start().await;
    make_capacity_reservation(&server, "conf-dcr").await;
    server
        .athena_client()
        .await
        .delete_capacity_reservation()
        .name("conf-dcr")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "PutCapacityAssignmentConfiguration", checksum = "3ab3621c")]
#[tokio::test]
async fn athena_put_capacity_assignment_configuration() {
    let server = TestServer::start().await;
    make_capacity_reservation(&server, "conf-pcac").await;
    server
        .athena_client()
        .await
        .put_capacity_assignment_configuration()
        .capacity_reservation_name("conf-pcac")
        .capacity_assignments(
            aws_sdk_athena::types::CapacityAssignment::builder()
                .work_group_names("primary")
                .build(),
        )
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetCapacityAssignmentConfiguration", checksum = "eabd00d4")]
#[tokio::test]
async fn athena_get_capacity_assignment_configuration() {
    let server = TestServer::start().await;
    make_capacity_reservation(&server, "conf-gcac").await;
    server
        .athena_client()
        .await
        .put_capacity_assignment_configuration()
        .capacity_reservation_name("conf-gcac")
        .capacity_assignments(
            aws_sdk_athena::types::CapacityAssignment::builder()
                .work_group_names("primary")
                .build(),
        )
        .send()
        .await
        .unwrap();
    server
        .athena_client()
        .await
        .get_capacity_assignment_configuration()
        .capacity_reservation_name("conf-gcac")
        .send()
        .await
        .unwrap();
}

// ─── Tags ──────────────────────────────────────────────────────────

#[test_action("athena", "TagResource", checksum = "927aea3f")]
#[tokio::test]
async fn athena_tag_resource() {
    let server = TestServer::start().await;
    make_workgroup(&server, "conf-tag").await;
    let arn = "arn:aws:athena:us-east-1:123456789012:workgroup/conf-tag";
    server
        .athena_client()
        .await
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("k").value("v").build())
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "UntagResource", checksum = "d42f1207")]
#[tokio::test]
async fn athena_untag_resource() {
    let server = TestServer::start().await;
    make_workgroup(&server, "conf-untag").await;
    let arn = "arn:aws:athena:us-east-1:123456789012:workgroup/conf-untag";
    server
        .athena_client()
        .await
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("k").value("v").build())
        .send()
        .await
        .unwrap();
    server
        .athena_client()
        .await
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("k")
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListTagsForResource", checksum = "6d3f9a55")]
#[tokio::test]
async fn athena_list_tags_for_resource() {
    let server = TestServer::start().await;
    make_workgroup(&server, "conf-lt").await;
    let arn = "arn:aws:athena:us-east-1:123456789012:workgroup/conf-lt";
    server
        .athena_client()
        .await
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
}

// ─── Misc / read-only ──────────────────────────────────────────────

#[test_action("athena", "ListEngineVersions", checksum = "8096205b")]
#[tokio::test]
async fn athena_list_engine_versions() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_engine_versions()
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListApplicationDPUSizes", checksum = "a07049b7")]
#[tokio::test]
async fn athena_list_application_dpu_sizes() {
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .list_application_dpu_sizes()
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "ListExecutors", checksum = "de3399bb")]
#[tokio::test]
async fn athena_list_executors() {
    let server = TestServer::start().await;
    let sid = make_session(&server).await;
    server
        .athena_client()
        .await
        .list_executors()
        .session_id(sid)
        .send()
        .await
        .unwrap();
}

#[test_action("athena", "GetResourceDashboard", checksum = "ee4e80ec")]
#[tokio::test]
async fn athena_get_resource_dashboard() {
    // Smithy GetResourceDashboardRequest declares ResourceARN @required, so a
    // realistic call provides an ARN. The handler currently returns the
    // canonical console URL regardless of which resource was specified.
    let server = TestServer::start().await;
    server
        .athena_client()
        .await
        .get_resource_dashboard()
        .resource_arn("arn:aws:athena:us-east-1:000000000000:workgroup/primary")
        .send()
        .await
        .unwrap();
}
