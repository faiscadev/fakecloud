mod helpers;

use aws_sdk_athena::types::{
    DataCatalogType, QueryExecutionContext, ResultConfiguration, WorkGroupConfiguration,
};
use helpers::TestServer;

/// Workgroups, data catalogs, named queries, prepared statements, and query
/// executions all survive a restart in persistent mode.
#[tokio::test]
async fn persistence_round_trip_core_resources() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let athena = server.athena_client().await;

    athena
        .create_work_group()
        .name("analytics")
        .configuration(
            WorkGroupConfiguration::builder()
                .enforce_work_group_configuration(true)
                .build(),
        )
        .send()
        .await
        .unwrap();

    athena
        .create_data_catalog()
        .name("cat1")
        .r#type(DataCatalogType::Lambda)
        .description("lambda catalog")
        .send()
        .await
        .unwrap();

    let nq_id = athena
        .create_named_query()
        .name("greet")
        .database("default")
        .query_string("SELECT 'hello'")
        .work_group("primary")
        .send()
        .await
        .unwrap()
        .named_query_id
        .unwrap();

    athena
        .create_prepared_statement()
        .statement_name("ps1")
        .work_group("analytics")
        .query_statement("SELECT ?")
        .send()
        .await
        .unwrap();

    let qid = athena
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
        .unwrap()
        .query_execution_id
        .unwrap();

    server.restart().await;
    let athena = server.athena_client().await;

    // Workgroup survives.
    assert_eq!(
        athena
            .get_work_group()
            .work_group("analytics")
            .send()
            .await
            .unwrap()
            .work_group()
            .unwrap()
            .name(),
        "analytics"
    );

    // Data catalog survives.
    assert_eq!(
        athena
            .get_data_catalog()
            .name("cat1")
            .send()
            .await
            .unwrap()
            .data_catalog()
            .unwrap()
            .r#type(),
        &DataCatalogType::Lambda
    );

    // Named query survives.
    assert_eq!(
        athena
            .get_named_query()
            .named_query_id(&nq_id)
            .send()
            .await
            .unwrap()
            .named_query()
            .unwrap()
            .name(),
        "greet"
    );

    // Prepared statement survives.
    assert_eq!(
        athena
            .get_prepared_statement()
            .statement_name("ps1")
            .work_group("analytics")
            .send()
            .await
            .unwrap()
            .prepared_statement()
            .unwrap()
            .statement_name(),
        Some("ps1")
    );

    // Query execution (and its terminal status) survives.
    let qe = athena
        .get_query_execution()
        .query_execution_id(&qid)
        .send()
        .await
        .unwrap();
    assert_eq!(
        qe.query_execution()
            .and_then(|q| q.status())
            .and_then(|s| s.state())
            .map(|s| s.as_str()),
        Some("SUCCEEDED")
    );
}

/// A deleted workgroup stays gone after restart.
#[tokio::test]
async fn persistence_delete_workgroup_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let athena = server.athena_client().await;

    athena
        .create_work_group()
        .name("ephemeral")
        .send()
        .await
        .unwrap();
    athena
        .delete_work_group()
        .work_group("ephemeral")
        .send()
        .await
        .unwrap();

    server.restart().await;
    let athena = server.athena_client().await;

    let listed = athena.list_work_groups().send().await.unwrap();
    assert!(!listed
        .work_groups()
        .iter()
        .any(|w| w.name() == Some("ephemeral")));
}
