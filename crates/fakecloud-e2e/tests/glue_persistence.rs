mod helpers;

use aws_sdk_glue::types::{Column, DatabaseInput, JobCommand, StorageDescriptor, TableInput};
use helpers::TestServer;

fn table_input(name: &str) -> TableInput {
    TableInput::builder()
        .name(name)
        .table_type("EXTERNAL_TABLE")
        .storage_descriptor(
            StorageDescriptor::builder()
                .columns(
                    Column::builder()
                        .name("id")
                        .r#type("string")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .build()
        .unwrap()
}

/// Databases, tables, and jobs survive a restart in persistent mode.
#[tokio::test]
async fn persistence_round_trip_database_table_job() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("warehouse").build().unwrap())
        .send()
        .await
        .unwrap();
    glue.create_table()
        .database_name("warehouse")
        .table_input(table_input("orders"))
        .send()
        .await
        .unwrap();
    glue.create_job()
        .name("etl-job")
        .role("arn:aws:iam::123456789012:role/glue")
        .command(
            JobCommand::builder()
                .name("glueetl")
                .script_location("s3://example/script.py")
                .build(),
        )
        .send()
        .await
        .unwrap();

    server.restart().await;
    let glue = server.glue_client().await;

    assert_eq!(
        glue.get_database()
            .name("warehouse")
            .send()
            .await
            .unwrap()
            .database()
            .unwrap()
            .name(),
        "warehouse"
    );
    assert_eq!(
        glue.get_table()
            .database_name("warehouse")
            .name("orders")
            .send()
            .await
            .unwrap()
            .table()
            .unwrap()
            .name(),
        "orders"
    );
    assert_eq!(
        glue.get_job()
            .job_name("etl-job")
            .send()
            .await
            .unwrap()
            .job()
            .unwrap()
            .name(),
        Some("etl-job")
    );
}

/// A deleted database stays gone after restart.
#[tokio::test]
async fn persistence_delete_database_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let glue = server.glue_client().await;

    glue.create_database()
        .database_input(DatabaseInput::builder().name("ephemeral").build().unwrap())
        .send()
        .await
        .unwrap();
    glue.delete_database()
        .name("ephemeral")
        .send()
        .await
        .unwrap();

    server.restart().await;
    let glue = server.glue_client().await;

    assert!(glue.get_database().name("ephemeral").send().await.is_err());
}
