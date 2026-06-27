//! RDS Data API (`rds-data`) runs REAL SQL against a fakecloud-managed RDS
//! Postgres container and returns AWS-shaped typed Field records — including a
//! binary (bytea) round-trip, which is exactly where rival emulators return
//! `{}` (floci #11479). Requires Docker (creates a real postgres instance);
//! runs in the Docker-enabled e2e partition like the other RDS tests.

mod helpers;

use aws_sdk_rdsdata::primitives::Blob;
use aws_sdk_rdsdata::types::{Field, SqlParameter};
use helpers::TestServer;

fn param(name: &str, value: Field) -> SqlParameter {
    SqlParameter::builder().name(name).value(value).build()
}

#[tokio::test]
async fn rds_data_api_executes_real_sql_with_typed_params() {
    let server = TestServer::start().await;
    let rds = server.rds_client().await;

    rds.create_db_instance()
        .db_instance_identifier("rdsdata-pg")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .expect("create postgres instance");

    let instance = helpers::wait_for_db_available(&rds, "rdsdata-pg", 240).await;
    let arn = instance
        .db_instance_arn()
        .expect("db instance arn")
        .to_string();

    let data = aws_sdk_rdsdata::Client::new(&server.aws_config().await);
    // fakecloud resolves credentials from the resourceArn, but AWS requires a
    // secretArn, so pass a placeholder.
    let secret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf";

    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("CREATE TABLE t (id int, name text, data bytea)")
        .send()
        .await
        .expect("create table");

    let blob = vec![0xDE, 0xAD, 0xBE, 0xEF];
    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("INSERT INTO t (id, name, data) VALUES (:id, :name, :data)")
        .parameters(param("id", Field::LongValue(7)))
        .parameters(param("name", Field::StringValue("alice".into())))
        .parameters(param("data", Field::BlobValue(Blob::new(blob.clone()))))
        .send()
        .await
        .expect("insert with typed params");

    let resp = data
        .execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("SELECT id, name, data FROM t WHERE id = :id")
        .parameters(param("id", Field::LongValue(7)))
        .include_result_metadata(true)
        .send()
        .await
        .expect("select typed");

    let records = resp.records();
    assert_eq!(records.len(), 1, "one row");
    let row = &records[0];
    assert_eq!(row[0].as_long_value().ok(), Some(&7), "int -> longValue");
    assert_eq!(
        row[1].as_string_value().map(String::as_str).ok(),
        Some("alice"),
        "text -> stringValue"
    );
    assert_eq!(
        row[2].as_blob_value().map(|b| b.as_ref().to_vec()).ok(),
        Some(blob),
        "bytea -> blobValue round-trips (floci #11479 returns {{}})"
    );
    assert!(
        !resp.column_metadata().is_empty(),
        "includeResultMetadata returns column metadata"
    );
}
