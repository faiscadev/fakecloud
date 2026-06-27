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

/// Transactions hold a real connection open across requests: a rolled-back
/// transaction leaves no rows, and a BatchExecuteStatement committed inside a
/// transaction persists every parameter set. This proves Begin/Commit/Rollback
/// run against the genuine engine, not an in-memory fake.
#[tokio::test]
async fn rds_data_api_transactions_and_batch() {
    let server = TestServer::start().await;
    let rds = server.rds_client().await;

    rds.create_db_instance()
        .db_instance_identifier("rdsdata-txn")
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

    let instance = helpers::wait_for_db_available(&rds, "rdsdata-txn", 240).await;
    let arn = instance
        .db_instance_arn()
        .expect("db instance arn")
        .to_string();

    let data = aws_sdk_rdsdata::Client::new(&server.aws_config().await);
    let secret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf";

    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("CREATE TABLE accounts (id int, name text)")
        .send()
        .await
        .expect("create table");

    // A transaction that is rolled back leaves no trace.
    let tx = data
        .begin_transaction()
        .resource_arn(&arn)
        .secret_arn(secret)
        .send()
        .await
        .expect("begin transaction");
    let txid = tx.transaction_id().expect("transaction id").to_string();
    assert!(!txid.is_empty(), "transaction id is non-empty");

    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .transaction_id(&txid)
        .sql("INSERT INTO accounts (id, name) VALUES (:id, :name)")
        .parameters(param("id", Field::LongValue(1)))
        .parameters(param("name", Field::StringValue("rollme".into())))
        .send()
        .await
        .expect("insert in transaction");

    data.rollback_transaction()
        .resource_arn(&arn)
        .secret_arn(secret)
        .transaction_id(&txid)
        .send()
        .await
        .expect("rollback");

    let after_rollback = data
        .execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("SELECT id FROM accounts")
        .send()
        .await
        .expect("select after rollback");
    assert_eq!(
        after_rollback.records().len(),
        0,
        "rolled-back insert left no rows"
    );

    // A BatchExecuteStatement committed inside a transaction persists every set.
    let tx2 = data
        .begin_transaction()
        .resource_arn(&arn)
        .secret_arn(secret)
        .send()
        .await
        .expect("begin transaction 2");
    let txid2 = tx2.transaction_id().expect("transaction id 2").to_string();

    let batch = data
        .batch_execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .transaction_id(&txid2)
        .sql("INSERT INTO accounts (id, name) VALUES (:id, :name)")
        .parameter_sets(vec![
            param("id", Field::LongValue(10)),
            param("name", Field::StringValue("alice".into())),
        ])
        .parameter_sets(vec![
            param("id", Field::LongValue(20)),
            param("name", Field::StringValue("bob".into())),
        ])
        .send()
        .await
        .expect("batch execute in transaction");
    assert_eq!(
        batch.update_results().len(),
        2,
        "one update result per parameter set"
    );

    data.commit_transaction()
        .resource_arn(&arn)
        .secret_arn(secret)
        .transaction_id(&txid2)
        .send()
        .await
        .expect("commit");

    let committed = data
        .execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("SELECT id, name FROM accounts ORDER BY id")
        .send()
        .await
        .expect("select after commit");
    let records = committed.records();
    assert_eq!(records.len(), 2, "both batch rows committed");
    assert_eq!(records[0][0].as_long_value().ok(), Some(&10));
    assert_eq!(
        records[1][1].as_string_value().map(String::as_str).ok(),
        Some("bob")
    );
}

/// Non-scalar Postgres column types (NUMERIC, TIMESTAMP, UUID, JSONB) come back
/// as AWS `stringValue` rather than silently collapsing to `isNull` (which is
/// what happens when these binary-format columns hit a `String` decode).
#[tokio::test]
async fn rds_data_api_decodes_rich_column_types() {
    let server = TestServer::start().await;
    let rds = server.rds_client().await;

    rds.create_db_instance()
        .db_instance_identifier("rdsdata-types")
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

    let instance = helpers::wait_for_db_available(&rds, "rdsdata-types", 240).await;
    let arn = instance
        .db_instance_arn()
        .expect("db instance arn")
        .to_string();

    let data = aws_sdk_rdsdata::Client::new(&server.aws_config().await);
    let secret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf";

    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("CREATE TABLE rich (n numeric(10,2), ts timestamp, u uuid, j jsonb)")
        .send()
        .await
        .expect("create table");

    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql(
            "INSERT INTO rich VALUES (123.45, '2024-01-02 03:04:05', \
             '550e8400-e29b-41d4-a716-446655440000', '{\"k\": \"v\"}')",
        )
        .send()
        .await
        .expect("insert rich types");

    let resp = data
        .execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("SELECT n, ts, u, j FROM rich")
        .send()
        .await
        .expect("select rich");

    let records = resp.records();
    assert_eq!(records.len(), 1);
    let row = &records[0];
    assert_eq!(
        row[0].as_string_value().map(String::as_str).ok(),
        Some("123.45"),
        "numeric -> stringValue"
    );
    assert_eq!(
        row[1].as_string_value().map(String::as_str).ok(),
        Some("2024-01-02 03:04:05"),
        "timestamp -> stringValue"
    );
    assert_eq!(
        row[2].as_string_value().map(String::as_str).ok(),
        Some("550e8400-e29b-41d4-a716-446655440000"),
        "uuid -> stringValue"
    );
    assert_eq!(
        row[3].as_string_value().map(String::as_str).ok(),
        Some("{\"k\":\"v\"}"),
        "jsonb -> stringValue (compact JSON)"
    );
}

/// Non-ASCII SQL (multi-byte UTF-8 literals and string parameters) must reach
/// the engine intact rather than being byte-cast into Latin-1 mojibake, and the
/// `database` request override must route the statement to the named database.
#[tokio::test]
async fn rds_data_api_utf8_literals_and_database_override() {
    let server = TestServer::start().await;
    let rds = server.rds_client().await;

    rds.create_db_instance()
        .db_instance_identifier("rdsdata-utf8")
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

    let instance = helpers::wait_for_db_available(&rds, "rdsdata-utf8", 240).await;
    let arn = instance
        .db_instance_arn()
        .expect("db instance arn")
        .to_string();

    let data = aws_sdk_rdsdata::Client::new(&server.aws_config().await);
    let secret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf";

    // A multi-byte literal embedded directly in the SQL text.
    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("CREATE TABLE u (id int, name text)")
        .send()
        .await
        .expect("create table");
    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("INSERT INTO u (id, name) VALUES (1, 'café 日本 €')")
        .send()
        .await
        .expect("insert literal");
    // A multi-byte string parameter.
    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("INSERT INTO u (id, name) VALUES (2, :name)")
        .parameters(param("name", Field::StringValue("naïve Ωmega".into())))
        .send()
        .await
        .expect("insert param");

    let resp = data
        .execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("SELECT name FROM u ORDER BY id")
        .send()
        .await
        .expect("select utf8");
    let records = resp.records();
    assert_eq!(
        records[0][0].as_string_value().map(String::as_str).ok(),
        Some("café 日本 €"),
        "multi-byte literal round-trips intact (not mojibake)"
    );
    assert_eq!(
        records[1][0].as_string_value().map(String::as_str).ok(),
        Some("naïve Ωmega"),
        "multi-byte string parameter round-trips intact"
    );

    // `database` override: create a second database and route statements to it.
    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("CREATE DATABASE otherdb")
        .send()
        .await
        .expect("create database");
    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .database("otherdb")
        .sql("CREATE TABLE only_here (id int)")
        .send()
        .await
        .expect("create table in otherdb");

    // The table exists in otherdb...
    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .database("otherdb")
        .sql("SELECT count(*) FROM only_here")
        .send()
        .await
        .expect("table visible in otherdb");
    // ...and NOT in the default database (proving the override actually routed).
    let in_default = data
        .execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("SELECT count(*) FROM only_here")
        .send()
        .await;
    assert!(
        in_default.is_err(),
        "table created via database=otherdb must not exist in the default db"
    );
}

/// MySQL path: a real INSERT returns the auto-increment key in `generatedFields`
/// (as AWS Aurora MySQL does), and typed values round-trip through the Data API.
#[tokio::test]
async fn rds_data_api_mysql_generated_fields() {
    let server = TestServer::start().await;
    let rds = server.rds_client().await;

    rds.create_db_instance()
        .db_instance_identifier("rdsdata-mysql")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("mysql")
        .engine_version("8.0")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .expect("create mysql instance");

    let instance = helpers::wait_for_db_available(&rds, "rdsdata-mysql", 300).await;
    let arn = instance
        .db_instance_arn()
        .expect("db instance arn")
        .to_string();

    let data = aws_sdk_rdsdata::Client::new(&server.aws_config().await);
    let secret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:db-AbCdEf";

    data.execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("CREATE TABLE m (id int AUTO_INCREMENT PRIMARY KEY, name text)")
        .send()
        .await
        .expect("create table");

    let insert = data
        .execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("INSERT INTO m (name) VALUES (:name)")
        .parameters(param("name", Field::StringValue("widget".into())))
        .send()
        .await
        .expect("insert");
    assert_eq!(insert.number_of_records_updated(), 1);

    let resp = data
        .execute_statement()
        .resource_arn(&arn)
        .secret_arn(secret)
        .sql("SELECT id, name FROM m")
        .send()
        .await
        .expect("select");
    let records = resp.records();
    assert_eq!(records.len(), 1);
    assert_eq!(
        records[0][0].as_long_value().ok(),
        Some(&1),
        "auto-increment produced id 1"
    );
    assert_eq!(
        records[0][1].as_string_value().map(String::as_str).ok(),
        Some("widget")
    );

    let generated = insert.generated_fields();
    assert_eq!(
        generated.first().and_then(|f| f.as_long_value().ok()),
        Some(&1),
        "MySQL INSERT returns the auto-increment id in generatedFields"
    );
}
