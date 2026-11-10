//! End-to-end tests for the RDS PostgreSQL `aws_lambda` extension.
//!
//! Drives a full happy path: create a Lambda, create a Postgres DB
//! instance (which triggers the lazy build of `fakecloud-postgres`),
//! connect via tokio_postgres, run `CREATE EXTENSION aws_lambda CASCADE`,
//! and call `aws_lambda.invoke()` with both a name and an
//! `aws_commons.create_lambda_function_arn` composite. Async (`Event`)
//! invocation path is exercised too.

mod helpers;

use std::io::Write;

use aws_sdk_lambda::primitives::Blob;
use helpers::TestServer;
use tokio_postgres::NoTls;

fn make_echo_zip() -> Vec<u8> {
    // Returns the raw event back to the caller so we can verify the
    // payload round-trips through plpython3u + the bridge endpoint; raises
    // for `{"fail": true}`.
    let buf = Vec::new();
    let cursor = std::io::Cursor::new(buf);
    let mut writer = zip::ZipWriter::new(cursor);
    let options = zip::write::SimpleFileOptions::default();
    writer.start_file("index.py", options).unwrap();
    writer
        .write_all(
            b"def handler(event, context):\n    if isinstance(event, dict) and event.get('fail'):\n        raise ValueError('db said no')\n    return event\n",
        )
        .unwrap();
    let cursor = writer.finish().unwrap();
    cursor.into_inner()
}

async fn connect_with_retry(
    host: &str,
    port: i32,
    user: &str,
    password: &str,
    dbname: &str,
) -> tokio_postgres::Client {
    let connection_string =
        format!("host={host} port={port} user={user} password={password} dbname={dbname}");
    let mut last_error = None;
    for _ in 0..30 {
        match tokio_postgres::connect(&connection_string, NoTls).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                return client;
            }
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
    panic!(
        "could not connect to postgres at {}:{}: {:?}",
        host, port, last_error
    );
}

#[tokio::test]
async fn aws_lambda_extension_invoke_round_trip() {
    // Build the postgres image from this tree's extension assets rather than
    // pulling the image published for this version, so the test exercises
    // the shipped `aws_lambda` SQL (step 8 depends on it).
    let server = TestServer::start_with_env(&[("FAKECLOUD_REBUILD_POSTGRES_IMAGE", "1")]).await;
    let lambda = server.lambda_client().await;
    let rds = server.rds_client().await;

    // 1. Create the echo Lambda.
    lambda
        .create_function()
        .function_name("echo")
        .runtime(aws_sdk_lambda::types::Runtime::Python312)
        .role("arn:aws:iam::000000000000:role/test-role")
        .handler("index.handler")
        .code(
            aws_sdk_lambda::types::FunctionCode::builder()
                .zip_file(Blob::new(make_echo_zip()))
                .build(),
        )
        .send()
        .await
        .expect("create echo lambda");

    // 2. Create the Postgres DB instance — triggers lazy fakecloud-postgres
    //    image build on first run, so this can take a while.
    rds.create_db_instance()
        .db_instance_identifier("aws-lambda-ext-db")
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

    let instance = helpers::wait_for_db_available(&rds, "aws-lambda-ext-db", 240).await;
    let endpoint = instance.endpoint().expect("endpoint");
    let host = endpoint.address().expect("address").to_string();
    let port = endpoint.port().expect("port");

    // 3. Connect and load the extension.
    let client = connect_with_retry(&host, port, "admin", "secret123", "appdb").await;
    client
        .simple_query("CREATE EXTENSION IF NOT EXISTS aws_lambda CASCADE")
        .await
        .expect("load aws_lambda extension");

    // tokio-postgres in this workspace doesn't ship the with-serde_json-1
    // feature, so we can't bind a Rust `&str` to a postgres `json`
    // parameter. Embed payloads as SQL literals (test fixtures, no
    // injection concern) and cast result `payload` to text on the wire.

    // 4. Sync invoke by function name + json payload — payload round-trips.
    let row = client
        .query_one(
            "SELECT status_code, payload::text \
             FROM aws_lambda.invoke('echo', '{\"hello\":\"world\"}'::json)",
            &[],
        )
        .await
        .expect("invoke by name");
    let status_code: i32 = row.get(0);
    let payload_text: String = row.get(1);
    let payload: serde_json::Value = serde_json::from_str(&payload_text).unwrap();
    assert_eq!(status_code, 200);
    assert_eq!(payload, serde_json::json!({"hello": "world"}));

    // 5. aws_commons.create_lambda_function_arn returns a composite type.
    let arn_row = client
        .query_one(
            "SELECT (aws_commons.create_lambda_function_arn('echo')).function_name",
            &[],
        )
        .await
        .expect("create_lambda_function_arn");
    let function_name: String = arn_row.get(0);
    assert_eq!(function_name, "echo");

    // 6. Sync invoke via the composite-typed overload.
    let row = client
        .query_one(
            "SELECT status_code, payload::text FROM aws_lambda.invoke(\
                aws_commons.create_lambda_function_arn('echo'), '{\"k\":1}'::json)",
            &[],
        )
        .await
        .expect("invoke via composite arn");
    let status_code: i32 = row.get(0);
    let payload_text: String = row.get(1);
    let payload: serde_json::Value = serde_json::from_str(&payload_text).unwrap();
    assert_eq!(status_code, 200);
    assert_eq!(payload, serde_json::json!({"k": 1}));

    // 7. Async (Event) invocation returns 202 immediately.
    let row = client
        .query_one(
            "SELECT status_code, payload::text FROM aws_lambda.invoke(\
                'echo', '{\"async\":true}'::json, NULL, 'Event')",
            &[],
        )
        .await
        .expect("invoke async");
    let status_code: i32 = row.get(0);
    let payload_text: Option<String> = row.get(1);
    assert_eq!(status_code, 202);
    assert!(payload_text.is_none());

    // 8. A function error fails the call the way RDS does: "lambda
    //    invocation failed" with the function's error in the DETAIL.
    let err = client
        .query_one(
            "SELECT status_code FROM aws_lambda.invoke('echo', '{\"fail\":true}'::json)",
            &[],
        )
        .await
        .expect_err("a function error fails aws_lambda.invoke");
    let db_err = err.as_db_error().expect("a database error");
    assert_eq!(db_err.message(), "lambda invocation failed");
    let detail = db_err.detail().unwrap_or_default();
    // The function ARN names the DB instance's account.
    assert!(
        detail.starts_with("\"arn:aws:lambda:us-east-1:"),
        "{detail}"
    );
    assert!(
        detail.contains(":function:echo\" returned error \"Unhandled\", details: \"{"),
        "{detail}"
    );
    assert!(detail.contains("db said no"), "{detail}");

    // 9. A database still on extension 1.0 (as one restored from a volume
    //    created before 1.1 would be) is brought up to 1.1 when the instance
    //    comes back, so it gets the function-error behaviour too.
    client
        .simple_query("CREATE DATABASE olddb")
        .await
        .expect("create olddb");
    let old = connect_with_retry(&host, port, "admin", "secret123", "olddb").await;
    old.simple_query("CREATE EXTENSION aws_lambda VERSION '1.0' CASCADE")
        .await
        .expect("install aws_lambda 1.0");
    let version: String = old
        .query_one(
            "SELECT extversion FROM pg_extension WHERE extname = 'aws_lambda'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(version, "1.0");
    drop(old);
    drop(client);
    rds.reboot_db_instance()
        .db_instance_identifier("aws-lambda-ext-db")
        .send()
        .await
        .expect("reboot");
    let instance = helpers::wait_for_db_available(&rds, "aws-lambda-ext-db", 240).await;
    let endpoint = instance.endpoint().expect("endpoint");
    let old = connect_with_retry(
        endpoint.address().unwrap(),
        endpoint.port().unwrap(),
        "admin",
        "secret123",
        "olddb",
    )
    .await;
    let version: String = old
        .query_one(
            "SELECT extversion FROM pg_extension WHERE extname = 'aws_lambda'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(version, "1.1", "the extension is updated in place");
    let err = old
        .query_one(
            "SELECT status_code FROM aws_lambda.invoke('echo', '{\"fail\":true}'::json)",
            &[],
        )
        .await
        .expect_err("the updated extension raises the function error");
    assert_eq!(
        err.as_db_error().unwrap().message(),
        "lambda invocation failed"
    );
}
