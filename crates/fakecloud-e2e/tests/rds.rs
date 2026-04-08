mod helpers;

use helpers::TestServer;
use tokio_postgres::NoTls;

#[tokio::test]
async fn rds_describe_db_engine_versions() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let response = client
        .describe_db_engine_versions()
        .engine("postgres")
        .send()
        .await
        .unwrap();

    let versions = response.db_engine_versions();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].engine(), Some("postgres"));
    assert_eq!(
        versions[0].db_engine_version_description(),
        Some("PostgreSQL 16.3")
    );
}

#[tokio::test]
async fn rds_describe_orderable_db_instance_options() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let response = client
        .describe_orderable_db_instance_options()
        .engine("postgres")
        .engine_version("16.3")
        .db_instance_class("db.t3.micro")
        .send()
        .await
        .unwrap();

    let options = response.orderable_db_instance_options();
    assert_eq!(options.len(), 1);
    assert_eq!(options[0].engine(), Some("postgres"));
    assert_eq!(options[0].storage_type(), Some("gp2"));
    assert_eq!(options[0].min_storage_size(), Some(20));
    assert_eq!(options[0].max_storage_size(), Some(16384));
}

#[tokio::test]
async fn rds_create_and_describe_db_instance() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let create_response = client
        .create_db_instance()
        .db_instance_identifier("orders-db")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .unwrap();

    let created = create_response.db_instance().expect("created instance");
    assert_eq!(created.db_instance_status(), Some("creating"));

    let describe_response = client
        .describe_db_instances()
        .db_instance_identifier("orders-db")
        .send()
        .await
        .unwrap();

    let instances = describe_response.db_instances();
    assert_eq!(instances.len(), 1);
    let instance = &instances[0];
    assert_eq!(instance.db_instance_status(), Some("available"));
    assert_eq!(instance.engine(), Some("postgres"));

    let endpoint = instance.endpoint().expect("endpoint");
    let host = endpoint.address().expect("address");
    let port = endpoint.port().expect("port");

    let (db_client, connection) = connect_with_retry(host, port, "admin", "secret123", "appdb")
        .await
        .expect("connect to postgres");
    tokio::spawn(connection);

    let row = db_client
        .query_one("SELECT 1", &[])
        .await
        .expect("select 1");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
}

async fn connect_with_retry(
    host: &str,
    port: i32,
    user: &str,
    password: &str,
    dbname: &str,
) -> Result<
    (
        tokio_postgres::Client,
        impl std::future::Future<Output = Result<(), tokio_postgres::Error>>,
    ),
    tokio_postgres::Error,
> {
    let connection_string =
        format!("host={host} port={port} user={user} password={password} dbname={dbname}");

    let mut last_error = None;
    for _ in 0..20 {
        match tokio_postgres::connect(&connection_string, NoTls).await {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                last_error = Some(error);
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }

    Err(last_error.expect("postgres connection error"))
}
