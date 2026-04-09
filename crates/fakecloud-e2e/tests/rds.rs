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

#[tokio::test]
async fn rds_tag_roundtrip() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let create = create_instance(&client, "orders-tags-db").await;
    let arn = create
        .db_instance()
        .and_then(|instance| instance.db_instance_arn())
        .expect("db instance arn");

    client
        .add_tags_to_resource()
        .resource_name(arn)
        .tags(
            aws_sdk_rds::types::Tag::builder()
                .key("env")
                .value("dev")
                .build(),
        )
        .tags(
            aws_sdk_rds::types::Tag::builder()
                .key("team")
                .value("core")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let listed = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.tag_list().len(), 2);

    client
        .remove_tags_from_resource()
        .resource_name(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let listed = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.tag_list().len(), 1);
    assert_eq!(listed.tag_list()[0].key(), Some("team"));
}

#[tokio::test]
async fn rds_delete_db_instance() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-delete-db").await;

    let response = client
        .delete_db_instance()
        .db_instance_identifier("orders-delete-db")
        .skip_final_snapshot(true)
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    assert_eq!(instance.db_instance_status(), Some("deleting"));

    let error = client
        .describe_db_instances()
        .db_instance_identifier("orders-delete-db")
        .send()
        .await
        .expect_err("instance should be gone");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("DBInstanceNotFound")
    );
}

#[tokio::test]
async fn rds_delete_db_instance_respects_deletion_protection() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance_with_deletion_protection(&client, "orders-protected-db", true).await;

    let error = client
        .delete_db_instance()
        .db_instance_identifier("orders-protected-db")
        .skip_final_snapshot(true)
        .send()
        .await
        .expect_err("deletion protection should block deletion");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("InvalidDBInstanceState")
    );

    let response = client
        .describe_db_instances()
        .db_instance_identifier("orders-protected-db")
        .send()
        .await
        .unwrap();
    assert_eq!(response.db_instances().len(), 1);
}

#[tokio::test]
async fn rds_modify_db_instance() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-modify-db").await;

    let response = client
        .modify_db_instance()
        .db_instance_identifier("orders-modify-db")
        .deletion_protection(true)
        .apply_immediately(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response
            .db_instance()
            .and_then(|instance| instance.deletion_protection()),
        Some(true)
    );

    let delete_error = client
        .delete_db_instance()
        .db_instance_identifier("orders-modify-db")
        .skip_final_snapshot(true)
        .send()
        .await
        .expect_err("deletion protection should block deletion");
    assert_eq!(
        delete_error.into_service_error().meta().code(),
        Some("InvalidDBInstanceState")
    );
}

#[tokio::test]
async fn rds_reboot_db_instance() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-reboot-db").await;

    let response = client
        .reboot_db_instance()
        .db_instance_identifier("orders-reboot-db")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response
            .db_instance()
            .and_then(|instance| instance.db_instance_status()),
        Some("rebooting")
    );

    let describe_after = client
        .describe_db_instances()
        .db_instance_identifier("orders-reboot-db")
        .send()
        .await
        .unwrap();
    let endpoint = describe_after.db_instances()[0]
        .endpoint()
        .expect("endpoint after reboot");
    let address = endpoint.address().expect("address after reboot");
    let port = endpoint.port().expect("port after reboot");

    let (db_client, connection) = connect_with_retry(address, port, "admin", "secret123", "appdb")
        .await
        .expect("reconnect after reboot");
    tokio::spawn(connection);
    let row = db_client
        .query_one("SELECT 1", &[])
        .await
        .expect("select 1");
    let value: i32 = row.get(0);
    assert_eq!(value, 1);
}

#[tokio::test]
async fn rds_reboot_db_instance_rejects_force_failover() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-force-failover-db").await;

    let error = client
        .reboot_db_instance()
        .db_instance_identifier("orders-force-failover-db")
        .force_failover(true)
        .send()
        .await
        .expect_err("force failover should be rejected");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("InvalidParameterCombination")
    );
}

async fn create_instance(
    client: &aws_sdk_rds::Client,
    db_instance_identifier: &str,
) -> aws_sdk_rds::operation::create_db_instance::CreateDbInstanceOutput {
    create_instance_with_deletion_protection(client, db_instance_identifier, false).await
}

async fn create_instance_with_deletion_protection(
    client: &aws_sdk_rds::Client,
    db_instance_identifier: &str,
    deletion_protection: bool,
) -> aws_sdk_rds::operation::create_db_instance::CreateDbInstanceOutput {
    client
        .create_db_instance()
        .db_instance_identifier(db_instance_identifier)
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .deletion_protection(deletion_protection)
        .db_name("appdb")
        .send()
        .await
        .unwrap()
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
