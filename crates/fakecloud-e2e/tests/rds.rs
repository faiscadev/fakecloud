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
    assert_eq!(versions.len(), 4); // All postgres versions
    assert!(versions.iter().all(|v| v.engine() == Some("postgres")));
    assert!(versions.iter().any(|v| v.engine_version() == Some("16.3")));
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

    // Test with skip_final_snapshot=true
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

    // Test with final snapshot - should fail BEFORE creating snapshot
    let error = client
        .delete_db_instance()
        .db_instance_identifier("orders-protected-db")
        .final_db_snapshot_identifier("protected-snapshot")
        .send()
        .await
        .expect_err("deletion protection should block deletion before snapshot creation");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("InvalidDBInstanceState")
    );

    // Verify instance still exists
    let response = client
        .describe_db_instances()
        .db_instance_identifier("orders-protected-db")
        .send()
        .await
        .unwrap();
    assert_eq!(response.db_instances().len(), 1);

    // Verify NO snapshot was created (critical: proves deletion protection checked BEFORE snapshot)
    let snapshots_response = client.describe_db_snapshots().send().await.unwrap();
    let protected_snapshot = snapshots_response
        .db_snapshots()
        .iter()
        .find(|s| s.db_snapshot_identifier() == Some("protected-snapshot"));
    assert!(
        protected_snapshot.is_none(),
        "Snapshot should NOT be created when deletion protection blocks deletion"
    );
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

#[tokio::test]
async fn rds_create_describe_delete_snapshot() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-snapshot-test-db").await;

    let create_response = client
        .create_db_snapshot()
        .db_instance_identifier("orders-snapshot-test-db")
        .db_snapshot_identifier("test-snapshot")
        .send()
        .await
        .unwrap();

    let snapshot = create_response.db_snapshot().unwrap();
    assert_eq!(snapshot.db_snapshot_identifier(), Some("test-snapshot"));
    assert_eq!(
        snapshot.db_instance_identifier(),
        Some("orders-snapshot-test-db")
    );
    assert_eq!(snapshot.engine(), Some("postgres"));
    assert_eq!(snapshot.status(), Some("available"));
    assert_eq!(snapshot.master_username(), Some("admin"));

    let describe_response = client
        .describe_db_snapshots()
        .db_snapshot_identifier("test-snapshot")
        .send()
        .await
        .unwrap();
    assert_eq!(describe_response.db_snapshots().len(), 1);

    let describe_by_instance = client
        .describe_db_snapshots()
        .db_instance_identifier("orders-snapshot-test-db")
        .send()
        .await
        .unwrap();
    assert_eq!(describe_by_instance.db_snapshots().len(), 1);

    client
        .delete_db_snapshot()
        .db_snapshot_identifier("test-snapshot")
        .send()
        .await
        .unwrap();

    let error = client
        .describe_db_snapshots()
        .db_snapshot_identifier("test-snapshot")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("DBSnapshotNotFound")
    );
}

#[tokio::test]
async fn rds_restore_from_snapshot() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-source-db").await;

    let create_instance_response = client
        .describe_db_instances()
        .db_instance_identifier("orders-source-db")
        .send()
        .await
        .unwrap();
    let source_instance = &create_instance_response.db_instances()[0];
    let source_endpoint = source_instance.endpoint().unwrap();

    let (source_client, source_connection) = connect_with_retry(
        source_endpoint.address().unwrap(),
        source_endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        if let Err(e) = source_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    source_client
        .execute("CREATE TABLE test_table (id INT, name TEXT)", &[])
        .await
        .unwrap();
    source_client
        .execute(
            "INSERT INTO test_table (id, name) VALUES (1, 'snapshot test data')",
            &[],
        )
        .await
        .unwrap();

    client
        .create_db_snapshot()
        .db_instance_identifier("orders-source-db")
        .db_snapshot_identifier("restore-test-snapshot")
        .send()
        .await
        .unwrap();

    let restore_response = client
        .restore_db_instance_from_db_snapshot()
        .db_instance_identifier("orders-restored-db")
        .db_snapshot_identifier("restore-test-snapshot")
        .send()
        .await
        .unwrap();

    let restored_instance = restore_response.db_instance().unwrap();
    assert_eq!(
        restored_instance.db_instance_identifier(),
        Some("orders-restored-db")
    );
    assert_eq!(restored_instance.engine(), Some("postgres"));
    assert_eq!(restored_instance.master_username(), Some("admin"));
    assert_eq!(restored_instance.db_name(), Some("appdb"));

    let describe_response = client
        .describe_db_instances()
        .db_instance_identifier("orders-restored-db")
        .send()
        .await
        .unwrap();
    let instances = describe_response.db_instances();
    assert_eq!(instances.len(), 1);
    let restored_endpoint = instances[0].endpoint().unwrap();

    let (restored_client, restored_connection) = connect_with_retry(
        restored_endpoint.address().unwrap(),
        restored_endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        if let Err(e) = restored_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let row = restored_client
        .query_one("SELECT name FROM test_table WHERE id = 1", &[])
        .await
        .unwrap();
    let name: String = row.get(0);
    assert_eq!(name, "snapshot test data");
}

#[tokio::test]
async fn rds_create_and_query_read_replica() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-source-db").await;

    let source_describe = client
        .describe_db_instances()
        .db_instance_identifier("orders-source-db")
        .send()
        .await
        .unwrap();
    let source_instance = &source_describe.db_instances()[0];
    let source_endpoint = source_instance.endpoint().unwrap();

    let (source_client, source_connection) = connect_with_retry(
        source_endpoint.address().unwrap(),
        source_endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        if let Err(e) = source_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    source_client
        .execute("CREATE TABLE test_table (id INT, name TEXT)", &[])
        .await
        .unwrap();
    source_client
        .execute(
            "INSERT INTO test_table (id, name) VALUES (1, 'primary data')",
            &[],
        )
        .await
        .unwrap();

    let replica_response = client
        .create_db_instance_read_replica()
        .db_instance_identifier("orders-replica-db")
        .source_db_instance_identifier("orders-source-db")
        .send()
        .await
        .unwrap();

    let replica_instance = replica_response.db_instance().unwrap();
    assert_eq!(
        replica_instance.db_instance_identifier(),
        Some("orders-replica-db")
    );
    assert_eq!(
        replica_instance.read_replica_source_db_instance_identifier(),
        Some("orders-source-db")
    );

    let source_describe_after = client
        .describe_db_instances()
        .db_instance_identifier("orders-source-db")
        .send()
        .await
        .unwrap();
    let source_after = &source_describe_after.db_instances()[0];
    assert_eq!(source_after.read_replica_db_instance_identifiers().len(), 1);
    assert_eq!(
        source_after.read_replica_db_instance_identifiers()[0],
        "orders-replica-db"
    );

    let replica_describe = client
        .describe_db_instances()
        .db_instance_identifier("orders-replica-db")
        .send()
        .await
        .unwrap();
    let replica_endpoint = replica_describe.db_instances()[0].endpoint().unwrap();

    let (replica_client, replica_connection) = connect_with_retry(
        replica_endpoint.address().unwrap(),
        replica_endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        if let Err(e) = replica_connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let row = replica_client
        .query_one("SELECT name FROM test_table WHERE id = 1", &[])
        .await
        .unwrap();
    let name: String = row.get(0);
    assert_eq!(name, "primary data");
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

#[tokio::test]
async fn vpc_security_groups() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Create instance with VPC security groups
    let response = client
        .create_db_instance()
        .db_instance_identifier("e2e-rds-sg")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("sgtest")
        .vpc_security_group_ids("sg-initial1")
        .vpc_security_group_ids("sg-initial2")
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    let sg_memberships = instance.vpc_security_groups();
    assert_eq!(sg_memberships.len(), 2);
    assert_eq!(
        sg_memberships[0].vpc_security_group_id(),
        Some("sg-initial1")
    );
    assert_eq!(
        sg_memberships[1].vpc_security_group_id(),
        Some("sg-initial2")
    );

    // Modify security groups
    let response = client
        .modify_db_instance()
        .db_instance_identifier("e2e-rds-sg")
        .vpc_security_group_ids("sg-updated1")
        .vpc_security_group_ids("sg-updated2")
        .vpc_security_group_ids("sg-updated3")
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    let sg_memberships = instance.vpc_security_groups();
    assert_eq!(sg_memberships.len(), 3);
    assert_eq!(
        sg_memberships[0].vpc_security_group_id(),
        Some("sg-updated1")
    );
    assert_eq!(
        sg_memberships[1].vpc_security_group_id(),
        Some("sg-updated2")
    );
    assert_eq!(
        sg_memberships[2].vpc_security_group_id(),
        Some("sg-updated3")
    );

    // Verify persistence in describe
    let response = client
        .describe_db_instances()
        .db_instance_identifier("e2e-rds-sg")
        .send()
        .await
        .unwrap();

    let instances = response.db_instances();
    assert_eq!(instances.len(), 1);
    let sg_memberships = instances[0].vpc_security_groups();
    assert_eq!(sg_memberships.len(), 3);
    assert_eq!(
        sg_memberships[0].vpc_security_group_id(),
        Some("sg-updated1")
    );
    assert_eq!(
        sg_memberships[1].vpc_security_group_id(),
        Some("sg-updated2")
    );
    assert_eq!(
        sg_memberships[2].vpc_security_group_id(),
        Some("sg-updated3")
    );
}

#[tokio::test]
async fn final_snapshot_on_delete() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Create instance
    let response = client
        .create_db_instance()
        .db_instance_identifier("e2e-rds-final")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("testdb")
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    let port = instance.endpoint().unwrap().port().unwrap();

    // Wait for instance and insert test data
    let (postgres, connection) =
        connect_with_retry("127.0.0.1", port, "admin", "secret123", "testdb")
            .await
            .expect("connect to db");

    tokio::spawn(connection);

    postgres
        .execute("CREATE TABLE test_final (id INT, value TEXT)", &[])
        .await
        .expect("create table");
    postgres
        .execute("INSERT INTO test_final VALUES (1, 'preserved')", &[])
        .await
        .expect("insert data");

    // Delete with final snapshot
    client
        .delete_db_instance()
        .db_instance_identifier("e2e-rds-final")
        .final_db_snapshot_identifier("e2e-final-snap")
        .send()
        .await
        .unwrap();

    // Verify snapshot exists
    let snapshots = client
        .describe_db_snapshots()
        .db_snapshot_identifier("e2e-final-snap")
        .send()
        .await
        .unwrap();

    assert_eq!(snapshots.db_snapshots().len(), 1);

    // Restore from snapshot and verify data
    let response = client
        .restore_db_instance_from_db_snapshot()
        .db_instance_identifier("e2e-rds-restored")
        .db_snapshot_identifier("e2e-final-snap")
        .send()
        .await
        .unwrap();

    let restored = response.db_instance().expect("db instance");
    let restored_port = restored.endpoint().unwrap().port().unwrap();

    let (postgres, connection) =
        connect_with_retry("127.0.0.1", restored_port, "admin", "secret123", "testdb")
            .await
            .expect("connect to restored db");

    tokio::spawn(connection);

    let row = postgres
        .query_one("SELECT value FROM test_final WHERE id = 1", &[])
        .await
        .expect("query restored data");

    let value: &str = row.get(0);
    assert_eq!(value, "preserved");
}

#[tokio::test]
async fn pagination_with_real_instances() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Create 150 instances to test pagination
    let mut instance_ids = Vec::new();
    for i in 1..=150 {
        let id = format!("e2e-paginate-{:03}", i);
        instance_ids.push(id.clone());

        client
            .create_db_instance()
            .db_instance_identifier(&id)
            .allocated_storage(20)
            .db_instance_class("db.t3.micro")
            .engine("postgres")
            .engine_version("16.3")
            .master_username("admin")
            .master_user_password("secret123")
            .send()
            .await
            .unwrap();
    }

    // Paginate through all instances
    let mut collected_ids = Vec::new();
    let mut marker: Option<String> = None;

    loop {
        let mut request = client.describe_db_instances().set_max_records(Some(100));
        if let Some(m) = marker {
            request = request.marker(m);
        }

        let response = request.send().await.unwrap();
        let instances = response.db_instances();

        for instance in instances {
            collected_ids.push(instance.db_instance_identifier().unwrap().to_string());
        }

        marker = response.marker().map(|s| s.to_string());
        if marker.is_none() {
            break;
        }
    }

    // Verify all instances were returned
    assert_eq!(collected_ids.len(), 150);

    // Verify all our instance IDs are present
    for id in &instance_ids {
        assert!(collected_ids.contains(id), "Missing instance: {}", id);
    }
}
#[tokio::test]
async fn rds_parameter_group_families() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Test all supported parameter group families
    let families = vec![
        "postgres16",
        "postgres15",
        "mysql8.0",
        "mariadb10.11",
    ];

    for family in families {
        let group_name = format!("test-pg-{}", family.replace('.', "-"));
        client
            .create_db_parameter_group()
            .db_parameter_group_name(&group_name)
            .db_parameter_group_family(family)
            .description(format!("Test parameter group for {}", family))
            .send()
            .await
            .unwrap();
    }

    // Test invalid family
    let error = client
        .create_db_parameter_group()
        .db_parameter_group_name("test-invalid")
        .db_parameter_group_family("postgres99")
        .description("Invalid family")
        .send()
        .await
        .expect_err("Invalid family should be rejected");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("InvalidParameterValue")
    );
}
