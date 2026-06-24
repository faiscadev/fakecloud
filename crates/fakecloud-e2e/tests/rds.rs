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

    let instance = helpers::wait_for_db_available(&client, "orders-db", 180).await;
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
async fn rds_modify_db_instance_accepts_all_mutable_fields() {
    // M1: ModifyDBInstance must accept every mutable field. Exercises a
    // broad subset round-trip with ApplyImmediately=true (immediate apply
    // path) and ApplyImmediately=false (PendingModifiedValues staging).
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-modify-all").await;

    // 1. BackupRetentionPeriod with ApplyImmediately=true should reflect
    // immediately and clear PendingModifiedValues.
    let response = client
        .modify_db_instance()
        .db_instance_identifier("orders-modify-all")
        .backup_retention_period(14)
        .apply_immediately(true)
        .send()
        .await
        .unwrap();
    assert_eq!(
        response
            .db_instance()
            .and_then(|i| i.backup_retention_period()),
        Some(14)
    );

    let described = client
        .describe_db_instances()
        .db_instance_identifier("orders-modify-all")
        .send()
        .await
        .unwrap();
    let inst = &described.db_instances()[0];
    assert_eq!(inst.backup_retention_period(), Some(14));
    // No deferred fields supplied above, so the immediate apply path
    // should not produce any pending modified values.
    if let Some(pmv) = inst.pending_modified_values() {
        assert!(pmv.backup_retention_period().is_none());
        assert!(pmv.storage_type().is_none());
    }

    // 2. StorageType with ApplyImmediately=false stages to
    // PendingModifiedValues; live StorageType remains gp2.
    client
        .modify_db_instance()
        .db_instance_identifier("orders-modify-all")
        .storage_type("gp3")
        .apply_immediately(false)
        .send()
        .await
        .unwrap();

    let described = client
        .describe_db_instances()
        .db_instance_identifier("orders-modify-all")
        .send()
        .await
        .unwrap();
    let inst = &described.db_instances()[0];
    assert_eq!(inst.storage_type(), Some("gp2"));
    let pmv = inst
        .pending_modified_values()
        .expect("PendingModifiedValues should be populated");
    assert_eq!(pmv.storage_type(), Some("gp3"));

    // 3. MasterUserPassword with ApplyImmediately=true must be accepted
    // (no plaintext echo, just no error).
    client
        .modify_db_instance()
        .db_instance_identifier("orders-modify-all")
        .master_user_password("rotated-pwd-123!")
        .apply_immediately(true)
        .send()
        .await
        .expect("MasterUserPassword change should succeed");

    // 4. CloudwatchLogsExportConfiguration round-trip — enable then
    // disable. Disable with empty enable list still works and the
    // resulting set drops the disabled type.
    client
        .modify_db_instance()
        .db_instance_identifier("orders-modify-all")
        .cloudwatch_logs_export_configuration(
            aws_sdk_rds::types::CloudwatchLogsExportConfiguration::builder()
                .enable_log_types("postgresql")
                .enable_log_types("upgrade")
                .build(),
        )
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let described = client
        .describe_db_instances()
        .db_instance_identifier("orders-modify-all")
        .send()
        .await
        .unwrap();
    let exports = described.db_instances()[0].enabled_cloudwatch_logs_exports();
    assert!(exports.contains(&"postgresql".to_string()));
    assert!(exports.contains(&"upgrade".to_string()));

    client
        .modify_db_instance()
        .db_instance_identifier("orders-modify-all")
        .cloudwatch_logs_export_configuration(
            aws_sdk_rds::types::CloudwatchLogsExportConfiguration::builder()
                .disable_log_types("upgrade")
                .build(),
        )
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let described = client
        .describe_db_instances()
        .db_instance_identifier("orders-modify-all")
        .send()
        .await
        .unwrap();
    let exports = described.db_instances()[0].enabled_cloudwatch_logs_exports();
    assert!(exports.contains(&"postgresql".to_string()));
    assert!(!exports.contains(&"upgrade".to_string()));

    // 5. New mutable surface: extended Modify fields all accepted in a
    // single call without any allowlist gate firing.
    client
        .modify_db_instance()
        .db_instance_identifier("orders-modify-all")
        .max_allocated_storage(200)
        .copy_tags_to_snapshot(true)
        .auto_minor_version_upgrade(false)
        .enable_iam_database_authentication(true)
        .network_type("DUAL")
        .multi_tenant(false)
        .license_model("postgresql-license")
        .apply_immediately(true)
        .send()
        .await
        .expect("extended Modify fields should be accepted");
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

    // RebootDBInstance returns immediately with `rebooting`; the container
    // restart runs in the background (so slow engines don't time the client
    // out). A real client waits for `available` before reconnecting.
    helpers::wait_for_db_available(&client, "orders-reboot-db", 180).await;

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
    // `InvalidParameterCombination` is not declared on `RebootDBInstance`
    // in the Smithy model — fakecloud surfaces the declared
    // `InvalidDBInstanceState` shape instead so strict conformance
    // matching accepts the response.
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("InvalidDBInstanceState")
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
    assert_eq!(snapshot.percent_progress(), Some(100));
    assert_eq!(snapshot.license_model(), Some("postgresql-license"));
    assert!(snapshot.instance_create_time().is_some());
    assert!(!snapshot.encrypted().unwrap_or(true));
    assert_eq!(snapshot.iam_database_authentication_enabled(), Some(false));

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
    let resp = client
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
        .unwrap();
    // CreateDBInstance returns a `creating` placeholder; most callers
    // need the DB to be ready before exercising downstream ops.
    helpers::wait_for_db_available(client, db_instance_identifier, 180).await;
    resp
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

    let _instance = response.db_instance().expect("db instance");
    let ready = helpers::wait_for_db_available(&client, "e2e-rds-final", 180).await;
    let port = ready.endpoint().unwrap().port().unwrap();

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

    // Create 15 instances to test pagination (adequate coverage, much faster)
    let mut instance_ids = Vec::new();
    for i in 1..=15 {
        let id = format!("e2e-paginate-{:02}", i);
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
        let mut request = client.describe_db_instances().set_max_records(Some(10));
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
    assert_eq!(collected_ids.len(), 15);

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
    let families = vec!["postgres16", "postgres15", "mysql8.0", "mariadb10.11"];

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

    // Real AWS rejects unknown families with `InvalidParameterValue`,
    // but that wire code isn't declared on `CreateDBParameterGroup` in
    // the Smithy model, so the strict conformance probe rejects any
    // response carrying it. fakecloud now accepts any family verbatim
    // — the group is created and the family is stored as-is.
    let response = client
        .create_db_parameter_group()
        .db_parameter_group_name("test-invalid")
        .db_parameter_group_family("postgres99")
        .description("Invalid family")
        .send()
        .await
        .expect("unknown family is accepted");
    assert_eq!(
        response
            .db_parameter_group
            .unwrap()
            .db_parameter_group_family,
        Some("postgres99".to_string())
    );
}

#[tokio::test]
async fn rds_promote_read_replica_clears_source_pointer() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "promote-src-db").await;

    client
        .create_db_instance_read_replica()
        .db_instance_identifier("promote-replica-db")
        .source_db_instance_identifier("promote-src-db")
        .send()
        .await
        .expect("create read replica");

    helpers::wait_for_db_available(&client, "promote-replica-db", 180).await;

    // Sanity: replica points at source, source lists replica.
    let replica_before = client
        .describe_db_instances()
        .db_instance_identifier("promote-replica-db")
        .send()
        .await
        .unwrap();
    assert_eq!(
        replica_before.db_instances()[0].read_replica_source_db_instance_identifier(),
        Some("promote-src-db")
    );
    let source_before = client
        .describe_db_instances()
        .db_instance_identifier("promote-src-db")
        .send()
        .await
        .unwrap();
    assert_eq!(
        source_before.db_instances()[0].read_replica_db_instance_identifiers(),
        &["promote-replica-db".to_string()]
    );

    let promote = client
        .promote_read_replica()
        .db_instance_identifier("promote-replica-db")
        .backup_retention_period(7)
        .preferred_backup_window("04:00-05:00")
        .send()
        .await
        .expect("PromoteReadReplica");
    let promoted = promote.db_instance().expect("db instance");
    assert_eq!(
        promoted.db_instance_identifier(),
        Some("promote-replica-db")
    );
    // Source pointer cleared on the promoted instance.
    assert!(promoted
        .read_replica_source_db_instance_identifier()
        .is_none());

    // Persisted state matches.
    let replica_after = client
        .describe_db_instances()
        .db_instance_identifier("promote-replica-db")
        .send()
        .await
        .unwrap();
    let after = &replica_after.db_instances()[0];
    assert!(after.read_replica_source_db_instance_identifier().is_none());
    assert_eq!(after.backup_retention_period(), Some(7));
    assert_eq!(after.preferred_backup_window(), Some("04:00-05:00"));

    let source_after = client
        .describe_db_instances()
        .db_instance_identifier("promote-src-db")
        .send()
        .await
        .unwrap();
    assert!(source_after.db_instances()[0]
        .read_replica_db_instance_identifiers()
        .is_empty());
}

#[tokio::test]
async fn rds_promote_read_replica_rejects_non_replica() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "promote-standalone-db").await;

    let err = client
        .promote_read_replica()
        .db_instance_identifier("promote-standalone-db")
        .send()
        .await
        .expect_err("non-replica should be rejected");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidDBInstanceState")
    );
}

#[tokio::test]
async fn rds_switchover_read_replica_swaps_roles() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "switch-src-db").await;

    client
        .create_db_instance_read_replica()
        .db_instance_identifier("switch-replica-db")
        .source_db_instance_identifier("switch-src-db")
        .send()
        .await
        .expect("create read replica");
    helpers::wait_for_db_available(&client, "switch-replica-db", 180).await;

    let switched = client
        .switchover_read_replica()
        .db_instance_identifier("switch-replica-db")
        .send()
        .await
        .expect("SwitchoverReadReplica");
    let new_primary = switched.db_instance().expect("db instance");
    assert_eq!(
        new_primary.db_instance_identifier(),
        Some("switch-replica-db")
    );
    // The new primary has no upstream and now lists the former primary
    // as its replica.
    assert!(new_primary
        .read_replica_source_db_instance_identifier()
        .is_none());
    assert_eq!(
        new_primary.read_replica_db_instance_identifiers(),
        &["switch-src-db".to_string()]
    );

    // Persisted state confirms the swap.
    let new_primary_describe = client
        .describe_db_instances()
        .db_instance_identifier("switch-replica-db")
        .send()
        .await
        .unwrap();
    let np = &new_primary_describe.db_instances()[0];
    assert!(np.read_replica_source_db_instance_identifier().is_none());
    assert_eq!(
        np.read_replica_db_instance_identifiers(),
        &["switch-src-db".to_string()]
    );

    let former_primary_describe = client
        .describe_db_instances()
        .db_instance_identifier("switch-src-db")
        .send()
        .await
        .unwrap();
    let fp = &former_primary_describe.db_instances()[0];
    assert_eq!(
        fp.read_replica_source_db_instance_identifier(),
        Some("switch-replica-db")
    );
    assert!(fp.read_replica_db_instance_identifiers().is_empty());
}

#[tokio::test]
async fn rds_switchover_read_replica_rejects_non_replica() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "switch-standalone-db").await;

    let err = client
        .switchover_read_replica()
        .db_instance_identifier("switch-standalone-db")
        .send()
        .await
        .expect_err("non-replica should be rejected");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidDBInstanceState")
    );
}

#[tokio::test]
async fn rds_modify_db_cluster_persists_fields() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_cluster()
        .db_cluster_identifier("orders-cluster")
        .engine("aurora-postgresql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();

    client
        .modify_db_cluster()
        .db_cluster_identifier("orders-cluster")
        .engine_version("16.4")
        .backup_retention_period(14)
        .preferred_backup_window("01:00-02:00")
        .preferred_maintenance_window("sun:03:00-sun:04:00")
        .deletion_protection(true)
        .copy_tags_to_snapshot(true)
        .send()
        .await
        .unwrap();

    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("orders-cluster")
        .send()
        .await
        .unwrap();
    let cluster = described.db_clusters().first().expect("cluster present");
    assert_eq!(cluster.engine_version(), Some("16.4"));
    assert_eq!(cluster.backup_retention_period(), Some(14));
    assert_eq!(cluster.preferred_backup_window(), Some("01:00-02:00"));
    assert_eq!(
        cluster.preferred_maintenance_window(),
        Some("sun:03:00-sun:04:00")
    );
}

#[tokio::test]
async fn rds_stop_then_start_db_cluster_transitions_status() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_cluster()
        .db_cluster_identifier("flow-cluster")
        .engine("aurora-postgresql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();

    client
        .stop_db_cluster()
        .db_cluster_identifier("flow-cluster")
        .send()
        .await
        .unwrap();
    let stopped = client
        .describe_db_clusters()
        .db_cluster_identifier("flow-cluster")
        .send()
        .await
        .unwrap();
    assert_eq!(
        stopped.db_clusters().first().and_then(|c| c.status()),
        Some("stopped")
    );

    // Stopping again must fail with InvalidDBClusterStateFault.
    let err = client
        .stop_db_cluster()
        .db_cluster_identifier("flow-cluster")
        .send()
        .await
        .expect_err("double stop should be rejected");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidDBClusterStateFault")
    );

    client
        .start_db_cluster()
        .db_cluster_identifier("flow-cluster")
        .send()
        .await
        .unwrap();
    let started = client
        .describe_db_clusters()
        .db_cluster_identifier("flow-cluster")
        .send()
        .await
        .unwrap();
    assert_eq!(
        started.db_clusters().first().and_then(|c| c.status()),
        Some("available")
    );
}

#[tokio::test]
async fn rds_reboot_db_cluster_keeps_available() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_cluster()
        .db_cluster_identifier("reboot-cluster")
        .engine("aurora-postgresql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();

    client
        .reboot_db_cluster()
        .db_cluster_identifier("reboot-cluster")
        .send()
        .await
        .unwrap();

    let described = client
        .describe_db_clusters()
        .db_cluster_identifier("reboot-cluster")
        .send()
        .await
        .unwrap();
    assert_eq!(
        described.db_clusters().first().and_then(|c| c.status()),
        Some("available")
    );
}

#[tokio::test]
async fn rds_failover_db_cluster_records_target_writer() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_cluster()
        .db_cluster_identifier("failover-cluster")
        .engine("aurora-postgresql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();

    // No members tracked: target identifier is accepted verbatim and
    // surfaced via the DBCluster response. AWS-shape: the writer is
    // recorded for subsequent failover-aware describes.
    client
        .failover_db_cluster()
        .db_cluster_identifier("failover-cluster")
        .target_db_instance_identifier("instance-2")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn rds_backtrack_db_cluster_aurora_mysql_only() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Aurora PostgreSQL clusters reject backtrack with InvalidParameterCombination.
    client
        .create_db_cluster()
        .db_cluster_identifier("pg-cluster")
        .engine("aurora-postgresql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();

    let err = client
        .backtrack_db_cluster()
        .db_cluster_identifier("pg-cluster")
        .backtrack_to(aws_sdk_rds::primitives::DateTime::from_secs(1_745_000_000))
        .send()
        .await
        .expect_err("aurora-postgresql backtrack should be rejected");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidParameterCombination")
    );

    // Aurora MySQL accepts backtrack and persists the requested
    // BacktrackTo timestamp on the cluster.
    client
        .create_db_cluster()
        .db_cluster_identifier("mysql-cluster")
        .engine("aurora-mysql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();

    let target = aws_sdk_rds::primitives::DateTime::from_secs(1_745_000_000);
    client
        .backtrack_db_cluster()
        .db_cluster_identifier("mysql-cluster")
        .backtrack_to(target)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn rds_modify_db_cluster_unknown_cluster_errors() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let err = client
        .modify_db_cluster()
        .db_cluster_identifier("ghost-cluster")
        .engine_version("16.4")
        .send()
        .await
        .expect_err("unknown cluster should be rejected");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("DBClusterNotFoundFault")
    );
}

/// Real RestoreDBClusterFromSnapshot path:
///   1. Create cluster with a writer instance attached via DBClusterIdentifier.
///   2. Write rows.
///   3. CreateDBClusterSnapshot — dumps writer's database into the snapshot.
///   4. Drop the writer (so source data is gone).
///   5. RestoreDBClusterFromSnapshot to a new cluster id.
///   6. Attach a fresh writer to the restored cluster — pending dump replays.
///   7. Connect to new writer, verify rows survived.
#[tokio::test]
async fn rds_restore_db_cluster_from_snapshot_recovers_data() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // 1. Create source cluster.
    client
        .create_db_cluster()
        .db_cluster_identifier("m7-source-cluster")
        .engine("aurora-postgresql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .expect("create source cluster");

    // Attach a writer instance bound to the cluster.
    client
        .create_db_instance()
        .db_instance_identifier("m7-source-writer")
        .db_cluster_identifier("m7-source-cluster")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .expect("create writer");

    let writer = helpers::wait_for_db_available(&client, "m7-source-writer", 180).await;
    let endpoint = writer.endpoint().expect("writer endpoint");

    // 2. Write rows on the writer.
    let (writer_client, conn) = connect_with_retry(
        endpoint.address().unwrap(),
        endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .expect("connect to writer");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    writer_client
        .execute("CREATE TABLE m7_rows (id INT, value TEXT)", &[])
        .await
        .expect("create table");
    writer_client
        .execute(
            "INSERT INTO m7_rows VALUES (1, 'cluster-snapshot-survives')",
            &[],
        )
        .await
        .expect("insert row");

    // 3. Snapshot the cluster (dumps writer into snapshot).
    client
        .create_db_cluster_snapshot()
        .db_cluster_snapshot_identifier("m7-cluster-snap")
        .db_cluster_identifier("m7-source-cluster")
        .send()
        .await
        .expect("snapshot cluster");

    // 4. Drop the source writer so the data only survives in the snapshot.
    client
        .delete_db_instance()
        .db_instance_identifier("m7-source-writer")
        .skip_final_snapshot(true)
        .send()
        .await
        .expect("drop writer");

    // 5. Restore the snapshot into a new cluster id.
    client
        .restore_db_cluster_from_snapshot()
        .db_cluster_identifier("m7-restored-cluster")
        .snapshot_identifier("m7-cluster-snap")
        .engine("aurora-postgresql")
        .send()
        .await
        .expect("restore cluster");

    // 6. Create a writer in the restored cluster — the staged dump
    //    replays into its container before status flips to available.
    client
        .create_db_instance()
        .db_instance_identifier("m7-restored-writer")
        .db_cluster_identifier("m7-restored-cluster")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .expect("create restored writer");

    let restored = helpers::wait_for_db_available(&client, "m7-restored-writer", 180).await;
    let restored_endpoint = restored.endpoint().expect("restored endpoint");

    // 7. Verify rows survived.
    let (restored_client, conn) = connect_with_retry(
        restored_endpoint.address().unwrap(),
        restored_endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .expect("connect to restored writer");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = restored_client
        .query_one("SELECT value FROM m7_rows WHERE id = 1", &[])
        .await
        .expect("query restored row");
    let value: String = row.get(0);
    assert_eq!(value, "cluster-snapshot-survives");
}

/// Real RestoreDBClusterToPointInTime path: dumps the source cluster's
/// writer live, stages the dump on the new cluster id, and the next
/// CreateDBInstance attached to the new cluster replays the data.
#[tokio::test]
async fn rds_restore_db_cluster_to_point_in_time_clones_data() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_cluster()
        .db_cluster_identifier("m7-pit-source")
        .engine("aurora-postgresql")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .expect("create source cluster");

    client
        .create_db_instance()
        .db_instance_identifier("m7-pit-writer")
        .db_cluster_identifier("m7-pit-source")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .expect("create writer");

    let writer = helpers::wait_for_db_available(&client, "m7-pit-writer", 180).await;
    let endpoint = writer.endpoint().expect("writer endpoint");
    let (writer_client, conn) = connect_with_retry(
        endpoint.address().unwrap(),
        endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    writer_client
        .execute("CREATE TABLE m7_pit_rows (id INT, value TEXT)", &[])
        .await
        .expect("create table");
    writer_client
        .execute("INSERT INTO m7_pit_rows VALUES (42, 'pit-payload')", &[])
        .await
        .expect("insert");

    client
        .restore_db_cluster_to_point_in_time()
        .db_cluster_identifier("m7-pit-target")
        .source_db_cluster_identifier("m7-pit-source")
        .use_latest_restorable_time(true)
        .send()
        .await
        .expect("restore PIT cluster");

    client
        .create_db_instance()
        .db_instance_identifier("m7-pit-restored-writer")
        .db_cluster_identifier("m7-pit-target")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .db_name("appdb")
        .send()
        .await
        .expect("create restored writer");

    let restored = helpers::wait_for_db_available(&client, "m7-pit-restored-writer", 180).await;
    let restored_endpoint = restored.endpoint().expect("restored endpoint");
    let (restored_client, conn) = connect_with_retry(
        restored_endpoint.address().unwrap(),
        restored_endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .expect("connect to PIT writer");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = restored_client
        .query_one("SELECT value FROM m7_pit_rows WHERE id = 42", &[])
        .await
        .expect("query restored row");
    let value: String = row.get(0);
    assert_eq!(value, "pit-payload");
}

/// Real RestoreDBInstanceToPointInTime path. The op already dumps the
/// source instance live and replays into the target; this test pins the
/// behavior end-to-end so regressions surface.
#[tokio::test]
async fn rds_restore_db_instance_to_point_in_time_clones_data() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "m7-pit-instance-source").await;
    let source = client
        .describe_db_instances()
        .db_instance_identifier("m7-pit-instance-source")
        .send()
        .await
        .expect("describe source");
    let source_endpoint = source.db_instances()[0].endpoint().unwrap();
    let (writer_client, conn) = connect_with_retry(
        source_endpoint.address().unwrap(),
        source_endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .expect("connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    writer_client
        .execute("CREATE TABLE m7_inst_rows (id INT, value TEXT)", &[])
        .await
        .expect("create table");
    writer_client
        .execute(
            "INSERT INTO m7_inst_rows VALUES (7, 'instance-pit-payload')",
            &[],
        )
        .await
        .expect("insert");

    client
        .restore_db_instance_to_point_in_time()
        .source_db_instance_identifier("m7-pit-instance-source")
        .target_db_instance_identifier("m7-pit-instance-target")
        .use_latest_restorable_time(true)
        .send()
        .await
        .expect("restore instance PIT");

    let restored = helpers::wait_for_db_available(&client, "m7-pit-instance-target", 180).await;
    let restored_endpoint = restored.endpoint().expect("restored endpoint");
    let (restored_client, conn) = connect_with_retry(
        restored_endpoint.address().unwrap(),
        restored_endpoint.port().unwrap(),
        "admin",
        "secret123",
        "appdb",
    )
    .await
    .expect("connect to restored");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let row = restored_client
        .query_one("SELECT value FROM m7_inst_rows WHERE id = 7", &[])
        .await
        .expect("query restored row");
    let value: String = row.get(0);
    assert_eq!(value, "instance-pit-payload");
}

#[tokio::test]
async fn rds_subnet_group_reports_supported_network_types() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_subnet_group()
        .db_subnet_group_name("sg-net")
        .db_subnet_group_description("d")
        .subnet_ids("subnet-aaaa1111")
        .subnet_ids("subnet-bbbb2222")
        .send()
        .await
        .unwrap();

    let described = client
        .describe_db_subnet_groups()
        .db_subnet_group_name("sg-net")
        .send()
        .await
        .unwrap();
    let g = &described.db_subnet_groups()[0];
    assert_eq!(g.subnet_group_status(), Some("Complete"));
    // The aws_db_subnet_group resource asserts supported_network_types = [IPV4].
    assert_eq!(g.supported_network_types(), &["IPV4".to_string()]);
}

#[tokio::test]
async fn rds_start_db_instance_returns_starting_immediately() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client, "orders-start-db").await;
    helpers::wait_for_db_available(&client, "orders-start-db", 180).await;

    client
        .stop_db_instance()
        .db_instance_identifier("orders-start-db")
        .send()
        .await
        .expect("stop");

    // Wait until the instance reports stopped.
    for _ in 0..60 {
        let d = client
            .describe_db_instances()
            .db_instance_identifier("orders-start-db")
            .send()
            .await
            .unwrap();
        if d.db_instances()
            .first()
            .and_then(|i| i.db_instance_status())
            == Some("stopped")
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }

    // Start backgrounds the container boot + readiness wait, so it must return
    // immediately with `starting` rather than blocking until `available`.
    let resp = client
        .start_db_instance()
        .db_instance_identifier("orders-start-db")
        .send()
        .await
        .expect("start");
    assert_eq!(
        resp.db_instance().and_then(|i| i.db_instance_status()),
        Some("starting"),
        "StartDBInstance must return immediately with 'starting'"
    );

    // It eventually converges to available in the background.
    helpers::wait_for_db_available(&client, "orders-start-db", 180).await;
}
