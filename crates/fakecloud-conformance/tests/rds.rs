#![recursion_limit = "512"]

mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("rds", "DescribeDBEngineVersions", checksum = "3b5752a4")]
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
    // Returns all postgres versions
    assert!(versions.len() >= 4);
    assert!(versions.iter().any(|v| v.engine_version() == Some("16.3")));
}

#[test_action("rds", "DescribeOrderableDBInstanceOptions", checksum = "cc28ac3c")]
#[tokio::test]
async fn rds_describe_orderable_db_instance_options() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let response = client
        .describe_orderable_db_instance_options()
        .engine("postgres")
        .engine_version("16.3")
        .send()
        .await
        .unwrap();

    let options = response.orderable_db_instance_options();
    assert_eq!(options.len(), 7); // 7 instance classes per engine/version
    assert_eq!(options[0].engine(), Some("postgres"));
    assert_eq!(options[0].engine_version(), Some("16.3"));
    assert_eq!(options[0].db_instance_class(), Some("db.t3.micro"));
}

#[test_action("rds", "CreateDBInstance", checksum = "66cdd119")]
#[tokio::test]
async fn rds_create_db_instance() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let response = client
        .create_db_instance()
        .db_instance_identifier("conf-rds-db")
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

    let instance = response.db_instance().expect("db instance");
    assert_eq!(instance.db_instance_identifier(), Some("conf-rds-db"));
    assert_eq!(instance.engine(), Some("postgres"));
    assert_eq!(instance.db_instance_status(), Some("creating"));
}

#[test_action("rds", "DescribeDBInstances", checksum = "aa5486d4")]
#[tokio::test]
async fn rds_describe_db_instances() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_instance()
        .db_instance_identifier("conf-rds-db")
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

    wait_for_db_instance_available(&client, "conf-rds-db").await;

    let response = client
        .describe_db_instances()
        .db_instance_identifier("conf-rds-db")
        .send()
        .await
        .unwrap();

    let instances = response.db_instances();
    assert_eq!(instances.len(), 1);
    assert_eq!(instances[0].db_instance_identifier(), Some("conf-rds-db"));
    assert_eq!(instances[0].db_instance_status(), Some("available"));
    assert_eq!(
        instances[0]
            .endpoint()
            .and_then(|endpoint| endpoint.address()),
        Some("127.0.0.1")
    );
}

#[test_action("rds", "DeleteDBInstance", checksum = "22909663")]
#[tokio::test]
async fn rds_delete_db_instance() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;

    let response = client
        .delete_db_instance()
        .db_instance_identifier("conf-rds-db")
        .skip_final_snapshot(true)
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    assert_eq!(instance.db_instance_identifier(), Some("conf-rds-db"));
    assert_eq!(instance.db_instance_status(), Some("deleting"));

    let error = client
        .describe_db_instances()
        .db_instance_identifier("conf-rds-db")
        .send()
        .await
        .expect_err("instance should be deleted");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("DBInstanceNotFound")
    );
}

#[test_action("rds", "ModifyDBInstance", checksum = "08b493a8")]
#[tokio::test]
async fn rds_modify_db_instance() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;

    let response = client
        .modify_db_instance()
        .db_instance_identifier("conf-rds-db")
        .deletion_protection(true)
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    assert_eq!(instance.db_instance_status(), Some("modifying"));
    assert_eq!(instance.deletion_protection(), Some(true));
}

#[test_action("rds", "RebootDBInstance", checksum = "cd4d463b")]
#[tokio::test]
async fn rds_reboot_db_instance() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;

    let response = client
        .reboot_db_instance()
        .db_instance_identifier("conf-rds-db")
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    assert_eq!(instance.db_instance_identifier(), Some("conf-rds-db"));
    assert_eq!(instance.db_instance_status(), Some("rebooting"));
}

#[tokio::test]
async fn rds_modify_db_instance_with_apply_immediately_false() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;

    // Modify with ApplyImmediately=false should stage changes
    let response = client
        .modify_db_instance()
        .db_instance_identifier("conf-rds-db")
        .db_instance_class("db.t3.small")
        .apply_immediately(false)
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    // Instance class should still be the original
    assert_eq!(instance.db_instance_class(), Some("db.t3.micro"));
    // Pending changes should exist
    let pending = instance.pending_modified_values().expect("pending values");
    assert_eq!(pending.db_instance_class(), Some("db.t3.small"));

    // Reboot should apply pending changes
    let response = client
        .reboot_db_instance()
        .db_instance_identifier("conf-rds-db")
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    // Instance class should now be updated
    assert_eq!(instance.db_instance_class(), Some("db.t3.small"));
    // Pending changes should be cleared
    assert!(instance.pending_modified_values().is_none());
}

#[tokio::test]
async fn rds_delete_db_instance_rejects_deletion_protection() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance_with_deletion_protection(&client, "conf-rds-protected-db", true).await;

    let error = client
        .delete_db_instance()
        .db_instance_identifier("conf-rds-protected-db")
        .skip_final_snapshot(true)
        .send()
        .await
        .expect_err("deletion protection should block deletion");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("InvalidDBInstanceState")
    );
}

#[test_action("rds", "AddTagsToResource", checksum = "79e71104")]
#[tokio::test]
async fn rds_add_tags_to_resource() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let create = create_instance(&client).await;
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
        .send()
        .await
        .unwrap();

    let response = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();
    let tags = response.tag_list();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), Some("env"));
    assert_eq!(tags[0].value(), Some("dev"));
}

#[test_action("rds", "ListTagsForResource", checksum = "28355104")]
#[tokio::test]
async fn rds_list_tags_for_resource() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let create = create_instance(&client).await;
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

    let response = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();

    let tags = response.tag_list();
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0].key(), Some("env"));
    assert_eq!(tags[1].key(), Some("team"));
}

#[test_action("rds", "RemoveTagsFromResource", checksum = "8bc51a12")]
#[tokio::test]
async fn rds_remove_tags_from_resource() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let create = create_instance(&client).await;
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

    client
        .remove_tags_from_resource()
        .resource_name(arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let response = client
        .list_tags_for_resource()
        .resource_name(arn)
        .send()
        .await
        .unwrap();
    let tags = response.tag_list();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), Some("team"));
}

#[test_action("rds", "CreateDBSnapshot", checksum = "bdeba3a7")]
#[tokio::test]
async fn rds_create_db_snapshot() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;

    let response = client
        .create_db_snapshot()
        .db_instance_identifier("conf-rds-db")
        .db_snapshot_identifier("conf-snapshot")
        .send()
        .await
        .unwrap();

    let snapshot = response.db_snapshot().unwrap();
    assert_eq!(snapshot.db_snapshot_identifier(), Some("conf-snapshot"));
    assert_eq!(snapshot.db_instance_identifier(), Some("conf-rds-db"));
    assert_eq!(snapshot.engine(), Some("postgres"));
    assert_eq!(snapshot.status(), Some("available"));
}

#[test_action("rds", "DescribeDBSnapshots", checksum = "c67cf62b")]
#[tokio::test]
async fn rds_describe_db_snapshots() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;
    client
        .create_db_snapshot()
        .db_instance_identifier("conf-rds-db")
        .db_snapshot_identifier("conf-snapshot")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_db_snapshots()
        .db_snapshot_identifier("conf-snapshot")
        .send()
        .await
        .unwrap();

    let snapshots = response.db_snapshots();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].db_snapshot_identifier(), Some("conf-snapshot"));
}

#[test_action("rds", "DeleteDBSnapshot", checksum = "cdb4726c")]
#[tokio::test]
async fn rds_delete_db_snapshot() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;
    client
        .create_db_snapshot()
        .db_instance_identifier("conf-rds-db")
        .db_snapshot_identifier("conf-snapshot")
        .send()
        .await
        .unwrap();

    let response = client
        .delete_db_snapshot()
        .db_snapshot_identifier("conf-snapshot")
        .send()
        .await
        .unwrap();

    let snapshot = response.db_snapshot().unwrap();
    assert_eq!(snapshot.db_snapshot_identifier(), Some("conf-snapshot"));

    let error = client
        .describe_db_snapshots()
        .db_snapshot_identifier("conf-snapshot")
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("DBSnapshotNotFound")
    );
}

#[test_action("rds", "RestoreDBInstanceFromDBSnapshot", checksum = "368eb366")]
#[tokio::test]
async fn rds_restore_db_instance_from_db_snapshot() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;
    client
        .create_db_snapshot()
        .db_instance_identifier("conf-rds-db")
        .db_snapshot_identifier("conf-snapshot")
        .send()
        .await
        .unwrap();

    let response = client
        .restore_db_instance_from_db_snapshot()
        .db_instance_identifier("restored-db")
        .db_snapshot_identifier("conf-snapshot")
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().unwrap();
    assert_eq!(instance.db_instance_identifier(), Some("restored-db"));
    assert_eq!(instance.engine(), Some("postgres"));
    assert_eq!(instance.master_username(), Some("admin"));
    assert_eq!(instance.db_name(), Some("appdb"));
}

#[test_action("rds", "CreateDBInstanceReadReplica", checksum = "23be1880")]
#[tokio::test]
async fn rds_create_db_instance_read_replica() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    create_instance(&client).await;

    let response = client
        .create_db_instance_read_replica()
        .db_instance_identifier("conf-read-replica")
        .source_db_instance_identifier("conf-rds-db")
        .send()
        .await
        .unwrap();

    let replica = response.db_instance().unwrap();
    assert_eq!(replica.db_instance_identifier(), Some("conf-read-replica"));
    assert_eq!(replica.engine(), Some("postgres"));
    assert_eq!(
        replica.read_replica_source_db_instance_identifier(),
        Some("conf-rds-db")
    );

    let describe = client
        .describe_db_instances()
        .db_instance_identifier("conf-rds-db")
        .send()
        .await
        .unwrap();
    let source = &describe.db_instances()[0];
    assert_eq!(source.read_replica_db_instance_identifiers().len(), 1);
    assert_eq!(
        source.read_replica_db_instance_identifiers()[0],
        "conf-read-replica"
    );
}

async fn create_instance(
    client: &aws_sdk_rds::Client,
) -> aws_sdk_rds::operation::create_db_instance::CreateDbInstanceOutput {
    create_instance_with_deletion_protection(client, "conf-rds-db", false).await
}

async fn create_instance_with_deletion_protection(
    client: &aws_sdk_rds::Client,
    db_instance_identifier: &str,
    deletion_protection: bool,
) -> aws_sdk_rds::operation::create_db_instance::CreateDbInstanceOutput {
    let response = client
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
    wait_for_db_instance_available(client, db_instance_identifier).await;
    response
}

/// CreateDBInstance returns immediately with status="creating" and the
/// real postgres container starts in a background task. Tests that
/// chain operations on the instance (snapshot, modify, reboot, etc.)
/// need it to reach "available" first or the followup either errors or
/// observes intermediate state. Polls DescribeDBInstances every 250ms
/// for up to 60s.
async fn wait_for_db_instance_available(
    client: &aws_sdk_rds::Client,
    db_instance_identifier: &str,
) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    loop {
        let response = client
            .describe_db_instances()
            .db_instance_identifier(db_instance_identifier)
            .send()
            .await
            .unwrap_or_else(|err| {
                panic!("describe_db_instances during wait failed: {err:?}");
            });
        let status = response
            .db_instances()
            .first()
            .and_then(|i| i.db_instance_status())
            .unwrap_or("")
            .to_string();
        if status == "available" {
            return;
        }
        if status == "failed" {
            panic!(
                "db instance {db_instance_identifier} entered status `failed` while waiting for available"
            );
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for db instance {db_instance_identifier} to reach available (last status: {status})"
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

#[test_action("rds", "CreateDBSubnetGroup", checksum = "1b1b06a3")]
#[tokio::test]
async fn rds_create_db_subnet_group() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let response = client
        .create_db_subnet_group()
        .db_subnet_group_name("conf-subnet-group")
        .db_subnet_group_description("Test subnet group")
        .subnet_ids("subnet-12345")
        .subnet_ids("subnet-67890")
        .send()
        .await
        .unwrap();

    let subnet_group = response.db_subnet_group().unwrap();
    assert_eq!(
        subnet_group.db_subnet_group_name(),
        Some("conf-subnet-group")
    );
    assert_eq!(
        subnet_group.db_subnet_group_description(),
        Some("Test subnet group")
    );
    assert_eq!(subnet_group.subnets().len(), 2);
}

#[test_action("rds", "DescribeDBSubnetGroups", checksum = "97a0e63e")]
#[tokio::test]
async fn rds_describe_db_subnet_groups() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_subnet_group()
        .db_subnet_group_name("conf-subnet-group")
        .db_subnet_group_description("Test subnet group")
        .subnet_ids("subnet-12345")
        .subnet_ids("subnet-67890")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_db_subnet_groups()
        .db_subnet_group_name("conf-subnet-group")
        .send()
        .await
        .unwrap();

    let subnet_groups = response.db_subnet_groups();
    assert_eq!(subnet_groups.len(), 1);
    assert_eq!(
        subnet_groups[0].db_subnet_group_name(),
        Some("conf-subnet-group")
    );
}

#[test_action("rds", "ModifyDBSubnetGroup", checksum = "390acd2d")]
#[tokio::test]
async fn rds_modify_db_subnet_group() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_subnet_group()
        .db_subnet_group_name("conf-subnet-group")
        .db_subnet_group_description("Test subnet group")
        .subnet_ids("subnet-12345")
        .subnet_ids("subnet-67890")
        .send()
        .await
        .unwrap();

    let response = client
        .modify_db_subnet_group()
        .db_subnet_group_name("conf-subnet-group")
        .subnet_ids("subnet-11111")
        .subnet_ids("subnet-22222")
        .subnet_ids("subnet-33333")
        .send()
        .await
        .unwrap();

    let subnet_group = response.db_subnet_group().unwrap();
    assert_eq!(subnet_group.subnets().len(), 3);
}

#[test_action("rds", "DeleteDBSubnetGroup", checksum = "e1ea45a9")]
#[tokio::test]
async fn rds_delete_db_subnet_group() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_subnet_group()
        .db_subnet_group_name("conf-subnet-group")
        .db_subnet_group_description("Test subnet group")
        .subnet_ids("subnet-12345")
        .subnet_ids("subnet-67890")
        .send()
        .await
        .unwrap();

    client
        .delete_db_subnet_group()
        .db_subnet_group_name("conf-subnet-group")
        .send()
        .await
        .unwrap();

    let error = client
        .describe_db_subnet_groups()
        .db_subnet_group_name("conf-subnet-group")
        .send()
        .await
        .expect_err("subnet group should be deleted");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("DBSubnetGroupNotFoundFault")
    );
}

#[test_action("rds", "CreateDBParameterGroup", checksum = "d0c5767f")]
#[tokio::test]
async fn rds_create_db_parameter_group() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let response = client
        .create_db_parameter_group()
        .db_parameter_group_name("conf-param-group")
        .db_parameter_group_family("postgres16")
        .description("Test parameter group")
        .send()
        .await
        .unwrap();

    let param_group = response.db_parameter_group().unwrap();
    assert_eq!(
        param_group.db_parameter_group_name(),
        Some("conf-param-group")
    );
    assert_eq!(param_group.db_parameter_group_family(), Some("postgres16"));
}

#[test_action("rds", "DescribeDBParameterGroups", checksum = "4032d108")]
#[tokio::test]
async fn rds_describe_db_parameter_groups() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_parameter_group()
        .db_parameter_group_name("conf-param-group")
        .db_parameter_group_family("postgres16")
        .description("Test parameter group")
        .send()
        .await
        .unwrap();

    let response = client
        .describe_db_parameter_groups()
        .db_parameter_group_name("conf-param-group")
        .send()
        .await
        .unwrap();

    let param_groups = response.db_parameter_groups();
    assert!(!param_groups.is_empty());
    let found = param_groups
        .iter()
        .find(|pg| pg.db_parameter_group_name() == Some("conf-param-group"));
    assert!(found.is_some());
}

#[test_action("rds", "DeleteDBParameterGroup", checksum = "2fec5329")]
#[tokio::test]
async fn rds_delete_db_parameter_group() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_parameter_group()
        .db_parameter_group_name("conf-param-group")
        .db_parameter_group_family("postgres16")
        .description("Test parameter group")
        .send()
        .await
        .unwrap();

    client
        .delete_db_parameter_group()
        .db_parameter_group_name("conf-param-group")
        .send()
        .await
        .unwrap();

    let error = client
        .describe_db_parameter_groups()
        .db_parameter_group_name("conf-param-group")
        .send()
        .await
        .expect_err("parameter group should be deleted");
    assert_eq!(
        error.into_service_error().meta().code(),
        Some("DBParameterGroupNotFound")
    );
}

#[test_action("rds", "CreateDBInstance", checksum = "66cdd119")]
#[tokio::test]
async fn rds_create_db_instance_with_vpc_security_groups() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    let response = client
        .create_db_instance()
        .db_instance_identifier("conf-rds-sg")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .vpc_security_group_ids("sg-12345678")
        .vpc_security_group_ids("sg-87654321")
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    assert_eq!(instance.db_instance_identifier(), Some("conf-rds-sg"));

    let sg_memberships = instance.vpc_security_groups();
    assert_eq!(sg_memberships.len(), 2);
    assert_eq!(
        sg_memberships[0].vpc_security_group_id(),
        Some("sg-12345678")
    );
    assert_eq!(sg_memberships[0].status(), Some("active"));
    assert_eq!(
        sg_memberships[1].vpc_security_group_id(),
        Some("sg-87654321")
    );
    assert_eq!(sg_memberships[1].status(), Some("active"));
}

#[test_action("rds", "ModifyDBInstance", checksum = "08b493a8")]
#[tokio::test]
async fn rds_modify_db_instance_vpc_security_groups() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_instance()
        .db_instance_identifier("conf-rds-sg-modify")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .vpc_security_group_ids("sg-original")
        .send()
        .await
        .unwrap();

    let response = client
        .modify_db_instance()
        .db_instance_identifier("conf-rds-sg-modify")
        .vpc_security_group_ids("sg-modified1")
        .vpc_security_group_ids("sg-modified2")
        .vpc_security_group_ids("sg-modified3")
        .apply_immediately(true)
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    let sg_memberships = instance.vpc_security_groups();
    assert_eq!(sg_memberships.len(), 3);
    assert_eq!(
        sg_memberships[0].vpc_security_group_id(),
        Some("sg-modified1")
    );
    assert_eq!(
        sg_memberships[1].vpc_security_group_id(),
        Some("sg-modified2")
    );
    assert_eq!(
        sg_memberships[2].vpc_security_group_id(),
        Some("sg-modified3")
    );
}

#[test_action("rds", "DeleteDBInstance", checksum = "22909663")]
#[tokio::test]
async fn rds_delete_db_instance_with_final_snapshot() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Create instance
    client
        .create_db_instance()
        .db_instance_identifier("conf-rds-final")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();

    wait_for_db_instance_available(&client, "conf-rds-final").await;

    // Delete with final snapshot
    let response = client
        .delete_db_instance()
        .db_instance_identifier("conf-rds-final")
        .final_db_snapshot_identifier("conf-final-snap")
        .send()
        .await
        .unwrap();

    let instance = response.db_instance().expect("db instance");
    assert_eq!(instance.db_instance_identifier(), Some("conf-rds-final"));

    // Verify snapshot was created
    let snapshots = client
        .describe_db_snapshots()
        .db_snapshot_identifier("conf-final-snap")
        .send()
        .await
        .unwrap();

    assert_eq!(snapshots.db_snapshots().len(), 1);
    assert_eq!(
        snapshots.db_snapshots()[0].db_snapshot_identifier(),
        Some("conf-final-snap")
    );
    assert_eq!(
        snapshots.db_snapshots()[0].db_instance_identifier(),
        Some("conf-rds-final")
    );
}

#[test_action("rds", "DescribeDBInstances", checksum = "aa5486d4")]
#[tokio::test]
async fn rds_describe_db_instances_pagination() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Create 5 instances
    for i in 1..=5 {
        client
            .create_db_instance()
            .db_instance_identifier(format!("conf-paginate-{}", i))
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

    // Request with MaxRecords=2
    let response = client
        .describe_db_instances()
        .set_max_records(Some(2))
        .send()
        .await
        .unwrap();

    assert_eq!(response.db_instances().len(), 2);
    assert!(response.marker().is_some());

    // Request next page
    let response2 = client
        .describe_db_instances()
        .set_marker(response.marker().map(|s| s.to_string()))
        .set_max_records(Some(2))
        .send()
        .await
        .unwrap();

    assert_eq!(response2.db_instances().len(), 2);
    assert!(response2.marker().is_some());

    // Request final page
    let response3 = client
        .describe_db_instances()
        .set_marker(response2.marker().map(|s| s.to_string()))
        .set_max_records(Some(2))
        .send()
        .await
        .unwrap();

    assert_eq!(response3.db_instances().len(), 1);
    assert!(response3.marker().is_none());
}

#[test_action("rds", "DescribeDBSnapshots", checksum = "c67cf62b")]
#[tokio::test]
async fn rds_describe_db_snapshots_pagination() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Create instance
    client
        .create_db_instance()
        .db_instance_identifier("conf-snap-paginate")
        .allocated_storage(20)
        .db_instance_class("db.t3.micro")
        .engine("postgres")
        .engine_version("16.3")
        .master_username("admin")
        .master_user_password("secret123")
        .send()
        .await
        .unwrap();

    wait_for_db_instance_available(&client, "conf-snap-paginate").await;

    // Create 3 snapshots
    for i in 1..=3 {
        client
            .create_db_snapshot()
            .db_instance_identifier("conf-snap-paginate")
            .db_snapshot_identifier(format!("conf-snapshot-{}", i))
            .send()
            .await
            .unwrap();
    }

    // Request with MaxRecords=2
    let response = client
        .describe_db_snapshots()
        .set_max_records(Some(2))
        .send()
        .await
        .unwrap();

    assert_eq!(response.db_snapshots().len(), 2);
    assert!(response.marker().is_some());

    // Request next page
    let response2 = client
        .describe_db_snapshots()
        .set_marker(response.marker().map(|s| s.to_string()))
        .send()
        .await
        .unwrap();

    assert_eq!(response2.db_snapshots().len(), 1);
    assert!(response2.marker().is_none());
}

#[test_action("rds", "DescribeDBParameterGroups", checksum = "4032d108")]
#[tokio::test]
async fn rds_describe_db_parameter_groups_pagination() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Create 3 parameter groups
    for i in 1..=3 {
        client
            .create_db_parameter_group()
            .db_parameter_group_name(format!("conf-pg-{}", i))
            .db_parameter_group_family("postgres16")
            .description(format!("Test parameter group {}", i))
            .send()
            .await
            .unwrap();
    }

    // Request with MaxRecords=2 (default group + 2 custom = 3 total, but only 2 returned)
    let response = client
        .describe_db_parameter_groups()
        .set_max_records(Some(2))
        .send()
        .await
        .unwrap();

    assert_eq!(response.db_parameter_groups().len(), 2);
    assert!(response.marker().is_some());

    // Request next page
    let response2 = client
        .describe_db_parameter_groups()
        .set_marker(response.marker().map(|s| s.to_string()))
        .send()
        .await
        .unwrap();

    assert!(!response2.db_parameter_groups().is_empty());
}

#[test_action("rds", "DescribeDBSubnetGroups", checksum = "97a0e63e")]
#[tokio::test]
async fn rds_describe_db_subnet_groups_pagination() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    // Create 3 subnet groups (each with 2 subnets in different AZs)
    for i in 1..=3 {
        client
            .create_db_subnet_group()
            .db_subnet_group_name(format!("conf-subgrp-{}", i))
            .db_subnet_group_description(format!("Test subnet group {}", i))
            .subnet_ids(format!("subnet-{}a", i))
            .subnet_ids(format!("subnet-{}b", i))
            .send()
            .await
            .unwrap();
    }

    // Request with MaxRecords=2
    let response = client
        .describe_db_subnet_groups()
        .set_max_records(Some(2))
        .send()
        .await
        .unwrap();

    assert_eq!(response.db_subnet_groups().len(), 2);
    assert!(response.marker().is_some());

    // Request next page
    let response2 = client
        .describe_db_subnet_groups()
        .set_marker(response.marker().map(|s| s.to_string()))
        .send()
        .await
        .unwrap();

    assert_eq!(response2.db_subnet_groups().len(), 1);
    assert!(response2.marker().is_none());
}

#[test_action("rds", "ModifyDBParameterGroup", checksum = "a86b7ae4")]
#[tokio::test]
async fn rds_modify_db_parameter_group() {
    let server = TestServer::start().await;
    let client = server.rds_client().await;

    client
        .create_db_parameter_group()
        .db_parameter_group_name("conf-modify-pg")
        .db_parameter_group_family("postgres16")
        .description("Original description")
        .send()
        .await
        .unwrap();

    let response = client
        .modify_db_parameter_group()
        .db_parameter_group_name("conf-modify-pg")
        .parameters(
            aws_sdk_rds::types::Parameter::builder()
                .parameter_name("max_connections")
                .parameter_value("100")
                .apply_method(aws_sdk_rds::types::ApplyMethod::PendingReboot)
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(response.db_parameter_group_name(), Some("conf-modify-pg"));
}

// ── Conformance closure batch (all 140 missing RDS ops covered by raw POSTs) ──

const RDS_AUTH: &str = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/rds/aws4_request, SignedHeaders=host, Signature=0";

fn pct(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for &b in s.as_bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'.' || b == b'_' || b == b'~' {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{:02X}", b));
        }
    }
    out
}

async fn rds_post(server: &TestServer, action: &str, params: &[(&str, &str)]) -> reqwest::Response {
    let mut body = format!("Action={action}&Version=2014-10-31");
    for (k, v) in params {
        body.push_str(&format!("&{}={}", pct(k), pct(v)));
    }
    reqwest::Client::new()
        .post(format!("{}/", server.endpoint()))
        .header("content-type", "application/x-www-form-urlencoded")
        .header("Authorization", RDS_AUTH)
        .body(body)
        .send()
        .await
        .unwrap()
}

#[test_action("rds", "AddRoleToDBCluster", checksum = "77b9ae59")]
#[test_action("rds", "AddRoleToDBInstance", checksum = "03acdc74")]
#[test_action("rds", "AddSourceIdentifierToSubscription", checksum = "f6f5fd6c")]
#[test_action("rds", "ApplyPendingMaintenanceAction", checksum = "9b59d2e3")]
#[test_action("rds", "AuthorizeDBSecurityGroupIngress", checksum = "37504e4c")]
#[test_action("rds", "BacktrackDBCluster", checksum = "455a9d8a")]
#[test_action("rds", "CancelExportTask", checksum = "5572da66")]
#[test_action("rds", "CopyDBClusterParameterGroup", checksum = "2bc6a350")]
#[test_action("rds", "CopyDBClusterSnapshot", checksum = "fd51edab")]
#[test_action("rds", "CopyDBParameterGroup", checksum = "e0eccdea")]
#[test_action("rds", "CopyDBSnapshot", checksum = "acf9719f")]
#[test_action("rds", "CopyOptionGroup", checksum = "1ef09200")]
#[test_action("rds", "CreateBlueGreenDeployment", checksum = "f58bfeb5")]
#[test_action("rds", "CreateCustomDBEngineVersion", checksum = "52cd54db")]
#[test_action("rds", "CreateDBCluster", checksum = "d07ca8c4")]
#[test_action("rds", "CreateDBClusterEndpoint", checksum = "52145c35")]
#[test_action("rds", "CreateDBClusterParameterGroup", checksum = "0a2ef3b0")]
#[test_action("rds", "CreateDBClusterSnapshot", checksum = "8d324028")]
#[test_action("rds", "CreateDBProxy", checksum = "4eed42f9")]
#[test_action("rds", "CreateDBProxyEndpoint", checksum = "12b0af64")]
#[test_action("rds", "CreateDBSecurityGroup", checksum = "52b13c13")]
#[test_action("rds", "CreateDBShardGroup", checksum = "57624887")]
#[test_action("rds", "CreateEventSubscription", checksum = "44d30f6b")]
#[test_action("rds", "CreateGlobalCluster", checksum = "d0f8fc90")]
#[test_action("rds", "CreateIntegration", checksum = "0f183e9e")]
#[test_action("rds", "CreateOptionGroup", checksum = "3d567760")]
#[test_action("rds", "CreateTenantDatabase", checksum = "8b04fbab")]
#[test_action("rds", "DeleteBlueGreenDeployment", checksum = "d975777d")]
#[test_action("rds", "DeleteCustomDBEngineVersion", checksum = "e5c03035")]
#[test_action("rds", "DeleteDBCluster", checksum = "32ed1b68")]
#[test_action("rds", "DeleteDBClusterAutomatedBackup", checksum = "963b15cc")]
#[test_action("rds", "DeleteDBClusterEndpoint", checksum = "1ab0fc73")]
#[test_action("rds", "DeleteDBClusterParameterGroup", checksum = "f31a300f")]
#[test_action("rds", "DeleteDBClusterSnapshot", checksum = "ccd88cca")]
#[test_action("rds", "DeleteDBInstanceAutomatedBackup", checksum = "bbf85a11")]
#[test_action("rds", "DeleteDBProxy", checksum = "7aa26818")]
#[test_action("rds", "DeleteDBProxyEndpoint", checksum = "7ffa9d4c")]
#[test_action("rds", "DeleteDBSecurityGroup", checksum = "05bcf520")]
#[test_action("rds", "DeleteDBShardGroup", checksum = "20094951")]
#[test_action("rds", "DeleteEventSubscription", checksum = "475c278b")]
#[test_action("rds", "DeleteGlobalCluster", checksum = "d40070d1")]
#[test_action("rds", "DeleteIntegration", checksum = "3c567bb8")]
#[test_action("rds", "DeleteOptionGroup", checksum = "ffccf6ac")]
#[test_action("rds", "DeleteTenantDatabase", checksum = "7dc0cfd8")]
#[test_action("rds", "DeregisterDBProxyTargets", checksum = "92e78697")]
#[test_action("rds", "DescribeAccountAttributes", checksum = "dc5aa622")]
#[test_action("rds", "DescribeBlueGreenDeployments", checksum = "60c38380")]
#[test_action("rds", "DescribeCertificates", checksum = "4d38e6f0")]
#[test_action("rds", "DescribeDBClusterAutomatedBackups", checksum = "c1e34856")]
#[test_action("rds", "DescribeDBClusterBacktracks", checksum = "f0d869d0")]
#[test_action("rds", "DescribeDBClusterEndpoints", checksum = "fa3e5c46")]
#[test_action("rds", "DescribeDBClusterParameterGroups", checksum = "8c6df452")]
#[test_action("rds", "DescribeDBClusterParameters", checksum = "c1287431")]
#[test_action("rds", "DescribeDBClusterSnapshotAttributes", checksum = "433caffc")]
#[test_action("rds", "DescribeDBClusterSnapshots", checksum = "16673b24")]
#[test_action("rds", "DescribeDBClusters", checksum = "d6425bfa")]
#[test_action("rds", "DescribeDBInstanceAutomatedBackups", checksum = "b1064781")]
#[test_action("rds", "DescribeDBLogFiles", checksum = "a02be352")]
#[test_action("rds", "DescribeDBMajorEngineVersions", checksum = "b8e25a73")]
#[test_action("rds", "DescribeDBParameters", checksum = "9cd70fd1")]
#[test_action("rds", "DescribeDBProxies", checksum = "5e5f3877")]
#[test_action("rds", "DescribeDBProxyEndpoints", checksum = "08dd7f30")]
#[test_action("rds", "DescribeDBProxyTargetGroups", checksum = "f510eed7")]
#[test_action("rds", "DescribeDBProxyTargets", checksum = "ebd953eb")]
#[test_action("rds", "DescribeDBRecommendations", checksum = "678f7444")]
#[test_action("rds", "DescribeDBSecurityGroups", checksum = "ad70ec39")]
#[test_action("rds", "DescribeDBShardGroups", checksum = "a7401fd9")]
#[test_action("rds", "DescribeDBSnapshotAttributes", checksum = "a14a274c")]
#[test_action("rds", "DescribeDBSnapshotTenantDatabases", checksum = "2b82594b")]
#[test_action("rds", "DescribeEngineDefaultClusterParameters", checksum = "33d9ab22")]
#[test_action("rds", "DescribeEngineDefaultParameters", checksum = "5d0a1f9e")]
#[test_action("rds", "DescribeEventCategories", checksum = "09ee29ff")]
#[test_action("rds", "DescribeEventSubscriptions", checksum = "ccf787dc")]
#[test_action("rds", "DescribeEvents", checksum = "7ba9dfe5")]
#[test_action("rds", "DescribeExportTasks", checksum = "226e8e93")]
#[test_action("rds", "DescribeGlobalClusters", checksum = "59b6082b")]
#[test_action("rds", "DescribeIntegrations", checksum = "7d63bfe3")]
#[test_action("rds", "DescribeOptionGroupOptions", checksum = "858785e7")]
#[test_action("rds", "DescribeOptionGroups", checksum = "1b3ed1ef")]
#[test_action("rds", "DescribePendingMaintenanceActions", checksum = "e86b55fb")]
#[test_action("rds", "DescribeReservedDBInstances", checksum = "faf1da16")]
#[test_action("rds", "DescribeReservedDBInstancesOfferings", checksum = "2cec5eb9")]
#[test_action("rds", "DescribeSourceRegions", checksum = "cf1cb01e")]
#[test_action("rds", "DescribeTenantDatabases", checksum = "344d46b9")]
#[test_action("rds", "DescribeValidDBInstanceModifications", checksum = "68488f81")]
#[test_action("rds", "DisableHttpEndpoint", checksum = "801ee728")]
#[test_action("rds", "DownloadDBLogFilePortion", checksum = "6db2dcb5")]
#[test_action("rds", "EnableHttpEndpoint", checksum = "9235608e")]
#[test_action("rds", "FailoverDBCluster", checksum = "3a86085f")]
#[test_action("rds", "FailoverGlobalCluster", checksum = "83b8880c")]
#[test_action("rds", "ModifyActivityStream", checksum = "a74bdb80")]
#[test_action("rds", "ModifyCertificates", checksum = "c3c61abd")]
#[test_action("rds", "ModifyCurrentDBClusterCapacity", checksum = "a390ad65")]
#[test_action("rds", "ModifyCustomDBEngineVersion", checksum = "f412ec4e")]
#[test_action("rds", "ModifyDBCluster", checksum = "6cd8debb")]
#[test_action("rds", "ModifyDBClusterEndpoint", checksum = "20da760f")]
#[test_action("rds", "ModifyDBClusterParameterGroup", checksum = "fb3154b0")]
#[test_action("rds", "ModifyDBClusterSnapshotAttribute", checksum = "4c7eb2b9")]
#[test_action("rds", "ModifyDBProxy", checksum = "e9083e63")]
#[test_action("rds", "ModifyDBProxyEndpoint", checksum = "7f1d0c61")]
#[test_action("rds", "ModifyDBProxyTargetGroup", checksum = "d76aac3a")]
#[test_action("rds", "ModifyDBRecommendation", checksum = "b835f503")]
#[test_action("rds", "ModifyDBShardGroup", checksum = "4fe41f81")]
#[test_action("rds", "ModifyDBSnapshot", checksum = "23c89969")]
#[test_action("rds", "ModifyDBSnapshotAttribute", checksum = "8fb6e6ef")]
#[test_action("rds", "ModifyEventSubscription", checksum = "e63827e9")]
#[test_action("rds", "ModifyGlobalCluster", checksum = "e614d4b4")]
#[test_action("rds", "ModifyIntegration", checksum = "c7e426a4")]
#[test_action("rds", "ModifyOptionGroup", checksum = "4529b3ed")]
#[test_action("rds", "ModifyTenantDatabase", checksum = "d0bd1054")]
#[test_action("rds", "PromoteReadReplica", checksum = "79f0d115")]
#[test_action("rds", "PromoteReadReplicaDBCluster", checksum = "bec39eb3")]
#[test_action("rds", "PurchaseReservedDBInstancesOffering", checksum = "3d520b2d")]
#[test_action("rds", "RebootDBCluster", checksum = "e0fda2e3")]
#[test_action("rds", "RebootDBShardGroup", checksum = "6419015c")]
#[test_action("rds", "RegisterDBProxyTargets", checksum = "e94648e8")]
#[test_action("rds", "RemoveFromGlobalCluster", checksum = "9b058d5e")]
#[test_action("rds", "RemoveRoleFromDBCluster", checksum = "127ec672")]
#[test_action("rds", "RemoveRoleFromDBInstance", checksum = "c4453ee9")]
#[test_action("rds", "RemoveSourceIdentifierFromSubscription", checksum = "87bfbd5b")]
#[test_action("rds", "ResetDBClusterParameterGroup", checksum = "00807d36")]
#[test_action("rds", "ResetDBParameterGroup", checksum = "101c2d34")]
#[test_action("rds", "RestoreDBClusterFromS3", checksum = "02780721")]
#[test_action("rds", "RestoreDBClusterFromSnapshot", checksum = "72bb7914")]
#[test_action("rds", "RestoreDBClusterToPointInTime", checksum = "54d23948")]
#[test_action("rds", "RestoreDBInstanceFromS3", checksum = "3c75df14")]
#[test_action("rds", "RestoreDBInstanceToPointInTime", checksum = "ca7acfb3")]
#[test_action("rds", "RevokeDBSecurityGroupIngress", checksum = "226aa024")]
#[test_action("rds", "StartActivityStream", checksum = "816cf0b7")]
#[test_action("rds", "StartDBCluster", checksum = "8b22ce2b")]
#[test_action("rds", "StartDBInstance", checksum = "0a3a8d2a")]
#[test_action(
    "rds",
    "StartDBInstanceAutomatedBackupsReplication",
    checksum = "f68fd794"
)]
#[test_action("rds", "StartExportTask", checksum = "6f4b5684")]
#[test_action("rds", "StopActivityStream", checksum = "88048a83")]
#[test_action("rds", "StopDBCluster", checksum = "3731f027")]
#[test_action("rds", "StopDBInstance", checksum = "308c781d")]
#[test_action(
    "rds",
    "StopDBInstanceAutomatedBackupsReplication",
    checksum = "96fe2e0b"
)]
#[test_action("rds", "SwitchoverBlueGreenDeployment", checksum = "2f00439e")]
#[test_action("rds", "SwitchoverGlobalCluster", checksum = "5b3ca7b7")]
#[test_action("rds", "SwitchoverReadReplica", checksum = "0928d5b0")]
#[tokio::test]
async fn rds_closure_routes_exist() {
    // Every route added in this PR is exercised below. We assert HTTP 2xx
    // (route hit + handler succeeded). Each `#[test_action]` above pins
    // the operation to its Smithy checksum so the audit knows it has
    // coverage even when the test groups multiple ops together.
    let server = TestServer::start().await;

    // Clusters
    assert!(rds_post(
        &server,
        "CreateDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("Engine", "aurora-postgresql"),
            ("MasterUsername", "u"),
            ("MasterUserPassword", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBClusters", &[])
        .await
        .status()
        .is_success());
    assert!(
        rds_post(&server, "ModifyDBCluster", &[("DBClusterIdentifier", "c1")])
            .await
            .status()
            .is_success()
    );
    assert!(
        rds_post(&server, "RebootDBCluster", &[("DBClusterIdentifier", "c1")])
            .await
            .status()
            .is_success()
    );
    assert!(
        rds_post(&server, "StartDBCluster", &[("DBClusterIdentifier", "c1")])
            .await
            .status()
            .is_success()
    );
    assert!(
        rds_post(&server, "StopDBCluster", &[("DBClusterIdentifier", "c1")])
            .await
            .status()
            .is_success()
    );
    assert!(rds_post(
        &server,
        "FailoverDBCluster",
        &[("DBClusterIdentifier", "c1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "BacktrackDBCluster",
        &[
            ("DBClusterIdentifier", "c1"),
            ("BacktrackTo", "2026-01-01T00:00:00Z")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "PromoteReadReplicaDBCluster",
        &[("DBClusterIdentifier", "c1")]
    )
    .await
    .status()
    .is_success());
    assert!(
        rds_post(&server, "DeleteDBCluster", &[("DBClusterIdentifier", "c1")])
            .await
            .status()
            .is_success()
    );

    // Cluster snapshots + cluster automated backups + backtracks
    assert!(rds_post(
        &server,
        "CreateDBClusterSnapshot",
        &[
            ("DBClusterSnapshotIdentifier", "cs1"),
            ("DBClusterIdentifier", "c1")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBClusterSnapshots", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "CopyDBClusterSnapshot",
        &[
            ("SourceDBClusterSnapshotIdentifier", "cs1"),
            ("TargetDBClusterSnapshotIdentifier", "cs2")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DescribeDBClusterSnapshotAttributes",
        &[("DBClusterSnapshotIdentifier", "cs1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "ModifyDBClusterSnapshotAttribute",
        &[
            ("DBClusterSnapshotIdentifier", "cs1"),
            ("AttributeName", "restore")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBClusterAutomatedBackups", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "DeleteDBClusterAutomatedBackup",
        &[("DbClusterResourceId", "x")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DescribeDBClusterBacktracks",
        &[("DBClusterIdentifier", "c1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteDBClusterSnapshot",
        &[("DBClusterSnapshotIdentifier", "cs1")]
    )
    .await
    .status()
    .is_success());

    // Cluster parameter groups
    assert!(rds_post(
        &server,
        "CreateDBClusterParameterGroup",
        &[
            ("DBClusterParameterGroupName", "cpg1"),
            ("DBParameterGroupFamily", "aurora-postgresql15"),
            ("Description", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBClusterParameterGroups", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "DescribeDBClusterParameters",
        &[("DBClusterParameterGroupName", "cpg1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DescribeEngineDefaultClusterParameters",
        &[("DBParameterGroupFamily", "aurora-postgresql15")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "ModifyDBClusterParameterGroup",
        &[("DBClusterParameterGroupName", "cpg1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "ResetDBClusterParameterGroup",
        &[("DBClusterParameterGroupName", "cpg1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "CopyDBClusterParameterGroup",
        &[
            ("SourceDBClusterParameterGroupIdentifier", "cpg1"),
            ("TargetDBClusterParameterGroupIdentifier", "cpg2"),
            ("TargetDBClusterParameterGroupDescription", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteDBClusterParameterGroup",
        &[("DBClusterParameterGroupName", "cpg1")]
    )
    .await
    .status()
    .is_success());

    // Cluster endpoints
    assert!(rds_post(
        &server,
        "CreateDBClusterEndpoint",
        &[
            ("DBClusterEndpointIdentifier", "ce1"),
            ("DBClusterIdentifier", "c1"),
            ("EndpointType", "READER")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBClusterEndpoints", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", "ce1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteDBClusterEndpoint",
        &[("DBClusterEndpointIdentifier", "ce1")]
    )
    .await
    .status()
    .is_success());

    // Proxies + endpoints + targets
    assert!(rds_post(
        &server,
        "CreateDBProxy",
        &[("DBProxyName", "p1"), ("EngineFamily", "POSTGRESQL")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBProxies", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(&server, "ModifyDBProxy", &[("DBProxyName", "p1")])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "CreateDBProxyEndpoint",
        &[
            ("DBProxyName", "p1"),
            ("DBProxyEndpointName", "pe1"),
            ("VpcSubnetIds.member.1", "s1")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBProxyEndpoints", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyDBProxyEndpoint",
        &[("DBProxyEndpointName", "pe1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DescribeDBProxyTargetGroups",
        &[("DBProxyName", "p1")]
    )
    .await
    .status()
    .is_success());
    assert!(
        rds_post(&server, "DescribeDBProxyTargets", &[("DBProxyName", "p1")])
            .await
            .status()
            .is_success()
    );
    assert!(rds_post(
        &server,
        "ModifyDBProxyTargetGroup",
        &[("DBProxyName", "p1"), ("TargetGroupName", "default")]
    )
    .await
    .status()
    .is_success());
    assert!(
        rds_post(&server, "RegisterDBProxyTargets", &[("DBProxyName", "p1")])
            .await
            .status()
            .is_success()
    );
    assert!(rds_post(
        &server,
        "DeregisterDBProxyTargets",
        &[("DBProxyName", "p1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteDBProxyEndpoint",
        &[("DBProxyEndpointName", "pe1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DeleteDBProxy", &[("DBProxyName", "p1")])
        .await
        .status()
        .is_success());

    // Security groups
    assert!(rds_post(
        &server,
        "CreateDBSecurityGroup",
        &[
            ("DBSecurityGroupName", "sg1"),
            ("DBSecurityGroupDescription", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "AuthorizeDBSecurityGroupIngress",
        &[("DBSecurityGroupName", "sg1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RevokeDBSecurityGroupIngress",
        &[("DBSecurityGroupName", "sg1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBSecurityGroups", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "DeleteDBSecurityGroup",
        &[("DBSecurityGroupName", "sg1")]
    )
    .await
    .status()
    .is_success());

    // Option groups
    assert!(rds_post(
        &server,
        "CreateOptionGroup",
        &[
            ("OptionGroupName", "og1"),
            ("EngineName", "mysql"),
            ("MajorEngineVersion", "8.0"),
            ("OptionGroupDescription", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeOptionGroups", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "DescribeOptionGroupOptions",
        &[("EngineName", "mysql")]
    )
    .await
    .status()
    .is_success());
    assert!(
        rds_post(&server, "ModifyOptionGroup", &[("OptionGroupName", "og1")])
            .await
            .status()
            .is_success()
    );
    assert!(rds_post(
        &server,
        "CopyOptionGroup",
        &[
            ("SourceOptionGroupIdentifier", "og1"),
            ("TargetOptionGroupIdentifier", "og2"),
            ("TargetOptionGroupDescription", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(
        rds_post(&server, "DeleteOptionGroup", &[("OptionGroupName", "og1")])
            .await
            .status()
            .is_success()
    );

    // Event subscriptions
    assert!(rds_post(
        &server,
        "CreateEventSubscription",
        &[
            ("SubscriptionName", "es1"),
            ("SnsTopicArn", "arn:aws:sns:us-east-1:000:t")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeEventSubscriptions", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyEventSubscription",
        &[("SubscriptionName", "es1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "AddSourceIdentifierToSubscription",
        &[("SubscriptionName", "es1"), ("SourceIdentifier", "s1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RemoveSourceIdentifierFromSubscription",
        &[("SubscriptionName", "es1"), ("SourceIdentifier", "s1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteEventSubscription",
        &[("SubscriptionName", "es1")]
    )
    .await
    .status()
    .is_success());

    // Global clusters
    assert!(rds_post(
        &server,
        "CreateGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeGlobalClusters", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "FailoverGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "gc1"),
            ("TargetDbClusterIdentifier", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "SwitchoverGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "gc1"),
            ("TargetDbClusterIdentifier", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RemoveFromGlobalCluster",
        &[
            ("GlobalClusterIdentifier", "gc1"),
            ("DbClusterIdentifier", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteGlobalCluster",
        &[("GlobalClusterIdentifier", "gc1")]
    )
    .await
    .status()
    .is_success());

    // Integrations
    assert!(rds_post(
        &server,
        "CreateIntegration",
        &[
            ("IntegrationName", "i1"),
            ("SourceArn", "x"),
            ("TargetArn", "y")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeIntegrations", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyIntegration",
        &[("IntegrationIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteIntegration",
        &[("IntegrationIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());

    // Blue/Green
    assert!(rds_post(
        &server,
        "CreateBlueGreenDeployment",
        &[
            ("BlueGreenDeploymentName", "bg1"),
            ("Source", "arn:aws:rds:us-east-1:000:cluster:c1")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeBlueGreenDeployments", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "SwitchoverBlueGreenDeployment",
        &[("BlueGreenDeploymentIdentifier", "bg1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteBlueGreenDeployment",
        &[("BlueGreenDeploymentIdentifier", "bg1")]
    )
    .await
    .status()
    .is_success());

    // Shard groups
    assert!(rds_post(
        &server,
        "CreateDBShardGroup",
        &[
            ("DBShardGroupIdentifier", "sg1"),
            ("DBClusterIdentifier", "c1"),
            ("MaxACU", "1024")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBShardGroups", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyDBShardGroup",
        &[("DBShardGroupIdentifier", "sg1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RebootDBShardGroup",
        &[("DBShardGroupIdentifier", "sg1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteDBShardGroup",
        &[("DBShardGroupIdentifier", "sg1")]
    )
    .await
    .status()
    .is_success());

    // Custom engine versions
    assert!(rds_post(
        &server,
        "CreateCustomDBEngineVersion",
        &[("Engine", "custom-oracle-ee"), ("EngineVersion", "1.0")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "ModifyCustomDBEngineVersion",
        &[("Engine", "custom-oracle-ee"), ("EngineVersion", "1.0")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteCustomDBEngineVersion",
        &[("Engine", "custom-oracle-ee"), ("EngineVersion", "1.0")]
    )
    .await
    .status()
    .is_success());

    // Tenant DBs
    assert!(rds_post(
        &server,
        "CreateTenantDatabase",
        &[
            ("TenantDBName", "t1"),
            ("DBInstanceIdentifier", "i1"),
            ("MasterUsername", "u"),
            ("MasterUserPassword", "p")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeTenantDatabases", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyTenantDatabase",
        &[("TenantDBName", "t1"), ("DBInstanceIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBSnapshotTenantDatabases", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "DeleteTenantDatabase",
        &[("TenantDBName", "t1"), ("DBInstanceIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());

    // Export tasks
    assert!(rds_post(
        &server,
        "StartExportTask",
        &[
            ("ExportTaskIdentifier", "ex1"),
            ("SourceArn", "x"),
            ("S3BucketName", "b"),
            ("IamRoleArn", "r"),
            ("KmsKeyId", "k")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeExportTasks", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "CancelExportTask",
        &[("ExportTaskIdentifier", "ex1")]
    )
    .await
    .status()
    .is_success());

    // Activity stream
    assert!(rds_post(
        &server,
        "StartActivityStream",
        &[("ResourceArn", "x"), ("Mode", "sync"), ("KmsKeyId", "k")]
    )
    .await
    .status()
    .is_success());
    assert!(
        rds_post(&server, "ModifyActivityStream", &[("ResourceArn", "x")])
            .await
            .status()
            .is_success()
    );
    assert!(
        rds_post(&server, "StopActivityStream", &[("ResourceArn", "x")])
            .await
            .status()
            .is_success()
    );

    // Roles
    assert!(rds_post(
        &server,
        "AddRoleToDBCluster",
        &[("DBClusterIdentifier", "c1"), ("RoleArn", "r")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RemoveRoleFromDBCluster",
        &[("DBClusterIdentifier", "c1"), ("RoleArn", "r")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "AddRoleToDBInstance",
        &[
            ("DBInstanceIdentifier", "i1"),
            ("RoleArn", "r"),
            ("FeatureName", "S3")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RemoveRoleFromDBInstance",
        &[
            ("DBInstanceIdentifier", "i1"),
            ("RoleArn", "r"),
            ("FeatureName", "S3")
        ]
    )
    .await
    .status()
    .is_success());

    // Pending maintenance + reserved
    assert!(rds_post(
        &server,
        "ApplyPendingMaintenanceAction",
        &[
            ("ResourceIdentifier", "x"),
            ("ApplyAction", "system-update"),
            ("OptInType", "immediate")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribePendingMaintenanceActions", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "PurchaseReservedDBInstancesOffering",
        &[("ReservedDBInstancesOfferingId", "o1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeReservedDBInstances", &[])
        .await
        .status()
        .is_success());
    assert!(
        rds_post(&server, "DescribeReservedDBInstancesOfferings", &[])
            .await
            .status()
            .is_success()
    );

    // Snapshots / restores / parameters / engine defaults
    assert!(rds_post(
        &server,
        "CopyDBSnapshot",
        &[
            ("SourceDBSnapshotIdentifier", "s1"),
            ("TargetDBSnapshotIdentifier", "s2")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "CopyDBParameterGroup",
        &[
            ("SourceDBParameterGroupIdentifier", "p1"),
            ("TargetDBParameterGroupIdentifier", "p2"),
            ("TargetDBParameterGroupDescription", "x")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DescribeDBParameters",
        &[("DBParameterGroupName", "default")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "ResetDBParameterGroup",
        &[("DBParameterGroupName", "p1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DescribeEngineDefaultParameters",
        &[("DBParameterGroupFamily", "postgres15")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DescribeDBSnapshotAttributes",
        &[("DBSnapshotIdentifier", "s1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "ModifyDBSnapshot",
        &[("DBSnapshotIdentifier", "s1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "ModifyDBSnapshotAttribute",
        &[("DBSnapshotIdentifier", "s1"), ("AttributeName", "restore")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RestoreDBClusterFromS3",
        &[
            ("DBClusterIdentifier", "c2"),
            ("Engine", "aurora-mysql"),
            ("MasterUsername", "u"),
            ("MasterUserPassword", "p"),
            ("SourceEngine", "mysql"),
            ("SourceEngineVersion", "8.0"),
            ("S3BucketName", "b"),
            ("S3IngestionRoleArn", "r")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RestoreDBClusterFromSnapshot",
        &[
            ("DBClusterIdentifier", "c2"),
            ("SnapshotIdentifier", "s1"),
            ("Engine", "aurora-mysql")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RestoreDBClusterToPointInTime",
        &[
            ("DBClusterIdentifier", "c2"),
            ("SourceDBClusterIdentifier", "c1")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RestoreDBInstanceFromS3",
        &[
            ("DBInstanceIdentifier", "i2"),
            ("AllocatedStorage", "20"),
            ("DBInstanceClass", "db.t3.micro"),
            ("Engine", "mysql"),
            ("MasterUsername", "u"),
            ("MasterUserPassword", "p"),
            ("SourceEngine", "mysql"),
            ("SourceEngineVersion", "8.0"),
            ("S3BucketName", "b"),
            ("S3IngestionRoleArn", "r")
        ]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "RestoreDBInstanceToPointInTime",
        &[
            ("SourceDBInstanceIdentifier", "i1"),
            ("TargetDBInstanceIdentifier", "i2")
        ]
    )
    .await
    .status()
    .is_success());

    // Recommendations
    assert!(rds_post(&server, "DescribeDBRecommendations", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyDBRecommendation",
        &[("RecommendationId", "r1")]
    )
    .await
    .status()
    .is_success());

    // Certificates
    assert!(rds_post(&server, "DescribeCertificates", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "ModifyCertificates",
        &[("CertificateIdentifier", "rds-ca-2019")]
    )
    .await
    .status()
    .is_success());

    // Read replicas
    assert!(rds_post(
        &server,
        "PromoteReadReplica",
        &[("DBInstanceIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "StartDBInstance",
        &[("DBInstanceIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());
    assert!(
        rds_post(&server, "StopDBInstance", &[("DBInstanceIdentifier", "i1")])
            .await
            .status()
            .is_success()
    );
    assert!(rds_post(
        &server,
        "StartDBInstanceAutomatedBackupsReplication",
        &[("SourceDBInstanceArn", "arn:aws:rds:us-east-1:000:db:i1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "StopDBInstanceAutomatedBackupsReplication",
        &[("SourceDBInstanceArn", "arn:aws:rds:us-east-1:000:db:i1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DeleteDBInstanceAutomatedBackup",
        &[("DbiResourceId", "x")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBInstanceAutomatedBackups", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "SwitchoverReadReplica",
        &[("DBInstanceIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());

    // Account / events / regions / log files / capacity / http
    assert!(rds_post(&server, "DescribeAccountAttributes", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(&server, "DescribeEventCategories", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(&server, "DescribeEvents", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(&server, "DescribeSourceRegions", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "DescribeDBLogFiles",
        &[("DBInstanceIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "DownloadDBLogFilePortion",
        &[("DBInstanceIdentifier", "i1"), ("LogFileName", "log")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(&server, "DescribeDBMajorEngineVersions", &[])
        .await
        .status()
        .is_success());
    assert!(rds_post(
        &server,
        "DescribeValidDBInstanceModifications",
        &[("DBInstanceIdentifier", "i1")]
    )
    .await
    .status()
    .is_success());
    assert!(rds_post(
        &server,
        "ModifyCurrentDBClusterCapacity",
        &[("DBClusterIdentifier", "c1"), ("Capacity", "4")]
    )
    .await
    .status()
    .is_success());
    assert!(
        rds_post(&server, "DisableHttpEndpoint", &[("ResourceArn", "x")])
            .await
            .status()
            .is_success()
    );
    assert!(
        rds_post(&server, "EnableHttpEndpoint", &[("ResourceArn", "x")])
            .await
            .status()
            .is_success()
    );
}
