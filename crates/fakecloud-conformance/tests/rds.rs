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
