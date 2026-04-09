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
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].engine(), Some("postgres"));
    assert_eq!(versions[0].engine_version(), Some("16.3"));
    assert_eq!(versions[0].db_parameter_group_family(), Some("postgres16"));
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
    assert_eq!(options.len(), 1);
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
