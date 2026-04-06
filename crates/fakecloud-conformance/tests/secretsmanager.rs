mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// -- Basic secret lifecycle --

#[test_action("secretsmanager", "CreateSecret", checksum = "cbecb465")]
#[test_action("secretsmanager", "DescribeSecret", checksum = "0174c78c")]
#[test_action("secretsmanager", "DeleteSecret", checksum = "cb942663")]
#[tokio::test]
async fn sm_create_describe_delete_secret() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    let create = client
        .create_secret()
        .name("conf/secret1")
        .secret_string("s3cret")
        .send()
        .await
        .unwrap();
    assert!(create.arn().is_some());

    let desc = client
        .describe_secret()
        .secret_id("conf/secret1")
        .send()
        .await
        .unwrap();
    assert_eq!(desc.name().unwrap(), "conf/secret1");

    client
        .delete_secret()
        .secret_id("conf/secret1")
        .force_delete_without_recovery(true)
        .send()
        .await
        .unwrap();
}

// -- Get / Put secret value --

#[test_action("secretsmanager", "GetSecretValue", checksum = "62d26559")]
#[test_action("secretsmanager", "PutSecretValue", checksum = "303f646a")]
#[tokio::test]
async fn sm_get_put_secret_value() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/getput")
        .secret_string("initial")
        .send()
        .await
        .unwrap();

    client
        .put_secret_value()
        .secret_id("conf/getput")
        .secret_string("updated")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_secret_value()
        .secret_id("conf/getput")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.secret_string().unwrap(), "updated");
}

// -- Update secret --

#[test_action("secretsmanager", "UpdateSecret", checksum = "cef3ebb2")]
#[tokio::test]
async fn sm_update_secret() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/update")
        .secret_string("v1")
        .send()
        .await
        .unwrap();
    client
        .update_secret()
        .secret_id("conf/update")
        .description("updated description")
        .send()
        .await
        .unwrap();
}

// -- List secrets --

#[test_action("secretsmanager", "ListSecrets", checksum = "4a9247e4")]
#[tokio::test]
async fn sm_list_secrets() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/list1")
        .secret_string("x")
        .send()
        .await
        .unwrap();
    let resp = client.list_secrets().send().await.unwrap();
    assert!(!resp.secret_list().is_empty());
}

// -- Restore secret --

#[test_action("secretsmanager", "RestoreSecret", checksum = "2bd0c57c")]
#[tokio::test]
async fn sm_restore_secret() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/restore")
        .secret_string("x")
        .send()
        .await
        .unwrap();
    client
        .delete_secret()
        .secret_id("conf/restore")
        .send()
        .await
        .unwrap();
    client
        .restore_secret()
        .secret_id("conf/restore")
        .send()
        .await
        .unwrap();
}

// -- Tags --

#[test_action("secretsmanager", "TagResource", checksum = "c6ae0114")]
#[test_action("secretsmanager", "UntagResource", checksum = "50c098a5")]
#[tokio::test]
async fn sm_tag_untag_resource() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    let create = client
        .create_secret()
        .name("conf/tags")
        .secret_string("x")
        .send()
        .await
        .unwrap();
    let arn = create.arn().unwrap().to_string();

    client
        .tag_resource()
        .secret_id(&arn)
        .tags(
            aws_sdk_secretsmanager::types::Tag::builder()
                .key("env")
                .value("test")
                .build(),
        )
        .send()
        .await
        .unwrap();

    client
        .untag_resource()
        .secret_id(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();
}

// -- Version IDs --

#[test_action("secretsmanager", "ListSecretVersionIds", checksum = "1121a7d4")]
#[tokio::test]
async fn sm_list_secret_version_ids() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/versions")
        .secret_string("x")
        .send()
        .await
        .unwrap();
    let resp = client
        .list_secret_version_ids()
        .secret_id("conf/versions")
        .send()
        .await
        .unwrap();
    assert!(!resp.versions().is_empty());
}

// -- Random password --

#[test_action("secretsmanager", "GetRandomPassword", checksum = "8b24f8b9")]
#[tokio::test]
async fn sm_get_random_password() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    let resp = client
        .get_random_password()
        .password_length(32)
        .send()
        .await
        .unwrap();
    assert!(resp.random_password().is_some());
}

// -- Rotation --

#[test_action("secretsmanager", "RotateSecret", checksum = "4e2d1c9f")]
#[test_action("secretsmanager", "CancelRotateSecret", checksum = "33ed389c")]
#[tokio::test]
async fn sm_rotate_cancel_rotate() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/rotate")
        .secret_string("x")
        .send()
        .await
        .unwrap();
    let _ = client.rotate_secret().secret_id("conf/rotate").send().await;
    let _ = client
        .cancel_rotate_secret()
        .secret_id("conf/rotate")
        .send()
        .await;
}

// -- Version stage --

#[test_action("secretsmanager", "UpdateSecretVersionStage", checksum = "b7be3e8e")]
#[tokio::test]
async fn sm_update_secret_version_stage() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    let create = client
        .create_secret()
        .name("conf/stage")
        .secret_string("x")
        .send()
        .await
        .unwrap();
    let version_id = create.version_id().unwrap().to_string();
    let _ = client
        .update_secret_version_stage()
        .secret_id("conf/stage")
        .version_stage("AWSCURRENT")
        .move_to_version_id(&version_id)
        .send()
        .await;
}

// -- Batch get --

#[test_action("secretsmanager", "BatchGetSecretValue", checksum = "9948f85b")]
#[tokio::test]
async fn sm_batch_get_secret_value() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/batch1")
        .secret_string("v1")
        .send()
        .await
        .unwrap();
    let resp = client
        .batch_get_secret_value()
        .secret_id_list("conf/batch1")
        .send()
        .await
        .unwrap();
    assert!(!resp.secret_values().is_empty());
}

// -- Resource policy --

#[test_action("secretsmanager", "PutResourcePolicy", checksum = "7ea18f4d")]
#[test_action("secretsmanager", "GetResourcePolicy", checksum = "d59157c6")]
#[test_action("secretsmanager", "DeleteResourcePolicy", checksum = "9be6ed12")]
#[tokio::test]
async fn sm_resource_policy() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/policy")
        .secret_string("x")
        .send()
        .await
        .unwrap();

    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"secretsmanager:GetSecretValue","Resource":"*"}]}"#;
    client
        .put_resource_policy()
        .secret_id("conf/policy")
        .resource_policy(policy)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_resource_policy()
        .secret_id("conf/policy")
        .send()
        .await
        .unwrap();
    assert!(resp.resource_policy().is_some());

    client
        .delete_resource_policy()
        .secret_id("conf/policy")
        .send()
        .await
        .unwrap();
}

// -- Validate resource policy --

#[test_action("secretsmanager", "ValidateResourcePolicy", checksum = "e3cfeb7e")]
#[tokio::test]
async fn sm_validate_resource_policy() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"secretsmanager:GetSecretValue","Resource":"*"}]}"#;
    let _ = client
        .validate_resource_policy()
        .resource_policy(policy)
        .send()
        .await;
}

// -- Replication --

#[test_action("secretsmanager", "ReplicateSecretToRegions", checksum = "c0347eac")]
#[test_action(
    "secretsmanager",
    "RemoveRegionsFromReplication",
    checksum = "afa944b1"
)]
#[test_action("secretsmanager", "StopReplicationToReplica", checksum = "c4f70e74")]
#[tokio::test]
async fn sm_replication() {
    let server = TestServer::start().await;
    let client = server.secretsmanager_client().await;
    client
        .create_secret()
        .name("conf/replicate")
        .secret_string("x")
        .send()
        .await
        .unwrap();

    let _ = client
        .replicate_secret_to_regions()
        .secret_id("conf/replicate")
        .add_replica_regions(
            aws_sdk_secretsmanager::types::ReplicaRegionType::builder()
                .region("eu-west-1")
                .build(),
        )
        .send()
        .await;

    let _ = client
        .remove_regions_from_replication()
        .secret_id("conf/replicate")
        .remove_replica_regions("eu-west-1")
        .send()
        .await;

    let _ = client
        .stop_replication_to_replica()
        .secret_id("conf/replicate")
        .send()
        .await;
}
