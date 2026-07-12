//! End-to-end tests for the AWS IoT Wireless control plane, driven through the
//! real `aws-sdk-iotwireless` client against a live fakecloud server. Exercises
//! the destination / device-profile / FUOTA-task / tagging control plane end to
//! end: create a destination -> get / list / update / delete it (round-tripping
//! every field), mint a device profile (server-assigned id) and read it back,
//! create a FUOTA task and assert its server-generated `CreatedAt` timestamp
//! deserialises (restJson1 epoch-seconds), and tag / list-tags / untag a
//! resource by ARN.

use aws_sdk_iotwireless::types::{ExpressionType, Tag};
use fakecloud_testkit::TestServer;

async fn iotwireless_client(server: &TestServer) -> aws_sdk_iotwireless::Client {
    let conf = aws_sdk_iotwireless::config::Builder::from(&server.aws_config().await).build();
    aws_sdk_iotwireless::Client::from_conf(conf)
}

#[tokio::test]
async fn iotwireless_control_plane_lifecycle() {
    let server = TestServer::start().await;
    let client = iotwireless_client(&server).await;

    // --- Destination (name-addressed) ---
    let created = client
        .create_destination()
        .name("uplink-dest")
        .expression_type(ExpressionType::RuleName)
        .expression("my-rule")
        .role_arn("arn:aws:iam::000000000000:role/iotwireless")
        .description("original")
        .send()
        .await
        .expect("create_destination");
    let dest_arn = created.arn().expect("destination arn").to_string();
    assert!(dest_arn.contains(":Destination/uplink-dest"));
    assert_eq!(created.name(), Some("uplink-dest"));

    let got = client
        .get_destination()
        .name("uplink-dest")
        .send()
        .await
        .expect("get_destination");
    assert_eq!(got.name(), Some("uplink-dest"));
    assert_eq!(got.expression(), Some("my-rule"));
    assert_eq!(got.description(), Some("original"));

    let listed = client
        .list_destinations()
        .send()
        .await
        .expect("list_destinations");
    assert!(listed
        .destination_list()
        .iter()
        .any(|d| d.name() == Some("uplink-dest")));

    // Update round-trips the new description through the next GET.
    client
        .update_destination()
        .name("uplink-dest")
        .description("updated")
        .send()
        .await
        .expect("update_destination");
    let got = client
        .get_destination()
        .name("uplink-dest")
        .send()
        .await
        .expect("get_destination after update");
    assert_eq!(got.description(), Some("updated"));

    client
        .delete_destination()
        .name("uplink-dest")
        .send()
        .await
        .expect("delete_destination");
    assert!(client
        .get_destination()
        .name("uplink-dest")
        .send()
        .await
        .is_err());

    // --- Device profile (server-assigned id) ---
    let profile = client
        .create_device_profile()
        .name("profile-a")
        .send()
        .await
        .expect("create_device_profile");
    let profile_id = profile.id().expect("device profile id").to_string();
    assert!(!profile_id.is_empty());
    assert!(profile.arn().is_some());

    let got_profile = client
        .get_device_profile()
        .id(&profile_id)
        .send()
        .await
        .expect("get_device_profile");
    assert_eq!(got_profile.id(), Some(profile_id.as_str()));
    assert_eq!(got_profile.name(), Some("profile-a"));

    // --- FUOTA task: server-generated CreatedAt must deserialise as a
    // restJson1 epoch-seconds timestamp (an RFC3339 string would be rejected). ---
    let task = client
        .create_fuota_task()
        .firmware_update_image("s3://bucket/image.bin")
        .firmware_update_role("arn:aws:iam::000000000000:role/fuota")
        .send()
        .await
        .expect("create_fuota_task");
    let task_id = task.id().expect("fuota task id").to_string();
    let got_task = client
        .get_fuota_task()
        .id(&task_id)
        .send()
        .await
        .expect("get_fuota_task");
    let created_at = got_task.created_at().expect("fuota CreatedAt present");
    assert!(created_at.secs() > 1_600_000_000);

    // --- Tags (ARN-keyed) ---
    client
        .tag_resource()
        .resource_arn(&dest_arn)
        .tags(
            Tag::builder()
                .key("env")
                .value("prod")
                .build()
                .expect("build tag"),
        )
        .send()
        .await
        .expect("tag_resource");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&dest_arn)
        .send()
        .await
        .expect("list_tags_for_resource");
    assert!(tags
        .tags()
        .iter()
        .any(|t| t.key() == "env" && t.value() == "prod"));

    client
        .untag_resource()
        .resource_arn(&dest_arn)
        .tag_keys("env")
        .send()
        .await
        .expect("untag_resource");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&dest_arn)
        .send()
        .await
        .expect("list_tags_for_resource after untag");
    assert!(!tags.tags().iter().any(|t| t.key() == "env"));
}
