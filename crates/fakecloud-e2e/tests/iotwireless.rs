//! End-to-end tests for the AWS IoT Wireless control plane, driven through the
//! real `aws-sdk-iotwireless` client against a live fakecloud server. Exercises
//! the destination / device-profile / FUOTA-task / tagging control plane end to
//! end: create a destination -> get / list / update / delete it (round-tripping
//! every field), mint a device profile (server-assigned id) and read it back,
//! create a FUOTA task and assert its server-generated `CreatedAt` timestamp
//! deserialises (restJson1 epoch-seconds), and tag / list-tags / untag a
//! resource by ARN.

use aws_sdk_iotwireless::types::{
    ExpressionType, LogLevel, SummaryMetricConfiguration, SummaryMetricConfigurationStatus, Tag,
};
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

/// Round-trips the operations that previously accepted-and-discarded their
/// inputs: per-resource log levels, account-scoped singleton configs,
/// association membership edges, and the minted `MessageId` of a downlink.
#[tokio::test]
async fn iotwireless_stub_fixes_round_trip() {
    let server = TestServer::start().await;
    let client = iotwireless_client(&server).await;

    // --- Per-resource log level: Put -> Get, Reset -> 404 (finding 1) ---
    client
        .put_resource_log_level()
        .resource_identifier("dev-log-1")
        .resource_type("WirelessDevice")
        .log_level(LogLevel::Error)
        .send()
        .await
        .expect("put_resource_log_level");
    let got = client
        .get_resource_log_level()
        .resource_identifier("dev-log-1")
        .resource_type("WirelessDevice")
        .send()
        .await
        .expect("get_resource_log_level");
    assert_eq!(got.log_level(), Some(&LogLevel::Error));

    client
        .reset_resource_log_level()
        .resource_identifier("dev-log-1")
        .resource_type("WirelessDevice")
        .send()
        .await
        .expect("reset_resource_log_level");
    assert!(client
        .get_resource_log_level()
        .resource_identifier("dev-log-1")
        .resource_type("WirelessDevice")
        .send()
        .await
        .is_err());

    // --- Singleton metric configuration: Update -> Get (finding 2) ---
    client
        .update_metric_configuration()
        .summary_metric(
            SummaryMetricConfiguration::builder()
                .status(SummaryMetricConfigurationStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("update_metric_configuration");
    let cfg = client
        .get_metric_configuration()
        .send()
        .await
        .expect("get_metric_configuration");
    assert_eq!(
        cfg.summary_metric().and_then(|m| m.status()),
        Some(&SummaryMetricConfigurationStatus::Enabled)
    );

    // --- Association edge: Associate -> List -> Disassociate (finding 4) ---
    let task = client
        .create_fuota_task()
        .firmware_update_image("s3://bucket/image.bin")
        .firmware_update_role("arn:aws:iam::000000000000:role/fuota")
        .send()
        .await
        .expect("create_fuota_task");
    let task_id = task.id().expect("fuota task id").to_string();

    client
        .associate_multicast_group_with_fuota_task()
        .id(&task_id)
        .multicast_group_id("mc-assoc-1")
        .send()
        .await
        .expect("associate_multicast_group_with_fuota_task");
    let listed = client
        .list_multicast_groups_by_fuota_task()
        .id(&task_id)
        .send()
        .await
        .expect("list_multicast_groups_by_fuota_task");
    assert!(listed
        .multicast_group_list()
        .iter()
        .any(|g| g.id() == Some("mc-assoc-1")));

    client
        .disassociate_multicast_group_from_fuota_task()
        .id(&task_id)
        .multicast_group_id("mc-assoc-1")
        .send()
        .await
        .expect("disassociate_multicast_group_from_fuota_task");
    let listed = client
        .list_multicast_groups_by_fuota_task()
        .id(&task_id)
        .send()
        .await
        .expect("list_multicast_groups_by_fuota_task after disassociate");
    assert!(!listed
        .multicast_group_list()
        .iter()
        .any(|g| g.id() == Some("mc-assoc-1")));

    // --- SendDataToWirelessDevice mints a MessageId (finding 3) ---
    let sent = client
        .send_data_to_wireless_device()
        .id("dev-downlink-1")
        .transmit_mode(1)
        .payload_data("aGVsbG8=")
        .send()
        .await
        .expect("send_data_to_wireless_device");
    assert!(sent.message_id().is_some_and(|m| !m.is_empty()));
}

#[tokio::test]
async fn iotwireless_gateway_thing_and_certificate_associations() {
    // Bug-hunt 1.22: Associate*WithThing / *WithCertificate were accept-and-
    // discard no-ops. Associations must round-trip through the reads.
    let server = TestServer::start().await;
    let client = iotwireless_client(&server).await;

    let gw = client
        .create_wireless_gateway()
        .lo_ra_wan(
            aws_sdk_iotwireless::types::LoRaWanGateway::builder()
                .gateway_eui("0000000000000001")
                .rf_region("US915")
                .build(),
        )
        .send()
        .await
        .expect("create_wireless_gateway");
    let gw_id = gw.id().expect("gateway id").to_string();

    // Associate with an IoT thing.
    let thing_arn = "arn:aws:iot:us-east-1:000000000000:thing/gw-thing";
    client
        .associate_wireless_gateway_with_thing()
        .id(&gw_id)
        .thing_arn(thing_arn)
        .send()
        .await
        .expect("associate_wireless_gateway_with_thing");
    let got = client
        .get_wireless_gateway()
        .identifier(&gw_id)
        .identifier_type(aws_sdk_iotwireless::types::WirelessGatewayIdType::WirelessGatewayId)
        .send()
        .await
        .expect("get_wireless_gateway");
    assert_eq!(got.thing_arn(), Some(thing_arn));
    assert_eq!(got.thing_name(), Some("gw-thing"));

    // Associate a certificate.
    client
        .associate_wireless_gateway_with_certificate()
        .id(&gw_id)
        .iot_certificate_id("cert-abc123")
        .send()
        .await
        .expect("associate_wireless_gateway_with_certificate");
    let cert = client
        .get_wireless_gateway_certificate()
        .id(&gw_id)
        .send()
        .await
        .expect("get_wireless_gateway_certificate");
    assert_eq!(cert.iot_certificate_id(), Some("cert-abc123"));

    // Disassociate the thing clears it.
    client
        .disassociate_wireless_gateway_from_thing()
        .id(&gw_id)
        .send()
        .await
        .expect("disassociate_wireless_gateway_from_thing");
    let got = client
        .get_wireless_gateway()
        .identifier(&gw_id)
        .identifier_type(aws_sdk_iotwireless::types::WirelessGatewayIdType::WirelessGatewayId)
        .send()
        .await
        .expect("get after disassociate");
    assert!(got.thing_arn().is_none() || got.thing_arn() == Some(""));
}

#[tokio::test]
async fn iotwireless_partner_account_association() {
    // Bug-hunt 1.22: AssociateAwsAccountWithPartnerAccount persisted nothing.
    let server = TestServer::start().await;
    let client = iotwireless_client(&server).await;

    client
        .associate_aws_account_with_partner_account()
        .sidewalk(
            aws_sdk_iotwireless::types::SidewalkAccountInfo::builder()
                .amazon_id("amzn-partner-1")
                .app_server_private_key("0123456789abcdef")
                .build(),
        )
        .send()
        .await
        .expect("associate_aws_account_with_partner_account");

    let list = client
        .list_partner_accounts()
        .send()
        .await
        .expect("list_partner_accounts");
    assert!(
        list.sidewalk()
            .iter()
            .any(|s| s.amazon_id() == Some("amzn-partner-1")),
        "partner account must be listed"
    );
}
