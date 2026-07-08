//! End-to-end tests for Amazon Pinpoint, driven through the real
//! `aws-sdk-pinpoint` client against a live fakecloud server. Exercises the
//! control plane end to end: create an app -> update APNS/GCM/SMS channels ->
//! create a segment -> create a campaign -> update an endpoint -> send messages
//! -> create a journey -> template CRUD -> tags.

use aws_sdk_pinpoint::types::{
    ApnsChannelRequest, CreateApplicationRequest, DirectMessageConfiguration, EmailTemplateRequest,
    EndpointRequest, GcmChannelRequest, MessageRequest, SmsChannelRequest, SmsMessage, TagsModel,
    WriteCampaignRequest, WriteJourneyRequest, WriteSegmentRequest,
};
use fakecloud_testkit::TestServer;

async fn pinpoint_client(server: &TestServer) -> aws_sdk_pinpoint::Client {
    let conf = aws_sdk_pinpoint::config::Builder::from(&server.aws_config().await).build();
    aws_sdk_pinpoint::Client::from_conf(conf)
}

#[tokio::test]
async fn pinpoint_full_lifecycle() {
    let server = TestServer::start().await;
    let client = pinpoint_client(&server).await;

    // --- Create app ---
    let created = client
        .create_app()
        .create_application_request(
            CreateApplicationRequest::builder()
                .name("e2e-pinpoint")
                .build(),
        )
        .send()
        .await
        .expect("create_app");
    let app = created
        .application_response()
        .expect("application_response");
    let app_id = app.id().unwrap_or_default().to_string();
    assert_eq!(app.name(), Some("e2e-pinpoint"));
    assert!(app.arn().unwrap_or_default().contains(":apps/"));

    // --- Update APNS / GCM / SMS channels ---
    let apns = client
        .update_apns_channel()
        .application_id(&app_id)
        .apns_channel_request(
            ApnsChannelRequest::builder()
                .enabled(true)
                .bundle_id("com.example")
                .build(),
        )
        .send()
        .await
        .expect("update_apns_channel");
    assert_eq!(
        apns.apns_channel_response().and_then(|c| c.platform()),
        Some("APNS")
    );

    client
        .update_gcm_channel()
        .application_id(&app_id)
        .gcm_channel_request(
            GcmChannelRequest::builder()
                .api_key("key")
                .enabled(true)
                .build(),
        )
        .send()
        .await
        .expect("update_gcm_channel");

    let sms = client
        .update_sms_channel()
        .application_id(&app_id)
        .sms_channel_request(SmsChannelRequest::builder().enabled(true).build())
        .send()
        .await
        .expect("update_sms_channel");
    assert_eq!(
        sms.sms_channel_response().and_then(|c| c.platform()),
        Some("SMS")
    );

    // --- Create a segment ---
    let segment = client
        .create_segment()
        .application_id(&app_id)
        .write_segment_request(WriteSegmentRequest::builder().name("all-users").build())
        .send()
        .await
        .expect("create_segment");
    let segment_id = segment
        .segment_response()
        .expect("segment_response")
        .id()
        .unwrap_or_default()
        .to_string();

    // --- Create a campaign referencing the segment ---
    let campaign = client
        .create_campaign()
        .application_id(&app_id)
        .write_campaign_request(
            WriteCampaignRequest::builder()
                .name("welcome")
                .segment_id(&segment_id)
                .build(),
        )
        .send()
        .await
        .expect("create_campaign");
    let campaign_id = campaign
        .campaign_response()
        .expect("campaign_response")
        .id()
        .unwrap_or_default()
        .to_string();

    // --- Update an endpoint ---
    client
        .update_endpoint()
        .application_id(&app_id)
        .endpoint_id("endpoint-1")
        .endpoint_request(
            EndpointRequest::builder()
                .address("device-token")
                .channel_type(aws_sdk_pinpoint::types::ChannelType::Gcm)
                .build(),
        )
        .send()
        .await
        .expect("update_endpoint");
    let endpoint = client
        .get_endpoint()
        .application_id(&app_id)
        .endpoint_id("endpoint-1")
        .send()
        .await
        .expect("get_endpoint");
    assert_eq!(
        endpoint.endpoint_response().and_then(|e| e.address()),
        Some("device-token")
    );

    // --- Send messages (no real delivery, but a structural response) ---
    client
        .send_messages()
        .application_id(&app_id)
        .message_request(
            MessageRequest::builder()
                .addresses(
                    "+15555550100",
                    aws_sdk_pinpoint::types::AddressConfiguration::builder()
                        .channel_type(aws_sdk_pinpoint::types::ChannelType::Sms)
                        .build(),
                )
                .message_configuration(
                    DirectMessageConfiguration::builder()
                        .sms_message(SmsMessage::builder().body("hello from fakecloud").build())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("send_messages");

    // --- Create a journey ---
    let journey = client
        .create_journey()
        .application_id(&app_id)
        .write_journey_request(WriteJourneyRequest::builder().name("onboarding").build())
        .send()
        .await
        .expect("create_journey");
    assert_eq!(
        journey
            .journey_response()
            .and_then(|j| j.state())
            .map(|s| s.as_str()),
        Some("DRAFT")
    );

    // --- Template CRUD ---
    client
        .create_email_template()
        .template_name("welcome-email")
        .email_template_request(EmailTemplateRequest::builder().subject("Welcome!").build())
        .send()
        .await
        .expect("create_email_template");
    let template = client
        .get_email_template()
        .template_name("welcome-email")
        .send()
        .await
        .expect("get_email_template");
    assert_eq!(
        template
            .email_template_response()
            .and_then(|t| t.template_name()),
        Some("welcome-email")
    );

    // --- Tags ---
    let arn = app.arn().unwrap_or_default().to_string();
    client
        .tag_resource()
        .resource_arn(&arn)
        .tags_model(TagsModel::builder().tags("team", "growth").build())
        .send()
        .await
        .expect("tag_resource");
    let tags = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list_tags_for_resource");
    assert_eq!(
        tags.tags_model()
            .and_then(|t| t.tags())
            .and_then(|m| m.get("team"))
            .map(String::as_str),
        Some("growth")
    );

    // --- Clean up campaign + app ---
    client
        .delete_campaign()
        .application_id(&app_id)
        .campaign_id(&campaign_id)
        .send()
        .await
        .expect("delete_campaign");
    client
        .delete_app()
        .application_id(&app_id)
        .send()
        .await
        .expect("delete_app");
    let after = client.get_app().application_id(&app_id).send().await;
    assert!(after.is_err(), "app should be gone after delete");
}
