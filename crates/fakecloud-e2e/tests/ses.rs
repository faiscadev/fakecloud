mod helpers;

use aws_sdk_sesv2::types::{
    BehaviorOnMxFailure, Body, Content, Destination, DkimSigningAttributes,
    DkimSigningAttributesOrigin, EmailContent, EmailTemplateContent, EventDestinationDefinition,
    EventType, HttpsPolicy, Message, RawMessage, SnsDestination, SubscriptionStatus,
    SuppressionListReason, Tag, Template, TlsPolicy, Topic, TopicPreference,
};
use helpers::TestServer;

#[tokio::test]
async fn ses_identity_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create email identity
    let resp = client
        .create_email_identity()
        .email_identity("test@example.com")
        .send()
        .await
        .unwrap();
    assert!(resp.verified_for_sending_status());
    assert_eq!(resp.identity_type().unwrap().as_str(), "EMAIL_ADDRESS");

    // Create domain identity
    client
        .create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();

    // List identities
    let list = client.list_email_identities().send().await.unwrap();
    assert_eq!(list.email_identities().len(), 2);

    // Get identity — verify auto-verified
    let get = client
        .get_email_identity()
        .email_identity("test@example.com")
        .send()
        .await
        .unwrap();
    assert!(get.verified_for_sending_status());
    let dkim = get.dkim_attributes().unwrap();
    assert_eq!(dkim.status().unwrap().as_str(), "SUCCESS");

    // Delete identity
    client
        .delete_email_identity()
        .email_identity("test@example.com")
        .send()
        .await
        .unwrap();

    // Confirm gone
    let list = client.list_email_identities().send().await.unwrap();
    assert_eq!(list.email_identities().len(), 1);
}

#[tokio::test]
async fn ses_template_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create template
    client
        .create_email_template()
        .template_name("welcome")
        .template_content(
            EmailTemplateContent::builder()
                .subject("Welcome {{name}}")
                .html("<h1>Hello {{name}}</h1>")
                .text("Hello {{name}}")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Get template
    let get = client
        .get_email_template()
        .template_name("welcome")
        .send()
        .await
        .unwrap();
    assert_eq!(get.template_name(), "welcome");
    let content = get.template_content().unwrap();
    assert_eq!(content.subject().unwrap(), "Welcome {{name}}");
    assert_eq!(content.html().unwrap(), "<h1>Hello {{name}}</h1>");

    // Update template
    client
        .update_email_template()
        .template_name("welcome")
        .template_content(
            EmailTemplateContent::builder()
                .subject("Updated Welcome {{name}}")
                .html("<h1>Updated Hello {{name}}</h1>")
                .text("Updated Hello {{name}}")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Verify update
    let get = client
        .get_email_template()
        .template_name("welcome")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.template_content().unwrap().subject().unwrap(),
        "Updated Welcome {{name}}"
    );

    // List templates
    let list = client.list_email_templates().send().await.unwrap();
    assert_eq!(list.templates_metadata().len(), 1);

    // Delete template
    client
        .delete_email_template()
        .template_name("welcome")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let list = client.list_email_templates().send().await.unwrap();
    assert!(list.templates_metadata().is_empty());
}

#[tokio::test]
async fn ses_configuration_set_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create
    client
        .create_configuration_set()
        .configuration_set_name("my-config")
        .send()
        .await
        .unwrap();

    // Get
    let get = client
        .get_configuration_set()
        .configuration_set_name("my-config")
        .send()
        .await
        .unwrap();
    assert_eq!(get.configuration_set_name(), Some("my-config"));

    // List
    let list = client.list_configuration_sets().send().await.unwrap();
    assert_eq!(list.configuration_sets().len(), 1);

    // Delete
    client
        .delete_configuration_set()
        .configuration_set_name("my-config")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let list = client.list_configuration_sets().send().await.unwrap();
    assert!(list.configuration_sets().is_empty());
}

#[tokio::test]
async fn ses_send_email_simple() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create identity first
    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

    // Send email with simple content
    let resp = client
        .send_email()
        .from_email_address("sender@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("Test Subject").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("Hello world").build().unwrap())
                                .html(
                                    Content::builder()
                                        .data("<p>Hello world</p>")
                                        .build()
                                        .unwrap(),
                                )
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let message_id = resp.message_id().unwrap();
    assert!(!message_id.is_empty());
}

#[tokio::test]
async fn ses_send_email_template() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create identity
    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

    // Create template
    client
        .create_email_template()
        .template_name("greet")
        .template_content(
            EmailTemplateContent::builder()
                .subject("Hello {{name}}")
                .text("Hi {{name}}")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Send email referencing template
    let resp = client
        .send_email()
        .from_email_address("sender@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .template(
                    Template::builder()
                        .template_name("greet")
                        .template_data(r#"{"name":"World"}"#)
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert!(!resp.message_id().unwrap().is_empty());
}

#[tokio::test]
async fn ses_send_email_raw() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create identity
    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

    let raw_message = "From: sender@example.com\r\nTo: recipient@example.com\r\nSubject: Raw Test\r\n\r\nRaw body";

    let resp = client
        .send_email()
        .from_email_address("sender@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .raw(
                    RawMessage::builder()
                        .data(aws_sdk_sesv2::primitives::Blob::new(raw_message.as_bytes()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert!(!resp.message_id().unwrap().is_empty());
}

#[tokio::test]
async fn ses_get_account() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    let resp = client.get_account().send().await.unwrap();
    assert!(resp.sending_enabled());
    let quota = resp.send_quota().unwrap();
    assert!(quota.max24_hour_send() > 0.0);
    assert!(quota.max_send_rate() > 0.0);
}

#[tokio::test]
async fn ses_error_get_nonexistent_identity() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    let result = client
        .get_email_identity()
        .email_identity("nonexistent@example.com")
        .send()
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    let service_err = err.as_service_error().unwrap();
    assert!(service_err.is_not_found_exception());
}

#[tokio::test]
async fn ses_error_create_duplicate_identity() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("dup@example.com")
        .send()
        .await
        .unwrap();

    let result = client
        .create_email_identity()
        .email_identity("dup@example.com")
        .send()
        .await;

    assert!(result.is_err());
    let err = result.unwrap_err();
    let service_err = err.as_service_error().unwrap();
    assert!(service_err.is_already_exists_exception());
}

#[tokio::test]
async fn ses_contact_list_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create contact list with topics
    client
        .create_contact_list()
        .contact_list_name("my-list")
        .description("A test list")
        .topics(
            Topic::builder()
                .topic_name("newsletters")
                .display_name("Newsletters")
                .description("Weekly newsletters")
                .default_subscription_status(SubscriptionStatus::OptIn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Get contact list
    let get = client
        .get_contact_list()
        .contact_list_name("my-list")
        .send()
        .await
        .unwrap();
    assert_eq!(get.contact_list_name(), Some("my-list"));
    assert_eq!(get.description(), Some("A test list"));
    let topics = get.topics();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].topic_name(), "newsletters");
    assert_eq!(
        topics[0].default_subscription_status(),
        &SubscriptionStatus::OptIn
    );

    // List contact lists
    let list = client.list_contact_lists().send().await.unwrap();
    assert_eq!(list.contact_lists().len(), 1);
    assert_eq!(list.contact_lists()[0].contact_list_name(), Some("my-list"));

    // Update contact list
    client
        .update_contact_list()
        .contact_list_name("my-list")
        .description("Updated description")
        .topics(
            Topic::builder()
                .topic_name("promotions")
                .display_name("Promotions")
                .description("Promo emails")
                .default_subscription_status(SubscriptionStatus::OptOut)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Verify update
    let get = client
        .get_contact_list()
        .contact_list_name("my-list")
        .send()
        .await
        .unwrap();
    assert_eq!(get.description(), Some("Updated description"));
    assert_eq!(get.topics().len(), 1);
    assert_eq!(get.topics()[0].topic_name(), "promotions");

    // Delete contact list
    client
        .delete_contact_list()
        .contact_list_name("my-list")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client
        .get_contact_list()
        .contact_list_name("my-list")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn ses_contact_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create contact list with a topic
    client
        .create_contact_list()
        .contact_list_name("my-list")
        .topics(
            Topic::builder()
                .topic_name("newsletters")
                .display_name("Newsletters")
                .description("Weekly newsletters")
                .default_subscription_status(SubscriptionStatus::OptOut)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Create contact with topic preferences
    client
        .create_contact()
        .contact_list_name("my-list")
        .email_address("user@example.com")
        .topic_preferences(
            TopicPreference::builder()
                .topic_name("newsletters")
                .subscription_status(SubscriptionStatus::OptIn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Get contact
    let get = client
        .get_contact()
        .contact_list_name("my-list")
        .email_address("user@example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(get.email_address(), Some("user@example.com"));
    assert_eq!(get.contact_list_name(), Some("my-list"));
    assert!(!get.unsubscribe_all());
    let prefs = get.topic_preferences();
    assert_eq!(prefs.len(), 1);
    assert_eq!(prefs[0].topic_name(), "newsletters");
    assert_eq!(prefs[0].subscription_status(), &SubscriptionStatus::OptIn);
    // Check topic defaults
    let defaults = get.topic_default_preferences();
    assert_eq!(defaults.len(), 1);
    assert_eq!(
        defaults[0].subscription_status(),
        &SubscriptionStatus::OptOut
    );

    // List contacts
    let list = client
        .list_contacts()
        .contact_list_name("my-list")
        .send()
        .await
        .unwrap();
    assert_eq!(list.contacts().len(), 1);

    // Update contact
    client
        .update_contact()
        .contact_list_name("my-list")
        .email_address("user@example.com")
        .unsubscribe_all(true)
        .send()
        .await
        .unwrap();

    // Verify update
    let get = client
        .get_contact()
        .contact_list_name("my-list")
        .email_address("user@example.com")
        .send()
        .await
        .unwrap();
    assert!(get.unsubscribe_all());

    // Delete contact
    client
        .delete_contact()
        .contact_list_name("my-list")
        .email_address("user@example.com")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client
        .get_contact()
        .contact_list_name("my-list")
        .email_address("user@example.com")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn ses_error_duplicate_contact_list() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_contact_list()
        .contact_list_name("dup-list")
        .send()
        .await
        .unwrap();

    let result = client
        .create_contact_list()
        .contact_list_name("dup-list")
        .send()
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let service_err = err.as_service_error().unwrap();
    assert!(service_err.is_already_exists_exception());
}

#[tokio::test]
async fn ses_error_contact_in_nonexistent_list() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    let result = client
        .create_contact()
        .contact_list_name("nonexistent")
        .email_address("user@example.com")
        .send()
        .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    let service_err = err.as_service_error().unwrap();
    assert!(service_err.is_not_found_exception());
}

#[tokio::test]
async fn ses_introspection_endpoint() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create identity and send email
    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

    client
        .send_email()
        .from_email_address("sender@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(
                            Content::builder()
                                .data("Introspection Test")
                                .build()
                                .unwrap(),
                        )
                        .body(
                            Body::builder()
                                .text(Content::builder().data("body text").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Query introspection endpoint
    let url = format!("{}/_fakecloud/ses/emails", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let emails = resp["emails"].as_array().unwrap();
    assert_eq!(emails.len(), 1);
    assert_eq!(emails[0]["from"], "sender@example.com");
    assert_eq!(emails[0]["to"][0], "recipient@example.com");
    assert_eq!(emails[0]["subject"], "Introspection Test");
    assert!(!emails[0]["messageId"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn ses_tag_identity() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create identity
    client
        .create_email_identity()
        .email_identity("tag-test@example.com")
        .send()
        .await
        .unwrap();

    let arn = "arn:aws:ses:us-east-1:000000000000:identity/tag-test@example.com".to_string();

    // Tag it
    client
        .tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .tags(Tag::builder().key("team").value("backend").build().unwrap())
        .send()
        .await
        .unwrap();

    // List tags
    let resp = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags = resp.tags();
    assert_eq!(tags.len(), 2);

    // Untag
    client
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    let tags = resp.tags();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), "team");
    assert_eq!(tags[0].value(), "backend");
}

#[tokio::test]
async fn ses_tag_configuration_set() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_configuration_set()
        .configuration_set_name("tagged-config")
        .send()
        .await
        .unwrap();

    let arn = "arn:aws:ses:us-east-1:000000000000:configuration-set/tagged-config";

    client
        .tag_resource()
        .resource_arn(arn)
        .tags(
            Tag::builder()
                .key("project")
                .value("fakecloud")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().len(), 1);
    assert_eq!(resp.tags()[0].key(), "project");

    // Delete config set and verify tags are cleaned up
    client
        .delete_configuration_set()
        .configuration_set_name("tagged-config")
        .send()
        .await
        .unwrap();

    // Listing tags for a deleted resource should fail
    let err = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn ses_delete_identity_removes_tags() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("deleteme@example.com")
        .send()
        .await
        .unwrap();

    let arn = "arn:aws:ses:us-east-1:000000000000:identity/deleteme@example.com";

    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("k").value("v").build().unwrap())
        .send()
        .await
        .unwrap();

    client
        .delete_email_identity()
        .email_identity("deleteme@example.com")
        .send()
        .await
        .unwrap();

    let err = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn ses_untag_multiple_keys() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("multi@example.com")
        .send()
        .await
        .unwrap();

    let arn = "arn:aws:ses:us-east-1:000000000000:identity/multi@example.com";

    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("a").value("1").build().unwrap())
        .tags(Tag::builder().key("b").value("2").build().unwrap())
        .tags(Tag::builder().key("c").value("3").build().unwrap())
        .send()
        .await
        .unwrap();

    // Remove two keys at once
    client
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("a")
        .tag_keys("c")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    let tags = resp.tags();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), "b");
    assert_eq!(tags[0].value(), "2");
}

// --- Suppression List ---

#[tokio::test]
async fn ses_suppression_list_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Put suppressed destination
    client
        .put_suppressed_destination()
        .email_address("bounce@example.com")
        .reason(SuppressionListReason::Bounce)
        .send()
        .await
        .unwrap();

    // Get suppressed destination
    let get = client
        .get_suppressed_destination()
        .email_address("bounce@example.com")
        .send()
        .await
        .unwrap();
    let dest = get.suppressed_destination().unwrap();
    assert_eq!(dest.email_address(), "bounce@example.com");
    assert_eq!(dest.reason(), &SuppressionListReason::Bounce);
    // last_update_time is a required field; verify it's a positive epoch
    assert!(dest.last_update_time().secs() > 0);

    // Put another with COMPLAINT
    client
        .put_suppressed_destination()
        .email_address("complaint@example.com")
        .reason(SuppressionListReason::Complaint)
        .send()
        .await
        .unwrap();

    // List suppressed destinations
    let list = client.list_suppressed_destinations().send().await.unwrap();
    assert_eq!(list.suppressed_destination_summaries().len(), 2);

    // Delete suppressed destination
    client
        .delete_suppressed_destination()
        .email_address("bounce@example.com")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client
        .get_suppressed_destination()
        .email_address("bounce@example.com")
        .send()
        .await;
    assert!(result.is_err());

    // List should have 1
    let list = client.list_suppressed_destinations().send().await.unwrap();
    assert_eq!(list.suppressed_destination_summaries().len(), 1);
}

// --- Event Destinations ---

#[tokio::test]
async fn ses_event_destination_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create config set
    client
        .create_configuration_set()
        .configuration_set_name("evt-config")
        .send()
        .await
        .unwrap();

    // Create event destination
    client
        .create_configuration_set_event_destination()
        .configuration_set_name("evt-config")
        .event_destination_name("my-sns-dest")
        .event_destination(
            EventDestinationDefinition::builder()
                .enabled(true)
                .matching_event_types(EventType::Send)
                .matching_event_types(EventType::Bounce)
                .sns_destination(
                    SnsDestination::builder()
                        .topic_arn("arn:aws:sns:us-east-1:123456789012:my-topic")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Get event destinations
    let get = client
        .get_configuration_set_event_destinations()
        .configuration_set_name("evt-config")
        .send()
        .await
        .unwrap();
    let dests = get.event_destinations();
    assert_eq!(dests.len(), 1);
    assert_eq!(dests[0].name(), "my-sns-dest");
    assert!(dests[0].enabled());
    assert_eq!(dests[0].matching_event_types().len(), 2);
    assert!(dests[0].sns_destination().is_some());
    assert_eq!(
        dests[0].sns_destination().unwrap().topic_arn(),
        "arn:aws:sns:us-east-1:123456789012:my-topic"
    );

    // Update event destination
    client
        .update_configuration_set_event_destination()
        .configuration_set_name("evt-config")
        .event_destination_name("my-sns-dest")
        .event_destination(
            EventDestinationDefinition::builder()
                .enabled(false)
                .matching_event_types(EventType::Delivery)
                .sns_destination(
                    SnsDestination::builder()
                        .topic_arn("arn:aws:sns:us-east-1:123456789012:updated-topic")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Verify update
    let get = client
        .get_configuration_set_event_destinations()
        .configuration_set_name("evt-config")
        .send()
        .await
        .unwrap();
    let dests = get.event_destinations();
    assert!(!dests[0].enabled());
    assert_eq!(dests[0].matching_event_types().len(), 1);

    // Delete event destination
    client
        .delete_configuration_set_event_destination()
        .configuration_set_name("evt-config")
        .event_destination_name("my-sns-dest")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let get = client
        .get_configuration_set_event_destinations()
        .configuration_set_name("evt-config")
        .send()
        .await
        .unwrap();
    assert!(get.event_destinations().is_empty());
}

// --- Email Identity Policies ---

#[tokio::test]
async fn ses_identity_policy_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create identity
    client
        .create_email_identity()
        .email_identity("policy-test@example.com")
        .send()
        .await
        .unwrap();

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"ses:SendEmail","Resource":"*"}]}"#;

    // Create policy
    client
        .create_email_identity_policy()
        .email_identity("policy-test@example.com")
        .policy_name("my-policy")
        .policy(policy_doc)
        .send()
        .await
        .unwrap();

    // Get policies
    let get = client
        .get_email_identity_policies()
        .email_identity("policy-test@example.com")
        .send()
        .await
        .unwrap();
    let policies = get.policies().unwrap();
    assert_eq!(policies.len(), 1);
    assert!(policies.contains_key("my-policy"));
    assert_eq!(policies.get("my-policy").unwrap().as_str(), policy_doc);

    // Update policy
    let updated_doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
    client
        .update_email_identity_policy()
        .email_identity("policy-test@example.com")
        .policy_name("my-policy")
        .policy(updated_doc)
        .send()
        .await
        .unwrap();

    // Verify update
    let get = client
        .get_email_identity_policies()
        .email_identity("policy-test@example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.policies().unwrap().get("my-policy").unwrap().as_str(),
        updated_doc
    );

    // Delete policy
    client
        .delete_email_identity_policy()
        .email_identity("policy-test@example.com")
        .policy_name("my-policy")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let get = client
        .get_email_identity_policies()
        .email_identity("policy-test@example.com")
        .send()
        .await
        .unwrap();
    assert!(get.policies().unwrap().is_empty());
}

// --- Group 1: DKIM & Identity Attributes ---

#[tokio::test]
async fn ses_identity_dkim_attributes() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create domain identity
    client
        .create_email_identity()
        .email_identity("dkim-test.com")
        .send()
        .await
        .unwrap();

    // Disable DKIM signing
    client
        .put_email_identity_dkim_attributes()
        .email_identity("dkim-test.com")
        .signing_enabled(false)
        .send()
        .await
        .unwrap();

    // Verify via get
    let get = client
        .get_email_identity()
        .email_identity("dkim-test.com")
        .send()
        .await
        .unwrap();
    assert!(!get.dkim_attributes().unwrap().signing_enabled());

    // Re-enable
    client
        .put_email_identity_dkim_attributes()
        .email_identity("dkim-test.com")
        .signing_enabled(true)
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity()
        .email_identity("dkim-test.com")
        .send()
        .await
        .unwrap();
    assert!(get.dkim_attributes().unwrap().signing_enabled());
}

#[tokio::test]
async fn ses_identity_dkim_signing_attributes() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("dkim-signing.com")
        .send()
        .await
        .unwrap();

    let resp = client
        .put_email_identity_dkim_signing_attributes()
        .email_identity("dkim-signing.com")
        .signing_attributes_origin(DkimSigningAttributesOrigin::External)
        .signing_attributes(
            DkimSigningAttributes::builder()
                .domain_signing_private_key("private-key-data")
                .domain_signing_selector("selector1")
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.dkim_status().unwrap().as_str(), "SUCCESS");
    assert!(!resp.dkim_tokens().is_empty());

    // Verify the origin changed
    let get = client
        .get_email_identity()
        .email_identity("dkim-signing.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.dkim_attributes()
            .unwrap()
            .signing_attributes_origin()
            .unwrap()
            .as_str(),
        "EXTERNAL"
    );
}

#[tokio::test]
async fn ses_identity_feedback_attributes() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("feedback@example.com")
        .send()
        .await
        .unwrap();

    // Disable feedback forwarding
    client
        .put_email_identity_feedback_attributes()
        .email_identity("feedback@example.com")
        .email_forwarding_enabled(false)
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity()
        .email_identity("feedback@example.com")
        .send()
        .await
        .unwrap();
    assert!(!get.feedback_forwarding_status());
}

#[tokio::test]
async fn ses_identity_mail_from_attributes() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("mailfrom.com")
        .send()
        .await
        .unwrap();

    client
        .put_email_identity_mail_from_attributes()
        .email_identity("mailfrom.com")
        .mail_from_domain("mail.mailfrom.com")
        .behavior_on_mx_failure(BehaviorOnMxFailure::RejectMessage)
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity()
        .email_identity("mailfrom.com")
        .send()
        .await
        .unwrap();
    let mf = get.mail_from_attributes().unwrap();
    assert_eq!(mf.mail_from_domain(), "mail.mailfrom.com");
    assert_eq!(mf.behavior_on_mx_failure().as_str(), "REJECT_MESSAGE");
}

#[tokio::test]
async fn ses_identity_configuration_set_attributes() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("cs-assoc.com")
        .send()
        .await
        .unwrap();

    client
        .put_email_identity_configuration_set_attributes()
        .email_identity("cs-assoc.com")
        .configuration_set_name("my-config-set")
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity()
        .email_identity("cs-assoc.com")
        .send()
        .await
        .unwrap();
    assert_eq!(get.configuration_set_name().unwrap(), "my-config-set");
}

// --- Group 2: Configuration Set Options ---

#[tokio::test]
async fn ses_configuration_set_options() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create config set
    client
        .create_configuration_set()
        .configuration_set_name("opts-test")
        .send()
        .await
        .unwrap();

    // Sending options
    client
        .put_configuration_set_sending_options()
        .configuration_set_name("opts-test")
        .sending_enabled(false)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("opts-test")
        .send()
        .await
        .unwrap();
    assert!(!get.sending_options().unwrap().sending_enabled());

    // Delivery options
    client
        .put_configuration_set_delivery_options()
        .configuration_set_name("opts-test")
        .tls_policy(TlsPolicy::Require)
        .sending_pool_name("pool-1")
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("opts-test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.delivery_options()
            .unwrap()
            .tls_policy()
            .unwrap()
            .as_str(),
        "REQUIRE"
    );
    assert_eq!(
        get.delivery_options().unwrap().sending_pool_name().unwrap(),
        "pool-1"
    );

    // Tracking options
    client
        .put_configuration_set_tracking_options()
        .configuration_set_name("opts-test")
        .custom_redirect_domain("track.example.com")
        .https_policy(HttpsPolicy::Require)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("opts-test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.tracking_options().unwrap().custom_redirect_domain(),
        "track.example.com"
    );
    assert_eq!(
        get.tracking_options()
            .unwrap()
            .https_policy()
            .unwrap()
            .as_str(),
        "REQUIRE"
    );

    // Suppression options
    client
        .put_configuration_set_suppression_options()
        .configuration_set_name("opts-test")
        .suppressed_reasons(SuppressionListReason::Bounce)
        .suppressed_reasons(SuppressionListReason::Complaint)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("opts-test")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.suppression_options()
            .unwrap()
            .suppressed_reasons()
            .len(),
        2
    );

    // Reputation options
    client
        .put_configuration_set_reputation_options()
        .configuration_set_name("opts-test")
        .reputation_metrics_enabled(true)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("opts-test")
        .send()
        .await
        .unwrap();
    assert!(get
        .reputation_options()
        .unwrap()
        .reputation_metrics_enabled());

    // Archiving options
    client
        .put_configuration_set_archiving_options()
        .configuration_set_name("opts-test")
        .archive_arn("arn:aws:ses:us-east-1:123456789012:mailmanager-archive/test")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn ses_configuration_set_options_not_found() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    let err = client
        .put_configuration_set_sending_options()
        .configuration_set_name("no-such-set")
        .sending_enabled(false)
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{:?}", err).contains("NotFoundException"),
        "Expected NotFoundException"
    );
}

// --- Group 3: Custom Verification Email Templates ---

#[tokio::test]
async fn ses_custom_verification_email_template_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create
    client
        .create_custom_verification_email_template()
        .template_name("verify-tmpl")
        .from_email_address("noreply@example.com")
        .template_subject("Please verify")
        .template_content("<h1>Verify your email</h1>")
        .success_redirection_url("https://example.com/ok")
        .failure_redirection_url("https://example.com/fail")
        .send()
        .await
        .unwrap();

    // Get
    let get = client
        .get_custom_verification_email_template()
        .template_name("verify-tmpl")
        .send()
        .await
        .unwrap();
    assert_eq!(get.template_name().unwrap(), "verify-tmpl");
    assert_eq!(get.from_email_address().unwrap(), "noreply@example.com");
    assert_eq!(get.template_subject().unwrap(), "Please verify");
    assert_eq!(
        get.template_content().unwrap(),
        "<h1>Verify your email</h1>"
    );
    assert_eq!(
        get.success_redirection_url().unwrap(),
        "https://example.com/ok"
    );
    assert_eq!(
        get.failure_redirection_url().unwrap(),
        "https://example.com/fail"
    );

    // List
    let list = client
        .list_custom_verification_email_templates()
        .send()
        .await
        .unwrap();
    assert_eq!(list.custom_verification_email_templates().len(), 1);

    // Update
    client
        .update_custom_verification_email_template()
        .template_name("verify-tmpl")
        .from_email_address("noreply@example.com")
        .template_subject("Updated subject")
        .template_content("<h1>Updated</h1>")
        .success_redirection_url("https://example.com/ok")
        .failure_redirection_url("https://example.com/fail")
        .send()
        .await
        .unwrap();

    let get = client
        .get_custom_verification_email_template()
        .template_name("verify-tmpl")
        .send()
        .await
        .unwrap();
    assert_eq!(get.template_subject().unwrap(), "Updated subject");

    // Delete
    client
        .delete_custom_verification_email_template()
        .template_name("verify-tmpl")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let err = client
        .get_custom_verification_email_template()
        .template_name("verify-tmpl")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{:?}", err).contains("NotFoundException"),
        "Expected NotFoundException"
    );
}

#[tokio::test]
async fn ses_send_custom_verification_email() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create template first
    client
        .create_custom_verification_email_template()
        .template_name("send-verify")
        .from_email_address("noreply@example.com")
        .template_subject("Verify")
        .template_content("content")
        .success_redirection_url("https://ok")
        .failure_redirection_url("https://fail")
        .send()
        .await
        .unwrap();

    // Send
    let resp = client
        .send_custom_verification_email()
        .email_address("user@example.com")
        .template_name("send-verify")
        .send()
        .await
        .unwrap();
    assert!(resp.message_id().is_some());
}

// --- Group 4: TestRenderEmailTemplate ---

#[tokio::test]
async fn ses_test_render_email_template() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create template
    client
        .create_email_template()
        .template_name("render-test")
        .template_content(
            EmailTemplateContent::builder()
                .subject("Hello {{name}}")
                .html("<p>Welcome, {{name}}! Code: {{code}}</p>")
                .text("Welcome, {{name}}! Code: {{code}}")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Render
    let resp = client
        .test_render_email_template()
        .template_name("render-test")
        .template_data(r#"{"name": "Alice", "code": "5678"}"#)
        .send()
        .await
        .unwrap();

    let rendered = resp.rendered_template();
    assert!(rendered.contains("Subject: Hello Alice"));
    assert!(rendered.contains("Welcome, Alice!"));
    assert!(rendered.contains("Code: 5678"));
}

#[tokio::test]
async fn ses_test_render_email_template_not_found() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    let err = client
        .test_render_email_template()
        .template_name("nonexistent")
        .template_data("{}")
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{:?}", err).contains("NotFoundException"),
        "Expected NotFoundException"
    );
}
