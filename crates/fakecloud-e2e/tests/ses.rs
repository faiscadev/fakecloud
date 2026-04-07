mod helpers;

use aws_sdk_sesv2::types::{
    Body, Content, Destination, EmailContent, EmailTemplateContent, Message, RawMessage,
    SubscriptionStatus, Template, Topic, TopicPreference,
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
