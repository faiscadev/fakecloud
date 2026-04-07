mod helpers;

use aws_sdk_sesv2::types::{
    Body, Content, Destination, EmailContent, EmailTemplateContent, Message, SubscriptionStatus,
    Tag, Template, Topic, TopicPreference,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

// -- Account --

#[test_action("ses", "GetAccount", checksum = "3104b701")]
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

// -- Email Identity CRUD --

#[test_action("ses", "CreateEmailIdentity", checksum = "1ff0be27")]
#[test_action("ses", "GetEmailIdentity", checksum = "a298f1a4")]
#[test_action("ses", "ListEmailIdentities", checksum = "3301504d")]
#[test_action("ses", "DeleteEmailIdentity", checksum = "7b850c25")]
#[tokio::test]
async fn ses_identity_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create
    let resp = client
        .create_email_identity()
        .email_identity("test@example.com")
        .send()
        .await
        .unwrap();
    assert!(resp.verified_for_sending_status());
    assert_eq!(resp.identity_type().unwrap().as_str(), "EMAIL_ADDRESS");

    // Get — verify auto-verified and DKIM
    let get = client
        .get_email_identity()
        .email_identity("test@example.com")
        .send()
        .await
        .unwrap();
    assert!(get.verified_for_sending_status());
    assert_eq!(
        get.dkim_attributes().unwrap().status().unwrap().as_str(),
        "SUCCESS"
    );

    // List
    let list = client.list_email_identities().send().await.unwrap();
    assert_eq!(list.email_identities().len(), 1);

    // Delete
    client
        .delete_email_identity()
        .email_identity("test@example.com")
        .send()
        .await
        .unwrap();

    let list = client.list_email_identities().send().await.unwrap();
    assert!(list.email_identities().is_empty());
}

// -- Configuration Set CRUD --

#[test_action("ses", "CreateConfigurationSet", checksum = "a48841bc")]
#[test_action("ses", "GetConfigurationSet", checksum = "00b213d2")]
#[test_action("ses", "ListConfigurationSets", checksum = "31486196")]
#[test_action("ses", "DeleteConfigurationSet", checksum = "3c50e07a")]
#[tokio::test]
async fn ses_configuration_set_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_configuration_set()
        .configuration_set_name("my-config")
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("my-config")
        .send()
        .await
        .unwrap();
    assert_eq!(get.configuration_set_name(), Some("my-config"));

    let list = client.list_configuration_sets().send().await.unwrap();
    assert_eq!(list.configuration_sets().len(), 1);

    client
        .delete_configuration_set()
        .configuration_set_name("my-config")
        .send()
        .await
        .unwrap();

    let list = client.list_configuration_sets().send().await.unwrap();
    assert!(list.configuration_sets().is_empty());
}

// -- Email Template CRUD --

#[test_action("ses", "CreateEmailTemplate", checksum = "0f6b9b5f")]
#[test_action("ses", "GetEmailTemplate", checksum = "24e82803")]
#[test_action("ses", "UpdateEmailTemplate", checksum = "53fcbe68")]
#[test_action("ses", "ListEmailTemplates", checksum = "d266ac1a")]
#[test_action("ses", "DeleteEmailTemplate", checksum = "92237e2c")]
#[tokio::test]
async fn ses_template_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

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

    let get = client
        .get_email_template()
        .template_name("welcome")
        .send()
        .await
        .unwrap();
    assert_eq!(get.template_name(), "welcome");
    assert_eq!(
        get.template_content().unwrap().subject().unwrap(),
        "Welcome {{name}}"
    );

    client
        .update_email_template()
        .template_name("welcome")
        .template_content(
            EmailTemplateContent::builder()
                .subject("Updated {{name}}")
                .html("<h1>Updated</h1>")
                .text("Updated")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_template()
        .template_name("welcome")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.template_content().unwrap().subject().unwrap(),
        "Updated {{name}}"
    );

    let list = client.list_email_templates().send().await.unwrap();
    assert_eq!(list.templates_metadata().len(), 1);

    client
        .delete_email_template()
        .template_name("welcome")
        .send()
        .await
        .unwrap();

    let list = client.list_email_templates().send().await.unwrap();
    assert!(list.templates_metadata().is_empty());
}

// -- SendEmail --

#[test_action("ses", "SendEmail", checksum = "364cd183")]
#[tokio::test]
async fn ses_send_email() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

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
                                .text(Content::builder().data("Hello").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert!(!resp.message_id().unwrap().is_empty());
}

// -- SendEmail with template --

#[tokio::test]
async fn ses_send_email_with_template() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

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

// -- SendBulkEmail --

#[test_action("ses", "SendBulkEmail", checksum = "a88f124e")]
#[tokio::test]
async fn ses_send_bulk_email() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("sender@example.com")
        .send()
        .await
        .unwrap();

    client
        .create_email_template()
        .template_name("bulk-tmpl")
        .template_content(
            EmailTemplateContent::builder()
                .subject("Hello {{name}}")
                .text("Hi {{name}}")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .send_bulk_email()
        .from_email_address("sender@example.com")
        .default_content(
            aws_sdk_sesv2::types::BulkEmailContent::builder()
                .template(
                    Template::builder()
                        .template_name("bulk-tmpl")
                        .template_data(r#"{"name":"Default"}"#)
                        .build(),
                )
                .build(),
        )
        .bulk_email_entries(
            aws_sdk_sesv2::types::BulkEmailEntry::builder()
                .destination(Destination::builder().to_addresses("a@example.com").build())
                .build(),
        )
        .bulk_email_entries(
            aws_sdk_sesv2::types::BulkEmailEntry::builder()
                .destination(Destination::builder().to_addresses("b@example.com").build())
                .build(),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(resp.bulk_email_entry_results().len(), 2);
    for result in resp.bulk_email_entry_results() {
        assert_eq!(result.status().unwrap().as_str(), "SUCCESS");
        assert!(result.message_id().is_some());
    }
}

// -- Contact List CRUD --

#[test_action("ses", "CreateContactList", checksum = "7f6cc2fa")]
#[test_action("ses", "GetContactList", checksum = "7e2e0316")]
#[test_action("ses", "ListContactLists", checksum = "cdc01160")]
#[test_action("ses", "UpdateContactList", checksum = "8e3bd6e3")]
#[test_action("ses", "DeleteContactList", checksum = "328a2af5")]
#[tokio::test]
async fn ses_contact_list_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create
    client
        .create_contact_list()
        .contact_list_name("my-list")
        .description("Test list")
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

    // Get
    let get = client
        .get_contact_list()
        .contact_list_name("my-list")
        .send()
        .await
        .unwrap();
    assert_eq!(get.contact_list_name(), Some("my-list"));
    assert_eq!(get.description(), Some("Test list"));
    assert_eq!(get.topics().len(), 1);
    assert_eq!(get.topics()[0].topic_name(), "newsletters");

    // List
    let list = client.list_contact_lists().send().await.unwrap();
    assert_eq!(list.contact_lists().len(), 1);

    // Update
    client
        .update_contact_list()
        .contact_list_name("my-list")
        .description("Updated")
        .send()
        .await
        .unwrap();

    let get = client
        .get_contact_list()
        .contact_list_name("my-list")
        .send()
        .await
        .unwrap();
    assert_eq!(get.description(), Some("Updated"));

    // Delete
    client
        .delete_contact_list()
        .contact_list_name("my-list")
        .send()
        .await
        .unwrap();

    let list = client.list_contact_lists().send().await.unwrap();
    assert!(list.contact_lists().is_empty());
}

// -- Contact CRUD --

#[test_action("ses", "CreateContact", checksum = "6919c110")]
#[test_action("ses", "GetContact", checksum = "606051bc")]
#[test_action("ses", "ListContacts", checksum = "0762c146")]
#[test_action("ses", "UpdateContact", checksum = "4846a375")]
#[test_action("ses", "DeleteContact", checksum = "ff3abfb5")]
#[tokio::test]
async fn ses_contact_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create contact list first
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

    // Create contact
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
    assert!(!get.unsubscribe_all());
    assert_eq!(get.topic_preferences().len(), 1);
    assert_eq!(
        get.topic_preferences()[0].subscription_status(),
        &SubscriptionStatus::OptIn
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

    let list = client
        .list_contacts()
        .contact_list_name("my-list")
        .send()
        .await
        .unwrap();
    assert!(list.contacts().is_empty());
}

// -- Tagging --

#[test_action("ses", "TagResource", checksum = "473ff38c")]
#[test_action("ses", "UntagResource", checksum = "b8406c4d")]
#[test_action("ses", "ListTagsForResource", checksum = "35efea8c")]
#[tokio::test]
async fn ses_tagging_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create an identity to tag
    client
        .create_email_identity()
        .email_identity("tag-test@example.com")
        .send()
        .await
        .unwrap();

    let arn = "arn:aws:ses:us-east-1:000000000000:identity/tag-test@example.com";

    // Tag
    client
        .tag_resource()
        .resource_arn(arn)
        .tags(Tag::builder().key("env").value("prod").build().unwrap())
        .tags(Tag::builder().key("team").value("backend").build().unwrap())
        .send()
        .await
        .unwrap();

    // List
    let resp = client
        .list_tags_for_resource()
        .resource_arn(arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().len(), 2);

    // Untag
    client
        .untag_resource()
        .resource_arn(arn)
        .tag_keys("env")
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
    assert_eq!(resp.tags()[0].key(), "team");
    assert_eq!(resp.tags()[0].value(), "backend");
}
