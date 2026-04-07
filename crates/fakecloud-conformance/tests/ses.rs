mod helpers;

use aws_sdk_sesv2::types::{
    BehaviorOnMxFailure, Body, Content, Destination, DkimSigningAttributes,
    DkimSigningAttributesOrigin, EmailContent, EmailTemplateContent, EventDestinationDefinition,
    EventType, HttpsPolicy, Message, SnsDestination, SubscriptionStatus, SuppressionListReason,
    Tag, Template, TlsPolicy, Topic, TopicPreference,
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

// -- Suppression List --

#[test_action("ses", "PutSuppressedDestination", checksum = "6c67e4ef")]
#[test_action("ses", "GetSuppressedDestination", checksum = "7c4f3480")]
#[test_action("ses", "ListSuppressedDestinations", checksum = "3ef5cbaf")]
#[test_action("ses", "DeleteSuppressedDestination", checksum = "e8abb2a8")]
#[tokio::test]
async fn ses_suppression_list_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Put
    client
        .put_suppressed_destination()
        .email_address("bounce@example.com")
        .reason(SuppressionListReason::Bounce)
        .send()
        .await
        .unwrap();

    // Get
    let get = client
        .get_suppressed_destination()
        .email_address("bounce@example.com")
        .send()
        .await
        .unwrap();
    let dest = get.suppressed_destination().unwrap();
    assert_eq!(dest.email_address(), "bounce@example.com");
    assert_eq!(dest.reason(), &SuppressionListReason::Bounce);

    // List
    let list = client.list_suppressed_destinations().send().await.unwrap();
    assert_eq!(list.suppressed_destination_summaries().len(), 1);

    // Delete
    client
        .delete_suppressed_destination()
        .email_address("bounce@example.com")
        .send()
        .await
        .unwrap();

    let list = client.list_suppressed_destinations().send().await.unwrap();
    assert!(list.suppressed_destination_summaries().is_empty());
}

// -- Event Destinations --

#[test_action("ses", "CreateConfigurationSetEventDestination", checksum = "0fdfd515")]
#[test_action("ses", "GetConfigurationSetEventDestinations", checksum = "b4b98ef8")]
#[test_action("ses", "UpdateConfigurationSetEventDestination", checksum = "e82dd562")]
#[test_action("ses", "DeleteConfigurationSetEventDestination", checksum = "acc3da31")]
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
        .event_destination_name("my-dest")
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
    assert_eq!(get.event_destinations().len(), 1);
    assert_eq!(get.event_destinations()[0].name(), "my-dest");
    assert!(get.event_destinations()[0].enabled());
    assert_eq!(get.event_destinations()[0].matching_event_types().len(), 2);

    // Update
    client
        .update_configuration_set_event_destination()
        .configuration_set_name("evt-config")
        .event_destination_name("my-dest")
        .event_destination(
            EventDestinationDefinition::builder()
                .enabled(false)
                .matching_event_types(EventType::Delivery)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set_event_destinations()
        .configuration_set_name("evt-config")
        .send()
        .await
        .unwrap();
    assert!(!get.event_destinations()[0].enabled());
    assert_eq!(get.event_destinations()[0].matching_event_types().len(), 1);

    // Delete
    client
        .delete_configuration_set_event_destination()
        .configuration_set_name("evt-config")
        .event_destination_name("my-dest")
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set_event_destinations()
        .configuration_set_name("evt-config")
        .send()
        .await
        .unwrap();
    assert!(get.event_destinations().is_empty());
}

// -- Email Identity Policies --

#[test_action("ses", "CreateEmailIdentityPolicy", checksum = "bdf62512")]
#[test_action("ses", "GetEmailIdentityPolicies", checksum = "76a5e27d")]
#[test_action("ses", "UpdateEmailIdentityPolicy", checksum = "fddbfe3c")]
#[test_action("ses", "DeleteEmailIdentityPolicy", checksum = "54dd160b")]
#[tokio::test]
async fn ses_identity_policy_lifecycle() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create identity
    client
        .create_email_identity()
        .email_identity("policy@example.com")
        .send()
        .await
        .unwrap();

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"ses:SendEmail","Resource":"*"}]}"#;

    // Create policy
    client
        .create_email_identity_policy()
        .email_identity("policy@example.com")
        .policy_name("my-policy")
        .policy(policy_doc)
        .send()
        .await
        .unwrap();

    // Get policies
    let get = client
        .get_email_identity_policies()
        .email_identity("policy@example.com")
        .send()
        .await
        .unwrap();
    let policies = get.policies().unwrap();
    assert_eq!(policies.len(), 1);
    assert_eq!(policies.get("my-policy").unwrap().as_str(), policy_doc);

    // Update
    let updated_doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
    client
        .update_email_identity_policy()
        .email_identity("policy@example.com")
        .policy_name("my-policy")
        .policy(updated_doc)
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity_policies()
        .email_identity("policy@example.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.policies().unwrap().get("my-policy").unwrap().as_str(),
        updated_doc
    );

    // Delete
    client
        .delete_email_identity_policy()
        .email_identity("policy@example.com")
        .policy_name("my-policy")
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity_policies()
        .email_identity("policy@example.com")
        .send()
        .await
        .unwrap();
    assert!(get.policies().unwrap().is_empty());
}

// -- DKIM & Identity Attributes --

#[test_action("ses", "PutEmailIdentityDkimAttributes", checksum = "e21fbf7e")]
#[test_action("ses", "PutEmailIdentityDkimSigningAttributes", checksum = "9127433a")]
#[test_action("ses", "PutEmailIdentityFeedbackAttributes", checksum = "8d28daf6")]
#[test_action("ses", "PutEmailIdentityMailFromAttributes", checksum = "c31d8e2b")]
#[test_action(
    "ses",
    "PutEmailIdentityConfigurationSetAttributes",
    checksum = "332e93ce"
)]
#[tokio::test]
async fn ses_identity_attributes() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("attrs.com")
        .send()
        .await
        .unwrap();

    // DKIM attributes
    client
        .put_email_identity_dkim_attributes()
        .email_identity("attrs.com")
        .signing_enabled(false)
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity()
        .email_identity("attrs.com")
        .send()
        .await
        .unwrap();
    assert!(!get.dkim_attributes().unwrap().signing_enabled());

    // DKIM signing attributes
    let resp = client
        .put_email_identity_dkim_signing_attributes()
        .email_identity("attrs.com")
        .signing_attributes_origin(DkimSigningAttributesOrigin::External)
        .signing_attributes(
            DkimSigningAttributes::builder()
                .domain_signing_private_key("key")
                .domain_signing_selector("sel")
                .build(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.dkim_status().unwrap().as_str(), "SUCCESS");

    // Feedback attributes
    client
        .put_email_identity_feedback_attributes()
        .email_identity("attrs.com")
        .email_forwarding_enabled(false)
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity()
        .email_identity("attrs.com")
        .send()
        .await
        .unwrap();
    assert!(!get.feedback_forwarding_status());

    // Mail-from attributes
    client
        .put_email_identity_mail_from_attributes()
        .email_identity("attrs.com")
        .mail_from_domain("mail.attrs.com")
        .behavior_on_mx_failure(BehaviorOnMxFailure::RejectMessage)
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity()
        .email_identity("attrs.com")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.mail_from_attributes().unwrap().mail_from_domain(),
        "mail.attrs.com"
    );

    // Configuration set attributes
    client
        .put_email_identity_configuration_set_attributes()
        .email_identity("attrs.com")
        .configuration_set_name("my-cs")
        .send()
        .await
        .unwrap();

    let get = client
        .get_email_identity()
        .email_identity("attrs.com")
        .send()
        .await
        .unwrap();
    assert_eq!(get.configuration_set_name().unwrap(), "my-cs");
}

// -- Configuration Set Options --

#[test_action("ses", "PutConfigurationSetSendingOptions", checksum = "e420c1ea")]
#[test_action("ses", "PutConfigurationSetDeliveryOptions", checksum = "554afc97")]
#[test_action("ses", "PutConfigurationSetTrackingOptions", checksum = "10410773")]
#[test_action("ses", "PutConfigurationSetSuppressionOptions", checksum = "8330b701")]
#[test_action("ses", "PutConfigurationSetReputationOptions", checksum = "eeda6d26")]
#[test_action("ses", "PutConfigurationSetVdmOptions", checksum = "b745e5c2")]
#[test_action("ses", "PutConfigurationSetArchivingOptions", checksum = "c5730f19")]
#[tokio::test]
async fn ses_configuration_set_options() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_configuration_set()
        .configuration_set_name("cs-opts")
        .send()
        .await
        .unwrap();

    // Sending
    client
        .put_configuration_set_sending_options()
        .configuration_set_name("cs-opts")
        .sending_enabled(false)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("cs-opts")
        .send()
        .await
        .unwrap();
    assert!(!get.sending_options().unwrap().sending_enabled());

    // Delivery
    client
        .put_configuration_set_delivery_options()
        .configuration_set_name("cs-opts")
        .tls_policy(TlsPolicy::Require)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("cs-opts")
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

    // Tracking
    client
        .put_configuration_set_tracking_options()
        .configuration_set_name("cs-opts")
        .custom_redirect_domain("t.example.com")
        .https_policy(HttpsPolicy::Require)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("cs-opts")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.tracking_options().unwrap().custom_redirect_domain(),
        "t.example.com"
    );

    // Suppression
    client
        .put_configuration_set_suppression_options()
        .configuration_set_name("cs-opts")
        .suppressed_reasons(SuppressionListReason::Bounce)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("cs-opts")
        .send()
        .await
        .unwrap();
    assert_eq!(
        get.suppression_options()
            .unwrap()
            .suppressed_reasons()
            .len(),
        1
    );

    // Reputation
    client
        .put_configuration_set_reputation_options()
        .configuration_set_name("cs-opts")
        .reputation_metrics_enabled(true)
        .send()
        .await
        .unwrap();

    let get = client
        .get_configuration_set()
        .configuration_set_name("cs-opts")
        .send()
        .await
        .unwrap();
    assert!(get
        .reputation_options()
        .unwrap()
        .reputation_metrics_enabled());

    // VDM options (just test it doesn't error)
    client
        .put_configuration_set_vdm_options()
        .configuration_set_name("cs-opts")
        .send()
        .await
        .unwrap();

    // Archiving options
    client
        .put_configuration_set_archiving_options()
        .configuration_set_name("cs-opts")
        .archive_arn("arn:aws:ses:us-east-1:123456789012:mailmanager-archive/a")
        .send()
        .await
        .unwrap();
}

// -- Custom Verification Email Templates --

#[test_action("ses", "CreateCustomVerificationEmailTemplate", checksum = "31179503")]
#[test_action("ses", "GetCustomVerificationEmailTemplate", checksum = "10932ab0")]
#[test_action("ses", "ListCustomVerificationEmailTemplates", checksum = "4c8404df")]
#[test_action("ses", "UpdateCustomVerificationEmailTemplate", checksum = "0086fd07")]
#[test_action("ses", "DeleteCustomVerificationEmailTemplate", checksum = "e78b69af")]
#[test_action("ses", "SendCustomVerificationEmail", checksum = "873cbcd3")]
#[tokio::test]
async fn ses_custom_verification_email_templates() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Create
    client
        .create_custom_verification_email_template()
        .template_name("cv-tmpl")
        .from_email_address("noreply@example.com")
        .template_subject("Verify")
        .template_content("<h1>Verify</h1>")
        .success_redirection_url("https://ok.example.com")
        .failure_redirection_url("https://fail.example.com")
        .send()
        .await
        .unwrap();

    // Get
    let get = client
        .get_custom_verification_email_template()
        .template_name("cv-tmpl")
        .send()
        .await
        .unwrap();
    assert_eq!(get.template_name().unwrap(), "cv-tmpl");
    assert_eq!(get.template_subject().unwrap(), "Verify");

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
        .template_name("cv-tmpl")
        .from_email_address("noreply@example.com")
        .template_subject("Updated verify")
        .template_content("<h1>Updated</h1>")
        .success_redirection_url("https://ok.example.com")
        .failure_redirection_url("https://fail.example.com")
        .send()
        .await
        .unwrap();

    let get = client
        .get_custom_verification_email_template()
        .template_name("cv-tmpl")
        .send()
        .await
        .unwrap();
    assert_eq!(get.template_subject().unwrap(), "Updated verify");

    // Send custom verification email
    let resp = client
        .send_custom_verification_email()
        .email_address("user@example.com")
        .template_name("cv-tmpl")
        .send()
        .await
        .unwrap();
    assert!(resp.message_id().is_some());

    // Delete
    client
        .delete_custom_verification_email_template()
        .template_name("cv-tmpl")
        .send()
        .await
        .unwrap();

    let err = client
        .get_custom_verification_email_template()
        .template_name("cv-tmpl")
        .send()
        .await;
    assert!(err.is_err());
}

// -- TestRenderEmailTemplate --

#[test_action("ses", "TestRenderEmailTemplate", checksum = "77a61db8")]
#[tokio::test]
async fn ses_test_render_email_template() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_template()
        .template_name("render-tmpl")
        .template_content(
            EmailTemplateContent::builder()
                .subject("Hi {{name}}")
                .html("<p>Hello {{name}}, code={{code}}</p>")
                .text("Hello {{name}}, code={{code}}")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .test_render_email_template()
        .template_name("render-tmpl")
        .template_data(r#"{"name": "Bob", "code": "42"}"#)
        .send()
        .await
        .unwrap();

    let rendered = resp.rendered_template();
    assert!(rendered.contains("Subject: Hi Bob"));
    assert!(rendered.contains("Hello Bob"));
    assert!(rendered.contains("code=42"));
}
