mod helpers;
use helpers::TestServer;

// ── Identity Management ──

#[tokio::test]
async fn test_verify_email_identity() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_email_identity()
        .email_address("test@example.com")
        .send()
        .await
        .expect("verify email identity");

    // List identities — should include the email
    let list = client
        .list_identities()
        .send()
        .await
        .expect("list identities");
    assert!(list.identities().contains(&"test@example.com".to_string()));
}

#[tokio::test]
async fn test_verify_domain_identity() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    let resp = client
        .verify_domain_identity()
        .domain("example.com")
        .send()
        .await
        .expect("verify domain identity");

    // Should return a verification token
    assert!(!resp.verification_token().is_empty());

    let list = client
        .list_identities()
        .send()
        .await
        .expect("list identities");
    assert!(list.identities().contains(&"example.com".to_string()));
}

#[tokio::test]
async fn test_verify_domain_dkim() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    let resp = client
        .verify_domain_dkim()
        .domain("example.com")
        .send()
        .await
        .expect("verify domain dkim");

    // Should return 3 DKIM tokens
    assert_eq!(resp.dkim_tokens().len(), 3);
}

#[tokio::test]
async fn test_list_identities_filter_by_type() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_email_identity()
        .email_address("user@example.com")
        .send()
        .await
        .unwrap();
    client
        .verify_domain_identity()
        .domain("example.com")
        .send()
        .await
        .unwrap();

    // Filter by EmailAddress
    let list = client
        .list_identities()
        .identity_type(aws_sdk_ses::types::IdentityType::EmailAddress)
        .send()
        .await
        .unwrap();
    assert!(list.identities().contains(&"user@example.com".to_string()));
    assert!(!list.identities().contains(&"example.com".to_string()));

    // Filter by Domain
    let list = client
        .list_identities()
        .identity_type(aws_sdk_ses::types::IdentityType::Domain)
        .send()
        .await
        .unwrap();
    assert!(!list.identities().contains(&"user@example.com".to_string()));
    assert!(list.identities().contains(&"example.com".to_string()));
}

#[tokio::test]
async fn test_get_identity_verification_attributes() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_email_identity()
        .email_address("verified@example.com")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_identity_verification_attributes()
        .identities("verified@example.com")
        .identities("unknown@example.com")
        .send()
        .await
        .expect("get verification attributes");

    let attrs = resp.verification_attributes();
    let verified = attrs.get("verified@example.com").expect("verified entry");
    assert_eq!(
        *verified.verification_status(),
        aws_sdk_ses::types::VerificationStatus::Success
    );

    let unknown = attrs.get("unknown@example.com").expect("unknown entry");
    assert_eq!(
        *unknown.verification_status(),
        aws_sdk_ses::types::VerificationStatus::NotStarted
    );
}

#[tokio::test]
async fn test_delete_identity() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_email_identity()
        .email_address("delete-me@example.com")
        .send()
        .await
        .unwrap();

    client
        .delete_identity()
        .identity("delete-me@example.com")
        .send()
        .await
        .expect("delete identity");

    let list = client.list_identities().send().await.unwrap();
    assert!(!list
        .identities()
        .contains(&"delete-me@example.com".to_string()));
}

#[tokio::test]
async fn test_set_identity_dkim_enabled() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_email_identity()
        .email_address("dkim@example.com")
        .send()
        .await
        .unwrap();

    client
        .set_identity_dkim_enabled()
        .identity("dkim@example.com")
        .dkim_enabled(true)
        .send()
        .await
        .expect("enable dkim");

    let resp = client
        .get_identity_dkim_attributes()
        .identities("dkim@example.com")
        .send()
        .await
        .unwrap();

    let attrs = resp.dkim_attributes();
    let entry = attrs.get("dkim@example.com").expect("dkim entry");
    assert!(entry.dkim_enabled());
}

#[tokio::test]
async fn test_get_identity_dkim_attributes() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_domain_identity()
        .domain("dkim-test.com")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_identity_dkim_attributes()
        .identities("dkim-test.com")
        .send()
        .await
        .expect("get dkim attributes");

    let attrs = resp.dkim_attributes();
    let entry = attrs.get("dkim-test.com").expect("dkim entry");
    assert_eq!(
        *entry.dkim_verification_status(),
        aws_sdk_ses::types::VerificationStatus::Success
    );
    // Domains should have DKIM tokens
    assert_eq!(entry.dkim_tokens().len(), 3);
}

// ── Identity Attributes ──

#[tokio::test]
async fn test_set_identity_feedback_forwarding() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_email_identity()
        .email_address("fwd@example.com")
        .send()
        .await
        .unwrap();

    // Default should be enabled
    let resp = client
        .get_identity_notification_attributes()
        .identities("fwd@example.com")
        .send()
        .await
        .unwrap();
    let attrs = resp.notification_attributes();
    assert!(attrs.get("fwd@example.com").unwrap().forwarding_enabled());

    // Disable
    client
        .set_identity_feedback_forwarding_enabled()
        .identity("fwd@example.com")
        .forwarding_enabled(false)
        .send()
        .await
        .expect("disable forwarding");

    let resp = client
        .get_identity_notification_attributes()
        .identities("fwd@example.com")
        .send()
        .await
        .unwrap();
    let attrs = resp.notification_attributes();
    assert!(!attrs.get("fwd@example.com").unwrap().forwarding_enabled());
}

#[tokio::test]
async fn test_set_identity_mail_from_domain() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_domain_identity()
        .domain("example.com")
        .send()
        .await
        .unwrap();

    client
        .set_identity_mail_from_domain()
        .identity("example.com")
        .mail_from_domain("mail.example.com")
        .behavior_on_mx_failure(aws_sdk_ses::types::BehaviorOnMxFailure::RejectMessage)
        .send()
        .await
        .expect("set mail from domain");

    let resp = client
        .get_identity_mail_from_domain_attributes()
        .identities("example.com")
        .send()
        .await
        .unwrap();
    let attrs = resp.mail_from_domain_attributes();
    let entry = attrs.get("example.com").expect("mail from entry");
    assert_eq!(entry.mail_from_domain(), "mail.example.com");
}

#[tokio::test]
async fn test_set_identity_notification_topic() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .verify_email_identity()
        .email_address("notif@example.com")
        .send()
        .await
        .unwrap();

    // Should not error
    client
        .set_identity_notification_topic()
        .identity("notif@example.com")
        .notification_type(aws_sdk_ses::types::NotificationType::Bounce)
        .sns_topic("arn:aws:sns:us-east-1:123456789012:bounce-topic")
        .send()
        .await
        .expect("set notification topic");
}

// ── Sending ──

#[tokio::test]
async fn test_send_email_v1() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    let resp = client
        .send_email()
        .source("sender@example.com")
        .destination(
            aws_sdk_ses::types::Destination::builder()
                .to_addresses("to@example.com")
                .cc_addresses("cc@example.com")
                .build(),
        )
        .message(
            aws_sdk_ses::types::Message::builder()
                .subject(
                    aws_sdk_ses::types::Content::builder()
                        .data("Test Subject")
                        .build()
                        .unwrap(),
                )
                .body(
                    aws_sdk_ses::types::Body::builder()
                        .html(
                            aws_sdk_ses::types::Content::builder()
                                .data("<h1>Hello</h1>")
                                .build()
                                .unwrap(),
                        )
                        .text(
                            aws_sdk_ses::types::Content::builder()
                                .data("Hello text")
                                .build()
                                .unwrap(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("send email v1");

    assert!(!resp.message_id().is_empty());

    // Verify via introspection
    let url = format!("{}/_fakecloud/ses/emails", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let emails = resp["emails"].as_array().unwrap();
    assert_eq!(emails.len(), 1);
    assert_eq!(emails[0]["from"], "sender@example.com");
    assert_eq!(emails[0]["to"][0], "to@example.com");
    assert_eq!(emails[0]["subject"], "Test Subject");
}

#[tokio::test]
async fn test_send_raw_email_v1() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    let raw_message = aws_sdk_ses::types::RawMessage::builder()
        .data(aws_sdk_ses::primitives::Blob::new(
            "From: sender@example.com\r\nTo: to@example.com\r\nSubject: Raw Test\r\n\r\nBody",
        ))
        .build()
        .unwrap();

    let resp = client
        .send_raw_email()
        .raw_message(raw_message)
        .source("sender@example.com")
        .destinations("to@example.com")
        .send()
        .await
        .expect("send raw email");

    assert!(!resp.message_id().is_empty());

    let url = format!("{}/_fakecloud/ses/emails", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let emails = resp["emails"].as_array().unwrap();
    assert_eq!(emails.len(), 1);
    assert!(emails[0]["rawData"].as_str().is_some());
}

// ── Templates ──

#[tokio::test]
async fn test_template_lifecycle_v1() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    // Create template
    client
        .create_template()
        .template(
            aws_sdk_ses::types::Template::builder()
                .template_name("welcome")
                .subject_part("Welcome {{name}}")
                .html_part("<h1>Welcome {{name}}</h1>")
                .text_part("Welcome {{name}}")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create template");

    // Get template
    let resp = client
        .get_template()
        .template_name("welcome")
        .send()
        .await
        .expect("get template");
    let tmpl = resp.template().unwrap();
    assert_eq!(tmpl.template_name(), "welcome");
    assert_eq!(tmpl.subject_part(), Some("Welcome {{name}}"));

    // List templates
    let list = client
        .list_templates()
        .send()
        .await
        .expect("list templates");
    assert_eq!(list.templates_metadata().len(), 1);
    assert_eq!(list.templates_metadata()[0].name(), Some("welcome"));

    // Update template
    client
        .update_template()
        .template(
            aws_sdk_ses::types::Template::builder()
                .template_name("welcome")
                .subject_part("Updated Welcome {{name}}")
                .html_part("<h1>Updated</h1>")
                .text_part("Updated")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("update template");

    let resp = client
        .get_template()
        .template_name("welcome")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.template().unwrap().subject_part(),
        Some("Updated Welcome {{name}}")
    );

    // Delete template
    client
        .delete_template()
        .template_name("welcome")
        .send()
        .await
        .expect("delete template");

    // Should be gone
    let err = client.get_template().template_name("welcome").send().await;
    assert!(err.is_err());
}

#[tokio::test]
async fn test_send_templated_email_v1() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    // Create template first
    client
        .create_template()
        .template(
            aws_sdk_ses::types::Template::builder()
                .template_name("greet")
                .subject_part("Hi {{name}}")
                .html_part("<p>Hello {{name}}</p>")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .send_templated_email()
        .source("sender@example.com")
        .destination(
            aws_sdk_ses::types::Destination::builder()
                .to_addresses("to@example.com")
                .build(),
        )
        .template("greet")
        .template_data("{\"name\":\"World\"}")
        .send()
        .await
        .expect("send templated email");

    assert!(!resp.message_id().is_empty());

    let url = format!("{}/_fakecloud/ses/emails", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let emails = resp["emails"].as_array().unwrap();
    assert_eq!(emails.len(), 1);
    assert_eq!(emails[0]["templateName"], "greet");
}

#[tokio::test]
async fn test_send_templated_email_missing_template() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    let err = client
        .send_templated_email()
        .source("sender@example.com")
        .destination(
            aws_sdk_ses::types::Destination::builder()
                .to_addresses("to@example.com")
                .build(),
        )
        .template("nonexistent")
        .template_data("{}")
        .send()
        .await;

    assert!(err.is_err());
}

#[tokio::test]
async fn test_send_bulk_templated_email_v1() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .create_template()
        .template(
            aws_sdk_ses::types::Template::builder()
                .template_name("bulk")
                .subject_part("Hi {{name}}")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .send_bulk_templated_email()
        .source("sender@example.com")
        .template("bulk")
        .default_template_data("{\"name\":\"default\"}")
        .destinations(
            aws_sdk_ses::types::BulkEmailDestination::builder()
                .destination(
                    aws_sdk_ses::types::Destination::builder()
                        .to_addresses("a@example.com")
                        .build(),
                )
                .build(),
        )
        .destinations(
            aws_sdk_ses::types::BulkEmailDestination::builder()
                .destination(
                    aws_sdk_ses::types::Destination::builder()
                        .to_addresses("b@example.com")
                        .build(),
                )
                .replacement_template_data("{\"name\":\"custom\"}")
                .build(),
        )
        .send()
        .await
        .expect("send bulk templated email");

    let status = resp.status();
    assert_eq!(status.len(), 2);

    let url = format!("{}/_fakecloud/ses/emails", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let emails = resp["emails"].as_array().unwrap();
    assert_eq!(emails.len(), 2);
}

// ── Configuration Sets ──

#[tokio::test]
async fn test_configuration_set_lifecycle_v1() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    // Create
    client
        .create_configuration_set()
        .configuration_set(
            aws_sdk_ses::types::ConfigurationSet::builder()
                .name("my-config-set")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create config set");

    // List
    let list = client
        .list_configuration_sets()
        .send()
        .await
        .expect("list config sets");
    let sets = list.configuration_sets();
    assert_eq!(sets.len(), 1);
    assert_eq!(sets[0].name(), "my-config-set");

    // Describe
    let desc = client
        .describe_configuration_set()
        .configuration_set_name("my-config-set")
        .send()
        .await
        .expect("describe config set");
    assert_eq!(desc.configuration_set().unwrap().name(), "my-config-set");

    // Delete
    client
        .delete_configuration_set()
        .configuration_set_name("my-config-set")
        .send()
        .await
        .expect("delete config set");

    let list = client.list_configuration_sets().send().await.unwrap();
    assert!(list.configuration_sets().is_empty());
}

#[tokio::test]
async fn test_configuration_set_duplicate() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    client
        .create_configuration_set()
        .configuration_set(
            aws_sdk_ses::types::ConfigurationSet::builder()
                .name("dup-set")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let err = client
        .create_configuration_set()
        .configuration_set(
            aws_sdk_ses::types::ConfigurationSet::builder()
                .name("dup-set")
                .build()
                .unwrap(),
        )
        .send()
        .await;

    assert!(err.is_err());
}

// ── Account / Quota ──

#[tokio::test]
async fn test_get_send_quota() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    let resp = client
        .get_send_quota()
        .send()
        .await
        .expect("get send quota");

    assert!(resp.max24_hour_send() > 0.0);
    assert!(resp.max_send_rate() > 0.0);
    assert_eq!(resp.sent_last24_hours(), 0.0);
}

#[tokio::test]
async fn test_get_send_statistics() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    let resp = client
        .get_send_statistics()
        .send()
        .await
        .expect("get send statistics");

    assert!(!resp.send_data_points().is_empty());
}

#[tokio::test]
async fn test_get_account_sending_enabled() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    let resp = client
        .get_account_sending_enabled()
        .send()
        .await
        .expect("get account sending enabled");

    assert!(resp.enabled());
}
