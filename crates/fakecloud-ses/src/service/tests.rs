use super::*;
use bytes::Bytes;
use fakecloud_core::service::AwsService;
use http::{HeaderMap, Method};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

fn make_state() -> SharedSesState {
    Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4569",
        ),
    ))
}

/// Flip the account out of sandbox so tests focused on send mechanics
/// don't trip the recipient-verification gate.
fn enable_production_access(state: &SharedSesState) {
    let mut accounts = state.write();
    let st = accounts.get_or_create("123456789012");
    st.account_settings.production_access_enabled = true;
}

/// Flip into sandbox mode for tests that explicitly verify the sandbox
/// recipient gate. Default is production access (set in
/// `MultiAccountState::new`).
fn disable_production_access(state: &SharedSesState) {
    let mut accounts = state.write();
    let st = accounts.get_or_create("123456789012");
    st.account_settings.production_access_enabled = false;
}

/// Seed a verified identity for the default test account so send tests
/// don't trip the verified-sender gate.
fn seed_identity(state: &SharedSesState, name: &str) {
    use crate::state::EmailIdentity;
    let mut accounts = state.write();
    let st = accounts.get_or_create("123456789012");
    let identity_type = if name.contains('@') {
        "EMAIL_ADDRESS"
    } else {
        "DOMAIN"
    };
    st.identities.insert(
        name.to_string(),
        EmailIdentity {
            identity_name: name.to_string(),
            identity_type: identity_type.to_string(),
            verified: true,
            created_at: chrono::Utc::now(),
            dkim_signing_enabled: true,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            dkim_public_key_b64: None,
            configuration_set_name: None,
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
        },
    );
}

fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
    make_request_with_query(method, path, body, "", HashMap::new())
}

fn make_request_with_query(
    method: Method,
    path: &str,
    body: &str,
    raw_query: &str,
    query_params: HashMap<String, String>,
) -> AwsRequest {
    let path_segments: Vec<String> = path
        .split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    AwsRequest {
        service: "ses".to_string(),
        action: String::new(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-request-id".to_string(),
        headers: HeaderMap::new(),
        query_params,
        body: Bytes::from(body.to_string()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments,
        raw_path: path.to_string(),
        raw_query: raw_query.to_string(),
        method,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

#[tokio::test]
async fn test_identity_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create identity
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["VerifiedForSendingStatus"], true);
    assert_eq!(body["IdentityType"], "EMAIL_ADDRESS");

    // List identities
    let req = make_request(Method::GET, "/v2/email/identities", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EmailIdentities"].as_array().unwrap().len(), 1);

    // Get identity
    let req = make_request(Method::GET, "/v2/email/identities/test%40example.com", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["VerifiedForSendingStatus"], true);
    assert_eq!(body["DkimAttributes"]["Status"], "SUCCESS");

    // Delete identity
    let req = make_request(
        Method::DELETE,
        "/v2/email/identities/test%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(Method::GET, "/v2/email/identities/test%40example.com", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_domain_identity() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["IdentityType"], "DOMAIN");
}

#[tokio::test]
async fn test_duplicate_identity() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_template_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create template
    let req = make_request(
        Method::POST,
        "/v2/email/templates",
        r#"{"TemplateName": "welcome", "TemplateContent": {"Subject": "Welcome", "Html": "<h1>Hi</h1>", "Text": "Hi"}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get template
    let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TemplateName"], "welcome");
    assert_eq!(body["TemplateContent"]["Subject"], "Welcome");

    // Update template
    let req = make_request(
        Method::PUT,
        "/v2/email/templates/welcome",
        r#"{"TemplateContent": {"Subject": "Updated Welcome"}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify update
    let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TemplateContent"]["Subject"], "Updated Welcome");

    // List templates
    let req = make_request(Method::GET, "/v2/email/templates", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TemplatesMetadata"].as_array().unwrap().len(), 1);

    // Delete template
    let req = make_request(Method::DELETE, "/v2/email/templates/welcome", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(Method::GET, "/v2/email/templates/welcome", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_send_email() {
    let state = make_state();
    enable_production_access(&state);
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "sender@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {
                "ToAddresses": ["recipient@example.com"]
            },
            "Content": {
                "Simple": {
                    "Subject": {"Data": "Test Subject"},
                    "Body": {
                        "Text": {"Data": "Hello world"},
                        "Html": {"Data": "<p>Hello world</p>"}
                    }
                }
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["MessageId"].as_str().is_some());

    // Verify stored
    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert_eq!(s.sent_emails.len(), 1);
    assert_eq!(s.sent_emails[0].from, "sender@example.com");
    assert_eq!(s.sent_emails[0].to, vec!["recipient@example.com"]);
    assert_eq!(s.sent_emails[0].subject.as_deref(), Some("Test Subject"));
    let sig = s.sent_emails[0].dkim_signature.as_deref().unwrap();
    assert!(sig.contains("v=1"));
    assert!(sig.contains("d=example.com"));
    assert!(sig.contains("a=rsa-sha256"));
}

#[tokio::test]
async fn test_send_email_skips_dkim_when_signing_disabled() {
    let state = make_state();
    seed_identity(&state, "plain@example.com");
    enable_production_access(&state);
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        let id = st.identities.get_mut("plain@example.com").unwrap();
        id.dkim_signing_enabled = false;
    }
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "plain@example.com",
            "Destination": {"ToAddresses": ["a@b.com"]},
            "Content": {"Simple": {"Subject": {"Data": "x"}, "Body": {"Text": {"Data": "y"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let mas_r = state.read();
    let s = mas_r.default_ref();
    assert_eq!(s.sent_emails.len(), 1);
    assert!(s.sent_emails[0].dkim_signature.is_none());
}

#[tokio::test]
async fn test_get_account() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(Method::GET, "/v2/email/account", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SendingEnabled"], true);
    assert!(body["SendQuota"]["Max24HourSend"].as_f64().unwrap() > 0.0);
}

#[tokio::test]
async fn test_configuration_set_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create
    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get
    let req = make_request(Method::GET, "/v2/email/configuration-sets/my-config", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ConfigurationSetName"], "my-config");

    // List
    let req = make_request(Method::GET, "/v2/email/configuration-sets", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ConfigurationSets"].as_array().unwrap().len(), 1);

    // Delete
    let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(Method::GET, "/v2/email/configuration-sets/my-config", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_send_email_raw_content() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {
                "ToAddresses": ["to@example.com"]
            },
            "Content": {
                "Raw": {
                    "Data": "From: sender@example.com\r\nTo: to@example.com\r\nSubject: Raw\r\n\r\nBody"
                }
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["MessageId"].as_str().is_some());

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert_eq!(s.sent_emails.len(), 1);
    assert!(s.sent_emails[0].raw_data.is_some());
    assert!(
        s.sent_emails[0].subject.is_none(),
        "Raw emails should not have parsed subject"
    );
}

#[tokio::test]
async fn test_send_email_template_content() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        use crate::state::EmailTemplate;
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.templates.insert(
            "welcome".to_string(),
            EmailTemplate {
                template_name: "welcome".to_string(),
                subject: Some("Hi {{name}}".to_string()),
                html_body: None,
                text_body: None,
                created_at: chrono::Utc::now(),
            },
        );
    }
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {
                "ToAddresses": ["to@example.com"]
            },
            "Content": {
                "Template": {
                    "TemplateName": "welcome",
                    "TemplateData": "{\"name\": \"Alice\"}"
                }
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert_eq!(s.sent_emails.len(), 1);
    assert_eq!(s.sent_emails[0].template_name.as_deref(), Some("welcome"));
    assert_eq!(
        s.sent_emails[0].template_data.as_deref(),
        Some("{\"name\": \"Alice\"}")
    );
}

#[tokio::test]
async fn test_send_email_template_renders_subject_and_body() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        use crate::state::EmailTemplate;
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.templates.insert(
            "welcome".to_string(),
            EmailTemplate {
                template_name: "welcome".to_string(),
                subject: Some("Hi {{name}}".to_string()),
                html_body: Some("<p>Hi {{name}}</p>".to_string()),
                text_body: Some("Hi {{name}}".to_string()),
                created_at: chrono::Utc::now(),
            },
        );
    }
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["to@example.com"]},
            "Content": {
                "Template": {
                    "TemplateName": "welcome",
                    "TemplateData": "{\"name\": \"Alice\"}"
                }
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let mas_r = state.read();
    let s = mas_r.default_ref();
    let sent = &s.sent_emails[0];
    assert_eq!(sent.subject.as_deref(), Some("Hi Alice"));
    assert_eq!(sent.html_body.as_deref(), Some("<p>Hi Alice</p>"));
    assert_eq!(sent.text_body.as_deref(), Some("Hi Alice"));
    assert_eq!(sent.template_name.as_deref(), Some("welcome"));
}

#[tokio::test]
async fn test_send_email_missing_content() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{"FromEmailAddress": "sender@example.com", "Destination": {"ToAddresses": ["to@example.com"]}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_send_email_with_cc_and_bcc() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {
                "ToAddresses": ["to@example.com"],
                "CcAddresses": ["cc@example.com"],
                "BccAddresses": ["bcc@example.com"]
            },
            "Content": {
                "Simple": {
                    "Subject": {"Data": "Test"},
                    "Body": {"Text": {"Data": "Hello"}}
                }
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert_eq!(s.sent_emails[0].cc, vec!["cc@example.com"]);
    assert_eq!(s.sent_emails[0].bcc, vec!["bcc@example.com"]);
}

#[tokio::test]
async fn test_send_bulk_email() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        use crate::state::EmailTemplate;
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.templates.insert(
            "bulk-template".to_string(),
            EmailTemplate {
                template_name: "bulk-template".to_string(),
                subject: Some("hi".to_string()),
                html_body: None,
                text_body: None,
                created_at: chrono::Utc::now(),
            },
        );
    }
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-bulk-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "DefaultContent": {
                "Template": {
                    "TemplateName": "bulk-template",
                    "TemplateData": "{\"default\": true}"
                }
            },
            "BulkEmailEntries": [
                {"Destination": {"ToAddresses": ["a@example.com"]}},
                {"Destination": {"ToAddresses": ["b@example.com"]}}
            ]
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let results = body["BulkEmailEntryResults"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["Status"], "SUCCESS");
    assert_eq!(results[1]["Status"], "SUCCESS");

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert_eq!(s.sent_emails.len(), 2);
    assert_eq!(s.sent_emails[0].to, vec!["a@example.com"]);
    assert_eq!(s.sent_emails[1].to, vec!["b@example.com"]);
}

#[tokio::test]
async fn test_send_email_rejects_when_account_paused() {
    let state = make_state();
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.account_settings.sending_enabled = false;
    }
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["r@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["__type"], "SendingPausedException");
}

#[tokio::test]
async fn test_send_email_rejects_when_config_set_paused() {
    let state = make_state();
    {
        use crate::state::ConfigurationSet;
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.configuration_sets.insert(
            "paused".to_string(),
            ConfigurationSet {
                name: "paused".to_string(),
                sending_enabled: false,
                tls_policy: "OPTIONAL".to_string(),
                sending_pool_name: None,
                custom_redirect_domain: None,
                https_policy: None,
                suppressed_reasons: Vec::new(),
                reputation_metrics_enabled: false,
                vdm_options: None,
                archive_arn: None,
                archiving_options_present: false,
            },
        );
    }
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["r@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}},
            "ConfigurationSetName": "paused"
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["__type"], "SendingPausedException");
}

#[tokio::test]
async fn test_send_email_rejects_unverified_sender() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // No identity registered for sender@example.com → v2 surfaces this
    // as MailFromDomainNotVerifiedException.
    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["recipient@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["__type"], "MailFromDomainNotVerifiedException");
}

#[tokio::test]
async fn test_send_email_accepts_verified_domain() {
    let state = make_state();
    // Verify the domain only → both sender and (in sandbox) recipient
    // resolve through the same verified domain identity.
    seed_identity(&state, "example.com");
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["recipient@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn send_email_v2_rejects_unverified_from_in_sandbox() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "noreply@example.com",
            "Destination": {"ToAddresses": ["someone@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["__type"], "MailFromDomainNotVerifiedException");
}

#[tokio::test]
async fn send_email_v2_accepts_verified_from_when_recipient_also_verified_in_sandbox() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    seed_identity(&state, "recipient@example.com");
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["recipient@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn send_email_v2_rejects_unverified_recipient_in_sandbox() {
    let state = make_state();
    disable_production_access(&state);
    seed_identity(&state, "sender@example.com");
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["unverified@elsewhere.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["__type"], "MessageRejected");
    assert!(body["message"]
        .as_str()
        .unwrap_or("")
        .contains("unverified@elsewhere.com"));
}

#[tokio::test]
async fn send_email_v2_skips_recipient_check_in_production() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["unverified@elsewhere.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn send_email_v2_accepts_verified_domain_when_from_uses_subdomain_or_address() {
    let state = make_state();
    seed_identity(&state, "example.com");
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "bot@example.com",
            "Destination": {"ToAddresses": ["other@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn send_email_v2_accepts_simulator_recipients_in_sandbox() {
    // Real SES treats `*@simulator.amazonses.com` as always-verified so
    // bounce/complaint/suppression flows can be exercised from sandbox
    // accounts without registering the domain as a verified identity.
    let state = make_state();
    disable_production_access(&state);
    seed_identity(&state, "sender@example.com");
    let svc = SesV2Service::new(state);

    for recipient in [
        "bounce@simulator.amazonses.com",
        "complaint@simulator.amazonses.com",
        "success@simulator.amazonses.com",
        "suppressionlist@simulator.amazonses.com",
    ] {
        let body = format!(
            r#"{{
                "FromEmailAddress": "sender@example.com",
                "Destination": {{"ToAddresses": ["{recipient}"]}},
                "Content": {{"Simple": {{"Subject": {{"Data": "S"}}, "Body": {{"Text": {{"Data": "B"}}}}}}}}
            }}"#
        );
        let req = make_request(Method::POST, "/v2/email/outbound-emails", &body);
        let resp = svc.handle(req).await.unwrap();
        assert_eq!(
            resp.status,
            StatusCode::OK,
            "simulator recipient {recipient} should bypass the gate"
        );
    }
}

#[tokio::test]
async fn send_email_v2_accepts_simulator_sender_without_verified_identity() {
    // Sending from the simulator domain itself should succeed even with no
    // verified identities on the account — matches AWS docs that call out
    // the domain as "always verified" for both sender and recipient.
    let state = make_state();
    disable_production_access(&state);
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "ooto@simulator.amazonses.com",
            "Destination": {"ToAddresses": ["bounce@simulator.amazonses.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn test_send_bulk_email_empty_entries() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-bulk-emails",
        r#"{"FromEmailAddress": "s@example.com", "BulkEmailEntries": []}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_delete_nonexistent_identity() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::DELETE,
        "/v2/email/identities/nobody%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_duplicate_configuration_set() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "dup-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "dup-config"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_duplicate_template() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/templates",
        r#"{"TemplateName": "dup-tmpl", "TemplateContent": {}}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/templates",
        r#"{"TemplateName": "dup-tmpl", "TemplateContent": {}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_delete_nonexistent_template() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(Method::DELETE, "/v2/email/templates/nope", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_nonexistent_configuration_set() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(Method::DELETE, "/v2/email/configuration-sets/nope", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_unknown_route() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(Method::GET, "/v2/email/unknown-resource", "");
    let result = svc.handle(req).await;
    assert!(result.is_err(), "Unknown route should return error");
}

#[tokio::test]
async fn test_update_nonexistent_template() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/templates/nonexistent",
        r#"{"TemplateContent": {"Subject": "Updated"}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_invalid_json_body() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(Method::POST, "/v2/email/identities", "not valid json {{{");
    let result = svc.handle(req).await;
    assert!(result.is_err(), "Invalid JSON body should return error");
}

#[tokio::test]
async fn test_create_identity_missing_name() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(Method::POST, "/v2/email/identities", r#"{}"#);
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

// --- Contact List tests ---

#[tokio::test]
async fn test_contact_list_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create contact list with topics
    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{
            "ContactListName": "my-list",
            "Description": "Test list",
            "Topics": [
                {
                    "TopicName": "newsletters",
                    "DisplayName": "Newsletters",
                    "Description": "Weekly newsletters",
                    "DefaultSubscriptionStatus": "OPT_IN"
                }
            ]
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get contact list
    let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ContactListName"], "my-list");
    assert_eq!(body["Description"], "Test list");
    assert_eq!(body["Topics"][0]["TopicName"], "newsletters");
    assert_eq!(body["Topics"][0]["DefaultSubscriptionStatus"], "OPT_IN");
    assert!(body["CreatedTimestamp"].as_f64().is_some());
    assert!(body["LastUpdatedTimestamp"].as_f64().is_some());

    // List contact lists
    let req = make_request(Method::GET, "/v2/email/contact-lists", "{}");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ContactLists"].as_array().unwrap().len(), 1);
    assert_eq!(body["ContactLists"][0]["ContactListName"], "my-list");

    // Update contact list
    let req = make_request(
        Method::PUT,
        "/v2/email/contact-lists/my-list",
        r#"{
            "Description": "Updated description",
            "Topics": [
                {
                    "TopicName": "newsletters",
                    "DisplayName": "Updated Newsletters",
                    "Description": "Updated desc",
                    "DefaultSubscriptionStatus": "OPT_OUT"
                },
                {
                    "TopicName": "promotions",
                    "DisplayName": "Promotions",
                    "Description": "Promo emails",
                    "DefaultSubscriptionStatus": "OPT_OUT"
                }
            ]
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify update
    let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Description"], "Updated description");
    assert_eq!(body["Topics"].as_array().unwrap().len(), 2);

    // Delete contact list
    let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(Method::GET, "/v2/email/contact-lists/my-list", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_duplicate_contact_list() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "dup-list"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "dup-list"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_contact_list_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(Method::GET, "/v2/email/contact-lists/nonexistent", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

// --- Contact tests ---

#[tokio::test]
async fn test_contact_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create contact list first
    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{
            "ContactListName": "my-list",
            "Topics": [
                {
                    "TopicName": "newsletters",
                    "DisplayName": "Newsletters",
                    "Description": "Weekly newsletters",
                    "DefaultSubscriptionStatus": "OPT_OUT"
                }
            ]
        }"#,
    );
    svc.handle(req).await.unwrap();

    // Create contact
    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists/my-list/contacts",
        r#"{
            "EmailAddress": "user@example.com",
            "TopicPreferences": [
                {"TopicName": "newsletters", "SubscriptionStatus": "OPT_IN"}
            ],
            "UnsubscribeAll": false
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get contact
    let req = make_request(
        Method::GET,
        "/v2/email/contact-lists/my-list/contacts/user%40example.com",
        "{}",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EmailAddress"], "user@example.com");
    assert_eq!(body["ContactListName"], "my-list");
    assert_eq!(body["UnsubscribeAll"], false);
    assert_eq!(body["TopicPreferences"][0]["TopicName"], "newsletters");
    assert_eq!(body["TopicPreferences"][0]["SubscriptionStatus"], "OPT_IN");
    assert_eq!(
        body["TopicDefaultPreferences"][0]["SubscriptionStatus"],
        "OPT_OUT"
    );
    assert!(body["CreatedTimestamp"].as_f64().is_some());

    // List contacts
    let req = make_request(
        Method::GET,
        "/v2/email/contact-lists/my-list/contacts",
        "{}",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Contacts"].as_array().unwrap().len(), 1);
    assert_eq!(body["Contacts"][0]["EmailAddress"], "user@example.com");

    // Update contact
    let req = make_request(
        Method::PUT,
        "/v2/email/contact-lists/my-list/contacts/user%40example.com",
        r#"{
            "TopicPreferences": [
                {"TopicName": "newsletters", "SubscriptionStatus": "OPT_OUT"}
            ],
            "UnsubscribeAll": true
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify update
    let req = make_request(
        Method::GET,
        "/v2/email/contact-lists/my-list/contacts/user%40example.com",
        "{}",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["UnsubscribeAll"], true);
    assert_eq!(body["TopicPreferences"][0]["SubscriptionStatus"], "OPT_OUT");

    // Delete contact
    let req = make_request(
        Method::DELETE,
        "/v2/email/contact-lists/my-list/contacts/user%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(
        Method::GET,
        "/v2/email/contact-lists/my-list/contacts/user%40example.com",
        "{}",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_duplicate_contact() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "my-list"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists/my-list/contacts",
        r#"{"EmailAddress": "dup@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists/my-list/contacts",
        r#"{"EmailAddress": "dup@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_contact_in_nonexistent_list() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists/no-such-list/contacts",
        r#"{"EmailAddress": "user@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_get_nonexistent_contact() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "my-list"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::GET,
        "/v2/email/contact-lists/my-list/contacts/nobody%40example.com",
        "{}",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_contact_list_cascades_contacts() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    // Create list and contact
    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "my-list"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists/my-list/contacts",
        r#"{"EmailAddress": "user@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    // Delete the contact list
    let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
    svc.handle(req).await.unwrap();

    // Verify contacts map is cleaned up
    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert!(!s.contacts.contains_key("my-list"));
}

#[tokio::test]
async fn test_tag_resource() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    // Create an identity
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    // Tag it
    let req = make_request(
        Method::POST,
        "/v2/email/tags",
        r#"{"ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com", "Tags": [{"Key": "env", "Value": "prod"}, {"Key": "team", "Value": "backend"}]}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // List tags
    let mut qp = HashMap::new();
    qp.insert(
        "ResourceArn".to_string(),
        "arn:aws:ses:us-east-1:123456789012:identity/test@example.com".to_string(),
    );
    let req = make_request_with_query(
        Method::GET,
        "/v2/email/tags",
        "",
        "ResourceArn=arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com",
        qp,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 2);
}

#[tokio::test]
async fn test_untag_resource() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    // Create an identity
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let arn = "arn:aws:ses:us-east-1:123456789012:identity/test@example.com";

    // Tag it
    let req = make_request(
        Method::POST,
        "/v2/email/tags",
        &format!(
            r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "env", "Value": "prod"}}, {{"Key": "team", "Value": "backend"}}]}}"#
        ),
    );
    svc.handle(req).await.unwrap();

    // Untag - remove "env"
    let mut qp = HashMap::new();
    qp.insert("ResourceArn".to_string(), arn.to_string());
    qp.insert("TagKeys".to_string(), "env".to_string());
    let raw_query = format!("ResourceArn={}&TagKeys=env", urlencoded(arn));
    let req = make_request_with_query(Method::DELETE, "/v2/email/tags", "", &raw_query, qp);
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify only "team" remains
    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    let tags = s.tags.get(arn).unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags.get("team").unwrap(), "backend");
}

#[tokio::test]
async fn test_tag_nonexistent_resource() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/tags",
        r#"{"ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/nope", "Tags": [{"Key": "k", "Value": "v"}]}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_identity_removes_tags() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let arn = "arn:aws:ses:us-east-1:123456789012:identity/test@example.com";
    let req = make_request(
        Method::POST,
        "/v2/email/tags",
        &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
    );
    svc.handle(req).await.unwrap();

    // Delete identity
    let req = make_request(
        Method::DELETE,
        "/v2/email/identities/test%40example.com",
        "",
    );
    svc.handle(req).await.unwrap();

    // Tags should be gone
    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert!(!s.tags.contains_key(arn));
}

#[tokio::test]
async fn test_delete_config_set_removes_tags() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let arn = "arn:aws:ses:us-east-1:123456789012:configuration-set/my-config";
    let req = make_request(
        Method::POST,
        "/v2/email/tags",
        &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
    );
    svc.handle(req).await.unwrap();

    // Delete config set
    let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
    svc.handle(req).await.unwrap();

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert!(!s.tags.contains_key(arn));
}

#[tokio::test]
async fn test_delete_contact_list_removes_tags() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "my-list"}"#,
    );
    svc.handle(req).await.unwrap();

    let arn = "arn:aws:ses:us-east-1:123456789012:contact-list/my-list";
    let req = make_request(
        Method::POST,
        "/v2/email/tags",
        &format!(r#"{{"ResourceArn": "{arn}", "Tags": [{{"Key": "k", "Value": "v"}}]}}"#),
    );
    svc.handle(req).await.unwrap();

    // Delete contact list
    let req = make_request(Method::DELETE, "/v2/email/contact-lists/my-list", "");
    svc.handle(req).await.unwrap();

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert!(!s.tags.contains_key(arn));
}

fn urlencoded(s: &str) -> String {
    form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

// --- Suppression List tests ---

#[tokio::test]
async fn test_suppressed_destination_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Put suppressed destination
    let req = make_request(
        Method::PUT,
        "/v2/email/suppression/addresses",
        r#"{"EmailAddress": "bounce@example.com", "Reason": "BOUNCE"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get suppressed destination
    let req = make_request(
        Method::GET,
        "/v2/email/suppression/addresses/bounce%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["SuppressedDestination"]["EmailAddress"],
        "bounce@example.com"
    );
    assert_eq!(body["SuppressedDestination"]["Reason"], "BOUNCE");
    assert!(body["SuppressedDestination"]["LastUpdateTime"]
        .as_f64()
        .is_some());

    // List suppressed destinations
    let req = make_request(Method::GET, "/v2/email/suppression/addresses", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["SuppressedDestinationSummaries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // Delete suppressed destination
    let req = make_request(
        Method::DELETE,
        "/v2/email/suppression/addresses/bounce%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(
        Method::GET,
        "/v2/email/suppression/addresses/bounce%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_suppressed_destination_complaint() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/suppression/addresses",
        r#"{"EmailAddress": "complaint@example.com", "Reason": "COMPLAINT"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(
        Method::GET,
        "/v2/email/suppression/addresses/complaint%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SuppressedDestination"]["Reason"], "COMPLAINT");
}

#[tokio::test]
async fn test_suppressed_destination_invalid_reason() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/suppression/addresses",
        r#"{"EmailAddress": "bad@example.com", "Reason": "INVALID"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_suppressed_destination_upsert() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Put with BOUNCE
    let req = make_request(
        Method::PUT,
        "/v2/email/suppression/addresses",
        r#"{"EmailAddress": "user@example.com", "Reason": "BOUNCE"}"#,
    );
    svc.handle(req).await.unwrap();

    // Put again with COMPLAINT (upsert)
    let req = make_request(
        Method::PUT,
        "/v2/email/suppression/addresses",
        r#"{"EmailAddress": "user@example.com", "Reason": "COMPLAINT"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::GET,
        "/v2/email/suppression/addresses/user%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SuppressedDestination"]["Reason"], "COMPLAINT");
}

#[tokio::test]
async fn test_delete_nonexistent_suppressed_destination() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::DELETE,
        "/v2/email/suppression/addresses/nobody%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

// --- Event Destination tests ---

#[tokio::test]
async fn test_event_destination_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create config set first
    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    svc.handle(req).await.unwrap();

    // Create event destination
    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets/my-config/event-destinations",
        r#"{
            "EventDestinationName": "my-dest",
            "EventDestination": {
                "Enabled": true,
                "MatchingEventTypes": ["SEND", "BOUNCE"],
                "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:my-topic"}
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get event destinations
    let req = make_request(
        Method::GET,
        "/v2/email/configuration-sets/my-config/event-destinations",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let dests = body["EventDestinations"].as_array().unwrap();
    assert_eq!(dests.len(), 1);
    assert_eq!(dests[0]["Name"], "my-dest");
    assert_eq!(dests[0]["Enabled"], true);
    assert_eq!(dests[0]["MatchingEventTypes"], json!(["SEND", "BOUNCE"]));
    assert_eq!(
        dests[0]["SnsDestination"]["TopicArn"],
        "arn:aws:sns:us-east-1:123456789012:my-topic"
    );

    // Update event destination
    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/my-config/event-destinations/my-dest",
        r#"{
            "EventDestination": {
                "Enabled": false,
                "MatchingEventTypes": ["DELIVERY"],
                "SnsDestination": {"TopicArn": "arn:aws:sns:us-east-1:123456789012:updated-topic"}
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify update
    let req = make_request(
        Method::GET,
        "/v2/email/configuration-sets/my-config/event-destinations",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let dests = body["EventDestinations"].as_array().unwrap();
    assert_eq!(dests[0]["Enabled"], false);
    assert_eq!(dests[0]["MatchingEventTypes"], json!(["DELIVERY"]));

    // Delete event destination
    let req = make_request(
        Method::DELETE,
        "/v2/email/configuration-sets/my-config/event-destinations/my-dest",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(
        Method::GET,
        "/v2/email/configuration-sets/my-config/event-destinations",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["EventDestinations"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_event_destination_config_set_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets/nonexistent/event-destinations",
        r#"{
            "EventDestinationName": "dest",
            "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_event_destination_duplicate() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let body = r#"{
        "EventDestinationName": "dup-dest",
        "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
    }"#;

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets/my-config/event-destinations",
        body,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets/my-config/event-destinations",
        body,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_update_nonexistent_event_destination() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/my-config/event-destinations/nonexistent",
        r#"{"EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_nonexistent_event_destination() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::DELETE,
        "/v2/email/configuration-sets/my-config/event-destinations/nonexistent",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_event_destinations_cleaned_on_config_set_delete() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets/my-config/event-destinations",
        r#"{
            "EventDestinationName": "dest1",
            "EventDestination": {"Enabled": true, "MatchingEventTypes": ["SEND"]}
        }"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::DELETE, "/v2/email/configuration-sets/my-config", "");
    svc.handle(req).await.unwrap();

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert!(!s.event_destinations.contains_key("my-config"));
}

// --- Email Identity Policy tests ---

#[tokio::test]
async fn test_identity_policy_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create identity first
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    // Create policy
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"ses:SendEmail","Resource":"*"}]}"#;
    let req = make_request(
        Method::POST,
        "/v2/email/identities/test%40example.com/policies/my-policy",
        &format!(
            r#"{{"Policy": {}}}"#,
            serde_json::to_string(policy_doc).unwrap()
        ),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get policies
    let req = make_request(
        Method::GET,
        "/v2/email/identities/test%40example.com/policies",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Policies"]["my-policy"].is_string());
    assert_eq!(body["Policies"]["my-policy"].as_str().unwrap(), policy_doc);

    // Update policy
    let updated_doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
    let req = make_request(
        Method::PUT,
        "/v2/email/identities/test%40example.com/policies/my-policy",
        &format!(
            r#"{{"Policy": {}}}"#,
            serde_json::to_string(updated_doc).unwrap()
        ),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify update
    let req = make_request(
        Method::GET,
        "/v2/email/identities/test%40example.com/policies",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Policies"]["my-policy"].as_str().unwrap(), updated_doc);

    // Delete policy
    let req = make_request(
        Method::DELETE,
        "/v2/email/identities/test%40example.com/policies/my-policy",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(
        Method::GET,
        "/v2/email/identities/test%40example.com/policies",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Policies"].as_object().unwrap().is_empty());
}

#[tokio::test]
async fn test_identity_policy_identity_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/identities/nonexistent%40example.com/policies/my-policy",
        r#"{"Policy": "{}"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_identity_policy_duplicate() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/identities/test%40example.com/policies/my-policy",
        r#"{"Policy": "{}"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/identities/test%40example.com/policies/my-policy",
        r#"{"Policy": "{}"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_update_nonexistent_policy() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/identities/test%40example.com/policies/nonexistent",
        r#"{"Policy": "{}"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_delete_nonexistent_policy() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::DELETE,
        "/v2/email/identities/test%40example.com/policies/nonexistent",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_policies_cleaned_on_identity_delete() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/identities/test%40example.com/policies/my-policy",
        r#"{"Policy": "{}"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::DELETE,
        "/v2/email/identities/test%40example.com",
        "",
    );
    svc.handle(req).await.unwrap();

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert!(!s.identity_policies.contains_key("test@example.com"));
}

// --- Identity Attribute tests ---

#[tokio::test]
async fn test_put_email_identity_dkim_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    // Create identity first
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    // Disable DKIM signing
    let req = make_request(
        Method::PUT,
        "/v2/email/identities/example.com/dkim",
        r#"{"SigningEnabled": false}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify via GetEmailIdentity
    let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DkimAttributes"]["SigningEnabled"], false);
}

#[tokio::test]
async fn test_put_email_identity_dkim_attributes_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/identities/nonexistent.com/dkim",
        r#"{"SigningEnabled": false}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_put_email_identity_dkim_signing_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/identities/example.com/dkim/signing",
        r#"{"SigningAttributesOrigin": "EXTERNAL", "SigningAttributes": {"DomainSigningPrivateKey": "key123", "DomainSigningSelector": "sel1"}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DkimStatus"], "SUCCESS");
    assert!(!body["DkimTokens"].as_array().unwrap().is_empty());

    // Verify stored
    let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["DkimAttributes"]["SigningAttributesOrigin"],
        "EXTERNAL"
    );
}

#[tokio::test]
async fn test_put_email_identity_feedback_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "test@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/identities/test%40example.com/feedback",
        r#"{"EmailForwardingEnabled": false}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/identities/test%40example.com", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["FeedbackForwardingStatus"], false);
}

#[tokio::test]
async fn test_put_email_identity_mail_from_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/identities/example.com/mail-from",
        r#"{"MailFromDomain": "mail.example.com", "BehaviorOnMxFailure": "REJECT_MESSAGE"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["MailFromAttributes"]["MailFromDomain"],
        "mail.example.com"
    );
    assert_eq!(
        body["MailFromAttributes"]["BehaviorOnMxFailure"],
        "REJECT_MESSAGE"
    );
}

#[tokio::test]
async fn test_mail_from_status_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    // No mail-from configured -> NotStarted, no DNS records.
    let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["MailFromAttributes"]["MailFromDomainStatus"],
        "NotStarted"
    );
    assert!(body["MailFromAttributes"]["MailFromDomainDnsRecords"].is_null());

    // Set mail-from -> first read auto-advances Pending -> Success +
    // emits expected MX/TXT records.
    let req = make_request(
        Method::PUT,
        "/v2/email/identities/example.com/mail-from",
        r#"{"MailFromDomain": "mail.example.com", "BehaviorOnMxFailure": "USE_DEFAULT_VALUE"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["MailFromAttributes"]["MailFromDomainStatus"],
        "Success"
    );
    let dns = &body["MailFromAttributes"]["MailFromDomainDnsRecords"];
    assert_eq!(dns.as_array().map(|a| a.len()), Some(2));
    assert_eq!(dns[0]["Name"], "mail.example.com");
    assert_eq!(dns[0]["Type"], "MX");
    assert_eq!(dns[1]["Type"], "TXT");
}

#[tokio::test]
async fn test_put_email_identity_configuration_set_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/identities/example.com/configuration-set",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/identities/example.com", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ConfigurationSetName"], "my-config");
}

// --- Configuration Set Options tests ---

#[tokio::test]
async fn test_put_configuration_set_sending_options() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create config set
    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "test-config"}"#,
    );
    svc.handle(req).await.unwrap();

    // Disable sending
    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/test-config/sending",
        r#"{"SendingEnabled": false}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify
    let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SendingOptions"]["SendingEnabled"], false);
}

#[tokio::test]
async fn test_put_configuration_set_sending_options_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/nonexistent/sending",
        r#"{"SendingEnabled": false}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_put_configuration_set_delivery_options() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "test-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/test-config/delivery-options",
        r#"{"TlsPolicy": "REQUIRE", "SendingPoolName": "my-pool"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DeliveryOptions"]["TlsPolicy"], "REQUIRE");
    assert_eq!(body["DeliveryOptions"]["SendingPoolName"], "my-pool");
}

#[tokio::test]
async fn test_put_configuration_set_tracking_options() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "test-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/test-config/tracking-options",
        r#"{"CustomRedirectDomain": "track.example.com", "HttpsPolicy": "REQUIRE"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["TrackingOptions"]["CustomRedirectDomain"],
        "track.example.com"
    );
    assert_eq!(body["TrackingOptions"]["HttpsPolicy"], "REQUIRE");
}

#[tokio::test]
async fn test_put_configuration_set_suppression_options() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "test-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/test-config/suppression-options",
        r#"{"SuppressedReasons": ["BOUNCE", "COMPLAINT"]}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let reasons = body["SuppressionOptions"]["SuppressedReasons"]
        .as_array()
        .unwrap();
    assert_eq!(reasons.len(), 2);
}

#[tokio::test]
async fn test_put_configuration_set_reputation_options() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "test-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/test-config/reputation-options",
        r#"{"ReputationMetricsEnabled": true}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ReputationOptions"]["ReputationMetricsEnabled"], true);
}

#[tokio::test]
async fn test_put_configuration_set_vdm_options() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "test-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/test-config/vdm-options",
        r#"{"DashboardOptions": {"EngagementMetrics": "ENABLED"}, "GuardianOptions": {"OptimizedSharedDelivery": "ENABLED"}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["VdmOptions"]["DashboardOptions"]["EngagementMetrics"],
        "ENABLED"
    );
}

#[tokio::test]
async fn test_put_configuration_set_archiving_options() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "test-config"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::PUT,
        "/v2/email/configuration-sets/test-config/archiving-options",
        r#"{"ArchiveArn": "arn:aws:ses:us-east-1:123456789012:mailmanager-archive/my-archive"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/configuration-sets/test-config", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["ArchivingOptions"]["ArchiveArn"]
        .as_str()
        .unwrap()
        .contains("my-archive"));
}

// --- Custom Verification Email Template tests ---

#[tokio::test]
async fn test_custom_verification_email_template_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create
    let req = make_request(
        Method::POST,
        "/v2/email/custom-verification-email-templates",
        r#"{
            "TemplateName": "my-verification",
            "FromEmailAddress": "noreply@example.com",
            "TemplateSubject": "Verify your email",
            "TemplateContent": "<h1>Please verify</h1>",
            "SuccessRedirectionURL": "https://example.com/success",
            "FailureRedirectionURL": "https://example.com/failure"
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get
    let req = make_request(
        Method::GET,
        "/v2/email/custom-verification-email-templates/my-verification",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TemplateName"], "my-verification");
    assert_eq!(body["FromEmailAddress"], "noreply@example.com");
    assert_eq!(body["TemplateSubject"], "Verify your email");
    assert_eq!(body["TemplateContent"], "<h1>Please verify</h1>");
    assert_eq!(body["SuccessRedirectionURL"], "https://example.com/success");
    assert_eq!(body["FailureRedirectionURL"], "https://example.com/failure");

    // List
    let req = make_request(
        Method::GET,
        "/v2/email/custom-verification-email-templates",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["CustomVerificationEmailTemplates"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // Update
    let req = make_request(
        Method::PUT,
        "/v2/email/custom-verification-email-templates/my-verification",
        r#"{"TemplateSubject": "Updated subject"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify update
    let req = make_request(
        Method::GET,
        "/v2/email/custom-verification-email-templates/my-verification",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TemplateSubject"], "Updated subject");

    // Delete
    let req = make_request(
        Method::DELETE,
        "/v2/email/custom-verification-email-templates/my-verification",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(
        Method::GET,
        "/v2/email/custom-verification-email-templates/my-verification",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_duplicate_custom_verification_template() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let body = r#"{
        "TemplateName": "dup-tmpl",
        "FromEmailAddress": "a@b.com",
        "TemplateSubject": "s",
        "TemplateContent": "c",
        "SuccessRedirectionURL": "https://ok",
        "FailureRedirectionURL": "https://fail"
    }"#;

    let req = make_request(
        Method::POST,
        "/v2/email/custom-verification-email-templates",
        body,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/custom-verification-email-templates",
        body,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_send_custom_verification_email() {
    let state = make_state();
    // Seed verified sender so the verified-identity gate lets the send through.
    seed_identity(&state, "a@b.com");
    let svc = SesV2Service::new(state.clone());

    // Create template first
    let req = make_request(
        Method::POST,
        "/v2/email/custom-verification-email-templates",
        r#"{
            "TemplateName": "verify",
            "FromEmailAddress": "a@b.com",
            "TemplateSubject": "Verify",
            "TemplateContent": "content",
            "SuccessRedirectionURL": "https://ok",
            "FailureRedirectionURL": "https://fail"
        }"#,
    );
    svc.handle(req).await.unwrap();

    // Send
    let req = make_request(
        Method::POST,
        "/v2/email/outbound-custom-verification-emails",
        r#"{"EmailAddress": "user@example.com", "TemplateName": "verify"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["MessageId"].as_str().is_some());

    // Verify stored in sent_emails with the resolved sender on the record.
    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert_eq!(s.sent_emails.len(), 1);
    assert_eq!(s.sent_emails[0].to, vec!["user@example.com"]);
    assert_eq!(s.sent_emails[0].from, "a@b.com");
}

#[tokio::test]
async fn test_send_custom_verification_email_unverified_sender() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Template's FromEmailAddress has no matching verified identity.
    let req = make_request(
        Method::POST,
        "/v2/email/custom-verification-email-templates",
        r#"{
            "TemplateName": "verify",
            "FromEmailAddress": "unverified@nowhere.com",
            "TemplateSubject": "Verify",
            "TemplateContent": "content",
            "SuccessRedirectionURL": "https://ok",
            "FailureRedirectionURL": "https://fail"
        }"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-custom-verification-emails",
        r#"{"EmailAddress": "user@example.com", "TemplateName": "verify"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["__type"].as_str(),
        Some("MailFromDomainNotVerifiedException")
    );
}

#[tokio::test]
async fn test_send_custom_verification_email_domain_verified_sender() {
    let state = make_state();
    // Verifying the domain implicitly verifies all addresses on it.
    seed_identity(&state, "example.com");
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/custom-verification-email-templates",
        r#"{
            "TemplateName": "verify",
            "FromEmailAddress": "noreply@example.com",
            "TemplateSubject": "Verify",
            "TemplateContent": "content",
            "SuccessRedirectionURL": "https://ok",
            "FailureRedirectionURL": "https://fail"
        }"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-custom-verification-emails",
        r#"{"EmailAddress": "user@example.com", "TemplateName": "verify"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert_eq!(s.sent_emails.len(), 1);
    assert_eq!(s.sent_emails[0].from, "noreply@example.com");
}

#[tokio::test]
async fn test_send_custom_verification_email_template_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-custom-verification-emails",
        r#"{"EmailAddress": "user@example.com", "TemplateName": "nonexistent"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

// --- TestRenderEmailTemplate tests ---

#[tokio::test]
async fn test_render_email_template() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create template
    let req = make_request(
        Method::POST,
        "/v2/email/templates",
        r#"{
            "TemplateName": "greet",
            "TemplateContent": {
                "Subject": "Hello {{name}}",
                "Html": "<h1>Welcome, {{name}}!</h1><p>Your code is {{code}}.</p>",
                "Text": "Welcome, {{name}}! Your code is {{code}}."
            }
        }"#,
    );
    svc.handle(req).await.unwrap();

    // Render
    let req = make_request(
        Method::POST,
        "/v2/email/templates/greet/render",
        r#"{"TemplateData": "{\"name\": \"Alice\", \"code\": \"1234\"}"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let rendered = body["RenderedTemplate"].as_str().unwrap();
    assert!(rendered.contains("Subject: Hello Alice"));
    assert!(rendered.contains("Welcome, Alice!"));
    assert!(rendered.contains("Your code is 1234."));
}

#[tokio::test]
async fn test_render_email_template_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/templates/nonexistent/render",
        r#"{"TemplateData": "{}"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_render_email_template_missing_data() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create template
    let req = make_request(
        Method::POST,
        "/v2/email/templates",
        r#"{"TemplateName": "t1", "TemplateContent": {"Subject": "Hi"}}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::POST, "/v2/email/templates/t1/render", r#"{}"#);
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

// ── Dedicated IP Pool tests ─────────────────────────────────────────

#[tokio::test]
async fn test_dedicated_ip_pool_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create pool
    let req = make_request(
        Method::POST,
        "/v2/email/dedicated-ip-pools",
        r#"{"PoolName": "my-pool", "ScalingMode": "STANDARD"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // List pools
    let req = make_request(Method::GET, "/v2/email/dedicated-ip-pools", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DedicatedIpPools"].as_array().unwrap().len(), 1);

    // Duplicate
    let req = make_request(
        Method::POST,
        "/v2/email/dedicated-ip-pools",
        r#"{"PoolName": "my-pool"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);

    // Delete pool
    let req = make_request(Method::DELETE, "/v2/email/dedicated-ip-pools/my-pool", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Delete non-existent
    let req = make_request(Method::DELETE, "/v2/email/dedicated-ip-pools/my-pool", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_managed_pool_generates_ips() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create managed pool
    let req = make_request(
        Method::POST,
        "/v2/email/dedicated-ip-pools",
        r#"{"PoolName": "managed-pool", "ScalingMode": "MANAGED"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // List dedicated IPs filtered by pool
    let req = make_request_with_query(
        Method::GET,
        "/v2/email/dedicated-ips",
        "",
        "PoolName=managed-pool",
        {
            let mut m = HashMap::new();
            m.insert("PoolName".to_string(), "managed-pool".to_string());
            m
        },
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let ips = body["DedicatedIps"].as_array().unwrap();
    assert_eq!(ips.len(), 3);
    assert_eq!(ips[0]["WarmupStatus"], "NOT_APPLICABLE");
    assert_eq!(ips[0]["WarmupPercentage"], -1);
}

#[tokio::test]
async fn test_dedicated_ip_operations() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create two pools
    let req = make_request(
        Method::POST,
        "/v2/email/dedicated-ip-pools",
        r#"{"PoolName": "pool-a", "ScalingMode": "MANAGED"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/dedicated-ip-pools",
        r#"{"PoolName": "pool-b", "ScalingMode": "STANDARD"}"#,
    );
    svc.handle(req).await.unwrap();

    // Get a specific IP
    let req = make_request(Method::GET, "/v2/email/dedicated-ips/198.51.100.1", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DedicatedIp"]["PoolName"], "pool-a");

    // Move IP to pool-b
    let req = make_request(
        Method::PUT,
        "/v2/email/dedicated-ips/198.51.100.1/pool",
        r#"{"DestinationPoolName": "pool-b"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify it moved
    let req = make_request(Method::GET, "/v2/email/dedicated-ips/198.51.100.1", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DedicatedIp"]["PoolName"], "pool-b");

    // Set warmup
    let req = make_request(
        Method::PUT,
        "/v2/email/dedicated-ips/198.51.100.1/warmup",
        r#"{"WarmupPercentage": 50}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/dedicated-ips/198.51.100.1", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DedicatedIp"]["WarmupPercentage"], 50);
    assert_eq!(body["DedicatedIp"]["WarmupStatus"], "IN_PROGRESS");

    // Non-existent IP
    let req = make_request(Method::GET, "/v2/email/dedicated-ips/1.2.3.4", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_pool_scaling_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/dedicated-ip-pools",
        r#"{"PoolName": "scalable", "ScalingMode": "STANDARD"}"#,
    );
    svc.handle(req).await.unwrap();

    // Change to MANAGED
    let req = make_request(
        Method::PUT,
        "/v2/email/dedicated-ip-pools/scalable/scaling",
        r#"{"ScalingMode": "MANAGED"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Cannot change from MANAGED to STANDARD
    let req = make_request(
        Method::PUT,
        "/v2/email/dedicated-ip-pools/scalable/scaling",
        r#"{"ScalingMode": "STANDARD"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_account_dedicated_ip_warmup() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/account/dedicated-ips/warmup",
        r#"{"AutoWarmupEnabled": true}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/account", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DedicatedIpAutoWarmupEnabled"], true);
}

// ── Multi-region Endpoint tests ─────────────────────────────────────

#[tokio::test]
async fn test_multi_region_endpoint_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create
    let req = make_request(
        Method::POST,
        "/v2/email/multi-region-endpoints",
        r#"{"EndpointName": "global-ep", "Details": {"RoutesDetails": [{"Region": "us-west-2"}]}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Status"], "READY");
    assert!(body["EndpointId"].as_str().is_some());

    // Get
    let req = make_request(
        Method::GET,
        "/v2/email/multi-region-endpoints/global-ep",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["EndpointName"], "global-ep");
    assert_eq!(body["Status"], "READY");
    let routes = body["Routes"].as_array().unwrap();
    assert!(!routes.is_empty());

    // List
    let req = make_request(Method::GET, "/v2/email/multi-region-endpoints", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["MultiRegionEndpoints"].as_array().unwrap().len(), 1);

    // Duplicate
    let req = make_request(
        Method::POST,
        "/v2/email/multi-region-endpoints",
        r#"{"EndpointName": "global-ep", "Details": {"RoutesDetails": [{"Region": "eu-west-1"}]}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);

    // Delete
    let req = make_request(
        Method::DELETE,
        "/v2/email/multi-region-endpoints/global-ep",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Status"], "DELETING");

    // Get after delete
    let req = make_request(
        Method::GET,
        "/v2/email/multi-region-endpoints/global-ep",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

// ── Account Settings tests ──────────────────────────────────────────

#[tokio::test]
async fn test_account_details() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/account/details",
        r#"{"MailType": "TRANSACTIONAL", "WebsiteURL": "https://example.com", "UseCaseDescription": "Testing"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/account", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Details"]["MailType"], "TRANSACTIONAL");
    assert_eq!(body["Details"]["WebsiteURL"], "https://example.com");
    assert_eq!(body["Details"]["UseCaseDescription"], "Testing");
}

#[tokio::test]
async fn test_account_sending_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Disable sending
    let req = make_request(
        Method::PUT,
        "/v2/email/account/sending",
        r#"{"SendingEnabled": false}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/account", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SendingEnabled"], false);

    // Re-enable
    let req = make_request(
        Method::PUT,
        "/v2/email/account/sending",
        r#"{"SendingEnabled": true}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(Method::GET, "/v2/email/account", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["SendingEnabled"], true);
}

#[tokio::test]
async fn test_account_suppression_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/account/suppression",
        r#"{"SuppressedReasons": ["BOUNCE", "COMPLAINT"]}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/account", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let reasons = body["SuppressionAttributes"]["SuppressedReasons"]
        .as_array()
        .unwrap();
    assert_eq!(reasons.len(), 2);
}

#[tokio::test]
async fn test_account_vdm_attributes() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/account/vdm",
        r#"{"VdmAttributes": {"VdmEnabled": "ENABLED", "DashboardAttributes": {"EngagementMetrics": "ENABLED"}}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/account", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["VdmAttributes"]["VdmEnabled"], "ENABLED");
}

#[tokio::test]
async fn test_import_job_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create import job
    let req = make_request(
        Method::POST,
        "/v2/email/import-jobs",
        r#"{
            "ImportDestination": {
                "SuppressionListDestination": {"SuppressionListImportAction": "PUT"}
            },
            "ImportDataSource": {
                "S3Url": "s3://bucket/file.csv",
                "DataFormat": "CSV"
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let job_id = body["JobId"].as_str().unwrap().to_string();

    // Get import job
    let req = make_request(
        Method::GET,
        &format!("/v2/email/import-jobs/{}", job_id),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["JobId"], job_id);
    assert_eq!(body["JobStatus"], "COMPLETED");

    // List import jobs
    let req = make_request(Method::POST, "/v2/email/import-jobs/list", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ImportJobs"].as_array().unwrap().len(), 1);

    // Get non-existent job
    let req = make_request(Method::GET, "/v2/email/import-jobs/nonexistent", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_export_job_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create export job
    let req = make_request(
        Method::POST,
        "/v2/email/export-jobs",
        r#"{
            "ExportDataSource": {
                "MetricsDataSource": {
                    "Dimensions": {},
                    "Namespace": "VDM",
                    "Metrics": []
                }
            },
            "ExportDestination": {
                "DataFormat": "CSV",
                "S3Url": "s3://bucket/export"
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let job_id = body["JobId"].as_str().unwrap().to_string();

    // Get export job
    let req = make_request(
        Method::GET,
        &format!("/v2/email/export-jobs/{}", job_id),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["JobId"], job_id);
    assert_eq!(body["JobStatus"], "COMPLETED");
    assert_eq!(body["ExportSourceType"], "METRICS_DATA");

    // List export jobs
    let req = make_request(Method::POST, "/v2/email/list-export-jobs", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ExportJobs"].as_array().unwrap().len(), 1);

    // Cancel — should fail since already COMPLETED
    let req = make_request(
        Method::PUT,
        &format!("/v2/email/export-jobs/{}/cancel", job_id),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_tenant_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create tenant
    let req = make_request(
        Method::POST,
        "/v2/email/tenants",
        r#"{"TenantName": "my-tenant"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TenantName"], "my-tenant");
    assert!(body["TenantId"].as_str().is_some());
    assert_eq!(body["SendingStatus"], "ENABLED");

    // Get tenant
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/get",
        r#"{"TenantName": "my-tenant"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Tenant"]["TenantName"], "my-tenant");

    // List tenants
    let req = make_request(Method::POST, "/v2/email/tenants/list", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Tenants"].as_array().unwrap().len(), 1);

    // Create resource association
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/resources",
        r#"{"TenantName": "my-tenant", "ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // List tenant resources
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/resources/list",
        r#"{"TenantName": "my-tenant"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TenantResources"].as_array().unwrap().len(), 1);

    // List resource tenants
    let req = make_request(
        Method::POST,
        "/v2/email/resources/tenants/list",
        r#"{"ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ResourceTenants"].as_array().unwrap().len(), 1);

    // Delete resource association
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/resources/delete",
        r#"{"TenantName": "my-tenant", "ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/test@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify association is gone
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/resources/list",
        r#"{"TenantName": "my-tenant"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["TenantResources"].as_array().unwrap().is_empty());

    // Delete tenant
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/delete",
        r#"{"TenantName": "my-tenant"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify deleted
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/get",
        r#"{"TenantName": "my-tenant"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_reputation_entity() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Get default reputation entity (auto-created)
    let req = make_request(
        Method::GET,
        "/v2/email/reputation/entities/RESOURCE/arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["ReputationEntity"]["SendingStatusAggregate"],
        "ENABLED"
    );

    // Update customer managed status
    let req = make_request(
        Method::PUT,
        "/v2/email/reputation/entities/RESOURCE/arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com/customer-managed-status",
        r#"{"SendingStatus": "DISABLED"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Update policy
    let req = make_request(
        Method::PUT,
        "/v2/email/reputation/entities/RESOURCE/arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com/policy",
        r#"{"ReputationEntityPolicy": "arn:aws:ses:us-east-1:123456789012:policy/my-policy"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify via get
    let req = make_request(
        Method::GET,
        "/v2/email/reputation/entities/RESOURCE/arn%3Aaws%3Ases%3Aus-east-1%3A123456789012%3Aidentity%2Ftest%40example.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["ReputationEntity"]["CustomerManagedStatus"]["Status"],
        "DISABLED"
    );

    // List reputation entities
    let req = make_request(Method::POST, "/v2/email/reputation/entities", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ReputationEntities"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_batch_get_metric_data() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/metrics/batch",
        r#"{
            "Queries": [
                {
                    "Id": "q1",
                    "Namespace": "VDM",
                    "Metric": "SEND",
                    "StartDate": "2024-01-01T00:00:00Z",
                    "EndDate": "2024-01-02T00:00:00Z"
                }
            ]
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Results"].as_array().unwrap().len(), 1);
    assert_eq!(body["Results"][0]["Id"], "q1");
    assert!(body["Errors"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_duplicate_tenant() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/tenants",
        r#"{"TenantName": "dup"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/tenants",
        r#"{"TenantName": "dup"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

// ── contact_lists coverage ──

#[tokio::test]
async fn contact_list_duplicate_conflict() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "newsletter"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "newsletter"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn contact_list_get_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::GET, "/v2/email/contact-lists/ghost", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn contact_list_delete_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::DELETE, "/v2/email/contact-lists/ghost", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn contact_in_nonexistent_list() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists/ghost/contacts",
        r#"{"EmailAddress": "u@x.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn contact_list_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create list
    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "news"}"#,
    );
    svc.handle(req).await.unwrap();

    // Get list
    let req = make_request(Method::GET, "/v2/email/contact-lists/news", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Create contact
    let req = make_request(
        Method::POST,
        "/v2/email/contact-lists/news/contacts",
        r#"{"EmailAddress": "u@x.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // List contacts
    let req = make_request(Method::GET, "/v2/email/contact-lists/news/contacts", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get contact
    let req = make_request(
        Method::GET,
        "/v2/email/contact-lists/news/contacts/u@x.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Delete contact
    let req = make_request(
        Method::DELETE,
        "/v2/email/contact-lists/news/contacts/u@x.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Delete list
    let req = make_request(Method::DELETE, "/v2/email/contact-lists/news", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

// ── templates lifecycle ──

#[tokio::test]
async fn template_duplicate_conflict() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/templates",
        r#"{"TemplateName":"t1","TemplateContent":{"Subject":"s"}}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/templates",
        r#"{"TemplateName":"t1","TemplateContent":{"Subject":"s"}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn template_get_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::GET, "/v2/email/templates/ghost", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

// ── configuration sets ──

#[tokio::test]
async fn configuration_set_duplicate_conflict() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName":"cs1"}"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName":"cs1"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn configuration_set_get_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::GET, "/v2/email/configuration-sets/ghost", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn configuration_set_delete_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::DELETE, "/v2/email/configuration-sets/ghost", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

// ── suppression destination ──

#[tokio::test]
async fn put_suppressed_destination_and_get() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::PUT,
        "/v2/email/suppression/addresses",
        r#"{"EmailAddress":"block@x.com","Reason":"BOUNCE"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(
        Method::GET,
        "/v2/email/suppression/addresses/block@x.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(
        Method::DELETE,
        "/v2/email/suppression/addresses/block@x.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn get_suppressed_destination_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::GET,
        "/v2/email/suppression/addresses/ghost@x.com",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

// ── misc.rs extra coverage (import/export jobs, tenant resources) ─

#[tokio::test]
async fn test_import_job_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::GET, "/v2/email/import-jobs/nope", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_export_job_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::GET, "/v2/email/export-jobs/nope", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_create_import_job_missing_destination() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/import-jobs",
        r#"{"ImportDataSource": {"S3Url": "s3://b/k", "DataFormat": "CSV"}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_create_import_job_missing_data_source() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/import-jobs",
        r#"{"ImportDestination": {"SuppressionListDestination": {"SuppressionListImportAction": "PUT"}}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_list_import_jobs_filter_by_suppression_list() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/import-jobs",
        r#"{
            "ImportDestination": {"SuppressionListDestination": {"SuppressionListImportAction": "PUT"}},
            "ImportDataSource": {"S3Url": "s3://b/k", "DataFormat": "CSV"}
        }"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/import-jobs",
        r#"{
            "ImportDestination": {"ContactListDestination": {"ContactListName": "x", "ContactListImportAction": "PUT"}},
            "ImportDataSource": {"S3Url": "s3://b/k", "DataFormat": "CSV"}
        }"#,
    );
    svc.handle(req).await.unwrap();

    let req = make_request(
        Method::POST,
        "/v2/email/import-jobs/list",
        r#"{"ImportDestinationType": "SUPPRESSION_LIST"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ImportJobs"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_cancel_export_job_conflict() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/export-jobs",
        r#"{
            "ExportDataSource": {"MessageInsightsDataSource": {"StartDate": 0, "EndDate": 0}},
            "ExportDestination": {"DataFormat": "CSV"}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let job_id = body["JobId"].as_str().unwrap().to_string();

    let path = format!("/v2/email/export-jobs/{}/cancel", job_id);
    let req = make_request(Method::PUT, &path, "{}");
    let resp = svc.handle(req).await.unwrap();
    // First cancel: COMPLETED -> Conflict, since jobs finish immediately
    assert_eq!(resp.status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn test_cancel_export_job_not_found() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::PUT, "/v2/email/export-jobs/ghost/cancel", "{}");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_tenant_resource_association_crud() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create tenant
    let req = make_request(
        Method::POST,
        "/v2/email/tenants",
        r#"{"TenantName": "tenant-a"}"#,
    );
    svc.handle(req).await.unwrap();

    // Create identity so resource exists
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "tres@example.com"}"#,
    );
    svc.handle(req).await.unwrap();

    // Associate
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/resources",
        r#"{"TenantName": "tenant-a", "ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/tres@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // List tenant resources
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/resources/list",
        r#"{"TenantName": "tenant-a"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Delete association
    let req = make_request(
        Method::POST,
        "/v2/email/tenants/resources/delete",
        r#"{"TenantName": "tenant-a", "ResourceArn": "arn:aws:ses:us-east-1:123456789012:identity/tres@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

// ── Coverage for the deliverability + insights closure batch ──

#[tokio::test]
async fn test_deliverability_dashboard_round_trip() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Default: not enabled.
    let req = make_request(Method::GET, "/v2/email/deliverability-dashboard", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DashboardEnabled"], false);

    // Enable + add a subscribed domain.
    let req = make_request(
        Method::PUT,
        "/v2/email/deliverability-dashboard",
        r#"{"DashboardEnabled":true,"SubscribedDomains":[{"Domain":"example.com","InboxPlacementTrackingOption":{"Global":true,"TrackedIsps":["gmail.com"]}}]}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(Method::GET, "/v2/email/deliverability-dashboard", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DashboardEnabled"], true);
    assert_eq!(body["AccountStatus"], "ACTIVE");
    let domains = body["ActiveSubscribedDomains"].as_array().unwrap();
    assert_eq!(domains.len(), 1);
    assert_eq!(domains[0]["Domain"], "example.com");
}

#[tokio::test]
async fn test_put_deliverability_dashboard_option_requires_flag() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::PUT, "/v2/email/deliverability-dashboard", r#"{}"#);
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "BadRequestException");
}

#[tokio::test]
async fn test_deliverability_test_report_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/deliverability-dashboard/test",
        r#"{"FromEmailAddress":"a@example.com","Content":{"Simple":{"Subject":{"Data":"Hi"}}}}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let report_id = body["ReportId"].as_str().unwrap().to_string();

    let req = make_request(
        Method::GET,
        &format!("/v2/email/deliverability-dashboard/test-reports/{report_id}"),
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(
        Method::GET,
        "/v2/email/deliverability-dashboard/test-reports",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["DeliverabilityTestReports"].as_array().unwrap().len(),
        1
    );
}

#[tokio::test]
async fn test_create_deliverability_test_report_requires_from() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/deliverability-dashboard/test",
        r#"{}"#,
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "BadRequestException");
}

#[tokio::test]
async fn test_get_deliverability_test_report_unknown() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::GET,
        "/v2/email/deliverability-dashboard/test-reports/no-such",
        "",
    );
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "NotFoundException");
}

#[tokio::test]
async fn test_blacklist_reports_route() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request_with_query(
        Method::GET,
        "/v2/email/deliverability-dashboard/blacklist-report",
        "",
        "BlacklistItemNames=1.2.3.4",
        HashMap::new(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn test_domain_deliverability_campaign_route() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::GET,
        "/v2/email/deliverability-dashboard/campaigns/camp-1",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn test_domain_statistics_report_route() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request_with_query(
        Method::GET,
        "/v2/email/deliverability-dashboard/statistics-report/example.com",
        "",
        "StartDate=2024-01-01&EndDate=2024-01-31",
        HashMap::new(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn test_list_domain_deliverability_campaigns_route() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request_with_query(
        Method::GET,
        "/v2/email/deliverability-dashboard/domains/example.com/campaigns",
        "",
        "StartDate=2024-01-01&EndDate=2024-01-31",
        HashMap::new(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn test_get_email_address_insights_route() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/email-address-insights",
        r#"{"EmailAddress":"a@example.com"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn test_get_email_address_insights_requires_email() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::POST, "/v2/email/email-address-insights", r#"{}"#);
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "BadRequestException");
}

#[tokio::test]
async fn test_get_message_insights_unknown() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::GET, "/v2/email/insights/no-such-id", "");
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "NotFoundException");
}

#[tokio::test]
async fn test_get_message_insights_real() {
    use crate::state::{DeliveryInsightEvent, EmailRecipientInsight};

    let state = make_state();
    enable_production_access(&state);

    // Seed a verified identity
    seed_identity(&state, "sender@example.com");

    // Pre-populate a sent email with insights
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.sent_emails.push(crate::state::SentEmail {
            message_id: "msg-123".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["recipient@gmail.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: Some("Hello".to_string()),
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: chrono::Utc::now(),
            email_tags: vec![("campaign".to_string(), "test".to_string())],
            delivery_insights: vec![EmailRecipientInsight {
                destination: "recipient@gmail.com".to_string(),
                isp: "Gmail".to_string(),
                events: vec![
                    DeliveryInsightEvent {
                        timestamp: chrono::Utc::now(),
                        event_type: "SEND".to_string(),
                        ..Default::default()
                    },
                    DeliveryInsightEvent {
                        timestamp: chrono::Utc::now(),
                        event_type: "DELIVERY".to_string(),
                        ..Default::default()
                    },
                ],
            }],
        });
    }

    let svc = SesV2Service::new(state);
    let req = make_request(Method::GET, "/v2/email/insights/msg-123", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["MessageId"], "msg-123");
    assert_eq!(body["FromEmailAddress"], "sender@example.com");
    assert_eq!(body["Subject"], "Hello");
    let tags = body["EmailTags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Name"], "campaign");
    assert_eq!(tags[0]["Value"], "test");
    let insights = body["Insights"].as_array().unwrap();
    assert_eq!(insights.len(), 1);
    assert_eq!(insights[0]["Destination"], "recipient@gmail.com");
    assert_eq!(insights[0]["Isp"], "Gmail");
    let events = insights[0]["Events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["Type"], "SEND");
    assert!(!events[0]["Timestamp"].is_null());
    assert_eq!(events[1]["Type"], "DELIVERY");
}

#[tokio::test]
async fn test_list_recommendations_seeds_default() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::POST, "/v2/email/vdm/recommendations", "{}");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(!body["Recommendations"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn test_get_dedicated_ip_pool_round_trip() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/dedicated-ip-pools",
        r#"{"PoolName":"pool1"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let req = make_request(Method::GET, "/v2/email/dedicated-ip-pools/pool1", "");
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn test_get_dedicated_ip_pool_unknown() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(Method::GET, "/v2/email/dedicated-ip-pools/no-such", "");
    let err = match svc.handle(req).await {
        Err(e) => e,
        Ok(_) => panic!("expected error"),
    };
    assert_eq!(err.code(), "NotFoundException");
}

// ── X2: account + config-set sending pause + suppression enforcement ──

#[tokio::test]
async fn send_email_v2_rejects_when_account_sending_paused() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.account_settings.sending_enabled = false;
    }
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["r@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["__type"], "SendingPausedException");
    assert_eq!(body["message"], "Email sending for the account is paused.");
}

#[tokio::test]
async fn send_email_v2_succeeds_when_paused_then_resumed() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.account_settings.sending_enabled = false;
    }
    let svc = SesV2Service::new(state.clone());

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["r@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Resume sending
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.account_settings.sending_enabled = true;
    }

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["r@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

#[tokio::test]
async fn send_email_v2_rejects_when_config_set_sending_paused() {
    use crate::state::ConfigurationSet;
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.configuration_sets.insert(
            "my-cs".to_string(),
            ConfigurationSet {
                name: "my-cs".to_string(),
                sending_enabled: false,
                tls_policy: "OPTIONAL".to_string(),
                sending_pool_name: None,
                custom_redirect_domain: None,
                https_policy: None,
                suppressed_reasons: Vec::new(),
                reputation_metrics_enabled: false,
                vdm_options: None,
                archive_arn: None,
                archiving_options_present: false,
            },
        );
    }
    let svc = SesV2Service::new(state);

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["r@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}},
            "ConfigurationSetName": "my-cs"
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["__type"], "SendingPausedException");
    assert_eq!(
        body["message"],
        "Email sending for the configuration set my-cs is paused."
    );
}

#[tokio::test]
async fn send_email_v2_skips_suppressed_recipient() {
    use crate::state::{EmailTemplate, SuppressedDestination};
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    {
        let mut accounts = state.write();
        let st = accounts.get_or_create("123456789012");
        st.suppressed_destinations.insert(
            "blocked@example.com".to_string(),
            SuppressedDestination {
                email_address: "blocked@example.com".to_string(),
                reason: "BOUNCE".to_string(),
                last_update_time: chrono::Utc::now(),
            },
        );
        st.templates.insert(
            "t".to_string(),
            EmailTemplate {
                template_name: "t".to_string(),
                subject: Some("hi".to_string()),
                html_body: None,
                text_body: None,
                created_at: chrono::Utc::now(),
            },
        );
    }
    let svc = SesV2Service::new(state);

    // Single-recipient send: a suppressed To address fails the entire
    // send with MessageRejected.
    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["blocked@example.com"]},
            "Content": {"Simple": {"Subject": {"Data": "S"}, "Body": {"Text": {"Data": "B"}}}}
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["__type"], "MessageRejected");
    assert!(body["message"]
        .as_str()
        .unwrap_or("")
        .contains("Address is on the suppression list"));

    // Bulk send: suppressed entry is dropped, others go through.
    let req = make_request(
        Method::POST,
        "/v2/email/outbound-bulk-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "DefaultContent": {"Template": {"TemplateName": "t", "TemplateData": "{}"}},
            "BulkEmailEntries": [
                {"Destination": {"ToAddresses": ["ok@example.com"]}},
                {"Destination": {"ToAddresses": ["blocked@example.com"]}}
            ]
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let results = body["BulkEmailEntryResults"].as_array().unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["Status"], "SUCCESS");
    assert_eq!(results[1]["Status"], "MESSAGE_REJECTED");
    assert_eq!(results[1]["Error"], "Address is on the suppression list");
}

#[tokio::test]
async fn test_event_destination_kinesis_firehose_cloudwatch() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    // Create config set first
    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "my-config"}"#,
    );
    svc.handle(req).await.unwrap();

    // Create event destination with Kinesis, Firehose, and CloudWatch
    let req = make_request(
        Method::POST,
        "/v2/email/configuration-sets/my-config/event-destinations",
        r#"{
            "EventDestinationName": "multi-dest",
            "EventDestination": {
                "Enabled": true,
                "MatchingEventTypes": ["SEND", "DELIVERY"],
                "KinesisFirehoseDestination": {
                    "DeliveryStreamARN": "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream",
                    "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/my-kinesis"
                },
                "CloudWatchDestination": {
                    "DimensionConfigurations": [
                        {
                            "DimensionName": "EventType",
                            "DimensionValueSource": "MESSAGE_TAG",
                            "DefaultDimensionValue": "Send"
                        }
                    ]
                }
            }
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Get event destinations and verify all fields round-trip
    let req = make_request(
        Method::GET,
        "/v2/email/configuration-sets/my-config/event-destinations",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let dests = body["EventDestinations"].as_array().unwrap();
    assert_eq!(dests.len(), 1);
    assert_eq!(dests[0]["Name"], "multi-dest");
    assert_eq!(
        dests[0]["KinesisFirehoseDestination"]["DeliveryStreamARN"],
        "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream"
    );
    assert_eq!(
        dests[0]["KinesisFirehoseDestination"]["StreamARN"],
        "arn:aws:kinesis:us-east-1:123456789012:stream/my-kinesis"
    );
    assert_eq!(
        dests[0]["CloudWatchDestination"]["DimensionConfigurations"][0]["DimensionName"],
        "EventType"
    );
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = SesV2Service::new(make_state());
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating SES state
/// directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = SesV2Service::new(make_state()).with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}
