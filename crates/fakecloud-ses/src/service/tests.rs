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
            verification_token: None,
        },
    );
}

fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
    make_request_with_query(method, path, body, "", HashMap::new())
}

struct SendEventRecorder {
    state: SharedSesState,
    records: std::sync::Mutex<Vec<(String, Value)>>,
}

impl fakecloud_core::delivery::FirehoseDelivery for SendEventRecorder {
    fn put_record(&self, arn: &str, data: &[u8]) {
        assert!(
            self.state.try_write().is_some(),
            "SES lock held during callback"
        );
        self.records
            .lock()
            .unwrap()
            .push((arn.to_string(), serde_json::from_slice(data).unwrap()));
    }
}

fn event_service() -> (SesV2Service, Arc<SendEventRecorder>) {
    let state = make_state();
    seed_identity(&state, "example.com");
    state.write().default_mut().templates.insert(
        "tpl".into(),
        crate::state::EmailTemplate {
            template_name: "tpl".into(),
            subject: Some("Hello {{name}}".into()),
            html_body: None,
            text_body: Some("Body {{name}}".into()),
            created_at: chrono::Utc::now(),
        },
    );
    let recorder = Arc::new(SendEventRecorder {
        state: state.clone(),
        records: Default::default(),
    });
    let svc = SesV2Service::new(state.clone()).with_delivery(SesDeliveryContext {
        ses_state: state,
        delivery_bus: Arc::new(
            fakecloud_core::delivery::DeliveryBus::new().with_firehose(recorder.clone()),
        ),
    });
    (svc, recorder)
}

fn query_request(action: &str, params: &[(&str, &str)]) -> AwsRequest {
    let mut req = make_request(Method::POST, "/", "");
    req.is_query_protocol = true;
    req.action = action.to_string();
    req.query_params = params
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    req
}

#[tokio::test]
async fn optional_v2_raw_resolves_headers_before_admission_and_honors_api_precedence() {
    for explicit_from in [false, true] {
        for explicit_destination in [false, true] {
            let (svc, recorder) = event_service();
            create_v1_event_config(&svc, "header-cs").await;
            create_v1_event_config(&svc, "api-cs").await;
            svc.state
                .write()
                .default_mut()
                .account_settings
                .production_access_enabled = false;
            let mime = "From: Header <header@example.com>\r\nTo: header-to@example.com\r\nCc: header-cc@example.com\r\nBcc: header-bcc@example.com\r\nX-SES-CONFIGURATION-SET: header-cs\r\nSubject: raw\r\n\r\nbody";
            let raw = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, mime);
            let mut body = json!({"Content":{"Raw":{"Data":raw}}});
            if explicit_from {
                body["FromEmailAddress"] = json!("API <api@example.com>");
            }
            if explicit_destination {
                body["Destination"] = json!({"ToAddresses":["api-to@example.com"]});
                body["ConfigurationSetName"] = json!("api-cs");
            }
            let response = svc
                .handle(make_request(
                    Method::POST,
                    "/v2/email/outbound-emails",
                    &body.to_string(),
                ))
                .await
                .unwrap();
            assert!(
                response.status.is_success(),
                "{}",
                std::str::from_utf8(response.body.expect_bytes()).unwrap()
            );
            let calls = recorder.records.lock().unwrap();
            assert_eq!(calls.len(), 1);
            assert_eq!(
                calls[0].1["mail"]["source"],
                if explicit_from {
                    "api@example.com"
                } else {
                    "header@example.com"
                }
            );
            assert_eq!(
                calls[0].1["mail"]["destination"],
                if explicit_destination {
                    json!(["api-to@example.com"])
                } else {
                    json!([
                        "header-to@example.com",
                        "header-cc@example.com",
                        "header-bcc@example.com"
                    ])
                }
            );
            assert_eq!(
                calls[0].1["mail"]["tags"]["ses:configuration-set"],
                if explicit_destination {
                    json!(["api-cs"])
                } else {
                    json!(["header-cs"])
                }
            );
            let accounts = svc.state.read();
            let sent = &accounts.default_ref().sent_emails[0];
            assert_eq!(
                sent.from,
                if explicit_from {
                    "API <api@example.com>"
                } else {
                    "Header <header@example.com>"
                }
            );
            assert_eq!(sent.raw_data.as_deref(), Some(mime));
            assert_eq!(sent.text_body.as_deref(), Some("body"));
        }
    }
}

#[tokio::test]
async fn optional_v2_raw_missing_or_bad_effective_fields_do_not_record_or_publish() {
    for text in [
        "From: sender@example.com\r\n\r\nbody",
        "To: to@example.com\r\n\r\nbody",
        "From: sender@example.com\r\nTo: not-an-address\r\n\r\nbody",
    ] {
        let (svc, recorder) = event_service();
        create_v1_event_config(&svc, "cs").await;
        let raw = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, text);
        let body = json!({"ConfigurationSetName":"cs", "Content":{"Raw":{"Data":raw}}});
        assert!(!svc
            .handle(make_request(
                Method::POST,
                "/v2/email/outbound-emails",
                &body.to_string()
            ))
            .await
            .unwrap()
            .status
            .is_success());
        assert!(recorder.records.lock().unwrap().is_empty());
        assert!(svc.state.read().default_ref().sent_emails.is_empty());
    }
}

#[tokio::test]
async fn optional_quoted_sender_uses_domain_default_configuration_and_dkim_identity() {
    let (key, _) = crate::dkim::generate_easy_dkim_keypair();
    for mode in ["v1", "v2", "v2-bulk"] {
        let (svc, recorder) = event_service();
        create_v1_event_config(&svc, "default-cs").await;
        {
            let mut accounts = svc.state.write();
            let identity = accounts
                .default_mut()
                .identities
                .get_mut("example.com")
                .unwrap();
            identity.configuration_set_name = Some("default-cs".into());
            identity.dkim_domain_signing_private_key = Some(key.clone());
        }
        let from = r#""user@department"@example.com"#;
        let request = if mode == "v1" {
            query_request(
                "SendEmail",
                &[
                    ("Source", from),
                    ("Destination.ToAddresses.member.1", "to@example.com"),
                ],
            )
        } else if mode == "v2" {
            let body = json!({"FromEmailAddress":from,"Destination":{"ToAddresses":["to@example.com"]},
                "Content":{"Simple":{"Subject":{"Data":"subject"},"Body":{"Text":{"Data":"body"}}}}});
            make_request(Method::POST, "/v2/email/outbound-emails", &body.to_string())
        } else {
            let body = json!({"FromEmailAddress":from,"DefaultContent":{"Template":{"TemplateName":"tpl","TemplateData":"{}"}},
                "BulkEmailEntries":[{"Destination":{"ToAddresses":["to@example.com"]}}]});
            make_request(
                Method::POST,
                "/v2/email/outbound-bulk-emails",
                &body.to_string(),
            )
        };
        assert!(svc.handle(request).await.unwrap().status.is_success());
        let calls = recorder.records.lock().unwrap();
        assert_eq!(calls.len(), 1, "{mode}");
        assert_eq!(
            calls[0].1["mail"]["tags"]["ses:configuration-set"],
            json!(["default-cs"])
        );
        assert!(
            svc.state.read().default_ref().sent_emails[0]
                .dkim_signature
                .is_some(),
            "{mode}"
        );
    }
}

#[tokio::test]
async fn optional_raw_group_trailing_comments_are_not_missing_mailboxes() {
    for v2 in [false, true] {
        let (svc, recorder) = event_service();
        create_v1_event_config(&svc, "cs").await;
        svc.state
            .write()
            .default_mut()
            .account_settings
            .production_access_enabled = false;
        let raw = base64::Engine::encode(&base64::engine::general_purpose::STANDARD,
            "From: sender@example.com\r\nTo: Team: good@example.com; (team (nested))\r\nCc: Empty: (nobody); (end)\r\nX-SES-CONFIGURATION-SET: cs\r\n\r\nbody");
        let request = if v2 {
            let body = json!({"Content":{"Raw":{"Data":raw}}});
            make_request(Method::POST, "/v2/email/outbound-emails", &body.to_string())
        } else {
            query_request("SendRawEmail", &[("RawMessage.Data", &raw)])
        };
        let response = svc.handle(request).await.unwrap();
        assert!(response.status.is_success());
        assert_eq!(
            recorder.records.lock().unwrap()[0].1["mail"]["destination"],
            json!(["good@example.com"])
        );
        let bad = base64::Engine::encode(&base64::engine::general_purpose::STANDARD,
            "From: sender@example.com\r\nTo: good@example.com, not-an-address (label)\r\nX-SES-CONFIGURATION-SET: cs\r\n\r\nbody");
        let request = if v2 {
            let body = json!({"Content":{"Raw":{"Data":bad}}});
            make_request(Method::POST, "/v2/email/outbound-emails", &body.to_string())
        } else {
            query_request("SendRawEmail", &[("RawMessage.Data", &bad)])
        };
        let result = svc.handle(request).await;
        assert!(result.is_err() || !result.as_ref().unwrap().status.is_success());
        assert_eq!(recorder.records.lock().unwrap().len(), 1);
        assert_eq!(svc.state.read().default_ref().sent_emails.len(), 1);
    }
}

#[test]
fn optional_quoted_local_part_domain_helpers_use_last_at_sign() {
    assert_eq!(
        crate::fanout::infer_isp(r#""user@department"@gmail.com"#),
        "Gmail"
    );
    assert!(super::sending::is_simulator_address(
        r#""user@department"@simulator.amazonses.com"#
    ));
}

#[tokio::test]
async fn optional_default_configuration_pause_is_checked_before_dispatch() {
    for bulk in [false, true] {
        let (svc, recorder) = event_service();
        create_v1_event_config(&svc, "default-cs").await;
        {
            let mut accounts = svc.state.write();
            let state = accounts.default_mut();
            state
                .identities
                .get_mut("example.com")
                .unwrap()
                .configuration_set_name = Some("default-cs".into());
            state
                .configuration_sets
                .get_mut("default-cs")
                .unwrap()
                .sending_enabled = false;
        }
        let body = if bulk {
            json!({"FromEmailAddress":"sender@example.com", "DefaultContent":{"Template":{"TemplateName":"tpl","TemplateData":"{}"}},
                "BulkEmailEntries":[{"Destination":{"ToAddresses":["to@example.com"]}}]})
        } else {
            json!({"FromEmailAddress":"sender@example.com","Destination":{"ToAddresses":["to@example.com"]},
                "Content":{"Simple":{"Subject":{"Data":"subject"},"Body":{"Text":{"Data":"body"}}}}})
        };
        let path = if bulk {
            "/v2/email/outbound-bulk-emails"
        } else {
            "/v2/email/outbound-emails"
        };
        let response = svc
            .handle(make_request(Method::POST, path, &body.to_string()))
            .await
            .unwrap();
        assert!(!response.status.is_success());
        let result: Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
        assert_eq!(result["__type"], "SendingPausedException");
        assert!(recorder.records.lock().unwrap().is_empty());
        assert!(svc.state.read().default_ref().sent_emails.is_empty());
    }
}

#[tokio::test]
async fn admission_v2_single_rejects_group_members_before_verification() {
    for production in [false, true] {
        for address in [
            "Team: Unverified <one@other.test>, Verified <two@example.com>;",
            "Team: Verified <two@example.com>;",
        ] {
            let (svc, recorder) = event_service();
            create_v1_event_config(&svc, "cs").await;
            svc.state
                .write()
                .default_mut()
                .account_settings
                .production_access_enabled = production;
            let body = json!({"FromEmailAddress":"sender@example.com", "ConfigurationSetName":"cs",
                "Destination":{"ToAddresses":[address]}, "Content":{"Simple":{"Subject":{"Data":"subject"}, "Body":{"Text":{"Data":"body"}}}}});
            let response = svc
                .handle(make_request(
                    Method::POST,
                    "/v2/email/outbound-emails",
                    &body.to_string(),
                ))
                .await
                .unwrap();
            assert!(
                !response.status.is_success(),
                "API groups are not single mailboxes: {address}"
            );
            assert!(recorder.records.lock().unwrap().is_empty());
            assert!(svc.state.read().default_ref().sent_emails.is_empty());
        }
    }
}

#[tokio::test]
async fn admission_v2_bulk_rejects_group_members_without_recording() {
    for production in [false, true] {
        let (svc, recorder) = event_service();
        create_v1_event_config(&svc, "cs").await;
        svc.state
            .write()
            .default_mut()
            .account_settings
            .production_access_enabled = production;
        let body = json!({"FromEmailAddress":"sender@example.com", "ConfigurationSetName":"cs",
            "DefaultContent":{"Template":{"TemplateName":"tpl","TemplateData":"{}"}},
            "BulkEmailEntries":[{"Destination":{"ToAddresses":["Team: Unverified <one@other.test>, Verified <two@example.com>;"]}}]});
        let response = svc
            .handle(make_request(
                Method::POST,
                "/v2/email/outbound-bulk-emails",
                &body.to_string(),
            ))
            .await
            .unwrap();
        let result: Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
        assert_ne!(
            result["BulkEmailEntryResults"][0]["Status"], "SUCCESS",
            "{result}"
        );
        assert!(result["BulkEmailEntryResults"][0]["MessageId"].is_null());
        assert!(recorder.records.lock().unwrap().is_empty());
        assert!(svc.state.read().default_ref().sent_emails.is_empty());
    }
}

#[tokio::test]
async fn admission_raw_rejects_invalid_entries_and_empty_effective_envelopes() {
    for recipient_headers in [
        "To: not-an-address\r\n",
        "To: good@example.com, not-an-address\r\n",
        "To: good@example.com\r\nCc: not-an-address\r\n",
        "To: Empty:\r\nCc: good@example.com\r\n",
        "To: Empty:;\r\n",
        "",
    ] {
        let (svc, recorder) = event_service();
        create_v1_event_config(&svc, "cs").await;
        svc.state
            .write()
            .default_mut()
            .account_settings
            .production_access_enabled = false;
        let raw = base64::Engine::encode(&base64::engine::general_purpose::STANDARD,
            format!("From: sender@example.com\r\n{recipient_headers}X-SES-CONFIGURATION-SET: cs\r\n\r\nbody"));
        let result = svc
            .handle(query_request("SendRawEmail", &[("RawMessage.Data", &raw)]))
            .await;
        assert!(
            result.is_err(),
            "must reject malformed/empty envelope: {recipient_headers:?}"
        );
        assert!(recorder.records.lock().unwrap().is_empty());
        assert!(svc.state.read().default_ref().sent_emails.is_empty());
    }
}

#[tokio::test]
async fn admission_raw_empty_group_with_valid_recipient_is_accepted() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "cs").await;
    svc.state
        .write()
        .default_mut()
        .account_settings
        .production_access_enabled = false;
    let raw = base64::Engine::encode(&base64::engine::general_purpose::STANDARD,
        "From: sender@example.com\r\nTo: Empty:;\r\nCc: Good <good@example.com>\r\nX-SES-CONFIGURATION-SET: cs\r\n\r\nbody");
    assert!(svc
        .handle(query_request("SendRawEmail", &[("RawMessage.Data", &raw)]))
        .await
        .unwrap()
        .status
        .is_success());
    assert_eq!(
        recorder.records.lock().unwrap()[0].1["mail"]["destination"],
        json!(["good@example.com"])
    );
}

#[tokio::test]
async fn admission_quoted_local_parts_survive_api_and_raw_sends() {
    for address in [r#""a,b"@example.com"#, r#"Recipient <"a,b"@example.com>"#] {
        for mode in [
            "v1-api",
            "v1-raw-headers",
            "v1-raw-api",
            "v1-template",
            "v1-bulk",
            "v2-api",
            "v2-bulk",
        ] {
            let (svc, recorder) = event_service();
            create_v1_event_config(&svc, "cs").await;
            svc.state
                .write()
                .default_mut()
                .account_settings
                .production_access_enabled = false;
            let request = if mode.starts_with("v1-raw") {
                let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD,
                    format!("From: {address}\r\nTo: {address}\r\nX-SES-CONFIGURATION-SET: cs\r\n\r\nbody"));
                let mut request = query_request("SendRawEmail", &[("RawMessage.Data", &encoded)]);
                if mode == "v1-raw-api" {
                    request.query_params.insert("Source".into(), address.into());
                    request
                        .query_params
                        .insert("Destinations.member.1".into(), address.into());
                }
                request
            } else if mode.starts_with("v1-") {
                let action = match mode {
                    "v1-template" => "SendTemplatedEmail",
                    "v1-bulk" => "SendBulkTemplatedEmail",
                    _ => "SendEmail",
                };
                query_request(
                    action,
                    &[
                        ("Source", address),
                        ("ConfigurationSetName", "cs"),
                        ("Destination.ToAddresses.member.1", address),
                        ("Template", "tpl"),
                        ("TemplateData", "{}"),
                        (
                            "Destinations.member.1.Destination.ToAddresses.member.1",
                            address,
                        ),
                    ],
                )
            } else if mode == "v2-api" {
                let body = json!({"FromEmailAddress":address,"ConfigurationSetName":"cs", "Destination":{"ToAddresses":[address]},
                    "Content":{"Simple":{"Subject":{"Data":"subject"},"Body":{"Text":{"Data":"body"}}}}});
                make_request(Method::POST, "/v2/email/outbound-emails", &body.to_string())
            } else {
                let body = json!({"FromEmailAddress":address,"ConfigurationSetName":"cs", "DefaultContent":{"Template":{"TemplateName":"tpl","TemplateData":"{}"}},
                    "BulkEmailEntries":[{"Destination":{"ToAddresses":[address]}}]});
                make_request(
                    Method::POST,
                    "/v2/email/outbound-bulk-emails",
                    &body.to_string(),
                )
            };
            assert!(
                svc.handle(request).await.unwrap().status.is_success(),
                "{mode}"
            );
            let calls = recorder.records.lock().unwrap();
            assert_eq!(calls.len(), 1, "{mode}");
            assert_eq!(calls[0].1["mail"]["source"], r#""a,b"@example.com"#);
            assert_eq!(
                calls[0].1["mail"]["destination"],
                json!([r#""a,b"@example.com"#])
            );
            let accounts = svc.state.read();
            let sent = &accounts.default_ref().sent_emails[0];
            assert_eq!(sent.from, address);
            assert_eq!(
                sent.to,
                [if mode == "v1-raw-headers" {
                    r#""a,b"@example.com"#
                } else {
                    address
                }],
                "{mode}"
            );
            assert_eq!(
                sent.delivery_insights[0].destination,
                r#""a,b"@example.com"#
            );
        }
    }
}

async fn create_v1_event_config(svc: &SesV2Service, name: &str) {
    svc.handle(query_request(
        "CreateConfigurationSet",
        &[("ConfigurationSet.Name", name)],
    ))
    .await
    .unwrap();
    svc.handle(query_request(
        "CreateConfigurationSetEventDestination",
        &[
            ("ConfigurationSetName", name),
            ("EventDestination.Name", "fh"),
            ("EventDestination.Enabled", "true"),
            ("EventDestination.MatchingEventTypes.member.1", "send"),
            ("EventDestination.MatchingEventTypes.member.2", "bounce"),
            (
                "EventDestination.KinesisFirehoseDestination.DeliveryStreamARN",
                "arn:aws:firehose:us-east-1:123456789012:deliverystream/events",
            ),
            (
                "EventDestination.KinesisFirehoseDestination.IAMRoleARN",
                "arn:aws:iam::123456789012:role/events",
            ),
        ],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn v1_send_dispatch_publishes_each_successful_message_and_preserves_envelope() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "cs").await;
    for action in [
        "SendEmail",
        "SendRawEmail",
        "SendTemplatedEmail",
        "SendBulkTemplatedEmail",
    ] {
        let raw = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "From: sender@example.com\r\nTo: header@example.com\r\n\r\nbody",
        );
        let req = query_request(
            action,
            &[
                ("Source", "sender@example.com"),
                ("ConfigurationSetName", "cs"),
                ("Destination.ToAddresses.member.1", "to@example.com"),
                ("Destination.CcAddresses.member.1", "cc@example.com"),
                ("Destination.BccAddresses.member.1", "bcc@example.com"),
                ("Destinations.member.1", "to@example.com"),
                ("RawMessage.Data", &raw),
                ("Template", "tpl"),
                ("TemplateData", r#"{"name":"Ada"}"#),
                ("DefaultTemplateData", r#"{"name":"Ada"}"#),
                (
                    "Destinations.member.1.Destination.ToAddresses.member.1",
                    "to@example.com",
                ),
                (
                    "Destinations.member.1.Destination.CcAddresses.member.1",
                    "cc@example.com",
                ),
                (
                    "Destinations.member.1.Destination.BccAddresses.member.1",
                    "bcc@example.com",
                ),
                ("Tags.member.1.Name", "campaign"),
                ("Tags.member.1.Value", "fall"),
                ("DefaultTags.member.1.Name", "campaign"),
                ("DefaultTags.member.1.Value", "fall"),
            ],
        );
        let resp = svc.handle(req).await.unwrap();
        assert!(resp.status.is_success(), "{action}");
        let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        let id = xml
            .split("<MessageId>")
            .nth(1)
            .unwrap()
            .split("</MessageId>")
            .next()
            .unwrap();
        let calls = recorder.records.lock().unwrap();
        let (_, event) = calls.last().expect("successful send must publish an event");
        assert_eq!(event["mail"]["messageId"], id, "{action}");
        assert_eq!(event["mail"]["source"], "sender@example.com");
        let want = if action == "SendRawEmail" {
            json!(["to@example.com"])
        } else {
            json!(["to@example.com", "cc@example.com", "bcc@example.com"])
        };
        assert_eq!(event["mail"]["destination"], want, "{action}");
        assert_eq!(event["mail"]["tags"]["campaign"], json!(["fall"]));
        drop(calls);
        let accounts = svc.state.read();
        let sent = accounts.default_ref().sent_emails.last().unwrap();
        assert_eq!(sent.message_id, id);
        assert!(!sent.delivery_insights.is_empty());
        if action.contains("Templated") {
            assert_eq!(sent.subject.as_deref(), Some("Hello Ada"));
        }
    }
    assert_eq!(recorder.records.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn v1_raw_send_uses_unfolded_headers_and_explicit_arguments_take_precedence() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "header-cs").await;
    create_v1_event_config(&svc, "explicit-cs").await;
    let raw = base64::Engine::encode(&base64::engine::general_purpose::STANDARD,
        "From: Sender <sender@example.com>\r\nTo: \"Recipient, One\" <to@example.com>\r\nCc: cc@example.com\r\nBcc: bcc@example.com\r\nx-ses-configuration-set:\r\n\theader-cs\r\nX-SES-MESSAGE-TAGS: campaign=header, team=ses\r\nSubject: raw\r\n\r\nX-SES-CONFIGURATION-SET: body-is-not-a-header");
    for explicit in [false, true] {
        let mut req = query_request("SendRawEmail", &[("RawMessage.Data", &raw)]);
        if explicit {
            for (k, v) in [
                ("ConfigurationSetName", "explicit-cs"),
                ("Source", "override@example.com"),
                ("Destinations.member.1", "override-to@example.com"),
                ("Tags.member.1.Name", "campaign"),
                ("Tags.member.1.Value", "api"),
            ] {
                req.query_params.insert(k.into(), v.into());
            }
        }
        let resp = svc.handle(req).await.unwrap();
        assert!(resp.status.is_success());
        let calls = recorder.records.lock().unwrap();
        assert_eq!(calls.len(), if explicit { 2 } else { 1 });
        let event = &calls.last().unwrap().1;
        assert_eq!(
            event["mail"]["tags"]["campaign"],
            if explicit {
                json!(["api"])
            } else {
                json!(["header"])
            }
        );
        assert_eq!(
            event["mail"]["tags"]["team"],
            if explicit {
                Value::Null
            } else {
                json!(["ses"])
            }
        );
        assert_eq!(
            event["mail"]["tags"]["ses:configuration-set"],
            if explicit {
                json!(["explicit-cs"])
            } else {
                json!(["header-cs"])
            }
        );
        assert_eq!(
            event["mail"]["source"],
            if explicit {
                "override@example.com"
            } else {
                "sender@example.com"
            }
        );
        assert_eq!(
            event["mail"]["destination"],
            if explicit {
                json!(["override-to@example.com"])
            } else {
                json!(["to@example.com", "cc@example.com", "bcc@example.com"])
            }
        );
    }
}

#[tokio::test]
async fn v1_failed_sends_and_bulk_entries_do_not_publish() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "cs").await;
    let req = query_request(
        "SendEmail",
        &[
            ("Source", "unverified@other.test"),
            ("ConfigurationSetName", "cs"),
            ("Destination.ToAddresses.member.1", "to@example.com"),
        ],
    );
    assert!(svc.handle(req).await.is_err());
    svc.state
        .write()
        .default_mut()
        .account_settings
        .sending_enabled = false;
    let req = query_request(
        "SendEmail",
        &[
            ("Source", "sender@example.com"),
            ("ConfigurationSetName", "cs"),
            ("Destination.ToAddresses.member.1", "to@example.com"),
        ],
    );
    assert!(svc.handle(req).await.is_err());
    svc.state
        .write()
        .default_mut()
        .account_settings
        .sending_enabled = true;
    svc.state
        .write()
        .default_mut()
        .suppressed_destinations
        .insert(
            "blocked@example.com".into(),
            crate::state::SuppressedDestination {
                email_address: "blocked@example.com".into(),
                reason: "BOUNCE".into(),
                last_update_time: chrono::Utc::now(),
            },
        );
    let raw = base64::Engine::encode(&base64::engine::general_purpose::STANDARD,
        "From: sender@example.com\r\nTo: blocked@example.com\r\nX-SES-CONFIGURATION-SET: cs\r\n\r\nbody");
    assert!(svc
        .handle(query_request("SendRawEmail", &[("RawMessage.Data", &raw)]))
        .await
        .is_err());
    assert!(recorder.records.lock().unwrap().is_empty());
    let req = query_request(
        "SendTemplatedEmail",
        &[
            ("Source", "sender@example.com"),
            ("ConfigurationSetName", "cs"),
            ("Template", "missing"),
            ("TemplateData", "{}"),
        ],
    );
    assert!(svc.handle(req).await.is_err());
    svc.state
        .write()
        .default_mut()
        .account_settings
        .production_access_enabled = false;
    let req = query_request(
        "SendBulkTemplatedEmail",
        &[
            ("Source", "sender@example.com"),
            ("ConfigurationSetName", "cs"),
            ("Template", "tpl"),
            (
                "Destinations.member.1.Destination.ToAddresses.member.1",
                "unverified@other.test",
            ),
            (
                "Destinations.member.2.Destination.ToAddresses.member.1",
                "bounce@simulator.amazonses.com",
            ),
        ],
    );
    let resp = svc.handle(req).await.unwrap();
    let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(xml.contains("<Status>MessageRejected</Status>"));
    assert!(xml.contains("<Status>Success</Status>"));
    let calls = recorder.records.lock().unwrap();
    assert_eq!(calls.len(), 2, "only successful entry emits Send + Bounce");
    assert_eq!(calls[0].1["eventType"], "Send");
    assert_eq!(calls[1].1["eventType"], "Bounce");
}

#[tokio::test]
async fn v1_send_fanout_uses_request_account_and_region() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "cs").await;
    let account = "111111111111";
    let region = "eu-west-1";
    for mut req in [
        query_request("VerifyDomainIdentity", &[("Domain", "other.test")]),
        query_request("CreateConfigurationSet", &[("ConfigurationSet.Name", "cs")]),
        query_request(
            "CreateConfigurationSetEventDestination",
            &[
                ("ConfigurationSetName", "cs"),
                ("EventDestination.Name", "other-fh"),
                ("EventDestination.Enabled", "true"),
                ("EventDestination.MatchingEventTypes.member.1", "send"),
                (
                    "EventDestination.KinesisFirehoseDestination.DeliveryStreamARN",
                    "arn:aws:firehose:eu-west-1:111111111111:deliverystream/events",
                ),
                (
                    "EventDestination.KinesisFirehoseDestination.IAMRoleARN",
                    "arn:aws:iam::111111111111:role/events",
                ),
            ],
        ),
        query_request(
            "SendEmail",
            &[
                ("Source", "sender@other.test"),
                ("ConfigurationSetName", "cs"),
                ("Destination.ToAddresses.member.1", "to@other.test"),
            ],
        ),
    ] {
        req.account_id = account.into();
        req.region = region.into();
        assert!(svc.handle(req).await.unwrap().status.is_success());
    }
    let calls = recorder.records.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].0,
        "arn:aws:firehose:eu-west-1:111111111111:deliverystream/events"
    );
    assert_eq!(calls[0].1["mail"]["sendingAccountId"], account);
    assert_eq!(
        calls[0].1["mail"]["sourceArn"],
        "arn:aws:ses:eu-west-1:111111111111:identity/other.test"
    );
    assert!(svc
        .state
        .read()
        .default_ref()
        .event_destination_dispatches
        .is_empty());
    assert!(svc.state.read().default_ref().sent_emails.is_empty());
}

#[tokio::test]
async fn v1_identity_default_configuration_and_cc_bounce_are_published() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "default-cs").await;
    svc.state
        .write()
        .default_mut()
        .event_destinations
        .get_mut("default-cs")
        .unwrap()[0]
        .matching_event_types
        .push("delivery".into());
    svc.state
        .write()
        .default_mut()
        .identities
        .get_mut("example.com")
        .unwrap()
        .configuration_set_name = Some("default-cs".into());
    let resp = svc
        .handle(query_request(
            "SendEmail",
            &[
                ("Source", "Sender <sender@example.com>"),
                ("Destination.ToAddresses.member.1", "to@example.com"),
                (
                    "Destination.CcAddresses.member.1",
                    "bounce@simulator.amazonses.com",
                ),
            ],
        ))
        .await
        .unwrap();
    assert!(resp.status.is_success());
    let calls = recorder.records.lock().unwrap();
    assert_eq!(
        calls.len(),
        3,
        "Send once, normal To delivered, simulator Cc bounced"
    );
    let delivery = &calls
        .iter()
        .find(|c| c.1["eventType"] == "Delivery")
        .unwrap()
        .1;
    let bounce = &calls
        .iter()
        .find(|c| c.1["eventType"] == "Bounce")
        .unwrap()
        .1;
    assert_eq!(
        bounce["bounce"]["bouncedRecipients"][0]["emailAddress"],
        "bounce@simulator.amazonses.com"
    );
    assert_eq!(
        bounce["bounce"]["bouncedRecipients"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        delivery["delivery"]["recipients"],
        json!(["to@example.com"])
    );
    assert_eq!(
        calls[1].1["mail"]["tags"]["ses:configuration-set"],
        json!(["default-cs"])
    );
}

#[tokio::test]
async fn review_mixed_simulator_outcomes_partition_details_and_insights() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "cs").await;
    svc.state
        .write()
        .default_mut()
        .event_destinations
        .get_mut("cs")
        .unwrap()[0]
        .matching_event_types = ["send", "delivery", "bounce", "complaint"]
        .map(str::to_string)
        .to_vec();
    let response = svc
        .handle(query_request(
            "SendEmail",
            &[
                ("Source", "sender@example.com"),
                ("ConfigurationSetName", "cs"),
                ("Destination.ToAddresses.member.1", "normal@example.com"),
                (
                    "Destination.CcAddresses.member.1",
                    "bounce@simulator.amazonses.com",
                ),
                (
                    "Destination.BccAddresses.member.1",
                    "complaint@simulator.amazonses.com",
                ),
            ],
        ))
        .await
        .unwrap();
    assert!(response.status.is_success());
    let calls = recorder.records.lock().unwrap();
    assert_eq!(
        calls.len(),
        4,
        "one Send plus each distinct followup outcome"
    );
    let event = |kind: &str| &calls.iter().find(|c| c.1["eventType"] == kind).unwrap().1;
    assert_eq!(
        calls.iter().filter(|c| c.1["eventType"] == "Send").count(),
        1
    );
    assert_eq!(
        event("Delivery")["delivery"]["recipients"],
        json!(["normal@example.com", "complaint@simulator.amazonses.com"])
    );
    let bounced = event("Bounce")["bounce"]["bouncedRecipients"]
        .as_array()
        .unwrap();
    assert_eq!(bounced.len(), 1);
    assert_eq!(bounced[0]["emailAddress"], "bounce@simulator.amazonses.com");
    assert_eq!(
        event("Complaint")["complaint"]["complainedRecipients"],
        json!([{"emailAddress":"complaint@simulator.amazonses.com"}])
    );
    for (_, event) in calls.iter() {
        assert_eq!(
            event["mail"]["destination"],
            json!([
                "normal@example.com",
                "bounce@simulator.amazonses.com",
                "complaint@simulator.amazonses.com"
            ])
        );
    }
    let accounts = svc.state.read();
    let insights = &accounts.default_ref().sent_emails[0].delivery_insights;
    for (address, expected) in [
        ("normal@example.com", vec!["SEND", "DELIVERY"]),
        ("bounce@simulator.amazonses.com", vec!["SEND", "BOUNCE"]),
        (
            "complaint@simulator.amazonses.com",
            vec!["SEND", "DELIVERY", "COMPLAINT"],
        ),
    ] {
        let insight = insights.iter().find(|i| i.destination == address).unwrap();
        assert_eq!(
            insight
                .events
                .iter()
                .map(|e| e.event_type.as_str())
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[tokio::test]
async fn review_raw_comments_groups_quoted_commas_and_header_from_work_in_sandbox() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "cs").await;
    svc.state
        .write()
        .default_mut()
        .account_settings
        .production_access_enabled = false;
    for from in [
        "sender@example.com (Sender)",
        "\"Sender, One\" <sender@example.com>",
    ] {
        let text = format!("From: {from}\r\nTo: to@example.com (Recipient), Team: one@example.com, two@example.com;\r\nCc: \"Recipient, Three\" <three@example.com>\r\nX-SES-CONFIGURATION-SET: cs\r\n\r\nTo: body@other.test");
        let raw = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &text);
        let resp = svc
            .handle(query_request("SendRawEmail", &[("RawMessage.Data", &raw)]))
            .await
            .unwrap();
        assert!(resp.status.is_success());
        let calls = recorder.records.lock().unwrap();
        let event = &calls.last().unwrap().1;
        assert_eq!(event["mail"]["source"], "sender@example.com");
        assert_eq!(
            event["mail"]["destination"],
            json!([
                "to@example.com",
                "one@example.com",
                "two@example.com",
                "three@example.com"
            ])
        );
        let accounts = svc.state.read();
        let email = accounts.default_ref().sent_emails.last().unwrap();
        assert_eq!(email.from, from, "display-name header storage is preserved");
        assert_eq!(email.raw_data.as_deref(), Some(text.as_str()));
    }
}

#[tokio::test]
async fn review_api_display_names_are_bare_in_events_but_preserved_in_storage() {
    for action in [
        "SendEmail",
        "SendRawEmail",
        "SendTemplatedEmail",
        "SendBulkTemplatedEmail",
    ] {
        let (svc, recorder) = event_service();
        create_v1_event_config(&svc, "cs").await;
        let raw = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            "From: sender@example.com\r\nTo: ignored@example.com\r\n\r\nbody",
        );
        let resp = svc
            .handle(query_request(
                action,
                &[
                    ("Source", "Sender <sender@example.com>"),
                    ("ConfigurationSetName", "cs"),
                    (
                        "Destination.ToAddresses.member.1",
                        "Bounce <bounce@simulator.amazonses.com>",
                    ),
                    (
                        "Destinations.member.1",
                        "Bounce <bounce@simulator.amazonses.com>",
                    ),
                    ("RawMessage.Data", &raw),
                    ("Template", "tpl"),
                    ("TemplateData", "{}"),
                    (
                        "Destinations.member.1.Destination.ToAddresses.member.1",
                        "Bounce <bounce@simulator.amazonses.com>",
                    ),
                ],
            ))
            .await
            .unwrap();
        assert!(resp.status.is_success(), "{action}");
        let calls = recorder.records.lock().unwrap();
        assert_eq!(calls.len(), 2, "{action} emits Send and Bounce");
        for (_, event) in calls.iter() {
            assert_eq!(event["mail"]["source"], "sender@example.com");
            assert_eq!(
                event["mail"]["destination"],
                json!(["bounce@simulator.amazonses.com"])
            );
        }
        assert_eq!(calls[1].1["eventType"], "Bounce");
        assert_eq!(
            calls[1].1["bounce"]["bouncedRecipients"][0]["emailAddress"],
            "bounce@simulator.amazonses.com"
        );
        let accounts = svc.state.read();
        let sent = &accounts.default_ref().sent_emails[0];
        assert_eq!(sent.from, "Sender <sender@example.com>");
        assert_eq!(sent.to, ["Bounce <bounce@simulator.amazonses.com>"]);
        assert_eq!(
            sent.delivery_insights[0].destination,
            "bounce@simulator.amazonses.com"
        );
        assert_eq!(
            sent.delivery_insights[0]
                .events
                .iter()
                .map(|e| e.event_type.as_str())
                .collect::<Vec<_>>(),
            ["SEND", "BOUNCE"]
        );
    }
}

#[tokio::test]
async fn review_api_group_cannot_bypass_sandbox_recipient_verification() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "cs").await;
    svc.state
        .write()
        .default_mut()
        .account_settings
        .production_access_enabled = false;
    let result = svc
        .handle(query_request(
            "SendEmail",
            &[
                ("Source", "sender@example.com"),
                ("ConfigurationSetName", "cs"),
                (
                    "Destination.ToAddresses.member.1",
                    "Team: one@other.test, two@other.test;",
                ),
            ],
        ))
        .await;
    assert!(
        result.is_err(),
        "a non-mailbox API value must not skip the sandbox check"
    );
    assert!(recorder.records.lock().unwrap().is_empty());
    assert!(svc.state.read().default_ref().sent_emails.is_empty());
}

#[tokio::test]
async fn v1_bulk_cc_only_destination_publishes_with_replacement_tags() {
    let (svc, recorder) = event_service();
    create_v1_event_config(&svc, "cs").await;
    let resp = svc
        .handle(query_request(
            "SendBulkTemplatedEmail",
            &[
                ("Source", "sender@example.com"),
                ("ConfigurationSetName", "cs"),
                ("Template", "tpl"),
                ("DefaultTemplateData", r#"{"name":"Ada"}"#),
                (
                    "Destinations.member.1.Destination.CcAddresses.member.1",
                    "cc@example.com",
                ),
                (
                    "Destinations.member.1.ReplacementTemplateData",
                    r#"{"name":"Grace"}"#,
                ),
                ("DefaultTags.member.1.Name", "campaign"),
                ("DefaultTags.member.1.Value", "fall"),
                ("DefaultTags.member.2.Name", "team"),
                ("DefaultTags.member.2.Value", "ses"),
                (
                    "Destinations.member.1.ReplacementTags.member.1.Name",
                    "campaign",
                ),
                (
                    "Destinations.member.1.ReplacementTags.member.1.Value",
                    "winter",
                ),
            ],
        ))
        .await
        .unwrap();
    let xml = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
    assert!(xml.contains("<Status>Success</Status>"), "{xml}");
    let calls = recorder.records.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let event = &calls[0].1;
    assert_eq!(event["mail"]["destination"], json!(["cc@example.com"]));
    assert_eq!(event["mail"]["tags"]["campaign"], json!(["winter"]));
    assert_eq!(event["mail"]["tags"]["team"], json!(["ses"]));
    let accounts = svc.state.read();
    assert_eq!(
        accounts.default_ref().sent_emails[0].subject.as_deref(),
        Some("Hello Grace")
    );
}

fn make_request_with_query(
    method: Method,
    path: &str,
    body: &str,
    raw_query: &str,
    query_params: HashMap<String, String>,
) -> AwsRequest {
    let path_segments = fakecloud_core::path::split_path_segments(path);
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
    // Raw.Data is a JSON-protocol blob, serialized as base64 by the SDK.
    let raw = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        "From: sender@example.com\r\nTo: to@example.com\r\nSubject: =?UTF-8?B?UmF3IGjDqQ==?=\r\n\r\nBody",
    );
    let request_body = json!({
        "FromEmailAddress":"sender@example.com", "Destination":{"ToAddresses":["to@example.com"]},
        "Content":{"Raw":{"Data":raw}}
    });

    let req = make_request(
        Method::POST,
        "/v2/email/outbound-emails",
        &request_body.to_string(),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["MessageId"].as_str().is_some());

    let _mas_r = state.read();
    let s = _mas_r.default_ref();
    assert_eq!(s.sent_emails.len(), 1);
    assert_eq!(
        s.sent_emails[0].raw_data.as_deref(),
        Some("From: sender@example.com\r\nTo: to@example.com\r\nSubject: =?UTF-8?B?UmF3IGjDqQ==?=\r\n\r\nBody")
    );
    assert_eq!(s.sent_emails[0].text_body.as_deref(), Some("Body"));
    assert_eq!(s.sent_emails[0].html_body, None);
    // The RFC 2047 Subject is decoded, matching v1 SendRawEmail.
    assert_eq!(s.sent_emails[0].subject.as_deref(), Some("Raw h\u{e9}"));
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
async fn test_send_bulk_email_applies_replacement_and_default_tags() {
    let state = make_state();
    seed_identity(&state, "sender@example.com");
    enable_production_access(&state);
    let svc = SesV2Service::new(state.clone());

    // DefaultEmailTags apply to every entry; per-entry ReplacementTags add to
    // or override them by name (previously both were dropped).
    let req = make_request(
        Method::POST,
        "/v2/email/outbound-bulk-emails",
        r#"{
            "FromEmailAddress": "sender@example.com",
            "DefaultEmailTags": [
                {"Name": "campaign", "Value": "default"},
                {"Name": "env", "Value": "prod"}
            ],
            "DefaultContent": {
                "Template": {"TemplateName": null, "TemplateData": "{}"}
            },
            "BulkEmailEntries": [
                {
                    "Destination": {"ToAddresses": ["a@example.com"]},
                    "ReplacementTags": [
                        {"Name": "campaign", "Value": "winback"},
                        {"Name": "segment", "Value": "vip"}
                    ]
                },
                {"Destination": {"ToAddresses": ["b@example.com"]}}
            ]
        }"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    let guard = state.read();
    let s = guard.default_ref();
    assert_eq!(s.sent_emails.len(), 2);

    // Entry 0: ReplacementTags override `campaign` and add `segment`; `env`
    // inherited from the defaults.
    let t0: std::collections::BTreeMap<_, _> =
        s.sent_emails[0].email_tags.iter().cloned().collect();
    assert_eq!(t0.get("campaign").map(String::as_str), Some("winback"));
    assert_eq!(t0.get("segment").map(String::as_str), Some("vip"));
    assert_eq!(t0.get("env").map(String::as_str), Some("prod"));

    // Entry 1: no ReplacementTags -> exactly the defaults.
    let t1: std::collections::BTreeMap<_, _> =
        s.sent_emails[1].email_tags.iter().cloned().collect();
    assert_eq!(t1.get("campaign").map(String::as_str), Some("default"));
    assert_eq!(t1.get("env").map(String::as_str), Some("prod"));
    assert!(!t1.contains_key("segment"));
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
                max_delivery_seconds: None,
                custom_redirect_domain: None,
                https_policy: None,
                suppressed_reasons: Vec::new(),
                reputation_metrics_enabled: false,
                vdm_options: None,
                archive_arn: None,
                message_security_options: None,
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
    // as MessageRejected (matching real SES and the v1 path).
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
    assert_eq!(body["__type"], "MessageRejected");
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
    assert_eq!(body["__type"], "MessageRejected");
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
async fn list_contacts_applies_filtered_status() {
    // Regression: ListContacts must apply Filter.FilteredStatus rather than
    // returning every contact. A contact's overall status is OPT_OUT when
    // UnsubscribeAll is set, else OPT_IN.
    let state = make_state();
    let svc = SesV2Service::new(state);

    svc.handle(make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{"ContactListName": "flist"}"#,
    ))
    .await
    .unwrap();
    svc.handle(make_request(
        Method::POST,
        "/v2/email/contact-lists/flist/contacts",
        r#"{"EmailAddress": "in@example.com", "UnsubscribeAll": false}"#,
    ))
    .await
    .unwrap();
    svc.handle(make_request(
        Method::POST,
        "/v2/email/contact-lists/flist/contacts",
        r#"{"EmailAddress": "out@example.com", "UnsubscribeAll": true}"#,
    ))
    .await
    .unwrap();

    // No filter -> both contacts.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/v2/email/contact-lists/flist/contacts",
            "{}",
        ))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Contacts"].as_array().unwrap().len(), 2);

    // FilteredStatus OPT_OUT -> only the opted-out contact.
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/v2/email/contact-lists/flist/contacts/list",
            r#"{"Filter": {"FilteredStatus": "OPT_OUT"}}"#,
        ))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let contacts = body["Contacts"].as_array().unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0]["EmailAddress"], "out@example.com");
}

#[tokio::test]
async fn list_contacts_applies_topic_filter() {
    // Regression: ListContacts must apply Filter.TopicFilter, matching only
    // contacts with the requested subscription status for that topic.
    let state = make_state();
    let svc = SesV2Service::new(state);

    svc.handle(make_request(
        Method::POST,
        "/v2/email/contact-lists",
        r#"{
            "ContactListName": "tlist",
            "Topics": [{
                "TopicName": "news",
                "DisplayName": "News",
                "Description": "d",
                "DefaultSubscriptionStatus": "OPT_OUT"
            }]
        }"#,
    ))
    .await
    .unwrap();
    // Explicitly subscribed to "news".
    svc.handle(make_request(
        Method::POST,
        "/v2/email/contact-lists/tlist/contacts",
        r#"{
            "EmailAddress": "sub@example.com",
            "TopicPreferences": [{"TopicName": "news", "SubscriptionStatus": "OPT_IN"}]
        }"#,
    ))
    .await
    .unwrap();
    // No preference for "news".
    svc.handle(make_request(
        Method::POST,
        "/v2/email/contact-lists/tlist/contacts",
        r#"{"EmailAddress": "nopref@example.com"}"#,
    ))
    .await
    .unwrap();

    let resp = svc
        .handle(make_request(
            Method::POST,
            "/v2/email/contact-lists/tlist/contacts/list",
            r#"{"Filter": {"FilteredStatus": "OPT_IN", "TopicFilter": {"TopicName": "news"}}}"#,
        ))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let contacts = body["Contacts"].as_array().unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0]["EmailAddress"], "sub@example.com");
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
async fn test_mail_from_mx_uses_request_region() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    // Client operates in eu-west-1; the MAIL FROM MX host must carry that
    // region (feedback-smtp.eu-west-1.amazonses.com), matching the identity
    // ARN in the same response — not the frozen state region (us-east-1).
    let with_region = |method: Method, path: &str, body: &str| {
        let mut r = make_request(method, path, body);
        r.region = "eu-west-1".to_string();
        r
    };

    svc.handle(with_region(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "example.com"}"#,
    ))
    .await
    .unwrap();
    svc.handle(with_region(
        Method::PUT,
        "/v2/email/identities/example.com/mail-from",
        r#"{"MailFromDomain": "mail.example.com", "BehaviorOnMxFailure": "USE_DEFAULT_VALUE"}"#,
    ))
    .await
    .unwrap();

    let resp = svc
        .handle(with_region(
            Method::GET,
            "/v2/email/identities/example.com",
            "",
        ))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let dns = &body["MailFromAttributes"]["MailFromDomainDnsRecords"];
    assert_eq!(
        dns[0]["Value"], "10 feedback-smtp.eu-west-1.amazonses.com",
        "MX host must use the request region, not us-east-1"
    );
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
    assert_eq!(body["__type"].as_str(), Some("MessageRejected"));
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
                max_delivery_seconds: None,
                custom_redirect_domain: None,
                https_policy: None,
                suppressed_reasons: Vec::new(),
                reputation_metrics_enabled: false,
                vdm_options: None,
                archive_arn: None,
                message_security_options: None,
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
                    "IamRoleArn": "arn:aws:iam::123456789012:role/firehose",
                    "DeliveryStreamArn": "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream"
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
        dests[0]["KinesisFirehoseDestination"],
        json!({
            "IamRoleArn": "arn:aws:iam::123456789012:role/firehose",
            "DeliveryStreamArn": "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream"
        })
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

#[tokio::test]
async fn test_create_email_identity_byodkim() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    // BYODKIM: caller supplies selector + private key.
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "byo.example.com",
            "DkimSigningAttributes": {
                "DomainSigningSelector": "myselector",
                "DomainSigningPrivateKey": "myprivatekeybase64"
            }}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["DkimAttributes"]["SigningAttributesOrigin"], "EXTERNAL",
        "BYODKIM must report EXTERNAL origin"
    );
    // DkimAttributes carries the BYODKIM selector in Tokens; it has no
    // DomainSigningSelector member.
    assert_eq!(body["DkimAttributes"]["Tokens"], json!(["myselector"]));
    assert!(body["DkimAttributes"]
        .get("DomainSigningSelector")
        .is_none());

    // Persisted: the caller's private key + selector are stored, not a
    // freshly generated Easy-DKIM keypair.
    {
        let accts = state.read();
        let s = accts.default_ref();
        let id = s.identities.get("byo.example.com").unwrap();
        assert_eq!(id.dkim_signing_attributes_origin, "EXTERNAL");
        assert_eq!(
            id.dkim_domain_signing_selector.as_deref(),
            Some("myselector")
        );
        assert_eq!(
            id.dkim_domain_signing_private_key.as_deref(),
            Some("myprivatekeybase64")
        );
    }

    // GetEmailIdentity round-trips EXTERNAL origin + selector.
    let req = make_request(Method::GET, "/v2/email/identities/byo.example.com", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["DkimAttributes"]["SigningAttributesOrigin"],
        "EXTERNAL"
    );
    // DkimAttributes carries the BYODKIM selector in Tokens; it has no
    // DomainSigningSelector member.
    assert_eq!(body["DkimAttributes"]["Tokens"], json!(["myselector"]));
    assert!(body["DkimAttributes"]
        .get("DomainSigningSelector")
        .is_none());
}

#[tokio::test]
async fn test_create_email_identity_easy_dkim_key_length() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    // Easy DKIM with an explicit NextSigningKeyLength.
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "easy.example.com",
            "DkimSigningAttributes": { "NextSigningKeyLength": "RSA_1024_BIT" }}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DkimAttributes"]["SigningAttributesOrigin"], "AWS_SES");
    assert_eq!(
        body["DkimAttributes"]["NextSigningKeyLength"],
        "RSA_1024_BIT"
    );

    let accts = state.read();
    let s = accts.default_ref();
    let id = s.identities.get("easy.example.com").unwrap();
    assert_eq!(
        id.dkim_next_signing_key_length.as_deref(),
        Some("RSA_1024_BIT")
    );
    // Easy DKIM still auto-provisions a keypair.
    assert!(id.dkim_domain_signing_private_key.is_some());
}

// --- S/MIME certificate associations ---

const TEST_CERT_ARN: &str = "arn:aws:acm:us-east-1:123456789012:certificate/abc-123";
const TEST_CERT_ARN_2: &str = "arn:aws:acm:us-east-1:123456789012:certificate/def-456";

async fn create_identity(svc: &SesV2Service, name: &str) {
    let req = make_request(
        Method::POST,
        "/v2/email/identities",
        &format!(r#"{{"EmailIdentity": "{name}"}}"#),
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
}

async fn associate_certificate(svc: &SesV2Service, body: &str) -> AwsResponse {
    svc.handle(make_request(
        Method::POST,
        "/v2/email/identity/certificates",
        body,
    ))
    .await
    .unwrap()
}

async fn list_certificates(svc: &SesV2Service, body: &str) -> AwsResponse {
    svc.handle(make_request(
        Method::POST,
        "/v2/email/identity/certificates/list",
        body,
    ))
    .await
    .unwrap()
}

async fn disassociate_certificate(svc: &SesV2Service, body: &str) -> AwsResponse {
    svc.handle(make_request(
        Method::POST,
        "/v2/email/identity/certificates/delete",
        body,
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn test_identity_certificate_lifecycle() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    create_identity(&svc, "smime@example.com").await;

    let resp = associate_certificate(
        &svc,
        &format!(
            r#"{{"EmailIdentity": "smime@example.com", "CertificateArn": "{TEST_CERT_ARN}"}}"#
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);

    // The association is persisted against the identity, defaulting the
    // from-address to the email identity itself, and starts PROVISIONING.
    {
        let accts = state.read();
        let s = accts.default_ref();
        let certs = s.identity_certificates.get("smime@example.com").unwrap();
        assert_eq!(certs.len(), 1);
        assert_eq!(certs[0].from_address, "smime@example.com");
        assert_eq!(certs[0].certificate_arn, TEST_CERT_ARN);
        assert_eq!(certs[0].status, "PROVISIONING");
    }

    // List reports it and advances PROVISIONING -> ACTIVE.
    let resp = list_certificates(&svc, r#"{"EmailIdentity": "smime@example.com"}"#).await;
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Certificates"].as_array().unwrap().len(), 1);
    assert_eq!(body["Certificates"][0]["FromAddress"], "smime@example.com");
    assert_eq!(body["Certificates"][0]["CertificateArn"], TEST_CERT_ARN);
    assert_eq!(body["Certificates"][0]["Status"], "ACTIVE");
    assert!(body["NextToken"].is_null());

    // Disassociate drops the stored association.
    let resp = disassociate_certificate(&svc, r#"{"EmailIdentity": "smime@example.com"}"#).await;
    assert_eq!(resp.status, StatusCode::OK);

    let resp = list_certificates(&svc, r#"{"EmailIdentity": "smime@example.com"}"#).await;
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["Certificates"].as_array().unwrap().is_empty());
    {
        let accts = state.read();
        let s = accts.default_ref();
        assert!(!s.identity_certificates.contains_key("smime@example.com"));
    }
}

#[tokio::test]
async fn test_associate_certificate_duplicate_conflicts() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    create_identity(&svc, "dup@example.com").await;

    let body =
        format!(r#"{{"EmailIdentity": "dup@example.com", "CertificateArn": "{TEST_CERT_ARN}"}}"#);
    assert_eq!(
        associate_certificate(&svc, &body).await.status,
        StatusCode::OK
    );
    let resp = associate_certificate(&svc, &body).await;
    assert_eq!(resp.status, StatusCode::CONFLICT);
    let err: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(err["__type"], "AlreadyExistsException");

    // A DEPROVISIONING association is replaced rather than rejected.
    {
        let mut accts = state.write();
        let s = accts.get_or_create("123456789012");
        s.identity_certificates.get_mut("dup@example.com").unwrap()[0].status =
            "DEPROVISIONING".to_string();
    }
    let replacement =
        format!(r#"{{"EmailIdentity": "dup@example.com", "CertificateArn": "{TEST_CERT_ARN_2}"}}"#);
    assert_eq!(
        associate_certificate(&svc, &replacement).await.status,
        StatusCode::OK
    );
    let accts = state.read();
    let s = accts.default_ref();
    let certs = s.identity_certificates.get("dup@example.com").unwrap();
    assert_eq!(certs.len(), 1);
    assert_eq!(certs[0].certificate_arn, TEST_CERT_ARN_2);
    assert_eq!(certs[0].status, "PROVISIONING");
}

#[tokio::test]
async fn test_associate_certificate_unknown_identity_is_404() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let resp = associate_certificate(
        &svc,
        &format!(r#"{{"EmailIdentity": "nope@example.com", "CertificateArn": "{TEST_CERT_ARN}"}}"#),
    )
    .await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    let err: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(err["__type"], "NotFoundException");
}

#[tokio::test]
async fn test_associate_certificate_validates_input() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    create_identity(&svc, "val@example.com").await;
    create_identity(&svc, "example.org").await;

    // Missing CertificateArn.
    let resp = associate_certificate(&svc, r#"{"EmailIdentity": "val@example.com"}"#).await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let err: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(err["__type"], "BadRequestException");

    // Missing EmailIdentity.
    let resp =
        associate_certificate(&svc, &format!(r#"{{"CertificateArn": "{TEST_CERT_ARN}"}}"#)).await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // CertificateArn that isn't an ACM certificate ARN.
    let resp = associate_certificate(
        &svc,
        r#"{"EmailIdentity": "val@example.com", "CertificateArn": "arn:aws:acm:us-east-1:123456789012:key/abc"}"#,
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // FromAddress that doesn't match an email-address identity.
    let resp = associate_certificate(
        &svc,
        &format!(
            r#"{{"EmailIdentity": "val@example.com", "FromAddress": "other@example.com", "CertificateArn": "{TEST_CERT_ARN}"}}"#
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Domain identity without a FromAddress.
    let resp = associate_certificate(
        &svc,
        &format!(r#"{{"EmailIdentity": "example.org", "CertificateArn": "{TEST_CERT_ARN}"}}"#),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Domain identity with a FromAddress in another domain.
    let resp = associate_certificate(
        &svc,
        &format!(
            r#"{{"EmailIdentity": "example.org", "FromAddress": "a@elsewhere.net", "CertificateArn": "{TEST_CERT_ARN}"}}"#
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_associate_certificate_accepts_subdomain_from_address() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    create_identity(&svc, "example.org").await;

    let resp = associate_certificate(
        &svc,
        &format!(
            r#"{{"EmailIdentity": "example.org", "FromAddress": "sales@mail.example.org", "CertificateArn": "{TEST_CERT_ARN}"}}"#
        ),
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);

    let accts = state.read();
    let s = accts.default_ref();
    let certs = s.identity_certificates.get("example.org").unwrap();
    assert_eq!(certs[0].from_address, "sales@mail.example.org");
}

#[tokio::test]
async fn test_disassociate_certificate_is_idempotent_but_404s_unknown_identity() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    create_identity(&svc, "idem@example.com").await;

    // No association yet: AWS documents this as a no-op success.
    let resp = disassociate_certificate(&svc, r#"{"EmailIdentity": "idem@example.com"}"#).await;
    assert_eq!(resp.status, StatusCode::OK);

    // Unknown identity is the only NotFoundException case.
    let resp = disassociate_certificate(&svc, r#"{"EmailIdentity": "ghost@example.com"}"#).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    let err: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(err["__type"], "NotFoundException");

    // Missing EmailIdentity is a BadRequestException.
    let resp = disassociate_certificate(&svc, "{}").await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_list_certificates_paginates() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    create_identity(&svc, "example.org").await;

    for local in ["a", "b", "c"] {
        let resp = associate_certificate(
            &svc,
            &format!(
                r#"{{"EmailIdentity": "example.org", "FromAddress": "{local}@example.org", "CertificateArn": "{TEST_CERT_ARN}"}}"#
            ),
        )
        .await;
        assert_eq!(resp.status, StatusCode::OK);
    }

    let resp = list_certificates(&svc, r#"{"EmailIdentity": "example.org", "PageSize": 2}"#).await;
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Certificates"].as_array().unwrap().len(), 2);
    assert_eq!(body["Certificates"][0]["FromAddress"], "a@example.org");
    assert_eq!(body["NextToken"], "c@example.org");

    let resp = list_certificates(
        &svc,
        r#"{"EmailIdentity": "example.org", "PageSize": 2, "NextToken": "c@example.org"}"#,
    )
    .await;
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Certificates"].as_array().unwrap().len(), 1);
    assert_eq!(body["Certificates"][0]["FromAddress"], "c@example.org");
    assert!(body["NextToken"].is_null());
}

#[tokio::test]
async fn test_list_certificates_errors() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    create_identity(&svc, "list@example.com").await;

    // Unknown identity.
    let resp = list_certificates(&svc, r#"{"EmailIdentity": "ghost@example.com"}"#).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    let err: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(err["__type"], "NotFoundException");

    // Non-positive PageSize.
    let resp = list_certificates(
        &svc,
        r#"{"EmailIdentity": "list@example.com", "PageSize": 0}"#,
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // Missing EmailIdentity.
    let resp = list_certificates(&svc, "{}").await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_delete_identity_drops_certificate_associations() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    create_identity(&svc, "gone@example.com").await;
    associate_certificate(
        &svc,
        &format!(r#"{{"EmailIdentity": "gone@example.com", "CertificateArn": "{TEST_CERT_ARN}"}}"#),
    )
    .await;

    let req = make_request(Method::DELETE, "/v2/email/identities/gone@example.com", "");
    assert_eq!(svc.handle(req).await.unwrap().status, StatusCode::OK);

    let accts = state.read();
    let s = accts.default_ref();
    assert!(!s.identity_certificates.contains_key("gone@example.com"));
}

// --- UpdateConfigurationSet ---

async fn create_configuration_set(svc: &SesV2Service, body: &str) -> AwsResponse {
    svc.handle(make_request(
        Method::POST,
        "/v2/email/configuration-sets",
        body,
    ))
    .await
    .unwrap()
}

async fn update_configuration_set(svc: &SesV2Service, body: &str) -> AwsResponse {
    svc.handle(make_request(
        Method::POST,
        "/v2/email/update-configuration-sets",
        body,
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn test_update_configuration_set_message_security_options() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());

    let resp = create_configuration_set(
        &svc,
        r#"{"ConfigurationSetName": "cs-security", "ReputationOptions": {"ReputationMetricsEnabled": true}}"#,
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);

    let resp = update_configuration_set(
        &svc,
        r#"{"ConfigurationSetName": "cs-security",
            "MessageSecurityOptions": {"SigningScheme": {"SmimeScheme": {"SignatureFormat": "DETACHED"}}}}"#,
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);

    // Stored on the configuration set...
    {
        let accts = state.read();
        let s = accts.default_ref();
        let cs = s.configuration_sets.get("cs-security").unwrap();
        assert_eq!(
            cs.message_security_options.as_ref().unwrap()["SigningScheme"]["SmimeScheme"]
                ["SignatureFormat"],
            "DETACHED"
        );
        // ... and the partial update left the other attributes alone.
        assert!(cs.reputation_metrics_enabled);
    }

    // ... and echoed by GetConfigurationSet.
    let req = make_request(Method::GET, "/v2/email/configuration-sets/cs-security", "");
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["MessageSecurityOptions"]["SigningScheme"]["SmimeScheme"]["SignatureFormat"],
        "DETACHED"
    );
    assert_eq!(body["ReputationOptions"]["ReputationMetricsEnabled"], true);
}

#[tokio::test]
async fn test_create_configuration_set_round_trips_message_security_options() {
    let state = make_state();
    let svc = SesV2Service::new(state);

    let resp = create_configuration_set(
        &svc,
        r#"{"ConfigurationSetName": "cs-create-security",
            "MessageSecurityOptions": {"SigningScheme": {"DefaultScheme": {}}}}"#,
    )
    .await;
    assert_eq!(resp.status, StatusCode::OK);

    let req = make_request(
        Method::GET,
        "/v2/email/configuration-sets/cs-create-security",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["MessageSecurityOptions"]["SigningScheme"]["DefaultScheme"].is_object());
}

#[tokio::test]
async fn test_update_configuration_set_errors() {
    let state = make_state();
    let svc = SesV2Service::new(state);
    create_configuration_set(&svc, r#"{"ConfigurationSetName": "cs-err"}"#).await;

    // Unknown configuration set.
    let resp = update_configuration_set(&svc, r#"{"ConfigurationSetName": "cs-missing"}"#).await;
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    let err: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(err["__type"], "NotFoundException");

    // Missing name.
    let resp = update_configuration_set(&svc, "{}").await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);

    // SigningScheme is a union: two members set is invalid.
    let resp = update_configuration_set(
        &svc,
        r#"{"ConfigurationSetName": "cs-err",
            "MessageSecurityOptions": {"SigningScheme": {"DefaultScheme": {}, "SmimeScheme": {}}}}"#,
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let err: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(err["__type"], "BadRequestException");

    // DETACHED is the only modeled signature format.
    let resp = update_configuration_set(
        &svc,
        r#"{"ConfigurationSetName": "cs-err",
            "MessageSecurityOptions": {"SigningScheme": {"SmimeScheme": {"SignatureFormat": "ATTACHED"}}}}"#,
    )
    .await;
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_update_configuration_set_route_is_not_a_configuration_set_name() {
    // `/v2/email/update-configuration-sets` is a fixed URI, so it must not
    // be mistaken for a configuration set named "update-configuration-sets"
    // nor fall through to the unknown-operation handler.
    let state = make_state();
    let svc = SesV2Service::new(state);
    let req = make_request(
        Method::POST,
        "/v2/email/update-configuration-sets",
        r#"{"ConfigurationSetName": "nope"}"#,
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::NOT_FOUND);
    let err: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(err["__type"], "NotFoundException");
}

#[tokio::test]
async fn china_region_resource_arns_use_the_aws_cn_partition() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    let in_china = |method: Method, path: &str, body: &str| {
        let mut req = make_request(method, path, body);
        req.region = "cn-north-1".to_string();
        req
    };
    svc.handle(in_china(
        Method::POST,
        "/v2/email/identities",
        r#"{"EmailIdentity": "cn@example.com", "Tags": [{"Key": "env", "Value": "cn"}]}"#,
    ))
    .await
    .unwrap();
    svc.handle(in_china(
        Method::POST,
        "/v2/email/configuration-sets",
        r#"{"ConfigurationSetName": "cn-set", "Tags": [{"Key": "env", "Value": "cn"}]}"#,
    ))
    .await
    .unwrap();
    {
        let accounts = state.read();
        let tags = &accounts.default_ref().tags;
        assert!(tags.contains_key("arn:aws-cn:ses:cn-north-1:123456789012:identity/cn@example.com"));
        assert!(
            tags.contains_key("arn:aws-cn:ses:cn-north-1:123456789012:configuration-set/cn-set")
        );
    }

    let resp = svc
        .handle(in_china(
            Method::GET,
            "/v2/email/identities/cn@example.com",
            "",
        ))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Tags"][0]["Value"], "cn");
}

async fn post_json(svc: &SesV2Service, path: &str, body: Value) -> (StatusCode, Value) {
    let resp = svc
        .handle(make_request(Method::POST, path, &body.to_string()))
        .await
        .unwrap();
    let status = resp.status;
    let json: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    (status, json)
}

#[tokio::test]
async fn list_configuration_sets_filters_by_name_and_pages() {
    let svc = SesV2Service::new(make_state());
    for name in ["alpha-prod", "beta-prod", "gamma-dev", "delta-prod"] {
        let (status, _) = post_json(
            &svc,
            "/v2/email/configuration-sets",
            json!({"ConfigurationSetName": name}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    let (_, body) = post_json(
        &svc,
        "/v2/email/list-configuration-sets",
        json!({"Filter": {"CONFIGURATION_SET_NAME_CONTAINS": "prod"}}),
    )
    .await;
    assert_eq!(
        body["ConfigurationSets"],
        json!(["alpha-prod", "beta-prod", "delta-prod"])
    );
    assert!(body.get("NextToken").is_none());

    // Paged: two per page, the token resumes where the first page stopped.
    let (_, page1) = post_json(
        &svc,
        "/v2/email/list-configuration-sets",
        json!({"PageSize": 2, "Filter": {"CONFIGURATION_SET_NAME_CONTAINS": "prod"}}),
    )
    .await;
    assert_eq!(
        page1["ConfigurationSets"],
        json!(["alpha-prod", "beta-prod"])
    );
    let token = page1["NextToken"].as_str().unwrap().to_string();
    let (_, page2) = post_json(
        &svc,
        "/v2/email/list-configuration-sets",
        json!({"PageSize": 2, "NextToken": token,
               "Filter": {"CONFIGURATION_SET_NAME_CONTAINS": "prod"}}),
    )
    .await;
    assert_eq!(page2["ConfigurationSets"], json!(["delta-prod"]));
    assert!(page2.get("NextToken").is_none());

    // The legacy GET binding still lists everything.
    let resp = svc
        .handle(make_request(
            Method::GET,
            "/v2/email/configuration-sets",
            "",
        ))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ConfigurationSets"].as_array().unwrap().len(), 4);

    // An unknown filter key is rejected.
    let err = svc
        .handle(make_request(
            Method::POST,
            "/v2/email/list-configuration-sets",
            &json!({"Filter": {"NAME_EQUALS": "x"}}).to_string(),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "BadRequestException");
}

#[tokio::test]
async fn list_email_identities_filters_by_name_type_and_verification_status() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    seed_identity(&state, "ops@example.com");
    seed_identity(&state, "example.com");
    seed_identity(&state, "pending.example.org");
    state
        .write()
        .get_or_create("123456789012")
        .identities
        .get_mut("pending.example.org")
        .unwrap()
        .verified = false;

    let list = |filter: Value| {
        let svc = &svc;
        async move {
            post_json(svc, "/v2/email/list-identities", json!({"Filter": filter}))
                .await
                .1
        }
    };
    let names = |body: &Value| -> Vec<String> {
        body["EmailIdentities"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["IdentityName"].as_str().unwrap().to_string())
            .collect()
    };

    let body = list(json!({"IDENTITY_TYPE": "DOMAIN"})).await;
    assert_eq!(names(&body), vec!["example.com", "pending.example.org"]);

    let body = list(json!({"IDENTITY_NAME_CONTAINS": "example.com"})).await;
    assert_eq!(names(&body), vec!["example.com", "ops@example.com"]);

    let body = list(json!({"VERIFICATION_STATUS": "PENDING"})).await;
    assert_eq!(names(&body), vec!["pending.example.org"]);
    assert_eq!(body["EmailIdentities"][0]["VerificationStatus"], "PENDING");

    // Filters combine (AND).
    let body = list(json!({"IDENTITY_TYPE": "DOMAIN", "VERIFICATION_STATUS": "SUCCESS"})).await;
    assert_eq!(names(&body), vec!["example.com"]);

    let (_, page) = post_json(&svc, "/v2/email/list-identities", json!({"PageSize": 1})).await;
    assert_eq!(page["EmailIdentities"].as_array().unwrap().len(), 1);
    assert_eq!(page["NextToken"], "ops@example.com");
}

#[tokio::test]
async fn list_tenants_filters_and_reports_sending_status() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    for name in ["acme-eu", "acme-us", "globex"] {
        post_json(&svc, "/v2/email/tenants", json!({"TenantName": name})).await;
    }
    state
        .write()
        .get_or_create("123456789012")
        .tenants
        .get_mut("acme-us")
        .unwrap()
        .sending_status = "DISABLED".to_string();

    let (_, body) = post_json(&svc, "/v2/email/tenants/list", json!({})).await;
    assert_eq!(body["Tenants"].as_array().unwrap().len(), 3);
    assert_eq!(body["Tenants"][0]["SendingStatus"], "ENABLED");

    let (_, body) = post_json(
        &svc,
        "/v2/email/tenants/list",
        json!({"Filter": {"TENANT_NAME_CONTAINS": "acme"}}),
    )
    .await;
    assert_eq!(body["Tenants"].as_array().unwrap().len(), 2);

    let (_, body) = post_json(
        &svc,
        "/v2/email/tenants/list",
        json!({"Filter": {"SENDING_STATUS": "DISABLED"}}),
    )
    .await;
    let tenants = body["Tenants"].as_array().unwrap();
    assert_eq!(tenants.len(), 1);
    assert_eq!(tenants[0]["TenantName"], "acme-us");
    assert_eq!(tenants[0]["SendingStatus"], "DISABLED");
}

#[tokio::test]
async fn get_event_destinations_renders_v1_created_destination_in_v2_shape() {
    use crate::v1::tests::make_v1_request;
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    for req in [
        make_v1_request(
            "CreateConfigurationSet",
            vec![("ConfigurationSet.Name", "cs")],
        ),
        make_v1_request(
            "CreateConfigurationSetEventDestination",
            vec![
                ("ConfigurationSetName", "cs"),
                ("EventDestination.Name", "fh"),
                ("EventDestination.Enabled", "true"),
                ("EventDestination.MatchingEventTypes.member.1", "send"),
                (
                    "EventDestination.MatchingEventTypes.member.2",
                    "renderingFailure",
                ),
                (
                    "EventDestination.KinesisFirehoseDestination.IAMRoleARN",
                    "arn:aws:iam::123456789012:role/r",
                ),
                (
                    "EventDestination.KinesisFirehoseDestination.DeliveryStreamARN",
                    "arn:aws:firehose:us-east-1:123456789012:deliverystream/s",
                ),
            ],
        ),
        // Role omitted: v1 stores "", which v2 must not render.
        make_v1_request(
            "CreateConfigurationSetEventDestination",
            vec![
                ("ConfigurationSetName", "cs"),
                ("EventDestination.Name", "fh-no-role"),
                ("EventDestination.Enabled", "true"),
                ("EventDestination.MatchingEventTypes.member.1", "send"),
                (
                    "EventDestination.KinesisFirehoseDestination.DeliveryStreamARN",
                    "arn:aws:firehose:us-east-1:123456789012:deliverystream/s",
                ),
            ],
        ),
        make_v1_request(
            "CreateConfigurationSetEventDestination",
            vec![
                ("ConfigurationSetName", "cs"),
                ("EventDestination.Name", "cw"),
                ("EventDestination.Enabled", "true"),
                ("EventDestination.MatchingEventTypes.member.1", "send"),
                (
                    "EventDestination.CloudWatchDestination.DimensionConfigurations.member.1.DimensionName",
                    "tag",
                ),
                (
                    "EventDestination.CloudWatchDestination.DimensionConfigurations.member.1.DimensionValueSource",
                    "messageTag",
                ),
                (
                    "EventDestination.CloudWatchDestination.DimensionConfigurations.member.1.DefaultDimensionValue",
                    "none",
                ),
            ],
        ),
    ] {
        assert_eq!(svc.handle(req).await.unwrap().status, StatusCode::OK);
    }
    // As the CloudFormation provisioner stores it: the template's `TopicARN`.
    state
        .write()
        .default_mut()
        .event_destinations
        .get_mut("cs")
        .unwrap()
        .push(parse_event_destination_definition(
            "cfn-sns",
            &json!({
                "Enabled": true,
                "MatchingEventTypes": ["send"],
                "SnsDestination": {"TopicARN": "arn:aws:sns:us-east-1:123456789012:t"}
            }),
        ));

    let req = make_request(
        Method::GET,
        "/v2/email/configuration-sets/cs/event-destinations",
        "",
    );
    let resp = svc.handle(req).await.unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let dest = &body["EventDestinations"][0];
    assert_eq!(
        dest["MatchingEventTypes"],
        json!(["SEND", "RENDERING_FAILURE"])
    );
    assert_eq!(
        dest["KinesisFirehoseDestination"],
        json!({
            "IamRoleArn": "arn:aws:iam::123456789012:role/r",
            "DeliveryStreamArn": "arn:aws:firehose:us-east-1:123456789012:deliverystream/s"
        })
    );
    let dests = body["EventDestinations"].as_array().unwrap();
    let by_name = |name: &str| {
        dests
            .iter()
            .find(|d| d["Name"] == name)
            .unwrap_or_else(|| panic!("{name} missing: {body}"))
    };
    assert_eq!(
        by_name("fh-no-role")["KinesisFirehoseDestination"],
        json!({"DeliveryStreamArn": "arn:aws:firehose:us-east-1:123456789012:deliverystream/s"})
    );
    assert_eq!(
        by_name("cw")["CloudWatchDestination"]["DimensionConfigurations"][0]
            ["DimensionValueSource"],
        "MESSAGE_TAG"
    );
    assert_eq!(
        by_name("cfn-sns")["SnsDestination"],
        json!({"TopicArn": "arn:aws:sns:us-east-1:123456789012:t"})
    );
    // Stored spelling is untouched, so v1 still round-trips its own names.
    let mas = state.read();
    let stored = &mas.default_ref().event_destinations["cs"][0];
    assert_eq!(stored.matching_event_types, ["send", "renderingFailure"]);
}

/// Status (and the response, on success) of a handler call, mapping an
/// `AwsServiceError` to its HTTP status.
fn status_of(r: Result<AwsResponse, AwsServiceError>) -> (StatusCode, Option<AwsResponse>) {
    match r {
        Ok(resp) => (resp.status, Some(resp)),
        Err(e) => (e.status(), None),
    }
}

fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[tokio::test]
async fn suppressed_destinations_are_scoped_per_tenant() {
    let state = make_state();
    seed_identity(&state, "example.com");
    let svc = SesV2Service::new(state);
    let put = |body: &str| make_request(Method::PUT, "/v2/email/suppression/addresses", body);

    // An empty TenantName violates @length(min: 1).
    let resp = status_of(
        svc.handle(put(
            r#"{"EmailAddress":"a@x.com","Reason":"BOUNCE","TenantName":""}"#,
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::BAD_REQUEST);
    let resp = status_of(
        svc.handle(make_request_with_query(
            Method::GET,
            "/v2/email/suppression/addresses",
            "",
            "TenantName=",
            q(&[("TenantName", "")]),
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::BAD_REQUEST);

    // An unknown tenant is not found.
    let resp = status_of(
        svc.handle(put(
            r#"{"EmailAddress":"a@x.com","Reason":"BOUNCE","TenantName":"ghost"}"#,
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::NOT_FOUND);

    let resp = status_of(
        svc.handle(make_request(
            Method::POST,
            "/v2/email/tenants",
            r#"{"TenantName":"t1"}"#,
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::OK);
    let resp = status_of(
        svc.handle(put(
            r#"{"EmailAddress":"tenant@x.com","Reason":"COMPLAINT","TenantName":"t1"}"#,
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::OK);
    let resp = status_of(
        svc.handle(put(r#"{"EmailAddress":"acct@x.com","Reason":"BOUNCE"}"#))
            .await,
    );
    assert_eq!(resp.0, StatusCode::OK);

    let list = |query: Vec<(&str, &str)>| {
        let raw = query
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        make_request_with_query(
            Method::GET,
            "/v2/email/suppression/addresses",
            "",
            &raw,
            q(&query),
        )
    };
    let emails = |resp: &AwsResponse| -> Vec<String> {
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        body["SuppressedDestinationSummaries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["EmailAddress"].as_str().unwrap().to_string())
            .collect()
    };
    let resp = status_of(svc.handle(list(vec![])).await);
    assert_eq!(emails(resp.1.as_ref().unwrap()), vec!["acct@x.com"]);
    let resp = status_of(svc.handle(list(vec![("TenantName", "t1")])).await);
    assert_eq!(emails(resp.1.as_ref().unwrap()), vec!["tenant@x.com"]);
    let resp = status_of(
        svc.handle(list(vec![("TenantName", "t1"), ("Reason", "BOUNCE")]))
            .await,
    );
    assert!(emails(resp.1.as_ref().unwrap()).is_empty());

    // Get / Delete honor the tenant scope too.
    let resp = status_of(
        svc.handle(make_request_with_query(
            Method::GET,
            "/v2/email/suppression/addresses/tenant@x.com",
            "",
            "TenantName=t1",
            q(&[("TenantName", "t1")]),
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::OK);
    let resp = status_of(
        svc.handle(make_request(
            Method::GET,
            "/v2/email/suppression/addresses/tenant@x.com",
            "",
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::NOT_FOUND);

    // A send on behalf of the tenant is blocked by the tenant's list; the
    // same recipient is deliverable outside the tenant.
    let send = |tenant: Option<&str>| {
        let mut body = json!({
            "FromEmailAddress": "sender@example.com",
            "Destination": {"ToAddresses": ["tenant@x.com"]},
            "Content": {"Simple": {"Subject": {"Data": "s"}, "Body": {"Text": {"Data": "b"}}}}
        });
        if let Some(t) = tenant {
            body["TenantName"] = json!(t);
        }
        make_request(Method::POST, "/v2/email/outbound-emails", &body.to_string())
    };
    let resp = status_of(svc.handle(send(Some("t1"))).await);
    assert_eq!(resp.0, StatusCode::BAD_REQUEST);
    let resp = status_of(svc.handle(send(None)).await);
    assert_eq!(resp.0, StatusCode::OK);

    let resp = status_of(
        svc.handle(make_request_with_query(
            Method::DELETE,
            "/v2/email/suppression/addresses/tenant@x.com",
            "",
            "TenantName=t1",
            q(&[("TenantName", "t1")]),
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::OK);
    let resp = status_of(svc.handle(send(Some("t1"))).await);
    assert_eq!(resp.0, StatusCode::OK);
}

#[tokio::test]
async fn suppression_tenant_delete_miss_and_bad_dates() {
    let state = make_state();
    let svc = SesV2Service::new(state.clone());
    let resp = status_of(
        svc.handle(make_request(
            Method::POST,
            "/v2/email/tenants",
            r#"{"TenantName":"t2"}"#,
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::OK);
    // Deleting an address the tenant never suppressed is a miss, and must not
    // materialize an empty list for the tenant.
    let resp = status_of(
        svc.handle(make_request_with_query(
            Method::DELETE,
            "/v2/email/suppression/addresses/nobody@x.com",
            "",
            "TenantName=t2",
            q(&[("TenantName", "t2")]),
        ))
        .await,
    );
    assert_eq!(resp.0, StatusCode::NOT_FOUND);
    assert!(!state
        .read()
        .default_ref()
        .tenant_suppressed_destinations
        .contains_key("t2"));

    for bad in ["NaN", "inf", "-inf", "1e300", "yesterday"] {
        let resp = status_of(
            svc.handle(make_request_with_query(
                Method::GET,
                "/v2/email/suppression/addresses",
                "",
                &format!("StartDate={bad}"),
                q(&[("StartDate", bad)]),
            ))
            .await,
        );
        assert_eq!(resp.0, StatusCode::BAD_REQUEST, "{bad}");
    }
}

#[tokio::test]
async fn contact_list_topic_description_is_omitted_when_unset() {
    let svc = SesV2Service::new(make_state());
    let resp = svc
        .handle(make_request(
            Method::POST,
            "/v2/email/contact-lists",
            r#"{"ContactListName":"cl","Topics":[
                {"TopicName":"t","DisplayName":"T","DefaultSubscriptionStatus":"OPT_IN"},
                {"TopicName":"u","DisplayName":"U","Description":"news","DefaultSubscriptionStatus":"OPT_OUT"}
            ]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let resp = svc
        .handle(make_request(Method::GET, "/v2/email/contact-lists/cl", ""))
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["Topics"][0],
        json!({"TopicName":"t","DisplayName":"T","DefaultSubscriptionStatus":"OPT_IN"})
    );
    assert_eq!(body["Topics"][1]["Description"], "news");
}
