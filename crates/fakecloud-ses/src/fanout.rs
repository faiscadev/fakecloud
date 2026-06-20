//! SES event fanout: publishes send/delivery/bounce/complaint events
//! to configured event destinations (SNS topics, EventBridge buses).

use base64::Engine;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;

use crate::state::{
    DeliveryInsightEvent, EmailRecipientInsight, EventDestination, EventDestinationDispatch,
    SentEmail, SharedSesState, SuppressedDestination,
};

/// Shared references needed for cross-service event delivery.
#[derive(Clone)]
pub struct SesDeliveryContext {
    pub ses_state: SharedSesState,
    pub delivery_bus: Arc<DeliveryBus>,
    pub account_id: String,
    pub region: String,
}

/// Mailbox simulator addresses.
const BOUNCE_ADDR: &str = "bounce@simulator.amazonses.com";
const COMPLAINT_ADDR: &str = "complaint@simulator.amazonses.com";
#[cfg(test)]
const SUCCESS_ADDR: &str = "success@simulator.amazonses.com";
const SUPPRESSION_ADDR: &str = "suppressionlist@simulator.amazonses.com";
const OOTO_ADDR: &str = "ooto@simulator.amazonses.com";
const SOFTBOUNCE_ADDR: &str = "softbounce@simulator.amazonses.com";
const FORWARDING_ADDR: &str = "forwarding@simulator.amazonses.com";
const TRANSIENT_BOUNCE_ADDR: &str = "transient-bounce@simulator.amazonses.com";

/// The event types we generate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SesEventType {
    Send,
    Delivery,
    Bounce,
    Complaint,
}

impl SesEventType {
    fn as_str(self) -> &'static str {
        match self {
            SesEventType::Send => "SEND",
            SesEventType::Delivery => "DELIVERY",
            SesEventType::Bounce => "BOUNCE",
            SesEventType::Complaint => "COMPLAINT",
        }
    }

    fn event_type_name(self) -> &'static str {
        match self {
            SesEventType::Send => "Send",
            SesEventType::Delivery => "Delivery",
            SesEventType::Bounce => "Bounce",
            SesEventType::Complaint => "Complaint",
        }
    }
}

/// Build the SES event JSON payload matching the AWS notification format.
pub fn build_ses_event(event_type: SesEventType, email: &SentEmail) -> serde_json::Value {
    let mut event = json!({
        "eventType": event_type.event_type_name(),
        "mail": {
            "messageId": email.message_id,
            "source": email.from,
            "destination": email.to,
            "timestamp": email.timestamp.to_rfc3339(),
        },
    });

    // Add event-type-specific detail blocks
    match event_type {
        SesEventType::Send => {
            event["send"] = json!({});
        }
        SesEventType::Delivery => {
            event["delivery"] = json!({
                "timestamp": Utc::now().to_rfc3339(),
                "recipients": email.to,
                "processingTimeMillis": 42,
                "smtpResponse": "250 2.0.0 Ok",
            });
        }
        SesEventType::Bounce => {
            let bounced: Vec<serde_json::Value> = email
                .to
                .iter()
                .map(|addr| {
                    json!({
                        "emailAddress": addr,
                        "action": "failed",
                        "status": "5.1.1",
                        "diagnosticCode": "smtp; 550 5.1.1 user unknown",
                    })
                })
                .collect();
            event["bounce"] = json!({
                "bounceType": "Permanent",
                "bounceSubType": "General",
                "bouncedRecipients": bounced,
                "timestamp": Utc::now().to_rfc3339(),
            });
        }
        SesEventType::Complaint => {
            let complained: Vec<serde_json::Value> = email
                .to
                .iter()
                .map(|addr| json!({ "emailAddress": addr }))
                .collect();
            event["complaint"] = json!({
                "complainedRecipients": complained,
                "complaintFeedbackType": "abuse",
                "timestamp": Utc::now().to_rfc3339(),
            });
        }
    }

    event
}

/// Determine which event types to generate based on recipient addresses.
/// Returns the list of event types to emit and whether to add to suppression list.
pub fn classify_recipients(recipients: &[String]) -> (Vec<SesEventType>, bool) {
    let mut events = Vec::new();
    let mut suppress = false;

    // Check for simulator addresses in any recipient
    let has_bounce = recipients.iter().any(|r| r == BOUNCE_ADDR);
    let has_complaint = recipients.iter().any(|r| r == COMPLAINT_ADDR);
    let has_suppression = recipients.iter().any(|r| r == SUPPRESSION_ADDR);
    let has_ooto = recipients.iter().any(|r| r == OOTO_ADDR);
    let has_softbounce = recipients.iter().any(|r| r == SOFTBOUNCE_ADDR);
    let has_forwarding = recipients.iter().any(|r| r == FORWARDING_ADDR);
    let has_transient_bounce = recipients.iter().any(|r| r == TRANSIENT_BOUNCE_ADDR);
    // success@simulator is the default behavior, no special handling needed

    if has_bounce {
        events.push(SesEventType::Send);
        events.push(SesEventType::Bounce);
    } else if has_complaint {
        events.push(SesEventType::Send);
        events.push(SesEventType::Delivery);
        events.push(SesEventType::Complaint);
    } else if has_suppression {
        events.push(SesEventType::Send);
        events.push(SesEventType::Bounce);
        suppress = true;
    } else if has_ooto {
        events.push(SesEventType::Send);
        events.push(SesEventType::Delivery);
        events.push(SesEventType::Complaint);
    } else if has_softbounce {
        events.push(SesEventType::Send);
        events.push(SesEventType::Bounce);
    } else if has_forwarding {
        events.push(SesEventType::Send);
        events.push(SesEventType::Delivery);
    } else if has_transient_bounce {
        events.push(SesEventType::Send);
        events.push(SesEventType::Bounce);
    } else {
        // Normal send or success@simulator
        events.push(SesEventType::Send);
        events.push(SesEventType::Delivery);
    }

    (events, suppress)
}

/// Check if any recipient is on the suppression list AND the stored
/// reason is enforced under the effective `SuppressedReasons` filter
/// (configuration-set scope first, then account-level fallback). Lookup
/// is case-insensitive. Returns the suppressed address if found.
pub fn check_suppression_list(
    ses_state: &SharedSesState,
    recipients: &[String],
    config_set_name: Option<&str>,
) -> Option<String> {
    let mas = ses_state.read();
    let state = mas.default_ref();
    for addr in recipients {
        if state.suppressed_match(addr, config_set_name).is_some() {
            return Some(addr.clone());
        }
    }
    None
}

/// Resolve the configuration set name for an email send.
/// Checks the explicit request param first, then the identity's default.
pub fn resolve_config_set(
    ses_state: &SharedSesState,
    explicit_config_set: Option<&str>,
    from_address: &str,
) -> Option<String> {
    if let Some(name) = explicit_config_set {
        return Some(name.to_string());
    }

    // Check identity's default configuration set
    let mas = ses_state.read();
    let state = mas.default_ref();
    if let Some(identity) = state.identities.get(from_address) {
        return identity.configuration_set_name.clone();
    }
    // Also check domain identity
    if let Some(at_pos) = from_address.find('@') {
        let domain = &from_address[at_pos + 1..];
        if let Some(identity) = state.identities.get(domain) {
            return identity.configuration_set_name.clone();
        }
    }
    None
}

/// Get enabled event destinations for a configuration set that match the given event type.
fn get_matching_destinations(
    ses_state: &SharedSesState,
    config_set_name: &str,
    event_type: SesEventType,
) -> Vec<EventDestination> {
    let mas = ses_state.read();
    let state = mas.default_ref();
    let event_type_str = event_type.as_str();

    state
        .event_destinations
        .get(config_set_name)
        .map(|dests| {
            dests
                .iter()
                .filter(|d| d.enabled && d.matching_event_types.iter().any(|t| t == event_type_str))
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Fan out a single event to all matching destinations.
fn deliver_event(
    ctx: &SesDeliveryContext,
    event: &serde_json::Value,
    event_type: SesEventType,
    config_set_name: &str,
) {
    let destinations = get_matching_destinations(&ctx.ses_state, config_set_name, event_type);

    let mut dispatches: Vec<EventDestinationDispatch> = Vec::new();
    let message_id = event["mail"]["messageId"]
        .as_str()
        .unwrap_or("")
        .to_string();
    for dest in destinations {
        // SNS destination
        if let Some(ref sns_dest) = dest.sns_destination {
            if let Some(topic_arn) = sns_dest["TopicArn"].as_str() {
                let message = event.to_string();
                tracing::info!(
                    topic_arn = %topic_arn,
                    event_type = ?event_type,
                    "SES event fanout -> SNS"
                );
                ctx.delivery_bus.publish_to_sns(
                    topic_arn,
                    &message,
                    Some("Amazon SES Email Event"),
                );
                dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "sns".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn: topic_arn.to_string(),
                });
            }
        }

        // EventBridge destination
        if let Some(ref eb) = dest.event_bridge_destination {
            let detail = event.to_string();
            tracing::info!(
                event_type = ?event_type,
                "SES event fanout -> EventBridge"
            );
            ctx.delivery_bus.put_event_to_eventbridge(
                "aws.ses",
                "SES Email Sending",
                &detail,
                "default",
            );
            let target_arn = eb
                .get("EventBusArn")
                .and_then(|v| v.as_str())
                .unwrap_or("default")
                .to_string();
            dispatches.push(EventDestinationDispatch {
                destination_name: dest.name.clone(),
                destination_type: "eventbridge".to_string(),
                event_type: event_type.as_str().to_string(),
                message_id: message_id.clone(),
                dispatched_at: Utc::now(),
                target_arn,
            });
        }

        // Kinesis / Firehose destination
        if let Some(ref kf) = dest.kinesis_firehose_destination {
            let event_json = event.to_string();
            if let Some(ds_arn) = kf.get("DeliveryStreamARN").and_then(|v| v.as_str()) {
                tracing::info!(
                    delivery_stream_arn = %ds_arn,
                    event_type = ?event_type,
                    "SES event fanout -> Firehose"
                );
                ctx.delivery_bus
                    .put_record_to_firehose(ds_arn, event_json.as_bytes());
                dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "firehose".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn: ds_arn.to_string(),
                });
            }
            if let Some(stream_arn) = kf.get("StreamARN").and_then(|v| v.as_str()) {
                tracing::info!(
                    stream_arn = %stream_arn,
                    event_type = ?event_type,
                    "SES event fanout -> Kinesis"
                );
                let data_b64 = base64::engine::general_purpose::STANDARD.encode(&event_json);
                let partition_key = event["mail"]["messageId"].as_str().unwrap_or("default");
                ctx.delivery_bus
                    .put_record_to_kinesis(stream_arn, &data_b64, partition_key);
                dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "kinesis".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn: stream_arn.to_string(),
                });
            }
        }

        // CloudWatch destination
        if let Some(ref cw) = dest.cloud_watch_destination {
            if let Some(dims) = cw.get("DimensionConfigurations").and_then(|v| v.as_array()) {
                for dim_cfg in dims {
                    let dim_name = dim_cfg["DimensionName"].as_str().unwrap_or("EventType");
                    let dim_value = dim_cfg["DefaultDimensionValue"]
                        .as_str()
                        .unwrap_or(event_type.as_str());
                    let mut dimensions = std::collections::BTreeMap::new();
                    dimensions.insert(dim_name.to_string(), dim_value.to_string());
                    tracing::info!(
                        dimension = %dim_name,
                        event_type = ?event_type,
                        "SES event fanout -> CloudWatch"
                    );
                    ctx.delivery_bus.put_cloudwatch_metric(
                        &ctx.account_id,
                        &ctx.region,
                        "AWS/SES2",
                        event_type.as_str(),
                        1.0,
                        Some("Count"),
                        dimensions,
                        Utc::now().timestamp_millis(),
                    );
                }
                dispatches.push(EventDestinationDispatch {
                    destination_name: dest.name.clone(),
                    destination_type: "cloudwatch".to_string(),
                    event_type: event_type.as_str().to_string(),
                    message_id: message_id.clone(),
                    dispatched_at: Utc::now(),
                    target_arn: String::new(),
                });
            }
        }
    }
    if !dispatches.is_empty() {
        let mut mas = ctx.ses_state.write();
        let state = mas.default_mut();
        state.event_destination_dispatches.extend(dispatches);
    }
}

/// Infer ISP name from an email address domain.
pub fn infer_isp(email: &str) -> String {
    let domain = email.split('@').nth(1).unwrap_or("").to_ascii_lowercase();
    match domain.as_str() {
        "gmail.com" => "Gmail",
        "yahoo.com" | "ymail.com" => "Yahoo",
        "hotmail.com" | "outlook.com" | "live.com" => "Outlook",
        "aol.com" => "AOL",
        "icloud.com" | "me.com" | "mac.com" => "Apple",
        "protonmail.com" => "ProtonMail",
        "zoho.com" => "Zoho",
        _ => "Other",
    }
    .to_string()
}

/// Build per-recipient delivery insights from the generated event types.
fn build_delivery_insights(
    email: &SentEmail,
    event_types: &[SesEventType],
) -> Vec<EmailRecipientInsight> {
    let mut insights = Vec::new();
    for dest in &email.to {
        let mut events = Vec::new();
        for et in event_types {
            let mut ev = DeliveryInsightEvent {
                timestamp: Utc::now(),
                event_type: et.as_str().to_string(),
                ..Default::default()
            };
            match et {
                SesEventType::Bounce => {
                    ev.bounce_type = Some("Permanent".to_string());
                    ev.bounce_sub_type = Some("General".to_string());
                    ev.diagnostic_code = Some("smtp; 550 5.1.1 user unknown".to_string());
                }
                SesEventType::Complaint => {
                    ev.complaint_feedback_type = Some("abuse".to_string());
                }
                _ => {}
            }
            events.push(ev);
        }
        insights.push(EmailRecipientInsight {
            destination: dest.clone(),
            isp: infer_isp(dest),
            events,
        });
    }
    insights
}

/// Process event fanout for a sent email.
///
/// This is the main entry point called from SendEmail / SendBulkEmail.
/// It:
/// 1. Checks the suppression list (returns true if suppressed → caller should bounce)
/// 2. Classifies recipients for mailbox simulator behavior
/// 3. Generates appropriate events
/// 4. Fans out to configured destinations
/// 5. Stores per-recipient delivery insights on the SentEmail
///
/// Returns `true` if the email was suppressed (caller should handle accordingly).
pub fn process_send_events(
    ctx: &SesDeliveryContext,
    email: &mut SentEmail,
    config_set_name: Option<&str>,
) -> bool {
    let config_set = resolve_config_set(&ctx.ses_state, config_set_name, &email.from);

    // Check suppression list with the effective reasons filter when a config set exists
    if let Some(ref cs) = config_set {
        if let Some(suppressed_addr) =
            check_suppression_list(&ctx.ses_state, &email.to, Some(cs.as_str()))
        {
            tracing::info!(
                address = %suppressed_addr,
                config_set = %cs,
                "SES: recipient is on suppression list, generating bounce"
            );
            // Increment the suppression-drop counter so introspection /
            // /_fakecloud/ses/metrics callers can observe the gate firing.
            {
                let mut mas = ctx.ses_state.write();
                let state = mas.default_mut();
                state.suppressed_drops_total = state.suppressed_drops_total.saturating_add(1);
            }
            let bounce_event = build_ses_event(SesEventType::Bounce, email);
            deliver_event(ctx, &bounce_event, SesEventType::Bounce, cs);
            // Store insights: one BOUNCE event per recipient
            email.delivery_insights = build_delivery_insights(email, &[SesEventType::Bounce]);
            return true;
        }
    }

    // Classify recipients for simulator behavior
    let (event_types, add_to_suppression) = classify_recipients(&email.to);

    // Handle suppression list addition
    if add_to_suppression {
        let mut mas = ctx.ses_state.write();
        let state = mas.default_mut();
        for addr in &email.to {
            if addr == SUPPRESSION_ADDR {
                state.suppressed_destinations.insert(
                    addr.clone(),
                    SuppressedDestination {
                        email_address: addr.clone(),
                        reason: "BOUNCE".to_string(),
                        last_update_time: Utc::now(),
                    },
                );
            }
        }
    }

    // Generate and deliver events to configured destinations
    if let Some(ref cs) = config_set {
        for event_type in &event_types {
            let event = build_ses_event(*event_type, email);
            deliver_event(ctx, &event, *event_type, cs);
        }
    }

    // Store per-recipient delivery insights regardless of config set
    email.delivery_insights = build_delivery_insights(email, &event_types);

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_normal_recipients() {
        let (events, suppress) = classify_recipients(&["user@example.com".to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Delivery);
        assert!(!suppress);
    }

    #[test]
    fn classify_bounce_simulator() {
        let (events, suppress) = classify_recipients(&[BOUNCE_ADDR.to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Bounce);
        assert!(!suppress);
    }

    #[test]
    fn classify_complaint_simulator() {
        let (events, suppress) = classify_recipients(&[COMPLAINT_ADDR.to_string()]);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Delivery);
        assert_eq!(events[2], SesEventType::Complaint);
        assert!(!suppress);
    }

    #[test]
    fn classify_suppression_simulator() {
        let (events, suppress) = classify_recipients(&[SUPPRESSION_ADDR.to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Bounce);
        assert!(suppress);
    }

    #[test]
    fn classify_success_simulator() {
        let (events, suppress) = classify_recipients(&[SUCCESS_ADDR.to_string()]);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], SesEventType::Send);
        assert_eq!(events[1], SesEventType::Delivery);
        assert!(!suppress);
    }

    #[test]
    fn build_send_event_format() {
        let email = SentEmail {
            message_id: "test-msg-id".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["recipient@example.com".to_string()],
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
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let event = build_ses_event(SesEventType::Send, &email);
        assert_eq!(event["eventType"], "Send");
        assert_eq!(event["mail"]["messageId"], "test-msg-id");
        assert_eq!(event["mail"]["source"], "sender@example.com");
        assert!(event["send"].is_object());
    }

    #[test]
    fn build_bounce_event_format() {
        let email = SentEmail {
            message_id: "bounce-msg".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["bounce@simulator.amazonses.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: None,
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let event = build_ses_event(SesEventType::Bounce, &email);
        assert_eq!(event["eventType"], "Bounce");
        assert_eq!(event["bounce"]["bounceType"], "Permanent");
        assert!(event["bounce"]["bouncedRecipients"].is_array());
    }

    #[test]
    fn build_delivery_event_format() {
        let email = SentEmail {
            message_id: "deliver-msg".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["user@example.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: None,
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let event = build_ses_event(SesEventType::Delivery, &email);
        assert_eq!(event["eventType"], "Delivery");
        assert!(event["delivery"]["timestamp"].is_string());
        assert_eq!(event["delivery"]["smtpResponse"], "250 2.0.0 Ok");
    }

    #[test]
    fn build_complaint_event_format() {
        let email = SentEmail {
            message_id: "complaint-msg".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["complaint@simulator.amazonses.com".to_string()],
            cc: vec![],
            bcc: vec![],
            subject: None,
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let event = build_ses_event(SesEventType::Complaint, &email);
        assert_eq!(event["eventType"], "Complaint");
        assert_eq!(event["complaint"]["complaintFeedbackType"], "abuse");
    }

    #[test]
    fn classify_multiple_recipients_no_simulator() {
        let recipients = vec![
            "a@example.com".to_string(),
            "b@example.com".to_string(),
            "c@example.com".to_string(),
        ];
        let (events, suppress) = classify_recipients(&recipients);
        assert!(events.contains(&SesEventType::Send));
        assert!(events.contains(&SesEventType::Delivery));
        assert!(!suppress);
    }

    #[test]
    fn classify_empty_recipients() {
        let (events, suppress) = classify_recipients(&[]);
        assert!(!events.is_empty());
        assert!(!suppress);
    }

    fn shared_state() -> SharedSesState {
        use fakecloud_core::multi_account::MultiAccountState;
        Arc::new(parking_lot::RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )))
    }

    #[test]
    fn check_suppression_list_finds_suppressed() {
        let state = shared_state();
        state.write().default_mut().suppressed_destinations.insert(
            "blocked@example.com".to_string(),
            SuppressedDestination {
                email_address: "blocked@example.com".to_string(),
                reason: "BOUNCE".to_string(),
                last_update_time: Utc::now(),
            },
        );
        let hit = check_suppression_list(
            &state,
            &[
                "ok@example.com".to_string(),
                "blocked@example.com".to_string(),
            ],
            None,
        );
        assert_eq!(hit.as_deref(), Some("blocked@example.com"));
    }

    #[test]
    fn check_suppression_list_none_when_clean() {
        let state = shared_state();
        let hit = check_suppression_list(&state, &["ok@example.com".to_string()], None);
        assert!(hit.is_none());
    }

    #[test]
    fn check_suppression_list_skips_when_reason_filter_excludes() {
        // Address suppressed for COMPLAINT, but the resolved config set
        // only enforces BOUNCE — fanout must not bounce that recipient.
        let state = shared_state();
        {
            let mut mas = state.write();
            let st = mas.default_mut();
            st.suppressed_destinations.insert(
                "blocked@example.com".to_string(),
                SuppressedDestination {
                    email_address: "blocked@example.com".to_string(),
                    reason: "COMPLAINT".to_string(),
                    last_update_time: Utc::now(),
                },
            );
            st.configuration_sets.insert(
                "bounce-only".to_string(),
                crate::state::ConfigurationSet {
                    name: "bounce-only".to_string(),
                    sending_enabled: true,
                    tls_policy: "OPTIONAL".to_string(),
                    sending_pool_name: None,
                    custom_redirect_domain: None,
                    https_policy: None,
                    suppressed_reasons: vec!["BOUNCE".to_string()],
                    reputation_metrics_enabled: false,
                    vdm_options: None,
                    archive_arn: None,
                    archiving_options_present: false,
                },
            );
        }
        let hit = check_suppression_list(
            &state,
            &["blocked@example.com".to_string()],
            Some("bounce-only"),
        );
        assert!(hit.is_none());
    }

    #[test]
    fn check_suppression_list_account_fallback_when_config_set_empty() {
        // Config set has no suppressed_reasons; fall back to account
        // scope which only enforces COMPLAINT.
        let state = shared_state();
        {
            let mut mas = state.write();
            let st = mas.default_mut();
            st.suppressed_destinations.insert(
                "blocked@example.com".to_string(),
                SuppressedDestination {
                    email_address: "blocked@example.com".to_string(),
                    reason: "BOUNCE".to_string(),
                    last_update_time: Utc::now(),
                },
            );
            st.account_settings.suppressed_reasons = vec!["COMPLAINT".to_string()];
            st.configuration_sets.insert(
                "passthrough".to_string(),
                crate::state::ConfigurationSet {
                    name: "passthrough".to_string(),
                    sending_enabled: true,
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
        let hit = check_suppression_list(
            &state,
            &["blocked@example.com".to_string()],
            Some("passthrough"),
        );
        assert!(hit.is_none());
    }

    #[test]
    fn check_suppression_list_case_insensitive() {
        let state = shared_state();
        state.write().default_mut().suppressed_destinations.insert(
            "Blocked@Example.com".to_string(),
            SuppressedDestination {
                email_address: "Blocked@Example.com".to_string(),
                reason: "BOUNCE".to_string(),
                last_update_time: Utc::now(),
            },
        );
        let hit = check_suppression_list(&state, &["BLOCKED@example.COM".to_string()], None);
        assert_eq!(hit.as_deref(), Some("BLOCKED@example.COM"));
    }

    fn make_identity(name: &str, config_set: Option<&str>) -> crate::state::EmailIdentity {
        crate::state::EmailIdentity {
            identity_name: name.to_string(),
            identity_type: "EmailAddress".to_string(),
            verified: true,
            created_at: Utc::now(),
            dkim_signing_enabled: false,
            dkim_signing_attributes_origin: "AWS_SES".to_string(),
            dkim_domain_signing_private_key: None,
            dkim_domain_signing_selector: None,
            dkim_next_signing_key_length: None,
            dkim_public_key_b64: None,
            email_forwarding_enabled: true,
            mail_from_domain: None,
            mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
            mail_from_domain_status: "NotStarted".to_string(),
            configuration_set_name: config_set.map(|s| s.to_string()),
            bounce_topic: None,
            complaint_topic: None,
            delivery_topic: None,
        }
    }

    #[test]
    fn resolve_config_set_uses_explicit_arg_first() {
        let state = shared_state();
        let resolved = resolve_config_set(&state, Some("my-cs"), "sender@example.com");
        assert_eq!(resolved.as_deref(), Some("my-cs"));
    }

    #[test]
    fn resolve_config_set_uses_identity_default_when_no_explicit() {
        let state = shared_state();
        state.write().default_mut().identities.insert(
            "sender@example.com".to_string(),
            make_identity("sender@example.com", Some("identity-cs")),
        );
        let resolved = resolve_config_set(&state, None, "sender@example.com");
        assert_eq!(resolved.as_deref(), Some("identity-cs"));
    }

    #[test]
    fn resolve_config_set_falls_back_to_domain_identity() {
        let state = shared_state();
        state.write().default_mut().identities.insert(
            "example.com".to_string(),
            make_identity("example.com", Some("domain-cs")),
        );
        let resolved = resolve_config_set(&state, None, "sender@example.com");
        assert_eq!(resolved.as_deref(), Some("domain-cs"));
    }

    #[test]
    fn resolve_config_set_none_when_nothing_set() {
        let state = shared_state();
        assert!(resolve_config_set(&state, None, "sender@example.com").is_none());
    }

    #[test]
    fn get_matching_destinations_filters_by_enabled_and_event_type() {
        let state = shared_state();
        state.write().default_mut().event_destinations.insert(
            "cs".to_string(),
            vec![
                EventDestination {
                    name: "sns-dest".to_string(),
                    enabled: true,
                    matching_event_types: vec!["SEND".to_string(), "BOUNCE".to_string()],
                    kinesis_firehose_destination: None,
                    cloud_watch_destination: None,
                    sns_destination: Some(serde_json::json!({"TopicArn": "arn"})),
                    event_bridge_destination: None,
                    pinpoint_destination: None,
                },
                EventDestination {
                    name: "disabled".to_string(),
                    enabled: false,
                    matching_event_types: vec!["SEND".to_string()],
                    kinesis_firehose_destination: None,
                    cloud_watch_destination: None,
                    sns_destination: None,
                    event_bridge_destination: None,
                    pinpoint_destination: None,
                },
            ],
        );
        let dests = get_matching_destinations(&state, "cs", SesEventType::Send);
        assert_eq!(dests.len(), 1);
        assert_eq!(dests[0].name, "sns-dest");
        let none = get_matching_destinations(&state, "cs", SesEventType::Delivery);
        assert!(none.is_empty());
        let missing = get_matching_destinations(&state, "unknown", SesEventType::Send);
        assert!(missing.is_empty());
    }

    #[test]
    fn deliver_event_kinesis_firehose_cloudwatch_no_panic() {
        let state = shared_state();
        state.write().default_mut().event_destinations.insert(
            "cs".to_string(),
            vec![EventDestination {
                name: "multi-dest".to_string(),
                enabled: true,
                matching_event_types: vec!["SEND".to_string()],
                kinesis_firehose_destination: Some(serde_json::json!({
                    "DeliveryStreamARN": "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream",
                    "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/my-kinesis"
                })),
                cloud_watch_destination: Some(serde_json::json!({
                    "DimensionConfigurations": [
                        {
                            "DimensionName": "EventType",
                            "DimensionValueSource": "MESSAGE_TAG",
                            "DefaultDimensionValue": "Send"
                        }
                    ]
                })),
                sns_destination: None,
                event_bridge_destination: None,
                pinpoint_destination: None,
            }],
        );
        let ctx = SesDeliveryContext {
            ses_state: state,
            delivery_bus: Arc::new(DeliveryBus::new()),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
        };
        let event = build_ses_event(
            SesEventType::Send,
            &SentEmail {
                message_id: "msg-1".to_string(),
                from: "sender@example.com".to_string(),
                to: vec!["recipient@example.com".to_string()],
                cc: vec![],
                bcc: vec![],
                subject: None,
                html_body: None,
                text_body: None,
                raw_data: None,
                template_name: None,
                template_data: None,
                dkim_signature: None,
                headers: Vec::new(),
                timestamp: Utc::now(),
                email_tags: Vec::new(),
                delivery_insights: Vec::new(),
            },
        );
        // No senders wired — must not panic.
        deliver_event(&ctx, &event, SesEventType::Send, "cs");
    }
}
