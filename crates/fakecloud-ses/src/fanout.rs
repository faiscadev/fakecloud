//! SES event fanout: publishes send/delivery/bounce/complaint events
//! to configured event destinations (SNS topics, EventBridge buses).

use chrono::Utc;
use serde_json::json;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;

use crate::state::{EventDestination, SentEmail, SharedSesState, SuppressedDestination};

/// Shared references needed for cross-service event delivery.
#[derive(Clone)]
pub struct SesDeliveryContext {
    pub ses_state: SharedSesState,
    pub delivery_bus: Arc<DeliveryBus>,
}

/// Mailbox simulator addresses.
const BOUNCE_ADDR: &str = "bounce@simulator.amazonses.com";
const COMPLAINT_ADDR: &str = "complaint@simulator.amazonses.com";
#[cfg(test)]
const SUCCESS_ADDR: &str = "success@simulator.amazonses.com";
const SUPPRESSION_ADDR: &str = "suppressionlist@simulator.amazonses.com";

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
    } else {
        // Normal send or success@simulator
        events.push(SesEventType::Send);
        events.push(SesEventType::Delivery);
    }

    (events, suppress)
}

/// Check if any recipient is on the suppression list.
/// Returns the suppressed address if found.
pub fn check_suppression_list(ses_state: &SharedSesState, recipients: &[String]) -> Option<String> {
    let state = ses_state.read();
    for addr in recipients {
        if state.suppressed_destinations.contains_key(addr) {
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
    let state = ses_state.read();
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
    let state = ses_state.read();
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
            }
        }

        // EventBridge destination
        if dest.event_bridge_destination.is_some() {
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
        }
    }
}

/// Process event fanout for a sent email.
///
/// This is the main entry point called from SendEmail / SendBulkEmail.
/// It:
/// 1. Checks the suppression list (returns true if suppressed → caller should bounce)
/// 2. Classifies recipients for mailbox simulator behavior
/// 3. Generates appropriate events
/// 4. Fans out to configured destinations
///
/// Returns `true` if the email was suppressed (caller should handle accordingly).
pub fn process_send_events(
    ctx: &SesDeliveryContext,
    email: &SentEmail,
    config_set_name: Option<&str>,
) -> bool {
    let config_set = match resolve_config_set(&ctx.ses_state, config_set_name, &email.from) {
        Some(cs) => cs,
        None => return false, // No config set, no event destinations to fan out to
    };

    // Check suppression list
    if let Some(suppressed_addr) = check_suppression_list(&ctx.ses_state, &email.to) {
        tracing::info!(
            address = %suppressed_addr,
            "SES: recipient is on suppression list, generating bounce"
        );
        let bounce_event = build_ses_event(SesEventType::Bounce, email);
        deliver_event(ctx, &bounce_event, SesEventType::Bounce, &config_set);
        return true;
    }

    // Classify recipients for simulator behavior
    let (event_types, add_to_suppression) = classify_recipients(&email.to);

    // Handle suppression list addition
    if add_to_suppression {
        let mut state = ctx.ses_state.write();
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

    // Generate and deliver events
    for event_type in event_types {
        let event = build_ses_event(event_type, email);
        deliver_event(ctx, &event, event_type, &config_set);
    }

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
            timestamp: Utc::now(),
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
            timestamp: Utc::now(),
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
            timestamp: Utc::now(),
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
            timestamp: Utc::now(),
        };
        let event = build_ses_event(SesEventType::Complaint, &email);
        assert_eq!(event["eventType"], "Complaint");
        assert_eq!(event["complaint"]["complaintFeedbackType"], "abuse");
    }
}
