use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;

use fakecloud_core::delivery::{DeliveryBus, SnsDelivery};

use crate::state::{PublishedMessage, SharedSnsState};

/// Implements SnsDelivery so other services (EventBridge) can publish to SNS topics.
pub struct SnsDeliveryImpl {
    state: SharedSnsState,
    delivery: Arc<DeliveryBus>,
}

impl SnsDeliveryImpl {
    pub fn new(state: SharedSnsState, delivery: Arc<DeliveryBus>) -> Self {
        Self { state, delivery }
    }
}

impl SnsDelivery for SnsDeliveryImpl {
    fn publish_to_topic(&self, topic_arn: &str, message: &str, subject: Option<&str>) {
        let mut state = self.state.write();

        if !state.topics.contains_key(topic_arn) {
            tracing::warn!(topic_arn, "SNS delivery target topic not found");
            return;
        }

        let msg_id = uuid::Uuid::new_v4().to_string();
        state.published.push(PublishedMessage {
            message_id: msg_id.clone(),
            topic_arn: topic_arn.to_string(),
            message: message.to_string(),
            subject: subject.map(|s| s.to_string()),
            message_attributes: HashMap::new(),
            message_group_id: None,
            message_dedup_id: None,
            timestamp: Utc::now(),
        });

        // Fan out to SQS subscribers
        let sqs_subscribers: Vec<String> = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && s.protocol == "sqs" && s.confirmed)
            .map(|s| s.endpoint.clone())
            .collect();

        // Collect Lambda, email, and SMS subscribers
        let lambda_subscribers: Vec<(String, String)> = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && s.protocol == "lambda" && s.confirmed)
            .map(|s| (s.endpoint.clone(), s.subscription_arn.clone()))
            .collect();

        let email_subscribers: Vec<String> = state
            .subscriptions
            .values()
            .filter(|s| {
                s.topic_arn == topic_arn
                    && (s.protocol == "email" || s.protocol == "email-json")
                    && s.confirmed
            })
            .map(|s| s.endpoint.clone())
            .collect();

        let sms_subscribers: Vec<String> = state
            .subscriptions
            .values()
            .filter(|s| s.topic_arn == topic_arn && s.protocol == "sms" && s.confirmed)
            .map(|s| s.endpoint.clone())
            .collect();

        let endpoint = state.endpoint.clone();

        // Build SNS Lambda event payload (matches real AWS format)
        let now = Utc::now();
        let empty_attrs = serde_json::Map::new();
        let lambda_payloads: Vec<(String, String)> = lambda_subscribers
            .iter()
            .map(|(function_arn, subscription_arn)| {
                let payload = crate::service::build_sns_lambda_event(
                    &msg_id,
                    topic_arn,
                    subscription_arn,
                    message,
                    subject,
                    &empty_attrs,
                    &now,
                    &endpoint,
                );
                (function_arn.clone(), payload)
            })
            .collect();

        // Record invocations in state
        for (function_arn, _) in &lambda_payloads {
            state
                .lambda_invocations
                .push(crate::state::LambdaInvocation {
                    function_arn: function_arn.clone(),
                    message: message.to_string(),
                    subject: subject.map(|s| s.to_string()),
                    timestamp: now,
                });
        }

        // Store email deliveries
        for email_address in &email_subscribers {
            tracing::info!(
                email = %email_address,
                topic_arn = %topic_arn,
                "SNS cross-service delivering to email (stub)"
            );
            state.sent_emails.push(crate::state::SentEmail {
                email_address: email_address.clone(),
                message: message.to_string(),
                subject: subject.map(|s| s.to_string()),
                topic_arn: topic_arn.to_string(),
                timestamp: now,
            });
        }

        // Store SMS deliveries
        for phone_number in &sms_subscribers {
            tracing::info!(
                phone_number = %phone_number,
                topic_arn = %topic_arn,
                "SNS cross-service delivering to SMS (stub)"
            );
            state
                .sms_messages
                .push((phone_number.clone(), message.to_string()));
        }

        // Drop the lock before calling into SQS delivery
        drop(state);

        // Wrap the message in SNS notification envelope (matches real AWS format)
        let sns_envelope = serde_json::json!({
            "Type": "Notification",
            "MessageId": msg_id,
            "TopicArn": topic_arn,
            "Subject": subject.unwrap_or(""),
            "Message": message,
            "Timestamp": Utc::now().to_rfc3339(),
            "SignatureVersion": "1",
            "Signature": "FAKE_SIGNATURE",
            "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-0000000000000000000000.pem",
            "UnsubscribeURL": format!("{}/?Action=Unsubscribe&SubscriptionArn={}", endpoint, topic_arn),
        });
        let envelope_str = sns_envelope.to_string();

        for queue_arn in sqs_subscribers {
            self.delivery
                .send_to_sqs(&queue_arn, &envelope_str, &HashMap::new());
        }

        // Invoke Lambda subscribers via container runtime
        if !lambda_payloads.is_empty() {
            let delivery = self.delivery.clone();
            tokio::spawn(async move {
                for (function_arn, payload) in lambda_payloads {
                    tracing::info!(
                        function_arn = %function_arn,
                        "SNS invoking Lambda function"
                    );
                    match delivery.invoke_lambda(&function_arn, &payload).await {
                        Some(Ok(_)) => {
                            tracing::info!(
                                function_arn = %function_arn,
                                "SNS->Lambda invocation succeeded"
                            );
                        }
                        Some(Err(e)) => {
                            tracing::error!(
                                function_arn = %function_arn,
                                error = %e,
                                "SNS->Lambda invocation failed"
                            );
                        }
                        None => {
                            tracing::info!(
                                function_arn = %function_arn,
                                "SNS->Lambda: no container runtime available, skipping real execution"
                            );
                        }
                    }
                }
            });
        }
    }
}
