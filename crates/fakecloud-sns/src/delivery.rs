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
            "UnsubscribeURL": format!("http://localhost:4566/?Action=Unsubscribe&SubscriptionArn={}", topic_arn),
        });
        let envelope_str = sns_envelope.to_string();

        for queue_arn in sqs_subscribers {
            self.delivery
                .send_to_sqs(&queue_arn, &envelope_str, &HashMap::new());
        }
    }
}
