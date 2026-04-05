use std::collections::HashMap;

use chrono::Utc;

use fakecloud_core::delivery::{SqsDelivery, SqsMessageAttribute};

use crate::state::{MessageAttribute, SharedSqsState, SqsMessage};

/// Implements SqsDelivery so other services can push messages into SQS queues.
pub struct SqsDeliveryImpl {
    state: SharedSqsState,
}

impl SqsDeliveryImpl {
    pub fn new(state: SharedSqsState) -> Self {
        Self { state }
    }
}

impl SqsDelivery for SqsDeliveryImpl {
    fn deliver_to_queue(
        &self,
        queue_arn: &str,
        message_body: &str,
        _attributes: &HashMap<String, String>,
    ) {
        self.deliver_to_queue_with_attrs(queue_arn, message_body, &HashMap::new(), None, None);
    }

    fn deliver_to_queue_with_attrs(
        &self,
        queue_arn: &str,
        message_body: &str,
        message_attributes: &HashMap<String, SqsMessageAttribute>,
        message_group_id: Option<&str>,
        message_dedup_id: Option<&str>,
    ) {
        let mut state = self.state.write();

        // Find queue by ARN
        let queue = state.queues.values_mut().find(|q| q.arn == queue_arn);

        if let Some(queue) = queue {
            // For FIFO queues without content-based dedup, require explicit dedup ID
            if queue.is_fifo && message_dedup_id.is_none() {
                let content_based = queue
                    .attributes
                    .get("ContentBasedDeduplication")
                    .map(|v| v.as_str())
                    == Some("true");
                if !content_based {
                    tracing::debug!(
                        queue_arn,
                        "skipping delivery: FIFO queue requires dedup ID or content-based dedup"
                    );
                    return;
                }
            }

            let now = Utc::now();

            // For FIFO queues with content-based dedup, generate dedup ID if not provided
            let effective_dedup_id = if message_dedup_id.is_some() {
                message_dedup_id.map(|s| s.to_string())
            } else if queue.is_fifo {
                // Content-based dedup: use SHA-256 of body (matches real SQS behavior)
                Some(crate::service::sha256_hex(message_body))
            } else {
                None
            };

            // Convert SqsMessageAttribute to the SQS state MessageAttribute
            let sqs_attrs: HashMap<String, MessageAttribute> = message_attributes
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        MessageAttribute {
                            data_type: v.data_type.clone(),
                            string_value: v.string_value.clone(),
                            binary_value: v.binary_value.as_ref().map(|s| s.as_bytes().to_vec()),
                        },
                    )
                })
                .collect();

            let msg = SqsMessage {
                message_id: uuid::Uuid::new_v4().to_string(),
                receipt_handle: None,
                md5_of_body: crate::service::md5_hex(message_body),
                body: message_body.to_string(),
                sent_timestamp: now.timestamp_millis(),
                attributes: HashMap::new(),
                message_attributes: sqs_attrs,
                visible_at: None,
                receive_count: 0,
                message_group_id: message_group_id.map(|s| s.to_string()),
                message_dedup_id: effective_dedup_id,
                created_at: now,
                sequence_number: None,
            };
            queue.messages.push_back(msg);
            tracing::debug!(queue_arn, "delivered message to SQS queue");
        } else {
            tracing::warn!(queue_arn, "SQS delivery target queue not found");
        }
    }
}
