//! Background poller that bridges SQS -> Lambda event source mappings.
//!
//! Periodically checks Lambda state for enabled event source mappings
//! pointing to SQS queues, polls those queues for messages, and either
//! invokes the Lambda function via the container runtime (when available)
//! or records the invocation in Lambda state.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use fakecloud_core::delivery::LambdaDelivery;
use fakecloud_lambda::state::{LambdaInvocation, SharedLambdaState};
use fakecloud_sqs::state::SharedSqsState;

/// SQS -> Lambda event source mapping poller.
pub struct SqsLambdaPoller {
    sqs_state: SharedSqsState,
    lambda_state: SharedLambdaState,
    lambda_delivery: Option<Arc<dyn LambdaDelivery>>,
}

impl SqsLambdaPoller {
    pub fn new(sqs_state: SharedSqsState, lambda_state: SharedLambdaState) -> Self {
        Self {
            sqs_state,
            lambda_state,
            lambda_delivery: None,
        }
    }

    pub fn with_lambda_delivery(mut self, delivery: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_delivery = Some(delivery);
        self
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            self.poll().await;
        }
    }

    async fn poll(&self) {
        // Collect enabled mappings that point to SQS sources
        let mappings: Vec<(String, String, i64)> = {
            let lambda = self.lambda_state.read();
            lambda
                .event_source_mappings
                .values()
                .filter(|m| m.enabled && m.event_source_arn.contains(":sqs:"))
                .map(|m| {
                    (
                        m.event_source_arn.clone(),
                        m.function_arn.clone(),
                        m.batch_size,
                    )
                })
                .collect()
        };

        if mappings.is_empty() {
            return;
        }

        let now = Utc::now();

        for (queue_arn, function_arn, batch_size) in mappings {
            let messages = {
                let mut sqs = self.sqs_state.write();
                let queue = sqs.queues.values_mut().find(|q| q.arn == queue_arn);
                let queue = match queue {
                    Some(q) => q,
                    None => continue,
                };

                // Pull up to batch_size visible messages
                let mut batch = Vec::new();
                let limit = batch_size.min(10) as usize;

                for msg in queue.messages.iter() {
                    if batch.len() >= limit {
                        break;
                    }
                    if let Some(vis) = msg.visible_at {
                        if vis > now {
                            continue;
                        }
                    }
                    batch.push(msg.clone());
                }

                // Remove consumed messages from the queue
                let consumed_ids: Vec<String> =
                    batch.iter().map(|m| m.message_id.clone()).collect();
                queue
                    .messages
                    .retain(|m| !consumed_ids.contains(&m.message_id));

                batch
            };

            if messages.is_empty() {
                continue;
            }

            // Build the Lambda event payload matching AWS SQS event format
            let records: Vec<serde_json::Value> = messages
                .iter()
                .map(|msg| {
                    json!({
                        "messageId": msg.message_id,
                        "receiptHandle": msg.receipt_handle,
                        "body": msg.body,
                        "attributes": {
                            "ApproximateReceiveCount": msg.receive_count.to_string(),
                            "SentTimestamp": msg.sent_timestamp.to_string(),
                        },
                        "md5OfBody": msg.md5_of_body,
                        "eventSource": "aws:sqs",
                        "eventSourceARN": queue_arn,
                    })
                })
                .collect();

            let payload = json!({ "Records": records }).to_string();

            tracing::debug!(
                function_arn = %function_arn,
                queue_arn = %queue_arn,
                message_count = messages.len(),
                "SQS->Lambda: delivering messages to function"
            );

            // If we have a real Lambda delivery mechanism, invoke the function
            if let Some(ref delivery) = self.lambda_delivery {
                match delivery.invoke_lambda(&function_arn, &payload).await {
                    Ok(_) => {
                        tracing::debug!(
                            function_arn = %function_arn,
                            "SQS->Lambda: function invoked successfully"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(
                            function_arn = %function_arn,
                            error = %e,
                            "SQS->Lambda: function invocation failed"
                        );
                    }
                }
            }

            // Record the invocation in Lambda state (for observability / testing)
            let mut lambda = self.lambda_state.write();
            lambda.invocations.push(LambdaInvocation {
                function_arn,
                payload,
                timestamp: now,
                source: "aws:sqs".to_string(),
            });
        }
    }
}
