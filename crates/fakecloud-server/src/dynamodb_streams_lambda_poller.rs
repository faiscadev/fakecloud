//! Background poller that bridges DynamoDB Streams -> Lambda event source mappings.
//!
//! Periodically checks Lambda state for enabled event source mappings
//! pointing to DynamoDB streams, reads stream records, and invokes Lambda
//! functions with batches of records.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use fakecloud_core::delivery::LambdaDelivery;
use fakecloud_dynamodb::state::SharedDynamoDbState;
use fakecloud_lambda::state::{LambdaInvocation, SharedLambdaState};

/// DynamoDB Streams -> Lambda event source mapping poller.
pub struct DynamoDbStreamsLambdaPoller {
    dynamodb_state: SharedDynamoDbState,
    lambda_state: SharedLambdaState,
    lambda_delivery: Option<Arc<dyn LambdaDelivery>>,
    /// Track the last processed sequence number per mapping
    checkpoints: parking_lot::RwLock<HashMap<String, String>>,
}

impl DynamoDbStreamsLambdaPoller {
    pub fn new(dynamodb_state: SharedDynamoDbState, lambda_state: SharedLambdaState) -> Self {
        Self {
            dynamodb_state,
            lambda_state,
            lambda_delivery: None,
            checkpoints: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    pub fn with_lambda_delivery(mut self, delivery: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_delivery = Some(delivery);
        self
    }

    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            self.poll().await;
        }
    }

    async fn poll(&self) {
        // Collect enabled mappings that point to DynamoDB streams
        let mappings: Vec<(String, String, String, i64)> = {
            let lambda = self.lambda_state.read();
            lambda
                .event_source_mappings
                .values()
                .filter(|m| {
                    m.enabled
                        && m.event_source_arn.contains(":dynamodb:")
                        && m.event_source_arn.contains("/stream/")
                })
                .map(|m| {
                    (
                        m.uuid.clone(),
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

        for (mapping_id, stream_arn, function_arn, batch_size) in mappings {
            // Extract table name from stream ARN
            // Format: arn:aws:dynamodb:region:account:table/TableName/stream/timestamp
            let table_name = if let Some(table_part) = stream_arn.split("/table/").nth(1) {
                table_part.split('/').next().unwrap_or("")
            } else {
                continue;
            };

            // Get checkpoint for this mapping
            let checkpoint = self.checkpoints.read().get(&mapping_id).cloned();

            // Read stream records from DynamoDB
            let records = {
                let dynamodb = self.dynamodb_state.read();
                let table = match dynamodb.tables.get(table_name) {
                    Some(t) => t,
                    None => continue,
                };

                if !table.stream_enabled {
                    continue;
                }

                let stream_records = table.stream_records.read();

                // Filter records after checkpoint
                let mut filtered: Vec<_> = stream_records
                    .iter()
                    .filter(|r| {
                        if let Some(ref cp) = checkpoint {
                            &r.dynamodb.sequence_number > cp
                        } else {
                            true
                        }
                    })
                    .take(batch_size as usize)
                    .cloned()
                    .collect();

                // Sort by sequence number to ensure order
                filtered
                    .sort_by(|a, b| a.dynamodb.sequence_number.cmp(&b.dynamodb.sequence_number));

                filtered
            };

            if records.is_empty() {
                continue;
            }

            // Build Lambda event payload
            let event = json!({
                "Records": records.iter().map(|record| {
                    let mut event_record = json!({
                        "eventID": record.event_id,
                        "eventName": record.event_name,
                        "eventVersion": record.event_version,
                        "eventSource": record.event_source,
                        "awsRegion": record.aws_region,
                        "dynamodb": {
                            "Keys": record.dynamodb.keys,
                            "SequenceNumber": record.dynamodb.sequence_number,
                            "SizeBytes": record.dynamodb.size_bytes,
                            "StreamViewType": record.dynamodb.stream_view_type,
                        },
                        "eventSourceARN": record.event_source_arn,
                    });

                    if let Some(ref new_img) = record.dynamodb.new_image {
                        event_record["dynamodb"]["NewImage"] = json!(new_img);
                    }
                    if let Some(ref old_img) = record.dynamodb.old_image {
                        event_record["dynamodb"]["OldImage"] = json!(old_img);
                    }

                    event_record
                }).collect::<Vec<_>>()
            });

            let payload = serde_json::to_string(&event).unwrap_or_default();

            // Invoke Lambda
            if let Some(ref delivery) = self.lambda_delivery {
                match delivery.invoke_lambda(&function_arn, &payload).await {
                    Ok(_) => {
                        tracing::info!(
                            function_arn = %function_arn,
                            record_count = records.len(),
                            "DynamoDB Streams->Lambda invocation succeeded"
                        );

                        // Update checkpoint to last processed sequence number
                        if let Some(last_record) = records.last() {
                            self.checkpoints.write().insert(
                                mapping_id.clone(),
                                last_record.dynamodb.sequence_number.clone(),
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            function_arn = %function_arn,
                            error = %e,
                            "DynamoDB Streams->Lambda invocation failed"
                        );
                    }
                }
            } else {
                // No delivery mechanism, just record
                let mut lambda = self.lambda_state.write();
                lambda.invocations.push(LambdaInvocation {
                    function_arn: function_arn.clone(),
                    payload: payload.clone(),
                    timestamp: Utc::now(),
                    source: "dynamodb:streams".to_string(),
                });

                // Update checkpoint
                if let Some(last_record) = records.last() {
                    self.checkpoints
                        .write()
                        .insert(mapping_id, last_record.dynamodb.sequence_number.clone());
                }
            }
        }
    }
}
