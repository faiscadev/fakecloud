use std::sync::Arc;

use base64::Engine;
use chrono::Utc;
use fakecloud_core::delivery::KinesisDelivery;

use crate::state::{KinesisRecord, SharedKinesisState};

/// Kinesis delivery implementation for cross-service integrations.
pub struct KinesisDeliveryImpl {
    state: SharedKinesisState,
}

impl KinesisDeliveryImpl {
    pub fn new(state: SharedKinesisState) -> Arc<Self> {
        Arc::new(Self { state })
    }
}

impl KinesisDelivery for KinesisDeliveryImpl {
    fn put_record(&self, stream_arn: &str, data: &str, partition_key: &str) {
        // Extract stream name from ARN: arn:aws:kinesis:region:account:stream/StreamName
        let stream_name = if let Some(name_part) = stream_arn.rsplit('/').next() {
            // Handles both arn:aws:kinesis:region:account:stream/Name and plain name
            name_part
        } else {
            stream_arn
        };

        let mut state = self.state.write();
        if let Some(stream) = state.streams.get_mut(stream_name) {
            // Find the shard to write to based on partition key
            // For simplicity, hash the partition key and mod by shard count
            let shard_idx = if stream.shards.is_empty() {
                0
            } else {
                partition_key
                    .bytes()
                    .fold(0u64, |acc, b| acc.wrapping_add(b as u64))
                    % stream.shards.len() as u64
            };

            if let Some(shard) = stream.shards.get_mut(shard_idx as usize) {
                let now = Utc::now();
                let sequence_number = now.timestamp_nanos_opt().unwrap_or(0).to_string();

                // Data is base64-encoded; decode to raw bytes for storage.
                // GetRecords will base64-encode when returning, matching AWS behavior.
                let data_bytes = base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .unwrap_or_else(|_| data.as_bytes().to_vec());

                shard.records.push(KinesisRecord {
                    sequence_number: sequence_number.clone(),
                    partition_key: partition_key.to_string(),
                    data: data_bytes,
                    approximate_arrival_timestamp: now,
                });

                tracing::debug!(
                    stream_name = %stream_name,
                    partition_key = %partition_key,
                    sequence_number = %sequence_number,
                    "Delivered record to Kinesis stream"
                );
            }
        } else {
            tracing::warn!(
                stream_arn = %stream_arn,
                stream_name = %stream_name,
                "Stream not found for Kinesis delivery"
            );
        }
    }
}
