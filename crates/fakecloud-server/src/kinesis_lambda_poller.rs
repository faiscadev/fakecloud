//! Kinesis -> Lambda event source mapping poller.
//!
//! Honors:
//! - `FilterCriteria` — non-matching records are dropped (advanced past).
//! - `StartingPosition` — `TRIM_HORIZON` (default), `LATEST`, or
//!   `AT_TIMESTAMP` paired with `StartingPositionTimestamp` to seed
//!   the per-shard checkpoint on first poll.

use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::delivery::LambdaDelivery;
use fakecloud_kinesis::SharedKinesisState;
use fakecloud_lambda::filter::FilterSet;
use fakecloud_lambda::{LambdaInvocation, SharedLambdaState};
use fakecloud_persistence::SnapshotHook;

#[derive(Clone)]
struct Mapping {
    uuid: String,
    function_arn: String,
    stream_arn: String,
    batch_size: i64,
    filter: FilterSet,
    starting_position: Option<String>,
    starting_position_timestamp: Option<f64>,
    /// True when the function opted into partial-batch failure handling
    /// via `FunctionResponseTypes: ["ReportBatchItemFailures"]`. The
    /// checkpoint advances only past the first failed sequence number;
    /// records at or after that point are retried on the next poll.
    report_batch_item_failures: bool,
}

pub struct KinesisLambdaPoller {
    kinesis_state: SharedKinesisState,
    lambda_state: SharedLambdaState,
    lambda_delivery: Option<Arc<dyn LambdaDelivery>>,
    /// Persists Kinesis state after the poller advances a lambda checkpoint.
    /// Without it a checkpoint advance only reached disk on the next unrelated
    /// Kinesis API write, so a restart in the window re-delivered records the
    /// mapping had already consumed (bug-audit 4.5).
    snapshot_hook: Option<SnapshotHook>,
}

impl KinesisLambdaPoller {
    pub fn new(kinesis_state: SharedKinesisState, lambda_state: SharedLambdaState) -> Self {
        Self {
            kinesis_state,
            lambda_state,
            lambda_delivery: None,
            snapshot_hook: None,
        }
    }

    pub fn with_lambda_delivery(mut self, delivery: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_delivery = Some(delivery);
        self
    }

    pub fn with_snapshot_hook(mut self, hook: SnapshotHook) -> Self {
        self.snapshot_hook = Some(hook);
        self
    }

    /// Persist Kinesis state through the same snapshot path a mutating Kinesis
    /// API call uses. Called only after the write guard is released so the
    /// blocking snapshot IO never runs under the lock.
    async fn persist(&self) {
        if let Some(hook) = &self.snapshot_hook {
            hook().await;
        }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            self.poll().await;
        }
    }

    async fn poll(&self) {
        let mappings: Vec<Mapping> = {
            let lambda_accounts = self.lambda_state.read();
            lambda_accounts
                .iter()
                .flat_map(|(_, lambda)| {
                    lambda
                        .event_source_mappings
                        .values()
                        .filter(|m| m.enabled && m.event_source_arn.contains(":kinesis:"))
                        .map(|m| Mapping {
                            uuid: m.uuid.clone(),
                            function_arn: m.function_arn.clone(),
                            stream_arn: m.event_source_arn.clone(),
                            batch_size: m.batch_size,
                            filter: FilterSet::from_strings(m.filter_patterns.iter()),
                            starting_position: m.starting_position.clone(),
                            starting_position_timestamp: m.starting_position_timestamp,
                            report_batch_item_failures: m
                                .function_response_types
                                .iter()
                                .any(|t| t.eq_ignore_ascii_case("ReportBatchItemFailures")),
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        };

        if mappings.is_empty() {
            return;
        }

        for mapping in mappings {
            self.process_mapping(&mapping).await;
        }
    }

    async fn process_mapping(&self, mapping: &Mapping) {
        // Compute per-shard deliveries: snapshot current shard
        // contents, seed missing checkpoints based on StartingPosition,
        // then collect a batch from each shard up to batch_size.
        let deliveries = {
            let mut kinesis_accounts = self.kinesis_state.write();
            let account_id = mapping.stream_arn.split(':').nth(4).unwrap_or("");
            let kinesis = match kinesis_accounts.get_mut(account_id) {
                Some(k) => k,
                None => return,
            };
            let stream_idx = kinesis
                .streams
                .iter()
                .find(|(_, s)| s.stream_arn == mapping.stream_arn)
                .map(|(name, _)| name.clone());
            let Some(stream_name) = stream_idx else {
                return;
            };

            // Initialize per-shard checkpoints once based on
            // StartingPosition. Subsequent polls just read what's already
            // there.
            let init_pairs: Vec<(String, usize)> = {
                let stream = kinesis
                    .streams
                    .get(&stream_name)
                    .expect("stream exists, just looked up");
                stream
                    .shards
                    .iter()
                    .filter_map(|shard| {
                        let key = format!("{}:{}", mapping.uuid, shard.shard_id);
                        if kinesis.lambda_checkpoints.contains_key(&key) {
                            return None;
                        }
                        let init = match mapping
                            .starting_position
                            .as_deref()
                            .unwrap_or("TRIM_HORIZON")
                        {
                            "LATEST" => shard.records.len(),
                            "AT_TIMESTAMP" => {
                                let target = mapping
                                    .starting_position_timestamp
                                    .map(|t| t as i64)
                                    .unwrap_or(0);
                                shard
                                    .records
                                    .iter()
                                    .position(|r| {
                                        r.approximate_arrival_timestamp.timestamp() >= target
                                    })
                                    .unwrap_or(shard.records.len())
                            }
                            _ => 0, // TRIM_HORIZON
                        };
                        Some((shard.shard_id.clone(), init))
                    })
                    .collect()
            };
            for (shard_id, init) in init_pairs {
                kinesis.set_lambda_checkpoint(&mapping.uuid, &shard_id, init);
            }

            let stream = kinesis
                .streams
                .get(&stream_name)
                .expect("stream exists, just looked up");
            let limit = mapping.batch_size.max(1) as usize;
            stream
                .shards
                .iter()
                .filter_map(|shard| {
                    let start = kinesis.lambda_checkpoint(&mapping.uuid, &shard.shard_id);
                    if start >= shard.records.len() {
                        return None;
                    }
                    let end = shard.records.len().min(start.saturating_add(limit));
                    let records = shard.records[start..end].to_vec();
                    Some((shard.shard_id.clone(), start, end, records))
                })
                .collect::<Vec<_>>()
        };

        for (shard_id, start, end, records) in deliveries {
            // Build per-record JSON, then split into matched + dropped
            // by FilterCriteria. Dropped records still advance the
            // checkpoint — AWS docs say filtered-out records "do not
            // count toward batch size and are discarded".
            let record_jsons: Vec<Value> = records
                .iter()
                .map(|record| {
                    json!({
                        "awsRegion": "us-east-1",
                        "eventID": format!("{}:{}", shard_id, record.sequence_number),
                        "eventName": "aws:kinesis:record",
                        "eventSource": "aws:kinesis",
                        "eventSourceARN": mapping.stream_arn,
                        "eventVersion": "1.0",
                        "invokeIdentityArn": "arn:aws:iam::123456789012:role/lambda-role",
                        "kinesis": {
                            "approximateArrivalTimestamp": record.approximate_arrival_timestamp.timestamp_millis() as f64 / 1000.0,
                            "data": base64::engine::general_purpose::STANDARD.encode(&record.data),
                            "kinesisSchemaVersion": "1.0",
                            "partitionKey": record.partition_key,
                            "sequenceNumber": record.sequence_number,
                        }
                    })
                })
                .collect();

            let matched: Vec<Value> = if mapping.filter.is_empty() {
                record_jsons
            } else {
                record_jsons
                    .into_iter()
                    .filter(|r| mapping.filter.matches(r))
                    .collect()
            };

            // If the filter dropped every record, advance the
            // checkpoint past them — AWS treats filtered-out records
            // as consumed and never retries them.
            if matched.is_empty() {
                {
                    let account_id = mapping.stream_arn.split(':').nth(4).unwrap_or("");
                    let mut kinesis_accounts = self.kinesis_state.write();
                    let kinesis = kinesis_accounts.get_or_create(account_id);
                    kinesis.set_lambda_checkpoint(&mapping.uuid, &shard_id, end);
                }
                self.persist().await;
                continue;
            }

            let payload = json!({ "Records": matched }).to_string();

            let used_real_delivery = self.lambda_delivery.is_some();
            // Sequence numbers of the source records in batch order —
            // used below to compute the partial-batch checkpoint when
            // the function opted into ReportBatchItemFailures. We use
            // `records` (not `matched`) so a failure at a given seqno
            // also retries the filter-dropped records before it on the
            // next poll, which is what AWS does — filtered records get
            // re-evaluated and dropped again, but the failure point
            // anchors to the actual stream offset.
            let record_seqs: Vec<String> =
                records.iter().map(|r| r.sequence_number.clone()).collect();

            let invoke_result: Option<Result<Vec<u8>, String>> =
                if let Some(ref delivery) = self.lambda_delivery {
                    Some(
                        delivery
                            .invoke_lambda(&mapping.function_arn, &payload)
                            .await,
                    )
                } else {
                    None
                };

            let advance_to: Option<usize> = match &invoke_result {
                Some(Ok(body)) if mapping.report_batch_item_failures => {
                    match first_failed_index(body, &record_seqs) {
                        Some(idx) => Some(start.saturating_add(idx)),
                        None => Some(end),
                    }
                }
                Some(Ok(_)) => Some(end),
                Some(Err(error)) => {
                    tracing::warn!(
                        function_arn = %mapping.function_arn,
                        stream_arn = %mapping.stream_arn,
                        shard_id = %shard_id,
                        error = %error,
                        "Kinesis->Lambda: function invocation failed; batch will be retried"
                    );
                    None
                }
                None => Some(end),
            };

            // Only advance the checkpoint after a successful invoke.
            // A failed invoke leaves the records pending so the next
            // poll retries them — matches AWS's at-least-once guarantee.
            let Some(new_checkpoint) = advance_to else {
                continue;
            };

            {
                let account_id = mapping.stream_arn.split(':').nth(4).unwrap_or("");
                let mut kinesis_accounts = self.kinesis_state.write();
                let kinesis = kinesis_accounts.get_or_create(account_id);
                kinesis.set_lambda_checkpoint(&mapping.uuid, &shard_id, new_checkpoint);
            }
            // Persist the advanced checkpoint so a restart resumes past the
            // records this batch already delivered.
            self.persist().await;

            if !used_real_delivery {
                let fn_account = mapping.function_arn.split(':').nth(4).unwrap_or("");
                let mut lambda_accounts = self.lambda_state.write();
                let lambda = lambda_accounts.get_or_create(fn_account);
                lambda.invocations.push(LambdaInvocation {
                    function_arn: mapping.function_arn.clone(),
                    payload,
                    timestamp: Utc::now(),
                    source: "aws:kinesis".to_string(),
                });
            }
        }
    }
}

/// Parse the Lambda response body as `{"batchItemFailures":[{"itemIdentifier":"<seqno>"}]}`
/// and return the index in `batch_seqs` of the first failed sequence
/// number. Returns `None` when the body doesn't decode, the failures
/// list is empty, or no failure references a sequence number actually
/// in the batch (AWS ignores stale identifiers). Kinesis-specific:
/// callers advance the shard checkpoint to this index, so the failed
/// record and everything after it gets retried on the next poll.
fn first_failed_index(body: &[u8], batch_seqs: &[String]) -> Option<usize> {
    let parsed: Value = serde_json::from_slice(body).ok()?;
    let failures = parsed.get("batchItemFailures")?.as_array()?;
    let failed_seqs: Vec<&str> = failures
        .iter()
        .filter_map(|f| f.get("itemIdentifier").and_then(|v| v.as_str()))
        .collect();
    if failed_seqs.is_empty() {
        return None;
    }
    batch_seqs
        .iter()
        .enumerate()
        .filter(|(_, seq)| failed_seqs.contains(&seq.as_str()))
        .map(|(idx, _)| idx)
        .min()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_failed_index_finds_lowest_in_batch_order() {
        let body = br#"{"batchItemFailures":[{"itemIdentifier":"3"},{"itemIdentifier":"1"}]}"#;
        let seqs = ["0", "1", "2", "3", "4"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        // Lowest index in batch order is 1 (seq "1").
        assert_eq!(first_failed_index(body, &seqs), Some(1));
    }

    #[test]
    fn first_failed_index_ignores_stale_identifiers() {
        let body = br#"{"batchItemFailures":[{"itemIdentifier":"99"}]}"#;
        let seqs = ["0", "1", "2"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        assert!(first_failed_index(body, &seqs).is_none());
    }

    #[test]
    fn first_failed_index_empty_failures_returns_none() {
        let body = br#"{"batchItemFailures":[]}"#;
        let seqs = ["a", "b"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(first_failed_index(body, &seqs).is_none());
    }

    #[test]
    fn first_failed_index_invalid_json_returns_none() {
        let body = b"not json";
        let seqs = ["a"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(first_failed_index(body, &seqs).is_none());
    }
}
