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
    /// The function's execution role ARN, reported as each record's
    /// `invokeIdentityArn` (the identity Lambda polls the stream with).
    /// `None` when the mapped function no longer exists.
    invoke_identity_arn: Option<String>,
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
        let mappings = self.collect_mappings();

        if mappings.is_empty() {
            return;
        }

        for mapping in mappings {
            self.process_mapping(&mapping).await;
        }
    }

    /// Snapshot every enabled Kinesis-source event source mapping.
    fn collect_mappings(&self) -> Vec<Mapping> {
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
                        invoke_identity_arn: m
                            .function_arn
                            .split(':')
                            .nth(6)
                            .and_then(|name| lambda.functions.get(name))
                            .map(|f| f.role.clone()),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    async fn process_mapping(&self, mapping: &Mapping) {
        // Compute per-shard deliveries: snapshot current shard
        // contents, seed missing checkpoints based on StartingPosition,
        // then collect a batch from each shard up to batch_size.
        let deliveries = {
            let mut kinesis_accounts = self.kinesis_state.write();
            // The stream lives in the account and region its ARN names.
            let kinesis = match kinesis_accounts.by_arn_mut(&mapping.stream_arn) {
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
                    kinesis_event_record(
                        record,
                        &shard_id,
                        &mapping.stream_arn,
                        mapping.invoke_identity_arn.as_deref(),
                    )
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
                if let Some(kinesis) = self.kinesis_state.write().by_arn_mut(&mapping.stream_arn) {
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

            if let Some(kinesis) = self.kinesis_state.write().by_arn_mut(&mapping.stream_arn) {
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

/// Build the Lambda event record for one Kinesis record. `awsRegion` is the
/// stream's region, taken from its ARN (the region the records were read
/// from), not a fixed default. `invokeIdentityArn` is the function's
/// execution role, the identity the mapping reads the stream with.
fn kinesis_event_record(
    record: &fakecloud_kinesis::KinesisRecord,
    shard_id: &str,
    stream_arn: &str,
    invoke_identity_arn: Option<&str>,
) -> Value {
    let region = stream_arn.split(':').nth(3).unwrap_or_default();
    let mut event = json!({
        "awsRegion": region,
        "eventID": format!("{}:{}", shard_id, record.sequence_number),
        "eventName": "aws:kinesis:record",
        "eventSource": "aws:kinesis",
        "eventSourceARN": stream_arn,
        "eventVersion": "1.0",
        "kinesis": {
            "approximateArrivalTimestamp": record.approximate_arrival_timestamp.timestamp_millis() as f64 / 1000.0,
            "data": base64::engine::general_purpose::STANDARD.encode(&record.data),
            "kinesisSchemaVersion": "1.0",
            "sequenceNumber": record.sequence_number,
        }
    });
    // A record written without a key (AUTO record distribution) carries no
    // partitionKey, matching what GetRecords returns for it.
    if !record.partition_key.is_empty() {
        event["kinesis"]["partitionKey"] = json!(record.partition_key);
    }
    if let Some(role) = invoke_identity_arn {
        event["invokeIdentityArn"] = json!(role);
    }
    event
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
    fn event_record_carries_stream_region() {
        let record = fakecloud_kinesis::KinesisRecord {
            sequence_number: "49590338271490256608559692538361571095921575989136588898".into(),
            partition_key: "pk".into(),
            data: b"hello".to_vec(),
            approximate_arrival_timestamp: Utc::now(),
        };
        let arn = "arn:aws:kinesis:eu-west-2:111122223333:stream/orders";
        let ev = kinesis_event_record(&record, "shardId-000000000000", arn, None);
        assert_eq!(ev["awsRegion"], "eu-west-2");
        assert_eq!(ev["eventSourceARN"], arn);
        assert_eq!(
            ev["eventID"],
            "shardId-000000000000:49590338271490256608559692538361571095921575989136588898"
        );
        assert_eq!(ev["kinesis"]["data"], "aGVsbG8=");

        let cn = "arn:aws-cn:kinesis:cn-north-1:111122223333:stream/orders";
        let ev = kinesis_event_record(&record, "shardId-000000000000", cn, None);
        assert_eq!(ev["awsRegion"], "cn-north-1");
        assert_eq!(ev["kinesis"]["partitionKey"], "pk");

        // A keyless (AUTO-distributed) record carries no partitionKey.
        let keyless = fakecloud_kinesis::KinesisRecord {
            partition_key: String::new(),
            ..record
        };
        let ev = kinesis_event_record(&keyless, "shardId-000000000000", arn, None);
        assert!(ev["kinesis"].get("partitionKey").is_none());
    }

    fn esm(
        uuid: &str,
        function_arn: &str,
        stream_arn: &str,
    ) -> fakecloud_lambda::EventSourceMapping {
        fakecloud_lambda::EventSourceMapping {
            uuid: uuid.to_string(),
            function_arn: function_arn.to_string(),
            event_source_arn: stream_arn.to_string(),
            batch_size: 100,
            enabled: true,
            state: "Enabled".to_string(),
            last_modified: Utc::now(),
            filter_patterns: Vec::new(),
            maximum_batching_window_in_seconds: None,
            starting_position: Some("TRIM_HORIZON".to_string()),
            starting_position_timestamp: None,
            parallelization_factor: None,
            function_response_types: Vec::new(),
            kms_key_arn: None,
            metrics_config: None,
            destination_config: None,
            maximum_retry_attempts: None,
            maximum_record_age_in_seconds: None,
            bisect_batch_on_function_error: None,
            tumbling_window_in_seconds: None,
            topics: Vec::new(),
            queues: Vec::new(),
            source_access_configurations: Vec::new(),
            self_managed_event_source: None,
            self_managed_kafka_event_source_config: None,
            document_db_event_source_config: None,
        }
    }

    /// `invokeIdentityArn` is the mapped function's execution role in the
    /// function's own account and partition, not a fixed placeholder.
    #[test]
    fn invoke_identity_arn_is_the_function_execution_role() {
        use fakecloud_core::multi_account::MultiAccountState;
        use fakecloud_lambda::{LambdaFunction, LambdaState};
        use parking_lot::RwLock;

        let account = "444455556666";
        let region = "cn-north-1";
        let role = format!("arn:aws-cn:iam::{account}:role/service-role/orders-consumer");
        let fn_arn = format!("arn:aws-cn:lambda:{region}:{account}:function:orders");
        let stream_arn = format!("arn:aws-cn:kinesis:{region}:{account}:stream/orders");

        let mut lambda: MultiAccountState<LambdaState> =
            MultiAccountState::new(account, region, "http://localhost:4566");
        {
            let l = lambda.default_mut();
            l.functions.insert(
                "orders".to_string(),
                LambdaFunction {
                    function_name: "orders".to_string(),
                    function_arn: fn_arn.clone(),
                    role: role.clone(),
                    ..Default::default()
                },
            );
            // A mapping on a qualified (alias) ARN still resolves the function.
            l.event_source_mappings.insert(
                "esm-1".to_string(),
                esm("esm-1", &format!("{fn_arn}:live"), &stream_arn),
            );
            // A mapping whose function is gone reports no identity.
            l.event_source_mappings.insert(
                "esm-2".to_string(),
                esm(
                    "esm-2",
                    &format!("arn:aws-cn:lambda:{region}:{account}:function:gone"),
                    &stream_arn,
                ),
            );
        }
        let kinesis: fakecloud_kinesis::SharedKinesisState = Arc::new(RwLock::new(
            MultiAccountState::new(account, region, "http://localhost:4566"),
        ));
        let poller = KinesisLambdaPoller::new(kinesis, Arc::new(RwLock::new(lambda)));

        let mut mappings = poller.collect_mappings();
        mappings.sort_by(|a, b| a.uuid.cmp(&b.uuid));
        assert_eq!(mappings.len(), 2);
        assert_eq!(
            mappings[0].invoke_identity_arn.as_deref(),
            Some(role.as_str())
        );
        assert_eq!(mappings[1].invoke_identity_arn, None);

        let record = fakecloud_kinesis::KinesisRecord {
            sequence_number: "1".into(),
            partition_key: "pk".into(),
            data: b"x".to_vec(),
            approximate_arrival_timestamp: Utc::now(),
        };
        let ev = kinesis_event_record(
            &record,
            "shardId-000000000000",
            &stream_arn,
            mappings[0].invoke_identity_arn.as_deref(),
        );
        assert_eq!(ev["invokeIdentityArn"], role.as_str());
        let ev = kinesis_event_record(&record, "shardId-000000000000", &stream_arn, None);
        assert!(ev.get("invokeIdentityArn").is_none());
    }

    struct RecordingDelivery(parking_lot::Mutex<Vec<String>>);

    impl LambdaDelivery for RecordingDelivery {
        fn invoke_lambda(
            &self,
            _function_arn: &str,
            payload: &str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>>
        {
            self.0.lock().push(payload.to_string());
            Box::pin(async { Ok(b"{}".to_vec()) })
        }
    }

    /// The poller reads the stream in the region its ARN names, not a
    /// same-named stream of another region, and checkpoints there.
    #[tokio::test]
    async fn poller_reads_the_stream_in_its_arn_region() {
        use fakecloud_core::delivery::KinesisDelivery;
        use fakecloud_core::multi_account::MultiAccountState;
        use parking_lot::RwLock;

        let account = "123456789012";
        let kinesis: fakecloud_kinesis::SharedKinesisState = Arc::new(RwLock::new(
            MultiAccountState::new(account, "us-east-1", "http://localhost:4566"),
        ));
        for region in ["us-east-1", "eu-west-1"] {
            let mut mas = kinesis.write();
            let st = mas.regional_mut(account, region);
            let arn = st.stream_arn(region, "orders");
            st.streams.insert(
                "orders".to_string(),
                fakecloud_kinesis::KinesisStream {
                    stream_name: "orders".to_string(),
                    stream_arn: arn,
                    stream_status: "ACTIVE".to_string(),
                    stream_creation_timestamp: Utc::now(),
                    retention_period_hours: 24,
                    stream_mode: "PROVISIONED".to_string(),
                    encryption_type: "NONE".to_string(),
                    key_id: None,
                    shard_count: 1,
                    open_shard_count: 1,
                    tags: Default::default(),
                    shards: fakecloud_kinesis::build_stream_shards(1),
                    next_shard_index: 1,
                    enhanced_metrics: Vec::new(),
                    warm_throughput_mibps: None,
                    max_record_size_kib: None,
                    record_distribution_strategy:
                        fakecloud_kinesis::default_record_distribution_strategy(),
                    auto_distribution_cursor: 0,
                },
            );
        }
        let west_arn = format!("arn:aws:kinesis:eu-west-1:{account}:stream/orders");
        fakecloud_kinesis::delivery::KinesisDeliveryImpl::new(kinesis.clone())
            .put_record(&west_arn, "aGVsbG8=", "pk");

        let delivery = Arc::new(RecordingDelivery(parking_lot::Mutex::new(Vec::new())));
        let poller = KinesisLambdaPoller::new(
            kinesis.clone(),
            Arc::new(RwLock::new(MultiAccountState::new(
                account,
                "us-east-1",
                "http://localhost:4566",
            ))),
        )
        .with_lambda_delivery(delivery.clone());
        let mapping = Mapping {
            uuid: "esm-west".to_string(),
            function_arn: format!("arn:aws:lambda:eu-west-1:{account}:function:f"),
            stream_arn: west_arn,
            batch_size: 10,
            filter: FilterSet::from_strings(std::iter::empty::<&String>()),
            starting_position: Some("TRIM_HORIZON".to_string()),
            starting_position_timestamp: None,
            report_batch_item_failures: false,
            invoke_identity_arn: None,
        };
        poller.process_mapping(&mapping).await;

        let payloads = delivery.0.lock().clone();
        assert_eq!(payloads.len(), 1, "one batch from the eu-west-1 stream");
        assert!(payloads[0].contains("eu-west-1"));
        let mas = kinesis.read();
        let west = mas.regional(account, "eu-west-1").unwrap();
        assert_eq!(
            west.lambda_checkpoint("esm-west", "shardId-000000000000"),
            1
        );
        let east = mas.regional(account, "us-east-1").unwrap();
        assert!(east.lambda_checkpoints.is_empty());
    }

    #[test]
    fn first_failed_index_invalid_json_returns_none() {
        let body = b"not json";
        let seqs = ["a"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(first_failed_index(body, &seqs).is_none());
    }
}
