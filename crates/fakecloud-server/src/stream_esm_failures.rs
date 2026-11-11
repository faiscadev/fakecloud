//! What a stream event source mapping (DynamoDB Streams, Kinesis) does with
//! a batch its function fails, as recorded against AWS
//! (fakecloud-lambda/tests/fixtures/stream-esm-failures/aws-recordings.json):
//!
//! - A failing batch is retried until `MaximumRetryAttempts` is used up
//!   (`-1`, the default, retries until the records expire), backing off
//!   exponentially from about 200ms. A batch Lambda could not run at all
//!   (recorded with a throttled function) is retried the same way without
//!   using up retries and is never bisected: only its records' age ends it.
//! - `MaximumRecordAgeInSeconds` discards a batch whose oldest record is
//!   older than that before it is invoked again.
//! - `BisectBatchOnFunctionError` splits a failing batch in two, earlier half
//!   first; a split does not use up a retry, so retries only count once a
//!   batch is down to one record.
//! - With `ReportBatchItemFailures`, the shard checkpoints up to the first
//!   reported failure and the rest is a new batch with fresh retries.
//! - A discarded batch is described to the `OnFailure` destination (SQS,
//!   SNS or S3, where the record also carries the batch's event as
//!   `payload`) and the shard moves past it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use fakecloud_core::delivery::{DeliveryBus, LambdaInvocationOutput};
use fakecloud_lambda::EventSourceMapping;

/// First wait before retrying a failed batch; doubles per failure.
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(200);
const RETRY_BACKOFF_MAX: Duration = Duration::from_secs(300);

/// A mapping's failure handling settings.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct FailurePolicy {
    /// `None`: retry until the records expire.
    pub max_retries: Option<u32>,
    /// `None`: no age limit beyond the stream's retention.
    pub max_record_age: Option<chrono::Duration>,
    pub bisect: bool,
    pub report_batch_item_failures: bool,
    pub on_failure: Option<String>,
}

impl FailurePolicy {
    pub(crate) fn of(mapping: &EventSourceMapping) -> Self {
        Self {
            max_retries: mapping
                .maximum_retry_attempts
                .and_then(|n| u32::try_from(n).ok()),
            max_record_age: mapping
                .maximum_record_age_in_seconds
                .filter(|secs| *secs >= 0)
                .map(chrono::Duration::seconds),
            bisect: mapping.bisect_batch_on_function_error.unwrap_or(false),
            report_batch_item_failures: mapping
                .function_response_types
                .iter()
                .any(|t| t.eq_ignore_ascii_case("ReportBatchItemFailures")),
            on_failure: mapping
                .destination_config
                .as_ref()
                .and_then(|d| d["OnFailure"]["Destination"].as_str())
                .filter(|d| !d.is_empty())
                .map(str::to_string),
        }
    }
}

/// How one invocation of a batch went.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Attempt {
    /// Every record was processed.
    Succeeded,
    /// The function handled the records before this index (only with
    /// `ReportBatchItemFailures`); the rest failed.
    Partial(usize),
    /// The function failed the batch: a function error (`function_error`
    /// its type), or a partial failure that made no progress (`None`).
    Failed { function_error: Option<String> },
    /// Lambda could not run the invocation at all.
    Unrunnable,
}

impl Attempt {
    /// Classify a Lambda invocation result for a batch whose records have
    /// the sequence numbers `batch_seqs`.
    pub(crate) fn of(
        result: &Result<LambdaInvocationOutput, String>,
        policy: &FailurePolicy,
        batch_seqs: &[String],
    ) -> Self {
        match result {
            Ok(output) if output.is_function_error() => Attempt::Failed {
                function_error: output.function_error.clone(),
            },
            Ok(output) if policy.report_batch_item_failures => {
                match first_failed_index(&output.payload, batch_seqs) {
                    None => Attempt::Succeeded,
                    Some(0) => Attempt::Failed {
                        function_error: None,
                    },
                    Some(idx) => Attempt::Partial(idx),
                }
            }
            Ok(_) => Attempt::Succeeded,
            Err(_) => Attempt::Unrunnable,
        }
    }
}

/// What the poller does next with a shard's pending records.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Step {
    /// Waiting out a retry backoff.
    Wait,
    /// Invoke the function with the first `n` pending records.
    Invoke(usize),
    /// Discard the first `n` pending records: their age is past
    /// `MaximumRecordAgeInSeconds`.
    DiscardTooOld { n: usize, invoke_count: u32 },
}

/// What follows an invocation.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Resolution {
    /// Checkpoint past the first `n` records of the batch.
    Advance(usize),
    /// Keep the checkpoint; the batch is retried (whole or split).
    Retry,
    /// Retries are used up: discard the batch (checkpoint past its `n`
    /// records) and describe it to the OnFailure destination, with the last
    /// invocation's function error.
    Discard {
        n: usize,
        invoke_count: u32,
        function_error: Option<String>,
    },
}

#[derive(Debug, Default)]
struct ShardRetry {
    /// Invocations of the current batch that failed.
    failures: u32,
    /// Bisected batches still to deliver, by length from the checkpoint;
    /// the last one is next.
    segments: Vec<usize>,
    not_before: Option<Instant>,
    /// Attempts of the current batch Lambda could not run.
    unrun: u32,
    /// The sequence number the next batch should start at. A batch starting
    /// anywhere else (its records expired, the checkpoint moved) is not the
    /// one this state describes, and starts afresh.
    next_first: Option<String>,
}

/// Retry bookkeeping for every (mapping, shard) pair of one poller, keyed
/// `<mapping uuid>:<shard id>`.
#[derive(Debug, Default)]
pub(crate) struct RetryTracker {
    shards: parking_lot::Mutex<HashMap<String, ShardRetry>>,
}

/// A pending record: its sequence number and arrival time.
pub(crate) type Pending = (String, DateTime<Utc>);

impl RetryTracker {
    /// Decide what to do with the shard `key`'s `pending` records, oldest
    /// first.
    pub(crate) fn plan(
        &self,
        key: &str,
        policy: &FailurePolicy,
        pending: &[Pending],
        now: DateTime<Utc>,
    ) -> Step {
        let mut shards = self.shards.lock();
        let shard = shards.entry(key.to_string()).or_default();
        let first = pending.first().map(|(seq, _)| seq);
        if shard.next_first.is_some() && shard.next_first.as_ref() != first {
            *shard = ShardRetry::default();
        }
        shard.next_first = first.cloned();
        if shard.not_before.is_some_and(|t| Instant::now() < t) {
            return Step::Wait;
        }
        let n = shard
            .segments
            .last()
            .copied()
            .unwrap_or(pending.len())
            .min(pending.len());
        if let (Some(max_age), Some((_, oldest))) = (policy.max_record_age, pending.first()) {
            if now - *oldest > max_age {
                return Step::DiscardTooOld {
                    n,
                    invoke_count: shard.failures + 1,
                };
            }
        }
        Step::Invoke(n)
    }

    /// Record how invoking the first `n` of the shard `key`'s `pending`
    /// records went.
    pub(crate) fn resolve(
        &self,
        key: &str,
        policy: &FailurePolicy,
        pending: &[Pending],
        n: usize,
        attempt: Attempt,
    ) -> Resolution {
        let mut shards = self.shards.lock();
        let shard = shards.entry(key.to_string()).or_default();
        let resolution = match attempt {
            Attempt::Succeeded => {
                shard.finish_segment();
                Resolution::Advance(n)
            }
            Attempt::Partial(idx) => {
                // The rest of this batch is a new batch with fresh retries;
                // the bisected batches after it stay as they were.
                if let Some(last) = shard.segments.last_mut() {
                    *last -= idx;
                }
                shard.failures = 0;
                shard.not_before = None;
                Resolution::Advance(idx)
            }
            Attempt::Failed { function_error } => {
                if policy.bisect && n > 1 {
                    let left = n.div_ceil(2);
                    shard.segments.pop();
                    shard.segments.push(n - left);
                    shard.segments.push(left);
                    shard.failures = 0;
                    shard.not_before = None;
                    Resolution::Retry
                } else {
                    shard.retry_or_discard(policy, n, function_error)
                }
            }
            // A batch Lambda could not run says nothing about its records:
            // it is retried whole, never bisected, and uses up no retries
            // (recorded with a throttled function).
            Attempt::Unrunnable => {
                shard.unrun += 1;
                shard.not_before = Some(Instant::now() + backoff(shard.unrun));
                Resolution::Retry
            }
        };
        let consumed = match &resolution {
            Resolution::Advance(k) => *k,
            Resolution::Retry => 0,
            Resolution::Discard { n, .. } => *n,
        };
        shard.next_first = pending.get(consumed).map(|(seq, _)| seq.clone());
        resolution
    }

    /// The first `n` of the shard `key`'s `pending` records were discarded
    /// for their age.
    pub(crate) fn discarded(&self, key: &str, pending: &[Pending], n: usize) {
        if let Some(shard) = self.shards.lock().get_mut(key) {
            shard.finish_segment();
            shard.next_first = pending.get(n).map(|(seq, _)| seq.clone());
        }
    }

    /// Forget shards whose mapping is gone.
    pub(crate) fn retain(&self, keep: impl Fn(&str) -> bool) {
        self.shards.lock().retain(|key, _| keep(key));
    }
}

impl ShardRetry {
    fn finish_segment(&mut self) {
        self.segments.pop();
        self.failures = 0;
        self.unrun = 0;
        self.not_before = None;
    }

    /// Count a failed invocation: discard once retries are used up, else
    /// back off.
    fn retry_or_discard(
        &mut self,
        policy: &FailurePolicy,
        n: usize,
        function_error: Option<String>,
    ) -> Resolution {
        self.failures += 1;
        if policy.max_retries.is_some_and(|max| self.failures > max) {
            let invoke_count = self.failures;
            self.finish_segment();
            return Resolution::Discard {
                n,
                invoke_count,
                function_error,
            };
        }
        self.not_before = Some(Instant::now() + backoff(self.failures));
        Resolution::Retry
    }
}

/// The wait after the `nth` failure in a row.
fn backoff(nth: u32) -> Duration {
    RETRY_BACKOFF_BASE
        .saturating_mul(1u32 << nth.saturating_sub(1).min(16))
        .min(RETRY_BACKOFF_MAX)
}

/// Which stream a discarded batch came from.
pub(crate) enum StreamKind {
    Kinesis,
    DynamoDb,
}

/// Why a batch was discarded.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DiscardCause {
    /// Its oldest record was older than `MaximumRecordAgeInSeconds`.
    RecordAge,
    /// `MaximumRetryAttempts` were used up; the last invocation's function
    /// error.
    RetriesExhausted { function_error: Option<String> },
}

/// A discarded batch, as its OnFailure record describes it.
pub(crate) struct DiscardedBatch<'a> {
    pub kind: StreamKind,
    pub mapping_uuid: &'a str,
    pub function_arn: &'a str,
    pub stream_arn: &'a str,
    pub shard_id: &'a str,
    pub first_sequence: &'a str,
    pub last_sequence: &'a str,
    pub first_arrival: DateTime<Utc>,
    pub last_arrival: DateTime<Utc>,
    pub batch_size: usize,
    pub invoke_count: u32,
    pub cause: DiscardCause,
    /// The batch's event (`{"Records": [...]}`), which an S3 record carries
    /// as `payload`.
    pub payload: String,
}

impl DiscardedBatch<'_> {
    fn condition(&self) -> &'static str {
        match self.cause {
            DiscardCause::RetriesExhausted { .. } => "RetryAttemptsExhausted",
            DiscardCause::RecordAge => "RecordAgeExceeded",
        }
    }

    /// The record Lambda sends the OnFailure destination; an S3 one also
    /// carries the batch's event.
    pub(crate) fn record(&self, with_payload: bool) -> Value {
        let unqualified: String = self
            .function_arn
            .splitn(8, ':')
            .take(7)
            .collect::<Vec<_>>()
            .join(":");
        let (info_key, first, last) = match self.kind {
            StreamKind::Kinesis => (
                "KinesisBatchInfo",
                self.first_arrival
                    .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                    .to_string(),
                self.last_arrival
                    .format("%Y-%m-%dT%H:%M:%S%.3fZ")
                    .to_string(),
            ),
            StreamKind::DynamoDb => (
                "DDBStreamBatchInfo",
                self.first_arrival.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                self.last_arrival.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            ),
        };
        let mut record = json!({
            "requestContext": {
                "requestId": uuid::Uuid::new_v4().to_string(),
                "functionArn": unqualified,
                "condition": self.condition(),
                "approximateInvokeCount": self.invoke_count,
            },
            "version": "1.0",
            "timestamp": Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        });
        if with_payload {
            record["payload"] = Value::String(self.payload.clone());
        }
        if let DiscardCause::RetriesExhausted { function_error } = &self.cause {
            record["responseContext"] = json!({
                "statusCode": 200,
                "executedVersion": "$LATEST",
                "functionError": function_error,
            });
        }
        record[info_key] = json!({
            "shardId": self.shard_id,
            "startSequenceNumber": self.first_sequence,
            "endSequenceNumber": self.last_sequence,
            "approximateArrivalOfFirstRecord": first,
            "approximateArrivalOfLastRecord": last,
            "batchSize": self.batch_size,
            "streamArn": self.stream_arn,
        });
        record
    }

    /// Describe the batch to `destination`.
    pub(crate) fn send(&self, bus: &Arc<DeliveryBus>, destination: &str) {
        let service = destination.split(':').nth(2).unwrap_or_default();
        let body = self.record(service == "s3").to_string();
        match service {
            "sqs" => bus.send_to_sqs(destination, &body, &HashMap::new()),
            "sns" => bus.publish_to_sns(destination, &body, None),
            "s3" => {
                let bucket = destination.rsplit(':').next().unwrap_or_default();
                let now = Utc::now();
                let key = format!(
                    "aws/lambda/{}/{}/{}/{}-{}",
                    self.mapping_uuid,
                    self.shard_id,
                    now.format("%Y/%m/%d"),
                    now.format("%Y-%m-%dT%H.%M.%S"),
                    uuid::Uuid::new_v4()
                );
                let account_id = self.function_arn.split(':').nth(4).unwrap_or_default();
                if let Err(err) = bus.put_object_to_s3(
                    account_id,
                    bucket,
                    &key,
                    body.into_bytes(),
                    Some("application/octet-stream"),
                ) {
                    tracing::warn!(destination, %err, "stream mapping OnFailure: S3 refused the record");
                }
            }
            _ => tracing::warn!(
                destination,
                "stream mapping OnFailure: unsupported destination"
            ),
        }
    }
}

/// Apply a `ReportBatchItemFailures` response
/// (`{"batchItemFailures":[{"itemIdentifier":"<seqno>"}]}`) to the batch:
/// the index in `batch_seqs` of the first failed record, or `None` when the
/// whole batch succeeded.
///
/// As on AWS, an empty or null `batchItemFailures` list or an empty or null
/// response is a complete success, while an invalid JSON response or an
/// entry whose `itemIdentifier` is empty, null, missing (a bad key name) or
/// names a sequence number not in the batch is a complete failure (index 0).
pub(crate) fn first_failed_index(body: &[u8], batch_seqs: &[String]) -> Option<usize> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return None;
    }
    let Ok(parsed) = serde_json::from_slice::<Value>(body) else {
        return Some(0);
    };
    let failures = parsed.get("batchItemFailures")?.as_array()?;
    let mut first: Option<usize> = None;
    for failure in failures {
        let idx = failure
            .get("itemIdentifier")
            .and_then(|v| v.as_str())
            .filter(|id| !id.is_empty())
            .and_then(|id| batch_seqs.iter().position(|seq| seq == id));
        match idx {
            Some(idx) => first = Some(first.map_or(idx, |f| f.min(idx))),
            None => return Some(0),
        }
    }
    first
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(max_retries: Option<u32>, bisect: bool, rbif: bool) -> FailurePolicy {
        FailurePolicy {
            max_retries,
            bisect,
            report_batch_item_failures: rbif,
            ..FailurePolicy::default()
        }
    }

    fn failed() -> Attempt {
        Attempt::Failed {
            function_error: Some("Unhandled".to_string()),
        }
    }

    /// A discarded batch's record ids and invoke count.
    type Discarded = (Vec<String>, u32);

    /// Drive a shard of `records` (each `true` = poison) the way the
    /// recordings' handlers fail, returning each invocation's record ids
    /// and the discarded batches (ids, invoke count).
    fn drive(
        records: &[(&str, bool)],
        policy: &FailurePolicy,
    ) -> (Vec<Vec<String>>, Vec<Discarded>) {
        let tracker = RetryTracker::default();
        let mut pending: Vec<(&str, bool)> = records.to_vec();
        let mut invocations = Vec::new();
        let mut discarded = Vec::new();
        let now = Utc::now();
        while !pending.is_empty() {
            let arrivals: Vec<Pending> = pending.iter().map(|r| (r.0.to_string(), now)).collect();
            // Skip backoffs: the tracker is told nothing about time.
            if let Some(shard) = tracker.shards.lock().get_mut("k") {
                shard.not_before = None;
            }
            let Step::Invoke(n) = tracker.plan("k", policy, &arrivals, now) else {
                panic!("expected an invocation");
            };
            let batch = &pending[..n];
            invocations.push(batch.iter().map(|r| r.0.to_string()).collect());
            let attempt = if policy.report_batch_item_failures {
                match batch.iter().position(|r| r.1) {
                    None => Attempt::Succeeded,
                    Some(0) => Attempt::Failed {
                        function_error: None,
                    },
                    Some(i) => Attempt::Partial(i),
                }
            } else if batch.iter().any(|r| r.1) {
                failed()
            } else {
                Attempt::Succeeded
            };
            match tracker.resolve("k", policy, &arrivals, n, attempt) {
                Resolution::Advance(k) => {
                    pending.drain(..k);
                }
                Resolution::Retry => {}
                Resolution::Discard {
                    n, invoke_count, ..
                } => {
                    discarded.push((
                        pending[..n].iter().map(|r| r.0.to_string()).collect(),
                        invoke_count,
                    ));
                    pending.drain(..n);
                }
            }
        }
        (invocations, discarded)
    }

    fn ids(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    /// Recorded scenario C: retries=1 invokes twice, then discards.
    #[test]
    fn retries_then_discard() {
        let (inv, dropped) = drive(
            &[("c-1", true), ("c-2", true)],
            &policy(Some(1), false, false),
        );
        assert_eq!(inv, vec![ids(&["c-1", "c-2"]), ids(&["c-1", "c-2"])]);
        assert_eq!(dropped, vec![(ids(&["c-1", "c-2"]), 2)]);
    }

    /// Recorded scenario D: bisect splits until the poison record is alone,
    /// which then uses its retries.
    #[test]
    fn bisect_isolates_the_poison_record() {
        let (inv, dropped) = drive(
            &[
                ("d-1", false),
                ("d-2", false),
                ("d-poison-3", true),
                ("d-4", false),
            ],
            &policy(Some(2), true, false),
        );
        assert_eq!(
            inv,
            vec![
                ids(&["d-1", "d-2", "d-poison-3", "d-4"]),
                ids(&["d-1", "d-2"]),
                ids(&["d-poison-3", "d-4"]),
                ids(&["d-poison-3"]),
                ids(&["d-poison-3"]),
                ids(&["d-poison-3"]),
                ids(&["d-4"]),
            ]
        );
        assert_eq!(dropped, vec![(ids(&["d-poison-3"]), 3)]);
    }

    /// Recorded scenario E: a partial failure checkpoints up to the failed
    /// record; the remainder is a new batch whose retries run out.
    #[test]
    fn partial_failure_checkpoints_then_retries_the_rest() {
        let (inv, dropped) = drive(
            &[
                ("e-1", false),
                ("e-poison-2", true),
                ("e-3", false),
                ("e-4", false),
            ],
            &policy(Some(1), false, true),
        );
        assert_eq!(
            inv,
            vec![
                ids(&["e-1", "e-poison-2", "e-3", "e-4"]),
                ids(&["e-poison-2", "e-3", "e-4"]),
                ids(&["e-poison-2", "e-3", "e-4"]),
            ]
        );
        assert_eq!(dropped, vec![(ids(&["e-poison-2", "e-3", "e-4"]), 2)]);
    }

    /// Recorded scenario H: with infinite retries a batch is discarded once
    /// its oldest record is too old, counting the attempt it did not make.
    #[test]
    fn record_age_discards_with_one_more_invoke_counted() {
        let tracker = RetryTracker::default();
        let p = FailurePolicy {
            max_retries: None,
            max_record_age: Some(chrono::Duration::seconds(60)),
            ..FailurePolicy::default()
        };
        let now = Utc::now();
        let arrival = vec![("1".to_string(), now - chrono::Duration::seconds(10))];
        for _ in 0..9 {
            tracker
                .shards
                .lock()
                .entry("k".into())
                .or_default()
                .not_before = None;
            assert_eq!(tracker.plan("k", &p, &arrival, now), Step::Invoke(1));
            assert_eq!(
                tracker.resolve("k", &p, &arrival, 1, failed()),
                Resolution::Retry
            );
        }
        tracker.shards.lock().get_mut("k").unwrap().not_before = None;
        let old = vec![("1".to_string(), now - chrono::Duration::seconds(61))];
        assert_eq!(
            tracker.plan("k", &p, &old, now),
            Step::DiscardTooOld {
                n: 1,
                invoke_count: 10
            }
        );
    }

    fn pending(seqs: &[&str]) -> Vec<Pending> {
        seqs.iter().map(|s| (s.to_string(), Utc::now())).collect()
    }

    fn ready(tracker: &RetryTracker) {
        if let Some(shard) = tracker.shards.lock().get_mut("k") {
            shard.not_before = None;
        }
    }

    /// Retries belong to the batch they counted: a batch starting at another
    /// record (the earlier ones expired) starts with fresh retries.
    #[test]
    fn a_batch_starting_elsewhere_starts_afresh() {
        let tracker = RetryTracker::default();
        let p = policy(Some(1), false, false);
        let first = pending(&["1", "2"]);
        assert_eq!(tracker.plan("k", &p, &first, Utc::now()), Step::Invoke(2));
        assert_eq!(
            tracker.resolve("k", &p, &first, 2, failed()),
            Resolution::Retry
        );
        ready(&tracker);
        let moved = pending(&["2", "3"]);
        assert_eq!(tracker.plan("k", &p, &moved, Utc::now()), Step::Invoke(2));
        assert_eq!(
            tracker.resolve("k", &p, &moved, 2, failed()),
            Resolution::Retry,
            "the retry used on the old batch does not count"
        );
    }

    /// Recorded (scenario THROTTLED): a batch Lambda could not run is
    /// retried whole, never bisected, and uses up no retries.
    #[test]
    fn an_unrunnable_batch_is_not_bisected_nor_discarded() {
        let tracker = RetryTracker::default();
        let p = policy(Some(1), true, false);
        let batch = pending(&["1", "2", "3", "4"]);
        for _ in 0..5 {
            ready(&tracker);
            assert_eq!(tracker.plan("k", &p, &batch, Utc::now()), Step::Invoke(4));
            assert_eq!(
                tracker.resolve("k", &p, &batch, 4, Attempt::Unrunnable),
                Resolution::Retry
            );
        }
        // It backs off like any failure.
        assert_eq!(tracker.plan("k", &p, &batch, Utc::now()), Step::Wait);
    }

    /// A partial failure inside a bisected half keeps the other half
    /// pending.
    #[test]
    fn a_partial_failure_keeps_the_pending_halves() {
        let tracker = RetryTracker::default();
        let p = policy(Some(2), true, true);
        let batch = pending(&["1", "2", "3", "4"]);
        tracker.plan("k", &p, &batch, Utc::now());
        tracker.resolve("k", &p, &batch, 4, failed());
        // Left half: the second record fails.
        assert_eq!(tracker.plan("k", &p, &batch, Utc::now()), Step::Invoke(2));
        assert_eq!(
            tracker.resolve("k", &p, &batch, 2, Attempt::Partial(1)),
            Resolution::Advance(1)
        );
        let rest = pending(&["2", "3", "4"]);
        assert_eq!(
            tracker.plan("k", &p, &rest, Utc::now()),
            Step::Invoke(1),
            "the rest of the left half"
        );
        assert_eq!(
            tracker.resolve("k", &p, &rest, 1, Attempt::Succeeded),
            Resolution::Advance(1)
        );
        let right = pending(&["3", "4"]);
        assert_eq!(
            tracker.plan("k", &p, &right, Utc::now()),
            Step::Invoke(2),
            "then the right half"
        );
    }

    #[test]
    fn failures_back_off_exponentially() {
        let tracker = RetryTracker::default();
        let p = policy(None, false, false);
        let now = Utc::now();
        let arrivals = vec![("1".to_string(), now)];
        assert_eq!(tracker.plan("k", &p, &arrivals, now), Step::Invoke(1));
        tracker.resolve("k", &p, &arrivals, 1, failed());
        assert_eq!(tracker.plan("k", &p, &arrivals, now), Step::Wait);
        let waits: Vec<Duration> = (0..3)
            .map(|_| {
                let before = Instant::now();
                tracker.resolve("k", &p, &arrivals, 1, failed());
                tracker.shards.lock()["k"].not_before.unwrap() - before
            })
            .collect();
        assert!(waits[0] >= Duration::from_millis(400) && waits[0] < Duration::from_millis(500));
        assert!(waits[2] >= Duration::from_millis(1600));
    }

    #[test]
    fn records_match_the_recorded_shapes() {
        let at = DateTime::parse_from_rfc3339("2026-11-10T19:49:37.502Z")
            .unwrap()
            .with_timezone(&Utc);
        let mut batch = DiscardedBatch {
            kind: StreamKind::Kinesis,
            mapping_uuid: "u",
            function_arn: "arn:aws:lambda:us-east-1:123456789012:function:f",
            stream_arn: "arn:aws:kinesis:us-east-1:123456789012:stream/s",
            shard_id: "shardId-000000000000",
            first_sequence: "1",
            last_sequence: "3",
            first_arrival: at,
            last_arrival: at,
            batch_size: 3,
            invoke_count: 2,
            cause: DiscardCause::RetriesExhausted {
                function_error: None,
            },
            payload: r#"{"Records":[]}"#.to_string(),
        };
        let r = batch.record(false);
        assert!(r.get("payload").is_none());
        assert_eq!(
            batch.record(true)["payload"],
            r#"{"Records":[]}"#,
            "an S3 record carries the batch's event (scenario S3_DESTINATION)"
        );
        assert_eq!(r["requestContext"]["condition"], "RetryAttemptsExhausted");
        assert_eq!(r["requestContext"]["approximateInvokeCount"], 2);
        assert_eq!(
            r["requestContext"]["functionArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:f"
        );
        assert_eq!(r["responseContext"]["statusCode"], 200);
        assert!(r["responseContext"]["functionError"].is_null());
        assert_eq!(r["version"], "1.0");
        assert_eq!(
            r["KinesisBatchInfo"]["approximateArrivalOfFirstRecord"],
            "2026-11-10T19:49:37.502Z"
        );
        assert_eq!(r["KinesisBatchInfo"]["batchSize"], 3);

        batch.kind = StreamKind::DynamoDb;
        batch.cause = DiscardCause::RecordAge;
        let r = batch.record(false);
        assert_eq!(r["requestContext"]["condition"], "RecordAgeExceeded");
        assert!(r.get("responseContext").is_none());
        assert_eq!(
            r["DDBStreamBatchInfo"]["approximateArrivalOfLastRecord"],
            "2026-11-10T19:49:37Z"
        );
    }

    #[test]
    fn policy_reads_the_mapping() {
        let mapping: EventSourceMapping = serde_json::from_value(json!({
            "uuid": "u",
            "function_arn": "arn:aws:lambda:us-east-1:1:function:f",
            "event_source_arn": "arn:aws:kinesis:us-east-1:1:stream/s",
            "batch_size": 10,
            "enabled": true,
            "state": "Enabled",
            "last_modified": "2026-11-10T00:00:00Z",
            "maximum_retry_attempts": -1,
            "maximum_record_age_in_seconds": 60,
            "bisect_batch_on_function_error": true,
            "function_response_types": ["ReportBatchItemFailures"],
            "destination_config": {"OnFailure": {"Destination": "arn:aws:sqs:us-east-1:1:q"}},
        }))
        .unwrap();
        let p = FailurePolicy::of(&mapping);
        assert_eq!(p.max_retries, None);
        assert_eq!(p.max_record_age, Some(chrono::Duration::seconds(60)));
        assert!(p.bisect && p.report_batch_item_failures);
        assert_eq!(p.on_failure.as_deref(), Some("arn:aws:sqs:us-east-1:1:q"));
    }
}
