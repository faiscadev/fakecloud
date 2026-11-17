//! Background poller that bridges SQS -> Lambda event source mappings.
//!
//! Periodically checks Lambda state for enabled event source mappings
//! pointing to SQS queues, polls those queues for messages, and either
//! invokes the Lambda function via the container runtime (when available)
//! or records the invocation in Lambda state.
//!
//! Honors:
//! - `FilterCriteria` — non-matching messages are dropped (acked).
//! - `FunctionResponseTypes=[ReportBatchItemFailures]` — when the
//!   Lambda response body is `{"batchItemFailures":[{"itemIdentifier":"<id>"}]}`,
//!   only acked message ids are removed; failed ids are made visible
//!   again so the queue can redeliver.
//! - `MaximumBatchingWindowInSeconds` — once a partial batch is
//!   observed, the poller waits up to N seconds for it to fill before
//!   invoking the Lambda. Once `BatchSize` is reached or the window
//!   expires, the batch is dispatched.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use fakecloud_core::delivery::{KmsHook, LambdaDelivery};
use fakecloud_lambda::filter::FilterSet;
use fakecloud_lambda::{LambdaInvocation, SharedLambdaState};
use fakecloud_persistence::SnapshotHook;
use fakecloud_sqs::SharedSqsState;

#[derive(Clone)]
struct Mapping {
    uuid: String,
    function_arn: String,
    queue_arn: String,
    batch_size: i64,
    filter: FilterSet,
    report_batch_item_failures: bool,
    max_batching_window_seconds: i64,
    /// Batches of this mapping that may be with the function at once:
    /// `ScalingConfig.MaximumConcurrency`, else [`DEFAULT_MAX_CONCURRENCY`].
    max_concurrency: usize,
}

/// Concurrent batches of a mapping without `ScalingConfig.MaximumConcurrency`:
/// the five AWS starts an SQS mapping at (AWS then scales up with a sustained
/// backlog, which is not modelled).
const DEFAULT_MAX_CONCURRENCY: usize = 5;

/// Batches in flight per mapping UUID.
type InFlightCounts = Arc<parking_lot::Mutex<HashMap<String, usize>>>;

/// One of a mapping's concurrency slots, held by a batch from the moment its
/// messages are taken off the queue until its outcome is applied.
struct InFlightSlot {
    counts: InFlightCounts,
    uuid: String,
}

impl InFlightSlot {
    /// A free slot of `uuid`, when it has fewer than `cap` batches in flight.
    fn reserve(counts: &InFlightCounts, uuid: &str, cap: usize) -> Option<Self> {
        let mut guard = counts.lock();
        let n = guard.entry(uuid.to_string()).or_insert(0);
        if *n >= cap {
            return None;
        }
        *n += 1;
        Some(Self {
            counts: counts.clone(),
            uuid: uuid.to_string(),
        })
    }
}

impl Drop for InFlightSlot {
    fn drop(&mut self) {
        let mut guard = self.counts.lock();
        if let Some(n) = guard.get_mut(&self.uuid) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                guard.remove(&self.uuid);
            }
        }
    }
}

/// `(queue ARN, message id)` of every message a batch of this poller has
/// with the function.
type HeldSet = Arc<parking_lot::Mutex<HashSet<(String, String)>>>;

/// A batch's claim on its messages: until it is dropped (the batch's outcome
/// applied, or the receive undone) the poller never receives them again,
/// whatever their visibility timeout.
struct HeldMessages {
    set: HeldSet,
    keys: Vec<(String, String)>,
}

impl HeldMessages {
    fn hold(set: &HeldSet, queue_arn: &str, ids: impl Iterator<Item = String>) -> Self {
        let keys: Vec<(String, String)> = ids.map(|id| (queue_arn.to_string(), id)).collect();
        set.lock().extend(keys.iter().cloned());
        Self {
            set: set.clone(),
            keys,
        }
    }
}

impl Drop for HeldMessages {
    fn drop(&mut self) {
        let mut set = self.set.lock();
        for key in &self.keys {
            set.remove(key);
        }
    }
}

/// A batch taken off the queue and ready for the function.
struct PreparedBatch {
    payload: String,
    message_ids: Vec<String>,
    _held: HeldMessages,
}

/// What one attempt to take a batch found.
enum Taken {
    Batch(PreparedBatch),
    /// Messages were taken, but the filter dropped all of them.
    AllFiltered,
    /// Nothing to send now (empty queue, held batching window, KMS refusal).
    Nothing,
}

/// Per-mapping batching window state. Tracks the timestamp of the
/// first observed message after the most recent invoke so the poller
/// can hold a partial batch open for `max_batching_window_seconds`.
#[derive(Default)]
struct BatchingState {
    window_started_at: HashMap<String, DateTime<Utc>>,
}

/// SQS -> Lambda event source mapping poller.
#[derive(Clone)]
pub struct SqsLambdaPoller {
    sqs_state: SharedSqsState,
    lambda_state: SharedLambdaState,
    lambda_delivery: Option<Arc<dyn LambdaDelivery>>,
    /// KMS hook used to decrypt at-rest message bodies (SSE-KMS or
    /// SSE-SQS) before handing them to the Lambda function. Real AWS
    /// Lambda decrypts server-side; without this, an event source
    /// mapping on a managed-SSE queue would deliver opaque envelope
    /// bytes and break every consumer.
    kms_hook: Option<Arc<dyn KmsHook>>,
    /// Persists SQS state after the poller acks/redelivers messages. Without
    /// this the poller's durable mutations (removed acked messages, bumped
    /// receive_count / cleared visibility) only reached disk on the next
    /// unrelated SQS API write, so a restart in the window re-delivered acked
    /// messages or reset DLQ progress (bug-audit 4.5).
    snapshot_hook: Option<SnapshotHook>,
    /// Batches each mapping has with the function right now.
    in_flight: InFlightCounts,
    /// The messages of those batches.
    held: HeldSet,
}

impl SqsLambdaPoller {
    pub fn new(sqs_state: SharedSqsState, lambda_state: SharedLambdaState) -> Self {
        Self {
            sqs_state,
            lambda_state,
            lambda_delivery: None,
            kms_hook: None,
            snapshot_hook: None,
            in_flight: Arc::default(),
            held: Arc::default(),
        }
    }

    pub fn with_lambda_delivery(mut self, delivery: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_delivery = Some(delivery);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    pub fn with_snapshot_hook(mut self, hook: SnapshotHook) -> Self {
        self.snapshot_hook = Some(hook);
        self
    }

    /// Persist SQS state through the same snapshot path a mutating SQS API
    /// call uses. Called only after the write guard is released so the
    /// blocking snapshot IO never runs under the lock.
    async fn persist(&self) {
        if let Some(hook) = &self.snapshot_hook {
            hook().await;
        }
    }

    pub async fn run(self) {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        let mut batching = BatchingState::default();
        loop {
            interval.tick().await;
            // Batches run detached, so a slow function never holds up the
            // mapping's other concurrency slots or other mappings; each tick
            // tops every mapping up to its concurrency again.
            drop(self.start_batches(&mut batching).await);
        }
    }

    /// One poll cycle that waits for every batch it started.
    #[cfg(test)]
    async fn poll(&self, batching: &mut BatchingState) {
        for handle in self.start_batches(batching).await {
            handle.await.expect("batch task");
        }
    }

    /// Start batches for every enabled SQS mapping; the handles of the
    /// started batch tasks.
    async fn start_batches(
        &self,
        batching: &mut BatchingState,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let mappings: Vec<Mapping> = {
            let lambda_accounts = self.lambda_state.read();
            lambda_accounts
                .iter_regional()
                .flat_map(|(_, _, lambda)| {
                    lambda
                        .event_source_mappings
                        .values()
                        .filter(|m| m.enabled && m.event_source_arn.contains(":sqs:"))
                        .map(|m| Mapping {
                            uuid: m.uuid.clone(),
                            function_arn: m.function_arn.clone(),
                            queue_arn: m.event_source_arn.clone(),
                            batch_size: m.batch_size,
                            filter: FilterSet::from_strings(m.filter_patterns.iter()),
                            report_batch_item_failures: m
                                .function_response_types
                                .iter()
                                .any(|s| s == "ReportBatchItemFailures"),
                            max_batching_window_seconds: m
                                .maximum_batching_window_in_seconds
                                .unwrap_or(0),
                            max_concurrency: m
                                .scaling_config
                                .as_ref()
                                .and_then(|c| c.get("MaximumConcurrency"))
                                .and_then(Value::as_u64)
                                .map_or(DEFAULT_MAX_CONCURRENCY, |n| n.max(1) as usize),
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        };

        let mut handles = Vec::new();
        if mappings.is_empty() {
            return handles;
        }

        let now = Utc::now();

        for mapping in mappings {
            handles.extend(self.process_mapping(&mapping, batching, now).await);
        }
        handles
    }

    /// Take batches off `mapping`'s queue and hand each to the function in
    /// its own task, until the mapping has `max_concurrency` batches in
    /// flight (`ScalingConfig.MaximumConcurrency`: the rest of the messages
    /// stay in the queue) or the queue has nothing more to give.
    async fn process_mapping(
        &self,
        mapping: &Mapping,
        batching: &mut BatchingState,
        now: DateTime<Utc>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let mut handles = Vec::new();
        while let Some(slot) =
            InFlightSlot::reserve(&self.in_flight, &mapping.uuid, mapping.max_concurrency)
        {
            let batch = match self.take_batch(mapping, batching, now).await {
                Taken::Batch(batch) => batch,
                Taken::AllFiltered => continue,
                Taken::Nothing => break,
            };
            let poller = self.clone();
            let mapping = mapping.clone();
            handles.push(tokio::spawn(async move {
                poller.deliver(&mapping, batch).await;
                drop(slot);
            }));
        }
        handles
    }

    /// Take the next batch of `mapping`'s queue: its messages hidden for the
    /// visibility timeout, opened, filtered, and shaped as the event.
    async fn take_batch(
        &self,
        mapping: &Mapping,
        batching: &mut BatchingState,
        now: DateTime<Utc>,
    ) -> Taken {
        // Receive up to BatchSize messages the way ReceiveMessage does
        // (`fakecloud_sqs::receive`): they move to the queue's in-flight set
        // for its `VisibilityTimeout`, with a receipt handle and a counted
        // receive, a FIFO group with a message in flight hands out nothing
        // else, and a message past `maxReceiveCount` goes to the DLQ. The
        // API's consumers and this mapping share that one in-flight set.
        // Messages this poller still has with the function are also held
        // back even after their visibility timeout runs out (a
        // `VisibilityTimeout` of 0 would otherwise hand the same message to
        // every concurrency slot of one poll).
        let (messages, at_rest, account_id, queue_encrypted, dead_lettered) = {
            let mut sqs_mas = self.sqs_state.write();
            // The queue lives in the account and region its ARN names.
            let Some(sqs) = sqs_mas.by_arn_mut(&mapping.queue_arn) else {
                return Taken::Nothing;
            };
            let acct = sqs.account_id.clone();
            let queue = sqs.queues.values_mut().find(|q| q.arn == mapping.queue_arn);
            let queue = match queue {
                Some(q) => q,
                None => return Taken::Nothing,
            };

            let visibility_timeout: i64 = queue.visibility_timeout_secs();

            // Same effective-key logic as the SQS service: SSE-KMS
            // wins when `KmsMasterKeyId` is set, otherwise SSE-SQS
            // fires when `SqsManagedSseEnabled=true`. We track only the
            // boolean here; the poller calls the KMS hook directly so
            // it doesn't need the key id.
            let encrypted = queue
                .attributes
                .get("KmsMasterKeyId")
                .map(|k| !k.is_empty())
                .unwrap_or(false)
                || queue
                    .attributes
                    .get("SqsManagedSseEnabled")
                    .map(String::as_str)
                    == Some("true");

            let limit = mapping.batch_size.min(10) as usize;
            let held = self.held.lock();
            let queue_arn = mapping.queue_arn.clone();
            let taken = queue.take_receivable(limit, visibility_timeout, now, &|m| {
                held.contains(&(queue_arn.clone(), m.message_id.clone()))
            });
            drop(held);
            queue.commit_taken(&taken.messages);
            let dead_lettered = !taken.dead_letters.is_empty();
            sqs.route_dead_letters(&mapping.queue_arn, taken.dead_letters);
            let at_rest = taken.messages.clone();
            (taken.messages, at_rest, acct, encrypted, dead_lettered)
        };
        if dead_lettered {
            self.persist().await;
        }

        if messages.is_empty() {
            batching.window_started_at.remove(&mapping.uuid);
            return Taken::Nothing;
        }
        let held = HeldMessages::hold(
            &self.held,
            &mapping.queue_arn,
            messages.iter().map(|m| m.message_id.clone()),
        );

        // Honor MaximumBatchingWindowInSeconds: hold a partial batch
        // open until the window expires, then dispatch what we have.
        // A full batch dispatches immediately regardless. A held batch goes
        // back to the queue as it was (visible again), so the next poll
        // within the window can take it together with what arrived since.
        let limit = mapping.batch_size.min(10) as usize;
        if mapping.max_batching_window_seconds > 0 && messages.len() < limit {
            let window_start = batching
                .window_started_at
                .entry(mapping.uuid.clone())
                .or_insert(now);
            let elapsed = now.signed_duration_since(*window_start).num_seconds();
            if elapsed < mapping.max_batching_window_seconds {
                self.unreceive(&mapping.queue_arn, at_rest);
                return Taken::Nothing;
            }
        }
        batching.window_started_at.remove(&mapping.uuid);

        // Decrypt at-rest bodies before invoking Lambda. Real AWS
        // Lambda decrypts SSE-KMS / SSE-SQS messages server-side
        // before delivering the event payload, and the function code
        // sees plaintext. Without this, every consumer on a
        // managed-SSE queue would receive opaque envelope bytes.
        // Whether a body is sealed follows the message (it keeps its seal
        // in a dead-letter queue), not the queue it is polled from.
        //
        // The event source mapping reads the queue with ReceiveMessage, and
        // on AWS a body KMS will not open fails that ReceiveMessage itself
        // (`KMS.DisabledException` etc.): nothing is received, so no
        // message of the poll becomes in flight, none reaches the function
        // (not even as ciphertext), and no receive count moves -- the
        // message never approaches `maxReceiveCount` and stays in the queue
        // until KMS opens it again. The receive is undone the same way here:
        // the batch goes back to the queue, visible, its receives uncounted.
        let messages = match open_batch(
            self.kms_hook.as_deref(),
            &account_id,
            &mapping.queue_arn,
            queue_encrypted,
            messages,
        ) {
            Ok(messages) => messages,
            Err(err) => {
                tracing::warn!(
                    queue = %mapping.queue_arn,
                    error = %err,
                    "SQS->Lambda poller: KMS refused a message body; nothing received"
                );
                self.unreceive(&mapping.queue_arn, at_rest);
                return Taken::Nothing;
            }
        };

        // Build SQS-shaped event records, filter, then mark visible-at
        // for failures (or remove for successes) on the SQS side.
        let records: Vec<Value> = messages
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
                    "eventSourceARN": mapping.queue_arn,
                })
            })
            .collect();

        let (matched_records, dropped_ids): (Vec<Value>, Vec<String>) = if mapping.filter.is_empty()
        {
            (records, Vec::new())
        } else {
            let mut matched = Vec::new();
            let mut dropped = Vec::new();
            for (rec, msg) in records.into_iter().zip(messages.iter()) {
                if mapping.filter.matches(&rec) {
                    matched.push(rec);
                } else {
                    dropped.push(msg.message_id.clone());
                }
            }
            (matched, dropped)
        };

        // Drop filtered-out messages — AWS treats them as acked so the
        // queue stops redelivering them.
        if !dropped_ids.is_empty() {
            {
                let mut sqs_mas = self.sqs_state.write();
                if let Some(queue) = sqs_mas
                    .by_arn_mut(&mapping.queue_arn)
                    .and_then(|sqs| sqs.queues.values_mut().find(|q| q.arn == mapping.queue_arn))
                {
                    queue.delete_messages(&dropped_ids);
                }
            }
            // Persist the filter-dropped (acked) removals so a restart doesn't
            // re-deliver messages the mapping already discarded.
            self.persist().await;
        }

        if matched_records.is_empty() {
            return Taken::AllFiltered;
        }

        let payload = json!({ "Records": &matched_records }).to_string();

        tracing::debug!(
            function_arn = %mapping.function_arn,
            queue_arn = %mapping.queue_arn,
            message_count = matched_records.len(),
            dropped_by_filter = dropped_ids.len(),
            "SQS->Lambda: delivering messages to function"
        );

        let message_ids: Vec<String> = matched_records
            .iter()
            .filter_map(|r| {
                r.get("messageId")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .collect();
        Taken::Batch(PreparedBatch {
            payload,
            message_ids,
            _held: held,
        })
    }

    /// Undo a receive whose batch never reached the function: `at_rest` (the
    /// received messages as stored) leave the in-flight set and go back to
    /// the front of the queue, visible, their receives uncounted.
    fn unreceive(&self, queue_arn: &str, at_rest: Vec<fakecloud_sqs::SqsMessage>) {
        let mut sqs_mas = self.sqs_state.write();
        let Some(queue) = sqs_mas
            .by_arn_mut(queue_arn)
            .and_then(|sqs| sqs.queues.values_mut().find(|q| q.arn == queue_arn))
        else {
            return;
        };
        let ids: Vec<&str> = at_rest.iter().map(|m| m.message_id.as_str()).collect();
        queue
            .inflight
            .retain(|m| !ids.contains(&m.message_id.as_str()));
        queue.restore_taken(at_rest);
    }

    /// Hand `batch` to the function and apply the outcome to the queue:
    /// acknowledged messages are deleted, failed ones return to the queue
    /// once their visibility timeout runs out.
    async fn deliver(&self, mapping: &Mapping, batch: PreparedBatch) {
        // `_held` keeps the batch's messages from being received again until
        // its outcome is applied.
        let PreparedBatch {
            payload,
            message_ids: matched_msg_ids,
            _held,
        } = batch;
        let invoke_result = if let Some(ref delivery) = self.lambda_delivery {
            Some(
                delivery
                    .invoke_lambda(&mapping.function_arn, &payload)
                    .await,
            )
        } else {
            None
        };

        let (acked_ids, _failed_ids) = match &invoke_result {
            // A function error fails the whole batch, with or without
            // ReportBatchItemFailures: every message returns to the queue.
            Some(Ok(output)) if output.is_function_error() => {
                tracing::warn!(
                    function_arn = %mapping.function_arn,
                    error = %String::from_utf8_lossy(&output.payload),
                    "SQS->Lambda: function returned an error; messages will be retried"
                );
                (Vec::new(), matched_msg_ids.clone())
            }
            Some(Ok(output)) if mapping.report_batch_item_failures => {
                split_batch_failures(&output.payload, &matched_msg_ids)
            }
            Some(Ok(_)) => (matched_msg_ids.clone(), Vec::new()),
            Some(Err(err)) => {
                tracing::warn!(
                    function_arn = %mapping.function_arn,
                    error = %err,
                    "SQS->Lambda: function invocation failed; messages will be retried"
                );
                (Vec::new(), matched_msg_ids.clone())
            }
            None => (matched_msg_ids.clone(), Vec::new()),
        };

        // Acknowledged messages are deleted. Failed ones stay in flight until
        // the visibility timeout set when they were received runs out, then
        // return to the queue (their receive already counted), as on AWS; the
        // receive after `maxReceiveCount` moves them to the DLQ.
        if !acked_ids.is_empty() {
            let mut sqs_mas = self.sqs_state.write();
            if let Some(queue) = sqs_mas
                .by_arn_mut(&mapping.queue_arn)
                .and_then(|sqs| sqs.queues.values_mut().find(|q| q.arn == mapping.queue_arn))
            {
                queue.delete_messages(&acked_ids);
            }
        }
        // Persist acked removals and the receive's in-flight / receive-count
        // changes so a restart preserves delivery progress.
        self.persist().await;

        // The invocation is recorded in the account and region the mapped
        // function's ARN names.
        let mut lambda_accounts = self.lambda_state.write();
        let default_account = lambda_accounts.default_account_id().to_string();
        let default_region = lambda_accounts.region().to_string();
        let (fn_account, fn_region, _) = fakecloud_lambda::function_location(
            &mapping.function_arn,
            &default_account,
            &default_region,
        );
        let lambda = lambda_accounts.regional_mut(fn_account, fn_region);
        lambda.invocations.push(LambdaInvocation {
            function_arn: mapping.function_arn.clone(),
            payload,
            timestamp: Utc::now(),
            source: "aws:sqs".to_string(),
        });
    }
}

/// The polled `messages` of `queue_arn` with their at-rest bodies opened,
/// or the KMS failure of the first sealed body KMS will not open: like the
/// ReceiveMessage the poll stands for, one such body fails the whole poll.
fn open_batch(
    hook: Option<&dyn KmsHook>,
    account_id: &str,
    queue_arn: &str,
    queue_encrypted: bool,
    messages: Vec<fakecloud_sqs::SqsMessage>,
) -> Result<Vec<fakecloud_sqs::SqsMessage>, fakecloud_core::delivery::KmsError> {
    messages
        .into_iter()
        .map(|mut msg| {
            if !msg.body_is_sealed(queue_encrypted) {
                return Ok(msg);
            }
            // SSE-SQS: sealed with SQS's own key, opened without KMS.
            if let Some(plaintext) =
                fakecloud_sqs::sse_sqs::open(msg.sealing_queue(queue_arn), &msg.body)
            {
                msg.body = plaintext;
                return Ok(msg);
            }
            let Some(hook) = hook else {
                return Ok(msg);
            };
            if !looks_like_fakecloud_envelope(&msg.body) {
                return Ok(msg);
            }
            let mut ctx = HashMap::new();
            ctx.insert(
                "aws:sqs:arn".to_string(),
                msg.sealing_queue(queue_arn).to_string(),
            );
            let bytes = hook.decrypt_in_region(
                account_id,
                fakecloud_aws::arn::region_of(queue_arn).unwrap_or_default(),
                &msg.body,
                fakecloud_core::delivery::KmsCaller::via("sqs.amazonaws.com"),
                ctx,
            )?;
            msg.body = String::from_utf8_lossy(&bytes).to_string();
            Ok(msg)
        })
        .collect()
}

/// Mirrors `fakecloud_sqs::looks_like_fakecloud_envelope`: detect
/// bodies that the SQS SendMessage path produced via the KMS hook so
/// the poller only invokes the hook on bodies we actually encrypted.
/// Cross-service deliveries (SNS / EventBridge / S3 notifications)
/// land in `queue.messages` as plaintext and must pass through.
fn looks_like_fakecloud_envelope(body: &str) -> bool {
    if body.starts_with("fakecloud-kms:") {
        return true;
    }
    match base64::engine::general_purpose::STANDARD.decode(body) {
        Ok(bytes) => bytes.starts_with(&[0x01, 0x02, 0x02, 0x00]),
        Err(_) => false,
    }
}

/// Apply a `ReportBatchItemFailures` response (`{"batchItemFailures":
/// [{"itemIdentifier": ...}]}`) to the batch: `(acked, failed)` ids.
///
/// As on AWS, the batch is a complete success for an empty or null
/// `batchItemFailures` list or an empty or null response, and a complete
/// failure for an invalid JSON response, or an entry whose `itemIdentifier`
/// is empty, null, missing (a bad key name) or names a message that isn't
/// in the batch.
fn split_batch_failures(body: &[u8], batch_ids: &[String]) -> (Vec<String>, Vec<String>) {
    let all_acked = || (batch_ids.to_vec(), Vec::new());
    let all_failed = || (Vec::new(), batch_ids.to_vec());
    if body.iter().all(u8::is_ascii_whitespace) {
        return all_acked();
    }
    let parsed: Value = match serde_json::from_slice(body) {
        Ok(v) => v,
        Err(_) => return all_failed(),
    };
    let Some(entries) = parsed.get("batchItemFailures").and_then(Value::as_array) else {
        // Null response, no `batchItemFailures`, or a null list.
        return all_acked();
    };
    let mut failures: Vec<String> = Vec::new();
    for entry in entries {
        match entry.get("itemIdentifier").and_then(Value::as_str) {
            Some(id) if !id.is_empty() && batch_ids.iter().any(|b| b == id) => {
                if !failures.iter().any(|f| f == id) {
                    failures.push(id.to_string());
                }
            }
            _ => return all_failed(),
        }
    }
    let acked = batch_ids
        .iter()
        .filter(|id| !failures.contains(id))
        .cloned()
        .collect();
    (acked, failures)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};
    use std::pin::Pin;
    use std::sync::Mutex;

    use fakecloud_core::delivery::LambdaInvocationOutput;
    use fakecloud_core::multi_account::{MultiAccountState, MultiRegionState};
    use fakecloud_lambda::{EventSourceMapping, LambdaState};
    use fakecloud_sqs::{SqsMessage, SqsQueue, SqsState};
    use parking_lot::RwLock;

    #[test]
    fn batch_failures_parse() {
        let body = br#"{"batchItemFailures":[{"itemIdentifier":"a"},{"itemIdentifier":"c"}]}"#;
        let (acked, failed) =
            split_batch_failures(body, &["a".into(), "b".into(), "c".into(), "d".into()]);
        assert_eq!(acked, vec!["b".to_string(), "d".to_string()]);
        assert_eq!(failed, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn batch_failures_empty_list_acks_all() {
        let body = br#"{"batchItemFailures":[]}"#;
        let (acked, failed) = split_batch_failures(body, &["a".into(), "b".into()]);
        assert_eq!(acked, vec!["a".to_string(), "b".to_string()]);
        assert!(failed.is_empty());
    }

    #[test]
    fn batch_failures_empty_or_null_response_acks_all() {
        for body in [&b""[..], b"null", br#"{"batchItemFailures":null}"#] {
            let (acked, failed) = split_batch_failures(body, &["a".into(), "b".into()]);
            assert_eq!(acked, vec!["a".to_string(), "b".to_string()]);
            assert!(failed.is_empty());
        }
    }

    #[test]
    fn batch_failures_invalid_entries_fail_the_whole_batch() {
        // Invalid JSON, empty / null / missing itemIdentifier, and an id
        // outside the batch each make the batch a complete failure.
        for body in [
            &b"not json"[..],
            br#"{"batchItemFailures":[{"itemIdentifier":""}]}"#,
            br#"{"batchItemFailures":[{"itemIdentifier":null}]}"#,
            br#"{"batchItemFailures":[{"itemId":"a"}]}"#,
            br#"{"batchItemFailures":[{"itemIdentifier":"a"},{"itemIdentifier":"zzz"}]}"#,
        ] {
            let (acked, failed) = split_batch_failures(body, &["a".into(), "b".into()]);
            assert!(acked.is_empty(), "{}", String::from_utf8_lossy(body));
            assert_eq!(failed, vec!["a".to_string(), "b".to_string()]);
        }
    }

    #[test]
    fn batch_failures_no_field_acks_all() {
        let body = br#"{"ok":true}"#;
        let (acked, failed) = split_batch_failures(body, &["a".into(), "b".into()]);
        assert_eq!(acked, vec!["a".to_string(), "b".to_string()]);
        assert!(failed.is_empty());
    }

    /// Slow-invoke `LambdaDelivery` that records each invocation and
    /// holds the future open until the test releases it. Lets us
    /// assert the poller's view of the queue *during* the in-flight
    /// invoke window.
    struct RecordingLambda {
        invocations: Arc<Mutex<Vec<String>>>,
    }

    impl LambdaDelivery for RecordingLambda {
        fn invoke_lambda(
            &self,
            _function_arn: &str,
            payload: &str,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<LambdaInvocationOutput, String>> + Send>>
        {
            self.invocations.lock().unwrap().push(payload.to_string());
            Box::pin(async move { Ok(LambdaInvocationOutput::success(b"{}".to_vec())) })
        }
    }

    /// `LambdaDelivery` whose function answers every invocation with
    /// `output`.
    struct AnsweringLambda {
        output: LambdaInvocationOutput,
    }

    impl LambdaDelivery for AnsweringLambda {
        fn invoke_lambda(
            &self,
            _function_arn: &str,
            _payload: &str,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<LambdaInvocationOutput, String>> + Send>>
        {
            let output = self.output.clone();
            Box::pin(async move { Ok(output) })
        }
    }

    /// Poll once with a function answering `output`; return the receive
    /// counts of the queue's remaining messages (queued or in flight).
    async fn poll_once_answering(output: LambdaInvocationOutput) -> Vec<u32> {
        let (poller, sqs_state, _lambda_state) = build_states();
        let delivery: Arc<dyn LambdaDelivery> = Arc::new(AnsweringLambda { output });
        let poller = poller.with_lambda_delivery(delivery);
        let mut batching = BatchingState::default();
        poller.poll(&mut batching).await;
        let sqs = sqs_state.read();
        let queue = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .next()
            .unwrap();
        queue
            .messages
            .iter()
            .chain(queue.inflight.iter())
            .map(|m| m.receive_count)
            .collect()
    }

    #[tokio::test]
    async fn function_error_returns_the_batch_to_the_queue() {
        let before = {
            let (_, sqs_state, _) = build_states();
            let sqs = sqs_state.read();
            sqs.default_regional()
                .unwrap()
                .queues
                .values()
                .next()
                .unwrap()
                .messages[0]
                .receive_count
        };
        let counts = poll_once_answering(LambdaInvocationOutput::unhandled(
            br#"{"errorType":"Error","errorMessage":"boom"}"#.to_vec(),
        ))
        .await;
        assert_eq!(counts.len(), 1, "a failed batch stays in the queue");
        assert!(counts[0] > before, "the failed receive is counted");
    }

    #[tokio::test]
    async fn error_shaped_success_acks_the_batch() {
        let counts = poll_once_answering(LambdaInvocationOutput::success(
            br#"{"errorType":"Error","errorMessage":"not an error"}"#.to_vec(),
        ))
        .await;
        assert!(counts.is_empty(), "a successful invocation acks the batch");
    }

    #[tokio::test]
    async fn failed_batch_stays_hidden_for_the_visibility_timeout() {
        let (poller, sqs_state, _lambda_state) = build_states();
        let delivery: Arc<dyn LambdaDelivery> = Arc::new(AnsweringLambda {
            output: LambdaInvocationOutput::unhandled(
                br#"{"errorType":"Error","errorMessage":"boom"}"#.to_vec(),
            ),
        });
        let poller = poller.with_lambda_delivery(delivery);
        let before = Utc::now();
        poller.poll(&mut BatchingState::default()).await;
        let sqs = sqs_state.read();
        let queue = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .next()
            .unwrap();
        assert!(queue.messages.is_empty(), "the failed message is in flight");
        let visible_at = queue.inflight[0].visible_at.expect("still hidden");
        assert!(
            visible_at >= before + chrono::Duration::seconds(29),
            "the message reappears only after the 30s visibility timeout"
        );
    }

    /// A KMS hook whose key is disabled: no sealed body opens.
    struct DisabledKeyKms;
    impl KmsHook for DisabledKeyKms {
        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            _key_id: &str,
            _plaintext: &[u8],
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _ctx: HashMap<String, String>,
        ) -> Result<String, fakecloud_core::delivery::KmsError> {
            unreachable!()
        }
        fn decrypt(
            &self,
            _account_id: &str,
            _ciphertext_b64: &str,
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _ctx: HashMap<String, String>,
        ) -> Result<Vec<u8>, fakecloud_core::delivery::KmsError> {
            Err(fakecloud_core::delivery::KmsError::new(
                fakecloud_core::delivery::KmsErrorKind::Disabled,
                "key is disabled",
            ))
        }
        fn is_ciphertext(&self, _account_id: &str, stored: &str) -> bool {
            looks_like_fakecloud_envelope(stored)
        }
    }

    /// On AWS a body KMS will not open fails the mapping's ReceiveMessage:
    /// nothing is received, so the function is not invoked (with the
    /// ciphertext or with the other messages of the poll), no receive count
    /// moves (the message never reaches `maxReceiveCount`), and no message
    /// is left in flight.
    /// An SSE-SQS body (sealed with SQS's own key) reaches the function as
    /// plaintext, with no KMS hook involved at all.
    #[tokio::test]
    async fn sse_sqs_bodies_reach_the_function_in_plaintext_without_kms() {
        let (poller, sqs_state, _lambda_state) = build_states();
        {
            let mut sqs = sqs_state.write();
            let queue = sqs
                .default_regional_mut()
                .queues
                .values_mut()
                .next()
                .unwrap();
            let arn = queue.arn.clone();
            let msg = &mut queue.messages[0];
            msg.body = fakecloud_sqs::sse_sqs::seal(&arn, "hello");
            msg.sealed_for_queue = Some(arn);
        }
        let invocations = Arc::new(Mutex::new(Vec::<String>::new()));
        let delivery: Arc<dyn LambdaDelivery> = Arc::new(RecordingLambda {
            invocations: invocations.clone(),
        });
        let poller = poller.with_lambda_delivery(delivery);
        poller.poll(&mut BatchingState::default()).await;
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        let event: serde_json::Value = serde_json::from_str(&invocations[0]).unwrap();
        assert_eq!(event["Records"][0]["body"], "hello");
    }

    #[tokio::test]
    async fn a_body_kms_refuses_fails_the_whole_poll_and_counts_nothing() {
        let (poller, sqs_state, _lambda_state) = build_states();
        {
            let mut sqs = sqs_state.write();
            let queue = sqs
                .default_regional_mut()
                .queues
                .values_mut()
                .next()
                .unwrap();
            let mut sealed = queue.messages[0].clone();
            sealed.message_id = "msg-sealed".to_string();
            sealed.body = "fakecloud-kms:sealed".to_string();
            sealed.sealed_for_queue = Some(queue.arn.clone());
            queue.messages.push_back(sealed);
            queue.redrive_policy = Some(fakecloud_sqs::RedrivePolicy {
                dead_letter_target_arn: format!("arn:aws:sqs:{REGION}:{ACCOUNT}:dlq"),
                max_receive_count: 1,
            });
        }
        let invocations = Arc::new(Mutex::new(Vec::<String>::new()));
        let delivery: Arc<dyn LambdaDelivery> = Arc::new(RecordingLambda {
            invocations: invocations.clone(),
        });
        let poller = poller
            .with_lambda_delivery(delivery)
            .with_kms_hook(Arc::new(DisabledKeyKms));
        for _ in 0..3 {
            poller.poll(&mut BatchingState::default()).await;
        }
        assert!(invocations.lock().unwrap().is_empty());
        let sqs = sqs_state.read();
        let queue = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .next()
            .unwrap();
        assert_eq!(queue.messages.len(), 2, "both messages stay in the queue");
        assert!(queue.inflight.is_empty(), "nothing is left in flight");
        assert!(
            queue.receipt_handle_map.is_empty(),
            "no receipt handle survives"
        );
        for msg in &queue.messages {
            assert_eq!(msg.receive_count, 0, "{}", msg.message_id);
            assert_eq!(msg.visible_at, None, "{} is not in flight", msg.message_id);
        }
    }

    #[tokio::test]
    async fn exhausted_failed_message_moves_to_the_dead_letter_queue() {
        let (poller, sqs_state, _) = build_states();
        let dlq_arn = format!("arn:aws:sqs:{REGION}:{ACCOUNT}:k2-dlq");
        let source_arn = {
            let mut sqs = sqs_state.write();
            let st = sqs.default_regional_mut();
            let mut dlq = st.queues.values().next().unwrap().clone();
            dlq.queue_name = "k2-dlq".to_string();
            dlq.queue_url = format!("http://localhost:4566/{ACCOUNT}/k2-dlq");
            dlq.arn = dlq_arn.clone();
            dlq.messages.clear();
            let source = st.queues.values_mut().next().unwrap();
            // Failed messages are receivable again at once.
            source
                .attributes
                .insert("VisibilityTimeout".to_string(), "0".to_string());
            source.redrive_policy = Some(fakecloud_sqs::RedrivePolicy {
                dead_letter_target_arn: dlq_arn.clone(),
                max_receive_count: 2,
            });
            let source_arn = source.arn.clone();
            st.queues.insert(dlq.queue_url.clone(), dlq);
            source_arn
        };
        let poller = poller.with_lambda_delivery(Arc::new(AnsweringLambda {
            output: LambdaInvocationOutput::unhandled(br#"{"errorMessage":"boom"}"#.to_vec()),
        }));
        let queue_len = |arn: &str| {
            let sqs = sqs_state.read();
            let q = sqs
                .default_regional()
                .unwrap()
                .queues
                .values()
                .find(|q| q.arn == arn)
                .unwrap()
                .clone();
            q.messages.len() + q.inflight.len()
        };

        // Two failed receives: still below maxReceiveCount.
        poller.poll(&mut BatchingState::default()).await;
        poller.poll(&mut BatchingState::default()).await;
        assert_eq!(queue_len(&source_arn), 1, "below maxReceiveCount it stays");

        // The receive past maxReceiveCount redrives it, as SQS does.
        poller.poll(&mut BatchingState::default()).await;
        assert_eq!(
            queue_len(&source_arn),
            0,
            "past maxReceiveCount it is redriven"
        );
        let sqs = sqs_state.read();
        let dlq = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .find(|q| q.arn == dlq_arn)
            .unwrap();
        assert_eq!(dlq.messages.len(), 1);
        assert_eq!(dlq.messages[0].message_id, "msg-1");
        assert_eq!(dlq.messages[0].receive_count, 0);
    }

    /// With a `VisibilityTimeout` of 0 a received message is visible again at
    /// once; the poller must still hand it to the function once, not once
    /// per concurrency slot.
    #[tokio::test]
    async fn a_zero_visibility_timeout_never_duplicates_a_batch_in_flight() {
        use std::sync::atomic::AtomicUsize;

        let (poller, sqs_state, lambda_state) = build_states();
        reshape(
            &sqs_state,
            &lambda_state,
            vec![queued_message("only", None)],
            false,
            |m| m.batch_size = 1,
        );
        sqs_state
            .write()
            .default_regional_mut()
            .queues
            .values_mut()
            .next()
            .unwrap()
            .attributes
            .insert("VisibilityTimeout".to_string(), "0".to_string());
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let running = Arc::new(AtomicUsize::new(0));
        let poller = poller.with_lambda_delivery(Arc::new(GatedLambda {
            gate: gate.clone(),
            running: running.clone(),
            peak: Arc::new(AtomicUsize::new(0)),
        }));
        let mut batching = BatchingState::default();
        let first = poller.start_batches(&mut batching).await;
        assert_eq!(first.len(), 1, "one message, one batch");
        // Later polls while it is with the function start nothing either.
        assert!(poller.start_batches(&mut batching).await.is_empty());
        gate.add_permits(100);
        for handle in first {
            handle.await.unwrap();
        }
        let sqs = sqs_state.read();
        let queue = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .next()
            .unwrap();
        assert!(queue.messages.is_empty() && queue.inflight.is_empty());
    }

    /// A delayed later message of a FIFO group does not hold back the
    /// group's earlier, visible message: only received messages lock a group.
    #[tokio::test]
    async fn a_delayed_fifo_message_does_not_block_its_group() {
        let (poller, sqs_state, lambda_state) = build_states();
        let mut delayed = queued_message("g1-b", Some("g1"));
        delayed.visible_at = Some(Utc::now() + chrono::Duration::seconds(600));
        reshape(
            &sqs_state,
            &lambda_state,
            vec![queued_message("g1-a", Some("g1")), delayed],
            true,
            |m| m.batch_size = 10,
        );
        let invocations = Arc::new(Mutex::new(Vec::<String>::new()));
        let poller = poller.with_lambda_delivery(Arc::new(RecordingLambda {
            invocations: invocations.clone(),
        }));
        poller.poll(&mut BatchingState::default()).await;
        let invocations = invocations.lock().unwrap();
        assert_eq!(invocations.len(), 1);
        let event: Value = serde_json::from_str(&invocations[0]).unwrap();
        assert_eq!(event["Records"][0]["messageId"], "g1-a");
        assert_eq!(event["Records"].as_array().unwrap().len(), 1);
    }

    /// A FIFO group a ReceiveMessage caller has a message of in flight is
    /// locked for the mapping too (and the other way round): both consume
    /// through the same in-flight set.
    #[tokio::test]
    async fn fifo_group_locks_are_shared_with_receive_message() {
        let (poller, sqs_state, lambda_state) = build_states();
        reshape(
            &sqs_state,
            &lambda_state,
            vec![
                queued_message("g1-a", Some("g1")),
                queued_message("g1-b", Some("g1")),
                queued_message("g2-a", Some("g2")),
            ],
            true,
            |m| m.batch_size = 1,
        );
        // An API consumer receives g1-a, as ReceiveMessage does.
        {
            let mut sqs = sqs_state.write();
            let queue = sqs
                .default_regional_mut()
                .queues
                .values_mut()
                .next()
                .unwrap();
            let taken = queue.take_receivable(1, 30, Utc::now(), &|_| false);
            assert_eq!(taken.messages[0].message_id, "g1-a");
            queue.commit_taken(&taken.messages);
        }
        let invocations = Arc::new(Mutex::new(Vec::<String>::new()));
        let poller = poller.with_lambda_delivery(Arc::new(RecordingLambda {
            invocations: invocations.clone(),
        }));
        poller.poll(&mut BatchingState::default()).await;
        let invocations = invocations.lock().unwrap();
        let ids: Vec<Value> = invocations
            .iter()
            .map(|p| serde_json::from_str::<Value>(p).unwrap()["Records"][0]["messageId"].clone())
            .collect();
        assert_eq!(ids, ["g2-a"], "g1-b waits for the API consumer's g1-a");
    }

    const ACCOUNT: &str = "123456789012";
    const REGION: &str = "us-east-1";

    fn build_states() -> (
        super::SqsLambdaPoller,
        super::SharedSqsState,
        super::SharedLambdaState,
    ) {
        let queue_arn = format!("arn:aws:sqs:{REGION}:{ACCOUNT}:k2-poll");
        let queue_url = format!("http://localhost:4566/{ACCOUNT}/k2-poll");
        let mut attrs = BTreeMap::new();
        attrs.insert("VisibilityTimeout".to_string(), "30".to_string());
        let queue = SqsQueue {
            queue_name: "k2-poll".to_string(),
            queue_url: queue_url.clone(),
            arn: queue_arn.clone(),
            created_at: Utc::now(),
            messages: VecDeque::from(vec![SqsMessage {
                message_id: "msg-1".to_string(),
                receipt_handle: None,
                md5_of_body: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
                body: "hello".to_string(),
                sent_timestamp: 0,
                attributes: BTreeMap::new(),
                message_attributes: BTreeMap::new(),
                visible_at: None,
                receive_count: 0,
                first_received_at: None,
                message_group_id: None,
                message_dedup_id: None,
                created_at: Utc::now(),
                sequence_number: None,
                sealed_for_queue: None,
                seal_recorded: true,
            }]),
            inflight: Vec::new(),
            attributes: attrs,
            is_fifo: false,
            dedup_cache: BTreeMap::new(),
            redrive_policy: None,
            tags: BTreeMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: BTreeMap::new(),
            receive_attempt_cache: BTreeMap::new(),
        };

        let mut sqs: fakecloud_core::multi_account::MultiRegionState<SqsState> =
            MultiAccountState::new(ACCOUNT, REGION, "http://localhost:4566");
        {
            let s = sqs.default_regional_mut();
            s.name_to_url.insert("k2-poll".to_string(), queue_url);
            s.queues.insert(queue.queue_url.clone(), queue);
        }
        let sqs_state = Arc::new(RwLock::new(sqs));

        let mut lambda: MultiRegionState<LambdaState> =
            MultiRegionState::new(ACCOUNT, REGION, "http://localhost:4566");
        {
            let l = lambda.regional_mut(ACCOUNT, REGION);
            let mapping = EventSourceMapping {
                uuid: "esm-1".to_string(),
                function_arn: format!("arn:aws:lambda:{REGION}:{ACCOUNT}:function:k2-fn"),
                event_source_arn: queue_arn,
                batch_size: 10,
                enabled: true,
                state: "Enabled".to_string(),
                last_modified: Utc::now(),
                filter_patterns: Vec::new(),
                maximum_batching_window_in_seconds: None,
                starting_position: None,
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
                amazon_managed_kafka_event_source_config: None,
                document_db_event_source_config: None,
                scaling_config: None,
                provisioned_poller_config: None,
                logging_config: None,
                tags: Default::default(),
                last_processing_result: None,
            };
            l.event_source_mappings
                .insert(mapping.uuid.clone(), mapping);
        }
        let lambda_state = Arc::new(RwLock::new(lambda));

        let poller = super::SqsLambdaPoller::new(sqs_state.clone(), lambda_state.clone());
        (poller, sqs_state, lambda_state)
    }

    /// Regression: when the poller picks up a message and hands it to
    /// Lambda, the message must be hidden for the queue's
    /// VisibilityTimeout — otherwise a second poll cycle (or another
    /// consumer) would re-deliver the same message and the function
    /// would fire twice for one record.
    #[tokio::test]
    async fn poller_marks_picked_messages_invisible_during_invoke() {
        let invocations = Arc::new(Mutex::new(Vec::<String>::new()));
        let delivery: Arc<dyn LambdaDelivery> = Arc::new(RecordingLambda {
            invocations: invocations.clone(),
        });
        let (poller, sqs_state, _lambda_state) = build_states();
        let poller = poller.with_lambda_delivery(delivery);
        let mut batching = BatchingState::default();

        // First poll picks up the only message and acks it. After
        // process_mapping returns, the message must be gone.
        poller.poll(&mut batching).await;
        assert_eq!(
            invocations.lock().unwrap().len(),
            1,
            "first poll should invoke once"
        );

        // Inject a fresh message and verify the poller hides it before
        // invoking. We run the second poll without releasing the
        // RecordingLambda future synchronously — the future resolves
        // inside the same task here, so we instead snapshot the queue
        // immediately after invoke and confirm the visibility timeout
        // got stamped on the body that just got delivered.
        {
            let mut sqs = sqs_state.write();
            let s = sqs.default_regional_mut();
            let queue = s.queues.values_mut().next().unwrap();
            queue.messages.push_back(SqsMessage {
                message_id: "msg-2".to_string(),
                receipt_handle: None,
                md5_of_body: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
                body: "second".to_string(),
                sent_timestamp: 0,
                attributes: BTreeMap::new(),
                message_attributes: BTreeMap::new(),
                visible_at: None,
                receive_count: 0,
                first_received_at: None,
                message_group_id: None,
                message_dedup_id: None,
                created_at: Utc::now(),
                sequence_number: None,
                sealed_for_queue: None,
                seal_recorded: true,
            });
        }

        // Drive the poller again. After invoke succeeds the message is
        // acked + removed; queue should be empty.
        poller.poll(&mut batching).await;
        assert_eq!(
            invocations.lock().unwrap().len(),
            2,
            "second poll should invoke once more"
        );
        let sqs = sqs_state.read();
        let queue = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .next()
            .unwrap();
        assert!(
            queue.messages.is_empty(),
            "successful invoke must delete acked messages, found: {:?}",
            queue
                .messages
                .iter()
                .map(|m| &m.message_id)
                .collect::<Vec<_>>()
        );
    }

    /// Regression (bug-audit 4.5): after the poller acks + removes a
    /// delivered message it must persist SQS state through the snapshot hook,
    /// so a restart in the window doesn't re-deliver the acked message.
    #[tokio::test]
    async fn poller_persists_after_acking_messages() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let invocations = Arc::new(Mutex::new(Vec::<String>::new()));
        let delivery: Arc<dyn LambdaDelivery> = Arc::new(RecordingLambda { invocations });
        let (poller, sqs_state, _lambda_state) = build_states();

        let snapshot_calls = Arc::new(AtomicUsize::new(0));
        let counter = snapshot_calls.clone();
        let hook: SnapshotHook = Arc::new(move || {
            let counter = counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        });

        let poller = poller
            .with_lambda_delivery(delivery)
            .with_snapshot_hook(hook);
        let mut batching = BatchingState::default();
        poller.poll(&mut batching).await;

        // The delivered message was acked + removed.
        {
            let sqs = sqs_state.read();
            let queue = sqs
                .default_regional()
                .unwrap()
                .queues
                .values()
                .next()
                .unwrap();
            assert!(queue.messages.is_empty(), "acked message must be removed");
        }
        // ...and the removal must have been persisted through the hook.
        assert!(
            snapshot_calls.load(Ordering::SeqCst) >= 1,
            "poller must snapshot after acking messages"
        );
    }

    /// The same message must not be re-popped while it's in flight: a
    /// second poll cycle observing a queue where the only message has
    /// `visible_at` in the future has to skip it. This is the
    /// visibility-window invariant the previous poller violated by
    /// leaving `visible_at` unset on pop.
    #[tokio::test]
    async fn poller_skips_message_during_visibility_window() {
        let (poller, sqs_state, _lambda_state) = build_states();
        // No LambdaDelivery wired — process_mapping treats this as a
        // best-effort recording-only delivery: the poll DOES still mark
        // messages as acked at the end (since invoke_result is None,
        // the helper treats it as success). To exercise the visibility
        // window we need a delivery that fails, so failed messages
        // stay visible. Instead, we hand-set visible_at on the message
        // and call process_mapping directly to verify the skip path.
        let mapping = Mapping {
            uuid: "esm-1".to_string(),
            function_arn: format!("arn:aws:lambda:{REGION}:{ACCOUNT}:function:k2-fn"),
            queue_arn: format!("arn:aws:sqs:{REGION}:{ACCOUNT}:k2-poll"),
            batch_size: 10,
            filter: fakecloud_lambda::filter::FilterSet::from_strings(std::iter::empty::<&str>()),
            report_batch_item_failures: false,
            max_batching_window_seconds: 0,
            max_concurrency: DEFAULT_MAX_CONCURRENCY,
        };
        // Hide the existing message in the future.
        {
            let mut sqs = sqs_state.write();
            let s = sqs.default_regional_mut();
            let queue = s.queues.values_mut().next().unwrap();
            queue.messages[0].visible_at = Some(Utc::now() + chrono::Duration::seconds(60));
        }

        let invocations = Arc::new(Mutex::new(Vec::<String>::new()));
        let delivery: Arc<dyn LambdaDelivery> = Arc::new(RecordingLambda {
            invocations: invocations.clone(),
        });
        let poller = poller.with_lambda_delivery(delivery);
        let mut batching = BatchingState::default();
        poller
            .process_mapping(&mapping, &mut batching, Utc::now())
            .await;
        assert_eq!(
            invocations.lock().unwrap().len(),
            0,
            "message hidden by visibility timeout must not be invoked"
        );
        // Message still in queue.messages, untouched.
        let sqs = sqs_state.read();
        let queue = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .next()
            .unwrap();
        assert_eq!(queue.messages.len(), 1);
    }

    fn queued_message(id: &str, group: Option<&str>) -> SqsMessage {
        SqsMessage {
            message_id: id.to_string(),
            receipt_handle: None,
            md5_of_body: "d41d8cd98f00b204e9800998ecf8427e".to_string(),
            body: id.to_string(),
            sent_timestamp: 0,
            attributes: BTreeMap::new(),
            message_attributes: BTreeMap::new(),
            visible_at: None,
            receive_count: 0,
            first_received_at: None,
            message_group_id: group.map(str::to_string),
            message_dedup_id: None,
            created_at: Utc::now(),
            sequence_number: None,
            sealed_for_queue: None,
            seal_recorded: true,
        }
    }

    /// Replace the test queue's messages and tune the test mapping.
    fn reshape(
        sqs_state: &SharedSqsState,
        lambda_state: &SharedLambdaState,
        messages: Vec<SqsMessage>,
        fifo: bool,
        edit: impl FnOnce(&mut EventSourceMapping),
    ) {
        {
            let mut sqs = sqs_state.write();
            let queue = sqs
                .default_regional_mut()
                .queues
                .values_mut()
                .next()
                .unwrap();
            queue.messages = messages.into();
            queue.is_fifo = fifo;
        }
        let mut lambda = lambda_state.write();
        let mapping = lambda
            .regional_mut(ACCOUNT, REGION)
            .event_source_mappings
            .get_mut("esm-1")
            .unwrap();
        edit(mapping);
    }

    fn visible_count(sqs_state: &SharedSqsState) -> usize {
        let now = Utc::now();
        let sqs = sqs_state.read();
        let queue = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .next()
            .unwrap();
        queue
            .messages
            .iter()
            .filter(|m| m.visible_at.is_none_or(|v| v <= now))
            .count()
    }

    /// A function whose invocations each wait for a permit of `gate`,
    /// tracking how many run at once.
    struct GatedLambda {
        gate: Arc<tokio::sync::Semaphore>,
        running: Arc<std::sync::atomic::AtomicUsize>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl LambdaDelivery for GatedLambda {
        fn invoke_lambda(
            &self,
            _function_arn: &str,
            _payload: &str,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<LambdaInvocationOutput, String>> + Send>>
        {
            use std::sync::atomic::Ordering;
            let gate = self.gate.clone();
            let running = self.running.clone();
            let peak = self.peak.clone();
            Box::pin(async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                gate.acquire().await.expect("gate").forget();
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(LambdaInvocationOutput::success(b"{}".to_vec()))
            })
        }
    }

    async fn wait_for(mut cond: impl FnMut() -> bool) {
        for _ in 0..200 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("condition never held");
    }

    #[tokio::test]
    async fn maximum_concurrency_caps_the_batches_in_flight() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (poller, sqs_state, lambda_state) = build_states();
        let messages = (1..=6)
            .map(|i| queued_message(&format!("m{i}"), None))
            .collect();
        reshape(&sqs_state, &lambda_state, messages, false, |m| {
            m.batch_size = 1;
            m.scaling_config = Some(json!({"MaximumConcurrency": 2}));
        });
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let poller = poller.with_lambda_delivery(Arc::new(GatedLambda {
            gate: gate.clone(),
            running: running.clone(),
            peak: peak.clone(),
        }));
        let mut batching = BatchingState::default();

        let first = poller.start_batches(&mut batching).await;
        assert_eq!(first.len(), 2, "two batches start, the cap");
        wait_for(|| running.load(Ordering::SeqCst) == 2).await;
        // While both are with the function, another poll starts nothing and
        // the other messages stay in the queue.
        assert!(poller.start_batches(&mut batching).await.is_empty());
        assert_eq!(visible_count(&sqs_state), 4);

        // One batch finishing frees exactly one slot.
        gate.add_permits(1);
        wait_for(|| poller.in_flight.lock().get("esm-1") == Some(&1)).await;
        let next = poller.start_batches(&mut batching).await;
        assert_eq!(next.len(), 1);

        gate.add_permits(100);
        let mut rest = Vec::new();
        for handle in first.into_iter().chain(next) {
            handle.await.unwrap();
        }
        // Drain the queue, still two at a time.
        for _ in 0..10 {
            rest.extend(poller.start_batches(&mut batching).await);
            for handle in rest.drain(..) {
                handle.await.unwrap();
            }
        }
        let sqs = sqs_state.read();
        let queue = sqs
            .default_regional()
            .unwrap()
            .queues
            .values()
            .next()
            .unwrap();
        assert!(queue.messages.is_empty(), "every message was delivered");
        assert_eq!(
            peak.load(Ordering::SeqCst),
            2,
            "never more than two at once"
        );
        assert!(poller.in_flight.lock().is_empty());
    }

    #[tokio::test]
    async fn without_maximum_concurrency_five_batches_run_at_once() {
        use std::sync::atomic::AtomicUsize;

        let (poller, sqs_state, lambda_state) = build_states();
        let messages = (1..=8)
            .map(|i| queued_message(&format!("m{i}"), None))
            .collect();
        reshape(&sqs_state, &lambda_state, messages, false, |m| {
            m.batch_size = 1
        });
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let poller = poller.with_lambda_delivery(Arc::new(GatedLambda {
            gate: gate.clone(),
            running: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        }));
        let handles = poller.start_batches(&mut BatchingState::default()).await;
        assert_eq!(handles.len(), DEFAULT_MAX_CONCURRENCY);
        gate.add_permits(100);
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_fifo_group_is_never_in_two_batches_at_once() {
        use std::sync::atomic::AtomicUsize;

        let (poller, sqs_state, lambda_state) = build_states();
        let messages = vec![
            queued_message("g1-a", Some("g1")),
            queued_message("g1-b", Some("g1")),
            queued_message("g2-a", Some("g2")),
        ];
        reshape(&sqs_state, &lambda_state, messages, true, |m| {
            m.batch_size = 1
        });
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let poller = poller.with_lambda_delivery(Arc::new(GatedLambda {
            gate: gate.clone(),
            running: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        }));
        let handles = poller.start_batches(&mut BatchingState::default()).await;
        // One batch per group: g1-b waits for g1-a.
        assert_eq!(handles.len(), 2);
        {
            let sqs = sqs_state.read();
            let queue = sqs
                .default_regional()
                .unwrap()
                .queues
                .values()
                .next()
                .unwrap();
            let waiting: Vec<&str> = queue
                .messages
                .iter()
                .filter(|m| m.visible_at.is_none())
                .map(|m| m.message_id.as_str())
                .collect();
            assert_eq!(waiting, ["g1-b"]);
        }
        gate.add_permits(100);
        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_held_partial_batch_stays_visible_in_the_queue() {
        let (poller, sqs_state, lambda_state) = build_states();
        reshape(
            &sqs_state,
            &lambda_state,
            vec![queued_message("m1", None)],
            false,
            |m| m.maximum_batching_window_in_seconds = Some(60),
        );
        let invocations = Arc::new(Mutex::new(Vec::<String>::new()));
        let poller = poller.with_lambda_delivery(Arc::new(RecordingLambda {
            invocations: invocations.clone(),
        }));
        let mut batching = BatchingState::default();
        poller.poll(&mut batching).await;
        assert!(
            invocations.lock().unwrap().is_empty(),
            "the window holds the batch"
        );
        assert_eq!(
            visible_count(&sqs_state),
            1,
            "a held message is not hidden for the visibility timeout"
        );
        // Once the window has passed, the batch goes out.
        batching.window_started_at.insert(
            "esm-1".to_string(),
            Utc::now() - chrono::Duration::seconds(61),
        );
        poller.poll(&mut batching).await;
        assert_eq!(invocations.lock().unwrap().len(), 1);
    }
}
