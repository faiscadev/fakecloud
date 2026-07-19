//! Background runner that executes EventBridge Pipes.
//!
//! For each RUNNING pipe the runner polls its source, applies the pipe's
//! EventBridge-pattern filter (matching events are forwarded, non-matching
//! events are acked — "filter drop-as-ack", exactly as AWS Pipes does),
//! optionally runs the matched batch through an *enrichment* (a Lambda
//! round-trip whose JSON return replaces the batch), applies the target
//! `InputTemplate` transform per event, and delivers the result to the pipe's
//! target via the shared `DeliveryBus`, reusing the same cross-service
//! delivery paths every other fakecloud feature uses.
//!
//! Sources: SQS queues, Kinesis streams, and DynamoDB streams. SQS acks by
//! deleting the source message only after it is filtered out or successfully
//! delivered; the stream sources keep a durable per-pipe checkpoint (in the
//! Pipes state, so it survives a restart) and advance it only past records
//! that are filtered out or successfully delivered — a delivery failure
//! leaves the window for redelivery (at-least-once, like AWS).
//!
//! Targets: Lambda / SQS / SNS / Step Functions / EventBridge bus / Kinesis.
//! Other targets (ECS / Batch / Redshift / HTTP / …) and non-Lambda
//! enrichment land in a later batch; an unsupported source/target/enrichment
//! is left untouched (the pipe simply does no work and never acks) rather
//! than faking delivery.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use fakecloud_core::delivery::{DeliveryBus, KmsHook};
use fakecloud_dynamodb::SharedDynamoDbState;
use fakecloud_kinesis::SharedKinesisState;
use fakecloud_lambda::filter::FilterSet;
use fakecloud_persistence::SnapshotHook;
use fakecloud_pipes::SharedPipesState;
use fakecloud_sqs::SharedSqsState;

/// What kind of source a RUNNING pipe reads from.
enum SourceKind {
    Sqs,
    Kinesis,
    DynamoDbStream,
}

/// One RUNNING pipe resolved for this tick.
struct RunningPipe {
    /// The pipe's own ARN — used to key stream checkpoints.
    arn: String,
    source_arn: String,
    source_kind: SourceKind,
    target_arn: String,
    target_params: Option<Value>,
    /// `TargetParameters.InputTemplate`, applied per event before delivery.
    input_template: Option<String>,
    /// `Enrichment` ARN (a Lambda, this batch); the matched batch is sent to
    /// it and its JSON return replaces the batch before target delivery.
    enrichment: Option<String>,
    /// `EnrichmentParameters.InputTemplate`, applied per event before the
    /// enrichment invocation.
    enrichment_input_template: Option<String>,
    filter: FilterSet,
    batch_size: usize,
    /// `StartingPosition` for a stream source (TRIM_HORIZON / LATEST /
    /// AT_TIMESTAMP); ignored for SQS.
    starting_position: Option<String>,
    starting_position_timestamp: Option<f64>,
    /// `SourceParameters.{Kinesis,DynamoDB}StreamParameters.MaximumRetryAttempts`.
    /// `None` (or a negative value) means retry forever — AWS's default. A
    /// non-negative value caps retries before the poison batch is routed to the
    /// DLQ and the checkpoint advances past it.
    max_retry_attempts: Option<i64>,
    /// `...StreamParameters.DeadLetterConfig.Arn` — where an exhausted batch is
    /// sent so a poison record can't wedge the stream forever.
    dlq_arn: Option<String>,
}

pub struct PipesRunner {
    pipes_state: SharedPipesState,
    sqs_state: SharedSqsState,
    kinesis_state: Option<SharedKinesisState>,
    dynamodb_state: Option<SharedDynamoDbState>,
    delivery: Arc<DeliveryBus>,
    /// Decrypts at-rest SSE-KMS / SSE-SQS message bodies before forwarding,
    /// mirroring the SQS->Lambda poller: AWS consumers see plaintext, so a
    /// pipe must too rather than forward opaque envelope bytes.
    kms_hook: Option<Arc<dyn KmsHook>>,
    /// Persists pipes state (source checkpoints) after a poll advances a
    /// checkpoint. Without this the runner mutates `pipes_state` in memory but
    /// the advance never reaches disk until an unrelated API mutation triggers
    /// the pipes snapshot hook, so a restart re-replays the retained backlog
    /// (bug-hunt 2026-07-01 M1). `None` in memory-only mode.
    persist_hook: Option<SnapshotHook>,
    /// Persists SQS state after an SQS-source pipe hides (picks) or deletes
    /// (acks) messages. Consuming a message mutates the shared SQS state in
    /// memory only; without this the ack never reaches disk until an unrelated
    /// SQS API write triggers its snapshot, so a restart resurrects the whole
    /// consumed backlog and the pipe re-delivers it (guaranteed duplicate
    /// delivery). Mirrors the SQS->Lambda poller's snapshot hook. `None` in
    /// memory-only mode.
    sqs_snapshot_hook: Option<SnapshotHook>,
    /// Set by `pick_messages` / `delete_messages`; drained once per poll to
    /// flush at most one SQS snapshot per cycle rather than one per message.
    sqs_dirty: Arc<AtomicBool>,
    /// Set by `set_checkpoint`; drained once per poll to flush at most one
    /// snapshot per cycle rather than one per delivered record.
    checkpoints_dirty: Arc<AtomicBool>,
    /// Per-window delivery-failure counts for stream sources, keyed by
    /// `{pipe_arn}\u{1f}{window_head_sequence}`. Drives MaximumRetryAttempts:
    /// once a window's count exceeds the cap it is routed to the DLQ and the
    /// checkpoint advances past it so a poison record can't wedge the stream.
    retry_counts: Mutex<HashMap<String, i64>>,
}

impl PipesRunner {
    pub fn new(
        pipes_state: SharedPipesState,
        sqs_state: SharedSqsState,
        delivery: Arc<DeliveryBus>,
    ) -> Self {
        Self {
            pipes_state,
            sqs_state,
            kinesis_state: None,
            dynamodb_state: None,
            delivery,
            kms_hook: None,
            persist_hook: None,
            sqs_snapshot_hook: None,
            sqs_dirty: Arc::new(AtomicBool::new(false)),
            checkpoints_dirty: Arc::new(AtomicBool::new(false)),
            retry_counts: Mutex::new(HashMap::new()),
        }
    }

    /// Wire the pipes snapshot hook so checkpoint advances are flushed to disk.
    pub fn with_persist_hook(mut self, hook: SnapshotHook) -> Self {
        self.persist_hook = Some(hook);
        self
    }

    /// Wire the SQS snapshot hook so SQS-source acks (picked/deleted messages)
    /// are flushed to disk and don't re-deliver on restart.
    pub fn with_sqs_snapshot_hook(mut self, hook: SnapshotHook) -> Self {
        self.sqs_snapshot_hook = Some(hook);
        self
    }

    pub fn with_kinesis_state(mut self, state: SharedKinesisState) -> Self {
        self.kinesis_state = Some(state);
        self
    }

    pub fn with_dynamodb_state(mut self, state: SharedDynamoDbState) -> Self {
        self.dynamodb_state = Some(state);
        self
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    pub async fn run(self) {
        // Sleep *after* each poll rather than using a fixed-rate `interval`:
        // `interval` with the default Burst behaviour fires back-to-back to
        // "catch up" whenever a poll runs long, which on an oversubscribed
        // 2-core CI runner turns the runner into a busy loop that starves the
        // request handlers. A trailing sleep guarantees at least this gap
        // between polls no matter how long a poll took. One second is well
        // within Pipes' delivery-latency tolerance and keeps the runner's
        // constant background load low so it doesn't tip a constrained runner
        // over when it runs alongside concurrent terraform applies.
        let this = Arc::new(self);
        loop {
            this.poll().await;
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    async fn poll(self: &Arc<Self>) {
        // Backstop the per-request `spawn_settle` tasks: rescue any pipe that
        // has been transient for over a second, so a DELETING/UPDATING pipe
        // still settles even when a CPU-starved runner delays its spawned
        // settle task past the provider's waiter timeout. Cheap (a single
        // write lock over the pipe map) and a no-op under normal load.
        // A settled DELETING pipe removes it and purges its checkpoints; mark
        // dirty so the poll's snapshot flush persists that removal (otherwise a
        // restart resurrects the pipe and its stale checkpoints).
        if fakecloud_pipes::drain_overdue_transient_pipes(&self.pipes_state, 1.0) > 0 {
            self.checkpoints_dirty.store(true, Ordering::Relaxed);
        }

        // Process each pipe concurrently. Serially awaiting them let one pipe
        // whose target is slow (or momentarily unreachable) stall delivery for
        // every other pipe on the same tick — head-of-line blocking (bug-hunt
        // 2026-07-01 L3). Each pipe keys its own checkpoints/acks, so the work
        // is independent; per-pipe ordering is preserved because a single task
        // drives each pipe.
        let mut set = tokio::task::JoinSet::new();
        for pipe in self.collect_running_pipes() {
            let me = Arc::clone(self);
            set.spawn(async move {
                match pipe.source_kind {
                    SourceKind::Sqs => me.process_sqs_pipe(&pipe).await,
                    SourceKind::Kinesis => me.process_kinesis_pipe(&pipe).await,
                    SourceKind::DynamoDbStream => me.process_ddb_pipe(&pipe).await,
                }
            });
        }
        while set.join_next().await.is_some() {}

        // Flush at most one snapshot per poll cycle, and only when a checkpoint
        // actually advanced, so a restart resumes from the delivered position
        // instead of re-replaying the retained backlog (M1).
        if self.checkpoints_dirty.swap(false, Ordering::Relaxed) {
            if let Some(hook) = &self.persist_hook {
                hook().await;
            }
        }

        // Flush SQS state once per cycle when an SQS-source pipe consumed
        // messages this tick, so acked (deleted) and in-flight (hidden) messages
        // survive a restart instead of the queue reverting and the pipe
        // re-delivering the backlog. Done after the write guards are released so
        // the blocking snapshot IO never runs under the SQS lock.
        if self.sqs_dirty.swap(false, Ordering::Relaxed) {
            if let Some(hook) = &self.sqs_snapshot_hook {
                hook().await;
            }
        }
    }

    /// Snapshot every RUNNING pipe whose source is one we execute.
    fn collect_running_pipes(&self) -> Vec<RunningPipe> {
        let accounts = self.pipes_state.read();
        let mut out = Vec::new();
        for state in accounts.accounts.values() {
            for pipe in state.pipes.values() {
                if pipe.get("CurrentState").and_then(Value::as_str) != Some("RUNNING") {
                    continue;
                }
                let Some(source_arn) = pipe.get("Source").and_then(Value::as_str) else {
                    continue;
                };
                let source_kind = if source_arn.contains(":sqs:") {
                    SourceKind::Sqs
                } else if source_arn.contains(":kinesis:") {
                    SourceKind::Kinesis
                } else if source_arn.contains(":dynamodb:") && source_arn.contains("/stream/") {
                    SourceKind::DynamoDbStream
                } else {
                    continue;
                };
                let Some(target_arn) = pipe.get("Target").and_then(Value::as_str) else {
                    continue;
                };
                let source_params = pipe.get("SourceParameters");
                let filter = build_filter(source_params);
                let batch_size = source_batch_size(source_params, &source_kind);
                let (starting_position, starting_position_timestamp) =
                    starting_position(source_params, &source_kind);
                let (max_retry_attempts, dlq_arn) =
                    stream_retry_config(source_params, &source_kind);
                let target_params = pipe.get("TargetParameters").cloned();
                let input_template = target_params
                    .as_ref()
                    .and_then(|p| p.get("InputTemplate"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                let enrichment = pipe
                    .get("Enrichment")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                let enrichment_input_template = pipe
                    .get("EnrichmentParameters")
                    .and_then(|p| p.get("InputTemplate"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(str::to_string);
                let arn = pipe
                    .get("Arn")
                    .and_then(Value::as_str)
                    .unwrap_or(source_arn)
                    .to_string();
                out.push(RunningPipe {
                    arn,
                    source_arn: source_arn.to_string(),
                    source_kind,
                    target_arn: target_arn.to_string(),
                    target_params,
                    input_template,
                    enrichment,
                    enrichment_input_template,
                    filter,
                    batch_size,
                    starting_position,
                    starting_position_timestamp,
                    max_retry_attempts,
                    dlq_arn,
                });
            }
        }
        out
    }

    // --- SQS source ---------------------------------------------------------

    async fn process_sqs_pipe(&self, pipe: &RunningPipe) {
        let now = Utc::now();
        // Cheap read-locked pre-check: only escalate to the SQS *write* lock
        // (which `pick_messages` takes, and which contends with every SQS
        // request handler) when the source queue actually has a visible
        // message. An idle SQS-source pipe is the common case — polled every
        // 500ms — and a per-pipe write-lock storm on the global SQS state
        // starves the request loop under load (SQS/DescribePipe calls time out,
        // the tfacc suite hangs). With no work to do, take only the read lock.
        if !self.sqs_source_has_visible_messages(&pipe.source_arn, now) {
            return;
        }
        // Pull up to BatchSize visible messages and hide them for the queue's
        // visibility window so a slow delivery doesn't re-pull the same batch.
        let picked = self.pick_messages(&pipe.source_arn, pipe.batch_size, now);
        if picked.is_empty() {
            return;
        }

        // Partition by the filter: matching events are delivered, the rest are
        // acked (deleted) immediately — AWS Pipes drops filtered events.
        let mut matched: Vec<(String, Value)> = Vec::new();
        let mut ack_ids: Vec<String> = Vec::new();
        for (id, event) in picked {
            if pipe.filter.is_empty() || pipe.filter.matches(&event) {
                matched.push((id, event));
            } else {
                ack_ids.push(id);
            }
        }

        if !matched.is_empty() {
            let events: Vec<Value> = matched.iter().map(|(_, e)| e.clone()).collect();
            if self.enrich_and_deliver(pipe, events).await {
                ack_ids.extend(matched.into_iter().map(|(id, _)| id));
            }
        }

        if !ack_ids.is_empty() {
            self.delete_messages(&pipe.source_arn, &ack_ids);
        }
    }

    /// Read-locked check for whether the source queue has at least one visible
    /// message right now. Used to gate the write-locking `pick_messages` so an
    /// idle pipe never contends on the global SQS write lock.
    fn sqs_source_has_visible_messages(&self, source_arn: &str, now: DateTime<Utc>) -> bool {
        let sqs_mas = self.sqs_state.read();
        let acct = source_arn.split(':').nth(4).unwrap_or("");
        let Some(sqs) = sqs_mas.get(acct) else {
            return false;
        };
        let Some(queue) = sqs.queues.values().find(|q| q.arn == source_arn) else {
            return false;
        };
        queue
            .messages
            .iter()
            .any(|m| m.visible_at.map(|v| v <= now).unwrap_or(true))
    }

    /// Pull up to `limit` currently-visible messages from the source queue and
    /// mark them invisible for the queue's `VisibilityTimeout`. Returns
    /// `(message_id, pipes-source-event)` pairs.
    fn pick_messages(
        &self,
        source_arn: &str,
        limit: usize,
        now: DateTime<Utc>,
    ) -> Vec<(String, Value)> {
        let mut sqs_mas = self.sqs_state.write();
        let default_acct = sqs_mas.default_account_id().to_string();
        let acct = source_arn
            .split(':')
            .nth(4)
            .unwrap_or(&default_acct)
            .to_string();
        let region = source_arn
            .split(':')
            .nth(3)
            .unwrap_or("us-east-1")
            .to_string();
        let sqs = sqs_mas.get_or_create(&acct);
        let Some(queue) = sqs.queues.values_mut().find(|q| q.arn == source_arn) else {
            return Vec::new();
        };
        let visibility_timeout: i64 = queue
            .attributes
            .get("VisibilityTimeout")
            .and_then(|s| s.parse().ok())
            .unwrap_or(30);
        let visible_at = now + chrono::Duration::seconds(visibility_timeout);
        // Bodies are stored encrypted at rest on an SSE-KMS / managed-SSE
        // queue; decrypt before forwarding so the target sees plaintext.
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

        let mut out = Vec::new();
        for msg in queue.messages.iter_mut() {
            if out.len() >= limit {
                break;
            }
            if let Some(vis) = msg.visible_at {
                if vis > now {
                    continue;
                }
            }
            msg.visible_at = Some(visible_at);
            let body = self.decrypt_body(&msg.body, source_arn, &acct, encrypted);
            out.push((
                msg.message_id.clone(),
                sqs_source_event(msg, &body, source_arn, &region),
            ));
        }
        // Hiding messages for the visibility window is a durable mutation of SQS
        // state; mark it so the poll flushes it and a restart doesn't re-pull an
        // in-flight batch.
        if !out.is_empty() {
            self.sqs_dirty.store(true, Ordering::Relaxed);
        }
        out
    }

    /// Decrypt an at-rest SQS body when the queue is encrypted and the body
    /// is a fakecloud KMS envelope. Falls back to the raw body otherwise.
    fn decrypt_body(&self, body: &str, source_arn: &str, account: &str, encrypted: bool) -> String {
        if !encrypted || !looks_like_fakecloud_envelope(body) {
            return body.to_string();
        }
        let Some(hook) = self.kms_hook.as_ref() else {
            return body.to_string();
        };
        let mut ctx = HashMap::new();
        ctx.insert("aws:sqs:arn".to_string(), source_arn.to_string());
        match hook.decrypt(account, body, "sqs.amazonaws.com", ctx) {
            Ok(bytes) => String::from_utf8_lossy(&bytes).to_string(),
            Err(err) => {
                tracing::warn!(%source_arn, %err, "pipes: KMS decrypt failed; forwarding opaque body");
                body.to_string()
            }
        }
    }

    fn delete_messages(&self, source_arn: &str, ids: &[String]) {
        let mut sqs_mas = self.sqs_state.write();
        let default_acct = sqs_mas.default_account_id().to_string();
        let acct = source_arn
            .split(':')
            .nth(4)
            .unwrap_or(&default_acct)
            .to_string();
        let sqs = sqs_mas.get_or_create(&acct);
        if let Some(queue) = sqs.queues.values_mut().find(|q| q.arn == source_arn) {
            let before = queue.messages.len();
            queue.messages.retain(|m| !ids.contains(&m.message_id));
            // Deleting acked messages is the durable mutation that must survive a
            // restart, else the pipe re-delivers them (duplicate delivery).
            if queue.messages.len() != before {
                self.sqs_dirty.store(true, Ordering::Relaxed);
            }
        }
    }

    // --- Kinesis source -----------------------------------------------------

    async fn process_kinesis_pipe(&self, pipe: &RunningPipe) {
        let Some(kinesis_state) = self.kinesis_state.as_ref() else {
            return;
        };
        let account = arn_account(&pipe.source_arn);
        let region = arn_region(&pipe.source_arn);

        // Snapshot each shard's pending window and seed missing checkpoints.
        //
        // The checkpoint stores the *sequence number* of the last processed
        // record (empty string = before the first record), NOT a raw index.
        // Kinesis physically trims expired records off the front of a shard
        // (`trim_expired_records`), which shifts every index — an index-based
        // checkpoint would then skip or re-read records (silent data loss,
        // bug-hunt 2026-07-01 H1). A sequence number is stable across trims, so
        // resolving the window start by `seq > checkpoint` stays correct.
        struct ShardWindow {
            shard_id: String,
            last_seq: String,
            records: Vec<fakecloud_kinesis::KinesisRecord>,
        }
        let windows: Vec<ShardWindow> = {
            let ks = kinesis_state.read();
            let Some(kinesis) = ks.get(&account) else {
                return;
            };
            let Some(stream) = kinesis
                .streams
                .values()
                .find(|s| s.stream_arn == pipe.source_arn)
            else {
                return;
            };
            let mut seeds: Vec<(String, String)> = Vec::new();
            let mut windows = Vec::new();
            for shard in &stream.shards {
                let key = format!("{}#{}", pipe.arn, shard.shard_id);
                let checkpoint = match self.checkpoint(&account, &key) {
                    Some(cp) => cp,
                    None => {
                        // First tick: resolve StartingPosition to an index, then
                        // record the sequence number *just before* the first
                        // record to deliver (empty for TRIM_HORIZON / empty
                        // shard) so subsequent ticks resume by sequence number.
                        let init = kinesis_start_index(
                            &shard.records,
                            pipe.starting_position.as_deref(),
                            pipe.starting_position_timestamp,
                        );
                        let seed = if init == 0 {
                            String::new()
                        } else {
                            shard.records[init - 1].sequence_number.clone()
                        };
                        seeds.push((key.clone(), seed.clone()));
                        seed
                    }
                };
                let start = kinesis_window_start(&shard.records, &checkpoint);
                if start >= shard.records.len() {
                    continue;
                }
                let end = shard
                    .records
                    .len()
                    .min(start.saturating_add(pipe.batch_size));
                windows.push(ShardWindow {
                    shard_id: shard.shard_id.clone(),
                    last_seq: shard.records[end - 1].sequence_number.clone(),
                    records: shard.records[start..end].to_vec(),
                });
            }
            drop(ks);
            // Persist the first-seen checkpoints so LATEST/AT_TIMESTAMP don't
            // re-resolve (and re-skip) on every subsequent tick.
            for (key, seed) in seeds {
                self.set_checkpoint(&account, &key, seed);
            }
            windows
        };

        for window in windows {
            let events: Vec<Value> = window
                .records
                .iter()
                .map(|r| kinesis_source_event(r, &window.shard_id, &pipe.source_arn, &region))
                .collect();
            let key = format!("{}#{}", pipe.arn, window.shard_id);
            self.deliver_stream_window(&account, &key, window.last_seq, pipe, events)
                .await;
        }
    }

    // --- DynamoDB stream source --------------------------------------------

    async fn process_ddb_pipe(&self, pipe: &RunningPipe) {
        let Some(dynamodb_state) = self.dynamodb_state.as_ref() else {
            return;
        };
        let account = arn_account(&pipe.source_arn);
        let Some(table_name) = pipe
            .source_arn
            .split(":table/")
            .nth(1)
            .and_then(|t| t.split('/').next())
            .map(str::to_string)
        else {
            return;
        };

        // Seed the checkpoint the first time we see this pipe.
        if self.checkpoint(&account, &pipe.arn).is_none() {
            let init = {
                let ds = dynamodb_state.read();
                let Some(dynamodb) = ds.get(&account) else {
                    return;
                };
                let Some(table) = dynamodb.tables.get(&table_name) else {
                    return;
                };
                // DDB streams only accept TRIM_HORIZON (replay) and LATEST
                // (skip the existing backlog).
                if pipe.starting_position.as_deref() == Some("LATEST") {
                    let records = table.stream_records.read();
                    records
                        .iter()
                        .map(|r| r.dynamodb.sequence_number.clone())
                        .max()
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            };
            self.set_checkpoint(&account, &pipe.arn, init);
        }
        let checkpoint = self.checkpoint(&account, &pipe.arn).unwrap_or_default();

        let (last_seq, events) = {
            let ds = dynamodb_state.read();
            let Some(dynamodb) = ds.get(&account) else {
                return;
            };
            let Some(table) = dynamodb.tables.get(&table_name) else {
                return;
            };
            if !table.stream_enabled {
                return;
            }
            let stream_records = table.stream_records.read();
            let mut window: Vec<_> = stream_records
                .iter()
                .filter(|r| {
                    checkpoint.is_empty()
                        || r.dynamodb.sequence_number.as_str() > checkpoint.as_str()
                })
                .take(pipe.batch_size)
                .cloned()
                .collect();
            window.sort_by(|a, b| a.dynamodb.sequence_number.cmp(&b.dynamodb.sequence_number));
            let last_seq = window.last().map(|r| r.dynamodb.sequence_number.clone());
            let events: Vec<Value> = window.iter().map(ddb_source_event).collect();
            (last_seq, events)
        };

        let Some(last_seq) = last_seq else {
            return;
        };
        let arn = pipe.arn.clone();
        self.deliver_stream_window(&account, &arn, last_seq, pipe, events)
            .await;
    }

    /// Deliver one stream window and apply the checkpoint + retry policy:
    ///
    /// * On success the checkpoint advances and the window's failure count is
    ///   cleared.
    /// * On failure the window's failure count is bumped. Once it exceeds
    ///   `MaximumRetryAttempts` (a non-negative cap), the batch is routed to the
    ///   configured DLQ (if any) and the checkpoint advances past it so a poison
    ///   record can't wedge the stream forever. A negative/absent cap means
    ///   retry indefinitely (AWS default), so the checkpoint stays put.
    async fn deliver_stream_window(
        &self,
        account: &str,
        checkpoint_key: &str,
        window_head_seq: String,
        pipe: &RunningPipe,
        events: Vec<Value>,
    ) {
        let retry_key = format!("{checkpoint_key}\u{1f}{window_head_seq}");
        // Retain a copy for the DLQ before delivery consumes the batch.
        let events_for_dlq = events.clone();
        if self.process_stream_window(pipe, events).await {
            self.set_checkpoint(account, checkpoint_key, window_head_seq);
            self.retry_counts.lock().unwrap().remove(&retry_key);
            return;
        }
        let attempts = {
            let mut counts = self.retry_counts.lock().unwrap();
            let c = counts.entry(retry_key.clone()).or_insert(0);
            *c += 1;
            *c
        };
        if let Some(max) = pipe.max_retry_attempts {
            if max >= 0 && attempts > max {
                tracing::warn!(
                    pipe = %pipe.arn,
                    seq = %window_head_seq,
                    attempts,
                    "pipes: stream window exceeded MaximumRetryAttempts; routing to DLQ and advancing"
                );
                self.route_stream_batch_to_dlq(pipe, &window_head_seq, &events_for_dlq);
                self.set_checkpoint(account, checkpoint_key, window_head_seq);
                self.retry_counts.lock().unwrap().remove(&retry_key);
            }
        }
    }

    /// Send an exhausted stream batch to the pipe's DeadLetterConfig target
    /// (SQS or SNS). The DLQ record carries the failed batch's metadata plus the
    /// records, mirroring the envelope AWS Pipes writes. A no-op when no DLQ is
    /// configured — the batch is still dropped so the stream unblocks.
    fn route_stream_batch_to_dlq(
        &self,
        pipe: &RunningPipe,
        window_head_seq: &str,
        events: &[Value],
    ) {
        let Some(dlq_arn) = pipe.dlq_arn.as_deref() else {
            return;
        };
        let payload = serde_json::json!({
            "timestamp": Utc::now().to_rfc3339(),
            "pipeArn": pipe.arn,
            "sourceArn": pipe.source_arn,
            "endSequenceNumber": window_head_seq,
            "recordCount": events.len(),
            "records": events,
        });
        let body = serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_string());
        if dlq_arn.contains(":sqs:") {
            self.delivery.send_to_sqs(dlq_arn, &body, &HashMap::new());
        } else if dlq_arn.contains(":sns:") {
            self.delivery.publish_to_sns(dlq_arn, &body, None);
        }
    }

    /// Filter, enrich, transform and deliver one stream window. Returns true
    /// when the window may be checkpointed past (filtered-only windows count
    /// as success; a delivery failure returns false so the window is retried).
    async fn process_stream_window(&self, pipe: &RunningPipe, events: Vec<Value>) -> bool {
        let matched: Vec<Value> = if pipe.filter.is_empty() {
            events
        } else {
            events
                .into_iter()
                .filter(|e| pipe.filter.matches(e))
                .collect()
        };
        // Every record in the window is filtered out: nothing to deliver, but
        // the checkpoint must still advance past them (AWS consumes them).
        if matched.is_empty() {
            return true;
        }
        self.enrich_and_deliver(pipe, matched).await
    }

    // --- Shared enrichment + transform + delivery --------------------------

    /// Run the matched batch through the optional enrichment, apply the target
    /// `InputTemplate` per resulting event, and deliver. Returns true when the
    /// batch may be acked/checkpointed (delivered, or the enrichment dropped
    /// every event).
    async fn enrich_and_deliver(&self, pipe: &RunningPipe, events: Vec<Value>) -> bool {
        let enriched = match self.enrich(pipe, events).await {
            Some(events) => events,
            // Enrichment failed or is unsupported: don't ack, retry later.
            None => return false,
        };
        if enriched.is_empty() {
            // Enrichment legitimately dropped every event — consume them.
            return true;
        }
        let transformed: Vec<Value> = enriched
            .iter()
            .map(|e| apply_input_template(pipe.input_template.as_deref(), e))
            .collect();
        self.deliver(&pipe.target_arn, pipe.target_params.as_ref(), &transformed)
            .await
    }

    /// Send the batch through the pipe's enrichment (a Lambda) and return the
    /// events its JSON response yields. `Some(events)` on success (possibly
    /// empty — the enrichment dropped everything); `None` when the enrichment
    /// failed, isn't wired, or is an unsupported type (so the caller retries
    /// rather than fakes delivery). With no enrichment configured the batch
    /// passes through unchanged.
    async fn enrich(&self, pipe: &RunningPipe, events: Vec<Value>) -> Option<Vec<Value>> {
        let Some(enr_arn) = pipe.enrichment.as_deref() else {
            return Some(events);
        };
        // The enrichment receives the batch as an array, each event first run
        // through the enrichment InputTemplate when one is configured.
        let input: Vec<Value> = events
            .iter()
            .map(|e| apply_input_template(pipe.enrichment_input_template.as_deref(), e))
            .collect();
        if enr_arn.contains(":lambda:") {
            let payload =
                serde_json::to_string(&Value::Array(input)).unwrap_or_else(|_| "[]".into());
            match self.delivery.invoke_lambda(enr_arn, &payload).await {
                Some(Ok(bytes)) => Some(enrichment_output(&bytes)),
                Some(Err(err)) => {
                    tracing::warn!(%enr_arn, %err, "pipes: enrichment Lambda failed; events will be retried");
                    None
                }
                // No Lambda runtime wired (unit/no-runtime): leave events.
                None => None,
            }
        } else {
            // Non-Lambda enrichment (Step Functions sync / API destination /
            // API Gateway) needs a synchronous round-trip not yet wired; don't
            // fake it — leave the events so they deliver once supported.
            tracing::warn!(%enr_arn, "pipes: unsupported enrichment type; events held");
            None
        }
    }

    /// Deliver the batch to the target. Returns true if delivery succeeded (and
    /// the events may be acked). Lambda receives the whole batch as a JSON
    /// array (Pipes batches to Lambda); SQS/SNS/SFN/Kinesis receive one
    /// message per event.
    async fn deliver(
        &self,
        target_arn: &str,
        target_params: Option<&Value>,
        batch: &[Value],
    ) -> bool {
        if target_arn.contains(":lambda:") {
            let payload = serde_json::to_string(&Value::Array(batch.to_vec()))
                .unwrap_or_else(|_| "[]".to_string());
            match self.delivery.invoke_lambda(target_arn, &payload).await {
                Some(Ok(_)) => true,
                Some(Err(err)) => {
                    tracing::warn!(%target_arn, %err, "pipes: Lambda target invocation failed; events will be retried");
                    false
                }
                // No Lambda delivery wired (unit/no-runtime): leave events.
                None => false,
            }
        } else if target_arn.contains(":sqs:") {
            // FIFO queues need MessageGroupId (and optionally
            // MessageDeduplicationId) from TargetParameters.SqsQueueParameters,
            // else the SQS send is rejected for a .fifo target.
            let sqs_params = target_params.and_then(|p| p.get("SqsQueueParameters"));
            let group_id = sqs_params
                .and_then(|p| p.get("MessageGroupId"))
                .and_then(Value::as_str);
            let dedup_id = sqs_params
                .and_then(|p| p.get("MessageDeduplicationId"))
                .and_then(Value::as_str);
            for event in batch {
                let body = event_to_payload(event);
                if group_id.is_some() || dedup_id.is_some() {
                    self.delivery.send_to_sqs_with_attrs(
                        target_arn,
                        &body,
                        &HashMap::new(),
                        group_id,
                        dedup_id,
                    );
                } else {
                    self.delivery
                        .send_to_sqs(target_arn, &body, &HashMap::new());
                }
            }
            true
        } else if target_arn.contains(":sns:") {
            for event in batch {
                let body = event_to_payload(event);
                self.delivery.publish_to_sns(target_arn, &body, None);
            }
            true
        } else if target_arn.contains(":states:") {
            // Step Functions: start one execution per event with the event as
            // input (Pipes invokes standard state machines per record).
            for event in batch {
                let input = event_to_payload(event);
                self.delivery
                    .start_stepfunctions_execution(target_arn, &input);
            }
            true
        } else if target_arn.contains(":events:") {
            // EventBridge bus: PutEvents one entry per event. Source/DetailType
            // come from the target's EventBridgeEventBusParameters (AWS
            // requires them), defaulting to Pipes' conventional values.
            let bus_name = target_arn
                .rsplit_once("event-bus/")
                .map(|(_, n)| n)
                .unwrap_or("default");
            let eb_params = target_params.and_then(|p| p.get("EventBridgeEventBusParameters"));
            let source = eb_params
                .and_then(|p| p.get("Source"))
                .and_then(Value::as_str)
                .unwrap_or("Pipes");
            let detail_type = eb_params
                .and_then(|p| p.get("DetailType"))
                .and_then(Value::as_str)
                .unwrap_or("Event");
            for event in batch {
                let detail = event_to_payload(event);
                self.delivery
                    .put_event_to_eventbridge(source, detail_type, &detail, bus_name);
            }
            true
        } else if target_arn.contains(":kinesis:") {
            // Kinesis: one record per event. PartitionKey comes from the
            // target's KinesisStreamParameters (a literal here), falling back
            // to the event's index so records still spread across shards.
            let configured_pk = target_params
                .and_then(|p| p.get("KinesisStreamParameters"))
                .and_then(|p| p.get("PartitionKey"))
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty());
            for (idx, event) in batch.iter().enumerate() {
                let data = event_to_payload(event);
                let fallback = idx.to_string();
                let pk = configured_pk.unwrap_or(fallback.as_str());
                self.delivery.send_to_kinesis(target_arn, &data, pk);
            }
            true
        } else {
            // Other targets (ECS / Batch / Redshift / HTTP / SageMaker / …)
            // land in a later batch. Don't ack — leave the events so they're
            // delivered once the target type is supported, never faked.
            false
        }
    }

    // --- Checkpoint helpers -------------------------------------------------

    fn checkpoint(&self, account: &str, key: &str) -> Option<String> {
        self.pipes_state
            .read()
            .get(account)
            .and_then(|s| s.source_checkpoints.get(key).cloned())
    }

    fn set_checkpoint(&self, account: &str, key: &str, value: String) {
        self.pipes_state
            .write()
            .get_or_create(account)
            .source_checkpoints
            .insert(key.to_string(), value);
        self.checkpoints_dirty.store(true, Ordering::Relaxed);
    }
}

/// Build the pipe's source filter from `SourceParameters.FilterCriteria`.
fn build_filter(source_params: Option<&Value>) -> FilterSet {
    let patterns: Vec<String> = source_params
        .and_then(|p| p.get("FilterCriteria"))
        .and_then(|f| f.get("Filters"))
        .and_then(Value::as_array)
        .map(|filters| {
            filters
                .iter()
                .filter_map(|f| f.get("Pattern").and_then(Value::as_str))
                .map(|s| s.to_string())
                .collect()
        })
        .unwrap_or_default();
    FilterSet::from_strings(patterns)
}

/// Resolve the per-source `BatchSize` (clamped to each source's AWS ceiling).
fn source_batch_size(source_params: Option<&Value>, kind: &SourceKind) -> usize {
    let (param_key, default, max) = match kind {
        SourceKind::Sqs => ("SqsQueueParameters", 10, 10),
        SourceKind::Kinesis => ("KinesisStreamParameters", 100, 10_000),
        SourceKind::DynamoDbStream => ("DynamoDBStreamParameters", 100, 10_000),
    };
    source_params
        .and_then(|p| p.get(param_key))
        .and_then(|p| p.get("BatchSize"))
        .and_then(Value::as_i64)
        .filter(|n| *n > 0)
        .unwrap_or(default)
        .min(max) as usize
}

/// Resolve `MaximumRetryAttempts` + `DeadLetterConfig.Arn` for a stream source.
/// Returns `(None, None)` for SQS (which has its own redrive) or when the
/// parameters are absent.
fn stream_retry_config(
    source_params: Option<&Value>,
    kind: &SourceKind,
) -> (Option<i64>, Option<String>) {
    let param_key = match kind {
        SourceKind::Kinesis => "KinesisStreamParameters",
        SourceKind::DynamoDbStream => "DynamoDBStreamParameters",
        SourceKind::Sqs => return (None, None),
    };
    let params = source_params.and_then(|p| p.get(param_key));
    let max_retry = params
        .and_then(|p| p.get("MaximumRetryAttempts"))
        .and_then(Value::as_i64);
    let dlq = params
        .and_then(|p| p.get("DeadLetterConfig"))
        .and_then(|d| d.get("Arn"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    (max_retry, dlq)
}

/// Resolve `StartingPosition` (+ timestamp) for a stream source.
fn starting_position(
    source_params: Option<&Value>,
    kind: &SourceKind,
) -> (Option<String>, Option<f64>) {
    let param_key = match kind {
        SourceKind::Kinesis => "KinesisStreamParameters",
        SourceKind::DynamoDbStream => "DynamoDBStreamParameters",
        SourceKind::Sqs => return (None, None),
    };
    let params = source_params.and_then(|p| p.get(param_key));
    let pos = params
        .and_then(|p| p.get("StartingPosition"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let ts = params
        .and_then(|p| p.get("StartingPositionTimestamp"))
        .and_then(Value::as_f64);
    (pos, ts)
}

/// Index of the first record strictly after `checkpoint` (the sequence number
/// of the last delivered record; empty = before the first record). Records are
/// stored in ascending, fixed-width, per-shard-monotonic sequence order, so a
/// lexicographic compare equals a numeric compare and `partition_point` finds
/// the boundary. Because the cursor is a sequence number rather than an index,
/// it stays correct after Kinesis trims expired records off the front (H1).
fn kinesis_window_start(records: &[fakecloud_kinesis::KinesisRecord], checkpoint: &str) -> usize {
    records.partition_point(|r| r.sequence_number.as_str() <= checkpoint)
}

/// First record index a Kinesis pipe should read given its StartingPosition.
fn kinesis_start_index(
    records: &[fakecloud_kinesis::KinesisRecord],
    starting_position: Option<&str>,
    starting_position_timestamp: Option<f64>,
) -> usize {
    match starting_position.unwrap_or("TRIM_HORIZON") {
        "LATEST" => records.len(),
        "AT_TIMESTAMP" => {
            let target = starting_position_timestamp.map(|t| t as i64).unwrap_or(0);
            records
                .iter()
                .position(|r| r.approximate_arrival_timestamp.timestamp() >= target)
                .unwrap_or(records.len())
        }
        _ => 0, // TRIM_HORIZON
    }
}

fn arn_account(arn: &str) -> String {
    arn.split(':').nth(4).unwrap_or("").to_string()
}

fn arn_region(arn: &str) -> String {
    arn.split(':').nth(3).unwrap_or("us-east-1").to_string()
}

/// Serialize a (possibly already-transformed) event for a one-per-record
/// target. A string event is sent as-is (it was rendered by an InputTemplate);
/// anything else is JSON-serialized.
fn event_to_payload(event: &Value) -> String {
    match event {
        Value::String(s) => s.clone(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// Normalize an enrichment Lambda's JSON response into a list of events. An
/// array becomes its elements; a single object becomes a one-element batch;
/// null / empty drops the batch. Pipes enrichment may return 0..N events.
fn enrichment_output(bytes: &[u8]) -> Vec<Value> {
    match serde_json::from_slice::<Value>(bytes) {
        Ok(Value::Array(items)) => items,
        Ok(Value::Null) => Vec::new(),
        Ok(Value::String(s)) if s.is_empty() => Vec::new(),
        Ok(other) => vec![other],
        Err(_) => Vec::new(),
    }
}

/// Apply an AWS Pipes `InputTemplate` to one event. With no template the event
/// passes through unchanged. Otherwise every `<$.json.path>` placeholder is
/// resolved against the event (and `<aws.pipes.event.json>` expands to the
/// whole event); the rendered string is returned parsed as JSON when it parses
/// and as a JSON string otherwise — matching how AWS treats a template that
/// produces a JSON object versus free text.
fn apply_input_template(template: Option<&str>, event: &Value) -> Value {
    let Some(template) = template else {
        return event.clone();
    };
    let rendered = render_template(template, event);
    match serde_json::from_str::<Value>(&rendered) {
        Ok(value) => value,
        Err(_) => Value::String(rendered),
    }
}

/// Substitute `<...>` placeholders in a Pipes InputTemplate.
fn render_template(template: &str, event: &Value) -> String {
    let mut out = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('<') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        if let Some(close) = after.find('>') {
            out.push_str(&resolve_token(after[..close].trim(), event));
            rest = &after[close + 1..];
        } else {
            // Unterminated '<' — emit literally and stop scanning.
            out.push('<');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

/// Resolve one InputTemplate token to its replacement text.
fn resolve_token(token: &str, event: &Value) -> String {
    if token == "aws.pipes.event.json" {
        return serde_json::to_string(event).unwrap_or_default();
    }
    if let Some(path) = token.strip_prefix('$') {
        return match resolve_json_path(event, path) {
            Some(Value::String(s)) => s,
            Some(other) => serde_json::to_string(&other).unwrap_or_default(),
            // Unresolved path -> empty, as AWS substitutes nothing.
            None => String::new(),
        };
    }
    // Unknown token: leave it intact rather than silently blanking it.
    format!("<{token}>")
}

/// Resolve a simple JSON path like `.detail.name` against an event.
fn resolve_json_path(event: &Value, path: &str) -> Option<Value> {
    let mut current = event;
    for segment in path.split('.') {
        if segment.is_empty() {
            continue;
        }
        current = current.get(segment)?;
    }
    Some(current.clone())
}

/// Build the AWS Pipes SQS-source event envelope for one message. `body` is the
/// (already decrypted) message body.
fn sqs_source_event(
    msg: &fakecloud_sqs::SqsMessage,
    body: &str,
    source_arn: &str,
    region: &str,
) -> Value {
    let attributes: serde_json::Map<String, Value> = msg
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();
    json!({
        "messageId": msg.message_id,
        "receiptHandle": msg.receipt_handle.clone().unwrap_or_default(),
        "body": body,
        "attributes": attributes,
        "messageAttributes": {},
        "md5OfBody": msg.md5_of_body,
        "eventSource": "aws:sqs",
        "eventSourceArn": source_arn,
        "awsRegion": region,
    })
}

/// Build the AWS Pipes Kinesis-source event envelope for one record.
fn kinesis_source_event(
    record: &fakecloud_kinesis::KinesisRecord,
    shard_id: &str,
    source_arn: &str,
    region: &str,
) -> Value {
    json!({
        "eventSource": "aws:kinesis",
        "eventVersion": "1.0",
        "eventID": format!("{}:{}", shard_id, record.sequence_number),
        "eventName": "aws:kinesis:record",
        "invokeIdentityArn": "arn:aws:iam::123456789012:role/pipes-role",
        "awsRegion": region,
        "eventSourceARN": source_arn,
        "kinesis": {
            "kinesisSchemaVersion": "1.0",
            "partitionKey": record.partition_key,
            "sequenceNumber": record.sequence_number,
            "data": base64::engine::general_purpose::STANDARD.encode(&record.data),
            "approximateArrivalTimestamp":
                record.approximate_arrival_timestamp.timestamp_millis() as f64 / 1000.0,
        }
    })
}

/// Build the AWS Pipes DynamoDB-stream-source event envelope for one record.
fn ddb_source_event(record: &fakecloud_dynamodb::StreamRecord) -> Value {
    let mut event = json!({
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
        event["dynamodb"]["NewImage"] = json!(new_img);
    }
    if let Some(ref old_img) = record.dynamodb.old_image {
        event["dynamodb"]["OldImage"] = json!(old_img);
    }
    event
}

/// Detect a fakecloud KMS-at-rest envelope (matches the SQS service's own
/// encryption format) so plaintext bodies are passed through untouched.
fn looks_like_fakecloud_envelope(body: &str) -> bool {
    if body.starts_with("fakecloud-kms:") {
        return true;
    }
    match base64::engine::general_purpose::STANDARD.decode(body) {
        Ok(bytes) => bytes.starts_with(&[0x01, 0x02, 0x02, 0x00]),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_template_renders_json_object() {
        let event = json!({"body": "hello", "messageId": "m-1"});
        let tmpl = r#"{"text": "<$.body>", "id": "<$.messageId>"}"#;
        let out = apply_input_template(Some(tmpl), &event);
        assert_eq!(out, json!({"text": "hello", "id": "m-1"}));
    }

    #[test]
    fn input_template_whole_event_token() {
        let event = json!({"a": 1});
        let out = apply_input_template(Some("<aws.pipes.event.json>"), &event);
        assert_eq!(out, event);
    }

    #[test]
    fn input_template_non_string_value_is_injected_raw() {
        let event = json!({"count": 5, "nested": {"k": "v"}});
        let tmpl = r#"{"n": <$.count>, "obj": <$.nested>}"#;
        let out = apply_input_template(Some(tmpl), &event);
        assert_eq!(out, json!({"n": 5, "obj": {"k": "v"}}));
    }

    #[test]
    fn input_template_freeform_text_stays_string() {
        let event = json!({"name": "ada"});
        let out = apply_input_template(Some("hello <$.name>"), &event);
        assert_eq!(out, Value::String("hello ada".into()));
    }

    #[test]
    fn input_template_missing_path_substitutes_empty() {
        let event = json!({"a": 1});
        let out = apply_input_template(Some("x<$.missing>y"), &event);
        assert_eq!(out, Value::String("xy".into()));
    }

    #[test]
    fn no_template_passes_event_through() {
        let event = json!({"a": 1});
        assert_eq!(apply_input_template(None, &event), event);
    }

    #[test]
    fn enrichment_output_array_passthrough() {
        let bytes = br#"[{"a":1},{"a":2}]"#;
        assert_eq!(enrichment_output(bytes).len(), 2);
    }

    #[test]
    fn enrichment_output_single_object_wraps() {
        let bytes = br#"{"a":1}"#;
        assert_eq!(enrichment_output(bytes), vec![json!({"a": 1})]);
    }

    #[test]
    fn enrichment_output_null_drops() {
        assert!(enrichment_output(b"null").is_empty());
        assert!(enrichment_output(b"\"\"").is_empty());
        assert!(enrichment_output(b"not json").is_empty());
    }

    #[test]
    fn kinesis_start_index_respects_starting_position() {
        let records: Vec<fakecloud_kinesis::KinesisRecord> = Vec::new();
        assert_eq!(kinesis_start_index(&records, Some("LATEST"), None), 0);
        assert_eq!(kinesis_start_index(&records, Some("TRIM_HORIZON"), None), 0);
        assert_eq!(kinesis_start_index(&records, None, None), 0);
    }

    #[test]
    fn batch_size_clamps_per_source() {
        let params = json!({"SqsQueueParameters": {"BatchSize": 50}});
        assert_eq!(source_batch_size(Some(&params), &SourceKind::Sqs), 10);
        let params = json!({"KinesisStreamParameters": {"BatchSize": 500}});
        assert_eq!(source_batch_size(Some(&params), &SourceKind::Kinesis), 500);
    }

    #[test]
    fn stream_retry_config_reads_max_attempts_and_dlq() {
        let params = json!({
            "KinesisStreamParameters": {
                "MaximumRetryAttempts": 3,
                "DeadLetterConfig": {"Arn": "arn:aws:sqs:us-east-1:1:pipe-dlq"}
            }
        });
        let (max, dlq) = stream_retry_config(Some(&params), &SourceKind::Kinesis);
        assert_eq!(max, Some(3));
        assert_eq!(dlq.as_deref(), Some("arn:aws:sqs:us-east-1:1:pipe-dlq"));

        // DynamoDB stream parameters key.
        let ddb = json!({
            "DynamoDBStreamParameters": {"MaximumRetryAttempts": 0}
        });
        let (max, dlq) = stream_retry_config(Some(&ddb), &SourceKind::DynamoDbStream);
        assert_eq!(max, Some(0));
        assert!(dlq.is_none());

        // SQS sources have their own redrive; no stream retry config.
        assert_eq!(
            stream_retry_config(Some(&params), &SourceKind::Sqs),
            (None, None)
        );
        // Absent parameters => defaults (retry forever, no DLQ).
        assert_eq!(
            stream_retry_config(None, &SourceKind::Kinesis),
            (None, None)
        );
    }

    fn rec(seq: &str) -> fakecloud_kinesis::KinesisRecord {
        fakecloud_kinesis::KinesisRecord {
            sequence_number: seq.to_string(),
            partition_key: "pk".into(),
            data: Vec::new(),
            approximate_arrival_timestamp: chrono::DateTime::<Utc>::from_timestamp(0, 0).unwrap(),
        }
    }

    #[test]
    fn kinesis_window_start_resolves_by_sequence_number() {
        // Fixed-width, per-shard-monotonic sequence numbers.
        let records = vec![rec("00001"), rec("00002"), rec("00003")];
        // Empty checkpoint => start at the beginning.
        assert_eq!(kinesis_window_start(&records, ""), 0);
        // After delivering up to "00002", resume at index 2 ("00003").
        assert_eq!(kinesis_window_start(&records, "00002"), 2);
        // Caught up.
        assert_eq!(kinesis_window_start(&records, "00003"), 3);
    }

    #[test]
    fn kinesis_window_start_survives_retention_trim() {
        // A raw index checkpoint would skip/re-read after the front is trimmed;
        // a sequence-number checkpoint stays correct (H1).
        let before = vec![rec("00001"), rec("00002"), rec("00003"), rec("00004")];
        // Delivered through "00002" => next is "00003" at index 2.
        assert_eq!(kinesis_window_start(&before, "00002"), 2);
        // Kinesis trims the first two expired records; same checkpoint now
        // resolves to index 0 of the trimmed shard — still "00003", no loss.
        let after = vec![rec("00003"), rec("00004")];
        assert_eq!(kinesis_window_start(&after, "00002"), 0);
        assert_eq!(
            after[kinesis_window_start(&after, "00002")].sequence_number,
            "00003"
        );
    }

    /// Regression (bug-hunt Tier 0 side-channel-persistence): an SQS-source
    /// pipe consumes (acks/deletes) source messages by mutating shared SQS
    /// state in memory. Without the SQS snapshot hook that ack never reaches
    /// disk, so a restart resurrects the whole consumed backlog and the pipe
    /// re-delivers it. A poll that consumes messages must flush SQS state
    /// through the hook, and the acked message must be gone.
    #[tokio::test]
    async fn sqs_source_pipe_persists_consumed_messages() {
        use fakecloud_core::multi_account::MultiAccountState;
        use fakecloud_sqs::{SqsMessage, SqsQueue, SqsState};
        use parking_lot::RwLock;
        use std::collections::{BTreeMap, VecDeque};
        use std::sync::atomic::AtomicUsize;

        const ACCOUNT: &str = "123456789012";
        const REGION: &str = "us-east-1";
        let queue_arn = format!("arn:aws:sqs:{REGION}:{ACCOUNT}:pipe-src");
        let queue_url = format!("http://localhost:4566/{ACCOUNT}/pipe-src");

        let mut attrs = BTreeMap::new();
        attrs.insert("VisibilityTimeout".to_string(), "30".to_string());
        let queue = SqsQueue {
            queue_name: "pipe-src".to_string(),
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
        let mut sqs: MultiAccountState<SqsState> =
            MultiAccountState::new(ACCOUNT, REGION, "http://localhost:4566");
        sqs.default_mut()
            .queues
            .insert(queue.queue_url.clone(), queue);
        let sqs_state = Arc::new(RwLock::new(sqs));

        // One RUNNING pipe with an SQS source and a FilterCriteria that matches
        // nothing, so the message is acked via Pipes' "filter drop-as-ack" path
        // (delete) without needing a live target — exactly the durable mutation
        // that must be persisted.
        let mut pipes = fakecloud_pipes::PipesAccounts::new();
        pipes.accounts.insert(ACCOUNT.to_string(), {
            let mut state = fakecloud_pipes::PipesState::default();
            state.pipes.insert(
                "p1".to_string(),
                json!({
                    "Arn": format!("arn:aws:pipes:{REGION}:{ACCOUNT}:pipe/p1"),
                    "CurrentState": "RUNNING",
                    "Source": queue_arn,
                    "Target": format!("arn:aws:lambda:{REGION}:{ACCOUNT}:function:noop"),
                    "SourceParameters": {
                        "FilterCriteria": {
                            "Filters": [{ "Pattern": "{\"body\":[\"__never_matches__\"]}" }]
                        }
                    }
                }),
            );
            state
        });
        let pipes_state = Arc::new(RwLock::new(pipes));

        let snapshot_calls = Arc::new(AtomicUsize::new(0));
        let counter = snapshot_calls.clone();
        let hook: SnapshotHook = Arc::new(move || {
            let counter = counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
            })
        });

        let runner = Arc::new(
            PipesRunner::new(pipes_state, sqs_state.clone(), Arc::new(DeliveryBus::new()))
                .with_sqs_snapshot_hook(hook),
        );
        runner.poll().await;

        // The consumed message was acked (deleted) from the source queue...
        {
            let sqs = sqs_state.read();
            let queue = sqs.get(ACCOUNT).unwrap().queues.values().next().unwrap();
            assert!(
                queue.messages.is_empty(),
                "filtered-out source message must be acked (deleted)"
            );
        }
        // ...and that ack was flushed through the SQS snapshot hook so it
        // survives a restart instead of re-delivering.
        assert!(
            snapshot_calls.load(Ordering::SeqCst) >= 1,
            "consuming SQS messages must persist SQS state through the hook"
        );
    }
}
