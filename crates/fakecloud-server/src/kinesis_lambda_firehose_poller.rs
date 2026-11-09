//! Kinesis poller for Lambda event source mappings and Firehose
//! `KinesisStreamAsSource` delivery streams.
//!
//! Lambda mappings honor:
//! - `FilterCriteria` — non-matching records are dropped (advanced past).
//! - `StartingPosition` — `TRIM_HORIZON` (default), `LATEST`, or
//!   `AT_TIMESTAMP` paired with `StartingPositionTimestamp` to seed
//!   the per-shard checkpoint on first poll.
//!
//! Each Firehose delivery stream reads its source stream from the position it
//! was created at (AWS starts at LATEST) and hands every record's data to the
//! Firehose engine, which buffers and delivers it like Direct PUT input. Its
//! persisted checkpoint moves only past records whose batch has committed, and
//! reading pauses while the engine reports the stream backlogged or its
//! destination refuses records. Positions are scoped to the source stream's
//! creation time, so a source stream recreated under the same name starts over.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use fakecloud_core::delivery::LambdaDelivery;
use fakecloud_firehose::{
    FirehoseEngine, SharedFirehoseState, SourceLimit, SourceRead, SourceRecord, StreamKey,
};
use fakecloud_kinesis::{KinesisRecord, KinesisShard, KinesisStream, SharedKinesisState};
use fakecloud_lambda::filter::FilterSet;
use fakecloud_lambda::{LambdaInvocation, SharedLambdaState};
use fakecloud_persistence::SnapshotHook;

/// AWS caps a single GetRecords call at 10,000 records and 10 MiB.
const MAX_RECORDS_PER_READ: usize = 10_000;
const MAX_BYTES_PER_READ: usize = 10 * 1024 * 1024;

/// How much one `read_shards` call may take. `shard_bytes` is per shard and
/// `total_bytes` is shared by the shards. The only read past `total_bytes` is a
/// single record larger than all of it, which could not be read otherwise.
#[derive(Clone, Copy)]
struct ReadLimits {
    records: usize,
    shard_bytes: usize,
    total_bytes: usize,
    /// The incarnation of the stream the caller derived its checkpoints from.
    /// Another one reads nothing, so its records are not tagged as this one's.
    generation: Option<i64>,
}

struct ShardRead {
    shard_id: String,
    start: usize,
    end: usize,
    records: Vec<KinesisRecord>,
    trimmed: usize,
}

/// Records retention has removed from the front of `shard`. Adding it to a
/// record index gives a position that trimming does not shift.
fn trimmed(shard: &KinesisShard) -> usize {
    usize::try_from(shard.next_sequence_number)
        .unwrap_or(usize::MAX)
        .saturating_sub(shard.records.len())
}

struct Source {
    key: StreamKey,
    source_arn: String,
    checkpoint_id: String,
    created_at: DateTime<Utc>,
}

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

fn stream_generation(stream: &KinesisStream) -> i64 {
    stream
        .stream_creation_timestamp
        .timestamp_nanos_opt()
        .unwrap_or_default()
}

pub struct KinesisLambdaFirehosePoller {
    kinesis_state: SharedKinesisState,
    lambda_state: SharedLambdaState,
    lambda_delivery: Option<Arc<dyn LambdaDelivery>>,
    /// Persists Kinesis state after the poller advances a lambda checkpoint.
    /// Without it a checkpoint advance only reached disk on the next unrelated
    /// Kinesis API write, so a restart in the window re-delivered records the
    /// mapping had already consumed (bug-audit 4.5).
    snapshot_hook: Option<SnapshotHook>,
    firehose: Option<(SharedFirehoseState, Arc<FirehoseEngine>)>,
    /// Why each delivery stream last refused records, so the reason is logged
    /// once per change rather than on every poll.
    firehose_refusals: parking_lot::Mutex<BTreeMap<StreamKey, String>>,
}

impl KinesisLambdaFirehosePoller {
    pub fn new(kinesis_state: SharedKinesisState, lambda_state: SharedLambdaState) -> Self {
        Self {
            kinesis_state,
            lambda_state,
            lambda_delivery: None,
            snapshot_hook: None,
            firehose: None,
            firehose_refusals: parking_lot::Mutex::default(),
        }
    }

    pub fn with_lambda_delivery(mut self, delivery: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_delivery = Some(delivery);
        self
    }

    pub fn with_firehose(
        mut self,
        firehose_state: SharedFirehoseState,
        engine: Arc<FirehoseEngine>,
    ) -> Self {
        self.firehose = Some((firehose_state, engine));
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

    /// Polls Lambda mappings and Firehose sources side by side, so a Lambda
    /// invocation awaiting its function does not hold up Firehose reads.
    pub async fn run(self: Arc<Self>) {
        self.settle_restored_checkpoints().await;
        let lambda = async {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                self.poll().await;
            }
        };
        let firehose = async {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                self.poll_firehose().await;
                self.sweep_orphaned_checkpoints().await;
            }
        };
        tokio::join!(lambda, firehose);
    }

    /// Forgets checkpoints whose reader is gone: a deleted delivery stream or
    /// event source mapping never polls again, so nothing else would remove them.
    /// Candidates are listed before their owners are read: a reader created
    /// meanwhile cannot have its new checkpoint mistaken for an orphan.
    async fn sweep_orphaned_checkpoints(&self) {
        let keys: Vec<String> = self
            .kinesis_state
            .read()
            .iter_regional()
            .flat_map(|(_, _, kinesis)| kinesis.lambda_checkpoints.keys().cloned())
            .collect();
        if keys.is_empty() {
            return;
        }
        let mappings: HashSet<String> = self
            .lambda_state
            .read()
            .iter_regional()
            .flat_map(|(_, _, lambda)| lambda.event_source_mappings.keys().cloned())
            .collect();
        let delivery_streams: Option<HashSet<String>> =
            self.firehose.as_ref().map(|(firehose_state, _)| {
                collect_sources(firehose_state)
                    .into_iter()
                    .map(|source| source.checkpoint_id)
                    .collect()
            });
        let orphans: HashSet<String> = keys
            .into_iter()
            .filter(|key| is_orphaned_checkpoint(key, &mappings, delivery_streams.as_ref()))
            .collect();
        if orphans.is_empty() {
            return;
        }
        for (_, _, kinesis) in self.kinesis_state.write().iter_regional_mut() {
            kinesis
                .lambda_checkpoints
                .retain(|key, _| !orphans.contains(key));
            kinesis
                .lambda_checkpoint_streams
                .retain(|key, _| !orphans.contains(key));
        }
        self.persist().await;
    }

    /// Snapshots from before checkpoints named their stream restore entries with
    /// no owner, which deleting a stream would not forget. Name the stream each
    /// one's mapping reads, and drop those whose mapping or stream is not here:
    /// they would apply their old offset to a stream recreated later. The binary
    /// that wrote them also kept them across a delete and recreate of the stream,
    /// so each is checked against the incarnation there now.
    async fn settle_restored_checkpoints(&self) {
        let mapped: HashMap<String, (String, DateTime<Utc>)> = self
            .lambda_state
            .read()
            .iter_regional()
            .flat_map(|(_, _, lambda)| lambda.event_source_mappings.values())
            .map(|m| {
                (
                    m.uuid.clone(),
                    (m.event_source_arn.clone(), m.last_modified),
                )
            })
            .collect();
        let mut settled = false;
        for (_, _, kinesis) in self.kinesis_state.write().iter_regional_mut() {
            let ownerless: Vec<String> = kinesis
                .lambda_checkpoints
                .keys()
                .filter(|key| !kinesis.lambda_checkpoint_streams.contains_key(*key))
                .cloned()
                .collect();
            for key in ownerless {
                settled = true;
                let Some((uuid, shard_id)) = key.rsplit_once(':') else {
                    kinesis.lambda_checkpoints.remove(&key);
                    continue;
                };
                let owner = mapped.get(uuid).and_then(|(arn, last_modified)| {
                    let stream = kinesis.streams.values().find(|s| s.stream_arn == *arn)?;
                    let predates = *last_modified < stream.stream_creation_timestamp;
                    let shard = stream.shards.iter().find(|s| s.shard_id == shard_id);
                    Some((arn.clone(), predates, shard.map(|s| s.records.len())))
                });
                let offset = kinesis.lambda_checkpoints.get(&key).copied().unwrap_or(0);
                match owner.map(|(arn, predates, records)| {
                    (arn, restored_position(offset, predates, records))
                }) {
                    Some((arn, Some(position))) => {
                        kinesis.lambda_checkpoints.insert(key.clone(), position);
                        kinesis.lambda_checkpoint_streams.insert(key, arn);
                    }
                    _ => {
                        kinesis.lambda_checkpoints.remove(&key);
                    }
                }
            }
        }
        if settled {
            self.persist().await;
        }
    }

    /// Reads from each shard of `stream_arn` within `limits`, starting at the
    /// shard's checkpoint under `checkpoint_id`, at the index `resume(shard)`
    /// returns when that is further, and past what retention has expired even if
    /// no write has trimmed it yet, as GetRecords does. A shard with no checkpoint
    /// yet is seeded at `seed(&shard.records)`. Also reports whether any
    /// checkpoint was seeded.
    fn read_shards(
        &self,
        stream_arn: &str,
        checkpoint_id: &str,
        limits: ReadLimits,
        resume: impl Fn(&KinesisShard) -> Option<usize>,
        seed: impl Fn(&[KinesisRecord]) -> usize,
    ) -> (Vec<ShardRead>, bool) {
        let mut kinesis_accounts = self.kinesis_state.write();
        let Some(kinesis) = kinesis_accounts.by_arn_mut(stream_arn) else {
            return (Vec::new(), false);
        };
        let Some(stream) = kinesis
            .streams
            .values()
            .find(|s| s.stream_arn == stream_arn)
        else {
            return (Vec::new(), false);
        };
        if limits
            .generation
            .is_some_and(|generation| generation != stream_generation(stream))
        {
            return (Vec::new(), false);
        }
        let mut pending = Vec::new();
        let mut seeds = Vec::new();
        let cutoff = Utc::now() - chrono::Duration::hours(i64::from(stream.retention_period_hours));
        for shard in &stream.shards {
            let key = format!("{}:{}", checkpoint_id, shard.shard_id);
            let start = match kinesis.lambda_checkpoints.get(&key) {
                Some(offset) => *offset,
                None => {
                    let offset = seed(&shard.records);
                    seeds.push((shard.shard_id.clone(), offset));
                    offset
                }
            };
            let live = shard
                .records
                .partition_point(|r| r.approximate_arrival_timestamp < cutoff);
            let start = start.max(resume(shard).unwrap_or(0)).max(live);
            if start < shard.records.len() {
                pending.push((shard, start));
            }
        }
        // Oldest unread record first, so a tight budget reaches every shard
        // before retention expires what it holds.
        pending.sort_by_key(|(shard, start)| shard.records[*start].approximate_arrival_timestamp);
        let mut reads = Vec::new();
        let mut total_left = limits.total_bytes;
        for (position, (shard, start)) in pending.iter().enumerate() {
            if total_left == 0 {
                break;
            }
            let start = *start;
            let mut end = shard
                .records
                .len()
                .min(start.saturating_add(limits.records));
            let share = total_left / (pending.len() - position);
            let max_bytes = limits.shard_bytes.min(share);
            let size = |r: &KinesisRecord| r.data.len() + r.partition_key.len();
            let mut bytes = 0;
            if let Some(over) = shard.records[start..end].iter().position(|r| {
                bytes += size(r);
                bytes > max_bytes
            }) {
                // A first record past this shard's share is still read if the
                // budget holds it. One that does not fit waits for a later read,
                // where its age puts its shard first, unless it could never fit.
                let first = size(&shard.records[start]);
                let readable = first <= total_left
                    || (first > limits.total_bytes && total_left == limits.total_bytes);
                if over == 0 && !readable {
                    continue;
                }
                end = start + over.max(1);
            }
            let taken: usize = shard.records[start..end].iter().map(size).sum();
            total_left = total_left.saturating_sub(taken);
            reads.push(ShardRead {
                shard_id: shard.shard_id.clone(),
                start,
                end,
                records: shard.records[start..end].to_vec(),
                trimmed: trimmed(shard),
            });
        }
        let seeded = !seeds.is_empty();
        for (shard_id, offset) in seeds {
            kinesis.set_lambda_checkpoint(stream_arn, checkpoint_id, &shard_id, offset);
        }
        (reads, seeded)
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
            .iter_regional()
            .flat_map(|(_, _, lambda)| {
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
        let limit = mapping.batch_size.max(1) as usize;
        let (deliveries, _) = self.read_shards(
            &mapping.stream_arn,
            &mapping.uuid,
            ReadLimits {
                records: limit,
                shard_bytes: usize::MAX,
                total_bytes: usize::MAX,
                generation: None,
            },
            |_| None,
            |records| {
                match mapping
                    .starting_position
                    .as_deref()
                    .unwrap_or("TRIM_HORIZON")
                {
                    "LATEST" => records.len(),
                    "AT_TIMESTAMP" => {
                        let target = mapping
                            .starting_position_timestamp
                            .map(|t| t as i64)
                            .unwrap_or(0);
                        records
                            .iter()
                            .position(|r| r.approximate_arrival_timestamp.timestamp() >= target)
                            .unwrap_or(records.len())
                    }
                    _ => 0, // TRIM_HORIZON
                }
            },
        );

        for ShardRead {
            shard_id,
            start,
            end,
            records,
            ..
        } in deliveries
        {
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
                    kinesis.set_lambda_checkpoint(
                        &mapping.stream_arn,
                        &mapping.uuid,
                        &shard_id,
                        end,
                    );
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
                kinesis.set_lambda_checkpoint(
                    &mapping.stream_arn,
                    &mapping.uuid,
                    &shard_id,
                    new_checkpoint,
                );
            }
            // Persist the advanced checkpoint so a restart resumes past the
            // records this batch already delivered.
            self.persist().await;

            if !used_real_delivery {
                // Recorded in the account and region the function ARN names.
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
                    source: "aws:kinesis".to_string(),
                });
            }
        }
    }

    async fn poll_firehose(&self) {
        let Some((firehose_state, engine)) = &self.firehose else {
            return;
        };
        let sources = collect_sources(firehose_state);
        self.firehose_refusals
            .lock()
            .retain(|key, _| sources.iter().any(|source| source.key == *key));
        for source in sources {
            self.process_source(engine, &source).await;
        }
    }

    /// Saves what the Firehose engine committed since the last poll. Run after
    /// its shutdown drain too, or a graceful restart redelivers everything the
    /// drain just delivered.
    pub async fn sync_firehose_checkpoints(&self) {
        let Some((firehose_state, engine)) = &self.firehose else {
            return;
        };
        let mut advanced = false;
        for source in collect_sources(firehose_state) {
            if let Some((generation, _)) = self.source_stream(&source.source_arn) {
                advanced |= self.commit_source(engine, &source, generation);
            }
        }
        if advanced {
            self.persist().await;
        }
    }

    /// Identifies this incarnation of a source stream, and how long it retains
    /// records. A recreated stream reuses its shard IDs and sequence numbers, so
    /// positions must not carry over.
    fn source_stream(&self, source_arn: &str) -> Option<(i64, Duration)> {
        let accounts = self.kinesis_state.read();
        let stream = accounts
            .by_arn(source_arn)?
            .streams
            .values()
            .find(|stream| stream.stream_arn == source_arn)?;
        let generation = stream_generation(stream);
        let hours = u64::try_from(stream.retention_period_hours).unwrap_or_default();
        Some((generation, Duration::from_secs(hours * 60 * 60)))
    }

    fn note_refusal(&self, source: &Source, refused: Option<&String>) {
        let mut refusals = self.firehose_refusals.lock();
        let Some(reason) = refused else {
            refusals.remove(&source.key);
            return;
        };
        if refusals.get(&source.key) != Some(reason) {
            tracing::warn!(stream = ?source.key.name, source_arn = %source.source_arn, ?reason,
                "Kinesis->Firehose: not reading the source while the destination refuses records");
            refusals.insert(source.key.clone(), reason.clone());
        }
    }

    async fn process_source(&self, engine: &FirehoseEngine, source: &Source) {
        let stream = self.source_stream(&source.source_arn);
        engine.limit_source_retries(
            &source.key,
            SourceLimit {
                retention: stream.map_or(Duration::ZERO, |(_, retention)| retention),
                current_generation: stream.map(|(generation, _)| generation),
            },
        );
        let Some((generation, _)) = stream else {
            return;
        };
        let progress = engine.source_progress(&source.key);
        self.note_refusal(source, progress.refused.as_ref());
        let (reads, seeded) = if progress.backlogged || progress.refused.is_some() {
            (Vec::new(), false)
        } else {
            self.read_shards(
                &source.source_arn,
                &source_checkpoint_id(source, generation),
                ReadLimits {
                    records: MAX_RECORDS_PER_READ,
                    shard_bytes: MAX_BYTES_PER_READ,
                    total_bytes: progress.remaining_bytes,
                    generation: Some(generation),
                },
                |shard| {
                    let read = progress
                        .read
                        .get(&source_shard_key(generation, &shard.shard_id))?;
                    Some(read.saturating_sub(trimmed(shard)))
                },
                |records| {
                    records
                        .iter()
                        .position(|r| r.approximate_arrival_timestamp >= source.created_at)
                        .unwrap_or(records.len())
                },
            )
        };

        for ShardRead {
            shard_id,
            end,
            records,
            trimmed,
            ..
        } in reads
        {
            let read = SourceRead {
                generation,
                records: records
                    .iter()
                    .map(|r| SourceRecord {
                        data: Bytes::copy_from_slice(&r.data),
                        // As recorded: the sequence number and a subsequence,
                        // stable across re-reads.
                        document_id: format!("{}.0", r.sequence_number),
                        arrival: r.approximate_arrival_timestamp,
                    })
                    .collect(),
                shard_id: source_shard_key(generation, &shard_id),
                end: end + trimmed,
            };
            if let Err(error) = engine.enqueue_from_source(&source.key, read) {
                tracing::warn!(stream = ?source.key.name, source_arn = %source.source_arn,
                    shard_id = %shard_id, error = ?error.message(),
                    "Kinesis->Firehose: records not admitted; will retry");
            }
        }

        let advanced = self.commit_source(engine, source, generation);
        if seeded || advanced {
            self.persist().await;
        }
    }

    /// Moves the source's checkpoints up to what the engine has committed and
    /// reports whether any moved.
    fn commit_source(&self, engine: &FirehoseEngine, source: &Source, generation: i64) -> bool {
        let committed = engine.source_progress(&source.key).committed;
        let checkpoint_id = source_checkpoint_id(source, generation);
        let mut advanced = false;
        if let Some(kinesis) = self.kinesis_state.write().by_arn_mut(&source.source_arn) {
            let shards: Vec<(String, usize)> = kinesis
                .streams
                .values()
                .find(|s| s.stream_arn == source.source_arn)
                .map(|s| {
                    s.shards
                        .iter()
                        .map(|shard| (shard.shard_id.clone(), trimmed(shard)))
                        .collect()
                })
                .unwrap_or_default();
            for (shard_id, trimmed) in shards {
                let Some(committed) = committed.get(&source_shard_key(generation, &shard_id))
                else {
                    continue;
                };
                let end = committed.saturating_sub(trimmed);
                if end > kinesis.lambda_checkpoint(&checkpoint_id, &shard_id) {
                    kinesis.set_lambda_checkpoint(
                        &source.source_arn,
                        &checkpoint_id,
                        &shard_id,
                        end,
                    );
                    advanced = true;
                }
            }
        }
        advanced
    }
}

/// Whether nothing reads a checkpoint any more. Delivery stream checkpoints are
/// `<stream ARN>@<created>@<source generation>:<shard>`, and mapping ones
/// `<uuid>:<shard>`. Without Firehose wired, its checkpoints are left alone.
fn is_orphaned_checkpoint(
    key: &str,
    mappings: &HashSet<String>,
    delivery_streams: Option<&HashSet<String>>,
) -> bool {
    if key.starts_with("arn:") {
        let reader = key.rsplit_once('@').map_or(key, |(reader, _)| reader);
        return delivery_streams.is_some_and(|streams| !streams.contains(reader));
    }
    key.rsplit_once(':')
        .is_some_and(|(uuid, _)| !mappings.contains(uuid))
}

/// Where a mapping resumes from an ownerless restored checkpoint: `None` to seed
/// it afresh from its starting position.
///
/// `predates` says the mapping last changed before the stream there now was
/// created, so that stream was recreated under it and every record in it arrived
/// after the mapping started: the start of the shard loses nothing.
///
/// - Past the shard's end, the offset cannot be this incarnation's.
/// - At the end nothing is unread, so it is harmless whatever its history.
/// - Below the end, a mapping that predates the stream may be skipping records it
///   never saw, so it restarts: at-least-once allows the duplicates. Otherwise the
///   offset is kept: a bare integer below the end is identical for this incarnation
///   and an earlier one, when the mapping was also updated after the recreate.
fn restored_position(offset: usize, predates: bool, records: Option<usize>) -> Option<usize> {
    let records = records?;
    if offset == records || (offset < records && !predates) {
        Some(offset)
    } else if predates {
        Some(0)
    } else {
        None
    }
}

fn source_checkpoint_id(source: &Source, generation: i64) -> String {
    format!("{}@{generation}", source.checkpoint_id)
}

fn source_shard_key(generation: i64, shard_id: &str) -> String {
    format!("{generation}/{shard_id}")
}

fn collect_sources(firehose_state: &SharedFirehoseState) -> Vec<Source> {
    let accounts = firehose_state.read();
    accounts
        .accounts
        .iter()
        .flat_map(|(account, state)| {
            state
                .streams_by_region
                .iter()
                .flat_map(move |(region, streams)| {
                    streams.values().filter_map(move |stream| {
                        let source = stream.kinesis_source.as_ref()?;
                        Some(Source {
                            key: StreamKey {
                                account_id: account.clone(),
                                region: region.clone(),
                                name: stream.name.clone(),
                                instance_id: stream.instance_id,
                            },
                            source_arn: source.kinesis_stream_arn.clone(),
                            // Keyed by creation time too, so a recreated stream
                            // starts fresh instead of resuming the old position.
                            checkpoint_id: format!(
                                "{}@{}",
                                stream.arn,
                                stream.created_at.timestamp_nanos_opt().unwrap_or_default()
                            ),
                            created_at: stream.created_at,
                        })
                    })
                })
        })
        .collect()
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
        use fakecloud_core::multi_account::{MultiAccountState, MultiRegionState};
        use fakecloud_lambda::{LambdaFunction, LambdaState};
        use parking_lot::RwLock;

        let account = "444455556666";
        let region = "cn-north-1";
        let role = format!("arn:aws-cn:iam::{account}:role/service-role/orders-consumer");
        let fn_arn = format!("arn:aws-cn:lambda:{region}:{account}:function:orders");
        let stream_arn = format!("arn:aws-cn:kinesis:{region}:{account}:stream/orders");

        let mut lambda: MultiRegionState<LambdaState> =
            MultiRegionState::new(account, region, "http://localhost:4566");
        {
            let l = lambda.regional_mut(account, region);
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
        let poller = KinesisLambdaFirehosePoller::new(kinesis, Arc::new(RwLock::new(lambda)));

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
        let poller = KinesisLambdaFirehosePoller::new(
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

    mod firehose {
        use super::*;
        use fakecloud_core::delivery::KinesisDelivery;
        use fakecloud_core::multi_account::MultiAccountState;
        use parking_lot::RwLock;

        const ACCOUNT: &str = "123456789012";
        const REGION: &str = "us-east-1";
        const SHARD: &str = "shardId-000000000000";

        fn stream_arn() -> String {
            format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/orders")
        }

        struct Fixture {
            kinesis: SharedKinesisState,
            firehose: SharedFirehoseState,
            s3: fakecloud_s3::SharedS3State,
            engine: Arc<FirehoseEngine>,
            poller: KinesisLambdaFirehosePoller,
        }

        impl Fixture {
            fn new(extra_destinations: Value) -> Self {
                let kinesis: SharedKinesisState = Arc::new(RwLock::new(MultiAccountState::new(
                    ACCOUNT,
                    REGION,
                    "http://localhost:4566",
                )));
                let firehose: SharedFirehoseState =
                    Arc::new(RwLock::new(fakecloud_firehose::FirehoseAccounts::new()));
                let mut s3 = fakecloud_s3::S3State::new(ACCOUNT, REGION);
                s3.buckets.insert(
                    "bucket".into(),
                    fakecloud_s3::S3Bucket::new("bucket", REGION, "owner"),
                );
                let mut s3_accounts =
                    MultiAccountState::new(ACCOUNT, REGION, "http://localhost:4566");
                *s3_accounts.get_or_create(ACCOUNT) = s3;
                let s3: fakecloud_s3::SharedS3State = Arc::new(RwLock::new(s3_accounts));
                let engine = Arc::new(FirehoseEngine::new(firehose.clone()));
                engine.set_s3(s3.clone());
                let stream: fakecloud_firehose::DeliveryStream = serde_json::from_value(json!({
                    "name": "fh",
                    "arn": format!("arn:aws:firehose:{REGION}:{ACCOUNT}:deliverystream/fh"),
                    "status": "ACTIVE",
                    "stream_type": "KinesisStreamAsSource",
                    "created_at": "2020-01-01T00:00:00Z",
                    "last_update": "2020-01-01T00:00:00Z",
                    "version_id": "1",
                    "destination": {
                        "destination_id": "destinationId-000000000001",
                        "role_arn": "role",
                        "bucket_arn": "arn:aws:s3:::bucket",
                        "buffering_size_mb": 1,
                        "buffering_interval_seconds": 300,
                    },
                    "tags": {},
                    "extra_destinations": extra_destinations,
                    "kinesis_source": {
                        "kinesis_stream_arn": stream_arn(),
                        "role_arn": "role",
                    },
                }))
                .unwrap();
                firehose
                    .write()
                    .get_or_create(ACCOUNT, REGION)
                    .streams_mut(REGION)
                    .insert("fh".into(), stream);
                let lambda: SharedLambdaState = Arc::new(RwLock::new(MultiAccountState::new(
                    ACCOUNT,
                    REGION,
                    "http://localhost:4566",
                )));
                let poller = KinesisLambdaFirehosePoller::new(kinesis.clone(), lambda)
                    .with_firehose(firehose.clone(), engine.clone());
                let fixture = Self {
                    kinesis,
                    firehose,
                    s3,
                    engine,
                    poller,
                };
                fixture.create_kinesis_stream(Utc::now());
                fixture
            }

            fn create_kinesis_stream(&self, created: DateTime<Utc>) {
                self.kinesis
                    .write()
                    .regional_mut(ACCOUNT, REGION)
                    .streams
                    .insert(
                        "orders".to_string(),
                        fakecloud_kinesis::KinesisStream {
                            stream_name: "orders".to_string(),
                            stream_arn: stream_arn(),
                            stream_status: "ACTIVE".to_string(),
                            stream_creation_timestamp: created,
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

            fn put(&self) {
                fakecloud_kinesis::delivery::KinesisDeliveryImpl::new(self.kinesis.clone())
                    .put_record(&stream_arn(), "aGVsbG8=", "pk");
            }

            fn source(&self) -> Source {
                collect_sources(&self.firehose).remove(0)
            }

            async fn poll(&self) {
                self.poller
                    .process_source(&self.engine, &self.source())
                    .await;
            }

            fn generation(&self) -> i64 {
                self.poller.source_stream(&stream_arn()).unwrap().0
            }

            fn checkpoint(&self) -> Option<usize> {
                let id = source_checkpoint_id(&self.source(), self.generation());
                self.kinesis
                    .read()
                    .regional(ACCOUNT, REGION)
                    .unwrap()
                    .lambda_checkpoints
                    .get(&format!("{id}:{SHARD}"))
                    .copied()
            }

            fn read_position(&self) -> Option<usize> {
                self.engine
                    .source_progress(&self.source().key)
                    .read
                    .get(&source_shard_key(self.generation(), SHARD))
                    .copied()
            }

            fn delivered_objects(&self) -> usize {
                self.s3.read().get(ACCOUNT).unwrap().buckets["bucket"]
                    .objects
                    .len()
            }
        }

        /// A source stream deleted and recreated under its name restarts its
        /// shard IDs and sequence numbers: records in the new one must not be
        /// skipped as if the old position still applied.
        #[tokio::test]
        async fn recreated_source_stream_is_read_from_its_start() {
            let f = Fixture::new(json!({}));
            for _ in 0..3 {
                f.put();
            }
            f.poll().await;
            assert_eq!(f.read_position(), Some(4), "three records past position 1");

            f.create_kinesis_stream(Utc::now() + chrono::Duration::seconds(1));
            f.put();
            f.poll().await;
            assert_eq!(
                f.read_position(),
                Some(2),
                "the new stream's one record was read"
            );

            f.engine.shutdown().await;
            let accounts = f.s3.read();
            let objects = &accounts.get(ACCOUNT).unwrap().buckets["bucket"].objects;
            let mut bodies: Vec<&[u8]> = objects
                .values()
                .map(|object| {
                    let fakecloud_persistence::BodyRef::Memory(body) = &object.body else {
                        panic!("memory store");
                    };
                    body.as_ref()
                })
                .collect();
            bodies.sort_by_key(|body| body.len());
            assert_eq!(
                bodies,
                vec![&b"hello"[..], &b"hellohellohello"[..]],
                "each stream incarnation has its own batch"
            );
        }

        fn shard_records(f: &Fixture, ages: &[i64]) {
            let mut accounts = f.kinesis.write();
            let stream = accounts
                .regional_mut(ACCOUNT, REGION)
                .streams
                .get_mut("orders")
                .unwrap();
            let shard = &mut stream.shards[0];
            for age in ages {
                shard.records.push(KinesisRecord {
                    sequence_number: shard.next_sequence_number.to_string(),
                    partition_key: "pk".into(),
                    data: b"x".to_vec(),
                    approximate_arrival_timestamp: Utc::now() - chrono::Duration::hours(*age),
                });
                shard.next_sequence_number += 1;
            }
        }

        fn read_everything(f: &Fixture, checkpoint: &str, limits: ReadLimits) -> Vec<ShardRead> {
            f.poller
                .read_shards(&stream_arn(), checkpoint, limits, |_| None, |_| 0)
                .0
        }

        const NO_LIMIT: ReadLimits = ReadLimits {
            records: usize::MAX,
            shard_bytes: usize::MAX,
            total_bytes: usize::MAX,
            generation: None,
        };

        /// Records past retention are not returned even when no write has
        /// trimmed them, like GetRecords.
        #[tokio::test]
        async fn reads_skip_records_past_retention_before_they_are_trimmed() {
            let f = Fixture::new(json!({}));
            shard_records(&f, &[25, 25, 1]);
            let reads = read_everything(&f, "reader", NO_LIMIT);
            assert_eq!(reads.len(), 1);
            assert_eq!(reads[0].start, 2);
            assert_eq!(reads[0].records.len(), 1);

            let mut accounts = f.kinesis.write();
            let stream = accounts
                .regional_mut(ACCOUNT, REGION)
                .streams
                .get_mut("orders")
                .unwrap();
            stream.shards[0].records.truncate(2);
            drop(accounts);
            assert!(
                read_everything(&f, "reader-2", NO_LIMIT).is_empty(),
                "all expired"
            );
        }

        /// One read stays within the budget across shards, however many there
        /// are, and every shard with records gets a share of it.
        #[tokio::test]
        async fn a_tight_budget_reaches_every_shard_oldest_first() {
            let f = Fixture::new(json!({}));
            let size = 1000;
            let base = Utc::now() - chrono::Duration::minutes(1);
            {
                let mut accounts = f.kinesis.write();
                let stream = accounts
                    .regional_mut(ACCOUNT, REGION)
                    .streams
                    .get_mut("orders")
                    .unwrap();
                stream.shards = fakecloud_kinesis::build_stream_shards(4);
                for shard in &mut stream.shards {
                    for age in 0..4 {
                        shard.records.push(KinesisRecord {
                            sequence_number: shard.next_sequence_number.to_string(),
                            partition_key: String::new(),
                            data: vec![b'x'; size],
                            approximate_arrival_timestamp: base + chrono::Duration::seconds(age),
                        });
                        shard.next_sequence_number += 1;
                    }
                }
            }
            let limits = ReadLimits {
                records: usize::MAX,
                shard_bytes: usize::MAX,
                total_bytes: size,
                generation: None,
            };
            let mut read: HashMap<String, usize> = HashMap::new();
            let mut order = Vec::new();
            for _ in 0..4 {
                let (reads, _) = f.poller.read_shards(
                    &stream_arn(),
                    "tight",
                    limits,
                    |shard| read.get(&shard.shard_id).copied(),
                    |_| 0,
                );
                assert_eq!(reads.len(), 1);
                read.insert(reads[0].shard_id.clone(), reads[0].end);
                order.push(reads[0].shard_id.clone());
            }
            order.sort();
            order.dedup();
            assert_eq!(order.len(), 4, "every shard was read: {order:?}");
        }

        #[tokio::test]
        async fn a_read_for_another_stream_incarnation_takes_nothing() {
            let f = Fixture::new(json!({}));
            f.kinesis
                .write()
                .regional_mut(ACCOUNT, REGION)
                .streams
                .get_mut("orders")
                .unwrap()
                .shards[0]
                .records
                .push(KinesisRecord {
                    sequence_number: "1".to_string(),
                    partition_key: String::new(),
                    data: b"x".to_vec(),
                    approximate_arrival_timestamp: Utc::now(),
                });
            let generation = f.poller.source_stream(&stream_arn()).unwrap().0;
            let read = |generation| {
                f.poller.read_shards(
                    &stream_arn(),
                    "incarnation",
                    ReadLimits {
                        records: usize::MAX,
                        shard_bytes: usize::MAX,
                        total_bytes: usize::MAX,
                        generation: Some(generation),
                    },
                    |_| None,
                    |_| 0,
                )
            };
            let (reads, seeded) = read(generation - 1);
            assert!(reads.is_empty());
            assert!(!seeded, "a stale read leaves no checkpoint behind");
            assert_eq!(read(generation).0.len(), 1);
        }

        #[tokio::test]
        async fn reads_share_a_byte_budget_across_shards() {
            let f = Fixture::new(json!({}));
            let size = 1000;
            {
                let mut accounts = f.kinesis.write();
                let stream = accounts
                    .regional_mut(ACCOUNT, REGION)
                    .streams
                    .get_mut("orders")
                    .unwrap();
                stream.shards = fakecloud_kinesis::build_stream_shards(4);
                for shard in &mut stream.shards {
                    for _ in 0..10 {
                        shard.records.push(KinesisRecord {
                            sequence_number: shard.next_sequence_number.to_string(),
                            partition_key: String::new(),
                            data: vec![b'x'; size],
                            approximate_arrival_timestamp: Utc::now(),
                        });
                        shard.next_sequence_number += 1;
                    }
                }
            }
            let reads = read_everything(
                &f,
                "reader",
                ReadLimits {
                    records: usize::MAX,
                    shard_bytes: usize::MAX,
                    total_bytes: 8 * size,
                    generation: None,
                },
            );
            let taken: Vec<usize> = reads.iter().map(|read| read.records.len()).collect();
            assert_eq!(taken, vec![2, 2, 2, 2]);

            let reads = read_everything(
                &f,
                "reader-2",
                ReadLimits {
                    records: usize::MAX,
                    shard_bytes: usize::MAX,
                    total_bytes: size,
                    generation: None,
                },
            );
            let taken: usize = reads.iter().map(|read| read.records.len()).sum();
            assert_eq!(taken, 1, "within one record of a nearly spent budget");

            // Unread shards are still seeded.
            let accounts = f.kinesis.read();
            let state = accounts.regional(ACCOUNT, REGION).unwrap();
            let seeded = state
                .lambda_checkpoints
                .keys()
                .filter(|key| key.starts_with("reader-2:"))
                .count();
            assert_eq!(seeded, 4);
        }

        /// No read passes its budget, however small the shares, except
        /// one record larger than all of it.
        #[tokio::test]
        async fn a_read_never_passes_its_byte_budget() {
            let f = Fixture::new(json!({}));
            let base = Utc::now() - chrono::Duration::minutes(1);
            {
                let mut accounts = f.kinesis.write();
                let stream = accounts
                    .regional_mut(ACCOUNT, REGION)
                    .streams
                    .get_mut("orders")
                    .unwrap();
                stream.shards = fakecloud_kinesis::build_stream_shards(3);
                for (age, (shard, size)) in
                    stream.shards.iter_mut().zip([600, 600, 600]).enumerate()
                {
                    shard.records.push(KinesisRecord {
                        sequence_number: "1".to_string(),
                        partition_key: String::new(),
                        data: vec![b'x'; size],
                        approximate_arrival_timestamp: base + chrono::Duration::seconds(age as i64),
                    });
                }
            }
            let budget = |total_bytes| ReadLimits {
                records: usize::MAX,
                shard_bytes: usize::MAX,
                total_bytes,
                generation: None,
            };
            // Each record is past its share, and only one fits the budget.
            let reads = read_everything(&f, "tight", budget(1000));
            let read: usize = reads.iter().map(|r| r.records.len()).sum();
            assert_eq!(read, 1, "taking one per shard would read 1800 of 1000");
            assert_eq!(reads[0].shard_id, "shardId-000000000000", "oldest first");

            // A record larger than the budget is read alone, or never.
            let reads = read_everything(&f, "oversized", budget(100));
            let read: usize = reads.iter().map(|r| r.records.len()).sum();
            assert_eq!(read, 1);
        }

        /// A deleted delivery stream's checkpoints go with it, and a live one's stay.
        #[tokio::test]
        async fn sweep_forgets_checkpoints_of_deleted_delivery_streams() {
            let f = Fixture::new(json!({}));
            f.put();
            f.poll().await;
            assert_eq!(f.checkpoint(), Some(0));
            f.poller.sweep_orphaned_checkpoints().await;
            assert_eq!(f.checkpoint(), Some(0), "its reader is alive");
            f.firehose
                .write()
                .get_or_create(ACCOUNT, REGION)
                .streams_mut(REGION)
                .remove("fh");
            f.poller.sweep_orphaned_checkpoints().await;
            let accounts = f.kinesis.read();
            let state = accounts.regional(ACCOUNT, REGION).unwrap();
            assert!(state.lambda_checkpoints.is_empty());
            assert!(state.lambda_checkpoint_streams.is_empty());
        }

        /// A mapping that is gone, enabled or not when it went, leaves nothing behind.
        #[tokio::test]
        async fn sweep_forgets_checkpoints_of_deleted_mappings() {
            let Fixture { kinesis, .. } = Fixture::new(json!({}));
            let lambda: SharedLambdaState = Arc::new(RwLock::new(MultiAccountState::new(
                ACCOUNT,
                REGION,
                "http://localhost:4566",
            )));
            let function = format!("arn:aws:lambda:{REGION}:{ACCOUNT}:function:f");
            let mut disabled = esm("disabled", &function, &stream_arn());
            disabled.enabled = false;
            lambda
                .write()
                .regional_mut(ACCOUNT, REGION)
                .event_source_mappings
                .insert("disabled".into(), disabled);
            for uuid in ["disabled", "deleted"] {
                kinesis
                    .write()
                    .regional_mut(ACCOUNT, REGION)
                    .set_lambda_checkpoint(&stream_arn(), uuid, SHARD, 3);
            }
            KinesisLambdaFirehosePoller::new(kinesis.clone(), lambda)
                .sweep_orphaned_checkpoints()
                .await;
            let accounts = kinesis.read();
            let state = accounts.regional(ACCOUNT, REGION).unwrap();
            assert_eq!(state.lambda_checkpoints.len(), 1);
            assert_eq!(state.lambda_checkpoint("disabled", SHARD), 3);
            assert_eq!(state.lambda_checkpoint_streams.len(), 1);
        }

        /// The clamp at the retention boundary reaches Lambda mappings: an expired
        /// record no write has trimmed yet is neither invoked with nor re-read, and
        /// the checkpoint moves past it.
        #[tokio::test]
        async fn retention_clamp_skips_expired_records_for_lambda_mappings() {
            let f = Fixture::new(json!({}));
            shard_records(&f, &[25, 25, 1]);
            let delivery = Arc::new(RecordingDelivery(parking_lot::Mutex::new(Vec::new())));
            let lambda: SharedLambdaState = Arc::new(RwLock::new(MultiAccountState::new(
                ACCOUNT,
                REGION,
                "http://localhost:4566",
            )));
            let poller = KinesisLambdaFirehosePoller::new(f.kinesis.clone(), lambda)
                .with_lambda_delivery(delivery.clone());
            let mapping = Mapping {
                uuid: "esm".to_string(),
                function_arn: format!("arn:aws:lambda:{REGION}:{ACCOUNT}:function:f"),
                stream_arn: stream_arn(),
                batch_size: 10,
                filter: FilterSet::from_strings(std::iter::empty::<&String>()),
                starting_position: Some("TRIM_HORIZON".to_string()),
                starting_position_timestamp: None,
                report_batch_item_failures: false,
                invoke_identity_arn: None,
            };
            poller.process_mapping(&mapping).await;
            let payloads = delivery.0.lock().clone();
            assert_eq!(payloads.len(), 1);
            let event: Value = serde_json::from_str(&payloads[0]).unwrap();
            assert_eq!(
                event["Records"].as_array().unwrap().len(),
                1,
                "only the live one"
            );
            assert_eq!(
                f.kinesis
                    .read()
                    .regional(ACCOUNT, REGION)
                    .unwrap()
                    .lambda_checkpoint("esm", SHARD),
                3
            );
        }

        /// The handoff carries each record's Kinesis arrival, not the read time.
        #[tokio::test]
        async fn source_handoff_carries_kinesis_arrival_times() {
            let f = Fixture::new(json!({}));
            shard_records(&f, &[2]);
            let arrival = f.kinesis.read().regional(ACCOUNT, REGION).unwrap().streams["orders"]
                .shards[0]
                .records[0]
                .approximate_arrival_timestamp;
            f.poll().await;
            f.engine.shutdown().await;
            let accounts = f.s3.read();
            let objects = &accounts.get(ACCOUNT).unwrap().buckets["bucket"].objects;
            let key = objects.keys().next().unwrap();
            assert!(
                key.starts_with(&arrival.format("%Y/%m/%d/%H").to_string()),
                "{key}"
            );
        }

        /// Refusal bookkeeping goes with the stream, or repeated create and
        /// delete of refused streams would grow it forever.
        #[tokio::test]
        async fn refusals_are_forgotten_with_their_delivery_stream() {
            let f = Fixture::new(json!({"SplunkDestinationDescription": {}}));
            f.poller.poll_firehose().await;
            assert_eq!(f.poller.firehose_refusals.lock().len(), 1);
            f.firehose
                .write()
                .get_or_create(ACCOUNT, REGION)
                .streams_mut(REGION)
                .remove("fh");
            f.poller.poll_firehose().await;
            assert!(f.poller.firehose_refusals.lock().is_empty());
        }

        /// The shutdown drain's commits reach the checkpoint, or every graceful
        /// restart would deliver the drained records a second time.
        #[tokio::test]
        async fn checkpoint_sync_after_shutdown_saves_the_drained_position() {
            let f = Fixture::new(json!({}));
            f.put();
            f.poll().await;
            assert_eq!(
                f.checkpoint(),
                Some(0),
                "buffered records are not committed"
            );

            f.engine.shutdown().await;
            assert_eq!(f.delivered_objects(), 1);
            f.poller.sync_firehose_checkpoints().await;
            assert_eq!(f.checkpoint(), Some(1));
        }

        /// A destination that refuses records leaves them unread and unseeded:
        /// they are delivered once an update makes the destination usable.
        #[tokio::test]
        async fn refusing_destination_pauses_reading_without_seeding() {
            let f = Fixture::new(json!({"SplunkDestinationDescription": {}}));
            f.put();
            f.poll().await;
            assert_eq!(f.checkpoint(), None);
            assert_eq!(f.read_position(), None);
            assert_eq!(f.poller.firehose_refusals.lock().len(), 1);

            f.firehose
                .write()
                .get_or_create(ACCOUNT, REGION)
                .streams_mut(REGION)
                .get_mut("fh")
                .unwrap()
                .extra_destinations
                .clear();
            f.poll().await;
            assert_eq!(f.read_position(), Some(2));
            assert!(f.poller.firehose_refusals.lock().is_empty());
        }

        struct HangingDelivery(Arc<tokio::sync::Notify>);

        impl LambdaDelivery for HangingDelivery {
            fn invoke_lambda(
                &self,
                _function_arn: &str,
                _payload: &str,
            ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>, String>> + Send>>
            {
                self.0.notify_one();
                Box::pin(std::future::pending())
            }
        }

        /// A deleted source ends the retries of batches read from it, and a
        /// recreated one ends those of its earlier incarnation.
        #[tokio::test]
        async fn source_stream_state_bounds_batch_retries() {
            let f = Fixture::new(json!({}));
            let limit = || f.engine.source_progress(&f.source().key).limit.unwrap();
            f.poll().await;
            let generation = f.generation();
            assert_eq!(limit().current_generation, Some(generation));
            assert_eq!(limit().retention, Duration::from_secs(24 * 60 * 60));

            f.create_kinesis_stream(Utc::now() + chrono::Duration::seconds(1));
            f.poll().await;
            assert_ne!(limit().current_generation, Some(generation));

            f.kinesis
                .write()
                .regional_mut(ACCOUNT, REGION)
                .streams
                .remove("orders");
            f.poll().await;
            assert_eq!(limit().current_generation, None);
        }

        /// A checkpoint restored from a snapshot that predates stream ownership
        /// is tied to its mapping's stream at startup, even for a disabled
        /// mapping that is not polling, so deleting the stream forgets it. One
        /// with no mapping or no stream here could only skip records later.
        #[tokio::test]
        async fn restored_checkpoints_are_settled_at_startup() {
            let Fixture { kinesis, .. } = Fixture::new(json!({}));
            let lambda: SharedLambdaState = Arc::new(RwLock::new(MultiAccountState::new(
                ACCOUNT,
                REGION,
                "http://localhost:4566",
            )));
            let function = format!("arn:aws:lambda:{REGION}:{ACCOUNT}:function:f");
            let mut disabled = esm("disabled", &function, &stream_arn());
            disabled.enabled = false;
            let gone_stream = format!("arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/gone");
            {
                let mut accounts = lambda.write();
                let mappings = &mut accounts.regional_mut(ACCOUNT, REGION).event_source_mappings;
                mappings.insert("disabled".into(), disabled);
                mappings.insert("gone".into(), esm("gone", &function, &gone_stream));
            }
            {
                let mut accounts = kinesis.write();
                let state = accounts.regional_mut(ACCOUNT, REGION);
                for uuid in ["disabled", "gone", "unmapped"] {
                    state
                        .lambda_checkpoints
                        .insert(format!("{uuid}:{SHARD}"), 7);
                }
                let stream = state.streams.get_mut("orders").unwrap();
                for n in 0..7 {
                    stream.shards[0].records.push(KinesisRecord {
                        sequence_number: n.to_string(),
                        partition_key: String::new(),
                        data: Vec::new(),
                        approximate_arrival_timestamp: Utc::now(),
                    });
                }
            }
            let poller = KinesisLambdaFirehosePoller::new(kinesis.clone(), lambda);
            poller.settle_restored_checkpoints().await;

            let mut accounts = kinesis.write();
            let state = accounts.regional_mut(ACCOUNT, REGION);
            assert_eq!(state.lambda_checkpoints.len(), 1);
            assert_eq!(state.lambda_checkpoint("disabled", SHARD), 7);
            state.forget_stream_checkpoints(&stream_arn());
            assert!(state.lambda_checkpoints.is_empty());
        }

        /// A function that never answers must not stop Firehose reading: a
        /// record put once the invocation is hanging must still be read.
        #[tokio::test]
        async fn hung_lambda_invocation_does_not_block_firehose_reads() {
            let Fixture {
                kinesis,
                firehose,
                engine,
                ..
            } = Fixture::new(json!({}));
            let lambda: SharedLambdaState = Arc::new(RwLock::new(MultiAccountState::new(
                ACCOUNT,
                REGION,
                "http://localhost:4566",
            )));
            lambda
                .write()
                .regional_mut(ACCOUNT, REGION)
                .event_source_mappings
                .insert(
                    "esm".to_string(),
                    esm(
                        "esm",
                        &format!("arn:aws:lambda:{REGION}:{ACCOUNT}:function:f"),
                        &stream_arn(),
                    ),
                );
            let key = collect_sources(&firehose).remove(0).key;
            let hanging = Arc::new(tokio::sync::Notify::new());
            let poller = Arc::new(
                KinesisLambdaFirehosePoller::new(kinesis.clone(), lambda)
                    .with_lambda_delivery(Arc::new(HangingDelivery(hanging.clone())))
                    .with_firehose(firehose, engine.clone()),
            );
            fakecloud_kinesis::delivery::KinesisDeliveryImpl::new(kinesis).put_record(
                &stream_arn(),
                "aGVsbG8=",
                "pk",
            );
            let running = tokio::spawn(poller.run());
            let read = tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if !engine.source_progress(&key).read.is_empty() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
            .await;
            running.abort();
            assert!(read.is_ok(), "Firehose read nothing behind a hung Lambda");
        }
    }

    /// An ownerless restored checkpoint against the stream that exists now.
    #[test]
    fn restored_checkpoints_of_an_earlier_incarnation_are_not_applied() {
        // No such shard: nothing to resume.
        assert_eq!(restored_position(3, false, None), None);
        // Past the end of the shard: cannot be this incarnation's.
        assert_eq!(restored_position(9, false, Some(4)), None);
        assert_eq!(restored_position(9, true, Some(4)), Some(0));
        // At the end nothing is unread either way.
        assert_eq!(restored_position(4, true, Some(4)), Some(4));
        assert_eq!(restored_position(4, false, Some(4)), Some(4));
        // Below it: the mapping predating the stream shows it was recreated, and
        // everything in it arrived after the mapping started.
        assert_eq!(restored_position(2, true, Some(4)), Some(0));
        assert_eq!(restored_position(2, false, Some(4)), Some(2));
    }

    #[test]
    fn checkpoints_are_orphaned_only_when_their_reader_is_known_to_be_gone() {
        let mappings = HashSet::from(["live".to_string()]);
        let reader = "arn:aws:firehose:us-east-1:1:deliverystream/fh@7";
        let streams = HashSet::from([reader.to_string()]);
        let key = format!("{reader}@9:shardId-000000000000");
        assert!(!is_orphaned_checkpoint(&key, &mappings, Some(&streams)));
        assert!(is_orphaned_checkpoint(
            &key,
            &mappings,
            Some(&HashSet::new())
        ));
        assert!(!is_orphaned_checkpoint(&key, &mappings, None), "not wired");
        assert!(!is_orphaned_checkpoint("live:s", &mappings, None));
        assert!(is_orphaned_checkpoint("gone:s", &mappings, None));
    }

    #[test]
    fn trimmed_keeps_record_positions_stable_across_retention_trim() {
        let mut shard = fakecloud_kinesis::build_stream_shards(1).remove(0);
        for n in 0..3 {
            shard.records.push(KinesisRecord {
                sequence_number: n.to_string(),
                partition_key: "pk".into(),
                data: Vec::new(),
                approximate_arrival_timestamp: Utc::now(),
            });
            shard.next_sequence_number += 1;
        }
        let third = 2 + trimmed(&shard);
        shard.records.drain(0..2);
        assert_eq!(trimmed(&shard), third);
    }

    #[test]
    fn first_failed_index_invalid_json_returns_none() {
        let body = b"not json";
        let seqs = ["a"].iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert!(first_failed_index(body, &seqs).is_none());
    }
}
