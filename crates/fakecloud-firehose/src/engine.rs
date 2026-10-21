use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use fakecloud_core::quota::{
    QuotaProvider, FIREHOSE_MIB_PER_SECOND, FIREHOSE_PUT_REQUESTS_PER_SECOND,
    FIREHOSE_RECORDS_PER_SECOND, FIREHOSE_SERVICE_CODE,
};
use fakecloud_core::service::AwsServiceError;
use fakecloud_persistence::S3Store;
use fakecloud_s3::{delivery::DeliveryWriteOutcome, SharedS3State};
use http::StatusCode;
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio::task::{Id, JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::output::object_key;
use crate::throughput::{ThroughputLimits, ThroughputWindow};
use crate::writer::{BatchWriter, DeliveryBatch, S3BatchWriter};
use crate::{delivery_config, DeliveryConfig, DeliveryStream, SharedFirehoseState, StreamKey};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DrainReport {
    pub delivered_batches: usize,
    pub undelivered_records: usize,
    pub undelivered_bytes: usize,
}

/// Returned by [`FirehoseEngine::start`] when the worker already runs or
/// shutdown has closed admission.
#[derive(Debug, thiserror::Error)]
#[error("Firehose worker has already started or admission is closed")]
pub struct WorkerAlreadyStarted;

/// AWS keeps retrying DirectPut delivery to S3 for up to 24 hours after the
/// data arrives, then discards it.
const MAX_RETENTION: Duration = Duration::from_secs(24 * 60 * 60);

/// Delay before retrying a failed S3 delivery. AWS documents that Firehose
/// retries delivery to S3 every 5 seconds until the 24-hour retention ends
/// ([`MAX_RETENTION`]). Retries still start only on worker ticks.
const RETRY_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct TimePoint {
    monotonic: Instant,
    wall: DateTime<Utc>,
}

trait Clock: Send + Sync {
    fn now(&self) -> TimePoint;
}

struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> TimePoint {
        TimePoint {
            monotonic: Instant::now(),
            wall: Utc::now(),
        }
    }
}

struct ActiveBuffer {
    records: Vec<Bytes>,
    // Decoded record bytes only: size triggers and drain reports never count
    // an `AppendDelimiterToRecord` newline.
    input_bytes: usize,
    first: TimePoint,
}

impl ActiveBuffer {
    /// Nothing to write: no records, or only empty records for a destination
    /// that adds no delimiter. Judged on the payload before any codec runs,
    /// since even GZIP encodes an empty payload into nonempty bytes, while
    /// `AppendDelimiterToRecord` writes a newline for every empty record.
    fn is_empty(&self, destination: &DeliveryConfig) -> bool {
        self.records.is_empty() || (self.input_bytes == 0 && !destination.output.append_delimiter)
    }
}

// The queue retains this Arc while a writer runs, including when its blocking
// task panics. Records/config/key are never reconstructed for a retry.
struct DetachedBatch {
    batch: DeliveryBatch,
    input_bytes: usize,
    // Oldest record's arrival; bounds retries to `MAX_RETENTION`.
    first_arrival: Instant,
}

#[derive(Default)]
struct StreamBuffer {
    active: Option<ActiveBuffer>,
    queue: VecDeque<Arc<DetachedBatch>>,
    in_flight: bool,
    retry_not_before: Option<Instant>,
    // Consecutive failed attempts of the current head, reported in warnings.
    failures: u32,
    // Runtime-only diagnostic latch: the stream version whose unsupported
    // configuration was last reported, so each update warns once rather than
    // on every one-second scan.
    blocked_version: Option<String>,
}

impl StreamBuffer {
    fn detach(
        &mut self,
        key: &StreamKey,
        stream: &DeliveryStream,
        config: DeliveryConfig,
        now: &TimePoint,
        force: bool,
    ) {
        let Some(active) = &self.active else { return };
        if !force
            && active.input_bytes < config.buffering.size_bytes
            && now.monotonic.duration_since(active.first.monotonic) < config.buffering.interval
        {
            return;
        }
        let active = self.active.take().unwrap();
        if active.is_empty(&config) {
            return;
        }
        let object_key = object_key(
            &key.name,
            &stream.version_id,
            &config.output,
            active.first.wall,
            now.wall,
            uuid::Uuid::new_v4(),
        );
        self.queue.push_back(Arc::new(DetachedBatch {
            input_bytes: active.input_bytes,
            first_arrival: active.first.monotonic,
            batch: DeliveryBatch {
                stream: key.clone(),
                version_id: stream.version_id.clone(),
                destination: config,
                records: active.records,
                oldest_arrival: active.first.wall,
                delivery_time: now.wall,
                object_key,
            },
        }));
    }
}

#[derive(Default)]
struct Registry {
    streams: BTreeMap<StreamKey, StreamBuffer>,
    // Ingestion accounting outlives a drained S3 buffer and also covers streams
    // with configuration-only destinations. Never used by delivery retries.
    windows: BTreeMap<StreamKey, ThroughputWindow>,
    // Last assigned stream, not last scanned. A removed generation remains a
    // valid ordering cursor; range selection skips it and wraps naturally.
    last_scheduled: Option<StreamKey>,
    closed: bool,
    drain_delivered: usize,
    report: Option<DrainReport>,
}

struct EngineCore {
    state: SharedFirehoseState,
    registry: Mutex<Registry>,
    clock: Arc<dyn Clock>,
    writer: RwLock<Option<Arc<dyn BatchWriter>>>,
    quota_provider: RwLock<Option<Arc<dyn QuotaProvider>>>,
    store: Arc<RwLock<Option<Arc<dyn S3Store>>>>,
    wake: Arc<Notify>,
}

#[derive(Default)]
struct Worker {
    started: bool,
    handle: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct DrainState {
    worker: Option<JoinHandle<()>>,
    jobs: Writes,
    blocked: BTreeSet<StreamKey>,
}

/// Shared process-local buffering. Construction is inert; `start` owns the
/// ordinary worker lifecycle and `shutdown` is the explicit final drain.
pub struct FirehoseEngine {
    core: Arc<EngineCore>,
    worker: Mutex<Worker>,
    drain: tokio::sync::Mutex<DrainState>,
}

impl FirehoseEngine {
    pub(crate) fn shared_state(&self) -> SharedFirehoseState {
        self.core.state.clone()
    }

    pub fn new(state: SharedFirehoseState) -> Self {
        Self {
            core: Arc::new(EngineCore {
                state,
                registry: Mutex::new(Registry::default()),
                clock: Arc::new(SystemClock),
                writer: RwLock::new(None),
                quota_provider: RwLock::new(None),
                store: Arc::new(RwLock::new(None)),
                wake: Arc::new(Notify::new()),
            }),
            worker: Mutex::new(Worker::default()),
            drain: tokio::sync::Mutex::new(DrainState::default()),
        }
    }

    pub fn set_s3(&self, s3: SharedS3State) {
        *self.core.writer.write() = Some(Arc::new(S3BatchWriter::new(
            self.core.state.clone(),
            s3,
            self.core.store.clone(),
        )));
        self.core.wake.notify_one();
    }

    pub fn set_s3_store(&self, store: Arc<dyn S3Store>) {
        *self.core.store.write() = Some(store);
        self.core.wake.notify_one();
    }

    pub fn set_quota_provider(&self, provider: Arc<dyn QuotaProvider>) {
        *self.core.quota_provider.write() = Some(provider);
    }

    pub fn enqueue(
        &self,
        account: &str,
        region: &str,
        name: &str,
        records: Vec<Bytes>,
    ) -> Result<(), AwsServiceError> {
        self.enqueue_admission(account, region, name, records, None)
            .map(|_| ())
    }

    pub(crate) fn enqueue_put(
        &self,
        account: &str,
        region: &str,
        name: &str,
        records: Vec<Bytes>,
    ) -> Result<Vec<bool>, AwsServiceError> {
        // Provider implementations can consult other services (including
        // Firehose). Release even the provider guard before resolving limits,
        // and never call them under configuration or registry locks.
        let provider = self.core.quota_provider.read().clone();
        let limits = provider.map(|provider| {
            let values = provider.enforced_limits(
                account,
                region,
                FIREHOSE_SERVICE_CODE,
                &[
                    FIREHOSE_PUT_REQUESTS_PER_SECOND,
                    FIREHOSE_RECORDS_PER_SECOND,
                    FIREHOSE_MIB_PER_SECOND,
                ],
            );
            ThroughputLimits {
                requests: values[0],
                records: values[1],
                mib: values[2],
            }
        });
        let limits =
            limits.filter(|l| l.requests.is_some() || l.records.is_some() || l.mib.is_some());
        self.enqueue_admission(account, region, name, records, limits)
    }

    fn enqueue_admission(
        &self,
        account: &str,
        region: &str,
        name: &str,
        records: Vec<Bytes>,
        limits: Option<ThroughputLimits>,
    ) -> Result<Vec<bool>, AwsServiceError> {
        // Every path holding both takes configuration before the registry. Keep
        // the read guard until admission and immediate detachment both commit.
        let config = self.core.state.read();
        let mut registry = self.core.registry.lock();
        if registry.closed {
            return Err(AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "ServiceUnavailableException",
                "Firehose admission is closed",
            ));
        }
        let stream = config
            .get(account)
            .and_then(|s| s.streams(region))
            .and_then(|s| s.get(name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("DeliveryStream {name} not found"),
                )
            })?;
        // Public APIs and internal hooks are DirectPut producers, even without
        // an S3 destination/backend. Preserve the shared source admission guard.
        if stream.stream_type == "KinesisStreamAsSource" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidArgumentException",
                "This operation is not permitted on KinesisStreamAsSource delivery stream type.",
            ));
        }
        // Complete destination preflight before touching usage or buffers.
        let destination = stream_delivery(stream)?;
        let key = StreamKey {
            account_id: account.into(),
            region: region.into(),
            name: name.into(),
            instance_id: stream.instance_id,
        };
        let now = self.core.clock.now();
        let mut accepted = vec![true; records.len()];
        if let Some(limits) = limits {
            let window = registry
                .windows
                .entry(key.clone())
                .or_insert_with(|| ThroughputWindow::new(now.monotonic));
            if !window.try_request(now.monotonic, limits) {
                return Err(throughput_error());
            }
            for (admitted, record) in accepted.iter_mut().zip(&records) {
                *admitted = window.try_record(limits, record.len());
            }
        }
        // Non-S3 streams retain the configuration-only acknowledgement, but
        // public ingestion is still metered even without an S3 backend.
        let Some(destination) = destination else {
            return Ok(accepted);
        };
        let buffer = registry.streams.entry(key.clone()).or_default();
        if accepted.iter().any(|admitted| *admitted) {
            let active = buffer.active.get_or_insert_with(|| ActiveBuffer {
                records: Vec::new(),
                input_bytes: 0,
                first: now.clone(),
            });
            for (record, admitted) in records.into_iter().zip(&accepted) {
                if !admitted {
                    continue;
                }
                active.input_bytes += record.len();
                active.records.push(record);
            }
            if active.is_empty(&destination) {
                // Clear before invoking any codec or writer.
                buffer.active = None;
            } else {
                buffer.detach(&key, stream, destination, &now, false);
            }
        }
        drop(registry);
        drop(config);
        self.core.wake.notify_one();
        Ok(accepted)
    }

    /// Start once inside an active Tokio runtime. Admission can precede startup.
    pub fn start(self: &Arc<Self>) -> Result<(), WorkerAlreadyStarted> {
        let mut worker = self.worker.lock();
        let registry = self.core.registry.lock();
        if worker.started || registry.closed {
            return Err(WorkerAlreadyStarted);
        }
        worker.started = true;
        worker.handle = Some(tokio::spawn(run_worker(
            Arc::downgrade(&self.core),
            self.core.wake.clone(),
        )));
        Ok(())
    }

    /// Close admission and drain without a grace window. Delivered counts cover
    /// completions observed after closing admission, including ordinary jobs
    /// already in flight. Repeated calls return the same final report.
    pub async fn shutdown(self: &Arc<Self>) -> DrainReport {
        // This gate shares the admission mutex: every handoff is either visible
        // to this drain or rejected. No preflight can slip through afterwards.
        self.core.registry.lock().closed = true;
        self.core.wake.notify_one();
        let mut drain = self.drain.lock().await;
        if let Some(report) = self.core.registry.lock().report.clone() {
            return report;
        }

        // Keep handles and final-job state in the async-locked drain, rather
        // than in this caller's future. Cancelling a shutdown waiter must not
        // orphan jobs, reopen admission, or allow duplicate final attempts.
        if drain.worker.is_none() {
            drain.worker = self.worker.lock().handle.take();
        }
        if let Some(handle) = &mut drain.worker {
            if let Err(error) = handle.await {
                // Blocking task failures are normally handled individually. A
                // worker failure must still retain and account for every head.
                tracing::warn!(?error, "Firehose worker stopped unexpectedly");
                // An unexpected worker failure cannot prove its blocking jobs
                // have stopped; retain those heads instead of duplicating them.
                drain
                    .blocked
                    .extend(self.core.registry.lock().streams.keys().cloned());
            }
            drain.worker = None;
        }

        loop {
            let capacity = 4 - drain.jobs.len();
            let ready = self.core.prepare(capacity, false, true, &drain.blocked);
            launch(&mut drain.jobs, ready);
            if let Some(completion) = drain.jobs.next().await {
                let key = completion.batch.batch.stream.clone();
                if !self.core.complete(completion) {
                    tracing::warn!(stream = ?key.name, account = %key.account_id, region = %key.region,
                        "Firehose final delivery attempt failed; batch retained undelivered");
                    drain.blocked.insert(key);
                }
            } else if let Some(report) = self.core.finish_shutdown(&drain.blocked) {
                return report;
            }
        }
    }
}

struct WriteJob {
    batch: Arc<DetachedBatch>,
    writer: Option<Arc<dyn BatchWriter>>,
}

struct Completion {
    batch: Arc<DetachedBatch>,
    result: Result<DeliveryWriteOutcome, String>,
}

#[derive(Default)]
struct Writes {
    tasks: JoinSet<Result<DeliveryWriteOutcome, String>>,
    batches: HashMap<Id, Arc<DetachedBatch>>,
}

impl Writes {
    fn len(&self) -> usize {
        self.tasks.len()
    }
    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    async fn next(&mut self) -> Option<Completion> {
        let (id, result) = match self.tasks.join_next_with_id().await? {
            Ok((id, result)) => (id, result),
            Err(error) => (
                error.id(),
                Err(format!("Firehose writer task failed: {error}")),
            ),
        };
        Some(Completion {
            batch: self
                .batches
                .remove(&id)
                .expect("every writer job retains its batch"),
            result,
        })
    }
}

fn launch(jobs: &mut Writes, ready: Vec<WriteJob>) {
    for job in ready {
        let batch = job.batch.clone();
        let handle = jobs.tasks.spawn_blocking(move || {
            job.writer
                .ok_or_else(|| "Firehose S3 backend is not configured".to_owned())?
                .write(&batch.batch)
        });
        jobs.batches.insert(handle.id(), job.batch);
    }
}

async fn run_worker(core: Weak<EngineCore>, wake: Arc<Notify>) {
    let mut jobs = Writes::default();
    let mut timer = tokio::time::interval_at(
        Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut tick = false;
    loop {
        let Some(engine) = core.upgrade() else { return };
        let closed = engine.registry.lock().closed;
        if !closed {
            let capacity = 4 - jobs.len();
            launch(
                &mut jobs,
                engine.prepare(capacity, tick, false, &BTreeSet::new()),
            );
        }
        drop(engine); // Neither a configuration guard nor an owning Arc crosses the wait.
        tick = false;
        if closed && jobs.is_empty() {
            return;
        }
        tokio::select! {
            completion = jobs.next(), if !jobs.is_empty() => {
                if let (Some(engine), Some(completion)) = (core.upgrade(), completion) { engine.complete(completion); }
            }
            _ = wake.notified() => {}
            _ = timer.tick(), if !closed => { tick = true; }
        }
    }
}

impl EngineCore {
    fn finish_shutdown(&self, blocked: &BTreeSet<StreamKey>) -> Option<DrainReport> {
        // Pin configuration through the final snapshot. A restoration between
        // the last readiness scan and here must still get its final attempt.
        let config = self.state.read();
        let mut registry = self.registry.lock();
        registry
            .streams
            .retain(|key, _| current(&config, key).is_some());
        registry
            .windows
            .retain(|key, _| current(&config, key).is_some());
        for (key, buffer) in &registry.streams {
            if blocked.contains(key) || buffer.in_flight {
                continue;
            }
            let stream = current(&config, key).unwrap();
            let supported_active =
                buffer.active.is_some() && matches!(stream_delivery(stream), Ok(Some(_)));
            if !buffer.queue.is_empty() || supported_active {
                return None;
            }
        }
        let mut report = DrainReport {
            delivered_batches: registry.drain_delivered,
            ..DrainReport::default()
        };
        for buffer in registry.streams.values() {
            if let Some(active) = &buffer.active {
                report.undelivered_records += active.records.len();
                report.undelivered_bytes += active.input_bytes;
            }
            for batch in &buffer.queue {
                report.undelivered_records += batch.batch.records.len();
                report.undelivered_bytes += batch.input_bytes;
            }
        }
        registry.report = Some(report.clone());
        Some(report)
    }

    fn prepare(
        &self,
        capacity: usize,
        tick: bool,
        force: bool,
        blocked: &BTreeSet<StreamKey>,
    ) -> Vec<WriteJob> {
        let writer = self.writer.read().clone();
        let config = self.state.read();
        let mut registry = self.registry.lock();
        // Sample within the decision, after any waiting admissions. Otherwise
        // a concurrent arrival could be newer than this turn's captured time.
        let now = self.clock.now();
        // Prune even retry-blocked or not-yet-due buffers; job Arcs live until
        // completion and the S3 writer's commit guard handles invalidation there.
        registry
            .streams
            .retain(|key, _| current(&config, key).is_some());
        registry
            .windows
            .retain(|key, _| current(&config, key).is_some());
        if registry.closed && !force {
            return Vec::new();
        }
        let mut ready = Vec::new();
        let mut keys: Vec<_> = registry.streams.keys().cloned().collect();
        if let Some(cursor) = &registry.last_scheduled {
            let start = keys.partition_point(|key| key <= cursor);
            keys.rotate_left(start);
        }
        for key in &keys {
            let buffer = registry.streams.get_mut(key).unwrap();
            let stream = current(&config, key).unwrap();
            // Unsupported updates pause accepted active data in place. Detached
            // batches continue using their original, supported configuration.
            let destination = match stream_delivery(stream) {
                Ok(Some(destination)) => Ok(destination),
                Ok(None) => Err("S3 destination is not configured".to_owned()),
                Err(error) => Err(error.message()),
            };
            match destination {
                Ok(destination) => {
                    buffer.blocked_version = None;
                    buffer.detach(key, stream, destination, &now, force);
                }
                Err(reason) => {
                    if let Some(active) = &buffer.active {
                        if buffer.blocked_version.as_deref() != Some(stream.version_id.as_str()) {
                            let retained_records = active.records.len()
                                + buffer
                                    .queue
                                    .iter()
                                    .map(|b| b.batch.records.len())
                                    .sum::<usize>();
                            let retained_bytes = active.input_bytes
                                + buffer.queue.iter().map(|b| b.input_bytes).sum::<usize>();
                            // Debug formatting escapes caller-supplied values in the reason.
                            tracing::warn!(account = %key.account_id, region = %key.region,
                                stream = ?key.name, version = %stream.version_id,
                                retained_records, retained_bytes, ?reason,
                                "Firehose active delivery paused by configuration");
                            buffer.blocked_version = Some(stream.version_id.clone());
                        }
                    } else {
                        buffer.blocked_version = None;
                    }
                }
            }
            if tick
                && buffer
                    .retry_not_before
                    .is_some_and(|deadline| now.monotonic >= deadline)
            {
                buffer.retry_not_before = None;
            }
            if ready.len() < capacity
                && !buffer.in_flight
                && !blocked.contains(key)
                && (force || buffer.retry_not_before.is_none())
            {
                if let Some(batch) = buffer.queue.front() {
                    buffer.in_flight = true;
                    ready.push(WriteJob {
                        batch: batch.clone(),
                        writer: writer.clone(),
                    });
                }
            }
        }
        if let Some(job) = ready.last() {
            registry.last_scheduled = Some(job.batch.batch.stream.clone());
        }
        registry
            .streams
            .retain(|_, b| b.active.is_some() || !b.queue.is_empty());
        ready
    }

    fn complete(&self, completion: Completion) -> bool {
        let mut registry = self.registry.lock();
        let closed = registry.closed;
        // Consuming the joined job owns this count exactly once. Pruning its
        // generation after commit does not undo the delivered object.
        if closed && matches!(&completion.result, Ok(DeliveryWriteOutcome::Committed)) {
            registry.drain_delivered += 1;
        }
        let now = self.clock.now();
        let key = &completion.batch.batch.stream;
        let Some(buffer) = registry.streams.get_mut(key) else {
            return true;
        };
        if !buffer
            .queue
            .front()
            .is_some_and(|head| Arc::ptr_eq(head, &completion.batch))
        {
            return true;
        }
        buffer.in_flight = false;
        match completion.result {
            Ok(_) => {
                buffer.queue.pop_front();
                buffer.retry_not_before = None;
                buffer.failures = 0;
                true
            }
            Err(error) => {
                buffer.failures = buffer.failures.saturating_add(1);
                // Keys, errors, and CloudFormation-created stream names can carry
                // caller-supplied text; Debug formatting escapes line breaks.
                let object_key = &completion.batch.batch.object_key;
                if closed {
                    // The drain decides whether this stream gets a final attempt.
                    tracing::warn!(stream = ?key.name, account = %key.account_id, region = %key.region,
                        ?object_key, attempt = buffer.failures, ?error,
                        "Firehose delivery attempt failed after admission closed");
                } else if now.monotonic.duration_since(completion.batch.first_arrival)
                    >= MAX_RETENTION
                {
                    buffer.queue.pop_front();
                    buffer.retry_not_before = None;
                    buffer.failures = 0;
                    tracing::warn!(stream = ?key.name, account = %key.account_id, region = %key.region,
                        ?object_key, records = completion.batch.batch.records.len(),
                        bytes = completion.batch.input_bytes, ?error,
                        "Firehose batch discarded after failing delivery for 24 hours");
                    return true;
                } else {
                    let delay = RETRY_INTERVAL;
                    buffer.retry_not_before = Some(now.monotonic + delay);
                    tracing::warn!(stream = ?key.name, account = %key.account_id, region = %key.region,
                        ?object_key, attempt = buffer.failures, retry_in_secs = delay.as_secs(), ?error,
                        "Firehose batch retained for retry");
                }
                false
            }
        }
    }
}

/// Throttle response using the API reference's HTTP 500, not the model's 503.
/// Closed admission is a separate, local shutdown condition and retains 503.
pub(crate) fn throughput_error() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        crate::throughput::THROUGHPUT_ERROR_CODE,
        crate::throughput::THROUGHPUT_ERROR_MESSAGE,
    )
}

/// The one delivery decision shared by admission, scheduling, and the final
/// drain, so they cannot disagree about whether a stream can deliver.
/// `Ok(None)`: no destination is configured, so nothing is written.
fn stream_delivery(stream: &DeliveryStream) -> Result<Option<DeliveryConfig>, AwsServiceError> {
    crate::admission::validate_stream_destination(stream)?;
    stream.destination.as_ref().map(delivery_config).transpose()
}

fn current<'a>(config: &'a crate::FirehoseAccounts, key: &StreamKey) -> Option<&'a DeliveryStream> {
    config
        .get(&key.account_id)?
        .streams(&key.region)?
        .get(&key.name)
        .filter(|stream| stream.instance_id == key.instance_id)
}

#[cfg(test)]
mod tests {
    mod throughput;

    use super::*;
    use crate::{config::parse_s3_destination, DeliveryStream, FirehoseAccounts, S3Destination};
    use serde_json::json;

    const ACCOUNT: &str = "111111111111";
    const REGION: &str = "us-east-1";
    const NAME: &str = "events";

    struct ControlledClock(Mutex<TimePoint>);

    impl Clock for ControlledClock {
        fn now(&self) -> TimePoint {
            self.0.lock().clone()
        }
    }

    #[derive(Default)]
    struct RecordingWriter(Mutex<Vec<DeliveryBatch>>);

    impl BatchWriter for RecordingWriter {
        fn write(&self, batch: &DeliveryBatch) -> Result<DeliveryWriteOutcome, String> {
            self.0.lock().push(batch.clone());
            Ok(DeliveryWriteOutcome::Committed)
        }
    }

    struct Fixture {
        engine: Arc<FirehoseEngine>,
        clock: Arc<ControlledClock>,
        writer: Arc<RecordingWriter>,
    }

    fn destination(interval: i64) -> S3Destination {
        parse_s3_destination(&json!({
            "RoleARN": "arn:aws:iam::111111111111:role/firehose",
            "BucketARN": "arn:aws:s3:::bucket",
            "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": interval}
        }))
        .unwrap()
        .unwrap()
    }

    impl Fixture {
        fn new(interval: i64) -> Self {
            let clock = Arc::new(ControlledClock(Mutex::new(TimePoint {
                monotonic: Instant::now(),
                wall: "2026-10-12T23:59:30Z".parse().unwrap(),
            })));
            let writer = Arc::new(RecordingWriter::default());
            let engine = Arc::new(FirehoseEngine::new(Arc::new(RwLock::new(
                FirehoseAccounts::new(),
            ))));
            // Injection is private to this inline module; the public constructor never spawns.
            let mut engine = Arc::try_unwrap(engine).ok().unwrap();
            Arc::get_mut(&mut engine.core).unwrap().clock = clock.clone();
            *engine.core.writer.write() = Some(writer.clone());
            let fixture = Self {
                engine: Arc::new(engine),
                clock,
                writer,
            };
            fixture.add_stream(ACCOUNT, REGION, NAME, interval);
            fixture
        }

        fn add_stream(&self, account: &str, region: &str, name: &str, interval: i64) {
            let mut stream: DeliveryStream = serde_json::from_value(json!({
                "name": name, "arn": "arn:aws:firehose:us-east-1:111111111111:deliverystream/events",
                "status": "ACTIVE", "stream_type": "DirectPut", "created_at": "2026-10-12T00:00:00Z",
                "last_update": "2026-10-12T00:00:00Z", "version_id": "1", "destination": null, "tags": {}
            })).unwrap();
            stream.destination = Some(destination(interval));
            self.engine
                .core
                .state
                .write()
                .get_or_create(account, region)
                .streams_mut(region)
                .insert(name.into(), stream);
        }

        fn enqueue(&self, records: Vec<Bytes>) {
            self.engine.enqueue(ACCOUNT, REGION, NAME, records).unwrap();
        }

        fn advance(&self, seconds: u64) {
            let mut point = self.clock.0.lock();
            point.monotonic += Duration::from_secs(seconds);
            point.wall += chrono::Duration::seconds(seconds as i64);
        }

        fn batches(&self) -> Vec<DeliveryBatch> {
            self.writer.0.lock().clone()
        }

        async fn turn(&self) {
            let mut jobs = Writes::default();
            let mut tick = true;
            loop {
                let capacity = 4 - jobs.len();
                launch(
                    &mut jobs,
                    self.engine
                        .core
                        .prepare(capacity, tick, false, &BTreeSet::new()),
                );
                tick = false;
                if let Some(result) = jobs.next().await {
                    self.engine.core.complete(result);
                } else {
                    break;
                }
            }
        }
    }

    fn record(byte: u8, size: usize) -> Bytes {
        Bytes::from(vec![byte; size])
    }

    #[tokio::test]
    async fn cross_call_size_trigger() {
        let f = Fixture::new(60);
        f.enqueue(vec![record(b'a', 524_288)]);
        f.turn().await;
        assert!(f.batches().is_empty());
        f.enqueue(vec![record(b'b', 524_288)]);
        f.turn().await;
        let batches = f.batches();
        assert_eq!(batches.len(), 1);
        let input_bytes: usize = batches[0].records.iter().map(Bytes::len).sum();
        assert_eq!(input_bytes, 1_048_576);
        assert_eq!(
            batches[0].records,
            vec![record(b'a', 524_288), record(b'b', 524_288)]
        );
    }

    #[tokio::test]
    async fn service_missing_bucket_is_accepted_and_retained_until_retry() {
        let f = Fixture::new(0);
        let s3: SharedS3State = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                ACCOUNT,
                REGION,
                "http://localhost",
            ),
        ));
        f.engine.set_s3(s3.clone());
        let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
        svc.deliver_records(ACCOUNT, REGION, NAME, vec![b"retained".to_vec()])
            .unwrap();
        f.turn().await; // Real memory writer fails because the bucket is missing.
        let original_key = {
            let registry = f.engine.core.registry.lock();
            let buffer = registry.streams.values().next().unwrap();
            assert_eq!(buffer.queue.len(), 1);
            assert_eq!(buffer.queue[0].input_bytes, 8);
            assert_eq!(
                buffer.queue[0].batch.records,
                vec![Bytes::from_static(b"retained")]
            );
            buffer.queue[0].batch.object_key.clone()
        };
        s3.write().get_or_create(ACCOUNT).buckets.insert(
            "bucket".into(),
            fakecloud_s3::S3Bucket::new("bucket", REGION, "owner"),
        );
        f.advance(RETRY_INTERVAL.as_secs());
        f.turn().await;
        {
            let state = s3.read();
            let objects = &state.get(ACCOUNT).unwrap().buckets["bucket"].objects;
            assert_eq!(objects.len(), 1);
            let object = &objects[&original_key];
            let fakecloud_persistence::BodyRef::Memory(body) = &object.body else {
                panic!("memory fixture")
            };
            assert_eq!(body.as_ref(), b"retained");
        }
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
    }

    #[tokio::test]
    async fn batch_overshoot_keeps_records() {
        let f = Fixture::new(60);
        f.enqueue(vec![record(b'a', 900 * 1024)]);
        f.enqueue(vec![record(b'b', 200 * 1024), record(b'c', 200 * 1024)]);
        f.turn().await;
        let batches = f.batches();
        assert_eq!(batches.len(), 1);
        assert_eq!(
            batches[0].records.iter().map(Bytes::len).sum::<usize>(),
            1_300 * 1024
        );
        assert_eq!(batches[0].records.len(), 3);
    }

    #[tokio::test]
    async fn idle_time_and_new_windows() {
        let f = Fixture::new(60);
        f.enqueue(vec![Bytes::from_static(b"first")]);
        f.advance(59);
        f.enqueue(vec![Bytes::from_static(b"later")]);
        f.turn().await;
        assert!(f.batches().is_empty());
        f.advance(1);
        f.turn().await;
        assert_eq!(
            f.batches()[0].records,
            vec![Bytes::from_static(b"first"), Bytes::from_static(b"later")]
        );
        f.enqueue(vec![Bytes::from_static(b"new")]);
        f.advance(59);
        f.turn().await;
        assert_eq!(f.batches().len(), 1);
        f.advance(1);
        f.turn().await;
        f.advance(60);
        f.turn().await;
        assert_eq!(f.batches().len(), 2);
    }

    #[tokio::test]
    async fn monotonic_deadline() {
        let f = Fixture::new(60);
        f.enqueue(vec![Bytes::from_static(b"data")]);
        f.clock.0.lock().wall -= chrono::Duration::days(1);
        f.advance(60);
        f.turn().await;
        assert_eq!(f.batches().len(), 1);
    }

    #[tokio::test]
    async fn zero_interval_and_prompt_size() {
        for (interval, bytes) in [(0, 1), (60, 1_048_576)] {
            let f = Fixture::new(interval);
            f.enqueue(vec![record(b'x', bytes)]);
            // Admission must detach immediately, before any periodic turn.
            assert_eq!(
                f.engine
                    .core
                    .registry
                    .lock()
                    .streams
                    .values()
                    .next()
                    .unwrap()
                    .queue
                    .len(),
                1
            );
            f.turn().await;
            assert_eq!(f.batches().len(), 1);
        }
    }

    #[tokio::test]
    async fn same_names_are_account_and_region_isolated() {
        let f = Fixture::new(60);
        for account in [ACCOUNT, "222222222222"] {
            for region in [REGION, "us-west-2"] {
                if (account, region) != (ACCOUNT, REGION) {
                    f.add_stream(account, region, NAME, 60);
                }
                f.engine
                    .enqueue(account, region, NAME, vec![record(b'a', 524_288)])
                    .unwrap();
            }
        }
        f.turn().await;
        assert!(f.batches().is_empty());
        f.engine
            .enqueue(
                "222222222222",
                "us-west-2",
                NAME,
                vec![record(b'b', 524_288)],
            )
            .unwrap();
        f.turn().await;
        assert_eq!(f.batches().len(), 1);
        let batch = &f.batches()[0];
        assert_eq!(batch.stream.account_id, "222222222222");
        assert_eq!(batch.stream.region, "us-west-2");
        assert_eq!(
            batch.records,
            vec![record(b'a', 524_288), record(b'b', 524_288)]
        );
    }

    enum Action {
        Commit,
        Fail,
        Panic,
    }

    /// Every cross-thread wait is bounded: a scheduling regression must fail
    /// the test rather than hang it.
    const TEST_WAIT: Duration = Duration::from_secs(10);

    async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
        tokio::time::timeout(TEST_WAIT, future)
            .await
            .expect("engine operation did not finish within the test bound")
    }

    async fn next_attempt(entered: &mut tokio::sync::mpsc::UnboundedReceiver<Attempt>) -> Attempt {
        bounded(entered.recv())
            .await
            .expect("writer attempt channel closed")
    }

    async fn wait_closed(engine: &FirehoseEngine) {
        bounded(async {
            while !engine.core.registry.lock().closed {
                tokio::task::yield_now().await;
            }
        })
        .await
    }

    struct Attempt {
        batch: DeliveryBatch,
        reply: std::sync::mpsc::Sender<Action>,
    }

    struct ControlledWriter {
        entered: tokio::sync::mpsc::UnboundedSender<Attempt>,
    }

    impl BatchWriter for ControlledWriter {
        fn write(&self, batch: &DeliveryBatch) -> Result<DeliveryWriteOutcome, String> {
            let (reply, receive) = std::sync::mpsc::channel();
            self.entered
                .send(Attempt {
                    batch: batch.clone(),
                    reply,
                })
                .unwrap();
            match receive.recv_timeout(TEST_WAIT).unwrap() {
                Action::Commit => Ok(DeliveryWriteOutcome::Committed),
                Action::Fail => Err("controlled failure".into()),
                Action::Panic => panic!("controlled blocking-writer panic"),
            }
        }
    }

    impl Fixture {
        fn controlled(&self) -> tokio::sync::mpsc::UnboundedReceiver<Attempt> {
            let (send, receive) = tokio::sync::mpsc::unbounded_channel();
            *self.engine.core.writer.write() = Some(Arc::new(ControlledWriter { entered: send }));
            receive
        }

        fn update(&self, name: &str, edit: impl FnOnce(&mut DeliveryStream)) {
            edit(
                self.engine
                    .core
                    .state
                    .write()
                    .get_or_create(ACCOUNT, REGION)
                    .streams_mut(REGION)
                    .get_mut(name)
                    .unwrap(),
            );
        }

        fn schedule(&self, jobs: &mut Writes, tick: bool) {
            let capacity = 4 - jobs.len();
            launch(
                jobs,
                self.engine
                    .core
                    .prepare(capacity, tick, false, &BTreeSet::new()),
            );
        }

        async fn finish(&self, jobs: &mut Writes) {
            self.engine
                .core
                .complete(bounded(jobs.next()).await.unwrap());
        }
    }

    #[tokio::test]
    async fn failed_batch_keeps_key_and_blocks_successors() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"head")]);
        let mut jobs = Writes::default();
        f.schedule(&mut jobs, false);
        let first = next_attempt(&mut entered).await;
        first.reply.send(Action::Fail).unwrap();
        f.finish(&mut jobs).await;
        f.update(NAME, |s| {
            s.version_id = "2".into();
            s.destination.as_mut().unwrap().prefix = Some("changed/".into());
        });
        f.enqueue(vec![Bytes::from_static(b"successor")]);
        f.schedule(&mut jobs, true);
        assert!(jobs.is_empty());
        f.advance(RETRY_INTERVAL.as_secs());
        // Clock movement plus admission notification alone cannot retry a head.
        f.schedule(&mut jobs, false);
        assert!(jobs.is_empty());
        f.schedule(&mut jobs, true);
        let retry = next_attempt(&mut entered).await;
        assert_eq!(retry.batch.object_key, first.batch.object_key);
        assert_eq!(retry.batch.version_id, "1");
        assert_eq!(retry.batch.destination.output.prefix, "");
        assert_eq!(retry.batch.records, vec![Bytes::from_static(b"head")]);
        retry.reply.send(Action::Commit).unwrap();
        f.finish(&mut jobs).await;
        f.schedule(&mut jobs, false);
        let next = next_attempt(&mut entered).await;
        assert_eq!(next.batch.records, vec![Bytes::from_static(b"successor")]);
        assert_eq!(next.batch.version_id, "2");
        assert!(next
            .batch
            .object_key
            .starts_with("changed/2026/10/12/23/events-2-"));
        next.reply.send(Action::Commit).unwrap();
        f.finish(&mut jobs).await;
    }

    /// A detached batch keeps the SSE-KMS key captured when it detached
    /// through every retry, even after UpdateDestination changes the key or
    /// removes EncryptionConfiguration; only later batches use the new one.
    #[tokio::test]
    async fn detached_batch_keeps_captured_kms_key_across_updates() {
        use fakecloud_s3::delivery::DeliveredEncryption;
        const CAPTURED: &str = "arn:aws:kms:us-east-1:111111111111:key/captured";
        const REPLACED: &str = "arn:aws:kms:us-east-1:111111111111:key/replaced";
        let kms = |arn: &str| json!({"KMSEncryptionConfig": {"AWSKMSKeyARN": arn}});
        for (replacement, successor_encryption) in [
            (
                Some(kms(REPLACED)),
                DeliveredEncryption::SseKms {
                    key_id: Some(REPLACED.into()),
                },
            ),
            (None, DeliveredEncryption::AsDelivered),
            (
                Some(json!({"NoEncryptionConfig": "NoEncryption"})),
                DeliveredEncryption::AsDelivered,
            ),
        ] {
            let f = Fixture::new(0);
            f.update(NAME, |s| {
                s.destination.as_mut().unwrap().encryption_configuration = Some(kms(CAPTURED));
            });
            let mut entered = f.controlled();
            f.enqueue(vec![Bytes::from_static(b"head")]);
            let mut jobs = Writes::default();
            f.schedule(&mut jobs, false);
            let first = next_attempt(&mut entered).await;
            let captured = DeliveredEncryption::SseKms {
                key_id: Some(CAPTURED.into()),
            };
            assert_eq!(first.batch.destination.encryption, captured);
            first.reply.send(Action::Fail).unwrap();
            f.finish(&mut jobs).await;

            f.update(NAME, |s| {
                s.version_id = "2".into();
                s.destination.as_mut().unwrap().encryption_configuration = replacement.clone();
            });
            f.enqueue(vec![Bytes::from_static(b"successor")]);
            for _ in 0..2 {
                f.advance(RETRY_INTERVAL.as_secs());
                f.schedule(&mut jobs, true);
                let retry = next_attempt(&mut entered).await;
                assert_eq!(retry.batch.object_key, first.batch.object_key);
                assert_eq!(
                    retry.batch.destination.encryption, captured,
                    "{replacement:?}"
                );
                retry.reply.send(Action::Fail).unwrap();
                f.finish(&mut jobs).await;
            }
            f.advance(RETRY_INTERVAL.as_secs());
            f.schedule(&mut jobs, true);
            let retry = next_attempt(&mut entered).await;
            assert_eq!(retry.batch.destination.encryption, captured);
            retry.reply.send(Action::Commit).unwrap();
            f.finish(&mut jobs).await;

            f.schedule(&mut jobs, false);
            let next = next_attempt(&mut entered).await;
            assert_eq!(next.batch.records, vec![Bytes::from_static(b"successor")]);
            assert_eq!(next.batch.destination.encryption, successor_encryption);
            next.reply.send(Action::Commit).unwrap();
            f.finish(&mut jobs).await;
        }
    }

    #[tokio::test]
    async fn ingestion_during_write_is_preserved() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        let mut jobs = Writes::default();
        f.enqueue(vec![Bytes::from_static(b"first")]);
        f.schedule(&mut jobs, false);
        let first = next_attempt(&mut entered).await;
        f.enqueue(vec![Bytes::from_static(b"second")]);
        f.schedule(&mut jobs, false);
        assert_eq!(jobs.len(), 1);
        first.reply.send(Action::Commit).unwrap();
        f.finish(&mut jobs).await;
        f.schedule(&mut jobs, false);
        let second = next_attempt(&mut entered).await;
        assert_eq!(first.batch.records, vec![Bytes::from_static(b"first")]);
        assert_eq!(second.batch.records, vec![Bytes::from_static(b"second")]);
        assert_ne!(first.batch.object_key, second.batch.object_key);
        second.reply.send(Action::Commit).unwrap();
        f.finish(&mut jobs).await;
        f.schedule(&mut jobs, false);
        assert!(jobs.is_empty());
    }

    #[tokio::test]
    async fn healthy_backlogs_rotate_fifth_stream_into_bounded_slots() {
        // Exercise both name ordering and the account prefix of StreamKey.
        for fifth_account in [ACCOUNT, "222222222222"] {
            let f = Fixture::new(0);
            let mut entered = f.controlled();
            let mut next_record = BTreeMap::new();
            for name in ["a", "b", "c", "d", "e"] {
                let account = if name == "e" { fifth_account } else { ACCOUNT };
                f.add_stream(account, REGION, name, 0);
                for sequence in 0..2 {
                    f.engine
                        .enqueue(account, REGION, name, vec![record(sequence, 1)])
                        .unwrap();
                }
                next_record.insert(name.to_owned(), 2u8);
            }
            let mut jobs = Writes::default();
            f.schedule(&mut jobs, false);
            assert_eq!(jobs.len(), 4);
            let mut initial = BTreeMap::new();
            for _ in 0..4 {
                let attempt = next_attempt(&mut entered).await;
                assert_eq!(attempt.batch.records, vec![record(0, 1)]);
                assert!(initial
                    .insert(attempt.batch.stream.name.clone(), attempt)
                    .is_none());
            }
            assert_eq!(
                initial.keys().map(String::as_str).collect::<Vec<_>>(),
                ["a", "b", "c", "d"]
            );
            let mut pending: VecDeque<_> = initial.into_values().collect();
            let mut expected = BTreeMap::from([
                ("a".to_owned(), 1u8),
                ("b".to_owned(), 1),
                ("c".to_owned(), 1),
                ("d".to_owned(), 1),
                ("e".to_owned(), 0),
            ]);
            let mut fifth_decision = None;
            for decision in 1..=20 {
                let completed = pending.pop_front().unwrap();
                let key = &completed.batch.stream;
                // The completed stream always has a successor; replenish it
                // before freeing the slot, so backoff/empty queues cannot help.
                let sequence = next_record.get_mut(&key.name).unwrap();
                f.engine
                    .enqueue(
                        &key.account_id,
                        REGION,
                        &key.name,
                        vec![record(*sequence, 1)],
                    )
                    .unwrap();
                *sequence += 1;
                completed.reply.send(Action::Commit).unwrap();
                f.finish(&mut jobs).await;
                f.schedule(&mut jobs, false);
                assert_eq!(jobs.len(), 4);
                let next = next_attempt(&mut entered).await;
                assert!(pending.iter().all(|p| p.batch.stream != next.batch.stream));
                let sequence = expected.get_mut(&next.batch.stream.name).unwrap();
                assert_eq!(next.batch.records, vec![record(*sequence, 1)]);
                *sequence += 1;
                if next.batch.stream.name == "e" {
                    fifth_decision.get_or_insert(decision);
                }
                pending.push_back(next);
            }
            for attempt in pending {
                attempt.reply.send(Action::Commit).unwrap();
            }
            while !jobs.is_empty() {
                f.finish(&mut jobs).await;
            }
            assert!(
                fifth_decision.is_some_and(|decision| decision <= 4),
                "fifth healthy stream starved: first slot {fifth_decision:?}"
            );
            assert!(expected.values().all(|count| *count >= 4));
            // Final drain still delivers all queued successors under B1.
            *f.engine.core.writer.write() = Some(f.writer.clone());
            assert_eq!(f.engine.shutdown().await.undelivered_records, 0);
        }
    }

    #[tokio::test]
    async fn rotation_skips_pruned_cursor_and_tick_ineligible_retry() {
        let f = Fixture::new(0);
        for name in ["a", "b", "c", "d", "e"] {
            f.add_stream(ACCOUNT, REGION, name, 0);
            f.engine
                .enqueue(ACCOUNT, REGION, name, vec![Bytes::from_static(b"head")])
                .unwrap();
        }
        // Model completions directly: the scheduling decision owns these heads,
        // but no writer thread/timing is needed to prove skip eligibility.
        let mut first = f.engine.core.prepare(4, false, false, &BTreeSet::new());
        let cursor_job = first.pop().unwrap();
        assert_eq!(cursor_job.batch.batch.stream.name, "d");
        f.engine
            .core
            .state
            .write()
            .get_or_create(ACCOUNT, REGION)
            .streams_mut(REGION)
            .remove("d");
        for job in first {
            let failed = job.batch.batch.stream.name == "a";
            f.engine.core.complete(Completion {
                batch: job.batch,
                result: if failed {
                    Err("controlled failure".into())
                } else {
                    Ok(DeliveryWriteOutcome::Committed)
                },
            });
        }
        f.advance(RETRY_INTERVAL.as_secs());
        let ready = f.engine.core.prepare(4, false, false, &BTreeSet::new());
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].batch.batch.stream.name, "e");
        for job in ready {
            f.engine.core.complete(Completion {
                batch: job.batch,
                result: Ok(DeliveryWriteOutcome::Committed),
            });
        }
        assert!(f
            .engine
            .core
            .prepare(4, false, false, &BTreeSet::new())
            .is_empty());
        let retry = f.engine.core.prepare(4, true, false, &BTreeSet::new());
        assert_eq!(retry.len(), 1);
        assert_eq!(retry[0].batch.batch.stream.name, "a");
        for job in retry {
            f.engine.core.complete(Completion {
                batch: job.batch,
                result: Ok(DeliveryWriteOutcome::Committed),
            });
        }
        // Late cancelled completion for the pruned cursor still owns no buffer.
        f.engine.core.complete(Completion {
            batch: cursor_job.batch,
            result: Ok(DeliveryWriteOutcome::Cancelled),
        });
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
    }

    #[tokio::test]
    async fn four_jobs_and_per_stream_serialization() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        for name in ["a", "b", "c", "d", "e"] {
            f.add_stream(ACCOUNT, REGION, name, 0);
            f.engine
                .enqueue(ACCOUNT, REGION, name, vec![Bytes::from_static(b"one")])
                .unwrap();
        }
        f.engine
            .enqueue(ACCOUNT, REGION, "a", vec![Bytes::from_static(b"two")])
            .unwrap();
        let mut jobs = Writes::default();
        f.schedule(&mut jobs, true);
        assert_eq!(jobs.len(), 4);
        let mut attempts = BTreeMap::new();
        for _ in 0..4 {
            let attempt = next_attempt(&mut entered).await;
            assert!(attempts
                .insert(attempt.batch.stream.name.clone(), attempt)
                .is_none());
        }
        assert_eq!(
            attempts.keys().map(String::as_str).collect::<Vec<_>>(),
            vec!["a", "b", "c", "d"]
        );
        f.schedule(&mut jobs, false);
        assert_eq!(jobs.len(), 4);
        attempts
            .remove("a")
            .unwrap()
            .reply
            .send(Action::Fail)
            .unwrap();
        f.finish(&mut jobs).await;
        f.schedule(&mut jobs, false);
        assert_eq!(jobs.len(), 4);
        let fifth = next_attempt(&mut entered).await;
        assert_eq!(fifth.batch.stream.name, "e");
        fifth.reply.send(Action::Commit).unwrap();
        for attempt in attempts.into_values() {
            attempt.reply.send(Action::Commit).unwrap();
        }
        while !jobs.is_empty() {
            f.finish(&mut jobs).await;
        }
        f.schedule(&mut jobs, false);
        assert!(jobs.is_empty());
        *f.engine.core.writer.write() = Some(Arc::new(FailingWriter));
        let report = f.engine.shutdown().await;
        assert_eq!(report.undelivered_records, 2);
    }

    #[tokio::test]
    async fn failed_head_retries_every_five_seconds_and_success_resets() {
        let f = Fixture::new(0);
        let fail = |job: WriteJob| {
            assert!(!f.engine.core.complete(Completion {
                batch: job.batch,
                result: Err("unavailable".into()),
            }));
        };
        // Whole seconds of tick-driven scanning until the retained head is eligible.
        let eligible_after = || {
            for waited in 1..=60 {
                f.advance(1);
                let mut ready = f.engine.core.prepare(4, true, false, &BTreeSet::new());
                if let Some(job) = ready.pop() {
                    assert!(ready.is_empty());
                    return (waited, job);
                }
            }
            panic!("retained head never became eligible");
        };
        f.enqueue(vec![Bytes::from_static(b"head")]);
        let first = f
            .engine
            .core
            .prepare(4, false, false, &BTreeSet::new())
            .pop()
            .unwrap();
        let capture = CapturedEvents::default();
        let mut delays = Vec::new();
        let job = tracing::subscriber::with_default(capture.clone(), || {
            let mut job = first;
            for _ in 0..7 {
                fail(job);
                let (waited, next) = eligible_after();
                assert_eq!(next.batch.batch.records, vec![Bytes::from_static(b"head")]);
                delays.push(waited);
                job = next;
            }
            job
        });
        // AWS retries S3 delivery every 5 seconds, with no backoff.
        assert_eq!(delays, [5; 7]);
        let warnings = capture.0.lock().clone();
        assert_eq!(
            warnings
                .iter()
                .map(|w| (w["attempt"].as_str(), w["retry_in_secs"].as_str()))
                .collect::<Vec<_>>(),
            [
                ("1", "5"),
                ("2", "5"),
                ("3", "5"),
                ("4", "5"),
                ("5", "5"),
                ("6", "5"),
                ("7", "5")
            ]
        );
        assert!(warnings.iter().all(|w| w["level"] == "WARN"));
        // A delivered head resets the attempt count; its successor retries
        // on the same 5-second cadence.
        assert!(f.engine.core.complete(Completion {
            batch: job.batch,
            result: Ok(DeliveryWriteOutcome::Committed),
        }));
        f.enqueue(vec![Bytes::from_static(b"next")]);
        let next = f
            .engine
            .core
            .prepare(4, false, false, &BTreeSet::new())
            .pop()
            .unwrap();
        assert_eq!(next.batch.batch.records, vec![Bytes::from_static(b"next")]);
        fail(next);
        let (waited, retry) = eligible_after();
        assert_eq!(waited, 5);
        assert!(f.engine.core.complete(Completion {
            batch: retry.batch,
            result: Ok(DeliveryWriteOutcome::Committed),
        }));
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
    }

    #[tokio::test]
    async fn wall_prefix_retains_oldest_arrival() {
        let f = Fixture::new(60);
        f.enqueue(vec![Bytes::from_static(b"first")]);
        f.advance(45);
        f.enqueue(vec![Bytes::from_static(b"later")]);
        f.advance(15);
        f.turn().await;
        let batches = f.batches();
        assert_eq!(
            batches[0].oldest_arrival.to_rfc3339(),
            "2026-10-12T23:59:30+00:00"
        );
        assert!(batches[0]
            .object_key
            .starts_with("2026/10/12/23/events-1-2026-10-13-00-00-30-"));
    }

    #[tokio::test]
    async fn hint_update_does_not_restart_window() {
        let f = Fixture::new(300);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().buffering_size_mb = Some(5)
        });
        f.enqueue(vec![record(b'x', 1_048_576)]);
        f.advance(59);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().buffering_interval_seconds = Some(60)
        });
        f.turn().await;
        assert!(f.batches().is_empty());
        f.advance(1);
        f.turn().await;
        assert_eq!(f.batches().len(), 1);
        f.enqueue(vec![record(b'y', 1_048_576)]);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().buffering_size_mb = Some(1)
        });
        f.turn().await;
        assert_eq!(f.batches().len(), 2);
    }

    struct FailingWriter;
    impl BatchWriter for FailingWriter {
        fn write(&self, _: &DeliveryBatch) -> Result<DeliveryWriteOutcome, String> {
            Err("unavailable".into())
        }
    }

    #[tokio::test]
    async fn failed_batches_are_discarded_after_24_hours_of_retries() {
        let f = Fixture::new(0);
        *f.engine.core.writer.write() = Some(Arc::new(FailingWriter));
        f.enqueue(vec![Bytes::from_static(b"stale")]);
        f.turn().await;
        let queued = |f: &Fixture| {
            f.engine
                .core
                .registry
                .lock()
                .streams
                .values()
                .map(|buffer| buffer.queue.len())
                .sum::<usize>()
        };
        assert_eq!(queued(&f), 1);
        // Just inside the window: the retry fails and the batch is kept.
        f.advance(86_399);
        f.turn().await;
        assert_eq!(queued(&f), 1);
        // The next failed retry is past 24 hours from arrival: discarded.
        f.advance(RETRY_INTERVAL.as_secs());
        let capture = CapturedEvents::default();
        {
            let _default = tracing::subscriber::set_default(capture.clone());
            f.turn().await;
        }
        assert_eq!(queued(&f), 0);
        let warnings = capture.0.lock().clone();
        let discarded = warnings
            .iter()
            .find(|warning| warning["message"].contains("discarded"))
            .expect("discard warning");
        assert_eq!(discarded["records"], "1");
        assert_eq!(discarded["bytes"], "5");
        // A newer batch on the same stream is retained and retried normally.
        f.enqueue(vec![Bytes::from_static(b"fresh")]);
        f.turn().await;
        assert_eq!(queued(&f), 1);
        *f.engine.core.writer.write() = Some(f.writer.clone());
        f.advance(RETRY_INTERVAL.as_secs());
        f.turn().await;
        assert_eq!(queued(&f), 0);
        assert_eq!(
            f.batches()
                .into_iter()
                .map(|batch| batch.records)
                .collect::<Vec<_>>(),
            vec![vec![Bytes::from_static(b"fresh")]]
        );
    }

    #[tokio::test]
    async fn batches_older_than_24_hours_still_get_the_final_drain_attempt() {
        let f = Fixture::new(0);
        *f.engine.core.writer.write() = Some(Arc::new(FailingWriter));
        f.enqueue(vec![Bytes::from_static(b"old")]);
        f.turn().await;
        // Retention is enforced only when an ordinary retry fails; shutdown
        // still makes its single final attempt.
        f.advance(86_401);
        *f.engine.core.writer.write() = Some(f.writer.clone());
        assert_eq!(
            f.engine.shutdown().await,
            DrainReport {
                delivered_batches: 1,
                undelivered_records: 0,
                undelivered_bytes: 0
            }
        );
        assert_eq!(f.batches()[0].records, vec![Bytes::from_static(b"old")]);
    }

    #[tokio::test]
    async fn caller_supplied_values_are_escaped_in_warnings() {
        struct KeyEchoWriter;
        impl BatchWriter for KeyEchoWriter {
            fn write(&self, batch: &DeliveryBatch) -> Result<DeliveryWriteOutcome, String> {
                Err(format!("cannot write {}", batch.object_key))
            }
        }
        let f = Fixture::new(0);
        f.update(NAME, |stream| {
            stream.destination.as_mut().unwrap().prefix = Some("forged\nWARN line/".into());
        });
        *f.engine.core.writer.write() = Some(Arc::new(KeyEchoWriter));
        f.enqueue(vec![Bytes::from_static(b"x")]);
        let capture = CapturedEvents::default();
        {
            let _default = tracing::subscriber::set_default(capture.clone());
            f.turn().await;
        }
        let warnings = capture.0.lock().clone();
        let retained = warnings
            .iter()
            .find(|warning| warning["message"].contains("retained for retry"))
            .expect("retry warning");
        for field in ["object_key", "error"] {
            assert!(
                retained[field].contains("forged\\nWARN line/"),
                "{field}: {}",
                retained[field]
            );
        }
        for warning in &warnings {
            for value in warning.values() {
                assert!(!value.contains('\n'), "unescaped line break: {value}");
            }
        }
    }

    #[tokio::test]
    async fn panic_retains_batch_and_counts() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"panic")]);
        let mut jobs = Writes::default();
        f.schedule(&mut jobs, false);
        let first = next_attempt(&mut entered).await;
        first.reply.send(Action::Panic).unwrap();
        f.finish(&mut jobs).await;
        *f.engine.core.writer.write() = Some(Arc::new(FailingWriter));
        f.enqueue(vec![Bytes::from_static(b"behind")]);
        let report = f.engine.shutdown().await;
        assert_eq!(
            report,
            DrainReport {
                delivered_batches: 0,
                undelivered_records: 2,
                undelivered_bytes: 11
            }
        );
        assert_eq!(
            f.engine
                .core
                .registry
                .lock()
                .streams
                .values()
                .next()
                .unwrap()
                .queue
                .front()
                .unwrap()
                .batch
                .object_key,
            first.batch.object_key
        );
    }

    #[tokio::test]
    async fn shutdown_drains_healthy_streams_once() {
        let f = Fixture::new(0);
        f.enqueue(vec![Bytes::from_static(b"one")]);
        f.enqueue(vec![Bytes::from_static(b"two")]);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().buffering_interval_seconds = Some(900)
        });
        f.enqueue(vec![Bytes::from_static(b"active")]);
        f.add_stream(ACCOUNT, REGION, "other", 900);
        f.engine
            .enqueue(ACCOUNT, REGION, "other", vec![Bytes::from_static(b"other")])
            .unwrap();
        // No start: embedded/unit callers still receive a complete final drain.
        let (first, duplicate) = tokio::join!(f.engine.shutdown(), f.engine.shutdown());
        assert_eq!(
            first,
            DrainReport {
                delivered_batches: 4,
                undelivered_records: 0,
                undelivered_bytes: 0
            }
        );
        assert_eq!(first, duplicate);
        assert_eq!(f.batches().len(), 4);
        let events: Vec<_> = f
            .batches()
            .into_iter()
            .filter(|b| b.stream.name == NAME)
            .map(|b| b.records)
            .collect();
        assert_eq!(
            events,
            vec![
                vec![Bytes::from_static(b"one")],
                vec![Bytes::from_static(b"two")],
                vec![Bytes::from_static(b"active")]
            ]
        );
    }

    #[tokio::test]
    async fn shutdown_failure_reports_remainder_without_retry_loop() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"head")]);
        f.enqueue(vec![Bytes::from_static(b"next")]);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().buffering_interval_seconds = Some(900)
        });
        f.enqueue(vec![Bytes::from_static(b"active")]);
        f.add_stream(ACCOUNT, REGION, "healthy", 900);
        f.engine
            .enqueue(ACCOUNT, REGION, "healthy", vec![Bytes::from_static(b"ok")])
            .unwrap();
        let engine = f.engine.clone();
        let drain = tokio::spawn(async move { engine.shutdown().await });
        for _ in 0..2 {
            let attempt = next_attempt(&mut entered).await;
            attempt
                .reply
                .send(if attempt.batch.stream.name == NAME {
                    Action::Fail
                } else {
                    Action::Commit
                })
                .unwrap();
        }
        let report = bounded(drain).await.unwrap();
        assert_eq!(
            report,
            DrainReport {
                delivered_batches: 1,
                undelivered_records: 3,
                undelivered_bytes: 14
            }
        );
        assert!(entered.try_recv().is_err());
        assert_eq!(f.engine.shutdown().await, report);
    }

    #[tokio::test]
    async fn failed_in_flight_gets_one_final_attempt() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"head")]);
        f.engine.start().unwrap();
        let ordinary = next_attempt(&mut entered).await;
        let engine = f.engine.clone();
        let drain = tokio::spawn(async move { engine.shutdown().await });
        // Poll the drain future once to close admission deterministically.
        wait_closed(&f.engine).await;
        assert!(!drain.is_finished());
        ordinary.reply.send(Action::Fail).unwrap();
        let final_attempt = next_attempt(&mut entered).await;
        assert_eq!(final_attempt.batch.object_key, ordinary.batch.object_key);
        final_attempt.reply.send(Action::Fail).unwrap();
        assert_eq!(
            bounded(drain).await.unwrap(),
            DrainReport {
                delivered_batches: 0,
                undelivered_records: 1,
                undelivered_bytes: 4
            }
        );
        assert!(entered.try_recv().is_err());
    }

    #[tokio::test]
    async fn worker_failure_retains_every_stream_without_duplicate_attempts() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"head")]);
        f.add_stream(ACCOUNT, REGION, "other", 900);
        f.engine
            .enqueue(
                ACCOUNT,
                REGION,
                "other",
                vec![Bytes::from_static(b"active")],
            )
            .unwrap();
        f.engine.start().unwrap();
        let orphan = next_attempt(&mut entered).await;
        // An unexpected worker exit leaves its blocking job running unobserved.
        f.engine.worker.lock().handle.as_ref().unwrap().abort();
        let capture = CapturedEvents::default();
        let report = {
            let _events = tracing::subscriber::set_default(capture.clone());
            bounded(f.engine.shutdown()).await
        };
        // Neither the orphaned head nor the healthy stream is drained: the engine
        // cannot prove the old job stopped, so it reports instead of duplicating.
        assert_eq!(
            report,
            DrainReport {
                delivered_batches: 0,
                undelivered_records: 2,
                undelivered_bytes: 10
            }
        );
        assert!(entered.try_recv().is_err(), "no final attempt may run");
        assert!(capture.0.lock().iter().any(|event| event["level"] == "WARN"
            && event["message"].contains("worker stopped unexpectedly")));
        orphan.reply.send(Action::Commit).unwrap();
        assert_eq!(bounded(f.engine.shutdown()).await, report);
        assert!(entered.try_recv().is_err());
    }

    #[tokio::test]
    async fn start_is_explicit_and_once() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"ready")]);
        assert!(entered.try_recv().is_err());
        f.engine.start().unwrap();
        assert!(f.engine.start().is_err());
        let ready = next_attempt(&mut entered).await;
        let engine = f.engine.clone();
        let drain = tokio::spawn(async move { engine.shutdown().await });
        wait_closed(&f.engine).await;
        ready.reply.send(Action::Commit).unwrap();
        assert_eq!(bounded(drain).await.unwrap().delivered_batches, 1);
        assert!(f.engine.start().is_err());
    }

    #[tokio::test]
    async fn worker_does_not_own_engine_lifetime() {
        let f = Fixture::new(60);
        let engine = Arc::downgrade(&f.engine);
        let core = Arc::downgrade(&f.engine.core);
        f.engine.start().unwrap();
        tokio::task::yield_now().await;
        drop(f);
        assert!(engine.upgrade().is_none());
        assert!(core.upgrade().is_none());
    }

    // Interleaving is not forced: any split between accepted and rejected calls
    // is valid, but every accepted record must be delivered or reported.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enqueue_concurrent_with_shutdown_accounts_every_accepted_record() {
        let f = Fixture::new(900);
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let engine = f.engine.clone();
        let gate = barrier.clone();
        let enqueue = std::thread::spawn(move || {
            gate.wait();
            (0..100)
                .map(|_| engine.enqueue(ACCOUNT, REGION, NAME, vec![Bytes::from_static(b"abc")]))
                .collect::<Vec<_>>()
        });
        barrier.wait();
        let report = f.engine.shutdown().await;
        let results = enqueue.join().unwrap();
        let accepted = results.iter().filter(|r| r.is_ok()).count();
        for error in results.into_iter().filter_map(Result::err) {
            assert_eq!(error.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(error.code(), "ServiceUnavailableException");
        }
        assert_eq!(
            f.batches().iter().map(|b| b.records.len()).sum::<usize>() + report.undelivered_records,
            accepted
        );
        assert_eq!(
            f.batches()
                .iter()
                .flat_map(|b| &b.records)
                .map(Bytes::len)
                .sum::<usize>()
                + report.undelivered_bytes,
            accepted * 3
        );
        assert_eq!(
            f.engine
                .enqueue(ACCOUNT, REGION, NAME, vec![Bytes::new()])
                .unwrap_err()
                .code(),
            "ServiceUnavailableException"
        );
    }

    #[tokio::test]
    async fn all_empty_payloads_do_not_write_objects() {
        for codec in ["UNCOMPRESSED", "GZIP", "Snappy"] {
            let f = Fixture::new(0);
            f.update(NAME, |s| {
                s.destination.as_mut().unwrap().compression_format = Some(codec.into())
            });
            f.enqueue(vec![Bytes::new(), Bytes::new()]);
            f.turn().await;
            assert_eq!(f.engine.shutdown().await, DrainReport::default());
            assert!(f.batches().is_empty());
        }
    }

    fn set_processing(f: &Fixture, processing: Option<serde_json::Value>) {
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().processing_configuration = processing
        });
    }

    #[tokio::test]
    async fn only_append_delimiter_processing_is_admitted() {
        let f = Fixture::new(0);
        for processing in [
            json!({"Enabled": true}),
            json!({"Enabled": true, "Processors": []}),
            json!({"Enabled": true, "Processors": [{"Type": "Lambda"}]}),
            json!({"Enabled": true, "Processors": [
                {"Type": "Lambda", "Parameters": [{"ParameterName": "LambdaArn", "ParameterValue": "arn:aws:lambda:us-east-1:111111111111:function:f"}]},
                {"Type": "AppendDelimiterToRecord"}
            ]}),
        ] {
            set_processing(&f, Some(processing.clone()));
            let error = f
                .engine
                .enqueue(ACCOUNT, REGION, NAME, vec![Bytes::from_static(b"x")])
                .unwrap_err();
            assert_eq!(error.status(), StatusCode::BAD_REQUEST, "{processing}");
            assert_eq!(error.code(), "InvalidArgumentException");
            assert!(
                error.message().contains("ProcessingConfiguration"),
                "{processing}"
            );
        }
        assert!(
            f.engine.core.registry.lock().streams.is_empty(),
            "rejected records must not be admitted"
        );
        set_processing(
            &f,
            Some(json!({"Enabled": true, "Processors": [{"Type": "AppendDelimiterToRecord"}]})),
        );
        f.enqueue(vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]);
        f.turn().await;
        let batches = f.batches();
        assert_eq!(batches.len(), 1);
        assert!(batches[0].destination.output.append_delimiter);
        // Delimiters are written by the encoder; buffered records stay verbatim.
        assert_eq!(
            batches[0].records,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
    }

    #[tokio::test]
    async fn empty_records_are_kept_only_for_a_delimiting_destination() {
        let delimiter =
            json!({"Enabled": true, "Processors": [{"Type": "AppendDelimiterToRecord"}]});
        // Admission: an immediately due delivery of only empty records detaches.
        let f = Fixture::new(0);
        set_processing(&f, Some(delimiter.clone()));
        f.enqueue(vec![Bytes::new(), Bytes::new()]);
        f.turn().await;
        assert_eq!(f.batches().len(), 1);
        assert_eq!(f.batches()[0].records, vec![Bytes::new(), Bytes::new()]);

        // Detachment judges emptiness by the configuration it captures.
        for keep in [true, false] {
            let f = Fixture::new(60);
            set_processing(&f, Some(delimiter.clone()));
            f.enqueue(vec![Bytes::new()]);
            if !keep {
                set_processing(&f, None);
            }
            f.advance(60);
            f.turn().await;
            assert_eq!(f.batches().len(), usize::from(keep), "keep={keep}");
            assert_eq!(f.engine.shutdown().await, DrainReport::default());
        }
    }

    #[tokio::test]
    async fn append_delimiter_terminates_records_through_the_s3_writer() {
        use std::io::Read;

        for codec in ["UNCOMPRESSED", "GZIP", "Snappy"] {
            let f = Fixture::new(0);
            let s3 = s3();
            f.engine.set_s3(s3.clone());
            f.update(NAME, |s| {
                let dest = s.destination.as_mut().unwrap();
                dest.compression_format = Some(codec.into());
                dest.processing_configuration = Some(
                    json!({"Enabled": true, "Processors": [{"Type": "AppendDelimiterToRecord"}]}),
                );
            });
            f.enqueue(vec![
                Bytes::from_static(b"a"),
                Bytes::new(),
                Bytes::from_static(b"b\n"),
            ]);
            // Only empty records still produce one newline each.
            f.enqueue(vec![Bytes::new(), Bytes::new()]);
            // A delimiter can close a Snappy block: windows stay 32768 bytes.
            f.enqueue(vec![record(b'x', 32767), record(b'y', 5)]);
            assert_eq!(f.engine.shutdown().await.delivered_batches, 3, "{codec}");
            let state = s3.read();
            let objects = &state.get(ACCOUNT).unwrap().buckets["bucket"].objects;
            let mut decoded: Vec<Vec<u8>> = objects
                .values()
                .map(|object| {
                    let BodyRef::Memory(body) = &object.body else {
                        panic!("memory fixture")
                    };
                    match codec {
                        "GZIP" => {
                            assert!(object.key.ends_with(".gz"), "{}", object.key);
                            assert_eq!(object.content_encoding.as_deref(), Some("gzip"));
                            let mut plain = Vec::new();
                            flate2::read::GzDecoder::new(body.as_ref())
                                .read_to_end(&mut plain)
                                .unwrap();
                            plain
                        }
                        "Snappy" => {
                            assert!(object.key.ends_with(".snappy"), "{}", object.key);
                            assert_eq!(object.content_encoding.as_deref(), Some("snappy-java"));
                            let blocks = crate::test_xerial_decoder::decode(body).unwrap();
                            if blocks.len() > 1 {
                                assert_eq!(
                                    blocks.iter().map(Vec::len).collect::<Vec<_>>(),
                                    [32768, 6]
                                );
                                assert_eq!(blocks[0][32767], b'\n');
                            }
                            blocks.concat()
                        }
                        _ => {
                            assert_eq!(object.content_encoding, None);
                            body.to_vec()
                        }
                    }
                })
                .collect();
            decoded.sort();
            let mut large = vec![b'x'; 32767];
            large.extend_from_slice(b"\nyyyyy\n");
            assert_eq!(
                decoded,
                vec![b"\n\n".to_vec(), b"a\n\nb\n\n".to_vec(), large],
                "{codec}"
            );
        }
    }

    #[tokio::test]
    async fn blocked_active_configuration_warns_once_and_resumes_original_deadline() {
        for processing in [false, true] {
            let f = Fixture::new(60);
            f.enqueue(vec![Bytes::from_static(b"accepted"), Bytes::new()]);
            let first = f
                .engine
                .core
                .registry
                .lock()
                .streams
                .values()
                .next()
                .unwrap()
                .active
                .as_ref()
                .unwrap()
                .first
                .monotonic;
            f.advance(59);
            let block = |stream: &mut DeliveryStream| {
                let dest = stream.destination.as_mut().unwrap();
                if processing {
                    dest.processing_configuration = Some(json!({"Enabled": true}));
                } else {
                    dest.compression_format = Some("ZIP".into());
                }
            };
            f.update(NAME, |stream| {
                stream.version_id = "2".into();
                block(stream);
            });
            let capture = CapturedEvents::default();
            let scan = || {
                tracing::subscriber::with_default(capture.clone(), || {
                    assert!(f
                        .engine
                        .core
                        .prepare(4, true, false, &BTreeSet::new())
                        .is_empty());
                })
            };
            scan();
            scan();
            assert_eq!(
                capture.0.lock().len(),
                1,
                "pending unsupported data needs one observed warning per version"
            );
            // Every UpdateDestination bumps the version: each update warns once,
            // never once per scan, without comparing configuration bodies.
            f.update(NAME, |stream| stream.version_id = "3".into());
            scan();
            scan();
            let warnings = capture.0.lock().clone();
            assert_eq!(warnings.len(), 2);
            assert_eq!(warnings[1]["version"], "3");
            let warning = &warnings[0];
            assert_eq!(warning["level"], "WARN");
            assert_eq!(warning["account"], ACCOUNT);
            assert_eq!(warning["region"], REGION);
            assert_eq!(warning["stream"], format!("{NAME:?}"));
            assert_eq!(warning["version"], "2");
            assert_eq!(warning["retained_records"], "2");
            assert_eq!(warning["retained_bytes"], "8");
            assert!(warning["reason"].contains(if processing {
                "ProcessingConfiguration"
            } else {
                "ZIP"
            }));
            assert!(warning["message"].contains("paused"));
            {
                let registry = f.engine.core.registry.lock();
                let active = registry
                    .streams
                    .values()
                    .next()
                    .unwrap()
                    .active
                    .as_ref()
                    .unwrap();
                assert_eq!(
                    active.records,
                    vec![Bytes::from_static(b"accepted"), Bytes::new()]
                );
                assert_eq!(active.first.monotonic, first);
            }
            let restore = |stream: &mut DeliveryStream| {
                let dest = stream.destination.as_mut().unwrap();
                dest.compression_format = Some("GZIP".into());
                dest.processing_configuration = None;
            };
            f.update(NAME, restore);
            scan(); // Supported but not due; clears the diagnostic latch.
            assert!(f.batches().is_empty());
            f.update(NAME, block);
            scan();
            assert_eq!(
                capture.0.lock().len(),
                3,
                "restoration must re-arm the warning"
            );
            f.advance(1);
            scan();
            assert_eq!(capture.0.lock().len(), 3);
            f.update(NAME, restore);
            f.turn().await;
            let batches = f.batches();
            assert_eq!(
                batches.len(),
                1,
                "restoration must retain the first-arrival deadline"
            );
            assert_eq!(
                batches[0].records,
                vec![Bytes::from_static(b"accepted"), Bytes::new()]
            );
            assert_eq!(
                batches[0].oldest_arrival.to_rfc3339(),
                "2026-10-12T23:59:30+00:00"
            );
            assert_eq!(f.engine.shutdown().await, DrainReport::default());
        }
    }

    // Capture actual structured tracing events without adding a test dependency
    // or installing a process-global subscriber that races parallel tests.
    #[derive(Clone, Default)]
    struct CapturedEvents(Arc<Mutex<Vec<BTreeMap<String, String>>>>);

    impl tracing::Subscriber for CapturedEvents {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(BTreeMap<String, String>);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0.insert(field.name().into(), format!("{value:?}"));
                }
                fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                    self.0.insert(field.name().into(), value.into());
                }
            }
            let mut fields = Fields(BTreeMap::from([(
                "level".into(),
                event.metadata().level().to_string(),
            )]));
            event.record(&mut fields);
            self.0.lock().push(fields.0);
        }
    }

    #[tokio::test]
    async fn blocked_diagnostic_tracks_config_changes_and_all_retained_data() {
        let f = Fixture::new(0);
        f.enqueue(vec![Bytes::from_static(b"head")]);
        f.update(NAME, |stream| {
            stream
                .destination
                .as_mut()
                .unwrap()
                .buffering_interval_seconds = Some(60);
        });
        f.enqueue(vec![Bytes::from_static(b"active")]);
        f.update(NAME, |stream| {
            stream.destination.as_mut().unwrap().compression_format = Some("ZIP".into());
        });
        let capture = CapturedEvents::default();
        let scan = || {
            tracing::subscriber::with_default(capture.clone(), || {
                // Zero capacity still scans and diagnoses, without scheduling
                // the supported, already-detached head.
                assert!(f
                    .engine
                    .core
                    .prepare(0, true, false, &BTreeSet::new())
                    .is_empty());
            })
        };
        scan();
        f.update(NAME, |stream| {
            stream.version_id = "2".into();
            stream.destination.as_mut().unwrap().prefix = Some("changed/".into());
        });
        scan();
        scan();
        {
            let warnings = capture.0.lock();
            assert_eq!(warnings.len(), 2);
            for warning in warnings.iter() {
                assert_eq!(warning["retained_records"], "2");
                assert_eq!(warning["retained_bytes"], "10");
                assert!(warning["reason"].contains("ZIP"));
            }
        }
        f.turn().await;
        assert_eq!(f.batches().len(), 1);
        assert_eq!(f.batches()[0].records, vec![Bytes::from_static(b"head")]);
        assert_eq!(f.engine.shutdown().await.undelivered_bytes, 6);

        let queue_only = Fixture::new(0);
        queue_only.enqueue(vec![Bytes::from_static(b"detached")]);
        queue_only.update(NAME, |stream| {
            stream.destination.as_mut().unwrap().compression_format = Some("ZIP".into());
        });
        let no_pause = CapturedEvents::default();
        tracing::subscriber::with_default(no_pause.clone(), || {
            assert!(queue_only
                .engine
                .core
                .prepare(0, true, false, &BTreeSet::new())
                .is_empty());
        });
        assert!(no_pause.0.lock().is_empty(), "detached data is not paused");
        assert_eq!(queue_only.engine.shutdown().await.delivered_batches, 1);
    }

    #[tokio::test]
    async fn unsupported_update_pauses_active_and_restoration_keeps_deadline() {
        let f = Fixture::new(60);
        f.enqueue(vec![Bytes::from_static(b"accepted")]);
        f.advance(59);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().compression_format = Some("ZIP".into())
        });
        let error = f
            .engine
            .enqueue(ACCOUNT, REGION, NAME, vec![Bytes::from_static(b"rejected")])
            .unwrap_err();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "InvalidArgumentException");
        f.advance(1);
        f.turn().await;
        assert!(f.batches().is_empty());
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().compression_format = Some("GZIP".into());
            s.version_id = "2".into();
        });
        f.turn().await;
        assert_eq!(
            f.batches()[0].records,
            vec![Bytes::from_static(b"accepted")]
        );
        assert_eq!(f.batches()[0].version_id, "2");
        assert!(f.batches()[0].object_key.ends_with(".gz"));
    }

    #[tokio::test]
    async fn non_s3_update_pauses_active_until_removed() {
        let f = Fixture::new(60);
        f.enqueue(vec![Bytes::from_static(b"accepted")]);
        f.update(NAME, |stream| {
            stream
                .extra_destinations
                .insert("HttpEndpointDestinationDescription".into(), json!({}));
        });
        let error = f
            .engine
            .enqueue(ACCOUNT, REGION, NAME, vec![Bytes::from_static(b"rejected")])
            .expect_err("mixed non-S3 settings must reject new admission");
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "InvalidArgumentException");
        assert!(error
            .message()
            .contains("HttpEndpointDestinationConfiguration"));
        f.advance(60);
        f.turn().await;
        assert!(
            f.batches().is_empty(),
            "unsupported active data must remain paused"
        );
        f.update(NAME, |stream| stream.extra_destinations.clear());
        f.turn().await;
        assert_eq!(
            f.batches()[0].records,
            vec![Bytes::from_static(b"accepted")]
        );
    }

    #[tokio::test]
    async fn unsupported_shutdown_counts_paused_active() {
        let f = Fixture::new(60);
        f.enqueue(vec![Bytes::from_static(b"accepted"), Bytes::new()]);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().prefix = Some("!{timestamp:yyyy}/".into())
        });
        assert_eq!(
            f.engine.shutdown().await,
            DrainReport {
                delivered_batches: 0,
                undelivered_records: 2,
                undelivered_bytes: 8
            }
        );
        assert!(f.batches().is_empty());
    }

    #[tokio::test]
    async fn non_s3_shutdown_finalizes_paused_active() {
        let f = Fixture::new(60);
        f.enqueue(vec![Bytes::from_static(b"accepted"), Bytes::new()]);
        f.update(NAME, |stream| {
            stream
                .extra_destinations
                .insert("HttpEndpointDestinationDescription".into(), json!({}));
        });
        f.engine.core.registry.lock().closed = true;
        assert!(f
            .engine
            .core
            .prepare(4, false, true, &BTreeSet::new())
            .is_empty());
        // A paused active buffer must be counted, not declared ready forever.
        // Check finalization directly so the regression fails without spinning.
        let report = DrainReport {
            delivered_batches: 0,
            undelivered_records: 2,
            undelivered_bytes: 8,
        };
        assert_eq!(
            f.engine.core.finish_shutdown(&BTreeSet::new()),
            Some(report.clone())
        );
        assert_eq!(f.engine.shutdown().await, report);
        assert!(f.batches().is_empty());
    }

    #[tokio::test]
    async fn restoration_at_drain_finalization_is_attempted() {
        let f = Fixture::new(900);
        f.enqueue(vec![Bytes::from_static(b"paused")]);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().compression_format = Some("ZIP".into())
        });
        f.engine.core.registry.lock().closed = true;
        assert!(f
            .engine
            .core
            .prepare(4, false, true, &BTreeSet::new())
            .is_empty());
        // Model restoration exactly between the last empty readiness scan and
        // its report snapshot. A now-supported active buffer still gets a turn.
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().compression_format = Some("GZIP".into())
        });
        assert!(f.engine.core.finish_shutdown(&BTreeSet::new()).is_none());
        assert_eq!(
            f.engine.shutdown().await,
            DrainReport {
                delivered_batches: 1,
                undelivered_records: 0,
                undelivered_bytes: 0
            }
        );
        assert_eq!(f.batches()[0].records, vec![Bytes::from_static(b"paused")]);
    }

    #[tokio::test]
    async fn updates_capture_only_at_detachment() {
        let f = Fixture::new(60);
        f.enqueue(vec![Bytes::from_static(b"active")]);
        f.update(NAME, |s| {
            s.version_id = "2".into();
            let d = s.destination.as_mut().unwrap();
            d.bucket_arn = "arn:aws:s3:::new-bucket".into();
            d.prefix = Some("new/".into());
        });
        f.advance(60);
        // Capture before a later update, even if execution hasn't started yet.
        let mut jobs = Writes::default();
        f.schedule(&mut jobs, true);
        f.update(NAME, |s| {
            s.version_id = "3".into();
            s.destination.as_mut().unwrap().bucket_arn = "arn:aws:s3:::later-bucket".into();
        });
        f.finish(&mut jobs).await;
        let batches = f.batches();
        assert_eq!(batches[0].version_id, "2");
        assert_eq!(batches[0].destination.bucket, "new-bucket");
        assert!(batches[0]
            .object_key
            .starts_with("new/2026/10/12/23/events-2-"));
    }

    #[tokio::test]
    async fn delete_recreate_and_reset_prune_not_yet_due_and_failed_generations() {
        for reset in [false, true] {
            let f = Fixture::new(0);
            *f.engine.core.writer.write() = Some(Arc::new(FailingWriter));
            f.enqueue(vec![Bytes::from_static(b"failed")]);
            f.turn().await;
            f.update(NAME, |s| {
                s.destination.as_mut().unwrap().buffering_interval_seconds = Some(900)
            });
            f.enqueue(vec![Bytes::from_static(b"not due")]);
            if reset {
                f.engine.core.state.write().accounts.clear();
            } else {
                f.engine
                    .core
                    .state
                    .write()
                    .get_or_create(ACCOUNT, REGION)
                    .streams_mut(REGION)
                    .remove(NAME);
            }
            f.add_stream(ACCOUNT, REGION, NAME, 900);
            // Stale retry heads and active data are pruned before either is due.
            f.turn().await;
            assert!(f.engine.core.registry.lock().streams.is_empty());
            *f.engine.core.writer.write() = Some(f.writer.clone());
            f.enqueue(vec![Bytes::from_static(b"new generation")]);
            assert_eq!(f.engine.shutdown().await.undelivered_records, 0);
            assert_eq!(
                f.batches()[0].records,
                vec![Bytes::from_static(b"new generation")]
            );
        }
    }

    #[tokio::test]
    async fn no_s3_destination_ack_and_closed_gate() {
        let f = Fixture::new(0);
        f.update(NAME, |s| s.destination = None);
        f.enqueue(vec![Bytes::from_static(b"configuration only")]);
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
        assert!(f.batches().is_empty());
        assert_eq!(
            f.engine
                .enqueue(ACCOUNT, REGION, NAME, vec![Bytes::from_static(b"late")])
                .unwrap_err()
                .code(),
            "ServiceUnavailableException"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn started_worker_detaches_idle_buffer_on_tick() {
        let f = Fixture::new(60);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"idle")]);
        f.engine.start().unwrap();
        // Let startup register the timer, then move the independent clocks.
        tokio::task::yield_now().await;
        f.advance(59);
        tokio::time::advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;
        assert!(entered.try_recv().is_err());
        f.advance(1);
        tokio::time::advance(Duration::from_secs(1)).await;
        let due = next_attempt(&mut entered).await;
        assert_eq!(due.batch.records, vec![Bytes::from_static(b"idle")]);
        let engine = f.engine.clone();
        let drain = tokio::spawn(async move { engine.shutdown().await });
        wait_closed(&f.engine).await;
        due.reply.send(Action::Commit).unwrap();
        assert_eq!(bounded(drain).await.unwrap().delivered_batches, 1);
    }

    #[tokio::test]
    async fn panic_retry_reuses_arc_and_key() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"retain")]);
        let retained = f
            .engine
            .core
            .registry
            .lock()
            .streams
            .values()
            .next()
            .unwrap()
            .queue
            .front()
            .unwrap()
            .clone();
        let mut jobs = Writes::default();
        f.schedule(&mut jobs, false);
        let first = next_attempt(&mut entered).await;
        first.reply.send(Action::Panic).unwrap();
        f.finish(&mut jobs).await;
        f.advance(RETRY_INTERVAL.as_secs());
        f.schedule(&mut jobs, true);
        assert!(Arc::ptr_eq(
            jobs.batches.values().next().unwrap(),
            &retained
        ));
        let retry = next_attempt(&mut entered).await;
        assert_eq!(retry.batch.object_key, first.batch.object_key);
        assert_eq!(retry.batch.records, vec![Bytes::from_static(b"retain")]);
        retry.reply.send(Action::Commit).unwrap();
        f.finish(&mut jobs).await;
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
    }

    #[tokio::test]
    async fn cancelled_shutdown_can_resume_ordinary_and_final_jobs() {
        use std::future::{poll_fn, Future};
        use std::task::Poll;

        for started in [false, true] {
            let f = Fixture::new(0);
            let mut entered = f.controlled();
            f.enqueue(vec![Bytes::from_static(b"head")]);
            f.enqueue(vec![Bytes::from_static(b"successor")]);
            let mut ordinary = None;
            if started {
                f.engine.start().unwrap();
                ordinary = Some(next_attempt(&mut entered).await);
            }
            let mut first_drain = Box::pin(f.engine.shutdown());
            poll_fn(|cx| {
                assert!(first_drain.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            let head = if let Some(head) = ordinary {
                head
            } else {
                next_attempt(&mut entered).await
            };
            drop(first_drain);
            let mut resumed = Box::pin(f.engine.shutdown());
            poll_fn(|cx| {
                assert!(
                    resumed.as_mut().poll(cx).is_pending(),
                    "a repeated drain must await the retained in-flight job"
                );
                Poll::Ready(())
            })
            .await;
            head.reply.send(Action::Commit).unwrap();
            let (report, ()) = bounded(async {
                tokio::join!(resumed, async {
                    let successor = next_attempt(&mut entered).await;
                    assert_eq!(
                        successor.batch.records,
                        vec![Bytes::from_static(b"successor")]
                    );
                    successor.reply.send(Action::Commit).unwrap();
                })
            })
            .await;
            assert_eq!(
                report,
                DrainReport {
                    delivered_batches: 2,
                    undelivered_records: 0,
                    undelivered_bytes: 0
                }
            );
            assert!(entered.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn unsupported_update_does_not_cancel_detached_head() {
        let f = Fixture::new(0);
        let mut entered = f.controlled();
        f.enqueue(vec![Bytes::from_static(b"detached")]);
        let mut jobs = Writes::default();
        f.schedule(&mut jobs, false);
        let head = next_attempt(&mut entered).await;
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().buffering_interval_seconds = Some(60)
        });
        f.enqueue(vec![Bytes::from_static(b"active")]);
        f.update(NAME, |s| {
            s.destination.as_mut().unwrap().compression_format = Some("ZIP".into())
        });
        f.advance(60);
        f.schedule(&mut jobs, true);
        assert_eq!(jobs.len(), 1);
        head.reply.send(Action::Commit).unwrap();
        f.finish(&mut jobs).await;
        f.schedule(&mut jobs, true);
        assert!(jobs.is_empty());
        *f.engine.core.writer.write() = Some(f.writer.clone());
        assert_eq!(
            f.engine.shutdown().await,
            DrainReport {
                delivered_batches: 0,
                undelivered_records: 1,
                undelivered_bytes: 6
            }
        );
        assert!(f.batches().is_empty());
    }

    #[tokio::test]
    async fn missing_backend_retains_accepted_data_for_shutdown_report() {
        let f = Fixture::new(0);
        *f.engine.core.writer.write() = None;
        f.enqueue(vec![Bytes::from_static(b"accepted")]);
        f.turn().await;
        assert_eq!(
            f.engine.shutdown().await,
            DrainReport {
                delivered_batches: 0,
                undelivered_records: 1,
                undelivered_bytes: 8
            }
        );
    }

    struct BoundaryClock {
        clock: Arc<ControlledClock>,
        gate: Mutex<
            Option<(
                tokio::sync::mpsc::UnboundedSender<()>,
                std::sync::mpsc::Receiver<()>,
            )>,
        >,
    }

    impl Clock for BoundaryClock {
        fn now(&self) -> TimePoint {
            let gate = self.gate.lock().take();
            if let Some((entered, resume)) = gate {
                entered.send(()).unwrap();
                resume.recv_timeout(TEST_WAIT).unwrap();
            }
            self.clock.now()
        }
    }

    #[tokio::test]
    async fn admission_and_timer_capture_pin_configuration_through_detachment() {
        for admission in [true, false] {
            let mut f = Fixture::new(if admission { 0 } else { 60 });
            if !admission {
                f.enqueue(vec![Bytes::from_static(b"timer")]);
                f.advance(60);
            }
            let (entered, mut receive_entered) = tokio::sync::mpsc::unbounded_channel();
            let (resume, receive_resume) = std::sync::mpsc::channel();
            Arc::get_mut(&mut Arc::get_mut(&mut f.engine).unwrap().core)
                .unwrap()
                .clock = Arc::new(BoundaryClock {
                clock: f.clock.clone(),
                gate: Mutex::new(Some((entered, receive_resume))),
            });
            let engine = f.engine.clone();
            let decision = std::thread::spawn(move || {
                if admission {
                    engine
                        .enqueue(
                            ACCOUNT,
                            REGION,
                            NAME,
                            vec![Bytes::from_static(b"admission")],
                        )
                        .unwrap();
                    Vec::new()
                } else {
                    engine.core.prepare(4, true, false, &BTreeSet::new())
                }
            });
            bounded(receive_entered.recv()).await.unwrap();
            assert!(
                f.engine.core.state.try_write().is_none(),
                "configuration must be pinned when the detachment time is sampled"
            );
            assert!(
                f.engine.core.registry.try_lock().is_none(),
                "arrival and detachment time must be sampled inside the registry decision"
            );
            resume.send(()).unwrap();
            let ready = decision.join().unwrap();
            f.update(NAME, |s| {
                s.version_id = "2".into();
                s.destination.as_mut().unwrap().compression_format = Some("ZIP".into());
            });
            let mut jobs = Writes::default();
            launch(&mut jobs, ready);
            if !jobs.is_empty() {
                f.finish(&mut jobs).await;
            }
            f.turn().await;
            assert_eq!(f.batches().len(), 1);
            assert_eq!(f.batches()[0].version_id, "1");
            assert_eq!(
                f.batches()[0].destination.output.compression,
                crate::Compression::Uncompressed
            );
            assert_eq!(f.engine.shutdown().await, DrainReport::default());
        }
    }

    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_persistence::s3::{DiskS3Store, StoreResult};
    use fakecloud_persistence::{
        BodyRef, BodySource, BucketMeta, BucketSubresource, MpuInit, ObjectMeta,
    };
    use fakecloud_s3::{S3Bucket, S3State};
    use std::path::PathBuf;

    fn s3() -> SharedS3State {
        let mut state = MultiAccountState::<S3State>::new(ACCOUNT, REGION, "http://localhost");
        for bucket in ["bucket", "changed-bucket"] {
            state
                .get_or_create(ACCOUNT)
                .buckets
                .insert(bucket.into(), S3Bucket::new(bucket, REGION, "owner"));
        }
        Arc::new(RwLock::new(state))
    }

    fn disk(tmp: &tempfile::TempDir) -> DiskS3Store {
        let disk = DiskS3Store::new(
            tmp.path().join("s3"),
            Arc::new(fakecloud_persistence::cache::BodyCache::new(1024)),
        );
        for name in ["bucket", "changed-bucket"] {
            disk.put_bucket_meta(
                name,
                &BucketMeta {
                    name: name.into(),
                    region: REGION.into(),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        disk
    }

    #[tokio::test]
    async fn retained_adapter_constructor_exposes_explicit_lifecycle() {
        use fakecloud_core::delivery::FirehoseDelivery;

        let f = Fixture::new(0);
        let s3 = s3();
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = Arc::new(disk(&tmp));
        let adapter =
            crate::delivery::FirehoseDeliveryImpl::new(f.engine.shared_state(), s3.clone());
        adapter.set_s3_store(disk.clone());
        let engine = adapter.delivery_engine();
        let arn = "arn:aws:firehose:us-east-1:111111111111:deliverystream/events";
        adapter.put_record(arn, b"ordinary\x00\xff");
        assert!(
            !engine.worker.lock().started,
            "construction and handoff must be inert"
        );
        assert!(s3.read().get(ACCOUNT).unwrap().buckets["bucket"]
            .objects
            .is_empty());
        assert_eq!(
            engine
                .core
                .registry
                .lock()
                .streams
                .values()
                .next()
                .unwrap()
                .queue
                .len(),
            1
        );
        engine.start().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if s3.read().get(ACCOUNT).unwrap().buckets["bucket"]
                    .objects
                    .len()
                    == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("explicit start must deliver through the constructor's real writer");
        // A long-interval active record proves shutdown drains this same engine.
        f.update(NAME, |stream| {
            stream
                .destination
                .as_mut()
                .unwrap()
                .buffering_interval_seconds = Some(900);
        });
        adapter.put_record(arn, b"final");
        let report = engine.shutdown().await;
        assert_eq!(report.undelivered_records, 0);
        assert_eq!(report.undelivered_bytes, 0);
        // The first ordinary completion may be observed before or after cutoff.
        assert!((1..=2).contains(&report.delivered_batches));
        adapter.put_record(arn, b"too late");
        assert_eq!(
            engine
                .enqueue(ACCOUNT, REGION, NAME, vec![record(1, 1)])
                .unwrap_err()
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(engine.shutdown().await, report);
        assert!(engine.core.registry.lock().streams.is_empty());
        let objects = s3.read().get(ACCOUNT).unwrap().buckets["bucket"]
            .objects
            .clone();
        assert_eq!(objects.len(), 2);
        let loaded = disk.load().unwrap();
        assert_eq!(loaded.buckets["bucket"].objects.len(), 2);
        let mut bodies: Vec<_> = objects
            .keys()
            .map(|key| {
                let object = &loaded.buckets["bucket"].objects[key];
                let body = disk.open_object_body(&object.body).unwrap();
                assert_eq!(object.meta.size as usize, body.len());
                body.to_vec()
            })
            .collect();
        bodies.sort();
        assert_eq!(
            bodies,
            vec![b"final".to_vec(), b"ordinary\x00\xff".to_vec()]
        );
    }

    #[tokio::test]
    async fn late_s3_and_store_setters_share_live_writer_handle() {
        for store_first in [false, true] {
            let f = Fixture::new(0);
            let tmp = tempfile::TempDir::new().unwrap();
            let disk = Arc::new(disk(&tmp));
            let s3 = s3();
            if store_first {
                f.engine.set_s3_store(disk.clone());
            }
            f.engine.set_s3(s3.clone());
            f.enqueue(vec![Bytes::from_static(b"first\x00\xff")]);
            if !store_first {
                f.engine.set_s3_store(disk.clone());
            }
            f.turn().await;
            {
                let state = s3.read();
                let bucket = &state.get(ACCOUNT).unwrap().buckets["bucket"];
                assert_eq!(bucket.objects.len(), 1);
                let key = bucket.objects.keys().next().unwrap();
                let loaded = disk.load().unwrap();
                let object = &loaded.buckets["bucket"].objects[key];
                assert_eq!(
                    disk.open_object_body(&object.body).unwrap(),
                    Bytes::from_static(b"first\x00\xff")
                );
            }
            // Replacing a store after construction affects subsequent jobs too.
            let other_tmp = tempfile::TempDir::new().unwrap();
            let replacement = Arc::new(self::disk(&other_tmp));
            f.engine.set_s3_store(replacement.clone());
            f.enqueue(vec![Bytes::from_static(b"second")]);
            f.turn().await;
            let loaded = replacement.load().unwrap();
            assert_eq!(loaded.buckets["bucket"].objects.len(), 1);
            let object = loaded.buckets["bucket"].objects.values().next().unwrap();
            assert_eq!(
                replacement.open_object_body(&object.body).unwrap(),
                Bytes::from_static(b"second")
            );
            assert_eq!(disk.load().unwrap().buckets["bucket"].objects.len(), 1);
            assert_eq!(f.engine.shutdown().await, DrainReport::default());
        }
    }

    #[tokio::test]
    async fn committed_completion_survives_postcommit_generation_pruning() {
        for replace in [false, true] {
            let f = Fixture::new(0);
            let s3 = s3();
            let tmp = tempfile::TempDir::new().unwrap();
            let store = Arc::new(disk(&tmp));
            f.engine.set_s3(s3.clone());
            f.engine.set_s3_store(store.clone());
            f.enqueue(vec![Bytes::from_static(b"committed\x00\xff")]);
            f.engine.core.registry.lock().closed = true;

            let mut jobs = Writes::default();
            launch(
                &mut jobs,
                f.engine.core.prepare(4, false, true, &BTreeSet::new()),
            );
            // Retain the real successful join without processing it in the
            // engine, making post-commit invalidation deterministic.
            let completion = jobs.next().await.unwrap();
            assert_eq!(completion.result, Ok(DeliveryWriteOutcome::Committed));
            let key = completion.batch.batch.object_key.clone();
            assert!(s3.read().get(ACCOUNT).unwrap().buckets["bucket"]
                .objects
                .contains_key(&key));
            let loaded = store.load().unwrap();
            assert_eq!(
                store
                    .open_object_body(&loaded.buckets["bucket"].objects[&key].body)
                    .unwrap(),
                Bytes::from_static(b"committed\x00\xff")
            );

            if replace {
                f.update(NAME, |s| s.instance_id = uuid::Uuid::new_v4());
            } else {
                f.engine
                    .core
                    .state
                    .write()
                    .get_or_create(ACCOUNT, REGION)
                    .streams_mut(REGION)
                    .remove(NAME);
            }
            assert!(f
                .engine
                .core
                .prepare(4, false, true, &BTreeSet::new())
                .is_empty());
            assert!(f.engine.core.registry.lock().streams.is_empty());
            assert!(f.engine.core.complete(completion));
            assert!(jobs.next().await.is_none());

            let report = f.engine.shutdown().await;
            assert_eq!(
                report,
                DrainReport {
                    delivered_batches: 1,
                    undelivered_records: 0,
                    undelivered_bytes: 0,
                }
            );
            assert_eq!(f.engine.shutdown().await, report);
            assert!(f.engine.core.registry.lock().streams.is_empty());
            assert_eq!(
                s3.read().get(ACCOUNT).unwrap().buckets["bucket"]
                    .objects
                    .len(),
                1
            );
            assert_eq!(store.load().unwrap().buckets["bucket"].objects.len(), 1);
        }
    }

    #[tokio::test]
    async fn cancelled_completion_after_generation_pruning_is_not_delivered() {
        let f = Fixture::new(0);
        let s3 = s3();
        let tmp = tempfile::TempDir::new().unwrap();
        let store = Arc::new(disk(&tmp));
        f.engine.set_s3(s3.clone());
        f.engine.set_s3_store(store.clone());
        f.enqueue(vec![Bytes::from_static(b"cancelled")]);
        f.engine.core.registry.lock().closed = true;
        let ready = f.engine.core.prepare(4, false, true, &BTreeSet::new());
        // Invalidate before the real writer's commit guard, contrasting with
        // the successful write retained by the post-commit regression.
        f.update(NAME, |s| s.instance_id = uuid::Uuid::new_v4());
        let mut jobs = Writes::default();
        launch(&mut jobs, ready);
        let completion = jobs.next().await.unwrap();
        assert_eq!(completion.result, Ok(DeliveryWriteOutcome::Cancelled));
        assert!(f
            .engine
            .core
            .prepare(4, false, true, &BTreeSet::new())
            .is_empty());
        assert!(f.engine.core.registry.lock().streams.is_empty());
        assert!(f.engine.core.complete(completion));
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
        assert!(s3.read().get(ACCOUNT).unwrap().buckets["bucket"]
            .objects
            .is_empty());
        assert!(store.load().unwrap().buckets["bucket"].objects.is_empty());
    }

    // Only the staging boundary is controlled. Durable commits/loads use the
    // real disk store, and generation authorization uses the real S3BatchWriter.
    struct StagingStore {
        disk: DiskS3Store,
        boundary: Box<dyn Fn() + Send + Sync>,
    }

    macro_rules! forward_store {
        ($($name:ident($($arg:ident: $ty:ty),*) -> $result:ty;)*) => {
            $(fn $name(&self, $($arg: $ty),*) -> $result { self.disk.$name($($arg),*) })*
        };
    }

    impl S3Store for StagingStore {
        forward_store! {
            load() -> StoreResult<fakecloud_persistence::s3::S3State>;
            put_bucket_meta(bucket: &str, meta: &BucketMeta) -> StoreResult<()>;
            put_bucket_subresource(bucket: &str, kind: BucketSubresource, payload: &str) -> StoreResult<()>;
            delete_bucket_subresource(bucket: &str, kind: BucketSubresource) -> StoreResult<()>;
            delete_bucket(bucket: &str) -> StoreResult<()>;
            put_object_meta(bucket: &str, key: &str, version: Option<&str>, meta: &ObjectMeta) -> StoreResult<()>;
            delete_object(bucket: &str, key: &str, version: Option<&str>) -> StoreResult<()>;
            open_object_body(body: &BodyRef) -> StoreResult<Bytes>;
            put_object(bucket: &str, key: &str, version: Option<&str>, body: BodySource, meta: &ObjectMeta) -> StoreResult<BodyRef>;
            mpu_create(bucket: &str, upload_id: &str, init: &MpuInit) -> StoreResult<()>;
            mpu_put_part(bucket: &str, upload_id: &str, part_number: u32, body: BodySource, etag: &str, checksum: Option<fakecloud_persistence::PartChecksum<'_>>) -> StoreResult<BodyRef>;
            mpu_abort(bucket: &str, upload_id: &str) -> StoreResult<()>;
            mpu_complete(bucket: &str, upload_id: &str, final_key: &str, version: Option<&str>, meta: &ObjectMeta) -> StoreResult<BodyRef>;
        }

        fn spool_dir(&self) -> Option<PathBuf> {
            (self.boundary)();
            self.disk.spool_dir()
        }
    }

    async fn staged_mutation(
        edit: impl FnOnce(&mut FirehoseAccounts) + Send + 'static,
    ) -> (Fixture, SharedS3State, Arc<StagingStore>, tempfile::TempDir) {
        let f = Fixture::new(0);
        let s3 = s3();
        let tmp = tempfile::TempDir::new().unwrap();
        let disk = disk(&tmp);
        let spool = disk.spool_dir().unwrap();
        let (entered, receive_entered) = std::sync::mpsc::channel();
        let (resume, receive_resume) = std::sync::mpsc::channel();
        let receive_resume = Mutex::new(receive_resume);
        let store = Arc::new(StagingStore {
            disk,
            boundary: Box::new(move || {
                entered.send(()).unwrap();
                receive_resume.lock().recv_timeout(TEST_WAIT).unwrap();
            }),
        });
        f.engine.set_s3(s3.clone());
        f.engine.set_s3_store(store.clone());
        let state = f.engine.core.state.clone();
        let registry = f.engine.core.clone();
        let check_s3 = s3.clone();
        let mutation = std::thread::spawn(move || {
            receive_entered.recv_timeout(TEST_WAIT).unwrap();
            assert!(
                check_s3.try_write().is_some(),
                "staging must not hold S3 lock"
            );
            assert!(
                registry.registry.try_lock().is_some(),
                "writer must not hold registry lock"
            );
            // Pin before_commit while the real spool is written. This thread
            // holds no guard across await; all waits are explicit boundary/data
            // conditions, with no elapsed-time assumptions or sleeps.
            let mut config = state.write();
            resume.send(()).unwrap();
            let deadline = std::time::Instant::now() + TEST_WAIT;
            loop {
                let staged = std::fs::read_dir(&spool).ok().is_some_and(|entries| {
                    entries.filter_map(Result::ok).any(|entry| {
                        std::fs::read(entry.path()).ok().as_deref()
                            == Some(b"staged\x00\xff".as_slice())
                    })
                });
                if staged {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "real staging did not complete"
                );
                std::thread::yield_now();
            }
            edit(&mut config);
        });
        f.enqueue(vec![Bytes::from_static(b"staged\x00\xff")]);
        f.turn().await;
        mutation.join().unwrap();
        (f, s3, store, tmp)
    }

    #[tokio::test]
    async fn delete_and_reset_between_stage_and_commit_cancel_engine_job() {
        for mutation in 0..4 {
            let (f, s3, store, _tmp) = staged_mutation(move |state| match mutation {
                0 => {
                    state
                        .get_or_create(ACCOUNT, REGION)
                        .streams_mut(REGION)
                        .remove(NAME);
                }
                1 => {
                    state.accounts.clear();
                }
                2 => {
                    let stream = state
                        .get_or_create(ACCOUNT, REGION)
                        .streams_mut(REGION)
                        .get_mut(NAME)
                        .unwrap();
                    stream.instance_id = uuid::Uuid::new_v4();
                }
                3 => {
                    *state =
                        serde_json::from_value(serde_json::to_value(&*state).unwrap()).unwrap();
                }
                _ => unreachable!(),
            })
            .await;
            assert!(s3.read().get(ACCOUNT).unwrap().buckets["bucket"]
                .objects
                .is_empty());
            assert!(store.load().unwrap().buckets["bucket"].objects.is_empty());
            assert_eq!(
                std::fs::read_dir(store.disk.spool_dir().unwrap())
                    .unwrap()
                    .count(),
                0
            );
            assert_eq!(f.engine.shutdown().await, DrainReport::default());
        }
    }

    #[tokio::test]
    async fn update_between_stage_and_commit_preserves_captured_destination() {
        let (f, s3, store, _tmp) = staged_mutation(|state| {
            let stream = state
                .get_or_create(ACCOUNT, REGION)
                .streams_mut(REGION)
                .get_mut(NAME)
                .unwrap();
            stream.version_id = "2".into();
            let d = stream.destination.as_mut().unwrap();
            d.bucket_arn = "arn:aws:s3:::changed-bucket".into();
            d.prefix = Some("changed/".into());
            d.compression_format = Some("ZIP".into());
        })
        .await;
        {
            let state = s3.read();
            let account = state.get(ACCOUNT).unwrap();
            assert!(account.buckets["changed-bucket"].objects.is_empty());
            let objects = &account.buckets["bucket"].objects;
            assert_eq!(objects.len(), 1);
            let key = objects.keys().next().unwrap();
            assert!(key.starts_with("2026/10/12/23/events-1-"));
            let loaded = store.load().unwrap();
            assert!(loaded.buckets["changed-bucket"].objects.is_empty());
            assert_eq!(
                store
                    .open_object_body(&loaded.buckets["bucket"].objects[key].body)
                    .unwrap(),
                Bytes::from_static(b"staged\x00\xff")
            );
        }
        assert_eq!(f.engine.shutdown().await, DrainReport::default());
    }
}
