//! Background poller bridging Apache Kafka topics -> Lambda event source
//! mappings: Amazon MSK clusters (`EventSourceArn` a `kafka` cluster ARN,
//! read through the cluster's real broker container) and self-managed Kafka
//! (`SelfManagedEventSource.Endpoints.KAFKA_BOOTSTRAP_SERVERS`).
//!
//! Each enabled mapping gets its own consumer task, a member of the consumer
//! group its Kafka config names (`ConsumerGroupId`, the mapping's UUID unless
//! set). A partition the group has no committed offset for starts at the
//! mapping's `StartingPosition` (`TRIM_HORIZON`, `LATEST`, `AT_TIMESTAMP`),
//! which is committed at once so the group's position is pinned from the
//! moment the mapping starts. Records are batched up to `BatchSize`, the
//! 6 MB synchronous payload limit, or the batching window
//! (`MaximumBatchingWindowInSeconds`, 500 ms by default), and the function is
//! invoked with Lambda's Kafka event (`eventSource` `aws:kafka` or
//! `SelfManagedKafka`, records keyed `<topic>-<partition>`). Offsets are
//! committed to the broker only once a batch succeeded (or was discarded), so
//! a restart, a disabled-then-enabled mapping, or a new mapping in the same
//! group resumes after the last handled record.
//!
//! A failing batch is retried until it succeeds or its records expire, as on
//! AWS; `MaximumRetryAttempts` (10 when an OnFailure destination is set and
//! no value is), `MaximumRecordAgeInSeconds`, `BisectBatchOnFunctionError`
//! and the OnFailure destination (SQS, SNS, S3, with a `KafkaBatchInfo`
//! record) follow `stream_esm_failures`.
//!
//! Records are fetched `read_committed` (aborted transactions dropped; AWS
//! does not document its poller's level). Commits never move a group offset
//! below what the session read as committed, and the API refuses a second
//! mapping in a group. A leaderless partition is skipped until a later
//! metadata refresh, an unreadable one (legacy message format, corrupt
//! batch) is reported and skipped, and the partition that leads a fetch
//! rotates so an oversized batch (read alone) never starves.
//!
//! Authentication: a self-managed cluster is reached over PLAINTEXT, or
//! SASL_PLAINTEXT with `BASIC_AUTH` (SASL/PLAIN) or `SASL_SCRAM_256_AUTH` /
//! `SASL_SCRAM_512_AUTH` credentials read from Secrets Manager. An MSK
//! cluster's broker serves every client on its PLAINTEXT listener, so its
//! `SASL_SCRAM_512_AUTH` / `CLIENT_CERTIFICATE_TLS_AUTH` settings are checked
//! against the cluster as MSK would (the scheme enabled on the cluster, the
//! secret associated with it and readable). What cannot be honored (TLS to a
//! self-managed broker, OAUTHBEARER, the `Queue` consumption mode, a schema
//! registry) is refused in the mapping's `LastProcessingResult`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use chrono::{TimeZone, Utc};
use serde_json::{json, Map, Value};

use fakecloud_core::delivery::{DeliveryBus, KmsHook, LambdaDelivery};
use fakecloud_kafka::client::{
    self, ClientError, ConsumedRecord, KafkaClient, SaslCredentials, SaslMechanism, ScramAlgorithm,
};
use fakecloud_kafka::SharedKafkaState;
use fakecloud_lambda::filter::FilterSet;
use fakecloud_lambda::{EventSourceMapping, LambdaInvocation, SharedLambdaState};
use fakecloud_secretsmanager::SharedSecretsManagerState;

use crate::stream_esm_failures::{
    deliver_failure_record, Attempt, DiscardCause, FailurePolicy, Pending, Resolution,
    RetryTracker, Step,
};

const B64: base64::engine::GeneralPurpose = base64::engine::general_purpose::STANDARD;

const SUPERVISE_INTERVAL: Duration = Duration::from_secs(1);
/// The batching window of a mapping created without
/// `MaximumBatchingWindowInSeconds`.
const DEFAULT_BATCHING_WINDOW: Duration = Duration::from_millis(500);
/// The synchronous invocation payload limit a batch is cut at.
const MAX_PAYLOAD_BYTES: usize = 6 * 1024 * 1024;
/// How long a Fetch waits on the broker for records.
const FETCH_MAX_WAIT_MS: i32 = 250;
const FETCH_MAX_BYTES: i32 = 8 * 1024 * 1024;
const FETCH_PARTITION_MAX_BYTES: i32 = 1024 * 1024;
const METADATA_REFRESH: Duration = Duration::from_secs(10);
const RECONNECT_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// `MaximumRetryAttempts` of a mapping with an OnFailure destination and no
/// value of its own.
const RETRIES_WITH_DESTINATION: u32 = 10;
/// Attempts at finding a group coordinator a broker is still electing.
const COORDINATOR_ATTEMPTS: u32 = 20;

const NO_BROKER: &str = "PROBLEM: Connection error. The cluster has no running Kafka broker to read from: fakecloud runs an MSK cluster's broker in a container, which needs Docker or Podman.";
const FUNCTION_CALL_FAILED: &str = "PROBLEM: Function call failed";

/// Where a mapping's records come from.
#[derive(Debug, Clone, PartialEq)]
enum KafkaSource {
    Msk { cluster_arn: String },
    SelfManaged { bootstrap: Vec<String> },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum StartingPosition {
    TrimHorizon,
    Latest,
    /// Milliseconds since the epoch.
    AtTimestamp(i64),
}

/// What a consumer task needs of its mapping. A change to any of it
/// restarts the task.
#[derive(Debug, Clone, PartialEq)]
struct KafkaMapping {
    uuid: String,
    account: String,
    region: String,
    function_arn: String,
    source: KafkaSource,
    topics: Vec<String>,
    group_id: String,
    batch_size: usize,
    batching_window: Duration,
    starting_position: StartingPosition,
    filter_patterns: Vec<String>,
    policy: FailurePolicy,
    source_access: Vec<Value>,
    /// A setting this poller cannot honor, as the mapping reports it.
    unsupported: Option<String>,
}

impl KafkaMapping {
    fn of(m: &EventSourceMapping, account: &str, region: &str) -> Option<Self> {
        let (source, config) = if m.event_source_arn.split(':').nth(2) == Some("kafka") {
            (
                KafkaSource::Msk {
                    cluster_arn: m.event_source_arn.clone(),
                },
                m.amazon_managed_kafka_event_source_config.as_ref(),
            )
        } else if m.event_source_arn.is_empty() {
            let bootstrap: Vec<String> = m
                .self_managed_event_source
                .as_ref()?
                .pointer("/Endpoints/KAFKA_BOOTSTRAP_SERVERS")?
                .as_array()?
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();
            if bootstrap.is_empty() {
                return None;
            }
            (
                KafkaSource::SelfManaged { bootstrap },
                m.self_managed_kafka_event_source_config.as_ref(),
            )
        } else {
            return None;
        };
        let group_id = config
            .and_then(|c| c["ConsumerGroupId"].as_str())
            .filter(|g| !g.is_empty())
            .unwrap_or(&m.uuid)
            .to_string();
        let unsupported = if config.is_some_and(|c| c["ConsumptionMode"].as_str() == Some("Queue"))
        {
            Some("PROBLEM: The Queue consumption mode (Kafka share groups) is not supported by fakecloud's Kafka poller; use the Stream consumption mode.".to_string())
        } else if config.is_some_and(|c| c["SchemaRegistryConfig"].is_object()) {
            Some("PROBLEM: Schema registry integration (SchemaRegistryConfig) is not supported by fakecloud's Kafka poller.".to_string())
        } else {
            None
        };
        let mut policy = FailurePolicy::of(m);
        // Kafka retries default to infinite, capped at 10 with an OnFailure
        // destination (the `MaximumRetryAttempts` member's documentation).
        if policy.on_failure.is_some() && policy.max_retries.is_none() {
            policy.max_retries = Some(RETRIES_WITH_DESTINATION);
        }
        // A Kafka partial batch response's item identifiers are not
        // interpreted: a batch succeeds or fails whole.
        policy.report_batch_item_failures = false;
        let starting_position = match m.starting_position.as_deref() {
            Some("LATEST") => StartingPosition::Latest,
            Some("AT_TIMESTAMP") => StartingPosition::AtTimestamp(
                (m.starting_position_timestamp.unwrap_or_default() * 1000.0) as i64,
            ),
            _ => StartingPosition::TrimHorizon,
        };
        Some(Self {
            uuid: m.uuid.clone(),
            account: account.to_string(),
            region: region.to_string(),
            function_arn: m.function_arn.clone(),
            source,
            topics: m.topics.clone(),
            group_id,
            batch_size: m.batch_size.clamp(1, 10_000) as usize,
            batching_window: m
                .maximum_batching_window_in_seconds
                .map(|s| Duration::from_secs(s.max(0) as u64))
                .unwrap_or(DEFAULT_BATCHING_WINDOW),
            starting_position,
            filter_patterns: m.filter_patterns.clone(),
            policy,
            source_access: m.source_access_configurations.clone(),
            unsupported,
        })
    }

    fn event_source(&self) -> &'static str {
        match self.source {
            KafkaSource::Msk { .. } => "aws:kafka",
            KafkaSource::SelfManaged { .. } => "SelfManagedKafka",
        }
    }

    fn cluster_arn(&self) -> Option<&str> {
        match &self.source {
            KafkaSource::Msk { cluster_arn } => Some(cluster_arn),
            KafkaSource::SelfManaged { .. } => None,
        }
    }

    /// The `URI` of the mapping's source access configuration of `kind`.
    fn access(&self, kind: &str) -> Option<&str> {
        self.source_access
            .iter()
            .find(|c| c["Type"].as_str() == Some(kind))
            .and_then(|c| c["URI"].as_str())
    }

    fn access_types(&self) -> impl Iterator<Item = &str> {
        self.source_access.iter().filter_map(|c| c["Type"].as_str())
    }
}

/// How the poller reaches a mapping's cluster.
#[derive(Debug, Clone, PartialEq)]
struct Endpoint {
    bootstrap: Vec<String>,
    /// The event's `bootstrapServers`.
    bootstrap_servers: String,
    sasl: Option<SaslCredentials>,
}

/// A record read but not yet handled.
#[derive(Debug, Clone)]
struct Buffered {
    topic: String,
    partition: i32,
    record: ConsumedRecord,
}

impl Buffered {
    /// The event's `records` key.
    fn partition_key(&self) -> String {
        format!("{}-{}", self.topic, self.partition)
    }

    /// The retry tracker's identifier for the record.
    fn id(&self) -> String {
        format!("{}-{}@{}", self.topic, self.partition, self.record.offset)
    }

    fn timestamp(&self) -> chrono::DateTime<Utc> {
        Utc.timestamp_millis_opt(self.record.timestamp_ms)
            .single()
            .unwrap_or_else(Utc::now)
    }
}

type TopicPartition = (String, i32);

/// `(topic, partition, offset)` to fetch from.
type FetchTarget = (String, i32, i64);

/// One record as the function receives it.
fn event_record(b: &Buffered) -> Value {
    let r = &b.record;
    let mut rec = Map::new();
    rec.insert("topic".into(), json!(b.topic));
    rec.insert("partition".into(), json!(b.partition));
    rec.insert("offset".into(), json!(r.offset));
    rec.insert("timestamp".into(), json!(r.timestamp_ms));
    rec.insert("timestampType".into(), json!(r.timestamp_type.as_str()));
    if let Some(key) = &r.key {
        rec.insert("key".into(), json!(B64.encode(key)));
    }
    if let Some(value) = &r.value {
        rec.insert("value".into(), json!(B64.encode(value)));
    }
    // Header values are byte arrays, as Lambda's (Java) poller writes a
    // `byte[]`: signed bytes.
    let headers: Vec<Value> = r
        .headers
        .iter()
        .map(|(name, value)| {
            let bytes = value
                .as_ref()
                .map(|v| json!(v.iter().map(|b| *b as i8).collect::<Vec<_>>()))
                .unwrap_or(Value::Null);
            let mut h = Map::new();
            h.insert(name.clone(), bytes);
            Value::Object(h)
        })
        .collect();
    rec.insert("headers".into(), Value::Array(headers));
    Value::Object(rec)
}

/// What a filter pattern sees of a record: its `value` decoded, as JSON
/// when it is JSON, else as text.
fn filter_view(b: &Buffered, record: &Value) -> Value {
    let mut view = record.clone();
    if let Some(text) = b
        .record
        .value
        .as_deref()
        .and_then(|v| std::str::from_utf8(v).ok())
    {
        view["value"] =
            serde_json::from_str::<Value>(text).unwrap_or_else(|_| Value::String(text.to_string()));
    }
    view
}

/// The function's event for `records` (already shaped by [`event_record`]).
fn kafka_event(m: &KafkaMapping, bootstrap_servers: &str, records: &[(&Buffered, Value)]) -> Value {
    let mut by_partition = Map::new();
    for (b, rec) in records {
        if let Value::Array(list) = by_partition
            .entry(b.partition_key())
            .or_insert_with(|| Value::Array(Vec::new()))
        {
            list.push(rec.clone());
        }
    }
    let mut event = Map::new();
    event.insert("eventSource".into(), json!(m.event_source()));
    if let Some(arn) = m.cluster_arn() {
        event.insert("eventSourceArn".into(), json!(arn));
    }
    event.insert("bootstrapServers".into(), json!(bootstrap_servers));
    event.insert("records".into(), Value::Object(by_partition));
    Value::Object(event)
}

/// How many of `sizes` (each record's serialized size) fit one invocation:
/// at most `batch_size`, within the payload limit, never fewer than one.
fn batch_len_within(sizes: &[usize], batch_size: usize, limit: usize) -> usize {
    let mut total = 0usize;
    let mut n = 0usize;
    for size in sizes.iter().take(batch_size) {
        // `,` between records plus the partition key arrays, roughly.
        total += size + 64;
        if n > 0 && total > limit {
            break;
        }
        n += 1;
    }
    n
}

/// The commit offsets of handling the first `n` buffered records: each
/// partition's next offset after its last handled record.
fn consumed_offsets(handled: &[Buffered]) -> HashMap<TopicPartition, i64> {
    let mut out: HashMap<TopicPartition, i64> = HashMap::new();
    for b in handled {
        let next = b.record.offset + 1;
        out.entry((b.topic.clone(), b.partition))
            .and_modify(|o| *o = (*o).max(next))
            .or_insert(next);
    }
    out
}

/// The OnFailure record of a discarded Kafka batch.
fn failure_record(
    m: &KafkaMapping,
    bootstrap_servers: &str,
    batch: &[Buffered],
    invoke_count: u32,
    cause: &DiscardCause,
    payload: &str,
    with_payload: bool,
) -> Value {
    let unqualified: String = m
        .function_arn
        .splitn(8, ':')
        .take(7)
        .collect::<Vec<_>>()
        .join(":");
    let ts = |b: &Buffered| b.timestamp().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    let mut records_info = Map::new();
    for b in batch {
        let entry = records_info.entry(b.partition_key()).or_insert_with(|| {
            json!({
                "firstRecordOffset": b.record.offset.to_string(),
                "firstRecordTimestamp": ts(b),
            })
        });
        entry["lastRecordOffset"] = json!(b.record.offset.to_string());
        entry["lastRecordTimestamp"] = json!(ts(b));
    }
    let mut info = Map::new();
    info.insert("batchSize".into(), json!(batch.len()));
    if let Some(arn) = m.cluster_arn() {
        info.insert("eventSourceArn".into(), json!(arn));
    }
    info.insert("bootstrapServers".into(), json!(bootstrap_servers));
    info.insert("payloadSize".into(), json!(payload.len()));
    info.insert("recordsInfo".into(), Value::Object(records_info));
    let mut record = json!({
        "requestContext": {
            "requestId": uuid::Uuid::new_v4().to_string(),
            "functionArn": unqualified,
            "condition": match cause {
                DiscardCause::RetriesExhausted { .. } => "RetryAttemptsExhausted",
                DiscardCause::RecordAge => "RecordAgeExceeded",
            },
            "approximateInvokeCount": invoke_count,
        },
        "version": "1.0",
        "timestamp": Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        "KafkaBatchInfo": Value::Object(info),
    });
    if let DiscardCause::RetriesExhausted { function_error } = cause {
        record["responseContext"] = json!({
            "statusCode": 200,
            "executedVersion": "$LATEST",
            "functionError": function_error,
        });
    }
    if with_payload {
        record["payload"] = Value::String(payload.to_string());
    }
    record
}

/// A client error as the mapping reports it.
fn problem(err: &ClientError) -> String {
    match err {
        ClientError::Authentication(reason) => {
            format!("PROBLEM: SASL authentication failed. {reason}")
        }
        ClientError::Broker { code, .. }
            if *code == client::ERROR_TOPIC_AUTHORIZATION_FAILED
                || *code == client::ERROR_GROUP_AUTHORIZATION_FAILED =>
        {
            format!("PROBLEM: Cluster failed to authorize Lambda. {err}")
        }
        ClientError::Connection { .. } => format!(
            "PROBLEM: Connection error. Please check your event source connection configuration. {err}"
        ),
        _ => format!("PROBLEM: {err}"),
    }
}

/// A connected consumer for one mapping.
struct Session {
    client: KafkaClient,
    bootstrap_servers: String,
    coordinator: Option<String>,
    /// Partition -> leader node.
    partitions: Vec<(TopicPartition, i32)>,
    /// Next offset to fetch, per partition.
    positions: HashMap<TopicPartition, i64>,
    /// Offsets handled but not committed yet.
    dirty: HashMap<TopicPartition, i64>,
    buffer: VecDeque<Buffered>,
    buffered_since: Option<Instant>,
    last_metadata: Option<Instant>,
    /// How long the partition placement is trusted: shorter while a
    /// partition has no leader.
    refresh_after: Duration,
    /// The group's committed offsets as this session knows them: a commit
    /// never moves one back.
    floor: HashMap<TopicPartition, i64>,
    /// Partitions whose records cannot be read, skipped until the next
    /// metadata refresh.
    blocked: HashMap<TopicPartition, String>,
    /// A problem to report that does not end the session.
    pending_problem: Option<String>,
    /// Which partition leads the next fetch, so every partition is first in
    /// turn.
    rotation: usize,
    /// A batch failed and waits for its retry.
    retrying: bool,
}

/// Leadership is moving: the partition is retried once metadata is fresh.
fn is_leader_error(code: Option<i16>) -> bool {
    matches!(
        code,
        Some(client::ERROR_NOT_LEADER_OR_FOLLOWER)
            | Some(client::ERROR_LEADER_NOT_AVAILABLE)
            | Some(client::ERROR_UNKNOWN_TOPIC_OR_PARTITION)
    )
}

/// The wait after a fetch hit a partition whose leadership is moving.
const LEADER_BACKOFF: Duration = Duration::from_millis(200);
/// How soon metadata is read again while a partition has no leader.
const LEADERLESS_REFRESH: Duration = Duration::from_secs(1);

/// The `(topic, partition, offset)` fetch targets, starting `rotation`
/// places in, so no partition is always behind the others in a fetch.
fn rotated<T: Clone>(targets: &[T], rotation: usize) -> Vec<T> {
    if targets.is_empty() {
        return Vec::new();
    }
    let start = rotation % targets.len();
    targets[start..]
        .iter()
        .chain(&targets[..start])
        .cloned()
        .collect()
}

/// The offsets of `dirty` worth committing: none at or below what the group
/// has committed already.
fn commit_targets(
    dirty: &HashMap<TopicPartition, i64>,
    floor: &HashMap<TopicPartition, i64>,
) -> Vec<FetchTarget> {
    let mut out: Vec<FetchTarget> = dirty
        .iter()
        .filter(|(tp, o)| floor.get(*tp).is_none_or(|f| *o > f))
        .map(|((t, p), o)| (t.clone(), *p, *o))
        .collect();
    out.sort();
    out
}

impl Session {
    fn new(endpoint: Endpoint, client_id: String, dirty: HashMap<TopicPartition, i64>) -> Self {
        Self {
            client: KafkaClient::new(endpoint.bootstrap, client_id, endpoint.sasl),
            bootstrap_servers: endpoint.bootstrap_servers,
            coordinator: None,
            partitions: Vec::new(),
            positions: HashMap::new(),
            dirty,
            buffer: VecDeque::new(),
            buffered_since: None,
            last_metadata: None,
            refresh_after: METADATA_REFRESH,
            floor: HashMap::new(),
            blocked: HashMap::new(),
            pending_problem: None,
            rotation: 0,
            retrying: false,
        }
    }

    fn metadata_stale(&self) -> bool {
        self.last_metadata
            .is_none_or(|t| t.elapsed() >= self.refresh_after)
    }

    fn leader(&self, tp: &TopicPartition) -> Option<i32> {
        self.partitions
            .iter()
            .find(|(p, _)| p == tp)
            .map(|(_, leader)| *leader)
    }

    async fn coordinator(&mut self, group: &str) -> Result<String, String> {
        if let Some(c) = &self.coordinator {
            return Ok(c.clone());
        }
        let mut last = None;
        for _ in 0..COORDINATOR_ATTEMPTS {
            match self.client.find_coordinator(group).await {
                Ok(addr) => {
                    self.coordinator = Some(addr.clone());
                    return Ok(addr);
                }
                // A fresh broker creates its offsets topic on first use.
                Err(e)
                    if matches!(
                        e.broker_code(),
                        Some(client::ERROR_COORDINATOR_NOT_AVAILABLE)
                            | Some(client::ERROR_COORDINATOR_LOAD_IN_PROGRESS)
                    ) =>
                {
                    last = Some(e);
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => return Err(problem(&e)),
            }
        }
        Err(problem(&last.expect("at least one attempt")))
    }

    /// The offset a partition with no committed offset starts at.
    async fn start_offset(
        &mut self,
        position: StartingPosition,
        tp: &TopicPartition,
        leader: i32,
    ) -> Result<i64, ClientError> {
        let at = match position {
            StartingPosition::TrimHorizon => client::OFFSET_EARLIEST,
            StartingPosition::Latest => client::OFFSET_LATEST,
            StartingPosition::AtTimestamp(ms) => ms,
        };
        if let Some(offset) = self.client.list_offset(leader, &tp.0, tp.1, at).await? {
            return Ok(offset);
        }
        // No record is as recent as the timestamp: start after the last.
        Ok(self
            .client
            .list_offset(leader, &tp.0, tp.1, client::OFFSET_LATEST)
            .await?
            .unwrap_or_default())
    }

    /// Learn the topics' partitions; new ones start at the group's committed
    /// offset, else at the starting position (committed right away).
    async fn refresh_partitions(&mut self, m: &KafkaMapping) -> Result<(), String> {
        let topics = self
            .client
            .metadata(&m.topics)
            .await
            .map_err(|e| problem(&e))?;
        let mut found = Vec::new();
        let mut leaderless = false;
        for topic in &m.topics {
            let Some(meta) = topics.iter().find(|t| &t.name == topic) else {
                return Err(format!("PROBLEM: Topic not found: {topic}"));
            };
            match meta.error_code {
                0 => {}
                client::ERROR_UNKNOWN_TOPIC_OR_PARTITION => {
                    return Err(format!("PROBLEM: Topic not found: {topic}"))
                }
                code => {
                    return Err(problem(&ClientError::Broker {
                        code,
                        context: format!(": topic {topic}"),
                    }))
                }
            }
            for p in &meta.partitions {
                // A partition electing a leader is skipped until it has one.
                if p.leader < 0 || p.error_code == client::ERROR_LEADER_NOT_AVAILABLE {
                    leaderless = true;
                    continue;
                }
                found.push(((topic.clone(), p.partition), p.leader));
            }
        }
        let new: Vec<TopicPartition> = found
            .iter()
            .map(|(tp, _)| tp.clone())
            .filter(|tp| !self.positions.contains_key(tp))
            .collect();
        self.partitions = found;
        self.last_metadata = Some(Instant::now());
        self.blocked.clear();
        self.refresh_after = if leaderless {
            LEADERLESS_REFRESH
        } else {
            METADATA_REFRESH
        };
        if new.is_empty() {
            return Ok(());
        }
        let coordinator = self.coordinator(&m.group_id).await?;
        let committed = match self
            .client
            .offset_fetch(&coordinator, &m.group_id, &new)
            .await
        {
            Ok(c) => c,
            Err(e) => {
                self.coordinator = None;
                return Err(problem(&e));
            }
        };
        for tp in new {
            let group_offset = committed.get(&tp).copied().flatten();
            if let Some(offset) = group_offset {
                self.floor.insert(tp.clone(), offset);
            }
            let offset = match self.dirty.get(&tp).copied().or(group_offset) {
                Some(offset) => offset,
                None => {
                    let leader = self.leader(&tp).unwrap_or(-1);
                    let offset = match self.start_offset(m.starting_position, &tp, leader).await {
                        Ok(offset) => offset,
                        // Leadership moved since the metadata: the partition
                        // starts after the next refresh.
                        Err(e) if is_leader_error(e.broker_code()) => {
                            self.refresh_after = LEADERLESS_REFRESH;
                            continue;
                        }
                        Err(e) => return Err(problem(&e)),
                    };
                    self.dirty.insert(tp.clone(), offset);
                    offset
                }
            };
            self.positions.insert(tp, offset);
        }
        self.commit(m).await
    }

    /// Commit the handled offsets. A coordinator that moved is looked up
    /// again on the next commit; the offsets stay pending until then.
    async fn commit(&mut self, m: &KafkaMapping) -> Result<(), String> {
        if self.dirty.is_empty() {
            return Ok(());
        }
        let offsets = commit_targets(&self.dirty, &self.floor);
        if offsets.is_empty() {
            self.dirty.clear();
            return Ok(());
        }
        let coordinator = self.coordinator(&m.group_id).await?;
        match self
            .client
            .offset_commit(&coordinator, &m.group_id, &offsets)
            .await
        {
            Ok(()) => {
                self.dirty.clear();
                for (t, p, o) in offsets {
                    self.floor.insert((t, p), o);
                }
                Ok(())
            }
            Err(e)
                if matches!(
                    e.broker_code(),
                    Some(client::ERROR_NOT_COORDINATOR)
                        | Some(client::ERROR_COORDINATOR_NOT_AVAILABLE)
                        | Some(client::ERROR_COORDINATOR_LOAD_IN_PROGRESS)
                ) =>
            {
                self.coordinator = None;
                Ok(())
            }
            Err(e) => {
                self.coordinator = None;
                Err(problem(&e))
            }
        }
    }

    /// One Fetch per partition leader, appending what arrives.
    async fn fetch(&mut self, m: &KafkaMapping, max_wait_ms: i32) -> Result<(), String> {
        let targets: Vec<(i32, FetchTarget)> = self
            .partitions
            .iter()
            .filter(|(tp, _)| !self.blocked.contains_key(tp))
            .filter_map(|(tp, leader)| {
                self.positions
                    .get(tp)
                    .map(|o| (*leader, (tp.0.clone(), tp.1, *o)))
            })
            .collect();
        let targets = rotated(&targets, self.rotation);
        self.rotation = self.rotation.wrapping_add(1);
        let mut by_leader: Vec<(i32, Vec<FetchTarget>)> = Vec::new();
        for (leader, entry) in targets {
            match by_leader.iter_mut().find(|(l, _)| *l == leader) {
                Some((_, parts)) => parts.push(entry),
                None => by_leader.push((leader, vec![entry])),
            }
        }
        if by_leader.is_empty() {
            tokio::time::sleep(Duration::from_millis(max_wait_ms.max(0) as u64)).await;
        }
        let mut leader_trouble = false;
        for (leader, parts) in by_leader {
            let fetched = self
                .client
                .fetch(
                    leader,
                    &parts,
                    max_wait_ms,
                    FETCH_MAX_BYTES,
                    FETCH_PARTITION_MAX_BYTES,
                )
                .await
                .map_err(|e| problem(&e))?;
            let mut stalled = Vec::new();
            for fp in fetched {
                if fp.truncated {
                    stalled.push(fp);
                } else {
                    leader_trouble |= self.apply_fetched(m, leader, fp).await?;
                }
            }
            // A batch larger than the partition's share of the fetch comes
            // back whole only when its partition leads the fetch: read it on
            // its own.
            for fp in stalled {
                let Some(offset) = self.positions.get(&(fp.topic.clone(), fp.partition)) else {
                    continue;
                };
                let alone = [(fp.topic.clone(), fp.partition, *offset)];
                let refetched = self
                    .client
                    .fetch(leader, &alone, 0, FETCH_MAX_BYTES, FETCH_MAX_BYTES)
                    .await
                    .map_err(|e| problem(&e))?;
                for fp in refetched {
                    leader_trouble |= self.apply_fetched(m, leader, fp).await?;
                }
            }
        }
        if leader_trouble {
            tokio::time::sleep(LEADER_BACKOFF).await;
        }
        Ok(())
    }

    /// Take in one partition of a Fetch response; `true` when its leadership
    /// is moving.
    async fn apply_fetched(
        &mut self,
        m: &KafkaMapping,
        leader: i32,
        fp: client::FetchedPartition,
    ) -> Result<bool, String> {
        let tp = (fp.topic.clone(), fp.partition);
        if let Some(reason) = fp.decode_error {
            // The partition is skipped; the others go on.
            let reported = format!(
                "PROBLEM: Partition {}-{} could not be read: {reason}",
                tp.0, tp.1
            );
            tracing::warn!(mapping = %m.uuid, %reported, "Kafka->Lambda: skipping a partition");
            self.blocked.insert(tp, reported.clone());
            self.pending_problem = Some(reported);
            return Ok(false);
        }
        match fp.error_code {
            0 => {
                let last = fp.records.last().map(|r| r.offset + 1);
                if let Some(next) = fp.next_offset.max(last) {
                    let pos = self.positions.entry(tp.clone()).or_default();
                    *pos = (*pos).max(next);
                }
                if !fp.records.is_empty() && self.buffered_since.is_none() {
                    self.buffered_since = Some(Instant::now());
                }
                self.buffer
                    .extend(fp.records.into_iter().map(|record| Buffered {
                        topic: fp.topic.clone(),
                        partition: fp.partition,
                        record,
                    }));
                Ok(false)
            }
            // Records gone (retention, deleted): restart where a partition
            // with no committed offset would.
            client::ERROR_OFFSET_OUT_OF_RANGE => {
                let position = match m.starting_position {
                    StartingPosition::Latest => StartingPosition::Latest,
                    _ => StartingPosition::TrimHorizon,
                };
                match self.start_offset(position, &tp, leader).await {
                    Ok(offset) => {
                        self.positions.insert(tp, offset);
                        Ok(false)
                    }
                    Err(e) if is_leader_error(e.broker_code()) => {
                        self.last_metadata = None;
                        Ok(true)
                    }
                    Err(e) => Err(problem(&e)),
                }
            }
            code if is_leader_error(Some(code)) => {
                self.last_metadata = None;
                Ok(true)
            }
            code => Err(problem(&ClientError::Broker {
                code,
                context: format!(": fetching {}-{}", tp.0, tp.1),
            })),
        }
    }

    /// Mark the first `n` buffered records handled.
    fn consume(&mut self, n: usize) {
        let handled: Vec<Buffered> = self.buffer.drain(..n.min(self.buffer.len())).collect();
        for (tp, offset) in consumed_offsets(&handled) {
            let entry = self.dirty.entry(tp).or_insert(offset);
            *entry = (*entry).max(offset);
        }
        if self.buffer.is_empty() {
            self.buffered_since = None;
        }
        self.retrying = false;
    }

    fn should_flush(&self, m: &KafkaMapping) -> bool {
        !self.buffer.is_empty()
            && (self.retrying
                || self.buffer.len() >= m.batch_size
                || self
                    .buffered_since
                    .is_some_and(|t| t.elapsed() >= m.batching_window))
    }
}

/// Kafka -> Lambda event source mapping poller.
pub struct KafkaLambdaPoller {
    lambda_state: SharedLambdaState,
    kafka_state: SharedKafkaState,
    secrets: SharedSecretsManagerState,
    kms_hook: Option<Arc<dyn KmsHook>>,
    lambda_delivery: Option<Arc<dyn LambdaDelivery>>,
    failure_destinations: Option<Arc<DeliveryBus>>,
}

impl KafkaLambdaPoller {
    pub fn new(
        lambda_state: SharedLambdaState,
        kafka_state: SharedKafkaState,
        secrets: SharedSecretsManagerState,
    ) -> Self {
        Self {
            lambda_state,
            kafka_state,
            secrets,
            kms_hook: None,
            lambda_delivery: None,
            failure_destinations: None,
        }
    }

    pub fn with_kms_hook(mut self, hook: Arc<dyn KmsHook>) -> Self {
        self.kms_hook = Some(hook);
        self
    }

    pub fn with_lambda_delivery(mut self, delivery: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_delivery = Some(delivery);
        self
    }

    pub fn with_failure_destinations(mut self, bus: Arc<DeliveryBus>) -> Self {
        self.failure_destinations = Some(bus);
        self
    }

    /// Keep one consumer task per enabled Kafka mapping, restarting a task
    /// whose mapping changed and stopping those whose mapping was disabled
    /// or deleted.
    pub async fn run(self: Arc<Self>) {
        let mut workers: HashMap<String, (KafkaMapping, tokio::task::JoinHandle<()>)> =
            HashMap::new();
        let mut interval = tokio::time::interval(SUPERVISE_INTERVAL);
        loop {
            interval.tick().await;
            let wanted = self.mappings();
            workers.retain(|uuid, (cfg, handle)| {
                let keep = wanted.get(uuid) == Some(&*cfg) && !handle.is_finished();
                if !keep {
                    handle.abort();
                }
                keep
            });
            for (uuid, cfg) in wanted {
                if workers.contains_key(&uuid) {
                    continue;
                }
                let handle = tokio::spawn(self.clone().consume(cfg.clone()));
                workers.insert(uuid, (cfg, handle));
            }
        }
    }

    /// Every enabled Kafka mapping, across all accounts and regions.
    fn mappings(&self) -> HashMap<String, KafkaMapping> {
        let accounts = self.lambda_state.read();
        accounts
            .iter_regional()
            .flat_map(|(account, region, lambda)| {
                lambda
                    .event_source_mappings
                    .values()
                    .filter(|m| m.enabled)
                    .filter_map(move |m| KafkaMapping::of(m, account, region))
            })
            .map(|m| (m.uuid.clone(), m))
            .collect()
    }

    /// Record the mapping's `LastProcessingResult`.
    fn report(&self, m: &KafkaMapping, result: &str) {
        let mut accounts = self.lambda_state.write();
        if let Some(esm) = accounts
            .regional_get_mut(&m.account, &m.region)
            .and_then(|s| s.event_source_mappings.get_mut(&m.uuid))
        {
            if esm.last_processing_result.as_deref() != Some(result) {
                esm.last_processing_result = Some(result.to_string());
            }
        }
    }

    /// `(username, password)` of a Secrets Manager secret holding Kafka
    /// credentials (`{"username": ..., "password": ...}`).
    fn credentials(&self, m: &KafkaMapping, secret_arn: &str) -> Result<(String, String), String> {
        let value = fakecloud_secretsmanager::value::read_secret_value(
            &self.secrets,
            self.kms_hook.as_deref(),
            &m.account,
            &m.region,
            secret_arn,
            None,
            None,
            None,
            None,
        )
        .map_err(|e| {
            format!(
                "PROBLEM: SASL authentication failed. Lambda could not read the secret {secret_arn}: {}",
                e.message()
            )
        })?;
        let parsed: Value = value
            .secret_string
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or_default();
        match (parsed["username"].as_str(), parsed["password"].as_str()) {
            (Some(user), Some(pass)) => Ok((user.to_string(), pass.to_string())),
            _ => Err(format!(
                "PROBLEM: SASL authentication failed. The secret {secret_arn} must be a JSON object with \"username\" and \"password\"."
            )),
        }
    }

    /// The cluster's address and the credentials to present.
    fn endpoint(&self, m: &KafkaMapping) -> Result<Endpoint, String> {
        match &m.source {
            KafkaSource::Msk { cluster_arn } => self.msk_endpoint(m, cluster_arn),
            KafkaSource::SelfManaged { bootstrap } => {
                if let Some(kind) = m.access_types().find(|t| {
                    matches!(
                        *t,
                        "CLIENT_CERTIFICATE_TLS_AUTH"
                            | "SERVER_ROOT_CA_CERTIFICATE"
                            | "OAUTHBEARER_AUTH"
                            | "IAM_OAUTHBEARER_AUTH"
                    )
                }) {
                    return Err(format!(
                        "PROBLEM: Connection error. {kind} is not supported by fakecloud's Kafka poller, which reaches self-managed brokers over PLAINTEXT or SASL_PLAINTEXT (BASIC_AUTH, SASL_SCRAM_256_AUTH, SASL_SCRAM_512_AUTH)."
                    ));
                }
                let mechanism = [
                    ("BASIC_AUTH", SaslMechanism::Plain),
                    (
                        "SASL_SCRAM_256_AUTH",
                        SaslMechanism::Scram(ScramAlgorithm::Sha256),
                    ),
                    (
                        "SASL_SCRAM_512_AUTH",
                        SaslMechanism::Scram(ScramAlgorithm::Sha512),
                    ),
                ]
                .into_iter()
                .find_map(|(kind, mech)| m.access(kind).map(|uri| (uri, mech)));
                let sasl = match mechanism {
                    Some((secret, mechanism)) => {
                        let (username, password) = self.credentials(m, secret)?;
                        Some(SaslCredentials {
                            mechanism,
                            username,
                            password,
                        })
                    }
                    None => None,
                };
                Ok(Endpoint {
                    bootstrap: bootstrap.clone(),
                    bootstrap_servers: bootstrap.join(","),
                    sasl,
                })
            }
        }
    }

    fn msk_endpoint(&self, m: &KafkaMapping, cluster_arn: &str) -> Result<Endpoint, String> {
        let account = cluster_arn.split(':').nth(4).unwrap_or_default();
        let (bootstrap, client_auth, scram_secrets) = {
            let kafka = self.kafka_state.read();
            let data = kafka.get(account);
            let cluster = data
                .and_then(|d| d.clusters.get(cluster_arn))
                .ok_or_else(|| {
                    format!("PROBLEM: Connection error. The cluster {cluster_arn} does not exist.")
                })?;
            let bootstrap = data
                .and_then(|d| d.data_plane.get(cluster_arn))
                .and_then(|dp| dp.bootstrap_string())
                .ok_or_else(|| NO_BROKER.to_string())?;
            (
                bootstrap,
                cluster["clientAuthentication"].clone(),
                data.and_then(|d| d.scram_secrets.get(cluster_arn))
                    .cloned()
                    .unwrap_or_default(),
            )
        };
        let enabled = |path: &str| client_auth.pointer(path).and_then(Value::as_bool) == Some(true);
        if let Some(secret) = m.access("SASL_SCRAM_512_AUTH") {
            if !enabled("/sasl/scram/enabled") {
                return Err(
                    "PROBLEM: SASL authentication failed. SASL/SCRAM authentication is not enabled on the cluster."
                        .to_string(),
                );
            }
            self.credentials(m, secret)?;
            if !scram_secrets.iter().any(|s| s == secret) {
                return Err(format!(
                    "PROBLEM: SASL authentication failed. The secret {secret} is not associated with the cluster."
                ));
            }
        }
        if let Some(secret) = m.access("CLIENT_CERTIFICATE_TLS_AUTH") {
            if !enabled("/tls/enabled") {
                return Err("PROBLEM: Server failed to authenticate Lambda. TLS client authentication is not enabled on the cluster.".to_string());
            }
            let value = fakecloud_secretsmanager::value::read_secret_value(
                &self.secrets,
                self.kms_hook.as_deref(),
                &m.account,
                &m.region,
                secret,
                None,
                None,
                None,
                None,
            )
            .map_err(|e| {
                format!(
                    "PROBLEM: Provided certificate or private key is invalid. Lambda could not read the secret {secret}: {}",
                    e.message()
                )
            })?;
            let parsed: Value = value
                .secret_string
                .as_deref()
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            if !(parsed["certificate"].is_string() && parsed["privateKey"].is_string()) {
                return Err(format!(
                    "PROBLEM: Provided certificate or private key is invalid. The secret {secret} must hold \"certificate\" and \"privateKey\"."
                ));
            }
        }
        Ok(Endpoint {
            bootstrap: vec![bootstrap.clone()],
            bootstrap_servers: bootstrap,
            sasl: None,
        })
    }

    /// The mapping's consumer: connect, then poll until the task is
    /// stopped, reconnecting with a backoff after a problem.
    async fn consume(self: Arc<Self>, m: KafkaMapping) {
        let retries = RetryTracker::default();
        let filter = FilterSet::from_strings(m.filter_patterns.iter());
        let mut failures: u32 = 0;
        let mut carried: HashMap<TopicPartition, i64> = HashMap::new();
        loop {
            let opened = match &m.unsupported {
                Some(reason) => Err(reason.clone()),
                None => self.endpoint(&m),
            };
            let mut session = match opened {
                Ok(endpoint) => Session::new(
                    endpoint,
                    format!("fakecloud-lambda-{}", m.uuid),
                    std::mem::take(&mut carried),
                ),
                Err(reason) => {
                    self.report(&m, &reason);
                    failures += 1;
                    tokio::time::sleep(reconnect_backoff(failures)).await;
                    continue;
                }
            };
            let outcome: Result<std::convert::Infallible, String> = async {
                session.refresh_partitions(&m).await?;
                // Connected: the next problem backs off from the start.
                failures = 0;
                loop {
                    self.cycle(&m, &mut session, &retries, &filter).await?;
                }
            }
            .await;
            let reason = match outcome {
                Ok(never) => match never {},
                Err(reason) => reason,
            };
            tracing::warn!(mapping = %m.uuid, %reason, "Kafka->Lambda: consumer problem");
            self.report(&m, &reason);
            carried = std::mem::take(&mut session.dirty);
            failures += 1;
            tokio::time::sleep(reconnect_backoff(failures)).await;
        }
    }

    /// Fetch what the buffer lacks, then invoke the function once a batch is
    /// due.
    async fn cycle(
        &self,
        m: &KafkaMapping,
        s: &mut Session,
        retries: &RetryTracker,
        filter: &FilterSet,
    ) -> Result<(), String> {
        if s.metadata_stale() {
            s.refresh_partitions(m).await?;
        }
        s.commit(m).await?;
        if let Some(reported) = s.pending_problem.take() {
            self.report(m, &reported);
        }
        if s.buffer.len() < m.batch_size && !s.retrying {
            // Wait on the broker no longer than the batching window has left.
            let wait = match s.buffered_since {
                Some(since) => m
                    .batching_window
                    .saturating_sub(since.elapsed())
                    .as_millis()
                    .min(FETCH_MAX_WAIT_MS as u128) as i32,
                None => FETCH_MAX_WAIT_MS,
            };
            s.fetch(m, wait).await?;
        }
        if s.should_flush(m) {
            self.flush(m, s, retries, filter).await?;
        }
        Ok(())
    }

    async fn flush(
        &self,
        m: &KafkaMapping,
        s: &mut Session,
        retries: &RetryTracker,
        filter: &FilterSet,
    ) -> Result<(), String> {
        let shaped: Vec<(Buffered, Value)> = s
            .buffer
            .iter()
            .take(m.batch_size)
            .map(|b| (b.clone(), event_record(b)))
            .collect();
        let sizes: Vec<usize> = shaped.iter().map(|(_, r)| r.to_string().len()).collect();
        let len = batch_len_within(&sizes, m.batch_size, MAX_PAYLOAD_BYTES);
        let shaped = &shaped[..len];
        let pending: Vec<Pending> = shaped
            .iter()
            .map(|(b, _)| (b.id(), b.timestamp()))
            .collect();
        let key = format!("{}:kafka", m.uuid);
        let n = match retries.plan(&key, &m.policy, &pending, Utc::now()) {
            Step::Wait => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                return Ok(());
            }
            Step::DiscardTooOld { n, invoke_count } => {
                let batch: Vec<(&Buffered, Value)> =
                    shaped[..n].iter().map(|(b, r)| (b, r.clone())).collect();
                let payload = kafka_event(m, &s.bootstrap_servers, &batch).to_string();
                self.discard(
                    m,
                    s,
                    &shaped[..n],
                    invoke_count,
                    DiscardCause::RecordAge,
                    &payload,
                );
                retries.discarded(&key, &pending, n);
                s.consume(n);
                return s.commit(m).await;
            }
            Step::Invoke(n) => n,
        };
        let batch = &shaped[..n];
        // Records the filter drops count as handled.
        let matched: Vec<(&Buffered, Value)> = batch
            .iter()
            .filter(|(b, r)| filter.is_empty() || filter.matches(&filter_view(b, r)))
            .map(|(b, r)| (b, r.clone()))
            .collect();
        if matched.is_empty() {
            retries.resolve(&key, &m.policy, &pending, n, Attempt::Succeeded);
            s.consume(n);
            return s.commit(m).await;
        }
        let payload = kafka_event(m, &s.bootstrap_servers, &matched).to_string();
        let Some(delivery) = &self.lambda_delivery else {
            self.record_invocation(m, &payload);
            retries.resolve(&key, &m.policy, &pending, n, Attempt::Succeeded);
            s.consume(n);
            self.report(m, "OK");
            return s.commit(m).await;
        };
        let result = delivery.invoke_lambda(&m.function_arn, &payload).await;
        let ids: Vec<String> = batch.iter().map(|(b, _)| b.id()).collect();
        let attempt = Attempt::of(&result, &m.policy, &ids);
        match &attempt {
            Attempt::Succeeded | Attempt::Partial(_) => {
                tracing::info!(
                    function_arn = %m.function_arn,
                    record_count = matched.len(),
                    "Kafka->Lambda invocation succeeded"
                );
                self.report(m, "OK");
            }
            Attempt::Failed { .. } | Attempt::Unrunnable => {
                tracing::warn!(
                    function_arn = %m.function_arn,
                    error = %match &result {
                        Ok(output) => String::from_utf8_lossy(&output.payload).into_owned(),
                        Err(e) => e.clone(),
                    },
                    "Kafka->Lambda: batch failed"
                );
                self.report(m, FUNCTION_CALL_FAILED);
            }
        }
        match retries.resolve(&key, &m.policy, &pending, n, attempt) {
            Resolution::Advance(k) => {
                s.consume(k);
                s.commit(m).await
            }
            Resolution::Retry => {
                s.retrying = true;
                Ok(())
            }
            Resolution::Discard {
                n,
                invoke_count,
                function_error,
            } => {
                self.discard(
                    m,
                    s,
                    &shaped[..n],
                    invoke_count,
                    DiscardCause::RetriesExhausted { function_error },
                    &payload,
                );
                s.consume(n);
                s.commit(m).await
            }
        }
    }

    /// Describe a discarded batch to the mapping's OnFailure destination.
    fn discard(
        &self,
        m: &KafkaMapping,
        s: &Session,
        batch: &[(Buffered, Value)],
        invoke_count: u32,
        cause: DiscardCause,
        payload: &str,
    ) {
        tracing::warn!(
            function_arn = %m.function_arn,
            records = batch.len(),
            "Kafka->Lambda: discarding a failed batch"
        );
        let (Some(destination), Some(bus), Some((first, _))) = (
            m.policy.on_failure.as_deref(),
            self.failure_destinations.as_ref(),
            batch.first(),
        ) else {
            return;
        };
        let records: Vec<Buffered> = batch.iter().map(|(b, _)| b.clone()).collect();
        deliver_failure_record(
            bus,
            destination,
            &m.function_arn,
            &m.uuid,
            &first.partition_key(),
            |with_payload| {
                failure_record(
                    m,
                    &s.bootstrap_servers,
                    &records,
                    invoke_count,
                    &cause,
                    payload,
                    with_payload,
                )
            },
        );
    }

    /// Without a container runtime the invocation is recorded, as the other
    /// pollers record theirs.
    fn record_invocation(&self, m: &KafkaMapping, payload: &str) {
        let mut accounts = self.lambda_state.write();
        let default_account = accounts.default_account_id().to_string();
        let default_region = accounts.region().to_string();
        let (account, region, _) =
            fakecloud_lambda::function_location(&m.function_arn, &default_account, &default_region);
        let (account, region) = (account.to_string(), region.to_string());
        accounts
            .regional_mut(&account, &region)
            .invocations
            .push(LambdaInvocation {
                function_arn: m.function_arn.clone(),
                payload: payload.to_string(),
                timestamp: Utc::now(),
                source: m.event_source().to_string(),
            });
    }
}

/// The wait before reconnecting after the `nth` problem in a row.
fn reconnect_backoff(nth: u32) -> Duration {
    Duration::from_millis(500)
        .saturating_mul(1u32 << nth.saturating_sub(1).min(8))
        .min(RECONNECT_BACKOFF_MAX)
}

#[cfg(test)]
mod tests;
