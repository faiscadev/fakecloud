use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<Vec<u8>>,
}

/// One FIFO dedup-cache entry: the original (non-duplicate) send result,
/// kept until `expiry`; a duplicate within the window replays it verbatim
/// (bug-audit 2026-05-28, 1.13).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DedupEntry {
    pub message_id: String,
    pub sequence_number: Option<String>,
    pub expiry: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsMessage {
    pub message_id: String,
    pub receipt_handle: Option<String>,
    pub body: String,
    pub md5_of_body: String,
    pub sent_timestamp: i64,
    pub attributes: BTreeMap<String, String>,
    pub message_attributes: BTreeMap<String, MessageAttribute>,
    /// When this message becomes visible again (after ReceiveMessage)
    pub visible_at: Option<DateTime<Utc>>,
    pub receive_count: u32,
    /// Epoch millis of the FIRST receipt; AWS pins
    /// ApproximateFirstReceiveTimestamp to this and keeps it constant across
    /// redeliveries. `None` until first received.
    #[serde(default)]
    pub first_received_at: Option<i64>,
    /// For FIFO: message group ID
    pub message_group_id: Option<String>,
    /// For FIFO: dedup ID
    pub message_dedup_id: Option<String>,
    /// When the message was created (for retention period expiry)
    pub created_at: DateTime<Utc>,
    /// FIFO sequence number
    pub sequence_number: Option<String>,
    /// The ARN of the queue whose SSE encryption context (`aws:sqs:arn`)
    /// the stored body was sealed with. A message keeps its body when it
    /// moves to a dead-letter queue or is redriven, so it is decrypted with
    /// this context, not the current queue's. `None` for a plaintext body or
    /// one stored before this was recorded (sealed with no context).
    #[serde(default)]
    pub sealed_for_queue: Option<String>,
    /// Whether `sealed_for_queue` records the body's seal: true for every
    /// message stored now, false (the default) for one restored from a
    /// snapshot taken before the seal was recorded, whose body was sealed by
    /// the queue holding it if that queue encrypts.
    #[serde(default)]
    pub seal_recorded: bool,
}

impl SqsMessage {
    /// The queue ARN whose encryption context opens the stored body, for a
    /// message now held by `holder_arn`.
    pub fn sealing_queue<'a>(&'a self, holder_arn: &'a str) -> &'a str {
        self.sealed_for_queue.as_deref().unwrap_or(holder_arn)
    }

    /// Whether the stored body is a ciphertext to open on the way out. A
    /// body is sealed by the queue it was sent to and keeps that seal when
    /// it moves (dead-letter queue, redrive), so this follows the message,
    /// not the queue holding it now: an encrypted queue's message in an
    /// unencrypted dead-letter queue is still decrypted, and a plaintext
    /// message in an encrypted one is not (a body that happens to be a KMS
    /// ciphertext the sender made is delivered as sent). A message stored
    /// before the seal was recorded falls back to its holder's encryption
    /// (`holder_encrypted`).
    pub fn body_is_sealed(&self, holder_encrypted: bool) -> bool {
        self.sealed_for_queue.is_some() || (!self.seal_recorded && holder_encrypted)
    }

    /// Record the seal of a message restored from a snapshot taken before
    /// messages recorded their own (`seal_recorded` false): its body was
    /// sealed by the queue holding it (`holder_arn`) if that queue encrypts
    /// (`holder_encrypted`), so that queue becomes its sealing queue. Done
    /// on restore, before the message can move: once it is in an unencrypted
    /// dead-letter queue its holder no longer says how it was sealed.
    pub fn record_legacy_seal(&mut self, holder_arn: &str, holder_encrypted: bool) {
        if self.seal_recorded {
            return;
        }
        if holder_encrypted && self.sealed_for_queue.is_none() {
            self.sealed_for_queue = Some(holder_arn.to_string());
        }
        self.seal_recorded = true;
    }
}

/// Whether a queue with `attributes` encrypts at rest (SSE-KMS, or SSE-SQS,
/// which messages stored before each recorded its own seal were sealed with
/// under `alias/aws/sqs`).
pub fn queue_encrypts_at_rest(attributes: &BTreeMap<String, String>) -> bool {
    attributes
        .get("KmsMasterKeyId")
        .is_some_and(|k| !k.is_empty())
        || attributes.get("SqsManagedSseEnabled").map(String::as_str) == Some("true")
}

impl SqsState {
    /// Record the seal of every message restored from an older snapshot
    /// (see [`SqsMessage::record_legacy_seal`]).
    fn record_legacy_seals(&mut self) {
        for queue in self.queues.values_mut() {
            let encrypted = queue_encrypts_at_rest(&queue.attributes);
            let arn = queue.arn.clone();
            for msg in queue.messages.iter_mut().chain(queue.inflight.iter_mut()) {
                msg.record_legacy_seal(&arn, encrypted);
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedrivePolicy {
    pub dead_letter_target_arn: String,
    pub max_receive_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsQueue {
    pub queue_name: String,
    pub queue_url: String,
    pub arn: String,
    pub created_at: DateTime<Utc>,
    pub messages: VecDeque<SqsMessage>,
    pub inflight: Vec<SqsMessage>,
    pub attributes: BTreeMap<String, String>,
    pub is_fifo: bool,
    /// For FIFO dedup: dedup_id -> the original send's result + expiry,
    /// so a duplicate within the window replays the ORIGINAL MessageId +
    /// SequenceNumber without re-enqueuing or advancing the counter
    /// (bug-audit 2026-05-28, 1.13).
    pub dedup_cache: BTreeMap<String, DedupEntry>,
    /// DLQ redrive policy
    pub redrive_policy: Option<RedrivePolicy>,
    /// Queue tags (key -> value)
    pub tags: BTreeMap<String, String>,
    /// FIFO: next sequence number counter
    pub next_sequence_number: u64,
    /// Permission labels stored on the queue
    pub permission_labels: Vec<String>,
    /// Tracks message_id -> list of all receipt handles ever issued for that message
    pub receipt_handle_map: BTreeMap<String, Vec<String>>,
    /// FIFO: ReceiveRequestAttemptId -> the message_ids returned by the
    /// original receive + an expiry. A retried receive with the same id
    /// within the visibility window replays the exact same batch instead
    /// of returning the next messages (AWS FIFO de-dup of receive
    /// attempts).
    #[serde(default)]
    pub receive_attempt_cache: BTreeMap<String, ReceiveAttemptEntry>,
}

impl SqsQueue {
    /// The queue's `VisibilityTimeout` in seconds (default 30), clamped to
    /// SQS's 0-43200 range. The API validates the attribute, but a queue
    /// provisioned from a template or an older snapshot may carry any string,
    /// and an out-of-range value must not overflow visibility arithmetic.
    pub fn visibility_timeout_secs(&self) -> i64 {
        self.attributes
            .get("VisibilityTimeout")
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(30)
            .clamp(0, 43_200)
    }

    /// The queue's `DelaySeconds`, clamped to SQS's 0-900 range for the same
    /// reason as [`Self::visibility_timeout_secs`].
    pub fn delay_secs(&self) -> Option<i64> {
        self.attributes
            .get("DelaySeconds")
            .and_then(|s| s.parse::<i64>().ok())
            .map(|d| d.clamp(0, 900))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiveAttemptEntry {
    pub message_ids: Vec<String>,
    pub expiry: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MessageMoveTaskStatus {
    Running,
    Completed,
    Cancelling,
    Cancelled,
    Failed,
}

impl MessageMoveTaskStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageMoveTaskStatus::Running => "RUNNING",
            MessageMoveTaskStatus::Completed => "COMPLETED",
            MessageMoveTaskStatus::Cancelling => "CANCELLING",
            MessageMoveTaskStatus::Cancelled => "CANCELLED",
            MessageMoveTaskStatus::Failed => "FAILED",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageMoveTask {
    pub task_handle: String,
    pub source_arn: String,
    pub destination_arn: Option<String>,
    pub max_messages_per_second: Option<i32>,
    pub status: MessageMoveTaskStatus,
    pub messages_moved: u64,
    pub messages_to_move: u64,
    pub started_timestamp: i64,
    pub failure_reason: Option<String>,
    /// Process id of the worker driving this task. A Running task whose pid is
    /// not the current process was orphaned by a restart (the background mover
    /// is never re-spawned on load), so it is treated as stale rather than
    /// blocking new move tasks for the queue forever (bug-audit 2026-05-28,
    /// 4.6). Defaults to 0 for tasks persisted before this field existed.
    #[serde(default)]
    pub driver_pid: u32,
    /// Set to `true` by `CancelMessageMoveTask` to request that the
    /// background mover stop after its current iteration. Not persisted
    /// — restored snapshots resume with a fresh flag in its default
    /// state (no in-flight cancellation).
    #[serde(skip, default = "default_cancel_flag")]
    pub cancel_flag: Arc<AtomicBool>,
}

fn default_cancel_flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsState {
    pub account_id: String,
    pub region: String,
    pub endpoint: String,
    pub queues: BTreeMap<String, SqsQueue>, // queue_url -> queue
    pub name_to_url: BTreeMap<String, String>, // queue_name -> queue_url
    #[serde(default)]
    pub message_move_tasks: Vec<MessageMoveTask>,
}

impl SqsState {
    pub fn new(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            endpoint: endpoint.to_string(),
            queues: BTreeMap::new(),
            name_to_url: BTreeMap::new(),
            message_move_tasks: Vec::new(),
        }
    }
}

impl SqsState {
    pub fn reset(&mut self) {
        self.queues.clear();
        self.name_to_url.clear();
        self.message_move_tasks.clear();
    }
}

/// SQS state partitioned by account and region: queues are regional, so the
/// same queue name can exist independently in two regions of one account.
/// A queue's URL (`<endpoint>/<account>/<name>`) carries no region, the way a
/// fakecloud endpoint serves every region; the request region picks which
/// region's queue it addresses, and a queue ARN always names its own region.
pub type SharedSqsState = Arc<RwLock<fakecloud_core::multi_account::MultiRegionState<SqsState>>>;

/// On-disk snapshot envelope for SQS. Mirrors the DynamoDB pattern: a
/// versioned wrapper around the full [`SqsState`] so format changes fail
/// loudly on upgrade instead of silently corrupting state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiRegionState<SqsState>>,
    /// Only set when a v1 (single-account) snapshot is migrated: that one
    /// account's state split by region, for the caller to merge into its own
    /// container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<fakecloud_core::multi_account::RegionalState<SqsState>>,
}

/// v3: state partitioned by (account, region). v2 kept one state per
/// account, every queue in one map keyed by URL; v1 a single account's.
pub const SQS_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

impl fakecloud_core::multi_account::AccountState for SqsState {
    fn new_for_account(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self::new(account_id, region, endpoint)
    }
}

impl fakecloud_core::multi_account::SplitByRegion for SqsState {
    /// Every queue goes to the region its ARN names; a message move task
    /// follows its source queue.
    fn split_by_region(self, into: &mut fakecloud_core::multi_account::RegionalState<Self>) {
        let mut queue_regions: BTreeMap<String, String> = BTreeMap::new();
        for (url, queue) in self.queues {
            let region = fakecloud_aws::arn::region_of(&queue.arn).map(str::to_string);
            let target = into.region_or_default_mut(region.as_deref());
            queue_regions.insert(queue.arn.clone(), target.region.clone());
            target
                .name_to_url
                .insert(queue.queue_name.clone(), url.clone());
            target.queues.insert(url, queue);
        }
        for task in self.message_move_tasks {
            let region = queue_regions
                .get(&task.source_arn)
                .cloned()
                .or_else(|| fakecloud_aws::arn::region_of(&task.source_arn).map(str::to_string));
            into.region_or_default_mut(region.as_deref())
                .message_move_tasks
                .push(task);
        }
    }
}

/// The shape v1 and v2 snapshots stored: one state per account.
#[derive(Debug, Deserialize)]
struct LegacySqsSnapshot {
    #[serde(default)]
    accounts: Option<fakecloud_core::multi_account::MultiAccountState<SqsState>>,
    #[serde(default)]
    state: Option<SqsState>,
}

#[derive(Deserialize)]
struct SnapshotVersion {
    schema_version: u32,
}

/// Parse a persisted SQS snapshot, migrating older schemas to the current one
/// by moving every queue into the region its ARN names. A snapshot newer than
/// this build comes back with its on-disk `schema_version` and no state, for
/// the caller to refuse.
pub fn parse_sqs_snapshot(bytes: &[u8]) -> Result<SqsSnapshot, serde_json::Error> {
    let SnapshotVersion { schema_version } = serde_json::from_slice(bytes)?;
    if schema_version > SQS_SNAPSHOT_SCHEMA_VERSION {
        return Ok(SqsSnapshot {
            schema_version,
            accounts: None,
            state: None,
        });
    }
    let mut snapshot = if schema_version == SQS_SNAPSHOT_SCHEMA_VERSION {
        serde_json::from_slice(bytes)?
    } else {
        migrate_legacy_snapshot(serde_json::from_slice(bytes)?)
    };
    if let Some(accounts) = snapshot.accounts.as_mut() {
        for (_, _, state) in accounts.iter_regional_mut() {
            state.record_legacy_seals();
        }
    }
    if let Some(state) = snapshot.state.as_mut() {
        for (_, state) in state.regions_mut() {
            state.record_legacy_seals();
        }
    }
    Ok(snapshot)
}

/// A v1 / v2 snapshot as the current schema.
fn migrate_legacy_snapshot(legacy: LegacySqsSnapshot) -> SqsSnapshot {
    SqsSnapshot {
        schema_version: SQS_SNAPSHOT_SCHEMA_VERSION,
        accounts: legacy.accounts.map(|a| a.into_regional()),
        state: legacy.state.map(|s| {
            let (account, region, endpoint) =
                (s.account_id.clone(), s.region.clone(), s.endpoint.clone());
            fakecloud_core::multi_account::RegionalState::from_legacy(
                &account, &region, &endpoint, s,
            )
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_has_empty_collections() {
        let state = SqsState::new("123456789012", "us-east-1", "http://localhost:4566");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert_eq!(state.endpoint, "http://localhost:4566");
        assert!(state.queues.is_empty());
        assert!(state.name_to_url.is_empty());
    }

    #[test]
    fn reset_clears_collections() {
        let mut state = SqsState::new("123456789012", "us-east-1", "http://localhost:4566");
        state
            .name_to_url
            .insert("q1".to_string(), "url".to_string());
        assert!(!state.name_to_url.is_empty());
        state.reset();
        assert!(state.name_to_url.is_empty());
    }

    #[test]
    fn account_state_trait_impl() {
        use fakecloud_core::multi_account::AccountState;
        let state = SqsState::new_for_account("111122223333", "eu-west-1", "http://x");
        assert_eq!(state.account_id, "111122223333");
        assert_eq!(state.region, "eu-west-1");
    }

    fn queue(name: &str, region: &str) -> SqsQueue {
        SqsQueue {
            queue_name: name.to_string(),
            queue_url: format!("http://localhost:4566/123456789012/{name}"),
            arn: format!("arn:aws:sqs:{region}:123456789012:{name}"),
            created_at: Utc::now(),
            messages: VecDeque::new(),
            inflight: Vec::new(),
            attributes: BTreeMap::new(),
            is_fifo: false,
            dedup_cache: BTreeMap::new(),
            redrive_policy: None,
            tags: BTreeMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: BTreeMap::new(),
            receive_attempt_cache: BTreeMap::new(),
        }
    }

    #[test]
    fn v2_snapshot_migrates_queues_into_their_arn_region() {
        use fakecloud_core::multi_account::MultiAccountState;
        let mut legacy: MultiAccountState<SqsState> =
            MultiAccountState::new("123456789012", "us-east-1", "http://localhost:4566");
        let st = legacy.default_mut();
        for (name, region) in [("east", "us-east-1"), ("west", "eu-west-1")] {
            let q = queue(name, region);
            st.name_to_url.insert(name.into(), q.queue_url.clone());
            st.queues.insert(q.queue_url.clone(), q);
        }
        st.message_move_tasks.push(MessageMoveTask {
            task_handle: "h".into(),
            source_arn: "arn:aws:sqs:eu-west-1:123456789012:west".into(),
            destination_arn: None,
            max_messages_per_second: None,
            status: MessageMoveTaskStatus::Completed,
            messages_moved: 0,
            messages_to_move: 0,
            started_timestamp: 0,
            failure_reason: None,
            driver_pid: 0,
            cancel_flag: default_cancel_flag(),
        });
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": 2,
            "accounts": legacy,
        }))
        .unwrap();
        let snap = parse_sqs_snapshot(&bytes).unwrap();
        assert_eq!(snap.schema_version, SQS_SNAPSHOT_SCHEMA_VERSION);
        let accounts = snap.accounts.unwrap();
        let east = accounts.regional("123456789012", "us-east-1").unwrap();
        let west = accounts.regional("123456789012", "eu-west-1").unwrap();
        assert_eq!(east.region, "us-east-1");
        assert_eq!(west.region, "eu-west-1");
        assert!(east.name_to_url.contains_key("east") && !east.name_to_url.contains_key("west"));
        assert!(west.name_to_url.contains_key("west") && !west.name_to_url.contains_key("east"));
        assert_eq!(west.message_move_tasks.len(), 1);
        assert!(east.message_move_tasks.is_empty());

        // The migrated snapshot round-trips as the current schema.
        let current = serde_json::to_vec(&SqsSnapshot {
            schema_version: SQS_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
            state: None,
        })
        .unwrap();
        let again = parse_sqs_snapshot(&current).unwrap().accounts.unwrap();
        assert!(again.regional("123456789012", "eu-west-1").is_some());
    }

    #[test]
    fn v1_single_account_snapshot_migrates_by_region() {
        let mut st = SqsState::new("123456789012", "us-east-1", "http://localhost:4566");
        let q = queue("west", "eu-west-1");
        st.name_to_url.insert("west".into(), q.queue_url.clone());
        st.queues.insert(q.queue_url.clone(), q);
        let bytes =
            serde_json::to_vec(&serde_json::json!({"schema_version": 1, "state": st})).unwrap();
        let snap = parse_sqs_snapshot(&bytes).unwrap();
        let regional = snap.state.unwrap();
        assert!(regional
            .region("eu-west-1")
            .unwrap()
            .name_to_url
            .contains_key("west"));
        assert!(regional.region("us-east-1").is_none());
    }

    #[test]
    fn newer_snapshot_is_reported_not_parsed() {
        let snap = parse_sqs_snapshot(br#"{"schema_version": 99, "accounts": 5}"#).unwrap();
        assert_eq!(snap.schema_version, 99);
        assert!(snap.accounts.is_none());
    }
}

#[cfg(test)]
mod sealing_tests {
    use super::*;

    fn message(sealed_for_queue: Option<&str>) -> SqsMessage {
        SqsMessage {
            message_id: "m".to_string(),
            receipt_handle: None,
            md5_of_body: String::new(),
            body: String::new(),
            sent_timestamp: 0,
            attributes: Default::default(),
            message_attributes: Default::default(),
            visible_at: None,
            receive_count: 0,
            first_received_at: None,
            message_group_id: None,
            message_dedup_id: None,
            created_at: Utc::now(),
            sequence_number: None,
            sealed_for_queue: sealed_for_queue.map(str::to_string),
            seal_recorded: true,
        }
    }

    #[test]
    fn a_body_keeps_its_seal_wherever_it_moves() {
        let src = "arn:aws:sqs:us-east-1:123456789012:src";
        // Sealed by an encrypted queue, now in a plain dead-letter queue.
        assert!(message(Some(src)).body_is_sealed(false));
        assert_eq!(message(Some(src)).sealing_queue("dlq"), src);
        // Sent in plaintext: not sealed, even in an encrypted queue (its
        // body may be a ciphertext the sender made).
        assert!(!message(None).body_is_sealed(false));
        assert!(!message(None).body_is_sealed(true));
        // Restored from before the seal was recorded: its holder sealed it.
        let mut legacy = message(None);
        legacy.seal_recorded = false;
        assert!(legacy.body_is_sealed(true));
        assert!(!legacy.body_is_sealed(false));
    }

    fn queue(name: &str, attributes: &[(&str, &str)]) -> SqsQueue {
        SqsQueue {
            queue_name: name.to_string(),
            queue_url: format!("http://localhost:4566/123456789012/{name}"),
            arn: format!("arn:aws:sqs:us-east-1:123456789012:{name}"),
            created_at: Utc::now(),
            messages: VecDeque::new(),
            inflight: Vec::new(),
            attributes: attributes
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            is_fifo: false,
            dedup_cache: BTreeMap::new(),
            redrive_policy: None,
            tags: BTreeMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: BTreeMap::new(),
            receive_attempt_cache: BTreeMap::new(),
        }
    }

    /// A message restored from a snapshot taken before messages recorded
    /// their seal gets its holder as its sealing queue when that queue
    /// encrypts, so it stays sealed (and is decrypted) once it moves to an
    /// unencrypted dead-letter queue instead of being delivered as its
    /// ciphertext.
    #[test]
    fn restore_records_the_seal_of_messages_from_older_snapshots() {
        use fakecloud_core::multi_account::MultiAccountState;
        let mut accounts: fakecloud_core::multi_account::MultiRegionState<SqsState> =
            MultiAccountState::new("123456789012", "us-east-1", "http://localhost:4566");
        let st = accounts.regional_mut("123456789012", "us-east-1");
        for (name, attrs) in [
            ("kms", &[("KmsMasterKeyId", "alias/app")][..]),
            ("sse-sqs", &[("SqsManagedSseEnabled", "true")][..]),
            ("plain", &[][..]),
        ] {
            let mut q = queue(name, attrs);
            let mut pending = message(None);
            pending.message_id = format!("{name}-pending");
            q.messages.push_back(pending);
            let mut inflight = message(None);
            inflight.message_id = format!("{name}-inflight");
            q.inflight.push(inflight);
            st.queues.insert(q.queue_url.clone(), q);
        }
        // Serialize as an older build did: no `seal_recorded` on messages.
        let mut json = serde_json::to_value(SqsSnapshot {
            schema_version: SQS_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
            state: None,
        })
        .unwrap();
        fn strip(v: &mut serde_json::Value) {
            match v {
                serde_json::Value::Object(map) => {
                    map.remove("seal_recorded");
                    map.values_mut().for_each(strip);
                }
                serde_json::Value::Array(items) => items.iter_mut().for_each(strip),
                _ => {}
            }
        }
        strip(&mut json);

        let snap = parse_sqs_snapshot(&serde_json::to_vec(&json).unwrap()).unwrap();
        let accounts = snap.accounts.unwrap();
        let st = accounts.regional("123456789012", "us-east-1").unwrap();
        for q in st.queues.values() {
            let expected = (q.queue_name != "plain").then(|| q.arn.clone());
            for m in q.messages.iter().chain(q.inflight.iter()) {
                assert!(m.seal_recorded, "{}", m.message_id);
                assert_eq!(m.sealed_for_queue, expected, "{}", m.message_id);
                // Its seal now follows it into an unencrypted queue.
                assert_eq!(m.body_is_sealed(false), expected.is_some());
            }
        }
    }
}
