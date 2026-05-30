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
    /// For FIFO: message group ID
    pub message_group_id: Option<String>,
    /// For FIFO: dedup ID
    pub message_dedup_id: Option<String>,
    /// When the message was created (for retention period expiry)
    pub created_at: DateTime<Utc>,
    /// FIFO sequence number
    pub sequence_number: Option<String>,
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

pub type SharedSqsState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<SqsState>>>;

/// On-disk snapshot envelope for SQS. Mirrors the DynamoDB pattern: a
/// versioned wrapper around the full [`SqsState`] so format changes fail
/// loudly on upgrade instead of silently corrupting state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<SqsState>>,
    #[serde(default)]
    pub state: Option<SqsState>,
}

pub const SQS_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

impl fakecloud_core::multi_account::AccountState for SqsState {
    fn new_for_account(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self::new(account_id, region, endpoint)
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
}
