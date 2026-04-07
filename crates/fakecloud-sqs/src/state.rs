use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct MessageAttribute {
    pub data_type: String,
    pub string_value: Option<String>,
    pub binary_value: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct SqsMessage {
    pub message_id: String,
    pub receipt_handle: Option<String>,
    pub body: String,
    pub md5_of_body: String,
    pub sent_timestamp: i64,
    pub attributes: HashMap<String, String>,
    pub message_attributes: HashMap<String, MessageAttribute>,
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

#[derive(Debug, Clone)]
pub struct RedrivePolicy {
    pub dead_letter_target_arn: String,
    pub max_receive_count: u32,
}

#[derive(Debug, Clone)]
pub struct SqsQueue {
    pub queue_name: String,
    pub queue_url: String,
    pub arn: String,
    pub created_at: DateTime<Utc>,
    pub messages: VecDeque<SqsMessage>,
    pub inflight: Vec<SqsMessage>,
    pub attributes: HashMap<String, String>,
    pub is_fifo: bool,
    /// For FIFO dedup: dedup_id -> expiry
    pub dedup_cache: HashMap<String, DateTime<Utc>>,
    /// DLQ redrive policy
    pub redrive_policy: Option<RedrivePolicy>,
    /// Queue tags (key -> value)
    pub tags: HashMap<String, String>,
    /// FIFO: next sequence number counter
    pub next_sequence_number: u64,
    /// Permission labels stored on the queue
    pub permission_labels: Vec<String>,
    /// Tracks message_id -> list of all receipt handles ever issued for that message
    pub receipt_handle_map: HashMap<String, Vec<String>>,
}

pub struct SqsState {
    pub account_id: String,
    pub region: String,
    pub endpoint: String,
    pub queues: HashMap<String, SqsQueue>, // queue_url -> queue
    pub name_to_url: HashMap<String, String>, // queue_name -> queue_url
}

impl SqsState {
    pub fn new(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            endpoint: endpoint.to_string(),
            queues: HashMap::new(),
            name_to_url: HashMap::new(),
        }
    }
}

impl SqsState {
    pub fn reset(&mut self) {
        self.queues.clear();
        self.name_to_url.clear();
    }
}

pub type SharedSqsState = Arc<RwLock<SqsState>>;
