use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SqsMessage {
    pub message_id: String,
    pub receipt_handle: Option<String>,
    pub body: String,
    pub md5_of_body: String,
    pub sent_timestamp: i64,
    pub attributes: HashMap<String, String>,
    /// When this message becomes visible again (after ReceiveMessage)
    pub visible_at: Option<DateTime<Utc>>,
    pub receive_count: u32,
    /// For FIFO: message group ID
    pub message_group_id: Option<String>,
    /// For FIFO: dedup ID
    pub message_dedup_id: Option<String>,
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
}

pub struct SqsState {
    pub account_id: String,
    pub region: String,
    pub queues: HashMap<String, SqsQueue>, // queue_url -> queue
    pub name_to_url: HashMap<String, String>, // queue_name -> queue_url
}

impl SqsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            queues: HashMap::new(),
            name_to_url: HashMap::new(),
        }
    }
}

pub type SharedSqsState = Arc<RwLock<SqsState>>;
