use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedKinesisState = Arc<RwLock<KinesisState>>;

pub struct KinesisState {
    pub account_id: String,
    pub region: String,
    pub streams: HashMap<String, KinesisStream>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisStream {
    pub stream_name: String,
    pub stream_arn: String,
    pub stream_status: String,
    pub stream_creation_timestamp: DateTime<Utc>,
    pub retention_period_hours: i32,
    pub stream_mode: String,
    pub encryption_type: String,
    pub shard_count: i32,
    pub open_shard_count: i32,
    pub tags: HashMap<String, String>,
}

impl KinesisState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            streams: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.streams.clear();
    }

    pub fn stream_arn(&self, stream_name: &str) -> String {
        format!(
            "arn:aws:kinesis:{}:{}:stream/{}",
            self.region, self.account_id, stream_name
        )
    }
}
