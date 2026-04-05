use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub type SharedLogsState = Arc<RwLock<LogsState>>;

pub struct LogsState {
    pub account_id: String,
    pub region: String,
    pub log_groups: HashMap<String, LogGroup>,
}

impl LogsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            log_groups: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.log_groups.clear();
    }
}

pub struct LogGroup {
    pub name: String,
    pub arn: String,
    pub creation_time: i64,
    pub retention_in_days: Option<i32>,
    pub tags: HashMap<String, String>,
    pub log_streams: HashMap<String, LogStream>,
    pub stored_bytes: i64,
}

pub struct LogStream {
    pub name: String,
    pub arn: String,
    pub creation_time: i64,
    pub first_event_timestamp: Option<i64>,
    pub last_event_timestamp: Option<i64>,
    pub last_ingestion_time: Option<i64>,
    pub upload_sequence_token: String,
    pub events: Vec<LogEvent>,
}

#[derive(Clone)]
pub struct LogEvent {
    pub timestamp: i64,
    pub message: String,
    pub ingestion_time: i64,
}
