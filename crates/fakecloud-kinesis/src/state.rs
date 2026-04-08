use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub type SharedKinesisState = Arc<RwLock<KinesisState>>;

pub struct KinesisState {
    pub account_id: String,
    pub region: String,
    pub streams: HashMap<String, KinesisStream>,
}

pub struct KinesisStream {
    pub stream_name: String,
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
}
