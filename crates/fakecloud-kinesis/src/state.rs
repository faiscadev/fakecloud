use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedKinesisState = Arc<RwLock<KinesisState>>;

pub struct KinesisState {
    pub account_id: String,
    pub region: String,
    pub streams: HashMap<String, KinesisStream>,
    pub iterators: HashMap<String, ShardIteratorLease>,
    pub lambda_checkpoints: HashMap<String, usize>,
    pub consumers: HashMap<String, KinesisConsumer>,
    pub resource_policies: HashMap<String, String>,
    pub shard_limit: i32,
    pub on_demand_stream_count_limit: i32,
    pub billing_commitment_status: String,
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
    pub key_id: Option<String>,
    pub shard_count: i32,
    pub open_shard_count: i32,
    pub tags: HashMap<String, String>,
    pub shards: Vec<KinesisShard>,
    pub next_shard_index: i32,
    pub enhanced_metrics: Vec<String>,
    pub warm_throughput_mibps: Option<i64>,
    pub max_record_size_kib: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisShard {
    pub shard_id: String,
    pub starting_hash_key: String,
    pub ending_hash_key: String,
    pub parent_shard_id: Option<String>,
    pub adjacent_parent_shard_id: Option<String>,
    pub is_open: bool,
    pub next_sequence_number: u128,
    pub records: Vec<KinesisRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisRecord {
    pub sequence_number: String,
    pub partition_key: String,
    pub data: Vec<u8>,
    pub approximate_arrival_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardIteratorLease {
    pub iterator_token: String,
    pub stream_name: String,
    pub shard_id: String,
    pub next_record_index: usize,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisConsumer {
    pub consumer_name: String,
    pub consumer_arn: String,
    pub consumer_status: String,
    pub consumer_creation_timestamp: DateTime<Utc>,
    pub stream_arn: String,
}

impl KinesisState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            streams: HashMap::new(),
            iterators: HashMap::new(),
            lambda_checkpoints: HashMap::new(),
            consumers: HashMap::new(),
            resource_policies: HashMap::new(),
            shard_limit: 500,
            on_demand_stream_count_limit: 50,
            billing_commitment_status: "DISABLED".to_string(),
        }
    }

    pub fn reset(&mut self) {
        self.streams.clear();
        self.iterators.clear();
        self.lambda_checkpoints.clear();
        self.consumers.clear();
        self.resource_policies.clear();
        self.billing_commitment_status = "DISABLED".to_string();
    }

    pub fn stream_name_from_arn(&self, arn: &str) -> Option<String> {
        arn.rsplit('/')
            .next()
            .filter(|name| self.streams.contains_key(*name))
            .map(|name| name.to_string())
    }

    pub fn stream_arn(&self, stream_name: &str) -> String {
        format!(
            "arn:aws:kinesis:{}:{}:stream/{}",
            self.region, self.account_id, stream_name
        )
    }

    pub fn insert_iterator(
        &mut self,
        stream_name: &str,
        shard_id: &str,
        next_record_index: usize,
    ) -> String {
        self.iterators
            .retain(|_, lease| lease.expires_at >= Utc::now());
        let token = format!(
            "{}:{}:{}:{}:{}",
            stream_name,
            shard_id,
            next_record_index,
            Utc::now().timestamp_millis(),
            self.iterators.len() + 1
        );
        self.iterators.insert(
            token.clone(),
            ShardIteratorLease {
                iterator_token: token.clone(),
                stream_name: stream_name.to_string(),
                shard_id: shard_id.to_string(),
                next_record_index,
                expires_at: Utc::now() + Duration::minutes(5),
            },
        );
        token
    }

    pub fn lambda_checkpoint(&self, mapping_uuid: &str, shard_id: &str) -> usize {
        self.lambda_checkpoints
            .get(&format!("{mapping_uuid}:{shard_id}"))
            .copied()
            .unwrap_or(0)
    }

    pub fn set_lambda_checkpoint(&mut self, mapping_uuid: &str, shard_id: &str, offset: usize) {
        self.lambda_checkpoints
            .insert(format!("{mapping_uuid}:{shard_id}"), offset);
    }
}
