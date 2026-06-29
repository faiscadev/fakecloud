use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedKinesisState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<KinesisState>>>;

impl fakecloud_core::multi_account::AccountState for KinesisState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct KinesisState {
    pub account_id: String,
    pub region: String,
    pub streams: BTreeMap<String, KinesisStream>,
    /// Shard-iterator leases are ephemeral (AWS expires them after 5 minutes
    /// and they never survive a service restart). They were serialized into the
    /// snapshot with an absolute `expires_at`, so after any restart taking
    /// longer than 5 minutes every restored iterator was already expired —
    /// half-done durability that guaranteed `ExpiredIteratorException` on the
    /// first GetRecords. Skip persisting them; consumers re-acquire via
    /// GetShardIterator against the (durable) sequence number, exactly as on AWS
    /// (bug-hunt 2026-06-24, 4.2).
    #[serde(skip)]
    pub iterators: BTreeMap<String, ShardIteratorLease>,
    pub lambda_checkpoints: BTreeMap<String, usize>,
    pub consumers: BTreeMap<String, KinesisConsumer>,
    pub resource_policies: BTreeMap<String, String>,
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
    pub tags: BTreeMap<String, String>,
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
            streams: BTreeMap::new(),
            iterators: BTreeMap::new(),
            lambda_checkpoints: BTreeMap::new(),
            consumers: BTreeMap::new(),
            resource_policies: BTreeMap::new(),
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

    /// Physically drop shard records older than each stream's retention
    /// period. `get_records` only advanced the read cursor past expired
    /// records, so they lived in memory (and in every snapshot) forever.
    /// Removing them from the front of a shard shifts all index-based
    /// offsets, so we decrement the Lambda checkpoints and shard-iterator
    /// leases that point into the trimmed shard by the same amount, keeping
    /// sequence/iterator correctness intact.
    pub fn trim_expired_records(&mut self) {
        self.trim_expired_records_at(Utc::now());
    }

    /// [`trim_expired_records`] with an explicit "now", for deterministic tests.
    pub fn trim_expired_records_at(&mut self, now: DateTime<Utc>) {
        for (stream_name, stream) in self.streams.iter_mut() {
            let cutoff = now - Duration::hours(stream.retention_period_hours as i64);
            for shard in stream.shards.iter_mut() {
                // Records are stored in arrival order, so the expired ones
                // are a contiguous prefix.
                let trim = shard
                    .records
                    .iter()
                    .take_while(|r| r.approximate_arrival_timestamp < cutoff)
                    .count();
                if trim == 0 {
                    continue;
                }
                shard.records.drain(0..trim);

                // Shift index-based offsets that referenced this shard.
                for (key, offset) in self.lambda_checkpoints.iter_mut() {
                    if key
                        .rsplit_once(':')
                        .map(|(_, sid)| sid == shard.shard_id)
                        .unwrap_or(false)
                    {
                        *offset = offset.saturating_sub(trim);
                    }
                }
                for lease in self.iterators.values_mut() {
                    if lease.stream_name == *stream_name && lease.shard_id == shard.shard_id {
                        lease.next_record_index = lease.next_record_index.saturating_sub(trim);
                    }
                }
            }
        }
    }
}

/// On-disk snapshot envelope for Kinesis state. Versioned so format
/// changes fail loudly on upgrade.
#[derive(Clone, Serialize, Deserialize)]
pub struct KinesisSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<KinesisState>>,
    #[serde(default)]
    pub state: Option<KinesisState>,
}

pub const KINESIS_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_has_empty_collections() {
        let state = KinesisState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.streams.is_empty());
        assert!(state.iterators.is_empty());
        assert_eq!(state.shard_limit, 500);
    }

    #[test]
    fn iterators_are_not_persisted_through_snapshot() {
        let mut state = KinesisState::new("123456789012", "us-east-1");
        let token = state.insert_iterator("s", "shardId-000000000000", 0);
        assert!(state.iterators.contains_key(&token));

        // Round-trip through the serde snapshot: the ephemeral iterator lease
        // must not survive (it would be expired-on-load otherwise, 4.2).
        let json = serde_json::to_string(&state).unwrap();
        assert!(
            !json.contains("expires_at"),
            "iterator leases must not be serialized: {json}"
        );
        let restored: KinesisState = serde_json::from_str(&json).unwrap();
        assert!(restored.iterators.is_empty());
    }

    #[test]
    fn stream_arn_format() {
        let state = KinesisState::new("123456789012", "us-east-1");
        assert_eq!(
            state.stream_arn("my-stream"),
            "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
        );
    }

    #[test]
    fn stream_name_from_arn_unknown_stream_returns_none() {
        let state = KinesisState::new("123456789012", "us-east-1");
        assert_eq!(
            state.stream_name_from_arn("arn:aws:kinesis:us-east-1:123:stream/ghost"),
            None
        );
    }

    #[test]
    fn reset_clears_all() {
        let mut state = KinesisState::new("123456789012", "us-east-1");
        state.billing_commitment_status = "ENABLED".to_string();
        state.reset();
        assert_eq!(state.billing_commitment_status, "DISABLED");
    }

    #[test]
    fn lambda_checkpoint_default_zero() {
        let state = KinesisState::new("123456789012", "us-east-1");
        assert_eq!(state.lambda_checkpoint("uuid-1", "shard-0"), 0);
    }

    #[test]
    fn set_and_get_lambda_checkpoint() {
        let mut state = KinesisState::new("123456789012", "us-east-1");
        state.set_lambda_checkpoint("uuid-1", "shard-0", 42);
        assert_eq!(state.lambda_checkpoint("uuid-1", "shard-0"), 42);
    }

    fn record_at(seq: &str, age_hours: i64) -> KinesisRecord {
        KinesisRecord {
            sequence_number: seq.to_string(),
            partition_key: "pk".to_string(),
            data: vec![1, 2, 3],
            approximate_arrival_timestamp: Utc::now() - Duration::hours(age_hours),
        }
    }

    #[test]
    fn trim_expired_records_drops_old_and_shifts_offsets() {
        let mut state = KinesisState::new("123456789012", "us-east-1");
        let shard_id = "shardId-000000000000".to_string();
        let stream = KinesisStream {
            stream_name: "s".to_string(),
            stream_arn: state.stream_arn("s"),
            stream_status: "ACTIVE".to_string(),
            stream_creation_timestamp: Utc::now(),
            retention_period_hours: 24,
            stream_mode: "PROVISIONED".to_string(),
            encryption_type: "NONE".to_string(),
            key_id: None,
            shard_count: 1,
            open_shard_count: 1,
            tags: BTreeMap::new(),
            shards: vec![KinesisShard {
                shard_id: shard_id.clone(),
                starting_hash_key: "0".to_string(),
                ending_hash_key: "1".to_string(),
                parent_shard_id: None,
                adjacent_parent_shard_id: None,
                is_open: true,
                next_sequence_number: 3,
                // Two records aged past the 24h window, one fresh.
                records: vec![record_at("0", 48), record_at("1", 48), record_at("2", 1)],
            }],
            next_shard_index: 1,
            enhanced_metrics: Vec::new(),
            warm_throughput_mibps: None,
            max_record_size_kib: None,
        };
        state.streams.insert("s".to_string(), stream);

        // A consumer checkpoint that has read all 3, and a live iterator
        // pointing past all 3 — both index-based.
        state.set_lambda_checkpoint("uuid-1", &shard_id, 3);
        state.iterators.insert(
            "tok".to_string(),
            ShardIteratorLease {
                iterator_token: "tok".to_string(),
                stream_name: "s".to_string(),
                shard_id: shard_id.clone(),
                next_record_index: 3,
                expires_at: Utc::now() + Duration::minutes(5),
            },
        );

        state.trim_expired_records();

        let shard = &state.streams["s"].shards[0];
        assert_eq!(shard.records.len(), 1, "two expired records trimmed");
        assert_eq!(shard.records[0].sequence_number, "2", "fresh record kept");
        assert_eq!(
            state.lambda_checkpoint("uuid-1", &shard_id),
            1,
            "checkpoint shifted down by the trimmed prefix"
        );
        assert_eq!(
            state.iterators["tok"].next_record_index, 1,
            "iterator offset shifted down by the trimmed prefix"
        );
    }
}
