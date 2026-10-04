use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use fakecloud_aws::arn::Arn;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Kinesis state partitioned by account and region: streams, consumers,
/// channels and resource policies are regional, so the same stream name can
/// exist independently in two regions of one account.
pub type SharedKinesisState =
    Arc<RwLock<fakecloud_core::multi_account::MultiRegionState<KinesisState>>>;

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
    /// Monotonic counter feeding the shard-iterator token's tie-breaker. Using
    /// `iterators.len()` collided when two tokens were minted in the same
    /// millisecond after an eviction rebalanced the map size; a strictly
    /// increasing counter guarantees a distinct token per insert. Ephemeral,
    /// like the leases it names.
    #[serde(skip)]
    pub iterator_counter: u64,
    pub lambda_checkpoints: BTreeMap<String, usize>,
    pub consumers: BTreeMap<String, KinesisConsumer>,
    /// Delivery channels, keyed by channel name (unique per account+region on
    /// AWS). Defaulted so snapshots written before channels existed still load.
    #[serde(default)]
    pub channels: BTreeMap<String, KinesisChannel>,
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
    /// `USER_PARTITION_KEY` (default) or `AUTO`. `AUTO` is only valid on
    /// ON_DEMAND streams; it makes PutRecord(s) ignore PartitionKey and
    /// ExplicitHashKey and spread records evenly across the open shards.
    #[serde(default = "default_record_distribution_strategy")]
    pub record_distribution_strategy: String,
    /// Round-robin cursor used to place records under the `AUTO` strategy.
    #[serde(default)]
    pub auto_distribution_cursor: u64,
}

pub const RECORD_DISTRIBUTION_USER_PARTITION_KEY: &str = "USER_PARTITION_KEY";
pub const RECORD_DISTRIBUTION_AUTO: &str = "AUTO";

pub fn default_record_distribution_strategy() -> String {
    RECORD_DISTRIBUTION_USER_PARTITION_KEY.to_string()
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

/// A delivery channel (`CreateChannel`): it fans records from one or more
/// source streams into a general purpose Amazon S3 bucket or into streaming
/// tables on Apache Iceberg in Amazon S3 Tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisChannel {
    pub channel_name: String,
    pub channel_arn: String,
    pub channel_id: String,
    pub channel_status: String,
    pub channel_creation_timestamp: DateTime<Utc>,
    pub service_execution_role_arn: String,
    pub streams: Vec<KinesisChannelStream>,
    pub destination: KinesisChannelDestination,
    pub encryption: Option<KinesisChannelEncryption>,
    pub logging: KinesisChannelLogging,
    pub tags: BTreeMap<String, String>,
}

/// One source stream of a channel (`ChannelStreamDescription`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisChannelStream {
    pub stream_arn: String,
    pub stream_creation_timestamp: DateTime<Utc>,
    pub record_format_type: String,
    pub gsr_schema_arn: Option<String>,
}

/// A channel's destination. Exactly one of the two destination shapes is
/// supplied to `CreateChannel`, so the stored form is an enum rather than two
/// optional structs that could both be set or both be missing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KinesisChannelDestination {
    /// `S3DestinationConfiguration`: a general purpose Amazon S3 bucket.
    S3 {
        data_freshness_in_seconds: i32,
        dead_letter_queue: KinesisChannelDeadLetterQueue,
        storage: KinesisChannelS3Storage,
    },
    /// `S3TablesDestinationConfiguration`: streaming tables on Apache Iceberg.
    S3Tables {
        data_freshness_in_seconds: i32,
        dead_letter_queue: KinesisChannelDeadLetterQueue,
        tables: Vec<KinesisChannelS3Table>,
    },
}

impl KinesisChannelDestination {
    /// `ChannelDestinationType` for this destination, as reported by
    /// `ListChannels`.
    pub fn destination_type(&self) -> &'static str {
        match self {
            Self::S3 { .. } => "S3",
            Self::S3Tables { .. } => "S3_TABLES",
        }
    }

    /// The only member `UpdateChannel` may change on either destination.
    pub fn data_freshness_mut(&mut self) -> &mut i32 {
        match self {
            Self::S3 {
                data_freshness_in_seconds,
                ..
            }
            | Self::S3Tables {
                data_freshness_in_seconds,
                ..
            } => data_freshness_in_seconds,
        }
    }
}

/// `S3StorageConfiguration`: where an S3-destination channel writes records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisChannelS3Storage {
    pub bucket_arn: String,
    pub expected_bucket_owner: String,
    pub output_key_template: String,
    pub storage_class: String,
    pub compression_type: String,
}

/// `DeadLetterQueueS3Configuration`: where undeliverable records land.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisChannelDeadLetterQueue {
    pub bucket_arn: String,
    pub expected_bucket_owner: String,
    pub error_output_prefix: String,
}

/// `S3TablesConfiguration`: one streaming table of an S3 Tables destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisChannelS3Table {
    pub table_bucket_arn: String,
    pub namespace: String,
    pub table_name: String,
    pub compression_type: String,
    /// `PartitionSpec.PartitionFields`; empty when no spec was supplied.
    pub partition_fields: Vec<KinesisChannelPartitionField>,
}

/// One `PartitionField` of a streaming table's `PartitionSpec`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisChannelPartitionField {
    pub transform: String,
    pub source_name: String,
}

/// `ChannelEncryptionConfiguration`: the customer managed KMS key used for
/// data delivered to the destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisChannelEncryption {
    pub encryption_type: String,
    pub key_id: String,
}

/// `ChannelLoggingConfiguration.CloudWatchLogs`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisChannelLogging {
    pub enabled: bool,
    pub log_group_name: String,
    pub log_stream_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisConsumer {
    pub consumer_name: String,
    pub consumer_arn: String,
    pub consumer_status: String,
    pub consumer_creation_timestamp: DateTime<Utc>,
    pub stream_arn: String,
    /// Consumer ARNs are Tags v2 resources (`TagResource` /
    /// `ListTagsForResource`), seeded by `RegisterStreamConsumer`'s `Tags`.
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

impl KinesisState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            streams: BTreeMap::new(),
            iterators: BTreeMap::new(),
            iterator_counter: 0,
            lambda_checkpoints: BTreeMap::new(),
            consumers: BTreeMap::new(),
            channels: BTreeMap::new(),
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
        self.channels.clear();
        self.resource_policies.clear();
        self.billing_commitment_status = "DISABLED".to_string();
    }

    /// Whether an ARN names this state's region. Streams and channels are
    /// regional: an ARN naming another region addresses a resource that does
    /// not live here, even when a resource of the same name does.
    pub fn arn_in_region(&self, arn: &str) -> bool {
        fakecloud_aws::arn::region_of(arn).is_none_or(|r| r == self.region)
    }

    pub fn stream_name_from_arn(&self, arn: &str) -> Option<String> {
        if !self.arn_in_region(arn) {
            return None;
        }
        arn.rsplit('/')
            .next()
            .filter(|name| self.streams.contains_key(*name))
            .map(|name| name.to_string())
    }

    // ARN carries the request's credential-scope region (req.region), which
    // is the region this state holds.
    pub fn stream_arn(&self, region: &str, stream_name: &str) -> String {
        Arn::regional(
            "kinesis",
            region,
            &self.account_id,
            &format!("stream/{stream_name}"),
        )
        .to_string()
    }

    // Like `stream_arn`, the ARN carries the request's credential-scope region.
    // AWS puts the channel's id, not its name, in the resource segment.
    pub fn channel_arn(&self, region: &str, channel_id: &str) -> String {
        Arn::regional(
            "kinesis",
            region,
            &self.account_id,
            &format!("channel/{channel_id}"),
        )
        .to_string()
    }

    /// Resolve a `ChannelARN` to the name of an existing channel in this
    /// region. The ARN's resource segment is the channel id, so the match is
    /// against `channel_id`.
    pub fn channel_name_from_arn(&self, arn: &str) -> Option<String> {
        if !self.arn_in_region(arn) {
            return None;
        }
        let (_, channel_id) = arn.rsplit_once(":channel/")?;
        self.channels
            .values()
            .find(|channel| channel.channel_id == channel_id)
            .map(|channel| channel.channel_name.clone())
    }

    /// Names of the channels that draw from the stream named `stream_name`. A
    /// stream cannot be deleted while any channel is attached to it.
    ///
    /// Each source is resolved through [`KinesisState::stream_name_from_arn`],
    /// the same way `CreateChannel` resolved the caller's ARN.
    pub fn channels_for_stream(&self, stream_name: &str) -> Vec<String> {
        self.channels
            .values()
            .filter(|channel| {
                channel.streams.iter().any(|source| {
                    self.stream_name_from_arn(&source.stream_arn).as_deref() == Some(stream_name)
                })
            })
            .map(|channel| channel.channel_name.clone())
            .collect()
    }

    pub fn insert_iterator(
        &mut self,
        stream_name: &str,
        shard_id: &str,
        next_record_index: usize,
    ) -> String {
        self.iterators
            .retain(|_, lease| lease.expires_at >= Utc::now());
        self.iterator_counter = self.iterator_counter.wrapping_add(1);
        let token = format!(
            "{}:{}:{}:{}:{}",
            stream_name,
            shard_id,
            next_record_index,
            Utc::now().timestamp_millis(),
            self.iterator_counter
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
    pub accounts: Option<fakecloud_core::multi_account::MultiRegionState<KinesisState>>,
    /// Only set when a v1 (single-account) snapshot is migrated: that one
    /// account's state split by region, for the caller to merge into its own
    /// container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<fakecloud_core::multi_account::RegionalState<KinesisState>>,
}

/// v3: state partitioned by (account, region). v2 kept one state per
/// account with every region's streams in one name-keyed map; v1 a single
/// account's.
pub const KINESIS_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

impl fakecloud_core::multi_account::SplitByRegion for KinesisState {
    /// Streams, consumers, channels and resource policies go to the region
    /// their ARN names. Account-level settings and Lambda checkpoints (keyed
    /// by event source mapping UUID, which names no region) are copied into
    /// every resulting region so nothing a region relied on is lost.
    fn split_by_region(self, into: &mut fakecloud_core::multi_account::RegionalState<Self>) {
        use fakecloud_aws::arn::region_of;
        let region_of_owned = |arn: &str| region_of(arn).map(str::to_string);
        // Make sure the default region exists even when the account was empty.
        into.region_or_default_mut(None);
        for (name, stream) in self.streams {
            let region = region_of_owned(&stream.stream_arn);
            into.region_or_default_mut(region.as_deref())
                .streams
                .insert(name, stream);
        }
        for (arn, consumer) in self.consumers {
            let region = region_of_owned(&consumer.consumer_arn)
                .or_else(|| region_of_owned(&consumer.stream_arn));
            into.region_or_default_mut(region.as_deref())
                .consumers
                .insert(arn, consumer);
        }
        for (name, channel) in self.channels {
            let region = region_of_owned(&channel.channel_arn);
            into.region_or_default_mut(region.as_deref())
                .channels
                .insert(name, channel);
        }
        for (arn, policy) in self.resource_policies {
            let region = region_of_owned(&arn);
            into.region_or_default_mut(region.as_deref())
                .resource_policies
                .insert(arn, policy);
        }
        for (_, state) in into.regions_mut() {
            state.shard_limit = self.shard_limit;
            state.on_demand_stream_count_limit = self.on_demand_stream_count_limit;
            state
                .billing_commitment_status
                .clone_from(&self.billing_commitment_status);
            state
                .lambda_checkpoints
                .extend(self.lambda_checkpoints.iter().map(|(k, v)| (k.clone(), *v)));
        }
    }
}

/// The shape v1 and v2 snapshots stored: one state per account.
#[derive(Deserialize)]
struct LegacyKinesisSnapshot {
    #[serde(default)]
    accounts: Option<fakecloud_core::multi_account::MultiAccountState<KinesisState>>,
    #[serde(default)]
    state: Option<KinesisState>,
}

#[derive(Deserialize)]
struct SnapshotVersion {
    schema_version: u32,
}

/// Parse a persisted Kinesis snapshot, migrating older schemas to the current
/// one by moving every resource into the region its ARN names. A snapshot
/// newer than this build comes back with its on-disk `schema_version` and no
/// state, for the caller to refuse.
pub fn parse_kinesis_snapshot(bytes: &[u8]) -> Result<KinesisSnapshot, serde_json::Error> {
    let SnapshotVersion { schema_version } = serde_json::from_slice(bytes)?;
    if schema_version > KINESIS_SNAPSHOT_SCHEMA_VERSION {
        return Ok(KinesisSnapshot {
            schema_version,
            accounts: None,
            state: None,
        });
    }
    if schema_version == KINESIS_SNAPSHOT_SCHEMA_VERSION {
        return serde_json::from_slice(bytes);
    }
    let legacy: LegacyKinesisSnapshot = serde_json::from_slice(bytes)?;
    Ok(KinesisSnapshot {
        schema_version: KINESIS_SNAPSHOT_SCHEMA_VERSION,
        accounts: legacy.accounts.map(|a| a.into_regional()),
        state: legacy.state.map(|s| {
            let (account, region) = (s.account_id.clone(), s.region.clone());
            fakecloud_core::multi_account::RegionalState::from_legacy(&account, &region, "", s)
        }),
    })
}

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
            state.stream_arn(&state.region, "my-stream"),
            "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
        );
    }

    fn test_channel(state: &KinesisState, name: &str) -> KinesisChannel {
        KinesisChannel {
            channel_name: name.to_string(),
            channel_arn: state.channel_arn(&state.region, "11111111-2222-3333-4444-555555555555"),
            channel_id: "11111111-2222-3333-4444-555555555555".to_string(),
            channel_status: "ACTIVE".to_string(),
            channel_creation_timestamp: Utc::now(),
            service_execution_role_arn: "arn:aws:iam::123456789012:role/channel".to_string(),
            streams: vec![KinesisChannelStream {
                stream_arn: state.stream_arn(&state.region, "orders"),
                stream_creation_timestamp: Utc::now(),
                record_format_type: "JSON".to_string(),
                gsr_schema_arn: None,
            }],
            destination: KinesisChannelDestination::S3 {
                data_freshness_in_seconds: 300,
                dead_letter_queue: KinesisChannelDeadLetterQueue {
                    bucket_arn: "arn:aws:s3:::channel-bucket".to_string(),
                    expected_bucket_owner: "123456789012".to_string(),
                    error_output_prefix: "errors/".to_string(),
                },
                storage: KinesisChannelS3Storage {
                    bucket_arn: "arn:aws:s3:::channel-bucket".to_string(),
                    expected_bucket_owner: "123456789012".to_string(),
                    output_key_template: "kinesis-channel/!{channel-name}".to_string(),
                    storage_class: "STANDARD".to_string(),
                    compression_type: "ZSTD".to_string(),
                },
            },
            encryption: None,
            logging: KinesisChannelLogging {
                enabled: false,
                log_group_name: format!("/aws/kinesis/{name}"),
                log_stream_name: "DestinationDelivery".to_string(),
            },
            tags: BTreeMap::new(),
        }
    }

    #[test]
    fn channel_arn_format() {
        let state = KinesisState::new("123456789012", "us-east-1");
        assert_eq!(
            state.channel_arn(&state.region, "11111111-2222-3333-4444-555555555555"),
            "arn:aws:kinesis:us-east-1:123456789012:channel/11111111-2222-3333-4444-555555555555"
        );
    }

    #[test]
    fn channel_name_from_arn_resolves_only_existing_channels() {
        let mut state = KinesisState::new("123456789012", "us-east-1");
        let channel = test_channel(&state, "deliveries");
        let arn = channel.channel_arn.clone();
        state.channels.insert("deliveries".to_string(), channel);

        assert_eq!(
            state.channel_name_from_arn(&arn),
            Some("deliveries".to_string())
        );
        // Channels are regional: another region's ARN names a channel that
        // does not live in this region's state.
        assert_eq!(
            state.channel_name_from_arn(
                "arn:aws:kinesis:eu-west-1:123456789012:channel/11111111-2222-3333-4444-555555555555"
            ),
            None
        );
        assert_eq!(
            state.channel_name_from_arn("arn:aws:kinesis:us-east-1:123456789012:channel/ghost"),
            None
        );
    }

    #[test]
    fn channels_survive_snapshot_round_trip() {
        let mut state = KinesisState::new("123456789012", "us-east-1");
        let channel = test_channel(&state, "deliveries");
        state.channels.insert("deliveries".to_string(), channel);

        let json = serde_json::to_string(&state).unwrap();
        let restored: KinesisState = serde_json::from_str(&json).unwrap();
        let restored_channel = &restored.channels["deliveries"];
        assert_eq!(restored_channel.channel_status, "ACTIVE");
        assert_eq!(restored_channel.destination.destination_type(), "S3");
        assert_eq!(restored_channel.streams.len(), 1);
    }

    #[test]
    fn snapshot_without_channels_still_loads() {
        // A snapshot written before channels existed has no `channels` key;
        // it must still deserialize (into an empty map) rather than fail the
        // whole restore.
        let state = KinesisState::new("123456789012", "us-east-1");
        let mut json = serde_json::to_value(&state).unwrap();
        json.as_object_mut().unwrap().remove("channels");
        let restored: KinesisState = serde_json::from_value(json).unwrap();
        assert!(restored.channels.is_empty());
    }

    /// A stream carrying only the members the ARN-resolution paths read.
    fn insert_test_stream(state: &mut KinesisState, name: &str) {
        let stream = KinesisStream {
            stream_name: name.to_string(),
            stream_arn: state.stream_arn(&state.region, name),
            stream_status: "ACTIVE".to_string(),
            stream_creation_timestamp: Utc::now(),
            retention_period_hours: 24,
            stream_mode: "PROVISIONED".to_string(),
            encryption_type: "NONE".to_string(),
            key_id: None,
            shard_count: 0,
            open_shard_count: 0,
            tags: BTreeMap::new(),
            shards: Vec::new(),
            next_shard_index: 0,
            enhanced_metrics: Vec::new(),
            warm_throughput_mibps: None,
            max_record_size_kib: None,
            record_distribution_strategy: crate::state::default_record_distribution_strategy(),
            auto_distribution_cursor: 0,
        };
        state.streams.insert(name.to_string(), stream);
    }

    #[test]
    fn channels_for_stream_lists_attached_channels() {
        let mut state = KinesisState::new("123456789012", "us-east-1");
        insert_test_stream(&mut state, "orders");
        insert_test_stream(&mut state, "other");
        let channel = test_channel(&state, "deliveries");
        state.channels.insert("deliveries".to_string(), channel);

        assert_eq!(
            state.channels_for_stream("orders"),
            vec!["deliveries".to_string()]
        );
        assert!(state.channels_for_stream("other").is_empty());
    }

    #[test]
    fn channels_for_stream_only_matches_this_regions_stream() {
        // A source ARN naming another region's stream of the same name is not
        // an attachment of this region's stream.
        let mut state = KinesisState::new("123456789012", "eu-west-1");
        insert_test_stream(&mut state, "orders");
        let mut channel = test_channel(&state, "deliveries");
        channel.streams[0].stream_arn =
            "arn:aws:kinesis:us-east-1:123456789012:stream/orders".to_string();
        state.channels.insert("deliveries".to_string(), channel);

        assert!(state.channels_for_stream("orders").is_empty());
    }

    #[test]
    fn stream_name_from_arn_rejects_other_regions() {
        let mut state = KinesisState::new("123456789012", "eu-west-1");
        insert_test_stream(&mut state, "orders");
        assert_eq!(
            state.stream_name_from_arn("arn:aws:kinesis:eu-west-1:123456789012:stream/orders"),
            Some("orders".to_string())
        );
        assert_eq!(
            state.stream_name_from_arn("arn:aws:kinesis:us-east-1:123456789012:stream/orders"),
            None
        );
    }

    #[test]
    fn v2_snapshot_migrates_resources_into_their_arn_region() {
        use fakecloud_core::multi_account::MultiAccountState;
        let mut legacy: MultiAccountState<KinesisState> =
            MultiAccountState::new("123456789012", "us-east-1", "");
        let st = legacy.default_mut();
        st.shard_limit = 77;
        st.set_lambda_checkpoint("uuid-1", "shardId-000000000000", 5);
        // Same-named streams cannot coexist in the legacy name-keyed map, so
        // use distinct names whose ARNs name different regions.
        for (name, region) in [("east", "us-east-1"), ("west", "eu-west-1")] {
            insert_test_stream(st, name);
            let arn = format!("arn:aws:kinesis:{region}:123456789012:stream/{name}");
            st.streams.get_mut(name).unwrap().stream_arn = arn.clone();
            st.resource_policies.insert(arn.clone(), "{}".to_string());
            let consumer_arn = format!("{arn}/consumer/c:1");
            st.consumers.insert(
                consumer_arn.clone(),
                KinesisConsumer {
                    consumer_name: "c".to_string(),
                    consumer_arn,
                    consumer_status: "ACTIVE".to_string(),
                    consumer_creation_timestamp: Utc::now(),
                    stream_arn: arn,
                },
            );
        }
        let mut channel = test_channel(st, "deliveries");
        channel.channel_arn =
            "arn:aws:kinesis:eu-west-1:123456789012:channel/11111111-2222-3333-4444-555555555555"
                .to_string();
        st.channels.insert("deliveries".to_string(), channel);

        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": 2,
            "accounts": legacy,
        }))
        .unwrap();
        let snap = parse_kinesis_snapshot(&bytes).unwrap();
        assert_eq!(snap.schema_version, KINESIS_SNAPSHOT_SCHEMA_VERSION);
        let accounts = snap.accounts.unwrap();
        let east = accounts.regional("123456789012", "us-east-1").unwrap();
        let west = accounts.regional("123456789012", "eu-west-1").unwrap();
        assert_eq!(west.region, "eu-west-1");
        assert!(east.streams.contains_key("east") && !east.streams.contains_key("west"));
        assert!(west.streams.contains_key("west") && !west.streams.contains_key("east"));
        assert_eq!(east.consumers.len(), 1);
        assert_eq!(west.consumers.len(), 1);
        assert_eq!(east.resource_policies.len(), 1);
        assert!(west.channels.contains_key("deliveries"));
        assert!(east.channels.is_empty());
        assert_eq!(west.shard_limit, 77);
        assert_eq!(west.lambda_checkpoint("uuid-1", "shardId-000000000000"), 5);

        // The migrated snapshot round-trips as the current schema.
        let current = serde_json::to_vec(&KinesisSnapshot {
            schema_version: KINESIS_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
            state: None,
        })
        .unwrap();
        let again = parse_kinesis_snapshot(&current).unwrap().accounts.unwrap();
        assert!(again
            .regional("123456789012", "eu-west-1")
            .unwrap()
            .streams
            .contains_key("west"));
    }

    #[test]
    fn newer_snapshot_is_reported_not_parsed() {
        let snap = parse_kinesis_snapshot(br#"{"schema_version": 99, "accounts": 5}"#).unwrap();
        assert_eq!(snap.schema_version, 99);
        assert!(snap.accounts.is_none());
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
            stream_arn: state.stream_arn(&state.region, "s"),
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
            record_distribution_strategy: crate::state::default_record_distribution_strategy(),
            auto_distribution_cursor: 0,
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
