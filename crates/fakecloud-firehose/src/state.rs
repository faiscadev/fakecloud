use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedFirehoseState = Arc<RwLock<FirehoseAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FirehoseAccounts {
    pub accounts: BTreeMap<String, FirehoseState>,
}

impl FirehoseAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, account_id: &str, region: &str) -> &mut FirehoseState {
        self.accounts
            .entry(account_id.to_string())
            .or_insert_with(|| FirehoseState::new(account_id, region))
    }

    pub fn get(&self, account_id: &str) -> Option<&FirehoseState> {
        self.accounts.get(account_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirehoseState {
    pub account_id: String,
    pub region: String,
    /// Streams isolated per region: region -> name -> stream.
    pub streams_by_region: BTreeMap<String, BTreeMap<String, DeliveryStream>>,
}

impl FirehoseState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            streams_by_region: BTreeMap::new(),
        }
    }

    pub fn streams(&self, region: &str) -> Option<&BTreeMap<String, DeliveryStream>> {
        self.streams_by_region.get(region)
    }

    pub fn streams_mut(&mut self, region: &str) -> &mut BTreeMap<String, DeliveryStream> {
        self.streams_by_region
            .entry(region.to_string())
            .or_default()
    }
}

/// On-disk snapshot envelope for Firehose state. Versioned so format changes
/// fail loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct FirehoseSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<FirehoseAccounts>,
}

pub const FIREHOSE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryStream {
    pub name: String,
    pub arn: String,
    pub status: String,
    pub stream_type: String,
    pub created_at: DateTime<Utc>,
    pub last_update: DateTime<Utc>,
    pub version_id: String,
    pub destination: Option<S3Destination>,
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub encryption: Option<EncryptionConfig>,
}

/// Server-side encryption (SSE) configuration persisted on a delivery stream.
///
/// Mirrors Firehose's `DeliveryStreamEncryptionConfiguration`: `Status` is one
/// of ENABLED/DISABLED, and when ENABLED with a customer-managed key the
/// `KeyARN`/`KeyType` describe the KMS key in use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub status: String,
    pub key_type: Option<String>,
    pub key_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Destination {
    pub destination_id: String,
    pub role_arn: String,
    pub bucket_arn: String,
    pub prefix: Option<String>,
    pub error_output_prefix: Option<String>,
    pub buffering_size_mb: Option<i64>,
    pub buffering_interval_seconds: Option<i64>,
    pub compression_format: Option<String>,
    /// Lambda data-transformation pipeline; stored + echoed so a Firehose with
    /// a transform round-trips (bug-audit 2026-06-20, 1.18).
    #[serde(default)]
    pub processing_configuration: Option<serde_json::Value>,
    /// Parquet/ORC conversion config; stored + echoed.
    #[serde(default)]
    pub data_format_conversion_configuration: Option<serde_json::Value>,
}
