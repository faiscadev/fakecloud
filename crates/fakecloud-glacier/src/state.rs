//! Account-partitioned, serializable state for Amazon S3 Glacier.
//!
//! Models vaults and everything scoped under them: archives (with their real
//! stored bytes and SHA-256 tree hash), in-flight multipart uploads and their
//! parts, retrieval/inventory jobs (with their computed output), the vault
//! notification configuration, the vault access policy, the vault-lock state
//! machine, and per-vault tags. Two resources are account-scoped rather than
//! vault-scoped: the data-retrieval policy and the list of provisioned
//! capacity units.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const GLACIER_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Tags on a vault.
pub type TagMap = BTreeMap<String, String>;

/// A stored archive. Glacier keeps the real bytes; they are test-sized so the
/// snapshot round-trips them verbatim (serde encodes `Vec<u8>` as a JSON array).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Archive {
    pub id: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub data: Vec<u8>,
    /// The SHA-256 tree hash (lowercase hex) computed over the bytes.
    pub tree_hash: String,
    pub size: u64,
}

/// A single uploaded part of an in-flight multipart upload, keyed by its
/// start-of-range byte offset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Part {
    pub range_start: u64,
    pub range_end: u64,
    pub data: Vec<u8>,
    pub tree_hash: String,
}

/// An in-flight multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub id: String,
    pub archive_description: String,
    pub part_size: u64,
    pub created_at: DateTime<Utc>,
    /// Parts keyed by their range start offset.
    pub parts: BTreeMap<u64, Part>,
}

/// A retrieval or inventory job. Jobs are created `InProgress` and settle to
/// `Succeeded` on the first read (mirroring how the control plane settles
/// pending transitions on describe), so `GetJobOutput` returns real bytes
/// end-to-end without waiting for the multi-hour AWS SLA.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    /// `ArchiveRetrieval` or `InventoryRetrieval`.
    pub action: String,
    pub description: Option<String>,
    pub archive_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub completed: bool,
    pub completion_date: Option<DateTime<Utc>>,
    /// `InProgress`, `Succeeded`, or `Failed`.
    pub status_code: String,
    pub status_message: Option<String>,
    pub sns_topic: Option<String>,
    pub tier: String,
    /// The job's output bytes: the archive bytes for an archive-retrieval job,
    /// the JSON inventory for an inventory-retrieval job.
    pub output: Vec<u8>,
    pub output_content_type: String,
    pub archive_size: Option<u64>,
    pub inventory_size: Option<u64>,
    pub sha256_tree_hash: Option<String>,
    pub archive_sha256_tree_hash: Option<String>,
    pub retrieval_byte_range: Option<String>,
}

/// The vault-lock state machine. AWS transitions
/// `Unlocked -> InProgress -> Locked`; `InitiateVaultLock` starts it, and the
/// lock id expires 24h later unless `CompleteVaultLock` finalises it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VaultLock {
    /// `""` (none), `"InProgress"`, or `"Locked"`.
    pub state: String,
    pub policy: Option<String>,
    pub lock_id: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    /// 24h after `InitiateVaultLock` while `InProgress`; cleared once `Locked`.
    pub expiration_date: Option<DateTime<Utc>>,
}

/// The vault notification configuration (SNS topic + subscribed events).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationConfig {
    pub sns_topic: Option<String>,
    pub events: Vec<String>,
}

/// A single vault.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Vault {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub last_inventory_date: Option<DateTime<Utc>>,
    #[serde(default)]
    pub archives: BTreeMap<String, Archive>,
    #[serde(default)]
    pub multipart_uploads: BTreeMap<String, MultipartUpload>,
    #[serde(default)]
    pub jobs: BTreeMap<String, Job>,
    #[serde(default)]
    pub notification_config: Option<NotificationConfig>,
    #[serde(default)]
    pub access_policy: Option<String>,
    #[serde(default)]
    pub lock: VaultLock,
    #[serde(default)]
    pub tags: TagMap,
}

impl Vault {
    pub fn total_size(&self) -> u64 {
        self.archives.values().map(|a| a.size).sum()
    }
}

/// A provisioned-capacity unit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProvisionedCapacity {
    pub capacity_id: String,
    pub start_date: DateTime<Utc>,
    pub expiration_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GlacierState {
    /// Vaults keyed by vault name.
    #[serde(default)]
    pub vaults: BTreeMap<String, Vault>,
    /// The account-scoped data-retrieval policy (the `Policy` object). `None`
    /// means the default `FreeTier` policy is reported.
    #[serde(default)]
    pub data_retrieval_policy: Option<serde_json::Value>,
    /// Account-scoped provisioned-capacity units.
    #[serde(default)]
    pub provisioned_capacity: Vec<ProvisionedCapacity>,
}

impl AccountState for GlacierState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedGlacierState = Arc<RwLock<MultiAccountState<GlacierState>>>;

/// The serialized snapshot form persisted to disk.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlacierSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<GlacierState>,
}

/// The ARN of a vault: `arn:aws:glacier:<region>:<account>:vaults/<name>`.
pub fn vault_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:glacier:{region}:{account_id}:vaults/{name}")
}
