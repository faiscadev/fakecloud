//! Account-partitioned, serializable state for Amazon S3 Tables.
//!
//! Models the S3 Tables control plane as a hierarchy of table buckets ->
//! namespaces -> tables, plus the per-bucket and per-table sub-resource
//! configurations (encryption, maintenance, metrics, policy, replication,
//! storage class, record expiration). There is no Apache Iceberg query engine:
//! each table keeps an opaque `metadata_location` S3 URI (the pointer to its
//! Iceberg metadata) that `CreateTable` seeds, `GetTableMetadataLocation`
//! returns, and `UpdateTableMetadataLocation` advances — mirroring how the real
//! service tracks the Iceberg metadata pointer without running Iceberg.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_aws::arn::Arn;
use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const S3TABLES_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

pub type TagMap = BTreeMap<String, String>;

/// A versioned sub-resource configuration (replication) stored as the opaque
/// JSON the client submitted, alongside the concurrency `version_token`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedConfig {
    pub version_token: String,
    pub configuration: Value,
}

/// A namespace within a table bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamespaceRecord {
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
    pub owner_account_id: String,
    pub namespace_id: String,
}

/// A table within a namespace.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableRecord {
    pub table_id: String,
    pub name: String,
    pub namespace: String,
    pub arn: String,
    /// `CUSTOMER` for user tables, `AWS` for service-managed tables.
    pub table_type: String,
    /// Open table format; the model only defines `ICEBERG`.
    pub format: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub created_by: String,
    pub modified_by: String,
    pub owner_account_id: String,
    pub version_token: String,
    /// Opaque S3 URI pointing at the table's Iceberg metadata document.
    pub metadata_location: Option<String>,
    /// The create-time `metadata` union (Iceberg schema) supplied to
    /// CreateTable, retained verbatim so the initial schema is not lost.
    #[serde(default)]
    pub iceberg_metadata: Option<Value>,
    pub warehouse_location: String,
    pub managed_by_service: Option<String>,
    pub namespace_id: String,
    // Per-table sub-resources.
    pub encryption: Option<Value>,
    pub maintenance: BTreeMap<String, Value>,
    pub policy: Option<String>,
    pub record_expiration: Option<Value>,
    pub replication: Option<VersionedConfig>,
    pub storage_class: Option<Value>,
    pub tags: TagMap,
}

/// A table bucket owned by the account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableBucketRecord {
    pub arn: String,
    pub name: String,
    pub owner_account_id: String,
    pub created_at: DateTime<Utc>,
    pub table_bucket_id: String,
    /// `CUSTOMER` (user-created) or `AWS` (service-managed).
    pub bucket_type: String,
    // Per-bucket sub-resources.
    pub encryption: Option<Value>,
    pub maintenance: BTreeMap<String, Value>,
    pub metrics_id: Option<String>,
    pub policy: Option<String>,
    pub replication: Option<VersionedConfig>,
    pub storage_class: Option<Value>,
    pub tags: TagMap,
    pub namespaces: BTreeMap<String, NamespaceRecord>,
    /// Tables keyed by their opaque table id (the UUID in the table ARN).
    pub tables: BTreeMap<String, TableRecord>,
}

/// A table migration (`StartTableMigration` / `SyncTableMigration`): an AWS
/// Glue table migrated into a table of a table bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationRecord {
    pub migration_id: String,
    /// `start` or `sync`.
    pub migration_type: String,
    /// `MigrationState` value (`planning`, `pendingReview`, `succeeded`, ...).
    pub state: String,
    pub table_bucket_arn: String,
    pub namespace: String,
    pub name: String,
    /// The destination table, once the migration has created it.
    pub table_arn: Option<String>,
    pub role_arn: String,
    pub review_migration_plan: bool,
    /// Set once planning finishes.
    pub has_valid_plan: Option<bool>,
    pub created_at: DateTime<Utc>,
    pub state_updated_at: DateTime<Utc>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
    /// `PlanningStats` / `MigrationStats`, once planning finishes.
    pub planning_stats: Option<Value>,
    pub migration_stats: Option<Value>,
    /// `SourceConfiguration` as requested (`{"glueTable": {...}}`).
    pub source: Value,
    /// The `DestinationTable` request, kept so a sync reuses its settings.
    pub destination: Value,
    /// `DataMigrationOverview` with every default applied.
    pub data_migration: Value,
    /// `MigrationOutputOverview`: bucket, prefixes and the written reports.
    pub migration_output: Value,
    /// Source objects this migration plans to migrate, with their ETags.
    #[serde(default)]
    pub planned_objects: BTreeMap<String, String>,
    /// Source objects this migration removes from the table (deleted from
    /// the source since an earlier migration).
    #[serde(default)]
    pub removed_objects: Vec<String>,
    /// Source objects whose data this migration committed to the table.
    #[serde(default)]
    pub migrated_objects: BTreeMap<String, String>,
}

/// The account-scoped S3 Tables state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct S3TablesState {
    #[serde(default)]
    pub table_buckets: BTreeMap<String, TableBucketRecord>,
    /// Table migrations, keyed by migration id.
    #[serde(default)]
    pub migrations: BTreeMap<String, MigrationRecord>,
    /// Tables whose migration was completed (`CompleteTableMigration`), by
    /// table ARN: they are writable and can no longer be synced.
    #[serde(default)]
    pub completed_migrations: std::collections::BTreeSet<String>,
}

impl AccountState for S3TablesState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedS3TablesState = Arc<RwLock<MultiAccountState<S3TablesState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct S3TablesSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<S3TablesState>,
}

// ---------------------------------------------------------------------------
// ARN builders
// ---------------------------------------------------------------------------

/// ARN for a table bucket: `arn:<partition>:s3tables:<region>:<account>:bucket/<name>`.
pub fn table_bucket_arn(region: &str, account_id: &str, name: &str) -> String {
    Arn::regional("s3tables", region, account_id, &format!("bucket/{name}")).to_string()
}

/// ARN for a table: `<bucketArn>/table/<table_id>`.
pub fn table_arn(bucket_arn: &str, table_id: &str) -> String {
    format!("{bucket_arn}/table/{table_id}")
}

/// The bucket ARN prefix of a table ARN (everything before `/table/`).
pub fn bucket_arn_of_table(table_arn: &str) -> Option<&str> {
    table_arn.split_once("/table/").map(|(b, _)| b)
}

/// The table id segment of a table ARN (everything after `/table/`).
pub fn table_id_of_arn(table_arn: &str) -> Option<&str> {
    table_arn.split_once("/table/").map(|(_, id)| id)
}
