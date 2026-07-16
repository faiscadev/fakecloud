use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A generic JSON-backed named-resource store. Each resource is the raw
/// create/update input plus generated metadata, persisted verbatim so reads
/// echo what the caller sent. Used for all control-plane families that don't
/// need bespoke typed state.
pub type JsonStore = BTreeMap<String, Value>;

pub type SharedGlueState = Arc<RwLock<GlueAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GlueAccounts {
    pub accounts: BTreeMap<String, GlueState>,
}

/// On-disk snapshot envelope for Glue state. Versioned so format changes fail
/// loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct GlueSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<GlueAccounts>,
}

pub const GLUE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

impl GlueAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, account_id: &str, region: &str) -> &mut GlueState {
        self.accounts
            .entry(account_id.to_string())
            .or_insert_with(|| GlueState::new(account_id, region))
    }

    pub fn get(&self, account_id: &str) -> Option<&GlueState> {
        self.accounts.get(account_id)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GlueState {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub region: String,
    /// region -> database_name -> Database
    #[serde(default)]
    pub databases: BTreeMap<String, BTreeMap<String, Database>>,
    #[serde(default)]
    pub jobs: BTreeMap<String, Job>,
    #[serde(default)]
    pub job_runs: BTreeMap<String, JobRun>,

    // --- generic JSON-backed control-plane resource families ---
    #[serde(default)]
    pub assets: JsonStore,
    #[serde(default)]
    pub crawlers: JsonStore,
    #[serde(default)]
    pub classifiers: JsonStore,
    #[serde(default)]
    pub connections: JsonStore,
    #[serde(default)]
    pub connection_types: JsonStore,
    #[serde(default)]
    pub security_configs: JsonStore,
    #[serde(default)]
    pub triggers: JsonStore,
    #[serde(default)]
    pub workflows: JsonStore,
    #[serde(default)]
    pub workflow_runs: JsonStore,
    #[serde(default)]
    pub blueprints: JsonStore,
    #[serde(default)]
    pub blueprint_runs: JsonStore,
    #[serde(default)]
    pub dev_endpoints: JsonStore,
    #[serde(default)]
    pub registries: JsonStore,
    #[serde(default)]
    pub schemas: JsonStore,
    #[serde(default)]
    pub schema_versions: JsonStore,
    #[serde(default)]
    pub sessions: JsonStore,
    #[serde(default)]
    pub statements: JsonStore,
    #[serde(default)]
    pub ml_transforms: JsonStore,
    #[serde(default)]
    pub ml_task_runs: JsonStore,
    #[serde(default)]
    pub dq_rulesets: JsonStore,
    #[serde(default)]
    pub dq_ruleset_runs: JsonStore,
    #[serde(default)]
    pub dq_recommendation_runs: JsonStore,
    #[serde(default)]
    pub dq_results: JsonStore,
    #[serde(default)]
    pub usage_profiles: JsonStore,
    #[serde(default)]
    pub catalogs: JsonStore,
    #[serde(default)]
    pub custom_entity_types: JsonStore,
    #[serde(default)]
    pub udfs: JsonStore,
    #[serde(default)]
    pub table_optimizers: JsonStore,
    #[serde(default)]
    pub integrations: JsonStore,
    #[serde(default)]
    pub integration_resource_props: JsonStore,
    #[serde(default)]
    pub integration_table_props: JsonStore,
    #[serde(default)]
    pub column_stats_task_runs: JsonStore,
    #[serde(default)]
    pub column_stats_task_settings: JsonStore,
    /// Per-table column statistics, keyed by "db\u{1f}table\u{1f}column".
    #[serde(default)]
    pub column_stats: JsonStore,
    /// Per-table-version archive, keyed by "db\u{1f}table\u{1f}version".
    #[serde(default)]
    pub table_versions: JsonStore,
    /// Partition indexes, keyed by "db\u{1f}table\u{1f}index".
    #[serde(default)]
    pub partition_indexes: JsonStore,
    /// Job bookmarks, keyed by job name.
    #[serde(default)]
    pub job_bookmarks: JsonStore,
    /// Resource policies, keyed by resource ARN ("" for the account policy).
    #[serde(default)]
    pub resource_policies: JsonStore,
    /// Tags by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Data-catalog encryption settings (single per account/region).
    #[serde(default)]
    pub encryption_settings: Option<Value>,
    /// Glue Identity Center configuration (single per account/region).
    #[serde(default)]
    pub identity_center: Option<Value>,
    /// Materialized-view refresh task runs.
    #[serde(default)]
    pub mv_refresh_runs: JsonStore,
    /// Schema-version metadata, keyed by "versionId\u{1f}key\u{1f}value".
    #[serde(default)]
    pub schema_version_metadata: JsonStore,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub name: String,
    pub description: Option<String>,
    pub role: String,
    pub created_on: DateTime<Utc>,
    pub last_modified_on: DateTime<Utc>,
    pub command: serde_json::Value,
    pub default_arguments: BTreeMap<String, String>,
    pub max_retries: i64,
    pub timeout: Option<i64>,
    pub glue_version: Option<String>,
    pub max_capacity: Option<f64>,
    pub worker_type: Option<String>,
    pub number_of_workers: Option<i64>,
    pub execution_class: Option<String>,
    /// Standard Job fields that were previously accepted then dropped
    /// (bug-audit 2026-06-20, 1.24). Round-tripped verbatim.
    #[serde(default)]
    pub connections: Option<serde_json::Value>,
    #[serde(default)]
    pub execution_property: Option<serde_json::Value>,
    #[serde(default)]
    pub security_configuration: Option<String>,
    #[serde(default)]
    pub notification_property: Option<serde_json::Value>,
    #[serde(default)]
    pub non_overridable_arguments: BTreeMap<String, String>,
    /// Standard Job fields that were previously accepted then dropped
    /// (bug-hunt 2026-07-16). Round-tripped verbatim. `CodeGenConfigurationNodes`
    /// is a large nested shape stored opaquely rather than dropped.
    #[serde(default)]
    pub source_control_details: Option<serde_json::Value>,
    #[serde(default)]
    pub code_gen_configuration_nodes: Option<serde_json::Value>,
    #[serde(default)]
    pub maintenance_window: Option<String>,
    #[serde(default)]
    pub log_uri: Option<String>,
    #[serde(default)]
    pub allocated_capacity: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRun {
    pub id: String,
    pub job_name: String,
    pub started_on: DateTime<Utc>,
    pub completed_on: Option<DateTime<Utc>>,
    pub state: String,
    pub arguments: BTreeMap<String, String>,
    pub error_message: Option<String>,
    pub execution_time: i64,
    pub attempt: i64,
}

impl GlueState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            databases: BTreeMap::new(),
            jobs: BTreeMap::new(),
            job_runs: BTreeMap::new(),
            ..Default::default()
        }
    }

    pub fn dbs_in(&self, region: &str) -> Option<&BTreeMap<String, Database>> {
        self.databases.get(region)
    }

    pub fn dbs_in_mut(&mut self, region: &str) -> &mut BTreeMap<String, Database> {
        self.databases.entry(region.to_string()).or_default()
    }

    fn tv_prefix(db: &str, table: &str) -> String {
        format!("{db}\u{1f}{table}\u{1f}")
    }

    /// Version ids currently archived for a table, sorted ascending.
    pub fn table_version_ids(&self, db: &str, table: &str) -> Vec<i64> {
        let prefix = Self::tv_prefix(db, table);
        let mut ids: Vec<i64> = self
            .table_versions
            .keys()
            .filter_map(|k| k.strip_prefix(&prefix))
            .filter_map(|v| v.parse::<i64>().ok())
            .collect();
        ids.sort_unstable();
        ids
    }

    /// Archive a new version of a table (`table_json` is the projected Table
    /// object) and return the assigned VersionId. Glue assigns monotonically
    /// increasing integer version ids per table starting at 1.
    pub fn archive_table_version(&mut self, db: &str, table: &str, table_json: Value) -> String {
        let next = self
            .table_version_ids(db, table)
            .last()
            .copied()
            .unwrap_or(0)
            + 1;
        let key = format!("{}{next}", Self::tv_prefix(db, table));
        self.table_versions.insert(
            key,
            serde_json::json!({
                "Table": table_json,
                "VersionId": next.to_string(),
            }),
        );
        next.to_string()
    }

    /// Drop every archived version of a table (used on DeleteTable).
    pub fn purge_table_versions(&mut self, db: &str, table: &str) {
        let prefix = Self::tv_prefix(db, table);
        self.table_versions.retain(|k, _| !k.starts_with(&prefix));
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub description: Option<String>,
    pub location_uri: Option<String>,
    pub parameters: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub catalog_id: String,
    /// Tables keyed by name.
    pub tables: BTreeMap<String, Table>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub database_name: String,
    /// Owning Data Catalog id (the account id). Reported on read as `CatalogId`.
    #[serde(default)]
    pub catalog_id: String,
    pub description: Option<String>,
    pub owner: Option<String>,
    pub create_time: DateTime<Utc>,
    pub update_time: DateTime<Utc>,
    pub last_access_time: Option<DateTime<Utc>>,
    pub retention: i64,
    pub storage_descriptor: Option<StorageDescriptor>,
    pub partition_keys: Vec<Column>,
    pub view_original_text: Option<String>,
    pub view_expanded_text: Option<String>,
    pub table_type: Option<String>,
    pub parameters: BTreeMap<String, String>,
    /// Partitions keyed by joined partition values ("v1/v2").
    pub partitions: BTreeMap<String, Partition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partition {
    pub values: Vec<String>,
    /// Owning Data Catalog id (the account id). Reported on read; the Terraform
    /// `aws_glue_partition` resource asserts `catalog_id`.
    #[serde(default)]
    pub catalog_id: String,
    pub database_name: String,
    pub table_name: String,
    pub create_time: DateTime<Utc>,
    pub last_access_time: Option<DateTime<Utc>>,
    pub storage_descriptor: Option<StorageDescriptor>,
    pub parameters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StorageDescriptor {
    pub columns: Vec<Column>,
    pub location: Option<String>,
    pub input_format: Option<String>,
    pub output_format: Option<String>,
    pub compressed: Option<bool>,
    pub serde_info: Option<SerdeInfo>,
    pub parameters: BTreeMap<String, String>,
    /// Bucketing columns. Round-tripped on GetTable/GetPartition so the
    /// Terraform `storage_descriptor` block sees a stable plan.
    #[serde(default)]
    pub bucket_columns: Vec<String>,
    #[serde(default)]
    pub number_of_buckets: Option<i64>,
    #[serde(default)]
    pub stored_as_sub_directories: Option<bool>,
    /// Sort columns: list of `{Column, SortOrder}`. Stored verbatim.
    #[serde(default)]
    pub sort_columns: Vec<Value>,
    /// Skew handling: `{SkewedColumnNames, SkewedColumnValues,
    /// SkewedColumnValueLocationMaps}`. Stored verbatim.
    #[serde(default)]
    pub skewed_info: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub comment: Option<String>,
    #[serde(default)]
    pub parameters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerdeInfo {
    pub name: Option<String>,
    pub serialization_library: Option<String>,
    pub parameters: BTreeMap<String, String>,
}
