//! Account-partitioned, serializable state for Amazon Timestream.
//!
//! Everything a Timestream caller can create lives here: databases, tables,
//! the ingested data points (a bounded in-memory buffer per table so `Query`
//! can read them back), scheduled queries, batch-load tasks, the per-account
//! query settings, and in-flight query ids (so `CancelQuery` resolves). Config
//! blobs (`RetentionProperties`, `MagneticStoreWriteProperties`, `Schema`, a
//! scheduled query's schedule/notification/target config) are stored as their
//! already-output-valid wire JSON so a `Describe*` echoes exactly what its
//! `Create*` persisted.
//!
//! Databases are keyed by name; tables and their record buffers by
//! `"{database}\u{1}{table}"` (see [`crate::shared::table_key`]); scheduled
//! queries and batch-load tasks by their minted id/ARN. Tags live in an
//! ARN-keyed side map so `ListTagsForResource` has a single source of truth.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const TIMESTREAM_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The maximum number of ingested points retained per table. Timestream itself
/// keeps data by retention window; fakecloud keeps a bounded ring so the
/// process footprint stays flat under sustained `WriteRecords`.
pub const MAX_RECORDS_PER_TABLE: usize = 100_000;

/// A Timestream database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    pub name: String,
    pub arn: String,
    #[serde(default)]
    pub kms_key_id: Option<String>,
    pub table_count: i64,
    pub creation_time: f64,
    pub last_updated_time: f64,
}

/// A Timestream table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub database_name: String,
    pub arn: String,
    /// `ACTIVE` / `DELETING` / `RESTORING`.
    pub status: String,
    /// The `RetentionProperties` wire object, stored verbatim.
    #[serde(default)]
    pub retention_properties: Value,
    /// The `MagneticStoreWriteProperties` wire object, stored verbatim.
    #[serde(default)]
    pub magnetic_store_write_properties: Option<Value>,
    /// The `Schema` wire object (composite partition key), stored verbatim.
    #[serde(default)]
    pub schema: Value,
    pub creation_time: f64,
    pub last_updated_time: f64,
}

/// One ingested Timestream data point, normalized to nanoseconds so the query
/// engine can time-filter and format it uniformly regardless of the write-time
/// `TimeUnit`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRecord {
    /// Epoch time in nanoseconds.
    pub time_nanos: i128,
    /// Dimension name -> value, in insertion order.
    pub dimensions: Vec<(String, String)>,
    pub measure_name: String,
    pub measure_value: String,
    /// `DOUBLE` / `BIGINT` / `VARCHAR` / `BOOLEAN` / `TIMESTAMP` / `MULTI`.
    pub measure_value_type: String,
}

/// A scheduled query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledQuery {
    pub arn: String,
    pub name: String,
    pub query_string: String,
    /// `ENABLED` / `DISABLED`.
    pub state: String,
    pub creation_time: f64,
    pub schedule_configuration: Value,
    pub notification_configuration: Value,
    #[serde(default)]
    pub target_configuration: Option<Value>,
    #[serde(default)]
    pub scheduled_query_execution_role_arn: Option<String>,
    #[serde(default)]
    pub kms_key_id: Option<String>,
    #[serde(default)]
    pub error_report_configuration: Option<Value>,
    #[serde(default)]
    pub previous_invocation_time: Option<f64>,
    #[serde(default)]
    pub last_run_status: Option<String>,
}

/// A batch-load task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchLoadTask {
    pub task_id: String,
    /// `CREATED` / `IN_PROGRESS` / `FAILED` / `SUCCEEDED` / `PROGRESS_STOPPED`
    /// / `PENDING_RESUME`.
    pub status: String,
    pub target_database_name: String,
    pub target_table_name: String,
    pub data_source_configuration: Value,
    pub report_configuration: Value,
    #[serde(default)]
    pub data_model_configuration: Option<Value>,
    #[serde(default)]
    pub record_version: Option<i64>,
    pub creation_time: f64,
    pub last_updated_time: f64,
    pub resumable_until: f64,
}

/// Per-account query settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSettings {
    #[serde(default)]
    pub max_query_tcu: Option<i64>,
    /// `BYTES_SCANNED` / `COMPUTE_UNITS`.
    pub query_pricing_model: String,
    #[serde(default)]
    pub query_compute: Option<Value>,
}

impl Default for AccountSettings {
    fn default() -> Self {
        Self {
            max_query_tcu: None,
            query_pricing_model: "BYTES_SCANNED".to_string(),
            query_compute: None,
        }
    }
}

/// Per-account Amazon Timestream state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TimestreamData {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub region: String,

    /// Databases keyed by name.
    #[serde(default)]
    pub databases: BTreeMap<String, Database>,
    /// Tables keyed by `"{database}\u{1}{table}"`.
    #[serde(default)]
    pub tables: BTreeMap<String, Table>,
    /// Ingested points keyed by `"{database}\u{1}{table}"`.
    #[serde(default)]
    pub records: BTreeMap<String, Vec<StoredRecord>>,
    /// Scheduled queries keyed by ARN.
    #[serde(default)]
    pub scheduled_queries: BTreeMap<String, ScheduledQuery>,
    /// Batch-load tasks keyed by task id.
    #[serde(default)]
    pub batch_load_tasks: BTreeMap<String, BatchLoadTask>,
    /// In-flight query ids (set membership; `CancelQuery` removes).
    #[serde(default)]
    pub active_queries: BTreeMap<String, f64>,
    /// Per-account query settings.
    #[serde(default)]
    pub account_settings: AccountSettings,
    /// Resource tags keyed by ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for TimestreamData {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            ..Default::default()
        }
    }
}

pub type SharedTimestreamState = Arc<RwLock<MultiAccountState<TimestreamData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct TimestreamSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<TimestreamData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_state_seeds_ids() {
        let d = TimestreamData::new_for_account("000000000000", "us-east-1", "");
        assert_eq!(d.account_id, "000000000000");
        assert_eq!(d.region, "us-east-1");
        assert!(d.databases.is_empty());
        assert_eq!(d.account_settings.query_pricing_model, "BYTES_SCANNED");
    }
}
