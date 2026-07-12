//! Account/region-partitioned, serializable state for Amazon Managed Service
//! for Apache Flink (`kinesisanalyticsv2`).
//!
//! Models the full control plane: applications (SQL + Flink flavors), their
//! version history, snapshots, async operation records, CloudWatch logging
//! options, VPC configurations, maintenance windows, and ARN-keyed tags. The
//! real Flink-job data plane (a running Docker container executing the job) is
//! layered on top of this control plane in a later batch, mirroring how other
//! FakeCloud services shipped their control plane first.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const KA2_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Tags on a resource, stored by ARN.
pub type TagMap = BTreeMap<String, String>;

/// A single application snapshot (Flink savepoint) record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub name: String,
    pub status: String,
    pub application_version_id: i64,
    pub create_timestamp: DateTime<Utc>,
    pub runtime_environment: String,
}

/// An async operation record (one per Start/Stop/Update/Rollback/config op).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationRecord {
    pub operation_id: String,
    pub operation: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub status: String,
    pub version_from: Option<i64>,
    pub version_to: Option<i64>,
}

/// A persisted application version (config snapshot).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionRecord {
    pub version_id: i64,
    pub status: String,
    pub create_timestamp: DateTime<Utc>,
    /// Full `ApplicationDetail` JSON captured for this version so
    /// `DescribeApplicationVersion` round-trips faithfully.
    pub detail: Value,
}

/// The persisted, describe-facing binding to an application's REAL backing
/// Flink session-cluster container + submitted job. Present only for
/// Flink-flavor applications that have been started with a container runtime
/// attached; `None` for SQL-flavor apps and the control-plane-only path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlinkBinding {
    /// Backing Flink cluster container id.
    pub container_id: String,
    /// Reach address for the published REST port (`127.0.0.1` or the sibling
    /// host alias when fakecloud is containerized).
    pub host: String,
    /// Host port the container's Flink REST port (8081) is published on.
    pub rest_port: u16,
    /// Flink jar id returned by `POST /jars/upload` (once the job is submitted).
    pub jar_id: Option<String>,
    /// Flink job id returned by `POST /jars/{jar_id}/run` (once submitted).
    pub job_id: Option<String>,
}

impl FlinkBinding {
    /// The reachable Flink Web Dashboard / REST base URL.
    pub fn dashboard_url(&self) -> String {
        format!("http://{}:{}", self.host, self.rest_port)
    }
}

/// A managed Flink / SQL application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Application {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub runtime_environment: String,
    pub service_execution_role: Option<String>,
    pub application_mode: Option<String>,
    pub status: String,
    pub version_id: i64,
    pub create_timestamp: DateTime<Utc>,
    pub last_update_timestamp: DateTime<Utc>,
    pub conditional_token: String,
    /// The `ApplicationConfigurationDescription` object (output-shaped), or
    /// `Value::Null` when the application has no configuration.
    pub config_description: Value,
    /// `CloudWatchLoggingOptionDescription` objects.
    pub cloudwatch_logging_options: Vec<Value>,
    pub maintenance_start: String,
    pub maintenance_end: String,
    pub snapshots: BTreeMap<String, Snapshot>,
    pub operations: Vec<OperationRecord>,
    pub versions: BTreeMap<i64, VersionRecord>,
    /// Monotonic counter used to mint sub-resource ids.
    pub id_counter: u64,
    pub version_updated_from: Option<i64>,
    pub version_rolled_back_from: Option<i64>,
    pub version_rolled_back_to: Option<i64>,
    /// Binding to the REAL backing Flink cluster + job, when one is running.
    /// `#[serde(default)]` so snapshots written before the data-plane batch
    /// (which had no such field) still load.
    #[serde(default)]
    pub flink_binding: Option<FlinkBinding>,
}

impl Application {
    /// Mint the next sub-resource id (CloudWatch logging option id, input id,
    /// output id, ...). AWS uses short numeric-ish ids; a monotonic counter is
    /// faithful in shape and guarantees uniqueness within the application.
    pub fn next_id(&mut self) -> String {
        self.id_counter += 1;
        self.id_counter.to_string()
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Ka2State {
    pub applications: BTreeMap<String, Application>,
    /// Tags keyed by resource ARN.
    pub tags: BTreeMap<String, TagMap>,
}

impl AccountState for Ka2State {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedKa2State = Arc<RwLock<MultiAccountState<Ka2State>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Ka2Snapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<Ka2State>,
}
