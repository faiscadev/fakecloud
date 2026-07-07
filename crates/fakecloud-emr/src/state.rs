//! Account-partitioned, serializable state for Amazon EMR (`elasticmapreduce`).
//!
//! EMR resources are stored as assembled, response-shaped `serde_json::Value`s
//! keyed by their natural identifiers so a read returns exactly what was
//! written (round-trip fidelity) and the on-the-wire JSON never carries a field
//! that isn't in the Smithy model. Cluster sub-resources (steps, instance
//! groups/fleets, instances, bootstrap actions) are kept in side tables keyed
//! by cluster id; policies and tags are keyed by cluster id or resource id.
//!
//! The Spark/Hadoop data plane (a real cluster running jobs in containers) is a
//! later batch: here a `RunJobFlow` settles to `WAITING`/`RUNNING` honestly via
//! the control-plane state machine, and every other operation is real,
//! persisted, account-partitioned CRUD.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const EMR_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The account-scoped EMR state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmrState {
    /// Clusters keyed by cluster id (`j-XXXXXXXXXXXXX`); value is a `Cluster`
    /// shape JSON.
    #[serde(default)]
    pub clusters: BTreeMap<String, Value>,
    /// Insertion order of cluster ids (most-recent first is applied at read).
    #[serde(default)]
    pub cluster_order: Vec<String>,
    /// `JobFlowDetail` shape JSON keyed by cluster id, serving the deprecated
    /// `DescribeJobFlows` operation (kept in sync with the cluster's lifecycle
    /// state on terminate/modify).
    #[serde(default)]
    pub job_flows: BTreeMap<String, Value>,
    /// Steps keyed by cluster id; value is a list of `Step` shape JSON
    /// (most-recently-added last).
    #[serde(default)]
    pub steps: BTreeMap<String, Vec<Value>>,
    /// Instance groups keyed by cluster id; value is a list of `InstanceGroup`.
    #[serde(default)]
    pub instance_groups: BTreeMap<String, Vec<Value>>,
    /// Instance fleets keyed by cluster id; value is a list of `InstanceFleet`.
    #[serde(default)]
    pub instance_fleets: BTreeMap<String, Vec<Value>>,
    /// Cluster EC2 instances keyed by cluster id; value is a list of `Instance`.
    #[serde(default)]
    pub instances: BTreeMap<String, Vec<Value>>,
    /// Bootstrap actions keyed by cluster id; value is a list of `Command`.
    #[serde(default)]
    pub bootstrap_actions: BTreeMap<String, Vec<Value>>,
    /// Security configurations keyed by name; value is
    /// `{Name, SecurityConfiguration, CreationDateTime}`.
    #[serde(default)]
    pub security_configurations: BTreeMap<String, Value>,
    /// Insertion order of security-configuration names.
    #[serde(default)]
    pub security_config_order: Vec<String>,
    /// Studios keyed by studio id (`es-XXXXXXXXXXXXX`); value is a `Studio`.
    #[serde(default)]
    pub studios: BTreeMap<String, Value>,
    /// Insertion order of studio ids.
    #[serde(default)]
    pub studio_order: Vec<String>,
    /// Studio session mappings keyed by
    /// `<studioId>\u{1}<identityType>\u{1}<identityKey>`; value is a
    /// `SessionMappingDetail`.
    #[serde(default)]
    pub studio_session_mappings: BTreeMap<String, Value>,
    /// Notebook executions keyed by notebook-execution id; value is a
    /// `NotebookExecution`.
    #[serde(default)]
    pub notebook_executions: BTreeMap<String, Value>,
    /// Insertion order of notebook-execution ids.
    #[serde(default)]
    pub notebook_order: Vec<String>,
    /// Persistent app UIs keyed by id; value is a `PersistentAppUI`.
    #[serde(default)]
    pub persistent_app_uis: BTreeMap<String, Value>,
    /// Interactive sessions keyed by `<clusterId>\u{1}<sessionId>`; value is a
    /// `Session`.
    #[serde(default)]
    pub sessions: BTreeMap<String, Value>,
    /// Account-level block-public-access `BlockPublicAccessConfiguration`.
    #[serde(default)]
    pub block_public_access: Option<Value>,
    /// Account-level block-public-access `BlockPublicAccessConfigurationMetadata`.
    #[serde(default)]
    pub block_public_access_metadata: Option<Value>,
    /// Tags keyed by resource id (cluster id / studio id); value is a `TagList`
    /// (list of `{Key,Value}` objects).
    #[serde(default)]
    pub tags: BTreeMap<String, Vec<Value>>,
    /// Auto-termination policies keyed by cluster id; value is an
    /// `AutoTerminationPolicy`.
    #[serde(default)]
    pub auto_termination_policies: BTreeMap<String, Value>,
    /// Managed-scaling policies keyed by cluster id; value is a
    /// `ManagedScalingPolicy`.
    #[serde(default)]
    pub managed_scaling_policies: BTreeMap<String, Value>,
    /// Auto-scaling policies keyed by `<clusterId>\u{1}<instanceGroupId>`; value
    /// is an `AutoScalingPolicyDescription`.
    #[serde(default)]
    pub auto_scaling_policies: BTreeMap<String, Value>,
}

impl AccountState for EmrState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedEmrState = Arc<RwLock<MultiAccountState<EmrState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct EmrSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<EmrState>,
}
