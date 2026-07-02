//! Account-partitioned, serializable state for AWS EKS.
//!
//! Batch 1 models the EKS cluster control plane: clusters (with their VPC
//! config, logging, Kubernetes network config, and tags) plus the per-cluster
//! `Update` objects produced by `UpdateClusterConfig` / `UpdateClusterVersion`
//! and read back through `DescribeUpdate` / `ListUpdates`. Node groups, addons,
//! Fargate profiles, access entries, and identity-provider configs arrive in
//! later batches.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const EKS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The default Kubernetes version a cluster is created with when the caller
/// omits `version`.
pub const DEFAULT_K8S_VERSION: &str = "1.31";

/// Tags on a resource, stored by ARN so `TagResource` / `UntagResource` /
/// `ListTagsForResource` work uniformly.
pub type TagMap = BTreeMap<String, String>;

/// A single control-plane update produced by `UpdateClusterConfig` or
/// `UpdateClusterVersion`. `params` preserves the `(type, value)` pairs AWS
/// reports back so `DescribeUpdate` round-trips faithfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Update {
    pub id: String,
    pub status: String,
    pub type_: String,
    pub params: Vec<(String, String)>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    pub name: String,
    pub arn: String,
    pub version: String,
    pub role_arn: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub endpoint: String,
    pub platform_version: String,
    pub certificate_authority_data: String,
    /// The `VpcConfigResponse`-shaped object echoed back on every describe.
    pub resources_vpc_config: serde_json::Value,
    /// The `KubernetesNetworkConfigResponse`-shaped object, if one applies.
    pub kubernetes_network_config: serde_json::Value,
    /// The `Logging` object echoed back (defaults to all types disabled).
    pub logging: serde_json::Value,
    pub tags: TagMap,
    /// Per-cluster updates keyed by update id.
    pub updates: BTreeMap<String, Update>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EksState {
    pub clusters: BTreeMap<String, Cluster>,
    /// Tags keyed by resource ARN. Cluster tags live on the cluster itself;
    /// this map holds tags applied to any other EKS ARN via `TagResource`.
    pub tags: BTreeMap<String, TagMap>,
}

impl AccountState for EksState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

/// Build the ARN for a cluster.
pub fn cluster_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:eks:{region}:{account_id}:cluster/{name}")
}

pub type SharedEksState = Arc<RwLock<MultiAccountState<EksState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct EksSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<EksState>,
}
