//! Account-partitioned, serializable state for AWS EKS.
//!
//! Models the EKS cluster control plane: clusters (with their VPC config,
//! logging, Kubernetes network config, and tags) plus the per-cluster `Update`
//! objects produced by `UpdateClusterConfig` / `UpdateClusterVersion` and read
//! back through `DescribeUpdate` / `ListUpdates`; managed node groups and
//! Fargate profiles (both cluster sub-resources, each with their own status
//! lifecycle, and node groups with their own update history). Addons, access
//! entries, and identity-provider configs arrive in later batches.

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

/// A managed node group, a sub-resource of a cluster. Complex members
/// (scaling config, subnets, labels, taints, ...) are stored as the
/// `VpcConfigResponse`-style JSON objects echoed back on every describe so
/// `DescribeNodegroup` round-trips faithfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nodegroup {
    pub name: String,
    pub arn: String,
    pub cluster_name: String,
    pub version: String,
    pub release_version: String,
    pub status: String,
    pub capacity_type: String,
    pub ami_type: String,
    pub node_role: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub disk_size: i64,
    pub scaling_config: serde_json::Value,
    pub update_config: serde_json::Value,
    pub instance_types: serde_json::Value,
    pub subnets: serde_json::Value,
    pub labels: serde_json::Value,
    pub taints: serde_json::Value,
    /// Present only when the caller supplied `remoteAccess`.
    pub remote_access: Option<serde_json::Value>,
    /// Present only when the caller supplied `launchTemplate`.
    pub launch_template: Option<serde_json::Value>,
    /// The synthetic Auto Scaling group name reported under `resources`.
    pub asg_name: String,
    pub tags: TagMap,
    /// Per-nodegroup updates keyed by update id, disambiguated from cluster
    /// updates by the `nodegroupName` query param on Describe/ListUpdates.
    pub updates: BTreeMap<String, Update>,
}

/// An EKS add-on, a sub-resource of a cluster (e.g. `vpc-cni`, `coredns`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Addon {
    pub name: String,
    pub arn: String,
    pub cluster_name: String,
    pub addon_version: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    /// Present only when the caller supplied `serviceAccountRoleArn`.
    pub service_account_role_arn: Option<String>,
    /// Present only when the caller supplied `configurationValues`.
    pub configuration_values: Option<String>,
    /// The custom install namespace, if the caller supplied `namespaceConfig`.
    pub namespace: Option<String>,
    /// Pod identity association ARNs (one per requested association), echoed
    /// back as the `podIdentityAssociations` StringList on describe.
    pub pod_identity_associations: Vec<String>,
    pub tags: TagMap,
    /// Per-addon updates keyed by update id, disambiguated from cluster and
    /// node-group updates by the `addonName` query param on Describe/ListUpdates.
    pub updates: BTreeMap<String, Update>,
}

/// An access policy associated to an access entry via `AssociateAccessPolicy`.
/// `access_scope` is stored as the `{type, namespaces}` JSON object echoed back
/// on describe/list so the round-trip is faithful.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssociatedPolicy {
    pub policy_arn: String,
    pub access_scope: serde_json::Value,
    pub associated_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
}

/// An EKS access entry, a sub-resource of a cluster keyed by principal ARN.
/// The access policies associated to the entry are a nested list.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessEntry {
    pub principal_arn: String,
    pub cluster_name: String,
    pub arn: String,
    pub kubernetes_groups: Vec<String>,
    pub username: String,
    pub type_: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub tags: TagMap,
    /// Access policies associated to this entry, keyed by policy ARN order.
    #[serde(default)]
    pub associated_policies: Vec<AssociatedPolicy>,
}

/// A Fargate profile, a sub-resource of a cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FargateProfile {
    pub name: String,
    pub arn: String,
    pub cluster_name: String,
    pub pod_execution_role_arn: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub subnets: serde_json::Value,
    pub selectors: serde_json::Value,
    pub tags: TagMap,
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
    /// Node groups nested as cluster name -> nodegroup name -> node group.
    /// Nesting (rather than a `(String, String)` tuple key) keeps the map
    /// JSON-serializable through the snapshot and scopes node groups per
    /// cluster for natural cross-cluster isolation.
    #[serde(default)]
    pub nodegroups: BTreeMap<String, BTreeMap<String, Nodegroup>>,
    /// Fargate profiles nested as cluster name -> profile name -> profile.
    #[serde(default)]
    pub fargate_profiles: BTreeMap<String, BTreeMap<String, FargateProfile>>,
    /// Add-ons nested as cluster name -> addon name -> addon.
    #[serde(default)]
    pub addons: BTreeMap<String, BTreeMap<String, Addon>>,
    /// Access entries nested as cluster name -> principal ARN -> access entry.
    #[serde(default)]
    pub access_entries: BTreeMap<String, BTreeMap<String, AccessEntry>>,
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

/// Build the ARN for a node group. AWS appends a random UUID segment after
/// `nodegroup/{cluster}/{name}` so each node group's ARN is unique even after
/// a delete/recreate.
pub fn nodegroup_arn(
    region: &str,
    account_id: &str,
    cluster: &str,
    name: &str,
    id: &str,
) -> String {
    format!("arn:aws:eks:{region}:{account_id}:nodegroup/{cluster}/{name}/{id}")
}

/// Build the ARN for a Fargate profile.
pub fn fargate_profile_arn(
    region: &str,
    account_id: &str,
    cluster: &str,
    name: &str,
    id: &str,
) -> String {
    format!("arn:aws:eks:{region}:{account_id}:fargateprofile/{cluster}/{name}/{id}")
}

/// Build the ARN for an add-on. AWS appends a random UUID segment after
/// `addon/{cluster}/{name}` so each add-on's ARN is unique across recreate.
pub fn addon_arn(region: &str, account_id: &str, cluster: &str, name: &str, id: &str) -> String {
    format!("arn:aws:eks:{region}:{account_id}:addon/{cluster}/{name}/{id}")
}

/// Build the ARN for an access entry. AWS's access-entry ARN embeds the
/// principal type (`role`/`user`), the account, the principal's resource name,
/// and a random UUID:
/// `arn:aws:eks:{region}:{account}:access-entry/{cluster}/{type}/{account}/{name}/{uuid}`.
pub fn access_entry_arn(
    region: &str,
    account_id: &str,
    cluster: &str,
    principal_type: &str,
    principal_name: &str,
    id: &str,
) -> String {
    format!(
        "arn:aws:eks:{region}:{account_id}:access-entry/{cluster}/{principal_type}/{account_id}/{principal_name}/{id}"
    )
}

/// Build the ARN for a pod identity association attached to an add-on.
pub fn pod_identity_association_arn(
    region: &str,
    account_id: &str,
    cluster: &str,
    id: &str,
) -> String {
    format!("arn:aws:eks:{region}:{account_id}:podidentityassociation/{cluster}/a-{id}")
}

pub type SharedEksState = Arc<RwLock<MultiAccountState<EksState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct EksSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<EksState>,
}
