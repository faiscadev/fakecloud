//! Account-partitioned, serializable state for AWS EKS.
//!
//! Models the EKS cluster control plane: clusters (with their VPC config,
//! logging, Kubernetes network config, and tags) plus the per-cluster `Update`
//! objects produced by `UpdateClusterConfig` / `UpdateClusterVersion` and read
//! back through `DescribeUpdate` / `ListUpdates`; managed node groups and
//! Fargate profiles (both cluster sub-resources, each with their own status
//! lifecycle, and node groups with their own update history), add-ons, access
//! entries, OIDC identity-provider configs, and Pod Identity associations (all
//! cluster sub-resources).

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
    /// Present only when the caller supplied `nodeRepairConfig`.
    #[serde(default)]
    pub node_repair_config: Option<serde_json::Value>,
    /// Present only when the caller supplied `warmPoolConfig`.
    #[serde(default)]
    pub warm_pool_config: Option<serde_json::Value>,
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

/// An OIDC identity-provider config associated to a cluster via
/// `AssociateIdentityProviderConfig`, keyed by `identityProviderConfigName`.
/// Optional OIDC members are stored as `Option`s so describe only echoes the
/// ones the caller supplied, and `required_claims` keeps the caller's map
/// verbatim for a faithful round-trip.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityProviderConfig {
    pub name: String,
    pub arn: String,
    pub cluster_name: String,
    pub issuer_url: String,
    pub client_id: String,
    pub username_claim: Option<String>,
    pub username_prefix: Option<String>,
    pub groups_claim: Option<String>,
    pub groups_prefix: Option<String>,
    pub required_claims: serde_json::Value,
    pub status: String,
    pub tags: TagMap,
}

/// An EKS Pod Identity association, a sub-resource of a cluster keyed by its
/// generated `associationId`. Binds a Kubernetes service account (in a
/// namespace) to an IAM role.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PodIdentityAssociation {
    pub cluster_name: String,
    pub namespace: String,
    pub service_account: String,
    pub role_arn: String,
    pub association_arn: String,
    pub association_id: String,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub disable_session_tags: bool,
    /// Present only when the caller supplied `targetRoleArn` (cross-account).
    pub target_role_arn: Option<String>,
    /// Generated for the target role's trust policy when `targetRoleArn` is set.
    pub external_id: Option<String>,
    pub tags: TagMap,
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
    /// The `ConnectorConfigResponse`-shaped object present only on clusters
    /// created via `RegisterCluster` (i.e. connected/external clusters).
    #[serde(default)]
    pub connector_config: Option<serde_json::Value>,
    /// The `EncryptionConfigList` applied via `AssociateEncryptionConfig`,
    /// echoed back on describe.
    #[serde(default)]
    pub encryption_config: Option<serde_json::Value>,
    /// The `AccessConfigResponse`-shaped object (`{authenticationMode}`) echoed
    /// back on every describe. Defaults to `CONFIG_MAP` when the caller omits
    /// `accessConfig` at create time, mirroring the real API default.
    #[serde(default = "default_access_config")]
    pub access_config: serde_json::Value,
    /// The `UpgradePolicyResponse`-shaped object (`{supportType}`) echoed back
    /// on every describe. Defaults to `EXTENDED` (extended support enabled) when
    /// the caller omits `upgradePolicy` at create time.
    #[serde(default = "default_upgrade_policy")]
    pub upgrade_policy: serde_json::Value,
    /// The `ComputeConfigResponse` (EKS Auto Mode) echoed back when the caller
    /// supplied `computeConfig` at create or via `UpdateClusterConfig`.
    #[serde(default)]
    pub compute_config: Option<serde_json::Value>,
    /// The `StorageConfigResponse` (EKS Auto Mode block storage) echoed back
    /// when supplied.
    #[serde(default)]
    pub storage_config: Option<serde_json::Value>,
    /// The `ZonalShiftConfigResponse` echoed back when supplied.
    #[serde(default)]
    pub zonal_shift_config: Option<serde_json::Value>,
    /// The `RemoteNetworkConfigResponse` (hybrid nodes) echoed back when supplied.
    #[serde(default)]
    pub remote_network_config: Option<serde_json::Value>,
    /// The `ControlPlaneScalingConfigResponse` echoed back when supplied.
    #[serde(default)]
    pub control_plane_scaling_config: Option<serde_json::Value>,
    /// Whether deletion protection is enabled, echoed back when set.
    #[serde(default)]
    pub deletion_protection: Option<bool>,
}

/// The `accessConfig` a cluster reports when none was supplied at create time:
/// `CONFIG_MAP` authentication (the backward-compatible default).
pub fn default_access_config() -> serde_json::Value {
    serde_json::json!({ "authenticationMode": "CONFIG_MAP" })
}

/// The `upgradePolicy` a cluster reports when none was supplied at create time:
/// `EXTENDED` support (extended support enabled by default).
pub fn default_upgrade_policy() -> serde_json::Value {
    serde_json::json!({ "supportType": "EXTENDED" })
}

/// An upgrade-readiness/misconfiguration Insight that EKS auto-generates for a
/// cluster, a cluster sub-resource keyed by its generated id. Read back through
/// `ListInsights` / `DescribeInsight`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Insight {
    pub id: String,
    pub name: String,
    pub category: String,
    pub kubernetes_version: String,
    pub description: String,
    pub recommendation: String,
    /// The `InsightStatusValue` (PASSING / WARNING / ERROR / UNKNOWN).
    pub status: String,
    pub reason: String,
    pub last_refresh_time: DateTime<Utc>,
    pub last_transition_time: DateTime<Utc>,
}

/// The state of the most recent `StartInsightsRefresh` for a cluster, read back
/// through `DescribeInsightsRefresh`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightsRefresh {
    /// The `InsightsRefreshStatus` (IN_PROGRESS / FAILED / COMPLETED).
    pub status: String,
    pub started_at: DateTime<Utc>,
    pub ended_at: Option<DateTime<Utc>>,
}

/// An EKS cluster capability (e.g. an ACK / KRO / ArgoCD controller), a
/// sub-resource of a cluster keyed by `capabilityName`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Capability {
    pub name: String,
    pub arn: String,
    pub cluster_name: String,
    /// The `CapabilityType` (ACK / KRO / ARGOCD).
    pub type_: String,
    pub role_arn: String,
    pub status: String,
    pub version: String,
    /// The `CapabilityConfigurationResponse`-shaped object, when supplied.
    pub configuration: Option<serde_json::Value>,
    pub tags: TagMap,
    pub created_at: DateTime<Utc>,
    pub modified_at: DateTime<Utc>,
    pub delete_propagation_policy: Option<String>,
}

/// An EKS Anywhere subscription, an account-scoped (not cluster-scoped)
/// resource keyed by its generated id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EksAnywhereSubscription {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
    pub effective_date: DateTime<Utc>,
    pub expiration_date: DateTime<Utc>,
    pub license_quantity: i64,
    /// The `EksAnywhereSubscriptionLicenseType` (only `Cluster` today).
    pub license_type: String,
    pub term_duration: i64,
    /// The `EksAnywhereSubscriptionTermUnit` (only `MONTHS` today).
    pub term_unit: String,
    pub status: String,
    pub auto_renew: bool,
    pub tags: TagMap,
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
    /// Identity-provider configs nested as cluster name -> config name -> config.
    #[serde(default)]
    pub identity_provider_configs: BTreeMap<String, BTreeMap<String, IdentityProviderConfig>>,
    /// Pod identity associations nested as cluster name -> associationId -> assoc.
    #[serde(default)]
    pub pod_identity_associations: BTreeMap<String, BTreeMap<String, PodIdentityAssociation>>,
    /// Auto-generated Insights nested as cluster name -> insight id -> insight.
    #[serde(default)]
    pub insights: BTreeMap<String, BTreeMap<String, Insight>>,
    /// The most recent insights-refresh state, keyed by cluster name.
    #[serde(default)]
    pub insights_refresh: BTreeMap<String, InsightsRefresh>,
    /// Capabilities nested as cluster name -> capability name -> capability.
    #[serde(default)]
    pub capabilities: BTreeMap<String, BTreeMap<String, Capability>>,
    /// EKS Anywhere subscriptions (account-scoped) keyed by subscription id.
    #[serde(default)]
    pub eks_anywhere_subscriptions: BTreeMap<String, EksAnywhereSubscription>,
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

/// Build the ARN for an OIDC identity-provider config. AWS embeds the config
/// type (`oidc`), the config name, and a random UUID.
pub fn identity_provider_config_arn(
    region: &str,
    account_id: &str,
    cluster: &str,
    name: &str,
    id: &str,
) -> String {
    format!("arn:aws:eks:{region}:{account_id}:identityproviderconfig/{cluster}/oidc/{name}/{id}")
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

/// Build the ARN for a cluster capability. AWS appends a random UUID segment
/// after `capability/{cluster}/{name}` so each capability's ARN is unique.
pub fn capability_arn(
    region: &str,
    account_id: &str,
    cluster: &str,
    name: &str,
    id: &str,
) -> String {
    format!("arn:aws:eks:{region}:{account_id}:capability/{cluster}/{name}/{id}")
}

/// Build the ARN for an EKS Anywhere subscription.
pub fn eks_anywhere_subscription_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:eks:{region}:{account_id}:eks-anywhere-subscription/{id}")
}

pub type SharedEksState = Arc<RwLock<MultiAccountState<EksState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct EksSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<EksState>,
}
