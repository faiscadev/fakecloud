//! Account-partitioned, serializable state for the Neptune control plane.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Shared, account-partitioned Neptune state handle.
pub type SharedNeptuneState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<NeptuneState>>>;

/// Schema version for the on-disk persistence snapshot. Bump on any
/// breaking change to the serialized shape.
pub const NEPTUNE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Persistence envelope written to `<data-dir>/neptune/snapshot.json`.
#[derive(Serialize, Deserialize)]
pub struct NeptuneSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<NeptuneState>>,
}

/// A resource tag.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// One member instance of a DB cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterMember {
    pub db_instance_identifier: String,
    pub is_writer: bool,
    pub promotion_tier: i32,
}

/// An IAM role associated with a cluster (AddRoleToDBCluster).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterRole {
    pub role_arn: String,
    pub status: String,
    pub feature_name: Option<String>,
}

/// A Neptune DB cluster (control-plane record).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbCluster {
    pub db_cluster_identifier: String,
    pub db_cluster_arn: String,
    pub db_cluster_resource_id: String,
    pub status: String,
    pub engine: String,
    pub engine_version: String,
    pub port: i32,
    pub master_username: String,
    pub endpoint: String,
    pub reader_endpoint: String,
    pub hosted_zone_id: String,
    pub db_subnet_group: String,
    pub db_cluster_parameter_group: String,
    pub storage_encrypted: bool,
    pub kms_key_id: Option<String>,
    pub deletion_protection: bool,
    pub iam_database_authentication_enabled: bool,
    pub backup_retention_period: i32,
    pub preferred_backup_window: String,
    pub preferred_maintenance_window: String,
    pub storage_type: String,
    pub availability_zones: Vec<String>,
    pub vpc_security_group_ids: Vec<String>,
    pub enabled_cloudwatch_logs_exports: Vec<String>,
    pub members: Vec<ClusterMember>,
    pub associated_roles: Vec<ClusterRole>,
    pub cluster_create_time: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

/// A Neptune DB instance attached to a cluster (control-plane record).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbInstance {
    pub db_instance_identifier: String,
    pub db_instance_arn: String,
    pub db_instance_class: String,
    pub engine: String,
    pub engine_version: String,
    pub status: String,
    pub db_cluster_identifier: String,
    pub endpoint_address: String,
    pub port: i32,
    pub availability_zone: String,
    pub publicly_accessible: bool,
    pub auto_minor_version_upgrade: bool,
    pub promotion_tier: i32,
    pub dbi_resource_id: String,
    pub ca_certificate_identifier: String,
    pub preferred_backup_window: String,
    pub preferred_maintenance_window: String,
    pub backup_retention_period: i32,
    pub storage_encrypted: bool,
    pub kms_key_id: Option<String>,
    pub db_subnet_group: String,
    pub enabled_cloudwatch_logs_exports: Vec<String>,
    pub instance_create_time: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

/// A custom or reader cluster endpoint (Neptune-specific).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbClusterEndpoint {
    pub db_cluster_endpoint_identifier: String,
    pub db_cluster_identifier: String,
    pub db_cluster_endpoint_resource_identifier: String,
    pub endpoint: String,
    pub status: String,
    pub endpoint_type: String,
    pub custom_endpoint_type: String,
    pub static_members: Vec<String>,
    pub excluded_members: Vec<String>,
    pub db_cluster_endpoint_arn: String,
    pub tags: Vec<Tag>,
}

/// A DB cluster snapshot capturing a point-in-time cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbClusterSnapshot {
    pub db_cluster_snapshot_identifier: String,
    pub db_cluster_snapshot_arn: String,
    pub db_cluster_identifier: String,
    pub status: String,
    pub engine: String,
    pub engine_version: String,
    pub port: i32,
    pub master_username: String,
    pub snapshot_type: String,
    pub storage_encrypted: bool,
    pub kms_key_id: Option<String>,
    pub percent_progress: i32,
    pub vpc_id: String,
    pub storage_type: String,
    pub availability_zones: Vec<String>,
    pub cluster_create_time: DateTime<Utc>,
    pub snapshot_create_time: DateTime<Utc>,
    pub source_db_cluster_snapshot_arn: Option<String>,
    /// Account ids the snapshot is shared with (ModifyDBClusterSnapshotAttribute `restore`).
    pub restore_attribute: Vec<String>,
    pub tags: Vec<Tag>,
}

/// A user-set parameter value plus its apply method.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParameterValue {
    pub value: String,
    pub apply_method: String,
}

/// A DB cluster parameter group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbClusterParameterGroup {
    pub db_cluster_parameter_group_name: String,
    pub db_cluster_parameter_group_arn: String,
    pub db_parameter_group_family: String,
    pub description: String,
    pub parameters: BTreeMap<String, ParameterValue>,
    pub tags: Vec<Tag>,
}

/// A DB (instance) parameter group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbParameterGroup {
    pub db_parameter_group_name: String,
    pub db_parameter_group_arn: String,
    pub db_parameter_group_family: String,
    pub description: String,
    pub parameters: BTreeMap<String, ParameterValue>,
    pub tags: Vec<Tag>,
}

/// One subnet belonging to a DB subnet group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Subnet {
    pub subnet_identifier: String,
    pub availability_zone: String,
    pub status: String,
}

/// A DB subnet group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DbSubnetGroup {
    pub db_subnet_group_name: String,
    pub db_subnet_group_arn: String,
    pub db_subnet_group_description: String,
    pub vpc_id: String,
    pub subnet_group_status: String,
    pub subnets: Vec<Subnet>,
    pub tags: Vec<Tag>,
}

/// One member cluster of a global cluster.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalClusterMember {
    pub db_cluster_arn: String,
    pub is_writer: bool,
}

/// A global cluster spanning regions.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GlobalCluster {
    pub global_cluster_identifier: String,
    pub global_cluster_arn: String,
    pub global_cluster_resource_id: String,
    pub status: String,
    pub engine: String,
    pub engine_version: String,
    pub database_name: Option<String>,
    pub storage_encrypted: bool,
    pub deletion_protection: bool,
    pub members: Vec<GlobalClusterMember>,
    pub tags: Vec<Tag>,
}

/// An RDS-event-style subscription delivering notifications to SNS.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventSubscription {
    pub subscription_name: String,
    pub event_subscription_arn: String,
    pub customer_aws_id: String,
    pub sns_topic_arn: String,
    pub status: String,
    pub subscription_creation_time: String,
    pub source_type: Option<String>,
    pub source_ids: Vec<String>,
    pub event_categories: Vec<String>,
    pub enabled: bool,
    pub tags: Vec<Tag>,
}

/// All Neptune state for a single AWS account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NeptuneState {
    pub account_id: String,
    pub region: String,
    #[serde(default)]
    pub clusters: BTreeMap<String, DbCluster>,
    #[serde(default)]
    pub instances: BTreeMap<String, DbInstance>,
    #[serde(default)]
    pub cluster_endpoints: BTreeMap<String, DbClusterEndpoint>,
    #[serde(default)]
    pub cluster_snapshots: BTreeMap<String, DbClusterSnapshot>,
    #[serde(default)]
    pub cluster_parameter_groups: BTreeMap<String, DbClusterParameterGroup>,
    #[serde(default)]
    pub parameter_groups: BTreeMap<String, DbParameterGroup>,
    #[serde(default)]
    pub subnet_groups: BTreeMap<String, DbSubnetGroup>,
    #[serde(default)]
    pub global_clusters: BTreeMap<String, GlobalCluster>,
    #[serde(default)]
    pub event_subscriptions: BTreeMap<String, EventSubscription>,
}

impl NeptuneState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            clusters: BTreeMap::new(),
            instances: BTreeMap::new(),
            cluster_endpoints: BTreeMap::new(),
            cluster_snapshots: BTreeMap::new(),
            cluster_parameter_groups: BTreeMap::new(),
            parameter_groups: BTreeMap::new(),
            subnet_groups: BTreeMap::new(),
            global_clusters: BTreeMap::new(),
            event_subscriptions: BTreeMap::new(),
        }
    }
}

impl fakecloud_core::multi_account::AccountState for NeptuneState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}
