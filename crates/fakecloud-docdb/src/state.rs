//! Account-partitioned, serializable state for the DocumentDB control plane.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_aws::arn::Arn;

/// An RDS-namespace ARN (`arn:<partition>:rds:<region>:<account>:<kind>:<id>`)
/// in the region's partition.
pub fn rds_arn(region: &str, account: &str, kind: &str, id: &str) -> String {
    Arn::regional("rds", region, account, &format!("{kind}:{id}")).to_string()
}

/// A global cluster's region-less ARN, in the partition of `region`.
pub fn global_cluster_arn(region: &str, account: &str, id: &str) -> String {
    Arn::global_in(region, "rds", account, &format!("global-cluster:{id}")).to_string()
}

/// Shared, account-partitioned DocumentDB state handle.
pub type SharedDocDbState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<DocDbState>>>;

/// Schema version for the on-disk persistence snapshot. Bump on any
/// breaking change to the serialized shape.
pub const DOCDB_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Persistence envelope written to `<data-dir>/docdb/snapshot.json`.
#[derive(Serialize, Deserialize)]
pub struct DocDbSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<DocDbState>>,
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

/// Serverless v2 capacity range of a cluster
/// (`ServerlessV2ScalingConfiguration`).
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub struct ServerlessV2Scaling {
    pub min_capacity: Option<f64>,
    pub max_capacity: Option<f64>,
}

/// One Amazon DocumentDB Serverless v2 platform version
/// (`ServerlessV2PlatformVersionInfo`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ServerlessV2PlatformVersion {
    pub version: &'static str,
    pub engine: &'static str,
    pub description: &'static str,
    /// `enabled` (in use) or `disabled` (no longer in use).
    pub status: &'static str,
    pub is_default: bool,
    /// DCU bounds the platform supports (`ServerlessV2FeaturesSupport`).
    pub min_capacity: f64,
    pub max_capacity: f64,
}

/// The serverless platform versions the emulator offers. New serverless
/// clusters land on the engine's default (enabled) version, which is what
/// `DBCluster.ServerlessV2PlatformVersion` reports.
pub const SERVERLESS_V2_PLATFORM_VERSIONS: &[ServerlessV2PlatformVersion] = &[
    ServerlessV2PlatformVersion {
        version: "1",
        engine: "docdb",
        description: "Amazon DocumentDB Serverless platform version 1",
        status: "disabled",
        is_default: false,
        min_capacity: 0.5,
        max_capacity: 128.0,
    },
    ServerlessV2PlatformVersion {
        version: "2",
        engine: "docdb",
        description: "Amazon DocumentDB Serverless platform version 2",
        status: "enabled",
        is_default: true,
        min_capacity: 0.5,
        max_capacity: 256.0,
    },
];

/// The default serverless platform version for `engine`, if it has one.
pub fn default_serverless_v2_platform_version(
    engine: &str,
) -> Option<&'static ServerlessV2PlatformVersion> {
    SERVERLESS_V2_PLATFORM_VERSIONS
        .iter()
        .find(|p| p.engine == engine && p.is_default)
}

/// A DocumentDB DB cluster (control-plane record).
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
    /// Copy the cluster's tags onto snapshots taken of it.
    #[serde(default)]
    pub copy_tags_to_snapshot: bool,
    pub backup_retention_period: i32,
    pub preferred_backup_window: String,
    pub preferred_maintenance_window: String,
    pub storage_type: String,
    #[serde(default)]
    pub serverless_v2_scaling: Option<ServerlessV2Scaling>,
    pub availability_zones: Vec<String>,
    pub vpc_security_group_ids: Vec<String>,
    pub enabled_cloudwatch_logs_exports: Vec<String>,
    pub members: Vec<ClusterMember>,
    pub cluster_create_time: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

/// A DocumentDB DB instance attached to a cluster (control-plane record).
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

/// All DocumentDB state for a single AWS account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DocDbState {
    pub account_id: String,
    pub region: String,
    #[serde(default)]
    pub clusters: BTreeMap<String, DbCluster>,
    #[serde(default)]
    pub instances: BTreeMap<String, DbInstance>,
    #[serde(default)]
    pub cluster_snapshots: BTreeMap<String, DbClusterSnapshot>,
    #[serde(default)]
    pub cluster_parameter_groups: BTreeMap<String, DbClusterParameterGroup>,
    #[serde(default)]
    pub subnet_groups: BTreeMap<String, DbSubnetGroup>,
    #[serde(default)]
    pub global_clusters: BTreeMap<String, GlobalCluster>,
    #[serde(default)]
    pub event_subscriptions: BTreeMap<String, EventSubscription>,
}

impl DocDbState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            clusters: BTreeMap::new(),
            instances: BTreeMap::new(),
            cluster_snapshots: BTreeMap::new(),
            cluster_parameter_groups: BTreeMap::new(),
            subnet_groups: BTreeMap::new(),
            global_clusters: BTreeMap::new(),
            event_subscriptions: BTreeMap::new(),
        }
    }
}

impl fakecloud_core::multi_account::AccountState for DocDbState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}
