use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use fakecloud_aws::arn::Arn;
use parking_lot::RwLock;

pub type SharedElastiCacheState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<ElastiCacheState>>>;

impl fakecloud_core::multi_account::AccountState for ElastiCacheState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEngineVersion {
    pub engine: String,
    pub engine_version: String,
    pub cache_parameter_group_family: String,
    pub cache_engine_description: String,
    pub cache_engine_version_description: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheParameterGroup {
    pub cache_parameter_group_name: String,
    pub cache_parameter_group_family: String,
    pub description: String,
    pub is_global: bool,
    pub arn: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EngineDefaultParameter {
    pub parameter_name: String,
    pub parameter_value: String,
    pub description: String,
    pub source: String,
    pub data_type: String,
    pub allowed_values: String,
    pub is_modifiable: bool,
    pub minimum_engine_version: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheSubnetGroup {
    pub cache_subnet_group_name: String,
    pub cache_subnet_group_description: String,
    pub vpc_id: String,
    pub subnet_ids: Vec<String>,
    pub arn: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RecurringCharge {
    pub recurring_charge_amount: f64,
    pub recurring_charge_frequency: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReservedCacheNode {
    pub reserved_cache_node_id: String,
    pub reserved_cache_nodes_offering_id: String,
    pub cache_node_type: String,
    pub start_time: String,
    pub duration: i32,
    pub fixed_price: f64,
    pub usage_price: f64,
    pub cache_node_count: i32,
    pub product_description: String,
    pub offering_type: String,
    pub state: String,
    pub recurring_charges: Vec<RecurringCharge>,
    pub reservation_arn: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReservedCacheNodesOffering {
    pub reserved_cache_nodes_offering_id: String,
    pub cache_node_type: String,
    pub duration: i32,
    pub fixed_price: f64,
    pub usage_price: f64,
    pub product_description: String,
    pub offering_type: String,
    pub recurring_charges: Vec<RecurringCharge>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheCluster {
    pub cache_cluster_id: String,
    pub cache_node_type: String,
    pub engine: String,
    pub engine_version: String,
    pub cache_cluster_status: String,
    pub num_cache_nodes: i32,
    pub preferred_availability_zone: String,
    pub cache_subnet_group_name: Option<String>,
    pub auto_minor_version_upgrade: bool,
    pub arn: String,
    pub created_at: String,
    pub endpoint_address: String,
    pub endpoint_port: u16,
    pub container_id: String,
    pub host_port: u16,
    pub replication_group_id: Option<String>,
    /// `CacheParameterGroup.CacheParameterGroupName` — group bound at
    /// create / modify time. Real AWS always emits this membership;
    /// fakecloud previously omitted the element entirely.
    #[serde(default)]
    pub cache_parameter_group_name: Option<String>,
    /// VPC security group ids attached at create time. Echoed via
    /// `<SecurityGroups>` for parity with AWS DescribeCacheClusters.
    #[serde(default)]
    pub security_group_ids: Vec<String>,
    /// `LogDeliveryConfigurations` — destinations + log types attached
    /// to the cluster. Round-tripped only.
    #[serde(default)]
    pub log_delivery_configurations: Vec<LogDeliveryConfiguration>,
    /// In-transit encryption flag. Real AWS always emits this; defaults
    /// to `false` for unencrypted clusters.
    #[serde(default)]
    pub transit_encryption_enabled: bool,
    /// At-rest encryption flag.
    #[serde(default)]
    pub at_rest_encryption_enabled: bool,
    /// `AuthTokenEnabled` — true when an AUTH token was supplied.
    #[serde(default)]
    pub auth_token_enabled: bool,
    /// Configured `Port` from the create request. Stored separately
    /// from `endpoint_port`/`host_port` so the engine default
    /// (6379 redis / 11211 memcached) round-trips even when the
    /// container listens elsewhere.
    #[serde(default)]
    pub port: u16,
    /// `PreferredMaintenanceWindow` from the request, e.g. `sun:23:00-mon:01:30`.
    #[serde(default)]
    pub preferred_maintenance_window: Option<String>,
    /// `PreferredAvailabilityZones.member.N` — populated for memcached clusters
    /// pinning each node to a specific AZ.
    #[serde(default)]
    pub preferred_availability_zones: Vec<String>,
    /// `NotificationTopicArn` for cluster events.
    #[serde(default)]
    pub notification_topic_arn: Option<String>,
    /// Legacy EC2-Classic security group names.
    #[serde(default)]
    pub cache_security_group_names: Vec<String>,
    /// `SnapshotArns.member.N` — RDB seed snapshot S3 ARNs (redis only).
    #[serde(default)]
    pub snapshot_arns: Vec<String>,
    /// `SnapshotName` — replication-group / cluster snapshot to seed from.
    #[serde(default)]
    pub snapshot_name: Option<String>,
    /// `SnapshotRetentionLimit` — daily snapshots to keep.
    #[serde(default)]
    pub snapshot_retention_limit: i32,
    /// `SnapshotWindow` — time range when automatic snapshots run.
    #[serde(default)]
    pub snapshot_window: Option<String>,
    /// `OutpostMode` — `single-outpost` or `cross-outpost`.
    #[serde(default)]
    pub outpost_mode: Option<String>,
    /// `PreferredOutpostArn` — ARN of the AWS Outpost the cluster pins to.
    #[serde(default)]
    pub preferred_outpost_arn: Option<String>,
    /// `NetworkType` — `ipv4`, `ipv6`, or `dual_stack`.
    #[serde(default)]
    pub network_type: Option<String>,
    /// `IpDiscovery` — `ipv4` or `ipv6`.
    #[serde(default)]
    pub ip_discovery: Option<String>,
    /// `AZMode` — `single-az` or `cross-az` (memcached multi-node).
    #[serde(default)]
    pub az_mode: Option<String>,
    /// Raw AUTH token. Stored verbatim so a future modify can
    /// compare/rotate; never echoed back in describe XML.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// `KmsKeyId` — at-rest encryption key passed at create time.
    /// AWS doesn't echo this on `DescribeCacheClusters`, but real
    /// SDKs (terraform plan diff, compliance scans) read it from
    /// state, so we round-trip it on the struct.
    #[serde(default)]
    pub kms_key_id: Option<String>,
    /// `TransitEncryptionMode` — `preferred` or `required`. Round-tripped
    /// onto `DescribeCacheClusters` exactly like AWS does.
    #[serde(default)]
    pub transit_encryption_mode: Option<String>,
    /// `DataTieringEnabled` toggle (Redis r6gd only). Stored verbatim
    /// so terraform plan diff and DescribeCacheClusters round-trip.
    #[serde(default)]
    pub data_tiering_enabled: Option<bool>,
    /// `ClusterMode` input — `compatible` / `enabled` / `disabled`.
    /// Stored separately from `cluster_enabled` because the input
    /// allows the tri-state `compatible` value.
    #[serde(default)]
    pub cluster_mode: Option<String>,
    /// `PreferredOutpostArns.member.N` — cross-outpost cluster placement.
    /// Round-tripped from input; not echoed by AWS but kept on the struct
    /// so the original request shape is preserved.
    #[serde(default)]
    pub preferred_outpost_arns: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReplicationGroup {
    pub replication_group_id: String,
    pub description: String,
    pub global_replication_group_id: Option<String>,
    pub global_replication_group_role: Option<String>,
    pub status: String,
    pub cache_node_type: String,
    pub engine: String,
    pub engine_version: String,
    pub num_cache_clusters: i32,
    pub automatic_failover_enabled: bool,
    pub endpoint_address: String,
    pub endpoint_port: u16,
    pub arn: String,
    pub created_at: String,
    pub container_id: String,
    pub host_port: u16,
    pub member_clusters: Vec<String>,
    pub snapshot_retention_limit: i32,
    pub snapshot_window: String,
    /// Stored at create / modify time so DescribeReplicationGroups returns
    /// the actual configuration instead of canned defaults. AWS always
    /// emits these flags; SDKs that read them (terraform plan diff,
    /// compliance checks) saw stale `false` for everyone.
    #[serde(default)]
    pub transit_encryption_enabled: bool,
    #[serde(default)]
    pub at_rest_encryption_enabled: bool,
    #[serde(default)]
    pub cluster_enabled: bool,
    #[serde(default)]
    pub kms_key_id: Option<String>,
    #[serde(default)]
    pub auth_token_enabled: bool,
    #[serde(default)]
    pub user_group_ids: Vec<String>,
    #[serde(default)]
    pub multi_az_enabled: bool,
    #[serde(default)]
    pub log_delivery_configurations: Vec<LogDeliveryConfiguration>,
    #[serde(default)]
    pub data_tiering: Option<String>,
    #[serde(default)]
    pub ip_discovery: Option<String>,
    #[serde(default)]
    pub network_type: Option<String>,
    #[serde(default)]
    pub transit_encryption_mode: Option<String>,
    #[serde(default)]
    pub num_node_groups: i32,
    #[serde(default)]
    pub configuration_endpoint_address: Option<String>,
    #[serde(default)]
    pub configuration_endpoint_port: Option<u16>,
    #[serde(default)]
    pub replicas_per_node_group: Option<i32>,
    /// Raw AUTH token. Stored verbatim so a future `ModifyReplicationGroup`
    /// can compare/rotate it; never echoed back in describe XML.
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Configured `Port` from the create request. AWS returns this on
    /// `<NodeGroups>.<PrimaryEndpoint>.<Port>` once the cluster is real;
    /// fakecloud uses the real container host port for connectivity but
    /// echoes the requested value through pending modifications.
    #[serde(default)]
    pub port: u16,
    /// SNS topic ARN for replication-group events.
    #[serde(default)]
    pub notification_topic_arn: Option<String>,
    /// `ClusterMode` input — distinct from the derived `cluster_enabled`
    /// flag. Valid values: `enabled` / `disabled` / `compatible`.
    #[serde(default)]
    pub cluster_mode: Option<String>,
    /// `DataTieringEnabled` boolean as supplied by the request. The
    /// existing `data_tiering` string field is the response-shape
    /// `enabled`/`disabled` projection.
    #[serde(default)]
    pub data_tiering_enabled: Option<bool>,
    /// `NotificationTopicStatus` from the most recent ModifyReplicationGroup
    /// call. Defaults to `active` when emitting describe XML if unset.
    #[serde(default)]
    pub notification_topic_status: Option<String>,
    /// `CacheParameterGroupName` from the create / modify request.
    /// Echoed via `<CacheParameterGroup>` in describe XML.
    #[serde(default)]
    pub cache_parameter_group_name: Option<String>,
    /// `CacheSubnetGroupName` from the create request. Persisted so
    /// `ModifyReplicationGroup` and tooling like terraform plan diff
    /// can recover the original placement.
    #[serde(default)]
    pub cache_subnet_group_name: Option<String>,
    /// VPC security group ids attached at create / modify time. AWS
    /// echoes these via `<SecurityGroups>` once the underlying clusters
    /// land, so we persist them on the replication group as well.
    #[serde(default)]
    pub security_group_ids: Vec<String>,
    /// `PreferredMaintenanceWindow` from the request, e.g.
    /// `sun:23:00-mon:01:30`. Round-tripped onto member clusters and
    /// echoed where AWS does.
    #[serde(default)]
    pub preferred_maintenance_window: Option<String>,
    /// `SnapshotName` — replication-group snapshot used to seed the
    /// new group. Stored verbatim for restore lineage; not echoed on
    /// describe since AWS only emits `SnapshottingClusterId`.
    #[serde(default)]
    pub snapshot_name: Option<String>,
    /// `SnapshotArns.member.N` — RDB seed snapshot S3 ARNs (redis only).
    #[serde(default)]
    pub snapshot_arns: Vec<String>,
    /// `AutoMinorVersionUpgrade` toggle. AWS always emits this on the
    /// describe response (default `true`) — tracked so ModifyReplicationGroup
    /// can flip it.
    #[serde(default = "default_auto_minor_version_upgrade")]
    pub auto_minor_version_upgrade: bool,
}

fn default_auto_minor_version_upgrade() -> bool {
    true
}

/// AWS's LogDeliveryConfiguration shape, retained verbatim so we can
/// echo the exact request back. Stored as raw fields for both
/// CloudWatch + Firehose destinations.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct LogDeliveryConfiguration {
    pub log_type: String,
    pub destination_type: String,
    pub destination_details: Option<String>,
    pub log_format: String,
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlobalReplicationGroupMember {
    pub replication_group_id: String,
    pub replication_group_region: String,
    pub role: String,
    pub automatic_failover: bool,
    pub status: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GlobalReplicationGroup {
    pub global_replication_group_id: String,
    pub global_replication_group_description: String,
    pub status: String,
    pub cache_node_type: String,
    pub engine: String,
    pub engine_version: String,
    pub members: Vec<GlobalReplicationGroupMember>,
    pub cluster_enabled: bool,
    pub arn: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ElastiCacheUser {
    pub user_id: String,
    pub user_name: String,
    pub engine: String,
    pub access_string: String,
    pub status: String,
    pub authentication_type: String,
    pub password_count: i32,
    pub arn: String,
    pub minimum_engine_version: String,
    pub user_group_ids: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ElastiCacheUserGroup {
    pub user_group_id: String,
    pub engine: String,
    pub status: String,
    pub user_ids: Vec<String>,
    pub arn: String,
    pub minimum_engine_version: String,
    pub pending_changes: Option<UserGroupPendingChanges>,
    pub replication_groups: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UserGroupPendingChanges {
    pub user_ids_to_add: Vec<String>,
    pub user_ids_to_remove: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheSnapshot {
    pub snapshot_name: String,
    pub replication_group_id: String,
    pub replication_group_description: String,
    pub snapshot_status: String,
    pub cache_node_type: String,
    pub engine: String,
    pub engine_version: String,
    pub num_cache_clusters: i32,
    pub arn: String,
    pub created_at: String,
    pub snapshot_source: String,
    /// Path to the dumped RDB file on the local disk, if the runtime was
    /// available at snapshot-create time.
    pub rdb_path: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ServerlessCacheUsageLimits {
    pub data_storage: Option<ServerlessCacheDataStorage>,
    pub ecpu_per_second: Option<ServerlessCacheEcpuPerSecond>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerlessCacheDataStorage {
    pub maximum: Option<i32>,
    pub minimum: Option<i32>,
    pub unit: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerlessCacheEcpuPerSecond {
    pub maximum: Option<i32>,
    pub minimum: Option<i32>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerlessCacheEndpoint {
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerlessCache {
    pub serverless_cache_name: String,
    pub description: String,
    pub engine: String,
    pub major_engine_version: String,
    pub full_engine_version: String,
    pub status: String,
    pub endpoint: ServerlessCacheEndpoint,
    pub reader_endpoint: ServerlessCacheEndpoint,
    pub arn: String,
    pub created_at: String,
    pub cache_usage_limits: Option<ServerlessCacheUsageLimits>,
    pub security_group_ids: Vec<String>,
    pub subnet_ids: Vec<String>,
    pub kms_key_id: Option<String>,
    pub user_group_id: Option<String>,
    pub snapshot_retention_limit: Option<i32>,
    pub daily_snapshot_time: Option<String>,
    pub container_id: String,
    pub host_port: u16,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServerlessCacheSnapshot {
    pub serverless_cache_snapshot_name: String,
    pub arn: String,
    pub kms_key_id: Option<String>,
    pub snapshot_type: String,
    pub status: String,
    pub create_time: String,
    pub expiry_time: Option<String>,
    pub bytes_used_for_cache: Option<String>,
    pub serverless_cache_name: String,
    pub engine: String,
    pub major_engine_version: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheSecurityGroup {
    pub cache_security_group_name: String,
    pub description: String,
    pub owner_id: String,
    pub arn: String,
    pub ec2_security_groups: Vec<Ec2SecurityGroupAuth>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Ec2SecurityGroupAuth {
    pub status: String,
    pub ec2_security_group_name: String,
    pub ec2_security_group_owner_id: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheParameter {
    pub parameter_name: String,
    pub parameter_value: String,
    pub description: String,
    pub source: String,
    pub data_type: String,
    pub allowed_values: String,
    pub is_modifiable: bool,
    pub minimum_engine_version: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CacheEvent {
    pub source_identifier: String,
    pub source_type: String,
    pub message: String,
    pub date: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ServiceUpdate {
    pub service_update_name: String,
    pub service_update_release_date: String,
    pub service_update_end_date: String,
    pub service_update_severity: String,
    pub service_update_status: String,
    pub service_update_recommended_apply_by_date: String,
    pub service_update_type: String,
    pub engine: String,
    pub engine_version: String,
    pub auto_update_after_recommended_apply_by_date: bool,
    pub estimated_update_time: String,
    pub service_update_description: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UpdateAction {
    pub replication_group_id: Option<String>,
    pub cache_cluster_id: Option<String>,
    pub service_update_name: String,
    pub service_update_release_date: String,
    pub service_update_severity: String,
    pub service_update_status: String,
    pub service_update_recommended_apply_by_date: String,
    pub service_update_type: String,
    pub update_action_available_date: String,
    pub update_action_status: String,
    pub nodes_updated: String,
    pub update_action_status_modified_date: String,
    pub sla_met: String,
    pub estimated_update_time: String,
    pub engine: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Migration {
    pub replication_group_id: String,
    pub customer_node_endpoint_address: String,
    pub customer_node_endpoint_port: i32,
    pub status: String,
    pub started_at: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ElastiCacheState {
    pub account_id: String,
    pub region: String,
    pub parameter_groups: Vec<CacheParameterGroup>,
    pub subnet_groups: BTreeMap<String, CacheSubnetGroup>,
    pub reserved_cache_nodes: BTreeMap<String, ReservedCacheNode>,
    pub reserved_cache_nodes_offerings: Vec<ReservedCacheNodesOffering>,
    pub cache_clusters: BTreeMap<String, CacheCluster>,
    pub replication_groups: BTreeMap<String, ReplicationGroup>,
    pub global_replication_groups: BTreeMap<String, GlobalReplicationGroup>,
    pub users: BTreeMap<String, ElastiCacheUser>,
    pub user_groups: BTreeMap<String, ElastiCacheUserGroup>,
    pub snapshots: BTreeMap<String, CacheSnapshot>,
    pub serverless_caches: BTreeMap<String, ServerlessCache>,
    pub serverless_cache_snapshots: BTreeMap<String, ServerlessCacheSnapshot>,
    pub tags: BTreeMap<String, Vec<(String, String)>>,
    in_progress_cache_cluster_ids: HashSet<String>,
    /// Cache cluster ids whose DeleteCacheCluster arrived while the cluster was
    /// still being created (its container started during the lock-drop window of
    /// CreateCacheCluster). The create's finish step consults this so it reaps
    /// the container and does NOT resurrect the deleted cluster
    /// (bug-audit 2026-05-28, 4.3).
    #[serde(default)]
    delete_requested_cache_clusters: HashSet<String>,
    in_progress_replication_group_ids: HashSet<String>,
    in_progress_serverless_cache_names: HashSet<String>,
    #[serde(default)]
    pub security_groups: BTreeMap<String, CacheSecurityGroup>,
    #[serde(default)]
    pub parameter_group_parameters: BTreeMap<String, Vec<CacheParameter>>,
    #[serde(default)]
    pub events: Vec<CacheEvent>,
    /// Active migrations keyed by replication group id.
    #[serde(default)]
    pub migrations: BTreeMap<String, Migration>,
}

impl ElastiCacheState {
    pub fn new(account_id: &str, region: &str) -> Self {
        let parameter_groups = default_parameter_groups(account_id, region);
        let subnet_groups = default_subnet_groups(account_id, region);
        let users = default_users(account_id, region);
        let mut tags: BTreeMap<String, Vec<(String, String)>> = subnet_groups
            .values()
            .map(|g| (g.arn.clone(), Vec::new()))
            .collect();
        for user in users.values() {
            tags.insert(user.arn.clone(), Vec::new());
        }
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            parameter_groups,
            subnet_groups,
            reserved_cache_nodes: BTreeMap::new(),
            reserved_cache_nodes_offerings: default_reserved_cache_nodes_offerings(),
            cache_clusters: BTreeMap::new(),
            replication_groups: BTreeMap::new(),
            global_replication_groups: BTreeMap::new(),
            users,
            user_groups: BTreeMap::new(),
            snapshots: BTreeMap::new(),
            serverless_caches: BTreeMap::new(),
            serverless_cache_snapshots: BTreeMap::new(),
            tags,
            in_progress_cache_cluster_ids: HashSet::new(),
            delete_requested_cache_clusters: HashSet::new(),
            in_progress_replication_group_ids: HashSet::new(),
            in_progress_serverless_cache_names: HashSet::new(),
            security_groups: BTreeMap::new(),
            parameter_group_parameters: BTreeMap::new(),
            events: Vec::new(),
            migrations: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.parameter_groups = default_parameter_groups(&self.account_id, &self.region);
        self.subnet_groups = default_subnet_groups(&self.account_id, &self.region);
        self.reserved_cache_nodes.clear();
        self.reserved_cache_nodes_offerings = default_reserved_cache_nodes_offerings();
        self.cache_clusters.clear();
        self.replication_groups.clear();
        self.global_replication_groups.clear();
        self.users = default_users(&self.account_id, &self.region);
        self.user_groups.clear();
        self.snapshots.clear();
        self.serverless_caches.clear();
        self.serverless_cache_snapshots.clear();
        self.tags.clear();
        for g in self.subnet_groups.values() {
            self.tags.insert(g.arn.clone(), Vec::new());
        }
        for user in self.users.values() {
            self.tags.insert(user.arn.clone(), Vec::new());
        }
        self.in_progress_cache_cluster_ids.clear();
        self.in_progress_replication_group_ids.clear();
        self.in_progress_serverless_cache_names.clear();
        self.security_groups.clear();
        self.parameter_group_parameters.clear();
        self.events.clear();
        self.migrations.clear();
    }

    pub fn begin_cache_cluster_creation(&mut self, cache_cluster_id: &str) -> bool {
        if self.cache_clusters.contains_key(cache_cluster_id)
            || self
                .in_progress_cache_cluster_ids
                .contains(cache_cluster_id)
        {
            return false;
        }
        self.in_progress_cache_cluster_ids
            .insert(cache_cluster_id.to_string());
        true
    }

    pub fn finish_cache_cluster_creation(&mut self, cluster: CacheCluster) {
        self.in_progress_cache_cluster_ids
            .remove(&cluster.cache_cluster_id);
        self.tags.insert(cluster.arn.clone(), Vec::new());
        self.cache_clusters
            .insert(cluster.cache_cluster_id.clone(), cluster);
    }

    pub fn cancel_cache_cluster_creation(&mut self, cache_cluster_id: &str) {
        self.in_progress_cache_cluster_ids.remove(cache_cluster_id);
    }

    /// True if `id` is currently being created (in the lock-drop window of
    /// CreateCacheCluster, before it is inserted into `cache_clusters`).
    pub fn cache_cluster_creation_in_progress(&self, cache_cluster_id: &str) -> bool {
        self.in_progress_cache_cluster_ids
            .contains(cache_cluster_id)
    }

    /// Record that a DeleteCacheCluster arrived for `id` while it was still
    /// being created. The create's finish step calls
    /// [`take_cache_cluster_delete_request`] to detect this and reap the
    /// container instead of resurrecting the deleted cluster
    /// (bug-audit 2026-05-28, 4.3).
    pub fn request_cache_cluster_delete_during_creation(&mut self, cache_cluster_id: &str) {
        self.delete_requested_cache_clusters
            .insert(cache_cluster_id.to_string());
    }

    /// Consume a pending delete request for `id` (set while it was creating).
    /// Returns true if one was present, in which case the caller must drop the
    /// in-progress marker and reap any started container without inserting the
    /// cluster.
    pub fn take_cache_cluster_delete_request(&mut self, cache_cluster_id: &str) -> bool {
        self.delete_requested_cache_clusters
            .remove(cache_cluster_id)
    }

    pub fn begin_replication_group_creation(&mut self, replication_group_id: &str) -> bool {
        if self.replication_groups.contains_key(replication_group_id)
            || self
                .in_progress_replication_group_ids
                .contains(replication_group_id)
        {
            return false;
        }
        self.in_progress_replication_group_ids
            .insert(replication_group_id.to_string());
        true
    }

    pub fn finish_replication_group_creation(&mut self, group: ReplicationGroup) {
        self.in_progress_replication_group_ids
            .remove(&group.replication_group_id);
        self.tags.insert(group.arn.clone(), Vec::new());
        self.replication_groups
            .insert(group.replication_group_id.clone(), group);
    }

    pub fn cancel_replication_group_creation(&mut self, replication_group_id: &str) {
        self.in_progress_replication_group_ids
            .remove(replication_group_id);
    }

    pub fn begin_serverless_cache_creation(&mut self, serverless_cache_name: &str) -> bool {
        if self.serverless_caches.contains_key(serverless_cache_name)
            || self
                .in_progress_serverless_cache_names
                .contains(serverless_cache_name)
        {
            return false;
        }
        self.in_progress_serverless_cache_names
            .insert(serverless_cache_name.to_string());
        true
    }

    pub fn finish_serverless_cache_creation(&mut self, cache: ServerlessCache) {
        self.in_progress_serverless_cache_names
            .remove(&cache.serverless_cache_name);
        self.tags.insert(cache.arn.clone(), Vec::new());
        self.serverless_caches
            .insert(cache.serverless_cache_name.clone(), cache);
    }

    pub fn cancel_serverless_cache_creation(&mut self, serverless_cache_name: &str) {
        self.in_progress_serverless_cache_names
            .remove(serverless_cache_name);
    }

    pub fn register_arn(&mut self, arn: &str) {
        self.tags.entry(arn.to_string()).or_default();
    }

    pub fn has_arn(&self, arn: &str) -> bool {
        self.tags.contains_key(arn)
    }
}

fn default_reserved_cache_nodes_offerings() -> Vec<ReservedCacheNodesOffering> {
    vec![
        ReservedCacheNodesOffering {
            reserved_cache_nodes_offering_id: "off-cache-t3-micro-redis-1yr-no-upfront".to_string(),
            cache_node_type: "cache.t3.micro".to_string(),
            duration: 31_536_000,
            fixed_price: 0.0,
            usage_price: 0.011,
            product_description: "redis".to_string(),
            offering_type: "No Upfront".to_string(),
            recurring_charges: Vec::new(),
        },
        ReservedCacheNodesOffering {
            reserved_cache_nodes_offering_id: "off-cache-t3-small-redis-1yr-partial-upfront"
                .to_string(),
            cache_node_type: "cache.t3.small".to_string(),
            duration: 31_536_000,
            fixed_price: 120.0,
            usage_price: 0.007,
            product_description: "redis".to_string(),
            offering_type: "Partial Upfront".to_string(),
            recurring_charges: Vec::new(),
        },
        ReservedCacheNodesOffering {
            reserved_cache_nodes_offering_id: "off-cache-m5-large-memcached-3yr-no-upfront"
                .to_string(),
            cache_node_type: "cache.m5.large".to_string(),
            duration: 94_608_000,
            fixed_price: 0.0,
            usage_price: 0.033,
            product_description: "memcached".to_string(),
            offering_type: "No Upfront".to_string(),
            recurring_charges: Vec::new(),
        },
        ReservedCacheNodesOffering {
            reserved_cache_nodes_offering_id: "off-cache-r6g-large-redis-3yr-all-upfront"
                .to_string(),
            cache_node_type: "cache.r6g.large".to_string(),
            duration: 94_608_000,
            fixed_price: 1_550.0,
            usage_price: 0.0,
            product_description: "redis".to_string(),
            offering_type: "All Upfront".to_string(),
            recurring_charges: vec![RecurringCharge {
                recurring_charge_amount: 0.0,
                recurring_charge_frequency: "Hourly".to_string(),
            }],
        },
    ]
}

pub fn default_engine_versions() -> Vec<CacheEngineVersion> {
    vec![
        CacheEngineVersion {
            engine: "redis".to_string(),
            engine_version: "7.1".to_string(),
            cache_parameter_group_family: "redis7".to_string(),
            cache_engine_description: "Redis".to_string(),
            cache_engine_version_description: "Redis 7.1".to_string(),
        },
        CacheEngineVersion {
            engine: "valkey".to_string(),
            engine_version: "8.0".to_string(),
            cache_parameter_group_family: "valkey8".to_string(),
            cache_engine_description: "Valkey".to_string(),
            cache_engine_version_description: "Valkey 8.0".to_string(),
        },
        CacheEngineVersion {
            engine: "memcached".to_string(),
            engine_version: "1.6.22".to_string(),
            cache_parameter_group_family: "memcached1.6".to_string(),
            cache_engine_description: "Memcached".to_string(),
            cache_engine_version_description: "Memcached 1.6.22".to_string(),
        },
    ]
}

fn default_parameter_groups(account_id: &str, region: &str) -> Vec<CacheParameterGroup> {
    vec![
        CacheParameterGroup {
            cache_parameter_group_name: "default.redis7".to_string(),
            cache_parameter_group_family: "redis7".to_string(),
            description: "Default parameter group for redis7".to_string(),
            is_global: false,
            arn: Arn::new(
                "elasticache",
                region,
                account_id,
                "parametergroup:default.redis7",
            )
            .to_string(),
        },
        CacheParameterGroup {
            cache_parameter_group_name: "default.valkey8".to_string(),
            cache_parameter_group_family: "valkey8".to_string(),
            description: "Default parameter group for valkey8".to_string(),
            is_global: false,
            arn: Arn::new(
                "elasticache",
                region,
                account_id,
                "parametergroup:default.valkey8",
            )
            .to_string(),
        },
        CacheParameterGroup {
            cache_parameter_group_name: "default.memcached1.6".to_string(),
            cache_parameter_group_family: "memcached1.6".to_string(),
            description: "Default parameter group for memcached1.6".to_string(),
            is_global: false,
            arn: Arn::new(
                "elasticache",
                region,
                account_id,
                "parametergroup:default.memcached1.6",
            )
            .to_string(),
        },
    ]
}

fn default_subnet_groups(account_id: &str, region: &str) -> BTreeMap<String, CacheSubnetGroup> {
    let default_group = CacheSubnetGroup {
        cache_subnet_group_name: "default".to_string(),
        cache_subnet_group_description: "Default CacheSubnetGroup".to_string(),
        vpc_id: "vpc-00000000".to_string(),
        subnet_ids: vec!["subnet-00000000".to_string()],
        arn: Arn::new("elasticache", region, account_id, "subnetgroup:default").to_string(),
    };
    let mut map = BTreeMap::new();
    map.insert("default".to_string(), default_group);
    map
}

pub fn default_parameters_for_family(family: &str) -> Vec<EngineDefaultParameter> {
    match family {
        "redis7" => vec![
            EngineDefaultParameter {
                parameter_name: "maxmemory-policy".to_string(),
                parameter_value: "volatile-lru".to_string(),
                description: "Max memory policy".to_string(),
                source: "system".to_string(),
                data_type: "string".to_string(),
                allowed_values: "volatile-lru,allkeys-lru,volatile-lfu,allkeys-lfu,volatile-random,allkeys-random,volatile-ttl,noeviction".to_string(),
                is_modifiable: true,
                minimum_engine_version: "7.0.0".to_string(),
            },
            EngineDefaultParameter {
                parameter_name: "cluster-enabled".to_string(),
                parameter_value: "no".to_string(),
                description: "Enable or disable Redis Cluster mode".to_string(),
                source: "system".to_string(),
                data_type: "string".to_string(),
                allowed_values: "yes,no".to_string(),
                is_modifiable: false,
                minimum_engine_version: "7.0.0".to_string(),
            },
            EngineDefaultParameter {
                parameter_name: "activedefrag".to_string(),
                parameter_value: "no".to_string(),
                description: "Enable active defragmentation".to_string(),
                source: "system".to_string(),
                data_type: "string".to_string(),
                allowed_values: "yes,no".to_string(),
                is_modifiable: true,
                minimum_engine_version: "7.0.0".to_string(),
            },
        ],
        "valkey8" => vec![
            EngineDefaultParameter {
                parameter_name: "maxmemory-policy".to_string(),
                parameter_value: "volatile-lru".to_string(),
                description: "Max memory policy".to_string(),
                source: "system".to_string(),
                data_type: "string".to_string(),
                allowed_values: "volatile-lru,allkeys-lru,volatile-lfu,allkeys-lfu,volatile-random,allkeys-random,volatile-ttl,noeviction".to_string(),
                is_modifiable: true,
                minimum_engine_version: "8.0.0".to_string(),
            },
            EngineDefaultParameter {
                parameter_name: "cluster-enabled".to_string(),
                parameter_value: "no".to_string(),
                description: "Enable or disable cluster mode".to_string(),
                source: "system".to_string(),
                data_type: "string".to_string(),
                allowed_values: "yes,no".to_string(),
                is_modifiable: false,
                minimum_engine_version: "8.0.0".to_string(),
            },
            EngineDefaultParameter {
                parameter_name: "activedefrag".to_string(),
                parameter_value: "no".to_string(),
                description: "Enable active defragmentation".to_string(),
                source: "system".to_string(),
                data_type: "string".to_string(),
                allowed_values: "yes,no".to_string(),
                is_modifiable: true,
                minimum_engine_version: "8.0.0".to_string(),
            },
        ],
        "memcached1.6" => vec![
            EngineDefaultParameter {
                parameter_name: "max_item_size".to_string(),
                parameter_value: "1048576".to_string(),
                description: "Maximum item size".to_string(),
                source: "system".to_string(),
                data_type: "integer".to_string(),
                allowed_values: "1048576-1073741824".to_string(),
                is_modifiable: true,
                minimum_engine_version: "1.4.5".to_string(),
            },
            EngineDefaultParameter {
                parameter_name: "max_simultaneous_connections".to_string(),
                parameter_value: "65000".to_string(),
                description: "Maximum number of concurrent connections".to_string(),
                source: "system".to_string(),
                data_type: "integer".to_string(),
                allowed_values: "1-65000".to_string(),
                is_modifiable: false,
                minimum_engine_version: "1.4.5".to_string(),
            },
        ],
        _ => Vec::new(),
    }
}

fn default_users(account_id: &str, region: &str) -> BTreeMap<String, ElastiCacheUser> {
    let mut map = BTreeMap::new();
    map.insert(
        "default".to_string(),
        ElastiCacheUser {
            user_id: "default".to_string(),
            user_name: "default".to_string(),
            engine: "redis".to_string(),
            access_string: "on ~* +@all".to_string(),
            status: "active".to_string(),
            authentication_type: "no-password".to_string(),
            password_count: 0,
            arn: Arn::new("elasticache", region, account_id, "user:default").to_string(),
            minimum_engine_version: "6.0".to_string(),
            user_group_ids: Vec::new(),
        },
    );
    map
}

pub const ELASTICACHE_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ElastiCacheSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<ElastiCacheState>>,
    #[serde(default)]
    pub state: Option<ElastiCacheState>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_engine_versions_contains_redis_valkey_memcached() {
        let versions = default_engine_versions();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].engine, "redis");
        assert_eq!(versions[0].engine_version, "7.1");
        assert_eq!(versions[1].engine, "valkey");
        assert_eq!(versions[1].engine_version, "8.0");
        assert_eq!(versions[2].engine, "memcached");
        assert_eq!(versions[2].engine_version, "1.6.22");
    }

    #[test]
    fn state_new_creates_default_parameter_groups() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert_eq!(state.parameter_groups.len(), 3);
        assert_eq!(
            state.parameter_groups[0].cache_parameter_group_name,
            "default.redis7"
        );
        assert_eq!(
            state.parameter_groups[1].cache_parameter_group_name,
            "default.valkey8"
        );
        assert_eq!(
            state.parameter_groups[2].cache_parameter_group_name,
            "default.memcached1.6"
        );
    }

    #[test]
    fn state_new_creates_default_subnet_group() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert_eq!(state.subnet_groups.len(), 1);
        let default = state.subnet_groups.get("default").unwrap();
        assert_eq!(default.cache_subnet_group_name, "default");
        assert_eq!(
            default.cache_subnet_group_description,
            "Default CacheSubnetGroup"
        );
        assert_eq!(default.vpc_id, "vpc-00000000");
        assert!(!default.subnet_ids.is_empty());
        assert!(default.arn.contains("subnetgroup:default"));
    }

    #[test]
    fn reset_restores_default_parameter_groups() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.parameter_groups.clear();
        assert!(state.parameter_groups.is_empty());
        state.reset();
        assert_eq!(state.parameter_groups.len(), 3);
    }

    #[test]
    fn reset_restores_default_subnet_groups() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.subnet_groups.clear();
        assert!(state.subnet_groups.is_empty());
        state.reset();
        assert_eq!(state.subnet_groups.len(), 1);
        assert!(state.subnet_groups.contains_key("default"));
    }

    #[test]
    fn default_parameters_for_redis7_returns_parameters() {
        let params = default_parameters_for_family("redis7");
        assert_eq!(params.len(), 3);
        assert_eq!(params[0].parameter_name, "maxmemory-policy");
    }

    #[test]
    fn default_parameters_for_unknown_family_returns_empty() {
        let params = default_parameters_for_family("unknown");
        assert!(params.is_empty());
    }

    #[test]
    fn state_new_has_empty_replication_groups() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert!(state.replication_groups.is_empty());
    }

    #[test]
    fn state_new_has_empty_global_replication_groups() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert!(state.global_replication_groups.is_empty());
    }

    #[test]
    fn state_new_has_empty_cache_clusters() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert!(state.cache_clusters.is_empty());
    }

    #[test]
    fn state_new_has_empty_serverless_caches() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert!(state.serverless_caches.is_empty());
        assert!(state.serverless_cache_snapshots.is_empty());
    }

    #[test]
    fn begin_cache_cluster_creation_rejects_duplicate_ids() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");

        assert!(state.begin_cache_cluster_creation("cluster-1"));
        assert!(!state.begin_cache_cluster_creation("cluster-1"));

        state.cancel_cache_cluster_creation("cluster-1");
        assert!(state.begin_cache_cluster_creation("cluster-1"));
    }

    #[test]
    fn begin_replication_group_creation_rejects_duplicate_ids() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");

        assert!(state.begin_replication_group_creation("rg-1"));
        assert!(!state.begin_replication_group_creation("rg-1"));

        state.cancel_replication_group_creation("rg-1");
        assert!(state.begin_replication_group_creation("rg-1"));
    }

    #[test]
    fn begin_serverless_cache_creation_rejects_duplicate_names() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");

        assert!(state.begin_serverless_cache_creation("cache-1"));
        assert!(!state.begin_serverless_cache_creation("cache-1"));

        state.cancel_serverless_cache_creation("cache-1");
        assert!(state.begin_serverless_cache_creation("cache-1"));
    }

    #[test]
    fn finish_serverless_cache_creation_registers_cache_and_tags() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        assert!(state.begin_serverless_cache_creation("cache-1"));

        let cache = ServerlessCache {
            serverless_cache_name: "cache-1".to_string(),
            description: "test".to_string(),
            engine: "redis".to_string(),
            major_engine_version: "7.1".to_string(),
            full_engine_version: "7.1".to_string(),
            status: "available".to_string(),
            endpoint: ServerlessCacheEndpoint {
                address: "127.0.0.1".to_string(),
                port: 6379,
            },
            reader_endpoint: ServerlessCacheEndpoint {
                address: "127.0.0.1".to_string(),
                port: 6379,
            },
            arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:cache-1".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            cache_usage_limits: None,
            security_group_ids: Vec::new(),
            subnet_ids: Vec::new(),
            kms_key_id: None,
            user_group_id: None,
            snapshot_retention_limit: None,
            daily_snapshot_time: None,
            container_id: "cid".to_string(),
            host_port: 6379,
        };

        state.finish_serverless_cache_creation(cache.clone());

        assert!(state.serverless_caches.contains_key("cache-1"));
        assert!(state.tags.contains_key(&cache.arn));
    }

    #[test]
    fn state_new_creates_default_user() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert_eq!(state.users.len(), 1);
        let default = state.users.get("default").unwrap();
        assert_eq!(default.user_id, "default");
        assert_eq!(default.user_name, "default");
        assert_eq!(default.engine, "redis");
        assert_eq!(default.access_string, "on ~* +@all");
        assert_eq!(default.status, "active");
        assert_eq!(default.authentication_type, "no-password");
        assert_eq!(default.password_count, 0);
        assert!(default.arn.contains("user:default"));
    }

    #[test]
    fn state_new_has_empty_user_groups() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert!(state.user_groups.is_empty());
    }

    #[test]
    fn reset_restores_default_user() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.users.clear();
        assert!(state.users.is_empty());
        state.reset();
        assert_eq!(state.users.len(), 1);
        assert!(state.users.contains_key("default"));
    }

    #[test]
    fn reset_clears_user_groups() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.user_groups.insert(
            "my-group".to_string(),
            ElastiCacheUserGroup {
                user_group_id: "my-group".to_string(),
                engine: "redis".to_string(),
                status: "active".to_string(),
                user_ids: vec!["default".to_string()],
                arn: "arn:aws:elasticache:us-east-1:123456789012:usergroup:my-group".to_string(),
                minimum_engine_version: "6.0".to_string(),
                pending_changes: None,
                replication_groups: Vec::new(),
            },
        );
        assert_eq!(state.user_groups.len(), 1);
        state.reset();
        assert!(state.user_groups.is_empty());
    }

    #[test]
    fn state_new_has_empty_snapshots() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert!(state.snapshots.is_empty());
    }

    #[test]
    fn reset_clears_snapshots() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.snapshots.insert(
            "my-snapshot".to_string(),
            CacheSnapshot {
                snapshot_name: "my-snapshot".to_string(),
                replication_group_id: "rg-1".to_string(),
                replication_group_description: "test".to_string(),
                snapshot_status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                num_cache_clusters: 1,
                arn: "arn:aws:elasticache:us-east-1:123456789012:snapshot:my-snapshot".to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                snapshot_source: "manual".to_string(),
                rdb_path: None,
            },
        );
        assert_eq!(state.snapshots.len(), 1);
        state.reset();
        assert!(state.snapshots.is_empty());
    }

    #[test]
    fn reset_clears_replication_groups() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.replication_groups.insert(
            "my-group".to_string(),
            ReplicationGroup {
                replication_group_id: "my-group".to_string(),
                description: "test".to_string(),
                global_replication_group_id: None,
                global_replication_group_role: None,
                status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                num_cache_clusters: 1,
                automatic_failover_enabled: false,
                endpoint_address: "127.0.0.1".to_string(),
                endpoint_port: 6379,
                arn: "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:my-group"
                    .to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                container_id: "abc123".to_string(),
                host_port: 12345,
                member_clusters: vec!["my-group-001".to_string()],
                snapshot_retention_limit: 0,
                snapshot_window: "05:00-09:00".to_string(),
                transit_encryption_enabled: false,
                at_rest_encryption_enabled: false,
                cluster_enabled: false,
                kms_key_id: None,
                auth_token_enabled: false,
                user_group_ids: Vec::new(),
                multi_az_enabled: false,
                log_delivery_configurations: Vec::new(),
                data_tiering: None,
                ip_discovery: None,
                network_type: None,
                transit_encryption_mode: None,
                num_node_groups: 1,
                configuration_endpoint_address: None,
                configuration_endpoint_port: None,
                replicas_per_node_group: None,
                auth_token: None,
                port: 6379,
                notification_topic_arn: None,
                cluster_mode: None,
                data_tiering_enabled: None,
                notification_topic_status: None,
                cache_parameter_group_name: None,
                cache_subnet_group_name: None,
                security_group_ids: Vec::new(),
                preferred_maintenance_window: None,
                snapshot_name: None,
                snapshot_arns: Vec::new(),
                auto_minor_version_upgrade: true,
            },
        );
        assert_eq!(state.replication_groups.len(), 1);
        state.reset();
        assert!(state.replication_groups.is_empty());
    }

    #[test]
    fn reset_clears_global_replication_groups() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.global_replication_groups.insert(
            "global-rg".to_string(),
            GlobalReplicationGroup {
                global_replication_group_id: "global-rg".to_string(),
                global_replication_group_description: "test".to_string(),
                status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                members: vec![GlobalReplicationGroupMember {
                    replication_group_id: "rg-1".to_string(),
                    replication_group_region: "us-east-1".to_string(),
                    role: "primary".to_string(),
                    automatic_failover: false,
                    status: "associated".to_string(),
                }],
                cluster_enabled: false,
                arn: "arn:aws:elasticache:us-east-1:123456789012:globalreplicationgroup:global-rg"
                    .to_string(),
            },
        );
        assert_eq!(state.global_replication_groups.len(), 1);
        state.reset();
        assert!(state.global_replication_groups.is_empty());
    }

    #[test]
    fn reset_clears_cache_clusters() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.cache_clusters.insert(
            "classic-cluster".to_string(),
            CacheCluster {
                cache_cluster_id: "classic-cluster".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                cache_cluster_status: "available".to_string(),
                num_cache_nodes: 1,
                preferred_availability_zone: "us-east-1a".to_string(),
                cache_subnet_group_name: Some("default".to_string()),
                auto_minor_version_upgrade: true,
                arn: "arn:aws:elasticache:us-east-1:123456789012:cluster:classic-cluster"
                    .to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                endpoint_address: "127.0.0.1".to_string(),
                endpoint_port: 6379,
                container_id: "abc123".to_string(),
                host_port: 12345,
                replication_group_id: None,
                cache_parameter_group_name: None,
                security_group_ids: Vec::new(),
                log_delivery_configurations: Vec::new(),
                transit_encryption_enabled: false,
                at_rest_encryption_enabled: false,
                auth_token_enabled: false,
                port: 6379,
                preferred_maintenance_window: None,
                preferred_availability_zones: Vec::new(),
                notification_topic_arn: None,
                cache_security_group_names: Vec::new(),
                snapshot_arns: Vec::new(),
                snapshot_name: None,
                snapshot_retention_limit: 0,
                snapshot_window: None,
                outpost_mode: None,
                preferred_outpost_arn: None,
                network_type: None,
                ip_discovery: None,
                az_mode: None,
                auth_token: None,
                kms_key_id: None,
                transit_encryption_mode: None,
                data_tiering_enabled: None,
                cluster_mode: None,
                preferred_outpost_arns: Vec::new(),
            },
        );
        assert_eq!(state.cache_clusters.len(), 1);
        state.reset();
        assert!(state.cache_clusters.is_empty());
    }

    #[test]
    fn reset_restores_reserved_cache_node_metadata() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.reserved_cache_nodes.insert(
            "rcn-a".to_string(),
            ReservedCacheNode {
                reserved_cache_node_id: "rcn-a".to_string(),
                reserved_cache_nodes_offering_id: "offering-a".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                start_time: "2024-01-01T00:00:00Z".to_string(),
                duration: 31_536_000,
                fixed_price: 0.0,
                usage_price: 0.011,
                cache_node_count: 1,
                product_description: "redis".to_string(),
                offering_type: "No Upfront".to_string(),
                state: "payment-pending".to_string(),
                recurring_charges: Vec::new(),
                reservation_arn:
                    "arn:aws:elasticache:us-east-1:123456789012:reserved-instance:test".to_string(),
            },
        );
        state.reserved_cache_nodes_offerings.clear();

        state.reset();

        assert!(state.reserved_cache_nodes.is_empty());
        assert!(!state.reserved_cache_nodes_offerings.is_empty());
    }

    #[test]
    fn reset_clears_serverless_cache_state() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.serverless_caches.insert(
            "serverless".to_string(),
            ServerlessCache {
                serverless_cache_name: "serverless".to_string(),
                description: "test".to_string(),
                engine: "redis".to_string(),
                major_engine_version: "7.1".to_string(),
                full_engine_version: "7.1".to_string(),
                status: "available".to_string(),
                endpoint: ServerlessCacheEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 6379,
                },
                reader_endpoint: ServerlessCacheEndpoint {
                    address: "127.0.0.1".to_string(),
                    port: 6379,
                },
                arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:serverless"
                    .to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                cache_usage_limits: None,
                security_group_ids: Vec::new(),
                subnet_ids: Vec::new(),
                kms_key_id: None,
                user_group_id: None,
                snapshot_retention_limit: None,
                daily_snapshot_time: None,
                container_id: "cid".to_string(),
                host_port: 6379,
            },
        );
        state.serverless_cache_snapshots.insert(
            "snap-1".to_string(),
            ServerlessCacheSnapshot {
                serverless_cache_snapshot_name: "snap-1".to_string(),
                arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-1"
                    .to_string(),
                kms_key_id: None,
                snapshot_type: "manual".to_string(),
                status: "available".to_string(),
                create_time: "2024-01-01T00:00:00Z".to_string(),
                expiry_time: None,
                bytes_used_for_cache: None,
                serverless_cache_name: "serverless".to_string(),
                engine: "redis".to_string(),
                major_engine_version: "7.1".to_string(),
            },
        );

        state.reset();

        assert!(state.serverless_caches.is_empty());
        assert!(state.serverless_cache_snapshots.is_empty());
    }
}
