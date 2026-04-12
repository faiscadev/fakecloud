use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use fakecloud_aws::arn::Arn;
use parking_lot::RwLock;

pub type SharedElastiCacheState = Arc<RwLock<ElastiCacheState>>;

#[derive(Debug, Clone)]
pub struct CacheEngineVersion {
    pub engine: String,
    pub engine_version: String,
    pub cache_parameter_group_family: String,
    pub cache_engine_description: String,
    pub cache_engine_version_description: String,
}

#[derive(Debug, Clone)]
pub struct CacheParameterGroup {
    pub cache_parameter_group_name: String,
    pub cache_parameter_group_family: String,
    pub description: String,
    pub is_global: bool,
    pub arn: String,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct CacheSubnetGroup {
    pub cache_subnet_group_name: String,
    pub cache_subnet_group_description: String,
    pub vpc_id: String,
    pub subnet_ids: Vec<String>,
    pub arn: String,
}

#[derive(Debug, Clone)]
pub struct RecurringCharge {
    pub recurring_charge_amount: f64,
    pub recurring_charge_frequency: String,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
}

#[derive(Debug, Clone)]
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
}

#[derive(Debug, Clone)]
pub struct GlobalReplicationGroupMember {
    pub replication_group_id: String,
    pub replication_group_region: String,
    pub role: String,
    pub automatic_failover: bool,
    pub status: String,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct UserGroupPendingChanges {
    pub user_ids_to_add: Vec<String>,
    pub user_ids_to_remove: Vec<String>,
}

#[derive(Debug, Clone)]
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
}

#[derive(Debug, Clone, Default)]
pub struct ServerlessCacheUsageLimits {
    pub data_storage: Option<ServerlessCacheDataStorage>,
    pub ecpu_per_second: Option<ServerlessCacheEcpuPerSecond>,
}

#[derive(Debug, Clone)]
pub struct ServerlessCacheDataStorage {
    pub maximum: Option<i32>,
    pub minimum: Option<i32>,
    pub unit: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ServerlessCacheEcpuPerSecond {
    pub maximum: Option<i32>,
    pub minimum: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct ServerlessCacheEndpoint {
    pub address: String,
    pub port: u16,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug)]
pub struct ElastiCacheState {
    pub account_id: String,
    pub region: String,
    pub parameter_groups: Vec<CacheParameterGroup>,
    pub subnet_groups: HashMap<String, CacheSubnetGroup>,
    pub reserved_cache_nodes: HashMap<String, ReservedCacheNode>,
    pub reserved_cache_nodes_offerings: Vec<ReservedCacheNodesOffering>,
    pub cache_clusters: HashMap<String, CacheCluster>,
    pub replication_groups: HashMap<String, ReplicationGroup>,
    pub global_replication_groups: HashMap<String, GlobalReplicationGroup>,
    pub users: HashMap<String, ElastiCacheUser>,
    pub user_groups: HashMap<String, ElastiCacheUserGroup>,
    pub snapshots: HashMap<String, CacheSnapshot>,
    pub serverless_caches: HashMap<String, ServerlessCache>,
    pub serverless_cache_snapshots: HashMap<String, ServerlessCacheSnapshot>,
    pub tags: HashMap<String, Vec<(String, String)>>,
    in_progress_cache_cluster_ids: HashSet<String>,
    in_progress_replication_group_ids: HashSet<String>,
    in_progress_serverless_cache_names: HashSet<String>,
}

impl ElastiCacheState {
    pub fn new(account_id: &str, region: &str) -> Self {
        let parameter_groups = default_parameter_groups(account_id, region);
        let subnet_groups = default_subnet_groups(account_id, region);
        let users = default_users(account_id, region);
        let mut tags: HashMap<String, Vec<(String, String)>> = subnet_groups
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
            reserved_cache_nodes: HashMap::new(),
            reserved_cache_nodes_offerings: default_reserved_cache_nodes_offerings(),
            cache_clusters: HashMap::new(),
            replication_groups: HashMap::new(),
            global_replication_groups: HashMap::new(),
            users,
            user_groups: HashMap::new(),
            snapshots: HashMap::new(),
            serverless_caches: HashMap::new(),
            serverless_cache_snapshots: HashMap::new(),
            tags,
            in_progress_cache_cluster_ids: HashSet::new(),
            in_progress_replication_group_ids: HashSet::new(),
            in_progress_serverless_cache_names: HashSet::new(),
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
    ]
}

fn default_parameter_groups(account_id: &str, region: &str) -> Vec<CacheParameterGroup> {
    vec![
        CacheParameterGroup {
            cache_parameter_group_name: "default.redis7".to_string(),
            cache_parameter_group_family: "redis7".to_string(),
            description: "Default parameter group for redis7".to_string(),
            is_global: false,
            arn: Arn::new("elasticache", region, account_id, "parametergroup:default.redis7").to_string(),
        },
        CacheParameterGroup {
            cache_parameter_group_name: "default.valkey8".to_string(),
            cache_parameter_group_family: "valkey8".to_string(),
            description: "Default parameter group for valkey8".to_string(),
            is_global: false,
            arn: Arn::new("elasticache", region, account_id, "parametergroup:default.valkey8").to_string(),
        },
    ]
}

fn default_subnet_groups(account_id: &str, region: &str) -> HashMap<String, CacheSubnetGroup> {
    let default_group = CacheSubnetGroup {
        cache_subnet_group_name: "default".to_string(),
        cache_subnet_group_description: "Default CacheSubnetGroup".to_string(),
        vpc_id: "vpc-00000000".to_string(),
        subnet_ids: vec!["subnet-00000000".to_string()],
        arn: Arn::new("elasticache", region, account_id, "subnetgroup:default").to_string(),
    };
    let mut map = HashMap::new();
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
        _ => Vec::new(),
    }
}

fn default_users(account_id: &str, region: &str) -> HashMap<String, ElastiCacheUser> {
    let mut map = HashMap::new();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_engine_versions_contains_redis_and_valkey() {
        let versions = default_engine_versions();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].engine, "redis");
        assert_eq!(versions[0].engine_version, "7.1");
        assert_eq!(versions[1].engine, "valkey");
        assert_eq!(versions[1].engine_version, "8.0");
    }

    #[test]
    fn state_new_creates_default_parameter_groups() {
        let state = ElastiCacheState::new("123456789012", "us-east-1");
        assert_eq!(state.parameter_groups.len(), 2);
        assert_eq!(
            state.parameter_groups[0].cache_parameter_group_name,
            "default.redis7"
        );
        assert_eq!(
            state.parameter_groups[1].cache_parameter_group_name,
            "default.valkey8"
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
        assert_eq!(state.parameter_groups.len(), 2);
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
