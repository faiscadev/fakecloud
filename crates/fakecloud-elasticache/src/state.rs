use std::collections::{HashMap, HashSet};
use std::sync::Arc;

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
pub struct ReplicationGroup {
    pub replication_group_id: String,
    pub description: String,
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
}

#[derive(Debug)]
pub struct ElastiCacheState {
    pub account_id: String,
    pub region: String,
    pub parameter_groups: Vec<CacheParameterGroup>,
    pub subnet_groups: HashMap<String, CacheSubnetGroup>,
    pub replication_groups: HashMap<String, ReplicationGroup>,
    pub tags: HashMap<String, Vec<(String, String)>>,
    in_progress_replication_group_ids: HashSet<String>,
}

impl ElastiCacheState {
    pub fn new(account_id: &str, region: &str) -> Self {
        let parameter_groups = default_parameter_groups(account_id, region);
        let subnet_groups = default_subnet_groups(account_id, region);
        let tags: HashMap<String, Vec<(String, String)>> = subnet_groups
            .values()
            .map(|g| (g.arn.clone(), Vec::new()))
            .collect();
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            parameter_groups,
            subnet_groups,
            replication_groups: HashMap::new(),
            tags,
            in_progress_replication_group_ids: HashSet::new(),
        }
    }

    pub fn reset(&mut self) {
        self.parameter_groups = default_parameter_groups(&self.account_id, &self.region);
        self.subnet_groups = default_subnet_groups(&self.account_id, &self.region);
        self.replication_groups.clear();
        self.tags.clear();
        for g in self.subnet_groups.values() {
            self.tags.insert(g.arn.clone(), Vec::new());
        }
        self.in_progress_replication_group_ids.clear();
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

    pub fn register_arn(&mut self, arn: &str) {
        self.tags.entry(arn.to_string()).or_default();
    }

    pub fn has_arn(&self, arn: &str) -> bool {
        self.tags.contains_key(arn)
    }
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
            arn: format!("arn:aws:elasticache:{region}:{account_id}:parametergroup:default.redis7"),
        },
        CacheParameterGroup {
            cache_parameter_group_name: "default.valkey8".to_string(),
            cache_parameter_group_family: "valkey8".to_string(),
            description: "Default parameter group for valkey8".to_string(),
            is_global: false,
            arn: format!(
                "arn:aws:elasticache:{region}:{account_id}:parametergroup:default.valkey8"
            ),
        },
    ]
}

fn default_subnet_groups(account_id: &str, region: &str) -> HashMap<String, CacheSubnetGroup> {
    let default_group = CacheSubnetGroup {
        cache_subnet_group_name: "default".to_string(),
        cache_subnet_group_description: "Default CacheSubnetGroup".to_string(),
        vpc_id: "vpc-00000000".to_string(),
        subnet_ids: vec!["subnet-00000000".to_string()],
        arn: format!("arn:aws:elasticache:{region}:{account_id}:subnetgroup:default"),
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
    fn begin_replication_group_creation_rejects_duplicate_ids() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");

        assert!(state.begin_replication_group_creation("rg-1"));
        assert!(!state.begin_replication_group_creation("rg-1"));

        state.cancel_replication_group_creation("rg-1");
        assert!(state.begin_replication_group_creation("rg-1"));
    }

    #[test]
    fn reset_clears_replication_groups() {
        let mut state = ElastiCacheState::new("123456789012", "us-east-1");
        state.replication_groups.insert(
            "my-group".to_string(),
            ReplicationGroup {
                replication_group_id: "my-group".to_string(),
                description: "test".to_string(),
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
            },
        );
        assert_eq!(state.replication_groups.len(), 1);
        state.reset();
        assert!(state.replication_groups.is_empty());
    }
}
