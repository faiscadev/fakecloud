//! Account-partitioned, serializable state for AWS MemoryDB.
//!
//! MemoryDB is a Redis/Valkey-compatible in-memory database. This models the
//! full control plane — clusters (with shards/nodes), ACLs, users, parameter
//! groups, subnet groups, and snapshots — as typed, serializable state keyed
//! by name within each account. Cluster data-plane backing (a real Redis
//! container) is layered on top of this control plane, mirroring ElastiCache.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const MEMORYDB_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Tags on a resource, stored by ARN so `ListTags`/`TagResource` work
/// uniformly across every MemoryDB resource type.
pub type TagMap = BTreeMap<String, String>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub address: String,
    pub port: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub name: String,
    pub status: String,
    pub availability_zone: String,
    pub create_time: DateTime<Utc>,
    pub endpoint: Endpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shard {
    pub name: String,
    pub status: String,
    pub slots: String,
    pub nodes: Vec<Node>,
    pub number_of_nodes: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    pub name: String,
    pub description: Option<String>,
    pub status: String,
    pub number_of_shards: i32,
    pub shards: Vec<Shard>,
    pub node_type: String,
    pub engine: String,
    pub engine_version: String,
    pub engine_patch_version: String,
    pub parameter_group_name: String,
    pub parameter_group_status: String,
    pub security_group_ids: Vec<String>,
    pub subnet_group_name: String,
    pub tls_enabled: bool,
    pub kms_key_id: Option<String>,
    pub arn: String,
    pub sns_topic_arn: Option<String>,
    pub snapshot_retention_limit: i32,
    pub maintenance_window: String,
    pub snapshot_window: String,
    pub acl_name: String,
    pub auto_minor_version_upgrade: bool,
    pub data_tiering: String,
    pub availability_mode: String,
    pub cluster_endpoint: Endpoint,
    pub network_type: String,
    pub ip_discovery: String,
    /// The port the cluster listens on (echoed into endpoints).
    pub port: i32,
    /// Backing Redis container id, when a data-plane container is running.
    pub container_id: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Acl {
    pub name: String,
    pub status: String,
    pub user_names: Vec<String>,
    pub minimum_engine_version: String,
    pub clusters: Vec<String>,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAuthentication {
    pub type_: String,
    pub password_count: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub name: String,
    pub status: String,
    pub access_string: String,
    pub acl_names: Vec<String>,
    pub minimum_engine_version: String,
    pub authentication: UserAuthentication,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterGroup {
    pub name: String,
    pub family: String,
    pub description: String,
    pub arn: String,
    /// Explicitly set (name -> value) parameter overrides.
    pub parameters: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubnetGroup {
    pub name: String,
    pub description: String,
    pub vpc_id: String,
    pub subnet_ids: Vec<String>,
    pub arn: String,
    pub supported_network_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub name: String,
    pub status: String,
    pub source: String,
    pub kms_key_id: Option<String>,
    pub arn: String,
    pub data_tiering: String,
    /// Snapshotted cluster configuration, stored as the response JSON so the
    /// full `ClusterConfiguration` shape round-trips faithfully.
    pub cluster_configuration: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiRegionCluster {
    pub name: String,
    pub description: Option<String>,
    pub status: String,
    pub node_type: String,
    pub engine: String,
    pub engine_version: String,
    pub number_of_shards: i32,
    pub multi_region_parameter_group_name: Option<String>,
    pub tls_enabled: bool,
    pub arn: String,
    /// Member clusters: (cluster_name, region, status, arn) JSON objects.
    pub clusters: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservedNode {
    pub reservation_id: String,
    pub reserved_nodes_offering_id: String,
    pub node_type: String,
    pub start_time: DateTime<Utc>,
    pub duration: i32,
    pub fixed_price: f64,
    pub node_count: i32,
    pub offering_type: String,
    pub state: String,
    pub arn: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryDbState {
    pub clusters: BTreeMap<String, Cluster>,
    pub acls: BTreeMap<String, Acl>,
    pub users: BTreeMap<String, User>,
    pub parameter_groups: BTreeMap<String, ParameterGroup>,
    pub subnet_groups: BTreeMap<String, SubnetGroup>,
    pub snapshots: BTreeMap<String, Snapshot>,
    pub multi_region_clusters: BTreeMap<String, MultiRegionCluster>,
    pub reserved_nodes: BTreeMap<String, ReservedNode>,
    /// Tags keyed by resource ARN.
    pub tags: BTreeMap<String, TagMap>,
}

impl MemoryDbState {
    /// Re-point the ARNs of the AWS-provided defaults (`default` user,
    /// `open-access` ACL, `default.memorydb-*` parameter groups) at the region
    /// the caller is asking about.
    ///
    /// Account state is shared across regions, so defaults seeded when the
    /// account was first touched from `us-east-1` would otherwise keep
    /// answering `DescribeUsers`/`DescribeACLs`/`DescribeParameterGroups` in
    /// `eu-west-1` with `us-east-1` ARNs, which the Terraform provider reads
    /// back as drift. These resources exist in every region, so re-point them
    /// rather than freezing whichever region created the account.
    pub fn retarget_default_arns(&mut self, region: &str, account_id: &str) {
        if let Some(user) = self.users.get_mut("default") {
            user.arn = format!("arn:aws:memorydb:{region}:{account_id}:user/default");
        }
        if let Some(acl) = self.acls.get_mut("open-access") {
            acl.arn = format!("arn:aws:memorydb:{region}:{account_id}:acl/open-access");
        }
        for (name, pg) in self.parameter_groups.iter_mut() {
            if name.starts_with("default.") {
                pg.arn = format!("arn:aws:memorydb:{region}:{account_id}:parametergroup/{name}");
            }
        }
    }
}

impl AccountState for MemoryDbState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        // AWS seeds every account with a default ACL and default user.
        let mut s = Self::default();
        s.users.insert(
            "default".to_string(),
            User {
                name: "default".to_string(),
                status: "active".to_string(),
                access_string: "on ~* &* +@all".to_string(),
                acl_names: vec!["open-access".to_string()],
                minimum_engine_version: "6.2".to_string(),
                authentication: UserAuthentication {
                    type_: "no-password".to_string(),
                    password_count: 0,
                },
                arn: format!("arn:aws:memorydb:{region}:{account_id}:user/default"),
            },
        );
        s.acls.insert(
            "open-access".to_string(),
            Acl {
                name: "open-access".to_string(),
                status: "active".to_string(),
                user_names: vec!["default".to_string()],
                minimum_engine_version: "6.2".to_string(),
                clusters: vec![],
                arn: format!("arn:aws:memorydb:{region}:{account_id}:acl/open-access"),
            },
        );
        // AWS provides a default parameter group per supported engine family.
        // The Terraform provider reads e.g. `default.memorydb-redis7` when
        // managing a parameter group, so these must exist out of the box.
        for (pg_name, family) in [
            ("default.memorydb-redis7", "memorydb_redis7"),
            ("default.memorydb-redis6", "memorydb_redis6"),
            ("default.memorydb-valkey7", "memorydb_valkey7"),
        ] {
            s.parameter_groups.insert(
                pg_name.to_string(),
                ParameterGroup {
                    name: pg_name.to_string(),
                    family: family.to_string(),
                    description: format!("Default parameter group for {family}"),
                    arn: format!("arn:aws:memorydb:{region}:{account_id}:parametergroup/{pg_name}"),
                    parameters: BTreeMap::new(),
                },
            );
        }
        s
    }
}

pub type SharedMemoryDbState = Arc<RwLock<MultiAccountState<MemoryDbState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct MemoryDbSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<MemoryDbState>,
}
