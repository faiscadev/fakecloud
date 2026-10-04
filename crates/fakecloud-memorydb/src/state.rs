//! Account- and region-partitioned, serializable state for AWS MemoryDB.
//!
//! MemoryDB is a Redis/Valkey-compatible in-memory database. This models the
//! full control plane — clusters (with shards/nodes), ACLs, users, parameter
//! groups, subnet groups, and snapshots — as typed, serializable state keyed
//! by name within each (account, region). Multi-region clusters span regions
//! and are kept once per account. Cluster data-plane backing (a real Redis
//! container) is layered on top of this control plane, mirroring ElastiCache.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_aws::arn::Arn;
use fakecloud_core::multi_account::{AccountState, MultiAccountState, RegionalState};

/// v2: per-account state split by region, multi-region clusters kept
/// account-wide. v1 kept one state per account for every region.
pub const MEMORYDB_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

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

/// One account's MemoryDB resources in one region. Every region carries its
/// own AWS-provided defaults (`default` user, `open-access` ACL,
/// `default.memorydb-*` parameter groups) with that region's ARNs.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryDbState {
    pub clusters: BTreeMap<String, Cluster>,
    pub acls: BTreeMap<String, Acl>,
    pub users: BTreeMap<String, User>,
    pub parameter_groups: BTreeMap<String, ParameterGroup>,
    pub subnet_groups: BTreeMap<String, SubnetGroup>,
    pub snapshots: BTreeMap<String, Snapshot>,
    pub reserved_nodes: BTreeMap<String, ReservedNode>,
    /// Tags keyed by resource ARN.
    pub tags: BTreeMap<String, TagMap>,
}

/// One account's MemoryDB state: the regional resources split per region,
/// plus the multi-region clusters. A multi-region cluster spans regions, so
/// it is account-wide (visible from every region) rather than regional.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryDbAccount {
    pub regions: RegionalState<MemoryDbState>,
    pub multi_region_clusters: BTreeMap<String, MultiRegionCluster>,
    /// Tags on multi-region clusters, keyed by ARN.
    pub multi_region_cluster_tags: BTreeMap<String, TagMap>,
}

impl AccountState for MemoryDbAccount {
    fn new_for_account(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self {
            regions: RegionalState::new(account_id, region, endpoint),
            multi_region_clusters: BTreeMap::new(),
            multi_region_cluster_tags: BTreeMap::new(),
        }
    }
}

/// Region-addressed access to the MemoryDB account container.
pub trait MemoryDbAccountsExt {
    /// The account's state in `region`, created (with that region's
    /// AWS-provided defaults) on first use.
    fn region_state_mut(&mut self, account_id: &str, region: &str) -> &mut MemoryDbState;

    /// The account's state in `region` for reading. A region nothing has
    /// written to yet still shows the AWS-provided defaults, without
    /// creating any state.
    fn region_state(&self, account_id: &str, region: &str) -> Cow<'_, MemoryDbState>;
}

impl MemoryDbAccountsExt for MultiAccountState<MemoryDbAccount> {
    fn region_state_mut(&mut self, account_id: &str, region: &str) -> &mut MemoryDbState {
        self.get_or_create(account_id).regions.region_mut(region)
    }

    fn region_state(&self, account_id: &str, region: &str) -> Cow<'_, MemoryDbState> {
        match self.get(account_id).and_then(|a| a.regions.region(region)) {
            Some(state) => Cow::Borrowed(state),
            None => Cow::Owned(MemoryDbState::new_for_account(account_id, region, "")),
        }
    }
}

/// `arn:<partition>:memorydb:<region>:<account>:<kind>/<name>`, in the
/// region's partition.
pub fn memorydb_arn(kind: &str, region: &str, account: &str, name: &str) -> String {
    Arn::regional("memorydb", region, account, &format!("{kind}/{name}")).to_string()
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
                arn: memorydb_arn("user", region, account_id, "default"),
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
                arn: memorydb_arn("acl", region, account_id, "open-access"),
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
                    arn: memorydb_arn("parametergroup", region, account_id, pg_name),
                    parameters: BTreeMap::new(),
                },
            );
        }
        s
    }
}

pub type SharedMemoryDbState = Arc<RwLock<MultiAccountState<MemoryDbAccount>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct MemoryDbSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<MemoryDbAccount>,
}

/// The per-account state v1 snapshots stored: every region's resources (and
/// the multi-region clusters) in one name-keyed state.
#[derive(Debug, Deserialize)]
pub(crate) struct LegacyMemoryDbState {
    #[serde(flatten)]
    regional: MemoryDbState,
    #[serde(default)]
    multi_region_clusters: BTreeMap<String, MultiRegionCluster>,
}

// Only so the legacy shape can sit in a `MultiAccountState` while it is
// migrated; nothing creates a legacy account.
impl AccountState for LegacyMemoryDbState {
    fn new_for_account(account_id: &str, region: &str, endpoint: &str) -> Self {
        Self {
            regional: MemoryDbState::new_for_account(account_id, region, endpoint),
            multi_region_clusters: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct LegacyMemoryDbSnapshot {
    pub(crate) accounts: MultiAccountState<LegacyMemoryDbState>,
}

impl LegacyMemoryDbState {
    /// Split a v1 account state into regions: every resource goes to the
    /// region its ARN names (the server default region when it names none),
    /// and its tags follow it. Multi-region clusters and their tags stay
    /// account-wide.
    pub(crate) fn into_account(
        self,
        account_id: &str,
        default_region: &str,
        endpoint: &str,
    ) -> MemoryDbAccount {
        use fakecloud_core::multi_account::arn_region_or;
        let mut account = MemoryDbAccount::new_for_account(account_id, default_region, endpoint);
        let MemoryDbState {
            clusters,
            acls,
            users,
            parameter_groups,
            subnet_groups,
            snapshots,
            reserved_nodes,
            mut tags,
        } = self.regional;
        for (name, mrc) in self.multi_region_clusters {
            if let Some(t) = tags.remove(&mrc.arn) {
                account.multi_region_cluster_tags.insert(mrc.arn.clone(), t);
            }
            account.multi_region_clusters.insert(name, mrc);
        }
        macro_rules! split {
            ($map:expr, $field:ident) => {
                for (name, item) in $map {
                    let region = arn_region_or(&item.arn, default_region).to_string();
                    account
                        .regions
                        .region_mut(&region)
                        .$field
                        .insert(name, item);
                }
            };
        }
        split!(clusters, clusters);
        split!(acls, acls);
        split!(users, users);
        split!(parameter_groups, parameter_groups);
        split!(subnet_groups, subnet_groups);
        split!(snapshots, snapshots);
        split!(reserved_nodes, reserved_nodes);
        for (arn, t) in tags {
            let region = arn_region_or(&arn, default_region).to_string();
            account.regions.region_mut(&region).tags.insert(arn, t);
        }
        account
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_region_seeds_its_own_defaults() {
        let mut accounts: MultiAccountState<MemoryDbAccount> =
            MultiAccountState::new("123456789012", "us-east-1", "");
        let west = accounts.region_state_mut("123456789012", "eu-west-1");
        assert_eq!(
            west.users["default"].arn,
            "arn:aws:memorydb:eu-west-1:123456789012:user/default"
        );
        let east = accounts.region_state("123456789012", "us-east-1");
        assert!(matches!(east, Cow::Owned(_)), "a read creates nothing");
        assert_eq!(
            east.acls["open-access"].arn,
            "arn:aws:memorydb:us-east-1:123456789012:acl/open-access"
        );
        assert_eq!(
            east.parameter_groups["default.memorydb-redis7"].arn,
            "arn:aws:memorydb:us-east-1:123456789012:parametergroup/default.memorydb-redis7"
        );
    }
}
