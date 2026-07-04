//! Account-partitioned, serializable state for AWS CodeConnections.
//!
//! Everything the service owns for one AWS account: connections, self-managed
//! hosts, repository links, and sync configurations. Tag maps live inline on
//! each taggable record (connections, hosts, repository links). Nested request
//! objects that fakecloud round-trips verbatim (a host's `VpcConfiguration`)
//! are stored as the raw request `Value`.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CODECONNECTIONS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Ordered key/value tag map.
pub type TagMap = BTreeMap<String, String>;

/// A connection to a third-party source-code provider.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionRecord {
    pub connection_arn: String,
    pub connection_name: String,
    /// One of the `ProviderType` enum values, or `None` when the connection was
    /// created against a host (the provider type is then carried by the host).
    #[serde(default)]
    pub provider_type: Option<String>,
    pub owner_account_id: String,
    /// `PENDING` | `AVAILABLE` | `ERROR`. A freshly-created connection is
    /// `PENDING` until its console handshake completes.
    pub connection_status: String,
    #[serde(default)]
    pub host_arn: Option<String>,
    #[serde(default)]
    pub tags: TagMap,
}

/// A self-managed host for an installed provider type (e.g. GitHub Enterprise
/// Server, GitLab Self Managed).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostRecord {
    pub host_arn: String,
    pub name: String,
    pub provider_type: String,
    pub provider_endpoint: String,
    /// `PENDING` | `AVAILABLE` | `VPC_CONFIG_INITIALIZING` |
    /// `VPC_CONFIG_FAILED_INITIALIZATION` | `VPC_CONFIG_DELETING` | `ERROR`.
    pub status: String,
    /// Raw `VpcConfiguration` value, round-tripped verbatim on describe.
    #[serde(default)]
    pub vpc_configuration: Option<Value>,
    #[serde(default)]
    pub tags: TagMap,
}

/// A link between a connection and a specific third-party repository.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryLinkRecord {
    pub repository_link_id: String,
    pub repository_link_arn: String,
    pub connection_arn: String,
    pub owner_id: String,
    pub repository_name: String,
    pub provider_type: String,
    #[serde(default)]
    pub encryption_key_arn: Option<String>,
    #[serde(default)]
    pub tags: TagMap,
}

/// A sync configuration (CloudFormation Git sync) tying a resource to a branch
/// of a linked repository. Keyed by `(sync_type, resource_name)`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfigurationRecord {
    pub branch: String,
    #[serde(default)]
    pub config_file: Option<String>,
    pub owner_id: String,
    pub provider_type: String,
    pub repository_link_id: String,
    pub repository_name: String,
    pub resource_name: String,
    pub role_arn: String,
    pub sync_type: String,
    #[serde(default)]
    pub publish_deployment_status: Option<String>,
    #[serde(default)]
    pub trigger_resource_update_on: Option<String>,
    #[serde(default)]
    pub pull_request_comment: Option<String>,
}

/// The account-scoped CodeConnections state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeConnectionsState {
    /// Connections keyed by connection ARN.
    #[serde(default)]
    pub connections: BTreeMap<String, ConnectionRecord>,
    /// Hosts keyed by host ARN.
    #[serde(default)]
    pub hosts: BTreeMap<String, HostRecord>,
    /// Repository links keyed by repository-link id.
    #[serde(default)]
    pub repository_links: BTreeMap<String, RepositoryLinkRecord>,
    /// Sync configurations keyed by `<syncType>\u{1}<resourceName>`.
    #[serde(default)]
    pub sync_configurations: BTreeMap<String, SyncConfigurationRecord>,
}

impl AccountState for CodeConnectionsState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCodeConnectionsState = Arc<RwLock<MultiAccountState<CodeConnectionsState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeConnectionsSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CodeConnectionsState>,
}

/// Composite key joiner using a control char that cannot appear in identifiers.
pub fn skey(sync_type: &str, resource_name: &str) -> String {
    format!("{sync_type}\u{1}{resource_name}")
}
