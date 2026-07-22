//! Account-partitioned, serializable state for Amazon OpenSearch Service and
//! Amazon Elasticsearch Service.
//!
//! Both APIs are backed by ONE domain store: a [`Domain`] is a single entity
//! whether it was created through `CreateElasticsearchDomain` (2015) or
//! `CreateDomain` (2021), and it is visible through either API's Describe/List
//! operations. The service maps the shared struct onto each API's response
//! shape (`ElasticsearchDomainStatus` vs the superset `DomainStatus`).

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const OPENSEARCH_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Tags on a resource, stored by ARN so tag ops work uniformly across both
/// APIs (which share one ARN space).
pub type TagMap = BTreeMap<String, String>;

/// A search domain — the single shared entity behind both APIs. Optional
/// config blobs are kept verbatim (as the JSON the caller sent) in `config`
/// so a Describe round-trips the caller's input faithfully; config updates
/// merge into the same map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Domain {
    pub name: String,
    pub domain_id: String,
    pub arn: String,
    /// The engine version string (e.g. `OpenSearch_2.11` or
    /// `Elasticsearch_7.10`).
    pub engine_version: String,
    /// Whether the domain was created through the legacy 2015 Elasticsearch
    /// API. Only affects the default engine-version prefix at create time; a
    /// domain is visible through both APIs regardless.
    pub created_via_es: bool,
    /// The synthetic domain search endpoint.
    pub endpoint: String,
    /// Settles to `true` once the domain finishes its (synthetic) creation on
    /// first Describe, mirroring the real control plane's eventual creation.
    pub created: bool,
    pub deleted: bool,
    /// The remaining create-input members, kept verbatim keyed by member name
    /// (both the 2015 and 2021 spellings are accepted and stored as sent).
    pub config: BTreeMap<String, Value>,
    pub tags: TagMap,
    pub created_at: DateTime<Utc>,
    /// Per-domain data sources (S3 glue) keyed by name.
    #[serde(default)]
    pub data_sources: BTreeMap<String, Value>,
    /// Per-domain indices keyed by index name.
    #[serde(default)]
    pub indices: BTreeMap<String, Value>,
    /// Per-domain scheduled actions keyed by id.
    #[serde(default)]
    pub scheduled_actions: BTreeMap<String, Value>,
    /// Per-domain maintenance actions keyed by id.
    #[serde(default)]
    pub maintenances: BTreeMap<String, Value>,
    /// Current `ServiceSoftwareOptions.UpdateStatus` after a
    /// Start/Cancel/RollbackServiceSoftwareUpdate. `None` means the default
    /// `NOT_ELIGIBLE` (no update in flight) is reported.
    #[serde(default)]
    pub service_software_status: Option<String>,
}

/// An installable package (dictionary / plugin / config) shared by both APIs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Package {
    pub id: String,
    pub name: String,
    pub package_type: String,
    pub description: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub available_version: String,
    pub source: Value,
    /// Version history keyed by package version.
    #[serde(default)]
    pub versions: BTreeMap<String, Value>,
    /// Domains this package is associated with, keyed by domain name.
    #[serde(default)]
    pub associations: BTreeMap<String, Value>,
    #[serde(default)]
    pub scope: Value,
}

/// A VPC endpoint fronting a domain, shared by both APIs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VpcEndpoint {
    pub id: String,
    pub domain_arn: String,
    pub status: String,
    pub vpc_options: Value,
    pub endpoint: String,
    pub account_id: String,
    /// Accounts authorized to access this endpoint, keyed by account id.
    #[serde(default)]
    pub authorized_principals: BTreeMap<String, Value>,
}

/// A cross-cluster search / cross-cluster connection (inbound or outbound),
/// shared by both APIs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub id: String,
    pub source: Value,
    pub destination: Value,
    pub status_code: String,
    pub status_message: String,
    pub alias: String,
    pub mode: String,
    pub properties: Value,
}

/// An OpenSearch application (2021 API only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Application {
    pub id: String,
    pub arn: String,
    pub name: String,
    pub status: String,
    pub endpoint: String,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
    pub data_sources: Value,
    pub iam_identity_center_options: Value,
    pub app_configs: Value,
    pub tags: TagMap,
    /// Capabilities registered on this application keyed by capability name.
    #[serde(default)]
    pub capabilities: BTreeMap<String, Value>,
    /// Data-source attachments keyed by attachmentId (AttachDataSource).
    #[serde(default)]
    pub attachments: BTreeMap<String, Value>,
    /// Whether this application is registered as the account default
    /// (PutDefaultApplicationSetting).
    #[serde(default)]
    pub is_default_setting: bool,
}

/// An application migration (2021 API only), account-scoped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Migration {
    pub migration_id: String,
    pub application_id: String,
    pub status: String,
    /// The `MigrationSource` JSON (`{ "datasourceArn": "..." }`).
    pub source: Value,
    pub exported_count: i64,
    pub imported_count: i64,
    /// Optional `MigrationError` JSON (`{ "code": "...", "message": "..." }`).
    #[serde(default)]
    pub error: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A direct-query data source (2021 API), account-scoped.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DirectQueryDataSource {
    pub name: String,
    pub arn: String,
    pub data_source_type: Value,
    pub description: String,
    pub open_search_arns: Value,
    pub tag_list: Value,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OpenSearchState {
    pub domains: BTreeMap<String, Domain>,
    /// Tags keyed by resource ARN (domains carry their own tags; this map
    /// holds tags for any other `es`/`opensearch` ARN).
    #[serde(default)]
    pub tags: BTreeMap<String, TagMap>,
    #[serde(default)]
    pub packages: BTreeMap<String, Package>,
    #[serde(default)]
    pub vpc_endpoints: BTreeMap<String, VpcEndpoint>,
    #[serde(default)]
    pub connections: BTreeMap<String, Connection>,
    #[serde(default)]
    pub applications: BTreeMap<String, Application>,
    #[serde(default)]
    pub direct_query_data_sources: BTreeMap<String, DirectQueryDataSource>,
    /// Application migrations keyed by migration id.
    #[serde(default)]
    pub migrations: BTreeMap<String, Migration>,
    /// Purchased reserved instances keyed by reservation id.
    #[serde(default)]
    pub reserved_instances: BTreeMap<String, Value>,
    /// VPC-endpoint-access principals authorized per domain: domain name ->
    /// principal key (account id or service SP) -> `AuthorizedPrincipal` JSON.
    #[serde(default)]
    pub vpc_endpoint_access: BTreeMap<String, BTreeMap<String, Value>>,
}

impl AccountState for OpenSearchState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

/// Build the ARN for a domain. Both APIs use the `es` service namespace.
pub fn domain_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:es:{region}:{account_id}:domain/{name}")
}

pub type SharedOpenSearchState = Arc<RwLock<MultiAccountState<OpenSearchState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenSearchSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<OpenSearchState>,
}
