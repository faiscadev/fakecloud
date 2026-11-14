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

use fakecloud_aws::arn::Arn;
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
    /// The most recent applied `UpdateDomainConfig` change, reported by
    /// `DescribeDomainChangeProgress`.
    #[serde(default)]
    pub last_change: Option<ConfigChange>,
    /// The most recent `UpdateDomainConfig` dry run, reported by
    /// `DescribeDryRunProgress`.
    #[serde(default)]
    pub last_dry_run: Option<DryRunRecord>,
    /// Earlier dry runs, oldest first and capped at `MAX_DRY_RUN_HISTORY`, so
    /// `DescribeDryRunProgress` can report a specific `DryRunId`.
    #[serde(default)]
    pub dry_runs: Vec<DryRunRecord>,
    /// The live backing cluster, set once it answers `_cluster/health`. `None`
    /// without a container runtime (control-plane-only domains) and while the
    /// cluster is being created or replaced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_plane: Option<DomainDataPlane>,
    /// The `DomainProcessingStatus` (`Creating`, `Modifying`,
    /// `UpgradingEngineVersion`) while the backing cluster is being brought
    /// up or replaced; `None` once settled. Only set when a container runtime
    /// backs the domain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub processing: Option<String>,
    /// Why the last bring-up of the backing cluster failed. The domain then
    /// stays control-plane only until its next configuration change or
    /// restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_plane_error: Option<String>,
    /// Whether the bring-up in progress (`processing`) was started by a
    /// configuration change (`last_change`), as opposed to a create or a
    /// restart's recovery: only then is that change reported in progress.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub change_pending: bool,
    /// This domain's side of the cross-Region replication relationship it
    /// takes part in (`AddReplicationPeer`), if any. Both domains of a
    /// relationship carry a link naming the other, with the same status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<ReplicationLink>,
}

/// One domain's side of a replication relationship.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationLink {
    /// The ARN of the other domain of the relationship.
    pub peer_arn: String,
    /// This domain's `ReplicationRole` (`PRIMARY` or `SECONDARY`).
    pub role: String,
    /// The relationship's `ReplicationStatus`.
    pub status: String,
    /// Why the relationship is in its current state, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_message: Option<String>,
}

impl OpenSearchState {
    /// Mark the relationship of the domain peered with `deleted_arn` as
    /// `DETACHED`: the domain it replicated with no longer exists.
    pub fn detach_replication_peer_of(&mut self, deleted_arn: &str) {
        for d in self.domains.values_mut() {
            if let Some(link) = d.replication.as_mut() {
                if link.peer_arn == deleted_arn {
                    link.status = "DETACHED".to_string();
                    link.status_message =
                        Some(format!("The peer domain {deleted_arn} was deleted."));
                }
            }
        }
    }
}

/// Where a domain's backing search cluster listens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DomainDataPlane {
    pub container_id: String,
    /// The address fakecloud reaches the published port at (`127.0.0.1`, or
    /// the sibling host alias when fakecloud is containerized).
    pub host: String,
    pub port: u16,
}

/// How many dry runs a domain remembers for `DescribeDryRunProgress`.
pub const MAX_DRY_RUN_HISTORY: usize = 100;

/// One applied domain configuration change.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigChange {
    pub change_id: String,
    pub start_time: DateTime<Utc>,
    /// The config members the change touched (`ClusterConfig`, ...).
    pub properties: Vec<String>,
    /// Advisory warning codes the caller accepted (`AcceptedWarnings`).
    #[serde(default)]
    pub accepted_warnings: Vec<String>,
}

/// One `UpdateDomainConfig` dry run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunRecord {
    pub dry_run_id: String,
    pub created_at: DateTime<Utc>,
    #[serde(default)]
    pub accepted_warnings: Vec<String>,
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
    Arn::regional("es", region, account_id, &format!("domain/{name}")).to_string()
}

/// Build the ARN for an OpenSearch UI application.
pub fn application_arn(region: &str, account_id: &str, id: &str) -> String {
    Arn::regional("es", region, account_id, &format!("application/{id}")).to_string()
}

/// Build the ARN for a direct-query data source.
pub fn direct_query_data_source_arn(region: &str, account_id: &str, name: &str) -> String {
    Arn::regional(
        "es",
        region,
        account_id,
        &format!("directquerydatasource/{name}"),
    )
    .to_string()
}

impl Domain {
    /// Where a client can reach this domain's cluster: the backing container's
    /// published HTTP port once it answers health. `None` for a
    /// control-plane-only domain (no container runtime, an engine version
    /// with no image, a failed bring-up) and while the cluster is being
    /// created or replaced; `endpoint` is the AWS-format name, which only
    /// fakecloud's own `Host`-routed proxy serves.
    pub fn reachable_url(&self) -> Option<String> {
        if self.deleted {
            return None;
        }
        self.data_plane
            .as_ref()
            .map(|dp| format!("http://{}:{}", dp.host, dp.port))
    }

    /// The account the domain belongs to, from its ARN.
    pub fn account_id(&self) -> &str {
        fakecloud_aws::arn::account_of(&self.arn).unwrap_or_default()
    }

    /// Stable id of this incarnation of the domain: a delete and a recreate
    /// under the same name get different ids, so their data volumes never
    /// collide.
    pub fn incarnation(&self) -> String {
        fakecloud_core::data_volume::incarnation_id(&[&self.arn, &self.created_at.to_rfc3339()])
    }

    /// The named volume holding the backing cluster's data directory, in the
    /// volume scope `scope_tag` (`fakecloud_core::data_volume`).
    pub fn data_volume_name(&self, scope_tag: &str) -> String {
        fakecloud_core::data_volume::scoped_volume_name(
            "opensearch",
            scope_tag,
            &[self.account_id(), &self.name, &self.incarnation()],
        )
    }
}

/// The live domain an ARN names, in the account the ARN names.
pub fn domain_by_arn<'a>(
    accounts: &'a MultiAccountState<OpenSearchState>,
    arn: &str,
) -> Option<&'a Domain> {
    let account = fakecloud_aws::arn::account_of(arn)?;
    accounts
        .get(account)?
        .domains
        .values()
        .find(|domain| domain.arn == arn && !domain.deleted)
}

pub type SharedOpenSearchState = Arc<RwLock<MultiAccountState<OpenSearchState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct OpenSearchSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<OpenSearchState>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arns_take_the_region_partition() {
        assert_eq!(
            domain_arn("us-east-1", "123456789012", "d"),
            "arn:aws:es:us-east-1:123456789012:domain/d"
        );
        assert_eq!(
            domain_arn("cn-north-1", "123456789012", "d"),
            "arn:aws-cn:es:cn-north-1:123456789012:domain/d"
        );
        assert_eq!(
            application_arn("cn-north-1", "123456789012", "a1"),
            "arn:aws-cn:es:cn-north-1:123456789012:application/a1"
        );
        assert_eq!(
            direct_query_data_source_arn("cn-north-1", "123456789012", "ds"),
            "arn:aws-cn:es:cn-north-1:123456789012:directquerydatasource/ds"
        );
    }
}
