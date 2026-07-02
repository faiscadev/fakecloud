//! Account-partitioned, serializable state for AWS Cloud Map
//! (`servicediscovery`).
//!
//! Cloud Map lets applications register instances behind public DNS, private
//! DNS, or HTTP namespaces. This batch models the namespace control plane and
//! its asynchronous operation-tracking primitive: every create/update/delete
//! mints an [`Operation`] whose id the caller polls via `GetOperation`. Services
//! and instances (which register *into* a namespace) are layered on top in a
//! later batch — the state is structured so those maps slot in alongside
//! `namespaces`/`operations` without reshaping what exists here.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const SERVICEDISCOVERY_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Tags on a resource, keyed by resource ARN (namespaces/services), so the
/// later tag-ops batch works uniformly across every Cloud Map resource type.
pub type TagMap = BTreeMap<String, String>;

/// The DNS-facing portion of a namespace's `Properties` (public/private DNS
/// namespaces only). HTTP namespaces have no `DnsProperties`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsProps {
    pub hosted_zone_id: String,
    pub soa_ttl: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub id: String,
    pub arn: String,
    pub name: String,
    /// One of `HTTP`, `DNS_PUBLIC`, `DNS_PRIVATE` (the `NamespaceType` enum).
    pub type_: String,
    pub description: Option<String>,
    pub service_count: i32,
    /// The HTTP discovery name. AWS sets this to the namespace name for every
    /// namespace type, so DNS namespaces expose `HttpProperties.HttpName` too.
    pub http_name: String,
    /// Present for `DNS_PUBLIC`/`DNS_PRIVATE` namespaces only.
    pub dns: Option<DnsProps>,
    /// The VPC a private DNS namespace is associated with (input only — not
    /// echoed in the `Namespace` response shape, but retained for fidelity).
    pub vpc: Option<String>,
    pub creator_request_id: String,
    pub create_date: DateTime<Utc>,
}

/// An asynchronous Cloud Map operation. Create/update/delete calls return an
/// `OperationId` referencing one of these; the caller polls `GetOperation` to
/// observe it settle from `SUBMITTED` -> `SUCCESS`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operation {
    pub id: String,
    /// One of the `OperationType` enum values, e.g. `CREATE_NAMESPACE`.
    pub type_: String,
    /// One of `SUBMITTED`, `PENDING`, `SUCCESS`, `FAIL`.
    pub status: String,
    pub error_message: Option<String>,
    pub error_code: Option<String>,
    pub create_date: DateTime<Utc>,
    pub update_date: DateTime<Utc>,
    /// `OperationTargetType` -> resource id, e.g. `NAMESPACE -> ns-xxxx`.
    pub targets: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceDiscoveryState {
    /// Namespaces keyed by their `ns-...` id.
    pub namespaces: BTreeMap<String, Namespace>,
    /// Operations keyed by their operation id.
    pub operations: BTreeMap<String, Operation>,
    /// Tags keyed by resource ARN (populated by the later tag-ops batch).
    pub tags: BTreeMap<String, TagMap>,
}

impl AccountState for ServiceDiscoveryState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        // Cloud Map seeds no default namespaces — an account starts empty.
        Self::default()
    }
}

pub type SharedServiceDiscoveryState = Arc<RwLock<MultiAccountState<ServiceDiscoveryState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceDiscoverySnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<ServiceDiscoveryState>,
}
