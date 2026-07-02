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

/// A single DNS record template in a service's `DnsConfig` — the record type
/// (`A`/`AAAA`/`SRV`/`CNAME`) and the TTL Cloud Map applies when it materializes
/// the record for a registered instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsRecord {
    /// One of the `RecordType` enum values.
    pub type_: String,
    pub ttl: i64,
}

/// The DNS routing configuration of a service (public/private DNS namespaces).
/// HTTP-only services carry no `DnsConfig`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DnsConfig {
    pub namespace_id: Option<String>,
    /// One of the `RoutingPolicy` enum values (`MULTIVALUE`/`WEIGHTED`).
    pub routing_policy: Option<String>,
    pub dns_records: Vec<DnsRecord>,
}

/// A Route 53 health check attached to a service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    /// One of the `HealthCheckType` enum values (`HTTP`/`HTTPS`/`TCP`).
    pub type_: String,
    pub resource_path: Option<String>,
    pub failure_threshold: Option<i32>,
}

/// A custom (third-party) health check configuration attached to a service.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckCustomConfig {
    pub failure_threshold: Option<i32>,
}

/// An instance registered into a service (via `RegisterInstance`). Cloud Map
/// keys instances by the caller-supplied `InstanceId` within a service and
/// materializes DNS records / health status from its `Attributes` map (the
/// well-known `AWS_INSTANCE_*` keys plus arbitrary custom attributes).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instance {
    pub id: String,
    pub creator_request_id: String,
    /// Arbitrary + well-known attributes (`AWS_INSTANCE_IPV4`, `AWS_INSTANCE_PORT`,
    /// `AWS_INSTANCE_CNAME`, `AWS_ALIAS_DNS_NAME`, `AWS_INSTANCE_IPV6`,
    /// `AWS_EC2_INSTANCE_ID`, and custom keys).
    pub attributes: BTreeMap<String, String>,
    /// Current health status, one of the `HealthStatus` enum values
    /// (`HEALTHY`/`UNHEALTHY`/`UNKNOWN`). Instances start `HEALTHY`; a
    /// custom-health service updates it via `UpdateInstanceCustomHealthStatus`.
    pub health: String,
}

/// A Cloud Map service: instances register *into* a service, which lives within
/// a namespace. Created synchronously (unlike namespaces), so `CreateService`
/// returns the full `Service` shape rather than an `OperationId`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Service {
    pub id: String,
    pub arn: String,
    pub name: String,
    /// The `ns-...` id of the parent namespace.
    pub namespace_id: String,
    /// One of the `ServiceType` enum values (`HTTP`/`DNS_HTTP`/`DNS`).
    pub type_: String,
    pub description: Option<String>,
    /// Count of instances currently registered against the service (0 until the
    /// instances batch lands).
    pub instance_count: i32,
    pub dns_config: Option<DnsConfig>,
    pub health_check_config: Option<HealthCheckConfig>,
    pub health_check_custom_config: Option<HealthCheckCustomConfig>,
    /// Service-level attributes (`GetServiceAttributes`/`UpdateServiceAttributes`).
    pub attributes: BTreeMap<String, String>,
    pub creator_request_id: String,
    pub create_date: DateTime<Utc>,
    /// Instances registered into this service, keyed by `InstanceId`.
    #[serde(default)]
    pub instances: BTreeMap<String, Instance>,
    /// Monotonic counter bumped on every register/deregister; surfaced by
    /// `DiscoverInstances`/`DiscoverInstancesRevision` so clients can detect a
    /// changed instance set without diffing.
    #[serde(default)]
    pub instances_revision: i64,
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
    /// Services keyed by their `srv-...` id.
    #[serde(default)]
    pub services: BTreeMap<String, Service>,
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
