//! AWS Cloud Map (`servicediscovery`) implementation for FakeCloud.

pub mod persistence;
pub(crate) mod service;
pub(crate) mod state;

pub use service::{ServiceDiscoveryService, SERVICEDISCOVERY_ACTIONS};
pub use state::{
    Service, ServiceDiscoverySnapshot, ServiceDiscoveryState, SharedServiceDiscoveryState,
    SERVICEDISCOVERY_SNAPSHOT_SCHEMA_VERSION,
};
// Re-exported for the CloudFormation resource_provisioner; `state` is pub(crate).
pub use state::{
    DnsConfig, DnsProps, DnsRecord, HealthCheckConfig, HealthCheckCustomConfig, Instance, Namespace,
};
