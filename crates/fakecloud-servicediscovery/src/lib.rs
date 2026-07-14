//! AWS Cloud Map (`servicediscovery`) implementation for FakeCloud.

pub mod persistence;
pub(crate) mod service;
pub mod state;

pub use service::{ServiceDiscoveryService, SERVICEDISCOVERY_ACTIONS};
pub use state::{
    Service, ServiceDiscoverySnapshot, ServiceDiscoveryState, SharedServiceDiscoveryState,
    SERVICEDISCOVERY_SNAPSHOT_SCHEMA_VERSION,
};
