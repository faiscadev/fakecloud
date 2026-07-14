//! AWS EKS (`eks`) implementation for FakeCloud.

pub mod persistence;
pub(crate) mod service;
pub mod state;

pub use service::{EksService, EKS_ACTIONS};
pub use state::{EksSnapshot, EksState, SharedEksState, EKS_SNAPSHOT_SCHEMA_VERSION};
