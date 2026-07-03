//! Amazon S3 Glacier (`glacier`) implementation for FakeCloud.

pub mod persistence;
pub mod service;
pub mod state;
pub mod tree_hash;

pub use service::{GlacierService, GLACIER_ACTIONS};
pub use state::{
    GlacierSnapshot, GlacierState, SharedGlacierState, GLACIER_SNAPSHOT_SCHEMA_VERSION,
};
