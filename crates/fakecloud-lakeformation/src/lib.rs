//! AWS Lake Formation (`lakeformation`) implementation for FakeCloud.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::{LakeFormationService, LAKEFORMATION_ACTIONS};
pub use state::{
    LakeFormationSnapshot, LakeFormationState, SharedLakeFormationState,
    LAKEFORMATION_SNAPSHOT_SCHEMA_VERSION,
};
