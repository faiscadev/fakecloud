//! AWS Resource Groups Tagging API (`tagging`) implementation for FakeCloud.

pub mod persistence;
pub mod service;
pub mod state;

pub use service::{ApiTagProvider, ResourceGroupsTaggingService, RESOURCE_GROUPS_TAGGING_ACTIONS};
pub use state::{
    ResourceGroupsTaggingSnapshot, ResourceGroupsTaggingState, SharedResourceGroupsTaggingState,
    RESOURCE_GROUPS_TAGGING_SNAPSHOT_SCHEMA_VERSION,
};
