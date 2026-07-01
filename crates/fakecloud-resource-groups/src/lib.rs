//! AWS Resource Groups (`resource-groups`) restJson1 service for fakecloud.
//!
//! Batch 1: the full 23-operation control plane from the AWS Smithy model --
//! group lifecycle (Create/Get/Update/Delete/ListGroups), resource queries
//! (GetGroupQuery/UpdateGroupQuery), explicit membership
//! (GroupResources/UngroupResources/ListGroupResources), search
//! (SearchResources), group configuration (Get/PutGroupConfiguration), tagging
//! (Tag/Untag/GetTags), account settings (Get/UpdateAccountSettings), grouping
//! statuses (ListGroupingStatuses), and tag-sync tasks
//! (Start/Get/List/CancelTagSyncTask). Every operation is backed by real,
//! account-partitioned, persisted state with AWS-shaped
//! `arn:aws:resource-groups:...:group/<name>/<id>` ARNs.
//!
//! Query-based membership resolution (evaluating a `TAG_FILTERS_1_0` /
//! `CLOUDFORMATION_STACK_1_0` query against live resources) depends on the
//! cross-service tag index that ships with the Resource Groups Tagging API
//! batch; until then a query-based group resolves to its explicitly associated
//! members. Explicit `GroupResources` membership is fully real now.

pub mod persistence;
pub mod service;
pub mod state;

pub use service::ResourceGroupsService;
pub use state::{
    ResourceGroupsState, SharedResourceGroupsState, RESOURCE_GROUPS_SNAPSHOT_SCHEMA_VERSION,
};
