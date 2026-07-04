//! AWS CodeConnections (`codeconnections`) awsJson1.0 service for fakecloud.
//!
//! The renamed successor to CodeStar Connections. Implements the full
//! 27-operation Smithy model: connections to third-party source providers
//! (GitHub, Bitbucket, GitLab, Azure DevOps, ...), self-managed hosts for
//! installed provider types (GitHub Enterprise Server, GitLab Self Managed),
//! repository links, sync configurations (CloudFormation Git sync), the sync
//! status / sync-blocker read surface, and resource tagging.
//!
//! A connection is created in the `PENDING` state: creating it only registers
//! the resource; completing the third-party OAuth handshake happens in the AWS
//! console, which fakecloud does not emulate, so a connection stays `PENDING`
//! until a real handshake it can never perform (the honest AWS default). There
//! is no backing source-provider integration or Git-sync engine, so the
//! sync-status / sync-blocker read paths report "not found" for resources that
//! were never synced, while everything else is real, account-partitioned,
//! persisted CRUD.

pub mod persistence;
pub mod service;
pub mod state;

pub use service::{CodeConnectionsService, CODECONNECTIONS_ACTIONS};
pub use state::{
    CodeConnectionsSnapshot, CodeConnectionsState, SharedCodeConnectionsState,
    CODECONNECTIONS_SNAPSHOT_SCHEMA_VERSION,
};
