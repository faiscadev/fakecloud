//! AWS CodeBuild (`codebuild`) implementation for FakeCloud.
//!
//! awsJson1.1 control plane for CodeBuild: build projects, builds and build
//! batches, report groups and reports, fleets, webhooks, source credentials,
//! resource policies, and command-execution sandboxes. There is no real build
//! container engine, so a `StartBuild` mints a [`state`] Build that lazily
//! settles from `IN_PROGRESS` to a terminal `SUCCEEDED` state on read (the same
//! deterministic settle pattern EKS and Cloud Map use); everything else is
//! real, persisted, account-partitioned CRUD.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::{CodeBuildService, CODEBUILD_ACTIONS};
pub use state::{
    CodeBuildSnapshot, CodeBuildState, SharedCodeBuildState, CODEBUILD_SNAPSHOT_SCHEMA_VERSION,
};
