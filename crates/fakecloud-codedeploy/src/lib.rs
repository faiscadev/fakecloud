//! AWS CodeDeploy (`codedeploy`) implementation for FakeCloud.
//!
//! awsJson1.1 control plane for CodeDeploy: applications, application
//! revisions, deployment groups, deployment configurations, deployments and
//! their targets/instances, on-premises instances, GitHub account tokens, and
//! resource tagging. There is no real deployment engine, so a `CreateDeployment`
//! mints a [`state`] deployment that lazily settles from `Created` through
//! `InProgress` to a terminal `Succeeded` state on read (the same deterministic
//! settle pattern CodeBuild, EKS, and Cloud Map use); everything else is real,
//! persisted, account-partitioned CRUD. The predefined `CodeDeployDefault.*`
//! deployment configurations are always resolvable.

pub mod persistence;
pub(crate) mod service;
pub(crate) mod state;
mod validate;

pub use service::{CodeDeployService, CODEDEPLOY_ACTIONS};
pub use state::{
    CodeDeploySnapshot, CodeDeployState, SharedCodeDeployState, CODEDEPLOY_SNAPSHOT_SCHEMA_VERSION,
};
