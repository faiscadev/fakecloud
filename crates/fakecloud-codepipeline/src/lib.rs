//! AWS CodePipeline (`codepipeline`) implementation for FakeCloud.
//!
//! awsJson1.1 control plane for CodePipeline: pipelines and their versions,
//! pipeline/action/rule executions, custom action types, webhooks, job and
//! third-party-job polling, stage transitions, approvals, and resource
//! tagging. There is no real release engine, so a `StartPipelineExecution`
//! mints a pipeline execution that lazily settles from `InProgress` to a
//! terminal `Succeeded` state on the reads that surface it
//! (`GetPipelineExecution`, `GetPipelineState`, `ListPipelineExecutions`) --
//! the same deterministic settle pattern CodeDeploy and CodeBuild use.
//! Everything else is real, persisted, account-partitioned CRUD.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::{CodePipelineService, CODEPIPELINE_ACTIONS};
pub use state::{
    CodePipelineSnapshot, CodePipelineState, SharedCodePipelineState,
    CODEPIPELINE_SNAPSHOT_SCHEMA_VERSION,
};
