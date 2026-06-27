//! AWS Batch (`batch`) restJson1 service for fakecloud.
//!
//! Batch 0 (foundation): vendored Smithy model + crate scaffold + restJson1
//! routing for all 45 operations + the core control plane (compute
//! environments, job queues, job definitions, tags). Real container-backed job
//! execution (SubmitJob -> ECS task) and the remaining resource families land
//! in later batches; unimplemented operations return a faithful
//! `ServerException`/not-implemented error rather than a fake success.

pub mod service;
pub mod state;

pub use service::BatchService;
pub use state::{
    BatchAccounts, BatchSnapshot, BatchState, SharedBatchState, BATCH_SNAPSHOT_SCHEMA_VERSION,
};
