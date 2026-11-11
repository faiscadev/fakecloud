//! Lambda durable functions (durable executions, 2025-12-01 API).
//!
//! Invoking a function that has a `DurableConfig` starts a durable
//! execution. The service then runs the function with the durable payload
//! (`DurableExecutionArn`, `CheckpointToken`, `InitialExecutionState`,
//! `UpdatedOperationIds`), and the official durable execution SDK inside
//! the function checkpoints its progress back through
//! `CheckpointDurableExecution`. When the function suspends (`PENDING`) the
//! service re-invokes it once a wait elapses, a step retry comes due, a
//! callback is completed or times out, or a chained invoke returns; the SDK
//! replays the recorded operations and continues.
//!
//! - [`model`]: the persisted execution, its operations and wire encodings.
//! - [`validate`]: checkpoint update validation.
//! - [`engine`]: the synchronous state machine.
//! - [`runner`]: the scheduler that invokes the function and fires timers.
//! - [`api`]: the HTTP operations and the `Invoke` integration.

pub(crate) mod api;
pub(crate) mod config;
pub(crate) mod engine;
pub(crate) mod model;
pub(crate) mod runner;
pub(crate) mod validate;

pub use model::DurableExecution;
pub use runner::DurableHub;
