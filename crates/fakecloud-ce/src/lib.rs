//! AWS Cost Explorer (`ce`) awsJson1.1 control plane for fakecloud.
//!
//! The full 47-operation Cost Explorer Smithy model
//! (`com.amazonaws.costexplorer#AWSInsightsIndexService`): anomaly monitors and
//! subscriptions, cost category definitions and their resource associations,
//! cost allocation tags (status + backfill history), resource tagging,
//! commitment-purchase and savings-plans recommendation analyses, and the full
//! cost/usage/reservation/savings-plans analytics surface.
//!
//! Cost Explorer reports BILLING data. A fake account incurs no real AWS cost,
//! so the honest, faithful state is a fresh account with zero recorded usage —
//! the same framing fakecloud uses for CloudTrail ("no event-recording
//! engine"). Concretely this splits into two halves:
//!
//! * Fully real, account-partitioned, persisted CRUD for every stored
//!   resource: anomaly monitors/subscriptions, cost categories (+ versioning
//!   on update), cost-allocation-tag status + backfill requests, tags, anomaly
//!   feedback, and analysis/generation requests that settle to a terminal
//!   status so `List`/`Get` reflect them. Every `Create` is echoed by its
//!   `Get`/`Describe`, every `Update` persists, every `Delete` deletes.
//! * Structurally-valid zero/empty analytics for the reporting operations:
//!   the model's exact output shape, correctly typed, with empty result series
//!   and zeroed-but-present required aggregate objects (`ResultsByTime`,
//!   forecasts, coverage, utilization, recommendations). This is the "no usage
//!   recorded" honest state, not a stub.
//!
//! Model-derived input validation (`@required`, `@length`, `@range`, enum
//! value sets) rejects malformed requests with a 4xx so negative variants get
//! the right error.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::CeService;
pub use state::{CeData, SharedCeState, CE_SNAPSHOT_SCHEMA_VERSION};
