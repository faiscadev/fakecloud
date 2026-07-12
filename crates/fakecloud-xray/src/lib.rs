//! AWS X-Ray (`xray`) restJson1 control plane + in-memory trace data plane for
//! fakecloud.
//!
//! The full 38-operation AWS X-Ray Smithy model. X-Ray signs SigV4 with the
//! `xray` scope and speaks restJson1; every operation is a `POST /<UriPath>`
//! (e.g. `POST /TraceSegments`, `POST /Traces`, `POST /ServiceGraph`), so
//! requests are routed by their `@http` URI path.
//!
//! This is real, persisted, account-partitioned state, not a set of stubs:
//!
//! * **Data plane.** `PutTraceSegments` ingests X-Ray trace segment documents
//!   (JSON), parsing each document's `trace_id` / `id` / `name` / `start_time` /
//!   `end_time` / `http` / `error`/`fault`/`throttle` flags and its nested
//!   `subsegments` (including `namespace: "remote"` downstream calls). Segments
//!   are stored keyed by trace id. `BatchGetTraces` reassembles stored traces,
//!   `GetTraceSummaries` filters them by time range (plus a documented
//!   filter-expression subset), and `GetServiceGraph` / `GetTraceGraph` /
//!   `GetTimeSeriesServiceStatistics` derive a service graph (nodes + edges with
//!   Ok/Error/Fault statistics and response time) computed deterministically
//!   from the ingested segments. See [`graph`] and [`segment`].
//! * **Control plane.** Sampling rules (with the built-in, undeletable `Default`
//!   rule seeded per account), groups, the account encryption config, resource
//!   policies, the indexing rule, the trace-segment destination, and ARN-keyed
//!   resource tagging are straightforward CRUD, persisted via snapshot/restore.
//!
//! Model-driven validation rejects contract violations with the error codes each
//! operation declares (`InvalidRequestException` universally,
//! `ResourceNotFoundException` on the ops that declare it, plus
//! `RuleLimitExceededException` / `TooManyTagsException`).

pub mod graph;
pub mod persistence;
pub mod segment;
pub mod service;
pub mod state;
mod validate;

pub use service::{XrayService, XRAY_ACTIONS};
pub use state::{SharedXrayState, XrayData, XraySnapshot, XRAY_SNAPSHOT_SCHEMA_VERSION};
