//! Amazon EMR (`elasticmapreduce`) awsJson1_1 service for fakecloud.
//!
//! Batch 1 (control plane): the vendored Smithy model + crate scaffold +
//! awsJson1_1 routing for all operations + the full control plane (clusters via
//! RunJobFlow, steps, instance groups/fleets, instances, bootstrap actions,
//! security configurations, EMR Studio + session mappings, notebook executions,
//! persistent app UIs, interactive sessions, block-public-access, auto-scaling /
//! managed-scaling / auto-termination policies, release labels, and tags), with
//! model-driven input validation and account-partitioned persistence.
//!
//! The Spark/Hadoop data plane (a real EMR cluster running jobs in containers)
//! is a later batch: here a `RunJobFlow` settles to a `WAITING` cluster via the
//! control-plane state machine, honestly, and every operation is real,
//! persisted CRUD -- no stubbed success responses.

pub mod persistence;
pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod validate;

pub use service::EmrService;
pub use state::{EmrSnapshot, EmrState, SharedEmrState, EMR_SNAPSHOT_SCHEMA_VERSION};
