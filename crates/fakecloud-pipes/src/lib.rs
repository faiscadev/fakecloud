//! AWS EventBridge Pipes (`pipes`) restJson1 service for fakecloud.
//!
//! Batch 1 (control plane): vendored Smithy model + crate scaffold + restJson1
//! path routing for all 10 operations + the core control plane (CreatePipe /
//! DescribePipe / ListPipes / UpdatePipe / DeletePipe / StartPipe / StopPipe +
//! tags), a faithful lifecycle state machine (CREATING -> RUNNING/STOPPED,
//! UPDATING, STARTING, STOPPING, DELETING) driven off a short async tick, and
//! snapshot persistence + restart recovery. Real source->enrichment->target
//! execution lands in later batches; a created pipe parks at its settled
//! control-plane state honestly (no fake event delivery yet).

pub mod service;
pub mod state;

pub use service::{
    drain_overdue_transient_pipes, ensure_source_param_defaults, normalize_empty_input_templates,
    validate_pipe_name, validate_resource_arn_len, PipesService,
};
pub use state::{
    PipesAccounts, PipesSnapshot, PipesState, SharedPipesState, PIPES_SNAPSHOT_SCHEMA_VERSION,
};
