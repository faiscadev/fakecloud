pub mod delivery;
pub mod resource_policy;
pub(crate) mod service;
pub mod simulation;
pub(crate) mod state;

pub use service::helpers::{parse_redrive_policy, validate_create_queue_attributes};
pub use service::helpers::{render_queue_url, resolve_endpoint_base, resolve_endpoint_base_with};
pub use service::SqsService;
pub use state::{
    parse_sqs_snapshot, RedrivePolicy, SharedSqsState, SqsMessage, SqsQueue, SqsSnapshot, SqsState,
    SQS_SNAPSHOT_SCHEMA_VERSION,
};
