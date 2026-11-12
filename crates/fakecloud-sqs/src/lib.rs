pub mod delivery;
pub mod resource_policy;
pub(crate) mod service;
pub mod simulation;
pub mod sse_sqs;
pub(crate) mod state;

pub use service::helpers::{
    is_valid_queue_name, parse_redrive_policy, plaintext_body, validate_create_queue_attributes,
};
pub use service::helpers::{render_queue_url, resolve_endpoint_base, resolve_endpoint_base_with};
pub use service::SqsService;
pub use state::{
    parse_sqs_snapshot, queue_encrypts_at_rest, RedrivePolicy, SharedSqsState, SqsMessage,
    SqsQueue, SqsSnapshot, SqsState, SQS_SNAPSHOT_SCHEMA_VERSION,
};
