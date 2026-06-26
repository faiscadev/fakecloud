pub mod advanced_prompt_optimization;
pub mod async_invoke;
pub mod automated_reasoning;
pub mod automated_reasoning_workflows;
pub mod converse;
pub mod custom_model_deployments;
pub mod custom_models;
pub mod customization;
pub mod data_retention;

pub mod enforced_guardrails;
pub mod evaluation;
pub mod faults;
pub mod foundation_model_agreements;
pub mod guardrail_checks;
pub mod guardrails;
pub mod inference_profiles;
pub mod invocation_jobs;
pub mod invoke;
pub mod logging;
pub mod marketplace;
pub mod model_copy;
pub mod model_import;
pub mod models;
pub mod prompt;
pub mod prompt_routers;
pub mod resource_policies;
pub(crate) mod runtime_validation;
pub(crate) mod service;
pub(crate) mod state;
pub mod streaming;
pub mod throughput;
pub(crate) mod validation;

/// Eight-character lowercase hex suffix derived from a fresh UUID.
///
/// Many Bedrock resources (guardrails, prompt routers, inference profiles,
/// custom models) generate a human-readable short identifier when the caller
/// doesn't supply one. We cut the UUID down to 8 characters so the resulting
/// name/id stays short and matches AWS's own convention for these resources.
pub(crate) fn short_uuid() -> String {
    uuid::Uuid::new_v4().to_string()[..8].to_string()
}

pub use service::BedrockService;
pub use state::{
    BedrockSnapshot, FaultRule, ResponseRule, SharedBedrockState, BEDROCK_SNAPSHOT_SCHEMA_VERSION,
};
