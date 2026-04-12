use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;

pub type SharedBedrockState = Arc<RwLock<BedrockState>>;

pub struct BedrockState {
    pub account_id: String,
    pub region: String,
    /// Tags keyed by resource ARN.
    pub tags: HashMap<String, HashMap<String, String>>,
    /// Guardrails keyed by guardrail ID.
    pub guardrails: HashMap<String, Guardrail>,
    /// Guardrail versions keyed by (guardrail_id, version).
    pub guardrail_versions: HashMap<(String, String), GuardrailVersion>,
    /// Model customization jobs keyed by job ARN.
    pub customization_jobs: HashMap<String, CustomizationJob>,
    /// Provisioned model throughputs keyed by provisioned model ID.
    pub provisioned_throughputs: HashMap<String, ProvisionedThroughput>,
    /// Model invocation logging configuration.
    pub logging_config: Option<LoggingConfig>,
    /// All model invocations recorded for introspection.
    pub invocations: Vec<ModelInvocation>,
    /// Custom responses configured per model ID via simulation endpoint.
    pub custom_responses: HashMap<String, String>,
    /// Async invocations keyed by invocation ARN.
    pub async_invocations: HashMap<String, AsyncInvocation>,
    /// Custom models keyed by model ARN.
    pub custom_models: HashMap<String, CustomModel>,
    /// Custom model deployments keyed by deployment ARN.
    pub custom_model_deployments: HashMap<String, CustomModelDeployment>,
    /// Model import jobs keyed by job ARN.
    pub model_import_jobs: HashMap<String, ModelImportJob>,
    /// Imported models keyed by model ARN.
    pub imported_models: HashMap<String, ImportedModel>,
    /// Model copy jobs keyed by job ARN.
    pub model_copy_jobs: HashMap<String, ModelCopyJob>,
    /// Model invocation jobs (batch inference) keyed by job ARN.
    pub model_invocation_jobs: HashMap<String, ModelInvocationJob>,
    /// Evaluation jobs keyed by job ARN.
    pub evaluation_jobs: HashMap<String, EvaluationJob>,
    /// Inference profiles keyed by ARN.
    pub inference_profiles: HashMap<String, InferenceProfile>,
    /// Prompt routers keyed by ARN.
    pub prompt_routers: HashMap<String, PromptRouter>,
    /// Resource policies keyed by resource ARN.
    pub resource_policies: HashMap<String, String>,
    /// Marketplace model endpoints keyed by endpoint ARN.
    pub marketplace_endpoints: HashMap<String, MarketplaceModelEndpoint>,
    /// Foundation model agreements keyed by agreement ID.
    pub foundation_model_agreements: HashMap<String, FoundationModelAgreement>,
    /// Use case for model access.
    pub use_case_for_model_access: Option<serde_json::Value>,
    /// Enforced guardrail configurations keyed by config ID.
    pub enforced_guardrail_configs: HashMap<String, serde_json::Value>,
}

impl BedrockState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            tags: HashMap::new(),
            guardrails: HashMap::new(),
            guardrail_versions: HashMap::new(),
            customization_jobs: HashMap::new(),
            provisioned_throughputs: HashMap::new(),
            logging_config: None,
            invocations: Vec::new(),
            custom_responses: HashMap::new(),
            async_invocations: HashMap::new(),
            custom_models: HashMap::new(),
            custom_model_deployments: HashMap::new(),
            model_import_jobs: HashMap::new(),
            imported_models: HashMap::new(),
            model_copy_jobs: HashMap::new(),
            model_invocation_jobs: HashMap::new(),
            evaluation_jobs: HashMap::new(),
            inference_profiles: HashMap::new(),
            prompt_routers: HashMap::new(),
            resource_policies: HashMap::new(),
            marketplace_endpoints: HashMap::new(),
            foundation_model_agreements: HashMap::new(),
            use_case_for_model_access: None,
            enforced_guardrail_configs: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.tags.clear();
        self.guardrails.clear();
        self.guardrail_versions.clear();
        self.customization_jobs.clear();
        self.provisioned_throughputs.clear();
        self.logging_config = None;
        self.invocations.clear();
        self.custom_responses.clear();
        self.async_invocations.clear();
        self.custom_models.clear();
        self.custom_model_deployments.clear();
        self.model_import_jobs.clear();
        self.imported_models.clear();
        self.model_copy_jobs.clear();
        self.model_invocation_jobs.clear();
        self.evaluation_jobs.clear();
        self.inference_profiles.clear();
        self.prompt_routers.clear();
        self.resource_policies.clear();
        self.marketplace_endpoints.clear();
        self.foundation_model_agreements.clear();
        self.use_case_for_model_access = None;
        self.enforced_guardrail_configs.clear();
    }
}

#[derive(Clone)]
pub struct Guardrail {
    pub guardrail_id: String,
    pub guardrail_arn: String,
    pub name: String,
    pub description: String,
    pub status: String,
    pub version: String,
    pub next_version_number: u32,
    pub blocked_input_messaging: String,
    pub blocked_outputs_messaging: String,
    pub content_policy: Option<serde_json::Value>,
    pub word_policy: Option<serde_json::Value>,
    pub sensitive_information_policy: Option<serde_json::Value>,
    pub topic_policy: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct GuardrailVersion {
    pub guardrail_id: String,
    pub guardrail_arn: String,
    pub version: String,
    pub name: String,
    pub description: String,
    pub status: String,
    pub blocked_input_messaging: String,
    pub blocked_outputs_messaging: String,
    pub content_policy: Option<serde_json::Value>,
    pub word_policy: Option<serde_json::Value>,
    pub sensitive_information_policy: Option<serde_json::Value>,
    pub topic_policy: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct CustomizationJob {
    pub job_arn: String,
    pub job_name: String,
    pub base_model_identifier: String,
    pub custom_model_name: String,
    pub role_arn: String,
    pub training_data_config: serde_json::Value,
    pub output_data_config: serde_json::Value,
    pub hyper_parameters: HashMap<String, String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub last_modified_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ProvisionedThroughput {
    pub provisioned_model_id: String,
    pub provisioned_model_arn: String,
    pub provisioned_model_name: String,
    pub model_arn: String,
    pub model_units: i32,
    pub desired_model_units: i32,
    pub status: String,
    pub commitment_duration: Option<String>,
    pub created_at: DateTime<Utc>,
    pub last_modified_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct LoggingConfig {
    pub cloud_watch_config: Option<serde_json::Value>,
    pub s3_config: Option<serde_json::Value>,
    pub text_data_delivery_enabled: bool,
    pub image_data_delivery_enabled: bool,
    pub embedding_data_delivery_enabled: bool,
}

#[derive(Clone)]
pub struct ModelInvocation {
    pub model_id: String,
    pub input: String,
    pub output: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone)]
pub struct AsyncInvocation {
    pub invocation_arn: String,
    pub model_arn: String,
    pub model_input: serde_json::Value,
    pub output_data_config: serde_json::Value,
    pub client_request_token: Option<String>,
    pub status: String,
    pub submit_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct CustomModel {
    pub model_arn: String,
    pub model_name: String,
    pub model_source_config: serde_json::Value,
    pub model_kms_key_arn: Option<String>,
    pub role_arn: Option<String>,
    pub model_status: String,
    pub creation_time: DateTime<Utc>,
}

#[derive(Clone)]
pub struct CustomModelDeployment {
    pub deployment_arn: String,
    pub deployment_name: String,
    pub model_arn: String,
    pub description: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ModelImportJob {
    pub job_arn: String,
    pub job_name: String,
    pub imported_model_name: String,
    pub imported_model_arn: String,
    pub role_arn: String,
    pub model_data_source: serde_json::Value,
    pub status: String,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ImportedModel {
    pub model_arn: String,
    pub model_name: String,
    pub job_arn: String,
    pub model_data_source: serde_json::Value,
    pub creation_time: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ModelCopyJob {
    pub job_arn: String,
    pub source_model_arn: String,
    pub target_model_arn: String,
    pub target_model_name: String,
    pub status: String,
    pub creation_time: DateTime<Utc>,
}

#[derive(Clone)]
pub struct ModelInvocationJob {
    pub job_arn: String,
    pub job_name: String,
    pub model_id: String,
    pub role_arn: String,
    pub input_data_config: serde_json::Value,
    pub output_data_config: serde_json::Value,
    pub status: String,
    pub submit_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
}

#[derive(Clone)]
pub struct EvaluationJob {
    pub job_arn: String,
    pub job_name: String,
    pub job_description: Option<String>,
    pub role_arn: String,
    pub status: String,
    pub job_type: String,
    pub evaluation_config: serde_json::Value,
    pub inference_config: serde_json::Value,
    pub output_data_config: serde_json::Value,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
}

#[derive(Clone)]
pub struct InferenceProfile {
    pub inference_profile_arn: String,
    pub inference_profile_name: String,
    pub description: Option<String>,
    pub model_source: serde_json::Value,
    pub status: String,
    pub inference_profile_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct PromptRouter {
    pub prompt_router_arn: String,
    pub prompt_router_name: String,
    pub description: Option<String>,
    pub models: serde_json::Value,
    pub routing_criteria: serde_json::Value,
    pub fallback_model: serde_json::Value,
    pub status: String,
    pub prompt_router_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct MarketplaceModelEndpoint {
    pub endpoint_arn: String,
    pub endpoint_name: String,
    pub model_source_identifier: String,
    pub status: String,
    pub endpoint_config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct FoundationModelAgreement {
    pub agreement_id: String,
    pub model_id: String,
    pub created_at: DateTime<Utc>,
}
