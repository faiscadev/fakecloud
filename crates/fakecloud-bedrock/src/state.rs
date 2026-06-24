use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;

pub type SharedBedrockState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<BedrockState>>>;

impl fakecloud_core::multi_account::AccountState for BedrockState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub const BEDROCK_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BedrockSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<BedrockState>>,
    #[serde(default)]
    pub state: Option<BedrockState>,
}

/// Serialize/deserialize `BTreeMap<(String, String), V>` as `Vec<(String, String, V)>`.
mod tuple2_map_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub(crate) fn serialize<V: Serialize, S: Serializer>(
        map: &BTreeMap<(String, String), V>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<(&String, &String, &V)> =
            map.iter().map(|((a, b), v)| (a, b, v)).collect();
        entries.serialize(s)
    }

    pub(crate) fn deserialize<'de, V: Deserialize<'de>, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<(String, String), V>, D::Error> {
        let entries: Vec<(String, String, V)> = Vec::deserialize(d)?;
        Ok(entries.into_iter().map(|(a, b, v)| ((a, b), v)).collect())
    }
}

/// Serialize/deserialize `BTreeMap<(String, String, String), V>` as `Vec<(String, String, String, V)>`.
#[allow(clippy::type_complexity)]
mod tuple3_map_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub(crate) fn serialize<V: Serialize, S: Serializer>(
        map: &BTreeMap<(String, String, String), V>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<(&String, &String, &String, &V)> =
            map.iter().map(|((a, b, c), v)| (a, b, c, v)).collect();
        entries.serialize(s)
    }

    pub(crate) fn deserialize<'de, V: Deserialize<'de>, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<(String, String, String), V>, D::Error> {
        let entries: Vec<(String, String, String, V)> = Vec::deserialize(d)?;
        Ok(entries
            .into_iter()
            .map(|(a, b, c, v)| ((a, b, c), v))
            .collect())
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct BedrockState {
    pub account_id: String,
    pub region: String,
    /// Tags keyed by resource ARN.
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Guardrails keyed by guardrail ID.
    pub guardrails: BTreeMap<String, Guardrail>,
    /// Guardrail versions keyed by (guardrail_id, version).
    #[serde(with = "tuple2_map_serde")]
    pub guardrail_versions: BTreeMap<(String, String), GuardrailVersion>,
    /// Model customization jobs keyed by job ARN.
    pub customization_jobs: BTreeMap<String, CustomizationJob>,
    /// Provisioned model throughputs keyed by provisioned model ID.
    pub provisioned_throughputs: BTreeMap<String, ProvisionedThroughput>,
    /// Model invocation logging configuration.
    pub logging_config: Option<LoggingConfig>,
    /// All model invocations recorded for introspection.
    #[serde(skip)]
    pub invocations: Vec<ModelInvocation>,
    /// Custom responses configured per model ID via simulation endpoint.
    #[serde(skip)]
    pub custom_responses: BTreeMap<String, String>,
    /// Prompt-conditional response rules per model ID.
    #[serde(skip)]
    pub response_rules: BTreeMap<String, Vec<ResponseRule>>,
    /// Queued fault-injection rules.
    #[serde(skip)]
    pub fault_rules: Vec<FaultRule>,
    /// Async invocations keyed by invocation ARN.
    pub async_invocations: BTreeMap<String, AsyncInvocation>,
    /// Custom models keyed by model ARN.
    pub custom_models: BTreeMap<String, CustomModel>,
    /// Custom model deployments keyed by deployment ARN.
    pub custom_model_deployments: BTreeMap<String, CustomModelDeployment>,
    /// Model import jobs keyed by job ARN.
    pub model_import_jobs: BTreeMap<String, ModelImportJob>,
    /// Imported models keyed by model ARN.
    pub imported_models: BTreeMap<String, ImportedModel>,
    /// Model copy jobs keyed by job ARN.
    pub model_copy_jobs: BTreeMap<String, ModelCopyJob>,
    /// Model invocation jobs (batch inference) keyed by job ARN.
    pub model_invocation_jobs: BTreeMap<String, ModelInvocationJob>,
    /// Evaluation jobs keyed by job ARN.
    pub evaluation_jobs: BTreeMap<String, EvaluationJob>,
    /// Advanced prompt optimization jobs keyed by job ARN.
    pub advanced_prompt_optimization_jobs: BTreeMap<String, AdvancedPromptOptimizationJob>,
    /// Inference profiles keyed by ARN.
    pub inference_profiles: BTreeMap<String, InferenceProfile>,
    /// Prompt routers keyed by ARN.
    pub prompt_routers: BTreeMap<String, PromptRouter>,
    /// Resource policies keyed by resource ARN.
    pub resource_policies: BTreeMap<String, String>,
    /// Marketplace model endpoints keyed by endpoint ARN.
    pub marketplace_endpoints: BTreeMap<String, MarketplaceModelEndpoint>,
    /// Foundation model agreements keyed by agreement ID.
    pub foundation_model_agreements: BTreeMap<String, FoundationModelAgreement>,
    /// Use case for model access.
    pub use_case_for_model_access: Option<serde_json::Value>,
    /// Enforced guardrail configurations keyed by config ID.
    pub enforced_guardrail_configs: BTreeMap<String, serde_json::Value>,
    /// Automated reasoning policies keyed by policy ARN.
    pub automated_reasoning_policies: BTreeMap<String, AutomatedReasoningPolicy>,
    /// Automated reasoning test cases keyed by (policy_arn, test_case_id).
    #[serde(with = "tuple2_map_serde")]
    pub automated_reasoning_test_cases: BTreeMap<(String, String), AutomatedReasoningTestCase>,
    /// Automated reasoning build workflows keyed by (policy_arn, workflow_id).
    #[serde(with = "tuple2_map_serde")]
    pub ar_build_workflows: BTreeMap<(String, String), AutomatedReasoningBuildWorkflow>,
    /// Automated reasoning test results keyed by (policy_arn, workflow_id, test_case_id).
    #[serde(with = "tuple3_map_serde")]
    pub ar_test_results: BTreeMap<(String, String, String), serde_json::Value>,
    /// Automated reasoning annotations keyed by (policy_arn, workflow_id).
    #[serde(with = "tuple2_map_serde")]
    pub ar_annotations: BTreeMap<(String, String), serde_json::Value>,
    /// Account-level data retention configuration.
    #[serde(default)]
    pub data_retention: Option<DataRetentionConfig>,
}

/// Account-level data retention setting and when it was last changed.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DataRetentionConfig {
    pub mode: String,
    pub updated_at: DateTime<Utc>,
}

impl BedrockState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            tags: BTreeMap::new(),
            guardrails: BTreeMap::new(),
            guardrail_versions: BTreeMap::new(),
            customization_jobs: BTreeMap::new(),
            provisioned_throughputs: BTreeMap::new(),
            logging_config: None,
            invocations: Vec::new(),
            custom_responses: BTreeMap::new(),
            response_rules: BTreeMap::new(),
            fault_rules: Vec::new(),
            async_invocations: BTreeMap::new(),
            custom_models: BTreeMap::new(),
            custom_model_deployments: BTreeMap::new(),
            model_import_jobs: BTreeMap::new(),
            imported_models: BTreeMap::new(),
            model_copy_jobs: BTreeMap::new(),
            model_invocation_jobs: BTreeMap::new(),
            evaluation_jobs: BTreeMap::new(),
            advanced_prompt_optimization_jobs: BTreeMap::new(),
            inference_profiles: BTreeMap::new(),
            prompt_routers: BTreeMap::new(),
            resource_policies: BTreeMap::new(),
            marketplace_endpoints: BTreeMap::new(),
            foundation_model_agreements: BTreeMap::new(),
            use_case_for_model_access: None,
            enforced_guardrail_configs: BTreeMap::new(),
            automated_reasoning_policies: BTreeMap::new(),
            automated_reasoning_test_cases: BTreeMap::new(),
            ar_build_workflows: BTreeMap::new(),
            ar_test_results: BTreeMap::new(),
            ar_annotations: BTreeMap::new(),
            data_retention: None,
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
        self.response_rules.clear();
        self.fault_rules.clear();
        self.async_invocations.clear();
        self.custom_models.clear();
        self.custom_model_deployments.clear();
        self.model_import_jobs.clear();
        self.imported_models.clear();
        self.model_copy_jobs.clear();
        self.model_invocation_jobs.clear();
        self.evaluation_jobs.clear();
        self.advanced_prompt_optimization_jobs.clear();
        self.inference_profiles.clear();
        self.prompt_routers.clear();
        self.resource_policies.clear();
        self.marketplace_endpoints.clear();
        self.foundation_model_agreements.clear();
        self.use_case_for_model_access = None;
        self.enforced_guardrail_configs.clear();
        self.automated_reasoning_policies.clear();
        self.automated_reasoning_test_cases.clear();
        self.ar_build_workflows.clear();
        self.ar_test_results.clear();
        self.ar_annotations.clear();
        self.data_retention = None;
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
    #[serde(default)]
    pub contextual_grounding_policy: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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
    #[serde(default)]
    pub contextual_grounding_policy: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct CustomizationJob {
    pub job_arn: String,
    pub job_name: String,
    pub base_model_identifier: String,
    pub custom_model_name: String,
    pub role_arn: String,
    pub training_data_config: serde_json::Value,
    pub output_data_config: serde_json::Value,
    pub hyper_parameters: BTreeMap<String, String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub last_modified_at: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LoggingConfig {
    pub cloud_watch_config: Option<serde_json::Value>,
    pub s3_config: Option<serde_json::Value>,
    pub text_data_delivery_enabled: bool,
    pub image_data_delivery_enabled: bool,
    pub embedding_data_delivery_enabled: bool,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ResponseRule {
    /// Substring to look for in the extracted prompt text. `None` or empty
    /// matches any prompt.
    pub prompt_contains: Option<String>,
    /// Raw response body to return when the rule matches.
    pub response: String,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelInvocation {
    pub model_id: String,
    pub input: String,
    pub output: String,
    pub timestamp: DateTime<Utc>,
    /// `Some("<errorType>: <message>")` when the call was faulted by an
    /// injected fault rule; `None` for successful calls.
    pub error: Option<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct FaultRule {
    pub error_type: String,
    pub message: String,
    pub http_status: u16,
    pub remaining: u32,
    pub model_id: Option<String>,
    pub operation: Option<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct CustomModel {
    pub model_arn: String,
    pub model_name: String,
    #[serde(default)]
    pub base_model_arn: String,
    #[serde(default)]
    pub base_model_name: String,
    pub model_source_config: serde_json::Value,
    pub model_kms_key_arn: Option<String>,
    pub role_arn: Option<String>,
    pub model_status: String,
    pub creation_time: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct CustomModelDeployment {
    pub deployment_arn: String,
    pub deployment_name: String,
    pub model_arn: String,
    pub description: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub last_updated_at: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ImportedModel {
    pub model_arn: String,
    pub model_name: String,
    pub job_arn: String,
    pub model_data_source: serde_json::Value,
    pub creation_time: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelCopyJob {
    pub job_arn: String,
    pub source_model_arn: String,
    pub target_model_arn: String,
    pub target_model_name: String,
    pub status: String,
    pub creation_time: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AdvancedPromptOptimizationJob {
    pub job_arn: String,
    pub job_name: String,
    pub job_description: Option<String>,
    pub status: String,
    pub input_config: serde_json::Value,
    pub output_config: serde_json::Value,
    pub encryption_key_arn: Option<String>,
    pub model_configurations: serde_json::Value,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MarketplaceModelEndpoint {
    pub endpoint_arn: String,
    pub endpoint_name: String,
    pub model_source_identifier: String,
    pub status: String,
    pub endpoint_config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct FoundationModelAgreement {
    pub agreement_id: String,
    pub model_id: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AutomatedReasoningPolicy {
    pub policy_arn: String,
    pub policy_name: String,
    pub description: Option<String>,
    pub policy_document: serde_json::Value,
    pub status: String,
    pub version: String,
    pub versions: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AutomatedReasoningBuildWorkflow {
    pub workflow_id: String,
    pub policy_arn: String,
    pub workflow_type: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AutomatedReasoningTestCase {
    pub test_case_id: String,
    pub policy_arn: String,
    pub test_case_name: String,
    pub description: Option<String>,
    pub input: serde_json::Value,
    pub expected_output: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_state() {
        let state = BedrockState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.guardrails.is_empty());
        assert!(state.custom_models.is_empty());
    }

    #[test]
    fn new_initializes_all_collections_empty() {
        let s = BedrockState::new("123", "us-east-1");
        assert!(s.tags.is_empty());
        assert!(s.guardrail_versions.is_empty());
        assert!(s.customization_jobs.is_empty());
        assert!(s.provisioned_throughputs.is_empty());
        assert!(s.logging_config.is_none());
        assert!(s.invocations.is_empty());
        assert!(s.custom_responses.is_empty());
        assert!(s.response_rules.is_empty());
        assert!(s.fault_rules.is_empty());
        assert!(s.async_invocations.is_empty());
        assert!(s.custom_model_deployments.is_empty());
        assert!(s.model_import_jobs.is_empty());
        assert!(s.imported_models.is_empty());
        assert!(s.model_copy_jobs.is_empty());
        assert!(s.model_invocation_jobs.is_empty());
        assert!(s.evaluation_jobs.is_empty());
        assert!(s.inference_profiles.is_empty());
        assert!(s.prompt_routers.is_empty());
        assert!(s.resource_policies.is_empty());
        assert!(s.marketplace_endpoints.is_empty());
        assert!(s.foundation_model_agreements.is_empty());
        assert!(s.use_case_for_model_access.is_none());
        assert!(s.enforced_guardrail_configs.is_empty());
        assert!(s.automated_reasoning_policies.is_empty());
        assert!(s.automated_reasoning_test_cases.is_empty());
        assert!(s.ar_build_workflows.is_empty());
        assert!(s.ar_test_results.is_empty());
        assert!(s.ar_annotations.is_empty());
    }

    #[test]
    fn reset_clears_all_collections() {
        let mut s = BedrockState::new("123", "us-east-1");
        s.tags.insert("arn".to_string(), BTreeMap::new());
        s.custom_responses.insert("m".to_string(), "r".to_string());
        s.fault_rules.push(FaultRule {
            error_type: "T".to_string(),
            message: "m".to_string(),
            http_status: 500,
            remaining: 1,
            model_id: None,
            operation: None,
        });
        s.use_case_for_model_access = Some(serde_json::json!({"a": 1}));
        s.logging_config = Some(LoggingConfig {
            cloud_watch_config: None,
            s3_config: None,
            text_data_delivery_enabled: false,
            image_data_delivery_enabled: false,
            embedding_data_delivery_enabled: false,
        });

        s.reset();

        assert!(s.tags.is_empty());
        assert!(s.custom_responses.is_empty());
        assert!(s.fault_rules.is_empty());
        assert!(s.use_case_for_model_access.is_none());
        assert!(s.logging_config.is_none());
    }
}
