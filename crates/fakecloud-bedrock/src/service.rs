use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::models;
use crate::state::SharedBedrockState;

pub struct BedrockService {
    state: SharedBedrockState,
}

impl BedrockService {
    pub fn new(state: SharedBedrockState) -> Self {
        Self { state }
    }

    fn resolve_action(req: &AwsRequest) -> Option<(&str, Option<String>, Option<String>)> {
        let segs = &req.path_segments;
        if segs.is_empty() {
            return None;
        }

        let decode = |s: &str| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into_owned()
        };

        match (req.method.clone(), segs.len()) {
            // Foundation models
            (Method::GET, 1) if segs[0] == "foundation-models" => {
                Some(("ListFoundationModels", None, None))
            }
            (Method::GET, 2) if segs[0] == "foundation-models" => {
                Some(("GetFoundationModel", Some(decode(&segs[1])), None))
            }

            // Guardrails
            (Method::POST, 1) if segs[0] == "guardrails" => Some(("CreateGuardrail", None, None)),
            (Method::GET, 1) if segs[0] == "guardrails" => Some(("ListGuardrails", None, None)),
            (Method::GET, 2) if segs[0] == "guardrails" => {
                Some(("GetGuardrail", Some(decode(&segs[1])), None))
            }
            (Method::PUT, 2) if segs[0] == "guardrails" => {
                Some(("UpdateGuardrail", Some(decode(&segs[1])), None))
            }
            (Method::DELETE, 2) if segs[0] == "guardrails" => {
                Some(("DeleteGuardrail", Some(decode(&segs[1])), None))
            }
            (Method::POST, 2) if segs[0] == "guardrails" => {
                Some(("CreateGuardrailVersion", Some(decode(&segs[1])), None))
            }

            // Custom models
            (Method::POST, 2) if segs[0] == "custom-models" && segs[1] == "create-custom-model" => {
                Some(("CreateCustomModel", None, None))
            }
            (Method::GET, 1) if segs[0] == "custom-models" => {
                Some(("ListCustomModels", None, None))
            }
            (Method::GET, 2) if segs[0] == "custom-models" => {
                Some(("GetCustomModel", Some(decode(&segs[1])), None))
            }
            (Method::DELETE, 2) if segs[0] == "custom-models" => {
                Some(("DeleteCustomModel", Some(decode(&segs[1])), None))
            }

            // Custom model deployments
            (Method::POST, 2)
                if segs[0] == "model-customization" && segs[1] == "custom-model-deployments" =>
            {
                Some(("CreateCustomModelDeployment", None, None))
            }
            (Method::GET, 2)
                if segs[0] == "model-customization" && segs[1] == "custom-model-deployments" =>
            {
                Some(("ListCustomModelDeployments", None, None))
            }
            (Method::GET, 3)
                if segs[0] == "model-customization" && segs[1] == "custom-model-deployments" =>
            {
                Some(("GetCustomModelDeployment", Some(decode(&segs[2])), None))
            }
            (Method::PATCH, 3)
                if segs[0] == "model-customization" && segs[1] == "custom-model-deployments" =>
            {
                Some(("UpdateCustomModelDeployment", Some(decode(&segs[2])), None))
            }
            (Method::DELETE, 3)
                if segs[0] == "model-customization" && segs[1] == "custom-model-deployments" =>
            {
                Some(("DeleteCustomModelDeployment", Some(decode(&segs[2])), None))
            }

            // Model import jobs
            (Method::POST, 1) if segs[0] == "model-import-jobs" => {
                Some(("CreateModelImportJob", None, None))
            }
            (Method::GET, 1) if segs[0] == "model-import-jobs" => {
                Some(("ListModelImportJobs", None, None))
            }
            (Method::GET, 2) if segs[0] == "model-import-jobs" => {
                Some(("GetModelImportJob", Some(decode(&segs[1])), None))
            }

            // Imported models
            (Method::GET, 1) if segs[0] == "imported-models" => {
                Some(("ListImportedModels", None, None))
            }
            (Method::GET, 2) if segs[0] == "imported-models" => {
                Some(("GetImportedModel", Some(decode(&segs[1])), None))
            }
            (Method::DELETE, 2) if segs[0] == "imported-models" => {
                Some(("DeleteImportedModel", Some(decode(&segs[1])), None))
            }

            // Model copy jobs
            (Method::POST, 1) if segs[0] == "model-copy-jobs" => {
                Some(("CreateModelCopyJob", None, None))
            }
            (Method::GET, 1) if segs[0] == "model-copy-jobs" => {
                Some(("ListModelCopyJobs", None, None))
            }
            (Method::GET, 2) if segs[0] == "model-copy-jobs" => {
                Some(("GetModelCopyJob", Some(decode(&segs[1])), None))
            }

            // Model invocation jobs (batch inference)
            (Method::POST, 1) if segs[0] == "model-invocation-job" => {
                Some(("CreateModelInvocationJob", None, None))
            }
            (Method::GET, 1) if segs[0] == "model-invocation-jobs" => {
                Some(("ListModelInvocationJobs", None, None))
            }
            (Method::GET, 2) if segs[0] == "model-invocation-job" => {
                Some(("GetModelInvocationJob", Some(decode(&segs[1])), None))
            }
            (Method::POST, 3) if segs[0] == "model-invocation-job" && segs[2] == "stop" => {
                Some(("StopModelInvocationJob", Some(decode(&segs[1])), None))
            }

            // Evaluation jobs
            (Method::POST, 1) if segs[0] == "evaluation-jobs" => {
                Some(("CreateEvaluationJob", None, None))
            }
            (Method::GET, 1) if segs[0] == "evaluation-jobs" => {
                Some(("ListEvaluationJobs", None, None))
            }
            (Method::GET, 2) if segs[0] == "evaluation-jobs" => {
                Some(("GetEvaluationJob", Some(decode(&segs[1])), None))
            }
            (Method::POST, 3) if segs[0] == "evaluation-job" && segs[2] == "stop" => {
                Some(("StopEvaluationJob", Some(decode(&segs[1])), None))
            }
            (Method::POST, 2) if segs[0] == "evaluation-jobs" && segs[1] == "batch-delete" => {
                Some(("BatchDeleteEvaluationJob", None, None))
            }

            // Inference profiles
            (Method::POST, 1) if segs[0] == "inference-profiles" => {
                Some(("CreateInferenceProfile", None, None))
            }
            (Method::GET, 1) if segs[0] == "inference-profiles" => {
                Some(("ListInferenceProfiles", None, None))
            }
            (Method::GET, 2) if segs[0] == "inference-profiles" => {
                Some(("GetInferenceProfile", Some(decode(&segs[1])), None))
            }
            (Method::DELETE, 2) if segs[0] == "inference-profiles" => {
                Some(("DeleteInferenceProfile", Some(decode(&segs[1])), None))
            }

            // Prompt routers
            (Method::POST, 1) if segs[0] == "prompt-routers" => {
                Some(("CreatePromptRouter", None, None))
            }
            (Method::GET, 1) if segs[0] == "prompt-routers" => {
                Some(("ListPromptRouters", None, None))
            }
            (Method::GET, 2) if segs[0] == "prompt-routers" => {
                Some(("GetPromptRouter", Some(decode(&segs[1])), None))
            }
            (Method::DELETE, 2) if segs[0] == "prompt-routers" => {
                Some(("DeletePromptRouter", Some(decode(&segs[1])), None))
            }

            // Resource policies
            (Method::POST, 1) if segs[0] == "resource-policy" => {
                Some(("PutResourcePolicy", None, None))
            }
            (Method::GET, 2) if segs[0] == "resource-policy" => {
                Some(("GetResourcePolicy", Some(decode(&segs[1])), None))
            }
            (Method::DELETE, 2) if segs[0] == "resource-policy" => {
                Some(("DeleteResourcePolicy", Some(decode(&segs[1])), None))
            }

            // Marketplace model endpoints
            (Method::POST, 2) if segs[0] == "marketplace-model" && segs[1] == "endpoints" => {
                Some(("CreateMarketplaceModelEndpoint", None, None))
            }
            (Method::GET, 2) if segs[0] == "marketplace-model" && segs[1] == "endpoints" => {
                Some(("ListMarketplaceModelEndpoints", None, None))
            }
            (Method::GET, 3) if segs[0] == "marketplace-model" && segs[1] == "endpoints" => {
                Some(("GetMarketplaceModelEndpoint", Some(decode(&segs[2])), None))
            }
            (Method::PATCH, 3) if segs[0] == "marketplace-model" && segs[1] == "endpoints" => {
                Some((
                    "UpdateMarketplaceModelEndpoint",
                    Some(decode(&segs[2])),
                    None,
                ))
            }
            (Method::DELETE, 3) if segs[0] == "marketplace-model" && segs[1] == "endpoints" => {
                Some((
                    "DeleteMarketplaceModelEndpoint",
                    Some(decode(&segs[2])),
                    None,
                ))
            }
            (Method::POST, 4)
                if segs[0] == "marketplace-model"
                    && segs[1] == "endpoints"
                    && segs[3] == "registration" =>
            {
                Some((
                    "RegisterMarketplaceModelEndpoint",
                    Some(decode(&segs[2])),
                    None,
                ))
            }
            (Method::DELETE, 4)
                if segs[0] == "marketplace-model"
                    && segs[1] == "endpoints"
                    && segs[3] == "registration" =>
            {
                Some((
                    "DeregisterMarketplaceModelEndpoint",
                    Some(decode(&segs[2])),
                    None,
                ))
            }

            // Foundation model agreements
            (Method::POST, 1) if segs[0] == "create-foundation-model-agreement" => {
                Some(("CreateFoundationModelAgreement", None, None))
            }
            (Method::POST, 1) if segs[0] == "delete-foundation-model-agreement" => {
                Some(("DeleteFoundationModelAgreement", None, None))
            }
            (Method::GET, 2) if segs[0] == "list-foundation-model-agreement-offers" => Some((
                "ListFoundationModelAgreementOffers",
                Some(decode(&segs[1])),
                None,
            )),
            (Method::GET, 2) if segs[0] == "foundation-model-availability" => Some((
                "GetFoundationModelAvailability",
                Some(decode(&segs[1])),
                None,
            )),
            (Method::GET, 1) if segs[0] == "use-case-for-model-access" => {
                Some(("GetUseCaseForModelAccess", None, None))
            }
            (Method::POST, 1) if segs[0] == "use-case-for-model-access" => {
                Some(("PutUseCaseForModelAccess", None, None))
            }

            // Enforced guardrails
            (Method::PUT, 1) if segs[0] == "enforcedGuardrailsConfiguration" => {
                Some(("PutEnforcedGuardrailConfiguration", None, None))
            }
            (Method::GET, 1) if segs[0] == "enforcedGuardrailsConfiguration" => {
                Some(("ListEnforcedGuardrailsConfiguration", None, None))
            }
            (Method::DELETE, 2) if segs[0] == "enforcedGuardrailsConfiguration" => Some((
                "DeleteEnforcedGuardrailConfiguration",
                Some(decode(&segs[1])),
                None,
            )),

            // Automated reasoning build workflows (longer paths first)
            (Method::GET, 7)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "test-cases"
                    && segs[6] == "test-results" =>
            {
                Some((
                    "GetAutomatedReasoningPolicyTestResult",
                    Some(decode(&segs[1])),
                    Some(format!("{}:{}", decode(&segs[3]), decode(&segs[5]))),
                ))
            }
            (Method::POST, 5)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "start" =>
            {
                Some((
                    "StartAutomatedReasoningPolicyBuildWorkflow",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::POST, 5)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "cancel" =>
            {
                Some((
                    "CancelAutomatedReasoningPolicyBuildWorkflow",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::POST, 5)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "test-workflows" =>
            {
                Some((
                    "StartAutomatedReasoningPolicyTestWorkflow",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::GET, 5)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "result-assets" =>
            {
                Some((
                    "GetAutomatedReasoningPolicyBuildWorkflowResultAssets",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::GET, 5)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "annotations" =>
            {
                Some((
                    "GetAutomatedReasoningPolicyAnnotations",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::PATCH, 5)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "annotations" =>
            {
                Some((
                    "UpdateAutomatedReasoningPolicyAnnotations",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::GET, 5)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "scenarios" =>
            {
                Some((
                    "GetAutomatedReasoningPolicyNextScenario",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::GET, 5)
                if segs[0] == "automated-reasoning-policies"
                    && segs[2] == "build-workflows"
                    && segs[4] == "test-results" =>
            {
                Some((
                    "ListAutomatedReasoningPolicyTestResults",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::GET, 4)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "build-workflows" =>
            {
                Some((
                    "GetAutomatedReasoningPolicyBuildWorkflow",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::DELETE, 4)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "build-workflows" =>
            {
                Some((
                    "DeleteAutomatedReasoningPolicyBuildWorkflow",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::GET, 3)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "build-workflows" =>
            {
                Some((
                    "ListAutomatedReasoningPolicyBuildWorkflows",
                    Some(decode(&segs[1])),
                    None,
                ))
            }

            // Automated reasoning policies
            (Method::POST, 1) if segs[0] == "automated-reasoning-policies" => {
                Some(("CreateAutomatedReasoningPolicy", None, None))
            }
            (Method::GET, 1) if segs[0] == "automated-reasoning-policies" => {
                Some(("ListAutomatedReasoningPolicies", None, None))
            }
            (Method::GET, 2) if segs[0] == "automated-reasoning-policies" => {
                Some(("GetAutomatedReasoningPolicy", Some(decode(&segs[1])), None))
            }
            (Method::PATCH, 2) if segs[0] == "automated-reasoning-policies" => Some((
                "UpdateAutomatedReasoningPolicy",
                Some(decode(&segs[1])),
                None,
            )),
            (Method::DELETE, 2) if segs[0] == "automated-reasoning-policies" => Some((
                "DeleteAutomatedReasoningPolicy",
                Some(decode(&segs[1])),
                None,
            )),
            (Method::POST, 3)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "versions" =>
            {
                Some((
                    "CreateAutomatedReasoningPolicyVersion",
                    Some(decode(&segs[1])),
                    None,
                ))
            }
            (Method::GET, 3)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "export" =>
            {
                Some((
                    "ExportAutomatedReasoningPolicyVersion",
                    Some(decode(&segs[1])),
                    None,
                ))
            }
            (Method::POST, 3)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "test-cases" =>
            {
                Some((
                    "CreateAutomatedReasoningPolicyTestCase",
                    Some(decode(&segs[1])),
                    None,
                ))
            }
            (Method::GET, 3)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "test-cases" =>
            {
                Some((
                    "ListAutomatedReasoningPolicyTestCases",
                    Some(decode(&segs[1])),
                    None,
                ))
            }
            (Method::GET, 4)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "test-cases" =>
            {
                Some((
                    "GetAutomatedReasoningPolicyTestCase",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::PATCH, 4)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "test-cases" =>
            {
                Some((
                    "UpdateAutomatedReasoningPolicyTestCase",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }
            (Method::DELETE, 4)
                if segs[0] == "automated-reasoning-policies" && segs[2] == "test-cases" =>
            {
                Some((
                    "DeleteAutomatedReasoningPolicyTestCase",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }

            // Model customization jobs
            (Method::POST, 1) if segs[0] == "model-customization-jobs" => {
                Some(("CreateModelCustomizationJob", None, None))
            }
            (Method::GET, 1) if segs[0] == "model-customization-jobs" => {
                Some(("ListModelCustomizationJobs", None, None))
            }
            (Method::GET, 2) if segs[0] == "model-customization-jobs" => {
                Some(("GetModelCustomizationJob", Some(decode(&segs[1])), None))
            }
            (Method::POST, 3) if segs[0] == "model-customization-jobs" && segs[2] == "stop" => {
                Some(("StopModelCustomizationJob", Some(decode(&segs[1])), None))
            }

            // Provisioned model throughput
            (Method::POST, 1) if segs[0] == "provisioned-model-throughput" => {
                Some(("CreateProvisionedModelThroughput", None, None))
            }
            (Method::GET, 1) if segs[0] == "provisioned-model-throughputs" => {
                Some(("ListProvisionedModelThroughputs", None, None))
            }
            (Method::GET, 2) if segs[0] == "provisioned-model-throughput" => Some((
                "GetProvisionedModelThroughput",
                Some(decode(&segs[1])),
                None,
            )),
            (Method::PATCH, 2) if segs[0] == "provisioned-model-throughput" => Some((
                "UpdateProvisionedModelThroughput",
                Some(decode(&segs[1])),
                None,
            )),
            (Method::DELETE, 2) if segs[0] == "provisioned-model-throughput" => Some((
                "DeleteProvisionedModelThroughput",
                Some(decode(&segs[1])),
                None,
            )),

            // Logging configuration
            (Method::PUT, 2) if segs[0] == "logging" && segs[1] == "modelinvocations" => {
                Some(("PutModelInvocationLoggingConfiguration", None, None))
            }
            (Method::GET, 2) if segs[0] == "logging" && segs[1] == "modelinvocations" => {
                Some(("GetModelInvocationLoggingConfiguration", None, None))
            }
            (Method::DELETE, 2) if segs[0] == "logging" && segs[1] == "modelinvocations" => {
                Some(("DeleteModelInvocationLoggingConfiguration", None, None))
            }

            // Runtime: ApplyGuardrail — POST /guardrail/{id}/version/{version}/apply
            (Method::POST, 5)
                if segs[0] == "guardrail" && segs[2] == "version" && segs[4] == "apply" =>
            {
                Some((
                    "ApplyGuardrail",
                    Some(decode(&segs[1])),
                    Some(decode(&segs[3])),
                ))
            }

            // Runtime: model operations
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "invoke" => {
                Some(("InvokeModel", Some(decode(&segs[1])), None))
            }
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "invoke-with-response-stream" => {
                Some((
                    "InvokeModelWithResponseStream",
                    Some(decode(&segs[1])),
                    None,
                ))
            }
            (Method::POST, 3)
                if segs[0] == "model" && segs[2] == "invoke-with-bidirectional-stream" =>
            {
                Some((
                    "InvokeModelWithBidirectionalStream",
                    Some(decode(&segs[1])),
                    None,
                ))
            }
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "converse" => {
                Some(("Converse", Some(decode(&segs[1])), None))
            }
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "converse-stream" => {
                Some(("ConverseStream", Some(decode(&segs[1])), None))
            }
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "count-tokens" => {
                Some(("CountTokens", Some(decode(&segs[1])), None))
            }

            // Runtime: async invoke
            (Method::POST, 1) if segs[0] == "async-invoke" => {
                Some(("StartAsyncInvoke", None, None))
            }
            (Method::GET, 1) if segs[0] == "async-invoke" => Some(("ListAsyncInvokes", None, None)),
            (Method::GET, 2) if segs[0] == "async-invoke" => {
                Some(("GetAsyncInvoke", Some(decode(&segs[1])), None))
            }

            // Tags — all POST with ARN in body
            (Method::POST, 1) if segs[0] == "tagResource" => Some(("TagResource", None, None)),
            (Method::POST, 1) if segs[0] == "untagResource" => Some(("UntagResource", None, None)),
            (Method::POST, 1) if segs[0] == "listTagsForResource" => {
                Some(("ListTagsForResource", None, None))
            }

            _ => None,
        }
    }

    fn list_foundation_models(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut model_summaries: Vec<Value> = Vec::new();

        let by_provider = req.query_params.get("byProvider");
        let by_output_modality = req.query_params.get("byOutputModality");
        let by_input_modality = req.query_params.get("byInputModality");
        let by_customization_type = req.query_params.get("byCustomizationType");
        let by_inference_type = req.query_params.get("byInferenceType");

        for model in models::FOUNDATION_MODELS {
            if let Some(provider) = by_provider {
                if model.provider_name != provider.as_str() {
                    continue;
                }
            }
            if let Some(modality) = by_output_modality {
                if !model.output_modalities.contains(&modality.as_str()) {
                    continue;
                }
            }
            if let Some(modality) = by_input_modality {
                if !model.input_modalities.contains(&modality.as_str()) {
                    continue;
                }
            }
            if let Some(customization) = by_customization_type {
                if !model
                    .customizations_supported
                    .contains(&customization.as_str())
                {
                    continue;
                }
            }
            if let Some(inference) = by_inference_type {
                if !model
                    .inference_types_supported
                    .contains(&inference.as_str())
                {
                    continue;
                }
            }
            model_summaries.push(model.to_summary_json());
        }

        Ok(AwsResponse::ok_json(json!({
            "modelSummaries": model_summaries
        })))
    }

    fn get_foundation_model(
        &self,
        req: &AwsRequest,
        model_id: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let model = models::find_model(model_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Could not find model {model_id}"),
            )
        })?;

        Ok(AwsResponse::ok_json(
            model.to_detail_json(&req.region, &req.account_id),
        ))
    }

    fn tag_resource(
        &self,
        _req: &AwsRequest,
        resource_arn: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let tags = body.get("tags").and_then(|t| t.as_array()).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "tags is required",
            )
        })?;

        let mut state = self.state.write();
        let resource_tags = state.tags.entry(resource_arn.to_string()).or_default();
        for tag in tags {
            let key = tag["key"].as_str().unwrap_or_default();
            let value = tag["value"].as_str().unwrap_or_default();
            resource_tags.insert(key.to_string(), value.to_string());
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
    }

    fn untag_resource_from_body(
        &self,
        resource_arn: &str,
        tag_keys: &[String],
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        if let Some(resource_tags) = state.tags.get_mut(resource_arn) {
            for key in tag_keys {
                resource_tags.remove(key);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
    }

    fn list_tags_for_resource(
        &self,
        _req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let tags = state.tags.get(resource_arn);
        let tags_arr: Vec<Value> = match tags {
            Some(t) => {
                let mut arr: Vec<Value> = t
                    .iter()
                    .map(|(k, v)| json!({"key": k, "value": v}))
                    .collect();
                arr.sort_by(|a, b| {
                    a["key"]
                        .as_str()
                        .unwrap_or("")
                        .cmp(b["key"].as_str().unwrap_or(""))
                });
                arr
            }
            None => Vec::new(),
        };

        Ok(AwsResponse::ok_json(json!({ "tags": tags_arr })))
    }
}

#[async_trait]
impl AwsService for BedrockService {
    fn service_name(&self) -> &str {
        "bedrock"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, resource_id, extra_id) =
            Self::resolve_action(&req).ok_or_else(|| AwsServiceError::ActionNotImplemented {
                service: "bedrock".to_string(),
                action: format!("{} {}", req.method, req.raw_path),
            })?;

        match action {
            "ListFoundationModels" => self.list_foundation_models(&req),
            "GetFoundationModel" => {
                self.get_foundation_model(&req, &resource_id.unwrap_or_default())
            }
            "TagResource" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                let arn = body["resourceARN"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "ValidationException",
                            "resourceARN is required",
                        )
                    })?;
                self.tag_resource(&req, arn, &body)
            }
            "UntagResource" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                let arn = body["resourceARN"].as_str().unwrap_or_default();
                let tag_keys: Vec<String> = body["tagKeys"]
                    .as_array()
                    .map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default();
                self.untag_resource_from_body(arn, &tag_keys)
            }
            "ListTagsForResource" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                let arn = body["resourceARN"].as_str().unwrap_or_default();
                self.list_tags_for_resource(&req, arn)
            }
            "CreateGuardrail" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::guardrails::create_guardrail(&self.state, &req, &body)
            }
            "GetGuardrail" => crate::guardrails::get_guardrail(
                &self.state,
                &req,
                &resource_id.unwrap_or_default(),
            ),
            "ListGuardrails" => crate::guardrails::list_guardrails(&self.state, &req),
            "UpdateGuardrail" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::guardrails::update_guardrail(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &body,
                )
            }
            "DeleteGuardrail" => {
                crate::guardrails::delete_guardrail(&self.state, &resource_id.unwrap_or_default())
            }
            "CreateGuardrailVersion" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::guardrails::create_guardrail_version(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &body,
                )
            }
            "ApplyGuardrail" => crate::guardrails::apply_guardrail(
                &self.state,
                &resource_id.unwrap_or_default(),
                &extra_id.unwrap_or_default(),
                &req.body,
            ),
            // Custom models
            "CreateCustomModel" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::custom_models::create_custom_model(&self.state, &req, &body)
            }
            "GetCustomModel" => crate::custom_models::get_custom_model(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "ListCustomModels" => crate::custom_models::list_custom_models(&self.state, &req),
            "DeleteCustomModel" => crate::custom_models::delete_custom_model(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            // Custom model deployments
            "CreateCustomModelDeployment" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::custom_model_deployments::create_custom_model_deployment(
                    &self.state,
                    &req,
                    &body,
                )
            }
            "GetCustomModelDeployment" => {
                crate::custom_model_deployments::get_custom_model_deployment(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            "ListCustomModelDeployments" => {
                crate::custom_model_deployments::list_custom_model_deployments(&self.state, &req)
            }
            "UpdateCustomModelDeployment" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::custom_model_deployments::update_custom_model_deployment(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &body,
                )
            }
            "DeleteCustomModelDeployment" => {
                crate::custom_model_deployments::delete_custom_model_deployment(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            // Model import jobs
            "CreateModelImportJob" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::model_import::create_model_import_job(&self.state, &req, &body)
            }
            "GetModelImportJob" => crate::model_import::get_model_import_job(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "ListModelImportJobs" => crate::model_import::list_model_import_jobs(&self.state, &req),
            "GetImportedModel" => crate::model_import::get_imported_model(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "ListImportedModels" => crate::model_import::list_imported_models(&self.state, &req),
            "DeleteImportedModel" => crate::model_import::delete_imported_model(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            // Model copy jobs
            "CreateModelCopyJob" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::model_copy::create_model_copy_job(&self.state, &req, &body)
            }
            "GetModelCopyJob" => {
                crate::model_copy::get_model_copy_job(&self.state, &resource_id.unwrap_or_default())
            }
            "ListModelCopyJobs" => crate::model_copy::list_model_copy_jobs(&self.state, &req),
            // Model invocation jobs
            "CreateModelInvocationJob" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::invocation_jobs::create_model_invocation_job(&self.state, &req, &body)
            }
            "GetModelInvocationJob" => crate::invocation_jobs::get_model_invocation_job(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "ListModelInvocationJobs" => {
                crate::invocation_jobs::list_model_invocation_jobs(&self.state, &req)
            }
            "StopModelInvocationJob" => crate::invocation_jobs::stop_model_invocation_job(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            // Evaluation jobs
            "CreateEvaluationJob" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::evaluation::create_evaluation_job(&self.state, &req, &body)
            }
            "GetEvaluationJob" => {
                crate::evaluation::get_evaluation_job(&self.state, &resource_id.unwrap_or_default())
            }
            "ListEvaluationJobs" => crate::evaluation::list_evaluation_jobs(&self.state, &req),
            "StopEvaluationJob" => crate::evaluation::stop_evaluation_job(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "BatchDeleteEvaluationJob" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::evaluation::batch_delete_evaluation_job(&self.state, &body)
            }
            // Inference profiles
            "CreateInferenceProfile" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::inference_profiles::create_inference_profile(&self.state, &req, &body)
            }
            "GetInferenceProfile" => crate::inference_profiles::get_inference_profile(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "ListInferenceProfiles" => {
                crate::inference_profiles::list_inference_profiles(&self.state, &req)
            }
            "DeleteInferenceProfile" => crate::inference_profiles::delete_inference_profile(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            // Prompt routers
            "CreatePromptRouter" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::prompt_routers::create_prompt_router(&self.state, &req, &body)
            }
            "GetPromptRouter" => crate::prompt_routers::get_prompt_router(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "ListPromptRouters" => crate::prompt_routers::list_prompt_routers(&self.state, &req),
            "DeletePromptRouter" => crate::prompt_routers::delete_prompt_router(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            // Resource policies
            "PutResourcePolicy" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::resource_policies::put_resource_policy(&self.state, &body)
            }
            "GetResourcePolicy" => crate::resource_policies::get_resource_policy(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "DeleteResourcePolicy" => crate::resource_policies::delete_resource_policy(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            // Marketplace model endpoints
            "CreateMarketplaceModelEndpoint" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::marketplace::create_marketplace_model_endpoint(&self.state, &req, &body)
            }
            "GetMarketplaceModelEndpoint" => crate::marketplace::get_marketplace_model_endpoint(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "ListMarketplaceModelEndpoints" => {
                crate::marketplace::list_marketplace_model_endpoints(&self.state, &req)
            }
            "UpdateMarketplaceModelEndpoint" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::marketplace::update_marketplace_model_endpoint(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &body,
                )
            }
            "DeleteMarketplaceModelEndpoint" => {
                crate::marketplace::delete_marketplace_model_endpoint(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            "RegisterMarketplaceModelEndpoint" => {
                crate::marketplace::register_marketplace_model_endpoint(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            "DeregisterMarketplaceModelEndpoint" => {
                crate::marketplace::deregister_marketplace_model_endpoint(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            // Foundation model agreements
            "CreateFoundationModelAgreement" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::foundation_model_agreements::create_foundation_model_agreement(
                    &self.state,
                    &req,
                    &body,
                )
            }
            "DeleteFoundationModelAgreement" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::foundation_model_agreements::delete_foundation_model_agreement(
                    &self.state,
                    &body,
                )
            }
            "ListFoundationModelAgreementOffers" => {
                crate::foundation_model_agreements::list_foundation_model_agreement_offers(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            "GetFoundationModelAvailability" => {
                crate::foundation_model_agreements::get_foundation_model_availability(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            "GetUseCaseForModelAccess" => {
                crate::foundation_model_agreements::get_use_case_for_model_access(&self.state)
            }
            "PutUseCaseForModelAccess" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::foundation_model_agreements::put_use_case_for_model_access(
                    &self.state,
                    &body,
                )
            }
            // Enforced guardrails
            "PutEnforcedGuardrailConfiguration" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::enforced_guardrails::put_enforced_guardrail_configuration(&self.state, &body)
            }
            "ListEnforcedGuardrailsConfiguration" => {
                crate::enforced_guardrails::list_enforced_guardrails_configuration(
                    &self.state,
                    &req,
                )
            }
            "DeleteEnforcedGuardrailConfiguration" => {
                crate::enforced_guardrails::delete_enforced_guardrail_configuration(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            // Automated reasoning policies
            "CreateAutomatedReasoningPolicy" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::automated_reasoning::create_automated_reasoning_policy(
                    &self.state,
                    &req,
                    &body,
                )
            }
            "GetAutomatedReasoningPolicy" => {
                crate::automated_reasoning::get_automated_reasoning_policy(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            "ListAutomatedReasoningPolicies" => {
                crate::automated_reasoning::list_automated_reasoning_policies(&self.state, &req)
            }
            "UpdateAutomatedReasoningPolicy" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::automated_reasoning::update_automated_reasoning_policy(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &body,
                )
            }
            "DeleteAutomatedReasoningPolicy" => {
                crate::automated_reasoning::delete_automated_reasoning_policy(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            "CreateAutomatedReasoningPolicyVersion" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::automated_reasoning::create_automated_reasoning_policy_version(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &body,
                )
            }
            "ExportAutomatedReasoningPolicyVersion" => {
                crate::automated_reasoning::export_automated_reasoning_policy_version(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &req,
                )
            }
            "CreateAutomatedReasoningPolicyTestCase" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::automated_reasoning::create_automated_reasoning_policy_test_case(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &body,
                )
            }
            "GetAutomatedReasoningPolicyTestCase" => {
                crate::automated_reasoning::get_automated_reasoning_policy_test_case(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            "ListAutomatedReasoningPolicyTestCases" => {
                crate::automated_reasoning::list_automated_reasoning_policy_test_cases(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &req,
                )
            }
            "UpdateAutomatedReasoningPolicyTestCase" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::automated_reasoning::update_automated_reasoning_policy_test_case(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                    &body,
                )
            }
            "DeleteAutomatedReasoningPolicyTestCase" => {
                crate::automated_reasoning::delete_automated_reasoning_policy_test_case(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            // Automated reasoning build workflows
            "StartAutomatedReasoningPolicyBuildWorkflow" => {
                crate::automated_reasoning_workflows::start_build_workflow(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            "GetAutomatedReasoningPolicyBuildWorkflow" => {
                crate::automated_reasoning_workflows::get_build_workflow(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            "ListAutomatedReasoningPolicyBuildWorkflows" => {
                crate::automated_reasoning_workflows::list_build_workflows(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &req,
                )
            }
            "CancelAutomatedReasoningPolicyBuildWorkflow" => {
                crate::automated_reasoning_workflows::cancel_build_workflow(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            "DeleteAutomatedReasoningPolicyBuildWorkflow" => {
                crate::automated_reasoning_workflows::delete_build_workflow(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            "GetAutomatedReasoningPolicyBuildWorkflowResultAssets" => {
                crate::automated_reasoning_workflows::get_build_workflow_result_assets(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            "StartAutomatedReasoningPolicyTestWorkflow" => {
                crate::automated_reasoning_workflows::start_test_workflow(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            "GetAutomatedReasoningPolicyTestResult" => {
                let extra = extra_id.unwrap_or_default();
                let parts: Vec<&str> = extra.splitn(2, ':').collect();
                let workflow_id = parts.first().copied().unwrap_or_default();
                let test_case_id = parts.get(1).copied().unwrap_or_default();
                crate::automated_reasoning_workflows::get_test_result(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    workflow_id,
                    test_case_id,
                )
            }
            "ListAutomatedReasoningPolicyTestResults" => {
                crate::automated_reasoning_workflows::list_test_results(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                    &req,
                )
            }
            "GetAutomatedReasoningPolicyAnnotations" => {
                crate::automated_reasoning_workflows::get_annotations(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            "UpdateAutomatedReasoningPolicyAnnotations" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::automated_reasoning_workflows::update_annotations(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                    &body,
                )
            }
            "GetAutomatedReasoningPolicyNextScenario" => {
                crate::automated_reasoning_workflows::get_next_scenario(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    extra_id.as_deref().unwrap_or_default(),
                )
            }
            // Model customization jobs
            "CreateModelCustomizationJob" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::customization::create_model_customization_job(&self.state, &req, &body)
            }
            "GetModelCustomizationJob" => crate::customization::get_model_customization_job(
                &self.state,
                &req,
                &resource_id.unwrap_or_default(),
            ),
            "ListModelCustomizationJobs" => {
                crate::customization::list_model_customization_jobs(&self.state, &req)
            }
            "StopModelCustomizationJob" => crate::customization::stop_model_customization_job(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            // Provisioned model throughput
            "CreateProvisionedModelThroughput" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::throughput::create_provisioned_model_throughput(&self.state, &req, &body)
            }
            "GetProvisionedModelThroughput" => crate::throughput::get_provisioned_model_throughput(
                &self.state,
                &resource_id.unwrap_or_default(),
            ),
            "ListProvisionedModelThroughputs" => {
                crate::throughput::list_provisioned_model_throughputs(&self.state, &req)
            }
            "UpdateProvisionedModelThroughput" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::throughput::update_provisioned_model_throughput(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                    &body,
                )
            }
            "DeleteProvisionedModelThroughput" => {
                crate::throughput::delete_provisioned_model_throughput(
                    &self.state,
                    &resource_id.unwrap_or_default(),
                )
            }
            // Logging configuration
            "PutModelInvocationLoggingConfiguration" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::logging::put_model_invocation_logging_configuration(&self.state, &body)
            }
            "GetModelInvocationLoggingConfiguration" => {
                crate::logging::get_model_invocation_logging_configuration(&self.state)
            }
            "DeleteModelInvocationLoggingConfiguration" => {
                crate::logging::delete_model_invocation_logging_configuration(&self.state)
            }
            // Runtime operations
            "InvokeModel" => crate::invoke::invoke_model(
                &self.state,
                &resource_id.unwrap_or_default(),
                &req.body,
            ),
            "CountTokens" => crate::invoke::count_tokens(
                &self.state,
                &resource_id.unwrap_or_default(),
                &req.body,
            ),
            "Converse" => {
                crate::converse::converse(&self.state, &resource_id.unwrap_or_default(), &req.body)
            }
            "InvokeModelWithResponseStream" | "InvokeModelWithBidirectionalStream" => {
                let model_id = resource_id.unwrap_or_default();
                let response_text = crate::streaming::get_response_text(&self.state, &model_id);
                let body =
                    crate::streaming::build_invoke_stream_response(&model_id, &response_text);

                // Record invocation
                {
                    let mut s = self.state.write();
                    s.invocations.push(crate::state::ModelInvocation {
                        model_id: model_id.clone(),
                        input: String::from_utf8_lossy(&req.body).to_string(),
                        output: response_text,
                        timestamp: chrono::Utc::now(),
                    });
                }

                Ok(AwsResponse {
                    status: http::StatusCode::OK,
                    content_type: "application/vnd.amazon.eventstream".to_string(),
                    body: bytes::Bytes::from(body),
                    headers: http::HeaderMap::new(),
                })
            }
            "ConverseStream" => {
                let model_id = resource_id.unwrap_or_default();
                let response_text = crate::streaming::get_response_text(&self.state, &model_id);
                let body = crate::streaming::build_converse_stream_response(&response_text);

                // Record invocation
                {
                    let mut s = self.state.write();
                    s.invocations.push(crate::state::ModelInvocation {
                        model_id: model_id.clone(),
                        input: String::from_utf8_lossy(&req.body).to_string(),
                        output: response_text,
                        timestamp: chrono::Utc::now(),
                    });
                }

                Ok(AwsResponse {
                    status: http::StatusCode::OK,
                    content_type: "application/vnd.amazon.eventstream".to_string(),
                    body: bytes::Bytes::from(body),
                    headers: http::HeaderMap::new(),
                })
            }
            // Async invoke
            "StartAsyncInvoke" => {
                let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
                crate::async_invoke::start_async_invoke(&self.state, &req, &body)
            }
            "GetAsyncInvoke" => {
                crate::async_invoke::get_async_invoke(&self.state, &resource_id.unwrap_or_default())
            }
            "ListAsyncInvokes" => crate::async_invoke::list_async_invokes(&self.state, &req),

            _ => Err(AwsServiceError::ActionNotImplemented {
                service: "bedrock".to_string(),
                action: action.to_string(),
            }),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "ListFoundationModels",
            "GetFoundationModel",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "CreateGuardrail",
            "GetGuardrail",
            "ListGuardrails",
            "UpdateGuardrail",
            "DeleteGuardrail",
            "CreateGuardrailVersion",
            "ApplyGuardrail",
            "CreateCustomModel",
            "GetCustomModel",
            "ListCustomModels",
            "DeleteCustomModel",
            "CreateCustomModelDeployment",
            "GetCustomModelDeployment",
            "ListCustomModelDeployments",
            "UpdateCustomModelDeployment",
            "DeleteCustomModelDeployment",
            "CreateModelImportJob",
            "GetModelImportJob",
            "ListModelImportJobs",
            "GetImportedModel",
            "ListImportedModels",
            "DeleteImportedModel",
            "CreateModelCopyJob",
            "GetModelCopyJob",
            "ListModelCopyJobs",
            "CreateModelInvocationJob",
            "GetModelInvocationJob",
            "ListModelInvocationJobs",
            "StopModelInvocationJob",
            "CreateEvaluationJob",
            "GetEvaluationJob",
            "ListEvaluationJobs",
            "StopEvaluationJob",
            "BatchDeleteEvaluationJob",
            "CreateInferenceProfile",
            "GetInferenceProfile",
            "ListInferenceProfiles",
            "DeleteInferenceProfile",
            "CreatePromptRouter",
            "GetPromptRouter",
            "ListPromptRouters",
            "DeletePromptRouter",
            "PutResourcePolicy",
            "GetResourcePolicy",
            "DeleteResourcePolicy",
            "CreateMarketplaceModelEndpoint",
            "GetMarketplaceModelEndpoint",
            "ListMarketplaceModelEndpoints",
            "UpdateMarketplaceModelEndpoint",
            "DeleteMarketplaceModelEndpoint",
            "RegisterMarketplaceModelEndpoint",
            "DeregisterMarketplaceModelEndpoint",
            "CreateFoundationModelAgreement",
            "DeleteFoundationModelAgreement",
            "ListFoundationModelAgreementOffers",
            "GetFoundationModelAvailability",
            "GetUseCaseForModelAccess",
            "PutUseCaseForModelAccess",
            "PutEnforcedGuardrailConfiguration",
            "ListEnforcedGuardrailsConfiguration",
            "DeleteEnforcedGuardrailConfiguration",
            "CreateAutomatedReasoningPolicy",
            "GetAutomatedReasoningPolicy",
            "ListAutomatedReasoningPolicies",
            "UpdateAutomatedReasoningPolicy",
            "DeleteAutomatedReasoningPolicy",
            "CreateAutomatedReasoningPolicyVersion",
            "ExportAutomatedReasoningPolicyVersion",
            "CreateAutomatedReasoningPolicyTestCase",
            "GetAutomatedReasoningPolicyTestCase",
            "ListAutomatedReasoningPolicyTestCases",
            "UpdateAutomatedReasoningPolicyTestCase",
            "DeleteAutomatedReasoningPolicyTestCase",
            "StartAutomatedReasoningPolicyBuildWorkflow",
            "GetAutomatedReasoningPolicyBuildWorkflow",
            "ListAutomatedReasoningPolicyBuildWorkflows",
            "CancelAutomatedReasoningPolicyBuildWorkflow",
            "DeleteAutomatedReasoningPolicyBuildWorkflow",
            "GetAutomatedReasoningPolicyBuildWorkflowResultAssets",
            "StartAutomatedReasoningPolicyTestWorkflow",
            "GetAutomatedReasoningPolicyTestResult",
            "ListAutomatedReasoningPolicyTestResults",
            "GetAutomatedReasoningPolicyAnnotations",
            "UpdateAutomatedReasoningPolicyAnnotations",
            "GetAutomatedReasoningPolicyNextScenario",
            "CreateModelCustomizationJob",
            "GetModelCustomizationJob",
            "ListModelCustomizationJobs",
            "StopModelCustomizationJob",
            "CreateProvisionedModelThroughput",
            "GetProvisionedModelThroughput",
            "ListProvisionedModelThroughputs",
            "UpdateProvisionedModelThroughput",
            "DeleteProvisionedModelThroughput",
            "PutModelInvocationLoggingConfiguration",
            "GetModelInvocationLoggingConfiguration",
            "DeleteModelInvocationLoggingConfiguration",
            "InvokeModel",
            "InvokeModelWithResponseStream",
            "InvokeModelWithBidirectionalStream",
            "Converse",
            "ConverseStream",
            "CountTokens",
            "StartAsyncInvoke",
            "GetAsyncInvoke",
            "ListAsyncInvokes",
        ]
    }
}
