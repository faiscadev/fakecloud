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

    /// Determine the action from the HTTP method and path segments.
    /// Bedrock control plane uses REST-JSON routing:
    ///   GET    /foundation-models                                  -> ListFoundationModels
    ///   GET    /foundation-models/{modelIdentifier}                -> GetFoundationModel
    ///   POST   /guardrails                                         -> CreateGuardrail
    ///   GET    /guardrails                                          -> ListGuardrails
    ///   GET    /guardrails/{guardrailIdentifier}                   -> GetGuardrail
    ///   PUT    /guardrails/{guardrailIdentifier}                   -> UpdateGuardrail
    ///   DELETE /guardrails/{guardrailIdentifier}                   -> DeleteGuardrail
    ///   POST   /guardrails/{guardrailIdentifier}                   -> CreateGuardrailVersion
    ///   POST   /model-customization-jobs                           -> CreateModelCustomizationJob
    ///   GET    /model-customization-jobs                            -> ListModelCustomizationJobs
    ///   GET    /model-customization-jobs/{jobIdentifier}           -> GetModelCustomizationJob
    ///   POST   /model-customization-jobs/{jobIdentifier}/stop      -> StopModelCustomizationJob
    ///   POST   /provisioned-model-throughput                       -> CreateProvisionedModelThroughput
    ///   GET    /provisioned-model-throughputs                       -> ListProvisionedModelThroughputs
    ///   GET    /provisioned-model-throughput/{provisionedModelId}  -> GetProvisionedModelThroughput
    ///   PATCH  /provisioned-model-throughput/{provisionedModelId}  -> UpdateProvisionedModelThroughput
    ///   DELETE /provisioned-model-throughput/{provisionedModelId}  -> DeleteProvisionedModelThroughput
    ///   PUT    /logging/modelinvocations                            -> PutModelInvocationLoggingConfiguration
    ///   GET    /logging/modelinvocations                            -> GetModelInvocationLoggingConfiguration
    ///   DELETE /logging/modelinvocations                            -> DeleteModelInvocationLoggingConfiguration
    ///   POST   /tagResource                                          -> TagResource
    ///   POST   /untagResource                                        -> UntagResource
    ///   POST   /listTagsForResource                                  -> ListTagsForResource
    fn resolve_action(req: &AwsRequest) -> Option<(&str, Option<String>)> {
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
                Some(("ListFoundationModels", None))
            }
            (Method::GET, 2) if segs[0] == "foundation-models" => {
                Some(("GetFoundationModel", Some(decode(&segs[1]))))
            }

            // Guardrails
            (Method::POST, 1) if segs[0] == "guardrails" => Some(("CreateGuardrail", None)),
            (Method::GET, 1) if segs[0] == "guardrails" => Some(("ListGuardrails", None)),
            (Method::GET, 2) if segs[0] == "guardrails" => {
                Some(("GetGuardrail", Some(decode(&segs[1]))))
            }
            (Method::PUT, 2) if segs[0] == "guardrails" => {
                Some(("UpdateGuardrail", Some(decode(&segs[1]))))
            }
            (Method::DELETE, 2) if segs[0] == "guardrails" => {
                Some(("DeleteGuardrail", Some(decode(&segs[1]))))
            }
            // POST /guardrails/{id} -> CreateGuardrailVersion (distinguished from CreateGuardrail by path length)
            (Method::POST, 2) if segs[0] == "guardrails" => {
                Some(("CreateGuardrailVersion", Some(decode(&segs[1]))))
            }
            // Model customization jobs
            (Method::POST, 1) if segs[0] == "model-customization-jobs" => {
                Some(("CreateModelCustomizationJob", None))
            }
            (Method::GET, 1) if segs[0] == "model-customization-jobs" => {
                Some(("ListModelCustomizationJobs", None))
            }
            (Method::GET, 2) if segs[0] == "model-customization-jobs" => {
                Some(("GetModelCustomizationJob", Some(decode(&segs[1]))))
            }
            (Method::POST, 3) if segs[0] == "model-customization-jobs" && segs[2] == "stop" => {
                Some(("StopModelCustomizationJob", Some(decode(&segs[1]))))
            }

            // Provisioned model throughput
            (Method::POST, 1) if segs[0] == "provisioned-model-throughput" => {
                Some(("CreateProvisionedModelThroughput", None))
            }
            (Method::GET, 1) if segs[0] == "provisioned-model-throughputs" => {
                Some(("ListProvisionedModelThroughputs", None))
            }
            (Method::GET, 2) if segs[0] == "provisioned-model-throughput" => {
                Some(("GetProvisionedModelThroughput", Some(decode(&segs[1]))))
            }
            (Method::PATCH, 2) if segs[0] == "provisioned-model-throughput" => {
                Some(("UpdateProvisionedModelThroughput", Some(decode(&segs[1]))))
            }
            (Method::DELETE, 2) if segs[0] == "provisioned-model-throughput" => {
                Some(("DeleteProvisionedModelThroughput", Some(decode(&segs[1]))))
            }

            // Logging configuration
            (Method::PUT, 2) if segs[0] == "logging" && segs[1] == "modelinvocations" => {
                Some(("PutModelInvocationLoggingConfiguration", None))
            }
            (Method::GET, 2) if segs[0] == "logging" && segs[1] == "modelinvocations" => {
                Some(("GetModelInvocationLoggingConfiguration", None))
            }
            (Method::DELETE, 2) if segs[0] == "logging" && segs[1] == "modelinvocations" => {
                Some(("DeleteModelInvocationLoggingConfiguration", None))
            }

            // Runtime operations (same SigV4 service name "bedrock")
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "invoke" => {
                Some(("InvokeModel", Some(decode(&segs[1]))))
            }
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "invoke-with-response-stream" => {
                Some(("InvokeModelWithResponseStream", Some(decode(&segs[1]))))
            }
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "converse" => {
                Some(("Converse", Some(decode(&segs[1]))))
            }
            (Method::POST, 3) if segs[0] == "model" && segs[2] == "converse-stream" => {
                Some(("ConverseStream", Some(decode(&segs[1]))))
            }

            // Tags — all POST with ARN in body
            (Method::POST, 1) if segs[0] == "tagResource" => Some(("TagResource", None)),
            (Method::POST, 1) if segs[0] == "untagResource" => Some(("UntagResource", None)),
            (Method::POST, 1) if segs[0] == "listTagsForResource" => {
                Some(("ListTagsForResource", None))
            }

            _ => None,
        }
    }

    fn list_foundation_models(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut model_summaries: Vec<Value> = Vec::new();

        // Optional filters from query parameters
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
        let (action, resource_id) =
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
                let arn = body["resourceARN"].as_str().unwrap_or_default();
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
            "ListGuardrails" => crate::guardrails::list_guardrails(&self.state),
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
                crate::customization::list_model_customization_jobs(&self.state)
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
                crate::throughput::list_provisioned_model_throughputs(&self.state)
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
            "Converse" => {
                crate::converse::converse(&self.state, &resource_id.unwrap_or_default(), &req.body)
            }
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
            "Converse",
        ]
    }
}
