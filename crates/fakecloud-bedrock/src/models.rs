use serde_json::{json, Value};

/// A foundation model in the catalog.
pub struct FoundationModel {
    pub model_id: &'static str,
    pub model_name: &'static str,
    pub provider_name: &'static str,
    pub input_modalities: &'static [&'static str],
    pub output_modalities: &'static [&'static str],
    pub customizations_supported: &'static [&'static str],
    pub inference_types_supported: &'static [&'static str],
    pub model_lifecycle_status: &'static str,
    pub response_streaming_supported: bool,
}

impl FoundationModel {
    pub fn to_summary_json(&self) -> Value {
        json!({
            "modelId": self.model_id,
            "modelName": self.model_name,
            "providerName": self.provider_name,
            "inputModalities": self.input_modalities,
            "outputModalities": self.output_modalities,
            "customizationsSupported": self.customizations_supported,
            "inferenceTypesSupported": self.inference_types_supported,
            "modelLifecycle": {
                "status": self.model_lifecycle_status
            },
            "responseStreamingSupported": self.response_streaming_supported,
        })
    }

    pub fn to_detail_json(&self, region: &str, account_id: &str) -> Value {
        let arn = format!(
            "arn:aws:bedrock:{}::foundation-model/{}",
            region, self.model_id
        );
        let _ = account_id; // ARN uses :: (no account) for foundation models
        json!({
            "modelDetails": {
                "modelArn": arn,
                "modelId": self.model_id,
                "modelName": self.model_name,
                "providerName": self.provider_name,
                "inputModalities": self.input_modalities,
                "outputModalities": self.output_modalities,
                "customizationsSupported": self.customizations_supported,
                "inferenceTypesSupported": self.inference_types_supported,
                "modelLifecycle": {
                    "status": self.model_lifecycle_status
                },
                "responseStreamingSupported": self.response_streaming_supported,
            }
        })
    }
}

/// The hardcoded catalog of foundation models available in the emulator.
pub static FOUNDATION_MODELS: &[FoundationModel] = &[
    // Anthropic Claude models
    FoundationModel {
        model_id: "anthropic.claude-3-5-sonnet-20241022-v2:0",
        model_name: "Claude 3.5 Sonnet v2",
        provider_name: "Anthropic",
        input_modalities: &["TEXT", "IMAGE"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND", "PROVISIONED"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "anthropic.claude-3-5-haiku-20241022-v1:0",
        model_name: "Claude 3.5 Haiku",
        provider_name: "Anthropic",
        input_modalities: &["TEXT", "IMAGE"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND", "PROVISIONED"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "anthropic.claude-3-opus-20240229-v1:0",
        model_name: "Claude 3 Opus",
        provider_name: "Anthropic",
        input_modalities: &["TEXT", "IMAGE"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "anthropic.claude-3-sonnet-20240229-v1:0",
        model_name: "Claude 3 Sonnet",
        provider_name: "Anthropic",
        input_modalities: &["TEXT", "IMAGE"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "anthropic.claude-3-haiku-20240307-v1:0",
        model_name: "Claude 3 Haiku",
        provider_name: "Anthropic",
        input_modalities: &["TEXT", "IMAGE"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "anthropic.claude-v2:1",
        model_name: "Claude v2.1",
        provider_name: "Anthropic",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    // Amazon Titan models
    FoundationModel {
        model_id: "amazon.titan-text-express-v1",
        model_name: "Titan Text G1 - Express",
        provider_name: "Amazon",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &["FINE_TUNING"],
        inference_types_supported: &["ON_DEMAND", "PROVISIONED"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "amazon.titan-text-lite-v1",
        model_name: "Titan Text G1 - Lite",
        provider_name: "Amazon",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &["FINE_TUNING"],
        inference_types_supported: &["ON_DEMAND", "PROVISIONED"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "amazon.titan-embed-text-v1",
        model_name: "Titan Embeddings G1 - Text",
        provider_name: "Amazon",
        input_modalities: &["TEXT"],
        output_modalities: &["EMBEDDING"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: false,
    },
    // Meta Llama models
    FoundationModel {
        model_id: "meta.llama3-1-70b-instruct-v1:0",
        model_name: "Llama 3.1 70B Instruct",
        provider_name: "Meta",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "meta.llama3-1-8b-instruct-v1:0",
        model_name: "Llama 3.1 8B Instruct",
        provider_name: "Meta",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    // Cohere models
    FoundationModel {
        model_id: "cohere.command-r-plus-v1:0",
        model_name: "Command R+",
        provider_name: "Cohere",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "cohere.command-r-v1:0",
        model_name: "Command R",
        provider_name: "Cohere",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    // Mistral models
    FoundationModel {
        model_id: "mistral.mistral-large-2407-v1:0",
        model_name: "Mistral Large (2407)",
        provider_name: "Mistral AI",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
    FoundationModel {
        model_id: "mistral.mixtral-8x7b-instruct-v0:1",
        model_name: "Mixtral 8x7B Instruct",
        provider_name: "Mistral AI",
        input_modalities: &["TEXT"],
        output_modalities: &["TEXT"],
        customizations_supported: &[],
        inference_types_supported: &["ON_DEMAND"],
        model_lifecycle_status: "ACTIVE",
        response_streaming_supported: true,
    },
];

/// Find a foundation model by its model ID.
pub fn find_model(model_id: &str) -> Option<&'static FoundationModel> {
    FOUNDATION_MODELS.iter().find(|m| m.model_id == model_id)
}
