use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

/// Invoke a model and return a provider-specific canned response.
/// If a custom response has been configured via simulation endpoint, use that instead.
pub fn invoke_model(
    state: &SharedBedrockState,
    model_id: &str,
    body: &[u8],
) -> Result<AwsResponse, AwsServiceError> {
    let input: Value = serde_json::from_slice(body).unwrap_or_default();

    let response_body = {
        let s = state.read();
        if let Some(custom) = s.custom_responses.get(model_id) {
            custom.clone()
        } else {
            generate_canned_response(model_id, &input)
        }
    };

    // Record invocation for introspection
    {
        let mut s = state.write();
        s.invocations.push(crate::state::ModelInvocation {
            model_id: model_id.to_string(),
            input: String::from_utf8_lossy(body).to_string(),
            output: response_body.clone(),
            timestamp: Utc::now(),
        });
    }

    let mut headers = http::HeaderMap::new();
    headers.insert("x-amzn-bedrock-input-token-count", "10".parse().unwrap());
    headers.insert("x-amzn-bedrock-output-token-count", "20".parse().unwrap());

    Ok(AwsResponse {
        status: StatusCode::OK,
        content_type: "application/json".to_string(),
        body: bytes::Bytes::from(response_body),
        headers,
    })
}

/// Generate a deterministic canned response based on the model provider.
fn generate_canned_response(model_id: &str, input: &Value) -> String {
    let provider = if model_id.starts_with("anthropic.") {
        "anthropic"
    } else if model_id.starts_with("amazon.") {
        "amazon"
    } else if model_id.starts_with("meta.") {
        "meta"
    } else if model_id.starts_with("cohere.") {
        "cohere"
    } else if model_id.starts_with("mistral.") {
        "mistral"
    } else {
        "generic"
    };

    match provider {
        "anthropic" => anthropic_response(model_id, input),
        "amazon" => amazon_titan_response(model_id, input),
        "meta" => meta_llama_response(input),
        "cohere" => cohere_response(input),
        "mistral" => mistral_response(input),
        _ => generic_response(input),
    }
}

fn anthropic_response(model_id: &str, _input: &Value) -> String {
    serde_json::to_string(&json!({
        "id": "msg_fakecloudtest01",
        "type": "message",
        "role": "assistant",
        "content": [
            {
                "type": "text",
                "text": "This is a test response from the emulated model."
            }
        ],
        "model": model_id,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": 10,
            "output_tokens": 20
        }
    }))
    .unwrap()
}

fn amazon_titan_response(_model_id: &str, _input: &Value) -> String {
    serde_json::to_string(&json!({
        "inputTextTokenCount": 10,
        "results": [
            {
                "tokenCount": 20,
                "outputText": "This is a test response from the emulated model.",
                "completionReason": "FINISH"
            }
        ]
    }))
    .unwrap()
}

fn meta_llama_response(_input: &Value) -> String {
    serde_json::to_string(&json!({
        "generation": "This is a test response from the emulated model.",
        "prompt_logprobs": null,
        "generation_logprobs": null,
        "stop_reason": "stop",
        "generation_token_count": 20,
        "prompt_token_count": 10
    }))
    .unwrap()
}

fn cohere_response(_input: &Value) -> String {
    serde_json::to_string(&json!({
        "generations": [
            {
                "id": "gen-fakecloud-01",
                "text": "This is a test response from the emulated model.",
                "finish_reason": "COMPLETE",
                "token_likelihoods": []
            }
        ],
        "prompt": ""
    }))
    .unwrap()
}

fn mistral_response(_input: &Value) -> String {
    serde_json::to_string(&json!({
        "outputs": [
            {
                "text": "This is a test response from the emulated model.",
                "stop_reason": "stop"
            }
        ]
    }))
    .unwrap()
}

fn generic_response(_input: &Value) -> String {
    serde_json::to_string(&json!({
        "output": "This is a test response from the emulated model."
    }))
    .unwrap()
}
