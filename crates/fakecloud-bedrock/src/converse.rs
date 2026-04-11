use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

/// Handle the Converse API — unified conversation format across all models.
pub fn converse(
    state: &SharedBedrockState,
    model_id: &str,
    body: &[u8],
) -> Result<AwsResponse, AwsServiceError> {
    let input: Value = serde_json::from_slice(body).unwrap_or_default();

    let response_text = {
        let s = state.read();
        if let Some(custom) = s.custom_responses.get(model_id) {
            // Parse custom response to extract text if it's a Converse-format response
            let parsed: Value = serde_json::from_str(custom).unwrap_or_default();
            if let Some(text) = parsed["output"]["message"]["content"][0]["text"].as_str() {
                text.to_string()
            } else {
                custom.clone()
            }
        } else {
            "This is a test response from the emulated model.".to_string()
        }
    };

    let response = json!({
        "output": {
            "message": {
                "role": "assistant",
                "content": [
                    {
                        "text": response_text
                    }
                ]
            }
        },
        "stopReason": "end_turn",
        "usage": {
            "inputTokens": 10,
            "outputTokens": 20,
            "totalTokens": 30
        },
        "metrics": {
            "latencyMs": 100
        }
    });

    let response_str = serde_json::to_string(&response).unwrap();

    // Record invocation for introspection
    {
        let mut s = state.write();
        s.invocations.push(crate::state::ModelInvocation {
            model_id: model_id.to_string(),
            input: String::from_utf8_lossy(body).to_string(),
            output: response_str.clone(),
            timestamp: Utc::now(),
        });
    }

    let _ = input;

    Ok(AwsResponse::json(StatusCode::OK, response_str))
}
