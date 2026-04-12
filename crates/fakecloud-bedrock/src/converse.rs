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

    let inference_config = input.get("inferenceConfig");
    let max_tokens = inference_config
        .and_then(|c| c["maxTokens"].as_u64())
        .unwrap_or(u64::MAX);
    let tool_config = input.get("toolConfig");

    let response_text = {
        let s = state.read();
        if let Some(custom) = s.custom_responses.get(model_id) {
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

    // Respect maxTokens by truncating (rough approximation: 1 token ~= 4 chars)
    let truncated_text = if max_tokens < u64::MAX {
        let char_limit = (max_tokens as usize) * 4;
        if response_text.chars().count() > char_limit {
            response_text.chars().take(char_limit).collect::<String>()
        } else {
            response_text
        }
    } else {
        response_text
    };

    // Build content blocks
    let mut content = vec![json!({"text": truncated_text})];

    // If toolConfig is provided with tools, include a toolUse block
    if let Some(tc) = tool_config {
        if let Some(tools) = tc["tools"].as_array() {
            if let Some(first_tool) = tools.first() {
                if let Some(tool_spec) = first_tool.get("toolSpec") {
                    let tool_name = tool_spec["name"].as_str().unwrap_or("tool");
                    content.push(json!({
                        "toolUse": {
                            "toolUseId": "tooluse_fakecloud_01",
                            "name": tool_name,
                            "input": {}
                        }
                    }));
                }
            }
        }
    }

    let stop_reason = if tool_config.is_some()
        && content.len() > 1
        && content
            .last()
            .map(|c| c.get("toolUse").is_some())
            .unwrap_or(false)
    {
        "tool_use"
    } else {
        "end_turn"
    };

    let input_tokens = estimate_tokens(&input);
    let output_tokens = truncated_text.split_whitespace().count().max(1) as u64;

    let response = json!({
        "output": {
            "message": {
                "role": "assistant",
                "content": content
            }
        },
        "stopReason": stop_reason,
        "usage": {
            "inputTokens": input_tokens,
            "outputTokens": output_tokens,
            "totalTokens": input_tokens + output_tokens
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

    Ok(AwsResponse::json(StatusCode::OK, response_str))
}

fn estimate_tokens(input: &Value) -> u64 {
    let mut text_len = 0usize;

    // Count system prompt tokens
    if let Some(system) = input.get("system").and_then(|s| s.as_array()) {
        for block in system {
            if let Some(text) = block["text"].as_str() {
                text_len += text.len();
            }
        }
    }

    // Count message tokens
    if let Some(messages) = input.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            if let Some(content) = msg.get("content").and_then(|c| c.as_array()) {
                for block in content {
                    if let Some(text) = block["text"].as_str() {
                        text_len += text.len();
                    }
                }
            }
        }
    }

    // Rough approximation: 1 token ~= 4 characters, minimum 1
    (text_len / 4).max(1) as u64
}
