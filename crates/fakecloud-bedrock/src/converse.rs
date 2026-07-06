use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

/// Handle the Converse API — unified conversation format across all models.
pub(crate) fn converse(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
) -> Result<AwsResponse, AwsServiceError> {
    if let Some(fault) = crate::faults::take_matching_fault(state, req, model_id, "Converse") {
        crate::faults::record_faulted_invocation(state, req, model_id, body, &fault);
        return Err(crate::faults::fault_to_error(&fault));
    }

    let input: Value = serde_json::from_slice(body).unwrap_or_default();

    let inference_config = input.get("inferenceConfig");
    let max_tokens = inference_config
        .and_then(|c| c["maxTokens"].as_u64())
        .unwrap_or(u64::MAX);
    let tool_config = input.get("toolConfig");

    let response_text = match crate::prompt::resolve_override(state, req, model_id, body) {
        Some(custom) => {
            let parsed: Value = serde_json::from_str(&custom).unwrap_or_default();
            if let Some(text) = parsed["output"]["message"]["content"][0]["text"].as_str() {
                text.to_string()
            } else {
                custom
            }
        }
        None => {
            // Echo mode short-circuits the canned phrase: reflect the
            // caller's prompt back as the assistant text so tests can
            // assert against their own input.
            if crate::prompt::echo_enabled() {
                let echoed = crate::prompt::extract_prompt_text(model_id, body);
                if echoed.is_empty() {
                    "(empty prompt)".to_string()
                } else {
                    echoed
                }
            } else {
                "This is a test response from the emulated model.".to_string()
            }
        }
    };

    // Respect maxTokens by truncating (rough approximation: 1 token ~= 4 chars).
    // `saturating_mul` keeps a hostile `maxTokens` (e.g. `u64::MAX - 1` sent
    // over raw HTTP) from overflowing `usize` — a debug-build panic (dropped
    // connection / 500) or a release-build wrap to a tiny garbage limit.
    let (truncated_text, truncated) = if max_tokens < u64::MAX {
        let char_limit = (max_tokens as usize).saturating_mul(4);
        if response_text.chars().count() > char_limit {
            (
                response_text.chars().take(char_limit).collect::<String>(),
                true,
            )
        } else {
            (response_text, false)
        }
    } else {
        (response_text, false)
    };

    // Build content blocks
    let mut content = vec![json!({"text": truncated_text})];

    // Only emit a toolUse block when the caller's `toolChoice` actually
    // requires one. A bare `toolConfig` (or `toolChoice:{auto:{}}`) leaves the
    // decision to the model, and offline we choose a plain text reply — forcing
    // an empty-arg tool call here derails LangChain / agent tool-loops.
    let forced_tool = forced_tool_name(tool_config);
    if let Some(tool_name) = &forced_tool {
        content.push(json!({
            "toolUse": {
                "toolUseId": "tooluse_fakecloud_01",
                "name": tool_name,
                "input": {}
            }
        }));
    }

    let stop_reason = if forced_tool.is_some() {
        "tool_use"
    } else if truncated {
        // Report the real reason the reply ended so tool/agent loops see the
        // truncation, matching the upstream path's `normalize_stop`.
        "max_tokens"
    } else {
        "end_turn"
    };

    let input_tokens = estimate_tokens(&input);
    let output_tokens = crate::prompt::count_tokens(&truncated_text).max(1);

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
        let mut accts = state.write();
        let s = accts.get_or_create(&req.account_id);
        s.invocations.push(crate::state::ModelInvocation {
            model_id: model_id.to_string(),
            input: String::from_utf8_lossy(body).to_string(),
            output: response_str.clone(),
            timestamp: Utc::now(),
            error: None,
        });
    }

    Ok(AwsResponse::json(StatusCode::OK, response_str))
}

/// Resolve the tool the caller *forced* the model to call via the Converse
/// `toolConfig.toolChoice`, if any.
///
/// Bedrock's `toolChoice` is one of `{auto:{}}`, `{any:{}}` or
/// `{tool:{name:...}}`. Only `any` (call some tool) and `tool` (call this
/// specific tool) force a tool call; `auto` — and an absent `toolChoice` —
/// let the model decide, which offline means a normal text reply. Returns the
/// tool name to invoke, or `None` when no tool is forced or no tools are
/// available. Shared by `Converse` and `ConverseStream` so both agree.
pub(crate) fn forced_tool_name(tool_config: Option<&Value>) -> Option<String> {
    let tc = tool_config?;
    let tools = tc.get("tools").and_then(|t| t.as_array())?;
    match tc.get("toolChoice") {
        // Force a specific named tool.
        Some(choice) if choice.get("tool").is_some() => {
            if let Some(name) = choice["tool"]["name"].as_str() {
                return Some(name.to_string());
            }
            // Malformed `{tool:{}}` with no name: fall back to the first tool.
            first_tool_name(tools)
        }
        // Force any tool: pick the first advertised one.
        Some(choice) if choice.get("any").is_some() => first_tool_name(tools),
        // `auto` or absent: the model decides; offline we reply with text.
        _ => None,
    }
}

fn first_tool_name(tools: &[Value]) -> Option<String> {
    tools
        .first()?
        .get("toolSpec")?
        .get("name")?
        .as_str()
        .map(|s| s.to_string())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::BedrockState;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::{Mutex, RwLock};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::OnceLock;

    /// Serialize tests in this module that observe the process-global
    /// `FAKECLOUD_BEDROCK_ECHO` flag.
    fn echo_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn shared() -> SharedBedrockState {
        let multi: MultiAccountState<BedrockState> =
            MultiAccountState::new("123456789012", "us-east-1", "http://x");
        Arc::new(RwLock::new(multi))
    }

    fn req() -> AwsRequest {
        AwsRequest {
            service: "bedrock".to_string(),
            action: "Converse".to_string(),
            method: Method::POST,
            raw_path: "/".to_string(),
            raw_query: String::new(),
            path_segments: vec![],
            query_params: HashMap::new(),
            headers: HeaderMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            account_id: "123456789012".to_string(),
            region: "us-east-1".to_string(),
            request_id: "r".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn estimate_tokens_empty_returns_min_one() {
        let v = json!({});
        assert_eq!(estimate_tokens(&v), 1);
    }

    #[test]
    fn estimate_tokens_counts_system_and_messages() {
        let v = json!({
            "system": [{"text": "12345678"}],
            "messages": [{"content": [{"text": "abcdefgh"}]}]
        });
        assert_eq!(estimate_tokens(&v), 4); // 16 chars / 4
    }

    #[test]
    fn converse_default_response_no_fault_no_override() {
        let s = shared();
        let body = br#"{"messages":[{"content":[{"text":"hi"}]}]}"#;
        let resp = converse(&s, &req(), "anthropic.claude-v2", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["output"]["message"]["role"], "assistant");
        assert_eq!(v["stopReason"], "end_turn");
        assert!(v["usage"]["totalTokens"].is_u64());
    }

    #[test]
    fn converse_truncates_via_max_tokens() {
        let s = shared();
        let body = br#"{"messages":[], "inferenceConfig": {"maxTokens": 2}}"#;
        let resp = converse(&s, &req(), "m", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        let text = v["output"]["message"]["content"][0]["text"]
            .as_str()
            .unwrap();
        // 2 tokens * 4 chars = 8-char cap
        assert!(text.len() <= 8);
    }

    #[test]
    fn converse_tool_config_without_choice_does_not_force_tool() {
        // A bare toolConfig (no toolChoice / implicit "auto") must NOT emit a
        // spurious empty-arg toolUse — that used to break agent tool-loops.
        let s = shared();
        let body = br#"{
            "messages": [],
            "toolConfig": {"tools": [{"toolSpec": {"name": "calculator"}}]}
        }"#;
        let resp = converse(&s, &req(), "m", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["stopReason"], "end_turn");
        let content = v["output"]["message"]["content"].as_array().unwrap();
        assert_eq!(content.len(), 1);
        assert!(content[0].get("toolUse").is_none());
    }

    #[test]
    fn converse_tool_choice_any_forces_first_tool() {
        let s = shared();
        let body = br#"{
            "messages": [],
            "toolConfig": {
                "tools": [{"toolSpec": {"name": "calculator"}}],
                "toolChoice": {"any": {}}
            }
        }"#;
        let resp = converse(&s, &req(), "m", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["stopReason"], "tool_use");
        let content = v["output"]["message"]["content"].as_array().unwrap();
        assert_eq!(content.len(), 2);
        assert_eq!(content[1]["toolUse"]["name"], "calculator");
    }

    #[test]
    fn converse_tool_choice_named_tool_is_honored() {
        let s = shared();
        let body = br#"{
            "messages": [],
            "toolConfig": {
                "tools": [
                    {"toolSpec": {"name": "calculator"}},
                    {"toolSpec": {"name": "weather"}}
                ],
                "toolChoice": {"tool": {"name": "weather"}}
            }
        }"#;
        let resp = converse(&s, &req(), "m", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["stopReason"], "tool_use");
        assert_eq!(
            v["output"]["message"]["content"][1]["toolUse"]["name"],
            "weather"
        );
    }

    #[test]
    fn converse_huge_max_tokens_does_not_overflow() {
        // maxTokens just below u64::MAX would overflow `(x as usize) * 4`;
        // saturating arithmetic must keep this a normal, un-truncated reply.
        let s = shared();
        let body = format!(
            r#"{{"messages":[],"inferenceConfig":{{"maxTokens":{}}}}}"#,
            u64::MAX - 1
        );
        let resp = converse(&s, &req(), "m", body.as_bytes()).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        // No truncation happened, so it's a normal end_turn reply.
        assert_eq!(v["stopReason"], "end_turn");
        assert!(!v["output"]["message"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn converse_reports_max_tokens_stop_reason_on_truncation() {
        let s = shared();
        let body = br#"{"messages":[], "inferenceConfig": {"maxTokens": 1}}"#;
        let resp = converse(&s, &req(), "m", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        // The canned reply is longer than 1*4 chars, so it was truncated.
        assert_eq!(v["stopReason"], "max_tokens");
        let text = v["output"]["message"]["content"][0]["text"]
            .as_str()
            .unwrap();
        assert!(text.chars().count() <= 4);
    }

    #[test]
    fn forced_tool_name_respects_tool_choice() {
        let cfg = json!({
            "tools": [{"toolSpec": {"name": "a"}}, {"toolSpec": {"name": "b"}}]
        });
        // No toolChoice -> not forced.
        assert_eq!(forced_tool_name(Some(&cfg)), None);
        // auto -> not forced.
        let mut auto = cfg.clone();
        auto["toolChoice"] = json!({"auto": {}});
        assert_eq!(forced_tool_name(Some(&auto)), None);
        // any -> first tool.
        let mut any = cfg.clone();
        any["toolChoice"] = json!({"any": {}});
        assert_eq!(forced_tool_name(Some(&any)), Some("a".to_string()));
        // named tool -> that tool.
        let mut named = cfg.clone();
        named["toolChoice"] = json!({"tool": {"name": "b"}});
        assert_eq!(forced_tool_name(Some(&named)), Some("b".to_string()));
        // No config at all -> None.
        assert_eq!(forced_tool_name(None), None);
    }

    #[test]
    fn converse_records_invocation() {
        let s = shared();
        let body = br#"{"messages":[]}"#;
        converse(&s, &req(), "model-x", body).unwrap();
        let state = s.read();
        let acct = state.default_ref();
        assert_eq!(acct.invocations.len(), 1);
        assert_eq!(acct.invocations[0].model_id, "model-x");
        assert!(acct.invocations[0].error.is_none());
    }

    #[test]
    fn converse_uses_response_rule_override() {
        let s = shared();
        s.write()
            .default_mut()
            .custom_responses
            .insert("model-y".to_string(), "override-output".to_string());
        let body = br#"{"messages":[]}"#;
        let resp = converse(&s, &req(), "model-y", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(
            v["output"]["message"]["content"][0]["text"],
            "override-output"
        );
    }

    #[test]
    fn converse_override_with_nested_text_extracts_from_json() {
        let s = shared();
        let payload = r#"{"output":{"message":{"content":[{"text":"nested-hello"}]}}}"#;
        s.write()
            .default_mut()
            .custom_responses
            .insert("model-z".to_string(), payload.to_string());
        let body = br#"{"messages":[]}"#;
        let resp = converse(&s, &req(), "model-z", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["output"]["message"]["content"][0]["text"], "nested-hello");
    }

    #[test]
    fn converse_echo_mode_reflects_prompt() {
        let _g = echo_lock().lock();
        let prev = std::env::var("FAKECLOUD_BEDROCK_ECHO").ok();
        // SAFETY: the lock above pins us to a single mutation window.
        unsafe { std::env::set_var("FAKECLOUD_BEDROCK_ECHO", "1") };

        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":[{"text":"hello echo"}]}]}"#;
        let resp = converse(&s, &req(), "anthropic.claude-v2", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        let text = v["output"]["message"]["content"][0]["text"]
            .as_str()
            .unwrap();
        assert!(text.contains("hello echo"), "echo missing prompt: {text}");

        // SAFETY: see above.
        unsafe {
            match prev {
                Some(p) => std::env::set_var("FAKECLOUD_BEDROCK_ECHO", p),
                None => std::env::remove_var("FAKECLOUD_BEDROCK_ECHO"),
            }
        }
    }

    #[test]
    fn converse_token_counts_scale_with_input_length() {
        let _g = echo_lock().lock();
        let s = shared();
        let short = br#"{"messages":[{"content":[{"text":"hi"}]}]}"#;
        let long = br#"{"messages":[{"content":[{"text":"please count many words across this longer message body"}]}]}"#;
        let r1 = converse(&s, &req(), "m", short).unwrap();
        let r2 = converse(&s, &req(), "m", long).unwrap();
        let v1: Value =
            serde_json::from_str(std::str::from_utf8(r1.body.expect_bytes()).unwrap()).unwrap();
        let v2: Value =
            serde_json::from_str(std::str::from_utf8(r2.body.expect_bytes()).unwrap()).unwrap();
        let in1 = v1["usage"]["inputTokens"].as_u64().unwrap();
        let in2 = v2["usage"]["inputTokens"].as_u64().unwrap();
        assert!(
            in2 > in1,
            "longer prompt should produce more input tokens (got {in1} vs {in2})"
        );
    }

    #[test]
    fn converse_returns_fault_when_matched_and_records_error_invocation() {
        let s = shared();
        s.write()
            .default_mut()
            .fault_rules
            .push(crate::state::FaultRule {
                error_type: "Throttled".to_string(),
                message: "slow".to_string(),
                http_status: 429,
                remaining: 1,
                model_id: None,
                operation: None,
            });
        let body = br#"{"messages":[]}"#;
        let err = converse(&s, &req(), "m", body).err().unwrap();
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
        let state = s.read();
        let acct = state.default_ref();
        assert_eq!(acct.invocations.len(), 1);
        assert!(acct.invocations[0].error.is_some());
    }
}
