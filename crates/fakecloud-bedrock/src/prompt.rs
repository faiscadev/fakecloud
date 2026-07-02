use serde_json::Value;

use fakecloud_core::service::AwsRequest;

use crate::state::{ResponseRule, SharedBedrockState};

/// Approximate token count for an arbitrary text using a whitespace
/// heuristic. Real Bedrock invokes the model's tokenizer; we don't ship
/// one, but SDKs only treat usage as opaque metering, so a stable
/// length-proportional scalar (minimum 1 for any non-empty input) is
/// sufficient. Used everywhere we need to populate `usage.input_tokens`
/// / `usage.output_tokens` from the actual prompt and generated text.
pub(crate) fn count_tokens(text: &str) -> u64 {
    let n = text.split_whitespace().count() as u64;
    if n == 0 && !text.is_empty() {
        1
    } else {
        n
    }
}

/// Read the echo-mode flag from the environment. Accepts either
/// `FAKECLOUD_BEDROCK_ECHO=1` (preferred, matches the project's
/// `FAKECLOUD_*` env-var convention) or the legacy `BEDROCK_ECHO=1`.
pub(crate) fn echo_enabled() -> bool {
    std::env::var("FAKECLOUD_BEDROCK_ECHO").as_deref() == Ok("1")
        || std::env::var("BEDROCK_ECHO").as_deref() == Ok("1")
}

/// Extract the user-visible prompt text from a runtime request body.
///
/// Handles both InvokeModel (provider-specific shapes) and Converse bodies.
/// Returns an empty string when nothing recognizable is found, so a rule
/// with `prompt_contains = ""` matches any call.
pub(crate) fn extract_prompt_text(model_id: &str, body: &[u8]) -> String {
    let Ok(value): Result<Value, _> = serde_json::from_slice(body) else {
        return String::new();
    };

    // Converse shape: top-level `messages` array with `content[].text`,
    // plus optional `system[].text`.
    let mut out = String::new();
    if let Some(system) = value.get("system").and_then(|s| s.as_array()) {
        for block in system {
            if let Some(t) = block.get("text").and_then(|t| t.as_str()) {
                out.push_str(t);
                out.push(' ');
            }
        }
    }
    if let Some(messages) = value.get("messages").and_then(|m| m.as_array()) {
        for msg in messages {
            match msg.get("content") {
                // Converse: content is an array of blocks.
                Some(Value::Array(blocks)) => {
                    for block in blocks {
                        if let Some(t) = block.get("text").and_then(|t| t.as_str()) {
                            out.push_str(t);
                            out.push(' ');
                        }
                    }
                }
                // Anthropic InvokeModel: content may be a plain string.
                Some(Value::String(s)) => {
                    out.push_str(s);
                    out.push(' ');
                }
                _ => {}
            }
        }
    }
    if !out.is_empty() {
        return out.trim_end().to_string();
    }

    // Provider-specific InvokeModel shapes.
    if model_id.starts_with("amazon.") {
        if let Some(t) = value.get("inputText").and_then(|t| t.as_str()) {
            return t.to_string();
        }
    }
    if let Some(t) = value.get("prompt").and_then(|t| t.as_str()) {
        return t.to_string();
    }
    if let Some(t) = value.get("inputText").and_then(|t| t.as_str()) {
        return t.to_string();
    }

    String::new()
}

/// Return the first rule whose `prompt_contains` filter matches the current prompt.
/// A rule with `prompt_contains = None` or an empty string matches anything.
pub(crate) fn match_rule<'a>(rules: &'a [ResponseRule], prompt: &str) -> Option<&'a ResponseRule> {
    rules.iter().find(|rule| match &rule.prompt_contains {
        None => true,
        Some(needle) if needle.is_empty() => true,
        Some(needle) => prompt.contains(needle.as_str()),
    })
}

/// Resolve the response body a runtime call should use, applying
/// rule-based overrides first, then the legacy single-response override.
/// Returns `None` when neither is configured — caller falls back to canned.
pub(crate) fn resolve_override(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
) -> Option<String> {
    let prompt = extract_prompt_text(model_id, body);
    let accts = state.read();
    let empty = crate::state::BedrockState::new(&req.account_id, &req.region);
    let s = accts.get(&req.account_id).unwrap_or(&empty);
    if let Some(rules) = s.response_rules.get(model_id) {
        if let Some(rule) = match_rule(rules, &prompt) {
            return Some(rule.response.clone());
        }
    }
    s.custom_responses.get(model_id).cloned()
}

/// Serializes tests across this crate that mutate the process-global
/// `FAKECLOUD_BEDROCK_ECHO` / `BEDROCK_ECHO` env vars. The `test` CI job runs
/// `cargo test` (one process, threaded), so without this lock the echo-mode
/// tests in `streaming.rs` and `prompt.rs` race on the shared environment.
/// Poison-tolerant: a panicking holder must not wedge the rest of the suite.
#[cfg(test)]
pub(crate) static ECHO_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn shared() -> SharedBedrockState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ),
        ))
    }

    fn req() -> AwsRequest {
        use bytes::Bytes;
        use http::{HeaderMap, Method};
        use std::collections::HashMap;
        AwsRequest {
            service: "bedrock".to_string(),
            action: "a".to_string(),
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
            request_id: "req".to_string(),
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn extract_prompt_from_converse_messages_and_system() {
        let body = br#"{
            "system": [{"text": "sys-prompt"}],
            "messages": [
                {"role": "user", "content": [{"text": "hello"}, {"text": "world"}]}
            ]
        }"#;
        let text = extract_prompt_text("anthropic.claude-v2", body);
        assert_eq!(text, "sys-prompt hello world");
    }

    #[test]
    fn extract_prompt_from_anthropic_invoke_string_content() {
        let body = br#"{"messages":[{"role":"user","content":"hi"}]}"#;
        let text = extract_prompt_text("anthropic.claude-v2", body);
        assert_eq!(text, "hi");
    }

    #[test]
    fn extract_prompt_amazon_input_text() {
        let body = br#"{"inputText":"amazon-prompt"}"#;
        let text = extract_prompt_text("amazon.titan-text", body);
        assert_eq!(text, "amazon-prompt");
    }

    #[test]
    fn extract_prompt_generic_prompt_field() {
        let body = br#"{"prompt":"raw-prompt"}"#;
        let text = extract_prompt_text("anthropic.claude-v1", body);
        assert_eq!(text, "raw-prompt");
    }

    #[test]
    fn extract_prompt_input_text_fallback_for_non_amazon() {
        let body = br#"{"inputText":"generic-input"}"#;
        let text = extract_prompt_text("cohere.command", body);
        assert_eq!(text, "generic-input");
    }

    #[test]
    fn extract_prompt_invalid_json_returns_empty() {
        let text = extract_prompt_text("any", b"{not-json");
        assert_eq!(text, "");
    }

    #[test]
    fn extract_prompt_empty_object_returns_empty() {
        let text = extract_prompt_text("any", b"{}");
        assert_eq!(text, "");
    }

    #[test]
    fn match_rule_none_filter_matches_anything() {
        let rules = vec![ResponseRule {
            prompt_contains: None,
            response: "r1".to_string(),
        }];
        assert!(match_rule(&rules, "x").is_some());
    }

    #[test]
    fn match_rule_empty_filter_matches_anything() {
        let rules = vec![ResponseRule {
            prompt_contains: Some(String::new()),
            response: "r".to_string(),
        }];
        assert!(match_rule(&rules, "").is_some());
    }

    #[test]
    fn match_rule_substring_match() {
        let rules = vec![
            ResponseRule {
                prompt_contains: Some("Bar".to_string()),
                response: "B".to_string(),
            },
            ResponseRule {
                prompt_contains: Some("Foo".to_string()),
                response: "F".to_string(),
            },
        ];
        assert_eq!(match_rule(&rules, "say Foo please").unwrap().response, "F");
        assert_eq!(match_rule(&rules, "try Bar now").unwrap().response, "B");
        assert!(match_rule(&rules, "neither").is_none());
    }

    #[test]
    fn resolve_override_uses_response_rule_first() {
        let state = shared();
        state.write().default_mut().response_rules.insert(
            "m".to_string(),
            vec![ResponseRule {
                prompt_contains: Some("hello".to_string()),
                response: "rule-wins".to_string(),
            }],
        );
        state
            .write()
            .default_mut()
            .custom_responses
            .insert("m".to_string(), "legacy-loses".to_string());
        let body = br#"{"prompt":"hello world"}"#;
        assert_eq!(
            resolve_override(&state, &req(), "m", body).as_deref(),
            Some("rule-wins")
        );
    }

    #[test]
    fn resolve_override_falls_back_to_custom_response() {
        let state = shared();
        state.write().default_mut().response_rules.insert(
            "m".to_string(),
            vec![ResponseRule {
                prompt_contains: Some("notfound".to_string()),
                response: "rule".to_string(),
            }],
        );
        state
            .write()
            .default_mut()
            .custom_responses
            .insert("m".to_string(), "fallback".to_string());
        let body = br#"{"prompt":"hi"}"#;
        assert_eq!(
            resolve_override(&state, &req(), "m", body).as_deref(),
            Some("fallback")
        );
    }

    #[test]
    fn resolve_override_none_when_nothing_configured() {
        let state = shared();
        assert!(resolve_override(&state, &req(), "m", b"{}").is_none());
    }

    #[test]
    fn count_tokens_basic_whitespace_split() {
        assert_eq!(count_tokens(""), 0);
        assert_eq!(count_tokens("hi"), 1);
        assert_eq!(count_tokens("hello world"), 2);
        assert_eq!(count_tokens("a b c d e"), 5);
    }

    #[test]
    fn count_tokens_proportional_to_input_length() {
        // FF1 acceptance: token counts should grow with input size.
        let short = count_tokens("one two");
        let long = count_tokens("one two three four five six seven eight");
        assert!(long > short);
    }

    #[test]
    fn count_tokens_no_whitespace_input_returns_at_least_one() {
        // Pathological input (e.g. one giant word) shouldn't report
        // zero tokens — SDKs treat 0 as "this didn't run".
        assert_eq!(count_tokens("supercalifragilistic"), 1);
    }

    #[test]
    fn echo_enabled_reads_canonical_var() {
        let _env = ECHO_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev_canonical = std::env::var("FAKECLOUD_BEDROCK_ECHO").ok();
        let prev_legacy = std::env::var("BEDROCK_ECHO").ok();
        // SAFETY: env is process-global; we restore both vars below.
        unsafe {
            std::env::remove_var("FAKECLOUD_BEDROCK_ECHO");
            std::env::remove_var("BEDROCK_ECHO");
        }
        assert!(!echo_enabled());
        // SAFETY: see above.
        unsafe { std::env::set_var("FAKECLOUD_BEDROCK_ECHO", "1") };
        assert!(echo_enabled());

        // SAFETY: see above.
        unsafe {
            match prev_canonical {
                Some(v) => std::env::set_var("FAKECLOUD_BEDROCK_ECHO", v),
                None => std::env::remove_var("FAKECLOUD_BEDROCK_ECHO"),
            }
            match prev_legacy {
                Some(v) => std::env::set_var("BEDROCK_ECHO", v),
                None => std::env::remove_var("BEDROCK_ECHO"),
            }
        }
    }
}
