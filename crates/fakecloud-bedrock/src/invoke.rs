use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

/// Invoke a model and return a provider-specific canned response.
/// If a custom response has been configured via simulation endpoint, use that instead.
pub(crate) fn invoke_model(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
) -> Result<AwsResponse, AwsServiceError> {
    // Validate model ID
    if model_id.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "modelId is required",
        ));
    }

    // Fault injection: if a matching rule is queued, record the attempt and fail.
    if let Some(fault) = crate::faults::take_matching_fault(state, req, model_id, "InvokeModel") {
        crate::faults::record_faulted_invocation(state, req, model_id, body, &fault);
        return Err(crate::faults::fault_to_error(&fault));
    }

    let input: Value = serde_json::from_slice(body).unwrap_or_default();

    let response_body = crate::prompt::resolve_override(state, req, model_id, body)
        .unwrap_or_else(|| generate_canned_response(model_id, &input));

    Ok(respond_with_body(
        state,
        req,
        model_id,
        body,
        &input,
        response_body,
    ))
}

/// Record an InvokeModel invocation for introspection.
pub(crate) fn record_invocation(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
    output: &str,
    error: Option<String>,
) {
    let mut accts = state.write();
    let s = accts.get_or_create(&req.account_id);
    s.invocations.push(crate::state::ModelInvocation {
        model_id: model_id.to_string(),
        input: String::from_utf8_lossy(body).to_string(),
        output: output.to_string(),
        timestamp: Utc::now(),
        error,
    });
}

/// Build the runtime `AwsResponse` for a completed InvokeModel call,
/// surfacing the supplied token counts through the standard Bedrock
/// `x-amzn-bedrock-*-token-count` headers.
pub(crate) fn runtime_response_with_tokens(
    response_body: String,
    input_tokens: u64,
    output_tokens: u64,
) -> AwsResponse {
    let mut headers = http::HeaderMap::new();
    headers.insert(
        "x-amzn-bedrock-input-token-count",
        http::HeaderValue::from_str(&input_tokens.to_string())
            .unwrap_or(http::HeaderValue::from_static("0")),
    );
    headers.insert(
        "x-amzn-bedrock-output-token-count",
        http::HeaderValue::from_str(&output_tokens.to_string())
            .unwrap_or(http::HeaderValue::from_static("0")),
    );
    headers.insert(
        "x-amzn-bedrock-performanceconfig-latency",
        http::HeaderValue::from_static("standard"),
    );

    AwsResponse {
        status: StatusCode::OK,
        content_type: "application/json".to_string(),
        body: bytes::Bytes::from(response_body).into(),
        headers,
    }
}

/// Record the invocation and build the runtime response, deriving token
/// counts from the request prompt and generated text. Used by the
/// deterministic (canned/override/echo) path where no real upstream usage
/// figures are available.
pub(crate) fn respond_with_body(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
    input: &Value,
    response_body: String,
) -> AwsResponse {
    record_invocation(state, req, model_id, body, &response_body, None);

    // Real Bedrock returns dynamic input/output token counts. Approximate
    // each by whitespace-splitting the relevant text — close enough for
    // SDKs that gate on usage.input_tokens / usage.output_tokens metering.
    let input_tokens = crate::prompt::count_tokens(&extract_input_text(input));
    let output_tokens = crate::prompt::count_tokens(&extract_output_text(model_id, &response_body));

    runtime_response_with_tokens(response_body, input_tokens, output_tokens)
}

/// Pull the user-supplied prompt out of an InvokeModel request body.
/// Each provider lays it out differently; we cover the most common
/// shapes and fall back to the raw body length when nothing matches.
pub(crate) fn extract_input_text(input: &Value) -> String {
    // Anthropic: messages[] with role+content (string or array of content blocks)
    if let Some(messages) = input.get("messages").and_then(|m| m.as_array()) {
        let mut text = String::new();
        for msg in messages {
            match &msg["content"] {
                Value::String(s) => {
                    text.push_str(s);
                    text.push(' ');
                }
                Value::Array(parts) => {
                    for part in parts {
                        if let Some(t) = part["text"].as_str() {
                            text.push_str(t);
                            text.push(' ');
                        }
                    }
                }
                _ => {}
            }
        }
        if !text.is_empty() {
            return text;
        }
    }
    // Anthropic claim/system field
    if let Some(s) = input.get("system").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    // Titan / Cohere / Meta single-prompt shape
    if let Some(s) = input.get("prompt").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    if let Some(s) = input.get("inputText").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    // Cohere generate
    if let Some(s) = input.get("texts").and_then(|v| v.as_array()) {
        return s
            .iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(" ");
    }
    serde_json::to_string(input).unwrap_or_default()
}

/// Pull the assistant text out of a generated response body so we can
/// approximate output_tokens. Same provider-specific shapes as
/// `generate_canned_response`.
fn extract_output_text(model_id: &str, response_body: &str) -> String {
    let value: Value = match serde_json::from_str(response_body) {
        Ok(v) => v,
        Err(_) => return response_body.to_string(),
    };
    if model_id.starts_with("anthropic.") {
        if let Some(parts) = value.get("content").and_then(|c| c.as_array()) {
            return parts
                .iter()
                .filter_map(|p| p["text"].as_str())
                .collect::<Vec<_>>()
                .join(" ");
        }
    }
    if model_id.starts_with("amazon.") {
        if let Some(results) = value.get("results").and_then(|r| r.as_array()) {
            return results
                .iter()
                .filter_map(|r| r["outputText"].as_str())
                .collect::<Vec<_>>()
                .join(" ");
        }
    }
    if model_id.starts_with("meta.") {
        if let Some(s) = value.get("generation").and_then(|v| v.as_str()) {
            return s.to_string();
        }
    }
    if model_id.starts_with("cohere.") {
        if let Some(generations) = value.get("generations").and_then(|g| g.as_array()) {
            return generations
                .iter()
                .filter_map(|g| g["text"].as_str())
                .collect::<Vec<_>>()
                .join(" ");
        }
    }
    if model_id.starts_with("mistral.") {
        if let Some(outputs) = value.get("outputs").and_then(|o| o.as_array()) {
            return outputs
                .iter()
                .filter_map(|o| o["text"].as_str())
                .collect::<Vec<_>>()
                .join(" ");
        }
    }
    response_body.to_string()
}

/// Count tokens for the given input text (rough approximation).
pub(crate) fn count_tokens(
    _state: &SharedBedrockState,
    _req: &AwsRequest,
    model_id: &str,
    body: &[u8],
) -> Result<AwsResponse, AwsServiceError> {
    if model_id.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "modelId is required",
        ));
    }

    let input: Value = serde_json::from_slice(body).unwrap_or_default();

    // CountTokensRequest declares `input` as @required; missing/null fails fast.
    if input.get("input").is_none() || input["input"].is_null() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "input is required",
        ));
    }

    // Extract text from either invokeModel or converse format
    let text = if let Some(invoke_input) = input.get("input") {
        if let Some(invoke_model) = invoke_input.get("invokeModel") {
            // InvokeModel format — body is a document
            if let Some(body_doc) = invoke_model.get("body") {
                serde_json::to_string(body_doc).unwrap_or_default()
            } else {
                String::new()
            }
        } else if let Some(converse) = invoke_input.get("converse") {
            // Converse format — extract messages and system text
            let mut all_text = String::new();
            if let Some(system) = converse.get("system").and_then(|s| s.as_array()) {
                for block in system {
                    if let Some(t) = block["text"].as_str() {
                        all_text.push_str(t);
                        all_text.push(' ');
                    }
                }
            }
            if let Some(messages) = converse.get("messages").and_then(|m| m.as_array()) {
                for msg in messages {
                    if let Some(content) = msg.get("content").and_then(|c| c.as_array()) {
                        for block in content {
                            if let Some(t) = block["text"].as_str() {
                                all_text.push_str(t);
                                all_text.push(' ');
                            }
                        }
                    }
                }
            }
            all_text
        } else {
            serde_json::to_string(&input).unwrap_or_default()
        }
    } else {
        serde_json::to_string(&input).unwrap_or_default()
    };

    let token_count = crate::prompt::count_tokens(&text);

    Ok(AwsResponse::ok_json(json!({
        "inputTokens": token_count
    })))
}

/// Generate a deterministic canned response based on the model provider.
/// When `FAKECLOUD_FAKECLOUD_BEDROCK_ECHO=1` (or legacy `FAKECLOUD_BEDROCK_ECHO=1`) is set in
/// the environment, the assistant text reflects the user-supplied prompt
/// instead of the canned phrase, so tests can pin assertions against
/// their own input.
fn generate_canned_response(model_id: &str, input: &Value) -> String {
    if crate::prompt::echo_enabled() {
        return echo_response(model_id, input);
    }
    canned_response_inner(model_id, input)
}

fn canned_response_inner(model_id: &str, input: &Value) -> String {
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

    let prompt = extract_input_text(input);
    let in_tokens = crate::prompt::count_tokens(&prompt);
    match provider {
        "anthropic" => anthropic_response(model_id, in_tokens),
        "amazon" => amazon_titan_response(model_id, in_tokens),
        "meta" => meta_llama_response(in_tokens),
        "cohere" => cohere_response(),
        "mistral" => mistral_response(),
        _ => generic_response(),
    }
}

/// Build a response that echoes the caller's prompt instead of the
/// canned phrase. The shape matches the same provider-specific JSON that
/// `canned_response_inner` produces — only the assistant text changes.
/// Usage fields inside the body are populated via `count_tokens` against
/// the actual input prompt and echoed output.
fn echo_response(model_id: &str, input: &Value) -> String {
    let prompt = extract_input_text(input);
    let echoed = if prompt.is_empty() {
        "(empty prompt)".to_string()
    } else {
        prompt
    };
    let in_tokens = crate::prompt::count_tokens(&echoed);
    let out_tokens = crate::prompt::count_tokens(&echoed);
    if model_id.starts_with("anthropic.") {
        return serde_json::to_string(&json!({
            "id": "msg_fakecloudtest01",
            "type": "message",
            "role": "assistant",
            "content": [{ "type": "text", "text": echoed }],
            "model": model_id,
            "stop_reason": "end_turn",
            "stop_sequence": null,
            "usage": { "input_tokens": in_tokens, "output_tokens": out_tokens }
        }))
        .expect("serde_json::Value serialization is infallible");
    }
    if model_id.starts_with("amazon.") {
        return serde_json::to_string(&json!({
            "inputTextTokenCount": in_tokens,
            "results": [{
                "tokenCount": out_tokens,
                "outputText": echoed,
                "completionReason": "FINISH"
            }]
        }))
        .expect("serde_json::Value serialization is infallible");
    }
    if model_id.starts_with("meta.") {
        return serde_json::to_string(&json!({
            "generation": echoed,
            "prompt_token_count": in_tokens,
            "generation_token_count": out_tokens,
            "stop_reason": "stop"
        }))
        .expect("serde_json::Value serialization is infallible");
    }
    if model_id.starts_with("cohere.") {
        return serde_json::to_string(&json!({
            "generations": [{ "id": "gen-fakecloud", "text": echoed, "finish_reason": "COMPLETE" }],
            "id": "fakecloud-echo",
            "prompt": echoed
        }))
        .expect("serde_json::Value serialization is infallible");
    }
    if model_id.starts_with("mistral.") {
        return serde_json::to_string(&json!({
            "outputs": [{ "text": echoed, "stop_reason": "stop" }]
        }))
        .expect("serde_json::Value serialization is infallible");
    }
    serde_json::to_string(&json!({ "completion": echoed }))
        .expect("serde_json::Value serialization is infallible")
}

/// Canned text every provider's non-echo branch returns. Centralized so
/// the body and the corresponding `usage.output_tokens` come from the
/// same source.
const CANNED_OUTPUT_TEXT: &str = "This is a test response from the emulated model.";

fn anthropic_response(model_id: &str, in_tokens: u64) -> String {
    let out_tokens = crate::prompt::count_tokens(CANNED_OUTPUT_TEXT);
    serde_json::to_string(&json!({
        "id": "msg_fakecloudtest01",
        "type": "message",
        "role": "assistant",
        "content": [
            {
                "type": "text",
                "text": CANNED_OUTPUT_TEXT
            }
        ],
        "model": model_id,
        "stop_reason": "end_turn",
        "stop_sequence": null,
        "usage": {
            "input_tokens": in_tokens,
            "output_tokens": out_tokens
        }
    }))
    .expect("serde_json::Value serialization is infallible")
}

fn amazon_titan_response(model_id: &str, in_tokens: u64) -> String {
    // Titan embed models return embedding vectors, not text completions
    if model_id.starts_with("amazon.titan-embed") {
        let embedding: Vec<f64> = (0..256).map(|i| (i as f64 * 0.001).sin()).collect();
        return serde_json::to_string(&json!({
            "embedding": embedding,
            "inputTextTokenCount": in_tokens
        }))
        .expect("serde_json::Value serialization is infallible");
    }

    let out_tokens = crate::prompt::count_tokens(CANNED_OUTPUT_TEXT);
    serde_json::to_string(&json!({
        "inputTextTokenCount": in_tokens,
        "results": [
            {
                "tokenCount": out_tokens,
                "outputText": CANNED_OUTPUT_TEXT,
                "completionReason": "FINISH"
            }
        ]
    }))
    .expect("serde_json::Value serialization is infallible")
}

fn meta_llama_response(in_tokens: u64) -> String {
    let out_tokens = crate::prompt::count_tokens(CANNED_OUTPUT_TEXT);
    serde_json::to_string(&json!({
        "generation": CANNED_OUTPUT_TEXT,
        "prompt_logprobs": null,
        "generation_logprobs": null,
        "stop_reason": "stop",
        "generation_token_count": out_tokens,
        "prompt_token_count": in_tokens
    }))
    .expect("serde_json::Value serialization is infallible")
}

fn cohere_response() -> String {
    serde_json::to_string(&json!({
        "generations": [
            {
                "id": "gen-fakecloud-01",
                "text": CANNED_OUTPUT_TEXT,
                "finish_reason": "COMPLETE",
                "token_likelihoods": []
            }
        ],
        "prompt": ""
    }))
    .expect("serde_json::Value serialization is infallible")
}

fn mistral_response() -> String {
    serde_json::to_string(&json!({
        "outputs": [
            {
                "text": CANNED_OUTPUT_TEXT,
                "stop_reason": "stop"
            }
        ]
    }))
    .expect("serde_json::Value serialization is infallible")
}

fn generic_response() -> String {
    serde_json::to_string(&json!({
        "output": CANNED_OUTPUT_TEXT
    }))
    .expect("serde_json::Value serialization is infallible")
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

    /// Global mutex to serialize tests that mutate the process-wide
    /// `FAKECLOUD_BEDROCK_ECHO` env var with sibling tests in the same binary that
    /// observe model behavior. Without it the parallel test harness
    /// races: e.g. `invoke_amazon_titan_embed_returns_vector` runs while
    /// `invoke_echo_mode_reflects_prompt` still has the flag flipped, and
    /// the embed handler returns the echo branch's text instead of the
    /// embedding vector.
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
            action: "InvokeModel".to_string(),
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
    fn invoke_empty_model_id_errors() {
        let s = shared();
        let err = invoke_model(&s, &req(), "", b"{}").err().unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn invoke_emits_dynamic_token_counts_in_headers() {
        let s = shared();
        // Anthropic-shaped body: 5 whitespace-separated tokens.
        let body = br#"{"messages":[{"role":"user","content":"please count my tokens here"}]}"#;
        let resp = invoke_model(&s, &req(), "anthropic.claude-3", body).unwrap();
        let input_hdr = resp
            .headers
            .get("x-amzn-bedrock-input-token-count")
            .unwrap()
            .to_str()
            .unwrap();
        let output_hdr = resp
            .headers
            .get("x-amzn-bedrock-output-token-count")
            .unwrap()
            .to_str()
            .unwrap();
        // No longer the hardcoded 10/20 — driven by actual prompt /
        // response text.
        assert_ne!(input_hdr, "10");
        let input_n: usize = input_hdr.parse().unwrap();
        let output_n: usize = output_hdr.parse().unwrap();
        assert!(input_n >= 5);
        assert!(output_n > 0);
    }

    #[test]
    fn invoke_echo_mode_reflects_prompt() {
        // FAKECLOUD_BEDROCK_ECHO mutation is process-global; the lock serializes
        // this test with sibling tests in the same binary that observe
        // model output (e.g. titan-embed) so they don't race the flag.
        let _g = echo_lock().lock();
        let prev = std::env::var("FAKECLOUD_BEDROCK_ECHO").ok();
        // SAFETY: lock above pins us to a single mutation window.
        unsafe { std::env::set_var("FAKECLOUD_BEDROCK_ECHO", "1") };

        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":"hello world from echo mode"}]}"#;
        let resp = invoke_model(&s, &req(), "anthropic.claude-3", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        let text = v["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("hello world from echo mode"),
            "echo response did not include prompt: {text}"
        );

        // SAFETY: see comment above.
        unsafe {
            match prev {
                Some(p) => std::env::set_var("FAKECLOUD_BEDROCK_ECHO", p),
                None => std::env::remove_var("FAKECLOUD_BEDROCK_ECHO"),
            }
        }
    }

    #[test]
    fn invoke_anthropic_returns_message_format() {
        // Body-shape tests in this module observe the response payload,
        // so they all must serialize with `invoke_echo_mode_reflects_prompt`,
        // which mutates the process-global `FAKECLOUD_BEDROCK_ECHO` flag and would
        // otherwise have them observe the echo branch mid-flip.
        let _g = echo_lock().lock();
        let s = shared();
        let resp = invoke_model(&s, &req(), "anthropic.claude-3", b"{}").unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["type"], "message");
        assert_eq!(v["role"], "assistant");
        assert_eq!(v["model"], "anthropic.claude-3");
    }

    #[test]
    fn invoke_amazon_titan_text_returns_results() {
        let _g = echo_lock().lock();
        let s = shared();
        let resp = invoke_model(&s, &req(), "amazon.titan-text-express-v1", b"{}").unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert!(v["results"].is_array());
    }

    #[test]
    fn invoke_amazon_titan_embed_returns_vector() {
        // Lock against `invoke_echo_mode_reflects_prompt` so we don't
        // observe the echo branch's text when the flag is mid-flip.
        let _g = echo_lock().lock();
        let s = shared();
        let resp = invoke_model(&s, &req(), "amazon.titan-embed-text-v1", b"{}").unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert!(v["embedding"].is_array());
    }

    #[test]
    fn invoke_meta_returns_generation() {
        let _g = echo_lock().lock();
        let s = shared();
        let resp = invoke_model(&s, &req(), "meta.llama3", b"{}").unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert!(v["generation"].is_string());
    }

    #[test]
    fn invoke_cohere_returns_generations() {
        let _g = echo_lock().lock();
        let s = shared();
        let resp = invoke_model(&s, &req(), "cohere.command", b"{}").unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert!(v["generations"].is_array());
    }

    #[test]
    fn invoke_mistral_returns_outputs() {
        let _g = echo_lock().lock();
        let s = shared();
        let resp = invoke_model(&s, &req(), "mistral.7b", b"{}").unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert!(v["outputs"].is_array());
    }

    #[test]
    fn invoke_unknown_provider_returns_generic() {
        let _g = echo_lock().lock();
        let s = shared();
        let resp = invoke_model(&s, &req(), "stability.diffusion", b"{}").unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(
            v["output"],
            "This is a test response from the emulated model."
        );
    }

    #[test]
    fn invoke_records_invocation_entry() {
        let s = shared();
        invoke_model(&s, &req(), "anthropic.claude", b"{}").unwrap();
        let state = s.read();
        assert_eq!(state.default_ref().invocations.len(), 1);
    }

    #[test]
    fn invoke_returns_bedrock_headers() {
        let s = shared();
        let resp = invoke_model(&s, &req(), "anthropic.claude", b"{}").unwrap();
        assert!(resp
            .headers
            .get("x-amzn-bedrock-input-token-count")
            .is_some());
        assert!(resp
            .headers
            .get("x-amzn-bedrock-output-token-count")
            .is_some());
    }

    #[test]
    fn invoke_fault_injected_records_error() {
        let s = shared();
        s.write()
            .default_mut()
            .fault_rules
            .push(crate::state::FaultRule {
                error_type: "Throttle".to_string(),
                message: "slow".to_string(),
                http_status: 429,
                remaining: 1,
                model_id: None,
                operation: None,
            });
        let err = invoke_model(&s, &req(), "m", b"{}").err().unwrap();
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
        let state = s.read();
        let acct = state.default_ref();
        assert_eq!(acct.invocations.len(), 1);
        assert!(acct.invocations[0].error.is_some());
    }

    #[test]
    fn count_tokens_empty_model_id_errors() {
        let s = shared();
        let err = count_tokens(&s, &req(), "", b"{}").err().unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn count_tokens_invoke_model_body_docs() {
        let s = shared();
        let body = br#"{"input":{"invokeModel":{"body":{"prompt":"hello world foo bar"}}}}"#;
        let resp = count_tokens(&s, &req(), "m", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert!(v["inputTokens"].as_u64().unwrap() > 0);
    }

    #[test]
    fn count_tokens_converse_format() {
        let s = shared();
        let body = br#"{"input":{"converse":{"system":[{"text":"sys prompt"}],"messages":[{"content":[{"text":"hello world"}]}]}}}"#;
        let resp = count_tokens(&s, &req(), "m", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert_eq!(v["inputTokens"].as_u64().unwrap(), 4);
    }

    #[test]
    fn count_tokens_unknown_input_falls_back_to_raw_json_count() {
        // CountTokens requires the @required `input` field; anything else
        // inside it is fair game and we fall back to a raw JSON token count.
        let s = shared();
        let body = br#"{"input":{"other":"field"}}"#;
        let resp = count_tokens(&s, &req(), "m", body).unwrap();
        let v: Value =
            serde_json::from_str(std::str::from_utf8(resp.body.expect_bytes()).unwrap()).unwrap();
        assert!(v["inputTokens"].as_u64().unwrap() >= 1);
    }

    #[test]
    fn count_tokens_missing_input_field_errors() {
        // Omitting `input` is a validation failure (mirrors AWS).
        let s = shared();
        let body = b"{}";
        let err = count_tokens(&s, &req(), "m", body).err().unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }
}
