//! Real model inference against a configurable upstream LLM endpoint.
//!
//! Bedrock's product *is* its data plane: model inference. By default
//! fakecloud returns deterministic canned completions (see
//! [`crate::invoke`]) so conformance and offline tests stay hermetic. When
//! the operator wires a real backing LLM (the Bedrock equivalent of
//! pointing RDS at a real Postgres container) the runtime operations
//! perform genuine HTTP inference against it.
//!
//! Configuration is entirely environment-driven (config, not API surface):
//!
//! | Variable | Meaning |
//! |----------|---------|
//! | `FAKECLOUD_BEDROCK_UPSTREAM_URL` | Base URL of the upstream endpoint. Presence enables real inference. |
//! | `FAKECLOUD_BEDROCK_UPSTREAM_KEY` | Optional API key/token. |
//! | `FAKECLOUD_BEDROCK_UPSTREAM_PROTOCOL` | `anthropic` \| `openai` \| `ollama` (default `openai`). |
//! | `FAKECLOUD_BEDROCK_UPSTREAM_MODEL` | Default upstream model id for text generation. |
//! | `FAKECLOUD_BEDROCK_UPSTREAM_EMBED_MODEL` | Default upstream model id for embeddings. |
//! | `FAKECLOUD_BEDROCK_UPSTREAM_MODEL_MAP` | `bedrock_id_or_prefix=upstream_model,...` alias map. |
//!
//! The incoming Bedrock provider-native request body (Anthropic Messages,
//! Titan/Nova, Llama, Cohere, Mistral) is translated into the configured
//! upstream protocol, the call is made with async `reqwest`, and the
//! upstream response is translated *back* into the exact Bedrock
//! provider-native shape the aws-sdk client expects, including real token
//! counts surfaced through the `x-amzn-bedrock-*-token-count` headers.

use std::sync::OnceLock;
use std::time::Duration;

use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::SharedBedrockState;

/// Upstream wire protocol selector.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Protocol {
    Anthropic,
    OpenAi,
    Ollama,
}

/// Parsed upstream configuration. Built once per request from the
/// environment via [`UpstreamConfig::from_env`].
#[derive(Clone, Debug)]
pub(crate) struct UpstreamConfig {
    pub url: String,
    pub key: Option<String>,
    pub protocol: Protocol,
    pub default_model: Option<String>,
    pub embed_model: Option<String>,
    pub model_map: Vec<(String, String)>,
}

impl UpstreamConfig {
    /// Read the upstream configuration from the environment. Returns
    /// `None` when `FAKECLOUD_BEDROCK_UPSTREAM_URL` is unset/blank, in
    /// which case callers fall back to the deterministic offline path.
    pub(crate) fn from_env() -> Option<Self> {
        let url = env_nonempty("FAKECLOUD_BEDROCK_UPSTREAM_URL")?;
        let protocol = match env_nonempty("FAKECLOUD_BEDROCK_UPSTREAM_PROTOCOL")
            .as_deref()
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("anthropic") => Protocol::Anthropic,
            Some("ollama") => Protocol::Ollama,
            _ => Protocol::OpenAi,
        };
        let model_map = env_nonempty("FAKECLOUD_BEDROCK_UPSTREAM_MODEL_MAP")
            .map(|raw| {
                raw.split(',')
                    .filter_map(|pair| {
                        let (k, v) = pair.split_once('=')?;
                        let k = k.trim();
                        let v = v.trim();
                        if k.is_empty() || v.is_empty() {
                            None
                        } else {
                            Some((k.to_string(), v.to_string()))
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        Some(Self {
            url: url.trim_end_matches('/').to_string(),
            key: env_nonempty("FAKECLOUD_BEDROCK_UPSTREAM_KEY"),
            protocol,
            default_model: env_nonempty("FAKECLOUD_BEDROCK_UPSTREAM_MODEL"),
            embed_model: env_nonempty("FAKECLOUD_BEDROCK_UPSTREAM_EMBED_MODEL"),
            model_map,
        })
    }

    /// Resolve which upstream model to forward a given Bedrock model id to.
    /// The user asked for a specific Bedrock model; we honour a configured
    /// alias, else the default model, else the Bedrock id with its
    /// `provider.` namespace stripped.
    fn resolve_model(&self, bedrock_model_id: &str) -> String {
        for (k, v) in &self.model_map {
            if bedrock_model_id == k || bedrock_model_id.starts_with(k) {
                return v.clone();
            }
        }
        self.default_model
            .clone()
            .unwrap_or_else(|| strip_provider(bedrock_model_id))
    }

    /// Resolve the upstream embedding model id.
    fn resolve_embed_model(&self, bedrock_model_id: &str) -> String {
        for (k, v) in &self.model_map {
            if bedrock_model_id == k || bedrock_model_id.starts_with(k) {
                return v.clone();
            }
        }
        self.embed_model
            .clone()
            .or_else(|| self.default_model.clone())
            .unwrap_or_else(|| strip_provider(bedrock_model_id))
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}{}", self.url, path)
    }
}

fn env_nonempty(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// Cross-region inference-profile ids prefix the provider with a region
/// family token, e.g. `us.anthropic.claude-3-5-sonnet-...` or
/// `apac.amazon.nova-lite-...`. Strip a leading region token so that
/// provider-shape resolution keys on the real `provider.` prefix.
pub(crate) fn strip_region_prefix(id: &str) -> &str {
    const REGION_PREFIXES: [&str; 4] = ["us.", "eu.", "apac.", "us-gov."];
    for p in REGION_PREFIXES {
        if let Some(rest) = id.strip_prefix(p) {
            return rest;
        }
    }
    id
}

fn strip_provider(id: &str) -> String {
    let id = strip_region_prefix(id);
    id.split_once('.')
        .map(|(_, rest)| rest.to_string())
        .unwrap_or_else(|| id.to_string())
}

// ---------------------------------------------------------------------------
// Unified intermediate representation
// ---------------------------------------------------------------------------

struct ChatMsg {
    role: String,
    text: String,
}

/// A tool the caller made available to the model, in provider-neutral form.
struct ToolSpec {
    name: String,
    description: Option<String>,
    input_schema: Value,
}

/// A tool invocation the model chose to make, translated back to the
/// Bedrock `toolUse` vocabulary.
struct ToolCall {
    id: String,
    name: String,
    input: Value,
}

struct Chat {
    system: Option<String>,
    messages: Vec<ChatMsg>,
    max_tokens: Option<u64>,
    temperature: Option<f64>,
    tools: Vec<ToolSpec>,
}

impl Chat {
    /// Concatenated prompt text, used as a token-count fallback when the
    /// upstream doesn't report `input_tokens`.
    fn prompt_text(&self) -> String {
        let mut out = String::new();
        if let Some(s) = &self.system {
            out.push_str(s);
            out.push(' ');
        }
        for m in &self.messages {
            out.push_str(&m.text);
            out.push(' ');
        }
        out
    }
}

struct Completion {
    text: String,
    tool_calls: Vec<ToolCall>,
    /// The upstream's own finish/stop reason, verbatim and un-mapped.
    stop_reason: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
}

/// Pull text out of a Bedrock content value that may be a plain string
/// (Anthropic string content) or an array of `{ "text": ... }` blocks
/// (Anthropic block content / Converse / Nova).
fn text_from_content(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(parts) => parts
            .iter()
            .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
            .collect::<Vec<_>>()
            .join(" "),
        _ => String::new(),
    }
}

fn extract_system(input: &Value) -> Option<String> {
    match input.get("system") {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        Some(Value::Array(blocks)) => {
            let joined = blocks
                .iter()
                .filter_map(|b| b.get("text").and_then(|t| t.as_str()))
                .collect::<Vec<_>>()
                .join(" ");
            (!joined.is_empty()).then_some(joined)
        }
        _ => None,
    }
}

/// Translate an incoming Bedrock provider-native request body (InvokeModel
/// or Converse) into the unified [`Chat`] shape. Handles the message-based
/// shapes (Anthropic, Nova, Converse) and the single-prompt shapes
/// (Titan `inputText`, Llama/Mistral/Cohere `prompt`, Cohere-R `message`).
fn bedrock_request_to_chat(input: &Value) -> Chat {
    let mut messages = Vec::new();
    if let Some(arr) = input.get("messages").and_then(|m| m.as_array()) {
        for msg in arr {
            let role = msg
                .get("role")
                .and_then(|r| r.as_str())
                .unwrap_or("user")
                .to_string();
            let text = text_from_content(msg.get("content").unwrap_or(&Value::Null));
            messages.push(ChatMsg { role, text });
        }
    }

    if messages.is_empty() {
        let prompt = input
            .get("inputText")
            .and_then(|v| v.as_str())
            .or_else(|| input.get("prompt").and_then(|v| v.as_str()))
            .or_else(|| input.get("message").and_then(|v| v.as_str()))
            .map(str::to_string)
            .or_else(|| {
                input.get("texts").and_then(|v| v.as_array()).map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str())
                        .collect::<Vec<_>>()
                        .join(" ")
                })
            })
            .unwrap_or_default();
        if !prompt.is_empty() {
            messages.push(ChatMsg {
                role: "user".to_string(),
                text: prompt,
            });
        }
    }

    let max_tokens = input
        .get("max_tokens")
        .and_then(|v| v.as_u64())
        .or_else(|| {
            input
                .pointer("/textGenerationConfig/maxTokenCount")
                .and_then(|v| v.as_u64())
        })
        .or_else(|| input.get("max_gen_len").and_then(|v| v.as_u64()))
        .or_else(|| {
            input
                .pointer("/inferenceConfig/maxTokens")
                .and_then(|v| v.as_u64())
        });

    let temperature = input
        .get("temperature")
        .and_then(|v| v.as_f64())
        .or_else(|| {
            input
                .pointer("/textGenerationConfig/temperature")
                .and_then(|v| v.as_f64())
        })
        .or_else(|| {
            input
                .pointer("/inferenceConfig/temperature")
                .and_then(|v| v.as_f64())
        });

    Chat {
        system: extract_system(input),
        messages,
        max_tokens,
        temperature,
        tools: extract_tools(input),
    }
}

/// Collect the tools the caller offered the model, from either the Converse
/// `toolConfig.tools[].toolSpec` shape or the Anthropic-native InvokeModel
/// `tools[]` shape.
fn extract_tools(input: &Value) -> Vec<ToolSpec> {
    let mut tools = Vec::new();
    if let Some(arr) = input
        .pointer("/toolConfig/tools")
        .and_then(|t| t.as_array())
    {
        for t in arr {
            if let Some(spec) = t.get("toolSpec") {
                if let Some(name) = spec.get("name").and_then(|n| n.as_str()) {
                    tools.push(ToolSpec {
                        name: name.to_string(),
                        description: spec
                            .get("description")
                            .and_then(|d| d.as_str())
                            .map(str::to_string),
                        input_schema: spec
                            .pointer("/inputSchema/json")
                            .cloned()
                            .unwrap_or_else(|| json!({ "type": "object" })),
                    });
                }
            }
        }
    }
    if tools.is_empty() {
        // Anthropic-native InvokeModel bodies carry tools at the top level.
        if let Some(arr) = input.get("tools").and_then(|t| t.as_array()) {
            for t in arr {
                if let Some(name) = t.get("name").and_then(|n| n.as_str()) {
                    tools.push(ToolSpec {
                        name: name.to_string(),
                        description: t
                            .get("description")
                            .and_then(|d| d.as_str())
                            .map(str::to_string),
                        input_schema: t
                            .get("input_schema")
                            .cloned()
                            .unwrap_or_else(|| json!({ "type": "object" })),
                    });
                }
            }
        }
    }
    tools
}

fn norm_anthropic_role(role: &str) -> &str {
    if role == "assistant" {
        "assistant"
    } else {
        "user"
    }
}

fn anthropic_messages(chat: &Chat) -> Vec<Value> {
    let mut msgs: Vec<Value> = chat
        .messages
        .iter()
        .map(|m| json!({ "role": norm_anthropic_role(&m.role), "content": m.text }))
        .collect();
    if msgs.is_empty() {
        msgs.push(json!({ "role": "user", "content": "" }));
    }
    msgs
}

fn openai_messages(chat: &Chat) -> Vec<Value> {
    let mut msgs = Vec::new();
    if let Some(s) = &chat.system {
        msgs.push(json!({ "role": "system", "content": s }));
    }
    for m in &chat.messages {
        let role = match m.role.as_str() {
            "assistant" => "assistant",
            "system" => "system",
            _ => "user",
        };
        msgs.push(json!({ "role": role, "content": m.text }));
    }
    if msgs.is_empty() {
        msgs.push(json!({ "role": "user", "content": "" }));
    }
    msgs
}

/// Render the chat's tools in the OpenAI / Ollama `tools` shape.
fn openai_tools(chat: &Chat) -> Value {
    Value::Array(
        chat.tools
            .iter()
            .map(|t| {
                json!({
                    "type": "function",
                    "function": {
                        "name": t.name,
                        "description": t.description.clone().unwrap_or_default(),
                        "parameters": t.input_schema,
                    }
                })
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// Stop / finish reason normalization
// ---------------------------------------------------------------------------

/// Provider-neutral view of why a generation ended. The upstream protocols
/// each use their own vocabulary; we normalize once and re-render into each
/// Bedrock provider's native reason so a `max_tokens`-truncated generation
/// is never reported as a natural completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopKind {
    EndTurn,
    MaxTokens,
    ToolUse,
    StopSequence,
    ContentFiltered,
}

/// Map an upstream finish/stop reason onto [`StopKind`]. A model that
/// emitted a tool call is always `ToolUse` regardless of the raw string.
pub(crate) fn normalize_stop(upstream: Option<&str>, has_tool_calls: bool) -> StopKind {
    if has_tool_calls {
        return StopKind::ToolUse;
    }
    match upstream.map(|s| s.to_ascii_lowercase()).as_deref() {
        Some("length") | Some("max_tokens") | Some("model_length") => StopKind::MaxTokens,
        Some("tool_calls") | Some("tool_use") | Some("function_call") => StopKind::ToolUse,
        Some("content_filter") | Some("content_filtered") => StopKind::ContentFiltered,
        Some("stop_sequence") => StopKind::StopSequence,
        _ => StopKind::EndTurn,
    }
}

/// Converse / Anthropic `stopReason` vocabulary.
pub(crate) fn stop_converse(kind: StopKind) -> &'static str {
    match kind {
        StopKind::EndTurn => "end_turn",
        StopKind::MaxTokens => "max_tokens",
        StopKind::ToolUse => "tool_use",
        StopKind::StopSequence => "stop_sequence",
        StopKind::ContentFiltered => "content_filtered",
    }
}

/// Anthropic-native `stop_reason` (no `content_filtered` in that vocabulary).
pub(crate) fn stop_anthropic(kind: StopKind) -> &'static str {
    match kind {
        StopKind::EndTurn | StopKind::ContentFiltered => "end_turn",
        StopKind::MaxTokens => "max_tokens",
        StopKind::ToolUse => "tool_use",
        StopKind::StopSequence => "stop_sequence",
    }
}

/// Amazon Titan `completionReason`.
pub(crate) fn stop_titan(kind: StopKind) -> &'static str {
    match kind {
        StopKind::MaxTokens => "LENGTH",
        StopKind::ContentFiltered => "CONTENT_FILTERED",
        _ => "FINISH",
    }
}

/// Meta Llama / Mistral `stop_reason`.
pub(crate) fn stop_meta_mistral(kind: StopKind) -> &'static str {
    match kind {
        StopKind::MaxTokens => "length",
        _ => "stop",
    }
}

/// Cohere `finish_reason`.
pub(crate) fn stop_cohere(kind: StopKind) -> &'static str {
    match kind {
        StopKind::MaxTokens => "MAX_TOKENS",
        _ => "COMPLETE",
    }
}

// ---------------------------------------------------------------------------
// Bedrock provider-native response construction
// ---------------------------------------------------------------------------

/// Wrap a completion back into the exact Bedrock provider-native response
/// body the caller expects for the given model, preserving real usage, the
/// upstream stop reason, and any tool-use blocks.
fn build_provider_response(model_id: &str, completion: &Completion) -> String {
    let family = strip_region_prefix(model_id);
    let text = completion.text.as_str();
    let input_tokens = completion.input_tokens;
    let output_tokens = completion.output_tokens;
    let stop = normalize_stop(
        completion.stop_reason.as_deref(),
        !completion.tool_calls.is_empty(),
    );

    let value = if family.starts_with("anthropic.") {
        let mut content: Vec<Value> = Vec::new();
        if !text.is_empty() {
            content.push(json!({ "type": "text", "text": text }));
        }
        for tc in &completion.tool_calls {
            content.push(json!({
                "type": "tool_use",
                "id": tc.id,
                "name": tc.name,
                "input": tc.input
            }));
        }
        if content.is_empty() {
            content.push(json!({ "type": "text", "text": "" }));
        }
        json!({
            "id": "msg_fakecloudupstream01",
            "type": "message",
            "role": "assistant",
            "content": content,
            "model": model_id,
            "stop_reason": stop_anthropic(stop),
            "stop_sequence": null,
            "usage": { "input_tokens": input_tokens, "output_tokens": output_tokens }
        })
    } else if family.starts_with("amazon.") {
        json!({
            "inputTextTokenCount": input_tokens,
            "results": [{
                "tokenCount": output_tokens,
                "outputText": text,
                "completionReason": stop_titan(stop)
            }]
        })
    } else if family.starts_with("meta.") {
        json!({
            "generation": text,
            "prompt_token_count": input_tokens,
            "generation_token_count": output_tokens,
            "stop_reason": stop_meta_mistral(stop)
        })
    } else if family.starts_with("cohere.") {
        json!({
            "generations": [{ "id": "gen-fakecloud-upstream", "text": text, "finish_reason": stop_cohere(stop) }],
            "id": "fakecloud-upstream",
            "prompt": ""
        })
    } else if family.starts_with("mistral.") {
        json!({ "outputs": [{ "text": text, "stop_reason": stop_meta_mistral(stop) }] })
    } else {
        json!({ "output": text })
    };
    serde_json::to_string(&value).expect("serde_json::Value serialization is infallible")
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn model_error(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::from_u16(424).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        "ModelErrorException",
        msg,
    )
}

fn throttling_error(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::TOO_MANY_REQUESTS, "ThrottlingException", msg)
}

fn validation_error(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

/// Parse a runtime request body as JSON. A body that isn't valid JSON is a
/// client error in real Bedrock (`ValidationException`), so we reject it
/// rather than coercing it to `null` and forwarding an empty request
/// upstream (which would yield a spurious 200 completion).
fn parse_body(body: &[u8]) -> Result<Value, AwsServiceError> {
    serde_json::from_slice(body).map_err(|e| {
        validation_error(format!(
            "Malformed input request: {e}, please reformat and try again."
        ))
    })
}

/// Map an unsuccessful upstream HTTP status onto a faithful Bedrock error.
fn map_upstream_status(status: StatusCode, body: &str) -> AwsServiceError {
    let snippet: String = body.chars().take(500).collect();
    if status == StatusCode::TOO_MANY_REQUESTS {
        throttling_error(format!("upstream throttled: {snippet}"))
    } else if status.is_client_error() {
        validation_error(format!("upstream rejected request ({status}): {snippet}"))
    } else {
        model_error(format!("upstream error ({status}): {snippet}"))
    }
}

/// A process-wide, lazily-built `reqwest::Client`. The client is internally
/// reference-counted and pools connections, so it is cached and cloned per
/// call rather than rebuilt (which would discard keep-alive / TLS reuse).
fn http_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        })
        .clone()
}

// ---------------------------------------------------------------------------
// Non-streaming chat
// ---------------------------------------------------------------------------

fn chat_request_builder(
    client: &reqwest::Client,
    cfg: &UpstreamConfig,
    upstream_model: &str,
    chat: &Chat,
    stream: bool,
) -> reqwest::RequestBuilder {
    match cfg.protocol {
        Protocol::Anthropic => {
            let mut body = json!({
                "model": upstream_model,
                "max_tokens": chat.max_tokens.unwrap_or(1024),
                "messages": anthropic_messages(chat),
            });
            if let Some(s) = &chat.system {
                body["system"] = json!(s);
            }
            if let Some(t) = chat.temperature {
                body["temperature"] = json!(t);
            }
            if !chat.tools.is_empty() {
                body["tools"] = Value::Array(
                    chat.tools
                        .iter()
                        .map(|t| {
                            let mut v = json!({
                                "name": t.name,
                                "input_schema": t.input_schema,
                            });
                            if let Some(d) = &t.description {
                                v["description"] = json!(d);
                            }
                            v
                        })
                        .collect(),
                );
            }
            if stream {
                body["stream"] = json!(true);
            }
            let mut rb = client
                .post(cfg.endpoint("/v1/messages"))
                .header("anthropic-version", "2023-06-01")
                .json(&body);
            if let Some(k) = &cfg.key {
                rb = rb.header("x-api-key", k);
            }
            rb
        }
        Protocol::OpenAi => {
            let mut body = json!({
                "model": upstream_model,
                "messages": openai_messages(chat),
            });
            if let Some(mt) = chat.max_tokens {
                body["max_tokens"] = json!(mt);
            }
            if let Some(t) = chat.temperature {
                body["temperature"] = json!(t);
            }
            if !chat.tools.is_empty() {
                body["tools"] = openai_tools(chat);
            }
            if stream {
                body["stream"] = json!(true);
                body["stream_options"] = json!({ "include_usage": true });
            }
            let mut rb = client
                .post(cfg.endpoint("/v1/chat/completions"))
                .json(&body);
            if let Some(k) = &cfg.key {
                rb = rb.bearer_auth(k);
            }
            rb
        }
        Protocol::Ollama => {
            let mut options = serde_json::Map::new();
            if let Some(mt) = chat.max_tokens {
                options.insert("num_predict".to_string(), json!(mt));
            }
            if let Some(t) = chat.temperature {
                options.insert("temperature".to_string(), json!(t));
            }
            let mut body = json!({
                "model": upstream_model,
                "messages": openai_messages(chat),
                "stream": stream,
            });
            if !options.is_empty() {
                body["options"] = Value::Object(options);
            }
            if !chat.tools.is_empty() {
                body["tools"] = openai_tools(chat);
            }
            let mut rb = client.post(cfg.endpoint("/api/chat")).json(&body);
            if let Some(k) = &cfg.key {
                rb = rb.bearer_auth(k);
            }
            rb
        }
    }
}

async fn send_and_read(rb: reqwest::RequestBuilder) -> Result<Value, AwsServiceError> {
    let resp = rb
        .send()
        .await
        .map_err(|e| model_error(format!("upstream request failed: {e}")))?;
    let status = resp.status();
    let status = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let bytes = resp
        .bytes()
        .await
        .map_err(|e| model_error(format!("reading upstream body failed: {e}")))?;
    if !status.is_success() {
        return Err(map_upstream_status(
            status,
            &String::from_utf8_lossy(&bytes),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|e| model_error(format!("malformed upstream response body: {e}")))
}

async fn call_chat(
    cfg: &UpstreamConfig,
    upstream_model: &str,
    chat: &Chat,
) -> Result<Completion, AwsServiceError> {
    let client = http_client();
    let rb = chat_request_builder(&client, cfg, upstream_model, chat, false);
    let v = send_and_read(rb).await?;

    let (text, tool_calls, stop_reason, in_t, out_t) = match cfg.protocol {
        Protocol::Anthropic => {
            let content = v.get("content").and_then(|c| c.as_array());
            let text = content
                .map(|a| {
                    a.iter()
                        .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("text"))
                        .filter_map(|p| p.get("text").and_then(|t| t.as_str()))
                        .collect::<Vec<_>>()
                        .join("")
                })
                .unwrap_or_default();
            let tool_calls = content
                .map(|a| {
                    a.iter()
                        .filter(|p| p.get("type").and_then(|t| t.as_str()) == Some("tool_use"))
                        .map(|p| ToolCall {
                            id: p
                                .get("id")
                                .and_then(|x| x.as_str())
                                .unwrap_or("tooluse_fakecloud_01")
                                .to_string(),
                            name: p
                                .get("name")
                                .and_then(|x| x.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            input: p.get("input").cloned().unwrap_or_else(|| json!({})),
                        })
                        .collect()
                })
                .unwrap_or_default();
            (
                text,
                tool_calls,
                v.get("stop_reason")
                    .and_then(|s| s.as_str())
                    .map(str::to_string),
                v.pointer("/usage/input_tokens").and_then(|x| x.as_u64()),
                v.pointer("/usage/output_tokens").and_then(|x| x.as_u64()),
            )
        }
        Protocol::OpenAi => {
            let text = v
                .pointer("/choices/0/message/content")
                .and_then(|t| t.as_str())
                .unwrap_or_default()
                .to_string();
            let tool_calls = v
                .pointer("/choices/0/message/tool_calls")
                .and_then(|c| c.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|tc| {
                            let name = tc.pointer("/function/name").and_then(|x| x.as_str())?;
                            let args = tc
                                .pointer("/function/arguments")
                                .and_then(|x| x.as_str())
                                .unwrap_or("{}");
                            Some(ToolCall {
                                id: tc
                                    .get("id")
                                    .and_then(|x| x.as_str())
                                    .unwrap_or("tooluse_fakecloud_01")
                                    .to_string(),
                                name: name.to_string(),
                                input: serde_json::from_str::<Value>(args)
                                    .unwrap_or_else(|_| json!({})),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            (
                text,
                tool_calls,
                v.pointer("/choices/0/finish_reason")
                    .and_then(|s| s.as_str())
                    .map(str::to_string),
                v.pointer("/usage/prompt_tokens").and_then(|x| x.as_u64()),
                v.pointer("/usage/completion_tokens")
                    .and_then(|x| x.as_u64()),
            )
        }
        Protocol::Ollama => {
            let text = v
                .pointer("/message/content")
                .and_then(|t| t.as_str())
                .unwrap_or_default()
                .to_string();
            let tool_calls = v
                .pointer("/message/tool_calls")
                .and_then(|c| c.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|tc| {
                            let name = tc.pointer("/function/name").and_then(|x| x.as_str())?;
                            Some(ToolCall {
                                id: "tooluse_fakecloud_01".to_string(),
                                name: name.to_string(),
                                input: tc
                                    .pointer("/function/arguments")
                                    .cloned()
                                    .unwrap_or_else(|| json!({})),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            (
                text,
                tool_calls,
                v.get("done_reason")
                    .and_then(|s| s.as_str())
                    .map(str::to_string),
                v.get("prompt_eval_count").and_then(|x| x.as_u64()),
                v.get("eval_count").and_then(|x| x.as_u64()),
            )
        }
    };

    let input_tokens = in_t.unwrap_or_else(|| crate::prompt::count_tokens(&chat.prompt_text()));
    let output_tokens = out_t.unwrap_or_else(|| crate::prompt::count_tokens(&text));
    Ok(Completion {
        text,
        tool_calls,
        stop_reason,
        input_tokens,
        output_tokens,
    })
}

// ---------------------------------------------------------------------------
// Streaming chat
// ---------------------------------------------------------------------------

/// Accumulator for a streaming upstream response.
struct StreamAcc {
    deltas: Vec<String>,
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    stop_reason: Option<String>,
}

/// Byte-oriented line buffer for the streaming path. Raw chunk bytes are
/// accumulated and only *complete* lines (delimited by `\n`, which can never
/// appear inside a multibyte UTF-8 sequence) are decoded. Decoding whole
/// lines rather than each network chunk avoids splitting a multibyte
/// character across a chunk boundary and corrupting it to U+FFFD.
#[derive(Default)]
struct LineBuffer {
    buf: Vec<u8>,
}

impl LineBuffer {
    fn push(&mut self, chunk: &[u8], protocol: Protocol, acc: &mut StreamAcc) {
        self.buf.extend_from_slice(chunk);
        while let Some(pos) = self.buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=pos).collect();
            process_stream_line(protocol, &String::from_utf8_lossy(&line), acc);
        }
    }

    fn finish(&mut self, protocol: Protocol, acc: &mut StreamAcc) {
        if !self.buf.is_empty() {
            process_stream_line(protocol, &String::from_utf8_lossy(&self.buf), acc);
            self.buf.clear();
        }
    }
}

/// Parse a single completed line from the upstream stream, appending any
/// text delta and capturing usage counts. Handles SSE `data:` framing for
/// OpenAI/Anthropic and raw NDJSON for Ollama.
fn process_stream_line(protocol: Protocol, line: &str, acc: &mut StreamAcc) {
    let line = line.trim();
    if line.is_empty() {
        return;
    }

    let json_str = match protocol {
        Protocol::Ollama => line,
        _ => {
            let Some(data) = line.strip_prefix("data:") else {
                return;
            };
            let data = data.trim();
            if data == "[DONE]" || data.is_empty() {
                return;
            }
            data
        }
    };

    let Ok(v) = serde_json::from_str::<Value>(json_str) else {
        return;
    };

    match protocol {
        Protocol::Anthropic => match v.get("type").and_then(|t| t.as_str()) {
            Some("message_start") => {
                if let Some(n) = v
                    .pointer("/message/usage/input_tokens")
                    .and_then(|x| x.as_u64())
                {
                    acc.input_tokens = Some(n);
                }
            }
            Some("content_block_delta") => {
                if let Some(t) = v.pointer("/delta/text").and_then(|x| x.as_str()) {
                    if !t.is_empty() {
                        acc.deltas.push(t.to_string());
                    }
                }
            }
            Some("message_delta") => {
                if let Some(n) = v.pointer("/usage/output_tokens").and_then(|x| x.as_u64()) {
                    acc.output_tokens = Some(n);
                }
                if let Some(s) = v.pointer("/delta/stop_reason").and_then(|x| x.as_str()) {
                    acc.stop_reason = Some(s.to_string());
                }
            }
            _ => {}
        },
        Protocol::OpenAi => {
            if let Some(t) = v
                .pointer("/choices/0/delta/content")
                .and_then(|x| x.as_str())
            {
                if !t.is_empty() {
                    acc.deltas.push(t.to_string());
                }
            }
            if let Some(s) = v
                .pointer("/choices/0/finish_reason")
                .and_then(|x| x.as_str())
            {
                acc.stop_reason = Some(s.to_string());
            }
            if let Some(n) = v.pointer("/usage/prompt_tokens").and_then(|x| x.as_u64()) {
                acc.input_tokens = Some(n);
            }
            if let Some(n) = v
                .pointer("/usage/completion_tokens")
                .and_then(|x| x.as_u64())
            {
                acc.output_tokens = Some(n);
            }
        }
        Protocol::Ollama => {
            if let Some(t) = v.pointer("/message/content").and_then(|x| x.as_str()) {
                if !t.is_empty() {
                    acc.deltas.push(t.to_string());
                }
            }
            if let Some(s) = v.get("done_reason").and_then(|x| x.as_str()) {
                acc.stop_reason = Some(s.to_string());
            }
            if let Some(n) = v.get("prompt_eval_count").and_then(|x| x.as_u64()) {
                acc.input_tokens = Some(n);
            }
            if let Some(n) = v.get("eval_count").and_then(|x| x.as_u64()) {
                acc.output_tokens = Some(n);
            }
        }
    }
}

/// Ordered real token deltas plus resolved usage and stop reason from a
/// streaming upstream response. The caller re-frames the deltas as a Bedrock
/// event stream.
struct StreamResult {
    deltas: Vec<String>,
    stop: StopKind,
    input_tokens: u64,
    output_tokens: u64,
}

/// Stream a completion from the upstream. The deltas are the upstream's
/// *real* incremental tokens.
async fn call_chat_stream(
    cfg: &UpstreamConfig,
    upstream_model: &str,
    chat: &Chat,
) -> Result<StreamResult, AwsServiceError> {
    let client = http_client();
    let rb = chat_request_builder(&client, cfg, upstream_model, chat, true);
    let mut resp = rb
        .send()
        .await
        .map_err(|e| model_error(format!("upstream request failed: {e}")))?;
    let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    if !status.is_success() {
        let bytes = resp.bytes().await.unwrap_or_default();
        return Err(map_upstream_status(
            status,
            &String::from_utf8_lossy(&bytes),
        ));
    }

    let mut acc = StreamAcc {
        deltas: Vec::new(),
        input_tokens: None,
        output_tokens: None,
        stop_reason: None,
    };
    let mut buf = LineBuffer::default();

    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| model_error(format!("reading upstream stream failed: {e}")))?
    {
        buf.push(&chunk, cfg.protocol, &mut acc);
    }
    buf.finish(cfg.protocol, &mut acc);

    let output_text = acc.deltas.concat();
    let input_tokens = acc
        .input_tokens
        .unwrap_or_else(|| crate::prompt::count_tokens(&chat.prompt_text()));
    let output_tokens = acc
        .output_tokens
        .unwrap_or_else(|| crate::prompt::count_tokens(&output_text));
    Ok(StreamResult {
        deltas: acc.deltas,
        stop: normalize_stop(acc.stop_reason.as_deref(), false),
        input_tokens,
        output_tokens,
    })
}

// ---------------------------------------------------------------------------
// Embeddings
// ---------------------------------------------------------------------------

fn is_embedding_model(model_id: &str) -> bool {
    model_id.starts_with("amazon.titan-embed") || model_id.starts_with("cohere.embed")
}

fn deterministic_embedding(dim: usize) -> Vec<f64> {
    (0..dim).map(|i| (i as f64 * 0.001).sin()).collect()
}

fn build_embedding_response(
    model_id: &str,
    embedding: Vec<Value>,
    input_tokens: u64,
    text: &str,
) -> String {
    let value = if model_id.starts_with("cohere.") {
        json!({
            "embeddings": [embedding],
            "id": "fakecloud-upstream-embed",
            "response_type": "embeddings_floats",
            "texts": [text]
        })
    } else {
        json!({
            "embedding": embedding,
            "inputTextTokenCount": input_tokens
        })
    };
    serde_json::to_string(&value).expect("serde_json::Value serialization is infallible")
}

/// Deterministic offline embedding body, used both as the no-upstream
/// default and when the configured protocol can't do embeddings.
fn deterministic_embedding_response(model_id: &str, input_tokens: u64, text: &str) -> String {
    let dim = if model_id.starts_with("cohere.") {
        1024
    } else {
        256
    };
    let emb: Vec<Value> = deterministic_embedding(dim)
        .into_iter()
        .map(|f| json!(f))
        .collect();
    build_embedding_response(model_id, emb, input_tokens, text)
}

/// Returns `(response_body, input_tokens, output_tokens)`. Embeddings have
/// no generated output tokens, so `output_tokens` is always `0`.
async fn invoke_embedding_upstream(
    cfg: &UpstreamConfig,
    model_id: &str,
    input: &Value,
) -> Result<(String, u64, u64), AwsServiceError> {
    let text = crate::invoke::extract_input_text(input);
    let fallback_tokens = crate::prompt::count_tokens(&text);
    let embed_model = cfg.resolve_embed_model(model_id);

    match cfg.protocol {
        // The Anthropic Messages protocol has no embeddings endpoint;
        // fall back to the deterministic vector.
        Protocol::Anthropic => Ok((
            deterministic_embedding_response(model_id, fallback_tokens, &text),
            fallback_tokens,
            0,
        )),
        Protocol::OpenAi => {
            let client = http_client();
            let body = json!({ "model": embed_model, "input": text });
            let mut rb = client.post(cfg.endpoint("/v1/embeddings")).json(&body);
            if let Some(k) = &cfg.key {
                rb = rb.bearer_auth(k);
            }
            let v = send_and_read(rb).await?;
            let emb = v
                .pointer("/data/0/embedding")
                .and_then(|e| e.as_array())
                .cloned()
                .ok_or_else(|| {
                    model_error("upstream embeddings response missing data[0].embedding")
                })?;
            let in_tokens = v
                .pointer("/usage/prompt_tokens")
                .and_then(|x| x.as_u64())
                .unwrap_or(fallback_tokens);
            Ok((
                build_embedding_response(model_id, emb, in_tokens, &text),
                in_tokens,
                0,
            ))
        }
        Protocol::Ollama => {
            let client = http_client();
            let body = json!({ "model": embed_model, "prompt": text });
            let mut rb = client.post(cfg.endpoint("/api/embeddings")).json(&body);
            if let Some(k) = &cfg.key {
                rb = rb.bearer_auth(k);
            }
            let v = send_and_read(rb).await?;
            let emb = v
                .get("embedding")
                .and_then(|e| e.as_array())
                .cloned()
                .ok_or_else(|| model_error("upstream embeddings response missing embedding"))?;
            Ok((
                build_embedding_response(model_id, emb, fallback_tokens, &text),
                fallback_tokens,
                0,
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Public entry points (called from the async service handler)
// ---------------------------------------------------------------------------

/// Record a failed invocation for introspection parity, then surface the
/// error unchanged.
fn record_error(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
    err: AwsServiceError,
) -> AwsServiceError {
    crate::invoke::record_invocation(
        state,
        req,
        model_id,
        body,
        "",
        Some(format!("{}: {}", err.code(), err.message())),
    );
    err
}

/// Real-inference `InvokeModel`. Explicit simulation overrides and fault
/// injection still take precedence; otherwise the request is translated,
/// sent upstream, and translated back.
pub(crate) async fn invoke_model_upstream(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
    cfg: &UpstreamConfig,
) -> Result<AwsResponse, AwsServiceError> {
    if let Some(fault) = crate::faults::take_matching_fault(state, req, model_id, "InvokeModel") {
        crate::faults::record_faulted_invocation(state, req, model_id, body, &fault);
        return Err(crate::faults::fault_to_error(&fault));
    }

    let input = parse_body(body)?;

    // Explicit simulation overrides (response rules / custom responses)
    // keep working even with an upstream configured.
    if let Some(custom) = crate::prompt::resolve_override(state, req, model_id, body) {
        return Ok(crate::invoke::respond_with_body(
            state, req, model_id, body, &input, custom,
        ));
    }

    let result = if is_embedding_model(model_id) {
        invoke_embedding_upstream(cfg, model_id, &input).await
    } else {
        let chat = bedrock_request_to_chat(&input);
        let upstream_model = cfg.resolve_model(model_id);
        call_chat(cfg, &upstream_model, &chat).await.map(|c| {
            let in_t = c.input_tokens;
            let out_t = c.output_tokens;
            (build_provider_response(model_id, &c), in_t, out_t)
        })
    };

    match result {
        Ok((response_body, in_t, out_t)) => {
            crate::invoke::record_invocation(state, req, model_id, body, &response_body, None);
            Ok(crate::invoke::runtime_response_with_tokens(
                response_body,
                in_t,
                out_t,
            ))
        }
        Err(e) => Err(record_error(state, req, model_id, body, e)),
    }
}

/// Real-inference `Converse`. Produces the unified Converse response shape.
pub(crate) async fn converse_upstream(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
    cfg: &UpstreamConfig,
) -> Result<AwsResponse, AwsServiceError> {
    if let Some(fault) = crate::faults::take_matching_fault(state, req, model_id, "Converse") {
        crate::faults::record_faulted_invocation(state, req, model_id, body, &fault);
        return Err(crate::faults::fault_to_error(&fault));
    }

    let input = parse_body(body)?;
    let chat = bedrock_request_to_chat(&input);
    let upstream_model = cfg.resolve_model(model_id);

    let completion = match call_chat(cfg, &upstream_model, &chat).await {
        Ok(c) => c,
        Err(e) => return Err(record_error(state, req, model_id, body, e)),
    };

    // Build Converse content blocks: the assistant text, then a `toolUse`
    // block for every tool the model chose to call, mirroring the offline
    // `converse()` shape.
    let mut content: Vec<Value> = Vec::new();
    if !completion.text.is_empty() || completion.tool_calls.is_empty() {
        content.push(json!({ "text": completion.text }));
    }
    for tc in &completion.tool_calls {
        content.push(json!({
            "toolUse": {
                "toolUseId": tc.id,
                "name": tc.name,
                "input": tc.input
            }
        }));
    }

    let stop = normalize_stop(
        completion.stop_reason.as_deref(),
        !completion.tool_calls.is_empty(),
    );

    let response = json!({
        "output": {
            "message": {
                "role": "assistant",
                "content": content
            }
        },
        "stopReason": stop_converse(stop),
        "usage": {
            "inputTokens": completion.input_tokens,
            "outputTokens": completion.output_tokens,
            "totalTokens": completion.input_tokens + completion.output_tokens
        },
        "metrics": { "latencyMs": 100 }
    });
    let response_str =
        serde_json::to_string(&response).expect("serde_json::Value serialization is infallible");

    crate::invoke::record_invocation(state, req, model_id, body, &response_str, None);
    Ok(AwsResponse::json(StatusCode::OK, response_str))
}

/// Real-inference `InvokeModelWithResponseStream`. Streams incremental
/// tokens from the upstream and re-encodes them as a Bedrock event stream.
pub(crate) async fn invoke_stream_upstream(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
    cfg: &UpstreamConfig,
) -> Result<AwsResponse, AwsServiceError> {
    // A configured simulation override wins over the live upstream, same as
    // the non-streaming path: stream the override text deterministically.
    if let Some(custom) = crate::prompt::resolve_override(state, req, model_id, body) {
        let text = crate::streaming::override_text(model_id, custom);
        let input = serde_json::from_slice(body).unwrap_or(Value::Null);
        let in_t = crate::prompt::count_tokens(&crate::invoke::extract_input_text(&input));
        let out_t = crate::prompt::count_tokens(&text);
        let deltas = vec![text];
        let event_body = crate::streaming::build_invoke_stream_from_deltas(
            model_id,
            &deltas,
            StopKind::EndTurn,
            in_t,
            out_t,
        );
        crate::invoke::record_invocation(state, req, model_id, body, &deltas.concat(), None);
        return Ok(eventstream_response(event_body));
    }

    let input = parse_body(body)?;
    let chat = bedrock_request_to_chat(&input);
    let upstream_model = cfg.resolve_model(model_id);

    let result = match call_chat_stream(cfg, &upstream_model, &chat).await {
        Ok(t) => t,
        Err(e) => return Err(record_error(state, req, model_id, body, e)),
    };

    let event_body = crate::streaming::build_invoke_stream_from_deltas(
        model_id,
        &result.deltas,
        result.stop,
        result.input_tokens,
        result.output_tokens,
    );

    crate::invoke::record_invocation(state, req, model_id, body, &result.deltas.concat(), None);
    Ok(eventstream_response(event_body))
}

fn eventstream_response(event_body: Vec<u8>) -> AwsResponse {
    AwsResponse {
        status: StatusCode::OK,
        content_type: "application/vnd.amazon.eventstream".to_string(),
        body: bytes::Bytes::from(event_body).into(),
        headers: http::HeaderMap::new(),
    }
}

/// Real-inference `ConverseStream`.
pub(crate) async fn converse_stream_upstream(
    state: &SharedBedrockState,
    req: &AwsRequest,
    model_id: &str,
    body: &[u8],
    cfg: &UpstreamConfig,
) -> Result<AwsResponse, AwsServiceError> {
    // A configured simulation override wins over the live upstream.
    if let Some(custom) = crate::prompt::resolve_override(state, req, model_id, body) {
        let text = crate::streaming::override_text(model_id, custom);
        let in_t = crate::prompt::count_tokens(&crate::prompt::extract_prompt_text(model_id, body));
        let out_t = crate::prompt::count_tokens(&text);
        let deltas = vec![text];
        let event_body = crate::streaming::build_converse_stream_from_deltas(
            &deltas,
            StopKind::EndTurn,
            in_t,
            out_t,
        );
        crate::invoke::record_invocation(state, req, model_id, body, &deltas.concat(), None);
        return Ok(eventstream_response(event_body));
    }

    let input = parse_body(body)?;
    let chat = bedrock_request_to_chat(&input);
    let upstream_model = cfg.resolve_model(model_id);

    let result = match call_chat_stream(cfg, &upstream_model, &chat).await {
        Ok(t) => t,
        Err(e) => return Err(record_error(state, req, model_id, body, e)),
    };

    let event_body = crate::streaming::build_converse_stream_from_deltas(
        &result.deltas,
        result.stop,
        result.input_tokens,
        result.output_tokens,
    );

    crate::invoke::record_invocation(state, req, model_id, body, &result.deltas.concat(), None);
    Ok(eventstream_response(event_body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use axum::Router;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn shared() -> SharedBedrockState {
        Arc::new(RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "http://x",
        )))
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

    fn cfg(url: String, protocol: Protocol) -> UpstreamConfig {
        UpstreamConfig {
            url,
            key: Some("test-key".to_string()),
            protocol,
            default_model: Some("upstream-model".to_string()),
            embed_model: None,
            model_map: vec![],
        }
    }

    async fn spawn(app: Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        format!("http://{addr}")
    }

    fn body_bytes(resp: &AwsResponse) -> Vec<u8> {
        resp.body.expect_bytes().to_vec()
    }

    fn header(resp: &AwsResponse, name: &str) -> String {
        resp.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string()
    }

    fn split_frames(buf: &[u8]) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        let mut i = 0;
        while i + 4 <= buf.len() {
            let total = u32::from_be_bytes([buf[i], buf[i + 1], buf[i + 2], buf[i + 3]]) as usize;
            if total < 16 || i + total > buf.len() {
                break;
            }
            frames.push(buf[i..i + total].to_vec());
            i += total;
        }
        frames
    }

    fn frame_payload(frame: &[u8]) -> Vec<u8> {
        let headers_len = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]) as usize;
        let start = 12 + headers_len;
        let end = frame.len() - 4;
        frame[start..end].to_vec()
    }

    fn invoke_frame_inner(frame: &[u8]) -> Value {
        use base64::Engine;
        let payload: Value = serde_json::from_slice(&frame_payload(frame)).unwrap();
        let b64 = payload["bytes"].as_str().unwrap();
        let raw = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .unwrap();
        serde_json::from_slice(&raw).unwrap()
    }

    fn converse_frame_inner(frame: &[u8]) -> Value {
        serde_json::from_slice(&frame_payload(frame)).unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invoke_openai_upstream_translates_to_anthropic_bedrock_shape() {
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|| async {
                axum::Json(json!({
                    "choices": [{ "message": { "content": "Hello from the upstream model" } }],
                    "usage": { "prompt_tokens": 11, "completion_tokens": 4 }
                }))
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":"hello"}],"max_tokens":50}"#;

        let resp = invoke_model_upstream(
            &s,
            &req(),
            "anthropic.claude-3-5-sonnet-20240620-v1:0",
            body,
            &c,
        )
        .await
        .unwrap();

        let v: Value = serde_json::from_slice(&body_bytes(&resp)).unwrap();
        assert_eq!(v["type"], "message");
        assert_eq!(v["role"], "assistant");
        assert_eq!(v["content"][0]["text"], "Hello from the upstream model");
        assert_eq!(v["usage"]["input_tokens"], 11);
        assert_eq!(v["usage"]["output_tokens"], 4);
        assert_eq!(header(&resp, "x-amzn-bedrock-input-token-count"), "11");
        assert_eq!(header(&resp, "x-amzn-bedrock-output-token-count"), "4");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invoke_titan_upstream_translates_to_amazon_bedrock_shape() {
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|| async {
                axum::Json(json!({
                    "choices": [{ "message": { "content": "titan-style answer" } }],
                    "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
                }))
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"inputText":"hi there","textGenerationConfig":{"maxTokenCount":32}}"#;

        let resp = invoke_model_upstream(&s, &req(), "amazon.titan-text-express-v1", body, &c)
            .await
            .unwrap();

        let v: Value = serde_json::from_slice(&body_bytes(&resp)).unwrap();
        assert_eq!(v["results"][0]["outputText"], "titan-style answer");
        assert_eq!(v["inputTextTokenCount"], 3);
        assert_eq!(v["results"][0]["tokenCount"], 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn converse_anthropic_upstream_translates_both_directions() {
        let app = Router::new().route(
            "/v1/messages",
            post(|axum::Json(payload): axum::Json<Value>| async move {
                assert_eq!(payload["model"], "upstream-model");
                assert_eq!(payload["messages"][0]["role"], "user");
                assert_eq!(payload["messages"][0]["content"], "convo prompt");
                axum::Json(json!({
                    "content": [{ "type": "text", "text": "converse reply" }],
                    "usage": { "input_tokens": 6, "output_tokens": 3 }
                }))
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::Anthropic);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":[{"text":"convo prompt"}]}]}"#;

        let resp = converse_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .unwrap();

        let v: Value = serde_json::from_slice(&body_bytes(&resp)).unwrap();
        assert_eq!(
            v["output"]["message"]["content"][0]["text"],
            "converse reply"
        );
        assert_eq!(v["usage"]["inputTokens"], 6);
        assert_eq!(v["usage"]["outputTokens"], 3);
        assert_eq!(v["usage"]["totalTokens"], 9);
        assert_eq!(v["stopReason"], "end_turn");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invoke_stream_upstream_reencodes_real_openai_deltas() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"Hel\"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{\"content\":\"lo \"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{\"content\":\"world\"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":8,\"completion_tokens\":3}}\n\n\
                   data: [DONE]\n\n";
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let sse = sse.to_string();
                async move { ([(http::header::CONTENT_TYPE, "text/event-stream")], sse) }
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":"stream please"}]}"#;

        let resp = invoke_stream_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .unwrap();

        let frames = split_frames(&body_bytes(&resp));
        let mut deltas = Vec::new();
        let mut out_tokens = None;
        let mut in_tokens = None;
        for f in &frames {
            let inner = invoke_frame_inner(f);
            match inner["type"].as_str() {
                Some("message_start") => {
                    in_tokens = inner["message"]["usage"]["input_tokens"].as_u64();
                }
                Some("content_block_delta") => {
                    deltas.push(inner["delta"]["text"].as_str().unwrap().to_string());
                }
                Some("message_delta") => {
                    out_tokens = inner["usage"]["output_tokens"].as_u64();
                }
                _ => {}
            }
        }
        assert_eq!(deltas.len(), 3);
        assert_eq!(deltas.concat(), "Hello world");
        assert_eq!(in_tokens, Some(8));
        assert_eq!(out_tokens, Some(3));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn converse_stream_upstream_reencodes_real_deltas_with_usage() {
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"foo\"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{\"content\":\"bar\"}}]}\n\n\
                   data: {\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2}}\n\n\
                   data: [DONE]\n\n";
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let sse = sse.to_string();
                async move { ([(http::header::CONTENT_TYPE, "text/event-stream")], sse) }
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":[{"text":"hi"}]}]}"#;

        let resp = converse_stream_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .unwrap();

        let frames = split_frames(&body_bytes(&resp));
        let mut delta_texts = Vec::new();
        let mut meta_in = None;
        let mut meta_out = None;
        for f in &frames {
            let inner = converse_frame_inner(f);
            if let Some(d) = inner.get("contentBlockDelta") {
                delta_texts.push(d["delta"]["text"].as_str().unwrap().to_string());
            }
            if let Some(m) = inner.get("metadata") {
                meta_in = m["usage"]["inputTokens"].as_u64();
                meta_out = m["usage"]["outputTokens"].as_u64();
            }
        }
        assert_eq!(delta_texts, vec!["foo".to_string(), "bar".to_string()]);
        assert_eq!(meta_in, Some(5));
        assert_eq!(meta_out, Some(2));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upstream_500_maps_to_model_error_exception_not_panic() {
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|| async { (http::StatusCode::INTERNAL_SERVER_ERROR, "boom") }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":"hi"}]}"#;

        let err = invoke_model_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .err()
            .expect("upstream 500 should surface as an error");
        assert_eq!(err.code(), "ModelErrorException");
        assert_eq!(err.status().as_u16(), 424);

        let st = s.read();
        assert_eq!(st.default_ref().invocations.len(), 1);
        assert!(st.default_ref().invocations[0].error.is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upstream_429_maps_to_throttling_exception() {
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|| async { (http::StatusCode::TOO_MANY_REQUESTS, "slow down") }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":"hi"}]}"#;

        let err = invoke_model_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ThrottlingException");
        assert_eq!(err.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn upstream_malformed_body_maps_to_model_error() {
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|| async { "this is not json" }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":"hi"}]}"#;

        let err = invoke_model_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ModelErrorException");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn embedding_openai_upstream_returns_titan_shape() {
        let app = Router::new().route(
            "/v1/embeddings",
            post(|| async {
                axum::Json(json!({
                    "data": [{ "embedding": [0.1, 0.2, 0.3] }],
                    "usage": { "prompt_tokens": 5 }
                }))
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"inputText":"embed me"}"#;

        let resp = invoke_model_upstream(&s, &req(), "amazon.titan-embed-text-v1", body, &c)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body_bytes(&resp)).unwrap();
        assert_eq!(v["embedding"], json!([0.1, 0.2, 0.3]));
        assert_eq!(v["inputTextTokenCount"], 5);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn embedding_anthropic_protocol_falls_back_to_deterministic_vector() {
        let c = cfg("http://127.0.0.1:1".to_string(), Protocol::Anthropic);
        let s = shared();
        let body = br#"{"inputText":"embed me"}"#;
        let resp = invoke_model_upstream(&s, &req(), "amazon.titan-embed-text-v1", body, &c)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body_bytes(&resp)).unwrap();
        assert!(v["embedding"].is_array());
        assert_eq!(v["embedding"].as_array().unwrap().len(), 256);
    }

    #[test]
    fn from_env_none_when_url_unset() {
        let prev = std::env::var("FAKECLOUD_BEDROCK_UPSTREAM_URL").ok();
        // SAFETY: single-threaded test; restored below.
        unsafe { std::env::remove_var("FAKECLOUD_BEDROCK_UPSTREAM_URL") };
        assert!(UpstreamConfig::from_env().is_none());
        // SAFETY: see above.
        if let Some(p) = prev {
            unsafe { std::env::set_var("FAKECLOUD_BEDROCK_UPSTREAM_URL", p) };
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invoke_stream_llama_reencodes_as_generation_not_output_text() {
        // A non-anthropic (Llama) stream must use the Meta-native
        // `generation` field, not Titan `outputText`, or the aws-sdk Llama
        // decoder yields empty chunks.
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"Lla\"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{\"content\":\"ma!\"}}]}\n\n\
                   data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\n\
                   data: [DONE]\n\n";
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let sse = sse.to_string();
                async move { ([(http::header::CONTENT_TYPE, "text/event-stream")], sse) }
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"prompt":"hello llama","max_gen_len":16}"#;

        let resp = invoke_stream_upstream(&s, &req(), "meta.llama3-70b-instruct-v1:0", body, &c)
            .await
            .unwrap();

        let frames = split_frames(&body_bytes(&resp));
        let mut gens = Vec::new();
        for f in &frames {
            let inner = invoke_frame_inner(f);
            // No frame may carry Titan `outputText`.
            assert!(
                inner.get("outputText").is_none(),
                "llama stream must not use outputText: {inner}"
            );
            if let Some(g) = inner.get("generation").and_then(|g| g.as_str()) {
                if !g.is_empty() {
                    gens.push(g.to_string());
                }
            }
        }
        assert_eq!(gens.concat(), "Llama!");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invoke_stream_regional_profile_uses_anthropic_shape() {
        // A cross-region inference-profile id (`us.anthropic.*`) must still
        // render the Anthropic Messages streaming shape.
        let sse = "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n\
                   data: [DONE]\n\n";
        let app = Router::new().route(
            "/v1/chat/completions",
            post(move || {
                let sse = sse.to_string();
                async move { ([(http::header::CONTENT_TYPE, "text/event-stream")], sse) }
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":"hi"}]}"#;

        let resp = invoke_stream_upstream(
            &s,
            &req(),
            "us.anthropic.claude-3-5-sonnet-20240620-v1:0",
            body,
            &c,
        )
        .await
        .unwrap();

        let frames = split_frames(&body_bytes(&resp));
        let types: Vec<String> = frames
            .iter()
            .filter_map(|f| invoke_frame_inner(f)["type"].as_str().map(str::to_string))
            .collect();
        assert!(types.iter().any(|t| t == "message_start"));
        assert!(types.iter().any(|t| t == "content_block_delta"));
    }

    #[test]
    fn line_buffer_decodes_multibyte_char_split_across_chunks() {
        // Regression: a UTF-8 sequence ('é' = 0xC3 0xA9) split across two
        // network chunks must not be corrupted to U+FFFD. The old code
        // lossy-decoded each chunk before buffering; the byte-oriented
        // LineBuffer only decodes complete lines.
        let line = "data: {\"choices\":[{\"delta\":{\"content\":\"café\"}}]}\n";
        let bytes = line.as_bytes();
        // Find the byte offset of 'é' (first non-ASCII byte) and split it.
        let split = bytes.iter().position(|&b| b >= 0x80).unwrap() + 1;
        assert!(split < bytes.len());

        let mut acc = StreamAcc {
            deltas: Vec::new(),
            input_tokens: None,
            output_tokens: None,
            stop_reason: None,
        };
        let mut buf = LineBuffer::default();
        buf.push(&bytes[..split], Protocol::OpenAi, &mut acc);
        buf.push(&bytes[split..], Protocol::OpenAi, &mut acc);
        buf.finish(Protocol::OpenAi, &mut acc);

        assert_eq!(acc.deltas.concat(), "café");
        assert!(
            !acc.deltas.concat().contains('\u{FFFD}'),
            "multibyte char was corrupted"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn converse_tool_config_produces_tool_use_block_on_upstream_path() {
        // The upstream returns an OpenAI-style tool call; converse_upstream
        // must map it back to a Bedrock toolUse block + stopReason tool_use,
        // and must have forwarded the tool spec to the upstream.
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|axum::Json(payload): axum::Json<Value>| async move {
                // Tool spec was translated into the OpenAI tools shape.
                assert_eq!(payload["tools"][0]["function"]["name"], "get_weather");
                axum::Json(json!({
                    "choices": [{
                        "message": {
                            "content": null,
                            "tool_calls": [{
                                "id": "call_abc",
                                "type": "function",
                                "function": {
                                    "name": "get_weather",
                                    "arguments": "{\"city\":\"Paris\"}"
                                }
                            }]
                        },
                        "finish_reason": "tool_calls"
                    }],
                    "usage": { "prompt_tokens": 9, "completion_tokens": 5 }
                }))
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{
            "messages":[{"role":"user","content":[{"text":"weather in Paris?"}]}],
            "toolConfig":{"tools":[{"toolSpec":{"name":"get_weather","description":"weather","inputSchema":{"json":{"type":"object"}}}}]}
        }"#;

        let resp = converse_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body_bytes(&resp)).unwrap();
        assert_eq!(v["stopReason"], "tool_use");
        let content = v["output"]["message"]["content"].as_array().unwrap();
        let tool_use = content
            .iter()
            .find(|b| b.get("toolUse").is_some())
            .expect("expected a toolUse content block");
        assert_eq!(tool_use["toolUse"]["name"], "get_weather");
        assert_eq!(tool_use["toolUse"]["input"]["city"], "Paris");
        assert_eq!(tool_use["toolUse"]["toolUseId"], "call_abc");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn converse_propagates_max_tokens_stop_reason() {
        // An upstream length-truncated generation must surface as Bedrock
        // stopReason `max_tokens`, not a natural `end_turn`.
        let app = Router::new().route(
            "/v1/chat/completions",
            post(|| async {
                axum::Json(json!({
                    "choices": [{ "message": { "content": "trunc" }, "finish_reason": "length" }],
                    "usage": { "prompt_tokens": 3, "completion_tokens": 2 }
                }))
            }),
        );
        let url = spawn(app).await;
        let c = cfg(url, Protocol::OpenAi);
        let s = shared();
        let body = br#"{"messages":[{"role":"user","content":[{"text":"hi"}]}],"inferenceConfig":{"maxTokens":2}}"#;

        let resp = converse_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(&body_bytes(&resp)).unwrap();
        assert_eq!(v["stopReason"], "max_tokens");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invoke_malformed_request_body_returns_validation_exception() {
        // A request body that isn't valid JSON is rejected before any
        // upstream call (real Bedrock returns ValidationException), rather
        // than being coerced to null and forwarded.
        let c = cfg("http://127.0.0.1:1".to_string(), Protocol::OpenAi);
        let s = shared();
        let err = invoke_model_upstream(&s, &req(), "anthropic.claude-3", b"not json {", &c)
            .await
            .err()
            .expect("malformed body must be rejected");
        assert_eq!(err.code(), "ValidationException");
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invoke_stream_override_streams_deterministically_without_upstream() {
        // With a configured override, the streaming path must NOT call the
        // live upstream; pointing at a dead URL proves the override wins.
        let c = cfg("http://127.0.0.1:1".to_string(), Protocol::OpenAi);
        let s = shared();
        {
            let mut st = s.write();
            st.default_mut().custom_responses.insert(
                "anthropic.claude-3".to_string(),
                "OVERRIDE TEXT".to_string(),
            );
        }
        let body = br#"{"messages":[{"role":"user","content":"hi"}]}"#;
        let resp = invoke_stream_upstream(&s, &req(), "anthropic.claude-3", body, &c)
            .await
            .expect("override path should not touch the dead upstream");
        let frames = split_frames(&body_bytes(&resp));
        let mut text = String::new();
        for f in &frames {
            let inner = invoke_frame_inner(f);
            if inner["type"] == "content_block_delta" {
                text.push_str(inner["delta"]["text"].as_str().unwrap());
            }
        }
        assert_eq!(text, "OVERRIDE TEXT");
    }

    #[test]
    fn resolve_model_prefers_map_then_default_then_stripped() {
        let c = UpstreamConfig {
            url: "http://x".to_string(),
            key: None,
            protocol: Protocol::OpenAi,
            default_model: Some("dflt".to_string()),
            embed_model: None,
            model_map: vec![(
                "anthropic.claude-3".to_string(),
                "mapped-claude".to_string(),
            )],
        };
        assert_eq!(
            c.resolve_model("anthropic.claude-3-5-sonnet"),
            "mapped-claude"
        );
        assert_eq!(c.resolve_model("meta.llama3"), "dflt");
        let c2 = UpstreamConfig {
            default_model: None,
            ..c
        };
        assert_eq!(c2.resolve_model("meta.llama3-70b"), "llama3-70b");
        // Cross-region inference-profile prefix is stripped before the
        // provider namespace when falling back to the bare model name.
        assert_eq!(c2.resolve_model("us.meta.llama3-70b"), "llama3-70b");
    }

    #[test]
    fn strip_region_prefix_handles_inference_profiles() {
        assert_eq!(
            strip_region_prefix("us.anthropic.claude-3"),
            "anthropic.claude-3"
        );
        assert_eq!(
            strip_region_prefix("eu.amazon.nova-lite"),
            "amazon.nova-lite"
        );
        assert_eq!(
            strip_region_prefix("apac.anthropic.claude-3"),
            "anthropic.claude-3"
        );
        // A bare provider id (no region) is unchanged.
        assert_eq!(
            strip_region_prefix("anthropic.claude-3"),
            "anthropic.claude-3"
        );
    }
}
