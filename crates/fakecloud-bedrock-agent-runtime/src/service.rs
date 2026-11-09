use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use regex::Regex;
use serde_json::{json, Value};

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::arns::{flow_execution_arn, session_arn};
use crate::state::{
    BedrockAgentRuntimeSnapshot, FlowExecution, InvocationRecord, InvocationStep, Session,
    SessionInvocation, SharedBedrockAgentRuntimeState,
    BEDROCK_AGENT_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
};

/// Whether `action` changes persisted state. Reads start with `Get` or `List`;
/// the rest listed here only append to the invocation log (an introspection
/// buffer that is not persisted) or touch no state at all. Every other action
/// is a mutation, so a newly added one is persisted by default.
fn is_mutating_action(action: &str) -> bool {
    !(action.starts_with("Get")
        || action.starts_with("List")
        || matches!(
            action,
            "InvokeAgent"
                | "InvokeInlineAgent"
                | "Retrieve"
                | "RetrieveAndGenerate"
                | "RetrieveAndGenerateStream"
                | "OptimizePrompt"
                | "GenerateQuery"
                | "Rerank"
                | "DeleteAgentMemory"
        ))
}

const SUPPORTED_ACTIONS: &[&str] = &[
    "InvokeAgent",
    "InvokeFlow",
    "InvokeInlineAgent",
    "OptimizePrompt",
    "Retrieve",
    "RetrieveAndGenerate",
    "RetrieveAndGenerateStream",
    "CreateSession",
    "DeleteSession",
    "EndSession",
    "GetSession",
    "ListSessions",
    "UpdateSession",
    "CreateInvocation",
    "GetInvocationStep",
    "ListInvocationSteps",
    "ListInvocations",
    "PutInvocationStep",
    "GetFlowExecution",
    "ListFlowExecutionEvents",
    "ListFlowExecutions",
    "StartFlowExecution",
    "StopFlowExecution",
    "GetExecutionFlowSnapshot",
    "GenerateQuery",
    "Rerank",
    "DeleteAgentMemory",
    "GetAgentMemory",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
];

pub struct BedrockAgentRuntimeService {
    state: SharedBedrockAgentRuntimeState,
    agent_state: Option<fakecloud_bedrock_agent::SharedBedrockAgentState>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl BedrockAgentRuntimeService {
    pub fn new(state: SharedBedrockAgentRuntimeState) -> Self {
        Self {
            state,
            agent_state: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Persist hook for callers that change this service's state from outside
    /// (the reset endpoints): writes the current snapshot. `None` in memory
    /// mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        Some(fakecloud_persistence::snapshot_hook(
            self.state.clone(),
            store,
            self.snapshot_lock.clone(),
            |state, store, lock| async move {
                save_bedrock_agent_runtime_snapshot(&state, Some(store), &lock).await;
            },
        ))
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_bedrock_agent_runtime_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    pub fn with_agent_state(
        mut self,
        agent_state: fakecloud_bedrock_agent::SharedBedrockAgentState,
    ) -> Self {
        self.agent_state = Some(agent_state);
        self
    }

    pub fn shared_state(&self) -> SharedBedrockAgentRuntimeState {
        Arc::clone(&self.state)
    }

    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<(String, String)>)> {
        let segs = &req.path_segments;
        if segs.is_empty() {
            return None;
        }

        let m = &req.method;
        let mut params: Vec<(String, String)> = Vec::new();

        // InvokeAgent: POST /agents/{agentId}/agentAliases/{agentAliasId}/sessions/{sessionId}/text
        if segs.len() == 7
            && segs[0] == "agents"
            && segs[2] == "agentAliases"
            && segs[4] == "sessions"
            && segs[6] == "text"
            && *m == Method::POST
        {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentAliasId".to_string(), segs[3].clone()));
            params.push(("sessionId".to_string(), segs[5].clone()));
            return Some(("InvokeAgent", params));
        }

        // InvokeFlow: POST /flows/{flowIdentifier}/aliases/{flowAliasIdentifier}
        if segs.len() == 4 && segs[0] == "flows" && segs[2] == "aliases" && *m == Method::POST {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            params.push(("flowAliasIdentifier".to_string(), segs[3].clone()));
            return Some(("InvokeFlow", params));
        }

        // InvokeInlineAgent: POST /agents/{sessionId}
        if segs.len() == 2 && segs[0] == "agents" && *m == Method::POST {
            params.push(("sessionId".to_string(), segs[1].clone()));
            return Some(("InvokeInlineAgent", params));
        }

        // OptimizePrompt: POST /optimize-prompt
        if segs.len() == 1 && segs[0] == "optimize-prompt" && *m == Method::POST {
            return Some(("OptimizePrompt", params));
        }

        // Retrieve: POST /knowledgebases/{knowledgeBaseId}/retrieve
        if segs.len() == 3
            && segs[0] == "knowledgebases"
            && segs[2] == "retrieve"
            && *m == Method::POST
        {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            return Some(("Retrieve", params));
        }

        // RetrieveAndGenerate: POST /retrieveAndGenerate
        if segs.len() == 1 && segs[0] == "retrieveAndGenerate" && *m == Method::POST {
            return Some(("RetrieveAndGenerate", params));
        }
        if segs.len() == 1 && segs[0] == "retrieveAndGenerateStream" && *m == Method::POST {
            return Some(("RetrieveAndGenerateStream", params));
        }

        // Sessions
        if segs.len() == 1 && segs[0] == "sessions" && *m == Method::PUT {
            return Some(("CreateSession", params));
        }
        if segs.len() == 1 && segs[0] == "sessions" && *m == Method::POST {
            return Some(("ListSessions", params));
        }
        if segs.len() == 2 && segs[0] == "sessions" {
            params.push(("sessionIdentifier".to_string(), segs[1].clone()));
            if *m == Method::GET {
                return Some(("GetSession", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateSession", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteSession", params));
            }
            if *m == Method::PATCH {
                return Some(("EndSession", params));
            }
        }

        // Session-nested resources
        if segs.len() == 3 && segs[0] == "sessions" && segs[2] == "invocations" {
            params.push(("sessionIdentifier".to_string(), segs[1].clone()));
            if *m == Method::PUT {
                return Some(("CreateInvocation", params));
            }
            if *m == Method::POST {
                return Some(("ListInvocations", params));
            }
        }
        if segs.len() == 3 && segs[0] == "sessions" && segs[2] == "invocationSteps" {
            params.push(("sessionIdentifier".to_string(), segs[1].clone()));
            if *m == Method::PUT {
                return Some(("PutInvocationStep", params));
            }
            if *m == Method::POST {
                return Some(("ListInvocationSteps", params));
            }
        }
        if segs.len() == 4
            && segs[0] == "sessions"
            && segs[2] == "invocationSteps"
            && *m == Method::POST
        {
            params.push(("sessionIdentifier".to_string(), segs[1].clone()));
            params.push(("invocationStepId".to_string(), segs[3].clone()));
            return Some(("GetInvocationStep", params));
        }

        // Flow executions
        if segs.len() == 3 && segs[0] == "flows" && segs[2] == "executions" && *m == Method::GET {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            return Some(("ListFlowExecutions", params));
        }
        if segs.len() == 6
            && segs[0] == "flows"
            && segs[2] == "aliases"
            && segs[4] == "executions"
            && *m == Method::GET
        {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            params.push(("flowAliasIdentifier".to_string(), segs[3].clone()));
            params.push(("executionIdentifier".to_string(), segs[5].clone()));
            return Some(("GetFlowExecution", params));
        }
        if segs.len() == 5
            && segs[0] == "flows"
            && segs[2] == "aliases"
            && segs[4] == "executions"
            && *m == Method::POST
        {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            params.push(("flowAliasIdentifier".to_string(), segs[3].clone()));
            return Some(("StartFlowExecution", params));
        }
        if segs.len() == 7
            && segs[0] == "flows"
            && segs[2] == "aliases"
            && segs[4] == "executions"
            && segs[6] == "stop"
            && *m == Method::POST
        {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            params.push(("flowAliasIdentifier".to_string(), segs[3].clone()));
            params.push(("executionIdentifier".to_string(), segs[5].clone()));
            return Some(("StopFlowExecution", params));
        }
        if segs.len() == 7
            && segs[0] == "flows"
            && segs[2] == "aliases"
            && segs[4] == "executions"
            && segs[6] == "events"
            && *m == Method::GET
        {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            params.push(("flowAliasIdentifier".to_string(), segs[3].clone()));
            params.push(("executionIdentifier".to_string(), segs[5].clone()));
            return Some(("ListFlowExecutionEvents", params));
        }
        if segs.len() == 7
            && segs[0] == "flows"
            && segs[2] == "aliases"
            && segs[4] == "executions"
            && segs[6] == "flowsnapshot"
            && *m == Method::GET
        {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            params.push(("flowAliasIdentifier".to_string(), segs[3].clone()));
            params.push(("executionIdentifier".to_string(), segs[5].clone()));
            return Some(("GetExecutionFlowSnapshot", params));
        }

        // GenerateQuery: POST /generateQuery
        if segs.len() == 1 && segs[0] == "generateQuery" && *m == Method::POST {
            return Some(("GenerateQuery", params));
        }

        // Rerank: POST /rerank
        if segs.len() == 1 && segs[0] == "rerank" && *m == Method::POST {
            return Some(("Rerank", params));
        }

        // Agent memory: /agents/{agentId}/agentAliases/{agentAliasId}/memories
        if segs.len() == 5
            && segs[0] == "agents"
            && segs[2] == "agentAliases"
            && segs[4] == "memories"
        {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentAliasId".to_string(), segs[3].clone()));
            if *m == Method::GET {
                return Some(("GetAgentMemory", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteAgentMemory", params));
            }
        }

        // Tagging
        if segs.len() == 2 && segs[0] == "tags" {
            params.push(("resourceArn".to_string(), segs[1].clone()));
            if *m == Method::POST {
                return Some(("TagResource", params));
            }
            if *m == Method::DELETE {
                return Some(("UntagResource", params));
            }
            if *m == Method::GET {
                return Some(("ListTagsForResource", params));
            }
        }

        None
    }
}

fn req_str(body: &Value, key: &str) -> Option<String> {
    body.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

pub(crate) fn make_error(status: StatusCode, code: &str, message: &str) -> AwsServiceError {
    AwsServiceError::aws_error(status, code, message)
}

fn validation(message: &str) -> AwsServiceError {
    make_error(StatusCode::BAD_REQUEST, "ValidationException", message)
}

/// Validate a string field is present and matches a regex pattern + length range.
fn validate_str(
    value: Option<&str>,
    name: &str,
    pattern: Option<&Regex>,
    min: Option<usize>,
    max: Option<usize>,
    required: bool,
) -> Result<(), AwsServiceError> {
    match value {
        None => {
            if required {
                Err(validation(&format!("{} is required", name)))
            } else {
                Ok(())
            }
        }
        Some(s) => {
            if required && s.is_empty() {
                return Err(validation(&format!("{} must not be empty", name)));
            }
            if let Some(mn) = min {
                if s.chars().count() < mn {
                    return Err(validation(&format!(
                        "{} must be at least {} characters",
                        name, mn
                    )));
                }
            }
            if let Some(mx) = max {
                if s.chars().count() > mx {
                    return Err(validation(&format!(
                        "{} must be at most {} characters",
                        name, mx
                    )));
                }
            }
            if let Some(re) = pattern {
                if !re.is_match(s) {
                    return Err(validation(&format!(
                        "{} does not match required pattern",
                        name
                    )));
                }
            }
            Ok(())
        }
    }
}

// Identifier patterns, compiled once on first use. They mirror the Smithy
// model's patterns byte-for-byte (ECMA semantics), so keep them verbatim.
fn re_session_identifier() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(arn:aws(-[^:]+)?:bedrock:[a-z0-9-]+:[0-9]{12}:session/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})|([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$").unwrap()
    });
    &RE
}
fn re_agent_id() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[0-9a-zA-Z]+$").unwrap());
    &RE
}
fn re_session_id() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[0-9a-zA-Z._:-]+$").unwrap());
    &RE
}
fn re_uuid() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$").unwrap()
    });
    &RE
}
fn re_memory_id() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[0-9a-zA-Z._:-]+$").unwrap());
    &RE
}
fn re_flow_execution_id() -> &'static Regex {
    // FlowExecutionIdentifier: max 2048, no pattern in model
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[\x21-\x7e]+$").unwrap());
    &RE
}
fn re_taggable_arn() -> &'static Regex {
    // TaggableResourcesArn: lenient; reject literal {placeholder}
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^arn:[a-zA-Z0-9-]+:[a-zA-Z0-9-]+:[a-z0-9-]*:[0-9]{12}:.+$").unwrap()
    });
    &RE
}
fn re_flow_identifier() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"^(arn:aws:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:flow/[0-9a-zA-Z]{10})|([0-9a-zA-Z]{10})$",
        )
        .unwrap()
    });
    &RE
}
fn re_flow_alias_identifier() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^(arn:aws:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:flow/[0-9a-zA-Z]{10}/alias/[0-9a-zA-Z]{10})|(\bTSTALIASID\b|[0-9a-zA-Z]+)$").unwrap()
    });
    &RE
}
fn re_no_whitespace() -> &'static Regex {
    // NextToken: ^\S*$
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\S*$").unwrap());
    &RE
}
fn re_aws_arn() -> &'static Regex {
    // Generic AWS resource ARN
    static RE: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"^arn:aws(-[^:]+)?:[a-zA-Z0-9-]+:[a-z0-9-]*:[0-9]{12}:.+$").unwrap()
    });
    &RE
}
fn re_flow_execution_name() -> &'static Regex {
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9-]+$").unwrap());
    &RE
}
fn re_knowledge_base_id() -> &'static Regex {
    // KnowledgeBaseId: alphanumeric; the length bound is checked separately.
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^[0-9a-zA-Z]+$").unwrap());
    &RE
}

/// Validate optional integer field against a range.
fn validate_int_range(
    value: Option<i64>,
    name: &str,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    if let Some(v) = value {
        if v < min || v > max {
            return Err(validation(&format!(
                "{} must be between {} and {}",
                name, min, max
            )));
        }
    }
    Ok(())
}

/// Validate optional NextToken-like query/body string.
fn validate_next_token(req: &AwsRequest, body: &Value) -> Result<(), AwsServiceError> {
    let token = req
        .query_params
        .get("nextToken")
        .cloned()
        .or_else(|| req_str(body, "nextToken"));
    if let Some(t) = token {
        validate_str(
            Some(&t),
            "nextToken",
            Some(re_no_whitespace()),
            Some(1),
            Some(2048),
            false,
        )?;
    }
    Ok(())
}

/// Pull a maxResults / maxItems integer from query (preferred) or body.
fn extract_int(req: &AwsRequest, body: &Value, name: &str) -> Option<i64> {
    if let Some(s) = req.query_params.get(name) {
        if let Ok(v) = s.parse::<i64>() {
            return Some(v);
        }
    }
    body.get(name).and_then(|v| v.as_i64())
}

fn parse_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Null)
}

fn merge_path_params(body: Value, path_params: &[(String, String)]) -> Value {
    let mut out = match body {
        Value::Object(m) => m,
        _ => serde_json::Map::new(),
    };
    // Labels come from `path_segments`, which dispatch already decoded (an
    // ARN label's `%3A` / `%2F` are real `:` / `/` here); never decode twice.
    for (k, v) in path_params {
        out.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(out)
}

#[async_trait]
impl AwsService for BedrockAgentRuntimeService {
    fn service_name(&self) -> &'static str {
        "bedrock-agent-runtime"
    }

    fn supported_actions(&self) -> &[&'static str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, mut req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, path_params) =
            Self::resolve_action(&req).ok_or_else(|| AwsServiceError::ActionNotImplemented {
                service: "bedrock-agent-runtime".to_string(),
                action: format!("{} {}", req.method, req.raw_path),
            })?;

        req.action = action.to_string();

        let body = merge_path_params(parse_body(&req), &path_params);

        let mutates = is_mutating_action(action);
        let result = match action {
            "InvokeAgent" => handle_invoke_agent(self, &req, &body).await,
            "InvokeFlow" => handle_invoke_flow(self, &req, &body).await,
            "InvokeInlineAgent" => handle_invoke_inline_agent(self, &req, &body).await,
            "OptimizePrompt" => handle_optimize_prompt(self, &req, &body).await,
            "Retrieve" => handle_retrieve(self, &req, &body).await,
            "RetrieveAndGenerate" => handle_retrieve_and_generate(self, &req, &body).await,
            "RetrieveAndGenerateStream" => {
                handle_retrieve_and_generate_stream(self, &req, &body).await
            }
            "CreateSession" => handle_create_session(self, &req, &body).await,
            "DeleteSession" => handle_delete_session(self, &req, &body).await,
            "EndSession" => handle_end_session(self, &req, &body).await,
            "GetSession" => handle_get_session(self, &req, &body).await,
            "ListSessions" => handle_list_sessions(self, &req, &body).await,
            "UpdateSession" => handle_update_session(self, &req, &body).await,
            "CreateInvocation" => handle_create_invocation(self, &req, &body).await,
            "GetInvocationStep" => handle_get_invocation_step(self, &req, &body).await,
            "ListInvocationSteps" => handle_list_invocation_steps(self, &req, &body).await,
            "ListInvocations" => handle_list_invocations(self, &req, &body).await,
            "PutInvocationStep" => handle_put_invocation_step(self, &req, &body).await,
            "GetFlowExecution" => handle_get_flow_execution(self, &req, &body).await,
            "ListFlowExecutionEvents" => handle_list_flow_execution_events(self, &req, &body).await,
            "ListFlowExecutions" => handle_list_flow_executions(self, &req, &body).await,
            "StartFlowExecution" => handle_start_flow_execution(self, &req, &body).await,
            "StopFlowExecution" => handle_stop_flow_execution(self, &req, &body).await,
            "GetExecutionFlowSnapshot" => {
                handle_get_execution_flow_snapshot(self, &req, &body).await
            }
            "GenerateQuery" => handle_generate_query(self, &req, &body).await,
            "Rerank" => handle_rerank(self, &req, &body).await,
            "DeleteAgentMemory" => handle_delete_agent_memory(self, &req, &body).await,
            "GetAgentMemory" => handle_get_agent_memory(self, &req, &body).await,
            "TagResource" => handle_tag_resource(self, &req, &body).await,
            "UntagResource" => handle_untag_resource(self, &req, &body).await,
            "ListTagsForResource" => handle_list_tags_for_resource(self, &req, &body).await,
            _ => Err(validation(&format!("Unknown action: {}", action))),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

/// Persist the current Bedrock Agent Runtime state as a snapshot. Offloads the
/// serde + blocking file write to the Tokio blocking pool. Noop when `store` is
/// `None` (memory mode).
async fn save_bedrock_agent_runtime_snapshot(
    state: &SharedBedrockAgentRuntimeState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = BedrockAgentRuntimeSnapshot {
        schema_version: BEDROCK_AGENT_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().persisted_copy()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write bedrock-agent-runtime snapshot"),
        Err(err) => tracing::error!(%err, "bedrock-agent-runtime snapshot task panicked"),
    }
}

// ── Invoke handlers ──────────────────────────────────────────────────

async fn handle_invoke_agent(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let agent_id = req_str(body, "agentId");
    let agent_alias_id = req_str(body, "agentAliasId");
    let session_id = req_str(body, "sessionId");

    validate_str(
        agent_id.as_deref(),
        "agentId",
        Some(re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    validate_str(
        agent_alias_id.as_deref(),
        "agentAliasId",
        Some(re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    validate_str(
        session_id.as_deref(),
        "sessionId",
        Some(re_session_id()),
        Some(2),
        Some(100),
        true,
    )?;
    let memory_id_opt = req_str(body, "memoryId");
    if let Some(ref m) = memory_id_opt {
        validate_str(
            Some(m),
            "memoryId",
            Some(re_memory_id()),
            Some(2),
            Some(100),
            false,
        )?;
    }
    // sourceArn is httpHeader; AWSResourceARN length max 2048
    let source_arn_opt = req
        .headers
        .get("x-amz-source-arn")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .or_else(|| req_str(body, "sourceArn"));
    if let Some(ref s) = source_arn_opt {
        validate_str(
            Some(s),
            "sourceArn",
            Some(re_aws_arn()),
            Some(1),
            Some(2048),
            false,
        )?;
    }

    let agent_id = agent_id.unwrap();
    let session_id = session_id.unwrap();
    let input_text = req_str(body, "inputText").unwrap_or_default();

    let agent_name = if let Some(ref agent_state) = svc.agent_state {
        let accounts = agent_state.read();
        accounts
            .accounts
            .get(&req.account_id)
            .and_then(|account| account.agents.get(&agent_id).map(|a| a.agent_name.clone()))
    } else {
        None
    }
    .unwrap_or_else(|| "TestAgent".to_string());

    let output = format!("Hello from agent {}. You said: {}", agent_name, input_text);

    let start = std::time::Instant::now();
    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        s.invocations.push(InvocationRecord {
            invocation_id: uuid::Uuid::new_v4().to_string(),
            op: "invoke_agent".to_string(),
            agent_id: Some(agent_id.clone()),
            flow_id: None,
            session_id: Some(session_id.clone()),
            input: input_text.clone(),
            output: output.clone(),
            output_chunks: 1,
            trace: None,
            citations: Vec::new(),
            timestamp: Utc::now(),
            duration_ms: start.elapsed().as_millis() as u64,
        });
    }

    let frame = crate::eventstream::chunk_frame(&output);
    let mut headers = http::HeaderMap::new();
    if let Ok(value) = http::HeaderValue::from_str(&session_id) {
        headers.insert("x-amz-bedrock-agent-session-id", value);
    }
    headers.insert(
        "x-amzn-bedrock-agent-content-type",
        http::HeaderValue::from_static("application/json"),
    );
    Ok(AwsResponse {
        status: StatusCode::OK,
        content_type: "application/vnd.amazon.eventstream".to_string(),
        body: fakecloud_core::service::ResponseBody::Bytes(frame.into()),
        headers,
    })
}

async fn handle_invoke_flow(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let (flow_identifier, alias_identifier) = validate_flow_labels(body)?;
    if body.get("inputs").is_none() {
        return Err(validation("inputs is required"));
    }
    // executionId optional, length 2..100, pattern session-id-like
    let execution_id_opt = req_str(body, "executionId");
    if let Some(ref s) = execution_id_opt {
        validate_str(
            Some(s),
            "executionId",
            Some(re_session_id()),
            Some(2),
            Some(100),
            false,
        )?;
    }

    let flow = crate::flows::resolve_flow(
        svc.agent_state.as_ref(),
        &req.account_id,
        &flow_identifier,
        &alias_identifier,
    )?;
    let flow_id = flow.flow_id.clone();
    // A caller-supplied executionId continues that execution (a multi-turn
    // conversation); otherwise the service mints one.
    let execution_id = execution_id_opt.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let first_input = &body["inputs"][0];
    let input = match &first_input["content"]["document"] {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    };

    let start = std::time::Instant::now();
    let plan = crate::flows::RunPlan::of(
        flow.definition.as_ref(),
        first_input["nodeName"].as_str().unwrap_or_default(),
        first_input["nodeOutputName"].as_str().unwrap_or("document"),
    );
    // A flow of only input and output nodes hands its input straight through.
    let document = if plan.is_pass_through {
        input.clone()
    } else {
        format!("Flow output for input: {}", input)
    };

    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        let key = crate::flows::execution_map_key(&flow.flow_id, &flow.alias_id, &execution_id);
        // An execution id is scoped to its flow alias (the same id under
        // another alias or flow names a different execution). Only an
        // InvokeFlow execution still waiting for input could be continued;
        // InvokeFlow here runs the flow to completion (it never pauses for
        // input), so an existing execution under this id is either finished
        // (Succeeded / Failed / TimedOut / Aborted) or a StartFlowExecution
        // run, and neither can be continued. Its state is left untouched.
        if let Some(existing) = s.flow_executions.get(&key) {
            return Err(validation(&format!(
                "Flow execution {execution_id} can't be continued: its status is {}.",
                existing.status
            )));
        }
        s.flow_executions.insert(
            key,
            new_execution(req, flow, execution_id.clone(), "Succeeded"),
        );
        s.invocations.push(InvocationRecord {
            invocation_id: execution_id.clone(),
            op: "invoke_flow".to_string(),
            agent_id: None,
            flow_id: Some(flow_id.clone()),
            session_id: None,
            input: input.clone(),
            output: document.clone(),
            output_chunks: 1,
            trace: None,
            citations: Vec::new(),
            timestamp: Utc::now(),
            duration_ms: start.elapsed().as_millis() as u64,
        });
    }

    // Trace events, then outputs, then completion, as recorded from AWS.
    let mut frame = Vec::new();
    if body["enableTrace"].as_bool() == Some(true) {
        let timestamp = || Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        frame.extend(crate::eventstream::flow_trace_frame(json!({
            "nodeOutputTrace": {
                "nodeName": first_input["nodeName"],
                "timestamp": timestamp(),
                "fields": [{
                    "nodeOutputName": plan.source_output,
                    "content": { "document": input },
                    "next": plan.targets.iter().map(|target| json!({
                        "nodeName": target.node,
                        "inputFieldName": target.input,
                    })).collect::<Vec<_>>(),
                    "type": plan.source_type,
                }],
            }
        })));
        for target in &plan.targets {
            frame.extend(crate::eventstream::flow_trace_frame(json!({
                "nodeInputTrace": {
                    "nodeName": target.node,
                    "timestamp": timestamp(),
                    "fields": [{
                        "nodeInputName": target.input,
                        "content": { "document": input },
                        "source": {
                            "nodeName": first_input["nodeName"],
                            "outputFieldName": plan.source_output,
                            "expression": target.expression,
                        },
                        "type": target.data_type,
                    }],
                }
            })));
        }
    }
    for target in &plan.targets {
        frame.extend(crate::eventstream::flow_output_frame(
            &target.node,
            &document,
        ));
    }
    frame.extend(crate::eventstream::flow_completion_frame());
    let mut headers = http::HeaderMap::new();
    if let Ok(value) = http::HeaderValue::from_str(&execution_id) {
        headers.insert("x-amz-bedrock-flow-execution-id", value);
    }
    Ok(AwsResponse {
        status: StatusCode::OK,
        content_type: "application/vnd.amazon.eventstream".to_string(),
        body: fakecloud_core::service::ResponseBody::Bytes(frame.into()),
        headers,
    })
}

async fn handle_invoke_inline_agent(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_id = req_str(body, "sessionId");
    validate_str(
        session_id.as_deref(),
        "sessionId",
        Some(re_session_id()),
        Some(2),
        Some(100),
        true,
    )?;
    // foundationModel is also required in the body
    let foundation_model = req_str(body, "foundationModel");
    validate_str(
        foundation_model.as_deref(),
        "foundationModel",
        None,
        Some(1),
        Some(2048),
        true,
    )?;
    // instruction also required
    let instruction = req_str(body, "instruction");
    validate_str(
        instruction.as_deref(),
        "instruction",
        None,
        Some(40),
        Some(8000),
        true,
    )?;

    let session_id = session_id.unwrap();
    let input_text = req_str(body, "inputText").unwrap_or_default();
    let start = std::time::Instant::now();
    let output = format!("Inline agent says: {}", input_text);
    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        s.invocations.push(InvocationRecord {
            invocation_id: uuid::Uuid::new_v4().to_string(),
            op: "invoke_inline_agent".to_string(),
            agent_id: None,
            flow_id: None,
            session_id: Some(session_id.clone()),
            input: input_text.clone(),
            output: output.clone(),
            output_chunks: 1,
            trace: None,
            citations: Vec::new(),
            timestamp: Utc::now(),
            duration_ms: start.elapsed().as_millis() as u64,
        });
    }
    let frame = crate::eventstream::chunk_frame(&output);
    let mut headers = http::HeaderMap::new();
    if let Ok(value) = http::HeaderValue::from_str(&session_id) {
        headers.insert("x-amz-bedrock-agent-session-id", value);
    }
    headers.insert(
        "x-amzn-bedrock-agent-content-type",
        http::HeaderValue::from_static("application/json"),
    );
    Ok(AwsResponse {
        status: StatusCode::OK,
        content_type: "application/vnd.amazon.eventstream".to_string(),
        body: fakecloud_core::service::ResponseBody::Bytes(frame.into()),
        headers,
    })
}

async fn handle_optimize_prompt(
    _svc: &BedrockAgentRuntimeService,
    _req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    // Required: input (InputPrompt union), targetModelId
    let target_model_id = req_str(body, "targetModelId");
    validate_str(
        target_model_id.as_deref(),
        "targetModelId",
        None,
        Some(1),
        Some(2048),
        true,
    )?;
    if body.get("input").is_none() {
        return Err(validation("input is required"));
    }

    let prompt_text = body
        .get("input")
        .and_then(|i| i.get("textPrompt"))
        .and_then(|t| t.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let optimized = format!("Optimized: {}", prompt_text);

    // OptimizePromptResponse is an eventstream — payload is OptimizedPromptStream
    // union with `optimizedPromptEvent` carrying `OptimizedPrompt` union
    // `{ textPrompt: { text } }`. Send a single eventstream frame.
    let event_body = serde_json::to_vec(&json!({
        "optimizedPrompt": {
            "textPrompt": { "text": optimized }
        }
    }))
    .unwrap();
    let frame = crate::eventstream::encode_frame(
        &[
            (":event-type", "optimizedPromptEvent"),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        &event_body,
    );

    let headers = http::HeaderMap::new();
    Ok(AwsResponse {
        status: StatusCode::OK,
        content_type: "application/vnd.amazon.eventstream".to_string(),
        body: fakecloud_core::service::ResponseBody::Bytes(frame.into()),
        headers,
    })
}

async fn handle_retrieve(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let kb_id = req_str(body, "knowledgeBaseId");
    // KnowledgeBaseId pattern: ^[0-9a-zA-Z]{10}$
    validate_str(
        kb_id.as_deref(),
        "knowledgeBaseId",
        Some(re_knowledge_base_id()),
        Some(0),
        Some(10),
        true,
    )?;
    if body.get("retrievalQuery").is_none() {
        return Err(validation("retrievalQuery is required"));
    }
    validate_next_token(req, body)?;

    let kb_id = kb_id.unwrap();
    let query = body
        .get("retrievalQuery")
        .and_then(|q| q.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let start = std::time::Instant::now();
    let result_text = format!(
        "Retrieved result for query '{}' from knowledge base {}",
        query, kb_id
    );
    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        s.invocations.push(InvocationRecord {
            invocation_id: uuid::Uuid::new_v4().to_string(),
            op: "retrieve".to_string(),
            agent_id: None,
            flow_id: None,
            session_id: None,
            input: query.clone(),
            output: result_text.clone(),
            output_chunks: 1,
            trace: Some(json!({ "knowledgeBaseId": kb_id })),
            citations: Vec::new(),
            timestamp: Utc::now(),
            duration_ms: start.elapsed().as_millis() as u64,
        });
    }

    Ok(AwsResponse::ok_json(json!({
        "retrievalResults": [
            {
                "content": { "text": result_text },
                "location": {
                    "type": "S3",
                    "s3Location": {
                        "uri": format!("s3://fakecloud-kb-{}/doc1.txt", kb_id)
                    }
                },
                "score": 0.95
            }
        ]
    })))
}

async fn handle_retrieve_and_generate(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    if body.get("input").is_none() {
        return Err(validation("input is required"));
    }
    let provided_session = req_str(body, "sessionId");
    if let Some(ref s) = provided_session {
        validate_str(
            Some(s),
            "sessionId",
            Some(re_session_id()),
            Some(2),
            Some(100),
            false,
        )?;
    }
    let session_id = provided_session.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let input = body
        .get("input")
        .and_then(|i| i.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let start = std::time::Instant::now();
    let output_text = format!("Generated response for: {}", input);
    let citation = rag_citation(&output_text);
    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        s.invocations.push(InvocationRecord {
            invocation_id: uuid::Uuid::new_v4().to_string(),
            op: "retrieve_and_generate".to_string(),
            agent_id: None,
            flow_id: None,
            session_id: Some(session_id.clone()),
            input: input.clone(),
            output: output_text.clone(),
            output_chunks: 1,
            trace: None,
            citations: vec![citation.clone()],
            timestamp: Utc::now(),
            duration_ms: start.elapsed().as_millis() as u64,
        });
    }

    Ok(AwsResponse::ok_json(json!({
        "sessionId": session_id,
        "output": { "text": output_text },
        "citations": [citation]
    })))
}

/// The citation a knowledge-base answer carries: the answer's text span and the
/// reference it came from.
fn rag_citation(output_text: &str) -> Value {
    json!({
        "generatedResponsePart": {
            "textResponsePart": {
                "text": output_text,
                "span": { "start": 0, "end": output_text.chars().count().saturating_sub(1) }
            }
        },
        "retrievedReferences": [
            {
                "content": { "text": "Reference text from knowledge base" },
                "location": {
                    "type": "CONFLUENCE",
                    "confluenceLocation": { "url": "https://example.com/doc" }
                }
            }
        ]
    })
}

async fn handle_retrieve_and_generate_stream(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    if body.get("input").is_none() {
        return Err(validation("input is required"));
    }
    let provided_session = req_str(body, "sessionId");
    if let Some(ref s) = provided_session {
        validate_str(
            Some(s),
            "sessionId",
            Some(re_session_id()),
            Some(2),
            Some(100),
            false,
        )?;
    }
    let session_id = provided_session.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let input = body
        .get("input")
        .and_then(|i| i.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let output_text = format!("Generated response for: {}", input);
    let citation = rag_citation(&output_text);
    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        s.invocations.push(InvocationRecord {
            invocation_id: uuid::Uuid::new_v4().to_string(),
            op: "retrieve_and_generate_stream".to_string(),
            agent_id: None,
            flow_id: None,
            session_id: Some(session_id.clone()),
            input: input.clone(),
            output: output_text.clone(),
            output_chunks: 1,
            trace: None,
            citations: vec![citation.clone()],
            timestamp: Utc::now(),
            duration_ms: 0,
        });
    }

    // Emit a single output event frame (eventstream payload).
    // The payload is the event's own members; `:event-type` names the event.
    let event_body = serde_json::to_vec(&json!({ "text": output_text })).unwrap();
    let event = |event_type: &str, payload: &[u8]| {
        crate::eventstream::encode_frame(
            &[
                (":event-type", event_type),
                (":content-type", "application/json"),
                (":message-type", "event"),
            ],
            payload,
        )
    };
    let mut frame = event("output", &event_body);
    frame.extend(event("citation", &serde_json::to_vec(&citation).unwrap()));
    // A configured guardrail reports what it did; this model output passes it.
    let has_guardrail = body
        .pointer("/retrieveAndGenerateConfiguration/knowledgeBaseConfiguration/generationConfiguration/guardrailConfiguration")
        .is_some();
    if has_guardrail {
        frame.extend(event(
            "guardrail",
            &serde_json::to_vec(&json!({ "action": "NONE" })).unwrap(),
        ));
    }

    let mut headers = http::HeaderMap::new();
    if let Ok(value) = http::HeaderValue::from_str(&session_id) {
        headers.insert("x-amzn-bedrock-knowledge-base-session-id", value);
    }
    Ok(AwsResponse {
        status: StatusCode::OK,
        content_type: "application/vnd.amazon.eventstream".to_string(),
        body: fakecloud_core::service::ResponseBody::Bytes(frame.into()),
        headers,
    })
}

// ── Session handlers ────────────────────────────────────────────────

async fn handle_create_session(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    // encryptionKeyArn length 1..2048 if present
    let enc_key = req_str(body, "encryptionKeyArn");
    validate_str(
        enc_key.as_deref(),
        "encryptionKeyArn",
        None,
        Some(1),
        Some(2048),
        false,
    )?;

    let session_id = uuid::Uuid::new_v4().to_string();
    // Force into UUID format
    let now = Utc::now();
    let arn = session_arn(&req.region, &req.account_id, &session_id);

    let metadata: std::collections::BTreeMap<String, String> = body
        .get("sessionMetadata")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();
    let tags: std::collections::BTreeMap<String, String> = body
        .get("tags")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        if !tags.is_empty() {
            s.tags.insert(arn.clone(), tags);
        }
        s.sessions.insert(
            session_id.clone(),
            Session {
                session_id: session_id.clone(),
                session_arn: arn.clone(),
                status: "ACTIVE".to_string(),
                created_at: now,
                updated_at: now,
                metadata,
                encryption_key_arn: enc_key,
            },
        );
    }

    Ok(AwsResponse::ok_json(json!({
        "sessionId": session_id,
        "sessionArn": arn,
        "sessionStatus": "ACTIVE",
        "createdAt": now.to_rfc3339(),
    })))
}

fn resolve_session_id<'a>(
    state: &'a crate::state::BedrockAgentRuntimeState,
    ident: &str,
) -> Option<&'a Session> {
    // identifier may be session ARN or UUID
    if let Some(s) = state.sessions.get(ident) {
        return Some(s);
    }
    // try to find by ARN
    state.sessions.values().find(|s| s.session_arn == ident)
}

async fn handle_delete_session(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    let ident = session_ident.unwrap();
    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    // Find key
    let key = if s.sessions.contains_key(&ident) {
        Some(ident.clone())
    } else {
        s.sessions
            .iter()
            .find(|(_, v)| v.session_arn == ident)
            .map(|(k, _)| k.clone())
    };
    if let Some(k) = key {
        // The session's invocations, their steps and its tags go with it.
        if let Some(session) = s.sessions.remove(&k) {
            s.tags.remove(&session.session_arn);
        }
        s.session_invocations.remove(&k);
        s.invocation_steps.retain(|_, step| step.session_id != k);
    } else {
        return Err(make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Session not found",
        ));
    }
    Ok(AwsResponse::ok_json(json!({})))
}

async fn handle_end_session(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    let ident = session_ident.unwrap();
    let now = Utc::now();
    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    let key = if s.sessions.contains_key(&ident) {
        Some(ident.clone())
    } else {
        s.sessions
            .iter()
            .find(|(_, v)| v.session_arn == ident)
            .map(|(k, _)| k.clone())
    };
    let key = key.ok_or_else(|| {
        make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Session not found",
        )
    })?;
    let session = s.sessions.get_mut(&key).unwrap();
    session.status = "ENDED".to_string();
    session.updated_at = now;
    let (sid, arn, status) = (
        session.session_id.clone(),
        session.session_arn.clone(),
        session.status.clone(),
    );
    Ok(AwsResponse::ok_json(json!({
        "sessionId": sid,
        "sessionArn": arn,
        "sessionStatus": status,
    })))
}

async fn handle_get_session(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    let ident = session_ident.unwrap();

    let accts = svc.state.read();
    let s = accts.accounts.get(&req.account_id).ok_or_else(|| {
        make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Session not found",
        )
    })?;
    let session = resolve_session_id(s, &ident).ok_or_else(|| {
        make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Session not found",
        )
    })?;

    Ok(AwsResponse::ok_json(json!({
        "sessionId": session.session_id,
        "sessionArn": session.session_arn,
        "sessionStatus": session.status,
        "createdAt": session.created_at.to_rfc3339(),
        "lastUpdatedAt": session.updated_at.to_rfc3339(),
        "sessionMetadata": session.metadata.clone(),
        "encryptionKeyArn": session.encryption_key_arn,
    })))
}

async fn handle_list_sessions(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    validate_int_range(extract_int(req, body, "maxResults"), "maxResults", 1, 1000)?;
    validate_next_token(req, body)?;
    let accts = svc.state.read();
    let summaries: Vec<Value> = accts
        .accounts
        .get(&req.account_id)
        .map(|state| {
            state
                .sessions
                .values()
                .map(|sess| {
                    json!({
                        "sessionId": sess.session_id,
                        "sessionArn": sess.session_arn,
                        "sessionStatus": sess.status,
                        "createdAt": sess.created_at.to_rfc3339(),
                        "lastUpdatedAt": sess.updated_at.to_rfc3339(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(AwsResponse::ok_json(
        json!({ "sessionSummaries": summaries }),
    ))
}

async fn handle_update_session(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    let ident = session_ident.unwrap();

    let now = Utc::now();
    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    let key = if s.sessions.contains_key(&ident) {
        Some(ident.clone())
    } else {
        s.sessions
            .iter()
            .find(|(_, v)| v.session_arn == ident)
            .map(|(k, _)| k.clone())
    };
    let key = key.ok_or_else(|| {
        make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Session not found",
        )
    })?;
    let session = s.sessions.get_mut(&key).unwrap();
    if let Some(meta) = body.get("sessionMetadata").and_then(|v| v.as_object()) {
        session.metadata = meta
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect();
    }
    session.updated_at = now;

    Ok(AwsResponse::ok_json(json!({
        "sessionId": session.session_id,
        "sessionArn": session.session_arn,
        "sessionStatus": session.status,
        "createdAt": session.created_at.to_rfc3339(),
        "lastUpdatedAt": session.updated_at.to_rfc3339(),
    })))
}

// ── Invocation handlers ─────────────────────────────────────────────

async fn handle_create_invocation(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    // description optional with min=1, max=200
    let description = req_str(body, "description");
    validate_str(
        description.as_deref(),
        "description",
        None,
        Some(1),
        Some(200),
        false,
    )?;
    // invocationId optional, but if provided must be UUID
    let invocation_id_opt = req_str(body, "invocationId");
    if let Some(ref id) = invocation_id_opt {
        if !re_uuid().is_match(id) {
            return Err(validation("invocationId must be UUID"));
        }
    }
    let ident = session_ident.unwrap();
    let invocation_id = invocation_id_opt.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let now = Utc::now();

    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    let session_key = if s.sessions.contains_key(&ident) {
        Some(ident.clone())
    } else {
        s.sessions
            .iter()
            .find(|(_, v)| v.session_arn == ident)
            .map(|(k, _)| k.clone())
    };
    let session_id = session_key.ok_or_else(|| {
        make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Session not found",
        )
    })?;

    s.session_invocations
        .entry(session_id.clone())
        .or_default()
        .push(SessionInvocation {
            invocation_id: invocation_id.clone(),
            session_id: session_id.clone(),
            description,
            created_at: now,
        });

    Ok(AwsResponse::ok_json(json!({
        "sessionId": session_id,
        "invocationId": invocation_id,
        "createdAt": now.to_rfc3339(),
    })))
}

async fn handle_get_invocation_step(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    let step_id = req_str(body, "invocationStepId");
    validate_str(
        step_id.as_deref(),
        "invocationStepId",
        Some(re_uuid()),
        None,
        None,
        true,
    )?;
    let invocation_id = req_str(body, "invocationIdentifier");
    validate_str(
        invocation_id.as_deref(),
        "invocationIdentifier",
        Some(re_uuid()),
        None,
        None,
        true,
    )?;

    let step_id = step_id.unwrap();
    let accts = svc.state.read();
    let s = accts.accounts.get(&req.account_id).ok_or_else(|| {
        make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Invocation step not found",
        )
    })?;
    let step = s.invocation_steps.get(&step_id).ok_or_else(|| {
        make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Invocation step not found",
        )
    })?;

    Ok(AwsResponse::ok_json(json!({
        "invocationStep": {
            "sessionId": step.session_id,
            "invocationId": step.invocation_id,
            "invocationStepId": step.invocation_step_id,
            "invocationStepTime": step.invocation_step_time.to_rfc3339(),
            "payload": step.payload,
        }
    })))
}

async fn handle_list_invocation_steps(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    let ident = session_ident.unwrap();
    let invocation_filter = req_str(body, "invocationIdentifier");

    let accts = svc.state.read();
    let s = match accts.accounts.get(&req.account_id) {
        Some(s) => s,
        None => {
            return Ok(AwsResponse::ok_json(json!({
                "invocationStepSummaries": []
            })));
        }
    };
    let session = resolve_session_id(s, &ident);
    let session_id = match session {
        Some(s) => s.session_id.as_str(),
        None => {
            return Ok(AwsResponse::ok_json(json!({
                "invocationStepSummaries": []
            })));
        }
    };

    let summaries: Vec<Value> = s
        .invocation_steps
        .values()
        .filter(|step| {
            step.session_id == session_id
                && invocation_filter
                    .as_ref()
                    .map(|f| step.invocation_id == *f)
                    .unwrap_or(true)
        })
        .map(|step| {
            json!({
                "sessionId": step.session_id,
                "invocationId": step.invocation_id,
                "invocationStepId": step.invocation_step_id,
                "invocationStepTime": step.invocation_step_time.to_rfc3339(),
            })
        })
        .collect();

    Ok(AwsResponse::ok_json(json!({
        "invocationStepSummaries": summaries
    })))
}

async fn handle_list_invocations(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    let ident = session_ident.unwrap();

    let accts = svc.state.read();
    let s = match accts.accounts.get(&req.account_id) {
        Some(s) => s,
        None => return Ok(AwsResponse::ok_json(json!({ "invocationSummaries": [] }))),
    };
    let session = match resolve_session_id(s, &ident) {
        Some(s) => s,
        None => return Ok(AwsResponse::ok_json(json!({ "invocationSummaries": [] }))),
    };

    let summaries: Vec<Value> = s
        .session_invocations
        .get(&session.session_id)
        .map(|list| {
            list.iter()
                .map(|inv| {
                    json!({
                        "sessionId": inv.session_id,
                        "invocationId": inv.invocation_id,
                        "createdAt": inv.created_at.to_rfc3339(),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(AwsResponse::ok_json(json!({
        "invocationSummaries": summaries
    })))
}

async fn handle_put_invocation_step(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let session_ident = req_str(body, "sessionIdentifier");
    validate_str(
        session_ident.as_deref(),
        "sessionIdentifier",
        Some(re_session_identifier()),
        None,
        None,
        true,
    )?;
    let invocation_id = req_str(body, "invocationIdentifier");
    validate_str(
        invocation_id.as_deref(),
        "invocationIdentifier",
        Some(re_uuid()),
        None,
        None,
        true,
    )?;
    if body.get("invocationStepTime").is_none() {
        return Err(validation("invocationStepTime is required"));
    }
    if body.get("payload").is_none() {
        return Err(validation("payload is required"));
    }

    let session_ident = session_ident.unwrap();
    let invocation_id = invocation_id.unwrap();
    let step_id =
        req_str(body, "invocationStepId").unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    if !re_uuid().is_match(&step_id) {
        return Err(validation("invocationStepId must be UUID"));
    }
    let step_time = body
        .get("invocationStepTime")
        .cloned()
        .unwrap_or(Value::String(Utc::now().to_rfc3339()));
    let parsed_time = step_time
        .as_str()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc))
        .unwrap_or_else(Utc::now);
    let payload = body.get("payload").cloned().unwrap_or(Value::Null);

    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    let session_id = if s.sessions.contains_key(&session_ident) {
        session_ident.clone()
    } else {
        s.sessions
            .iter()
            .find(|(_, v)| v.session_arn == session_ident)
            .map(|(k, _)| k.clone())
            .ok_or_else(|| {
                make_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    "Session not found",
                )
            })?
    };

    s.invocation_steps.insert(
        step_id.clone(),
        InvocationStep {
            session_id: session_id.clone(),
            invocation_id: invocation_id.clone(),
            invocation_step_id: step_id.clone(),
            invocation_step_time: parsed_time,
            payload,
        },
    );

    Ok(AwsResponse::ok_json(json!({ "invocationStepId": step_id })))
}

// ── Flow execution handlers ─────────────────────────────────────────

/// Validate the `flowIdentifier` / `flowAliasIdentifier` path labels every
/// flow-execution operation carries, returning them.
fn validate_flow_labels(body: &Value) -> Result<(String, String), AwsServiceError> {
    let flow_id = req_str(body, "flowIdentifier");
    let flow_alias_id = req_str(body, "flowAliasIdentifier");
    validate_str(
        flow_id.as_deref(),
        "flowIdentifier",
        Some(re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        flow_alias_id.as_deref(),
        "flowAliasIdentifier",
        Some(re_flow_alias_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    Ok((
        flow_id.unwrap_or_default(),
        flow_alias_id.unwrap_or_default(),
    ))
}

/// Validate the flow labels plus the `executionIdentifier` label of an
/// operation on one execution, returning all three.
fn validate_execution_labels(body: &Value) -> Result<(String, String, String), AwsServiceError> {
    let (flow_id, alias_id) = validate_flow_labels(body)?;
    let exec_id = req_str(body, "executionIdentifier");
    validate_str(
        exec_id.as_deref(),
        "executionIdentifier",
        Some(re_flow_execution_id()),
        None,
        Some(2048),
        true,
    )?;
    Ok((flow_id, alias_id, exec_id.unwrap_or_default()))
}

fn execution_not_found(exec_id: &str) -> AwsServiceError {
    crate::flows::not_found(format!("Flow execution {exec_id} not found."))
}

/// The execution an operation's labels name: `executionIdentifier` (an id or
/// execution ARN) under the labelled flow AND alias, in the caller's account.
/// Looked up directly by its map key.
fn find_execution<'a>(
    state: Option<&'a crate::state::BedrockAgentRuntimeState>,
    account_id: &str,
    flow_identifier: &str,
    alias_identifier: &str,
    exec_id: &str,
) -> Result<&'a FlowExecution, AwsServiceError> {
    let key =
        crate::flows::execution_key_for(account_id, flow_identifier, alias_identifier, exec_id)
            .ok_or_else(|| execution_not_found(exec_id))?;
    state
        .and_then(|s| s.flow_executions.get(&key))
        .ok_or_else(|| execution_not_found(exec_id))
}

/// Record a new execution of `flow`, capturing what it runs.
fn new_execution(
    req: &AwsRequest,
    flow: crate::flows::ResolvedFlow,
    execution_id: String,
    status: &str,
) -> FlowExecution {
    let now = Utc::now();
    FlowExecution {
        execution_arn: flow_execution_arn(
            &req.region,
            &req.account_id,
            &flow.flow_id,
            &flow.alias_id,
            &execution_id,
        ),
        execution_id,
        flow_id: flow.flow_id,
        flow_alias_id: flow.alias_id,
        flow_version: flow.version,
        status: status.to_string(),
        created_at: now,
        updated_at: now,
        ended_at: (status != "Running").then_some(now),
        definition: flow.definition,
        execution_role_arn: flow.execution_role_arn,
        customer_encryption_key_arn: flow.customer_encryption_key_arn,
    }
}

async fn handle_get_flow_execution(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let (flow_id, alias_id, exec_id) = validate_execution_labels(body)?;

    let accts = svc.state.read();
    let exec = find_execution(
        accts.accounts.get(&req.account_id),
        &req.account_id,
        &flow_id,
        &alias_id,
        &exec_id,
    )?;

    let mut out = json!({
        "executionArn": exec.execution_arn,
        "flowIdentifier": exec.flow_id,
        "flowAliasIdentifier": exec.flow_alias_id,
        "flowVersion": exec.flow_version,
        "status": exec.status,
        "startedAt": exec.created_at.to_rfc3339(),
    });
    if let Some(ended) = exec.ended_at {
        out["endedAt"] = json!(ended.to_rfc3339());
    }
    Ok(AwsResponse::ok_json(out))
}

async fn handle_list_flow_execution_events(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let (flow_id, alias_id, exec_id) = validate_execution_labels(body)?;
    // eventType is required (httpQuery)
    let event_type = req
        .query_params
        .get("eventType")
        .cloned()
        .or_else(|| req_str(body, "eventType"));
    let event_type = event_type.ok_or_else(|| validation("eventType is required"))?;
    if event_type != "Node" && event_type != "Flow" {
        return Err(validation("eventType must be Node or Flow"));
    }
    validate_int_range(extract_int(req, body, "maxResults"), "maxResults", 1, 1000)?;
    validate_next_token(req, body)?;

    let accts = svc.state.read();
    find_execution(
        accts.accounts.get(&req.account_id),
        &req.account_id,
        &flow_id,
        &alias_id,
        &exec_id,
    )?;

    // No node ran any work, so the execution has recorded no events.
    Ok(AwsResponse::ok_json(json!({
        "flowExecutionEvents": []
    })))
}

async fn handle_list_flow_executions(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let flow_id = req_str(body, "flowIdentifier");
    validate_str(
        flow_id.as_deref(),
        "flowIdentifier",
        Some(re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    let alias = req.query_params.get("flowAliasIdentifier").cloned();
    if let Some(ref a) = alias {
        validate_str(
            Some(a),
            "flowAliasIdentifier",
            Some(re_flow_alias_identifier()),
            Some(1),
            Some(2048),
            false,
        )?;
    }
    validate_int_range(extract_int(req, body, "maxResults"), "maxResults", 1, 1000)?;
    validate_next_token(req, body)?;
    let flow_identifier = flow_id.unwrap();
    // The flow (and optional alias filter) the listing is scoped to; an ARN in
    // another account, or an alias ARN of another flow, matches nothing.
    let scope = match &alias {
        Some(a) => crate::flows::flow_and_alias(&req.account_id, &flow_identifier, a)
            .map(|(f, a)| (f, Some(a))),
        None => crate::flows::flow_and_alias(
            &req.account_id,
            &flow_identifier,
            crate::flows::TEST_ALIAS_ID,
        )
        .map(|(f, _)| (f, None)),
    };

    let accts = svc.state.read();
    let summaries: Vec<Value> = match (scope, accts.accounts.get(&req.account_id)) {
        (Some((flow_id, alias_id)), Some(state)) => state
            .flow_executions
            .values()
            .filter(|e| e.flow_id == flow_id)
            .filter(|e| alias_id.as_deref().is_none_or(|a| e.flow_alias_id == a))
            .map(|e| {
                let mut o = json!({
                    "executionArn": e.execution_arn,
                    "flowIdentifier": e.flow_id,
                    "flowAliasIdentifier": e.flow_alias_id,
                    "flowVersion": e.flow_version,
                    "status": e.status,
                    "createdAt": e.created_at.to_rfc3339(),
                });
                if let Some(ended) = e.ended_at {
                    o["endedAt"] = json!(ended.to_rfc3339());
                }
                o
            })
            .collect(),
        _ => Vec::new(),
    };

    Ok(AwsResponse::ok_json(json!({
        "flowExecutionSummaries": summaries
    })))
}

async fn handle_start_flow_execution(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let (flow_identifier, alias_identifier) = validate_flow_labels(body)?;
    if body.get("inputs").is_none() {
        return Err(validation("inputs is required"));
    }
    let exec_name = req_str(body, "flowExecutionName");
    if let Some(ref n) = exec_name {
        validate_str(
            Some(n),
            "flowExecutionName",
            Some(re_flow_execution_name()),
            Some(1),
            Some(36),
            false,
        )?;
    }

    let flow = crate::flows::resolve_flow(
        svc.agent_state.as_ref(),
        &req.account_id,
        &flow_identifier,
        &alias_identifier,
    )?;
    // The execution's name is its id (the last segment of its ARN); without
    // one the service generates it.
    let execution_id = exec_name.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    let key = crate::flows::execution_map_key(&flow.flow_id, &flow.alias_id, &execution_id);

    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    if s.flow_executions.contains_key(&key) {
        return Err(make_error(
            StatusCode::CONFLICT,
            "ConflictException",
            &format!(
                "A flow execution named {execution_id} already exists for flow {} alias {}.",
                flow.flow_id, flow.alias_id
            ),
        ));
    }
    let execution = new_execution(req, flow, execution_id, "Running");
    let arn = execution.execution_arn.clone();
    s.flow_executions.insert(key, execution);

    Ok(AwsResponse::ok_json(json!({ "executionArn": arn })))
}

async fn handle_stop_flow_execution(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let (flow_id, alias_id, exec_id) = validate_execution_labels(body)?;
    let key = crate::flows::execution_key_for(&req.account_id, &flow_id, &alias_id, &exec_id)
        .ok_or_else(|| execution_not_found(&exec_id))?;

    let mut accts = svc.state.write();
    let e = accts
        .accounts
        .get_mut(&req.account_id)
        .and_then(|s| s.flow_executions.get_mut(&key))
        .ok_or_else(|| execution_not_found(&exec_id))?;
    // Only a running execution is aborted; one that already ended keeps (and
    // reports) the status it ended with.
    if e.status == "Running" {
        let now = Utc::now();
        e.status = "Aborted".to_string();
        e.updated_at = now;
        e.ended_at = Some(now);
    }

    Ok(AwsResponse::ok_json(json!({
        "executionArn": e.execution_arn,
        "status": e.status,
    })))
}

async fn handle_get_execution_flow_snapshot(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let (flow_id, alias_id, exec_id) = validate_execution_labels(body)?;

    let accts = svc.state.read();
    let exec = find_execution(
        accts.accounts.get(&req.account_id),
        &req.account_id,
        &flow_id,
        &alias_id,
        &exec_id,
    )?;

    // The definition the execution ran, captured at start, as the JSON
    // document string the model's `definition` member carries.
    let definition = exec
        .definition
        .as_ref()
        .map(Value::to_string)
        .unwrap_or_else(|| "{}".to_string());
    let mut out = json!({
        "flowIdentifier": exec.flow_id,
        "flowAliasIdentifier": exec.flow_alias_id,
        "flowVersion": exec.flow_version,
        "executionRoleArn": exec.execution_role_arn.clone().unwrap_or_default(),
        "definition": definition,
    });
    if let Some(key) = &exec.customer_encryption_key_arn {
        out["customerEncryptionKeyArn"] = json!(key);
    }
    Ok(AwsResponse::ok_json(out))
}

// ── Other handlers ──────────────────────────────────────────────────

async fn handle_generate_query(
    _svc: &BedrockAgentRuntimeService,
    _req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    if body.get("queryGenerationInput").is_none() {
        return Err(validation("queryGenerationInput is required"));
    }
    if body.get("transformationConfiguration").is_none() {
        return Err(validation("transformationConfiguration is required"));
    }
    let input_text = body
        .get("queryGenerationInput")
        .and_then(|i| i.get("text"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Ok(AwsResponse::ok_json(json!({
        "queries": [
            {
                "type": "REDSHIFT_SQL",
                "sql": format!("SELECT 1 -- {}", input_text),
            }
        ]
    })))
}

async fn handle_rerank(
    _svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    if body.get("queries").is_none() {
        return Err(validation("queries is required"));
    }
    if body.get("sources").is_none() {
        return Err(validation("sources is required"));
    }
    if body.get("rerankingConfiguration").is_none() {
        return Err(validation("rerankingConfiguration is required"));
    }
    validate_next_token(req, body)?;
    let sources = body
        .get("sources")
        .and_then(|s| s.as_array())
        .cloned()
        .unwrap_or_default();

    let results: Vec<Value> = sources
        .iter()
        .enumerate()
        .map(|(i, _)| {
            json!({
                "index": i,
                "relevanceScore": (0.9_f64 - (i as f64 * 0.1)).max(0.0),
            })
        })
        .collect();

    Ok(AwsResponse::ok_json(json!({ "results": results })))
}

async fn handle_delete_agent_memory(
    _svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let agent_id = req_str(body, "agentId");
    let agent_alias_id = req_str(body, "agentAliasId");
    validate_str(
        agent_id.as_deref(),
        "agentId",
        Some(re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    validate_str(
        agent_alias_id.as_deref(),
        "agentAliasId",
        Some(re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    let memory_id = req
        .query_params
        .get("memoryId")
        .cloned()
        .or_else(|| req_str(body, "memoryId"));
    let session_id = req
        .query_params
        .get("sessionId")
        .cloned()
        .or_else(|| req_str(body, "sessionId"));
    if let Some(ref m) = memory_id {
        validate_str(
            Some(m),
            "memoryId",
            Some(re_memory_id()),
            Some(2),
            Some(100),
            false,
        )?;
    }
    if let Some(ref s) = session_id {
        validate_str(
            Some(s),
            "sessionId",
            Some(re_session_id()),
            Some(2),
            Some(100),
            false,
        )?;
    }

    Ok(AwsResponse::ok_json(json!({})))
}

async fn handle_get_agent_memory(
    _svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let agent_id = req_str(body, "agentId");
    let agent_alias_id = req_str(body, "agentAliasId");
    validate_str(
        agent_id.as_deref(),
        "agentId",
        Some(re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    validate_str(
        agent_alias_id.as_deref(),
        "agentAliasId",
        Some(re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    let memory_type = req.query_params.get("memoryType").cloned();
    let memory_type = memory_type.ok_or_else(|| validation("memoryType is required"))?;
    if memory_type != "SESSION_SUMMARY" {
        return Err(validation("memoryType must be SESSION_SUMMARY"));
    }
    validate_int_range(extract_int(req, body, "maxItems"), "maxItems", 1, 1000)?;
    validate_next_token(req, body)?;
    let memory_id = req.query_params.get("memoryId").cloned();
    if memory_id.is_none() {
        return Err(validation("memoryId is required"));
    }
    let memory_id = memory_id.unwrap();
    validate_str(
        Some(&memory_id),
        "memoryId",
        Some(re_memory_id()),
        Some(2),
        Some(100),
        true,
    )?;

    Ok(AwsResponse::ok_json(json!({
        "memoryContents": []
    })))
}

// ── Tagging handlers ────────────────────────────────────────────────

async fn handle_tag_resource(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let resource_arn = req_str(body, "resourceArn");
    validate_str(
        resource_arn.as_deref(),
        "resourceArn",
        Some(re_taggable_arn()),
        Some(1),
        Some(1011),
        true,
    )?;
    let tags = body
        .get("tags")
        .and_then(|t| t.as_object())
        .cloned()
        .ok_or_else(|| validation("tags is required"))?;

    let arn = resource_arn.unwrap();
    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    let entry = s.tags.entry(arn).or_default();
    for (k, v) in tags {
        if let Some(s) = v.as_str() {
            entry.insert(k, s.to_string());
        }
    }

    Ok(AwsResponse::ok_json(json!({})))
}

async fn handle_untag_resource(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let resource_arn = req_str(body, "resourceArn");
    validate_str(
        resource_arn.as_deref(),
        "resourceArn",
        Some(re_taggable_arn()),
        Some(1),
        Some(1011),
        true,
    )?;
    // `tagKeys` is an `@httpQuery` list sent as repeated `tagKeys=a&tagKeys=b`
    // pairs; `query_params` collapses repeats to the last value, so parse every
    // occurrence out of the raw query string, percent-decoding each. Fall back
    // to a JSON body (list or comma-joined string) for clients that send it
    // there.
    let query_present = req
        .raw_query
        .split('&')
        .any(|pair| pair == "tagKeys" || pair.starts_with("tagKeys="));
    let mut keys: Vec<String> = req
        .raw_query
        .split('&')
        .filter_map(|pair| pair.strip_prefix("tagKeys="))
        .map(|v| {
            percent_encoding::percent_decode_str(v)
                .decode_utf8_lossy()
                .into_owned()
        })
        .filter(|s| !s.is_empty())
        .collect();
    let body_present = body.get("tagKeys").is_some();
    if !query_present {
        if let Some(v) = body.get("tagKeys") {
            if let Some(arr) = v.as_array() {
                keys = arr
                    .iter()
                    .filter_map(|x| x.as_str().map(str::to_string))
                    .filter(|s| !s.is_empty())
                    .collect();
            } else if let Some(s) = v.as_str() {
                keys = s
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect();
            }
        }
    }
    if !query_present && !body_present {
        return Err(validation("tagKeys is required"));
    }

    let arn = resource_arn.unwrap();
    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    if let Some(entry) = s.tags.get_mut(&arn) {
        for k in &keys {
            entry.remove(k);
        }
    }

    Ok(AwsResponse::ok_json(json!({})))
}

async fn handle_list_tags_for_resource(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let resource_arn = req_str(body, "resourceArn");
    validate_str(
        resource_arn.as_deref(),
        "resourceArn",
        Some(re_taggable_arn()),
        Some(1),
        Some(1011),
        true,
    )?;
    let arn = resource_arn.unwrap();
    let accts = svc.state.read();
    let tags: serde_json::Map<String, Value> = accts
        .accounts
        .get(&req.account_id)
        .and_then(|s| s.tags.get(&arn))
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect()
        })
        .unwrap_or_default();

    Ok(AwsResponse::ok_json(json!({ "tags": tags })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::BedrockAgentRuntimeAccounts;
    use http::HeaderMap;
    use parking_lot::RwLock;
    use std::collections::HashMap;

    fn cn_request() -> AwsRequest {
        AwsRequest {
            service: "bedrock-agent-runtime".to_string(),
            action: String::new(),
            region: "cn-north-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-id".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Default::default(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: String::new(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[tokio::test]
    async fn china_region_session_arn_round_trips() {
        let svc = BedrockAgentRuntimeService::new(Arc::new(RwLock::new(
            BedrockAgentRuntimeAccounts::new(),
        )));
        let req = cn_request();
        let created = body(handle_create_session(&svc, &req, &json!({})).await.unwrap());
        let arn = created["sessionArn"].as_str().unwrap();
        assert!(
            arn.starts_with("arn:aws-cn:bedrock:cn-north-1:123456789012:session/"),
            "{arn}"
        );

        let got = body(
            handle_get_session(&svc, &req, &json!({"sessionIdentifier": arn}))
                .await
                .unwrap(),
        );
        assert_eq!(got["sessionId"], created["sessionId"]);
    }

    const ACCT: &str = "123456789012";
    const FLOW: &str = "ABCDEFGHIJ";
    const OTHER_FLOW: &str = "ZYXWVUTSRQ";
    const ALIAS_V1: &str = "ALIASV1AAA";
    const OTHER_ALIAS: &str = "ALIASZZZZZ";
    const DRAFT_ROLE: &str = "arn:aws-cn:iam::123456789012:role/service-role/draft-role";
    const V1_ROLE: &str = "arn:aws-cn:iam::123456789012:role/service-role/v1-role";
    const V1_KEY: &str =
        "arn:aws-cn:kms:cn-north-1:123456789012:key/11111111-2222-3333-4444-555555555555";

    fn flow(id: &str, definition: Value, role: &str) -> fakecloud_bedrock_agent::Flow {
        let now = Utc::now();
        fakecloud_bedrock_agent::Flow {
            flow_id: id.to_string(),
            name: format!("flow-{id}"),
            description: None,
            execution_role_arn: Some(role.to_string()),
            status: "Prepared".to_string(),
            created_at: now,
            updated_at: now,
            version: "DRAFT".to_string(),
            definition: Some(definition),
            arn: format!("arn:aws-cn:bedrock:cn-north-1:{ACCT}:flow/{id}"),
            customer_encryption_key_arn: None,
            latest_version: 1,
        }
    }

    fn alias(id: &str, flow_id: &str, version: &str) -> fakecloud_bedrock_agent::FlowAlias {
        let now = Utc::now();
        fakecloud_bedrock_agent::FlowAlias {
            alias_id: id.to_string(),
            alias_name: id.to_lowercase(),
            flow_id: flow_id.to_string(),
            routing_configuration: vec![json!({ "flowVersion": version })],
            description: None,
            created_at: now,
            updated_at: now,
            concurrency_configuration: None,
        }
    }

    fn draft_definition() -> Value {
        json!({"nodes": [{"name": "Draft", "type": "Input"}], "connections": []})
    }

    fn v1_definition() -> Value {
        json!({"nodes": [{"name": "V1", "type": "Input"}], "connections": []})
    }

    /// A runtime wired to Bedrock Agents state holding FLOW (draft + version 1,
    /// routed by ALIAS_V1) and OTHER_FLOW (whose alias is OTHER_ALIAS).
    fn flow_svc() -> (
        BedrockAgentRuntimeService,
        fakecloud_bedrock_agent::SharedBedrockAgentState,
    ) {
        let mut accounts = fakecloud_bedrock_agent::BedrockAgentAccounts::new();
        let st = accounts.get_or_create(ACCT, "cn-north-1");
        st.flows
            .insert(FLOW.into(), flow(FLOW, draft_definition(), DRAFT_ROLE));
        st.flows
            .insert(OTHER_FLOW.into(), flow(OTHER_FLOW, json!({}), DRAFT_ROLE));
        let now = Utc::now();
        st.flow_versions.insert(
            FLOW.into(),
            vec![fakecloud_bedrock_agent::FlowVersion {
                flow_version: "1".into(),
                flow_id: FLOW.into(),
                description: None,
                created_at: now,
                updated_at: now,
                definition: Some(v1_definition()),
                name: Some(format!("flow-{FLOW}")),
                execution_role_arn: Some(V1_ROLE.into()),
                customer_encryption_key_arn: Some(V1_KEY.into()),
                status: Some("Prepared".into()),
            }],
        );
        st.flow_aliases
            .insert(ALIAS_V1.into(), alias(ALIAS_V1, FLOW, "1"));
        st.flow_aliases
            .insert(OTHER_ALIAS.into(), alias(OTHER_ALIAS, OTHER_FLOW, "DRAFT"));
        let agent_state = Arc::new(RwLock::new(accounts));
        let svc = BedrockAgentRuntimeService::new(Arc::new(RwLock::new(
            BedrockAgentRuntimeAccounts::new(),
        )))
        .with_agent_state(agent_state.clone());
        (svc, agent_state)
    }

    async fn start(svc: &BedrockAgentRuntimeService, flow: &str, alias: &str) -> String {
        body(
            handle_start_flow_execution(
                svc,
                &cn_request(),
                &json!({"flowIdentifier": flow, "flowAliasIdentifier": alias, "inputs": []}),
            )
            .await
            .unwrap(),
        )["executionArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn code(err: AwsServiceError) -> String {
        err.code().to_string()
    }

    fn exec_body(flow: &str, alias: &str, exec: &str) -> Value {
        json!({"flowIdentifier": flow, "flowAliasIdentifier": alias, "executionIdentifier": exec})
    }

    /// Captures every snapshot the service writes.
    #[derive(Default)]
    struct CapturingStore {
        saves: parking_lot::Mutex<Vec<Vec<u8>>>,
    }

    impl SnapshotStore for CapturingStore {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.saves.lock().last().cloned())
        }

        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            self.saves.lock().push(bytes.to_vec());
            Ok(())
        }
    }

    fn routed(method: Method, segs: &[&str], body: Value) -> AwsRequest {
        let mut req = cn_request();
        req.method = method;
        req.path_segments = segs.iter().map(|s| s.to_string()).collect();
        req.raw_path = format!("/{}", segs.join("/"));
        req.body = serde_json::to_vec(&body).unwrap().into();
        req
    }

    #[tokio::test]
    async fn mutations_write_a_snapshot_and_reads_do_not() {
        let store = Arc::new(CapturingStore::default());
        let (svc, _) = flow_svc();
        let svc = svc.with_snapshot_store(store.clone());

        let created = body(
            svc.handle(routed(Method::PUT, &["sessions"], json!({})))
                .await
                .unwrap(),
        );
        let session_id = created["sessionId"].as_str().unwrap().to_string();
        assert_eq!(store.saves.lock().len(), 1, "CreateSession persists");

        svc.handle(routed(Method::GET, &["sessions", &session_id], json!({})))
            .await
            .unwrap();
        assert_eq!(store.saves.lock().len(), 1, "GetSession does not persist");

        // A failed mutation writes nothing.
        assert!(svc
            .handle(routed(
                Method::GET,
                &["sessions", "missing-session"],
                json!({})
            ))
            .await
            .is_err());
        assert!(svc
            .handle(routed(
                Method::DELETE,
                &["sessions", "missing-session"],
                json!({})
            ))
            .await
            .is_err());
        assert_eq!(store.saves.lock().len(), 1);

        // InvokeFlow records a (completed) flow execution, so it persists.
        svc.handle(routed(
            Method::POST,
            &["flows", FLOW, "aliases", ALIAS_V1],
            json!({"inputs": [{"nodeName": "Input", "nodeOutputName": "document", "content": {"document": "hi"}}]}),
        ))
        .await
        .unwrap();
        assert_eq!(store.saves.lock().len(), 2, "InvokeFlow persists");

        let arn = start(&svc, FLOW, ALIAS_V1).await;
        // `start` calls the handler directly; persist via a routed StopFlowExecution.
        let exec_id = arn.rsplit('/').next().unwrap().to_string();
        svc.handle(routed(
            Method::POST,
            &[
                "flows",
                FLOW,
                "aliases",
                ALIAS_V1,
                "executions",
                &exec_id,
                "stop",
            ],
            json!({}),
        ))
        .await
        .unwrap();
        assert_eq!(store.saves.lock().len(), 3, "StopFlowExecution persists");

        let last = store.saves.lock().last().cloned().unwrap();
        let snap: BedrockAgentRuntimeSnapshot = serde_json::from_slice(&last).unwrap();
        assert_eq!(
            snap.schema_version,
            BEDROCK_AGENT_RUNTIME_SNAPSHOT_SCHEMA_VERSION
        );
        let st = &snap.accounts.unwrap().accounts[ACCT];
        assert!(st.sessions.contains_key(&session_id));
        assert_eq!(st.flow_executions.len(), 2);
        assert!(st
            .flow_executions
            .values()
            .any(|e| e.execution_id == exec_id && e.status == "Aborted"));
        // The introspection invocation log is never written to disk.
        assert!(st.invocations.is_empty());
        assert!(!svc.state.read().accounts[ACCT].invocations.is_empty());
    }

    #[tokio::test]
    async fn create_session_tags_and_delete_session_drops_its_children() {
        let svc = BedrockAgentRuntimeService::new(Arc::new(RwLock::new(
            BedrockAgentRuntimeAccounts::new(),
        )));
        let req = cn_request();
        let created = body(
            handle_create_session(&svc, &req, &json!({"tags": {"team": "flows"}}))
                .await
                .unwrap(),
        );
        let session_id = created["sessionId"].as_str().unwrap().to_string();
        let arn = created["sessionArn"].as_str().unwrap().to_string();
        let tags = body(
            handle_list_tags_for_resource(&svc, &req, &json!({"resourceArn": arn}))
                .await
                .unwrap(),
        );
        assert_eq!(tags["tags"]["team"], "flows");

        let inv = body(
            handle_create_invocation(&svc, &req, &json!({"sessionIdentifier": session_id}))
                .await
                .unwrap(),
        );
        handle_put_invocation_step(
            &svc,
            &req,
            &json!({
                "sessionIdentifier": session_id,
                "invocationIdentifier": inv["invocationId"],
                "invocationStepTime": "2026-01-01T00:00:00Z",
                "payload": {"contentBlocks": [{"text": "hi"}]},
            }),
        )
        .await
        .unwrap();
        // Another session's step survives the delete.
        let other = body(handle_create_session(&svc, &req, &json!({})).await.unwrap());
        let other_id = other["sessionId"].as_str().unwrap().to_string();
        let other_inv = body(
            handle_create_invocation(&svc, &req, &json!({"sessionIdentifier": other_id}))
                .await
                .unwrap(),
        );
        handle_put_invocation_step(
            &svc,
            &req,
            &json!({
                "sessionIdentifier": other_id,
                "invocationIdentifier": other_inv["invocationId"],
                "invocationStepTime": "2026-01-01T00:00:00Z",
                "payload": {"contentBlocks": [{"text": "keep"}]},
            }),
        )
        .await
        .unwrap();

        handle_delete_session(&svc, &req, &json!({"sessionIdentifier": arn}))
            .await
            .unwrap();
        let accts = svc.state.read();
        let st = &accts.accounts[&req.account_id];
        assert!(!st.tags.contains_key(&arn));
        assert!(!st.session_invocations.contains_key(&session_id));
        assert!(st
            .invocation_steps
            .values()
            .all(|s| s.session_id == other_id));
        assert_eq!(st.invocation_steps.len(), 1);
        assert!(st.session_invocations.contains_key(&other_id));
    }

    #[test]
    fn introspection_only_actions_are_not_mutations() {
        for action in [
            "InvokeAgent",
            "InvokeInlineAgent",
            "Retrieve",
            "RetrieveAndGenerate",
            "RetrieveAndGenerateStream",
            "OptimizePrompt",
            "GenerateQuery",
            "Rerank",
            "DeleteAgentMemory",
            "GetSession",
            "ListFlowExecutions",
        ] {
            assert!(!is_mutating_action(action), "{action}");
        }
        for action in [
            "InvokeFlow",
            "CreateSession",
            "UpdateSession",
            "EndSession",
            "DeleteSession",
            "CreateInvocation",
            "PutInvocationStep",
            "StartFlowExecution",
            "StopFlowExecution",
            "TagResource",
            "UntagResource",
        ] {
            assert!(is_mutating_action(action), "{action}");
        }
    }

    #[test]
    fn path_labels_are_not_decoded_twice() {
        // Dispatch already decoded `path_segments`; a label whose decoded
        // value still holds `%3A` (sent as `%253A`) must reach the handler
        // verbatim, not be decoded a second time.
        let merged = merge_path_params(
            json!({}),
            &[(
                "executionIdentifier".to_string(),
                "arn:aws:bedrock:us-east-1:123456789012:flow/F%3A".to_string(),
            )],
        );
        assert_eq!(
            merged["executionIdentifier"],
            "arn:aws:bedrock:us-east-1:123456789012:flow/F%3A"
        );
    }

    #[tokio::test]
    async fn executions_require_an_existing_flow_and_alias() {
        let (svc, _) = flow_svc();
        let req = cn_request();
        for (flow, alias) in [
            ("QQQQQQQQQQ", "TSTALIASID"),
            (FLOW, "NOSUCHALIA"),
            // An alias of another flow is not an alias of this one.
            (FLOW, OTHER_ALIAS),
            // A flow ARN in another account.
            (
                "arn:aws-cn:bedrock:cn-north-1:999999999999:flow/ABCDEFGHIJ",
                "TSTALIASID",
            ),
            // An alias ARN in another account.
            (
                FLOW,
                "arn:aws-cn:bedrock:cn-north-1:999999999999:flow/ABCDEFGHIJ/alias/ALIASV1AAA",
            ),
        ] {
            let b = json!({"flowIdentifier": flow, "flowAliasIdentifier": alias, "inputs": []});
            let err = handle_start_flow_execution(&svc, &req, &b)
                .await
                .err()
                .expect("expected an error");
            assert_eq!(code(err), "ResourceNotFoundException", "{flow} {alias}");
            let err = handle_invoke_flow(&svc, &req, &b)
                .await
                .err()
                .expect("expected an error");
            assert_eq!(code(err), "ResourceNotFoundException", "{flow} {alias}");
        }
        // Without Bedrock Agents state there is no flow to run.
        let bare = BedrockAgentRuntimeService::new(Arc::new(RwLock::new(
            BedrockAgentRuntimeAccounts::new(),
        )));
        let b = json!({"flowIdentifier": FLOW, "flowAliasIdentifier": "TSTALIASID", "inputs": []});
        let err = handle_start_flow_execution(&bare, &req, &b)
            .await
            .err()
            .expect("expected an error");
        assert_eq!(code(err), "ResourceNotFoundException");
        assert!(svc
            .state
            .read()
            .accounts
            .get(ACCT)
            .is_none_or(|s| s.flow_executions.is_empty()));
    }

    #[tokio::test]
    async fn test_alias_runs_the_draft_and_snapshot_is_captured_at_start() {
        let (svc, agent_state) = flow_svc();
        let flow_arn = format!("arn:aws-cn:bedrock:cn-north-1:{ACCT}:flow/{FLOW}");
        let arn = start(&svc, &flow_arn, "TSTALIASID").await;
        assert!(
            arn.starts_with(&format!(
                "arn:aws-cn:bedrock:cn-north-1:{ACCT}:flow/{FLOW}/alias/TSTALIASID/execution/"
            )),
            "{arn}"
        );
        // Editing the draft after start does not change what the execution ran.
        agent_state
            .write()
            .accounts
            .get_mut(ACCT)
            .unwrap()
            .flows
            .get_mut(FLOW)
            .unwrap()
            .definition = Some(json!({"nodes": [], "connections": []}));

        let snap = body(
            handle_get_execution_flow_snapshot(
                &svc,
                &cn_request(),
                &exec_body(&flow_arn, "TSTALIASID", &arn),
            )
            .await
            .unwrap(),
        );
        assert_eq!(snap["flowIdentifier"], FLOW);
        assert_eq!(snap["flowAliasIdentifier"], "TSTALIASID");
        assert_eq!(snap["flowVersion"], "DRAFT");
        assert_eq!(snap["executionRoleArn"], DRAFT_ROLE);
        let def: Value = serde_json::from_str(snap["definition"].as_str().unwrap()).unwrap();
        assert_eq!(def, draft_definition());
        assert!(snap.get("customerEncryptionKeyArn").is_none());

        let got = body(
            handle_get_flow_execution(&svc, &cn_request(), &exec_body(FLOW, "TSTALIASID", &arn))
                .await
                .unwrap(),
        );
        assert_eq!(got["status"], "Running");
        assert_eq!(got["flowVersion"], "DRAFT");
        assert!(got.get("endedAt").is_none());
    }

    #[tokio::test]
    async fn alias_routes_to_its_version() {
        let (svc, _) = flow_svc();
        let alias_arn =
            format!("arn:aws-cn:bedrock:cn-north-1:{ACCT}:flow/{FLOW}/alias/{ALIAS_V1}");
        let arn = start(&svc, FLOW, &alias_arn).await;
        let exec_id = arn.rsplit('/').next().unwrap();
        let snap = body(
            handle_get_execution_flow_snapshot(
                &svc,
                &cn_request(),
                &exec_body(FLOW, ALIAS_V1, exec_id),
            )
            .await
            .unwrap(),
        );
        assert_eq!(snap["flowVersion"], "1");
        assert_eq!(snap["flowAliasIdentifier"], ALIAS_V1);
        assert_eq!(snap["executionRoleArn"], V1_ROLE);
        assert_eq!(snap["customerEncryptionKeyArn"], V1_KEY);
        let def: Value = serde_json::from_str(snap["definition"].as_str().unwrap()).unwrap();
        assert_eq!(def, v1_definition());

        let listed = body(
            handle_list_flow_executions(&svc, &cn_request(), &json!({"flowIdentifier": FLOW}))
                .await
                .unwrap(),
        );
        let summaries = listed["flowExecutionSummaries"].as_array().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0]["flowVersion"], "1");
        assert_eq!(summaries[0]["status"], "Running");
    }

    #[tokio::test]
    async fn unknown_or_foreign_executions_are_not_found() {
        let (svc, _) = flow_svc();
        let arn = start(&svc, FLOW, "TSTALIASID").await;
        let other = start(&svc, OTHER_FLOW, OTHER_ALIAS).await;
        let req = cn_request();
        // Unknown execution id.
        let err =
            handle_get_execution_flow_snapshot(&svc, &req, &exec_body(FLOW, "TSTALIASID", "nope"))
                .await
                .err()
                .expect("expected an error");
        assert_eq!(code(err), "ResourceNotFoundException");
        // An execution of another flow, asked for under FLOW.
        for handler_result in [
            handle_get_execution_flow_snapshot(&svc, &req, &exec_body(FLOW, "TSTALIASID", &other))
                .await,
            handle_get_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", &other)).await,
            handle_stop_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", &other)).await,
            handle_stop_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", "nope")).await,
        ] {
            assert_eq!(
                code(handler_result.err().expect("expected an error")),
                "ResourceNotFoundException"
            );
        }
        // The foreign stop left the other execution running.
        let got = body(
            handle_get_flow_execution(&svc, &req, &exec_body(OTHER_FLOW, OTHER_ALIAS, &other))
                .await
                .unwrap(),
        );
        assert_eq!(got["status"], "Running");
        // ListFlowExecutionEvents is scoped the same way.
        let mut events_req = cn_request();
        events_req
            .query_params
            .insert("eventType".into(), "Flow".into());
        let err = handle_list_flow_execution_events(
            &svc,
            &events_req,
            &exec_body(FLOW, "TSTALIASID", &other),
        )
        .await
        .err()
        .expect("expected an error");
        assert_eq!(code(err), "ResourceNotFoundException");
        handle_list_flow_execution_events(&svc, &events_req, &exec_body(FLOW, "TSTALIASID", &arn))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn stop_aborts_a_running_execution_once() {
        let (svc, _) = flow_svc();
        let arn = start(&svc, FLOW, "TSTALIASID").await;
        let req = cn_request();
        let stopped = body(
            handle_stop_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", &arn))
                .await
                .unwrap(),
        );
        assert_eq!(stopped["executionArn"], arn.as_str());
        assert_eq!(stopped["status"], "Aborted");
        let got = body(
            handle_get_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", &arn))
                .await
                .unwrap(),
        );
        assert_eq!(got["status"], "Aborted");
        let ended = got["endedAt"].clone();
        assert!(ended.is_string());
        // Stopping again reports the terminal status without re-ending it.
        let again = body(
            handle_stop_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", &arn))
                .await
                .unwrap(),
        );
        assert_eq!(again["status"], "Aborted");
        let got = body(
            handle_get_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", &arn))
                .await
                .unwrap(),
        );
        assert_eq!(got["endedAt"], ended);
    }

    #[tokio::test]
    async fn invoke_flow_records_the_routed_version_and_caller_execution_id() {
        let (svc, _) = flow_svc();
        let resp = handle_invoke_flow(
            &svc,
            &cn_request(),
            &json!({
                "flowIdentifier": FLOW,
                "flowAliasIdentifier": ALIAS_V1,
                "inputs": [],
                "executionId": "my-execution-1",
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            resp.headers["x-amz-bedrock-flow-execution-id"],
            "my-execution-1"
        );
        let accts = svc.state.read();
        let exec = &accts.accounts[ACCT].flow_executions
            [&crate::flows::execution_map_key(FLOW, ALIAS_V1, "my-execution-1")];
        assert_eq!(exec.flow_version, "1");
        assert_eq!(exec.status, "Succeeded");
        assert!(exec.ended_at.is_some());
        assert_eq!(
            exec.execution_arn,
            format!("arn:aws-cn:bedrock:cn-north-1:{ACCT}:flow/{FLOW}/alias/{ALIAS_V1}/execution/my-execution-1")
        );
        assert_eq!(exec.execution_role_arn.as_deref(), Some(V1_ROLE));
    }

    #[tokio::test]
    async fn executions_are_scoped_to_their_alias() {
        let (svc, _) = flow_svc();
        let arn = start(&svc, FLOW, ALIAS_V1).await;
        let exec_id = arn.rsplit('/').next().unwrap().to_string();
        let req = cn_request();
        // Found under its own alias (by id or ARN)...
        for id in [exec_id.as_str(), arn.as_str()] {
            handle_get_flow_execution(&svc, &req, &exec_body(FLOW, ALIAS_V1, id))
                .await
                .unwrap();
        }
        // ...but not under the test alias of the same flow.
        for id in [exec_id.as_str(), arn.as_str()] {
            for result in [
                handle_get_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", id)).await,
                handle_get_execution_flow_snapshot(&svc, &req, &exec_body(FLOW, "TSTALIASID", id))
                    .await,
                handle_stop_flow_execution(&svc, &req, &exec_body(FLOW, "TSTALIASID", id)).await,
            ] {
                let err = result.err().expect("expected an error");
                assert_eq!(code(err), "ResourceNotFoundException");
            }
        }
        // ListFlowExecutions filters by alias (id or alias ARN of this flow);
        // an alias ARN of another flow matches nothing.
        let list = |alias: &str| {
            let mut r = cn_request();
            r.query_params
                .insert("flowAliasIdentifier".into(), alias.to_string());
            r
        };
        let listed = |r: AwsRequest| {
            let svc = &svc;
            async move {
                body(
                    handle_list_flow_executions(svc, &r, &json!({"flowIdentifier": FLOW}))
                        .await
                        .unwrap(),
                )["flowExecutionSummaries"]
                    .as_array()
                    .unwrap()
                    .len()
            }
        };
        assert_eq!(listed(list(ALIAS_V1)).await, 1);
        assert_eq!(listed(list("TSTALIASID")).await, 0);
        let v1_arn = format!("arn:aws-cn:bedrock:cn-north-1:{ACCT}:flow/{FLOW}/alias/{ALIAS_V1}");
        assert_eq!(listed(list(&v1_arn)).await, 1);
        let foreign =
            format!("arn:aws-cn:bedrock:cn-north-1:{ACCT}:flow/{OTHER_FLOW}/alias/{ALIAS_V1}");
        assert_eq!(listed(list(&foreign)).await, 0);
    }

    #[tokio::test]
    async fn execution_name_is_the_execution_id_and_unique_per_alias() {
        let (svc, _) = flow_svc();
        let req = cn_request();
        let named = |alias: &str| {
            json!({
                "flowIdentifier": FLOW,
                "flowAliasIdentifier": alias,
                "inputs": [],
                "flowExecutionName": "nightly-run"
            })
        };
        let arn = body(
            handle_start_flow_execution(&svc, &req, &named(ALIAS_V1))
                .await
                .unwrap(),
        )["executionArn"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            arn,
            format!("arn:aws-cn:bedrock:cn-north-1:{ACCT}:flow/{FLOW}/alias/{ALIAS_V1}/execution/nightly-run")
        );
        handle_get_flow_execution(&svc, &req, &exec_body(FLOW, ALIAS_V1, "nightly-run"))
            .await
            .unwrap();
        // The same name again under the same alias conflicts...
        let err = handle_start_flow_execution(&svc, &req, &named(ALIAS_V1))
            .await
            .err()
            .expect("expected an error");
        assert_eq!(code(err), "ConflictException");
        // ...but another alias may use it.
        handle_start_flow_execution(&svc, &req, &named("TSTALIASID"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn invoke_flow_execution_ids_are_scoped_to_flow_and_alias() {
        let (svc, _) = flow_svc();
        let req = cn_request();
        let invoke = |alias: &str, flow: &str, id: &str| {
            json!({
                "flowIdentifier": flow,
                "flowAliasIdentifier": alias,
                "inputs": [],
                "executionId": id,
            })
        };
        handle_invoke_flow(&svc, &req, &invoke(ALIAS_V1, FLOW, "conversation-1"))
            .await
            .unwrap();
        // The same id under another alias or flow is a separate execution.
        for (alias, flow) in [("TSTALIASID", FLOW), (OTHER_ALIAS, OTHER_FLOW)] {
            handle_invoke_flow(&svc, &req, &invoke(alias, flow, "conversation-1"))
                .await
                .unwrap();
            let other = crate::flows::execution_map_key(flow, alias, "conversation-1");
            assert!(svc.state.read().accounts[ACCT]
                .flow_executions
                .contains_key(&other));
        }
        assert_eq!(svc.state.read().accounts[ACCT].flow_executions.len(), 3);
    }

    /// InvokeFlow cannot continue an execution that already ended (any
    /// terminal status) or one StartFlowExecution is running; the rejected
    /// call leaves the execution untouched.
    #[tokio::test]
    async fn invoke_flow_does_not_continue_finished_or_started_executions() {
        let (svc, _) = flow_svc();
        let req = cn_request();
        let invoke = |id: &str| {
            json!({
                "flowIdentifier": FLOW,
                "flowAliasIdentifier": ALIAS_V1,
                "inputs": [],
                "executionId": id,
            })
        };
        // A finished InvokeFlow execution (Succeeded).
        handle_invoke_flow(&svc, &req, &invoke("finished"))
            .await
            .unwrap();
        // A StartFlowExecution run, still Running, and one it then aborted.
        let named = |name: &str| {
            json!({
                "flowIdentifier": FLOW,
                "flowAliasIdentifier": ALIAS_V1,
                "inputs": [],
                "flowExecutionName": name,
            })
        };
        handle_start_flow_execution(&svc, &req, &named("running"))
            .await
            .unwrap();
        handle_start_flow_execution(&svc, &req, &named("aborted"))
            .await
            .unwrap();
        handle_stop_flow_execution(&svc, &req, &exec_body(FLOW, ALIAS_V1, "aborted"))
            .await
            .unwrap();
        // A failed and a timed-out execution.
        for (id, status) in [("failed", "Failed"), ("timed-out", "TimedOut")] {
            handle_invoke_flow(&svc, &req, &invoke(id)).await.unwrap();
            let key = crate::flows::execution_map_key(FLOW, ALIAS_V1, id);
            svc.state
                .write()
                .accounts
                .get_mut(ACCT)
                .unwrap()
                .flow_executions
                .get_mut(&key)
                .unwrap()
                .status = status.to_string();
        }

        for id in ["finished", "running", "aborted", "failed", "timed-out"] {
            let key = crate::flows::execution_map_key(FLOW, ALIAS_V1, id);
            let before = svc.state.read().accounts[ACCT].flow_executions[&key].clone();
            let err = handle_invoke_flow(&svc, &req, &invoke(id))
                .await
                .err()
                .expect("expected an error");
            assert_eq!(code(err), "ValidationException", "{id}");
            let after = svc.state.read().accounts[ACCT].flow_executions[&key].clone();
            assert_eq!(after.status, before.status, "{id}");
            assert_eq!(after.updated_at, before.updated_at, "{id}");
            assert_eq!(after.ended_at, before.ended_at, "{id}");
        }
    }

    #[tokio::test]
    async fn the_draft_runs_only_once_prepared() {
        let (svc, agent_state) = flow_svc();
        agent_state
            .write()
            .accounts
            .get_mut(ACCT)
            .unwrap()
            .flows
            .get_mut(FLOW)
            .unwrap()
            .status = "NotPrepared".to_string();
        let req = cn_request();
        let b = json!({"flowIdentifier": FLOW, "flowAliasIdentifier": "TSTALIASID", "inputs": []});
        for result in [
            handle_start_flow_execution(&svc, &req, &b).await,
            handle_invoke_flow(&svc, &req, &b).await,
        ] {
            let err = result.err().expect("expected an error");
            assert_eq!(code(err), "ValidationException");
        }
        // A published version still runs through its alias.
        start(&svc, FLOW, ALIAS_V1).await;
    }
}
