use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use http::{Method, StatusCode};
use regex::Regex;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    FlowExecution, InvocationRecord, InvocationStep, Session, SessionInvocation,
    SharedBedrockAgentRuntimeState,
};

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
}

impl BedrockAgentRuntimeService {
    pub fn new(state: SharedBedrockAgentRuntimeState) -> Self {
        Self {
            state,
            agent_state: None,
        }
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

fn make_error(status: StatusCode, code: &str, message: &str) -> AwsServiceError {
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

// Cached regexes for the most common patterns
fn re_session_identifier() -> Regex {
    Regex::new(r"^(arn:aws(-[^:]+)?:bedrock:[a-z0-9-]+:[0-9]{12}:session/[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})|([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})$").unwrap()
}
fn re_agent_id() -> Regex {
    Regex::new(r"^[0-9a-zA-Z]+$").unwrap()
}
fn re_session_id() -> Regex {
    Regex::new(r"^[0-9a-zA-Z._:-]+$").unwrap()
}
fn re_uuid() -> Regex {
    Regex::new(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$").unwrap()
}
fn re_memory_id() -> Regex {
    Regex::new(r"^[0-9a-zA-Z._:-]+$").unwrap()
}
fn re_flow_execution_id() -> Regex {
    // FlowExecutionIdentifier: max 2048, no pattern in model
    Regex::new(r"^[\x21-\x7e]+$").unwrap()
}
fn re_taggable_arn() -> Regex {
    // TaggableResourcesArn: lenient; reject literal {placeholder}
    Regex::new(r"^arn:[a-zA-Z0-9-]+:[a-zA-Z0-9-]+:[a-z0-9-]*:[0-9]{12}:.+$").unwrap()
}
fn re_flow_identifier() -> Regex {
    Regex::new(
        r"^(arn:aws:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:flow/[0-9a-zA-Z]{10})|([0-9a-zA-Z]{10})$",
    )
    .unwrap()
}
fn re_flow_alias_identifier() -> Regex {
    Regex::new(r"^(arn:aws:bedrock:[a-z0-9-]{1,20}:[0-9]{12}:flow/[0-9a-zA-Z]{10}/alias/[0-9a-zA-Z]{10})|(\bTSTALIASID\b|[0-9a-zA-Z]+)$").unwrap()
}
fn re_no_whitespace() -> Regex {
    // NextToken: ^\S*$
    Regex::new(r"^\S*$").unwrap()
}
fn re_aws_arn() -> Regex {
    // Generic AWS resource ARN
    Regex::new(r"^arn:aws(-[^:]+)?:[a-zA-Z0-9-]+:[a-z0-9-]*:[0-9]{12}:.+$").unwrap()
}
fn re_flow_execution_name() -> Regex {
    Regex::new(r"^[a-zA-Z0-9-]+$").unwrap()
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
            Some(&re_no_whitespace()),
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
    for (k, v) in path_params {
        out.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(out)
}

fn session_arn(account_id: &str, region: &str, session_id: &str) -> String {
    format!(
        "arn:aws:bedrock:{}:{}:session/{}",
        if region.is_empty() {
            "us-east-1"
        } else {
            region
        },
        account_id,
        session_id
    )
}

fn flow_execution_arn(account_id: &str, region: &str, flow_id: &str, execution_id: &str) -> String {
    format!(
        "arn:aws:bedrock:{}:{}:flow/{}/execution/{}",
        if region.is_empty() {
            "us-east-1"
        } else {
            region
        },
        account_id,
        flow_id,
        execution_id
    )
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

        match action {
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
        }
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
        Some(&re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    validate_str(
        agent_alias_id.as_deref(),
        "agentAliasId",
        Some(&re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    validate_str(
        session_id.as_deref(),
        "sessionId",
        Some(&re_session_id()),
        Some(2),
        Some(100),
        true,
    )?;
    let memory_id_opt = req_str(body, "memoryId");
    if let Some(ref m) = memory_id_opt {
        validate_str(
            Some(m),
            "memoryId",
            Some(&re_memory_id()),
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
            Some(&re_aws_arn()),
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
    let flow_id = req_str(body, "flowIdentifier");
    let flow_alias_id = req_str(body, "flowAliasIdentifier");
    validate_str(
        flow_id.as_deref(),
        "flowIdentifier",
        Some(&re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        flow_alias_id.as_deref(),
        "flowAliasIdentifier",
        Some(&re_flow_alias_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    if body.get("inputs").is_none() {
        return Err(validation("inputs is required"));
    }
    // executionId optional, length 2..100, pattern session-id-like
    let execution_id_opt = req_str(body, "executionId");
    if let Some(ref s) = execution_id_opt {
        validate_str(
            Some(s),
            "executionId",
            Some(&re_session_id()),
            Some(2),
            Some(100),
            false,
        )?;
    }

    let flow_id = flow_id.unwrap();
    let execution_id = uuid::Uuid::new_v4().to_string();
    let input = req_str(body, "input").unwrap_or_default();

    let start = std::time::Instant::now();
    let document = format!("Flow output for input: {}", input);

    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        s.flow_executions.insert(
            execution_id.clone(),
            FlowExecution {
                execution_id: execution_id.clone(),
                execution_arn: flow_execution_arn(
                    &req.account_id,
                    &req.region,
                    &flow_id,
                    &execution_id,
                ),
                flow_id: flow_id.clone(),
                flow_alias_id: flow_alias_id.unwrap_or_default(),
                flow_version: "DRAFT".to_string(),
                status: "SUCCEEDED".to_string(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                ended_at: Some(Utc::now()),
            },
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

    let frame = crate::eventstream::flow_output_frame("StartNode", &document);
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
        Some(&re_session_id()),
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
        Some(&Regex::new(r"^[0-9a-zA-Z]+$").unwrap()),
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
            Some(&re_session_id()),
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
    let citation = json!({
        "generatedResponsePart": {
            "textResponsePart": {
                "text": output_text,
                "span": { "start": 0, "end": 30 }
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
    });
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
            Some(&re_session_id()),
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
            citations: Vec::new(),
            timestamp: Utc::now(),
            duration_ms: 0,
        });
    }

    // Emit a single output event frame (eventstream payload).
    let event_body = serde_json::to_vec(&json!({ "output": { "text": output_text } })).unwrap();
    let frame = crate::eventstream::encode_frame(
        &[
            (":event-type", "output"),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        &event_body,
    );

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
    let arn = session_arn(&req.account_id, &req.region, &session_id);

    let metadata: std::collections::BTreeMap<String, String> = body
        .get("sessionMetadata")
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
        Some(&re_session_identifier()),
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
        s.sessions.remove(&k);
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
        Some(&re_session_identifier()),
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
        Some(&re_session_identifier()),
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
        Some(&re_session_identifier()),
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
        Some(&re_session_identifier()),
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
        Some(&re_session_identifier()),
        None,
        None,
        true,
    )?;
    let step_id = req_str(body, "invocationStepId");
    validate_str(
        step_id.as_deref(),
        "invocationStepId",
        Some(&re_uuid()),
        None,
        None,
        true,
    )?;
    let invocation_id = req_str(body, "invocationIdentifier");
    validate_str(
        invocation_id.as_deref(),
        "invocationIdentifier",
        Some(&re_uuid()),
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
        Some(&re_session_identifier()),
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
        Some(&re_session_identifier()),
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
        Some(&re_session_identifier()),
        None,
        None,
        true,
    )?;
    let invocation_id = req_str(body, "invocationIdentifier");
    validate_str(
        invocation_id.as_deref(),
        "invocationIdentifier",
        Some(&re_uuid()),
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

async fn handle_get_flow_execution(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let flow_id = req_str(body, "flowIdentifier");
    let flow_alias_id = req_str(body, "flowAliasIdentifier");
    let exec_id = req_str(body, "executionIdentifier");
    validate_str(
        flow_id.as_deref(),
        "flowIdentifier",
        Some(&re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        flow_alias_id.as_deref(),
        "flowAliasIdentifier",
        Some(&re_flow_alias_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        exec_id.as_deref(),
        "executionIdentifier",
        Some(&re_flow_execution_id()),
        None,
        Some(2048),
        true,
    )?;

    let exec_id = exec_id.unwrap();
    let accts = svc.state.read();
    let s = accts.accounts.get(&req.account_id).ok_or_else(|| {
        make_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            "Flow execution not found",
        )
    })?;
    // Match by execution_id or ARN
    let exec = s
        .flow_executions
        .values()
        .find(|e| e.execution_id == exec_id || e.execution_arn == exec_id)
        .ok_or_else(|| {
            make_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                "Flow execution not found",
            )
        })?;

    Ok(AwsResponse::ok_json(json!({
        "executionArn": exec.execution_arn,
        "flowIdentifier": exec.flow_id,
        "flowAliasIdentifier": exec.flow_alias_id,
        "flowVersion": exec.flow_version,
        "status": exec.status,
        "startedAt": exec.created_at.to_rfc3339(),
        "endedAt": exec.ended_at.map(|d| d.to_rfc3339()),
    })))
}

async fn handle_list_flow_execution_events(
    _svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let flow_id = req_str(body, "flowIdentifier");
    let flow_alias_id = req_str(body, "flowAliasIdentifier");
    let exec_id = req_str(body, "executionIdentifier");
    validate_str(
        flow_id.as_deref(),
        "flowIdentifier",
        Some(&re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        flow_alias_id.as_deref(),
        "flowAliasIdentifier",
        Some(&re_flow_alias_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        exec_id.as_deref(),
        "executionIdentifier",
        Some(&re_flow_execution_id()),
        None,
        Some(2048),
        true,
    )?;
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
        Some(&re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    let alias = req.query_params.get("flowAliasIdentifier").cloned();
    if let Some(ref a) = alias {
        validate_str(
            Some(a),
            "flowAliasIdentifier",
            Some(&re_flow_alias_identifier()),
            Some(1),
            Some(2048),
            false,
        )?;
    }
    validate_int_range(extract_int(req, body, "maxResults"), "maxResults", 1, 1000)?;
    validate_next_token(req, body)?;
    let flow_id = flow_id.unwrap();

    let accts = svc.state.read();
    let summaries: Vec<Value> = accts
        .accounts
        .get(&req.account_id)
        .map(|state| {
            state
                .flow_executions
                .values()
                .filter(|e| e.flow_id == flow_id)
                .map(|e| {
                    json!({
                        "executionArn": e.execution_arn,
                        "flowIdentifier": e.flow_id,
                        "flowAliasIdentifier": e.flow_alias_id,
                        "flowVersion": e.flow_version,
                        "status": e.status,
                        "createdAt": e.created_at.to_rfc3339(),
                        "endedAt": e.ended_at.map(|d| d.to_rfc3339()),
                    })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(AwsResponse::ok_json(json!({
        "flowExecutionSummaries": summaries
    })))
}

async fn handle_start_flow_execution(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let flow_id = req_str(body, "flowIdentifier");
    let flow_alias_id = req_str(body, "flowAliasIdentifier");
    validate_str(
        flow_id.as_deref(),
        "flowIdentifier",
        Some(&re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        flow_alias_id.as_deref(),
        "flowAliasIdentifier",
        Some(&re_flow_alias_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    if body.get("inputs").is_none() {
        return Err(validation("inputs is required"));
    }
    let exec_name = req_str(body, "flowExecutionName");
    if let Some(ref n) = exec_name {
        validate_str(
            Some(n),
            "flowExecutionName",
            Some(&re_flow_execution_name()),
            Some(1),
            Some(36),
            false,
        )?;
    }

    let flow_id = flow_id.unwrap();
    let flow_alias_id = flow_alias_id.unwrap();
    let execution_id = uuid::Uuid::new_v4().to_string();
    let arn = flow_execution_arn(&req.account_id, &req.region, &flow_id, &execution_id);
    let now = Utc::now();

    {
        let mut accts = svc.state.write();
        let s = accts.get_or_create(&req.account_id);
        s.flow_executions.insert(
            execution_id.clone(),
            FlowExecution {
                execution_id: execution_id.clone(),
                execution_arn: arn.clone(),
                flow_id: flow_id.clone(),
                flow_alias_id,
                flow_version: "DRAFT".to_string(),
                status: "InProgress".to_string(),
                created_at: now,
                updated_at: now,
                ended_at: None,
            },
        );
    }

    Ok(AwsResponse::ok_json(json!({ "executionArn": arn })))
}

async fn handle_stop_flow_execution(
    svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let flow_id = req_str(body, "flowIdentifier");
    let flow_alias_id = req_str(body, "flowAliasIdentifier");
    let exec_id = req_str(body, "executionIdentifier");
    validate_str(
        flow_id.as_deref(),
        "flowIdentifier",
        Some(&re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        flow_alias_id.as_deref(),
        "flowAliasIdentifier",
        Some(&re_flow_alias_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        exec_id.as_deref(),
        "executionIdentifier",
        Some(&re_flow_execution_id()),
        None,
        Some(2048),
        true,
    )?;
    let exec_id = exec_id.unwrap();

    let now = Utc::now();
    let mut accts = svc.state.write();
    let s = accts.get_or_create(&req.account_id);
    let key = s
        .flow_executions
        .iter()
        .find(|(_, e)| e.execution_id == exec_id || e.execution_arn == exec_id)
        .map(|(k, _)| k.clone());
    let (arn, status) = if let Some(k) = key {
        let e = s.flow_executions.get_mut(&k).unwrap();
        e.status = "Aborted".to_string();
        e.updated_at = now;
        e.ended_at = Some(now);
        (e.execution_arn.clone(), e.status.clone())
    } else {
        // Allow stop on unknown execution: still return Aborted with synthetic ARN
        (
            flow_execution_arn(&req.account_id, &req.region, "unknown", &exec_id),
            "Aborted".to_string(),
        )
    };

    Ok(AwsResponse::ok_json(json!({
        "executionArn": arn,
        "status": status,
    })))
}

async fn handle_get_execution_flow_snapshot(
    _svc: &BedrockAgentRuntimeService,
    req: &AwsRequest,
    body: &Value,
) -> Result<AwsResponse, AwsServiceError> {
    let flow_id = req_str(body, "flowIdentifier");
    let flow_alias_id = req_str(body, "flowAliasIdentifier");
    let exec_id = req_str(body, "executionIdentifier");
    validate_str(
        flow_id.as_deref(),
        "flowIdentifier",
        Some(&re_flow_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        flow_alias_id.as_deref(),
        "flowAliasIdentifier",
        Some(&re_flow_alias_identifier()),
        Some(1),
        Some(2048),
        true,
    )?;
    validate_str(
        exec_id.as_deref(),
        "executionIdentifier",
        Some(&re_flow_execution_id()),
        None,
        Some(2048),
        true,
    )?;

    let flow_id = flow_id.unwrap();
    let flow_alias_id = flow_alias_id.unwrap();

    let role_arn = format!(
        "arn:aws:iam::{}:role/service-role/AmazonBedrockExecutionRoleForFlow_{}",
        req.account_id, flow_id
    );

    let definition = serde_json::to_string(&json!({
        "nodes": [
            {
                "name": "Start",
                "type": "Input",
                "configuration": { "input": {} }
            }
        ],
        "connections": []
    }))
    .unwrap();

    Ok(AwsResponse::ok_json(json!({
        "flowIdentifier": flow_id,
        "flowAliasIdentifier": flow_alias_id,
        "flowVersion": "DRAFT",
        "executionRoleArn": role_arn,
        "definition": definition,
    })))
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
        Some(&re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    validate_str(
        agent_alias_id.as_deref(),
        "agentAliasId",
        Some(&re_agent_id()),
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
            Some(&re_memory_id()),
            Some(2),
            Some(100),
            false,
        )?;
    }
    if let Some(ref s) = session_id {
        validate_str(
            Some(s),
            "sessionId",
            Some(&re_session_id()),
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
        Some(&re_agent_id()),
        Some(1),
        Some(10),
        true,
    )?;
    validate_str(
        agent_alias_id.as_deref(),
        "agentAliasId",
        Some(&re_agent_id()),
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
        Some(&re_memory_id()),
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
        Some(&re_taggable_arn()),
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
        Some(&re_taggable_arn()),
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
        Some(&re_taggable_arn()),
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
