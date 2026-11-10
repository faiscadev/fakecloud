use std::collections::BTreeMap;
use std::sync::Arc;

use crate::arns::{flow_arn, prompt_arn};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::{Method, StatusCode};
use parking_lot::RwLock;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    Agent, AgentActionGroup, AgentAlias, AgentCollaborator, AgentKnowledgeBase, AgentVersion,
    BedrockAgentAccounts, BedrockAgentSnapshot, DataSource, Flow, FlowAlias, FlowVersion,
    IngestionJob, KnowledgeBase, Prompt, PromptVersion, SharedBedrockAgentState,
    BEDROCK_AGENT_SNAPSHOT_SCHEMA_VERSION,
};

/// Bedrock Agent read actions all start with `Get`, `List`, or `Validate`;
/// every other action is a mutation. The inverse formulation guarantees no
/// mutation is ever missed.
fn is_mutating_action(action: &str) -> bool {
    !(action.starts_with("Get") || action.starts_with("List") || action.starts_with("Validate"))
}

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateAgent",
    "CreateAgentActionGroup",
    "CreateAgentAlias",
    "CreateDataSource",
    "CreateFlow",
    "CreateFlowAlias",
    "CreateFlowVersion",
    "CreateKnowledgeBase",
    "CreatePrompt",
    "CreatePromptVersion",
    "DeleteAgent",
    "DeleteAgentActionGroup",
    "DeleteAgentAlias",
    "DeleteAgentVersion",
    "DeleteDataSource",
    "DeleteFlow",
    "DeleteFlowAlias",
    "DeleteFlowVersion",
    "DeleteKnowledgeBase",
    "DeleteKnowledgeBaseDocuments",
    "DeletePrompt",
    "DisassociateAgentCollaborator",
    "DisassociateAgentKnowledgeBase",
    "GetAgent",
    "GetAgentActionGroup",
    "GetAgentAlias",
    "GetAgentCollaborator",
    "GetAgentKnowledgeBase",
    "GetAgentVersion",
    "GetDataSource",
    "GetFlow",
    "GetFlowAlias",
    "GetFlowVersion",
    "GetIngestionJob",
    "GetKnowledgeBase",
    "GetKnowledgeBaseDocuments",
    "GetPrompt",
    "IngestKnowledgeBaseDocuments",
    "ListAgentActionGroups",
    "ListAgentAliases",
    "ListAgentCollaborators",
    "ListAgentKnowledgeBases",
    "ListAgentVersions",
    "ListAgents",
    "ListDataSources",
    "ListFlowAliases",
    "ListFlowVersions",
    "ListFlows",
    "ListIngestionJobs",
    "ListKnowledgeBaseDocuments",
    "ListKnowledgeBases",
    "ListPrompts",
    "ListTagsForResource",
    "PrepareAgent",
    "PrepareFlow",
    "StartIngestionJob",
    "StopIngestionJob",
    "TagResource",
    "UntagResource",
    "UpdateAgent",
    "UpdateAgentActionGroup",
    "UpdateAgentAlias",
    "UpdateAgentCollaborator",
    "UpdateAgentKnowledgeBase",
    "UpdateDataSource",
    "UpdateFlow",
    "UpdateFlowAlias",
    "UpdateKnowledgeBase",
    "UpdatePrompt",
    "ValidateFlowDefinition",
    "AssociateAgentCollaborator",
    "AssociateAgentKnowledgeBase",
];

pub struct BedrockAgentService {
    state: SharedBedrockAgentState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

mod agents;
mod data_sources;
mod flows;
mod knowledge_bases;
mod prompts;
mod tags;

impl BedrockAgentService {
    pub fn new(state: SharedBedrockAgentState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn shared_state(&self) -> SharedBedrockAgentState {
        Arc::clone(&self.state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_bedrock_agent_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Bedrock Agent state when invoked,
    /// or `None` in memory mode. The CloudFormation provisioner mutates `state`
    /// directly and uses this to write a CFN-provisioned resource through to
    /// disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_bedrock_agent_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<(String, String)>)> {
        // Preserve empty path segments so synthetic probes that drop a
        // required `@httpLabel` (e.g. an empty `agentVersion` -> `//`) still
        // reach the right action and surface as a ValidationException rather
        // than a routing miss.
        let raw_segs: Vec<String> = req
            .raw_path
            .trim_start_matches('/')
            .split('/')
            .map(|s| s.to_string())
            .collect();
        let raw_segs: Vec<String> =
            if raw_segs.last().map(|s| s.is_empty()).unwrap_or(false) && raw_segs.len() > 1 {
                // Drop a trailing empty caused by a trailing slash on the URL,
                // which is purely cosmetic. Keep interior empties.
                raw_segs[..raw_segs.len() - 1].to_vec()
            } else {
                raw_segs
            };
        let segs = &raw_segs;
        if segs.is_empty() || segs.iter().all(|s| s.is_empty()) {
            return None;
        }

        let m = &req.method;
        let mut params = Vec::new();

        // Agents
        if segs.len() == 1 && segs[0] == "agents" {
            if *m == Method::PUT {
                return Some(("CreateAgent", params));
            }
            if *m == Method::POST {
                return Some(("ListAgents", params));
            }
        }
        if segs.len() == 2 && segs[0] == "agents" {
            params.push(("agentId".to_string(), segs[1].clone()));
            if *m == Method::GET {
                return Some(("GetAgent", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateAgent", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteAgent", params));
            }
            if *m == Method::POST {
                return Some(("PrepareAgent", params));
            }
        }
        if segs.len() == 3 && segs[0] == "agents" && segs[2] == "agentaliases" {
            params.push(("agentId".to_string(), segs[1].clone()));
            if *m == Method::PUT {
                return Some(("CreateAgentAlias", params));
            }
            if *m == Method::POST {
                return Some(("ListAgentAliases", params));
            }
        }
        if segs.len() == 4 && segs[0] == "agents" && segs[2] == "agentaliases" {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentAliasId".to_string(), segs[3].clone()));
            if *m == Method::GET {
                return Some(("GetAgentAlias", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateAgentAlias", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteAgentAlias", params));
            }
        }
        if segs.len() == 3 && segs[0] == "agents" && segs[2] == "agentversions" {
            params.push(("agentId".to_string(), segs[1].clone()));
            if *m == Method::POST {
                return Some(("ListAgentVersions", params));
            }
        }
        if segs.len() == 4 && segs[0] == "agents" && segs[2] == "agentversions" {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentVersion".to_string(), segs[3].clone()));
            if *m == Method::GET {
                return Some(("GetAgentVersion", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteAgentVersion", params));
            }
        }
        if segs.len() == 5
            && segs[0] == "agents"
            && segs[2] == "agentversions"
            && segs[4] == "actiongroups"
        {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentVersion".to_string(), segs[3].clone()));
            if *m == Method::PUT {
                return Some(("CreateAgentActionGroup", params));
            }
            if *m == Method::POST {
                return Some(("ListAgentActionGroups", params));
            }
        }
        if segs.len() == 6
            && segs[0] == "agents"
            && segs[2] == "agentversions"
            && segs[4] == "actiongroups"
        {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentVersion".to_string(), segs[3].clone()));
            params.push(("actionGroupId".to_string(), segs[5].clone()));
            if *m == Method::GET {
                return Some(("GetAgentActionGroup", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateAgentActionGroup", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteAgentActionGroup", params));
            }
        }
        if segs.len() == 5
            && segs[0] == "agents"
            && segs[2] == "agentversions"
            && segs[4] == "knowledgebases"
        {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentVersion".to_string(), segs[3].clone()));
            if *m == Method::PUT {
                return Some(("AssociateAgentKnowledgeBase", params));
            }
            if *m == Method::POST {
                return Some(("ListAgentKnowledgeBases", params));
            }
        }
        if segs.len() == 6
            && segs[0] == "agents"
            && segs[2] == "agentversions"
            && segs[4] == "knowledgebases"
        {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentVersion".to_string(), segs[3].clone()));
            params.push(("knowledgeBaseId".to_string(), segs[5].clone()));
            if *m == Method::GET {
                return Some(("GetAgentKnowledgeBase", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateAgentKnowledgeBase", params));
            }
            if *m == Method::DELETE {
                return Some(("DisassociateAgentKnowledgeBase", params));
            }
        }
        if segs.len() == 5
            && segs[0] == "agents"
            && segs[2] == "agentversions"
            && segs[4] == "agentcollaborators"
        {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentVersion".to_string(), segs[3].clone()));
            if *m == Method::PUT {
                return Some(("AssociateAgentCollaborator", params));
            }
            if *m == Method::POST {
                return Some(("ListAgentCollaborators", params));
            }
        }
        if segs.len() == 6
            && segs[0] == "agents"
            && segs[2] == "agentversions"
            && segs[4] == "agentcollaborators"
        {
            params.push(("agentId".to_string(), segs[1].clone()));
            params.push(("agentVersion".to_string(), segs[3].clone()));
            params.push(("collaboratorId".to_string(), segs[5].clone()));
            if *m == Method::GET {
                return Some(("GetAgentCollaborator", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateAgentCollaborator", params));
            }
            if *m == Method::DELETE {
                return Some(("DisassociateAgentCollaborator", params));
            }
        }

        // Knowledge bases
        if segs.len() == 1 && segs[0] == "knowledgebases" {
            if *m == Method::PUT {
                return Some(("CreateKnowledgeBase", params));
            }
            if *m == Method::POST {
                return Some(("ListKnowledgeBases", params));
            }
        }
        if segs.len() == 2 && segs[0] == "knowledgebases" {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            if *m == Method::GET {
                return Some(("GetKnowledgeBase", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateKnowledgeBase", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteKnowledgeBase", params));
            }
        }
        if segs.len() == 3 && segs[0] == "knowledgebases" && segs[2] == "datasources" {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            if *m == Method::PUT {
                return Some(("CreateDataSource", params));
            }
            if *m == Method::POST {
                return Some(("ListDataSources", params));
            }
        }
        if segs.len() == 4 && segs[0] == "knowledgebases" && segs[2] == "datasources" {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            params.push(("dataSourceId".to_string(), segs[3].clone()));
            if *m == Method::GET {
                return Some(("GetDataSource", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateDataSource", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteDataSource", params));
            }
        }
        if segs.len() == 5
            && segs[0] == "knowledgebases"
            && segs[2] == "datasources"
            && segs[4] == "ingestionjobs"
        {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            params.push(("dataSourceId".to_string(), segs[3].clone()));
            if *m == Method::PUT {
                return Some(("StartIngestionJob", params));
            }
            if *m == Method::POST {
                return Some(("ListIngestionJobs", params));
            }
        }
        if segs.len() == 6
            && segs[0] == "knowledgebases"
            && segs[2] == "datasources"
            && segs[4] == "ingestionjobs"
        {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            params.push(("dataSourceId".to_string(), segs[3].clone()));
            params.push(("ingestionJobId".to_string(), segs[5].clone()));
            if *m == Method::GET {
                return Some(("GetIngestionJob", params));
            }
        }
        if segs.len() == 7
            && segs[0] == "knowledgebases"
            && segs[2] == "datasources"
            && segs[4] == "ingestionjobs"
            && segs[6] == "stop"
        {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            params.push(("dataSourceId".to_string(), segs[3].clone()));
            params.push(("ingestionJobId".to_string(), segs[5].clone()));
            if *m == Method::POST {
                return Some(("StopIngestionJob", params));
            }
        }
        if segs.len() == 5
            && segs[0] == "knowledgebases"
            && segs[2] == "datasources"
            && segs[4] == "documents"
        {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            params.push(("dataSourceId".to_string(), segs[3].clone()));
            if *m == Method::PUT {
                return Some(("IngestKnowledgeBaseDocuments", params));
            }
            if *m == Method::POST {
                return Some(("ListKnowledgeBaseDocuments", params));
            }
        }
        if segs.len() == 6
            && segs[0] == "knowledgebases"
            && segs[2] == "datasources"
            && segs[4] == "documents"
            && segs[5] == "getDocuments"
        {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            params.push(("dataSourceId".to_string(), segs[3].clone()));
            if *m == Method::POST {
                return Some(("GetKnowledgeBaseDocuments", params));
            }
        }
        if segs.len() == 6
            && segs[0] == "knowledgebases"
            && segs[2] == "datasources"
            && segs[4] == "documents"
            && segs[5] == "deleteDocuments"
        {
            params.push(("knowledgeBaseId".to_string(), segs[1].clone()));
            params.push(("dataSourceId".to_string(), segs[3].clone()));
            if *m == Method::POST {
                return Some(("DeleteKnowledgeBaseDocuments", params));
            }
        }

        // Flows
        if segs.len() == 1 && segs[0] == "flows" {
            if *m == Method::POST {
                return Some(("CreateFlow", params));
            }
            if *m == Method::GET {
                return Some(("ListFlows", params));
            }
        }
        // Checked before `/flows/{flowIdentifier}`, whose POST (PrepareFlow)
        // would otherwise swallow it as a flow named `validate-definition`.
        if segs.len() == 2
            && segs[0] == "flows"
            && segs[1] == "validate-definition"
            && *m == Method::POST
        {
            return Some(("ValidateFlowDefinition", params));
        }
        if segs.len() == 2 && segs[0] == "flows" {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            if *m == Method::GET {
                return Some(("GetFlow", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateFlow", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteFlow", params));
            }
            if *m == Method::POST {
                return Some(("PrepareFlow", params));
            }
        }
        if segs.len() == 3 && segs[0] == "flows" && segs[2] == "aliases" {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            if *m == Method::POST {
                return Some(("CreateFlowAlias", params));
            }
            if *m == Method::GET {
                return Some(("ListFlowAliases", params));
            }
        }
        if segs.len() == 4 && segs[0] == "flows" && segs[2] == "aliases" {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            params.push(("aliasIdentifier".to_string(), segs[3].clone()));
            if *m == Method::GET {
                return Some(("GetFlowAlias", params));
            }
            if *m == Method::PUT {
                return Some(("UpdateFlowAlias", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteFlowAlias", params));
            }
        }
        if segs.len() == 3 && segs[0] == "flows" && segs[2] == "versions" {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            if *m == Method::POST {
                return Some(("CreateFlowVersion", params));
            }
            if *m == Method::GET {
                return Some(("ListFlowVersions", params));
            }
        }
        if segs.len() == 4 && segs[0] == "flows" && segs[2] == "versions" {
            params.push(("flowIdentifier".to_string(), segs[1].clone()));
            params.push(("flowVersion".to_string(), segs[3].clone()));
            if *m == Method::GET {
                return Some(("GetFlowVersion", params));
            }
            if *m == Method::DELETE {
                return Some(("DeleteFlowVersion", params));
            }
        }

        // Prompts
        if segs.len() == 1 && segs[0] == "prompts" {
            if *m == Method::POST {
                return Some(("CreatePrompt", params));
            }
            if *m == Method::GET {
                return Some(("ListPrompts", params));
            }
        }
        if segs.len() == 2 && segs[0] == "prompts" {
            params.push(("promptIdentifier".to_string(), segs[1].clone()));
            if *m == Method::GET {
                return Some(("GetPrompt", params));
            }
            if *m == Method::PUT {
                return Some(("UpdatePrompt", params));
            }
            if *m == Method::DELETE {
                return Some(("DeletePrompt", params));
            }
        }
        if segs.len() == 3 && segs[0] == "prompts" && segs[2] == "versions" {
            params.push(("promptIdentifier".to_string(), segs[1].clone()));
            if *m == Method::POST {
                return Some(("CreatePromptVersion", params));
            }
        }

        // Tags
        if segs.len() == 2 && segs[0] == "tags" {
            params.push(("resourceArn".to_string(), segs[1].clone()));
            if *m == Method::POST {
                return Some(("TagResource", params));
            }
            if *m == Method::GET {
                return Some(("ListTagsForResource", params));
            }
            if *m == Method::DELETE {
                return Some(("UntagResource", params));
            }
        }

        None
    }
}

impl Default for BedrockAgentService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())))
    }
}

#[async_trait]
impl AwsService for BedrockAgentService {
    fn service_name(&self) -> &str {
        "bedrock-agent"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, mut req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, path_params) =
            Self::resolve_action(&req).ok_or_else(|| AwsServiceError::ActionNotImplemented {
                service: "bedrock-agent".to_string(),
                action: format!("{} {}", req.method, req.raw_path),
            })?;

        req.action = action.to_string();

        if !path_params.is_empty() {
            let mut body = req.json_body();
            if body.is_null() {
                body = serde_json::Value::Object(serde_json::Map::new());
            }
            // The labels were cut from the raw wire path (to keep empty
            // segments), so they are still percent-encoded (an ARN identifier
            // is `arn%3Aaws%3A...`); decode each exactly once, the same way
            // dispatch decodes `path_segments`.
            for (k, v) in path_params {
                body[k] =
                    serde_json::Value::String(fakecloud_core::path::percent_decode_segment(&v));
            }
            req.body = serde_json::to_vec(&body).unwrap_or_default().into();
        }

        validate_inputs(action, &req)?;

        let mutates = is_mutating_action(action);
        let result = match action {
            "CreateAgent" => self.create_agent(&req),
            "CreateAgentActionGroup" => self.create_agent_action_group(&req),
            "CreateAgentAlias" => self.create_agent_alias(&req),
            "CreateDataSource" => self.create_data_source(&req),
            "CreateFlow" => self.create_flow(&req),
            "CreateFlowAlias" => self.create_flow_alias(&req),
            "CreateFlowVersion" => self.create_flow_version(&req),
            "CreateKnowledgeBase" => self.create_knowledge_base(&req),
            "CreatePrompt" => self.create_prompt(&req),
            "CreatePromptVersion" => self.create_prompt_version(&req),
            "DeleteAgent" => self.delete_agent(&req),
            "DeleteAgentActionGroup" => self.delete_agent_action_group(&req),
            "DeleteAgentAlias" => self.delete_agent_alias(&req),
            "DeleteAgentVersion" => self.delete_agent_version(&req),
            "DeleteDataSource" => self.delete_data_source(&req),
            "DeleteFlow" => self.delete_flow(&req),
            "DeleteFlowAlias" => self.delete_flow_alias(&req),
            "DeleteFlowVersion" => self.delete_flow_version(&req),
            "DeleteKnowledgeBase" => self.delete_knowledge_base(&req),
            "DeleteKnowledgeBaseDocuments" => self.delete_knowledge_base_documents(&req),
            "DeletePrompt" => self.delete_prompt(&req),
            "DisassociateAgentCollaborator" => self.disassociate_agent_collaborator(&req),
            "DisassociateAgentKnowledgeBase" => self.disassociate_agent_knowledge_base(&req),
            "GetAgent" => self.get_agent(&req),
            "GetAgentActionGroup" => self.get_agent_action_group(&req),
            "GetAgentAlias" => self.get_agent_alias(&req),
            "GetAgentCollaborator" => self.get_agent_collaborator(&req),
            "GetAgentKnowledgeBase" => self.get_agent_knowledge_base(&req),
            "GetAgentVersion" => self.get_agent_version(&req),
            "GetDataSource" => self.get_data_source(&req),
            "GetFlow" => self.get_flow(&req),
            "GetFlowAlias" => self.get_flow_alias(&req),
            "GetFlowVersion" => self.get_flow_version(&req),
            "GetIngestionJob" => self.get_ingestion_job(&req),
            "GetKnowledgeBase" => self.get_knowledge_base(&req),
            "GetKnowledgeBaseDocuments" => self.get_knowledge_base_documents(&req),
            "GetPrompt" => self.get_prompt(&req),
            "IngestKnowledgeBaseDocuments" => self.ingest_knowledge_base_documents(&req),
            "ListAgentActionGroups" => self.list_agent_action_groups(&req),
            "ListAgentAliases" => self.list_agent_aliases(&req),
            "ListAgentCollaborators" => self.list_agent_collaborators(&req),
            "ListAgentKnowledgeBases" => self.list_agent_knowledge_bases(&req),
            "ListAgentVersions" => self.list_agent_versions(&req),
            "ListAgents" => self.list_agents(&req),
            "ListDataSources" => self.list_data_sources(&req),
            "ListFlowAliases" => self.list_flow_aliases(&req),
            "ListFlowVersions" => self.list_flow_versions(&req),
            "ListFlows" => self.list_flows(&req),
            "ListIngestionJobs" => self.list_ingestion_jobs(&req),
            "ListKnowledgeBaseDocuments" => self.list_knowledge_base_documents(&req),
            "ListKnowledgeBases" => self.list_knowledge_bases(&req),
            "ListPrompts" => self.list_prompts(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "PrepareAgent" => self.prepare_agent(&req),
            "PrepareFlow" => self.prepare_flow(&req),
            "StartIngestionJob" => self.start_ingestion_job(&req),
            "StopIngestionJob" => self.stop_ingestion_job(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "UpdateAgent" => self.update_agent(&req),
            "UpdateAgentActionGroup" => self.update_agent_action_group(&req),
            "UpdateAgentAlias" => self.update_agent_alias(&req),
            "UpdateAgentCollaborator" => self.update_agent_collaborator(&req),
            "UpdateAgentKnowledgeBase" => self.update_agent_knowledge_base(&req),
            "UpdateDataSource" => self.update_data_source(&req),
            "UpdateFlow" => self.update_flow(&req),
            "UpdateFlowAlias" => self.update_flow_alias(&req),
            "UpdateKnowledgeBase" => self.update_knowledge_base(&req),
            "UpdatePrompt" => self.update_prompt(&req),
            "ValidateFlowDefinition" => self.validate_flow_definition(&req),
            "AssociateAgentCollaborator" => self.associate_agent_collaborator(&req),
            "AssociateAgentKnowledgeBase" => self.associate_agent_knowledge_base(&req),
            other => Err(AwsServiceError::action_not_implemented(
                "bedrock-agent",
                other,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

/// Persist the current Bedrock Agent state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `BedrockAgentService::save_snapshot` and the
/// CloudFormation provisioner persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_bedrock_agent_snapshot(
    state: &SharedBedrockAgentState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = BedrockAgentSnapshot {
        schema_version: BEDROCK_AGENT_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write bedrock-agent snapshot"),
        Err(err) => tracing::error!(%err, "bedrock-agent snapshot task panicked"),
    }
}

fn missing(field: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("Missing required field: {field}"),
    )
}

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg.into())
}

/// Smithy `Id` shape: `^[0-9a-zA-Z]{10}$`.
fn is_valid_id(s: &str) -> bool {
    s.len() == 10 && s.bytes().all(|b| b.is_ascii_alphanumeric())
}

/// Smithy `Version` shape: length 1..=5, pattern `^(DRAFT|[0-9]{0,4}[1-9][0-9]{0,4})$`.
fn is_valid_version(s: &str) -> bool {
    if s.is_empty() || s.len() > 5 {
        return false;
    }
    if s == "DRAFT" {
        return true;
    }
    if !s.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    // The pattern `[0-9]{0,4}[1-9][0-9]{0,4}` requires at least one non-zero
    // digit anywhere in the string. Leading zeros are explicitly allowed
    // (e.g. `"00001"` parses as the same five-character form AWS accepts).
    s.bytes().any(|b| b != b'0')
}

/// Validate path-bound `Id`-shape field. Empty/missing also rejected.
fn check_id(body: &Value, field: &str) -> Result<(), AwsServiceError> {
    let v = body
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| missing(field))?;
    if !is_valid_id(v) {
        return Err(validation(format!(
            "Value at '{field}' failed to satisfy constraint: Member must satisfy regular expression pattern: ^[0-9a-zA-Z]{{10}}$"
        )));
    }
    Ok(())
}

fn check_version(body: &Value, field: &str) -> Result<(), AwsServiceError> {
    let v = body
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| missing(field))?;
    if !is_valid_version(v) {
        return Err(validation(format!(
            "Value at '{field}' failed to satisfy constraint: Member must satisfy regular expression pattern: ^(DRAFT|[0-9]{{0,4}}[1-9][0-9]{{0,4}})$"
        )));
    }
    Ok(())
}

fn check_resource_arn(body: &Value, field: &str) -> Result<(), AwsServiceError> {
    let v = body
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| missing(field))?;
    let len = v.len();
    if !(20..=1011).contains(&len) {
        return Err(validation(format!(
            "Value at '{field}' failed to satisfy constraint: Member must have length between 20 and 1011, inclusive"
        )));
    }
    Ok(())
}

fn check_max_results(req: &AwsRequest, body: &Value) -> Result<(), AwsServiceError> {
    // `maxResults` lives in the query string for REST list ops, but the body
    // for body-only list ops (e.g. ListAgents, ListKnowledgeBases).
    let from_query = req.query_params.get("maxResults").map(|s| s.as_str());
    let from_body = body.get("maxResults");
    let parsed: Option<i64> = match (from_query, from_body) {
        (Some(s), _) => s.parse().ok(),
        (None, Some(v)) => v.as_i64(),
        _ => None,
    };
    if from_query.is_some() || from_body.is_some() {
        match parsed {
            Some(n) if (1..=1000).contains(&n) => Ok(()),
            _ => Err(validation(
                "Value at 'maxResults' failed to satisfy constraint: Member must be between 1 and 1000, inclusive",
            )),
        }
    } else {
        Ok(())
    }
}

fn check_next_token(req: &AwsRequest, body: &Value) -> Result<(), AwsServiceError> {
    let from_query = req.query_params.get("nextToken").map(|s| s.as_str());
    let from_body = body.get("nextToken").and_then(|v| v.as_str());
    let token = from_query.or(from_body);
    if let Some(raw) = token {
        let len = raw.len();
        if !(1..=2048).contains(&len) || raw.chars().any(|c| c.is_whitespace()) {
            return Err(validation(
                "Value at 'nextToken' failed to satisfy constraint: Member must have length between 1 and 2048, inclusive and match pattern ^\\S*$",
            ));
        }
    }
    Ok(())
}

fn check_tag_keys_required(req: &AwsRequest) -> Result<(), AwsServiceError> {
    // `tagKeys` is `@httpQuery("tagKeys")` and `@required` on UntagResource.
    // It can arrive as repeated `tagKeys=a&tagKeys=b` (only the last is in
    // `query_params`) — anything is fine, but the absence is a validation error.
    if !req.raw_query.split('&').any(|kv| {
        let k = kv.split_once('=').map(|(k, _)| k).unwrap_or(kv);
        k == "tagKeys"
    }) {
        return Err(missing("tagKeys"));
    }
    Ok(())
}

fn check_string_length(
    body: &Value,
    field: &str,
    min: usize,
    max: usize,
    required: bool,
) -> Result<(), AwsServiceError> {
    match body.get(field).and_then(|v| v.as_str()) {
        Some(v) => {
            let len = v.len();
            if !(min..=max).contains(&len) {
                return Err(validation(format!(
                    "Value at '{field}' failed to satisfy constraint: Member must have length between {min} and {max}, inclusive"
                )));
            }
            Ok(())
        }
        None => {
            if required {
                Err(missing(field))
            } else {
                Ok(())
            }
        }
    }
}

fn check_required_present(body: &Value, field: &str) -> Result<(), AwsServiceError> {
    if body.get(field).is_none() || body.get(field).map(|v| v.is_null()).unwrap_or(false) {
        return Err(missing(field));
    }
    Ok(())
}

fn validate_inputs(action: &str, req: &AwsRequest) -> Result<(), AwsServiceError> {
    let body = req.json_body();

    // Common pagination guards on every list/scan operation.
    let is_list = action.starts_with("List");
    if is_list {
        check_max_results(req, &body)?;
        check_next_token(req, &body)?;
    }

    match action {
        // Path-bound agentId only.
        "ListAgentAliases" | "ListAgentVersions" => {
            check_id(&body, "agentId")?;
        }
        // Path-bound agentId + agentVersion.
        "ListAgentActionGroups"
        | "ListAgentCollaborators"
        | "ListAgentKnowledgeBases"
        | "GetAgentActionGroup"
        | "GetAgentCollaborator"
        | "GetAgentKnowledgeBase" => {
            check_id(&body, "agentId")?;
            check_version(&body, "agentVersion")?;
        }
        // Path-bound knowledgeBaseId.
        "ListDataSources" => {
            check_id(&body, "knowledgeBaseId")?;
        }
        // Path-bound knowledgeBaseId + dataSourceId.
        "ListIngestionJobs" => {
            check_id(&body, "knowledgeBaseId")?;
            check_id(&body, "dataSourceId")?;
        }
        // Tag-resource family: validate ARN length/presence and (for Untag)
        // the required `tagKeys` query parameter.
        "TagResource" | "ListTagsForResource" => {
            check_resource_arn(&body, "resourceArn")?;
        }
        "UntagResource" => {
            check_resource_arn(&body, "resourceArn")?;
            check_tag_keys_required(req)?;
        }
        "CreateFlow" => {
            check_string_length(&body, "executionRoleArn", 1, 2048, true)?;
            check_string_length(&body, "clientToken", 33, 256, false)?;
            check_string_length(&body, "customerEncryptionKeyArn", 1, 2048, false)?;
            check_string_length(&body, "description", 1, 200, false)?;
        }
        "CreateKnowledgeBase" => {
            check_required_present(&body, "knowledgeBaseConfiguration")?;
            check_string_length(&body, "roleArn", 1, 2048, true)?;
            check_string_length(&body, "clientToken", 33, 256, false)?;
            check_string_length(&body, "description", 1, 200, false)?;
        }
        "CreatePrompt" => {
            check_string_length(&body, "clientToken", 33, 256, false)?;
            check_string_length(&body, "customerEncryptionKeyArn", 1, 2048, false)?;
            check_string_length(&body, "description", 1, 200, false)?;
        }
        _ => {}
    }

    Ok(())
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        msg.into(),
    )
}

fn conflict(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg.into())
}

fn opt_str(val: &Value, key: &str) -> Option<String> {
    val.get(key)?.as_str().map(|s| s.to_string())
}

fn req_str(val: &Value, key: &str) -> Result<String, AwsServiceError> {
    val.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| missing(key))
}

fn opt_i64(val: &Value, key: &str) -> Option<i64> {
    val.get(key)?.as_i64()
}

fn opt_json(val: &Value, key: &str) -> Option<Value> {
    val.get(key).cloned()
}

fn opt_array(val: &Value, key: &str) -> Vec<Value> {
    val.get(key)
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
}

fn now() -> DateTime<Utc> {
    Utc::now()
}

/// A resource identifier as a request names it.
enum Identifier<'a> {
    /// A bare resource ID.
    Id(&'a str),
    /// The resource part of a Bedrock ARN in the caller's account and region
    /// (`flow/<id>`, `prompt/<id>:<version>`, ...).
    Resource(&'a str),
}

/// Parse an identifier that is either a bare ID or a Bedrock ARN. An ARN that
/// is not Bedrock's, or that names another account, region or partition,
/// yields `None`: it can never resolve to a resource in this account's state.
fn parse_identifier<'a>(req: &AwsRequest, identifier: &'a str) -> Option<Identifier<'a>> {
    if !identifier.starts_with("arn:") {
        return Some(Identifier::Id(identifier));
    }
    let rest = fakecloud_aws::arn::arn_resource(identifier, "bedrock")?;
    let mut parts = rest.splitn(3, ':');
    let (region, account, resource) = (parts.next()?, parts.next()?, parts.next()?);
    let local = region == req.region
        && account == req.account_id
        && fakecloud_aws::arn::partition_of(identifier)
            == fakecloud_aws::arn::partition_for(region);
    local.then_some(Identifier::Resource(resource))
}

/// Mint the next version number for a resource whose highest minted number is
/// `latest` and whose live versions are `existing`. Numbers only grow, so a
/// deleted version's number is never reused; `existing` covers state persisted
/// before `latest` was tracked.
fn next_version<'a>(latest: &mut u64, existing: impl Iterator<Item = &'a str>) -> String {
    let highest_live = existing.filter_map(|v| v.parse::<u64>().ok()).max();
    *latest = (*latest).max(highest_live.unwrap_or(0)) + 1;
    latest.to_string()
}

fn short_id() -> String {
    // Smithy ResourceIdentifier shape is @pattern("^[0-9a-zA-Z]{10}$"): the
    // first 10 hex chars of a v4 UUID.
    fakecloud_core::ids::short_id(10)
}

fn agent_json(a: &Agent) -> Value {
    let mut o = json!({
        "agentId": a.agent_id,
        "agentName": a.agent_name,
        "agentArn": a.agent_arn,
        "agentVersion": a.agent_version,
        "agentStatus": a.agent_status,
        "idleSessionTTLInSeconds": a.idle_session_ttl_in_seconds,
        "agentResourceRoleArn": a.agent_resource_role_arn,
        "createdAt": a.created_at.to_rfc3339(),
        "updatedAt": a.updated_at.to_rfc3339(),
        "failureReasons": a.failure_reasons,
        "recommendedActions": a.recommended_actions,
    });
    if let Some(ref d) = a.description {
        o["description"] = json!(d);
    }
    if let Some(ref i) = a.instruction {
        o["instruction"] = json!(i);
    }
    if let Some(ref m) = a.foundation_model {
        o["foundationModel"] = json!(m);
    }
    if let Some(ref k) = a.customer_encryption_key_arn {
        o["customerEncryptionKeyArn"] = json!(k);
    }
    // AWS always returns a promptOverrideConfiguration: when the caller did
    // not override any prompts it still reports the four default prompt
    // configurations (promptCreationMode=DEFAULT). The terraform-provider-aws
    // agent reader dereferences `PromptOverrideConfiguration.PromptConfigurations`
    // unconditionally (removeDefaultPrompts), so the field must never be null.
    o["promptOverrideConfiguration"] = a
        .prompt_override_configuration
        .clone()
        .unwrap_or_else(default_prompt_override_configuration);
    // AWS always reports agentCollaboration; default DISABLED when unset.
    o["agentCollaboration"] = json!(if a.agent_collaboration.is_empty() {
        "DISABLED"
    } else {
        a.agent_collaboration.as_str()
    });
    if let Some(ref g) = a.guardrail_configuration {
        o["guardrailConfiguration"] = g.clone();
    }
    if let Some(ref p) = a.prepared_at {
        o["preparedAt"] = json!(p.to_rfc3339());
    }
    o
}

/// The default `promptOverrideConfiguration` AWS reports for an agent whose
/// caller overrode no prompts: one entry per prompt type, each in DEFAULT
/// creation mode. The terraform provider keeps only OVERRIDDEN entries, so
/// these are filtered out client-side and produce no diff.
fn default_prompt_override_configuration() -> Value {
    let default_prompt = |prompt_type: &str, enabled: bool| {
        json!({
            "promptType": prompt_type,
            "promptCreationMode": "DEFAULT",
            "promptState": if enabled { "ENABLED" } else { "DISABLED" },
            "parserMode": "DEFAULT",
        })
    };
    json!({
        "promptConfigurations": [
            default_prompt("PRE_PROCESSING", true),
            default_prompt("ORCHESTRATION", true),
            default_prompt("KNOWLEDGE_BASE_RESPONSE_GENERATION", true),
            default_prompt("POST_PROCESSING", false),
        ],
    })
}

fn action_group_json(ag: &AgentActionGroup) -> Value {
    let mut o = json!({
        "actionGroupId": ag.action_group_id,
        "agentId": ag.agent_id,
        "agentVersion": ag.agent_version,
        "actionGroupName": ag.action_group_name,
        "actionGroupState": ag.action_group_state,
        "createdAt": ag.created_at.to_rfc3339(),
        "updatedAt": ag.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = ag.description {
        o["description"] = json!(d);
    }
    if let Some(ref e) = ag.action_group_executor {
        o["actionGroupExecutor"] = e.clone();
    }
    if let Some(ref s) = ag.api_schema {
        o["apiSchema"] = s.clone();
    }
    if let Some(ref s) = ag.function_schema {
        o["functionSchema"] = s.clone();
    }
    if let Some(ref s) = ag.parent_action_group_signature {
        o["parentActionGroupSignature"] = json!(s);
    }
    o
}

fn action_group_summary_json(ag: &AgentActionGroup) -> Value {
    let mut o = json!({
        "actionGroupId": ag.action_group_id,
        "actionGroupName": ag.action_group_name,
        "actionGroupState": ag.action_group_state,
        "updatedAt": ag.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = ag.description {
        o["description"] = json!(d);
    }
    o
}

fn agent_version_json(v: &AgentVersion) -> Value {
    let mut o = json!({
        "agentVersion": v.agent_version,
        "agentId": v.agent_id,
        "agentName": v.agent_name,
        "createdAt": v.created_at.to_rfc3339(),
        "updatedAt": v.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = v.description {
        o["description"] = json!(d);
    }
    if let Some(ref i) = v.instruction {
        o["instruction"] = json!(i);
    }
    if let Some(ref m) = v.foundation_model {
        o["foundationModel"] = json!(m);
    }
    if let Some(ref g) = v.guardrail_configuration {
        o["guardrailConfiguration"] = g.clone();
    }
    if let Some(ref p) = v.prompt_override_configuration {
        o["promptOverrideConfiguration"] = p.clone();
    }
    o
}

fn alias_json(a: &AgentAlias) -> Value {
    let mut o = json!({
        "agentAliasId": a.alias_id,
        "agentAliasName": a.alias_name,
        "agentId": a.agent_id,
        "agentVersion": a.agent_version,
        "routingConfiguration": a.routing_configuration,
        "agentAliasArn": a.alias_arn,
        "agentAliasStatus": a.agent_alias_status,
        "failureReasons": a.failure_reasons,
        "createdAt": a.created_at.to_rfc3339(),
        "updatedAt": a.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = a.description {
        o["description"] = json!(d);
    }
    o
}

fn kb_json(k: &KnowledgeBase) -> Value {
    let mut o = json!({
        "knowledgeBaseId": k.knowledge_base_id,
        "name": k.name,
        "knowledgeBaseArn": k.knowledge_base_arn,
        "status": k.status,
        "roleArn": k.role_arn,
        "knowledgeBaseConfiguration": k.knowledge_base_configuration.clone(),
        "createdAt": k.created_at.to_rfc3339(),
        "updatedAt": k.updated_at.to_rfc3339(),
        "failureReasons": k.failure_reasons,
    });
    if let Some(ref d) = k.description {
        o["description"] = json!(d);
    }
    if let Some(ref s) = k.storage_configuration {
        o["storageConfiguration"] = s.clone();
    }
    o
}

fn data_source_json(d: &DataSource) -> Value {
    let mut o = json!({
        "dataSourceId": d.data_source_id,
        "name": d.name,
        "knowledgeBaseId": d.knowledge_base_id,
        "status": d.status,
        "createdAt": d.created_at.to_rfc3339(),
        "updatedAt": d.updated_at.to_rfc3339(),
        "failureReasons": d.failure_reasons,
    });
    if let Some(ref desc) = d.description {
        o["description"] = json!(desc);
    }
    if let Some(ref c) = d.data_source_configuration {
        o["dataSourceConfiguration"] = c.clone();
    }
    o
}

/// Build the `AgentSummary` shape used by `ListAgents`. The full `agent_json`
/// emits extra fields (agentArn, roleArn, etc.) that aren't on the Smithy
/// summary struct; surfacing them tripped strict-mode shape checks.
fn agent_summary_json(a: &Agent) -> Value {
    let mut o = json!({
        "agentId": a.agent_id,
        "agentName": a.agent_name,
        "agentStatus": a.agent_status,
        "updatedAt": a.updated_at.to_rfc3339(),
        "latestAgentVersion": a.agent_version,
    });
    if let Some(ref d) = a.description {
        o["description"] = json!(d);
    }
    if let Some(ref g) = a.guardrail_configuration {
        o["guardrailConfiguration"] = g.clone();
    }
    o
}

/// `FlowSummary` shape: requires `arn`, `id`, `name`, `status`, `createdAt`,
/// `updatedAt`, and `version`. The full `flow_json` adds `executionRoleArn`,
/// `customerEncryptionKeyArn`, and `definition`, none of which appear on the
/// summary.
fn flow_summary_json(f: &Flow) -> Value {
    let mut o = json!({
        "arn": f.arn,
        "id": f.flow_id,
        "name": f.name,
        "status": f.status,
        "createdAt": f.created_at.to_rfc3339(),
        "updatedAt": f.updated_at.to_rfc3339(),
        "version": f.version,
    });
    if let Some(ref d) = f.description {
        o["description"] = json!(d);
    }
    o
}

/// `KnowledgeBaseSummary`: only `knowledgeBaseId`, `name`, `status`,
/// `updatedAt` (plus optional `description`). The full record shape has
/// ARN, role, configuration, etc. that we strip here.
fn knowledge_base_summary_json(k: &KnowledgeBase) -> Value {
    let mut o = json!({
        "knowledgeBaseId": k.knowledge_base_id,
        "name": k.name,
        "status": k.status,
        "updatedAt": k.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = k.description {
        o["description"] = json!(d);
    }
    o
}

/// `PromptSummary` for a prompt's working draft: `arn`, `id`, `name`,
/// `version`, `createdAt`, `updatedAt`, and optional `description`. The full
/// `prompt_json` adds `variants`, `defaultVariant` and
/// `customerEncryptionKeyArn`, which the summary omits.
fn prompt_summary_json(p: &Prompt) -> Value {
    let mut o = json!({
        "arn": p.arn,
        "id": p.prompt_id,
        "name": p.name,
        "version": p.version,
        "createdAt": p.created_at.to_rfc3339(),
        "updatedAt": p.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = p.description {
        o["description"] = json!(d);
    }
    o
}

/// `GetFlowResponse` / `CreateFlowResponse` / `UpdateFlowResponse` members:
/// `name`, `description`, `executionRoleArn`, `customerEncryptionKeyArn`,
/// `id`, `arn`, `status`, `createdAt`, `updatedAt`, `version`, `definition`.
fn flow_json(f: &Flow) -> Value {
    let mut o = json!({
        "name": f.name,
        "id": f.flow_id,
        "arn": f.arn,
        "status": f.status,
        "createdAt": f.created_at.to_rfc3339(),
        "updatedAt": f.updated_at.to_rfc3339(),
        "version": f.version,
    });
    if let Some(ref d) = f.description {
        o["description"] = json!(d);
    }
    if let Some(ref r) = f.execution_role_arn {
        o["executionRoleArn"] = json!(r);
    }
    if let Some(ref k) = f.customer_encryption_key_arn {
        o["customerEncryptionKeyArn"] = json!(k);
    }
    if let Some(ref def) = f.definition {
        o["definition"] = def.clone();
    }
    if let Some(ref validations) = f.validations {
        o["validations"] = json!(validations);
    }
    o
}

/// `GetFlowVersionResponse` / `CreateFlowVersionResponse` members. `id` and
/// `arn` are the flow's (a flow version has no ARN of its own); the
/// flow-level fields come from the version snapshot, falling back to the flow
/// for versions persisted before they were captured.
fn flow_version_json(f: &Flow, v: &FlowVersion) -> Value {
    // A version's own snapshot is authoritative; only one persisted before the
    // snapshot was captured (`name` unset) falls back to the live flow.
    let legacy = v.name.is_none();
    let captured = |own: &Option<String>, live: &Option<String>| {
        if legacy {
            live.clone()
        } else {
            own.clone()
        }
    };
    let mut o = json!({
        "name": v.name.as_deref().unwrap_or(&f.name),
        "id": f.flow_id,
        "arn": f.arn,
        "status": v.status.as_deref().unwrap_or(&f.status),
        "createdAt": v.created_at.to_rfc3339(),
        "version": v.flow_version,
    });
    if let Some(ref d) = v.description {
        o["description"] = json!(d);
    }
    if let Some(r) = captured(&v.execution_role_arn, &f.execution_role_arn) {
        o["executionRoleArn"] = json!(r);
    }
    if let Some(k) = captured(
        &v.customer_encryption_key_arn,
        &f.customer_encryption_key_arn,
    ) {
        o["customerEncryptionKeyArn"] = json!(k);
    }
    if let Some(ref def) = v.definition {
        o["definition"] = def.clone();
    }
    o
}

/// `GetFlowAliasResponse` / `FlowAliasSummary` members: `name`,
/// `description`, `routingConfiguration`, `concurrencyConfiguration`,
/// `flowId`, `id`, `arn`, `createdAt`, `updatedAt`.
fn flow_alias_json(flow_arn: &str, a: &FlowAlias) -> Value {
    let mut o = json!({
        "name": a.alias_name,
        "flowId": a.flow_id,
        "id": a.alias_id,
        "arn": format!("{flow_arn}/alias/{}", a.alias_id),
        "routingConfiguration": a.routing_configuration,
        "createdAt": a.created_at.to_rfc3339(),
        "updatedAt": a.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = a.description {
        o["description"] = json!(d);
    }
    if let Some(ref c) = a.concurrency_configuration {
        o["concurrencyConfiguration"] = c.clone();
    }
    o
}

/// `GetPromptResponse` / `CreatePromptResponse` / `UpdatePromptResponse`
/// members for the working draft: `name`, `description`,
/// `customerEncryptionKeyArn`, `defaultVariant`, `variants`, `id`, `arn`,
/// `version`, `createdAt`, `updatedAt`.
fn prompt_json(p: &Prompt) -> Value {
    let mut o = json!({
        "name": p.name,
        "id": p.prompt_id,
        "arn": p.arn,
        "variants": p.variants,
        "version": p.version,
        "createdAt": p.created_at.to_rfc3339(),
        "updatedAt": p.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = p.description {
        o["description"] = json!(d);
    }
    if let Some(ref k) = p.customer_encryption_key_arn {
        o["customerEncryptionKeyArn"] = json!(k);
    }
    if let Some(ref dv) = p.default_variant {
        o["defaultVariant"] = json!(dv);
    }
    o
}

/// The same members for a numbered prompt version: its ARN is the prompt ARN
/// with a `:<version>` suffix. The version's own snapshot is authoritative; a
/// version persisted before the snapshot was captured (`name` unset) falls
/// back to the prompt.
fn prompt_version_json(p: &Prompt, v: &PromptVersion) -> Value {
    let legacy = v.name.is_none();
    let captured = |own: &Option<String>, live: &Option<String>| {
        if legacy {
            live.clone()
        } else {
            own.clone()
        }
    };
    let mut o = json!({
        "name": v.name.as_deref().unwrap_or(&p.name),
        "id": p.prompt_id,
        "arn": format!("{}:{}", p.arn, v.prompt_version),
        "variants": v.variants,
        "version": v.prompt_version,
        "createdAt": v.created_at.to_rfc3339(),
        "updatedAt": v.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = v.description {
        o["description"] = json!(d);
    }
    if let Some(k) = captured(
        &v.customer_encryption_key_arn,
        &p.customer_encryption_key_arn,
    ) {
        o["customerEncryptionKeyArn"] = json!(k);
    }
    if let Some(dv) = captured(&v.default_variant, &p.default_variant) {
        o["defaultVariant"] = json!(dv);
    }
    o
}

fn ingestion_job_json(j: &IngestionJob) -> Value {
    let mut o = json!({
        "ingestionJobId": j.ingestion_job_id,
        "knowledgeBaseId": j.knowledge_base_id,
        "dataSourceId": j.data_source_id,
        "status": j.status,
        "startedAt": j.started_at.to_rfc3339(),
        "updatedAt": j.updated_at.to_rfc3339(),
        "failureReasons": j.failure_reasons,
    });
    if let Some(ref d) = j.description {
        o["description"] = json!(d);
    }
    o
}

fn agent_kb_json(a: &AgentKnowledgeBase) -> Value {
    let mut o = json!({
        "agentId": a.agent_id,
        "knowledgeBaseId": a.knowledge_base_id,
        "knowledgeBaseState": a.knowledge_base_state,
        "createdAt": a.created_at.to_rfc3339(),
        "updatedAt": a.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = a.description {
        o["description"] = json!(d);
    }
    o
}

fn agent_collaborator_json(c: &AgentCollaborator) -> Value {
    let mut o = json!({
        "agentId": c.agent_id,
        "agentVersion": if c.agent_version.is_empty() { "DRAFT" } else { c.agent_version.as_str() },
        "collaboratorId": c.collaborator_id,
        "collaboratorName": c.collaborator_name,
        "collaborationInstruction": c.collaboration_instruction,
        "relayConversationHistory": c.relay_conversation_history,
        "createdAt": c.created_at.to_rfc3339(),
        "lastUpdatedAt": c.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = c.agent_descriptor {
        o["agentDescriptor"] = d.clone();
    }
    o
}

impl BedrockAgentService {}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::collections::HashMap;

    fn cn_request(body: Value) -> AwsRequest {
        AwsRequest {
            service: "bedrock-agent".to_string(),
            action: String::new(),
            region: "cn-north-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-id".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: body.to_string().into(),
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

    /// Send `method path[?query]` with a JSON body through the service's own
    /// REST routing, the way an SDK request arrives.
    async fn call(
        svc: &BedrockAgentService,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        payload: Value,
    ) -> (StatusCode, Value) {
        let resp = try_call(svc, method, path, query, payload).await.unwrap();
        let status = resp.status;
        (status, body(resp))
    }

    async fn try_call(
        svc: &BedrockAgentService,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        payload: Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut req = cn_request(payload);
        req.region = "us-east-1".to_string();
        req.method = method;
        req.raw_path = path.to_string();
        req.query_params = query
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        svc.handle(req).await
    }

    fn keys(v: &Value) -> Vec<&str> {
        let mut k: Vec<&str> = v.as_object().unwrap().keys().map(|k| k.as_str()).collect();
        k.sort_unstable();
        k
    }

    fn encode(arn: &str) -> String {
        arn.replace(':', "%3A").replace('/', "%2F")
    }

    /// The smallest flow that prepares cleanly: Input -> Output.
    fn valid_flow_definition() -> Value {
        json!({
            "nodes": [
                {"name": "FlowInputNode", "type": "Input", "configuration": {"input": {}},
                    "outputs": [{"name": "document", "type": "String"}]},
                {"name": "FlowOutputNode", "type": "Output", "configuration": {"output": {}},
                    "inputs": [{"name": "document", "type": "String", "expression": "$.data"}]}
            ],
            "connections": [
                {"name": "Link", "type": "Data", "source": "FlowInputNode", "target": "FlowOutputNode",
                    "configuration": {"data": {"sourceOutput": "document", "targetInput": "document"}}}
            ]
        })
    }

    fn validation_types(v: &Value) -> Vec<&str> {
        v["validations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["type"].as_str().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn version_numbers_are_never_reused_after_a_delete() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let (_, flow) = call(
            &svc,
            Method::POST,
            "/flows/",
            &[],
            json!({"name": "f", "executionRoleArn": "arn:aws:iam::123456789012:role/r",
                "definition": valid_flow_definition()}),
        )
        .await;
        let fid = flow["id"].as_str().unwrap().to_string();
        call(
            &svc,
            Method::POST,
            &format!("/flows/{fid}/"),
            &[],
            json!({}),
        )
        .await;
        let (_, prompt) = call(&svc, Method::POST, "/prompts/", &[], json!({"name": "p"})).await;
        let pid = prompt["id"].as_str().unwrap().to_string();
        let flow_versions = format!("/flows/{fid}/versions");
        let prompt_versions = format!("/prompts/{pid}/versions");
        let mint = |path: String| {
            let svc = &svc;
            async move {
                let (_, v) = call(svc, Method::POST, &path, &[], json!({})).await;
                v["version"].as_str().unwrap().to_string()
            }
        };

        assert_eq!(mint(flow_versions.clone()).await, "1");
        assert_eq!(mint(flow_versions.clone()).await, "2");
        call(
            &svc,
            Method::DELETE,
            &format!("/flows/{fid}/versions/1/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(mint(flow_versions.clone()).await, "3");
        // Deleting the highest version doesn't free its number either.
        call(
            &svc,
            Method::DELETE,
            &format!("/flows/{fid}/versions/3/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(mint(flow_versions).await, "4");

        assert_eq!(mint(prompt_versions.clone()).await, "1");
        assert_eq!(mint(prompt_versions.clone()).await, "2");
        let prompt_path = format!("/prompts/{pid}/");
        call(
            &svc,
            Method::DELETE,
            &prompt_path,
            &[("promptVersion", "1")],
            json!({}),
        )
        .await;
        assert_eq!(mint(prompt_versions.clone()).await, "3");
        call(
            &svc,
            Method::DELETE,
            &prompt_path,
            &[("promptVersion", "3")],
            json!({}),
        )
        .await;
        assert_eq!(mint(prompt_versions).await, "4");
    }

    #[tokio::test]
    async fn delete_prompt_rejects_a_non_numeric_version() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let (_, prompt) = call(&svc, Method::POST, "/prompts/", &[], json!({"name": "p"})).await;
        let path = format!("/prompts/{}/", prompt["id"].as_str().unwrap());
        for bad in ["DRAFT", "x1", "123456"] {
            let err = try_call(
                &svc,
                Method::DELETE,
                &path,
                &[("promptVersion", bad)],
                json!({}),
            )
            .await
            .err()
            .expect("non-numeric promptVersion must be rejected");
            assert_eq!(err.code(), "ValidationException", "{bad}");
        }
        // The prompt survived every rejected delete.
        let (status, got) = call(&svc, Method::GET, &path, &[], json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(got["id"], prompt["id"]);
        // GetPrompt's promptVersion does accept DRAFT: the working draft.
        let (_, draft) = call(
            &svc,
            Method::GET,
            &path,
            &[("promptVersion", "DRAFT")],
            json!({}),
        )
        .await;
        assert_eq!(draft, got);
    }

    #[tokio::test]
    async fn list_prompts_with_an_identifier_lists_that_prompts_versions() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let (_, p) = call(&svc, Method::POST, "/prompts/", &[], json!({"name": "p"})).await;
        call(
            &svc,
            Method::POST,
            "/prompts/",
            &[],
            json!({"name": "other"}),
        )
        .await;
        let id = p["id"].as_str().unwrap().to_string();
        let arn = p["arn"].as_str().unwrap().to_string();
        call(
            &svc,
            Method::POST,
            &format!("/prompts/{id}/versions"),
            &[],
            json!({}),
        )
        .await;

        let (_, all) = call(&svc, Method::GET, "/prompts/", &[], json!({})).await;
        assert_eq!(all["promptSummaries"].as_array().unwrap().len(), 2);

        for identifier in [id.as_str(), arn.as_str()] {
            let (_, versions) = call(
                &svc,
                Method::GET,
                "/prompts/",
                &[("promptIdentifier", identifier)],
                json!({}),
            )
            .await;
            let summaries = versions["promptSummaries"].as_array().unwrap();
            let listed: Vec<(&str, &str)> = summaries
                .iter()
                .map(|s| (s["version"].as_str().unwrap(), s["arn"].as_str().unwrap()))
                .collect();
            let v1_arn = format!("{arn}:1");
            assert_eq!(
                listed,
                vec![("DRAFT", arn.as_str()), ("1", v1_arn.as_str())]
            );
            assert_eq!(
                keys(&summaries[1]),
                vec!["arn", "createdAt", "id", "name", "updatedAt", "version"]
            );
        }

        let err = try_call(
            &svc,
            Method::GET,
            "/prompts/",
            &[("promptIdentifier", "NOSUCHID01")],
            json!({}),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");

        // The version routes the model doesn't have are gone.
        for (method, path) in [
            (Method::GET, format!("/prompts/{id}/versions")),
            (Method::GET, format!("/prompts/{id}/versions/1")),
        ] {
            let err = try_call(&svc, method, &path, &[], json!({}))
                .await
                .err()
                .unwrap();
            assert!(
                matches!(err, AwsServiceError::ActionNotImplemented { .. }),
                "{path}"
            );
        }
    }

    #[tokio::test]
    async fn prompt_version_keeps_the_key_it_was_created_with() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let k1 = "arn:aws:kms:us-east-1:123456789012:key/one";
        let k2 = "arn:aws:kms:us-east-1:123456789012:key/two";
        let (_, p) = call(
            &svc,
            Method::POST,
            "/prompts/",
            &[],
            json!({"name": "p", "customerEncryptionKeyArn": k1}),
        )
        .await;
        let id = p["id"].as_str().unwrap().to_string();
        call(
            &svc,
            Method::POST,
            &format!("/prompts/{id}/versions"),
            &[],
            json!({}),
        )
        .await;
        call(
            &svc,
            Method::PUT,
            &format!("/prompts/{id}/"),
            &[],
            json!({"name": "p", "customerEncryptionKeyArn": k2}),
        )
        .await;
        let path = format!("/prompts/{id}/");
        let (_, v1) = call(
            &svc,
            Method::GET,
            &path,
            &[("promptVersion", "1")],
            json!({}),
        )
        .await;
        assert_eq!(v1["customerEncryptionKeyArn"], k1);
        let (_, draft) = call(&svc, Method::GET, &path, &[], json!({})).await;
        assert_eq!(draft["customerEncryptionKeyArn"], k2);
    }

    #[tokio::test]
    async fn arn_identifiers_must_name_this_account_and_region() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let (_, flow) = call(
            &svc,
            Method::POST,
            "/flows/",
            &[],
            json!({"name": "f", "executionRoleArn": "arn:aws:iam::123456789012:role/r"}),
        )
        .await;
        let (_, prompt) = call(&svc, Method::POST, "/prompts/", &[], json!({"name": "p"})).await;
        let fid = flow["id"].as_str().unwrap();
        let pid = prompt["id"].as_str().unwrap();
        call(
            &svc,
            Method::POST,
            &format!("/prompts/{pid}/versions"),
            &[],
            json!({}),
        )
        .await;

        // The local ARNs (URL-encoded, as an SDK sends them) resolve.
        let (_, by_arn) = call(
            &svc,
            Method::GET,
            &format!("/flows/{}/", encode(flow["arn"].as_str().unwrap())),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(by_arn["id"], fid);
        let pinned = format!("{}:1", prompt["arn"].as_str().unwrap());
        let (_, v1) = call(
            &svc,
            Method::GET,
            &format!("/prompts/{}/", encode(&pinned)),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(v1["version"], "1");
        assert_eq!(v1["arn"], pinned);

        // The same IDs under another account, region or partition do not.
        for foreign in [
            format!("arn:aws:bedrock:us-east-1:999999999999:flow/{fid}"),
            format!("arn:aws:bedrock:eu-west-1:123456789012:flow/{fid}"),
            format!("arn:aws-cn:bedrock:us-east-1:123456789012:flow/{fid}"),
            format!("arn:aws:lambda:us-east-1:123456789012:flow/{fid}"),
        ] {
            let err = try_call(
                &svc,
                Method::GET,
                &format!("/flows/{}/", encode(&foreign)),
                &[],
                json!({}),
            )
            .await
            .err()
            .unwrap();
            assert_eq!(err.code(), "ResourceNotFoundException", "{foreign}");
        }
        let foreign_prompt = format!("arn:aws:bedrock:us-east-1:999999999999:prompt/{pid}");
        let err = try_call(
            &svc,
            Method::DELETE,
            &format!("/prompts/{}/", encode(&foreign_prompt)),
            &[],
            json!({}),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
        let (status, _) = call(
            &svc,
            Method::GET,
            &format!("/prompts/{pid}/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the foreign-ARN delete must not touch it"
        );

        // An alias ARN must belong to the flow it is addressed under.
        let (_, alias) = call(
            &svc,
            Method::POST,
            &format!("/flows/{fid}/aliases"),
            &[],
            json!({"name": "a", "routingConfiguration": []}),
        )
        .await;
        let alias_arn = alias["arn"].as_str().unwrap();
        let (_, got) = call(
            &svc,
            Method::GET,
            &format!("/flows/{fid}/aliases/{}", encode(alias_arn)),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(got["id"], alias["id"]);
        let other_flow = alias_arn.replace(fid, "OTHERFLOW1");
        let err = try_call(
            &svc,
            Method::GET,
            &format!("/flows/{fid}/aliases/{}", encode(&other_flow)),
            &[],
            json!({}),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn validate_flow_definition_accepts_one_node_feeding_two_inputs() {
        // The AWS sample: the flow input's document feeds both the `genre`
        // and the `number` input of one prompt node.
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let data = |name: &str, source: &str, target: &str, out: &str, input: &str| {
            json!({"name": name, "source": source, "target": target, "type": "Data",
                "configuration": {"data": {"sourceOutput": out, "targetInput": input}}})
        };
        let nodes = json!([
            {"name": "FlowInput", "type": "Input",
                "outputs": [{"name": "document", "type": "Object"}]},
            {"name": "MakePlaylist", "type": "Prompt",
                "inputs": [
                    {"name": "genre", "type": "String", "expression": "$.data.genre"},
                    {"name": "number", "type": "Number", "expression": "$.data.number"}
                ],
                "outputs": [{"name": "modelCompletion", "type": "String"}]},
            {"name": "FlowOutput", "type": "Output",
                "inputs": [{"name": "document", "type": "String", "expression": "$.data"}]}
        ]);
        let mut connections = vec![
            data("c1", "FlowInput", "MakePlaylist", "document", "genre"),
            data("c2", "FlowInput", "MakePlaylist", "document", "number"),
            data(
                "c3",
                "MakePlaylist",
                "FlowOutput",
                "modelCompletion",
                "document",
            ),
        ];
        let (_, ok) = call(
            &svc,
            Method::POST,
            "/flows/validate-definition",
            &[],
            json!({"definition": {"nodes": nodes, "connections": connections}}),
        )
        .await;
        assert_eq!(ok, json!({"validations": []}));

        // A true duplicate (same output into the same input) is still flagged.
        connections.push(data("c4", "FlowInput", "MakePlaylist", "document", "genre"));
        let (_, dup) = call(
            &svc,
            Method::POST,
            "/flows/validate-definition",
            &[],
            json!({"definition": {"nodes": nodes, "connections": connections}}),
        )
        .await;
        assert_eq!(
            validation_types(&dup),
            vec!["MultipleNodeInputConnections", "DuplicateConnections"]
        );
    }

    #[tokio::test]
    async fn validate_flow_definition_refuses_duplicate_and_reports_missing_node_names() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let input = json!({"name": "In", "type": "Input",
            "outputs": [{"name": "document", "type": "String"}]});
        // Two nodes sharing a name make the definition unaddressable: it is
        // refused outright, by ValidateFlowDefinition and CreateFlow alike.
        let shared = json!({"nodes": [input, {"name": "In", "type": "Output"}]});
        for path in ["/flows/validate-definition", "/flows/"] {
            let err = try_call(
                &svc,
                Method::POST,
                path,
                &[],
                json!({"name": "f", "executionRoleArn": "arn:aws:iam::123456789012:role/r", "definition": shared}),
            )
            .await
            .err()
            .unwrap();
            assert_eq!(err.code(), "ValidationException", "{path}");
            assert_eq!(err.message(), "Node name In must be unique", "{path}");
        }

        let (_, got) = call(
            &svc,
            Method::POST,
            "/flows/validate-definition",
            &[],
            json!({"definition": {
                "nodes": [input, {"type": "Output", "inputs": []}],
                "connections": []
            }}),
        )
        .await;
        let unspecified: Vec<&Value> = got["validations"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|v| v["type"] == "Unspecified")
            .collect();
        assert_eq!(unspecified.len(), 1, "{got}");
        assert_eq!(unspecified[0]["message"], "Node at index 1 has no name.");
        assert_eq!(unspecified[0]["severity"], "Error");
        assert_eq!(unspecified[0]["details"], json!({"unspecified": {}}));
        // The unnamed Output node can't count as the flow's ending node.
        assert!(validation_types(&got).contains(&"MissingEndingNodes"));
    }

    #[tokio::test]
    async fn flow_responses_carry_the_model_output_members() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let (status, created) = call(
            &svc,
            Method::POST,
            "/flows/",
            &[],
            json!({
                "name": "f",
                "description": "d",
                "executionRoleArn": "arn:aws:iam::123456789012:role/r",
                "customerEncryptionKeyArn": "arn:aws:kms:us-east-1:123456789012:key/k",
                "definition": valid_flow_definition(),
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let id = created["id"].as_str().unwrap().to_string();
        let arn = format!("arn:aws:bedrock:us-east-1:123456789012:flow/{id}");
        assert_eq!(created["arn"], arn);
        let flow_members = vec![
            "arn",
            "createdAt",
            "customerEncryptionKeyArn",
            "definition",
            "description",
            "executionRoleArn",
            "id",
            "name",
            "status",
            "updatedAt",
            "version",
        ];
        assert_eq!(keys(&created), flow_members);

        // GetFlow by ID and by ARN returns the same top-level shape.
        let (_, got) = call(&svc, Method::GET, &format!("/flows/{id}/"), &[], json!({})).await;
        assert_eq!(keys(&got), flow_members);
        assert_eq!(got["id"], id);
        assert_eq!(got["arn"], arn);
        assert_eq!(got["status"], "NotPrepared");
        assert!(got.get("flowId").is_none() && got.get("flow").is_none());
        let encoded_arn = arn.replace(':', "%3A").replace('/', "%2F");
        let (_, by_arn) = call(
            &svc,
            Method::GET,
            &format!("/flows/{encoded_arn}/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(by_arn["id"], id);

        let (status, prepared) =
            call(&svc, Method::POST, &format!("/flows/{id}/"), &[], json!({})).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(prepared, json!({"id": id, "status": "Preparing"}));
        // A prepared flow reports its (here empty) validation findings.
        let (_, got) = call(&svc, Method::GET, &format!("/flows/{id}/"), &[], json!({})).await;
        assert_eq!(got["status"], "Prepared");
        assert_eq!(got["validations"], json!([]));
        let mut prepared_members = flow_members.clone();
        prepared_members.push("validations");
        prepared_members.sort_unstable();
        assert_eq!(keys(&got), prepared_members);

        let (_, updated) = call(
            &svc,
            Method::PUT,
            &format!("/flows/{id}/"),
            &[],
            json!({"name": "f2", "executionRoleArn": "arn:aws:iam::123456789012:role/r"}),
        )
        .await;
        assert_eq!(keys(&updated), flow_members);
        assert_eq!(updated["name"], "f2");
        assert_eq!(updated["arn"], arn);
        assert_eq!(updated["status"], "NotPrepared");
        call(&svc, Method::POST, &format!("/flows/{id}/"), &[], json!({})).await;

        let (status, version) = call(
            &svc,
            Method::POST,
            &format!("/flows/{id}/versions"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(version["id"], id);
        assert_eq!(version["arn"], arn);
        assert_eq!(version["version"], "1");
        assert_eq!(version["name"], "f2");
        let (_, got_version) = call(
            &svc,
            Method::GET,
            &format!("/flows/{id}/versions/1/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(got_version, version);

        let (status, alias) = call(
            &svc,
            Method::POST,
            &format!("/flows/{id}/aliases"),
            &[],
            json!({"name": "live", "routingConfiguration": [{"flowVersion": "1"}]}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let alias_id = alias["id"].as_str().unwrap().to_string();
        assert_eq!(alias["flowId"], id);
        assert_eq!(alias["arn"], format!("{arn}/alias/{alias_id}"));
        assert_eq!(
            keys(&alias),
            vec![
                "arn",
                "createdAt",
                "flowId",
                "id",
                "name",
                "routingConfiguration",
                "updatedAt"
            ]
        );
        let (_, got_alias) = call(
            &svc,
            Method::GET,
            &format!("/flows/{id}/aliases/{alias_id}"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(got_alias, alias);

        let (_, deleted_alias) = call(
            &svc,
            Method::DELETE,
            &format!("/flows/{id}/aliases/{alias_id}"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(deleted_alias, json!({"flowId": id, "id": alias_id}));
        let (_, deleted_version) = call(
            &svc,
            Method::DELETE,
            &format!("/flows/{id}/versions/1/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(deleted_version, json!({"id": id, "version": "1"}));
        let (_, deleted) = call(
            &svc,
            Method::DELETE,
            &format!("/flows/{id}/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(deleted, json!({"id": id}));
    }

    #[tokio::test]
    async fn prepare_flow_validates_the_definition() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let role = "arn:aws:iam::123456789012:role/r";
        let create = |definition: Option<Value>| {
            let svc = &svc;
            async move {
                let mut body = json!({"name": "f", "executionRoleArn": role});
                if let Some(d) = definition {
                    body["definition"] = d;
                }
                let (_, flow) = call(svc, Method::POST, "/flows/", &[], body).await;
                flow["id"].as_str().unwrap().to_string()
            }
        };
        let prepare = |id: String| {
            let svc = &svc;
            async move {
                let (status, out) =
                    call(svc, Method::POST, &format!("/flows/{id}/"), &[], json!({})).await;
                assert_eq!(status, StatusCode::ACCEPTED);
                assert_eq!(out, json!({"id": id, "status": "Preparing"}));
                let (_, got) =
                    call(svc, Method::GET, &format!("/flows/{id}/"), &[], json!({})).await;
                got
            }
        };

        // A flow whose connection leads to a node that doesn't exist fails.
        let mut broken = valid_flow_definition();
        broken["connections"].as_array_mut().unwrap().push(json!({
            "name": "ToGhost", "type": "Data", "source": "FlowInputNode", "target": "Ghost",
            "configuration": {"data": {"sourceOutput": "document", "targetInput": "document"}}
        }));
        let id = create(Some(broken)).await;
        let (_, unprepared) =
            call(&svc, Method::GET, &format!("/flows/{id}/"), &[], json!({})).await;
        assert!(unprepared.get("validations").is_none(), "{unprepared}");
        let got = prepare(id.clone()).await;
        assert_eq!(got["status"], "Failed");
        assert_eq!(
            got["validations"],
            json!([{
                "message": "The connection between the nodes 'FlowInputNode' and 'Ghost' references an unknown target node 'Ghost'. Update the connection to reference a valid node that exists in the flow, or add the missing node to the flow.",
                "severity": "Error",
                "details": {"unknownConnectionTarget": {"connection": "ToGhost"}},
                "type": "UnknownConnectionTarget"
            }])
        );
        // A failed draft can't be versioned, but is listed as the DRAFT.
        let err = try_call(
            &svc,
            Method::POST,
            &format!("/flows/{id}/versions"),
            &[],
            json!({}),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(err.code(), "ConflictException");
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(
            err.message(),
            "Couldn't perform CreateFlowVersion operation. The resource that you specified is currently in status Failed. Retry your request after the resource changes to a valid status."
        );
        let (_, listed) = call(
            &svc,
            Method::GET,
            &format!("/flows/{id}/versions"),
            &[],
            json!({}),
        )
        .await;
        let summaries = listed["flowVersionSummaries"].as_array().unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0]["version"], "DRAFT");
        assert_eq!(summaries[0]["status"], "Failed");

        // Fixing the draft clears the findings until it is prepared again.
        let (_, updated) = call(
            &svc,
            Method::PUT,
            &format!("/flows/{id}/"),
            &[],
            json!({"name": "f", "executionRoleArn": role, "definition": valid_flow_definition()}),
        )
        .await;
        assert_eq!(updated["status"], "NotPrepared");
        let (_, got) = call(&svc, Method::GET, &format!("/flows/{id}/"), &[], json!({})).await;
        assert!(got.get("validations").is_none(), "{got}");
        let err = try_call(
            &svc,
            Method::POST,
            &format!("/flows/{id}/versions"),
            &[],
            json!({}),
        )
        .await
        .err()
        .unwrap();
        assert!(err.message().contains("currently in status NotPrepared"));
        let got = prepare(id.clone()).await;
        assert_eq!(got["status"], "Prepared");
        assert_eq!(got["validations"], json!([]));
        let (status, _) = call(
            &svc,
            Method::POST,
            &format!("/flows/{id}/versions"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        // Warnings alone (no Output node) still prepare.
        let mut input_only = valid_flow_definition();
        input_only["nodes"].as_array_mut().unwrap().pop();
        input_only["connections"] = json!([]);
        let got = prepare(create(Some(input_only)).await).await;
        assert_eq!(got["status"], "Prepared");
        assert_eq!(got["validations"][0]["type"], "MissingEndingNodes");
        assert_eq!(got["validations"][0]["severity"], "Warning");

        // An empty definition fails; no definition at all can't be prepared.
        let got = prepare(create(Some(json!({}))).await).await;
        assert_eq!(got["status"], "Failed");
        assert_eq!(
            validation_types(&got),
            vec!["MissingStartingNodes", "MissingEndingNodes"]
        );
        let bare = create(None).await;
        let err = try_call(
            &svc,
            Method::POST,
            &format!("/flows/{bare}/"),
            &[],
            json!({}),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(err.code(), "ValidationException");
        assert_eq!(
            err.message(),
            format!("You can\u{2019}t prepare flow {bare} because its definition is empty. Update your flow with a definition and retry your request.")
        );
        let (_, got) = call(
            &svc,
            Method::GET,
            &format!("/flows/{bare}/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(got["status"], "NotPrepared");

        // A stored definition holds at most one Input node.
        let mut two_inputs = valid_flow_definition();
        two_inputs["nodes"].as_array_mut().unwrap().push(json!({
            "name": "Other", "type": "Input",
            "outputs": [{"name": "document", "type": "String"}]
        }));
        for (method, path) in [
            (Method::POST, "/flows/".to_string()),
            (Method::PUT, format!("/flows/{id}/")),
        ] {
            let err = try_call(
                &svc,
                method,
                &path,
                &[],
                json!({"name": "f", "executionRoleArn": role, "definition": two_inputs}),
            )
            .await
            .err()
            .unwrap();
            assert_eq!(err.code(), "ValidationException", "{path}");
            assert!(
                err.message().starts_with(
                    "The limit for max-number-flow-input-nodes is 1, but the requested value is 2"
                ),
                "{}",
                err.message()
            );
        }

        // An alias routes to a version, never to the draft.
        let err = try_call(
            &svc,
            Method::POST,
            &format!("/flows/{id}/aliases"),
            &[],
            json!({"name": "live", "routingConfiguration": [{"flowVersion": "DRAFT"}]}),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(err.code(), "ValidationException");
        assert_eq!(
            err.message(),
            "Alias routing configuration must point to a non draft version. Retry your request with a valid version."
        );
    }

    #[tokio::test]
    async fn validate_flow_definition_reports_structural_problems() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let input = json!({"name": "In", "type": "Input",
            "outputs": [{"name": "document", "type": "String"}]});
        let output = json!({"name": "Out", "type": "Output",
            "inputs": [{"name": "document", "type": "String", "expression": "$.data"}]});
        let link = |name: &str, source: &str, target: &str| {
            json!({"name": name, "source": source, "target": target, "type": "Data",
                "configuration": {"data": {"sourceOutput": "document", "targetInput": "document"}}})
        };

        // A well-formed Input -> Output flow validates clean. The route must
        // not be swallowed by PrepareFlow's POST /flows/{flowIdentifier}.
        let (status, ok) = call(
            &svc,
            Method::POST,
            "/flows/validate-definition",
            &[],
            json!({"definition": {"nodes": [input, output],
                "connections": [link("c1", "In", "Out")]}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(ok, json!({"validations": []}));

        // No Output node, a connection to a missing node, an unreachable node.
        let stray = json!({"name": "Stray", "type": "Prompt", "inputs": [], "outputs": []});
        let (_, bad) = call(
            &svc,
            Method::POST,
            "/flows/validate-definition",
            &[],
            json!({"definition": {"nodes": [input, stray],
                "connections": [link("c1", "In", "Ghost")]}}),
        )
        .await;
        let types: Vec<&str> = bad["validations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            vec![
                "MissingEndingNodes",
                "UnknownConnectionTarget",
                "UnreachableNode"
            ]
        );
        assert_eq!(
            bad["validations"][1]["details"],
            json!({"unknownConnectionTarget": {"connection": "c1"}})
        );
        assert_eq!(bad["validations"][2]["severity"], "Warning");
    }

    #[tokio::test]
    async fn validation_refuses_a_node_two_unnested_iterators_repeat() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let data = |name: &str, source: &str, output: &str, target: &str, input: &str| {
            json!({"name": name, "type": "Data", "source": source, "target": target,
                "configuration": {"data": {"sourceOutput": output, "targetInput": input}}})
        };
        let iterator = |name: &str| {
            json!({"name": name, "type": "Iterator",
                "inputs": [{"name": "array", "type": "Array", "expression": "$.data"}],
                "outputs": [{"name": "arrayItem", "type": "String"},
                            {"name": "arraySize", "type": "Number"}]})
        };
        let definition = json!({
            "nodes": [
                {"name": "In", "type": "Input", "outputs": [{"name": "document", "type": "Array"}]},
                iterator("One"),
                iterator("Two"),
                {"name": "Shared", "type": "Prompt",
                 "inputs": [{"name": "a", "type": "String", "expression": "$.data"},
                            {"name": "b", "type": "String", "expression": "$.data"}],
                 "outputs": [{"name": "modelCompletion", "type": "String"}]},
                {"name": "Out", "type": "Output",
                 "inputs": [{"name": "document", "type": "String", "expression": "$.data"}]}
            ],
            "connections": [
                data("c1", "In", "document", "One", "array"),
                data("c2", "In", "document", "Two", "array"),
                data("c3", "One", "arrayItem", "Shared", "a"),
                data("c4", "Two", "arrayItem", "Shared", "b"),
                data("c5", "Shared", "modelCompletion", "Out", "document")
            ]
        });
        let (status, got) = call(
            &svc,
            Method::POST,
            "/flows/validate-definition",
            &[],
            json!({ "definition": definition }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let validations = got["validations"].as_array().unwrap();
        assert_eq!(validations.len(), 1, "{validations:?}");
        assert_eq!(validations[0]["severity"], "Error");
        assert_eq!(validations[0]["type"], "Unspecified");
        let message = validations[0]["message"].as_str().unwrap();
        assert!(
            message.contains("Shared") && message.contains("One") && message.contains("Two"),
            "{message}"
        );
    }

    #[tokio::test]
    async fn prompt_responses_carry_the_model_output_members() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let variants = json!([{"name": "v1", "templateType": "TEXT",
            "templateConfiguration": {"text": {"text": "hi"}}}]);
        let (status, created) = call(
            &svc,
            Method::POST,
            "/prompts/",
            &[],
            json!({"name": "p", "defaultVariant": "v1", "variants": variants}),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let id = created["id"].as_str().unwrap().to_string();
        let arn = format!("arn:aws:bedrock:us-east-1:123456789012:prompt/{id}");
        let prompt_members = vec![
            "arn",
            "createdAt",
            "defaultVariant",
            "id",
            "name",
            "updatedAt",
            "variants",
            "version",
        ];

        let (_, got) = call(
            &svc,
            Method::GET,
            &format!("/prompts/{id}/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(keys(&got), prompt_members);
        assert_eq!(got["id"], id);
        assert_eq!(got["arn"], arn);
        assert_eq!(got["version"], "DRAFT");
        assert_eq!(got["defaultVariant"], "v1");
        assert!(got.get("promptId").is_none() && got.get("prompt").is_none());

        let (_, updated) = call(
            &svc,
            Method::PUT,
            &format!("/prompts/{id}/"),
            &[],
            json!({"name": "p2", "variants": variants}),
        )
        .await;
        assert_eq!(keys(&updated), prompt_members);
        assert_eq!(updated["name"], "p2");
        assert_eq!(updated["arn"], arn);

        let (_, version) = call(
            &svc,
            Method::POST,
            &format!("/prompts/{id}/versions"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(version["arn"], format!("{arn}:1"));
        // GetPrompt with ?promptVersion= returns that version.
        let (_, got_version) = call(
            &svc,
            Method::GET,
            &format!("/prompts/{id}/"),
            &[("promptVersion", "1")],
            json!({}),
        )
        .await;
        assert_eq!(got_version, version);
        assert_eq!(got_version["version"], "1");
        assert_eq!(got_version["name"], "p2");

        let (_, deleted_version) = call(
            &svc,
            Method::DELETE,
            &format!("/prompts/{id}/"),
            &[("promptVersion", "1")],
            json!({}),
        )
        .await;
        assert_eq!(deleted_version, json!({"id": id, "version": "1"}));
        let (_, deleted) = call(
            &svc,
            Method::DELETE,
            &format!("/prompts/{id}/"),
            &[],
            json!({}),
        )
        .await;
        assert_eq!(deleted, json!({"id": id}));
    }

    #[test]
    fn listed_flow_and_prompt_arns_are_the_ones_they_were_created_with() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let flow = body(svc.create_flow(&cn_request(json!({"name": "f"}))).unwrap());
        let prompt = body(
            svc.create_prompt(&cn_request(json!({"name": "p"})))
                .unwrap(),
        );
        let version = body(
            svc.create_prompt_version(&cn_request(
                json!({"promptIdentifier": prompt["id"].clone()}),
            ))
            .unwrap(),
        );
        assert_eq!(
            version["arn"],
            format!("{}:1", prompt["arn"].as_str().unwrap())
        );

        let mut elsewhere = cn_request(json!({}));
        elsewhere.region = "us-east-1".to_string();
        let flows = body(svc.list_flows(&elsewhere).unwrap());
        assert_eq!(flows["flowSummaries"][0]["arn"], flow["arn"]);
        let prompts = body(svc.list_prompts(&elsewhere).unwrap());
        assert_eq!(prompts["promptSummaries"][0]["arn"], prompt["arn"]);
        elsewhere.query_params.insert(
            "promptIdentifier".to_string(),
            prompt["id"].as_str().unwrap().to_string(),
        );
        let versions = body(svc.list_prompts(&elsewhere).unwrap());
        assert_eq!(versions["promptSummaries"][1]["arn"], version["arn"]);
    }

    #[tokio::test]
    async fn loading_a_snapshot_rekeys_percent_encoded_tag_arns() {
        let arn = "arn:aws:bedrock:us-east-1:123456789012:flow/ABCDEFGHIJ";
        let other = "arn:aws:bedrock:us-east-1:123456789012:prompt/KLMNOPQRST";
        // An older build stored TagResource-over-the-wire tags under the
        // still-encoded path label. One ARN has only the encoded entry; the
        // other has both, the decoded one written later by a newer build.
        let raw = json!({"accounts": {"123456789012": {
            "account_id": "123456789012",
            "region": "us-east-1",
            "agents": {}, "agent_aliases": {}, "agent_versions": {},
            "knowledge_bases": {}, "data_sources": {}, "agent_knowledge_bases": {},
            "agent_collaborators": {}, "flows": {}, "flow_aliases": {},
            "flow_versions": {}, "prompts": {}, "prompt_versions": {},
            "ingestion_jobs": {},
            "tags": {
                encode(arn): {"team": "a", "env": "dev"},
                encode(other): {"team": "old", "cost": "1"},
                other: {"team": "new"}
            }
        }}});
        let loaded: BedrockAgentAccounts = serde_json::from_value(raw).unwrap();
        let tags = &loaded.get("123456789012").unwrap().tags;
        assert_eq!(
            tags.keys().map(String::as_str).collect::<Vec<_>>(),
            vec![arn, other],
            "every encoded key is re-keyed, none is left behind"
        );
        assert_eq!(tags[other]["team"], "new", "the later decoded value wins");
        assert_eq!(tags[other]["cost"], "1", "encoded-only keys are merged in");

        let svc = BedrockAgentService::new(Arc::new(RwLock::new(loaded)));
        let path = format!("/tags/{}", encode(arn));
        let (_, listed) = call(&svc, Method::GET, &path, &[], json!({})).await;
        assert_eq!(listed, json!({"tags": {"team": "a", "env": "dev"}}));

        let mut untag = cn_request(json!({}));
        untag.region = "us-east-1".to_string();
        untag.method = Method::DELETE;
        untag.raw_path = path.clone();
        untag.raw_query = "tagKeys=team&tagKeys=env".to_string();
        svc.handle(untag).await.unwrap();
        let (_, listed) = call(&svc, Method::GET, &path, &[], json!({})).await;
        assert_eq!(listed, json!({"tags": {}}));
    }

    #[test]
    fn loading_a_snapshot_without_arns_backfills_them_from_the_state_region() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));
        let flow = body(svc.create_flow(&cn_request(json!({"name": "f"}))).unwrap());
        let prompt = body(
            svc.create_prompt(&cn_request(json!({"name": "p"})))
                .unwrap(),
        );

        // A snapshot written before flows and prompts stored their ARN.
        let mut raw = serde_json::to_value(&*svc.state.read()).unwrap();
        let account = &mut raw["accounts"]["123456789012"];
        for collection in ["flows", "prompts"] {
            for record in account[collection].as_object_mut().unwrap().values_mut() {
                record.as_object_mut().unwrap().remove("arn");
            }
        }
        let loaded: BedrockAgentAccounts = serde_json::from_value(raw).unwrap();
        let state = loaded.get("123456789012").unwrap();
        let flow_id = flow["id"].as_str().unwrap();
        let prompt_id = prompt["id"].as_str().unwrap();
        assert_eq!(state.flows[flow_id].arn, flow["arn"].as_str().unwrap());
        assert_eq!(
            state.prompts[prompt_id].arn,
            prompt["arn"].as_str().unwrap()
        );
    }

    #[test]
    fn china_region_resources_and_default_roles_use_aws_cn_partition() {
        let svc = BedrockAgentService::new(Arc::new(RwLock::new(BedrockAgentAccounts::new())));

        let agent = body(
            svc.create_agent(&cn_request(json!({"agentName": "a"})))
                .unwrap(),
        );
        let agent = &agent["agent"];
        let agent_id = agent["agentId"].as_str().unwrap();
        assert_eq!(
            agent["agentArn"],
            format!("arn:aws-cn:bedrock:cn-north-1:123456789012:agent/{agent_id}")
        );
        assert_eq!(
            agent["agentResourceRoleArn"],
            "arn:aws-cn:iam::123456789012:role/fakecloud-bedrock-agent-role"
        );

        let kb = body(
            svc.create_knowledge_base(&cn_request(json!({"name": "kb"})))
                .unwrap(),
        );
        let kb = &kb["knowledgeBase"];
        assert!(kb["knowledgeBaseArn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws-cn:bedrock:cn-north-1:123456789012:knowledge-base/"));
        assert_eq!(
            kb["roleArn"],
            "arn:aws-cn:iam::123456789012:role/fakecloud-bedrock-kb-role"
        );

        let flow = body(svc.create_flow(&cn_request(json!({"name": "f"}))).unwrap());
        let flow_id = flow["id"].as_str().unwrap();
        assert_eq!(
            flow["arn"],
            format!("arn:aws-cn:bedrock:cn-north-1:123456789012:flow/{flow_id}")
        );
        assert_eq!(
            flow["executionRoleArn"],
            format!(
                "arn:aws-cn:iam::123456789012:role/service-role/AmazonBedrockExecutionRoleForFlows_{flow_id}"
            )
        );

        let prompt = body(
            svc.create_prompt(&cn_request(json!({"name": "p"})))
                .unwrap(),
        );
        assert!(prompt["arn"]
            .as_str()
            .unwrap()
            .starts_with("arn:aws-cn:bedrock:cn-north-1:123456789012:prompt/"));
    }
}
