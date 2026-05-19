use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fakecloud_aws::arn::Arn;
use http::{Method, StatusCode};
use parking_lot::RwLock;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    Agent, AgentAlias, AgentCollaborator, AgentKnowledgeBase, AgentVersion, BedrockAgentAccounts,
    DataSource, Flow, FlowAlias, FlowVersion, IngestionJob, KnowledgeBase, Prompt, PromptVersion,
    SharedBedrockAgentState,
};

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
}

impl BedrockAgentService {
    pub fn new(state: SharedBedrockAgentState) -> Self {
        Self { state }
    }

    pub fn shared_state(&self) -> SharedBedrockAgentState {
        Arc::clone(&self.state)
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
        if segs.len() == 2
            && segs[0] == "flows"
            && segs[1] == "validate-definition"
            && *m == Method::POST
        {
            return Some(("ValidateFlowDefinition", params));
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
            if *m == Method::GET {
                return Some(("ListPromptVersions", params));
            }
        }
        if segs.len() == 4 && segs[0] == "prompts" && segs[2] == "versions" {
            params.push(("promptIdentifier".to_string(), segs[1].clone()));
            params.push(("promptVersion".to_string(), segs[3].clone()));
            if *m == Method::GET {
                return Some(("GetPromptVersion", params));
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
            for (k, v) in path_params {
                body[k] = serde_json::Value::String(v);
            }
            req.body = serde_json::to_vec(&body).unwrap_or_default().into();
        }

        validate_inputs(action, &req)?;

        match action {
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
        }
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

fn short_id() -> String {
    // Smithy ResourceIdentifier shape is @pattern("^[0-9a-zA-Z]{10}$").
    // Use the first 10 hex chars of a v4 UUID (alphanumeric, deterministic
    // length, no dashes).
    let s = uuid::Uuid::new_v4().simple().to_string();
    s[..10].to_string()
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
    if let Some(ref p) = a.prompt_override_configuration {
        o["promptOverrideConfiguration"] = p.clone();
    }
    if let Some(ref g) = a.guardrail_configuration {
        o["guardrailConfiguration"] = g.clone();
    }
    if let Some(ref p) = a.prepared_at {
        o["preparedAt"] = json!(p.to_rfc3339());
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
/// `updatedAt`, and `version`. The full `flow_json` exposes `flowId`,
/// `executionRoleArn`, and `definition`, none of which appear on the summary.
fn flow_summary_json(f: &Flow, region: &str, account_id: &str) -> Value {
    let mut o = json!({
        "arn": flow_arn(&f.flow_id, region, account_id),
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

/// `PromptSummary`: `arn`, `id`, `name`, `version`, `createdAt`, `updatedAt`.
/// The full prompt JSON keys `promptId` (not `id`) and surfaces `variants`.
fn prompt_summary_json(p: &Prompt, region: &str, account_id: &str) -> Value {
    let mut o = json!({
        "arn": prompt_arn(&p.prompt_id, region, account_id),
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

fn flow_arn(flow_id: &str, region: &str, account_id: &str) -> String {
    Arn::new("bedrock", region, account_id, &format!("flow/{flow_id}")).to_string()
}

fn prompt_arn(prompt_id: &str, region: &str, account_id: &str) -> String {
    Arn::new(
        "bedrock",
        region,
        account_id,
        &format!("prompt/{prompt_id}"),
    )
    .to_string()
}

fn flow_json(f: &Flow) -> Value {
    let mut o = json!({
        "flowId": f.flow_id,
        "name": f.name,
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
    if let Some(ref def) = f.definition {
        o["definition"] = def.clone();
    }
    o
}

fn flow_version_json(v: &FlowVersion) -> Value {
    let mut o = json!({
        "flowVersion": v.flow_version,
        "flowId": v.flow_id,
        "createdAt": v.created_at.to_rfc3339(),
        "updatedAt": v.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = v.description {
        o["description"] = json!(d);
    }
    if let Some(ref def) = v.definition {
        o["definition"] = def.clone();
    }
    o
}

fn flow_alias_json(a: &FlowAlias) -> Value {
    let mut o = json!({
        "aliasId": a.alias_id,
        "aliasName": a.alias_name,
        "flowId": a.flow_id,
        "routingConfiguration": a.routing_configuration,
        "createdAt": a.created_at.to_rfc3339(),
        "updatedAt": a.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = a.description {
        o["description"] = json!(d);
    }
    o
}

fn prompt_json(p: &Prompt) -> Value {
    let mut o = json!({
        "promptId": p.prompt_id,
        "name": p.name,
        "variants": p.variants,
        "version": p.version,
        "createdAt": p.created_at.to_rfc3339(),
        "updatedAt": p.updated_at.to_rfc3339(),
    });
    if let Some(ref d) = p.description {
        o["description"] = json!(d);
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
    json!({
        "agentId": c.agent_id,
        "collaboratorId": c.collaborator_id,
        "collaboratorName": c.collaborator_name,
        "collaboratorAliasArn": c.collaborator_alias_arn,
        "relayConversationHistory": c.relay_conversation_history,
        "createdAt": c.created_at.to_rfc3339(),
        "updatedAt": c.updated_at.to_rfc3339(),
    })
}

impl BedrockAgentService {
    fn create_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "agentName")?;
        let id = short_id();
        let now_dt = now();
        let agent = Agent {
            agent_id: id.clone(),
            agent_name: name,
            agent_arn: format!(
                "arn:aws:bedrock:{}:{}:agent/{}",
                req.region, req.account_id, id
            ),
            agent_version: "DRAFT".to_string(),
            agent_resource_role_arn: opt_str(&body, "agentResourceRoleArn").unwrap_or_else(|| {
                format!(
                    "arn:aws:iam::{}:role/fakecloud-bedrock-agent-role",
                    req.account_id
                )
            }),
            description: opt_str(&body, "description"),
            instruction: opt_str(&body, "instruction"),
            foundation_model: opt_str(&body, "foundationModel"),
            idle_session_ttl_in_seconds: opt_i64(&body, "idleSessionTTLInSeconds").unwrap_or(1800),
            customer_encryption_key_arn: opt_str(&body, "customerEncryptionKeyArn"),
            prompt_override_configuration: opt_json(&body, "promptOverrideConfiguration"),
            guardrail_configuration: opt_json(&body, "guardrailConfiguration"),
            agent_status: "NOT_PREPARED".to_string(),
            prepared_at: None,
            created_at: now_dt,
            updated_at: now_dt,
            failure_reasons: Vec::new(),
            recommended_actions: Vec::new(),
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if state
            .agents
            .values()
            .any(|a| a.agent_name == agent.agent_name)
        {
            return Err(conflict(format!(
                "Agent with name {} already exists",
                agent.agent_name
            )));
        }
        state.agents.insert(id.clone(), agent);
        let a = state.agents.get(&id).unwrap();
        Ok(AwsResponse::ok_json(json!({ "agent": agent_json(a) })))
    }

    fn get_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        let a = state
            .agents
            .get(&id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "agent": agent_json(a) })))
    }

    fn list_agents(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| s.agents.values().map(agent_summary_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "agentSummaries": list })))
    }

    fn update_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "agentId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .agents
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        a.updated_at = now();
        if let Some(n) = opt_str(&body, "agentName") {
            a.agent_name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if let Some(i) = opt_str(&body, "instruction") {
            a.instruction = Some(i);
        }
        if let Some(m) = opt_str(&body, "foundationModel") {
            a.foundation_model = Some(m);
        }
        if let Some(t) = opt_i64(&body, "idleSessionTTLInSeconds") {
            a.idle_session_ttl_in_seconds = t;
        }
        if let Some(k) = opt_str(&body, "customerEncryptionKeyArn") {
            a.customer_encryption_key_arn = Some(k);
        }
        if body.get("promptOverrideConfiguration").is_some() {
            a.prompt_override_configuration = opt_json(&body, "promptOverrideConfiguration");
        }
        if body.get("guardrailConfiguration").is_some() {
            a.guardrail_configuration = opt_json(&body, "guardrailConfiguration");
        }
        Ok(AwsResponse::ok_json(json!({ "agent": agent_json(a) })))
    }

    fn delete_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "agentId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state
            .agents
            .remove(&id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        state.agent_versions.remove(&id);
        state.agent_knowledge_bases.remove(&id);
        state.agent_collaborators.remove(&id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn prepare_agent(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "agentId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .agents
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Agent {id} not found")))?;
        a.agent_status = "PREPARED".to_string();
        a.prepared_at = Some(now());
        a.updated_at = now();
        Ok(AwsResponse::ok_json(json!({
            "agentId": id,
            "agentStatus": "PREPARED",
            "agentVersion": "DRAFT",
            "preparedAt": a.prepared_at.as_ref().unwrap().to_rfc3339(),
        })))
    }

    fn create_agent_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let name = req_str(&body, "agentAliasName")?;
        let alias_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        let alias = AgentAlias {
            alias_id: alias_id.clone(),
            alias_name: name.clone(),
            agent_id: agent_id.clone(),
            agent_version: opt_str(&body, "agentVersion").unwrap_or_else(|| "DRAFT".to_string()),
            routing_configuration: opt_array(&body, "routingConfiguration"),
            description: opt_str(&body, "description"),
            alias_arn: format!(
                "arn:aws:bedrock:{}:{}:agent-alias/{}/{}",
                req.region, req.account_id, agent_id, alias_id
            ),
            agent_alias_status: "PREPARED".to_string(),
            failure_reasons: Vec::new(),
            created_at: now_dt,
            updated_at: now_dt,
        };
        state.agent_aliases.insert(alias_id.clone(), alias);
        let a = state.agent_aliases.get(&alias_id).unwrap();
        Ok(AwsResponse::ok_json(json!({ "agentAlias": alias_json(a) })))
    }

    fn get_agent_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let alias_id = req_str(&body, "agentAliasId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Alias {alias_id} not found")))?;
        let a = state
            .agent_aliases
            .get(&alias_id)
            .filter(|a| a.agent_id == agent_id)
            .ok_or_else(|| not_found(format!("Alias {alias_id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "agentAlias": alias_json(a) })))
    }

    fn list_agent_aliases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.agent_aliases
                    .values()
                    .filter(|a| a.agent_id == agent_id)
                    .map(alias_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "agentAliasSummaries": list })))
    }

    fn update_agent_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let alias_id = req_str(&body, "agentAliasId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .agent_aliases
            .get_mut(&alias_id)
            .filter(|a| a.agent_id == agent_id)
            .ok_or_else(|| not_found(format!("Alias {alias_id} not found")))?;
        a.updated_at = now();
        if let Some(n) = opt_str(&body, "agentAliasName") {
            a.alias_name = n;
        }
        if let Some(v) = opt_str(&body, "agentVersion") {
            a.agent_version = v;
        }
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if body.get("routingConfiguration").is_some() {
            a.routing_configuration = opt_array(&body, "routingConfiguration");
        }
        Ok(AwsResponse::ok_json(json!({ "agentAlias": alias_json(a) })))
    }

    fn delete_agent_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let alias_id = req_str(&body, "agentAliasId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let removed = state
            .agent_aliases
            .remove(&alias_id)
            .filter(|a| a.agent_id == agent_id)
            .is_some();
        if !removed {
            return Err(not_found(format!("Alias {alias_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_agent_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let version = req_str(&body, "agentVersion")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Version {version} not found")))?;
        let v = state
            .agent_versions
            .get(&agent_id)
            .and_then(|vec| vec.iter().find(|v| v.agent_version == version))
            .ok_or_else(|| not_found(format!("Version {version} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "agentVersion": agent_version_json(v) }),
        ))
    }

    fn list_agent_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.agent_versions.get(&agent_id))
            .map(|vec| vec.iter().map(agent_version_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "agentVersionSummaries": list }),
        ))
    }

    fn delete_agent_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let version = req_str(&body, "agentVersion")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let vec = state
            .agent_versions
            .get_mut(&agent_id)
            .ok_or_else(|| not_found(format!("Version {version} not found")))?;
        let pos = vec
            .iter()
            .position(|v| v.agent_version == version)
            .ok_or_else(|| not_found(format!("Version {version} not found")))?;
        vec.remove(pos);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn create_knowledge_base(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "name")?;
        let id = short_id();
        let now_dt = now();
        let kb = KnowledgeBase {
            knowledge_base_id: id.clone(),
            name,
            knowledge_base_arn: format!(
                "arn:aws:bedrock:{}:{}:knowledge-base/{}",
                req.region, req.account_id, id
            ),
            description: opt_str(&body, "description"),
            role_arn: opt_str(&body, "roleArn").unwrap_or_else(|| {
                format!(
                    "arn:aws:iam::{}:role/fakecloud-bedrock-kb-role",
                    req.account_id
                )
            }),
            knowledge_base_configuration: opt_json(&body, "knowledgeBaseConfiguration")
                .unwrap_or_else(|| json!({"type": "VECTOR"})),
            storage_configuration: opt_json(&body, "storageConfiguration"),
            status: "ACTIVE".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
            failure_reasons: Vec::new(),
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state.knowledge_bases.insert(id.clone(), kb);
        let k = state.knowledge_bases.get(&id).unwrap();
        Ok(AwsResponse::ok_json(json!({ "knowledgeBase": kb_json(k) })))
    }

    fn get_knowledge_base(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {id} not found")))?;
        let k = state
            .knowledge_bases
            .get(&id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "knowledgeBase": kb_json(k) })))
    }

    fn list_knowledge_bases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.knowledge_bases
                    .values()
                    .map(knowledge_base_summary_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "knowledgeBaseSummaries": list }),
        ))
    }

    fn update_knowledge_base(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let k = state
            .knowledge_bases
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {id} not found")))?;
        k.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            k.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            k.description = Some(d);
        }
        if let Some(r) = opt_str(&body, "roleArn") {
            k.role_arn = r;
        }
        if body.get("knowledgeBaseConfiguration").is_some() {
            k.knowledge_base_configuration =
                opt_json(&body, "knowledgeBaseConfiguration").unwrap_or_else(|| json!({}));
        }
        if body.get("storageConfiguration").is_some() {
            k.storage_configuration = opt_json(&body, "storageConfiguration");
        }
        Ok(AwsResponse::ok_json(json!({ "knowledgeBase": kb_json(k) })))
    }

    fn delete_knowledge_base(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state
            .knowledge_bases
            .remove(&id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {id} not found")))?;
        state
            .data_sources
            .retain(|_, ds| ds.knowledge_base_id != id);
        state
            .ingestion_jobs
            .retain(|_, jobs| !jobs.iter().any(|j| j.knowledge_base_id == id));
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn create_data_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let name = req_str(&body, "name")?;
        let id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        let ds = DataSource {
            data_source_id: id.clone(),
            name,
            description: opt_str(&body, "description"),
            knowledge_base_id: kb_id,
            data_source_configuration: opt_json(&body, "dataSourceConfiguration"),
            status: "ACTIVE".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
            failure_reasons: Vec::new(),
        };
        state.data_sources.insert(id.clone(), ds);
        Ok(AwsResponse::ok_json(json!({
            "dataSource": {
                "dataSourceId": id,
                "status": "ACTIVE",
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    fn get_data_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("DataSource {ds_id} not found")))?;
        let ds = state
            .data_sources
            .get(&ds_id)
            .filter(|d| d.knowledge_base_id == kb_id)
            .ok_or_else(|| not_found(format!("DataSource {ds_id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "dataSource": data_source_json(ds) }),
        ))
    }

    fn list_data_sources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.data_sources
                    .values()
                    .filter(|d| d.knowledge_base_id == kb_id)
                    .map(data_source_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "dataSourceSummaries": list })))
    }

    fn update_data_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let ds = state
            .data_sources
            .get_mut(&ds_id)
            .filter(|d| d.knowledge_base_id == kb_id)
            .ok_or_else(|| not_found(format!("DataSource {ds_id} not found")))?;
        ds.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            ds.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            ds.description = Some(d);
        }
        if body.get("dataSourceConfiguration").is_some() {
            ds.data_source_configuration = opt_json(&body, "dataSourceConfiguration");
        }
        Ok(AwsResponse::ok_json(
            json!({ "dataSource": data_source_json(ds) }),
        ))
    }

    fn delete_data_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let removed = state
            .data_sources
            .remove(&ds_id)
            .filter(|d| d.knowledge_base_id == kb_id)
            .is_some();
        if !removed {
            return Err(not_found(format!("DataSource {ds_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn start_ingestion_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let job_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        let job = IngestionJob {
            ingestion_job_id: job_id.clone(),
            knowledge_base_id: kb_id.clone(),
            data_source_id: ds_id,
            description: opt_str(&body, "description"),
            status: "COMPLETE".to_string(),
            failure_reasons: Vec::new(),
            started_at: now_dt,
            updated_at: now_dt,
        };
        state.ingestion_jobs.entry(kb_id).or_default().push(job);
        Ok(AwsResponse::ok_json(json!({
            "ingestionJob": {
                "ingestionJobId": job_id,
                "status": "COMPLETE",
                "startedAt": now_dt.to_rfc3339(),
                "updatedAt": now_dt.to_rfc3339(),
            }
        })))
    }

    fn get_ingestion_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let job_id = req_str(&body, "ingestionJobId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("IngestionJob {job_id} not found")))?;
        let job = state
            .ingestion_jobs
            .get(&kb_id)
            .and_then(|jobs| {
                jobs.iter()
                    .find(|j| j.ingestion_job_id == job_id && j.data_source_id == ds_id)
            })
            .ok_or_else(|| not_found(format!("IngestionJob {job_id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ingestionJob": ingestion_job_json(job) }),
        ))
    }

    fn list_ingestion_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.ingestion_jobs.get(&kb_id))
            .map(|jobs| {
                jobs.iter()
                    .filter(|j| j.data_source_id == ds_id)
                    .map(ingestion_job_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "ingestionJobSummaries": list }),
        ))
    }

    fn stop_ingestion_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let ds_id = req_str(&body, "dataSourceId")?;
        let job_id = req_str(&body, "ingestionJobId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let jobs = state
            .ingestion_jobs
            .get_mut(&kb_id)
            .ok_or_else(|| not_found(format!("IngestionJob {job_id} not found")))?;
        let job = jobs
            .iter_mut()
            .find(|j| j.ingestion_job_id == job_id && j.data_source_id == ds_id)
            .ok_or_else(|| not_found(format!("IngestionJob {job_id} not found")))?;
        job.status = "STOPPED".to_string();
        job.updated_at = now();
        Ok(AwsResponse::ok_json(
            json!({ "ingestionJob": ingestion_job_json(job) }),
        ))
    }

    fn ingest_knowledge_base_documents(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_knowledge_base_documents(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_knowledge_base_documents(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {kb_id} not found")))?;
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "documentDetails": [],
        })))
    }

    fn list_knowledge_base_documents(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("KnowledgeBase {kb_id} not found")))?;
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "documentDetails": [],
        })))
    }

    fn create_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "name")?;
        let id = short_id();
        let now_dt = now();
        // executionRoleArn is required by the Smithy model; synthesize a
        // plausible value when the caller omits one so the response still
        // satisfies the required shape.
        let role_arn = opt_str(&body, "executionRoleArn").unwrap_or_else(|| {
            format!(
                "arn:aws:iam::{}:role/service-role/AmazonBedrockExecutionRoleForFlows_{id}",
                req.account_id
            )
        });
        let arn = flow_arn(&id, &req.region, &req.account_id);
        let definition = opt_json(&body, "definition");
        let flow = Flow {
            flow_id: id.clone(),
            name: name.clone(),
            description: opt_str(&body, "description"),
            execution_role_arn: Some(role_arn.clone()),
            status: "NotPrepared".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
            version: "DRAFT".to_string(),
            definition: definition.clone(),
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state.flows.insert(id.clone(), flow);
        let mut out = json!({
            "name": name,
            "executionRoleArn": role_arn,
            "id": id,
            "arn": arn,
            "status": "NotPrepared",
            "createdAt": now_dt.to_rfc3339(),
            "updatedAt": now_dt.to_rfc3339(),
            "version": "DRAFT",
        });
        if let Some(d) = opt_str(&body, "description") {
            out["description"] = json!(d);
        }
        if let Some(k) = opt_str(&body, "customerEncryptionKeyArn") {
            out["customerEncryptionKeyArn"] = json!(k);
        }
        if let Some(def) = definition {
            out["definition"] = def;
        }
        Ok(AwsResponse::ok_json(out))
    }

    fn get_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "flowId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        let f = state
            .flows
            .get(&id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "flow": flow_json(f) })))
    }

    fn list_flows(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.flows
                    .values()
                    .map(|f| flow_summary_json(f, &req.region, &req.account_id))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "flowSummaries": list })))
    }

    fn update_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "flowId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let f = state
            .flows
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        f.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            f.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            f.description = Some(d);
        }
        if let Some(r) = opt_str(&body, "executionRoleArn") {
            f.execution_role_arn = Some(r);
        }
        if body.get("definition").is_some() {
            f.definition = opt_json(&body, "definition");
        }
        Ok(AwsResponse::ok_json(json!({ "flow": flow_json(f) })))
    }

    fn delete_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "flowId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state
            .flows
            .remove(&id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        state.flow_versions.remove(&id);
        state.flow_aliases.retain(|_, a| a.flow_id != id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn prepare_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "flowId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let f = state
            .flows
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Flow {id} not found")))?;
        f.status = "PREPARED".to_string();
        f.updated_at = now();
        Ok(AwsResponse::ok_json(json!({
            "flowId": id,
            "status": "PREPARED",
        })))
    }

    fn create_flow_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let flow = state
            .flows
            .get(&flow_id)
            .ok_or_else(|| not_found(format!("Flow {flow_id} not found")))?;
        let versions = state.flow_versions.entry(flow_id.clone()).or_default();
        let version_num = (versions.len() as u64 + 1).to_string();
        let fv = FlowVersion {
            flow_version: version_num.clone(),
            flow_id: flow_id.clone(),
            description: opt_str(&body, "description"),
            created_at: now_dt,
            updated_at: now_dt,
            definition: flow.definition.clone(),
        };
        versions.push(fv);
        Ok(AwsResponse::ok_json(json!({
            "flowVersion": {
                "flowVersion": version_num,
                "flowId": flow_id,
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    fn get_flow_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let version = req_str(&body, "flowVersion")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        let v = state
            .flow_versions
            .get(&flow_id)
            .and_then(|vec| vec.iter().find(|v| v.flow_version == version))
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "flowVersion": flow_version_json(v) }),
        ))
    }

    fn list_flow_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.flow_versions.get(&flow_id))
            .map(|vec| vec.iter().map(flow_version_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "flowVersionSummaries": list }),
        ))
    }

    fn delete_flow_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let version = req_str(&body, "flowVersion")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let vec = state
            .flow_versions
            .get_mut(&flow_id)
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        let pos = vec
            .iter()
            .position(|v| v.flow_version == version)
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        vec.remove(pos);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn validate_flow_definition(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _flow_id = req_str(&body, "flowId")?;
        Ok(AwsResponse::ok_json(json!({
            "isValid": true,
            "validationDetails": [],
        })))
    }

    fn create_flow_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let name = req_str(&body, "name")?;
        let alias_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.flows.contains_key(&flow_id) {
            return Err(not_found(format!("Flow {flow_id} not found")));
        }
        let alias = FlowAlias {
            alias_id: alias_id.clone(),
            alias_name: name.clone(),
            flow_id: flow_id.clone(),
            routing_configuration: opt_array(&body, "routingConfiguration"),
            description: opt_str(&body, "description"),
            created_at: now_dt,
            updated_at: now_dt,
        };
        state.flow_aliases.insert(alias_id.clone(), alias);
        Ok(AwsResponse::ok_json(json!({
            "flowAlias": {
                "aliasId": alias_id,
                "aliasName": name,
                "flowId": flow_id,
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    fn get_flow_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let alias_id = req_str(&body, "aliasId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Flow alias {alias_id} not found")))?;
        let a = state
            .flow_aliases
            .get(&alias_id)
            .filter(|a| a.flow_id == flow_id)
            .ok_or_else(|| not_found(format!("Flow alias {alias_id} not found")))?;
        Ok(AwsResponse::ok_json(
            json!({ "flowAlias": flow_alias_json(a) }),
        ))
    }

    fn list_flow_aliases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.flow_aliases
                    .values()
                    .filter(|a| a.flow_id == flow_id)
                    .map(flow_alias_json)
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "flowAliasSummaries": list })))
    }

    fn update_flow_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let alias_id = req_str(&body, "aliasId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .flow_aliases
            .get_mut(&alias_id)
            .filter(|a| a.flow_id == flow_id)
            .ok_or_else(|| not_found(format!("Flow alias {alias_id} not found")))?;
        a.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            a.alias_name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if body.get("routingConfiguration").is_some() {
            a.routing_configuration = opt_array(&body, "routingConfiguration");
        }
        Ok(AwsResponse::ok_json(
            json!({ "flowAlias": flow_alias_json(a) }),
        ))
    }

    fn delete_flow_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = req_str(&body, "flowId")?;
        let alias_id = req_str(&body, "aliasId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let removed = state
            .flow_aliases
            .remove(&alias_id)
            .filter(|a| a.flow_id == flow_id)
            .is_some();
        if !removed {
            return Err(not_found(format!("Flow alias {alias_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn create_prompt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "name")?;
        let id = short_id();
        let now_dt = now();
        let variants = opt_array(&body, "variants");
        let arn = prompt_arn(&id, &req.region, &req.account_id);
        let prompt = Prompt {
            prompt_id: id.clone(),
            name: name.clone(),
            description: opt_str(&body, "description"),
            variants: variants.clone(),
            version: "DRAFT".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state.prompts.insert(id.clone(), prompt);
        let mut out = json!({
            "name": name,
            "id": id,
            "arn": arn,
            "version": "DRAFT",
            "createdAt": now_dt.to_rfc3339(),
            "updatedAt": now_dt.to_rfc3339(),
            "variants": variants,
        });
        if let Some(d) = opt_str(&body, "description") {
            out["description"] = json!(d);
        }
        if let Some(k) = opt_str(&body, "customerEncryptionKeyArn") {
            out["customerEncryptionKeyArn"] = json!(k);
        }
        if let Some(dv) = opt_str(&body, "defaultVariant") {
            out["defaultVariant"] = json!(dv);
        }
        Ok(AwsResponse::ok_json(out))
    }

    fn get_prompt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "promptId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        let p = state
            .prompts
            .get(&id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        Ok(AwsResponse::ok_json(json!({ "prompt": prompt_json(p) })))
    }

    fn create_prompt_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // The routing layer surfaces the prompt identifier (path segment) into
        // the body under `promptIdentifier`, so we read it back here. The
        // resulting version is numbered incrementally per the Smithy contract.
        let body = req.json_body();
        let id = req_str(&body, "promptIdentifier")?;
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let prompt = state
            .prompts
            .get(&id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?
            .clone();
        let versions = state.prompt_versions.entry(id.clone()).or_default();
        let version_num = (versions.len() as u64 + 1).to_string();
        let pv = PromptVersion {
            prompt_version: version_num.clone(),
            prompt_id: id.clone(),
            description: opt_str(&body, "description"),
            created_at: now_dt,
            updated_at: now_dt,
            variants: prompt.variants.clone(),
        };
        versions.push(pv);
        let arn = format!(
            "{}:{version_num}",
            prompt_arn(&id, &req.region, &req.account_id)
        );
        let mut out = json!({
            "name": prompt.name,
            "id": id,
            "arn": arn,
            "version": version_num,
            "createdAt": now_dt.to_rfc3339(),
            "updatedAt": now_dt.to_rfc3339(),
            "variants": prompt.variants,
        });
        if let Some(d) = opt_str(&body, "description").or(prompt.description) {
            out["description"] = json!(d);
        }
        Ok(AwsResponse::json_value(StatusCode::CREATED, out))
    }

    fn list_prompts(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| {
                s.prompts
                    .values()
                    .map(|p| prompt_summary_json(p, &req.region, &req.account_id))
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "promptSummaries": list })))
    }

    fn update_prompt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "promptId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let p = state
            .prompts
            .get_mut(&id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        p.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            p.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            p.description = Some(d);
        }
        if body.get("variants").is_some() {
            p.variants = opt_array(&body, "variants");
        }
        Ok(AwsResponse::ok_json(json!({ "prompt": prompt_json(p) })))
    }

    fn delete_prompt(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = req_str(&body, "promptId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state
            .prompts
            .remove(&id)
            .ok_or_else(|| not_found(format!("Prompt {id} not found")))?;
        state.prompt_versions.remove(&id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn associate_agent_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        if !state.knowledge_bases.contains_key(&kb_id) {
            return Err(not_found(format!("KnowledgeBase {kb_id} not found")));
        }
        let list = state
            .agent_knowledge_bases
            .entry(agent_id.clone())
            .or_default();
        if list.iter().any(|a| a.knowledge_base_id == kb_id) {
            return Err(conflict(format!(
                "KnowledgeBase {kb_id} already associated with agent {agent_id}"
            )));
        }
        list.push(AgentKnowledgeBase {
            agent_id: agent_id.clone(),
            knowledge_base_id: kb_id.clone(),
            description: opt_str(&body, "description"),
            knowledge_base_state: opt_str(&body, "knowledgeBaseState")
                .unwrap_or_else(|| "ENABLED".to_string()),
            created_at: now_dt,
            updated_at: now_dt,
        });
        Ok(AwsResponse::ok_json(json!({
            "agentKnowledgeBase": {
                "agentId": agent_id,
                "knowledgeBaseId": kb_id,
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    fn disassociate_agent_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let list = state
            .agent_knowledge_bases
            .get_mut(&agent_id)
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        let pos = list
            .iter()
            .position(|a| a.knowledge_base_id == kb_id)
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        list.remove(pos);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_agent_knowledge_base(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        let a = state
            .agent_knowledge_bases
            .get(&agent_id)
            .and_then(|list| list.iter().find(|a| a.knowledge_base_id == kb_id))
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        Ok(AwsResponse::ok_json(
            json!({ "agentKnowledgeBase": agent_kb_json(a) }),
        ))
    }

    fn list_agent_knowledge_bases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.agent_knowledge_bases.get(&agent_id))
            .map(|list| list.iter().map(agent_kb_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "agentKnowledgeBaseSummaries": list }),
        ))
    }

    fn update_agent_knowledge_base(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let kb_id = req_str(&body, "knowledgeBaseId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let a = state
            .agent_knowledge_bases
            .get_mut(&agent_id)
            .and_then(|list| list.iter_mut().find(|a| a.knowledge_base_id == kb_id))
            .ok_or_else(|| not_found("Association not found".to_string()))?;
        a.updated_at = now();
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if let Some(s) = opt_str(&body, "knowledgeBaseState") {
            a.knowledge_base_state = s;
        }
        Ok(AwsResponse::ok_json(
            json!({ "agentKnowledgeBase": agent_kb_json(a) }),
        ))
    }

    fn associate_agent_collaborator(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let collaborator_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        let coll = AgentCollaborator {
            agent_id: agent_id.clone(),
            collaborator_id: collaborator_id.clone(),
            collaborator_name: req_str(&body, "collaboratorName")?,
            collaborator_alias_arn: opt_str(&body, "collaboratorAliasArn").unwrap_or_default(),
            relay_conversation_history: opt_str(&body, "relayConversationHistory")
                .unwrap_or_else(|| "DISABLED".to_string()),
            created_at: now_dt,
            updated_at: now_dt,
        };
        state
            .agent_collaborators
            .entry(agent_id.clone())
            .or_default()
            .push(coll);
        Ok(AwsResponse::ok_json(json!({
            "agentCollaborator": {
                "agentId": agent_id,
                "collaboratorId": collaborator_id,
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    fn disassociate_agent_collaborator(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let collaborator_id = req_str(&body, "collaboratorId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let list = state
            .agent_collaborators
            .get_mut(&agent_id)
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        let pos = list
            .iter()
            .position(|c| c.collaborator_id == collaborator_id)
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        list.remove(pos);
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn get_agent_collaborator(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let collaborator_id = req_str(&body, "collaboratorId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        let c = state
            .agent_collaborators
            .get(&agent_id)
            .and_then(|list| list.iter().find(|c| c.collaborator_id == collaborator_id))
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        Ok(AwsResponse::ok_json(
            json!({ "agentCollaborator": agent_collaborator_json(c) }),
        ))
    }

    fn list_agent_collaborators(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .and_then(|s| s.agent_collaborators.get(&agent_id))
            .map(|list| list.iter().map(agent_collaborator_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(
            json!({ "agentCollaboratorSummaries": list }),
        ))
    }

    fn update_agent_collaborator(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let collaborator_id = req_str(&body, "collaboratorId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let c = state
            .agent_collaborators
            .get_mut(&agent_id)
            .and_then(|list| {
                list.iter_mut()
                    .find(|c| c.collaborator_id == collaborator_id)
            })
            .ok_or_else(|| not_found("Collaborator not found".to_string()))?;
        c.updated_at = now();
        if let Some(n) = opt_str(&body, "collaboratorName") {
            c.collaborator_name = n;
        }
        if let Some(a) = opt_str(&body, "collaboratorAliasArn") {
            c.collaborator_alias_arn = a;
        }
        if let Some(r) = opt_str(&body, "relayConversationHistory") {
            c.relay_conversation_history = r;
        }
        Ok(AwsResponse::ok_json(
            json!({ "agentCollaborator": agent_collaborator_json(c) }),
        ))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "resourceArn")?;
        let tags = body["tags"]
            .as_object()
            .ok_or_else(|| missing("tags"))?
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect::<BTreeMap<_, _>>();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let entry = state.tags.entry(arn).or_default();
        for (k, v) in tags {
            entry.insert(k, v);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "resourceArn")?;
        let keys: Vec<String> = if let Some(v) = body.get("tagKeys").and_then(|v| v.as_array()) {
            v.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        } else {
            req.query_params
                .get("tagKeys")
                .map(|s| vec![s.clone()])
                .unwrap_or_default()
        };
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if let Some(entry) = state.tags.get_mut(&arn) {
            for k in keys {
                entry.remove(&k);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = req_str(&body, "resourceArn")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Resource {arn} not found")))?;
        let tags = state.tags.get(&arn).cloned().unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "tags": tags })))
    }

    fn create_agent_action_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let action_group_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "agentActionGroup": {
                "actionGroupId": action_group_id,
                "agentId": agent_id,
                "actionGroupName": req_str(&body, "actionGroupName")?,
                "createdAt": now_dt.to_rfc3339(),
            }
        })))
    }

    fn get_agent_action_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let action_group_id = req_str(&body, "actionGroupId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Action group {action_group_id} not found")))?;
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "agentActionGroup": {
                "actionGroupId": action_group_id,
                "agentId": agent_id,
                "actionGroupName": action_group_id,
                "createdAt": now().to_rfc3339(),
            }
        })))
    }

    fn list_agent_action_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| not_found(format!("Agent {agent_id} not found")))?;
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({ "actionGroupSummaries": [] })))
    }

    fn update_agent_action_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let action_group_id = req_str(&body, "actionGroupId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({
            "agentActionGroup": {
                "actionGroupId": action_group_id,
                "agentId": agent_id,
                "updatedAt": now().to_rfc3339(),
            }
        })))
    }

    fn delete_agent_action_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let agent_id = req_str(&body, "agentId")?;
        let _action_group_id = req_str(&body, "actionGroupId")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        if !state.agents.contains_key(&agent_id) {
            return Err(not_found(format!("Agent {agent_id} not found")));
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}
