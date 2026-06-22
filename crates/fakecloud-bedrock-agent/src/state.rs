use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedBedrockAgentState = Arc<RwLock<BedrockAgentAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BedrockAgentAccounts {
    pub accounts: BTreeMap<String, BedrockAgentState>,
}

/// On-disk snapshot envelope for Bedrock Agent state. Versioned so format
/// changes fail loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct BedrockAgentSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<BedrockAgentAccounts>,
}

pub const BEDROCK_AGENT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

impl BedrockAgentAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, account_id: &str, region: &str) -> &mut BedrockAgentState {
        self.accounts
            .entry(account_id.to_string())
            .or_insert_with(|| BedrockAgentState::new(account_id, region))
    }

    pub fn get(&self, account_id: &str) -> Option<&BedrockAgentState> {
        self.accounts.get(account_id)
    }

    pub fn reset(&mut self) {
        self.accounts.clear();
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BedrockAgentState {
    pub account_id: String,
    pub region: String,
    pub agents: BTreeMap<String, Agent>,
    pub agent_aliases: BTreeMap<String, AgentAlias>,
    pub agent_versions: BTreeMap<String, Vec<AgentVersion>>,
    pub knowledge_bases: BTreeMap<String, KnowledgeBase>,
    pub data_sources: BTreeMap<String, DataSource>,
    pub agent_knowledge_bases: BTreeMap<String, Vec<AgentKnowledgeBase>>,
    pub agent_collaborators: BTreeMap<String, Vec<AgentCollaborator>>,
    pub flows: BTreeMap<String, Flow>,
    pub flow_aliases: BTreeMap<String, FlowAlias>,
    pub flow_versions: BTreeMap<String, Vec<FlowVersion>>,
    pub prompts: BTreeMap<String, Prompt>,
    pub prompt_versions: BTreeMap<String, Vec<PromptVersion>>,
    pub ingestion_jobs: BTreeMap<String, Vec<IngestionJob>>,
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl BedrockAgentState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            agents: BTreeMap::new(),
            agent_aliases: BTreeMap::new(),
            agent_versions: BTreeMap::new(),
            knowledge_bases: BTreeMap::new(),
            data_sources: BTreeMap::new(),
            agent_knowledge_bases: BTreeMap::new(),
            agent_collaborators: BTreeMap::new(),
            flows: BTreeMap::new(),
            flow_aliases: BTreeMap::new(),
            flow_versions: BTreeMap::new(),
            prompts: BTreeMap::new(),
            prompt_versions: BTreeMap::new(),
            ingestion_jobs: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Agent {
    pub agent_id: String,
    pub agent_name: String,
    pub agent_arn: String,
    pub agent_version: String,
    pub agent_resource_role_arn: String,
    pub description: Option<String>,
    pub instruction: Option<String>,
    pub foundation_model: Option<String>,
    pub idle_session_ttl_in_seconds: i64,
    pub customer_encryption_key_arn: Option<String>,
    pub prompt_override_configuration: Option<serde_json::Value>,
    pub guardrail_configuration: Option<serde_json::Value>,
    pub agent_status: String,
    pub prepared_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub failure_reasons: Vec<String>,
    pub recommended_actions: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentAlias {
    pub alias_id: String,
    pub alias_name: String,
    pub agent_id: String,
    pub agent_version: String,
    pub routing_configuration: Vec<serde_json::Value>,
    pub description: Option<String>,
    pub alias_arn: String,
    pub agent_alias_status: String,
    pub failure_reasons: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentVersion {
    pub agent_version: String,
    pub agent_id: String,
    pub agent_name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub instruction: Option<String>,
    pub foundation_model: Option<String>,
    pub guardrail_configuration: Option<serde_json::Value>,
    pub prompt_override_configuration: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeBase {
    pub knowledge_base_id: String,
    pub name: String,
    pub knowledge_base_arn: String,
    pub description: Option<String>,
    pub role_arn: String,
    pub knowledge_base_configuration: serde_json::Value,
    pub storage_configuration: Option<serde_json::Value>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub failure_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSource {
    pub data_source_id: String,
    pub name: String,
    pub description: Option<String>,
    pub knowledge_base_id: String,
    pub data_source_configuration: Option<serde_json::Value>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub failure_reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentKnowledgeBase {
    pub agent_id: String,
    pub knowledge_base_id: String,
    pub description: Option<String>,
    pub knowledge_base_state: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentCollaborator {
    pub agent_id: String,
    pub collaborator_id: String,
    pub collaborator_name: String,
    pub collaborator_alias_arn: String,
    pub relay_conversation_history: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Flow {
    pub flow_id: String,
    pub name: String,
    pub description: Option<String>,
    pub execution_role_arn: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub version: String,
    pub definition: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowAlias {
    pub alias_id: String,
    pub alias_name: String,
    pub flow_id: String,
    pub routing_configuration: Vec<serde_json::Value>,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowVersion {
    pub flow_version: String,
    pub flow_id: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub definition: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prompt {
    pub prompt_id: String,
    pub name: String,
    pub description: Option<String>,
    pub variants: Vec<serde_json::Value>,
    pub version: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptVersion {
    pub prompt_version: String,
    pub prompt_id: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub variants: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestionJob {
    pub ingestion_job_id: String,
    pub knowledge_base_id: String,
    pub data_source_id: String,
    pub description: Option<String>,
    pub status: String,
    pub failure_reasons: Vec<String>,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
