use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedBedrockAgentState = Arc<RwLock<BedrockAgentAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(from = "StoredAccounts")]
pub struct BedrockAgentAccounts {
    pub accounts: BTreeMap<String, BedrockAgentState>,
}

/// The persisted form of [`BedrockAgentAccounts`]; loading it backfills the
/// ARNs older snapshots did not store.
#[derive(Deserialize)]
struct StoredAccounts {
    accounts: BTreeMap<String, BedrockAgentState>,
}

impl From<StoredAccounts> for BedrockAgentAccounts {
    fn from(stored: StoredAccounts) -> Self {
        let mut accounts = Self {
            accounts: stored.accounts,
        };
        accounts.backfill_arns();
        accounts
    }
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

    /// Give every flow and prompt persisted before ARNs were stored the ARN it
    /// was created with: the account state's region is the region its records
    /// were created in, falling back to the default server region.
    pub fn backfill_arns(&mut self) {
        for (account_id, state) in &mut self.accounts {
            let region = if state.region.is_empty() {
                "us-east-1"
            } else {
                state.region.as_str()
            };
            for flow in state.flows.values_mut() {
                if flow.arn.is_empty() {
                    flow.arn = crate::arns::flow_arn(region, account_id, &flow.flow_id);
                }
                // PrepareFlow used to store the enum name instead of its wire
                // value; FlowStatus serializes as `Prepared`.
                if flow.status == "PREPARED" {
                    flow.status = "Prepared".to_string();
                }
            }
            for prompt in state.prompts.values_mut().filter(|p| p.arn.is_empty()) {
                prompt.arn = crate::arns::prompt_arn(region, account_id, &prompt.prompt_id);
            }
            state.rekey_encoded_tags();
        }
    }
}

impl BedrockAgentState {
    /// Tags are keyed by the resource ARN as the caller wrote it. Builds that
    /// did not decode the `{resourceArn}` path label stored TagResource calls
    /// under the percent-encoded ARN (`arn%3Aaws%3Abedrock...`); move those
    /// entries to the decoded ARN. When both forms exist, the decoded entry
    /// was written later, so its values win and the encoded entry only
    /// contributes keys the decoded one lacks.
    fn rekey_encoded_tags(&mut self) {
        let encoded: Vec<String> = self
            .tags
            .keys()
            .filter(|k| k.contains('%'))
            .cloned()
            .collect();
        for key in encoded {
            let decoded = percent_encoding::percent_decode_str(&key)
                .decode_utf8_lossy()
                .into_owned();
            if decoded == key {
                continue;
            }
            let Some(old) = self.tags.remove(&key) else {
                continue;
            };
            let entry = self.tags.entry(decoded).or_default();
            for (k, v) in old {
                entry.entry(k).or_insert(v);
            }
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BedrockAgentState {
    pub account_id: String,
    pub region: String,
    pub agents: BTreeMap<String, Agent>,
    pub agent_aliases: BTreeMap<String, AgentAlias>,
    pub agent_versions: BTreeMap<String, Vec<AgentVersion>>,
    #[serde(default)]
    pub agent_action_groups: BTreeMap<String, AgentActionGroup>,
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
            agent_action_groups: BTreeMap::new(),
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
    /// `DISABLED` | `SUPERVISOR` | `SUPERVISOR_ROUTER`. AWS always reports it.
    #[serde(default)]
    pub agent_collaboration: String,
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
pub struct AgentActionGroup {
    pub action_group_id: String,
    pub agent_id: String,
    pub agent_version: String,
    pub action_group_name: String,
    pub description: Option<String>,
    pub action_group_state: String,
    pub action_group_executor: Option<serde_json::Value>,
    pub api_schema: Option<serde_json::Value>,
    pub function_schema: Option<serde_json::Value>,
    pub parent_action_group_signature: Option<String>,
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
    #[serde(default)]
    pub agent_version: String,
    pub collaborator_id: String,
    pub collaborator_name: String,
    /// The collaborator target, an `{ "aliasArn": ... }` object. AWS reads it
    /// back as `agent_descriptor.0.alias_arn`.
    #[serde(default)]
    pub agent_descriptor: Option<serde_json::Value>,
    #[serde(default)]
    pub collaboration_instruction: String,
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
    /// The ARN minted at creation. Snapshots written before it was stored get
    /// it backfilled on load (see [`BedrockAgentAccounts::backfill_arns`]).
    #[serde(default)]
    pub arn: String,
    #[serde(default)]
    pub customer_encryption_key_arn: Option<String>,
    /// The highest version number ever minted for this flow, so a deleted
    /// version's number is never handed out again.
    #[serde(default)]
    pub latest_version: u64,
    /// The findings of the last PrepareFlow, cleared when the draft changes.
    /// `None` until the flow is prepared, so GetFlow omits `validations`.
    #[serde(default)]
    pub validations: Option<Vec<serde_json::Value>>,
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
    #[serde(default)]
    pub concurrency_configuration: Option<serde_json::Value>,
}

/// A numbered, immutable snapshot of a flow. The flow-level fields are
/// captured at CreateFlowVersion time; snapshots written before they were
/// captured leave them `None`, and readers fall back to the parent flow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowVersion {
    pub flow_version: String,
    pub flow_id: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub definition: Option<serde_json::Value>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub execution_role_arn: Option<String>,
    #[serde(default)]
    pub customer_encryption_key_arn: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
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
    /// The ARN minted at creation; backfilled on load like [`Flow::arn`].
    #[serde(default)]
    pub arn: String,
    #[serde(default)]
    pub customer_encryption_key_arn: Option<String>,
    #[serde(default)]
    pub default_variant: Option<String>,
    /// The highest version number ever minted for this prompt; see
    /// [`Flow::latest_version`].
    #[serde(default)]
    pub latest_version: u64,
}

/// A numbered snapshot of a prompt. `name`, `default_variant` and
/// `customer_encryption_key_arn` are captured at CreatePromptVersion time;
/// older snapshots fall back to the prompt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PromptVersion {
    pub prompt_version: String,
    pub prompt_id: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub variants: Vec<serde_json::Value>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub default_variant: Option<String>,
    #[serde(default)]
    pub customer_encryption_key_arn: Option<String>,
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
