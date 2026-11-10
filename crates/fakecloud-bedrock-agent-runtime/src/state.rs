use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedBedrockAgentRuntimeState = Arc<RwLock<BedrockAgentRuntimeAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BedrockAgentRuntimeAccounts {
    pub accounts: BTreeMap<String, BedrockAgentRuntimeState>,
}

/// On-disk snapshot envelope for Bedrock Agent Runtime state. Versioned so
/// format changes fail loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct BedrockAgentRuntimeSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<BedrockAgentRuntimeAccounts>,
}

pub const BEDROCK_AGENT_RUNTIME_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

impl BedrockAgentRuntimeAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, account_id: &str) -> &mut BedrockAgentRuntimeState {
        self.accounts
            .entry(account_id.to_string())
            .or_insert_with(|| BedrockAgentRuntimeState::new(account_id))
    }

    pub fn reset(&mut self) {
        self.accounts.clear();
    }

    /// A copy of the persisted state, leaving out the introspection
    /// invocation log (which is never written to disk) so a save does not
    /// clone an ever-growing buffer.
    pub fn persisted_copy(&self) -> Self {
        Self {
            accounts: self
                .accounts
                .iter()
                .map(|(id, s)| {
                    let copy = BedrockAgentRuntimeState {
                        account_id: s.account_id.clone(),
                        invocations: Vec::new(),
                        sessions: s.sessions.clone(),
                        flow_executions: s.flow_executions.clone(),
                        session_invocations: s.session_invocations.clone(),
                        invocation_steps: s.invocation_steps.clone(),
                        tags: s.tags.clone(),
                    };
                    (id.clone(), copy)
                })
                .collect(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BedrockAgentRuntimeState {
    pub account_id: String,
    /// Data-plane invocation log served by the
    /// `/_fakecloud/bedrock-agent-runtime/invocations` introspection endpoint.
    /// Like every introspection buffer it is not persisted: it resets on
    /// restart.
    #[serde(skip)]
    pub invocations: Vec<InvocationRecord>,
    pub sessions: BTreeMap<String, Session>,
    /// Keyed by `crate::flows::execution_map_key` (flow, alias, execution id).
    pub flow_executions: BTreeMap<String, FlowExecution>,
    /// Per-session list of invocations created via `CreateInvocation`
    /// (separate from `invocations` which is the data-plane invocation log).
    #[serde(default)]
    pub session_invocations: BTreeMap<String, Vec<SessionInvocation>>,
    /// Invocation steps keyed by `(sessionId, invocationStepId)`. Stored as a
    /// flat map so `GetInvocationStep` can look up by step id alone while
    /// `ListInvocationSteps` can filter by session/invocation.
    #[serde(default)]
    pub invocation_steps: BTreeMap<String, InvocationStep>,
    /// Tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl BedrockAgentRuntimeState {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            invocations: Vec::new(),
            sessions: BTreeMap::new(),
            flow_executions: BTreeMap::new(),
            session_invocations: BTreeMap::new(),
            invocation_steps: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationRecord {
    pub invocation_id: String,
    /// One of `invoke_agent`, `invoke_inline_agent`, `invoke_flow`,
    /// `retrieve`, `retrieve_and_generate`, `create_invocation`.
    pub op: String,
    pub agent_id: Option<String>,
    pub flow_id: Option<String>,
    pub session_id: Option<String>,
    pub input: String,
    pub output: String,
    /// Number of chunk frames (or retrieval results, or flow outputs) emitted
    /// for this invocation. `0` for session-only `CreateInvocation` rows and for
    /// a flow run that reached no output node.
    pub output_chunks: u32,
    /// Optional trace blob captured for InvokeAgent-style ops. Stored as
    /// JSON so the introspection endpoint can hand it back unchanged.
    pub trace: Option<serde_json::Value>,
    /// Citations attached by RetrieveAndGenerate. Empty for ops that
    /// don't emit them.
    #[serde(default)]
    pub citations: Vec<serde_json::Value>,
    /// Every output an `InvokeFlow` produced, in order, as
    /// `{nodeName, document}`: a run can stream several, and empty means it
    /// reached no output node, where `output` alone can't tell that from an
    /// empty document.
    #[serde(default)]
    pub flow_outputs: Vec<serde_json::Value>,
    pub timestamp: DateTime<Utc>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub session_arn: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
    #[serde(default)]
    pub encryption_key_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInvocation {
    pub invocation_id: String,
    pub session_id: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationStep {
    pub session_id: String,
    pub invocation_id: String,
    pub invocation_step_id: String,
    pub invocation_step_time: DateTime<Utc>,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowExecution {
    pub execution_id: String,
    pub execution_arn: String,
    pub flow_id: String,
    #[serde(default)]
    pub flow_alias_id: String,
    #[serde(default)]
    pub flow_version: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
    /// The flow definition the execution runs, captured at start from the
    /// version its alias routed to. `GetExecutionFlowSnapshot` returns it.
    #[serde(default)]
    pub definition: Option<serde_json::Value>,
    /// The executed version's service role, captured at start.
    #[serde(default)]
    pub execution_role_arn: Option<String>,
    /// The executed version's customer-managed KMS key, captured at start.
    #[serde(default)]
    pub customer_encryption_key_arn: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_round_trips_persisted_state_but_not_the_invocation_log() {
        let now = Utc::now();
        let mut accounts = BedrockAgentRuntimeAccounts::new();
        let st = accounts.get_or_create("123456789012");
        st.sessions.insert(
            "sess-1".into(),
            Session {
                session_id: "sess-1".into(),
                session_arn: "arn:aws:bedrock:us-east-1:123456789012:session/sess-1".into(),
                status: "ACTIVE".into(),
                created_at: now,
                updated_at: now,
                metadata: BTreeMap::from([("k".to_string(), "v".to_string())]),
                encryption_key_arn: Some("arn:aws:kms:us-east-1:123456789012:key/k".into()),
            },
        );
        st.session_invocations.insert(
            "sess-1".into(),
            vec![SessionInvocation {
                invocation_id: "inv-1".into(),
                session_id: "sess-1".into(),
                description: Some("first".into()),
                created_at: now,
            }],
        );
        st.invocation_steps.insert(
            "step-1".into(),
            InvocationStep {
                session_id: "sess-1".into(),
                invocation_id: "inv-1".into(),
                invocation_step_id: "step-1".into(),
                invocation_step_time: now,
                payload: serde_json::json!({"contentBlocks": [{"text": "hi"}]}),
            },
        );
        st.flow_executions.insert(
            "FLOW/ALIAS/exec-1".into(),
            FlowExecution {
                execution_id: "exec-1".into(),
                execution_arn:
                    "arn:aws:bedrock:us-east-1:123456789012:flow/F/alias/A/execution/exec-1".into(),
                flow_id: "F".into(),
                flow_alias_id: "A".into(),
                flow_version: "1".into(),
                status: "Aborted".into(),
                created_at: now,
                updated_at: now,
                ended_at: Some(now),
                definition: Some(serde_json::json!({"nodes": []})),
                execution_role_arn: Some("arn:aws:iam::123456789012:role/r".into()),
                customer_encryption_key_arn: None,
            },
        );
        st.tags.insert(
            "arn:aws:bedrock:us-east-1:123456789012:session/sess-1".into(),
            BTreeMap::from([("env".to_string(), "test".to_string())]),
        );
        st.invocations.push(InvocationRecord {
            invocation_id: "log-1".into(),
            op: "invoke_agent".into(),
            agent_id: None,
            flow_id: None,
            session_id: None,
            input: String::new(),
            output: String::new(),
            output_chunks: 1,
            trace: None,
            citations: Vec::new(),
            flow_outputs: Vec::new(),
            timestamp: now,
            duration_ms: 0,
        });

        let snapshot = BedrockAgentRuntimeSnapshot {
            schema_version: BEDROCK_AGENT_RUNTIME_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts.persisted_copy()),
        };
        let bytes = serde_json::to_vec(&snapshot).unwrap();
        let loaded: BedrockAgentRuntimeSnapshot = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            loaded.schema_version,
            BEDROCK_AGENT_RUNTIME_SNAPSHOT_SCHEMA_VERSION
        );
        let loaded = loaded.accounts.unwrap();
        let st = &loaded.accounts["123456789012"];
        assert_eq!(st.account_id, "123456789012");
        let sess = &st.sessions["sess-1"];
        assert_eq!(sess.created_at, now);
        assert_eq!(sess.metadata["k"], "v");
        assert!(sess.encryption_key_arn.is_some());
        assert_eq!(
            st.session_invocations["sess-1"][0].description.as_deref(),
            Some("first")
        );
        assert_eq!(
            st.invocation_steps["step-1"].payload,
            serde_json::json!({"contentBlocks": [{"text": "hi"}]})
        );
        let exec = &st.flow_executions["FLOW/ALIAS/exec-1"];
        assert_eq!(exec.status, "Aborted");
        assert_eq!(exec.ended_at, Some(now));
        assert_eq!(exec.definition, Some(serde_json::json!({"nodes": []})));
        assert_eq!(
            st.tags["arn:aws:bedrock:us-east-1:123456789012:session/sess-1"]["env"],
            "test"
        );
        // The introspection log is not persisted, even from a full clone.
        assert!(st.invocations.is_empty());
        let full: BedrockAgentRuntimeAccounts =
            serde_json::from_slice(&serde_json::to_vec(&accounts).unwrap()).unwrap();
        assert!(full.accounts["123456789012"].invocations.is_empty());
    }

    #[test]
    fn snapshot_without_accounts_loads_as_none() {
        let snap: BedrockAgentRuntimeSnapshot =
            serde_json::from_str(r#"{"schema_version": 1}"#).unwrap();
        assert!(snap.accounts.is_none());
    }

    #[test]
    fn invocation_record_serializes_introspection_fields() {
        let rec = InvocationRecord {
            invocation_id: "inv-1".into(),
            op: "invoke_agent".into(),
            agent_id: Some("agent-1".into()),
            flow_id: None,
            session_id: Some("sess-1".into()),
            input: "hi".into(),
            output: "hello".into(),
            output_chunks: 1,
            trace: Some(serde_json::json!({"orchestration": "ok"})),
            citations: vec![serde_json::json!({"ref": "doc1"})],
            flow_outputs: Vec::new(),
            timestamp: Utc::now(),
            duration_ms: 42,
        };
        let v = serde_json::to_value(&rec).unwrap();
        assert_eq!(v["op"], "invoke_agent");
        assert_eq!(v["agent_id"], "agent-1");
        assert_eq!(v["session_id"], "sess-1");
        assert_eq!(v["output_chunks"], 1);
        assert_eq!(v["duration_ms"], 42);
        assert!(v["trace"].is_object());
        assert!(v["citations"].is_array());
    }
}
