//! Account-partitioned, serializable state for AWS CodePipeline.
//!
//! Resources are stored as assembled response-shaped JSON values so a read
//! returns exactly what was written (round-trip fidelity). A pipeline execution
//! settles purely from its own stored `status`: a read after
//! `StartPipelineExecution` advances a non-terminal execution one step toward a
//! terminal state (`InProgress` -> `Succeeded`, `Stopping` -> `Stopped`), and a
//! terminal execution is never re-settled. No separate counter is kept.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CODEPIPELINE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The account-scoped CodePipeline state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodePipelineState {
    /// Current pipeline declaration keyed by pipeline name; value is a
    /// `PipelineDeclaration` JSON carrying the current `version`.
    #[serde(default)]
    pub pipelines: BTreeMap<String, Value>,
    /// Insertion order of pipeline names (most-recent last).
    #[serde(default)]
    pub pipeline_order: Vec<String>,
    /// Per-pipeline metadata keyed by pipeline name; value is a
    /// `PipelineMetadata` JSON (`pipelineArn`, `created`, `updated`).
    #[serde(default)]
    pub pipeline_meta: BTreeMap<String, Value>,
    /// Historical pipeline declarations keyed by pipeline name; the element at
    /// index `version - 1` is the declaration as of that version.
    #[serde(default)]
    pub pipeline_versions: BTreeMap<String, Vec<Value>>,
    /// Pipeline executions keyed by execution id (a UUID); value is a stored
    /// `PipelineExecution`-shaped JSON.
    #[serde(default)]
    pub executions: BTreeMap<String, Value>,
    /// Per-pipeline execution id order (most-recent last).
    #[serde(default)]
    pub execution_order: BTreeMap<String, Vec<String>>,
    /// Disabled stage transitions keyed by pipeline name, then by
    /// `<stageName>/<transitionType>`; value is a `TransitionState`-shaped JSON
    /// (`enabled: false`, `disabledReason`, `lastChangedBy`, `lastChangedAt`).
    /// A `(pipeline, stage, transitionType)` absent here is enabled.
    #[serde(default)]
    pub transitions_disabled: BTreeMap<String, BTreeMap<String, Value>>,
    /// User-created custom action types keyed by `category:provider:version`;
    /// value is an `ActionType` JSON.
    #[serde(default)]
    pub custom_actions: BTreeMap<String, Value>,
    /// Insertion order of custom action-type keys.
    #[serde(default)]
    pub custom_action_order: Vec<String>,
    /// Webhooks keyed by webhook name; value is a `ListWebhookItem` JSON.
    #[serde(default)]
    pub webhooks: BTreeMap<String, Value>,
    /// Insertion order of webhook names.
    #[serde(default)]
    pub webhook_order: Vec<String>,
    /// Resource tags keyed by resource ARN; value is a `TagList` (list of
    /// `{key,value}` objects).
    #[serde(default)]
    pub tags: BTreeMap<String, Vec<Value>>,
}

impl AccountState for CodePipelineState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCodePipelineState = Arc<RwLock<MultiAccountState<CodePipelineState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CodePipelineSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CodePipelineState>,
}
