//! Account-partitioned, serializable state for AWS CodeBuild.
//!
//! Resources are stored as assembled response-shaped JSON values so a read
//! returns exactly what was written (round-trip fidelity). Builds and build
//! batches carry a `settled` flag so the first read after `StartBuild` can
//! transition them from `IN_PROGRESS` to a terminal state deterministically.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CODEBUILD_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The account-scoped CodeBuild state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeBuildState {
    /// Projects keyed by project name; value is the assembled `Project` JSON.
    #[serde(default)]
    pub projects: BTreeMap<String, Value>,
    /// Builds keyed by build id (`<project>:<uuid>`); value is a `Build` JSON.
    #[serde(default)]
    pub builds: BTreeMap<String, Value>,
    /// Insertion order of build ids (most-recent last) for time-ordered lists.
    #[serde(default)]
    pub build_order: Vec<String>,
    /// Per-project monotonic build-number counter, keyed by project name. Only
    /// ever increments (never reused after a build is deleted), matching AWS's
    /// per-project `buildNumber` semantics.
    #[serde(default)]
    pub build_numbers: BTreeMap<String, i64>,
    /// Build batches keyed by batch id (`<project>:<uuid>`); `BuildBatch` JSON.
    #[serde(default)]
    pub build_batches: BTreeMap<String, Value>,
    #[serde(default)]
    pub build_batch_order: Vec<String>,
    /// Per-project monotonic build-batch-number counter, keyed by project name.
    #[serde(default)]
    pub build_batch_numbers: BTreeMap<String, i64>,
    /// Report groups keyed by ARN; value is a `ReportGroup` JSON.
    #[serde(default)]
    pub report_groups: BTreeMap<String, Value>,
    /// Reports keyed by ARN; value is a `Report` JSON.
    #[serde(default)]
    pub reports: BTreeMap<String, Value>,
    #[serde(default)]
    pub report_order: Vec<String>,
    /// Fleets keyed by ARN; value is a `Fleet` JSON.
    #[serde(default)]
    pub fleets: BTreeMap<String, Value>,
    /// Source credentials keyed by ARN; value is a `SourceCredentialsInfo` JSON.
    #[serde(default)]
    pub source_credentials: BTreeMap<String, Value>,
    /// Resource policies keyed by resource ARN; value is the policy document.
    #[serde(default)]
    pub resource_policies: BTreeMap<String, String>,
    /// Sandboxes keyed by sandbox id; value is a `Sandbox` JSON.
    #[serde(default)]
    pub sandboxes: BTreeMap<String, Value>,
    #[serde(default)]
    pub sandbox_order: Vec<String>,
    /// Command executions keyed by id; value is a `CommandExecution` JSON.
    #[serde(default)]
    pub command_executions: BTreeMap<String, Value>,
    #[serde(default)]
    pub command_execution_order: Vec<String>,
}

impl AccountState for CodeBuildState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCodeBuildState = Arc<RwLock<MultiAccountState<CodeBuildState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeBuildSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CodeBuildState>,
}
