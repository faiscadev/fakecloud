//! In-memory state for AWS Batch. Control-plane resources are JSON-backed
//! (the raw create input plus generated metadata, echoed verbatim on read),
//! mirroring the Glue/Athena pattern. Real container-backed job execution
//! lands in a later batch.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type SharedBatchState = Arc<RwLock<BatchAccounts>>;

/// A JSON-backed named-resource store: name -> (raw input + generated fields).
pub type JsonStore = BTreeMap<String, Value>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BatchAccounts {
    pub accounts: BTreeMap<String, BatchState>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BatchState {
    /// Compute environments keyed by name.
    #[serde(default)]
    pub compute_environments: JsonStore,
    /// Job queues keyed by name.
    #[serde(default)]
    pub job_queues: JsonStore,
    /// Job definitions keyed by `name:revision`; the active revision per name
    /// is resolved at read time.
    #[serde(default)]
    pub job_definitions: JsonStore,
    /// Jobs keyed by jobId.
    #[serde(default)]
    pub jobs: JsonStore,
    /// Fair-share scheduling policies keyed by name.
    #[serde(default)]
    pub scheduling_policies: JsonStore,
    /// Tags keyed by resource ARN -> { key: value }.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Per-job-definition-name highest revision allocated.
    #[serde(default)]
    pub job_def_revisions: BTreeMap<String, i64>,
}

impl BatchAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, account_id: &str) -> &mut BatchState {
        self.accounts.entry(account_id.to_string()).or_default()
    }

    pub fn get(&self, account_id: &str) -> Option<&BatchState> {
        self.accounts.get(account_id)
    }
}

/// On-disk snapshot envelope; versioned so format changes fail loudly.
#[derive(Clone, Serialize, Deserialize)]
pub struct BatchSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<BatchAccounts>,
}

pub const BATCH_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
