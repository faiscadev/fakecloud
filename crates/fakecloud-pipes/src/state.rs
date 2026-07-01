//! In-memory state for AWS EventBridge Pipes. Each pipe is stored as a JSON
//! object (the raw create/update input plus generated metadata: Arn, state,
//! timestamps) and echoed verbatim on read, mirroring the Glue/Athena/Batch
//! pattern. Real source->enrichment->target execution lands in a later batch;
//! this batch is the control plane + faithful state machine + persistence.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type SharedPipesState = Arc<RwLock<PipesAccounts>>;

/// A JSON-backed pipe store: pipe name -> (raw input + generated fields).
pub type PipeStore = BTreeMap<String, Value>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PipesAccounts {
    pub accounts: BTreeMap<String, PipesState>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PipesState {
    /// Pipes keyed by name.
    #[serde(default)]
    pub pipes: PipeStore,
    /// Tags keyed by resource ARN -> { key: value }.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// Source checkpoints for streaming sources, so a restart resumes
    /// instead of re-replaying the retained backlog. Keyed by
    /// `"<pipeArn>#<shardId>"` for a Kinesis source (value = the sequence
    /// number of the last delivered record in that shard, so the cursor
    /// survives retention trims) and `"<pipeArn>"` for a DynamoDB-stream
    /// source (value = the last delivered sequence number). SQS sources
    /// don't checkpoint — they ack by deleting the source message.
    #[serde(default)]
    pub source_checkpoints: BTreeMap<String, String>,
}

impl PipesAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, account_id: &str) -> &mut PipesState {
        self.accounts.entry(account_id.to_string()).or_default()
    }

    pub fn get(&self, account_id: &str) -> Option<&PipesState> {
        self.accounts.get(account_id)
    }
}

/// On-disk snapshot envelope; versioned so format changes fail loudly.
#[derive(Clone, Serialize, Deserialize)]
pub struct PipesSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<PipesAccounts>,
}

pub const PIPES_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Pipe lifecycle states (subset of the AWS `PipeState` enum that this
/// emulator transitions through).
pub const STATE_CREATING: &str = "CREATING";
pub const STATE_RUNNING: &str = "RUNNING";
pub const STATE_STOPPED: &str = "STOPPED";
pub const STATE_UPDATING: &str = "UPDATING";
pub const STATE_STARTING: &str = "STARTING";
pub const STATE_STOPPING: &str = "STOPPING";
pub const STATE_DELETING: &str = "DELETING";

/// A pipe in one of these states has an in-flight transition; on restart the
/// recovery pass must re-drive it to its settled state, otherwise a pipe
/// snapshotted mid-transition would stay stuck forever.
pub fn is_transient_state(state: &str) -> bool {
    matches!(
        state,
        STATE_CREATING | STATE_UPDATING | STATE_STARTING | STATE_STOPPING | STATE_DELETING
    )
}
