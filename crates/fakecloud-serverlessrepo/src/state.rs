//! Account-partitioned, serializable state for the AWS Serverless Application
//! Repository (`serverlessrepo`).
//!
//! Every application is stored as a single JSON object keyed by its
//! `applicationId` (which, in SAR, is the application ARN -- a plain `String`,
//! so the snapshot never depends on the tuple-key serde adapter). The stored
//! object carries the full internal record (including create-only inputs like
//! `licenseBody` / `readmeBody` that never appear on the wire); the handlers
//! project the exact model output shape out of it on read.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const SERVERLESSREPO_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Serverless Application Repository state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServerlessRepoData {
    /// Applications keyed by `applicationId` (the application ARN). The stored
    /// value is the internal application record: metadata plus nested
    /// `versions` (map of semantic version -> version record), `policy`
    /// (list of policy statements), and `templates` (map of template id ->
    /// template record).
    #[serde(default)]
    pub applications: BTreeMap<String, Value>,
}

impl AccountState for ServerlessRepoData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedServerlessRepoState = Arc<RwLock<MultiAccountState<ServerlessRepoData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ServerlessRepoSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<ServerlessRepoData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_account_is_empty() {
        let data = ServerlessRepoData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.applications.is_empty());
    }
}
