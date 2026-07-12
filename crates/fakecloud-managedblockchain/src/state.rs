//! Account-partitioned, serializable state for Amazon Managed Blockchain
//! (`managedblockchain`).
//!
//! Every resource is stored as its already-output-valid wire JSON object so
//! reads echo exactly what writes persisted. All map keys are plain `String`s
//! (resource id or resource ARN), so the snapshot never depends on the
//! tuple-key serde adapter that has silently broken snapshot serialization on
//! other services.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const MANAGEDBLOCKCHAIN_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Amazon Managed Blockchain state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ManagedBlockchainData {
    /// Networks keyed by `Id`, stored as their `Network` wire object.
    #[serde(default)]
    pub networks: BTreeMap<String, Value>,
    /// Members keyed by `Id`, stored as their `Member` wire object.
    #[serde(default)]
    pub members: BTreeMap<String, Value>,
    /// Nodes keyed by `Id`, stored as their `Node` wire object.
    #[serde(default)]
    pub nodes: BTreeMap<String, Value>,
    /// Proposals keyed by `ProposalId`, stored as their `Proposal` wire object.
    #[serde(default)]
    pub proposals: BTreeMap<String, Value>,
    /// Recorded votes keyed by `ProposalId`, each a `VoteSummary` wire object.
    #[serde(default)]
    pub votes: BTreeMap<String, Vec<Value>>,
    /// Pending invitations keyed by `InvitationId`, stored as their `Invitation`
    /// wire object.
    #[serde(default)]
    pub invitations: BTreeMap<String, Value>,
    /// Token accessors keyed by `Id`, stored as their `Accessor` wire object.
    #[serde(default)]
    pub accessors: BTreeMap<String, Value>,
    /// Tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for ManagedBlockchainData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedManagedBlockchainState = Arc<RwLock<MultiAccountState<ManagedBlockchainData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ManagedBlockchainSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<ManagedBlockchainData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_account_is_empty() {
        let data = ManagedBlockchainData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.networks.is_empty());
        assert!(data.members.is_empty());
        assert!(data.accessors.is_empty());
    }
}
