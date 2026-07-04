//! Account-partitioned, serializable state for AWS Transfer Family's
//! (`transfer`) control plane.
//!
//! Every Transfer resource is stored as an already-output-valid JSON object
//! (`serde_json::Value`) keyed by its identifier. Storing the wire shape
//! directly keeps nested configuration objects (endpoint details,
//! identity-provider details, connector config, workflow steps) round-tripping
//! verbatim and guarantees the `Describe` responses echo exactly what was
//! persisted.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const TRANSFER_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Transfer Family state. Each resource is stored as its
/// already-output-valid `DescribedX` JSON object.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TransferData {
    /// Servers keyed by `ServerId` (`s-...`).
    #[serde(default)]
    pub servers: BTreeMap<String, Value>,
    /// Users keyed by `"{ServerId}/{UserName}"`.
    #[serde(default)]
    pub users: BTreeMap<String, Value>,
    /// Service-managed accesses keyed by `"{ServerId}/{ExternalId}"`.
    #[serde(default)]
    pub accesses: BTreeMap<String, Value>,
    /// Host keys keyed by `"{ServerId}/{HostKeyId}"`.
    #[serde(default)]
    pub host_keys: BTreeMap<String, Value>,
    /// Workflows keyed by `WorkflowId` (`w-...`).
    #[serde(default)]
    pub workflows: BTreeMap<String, Value>,
    /// Workflow executions keyed by `"{WorkflowId}/{ExecutionId}"`.
    #[serde(default)]
    pub executions: BTreeMap<String, Value>,
    /// AS2 agreements keyed by `"{ServerId}/{AgreementId}"`.
    #[serde(default)]
    pub agreements: BTreeMap<String, Value>,
    /// Connectors keyed by `ConnectorId` (`c-...`).
    #[serde(default)]
    pub connectors: BTreeMap<String, Value>,
    /// AS2 profiles keyed by `ProfileId` (`p-...`).
    #[serde(default)]
    pub profiles: BTreeMap<String, Value>,
    /// Certificates keyed by `CertificateId` (`cert-...`).
    #[serde(default)]
    pub certificates: BTreeMap<String, Value>,
    /// Web apps keyed by `WebAppId` (`webapp-...`).
    #[serde(default)]
    pub web_apps: BTreeMap<String, Value>,
    /// Web-app customizations keyed by `WebAppId`.
    #[serde(default)]
    pub web_app_customizations: BTreeMap<String, Value>,
    /// Connector file-transfer results keyed by `"{ConnectorId}/{TransferId}"`.
    #[serde(default)]
    pub file_transfers: BTreeMap<String, Value>,
    /// Resource tags keyed by `Arn` -> (key -> value).
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for TransferData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedTransferState = Arc<RwLock<MultiAccountState<TransferData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct TransferSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<TransferData>,
}
