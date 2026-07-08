//! Account-partitioned, serializable state for Amazon Textract.
//!
//! Textract resources are stored as assembled, response-shaped
//! `serde_json::Value`s keyed by their natural identifiers so a read returns
//! exactly what was written (round-trip fidelity) and the on-the-wire JSON
//! never carries a field that isn't in the Smithy model.
//!
//! - Adapters are keyed by `AdapterId`; adapter versions by
//!   `<adapterId>\u{1}<version>`.
//! - Asynchronous jobs (text detection / document analysis / expense analysis
//!   / lending analysis) are keyed by `JobId`; each carries its kind, status,
//!   creation time and the request context needed to build a `Get*` response.
//! - Tags are keyed by resource ARN.
//! - `ClientRequestToken` idempotency keys map a token to the id it minted.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const TEXTRACT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The account-scoped Textract state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TextractState {
    /// Adapters keyed by `AdapterId`; value is the assembled `GetAdapter`
    /// response shape (`AdapterId`, `AdapterName`, `CreationTime`,
    /// `Description`, `FeatureTypes`, `AutoUpdate`).
    #[serde(default)]
    pub adapters: BTreeMap<String, Value>,
    /// Insertion order of adapter ids (most-recent first is applied at read).
    #[serde(default)]
    pub adapter_order: Vec<String>,
    /// Adapter versions keyed by `<adapterId>\u{1}<version>`; value is the
    /// assembled `GetAdapterVersion` response shape.
    #[serde(default)]
    pub adapter_versions: BTreeMap<String, Value>,
    /// Insertion order of adapter-version keys.
    #[serde(default)]
    pub adapter_version_order: Vec<String>,
    /// Next monotonic version number per adapter id (starts at 1).
    #[serde(default)]
    pub next_adapter_version: BTreeMap<String, u64>,
    /// Asynchronous jobs keyed by `JobId`; value is a `Job` record.
    #[serde(default)]
    pub jobs: BTreeMap<String, Job>,
    /// Tags keyed by resource ARN; value is a `{Key: Value}` string map.
    #[serde(default)]
    pub tags: BTreeMap<String, Value>,
    /// `ClientRequestToken` -> minted job id, for idempotent `Start*`.
    #[serde(default)]
    pub job_tokens: BTreeMap<String, String>,
    /// `ClientRequestToken` -> minted adapter id, for idempotent
    /// `CreateAdapter`.
    #[serde(default)]
    pub adapter_tokens: BTreeMap<String, String>,
}

/// An asynchronous Textract job record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// One of `text_detection`, `document_analysis`, `expense_analysis`,
    /// `lending_analysis`.
    pub kind: String,
    /// Current job status (`IN_PROGRESS` on first read, then `SUCCEEDED`).
    pub status: String,
    /// Number of pages reported in `DocumentMetadata`.
    pub pages: u64,
    /// Whether this job has been read at least once (settles the lifecycle).
    #[serde(default)]
    pub settled: bool,
    /// `JobTag` echoed back on some downstream integrations; kept for fidelity.
    #[serde(default)]
    pub job_tag: Option<String>,
    /// Model version string reported for this job kind.
    pub model_version: String,
}

impl AccountState for TextractState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedTextractState = Arc<RwLock<MultiAccountState<TextractState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct TextractSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<TextractState>,
}
