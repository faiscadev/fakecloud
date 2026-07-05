//! Account-partitioned, serializable state for AWS CodeCommit.
//!
//! A repository owns a content-addressed object store: `blobs` maps a 40-char
//! SHA-1 blob id to its (base64) bytes, and `trees` maps a commit id to the
//! full flat path -> [`FileEntry`] snapshot of that commit's working tree (a
//! materialized tree, so `GetFile`/`GetFolder`/`GetDifferences` are simple
//! lookups). `commits` maps a commit id to its stored `Commit`-shaped JSON, and
//! `branches` maps a branch name to the commit id at its tip. Pull requests,
//! comments, approval-rule templates and their associations, triggers, and tags
//! are stored as response-shaped JSON so a read returns exactly what was
//! written.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CODECOMMIT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// One entry in a commit's materialized working tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// The 40-char SHA-1 blob id of the file content.
    pub blob_id: String,
    /// The git file mode: `100644` (NORMAL), `100755` (EXECUTABLE), or
    /// `120000` (SYMLINK).
    pub mode: String,
}

/// A single git repository and its content-addressed object store.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Repo {
    /// `RepositoryMetadata`-shaped JSON (id, arn, clone urls, dates, kms key).
    pub metadata: Value,
    /// Branch name -> tip commit id.
    #[serde(default)]
    pub branches: BTreeMap<String, String>,
    /// Commit id -> `Commit`-shaped JSON.
    #[serde(default)]
    pub commits: BTreeMap<String, Value>,
    /// Blob id -> base64-encoded content bytes.
    #[serde(default)]
    pub blobs: BTreeMap<String, String>,
    /// Commit id -> materialized working tree (path -> entry).
    #[serde(default)]
    pub trees: BTreeMap<String, BTreeMap<String, FileEntry>>,
    /// Configured repository triggers (`RepositoryTrigger`-shaped JSON list).
    #[serde(default)]
    pub triggers: Vec<Value>,
    /// The current triggers configuration id (a UUID), empty when never set.
    #[serde(default)]
    pub triggers_config_id: String,
    /// Approval-rule-template names associated with this repository.
    #[serde(default)]
    pub associated_templates: Vec<String>,
}

/// The account-scoped CodeCommit state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeCommitState {
    /// Repositories keyed by repository name.
    #[serde(default)]
    pub repositories: BTreeMap<String, Repo>,
    /// Insertion order of repository names (most-recent last).
    #[serde(default)]
    pub repository_order: Vec<String>,
    /// Approval-rule templates keyed by template name; value is an
    /// `ApprovalRuleTemplate`-shaped JSON.
    #[serde(default)]
    pub templates: BTreeMap<String, Value>,
    /// Insertion order of template names.
    #[serde(default)]
    pub template_order: Vec<String>,
    /// Pull requests keyed by pull-request id; value is a `PullRequest`-shaped
    /// JSON carrying its targets, status, revision id, and approval rules.
    #[serde(default)]
    pub pull_requests: BTreeMap<String, Value>,
    /// Insertion order of pull-request ids.
    #[serde(default)]
    pub pull_request_order: Vec<String>,
    /// Monotonic counter for minting pull-request ids.
    #[serde(default)]
    pub pull_request_counter: u64,
    /// Pull-request event log keyed by pull-request id.
    #[serde(default)]
    pub pull_request_events: BTreeMap<String, Vec<Value>>,
    /// Per-pull-request approval state, keyed by pull-request id then revision
    /// id then user ARN; value is `APPROVE` or `REVOKE`.
    #[serde(default)]
    pub pull_request_approvals: BTreeMap<String, BTreeMap<String, BTreeMap<String, String>>>,
    /// Per-pull-request override state, keyed by pull-request id then revision
    /// id; value is the overrider ARN (present means overridden).
    #[serde(default)]
    pub pull_request_overrides: BTreeMap<String, BTreeMap<String, String>>,
    /// Comments keyed by comment id; value is a `Comment`-shaped JSON with the
    /// thread context (`repositoryName`, before/after commit, `pullRequestId`,
    /// `location`) stored alongside under private `_ctx*` members that are
    /// stripped before a comment is returned to the wire.
    #[serde(default)]
    pub comments: BTreeMap<String, Value>,
    /// Insertion order of comment ids.
    #[serde(default)]
    pub comment_order: Vec<String>,
    /// Resource tags keyed by resource ARN; value is a `key -> value` map.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for CodeCommitState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCodeCommitState = Arc<RwLock<MultiAccountState<CodeCommitState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeCommitSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CodeCommitState>,
}
