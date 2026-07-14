//! AWS CodeCommit (`codecommit`) implementation for FakeCloud.
//!
//! awsJson1.1 git-repository control plane for CodeCommit: repositories with a
//! real content-addressed object store (blobs, trees, commits keyed by 40-char
//! SHA-1 ids), branches and the default branch, file put/delete/read, commit
//! history and differences, pull requests with targets/approvals/events,
//! approval-rule templates and their repository associations, per-revision
//! pull-request approval rules and overrides, comments on commits and pull
//! requests with reactions, repository triggers, and resource tagging.
//!
//! Everything is real, persisted, and account-partitioned: every `Create`/
//! `Put`/`Update` round-trips against its `Get`/`List`/`Describe`. Merges are
//! computed against the stored commit graph (fast-forward and non-conflicting
//! three-way/squash resolve; genuinely divergent trees report the declared
//! `ManualMergeRequiredException`). There is no live git transport, so clone
//! URLs are minted in exact AWS form but no smart-HTTP endpoint is served --
//! the same control-plane-only shape the sibling `Code*` services use.

pub mod persistence;
pub(crate) mod service;
pub(crate) mod state;
mod validate;

pub use service::{CodeCommitService, CODECOMMIT_ACTIONS};
pub use state::{
    CodeCommitSnapshot, CodeCommitState, SharedCodeCommitState, CODECOMMIT_SNAPSHOT_SCHEMA_VERSION,
};
// Re-exported for the CloudFormation resource_provisioner; `state` is pub(crate).
pub use state::Repo;
