//! Account-partitioned, serializable state for AWS CodeArtifact.
//!
//! Resources are stored as assembled response-shaped JSON values so a read
//! returns exactly what was written (round-trip fidelity). Keys are the
//! natural identifiers: a domain by name, a repository by `domain/repo`, a
//! package group by `domain\u{1}pattern`, a package by
//! `domain\u{1}repo\u{1}format\u{1}namespace\u{1}package`, and a package
//! version by that package key plus `\u{1}version`. Published asset bytes are
//! kept base64-encoded alongside the version so a later `GetPackageVersionAsset`
//! streams back exactly what `PublishPackageVersion` stored.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CODEARTIFACT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The account-scoped CodeArtifact state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeArtifactState {
    /// Domains keyed by domain name; value is a `DomainDescription` JSON.
    #[serde(default)]
    pub domains: BTreeMap<String, Value>,
    /// Insertion order of domain names (most-recent last).
    #[serde(default)]
    pub domain_order: Vec<String>,
    /// Domain permission policies keyed by domain name; value is a
    /// `ResourcePolicy`-shaped JSON (`resourceArn`, `revision`, `document`).
    #[serde(default)]
    pub domain_policies: BTreeMap<String, Value>,
    /// Repositories keyed by `domain/repo`; value is a `RepositoryDescription`
    /// JSON carrying `upstreams` and `externalConnections`.
    #[serde(default)]
    pub repositories: BTreeMap<String, Value>,
    /// Insertion order of `domain/repo` keys (most-recent last).
    #[serde(default)]
    pub repository_order: Vec<String>,
    /// Repository permission policies keyed by `domain/repo`.
    #[serde(default)]
    pub repository_policies: BTreeMap<String, Value>,
    /// Package groups keyed by `domain\u{1}pattern`; value is a
    /// `PackageGroupDescription` JSON.
    #[serde(default)]
    pub package_groups: BTreeMap<String, Value>,
    /// Insertion order of package-group keys (most-recent last).
    #[serde(default)]
    pub package_group_order: Vec<String>,
    /// Allowed repositories per package group + origin-restriction type, keyed
    /// by `domain\u{1}pattern\u{1}restrictionType`; value is a list of
    /// repository-name strings.
    #[serde(default)]
    pub package_group_allowed: BTreeMap<String, Vec<String>>,
    /// Packages keyed by the package key
    /// (`domain\u{1}repo\u{1}format\u{1}namespace\u{1}package`); value is a
    /// `PackageSummary`-shaped JSON carrying `originConfiguration`.
    #[serde(default)]
    pub packages: BTreeMap<String, Value>,
    /// Insertion order of package keys (most-recent last).
    #[serde(default)]
    pub package_order: Vec<String>,
    /// Package versions keyed by `<packageKey>\u{1}<version>`; value is a
    /// `PackageVersionDescription`-shaped JSON.
    #[serde(default)]
    pub package_versions: BTreeMap<String, Value>,
    /// Assets keyed by `<packageKey>\u{1}<version>\u{1}<assetName>`; value is an
    /// `AssetSummary`-shaped JSON (`name`, `size`, `hashes`).
    #[serde(default)]
    pub assets: BTreeMap<String, Value>,
    /// Base64-encoded asset bytes keyed identically to `assets`.
    #[serde(default)]
    pub asset_content: BTreeMap<String, String>,
    /// Package-version readmes keyed by `<packageKey>\u{1}<version>`.
    #[serde(default)]
    pub readmes: BTreeMap<String, String>,
    /// Resource tags keyed by resource ARN; value is a `TagList` (list of
    /// `{key,value}` objects).
    #[serde(default)]
    pub tags: BTreeMap<String, Vec<Value>>,
}

impl AccountState for CodeArtifactState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCodeArtifactState = Arc<RwLock<MultiAccountState<CodeArtifactState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeArtifactSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CodeArtifactState>,
}
