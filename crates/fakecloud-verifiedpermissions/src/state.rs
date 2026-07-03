//! Account-partitioned, serializable state for AWS Verified Permissions.
//!
//! Everything the service owns for one account: policy stores (each with its
//! Cedar schema, static / template-linked policies, policy templates, identity
//! sources and tags) and the account-level policy-store aliases. Nested
//! configuration objects (identity-source `Configuration`, policy
//! `EntityIdentifier`s, `ValidationSettings`, ...) are stored as the raw
//! request `Value` so they round-trip verbatim on describe.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const VERIFIEDPERMISSIONS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The Cedar schema attached to a policy store (at most one).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredSchema {
    /// The raw Cedar schema JSON string (`cedarJson`).
    pub cedar_json: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A policy in a policy store — either a `static` Cedar policy or a
/// `templateLinked` instantiation of a policy template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPolicy {
    pub policy_id: String,
    /// `STATIC` or `TEMPLATE_LINKED`.
    pub policy_type: String,
    #[serde(default)]
    pub name: Option<String>,
    // --- static ---
    #[serde(default)]
    pub description: Option<String>,
    /// The Cedar policy statement text (static policies only).
    #[serde(default)]
    pub statement: Option<String>,
    // --- template-linked ---
    #[serde(default)]
    pub template_id: Option<String>,
    /// Raw `EntityIdentifier` value bound to `?principal`.
    #[serde(default)]
    pub principal: Option<Value>,
    /// Raw `EntityIdentifier` value bound to `?resource`.
    #[serde(default)]
    pub resource: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A parameterized Cedar policy template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPolicyTemplate {
    pub template_id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// The Cedar template statement (may contain `?principal` / `?resource`).
    pub statement: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// An identity source linking a Cognito user pool or OIDC provider to the
/// policy store, used to resolve principals from a token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredIdentitySource {
    pub identity_source_id: String,
    /// Raw `Configuration` value round-tripped for describe.
    pub configuration: Value,
    #[serde(default)]
    pub principal_entity_type: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A single policy store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPolicyStore {
    pub policy_store_id: String,
    pub arn: String,
    /// `OFF` or `STRICT`.
    pub validation_mode: String,
    #[serde(default)]
    pub description: Option<String>,
    /// `ENABLED` or `DISABLED`.
    #[serde(default)]
    pub deletion_protection: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default)]
    pub schema: Option<StoredSchema>,
    /// Policies keyed by `policyId`.
    #[serde(default)]
    pub policies: BTreeMap<String, StoredPolicy>,
    /// Policy templates keyed by `policyTemplateId`.
    #[serde(default)]
    pub templates: BTreeMap<String, StoredPolicyTemplate>,
    /// Identity sources keyed by `identitySourceId`.
    #[serde(default)]
    pub identity_sources: BTreeMap<String, StoredIdentitySource>,
    /// Resource tags (key -> value).
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

/// A policy-store alias (a friendly name pointing at a policy store).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAlias {
    pub alias_name: String,
    pub policy_store_id: String,
    pub arn: String,
    /// `Active` or `PendingDeletion`.
    pub state: String,
    pub created_at: DateTime<Utc>,
}

/// Per-account Verified Permissions state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct VerifiedPermissionsData {
    /// Policy stores keyed by `policyStoreId`.
    #[serde(default)]
    pub stores: BTreeMap<String, StoredPolicyStore>,
    /// Policy-store aliases keyed by `aliasName`.
    #[serde(default)]
    pub aliases: BTreeMap<String, StoredAlias>,
}

impl AccountState for VerifiedPermissionsData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedVerifiedPermissionsState = Arc<RwLock<MultiAccountState<VerifiedPermissionsData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct VerifiedPermissionsSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<VerifiedPermissionsData>,
}
