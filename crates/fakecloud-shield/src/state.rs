//! Account-partitioned, serializable state for AWS Shield / Shield Advanced.
//!
//! Shield resources are stored as assembled, response-shaped `serde_json::Value`s
//! keyed by their natural identifiers so a read returns exactly what was written
//! (round-trip fidelity) and the on-the-wire JSON never carries a field that
//! isn't in the Smithy model. The subscription, emergency-contact list, DRT
//! access (role + log buckets) and proactive-engagement status are
//! account-level singletons; protections, protection groups, per-resource
//! application-layer automatic-response configs and tags are keyed maps.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const SHIELD_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The account-scoped Shield state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShieldState {
    /// Protections keyed by `ProtectionId` (a 36-char id); value is a
    /// `Protection` shape JSON.
    #[serde(default)]
    pub protections: BTreeMap<String, Value>,
    /// Insertion order of protection ids.
    #[serde(default)]
    pub protection_order: Vec<String>,
    /// Protection groups keyed by `ProtectionGroupId`; value is a
    /// `ProtectionGroup` shape JSON.
    #[serde(default)]
    pub protection_groups: BTreeMap<String, Value>,
    /// Insertion order of protection-group ids.
    #[serde(default)]
    pub protection_group_order: Vec<String>,
    /// The account's Shield Advanced `Subscription` shape JSON, if subscribed.
    #[serde(default)]
    pub subscription: Option<Value>,
    /// Emergency-contact list (`[{EmailAddress, PhoneNumber?, ContactNotes?}]`).
    #[serde(default)]
    pub emergency_contacts: Vec<Value>,
    /// The DDoS Response Team (DRT) role ARN, if a role has been associated.
    #[serde(default)]
    pub drt_role_arn: Option<String>,
    /// The DRT log-access buckets (`[bucketName]`).
    #[serde(default)]
    pub drt_log_buckets: Vec<String>,
    /// Proactive-engagement status: `ENABLED` / `DISABLED` / `PENDING`.
    #[serde(default)]
    pub proactive_engagement_status: Option<String>,
    /// Tags keyed by resource ARN; value is a `TagList` (`[{Key,Value}]`).
    #[serde(default)]
    pub tags: BTreeMap<String, Vec<Value>>,
}

impl AccountState for ShieldState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedShieldState = Arc<RwLock<MultiAccountState<ShieldState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ShieldSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<ShieldState>,
}
