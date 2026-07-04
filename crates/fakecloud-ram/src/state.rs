//! Account-partitioned, serializable state for AWS Resource Access Manager.
//!
//! Models the RAM control plane: resource shares (with their resource and
//! principal associations and attached permissions), resource-share
//! invitations, customer-managed permissions (with versions), and tags. AWS
//! seeds a catalogue of managed default permissions and supported resource
//! types; those are constant data served from [`service`](crate::service)
//! rather than stored per-account.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const RAM_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

pub type TagMap = BTreeMap<String, String>;

/// A resource-share / principal association within a share.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssociationRecord {
    /// The associated entity: a resource ARN (RESOURCE) or a principal id
    /// (PRINCIPAL) or a source account id (SOURCE).
    pub associated_entity: String,
    /// One of `RESOURCE`, `PRINCIPAL`.
    pub association_type: String,
    /// Progresses synthetically to `ASSOCIATED` on create.
    pub status: String,
    pub status_message: Option<String>,
    pub creation_time: DateTime<Utc>,
    pub last_updated_time: DateTime<Utc>,
    /// True when the associated entity is an account outside the caller's org.
    pub external: bool,
}

/// A resource share owned by (or shared with) the account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceShareRecord {
    pub arn: String,
    pub name: String,
    pub owning_account_id: String,
    pub allow_external_principals: bool,
    pub status: String,
    pub status_message: Option<String>,
    pub creation_time: DateTime<Utc>,
    pub last_updated_time: DateTime<Utc>,
    pub feature_set: String,
    pub tags: TagMap,
    /// `retainSharingOnAccountLeaveOrganization` from a `ResourceShareConfiguration`.
    pub retain_sharing: Option<bool>,
    pub resource_associations: Vec<AssociationRecord>,
    pub principal_associations: Vec<AssociationRecord>,
    pub permission_arns: Vec<String>,
}

/// A single version of a customer-managed permission.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionVersionRecord {
    pub version: u32,
    pub policy_template: String,
    pub is_default: bool,
    pub creation_time: DateTime<Utc>,
    pub last_updated_time: DateTime<Utc>,
}

/// A customer-managed permission with its versions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PermissionRecord {
    pub arn: String,
    pub name: String,
    pub resource_type: String,
    pub status: String,
    pub feature_set: String,
    pub creation_time: DateTime<Utc>,
    pub last_updated_time: DateTime<Utc>,
    pub default_version: u32,
    pub versions: Vec<PermissionVersionRecord>,
    pub tags: TagMap,
}

/// A resource-share invitation delivered to an external account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvitationRecord {
    pub arn: String,
    pub resource_share_arn: String,
    pub resource_share_name: String,
    pub sender_account_id: String,
    pub receiver_account_id: String,
    pub invitation_timestamp: DateTime<Utc>,
    pub status: String,
}

/// The account-scoped RAM state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RamState {
    #[serde(default)]
    pub resource_shares: BTreeMap<String, ResourceShareRecord>,
    #[serde(default)]
    pub permissions: BTreeMap<String, PermissionRecord>,
    #[serde(default)]
    pub invitations: BTreeMap<String, InvitationRecord>,
    /// Replace-permission-associations work items, keyed by work id, stored as
    /// the JSON object echoed back by `ListReplacePermissionAssociationsWork`.
    #[serde(default)]
    pub replace_works: BTreeMap<String, serde_json::Value>,
    /// Tags keyed by resource ARN (resource-share or permission ARN).
    #[serde(default)]
    pub tags: BTreeMap<String, TagMap>,
}

impl AccountState for RamState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedRamState = Arc<RwLock<MultiAccountState<RamState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct RamSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<RamState>,
}

// ---------------------------------------------------------------------------
// ARN builders
// ---------------------------------------------------------------------------

pub fn resource_share_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:ram:{region}:{account_id}:resource-share/{id}")
}

pub fn invitation_arn(region: &str, account_id: &str, id: &str) -> String {
    format!("arn:aws:ram:{region}:{account_id}:resource-share-invitation/{id}")
}

/// ARN for a customer-managed permission.
pub fn permission_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("arn:aws:ram:{region}:{account_id}:permission/{name}")
}

/// ARN for an AWS-managed default permission (partition-scoped, no region/account).
pub fn managed_permission_arn(name: &str) -> String {
    format!("arn:aws:ram::aws:permission/{name}")
}
