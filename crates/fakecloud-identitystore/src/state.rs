//! Account-partitioned, serializable state for AWS IAM Identity Center's
//! Identity Store (`identitystore`).
//!
//! An Identity Store is a per-account directory of users, groups, and the
//! memberships linking them. Real AWS provisions the store (a `d-xxxxxxxxxx`
//! id) when an IAM Identity Center instance is enabled; we create the store
//! lazily on first write so the directory API is usable without first standing
//! up an instance. User and group attribute bags (the nested SCIM `Name`,
//! `Emails`, `Addresses`, ... shapes) are stored as the raw request `Value` so
//! they round-trip faithfully on `DescribeUser`/`DescribeGroup`.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const IDENTITYSTORE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// A user in an Identity Store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredUser {
    pub user_id: String,
    /// Unique-per-store login name, indexed for `GetUserId` / filters.
    #[serde(default)]
    pub user_name: Option<String>,
    /// Full attribute bag with PascalCase keys matching the Smithy model
    /// (`UserName`, `Name`, `DisplayName`, `Emails`, `Addresses`,
    /// `PhoneNumbers`, ...). Echoed back verbatim on describe.
    pub attributes: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A group in an Identity Store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredGroup {
    pub group_id: String,
    /// Unique-per-store display name, indexed for `GetGroupId` / filters.
    #[serde(default)]
    pub display_name: Option<String>,
    pub attributes: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A membership linking a user (`member_user_id`) into a group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMembership {
    pub membership_id: String,
    pub group_id: String,
    pub member_user_id: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A single Identity Store directory.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentityStoreDir {
    /// Users keyed by `UserId`.
    #[serde(default)]
    pub users: BTreeMap<String, StoredUser>,
    /// Groups keyed by `GroupId`.
    #[serde(default)]
    pub groups: BTreeMap<String, StoredGroup>,
    /// Memberships keyed by `MembershipId`.
    #[serde(default)]
    pub memberships: BTreeMap<String, StoredMembership>,
}

/// Per-account state: every Identity Store the account owns, keyed by
/// `IdentityStoreId` (`d-xxxxxxxxxx`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct IdentityStoreData {
    #[serde(default)]
    pub stores: BTreeMap<String, IdentityStoreDir>,
}

impl AccountState for IdentityStoreData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedIdentityStoreState = Arc<RwLock<MultiAccountState<IdentityStoreData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct IdentityStoreSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<IdentityStoreData>,
}
