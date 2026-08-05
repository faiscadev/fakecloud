//! Account-partitioned, serializable state for AWS Account Management.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const ACCOUNT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The three alternate-contact slots an account exposes.
pub const ALTERNATE_CONTACT_TYPES: &[&str] = &["BILLING", "OPERATIONS", "SECURITY"];

/// An alternate contact (billing / operations / security) for an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlternateContact {
    pub name: String,
    pub title: String,
    pub email_address: String,
    pub phone_number: String,
    /// BILLING | OPERATIONS | SECURITY.
    pub contact_type: String,
}

/// A pending primary-email change awaiting OTP confirmation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEmailUpdate {
    pub email: String,
    pub otp: String,
}

/// Per-account state. Contact information is stored as the raw request `Value`
/// so the full `ContactInformation` shape round-trips faithfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountData {
    /// Alternate contacts keyed by type (BILLING/OPERATIONS/SECURITY).
    #[serde(default)]
    pub alternate_contacts: BTreeMap<String, AlternateContact>,
    /// The primary `ContactInformation` object, if `PutContactInformation` ran.
    #[serde(default)]
    pub contact_information: Option<serde_json::Value>,
    /// The account name (`PutAccountName`), if set.
    #[serde(default)]
    pub account_name: Option<String>,
    pub created_date: DateTime<Utc>,
    /// AccountState: ACTIVE | SUSPENDED | PENDING_CLOSURE.
    pub account_state: String,
    /// The account's primary (root) email address.
    pub primary_email: String,
    /// A primary-email change awaiting `AcceptPrimaryEmailUpdate`.
    #[serde(default)]
    pub pending_email_update: Option<PendingEmailUpdate>,
    /// When the primary-email update status last changed (a `StartPrimaryEmail
    /// Update` or `AcceptPrimaryEmailUpdate`), surfaced as `UpdatedAt` by
    /// `GetPrimaryEmailUpdateStatus`.
    #[serde(default)]
    pub primary_email_update_at: Option<DateTime<Utc>>,
    /// Per-region opt status overrides. A region absent here reports its default
    /// (opt-in regions -> DISABLED, all others -> ENABLED_BY_DEFAULT).
    #[serde(default)]
    pub region_opt_status: BTreeMap<String, String>,
}

impl Default for AccountData {
    fn default() -> Self {
        Self {
            alternate_contacts: BTreeMap::new(),
            contact_information: None,
            account_name: None,
            created_date: Utc::now(),
            account_state: "ACTIVE".to_string(),
            primary_email: String::new(),
            pending_email_update: None,
            primary_email_update_at: None,
            region_opt_status: BTreeMap::new(),
        }
    }
}

/// Every AWS Region and whether it is opt-in (disabled by default). Opt-in
/// regions report `DISABLED` until `EnableRegion` is called; every other region
/// reports `ENABLED_BY_DEFAULT`.
pub const REGIONS: &[(&str, bool)] = &[
    ("us-east-1", false),
    ("us-east-2", false),
    ("us-west-1", false),
    ("us-west-2", false),
    ("af-south-1", true),
    ("ap-east-1", true),
    ("ap-east-2", true),
    ("ap-south-1", false),
    ("ap-south-2", true),
    ("ap-northeast-1", false),
    ("ap-northeast-2", false),
    ("ap-northeast-3", false),
    ("ap-southeast-1", false),
    ("ap-southeast-2", false),
    ("ap-southeast-3", true),
    ("ap-southeast-4", true),
    ("ap-southeast-5", true),
    ("ap-southeast-7", true),
    ("ca-central-1", false),
    ("ca-west-1", true),
    ("eu-central-1", false),
    ("eu-central-2", true),
    ("eu-west-1", false),
    ("eu-west-2", false),
    ("eu-west-3", false),
    ("eu-north-1", false),
    ("eu-south-1", true),
    ("eu-south-2", true),
    ("il-central-1", true),
    ("me-central-1", true),
    ("me-south-1", true),
    ("mx-central-1", true),
    ("sa-east-1", false),
];

/// The default opt status for `region` (absent an explicit override).
pub fn default_region_status(region: &str) -> Option<&'static str> {
    REGIONS
        .iter()
        .find(|(r, _)| *r == region)
        .map(|(_, opt_in)| {
            if *opt_in {
                "DISABLED"
            } else {
                "ENABLED_BY_DEFAULT"
            }
        })
}

impl AccountState for AccountData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedAccountState = Arc<RwLock<MultiAccountState<AccountData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<AccountData>,
}
