//! Account-partitioned, serializable state for AWS IAM Identity Center's SSO
//! Admin (`sso-admin`) control plane.
//!
//! Everything the service owns for one account: IAM Identity Center instances
//! (each with its regions), permission sets (with their inline / managed /
//! customer-managed / boundary policies), account assignments and their async
//! operation statuses, permission-set provisioning statuses, applications (with
//! their assignments, access scopes, authentication methods, grants and
//! session/assignment configuration), trusted token issuers, per-instance
//! access-control attribute configuration, and resource tags. Nested config
//! objects are stored as the raw request `Value` so they echo back verbatim.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const SSOADMIN_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// A region enabled on an IAM Identity Center instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredRegion {
    pub region_name: String,
    pub status: String,
    pub added_date: i64,
    pub is_primary: bool,
}

/// An IAM Identity Center instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredInstance {
    pub arn: String,
    /// The `ssoins-xxxxxxxxxxxxxxxx` fragment embedded in child ARNs.
    pub ssoins: String,
    pub identity_store_id: String,
    pub owner_account_id: String,
    #[serde(default)]
    pub name: Option<String>,
    pub status: String,
    pub created_date: i64,
    #[serde(default)]
    pub regions: Vec<StoredRegion>,
    #[serde(default)]
    pub primary_region: Option<String>,
    /// Access-control attribute configuration for this instance, if created.
    /// Stores the raw `InstanceAccessControlAttributeConfiguration` value.
    #[serde(default)]
    pub attr_config: Option<Value>,
    #[serde(default)]
    pub attr_config_status: Option<String>,
}

/// A managed policy attached to a permission set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredManagedPolicy {
    pub name: String,
    pub arn: String,
}

/// A permission set within an instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPermissionSet {
    pub arn: String,
    pub instance_arn: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub session_duration: Option<String>,
    #[serde(default)]
    pub relay_state: Option<String>,
    pub created_date: i64,
    #[serde(default)]
    pub inline_policy: Option<String>,
    /// Raw `PermissionsBoundary` value.
    #[serde(default)]
    pub permissions_boundary: Option<Value>,
    #[serde(default)]
    pub managed_policies: Vec<StoredManagedPolicy>,
    /// Raw `CustomerManagedPolicyReference` values.
    #[serde(default)]
    pub customer_managed: Vec<Value>,
}

/// An account assignment linking a principal to a permission set on an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredAssignment {
    pub account_id: String,
    pub permission_set_arn: String,
    pub principal_type: String,
    pub principal_id: String,
}

/// Async status for account-assignment create/delete operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredOpStatus {
    pub request_id: String,
    pub status: String,
    pub target_id: String,
    pub target_type: String,
    pub permission_set_arn: String,
    pub principal_type: String,
    pub principal_id: String,
    pub created_date: i64,
}

/// Async status for permission-set provisioning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredProvisioningStatus {
    pub request_id: String,
    pub status: String,
    #[serde(default)]
    pub account_id: Option<String>,
    pub permission_set_arn: String,
    pub created_date: i64,
}

/// An application registered on an instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredApplication {
    pub arn: String,
    pub provider_arn: String,
    #[serde(default)]
    pub name: Option<String>,
    pub account: String,
    pub instance_arn: String,
    pub identity_store_arn: String,
    #[serde(default)]
    pub status: Option<String>,
    /// Raw `PortalOptions` value.
    #[serde(default)]
    pub portal_options: Option<Value>,
    #[serde(default)]
    pub description: Option<String>,
    pub created_date: i64,
    #[serde(default)]
    pub created_from: Option<String>,
    /// Assignments as `(principal_id, principal_type)`.
    #[serde(default)]
    pub assignments: Vec<(String, String)>,
    #[serde(default)]
    pub assignment_required: Option<bool>,
    /// scope -> raw `AuthorizedTargets` value.
    #[serde(default)]
    pub access_scopes: BTreeMap<String, Value>,
    /// authentication-method-type -> raw `AuthenticationMethod` value.
    #[serde(default)]
    pub auth_methods: BTreeMap<String, Value>,
    /// grant-type -> raw `Grant` value.
    #[serde(default)]
    pub grants: BTreeMap<String, Value>,
    #[serde(default)]
    pub session_status: Option<String>,
}

/// A trusted token issuer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTrustedTokenIssuer {
    pub arn: String,
    pub instance_arn: String,
    #[serde(default)]
    pub name: Option<String>,
    pub issuer_type: String,
    /// Raw `TrustedTokenIssuerConfiguration` value.
    #[serde(default)]
    pub config: Option<Value>,
}

/// Per-account SSO Admin state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SsoAdminData {
    /// Instances keyed by `InstanceArn`.
    #[serde(default)]
    pub instances: BTreeMap<String, StoredInstance>,
    /// Permission sets keyed by `PermissionSetArn`.
    #[serde(default)]
    pub permission_sets: BTreeMap<String, StoredPermissionSet>,
    #[serde(default)]
    pub assignments: Vec<StoredAssignment>,
    /// Account-assignment creation statuses keyed by request id.
    #[serde(default)]
    pub aa_create_status: BTreeMap<String, StoredOpStatus>,
    /// Account-assignment deletion statuses keyed by request id.
    #[serde(default)]
    pub aa_delete_status: BTreeMap<String, StoredOpStatus>,
    /// Permission-set provisioning statuses keyed by request id.
    #[serde(default)]
    pub ps_provisioning: BTreeMap<String, StoredProvisioningStatus>,
    /// Applications keyed by `ApplicationArn`.
    #[serde(default)]
    pub applications: BTreeMap<String, StoredApplication>,
    /// Trusted token issuers keyed by `TrustedTokenIssuerArn`.
    #[serde(default)]
    pub trusted_token_issuers: BTreeMap<String, StoredTrustedTokenIssuer>,
    /// Resource tags keyed by `ResourceArn` -> (key -> value).
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for SsoAdminData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedSsoAdminState = Arc<RwLock<MultiAccountState<SsoAdminData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct SsoAdminSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<SsoAdminData>,
}

/// Seed a default, always-`ACTIVE` IAM Identity Center instance for `account_id`
/// when it has none. Real AWS requires enabling IAM Identity Center before
/// instances exist, but seeding one makes the control plane (and the paired
/// Identity Store directory) usable out of the box — and lets tools that first
/// resolve `ListInstances` (e.g. Terraform's `aws_ssoadmin_instances`) proceed.
/// The instance id is derived from the account id so it is stable across
/// restarts. No-op if an instance already exists (e.g. restored from a
/// snapshot).
pub fn ensure_default_instance(state: &SharedSsoAdminState, account_id: &str, region: &str) {
    let mut guard = state.write();
    let acct = guard.get_or_create(account_id);
    if !acct.instances.is_empty() {
        return;
    }
    let pad = format!("{account_id:0<16}");
    let ssoins = format!("ssoins-{}", &pad[..16]);
    let arn = format!("arn:aws:sso:::instance/{ssoins}");
    let identity_store_id = format!("d-{}", &pad[..10]);
    let ts = chrono::Utc::now().timestamp();
    acct.instances.insert(
        arn.clone(),
        StoredInstance {
            arn,
            ssoins,
            identity_store_id,
            owner_account_id: account_id.to_string(),
            name: None,
            status: "ACTIVE".into(),
            created_date: ts,
            regions: vec![StoredRegion {
                region_name: region.to_string(),
                status: "ACTIVE".into(),
                added_date: ts,
                is_primary: true,
            }],
            primary_region: Some(region.to_string()),
            attr_config: None,
            attr_config_status: None,
        },
    );
}
