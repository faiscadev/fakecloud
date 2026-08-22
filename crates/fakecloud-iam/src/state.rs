use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IamUser {
    pub user_name: String,
    pub user_id: String,
    pub arn: String,
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub tags: Vec<Tag>,
    pub permissions_boundary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IamAccessKey {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub user_name: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IamRole {
    pub role_name: String,
    pub role_id: String,
    pub arn: String,
    pub path: String,
    pub assume_role_policy_document: String,
    pub created_at: DateTime<Utc>,
    pub description: Option<String>,
    pub max_session_duration: i32,
    pub tags: Vec<Tag>,
    pub permissions_boundary: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IamPolicy {
    pub policy_name: String,
    pub policy_id: String,
    pub arn: String,
    pub path: String,
    pub description: String,
    pub created_at: DateTime<Utc>,
    pub tags: Vec<Tag>,
    pub default_version_id: String,
    pub versions: Vec<PolicyVersion>,
    pub next_version_num: u32,
    pub attachment_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyVersion {
    pub version_id: String,
    pub document: String,
    pub is_default: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IamGroup {
    pub group_name: String,
    pub group_id: String,
    pub arn: String,
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub members: Vec<String>,                      // user names
    pub inline_policies: BTreeMap<String, String>, // policy_name -> document
    pub attached_policies: Vec<String>,            // policy ARNs
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IamInstanceProfile {
    pub instance_profile_name: String,
    pub instance_profile_id: String,
    pub arn: String,
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub roles: Vec<String>, // role names
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginProfile {
    pub user_name: String,
    pub created_at: DateTime<Utc>,
    pub password_reset_required: bool,
    /// Console password. Stored in plaintext for emulator parity —
    /// fakecloud is not a security boundary, and round-tripping it
    /// is what `ChangePassword` / `UpdateLoginProfile` need to
    /// validate against. Empty for legacy snapshots that pre-date
    /// password storage.
    #[serde(default)]
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SamlProvider {
    pub arn: String,
    pub name: String,
    pub saml_metadata_document: String,
    pub created_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcProvider {
    pub arn: String,
    pub url: String,
    pub client_id_list: Vec<String>,
    pub thumbprint_list: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerCertificate {
    pub server_certificate_name: String,
    pub server_certificate_id: String,
    pub arn: String,
    pub path: String,
    pub certificate_body: String,
    pub certificate_chain: Option<String>,
    pub upload_date: DateTime<Utc>,
    pub expiration: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SigningCertificate {
    pub certificate_id: String,
    pub user_name: String,
    pub certificate_body: String,
    pub status: String,
    pub upload_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountPasswordPolicy {
    pub minimum_password_length: u32,
    pub require_symbols: bool,
    pub require_numbers: bool,
    pub require_uppercase_characters: bool,
    pub require_lowercase_characters: bool,
    pub allow_users_to_change_password: bool,
    pub max_password_age: u32,
    pub password_reuse_prevention: u32,
    pub hard_expiry: bool,
}

impl Default for AccountPasswordPolicy {
    fn default() -> Self {
        Self {
            minimum_password_length: 6,
            require_symbols: false,
            require_numbers: false,
            require_uppercase_characters: false,
            require_lowercase_characters: false,
            allow_users_to_change_password: false,
            max_password_age: 0,
            password_reuse_prevention: 0,
            hard_expiry: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualMfaDevice {
    pub serial_number: String,
    pub base32_string_seed: String,
    pub qr_code_png: String,
    pub enable_date: Option<DateTime<Utc>>,
    pub user: Option<String>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceLinkedRoleDeletion {
    pub deletion_task_id: String,
    pub status: String,
}

/// Identity associated with a set of credentials, for GetCallerIdentity resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialIdentity {
    pub arn: String,
    pub user_id: String,
    pub account_id: String,
}

/// A temporary credential issued by STS (`AssumeRole`, `AssumeRoleWithWebIdentity`,
/// `AssumeRoleWithSAML`, `GetSessionToken`, `GetFederationToken`).
///
/// Unlike [`CredentialIdentity`], which only remembers the principal ARN for
/// `GetCallerIdentity`, this struct also retains the secret access key and
/// session token so that SigV4 verification and IAM enforcement (added in
/// later batches) can look them up when a client signs a request with
/// temporary credentials. `expiration` is the absolute wall-clock time at
/// which the credential becomes invalid.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StsTempCredential {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub principal_arn: String,
    pub user_id: String,
    pub account_id: String,
    pub expiration: DateTime<Utc>,
    /// Session policies passed to the STS call that minted this credential.
    /// Raw JSON policy documents. The `Policy` parameter contributes one
    /// entry; `PolicyArns` contribute additional entries (resolved to
    /// documents at mint time). Empty when the STS call carried no
    /// session policies.
    #[serde(default)]
    pub session_policies: Vec<String>,
    /// True iff the AssumeRole / GetSessionToken call that minted this
    /// credential supplied MFA (`SerialNumber` + `TokenCode`). Surfaces
    /// to downstream IAM evaluation as `aws:MultiFactorAuthPresent`.
    #[serde(default)]
    pub mfa_present: bool,
    /// Wall-clock time at which the credential was issued. Surfaces to
    /// downstream IAM evaluation as `aws:TokenIssueTime` and feeds
    /// `aws:MultiFactorAuthAge` (computed at evaluation time as
    /// `now - issued_at` in seconds when MFA was asserted).
    #[serde(default = "default_issued_at")]
    pub issued_at: DateTime<Utc>,
    /// `aws:FederatedProvider` — for AssumeRoleWithSAML this is the
    /// SAML provider ARN; for AssumeRoleWithWebIdentity it is the OIDC
    /// provider ARN (or a friendly host name like
    /// `cognito-identity.amazonaws.com`); `None` for plain AssumeRole /
    /// GetSessionToken / GetFederationToken.
    #[serde(default)]
    pub federated_provider: Option<String>,
}

/// Default for [`StsTempCredential::issued_at`] when deserializing
/// snapshots written before the field existed.
fn default_issued_at() -> DateTime<Utc> {
    Utc::now()
}

/// Result of looking up a set of credentials by access key ID.
///
/// Carries the secret + resolved principal + owning account id. The account
/// id is intentionally read from the credential itself rather than from
/// global config, so that once #381 (multi-account isolation) lands, the same
/// lookup already returns the correct account for the credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretLookup {
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub principal_arn: String,
    pub user_id: String,
    pub account_id: String,
    /// Session policies from the STS call that minted this credential.
    /// Empty for IAM user access keys.
    pub session_policies: Vec<String>,
    /// Tags on the principal (IAM user or assumed role) for
    /// `aws:PrincipalTag/<key>` condition evaluation.
    pub principal_tags: Option<BTreeMap<String, String>>,
    /// True when the underlying STS credential was minted with MFA;
    /// surfaces as `aws:MultiFactorAuthPresent` to downstream IAM
    /// evaluation. Always false for raw IAM user access keys.
    pub mfa_present: bool,
    /// Wall-clock time at which the underlying STS credential was
    /// issued. Surfaces as `aws:TokenIssueTime` and drives
    /// `aws:MultiFactorAuthAge`. `None` for raw IAM user access keys
    /// (AWS does not expose `aws:TokenIssueTime` for long-lived
    /// credentials).
    pub token_issued_at: Option<DateTime<Utc>>,
    /// `aws:FederatedProvider` for the underlying credential, when
    /// applicable. Set by AssumeRoleWithSAML / AssumeRoleWithWebIdentity;
    /// always `None` for IAM user keys, plain AssumeRole, GetSessionToken,
    /// and GetFederationToken.
    pub federated_provider: Option<String>,
}

/// Convert a `Vec<Tag>` to a `BTreeMap<String, String>`.
/// Returns `None` when the input is empty (no tags to evaluate).
pub fn tags_to_hashmap(tags: &[Tag]) -> Option<BTreeMap<String, String>> {
    if tags.is_empty() {
        return None;
    }
    Some(
        tags.iter()
            .map(|t| (t.key.clone(), t.value.clone()))
            .collect(),
    )
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SshPublicKey {
    pub ssh_public_key_id: String,
    pub user_name: String,
    pub ssh_public_key_body: String,
    pub status: String,
    pub upload_date: DateTime<Utc>,
    pub fingerprint: String,
}

/// Tracks when an access key was last used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessKeyLastUsed {
    pub last_used_date: DateTime<Utc>,
    pub service_name: String,
    pub region: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IamState {
    pub account_id: String,
    pub users: BTreeMap<String, IamUser>,
    pub access_keys: BTreeMap<String, Vec<IamAccessKey>>, // username -> keys
    pub roles: BTreeMap<String, IamRole>,
    pub policies: BTreeMap<String, IamPolicy>, // arn -> policy
    pub role_policies: BTreeMap<String, Vec<String>>, // role_name -> managed policy arns
    pub role_inline_policies: BTreeMap<String, BTreeMap<String, String>>, // role_name -> {policy_name -> doc}
    pub user_policies: BTreeMap<String, Vec<String>>, // user_name -> managed policy arns
    pub user_inline_policies: BTreeMap<String, BTreeMap<String, String>>, // user_name -> {policy_name -> doc}
    pub groups: BTreeMap<String, IamGroup>,
    pub instance_profiles: BTreeMap<String, IamInstanceProfile>,
    pub login_profiles: BTreeMap<String, LoginProfile>,
    pub saml_providers: BTreeMap<String, SamlProvider>, // arn -> provider
    pub oidc_providers: BTreeMap<String, OidcProvider>, // arn -> provider
    pub server_certificates: BTreeMap<String, ServerCertificate>, // name -> cert
    pub signing_certificates: BTreeMap<String, Vec<SigningCertificate>>, // user_name -> certs
    pub account_aliases: Vec<String>,
    pub account_password_policy: Option<AccountPasswordPolicy>,
    /// Account-level key/value properties set via PutAccountProperties and
    /// read back by GetAccountProperties. Keys are free-form strings the
    /// caller chooses; values are stored verbatim as strings.
    #[serde(default)]
    pub account_properties: BTreeMap<String, String>,
    pub virtual_mfa_devices: BTreeMap<String, VirtualMfaDevice>, // serial_number -> device
    pub service_linked_role_deletions: BTreeMap<String, ServiceLinkedRoleDeletion>,
    /// Maps access key ID to the identity that should be returned by GetCallerIdentity.
    pub credential_identities: BTreeMap<String, CredentialIdentity>,
    /// Temporary credentials issued by STS, keyed by access key ID. Includes
    /// the secret access key and session token — required for SigV4
    /// verification and IAM enforcement. Expired entries are purged lazily on
    /// lookup.
    pub sts_temp_credentials: BTreeMap<String, StsTempCredential>,
    pub credential_report_generated: bool,
    pub ssh_public_keys: BTreeMap<String, Vec<SshPublicKey>>, // user_name -> keys
    pub access_key_last_used: BTreeMap<String, AccessKeyLastUsed>,
    /// Per-user service-specific credentials (Codecommit/Keyspaces).
    #[serde(default)]
    pub service_specific_credentials: BTreeMap<String, Vec<ServiceSpecificCredential>>, // user -> creds
    /// Per-resource-arn tag map for SAML/Server cert/MFA device tags.
    #[serde(default)]
    pub extra_tags: BTreeMap<String, Vec<(String, String)>>,
    /// Organizations integration toggles.
    #[serde(default)]
    pub organizations_root_credentials_management: bool,
    #[serde(default)]
    pub organizations_root_sessions: bool,
    /// Generated ServiceLastAccessed jobs keyed by job id.
    #[serde(default)]
    pub service_last_accessed_jobs: BTreeMap<String, ServiceLastAccessedJob>,
    /// Organizations access reports keyed by job id.
    #[serde(default)]
    pub organizations_access_reports: BTreeMap<String, OrganizationsAccessReport>,
    /// `SetSecurityTokenServicePreferences` value (e.g. `v1Token`,
    /// `v2Token`). `None` means caller hasn't configured a preference.
    #[serde(default)]
    pub global_endpoint_token_version: Option<String>,
    /// Delegation requests keyed by id. Records every state transition
    /// (`PENDING` -> `ACCEPTED`/`REJECTED`/`SENT`) and the parameters
    /// supplied at create-time so `GetDelegationRequest` can roundtrip
    /// them.
    #[serde(default)]
    pub delegation_requests: BTreeMap<String, DelegationRequest>,
    /// Whether outbound web identity federation is enabled for this
    /// account. Toggled by `EnableOutboundWebIdentityFederation` /
    /// `DisableOutboundWebIdentityFederation`.
    #[serde(default)]
    pub outbound_web_identity_federation_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DelegationRequest {
    pub id: String,
    pub owner_account_id: Option<String>,
    pub description: String,
    pub request_message: Option<String>,
    pub requestor_workflow_id: String,
    pub redirect_url: Option<String>,
    pub notification_channel: String,
    pub session_duration: i64,
    pub only_send_by_owner: bool,
    pub status: String,
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
    pub policy_template_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSpecificCredential {
    pub credential_id: String,
    pub user_name: String,
    pub service_name: String,
    pub service_user_name: String,
    pub service_password: String,
    pub status: String,
    pub create_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceLastAccessedJob {
    pub job_id: String,
    pub status: String,
    pub job_creation_date: DateTime<Utc>,
    pub arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrganizationsAccessReport {
    pub job_id: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub entity_path: String,
}

impl IamState {
    pub fn new(account_id: &str) -> Self {
        let mut state = Self {
            account_id: account_id.to_string(),
            users: BTreeMap::new(),
            access_keys: BTreeMap::new(),
            roles: BTreeMap::new(),
            policies: BTreeMap::new(),
            role_policies: BTreeMap::new(),
            role_inline_policies: BTreeMap::new(),
            user_policies: BTreeMap::new(),
            user_inline_policies: BTreeMap::new(),
            groups: BTreeMap::new(),
            instance_profiles: BTreeMap::new(),
            login_profiles: BTreeMap::new(),
            saml_providers: BTreeMap::new(),
            oidc_providers: BTreeMap::new(),
            server_certificates: BTreeMap::new(),
            signing_certificates: BTreeMap::new(),
            account_aliases: Vec::new(),
            account_password_policy: None,
            account_properties: BTreeMap::new(),
            virtual_mfa_devices: BTreeMap::new(),
            service_linked_role_deletions: BTreeMap::new(),
            credential_identities: BTreeMap::new(),
            sts_temp_credentials: BTreeMap::new(),
            credential_report_generated: false,
            ssh_public_keys: BTreeMap::new(),
            access_key_last_used: BTreeMap::new(),
            service_specific_credentials: BTreeMap::new(),
            extra_tags: BTreeMap::new(),
            organizations_root_credentials_management: false,
            organizations_root_sessions: false,
            service_last_accessed_jobs: BTreeMap::new(),
            organizations_access_reports: BTreeMap::new(),
            global_endpoint_token_version: None,
            delegation_requests: BTreeMap::new(),
            outbound_web_identity_federation_enabled: false,
        };
        state.seed_default_service_linked_roles();
        state
    }

    /// AWS accounts ship with a handful of service-linked roles created
    /// automatically (Support, Trusted Advisor, ...). `aws_iam_roles` and other
    /// callers expect a non-empty `ListRoles` on a fresh account, so seed them.
    fn seed_default_service_linked_roles(&mut self) {
        let now = Utc::now();
        for (service, name) in [
            ("support.amazonaws.com", "AWSServiceRoleForSupport"),
            (
                "trustedadvisor.amazonaws.com",
                "AWSServiceRoleForTrustedAdvisor",
            ),
        ] {
            let path = format!("/aws-service-role/{service}/");
            let arn = format!("arn:aws:iam::{}:role{}{}", self.account_id, path, name);
            self.roles.insert(
                name.to_string(),
                IamRole {
                    role_name: name.to_string(),
                    role_id: format!("AROA{:0>17}", name.len()),
                    arn,
                    path,
                    assume_role_policy_document: format!(
                        "{{\"Version\":\"2012-10-17\",\"Statement\":[{{\"Effect\":\"Allow\",\"Principal\":{{\"Service\":\"{service}\"}},\"Action\":\"sts:AssumeRole\"}}]}}"
                    ),
                    created_at: now,
                    description: None,
                    max_session_duration: 3600,
                    tags: Vec::new(),
                    permissions_boundary: None,
                },
            );
        }
    }

    pub fn reset(&mut self) {
        let account_id = self.account_id.clone();
        *self = Self::new(&account_id);
    }

    /// Look up the secret access key, session token, and resolved principal
    /// for a given access key ID.
    ///
    /// Searches IAM user access keys first, then STS temporary credentials.
    /// Expired STS temporary credentials are purged in-place and skipped.
    ///
    /// Returns `None` if the AKID is unknown or its STS credential has
    /// expired.
    ///
    /// Required for SigV4 signature verification (batch 3) and principal
    /// resolution (batch 4). Callers must hold a write lock on
    /// [`IamState`] to allow the lazy purge; read-only callers should use
    /// [`IamState::credential_secret_readonly`].
    pub fn credential_secret(&mut self, access_key_id: &str) -> Option<SecretLookup> {
        // IAM user access keys: look up by scanning (same pattern the
        // existing GetCallerIdentity path uses).
        for keys in self.access_keys.values() {
            for key in keys {
                // Deactivated (Inactive) keys must not authenticate; AWS
                // returns InvalidClientTokenId. Skipping makes the lookup miss
                // (bug-audit 2026-05-28, 5.1).
                if key.access_key_id == access_key_id && key.status == "Active" {
                    if let Some(user) = self.users.get(&key.user_name) {
                        return Some(SecretLookup {
                            secret_access_key: key.secret_access_key.clone(),
                            session_token: None,
                            principal_arn: user.arn.clone(),
                            user_id: user.user_id.clone(),
                            account_id: self.account_id.clone(),
                            session_policies: Vec::new(),
                            principal_tags: tags_to_hashmap(&user.tags),
                            mfa_present: false,
                            token_issued_at: None,
                            federated_provider: None,
                        });
                    }
                }
            }
        }

        // STS temporary credentials: direct hash lookup, with lazy expiry
        // purging so expired entries don't accumulate.
        let now = Utc::now();
        if let Some(temp) = self.sts_temp_credentials.get(access_key_id) {
            if temp.expiration > now {
                let principal_tags = self.resolve_role_tags(&temp.principal_arn);
                return Some(SecretLookup {
                    secret_access_key: temp.secret_access_key.clone(),
                    session_token: Some(temp.session_token.clone()),
                    principal_arn: temp.principal_arn.clone(),
                    user_id: temp.user_id.clone(),
                    account_id: temp.account_id.clone(),
                    session_policies: temp.session_policies.clone(),
                    principal_tags,
                    mfa_present: temp.mfa_present,
                    token_issued_at: Some(temp.issued_at),
                    federated_provider: temp.federated_provider.clone(),
                });
            }
            self.sts_temp_credentials.remove(access_key_id);
        }
        None
    }

    /// Read-only variant of [`IamState::credential_secret`] that does not
    /// purge expired entries. Prefer the mutable variant wherever possible
    /// to keep the temp-credential table small.
    pub fn credential_secret_readonly(&self, access_key_id: &str) -> Option<SecretLookup> {
        for keys in self.access_keys.values() {
            for key in keys {
                // Deactivated (Inactive) keys must not authenticate; AWS
                // returns InvalidClientTokenId. Skipping makes the lookup miss
                // (bug-audit 2026-05-28, 5.1).
                if key.access_key_id == access_key_id && key.status == "Active" {
                    if let Some(user) = self.users.get(&key.user_name) {
                        return Some(SecretLookup {
                            secret_access_key: key.secret_access_key.clone(),
                            session_token: None,
                            principal_arn: user.arn.clone(),
                            user_id: user.user_id.clone(),
                            account_id: self.account_id.clone(),
                            session_policies: Vec::new(),
                            principal_tags: tags_to_hashmap(&user.tags),
                            mfa_present: false,
                            token_issued_at: None,
                            federated_provider: None,
                        });
                    }
                }
            }
        }

        let now = Utc::now();
        let temp = self.sts_temp_credentials.get(access_key_id)?;
        if temp.expiration <= now {
            return None;
        }
        let principal_tags = self.resolve_role_tags(&temp.principal_arn);
        Some(SecretLookup {
            secret_access_key: temp.secret_access_key.clone(),
            session_token: Some(temp.session_token.clone()),
            principal_arn: temp.principal_arn.clone(),
            user_id: temp.user_id.clone(),
            account_id: temp.account_id.clone(),
            session_policies: temp.session_policies.clone(),
            principal_tags,
            mfa_present: temp.mfa_present,
            token_issued_at: Some(temp.issued_at),
            federated_provider: temp.federated_provider.clone(),
        })
    }

    /// Resolve role tags from an assumed-role principal ARN.
    /// ARN format: `arn:aws:sts::<account>:assumed-role/<role-name>/<session>`
    /// Looks up the role by name and returns its tags.
    fn resolve_role_tags(&self, principal_arn: &str) -> Option<BTreeMap<String, String>> {
        // assumed-role ARNs: arn:aws:sts::<account>:assumed-role/<role>/<session>
        let parts: Vec<&str> = principal_arn.split(':').collect();
        if parts.len() < 6 {
            return None;
        }
        let resource = parts[5];
        if let Some(rest) = resource.strip_prefix("assumed-role/") {
            let role_name = rest.split('/').next()?;
            let role = self.roles.get(role_name)?;
            return tags_to_hashmap(&role.tags);
        }
        None
    }
}

impl AccountState for IamState {
    fn new_for_account(account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::new(account_id)
    }
}

pub type SharedIamState = std::sync::Arc<RwLock<MultiAccountState<IamState>>>;

/// On-disk snapshot envelope for IAM state. Versioned so future schema
/// changes fail loudly instead of silently corrupting state.
///
/// Schema v2 stores multi-account state. v1 snapshots are migrated on
/// load by wrapping the single `IamState` as the default account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IamSnapshot {
    pub schema_version: u32,
    /// v2+: multi-account state. Present when `schema_version >= 2`.
    #[serde(default)]
    pub accounts: Option<MultiAccountState<IamState>>,
    /// v1 compat: single-account state. Present when `schema_version == 1`.
    #[serde(default)]
    pub state: Option<IamState>,
}

pub const IAM_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_aws::arn::Arn;

    fn iam_user(name: &str, account_id: &str) -> IamUser {
        IamUser {
            user_name: name.to_string(),
            user_id: format!("AIDA{}", name.to_uppercase()),
            arn: Arn::global("iam", account_id, &format!("user/{name}")).to_string(),
            path: "/".to_string(),
            created_at: Utc::now(),
            tags: Vec::new(),
            permissions_boundary: None,
        }
    }

    fn iam_key(user: &str, akid: &str, secret: &str) -> IamAccessKey {
        IamAccessKey {
            access_key_id: akid.to_string(),
            secret_access_key: secret.to_string(),
            user_name: user.to_string(),
            status: "Active".to_string(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn credential_secret_returns_iam_user_key() {
        let mut state = IamState::new("123456789012");
        state
            .users
            .insert("alice".to_string(), iam_user("alice", "123456789012"));
        state.access_keys.insert(
            "alice".to_string(),
            vec![iam_key("alice", "FKIAALICE", "secret-alice")],
        );
        let lookup = state.credential_secret("FKIAALICE").unwrap();
        assert_eq!(lookup.secret_access_key, "secret-alice");
        assert_eq!(lookup.principal_arn, "arn:aws:iam::123456789012:user/alice");
        assert_eq!(lookup.account_id, "123456789012");
        assert_eq!(lookup.session_token, None);
    }

    #[test]
    fn credential_secret_returns_sts_temp_credential_when_unexpired() {
        let mut state = IamState::new("123456789012");
        state.sts_temp_credentials.insert(
            "FSIATEMPKEY".to_string(),
            StsTempCredential {
                access_key_id: "FSIATEMPKEY".to_string(),
                secret_access_key: "temp-secret".to_string(),
                session_token: "temp-token".to_string(),
                principal_arn: "arn:aws:sts::123456789012:assumed-role/R/s".to_string(),
                user_id: "AROA:session".to_string(),
                account_id: "123456789012".to_string(),
                expiration: Utc::now() + chrono::Duration::minutes(30),
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );
        let lookup = state.credential_secret("FSIATEMPKEY").unwrap();
        assert_eq!(lookup.secret_access_key, "temp-secret");
        assert_eq!(lookup.session_token.as_deref(), Some("temp-token"));
        assert_eq!(
            lookup.principal_arn,
            "arn:aws:sts::123456789012:assumed-role/R/s"
        );
    }

    #[test]
    fn credential_secret_purges_expired_sts_credentials() {
        let mut state = IamState::new("123456789012");
        state.sts_temp_credentials.insert(
            "FSIAOLD".to_string(),
            StsTempCredential {
                access_key_id: "FSIAOLD".to_string(),
                secret_access_key: "s".to_string(),
                session_token: "t".to_string(),
                principal_arn: "arn".to_string(),
                user_id: "id".to_string(),
                account_id: "123456789012".to_string(),
                expiration: Utc::now() - chrono::Duration::seconds(1),
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );
        assert!(state.credential_secret("FSIAOLD").is_none());
        assert!(!state.sts_temp_credentials.contains_key("FSIAOLD"));
    }

    #[test]
    fn credential_secret_readonly_does_not_purge() {
        let mut state = IamState::new("123456789012");
        state.sts_temp_credentials.insert(
            "FSIAOLD".to_string(),
            StsTempCredential {
                access_key_id: "FSIAOLD".to_string(),
                secret_access_key: "s".to_string(),
                session_token: "t".to_string(),
                principal_arn: "arn".to_string(),
                user_id: "id".to_string(),
                account_id: "123456789012".to_string(),
                expiration: Utc::now() - chrono::Duration::seconds(1),
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );
        assert!(state.credential_secret_readonly("FSIAOLD").is_none());
        assert!(state.sts_temp_credentials.contains_key("FSIAOLD"));
    }

    #[test]
    fn credential_secret_returns_none_for_unknown_akid() {
        let mut state = IamState::new("123456789012");
        assert!(state.credential_secret("FKIAUNKNOWN").is_none());
    }

    // bug-audit 2026-05-28, 5.1: a deactivated (Inactive) access key must not
    // authenticate, so the credential lookup must miss.
    #[test]
    fn credential_secret_skips_inactive_key() {
        let mut state = IamState::new("123456789012");
        let mut key = iam_key("alice", "FKIAALICE", "secret123");
        key.status = "Inactive".to_string();
        state.access_keys.insert("alice".to_string(), vec![key]);
        assert!(state.credential_secret("FKIAALICE").is_none());
    }
}
