use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct IamUser {
    pub user_name: String,
    pub user_id: String,
    pub arn: String,
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub tags: Vec<Tag>,
    pub permissions_boundary: Option<String>,
}

#[derive(Debug, Clone)]
pub struct IamAccessKey {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub user_name: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct PolicyVersion {
    pub version_id: String,
    pub document: String,
    pub is_default: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct IamGroup {
    pub group_name: String,
    pub group_id: String,
    pub arn: String,
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub members: Vec<String>,                     // user names
    pub inline_policies: HashMap<String, String>, // policy_name -> document
    pub attached_policies: Vec<String>,           // policy ARNs
}

#[derive(Debug, Clone)]
pub struct IamInstanceProfile {
    pub instance_profile_name: String,
    pub instance_profile_id: String,
    pub arn: String,
    pub path: String,
    pub created_at: DateTime<Utc>,
    pub roles: Vec<String>, // role names
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone)]
pub struct LoginProfile {
    pub user_name: String,
    pub created_at: DateTime<Utc>,
    pub password_reset_required: bool,
}

#[derive(Debug, Clone)]
pub struct SamlProvider {
    pub arn: String,
    pub name: String,
    pub saml_metadata_document: String,
    pub created_at: DateTime<Utc>,
    pub valid_until: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone)]
pub struct OidcProvider {
    pub arn: String,
    pub url: String,
    pub client_id_list: Vec<String>,
    pub thumbprint_list: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct SigningCertificate {
    pub certificate_id: String,
    pub user_name: String,
    pub certificate_body: String,
    pub status: String,
    pub upload_date: DateTime<Utc>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct VirtualMfaDevice {
    pub serial_number: String,
    pub base32_string_seed: String,
    pub qr_code_png: String,
    pub enable_date: Option<DateTime<Utc>>,
    pub user: Option<String>,
    pub tags: Vec<Tag>,
}

#[derive(Debug, Clone)]
pub struct ServiceLinkedRoleDeletion {
    pub deletion_task_id: String,
    pub status: String,
}

/// Identity associated with a set of credentials, for GetCallerIdentity resolution.
#[derive(Debug, Clone)]
pub struct CredentialIdentity {
    pub arn: String,
    pub user_id: String,
    pub account_id: String,
}

#[derive(Debug, Clone)]
pub struct SshPublicKey {
    pub ssh_public_key_id: String,
    pub user_name: String,
    pub ssh_public_key_body: String,
    pub status: String,
    pub upload_date: DateTime<Utc>,
    pub fingerprint: String,
}

/// Tracks when an access key was last used.
#[derive(Debug, Clone)]
pub struct AccessKeyLastUsed {
    pub last_used_date: DateTime<Utc>,
    pub service_name: String,
    pub region: String,
}

pub struct IamState {
    pub account_id: String,
    pub users: HashMap<String, IamUser>,
    pub access_keys: HashMap<String, Vec<IamAccessKey>>, // username -> keys
    pub roles: HashMap<String, IamRole>,
    pub policies: HashMap<String, IamPolicy>, // arn -> policy
    pub role_policies: HashMap<String, Vec<String>>, // role_name -> managed policy arns
    pub role_inline_policies: HashMap<String, HashMap<String, String>>, // role_name -> {policy_name -> doc}
    pub user_policies: HashMap<String, Vec<String>>, // user_name -> managed policy arns
    pub user_inline_policies: HashMap<String, HashMap<String, String>>, // user_name -> {policy_name -> doc}
    pub groups: HashMap<String, IamGroup>,
    pub instance_profiles: HashMap<String, IamInstanceProfile>,
    pub login_profiles: HashMap<String, LoginProfile>,
    pub saml_providers: HashMap<String, SamlProvider>, // arn -> provider
    pub oidc_providers: HashMap<String, OidcProvider>, // arn -> provider
    pub server_certificates: HashMap<String, ServerCertificate>, // name -> cert
    pub signing_certificates: HashMap<String, Vec<SigningCertificate>>, // user_name -> certs
    pub account_aliases: Vec<String>,
    pub account_password_policy: Option<AccountPasswordPolicy>,
    pub virtual_mfa_devices: HashMap<String, VirtualMfaDevice>, // serial_number -> device
    pub service_linked_role_deletions: HashMap<String, ServiceLinkedRoleDeletion>,
    /// Maps access key ID to the identity that should be returned by GetCallerIdentity.
    pub credential_identities: HashMap<String, CredentialIdentity>,
    pub credential_report_generated: bool,
    pub ssh_public_keys: HashMap<String, Vec<SshPublicKey>>, // user_name -> keys
    pub access_key_last_used: HashMap<String, AccessKeyLastUsed>,
}

impl IamState {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            users: HashMap::new(),
            access_keys: HashMap::new(),
            roles: HashMap::new(),
            policies: HashMap::new(),
            role_policies: HashMap::new(),
            role_inline_policies: HashMap::new(),
            user_policies: HashMap::new(),
            user_inline_policies: HashMap::new(),
            groups: HashMap::new(),
            instance_profiles: HashMap::new(),
            login_profiles: HashMap::new(),
            saml_providers: HashMap::new(),
            oidc_providers: HashMap::new(),
            server_certificates: HashMap::new(),
            signing_certificates: HashMap::new(),
            account_aliases: Vec::new(),
            account_password_policy: None,
            virtual_mfa_devices: HashMap::new(),
            service_linked_role_deletions: HashMap::new(),
            credential_identities: HashMap::new(),
            credential_report_generated: false,
            ssh_public_keys: HashMap::new(),
            access_key_last_used: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        let account_id = self.account_id.clone();
        *self = Self::new(&account_id);
    }
}

pub type SharedIamState = std::sync::Arc<RwLock<IamState>>;
