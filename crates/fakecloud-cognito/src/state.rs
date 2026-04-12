use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedCognitoState = Arc<RwLock<CognitoState>>;

pub struct CognitoState {
    pub account_id: String,
    pub region: String,
    pub user_pools: HashMap<String, UserPool>,
    pub user_pool_clients: HashMap<String, UserPoolClient>,
    /// pool_id -> (username -> User)
    pub users: HashMap<String, HashMap<String, User>>,
    /// refresh_token -> RefreshTokenData
    pub refresh_tokens: HashMap<String, RefreshTokenData>,
    /// session_token -> SessionData
    pub sessions: HashMap<String, SessionData>,
    /// access_token -> AccessTokenData
    pub access_tokens: HashMap<String, AccessTokenData>,
    /// pool_id -> (group_name -> Group)
    pub groups: HashMap<String, HashMap<String, Group>>,
    /// pool_id -> (username -> [group_names])
    pub user_groups: HashMap<String, HashMap<String, Vec<String>>>,
    /// pool_id -> (provider_name -> IdentityProvider)
    pub identity_providers: HashMap<String, HashMap<String, IdentityProvider>>,
    /// pool_id -> (identifier -> ResourceServer)
    pub resource_servers: HashMap<String, HashMap<String, ResourceServer>>,
    /// domain -> UserPoolDomain
    pub domains: HashMap<String, UserPoolDomain>,
    /// resource_arn -> tags
    pub tags: HashMap<String, HashMap<String, String>>,
    /// pool_id -> (job_id -> UserImportJob)
    pub import_jobs: HashMap<String, HashMap<String, UserImportJob>>,
    /// Auth events for introspection
    pub auth_events: Vec<AuthEvent>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthEvent {
    pub event_type: String,
    pub username: String,
    pub user_pool_id: String,
    pub client_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub success: bool,
}

impl CognitoState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            user_pools: HashMap::new(),
            user_pool_clients: HashMap::new(),
            users: HashMap::new(),
            refresh_tokens: HashMap::new(),
            sessions: HashMap::new(),
            access_tokens: HashMap::new(),
            groups: HashMap::new(),
            user_groups: HashMap::new(),
            identity_providers: HashMap::new(),
            resource_servers: HashMap::new(),
            domains: HashMap::new(),
            tags: HashMap::new(),
            import_jobs: HashMap::new(),
            auth_events: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.user_pools.clear();
        self.user_pool_clients.clear();
        self.users.clear();
        self.refresh_tokens.clear();
        self.sessions.clear();
        self.access_tokens.clear();
        self.groups.clear();
        self.user_groups.clear();
        self.identity_providers.clear();
        self.resource_servers.clear();
        self.domains.clear();
        self.tags.clear();
        self.import_jobs.clear();
        self.auth_events.clear();
    }
}

pub struct RefreshTokenData {
    pub user_pool_id: String,
    pub username: String,
    pub client_id: String,
    pub issued_at: DateTime<Utc>,
}

pub struct AccessTokenData {
    pub user_pool_id: String,
    pub username: String,
    pub client_id: String,
    pub issued_at: DateTime<Utc>,
}

pub struct SessionData {
    pub user_pool_id: String,
    pub username: String,
    pub client_id: String,
    pub challenge_name: String,
    /// History of challenge results for CUSTOM_AUTH multi-round flows.
    pub challenge_results: Vec<ChallengeResult>,
    /// Metadata from the CreateAuthChallenge Lambda (passed back to client).
    pub challenge_metadata: Option<String>,
}

/// Tracks the result of a single challenge round in a CUSTOM_AUTH flow.
#[derive(Clone, Debug)]
pub struct ChallengeResult {
    pub challenge_name: String,
    pub challenge_result: bool,
    /// Optional metadata returned by the CreateAuthChallenge Lambda.
    pub challenge_metadata: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserPool {
    pub id: String,
    pub name: String,
    pub arn: String,
    pub status: String,
    pub creation_date: DateTime<Utc>,
    pub last_modified_date: DateTime<Utc>,
    pub policies: PoolPolicies,
    pub auto_verified_attributes: Vec<String>,
    pub username_attributes: Option<Vec<String>>,
    pub alias_attributes: Option<Vec<String>>,
    pub schema_attributes: Vec<SchemaAttribute>,
    pub lambda_config: Option<serde_json::Value>,
    pub mfa_configuration: String,
    pub email_configuration: Option<EmailConfiguration>,
    pub sms_configuration: Option<SmsConfiguration>,
    pub admin_create_user_config: Option<AdminCreateUserConfig>,
    pub user_pool_tags: HashMap<String, String>,
    pub account_recovery_setting: Option<AccountRecoverySetting>,
    pub deletion_protection: Option<String>,
    pub estimated_number_of_users: i64,
    pub software_token_mfa_configuration: Option<SoftwareTokenMfaConfiguration>,
    pub sms_mfa_configuration: Option<SmsMfaConfiguration>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SoftwareTokenMfaConfiguration {
    pub enabled: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SmsMfaConfiguration {
    pub enabled: bool,
    pub sms_configuration: Option<SmsConfiguration>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolPolicies {
    pub password_policy: PasswordPolicy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PasswordPolicy {
    pub minimum_length: i64,
    pub require_uppercase: bool,
    pub require_lowercase: bool,
    pub require_numbers: bool,
    pub require_symbols: bool,
    pub temporary_password_validity_days: i64,
}

impl Default for PasswordPolicy {
    fn default() -> Self {
        Self {
            minimum_length: 8,
            require_uppercase: true,
            require_lowercase: true,
            require_numbers: true,
            require_symbols: true,
            temporary_password_validity_days: 7,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SchemaAttribute {
    pub name: String,
    pub attribute_data_type: String,
    pub developer_only_attribute: bool,
    pub mutable: bool,
    pub required: bool,
    pub string_attribute_constraints: Option<StringAttributeConstraints>,
    pub number_attribute_constraints: Option<NumberAttributeConstraints>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StringAttributeConstraints {
    pub min_length: Option<String>,
    pub max_length: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NumberAttributeConstraints {
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EmailConfiguration {
    pub source_arn: Option<String>,
    pub reply_to_email_address: Option<String>,
    pub email_sending_account: Option<String>,
    pub from_email_address: Option<String>,
    pub configuration_set: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SmsConfiguration {
    pub sns_caller_arn: Option<String>,
    pub external_id: Option<String>,
    pub sns_region: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdminCreateUserConfig {
    pub allow_admin_create_user_only: Option<bool>,
    pub invite_message_template: Option<InviteMessageTemplate>,
    pub unused_account_validity_days: Option<i64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InviteMessageTemplate {
    pub email_message: Option<String>,
    pub email_subject: Option<String>,
    pub sms_message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountRecoverySetting {
    pub recovery_mechanisms: Vec<RecoveryOption>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecoveryOption {
    pub name: String,
    pub priority: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserPoolClient {
    pub client_id: String,
    pub client_name: String,
    pub user_pool_id: String,
    pub client_secret: Option<String>,
    pub explicit_auth_flows: Vec<String>,
    pub token_validity_units: Option<TokenValidityUnits>,
    pub access_token_validity: Option<i64>,
    pub id_token_validity: Option<i64>,
    pub refresh_token_validity: Option<i64>,
    pub callback_urls: Vec<String>,
    pub logout_urls: Vec<String>,
    pub supported_identity_providers: Vec<String>,
    pub allowed_o_auth_flows: Vec<String>,
    pub allowed_o_auth_scopes: Vec<String>,
    pub allowed_o_auth_flows_user_pool_client: bool,
    pub prevent_user_existence_errors: Option<String>,
    pub read_attributes: Vec<String>,
    pub write_attributes: Vec<String>,
    pub creation_date: DateTime<Utc>,
    pub last_modified_date: DateTime<Utc>,
    pub enable_token_revocation: bool,
    pub auth_session_validity: Option<i64>,
    /// Additional client secrets (beyond the primary client_secret)
    pub client_secrets: Vec<ClientSecretDescriptor>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientSecretDescriptor {
    pub client_secret_id: String,
    pub client_secret_value: String,
    pub client_secret_create_date: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenValidityUnits {
    pub access_token: Option<String>,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub sub: String,
    pub attributes: Vec<UserAttribute>,
    pub enabled: bool,
    pub user_status: String,
    pub user_create_date: DateTime<Utc>,
    pub user_last_modified_date: DateTime<Utc>,
    pub password: Option<String>,
    pub temporary_password: Option<String>,
    pub confirmation_code: Option<String>,
    /// attribute_name -> verification_code (for GetUserAttributeVerificationCode / VerifyUserAttribute)
    pub attribute_verification_codes: HashMap<String, String>,
    pub mfa_preferences: Option<MfaPreferences>,
    pub totp_secret: Option<String>,
    pub totp_verified: bool,
    pub devices: HashMap<String, Device>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MfaPreferences {
    pub sms_enabled: bool,
    pub sms_preferred: bool,
    pub software_token_enabled: bool,
    pub software_token_preferred: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserAttribute {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Group {
    pub group_name: String,
    pub user_pool_id: String,
    pub description: Option<String>,
    pub precedence: Option<i64>,
    pub role_arn: Option<String>,
    pub creation_date: DateTime<Utc>,
    pub last_modified_date: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentityProvider {
    pub user_pool_id: String,
    pub provider_name: String,
    pub provider_type: String,
    pub provider_details: HashMap<String, String>,
    pub attribute_mapping: HashMap<String, String>,
    pub idp_identifiers: Vec<String>,
    pub creation_date: DateTime<Utc>,
    pub last_modified_date: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceServer {
    pub user_pool_id: String,
    pub identifier: String,
    pub name: String,
    pub scopes: Vec<ResourceServerScope>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResourceServerScope {
    pub scope_name: String,
    pub scope_description: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserPoolDomain {
    pub user_pool_id: String,
    pub domain: String,
    pub status: String,
    pub custom_domain_config: Option<CustomDomainConfig>,
    pub creation_date: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CustomDomainConfig {
    pub certificate_arn: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Device {
    pub device_key: String,
    pub device_attributes: HashMap<String, String>,
    pub device_create_date: DateTime<Utc>,
    pub device_last_modified_date: DateTime<Utc>,
    pub device_last_authenticated_date: Option<DateTime<Utc>>,
    pub device_remembered_status: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UserImportJob {
    pub job_id: String,
    pub job_name: String,
    pub user_pool_id: String,
    pub cloud_watch_logs_role_arn: String,
    pub status: String,
    pub creation_date: DateTime<Utc>,
    pub start_date: Option<DateTime<Utc>>,
    pub completion_date: Option<DateTime<Utc>>,
    pub pre_signed_url: Option<String>,
}

/// Generate default schema attributes that AWS adds to every user pool.
pub fn default_schema_attributes() -> Vec<SchemaAttribute> {
    let string_attrs = vec![
        ("sub", false, false, true, Some("1"), Some("2048")),
        ("name", false, true, false, Some("0"), Some("2048")),
        ("given_name", false, true, false, Some("0"), Some("2048")),
        ("family_name", false, true, false, Some("0"), Some("2048")),
        ("middle_name", false, true, false, Some("0"), Some("2048")),
        ("nickname", false, true, false, Some("0"), Some("2048")),
        (
            "preferred_username",
            false,
            true,
            false,
            Some("0"),
            Some("2048"),
        ),
        ("profile", false, true, false, Some("0"), Some("2048")),
        ("picture", false, true, false, Some("0"), Some("2048")),
        ("website", false, true, false, Some("0"), Some("2048")),
        ("email", false, true, false, Some("0"), Some("2048")),
        ("gender", false, true, false, Some("0"), Some("2048")),
        ("birthdate", false, true, false, Some("10"), Some("10")),
        ("zoneinfo", false, true, false, Some("0"), Some("2048")),
        ("locale", false, true, false, Some("0"), Some("2048")),
        ("phone_number", false, true, false, Some("0"), Some("2048")),
        ("address", false, true, false, Some("0"), Some("2048")),
        ("updated_at", false, true, false, None, None),
    ];

    let mut attrs: Vec<SchemaAttribute> = string_attrs
        .into_iter()
        .map(
            |(name, developer_only, mutable, required, min_len, max_len)| {
                let constraints = if min_len.is_some() || max_len.is_some() {
                    Some(StringAttributeConstraints {
                        min_length: min_len.map(|s| s.to_string()),
                        max_length: max_len.map(|s| s.to_string()),
                    })
                } else {
                    None
                };

                let attribute_data_type = if name == "updated_at" {
                    "Number".to_string()
                } else {
                    "String".to_string()
                };

                let number_constraints = if name == "updated_at" {
                    Some(NumberAttributeConstraints {
                        min_value: Some("0".to_string()),
                        max_value: None,
                    })
                } else {
                    None
                };

                SchemaAttribute {
                    name: name.to_string(),
                    attribute_data_type,
                    developer_only_attribute: developer_only,
                    mutable,
                    required,
                    string_attribute_constraints: constraints,
                    number_attribute_constraints: number_constraints,
                }
            },
        )
        .collect();

    // email_verified and phone_number_verified are Boolean attributes
    for name in &["email_verified", "phone_number_verified"] {
        attrs.push(SchemaAttribute {
            name: name.to_string(),
            attribute_data_type: "Boolean".to_string(),
            developer_only_attribute: false,
            mutable: true,
            required: false,
            string_attribute_constraints: None,
            number_attribute_constraints: None,
        });
    }

    attrs
}
