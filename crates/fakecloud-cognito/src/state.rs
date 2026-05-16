use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedCognitoState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<CognitoState>>>;

impl fakecloud_core::multi_account::AccountState for CognitoState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub const COGNITO_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Serialize, Deserialize)]
pub struct CognitoSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<CognitoState>>,
    #[serde(default)]
    pub state: Option<CognitoState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CognitoState {
    pub account_id: String,
    pub region: String,
    #[serde(default)]
    pub user_pools: BTreeMap<String, UserPool>,
    #[serde(default)]
    pub user_pool_clients: BTreeMap<String, UserPoolClient>,
    /// pool_id -> (username -> User)
    #[serde(default)]
    pub users: BTreeMap<String, BTreeMap<String, User>>,
    /// refresh_token -> RefreshTokenData
    #[serde(default)]
    pub refresh_tokens: BTreeMap<String, RefreshTokenData>,
    /// session_token -> SessionData
    #[serde(default)]
    pub sessions: BTreeMap<String, SessionData>,
    /// access_token -> AccessTokenData
    #[serde(default)]
    pub access_tokens: BTreeMap<String, AccessTokenData>,
    /// One-time authorization codes issued by `/oauth2/authorize` (Y4) and
    /// the admin mint endpoint, consumed exactly once by `/oauth2/token`'s
    /// `authorization_code` grant.
    #[serde(default)]
    pub authorization_codes: BTreeMap<String, AuthorizationCodeData>,
    /// pool_id -> (group_name -> Group)
    #[serde(default)]
    pub groups: BTreeMap<String, BTreeMap<String, Group>>,
    /// pool_id -> (username -> [group_names])
    #[serde(default)]
    pub user_groups: BTreeMap<String, BTreeMap<String, Vec<String>>>,
    /// pool_id -> (provider_name -> IdentityProvider)
    #[serde(default)]
    pub identity_providers: BTreeMap<String, BTreeMap<String, IdentityProvider>>,
    /// pool_id -> (identifier -> ResourceServer)
    #[serde(default)]
    pub resource_servers: BTreeMap<String, BTreeMap<String, ResourceServer>>,
    /// domain -> UserPoolDomain
    #[serde(default)]
    pub domains: BTreeMap<String, UserPoolDomain>,
    /// resource_arn -> tags
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// pool_id -> (job_id -> UserImportJob)
    #[serde(default)]
    pub import_jobs: BTreeMap<String, BTreeMap<String, UserImportJob>>,
    /// Auth events for introspection — not persisted across restarts.
    #[serde(default, skip)]
    pub auth_events: Vec<AuthEvent>,
    /// (pool_id, client_id|"") -> UICustomization JSON
    #[serde(default)]
    pub ui_customizations: BTreeMap<String, serde_json::Value>,
    /// pool_id -> LogDeliveryConfiguration JSON
    #[serde(default)]
    pub log_delivery_configs: BTreeMap<String, serde_json::Value>,
    /// (pool_id, client_id|"") -> RiskConfiguration JSON
    #[serde(default)]
    pub risk_configurations: BTreeMap<String, serde_json::Value>,
    /// branding_id -> ManagedLoginBranding JSON
    #[serde(default)]
    pub managed_login_brandings: BTreeMap<String, serde_json::Value>,
    /// terms_id -> Terms JSON
    #[serde(default)]
    pub terms: BTreeMap<String, serde_json::Value>,
    /// (pool_id:username) -> WebAuthn credentials
    #[serde(default)]
    pub webauthn_credentials: BTreeMap<String, Vec<WebAuthnCredential>>,
    /// identity_pool_id -> IdentityPool (Cognito Federated Identities)
    #[serde(default)]
    pub identity_pools: BTreeMap<String, IdentityPool>,
    /// identity_pool_id -> IdentityPoolRoleAttachment
    #[serde(default)]
    pub identity_pool_role_attachments: BTreeMap<String, IdentityPoolRoleAttachment>,
    /// identity_id -> FederatedIdentity. Identity IDs are minted by GetId
    /// and are namespaced `<region>:<uuid>`. They live alongside identity
    /// pools but are tracked in their own map so list operations don't
    /// have to walk every pool.
    #[serde(default)]
    pub federated_identities: BTreeMap<String, FederatedIdentity>,
    /// Set of compromised-password hashes (sha256 hex of plaintext).
    /// Populated via the `/_fakecloud/cognito/compromised-passwords`
    /// admin endpoint and consulted on `InitiateAuth`/`AdminInitiateAuth`
    /// when a pool has CompromisedCredentialsRiskConfiguration with
    /// `EventAction = BLOCK`.
    #[serde(default)]
    pub compromised_password_hashes: std::collections::BTreeSet<String>,
    /// `<identity_pool_id>:<identity_provider_name>` -> principal-tag
    /// attribute map. Set by `SetPrincipalTagAttributeMap`, read by
    /// `GetPrincipalTagAttributeMap`. Real Cognito Identity persists
    /// these per-(pool, provider) so federated callers can mint role
    /// session tags from the JWT they authenticated with.
    #[serde(default)]
    pub principal_tag_attribute_maps: BTreeMap<String, PrincipalTagAttributeMap>,
    /// PreTokenGeneration Lambda trigger invocation log.
    /// Captured every time `InitiateAuth` fires the
    /// `TokenGeneration_Authentication` trigger and the Lambda returns,
    /// regardless of whether the response actually contained an override
    /// block. Surfaced via `/_fakecloud/cognito/pretokengen/invocations`
    /// so tests can assert the claim mutation flow end-to-end without
    /// inspecting the JWT they just received.
    #[serde(default, skip)]
    pub pre_token_gen_invocations: Vec<PreTokenGenInvocation>,
}

/// One PreTokenGeneration Lambda trigger invocation captured for
/// introspection. `claims_added` / `claims_overridden` /
/// `group_overrides` are pre-parsed from the trigger response so test
/// callers don't have to walk the `claimsAndScopeOverrideDetails`
/// shape themselves.
#[derive(Debug, Clone, Serialize)]
pub struct PreTokenGenInvocation {
    pub pool_id: String,
    pub user_pool_arn: String,
    pub username: String,
    pub trigger_source: String,
    pub lambda_arn: String,
    pub request_payload: serde_json::Value,
    pub response_payload: Option<serde_json::Value>,
    /// Keys added or overridden across both id-token and access-token
    /// `claimsToAddOrOverride` blocks.
    pub claims_added: Vec<String>,
    /// Keys suppressed across both id-token and access-token
    /// `claimsToSuppress` blocks.
    pub claims_overridden: Vec<String>,
    /// Contents of `groupOverrideDetails.groupsToOverride` if present.
    pub group_overrides: Vec<String>,
    pub invoked_at: DateTime<Utc>,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct AuthEvent {
    pub event_id: String,
    pub event_type: String,
    pub username: String,
    pub user_pool_id: String,
    pub client_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub success: bool,
    pub feedback_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebAuthnCredential {
    pub credential_id: String,
    pub friendly_credential_name: Option<String>,
    pub relying_party_id: String,
    pub authenticator_attachment: Option<String>,
    pub authenticator_transport: Vec<String>,
    pub created_at: DateTime<Utc>,
    /// Parsed attestation introspection. `None` when the client did not
    /// send `attestationObject` (some flows ship only the credential id).
    #[serde(default)]
    pub attestation_info: Option<WebAuthnAttestationInfo>,
}

/// What fakecloud saw when it parsed the WebAuthn `attestationObject`.
/// Surfaced via `/_fakecloud/cognito/webauthn-credentials` so tests can
/// assert what we accepted — including the deliberate emulator gap that
/// `x5c` chains are taken structurally without anchoring to a real root.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebAuthnAttestationInfo {
    pub fmt: String,
    pub alg: i64,
    pub signature_len: usize,
    pub x5c_chain_len: usize,
    pub cose_public_key_present: bool,
    /// `true` when we ran real RS256/ES256 verification against the
    /// COSE public key; `false` when x5c was present (structurally
    /// accepted, no PKI anchoring).
    pub self_attest_verified: bool,
}

/// Linked external provider for a user
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkedProvider {
    pub provider_name: String,
    pub provider_attribute_name: Option<String>,
    pub provider_attribute_value: Option<String>,
}

impl CognitoState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            user_pools: BTreeMap::new(),
            user_pool_clients: BTreeMap::new(),
            users: BTreeMap::new(),
            refresh_tokens: BTreeMap::new(),
            sessions: BTreeMap::new(),
            access_tokens: BTreeMap::new(),
            authorization_codes: BTreeMap::new(),
            groups: BTreeMap::new(),
            user_groups: BTreeMap::new(),
            identity_providers: BTreeMap::new(),
            resource_servers: BTreeMap::new(),
            domains: BTreeMap::new(),
            tags: BTreeMap::new(),
            import_jobs: BTreeMap::new(),
            auth_events: Vec::new(),
            ui_customizations: BTreeMap::new(),
            log_delivery_configs: BTreeMap::new(),
            risk_configurations: BTreeMap::new(),
            managed_login_brandings: BTreeMap::new(),
            terms: BTreeMap::new(),
            webauthn_credentials: BTreeMap::new(),
            identity_pools: BTreeMap::new(),
            identity_pool_role_attachments: BTreeMap::new(),
            federated_identities: BTreeMap::new(),
            compromised_password_hashes: std::collections::BTreeSet::new(),
            principal_tag_attribute_maps: BTreeMap::new(),
            pre_token_gen_invocations: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.user_pools.clear();
        self.user_pool_clients.clear();
        self.users.clear();
        self.refresh_tokens.clear();
        self.sessions.clear();
        self.access_tokens.clear();
        self.authorization_codes.clear();
        self.groups.clear();
        self.user_groups.clear();
        self.identity_providers.clear();
        self.resource_servers.clear();
        self.domains.clear();
        self.tags.clear();
        self.import_jobs.clear();
        self.auth_events.clear();
        self.ui_customizations.clear();
        self.log_delivery_configs.clear();
        self.risk_configurations.clear();
        self.managed_login_brandings.clear();
        self.terms.clear();
        self.webauthn_credentials.clear();
        self.identity_pools.clear();
        self.identity_pool_role_attachments.clear();
        self.federated_identities.clear();
        self.principal_tag_attribute_maps.clear();
        self.pre_token_gen_invocations.clear();
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RefreshTokenData {
    pub user_pool_id: String,
    pub username: String,
    pub client_id: String,
    pub issued_at: DateTime<Utc>,
}

/// Single-use OAuth2 authorization code minted by `/oauth2/authorize`
/// (or the `_fakecloud` admin endpoint pre-Y4). Carries everything the
/// `/oauth2/token` `authorization_code` grant needs to bind the code
/// back to the original `(client_id, redirect_uri, scope, PKCE)` tuple
/// it was issued for.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthorizationCodeData {
    pub user_pool_id: String,
    pub client_id: String,
    pub username: String,
    pub redirect_uri: String,
    /// Space-separated scopes requested by the client at /authorize time.
    pub scopes: Vec<String>,
    /// Optional PKCE challenge — when present the token endpoint must
    /// verify the supplied `code_verifier` against this value per RFC
    /// 7636.
    pub code_challenge: Option<String>,
    /// `S256` (default) or `plain`. Stored as-supplied so token endpoint
    /// can replay the right transform.
    pub code_challenge_method: Option<String>,
    /// Optional OIDC `nonce` claim, propagated into the issued id_token
    /// per OIDC core §3.1.2.1.
    pub nonce: Option<String>,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccessTokenData {
    pub user_pool_id: String,
    pub username: String,
    pub client_id: String,
    pub issued_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
#[derive(Clone, Debug, Serialize, Deserialize)]
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
    pub user_pool_tags: BTreeMap<String, String>,
    pub account_recovery_setting: Option<AccountRecoverySetting>,
    pub deletion_protection: Option<String>,
    pub estimated_number_of_users: i64,
    pub software_token_mfa_configuration: Option<SoftwareTokenMfaConfiguration>,
    pub sms_mfa_configuration: Option<SmsMfaConfiguration>,
    pub user_pool_tier: String,
    pub verification_message_template: Option<VerificationMessageTemplate>,
    /// Per-pool RSA-2048 private key (PKCS#8 PEM). Real Cognito generates a
    /// keypair at pool creation and signs RS256 JWTs with it; the public
    /// half is published at the pool's `/.well-known/jwks.json` endpoint.
    #[serde(default)]
    pub signing_key_pem: Option<String>,
    /// Stable `kid` exposed in the JWT header and JWKS document.
    #[serde(default)]
    pub signing_kid: Option<String>,
    /// Legacy email-verification message template (echoed back on describe).
    #[serde(default)]
    pub email_verification_message: Option<String>,
    /// Legacy email-verification subject (echoed back on describe).
    #[serde(default)]
    pub email_verification_subject: Option<String>,
    /// Legacy SMS-verification message template.
    #[serde(default)]
    pub sms_verification_message: Option<String>,
    /// Legacy SMS-based MFA challenge message.
    #[serde(default)]
    pub sms_authentication_message: Option<String>,
    /// Device-tracking configuration (challenge-on-new-device / remember).
    #[serde(default)]
    pub device_configuration: Option<serde_json::Value>,
    /// Per-attribute verification-on-update settings.
    #[serde(default)]
    pub user_attribute_update_settings: Option<serde_json::Value>,
    /// Advanced security / threat protection add-on settings.
    #[serde(default)]
    pub user_pool_add_ons: Option<serde_json::Value>,
    /// Username case-sensitivity preference.
    #[serde(default)]
    pub username_configuration: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VerificationMessageTemplate {
    pub default_email_option: String,
    pub email_message: Option<String>,
    pub email_subject: Option<String>,
    pub email_message_by_link: Option<String>,
    pub email_subject_by_link: Option<String>,
    pub sms_message: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SignInPolicy {
    pub allowed_first_auth_factors: Vec<String>,
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
    pub sign_in_policy: SignInPolicy,
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
    /// "ENABLED" rotates refresh token on every refresh-token grant
    /// (old token invalidated). Default "DISABLED".
    #[serde(default)]
    pub refresh_token_rotation: Option<RefreshTokenRotationConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RefreshTokenRotationConfig {
    /// "ENABLED" or "DISABLED"
    pub feature: String,
    /// grace period in seconds during which old token still works
    #[serde(default)]
    pub retry_grace_period_seconds: Option<i64>,
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
    pub attribute_verification_codes: BTreeMap<String, String>,
    pub mfa_preferences: Option<MfaPreferences>,
    pub totp_secret: Option<String>,
    pub totp_verified: bool,
    pub devices: BTreeMap<String, Device>,
    pub linked_providers: Vec<LinkedProvider>,
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
    pub provider_details: BTreeMap<String, String>,
    pub attribute_mapping: BTreeMap<String, String>,
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

/// Cognito Federated Identities pool. Distinct from Cognito User Pools —
/// identity pools issue temporary AWS credentials by federating any of
/// several login providers (Cognito User Pool, Google, Facebook, SAML,
/// OIDC, developer-authenticated). The CFN type is `AWS::Cognito::IdentityPool`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentityPool {
    pub identity_pool_id: String,
    pub identity_pool_name: String,
    #[serde(default)]
    pub allow_unauthenticated_identities: bool,
    #[serde(default)]
    pub allow_classic_flow: bool,
    #[serde(default)]
    pub developer_provider_name: Option<String>,
    #[serde(default)]
    pub cognito_identity_providers: Vec<CognitoIdentityProvider>,
    #[serde(default)]
    pub open_id_connect_provider_arns: Vec<String>,
    #[serde(default)]
    pub saml_provider_arns: Vec<String>,
    #[serde(default)]
    pub supported_login_providers: BTreeMap<String, String>,
    /// Free-form CognitoStreams config blob.
    #[serde(default)]
    pub cognito_streams: Option<serde_json::Value>,
    /// Free-form PushSync config blob.
    #[serde(default)]
    pub push_sync: Option<serde_json::Value>,
    #[serde(default)]
    pub identity_pool_tags: BTreeMap<String, String>,
    pub creation_date: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CognitoIdentityProvider {
    pub provider_name: String,
    pub client_id: String,
    #[serde(default)]
    pub server_side_token_check: bool,
}

/// Maps an identity pool's authenticated/unauthenticated roles plus any
/// provider-specific role-mapping rules. CFN type `AWS::Cognito::IdentityPoolRoleAttachment`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IdentityPoolRoleAttachment {
    pub identity_pool_id: String,
    /// Attachment id is synthesised at create-time so CFN can produce
    /// a stable Ref of the form `<pool-id>:<attachment-id>`.
    pub attachment_id: String,
    /// `authenticated` / `unauthenticated` -> role ARN.
    #[serde(default)]
    pub roles: BTreeMap<String, String>,
    /// Provider key (e.g. `cognito-idp.us-east-1.amazonaws.com/<pool>:<client>`)
    /// -> RoleMapping rules JSON. Stored opaquely.
    #[serde(default)]
    pub role_mappings: BTreeMap<String, serde_json::Value>,
}

/// A single federated identity minted by the cognito-identity service's
/// `GetId` action. Each identity has a stable id `<region>:<uuid>` and
/// optionally a set of `Logins` provider mappings (the access tokens
/// that were exchanged for the identity).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FederatedIdentity {
    pub identity_id: String,
    pub identity_pool_id: String,
    /// `provider_name` -> `provider_user_id` for every login linked to
    /// this identity. Authenticated identities have at least one entry;
    /// unauthenticated identities have an empty map.
    #[serde(default)]
    pub logins: BTreeMap<String, String>,
    /// Set of developer-authenticated login keys (e.g.
    /// `login.mygame.com=user-123`). Tracked separately so
    /// `LookupDeveloperIdentity` and friends can scan without walking
    /// every login provider entry.
    #[serde(default)]
    pub developer_logins: BTreeMap<String, String>,
    pub creation_date: DateTime<Utc>,
    pub last_modified_date: DateTime<Utc>,
}

/// Principal-tag attribute map for an (identity_pool_id,
/// identity_provider_name) pair. Real Cognito Identity uses these to
/// translate IdP JWT claims into role session tags when
/// `GetCredentialsForIdentity` mints credentials.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PrincipalTagAttributeMap {
    pub identity_pool_id: String,
    pub identity_provider_name: String,
    pub use_defaults: bool,
    #[serde(default)]
    pub principal_tags: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Device {
    pub device_key: String,
    pub device_attributes: BTreeMap<String, String>,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_empty() {
        let state = CognitoState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.user_pools.is_empty());
        assert!(state.users.is_empty());
    }

    #[test]
    fn reset_clears_state() {
        let mut state = CognitoState::new("123456789012", "us-east-1");
        state.tags.insert("arn".to_string(), BTreeMap::new());
        state.reset();
        assert!(state.tags.is_empty());
    }

    #[test]
    fn default_schema_attributes_returns_standard() {
        let attrs = default_schema_attributes();
        assert!(attrs.iter().any(|a| a.name == "sub"));
        assert!(attrs.iter().any(|a| a.name == "email"));
    }
}
