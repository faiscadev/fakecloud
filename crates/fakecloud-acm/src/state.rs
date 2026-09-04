//! In-memory state for ACM certificates.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedAcmState = Arc<RwLock<AcmAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AcmAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl AcmAccounts {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Keyed by full certificate ARN.
    pub certificates: BTreeMap<String, StoredCertificate>,
    pub account_config: AccountConfig,
    /// ACME endpoints keyed by `AcmeEndpointArn`.
    #[serde(default)]
    pub acme_endpoints: BTreeMap<String, AcmeEndpoint>,
    /// External account bindings keyed by `AcmeExternalAccountBindingArn`.
    #[serde(default)]
    pub acme_bindings: BTreeMap<String, AcmeBinding>,
    /// Domain validations keyed by `AcmeDomainValidationArn`.
    #[serde(default)]
    pub acme_domain_validations: BTreeMap<String, AcmeDomainValidation>,
    /// ACME accounts keyed by `(endpoint arn, account url)`.
    #[serde(default)]
    pub acme_accounts: BTreeMap<String, AcmeAccount>,
}

/// An ACME endpoint: the directory a client talks to, plus the CA it issues
/// from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcmeEndpoint {
    pub arn: String,
    pub endpoint_url: String,
    /// `AcmeEndpointStatus` (CREATING | ACTIVE | DELETING | FAILED).
    pub status: String,
    pub authorization_behavior: String,
    pub contact: Option<String>,
    /// The `CertificateAuthority` union, stored as supplied.
    pub certificate_authority: serde_json::Value,
    pub certificate_tags: BTreeMap<String, String>,
    pub tags: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    /// Set by the caller's `IdempotencyToken`, so a repeat create returns the
    /// same endpoint rather than a second one.
    pub idempotency_token: Option<String>,
}

/// An external account binding: the HMAC credential an ACME client uses to
/// bind its account to this AWS account.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcmeBinding {
    pub arn: String,
    pub endpoint_arn: String,
    pub role_arn: String,
    /// The HMAC key id and secret returned by
    /// `GetAcmeExternalAccountBindingCredentials`.
    pub key_id: String,
    pub mac_key: String,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub last_used_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub tags: BTreeMap<String, String>,
    pub idempotency_token: Option<String>,
}

/// The three independent scope options of a DNS prevalidation, each
/// `ENABLED` or `DISABLED`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct DomainScope {
    #[serde(default)]
    pub exact_domain: Option<String>,
    #[serde(default)]
    pub subdomains: Option<String>,
    #[serde(default)]
    pub wildcards: Option<String>,
}

/// A pre-validated domain: the DNS record an ACME client can rely on instead
/// of answering a challenge per order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcmeDomainValidation {
    pub arn: String,
    pub endpoint_arn: String,
    pub domain_name: String,
    /// `PrevalidationType` — only DNS_PREVALIDATION exists today.
    pub prevalidation_type: String,
    /// `DomainScope`: which names the prevalidation covers. Modeled as a
    /// structure of three `ENABLED`/`DISABLED` options, not a single enum.
    #[serde(default)]
    pub domain_scope: Option<DomainScope>,
    pub hosted_zone_id: Option<String>,
    /// The CNAME an operator publishes to prove control.
    pub record_name: String,
    pub record_value: String,
    /// `AcmeDomainValidationStatus`.
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub tags: BTreeMap<String, String>,
    pub idempotency_token: Option<String>,
}

/// An ACME account registered against an endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcmeAccount {
    pub endpoint_arn: String,
    pub account_url: String,
    pub public_key_thumbprint: String,
    /// `AcmeAccountStatus` (VALID | DEACTIVATED | REVOKED).
    pub status: String,
    pub binding_arn: Option<String>,
    pub contacts: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountConfig {
    pub expiry_events_days_before_expiry: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCertificate {
    pub arn: String,
    pub domain_name: String,
    pub subject_alternative_names: Vec<String>,
    pub status: String,
    pub cert_type: String,
    /// Stored when present so we can round-trip it on `GetCertificate`.
    pub certificate_pem: Option<String>,
    pub certificate_chain_pem: Option<String>,
    /// Imported certs only — held in memory but never returned
    /// (matches real ACM, which never returns the private key).
    pub private_key_pem: Option<String>,
    pub idempotency_token: Option<String>,
    pub serial: String,
    pub subject: String,
    pub issuer: String,
    pub key_algorithm: String,
    pub signature_algorithm: String,
    pub created_at: DateTime<Utc>,
    pub issued_at: Option<DateTime<Utc>>,
    pub imported_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revocation_reason: Option<String>,
    /// Last reason recorded by the admin status mutator when the cert
    /// is flipped to `FAILED` / `VALIDATION_TIMED_OUT`. Surfaced in
    /// `DescribeCertificate` as `FailureReason` to match real ACM.
    #[serde(default)]
    pub failure_reason: Option<String>,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub validation_method: Option<String>,
    pub domain_validation: Vec<DomainValidation>,
    pub options: CertificateOptions,
    pub renewal_eligibility: String,
    pub managed_by: Option<String>,
    pub certificate_authority_arn: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub in_use_by: Vec<String>,
    /// Number of `DescribeCertificate` reads since the cert was issued.
    /// Legacy field kept for state-file compatibility; the read-count
    /// flip was removed in favour of the async auto-issue tick (see
    /// `AcmService::pending_validation_delay`).
    #[serde(default)]
    pub describe_read_count: u32,
    /// Snapshot of the last managed-renewal round. `None` until either
    /// the auto-issue tick fires (for DNS) or the admin `/approve`
    /// endpoint flips an EMAIL cert; refreshed on every successful
    /// `RenewCertificate`. Surfaced as `RenewalSummary` in
    /// `DescribeCertificate` for `AMAZON_ISSUED` certs.
    #[serde(default)]
    pub renewal_summary: Option<RenewalSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenewalSummary {
    /// One of `PENDING_AUTO_RENEWAL`, `PENDING_VALIDATION`, `SUCCESS`, `FAILED`.
    pub renewal_status: String,
    /// Per-domain validation snapshot at the moment the renewal summary
    /// was emitted. fakecloud copies the cert's current
    /// `domain_validation` into this field so callers see consistent
    /// data between top-level `DomainValidationOptions` and
    /// `RenewalSummary.DomainValidationOptions`.
    pub domain_validation: Vec<DomainValidation>,
    /// Optional renewal failure reason. Real ACM uses
    /// `RenewalStatusReason` (an enum: `NO_AVAILABLE_CONTACTS`,
    /// `ADDITIONAL_VERIFICATION_REQUIRED`, etc.); fakecloud just stores
    /// whatever string the admin endpoint or renew flow recorded.
    pub renewal_status_reason: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainValidation {
    pub domain_name: String,
    pub validation_status: String,
    pub validation_method: String,
    pub resource_record_name: Option<String>,
    pub resource_record_type: Option<String>,
    pub resource_record_value: Option<String>,
    /// HTTP validation redirect (`HttpRedirect.RedirectFrom`) — set for
    /// ValidationMethod=HTTP certificates.
    #[serde(default)]
    pub http_redirect_from: Option<String>,
    /// HTTP validation redirect target (`HttpRedirect.RedirectTo`).
    #[serde(default)]
    pub http_redirect_to: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CertificateOptions {
    pub certificate_transparency_logging_preference: String,
    pub export: String,
}

/// On-disk snapshot envelope for ACM state. Versioned so format changes fail
/// loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct AcmSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<AcmAccounts>,
}

/// Bumped to 2 when the ACME resources landed. An older binary reading a
/// snapshot that carries them would drop the unknown maps silently, so the
/// version guard has to reject the downgrade rather than lose state.
pub const ACM_SNAPSHOT_SCHEMA_VERSION: u32 = 2;
