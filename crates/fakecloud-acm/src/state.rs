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

pub const ACM_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
