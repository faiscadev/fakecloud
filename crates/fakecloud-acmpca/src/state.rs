//! In-memory state for ACM Private CA (`acm-pca`).
//!
//! State is partitioned per account. Each account owns a set of private
//! certificate authorities keyed by full CA ARN, plus resource-based
//! policies keyed by resource ARN. All maps use `String` keys so serde
//! round-trips cleanly (no tuple-key `KeyMustBeAString` trap).

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedAcmPcaState = Arc<RwLock<AcmPcaAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AcmPcaAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl AcmPcaAccounts {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Keyed by full certificate-authority ARN.
    pub authorities: BTreeMap<String, CertificateAuthority>,
    /// Resource-based policies keyed by resource (CA) ARN.
    pub policies: BTreeMap<String, String>,
}

/// A private certificate authority plus all the crypto material needed to keep
/// issuing certificates that verify after a restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateAuthority {
    pub arn: String,
    pub owner_account: String,
    pub created_at: DateTime<Utc>,
    pub last_state_change_at: Option<DateTime<Utc>>,
    /// `ROOT` or `SUBORDINATE`.
    pub ca_type: String,
    /// Hex serial of the CA certificate (populated once the CA cert exists).
    pub serial: Option<String>,
    /// `CREATING`, `PENDING_CERTIFICATE`, `ACTIVE`, `DISABLED`, `EXPIRED`,
    /// `DELETED`, `FAILED`.
    pub status: String,
    pub not_before: Option<DateTime<Utc>>,
    pub not_after: Option<DateTime<Utc>>,
    pub failure_reason: Option<String>,
    pub key_algorithm: String,
    pub signing_algorithm: String,
    /// The `CertificateAuthorityConfiguration` echoed back verbatim.
    pub configuration: serde_json::Value,
    /// The `RevocationConfiguration` (CRL/OCSP), if any.
    pub revocation_configuration: Option<serde_json::Value>,
    /// `GENERAL_PURPOSE` or `SHORT_LIVED_CERTIFICATE`.
    pub usage_mode: String,
    pub key_storage_security_standard: Option<String>,
    /// Set to `created + 30d` while the CA is soft-deleted (restorable).
    pub restorable_until: Option<DateTime<Utc>>,
    pub idempotency_token: Option<String>,
    /// Tags as an ordered list of key/value pairs (ACM PCA preserves order and
    /// permits duplicate-free keys).
    pub tags: Vec<TagEntry>,

    // ── Crypto material (persisted so issued certs still verify post-restart) ──
    /// CA private key, PKCS#8 PEM.
    pub ca_key_pem: String,
    /// CA certificate PEM. Self-signed for a ROOT CA; the imported signed cert
    /// for a SUBORDINATE CA. `None` until a SUBORDINATE CA is activated.
    pub ca_cert_pem: Option<String>,
    /// The parent chain installed at `ImportCertificateAuthorityCertificate`.
    pub ca_cert_chain_pem: Option<String>,
    /// The CSR served by `GetCertificateAuthorityCsr`.
    pub csr_pem: String,

    /// Issued end-entity / subordinate certificates keyed by certificate ARN.
    pub issued: BTreeMap<String, IssuedCertificate>,
    /// Revoked certificates keyed by hex serial.
    pub revoked: BTreeMap<String, RevokedCertificate>,
    /// Resource-share permissions keyed by principal.
    pub permissions: BTreeMap<String, Permission>,
    /// Audit reports keyed by audit report id.
    pub audit_reports: BTreeMap<String, AuditReport>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagEntry {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IssuedCertificate {
    pub arn: String,
    pub serial: String,
    pub certificate_pem: String,
    pub chain_pem: Option<String>,
    pub issued_at: DateTime<Utc>,
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    pub template_arn: Option<String>,
    pub signing_algorithm: String,
    pub idempotency_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevokedCertificate {
    pub serial: String,
    pub revoked_at: DateTime<Utc>,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Permission {
    pub certificate_authority_arn: String,
    pub created_at: DateTime<Utc>,
    pub principal: String,
    pub source_account: Option<String>,
    pub actions: Vec<String>,
    pub policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditReport {
    pub id: String,
    pub certificate_authority_arn: String,
    pub s3_bucket_name: String,
    pub s3_key: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub response_format: String,
    /// The real report body (JSON or CSV) describing issued/revoked certs.
    pub body: String,
}

/// On-disk snapshot envelope. Versioned so format changes fail loudly on
/// upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct AcmPcaSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<AcmPcaAccounts>,
}

pub const ACM_PCA_SNAPSHOT_SCHEMA_VERSION: u32 = 1;
