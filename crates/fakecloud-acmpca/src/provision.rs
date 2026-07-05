//! Shared certificate-authority construction used by BOTH the awsJson API
//! handler and the CloudFormation provisioner, so CA defaults, the
//! `CREATING -> PENDING_CERTIFICATE` lifecycle, and key/CSR generation have a
//! single source of truth (no per-creation-path drift).

use chrono::Utc;
use serde_json::{json, Value};

use crate::state::{CertificateAuthority, TagEntry};
use crate::validate;

/// Normalized parameters for creating a CA. The two creation paths (API +
/// CloudFormation) both populate this and hand it to [`build_creating_ca`].
pub struct CaCreateParams {
    pub arn: String,
    pub account: String,
    pub ca_type: String,
    pub key_algorithm: String,
    pub signing_algorithm: String,
    pub subject: Value,
    pub configuration: Value,
    pub usage_mode: String,
    pub key_storage_security_standard: Option<String>,
    pub revocation_configuration: Option<Value>,
    pub idempotency_token: Option<String>,
    pub tags: Vec<TagEntry>,
}

/// The AWS default `RevocationConfiguration` (CRL + OCSP both disabled) that
/// `DescribeCertificateAuthority` reports when the caller omits one.
pub fn default_revocation_configuration() -> Value {
    json!({
        "CrlConfiguration": { "Enabled": false },
        "OcspConfiguration": { "Enabled": false }
    })
}

/// The AWS default `KeyStorageSecurityStandard` in commercial regions.
pub const DEFAULT_KEY_STORAGE_STANDARD: &str = "FIPS_140_2_LEVEL_3_OR_HIGHER";

/// Build the initial CA record. It starts in `CREATING` with no key material;
/// key + CSR are filled in by [`fill_keygen`] once the (potentially slow) key
/// generation completes, at which point the CA settles to
/// `PENDING_CERTIFICATE`. Applies AWS defaults (FIPS L3 key storage, both
/// revocation methods disabled) in one place for every creation path.
pub fn build_creating_ca(params: CaCreateParams) -> CertificateAuthority {
    let now = Utc::now();
    CertificateAuthority {
        arn: params.arn,
        owner_account: params.account,
        created_at: now,
        last_state_change_at: Some(now),
        ca_type: params.ca_type,
        serial: None,
        status: "CREATING".to_string(),
        not_before: None,
        not_after: None,
        failure_reason: None,
        key_algorithm: params.key_algorithm,
        signing_algorithm: params.signing_algorithm,
        configuration: params.configuration,
        revocation_configuration: Some(
            params
                .revocation_configuration
                .unwrap_or_else(default_revocation_configuration),
        ),
        usage_mode: params.usage_mode,
        key_storage_security_standard: Some(
            params
                .key_storage_security_standard
                .unwrap_or_else(|| DEFAULT_KEY_STORAGE_STANDARD.to_string()),
        ),
        restorable_until: None,
        idempotency_token: params.idempotency_token,
        tags: params.tags,
        ca_key_pem: String::new(),
        ca_cert_pem: None,
        ca_cert_chain_pem: None,
        csr_pem: String::new(),
        issued: Default::default(),
        revoked: Default::default(),
        permissions: Default::default(),
        audit_reports: Default::default(),
    }
}

/// The `Subject` from a CA's stored configuration.
pub fn subject_of(ca: &CertificateAuthority) -> Value {
    ca.configuration
        .get("Subject")
        .cloned()
        .unwrap_or_else(|| json!({}))
}

/// Generate the real CA key pair + CSR for the given algorithm/subject. This is
/// the CPU-heavy step (real RSA keygen); callers run it off the async runtime
/// (background task / `spawn_blocking`) so it never blocks request handling.
pub fn generate_ca_material(
    key_algorithm: &str,
    subject: &Value,
) -> Result<(String, String), String> {
    let key_pair = validate::generate_key_pair(key_algorithm)?;
    let csr = validate::generate_ca_csr(subject, &key_pair)?;
    Ok((key_pair.serialize_pem(), csr))
}

/// Install freshly generated key material into a `CREATING` CA and settle it to
/// `PENDING_CERTIFICATE`.
pub fn fill_keygen(ca: &mut CertificateAuthority, key_pem: String, csr_pem: String) {
    ca.ca_key_pem = key_pem;
    ca.csr_pem = csr_pem;
    ca.status = "PENDING_CERTIFICATE".to_string();
    ca.last_state_change_at = Some(Utc::now());
}
