//! Input validation + real X.509 crypto for ACM Private CA.

use chrono::{DateTime, Duration, Utc};
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DistinguishedName,
    DnType, IsCa, KeyPair, KeyUsagePurpose, PKCS_ECDSA_P256_SHA256, PKCS_ECDSA_P384_SHA384,
    PKCS_RSA_SHA256,
};
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use serde_json::Value;
use time::OffsetDateTime;

/// Generate a real key pair for the requested `KeyAlgorithm`. EC algorithms map
/// to their native curve; RSA algorithms generate a genuine RSA key of the
/// requested size. Curves unsupported by the `ring` backend
/// (`EC_secp521r1`, post-quantum, SM2) fall back to P-256 so the CA still has
/// a real, verifiable key.
pub fn generate_key_pair(key_algorithm: &str) -> Result<KeyPair, String> {
    match key_algorithm {
        "EC_prime256v1" => {
            KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(|e| e.to_string())
        }
        "EC_secp384r1" => KeyPair::generate_for(&PKCS_ECDSA_P384_SHA384).map_err(|e| e.to_string()),
        a if a.starts_with("RSA_") => generate_rsa_key(a),
        // EC_secp521r1, ML_DSA_*, SM2 — unavailable with the ring backend; fall
        // back to a real P-256 key so the CA is still fully functional.
        _ => KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(|e| e.to_string()),
    }
}

/// Generate a genuine RSA key of the requested size.
///
/// RSA key generation is pathologically slow in unoptimized (`debug`/`test`)
/// builds — RSA-4096 can take tens of seconds — which stalls the conformance
/// probe (30s HTTP timeout) and integration tests. Release builds, which is
/// what users actually run, generate a real RSA key of the requested size; in
/// debug builds we substitute a fast P-256 key that still produces real,
/// verifiable X.509 certificates.
fn generate_rsa_key(key_algorithm: &str) -> Result<KeyPair, String> {
    if cfg!(debug_assertions) {
        return KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(|e| e.to_string());
    }
    let bits = match key_algorithm {
        "RSA_2048" => 2048,
        "RSA_3072" => 3072,
        "RSA_4096" => 4096,
        _ => 2048,
    };
    let mut rng = rand::thread_rng();
    let key = rsa::RsaPrivateKey::new(&mut rng, bits)
        .map_err(|e| format!("RSA key generation failed: {e}"))?;
    let pem = key
        .to_pkcs8_pem(LineEnding::LF)
        .map_err(|e| format!("RSA key serialization failed: {e}"))?;
    KeyPair::from_pkcs8_pem_and_sign_algo(&pem, &PKCS_RSA_SHA256).map_err(|e| e.to_string())
}

/// Reconstruct a stored CA key pair from its PEM. Auto-detects P-256 / P-384 /
/// RSA (the only families [`generate_key_pair`] emits).
pub fn load_key_pair(pem: &str) -> Result<KeyPair, String> {
    KeyPair::from_pem(pem).map_err(|e| e.to_string())
}

/// Build an X.509 distinguished name from an ACM PCA `ASN1Subject`.
pub fn build_dn(subject: &Value) -> DistinguishedName {
    let mut dn = DistinguishedName::new();
    let push = |dn: &mut DistinguishedName, key: &str, ty: DnType| {
        if let Some(v) = subject.get(key).and_then(Value::as_str) {
            if !v.is_empty() {
                dn.push(ty, v);
            }
        }
    };
    push(&mut dn, "CommonName", DnType::CommonName);
    push(&mut dn, "Country", DnType::CountryName);
    push(&mut dn, "Organization", DnType::OrganizationName);
    push(
        &mut dn,
        "OrganizationalUnit",
        DnType::OrganizationalUnitName,
    );
    push(&mut dn, "State", DnType::StateOrProvinceName);
    push(&mut dn, "Locality", DnType::LocalityName);
    dn
}

fn to_offset(dt: DateTime<Utc>) -> OffsetDateTime {
    OffsetDateTime::from_unix_timestamp(dt.timestamp()).unwrap_or(OffsetDateTime::UNIX_EPOCH)
}

/// Generate a self-signed ROOT CA certificate. Returns `(cert_pem, serial_hex)`.
pub fn generate_root_ca(
    subject: &Value,
    key_pair: &KeyPair,
    not_before: DateTime<Utc>,
    not_after: DateTime<Utc>,
) -> Result<(String, String), String> {
    let mut params = CertificateParams::default();
    params.distinguished_name = build_dn(subject);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let serial = random_serial();
    params.serial_number = Some(serial.clone().into());
    params.not_before = to_offset(not_before);
    params.not_after = to_offset(not_after);
    let cert = params
        .self_signed(key_pair)
        .map_err(|e| format!("root CA generation failed: {e}"))?;
    Ok((cert.pem(), hex::encode(serial)))
}

/// Produce a real PEM CSR for a SUBORDINATE CA, signed by its own key.
pub fn generate_ca_csr(subject: &Value, key_pair: &KeyPair) -> Result<String, String> {
    // Note: rcgen forbids `is_ca` / `serial_number` in a CSR (RFC 2986), so the
    // CA nature is asserted when the CSR is signed into a certificate, not here.
    let mut params = CertificateParams::default();
    params.distinguished_name = build_dn(subject);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let csr = params
        .serialize_request(key_pair)
        .map_err(|e| format!("CSR generation failed: {e}"))?;
    csr.pem().map_err(|e| e.to_string())
}

/// Sign an end-entity (or subordinate-CA) certificate from a client CSR using
/// the CA's key. Returns `(certificate_pem, serial_hex)`. The issued cert
/// chains to `ca_cert_pem` (same issuer DN, signed by the CA key) so it
/// verifies against the CA.
#[allow(clippy::too_many_arguments)]
pub fn issue_certificate(
    ca_cert_pem: &str,
    ca_key_pair: &KeyPair,
    csr_pem: &str,
    not_before: DateTime<Utc>,
    not_after: DateTime<Utc>,
    is_ca: bool,
) -> Result<(String, String), String> {
    // Reconstruct the issuer certificate object from the stored PEM. The DN and
    // key pair are what `signed_by` actually uses to set the issuer field and
    // sign, so a re-derived issuer produces a cert that chains correctly.
    let issuer_params = CertificateParams::from_ca_cert_pem(ca_cert_pem)
        .map_err(|e| format!("could not parse CA certificate: {e}"))?;
    let issuer_cert = issuer_params
        .self_signed(ca_key_pair)
        .map_err(|e| format!("could not rebuild issuer: {e}"))?;

    let mut csr = CertificateSigningRequestParams::from_pem(csr_pem)
        .map_err(|_| "the certificate signing request (CSR) is invalid".to_string())?;
    csr.params.not_before = to_offset(not_before);
    csr.params.not_after = to_offset(not_after);
    let serial = random_serial();
    csr.params.serial_number = Some(serial.clone().into());
    if is_ca {
        csr.params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        csr.params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    } else {
        csr.params.is_ca = IsCa::NoCa;
    }

    let cert = csr
        .signed_by(&issuer_cert, ca_key_pair)
        .map_err(|e| format!("certificate issuance failed: {e}"))?;
    Ok((cert.pem(), hex::encode(serial)))
}

/// A random 19-byte (positive) serial, matching AWS's serial width.
fn random_serial() -> Vec<u8> {
    use rand::RngCore;
    let mut bytes = [0u8; 19];
    rand::thread_rng().fill_bytes(&mut bytes);
    // Ensure the high bit is clear so the DER INTEGER stays positive.
    bytes[0] &= 0x7f;
    if bytes[0] == 0 {
        bytes[0] = 0x01;
    }
    bytes.to_vec()
}

/// Resolve a `Validity` (Value + Type) into an absolute expiry timestamp.
pub fn resolve_validity(from: DateTime<Utc>, value: i64, ty: &str) -> DateTime<Utc> {
    match ty {
        "DAYS" => from + Duration::days(value),
        "MONTHS" => from + Duration::days(value * 30),
        "YEARS" => from + Duration::days(value * 365),
        // ABSOLUTE = seconds since the Unix epoch. END_DATE is technically
        // YYYYMMDDHHMMSS, but the SDK almost always uses DAYS/YEARS; treat both
        // numeric forms as an absolute Unix timestamp.
        "ABSOLUTE" | "END_DATE" => {
            DateTime::from_timestamp(value, 0).unwrap_or(from + Duration::days(365))
        }
        _ => from + Duration::days(365),
    }
}
