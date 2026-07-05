//! Input validation + real X.509 crypto for ACM Private CA.

use chrono::{DateTime, TimeZone, Utc};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CertificateSigningRequestParams,
    DistinguishedName, DnType, IsCa, KeyPair, KeyUsagePurpose, SignatureAlgorithm,
    PKCS_ECDSA_P256_SHA256, PKCS_ECDSA_P384_SHA384, PKCS_RSA_SHA256, PKCS_RSA_SHA384,
    PKCS_RSA_SHA512,
};
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use serde_json::Value;
use time::OffsetDateTime;

/// CA key algorithms fakecloud can generate a genuine key pair for. AWS ACM PCA
/// itself only permits these as CA key algorithms (the `ring` crypto backend
/// additionally cannot produce P-521 / SM2 / ML-DSA keys), so any other
/// model-valid `KeyAlgorithm` enum value is rejected rather than silently
/// substituted with a different key type.
pub const SUPPORTED_CA_KEY_ALGORITHMS: &[&str] = &[
    "RSA_2048",
    "RSA_3072",
    "RSA_4096",
    "EC_prime256v1",
    "EC_secp384r1",
];

/// Generate a genuine key pair of exactly the requested `KeyAlgorithm`. No
/// silent substitution: RSA algorithms produce a real RSA key of the requested
/// size, EC algorithms produce a key on the requested curve. Unsupported
/// algorithms return `Err` (callers reject before reaching here).
pub fn generate_key_pair(key_algorithm: &str) -> Result<KeyPair, String> {
    match key_algorithm {
        "EC_prime256v1" => {
            KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).map_err(|e| e.to_string())
        }
        "EC_secp384r1" => KeyPair::generate_for(&PKCS_ECDSA_P384_SHA384).map_err(|e| e.to_string()),
        "RSA_2048" | "RSA_3072" | "RSA_4096" => generate_rsa_key(key_algorithm),
        other => Err(format!("unsupported CA key algorithm: {other}")),
    }
}

/// Generate a genuine RSA key of the requested size. This is slow in
/// unoptimized builds (RSA-4096 can take tens of seconds), which is why
/// `CreateCertificateAuthority` runs it on a background task and settles the CA
/// from `CREATING` to `PENDING_CERTIFICATE` when it completes — the same
/// pattern used for slow resource provisioning elsewhere. No debug-only
/// substitution: the key is always genuinely RSA.
fn generate_rsa_key(key_algorithm: &str) -> Result<KeyPair, String> {
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
/// RSA (the only families [`generate_key_pair`] emits). The signature hash for
/// RSA keys loaded this way defaults to SHA-256; use [`load_signing_key`] to
/// honour a specific requested signing algorithm.
pub fn load_key_pair(pem: &str) -> Result<KeyPair, String> {
    KeyPair::from_pem(pem).map_err(|e| e.to_string())
}

/// Load the CA key pair configured to sign with the hash implied by
/// `signing_algorithm`, honouring the caller's requested `SigningAlgorithm` so
/// the issued certificate's `signatureAlgorithm` matches what was asked for.
/// Validates that the requested algorithm's key family matches the CA key
/// family (RSA vs ECDSA).
pub fn load_signing_key(
    pem: &str,
    key_algorithm: &str,
    signing_algorithm: &str,
) -> Result<KeyPair, String> {
    let is_rsa_key = key_algorithm.starts_with("RSA_");
    let is_rsa_sig = signing_algorithm.ends_with("WITHRSA");
    let is_ecdsa_sig = signing_algorithm.ends_with("WITHECDSA");
    if is_rsa_key && !is_rsa_sig {
        return Err(format!(
            "SigningAlgorithm {signing_algorithm} is not compatible with an RSA certificate authority"
        ));
    }
    if !is_rsa_key && !is_ecdsa_sig {
        return Err(format!(
            "SigningAlgorithm {signing_algorithm} is not compatible with an EC certificate authority"
        ));
    }
    if is_rsa_key {
        let alg: &'static SignatureAlgorithm = match signing_algorithm {
            "SHA256WITHRSA" => &PKCS_RSA_SHA256,
            "SHA384WITHRSA" => &PKCS_RSA_SHA384,
            "SHA512WITHRSA" => &PKCS_RSA_SHA512,
            other => return Err(format!("unsupported RSA signing algorithm: {other}")),
        };
        KeyPair::from_pkcs8_pem_and_sign_algo(pem, alg).map_err(|e| e.to_string())
    } else {
        // ECDSA: the hash is bound to the curve (P-256 -> SHA-256,
        // P-384 -> SHA-384); load with the key's native algorithm.
        load_key_pair(pem)
    }
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

/// Produce a real PEM CSR for a certificate authority, signed by its own key.
pub fn generate_ca_csr(subject: &Value, key_pair: &KeyPair) -> Result<String, String> {
    // rcgen forbids `is_ca` / `serial_number` in a CSR (RFC 2986), so the CA
    // nature is asserted when the CSR is signed into a certificate, not here.
    let mut params = CertificateParams::default();
    params.distinguished_name = build_dn(subject);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let csr = params
        .serialize_request(key_pair)
        .map_err(|e| format!("CSR generation failed: {e}"))?;
    csr.pem().map_err(|e| e.to_string())
}

/// Build the issuer certificate object used to sign new certificates from a
/// stored, already-installed CA certificate PEM plus the CA key.
pub fn issuer_from_ca_cert(ca_cert_pem: &str, ca_key: &KeyPair) -> Result<Certificate, String> {
    let params = CertificateParams::from_ca_cert_pem(ca_cert_pem)
        .map_err(|e| format!("could not parse CA certificate: {e}"))?;
    params
        .self_signed(ca_key)
        .map_err(|e| format!("could not rebuild issuer: {e}"))
}

/// Build a self-issuer for a ROOT CA that has generated its key but not yet had
/// a certificate installed — used to self-sign the root's own CSR during the
/// `IssueCertificate(RootCACertificate)` activation step.
pub fn self_issuer(subject: &Value, ca_key: &KeyPair) -> Result<Certificate, String> {
    let mut params = CertificateParams::default();
    params.distinguished_name = build_dn(subject);
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    params
        .self_signed(ca_key)
        .map_err(|e| format!("could not build self-issuer: {e}"))
}

/// Sign an end-entity (or subordinate-CA) certificate from a client CSR using
/// the given issuer + CA key. Returns `(certificate_pem, serial_hex)`.
pub fn issue_certificate(
    issuer_cert: &Certificate,
    ca_key: &KeyPair,
    csr_pem: &str,
    not_before: DateTime<Utc>,
    not_after: DateTime<Utc>,
    is_ca: bool,
) -> Result<(String, String), String> {
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
        .signed_by(issuer_cert, ca_key)
        .map_err(|e| format!("certificate issuance failed: {e}"))?;
    Ok((cert.pem(), hex::encode(serial)))
}

/// Outcome of verifying an imported CA certificate against the CA key pair.
pub enum ImportCheck {
    Ok,
    Malformed(String),
    Mismatch,
}

/// Verify that an imported CA certificate parses as real X.509 and its public
/// key matches the CA's generated key pair (SubjectPublicKeyInfo equality).
pub fn verify_imported_cert(ca_cert_pem: &str, ca_key: &KeyPair) -> ImportCheck {
    let pem = match x509_parser::pem::parse_x509_pem(ca_cert_pem.as_bytes()) {
        Ok((_, p)) => p,
        Err(e) => return ImportCheck::Malformed(format!("not valid PEM: {e}")),
    };
    let cert = match pem.parse_x509() {
        Ok(c) => c,
        Err(e) => return ImportCheck::Malformed(format!("not a valid X.509 certificate: {e}")),
    };
    // Compare the full SubjectPublicKeyInfo DER of the imported certificate with
    // the CA key pair's public key.
    if cert.public_key().raw == ca_key.public_key_der().as_slice() {
        ImportCheck::Ok
    } else {
        ImportCheck::Mismatch
    }
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

/// Largest validity fakecloud will resolve, in days (~10,000 years). Keeps the
/// date arithmetic well inside `chrono`'s representable range so a hostile
/// `Validity.Value` produces a `ValidationException`, never an overflow panic.
const MAX_VALIDITY_DAYS: i64 = 3_650_000;

/// Resolve a `Validity` (Value + Type) into an absolute expiry timestamp using
/// checked arithmetic. Returns `Err` (surfaced as `ValidationException`) for a
/// non-positive value, an out-of-range/overflowing duration, or a malformed
/// `END_DATE`, rather than overflowing or panicking on hostile input.
pub fn resolve_validity(
    from: DateTime<Utc>,
    value: i64,
    ty: &str,
) -> Result<DateTime<Utc>, String> {
    match ty {
        "DAYS" | "MONTHS" | "YEARS" => {
            if value < 1 {
                return Err("Validity Value must be a positive integer".to_string());
            }
            let days = match ty {
                "DAYS" => value,
                "MONTHS" => value
                    .checked_mul(30)
                    .ok_or_else(|| "Validity Value is too large".to_string())?,
                "YEARS" => value
                    .checked_mul(365)
                    .ok_or_else(|| "Validity Value is too large".to_string())?,
                _ => unreachable!(),
            };
            if days > MAX_VALIDITY_DAYS {
                return Err("Validity Value is too large".to_string());
            }
            from.checked_add_signed(chrono::Duration::days(days))
                .ok_or_else(|| "Validity Value is out of range".to_string())
        }
        // ABSOLUTE: seconds since the Unix epoch.
        "ABSOLUTE" => DateTime::from_timestamp(value, 0)
            .ok_or_else(|| "Validity ABSOLUTE value is out of range".to_string()),
        // END_DATE: a UTC timestamp encoded as the integer YYYYMMDDHHMMSS.
        "END_DATE" => parse_end_date(value),
        other => Err(format!("Invalid Validity Type: {other}")),
    }
}

/// Parse an ACM PCA `END_DATE` validity, encoded as the integer
/// `YYYYMMDDHHMMSS`, into a UTC timestamp.
fn parse_end_date(value: i64) -> Result<DateTime<Utc>, String> {
    if value <= 0 {
        return Err("Validity END_DATE must be a positive YYYYMMDDHHMMSS value".to_string());
    }
    let sec = (value % 100) as u32;
    let min = ((value / 100) % 100) as u32;
    let hour = ((value / 10_000) % 100) as u32;
    let day = ((value / 1_000_000) % 100) as u32;
    let month = ((value / 100_000_000) % 100) as u32;
    let year = (value / 10_000_000_000) as i32;
    Utc.with_ymd_and_hms(year, month, day, hour, min, sec)
        .single()
        .ok_or_else(|| format!("Invalid END_DATE validity: {value}"))
}
