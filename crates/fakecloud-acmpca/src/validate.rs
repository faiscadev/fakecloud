//! Input validation + real X.509 crypto for ACM Private CA.

use base64::Engine;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, CertificateSigningRequestParams,
    CustomExtension, DistinguishedName, DnType, ExtendedKeyUsagePurpose, Ia5String, IsCa, KeyPair,
    KeyUsagePurpose, SanType, SignatureAlgorithm, PKCS_ECDSA_P256_SHA256, PKCS_ECDSA_P384_SHA384,
    PKCS_RSA_SHA256, PKCS_RSA_SHA384, PKCS_RSA_SHA512,
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
///
/// `api_passthrough` carries the caller's `IssueCertificate` `ApiPassthrough`
/// (Subject override + Extensions). ACM PCA stamps those values into the signed
/// certificate, so — unlike a naive re-sign of the CSR — the SANs, subject,
/// key-usage, extended-key-usage and custom extensions the caller asked for
/// actually appear in the certificate returned by `GetCertificate`.
pub fn issue_certificate(
    issuer_cert: &Certificate,
    ca_key: &KeyPair,
    csr_pem: &str,
    not_before: DateTime<Utc>,
    not_after: DateTime<Utc>,
    is_ca: bool,
    api_passthrough: Option<&Value>,
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
    if let Some(passthrough) = api_passthrough {
        apply_api_passthrough(&mut csr.params, passthrough, is_ca)?;
    }
    let cert = csr
        .signed_by(issuer_cert, ca_key)
        .map_err(|e| format!("certificate issuance failed: {e}"))?;
    Ok((cert.pem(), hex::encode(serial)))
}

/// Stamp an `ApiPassthrough` (Subject + Extensions) onto the certificate params
/// before signing, so the issued certificate really carries what the caller
/// requested. A `Subject` overrides the CSR's distinguished name; `Extensions`
/// (SANs, KeyUsage, ExtendedKeyUsage, CustomExtensions) override the
/// corresponding CSR extensions.
///
/// `CertificatePolicies` is the one `Extensions` sub-field left unapplied: it
/// requires hand-encoding the RFC 5280 certificatePolicies extension (policy
/// OIDs plus qualifier `IA5String`/`UserNotice` structures), for which rcgen
/// 0.13 exposes no first-class API. Rather than silently accept-and-drop it,
/// an explicit `CertificatePolicies` is rejected below so the caller is never
/// misled into thinking a policy was embedded.
fn apply_api_passthrough(
    params: &mut CertificateParams,
    passthrough: &Value,
    is_ca: bool,
) -> Result<(), String> {
    if let Some(subject) = passthrough.get("Subject").filter(|v| v.is_object()) {
        params.distinguished_name = build_dn(subject);
    }
    let Some(ext) = passthrough.get("Extensions").filter(|v| v.is_object()) else {
        return Ok(());
    };

    if let Some(sans) = ext.get("SubjectAlternativeNames").and_then(Value::as_array) {
        let mut mapped = Vec::with_capacity(sans.len());
        for san in sans {
            mapped.push(general_name_to_san(san)?);
        }
        params.subject_alt_names = mapped;
    }

    // KeyUsage passthrough applies to end-entity certificates. For a CA
    // certificate the KeyCertSign/CrlSign usages set above are mandatory, so a
    // passthrough KeyUsage is not allowed to weaken them.
    if !is_ca {
        if let Some(ku) = ext.get("KeyUsage").filter(|v| v.is_object()) {
            params.key_usages = key_usage_to_purposes(ku);
        }
    }

    if let Some(ekus) = ext.get("ExtendedKeyUsage").and_then(Value::as_array) {
        let mut mapped = Vec::with_capacity(ekus.len());
        for eku in ekus {
            mapped.push(extended_key_usage_to_purpose(eku)?);
        }
        params.extended_key_usages = mapped;
    }

    if let Some(customs) = ext.get("CustomExtensions").and_then(Value::as_array) {
        for custom in customs {
            params.custom_extensions.push(custom_extension(custom)?);
        }
    }

    if ext
        .get("CertificatePolicies")
        .and_then(Value::as_array)
        .is_some_and(|p| !p.is_empty())
    {
        return Err("ApiPassthrough Extensions.CertificatePolicies is not supported".to_string());
    }
    Ok(())
}

/// Map an ACM PCA `GeneralName` to an rcgen `SanType`. The four IA5String-based
/// name forms (DNS, RFC822/email, URI, IP) cover the SANs clients actually put
/// in a certificate; the ASN.1-structured forms (`DirectoryName`, `OtherName`,
/// `EdiPartyName`, `RegisteredId`) are rejected rather than dropped.
fn general_name_to_san(name: &Value) -> Result<SanType, String> {
    let ia5 = |field: &str| -> Result<Ia5String, String> {
        let s = name.get(field).and_then(Value::as_str).unwrap_or_default();
        Ia5String::try_from(s.to_string())
            .map_err(|_| format!("invalid IA5String in GeneralName.{field}: {s}"))
    };
    if name.get("DnsName").is_some() {
        Ok(SanType::DnsName(ia5("DnsName")?))
    } else if name.get("Rfc822Name").is_some() {
        Ok(SanType::Rfc822Name(ia5("Rfc822Name")?))
    } else if name.get("UniformResourceIdentifier").is_some() {
        Ok(SanType::URI(ia5("UniformResourceIdentifier")?))
    } else if let Some(ip) = name.get("IpAddress").and_then(Value::as_str) {
        let addr = ip
            .parse::<std::net::IpAddr>()
            .map_err(|_| format!("invalid IpAddress in SubjectAlternativeNames: {ip}"))?;
        Ok(SanType::IpAddress(addr))
    } else {
        Err("unsupported SubjectAlternativeNames GeneralName type".to_string())
    }
}

/// Map an ACM PCA `KeyUsage` (a struct of booleans) to rcgen key-usage purposes.
fn key_usage_to_purposes(ku: &Value) -> Vec<KeyUsagePurpose> {
    let on = |field: &str| ku.get(field).and_then(Value::as_bool).unwrap_or(false);
    let mut out = Vec::new();
    if on("DigitalSignature") {
        out.push(KeyUsagePurpose::DigitalSignature);
    }
    if on("NonRepudiation") {
        out.push(KeyUsagePurpose::ContentCommitment);
    }
    if on("KeyEncipherment") {
        out.push(KeyUsagePurpose::KeyEncipherment);
    }
    if on("DataEncipherment") {
        out.push(KeyUsagePurpose::DataEncipherment);
    }
    if on("KeyAgreement") {
        out.push(KeyUsagePurpose::KeyAgreement);
    }
    if on("KeyCertSign") {
        out.push(KeyUsagePurpose::KeyCertSign);
    }
    if on("CRLSign") {
        out.push(KeyUsagePurpose::CrlSign);
    }
    if on("EncipherOnly") {
        out.push(KeyUsagePurpose::EncipherOnly);
    }
    if on("DecipherOnly") {
        out.push(KeyUsagePurpose::DecipherOnly);
    }
    out
}

/// Map an ACM PCA `ExtendedKeyUsage` entry to an rcgen extended-key-usage
/// purpose. Either a named `ExtendedKeyUsageType` or an arbitrary dotted
/// `ExtendedKeyUsageObjectIdentifier` is accepted.
fn extended_key_usage_to_purpose(eku: &Value) -> Result<ExtendedKeyUsagePurpose, String> {
    if let Some(ty) = eku.get("ExtendedKeyUsageType").and_then(Value::as_str) {
        return match ty {
            "SERVER_AUTH" => Ok(ExtendedKeyUsagePurpose::ServerAuth),
            "CLIENT_AUTH" => Ok(ExtendedKeyUsagePurpose::ClientAuth),
            "CODE_SIGNING" => Ok(ExtendedKeyUsagePurpose::CodeSigning),
            "EMAIL_PROTECTION" => Ok(ExtendedKeyUsagePurpose::EmailProtection),
            "TIME_STAMPING" => Ok(ExtendedKeyUsagePurpose::TimeStamping),
            "OCSP_SIGNING" => Ok(ExtendedKeyUsagePurpose::OcspSigning),
            // Named ACM PCA purposes without a first-class rcgen variant map to
            // their well-known OIDs (RFC 5280 / Microsoft) via `Other`.
            "SMART_CARD_LOGIN" => Ok(ExtendedKeyUsagePurpose::Other(vec![
                1, 3, 6, 1, 4, 1, 311, 20, 2, 2,
            ])),
            "DOCUMENT_SIGNING" => Ok(ExtendedKeyUsagePurpose::Other(vec![
                1, 3, 6, 1, 4, 1, 311, 10, 3, 12,
            ])),
            "CERTIFICATE_TRANSPARENCY" => Ok(ExtendedKeyUsagePurpose::Other(vec![
                1, 3, 6, 1, 4, 1, 11129, 2, 4, 4,
            ])),
            other => Err(format!("Invalid ExtendedKeyUsageType: {other}")),
        };
    }
    if let Some(oid) = eku
        .get("ExtendedKeyUsageObjectIdentifier")
        .and_then(Value::as_str)
    {
        return Ok(ExtendedKeyUsagePurpose::Other(parse_oid(oid)?));
    }
    Err(
        "ExtendedKeyUsage requires ExtendedKeyUsageType or ExtendedKeyUsageObjectIdentifier"
            .to_string(),
    )
}

/// Build an rcgen `CustomExtension` from an ACM PCA `CustomExtension`
/// (dotted OID + base64 DER value + optional Critical flag).
fn custom_extension(custom: &Value) -> Result<CustomExtension, String> {
    let oid = custom
        .get("ObjectIdentifier")
        .and_then(Value::as_str)
        .ok_or_else(|| "CustomExtension.ObjectIdentifier is required".to_string())?;
    let value_b64 = custom
        .get("Value")
        .and_then(Value::as_str)
        .ok_or_else(|| "CustomExtension.Value is required".to_string())?;
    let content = base64::engine::general_purpose::STANDARD
        .decode(value_b64)
        .map_err(|_| "CustomExtension.Value must be base64".to_string())?;
    let mut ext = CustomExtension::from_oid_content(&parse_oid(oid)?, content);
    if custom
        .get("Critical")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        ext.set_criticality(true);
    }
    Ok(ext)
}

/// Parse a dotted OID string (`1.3.6.1.5.5.7.3.1`) into its arc components.
fn parse_oid(oid: &str) -> Result<Vec<u64>, String> {
    oid.split('.')
        .map(|arc| arc.parse::<u64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| format!("invalid object identifier: {oid}"))
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

/// The validity window + serial extracted from an imported CA certificate.
pub struct ImportedCertMeta {
    pub not_before: DateTime<Utc>,
    pub not_after: DateTime<Utc>,
    /// Lowercase hex, matching the serial format the issue path emits.
    pub serial_hex: String,
}

/// Read the real `NotBefore` / `NotAfter` / `Serial` from an imported CA
/// certificate so they can be stored instead of fabricated. Returns `None` only
/// if the PEM/X.509 fails to parse (already rejected by `verify_imported_cert`).
pub fn imported_cert_metadata(ca_cert_pem: &str) -> Option<ImportedCertMeta> {
    let (_, pem) = x509_parser::pem::parse_x509_pem(ca_cert_pem.as_bytes()).ok()?;
    let cert = pem.parse_x509().ok()?;
    let not_before = DateTime::from_timestamp(cert.validity().not_before.timestamp(), 0)?;
    let not_after = DateTime::from_timestamp(cert.validity().not_after.timestamp(), 0)?;
    Some(ImportedCertMeta {
        not_before,
        not_after,
        serial_hex: hex::encode(cert.raw_serial()),
    })
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

/// Largest validity fakecloud will resolve, in days. This keeps the DAYS/MONTHS/
/// YEARS arithmetic inside a range whose resulting `not_after` an X.509
/// certificate can actually encode (see [`ensure_representable`]). ~7,900 years
/// is comfortably beyond any real certificate lifetime yet stays under the
/// year-9999 ceiling the ASN.1 GeneralizedTime encoder enforces.
const MAX_VALIDITY_DAYS: i64 = 2_900_000;

/// Highest year an X.509 `not_after`/`not_before` can encode. ASN.1
/// GeneralizedTime carries a 4-digit year, so the encoder `rcgen`/`yasna` uses
/// *panics* on year >= 10000. The certificate is signed with this exact time
/// value, so this ceiling is a hard limit, not a soft one.
const MAX_CERT_YEAR: i32 = 9999;

/// Reject a resolved validity whose `not_after` an X.509 certificate cannot
/// encode. ASN.1 GeneralizedTime only expresses a 4-digit year; the `yasna`
/// encoder `rcgen` uses panics outright on year >= 10000. This check is
/// deliberately made against the calendar year via `chrono` (not the `time`
/// crate's configured date range, which Cargo feature unification can widen to
/// include years >= 10000), so an over-large validity is surfaced to the caller
/// as a `ValidationException` before any certificate is signed. Without it a
/// hostile `Validity` either crashes issuance or is silently clamped to the Unix
/// epoch, producing a certificate whose window runs backwards.
fn ensure_representable(dt: DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    if dt.year() > MAX_CERT_YEAR {
        return Err(
            "The requested validity exceeds the maximum certificate expiry that can be represented"
                .to_string(),
        );
    }
    Ok(dt)
}

/// Resolve a `Validity` (Value + Type) into an absolute expiry timestamp using
/// checked arithmetic. Returns `Err` (surfaced as `ValidationException`) for a
/// non-positive value, an out-of-range/overflowing duration, a value past the
/// representable certificate-expiry ceiling, or a malformed `END_DATE`, rather
/// than overflowing, panicking, or silently producing a backwards certificate.
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
            let end = from
                .checked_add_signed(chrono::Duration::days(days))
                .ok_or_else(|| "Validity Value is out of range".to_string())?;
            ensure_representable(end)
        }
        // ABSOLUTE: seconds since the Unix epoch.
        "ABSOLUTE" => DateTime::from_timestamp(value, 0)
            .ok_or_else(|| "Validity ABSOLUTE value is out of range".to_string())
            .and_then(ensure_representable),
        // END_DATE: a UTC timestamp encoded as the integer YYYYMMDDHHMMSS.
        "END_DATE" => parse_end_date(value).and_then(ensure_representable),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A resolved `not_after` at year 10000 or beyond is rejected — the ASN.1
    /// GeneralizedTime encoder can only carry a 4-digit year and panics on
    /// year >= 10000. This must hold regardless of the `time` crate's configured
    /// date range (feature unification can enable `large-dates`), so the check is
    /// exercised here independently of that crate.
    #[test]
    fn resolve_validity_rejects_year_10000_and_beyond() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        // END_DATE 10000-01-01 00:00:00.
        assert!(resolve_validity(now, 100_000_101_000_000, "END_DATE").is_err());
        // ABSOLUTE seconds that land past year 9999.
        let far = Utc
            .with_ymd_and_hms(9999, 1, 1, 0, 0, 0)
            .unwrap()
            .timestamp();
        assert!(resolve_validity(now, far, "ABSOLUTE").is_ok());
        let too_far = Utc
            .with_ymd_and_hms(9999, 12, 31, 23, 59, 59)
            .unwrap()
            .timestamp();
        assert!(resolve_validity(now, too_far, "ABSOLUTE").is_ok());
        // One second into year 10000 is rejected.
        assert!(resolve_validity(now, too_far + 1, "ABSOLUTE").is_err());
        // A huge relative validity is rejected before overflow, too.
        assert!(resolve_validity(now, 1_000_000, "YEARS").is_err());
    }

    /// A representable validity still resolves to a forward-ordered window.
    #[test]
    fn resolve_validity_accepts_normal_windows() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let end = resolve_validity(now, 365, "DAYS").unwrap();
        assert!(end > now);
        assert!(end.year() <= MAX_CERT_YEAR);
    }

    /// The imported certificate's real validity window + serial are recovered,
    /// so the CA can store them instead of fabricating now / now+10y / random.
    #[test]
    fn imported_cert_metadata_reads_real_window_and_serial() {
        let key = generate_key_pair("EC_prime256v1").unwrap();
        let mut params = CertificateParams::default();
        params.distinguished_name = build_dn(&serde_json::json!({ "CommonName": "Test Root CA" }));
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let nb = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let na = Utc.with_ymd_and_hms(2030, 6, 7, 8, 9, 10).unwrap();
        params.not_before = to_offset(nb);
        params.not_after = to_offset(na);
        // High bit clear so the DER INTEGER has no leading 0x00 pad byte.
        params.serial_number = Some(vec![0x01u8, 0x02, 0x03, 0x04].into());
        let cert = params.self_signed(&key).unwrap();
        let pem = cert.pem();

        let meta = imported_cert_metadata(&pem).expect("parses the just-built cert");
        assert_eq!(meta.not_before.timestamp(), nb.timestamp());
        assert_eq!(meta.not_after.timestamp(), na.timestamp());
        assert_eq!(meta.serial_hex, "01020304");
    }
}
