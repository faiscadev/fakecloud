//! WebAuthn attestation parsing and verification.
//!
//! Implements only the "packed" attestation format (self-attestation form).
//! Other formats (fido-u2f, android-key, android-safetynet, tpm, apple, none)
//! are rejected with [`AttestationError::UnsupportedFormat`] so callers can
//! surface `UnsupportedAttestationFormat` to clients.
//!
//! The packed format is the one issued by most platform authenticators
//! (Windows Hello, Android, modern YubiKeys configured for self-attest) and
//! is the only format the WebAuthn spec requires implementers to support.

use base64::Engine;
use ciborium::Value as CborValue;
use rsa::sha2::{Digest, Sha256};

/// Errors surfaced when parsing/verifying a WebAuthn attestation object.
#[derive(Debug)]
pub enum AttestationError {
    InvalidBase64,
    InvalidCbor,
    InvalidStructure(&'static str),
    UnsupportedFormat(String),
    SignatureInvalid,
}

impl std::fmt::Display for AttestationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidBase64 => write!(f, "attestationObject is not valid base64url"),
            Self::InvalidCbor => write!(f, "attestationObject is not valid CBOR"),
            Self::InvalidStructure(s) => write!(f, "attestationObject malformed: {s}"),
            Self::UnsupportedFormat(fmt) => {
                write!(f, "UnsupportedAttestationFormat: {fmt}")
            }
            Self::SignatureInvalid => write!(f, "attestation signature did not verify"),
        }
    }
}

/// Parsed `packed` attestation object pieces the caller needs.
#[derive(Debug)]
pub struct PackedAttestation {
    pub auth_data: Vec<u8>,
    pub alg: i64,
    pub sig: Vec<u8>,
    pub x5c: Vec<Vec<u8>>,
    pub cose_public_key: Option<Vec<u8>>,
}

fn b64url_decode(input: &str) -> Result<Vec<u8>, AttestationError> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(input.trim_end_matches('='))
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(input))
        .map_err(|_| AttestationError::InvalidBase64)
}

fn cbor_map_get<'a>(map: &'a [(CborValue, CborValue)], key: &str) -> Option<&'a CborValue> {
    map.iter().find_map(|(k, v)| match k {
        CborValue::Text(t) if t == key => Some(v),
        _ => None,
    })
}

/// Parse a base64url-encoded WebAuthn `attestationObject` and return the
/// "packed" attestation pieces. Rejects every other `fmt`.
pub(crate) fn parse_packed_attestation(
    attestation_object_b64: &str,
) -> Result<PackedAttestation, AttestationError> {
    let raw = b64url_decode(attestation_object_b64)?;
    let value: CborValue =
        ciborium::de::from_reader(raw.as_slice()).map_err(|_| AttestationError::InvalidCbor)?;

    let map = match value {
        CborValue::Map(m) => m,
        _ => return Err(AttestationError::InvalidStructure("top-level not a map")),
    };

    let fmt = cbor_map_get(&map, "fmt")
        .and_then(|v| match v {
            CborValue::Text(t) => Some(t.clone()),
            _ => None,
        })
        .ok_or(AttestationError::InvalidStructure("missing fmt"))?;

    if fmt != "packed" {
        return Err(AttestationError::UnsupportedFormat(fmt));
    }

    let auth_data = cbor_map_get(&map, "authData")
        .and_then(|v| match v {
            CborValue::Bytes(b) => Some(b.clone()),
            _ => None,
        })
        .ok_or(AttestationError::InvalidStructure("missing authData"))?;

    let att_stmt = match cbor_map_get(&map, "attStmt") {
        Some(CborValue::Map(m)) => m.clone(),
        _ => return Err(AttestationError::InvalidStructure("missing attStmt")),
    };

    let alg = match cbor_map_get(&att_stmt, "alg") {
        Some(CborValue::Integer(i)) => i128::from(*i) as i64,
        _ => return Err(AttestationError::InvalidStructure("missing attStmt.alg")),
    };

    let sig = match cbor_map_get(&att_stmt, "sig") {
        Some(CborValue::Bytes(b)) => b.clone(),
        _ => return Err(AttestationError::InvalidStructure("missing attStmt.sig")),
    };

    let x5c: Vec<Vec<u8>> = match cbor_map_get(&att_stmt, "x5c") {
        Some(CborValue::Array(arr)) => arr
            .iter()
            .filter_map(|v| match v {
                CborValue::Bytes(b) => Some(b.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };

    let cose_public_key = extract_cose_public_key(&auth_data);

    Ok(PackedAttestation {
        auth_data,
        alg,
        sig,
        x5c,
        cose_public_key,
    })
}

/// Pull the COSE public key bytes out of the authData attestedCredentialData.
///
/// authData layout (WebAuthn spec):
///   rpIdHash (32) | flags (1) | signCount (4) | attestedCredentialData? | ext?
/// attestedCredentialData layout:
///   aaguid (16) | credIdLen (2 BE) | credId (credIdLen) | credPublicKey (CBOR)
///
/// Returns `None` if the AT flag is unset or the slice is short.
fn extract_cose_public_key(auth_data: &[u8]) -> Option<Vec<u8>> {
    if auth_data.len() < 37 {
        return None;
    }
    let flags = auth_data[32];
    if flags & 0x40 == 0 {
        return None;
    }
    let after_header = &auth_data[37..];
    if after_header.len() < 18 {
        return None;
    }
    let cred_id_len = u16::from_be_bytes([after_header[16], after_header[17]]) as usize;
    let after_cred = after_header.get(18 + cred_id_len..)?;
    Some(after_cred.to_vec())
}

/// Verify a packed attestation signature.
///
/// `client_data_json` is the raw bytes the relying party received as the
/// `clientDataJSON` field of the credential response. The packed signature
/// is over `authData || SHA-256(clientDataJSON)`.
///
/// Only self-attestation (no x5c) with ES256 (alg=-7) or RS256 (alg=-257)
/// is verified end-to-end; for x5c-bearing attestations we accept the
/// signature as long as the cert chain is non-empty (real fakecloud usage
/// hits self-attest, and tying x5c to full PKI validation is out of scope
/// for the emulator). Returning [`AttestationError::SignatureInvalid`]
/// indicates self-attest verification failed.
pub fn verify_packed_attestation(
    att: &PackedAttestation,
    client_data_json: &[u8],
) -> Result<(), AttestationError> {
    if !att.x5c.is_empty() {
        return Ok(());
    }

    let cose = att
        .cose_public_key
        .as_ref()
        .ok_or(AttestationError::InvalidStructure(
            "self-attest requires credential public key",
        ))?;

    let mut hasher = Sha256::new();
    hasher.update(client_data_json);
    let cd_hash = hasher.finalize();
    let mut signed = att.auth_data.clone();
    signed.extend_from_slice(&cd_hash);

    match att.alg {
        -257 => verify_rs256(cose, &signed, &att.sig),
        -7 => verify_es256(cose, &signed, &att.sig),
        other => Err(AttestationError::UnsupportedFormat(format!("alg={other}"))),
    }
}

fn cose_key_map(cose: &[u8]) -> Result<Vec<(CborValue, CborValue)>, AttestationError> {
    let value: CborValue =
        ciborium::de::from_reader(cose).map_err(|_| AttestationError::InvalidCbor)?;
    match value {
        CborValue::Map(m) => Ok(m),
        _ => Err(AttestationError::InvalidStructure("COSE key not a map")),
    }
}

fn cose_key_int(map: &[(CborValue, CborValue)], key: i64) -> Option<&CborValue> {
    map.iter().find_map(|(k, v)| match k {
        CborValue::Integer(i) if i128::from(*i) as i64 == key => Some(v),
        _ => None,
    })
}

fn verify_rs256(cose: &[u8], signed: &[u8], sig: &[u8]) -> Result<(), AttestationError> {
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::signature::Verifier;
    use rsa::{BigUint, RsaPublicKey};

    let map = cose_key_map(cose)?;
    let n = match cose_key_int(&map, -1) {
        Some(CborValue::Bytes(b)) => b,
        _ => return Err(AttestationError::InvalidStructure("RSA n missing")),
    };
    let e = match cose_key_int(&map, -2) {
        Some(CborValue::Bytes(b)) => b,
        _ => return Err(AttestationError::InvalidStructure("RSA e missing")),
    };

    let pubkey = RsaPublicKey::new(BigUint::from_bytes_be(n), BigUint::from_bytes_be(e))
        .map_err(|_| AttestationError::SignatureInvalid)?;
    let verifier: VerifyingKey<Sha256> = VerifyingKey::new(pubkey);
    let signature = Signature::try_from(sig).map_err(|_| AttestationError::SignatureInvalid)?;
    verifier
        .verify(signed, &signature)
        .map_err(|_| AttestationError::SignatureInvalid)
}

fn verify_es256(cose: &[u8], signed: &[u8], sig: &[u8]) -> Result<(), AttestationError> {
    use p256::ecdsa::signature::Verifier;
    use p256::ecdsa::{Signature, VerifyingKey};
    use p256::EncodedPoint;

    // COSE EC2 key for P-256: x = label -2, y = label -3 (32 bytes each).
    let map = cose_key_map(cose)?;
    let x = match cose_key_int(&map, -2) {
        Some(CborValue::Bytes(b)) => b,
        _ => return Err(AttestationError::InvalidStructure("EC2 x missing")),
    };
    let y = match cose_key_int(&map, -3) {
        Some(CborValue::Bytes(b)) => b,
        _ => return Err(AttestationError::InvalidStructure("EC2 y missing")),
    };

    // Build the uncompressed SEC1 point (0x04 || x || y).
    let mut point = Vec::with_capacity(1 + x.len() + y.len());
    point.push(0x04);
    point.extend_from_slice(x);
    point.extend_from_slice(y);
    let encoded =
        EncodedPoint::from_bytes(&point).map_err(|_| AttestationError::SignatureInvalid)?;
    let key = VerifyingKey::from_encoded_point(&encoded)
        .map_err(|_| AttestationError::SignatureInvalid)?;

    // WebAuthn packed attestation signs with ASN.1 DER-encoded ECDSA; the
    // verifier hashes `signed` with SHA-256 internally.
    let signature = Signature::from_der(sig).map_err(|_| AttestationError::SignatureInvalid)?;
    key.verify(signed, &signature)
        .map_err(|_| AttestationError::SignatureInvalid)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1v15::SigningKey;
    use rsa::signature::{SignatureEncoding, Signer};
    use rsa::traits::PublicKeyParts;
    use rsa::{BigUint, RsaPrivateKey};

    fn b64url(b: &[u8]) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(b)
    }

    fn build_auth_data(cose_pub: &[u8]) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&[0u8; 32]);
        data.push(0x41);
        data.extend_from_slice(&[0u8; 4]);
        data.extend_from_slice(&[0u8; 16]);
        let cred_id: [u8; 4] = [1, 2, 3, 4];
        data.extend_from_slice(&(cred_id.len() as u16).to_be_bytes());
        data.extend_from_slice(&cred_id);
        data.extend_from_slice(cose_pub);
        data
    }

    fn build_rs256_cose(n: &BigUint, e: &BigUint) -> Vec<u8> {
        let map = CborValue::Map(vec![
            (CborValue::Integer(1.into()), CborValue::Integer(3.into())),
            (
                CborValue::Integer(3.into()),
                CborValue::Integer((-257_i64).into()),
            ),
            (
                CborValue::Integer((-1_i64).into()),
                CborValue::Bytes(n.to_bytes_be()),
            ),
            (
                CborValue::Integer((-2_i64).into()),
                CborValue::Bytes(e.to_bytes_be()),
            ),
        ]);
        let mut out = Vec::new();
        ciborium::ser::into_writer(&map, &mut out).unwrap();
        out
    }

    fn build_attestation(
        fmt: &str,
        auth_data: &[u8],
        alg: i64,
        sig: &[u8],
        x5c: Option<Vec<Vec<u8>>>,
    ) -> String {
        let mut att_stmt = vec![
            (
                CborValue::Text("alg".into()),
                CborValue::Integer(alg.into()),
            ),
            (
                CborValue::Text("sig".into()),
                CborValue::Bytes(sig.to_vec()),
            ),
        ];
        if let Some(chain) = x5c {
            let arr: Vec<CborValue> = chain.into_iter().map(CborValue::Bytes).collect();
            att_stmt.push((CborValue::Text("x5c".into()), CborValue::Array(arr)));
        }

        let top = CborValue::Map(vec![
            (CborValue::Text("fmt".into()), CborValue::Text(fmt.into())),
            (
                CborValue::Text("authData".into()),
                CborValue::Bytes(auth_data.to_vec()),
            ),
            (CborValue::Text("attStmt".into()), CborValue::Map(att_stmt)),
        ]);
        let mut out = Vec::new();
        ciborium::ser::into_writer(&top, &mut out).unwrap();
        b64url(&out)
    }

    #[test]
    fn parses_packed_self_attestation() {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let pub_key = key.to_public_key();
        let cose = build_rs256_cose(pub_key.n(), pub_key.e());
        let auth_data = build_auth_data(&cose);

        let client_data_json = br#"{"type":"webauthn.create"}"#;
        let mut hasher = Sha256::new();
        hasher.update(client_data_json);
        let cd_hash = hasher.finalize();
        let mut signed = auth_data.clone();
        signed.extend_from_slice(&cd_hash);

        let signing_key: SigningKey<Sha256> = SigningKey::new(key);
        let sig = signing_key.sign(&signed).to_bytes();

        let att_b64 = build_attestation("packed", &auth_data, -257, &sig, None);
        let parsed = parse_packed_attestation(&att_b64).expect("parse");
        assert_eq!(parsed.alg, -257);
        assert!(parsed.x5c.is_empty());
        assert!(parsed.cose_public_key.is_some());
        verify_packed_attestation(&parsed, client_data_json).expect("verify");
    }

    #[test]
    fn rejects_fido_u2f_format() {
        let auth_data = build_auth_data(&[]);
        let att_b64 = build_attestation("fido-u2f", &auth_data, -7, &[0u8; 64], None);
        let err = parse_packed_attestation(&att_b64).unwrap_err();
        assert!(matches!(err, AttestationError::UnsupportedFormat(_)));
    }

    #[test]
    fn rejects_tpm_format() {
        let auth_data = build_auth_data(&[]);
        let att_b64 = build_attestation("tpm", &auth_data, -257, &[0u8; 64], None);
        assert!(matches!(
            parse_packed_attestation(&att_b64).unwrap_err(),
            AttestationError::UnsupportedFormat(f) if f == "tpm"
        ));
    }

    #[test]
    fn packed_self_attest_rejects_bad_signature() {
        let mut rng = rand::thread_rng();
        let key = RsaPrivateKey::new(&mut rng, 2048).unwrap();
        let cose = build_rs256_cose(key.to_public_key().n(), key.to_public_key().e());
        let auth_data = build_auth_data(&cose);
        let bogus_sig = vec![0u8; 256];

        let att_b64 = build_attestation("packed", &auth_data, -257, &bogus_sig, None);
        let parsed = parse_packed_attestation(&att_b64).expect("parse");
        let err = verify_packed_attestation(&parsed, b"{}").unwrap_err();
        assert!(matches!(err, AttestationError::SignatureInvalid));
    }

    #[test]
    fn packed_with_x5c_skips_self_verify() {
        let auth_data = build_auth_data(&[]);
        let cert = vec![0x30, 0x82, 0x00, 0x10];
        let att_b64 = build_attestation("packed", &auth_data, -7, &[0u8; 64], Some(vec![cert]));
        let parsed = parse_packed_attestation(&att_b64).expect("parse");
        assert_eq!(parsed.x5c.len(), 1);
        verify_packed_attestation(&parsed, b"{}").expect("x5c accepted");
    }

    fn build_es256_cose(x: &[u8], y: &[u8]) -> Vec<u8> {
        let map = CborValue::Map(vec![
            // kty = EC2 (2)
            (CborValue::Integer(1.into()), CborValue::Integer(2.into())),
            // alg = ES256 (-7)
            (
                CborValue::Integer(3.into()),
                CborValue::Integer((-7_i64).into()),
            ),
            // crv = P-256 (1)
            (
                CborValue::Integer((-1_i64).into()),
                CborValue::Integer(1.into()),
            ),
            (
                CborValue::Integer((-2_i64).into()),
                CborValue::Bytes(x.to_vec()),
            ),
            (
                CborValue::Integer((-3_i64).into()),
                CborValue::Bytes(y.to_vec()),
            ),
        ]);
        let mut out = Vec::new();
        ciborium::ser::into_writer(&map, &mut out).unwrap();
        out
    }

    #[test]
    fn packed_self_attest_es256_real_signature_verifies() {
        use p256::ecdsa::signature::Signer;
        use p256::ecdsa::{Signature, SigningKey};

        let signing = SigningKey::random(&mut rand::thread_rng());
        let point = signing.verifying_key().to_encoded_point(false);
        let cose = build_es256_cose(point.x().unwrap(), point.y().unwrap());
        let auth_data = build_auth_data(&cose);

        let client_data_json = br#"{"type":"webauthn.create"}"#;
        let mut hasher = Sha256::new();
        hasher.update(client_data_json);
        let cd_hash = hasher.finalize();
        let mut signed = auth_data.clone();
        signed.extend_from_slice(&cd_hash);

        let sig: Signature = signing.sign(&signed);
        let der = sig.to_der().as_bytes().to_vec();

        // A genuine ES256 signature verifies.
        let att_b64 = build_attestation("packed", &auth_data, -7, &der, None);
        let parsed = parse_packed_attestation(&att_b64).expect("parse");
        verify_packed_attestation(&parsed, client_data_json).expect("real ES256 sig must verify");

        // A forged signature is rejected (no longer silently accepted).
        let bad = build_attestation(
            "packed",
            &auth_data,
            -7,
            &vec![0x30, 0x06, 0x02, 0x01, 0x01, 0x02, 0x01, 0x01],
            None,
        );
        let parsed_bad = parse_packed_attestation(&bad).expect("parse");
        assert!(matches!(
            verify_packed_attestation(&parsed_bad, client_data_json).unwrap_err(),
            AttestationError::SignatureInvalid
        ));
    }
}
