//! Real RSA-2048 keypair generation + RS256 JWT signing for User Pools.
//!
//! Each pool gets one keypair on creation; we sign every issued JWT with
//! the pool's private key and publish the public half at the pool's
//! JWKS endpoint so SDKs that verify tokens against the discovery URL
//! (the AWS-recommended path) get a real, verifiable signature.

use base64::Engine as _;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::EncodePrivateKey;
use rsa::pkcs8::EncodePublicKey;
use rsa::sha2::{Digest, Sha256};
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::traits::PublicKeyParts;
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde_json::Value;

/// A freshly-minted pool signing key plus the deterministic `kid`
/// derived from its SubjectPublicKeyInfo DER. Both halves travel
/// together so callers don't have to re-derive the kid on every issue.
pub struct PoolSigningKey {
    /// PKCS#8-encoded private key PEM (embeds the public half).
    pub private_key_pem: String,
    /// Stable JWKS key id: first 16 hex chars of `SHA-256(SPKI DER)`.
    pub kid: String,
}

/// Generate a fresh RSA-2048 keypair, returning the PKCS#8 PEM-encoded
/// private key alongside its deterministic kid. AWS Cognito generates
/// the keypair at pool-create time and serves the public half at
/// `/<pool>/.well-known/jwks.json`.
pub fn generate_pool_signing_key() -> PoolSigningKey {
    let mut rng = rand::thread_rng();
    // 2048 bits matches what Cognito issues today; the PKCS#8 encoding
    // round-trips through `RsaPrivateKey::from_pkcs8_pem` for sign-time
    // recovery.
    let private_key = RsaPrivateKey::new(&mut rng, 2048).expect("RSA-2048 keygen should not fail");
    let private_key_pem = private_key
        .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
        .expect("PKCS#8 PEM encode")
        .to_string();
    let public_key = RsaPublicKey::from(&private_key);
    let kid = compute_kid(&public_key);
    PoolSigningKey {
        private_key_pem,
        kid,
    }
}

/// Derive the JWKS `kid` deterministically from the public key.
///
/// `kid = first 16 hex chars of SHA-256(SubjectPublicKeyInfo DER)`.
/// 16 hex chars (8 bytes / 64 bits) is enough to make collisions
/// astronomically unlikely while keeping the JWT header compact.
pub fn compute_kid(public_key: &RsaPublicKey) -> String {
    let spki_der = public_key
        .to_public_key_der()
        .expect("SPKI DER encode")
        .to_vec();
    let mut hasher = Sha256::new();
    hasher.update(&spki_der);
    let digest = hasher.finalize();
    // 8 bytes -> 16 hex chars. Long enough that two keys never collide
    // in practice, short enough to fit comfortably in a JWT header.
    let mut hex = String::with_capacity(16);
    for byte in &digest[..8] {
        hex.push_str(&format!("{byte:02x}"));
    }
    hex
}

/// Sign `header` + `payload` with `private_key_pem` using PKCS#1 v1.5
/// RS256 and return the compact-serialized JWT
/// (`<header>.<payload>.<sig>`). Caller is expected to set `alg=RS256`
/// and a real `kid` on the header; this function trusts those values.
///
/// Returns `None` if the PEM fails to decode. Pools created through
/// `CreateUserPool` always carry a valid PEM, and
/// `ensure_pool_signing_key` lazily fills in pre-existing snapshots, so
/// `None` only happens when a caller passes garbage in.
pub(crate) fn sign_rs256(header: &Value, payload: &Value, private_key_pem: &str) -> Option<String> {
    use rsa::pkcs8::DecodePrivateKey;

    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let header_b64 = b64.encode(header.to_string().as_bytes());
    let payload_b64 = b64.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");

    let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem).ok()?;
    let signing_key = SigningKey::<Sha256>::new(private_key);
    let mut rng = rand::thread_rng();
    let signature = signing_key.sign_with_rng(&mut rng, signing_input.as_bytes());
    let sig_b64 = b64.encode(signature.to_bytes());
    Some(format!("{header_b64}.{payload_b64}.{sig_b64}"))
}

/// Render the pool's RSA public key as a single-key JWKS document
/// (`{"keys": [<jwk>]}`) with the `kid` baked in. Matches the shape the
/// AWS-published `/.well-known/jwks.json` endpoints serve. Used by the
/// JWKS HTTP endpoint (Y2) which is wired in fakecloud-server.
pub fn jwks_document(private_key_pem: &str, kid: &str) -> Value {
    use rsa::pkcs8::DecodePrivateKey;
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let Ok(private_key) = RsaPrivateKey::from_pkcs8_pem(private_key_pem) else {
        return serde_json::json!({"keys": []});
    };
    let public_key = RsaPublicKey::from(&private_key);
    let n_b64 = b64.encode(public_key.n().to_bytes_be());
    let e_b64 = b64.encode(public_key.e().to_bytes_be());
    serde_json::json!({
        "keys": [
            {
                "alg": "RS256",
                "e": e_b64,
                "kid": kid,
                "kty": "RSA",
                "n": n_b64,
                "use": "sig",
            }
        ]
    })
}

/// Verify an RS256-signed compact JWT against the public half of
/// `private_key_pem` (the pool's stored signing PEM embeds both halves).
/// Returns the decoded `(header, payload)` pair on success, or an error
/// string the caller can surface in a 401 response.
///
/// Only validates the cryptographic signature here; expiry/issuer/audience
/// are the caller's responsibility because policy varies by use case
/// (API Gateway v1 wants `iss` + `aud`/`client_id`; userinfo just wants
/// not-expired).
pub fn verify_rs256(token: &str, private_key_pem: &str) -> Result<(Value, Value), String> {
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::signature::Verifier;

    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("malformed JWT (expected three dot-separated parts)".to_string());
    }
    let header_bytes = b64
        .decode(parts[0])
        .map_err(|e| format!("invalid header base64url: {e}"))?;
    let payload_bytes = b64
        .decode(parts[1])
        .map_err(|e| format!("invalid payload base64url: {e}"))?;
    let sig_bytes = b64
        .decode(parts[2])
        .map_err(|e| format!("invalid signature base64url: {e}"))?;
    let header: Value =
        serde_json::from_slice(&header_bytes).map_err(|e| format!("invalid header JSON: {e}"))?;
    let payload: Value =
        serde_json::from_slice(&payload_bytes).map_err(|e| format!("invalid payload JSON: {e}"))?;

    let private_key = RsaPrivateKey::from_pkcs8_pem(private_key_pem)
        .map_err(|e| format!("invalid signing PEM: {e}"))?;
    let public_key = RsaPublicKey::from(&private_key);
    let verifying_key = VerifyingKey::<Sha256>::new(public_key);
    let signature = Signature::try_from(sig_bytes.as_slice())
        .map_err(|e| format!("invalid signature bytes: {e}"))?;
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    verifying_key
        .verify(signing_input.as_bytes(), &signature)
        .map_err(|e| format!("signature verification failed: {e}"))?;
    Ok((header, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::signature::Verifier;

    #[test]
    fn signed_jwt_verifies_with_pool_public_key() {
        let key = generate_pool_signing_key();
        let header = serde_json::json!({"alg": "RS256", "kid": &key.kid, "typ": "JWT"});
        let payload = serde_json::json!({"sub": "user-1"});
        let token = sign_rs256(&header, &payload, &key.private_key_pem)
            .expect("freshly-generated PEM must sign");

        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);

        let private_key = RsaPrivateKey::from_pkcs8_pem(&key.private_key_pem).unwrap();
        let public_key = RsaPublicKey::from(&private_key);
        let verifying_key = VerifyingKey::<Sha256>::new(public_key);

        let signing_input = format!("{}.{}", parts[0], parts[1]);
        let sig_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[2])
            .unwrap();
        let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();
        verifying_key
            .verify(signing_input.as_bytes(), &signature)
            .expect("token must verify against pool's public key");
    }

    #[test]
    fn jwks_document_emits_n_and_e() {
        let key = generate_pool_signing_key();
        let jwks = jwks_document(&key.private_key_pem, &key.kid);
        let jwk = &jwks["keys"][0];
        assert_eq!(jwk["kid"], key.kid);
        assert_eq!(jwk["alg"], "RS256");
        assert_eq!(jwk["kty"], "RSA");
        assert!(jwk["n"].as_str().unwrap().len() > 100);
        assert!(!jwk["e"].as_str().unwrap().is_empty());
    }

    #[test]
    fn kid_is_deterministic_16_hex_chars_per_key() {
        let key = generate_pool_signing_key();
        assert_eq!(key.kid.len(), 16, "kid must be 16 hex chars");
        assert!(
            key.kid.chars().all(|c| c.is_ascii_hexdigit()),
            "kid must be lowercase hex: {}",
            key.kid
        );
        // Re-deriving the kid from the same public half must reproduce
        // the same value — that's what JWKS clients depend on.
        let private_key = RsaPrivateKey::from_pkcs8_pem(&key.private_key_pem).unwrap();
        let public_key = RsaPublicKey::from(&private_key);
        assert_eq!(compute_kid(&public_key), key.kid);
    }

    #[test]
    fn distinct_keys_produce_distinct_kids() {
        let a = generate_pool_signing_key();
        let b = generate_pool_signing_key();
        assert_ne!(a.kid, b.kid);
    }
}
