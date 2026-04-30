//! Real ECDSA Sign/Verify for KMS asymmetric specs.
//!
//! AWS KMS supports four ECDSA curves: NIST P-256, P-384, P-521, and
//! SECG (Bitcoin's secp256k1). We implement P-256, P-384, and P-256K1
//! against the `p256` / `p384` / `k256` crates. P-521 has no widely
//! used pure-Rust ECDSA crate at this version, so it falls through to
//! the legacy fake-bytes path documented in `service_crypto.rs`.

use rsa::sha2::{Sha256, Sha384};
use signature::{Signer, Verifier};

#[derive(Debug, thiserror::Error)]
pub enum EcdsaError {
    #[error("unsupported signing algorithm for this curve: {0}")]
    UnsupportedAlgorithm(String),
    #[error("key material is corrupt: {0}")]
    CorruptKey(String),
    #[error("crypto failure: {0}")]
    CryptoFailure(String),
}

pub type KeyPair = (Vec<u8>, Vec<u8>);

/// Returns `(pkcs8_private_der, spki_public_der)` for an ECDSA spec
/// we generate real keypairs for. Returns `Ok(None)` for specs we
/// don't handle here (P-521, SM2) so the caller can fall back to the
/// placeholder bytes generator.
pub fn generate_keypair(key_spec: &str) -> Result<Option<KeyPair>, EcdsaError> {
    use p256::pkcs8::EncodePrivateKey as _;
    match key_spec {
        "ECC_NIST_P256" => {
            let mut rng = rand::thread_rng();
            let sk = p256::ecdsa::SigningKey::random(&mut rng);
            let priv_der = sk
                .to_pkcs8_der()
                .map_err(|e| EcdsaError::CryptoFailure(format!("p256 pkcs8 encode: {e}")))?
                .as_bytes()
                .to_vec();
            let vk = sk.verifying_key();
            let pub_der = {
                use p256::pkcs8::EncodePublicKey as _;
                vk.to_public_key_der()
                    .map_err(|e| EcdsaError::CryptoFailure(format!("p256 spki encode: {e}")))?
                    .as_bytes()
                    .to_vec()
            };
            Ok(Some((priv_der, pub_der)))
        }
        "ECC_NIST_P384" => {
            use p384::pkcs8::{EncodePrivateKey as _, EncodePublicKey as _};
            let mut rng = rand::thread_rng();
            let sk = p384::ecdsa::SigningKey::random(&mut rng);
            let priv_der = sk
                .to_pkcs8_der()
                .map_err(|e| EcdsaError::CryptoFailure(format!("p384 pkcs8 encode: {e}")))?
                .as_bytes()
                .to_vec();
            let vk = sk.verifying_key();
            let pub_der = vk
                .to_public_key_der()
                .map_err(|e| EcdsaError::CryptoFailure(format!("p384 spki encode: {e}")))?
                .as_bytes()
                .to_vec();
            Ok(Some((priv_der, pub_der)))
        }
        "ECC_SECG_P256K1" => {
            use k256::pkcs8::{EncodePrivateKey as _, EncodePublicKey as _};
            let mut rng = rand::thread_rng();
            let sk = k256::ecdsa::SigningKey::random(&mut rng);
            let priv_der = sk
                .to_pkcs8_der()
                .map_err(|e| EcdsaError::CryptoFailure(format!("k256 pkcs8 encode: {e}")))?
                .as_bytes()
                .to_vec();
            let vk = sk.verifying_key();
            let pub_der = vk
                .to_public_key_der()
                .map_err(|e| EcdsaError::CryptoFailure(format!("k256 spki encode: {e}")))?
                .as_bytes()
                .to_vec();
            Ok(Some((priv_der, pub_der)))
        }
        _ => Ok(None),
    }
}

/// Sign `message` with the curve+algorithm combination AWS expects.
/// `message_is_digest` corresponds to KMS's `MessageType=DIGEST`.
pub fn sign(
    key_spec: &str,
    priv_der: &[u8],
    signing_algorithm: &str,
    message: &[u8],
    message_is_digest: bool,
) -> Result<Vec<u8>, EcdsaError> {
    match (key_spec, signing_algorithm) {
        ("ECC_NIST_P256", "ECDSA_SHA_256") => {
            use p256::pkcs8::DecodePrivateKey as _;
            let sk = p256::ecdsa::SigningKey::from_pkcs8_der(priv_der)
                .map_err(|e| EcdsaError::CorruptKey(format!("p256 decode: {e}")))?;
            if message_is_digest {
                let digest = digest_to_array_32(message)?;
                let sig: p256::ecdsa::Signature = sk
                    .sign_prehash_recoverable(&digest)
                    .map(|(s, _)| s)
                    .map_err(|e| EcdsaError::CryptoFailure(format!("p256 sign prehash: {e}")))?;
                Ok(sig.to_der().as_bytes().to_vec())
            } else {
                let sig: p256::ecdsa::Signature = sk.sign(message);
                Ok(sig.to_der().as_bytes().to_vec())
            }
        }
        ("ECC_NIST_P384", "ECDSA_SHA_384") => {
            use p384::pkcs8::DecodePrivateKey as _;
            let sk = p384::ecdsa::SigningKey::from_pkcs8_der(priv_der)
                .map_err(|e| EcdsaError::CorruptKey(format!("p384 decode: {e}")))?;
            if message_is_digest {
                let digest = digest_to_array_48(message)?;
                let sig: p384::ecdsa::Signature = sk
                    .sign_prehash_recoverable(&digest)
                    .map(|(s, _)| s)
                    .map_err(|e| EcdsaError::CryptoFailure(format!("p384 sign prehash: {e}")))?;
                Ok(sig.to_der().as_bytes().to_vec())
            } else {
                let sig: p384::ecdsa::Signature = sk.sign(message);
                Ok(sig.to_der().as_bytes().to_vec())
            }
        }
        ("ECC_SECG_P256K1", "ECDSA_SHA_256") => {
            use k256::pkcs8::DecodePrivateKey as _;
            let sk = k256::ecdsa::SigningKey::from_pkcs8_der(priv_der)
                .map_err(|e| EcdsaError::CorruptKey(format!("k256 decode: {e}")))?;
            if message_is_digest {
                let digest = digest_to_array_32(message)?;
                let sig: k256::ecdsa::Signature = sk
                    .sign_prehash_recoverable(&digest)
                    .map(|(s, _)| s)
                    .map_err(|e| EcdsaError::CryptoFailure(format!("k256 sign prehash: {e}")))?;
                Ok(sig.to_der().as_bytes().to_vec())
            } else {
                let sig: k256::ecdsa::Signature = sk.sign(message);
                Ok(sig.to_der().as_bytes().to_vec())
            }
        }
        _ => Err(EcdsaError::UnsupportedAlgorithm(format!(
            "{key_spec}/{signing_algorithm}"
        ))),
    }
}

pub fn verify(
    key_spec: &str,
    priv_der: &[u8],
    signing_algorithm: &str,
    message: &[u8],
    signature: &[u8],
    message_is_digest: bool,
) -> Result<bool, EcdsaError> {
    match (key_spec, signing_algorithm) {
        ("ECC_NIST_P256", "ECDSA_SHA_256") => {
            use p256::pkcs8::DecodePrivateKey as _;
            let sk = p256::ecdsa::SigningKey::from_pkcs8_der(priv_der)
                .map_err(|e| EcdsaError::CorruptKey(format!("p256 decode: {e}")))?;
            let vk = sk.verifying_key();
            let sig = p256::ecdsa::Signature::from_der(signature).or_else(|_| {
                p256::ecdsa::Signature::try_from(signature)
                    .map_err(|e| EcdsaError::CryptoFailure(format!("p256 sig parse: {e}")))
            });
            let sig = match sig {
                Ok(s) => s,
                Err(_) => return Ok(false),
            };
            if message_is_digest {
                use p256::ecdsa::signature::hazmat::PrehashVerifier;
                Ok(vk.verify_prehash(message, &sig).is_ok())
            } else {
                Ok(vk.verify(message, &sig).is_ok())
            }
        }
        ("ECC_NIST_P384", "ECDSA_SHA_384") => {
            use p384::pkcs8::DecodePrivateKey as _;
            let sk = p384::ecdsa::SigningKey::from_pkcs8_der(priv_der)
                .map_err(|e| EcdsaError::CorruptKey(format!("p384 decode: {e}")))?;
            let vk = sk.verifying_key();
            let sig = p384::ecdsa::Signature::from_der(signature).or_else(|_| {
                p384::ecdsa::Signature::try_from(signature)
                    .map_err(|e| EcdsaError::CryptoFailure(format!("p384 sig parse: {e}")))
            });
            let sig = match sig {
                Ok(s) => s,
                Err(_) => return Ok(false),
            };
            if message_is_digest {
                use p384::ecdsa::signature::hazmat::PrehashVerifier;
                Ok(vk.verify_prehash(message, &sig).is_ok())
            } else {
                Ok(vk.verify(message, &sig).is_ok())
            }
        }
        ("ECC_SECG_P256K1", "ECDSA_SHA_256") => {
            use k256::pkcs8::DecodePrivateKey as _;
            let sk = k256::ecdsa::SigningKey::from_pkcs8_der(priv_der)
                .map_err(|e| EcdsaError::CorruptKey(format!("k256 decode: {e}")))?;
            let vk = sk.verifying_key();
            let sig = k256::ecdsa::Signature::from_der(signature).or_else(|_| {
                k256::ecdsa::Signature::try_from(signature)
                    .map_err(|e| EcdsaError::CryptoFailure(format!("k256 sig parse: {e}")))
            });
            let sig = match sig {
                Ok(s) => s,
                Err(_) => return Ok(false),
            };
            if message_is_digest {
                use k256::ecdsa::signature::hazmat::PrehashVerifier;
                Ok(vk.verify_prehash(message, &sig).is_ok())
            } else {
                Ok(vk.verify(message, &sig).is_ok())
            }
        }
        _ => Err(EcdsaError::UnsupportedAlgorithm(format!(
            "{key_spec}/{signing_algorithm}"
        ))),
    }
}

fn digest_to_array_32(d: &[u8]) -> Result<[u8; 32], EcdsaError> {
    d.try_into().map_err(|_| {
        EcdsaError::CryptoFailure("MessageType=DIGEST requires 32-byte digest for SHA-256".into())
    })
}

fn digest_to_array_48(d: &[u8]) -> Result<[u8; 48], EcdsaError> {
    d.try_into().map_err(|_| {
        EcdsaError::CryptoFailure("MessageType=DIGEST requires 48-byte digest for SHA-384".into())
    })
}

#[allow(dead_code)]
fn _unused_imports_keepalive() {
    let _ = Sha256::default();
    let _ = Sha384::default();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn p256_sign_verify_roundtrip_with_message() {
        let (priv_der, _pub_der) = generate_keypair("ECC_NIST_P256").unwrap().unwrap();
        let msg = b"hello p256";
        let sig = sign("ECC_NIST_P256", &priv_der, "ECDSA_SHA_256", msg, false).unwrap();
        assert!(verify(
            "ECC_NIST_P256",
            &priv_der,
            "ECDSA_SHA_256",
            msg,
            &sig,
            false
        )
        .unwrap());
    }

    #[test]
    fn p384_sign_verify_roundtrip_with_message() {
        let (priv_der, _) = generate_keypair("ECC_NIST_P384").unwrap().unwrap();
        let msg = b"hello p384";
        let sig = sign("ECC_NIST_P384", &priv_der, "ECDSA_SHA_384", msg, false).unwrap();
        assert!(verify(
            "ECC_NIST_P384",
            &priv_der,
            "ECDSA_SHA_384",
            msg,
            &sig,
            false
        )
        .unwrap());
    }

    #[test]
    fn k256_sign_verify_roundtrip_with_message() {
        let (priv_der, _) = generate_keypair("ECC_SECG_P256K1").unwrap().unwrap();
        let msg = b"hello secp256k1";
        let sig = sign("ECC_SECG_P256K1", &priv_der, "ECDSA_SHA_256", msg, false).unwrap();
        assert!(verify(
            "ECC_SECG_P256K1",
            &priv_der,
            "ECDSA_SHA_256",
            msg,
            &sig,
            false
        )
        .unwrap());
    }

    #[test]
    fn p256_tampered_message_fails_verify() {
        let (priv_der, _) = generate_keypair("ECC_NIST_P256").unwrap().unwrap();
        let sig = sign("ECC_NIST_P256", &priv_der, "ECDSA_SHA_256", b"a", false).unwrap();
        assert!(!verify(
            "ECC_NIST_P256",
            &priv_der,
            "ECDSA_SHA_256",
            b"b",
            &sig,
            false
        )
        .unwrap());
    }

    #[test]
    fn unsupported_curve_returns_none_for_keypair() {
        // P-521 is currently out of scope here.
        assert!(generate_keypair("ECC_NIST_P521").unwrap().is_none());
    }

    #[test]
    fn p256_get_public_key_is_real_parseable_spki() {
        use p256::pkcs8::DecodePublicKey;
        let (_priv_der, pub_der) = generate_keypair("ECC_NIST_P256").unwrap().unwrap();
        assert!(p256::PublicKey::from_public_key_der(&pub_der).is_ok());
    }
}
