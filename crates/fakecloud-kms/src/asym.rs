//! Real RSA asymmetric crypto for KMS Sign/Verify/GetPublicKey.
//!
//! AWS KMS represents asymmetric keys as PKCS#8 (private) and
//! SubjectPublicKeyInfo (public) DER. Generating the keypair at
//! CreateKey time and storing both halves means callers can sign
//! locally, retrieve the public key via GetPublicKey, and verify
//! against it with any standard tool — and importantly, our Verify
//! op produces the same answer because it uses the same crypto.

use rsa::pkcs1v15::{Signature, SigningKey, VerifyingKey};
use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey};
use rsa::pss::{BlindedSigningKey, VerifyingKey as PssVerifyingKey};
use rsa::sha2::{Sha256, Sha384, Sha512};
use rsa::{RsaPrivateKey, RsaPublicKey};
use signature::{RandomizedSigner, SignatureEncoding, Signer, Verifier};

#[derive(Debug, thiserror::Error)]
pub enum AsymError {
    #[error("unsupported signing algorithm for this key: {0}")]
    UnsupportedAlgorithm(String),
    #[error("key material is corrupt: {0}")]
    CorruptKey(String),
    #[error("crypto failure: {0}")]
    CryptoFailure(String),
}

pub type KeyPair = (Vec<u8>, Vec<u8>);

/// Returns (private_pkcs8_der, public_spki_der) for the given KMS
/// `KeySpec`. Returns `None` for symmetric / HMAC / unsupported specs
/// so the caller can keep the existing fake-bytes path for those.
pub fn generate_keypair(key_spec: &str) -> Result<Option<KeyPair>, AsymError> {
    let bits = match key_spec {
        "RSA_2048" => 2048,
        "RSA_3072" => 3072,
        "RSA_4096" => 4096,
        // ECDSA / ECDH / SM2 specs are out of scope for G1; G2 covers
        // ECDSA. Falling through to None keeps the fake-bytes legacy
        // path in place for those.
        _ => return Ok(None),
    };
    let mut rng = rand::thread_rng();
    let private = RsaPrivateKey::new(&mut rng, bits)
        .map_err(|e| AsymError::CryptoFailure(format!("rsa keygen: {e}")))?;
    let public = RsaPublicKey::from(&private);
    let priv_der = private
        .to_pkcs8_der()
        .map_err(|e| AsymError::CryptoFailure(format!("pkcs8 encode: {e}")))?
        .as_bytes()
        .to_vec();
    let pub_der = public
        .to_public_key_der()
        .map_err(|e| AsymError::CryptoFailure(format!("spki encode: {e}")))?
        .as_bytes()
        .to_vec();
    Ok(Some((priv_der, pub_der)))
}

/// Signs `message` with the private key encoded in `priv_der` using
/// the AWS KMS `signing_algorithm` name (e.g. `RSASSA_PSS_SHA_256`).
/// `message_is_digest` corresponds to KMS's `MessageType=DIGEST`.
pub fn rsa_sign(
    priv_der: &[u8],
    signing_algorithm: &str,
    message: &[u8],
    message_is_digest: bool,
) -> Result<Vec<u8>, AsymError> {
    let private = RsaPrivateKey::from_pkcs8_der(priv_der)
        .map_err(|e| AsymError::CorruptKey(format!("decode pkcs8: {e}")))?;
    if message_is_digest {
        return rsa_sign_prehashed(&private, signing_algorithm, message);
    }
    match signing_algorithm {
        "RSASSA_PKCS1_V1_5_SHA_256" => {
            let sk: SigningKey<Sha256> = SigningKey::new(private);
            let sig = sk.sign(message);
            Ok(sig.to_vec())
        }
        "RSASSA_PKCS1_V1_5_SHA_384" => {
            let sk: SigningKey<Sha384> = SigningKey::new(private);
            let sig = sk.sign(message);
            Ok(sig.to_vec())
        }
        "RSASSA_PKCS1_V1_5_SHA_512" => {
            let sk: SigningKey<Sha512> = SigningKey::new(private);
            let sig = sk.sign(message);
            Ok(sig.to_vec())
        }
        "RSASSA_PSS_SHA_256" => {
            let sk: BlindedSigningKey<Sha256> = BlindedSigningKey::new(private);
            let mut rng = rand::thread_rng();
            let sig = sk.sign_with_rng(&mut rng, message);
            Ok(sig.to_vec())
        }
        "RSASSA_PSS_SHA_384" => {
            let sk: BlindedSigningKey<Sha384> = BlindedSigningKey::new(private);
            let mut rng = rand::thread_rng();
            let sig = sk.sign_with_rng(&mut rng, message);
            Ok(sig.to_vec())
        }
        "RSASSA_PSS_SHA_512" => {
            let sk: BlindedSigningKey<Sha512> = BlindedSigningKey::new(private);
            let mut rng = rand::thread_rng();
            let sig = sk.sign_with_rng(&mut rng, message);
            Ok(sig.to_vec())
        }
        other => Err(AsymError::UnsupportedAlgorithm(other.to_string())),
    }
}

fn rsa_sign_prehashed(
    private: &RsaPrivateKey,
    signing_algorithm: &str,
    digest: &[u8],
) -> Result<Vec<u8>, AsymError> {
    use rsa::Pkcs1v15Sign;
    let mut rng = rand::thread_rng();
    match signing_algorithm {
        "RSASSA_PKCS1_V1_5_SHA_256" => private
            .sign(Pkcs1v15Sign::new::<Sha256>(), digest)
            .map_err(|e| AsymError::CryptoFailure(e.to_string())),
        "RSASSA_PKCS1_V1_5_SHA_384" => private
            .sign(Pkcs1v15Sign::new::<Sha384>(), digest)
            .map_err(|e| AsymError::CryptoFailure(e.to_string())),
        "RSASSA_PKCS1_V1_5_SHA_512" => private
            .sign(Pkcs1v15Sign::new::<Sha512>(), digest)
            .map_err(|e| AsymError::CryptoFailure(e.to_string())),
        "RSASSA_PSS_SHA_256" => private
            .sign_with_rng(&mut rng, rsa::Pss::new::<Sha256>(), digest)
            .map_err(|e| AsymError::CryptoFailure(e.to_string())),
        "RSASSA_PSS_SHA_384" => private
            .sign_with_rng(&mut rng, rsa::Pss::new::<Sha384>(), digest)
            .map_err(|e| AsymError::CryptoFailure(e.to_string())),
        "RSASSA_PSS_SHA_512" => private
            .sign_with_rng(&mut rng, rsa::Pss::new::<Sha512>(), digest)
            .map_err(|e| AsymError::CryptoFailure(e.to_string())),
        other => Err(AsymError::UnsupportedAlgorithm(other.to_string())),
    }
}

pub fn rsa_verify(
    priv_der: &[u8],
    signing_algorithm: &str,
    message: &[u8],
    signature: &[u8],
    message_is_digest: bool,
) -> Result<bool, AsymError> {
    let private = RsaPrivateKey::from_pkcs8_der(priv_der)
        .map_err(|e| AsymError::CorruptKey(format!("decode pkcs8: {e}")))?;
    let public = RsaPublicKey::from(&private);
    if message_is_digest {
        return rsa_verify_prehashed(&public, signing_algorithm, message, signature);
    }
    let sig = Signature::try_from(signature)
        .map_err(|e| AsymError::CryptoFailure(format!("decode sig: {e}")));
    match signing_algorithm {
        "RSASSA_PKCS1_V1_5_SHA_256" => {
            let vk: VerifyingKey<Sha256> = VerifyingKey::new(public);
            Ok(vk.verify(message, &sig?).is_ok())
        }
        "RSASSA_PKCS1_V1_5_SHA_384" => {
            let vk: VerifyingKey<Sha384> = VerifyingKey::new(public);
            Ok(vk.verify(message, &sig?).is_ok())
        }
        "RSASSA_PKCS1_V1_5_SHA_512" => {
            let vk: VerifyingKey<Sha512> = VerifyingKey::new(public);
            Ok(vk.verify(message, &sig?).is_ok())
        }
        "RSASSA_PSS_SHA_256" => {
            let vk: PssVerifyingKey<Sha256> = PssVerifyingKey::new(public);
            let pss_sig = rsa::pss::Signature::try_from(signature)
                .map_err(|e| AsymError::CryptoFailure(format!("decode pss sig: {e}")))?;
            Ok(vk.verify(message, &pss_sig).is_ok())
        }
        "RSASSA_PSS_SHA_384" => {
            let vk: PssVerifyingKey<Sha384> = PssVerifyingKey::new(public);
            let pss_sig = rsa::pss::Signature::try_from(signature)
                .map_err(|e| AsymError::CryptoFailure(format!("decode pss sig: {e}")))?;
            Ok(vk.verify(message, &pss_sig).is_ok())
        }
        "RSASSA_PSS_SHA_512" => {
            let vk: PssVerifyingKey<Sha512> = PssVerifyingKey::new(public);
            let pss_sig = rsa::pss::Signature::try_from(signature)
                .map_err(|e| AsymError::CryptoFailure(format!("decode pss sig: {e}")))?;
            Ok(vk.verify(message, &pss_sig).is_ok())
        }
        other => Err(AsymError::UnsupportedAlgorithm(other.to_string())),
    }
}

fn rsa_verify_prehashed(
    public: &RsaPublicKey,
    signing_algorithm: &str,
    digest: &[u8],
    signature: &[u8],
) -> Result<bool, AsymError> {
    use rsa::Pkcs1v15Sign;
    match signing_algorithm {
        "RSASSA_PKCS1_V1_5_SHA_256" => Ok(public
            .verify(Pkcs1v15Sign::new::<Sha256>(), digest, signature)
            .is_ok()),
        "RSASSA_PKCS1_V1_5_SHA_384" => Ok(public
            .verify(Pkcs1v15Sign::new::<Sha384>(), digest, signature)
            .is_ok()),
        "RSASSA_PKCS1_V1_5_SHA_512" => Ok(public
            .verify(Pkcs1v15Sign::new::<Sha512>(), digest, signature)
            .is_ok()),
        "RSASSA_PSS_SHA_256" => Ok(public
            .verify(rsa::Pss::new::<Sha256>(), digest, signature)
            .is_ok()),
        "RSASSA_PSS_SHA_384" => Ok(public
            .verify(rsa::Pss::new::<Sha384>(), digest, signature)
            .is_ok()),
        "RSASSA_PSS_SHA_512" => Ok(public
            .verify(rsa::Pss::new::<Sha512>(), digest, signature)
            .is_ok()),
        other => Err(AsymError::UnsupportedAlgorithm(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsa_2048_pkcs1_sign_verify_roundtrip() {
        let (priv_der, _pub_der) = generate_keypair("RSA_2048").unwrap().unwrap();
        let msg = b"hello world";
        let sig = rsa_sign(&priv_der, "RSASSA_PKCS1_V1_5_SHA_256", msg, false).unwrap();
        assert!(rsa_verify(&priv_der, "RSASSA_PKCS1_V1_5_SHA_256", msg, &sig, false).unwrap());
        assert!(!rsa_verify(
            &priv_der,
            "RSASSA_PKCS1_V1_5_SHA_256",
            b"tampered",
            &sig,
            false
        )
        .unwrap());
    }

    #[test]
    fn rsa_2048_pss_sign_verify_roundtrip() {
        let (priv_der, _pub_der) = generate_keypair("RSA_2048").unwrap().unwrap();
        let msg = b"hello world";
        let sig = rsa_sign(&priv_der, "RSASSA_PSS_SHA_256", msg, false).unwrap();
        assert!(rsa_verify(&priv_der, "RSASSA_PSS_SHA_256", msg, &sig, false).unwrap());
    }

    #[test]
    fn rsa_2048_public_key_is_parseable_spki() {
        use rsa::pkcs8::DecodePublicKey;
        let (_priv_der, pub_der) = generate_keypair("RSA_2048").unwrap().unwrap();
        let parsed = rsa::RsaPublicKey::from_public_key_der(&pub_der);
        assert!(
            parsed.is_ok(),
            "GetPublicKey should return parseable SPKI DER"
        );
    }

    #[test]
    fn unsupported_spec_returns_none() {
        assert!(generate_keypair("SYMMETRIC_DEFAULT").unwrap().is_none());
        assert!(generate_keypair("HMAC_256").unwrap().is_none());
    }

    #[test]
    fn pkcs1_digest_message_type_signs_prehashed() {
        let (priv_der, _) = generate_keypair("RSA_2048").unwrap().unwrap();
        use rsa::sha2::Digest;
        let digest = Sha256::digest(b"payload");
        let sig = rsa_sign(&priv_der, "RSASSA_PKCS1_V1_5_SHA_256", &digest, true).unwrap();
        assert!(rsa_verify(&priv_der, "RSASSA_PKCS1_V1_5_SHA_256", &digest, &sig, true).unwrap());
    }
}
