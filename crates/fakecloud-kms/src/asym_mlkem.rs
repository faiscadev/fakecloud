//! Real ML-KEM (FIPS 203) key encapsulation for KMS `ML_KEM_*` keys.
//!
//! As in AWS KMS, the public (encapsulation) key is a DER-encoded X.509
//! SubjectPublicKeyInfo carrying the `id-alg-ml-kem-*` OID with no
//! parameters, and the private (decapsulation) key is a DER-encoded PKCS#8
//! PrivateKeyInfo in seed format (the 64-byte `d || z` seed under an
//! implicit `[0]` tag), so the private key is the same length for every
//! ML-KEM spec. The same encodings are used for the `ML_KEM_*` data key
//! pairs GenerateDataKeyPair returns.

use ml_kem::kem::{Decapsulate, Encapsulate, Kem};
use ml_kem::pkcs8::{DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey};
use ml_kem::{MlKem1024, MlKem512, MlKem768};

/// The only `EncapsulationAlgorithmSpec` value.
pub const ML_KEM: &str = "ML_KEM";

#[derive(Debug, thiserror::Error)]
pub enum MlKemError {
    #[error("unsupported ML-KEM key spec: {0}")]
    UnsupportedSpec(String),
    #[error("key material is corrupt: {0}")]
    CorruptKey(String),
    #[error("the ciphertext is {actual} bytes; {spec} ciphertexts are {expected} bytes")]
    CiphertextLength {
        spec: String,
        expected: usize,
        actual: usize,
    },
    #[error("crypto failure: {0}")]
    CryptoFailure(String),
}

/// `(private_pkcs8_der, public_spki_der)`.
pub type KeyPair = (Vec<u8>, Vec<u8>);

/// Whether `key_spec` is one of the ML-KEM specs.
pub fn is_ml_kem_spec(key_spec: &str) -> bool {
    matches!(key_spec, "ML_KEM_512" | "ML_KEM_768" | "ML_KEM_1024")
}

/// The encapsulated shared secret's length for an ML-KEM spec (FIPS 203
/// table 3): 768, 1088 and 1568 bytes.
pub fn ciphertext_len(key_spec: &str) -> Option<usize> {
    match key_spec {
        "ML_KEM_512" => Some(768),
        "ML_KEM_768" => Some(1088),
        "ML_KEM_1024" => Some(1568),
        _ => None,
    }
}

macro_rules! with_params {
    ($spec:expr, $p:ident => $body:expr) => {
        match $spec {
            "ML_KEM_512" => {
                type $p = MlKem512;
                $body
            }
            "ML_KEM_768" => {
                type $p = MlKem768;
                $body
            }
            "ML_KEM_1024" => {
                type $p = MlKem1024;
                $body
            }
            other => Err(MlKemError::UnsupportedSpec(other.to_string())),
        }
    };
}

/// Mint a fresh ML-KEM keypair, or `Ok(None)` for a non-ML-KEM spec.
pub fn generate_keypair(key_spec: &str) -> Result<Option<KeyPair>, MlKemError> {
    if !is_ml_kem_spec(key_spec) {
        return Ok(None);
    }
    with_params!(key_spec, P => {
        let (dk, ek) = P::generate_keypair();
        let priv_der = dk
            .to_pkcs8_der()
            .map_err(|e| MlKemError::CryptoFailure(format!("pkcs8 encode: {e}")))?
            .as_bytes()
            .to_vec();
        let pub_der = ek
            .to_public_key_der()
            .map_err(|e| MlKemError::CryptoFailure(format!("spki encode: {e}")))?
            .as_bytes()
            .to_vec();
        Ok(Some((priv_der, pub_der)))
    })
}

/// Encapsulate a fresh 32-byte shared secret under the SPKI public key:
/// `(ciphertext, shared_secret)`.
pub fn encapsulate(key_spec: &str, public_spki_der: &[u8]) -> Result<KeyPair, MlKemError> {
    with_params!(key_spec, P => {
        let ek = <P as Kem>::EncapsulationKey::from_public_key_der(public_spki_der)
            .map_err(|e| MlKemError::CorruptKey(e.to_string()))?;
        let (ct, ss) = ek.encapsulate();
        Ok((ct.to_vec(), ss.to_vec()))
    })
}

/// Recover the shared secret from `ciphertext` with the PKCS#8 private key.
/// A ciphertext of the wrong length for the spec is refused; any other
/// ciphertext decapsulates (to a pseudo-random value when it was not made
/// under this key: FIPS 203 implicit rejection).
pub fn decapsulate(
    key_spec: &str,
    private_pkcs8_der: &[u8],
    ciphertext: &[u8],
) -> Result<Vec<u8>, MlKemError> {
    let expected = ciphertext_len(key_spec)
        .ok_or_else(|| MlKemError::UnsupportedSpec(key_spec.to_string()))?;
    if ciphertext.len() != expected {
        return Err(MlKemError::CiphertextLength {
            spec: key_spec.to_string(),
            expected,
            actual: ciphertext.len(),
        });
    }
    with_params!(key_spec, P => {
        let dk = <P as Kem>::DecapsulationKey::from_pkcs8_der(private_pkcs8_der)
            .map_err(|e| MlKemError::CorruptKey(e.to_string()))?;
        let ss = dk
            .decapsulate_slice(ciphertext)
            .map_err(|e| MlKemError::CryptoFailure(e.to_string()))?;
        Ok(ss.to_vec())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SPECS: [&str; 3] = ["ML_KEM_512", "ML_KEM_768", "ML_KEM_1024"];

    #[test]
    fn encapsulate_decapsulate_round_trip_every_spec() {
        for spec in SPECS {
            let (private, public) = generate_keypair(spec).unwrap().unwrap();
            let (ct, ss) = encapsulate(spec, &public).unwrap();
            assert_eq!(ct.len(), ciphertext_len(spec).unwrap(), "{spec}");
            assert_eq!(ss.len(), 32, "{spec}");
            assert_eq!(decapsulate(spec, &private, &ct).unwrap(), ss, "{spec}");
        }
    }

    #[test]
    fn private_key_is_seed_format_pkcs8_of_constant_length() {
        let lens: Vec<usize> = SPECS
            .iter()
            .map(|spec| generate_keypair(spec).unwrap().unwrap().0.len())
            .collect();
        // SEQUENCE { INTEGER 0, SEQUENCE { OID }, OCTET STRING { [0] seed } }.
        assert!(lens.iter().all(|l| *l == lens[0]), "{lens:?}");
        assert_eq!(lens[0], 86);
    }

    #[test]
    fn public_key_is_spki_with_the_fips203_oid() {
        // id-alg-ml-kem-768: 2.16.840.1.101.3.4.4.2.
        let oid = [
            0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x04, 0x02,
        ];
        let (_, public) = generate_keypair("ML_KEM_768").unwrap().unwrap();
        assert!(public.windows(oid.len()).any(|w| w == oid));
        // 1184-byte encapsulation key plus the SPKI framing.
        assert!(public.len() > 1184);
    }

    #[test]
    fn wrong_length_ciphertext_is_refused() {
        let (private, public) = generate_keypair("ML_KEM_512").unwrap().unwrap();
        let (ct, _) = encapsulate("ML_KEM_512", &public).unwrap();
        assert!(matches!(
            decapsulate("ML_KEM_512", &private, &ct[..ct.len() - 1]),
            Err(MlKemError::CiphertextLength { .. })
        ));
    }

    #[test]
    fn foreign_ciphertext_implicitly_rejects_to_a_different_secret() {
        let (_, public_a) = generate_keypair("ML_KEM_768").unwrap().unwrap();
        let (private_b, _) = generate_keypair("ML_KEM_768").unwrap().unwrap();
        let (ct, ss) = encapsulate("ML_KEM_768", &public_a).unwrap();
        let other = decapsulate("ML_KEM_768", &private_b, &ct).unwrap();
        assert_eq!(other.len(), 32);
        assert_ne!(other, ss);
    }

    #[test]
    fn non_ml_kem_spec_generates_nothing() {
        assert!(generate_keypair("RSA_2048").unwrap().is_none());
    }
}
