//! Real HMAC compute + verify for KMS `GenerateMac` / `VerifyMac`.
//!
//! Replaces the prior fake-bytes shim that just stuffed a stringified
//! `(key_id, alg, message)` triple into the response. Now backs the
//! response with a real HMAC computed over the message via the key's
//! stored `master_key_bytes`. Verify uses the underlying crate's
//! constant-time comparison.

use hmac::{Hmac, Mac};
use sha2::{Sha224, Sha256, Sha384, Sha512};

#[derive(Debug, thiserror::Error)]
pub enum MacError {
    #[error("unsupported MAC algorithm: {0}")]
    UnsupportedAlgorithm(String),
}

/// Compute the MAC of `message` keyed by `key_bytes`. Algorithm names
/// match KMS's `MacAlgorithmSpec` enum (`HMAC_SHA_224` / `_256` /
/// `_384` / `_512`). All four are supported — an HMAC_224 key advertises
/// `HMAC_SHA_224`, so rejecting it here made GenerateMac/VerifyMac
/// unusable for that key spec.
pub fn compute(algorithm: &str, key_bytes: &[u8], message: &[u8]) -> Result<Vec<u8>, MacError> {
    match algorithm {
        "HMAC_SHA_224" => {
            let mut mac = <Hmac<Sha224> as Mac>::new_from_slice(key_bytes)
                .expect("HMAC accepts any key length");
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        "HMAC_SHA_256" => {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key_bytes)
                .expect("HMAC accepts any key length");
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        "HMAC_SHA_384" => {
            let mut mac = <Hmac<Sha384> as Mac>::new_from_slice(key_bytes)
                .expect("HMAC accepts any key length");
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        "HMAC_SHA_512" => {
            let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key_bytes)
                .expect("HMAC accepts any key length");
            mac.update(message);
            Ok(mac.finalize().into_bytes().to_vec())
        }
        other => Err(MacError::UnsupportedAlgorithm(other.to_string())),
    }
}

/// Verify `mac_bytes` matches the expected MAC of `message` under
/// `key_bytes`. Uses each crate's `verify_slice` for constant-time
/// comparison so timing leaks don't help an attacker forge MACs.
pub fn verify(
    algorithm: &str,
    key_bytes: &[u8],
    message: &[u8],
    mac_bytes: &[u8],
) -> Result<bool, MacError> {
    match algorithm {
        "HMAC_SHA_224" => {
            let mut mac = <Hmac<Sha224> as Mac>::new_from_slice(key_bytes)
                .expect("HMAC accepts any key length");
            mac.update(message);
            Ok(mac.verify_slice(mac_bytes).is_ok())
        }
        "HMAC_SHA_256" => {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key_bytes)
                .expect("HMAC accepts any key length");
            mac.update(message);
            Ok(mac.verify_slice(mac_bytes).is_ok())
        }
        "HMAC_SHA_384" => {
            let mut mac = <Hmac<Sha384> as Mac>::new_from_slice(key_bytes)
                .expect("HMAC accepts any key length");
            mac.update(message);
            Ok(mac.verify_slice(mac_bytes).is_ok())
        }
        "HMAC_SHA_512" => {
            let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key_bytes)
                .expect("HMAC accepts any key length");
            mac.update(message);
            Ok(mac.verify_slice(mac_bytes).is_ok())
        }
        other => Err(MacError::UnsupportedAlgorithm(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_sha224() {
        // HMAC_224 keys advertise HMAC_SHA_224; the round trip must work
        // and produce a 28-byte (224-bit) tag.
        let key = b"sample-key-material";
        let msg = b"the quick brown fox";
        let mac = compute("HMAC_SHA_224", key, msg).unwrap();
        assert_eq!(mac.len(), 28);
        assert!(verify("HMAC_SHA_224", key, msg, &mac).unwrap());
        let mut tampered = mac.clone();
        tampered[0] ^= 0xff;
        assert!(!verify("HMAC_SHA_224", key, msg, &tampered).unwrap());
    }

    #[test]
    fn round_trip_sha256() {
        let key = b"sample-key-material";
        let msg = b"the quick brown fox";
        let mac = compute("HMAC_SHA_256", key, msg).unwrap();
        assert!(verify("HMAC_SHA_256", key, msg, &mac).unwrap());
    }

    #[test]
    fn round_trip_sha384() {
        let key = b"sample-key-material";
        let msg = b"the quick brown fox";
        let mac = compute("HMAC_SHA_384", key, msg).unwrap();
        assert!(verify("HMAC_SHA_384", key, msg, &mac).unwrap());
    }

    #[test]
    fn round_trip_sha512() {
        let key = b"sample-key-material";
        let msg = b"the quick brown fox";
        let mac = compute("HMAC_SHA_512", key, msg).unwrap();
        assert!(verify("HMAC_SHA_512", key, msg, &mac).unwrap());
    }

    #[test]
    fn verify_fails_on_tampered_mac() {
        let key = b"sample-key-material";
        let msg = b"original";
        let mac = compute("HMAC_SHA_256", key, msg).unwrap();
        let mut tampered = mac.clone();
        tampered[0] ^= 0xff;
        assert!(!verify("HMAC_SHA_256", key, msg, &tampered).unwrap());
    }

    #[test]
    fn verify_fails_on_wrong_key() {
        let mac = compute("HMAC_SHA_256", b"k1", b"m").unwrap();
        assert!(!verify("HMAC_SHA_256", b"k2", b"m", &mac).unwrap());
    }

    #[test]
    fn unsupported_algorithm_errors() {
        assert!(compute("HMAC_SHA_999", b"k", b"m").is_err());
    }

    #[test]
    fn mac_lengths_match_sha_output() {
        assert_eq!(compute("HMAC_SHA_256", b"k", b"m").unwrap().len(), 32);
        assert_eq!(compute("HMAC_SHA_384", b"k", b"m").unwrap().len(), 48);
        assert_eq!(compute("HMAC_SHA_512", b"k", b"m").unwrap().len(), 64);
    }
}
