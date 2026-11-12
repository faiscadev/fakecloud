//! SSE-SQS: server-side encryption with SQS-owned keys
//! (`SqsManagedSseEnabled=true`, the default for new queues).
//!
//! SQS encrypts the message body at rest with an encryption key SQS owns and
//! manages itself: no customer KMS key is involved, nothing is recorded as a
//! KMS call, and no `aws/sqs` key is created. The body is sealed with an
//! AES-256-GCM key owned by fakecloud's SQS, bound to the queue that sealed
//! it, in an envelope of its own (distinct from the KMS ciphertext format),
//! and opened on every read path that delivers it.

use aes_gcm::aead::{Aead, KeyInit, OsRng, Payload};
use aes_gcm::{AeadCore, Aes256Gcm, Key, Nonce};
use base64::Engine as _;
use sha2::Digest as _;

/// The leading bytes of an SSE-SQS envelope.
const MAGIC: &[u8; 8] = b"FCSQSSE1";
const NONCE_LEN: usize = 12;
const TAG_LEN: usize = 16;

/// The SQS-owned key: fixed, so sealed bodies survive a restart from a
/// snapshot without any key material of their own to persist.
fn cipher() -> Aes256Gcm {
    let key = sha2::Sha256::digest(b"fakecloud SQS-owned SSE key v1");
    Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&key))
}

/// Whether a queue with `attributes` encrypts with SQS-owned keys (SSE-SQS):
/// `SqsManagedSseEnabled` is `true` and no `KmsMasterKeyId` is set (which
/// would make it SSE-KMS instead).
pub fn enabled(attributes: &std::collections::BTreeMap<String, String>) -> bool {
    attributes
        .get("KmsMasterKeyId")
        .is_none_or(|k| k.is_empty())
        && attributes.get("SqsManagedSseEnabled").map(String::as_str) == Some("true")
}

/// `plaintext` sealed by the queue `queue_arn`, as stored at rest (base64).
pub fn seal(queue_arn: &str, plaintext: &str) -> String {
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
    let sealed = cipher()
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext.as_bytes(),
                aad: queue_arn.as_bytes(),
            },
        )
        .expect("AES-GCM encryption with a 96-bit nonce does not fail");
    let mut out = Vec::with_capacity(MAGIC.len() + NONCE_LEN + sealed.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(nonce.as_slice());
    out.extend_from_slice(&sealed);
    base64::engine::general_purpose::STANDARD.encode(out)
}

/// Whether `stored` is an SSE-SQS envelope.
pub fn is_envelope(stored: &str) -> bool {
    base64::engine::general_purpose::STANDARD
        .decode(stored)
        .is_ok_and(|bytes| {
            bytes.len() >= MAGIC.len() + NONCE_LEN + TAG_LEN && bytes.starts_with(MAGIC)
        })
}

/// The plaintext of an SSE-SQS envelope sealed by the queue `queue_arn`;
/// `None` when `stored` is not one, or was sealed by another queue.
pub fn open(queue_arn: &str, stored: &str) -> Option<String> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(stored)
        .ok()?;
    if bytes.len() < MAGIC.len() + NONCE_LEN + TAG_LEN || !bytes.starts_with(MAGIC) {
        return None;
    }
    let (nonce, sealed) = bytes[MAGIC.len()..].split_at(NONCE_LEN);
    let plaintext = cipher()
        .decrypt(
            Nonce::from_slice(nonce),
            Payload {
                msg: sealed,
                aad: queue_arn.as_bytes(),
            },
        )
        .ok()?;
    String::from_utf8(plaintext).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const Q: &str = "arn:aws:sqs:us-east-1:123456789012:q";

    #[test]
    fn sealed_bodies_are_opaque_and_open_only_for_their_queue() {
        let stored = seal(Q, "hello");
        assert_ne!(stored, "hello");
        assert!(!stored.contains("hello"));
        assert!(is_envelope(&stored));
        assert_eq!(open(Q, &stored).as_deref(), Some("hello"));
        assert_eq!(
            open("arn:aws:sqs:us-east-1:123456789012:other", &stored),
            None
        );
        // Each seal is fresh.
        assert_ne!(seal(Q, "hello"), stored);
        // Plain bodies and KMS ciphertexts are not SSE-SQS envelopes.
        assert!(!is_envelope("hello"));
        assert!(!is_envelope(
            &base64::engine::general_purpose::STANDARD.encode([1u8, 2, 2, 0, 0, 0, 0, 0])
        ));
        assert_eq!(open(Q, "hello"), None);
    }

    #[test]
    fn sse_sqs_is_managed_sse_without_a_kms_key() {
        let attrs = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert!(enabled(&attrs(&[("SqsManagedSseEnabled", "true")])));
        assert!(!enabled(&attrs(&[("SqsManagedSseEnabled", "false")])));
        assert!(!enabled(&attrs(&[])));
        assert!(!enabled(&attrs(&[
            ("SqsManagedSseEnabled", "true"),
            ("KmsMasterKeyId", "alias/aws/sqs"),
        ])));
    }
}
