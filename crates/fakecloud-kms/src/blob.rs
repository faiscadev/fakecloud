//! AWS-shaped KMS ciphertext blobs.
//!
//! Real AWS KMS ciphertext is opaque binary that round-trips through
//! `Encrypt` and `Decrypt`. Anyone inspecting the bytes sees a binary
//! header followed by AES-GCM ciphertext, *not* their plaintext. The
//! original fakecloud envelope (`fakecloud-kms:<key>:<base64-plaintext>`)
//! round-tripped correctly through real SDKs but leaked plaintext to
//! anyone who base64-decoded the blob.
//!
//! This module produces and consumes a binary envelope shaped like
//! AWS's, using a per-process master key:
//!
//! ```text
//! ┌──────────┬────────────┬──────────────┬────┬─────────────┬───────────────┐
//! │ version  │ key-id len │ key-id bytes │ IV │ ciphertext  │ AES-GCM tag   │
//! │ 4 bytes  │ 8 bytes BE │ N bytes UTF8 │ 12 │ M bytes     │ 16 bytes      │
//! └──────────┴────────────┴──────────────┴────┴─────────────┴───────────────┘
//! ```
//!
//! The version header (`0x01 0x02 0x02 0x00`) lets us distinguish
//! fakecloud blobs from random base64 noise. The key-id is included so
//! `Decrypt` can echo back the source `KeyId` without the caller having
//! to specify it, matching AWS behavior.

use aes_gcm::aead::generic_array::GenericArray;
use aes_gcm::aead::{Aead, KeyInit, OsRng, Payload};
use aes_gcm::{AeadCore, Aes256Gcm, Key};

const VERSION_HEADER: [u8; 4] = [0x01, 0x02, 0x02, 0x00];

fn cipher_for(master_key_bytes: &[u8]) -> Option<Aes256Gcm> {
    if master_key_bytes.len() != 32 {
        return None;
    }
    let key = Key::<Aes256Gcm>::from_slice(master_key_bytes);
    Some(Aes256Gcm::new(key))
}

/// Encode a plaintext byte slice into the AWS-shaped blob format. Uses
/// AES-256-GCM with the supplied 32-byte master key and a fresh random
/// IV. Output bytes are opaque — callers should base64 them before
/// placing in JSON responses. Panics on a master key that isn't 32
/// bytes; callers control the key, so this is a programming error.
pub fn encode(master_key_bytes: &[u8], key_id: &str, plaintext: &[u8]) -> Vec<u8> {
    encode_with_context(master_key_bytes, key_id, plaintext, &[])
}

/// Variant of [`encode`] that mixes additional bytes into the AEAD AAD.
/// Used by `Encrypt` / `GenerateDataKey` / `ReEncrypt` to bind the
/// caller's `EncryptionContext` into the ciphertext: a different EC
/// supplied at decrypt time fails AES-GCM verification, matching real
/// KMS behavior. When `extra_aad` is empty the AAD is just `key_id`,
/// so blobs produced by callers that don't pass EC stay byte-compatible
/// with the original `encode` output and pre-existing persisted blobs.
pub fn encode_with_context(
    master_key_bytes: &[u8],
    key_id: &str,
    plaintext: &[u8],
    extra_aad: &[u8],
) -> Vec<u8> {
    let cipher =
        cipher_for(master_key_bytes).expect("KMS master key must be 32 bytes for AES-256-GCM");
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

    // AES-GCM in this crate produces ciphertext || tag concatenated; we'll
    // split tag off the back to keep the wire format aligned with the AWS
    // shape (ciphertext segment followed by separate tag segment).
    // Bind the key-id and supplied EC into the AAD so any tampering
    // with the header bytes (e.g. flipping a character of the embedded
    // key-id) or supplying a different EC at decrypt time fails AEAD
    // verification.
    let mut aad = Vec::with_capacity(key_id.len() + extra_aad.len());
    aad.extend_from_slice(key_id.as_bytes());
    aad.extend_from_slice(extra_aad);
    let combined = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .expect("AES-GCM encrypt with 96-bit nonce never fails on valid key");
    debug_assert!(combined.len() >= 16, "AES-GCM output includes 16-byte tag");
    let tag_split = combined.len() - 16;
    let ciphertext = &combined[..tag_split];
    let tag = &combined[tag_split..];

    let key_bytes = key_id.as_bytes();
    let mut out = Vec::with_capacity(
        VERSION_HEADER.len() + 8 + key_bytes.len() + 12 + 4 + ciphertext.len() + 16,
    );
    out.extend_from_slice(&VERSION_HEADER);
    out.extend_from_slice(&(key_bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(key_bytes);
    out.extend_from_slice(nonce.as_slice());
    out.extend_from_slice(&(ciphertext.len() as u32).to_be_bytes());
    out.extend_from_slice(ciphertext);
    out.extend_from_slice(tag);
    out
}

/// A decoded blob from [`decode`]. Carries the embedded key-id and the
/// recovered plaintext.
pub struct Decoded {
    pub key_id: String,
    pub plaintext: Vec<u8>,
}

/// Decode an AWS-shaped fakecloud KMS blob back to its plaintext.
/// Returns `None` if the bytes don't carry the version header or fail
/// any structural check (including AEAD verification under the supplied
/// master key); callers fall back to legacy envelope formats in that
/// case.
pub fn decode(master_key_bytes: &[u8], blob: &[u8]) -> Option<Decoded> {
    decode_with_context(master_key_bytes, blob, &[])
}

/// Variant of [`decode`] that mixes additional bytes into the AEAD AAD
/// during verification. Returns `None` when the supplied `extra_aad`
/// doesn't match what was passed to [`encode_with_context`] at encrypt
/// time — the caller surfaces this as `InvalidCiphertextException`,
/// matching the real KMS error for an `EncryptionContext` mismatch.
pub fn decode_with_context(
    master_key_bytes: &[u8],
    blob: &[u8],
    extra_aad: &[u8],
) -> Option<Decoded> {
    if blob.len() < VERSION_HEADER.len() + 8 + 12 + 4 + 16 {
        return None;
    }
    if blob[..VERSION_HEADER.len()] != VERSION_HEADER {
        return None;
    }
    let mut cursor = VERSION_HEADER.len();

    let key_len = u64::from_be_bytes(blob[cursor..cursor + 8].try_into().ok()?) as usize;
    cursor += 8;
    // `key_len` is fully attacker-controlled (it comes straight from the
    // base64 `CiphertextBlob` a client hands to `Decrypt`/`ReEncrypt`).
    // Reject any length that alone exceeds the bytes still remaining
    // BEFORE it enters an addition: the release profile has no
    // `overflow-checks`, so a hostile `key_len` near `usize::MAX` would
    // otherwise wrap the `cursor + key_len + ...` sum to a tiny value,
    // pass the guard, and panic on the slice below. Bounding it first
    // makes every downstream index self-evidently in range.
    let remaining = blob.len().saturating_sub(cursor);
    if key_len > remaining {
        return None;
    }
    // Trailer after the key-id: IV (12) + ct_len (4) + tag (16). This
    // sum cannot wrap now that `key_len <= remaining <= blob.len()`.
    if cursor + key_len + 12 + 4 + 16 > blob.len() {
        return None;
    }
    let key_id = std::str::from_utf8(&blob[cursor..cursor + key_len])
        .ok()?
        .to_string();
    cursor += key_len;

    let nonce = GenericArray::from_slice(&blob[cursor..cursor + 12]);
    cursor += 12;

    let ct_len = u32::from_be_bytes(blob[cursor..cursor + 4].try_into().ok()?) as usize;
    cursor += 4;
    // On 64-bit `usize` this sum can't wrap (`cursor` is small and
    // `ct_len <= u32::MAX`), but use `checked_add` so the bound is
    // provably panic-free regardless of `usize` width, then require an
    // exact fit: the ciphertext-plus-tag must consume the rest of the
    // blob with nothing left over.
    let body_end = cursor.checked_add(ct_len)?.checked_add(16)?;
    if body_end != blob.len() {
        return None;
    }
    let ct_with_tag = &blob[cursor..body_end];

    let cipher = cipher_for(master_key_bytes)?;
    let mut aad = Vec::with_capacity(key_id.len() + extra_aad.len());
    aad.extend_from_slice(key_id.as_bytes());
    aad.extend_from_slice(extra_aad);
    let plaintext = cipher
        .decrypt(
            nonce,
            Payload {
                msg: ct_with_tag,
                aad: &aad,
            },
        )
        .ok()?;

    Some(Decoded { key_id, plaintext })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_master() -> Vec<u8> {
        // Deterministic 32-byte key so unit tests are reproducible.
        (0u8..32).collect()
    }

    #[test]
    fn round_trip_recovers_plaintext_and_key_id() {
        let plaintext = b"super-secret-value";
        let mk = fixed_master();
        let blob = encode(&mk, "alias/my-key", plaintext);
        let decoded = decode(&mk, &blob).unwrap();
        assert_eq!(decoded.plaintext, plaintext);
        assert_eq!(decoded.key_id, "alias/my-key");
    }

    #[test]
    fn blob_does_not_leak_plaintext() {
        let plaintext = b"NOT_TO_BE_FOUND_IN_BYTES";
        let blob = encode(&fixed_master(), "key-1", plaintext);
        assert!(blob.windows(plaintext.len()).all(|w| w != plaintext));
    }

    #[test]
    fn decode_rejects_random_bytes() {
        let mk = fixed_master();
        assert!(decode(&mk, b"this-is-not-a-blob").is_none());
        assert!(decode(&mk, &[0u8; 8]).is_none());
    }

    #[test]
    fn decode_rejects_wrong_header() {
        let mk = fixed_master();
        let mut blob = encode(&mk, "k", b"data");
        blob[0] = 0xFF;
        assert!(decode(&mk, &blob).is_none());
    }

    #[test]
    fn decode_rejects_tampered_ciphertext() {
        let mk = fixed_master();
        let mut blob = encode(&mk, "k", b"data");
        let last = blob.len() - 1;
        blob[last] ^= 0x01;
        assert!(decode(&mk, &blob).is_none());
    }

    #[test]
    fn decode_rejects_wrong_master_key() {
        let blob = encode(&fixed_master(), "k", b"data");
        let other_key: Vec<u8> = (32u8..64).collect();
        assert!(decode(&other_key, &blob).is_none());
    }

    #[test]
    fn decode_rejects_tampered_key_id_header() {
        let mk = fixed_master();
        let mut blob = encode(&mk, "alias/original-key", b"data");
        // The version header is 4 bytes, key-id length is the next 8.
        // The first byte of the key-id sits at offset 12. Flip one
        // character so the AAD no longer matches the bytes used at
        // encrypt time and AES-GCM rejects the ciphertext.
        let key_id_offset = 4 + 8;
        blob[key_id_offset] ^= 0x01;
        assert!(decode(&mk, &blob).is_none());
    }

    #[test]
    fn distinct_calls_produce_distinct_blobs() {
        let mk = fixed_master();
        let a = encode(&mk, "k", b"same");
        let b = encode(&mk, "k", b"same");
        assert_ne!(a, b, "fresh IV should make ciphertext non-deterministic");
    }

    #[test]
    fn decode_with_context_round_trips_when_ec_matches() {
        let mk = fixed_master();
        let aad = b"{\"app\":\"prod\"}";
        let blob = encode_with_context(&mk, "k", b"secret", aad);
        let decoded = decode_with_context(&mk, &blob, aad).expect("matching EC must decode");
        assert_eq!(decoded.plaintext, b"secret");
    }

    #[test]
    fn decode_with_context_rejects_mismatched_ec() {
        let mk = fixed_master();
        let blob = encode_with_context(&mk, "k", b"secret", b"{\"app\":\"prod\"}");
        // Different EC AAD bytes — AEAD verification must fail.
        assert!(decode_with_context(&mk, &blob, b"{\"app\":\"staging\"}").is_none());
        // Missing EC at decrypt time — same outcome.
        assert!(decode_with_context(&mk, &blob, b"").is_none());
    }

    #[test]
    fn decode_rejects_hostile_max_key_len_without_panicking() {
        // Regression: a `CiphertextBlob` whose key-id length field is
        // `u64::MAX` must be rejected as `None`, never panic. Before the
        // fix the `cursor + key_len + ...` guard wrapped `usize` (release
        // builds have no overflow-checks), passed, then the key-id slice
        // panicked with "slice index starts at 12 but ends at 11".
        let mk = fixed_master();
        let mut hostile = Vec::new();
        hostile.extend_from_slice(&VERSION_HEADER); // 4
        hostile.extend_from_slice(&[0xFF; 8]); // key_len = u64::MAX
        hostile.extend_from_slice(&[0u8; 32]); // padding to clear min-length gate
        assert_eq!(hostile.len(), 44);
        assert!(decode_with_context(&mk, &hostile, b"").is_none());
    }

    #[test]
    fn decode_rejects_large_key_len_without_panicking() {
        // A large-but-not-max `key_len` that still overruns the blob (and
        // whose `+ 12 + 4 + 16` would overrun) must also cleanly reject.
        let mk = fixed_master();
        let mut hostile = Vec::new();
        hostile.extend_from_slice(&VERSION_HEADER);
        hostile.extend_from_slice(&0x0000_0000_1000_0000u64.to_be_bytes());
        hostile.extend_from_slice(&[0u8; 32]);
        assert!(decode_with_context(&mk, &hostile, b"").is_none());
    }

    #[test]
    fn decode_with_context_round_trips_key_id_and_plaintext() {
        // Happy path stays intact after the bounds hardening.
        let mk = fixed_master();
        let aad = b"ctx-bytes";
        let blob = encode_with_context(&mk, "alias/rt-key", b"payload", aad);
        let decoded = decode_with_context(&mk, &blob, aad).expect("well-formed blob must decode");
        assert_eq!(decoded.key_id, "alias/rt-key");
        assert_eq!(decoded.plaintext, b"payload");
    }

    #[test]
    fn decode_without_context_rejects_blob_encoded_with_ec() {
        let mk = fixed_master();
        let blob = encode_with_context(&mk, "k", b"secret", b"{\"x\":\"y\"}");
        // Plain `decode` passes empty AAD; EC-bound blob must reject.
        assert!(decode(&mk, &blob).is_none());
    }
}
