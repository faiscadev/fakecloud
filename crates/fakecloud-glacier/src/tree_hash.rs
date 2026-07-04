//! The Amazon S3 Glacier SHA-256 tree hash.
//!
//! Glacier does not hash an archive as one flat SHA-256. It splits the payload
//! into 1 MiB chunks, takes the SHA-256 of each chunk, then builds a binary
//! hash tree: adjacent hashes are concatenated and re-hashed level by level
//! until a single root hash remains. For payloads of 1 MiB or less the tree
//! collapses to a plain SHA-256, which is why small test archives match a
//! naive SHA-256 of their bytes. The client sends this value in the
//! `x-amz-sha256-tree-hash` header and the service echoes it back; computing it
//! for real lets us validate the header instead of trusting it.

use sha2::{Digest, Sha256};

const ONE_MIB: usize = 1024 * 1024;

/// Compute the Glacier SHA-256 tree hash of `data`, returned as lowercase hex.
pub fn tree_hash_hex(data: &[u8]) -> String {
    let root = tree_hash_bytes(data);
    hex_encode(&root)
}

/// Compute the raw 32-byte tree-hash root of `data`.
pub fn tree_hash_bytes(data: &[u8]) -> [u8; 32] {
    if data.is_empty() {
        // SHA-256 of the empty input; Glacier treats a zero-length payload as a
        // single empty chunk.
        return sha256(&[]);
    }
    // Level 0: SHA-256 of each 1 MiB chunk.
    let mut level: Vec<[u8; 32]> = data.chunks(ONE_MIB).map(sha256).collect();
    // Reduce pairwise until one hash remains.
    while level.len() > 1 {
        let mut next: Vec<[u8; 32]> = Vec::with_capacity(level.len().div_ceil(2));
        let mut i = 0;
        while i < level.len() {
            if i + 1 < level.len() {
                let mut buf = [0u8; 64];
                buf[..32].copy_from_slice(&level[i]);
                buf[32..].copy_from_slice(&level[i + 1]);
                next.push(sha256(&buf));
            } else {
                // Odd hash out is promoted unchanged to the next level.
                next.push(level[i]);
            }
            i += 2;
        }
        level = next;
    }
    level[0]
}

/// Combine a list of already-computed per-part tree hashes (raw 32-byte roots,
/// one per 1 MiB-aligned part) into the overall archive tree hash. Used by
/// `CompleteMultipartUpload`, where each part already carries its own tree
/// hash. Parts must be supplied in ascending range order and each (except the
/// last) must be a whole number of 1 MiB blocks for the combined hash to equal
/// a from-scratch hash of the assembled archive.
pub fn combine_tree_hashes(parts: &[[u8; 32]]) -> [u8; 32] {
    if parts.is_empty() {
        return sha256(&[]);
    }
    let mut level: Vec<[u8; 32]> = parts.to_vec();
    while level.len() > 1 {
        let mut next: Vec<[u8; 32]> = Vec::with_capacity(level.len().div_ceil(2));
        let mut i = 0;
        while i < level.len() {
            if i + 1 < level.len() {
                let mut buf = [0u8; 64];
                buf[..32].copy_from_slice(&level[i]);
                buf[32..].copy_from_slice(&level[i + 1]);
                next.push(sha256(&buf));
            } else {
                next.push(level[i]);
            }
            i += 2;
        }
        level = next;
    }
    level[0]
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Decode a lowercase/uppercase hex string into raw bytes; `None` on malformed
/// input.
pub fn hex_decode(s: &str) -> Option<[u8; 32]> {
    if s.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    let bytes = s.as_bytes();
    for i in 0..32 {
        let hi = hex_val(bytes[2 * i])?;
        let lo = hex_val(bytes[2 * i + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_payload_matches_plain_sha256() {
        let data = b"hello glacier";
        let mut hasher = Sha256::new();
        hasher.update(data);
        let expected: [u8; 32] = hasher.finalize().into();
        assert_eq!(tree_hash_hex(data), hex_encode(&expected));
    }

    #[test]
    fn empty_payload_is_sha256_of_empty() {
        // Known SHA-256 of the empty string.
        assert_eq!(
            tree_hash_hex(&[]),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn multi_chunk_reduces_to_root() {
        // 2 MiB + a tail: three level-0 chunks reduce to one root.
        let data = vec![0x42u8; 2 * ONE_MIB + 123];
        let h = tree_hash_hex(&data);
        assert_eq!(h.len(), 64);
        // Combining the three per-chunk hashes must give the same root.
        let chunks: Vec<[u8; 32]> = data.chunks(ONE_MIB).map(sha256).collect();
        assert_eq!(hex_encode(&combine_tree_hashes(&chunks)), h);
    }

    #[test]
    fn hex_round_trips() {
        let data = b"round trip";
        let h = tree_hash_hex(data);
        let raw = hex_decode(&h).unwrap();
        assert_eq!(hex_encode(&raw), h);
    }
}
