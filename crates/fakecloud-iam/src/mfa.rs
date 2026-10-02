//! Virtual MFA: seed generation and RFC 6238 TOTP verification.
//!
//! `CreateVirtualMFADevice` hands out a base32 seed (RFC 4648) that an
//! authenticator app turns into 6-digit, 30-second TOTP codes (HMAC-SHA1).
//! STS checks the `TokenCode` a caller presents with `SerialNumber` against
//! that seed, so `aws:MultiFactorAuthPresent` is only ever set for a caller
//! that actually holds the device.

use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha1::Sha1;

/// Seed length AWS uses for virtual MFA devices: 40 bytes, which base32
/// encodes to 64 characters with no padding.
const SEED_BYTES: usize = 40;
const STEP_SECONDS: i64 = 30;
/// Steps of clock drift accepted on either side of the current one.
const DRIFT_STEPS: i64 = 1;
const ALPHABET: &[u8; 32] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// A fresh random seed, base32 encoded.
pub fn generate_seed() -> String {
    let mut bytes = [0u8; SEED_BYTES];
    rand::thread_rng().fill_bytes(&mut bytes);
    base32_encode(&bytes)
}

/// RFC 4648 base32 encoding without padding.
pub fn base32_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(5) * 8);
    let mut buffer: u64 = 0;
    let mut bits = 0u32;
    for &b in data {
        buffer = (buffer << 8) | u64::from(b);
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(ALPHABET[((buffer >> bits) & 0x1f) as usize] as char);
        }
    }
    if bits > 0 {
        out.push(ALPHABET[((buffer << (5 - bits)) & 0x1f) as usize] as char);
    }
    out
}

/// RFC 4648 base32 decoding. Case-insensitive, ignores `=` padding and
/// spaces; `None` on any other character.
pub fn base32_decode(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 5 / 8);
    let mut buffer: u64 = 0;
    let mut bits = 0u32;
    for c in s.chars().filter(|c| *c != '=' && *c != ' ') {
        let upper = c.to_ascii_uppercase() as u8;
        let v = ALPHABET.iter().position(|&a| a == upper)? as u64;
        buffer = (buffer << 5) | v;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push(((buffer >> bits) & 0xff) as u8);
        }
    }
    (!out.is_empty()).then_some(out)
}

/// The 6-digit TOTP code for `secret` at time step `counter`.
pub fn totp_at_step(secret: &[u8], counter: u64) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(&counter.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    let offset = (digest[digest.len() - 1] & 0x0f) as usize;
    let binary = (u32::from(digest[offset] & 0x7f) << 24)
        | (u32::from(digest[offset + 1]) << 16)
        | (u32::from(digest[offset + 2]) << 8)
        | u32::from(digest[offset + 3]);
    format!("{:06}", binary % 1_000_000)
}

/// The 6-digit TOTP code for a base32 seed at `now`.
pub fn totp_code(base32_seed: &str, now: DateTime<Utc>) -> Option<String> {
    let secret = base32_decode(base32_seed)?;
    Some(totp_at_step(
        &secret,
        (now.timestamp() / STEP_SECONDS) as u64,
    ))
}

/// Whether `code` is a valid TOTP for `base32_seed` at `now`, allowing one
/// step of clock drift either way. `None` when the seed is not a base32
/// TOTP secret (a hardware device registered by serial only), so the code
/// cannot be checked.
pub fn verify_totp(base32_seed: &str, code: &str, now: DateTime<Utc>) -> Option<bool> {
    let secret = base32_decode(base32_seed)?;
    let step = now.timestamp() / STEP_SECONDS;
    Some((-DRIFT_STEPS..=DRIFT_STEPS).any(|d| {
        let counter = step + d;
        counter >= 0 && totp_at_step(&secret, counter as u64) == code
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn rfc6238_sha1_vectors() {
        // RFC 6238 appendix B, SHA1 secret "12345678901234567890", truncated
        // to 6 digits.
        let secret = b"12345678901234567890";
        assert_eq!(totp_at_step(secret, 59 / 30), "287082");
        assert_eq!(totp_at_step(secret, 1_111_111_109 / 30), "081804");
        assert_eq!(totp_at_step(secret, 1_234_567_890 / 30), "005924");
        assert_eq!(totp_at_step(secret, 2_000_000_000 / 30), "279037");
    }

    #[test]
    fn base32_round_trip() {
        let data = b"12345678901234567890";
        let enc = base32_encode(data);
        assert_eq!(enc, "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ");
        assert_eq!(base32_decode(&enc).unwrap(), data);
        assert_eq!(base32_decode(&enc.to_lowercase()).unwrap(), data);
        assert!(base32_decode("not-base32!").is_none());
    }

    #[test]
    fn generated_seed_is_64_char_base32() {
        let seed = generate_seed();
        assert_eq!(seed.len(), 64);
        assert_eq!(base32_decode(&seed).unwrap().len(), SEED_BYTES);
    }

    #[test]
    fn verify_accepts_current_and_adjacent_steps_only() {
        let seed = base32_encode(b"12345678901234567890");
        let now = Utc.timestamp_opt(1_234_567_890, 0).unwrap();
        let code = totp_code(&seed, now).unwrap();
        assert_eq!(verify_totp(&seed, &code, now), Some(true));
        let next = now + chrono::Duration::seconds(30);
        assert_eq!(verify_totp(&seed, &code, next), Some(true));
        let far = now + chrono::Duration::seconds(120);
        assert_eq!(verify_totp(&seed, &code, far), Some(false));
        assert_eq!(verify_totp(&seed, "000000", now), Some(false));
        assert_eq!(verify_totp("", "123456", now), None);
    }
}
