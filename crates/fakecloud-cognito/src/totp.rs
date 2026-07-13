//! RFC 6238 TOTP (SHA1, 30s step, 6 digits) used by the software-token MFA
//! flow. `AssociateSoftwareToken` hands the client a base32 `SecretCode`; the
//! authenticator app derives a 6-digit code from it, and both
//! `VerifySoftwareToken` (enrollment) and the `SOFTWARE_TOKEN_MFA` sign-in
//! challenge validate the supplied code against the same routine here — with a
//! +/-1 time-step window to tolerate clock skew, exactly like real Cognito.

use hmac::{Hmac, Mac};
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

/// Seconds per TOTP time step (RFC 6238 default, and what Cognito uses).
const TIME_STEP_SECS: u64 = 30;

/// Decode an RFC 4648 base32 string (the alphabet Cognito emits for
/// `SecretCode`). Case-insensitive, ignores trailing `=` padding. Returns
/// `None` on any character outside the alphabet.
fn base32_decode(s: &str) -> Option<Vec<u8>> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut buffer: u64 = 0;
    let mut bits: u32 = 0;
    let mut out = Vec::new();
    for c in s.trim_end_matches('=').chars() {
        if c.is_whitespace() {
            continue;
        }
        let up = c.to_ascii_uppercase() as u8;
        let idx = ALPHABET.iter().position(|&a| a == up)?;
        buffer = (buffer << 5) | idx as u64;
        bits += 5;
        if bits >= 8 {
            bits -= 8;
            out.push((buffer >> bits) as u8);
        }
    }
    Some(out)
}

/// RFC 4226 HOTP: dynamic-truncation of `HMAC-SHA1(key, counter)` reduced to a
/// 6-digit value.
fn hotp(key: &[u8], counter: u64) -> u32 {
    let mut mac = HmacSha1::new_from_slice(key).expect("HMAC accepts keys of any size");
    mac.update(&counter.to_be_bytes());
    let digest = mac.finalize().into_bytes();
    // Dynamic truncation (RFC 4226 §5.3). SHA1 digest is 20 bytes, so the
    // low nibble of the last byte is always a valid offset (0..=15).
    let offset = (digest[19] & 0x0f) as usize;
    let bin = ((digest[offset] as u32 & 0x7f) << 24)
        | ((digest[offset + 1] as u32) << 16)
        | ((digest[offset + 2] as u32) << 8)
        | (digest[offset + 3] as u32);
    bin % 1_000_000
}

/// Compute the 6-digit TOTP for a base32 secret at a specific Unix time.
/// `None` when the secret is not decodable base32 or is empty.
pub fn compute_totp_at(secret_base32: &str, unix_secs: u64) -> Option<String> {
    let key = base32_decode(secret_base32)?;
    if key.is_empty() {
        return None;
    }
    let counter = unix_secs / TIME_STEP_SECS;
    Some(format!("{:06}", hotp(&key, counter)))
}

/// Compute the current 6-digit TOTP for a base32 secret. `None` on a bad
/// secret. Used by tests (and any client) to derive the expected code.
pub fn compute_totp_now(secret_base32: &str) -> Option<String> {
    let now = chrono::Utc::now().timestamp();
    if now < 0 {
        return None;
    }
    compute_totp_at(secret_base32, now as u64)
}

/// Validate a user-supplied 6-digit code against the base32 secret, accepting
/// the current step plus one step either side (+/-30s) to tolerate clock skew.
/// The comparison is constant-time so a wrong code leaks no timing signal.
pub fn verify_totp(secret_base32: &str, code: &str) -> bool {
    let now = chrono::Utc::now().timestamp();
    if now < 0 {
        return false;
    }
    let Some(key) = base32_decode(secret_base32) else {
        return false;
    };
    if key.is_empty() {
        return false;
    }
    let counter = (now as u64) / TIME_STEP_SECS;
    for delta in [-1i64, 0, 1] {
        let c = counter.wrapping_add(delta as u64);
        let expected = format!("{:06}", hotp(&key, c));
        if crate::srp::ct_eq(expected.as_bytes(), code.as_bytes()) {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 6238 Appendix B test vectors use an ASCII secret "12345678901234567890"
    // ("GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ" in base32) with SHA1. At T=59s
    // (counter 1) the 8-digit TOTP is 94287082, so the 6-digit truncation is
    // 287082.
    const RFC_SECRET_B32: &str = "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ";

    #[test]
    fn rfc6238_vector_t59() {
        assert_eq!(
            compute_totp_at(RFC_SECRET_B32, 59).as_deref(),
            Some("287082")
        );
    }

    #[test]
    fn rfc6238_vector_t1111111109() {
        // 8-digit 07081804 -> 6-digit 081804.
        assert_eq!(
            compute_totp_at(RFC_SECRET_B32, 1_111_111_109).as_deref(),
            Some("081804")
        );
    }

    #[test]
    fn base32_lowercase_and_padding_ok() {
        // Same secret, lowercased + padded, decodes identically.
        assert_eq!(
            compute_totp_at(&RFC_SECRET_B32.to_lowercase(), 59).as_deref(),
            Some("287082")
        );
    }

    #[test]
    fn verify_accepts_current_and_window() {
        let secret = RFC_SECRET_B32;
        let now = chrono::Utc::now().timestamp() as u64;
        let current = compute_totp_now(secret).unwrap();
        assert!(verify_totp(secret, &current));
        // One step earlier / later still accepted (skew tolerance).
        let prev = compute_totp_at(secret, now - TIME_STEP_SECS).unwrap();
        let next = compute_totp_at(secret, now + TIME_STEP_SECS).unwrap();
        assert!(verify_totp(secret, &prev));
        assert!(verify_totp(secret, &next));
    }

    #[test]
    fn verify_rejects_two_steps_away() {
        let secret = RFC_SECRET_B32;
        let now = chrono::Utc::now().timestamp() as u64;
        // Two steps in the past is outside the +/-1 window and must be rejected,
        // as long as it differs from the current code (it does for this secret).
        let far = compute_totp_at(secret, now - 2 * TIME_STEP_SECS).unwrap();
        let current = compute_totp_now(secret).unwrap();
        if far != current {
            assert!(!verify_totp(secret, &far));
        }
    }

    #[test]
    fn verify_rejects_bad_secret() {
        assert!(!verify_totp("not base32 !!!", "123456"));
        assert!(!verify_totp("", "123456"));
    }
}
