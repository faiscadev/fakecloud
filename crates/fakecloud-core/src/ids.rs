//! Shared random resource-id generation.
//!
//! Many services mint short resource ids by truncating a v4 UUID. Before this
//! module each did it inline (`Uuid::new_v4().simple().to_string()[..N]`) or in
//! a per-crate `short_id` helper with a slightly different length, which was a
//! maintenance trap. These helpers centralise the idiom so a caller states the
//! length it needs explicitly.

/// A lowercase-hex id of `len` characters, drawn from a v4 UUID.
///
/// AWS short resource ids in this shape match `^[0-9a-f]{len}$`. `len` must be
/// at most 32 (a UUID has 32 hex digits); larger values are clamped to 32.
pub fn short_id(len: usize) -> String {
    let hex = uuid::Uuid::new_v4().simple().to_string();
    hex[..len.min(hex.len())].to_string()
}

/// A base-36 (`[0-9a-z]`) id of `len` characters, drawn from UUID entropy.
///
/// Used where AWS ids are lowercase-alphanumeric rather than hex. Draws from as
/// many v4 UUIDs as needed to cover `len` characters.
pub fn alnum_id(len: usize) -> String {
    const ALPHABET: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = String::with_capacity(len);
    while out.len() < len {
        for b in uuid::Uuid::new_v4().into_bytes() {
            out.push(ALPHABET[(b as usize) % ALPHABET.len()] as char);
            if out.len() == len {
                break;
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_id_has_requested_length_and_is_hex() {
        for n in [6, 10, 12, 16, 20, 32] {
            let id = short_id(n);
            assert_eq!(id.len(), n);
            assert!(id
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        }
    }

    #[test]
    fn short_id_clamps_over_32() {
        assert_eq!(short_id(64).len(), 32);
    }

    #[test]
    fn alnum_id_has_requested_length_and_is_base36() {
        for n in [1, 26, 40] {
            let id = alnum_id(n);
            assert_eq!(id.len(), n);
            assert!(id
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
        }
    }
}
