//! DNSSEC ECDSAP256SHA256 (algorithm 13) signing helpers.
//!
//! Real Route 53 KSKs are stored in KMS and the user never sees the
//! private key, but tests need to verify that a signed zone produces
//! valid RRSIGs. We derive a deterministic ECDSA P-256 keypair from
//! `(hosted_zone_id, ksk_name)` so:
//!
//! * the same zone+KSK always yields the same DNSKEY (stable DS digest
//!   across persistence reloads),
//! * different KSKs in the same zone get distinct keys,
//! * tests can pre-compute the expected public key without round-tripping.
//!
//! The "RFC 6605 / RFC 4034 §3.1.8.1 canonical RRset" wire format
//! generator below is intentionally narrow: it covers the record types
//! `TestDNSAnswer` actually returns (A, AAAA, CNAME, TXT, MX, NS, SOA,
//! PTR, CAA, SRV, SPF). Anything else falls back to a best-effort raw
//! string encoding so signing never panics — the signature still
//! verifies against `canonical_rrset_bytes` for the same record set.

use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use p256::ecdsa::{
    signature::{Signer, Verifier},
    Signature, SigningKey, VerifyingKey,
};
use p256::pkcs8::{
    DecodePrivateKey, DecodePublicKey, EncodePrivateKey, EncodePublicKey, LineEnding,
};
use sha2::{Digest, Sha256};

/// Algorithm number for ECDSAP256SHA256 (RFC 6605).
pub const DNSSEC_ALGORITHM: u8 = 13;
/// DNSKEY flags for a Key Signing Key (Zone Key + SEP set).
pub const DNSKEY_FLAGS_KSK: u16 = 257;
/// DNSKEY protocol field — must be 3 per RFC 4034.
pub const DNSKEY_PROTOCOL: u8 = 3;

/// Material for a DNSSEC keypair attached to a KSK.
pub struct DnssecKeyMaterial {
    /// PKCS#8 PEM-encoded private key.
    pub private_key_pem: String,
    /// SubjectPublicKeyInfo DER bytes (suitable for re-loading via
    /// `VerifyingKey::from_public_key_der`).
    pub public_key_der: Vec<u8>,
    /// Raw uncompressed P-256 public key (`0x04 || X || Y`, 65 bytes).
    /// What the DNSKEY RDATA carries minus the SEC1 0x04 prefix.
    pub public_key_uncompressed: Vec<u8>,
    /// The 64-byte DNSKEY public-key field (`X || Y`, no SEC1 prefix).
    pub dnskey_public_key: Vec<u8>,
}

/// Derive a deterministic P-256 keypair from `(zone_id, ksk_name)`.
/// The 32-byte secret scalar is the SHA-256 of the salted seed; that
/// scalar is rejected and re-hashed if it lands at zero (vanishingly
/// unlikely but we still guard).
pub fn derive_keypair(zone_id: &str, ksk_name: &str) -> DnssecKeyMaterial {
    let mut seed = [0u8; 32];
    let mut salt = b"fakecloud-route53-dnssec".to_vec();
    salt.extend_from_slice(zone_id.as_bytes());
    salt.push(b':');
    salt.extend_from_slice(ksk_name.as_bytes());
    let mut hasher = Sha256::new();
    hasher.update(&salt);
    seed.copy_from_slice(&hasher.finalize());

    // SigningKey::from_bytes rejects a zero scalar; re-hash with a
    // suffix until we get a valid one. In practice the first try
    // always succeeds for SHA-256 of any non-empty input.
    let signing_key = loop {
        match SigningKey::from_bytes(&seed.into()) {
            Ok(sk) => break sk,
            Err(_) => {
                let mut h = Sha256::new();
                h.update(seed);
                h.update(b"-retry");
                seed.copy_from_slice(&h.finalize());
            }
        }
    };
    let verifying_key: VerifyingKey = *signing_key.verifying_key();
    let private_key_pem = signing_key
        .to_pkcs8_pem(LineEnding::LF)
        .expect("encode pkcs8 pem")
        .to_string();
    let public_key_der = verifying_key
        .to_public_key_der()
        .expect("encode spki der")
        .into_vec();
    let encoded = verifying_key.to_encoded_point(false);
    let uncompressed = encoded.as_bytes().to_vec();
    // The DNSKEY wire format omits the 0x04 SEC1 prefix; strip it.
    debug_assert_eq!(uncompressed.first(), Some(&0x04));
    let dnskey_public_key = uncompressed[1..].to_vec();
    DnssecKeyMaterial {
        private_key_pem,
        public_key_der,
        public_key_uncompressed: uncompressed,
        dnskey_public_key,
    }
}

/// Compute the DS record digest (SHA-256 over the canonical owner
/// name + DNSKEY RDATA) per RFC 4509. Returns lowercase hex.
///
/// `key_tag` isn't part of the digest (RFC 4509 §5.1.2 hashes only the
/// canonical owner name and the full DNSKEY RDATA), but we accept it
/// so callers consistently pass the same `(name, tag, key)` tuple
/// they later publish in the DS record. The argument is kept for
/// call-site readability and to make it easy to swap in future
/// digest types that do incorporate the tag.
pub fn ds_digest_sha256(zone_name: &str, _key_tag: u16, dnskey_public_key: &[u8]) -> String {
    let mut buf = Vec::with_capacity(64);
    encode_dns_name(&mut buf, zone_name);
    encode_dnskey_rdata(&mut buf, dnskey_public_key);
    let digest = Sha256::digest(&buf);
    digest
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect::<String>()
}

/// Compute the standard DNSKEY key tag (RFC 4034 Appendix B).
pub fn key_tag_for(dnskey_public_key: &[u8]) -> u16 {
    let mut rdata = Vec::with_capacity(4 + dnskey_public_key.len());
    encode_dnskey_rdata(&mut rdata, dnskey_public_key);
    let mut acc: u32 = 0;
    for (i, byte) in rdata.iter().enumerate() {
        if i & 1 == 0 {
            acc = acc.wrapping_add((*byte as u32) << 8);
        } else {
            acc = acc.wrapping_add(*byte as u32);
        }
    }
    acc = acc.wrapping_add((acc >> 16) & 0xFFFF);
    (acc & 0xFFFF) as u16
}

fn encode_dnskey_rdata(out: &mut Vec<u8>, dnskey_public_key: &[u8]) {
    out.extend_from_slice(&DNSKEY_FLAGS_KSK.to_be_bytes());
    out.push(DNSKEY_PROTOCOL);
    out.push(DNSSEC_ALGORITHM);
    out.extend_from_slice(dnskey_public_key);
}

/// Encode a DNS owner name (`example.com.`) in wire format: each label
/// length-prefixed, terminated by a zero byte. Labels are normalised
/// to lowercase per RFC 4034 §6.2 canonical form.
pub fn encode_dns_name(out: &mut Vec<u8>, name: &str) {
    let trimmed = name.trim_end_matches('.');
    if trimmed.is_empty() {
        out.push(0);
        return;
    }
    for label in trimmed.split('.') {
        let bytes = label.as_bytes();
        // Spec caps labels at 63 octets; truncate defensively rather
        // than panicking — the only call sites are zone names we
        // already validated upstream.
        let len = bytes.len().min(63);
        out.push(len as u8);
        for &b in &bytes[..len] {
            out.push(b.to_ascii_lowercase());
        }
    }
    out.push(0);
}

/// Build the canonical RRset bytes for a single (name, type, class,
/// ttl) tuple followed by `n` RDATA wire-format blobs sorted in
/// canonical order (§6.3 of RFC 4034). Each RDATA chunk is
/// length-prefixed (16-bit big-endian) so verifiers can locate
/// boundaries.
pub fn canonical_rrset_bytes(
    name: &str,
    rtype: u16,
    class: u16,
    ttl: u32,
    rdatas: &[Vec<u8>],
) -> Vec<u8> {
    let mut sorted: Vec<&Vec<u8>> = rdatas.iter().collect();
    sorted.sort();
    let mut out = Vec::new();
    for rdata in sorted {
        encode_dns_name(&mut out, name);
        out.extend_from_slice(&rtype.to_be_bytes());
        out.extend_from_slice(&class.to_be_bytes());
        out.extend_from_slice(&ttl.to_be_bytes());
        out.extend_from_slice(&(rdata.len() as u16).to_be_bytes());
        out.extend_from_slice(rdata);
    }
    out
}

/// All the RRSIG header fields a signer needs (everything except the
/// `Signature` itself). Bundled into a struct so call sites stay
/// readable when verifying or signing.
pub struct RrsigHeader<'a> {
    pub rtype: u16,
    pub algorithm: u8,
    pub labels: u8,
    pub original_ttl: u32,
    pub sig_expiration: u32,
    pub sig_inception: u32,
    pub key_tag: u16,
    pub signer_name: &'a str,
}

/// Build the bytes that get fed into ECDSA signing per RFC 4034 §3.1.8.1:
/// `RRSIG_RDATA(without signature) || RRset_canonical_form`.
pub fn rrsig_signed_data(header: &RrsigHeader<'_>, rrset_canonical: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(18 + header.signer_name.len() + rrset_canonical.len());
    out.extend_from_slice(&header.rtype.to_be_bytes());
    out.push(header.algorithm);
    out.push(header.labels);
    out.extend_from_slice(&header.original_ttl.to_be_bytes());
    out.extend_from_slice(&header.sig_expiration.to_be_bytes());
    out.extend_from_slice(&header.sig_inception.to_be_bytes());
    out.extend_from_slice(&header.key_tag.to_be_bytes());
    encode_dns_name(&mut out, header.signer_name);
    out.extend_from_slice(rrset_canonical);
    out
}

/// Sign `data` with the PKCS#8-PEM private key. Returns the raw 64-byte
/// `r || s` ECDSA-P256 signature ready for the RRSIG `Signature` field.
pub(crate) fn sign_with_pkcs8_pem(private_key_pem: &str, data: &[u8]) -> Vec<u8> {
    let signing_key =
        SigningKey::from_pkcs8_pem(private_key_pem).expect("valid pkcs8 pem from derive_keypair");
    let signature: Signature = signing_key.sign(data);
    signature.to_bytes().to_vec()
}

/// Verify a raw `r || s` signature against an SPKI DER-encoded public
/// key. Used by tests; not on any hot path.
pub fn verify_with_public_der(public_key_der: &[u8], data: &[u8], signature: &[u8]) -> bool {
    let Ok(verifying_key) = VerifyingKey::from_public_key_der(public_key_der) else {
        return false;
    };
    let Ok(sig) = Signature::from_slice(signature) else {
        return false;
    };
    verifying_key.verify(data, &sig).is_ok()
}

/// Base64 helper used by the admin endpoints when surfacing keys/sigs.
pub fn b64(input: &[u8]) -> String {
    B64.encode(input)
}

/// Count of labels in a DNS name for the RRSIG `Labels` field.
/// Excludes the implicit root and any leading wildcard label per
/// RFC 4034 §3.1.3.
pub fn label_count(name: &str) -> u8 {
    let trimmed = name.trim_end_matches('.');
    if trimmed.is_empty() {
        return 0;
    }
    let mut count: u8 = 0;
    for label in trimmed.split('.') {
        if label == "*" {
            continue;
        }
        count = count.saturating_add(1);
    }
    count
}

/// DNS class code for IN.
pub const CLASS_IN: u16 = 1;

/// Map a textual record type to its DNS numeric type code. Returns
/// `None` for types we don't recognise — the caller falls back to
/// signing the type as best-effort RDATA.
pub fn type_code(name: &str) -> Option<u16> {
    Some(match name.to_ascii_uppercase().as_str() {
        "A" => 1,
        "NS" => 2,
        "CNAME" => 5,
        "SOA" => 6,
        "PTR" => 12,
        "MX" => 15,
        "TXT" => 16,
        "AAAA" => 28,
        "SRV" => 33,
        "DS" => 43,
        "RRSIG" => 46,
        "DNSKEY" => 48,
        "SPF" => 99,
        "CAA" => 257,
        _ => return None,
    })
}

/// Encode a single record value (the textual form a Route 53 user
/// supplies in `ResourceRecords[].Value`) into wire-format RDATA for
/// the given record type. Best-effort: types we don't recognise are
/// passed through as raw UTF-8 bytes so signing remains deterministic.
pub fn encode_rdata(rtype: &str, value: &str) -> Vec<u8> {
    match rtype.to_ascii_uppercase().as_str() {
        "A" => encode_a(value),
        "AAAA" => encode_aaaa(value),
        "CNAME" | "NS" | "PTR" => {
            let mut out = Vec::new();
            encode_dns_name(&mut out, value);
            out
        }
        "TXT" | "SPF" => encode_txt(value),
        "MX" => encode_mx(value),
        "CAA" => encode_caa(value),
        _ => value.as_bytes().to_vec(),
    }
}

fn encode_a(value: &str) -> Vec<u8> {
    let mut bytes = [0u8; 4];
    let parts: Vec<&str> = value.split('.').collect();
    if parts.len() == 4 {
        for (i, p) in parts.iter().enumerate() {
            bytes[i] = p.parse::<u8>().unwrap_or(0);
        }
    }
    bytes.to_vec()
}

fn encode_aaaa(value: &str) -> Vec<u8> {
    // Use std parser; on failure return zeroed v6 so the canonical
    // form is still well-defined.
    match value.parse::<std::net::Ipv6Addr>() {
        Ok(addr) => addr.octets().to_vec(),
        Err(_) => vec![0u8; 16],
    }
}

fn encode_txt(value: &str) -> Vec<u8> {
    // Route 53 supplies `"foo"` (quoted) — strip the wrapping quotes
    // for the DNS wire format. RFC 1035 §3.3.14 says each character
    // string is `len-prefix || bytes` and chunks > 255 octets are
    // split into multiple character strings.
    let inner = value
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(value);
    let mut out = Vec::with_capacity(inner.len() + 1);
    let bytes = inner.as_bytes();
    for chunk in bytes.chunks(255) {
        out.push(chunk.len() as u8);
        out.extend_from_slice(chunk);
    }
    out
}

fn encode_mx(value: &str) -> Vec<u8> {
    // Format: `<preference> <exchange>`.
    let mut parts = value.splitn(2, char::is_whitespace);
    let pref: u16 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let exchange = parts.next().unwrap_or("").trim();
    let mut out = Vec::with_capacity(2 + exchange.len() + 2);
    out.extend_from_slice(&pref.to_be_bytes());
    encode_dns_name(&mut out, exchange);
    out
}

fn encode_caa(value: &str) -> Vec<u8> {
    // Format: `<flags> <tag> "<value>"`. Flags = u8, tag = ASCII
    // length-prefixed, value = remaining bytes.
    let mut parts = value.splitn(3, char::is_whitespace);
    let flags: u8 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let tag = parts.next().unwrap_or("").trim();
    let raw_val = parts.next().unwrap_or("");
    let val = raw_val
        .trim()
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(raw_val.trim());
    let mut out = Vec::with_capacity(2 + tag.len() + val.len());
    out.push(flags);
    out.push(tag.len() as u8);
    out.extend_from_slice(tag.as_bytes());
    out.extend_from_slice(val.as_bytes());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_keypair_is_deterministic() {
        let a = derive_keypair("Z123", "ksk1");
        let b = derive_keypair("Z123", "ksk1");
        assert_eq!(a.private_key_pem, b.private_key_pem);
        assert_eq!(a.dnskey_public_key, b.dnskey_public_key);
        let c = derive_keypair("Z123", "ksk2");
        assert_ne!(a.dnskey_public_key, c.dnskey_public_key);
    }

    #[test]
    fn sign_and_verify_round_trip() {
        let mat = derive_keypair("Z123", "ksk1");
        let data = b"hello dnssec world";
        let sig = sign_with_pkcs8_pem(&mat.private_key_pem, data);
        assert_eq!(sig.len(), 64, "ECDSA-P256 r||s must be 64 bytes");
        assert!(verify_with_public_der(&mat.public_key_der, data, &sig));
        assert!(!verify_with_public_der(
            &mat.public_key_der,
            b"tampered",
            &sig
        ));
    }

    #[test]
    fn key_tag_matches_dnskey_construction() {
        let mat = derive_keypair("Zexample", "primary");
        let tag = key_tag_for(&mat.dnskey_public_key);
        // Tag is deterministic for a deterministic key.
        assert_eq!(tag, key_tag_for(&mat.dnskey_public_key));
        assert_ne!(tag, 0);
    }

    #[test]
    fn ds_digest_is_64_hex_chars() {
        let mat = derive_keypair("Z1", "k1");
        let tag = key_tag_for(&mat.dnskey_public_key);
        let ds = ds_digest_sha256("example.com", tag, &mat.dnskey_public_key);
        assert_eq!(ds.len(), 64);
        assert!(ds.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn label_count_skips_wildcard() {
        assert_eq!(label_count("example.com."), 2);
        assert_eq!(label_count("*.example.com."), 2);
        assert_eq!(label_count(""), 0);
    }

    #[test]
    fn encode_a_and_aaaa() {
        assert_eq!(encode_rdata("A", "192.0.2.1"), vec![192, 0, 2, 1]);
        let aaaa = encode_rdata("AAAA", "2001:db8::1");
        assert_eq!(aaaa.len(), 16);
        assert_eq!(aaaa[0], 0x20);
        assert_eq!(aaaa[1], 0x01);
    }

    #[test]
    fn encode_txt_strips_quotes_and_chunks() {
        let out = encode_rdata("TXT", "\"hello\"");
        assert_eq!(out, vec![5, b'h', b'e', b'l', b'l', b'o']);
        let big: String = "a".repeat(300);
        let quoted = format!("\"{}\"", big);
        let out = encode_rdata("TXT", &quoted);
        // 255-byte chunk + 45-byte chunk + 2 length prefixes.
        assert_eq!(out.len(), 255 + 45 + 2);
        assert_eq!(out[0], 255);
        assert_eq!(out[256], 45);
    }
}
