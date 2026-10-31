//! DKIM signing for outbound email.
//!
//! When an `EmailIdentity` has DKIM enabled and a private key configured,
//! every email sent through that identity gets a `DKIM-Signature` header
//! computed over the message headers + body using relaxed/relaxed
//! canonicalization (RFC 6376 §3.4.2/§3.4.4) with RSA-SHA256. Real
//! verifiers can validate against the matching public key (Easy DKIM
//! publishes generated public keys via the per-identity `DkimTokens`;
//! BYODKIM uses the caller-supplied key).

use base64::Engine;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::signature::{SignatureEncoding, Signer};
use rsa::{RsaPrivateKey, RsaPublicKey};
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

use crate::state::{SentEmail, SesState};

const DEFAULT_KEY_BITS: usize = 2048;
const SIGNED_HEADERS: &[&str] = &["from", "to", "subject", "date", "message-id"];

/// Process-wide cached Easy-DKIM keypair. RSA-2048 generation costs
/// 100–500 ms; emulator workloads spin up identities in bursts and the
/// keypair only needs to be stable per-identity, not unique. Cache once
/// and reuse so CreateEmailIdentity stays sub-millisecond and tests
/// don't time out under concurrent conformance probes.
fn cached_easy_dkim_keypair() -> &'static (String, String) {
    static CACHE: OnceLock<(String, String)> = OnceLock::new();
    CACHE.get_or_init(generate_easy_dkim_keypair_uncached)
}

fn generate_easy_dkim_keypair_uncached() -> (String, String) {
    let mut rng = rand::thread_rng();
    let priv_key = RsaPrivateKey::new(&mut rng, DEFAULT_KEY_BITS).expect("rsa keypair generation");
    let pub_key = RsaPublicKey::from(&priv_key);
    let priv_pem = priv_key
        .to_pkcs8_pem(LineEnding::LF)
        .expect("encode pkcs8 pem")
        .to_string();
    let pub_der = pub_key
        .to_public_key_der()
        .expect("encode spki der")
        .as_bytes()
        .to_vec();
    let pub_b64 = base64::engine::general_purpose::STANDARD.encode(pub_der);
    (priv_pem, pub_b64)
}

/// Return a stable Easy-DKIM keypair (PKCS#8 PEM private key, base64
/// SubjectPublicKeyInfo DER). Generated lazily and reused across calls.
pub fn generate_easy_dkim_keypair() -> (String, String) {
    cached_easy_dkim_keypair().clone()
}

/// Normalize a BYODKIM `DomainSigningPrivateKey` for storage. AWS supplies
/// the key as base64-encoded DER (PKCS#8 or PKCS#1), so it must be decoded and
/// re-encoded to the PEM form the signer ([`sign_message`] via
/// [`parse_private_key`]) expects — otherwise valid AWS keys fail to parse and
/// no DKIM signatures are ever produced. Values already in PEM form, or that
/// can't be decoded/parsed, are returned unchanged as a best-effort fallback.
pub fn normalize_byodkim_private_key(raw: &str) -> String {
    if raw.contains("-----BEGIN") {
        return raw.to_string();
    }
    // AWS base64 material may carry wrapping whitespace/newlines; strip them.
    let compact: String = raw.split_whitespace().collect();
    let Ok(der) = base64::engine::general_purpose::STANDARD.decode(compact.as_bytes()) else {
        return raw.to_string();
    };
    let key = RsaPrivateKey::from_pkcs8_der(&der)
        .ok()
        .or_else(|| RsaPrivateKey::from_pkcs1_der(&der).ok());
    match key.and_then(|k| k.to_pkcs8_pem(LineEnding::LF).ok()) {
        Some(pem) => pem.to_string(),
        None => raw.to_string(),
    }
}

/// Sign the given message and return a fully-formed `DKIM-Signature`
/// header value (without the leading `DKIM-Signature: ` prefix). Returns
/// `None` when the private key cannot be parsed. Uses relaxed/relaxed
/// canonicalization per RFC 6376 §3.4.2 / §3.4.4.
pub fn sign_message(
    private_key_pem: &str,
    domain: &str,
    selector: &str,
    headers: &[(String, String)],
    body: &str,
) -> Option<String> {
    let priv_key = parse_private_key(private_key_pem)?;
    let signing_key = SigningKey::<Sha256>::new(priv_key);

    let canonical_body = canonicalize_body_relaxed(body);
    let mut body_hasher = Sha256::new();
    body_hasher.update(canonical_body.as_bytes());
    let bh = base64::engine::general_purpose::STANDARD.encode(body_hasher.finalize());

    let signed_headers: Vec<&(String, String)> = SIGNED_HEADERS
        .iter()
        .filter_map(|name| headers.iter().find(|(h, _)| h.eq_ignore_ascii_case(name)))
        .collect();
    let header_list = signed_headers
        .iter()
        .map(|(h, _)| h.to_lowercase())
        .collect::<Vec<_>>()
        .join(":");

    let mut header_block = String::new();
    for (h, v) in &signed_headers {
        header_block.push_str(&canonicalize_header_relaxed(h, v));
    }
    let dkim_unsigned = format!(
        "v=1; a=rsa-sha256; c=relaxed/relaxed; d={}; s={}; h={}; bh={}; b=",
        domain, selector, header_list, bh
    );
    // The signing input includes the canonicalized DKIM-Signature header
    // itself with an empty `b=` tag and NO trailing CRLF (RFC 6376 §3.7).
    header_block.push_str(&canonicalize_dkim_signature_for_signing(&dkim_unsigned));

    let signature = signing_key.sign(header_block.as_bytes());
    let b = base64::engine::general_purpose::STANDARD.encode(signature.to_bytes());
    Some(format!("{}{}", dkim_unsigned, b))
}

/// Look up the verified identity covering `sent.from` (full address first,
/// then domain) and compute the `DKIM-Signature` header value if signing
/// is enabled and a key is on file. Returns `None` if no matching identity
/// has DKIM signing wired.
pub fn signature_for_sent_email(state: &SesState, sent: &SentEmail) -> Option<String> {
    signed_headers_for_sent_email(state, sent).map(|(sig, _)| sig)
}

/// Variant of [`signature_for_sent_email`] that also returns the full
/// header block we stamped onto the message: the `DKIM-Signature`
/// header followed by the `From`/`To`/`Subject`/`Date`/`Message-ID`
/// lines covered by the signature (a raw message's own, otherwise the
/// ones SES builds). Returns
/// `None` when DKIM signing is disabled or no key is on file.
pub fn signed_headers_for_sent_email(
    state: &SesState,
    sent: &SentEmail,
) -> Option<(String, Vec<(String, String)>)> {
    let address = crate::mime::mailbox_address(&sent.from)?.to_lowercase();
    let domain = address.rsplit_once('@')?.1;
    let identity = state
        .identities
        .get(&address)
        .or_else(|| state.identities.get(domain))?;
    if !identity.dkim_signing_enabled {
        return None;
    }
    let private_key = identity.dkim_domain_signing_private_key.as_deref()?;
    let selector = identity
        .dkim_domain_signing_selector
        .as_deref()
        .unwrap_or("fakecloudses");
    let (signed_headers, body_text) = match sent.raw_data.as_deref() {
        // A raw message (plain MIME) is signed as sent: its own header
        // fields and its body. A field it lacks is the one SES adds (the
        // envelope sender, a generated Date / Message-ID).
        Some(raw) => {
            let mail = crate::mime::RawMail::from_mime(raw.as_bytes().to_vec());
            let headers = built_signed_headers(sent)
                .into_iter()
                .filter_map(|(name, built)| match mail.header(&name) {
                    Some(own) => Some((name, own.to_string())),
                    None if matches!(name.as_str(), "From" | "Date" | "Message-ID") => {
                        Some((name, built))
                    }
                    None => None,
                })
                .collect();
            (headers, crate::mime::body_section(raw).to_string())
        }
        None => {
            let body = sent
                .html_body
                .clone()
                .or_else(|| sent.text_body.clone())
                .unwrap_or_default();
            (built_signed_headers(sent), body)
        }
    };
    let signature = sign_message(private_key, domain, selector, &signed_headers, &body_text)?;
    let mut all_headers = Vec::with_capacity(signed_headers.len() + 1);
    all_headers.push(("DKIM-Signature".to_string(), signature.clone()));
    all_headers.extend(signed_headers);
    Some((signature, all_headers))
}

/// The signed header fields of a message SES builds from its fields.
fn built_signed_headers(sent: &SentEmail) -> Vec<(String, String)> {
    let date = sent
        .timestamp
        .format("%a, %d %b %Y %H:%M:%S +0000")
        .to_string();
    vec![
        ("From".to_string(), sent.from.clone()),
        ("To".to_string(), sent.to.join(", ")),
        // Sign the wire form of the subject: a decoded raw subject may hold
        // non-ASCII or line breaks, which only travel as an encoded-word.
        (
            "Subject".to_string(),
            crate::mime::encode_header(sent.subject.as_deref().unwrap_or_default()),
        ),
        ("Date".to_string(), date),
        (
            "Message-ID".to_string(),
            format!("<{}@fakecloud.local>", sent.message_id),
        ),
    ]
}

fn parse_private_key(pem: &str) -> Option<RsaPrivateKey> {
    RsaPrivateKey::from_pkcs8_pem(pem)
        .ok()
        .or_else(|| RsaPrivateKey::from_pkcs1_pem(pem).ok())
}

/// Relaxed body canonicalization per RFC 6376 §3.4.4:
/// - normalize line endings to CRLF
/// - reduce all WSP sequences within a line to a single SP
/// - strip trailing WSP from each line
/// - strip trailing empty lines
/// - if the body is non-empty, terminate with a single CRLF
fn canonicalize_body_relaxed(body: &str) -> String {
    let normalized = body.replace("\r\n", "\n").replace('\r', "\n");
    let mut lines: Vec<String> = normalized
        .split('\n')
        .map(|line| {
            let mut out = String::with_capacity(line.len());
            let mut prev_ws = false;
            for c in line.chars() {
                if c == ' ' || c == '\t' {
                    if !prev_ws {
                        out.push(' ');
                    }
                    prev_ws = true;
                } else {
                    out.push(c);
                    prev_ws = false;
                }
            }
            out.trim_end().to_string()
        })
        .collect();
    while lines.last().is_some_and(|l| l.is_empty()) {
        lines.pop();
    }
    if lines.is_empty() {
        String::new()
    } else {
        let mut out = lines.join("\r\n");
        out.push_str("\r\n");
        out
    }
}

/// Relaxed header canonicalization per RFC 6376 §3.4.2 for a single
/// header, returning the line terminated by CRLF. Header name is
/// lowercased; whitespace within the value is collapsed to a single
/// SP and trailing WSP is stripped.
fn canonicalize_header_relaxed(name: &str, value: &str) -> String {
    let canonical_value = canonicalize_header_value_relaxed(value);
    format!("{}:{}\r\n", name.to_lowercase(), canonical_value)
}

fn canonicalize_header_value_relaxed(value: &str) -> String {
    // Unfold first (CRLF + WSP -> single SP per the spec).
    let unfolded = value.replace("\r\n", "\n");
    let mut out = String::with_capacity(unfolded.len());
    let mut prev_ws = false;
    for c in unfolded.chars() {
        if c == ' ' || c == '\t' || c == '\n' {
            if !prev_ws {
                out.push(' ');
            }
            prev_ws = true;
        } else {
            out.push(c);
            prev_ws = false;
        }
    }
    out.trim().to_string()
}

/// Canonicalize the DKIM-Signature header for inclusion in the signing
/// input: uses the relaxed header rules, but the value is supplied
/// already-formatted with an empty `b=` tag and no trailing CRLF
/// (RFC 6376 §3.7).
fn canonicalize_dkim_signature_for_signing(unsigned_value: &str) -> String {
    let canonical_value = canonicalize_header_value_relaxed(unsigned_value);
    format!("dkim-signature:{}", canonical_value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_returns_parseable_pem_and_b64_pubkey() {
        let (pem, pub_b64) = generate_easy_dkim_keypair();
        assert!(pem.starts_with("-----BEGIN PRIVATE KEY-----"));
        assert!(parse_private_key(&pem).is_some());
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(pub_b64.as_bytes())
            .unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn sign_message_emits_dkim_signature_header_value() {
        let (pem, _) = generate_easy_dkim_keypair();
        let headers = vec![
            ("From".to_string(), "alice@example.com".to_string()),
            ("To".to_string(), "bob@example.com".to_string()),
            ("Subject".to_string(), "hi".to_string()),
            (
                "Date".to_string(),
                "Mon, 01 Jan 2024 00:00:00 +0000".to_string(),
            ),
            ("Message-ID".to_string(), "<x@example.com>".to_string()),
        ];
        let sig = sign_message(&pem, "example.com", "sel1", &headers, "hello world").unwrap();
        assert!(sig.contains("v=1"));
        assert!(sig.contains("a=rsa-sha256"));
        assert!(sig.contains("c=relaxed/relaxed"));
        assert!(sig.contains("d=example.com"));
        assert!(sig.contains("s=sel1"));
        assert!(sig.contains("h=from:to:subject:date:message-id"));
        assert!(sig.contains("bh="));
        assert!(sig.contains("b="));
    }

    #[test]
    fn sign_returns_none_for_garbage_pem() {
        let headers = vec![("From".to_string(), "x".to_string())];
        assert!(sign_message("not a key", "d", "s", &headers, "body").is_none());
    }

    #[test]
    fn normalize_byodkim_decodes_base64_der_to_signable_pem() {
        // AWS supplies BYODKIM keys as base64-encoded DER. Build one from the
        // Easy-DKIM PEM: decode the PEM, re-encode to DER, base64 it.
        let (pem, _) = generate_easy_dkim_keypair();
        let key = parse_private_key(&pem).unwrap();
        let der = key.to_pkcs8_der().unwrap();
        let b64 = base64::engine::general_purpose::STANDARD.encode(der.as_bytes());

        // Base64 DER -> normalized PEM that the signer can parse and use.
        let normalized = normalize_byodkim_private_key(&b64);
        assert!(
            normalized.contains("-----BEGIN"),
            "normalized BYODKIM key must be PEM, got: {normalized}"
        );
        assert!(
            parse_private_key(&normalized).is_some(),
            "normalized BYODKIM key must be parseable by the signer"
        );
        let headers = vec![("From".to_string(), "alice@example.com".to_string())];
        assert!(
            sign_message(&normalized, "example.com", "byo", &headers, "body").is_some(),
            "signer must produce a DKIM signature from the normalized key"
        );
    }

    #[test]
    fn normalize_byodkim_passes_pem_through_unchanged() {
        let (pem, _) = generate_easy_dkim_keypair();
        assert_eq!(normalize_byodkim_private_key(&pem), pem);
    }

    #[test]
    fn canonicalize_body_relaxed_normalizes_whitespace_and_endings() {
        assert_eq!(canonicalize_body_relaxed(""), "");
        assert_eq!(canonicalize_body_relaxed("a"), "a\r\n");
        assert_eq!(canonicalize_body_relaxed("a\nb\n\n\n"), "a\r\nb\r\n");
        // collapses internal WSP runs
        assert_eq!(canonicalize_body_relaxed("a   b\tc"), "a b c\r\n");
        // strips trailing WSP per line
        assert_eq!(canonicalize_body_relaxed("a   \nb"), "a\r\nb\r\n");
    }

    #[test]
    fn canonicalize_header_relaxed_lowercases_and_collapses() {
        assert_eq!(
            canonicalize_header_relaxed("From", "  Alice  <a@b.com>  "),
            "from:Alice <a@b.com>\r\n"
        );
        assert_eq!(
            canonicalize_header_relaxed("Subject", "  hello   world  "),
            "subject:hello world\r\n"
        );
    }

    #[test]
    fn signed_subject_is_wire_encoded_not_injectable() {
        let (pem, _) = generate_easy_dkim_keypair();
        let mut state = SesState::new("123456789012", "us-east-1");
        state.identities.insert(
            "example.com".to_string(),
            crate::state::EmailIdentity {
                identity_name: "example.com".to_string(),
                identity_type: "DOMAIN".to_string(),
                verified: true,
                created_at: chrono::Utc::now(),
                dkim_signing_enabled: true,
                dkim_signing_attributes_origin: "AWS_SES".to_string(),
                dkim_domain_signing_private_key: Some(pem),
                dkim_domain_signing_selector: None,
                dkim_next_signing_key_length: None,
                dkim_public_key_b64: None,
                email_forwarding_enabled: true,
                mail_from_domain: None,
                mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
                mail_from_domain_status: "NotStarted".to_string(),
                configuration_set_name: None,
                bounce_topic: None,
                complaint_topic: None,
                delivery_topic: None,
                verification_token: None,
            },
        );
        let sent = SentEmail {
            message_id: "m".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["to@example.com".to_string()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: Some("x\r\nBcc: evil@x".to_string()),
            html_body: None,
            text_body: Some("body".to_string()),
            raw_data: None,
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: chrono::Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let (_, headers) = signed_headers_for_sent_email(&state, &sent).unwrap();
        let subject = &headers.iter().find(|(n, _)| n == "Subject").unwrap().1;
        assert!(subject.starts_with("=?UTF-8?B?"), "{subject}");
        assert!(!subject.contains(['\r', '\n']));
    }

    #[test]
    fn raw_message_is_signed_over_its_own_headers_and_body() {
        let (pem, _) = generate_easy_dkim_keypair();
        let pem_copy = pem.clone();
        let mut state = SesState::new("123456789012", "us-east-1");
        state.identities.insert(
            "example.com".to_string(),
            crate::state::EmailIdentity {
                identity_name: "example.com".to_string(),
                identity_type: "DOMAIN".to_string(),
                verified: true,
                created_at: chrono::Utc::now(),
                dkim_signing_enabled: true,
                dkim_signing_attributes_origin: "AWS_SES".to_string(),
                dkim_domain_signing_private_key: Some(pem),
                dkim_domain_signing_selector: None,
                dkim_next_signing_key_length: None,
                dkim_public_key_b64: None,
                email_forwarding_enabled: true,
                mail_from_domain: None,
                mail_from_behavior_on_mx_failure: "USE_DEFAULT_VALUE".to_string(),
                mail_from_domain_status: "NotStarted".to_string(),
                configuration_set_name: None,
                bounce_topic: None,
                complaint_topic: None,
                delivery_topic: None,
                verification_token: None,
            },
        );
        let sent = SentEmail {
            message_id: "m".to_string(),
            from: "sender@example.com".to_string(),
            to: vec!["to@example.com".to_string()],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: Some("Ol\u{e1}".to_string()),
            html_body: None,
            text_body: Some("Hello\r\nthere\r\n".to_string()),
            raw_data: Some(
                "From: Sender <sender@example.com>\r\nTo: to@example.com\r\nSubject: =?UTF-8?B?T2zDoQ==?=\r\nMessage-ID: <raw-1@example.com>\r\n\r\nHello\r\nthere\r\n"
                    .to_string(),
            ),
            template_name: None,
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: chrono::Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };
        let (signature, headers) = signed_headers_for_sent_email(&state, &sent).unwrap();
        let own = vec![
            (
                "From".to_string(),
                "Sender <sender@example.com>".to_string(),
            ),
            ("To".to_string(), "to@example.com".to_string()),
            ("Subject".to_string(), "=?UTF-8?B?T2zDoQ==?=".to_string()),
            // Missing from the message: the Date SES adds.
            (
                "Date".to_string(),
                sent.timestamp
                    .format("%a, %d %b %Y %H:%M:%S +0000")
                    .to_string(),
            ),
            ("Message-ID".to_string(), "<raw-1@example.com>".to_string()),
        ];
        assert_eq!(headers[1..], own[..]);
        // Signing is deterministic: the same headers and the MIME body
        // (not the whole message, and never base64) give the same value.
        let expected = sign_message(
            &pem_copy,
            "example.com",
            "fakecloudses",
            &own,
            "Hello\r\nthere\r\n",
        )
        .unwrap();
        assert_eq!(signature, expected);
    }

    #[test]
    fn signed_signature_verifies_against_generated_public_key() {
        use rsa::pkcs1v15::{Signature, VerifyingKey};
        use rsa::pkcs8::DecodePublicKey;
        use rsa::signature::Verifier;

        let (pem, pub_b64) = generate_easy_dkim_keypair();
        let headers = vec![
            ("From".to_string(), "alice@example.com".to_string()),
            ("To".to_string(), "bob@example.com".to_string()),
            ("Subject".to_string(), "hello".to_string()),
            (
                "Date".to_string(),
                "Mon, 01 Jan 2024 00:00:00 +0000".to_string(),
            ),
            ("Message-ID".to_string(), "<x@example.com>".to_string()),
        ];
        let body = "hello world\r\n";
        let sig_value = sign_message(&pem, "example.com", "sel1", &headers, body).unwrap();

        // Reconstruct the signing input the same way sign_message does.
        let mut block = String::new();
        for (h, v) in &headers {
            block.push_str(&canonicalize_header_relaxed(h, v));
        }
        // Strip the b=<sig> tail off the DKIM-Signature value to recover
        // the unsigned form.
        let b_idx = sig_value.rfind("b=").unwrap();
        let unsigned = &sig_value[..b_idx + 2];
        block.push_str(&canonicalize_dkim_signature_for_signing(unsigned));

        // Decode the b= tag to get the raw signature bytes.
        let raw_b = sig_value[b_idx + 2..].to_string();
        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(raw_b.as_bytes())
            .unwrap();
        let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();

        // Decode the public key from the SPKI DER we exposed.
        let pub_der = base64::engine::general_purpose::STANDARD
            .decode(pub_b64.as_bytes())
            .unwrap();
        let pub_key = RsaPublicKey::from_public_key_der(&pub_der).unwrap();
        let verifying_key: VerifyingKey<Sha256> = VerifyingKey::new(pub_key);

        verifying_key.verify(block.as_bytes(), &signature).unwrap();
    }
}
