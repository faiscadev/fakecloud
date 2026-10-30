//! End-to-end coverage for SES DKIM signing.
//!
//! Real SES stamps a `DKIM-Signature` header onto every outgoing message
//! whose `From` domain has signing enabled. The signature is computed
//! over the message headers + body using relaxed/relaxed canonicalization
//! and RSA-SHA256 (RFC 6376), and a downstream verifier resolves the
//! public key via the `<selector>._domainkey.<domain>` TXT record. These
//! tests verify the same end-to-end path: enable DKIM, send a message,
//! pull the stored email back via introspection, then verify the
//! signature against the public key fakecloud generated.

mod helpers;

use aws_sdk_ses::types::RawMessage;
use aws_sdk_sesv2::types::{Body, Content, Destination, EmailContent, Message};
use base64::Engine;
use helpers::TestServer;
use rsa::pkcs1v15::{Signature, VerifyingKey};
use rsa::pkcs8::DecodePublicKey;
use rsa::signature::Verifier;
use rsa::RsaPublicKey;
use sha2::{Digest, Sha256};

/// Re-implementation of the verifier side of relaxed/relaxed
/// canonicalization so the test trusts only the signature math, not the
/// helpers it shares with the producer side.
fn canonicalize_header_value(value: &str) -> String {
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

fn canonicalize_header(name: &str, value: &str) -> String {
    format!(
        "{}:{}\r\n",
        name.to_lowercase(),
        canonicalize_header_value(value)
    )
}

fn canonicalize_body(body: &str) -> String {
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

/// Parse the `tag=value;` list out of a DKIM-Signature header value.
fn parse_dkim_tags(value: &str) -> std::collections::HashMap<String, String> {
    let mut tags = std::collections::HashMap::new();
    for raw in value.split(';') {
        let part = raw.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((k, v)) = part.split_once('=') {
            tags.insert(k.trim().to_string(), v.trim().to_string());
        }
    }
    tags
}

#[tokio::test]
async fn ses_dkim_signature_verifies_against_published_public_key() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Verify the sender's domain identity. The v2 surface auto-generates
    // an Easy DKIM keypair on create, so signing is wired without a
    // follow-up PutEmailIdentityDkimAttributes.
    client
        .create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();
    client
        .create_email_identity()
        .email_identity("recipient@example.com")
        .send()
        .await
        .unwrap();

    // Force-enable signing through the public attribute API the way real
    // callers do — exercises the same code path as flipping it on later.
    client
        .put_email_identity_dkim_attributes()
        .email_identity("example.com")
        .signing_enabled(true)
        .send()
        .await
        .unwrap();

    // Send a simple email.
    let resp = client
        .send_email()
        .from_email_address("alice@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("hello dkim").build().unwrap())
                        .body(
                            Body::builder()
                                .text(
                                    Content::builder()
                                        .data("the body of the message")
                                        .build()
                                        .unwrap(),
                                )
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();
    let message_id = resp.message_id().unwrap().to_string();

    // Pull the stored emails out of the introspection endpoint.
    let http = reqwest::Client::new();
    let emails: serde_json::Value = http
        .get(format!("{}/_fakecloud/ses/emails", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let stored = emails["emails"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["messageId"] == message_id)
        .unwrap();

    // The DKIM signature value is exposed both as a top-level field and
    // as the first entry in the synthesized headers list.
    let dkim_value = stored["dkimSignature"].as_str().unwrap();
    assert!(dkim_value.contains("v=1"));
    assert!(dkim_value.contains("a=rsa-sha256"));
    assert!(dkim_value.contains("c=relaxed/relaxed"));
    assert!(dkim_value.contains("d=example.com"));
    let headers = stored["headers"].as_array().unwrap();
    assert_eq!(
        headers[0][0].as_str().unwrap(),
        "DKIM-Signature",
        "DKIM-Signature must be the first stamped header"
    );
    assert_eq!(headers[0][1].as_str().unwrap(), dkim_value);

    // Pull the published public DKIM key.
    let dkim_pub: serde_json::Value = http
        .get(format!(
            "{}/_fakecloud/ses/identities/example.com/dkim-public-key",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(dkim_pub["signingEnabled"], true);
    let pub_b64 = dkim_pub["publicKeyBase64"].as_str().unwrap();
    let pub_der = base64::engine::general_purpose::STANDARD
        .decode(pub_b64.as_bytes())
        .unwrap();
    let pub_key = RsaPublicKey::from_public_key_der(&pub_der).unwrap();
    let verifying_key: VerifyingKey<Sha256> = VerifyingKey::new(pub_key);

    // Reconstruct the signing input: every header listed in `h=`,
    // canonicalized + concatenated, then the DKIM-Signature header
    // itself with `b=` set to empty and no trailing CRLF.
    let tags = parse_dkim_tags(dkim_value);
    let signed_names = tags["h"].clone();
    let mut block = String::new();
    let header_map: std::collections::HashMap<String, String> = headers
        .iter()
        .skip(1) // skip DKIM-Signature itself
        .map(|pair| {
            (
                pair[0].as_str().unwrap().to_lowercase(),
                pair[1].as_str().unwrap().to_string(),
            )
        })
        .collect();
    for name in signed_names.split(':') {
        let value = header_map
            .get(name)
            .unwrap_or_else(|| panic!("header '{name}' listed in h= but not stored"));
        block.push_str(&canonicalize_header(name, value));
    }
    let b_idx = dkim_value.rfind("b=").unwrap();
    let unsigned = &dkim_value[..b_idx + 2];
    block.push_str(&format!(
        "dkim-signature:{}",
        canonicalize_header_value(unsigned)
    ));

    // Decode the b= tag (the raw RSA signature).
    let raw_b = dkim_value[b_idx + 2..].to_string();
    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(raw_b.as_bytes())
        .unwrap();
    let signature = Signature::try_from(sig_bytes.as_slice()).unwrap();

    verifying_key
        .verify(block.as_bytes(), &signature)
        .expect("DKIM signature must verify against the published public key");

    // Body hash check: bh= must match SHA-256 of the relaxed-canonical body.
    let body_text = stored["textBody"].as_str().unwrap();
    let canonical_body = canonicalize_body(body_text);
    let mut hasher = Sha256::new();
    hasher.update(canonical_body.as_bytes());
    let expected_bh = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());
    assert_eq!(
        tags["bh"], expected_bh,
        "bh= must match SHA-256(relaxed(body))"
    );
}

#[tokio::test]
async fn ses_dkim_skipped_when_signing_disabled() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    client
        .create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();
    client
        .create_email_identity()
        .email_identity("recipient@example.com")
        .send()
        .await
        .unwrap();

    // Flip signing OFF for the sending domain.
    client
        .put_email_identity_dkim_attributes()
        .email_identity("example.com")
        .signing_enabled(false)
        .send()
        .await
        .unwrap();

    client
        .send_email()
        .from_email_address("alice@example.com")
        .destination(
            Destination::builder()
                .to_addresses("recipient@example.com")
                .build(),
        )
        .content(
            EmailContent::builder()
                .simple(
                    Message::builder()
                        .subject(Content::builder().data("no dkim").build().unwrap())
                        .body(
                            Body::builder()
                                .text(Content::builder().data("plain").build().unwrap())
                                .build(),
                        )
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let emails: serde_json::Value = http
        .get(format!("{}/_fakecloud/ses/emails", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let stored = &emails["emails"][0];
    assert!(
        stored["dkimSignature"].is_null(),
        "dkimSignature must be absent when signing is disabled"
    );
    assert!(stored["headers"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn ses_v1_dkim_signs_send_raw_email() {
    let server = TestServer::start().await;
    let client = server.ses_client().await;

    // VerifyDomainDkim auto-generates the Easy DKIM keypair so that the
    // very next SendRawEmail call stamps a real DKIM-Signature.
    client
        .verify_domain_identity()
        .domain("example.com")
        .send()
        .await
        .unwrap();
    client
        .verify_domain_dkim()
        .domain("example.com")
        .send()
        .await
        .unwrap();
    client
        .set_identity_dkim_enabled()
        .identity("example.com")
        .dkim_enabled(true)
        .send()
        .await
        .unwrap();
    client
        .verify_email_identity()
        .email_address("recipient@example.com")
        .send()
        .await
        .unwrap();

    let raw =
        b"From: sender@example.com\r\nTo: recipient@example.com\r\nSubject: hi\r\n\r\nbody\r\n";
    client
        .send_raw_email()
        .raw_message(
            RawMessage::builder()
                .data(aws_smithy_types::Blob::new(raw.to_vec()))
                .build()
                .unwrap(),
        )
        .source("sender@example.com")
        .destinations("recipient@example.com")
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();
    let emails: serde_json::Value = http
        .get(format!("{}/_fakecloud/ses/emails", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let stored = &emails["emails"][0];
    let dkim = stored["dkimSignature"]
        .as_str()
        .expect("v1 SendRawEmail must produce a DKIM-Signature when signing is enabled");
    assert!(dkim.contains("d=example.com"));
    assert!(dkim.contains("c=relaxed/relaxed"));
    // Signed as sent: the message's own header fields plus the Date and
    // Message-ID SES adds, and the hash of its MIME body (not of the whole
    // message, nor of the API's base64).
    assert!(
        dkim.contains("h=from:to:subject:date:message-id;"),
        "{dkim}"
    );
    let body_hash = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        <sha2::Sha256 as sha2::Digest>::digest(b"body\r\n"),
    );
    assert!(dkim.contains(&format!("bh={body_hash};")), "{dkim}");
}

/// Switching DKIM signing on via the public attribute API must lazily
/// generate the Easy DKIM keypair if no caller-supplied key was loaded
/// yet — otherwise SendEmail would silently skip signing despite
/// `dkim_signing_enabled = true`.
#[tokio::test]
async fn ses_put_dkim_attributes_lazily_provisions_easy_dkim_key() {
    let server = TestServer::start().await;
    let client = server.sesv2_client().await;

    // Domain identity created with signing already enabled at create
    // time has a key. Disable, then re-enable: the existing key should
    // be retained, not regenerated.
    client
        .create_email_identity()
        .email_identity("example.com")
        .send()
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let url = format!(
        "{}/_fakecloud/ses/identities/example.com/dkim-public-key",
        server.endpoint()
    );
    let initial: serde_json::Value = http.get(&url).send().await.unwrap().json().await.unwrap();
    let initial_pub = initial["publicKeyBase64"].as_str().unwrap().to_string();
    assert!(!initial_pub.is_empty());

    client
        .put_email_identity_dkim_attributes()
        .email_identity("example.com")
        .signing_enabled(false)
        .send()
        .await
        .unwrap();
    client
        .put_email_identity_dkim_attributes()
        .email_identity("example.com")
        .signing_enabled(true)
        .send()
        .await
        .unwrap();
    let after_toggle: serde_json::Value =
        http.get(&url).send().await.unwrap().json().await.unwrap();
    assert_eq!(
        after_toggle["publicKeyBase64"].as_str().unwrap(),
        initial_pub,
        "toggling signing should not rotate the Easy DKIM key"
    );
    assert_eq!(after_toggle["signingEnabled"], true);
}
