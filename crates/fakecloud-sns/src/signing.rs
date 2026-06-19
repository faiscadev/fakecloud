//! Real RSA-SHA256 signatures for SNS message envelopes.
//!
//! AWS SNS signs every notification with the topic owner's RSA key and
//! exposes the public certificate at `SigningCertURL`. Consumers (the
//! `sns_message_validator` family of SDKs across languages) fetch that
//! cert, build the canonical string, and verify the signature.
//!
//! fakecloud generates one self-signed RSA-2048 cert at process startup,
//! exposes the PEM at `/_fakecloud/sns/cert.pem`, and signs the canonical
//! string the same way real AWS does. Real verifier libraries accept it.

use std::sync::OnceLock;

use base64::Engine;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::EncodePrivateKey;
use rsa::sha2::Sha256;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use rsa::RsaPrivateKey;

const CERT_PATH: &str = "/_fakecloud/sns/cert.pem";

struct Material {
    signer: SigningKey<Sha256>,
    cert_pem: String,
}

static MATERIAL: OnceLock<Material> = OnceLock::new();

fn material() -> &'static Material {
    MATERIAL.get_or_init(|| {
        let mut rng = rand::thread_rng();
        let private_key =
            RsaPrivateKey::new(&mut rng, 2048).expect("generate RSA-2048 for SNS signing");
        let der = private_key
            .to_pkcs8_der()
            .expect("encode SNS signing key as PKCS8 DER");
        let key_pair = rcgen::KeyPair::try_from(der.as_bytes())
            .expect("rcgen accepts PKCS8 DER from rsa crate");
        let mut params = rcgen::CertificateParams::new(vec!["sns.fakecloud.local".to_string()])
            .expect("rcgen CertificateParams");
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "fakecloud SNS Signer");
        let cert = params
            .self_signed(&key_pair)
            .expect("self-sign SNS cert with generated key");
        let cert_pem = cert.pem();
        let signer = SigningKey::<Sha256>::new(private_key);
        Material { signer, cert_pem }
    })
}

/// PEM-encoded self-signed certificate. Served at `/_fakecloud/sns/cert.pem`.
pub fn cert_pem() -> &'static str {
    &material().cert_pem
}

/// Path under the fakecloud HTTP root where `cert_pem()` is served.
pub fn cert_path() -> &'static str {
    CERT_PATH
}

/// Build the absolute SigningCertURL given the request's endpoint base.
pub fn cert_url(endpoint: &str) -> String {
    format!("{}{}", endpoint.trim_end_matches('/'), CERT_PATH)
}

/// RSA-SHA256 sign the canonical string and return the base64 signature.
pub fn sign(canonical: &str) -> String {
    let signature = material()
        .signer
        .sign_with_rng(&mut rand::thread_rng(), canonical.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(signature.to_bytes())
}

/// Build the canonical string AWS uses for SNS Notification messages.
///
/// Order is alphabetical by key, with each `Key\nValue\n` pair appended.
/// `Subject` is included only when present.
pub(crate) fn canonical_notification(
    message: &str,
    message_id: &str,
    subject: Option<&str>,
    timestamp: &str,
    topic_arn: &str,
) -> String {
    let mut out = String::new();
    out.push_str("Message\n");
    out.push_str(message);
    out.push('\n');
    out.push_str("MessageId\n");
    out.push_str(message_id);
    out.push('\n');
    if let Some(s) = subject {
        out.push_str("Subject\n");
        out.push_str(s);
        out.push('\n');
    }
    out.push_str("Timestamp\n");
    out.push_str(timestamp);
    out.push('\n');
    out.push_str("TopicArn\n");
    out.push_str(topic_arn);
    out.push('\n');
    out.push_str("Type\n");
    out.push_str("Notification\n");
    out
}

/// Canonical string for SubscriptionConfirmation / UnsubscribeConfirmation.
pub fn canonical_subscription_confirmation(
    message: &str,
    message_id: &str,
    subscribe_url: &str,
    timestamp: &str,
    token: &str,
    topic_arn: &str,
    msg_type: &str,
) -> String {
    let mut out = String::new();
    out.push_str("Message\n");
    out.push_str(message);
    out.push('\n');
    out.push_str("MessageId\n");
    out.push_str(message_id);
    out.push('\n');
    out.push_str("SubscribeURL\n");
    out.push_str(subscribe_url);
    out.push('\n');
    out.push_str("Timestamp\n");
    out.push_str(timestamp);
    out.push('\n');
    out.push_str("Token\n");
    out.push_str(token);
    out.push('\n');
    out.push_str("TopicArn\n");
    out.push_str(topic_arn);
    out.push('\n');
    out.push_str("Type\n");
    out.push_str(msg_type);
    out.push('\n');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsa::pkcs1v15::VerifyingKey;
    use rsa::signature::{Keypair, Verifier};

    #[test]
    fn cert_pem_is_pem_encoded() {
        let pem = cert_pem();
        assert!(pem.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(pem.trim_end().ends_with("-----END CERTIFICATE-----"));
    }

    #[test]
    fn cert_url_appends_path() {
        assert_eq!(
            cert_url("http://localhost:7878"),
            "http://localhost:7878/_fakecloud/sns/cert.pem"
        );
        assert_eq!(
            cert_url("http://localhost:7878/"),
            "http://localhost:7878/_fakecloud/sns/cert.pem"
        );
    }

    #[test]
    fn signature_round_trips_through_public_key() {
        // Force material init so we can grab the signer's public counterpart.
        let _ = cert_pem();
        let canonical = canonical_notification(
            "hello",
            "11111111-2222-3333-4444-555555555555",
            Some("greetings"),
            "2026-04-25T12:34:56.000Z",
            "arn:aws:sns:us-east-1:123456789012:my-topic",
        );
        let sig_b64 = sign(&canonical);
        let sig_bytes = base64::engine::general_purpose::STANDARD
            .decode(&sig_b64)
            .unwrap();

        // Verify with the public key derived from the same private key.
        let signing_key = &material().signer;
        let verifying: VerifyingKey<Sha256> = signing_key.verifying_key();
        let signature = rsa::pkcs1v15::Signature::try_from(sig_bytes.as_slice()).unwrap();
        verifying.verify(canonical.as_bytes(), &signature).unwrap();
    }

    #[test]
    fn canonical_notification_omits_subject_when_absent() {
        let s = canonical_notification("m", "id", None, "t", "arn");
        assert!(!s.contains("Subject"));
    }

    #[test]
    fn canonical_notification_includes_subject_when_present() {
        let s = canonical_notification("m", "id", Some("sub"), "t", "arn");
        assert!(s.contains("Subject\nsub\n"));
    }
}
