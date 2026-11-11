//! Encrypting received mail for a receipt rule's S3 action.
//!
//! When an S3 action names a `KmsKeyArn`, SES encrypts each message itself
//! before writing it to the bucket, the way the Amazon S3 Encryption Client
//! does (SES developer guide, "Deliver to S3 bucket action"): a fresh data
//! key encrypts the message with AES/GCM/NoPadding, KMS encrypts the data
//! key, and the object carries the envelope in its user metadata
//! (`x-amz-key-v2`, `x-amz-iv`, `x-amz-matdesc`, `x-amz-wrap-alg`,
//! `x-amz-cek-alg`, `x-amz-tag-len`). The object itself is not SSE-encrypted:
//! a reader decrypts it client-side, calling KMS `Decrypt` on the data key.
//!
//! SES calls KMS as its service principal (`ses.<region>.amazonaws.com`),
//! with the encryption context the AWS-managed `aws/ses` key's policy
//! requires (recorded: `aws:ses:source-account`, `aws:ses:rule-name`,
//! `aws:ses:message-id`); that policy also lets the account decrypt directly.
//! A customer managed key must grant SES the same in its policy. The context
//! is also the object's material description (`x-amz-matdesc`), from which a
//! reader rebuilds it.

use std::collections::{BTreeMap, HashMap};

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Key};
use base64::Engine;
use fakecloud_core::delivery::{KmsCaller, KmsError, KmsHook};

/// A message sealed for the S3 action: the object body and the user
/// metadata (keys without the `x-amz-meta-` prefix) that describe it.
#[derive(Debug, Clone)]
pub struct SealedMail {
    pub body: Vec<u8>,
    pub metadata: BTreeMap<String, String>,
}

/// The encryption context SES binds a message's data key to.
pub fn encryption_context(
    account_id: &str,
    rule_name: &str,
    message_id: &str,
) -> HashMap<String, String> {
    [
        ("aws:ses:source-account".to_string(), account_id.to_string()),
        ("aws:ses:rule-name".to_string(), rule_name.to_string()),
        ("aws:ses:message-id".to_string(), message_id.to_string()),
    ]
    .into()
}

/// The ARN of receipt rule `rule_name` in rule set `rule_set`.
pub fn receipt_rule_arn(region: &str, account_id: &str, rule_set: &str, rule_name: &str) -> String {
    fakecloud_aws::arn::Arn::regional(
        "ses",
        region,
        account_id,
        &format!("receipt-rule-set/{rule_set}:receipt-rule/{rule_name}"),
    )
    .to_string()
}

/// Seal `message` for the S3 action of rule `rule_name` (in `rule_set`)
/// under `kms_key_arn`, in `account_id` and `region`. Fails with KMS's error
/// when KMS refuses SES the key.
#[allow(clippy::too_many_arguments)]
pub fn seal(
    hook: &dyn KmsHook,
    account_id: &str,
    region: &str,
    kms_key_arn: &str,
    rule_set: &str,
    rule_name: &str,
    message_id: &str,
    message: &[u8],
) -> Result<SealedMail, KmsError> {
    let context = encryption_context(account_id, rule_name, message_id);
    let principal = format!("ses.{region}.amazonaws.com");
    let rule_arn = receipt_rule_arn(region, account_id, rule_set, rule_name);
    let data_key = Aes256Gcm::generate_key(&mut OsRng);
    let wrapped = hook.encrypt(
        account_id,
        region,
        kms_key_arn,
        data_key.as_slice(),
        KmsCaller::Service {
            service_principal: &principal,
            source_arn: &rule_arn,
            source_account: account_id,
        },
        context.clone(),
    )?;
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(data_key.as_slice()));
    let iv = Aes256Gcm::generate_nonce(&mut OsRng);
    let body = cipher
        .encrypt(&iv, message)
        .expect("AES-GCM encryption with a 96-bit nonce does not fail");
    let b64 = base64::engine::general_purpose::STANDARD;
    let matdesc: BTreeMap<&String, &String> = context.iter().collect();
    let metadata = BTreeMap::from([
        ("x-amz-key-v2".to_string(), wrapped),
        ("x-amz-iv".to_string(), b64.encode(iv)),
        (
            "x-amz-matdesc".to_string(),
            serde_json::to_string(&matdesc).unwrap_or_default(),
        ),
        ("x-amz-wrap-alg".to_string(), "kms".to_string()),
        ("x-amz-cek-alg".to_string(), "AES/GCM/NoPadding".to_string()),
        ("x-amz-tag-len".to_string(), "128".to_string()),
        (
            "x-amz-unencrypted-content-length".to_string(),
            message.len().to_string(),
        ),
    ]);
    Ok(SealedMail { body, metadata })
}

/// Open a message [`seal`] produced, given the data key KMS decrypted from
/// its `x-amz-key-v2` (what a reader's S3 encryption client does).
pub fn open(data_key: &[u8], sealed: &SealedMail) -> Option<Vec<u8>> {
    let b64 = base64::engine::general_purpose::STANDARD;
    let iv = b64.decode(sealed.metadata.get("x-amz-iv")?).ok()?;
    if data_key.len() != 32 || iv.len() != 12 {
        return None;
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(data_key));
    cipher
        .decrypt(aes_gcm::Nonce::from_slice(&iv), sealed.body.as_slice())
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::delivery::KmsErrorKind;

    /// A hook standing in for KMS: "wraps" the data key reversibly and
    /// records the caller and context it was given.
    #[derive(Default)]
    struct RecordingKms {
        seen: parking_lot::Mutex<Vec<(String, HashMap<String, String>)>>,
        refuse: bool,
    }

    impl KmsHook for RecordingKms {
        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            key_id: &str,
            plaintext: &[u8],
            caller: KmsCaller<'_>,
            encryption_context: HashMap<String, String>,
        ) -> Result<String, KmsError> {
            let KmsCaller::Service {
                service_principal,
                source_arn,
                ..
            } = caller
            else {
                panic!("SES must call KMS as its service principal");
            };
            self.seen.lock().push((
                format!("{service_principal} {source_arn} {key_id}"),
                encryption_context,
            ));
            if self.refuse {
                return Err(KmsError::new(KmsErrorKind::AccessDenied, "denied"));
            }
            Ok(base64::engine::general_purpose::STANDARD.encode(plaintext))
        }

        fn decrypt(
            &self,
            _account_id: &str,
            ciphertext_b64: &str,
            _caller: KmsCaller<'_>,
            _encryption_context: HashMap<String, String>,
        ) -> Result<Vec<u8>, KmsError> {
            Ok(base64::engine::general_purpose::STANDARD
                .decode(ciphertext_b64)
                .unwrap())
        }
    }

    #[test]
    fn seals_mail_as_ses_with_its_context() {
        let kms = RecordingKms::default();
        let key = "arn:aws:kms:us-east-1:123456789012:alias/aws/ses";
        let sealed = seal(
            &kms,
            "123456789012",
            "us-east-1",
            key,
            "set",
            "rule",
            "msg-1",
            b"Subject: hi\r\n\r\nbody",
        )
        .unwrap();
        let (who, ctx) = kms.seen.lock()[0].clone();
        assert_eq!(
            who,
            format!("ses.us-east-1.amazonaws.com arn:aws:ses:us-east-1:123456789012:receipt-rule-set/set:receipt-rule/rule {key}")
        );
        assert_eq!(ctx, encryption_context("123456789012", "rule", "msg-1"));
        let matdesc: HashMap<String, String> =
            serde_json::from_str(&sealed.metadata["x-amz-matdesc"]).unwrap();
        assert_eq!(matdesc, ctx);
        assert_eq!(sealed.metadata["x-amz-cek-alg"], "AES/GCM/NoPadding");
        assert_eq!(sealed.metadata["x-amz-wrap-alg"], "kms");
        assert_eq!(sealed.metadata["x-amz-tag-len"], "128");
        assert_ne!(sealed.body, b"Subject: hi\r\n\r\nbody");
        let data_key = kms
            .decrypt(
                "",
                &sealed.metadata["x-amz-key-v2"],
                KmsCaller::via(""),
                ctx,
            )
            .unwrap();
        assert_eq!(
            open(&data_key, &sealed).as_deref(),
            Some(&b"Subject: hi\r\n\r\nbody"[..])
        );
        assert_eq!(open(&[0u8; 32], &sealed), None);
    }

    #[test]
    fn a_refused_key_fails_the_action() {
        let kms = RecordingKms {
            refuse: true,
            ..Default::default()
        };
        let err = seal(&kms, "1", "us-east-1", "k", "s", "r", "m", b"x").unwrap_err();
        assert_eq!(err.kind, KmsErrorKind::AccessDenied);
    }
}
