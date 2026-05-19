//! `SqsService` `auth` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl SqsService {
    /// Resolve the effective at-rest encryption key id for a queue.
    ///
    /// Real AWS SQS exposes two SSE modes that are mutually exclusive at
    /// the API level but flow through the same KMS audit trail:
    ///
    /// - **SSE-KMS**: customer or AWS-managed CMK supplied via
    ///   `KmsMasterKeyId`. Wins when set to a non-empty value.
    /// - **SSE-SQS**: AWS-managed `alias/aws/sqs` key, implicit when
    ///   `SqsManagedSseEnabled=true` (the default since May 2023). The
    ///   alias is provisioned on first use by the KMS hook.
    ///
    /// Returns `None` when neither attribute selects a key — in that
    /// case the body is stored as plaintext (matches a queue with
    /// `SqsManagedSseEnabled=false` and no KMS key).
    pub(super) fn effective_kms_key_id(attributes: &BTreeMap<String, String>) -> Option<String> {
        if let Some(k) = attributes.get("KmsMasterKeyId") {
            if !k.is_empty() {
                return Some(k.clone());
            }
        }
        if attributes.get("SqsManagedSseEnabled").map(String::as_str) == Some("true") {
            return Some("alias/aws/sqs".to_string());
        }
        None
    }
}
