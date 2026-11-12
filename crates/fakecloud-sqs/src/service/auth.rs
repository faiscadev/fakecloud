//! `SqsService` `auth` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl SqsService {
    /// The KMS key a queue seals message bodies with: its `KmsMasterKeyId`
    /// (SSE-KMS). SSE-SQS (`SqsManagedSseEnabled=true`, the default) uses
    /// SQS-owned keys and never calls KMS: no `aws/sqs` key is created and
    /// no key policy is consulted, so a service principal can deliver to
    /// such a queue with no KMS permission (recorded: an SNS topic feeding
    /// an SSE-SQS queue delivers, and the queue reports no
    /// `KmsMasterKeyId`). Its bodies are stored as sent.
    pub(super) fn effective_kms_key_id(attributes: &BTreeMap<String, String>) -> Option<String> {
        attributes
            .get("KmsMasterKeyId")
            .filter(|k| !k.is_empty())
            .cloned()
    }

    /// Whether a queue encrypts at rest (SSE-KMS or SSE-SQS). Used only for
    /// messages stored before each recorded its own seal (see
    /// [`SqsMessage::body_is_sealed`]): those were sealed by the queue
    /// holding them, SSE-SQS ones under `alias/aws/sqs`.
    pub(super) fn encrypted_at_rest(attributes: &BTreeMap<String, String>) -> bool {
        crate::state::queue_encrypts_at_rest(attributes)
    }
}
