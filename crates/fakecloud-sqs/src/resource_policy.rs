//! SQS implementation of [`ResourcePolicyProvider`].
//!
//! SQS persists queue access policies as raw JSON in
//! [`crate::state::SqsQueue::attributes`] under the `Policy` key — both
//! `SetQueueAttributes` and `AddPermission` / `RemovePermission` write
//! through that same slot. This file is the read-side bridge that
//! dispatch consults via `evaluate_with_resource_policy`, so cross-account
//! callers are gated by the queue's policy in addition to identity
//! policies and SCPs.
//!
//! Mirrors `fakecloud_sns::resource_policy` intentionally: single-service
//! gate, ARN parsing, state lookup, return `None` for anything not owned
//! here so providers compose safely.

use std::sync::Arc;

use fakecloud_core::auth::ResourcePolicyProvider;

use crate::state::SharedSqsState;

/// Concrete [`ResourcePolicyProvider`] backed by the in-memory
/// [`crate::state::SqsState`]. Server bootstrap clone-shares it via
/// [`fakecloud_core::auth::MultiResourcePolicyProvider`].
pub struct SqsResourcePolicyProvider {
    state: SharedSqsState,
}

impl SqsResourcePolicyProvider {
    pub fn new(state: SharedSqsState) -> Self {
        Self { state }
    }

    /// Convenience constructor returning an
    /// `Arc<dyn ResourcePolicyProvider>` so bootstrap can push it
    /// directly into a `MultiResourcePolicyProvider`.
    pub fn shared(state: SharedSqsState) -> Arc<dyn ResourcePolicyProvider> {
        Arc::new(Self::new(state))
    }
}

impl ResourcePolicyProvider for SqsResourcePolicyProvider {
    fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String> {
        if !service.eq_ignore_ascii_case("sqs") {
            return None;
        }
        if !is_sqs_queue_arn(resource_arn) {
            return None;
        }
        let accts = self.state.read();
        let acct = resource_arn.split(':').nth(4).unwrap_or("");
        let state = accts.get(acct).unwrap_or_else(|| accts.default_ref());
        state
            .queues
            .values()
            .find(|q| q.arn == resource_arn)
            .and_then(|q| q.attributes.get("Policy"))
            .cloned()
    }
}

/// Light validity check on an SQS queue ARN. Accepts the
/// `arn:aws:sqs:REGION:ACCOUNT:NAME` shape and nothing else — anything
/// that doesn't look like an SQS queue ARN short-circuits to `None`.
fn is_sqs_queue_arn(arn: &str) -> bool {
    let Some(rest) = arn.strip_prefix("arn:aws:sqs:") else {
        return false;
    };
    let parts: Vec<&str> = rest.splitn(3, ':').collect();
    if parts.len() != 3 {
        return false;
    }
    parts.iter().all(|p| !p.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{SqsQueue, SqsState};
    use chrono::Utc;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::collections::{BTreeMap, VecDeque};

    fn state_with_queue(arn: &str, policy: Option<&str>) -> SharedSqsState {
        let state = Arc::new(RwLock::new(MultiAccountState::<SqsState>::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )));
        let mut attrs = BTreeMap::new();
        if let Some(p) = policy {
            attrs.insert("Policy".to_string(), p.to_string());
        }
        let queue_name = arn.rsplit(':').next().unwrap_or("q").to_string();
        let queue_url = format!("http://localhost:4566/123456789012/{queue_name}");
        let queue = SqsQueue {
            queue_name: queue_name.clone(),
            queue_url: queue_url.clone(),
            arn: arn.to_string(),
            created_at: Utc::now(),
            messages: VecDeque::new(),
            inflight: Vec::new(),
            attributes: attrs,
            is_fifo: queue_name.ends_with(".fifo"),
            dedup_cache: BTreeMap::new(),
            redrive_policy: None,
            tags: BTreeMap::new(),
            next_sequence_number: 0,
            permission_labels: Vec::new(),
            receipt_handle_map: BTreeMap::new(),
            receive_attempt_cache: BTreeMap::new(),
        };
        state.write().default_mut().queues.insert(queue_url, queue);
        state
    }

    #[test]
    fn returns_stored_policy_for_sqs_arn() {
        let policy_json = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let arn = "arn:aws:sqs:us-east-1:123456789012:my-queue";
        let state = state_with_queue(arn, Some(policy_json));
        let provider = SqsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sqs", arn),
            Some(policy_json.to_string())
        );
    }

    #[test]
    fn returns_none_when_queue_has_no_policy_attribute() {
        let arn = "arn:aws:sqs:us-east-1:123456789012:my-queue";
        let state = state_with_queue(arn, None);
        let provider = SqsResourcePolicyProvider::new(state);
        assert_eq!(provider.resource_policy("sqs", arn), None);
    }

    #[test]
    fn returns_none_when_queue_missing() {
        let arn = "arn:aws:sqs:us-east-1:123456789012:other";
        let state = state_with_queue(arn, Some("{}"));
        let provider = SqsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sqs", "arn:aws:sqs:us-east-1:123456789012:my-queue"),
            None
        );
    }

    #[test]
    fn returns_none_for_non_sqs_service_prefix() {
        let arn = "arn:aws:sqs:us-east-1:123456789012:q";
        let state = state_with_queue(arn, Some("{}"));
        let provider = SqsResourcePolicyProvider::new(state);
        assert_eq!(provider.resource_policy("sns", arn), None);
        assert_eq!(provider.resource_policy("s3", arn), None);
    }

    #[test]
    fn service_prefix_match_is_case_insensitive() {
        let arn = "arn:aws:sqs:us-east-1:123456789012:q";
        let state = state_with_queue(arn, Some("{}"));
        let provider = SqsResourcePolicyProvider::new(state);
        assert!(provider.resource_policy("SQS", arn).is_some());
    }

    #[test]
    fn returns_none_for_malformed_arn() {
        let arn = "arn:aws:sqs:us-east-1:123456789012:q";
        let state = state_with_queue(arn, Some("{}"));
        let provider = SqsResourcePolicyProvider::new(state);
        assert_eq!(provider.resource_policy("sqs", ""), None);
        assert_eq!(provider.resource_policy("sqs", "not-an-arn"), None);
        assert_eq!(provider.resource_policy("sqs", "arn:aws:sqs:"), None);
        assert_eq!(
            provider.resource_policy("sqs", "arn:aws:sns:us-east-1:123456789012:q"),
            None
        );
    }

    #[test]
    fn is_sqs_queue_arn_rejects_empty_segments() {
        assert!(!is_sqs_queue_arn("arn:aws:sqs:::q"));
        assert!(!is_sqs_queue_arn("arn:aws:sqs:us-east-1::q"));
        assert!(!is_sqs_queue_arn("arn:aws:sqs:us-east-1:123456789012:"));
    }

    #[test]
    fn is_sqs_queue_arn_accepts_fifo_suffix() {
        assert!(is_sqs_queue_arn(
            "arn:aws:sqs:us-east-1:123456789012:q.fifo"
        ));
    }

    #[test]
    fn shared_constructor_wraps_in_arc() {
        let arn = "arn:aws:sqs:us-east-1:123456789012:q";
        let state = state_with_queue(arn, Some("doc"));
        let arc = SqsResourcePolicyProvider::shared(state);
        assert_eq!(arc.resource_policy("sqs", arn).as_deref(), Some("doc"));
    }
}
