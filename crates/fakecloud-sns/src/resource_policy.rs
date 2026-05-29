//! SNS implementation of [`ResourcePolicyProvider`].
//!
//! SNS persists topic policies as raw JSON in
//! [`crate::state::SnsTopic::attributes`] under the `Policy` key; both
//! `SetTopicAttributes` and `AddPermission` / `RemovePermission` write
//! through that same slot. This file is the read-side bridge into the
//! `fakecloud-core::auth::ResourcePolicyProvider` trait — dispatch
//! fetches the stored document and hands it to the evaluator alongside
//! the caller's identity policies.
//!
//! Mirrors the shape of `fakecloud_s3::resource_policy` intentionally:
//! single-service gate, ARN parsing, state lookup, return `None` for
//! anything not owned here so composition is safe.

use std::sync::Arc;

use fakecloud_core::auth::ResourcePolicyProvider;

use crate::state::SharedSnsState;

/// Concrete [`ResourcePolicyProvider`] backed by the in-memory
/// [`crate::state::SnsState`]. Server bootstrap clone-shares it via
/// [`fakecloud_core::auth::MultiResourcePolicyProvider`].
pub struct SnsResourcePolicyProvider {
    state: SharedSnsState,
}

impl SnsResourcePolicyProvider {
    pub fn new(state: SharedSnsState) -> Self {
        Self { state }
    }

    /// Convenience constructor returning an
    /// `Arc<dyn ResourcePolicyProvider>` so bootstrap can push it
    /// directly into a `MultiResourcePolicyProvider`.
    pub fn shared(state: SharedSnsState) -> Arc<dyn ResourcePolicyProvider> {
        Arc::new(Self::new(state))
    }
}

impl ResourcePolicyProvider for SnsResourcePolicyProvider {
    fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String> {
        if !service.eq_ignore_ascii_case("sns") {
            return None;
        }
        if !is_sns_topic_arn(resource_arn) {
            return None;
        }
        let accts = self.state.read();
        let acct = resource_arn.split(':').nth(4).unwrap_or("");
        let state = accts.get(acct).unwrap_or_else(|| accts.default_ref());
        state
            .topics
            .get(resource_arn)
            .and_then(|t| t.attributes.get("Policy"))
            .cloned()
    }
}

/// A very light validity check on an SNS topic ARN. Accepts the
/// `arn:aws:sns:REGION:ACCOUNT:NAME` shape and nothing else — anything
/// that doesn't look like an SNS ARN short-circuits to `None` before
/// we reach the state map so we never accidentally hand out a policy
/// belonging to some unrelated ARN that happens to be a map key.
fn is_sns_topic_arn(arn: &str) -> bool {
    let Some(rest) = arn.strip_prefix("arn:aws:sns:") else {
        return false;
    };
    // Expect exactly 3 colon-separated segments after the prefix:
    // REGION, ACCOUNT, NAME. Anything else is malformed for SNS.
    let parts: Vec<&str> = rest.splitn(3, ':').collect();
    if parts.len() != 3 {
        return false;
    }
    // All three segments must be non-empty — AWS doesn't allow blank
    // region, account, or name on a real SNS ARN, and our state map is
    // keyed by the fully-qualified ARN so blanks would never match
    // anyway.
    parts.iter().all(|p| !p.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{SnsState, SnsTopic};
    use chrono::Utc;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::collections::BTreeMap;

    fn state_with_topic(arn: &str, policy: Option<&str>) -> SharedSnsState {
        let state = Arc::new(RwLock::new(MultiAccountState::<SnsState>::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        )));
        let mut attrs = BTreeMap::new();
        if let Some(p) = policy {
            attrs.insert("Policy".to_string(), p.to_string());
        }
        state.write().default_mut().topics.insert(
            arn.to_string(),
            SnsTopic {
                topic_arn: arn.to_string(),
                name: arn.rsplit(':').next().unwrap_or("t").to_string(),
                attributes: attrs,
                tags: Vec::new(),
                is_fifo: false,
                created_at: Utc::now(),
                subscriptions_deleted: 0,
                fifo_sequence: 0,
            },
        );
        state
    }

    #[test]
    fn returns_stored_policy_for_sns_arn() {
        let policy_json = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let state = state_with_topic(
            "arn:aws:sns:us-east-1:123456789012:my-topic",
            Some(policy_json),
        );
        let provider = SnsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sns", "arn:aws:sns:us-east-1:123456789012:my-topic"),
            Some(policy_json.to_string())
        );
    }

    #[test]
    fn returns_none_when_topic_has_no_policy_attribute() {
        let state = state_with_topic("arn:aws:sns:us-east-1:123456789012:my-topic", None);
        let provider = SnsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sns", "arn:aws:sns:us-east-1:123456789012:my-topic"),
            None
        );
    }

    #[test]
    fn returns_none_when_topic_missing() {
        let state = state_with_topic("arn:aws:sns:us-east-1:123456789012:other", Some("{}"));
        let provider = SnsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sns", "arn:aws:sns:us-east-1:123456789012:my-topic"),
            None
        );
    }

    #[test]
    fn returns_none_for_non_sns_service_prefix() {
        let state = state_with_topic("arn:aws:sns:us-east-1:123456789012:t", Some("{}"));
        let provider = SnsResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("s3", "arn:aws:sns:us-east-1:123456789012:t"),
            None
        );
        assert_eq!(
            provider.resource_policy("lambda", "arn:aws:sns:us-east-1:123456789012:t"),
            None
        );
    }

    #[test]
    fn service_prefix_match_is_case_insensitive() {
        let state = state_with_topic("arn:aws:sns:us-east-1:123456789012:t", Some("{}"));
        let provider = SnsResourcePolicyProvider::new(state);
        assert!(provider
            .resource_policy("SNS", "arn:aws:sns:us-east-1:123456789012:t")
            .is_some());
    }

    #[test]
    fn returns_none_for_malformed_arn() {
        let state = state_with_topic("arn:aws:sns:us-east-1:123456789012:t", Some("{}"));
        let provider = SnsResourcePolicyProvider::new(state);
        assert_eq!(provider.resource_policy("sns", ""), None);
        assert_eq!(provider.resource_policy("sns", "not-an-arn"), None);
        assert_eq!(provider.resource_policy("sns", "arn:aws:sns:"), None);
        assert_eq!(
            provider.resource_policy("sns", "arn:aws:sns:us-east-1:"),
            None
        );
        // S3-shaped ARN must not match even if service prefix is sns
        assert_eq!(
            provider.resource_policy("sns", "arn:aws:s3:::my-bucket"),
            None
        );
    }

    #[test]
    fn is_sns_topic_arn_rejects_empty_segments() {
        assert!(!is_sns_topic_arn("arn:aws:sns:::t"));
        assert!(!is_sns_topic_arn("arn:aws:sns:us-east-1::t"));
        assert!(!is_sns_topic_arn("arn:aws:sns:us-east-1:123456789012:"));
    }

    #[test]
    fn is_sns_topic_arn_accepts_fifo_suffix() {
        // FIFO topic ARNs end with .fifo; the prefix parser shouldn't
        // care about the suffix.
        assert!(is_sns_topic_arn(
            "arn:aws:sns:us-east-1:123456789012:my-topic.fifo"
        ));
    }

    #[test]
    fn shared_constructor_wraps_in_arc() {
        let state = state_with_topic("arn:aws:sns:us-east-1:123456789012:t", Some("doc"));
        let arc = SnsResourcePolicyProvider::shared(state);
        assert_eq!(
            arc.resource_policy("sns", "arn:aws:sns:us-east-1:123456789012:t")
                .as_deref(),
            Some("doc")
        );
    }
}
