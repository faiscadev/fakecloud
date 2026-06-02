//! S3 implementation of [`ResourcePolicyProvider`].
//!
//! Phase 2 of IAM enforcement adds resource-based policy evaluation:
//! dispatch fetches the policy attached to the target resource and hands
//! it to the evaluator so cross-account Allow/Deny semantics can be
//! computed alongside the caller's identity policies.
//!
//! This provider owns the `s3` service prefix only. SNS topic policies,
//! KMS key policies, and Lambda resource policies are distinct future
//! rollouts with their own storage models.
//!
//! Storage happens already — [`crate::service::config`] persists the
//! raw JSON policy on [`S3Bucket::policy`] via PutBucketPolicy /
//! DeleteBucketPolicy. This file is the read-side bridge into the
//! `fakecloud-core::auth::ResourcePolicyProvider` trait.

use std::sync::Arc;

use fakecloud_core::auth::ResourcePolicyProvider;

use crate::state::SharedS3State;

/// Concrete [`ResourcePolicyProvider`] backed by the in-memory
/// [`crate::state::S3State`]. Clone-shared via `Arc` into
/// [`fakecloud_core::dispatch::DispatchConfig::resource_policy_provider`]
/// at server bootstrap.
pub struct S3ResourcePolicyProvider {
    state: SharedS3State,
}

impl S3ResourcePolicyProvider {
    pub fn new(state: SharedS3State) -> Self {
        Self { state }
    }

    /// Convenience constructor returning an `Arc<dyn ResourcePolicyProvider>`
    /// directly — matches the pattern used by the IAM crate for its
    /// evaluator / credential-resolver shared constructors so the server
    /// bootstrap reads uniformly.
    pub fn shared(state: SharedS3State) -> Arc<dyn ResourcePolicyProvider> {
        Arc::new(Self::new(state))
    }
}

impl ResourcePolicyProvider for S3ResourcePolicyProvider {
    fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String> {
        if !service.eq_ignore_ascii_case("s3") {
            return None;
        }
        let bucket_name = parse_bucket_name(resource_arn)?;
        let mas = self.state.read();
        let acct = mas.find_account(|s| s.buckets.contains_key(bucket_name))?;
        let state = mas.get(acct)?;
        state
            .buckets
            .get(bucket_name)
            .and_then(|b| b.policy.clone())
    }

    fn resource_owner_account(&self, service: &str, resource_arn: &str) -> Option<String> {
        if !service.eq_ignore_ascii_case("s3") {
            return None;
        }
        // S3 ARNs carry no account, so resolve the bucket's owner from state.
        // This lets the dispatcher detect cross-account access (account A
        // reaching account B's bucket) and require B's bucket policy to grant
        // it, instead of falling back to the caller's account and treating it
        // as same-account (bug-audit 2026-05-28, 5.3).
        let bucket_name = parse_bucket_name(resource_arn)?;
        let mas = self.state.read();
        mas.find_account(|s| s.buckets.contains_key(bucket_name))
            .map(|a| a.to_string())
    }
}

/// Extract the bucket name from an S3 ARN.
///
/// Valid inputs look like `arn:aws:s3:::bucket` or
/// `arn:aws:s3:::bucket/key/with/slashes`. The account and region
/// fields on an S3 ARN are always empty — that's a real AWS quirk, not
/// a bug in the fakecloud parser. Malformed input returns `None`; the
/// caller treats that as "no resource policy attached," which falls
/// through to identity-only evaluation rather than silently allowing.
fn parse_bucket_name(arn: &str) -> Option<&str> {
    let rest = arn.strip_prefix("arn:aws:s3:::")?;
    if rest.is_empty() {
        return None;
    }
    let bucket = rest.split('/').next()?;
    if bucket.is_empty() {
        None
    } else {
        Some(bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{S3Bucket, S3State};
    use parking_lot::RwLock;

    fn state_with_bucket(name: &str, policy: Option<&str>) -> SharedS3State {
        let mut mas: fakecloud_core::multi_account::MultiAccountState<S3State> =
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", "");
        let s = mas.get_or_create("123456789012");
        let mut b = S3Bucket::new(name, "us-east-1", "owner");
        b.policy = policy.map(|p| p.to_string());
        s.buckets.insert(name.to_string(), b);
        Arc::new(RwLock::new(mas))
    }

    #[test]
    fn parse_bucket_name_extracts_bucket_from_valid_arns() {
        assert_eq!(
            parse_bucket_name("arn:aws:s3:::my-bucket"),
            Some("my-bucket")
        );
        assert_eq!(
            parse_bucket_name("arn:aws:s3:::my-bucket/some/key"),
            Some("my-bucket")
        );
    }

    #[test]
    fn parse_bucket_name_rejects_malformed() {
        assert_eq!(parse_bucket_name(""), None);
        assert_eq!(parse_bucket_name("arn:aws:s3:::"), None);
        assert_eq!(parse_bucket_name("arn:aws:s3:::/key"), None);
        assert_eq!(parse_bucket_name("arn:aws:sqs:us-east-1:123:q"), None);
        assert_eq!(parse_bucket_name("not-an-arn"), None);
    }

    #[test]
    fn returns_stored_policy_for_s3_arn() {
        let policy_json = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let state = state_with_bucket("mybucket", Some(policy_json));
        let provider = S3ResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("s3", "arn:aws:s3:::mybucket"),
            Some(policy_json.to_string())
        );
    }

    #[test]
    fn returns_stored_policy_for_arn_with_object_key() {
        let policy_json = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let state = state_with_bucket("mybucket", Some(policy_json));
        let provider = S3ResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("s3", "arn:aws:s3:::mybucket/path/to/object.txt"),
            Some(policy_json.to_string())
        );
    }

    #[test]
    fn returns_none_when_bucket_exists_but_no_policy_attached() {
        let state = state_with_bucket("mybucket", None);
        let provider = S3ResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("s3", "arn:aws:s3:::mybucket"),
            None
        );
    }

    #[test]
    fn resource_owner_account_resolves_bucket_owner() {
        // bug-audit 5.3: a bucket owned by account B must report B as its
        // owner so the dispatcher detects cross-account access from account A
        // and requires B's bucket policy to grant it.
        let mut mas: fakecloud_core::multi_account::MultiAccountState<S3State> =
            fakecloud_core::multi_account::MultiAccountState::new("111111111111", "us-east-1", "");
        let s = mas.get_or_create("222222222222");
        s.buckets.insert(
            "acct-b-bucket".to_string(),
            S3Bucket::new("acct-b-bucket", "us-east-1", "owner"),
        );
        let provider = S3ResourcePolicyProvider::new(Arc::new(RwLock::new(mas)));
        assert_eq!(
            provider.resource_owner_account("s3", "arn:aws:s3:::acct-b-bucket"),
            Some("222222222222".to_string())
        );
    }

    #[test]
    fn resource_owner_account_none_for_unknown_bucket() {
        let state = state_with_bucket("mybucket", None);
        let provider = S3ResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_owner_account("s3", "arn:aws:s3:::ghost"),
            None
        );
    }

    #[test]
    fn resource_owner_account_none_for_non_s3_service() {
        let state = state_with_bucket("mybucket", None);
        let provider = S3ResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_owner_account("sqs", "arn:aws:s3:::mybucket"),
            None
        );
    }

    #[test]
    fn returns_none_when_bucket_missing() {
        let state = state_with_bucket("other", Some("{}"));
        let provider = S3ResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("s3", "arn:aws:s3:::mybucket"),
            None
        );
    }

    #[test]
    fn returns_none_for_non_s3_service_prefix() {
        let state = state_with_bucket("mybucket", Some("{}"));
        let provider = S3ResourcePolicyProvider::new(state);
        assert_eq!(
            provider.resource_policy("sns", "arn:aws:s3:::mybucket"),
            None
        );
        assert_eq!(
            provider.resource_policy("sqs", "arn:aws:s3:::mybucket"),
            None
        );
    }

    #[test]
    fn service_prefix_match_is_case_insensitive() {
        let state = state_with_bucket("mybucket", Some("{}"));
        let provider = S3ResourcePolicyProvider::new(state);
        assert!(provider
            .resource_policy("S3", "arn:aws:s3:::mybucket")
            .is_some());
    }

    #[test]
    fn returns_none_for_malformed_arn() {
        let state = state_with_bucket("mybucket", Some("{}"));
        let provider = S3ResourcePolicyProvider::new(state);
        assert_eq!(provider.resource_policy("s3", "not-an-arn"), None);
        assert_eq!(provider.resource_policy("s3", ""), None);
        assert_eq!(provider.resource_policy("s3", "arn:aws:s3:::"), None);
    }
}
