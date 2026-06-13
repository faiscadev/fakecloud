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

    fn public_acl_allows(&self, service: &str, resource_arn: &str, action: &str) -> bool {
        if !service.eq_ignore_ascii_case("s3") {
            return false;
        }
        // Which canned grant satisfies the action: object-read actions need
        // an object (or bucket) ACL; bucket-list actions need a bucket ACL.
        let needs_object = match action {
            "GetObject" | "GetObjectVersion" | "HeadObject" => true,
            "ListBucket" | "ListObjects" | "ListObjectsV2" | "ListObjectVersions" => false,
            // Other actions are not granted anonymously via a public-read ACL.
            _ => return false,
        };
        let Some(bucket_name) = parse_bucket_name(resource_arn) else {
            return false;
        };
        let mas = self.state.read();
        let Some(acct) = mas.find_account(|s| s.buckets.contains_key(bucket_name)) else {
            return false;
        };
        let Some(state) = mas.get(acct) else {
            return false;
        };
        let Some(bucket) = state.buckets.get(bucket_name) else {
            return false;
        };
        // PublicAccessBlock.IgnorePublicAcls makes public ACLs inert, so a
        // bucket with it set is never publicly readable via ACL — matching
        // the read-path gate in objects/read.rs and AWS behavior.
        if let Some(xml) = bucket.public_access_block.as_ref() {
            if crate::service::config::PublicAccessBlockFlags::parse(xml).ignore_public_acls {
                return false;
            }
        }
        if needs_object {
            let key = object_key(resource_arn);
            let object_grants = key.and_then(|k| {
                bucket.objects.get(k).map(|o| &o.acl_grants).or_else(|| {
                    bucket
                        .object_versions
                        .get(k)
                        .and_then(|v| v.last())
                        .map(|o| &o.acl_grants)
                })
            });
            if let Some(grants) = object_grants {
                if grants_all_users_read(grants) {
                    return true;
                }
            }
        }
        // Bucket-level READ grant covers list actions, and also serves as a
        // fallback public-read for object GETs when the bucket ACL is public.
        grants_all_users_read(&bucket.acl_grants)
    }
}

/// Extract the object key (everything after the first `/`) from an S3 ARN
/// like `arn:aws:s3:::bucket/path/to/key`. Returns `None` for bucket-level
/// ARNs that carry no key.
fn object_key(arn: &str) -> Option<&str> {
    let rest = arn.strip_prefix("arn:aws:s3:::")?;
    rest.split_once('/')
        .map(|(_, key)| key)
        .filter(|k| !k.is_empty())
}

/// True iff the grant set grants READ (or FULL_CONTROL) to the `AllUsers`
/// group — the canned `public-read` shape that makes content readable by
/// anonymous callers. `AuthenticatedUsers` is intentionally excluded: it
/// grants to any *authenticated* AWS principal, not to anonymous callers.
fn grants_all_users_read(grants: &[crate::state::AclGrant]) -> bool {
    grants.iter().any(|g| {
        g.grantee_type == "Group"
            && g.grantee_uri
                .as_deref()
                .is_some_and(|u| u.contains("acs.amazonaws.com/groups/global/AllUsers"))
            && matches!(g.permission.as_str(), "READ" | "FULL_CONTROL")
    })
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

    fn public_read_grant() -> crate::state::AclGrant {
        crate::state::AclGrant {
            grantee_type: "Group".to_string(),
            grantee_id: None,
            grantee_display_name: None,
            grantee_uri: Some("http://acs.amazonaws.com/groups/global/AllUsers".to_string()),
            permission: "READ".to_string(),
        }
    }

    /// Build state with a single bucket holding one object, applying the
    /// supplied bucket/object ACL grants and optional PublicAccessBlock XML.
    fn state_with_object(
        bucket: &str,
        key: &str,
        object_grants: Vec<crate::state::AclGrant>,
        bucket_grants: Vec<crate::state::AclGrant>,
        pab: Option<&str>,
    ) -> SharedS3State {
        let mut mas: fakecloud_core::multi_account::MultiAccountState<S3State> =
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", "");
        let s = mas.get_or_create("123456789012");
        let mut b = S3Bucket::new(bucket, "us-east-1", "owner");
        b.acl_grants = bucket_grants;
        b.public_access_block = pab.map(|p| p.to_string());
        let mut obj = crate::state::S3Object {
            key: key.to_string(),
            ..Default::default()
        };
        obj.acl_grants = object_grants;
        b.objects.insert(key.to_string(), obj);
        s.buckets.insert(bucket.to_string(), b);
        Arc::new(RwLock::new(mas))
    }

    #[test]
    fn public_acl_allows_get_object_with_public_read_object_acl() {
        let state = state_with_object("b", "k.txt", vec![public_read_grant()], vec![], None);
        let provider = S3ResourcePolicyProvider::new(state);
        assert!(provider.public_acl_allows("s3", "arn:aws:s3:::b/k.txt", "GetObject"));
    }

    #[test]
    fn public_acl_allows_get_object_with_public_read_bucket_acl() {
        let state = state_with_object("b", "k.txt", vec![], vec![public_read_grant()], None);
        let provider = S3ResourcePolicyProvider::new(state);
        assert!(provider.public_acl_allows("s3", "arn:aws:s3:::b/k.txt", "GetObject"));
    }

    #[test]
    fn public_acl_denies_private_object() {
        let state = state_with_object("b", "k.txt", vec![], vec![], None);
        let provider = S3ResourcePolicyProvider::new(state);
        assert!(!provider.public_acl_allows("s3", "arn:aws:s3:::b/k.txt", "GetObject"));
    }

    #[test]
    fn public_acl_ignored_when_public_access_block_set() {
        let pab = "<PublicAccessBlockConfiguration><IgnorePublicAcls>true</IgnorePublicAcls></PublicAccessBlockConfiguration>";
        let state = state_with_object("b", "k.txt", vec![public_read_grant()], vec![], Some(pab));
        let provider = S3ResourcePolicyProvider::new(state);
        assert!(!provider.public_acl_allows("s3", "arn:aws:s3:::b/k.txt", "GetObject"));
    }

    #[test]
    fn public_acl_list_bucket_uses_bucket_acl() {
        let state = state_with_object("b", "k.txt", vec![], vec![public_read_grant()], None);
        let provider = S3ResourcePolicyProvider::new(state);
        assert!(provider.public_acl_allows("s3", "arn:aws:s3:::b", "ListBucket"));
        // A bucket-only public ACL does not expose unrelated write actions.
        assert!(!provider.public_acl_allows("s3", "arn:aws:s3:::b/k.txt", "PutObject"));
    }

    #[test]
    fn public_acl_false_for_non_s3_service() {
        let state = state_with_object("b", "k.txt", vec![public_read_grant()], vec![], None);
        let provider = S3ResourcePolicyProvider::new(state);
        assert!(!provider.public_acl_allows("sqs", "arn:aws:s3:::b/k.txt", "GetObject"));
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
