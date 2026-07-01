//! Cross-service tag index backing the Resource Groups Tagging API.
//!
//! AWS's Resource Groups Tagging API (`tagging` endpoint) reports the tags of
//! every resource across every service in an account/region. Rather than
//! dual-writing tags into a second store (which drifts from each service's own
//! authoritative tag state), services expose their live taggable resources
//! through a [`TagProvider`]. The tagging API aggregates every registered
//! provider at read time, so results always reflect current service state.
//!
//! The tagging API additionally keeps its own store (see the
//! `fakecloud-resource-groups-tagging` crate) for `TagResources` /
//! `UntagResources` calls that target arbitrary ARNs which no modelled service
//! owns — AWS accepts those too.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

/// A resource discovered from a service's live state, with its tags.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaggedResource {
    /// The resource ARN. Its embedded region is used for region filtering.
    pub arn: String,
    /// AWS tagging-API resource type, `service:resourceType`
    /// (e.g. `s3:bucket`, `ec2:instance`, `dynamodb:table`). Empty when the
    /// owning service has no meaningful sub-type.
    pub resource_type: String,
    /// The resource's tags, key -> value.
    pub tags: BTreeMap<String, String>,
}

impl TaggedResource {
    pub fn new(
        arn: impl Into<String>,
        resource_type: impl Into<String>,
        tags: BTreeMap<String, String>,
    ) -> Self {
        Self {
            arn: arn.into(),
            resource_type: resource_type.into(),
            tags,
        }
    }

    /// Region parsed from the ARN (`arn:partition:service:region:...`).
    /// Empty for global resources (IAM, S3, Route 53, ...).
    pub fn region(&self) -> &str {
        self.arn.split(':').nth(3).unwrap_or("")
    }
}

/// A service that can enumerate its taggable resources for an account.
/// Implemented by each service's shared-state handle so the tagging API can
/// aggregate tags across every service without a lossy second copy.
pub trait TagProvider: Send + Sync {
    fn tagged_resources(&self, account_id: &str) -> Vec<TaggedResource>;
}

/// Registry of every service's [`TagProvider`]. Both the Resource Groups
/// Tagging API and Resource Groups tag-query resolution read through this.
#[derive(Clone, Default)]
pub struct TagProviderRegistry {
    providers: Arc<RwLock<Vec<Arc<dyn TagProvider>>>>,
}

impl TagProviderRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a service's provider. Idempotent per unique `Arc` only in the
    /// sense that duplicate registrations would double-list; callers register
    /// exactly once at server startup.
    pub fn register(&self, provider: Arc<dyn TagProvider>) {
        self.providers.write().push(provider);
    }

    /// All taggable resources across every registered service for `account_id`,
    /// optionally filtered to `region` (global resources — empty ARN region —
    /// are always included). Pass `None` for `region` to include everything.
    pub fn resources(&self, account_id: &str, region: Option<&str>) -> Vec<TaggedResource> {
        self.providers
            .read()
            .iter()
            .flat_map(|p| p.tagged_resources(account_id))
            .filter(|r| match region {
                Some(want) => r.region().is_empty() || r.region() == want,
                None => true,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(Vec<TaggedResource>);
    impl TagProvider for Fixed {
        fn tagged_resources(&self, _account_id: &str) -> Vec<TaggedResource> {
            self.0.clone()
        }
    }

    fn res(arn: &str) -> TaggedResource {
        TaggedResource::new(arn, "svc:type", BTreeMap::new())
    }

    #[test]
    fn region_parsed_from_arn() {
        assert_eq!(
            res("arn:aws:ec2:us-east-1:123456789012:instance/i-1").region(),
            "us-east-1"
        );
        assert_eq!(res("arn:aws:s3:::my-bucket").region(), "");
    }

    #[test]
    fn registry_aggregates_and_filters_region() {
        let reg = TagProviderRegistry::new();
        reg.register(Arc::new(Fixed(vec![
            res("arn:aws:ec2:us-east-1:123456789012:instance/i-1"),
            res("arn:aws:ec2:us-west-2:123456789012:instance/i-2"),
            res("arn:aws:s3:::global-bucket"),
        ])));
        assert_eq!(reg.resources("123456789012", None).len(), 3);
        // us-east-1 + the global S3 bucket, not the us-west-2 instance.
        let east = reg.resources("123456789012", Some("us-east-1"));
        assert_eq!(east.len(), 2);
        assert!(east.iter().any(|r| r.arn.ends_with("global-bucket")));
    }
}
