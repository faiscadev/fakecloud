//! HTTP method + URI to action routing for CloudFront's REST-XML API.
//!
//! Every CloudFront operation is keyed off `(Method, segments[..])`. The
//! returned [`Route`] carries the operation name, any captured path
//! parameters (e.g. distribution `Id`), and a flag for the
//! `WithTags`-style query toggle so handlers don't have to re-parse it.

use http::Method;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub action: &'static str,
    pub id: Option<String>,
    pub second_id: Option<String>,
    pub with_tags: bool,
}

impl Route {
    fn just(action: &'static str) -> Self {
        Self {
            action,
            id: None,
            second_id: None,
            with_tags: false,
        }
    }

    fn with_id(action: &'static str, id: &str) -> Self {
        Self {
            action,
            id: Some(id.to_string()),
            second_id: None,
            with_tags: false,
        }
    }

    fn with_two(action: &'static str, id: &str, second: &str) -> Self {
        Self {
            action,
            id: Some(id.to_string()),
            second_id: Some(second.to_string()),
            with_tags: false,
        }
    }

    fn flag_with_tags(mut self) -> Self {
        self.with_tags = true;
        self
    }
}

pub fn route(method: &Method, path: &str, raw_query: &str) -> Option<Route> {
    let path = path.strip_prefix("/2020-05-31").unwrap_or(path);
    let path = path.trim_start_matches('/');
    // Filter out empty segments so a trailing slash (`/distribution/`, which
    // botocore/AWS CLI < 1.40 and curl emit for a collection root) routes to the
    // List op, not GetDistribution(id="") -> NoSuchDistribution (the #1645
    // shape; bug-audit 2026-06-20, 1.1).
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    let q = QueryFlags::parse(raw_query);
    match (method, segs.as_slice()) {
        // ─── Distributions ──────────────────────────────────────────
        (&Method::POST, ["distribution"]) if q.with_tags => {
            Some(Route::just("CreateDistributionWithTags").flag_with_tags())
        }
        (&Method::POST, ["distribution"]) => Some(Route::just("CreateDistribution")),
        (&Method::GET, ["distribution"]) => Some(Route::just("ListDistributions")),
        (&Method::GET, ["distribution", id]) => Some(Route::with_id("GetDistribution", id)),
        (&Method::GET, ["distribution", id, "config"]) => {
            Some(Route::with_id("GetDistributionConfig", id))
        }
        (&Method::PUT, ["distribution", id, "config"]) => {
            Some(Route::with_id("UpdateDistribution", id))
        }
        (&Method::PUT, ["distribution", id, "promote-staging-config"]) => {
            Some(Route::with_id("UpdateDistributionWithStagingConfig", id))
        }
        (&Method::DELETE, ["distribution", id]) => Some(Route::with_id("DeleteDistribution", id)),
        (&Method::POST, ["distribution", id, "copy"]) => {
            Some(Route::with_id("CopyDistribution", id))
        }
        (&Method::PUT, ["distribution", id, "associate-alias"]) => {
            Some(Route::with_id("AssociateAlias", id))
        }
        (&Method::PUT, ["distribution", id, "associate-web-acl"]) => {
            Some(Route::with_id("AssociateDistributionWebACL", id))
        }
        (&Method::PUT, ["distribution", id, "disassociate-web-acl"]) => {
            Some(Route::with_id("DisassociateDistributionWebACL", id))
        }

        // ─── Distributions-by-X listings ────────────────────────────
        (&Method::GET, ["distributionsByCachePolicyId", id]) => {
            Some(Route::with_id("ListDistributionsByCachePolicyId", id))
        }
        (&Method::GET, ["distributionsByOriginRequestPolicyId", id]) => Some(Route::with_id(
            "ListDistributionsByOriginRequestPolicyId",
            id,
        )),
        (&Method::GET, ["distributionsByResponseHeadersPolicyId", id]) => Some(Route::with_id(
            "ListDistributionsByResponseHeadersPolicyId",
            id,
        )),
        (&Method::GET, ["distributionsByKeyGroupId", id]) => {
            Some(Route::with_id("ListDistributionsByKeyGroup", id))
        }
        (&Method::GET, ["distributionsByWebACLId", id]) => {
            Some(Route::with_id("ListDistributionsByWebACLId", id))
        }
        (&Method::GET, ["distributionsByVpcOriginId", id]) => {
            Some(Route::with_id("ListDistributionsByVpcOriginId", id))
        }
        (&Method::GET, ["distributionsByAnycastIpListId", id]) => {
            Some(Route::with_id("ListDistributionsByAnycastIpListId", id))
        }
        (&Method::GET, ["distributionsByConnectionMode", id]) => {
            Some(Route::with_id("ListDistributionsByConnectionMode", id))
        }
        (&Method::GET, ["distributionsByConnectionFunction"]) => {
            Some(Route::just("ListDistributionsByConnectionFunction"))
        }
        (&Method::GET, ["distributionsByOwnedResource", arn]) => {
            Some(Route::with_id("ListDistributionsByOwnedResource", arn))
        }
        (&Method::GET, ["distributionsByTrustStore"]) => {
            Some(Route::just("ListDistributionsByTrustStore"))
        }
        (&Method::POST, ["distributionsByRealtimeLogConfig"]) => {
            Some(Route::just("ListDistributionsByRealtimeLogConfig"))
        }
        (&Method::GET, ["conflicting-alias"]) => Some(Route::just("ListConflictingAliases")),

        // ─── Invalidations ──────────────────────────────────────────
        (&Method::POST, ["distribution", dist, "invalidation"]) => {
            Some(Route::with_id("CreateInvalidation", dist))
        }
        (&Method::GET, ["distribution", dist, "invalidation"]) => {
            Some(Route::with_id("ListInvalidations", dist))
        }
        (&Method::GET, ["distribution", dist, "invalidation", id]) => {
            Some(Route::with_two("GetInvalidation", dist, id))
        }

        // ─── Tags ───────────────────────────────────────────────────
        (&Method::GET, ["tagging"]) => Some(Route::just("ListTagsForResource")),
        (&Method::POST, ["tagging"]) if q.tag_op.as_deref() == Some("Tag") => {
            Some(Route::just("TagResource"))
        }
        (&Method::POST, ["tagging"]) if q.tag_op.as_deref() == Some("Untag") => {
            Some(Route::just("UntagResource"))
        }

        // ─── Monitoring Subscription ────────────────────────────────
        (&Method::POST, ["distributions", dist, "monitoring-subscription"]) => {
            Some(Route::with_id("CreateMonitoringSubscription", dist))
        }
        (&Method::GET, ["distributions", dist, "monitoring-subscription"]) => {
            Some(Route::with_id("GetMonitoringSubscription", dist))
        }
        (&Method::DELETE, ["distributions", dist, "monitoring-subscription"]) => {
            Some(Route::with_id("DeleteMonitoringSubscription", dist))
        }

        // ─── Origin Access Control ──────────────────────────────────
        (&Method::POST, ["origin-access-control"]) => {
            Some(Route::just("CreateOriginAccessControl"))
        }
        (&Method::GET, ["origin-access-control"]) => Some(Route::just("ListOriginAccessControls")),
        (&Method::GET, ["origin-access-control", id]) => {
            Some(Route::with_id("GetOriginAccessControl", id))
        }
        (&Method::GET, ["origin-access-control", id, "config"]) => {
            Some(Route::with_id("GetOriginAccessControlConfig", id))
        }
        (&Method::PUT, ["origin-access-control", id, "config"]) => {
            Some(Route::with_id("UpdateOriginAccessControl", id))
        }
        (&Method::DELETE, ["origin-access-control", id]) => {
            Some(Route::with_id("DeleteOriginAccessControl", id))
        }

        // ─── Cache Policy ───────────────────────────────────────────
        (&Method::POST, ["cache-policy"]) => Some(Route::just("CreateCachePolicy")),
        (&Method::GET, ["cache-policy"]) => Some(Route::just("ListCachePolicies")),
        (&Method::GET, ["cache-policy", id]) => Some(Route::with_id("GetCachePolicy", id)),
        (&Method::GET, ["cache-policy", id, "config"]) => {
            Some(Route::with_id("GetCachePolicyConfig", id))
        }
        (&Method::PUT, ["cache-policy", id]) => Some(Route::with_id("UpdateCachePolicy", id)),
        (&Method::DELETE, ["cache-policy", id]) => Some(Route::with_id("DeleteCachePolicy", id)),

        // ─── Origin Request Policy ──────────────────────────────────
        (&Method::POST, ["origin-request-policy"]) => {
            Some(Route::just("CreateOriginRequestPolicy"))
        }
        (&Method::GET, ["origin-request-policy"]) => Some(Route::just("ListOriginRequestPolicies")),
        (&Method::GET, ["origin-request-policy", id]) => {
            Some(Route::with_id("GetOriginRequestPolicy", id))
        }
        (&Method::GET, ["origin-request-policy", id, "config"]) => {
            Some(Route::with_id("GetOriginRequestPolicyConfig", id))
        }
        (&Method::PUT, ["origin-request-policy", id]) => {
            Some(Route::with_id("UpdateOriginRequestPolicy", id))
        }
        (&Method::DELETE, ["origin-request-policy", id]) => {
            Some(Route::with_id("DeleteOriginRequestPolicy", id))
        }

        // ─── Response Headers Policy ────────────────────────────────
        (&Method::POST, ["response-headers-policy"]) => {
            Some(Route::just("CreateResponseHeadersPolicy"))
        }
        (&Method::GET, ["response-headers-policy"]) => {
            Some(Route::just("ListResponseHeadersPolicies"))
        }
        (&Method::GET, ["response-headers-policy", id]) => {
            Some(Route::with_id("GetResponseHeadersPolicy", id))
        }
        (&Method::GET, ["response-headers-policy", id, "config"]) => {
            Some(Route::with_id("GetResponseHeadersPolicyConfig", id))
        }
        (&Method::PUT, ["response-headers-policy", id]) => {
            Some(Route::with_id("UpdateResponseHeadersPolicy", id))
        }
        (&Method::DELETE, ["response-headers-policy", id]) => {
            Some(Route::with_id("DeleteResponseHeadersPolicy", id))
        }

        // ─── Continuous Deployment Policy ───────────────────────────
        (&Method::POST, ["continuous-deployment-policy"]) => {
            Some(Route::just("CreateContinuousDeploymentPolicy"))
        }
        (&Method::GET, ["continuous-deployment-policy"]) => {
            Some(Route::just("ListContinuousDeploymentPolicies"))
        }
        (&Method::GET, ["continuous-deployment-policy", id]) => {
            Some(Route::with_id("GetContinuousDeploymentPolicy", id))
        }
        (&Method::GET, ["continuous-deployment-policy", id, "config"]) => {
            Some(Route::with_id("GetContinuousDeploymentPolicyConfig", id))
        }
        (&Method::PUT, ["continuous-deployment-policy", id]) => {
            Some(Route::with_id("UpdateContinuousDeploymentPolicy", id))
        }
        (&Method::DELETE, ["continuous-deployment-policy", id]) => {
            Some(Route::with_id("DeleteContinuousDeploymentPolicy", id))
        }

        // ─── CloudFront Functions ───────────────────────────────────
        (&Method::POST, ["function"]) => Some(Route::just("CreateFunction")),
        (&Method::GET, ["function"]) => Some(Route::just("ListFunctions")),
        (&Method::GET, ["function", name]) => Some(Route::with_id("GetFunction", name)),
        (&Method::GET, ["function", name, "describe"]) => {
            Some(Route::with_id("DescribeFunction", name))
        }
        (&Method::PUT, ["function", name]) => Some(Route::with_id("UpdateFunction", name)),
        (&Method::DELETE, ["function", name]) => Some(Route::with_id("DeleteFunction", name)),
        (&Method::POST, ["function", name, "publish"]) => {
            Some(Route::with_id("PublishFunction", name))
        }
        (&Method::POST, ["function", name, "test"]) => Some(Route::with_id("TestFunction", name)),

        // ─── Public Keys ────────────────────────────────────────────
        (&Method::POST, ["public-key"]) => Some(Route::just("CreatePublicKey")),
        (&Method::GET, ["public-key"]) => Some(Route::just("ListPublicKeys")),
        (&Method::GET, ["public-key", id]) => Some(Route::with_id("GetPublicKey", id)),
        (&Method::GET, ["public-key", id, "config"]) => {
            Some(Route::with_id("GetPublicKeyConfig", id))
        }
        (&Method::PUT, ["public-key", id, "config"]) => Some(Route::with_id("UpdatePublicKey", id)),
        (&Method::DELETE, ["public-key", id]) => Some(Route::with_id("DeletePublicKey", id)),

        // ─── Key Groups ─────────────────────────────────────────────
        (&Method::POST, ["key-group"]) => Some(Route::just("CreateKeyGroup")),
        (&Method::GET, ["key-group"]) => Some(Route::just("ListKeyGroups")),
        (&Method::GET, ["key-group", id]) => Some(Route::with_id("GetKeyGroup", id)),
        (&Method::GET, ["key-group", id, "config"]) => {
            Some(Route::with_id("GetKeyGroupConfig", id))
        }
        (&Method::PUT, ["key-group", id]) => Some(Route::with_id("UpdateKeyGroup", id)),
        (&Method::DELETE, ["key-group", id]) => Some(Route::with_id("DeleteKeyGroup", id)),

        // ─── Key Value Stores ───────────────────────────────────────
        (&Method::POST, ["key-value-store"]) => Some(Route::just("CreateKeyValueStore")),
        (&Method::GET, ["key-value-store"]) => Some(Route::just("ListKeyValueStores")),
        (&Method::GET, ["key-value-store", name]) => {
            Some(Route::with_id("DescribeKeyValueStore", name))
        }
        (&Method::PUT, ["key-value-store", name]) => {
            Some(Route::with_id("UpdateKeyValueStore", name))
        }
        (&Method::DELETE, ["key-value-store", name]) => {
            Some(Route::with_id("DeleteKeyValueStore", name))
        }

        // ─── Origin Access Identity (legacy) ────────────────────────
        (&Method::POST, ["origin-access-identity", "cloudfront"]) => {
            Some(Route::just("CreateCloudFrontOriginAccessIdentity"))
        }
        (&Method::GET, ["origin-access-identity", "cloudfront"]) => {
            Some(Route::just("ListCloudFrontOriginAccessIdentities"))
        }
        (&Method::GET, ["origin-access-identity", "cloudfront", id]) => {
            Some(Route::with_id("GetCloudFrontOriginAccessIdentity", id))
        }
        (&Method::GET, ["origin-access-identity", "cloudfront", id, "config"]) => Some(
            Route::with_id("GetCloudFrontOriginAccessIdentityConfig", id),
        ),
        (&Method::PUT, ["origin-access-identity", "cloudfront", id, "config"]) => {
            Some(Route::with_id("UpdateCloudFrontOriginAccessIdentity", id))
        }
        (&Method::DELETE, ["origin-access-identity", "cloudfront", id]) => {
            Some(Route::with_id("DeleteCloudFrontOriginAccessIdentity", id))
        }

        // ─── Streaming Distribution (legacy) ────────────────────────
        (&Method::POST, ["streaming-distribution"]) if q.with_tags => {
            Some(Route::just("CreateStreamingDistributionWithTags").flag_with_tags())
        }
        (&Method::POST, ["streaming-distribution"]) => {
            Some(Route::just("CreateStreamingDistribution"))
        }
        (&Method::GET, ["streaming-distribution"]) => {
            Some(Route::just("ListStreamingDistributions"))
        }
        (&Method::GET, ["streaming-distribution", id]) => {
            Some(Route::with_id("GetStreamingDistribution", id))
        }
        (&Method::GET, ["streaming-distribution", id, "config"]) => {
            Some(Route::with_id("GetStreamingDistributionConfig", id))
        }
        (&Method::PUT, ["streaming-distribution", id, "config"]) => {
            Some(Route::with_id("UpdateStreamingDistribution", id))
        }
        (&Method::DELETE, ["streaming-distribution", id]) => {
            Some(Route::with_id("DeleteStreamingDistribution", id))
        }

        // ─── Field-Level Encryption ─────────────────────────────────
        (&Method::POST, ["field-level-encryption"]) => {
            Some(Route::just("CreateFieldLevelEncryptionConfig"))
        }
        (&Method::GET, ["field-level-encryption"]) => {
            Some(Route::just("ListFieldLevelEncryptionConfigs"))
        }
        (&Method::GET, ["field-level-encryption", id]) => {
            Some(Route::with_id("GetFieldLevelEncryption", id))
        }
        (&Method::GET, ["field-level-encryption", id, "config"]) => {
            Some(Route::with_id("GetFieldLevelEncryptionConfig", id))
        }
        (&Method::PUT, ["field-level-encryption", id, "config"]) => {
            Some(Route::with_id("UpdateFieldLevelEncryptionConfig", id))
        }
        (&Method::DELETE, ["field-level-encryption", id]) => {
            Some(Route::with_id("DeleteFieldLevelEncryptionConfig", id))
        }
        (&Method::POST, ["field-level-encryption-profile"]) => {
            Some(Route::just("CreateFieldLevelEncryptionProfile"))
        }
        (&Method::GET, ["field-level-encryption-profile"]) => {
            Some(Route::just("ListFieldLevelEncryptionProfiles"))
        }
        (&Method::GET, ["field-level-encryption-profile", id]) => {
            Some(Route::with_id("GetFieldLevelEncryptionProfile", id))
        }
        (&Method::GET, ["field-level-encryption-profile", id, "config"]) => {
            Some(Route::with_id("GetFieldLevelEncryptionProfileConfig", id))
        }
        (&Method::PUT, ["field-level-encryption-profile", id, "config"]) => {
            Some(Route::with_id("UpdateFieldLevelEncryptionProfile", id))
        }
        (&Method::DELETE, ["field-level-encryption-profile", id]) => {
            Some(Route::with_id("DeleteFieldLevelEncryptionProfile", id))
        }

        // ─── Real-time Log Configs ──────────────────────────────────
        (&Method::POST, ["realtime-log-config"]) => Some(Route::just("CreateRealtimeLogConfig")),
        (&Method::GET, ["realtime-log-config"]) => Some(Route::just("ListRealtimeLogConfigs")),
        (&Method::PUT, ["realtime-log-config"]) => Some(Route::just("UpdateRealtimeLogConfig")),
        (&Method::POST, ["get-realtime-log-config"]) => Some(Route::just("GetRealtimeLogConfig")),
        (&Method::POST, ["delete-realtime-log-config"]) => {
            Some(Route::just("DeleteRealtimeLogConfig"))
        }

        // ─── Resource Policy ────────────────────────────────────────
        (&Method::POST, ["get-resource-policy"]) => Some(Route::just("GetResourcePolicy")),
        (&Method::POST, ["put-resource-policy"]) => Some(Route::just("PutResourcePolicy")),
        (&Method::POST, ["delete-resource-policy"]) => Some(Route::just("DeleteResourcePolicy")),

        // ─── VPC Origins ────────────────────────────────────────────
        (&Method::POST, ["vpc-origin"]) => Some(Route::just("CreateVpcOrigin")),
        (&Method::GET, ["vpc-origin"]) => Some(Route::just("ListVpcOrigins")),
        (&Method::GET, ["vpc-origin", id]) => Some(Route::with_id("GetVpcOrigin", id)),
        (&Method::PUT, ["vpc-origin", id]) => Some(Route::with_id("UpdateVpcOrigin", id)),
        (&Method::DELETE, ["vpc-origin", id]) => Some(Route::with_id("DeleteVpcOrigin", id)),

        // ─── Anycast IP Lists ───────────────────────────────────────
        (&Method::POST, ["anycast-ip-list"]) => Some(Route::just("CreateAnycastIpList")),
        (&Method::GET, ["anycast-ip-list"]) => Some(Route::just("ListAnycastIpLists")),
        (&Method::GET, ["anycast-ip-list", id]) => Some(Route::with_id("GetAnycastIpList", id)),
        (&Method::PUT, ["anycast-ip-list", id]) => Some(Route::with_id("UpdateAnycastIpList", id)),
        (&Method::DELETE, ["anycast-ip-list", id]) => {
            Some(Route::with_id("DeleteAnycastIpList", id))
        }

        // ─── Trust Stores ───────────────────────────────────────────
        (&Method::POST, ["trust-store"]) => Some(Route::just("CreateTrustStore")),
        (&Method::POST, ["trust-stores"]) => Some(Route::just("ListTrustStores")),
        (&Method::GET, ["trust-store", id]) => Some(Route::with_id("GetTrustStore", id)),
        (&Method::PUT, ["trust-store", id]) => Some(Route::with_id("UpdateTrustStore", id)),
        (&Method::DELETE, ["trust-store", id]) => Some(Route::with_id("DeleteTrustStore", id)),

        // ─── Distribution Tenants ───────────────────────────────────
        (&Method::POST, ["distribution-tenant"]) => Some(Route::just("CreateDistributionTenant")),
        (&Method::GET, ["distribution-tenant"]) => {
            Some(Route::just("GetDistributionTenantByDomain"))
        }
        (&Method::GET, ["distribution-tenant", id]) => {
            Some(Route::with_id("GetDistributionTenant", id))
        }
        (&Method::PUT, ["distribution-tenant", id]) => {
            Some(Route::with_id("UpdateDistributionTenant", id))
        }
        (&Method::DELETE, ["distribution-tenant", id]) => {
            Some(Route::with_id("DeleteDistributionTenant", id))
        }
        (&Method::POST, ["distribution-tenants"]) => Some(Route::just("ListDistributionTenants")),
        (&Method::POST, ["distribution-tenants-by-customization"]) => {
            Some(Route::just("ListDistributionTenantsByCustomization"))
        }
        (&Method::PUT, ["distribution-tenant", id, "associate-web-acl"]) => {
            Some(Route::with_id("AssociateDistributionTenantWebACL", id))
        }
        (&Method::PUT, ["distribution-tenant", id, "disassociate-web-acl"]) => {
            Some(Route::with_id("DisassociateDistributionTenantWebACL", id))
        }
        (&Method::POST, ["distribution-tenant", dist, "invalidation"]) => Some(Route::with_id(
            "CreateInvalidationForDistributionTenant",
            dist,
        )),
        (&Method::GET, ["distribution-tenant", dist, "invalidation"]) => Some(Route::with_id(
            "ListInvalidationsForDistributionTenant",
            dist,
        )),
        (&Method::GET, ["distribution-tenant", dist, "invalidation", id]) => Some(Route::with_two(
            "GetInvalidationForDistributionTenant",
            dist,
            id,
        )),
        (&Method::POST, ["domain-association"]) => Some(Route::just("UpdateDomainAssociation")),
        (&Method::POST, ["domain-conflicts"]) => Some(Route::just("ListDomainConflicts")),
        (&Method::POST, ["verify-dns-configuration"]) => {
            Some(Route::just("VerifyDnsConfiguration"))
        }
        (&Method::GET, ["managed-certificate", id]) => {
            Some(Route::with_id("GetManagedCertificateDetails", id))
        }

        // ─── Connection Functions / Groups ──────────────────────────
        (&Method::POST, ["connection-function"]) => Some(Route::just("CreateConnectionFunction")),
        (&Method::POST, ["connection-functions"]) => Some(Route::just("ListConnectionFunctions")),
        (&Method::GET, ["connection-function", id]) => {
            Some(Route::with_id("GetConnectionFunction", id))
        }
        (&Method::GET, ["connection-function", id, "describe"]) => {
            Some(Route::with_id("DescribeConnectionFunction", id))
        }
        (&Method::PUT, ["connection-function", id]) => {
            Some(Route::with_id("UpdateConnectionFunction", id))
        }
        (&Method::DELETE, ["connection-function", id]) => {
            Some(Route::with_id("DeleteConnectionFunction", id))
        }
        (&Method::POST, ["connection-function", id, "publish"]) => {
            Some(Route::with_id("PublishConnectionFunction", id))
        }
        (&Method::POST, ["connection-function", id, "test"]) => {
            Some(Route::with_id("TestConnectionFunction", id))
        }
        (&Method::POST, ["connection-group"]) => Some(Route::just("CreateConnectionGroup")),
        (&Method::POST, ["connection-groups"]) => Some(Route::just("ListConnectionGroups")),
        (&Method::GET, ["connection-group"]) => {
            Some(Route::just("GetConnectionGroupByRoutingEndpoint"))
        }
        (&Method::GET, ["connection-group", id]) => Some(Route::with_id("GetConnectionGroup", id)),
        (&Method::PUT, ["connection-group", id]) => {
            Some(Route::with_id("UpdateConnectionGroup", id))
        }
        (&Method::DELETE, ["connection-group", id]) => {
            Some(Route::with_id("DeleteConnectionGroup", id))
        }

        _ => None,
    }
}

#[derive(Default, Debug)]
struct QueryFlags {
    with_tags: bool,
    tag_op: Option<String>,
}

impl QueryFlags {
    fn parse(raw: &str) -> Self {
        let mut out = QueryFlags::default();
        for pair in raw.split('&').filter(|p| !p.is_empty()) {
            if pair == "WithTags" || pair.starts_with("WithTags=") {
                out.with_tags = true;
                continue;
            }
            if let Some(rest) = pair.strip_prefix("Operation=") {
                let value = decode(rest);
                out.tag_op = Some(value);
            }
        }
        out
    }
}

fn decode(s: &str) -> String {
    // Minimal percent-decode (CloudFront only sends ASCII operation values).
    let mut out = String::with_capacity(s.len());
    let mut bytes = s.bytes();
    while let Some(b) = bytes.next() {
        if b == b'%' {
            let h1 = bytes.next();
            let h2 = bytes.next();
            if let (Some(a), Some(b2)) = (h1, h2) {
                if let (Some(a), Some(b2)) = (hex(a), hex(b2)) {
                    out.push(((a << 4) | b2) as char);
                    continue;
                }
            }
            out.push('%');
        } else if b == b'+' {
            out.push(' ');
        } else {
            out.push(b as char);
        }
    }
    out
}

fn hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_distribution() {
        let r = route(&Method::POST, "/2020-05-31/distribution", "").unwrap();
        assert_eq!(r.action, "CreateDistribution");
    }

    #[test]
    fn create_distribution_with_tags() {
        let r = route(&Method::POST, "/2020-05-31/distribution", "WithTags").unwrap();
        assert_eq!(r.action, "CreateDistributionWithTags");
        assert!(r.with_tags);
    }

    #[test]
    fn get_distribution() {
        let r = route(&Method::GET, "/2020-05-31/distribution/EDFDVBD632BHDS5", "").unwrap();
        assert_eq!(r.action, "GetDistribution");
        assert_eq!(r.id.as_deref(), Some("EDFDVBD632BHDS5"));
    }

    #[test]
    fn trailing_slash_collection_routes_to_list() {
        // A trailing slash on the collection root must list, not
        // GetDistribution(id="") -> NoSuchDistribution (1.1).
        let r = route(&Method::GET, "/2020-05-31/distribution/", "").unwrap();
        assert_eq!(r.action, "ListDistributions");
    }

    #[test]
    fn create_invalidation() {
        let r = route(
            &Method::POST,
            "/2020-05-31/distribution/EDFDVBD632BHDS5/invalidation",
            "",
        )
        .unwrap();
        assert_eq!(r.action, "CreateInvalidation");
        assert_eq!(r.id.as_deref(), Some("EDFDVBD632BHDS5"));
    }

    #[test]
    fn get_invalidation() {
        let r = route(
            &Method::GET,
            "/2020-05-31/distribution/EDFDVBD632BHDS5/invalidation/IDFDVBD632BHDS5",
            "",
        )
        .unwrap();
        assert_eq!(r.action, "GetInvalidation");
        assert_eq!(r.id.as_deref(), Some("EDFDVBD632BHDS5"));
        assert_eq!(r.second_id.as_deref(), Some("IDFDVBD632BHDS5"));
    }

    #[test]
    fn tag_resource() {
        let r = route(
            &Method::POST,
            "/2020-05-31/tagging",
            "Operation=Tag&Resource=arn:aws:cloudfront",
        )
        .unwrap();
        assert_eq!(r.action, "TagResource");
    }

    #[test]
    fn untag_resource() {
        let r = route(
            &Method::POST,
            "/2020-05-31/tagging",
            "Operation=Untag&Resource=arn:aws:cloudfront",
        )
        .unwrap();
        assert_eq!(r.action, "UntagResource");
    }

    #[test]
    fn list_tags() {
        let r = route(
            &Method::GET,
            "/2020-05-31/tagging",
            "Resource=arn:aws:cloudfront",
        )
        .unwrap();
        assert_eq!(r.action, "ListTagsForResource");
    }

    #[test]
    fn unknown_returns_none() {
        assert!(route(&Method::GET, "/2020-05-31/totally-bogus", "").is_none());
    }
}
