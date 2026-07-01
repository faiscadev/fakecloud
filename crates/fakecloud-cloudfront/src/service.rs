//! CloudFront REST-XML service implementation.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use http::header::{HeaderName, HeaderValue, ETAG, IF_MATCH, LOCATION};
use http::{HeaderMap, StatusCode};
use parking_lot::RwLock;
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError, ResponseBody};
use fakecloud_persistence::SnapshotStore;

use crate::model::{
    DistributionConfig, DistributionConfigWithTags, InvalidationBatch, TagKeys, Tags as ModelTags,
};
use crate::router::{route, Route};
use crate::state::{
    CloudFrontAccounts, CloudFrontSnapshot, SharedCloudFrontState, StoredDistribution,
    StoredInvalidation, Tag, CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION,
};
use crate::xml_io;

/// CloudFront mutating actions share these prefixes; everything else
/// (`Get*`/`List*`/`Describe*`/`Test*`/`Verify*`) is read-only. Used to decide
/// when a handled request must trigger a persistence snapshot.
fn is_mutating_action(action: &str) -> bool {
    const PREFIXES: &[&str] = &[
        "Create",
        "Update",
        "Delete",
        "Copy",
        "Associate",
        "Disassociate",
        "Tag",
        "Untag",
        "Publish",
        "Put",
    ];
    PREFIXES.iter().any(|p| action.starts_with(p))
}

pub(crate) const DEFAULT_ACCOUNT: &str = "000000000000";

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreateDistribution",
    "CreateDistributionWithTags",
    "GetDistribution",
    "GetDistributionConfig",
    "UpdateDistribution",
    "DeleteDistribution",
    "ListDistributions",
    "CopyDistribution",
    "CreateInvalidation",
    "GetInvalidation",
    "ListInvalidations",
    "TagResource",
    "UntagResource",
    "ListTagsForResource",
    "AssociateAlias",
    "ListConflictingAliases",
    "ListDistributionsByCachePolicyId",
    "ListDistributionsByOriginRequestPolicyId",
    "ListDistributionsByResponseHeadersPolicyId",
    "ListDistributionsByKeyGroup",
    "ListDistributionsByWebACLId",
    "ListDistributionsByVpcOriginId",
    "ListDistributionsByAnycastIpListId",
    "ListDistributionsByConnectionMode",
    "ListDistributionsByConnectionFunction",
    "ListDistributionsByOwnedResource",
    "ListDistributionsByTrustStore",
    "ListDistributionsByRealtimeLogConfig",
    "AssociateDistributionWebACL",
    "DisassociateDistributionWebACL",
    "CreateOriginAccessControl",
    "GetOriginAccessControl",
    "GetOriginAccessControlConfig",
    "UpdateOriginAccessControl",
    "DeleteOriginAccessControl",
    "ListOriginAccessControls",
    "CreateCachePolicy",
    "GetCachePolicy",
    "GetCachePolicyConfig",
    "UpdateCachePolicy",
    "DeleteCachePolicy",
    "ListCachePolicies",
    "CreateOriginRequestPolicy",
    "GetOriginRequestPolicy",
    "GetOriginRequestPolicyConfig",
    "UpdateOriginRequestPolicy",
    "DeleteOriginRequestPolicy",
    "ListOriginRequestPolicies",
    "CreateResponseHeadersPolicy",
    "GetResponseHeadersPolicy",
    "GetResponseHeadersPolicyConfig",
    "UpdateResponseHeadersPolicy",
    "DeleteResponseHeadersPolicy",
    "ListResponseHeadersPolicies",
    "CreateContinuousDeploymentPolicy",
    "GetContinuousDeploymentPolicy",
    "GetContinuousDeploymentPolicyConfig",
    "UpdateContinuousDeploymentPolicy",
    "DeleteContinuousDeploymentPolicy",
    "ListContinuousDeploymentPolicies",
    "CreateFunction",
    "DescribeFunction",
    "GetFunction",
    "UpdateFunction",
    "DeleteFunction",
    "ListFunctions",
    "PublishFunction",
    "TestFunction",
    "CreatePublicKey",
    "GetPublicKey",
    "GetPublicKeyConfig",
    "UpdatePublicKey",
    "DeletePublicKey",
    "ListPublicKeys",
    "CreateKeyGroup",
    "GetKeyGroup",
    "GetKeyGroupConfig",
    "UpdateKeyGroup",
    "DeleteKeyGroup",
    "ListKeyGroups",
    "CreateKeyValueStore",
    "DescribeKeyValueStore",
    "UpdateKeyValueStore",
    "DeleteKeyValueStore",
    "ListKeyValueStores",
    "CreateCloudFrontOriginAccessIdentity",
    "GetCloudFrontOriginAccessIdentity",
    "GetCloudFrontOriginAccessIdentityConfig",
    "UpdateCloudFrontOriginAccessIdentity",
    "DeleteCloudFrontOriginAccessIdentity",
    "ListCloudFrontOriginAccessIdentities",
    "CreateMonitoringSubscription",
    "GetMonitoringSubscription",
    "DeleteMonitoringSubscription",
    "CreateStreamingDistribution",
    "CreateStreamingDistributionWithTags",
    "GetStreamingDistribution",
    "GetStreamingDistributionConfig",
    "UpdateStreamingDistribution",
    "DeleteStreamingDistribution",
    "ListStreamingDistributions",
    "CreateFieldLevelEncryptionConfig",
    "GetFieldLevelEncryption",
    "GetFieldLevelEncryptionConfig",
    "UpdateFieldLevelEncryptionConfig",
    "DeleteFieldLevelEncryptionConfig",
    "ListFieldLevelEncryptionConfigs",
    "CreateFieldLevelEncryptionProfile",
    "GetFieldLevelEncryptionProfile",
    "GetFieldLevelEncryptionProfileConfig",
    "UpdateFieldLevelEncryptionProfile",
    "DeleteFieldLevelEncryptionProfile",
    "ListFieldLevelEncryptionProfiles",
    "CreateRealtimeLogConfig",
    "GetRealtimeLogConfig",
    "UpdateRealtimeLogConfig",
    "DeleteRealtimeLogConfig",
    "ListRealtimeLogConfigs",
    "CreateVpcOrigin",
    "GetVpcOrigin",
    "UpdateVpcOrigin",
    "DeleteVpcOrigin",
    "ListVpcOrigins",
    "CreateAnycastIpList",
    "GetAnycastIpList",
    "UpdateAnycastIpList",
    "DeleteAnycastIpList",
    "ListAnycastIpLists",
    "CreateTrustStore",
    "GetTrustStore",
    "UpdateTrustStore",
    "DeleteTrustStore",
    "ListTrustStores",
    "GetResourcePolicy",
    "PutResourcePolicy",
    "DeleteResourcePolicy",
    "CreateConnectionGroup",
    "GetConnectionGroup",
    "GetConnectionGroupByRoutingEndpoint",
    "UpdateConnectionGroup",
    "DeleteConnectionGroup",
    "ListConnectionGroups",
    "ListDomainConflicts",
    "UpdateDomainAssociation",
    "VerifyDnsConfiguration",
    "GetManagedCertificateDetails",
    "UpdateDistributionWithStagingConfig",
    "CreateDistributionTenant",
    "GetDistributionTenant",
    "GetDistributionTenantByDomain",
    "UpdateDistributionTenant",
    "DeleteDistributionTenant",
    "ListDistributionTenants",
    "ListDistributionTenantsByCustomization",
    "AssociateDistributionTenantWebACL",
    "DisassociateDistributionTenantWebACL",
    "CreateInvalidationForDistributionTenant",
    "GetInvalidationForDistributionTenant",
    "ListInvalidationsForDistributionTenant",
    "CreateConnectionFunction",
    "GetConnectionFunction",
    "DescribeConnectionFunction",
    "UpdateConnectionFunction",
    "DeleteConnectionFunction",
    "ListConnectionFunctions",
    "PublishConnectionFunction",
    "TestConnectionFunction",
];

pub struct CloudFrontService {
    pub(crate) state: SharedCloudFrontState,
    /// How long the propagation tick sleeps before flipping a freshly
    /// created or updated Distribution / DistributionTenant /
    /// ConnectionGroup / StreamingDistribution from `InProgress` to
    /// `Deployed`. Real CloudFront takes ~15 minutes; the default of 1s
    /// keeps SDK-driven integration tests fast while still simulating the
    /// async transition. Tests can shrink it via
    /// [`CloudFrontService::with_propagation_delay`], and operators can
    /// override the default via the
    /// `FAKECLOUD_CLOUDFRONT_STATUS_DELAY_SEC` env var.
    pub(crate) propagation_delay: std::time::Duration,
    pub(crate) snapshot_store: Option<Arc<dyn SnapshotStore>>,
    pub(crate) snapshot_lock: Arc<AsyncMutex<()>>,
}

/// Resolve the default `InProgress` -> `Deployed` delay from the
/// `FAKECLOUD_CLOUDFRONT_STATUS_DELAY_SEC` env var, falling back to 1
/// second. The value parses as either a non-negative integer (seconds) or
/// a floating-point number; `0` keeps the transition synchronous, which is
/// useful for fast unit tests. Invalid input falls back to 1 second so a
/// typo in the env var can't accidentally hang the process.
fn default_propagation_delay() -> std::time::Duration {
    let Ok(raw) = std::env::var("FAKECLOUD_CLOUDFRONT_STATUS_DELAY_SEC") else {
        return std::time::Duration::from_secs(1);
    };
    let trimmed = raw.trim();
    if let Ok(secs) = trimmed.parse::<u64>() {
        return std::time::Duration::from_secs(secs);
    }
    if let Ok(secs) = trimmed.parse::<f64>() {
        if secs.is_finite() && secs >= 0.0 {
            return std::time::Duration::from_secs_f64(secs);
        }
    }
    std::time::Duration::from_secs(1)
}

impl CloudFrontService {
    pub fn new(state: SharedCloudFrontState) -> Self {
        Self {
            state,
            propagation_delay: default_propagation_delay(),
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn shared_state(&self) -> SharedCloudFrontState {
        Arc::clone(&self.state)
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes, with serde
    /// + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_cloudfront_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current CloudFront state when invoked, or
    /// `None` in memory mode. The CloudFormation provisioner mutates `state`
    /// directly and uses this to write a CFN-provisioned resource through to
    /// disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_cloudfront_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    /// Re-arm the propagation tick for any Distribution / DistributionTenant /
    /// ConnectionGroup / StreamingDistribution that was still `InProgress` when
    /// the previous process exited, so they still transition to `Deployed`
    /// after a restart. Called by the server after loading a snapshot.
    pub fn rearm_in_progress(&self) {
        let (dists, tenants, groups, streaming) = {
            let s = self.state.read();
            let mut dists = Vec::new();
            let mut tenants = Vec::new();
            let mut groups = Vec::new();
            let mut streaming = Vec::new();
            for account in s.accounts.values() {
                for (id, d) in &account.distributions {
                    if d.status == "InProgress" {
                        dists.push(id.clone());
                    }
                }
                for (id, t) in &account.distribution_tenants {
                    if t.status == "InProgress" {
                        tenants.push(id.clone());
                    }
                }
                for (id, g) in &account.connection_groups {
                    if g.status == "InProgress" {
                        groups.push(id.clone());
                    }
                }
                for (id, d) in &account.streaming_distributions {
                    if d.status == "InProgress" {
                        streaming.push(id.clone());
                    }
                }
            }
            (dists, tenants, groups, streaming)
        };
        for id in dists {
            self.schedule_distribution_deploy(id);
        }
        for id in tenants {
            self.schedule_distribution_tenant_deploy(id);
        }
        for id in groups {
            self.schedule_connection_group_deploy(id);
        }
        for id in streaming {
            self.schedule_streaming_distribution_deploy(id);
        }
    }

    /// Override the propagation delay used by `CreateDistribution`,
    /// `UpdateDistribution`, and the equivalent DistributionTenant /
    /// ConnectionGroup ops. Tests pass a small duration so they don't
    /// have to wait wall-clock seconds for the `InProgress` -> `Deployed`
    /// transition.
    pub fn with_propagation_delay(mut self, delay: std::time::Duration) -> Self {
        self.propagation_delay = delay;
        self
    }

    /// Flip a stored Distribution's status synchronously. Returns `false`
    /// if no distribution matches `id` across any account. The admin
    /// endpoint `POST /_fakecloud/cloudfront/distributions/{id}/status`
    /// calls this so tests can force `InProgress` <-> `Deployed` without
    /// waiting on the propagation tick.
    pub fn set_distribution_status(&self, id: &str, status: &str) -> bool {
        let mut state = self.state.write();
        for account in state.accounts.values_mut() {
            if let Some(dist) = account.distributions.get_mut(id) {
                dist.status = status.to_string();
                return true;
            }
        }
        false
    }

    /// Admin-endpoint flavour of [`set_distribution_status`](Self::set_distribution_status)
    /// that persists the forced status so it survives a restart.
    pub async fn set_distribution_status_persistent(&self, id: &str, status: &str) -> bool {
        let changed = self.set_distribution_status(id, status);
        if changed {
            self.save_snapshot().await;
        }
        changed
    }
}

/// Persist the current CloudFront state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `CloudFrontService::save_snapshot`, the propagation
/// ticks, and the CloudFormation provisioner persist hook so all route through
/// the same serialize-and-write path.
pub async fn save_cloudfront_snapshot(
    state: &SharedCloudFrontState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = CloudFrontSnapshot {
        schema_version: CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write cloudfront snapshot"),
        Err(err) => tracing::error!(%err, "cloudfront snapshot task panicked"),
    }
}

impl Default for CloudFrontService {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(CloudFrontAccounts::new())))
    }
}

#[async_trait]
impl AwsService for CloudFrontService {
    fn service_name(&self) -> &str {
        "cloudfront"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resolved = match route(&req.method, &req.raw_path, &req.raw_query) {
            Some(r) => r,
            None => {
                return Err(aws_error(
                    StatusCode::NOT_FOUND,
                    "InvalidArgument",
                    format!("Unknown CloudFront route: {} {}", req.method, req.raw_path),
                ));
            }
        };

        let mutates = is_mutating_action(resolved.action);
        let result = match resolved.action {
            "CreateDistribution" => self.create_distribution(&req, false),
            "CreateDistributionWithTags" => self.create_distribution(&req, true),
            "GetDistribution" => self.get_distribution(&resolved),
            "GetDistributionConfig" => self.get_distribution_config(&resolved),
            "UpdateDistribution" => self.update_distribution(&req, &resolved),
            "DeleteDistribution" => self.delete_distribution(&req, &resolved),
            "ListDistributions" => self.list_distributions(&req),
            "CopyDistribution" => self.copy_distribution(&req, &resolved),
            "CreateInvalidation" => self.create_invalidation(&req, &resolved),
            "GetInvalidation" => self.get_invalidation(&resolved),
            "ListInvalidations" => self.list_invalidations(&resolved),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "AssociateAlias" => self.associate_alias(&req, &resolved),
            "ListConflictingAliases" => self.list_conflicting_aliases(&req),
            "AssociateDistributionWebACL" => self.associate_web_acl(&req, &resolved),
            "DisassociateDistributionWebACL" => self.disassociate_web_acl(&req, &resolved),
            "ListDistributionsByCachePolicyId"
            | "ListDistributionsByOriginRequestPolicyId"
            | "ListDistributionsByResponseHeadersPolicyId"
            | "ListDistributionsByKeyGroup"
            | "ListDistributionsByWebACLId"
            | "ListDistributionsByVpcOriginId"
            | "ListDistributionsByAnycastIpListId"
            | "ListDistributionsByConnectionMode"
            | "ListDistributionsByConnectionFunction"
            | "ListDistributionsByOwnedResource"
            | "ListDistributionsByTrustStore"
            | "ListDistributionsByRealtimeLogConfig" => {
                self.list_distributions_by(&req, &resolved, resolved.action)
            }
            "CreateOriginAccessControl" => self.create_origin_access_control(&req),
            "GetOriginAccessControl" => self.get_origin_access_control(&resolved),
            "GetOriginAccessControlConfig" => self.get_origin_access_control_config(&resolved),
            "UpdateOriginAccessControl" => self.update_origin_access_control(&req, &resolved),
            "DeleteOriginAccessControl" => self.delete_origin_access_control(&req, &resolved),
            "ListOriginAccessControls" => self.list_origin_access_controls(&req),
            "CreateCachePolicy" => self.create_cache_policy(&req),
            "GetCachePolicy" => self.get_cache_policy(&resolved),
            "GetCachePolicyConfig" => self.get_cache_policy_config(&resolved),
            "UpdateCachePolicy" => self.update_cache_policy(&req, &resolved),
            "DeleteCachePolicy" => self.delete_cache_policy(&req, &resolved),
            "ListCachePolicies" => self.list_cache_policies(&req),
            "CreateOriginRequestPolicy" => self.create_origin_request_policy(&req),
            "GetOriginRequestPolicy" => self.get_origin_request_policy(&resolved),
            "GetOriginRequestPolicyConfig" => self.get_origin_request_policy_config(&resolved),
            "UpdateOriginRequestPolicy" => self.update_origin_request_policy(&req, &resolved),
            "DeleteOriginRequestPolicy" => self.delete_origin_request_policy(&req, &resolved),
            "ListOriginRequestPolicies" => self.list_origin_request_policies(&req),
            "CreateResponseHeadersPolicy" => self.create_response_headers_policy(&req),
            "GetResponseHeadersPolicy" => self.get_response_headers_policy(&resolved),
            "GetResponseHeadersPolicyConfig" => self.get_response_headers_policy_config(&resolved),
            "UpdateResponseHeadersPolicy" => self.update_response_headers_policy(&req, &resolved),
            "DeleteResponseHeadersPolicy" => self.delete_response_headers_policy(&req, &resolved),
            "ListResponseHeadersPolicies" => self.list_response_headers_policies(&req),
            "CreateContinuousDeploymentPolicy" => self.create_continuous_deployment_policy(&req),
            "GetContinuousDeploymentPolicy" => self.get_continuous_deployment_policy(&resolved),
            "GetContinuousDeploymentPolicyConfig" => {
                self.get_continuous_deployment_policy_config(&resolved)
            }
            "UpdateContinuousDeploymentPolicy" => {
                self.update_continuous_deployment_policy(&req, &resolved)
            }
            "DeleteContinuousDeploymentPolicy" => {
                self.delete_continuous_deployment_policy(&req, &resolved)
            }
            "ListContinuousDeploymentPolicies" => self.list_continuous_deployment_policies(&req),
            "CreateFunction" => self.create_function(&req),
            "DescribeFunction" => self.describe_function(&req, &resolved),
            "GetFunction" => self.get_function(&req, &resolved),
            "UpdateFunction" => self.update_function(&req, &resolved),
            "DeleteFunction" => self.delete_function(&req, &resolved),
            "ListFunctions" => self.list_functions(&req),
            "PublishFunction" => self.publish_function(&req, &resolved),
            "TestFunction" => self.test_function(&req, &resolved),
            "CreatePublicKey" => self.create_public_key(&req),
            "GetPublicKey" => self.get_public_key(&resolved),
            "GetPublicKeyConfig" => self.get_public_key_config(&resolved),
            "UpdatePublicKey" => self.update_public_key(&req, &resolved),
            "DeletePublicKey" => self.delete_public_key(&req, &resolved),
            "ListPublicKeys" => self.list_public_keys(&req),
            "CreateKeyGroup" => self.create_key_group(&req),
            "GetKeyGroup" => self.get_key_group(&resolved),
            "GetKeyGroupConfig" => self.get_key_group_config(&resolved),
            "UpdateKeyGroup" => self.update_key_group(&req, &resolved),
            "DeleteKeyGroup" => self.delete_key_group(&req, &resolved),
            "ListKeyGroups" => self.list_key_groups(&req),
            "CreateKeyValueStore" => self.create_key_value_store(&req),
            "DescribeKeyValueStore" => self.describe_key_value_store(&resolved),
            "UpdateKeyValueStore" => self.update_key_value_store(&req, &resolved),
            "DeleteKeyValueStore" => self.delete_key_value_store(&req, &resolved),
            "ListKeyValueStores" => self.list_key_value_stores(&req),
            "CreateCloudFrontOriginAccessIdentity" => self.create_oai(&req),
            "GetCloudFrontOriginAccessIdentity" => self.get_oai(&resolved),
            "GetCloudFrontOriginAccessIdentityConfig" => self.get_oai_config(&resolved),
            "UpdateCloudFrontOriginAccessIdentity" => self.update_oai(&req, &resolved),
            "DeleteCloudFrontOriginAccessIdentity" => self.delete_oai(&req, &resolved),
            "ListCloudFrontOriginAccessIdentities" => self.list_oai(&req),
            "CreateMonitoringSubscription" => self.create_monitoring_subscription(&req, &resolved),
            "GetMonitoringSubscription" => self.get_monitoring_subscription(&resolved),
            "DeleteMonitoringSubscription" => self.delete_monitoring_subscription(&resolved),
            "CreateStreamingDistribution" => self.create_streaming_distribution(&req, false),
            "CreateStreamingDistributionWithTags" => self.create_streaming_distribution(&req, true),
            "GetStreamingDistribution" => self.get_streaming_distribution(&resolved),
            "GetStreamingDistributionConfig" => self.get_streaming_distribution_config(&resolved),
            "UpdateStreamingDistribution" => self.update_streaming_distribution(&req, &resolved),
            "DeleteStreamingDistribution" => self.delete_streaming_distribution(&req, &resolved),
            "ListStreamingDistributions" => self.list_streaming_distributions(&req),
            "CreateFieldLevelEncryptionConfig" => self.create_field_level_encryption_config(&req),
            "GetFieldLevelEncryption" => self.get_field_level_encryption(&resolved),
            "GetFieldLevelEncryptionConfig" => self.get_field_level_encryption_config(&resolved),
            "UpdateFieldLevelEncryptionConfig" => {
                self.update_field_level_encryption_config(&req, &resolved)
            }
            "DeleteFieldLevelEncryptionConfig" => {
                self.delete_field_level_encryption_config(&req, &resolved)
            }
            "ListFieldLevelEncryptionConfigs" => self.list_field_level_encryption_configs(&req),
            "CreateFieldLevelEncryptionProfile" => self.create_field_level_encryption_profile(&req),
            "GetFieldLevelEncryptionProfile" => self.get_field_level_encryption_profile(&resolved),
            "GetFieldLevelEncryptionProfileConfig" => {
                self.get_field_level_encryption_profile_config(&resolved)
            }
            "UpdateFieldLevelEncryptionProfile" => {
                self.update_field_level_encryption_profile(&req, &resolved)
            }
            "DeleteFieldLevelEncryptionProfile" => {
                self.delete_field_level_encryption_profile(&req, &resolved)
            }
            "ListFieldLevelEncryptionProfiles" => self.list_field_level_encryption_profiles(&req),
            "CreateRealtimeLogConfig" => self.create_realtime_log_config(&req),
            "GetRealtimeLogConfig" => self.get_realtime_log_config(&req),
            "UpdateRealtimeLogConfig" => self.update_realtime_log_config(&req),
            "DeleteRealtimeLogConfig" => self.delete_realtime_log_config(&req),
            "ListRealtimeLogConfigs" => self.list_realtime_log_configs(&req),
            "CreateVpcOrigin" => self.create_vpc_origin(&req),
            "GetVpcOrigin" => self.get_vpc_origin(&resolved),
            "UpdateVpcOrigin" => self.update_vpc_origin(&req, &resolved),
            "DeleteVpcOrigin" => self.delete_vpc_origin(&req, &resolved),
            "ListVpcOrigins" => self.list_vpc_origins(&req),
            "CreateAnycastIpList" => self.create_anycast_ip_list(&req),
            "GetAnycastIpList" => self.get_anycast_ip_list(&resolved),
            "UpdateAnycastIpList" => self.update_anycast_ip_list(&req, &resolved),
            "DeleteAnycastIpList" => self.delete_anycast_ip_list(&req, &resolved),
            "ListAnycastIpLists" => self.list_anycast_ip_lists(&req),
            "CreateTrustStore" => self.create_trust_store(&req),
            "GetTrustStore" => self.get_trust_store(&resolved),
            "UpdateTrustStore" => self.update_trust_store(&req, &resolved),
            "DeleteTrustStore" => self.delete_trust_store(&req, &resolved),
            "ListTrustStores" => self.list_trust_stores(&req),
            "GetResourcePolicy" => self.get_resource_policy(&req),
            "PutResourcePolicy" => self.put_resource_policy(&req),
            "DeleteResourcePolicy" => self.delete_resource_policy(&req),
            "CreateConnectionGroup" => self.create_connection_group(&req),
            "GetConnectionGroup" => self.get_connection_group(&resolved),
            "GetConnectionGroupByRoutingEndpoint" => {
                self.get_connection_group_by_routing_endpoint(&req)
            }
            "UpdateConnectionGroup" => self.update_connection_group(&req, &resolved),
            "DeleteConnectionGroup" => self.delete_connection_group(&req, &resolved),
            "ListConnectionGroups" => self.list_connection_groups(&req),
            "ListDomainConflicts" => self.list_domain_conflicts(&req),
            "UpdateDomainAssociation" => self.update_domain_association(&req),
            "VerifyDnsConfiguration" => self.verify_dns_configuration(&req),
            "GetManagedCertificateDetails" => self.get_managed_certificate_details(&resolved),
            "UpdateDistributionWithStagingConfig" => {
                self.update_distribution_with_staging_config(&req, &resolved)
            }
            "CreateDistributionTenant" => self.create_distribution_tenant(&req),
            "GetDistributionTenant" => self.get_distribution_tenant(&resolved),
            "GetDistributionTenantByDomain" => self.get_distribution_tenant_by_domain(&req),
            "UpdateDistributionTenant" => self.update_distribution_tenant(&req, &resolved),
            "DeleteDistributionTenant" => self.delete_distribution_tenant(&req, &resolved),
            "ListDistributionTenants" => self.list_distribution_tenants(&req),
            "ListDistributionTenantsByCustomization" => {
                self.list_distribution_tenants_by_customization(&req)
            }
            "AssociateDistributionTenantWebACL" => {
                self.associate_distribution_tenant_web_acl(&req, &resolved)
            }
            "DisassociateDistributionTenantWebACL" => {
                self.disassociate_distribution_tenant_web_acl(&req, &resolved)
            }
            "CreateInvalidationForDistributionTenant" => {
                self.create_invalidation_for_distribution_tenant(&req, &resolved)
            }
            "GetInvalidationForDistributionTenant" => {
                self.get_invalidation_for_distribution_tenant(&resolved)
            }
            "ListInvalidationsForDistributionTenant" => {
                self.list_invalidations_for_distribution_tenant(&resolved)
            }
            "CreateConnectionFunction" => self.create_connection_function(&req),
            "GetConnectionFunction" => self.get_connection_function(&resolved),
            "DescribeConnectionFunction" => self.describe_connection_function(&resolved),
            "UpdateConnectionFunction" => self.update_connection_function(&req, &resolved),
            "DeleteConnectionFunction" => self.delete_connection_function(&req, &resolved),
            "ListConnectionFunctions" => self.list_connection_functions(&req),
            "PublishConnectionFunction" => self.publish_connection_function(&req, &resolved),
            "TestConnectionFunction" => self.test_connection_function(&req, &resolved),
            other => Err(aws_error(
                StatusCode::NOT_IMPLEMENTED,
                "InvalidAction",
                format!("CloudFront action {other} is not implemented yet"),
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }
}

// ─── Distribution handlers ────────────────────────────────────────────

impl CloudFrontService {
    fn create_distribution(
        &self,
        req: &AwsRequest,
        with_tags: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let (config, tags) = if with_tags {
            let parsed: DistributionConfigWithTags = xml_io::from_xml_root(&req.body)
                .map_err(|e| invalid_argument(format!("invalid request XML: {e}")))?;
            let tags = parsed
                .tags
                .items
                .map(|i| {
                    i.tag
                        .into_iter()
                        .map(|t| Tag {
                            key: t.key,
                            value: t.value,
                        })
                        .collect()
                })
                .unwrap_or_default();
            (parsed.distribution_config, tags)
        } else {
            let parsed: DistributionConfig = xml_io::from_xml_root(&req.body)
                .map_err(|e| invalid_argument(format!("invalid request XML: {e}")))?;
            (parsed, Vec::new())
        };

        validate_caller_reference(&config.caller_reference)?;
        validate_origins(&config)?;

        let mut state = self.state.write();
        let account = state.entry(account_id(req));

        if let Some(existing) = account
            .distributions
            .values()
            .find(|d| d.config.caller_reference == config.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "DistributionAlreadyExists",
                format!(
                    "Distribution with the same CallerReference exists: {}",
                    existing.id
                ),
            ));
        }

        let id = generate_distribution_id();
        let now = Utc::now();
        let etag = generate_etag();
        let domain = format!("{}.cloudfront.net", id.to_lowercase());
        let arn = format!(
            "arn:aws:cloudfront::{}:distribution/{}",
            account_id(req),
            id
        );

        let stored = StoredDistribution {
            id: id.clone(),
            arn: arn.clone(),
            // Real CloudFront returns InProgress immediately and flips to
            // Deployed ~15 minutes later once the edge propagation completes.
            // The spawn below mirrors that lifecycle on a configurable delay.
            status: "InProgress".to_string(),
            last_modified_time: now,
            domain_name: domain,
            in_progress_invalidation_batches: 0,
            etag: etag.clone(),
            config,
        };
        account.distributions.insert(id.clone(), stored.clone());
        if !tags.is_empty() {
            account.tags.insert(arn.clone(), tags);
        }
        drop(state);

        self.schedule_distribution_deploy(id.clone());

        let body = build_distribution_xml(&stored);
        let mut headers = HeaderMap::new();
        set_header(&mut headers, ETAG, &etag);
        set_header(&mut headers, LOCATION, &stored.arn);
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    /// Spawn a tokio task that flips the named distribution from
    /// `InProgress` to `Deployed` after `propagation_delay`. Used by
    /// `CreateDistribution` and `UpdateDistribution` to model the real
    /// CloudFront edge propagation lifecycle.
    fn schedule_distribution_deploy(&self, id: String) {
        let state = Arc::clone(&self.state);
        let delay = self.propagation_delay;
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let flipped = {
                let mut s = state.write();
                let mut flipped = false;
                for account in s.accounts.values_mut() {
                    if let Some(d) = account.distributions.get_mut(&id) {
                        if d.status == "InProgress" {
                            d.status = "Deployed".to_string();
                            flipped = true;
                        }
                        break;
                    }
                }
                flipped
            };
            if flipped {
                save_cloudfront_snapshot(&state, store, &lock).await;
            }
        });
    }

    /// Same as [`schedule_distribution_deploy`] but for DistributionTenants.
    pub(crate) fn schedule_distribution_tenant_deploy(&self, id: String) {
        let state = Arc::clone(&self.state);
        let delay = self.propagation_delay;
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let flipped = {
                let mut s = state.write();
                let mut flipped = false;
                for account in s.accounts.values_mut() {
                    if let Some(t) = account.distribution_tenants.get_mut(&id) {
                        if t.status == "InProgress" {
                            t.status = "Deployed".to_string();
                            flipped = true;
                        }
                        break;
                    }
                }
                flipped
            };
            if flipped {
                save_cloudfront_snapshot(&state, store, &lock).await;
            }
        });
    }

    /// Same as [`schedule_distribution_deploy`] but for ConnectionGroups.
    pub(crate) fn schedule_connection_group_deploy(&self, id: String) {
        let state = Arc::clone(&self.state);
        let delay = self.propagation_delay;
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let flipped = {
                let mut s = state.write();
                let mut flipped = false;
                for account in s.accounts.values_mut() {
                    if let Some(g) = account.connection_groups.get_mut(&id) {
                        if g.status == "InProgress" {
                            g.status = "Deployed".to_string();
                            flipped = true;
                        }
                        break;
                    }
                }
                flipped
            };
            if flipped {
                save_cloudfront_snapshot(&state, store, &lock).await;
            }
        });
    }

    /// Same as [`schedule_distribution_deploy`] but for StreamingDistributions.
    /// Real CloudFront mirrors the `InProgress` -> `Deployed` lifecycle for
    /// RTMP streaming distributions, even though the resource is largely
    /// deprecated.
    pub(crate) fn schedule_streaming_distribution_deploy(&self, id: String) {
        let state = Arc::clone(&self.state);
        let delay = self.propagation_delay;
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            let flipped = {
                let mut s = state.write();
                let mut flipped = false;
                for account in s.accounts.values_mut() {
                    if let Some(d) = account.streaming_distributions.get_mut(&id) {
                        if d.status == "InProgress" {
                            d.status = "Deployed".to_string();
                            flipped = true;
                        }
                        break;
                    }
                }
                flipped
            };
            if flipped {
                save_cloudfront_snapshot(&state, store, &lock).await;
            }
        });
    }

    fn get_distribution(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_distribution(id))?;
        let dist = account
            .distributions
            .get(id)
            .ok_or_else(|| no_such_distribution(id))?
            .clone();
        drop(state);
        let body = build_distribution_xml(&dist);
        let mut headers = HeaderMap::new();
        set_header(&mut headers, ETAG, &dist.etag);
        Ok(xml_response(StatusCode::OK, body, headers))
    }

    fn get_distribution_config(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_distribution(id))?;
        let dist = account
            .distributions
            .get(id)
            .ok_or_else(|| no_such_distribution(id))?
            .clone();
        drop(state);
        let body = xml_io::to_xml_root("DistributionConfig", &dist.config)
            .map_err(|e| internal_error(format!("xml encode failed: {e}")))?;
        let mut headers = HeaderMap::new();
        set_header(&mut headers, ETAG, &dist.etag);
        Ok(xml_response(StatusCode::OK, body, headers))
    }

    fn update_distribution(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let if_match = req
            .headers
            .get(IF_MATCH)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidIfMatchVersion",
                    "Missing If-Match header for UpdateDistribution",
                )
            })?
            .to_string();
        let new_config: DistributionConfig = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid DistributionConfig XML: {e}")))?;
        validate_caller_reference(&new_config.caller_reference)?;
        validate_origins(&new_config)?;

        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_distribution(id))?;
        let dist = account
            .distributions
            .get_mut(id)
            .ok_or_else(|| no_such_distribution(id))?;
        if dist.etag != if_match {
            return Err(aws_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "If-Match header does not match the current ETag",
            ));
        }
        // ETag stability: only bump the ETag and flip status back to
        // InProgress when the new config actually differs from what we
        // have on disk. A no-op UpdateDistribution (PUT the same config
        // back) leaves the ETag intact, matching AWS behavior.
        let config_changed = !configs_equal(&dist.config, &new_config);
        if config_changed {
            dist.config = new_config;
            dist.etag = generate_etag();
            dist.last_modified_time = Utc::now();
            // UpdateDistribution kicks off a fresh edge propagation; AWS
            // flips the status back to InProgress until the new config
            // lands.
            dist.status = "InProgress".to_string();
        }
        let snapshot = dist.clone();
        drop(state);

        if config_changed {
            self.schedule_distribution_deploy(id.to_string());
        }

        let body = build_distribution_xml(&snapshot);
        let mut headers = HeaderMap::new();
        set_header(&mut headers, ETAG, &snapshot.etag);
        Ok(xml_response(StatusCode::OK, body, headers))
    }

    fn delete_distribution(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let if_match = req
            .headers
            .get(IF_MATCH)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidIfMatchVersion",
                    "Missing If-Match header for DeleteDistribution",
                )
            })?
            .to_string();
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_distribution(id))?;
        {
            let dist = account
                .distributions
                .get(id)
                .ok_or_else(|| no_such_distribution(id))?;
            if dist.etag != if_match {
                return Err(aws_error(
                    StatusCode::PRECONDITION_FAILED,
                    "PreconditionFailed",
                    "If-Match header does not match the current ETag",
                ));
            }
            if dist.config.enabled {
                return Err(aws_error(
                    StatusCode::PRECONDITION_FAILED,
                    "DistributionNotDisabled",
                    "Distribution must be disabled before delete",
                ));
            }
        }
        let removed = account.distributions.remove(id).unwrap();
        account.tags.remove(&removed.arn);
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    fn list_distributions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let mut dists: Vec<StoredDistribution> = state
            .accounts
            .values()
            .flat_map(|a| a.distributions.values().cloned())
            .collect();
        drop(state);
        dists.sort_by_key(|a| a.last_modified_time);

        // CloudFront paginates with an exclusive Marker (start after this id) +
        // MaxItems; shared with the ListDistributionsBy* handlers.
        let (marker, max_items, page, next_marker) = paginate_distributions(req, dists);

        let body = build_distribution_list_xml(
            &page,
            "DistributionList",
            &marker,
            max_items,
            next_marker.as_deref(),
        );
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_distributions_by(
        &self,
        req: &AwsRequest,
        route: &Route,
        action: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Each "by-X" listing has a Smithy-required identifier (path or query)
        // plus, for ConnectionMode, an enum-constrained discriminator. The
        // synthetic probe sends `negative_omit_*` variants without these, so
        // every handler that can short-circuit on a missing identifier must
        // do so up-front. Otherwise the empty-list 200 looks like an honest
        // pass to the probe and a 100% conformance number is unreachable.
        match action {
            // Path-id ops: route.id is the URL placeholder. If the probe
            // omitted the field, the substitution left the literal
            // `{Member}` braces in place — easy to detect.
            "ListDistributionsByCachePolicyId"
            | "ListDistributionsByOriginRequestPolicyId"
            | "ListDistributionsByResponseHeadersPolicyId"
            | "ListDistributionsByKeyGroup"
            | "ListDistributionsByWebACLId"
            | "ListDistributionsByVpcOriginId"
            | "ListDistributionsByAnycastIpListId"
            | "ListDistributionsByOwnedResource" => {
                let id = route.id.as_deref().unwrap_or("");
                if is_placeholder_label(id) {
                    return Err(invalid_argument(format!(
                        "Required URL identifier for {action} is missing or invalid"
                    )));
                }
            }
            "ListDistributionsByConnectionMode" => {
                let id = route.id.as_deref().unwrap_or("");
                if is_placeholder_label(id) {
                    return Err(invalid_argument(
                        "ConnectionMode is required for ListDistributionsByConnectionMode",
                    ));
                }
                if id != "direct" && id != "tenant-only" {
                    return Err(invalid_argument(format!(
                        "ConnectionMode must be 'direct' or 'tenant-only', got '{id}'"
                    )));
                }
            }
            "ListDistributionsByConnectionFunction"
                if parse_query_value(&req.raw_query, "ConnectionFunctionIdentifier").is_none() =>
            {
                return Err(invalid_argument(
                    "ConnectionFunctionIdentifier query parameter is required",
                ));
            }
            "ListDistributionsByTrustStore"
                if parse_query_value(&req.raw_query, "TrustStoreIdentifier").is_none() =>
            {
                return Err(invalid_argument(
                    "TrustStoreIdentifier query parameter is required",
                ));
            }
            _ => {}
        }

        // The "by-X" listings each have a distinct response root element.
        // For the seven predicate ops whose match field is persisted on the
        // distribution config we filter the live state; the remaining ops
        // (owned-resource, connection-mode/function, trust-store,
        // realtime-log-config) are not yet indexed and stay empty.
        let root = match action {
            "ListDistributionsByCachePolicyId"
            | "ListDistributionsByOriginRequestPolicyId"
            | "ListDistributionsByResponseHeadersPolicyId"
            | "ListDistributionsByKeyGroup"
            | "ListDistributionsByVpcOriginId" => "DistributionIdList",
            "ListDistributionsByOwnedResource" => "DistributionIdOwnerList",
            _ => "DistributionList",
        };

        // Collect all distributions once, sorted for stable ordering.
        let mut all: Vec<StoredDistribution> = {
            let state = self.state.read();
            state
                .accounts
                .values()
                .flat_map(|a| a.distributions.values().cloned())
                .collect()
        };
        all.sort_by_key(|d| d.last_modified_time);

        let path_id = route.id.as_deref().unwrap_or("");
        let matched: Vec<StoredDistribution> = match action {
            "ListDistributionsByCachePolicyId" => all
                .into_iter()
                .filter(|d| distribution_uses_cache_policy(d, path_id))
                .collect(),
            "ListDistributionsByOriginRequestPolicyId" => all
                .into_iter()
                .filter(|d| distribution_uses_origin_request_policy(d, path_id))
                .collect(),
            "ListDistributionsByResponseHeadersPolicyId" => all
                .into_iter()
                .filter(|d| distribution_uses_response_headers_policy(d, path_id))
                .collect(),
            "ListDistributionsByKeyGroup" => all
                .into_iter()
                .filter(|d| distribution_uses_key_group(d, path_id))
                .collect(),
            "ListDistributionsByVpcOriginId" => all
                .into_iter()
                .filter(|d| distribution_uses_vpc_origin(d, path_id))
                .collect(),
            // CloudFront treats the literal WebACLId "null" as "list the
            // distributions that aren't associated with any web ACL".
            "ListDistributionsByWebACLId" if path_id == "null" => all
                .into_iter()
                .filter(|d| {
                    d.config
                        .web_acl_id
                        .as_deref()
                        .map(|w| w.is_empty())
                        .unwrap_or(true)
                })
                .collect(),
            "ListDistributionsByWebACLId" => all
                .into_iter()
                .filter(|d| d.config.web_acl_id.as_deref() == Some(path_id))
                .collect(),
            "ListDistributionsByAnycastIpListId" => all
                .into_iter()
                .filter(|d| d.config.anycast_ip_list_id.as_deref() == Some(path_id))
                .collect(),
            _ => Vec::new(),
        };

        // Mirror the Marker/MaxItems pagination the plain ListDistributions
        // handler applies so large predicate result sets page correctly.
        let (marker, max_items, page, next_marker) = paginate_distributions(req, matched);

        let body = if root == "DistributionIdList" {
            let ids: Vec<&str> = page.iter().map(|d| d.id.as_str()).collect();
            build_distribution_id_list_xml(&ids, &marker, max_items, next_marker.as_deref())
        } else if root == "DistributionList"
            && matches!(
                action,
                "ListDistributionsByWebACLId" | "ListDistributionsByAnycastIpListId"
            )
        {
            build_distribution_list_xml(
                &page,
                "DistributionList",
                &marker,
                max_items,
                next_marker.as_deref(),
            )
        } else {
            build_empty_distribution_id_list(root)
        };
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn copy_distribution(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let primary_id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing primary distribution id"))?;
        let if_match = req
            .headers
            .get(IF_MATCH)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| {
                aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidIfMatchVersion",
                    "Missing If-Match header for CopyDistribution",
                )
            })?
            .to_string();
        let parsed: CopyDistributionRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid request XML: {e}")))?;
        validate_caller_reference(&parsed.caller_reference)?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_distribution(primary_id))?;
        let primary = account
            .distributions
            .get(primary_id)
            .ok_or_else(|| no_such_distribution(primary_id))?
            .clone();
        if primary.etag != if_match {
            return Err(aws_error(
                StatusCode::PRECONDITION_FAILED,
                "PreconditionFailed",
                "If-Match header does not match the current ETag",
            ));
        }
        if account
            .distributions
            .values()
            .any(|d| d.config.caller_reference == parsed.caller_reference)
        {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "DistributionAlreadyExists",
                "Distribution with the same CallerReference exists",
            ));
        }
        let new_id = generate_distribution_id();
        let mut config = primary.config.clone();
        config.caller_reference = parsed.caller_reference;
        config.enabled = parsed.enabled.unwrap_or(false);
        config.staging = parsed.staging;
        let now = Utc::now();
        let etag = generate_etag();
        let arn = format!(
            "arn:aws:cloudfront::{}:distribution/{}",
            account_id(req),
            new_id
        );
        let stored = StoredDistribution {
            id: new_id.clone(),
            arn: arn.clone(),
            status: "InProgress".to_string(),
            last_modified_time: now,
            domain_name: format!("{}.cloudfront.net", new_id.to_lowercase()),
            in_progress_invalidation_batches: 0,
            etag: etag.clone(),
            config,
        };
        account.distributions.insert(new_id.clone(), stored.clone());
        drop(state);
        self.schedule_distribution_deploy(new_id);
        let body = build_distribution_xml(&stored);
        let mut headers = HeaderMap::new();
        set_header(&mut headers, ETAG, &etag);
        set_header(&mut headers, LOCATION, &stored.arn);
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }
}

#[derive(Debug, serde::Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
struct CopyDistributionRequest {
    caller_reference: String,
    #[serde(default)]
    enabled: Option<bool>,
    #[serde(default)]
    staging: Option<bool>,
}

// ─── Invalidations ────────────────────────────────────────────────────

impl CloudFrontService {
    fn create_invalidation(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let dist_id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let batch: InvalidationBatch = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid InvalidationBatch XML: {e}")))?;
        if batch.caller_reference.is_empty() {
            return Err(invalid_argument("CallerReference is required"));
        }
        if batch.paths.quantity < 1 {
            return Err(invalid_argument(
                "InvalidationBatch.Paths must be non-empty",
            ));
        }
        let mut state = self.state.write();
        let account = state.entry(DEFAULT_ACCOUNT);
        if !account.distributions.contains_key(dist_id) {
            return Err(no_such_distribution(dist_id));
        }
        let id = generate_invalidation_id();
        let stored = StoredInvalidation {
            id: id.clone(),
            distribution_id: dist_id.to_string(),
            status: "Completed".to_string(),
            create_time: Utc::now(),
            batch: batch.clone(),
        };
        account.invalidations.insert(id.clone(), stored.clone());
        drop(state);
        let body = build_invalidation_xml(&stored);
        let mut headers = HeaderMap::new();
        set_header(
            &mut headers,
            LOCATION,
            &format!(
                "/2020-05-31/distribution/{dist_id}/invalidation/{}",
                stored.id
            ),
        );
        Ok(xml_response(StatusCode::CREATED, body, headers))
    }

    fn get_invalidation(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let dist_id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let inv_id = route
            .second_id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing invalidation id"))?;
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_invalidation(inv_id))?;
        if !account.distributions.contains_key(dist_id) {
            return Err(no_such_distribution(dist_id));
        }
        let inv = account
            .invalidations
            .get(inv_id)
            .filter(|i| i.distribution_id == dist_id)
            .ok_or_else(|| no_such_invalidation(inv_id))?
            .clone();
        drop(state);
        let body = build_invalidation_xml(&inv);
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn list_invalidations(&self, route: &Route) -> Result<AwsResponse, AwsServiceError> {
        let dist_id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let state = self.state.read();
        let account = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_distribution(dist_id))?;
        if !account.distributions.contains_key(dist_id) {
            return Err(no_such_distribution(dist_id));
        }
        let mut items: Vec<&StoredInvalidation> = account
            .invalidations
            .values()
            .filter(|i| i.distribution_id == dist_id)
            .collect();
        items.sort_by_key(|a| a.create_time);
        let body = build_invalidation_list_xml(&items);
        drop(state);
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Tags ─────────────────────────────────────────────────────────────

impl CloudFrontService {
    fn parse_arn_query(query: &str) -> Option<String> {
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            if let Some(rest) = pair.strip_prefix("Resource=") {
                return Some(percent_decode(rest));
            }
        }
        None
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = Self::parse_arn_query(&req.raw_query)
            .ok_or_else(|| invalid_argument("Resource query parameter is required"))?;
        let parsed: ModelTags = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid Tags XML: {e}")))?;
        let new_tags: Vec<Tag> = parsed
            .items
            .map(|i| {
                i.tag
                    .into_iter()
                    .map(|t| Tag {
                        key: t.key,
                        value: t.value,
                    })
                    .collect()
            })
            .unwrap_or_default();
        let mut state = self.state.write();
        let account = state.entry(DEFAULT_ACCOUNT);
        let entry = account.tags.entry(arn).or_default();
        for tag in new_tags {
            if let Some(existing) = entry.iter_mut().find(|t| t.key == tag.key) {
                existing.value = tag.value;
            } else {
                entry.push(tag);
            }
        }
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = Self::parse_arn_query(&req.raw_query)
            .ok_or_else(|| invalid_argument("Resource query parameter is required"))?;
        let parsed: TagKeys = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid TagKeys XML: {e}")))?;
        let keys: Vec<String> = parsed.items.map(|k| k.key).unwrap_or_default();
        let mut state = self.state.write();
        let account = state.entry(DEFAULT_ACCOUNT);
        if let Some(existing) = account.tags.get_mut(&arn) {
            existing.retain(|t| !keys.contains(&t.key));
        }
        Ok(empty_response(StatusCode::NO_CONTENT))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = Self::parse_arn_query(&req.raw_query)
            .ok_or_else(|| invalid_argument("Resource query parameter is required"))?;
        let state = self.state.read();
        let tags = state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.tags.get(&arn))
            .cloned()
            .unwrap_or_default();
        drop(state);
        let body = build_tags_xml(&tags);
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

// ─── Aliases / WebACL ─────────────────────────────────────────────────

impl CloudFrontService {
    fn associate_alias(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let alias = parse_query_value(&req.raw_query, "Alias")
            .ok_or_else(|| invalid_argument("Alias query parameter is required"))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_distribution(id))?;
        // Reject if the alias is already attached to a different distribution.
        if let Some(other) = account.distributions.values().find(|d| {
            d.id != id
                && d.config
                    .aliases
                    .as_ref()
                    .and_then(|a| a.items.as_ref())
                    .is_some_and(|i| i.cname.iter().any(|c| c == &alias))
        }) {
            return Err(aws_error(
                StatusCode::CONFLICT,
                "CNAMEAlreadyExists",
                format!(
                    "Alias {alias} is already associated with distribution {}",
                    other.id
                ),
            ));
        }
        let dist = account
            .distributions
            .get_mut(id)
            .ok_or_else(|| no_such_distribution(id))?;
        let aliases = dist.config.aliases.get_or_insert_with(Default::default);
        let items = aliases
            .items
            .get_or_insert_with(crate::model::AliasItems::default);
        if !items.cname.iter().any(|c| c == &alias) {
            items.cname.push(alias.clone());
            aliases.quantity = items.cname.len() as i32;
        }
        dist.etag = generate_etag();
        dist.last_modified_time = Utc::now();
        Ok(empty_response(StatusCode::OK))
    }

    fn list_conflicting_aliases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let alias = parse_query_value(&req.raw_query, "Alias")
            .ok_or_else(|| invalid_argument("Alias query parameter is required"))?;
        let dist_id = parse_query_value(&req.raw_query, "DistributionId")
            .ok_or_else(|| invalid_argument("DistributionId query parameter is required"))?;
        // aliasString max 253, distributionIdString max 25 per the Smithy
        // model. Reject probe-generated boundary variants that overrun the
        // documented length so they produce the declared InvalidArgument
        // instead of an empty 200.
        if alias.len() > 253 {
            return Err(invalid_argument(format!(
                "Alias length {} exceeds maximum 253",
                alias.len()
            )));
        }
        if dist_id.len() > 25 {
            return Err(invalid_argument(format!(
                "DistributionId length {} exceeds maximum 25",
                dist_id.len()
            )));
        }
        if let Some(max_items) = parse_query_value(&req.raw_query, "MaxItems") {
            let n: i64 = max_items.parse().map_err(|_| {
                invalid_argument(format!("MaxItems must be an integer, got '{max_items}'"))
            })?;
            if n > 100 {
                return Err(invalid_argument(format!(
                    "MaxItems {n} exceeds maximum 100"
                )));
            }
        }
        // We never produce conflicts because every alias is owned by one
        // distribution at most. Return an empty list with the proper shape.
        let body = format!(
            "{XML_DECL}<ConflictingAliasesList xmlns=\"{NS}\"><Quantity>0</Quantity></ConflictingAliasesList>",
            NS = crate::NAMESPACE,
            XML_DECL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        );
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn associate_web_acl(
        &self,
        req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        let parsed: AssociateAliasRequest = xml_io::from_xml_root(&req.body)
            .map_err(|e| invalid_argument(format!("invalid request XML: {e}")))?;
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(|| no_such_distribution(id))?;
        let dist = account
            .distributions
            .get_mut(id)
            .ok_or_else(|| no_such_distribution(id))?;
        dist.config.web_acl_id = Some(parsed.web_acl_arn.clone());
        dist.etag = generate_etag();
        dist.last_modified_time = Utc::now();
        let body = format!(
            "{XML_DECL}<AssociateDistributionWebACLResult xmlns=\"{NS}\"><Id>{}</Id><WebACLArn>{}</WebACLArn></AssociateDistributionWebACLResult>",
            esc(id), esc(&parsed.web_acl_arn),
            NS = crate::NAMESPACE,
            XML_DECL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        );
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }

    fn disassociate_web_acl(
        &self,
        _req: &AwsRequest,
        route: &Route,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = route
            .id
            .as_deref()
            .ok_or_else(|| invalid_argument("missing distribution id"))?;
        // DisassociateDistributionWebACL's Smithy model declares EntityNotFound
        // (not NoSuchDistribution) for unknown distribution IDs.
        let entity_not_found = || {
            aws_error(
                StatusCode::NOT_FOUND,
                "EntityNotFound",
                format!("The specified distribution does not exist: {id}"),
            )
        };
        let mut state = self.state.write();
        let account = state
            .accounts
            .get_mut(DEFAULT_ACCOUNT)
            .ok_or_else(entity_not_found)?;
        let dist = account
            .distributions
            .get_mut(id)
            .ok_or_else(entity_not_found)?;
        dist.config.web_acl_id = None;
        dist.etag = generate_etag();
        dist.last_modified_time = Utc::now();
        let body = format!(
            "{XML_DECL}<DisassociateDistributionWebACLResult xmlns=\"{NS}\"><Id>{}</Id></DisassociateDistributionWebACLResult>",
            esc(id),
            NS = crate::NAMESPACE,
            XML_DECL = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        );
        Ok(xml_response(StatusCode::OK, body, HeaderMap::new()))
    }
}

#[derive(serde::Deserialize, Default, Debug)]
#[serde(rename_all = "PascalCase")]
struct AssociateAliasRequest {
    #[serde(rename = "WebACLArn", default)]
    web_acl_arn: String,
}

// ─── XML body builders ────────────────────────────────────────────────

/// XML-escape a user-provided string before injecting it into hand-rolled
/// XML response bodies. The 5 standard XML metacharacters are escaped;
/// everything else passes through unchanged. Keep this in sync with
/// `quick_xml`'s entity table — using their own primitive would mean a
/// `Writer` per call and we serialize directly into a `String`.
pub(crate) fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&apos;"),
            _ => out.push(c),
        }
    }
    out
}

pub(crate) fn build_distribution_xml(dist: &StoredDistribution) -> String {
    let mut out = String::with_capacity(2048);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    out.push_str(&format!(
        "<Distribution xmlns=\"{ns}\">",
        ns = crate::NAMESPACE
    ));
    out.push_str(&format!("<Id>{}</Id>", esc(&dist.id)));
    out.push_str(&format!("<ARN>{}</ARN>", esc(&dist.arn)));
    out.push_str(&format!("<Status>{}</Status>", esc(&dist.status)));
    out.push_str(&format!(
        "<LastModifiedTime>{}</LastModifiedTime>",
        rfc3339(&dist.last_modified_time)
    ));
    out.push_str(&format!(
        "<InProgressInvalidationBatches>{}</InProgressInvalidationBatches>",
        dist.in_progress_invalidation_batches
    ));
    out.push_str(&format!(
        "<DomainName>{}</DomainName>",
        esc(&dist.domain_name)
    ));
    out.push_str("<ActiveTrustedSigners><Enabled>false</Enabled><Quantity>0</Quantity></ActiveTrustedSigners>");
    out.push_str("<ActiveTrustedKeyGroups><Enabled>false</Enabled><Quantity>0</Quantity></ActiveTrustedKeyGroups>");
    let inner = quick_xml::se::to_string_with_root("DistributionConfig", &dist.config)
        .unwrap_or_else(|_| String::new());
    out.push_str(&inner);
    out.push_str("</Distribution>");
    out
}

fn build_distribution_list_xml(
    dists: &[StoredDistribution],
    root: &str,
    marker: &str,
    max_items: usize,
    next_marker: Option<&str>,
) -> String {
    let mut out = String::with_capacity(2048);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    out.push_str(&format!("<{root} xmlns=\"{ns}\">", ns = crate::NAMESPACE));
    out.push_str(&format!("<Marker>{}</Marker>", esc(marker)));
    if let Some(nm) = next_marker {
        out.push_str(&format!("<NextMarker>{}</NextMarker>", esc(nm)));
    }
    out.push_str(&format!("<MaxItems>{max_items}</MaxItems>"));
    out.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        next_marker.is_some()
    ));
    out.push_str(&format!("<Quantity>{}</Quantity>", dists.len()));
    if dists.is_empty() {
        out.push_str(&format!("</{root}>"));
        return out;
    }
    out.push_str("<Items>");
    for d in dists {
        out.push_str("<DistributionSummary>");
        out.push_str(&format!("<Id>{}</Id>", esc(&d.id)));
        out.push_str(&format!("<ARN>{}</ARN>", esc(&d.arn)));
        out.push_str(&format!("<Status>{}</Status>", esc(&d.status)));
        out.push_str(&format!(
            "<LastModifiedTime>{}</LastModifiedTime>",
            rfc3339(&d.last_modified_time)
        ));
        out.push_str(&format!("<DomainName>{}</DomainName>", esc(&d.domain_name)));
        let aliases = d.config.aliases.clone().unwrap_or_default();
        out.push_str(&render_inline("Aliases", &aliases));
        let origins = d.config.origins.clone();
        out.push_str(&render_inline("Origins", &origins));
        let dcb = d.config.default_cache_behavior.clone();
        out.push_str(&render_inline("DefaultCacheBehavior", &dcb));
        let cb = d.config.cache_behaviors.clone().unwrap_or_default();
        out.push_str(&render_inline("CacheBehaviors", &cb));
        let cer = d.config.custom_error_responses.clone().unwrap_or_default();
        out.push_str(&render_inline("CustomErrorResponses", &cer));
        out.push_str(&format!("<Comment>{}</Comment>", esc(&d.config.comment)));
        out.push_str(&format!(
            "<PriceClass>{}</PriceClass>",
            esc(&d
                .config
                .price_class
                .clone()
                .unwrap_or_else(|| "PriceClass_All".to_string()))
        ));
        out.push_str(&format!("<Enabled>{}</Enabled>", d.config.enabled));
        out.push_str(&render_inline(
            "ViewerCertificate",
            &d.config.viewer_certificate.clone().unwrap_or_default(),
        ));
        out.push_str(&render_inline(
            "Restrictions",
            &d.config.restrictions.clone().unwrap_or_default(),
        ));
        out.push_str(&format!(
            "<WebACLId>{}</WebACLId>",
            esc(&d.config.web_acl_id.clone().unwrap_or_default())
        ));
        out.push_str(&format!(
            "<HttpVersion>{}</HttpVersion>",
            esc(&d
                .config
                .http_version
                .clone()
                .unwrap_or_else(|| "http2".to_string()))
        ));
        out.push_str(&format!(
            "<IsIPV6Enabled>{}</IsIPV6Enabled>",
            d.config.is_ipv6_enabled.unwrap_or(true)
        ));
        out.push_str(&format!(
            "<Staging>{}</Staging>",
            d.config.staging.unwrap_or(false)
        ));
        out.push_str("</DistributionSummary>");
    }
    out.push_str("</Items>");
    out.push_str(&format!("</{root}>"));
    out
}

fn build_empty_distribution_id_list(root: &str) -> String {
    let mut out = String::with_capacity(256);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    out.push_str(&format!("<{root} xmlns=\"{ns}\">", ns = crate::NAMESPACE));
    out.push_str("<Marker></Marker>");
    out.push_str("<MaxItems>100</MaxItems>");
    out.push_str("<IsTruncated>false</IsTruncated>");
    out.push_str("<Quantity>0</Quantity>");
    out.push_str(&format!("</{root}>"));
    out
}

/// Render a `DistributionIdList` response body listing the given ids, echoing
/// the Marker/MaxItems cursor and NextMarker/IsTruncated pagination metadata.
fn build_distribution_id_list_xml(
    ids: &[&str],
    marker: &str,
    max_items: usize,
    next_marker: Option<&str>,
) -> String {
    let mut out = String::with_capacity(256);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    out.push_str(&format!(
        "<DistributionIdList xmlns=\"{ns}\">",
        ns = crate::NAMESPACE
    ));
    out.push_str(&format!("<Marker>{}</Marker>", esc(marker)));
    if let Some(nm) = next_marker {
        out.push_str(&format!("<NextMarker>{}</NextMarker>", esc(nm)));
    }
    out.push_str(&format!("<MaxItems>{max_items}</MaxItems>"));
    out.push_str(&format!(
        "<IsTruncated>{}</IsTruncated>",
        next_marker.is_some()
    ));
    out.push_str(&format!("<Quantity>{}</Quantity>", ids.len()));
    if !ids.is_empty() {
        out.push_str("<Items>");
        for id in ids {
            out.push_str(&format!("<DistributionId>{}</DistributionId>", esc(id)));
        }
        out.push_str("</Items>");
    }
    out.push_str("</DistributionIdList>");
    out
}

/// Apply the CloudFront Marker/MaxItems pagination scheme (the same one the
/// plain `ListDistributions` handler uses) to an already-filtered, sorted set
/// of distributions. Returns the echoed marker, the effective MaxItems, the
/// page slice, and the NextMarker cursor when the result is truncated.
fn paginate_distributions(
    req: &AwsRequest,
    items: Vec<StoredDistribution>,
) -> (String, usize, Vec<StoredDistribution>, Option<String>) {
    let max_items = req
        .query_params
        .get("MaxItems")
        .or_else(|| req.query_params.get("maxitems"))
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(100);
    let marker = req
        .query_params
        .get("Marker")
        .or_else(|| req.query_params.get("marker"))
        .cloned()
        .unwrap_or_default();
    // CloudFront markers are EXCLUSIVE: a supplied Marker means "start after
    // this id". If the marker id isn't found, the page is empty (start past
    // the end), matching AWS.
    let start_idx = if marker.is_empty() {
        0
    } else {
        match items.iter().position(|d| d.id == marker) {
            Some(pos) => pos + 1,
            None => items.len(),
        }
    };
    let page: Vec<StoredDistribution> = items
        .iter()
        .skip(start_idx)
        .take(max_items)
        .cloned()
        .collect();
    // Truncated when unread items remain after this page. NextMarker is the id
    // of the LAST item on the current page (where listing left off), so the
    // caller passes it back as the next Marker to resume strictly after it.
    let has_more = start_idx + page.len() < items.len();
    let next_marker = if has_more {
        page.last().map(|d| d.id.clone())
    } else {
        None
    };
    (marker, max_items, page, next_marker)
}

/// Iterate the default cache behavior followed by every additional cache
/// behavior of a distribution.
fn distribution_cache_behaviors(
    d: &StoredDistribution,
) -> impl Iterator<Item = &crate::model::CacheBehavior> {
    d.config
        .cache_behaviors
        .as_ref()
        .and_then(|cb| cb.items.as_ref())
        .into_iter()
        .flat_map(|items| items.cache_behavior.iter())
}

fn distribution_uses_cache_policy(d: &StoredDistribution, id: &str) -> bool {
    d.config.default_cache_behavior.cache_policy_id.as_deref() == Some(id)
        || distribution_cache_behaviors(d).any(|b| b.cache_policy_id.as_deref() == Some(id))
}

fn distribution_uses_origin_request_policy(d: &StoredDistribution, id: &str) -> bool {
    d.config
        .default_cache_behavior
        .origin_request_policy_id
        .as_deref()
        == Some(id)
        || distribution_cache_behaviors(d)
            .any(|b| b.origin_request_policy_id.as_deref() == Some(id))
}

fn distribution_uses_response_headers_policy(d: &StoredDistribution, id: &str) -> bool {
    d.config
        .default_cache_behavior
        .response_headers_policy_id
        .as_deref()
        == Some(id)
        || distribution_cache_behaviors(d)
            .any(|b| b.response_headers_policy_id.as_deref() == Some(id))
}

fn trusted_key_groups_contains(tkg: Option<&crate::model::TrustedKeyGroups>, id: &str) -> bool {
    tkg.and_then(|g| g.items.as_ref())
        .map(|items| items.key_group.iter().any(|k| k == id))
        .unwrap_or(false)
}

fn distribution_uses_key_group(d: &StoredDistribution, id: &str) -> bool {
    trusted_key_groups_contains(
        d.config.default_cache_behavior.trusted_key_groups.as_ref(),
        id,
    ) || distribution_cache_behaviors(d)
        .any(|b| trusted_key_groups_contains(b.trusted_key_groups.as_ref(), id))
}

fn distribution_uses_vpc_origin(d: &StoredDistribution, id: &str) -> bool {
    d.config
        .origins
        .items
        .as_ref()
        .map(|items| {
            items.origin.iter().any(|o| {
                o.vpc_origin_config
                    .as_ref()
                    .map(|v| v.vpc_origin_id == id)
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

fn build_invalidation_xml(inv: &StoredInvalidation) -> String {
    let mut out = String::with_capacity(512);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    out.push_str(&format!(
        "<Invalidation xmlns=\"{ns}\">",
        ns = crate::NAMESPACE
    ));
    out.push_str(&format!("<Id>{}</Id>", esc(&inv.id)));
    out.push_str(&format!("<Status>{}</Status>", esc(&inv.status)));
    out.push_str(&format!(
        "<CreateTime>{}</CreateTime>",
        rfc3339(&inv.create_time)
    ));
    out.push_str(&render_inline("InvalidationBatch", &inv.batch));
    out.push_str("</Invalidation>");
    out
}

fn build_invalidation_list_xml(items: &[&StoredInvalidation]) -> String {
    let mut out = String::with_capacity(1024);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    out.push_str(&format!(
        "<InvalidationList xmlns=\"{ns}\">",
        ns = crate::NAMESPACE
    ));
    out.push_str("<Marker></Marker>");
    out.push_str("<MaxItems>100</MaxItems>");
    out.push_str("<IsTruncated>false</IsTruncated>");
    out.push_str(&format!("<Quantity>{}</Quantity>", items.len()));
    if !items.is_empty() {
        out.push_str("<Items>");
        for inv in items {
            out.push_str("<InvalidationSummary>");
            out.push_str(&format!("<Id>{}</Id>", esc(&inv.id)));
            out.push_str(&format!(
                "<CreateTime>{}</CreateTime>",
                rfc3339(&inv.create_time)
            ));
            out.push_str(&format!("<Status>{}</Status>", esc(&inv.status)));
            out.push_str("</InvalidationSummary>");
        }
        out.push_str("</Items>");
    }
    out.push_str("</InvalidationList>");
    out
}

fn build_tags_xml(tags: &[Tag]) -> String {
    let mut out = String::with_capacity(256);
    out.push_str("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    out.push_str(&format!("<Tags xmlns=\"{ns}\">", ns = crate::NAMESPACE));
    out.push_str("<Items>");
    for t in tags {
        out.push_str("<Tag>");
        out.push_str(&format!("<Key>{}</Key>", esc(&t.key)));
        if let Some(v) = &t.value {
            out.push_str(&format!("<Value>{}</Value>", esc(v)));
        }
        out.push_str("</Tag>");
    }
    out.push_str("</Items>");
    out.push_str("</Tags>");
    out
}

fn render_inline<T: serde::Serialize>(root: &str, value: &T) -> String {
    quick_xml::se::to_string_with_root(root, value).unwrap_or_default()
}

// ─── Helpers ──────────────────────────────────────────────────────────

fn validate_caller_reference(s: &str) -> Result<(), AwsServiceError> {
    if s.is_empty() {
        return Err(invalid_argument("CallerReference is required"));
    }
    Ok(())
}

fn validate_origins(config: &DistributionConfig) -> Result<(), AwsServiceError> {
    if config.origins.quantity < 1 {
        return Err(invalid_argument(
            "DistributionConfig.Origins must contain at least one origin",
        ));
    }
    Ok(())
}

/// Compare two `DistributionConfig`s by serializing them to canonical XML
/// and comparing bytes. Used by `UpdateDistribution` to detect no-op writes
/// so the ETag stays stable when the caller PUTs the same config back.
/// Falls back to "not equal" if either serialization fails so we still
/// honor the request rather than silently swallow a write.
fn configs_equal(lhs: &DistributionConfig, rhs: &DistributionConfig) -> bool {
    let Ok(a) = xml_io::to_xml_root("DistributionConfig", lhs) else {
        return false;
    };
    let Ok(b) = xml_io::to_xml_root("DistributionConfig", rhs) else {
        return false;
    };
    a == b
}

fn account_id(_req: &AwsRequest) -> &'static str {
    // Multi-account is wired through AwsRequest.account_id elsewhere; the
    // CloudFront control plane only uses the resolved id for the ARN
    // suffix. Until that field stabilizes for REST-XML we use the default
    // account ID consistently with the rest of the registered services.
    DEFAULT_ACCOUNT
}

fn generate_distribution_id() -> String {
    // CloudFront IDs are 14-char base32-ish uppercase strings starting with E.
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    format!("E{}", &raw[..13])
}

fn generate_invalidation_id() -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    format!("I{}", &raw[..13])
}

pub(crate) fn generate_etag() -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    format!("E{}", &raw[..13])
}

/// Generate an AWS-shaped CloudFront resource ID with the given prefix.
/// Used by Batch 2 policy resources (cache, origin request, response
/// headers, continuous deployment, OAC) so each gets a recognizable
/// alphabetic prefix in addition to the random suffix.
pub(crate) fn generate_id_with_prefix(prefix: &str) -> String {
    let raw = Uuid::new_v4().simple().to_string().to_uppercase();
    let suffix_len = 14usize.saturating_sub(prefix.len()).min(raw.len());
    format!("{prefix}{}", &raw[..suffix_len])
}

fn rfc3339(t: &chrono::DateTime<chrono::Utc>) -> String {
    t.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

pub(crate) fn invalid_argument(msg: impl Into<String>) -> AwsServiceError {
    aws_error(StatusCode::BAD_REQUEST, "InvalidArgument", msg)
}

fn no_such_distribution(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchDistribution",
        format!("The specified distribution does not exist: {id}"),
    )
}

fn no_such_invalidation(id: &str) -> AwsServiceError {
    aws_error(
        StatusCode::NOT_FOUND,
        "NoSuchInvalidation",
        format!("The specified invalidation does not exist: {id}"),
    )
}

fn internal_error(msg: impl Into<String>) -> AwsServiceError {
    aws_error(StatusCode::INTERNAL_SERVER_ERROR, "InternalError", msg)
}

pub(crate) fn aws_error(
    status: StatusCode,
    code: impl Into<String>,
    msg: impl Into<String>,
) -> AwsServiceError {
    AwsServiceError::aws_error(status, code.into(), msg)
}

fn set_header(headers: &mut HeaderMap, name: HeaderName, value: &str) {
    if let Ok(v) = HeaderValue::from_str(value) {
        headers.insert(name, v);
    }
}

pub(crate) fn xml_response(status: StatusCode, body: String, headers: HeaderMap) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "text/xml".to_string(),
        body: ResponseBody::Bytes(Bytes::from(body)),
        headers,
    }
}

fn empty_response(status: StatusCode) -> AwsResponse {
    AwsResponse {
        status,
        content_type: "text/xml".to_string(),
        body: ResponseBody::Bytes(Bytes::new()),
        headers: HeaderMap::new(),
    }
}

fn percent_decode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'%' && i + 2 < bytes.len() {
            if let (Some(a), Some(c)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2])) {
                out.push(((a << 4) | c) as char);
                i += 3;
                continue;
            }
        }
        if b == b'+' {
            out.push(' ');
        } else {
            out.push(b as char);
        }
        i += 1;
    }
    out
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// True when a URL-decoded label segment looks like an unsubstituted Smithy
/// URI placeholder (e.g. `{Identifier}` or its percent-encoded form
/// `%7BIdentifier%7D`). Conformance probes that omit a required `@httpLabel`
/// input leave the literal placeholder in the URL; handlers that can
/// short-circuit on it return the declared validation error instead of a
/// fabricated 200.
pub(crate) fn is_placeholder_label(value: &str) -> bool {
    if value.is_empty() {
        return true;
    }
    let lower = value.to_ascii_lowercase();
    value.starts_with('{') || lower.starts_with("%7b")
}

/// Best-effort field extractor for request bodies the probe sends as JSON
/// even when the service is REST-XML. Walks the body trying JSON first,
/// then a naive XML element scan. Returns the value for the first occurrence
/// of `key`. Used by handlers that need to validate optional/required body
/// members up-front (enum bounds, length, presence) without committing to a
/// full strongly-typed parse — the actual handler still uses the typed XML
/// parser for the structured fields it consumes.
pub(crate) fn extract_body_field(body: &[u8], key: &str) -> Option<String> {
    if let Ok(s) = std::str::from_utf8(body) {
        let trimmed = s.trim_start();
        if trimmed.starts_with('{') {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
                if let Some(field) = v.get(key) {
                    return match field {
                        serde_json::Value::String(s) => Some(s.clone()),
                        serde_json::Value::Number(n) => Some(n.to_string()),
                        serde_json::Value::Bool(b) => Some(b.to_string()),
                        _ => None,
                    };
                }
                return None;
            }
        }
        // Fall back to a naive XML extraction for genuine XML bodies.
        let open = format!("<{key}>");
        let close = format!("</{key}>");
        if let Some(start) = s.find(&open) {
            let after = start + open.len();
            if let Some(end_rel) = s[after..].find(&close) {
                return Some(s[after..after + end_rel].to_string());
            }
        }
    }
    None
}

fn parse_query_value(query: &str, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        if let Some(rest) = pair.strip_prefix(&prefix) {
            return Some(percent_decode(rest));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn distribution_list_xml_renders_pagination_fields() {
        // Truncated page: echoes the request marker, the requested MaxItems,
        // IsTruncated=true, and the NextMarker cursor.
        let xml =
            build_distribution_list_xml(&[], "DistributionList", "EDFDVBD6", 2, Some("E2NEXT"));
        assert!(xml.contains("<Marker>EDFDVBD6</Marker>"));
        assert!(xml.contains("<MaxItems>2</MaxItems>"));
        assert!(xml.contains("<IsTruncated>true</IsTruncated>"));
        assert!(xml.contains("<NextMarker>E2NEXT</NextMarker>"));
        // Final page: no NextMarker, IsTruncated=false.
        let xml = build_distribution_list_xml(&[], "DistributionList", "", 100, None);
        assert!(xml.contains("<IsTruncated>false</IsTruncated>"));
        assert!(!xml.contains("<NextMarker>"));
    }

    #[test]
    fn placeholder_label_detects_braces_and_percent_encoding() {
        assert!(is_placeholder_label(""));
        assert!(is_placeholder_label("{Identifier}"));
        assert!(is_placeholder_label("%7BIdentifier%7D"));
        assert!(is_placeholder_label("%7bidentifier%7d"));
        assert!(!is_placeholder_label("E1234567890ABC"));
        assert!(!is_placeholder_label(
            "arn:aws:cloudfront::000:distribution/E1"
        ));
    }

    #[test]
    fn extract_body_field_handles_json_and_xml() {
        let json = br#"{"Stage":"BROKEN","Marker":"x"}"#;
        assert_eq!(
            extract_body_field(json, "Stage"),
            Some("BROKEN".to_string())
        );
        assert_eq!(extract_body_field(json, "MaxItems"), None);

        let xml = br#"<?xml version="1.0"?><Body><Domain>example.com</Domain></Body>"#;
        assert_eq!(
            extract_body_field(xml, "Domain"),
            Some("example.com".to_string())
        );
        assert_eq!(extract_body_field(xml, "Missing"), None);

        assert_eq!(extract_body_field(b"", "x"), None);
    }

    fn make_state() -> SharedCloudFrontState {
        Arc::new(RwLock::new(CloudFrontAccounts::new()))
    }

    fn make_request(method: http::Method, path: &str, query: &str, body: &str) -> AwsRequest {
        AwsRequest {
            service: "cloudfront".into(),
            action: String::new(),
            region: "us-east-1".into(),
            account_id: DEFAULT_ACCOUNT.into(),
            request_id: Uuid::new_v4().to_string(),
            headers: HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body_stream: parking_lot::Mutex::new(None),
            body: Bytes::from(body.to_string()),
            path_segments: path
                .split('/')
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
            raw_path: path.into(),
            raw_query: query.into(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn minimal_dist_config_xml(caller_ref: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">
  <CallerReference>{caller_ref}</CallerReference>
  <Origins>
    <Quantity>1</Quantity>
    <Items>
      <Origin>
        <Id>primary</Id>
        <DomainName>example.com</DomainName>
      </Origin>
    </Items>
  </Origins>
  <DefaultCacheBehavior>
    <TargetOriginId>primary</TargetOriginId>
    <ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>
  </DefaultCacheBehavior>
  <Comment></Comment>
  <Enabled>true</Enabled>
</DistributionConfig>"#
        )
    }

    #[tokio::test]
    async fn create_then_get_then_delete_distribution() {
        let svc = CloudFrontService::new(make_state());
        let body = minimal_dist_config_xml("ref-1");
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &body,
            ))
            .await
            .unwrap();
        assert_eq!(create.status, StatusCode::CREATED);
        let etag = create
            .headers
            .get(ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let xml = std::str::from_utf8(create.body.expect_bytes()).unwrap();
        let id = xml
            .split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string();

        let get = svc
            .handle(make_request(
                http::Method::GET,
                &format!("/2020-05-31/distribution/{id}"),
                "",
                "",
            ))
            .await
            .unwrap();
        assert_eq!(get.status, StatusCode::OK);

        // Disable then delete (CloudFront requires Disabled before delete).
        let disable_body = body.replace("<Enabled>true</Enabled>", "<Enabled>false</Enabled>");
        let mut update_req = make_request(
            http::Method::PUT,
            &format!("/2020-05-31/distribution/{id}/config"),
            "",
            &disable_body,
        );
        update_req.headers.insert(IF_MATCH, etag.parse().unwrap());
        let updated = svc.handle(update_req).await.unwrap();
        assert_eq!(updated.status, StatusCode::OK);
        let new_etag = updated
            .headers
            .get(ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let mut del_req = make_request(
            http::Method::DELETE,
            &format!("/2020-05-31/distribution/{id}"),
            "",
            "",
        );
        del_req.headers.insert(IF_MATCH, new_etag.parse().unwrap());
        let del = svc.handle(del_req).await.unwrap();
        assert_eq!(del.status, StatusCode::NO_CONTENT);
    }

    async fn create_distribution_returning_id(svc: &CloudFrontService, caller_ref: &str) -> String {
        let body = minimal_dist_config_xml(caller_ref);
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &body,
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(create.body.expect_bytes())
            .unwrap()
            .to_string();
        xml.split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string()
    }

    fn distribution_status(svc: &CloudFrontService, id: &str) -> String {
        let state = svc.state.read();
        state
            .accounts
            .get(DEFAULT_ACCOUNT)
            .and_then(|a| a.distributions.get(id))
            .map(|d| d.status.clone())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn create_distribution_starts_in_progress() {
        // Use a long delay so the auto-tick can't race the assertion.
        let svc = CloudFrontService::new(make_state())
            .with_propagation_delay(std::time::Duration::from_secs(60));
        let body = minimal_dist_config_xml("status-ref");
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &body,
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(create.body.expect_bytes())
            .unwrap()
            .to_string();
        assert!(
            xml.contains("<Status>InProgress</Status>"),
            "expected initial status InProgress, got: {xml}"
        );
    }

    #[tokio::test]
    async fn auto_transition_after_tick_marks_deployed() {
        let svc = CloudFrontService::new(make_state())
            .with_propagation_delay(std::time::Duration::from_millis(50));
        let id = create_distribution_returning_id(&svc, "auto-tick-ref").await;
        assert_eq!(distribution_status(&svc, &id), "InProgress");
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(distribution_status(&svc, &id), "Deployed");
    }

    #[tokio::test]
    async fn set_distribution_status_via_admin_flips_synchronously() {
        let svc = CloudFrontService::new(make_state())
            .with_propagation_delay(std::time::Duration::from_secs(60));
        let id = create_distribution_returning_id(&svc, "admin-ref").await;
        assert_eq!(distribution_status(&svc, &id), "InProgress");
        assert!(svc.set_distribution_status(&id, "Deployed"));
        assert_eq!(distribution_status(&svc, &id), "Deployed");
        assert!(svc.set_distribution_status(&id, "InProgress"));
        assert_eq!(distribution_status(&svc, &id), "InProgress");
    }

    #[tokio::test]
    async fn set_distribution_status_unknown_id_returns_false() {
        let svc = CloudFrontService::new(make_state());
        assert!(!svc.set_distribution_status("E-DOES-NOT-EXIST", "Deployed"));
    }

    #[tokio::test]
    async fn update_distribution_resets_to_in_progress() {
        let svc = CloudFrontService::new(make_state())
            .with_propagation_delay(std::time::Duration::from_secs(60));
        let body = minimal_dist_config_xml("update-reset-ref");
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &body,
            ))
            .await
            .unwrap();
        let etag = create
            .headers
            .get(ETAG)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let xml = std::str::from_utf8(create.body.expect_bytes())
            .unwrap()
            .to_string();
        let id = xml
            .split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string();
        // Force the distribution to Deployed via the admin mutator so we can
        // observe the UpdateDistribution flip back to InProgress.
        assert!(svc.set_distribution_status(&id, "Deployed"));
        assert_eq!(distribution_status(&svc, &id), "Deployed");

        let updated_body = body.replace(
            "<ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>",
            "<ViewerProtocolPolicy>https-only</ViewerProtocolPolicy>",
        );
        let mut update_req = make_request(
            http::Method::PUT,
            &format!("/2020-05-31/distribution/{id}/config"),
            "",
            &updated_body,
        );
        update_req.headers.insert(IF_MATCH, etag.parse().unwrap());
        let updated = svc.handle(update_req).await.unwrap();
        assert_eq!(updated.status, StatusCode::OK);
        assert_eq!(distribution_status(&svc, &id), "InProgress");
    }

    #[tokio::test]
    async fn duplicate_caller_reference_is_rejected() {
        let svc = CloudFrontService::new(make_state());
        let body = minimal_dist_config_xml("dup-ref");
        svc.handle(make_request(
            http::Method::POST,
            "/2020-05-31/distribution",
            "",
            &body,
        ))
        .await
        .unwrap();
        let result = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &body,
            ))
            .await;
        let err = match result {
            Ok(_) => panic!("expected duplicate caller-reference to fail"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "DistributionAlreadyExists");
        assert_eq!(err.status(), StatusCode::CONFLICT);
    }

    #[tokio::test]
    async fn invalidation_lifecycle() {
        let svc = CloudFrontService::new(make_state());
        let body = minimal_dist_config_xml("inv-ref");
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &body,
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(create.body.expect_bytes()).unwrap();
        let dist_id = xml
            .split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap();
        let inv_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<InvalidationBatch xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">
  <Paths><Quantity>1</Quantity><Items><Path>/*</Path></Items></Paths>
  <CallerReference>inv-1</CallerReference>
</InvalidationBatch>"#;
        let inv_resp = svc
            .handle(make_request(
                http::Method::POST,
                &format!("/2020-05-31/distribution/{dist_id}/invalidation"),
                "",
                inv_body,
            ))
            .await
            .unwrap();
        assert_eq!(inv_resp.status, StatusCode::CREATED);
        let inv_xml = std::str::from_utf8(inv_resp.body.expect_bytes()).unwrap();
        let inv_id = inv_xml
            .split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap();
        let get = svc
            .handle(make_request(
                http::Method::GET,
                &format!("/2020-05-31/distribution/{dist_id}/invalidation/{inv_id}"),
                "",
                "",
            ))
            .await
            .unwrap();
        assert_eq!(get.status, StatusCode::OK);
        let list = svc
            .handle(make_request(
                http::Method::GET,
                &format!("/2020-05-31/distribution/{dist_id}/invalidation"),
                "",
                "",
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(list.body.expect_bytes()).unwrap();
        assert!(xml.contains("<Quantity>1</Quantity>"));
    }

    #[tokio::test]
    async fn tags_roundtrip() {
        let svc = CloudFrontService::new(make_state());
        let body = minimal_dist_config_xml("tag-ref");
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &body,
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(create.body.expect_bytes()).unwrap();
        let arn = xml
            .split("<ARN>")
            .nth(1)
            .unwrap()
            .split("</ARN>")
            .next()
            .unwrap();
        let tag_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">
  <Items><Tag><Key>env</Key><Value>prod</Value></Tag></Items>
</Tags>"#;
        let arn_q = format!("Operation=Tag&Resource={}", arn);
        let resp = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/tagging",
                &arn_q,
                tag_body,
            ))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::NO_CONTENT);
        let list = svc
            .handle(make_request(
                http::Method::GET,
                "/2020-05-31/tagging",
                &format!("Resource={}", arn),
                "",
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(list.body.expect_bytes()).unwrap();
        assert!(xml.contains("<Key>env</Key>"));
        assert!(xml.contains("<Value>prod</Value>"));
    }

    #[tokio::test]
    async fn xml_metacharacters_in_user_input_are_escaped() {
        let svc = CloudFrontService::new(make_state());
        let body = minimal_dist_config_xml("escape-ref").replace(
            "<Comment></Comment>",
            "<Comment><![CDATA[a&b<c>d]]></Comment>",
        );
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &body,
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(create.body.expect_bytes()).unwrap();
        let dist_id = xml
            .split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap();
        let arn = xml
            .split("<ARN>")
            .nth(1)
            .unwrap()
            .split("</ARN>")
            .next()
            .unwrap();

        let tag_body = r#"<?xml version="1.0" encoding="UTF-8"?>
<Tags xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">
  <Items><Tag><Key>env</Key><Value>a&amp;b&lt;c&gt;d</Value></Tag></Items>
</Tags>"#;
        let arn_q = format!("Operation=Tag&Resource={}", arn);
        svc.handle(make_request(
            http::Method::POST,
            "/2020-05-31/tagging",
            &arn_q,
            tag_body,
        ))
        .await
        .unwrap();

        let list = svc
            .handle(make_request(
                http::Method::GET,
                "/2020-05-31/tagging",
                &format!("Resource={}", arn),
                "",
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(list.body.expect_bytes()).unwrap();
        assert!(xml.contains("<Value>a&amp;b&lt;c&gt;d</Value>"));
        assert!(!xml.contains("<Value>a&b<c>d</Value>"));

        // Force the distribution into the list-rendering path so the
        // unescaped Comment field would surface there.
        let list_resp = svc
            .handle(make_request(
                http::Method::GET,
                "/2020-05-31/distribution",
                "",
                "",
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(list_resp.body.expect_bytes()).unwrap();
        // Ensure raw `<` from the user-supplied comment never lands inside
        // a Comment element on the wire.
        assert!(!xml.contains("<Comment>a&b<c>d"));
        assert!(xml.contains(dist_id));
    }

    fn predicate_dist_config_xml(caller_ref: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">
  <CallerReference>{caller_ref}</CallerReference>
  <Origins>
    <Quantity>1</Quantity>
    <Items>
      <Origin>
        <Id>primary</Id>
        <DomainName>example.com</DomainName>
        <VpcOriginConfig><VpcOriginId>vo-123</VpcOriginId></VpcOriginConfig>
      </Origin>
    </Items>
  </Origins>
  <DefaultCacheBehavior>
    <TargetOriginId>primary</TargetOriginId>
    <ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>
    <CachePolicyId>cp-123</CachePolicyId>
    <OriginRequestPolicyId>orp-123</OriginRequestPolicyId>
    <ResponseHeadersPolicyId>rhp-123</ResponseHeadersPolicyId>
    <TrustedKeyGroups>
      <Enabled>true</Enabled>
      <Quantity>1</Quantity>
      <Items><KeyGroup>kg-123</KeyGroup></Items>
    </TrustedKeyGroups>
  </DefaultCacheBehavior>
  <Comment></Comment>
  <WebACLId>waf-abc</WebACLId>
  <AnycastIpListId>ail-123</AnycastIpListId>
  <Enabled>true</Enabled>
</DistributionConfig>"#
        )
    }

    async fn list_by(svc: &CloudFrontService, path: &str) -> String {
        let resp = svc
            .handle(make_request(http::Method::GET, path, "", ""))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        std::str::from_utf8(resp.body.expect_bytes())
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn list_distributions_by_predicate_fields_filters_state() {
        let svc = CloudFrontService::new(make_state());
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                &predicate_dist_config_xml("pred-ref"),
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(create.body.expect_bytes()).unwrap();
        let id = xml
            .split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string();

        // DistributionIdList-shaped ops: match id must appear, mismatch empty.
        for (path, root) in [
            ("/2020-05-31/distributionsByCachePolicyId/cp-123", "cp-123"),
            (
                "/2020-05-31/distributionsByOriginRequestPolicyId/orp-123",
                "orp-123",
            ),
            (
                "/2020-05-31/distributionsByResponseHeadersPolicyId/rhp-123",
                "rhp-123",
            ),
            ("/2020-05-31/distributionsByKeyGroupId/kg-123", "kg-123"),
            ("/2020-05-31/distributionsByVpcOriginId/vo-123", "vo-123"),
        ] {
            let body = list_by(&svc, path).await;
            assert!(body.contains("<DistributionIdList"), "{root}: {body}");
            assert!(
                body.contains(&format!("<DistributionId>{id}</DistributionId>")),
                "{root} did not list distribution: {body}"
            );
            assert!(body.contains("<Quantity>1</Quantity>"), "{root}: {body}");
        }

        // Mismatched id -> empty.
        let empty = list_by(&svc, "/2020-05-31/distributionsByCachePolicyId/other").await;
        assert!(empty.contains("<Quantity>0</Quantity>"), "{empty}");
        assert!(!empty.contains("<DistributionId>"), "{empty}");

        // DistributionList-shaped ops: WebACLId + AnycastIpListId.
        let web = list_by(&svc, "/2020-05-31/distributionsByWebACLId/waf-abc").await;
        assert!(web.contains("<DistributionList"), "{web}");
        assert!(web.contains(&format!("<Id>{id}</Id>")), "{web}");

        let anycast = list_by(&svc, "/2020-05-31/distributionsByAnycastIpListId/ail-123").await;
        assert!(anycast.contains("<DistributionList"), "{anycast}");
        assert!(anycast.contains(&format!("<Id>{id}</Id>")), "{anycast}");

        let anycast_miss = list_by(&svc, "/2020-05-31/distributionsByAnycastIpListId/nope").await;
        assert!(
            anycast_miss.contains("<Quantity>0</Quantity>"),
            "{anycast_miss}"
        );
    }

    fn dist_config_with_cache_policy(caller_ref: &str, cache_policy: &str) -> String {
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DistributionConfig xmlns="http://cloudfront.amazonaws.com/doc/2020-05-31/">
  <CallerReference>{caller_ref}</CallerReference>
  <Origins>
    <Quantity>1</Quantity>
    <Items><Origin><Id>primary</Id><DomainName>example.com</DomainName></Origin></Items>
  </Origins>
  <DefaultCacheBehavior>
    <TargetOriginId>primary</TargetOriginId>
    <ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>
    <CachePolicyId>{cache_policy}</CachePolicyId>
  </DefaultCacheBehavior>
  <Comment></Comment>
  <Enabled>true</Enabled>
</DistributionConfig>"#
        )
    }

    async fn create_dist_returning_id(svc: &CloudFrontService, config_xml: &str) -> String {
        let create = svc
            .handle(make_request(
                http::Method::POST,
                "/2020-05-31/distribution",
                "",
                config_xml,
            ))
            .await
            .unwrap();
        let xml = std::str::from_utf8(create.body.expect_bytes()).unwrap();
        xml.split("<Id>")
            .nth(1)
            .unwrap()
            .split("</Id>")
            .next()
            .unwrap()
            .to_string()
    }

    fn make_request_with_params(path: &str, params: &[(&str, &str)]) -> AwsRequest {
        let mut r = make_request(http::Method::GET, path, "", "");
        r.query_params = params
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        r
    }

    #[tokio::test]
    async fn list_distributions_by_web_acl_id_null_lists_unassociated() {
        // Finding #2: WebACLId "null" lists distributions with no web ACL.
        let svc = CloudFrontService::new(make_state());
        let with_acl = create_dist_returning_id(
            &svc,
            &predicate_dist_config_xml("null-with-acl"), // has WebACLId waf-abc
        )
        .await;
        let without_acl =
            create_dist_returning_id(&svc, &dist_config_with_cache_policy("null-no-acl", "cp-x"))
                .await;

        let body = list_by(&svc, "/2020-05-31/distributionsByWebACLId/null").await;
        assert!(body.contains("<DistributionList"), "{body}");
        assert!(
            body.contains(&format!("<Id>{without_acl}</Id>")),
            "unassociated distribution missing from null listing: {body}"
        );
        assert!(
            !body.contains(&format!("<Id>{with_acl}</Id>")),
            "distribution WITH a web ACL leaked into the null listing: {body}"
        );
    }

    fn only_distribution_id(xml: &str) -> String {
        assert_eq!(
            xml.matches("<DistributionId>").count(),
            1,
            "expected exactly one DistributionId: {xml}"
        );
        xml.split("<DistributionId>")
            .nth(1)
            .unwrap()
            .split("</DistributionId>")
            .next()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn list_distributions_by_predicate_paginates() {
        // Finding #3: Marker/MaxItems pagination applies to the filtered set,
        // with EXCLUSIVE markers (NextMarker = last id on the page; the next
        // request resumes strictly after it).
        let svc = CloudFrontService::new(make_state());
        // Two distributions sharing the same cache policy.
        let d1 =
            create_dist_returning_id(&svc, &dist_config_with_cache_policy("pg-1", "cp-pg")).await;
        let d2 =
            create_dist_returning_id(&svc, &dist_config_with_cache_policy("pg-2", "cp-pg")).await;

        // First page: MaxItems=1 -> one id, truncated, NextMarker == that id.
        let page1 = svc
            .handle(make_request_with_params(
                "/2020-05-31/distributionsByCachePolicyId/cp-pg",
                &[("MaxItems", "1")],
            ))
            .await
            .unwrap();
        let xml1 = std::str::from_utf8(page1.body.expect_bytes()).unwrap();
        assert!(xml1.contains("<MaxItems>1</MaxItems>"), "{xml1}");
        assert!(xml1.contains("<IsTruncated>true</IsTruncated>"), "{xml1}");
        let first_id = only_distribution_id(xml1);
        let next = xml1
            .split("<NextMarker>")
            .nth(1)
            .expect("NextMarker present")
            .split("</NextMarker>")
            .next()
            .unwrap()
            .to_string();
        // Exclusive marker: NextMarker is the LAST id on the current page.
        assert_eq!(next, first_id, "NextMarker must be the last id on the page");

        // Second page via the cursor: resumes AFTER the marker -> the other id,
        // never the marker item again, and not truncated.
        let page2 = svc
            .handle(make_request_with_params(
                "/2020-05-31/distributionsByCachePolicyId/cp-pg",
                &[("MaxItems", "1"), ("Marker", &next)],
            ))
            .await
            .unwrap();
        let xml2 = std::str::from_utf8(page2.body.expect_bytes()).unwrap();
        assert!(xml2.contains(&format!("<Marker>{next}</Marker>")), "{xml2}");
        assert!(xml2.contains("<IsTruncated>false</IsTruncated>"), "{xml2}");
        let second_id = only_distribution_id(xml2);
        assert_ne!(
            second_id, first_id,
            "exclusive marker must not return the marker item again"
        );
        // The two pages together cover both distributions exactly once.
        let mut seen = [first_id, second_id];
        seen.sort();
        let mut expected = [d1, d2];
        expected.sort();
        assert_eq!(seen, expected, "pages did not cover both distributions");
    }
}
