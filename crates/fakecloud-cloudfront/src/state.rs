//! In-memory state for CloudFront resources.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::cfunctions::StoredConnectionFunction;
use crate::extras::{StoredAnycastIpList, StoredResourcePolicy, StoredTrustStore, StoredVpcOrigin};
use crate::extras2::StoredConnectionGroup;
use crate::fle::{
    StoredFieldLevelEncryption, StoredFieldLevelEncryptionProfile, StoredRealtimeLogConfig,
};
use crate::functions::{
    StoredFunction, StoredKeyGroup, StoredKeyValueStore, StoredMonitoringSubscription,
    StoredOriginAccessIdentity, StoredPublicKey,
};
use crate::model::{DistributionConfig, InvalidationBatch};
use crate::policies::{
    StoredCachePolicy, StoredContinuousDeploymentPolicy, StoredOriginAccessControl,
    StoredOriginRequestPolicy, StoredResponseHeadersPolicy,
};
use crate::streaming::StoredStreamingDistribution;
use crate::tenants::{StoredDistributionTenant, StoredTenantInvalidation};

pub type SharedCloudFrontState = Arc<RwLock<CloudFrontAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CloudFrontAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

/// On-disk snapshot envelope for CloudFront state. Versioned so format changes
/// fail loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct CloudFrontSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<CloudFrontAccounts>,
}

pub const CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

impl CloudFrontAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn account_count(&self) -> usize {
        self.accounts.len()
    }

    pub fn entry(&mut self, account_id: &str) -> &mut AccountState {
        self.accounts.entry(account_id.to_string()).or_default()
    }

    pub fn get(&self, account_id: &str) -> Option<&AccountState> {
        self.accounts.get(account_id)
    }

    /// Iterate every stored distribution across all accounts, paired with the
    /// owning account id. Used by the `/_fakecloud/cloudfront/distributions`
    /// introspection route (and, later, the data-plane supervisor).
    pub fn all_distributions(&self) -> impl Iterator<Item = (&String, &StoredDistribution)> {
        self.accounts.iter().flat_map(|(account_id, state)| {
            state.distributions.values().map(move |d| (account_id, d))
        })
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    pub distributions: BTreeMap<String, StoredDistribution>,
    pub invalidations: BTreeMap<String, StoredInvalidation>,
    /// Tags keyed by ARN.
    pub tags: BTreeMap<String, Vec<Tag>>,
    pub origin_access_controls: BTreeMap<String, StoredOriginAccessControl>,
    pub cache_policies: BTreeMap<String, StoredCachePolicy>,
    pub origin_request_policies: BTreeMap<String, StoredOriginRequestPolicy>,
    pub response_headers_policies: BTreeMap<String, StoredResponseHeadersPolicy>,
    pub continuous_deployment_policies: BTreeMap<String, StoredContinuousDeploymentPolicy>,
    pub functions: BTreeMap<String, StoredFunction>,
    pub public_keys: BTreeMap<String, StoredPublicKey>,
    pub key_groups: BTreeMap<String, StoredKeyGroup>,
    pub key_value_stores: BTreeMap<String, StoredKeyValueStore>,
    pub origin_access_identities: BTreeMap<String, StoredOriginAccessIdentity>,
    /// Per-distribution monitoring subscription, keyed by distribution id.
    pub monitoring_subscriptions: BTreeMap<String, StoredMonitoringSubscription>,
    pub streaming_distributions: BTreeMap<String, StoredStreamingDistribution>,
    pub field_level_encryptions: BTreeMap<String, StoredFieldLevelEncryption>,
    pub field_level_encryption_profiles: BTreeMap<String, StoredFieldLevelEncryptionProfile>,
    /// Realtime log configs keyed by ARN.
    pub realtime_log_configs: BTreeMap<String, StoredRealtimeLogConfig>,
    pub vpc_origins: BTreeMap<String, StoredVpcOrigin>,
    pub anycast_ip_lists: BTreeMap<String, StoredAnycastIpList>,
    pub trust_stores: BTreeMap<String, StoredTrustStore>,
    /// Resource policies keyed by resource ARN.
    pub resource_policies: BTreeMap<String, StoredResourcePolicy>,
    pub connection_groups: BTreeMap<String, StoredConnectionGroup>,
    pub distribution_tenants: BTreeMap<String, StoredDistributionTenant>,
    pub tenant_invalidations: BTreeMap<String, StoredTenantInvalidation>,
    pub connection_functions: BTreeMap<String, StoredConnectionFunction>,
}

impl CloudFrontAccounts {
    /// Pre-seed the AWS-managed Cache, Origin Request, and Response
    /// Headers policies into the default account so callers that look
    /// them up by their well-known IDs (Terraform, CDK) get the same
    /// shape they get against AWS. The IDs and names mirror the AWS
    /// console output verbatim — the easiest way to keep tests source
    /// of truth.
    pub fn seed_managed_policies(&mut self, account_id: &str) {
        let account = self.entry(account_id);
        crate::policies::seed_managed(account);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredDistribution {
    pub id: String,
    pub arn: String,
    pub status: String,
    pub last_modified_time: DateTime<Utc>,
    pub domain_name: String,
    pub in_progress_invalidation_batches: u32,
    pub etag: String,
    pub config: DistributionConfig,
    /// Data-plane listener port (fakecloud extension; None until the
    /// supervisor binds a listener for an enabled distribution). Not part
    /// of the AWS API surface — surfaced only via /_fakecloud/cloudfront/*.
    /// Runtime-only: never persisted (restarts get a fresh port on the first
    /// reconcile tick), mirroring the ELBv2 data plane's `bound_port`.
    #[serde(skip)]
    pub bound_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredInvalidation {
    pub id: String,
    pub distribution_id: String,
    pub status: String,
    pub create_time: DateTime<Utc>,
    pub batch: InvalidationBatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
    pub key: String,
    pub value: Option<String>,
}
