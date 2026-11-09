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

/// v2: distribution config members AWS spells with an upper-case acronym
/// (`MinTTL`, `IsIPV6Enabled`, `ACMCertificateArn`, ...) are stored under that
/// spelling; v1 wrote the PascalCase form (`MinTtl`, ...).
///
/// v3: resources live under the account that created them. v1/v2 kept every
/// resource in one [`LEGACY_ACCOUNT`] bucket whatever the caller's account.
pub const CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

/// The single account bucket v1/v2 snapshots stored every resource under.
const LEGACY_ACCOUNT: &str = "000000000000";

/// Distribution config member names v1 snapshots wrote, paired with the AWS
/// spelling v2 uses.
const V1_DISTRIBUTION_CONFIG_RENAMES: &[(&str, &str)] = &[
    ("MinTtl", "MinTTL"),
    ("DefaultTtl", "DefaultTTL"),
    ("MaxTtl", "MaxTTL"),
    ("ErrorCachingMinTtl", "ErrorCachingMinTTL"),
    ("IsIpv6Enabled", "IsIPV6Enabled"),
    ("IamCertificateId", "IAMCertificateId"),
    ("AcmCertificateArn", "ACMCertificateArn"),
    ("SslSupportMethod", "SSLSupportMethod"),
    ("FunctionArn", "FunctionARN"),
    ("LambdaFunctionArn", "LambdaFunctionARN"),
];

/// Parse an on-disk CloudFront snapshot, migrating older schema versions.
///
/// The migration rewrites the stored JSON rather than teaching the model
/// structs the old names, so the old spellings never become accepted on the
/// XML wire.
///
/// `default_account` is the server's configured account: a pre-v3 snapshot's
/// single shared bucket is moved there, since every resource in it was created
/// before CloudFront state was scoped to the caller's account.
pub fn parse_cloudfront_snapshot(
    bytes: &[u8],
    default_account: &str,
) -> Result<CloudFrontSnapshot, serde_json::Error> {
    let mut value: serde_json::Value = serde_json::from_slice(bytes)?;
    let version = value
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    if version < 2 {
        migrate_v1_distribution_configs(&mut value);
    }
    if version < 3 {
        migrate_legacy_bucket(&mut value, default_account);
    }
    let mut snapshot: CloudFrontSnapshot = serde_json::from_value(value)?;
    // A snapshot written before configs were made canonical on the way in must
    // read back like any other distribution, and an unchanged config PUT over
    // it must still compare equal. Filling only absent members is idempotent,
    // so this needs no schema bump.
    for account in snapshot
        .accounts
        .iter_mut()
        .flat_map(|a| a.accounts.values_mut())
    {
        for dist in account.distributions.values_mut() {
            dist.config.ensure_aws_defaults();
        }
    }
    Ok(snapshot)
}

/// Split the pre-v3 shared bucket back out by owner.
///
/// Each entry goes to the account its own CloudFront ARN names (the
/// CloudFormation provisioner stamped the stack's account into the ARNs it
/// minted even though it stored under the shared bucket), invalidations and
/// monitoring subscriptions follow their distribution, tenant invalidations
/// follow their tenant, and everything else (entries with no ARN, or ARNs the
/// API minted under the legacy account) lands in `default_account`. The
/// legacy account segment of every CloudFront ARN moved to `default_account`
/// is rewritten to it, keys included (tags, resource policies and realtime
/// log configs are keyed by ARN). Entries already stored under the owning
/// account win over legacy ones with the same key.
fn migrate_legacy_bucket(snapshot: &mut serde_json::Value, default_account: &str) {
    use serde_json::{Map, Value};

    let Some(accounts) = snapshot
        .pointer_mut("/accounts/accounts")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    let Some(Value::Object(legacy)) = accounts.remove(LEGACY_ACCOUNT) else {
        return;
    };

    // Owner of every distribution and tenant, for the entries that only
    // reference one by id.
    let owners_by_id = |field: &str| -> BTreeMap<String, String> {
        legacy
            .get(field)
            .and_then(Value::as_object)
            .map(|entries| {
                entries
                    .iter()
                    .filter_map(|(id, e)| {
                        let owner = e.get("arn").and_then(Value::as_str).and_then(arn_owner)?;
                        Some((id.clone(), owner))
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let distribution_owners = owners_by_id("distributions");
    let tenant_owners = owners_by_id("distribution_tenants");

    let owner_of = |field: &str, key: &str, entry: &Value| -> Option<String> {
        if let Some(owner) = arn_owner(key) {
            return Some(owner);
        }
        for arn_field in ["arn", "function_arn", "resource_arn"] {
            if let Some(owner) = entry
                .get(arn_field)
                .and_then(Value::as_str)
                .and_then(arn_owner)
            {
                return Some(owner);
            }
        }
        match field {
            "invalidations" => entry
                .get("distribution_id")
                .and_then(Value::as_str)
                .and_then(|id| distribution_owners.get(id).cloned()),
            "monitoring_subscriptions" => distribution_owners.get(key).cloned(),
            "tenant_invalidations" => entry
                .get("tenant_id")
                .and_then(Value::as_str)
                .and_then(|id| tenant_owners.get(id).cloned()),
            _ => None,
        }
    };

    let from = format!(":cloudfront::{LEGACY_ACCOUNT}:");
    let to = format!(":cloudfront::{default_account}:");
    for (field, entries) in &legacy {
        let Value::Object(entries) = entries else {
            continue;
        };
        for (key, entry) in entries {
            let owner = owner_of(field, key, entry).unwrap_or_else(|| default_account.to_string());
            let (key, entry) = if owner == default_account {
                let mut entry = entry.clone();
                rewrite_arn_account(&mut entry, &from, &to);
                (key.replace(&from, &to), entry)
            } else {
                (key.clone(), entry.clone())
            };
            let bucket = accounts
                .entry(owner)
                .or_insert_with(|| Value::Object(Map::new()));
            let Some(bucket) = bucket.as_object_mut() else {
                continue;
            };
            let slot = bucket
                .entry(field.clone())
                .or_insert_with(|| Value::Object(Map::new()));
            if let Some(slot) = slot.as_object_mut() {
                slot.entry(key).or_insert(entry);
            }
        }
    }
    // Buckets created above only carry the fields they received entries for;
    // give every bucket the full set of (possibly empty) maps.
    accounts
        .entry(default_account.to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    for bucket in accounts.values_mut() {
        if let Some(bucket) = bucket.as_object_mut() {
            for field in legacy.keys() {
                bucket
                    .entry(field.clone())
                    .or_insert_with(|| Value::Object(Map::new()));
            }
        }
    }
}

/// The owning account a CloudFront ARN names, unless it is the legacy shared
/// account (which carries no ownership information).
fn arn_owner(arn: &str) -> Option<String> {
    let rest = arn.strip_prefix("arn:")?;
    let mut parts = rest.splitn(5, ':');
    let (_partition, service, _region, account) =
        (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    (service == "cloudfront"
        && account.len() == 12
        && account.bytes().all(|b| b.is_ascii_digit())
        && account != LEGACY_ACCOUNT)
        .then(|| account.to_string())
}

fn rewrite_arn_account(value: &mut serde_json::Value, from: &str, to: &str) {
    match value {
        serde_json::Value::String(s) if s.contains(from) => *s = s.replace(from, to),
        serde_json::Value::Object(map) => {
            let entries = std::mem::take(map);
            for (key, mut v) in entries {
                rewrite_arn_account(&mut v, from, to);
                map.insert(key.replace(from, to), v);
            }
        }
        serde_json::Value::Array(items) => items
            .iter_mut()
            .for_each(|v| rewrite_arn_account(v, from, to)),
        _ => {}
    }
}

fn migrate_v1_distribution_configs(snapshot: &mut serde_json::Value) {
    let Some(accounts) = snapshot
        .pointer_mut("/accounts/accounts")
        .and_then(serde_json::Value::as_object_mut)
    else {
        return;
    };
    for account in accounts.values_mut() {
        let Some(distributions) = account
            .get_mut("distributions")
            .and_then(serde_json::Value::as_object_mut)
        else {
            continue;
        };
        for distribution in distributions.values_mut() {
            if let Some(config) = distribution.get_mut("config") {
                rename_keys(config);
            }
        }
    }
}

fn rename_keys(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (old, new) in V1_DISTRIBUTION_CONFIG_RENAMES {
                if let Some(v) = map.remove(*old) {
                    map.insert((*new).to_string(), v);
                }
            }
            map.values_mut().for_each(rename_keys);
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(rename_keys),
        _ => {}
    }
}

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

    /// Draw ids from `generate` until one is held by no account.
    ///
    /// CloudFront is global: a distribution's id is also its
    /// `<id>.cloudfront.net` domain, and the propagation ticks, the admin
    /// status endpoint and the data plane find resources by id or domain
    /// without knowing the owning account. Ids therefore stay unique across
    /// every account even though the resources themselves are per-account.
    pub fn unused_id(
        &self,
        generate: impl Fn() -> String,
        held: impl Fn(&AccountState, &str) -> bool,
    ) -> String {
        loop {
            let id = generate();
            if !self.accounts.values().any(|a| held(a, &id)) {
                return id;
            }
        }
    }

    /// Whether `domain` is an alternate domain name of any distribution, or a
    /// domain of any distribution tenant, in any account, other than the
    /// resource `owner` names.
    ///
    /// Domains are unique across all of CloudFront and are matched
    /// case-insensitively, as the data plane routes `Host` headers.
    pub(crate) fn domain_in_use(&self, domain: &str, owner: DomainOwner<'_>) -> bool {
        self.accounts.values().any(|a| {
            a.distributions.values().any(|d| {
                owner != DomainOwner::Distribution(&d.id)
                    && d.config
                        .aliases
                        .as_ref()
                        .and_then(|al| al.items.as_ref())
                        .is_some_and(|i| i.cname.iter().any(|c| c.eq_ignore_ascii_case(domain)))
            }) || a.distribution_tenants.values().any(|t| {
                owner != DomainOwner::Tenant(&t.id)
                    && t.domains.iter().any(|d| d.eq_ignore_ascii_case(domain))
            })
        })
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

/// The resource a domain is being attached to, excluded from
/// [`CloudFrontAccounts::domain_in_use`] so re-submitting its own domains is
/// not a conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DomainOwner<'a> {
    New,
    Distribution(&'a str),
    Tenant(&'a str),
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

#[cfg(test)]
mod snapshot_migration_tests {
    use super::*;

    /// A config exercising every member v2 renamed.
    fn config_with_renamed_members() -> serde_json::Value {
        serde_json::json!({
            "CallerReference": "ref",
            "Origins": {"Quantity": 0},
            "DefaultCacheBehavior": {
                "TargetOriginId": "o1",
                "ViewerProtocolPolicy": "allow-all",
                "MinTtl": 1, "DefaultTtl": 2, "MaxTtl": 3,
                "FunctionAssociations": {"Quantity": 1, "Items": {"FunctionAssociation": [
                    {"FunctionArn": "arn:f", "EventType": "viewer-request"}]}},
                "LambdaFunctionAssociations": {"Quantity": 1, "Items": {"LambdaFunctionAssociation": [
                    {"LambdaFunctionArn": "arn:l", "EventType": "origin-request"}]}}
            },
            "CustomErrorResponses": {"Quantity": 1, "Items": {"CustomErrorResponse": [
                {"ErrorCode": 404, "ErrorCachingMinTtl": 10}]}},
            "Comment": "",
            "Enabled": true,
            "ViewerCertificate": {"IamCertificateId": "iam-1", "AcmCertificateArn": "arn:c", "SslSupportMethod": "sni-only"},
            "IsIpv6Enabled": false
        })
    }

    /// A v1 snapshot: a current one with the distribution config swapped for
    /// one written under the PascalCase names.
    fn v1_snapshot_bytes() -> Vec<u8> {
        let mut accounts = CloudFrontAccounts::new();
        accounts.entry("000000000000").distributions.insert(
            "E1".to_string(),
            StoredDistribution {
                id: "E1".to_string(),
                arn: "arn".to_string(),
                status: "Deployed".to_string(),
                last_modified_time: Utc::now(),
                domain_name: "e1.cloudfront.net".to_string(),
                in_progress_invalidation_batches: 0,
                etag: "T".to_string(),
                config: DistributionConfig::default(),
            },
        );
        let mut value = serde_json::to_value(CloudFrontSnapshot {
            schema_version: 1,
            accounts: Some(accounts),
        })
        .unwrap();
        *value
            .pointer_mut("/accounts/accounts/000000000000/distributions/E1/config")
            .unwrap() = config_with_renamed_members();
        serde_json::to_vec(&value).unwrap()
    }

    #[test]
    fn a_v1_snapshot_loads_with_its_pascal_case_member_names_migrated() {
        let snapshot = parse_cloudfront_snapshot(&v1_snapshot_bytes(), LEGACY_ACCOUNT)
            .expect("v1 snapshot loads");
        let accounts = snapshot.accounts.unwrap();
        let config = &accounts.get("000000000000").unwrap().distributions["E1"].config;
        let dcb = &config.default_cache_behavior;
        assert_eq!(
            (dcb.min_ttl, dcb.default_ttl, dcb.max_ttl),
            (Some(1), Some(2), Some(3))
        );
        let fa = dcb.function_associations.as_ref().unwrap();
        assert_eq!(
            fa.items.as_ref().unwrap().function_association[0].function_arn,
            "arn:f"
        );
        let la = dcb.lambda_function_associations.as_ref().unwrap();
        assert_eq!(
            la.items.as_ref().unwrap().lambda_function_association[0].lambda_function_arn,
            "arn:l"
        );
        let rules = config.custom_error_responses.as_ref().unwrap();
        assert_eq!(
            rules.items.as_ref().unwrap().custom_error_response[0].error_caching_min_ttl,
            Some(10)
        );
        let vc = config.viewer_certificate.as_ref().unwrap();
        assert_eq!(vc.iam_certificate_id.as_deref(), Some("iam-1"));
        assert_eq!(vc.acm_certificate_arn.as_deref(), Some("arn:c"));
        assert_eq!(vc.ssl_support_method.as_deref(), Some("sni-only"));
        assert_eq!(config.is_ipv6_enabled, Some(false));
    }

    #[test]
    fn a_snapshot_with_a_sparse_config_loads_with_the_cloudfront_defaults() {
        let mut accounts = CloudFrontAccounts::new();
        accounts
            .entry("000000000000")
            .distributions
            .insert("E1".to_string(), legacy_distribution("E1", "000000000000"));
        let bytes = serde_json::to_vec(&CloudFrontSnapshot {
            schema_version: CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
        })
        .unwrap();
        // The struct-built config above carries none of the optional members.
        assert!(!String::from_utf8_lossy(&bytes).contains("OriginGroups"));

        let snapshot = parse_cloudfront_snapshot(&bytes, LEGACY_ACCOUNT).unwrap();
        let accounts = snapshot.accounts.unwrap();
        let config = &accounts.get("000000000000").unwrap().distributions["E1"].config;
        assert_eq!(config.origin_groups.as_ref().unwrap().quantity, 0);
        assert_eq!(config.aliases.as_ref().unwrap().quantity, 0);
        assert_eq!(config.http_version.as_deref(), Some("http2"));
        let geo = &config.restrictions.as_ref().unwrap().geo_restriction;
        assert_eq!(geo.restriction_type, "none");
        assert!(config.default_cache_behavior.trusted_signers.is_some());
    }

    #[test]
    fn the_migration_only_runs_for_v1_snapshots() {
        // A v2 snapshot is read as written: the PascalCase names are not
        // CloudFront member names, so the required `FunctionARN` is missing.
        let mut value: serde_json::Value = serde_json::from_slice(&v1_snapshot_bytes()).unwrap();
        value["schema_version"] = serde_json::json!(2);
        let err = parse_cloudfront_snapshot(&serde_json::to_vec(&value).unwrap(), LEGACY_ACCOUNT)
            .err()
            .expect("not migrated");
        assert!(err.to_string().contains("FunctionARN"), "{err}");
    }

    fn legacy_distribution(id: &str, account: &str) -> StoredDistribution {
        StoredDistribution {
            id: id.to_string(),
            arn: format!("arn:aws:cloudfront::{account}:distribution/{id}"),
            status: "Deployed".to_string(),
            last_modified_time: Utc::now(),
            domain_name: format!("{}.cloudfront.net", id.to_lowercase()),
            in_progress_invalidation_batches: 0,
            etag: "T".to_string(),
            config: DistributionConfig::default(),
        }
    }

    /// A v2 snapshot: every resource in the shared legacy bucket, with ARNs
    /// (and ARN-keyed tags) minted under the legacy account id.
    fn v2_snapshot_bytes(extra: impl FnOnce(&mut CloudFrontAccounts)) -> Vec<u8> {
        let mut accounts = CloudFrontAccounts::new();
        let legacy = accounts.entry(LEGACY_ACCOUNT);
        let dist = legacy_distribution("E1", LEGACY_ACCOUNT);
        legacy.tags.insert(
            dist.arn.clone(),
            vec![Tag {
                key: "team".to_string(),
                value: Some("web".to_string()),
            }],
        );
        legacy.distributions.insert("E1".to_string(), dist);
        extra(&mut accounts);
        serde_json::to_vec(&CloudFrontSnapshot {
            schema_version: 2,
            accounts: Some(accounts),
        })
        .unwrap()
    }

    #[test]
    fn a_v2_snapshot_moves_the_shared_bucket_into_the_default_account() {
        let parsed = parse_cloudfront_snapshot(&v2_snapshot_bytes(|_| {}), "123456789012")
            .expect("v2 snapshot loads");
        let accounts = parsed.accounts.unwrap();
        assert!(
            accounts.get(LEGACY_ACCOUNT).is_none(),
            "legacy bucket moved"
        );
        let owner = accounts.get("123456789012").expect("default account");
        let dist = &owner.distributions["E1"];
        assert_eq!(dist.arn, "arn:aws:cloudfront::123456789012:distribution/E1");
        assert_eq!(dist.domain_name, "e1.cloudfront.net");
        let tags = &owner.tags["arn:aws:cloudfront::123456789012:distribution/E1"];
        assert_eq!(tags[0].key, "team");
    }

    #[test]
    fn a_v2_snapshot_merges_into_an_existing_default_account_bucket() {
        let bytes = v2_snapshot_bytes(|accounts| {
            // An existing bucket for the default account keeps its entries.
            let dist = legacy_distribution("E2", "123456789012");
            accounts
                .entry("123456789012")
                .distributions
                .insert("E2".to_string(), dist);
        });
        let parsed = parse_cloudfront_snapshot(&bytes, "123456789012").unwrap();
        let accounts = parsed.accounts.unwrap();
        let owner = accounts.get("123456789012").unwrap();
        let mut ids: Vec<&String> = owner.distributions.keys().collect();
        ids.sort();
        assert_eq!(ids, ["E1", "E2"]);
        assert_eq!(accounts.account_count(), 1);
    }

    #[test]
    fn a_v2_snapshot_routes_stack_provisioned_entries_to_their_arn_account() {
        let bytes = v2_snapshot_bytes(|accounts| {
            // Pre-v3 CloudFormation stored in the shared bucket but minted
            // ARNs under the stack's account.
            let legacy = accounts.entry(LEGACY_ACCOUNT);
            legacy
                .distributions
                .insert("E3".to_string(), legacy_distribution("E3", "222222222222"));
            legacy.invalidations.insert(
                "I1".to_string(),
                StoredInvalidation {
                    id: "I1".to_string(),
                    distribution_id: "E3".to_string(),
                    status: "Completed".to_string(),
                    create_time: Utc::now(),
                    batch: InvalidationBatch::default(),
                },
            );
            legacy.tags.insert(
                "arn:aws:cloudfront::222222222222:distribution/E3".to_string(),
                vec![],
            );
        });
        let parsed = parse_cloudfront_snapshot(&bytes, "123456789012").unwrap();
        let accounts = parsed.accounts.unwrap();
        let stack = accounts.get("222222222222").expect("stack account bucket");
        assert!(stack.distributions.contains_key("E3"));
        assert!(
            stack.invalidations.contains_key("I1"),
            "follows its distribution"
        );
        assert!(stack
            .tags
            .contains_key("arn:aws:cloudfront::222222222222:distribution/E3"));
        let default = accounts.get("123456789012").unwrap();
        assert!(default.distributions.contains_key("E1"));
        assert!(!default.distributions.contains_key("E3"));

        // Same split when the default account is the legacy one.
        let parsed = parse_cloudfront_snapshot(&bytes, LEGACY_ACCOUNT).unwrap();
        let accounts = parsed.accounts.unwrap();
        assert!(accounts
            .get("222222222222")
            .unwrap()
            .distributions
            .contains_key("E3"));
        assert!(accounts
            .get(LEGACY_ACCOUNT)
            .unwrap()
            .distributions
            .contains_key("E1"));
    }

    #[test]
    fn a_v2_snapshot_stays_put_when_the_default_account_is_the_legacy_one() {
        let parsed = parse_cloudfront_snapshot(&v2_snapshot_bytes(|_| {}), LEGACY_ACCOUNT).unwrap();
        let accounts = parsed.accounts.unwrap();
        let dist = &accounts.get(LEGACY_ACCOUNT).unwrap().distributions["E1"];
        assert_eq!(dist.arn, "arn:aws:cloudfront::000000000000:distribution/E1");
    }

    #[test]
    fn a_v3_snapshot_keeps_its_per_account_buckets() {
        let mut accounts = CloudFrontAccounts::new();
        accounts
            .entry(LEGACY_ACCOUNT)
            .distributions
            .insert("E1".to_string(), legacy_distribution("E1", LEGACY_ACCOUNT));
        let bytes = serde_json::to_vec(&CloudFrontSnapshot {
            schema_version: CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
        })
        .unwrap();
        let parsed = parse_cloudfront_snapshot(&bytes, "123456789012").unwrap();
        let accounts = parsed.accounts.unwrap();
        assert!(accounts.get(LEGACY_ACCOUNT).is_some());
        assert!(accounts.get("123456789012").is_none());
    }

    #[test]
    fn unused_id_skips_ids_held_by_any_account() {
        let mut accounts = CloudFrontAccounts::new();
        accounts
            .entry("111111111111")
            .distributions
            .insert("E1".to_string(), legacy_distribution("E1", "111111111111"));
        let draws = std::cell::RefCell::new(vec!["E2", "E1"]);
        let id = accounts.unused_id(
            || draws.borrow_mut().pop().unwrap().to_string(),
            |a, id| a.distributions.contains_key(id),
        );
        assert_eq!(id, "E2", "E1 is taken in another account");
    }

    #[test]
    fn a_current_snapshot_round_trips_through_the_parser() {
        let mut accounts = CloudFrontAccounts::new();
        let mut config = DistributionConfig::default();
        config.default_cache_behavior.min_ttl = Some(7);
        accounts.entry("000000000000").distributions.insert(
            "E2".to_string(),
            StoredDistribution {
                id: "E2".to_string(),
                arn: "arn".to_string(),
                status: "Deployed".to_string(),
                last_modified_time: Utc::now(),
                domain_name: "e2.cloudfront.net".to_string(),
                in_progress_invalidation_batches: 0,
                etag: "T".to_string(),
                config,
            },
        );
        let bytes = serde_json::to_vec(&CloudFrontSnapshot {
            schema_version: CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(accounts),
        })
        .unwrap();
        let parsed = parse_cloudfront_snapshot(&bytes, "123456789012").unwrap();
        let accounts = parsed.accounts.unwrap();
        let dist = &accounts.get("000000000000").unwrap().distributions["E2"];
        assert_eq!(dist.config.default_cache_behavior.min_ttl, Some(7));
    }
}
