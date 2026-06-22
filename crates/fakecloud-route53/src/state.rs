//! In-memory state for Route 53 resources.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::model::{HealthCheckConfig, HostedZoneFeatures, ResourceRecordSet, VPC};

pub type SharedRoute53State = Arc<RwLock<Route53Accounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Route53Accounts {
    pub accounts: BTreeMap<String, AccountState>,
}

/// On-disk snapshot envelope for Route 53 state. Versioned so format changes
/// fail loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct Route53Snapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<Route53Accounts>,
}

pub const ROUTE53_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// (De)serialize a `(A, B) -> V` map as a sequence of `(A, B, V)` triples. JSON
/// object keys must be strings, so tuple-keyed maps (traffic policies keyed by
/// `(id, version)`, KSKs / tags keyed by `(a, b)`) cannot be serialized
/// directly — without this, whole-state snapshot writes fail.
mod tuple2_map_serde {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S, A, B, V>(
        map: &BTreeMap<(A, B), V>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        A: Serialize,
        B: Serialize,
        V: Serialize,
    {
        let entries: Vec<(&A, &B, &V)> = map.iter().map(|((a, b), v)| (a, b, v)).collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D, A, B, V>(deserializer: D) -> Result<BTreeMap<(A, B), V>, D::Error>
    where
        D: Deserializer<'de>,
        A: Deserialize<'de> + Ord,
        B: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
    {
        let entries: Vec<(A, B, V)> = Vec::deserialize(deserializer)?;
        Ok(entries.into_iter().map(|(a, b, v)| ((a, b), v)).collect())
    }
}

impl Route53Accounts {
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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    pub hosted_zones: BTreeMap<String, StoredHostedZone>,
    pub changes: BTreeMap<String, StoredChange>,
    pub health_checks: BTreeMap<String, StoredHealthCheck>,
    /// Keyed by `(traffic_policy_id, version)`. Each `CreateTrafficPolicyVersion`
    /// inserts a new entry alongside the existing versions.
    #[serde(with = "tuple2_map_serde")]
    pub traffic_policies: BTreeMap<(String, i64), StoredTrafficPolicy>,
    pub traffic_policy_instances: BTreeMap<String, StoredTrafficPolicyInstance>,
    /// Per-zone DNSSEC `ServeSignature` status (SIGNING / NOT_SIGNING). Absent
    /// entries are treated as NOT_SIGNING.
    pub dnssec_status: BTreeMap<String, String>,
    /// Keyed by `(hosted_zone_id, ksk_name)`.
    #[serde(with = "tuple2_map_serde")]
    pub key_signing_keys: BTreeMap<(String, String), StoredKeySigningKey>,
    pub query_logging_configs: BTreeMap<String, StoredQueryLoggingConfig>,
    pub cidr_collections: BTreeMap<String, StoredCidrCollection>,
    pub reusable_delegation_sets: BTreeMap<String, StoredReusableDelegationSet>,
    /// Per-zone authorized cross-account VPCs that may be associated next.
    pub vpc_authorizations: BTreeMap<String, Vec<VPC>>,
    /// Tag bag keyed by `(resource_type, resource_id)`. Both supported
    /// resource types ("healthcheck", "hostedzone") share the bag; the
    /// resource-type discriminator is in the key tuple.
    #[serde(with = "tuple2_map_serde")]
    pub tags: BTreeMap<(String, String), BTreeMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredHostedZone {
    pub id: String,
    pub name: String,
    pub caller_reference: String,
    pub comment: Option<String>,
    pub private_zone: bool,
    pub features: Option<HostedZoneFeatures>,
    pub vpcs: Vec<VPC>,
    pub delegation_set_id: Option<String>,
    pub name_servers: Vec<String>,
    pub created_time: DateTime<Utc>,
    pub resource_record_sets: Vec<ResourceRecordSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredChange {
    pub id: String,
    pub status: String,
    pub submitted_at: DateTime<Utc>,
    pub comment: Option<String>,
    /// Number of times GetChange has read this row. New changes start
    /// at PENDING; once a few reads have happened we flip to INSYNC to
    /// mirror real Route53's propagation delay without making tests
    /// wait wall-clock seconds.
    #[serde(default)]
    pub read_count: u32,
}

impl StoredChange {
    pub fn pending(id: String, submitted_at: DateTime<Utc>, comment: Option<String>) -> Self {
        Self {
            id,
            status: "PENDING".to_string(),
            submitted_at,
            comment,
            read_count: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum HealthCheckStatus {
    #[default]
    Success,
    Failure,
    /// The endpoint did not respond within the request timeout. Surfaced
    /// in `<Status>` as `"Failure: Connection timed out"` unless an
    /// explicit `last_failure_reason` is supplied.
    Timeout,
    /// Route 53 could not resolve the FQDN. Surfaced as
    /// `"Failure: DNS resolution failed"`.
    DnsError,
    /// Not enough recent observations to compute a definitive verdict.
    /// Surfaced as `"InsufficientDataPoints"` (no `Success`/`Failure`
    /// prefix, mirroring real AWS status strings).
    InsufficientDataPoints,
    /// Status could not be determined for any other reason. Surfaced as
    /// `"Unknown"`.
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredHealthCheck {
    pub id: String,
    pub caller_reference: String,
    pub version: i64,
    pub config: HealthCheckConfig,
    pub created_time: DateTime<Utc>,
    /// Status reported by `GetHealthCheckStatus`. Defaults to `Success`;
    /// flipped via the admin endpoint at
    /// `POST /_fakecloud/route53/health-checks/{id}/status` so callers
    /// can simulate failover scenarios in tests.
    #[serde(default)]
    pub status: HealthCheckStatus,
    /// Last failure reason returned by `GetHealthCheckLastFailureReason`
    /// and appended to the `Status` element when `status = Failure`.
    /// `None` when the check has never reported a failure.
    #[serde(default)]
    pub last_failure_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTrafficPolicy {
    pub id: String,
    pub version: i64,
    pub name: String,
    pub policy_type: String,
    pub document: String,
    pub comment: Option<String>,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTrafficPolicyInstance {
    pub id: String,
    pub hosted_zone_id: String,
    pub name: String,
    pub ttl: i64,
    pub state: String,
    pub message: String,
    pub traffic_policy_id: String,
    pub traffic_policy_version: i64,
    pub traffic_policy_type: String,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredKeySigningKey {
    pub hosted_zone_id: String,
    pub name: String,
    pub kms_arn: String,
    pub status: String,
    pub caller_reference: String,
    pub created_date: DateTime<Utc>,
    pub last_modified_date: DateTime<Utc>,
    pub key_tag: i32,
    /// PKCS#8 PEM-encoded ECDSA P-256 private key (algorithm 13 / RFC
    /// 6605). Generated deterministically from `(hosted_zone_id, name)`
    /// so persistence snapshots restore the same DNSKEY/RRSIGs across
    /// restarts. Not exposed to the AWS-facing XML — only used by the
    /// `/_fakecloud/route53/zones/{id}/dnssec/*` admin endpoints and the
    /// signed-RRset machinery surfaced through `TestDNSAnswer`.
    #[serde(default)]
    pub private_key_pem: String,
    /// SubjectPublicKeyInfo DER bytes for the matching public key.
    /// Stored alongside the private key so consumers can fetch the
    /// public half without re-deriving it on every read.
    #[serde(default)]
    pub public_key_der: Vec<u8>,
    /// DS record digest (SHA-256, hex) over the canonical DNSKEY RDATA
    /// for the parent zone to publish. Equivalent to the digest a
    /// real Route 53 returns alongside the KSK.
    #[serde(default)]
    pub ds_digest_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredQueryLoggingConfig {
    pub id: String,
    pub hosted_zone_id: String,
    pub cloud_watch_logs_log_group_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredCidrCollection {
    pub id: String,
    pub name: String,
    pub arn: String,
    pub version: i64,
    pub caller_reference: String,
    /// Maps location name -> sorted list of CIDR blocks.
    pub locations: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredReusableDelegationSet {
    pub id: String,
    pub caller_reference: String,
    pub name_servers: Vec<String>,
}
