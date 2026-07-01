//! Account-partitioned, serializable state for the Aurora DSQL control plane.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};

use fakecloud_aws::arn::Arn;
use fakecloud_core::multi_account::{AccountState, MultiAccountState};

/// Bumped whenever the on-disk shape of [`DsqlSnapshot`] changes.
pub const DSQL_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// A DSQL cluster and everything hanging off it (policy + change streams).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cluster {
    pub identifier: String,
    pub arn: String,
    /// `ClusterStatus`: CREATING | ACTIVE | IDLE | INACTIVE | UPDATING |
    /// DELETING | DELETED | FAILED | PENDING_SETUP | PENDING_DELETE.
    pub status: String,
    pub creation_time: DateTime<Utc>,
    pub deletion_protection_enabled: bool,
    pub multi_region_properties: Option<MultiRegionProperties>,
    pub encryption_details: EncryptionDetails,
    /// The customer-facing SQL endpoint host (`<id>.dsql.<region>.on.aws`).
    pub endpoint: String,
    pub tags: BTreeMap<String, String>,
    /// Resource-based policy document (JSON) and its monotonically increasing
    /// version, set via `PutClusterPolicy`.
    pub policy: Option<String>,
    pub policy_version: u64,
    /// The `clientToken` that created this cluster, for create idempotency.
    #[serde(default)]
    pub client_token: Option<String>,
    /// Change streams keyed by `streamIdentifier`.
    #[serde(default)]
    pub streams: BTreeMap<String, Stream>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiRegionProperties {
    pub witness_region: Option<String>,
    pub clusters: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionDetails {
    /// `EncryptionType`: AWS_OWNED_KMS_KEY | CUSTOMER_MANAGED_KMS_KEY.
    pub encryption_type: String,
    pub kms_key_arn: Option<String>,
    /// `EncryptionStatus`: ENABLED | UPDATING | KMS_KEY_INACCESSIBLE | ENABLING.
    pub encryption_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Stream {
    pub cluster_identifier: String,
    pub stream_identifier: String,
    pub arn: String,
    /// `StreamStatus`: CREATING | ACTIVE | DELETING | DELETED | FAILED | IMPAIRED.
    pub status: String,
    pub creation_time: DateTime<Utc>,
    /// `StreamOrdering`: only UNORDERED is modeled by AWS today.
    pub ordering: String,
    /// `StreamFormat`: only JSON is modeled by AWS today.
    pub format: String,
    /// `TargetDefinition` union, round-tripped verbatim (currently `{ kinesis }`).
    pub target_definition: serde_json::Value,
    pub status_reason: Option<String>,
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub client_token: Option<String>,
}

/// One account's DSQL clusters, keyed by cluster identifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DsqlState {
    pub account_id: String,
    pub region: String,
    pub clusters: BTreeMap<String, Cluster>,
}

impl DsqlState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            clusters: BTreeMap::new(),
        }
    }
}

impl AccountState for DsqlState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

pub type SharedDsqlState = Arc<RwLock<MultiAccountState<DsqlState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct DsqlSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<DsqlState>,
}

/// Generate a 26-character lowercase-alphanumeric identifier, matching the AWS
/// `^[a-z0-9]{26}$` pattern used for both cluster and stream ids.
pub fn gen_id() -> String {
    const CHARS: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..26)
        .map(|_| CHARS[rng.gen_range(0..CHARS.len())] as char)
        .collect()
}

/// Build a cluster ARN: `arn:aws:dsql:<region>:<account>:cluster/<id>`.
pub fn cluster_arn(region: &str, account_id: &str, id: &str) -> String {
    Arn::new("dsql", region, account_id, &format!("cluster/{id}")).to_string()
}

/// Build a stream ARN:
/// `arn:aws:dsql:<region>:<account>:cluster/<cluster>/stream/<stream>`.
pub fn stream_arn(region: &str, account_id: &str, cluster: &str, stream: &str) -> String {
    Arn::new(
        "dsql",
        region,
        account_id,
        &format!("cluster/{cluster}/stream/{stream}"),
    )
    .to_string()
}

/// The customer-facing SQL endpoint host for a cluster.
pub fn cluster_endpoint(id: &str, region: &str) -> String {
    format!("{id}.dsql.{region}.on.aws")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gen_id_matches_aws_pattern() {
        let id = gen_id();
        assert_eq!(id.len(), 26);
        assert!(id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()));
    }

    #[test]
    fn arns_have_expected_shape() {
        let id = "abcdefghij0123456789klmnop";
        let c = cluster_arn("us-east-1", "123456789012", id);
        assert_eq!(
            c,
            format!("arn:aws:dsql:us-east-1:123456789012:cluster/{id}")
        );
        let s = stream_arn("us-east-1", "123456789012", id, id);
        assert_eq!(
            s,
            format!("arn:aws:dsql:us-east-1:123456789012:cluster/{id}/stream/{id}")
        );
    }

    #[test]
    fn endpoint_shape() {
        assert_eq!(
            cluster_endpoint("abcdefghij0123456789klmnop", "us-west-2"),
            "abcdefghij0123456789klmnop.dsql.us-west-2.on.aws"
        );
    }
}
