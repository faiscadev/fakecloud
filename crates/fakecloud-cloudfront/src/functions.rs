//! Data types for CloudFront Batch 3 resources: Functions, Public Keys,
//! Key Groups, Key Value Stores, Origin Access Identities (legacy),
//! Monitoring Subscriptions.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

fn skip_if_none<T>(x: &Option<T>) -> bool {
    x.is_none()
}

// ─── CloudFront Function ──────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct FunctionConfig {
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
    pub runtime: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub key_value_store_associations: Option<KeyValueStoreAssociations>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct KeyValueStoreAssociations {
    pub quantity: i32,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub items: Option<KeyValueStoreAssociationItems>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct KeyValueStoreAssociationItems {
    #[serde(default, rename = "KeyValueStoreAssociation")]
    pub key_value_store_association: Vec<KeyValueStoreAssociation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct KeyValueStoreAssociation {
    // AWS spells the member `KeyValueStoreARN` (upper-case ARN), which the
    // default PascalCase rule would render as `KeyValueStoreArn`. Pin the
    // exact wire name so real SDK requests parse and responses round-trip.
    #[serde(rename = "KeyValueStoreARN")]
    pub key_value_store_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredFunction {
    pub name: String,
    pub etag: String,
    pub status: String,
    /// "DEVELOPMENT" or "LIVE"
    pub stage: String,
    pub function_arn: String,
    pub created_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub config: FunctionConfig,
    /// Function source code (base64-encoded as the API receives it).
    /// This is the DEVELOPMENT-stage code: it tracks the latest
    /// CreateFunction / UpdateFunction body and is the version that
    /// `TestFunction(Stage=DEVELOPMENT)` runs against.
    pub function_code: String,
    /// Snapshot of `function_code` taken when the function is published.
    /// `TestFunction(Stage=LIVE)` (and the corresponding distribution
    /// data plane, once wired) reads from here so the published
    /// behaviour stays stable while DEVELOPMENT keeps mutating.
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub live_function_code: Option<String>,
}

// ─── Public Key ───────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct PublicKeyConfig {
    pub caller_reference: String,
    pub name: String,
    pub encoded_key: String,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredPublicKey {
    pub id: String,
    pub etag: String,
    pub created_time: DateTime<Utc>,
    pub config: PublicKeyConfig,
}

// ─── Key Group ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct KeyGroupConfig {
    pub name: String,
    pub items: KeyGroupItems,
    #[serde(default, skip_serializing_if = "skip_if_none")]
    pub comment: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct KeyGroupItems {
    #[serde(default, rename = "PublicKey")]
    pub public_key: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredKeyGroup {
    pub id: String,
    pub etag: String,
    pub last_modified_time: DateTime<Utc>,
    pub config: KeyGroupConfig,
}

// ─── Key Value Store ──────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct ImportSource {
    #[serde(default)]
    pub source_type: String,
    #[serde(default)]
    pub source_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredKeyValueStore {
    pub name: String,
    pub id: String,
    pub etag: String,
    pub arn: String,
    pub status: String,
    pub created_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub comment: Option<String>,
    pub import_source: Option<ImportSource>,
}

// ─── Origin Access Identity (legacy) ──────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct CloudFrontOriginAccessIdentityConfig {
    pub caller_reference: String,
    pub comment: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredOriginAccessIdentity {
    pub id: String,
    pub etag: String,
    pub s3_canonical_user_id: String,
    pub config: CloudFrontOriginAccessIdentityConfig,
}

// ─── Monitoring Subscription ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct MonitoringSubscriptionBody {
    pub realtime_metrics_subscription_config: RealtimeMetricsSubscriptionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub struct RealtimeMetricsSubscriptionConfig {
    pub realtime_metrics_subscription_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredMonitoringSubscription {
    pub distribution_id: String,
    pub config: RealtimeMetricsSubscriptionConfig,
}
