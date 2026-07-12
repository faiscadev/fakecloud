//! Account-partitioned, serializable state for AWS AppSync (`appsync`).
//!
//! Every resource is stored as its already-output-valid wire JSON object so
//! reads echo exactly what writes persisted. All map keys are plain `String`s
//! (apiId, resource name, ARN, association id), so the snapshot never depends on
//! the tuple-key serde adapter that has silently broken snapshot serialization
//! on other services. Sub-resources keyed by a compound `String` (e.g. a
//! resolver's `typeName::fieldName`) reuse the `::` delimiter, which the
//! `ResourceName` grammar (`[_A-Za-z][_0-9A-Za-z]*`) can never contain.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};
use serde_json::Value;

pub const APPSYNC_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// A stored GraphQL-schema document + its async creation status.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaState {
    /// The raw SDL schema document ingested by `StartSchemaCreation`.
    #[serde(default)]
    pub definition: String,
    /// Current `SchemaStatus` (`Processing` until the first status read
    /// settles it to `Success`), matching the async lifecycle of other ops.
    #[serde(default)]
    pub status: String,
    /// Human-readable status details.
    #[serde(default)]
    pub details: String,
}

/// Per-account AWS AppSync state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppSyncData {
    /// GraphQL APIs keyed by `apiId`, stored as their `GraphqlApi` wire object.
    #[serde(default)]
    pub graphql_apis: BTreeMap<String, Value>,
    /// Schema state keyed by `apiId`.
    #[serde(default)]
    pub schemas: BTreeMap<String, SchemaState>,
    /// API keys keyed by `apiId` -> `id` -> `ApiKey` wire object.
    #[serde(default)]
    pub api_keys: BTreeMap<String, BTreeMap<String, Value>>,
    /// Data sources keyed by `apiId` -> `name` -> `DataSource` wire object.
    #[serde(default)]
    pub data_sources: BTreeMap<String, BTreeMap<String, Value>>,
    /// Resolvers keyed by `apiId` -> `typeName::fieldName` -> `Resolver`.
    #[serde(default)]
    pub resolvers: BTreeMap<String, BTreeMap<String, Value>>,
    /// Functions keyed by `apiId` -> `functionId` -> `FunctionConfiguration`.
    #[serde(default)]
    pub functions: BTreeMap<String, BTreeMap<String, Value>>,
    /// Schema types keyed by `apiId` -> `typeName` -> `Type` wire object.
    #[serde(default)]
    pub types: BTreeMap<String, BTreeMap<String, Value>>,
    /// API caches keyed by `apiId`, stored as their `ApiCache` wire object.
    #[serde(default)]
    pub api_caches: BTreeMap<String, Value>,
    /// GraphQL-API environment variables keyed by `apiId`.
    #[serde(default)]
    pub env_vars: BTreeMap<String, BTreeMap<String, String>>,
    /// Custom domain names keyed by `domainName` -> `DomainNameConfig`.
    #[serde(default)]
    pub domain_names: BTreeMap<String, Value>,
    /// Domain-name -> `ApiAssociation` links (`AssociateApi`).
    #[serde(default)]
    pub api_associations: BTreeMap<String, Value>,
    /// Event APIs keyed by `apiId`, stored as their `Api` wire object.
    #[serde(default)]
    pub apis: BTreeMap<String, Value>,
    /// Channel namespaces keyed by event-`apiId` -> `name` -> `ChannelNamespace`.
    #[serde(default)]
    pub channel_namespaces: BTreeMap<String, BTreeMap<String, Value>>,
    /// Source-API associations keyed by `associationId` -> `SourceApiAssociation`.
    #[serde(default)]
    pub source_api_associations: BTreeMap<String, Value>,
    /// Data-source introspection jobs keyed by `introspectionId`.
    #[serde(default)]
    pub introspections: BTreeMap<String, Value>,
    /// Tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

/// Current time as an RFC3339 string (AppSync's timestamp members serialise as
/// epoch-seconds on the restJson1 wire, but stored objects only need to be
/// self-consistent; epoch floats are used where the model declares a timestamp).
pub fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

impl AccountState for AppSyncData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedAppSyncState = Arc<RwLock<MultiAccountState<AppSyncData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct AppSyncSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<AppSyncData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_account_is_empty() {
        let data = AppSyncData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.graphql_apis.is_empty());
        assert!(data.tags.is_empty());
    }
}
