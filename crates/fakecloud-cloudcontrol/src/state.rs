//! Account-partitioned, serializable state for Cloud Control API.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CLOUDCONTROL_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// A resource created through Cloud Control, keyed by `(type_name, identifier)`.
/// `properties` is the canonical JSON of the resource's current desired state
/// (what `GetResource` returns), `attributes` are the provisioner's
/// `GetAtt`-resolvable outputs used to satisfy read-only identifiers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedResource {
    pub type_name: String,
    pub identifier: String,
    pub properties: serde_json::Value,
    pub attributes: BTreeMap<String, String>,
    pub created_at: DateTime<Utc>,
}

/// A resource-operation request, keyed by its `RequestToken`. Cloud Control
/// provisioning is synchronous here, so `operation_status` is terminal
/// (`SUCCESS`/`FAILED`) by the time the request returns.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceRequest {
    pub request_token: String,
    pub type_name: String,
    pub identifier: Option<String>,
    /// `Operation`: CREATE | UPDATE | DELETE.
    pub operation: String,
    /// `OperationStatus`: PENDING | IN_PROGRESS | SUCCESS | FAILED |
    /// CANCEL_IN_PROGRESS | CANCEL_COMPLETE.
    pub operation_status: String,
    pub event_time: DateTime<Utc>,
    pub resource_model: Option<serde_json::Value>,
    pub status_message: Option<String>,
    pub error_code: Option<String>,
    /// The `ClientToken` that created this request, for idempotency.
    pub client_token: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CloudControlState {
    /// Created resources keyed by `"<type_name>\u{1f}<identifier>"`.
    pub resources: BTreeMap<String, ManagedResource>,
    /// Request ledger keyed by `RequestToken`.
    pub requests: BTreeMap<String, ResourceRequest>,
}

impl CloudControlState {
    pub fn resource_key(type_name: &str, identifier: &str) -> String {
        format!("{type_name}\u{1f}{identifier}")
    }
}

impl AccountState for CloudControlState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCloudControlState = Arc<RwLock<MultiAccountState<CloudControlState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CloudControlSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CloudControlState>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_key_is_unit_separated() {
        let k = CloudControlState::resource_key("AWS::S3::Bucket", "my-bucket");
        assert!(k.contains('\u{1f}'));
        assert!(k.starts_with("AWS::S3::Bucket"));
        assert!(k.ends_with("my-bucket"));
    }
}
