//! Account-partitioned, serializable state for Amazon Pinpoint (`pinpoint`).
//!
//! Every resource is stored under its owning application (or, for the global
//! families, at the top level) as a plain JSON `Value` record keyed by a
//! `String` id — so the snapshot never depends on the tuple-key serde adapter.
//! The handlers project the exact model output shape out of these records on
//! read.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const PINPOINT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// A versioned resource (campaign / segment): the current record plus every
/// historical version. The 1-based version number indexes `versions`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Versioned {
    /// The current (latest) version's record.
    pub current: Value,
    /// All version records, oldest first (index + 1 == `Version`).
    pub versions: Vec<Value>,
}

/// A versioned message template (email / push / sms / voice / inapp).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Template {
    /// `EMAIL` / `SMS` / `VOICE` / `PUSH` / `INAPP`.
    pub template_type: String,
    /// All version records, oldest first (index + 1 == version number).
    pub versions: Vec<Value>,
    /// The active version number, as a string (Pinpoint versions are strings).
    pub active_version: String,
}

/// Per-application Pinpoint state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct App {
    /// The `ApplicationResponse` core record (`Id`, `Arn`, `Name`, `tags`,
    /// `CreationDate`).
    pub record: Value,
    /// The `ApplicationSettingsResource` projection.
    #[serde(default)]
    pub settings: Value,
    /// Campaigns keyed by campaign id.
    #[serde(default)]
    pub campaigns: BTreeMap<String, Versioned>,
    /// Segments keyed by segment id.
    #[serde(default)]
    pub segments: BTreeMap<String, Versioned>,
    /// Journeys keyed by journey id.
    #[serde(default)]
    pub journeys: BTreeMap<String, Value>,
    /// Endpoints keyed by endpoint id.
    #[serde(default)]
    pub endpoints: BTreeMap<String, Value>,
    /// Channels keyed by canonical channel key (`adm`, `apns`, `sms`, ...).
    #[serde(default)]
    pub channels: BTreeMap<String, Value>,
    /// Import jobs keyed by job id.
    #[serde(default)]
    pub import_jobs: BTreeMap<String, Value>,
    /// Export jobs keyed by job id.
    #[serde(default)]
    pub export_jobs: BTreeMap<String, Value>,
    /// The single event stream, if configured.
    #[serde(default)]
    pub event_stream: Option<Value>,
}

/// Per-account Pinpoint state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PinpointData {
    /// Applications keyed by application id.
    #[serde(default)]
    pub apps: BTreeMap<String, App>,
    /// Message templates keyed by template name (global, not app-scoped).
    #[serde(default)]
    pub templates: BTreeMap<String, Template>,
    /// Recommender configurations keyed by recommender id (global).
    #[serde(default)]
    pub recommenders: BTreeMap<String, Value>,
    /// Tag sets keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for PinpointData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedPinpointState = Arc<RwLock<MultiAccountState<PinpointData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct PinpointSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<PinpointData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_account_is_empty() {
        let data = PinpointData::new_for_account("000000000000", "us-east-1", "");
        assert!(data.apps.is_empty());
        assert!(data.templates.is_empty());
        assert!(data.recommenders.is_empty());
        assert!(data.tags.is_empty());
    }
}
