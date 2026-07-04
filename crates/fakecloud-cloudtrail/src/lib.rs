//! AWS CloudTrail (`cloudtrail`) awsJson1.1 control plane for fakecloud.
//!
//! The full 60-operation CloudTrail Smithy model: trails (with logging
//! start/stop and per-trail status), event selectors and insight selectors,
//! CloudTrail Lake event data stores (+ ingestion toggle, restore, federation),
//! channels, imports, queries, dashboards, resource policies, organization
//! delegated admins, event configuration, and resource tagging.
//!
//! CloudTrail is modelled as a state CRUD control plane: there is no real
//! event-recording engine (a fake needn't record its own API activity, and no
//! conformance/tfacc assertion depends on recorded events). Every resource is
//! real, account-partitioned, persisted state. Every `Create` is reflected by
//! its `Get`/`Describe`/`List`, every `Update` persists, every `Delete`
//! deletes. `StartLogging`/`StopLogging` toggle a per-trail logging flag that
//! `GetTrailStatus` reports. `LookupEvents`, `ListPublicKeys`, and
//! `ListInsightsMetricData` return real, empty result sets. CloudTrail Lake
//! queries settle to `FINISHED` synchronously with empty result rows.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::CloudTrailService;
pub use state::{CloudTrailData, SharedCloudTrailState, CLOUDTRAIL_SNAPSHOT_SCHEMA_VERSION};
