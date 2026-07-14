//! AWS Database Migration Service (`dms`) awsJson1.1 control plane for
//! fakecloud.
//!
//! The full 119-operation DMS control plane from the AWS Smithy model:
//! replication instances, endpoints (with per-engine settings), replication
//! tasks (+ assessment runs, table statistics), replication subnet groups,
//! event subscriptions, certificates, connections (`TestConnection`),
//! serverless replication configs and replications, data providers, instance
//! profiles, migration projects, schema-conversion / metadata-model requests,
//! Fleet Advisor, recommendations, account attributes and resource tagging.
//!
//! DMS is modelled as a state CRUD control plane: there is no real data
//! migration engine (LocalStack Community treats DMS the same way — the actual
//! row movement is out of scope). Every resource is real, account-partitioned,
//! persisted state. Every `Create` is reflected by its `Describe`, every
//! `Modify` persists, every `Delete` deletes, `TestConnection` returns a
//! successful connection, and newly-created replication instances / started
//! tasks settle into their steady state (`available` / `running` / `stopped`)
//! synchronously so waiters complete. Per-engine endpoint settings and other
//! nested configuration objects are stored as the raw request `Value` so they
//! round-trip verbatim.

pub mod persistence;
pub(crate) mod service;
pub(crate) mod state;

pub use service::DmsService;
pub use state::{DmsData, SharedDmsState, DMS_SNAPSHOT_SCHEMA_VERSION};
