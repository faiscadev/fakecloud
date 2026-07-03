//! Amazon Redshift control-plane implementation for FakeCloud.
//!
//! Redshift's control plane is pure state CRUD: clusters, snapshots,
//! parameter/subnet/security groups, HSM objects, event subscriptions,
//! scheduled actions, usage limits, endpoint access, datashares, and so on.
//! Unlike RDS/ElastiCache there is no data plane to run (SQL access is a
//! separate service, `redshift-data`), so nothing here spawns containers —
//! every resource is modelled as real persisted state with real CRUD,
//! validation, pagination, and AWS-faithful error codes.

pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod validation;

pub use service::RedshiftService;
pub use state::{
    RedshiftAccounts, RedshiftSnapshot, RedshiftState, SharedRedshiftState,
    REDSHIFT_SNAPSHOT_SCHEMA_VERSION,
};
