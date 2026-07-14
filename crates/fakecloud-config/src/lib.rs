//! AWS Config (`config`) implementation for FakeCloud.
//!
//! Real data plane: a running configuration recorder snapshots the live state
//! of other FakeCloud services (S3, EC2, IAM) into genuine `ConfigurationItem`
//! history, config rules run real evaluation logic (AWS managed rules against
//! the recorded items, custom rules by invoking the referenced Lambda), and the
//! `SelectResourceConfig` query language runs over the recorded items.

pub(crate) mod model_validate;
pub(crate) mod persistence;
pub mod provision;
pub(crate) mod service;
pub(crate) mod state;
pub mod validate;

#[cfg(test)]
mod e2e_tests;

pub use persistence::save_config_snapshot;
pub use service::ConfigService;
pub use state::{
    ConfigAccounts, ConfigSnapshot, ConfigurationItem, SharedConfigState,
    CONFIG_SNAPSHOT_SCHEMA_VERSION,
};
pub use validate::CrossServiceStates;
