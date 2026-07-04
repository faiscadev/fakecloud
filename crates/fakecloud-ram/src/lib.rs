//! AWS Resource Access Manager (`ram`) implementation for FakeCloud.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::{RamService, RAM_ACTIONS};
pub use state::{RamSnapshot, RamState, SharedRamState, RAM_SNAPSHOT_SCHEMA_VERSION};
