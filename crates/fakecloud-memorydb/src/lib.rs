//! AWS MemoryDB (`memorydb`) implementation for FakeCloud.

pub mod persistence;
pub mod service;
pub mod state;

pub use service::{MemoryDbService, MEMORYDB_ACTIONS};
pub use state::{
    MemoryDbSnapshot, MemoryDbState, SharedMemoryDbState, MEMORYDB_SNAPSHOT_SCHEMA_VERSION,
};
