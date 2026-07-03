//! AWS Backup (`backup`) implementation for FakeCloud.

pub mod persistence;
pub mod service;
pub mod state;

pub use service::{BackupService, BACKUP_ACTIONS};
pub use state::{BackupSnapshot, BackupState, SharedBackupState, BACKUP_SNAPSHOT_SCHEMA_VERSION};
