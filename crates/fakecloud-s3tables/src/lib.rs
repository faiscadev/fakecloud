//! Amazon S3 Tables (`s3tables`) implementation for FakeCloud.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::{S3TablesService, S3TABLES_ACTIONS};
pub use state::{
    S3TablesSnapshot, S3TablesState, SharedS3TablesState, S3TABLES_SNAPSHOT_SCHEMA_VERSION,
};
