//! Amazon Managed Service for Apache Flink (`kinesisanalyticsv2`, formerly
//! Kinesis Data Analytics v2) implementation for FakeCloud.

pub mod persistence;
pub mod service;
pub mod state;

pub use service::{Ka2Service, KA2_ACTIONS};
pub use state::{Ka2Snapshot, Ka2State, SharedKa2State, KA2_SNAPSHOT_SCHEMA_VERSION};
