//! AWS RDS Data API (`rds-data`) restJson1 service for fakecloud.
//!
//! Batch 1: vendored Smithy model + crate scaffold + restJson1 routing for the
//! Data API operations + a real `ExecuteStatement` that resolves the
//! `resourceArn` to a fakecloud-managed RDS container DB and runs the SQL for
//! real over `tokio-postgres` / `mysql_async`, returning AWS-shaped typed
//! `Field` records + `columnMetadata`. This is the wedge: rival emulators serve
//! canned rows or return `{}` for binary columns; fakecloud runs genuine SQL on
//! a real engine. Transactions + `BatchExecuteStatement` land in later batches;
//! unimplemented operations return a faithful `BadRequestException`/not-yet
//! error rather than a fake success.

pub mod service;

pub use service::RdsDataService;
