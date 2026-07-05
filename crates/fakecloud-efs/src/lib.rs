//! Amazon Elastic File System (`elasticfilesystem`) restJson1 control plane
//! for fakecloud.
//!
//! The full 31-operation EFS Smithy model: file systems (with the async
//! `creating` -> `available` lifecycle, size, performance/throughput modes,
//! encryption, and replication-overwrite protection), mount targets (one per
//! Availability Zone per file system, each with a synthesized IP address,
//! network interface, and VPC/AZ derived deterministically from its subnet),
//! access points (POSIX user + root directory), lifecycle configuration,
//! backup policy, file-system resource policy, replication configurations,
//! resource tagging (both the resource-id tagging API and the deprecated
//! per-file-system `Create`/`Delete`/`Describe` tags API), and account
//! preferences.
//!
//! Requests are routed to an operation by HTTP method + `@http` URI path;
//! path labels are captured positionally and query parameters are read from the
//! raw query string so repeated multi-value keys (`tagKeys=a&tagKeys=b`)
//! survive intact. Everything is real, persisted, account-partitioned state:
//! every `Create`/`Put`/`Update` is reflected by its `Describe`, every `Delete`
//! deletes, and AWS's async lifecycle is modelled by returning the transient
//! `creating` state and settling to `available` on the next describe (with
//! in-flight transitions reconciled on restart).

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::{EfsService, EFS_ACTIONS};
pub use state::{EfsData, EfsSnapshot, SharedEfsState, EFS_SNAPSHOT_SCHEMA_VERSION};
