//! AWS Aurora DSQL (`dsql`) restJson1 control-plane service for fakecloud.
//!
//! Batch 1: the full 16-operation control plane from the AWS Smithy model --
//! cluster lifecycle (Create/Get/Update/Delete/ListClusters), resource-based
//! cluster policies (Put/Get/DeleteClusterPolicy), change streams
//! (Create/Get/Delete/ListStreams), the VPC endpoint service-name lookup, and
//! tagging (Tag/Untag/ListTagsForResource). Every operation is backed by real,
//! account-partitioned, persisted state with AWS-shaped identifiers (26-char
//! lowercase cluster ids, `arn:aws:dsql:...:cluster/<id>` ARNs), an async
//! CREATING -> ACTIVE / DELETING -> DELETED lifecycle, `clientToken`
//! idempotency, deletion-protection enforcement, and multi-region property
//! round-tripping.
//!
//! The Postgres-compatible data plane (a real container the cluster endpoint
//! resolves to, reachable with an IAM-derived connection token) lands in a
//! follow-up batch; this batch establishes the control plane and full
//! non-code surface. Unimplemented behavior returns a faithful modeled error,
//! never a stub success.

pub mod persistence;
pub mod service;
pub mod state;
pub mod ticker;

pub use service::DsqlService;
pub use state::{DsqlState, SharedDsqlState, DSQL_SNAPSHOT_SCHEMA_VERSION};
