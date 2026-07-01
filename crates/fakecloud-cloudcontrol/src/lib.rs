//! AWS Cloud Control API (`cloudcontrolapi`) awsJson1.0 service for fakecloud.
//!
//! Cloud Control is a uniform CRUD+L facade over every CloudFormation resource
//! type. Rather than reimplement per-type handlers, this service drives the
//! EXISTING CloudFormation `ResourceProvisioner` one resource at a time via the
//! `CloudControlOutcome` bridge on `CloudFormationService`: a `CreateResource`
//! with a `DesiredState` of `AWS::S3::Bucket` properties provisions a real
//! bucket through the same handler `CreateStack` would use (real container for
//! container-backed types included). This is what unblocks the IaC tools that
//! speak Cloud Control instead of CloudFormation templates -- Terraform's
//! `awscc` provider and Pulumi's `aws-native`.
//!
//! All eight operations are implemented against real, account-partitioned,
//! persisted state: the created-resource registry (so Get/List/Update/Delete
//! resolve) and the resource-request ledger (so `GetResourceRequestStatus` /
//! `ListResourceRequests` return real `ProgressEvent`s). Provisioning is
//! synchronous here, so requests reach a terminal `SUCCESS`/`FAILED` status
//! immediately, and the returned `ProgressEvent` reflects that.

pub mod patch;
pub mod persistence;
pub mod service;
pub mod state;

pub use service::CloudControlService;
pub use state::{CloudControlState, SharedCloudControlState, CLOUDCONTROL_SNAPSHOT_SCHEMA_VERSION};
