//! AWS Verified Permissions (`verifiedpermissions`) awsJson1.0 service for
//! fakecloud.
//!
//! The full 34-operation control plane from the AWS Smithy model: policy
//! stores, their Cedar schemas, static and template-linked policies, policy
//! templates, identity sources, policy-store aliases, tagging, and the four
//! real Cedar authorization operations (`IsAuthorized`,
//! `IsAuthorizedWithToken`, `BatchIsAuthorized`, `BatchIsAuthorizedWithToken`).
//!
//! Authorization decisions are computed by the official `cedar-policy` crate:
//! the store's static + template-linked policies are compiled into a Cedar
//! `PolicySet`, the request principal/action/resource/context/entities are
//! translated from the Verified Permissions wire shapes into Cedar values, and
//! the resulting decision (`ALLOW`/`DENY`), determining policies and evaluation
//! errors are returned. `*WithToken` operations decode the supplied identity /
//! access JWT to resolve the principal entity per the identity source config.
//!
//! Every operation is backed by real, account-partitioned, persisted state.

pub mod cedar;
pub mod persistence;
pub mod service;
pub mod state;

pub use service::VerifiedPermissionsService;
pub use state::{
    SharedVerifiedPermissionsState, VerifiedPermissionsData,
    VERIFIEDPERMISSIONS_SNAPSHOT_SCHEMA_VERSION,
};
