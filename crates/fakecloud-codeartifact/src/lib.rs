//! AWS CodeArtifact (`codeartifact`) implementation for FakeCloud.
//!
//! restJson1 control plane for CodeArtifact: domains, repositories (with
//! upstreams and external connections), package groups, packages and their
//! versions/assets/dependencies, resource permission policies, authorization
//! tokens, and resource tagging. Requests are routed by HTTP method + `@http`
//! URI path; list/query parameters are read from the raw query string so
//! repeated multi-value keys survive. Everything is real, persisted,
//! account-partitioned CRUD -- a `PublishPackageVersion` actually stores the
//! asset bytes so a later `GetPackageVersionAsset` returns them, and a
//! `GetAuthorizationToken` mints a synthetic bearer token with an expiration.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::{CodeArtifactService, CODEARTIFACT_ACTIONS};
pub use state::{
    CodeArtifactSnapshot, CodeArtifactState, SharedCodeArtifactState,
    CODEARTIFACT_SNAPSHOT_SCHEMA_VERSION,
};
