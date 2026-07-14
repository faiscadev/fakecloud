//! AWS CodeBuild (`codebuild`) implementation for FakeCloud.
//!
//! awsJson1.1 control plane for CodeBuild: build projects, builds and build
//! batches, report groups and reports, fleets, webhooks, source credentials,
//! resource policies, and command-execution sandboxes.
//!
//! `StartBuild` mints a [`state`] Build `IN_PROGRESS` and returns immediately.
//! When a container backend is available (and not disabled via
//! `FAKECLOUD_CODEBUILD_DISABLE_BACKEND`), a background task ([`runtime`]) runs
//! the build for real: it resolves the environment image, parses the buildspec
//! phases, executes each phase's `commands` in a real Docker/Podman container,
//! streams output to CloudWatch Logs, uploads S3 artifacts, and settles
//! `buildStatus` on the REAL container exit codes. When the backend is disabled
//! (conformance/tfacc), the build falls back to the deterministic
//! settle-to-`SUCCEEDED`-on-read path so response shapes are unchanged.
//! Everything else is real, persisted, account-partitioned CRUD.

pub mod persistence;
pub mod runtime;
pub(crate) mod service;
pub(crate) mod state;
mod validate;

pub use service::{CodeBuildService, CODEBUILD_ACTIONS};
pub use state::{
    CodeBuildSnapshot, CodeBuildState, SharedCodeBuildState, CODEBUILD_SNAPSHOT_SCHEMA_VERSION,
};
