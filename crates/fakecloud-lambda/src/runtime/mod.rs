//! Lambda execution runtime — pluggable backend behind a shared facade.
//!
//! [`LambdaRuntime`] owns the warm-pool bookkeeping and the HTTP
//! invocation path. The actual lifecycle (container/pod create, port
//! discovery, teardown) is delegated to whatever [`LambdaBackend`] the
//! facade was constructed with — today that's [`DockerBackend`]; a
//! Kubernetes backend ships in a follow-up.

pub(crate) mod backend;
pub(crate) mod docker;
pub(crate) mod env_rewrite;
pub(crate) mod facade;

pub use backend::{BackendHandle, LambdaBackend, RuntimeError, StreamingInvocation, WarmInstance};
pub use docker::{extract_zip, runtime_to_image, DockerBackend};
pub use facade::LambdaRuntime;

/// Backwards-compatible alias used by callers across the workspace
/// (eventbridge bridge, scheduler, lambda_delivery, reset state, server
/// main). Keeping the old name lets the trait extraction land without
/// churn in every consumer.
pub type ContainerRuntime = LambdaRuntime;
