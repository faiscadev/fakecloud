//! Lambda execution runtime — pluggable backend behind a shared facade.
//!
//! [`LambdaRuntime`] owns the warm-pool bookkeeping and the invocation
//! path, which runs through the Lambda Runtime API fakecloud serves to every
//! instance ([`api`]). The actual lifecycle (container/pod create, exit
//! watching, teardown) is delegated to whatever [`LambdaBackend`] the facade
//! was constructed with: [`DockerBackend`] (Docker or Podman) or
//! [`K8sBackend`].

pub mod api;
pub(crate) mod backend;
pub(crate) mod container_exits;
pub(crate) mod docker;
pub(crate) mod env_rewrite;
pub mod environment;
pub(crate) mod extensions;
pub(crate) mod facade;
pub mod k8s;

pub use api::RuntimeApi;
pub use backend::{
    BackendHandle, ExitWatch, InstanceExit, LambdaBackend, RuntimeApiAddress, RuntimeError,
    WarmInstance,
};
pub use docker::{extract_zip, runtime_to_image, DockerBackend};
pub use facade::{InvocationContext, LambdaRuntime, RunOutcome, TakenInstances};
pub use k8s::{K8sBackend, K8sBackendError};

/// Backwards-compatible alias used by callers across the workspace
/// (eventbridge bridge, scheduler, lambda_delivery, reset state, server
/// main). Keeping the old name lets the trait extraction land without
/// churn in every consumer.
pub type ContainerRuntime = LambdaRuntime;
