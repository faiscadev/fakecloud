//! Shared Kubernetes-backend primitives for FakeCloud.
//!
//! FakeCloud can run its container-backed services (Lambda, ElastiCache,
//! RDS, ECS) either by shelling out to a local Docker/Podman daemon (the
//! default) or by spawning native Pods in a Kubernetes cluster. The k8s
//! path is the same for every service — connect a client, build a Pod,
//! wait for it to come up, optionally `exec` into it, tear it down, and
//! reap orphans left by a previous process. This crate holds that common
//! machinery so each service only has to describe *its* Pod, not
//! re-implement the client bootstrap, readiness polling, exec plumbing,
//! reaping, and DNS-safe naming.
//!
//! Backend selection (`FAKECLOUD_CONTAINER_BACKEND` /
//! `FAKECLOUD_<SERVICE>_BACKEND`) lives in [`backend`]. The kube client
//! wrapper and Pod lifecycle live in [`client`]. Env parsing
//! (`FAKECLOUD_K8S_*`) lives in [`env`]. Naming + label conventions live
//! in [`names`] and [`labels`].
//!
//! See `website/content/docs/guides/kubernetes-backend.md` for the
//! operator-facing setup (ServiceAccount, RBAC, Deployment yaml).

pub mod backend;
pub mod client;
pub mod env;
pub mod labels;
pub mod names;

pub use backend::{backend_choice, Backend};
pub use client::{ExecOutput, K8sClient, K8sError};
pub use env::{K8sEnv, K8sEnvError};
