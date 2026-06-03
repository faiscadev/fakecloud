//! Kubernetes [`LambdaBackend`] implementation.
//!
//! Spawns Lambda function runtimes as native Pods in a Kubernetes
//! cluster instead of as Docker containers. Gated by
//! `FAKECLOUD_LAMBDA_BACKEND=k8s` (or the global
//! `FAKECLOUD_CONTAINER_BACKEND=k8s`) on the fakecloud server.
//!
//! The shared client bootstrap, Pod lifecycle (create/wait/delete),
//! reaping, and naming live in the `fakecloud-k8s` crate; this module
//! only builds the Lambda-specific Pod spec and wires the lifecycle into
//! the [`LambdaBackend`] trait.
//!
//! See `website/content/docs/guides/kubernetes-backend.md` for the
//! operator-facing setup (ServiceAccount, RBAC, Deployment yaml).

pub mod spec;

use std::time::Duration;

use async_trait::async_trait;
use fakecloud_k8s::{K8sClient, K8sEnv, K8sEnvError};

use super::backend::{BackendHandle, LambdaBackend, RuntimeError, WarmInstance};
use crate::state::LambdaFunction;
use spec::{build_pod_spec, pod_name_for, PodSpecContext};

/// Which `fakecloud-service` label Lambda Pods carry, so reaping only
/// touches Lambda Pods.
const SERVICE: &str = "lambda";

/// Errors that can prevent the K8s backend from initializing. Surfaced
/// to the operator at fakecloud startup; never silently swallowed.
#[derive(Debug, thiserror::Error)]
pub enum K8sBackendError {
    #[error(transparent)]
    Env(#[from] K8sEnvError),
    #[error("failed to connect to the Kubernetes cluster: {0}")]
    Connect(String),
}

/// Native Kubernetes Lambda execution backend.
pub struct K8sBackend {
    client: K8sClient,
    /// In-cluster URL of the fakecloud server (e.g.
    /// `http://fakecloud.fakecloud.svc.cluster.local:4566`). Init
    /// containers fetch code + layers from this host.
    self_url: String,
    /// Just the host part of `self_url` — used to rewrite localhost env
    /// values so user code can talk to fakecloud from inside the Pod.
    self_host: String,
    /// Host:port for the fakecloud ECR endpoint (defaults to the host
    /// of `self_url` when `FAKECLOUD_K8S_ECR_URL` is unset).
    ecr_host: String,
    ecr_port: u16,
    /// Bearer token the init container presents when fetching code +
    /// layers. Generated at server startup, kept in process memory only.
    internal_token: String,
    /// Optional `imagePullSecrets` reference for image-package functions
    /// that pull from a registry needing credentials.
    pull_secret: Option<String>,
}

impl K8sBackend {
    /// Read configuration from env vars and connect to the cluster.
    /// Fails fast on missing required config — never silently degrades.
    /// `default_ecr_port` is fakecloud's bound port; used as the ECR
    /// port when `FAKECLOUD_K8S_ECR_URL` is unset.
    pub async fn from_env(
        default_ecr_port: u16,
        internal_token: String,
    ) -> Result<Self, K8sBackendError> {
        let env = K8sEnv::from_env(default_ecr_port)?;
        let client = K8sClient::connect(env.namespace.clone())
            .await
            .map_err(|e| K8sBackendError::Connect(e.to_string()))?;

        tracing::info!(
            namespace = %env.namespace,
            self_url = %env.self_url,
            ecr = %format!("{}:{}", env.ecr_host, env.ecr_port),
            "K8s Lambda backend initialized"
        );

        Ok(Self {
            client,
            self_url: env.self_url,
            self_host: env.self_host,
            ecr_host: env.ecr_host,
            ecr_port: env.ecr_port,
            internal_token,
            pull_secret: env.pull_secret,
        })
    }
}

/// Extract the account ID from a function ARN
/// (`arn:aws:lambda:<region>:<account>:function:<name>[:<qual>]`).
fn account_id_from_arn(arn: &str) -> &str {
    arn.split(':').nth(4).unwrap_or("000000000000")
}

#[async_trait]
impl LambdaBackend for K8sBackend {
    fn name(&self) -> &str {
        "kubernetes"
    }

    async fn launch(
        &self,
        func: &LambdaFunction,
        _code_zip: Option<&[u8]>,
        _layers: &[Vec<u8>],
        deploy_id: &str,
    ) -> Result<WarmInstance, RuntimeError> {
        let account_id = account_id_from_arn(&func.function_arn);
        let ctx = PodSpecContext {
            instance_id: self.client.instance_id(),
            namespace: self.client.namespace(),
            self_url: &self.self_url,
            self_host: &self.self_host,
            ecr_host: &self.ecr_host,
            ecr_port: self.ecr_port,
            internal_token: &self.internal_token,
            account_id,
            pull_secret: self.pull_secret.as_deref(),
        };
        let pod =
            build_pod_spec(func, deploy_id, &ctx).map_err(RuntimeError::ContainerStartFailed)?;
        let pod_name = pod
            .metadata
            .name
            .clone()
            .unwrap_or_else(|| pod_name_for(&func.function_name, deploy_id));

        self.client
            .create_pod(&pod)
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("k8s create pod: {e}")))?;

        // Tear the Pod down again if it never becomes ready, so a failed
        // launch doesn't leak a Pod.
        let pod_ip = match self
            .client
            .wait_for_pod_ip(&pod_name, Duration::from_secs(60))
            .await
        {
            Ok(ip) => ip,
            Err(e) => {
                self.client.delete_pod(&pod_name).await;
                return Err(RuntimeError::ContainerStartFailed(e.to_string()));
            }
        };
        // Pod-Running doesn't guarantee the RIE inside the main container
        // is listening yet — TCP-handshake the invoke port like Docker.
        if let Err(e) = K8sClient::wait_for_tcp(&pod_ip, 8080, Duration::from_secs(10)).await {
            self.client.delete_pod(&pod_name).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "RIE on {pod_ip}:8080 not ready: {e}"
            )));
        }

        tracing::info!(
            function = %func.function_name,
            pod = %pod_name,
            namespace = %self.client.namespace(),
            pod_ip = %pod_ip,
            "Lambda Pod started"
        );

        Ok(WarmInstance {
            endpoint: format!("{pod_ip}:8080"),
            handle: BackendHandle::Pod {
                namespace: self.client.namespace().to_string(),
                name: pod_name,
            },
        })
    }

    async fn terminate(&self, handle: &BackendHandle) {
        match handle {
            BackendHandle::Pod { name, .. } => self.client.delete_pod(name).await,
            // Docker handles aren't ours to manage — defensive no-op.
            BackendHandle::Container { .. } => {}
        }
    }

    /// Sweep Lambda Pods that belong to a previous fakecloud process.
    /// Without this, a fakecloud restart leaks the previous run's Pods
    /// and `Create` collides on function names. Mirrors the docker
    /// `reaper` semantics.
    async fn reap_stale(&self) {
        self.client.reap_stale(SERVICE).await;
    }
}

#[cfg(test)]
mod tests {
    use super::account_id_from_arn;

    #[test]
    fn account_id_from_simple_arn() {
        assert_eq!(
            account_id_from_arn("arn:aws:lambda:us-east-1:123456789012:function:my-fn"),
            "123456789012"
        );
    }

    #[test]
    fn account_id_from_qualified_arn() {
        assert_eq!(
            account_id_from_arn("arn:aws:lambda:us-east-1:000000000000:function:my-fn:PROD"),
            "000000000000"
        );
    }

    #[test]
    fn account_id_falls_back_for_malformed_arn() {
        assert_eq!(account_id_from_arn("not-an-arn"), "000000000000");
    }
}
