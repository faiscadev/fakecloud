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
use fakecloud_core::auth::SessionCredentials;
use fakecloud_k8s::{K8sClient, K8sEnv, K8sEnvError, K8sPodConfig, K8sPodConfigError};

use k8s_openapi::api::core::v1::Pod;
use parking_lot::Mutex;

use super::backend::{
    BackendHandle, InstanceExit, LambdaBackend, RuntimeApiAddress, RuntimeError, WarmInstance,
};
use crate::state::LambdaFunction;
use spec::{build_credentials_secret, build_pod_spec, unique_pod_name, PodSpecContext};

/// Which `fakecloud-service` label Lambda Pods carry, so reaping only
/// touches Lambda Pods.
const SERVICE: &str = "lambda";

/// Env prefix for Lambda-scoped Pod config overrides
/// (`FAKECLOUD_LAMBDA_K8S_NODE_SELECTOR`, etc.). Merged over the global
/// `FAKECLOUD_K8S_*` config.
const POD_CONFIG_PREFIX: &str = "FAKECLOUD_LAMBDA_K8S";

/// Errors that can prevent the K8s backend from initializing. Surfaced
/// to the operator at fakecloud startup; never silently swallowed.
#[derive(Debug, thiserror::Error)]
pub enum K8sBackendError {
    #[error(transparent)]
    Env(#[from] K8sEnvError),
    #[error(transparent)]
    PodConfig(#[from] K8sPodConfigError),
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
    /// Port of `self_url`: where Pods reach fakecloud's listener.
    self_port: u16,
    /// The address `self_host` resolves to, once resolved: each Pod maps its
    /// Runtime API host name to it (`hostAliases`).
    self_ip: Mutex<Option<String>>,
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
    /// Global + Lambda-service node selector / tolerations / annotations
    /// applied to every Lambda Pod. Per-function tag overrides are merged
    /// over this at launch time.
    pod_config: K8sPodConfig,
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
        let pod_config = K8sPodConfig::resolved_base(POD_CONFIG_PREFIX)?;
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
            self_port: env.self_port,
            self_ip: Mutex::new(None),
            ecr_host: env.ecr_host,
            ecr_port: env.ecr_port,
            internal_token,
            pull_secret: env.pull_secret,
            pod_config,
        })
    }
}

/// How often a running Lambda Pod is checked for its runtime having exited.
const EXIT_POLL_INTERVAL: Duration = Duration::from_secs(1);

/// Name of the container running the function's runtime.
const MAIN_CONTAINER: &str = "fakecloud-lambda";

/// How the runtime container of `pod` ended, if it has.
fn runtime_exit(pod: &Pod) -> Option<InstanceExit> {
    let status = pod.status.as_ref()?;
    let terminated = status
        .container_statuses
        .as_ref()?
        .iter()
        .find(|c| c.name == MAIN_CONTAINER)?
        .state
        .as_ref()?
        .terminated
        .as_ref()?;
    let oom = terminated.reason.as_deref() == Some("OOMKilled");
    Some(match terminated.signal {
        Some(signal) if signal > 0 => InstanceExit {
            code: None,
            signal: Some(signal),
            oom,
        },
        _ => InstanceExit::from_container_code(i64::from(terminated.exit_code), oom),
    })
}

impl K8sBackend {
    /// The IP Pods map their Runtime API host to: `self_host` itself when it
    /// is an address, else what it resolves to here (fakecloud runs
    /// in-cluster, so its Service name resolves the same as in a Pod).
    async fn self_ip(&self) -> Result<String, RuntimeError> {
        if let Some(ip) = self.self_ip.lock().clone() {
            return Ok(ip);
        }
        let host = self.self_host.trim_start_matches('[').trim_end_matches(']');
        let ip = if host.parse::<std::net::IpAddr>().is_ok() {
            host.to_string()
        } else {
            tokio::net::lookup_host((host, self.self_port))
                .await
                .ok()
                .and_then(|mut addrs| addrs.next())
                .map(|a| a.ip().to_string())
                .ok_or_else(|| {
                    RuntimeError::ContainerStartFailed(format!(
                        "could not resolve FAKECLOUD_K8S_SELF_URL host {host} for the \
                         Lambda Runtime API; set it to an address or a name fakecloud resolves"
                    ))
                })?
        };
        *self.self_ip.lock() = Some(ip.clone());
        Ok(ip)
    }

    /// Wait for the Pod to be Running, or for its runtime to have already
    /// exited (an init that crashed at once). Errors for a Pod that never got
    /// its runtime started (image pull, code download, scheduling).
    async fn wait_started(&self, name: &str) -> Result<Option<InstanceExit>, RuntimeError> {
        let api = self.client.pods();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
        loop {
            let pod = api
                .get(name)
                .await
                .map_err(|e| RuntimeError::ContainerStartFailed(format!("k8s get pod: {e}")))?;
            if let Some(exit) = runtime_exit(&pod) {
                return Ok(Some(exit));
            }
            let phase = pod
                .status
                .as_ref()
                .and_then(|s| s.phase.as_deref())
                .unwrap_or("Unknown");
            match phase {
                "Running" => return Ok(None),
                "Failed" | "Succeeded" => {
                    return Err(RuntimeError::ContainerStartFailed(format!(
                        "pod {name} reached terminal phase {phase} before its runtime started"
                    )))
                }
                _ => {}
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "pod {name} did not start its runtime within 60s"
                )));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// Report the Pod's runtime exit (or the Pod vanishing) on `tx` until
    /// it happens or nobody watches any more.
    fn watch_exit(&self, name: String, tx: tokio::sync::watch::Sender<Option<InstanceExit>>) {
        let api = self.client.pods();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(EXIT_POLL_INTERVAL).await;
                if tx.is_closed() {
                    return;
                }
                let exit = match api.get_opt(&name).await {
                    Ok(Some(pod)) => runtime_exit(&pod).or_else(|| {
                        let phase = pod.status.as_ref().and_then(|s| s.phase.as_deref());
                        matches!(phase, Some("Failed" | "Succeeded")).then(InstanceExit::default)
                    }),
                    Ok(None) => Some(InstanceExit::default()),
                    // A failed poll proves nothing; try again.
                    Err(_) => None,
                };
                if let Some(exit) = exit {
                    let _ = tx.send(Some(exit));
                    return;
                }
            }
        });
    }
}

/// Extract the account ID from a function ARN
/// (`arn:aws:lambda:<region>:<account>:function:<name>[:<qual>]`).
fn account_id_from_arn(arn: &str) -> &str {
    fakecloud_aws::arn::account_of(arn).unwrap_or("000000000000")
}

#[async_trait]
impl LambdaBackend for K8sBackend {
    fn callback_endpoint(&self) -> Option<String> {
        Some(self.self_url.trim_end_matches('/').to_string())
    }

    fn name(&self) -> &str {
        "kubernetes"
    }

    /// Per-function tags carry scheduling overrides (`K8sPodConfig::from_tags`).
    fn launch_uses_tags(&self) -> bool {
        true
    }

    async fn launch(
        &self,
        func: &LambdaFunction,
        _code_zip: Option<&[u8]>,
        layers: &[Vec<u8>],
        deploy_id: &str,
        credentials: Option<&SessionCredentials>,
        runtime_api: &RuntimeApiAddress,
    ) -> Result<WarmInstance, RuntimeError> {
        let account_id = account_id_from_arn(&func.function_arn);
        let runtime_api_ip = self.self_ip().await?;
        // A per-launch unique name (instead of the deterministic
        // function+deploy one) so concurrent instances of the same function
        // don't collide and a terminating Pod never blocks its replacement
        // (see `unique_pod_name`). The credentials Secret shares it.
        let pod_name = unique_pod_name(&func.function_name, deploy_id);
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
            credentials_secret: credentials.map(|_| pod_name.as_str()),
            runtime_api_host: &runtime_api.host,
            runtime_api_ip: &runtime_api_ip,
            self_port: self.self_port,
            extensions: if func.package_type == "Image" {
                0
            } else {
                super::extensions::layer_extension_count(layers)
            },
        };
        runtime_api.sandbox.expect_extensions(ctx.extensions);
        let mut pod =
            build_pod_spec(func, deploy_id, &ctx).map_err(RuntimeError::ContainerStartFailed)?;
        pod.metadata.name = Some(pod_name.clone());

        // Apply operator-configured scheduling/metadata: global +
        // service base with this function's reserved-tag overrides merged
        // over it (per-function tags win).
        self.pod_config
            .clone()
            .merge(K8sPodConfig::from_tags(&func.tags))
            .apply(&mut pod);

        // The Secret goes first so the Pod's secretKeyRefs resolve at start.
        if let Some(creds) = credentials {
            let secret = build_credentials_secret(&pod_name, creds, self.client.instance_id());
            self.client.create_secret(&secret).await.map_err(|e| {
                RuntimeError::ContainerStartFailed(format!(
                    "k8s create credentials secret (the ServiceAccount needs \
                     create/delete/patch on secrets): {e}"
                ))
            })?;
        }
        if let Err(e) = self.client.create_pod(&pod).await {
            self.client.delete_secret(&pod_name).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "k8s create pod: {e}"
            )));
        }
        if credentials.is_some() {
            // Owned by the Pod: garbage-collected with it however it goes.
            self.client.adopt_secret(&pod_name, &pod_name).await;
        }

        // Tear the Pod down again if its runtime never starts, so a failed
        // launch doesn't leak a Pod.
        let exited = match self.wait_started(&pod_name).await {
            Ok(exited) => exited,
            Err(e) => {
                self.client.delete_pod(&pod_name).await;
                self.client.delete_secret(&pod_name).await;
                return Err(e);
            }
        };
        let (tx, exit) = tokio::sync::watch::channel(exited);
        if exited.is_none() {
            self.watch_exit(pod_name.clone(), tx);
        }

        tracing::info!(
            function = %func.function_name,
            pod = %pod_name,
            namespace = %self.client.namespace(),
            "Lambda Pod started"
        );

        Ok(WarmInstance {
            handle: BackendHandle::Pod {
                namespace: self.client.namespace().to_string(),
                name: pod_name,
            },
            exit,
        })
    }

    async fn terminate(&self, handle: &BackendHandle) {
        match handle {
            BackendHandle::Pod { name, .. } => {
                self.client.delete_pod(name).await;
                // Also garbage-collected with the Pod; deleting explicitly
                // drops the credentials without waiting on the collector.
                self.client.delete_secret(name).await;
            }
            // Docker handles aren't ours to manage — defensive no-op.
            BackendHandle::Container { .. } => {}
        }
    }

    async fn instance_logs(&self, handle: &BackendHandle) -> Option<String> {
        let BackendHandle::Pod { name, .. } = handle else {
            return None;
        };
        // The function's logs go to the runtime container's stdout/stderr.
        self.client
            .pod_logs(name, None)
            .await
            .ok()
            .filter(|s| !s.is_empty())
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
