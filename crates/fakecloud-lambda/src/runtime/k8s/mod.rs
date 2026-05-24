//! Kubernetes [`LambdaBackend`] implementation.
//!
//! Spawns Lambda function runtimes as native Pods in a Kubernetes
//! cluster instead of as Docker containers. Gated by
//! `FAKECLOUD_LAMBDA_BACKEND=k8s` on the fakecloud server.
//!
//! See `website/content/docs/guides/kubernetes-backend.md` for the
//! operator-facing setup (ServiceAccount, RBAC, Deployment yaml).

pub mod spec;

use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use kube::api::{Api, DeleteParams, ListParams, PostParams};
use kube::Client;

use super::backend::{BackendHandle, LambdaBackend, RuntimeError, WarmInstance};
use crate::state::LambdaFunction;
use spec::{build_pod_spec, pod_name_for, PodSpecContext};

/// Errors that can prevent the K8s backend from initializing. Surfaced
/// to the operator at fakecloud startup; never silently swallowed.
#[derive(Debug, thiserror::Error)]
pub enum K8sBackendError {
    #[error("FAKECLOUD_K8S_SELF_URL must be set when FAKECLOUD_LAMBDA_BACKEND=k8s")]
    MissingSelfUrl,
    #[error("FAKECLOUD_K8S_SELF_URL is not a valid URL: {0}")]
    InvalidSelfUrl(String),
    #[error("failed to construct Kubernetes client: {0}")]
    Client(#[from] kube::Error),
    #[error("failed to read kubeconfig / in-cluster config: {0}")]
    Config(String),
}

/// Native Kubernetes Lambda execution backend.
pub struct K8sBackend {
    client: Client,
    namespace: String,
    instance_id: String,
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
        ensure_crypto_provider();
        let self_url =
            std::env::var("FAKECLOUD_K8S_SELF_URL").map_err(|_| K8sBackendError::MissingSelfUrl)?;
        let parsed = reqwest::Url::parse(&self_url)
            .map_err(|e| K8sBackendError::InvalidSelfUrl(e.to_string()))?;
        let self_host = parsed
            .host_str()
            .ok_or_else(|| K8sBackendError::InvalidSelfUrl("missing host".into()))?
            .to_string();
        let self_port = parsed.port_or_known_default().unwrap_or(default_ecr_port);

        let (ecr_host, ecr_port) = match std::env::var("FAKECLOUD_K8S_ECR_URL").ok() {
            Some(raw) => {
                let u = reqwest::Url::parse(&raw)
                    .map_err(|e| K8sBackendError::InvalidSelfUrl(e.to_string()))?;
                let h = u
                    .host_str()
                    .ok_or_else(|| K8sBackendError::InvalidSelfUrl("ECR url missing host".into()))?
                    .to_string();
                let p = u.port_or_known_default().unwrap_or(default_ecr_port);
                (h, p)
            }
            None => (self_host.clone(), self_port),
        };

        let namespace =
            std::env::var("FAKECLOUD_K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let pull_secret = std::env::var("FAKECLOUD_K8S_PULL_SECRET").ok();

        let client = Client::try_default()
            .await
            .map_err(|e| K8sBackendError::Config(e.to_string()))?;

        let instance_id = format!("fakecloud-{}", std::process::id());

        tracing::info!(
            namespace = %namespace,
            self_url = %self_url,
            ecr = %format!("{ecr_host}:{ecr_port}"),
            "K8s Lambda backend initialized"
        );

        Ok(Self {
            client,
            namespace,
            instance_id,
            self_url,
            self_host,
            ecr_host,
            ecr_port,
            internal_token,
            pull_secret,
        })
    }

    fn pods_api(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    /// Watch the pod until it has a non-empty `status.podIP`. Polled
    /// rather than streamed — simpler, and the polling cadence (1s) is
    /// well below the typical 5-30s pod boot time for an RIE image.
    async fn wait_for_pod_ip(&self, pod_name: &str) -> Result<String, RuntimeError> {
        let api = self.pods_api();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            let pod = api.get(pod_name).await.map_err(|e| {
                RuntimeError::ContainerStartFailed(format!("k8s get pod {pod_name}: {e}"))
            })?;
            if let Some(ip) = pod
                .status
                .as_ref()
                .and_then(|s| s.pod_ip.as_ref())
                .filter(|s| !s.is_empty())
            {
                // Pod might have IP but not yet be Running — check phase
                let phase = pod
                    .status
                    .as_ref()
                    .and_then(|s| s.phase.as_deref())
                    .unwrap_or("Unknown");
                if phase == "Running" {
                    return Ok(ip.clone());
                }
                if phase == "Failed" || phase == "Succeeded" {
                    return Err(RuntimeError::ContainerStartFailed(format!(
                        "pod {pod_name} reached terminal phase {phase} during startup"
                    )));
                }
            }
            if std::time::Instant::now() >= deadline {
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "pod {pod_name} did not become Running with podIP within 60s"
                )));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// TCP-handshake the RIE port. Pod-Running doesn't guarantee the
    /// RIE inside the main container is listening yet — same logic as
    /// Docker's `wait_for_ready`.
    async fn wait_for_rie_ready(&self, pod_ip: &str) -> Result<(), RuntimeError> {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if tokio::net::TcpStream::connect(format!("{pod_ip}:8080"))
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        Err(RuntimeError::ContainerStartFailed(format!(
            "RIE on {pod_ip}:8080 did not accept connections within 10s"
        )))
    }
}

/// Install rustls' `ring` CryptoProvider once per process. Rustls
/// 0.23 dropped the implicit default and every TLS connection now
/// panics until something installs one. Kube's `rustls-tls` feature
/// doesn't pull in a provider on our behalf, so we do it here. Safe
/// to call concurrently; the `.ok()` swallows the "already installed"
/// error in case another component (a different test, a future
/// service) beat us to it.
fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
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
            instance_id: &self.instance_id,
            namespace: &self.namespace,
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

        let api = self.pods_api();

        // Best-effort delete of any stale pod with the same name (e.g.
        // a previous fakecloud process left it behind). `Created` from
        // a stale pod would otherwise come back as `Conflict (409)`.
        let _ = api.delete(&pod_name, &DeleteParams::default()).await;
        // Give the API server a moment to actually delete; otherwise
        // create can race and 409. Polling for absence would be more
        // correct — keep simple for now and retry create on 409.
        for attempt in 0..6 {
            match api.create(&PostParams::default(), &pod).await {
                Ok(_) => break,
                Err(kube::Error::Api(e)) if e.code == 409 && attempt < 5 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let _ = api.delete(&pod_name, &DeleteParams::default()).await;
                    continue;
                }
                Err(e) => {
                    return Err(RuntimeError::ContainerStartFailed(format!(
                        "k8s create pod {pod_name}: {e}"
                    )));
                }
            }
        }

        let pod_ip = self.wait_for_pod_ip(&pod_name).await.inspect_err(|_| {
            let api = self.pods_api();
            let name = pod_name.clone();
            tokio::spawn(async move {
                let _ = api.delete(&name, &DeleteParams::default()).await;
            });
        })?;
        self.wait_for_rie_ready(&pod_ip).await.inspect_err(|_| {
            let api = self.pods_api();
            let name = pod_name.clone();
            tokio::spawn(async move {
                let _ = api.delete(&name, &DeleteParams::default()).await;
            });
        })?;

        tracing::info!(
            function = %func.function_name,
            pod = %pod_name,
            namespace = %self.namespace,
            pod_ip = %pod_ip,
            "Lambda Pod started"
        );

        Ok(WarmInstance {
            endpoint: format!("{pod_ip}:8080"),
            handle: BackendHandle::Pod {
                namespace: self.namespace.clone(),
                name: pod_name,
            },
        })
    }

    async fn terminate(&self, handle: &BackendHandle) {
        let (ns, name) = match handle {
            BackendHandle::Pod { namespace, name } => (namespace.clone(), name.clone()),
            // Docker handles aren't ours to manage — defensive no-op.
            BackendHandle::Container { .. } => return,
        };
        let api: Api<Pod> = Api::namespaced(self.client.clone(), &ns);
        if let Err(e) = api.delete(&name, &DeleteParams::default()).await {
            // 404 is normal on idempotent re-delete; surface anything else.
            if let kube::Error::Api(api_err) = &e {
                if api_err.code == 404 {
                    return;
                }
            }
            tracing::warn!(pod = %name, namespace = %ns, error = %e, "k8s delete pod failed");
        }
    }

    /// Sweep Lambda Pods that belong to a previous fakecloud process.
    /// Without this, a fakecloud restart leaks the previous run's Pods
    /// and `Create` collides on function names. Mirrors the docker
    /// `reaper` semantics.
    async fn reap_stale(&self) {
        let api = self.pods_api();
        let lp = ListParams::default().labels("fakecloud-managed-by=fakecloud");
        let list = match api.list(&lp).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "k8s reap_stale: list pods failed");
                return;
            }
        };
        let mut reaped = 0usize;
        for pod in list.items {
            let labels = pod.metadata.labels.as_ref();
            let inst = labels.and_then(|l| l.get("fakecloud-instance")).cloned();
            if inst.as_deref() == Some(self.instance_id.as_str()) {
                continue;
            }
            if let Some(name) = pod.metadata.name {
                if let Err(e) = api.delete(&name, &DeleteParams::default()).await {
                    tracing::warn!(pod = %name, error = %e, "k8s reap_stale: delete failed");
                } else {
                    reaped += 1;
                }
            }
        }
        if reaped > 0 {
            tracing::info!(reaped, "k8s reap_stale: removed orphan Lambda Pods");
        }
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
