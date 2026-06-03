//! Kube client wrapper + generic Pod lifecycle shared by every backend.
//!
//! Wraps a [`kube::Client`] scoped to one namespace and provides the
//! handful of operations every FakeCloud k8s backend performs: create a
//! Pod (idempotently, replacing any stale same-named Pod), wait for it
//! to get an IP and become reachable, `exec` a command inside it, delete
//! it, and reap Pods orphaned by a previous process. Service-specific
//! Pod *construction* stays in each service; this is only the plumbing.

use std::time::{Duration, Instant};

use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Status;
use kube::api::{Api, AttachParams, DeleteParams, ListParams, PostParams};
use kube::Client;
use tokio::io::AsyncReadExt;

use crate::labels;

/// Errors from Pod lifecycle operations.
#[derive(Debug, thiserror::Error)]
pub enum K8sError {
    #[error("kubernetes API error: {0}")]
    Kube(#[from] kube::Error),
    #[error("failed to construct Kubernetes client: {0}")]
    Connect(String),
    #[error("timed out: {0}")]
    Timeout(String),
    #[error("{0}")]
    Other(String),
}

/// Output of an [`K8sClient::exec`] call.
#[derive(Debug, Clone)]
pub struct ExecOutput {
    /// Bytes written to the command's stdout.
    pub stdout: Vec<u8>,
    /// The command's stderr, decoded lossily as UTF-8.
    pub stderr: String,
    /// The command's exit code, if Kubernetes reported one. `Some(0)` on
    /// success, `Some(n)` on a non-zero exit, `None` if the status was
    /// unparseable.
    pub exit_code: Option<i32>,
}

impl ExecOutput {
    /// Whether the command exited successfully (exit code 0).
    pub fn success(&self) -> bool {
        self.exit_code == Some(0)
    }

    /// Stdout decoded lossily as UTF-8.
    pub fn stdout_str(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.stdout)
    }
}

/// A namespaced kube client plus this process's instance identity.
#[derive(Clone)]
pub struct K8sClient {
    client: Client,
    namespace: String,
    instance_id: String,
}

impl K8sClient {
    /// Install the rustls CryptoProvider, connect a client from the
    /// ambient config (in-cluster ServiceAccount or kubeconfig), and
    /// scope it to `namespace`.
    pub async fn connect(namespace: impl Into<String>) -> Result<Self, K8sError> {
        ensure_crypto_provider();
        let client = Client::try_default()
            .await
            .map_err(|e| K8sError::Connect(e.to_string()))?;
        Ok(Self::from_client(client, namespace.into()))
    }

    /// Wrap an already-constructed client (used by tests and callers that
    /// share a client across backends).
    pub fn from_client(client: Client, namespace: String) -> Self {
        Self {
            client,
            namespace,
            instance_id: labels::instance_id(),
        }
    }

    /// The namespace Pods are created in.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// This process's instance identity (`fakecloud-<pid>`), used for the
    /// [`labels::INSTANCE`] label.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// The underlying client, for callers that need raw API access.
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Namespaced Pod API handle.
    pub fn pods(&self) -> Api<Pod> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    /// Create `pod`, first deleting any stale Pod with the same name left
    /// behind by a previous process (which would otherwise make `create`
    /// return `409 Conflict`). Retries the create a few times while the
    /// API server finishes deleting the old Pod.
    pub async fn create_pod(&self, pod: &Pod) -> Result<(), K8sError> {
        let name = pod
            .metadata
            .name
            .clone()
            .ok_or_else(|| K8sError::Other("pod spec has no metadata.name".into()))?;
        let api = self.pods();
        let _ = api.delete(&name, &DeleteParams::default()).await;
        for attempt in 0..6 {
            match api.create(&PostParams::default(), pod).await {
                Ok(_) => return Ok(()),
                Err(kube::Error::Api(e)) if e.code == 409 && attempt < 5 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    let _ = api.delete(&name, &DeleteParams::default()).await;
                    continue;
                }
                Err(e) => return Err(K8sError::Kube(e)),
            }
        }
        Err(K8sError::Timeout(format!(
            "pod {name} could not be created after repeated 409 conflicts"
        )))
    }

    /// Poll until the Pod has a non-empty `status.podIP` and phase
    /// `Running`, returning the IP. Errors if the Pod reaches a terminal
    /// phase (`Failed`/`Succeeded`) during startup or if `timeout`
    /// elapses first.
    pub async fn wait_for_pod_ip(&self, name: &str, timeout: Duration) -> Result<String, K8sError> {
        let api = self.pods();
        let deadline = Instant::now() + timeout;
        loop {
            let pod = api.get(name).await?;
            let status = pod.status.as_ref();
            let ip = status
                .and_then(|s| s.pod_ip.as_ref())
                .filter(|s| !s.is_empty());
            if let Some(ip) = ip {
                match status.and_then(|s| s.phase.as_deref()).unwrap_or("Unknown") {
                    "Running" => return Ok(ip.clone()),
                    phase @ ("Failed" | "Succeeded") => {
                        return Err(K8sError::Other(format!(
                            "pod {name} reached terminal phase {phase} during startup"
                        )));
                    }
                    _ => {}
                }
            }
            if Instant::now() >= deadline {
                return Err(K8sError::Timeout(format!(
                    "pod {name} did not become Running with a podIP within {timeout:?}"
                )));
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    /// TCP-handshake `ip:port` until it accepts a connection or `timeout`
    /// elapses. A Pod being `Running` doesn't guarantee the process
    /// inside it is listening yet, so backends follow [`wait_for_pod_ip`]
    /// with this.
    ///
    /// [`wait_for_pod_ip`]: Self::wait_for_pod_ip
    pub async fn wait_for_tcp(ip: &str, port: u16, timeout: Duration) -> Result<(), K8sError> {
        let deadline = Instant::now() + timeout;
        let addr = format!("{ip}:{port}");
        loop {
            if tokio::net::TcpStream::connect(&addr).await.is_ok() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(K8sError::Timeout(format!(
                    "{addr} did not accept connections within {timeout:?}"
                )));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    /// Run `cmd` inside `pod` (in `container`, or the default container
    /// when `None`) and collect stdout/stderr/exit-code. This is the k8s
    /// equivalent of `docker exec` — used for operations like issuing
    /// `redis-cli` commands or copying a file out of a Pod.
    pub async fn exec(
        &self,
        pod: &str,
        container: Option<&str>,
        cmd: &[&str],
    ) -> Result<ExecOutput, K8sError> {
        let api = self.pods();
        let mut ap = AttachParams::default()
            .stdin(false)
            .stdout(true)
            .stderr(true);
        if let Some(c) = container {
            ap = ap.container(c.to_string());
        }
        let mut proc = api.exec(pod, cmd.iter().copied(), &ap).await?;

        let mut stdout = Vec::new();
        if let Some(mut s) = proc.stdout() {
            s.read_to_end(&mut stdout)
                .await
                .map_err(|e| K8sError::Other(format!("reading exec stdout: {e}")))?;
        }
        let mut stderr_buf = Vec::new();
        if let Some(mut s) = proc.stderr() {
            // stderr being unreadable shouldn't mask a successful command.
            let _ = s.read_to_end(&mut stderr_buf).await;
        }
        let status = match proc.take_status() {
            Some(fut) => fut.await,
            None => None,
        };
        // Drain the connection so the websocket closes cleanly.
        let _ = proc.join().await;

        Ok(ExecOutput {
            stdout,
            stderr: String::from_utf8_lossy(&stderr_buf).into_owned(),
            exit_code: exit_code_from_status(status.as_ref()),
        })
    }

    /// Delete a Pod by name. Idempotent — a `404` (already gone) is
    /// treated as success; other errors are logged but not returned,
    /// since teardown is best-effort.
    pub async fn delete_pod(&self, name: &str) {
        let api = self.pods();
        if let Err(e) = api.delete(name, &DeleteParams::default()).await {
            if let kube::Error::Api(api_err) = &e {
                if api_err.code == 404 {
                    return;
                }
            }
            tracing::warn!(pod = %name, namespace = %self.namespace, error = %e, "k8s delete pod failed");
        }
    }

    /// Delete Pods of the given `service` left behind by a *different*
    /// process. Lists Pods labelled with both [`labels::MANAGED_BY`] and
    /// the `service` value, and deletes those whose [`labels::INSTANCE`]
    /// differs from this process's. Mirrors the Docker reaper so a
    /// restart doesn't leak the previous run's Pods. Returns the count
    /// reaped.
    pub async fn reap_stale(&self, service: &str) -> usize {
        let api = self.pods();
        let selector = format!(
            "{}={},{}={}",
            labels::MANAGED_BY,
            labels::MANAGED_BY_VALUE,
            labels::SERVICE,
            service
        );
        let lp = ListParams::default().labels(&selector);
        let list = match api.list(&lp).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(service, error = %e, "k8s reap_stale: list pods failed");
                return 0;
            }
        };
        let mut reaped = 0usize;
        for pod in list.items {
            let inst = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(labels::INSTANCE))
                .map(String::as_str);
            if inst == Some(self.instance_id.as_str()) {
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
            tracing::info!(service, reaped, "k8s reap_stale: removed orphan Pods");
        }
        reaped
    }
}

/// Install rustls' `ring` CryptoProvider once per process. Rustls 0.23
/// dropped the implicit default and every TLS connection panics until
/// one is installed; kube's `rustls-tls` feature doesn't pull one in on
/// our behalf. Safe to call concurrently and repeatedly — the `.ok()`
/// swallows the "already installed" error.
pub fn ensure_crypto_provider() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Derive an exit code from the terminal `Status` Kubernetes returns for
/// an `exec`. `Success` -> 0; a `Failure` carries the real code in
/// `details.causes[reason=ExitCode].message`, falling back to 1 when the
/// cause is absent.
fn exit_code_from_status(status: Option<&Status>) -> Option<i32> {
    let status = status?;
    match status.status.as_deref() {
        Some("Success") => Some(0),
        Some("Failure") => status
            .details
            .as_ref()
            .and_then(|d| d.causes.as_ref())
            .and_then(|causes| {
                causes
                    .iter()
                    .find(|c| c.reason.as_deref() == Some("ExitCode"))
            })
            .and_then(|c| c.message.as_ref())
            .and_then(|m| m.parse::<i32>().ok())
            .or(Some(1)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::{StatusCause, StatusDetails};

    fn status(state: &str, causes: Option<Vec<StatusCause>>) -> Status {
        Status {
            status: Some(state.to_string()),
            details: causes.map(|c| StatusDetails {
                causes: Some(c),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn success_status_is_exit_zero() {
        assert_eq!(
            exit_code_from_status(Some(&status("Success", None))),
            Some(0)
        );
    }

    #[test]
    fn failure_with_exit_code_cause_parses_code() {
        let causes = vec![StatusCause {
            reason: Some("ExitCode".into()),
            message: Some("137".into()),
            ..Default::default()
        }];
        assert_eq!(
            exit_code_from_status(Some(&status("Failure", Some(causes)))),
            Some(137)
        );
    }

    #[test]
    fn failure_without_cause_defaults_to_one() {
        assert_eq!(
            exit_code_from_status(Some(&status("Failure", None))),
            Some(1)
        );
    }

    #[test]
    fn missing_status_is_none() {
        assert_eq!(exit_code_from_status(None), None);
    }

    #[test]
    fn exec_output_helpers() {
        let out = ExecOutput {
            stdout: b"hello".to_vec(),
            stderr: String::new(),
            exit_code: Some(0),
        };
        assert!(out.success());
        assert_eq!(out.stdout_str(), "hello");
    }
}
