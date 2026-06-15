//! Backing-container runtime for EC2 instances.
//!
//! `RunInstances` spins a real container per instance; the instance
//! lifecycle (`Start`/`Stop`/`Reboot`/`Terminate`) maps onto the container
//! lifecycle, and `DescribeInstances` reports the container's real private
//! IP. The container can run either as a local Docker/Podman container (the
//! default) or as a native Kubernetes Pod (`FAKECLOUD_EC2_BACKEND=k8s` or the
//! global `FAKECLOUD_CONTAINER_BACKEND=k8s`, added in a follow-up batch).
//!
//! The runtime is strictly additive: when no container backend is available
//! the control plane keeps its metadata-faithful behaviour (synthesized IPs,
//! state transitions) so every API call still succeeds. Real container
//! backing is best-effort fidelity layered on top.

use std::collections::HashSet;
use std::sync::Arc;

use parking_lot::RwLock;

/// Default base image an instance's container runs. AMIs don't map to a
/// concrete OS image, so we boot a real Amazon Linux container by default
/// (overridable via `FAKECLOUD_EC2_DEFAULT_IMAGE`, e.g. to a lighter image
/// in CI). The container is kept alive with `tail -f /dev/null` — EC2
/// instances are long-running hosts, not one-shot tasks. `tail` is used
/// rather than `sleep infinity` so any base image works (busybox `sleep`
/// rejects `infinity`).
const DEFAULT_IMAGE_ENV: &str = "FAKECLOUD_EC2_DEFAULT_IMAGE";
const DEFAULT_IMAGE: &str = "amazonlinux:2023";

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container failed to start: {0}")]
    ContainerStartFailed(String),
}

/// A running instance's backing container.
#[derive(Debug, Clone)]
pub struct RunningInstance {
    /// Backend-specific handle: a Docker container id, or a Pod name.
    pub container_id: String,
    /// The instance's private IP — the container's address on the daemon
    /// network (Docker) or the Pod IP (k8s).
    pub private_ip: String,
}

/// The selected backing-container backend.
#[derive(Debug, Clone)]
enum InstanceBackend {
    Docker(DockerInstances),
}

#[derive(Debug, Clone)]
pub struct Ec2Runtime {
    backend: InstanceBackend,
    /// Container ids this runtime has spawned and not yet torn down, so reset
    /// and shutdown can reap them without consulting service state.
    tracked: Arc<RwLock<HashSet<String>>>,
}

impl Ec2Runtime {
    /// Construct the Docker/Podman backend. Returns `None` when no container
    /// CLI is available — callers then run in metadata-only mode.
    pub fn new() -> Option<Self> {
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        Some(Self {
            backend: InstanceBackend::Docker(DockerInstances {
                cli,
                instance_id: format!("fakecloud-{}", std::process::id()),
            }),
            tracked: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Name of the active backend, for logging.
    pub fn cli_name(&self) -> &str {
        match &self.backend {
            InstanceBackend::Docker(d) => &d.cli,
        }
    }

    /// Boot a container for an instance. `user_data` is the base64-encoded
    /// user-data as received on the wire (RunInstances `UserData`), run at
    /// boot the way cloud-init would, if present.
    pub async fn run_instance(
        &self,
        instance_id: &str,
        user_data: Option<&str>,
    ) -> Result<RunningInstance, RuntimeError> {
        let running = match &self.backend {
            InstanceBackend::Docker(d) => d.run_instance(instance_id, user_data).await?,
        };
        self.tracked.write().insert(running.container_id.clone());
        Ok(running)
    }

    /// Stop an instance's container (maps to `StopInstances`).
    pub async fn stop_instance(&self, container_id: &str) {
        match &self.backend {
            InstanceBackend::Docker(d) => d.stop(container_id).await,
        }
    }

    /// Start a previously-stopped container (maps to `StartInstances`).
    /// Returns the (possibly new) private IP the container came up with.
    pub async fn start_instance(&self, container_id: &str) -> Option<String> {
        match &self.backend {
            InstanceBackend::Docker(d) => d.start(container_id).await,
        }
    }

    /// Restart an instance's container in place (maps to `RebootInstances`).
    pub async fn reboot_instance(&self, container_id: &str) {
        match &self.backend {
            InstanceBackend::Docker(d) => d.reboot(container_id).await,
        }
    }

    /// Remove an instance's container (maps to `TerminateInstances`).
    pub async fn terminate_instance(&self, container_id: &str) {
        self.tracked.write().remove(container_id);
        match &self.backend {
            InstanceBackend::Docker(d) => d.remove(container_id).await,
        }
    }

    /// Tear down every container this runtime spawned (used on reset and
    /// shutdown). The Docker backend leans on the shared reaper for any
    /// container it loses track of.
    pub async fn stop_all(&self) {
        let ids: Vec<String> = self.tracked.write().drain().collect();
        match &self.backend {
            InstanceBackend::Docker(d) => {
                for id in &ids {
                    d.remove(id).await;
                }
            }
        }
    }
}

fn default_image() -> String {
    std::env::var(DEFAULT_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_IMAGE.to_string())
}

/// Docker/Podman backend: shells out to the container CLI.
#[derive(Debug, Clone)]
struct DockerInstances {
    cli: String,
    instance_id: String,
}

impl DockerInstances {
    async fn run_instance(
        &self,
        instance_id: &str,
        user_data: Option<&str>,
    ) -> Result<RunningInstance, RuntimeError> {
        let image = default_image();
        let args: Vec<String> = vec![
            "run".to_string(),
            "-d".to_string(),
            "--label".to_string(),
            format!("fakecloud-ec2={instance_id}"),
            "--label".to_string(),
            format!("fakecloud-instance={}", self.instance_id),
            image.clone(),
            "tail".to_string(),
            "-f".to_string(),
            "/dev/null".to_string(),
        ];

        let output = tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();

        // Run user-data the way cloud-init does on a real instance: decode the
        // base64 the SDK sent and execute it as a root shell script,
        // asynchronously at boot. Detached (`-d`) so a slow or hanging script
        // never blocks the RunInstances response. Decoding happens inside the
        // container (`base64 -d`) so fakecloud needs no base64 dependency and
        // the bytes never touch the host.
        if let Some(b64) = user_data.filter(|s| !s.is_empty()) {
            let script = format!("printf %s '{b64}' | base64 -d | sh");
            let _ = tokio::process::Command::new(&self.cli)
                .args(["exec", "-d", &container_id, "sh", "-c", &script])
                .output()
                .await;
        }

        let private_ip = self
            .inspect_ip(&container_id)
            .await
            .unwrap_or_else(|| "10.0.0.1".to_string());

        Ok(RunningInstance {
            container_id,
            private_ip,
        })
    }

    /// Read the container's private IP from `inspect`. Returns `None` if the
    /// container has no address (e.g. host networking) — the caller falls
    /// back to a synthesized IP.
    async fn inspect_ip(&self, container_id: &str) -> Option<String> {
        let output = tokio::process::Command::new(&self.cli)
            .args([
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                container_id,
            ])
            .output()
            .await
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let ip = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if ip.is_empty() {
            None
        } else {
            Some(ip)
        }
    }

    async fn stop(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["stop", container_id])
            .output()
            .await;
    }

    async fn start(&self, container_id: &str) -> Option<String> {
        let started = tokio::process::Command::new(&self.cli)
            .args(["start", container_id])
            .output()
            .await
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !started {
            return None;
        }
        self.inspect_ip(container_id).await
    }

    async fn reboot(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["restart", container_id])
            .output()
            .await;
    }

    async fn remove(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }
}
