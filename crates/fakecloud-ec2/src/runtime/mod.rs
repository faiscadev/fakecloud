//! Backing-container runtime for EC2 instances.
//!
//! `RunInstances` spins a real container per instance; the instance
//! lifecycle (`Start`/`Stop`/`Reboot`/`Terminate`) maps onto the container
//! lifecycle, and `DescribeInstances` reports the container's real private
//! IP. The container can run either as a local Docker/Podman container (the
//! default) or as a native Kubernetes Pod (`FAKECLOUD_EC2_BACKEND=k8s` or the
//! global `FAKECLOUD_CONTAINER_BACKEND=k8s`).
//!
//! Operations are keyed by **instance id**, not the backend handle: a
//! Kubernetes Pod can't be stopped and restarted in place, so `Stop` deletes
//! the Pod and `Start`/`Reboot` recreate it. The runtime therefore keeps,
//! per instance, the handle plus enough of the original request (image,
//! user-data) to recreate the backing container deterministically.
//!
//! The runtime is strictly additive: when no container backend is available
//! the control plane keeps its metadata-faithful behaviour (synthesized IPs,
//! state transitions) so every API call still succeeds. Real container
//! backing is best-effort fidelity layered on top.

mod k8s;

use std::collections::HashMap;
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

/// Error initializing the Kubernetes backend at startup. Surfaced to the
/// operator so a misconfigured cluster fails fast rather than silently
/// falling back to Docker.
#[derive(Debug, thiserror::Error)]
pub enum BackendInitError {
    #[error(transparent)]
    Env(#[from] fakecloud_k8s::K8sEnvError),
    #[error("failed to connect to the Kubernetes cluster: {0}")]
    Connect(String),
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

/// What the runtime remembers per instance so it can drive the backing
/// container's lifecycle and recreate it (k8s `Start`/`Reboot`).
#[derive(Debug, Clone)]
struct InstanceRecord {
    /// Docker container id, or Pod name.
    handle: String,
    /// Resolved base image, captured at `RunInstances` so a recreate is
    /// identical even if `FAKECLOUD_EC2_DEFAULT_IMAGE` later changes.
    image: String,
    /// Base64 user-data to re-run on recreate, if any.
    user_data: Option<String>,
}

/// The selected backing-container backend.
#[derive(Debug, Clone)]
enum InstanceBackend {
    Docker(DockerInstances),
    K8s(k8s::K8sInstances),
}

#[derive(Debug, Clone)]
pub struct Ec2Runtime {
    backend: InstanceBackend,
    /// Per-instance backing records, keyed by EC2 instance id, so the
    /// lifecycle operations and reset/shutdown teardown work without
    /// consulting service state.
    instances: Arc<RwLock<HashMap<String, InstanceRecord>>>,
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
            instances: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Construct the Kubernetes backend. `server_port` is fakecloud's bound
    /// port (used when `FAKECLOUD_K8S_SELF_URL` omits one). Fails fast on
    /// misconfiguration — never silently degrades to Docker.
    pub async fn new_k8s(server_port: u16) -> Result<Self, BackendInitError> {
        let backend = k8s::K8sInstances::from_env(server_port).await?;
        Ok(Self {
            backend: InstanceBackend::K8s(backend),
            instances: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Name of the active backend, for logging.
    pub fn cli_name(&self) -> &str {
        match &self.backend {
            InstanceBackend::Docker(d) => &d.cli,
            InstanceBackend::K8s(_) => "kubernetes",
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
        let image = default_image();
        let running = match &self.backend {
            InstanceBackend::Docker(d) => d.run_instance(instance_id, &image, user_data).await?,
            InstanceBackend::K8s(k) => k.spawn_pod(instance_id, &image, user_data).await?,
        };
        self.instances.write().insert(
            instance_id.to_string(),
            InstanceRecord {
                handle: running.container_id.clone(),
                image,
                user_data: user_data.map(str::to_string),
            },
        );
        Ok(running)
    }

    /// Stop an instance's backing container (maps to `StopInstances`).
    /// Docker stops the container in place; k8s deletes the Pod (recreated
    /// on the next `Start`).
    pub async fn stop_instance(&self, instance_id: &str) {
        let Some(handle) = self.handle_of(instance_id) else {
            return;
        };
        match &self.backend {
            InstanceBackend::Docker(d) => d.stop(&handle).await,
            InstanceBackend::K8s(k) => k.delete_pod(&handle).await,
        }
    }

    /// Start a previously-stopped instance (maps to `StartInstances`).
    /// Returns the running container's (possibly new) handle and private IP.
    /// Docker starts the existing container; k8s recreates the Pod under a new
    /// unique name, so the handle changes — callers should persist it.
    pub async fn start_instance(&self, instance_id: &str) -> Option<RunningInstance> {
        let record = self.instances.read().get(instance_id)?.clone();
        match &self.backend {
            InstanceBackend::Docker(d) => {
                // Same container; only the IP may change.
                let private_ip = d.start(&record.handle).await?;
                Some(RunningInstance {
                    container_id: record.handle,
                    private_ip,
                })
            }
            InstanceBackend::K8s(k) => {
                let running = k
                    .spawn_pod(instance_id, &record.image, record.user_data.as_deref())
                    .await
                    .ok()?;
                self.update_handle(instance_id, &running.container_id);
                Some(running)
            }
        }
    }

    /// Restart an instance's backing container (maps to `RebootInstances`).
    /// Docker restarts in place; k8s deletes and recreates the Pod under a new
    /// name. Returns the running container's handle + IP when it changed (k8s),
    /// so callers can persist the new handle; `None` when nothing to update.
    pub async fn reboot_instance(&self, instance_id: &str) -> Option<RunningInstance> {
        let record = self.instances.read().get(instance_id).cloned()?;
        match &self.backend {
            InstanceBackend::Docker(d) => {
                d.reboot(&record.handle).await;
                None
            }
            InstanceBackend::K8s(k) => {
                k.delete_pod(&record.handle).await;
                let running = k
                    .spawn_pod(instance_id, &record.image, record.user_data.as_deref())
                    .await
                    .ok()?;
                self.update_handle(instance_id, &running.container_id);
                Some(running)
            }
        }
    }

    /// Remove an instance's backing container (maps to `TerminateInstances`).
    pub async fn terminate_instance(&self, instance_id: &str) {
        let record = self.instances.write().remove(instance_id);
        if let Some(record) = record {
            match &self.backend {
                InstanceBackend::Docker(d) => d.remove(&record.handle).await,
                InstanceBackend::K8s(k) => k.delete_pod(&record.handle).await,
            }
        }
    }

    /// Tear down every container this runtime spawned (used on reset and
    /// shutdown). The Docker backend leans on the shared reaper for any
    /// container it loses track of.
    pub async fn stop_all(&self) {
        let records: Vec<InstanceRecord> = {
            let mut instances = self.instances.write();
            instances.drain().map(|(_, r)| r).collect()
        };
        for record in records {
            match &self.backend {
                InstanceBackend::Docker(d) => d.remove(&record.handle).await,
                InstanceBackend::K8s(k) => k.delete_pod(&record.handle).await,
            }
        }
    }

    /// Sweep instance Pods orphaned by a previous fakecloud process (k8s
    /// only; the Docker backend relies on the shared reaper).
    pub async fn reap_stale(&self) {
        if let InstanceBackend::K8s(k) = &self.backend {
            k.reap_stale().await;
        }
    }

    fn handle_of(&self, instance_id: &str) -> Option<String> {
        self.instances
            .read()
            .get(instance_id)
            .map(|r| r.handle.clone())
    }

    fn update_handle(&self, instance_id: &str, handle: &str) {
        if let Some(record) = self.instances.write().get_mut(instance_id) {
            record.handle = handle.to_string();
        }
    }
}

fn default_image() -> String {
    std::env::var(DEFAULT_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_IMAGE.to_string())
}

/// Keep-alive command + user-data wrapper for a base image. Shared by both
/// backends so they boot identical containers. When `user_data` (base64) is
/// present it is decoded and run as a root shell script, backgrounded so a
/// slow script never blocks readiness, then the container tails forever.
fn boot_command(user_data: Option<&str>) -> Vec<String> {
    match user_data.filter(|s| !s.is_empty()) {
        Some(b64) => {
            let script = format!("printf %s '{b64}' | base64 -d | sh & exec tail -f /dev/null");
            vec!["sh".to_string(), "-c".to_string(), script]
        }
        None => vec![
            "tail".to_string(),
            "-f".to_string(),
            "/dev/null".to_string(),
        ],
    }
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
        image: &str,
        user_data: Option<&str>,
    ) -> Result<RunningInstance, RuntimeError> {
        let mut args: Vec<String> = vec![
            "run".to_string(),
            "-d".to_string(),
            "--label".to_string(),
            format!("fakecloud-ec2={instance_id}"),
            "--label".to_string(),
            format!("fakecloud-instance={}", self.instance_id),
            image.to_string(),
        ];
        args.extend(boot_command(user_data));

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
