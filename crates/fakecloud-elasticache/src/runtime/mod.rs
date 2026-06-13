//! Backing-container runtime for ElastiCache.
//!
//! ElastiCache cache clusters / replication groups / serverless caches
//! are backed by a real `redis` or `memcached` process. That process can
//! run either as a local Docker/Podman container (the default) or as a
//! native Kubernetes Pod (`FAKECLOUD_ELASTICACHE_BACKEND=k8s` or the
//! global `FAKECLOUD_CONTAINER_BACKEND=k8s`). The [`ElastiCacheRuntime`]
//! dispatches every operation to the selected [`CacheBackend`]; the
//! shared k8s plumbing lives in the `fakecloud-k8s` crate.

mod k8s;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

pub use k8s::PendingRdb;

/// Which cache engine a resource runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheEngineKind {
    Redis,
    Memcached,
}

impl CacheEngineKind {
    /// Container image for this engine.
    fn image(self) -> &'static str {
        match self {
            CacheEngineKind::Redis => "redis:7-alpine",
            CacheEngineKind::Memcached => "memcached:1.6-alpine",
        }
    }

    /// Default port the engine listens on.
    fn port(self) -> u16 {
        match self {
            CacheEngineKind::Redis => 6379,
            CacheEngineKind::Memcached => 11211,
        }
    }
}

/// A running cache backing instance (container or Pod).
#[derive(Debug, Clone)]
pub struct RunningCacheContainer {
    /// Backend-specific handle: a Docker container id, or a Pod name.
    pub container_id: String,
    /// The host port the engine is published on (Docker), or the engine's
    /// in-Pod port (k8s). Persisted in resource state.
    pub host_port: u16,
    /// Address clients connect to: `127.0.0.1` for Docker (published port
    /// on the host), or the Pod IP for k8s.
    pub endpoint_address: String,
    /// Port clients connect to: the published host port for Docker, the
    /// engine's standard port for k8s.
    pub endpoint_port: u16,
    /// Which engine this is — used by the k8s backend to respawn on
    /// reboot.
    pub engine: CacheEngineKind,
}

/// Outcome of a `redis-cli` invocation, normalized across backends so
/// callers don't depend on `std::process::Output`.
#[derive(Debug, Clone)]
pub struct CacheExec {
    /// Whether the command exited 0.
    pub success: bool,
    /// Raw stdout bytes.
    pub stdout: Vec<u8>,
    /// Raw stderr bytes.
    pub stderr: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container runtime is unavailable")]
    Unavailable,
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

/// The selected backing-container backend.
#[derive(Debug, Clone)]
enum CacheBackend {
    Docker(DockerCache),
    K8s(k8s::K8sCache),
}

#[derive(Debug, Clone)]
pub struct ElastiCacheRuntime {
    backend: CacheBackend,
    containers: Arc<RwLock<HashMap<String, RunningCacheContainer>>>,
}

impl ElastiCacheRuntime {
    /// Construct the Docker/Podman backend. Returns `None` when no
    /// container CLI is available.
    pub fn new() -> Option<Self> {
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        let net = fakecloud_core::container_net::HostNetworking::detect(&cli);
        Some(Self {
            backend: CacheBackend::Docker(DockerCache {
                cli,
                net,
                instance_id: format!("fakecloud-{}", std::process::id()),
            }),
            containers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Construct the Kubernetes backend. `server_port` is fakecloud's
    /// bound port (used when `FAKECLOUD_K8S_SELF_URL` omits one);
    /// `internal_token` guards the per-resource RDB endpoint that seeds
    /// snapshot data into restored Redis Pods. Fails fast on
    /// misconfiguration — never silently degrades.
    pub async fn new_k8s(
        server_port: u16,
        internal_token: String,
    ) -> Result<Self, BackendInitError> {
        let cache = k8s::K8sCache::from_env(server_port, internal_token).await?;
        Ok(Self {
            backend: CacheBackend::K8s(cache),
            containers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Name of the active backend, for logging.
    pub fn cli_name(&self) -> &str {
        match &self.backend {
            CacheBackend::Docker(d) => &d.cli,
            CacheBackend::K8s(_) => "kubernetes",
        }
    }

    /// The pending-RDB map the server's internal endpoint serves from.
    /// `None` on the Docker backend (which stages snapshots via the
    /// daemon, not HTTP).
    pub fn pending_rdb(&self) -> Option<PendingRdb> {
        match &self.backend {
            CacheBackend::K8s(k) => Some(k.pending_rdb()),
            CacheBackend::Docker(_) => None,
        }
    }

    /// Address fakecloud advertises for clients to reach a spawned cache
    /// container, and uses for readiness probes. `127.0.0.1` on the host;
    /// `host.docker.internal` when fakecloud is containerized (issue
    /// #1539, bug 0.4). Only meaningful for the Docker backend (k8s
    /// addresses are per-Pod and returned from `ensure_*`).
    pub fn endpoint_host(&self) -> &str {
        match &self.backend {
            CacheBackend::Docker(d) => &d.net.sibling_host,
            CacheBackend::K8s(_) => "127.0.0.1",
        }
    }

    pub async fn ensure_redis(
        &self,
        resource_id: &str,
        rdb_path: Option<&str>,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        let running = match &self.backend {
            CacheBackend::Docker(d) => {
                d.spawn_container(resource_id, CacheEngineKind::Redis, rdb_path)
                    .await?
            }
            CacheBackend::K8s(k) => {
                k.spawn_pod(resource_id, CacheEngineKind::Redis, rdb_path)
                    .await?
            }
        };
        self.containers
            .write()
            .insert(resource_id.to_string(), running.clone());
        Ok(running)
    }

    pub async fn ensure_memcached(
        &self,
        resource_id: &str,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        let running = match &self.backend {
            CacheBackend::Docker(d) => {
                d.spawn_container(resource_id, CacheEngineKind::Memcached, None)
                    .await?
            }
            CacheBackend::K8s(k) => {
                k.spawn_pod(resource_id, CacheEngineKind::Memcached, None)
                    .await?
            }
        };
        self.containers
            .write()
            .insert(resource_id.to_string(), running.clone());
        Ok(running)
    }

    pub async fn stop_container(&self, resource_id: &str) {
        let container = self.containers.write().remove(resource_id);
        if let Some(container) = container {
            match &self.backend {
                CacheBackend::Docker(d) => d.remove_container(&container.container_id).await,
                CacheBackend::K8s(k) => k.delete_pod(&container.container_id).await,
            }
        }
    }

    /// Restart the underlying backing instance, mirroring real
    /// ElastiCache's RebootCacheCluster behaviour. Returns `Unavailable`
    /// if the resource has no live instance tracked here.
    pub async fn restart_container(&self, resource_id: &str) -> Result<(), RuntimeError> {
        let running = {
            let containers = self.containers.read();
            containers.get(resource_id).cloned()
        };
        let running = running.ok_or(RuntimeError::Unavailable)?;
        match &self.backend {
            CacheBackend::Docker(d) => d.restart_container(&running.container_id).await,
            CacheBackend::K8s(k) => {
                // A Pod can't be restarted in place; recreate it,
                // preserving Redis data by snapshotting it across the
                // recreate. The new Pod keeps the same deterministic name.
                let updated = k.reboot_pod(resource_id, &running).await?;
                self.containers
                    .write()
                    .insert(resource_id.to_string(), updated);
                Ok(())
            }
        }
    }

    /// Execute a `redis-cli` command inside a tracked instance.
    pub async fn exec_redis(
        &self,
        resource_id: &str,
        redis_args: &[String],
    ) -> Result<CacheExec, RuntimeError> {
        let container_id = {
            let containers = self.containers.read();
            containers
                .get(resource_id)
                .map(|c| c.container_id.clone())
                .ok_or(RuntimeError::Unavailable)?
        };
        match &self.backend {
            CacheBackend::Docker(d) => d.exec_redis(&container_id, redis_args).await,
            CacheBackend::K8s(k) => k.exec_redis(&container_id, redis_args).await,
        }
    }

    /// Trigger `SAVE` inside a running Redis instance and copy the
    /// resulting `dump.rdb` out to `dest_path`.
    pub async fn dump_rdb(&self, resource_id: &str, dest_path: &str) -> Result<(), RuntimeError> {
        let container_id = {
            let containers = self.containers.read();
            containers
                .get(resource_id)
                .map(|c| c.container_id.clone())
                .ok_or(RuntimeError::Unavailable)?
        };
        match &self.backend {
            CacheBackend::Docker(d) => d.dump_rdb(&container_id, dest_path).await,
            CacheBackend::K8s(k) => k.dump_rdb(&container_id, dest_path).await,
        }
    }

    pub async fn stop_all(&self) {
        let containers: Vec<RunningCacheContainer> = {
            let mut containers = self.containers.write();
            containers.drain().map(|(_, c)| c).collect()
        };
        for c in containers {
            match &self.backend {
                CacheBackend::Docker(d) => d.remove_container(&c.container_id).await,
                CacheBackend::K8s(k) => k.delete_pod(&c.container_id).await,
            }
        }
    }

    /// Sweep cache Pods orphaned by a previous fakecloud process (k8s
    /// only; the Docker backend relies on the shared reaper).
    pub async fn reap_stale(&self) {
        if let CacheBackend::K8s(k) = &self.backend {
            k.reap_stale().await;
        }
    }
}

/// Docker/Podman backend: shells out to the container CLI, exactly as
/// ElastiCache always has.
#[derive(Debug, Clone)]
struct DockerCache {
    cli: String,
    net: fakecloud_core::container_net::HostNetworking,
    instance_id: String,
}

impl DockerCache {
    async fn spawn_container(
        &self,
        resource_id: &str,
        engine: CacheEngineKind,
        rdb_path: Option<&str>,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        let image = engine.image();
        let container_port = engine.port();

        let args: Vec<String> = vec![
            "create".to_string(),
            "-p".to_string(),
            format!(":{container_port}"),
            "--label".to_string(),
            format!("fakecloud-elasticache={resource_id}"),
            "--label".to_string(),
            format!("fakecloud-instance={}", self.instance_id),
            image.to_string(),
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

        // Stage the snapshot RDB into the created (not yet started)
        // container via `docker cp` rather than a `-v` bind mount. A bind
        // mount of a host path breaks when fakecloud runs in a container
        // (`FAKECLOUD_IN_CONTAINER=1`): the rdb is written inside
        // fakecloud's own filesystem, but the host daemon resolves the bind
        // source against the *host* filesystem, silently yielding an empty
        // cache. `docker cp` copies the bytes across the daemon, so it works
        // on host and in-container alike (issue #1539, bug 0.7). Redis loads
        // /data/dump.rdb at startup, so the copy must precede `start`.
        if let Some(path) = rdb_path {
            let cp_result = tokio::process::Command::new(&self.cli)
                .args(["cp", path, &format!("{container_id}:/data/dump.rdb")])
                .output()
                .await
                .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
            if !cp_result.status.success() {
                self.remove_container(&container_id).await;
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "failed to stage snapshot rdb into container: {}",
                    String::from_utf8_lossy(&cp_result.stderr).trim()
                )));
            }
        }

        let start_result = tokio::process::Command::new(&self.cli)
            .args(["start", &container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !start_result.status.success() {
            self.remove_container(&container_id).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "container start failed: {}",
                String::from_utf8_lossy(&start_result.stderr).trim()
            )));
        }

        let host_port = match self.lookup_port(&container_id, container_port).await {
            Ok(host_port) => host_port,
            Err(error) => {
                self.remove_container(&container_id).await;
                return Err(error);
            }
        };

        let wait_result = match engine {
            CacheEngineKind::Redis => self.wait_for_redis(host_port).await,
            CacheEngineKind::Memcached => self.wait_for_memcached(host_port).await,
        };
        if let Err(error) = wait_result {
            self.remove_container(&container_id).await;
            return Err(error);
        }

        Ok(RunningCacheContainer {
            container_id,
            host_port,
            // sibling_host is 127.0.0.1 on the host (CI, unit tests) and
            // host.docker.internal when fakecloud itself is containerized
            // (issue #1539) — the address a client actually reaches the
            // published port at.
            endpoint_address: self.net.sibling_host.clone(),
            endpoint_port: host_port,
            engine,
        })
    }

    async fn restart_container(&self, container_id: &str) -> Result<(), RuntimeError> {
        let output = tokio::process::Command::new(&self.cli)
            .args(["restart", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ));
        }
        Ok(())
    }

    async fn exec_redis(
        &self,
        container_id: &str,
        redis_args: &[String],
    ) -> Result<CacheExec, RuntimeError> {
        let mut args = vec![
            "exec".to_string(),
            container_id.to_string(),
            "redis-cli".to_string(),
        ];
        args.extend_from_slice(redis_args);
        let out = tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        Ok(CacheExec {
            success: out.status.success(),
            stdout: out.stdout,
            stderr: out.stderr,
        })
    }

    async fn dump_rdb(&self, container_id: &str, dest_path: &str) -> Result<(), RuntimeError> {
        let save_output = tokio::process::Command::new(&self.cli)
            .args(["exec", container_id, "redis-cli", "SAVE"])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !save_output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&save_output.stderr)
                    .trim()
                    .to_string(),
            ));
        }

        let cp_output = tokio::process::Command::new(&self.cli)
            .args(["cp", &format!("{container_id}:/data/dump.rdb"), dest_path])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !cp_output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&cp_output.stderr)
                    .trim()
                    .to_string(),
            ));
        }
        Ok(())
    }

    async fn lookup_port(
        &self,
        container_id: &str,
        container_port: u16,
    ) -> Result<u16, RuntimeError> {
        let port_output = tokio::process::Command::new(&self.cli)
            .args(["port", container_id, &container_port.to_string()])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !port_output.status.success() {
            let stderr = String::from_utf8_lossy(&port_output.stderr);
            return Err(RuntimeError::ContainerStartFailed(format!(
                "port lookup failed: {stderr}"
            )));
        }

        let port_str = String::from_utf8_lossy(&port_output.stdout);
        port_str
            .trim()
            .rsplit(':')
            .next()
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(|| {
                RuntimeError::ContainerStartFailed(format!(
                    "could not determine redis port from '{}'",
                    port_str.trim()
                ))
            })
    }

    async fn wait_for_redis(&self, host_port: u16) -> Result<(), RuntimeError> {
        // Probe the same address clients reach the published port at:
        // 127.0.0.1 on the host, host.docker.internal /
        // host.containers.internal when fakecloud is containerized (#1539).
        let host = &self.net.sibling_host;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if tokio::net::TcpStream::connect(format!("{host}:{host_port}"))
                .await
                .is_ok()
            {
                return Ok(());
            }
        }

        Err(RuntimeError::ContainerStartFailed(
            "redis container did not become ready within 20 seconds".to_string(),
        ))
    }

    async fn wait_for_memcached(&self, host_port: u16) -> Result<(), RuntimeError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let host = &self.net.sibling_host;
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let Ok(mut stream) =
                tokio::net::TcpStream::connect(format!("{host}:{host_port}")).await
            else {
                continue;
            };
            if stream.write_all(b"version\r\n").await.is_err() {
                continue;
            }
            let mut buf = [0u8; 32];
            match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 && buf.starts_with(b"VERSION") => return Ok(()),
                _ => continue,
            }
        }

        Err(RuntimeError::ContainerStartFailed(
            "memcached container did not become ready within 20 seconds".to_string(),
        ))
    }

    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }
}
