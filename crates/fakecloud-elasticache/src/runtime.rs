use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct RunningCacheContainer {
    pub container_id: String,
    pub host_port: u16,
}

#[derive(Debug, Clone, Copy)]
enum CacheEngineKind {
    Redis,
    Memcached,
}

#[derive(Debug, Clone)]
pub struct ElastiCacheRuntime {
    cli: String,
    containers: Arc<RwLock<HashMap<String, RunningCacheContainer>>>,
    instance_id: String,
    /// Container-to-host networking shared with the other runtimes via
    /// [`fakecloud_core::container_net`]. ElastiCache only needs the
    /// sibling-container address (redis/memcached don't call back into
    /// fakecloud) — used so readiness probes reach the spawned container
    /// instead of fakecloud's own loopback when containerized (issue
    /// #1539, bug 0.4).
    net: fakecloud_core::container_net::HostNetworking,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container runtime is unavailable")]
    Unavailable,
    #[error("container failed to start: {0}")]
    ContainerStartFailed(String),
}

impl ElastiCacheRuntime {
    pub fn new() -> Option<Self> {
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        let net = fakecloud_core::container_net::HostNetworking::detect(&cli);
        Some(Self {
            cli,
            containers: Arc::new(RwLock::new(HashMap::new())),
            instance_id: format!("fakecloud-{}", std::process::id()),
            net,
        })
    }

    pub fn cli_name(&self) -> &str {
        &self.cli
    }

    /// Address fakecloud advertises for clients to reach a spawned cache
    /// container, and uses for readiness probes. `127.0.0.1` on the host;
    /// `host.docker.internal` when fakecloud is containerized
    /// (`FAKECLOUD_IN_CONTAINER=1`) (issue #1539, bug 0.4).
    pub fn endpoint_host(&self) -> &str {
        &self.net.sibling_host
    }

    pub async fn ensure_redis(
        &self,
        resource_id: &str,
        rdb_path: Option<&str>,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        self.spawn_container(
            resource_id,
            "redis:7-alpine",
            6379,
            CacheEngineKind::Redis,
            rdb_path,
        )
        .await
    }

    pub async fn ensure_memcached(
        &self,
        resource_id: &str,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        self.spawn_container(
            resource_id,
            "memcached:1.6-alpine",
            11211,
            CacheEngineKind::Memcached,
            None,
        )
        .await
    }

    async fn spawn_container(
        &self,
        resource_id: &str,
        image: &str,
        container_port: u16,
        engine: CacheEngineKind,
        rdb_path: Option<&str>,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        self.stop_container(resource_id).await;

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

        let running = RunningCacheContainer {
            container_id,
            host_port,
        };
        self.containers
            .write()
            .insert(resource_id.to_string(), running.clone());
        Ok(running)
    }

    pub async fn stop_container(&self, resource_id: &str) {
        let container = self.containers.write().remove(resource_id);
        if let Some(container) = container {
            self.remove_container(&container.container_id).await;
        }
    }

    /// Restart the underlying container (`docker restart`) for an
    /// ElastiCache resource, mirroring real ElastiCache's
    /// RebootCacheCluster behaviour. Returns `Unavailable` if the
    /// resource has no live container tracked here. Any failure from
    /// the CLI surfaces as `ContainerStartFailed` so the caller can
    /// translate it to an AWS error.
    pub async fn restart_container(&self, resource_id: &str) -> Result<(), RuntimeError> {
        let container_id = {
            let containers = self.containers.read();
            containers
                .get(resource_id)
                .map(|c| c.container_id.clone())
                .ok_or(RuntimeError::Unavailable)?
        };
        let output = tokio::process::Command::new(&self.cli)
            .args(["restart", &container_id])
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

    /// Execute an arbitrary `redis-cli` command inside a tracked container.
    /// Returns the raw `std::process::Output` so the caller can decide
    /// whether a non-zero exit is fatal.
    pub async fn exec_redis(
        &self,
        resource_id: &str,
        redis_args: &[String],
    ) -> Result<std::process::Output, RuntimeError> {
        let container_id = {
            let containers = self.containers.read();
            containers
                .get(resource_id)
                .map(|c| c.container_id.clone())
                .ok_or(RuntimeError::Unavailable)?
        };
        let mut args = vec!["exec".to_string(), container_id, "redis-cli".to_string()];
        args.extend_from_slice(redis_args);
        tokio::process::Command::new(&self.cli)
            .args(&args)
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))
    }

    /// Trigger `SAVE` inside a running Redis container and copy the
    /// resulting `dump.rdb` out to `dest_path`.
    pub async fn dump_rdb(&self, resource_id: &str, dest_path: &str) -> Result<(), RuntimeError> {
        let container_id = {
            let containers = self.containers.read();
            containers
                .get(resource_id)
                .map(|c| c.container_id.clone())
                .ok_or(RuntimeError::Unavailable)?
        };

        let save_output = tokio::process::Command::new(&self.cli)
            .args(["exec", &container_id, "redis-cli", "SAVE"])
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

    pub async fn stop_all(&self) {
        let containers: Vec<String> = {
            let mut containers = self.containers.write();
            containers
                .drain()
                .map(|(_, container)| container.container_id)
                .collect()
        };
        for container_id in containers {
            self.remove_container(&container_id).await;
        }
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
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if tokio::net::TcpStream::connect(format!("127.0.0.1:{host_port}"))
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
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let Ok(mut stream) =
                tokio::net::TcpStream::connect(format!("127.0.0.1:{host_port}")).await
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
