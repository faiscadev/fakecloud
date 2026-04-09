use std::collections::HashMap;
use std::time::Duration;

use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub struct RunningCacheContainer {
    pub container_id: String,
    pub host_port: u16,
}

pub struct ElastiCacheRuntime {
    cli: String,
    containers: RwLock<HashMap<String, RunningCacheContainer>>,
    instance_id: String,
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
        let cli = if let Ok(cli) = std::env::var("FAKECLOUD_CONTAINER_CLI") {
            if cli_available(&cli) {
                cli
            } else {
                return None;
            }
        } else if cli_available("docker") {
            "docker".to_string()
        } else if cli_available("podman") {
            "podman".to_string()
        } else {
            return None;
        };

        Some(Self {
            cli,
            containers: RwLock::new(HashMap::new()),
            instance_id: format!("fakecloud-{}", std::process::id()),
        })
    }

    pub fn cli_name(&self) -> &str {
        &self.cli
    }

    pub async fn ensure_redis(
        &self,
        replication_group_id: &str,
        port: u16,
    ) -> Result<RunningCacheContainer, RuntimeError> {
        self.stop_container(replication_group_id).await;

        let container_port_mapping = format!(":{port}");
        let output = tokio::process::Command::new(&self.cli)
            .args([
                "create",
                "-p",
                &container_port_mapping,
                "--label",
                &format!("fakecloud-elasticache={replication_group_id}"),
                "--label",
                &format!("fakecloud-instance={}", self.instance_id),
                "redis:7-alpine",
            ])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&output.stderr).trim().to_string(),
            ));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();
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

        let host_port = match self.lookup_port(&container_id, port).await {
            Ok(host_port) => host_port,
            Err(error) => {
                self.remove_container(&container_id).await;
                return Err(error);
            }
        };

        if let Err(error) = self.wait_for_redis(host_port).await {
            self.remove_container(&container_id).await;
            return Err(error);
        }

        let running = RunningCacheContainer {
            container_id,
            host_port,
        };
        self.containers
            .write()
            .insert(replication_group_id.to_string(), running.clone());
        Ok(running)
    }

    pub async fn stop_container(&self, replication_group_id: &str) {
        let container = self.containers.write().remove(replication_group_id);
        if let Some(container) = container {
            self.remove_container(&container.container_id).await;
        }
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

    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }
}

fn cli_available(cli: &str) -> bool {
    std::process::Command::new(cli)
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
