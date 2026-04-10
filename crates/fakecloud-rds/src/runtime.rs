use std::collections::HashMap;
use std::time::Duration;

use parking_lot::RwLock;
use tokio_postgres::NoTls;

#[derive(Debug, Clone)]
pub struct RunningDbContainer {
    pub container_id: String,
    pub host_port: u16,
}

pub struct RdsRuntime {
    cli: String,
    containers: RwLock<HashMap<String, RunningDbContainer>>,
    instance_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container runtime is unavailable")]
    Unavailable,
    #[error("container failed to start: {0}")]
    ContainerStartFailed(String),
}

impl RdsRuntime {
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

    pub async fn ensure_postgres(
        &self,
        db_instance_identifier: &str,
        username: &str,
        password: &str,
        db_name: &str,
    ) -> Result<RunningDbContainer, RuntimeError> {
        self.stop_container(db_instance_identifier).await;

        let output = tokio::process::Command::new(&self.cli)
            .args([
                "create",
                "-p",
                ":5432",
                "--label",
                &format!("fakecloud-rds={db_instance_identifier}"),
                "--label",
                &format!("fakecloud-instance={}", self.instance_id),
                "-e",
                &format!("POSTGRES_USER={username}"),
                "-e",
                &format!("POSTGRES_PASSWORD={password}"),
                "-e",
                &format!("POSTGRES_DB={db_name}"),
                "postgres:16-alpine",
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

        let host_port = match self.lookup_port(&container_id).await {
            Ok(host_port) => host_port,
            Err(error) => {
                self.remove_container(&container_id).await;
                return Err(error);
            }
        };
        if let Err(error) = self
            .wait_for_postgres(username, password, db_name, host_port)
            .await
        {
            self.remove_container(&container_id).await;
            return Err(error);
        }

        let running = RunningDbContainer {
            container_id,
            host_port,
        };
        self.containers
            .write()
            .insert(db_instance_identifier.to_string(), running.clone());
        Ok(running)
    }

    pub async fn stop_container(&self, db_instance_identifier: &str) {
        let container = self.containers.write().remove(db_instance_identifier);
        if let Some(container) = container {
            self.remove_container(&container.container_id).await;
        }
    }

    pub async fn restart_container(
        &self,
        db_instance_identifier: &str,
        username: &str,
        password: &str,
        db_name: &str,
    ) -> Result<RunningDbContainer, RuntimeError> {
        let running = self
            .containers
            .read()
            .get(db_instance_identifier)
            .cloned()
            .ok_or(RuntimeError::Unavailable)?;

        let output = tokio::process::Command::new(&self.cli)
            .args(["restart", &running.container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "container restart failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        let host_port = self.lookup_port(&running.container_id).await?;
        self.wait_for_postgres(username, password, db_name, host_port)
            .await?;
        let running = RunningDbContainer {
            container_id: running.container_id,
            host_port,
        };
        self.containers
            .write()
            .insert(db_instance_identifier.to_string(), running.clone());
        Ok(running)
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

    async fn lookup_port(&self, container_id: &str) -> Result<u16, RuntimeError> {
        let port_output = tokio::process::Command::new(&self.cli)
            .args(["port", container_id, "5432"])
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
                    "could not determine postgres port from '{}'",
                    port_str.trim()
                ))
            })
    }

    async fn wait_for_postgres(
        &self,
        username: &str,
        password: &str,
        db_name: &str,
        host_port: u16,
    ) -> Result<(), RuntimeError> {
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let connection_string = format!(
                "host=127.0.0.1 port={host_port} user={username} password={password} dbname={db_name}"
            );
            if let Ok((client, connection)) =
                tokio_postgres::connect(&connection_string, NoTls).await
            {
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                if client.simple_query("SELECT 1").await.is_ok() {
                    return Ok(());
                }
            }
        }

        Err(RuntimeError::ContainerStartFailed(
            "postgres container did not become ready within 20 seconds".to_string(),
        ))
    }

    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }

    pub async fn dump_database(
        &self,
        db_instance_identifier: &str,
        username: &str,
        db_name: &str,
    ) -> Result<Vec<u8>, RuntimeError> {
        let container = self
            .containers
            .read()
            .get(db_instance_identifier)
            .cloned()
            .ok_or(RuntimeError::Unavailable)?;

        let output = tokio::process::Command::new(&self.cli)
            .args([
                "exec",
                &container.container_id,
                "pg_dump",
                "-U",
                username,
                "-d",
                db_name,
                "--no-password",
            ])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "pg_dump failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        Ok(output.stdout)
    }

    pub async fn restore_database(
        &self,
        db_instance_identifier: &str,
        username: &str,
        db_name: &str,
        dump_data: &[u8],
    ) -> Result<(), RuntimeError> {
        let container = self
            .containers
            .read()
            .get(db_instance_identifier)
            .cloned()
            .ok_or(RuntimeError::Unavailable)?;

        let mut child = tokio::process::Command::new(&self.cli)
            .args([
                "exec",
                "-i",
                &container.container_id,
                "psql",
                "-U",
                username,
                "-d",
                db_name,
                "--no-password",
                "-v",
                "ON_ERROR_STOP=1",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if let Some(mut stdin) = child.stdin.take() {
            use tokio::io::AsyncWriteExt;
            stdin
                .write_all(dump_data)
                .await
                .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
            drop(stdin);
        }

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "psql restore failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }

        Ok(())
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
