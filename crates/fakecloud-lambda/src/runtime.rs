use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tempfile::TempDir;

use crate::state::LambdaFunction;

/// A running container kept warm for reuse.
struct WarmContainer {
    container_id: String,
    host_port: u16,
    last_used: RwLock<Instant>,
    code_sha256: String,
}

/// Docker/Podman-based Lambda execution engine.
pub struct ContainerRuntime {
    cli: String,
    containers: RwLock<HashMap<String, WarmContainer>>,
    /// Serializes container startup per function to prevent duplicate containers.
    starting: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    instance_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("no code ZIP provided for function {0}")]
    NoCodeZip(String),
    #[error("unsupported runtime: {0}")]
    UnsupportedRuntime(String),
    #[error("container failed to start: {0}")]
    ContainerStartFailed(String),
    #[error("invocation failed: {0}")]
    InvocationFailed(String),
    #[error("ZIP extraction failed: {0}")]
    ZipExtractionFailed(String),
}

impl ContainerRuntime {
    /// Auto-detect Docker or Podman. Returns `None` if neither is available.
    /// Override with `FAKECLOUD_CONTAINER_CLI` env var.
    pub fn new() -> Option<Self> {
        let cli = if let Ok(cli) = std::env::var("FAKECLOUD_CONTAINER_CLI") {
            // Verify the configured CLI works
            if std::process::Command::new(&cli)
                .arg("info")
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
            {
                cli
            } else {
                return None;
            }
        } else if is_cli_available("docker") {
            "docker".to_string()
        } else if is_cli_available("podman") {
            "podman".to_string()
        } else {
            return None;
        };

        let instance_id = format!("fakecloud-{}", std::process::id());

        Some(Self {
            cli,
            containers: RwLock::new(HashMap::new()),
            starting: RwLock::new(HashMap::new()),
            instance_id,
        })
    }

    pub fn cli_name(&self) -> &str {
        &self.cli
    }

    /// Invoke a Lambda function, starting a container if needed.
    pub async fn invoke(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
    ) -> Result<Vec<u8>, RuntimeError> {
        let zip_bytes = func
            .code_zip
            .as_ref()
            .ok_or_else(|| RuntimeError::NoCodeZip(func.function_name.clone()))?;

        // Check for warm container with matching code
        let port = {
            let containers = self.containers.read();
            if let Some(container) = containers.get(&func.function_name) {
                if container.code_sha256 == func.code_sha256 {
                    *container.last_used.write() = Instant::now();
                    Some(container.host_port)
                } else {
                    None
                }
            } else {
                None
            }
        };

        let port = match port {
            Some(p) => p,
            None => {
                // Serialize container startup per function to prevent duplicates
                let startup_lock = {
                    let mut starting = self.starting.write();
                    starting
                        .entry(func.function_name.clone())
                        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                        .clone()
                };
                let _guard = startup_lock.lock().await;

                // Re-check after acquiring lock — another task may have started it
                let existing_port = {
                    let containers = self.containers.read();
                    containers
                        .get(&func.function_name)
                        .filter(|c| c.code_sha256 == func.code_sha256)
                        .map(|c| {
                            *c.last_used.write() = Instant::now();
                            c.host_port
                        })
                };
                if let Some(p) = existing_port {
                    p
                } else {
                    self.stop_container(&func.function_name).await;
                    let container = self.start_container(func, zip_bytes).await?;
                    let p = container.host_port;
                    self.containers
                        .write()
                        .insert(func.function_name.clone(), container);
                    p
                }
            }
        };

        // POST to the RIE endpoint
        let url = format!(
            "http://localhost:{}/2015-03-31/functions/function/invocations",
            port
        );
        let client = reqwest::Client::new();
        let resp = client
            .post(&url)
            .body(payload.to_vec())
            .timeout(Duration::from_secs(func.timeout as u64 + 5))
            .send()
            .await
            .map_err(|e| RuntimeError::InvocationFailed(e.to_string()))?;

        let body = resp
            .bytes()
            .await
            .map_err(|e| RuntimeError::InvocationFailed(e.to_string()))?;

        Ok(body.to_vec())
    }

    async fn start_container(
        &self,
        func: &LambdaFunction,
        zip_bytes: &[u8],
    ) -> Result<WarmContainer, RuntimeError> {
        let image = runtime_to_image(&func.runtime)
            .ok_or_else(|| RuntimeError::UnsupportedRuntime(func.runtime.clone()))?;

        // Extract ZIP to a temp directory (only needed during container setup)
        let code_dir =
            TempDir::new().map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;
        extract_zip(zip_bytes, code_dir.path())?;

        // Step 1: docker create (no volume mounts — works in Docker-in-Docker)
        let mut cmd = tokio::process::Command::new(&self.cli);
        cmd.arg("create")
            .arg("-p")
            .arg(":8080")
            .arg("--label")
            .arg(format!("fakecloud-lambda={}", func.function_name))
            .arg("--label")
            .arg(format!("fakecloud-instance={}", self.instance_id));

        for (key, value) in &func.environment {
            cmd.arg("-e").arg(format!("{}={}", key, value));
        }

        cmd.arg("-e")
            .arg(format!("AWS_LAMBDA_FUNCTION_TIMEOUT={}", func.timeout));

        cmd.arg(&image).arg(&func.handler);

        let output = cmd
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(RuntimeError::ContainerStartFailed(stderr.to_string()));
        }

        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();

        // Step 2: docker cp — copy code into the container
        let cp_result = tokio::process::Command::new(&self.cli)
            .arg("cp")
            .arg(format!("{}/.", code_dir.path().display()))
            .arg(format!("{}:/var/task", container_id))
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !cp_result.status.success() {
            let _ = self.remove_container(&container_id).await;
            let stderr = String::from_utf8_lossy(&cp_result.stderr);
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker cp failed: {}",
                stderr
            )));
        }

        // For provided/custom runtimes, also copy to /var/runtime
        if func.runtime.starts_with("provided") {
            let cp_runtime = tokio::process::Command::new(&self.cli)
                .arg("cp")
                .arg(format!("{}/.", code_dir.path().display()))
                .arg(format!("{}:/var/runtime", container_id))
                .output()
                .await
                .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

            if !cp_runtime.status.success() {
                let _ = self.remove_container(&container_id).await;
                let stderr = String::from_utf8_lossy(&cp_runtime.stderr);
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "docker cp to /var/runtime failed: {}",
                    stderr
                )));
            }
        }

        // TempDir is dropped here — code now lives inside the container

        // Step 3: docker start
        let start_result = tokio::process::Command::new(&self.cli)
            .args(["start", &container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !start_result.status.success() {
            let _ = self.remove_container(&container_id).await;
            let stderr = String::from_utf8_lossy(&start_result.stderr);
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker start failed: {}",
                stderr
            )));
        }

        // Query the actual assigned port
        let port_output = tokio::process::Command::new(&self.cli)
            .args(["port", &container_id, "8080"])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        let port_str = String::from_utf8_lossy(&port_output.stdout);
        let port: u16 = port_str
            .trim()
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(|| {
                RuntimeError::ContainerStartFailed(format!(
                    "could not determine port from: {}",
                    port_str.trim()
                ))
            })?;

        // Wait for RIE to start accepting connections
        let mut ready = false;
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
                .await
                .is_ok()
            {
                ready = true;
                break;
            }
        }

        if !ready {
            let _ = self.remove_container(&container_id).await;
            return Err(RuntimeError::ContainerStartFailed(
                "container did not become ready within 10 seconds".to_string(),
            ));
        }

        tracing::info!(
            function = %func.function_name,
            container_id = %container_id,
            port = port,
            runtime = %func.runtime,
            "Lambda container started"
        );

        Ok(WarmContainer {
            container_id,
            host_port: port,
            last_used: RwLock::new(Instant::now()),
            code_sha256: func.code_sha256.clone(),
        })
    }

    /// Remove a container (stop + rm, since we don't use --rm with docker create).
    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }

    /// Stop and remove a container for a specific function.
    pub async fn stop_container(&self, function_name: &str) {
        let container = self.containers.write().remove(function_name);
        if let Some(container) = container {
            tracing::info!(
                function = %function_name,
                container_id = %container.container_id,
                "stopping Lambda container"
            );
            self.remove_container(&container.container_id).await;
        }
    }

    /// Stop and remove all containers (used on server shutdown or reset).
    pub async fn stop_all(&self) {
        let containers: Vec<(String, String)> = {
            let mut map = self.containers.write();
            map.drain()
                .map(|(name, c)| (name, c.container_id))
                .collect()
        };
        for (name, container_id) in containers {
            tracing::info!(
                function = %name,
                container_id = %container_id,
                "stopping Lambda container (cleanup)"
            );
            self.remove_container(&container_id).await;
        }
    }

    /// List all warm containers and their metadata for introspection.
    pub fn list_warm_containers(
        &self,
        lambda_state: &crate::state::SharedLambdaState,
    ) -> Vec<serde_json::Value> {
        let containers = self.containers.read();
        let state = lambda_state.read();
        containers
            .iter()
            .map(|(name, container)| {
                let runtime = state
                    .functions
                    .get(name)
                    .map(|f| f.runtime.clone())
                    .unwrap_or_default();
                let last_used = container.last_used.read();
                let idle_secs = last_used.elapsed().as_secs();
                serde_json::json!({
                    "functionName": name,
                    "runtime": runtime,
                    "containerId": container.container_id,
                    "lastUsedSecsAgo": idle_secs,
                })
            })
            .collect()
    }

    /// Evict (stop and remove) the warm container for a specific function.
    /// Returns true if a container was found and evicted.
    pub async fn evict_container(&self, function_name: &str) -> bool {
        let container = self.containers.write().remove(function_name);
        if let Some(container) = container {
            tracing::info!(
                function = %function_name,
                container_id = %container.container_id,
                "evicting Lambda container via simulation API"
            );
            self.remove_container(&container.container_id).await;
            true
        } else {
            false
        }
    }

    /// Background loop that stops containers idle longer than `ttl`.
    pub async fn run_cleanup_loop(self: Arc<Self>, ttl: Duration) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            self.cleanup_idle(ttl).await;
        }
    }

    async fn cleanup_idle(&self, ttl: Duration) {
        let expired: Vec<String> = {
            let containers = self.containers.read();
            containers
                .iter()
                .filter(|(_, c)| c.last_used.read().elapsed() > ttl)
                .map(|(name, _)| name.clone())
                .collect()
        };
        for name in expired {
            tracing::info!(function = %name, "stopping idle Lambda container");
            self.stop_container(&name).await;
        }
    }
}

/// Map AWS runtime identifier to a Docker image tag.
pub fn runtime_to_image(runtime: &str) -> Option<String> {
    let (base, tag) = match runtime {
        "python3.13" => ("python", "3.13"),
        "python3.12" => ("python", "3.12"),
        "python3.11" => ("python", "3.11"),
        "nodejs22.x" => ("nodejs", "22"),
        "nodejs20.x" => ("nodejs", "20"),
        "nodejs18.x" => ("nodejs", "18"),
        "ruby3.4" => ("ruby", "3.4"),
        "ruby3.3" => ("ruby", "3.3"),
        "java21" => ("java", "21"),
        "java17" => ("java", "17"),
        "dotnet8" => ("dotnet", "8"),
        "provided.al2023" => ("provided", "al2023"),
        "provided.al2" => ("provided", "al2"),
        _ => return None,
    };
    Some(format!("public.ecr.aws/lambda/{}:{}", base, tag))
}

/// Extract a ZIP archive to a destination directory.
pub fn extract_zip(zip_bytes: &[u8], dest: &Path) -> Result<(), RuntimeError> {
    let cursor = std::io::Cursor::new(zip_bytes);
    let mut archive = zip::ZipArchive::new(cursor)
        .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;

        let out_path = dest.join(file.enclosed_name().ok_or_else(|| {
            RuntimeError::ZipExtractionFailed("invalid file name in ZIP".to_string())
        })?);

        if file.is_dir() {
            std::fs::create_dir_all(&out_path)
                .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;
            }
            let mut out_file = std::fs::File::create(&out_path)
                .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;
            std::io::copy(&mut file, &mut out_file)
                .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;

            // Preserve executable permissions
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Some(mode) = file.unix_mode() {
                    std::fs::set_permissions(&out_path, std::fs::Permissions::from_mode(mode))
                        .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

fn is_cli_available(name: &str) -> bool {
    std::process::Command::new(name)
        .arg("info")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;

    #[test]
    fn test_runtime_to_image() {
        assert_eq!(
            runtime_to_image("python3.12"),
            Some("public.ecr.aws/lambda/python:3.12".to_string())
        );
        assert_eq!(
            runtime_to_image("nodejs20.x"),
            Some("public.ecr.aws/lambda/nodejs:20".to_string())
        );
        assert_eq!(
            runtime_to_image("provided.al2023"),
            Some("public.ecr.aws/lambda/provided:al2023".to_string())
        );
        assert_eq!(
            runtime_to_image("ruby3.4"),
            Some("public.ecr.aws/lambda/ruby:3.4".to_string())
        );
        assert_eq!(
            runtime_to_image("java21"),
            Some("public.ecr.aws/lambda/java:21".to_string())
        );
        assert_eq!(
            runtime_to_image("dotnet8"),
            Some("public.ecr.aws/lambda/dotnet:8".to_string())
        );
        assert_eq!(runtime_to_image("unknown"), None);
    }

    #[test]
    fn test_extract_zip() {
        // Create a minimal ZIP in memory
        let buf = Vec::new();
        let cursor = std::io::Cursor::new(buf);
        let mut writer = zip::ZipWriter::new(cursor);
        let options = zip::write::SimpleFileOptions::default();
        writer.start_file("handler.py", options).unwrap();
        writer
            .write_all(b"def handler(event, context):\n    return {'statusCode': 200}\n")
            .unwrap();
        let cursor = writer.finish().unwrap();
        let zip_bytes = cursor.into_inner();

        let dir = TempDir::new().unwrap();
        extract_zip(&zip_bytes, dir.path()).unwrap();

        let handler_path = dir.path().join("handler.py");
        assert!(handler_path.exists());

        let mut content = String::new();
        std::fs::File::open(&handler_path)
            .unwrap()
            .read_to_string(&mut content)
            .unwrap();
        assert!(content.contains("def handler"));
    }
}
