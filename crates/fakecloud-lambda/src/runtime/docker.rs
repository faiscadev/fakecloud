//! Docker/Podman [`LambdaBackend`] implementation.
//!
//! Shells out to `docker` or `podman` CLI. Auto-detects which one is
//! available; honors `FAKECLOUD_CONTAINER_CLI` as an override.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use tempfile::TempDir;

use super::backend::{BackendHandle, LambdaBackend, RuntimeError, WarmInstance};
use super::env_rewrite::rewrite_localhost_envs;
use crate::state::LambdaFunction;

/// Docker/Podman-based Lambda execution backend.
pub struct DockerBackend {
    cli: String,
    instance_id: String,
    /// DNS name the container uses to reach fakecloud on the host. For
    /// docker we use the cross-platform `host.docker.internal` alias (and
    /// inject it via `--add-host host.docker.internal:host-gateway` on
    /// Mac/Windows; bridge gateway IP on Linux). For podman we use its
    /// built-in `host.containers.internal` alias, which podman injects
    /// automatically without an `--add-host` flag — passing `host-gateway`
    /// to podman on macOS fails with "host containers internal IP address
    /// is empty" because podman's gvproxy network doesn't populate the
    /// magic alias. See issue #1539.
    host_alias: String,
    /// `--add-host <alias>:<value>` argument injected into every container
    /// `create`, or `None` when the runtime provides the alias natively.
    add_host_arg: Option<String>,
    /// Port the main fakecloud server bound to. Used to translate AWS
    /// private-ECR URIs in `PackageType=Image` functions to fakecloud's
    /// local OCI v2 registry.
    server_port: u16,
    /// Isolated DOCKER_CONFIG dir with Basic auth for `127.0.0.1:<port>`.
    /// Lets `docker pull` talk to fakecloud ECR without mutating the user's
    /// `~/.docker/config.json`.
    docker_config: Option<Arc<TempDir>>,
}

impl DockerBackend {
    /// Auto-detect Docker or Podman. Returns `None` if neither is available.
    /// Override with `FAKECLOUD_CONTAINER_CLI` env var.
    /// `server_port` is the port the main fakecloud server bound to; used
    /// to resolve `PackageType=Image` ECR URIs against fakecloud ECR.
    pub fn auto_detect(server_port: u16) -> Option<Self> {
        let cli = if let Ok(cli) = std::env::var("FAKECLOUD_CONTAINER_CLI") {
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

        let (host_alias, add_host_arg) = if is_podman_binary(&cli) {
            // Podman ships `host.containers.internal` as a built-in container
            // DNS entry on every supported platform; injecting `host-gateway`
            // on macOS fails because rootless podman's gvproxy doesn't
            // expose the magic alias (issue #1539).
            ("host.containers.internal".to_string(), None)
        } else if cfg!(target_os = "linux") {
            // Bare docker on Linux: resolve the bridge gateway IP and add
            // an explicit alias. `host.docker.internal:host-gateway` only
            // works on Docker Desktop; native Linux docker has no such
            // magic.
            let ip = detect_bridge_gateway(&cli).unwrap_or_else(|| "172.17.0.1".to_string());
            (
                "host.docker.internal".to_string(),
                Some(format!("host.docker.internal:{ip}")),
            )
        } else {
            // Docker Desktop on Mac/Windows: `host-gateway` is a Docker
            // Desktop-only alias that resolves to the host's IP.
            (
                "host.docker.internal".to_string(),
                Some("host.docker.internal:host-gateway".to_string()),
            )
        };

        let docker_config = build_local_registry_docker_config(server_port).map(Arc::new);
        Some(Self {
            cli,
            instance_id,
            host_alias,
            add_host_arg,
            server_port,
            docker_config,
        })
    }

    /// Append `--add-host` arguments to `cmd` when the runtime needs an
    /// explicit host alias mapping (docker on Linux/Mac/Windows). No-op
    /// for podman, which provides `host.containers.internal` natively.
    fn apply_host_alias(&self, cmd: &mut tokio::process::Command) {
        if let Some(arg) = &self.add_host_arg {
            cmd.arg("--add-host").arg(arg);
        }
    }

    fn docker_config_path(&self) -> Option<PathBuf> {
        self.docker_config.as_ref().map(|d| d.path().to_path_buf())
    }

    /// Start a container for a `PackageType=Image` function. The image is
    /// expected to already embed the Runtime Interface Emulator (RIE) or
    /// an equivalent, exposing port 8080. AWS private-ECR URIs get
    /// translated to fakecloud's local OCI v2 registry and retagged so
    /// the container reports its user-visible image name.
    async fn start_image_container(
        &self,
        func: &LambdaFunction,
        layers: &[Vec<u8>],
    ) -> Result<WarmInstance, RuntimeError> {
        let image = func.image_uri.as_deref().ok_or_else(|| {
            RuntimeError::ContainerStartFailed("PackageType=Image function has no ImageUri".into())
        })?;

        let local_pull_uri = fakecloud_core::ecr_uri::translate_to_local(image, self.server_port);
        let pull_uri = local_pull_uri.as_deref().unwrap_or(image);

        let mut pull_cmd = tokio::process::Command::new(&self.cli);
        if let Some(p) = self.docker_config_path() {
            pull_cmd.env("DOCKER_CONFIG", p);
        }
        let pull_out = pull_cmd
            .args(["pull", pull_uri])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("docker pull: {e}")))?;
        if !pull_out.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker pull failed: {}",
                String::from_utf8_lossy(&pull_out.stderr)
            )));
        }
        // Retag the local pull URI to the AWS URI so `docker create`
        // finds the image under the user-visible name. Digest-pinned
        // refs can't be `docker tag` targets, so fall through and
        // create under the local URI instead.
        let run_image = if let Some(ref local_uri) = local_pull_uri {
            if fakecloud_core::ecr_uri::is_digest_ref(image) {
                local_uri.clone()
            } else {
                let _ = tokio::process::Command::new(&self.cli)
                    .args(["tag", local_uri, image])
                    .output()
                    .await;
                image.to_string()
            }
        } else {
            image.to_string()
        };

        let mut cmd = tokio::process::Command::new(&self.cli);
        cmd.arg("create")
            .arg("-p")
            .arg(":8080")
            .arg("--label")
            .arg(format!("fakecloud-lambda={}", func.function_name))
            .arg("--label")
            .arg(format!("fakecloud-instance={}", self.instance_id));
        self.apply_host_alias(&mut cmd);

        for (key, value) in rewrite_localhost_envs(&func.environment, &self.host_alias) {
            cmd.arg("-e").arg(format!("{key}={value}"));
        }
        cmd.arg("-e")
            .arg(format!("AWS_LAMBDA_FUNCTION_TIMEOUT={}", func.timeout));

        let tmpfs_arg = ephemeral_storage_tmpfs_arg(func.ephemeral_storage_size);
        cmd.arg("--tmpfs").arg(tmpfs_arg);

        cmd.arg(&run_image);

        let output = cmd
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }
        let container_id = String::from_utf8_lossy(&output.stdout).trim().to_string();

        if let Err(e) = self.copy_layers_into(&container_id, layers).await {
            self.remove_container(&container_id).await;
            return Err(e);
        }

        let start_result = tokio::process::Command::new(&self.cli)
            .args(["start", &container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !start_result.status.success() {
            self.remove_container(&container_id).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker start failed: {}",
                String::from_utf8_lossy(&start_result.stderr)
            )));
        }

        let port = self.query_host_port(&container_id).await?;
        self.wait_for_ready(&container_id, port).await?;

        tracing::info!(
            function = %func.function_name,
            container_id = %container_id,
            port = port,
            image = %image,
            "Lambda image container started"
        );

        Ok(WarmInstance {
            endpoint: format!("127.0.0.1:{port}"),
            handle: BackendHandle::Container { id: container_id },
        })
    }

    async fn start_zip_container(
        &self,
        func: &LambdaFunction,
        zip_bytes: &[u8],
        layers: &[Vec<u8>],
    ) -> Result<WarmInstance, RuntimeError> {
        let image = runtime_to_image(&func.runtime)
            .ok_or_else(|| RuntimeError::UnsupportedRuntime(func.runtime.clone()))?;

        // Extract ZIP to a temp directory (only needed during container setup).
        // Run in spawn_blocking to avoid blocking the async runtime with fs I/O.
        let code_dir =
            TempDir::new().map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;
        let zip_bytes = zip_bytes.to_vec();
        let code_path = code_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || extract_zip(&zip_bytes, &code_path))
            .await
            .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))??;

        // Step 1: docker create (no volume mounts — works in Docker-in-Docker)
        let mut cmd = tokio::process::Command::new(&self.cli);
        cmd.arg("create")
            .arg("-p")
            .arg(":8080")
            .arg("--label")
            .arg(format!("fakecloud-lambda={}", func.function_name))
            .arg("--label")
            .arg(format!("fakecloud-instance={}", self.instance_id));
        self.apply_host_alias(&mut cmd);

        for (key, value) in rewrite_localhost_envs(&func.environment, &self.host_alias) {
            cmd.arg("-e").arg(format!("{key}={value}"));
        }

        cmd.arg("-e")
            .arg(format!("AWS_LAMBDA_FUNCTION_TIMEOUT={}", func.timeout));

        let tmpfs_arg = ephemeral_storage_tmpfs_arg(func.ephemeral_storage_size);
        cmd.arg("--tmpfs").arg(tmpfs_arg);

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
            self.remove_container(&container_id).await;
            let stderr = String::from_utf8_lossy(&cp_result.stderr);
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker cp failed: {stderr}"
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
                self.remove_container(&container_id).await;
                let stderr = String::from_utf8_lossy(&cp_runtime.stderr);
                return Err(RuntimeError::ContainerStartFailed(format!(
                    "docker cp to /var/runtime failed: {stderr}"
                )));
            }
        }

        if let Err(e) = self.copy_layers_into(&container_id, layers).await {
            self.remove_container(&container_id).await;
            return Err(e);
        }

        // TempDir is dropped here — code now lives inside the container

        let start_result = tokio::process::Command::new(&self.cli)
            .args(["start", &container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;

        if !start_result.status.success() {
            self.remove_container(&container_id).await;
            let stderr = String::from_utf8_lossy(&start_result.stderr);
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker start failed: {stderr}"
            )));
        }

        let port = self.query_host_port(&container_id).await?;
        self.wait_for_ready(&container_id, port).await?;

        tracing::info!(
            function = %func.function_name,
            container_id = %container_id,
            port = port,
            runtime = %func.runtime,
            "Lambda container started"
        );

        Ok(WarmInstance {
            endpoint: format!("127.0.0.1:{port}"),
            handle: BackendHandle::Container { id: container_id },
        })
    }

    async fn query_host_port(&self, container_id: &str) -> Result<u16, RuntimeError> {
        let port_output = tokio::process::Command::new(&self.cli)
            .args(["port", container_id, "8080"])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        let port_str = String::from_utf8_lossy(&port_output.stdout);
        port_str
            .trim()
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(|| {
                RuntimeError::ContainerStartFailed(format!(
                    "could not determine port from: {}",
                    port_str.trim()
                ))
            })
    }

    async fn wait_for_ready(&self, container_id: &str, port: u16) -> Result<(), RuntimeError> {
        for _ in 0..20 {
            tokio::time::sleep(Duration::from_millis(500)).await;
            if tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
                .await
                .is_ok()
            {
                return Ok(());
            }
        }
        self.remove_container(container_id).await;
        Err(RuntimeError::ContainerStartFailed(
            "container did not become ready within 10 seconds".to_string(),
        ))
    }

    /// Extract each layer ZIP into a shared temp directory and `docker cp`
    /// it into `/opt/` of the target container. Layer ZIPs include
    /// language-specific subpaths (`python/`, `nodejs/`, `java/`, `lib/`,
    /// `bin/`) that AWS base images already wire onto the runtime's
    /// import paths, so plain extraction at the temp root produces the
    /// correct on-disk layout. Empty `layers` is a no-op.
    async fn copy_layers_into(
        &self,
        container_id: &str,
        layers: &[Vec<u8>],
    ) -> Result<(), RuntimeError> {
        if layers.is_empty() {
            return Ok(());
        }
        let layers_dir =
            TempDir::new().map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))?;
        let layers_path = layers_dir.path().to_path_buf();
        let layers_owned: Vec<Vec<u8>> = layers.to_vec();
        tokio::task::spawn_blocking(move || {
            for bytes in &layers_owned {
                extract_zip(bytes, &layers_path)?;
            }
            Ok::<_, RuntimeError>(())
        })
        .await
        .map_err(|e| RuntimeError::ZipExtractionFailed(e.to_string()))??;

        let cp_result = tokio::process::Command::new(&self.cli)
            .arg("cp")
            .arg(format!("{}/.", layers_dir.path().display()))
            .arg(format!("{}:/opt", container_id))
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !cp_result.status.success() {
            let stderr = String::from_utf8_lossy(&cp_result.stderr);
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker cp layers to /opt failed: {stderr}"
            )));
        }
        Ok(())
    }

    /// Remove a container (stop + rm, since we don't use --rm with docker create).
    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }
}

#[async_trait]
impl LambdaBackend for DockerBackend {
    fn name(&self) -> &str {
        &self.cli
    }

    async fn launch(
        &self,
        func: &LambdaFunction,
        code_zip: Option<&[u8]>,
        layers: &[Vec<u8>],
        _deploy_id: &str,
    ) -> Result<WarmInstance, RuntimeError> {
        if func.package_type == "Image" {
            self.start_image_container(func, layers).await
        } else {
            let bytes =
                code_zip.ok_or_else(|| RuntimeError::NoCodeZip(func.function_name.clone()))?;
            self.start_zip_container(func, bytes, layers).await
        }
    }

    async fn terminate(&self, handle: &BackendHandle) {
        match handle {
            BackendHandle::Container { id } => self.remove_container(id).await,
            // Pod handles belong to the K8s backend — defensive no-op
            // so a mis-wired multi-backend setup doesn't panic.
            BackendHandle::Pod { .. } => {}
        }
    }

    async fn prepull_image(&self, image: &str) -> Result<(), RuntimeError> {
        // Translate AWS-flavored ECR URIs to fakecloud's local registry so
        // private-ECR `Image` package functions can be warmed too. Falls
        // back to the URI as-is for public-ECR / Docker Hub / Quay images.
        let local_uri = fakecloud_core::ecr_uri::translate_to_local(image, self.server_port);
        let pull_uri = local_uri.as_deref().unwrap_or(image);

        let mut cmd = tokio::process::Command::new(&self.cli);
        if let Some(p) = self.docker_config_path() {
            cmd.env("DOCKER_CONFIG", p);
        }
        let out = cmd
            .args(["pull", pull_uri])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(format!("docker pull: {e}")))?;
        if !out.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "docker pull failed for {pull_uri}: {}",
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(())
    }
}

/// Map AWS runtime identifier to a Docker image tag.
pub fn runtime_to_image(runtime: &str) -> Option<String> {
    let (base, tag) = match runtime {
        "python3.14" => ("python", "3.14"),
        "python3.13" => ("python", "3.13"),
        "python3.12" => ("python", "3.12"),
        "python3.11" => ("python", "3.11"),
        "python3.10" => ("python", "3.10"),
        "python3.9" => ("python", "3.9"),
        "python3.8" => ("python", "3.8"),
        "nodejs24.x" => ("nodejs", "24"),
        "nodejs22.x" => ("nodejs", "22"),
        "nodejs20.x" => ("nodejs", "20"),
        "nodejs18.x" => ("nodejs", "18"),
        "nodejs16.x" => ("nodejs", "16"),
        "ruby3.4" => ("ruby", "3.4"),
        "ruby3.3" => ("ruby", "3.3"),
        "java25" => ("java", "25"),
        "java21" => ("java", "21"),
        "java17" => ("java", "17"),
        "java11" => ("java", "11"),
        "dotnet10" => ("dotnet", "10"),
        "dotnet8" => ("dotnet", "8"),
        "go1.x" => ("go", "1"),
        "provided.al2023" => ("provided", "al2023"),
        "provided.al2" => ("provided", "al2"),
        _ => return None,
    };
    Some(format!("public.ecr.aws/lambda/{base}:{tag}"))
}

/// Build the `--tmpfs` argument string used by `docker create` so that
/// `/tmp` inside the container is sized to the function's
/// `EphemeralStorage.Size`. Pure helper extracted from the container
/// boot path so unit tests can verify the flag without spawning Docker.
///
/// Defaults to AWS's 512 MiB when `size` is `None`, and clamps to a 64
/// MiB minimum so legacy snapshots that smuggled in absurd values still
/// produce a tmpfs Docker accepts. The `exec` mount option matches AWS
/// Lambda's `/tmp` behavior — handlers that unpack and run binaries
/// from `/tmp` would otherwise hit `EACCES` against Docker's default
/// `noexec` tmpfs.
pub(crate) fn ephemeral_storage_tmpfs_arg(size: Option<i64>) -> String {
    let mib = size.unwrap_or(512).max(64);
    format!("/tmp:size={mib}m,exec")
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

/// Detect the Docker bridge gateway IP on Linux. Returns None if detection fails.
fn detect_bridge_gateway(cli: &str) -> Option<String> {
    let output = std::process::Command::new(cli)
        .args([
            "network",
            "inspect",
            "bridge",
            "--format",
            "{{range .IPAM.Config}}{{.Gateway}}{{end}}",
        ])
        .output()
        .ok()?;

    if output.status.success() {
        let gateway = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !gateway.is_empty() && gateway.contains('.') {
            tracing::info!(
                gateway = %gateway,
                "Detected Docker bridge gateway for Lambda containers"
            );
            return Some(gateway);
        }
    }
    None
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

/// True when `cli` is podman or a podman-compatible binary. Matches on the
/// filename component so absolute paths (`/opt/homebrew/bin/podman`) and
/// wrappers (`podman-remote`) both register as podman. Docker Desktop's
/// compatibility CLI is named `docker`, so this check is safe.
fn is_podman_binary(cli: &str) -> bool {
    std::path::Path::new(cli)
        .file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.contains("podman"))
        .unwrap_or(false)
}

fn build_local_registry_docker_config(server_port: u16) -> Option<TempDir> {
    let dir = TempDir::new().ok()?;
    let auth = base64::engine::general_purpose::STANDARD.encode("AWS:fakecloud-lambda-runtime");
    let config = serde_json::json!({
        "auths": {
            format!("127.0.0.1:{server_port}"): { "auth": auth },
        }
    });
    std::fs::write(dir.path().join("config.json"), config.to_string()).ok()?;
    Some(dir)
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
        assert_eq!(
            runtime_to_image("nodejs16.x"),
            Some("public.ecr.aws/lambda/nodejs:16".to_string())
        );
        assert_eq!(
            runtime_to_image("python3.10"),
            Some("public.ecr.aws/lambda/python:3.10".to_string())
        );
        assert_eq!(
            runtime_to_image("python3.9"),
            Some("public.ecr.aws/lambda/python:3.9".to_string())
        );
        assert_eq!(
            runtime_to_image("python3.8"),
            Some("public.ecr.aws/lambda/python:3.8".to_string())
        );
        assert_eq!(
            runtime_to_image("java11"),
            Some("public.ecr.aws/lambda/java:11".to_string())
        );
        assert_eq!(
            runtime_to_image("go1.x"),
            Some("public.ecr.aws/lambda/go:1".to_string())
        );
        assert_eq!(
            runtime_to_image("nodejs24.x"),
            Some("public.ecr.aws/lambda/nodejs:24".to_string())
        );
        assert_eq!(
            runtime_to_image("python3.14"),
            Some("public.ecr.aws/lambda/python:3.14".to_string())
        );
        assert_eq!(
            runtime_to_image("java25"),
            Some("public.ecr.aws/lambda/java:25".to_string())
        );
        assert_eq!(
            runtime_to_image("dotnet10"),
            Some("public.ecr.aws/lambda/dotnet:10".to_string())
        );
        assert_eq!(runtime_to_image("unknown"), None);
    }

    #[test]
    fn is_podman_binary_matches_bare_name() {
        assert!(is_podman_binary("podman"));
        assert!(is_podman_binary("podman-remote"));
    }

    #[test]
    fn is_podman_binary_matches_absolute_path() {
        assert!(is_podman_binary("/opt/homebrew/bin/podman"));
        assert!(is_podman_binary("/usr/local/bin/podman-remote"));
    }

    #[test]
    fn is_podman_binary_rejects_docker() {
        assert!(!is_podman_binary("docker"));
        assert!(!is_podman_binary("/usr/local/bin/docker"));
        // Docker Desktop's compatibility CLI is `docker`, not `podman`.
        assert!(!is_podman_binary("docker-credential-helper"));
    }

    #[test]
    fn test_extract_zip() {
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

    #[test]
    fn ephemeral_storage_tmpfs_arg_defaults_to_512_when_none() {
        // None -> AWS default of 512 MiB. The `exec` flag is required so
        // handlers that unpack and run binaries from /tmp don't hit
        // EACCES against Docker's default `noexec` tmpfs.
        assert_eq!(ephemeral_storage_tmpfs_arg(None), "/tmp:size=512m,exec");
    }

    #[test]
    fn ephemeral_storage_tmpfs_arg_uses_supplied_size() {
        assert_eq!(
            ephemeral_storage_tmpfs_arg(Some(2048)),
            "/tmp:size=2048m,exec"
        );
        assert_eq!(
            ephemeral_storage_tmpfs_arg(Some(10240)),
            "/tmp:size=10240m,exec"
        );
    }

    #[test]
    fn ephemeral_storage_tmpfs_arg_clamps_to_64_floor() {
        // API-level validation already rejects values below 512, but the
        // runtime defends against legacy snapshots and stale state by
        // clamping to a 64 MiB floor that Docker still accepts.
        assert_eq!(ephemeral_storage_tmpfs_arg(Some(0)), "/tmp:size=64m,exec");
        assert_eq!(ephemeral_storage_tmpfs_arg(Some(32)), "/tmp:size=64m,exec");
    }
}
