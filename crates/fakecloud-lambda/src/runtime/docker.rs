//! Docker/Podman [`LambdaBackend`] implementation.
//!
//! Shells out to `docker` or `podman` CLI. Auto-detects which one is
//! available; honors `FAKECLOUD_CONTAINER_CLI` as an override.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use tempfile::TempDir;

use super::backend::{BackendHandle, LambdaBackend, RuntimeApiAddress, RuntimeError, WarmInstance};
use super::container_exits::ContainerExits;
use super::environment::{function_environment, CREDENTIAL_ENV_KEYS};
use crate::state::LambdaFunction;
use fakecloud_core::auth::SessionCredentials;

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
    /// What each instance's Runtime API host name is mapped to
    /// (`--add-host <host>:<this>`): the address `host_alias` resolves to
    /// inside a container, so the runtime reaches the same fakecloud
    /// listener `AWS_ENDPOINT_URL` names. Known up front when the alias is
    /// mapped explicitly or resolves from here; otherwise (podman's native
    /// alias) probed once in a container, see [`Self::runtime_api_target`].
    known_runtime_api_target: Option<String>,
    probed_runtime_api_target: tokio::sync::OnceCell<String>,
    /// When fakecloud itself runs in a container: the network Lambda
    /// containers join and fakecloud's address on it. Runtimes then reach
    /// fakecloud's own listener there (whatever port, if any, the host
    /// publishes for it), and so do SDK calls.
    self_network: Option<SelfNetwork>,
    /// Reports when each instance's runtime process exits.
    exits: Arc<ContainerExits>,
    /// Port the main fakecloud server bound to. Used to translate AWS
    /// private-ECR URIs in `PackageType=Image` functions to fakecloud's
    /// local OCI v2 registry.
    server_port: u16,
    /// Registry host used by the Docker daemon when pulling fakecloud ECR
    /// images. This is separate from the host alias: Docker Desktop and
    /// OrbStack accept the local HTTP registry only over loopback.
    registry_host: String,
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
        // Container-to-host networking (CLI detection, host alias,
        // --add-host injection, and the in-container sibling address) all
        // come from the shared `fakecloud-core::container_net` helper so the
        // four container-spawning runtimes (Lambda/ECS/RDS/ElastiCache) can't
        // drift apart on the issue #1539 fix. `detect_container_cli` honors
        // `FAKECLOUD_CONTAINER_CLI` and prefers docker then podman, returning
        // `None` when neither is usable.
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        let instance_id = format!("fakecloud-{}", std::process::id());
        let net = fakecloud_core::container_net::HostNetworking::detect(&cli);

        let docker_config = build_local_registry_docker_config(server_port).map(Arc::new);
        let registry_host = fakecloud_core::container_net::ecr_registry_host(&cli);
        // The alias only resolves meaningfully from here when fakecloud is
        // itself containerized (see `HostNetworking::detect`).
        let in_container = net.sibling_host != "127.0.0.1";
        let self_network = in_container.then(|| own_network(&cli)).flatten();
        let known_runtime_api_target = match &self_network {
            Some(n) => Some(n.ip.clone()),
            None => known_runtime_api_target(
                net.add_host_arg.as_deref(),
                in_container
                    .then(|| resolve_ipv4(&net.host_alias))
                    .flatten(),
            ),
        };
        let exits = ContainerExits::new(cli.clone(), format!("fakecloud-instance={instance_id}"));
        Some(Self {
            cli,
            instance_id,
            host_alias: net.host_alias,
            add_host_arg: net.add_host_arg,
            known_runtime_api_target,
            probed_runtime_api_target: tokio::sync::OnceCell::new(),
            self_network,
            exits,
            server_port,
            registry_host,
            docker_config,
        })
    }

    /// Append the `--add-host` arguments: the host alias mapping when the
    /// runtime needs an explicit one (docker; podman provides
    /// `host.containers.internal` natively), and the instance's Runtime API
    /// host, mapped to the same place.
    async fn apply_host_alias(
        &self,
        cmd: &mut tokio::process::Command,
        runtime_api: &RuntimeApiAddress,
    ) {
        if let Some(arg) = &self.add_host_arg {
            cmd.arg("--add-host").arg(arg);
        }
        if let Some(net) = &self.self_network {
            cmd.arg("--network").arg(&net.network);
        }
        let target = self.runtime_api_target().await;
        cmd.arg("--add-host")
            .arg(format!("{}:{target}", runtime_api.host));
    }

    /// The address the host alias names inside a container. When the engine
    /// provides the alias itself (podman's `host.containers.internal`), the
    /// address is only known in there: look it up once in a throwaway
    /// container of a small known image ([`PROBE_IMAGE`], whatever the
    /// function's image ships), caching the answer; `host-gateway` (not
    /// cached) when it cannot be found.
    async fn runtime_api_target(&self) -> String {
        if let Some(target) = &self.known_runtime_api_target {
            return target.clone();
        }
        if let Some(target) = self.probed_runtime_api_target.get() {
            return target.clone();
        }
        let image = PROBE_IMAGE;
        if let Err(e) = fakecloud_core::container_image::ensure_image(&self.cli, None, image).await
        {
            tracing::warn!(%image, error = %e, "could not pull the host-alias probe image");
        }
        let script = format!(
            "getent ahostsv4 {alias} 2>/dev/null || cat /etc/hosts",
            alias = self.host_alias
        );
        let probe = tokio::process::Command::new(&self.cli)
            .args([
                "run",
                "--rm",
                "--label",
                &format!("fakecloud-instance={}", self.instance_id),
                "--entrypoint",
                "/bin/sh",
                image,
                "-c",
                &script,
            ])
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output();
        let outcome = tokio::time::timeout(std::time::Duration::from_secs(60), probe).await;
        let found = match &outcome {
            Ok(Ok(out)) => alias_address(&String::from_utf8_lossy(&out.stdout), &self.host_alias),
            _ => None,
        };
        match found {
            Some(ip) => {
                let _ = self.probed_runtime_api_target.set(ip.clone());
                ip
            }
            None => {
                // Not cached: the next launch looks again.
                let detail = match outcome {
                    Ok(Ok(out)) => String::from_utf8_lossy(&out.stderr).trim().to_string(),
                    Ok(Err(e)) => e.to_string(),
                    Err(_) => "timed out".to_string(),
                };
                tracing::warn!(
                    alias = %self.host_alias,
                    %detail,
                    "could not find the container address of the host alias; \
                     mapping the Lambda Runtime API host to host-gateway"
                );
                "host-gateway".to_string()
            }
        }
    }

    /// How a Lambda container reaches fakecloud: its own address on the
    /// shared network when fakecloud runs in a container, else the host
    /// alias.
    fn endpoint_url(&self) -> String {
        let host = self
            .self_network
            .as_ref()
            .map_or(self.host_alias.as_str(), |n| n.ip.as_str());
        format!("http://{host}:{}", self.server_port)
    }

    /// Export the function's execution environment into the container. The
    /// container reaches fakecloud through the host alias, since `localhost`
    /// inside it is the container itself, and its runtime reaches the
    /// Runtime API through its own host name on the same listener.
    fn apply_function_env(
        &self,
        cmd: &mut tokio::process::Command,
        func: &LambdaFunction,
        credentials: Option<&SessionCredentials>,
        runtime_api: &RuntimeApiAddress,
    ) {
        let endpoint_url = self.endpoint_url();
        let env = function_environment(
            func,
            &endpoint_url,
            &self.host_alias,
            &format!("{}:{}", runtime_api.host, self.server_port),
            credentials,
        );
        let env = match &self.self_network {
            Some(net) => fakecloud_on_its_network(env, &self.host_alias, self.server_port, &net.ip),
            None => env,
        };
        let (args, child_env) = docker_env_args(env);
        cmd.args(args).envs(child_env);
    }

    fn docker_config_path(&self) -> Option<PathBuf> {
        self.docker_config.as_ref().map(|d| d.path().to_path_buf())
    }

    /// Start a container for a `PackageType=Image` function. Its runtime
    /// (the AWS base images' entrypoint, or a runtime interface client)
    /// talks to the Runtime API at `AWS_LAMBDA_RUNTIME_API`, as on Lambda.
    /// AWS private-ECR URIs get translated to fakecloud's local OCI v2
    /// registry and retagged so the container reports its user-visible
    /// image name.
    async fn start_image_container(
        &self,
        func: &LambdaFunction,
        layers: &[Vec<u8>],
        credentials: Option<&SessionCredentials>,
        runtime_api: &RuntimeApiAddress,
    ) -> Result<WarmInstance, RuntimeError> {
        let image = func.image_uri.as_deref().ok_or_else(|| {
            RuntimeError::ContainerStartFailed("PackageType=Image function has no ImageUri".into())
        })?;

        // Docker pulls through the host daemon. Unlike the Lambda callback
        // path, local ECR must use the loopback registry on Docker Desktop
        // and OrbStack because it is served over HTTP.
        let local_pull_uri = fakecloud_core::ecr_uri::translate_to_local_at(
            image,
            &self.registry_host,
            self.server_port,
        );
        let pull_uri = local_pull_uri.as_deref().unwrap_or(image);

        fakecloud_core::container_image::pull_image(
            &self.cli,
            self.docker_config_path().as_deref(),
            pull_uri,
            fakecloud_core::container_image::RegistryTransport::for_local_rewrite(
                local_pull_uri.is_some(),
            ),
        )
        .await
        .map_err(|e| RuntimeError::ContainerStartFailed(format!("docker pull failed: {e}")))?;
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

        let (entrypoint, image_cmd) = self.image_command(&run_image).await;
        let command = super::extensions::function_command(
            &entrypoint,
            &image_cmd,
            func.image_config.as_ref(),
        );
        let workdir = super::extensions::image_working_directory(func.image_config.as_ref());

        let mut container_id = self
            .create_container(
                func,
                &run_image,
                &command,
                true,
                workdir.as_deref(),
                credentials,
                runtime_api,
            )
            .await?;
        if let Err(e) = self.prepare_container(&container_id, layers, true).await {
            self.remove_container(&container_id).await;
            return Err(e);
        }
        // The launcher is a shell script: an image without `/bin/sh` runs
        // its command directly, without its extensions.
        let launcher = self.has_shell(&container_id).await;
        if !launcher {
            tracing::warn!(
                image = %image,
                "image has no /bin/sh: its /opt/extensions are not started"
            );
            self.remove_container(&container_id).await;
            container_id = self
                .create_container(
                    func,
                    &run_image,
                    &command,
                    false,
                    workdir.as_deref(),
                    credentials,
                    runtime_api,
                )
                .await?;
            if let Err(e) = self.prepare_container(&container_id, layers, false).await {
                self.remove_container(&container_id).await;
                return Err(e);
            }
        }

        let exit = self
            .start_container(&container_id, runtime_api, launcher)
            .await?;

        tracing::info!(
            function = %func.function_name,
            container_id = %container_id,
            image = %image,
            "Lambda image container started"
        );

        Ok(WarmInstance {
            handle: BackendHandle::Container { id: container_id },
            exit,
            launcher,
        })
    }

    async fn start_zip_container(
        &self,
        func: &LambdaFunction,
        zip_bytes: &[u8],
        layers: &[Vec<u8>],
        credentials: Option<&SessionCredentials>,
        runtime_api: &RuntimeApiAddress,
    ) -> Result<WarmInstance, RuntimeError> {
        let image = runtime_to_image(&func.runtime)
            .ok_or_else(|| RuntimeError::UnsupportedRuntime(func.runtime.clone()))?;

        // Extract ZIP to a temp directory (only needed during container setup).
        // Run in spawn_blocking to avoid blocking the async runtime with fs I/O.
        let code_dir = TempDir::new().map_err(|e| RuntimeError::LocalIo(e.to_string()))?;
        let zip_bytes = zip_bytes.to_vec();
        let code_path = code_dir.path().to_path_buf();
        tokio::task::spawn_blocking(move || extract_zip(&zip_bytes, &code_path))
            .await
            .map_err(|e| RuntimeError::LocalIo(e.to_string()))??;

        // Step 1: docker create (no volume mounts — works in Docker-in-Docker)
        let command = vec![
            super::extensions::BASE_IMAGE_ENTRYPOINT.to_string(),
            func.handler.clone(),
        ];
        let container_id = self
            .create_container(func, &image, &command, true, None, credentials, runtime_api)
            .await?;

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

        if let Err(e) = self.prepare_container(&container_id, layers, true).await {
            self.remove_container(&container_id).await;
            return Err(e);
        }

        // TempDir is dropped here — code now lives inside the container

        let exit = self
            .start_container(&container_id, runtime_api, true)
            .await?;

        tracing::info!(
            function = %func.function_name,
            container_id = %container_id,
            runtime = %func.runtime,
            "Lambda container started"
        );

        Ok(WarmInstance {
            handle: BackendHandle::Container { id: container_id },
            exit,
            launcher: true,
        })
    }

    /// `docker create` a function container running `command` (its
    /// entrypoint and arguments), through the extension launcher when
    /// `launcher`.
    #[allow(clippy::too_many_arguments)]
    async fn create_container(
        &self,
        func: &LambdaFunction,
        image: &str,
        command: &[String],
        launcher: bool,
        workdir: Option<&str>,
        credentials: Option<&SessionCredentials>,
        runtime_api: &RuntimeApiAddress,
    ) -> Result<String, RuntimeError> {
        let mut cmd = tokio::process::Command::new(&self.cli);
        cmd.arg("create")
            .arg("--label")
            .arg(format!("fakecloud-lambda={}", func.function_name))
            .arg("--label")
            .arg(format!("fakecloud-instance={}", self.instance_id));
        self.apply_host_alias(&mut cmd, runtime_api).await;
        self.apply_function_env(&mut cmd, func, credentials, runtime_api);
        cmd.arg("--tmpfs")
            .arg(ephemeral_storage_tmpfs_arg(func.ephemeral_storage_size));
        cmd.args(memory_limit_args(
            func.memory_size,
            func.ephemeral_storage_size,
        ));
        if let Some(dir) = workdir {
            cmd.arg("--workdir").arg(dir);
        }
        let (entry, args) = if launcher {
            (Some(super::extensions::LAUNCHER_PATH), command)
        } else {
            match command.split_first() {
                Some((first, rest)) => (Some(first.as_str()), rest),
                None => (None, command),
            }
        };
        if let Some(entry) = entry {
            cmd.arg("--entrypoint").arg(entry);
        }
        cmd.arg(image).args(args);
        let output = cmd
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !output.status.success() {
            return Err(RuntimeError::ContainerStartFailed(
                String::from_utf8_lossy(&output.stderr).to_string(),
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
    }

    /// Copy the layers (and, with `launcher`, the extension launcher) into
    /// a created container.
    async fn prepare_container(
        &self,
        container_id: &str,
        layers: &[Vec<u8>],
        launcher: bool,
    ) -> Result<(), RuntimeError> {
        self.copy_layers_into(container_id, layers).await?;
        if launcher {
            // Streamed in as a tar so the directory keeps its mode: world
            // writable (sticky) for the launcher's bookkeeping whatever user
            // the image runs as.
            let target = format!("{container_id}:/var");
            run_with_stdin(
                &self.cli,
                &["cp", "-", &target],
                &super::extensions::launcher_tar(),
            )
            .await
            .map_err(|e| {
                RuntimeError::ContainerStartFailed(format!(
                    "docker cp of the extension launcher failed: {e}"
                ))
            })?;
        }
        Ok(())
    }

    /// Whether a created container's image has `/bin/sh` (read from its
    /// filesystem, so nothing has to run).
    async fn has_shell(&self, container_id: &str) -> bool {
        tokio::process::Command::new(&self.cli)
            .args(["cp", &format!("{container_id}:/bin/sh"), "-"])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await
            .is_ok_and(|s| s.success())
    }

    /// The image's own entrypoint and command.
    async fn image_command(&self, image: &str) -> (Vec<String>, Vec<String>) {
        let out = tokio::process::Command::new(&self.cli)
            .args([
                "image",
                "inspect",
                "--format",
                "{{json .Config.Entrypoint}}\t{{json .Config.Cmd}}",
                image,
            ])
            .output()
            .await;
        let text = out
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
            .unwrap_or_default();
        let (entry, cmd) = text.trim().split_once('\t').unwrap_or(("null", "null"));
        let parse = |v: &str| {
            serde_json::from_str::<Option<Vec<String>>>(v)
                .ok()
                .flatten()
                .unwrap_or_default()
        };
        (parse(entry), parse(cmd))
    }

    /// Start a created container, watching its runtime process from before
    /// it starts so an immediate exit is never missed.
    async fn start_container(
        &self,
        container_id: &str,
        runtime_api: &RuntimeApiAddress,
        launcher: bool,
    ) -> Result<super::backend::ExitWatch, RuntimeError> {
        if launcher {
            // The first event waits for the launcher's extension count.
            runtime_api.sandbox.await_extension_count();
        }
        let exit = self.exits.watch(container_id);
        let start_result = tokio::process::Command::new(&self.cli)
            .args(["start", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()));
        let failure = match start_result {
            Ok(out) if out.status.success() => return Ok(exit),
            Ok(out) => format!(
                "docker start failed: {}",
                String::from_utf8_lossy(&out.stderr)
            ),
            Err(e) => e.to_string(),
        };
        self.exits.forget(container_id);
        self.remove_container(container_id).await;
        Err(RuntimeError::ContainerStartFailed(failure))
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
        let layers_dir = TempDir::new().map_err(|e| RuntimeError::LocalIo(e.to_string()))?;
        let layers_path = layers_dir.path().to_path_buf();
        let layers_owned: Vec<Vec<u8>> = layers.to_vec();
        tokio::task::spawn_blocking(move || {
            for bytes in &layers_owned {
                extract_zip(bytes, &layers_path)?;
            }
            Ok::<_, RuntimeError>(())
        })
        .await
        .map_err(|e| RuntimeError::LocalIo(e.to_string()))??;

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
    fn callback_endpoint(&self) -> Option<String> {
        Some(self.endpoint_url())
    }

    fn name(&self) -> &str {
        &self.cli
    }

    async fn launch(
        &self,
        func: &LambdaFunction,
        code_zip: Option<&[u8]>,
        layers: &[Vec<u8>],
        _deploy_id: &str,
        credentials: Option<&SessionCredentials>,
        runtime_api: &RuntimeApiAddress,
    ) -> Result<WarmInstance, RuntimeError> {
        if func.package_type == "Image" {
            self.start_image_container(func, layers, credentials, runtime_api)
                .await
        } else {
            let bytes =
                code_zip.ok_or_else(|| RuntimeError::NoCodeZip(func.function_name.clone()))?;
            self.start_zip_container(func, bytes, layers, credentials, runtime_api)
                .await
        }
    }

    async fn terminate(&self, handle: &BackendHandle) {
        match handle {
            BackendHandle::Container { id } => {
                self.remove_container(id).await;
                self.exits.forget(id);
            }
            // Pod handles belong to the K8s backend — defensive no-op
            // so a mis-wired multi-backend setup doesn't panic.
            BackendHandle::Pod { .. } => {}
        }
    }

    async fn read_instance_file(&self, handle: &BackendHandle, path: &str) -> Option<Vec<u8>> {
        let BackendHandle::Container { id } = handle else {
            return None;
        };
        // A caller that gives up on a hung CLI (see `monitor_launcher`)
        // must not leave the process behind.
        let out = tokio::process::Command::new(&self.cli)
            .args(["cp", &format!("{id}:{path}"), "-"])
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .ok()
            .filter(|o| o.status.success())?;
        let mut archive = tar::Archive::new(out.stdout.as_slice());
        let mut entry = archive.entries().ok()?.flatten().next()?;
        let mut contents = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut contents).ok()?;
        Some(contents)
    }

    async fn write_instance_file(
        &self,
        handle: &BackendHandle,
        path: &str,
        contents: &[u8],
    ) -> bool {
        let BackendHandle::Container { id } = handle else {
            return false;
        };
        let Ok(dir) = TempDir::new() else {
            return false;
        };
        let name = Path::new(path)
            .file_name()
            .map(|n| n.to_owned())
            .unwrap_or_default();
        let local = dir.path().join(name);
        if std::fs::write(&local, contents).is_err() {
            return false;
        }
        tokio::process::Command::new(&self.cli)
            .arg("cp")
            .arg(&local)
            .arg(format!("{id}:{path}"))
            .stdin(std::process::Stdio::null())
            .kill_on_drop(true)
            .output()
            .await
            .is_ok_and(|o| o.status.success())
    }

    async fn log_cursor(&self, handle: &BackendHandle) -> Option<String> {
        let BackendHandle::Container { id } = handle else {
            return None;
        };
        let read = |extra: Vec<String>| async move {
            let out = tokio::process::Command::new(&self.cli)
                .args(["logs", "--timestamps"])
                .args(extra)
                .arg(id)
                .output()
                .await
                .ok()?;
            Some((
                String::from_utf8_lossy(&out.stdout).into_owned(),
                String::from_utf8_lossy(&out.stderr).into_owned(),
            ))
        };
        let (out, err) = read(vec![
            "--tail".into(),
            super::logs::CURSOR_TAIL_LINES.to_string(),
        ])
        .await?;
        if super::logs::tail_covers_cursor(&[&out, &err]) {
            return super::logs::cursor(&[&out, &err]);
        }
        // Every tail line shares the latest timestamp: count them all.
        let latest = super::logs::cursor(&[&out, &err])?;
        let since = super::logs::cursor_since(&latest).to_string();
        let (out, err) = read(vec!["--since".into(), since]).await?;
        super::logs::cursor(&[&out, &err])
    }

    async fn instance_logs(&self, handle: &BackendHandle, since: Option<&str>) -> Option<String> {
        let BackendHandle::Container { id } = handle else {
            return None;
        };
        // `docker logs` writes the container's stdout to our stdout and its
        // stderr to our stderr; the timestamps put them back in order.
        let mut cmd = tokio::process::Command::new(&self.cli);
        cmd.args(["logs", "--timestamps"]);
        if let Some(since) = since {
            cmd.args(["--since", super::logs::cursor_since(since)]);
        }
        let output = cmd.arg(id).output().await.ok()?;
        Some(super::logs::lines_since(
            &[
                &String::from_utf8_lossy(&output.stdout),
                &String::from_utf8_lossy(&output.stderr),
            ],
            since,
        ))
    }

    async fn prepull_image(&self, image: &str) -> Result<(), RuntimeError> {
        // Translate AWS-flavored ECR URIs to fakecloud's local registry so
        // private-ECR `Image` package functions can be warmed too. Falls
        // back to the URI as-is for public-ECR / Docker Hub / Quay images.
        let local_uri = fakecloud_core::ecr_uri::translate_to_local_at(
            image,
            &self.registry_host,
            self.server_port,
        );
        let pull_uri = local_uri.as_deref().unwrap_or(image);

        fakecloud_core::container_image::pull_image(
            &self.cli,
            self.docker_config_path().as_deref(),
            pull_uri,
            fakecloud_core::container_image::RegistryTransport::for_local_rewrite(
                local_uri.is_some(),
            ),
        )
        .await
        .map_err(|e| {
            RuntimeError::ContainerStartFailed(format!("docker pull failed for {pull_uri}: {e}"))
        })?;
        Ok(())
    }
}

/// The image the host-alias address is looked up in: small, with a shell
/// and `getent`, and already what fakecloud runs helper containers from.
const PROBE_IMAGE: &str = "public.ecr.aws/docker/library/alpine:3.20";

/// What an instance's Runtime API host is mapped to with `--add-host`, when
/// known without looking inside a container: the value the host alias itself
/// is mapped to (`host-gateway` on Docker Desktop, the bridge gateway on
/// Linux docker), else the address the alias resolves to from here (fakecloud
/// running in a sibling container, where the engine provides the alias
/// natively).
fn known_runtime_api_target(
    alias_mapping: Option<&str>,
    resolved_alias: Option<String>,
) -> Option<String> {
    alias_mapping
        .and_then(|arg| arg.split_once(':'))
        .map(|(_, target)| target.to_string())
        .or(resolved_alias)
}

/// Point the function's own references to fakecloud (a loopback address on
/// fakecloud's port, already rewritten to the host alias) at fakecloud's
/// address on the shared network: a containerized fakecloud is not
/// necessarily reachable through the host on that port. Loopback addresses
/// on other ports still name the host.
fn fakecloud_on_its_network(
    env: Vec<(String, String)>,
    host_alias: &str,
    port: u16,
    ip: &str,
) -> Vec<(String, String)> {
    let from = format!("{host_alias}:{port}");
    let to = format!("{ip}:{port}");
    env.into_iter()
        .map(|(k, v)| {
            let v = v.replace(&from, &to);
            (k, v)
        })
        .collect()
}

/// The network a containerized fakecloud shares with its Lambda containers,
/// and fakecloud's address on it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SelfNetwork {
    network: String,
    ip: String,
}

/// Run `cli args...` with `input` on its stdin. `Err` names why it failed,
/// with the CLI's stderr: a CLI that exits before reading all of `input`
/// (a `cp -` refusing its target, say) makes the write fail with a broken
/// pipe, and what it said on stderr is the reason worth reporting.
async fn run_with_stdin(cli: &str, args: &[&str], input: &[u8]) -> Result<(), String> {
    use tokio::io::AsyncWriteExt;
    let mut child = tokio::process::Command::new(cli)
        .args(args)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("could not run {cli}: {e}"))?;
    let written = match child.stdin.take() {
        // Dropped once written, so the CLI sees the end of its input.
        Some(mut stdin) => stdin.write_all(input).await,
        None => Ok(()),
    };
    let out = child
        .wait_with_output()
        .await
        .map_err(|e| format!("could not wait for {cli}: {e}"))?;
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stderr = stderr.trim();
    match (written, out.status.success()) {
        (Ok(()), true) => Ok(()),
        (Err(e), true) => Err(format!("writing its input failed: {e}: {stderr}")),
        (_, false) => Err(format!("{}: {stderr}", out.status)),
    }
}

/// Find the container fakecloud runs in and the network to share with Lambda
/// containers. `None` outside a container the CLI knows (or in host network
/// mode, where the host alias already reaches fakecloud's port).
fn own_network(cli: &str) -> Option<SelfNetwork> {
    let id = own_container_id()?;
    let inspect = fakecloud_core::container_net::bounded_output(cli, &["inspect", &id])?;
    let selected = select_self_network(&inspect);
    match &selected {
        Some(n) => tracing::info!(
            network = %n.network,
            ip = %n.ip,
            "Lambda containers reach fakecloud on its own container network"
        ),
        None => tracing::warn!(
            container = %id,
            "fakecloud's container has no network Lambda containers can share; \
             they reach it through the host alias"
        ),
    }
    selected
}

/// This process's container ID: from the runtime's bookkeeping in
/// `/proc/self/mountinfo` (`.../containers/<id>/...` on Docker,
/// `.../overlay-containers/<id>/...` on Podman), else the host name, which
/// both engines set to the short ID.
fn own_container_id() -> Option<String> {
    if let Ok(info) = std::fs::read_to_string("/proc/self/mountinfo") {
        if let Some(id) = container_id_in_mountinfo(&info) {
            return Some(id);
        }
    }
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
}

fn container_id_in_mountinfo(info: &str) -> Option<String> {
    info.lines().find_map(|line| {
        line.split('/')
            .collect::<Vec<_>>()
            .windows(2)
            .find(|w| {
                matches!(w[0], "containers" | "overlay-containers")
                    && w[1].len() == 64
                    && w[1].bytes().all(|b| b.is_ascii_hexdigit())
            })
            .map(|w| w[1].to_string())
    })
}

/// From `<cli> inspect <own container>` output: the network (and the
/// container's address on it) Lambda containers should join. A user-defined
/// network (a compose project's) wins over the default bridge, which wins
/// over nothing; host and none mode share no network.
fn select_self_network(inspect: &str) -> Option<SelfNetwork> {
    let v: serde_json::Value = serde_json::from_str(inspect).ok()?;
    let networks = v
        .get(0)
        .unwrap_or(&v)
        .pointer("/NetworkSettings/Networks")?
        .as_object()?;
    let mut candidates: Vec<(bool, &String, String)> = networks
        .iter()
        .filter(|(name, _)| !matches!(name.as_str(), "host" | "none"))
        .filter_map(|(name, n)| {
            let ip = n.get("IPAddress")?.as_str()?;
            (!ip.is_empty()).then(|| {
                let default = matches!(name.as_str(), "bridge" | "podman");
                (default, name, ip.to_string())
            })
        })
        .collect();
    candidates.sort();
    candidates
        .into_iter()
        .next()
        .map(|(_, name, ip)| SelfNetwork {
            network: name.clone(),
            ip,
        })
}

/// The IPv4 address `alias` maps to in `getent ahostsv4` or `/etc/hosts`
/// output.
fn alias_address(output: &str, alias: &str) -> Option<String> {
    output.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let ip = fields.next()?.parse::<std::net::Ipv4Addr>().ok()?;
        fields.any(|f| f == alias).then(|| ip.to_string())
    })
}

/// The first IPv4 address `host` resolves to, bounded so a wedged resolver
/// can't stall startup.
fn resolve_ipv4(host: &str) -> Option<String> {
    let (tx, rx) = std::sync::mpsc::channel();
    let host = host.to_string();
    std::thread::spawn(move || {
        let found = std::net::ToSocketAddrs::to_socket_addrs(&(host.as_str(), 0))
            .ok()
            .and_then(|mut addrs| addrs.find(std::net::SocketAddr::is_ipv4))
            .map(|a| a.ip().to_string());
        let _ = tx.send(found);
    });
    rx.recv_timeout(fakecloud_core::container_net::HOST_ALIAS_RESOLVE_TIMEOUT)
        .ok()
        .flatten()
}

/// `--memory` and `--memory-swap` for a function's container. Without a limit a
/// runtime sizes itself from the host: a JVM takes a quarter of host memory for
/// its heap, so a few warm functions can starve the whole machine.
///
/// The `/tmp` tmpfs is charged to the same cgroup, unlike real Lambda where
/// ephemeral storage is separate from function memory, so its size is added.
/// A function that fills `/tmp` is never killed for it, at the price of one
/// that overruns `MemorySize` surviving by up to the size of `/tmp`.
fn memory_limit_args(memory_size_mb: i64, ephemeral_storage_mb: Option<i64>) -> [String; 4] {
    let limit = format!(
        "{}m",
        memory_size_mb
            .max(128)
            .saturating_add(tmpfs_size_mib(ephemeral_storage_mb))
    );
    [
        "--memory".to_string(),
        limit.clone(),
        "--memory-swap".to_string(),
        limit,
    ]
}

/// `-e` arguments for a container environment, plus the variables to set on
/// the container CLI's own process. Credentials go by name only (`-e KEY`),
/// which makes the CLI copy the value from its environment, so secrets never
/// appear in argv (`ps`, audit logs, `docker events`).
fn docker_env_args(env: Vec<(String, String)>) -> (Vec<String>, Vec<(String, String)>) {
    let mut args = Vec::with_capacity(env.len() * 2);
    let mut child_env = Vec::new();
    for (key, value) in env {
        args.push("-e".to_string());
        if CREDENTIAL_ENV_KEYS.contains(&key.as_str()) {
            args.push(key.clone());
            child_env.push((key, value));
        } else {
            args.push(format!("{key}={value}"));
        }
    }
    (args, child_env)
}

/// Deprecated runtimes AWS still models (existing functions keep reporting
/// them) that fakecloud does not execute. Most predate container-image
/// support and never had a `public.ecr.aws/lambda/*` base image; the rest are
/// past AWS's block-function-create date. Every other runtime in
/// `crate::service::LAMBDA_RUNTIMES` must map to an image in
/// [`runtime_to_image`]; `every_runtime_has_an_image_or_is_a_documented_exception`
/// enforces it, so a newly modeled runtime cannot silently lack one.
#[cfg(test)]
pub(crate) const RUNTIMES_WITHOUT_IMAGE: &[&str] = &[
    "nodejs",
    "nodejs4.3",
    "nodejs4.3-edge",
    "nodejs6.10",
    "nodejs8.10",
    "nodejs10.x",
    "nodejs12.x",
    "nodejs14.x",
    "java8",
    "python2.7",
    "python3.6",
    "python3.7",
    "dotnetcore1.0",
    "dotnetcore2.0",
    "dotnetcore2.1",
    "dotnetcore3.1",
    "dotnet6",
    "ruby2.5",
    "ruby2.7",
    "provided",
];

/// Map AWS runtime identifier to its AWS-published base image.
pub fn runtime_to_image(runtime: &str) -> Option<String> {
    let (base, tag) = match runtime {
        // Only a `-preview` tag is published for these so far.
        "python3.15" => ("python", "3.15-preview"),
        "python3.14" => ("python", "3.14"),
        "python3.13" => ("python", "3.13"),
        "python3.12" => ("python", "3.12"),
        "python3.11" => ("python", "3.11"),
        "python3.10" => ("python", "3.10"),
        "python3.9" => ("python", "3.9"),
        "python3.8" => ("python", "3.8"),
        "nodejs26.x" => ("nodejs", "26-preview"),
        "nodejs24.x" => ("nodejs", "24"),
        "nodejs22.x" => ("nodejs", "22"),
        "nodejs20.x" => ("nodejs", "20"),
        "nodejs18.x" => ("nodejs", "18"),
        "nodejs16.x" => ("nodejs", "16"),
        "ruby4.0" => ("ruby", "4.0"),
        "ruby3.4" => ("ruby", "3.4"),
        "ruby3.3" => ("ruby", "3.3"),
        "ruby3.2" => ("ruby", "3.2"),
        "java25" => ("java", "25"),
        "java21" => ("java", "21"),
        "java17" => ("java", "17"),
        "java17.al2023" => ("java", "17.al2023"),
        "java11" => ("java", "11"),
        "java11.al2023" => ("java", "11.al2023"),
        "java8.al2023" => ("java", "8.al2023"),
        "java8.al2" => ("java", "8.al2"),
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
    let mib = tmpfs_size_mib(size);
    format!("/tmp:size={mib}m,exec")
}

/// The size of a function's `/tmp` tmpfs. Also part of its memory limit, because
/// the tmpfs is charged to the same cgroup.
fn tmpfs_size_mib(size: Option<i64>) -> i64 {
    size.unwrap_or(512).max(64)
}

/// Extract a ZIP archive to a destination directory.
/// An entry copy failed: a corrupt entry (the archive's fault) reads as
/// `InvalidData`; anything else is writing it out locally.
fn copy_error(e: std::io::Error) -> RuntimeError {
    if e.kind() == std::io::ErrorKind::InvalidData {
        RuntimeError::ZipExtractionFailed(e.to_string())
    } else {
        RuntimeError::LocalIo(e.to_string())
    }
}

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
            std::fs::create_dir_all(&out_path).map_err(|e| RuntimeError::LocalIo(e.to_string()))?;
        } else {
            if let Some(parent) = out_path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| RuntimeError::LocalIo(e.to_string()))?;
            }
            let mut out_file = std::fs::File::create(&out_path)
                .map_err(|e| RuntimeError::LocalIo(e.to_string()))?;
            std::io::copy(&mut file, &mut out_file).map_err(copy_error)?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                // Keep only the rwx bits: an archive must not be able to plant
                // setuid/setgid/sticky files in the mounted code directory.
                if let Some(mode) = file.unix_mode() {
                    std::fs::set_permissions(
                        &out_path,
                        std::fs::Permissions::from_mode(mode & 0o777),
                    )
                    .map_err(|e| RuntimeError::LocalIo(e.to_string()))?;
                }
            }
        }
    }
    Ok(())
}

fn build_local_registry_docker_config(server_port: u16) -> Option<TempDir> {
    let dir = TempDir::new().ok()?;
    let auth = base64::engine::general_purpose::STANDARD.encode("AWS:fakecloud-lambda-runtime");
    // Authorize every hostname fakecloud's ECR can be addressed by:
    // `127.0.0.1` on the host, `host.docker.internal` (Docker) and
    // `host.containers.internal` (podman) when fakecloud runs in a container and
    // the pull URI is rewritten to the sibling host. Centralized in
    // container_net so Lambda and ECS can't drift (bug-audit 2026-06-20, 0.B2).
    let auths: serde_json::Map<String, serde_json::Value> =
        fakecloud_core::container_net::registry_auth_hosts(server_port)
            .into_iter()
            .map(|host| (host, serde_json::json!({ "auth": auth })))
            .collect();
    let config = serde_json::json!({ "auths": auths });
    std::fs::write(dir.path().join("config.json"), config.to_string()).ok()?;
    Some(dir)
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;

    #[test]
    fn every_runtime_has_an_image_or_is_a_documented_exception() {
        for rt in crate::service::LAMBDA_RUNTIMES {
            let mapped = runtime_to_image(rt).is_some();
            let exempt = RUNTIMES_WITHOUT_IMAGE.contains(rt);
            assert!(
                mapped != exempt,
                "runtime {rt}: mapped={mapped} exempt={exempt}; map it to its \
                 public.ecr.aws/lambda image or document why none exists"
            );
        }
        for rt in RUNTIMES_WITHOUT_IMAGE {
            assert!(
                crate::service::LAMBDA_RUNTIMES.contains(rt),
                "{rt} is exempt but not an allowlisted runtime"
            );
        }
    }

    #[test]
    fn new_runtimes_map_to_their_base_images() {
        for (rt, image) in [
            ("ruby4.0", "ruby:4.0"),
            ("ruby3.2", "ruby:3.2"),
            ("java8.al2", "java:8.al2"),
            ("java8.al2023", "java:8.al2023"),
            ("java11.al2023", "java:11.al2023"),
            ("java17.al2023", "java:17.al2023"),
            ("nodejs26.x", "nodejs:26-preview"),
            ("python3.15", "python:3.15-preview"),
        ] {
            assert_eq!(
                runtime_to_image(rt),
                Some(format!("public.ecr.aws/lambda/{image}")),
                "{rt}"
            );
        }
    }

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

    #[cfg(unix)]
    /// A broken archive is the package's fault; failing to write it out is
    /// fakecloud's (a full disk, a missing directory), and reads as such.
    #[test]
    fn extract_zip_tells_a_bad_archive_from_a_local_failure() {
        let dir = TempDir::new().unwrap();
        assert!(matches!(
            extract_zip(b"not a zip", dir.path()),
            Err(RuntimeError::ZipExtractionFailed(_))
        ));

        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        writer
            .start_file(
                "nested/handler.py",
                zip::write::SimpleFileOptions::default(),
            )
            .unwrap();
        writer.write_all(b"x = 1\n").unwrap();
        let zip_bytes = writer.finish().unwrap().into_inner();
        // The destination is a file, so its directories cannot be created.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"").unwrap();
        assert!(matches!(
            extract_zip(&zip_bytes, &blocker),
            Err(RuntimeError::LocalIo(_))
        ));
    }

    #[test]
    fn extract_zip_strips_setuid_setgid_and_sticky_bits() {
        use std::os::unix::fs::PermissionsExt;

        let mut writer = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
        let options = zip::write::SimpleFileOptions::default().unix_permissions(0o755);
        writer.start_file("bootstrap", options).unwrap();
        writer.write_all(b"#!/bin/sh\n").unwrap();
        let mut zip_bytes = writer.finish().unwrap().into_inner();

        // The writer masks modes to 0o777, so set the setuid/setgid/sticky
        // bits by hand in the central directory's external attributes (the
        // high 16 bits carry the Unix mode), as a hostile archive would.
        let cd = zip_bytes
            .windows(4)
            .position(|w| w == [0x50, 0x4b, 0x01, 0x02])
            .expect("central directory header");
        let attrs = cd + 38;
        let mut ext = u32::from_le_bytes(zip_bytes[attrs..attrs + 4].try_into().unwrap());
        ext |= 0o7000 << 16;
        zip_bytes[attrs..attrs + 4].copy_from_slice(&ext.to_le_bytes());

        let dir = TempDir::new().unwrap();
        extract_zip(&zip_bytes, dir.path()).unwrap();

        let mode = std::fs::metadata(dir.path().join("bootstrap"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o7777, 0o755);
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

    #[test]
    fn memory_limit_covers_function_memory_and_tmp_with_no_swap() {
        assert_eq!(
            memory_limit_args(1024, Some(2048)),
            ["--memory", "3072m", "--memory-swap", "3072m"]
        );
        assert_eq!(
            memory_limit_args(i64::MAX, None)[1],
            format!("{}m", i64::MAX)
        );
        // Out-of-range stored values still get a usable limit.
        assert_eq!(
            memory_limit_args(0, None),
            ["--memory", "640m", "--memory-swap", "640m"]
        );
    }

    #[test]
    fn a_containerized_fakecloud_is_named_by_its_network_address() {
        let mut func: LambdaFunction = serde_json::from_value(serde_json::json!({
            "function_name": "f", "function_arn": "arn:aws:lambda:us-east-1:123456789012:function:f",
            "runtime": "python3.12", "role": "", "handler": "h", "description": "", "timeout": 3,
            "memory_size": 128, "code_sha256": "x", "code_size": 1, "version": "$LATEST",
            "last_modified": "2020-01-01T00:00:00Z", "tags": {}, "environment": {},
            "architectures": ["x86_64"], "package_type": "Zip", "code_zip": null, "policy": null
        }))
        .unwrap();
        func.environment
            .insert("AWS_ENDPOINT_URL".into(), "http://localhost:4566".into());
        func.environment
            .insert("DB".into(), "postgres://localhost:5432/app".into());
        let env = function_environment(
            &func,
            "http://172.20.0.3:4566",
            "host.docker.internal",
            "rt:4566",
            None,
        );
        let env = fakecloud_on_its_network(env, "host.docker.internal", 4566, "172.20.0.3");
        let get = |k: &str| {
            env.iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("AWS_ENDPOINT_URL"), Some("http://172.20.0.3:4566"));
        // Another port on loopback still names the host.
        assert_eq!(get("DB"), Some("postgres://host.docker.internal:5432/app"));
    }

    #[test]
    fn a_containerized_fakecloud_shares_its_own_network() {
        let compose = r#"[{"Id":"abc","NetworkSettings":{"Networks":{
            "bridge":{"IPAddress":"172.17.0.5"},
            "myapp_default":{"IPAddress":"172.20.0.3"}}}}]"#;
        assert_eq!(
            select_self_network(compose),
            Some(SelfNetwork {
                network: "myapp_default".into(),
                ip: "172.20.0.3".into()
            })
        );
        let bridge = r#"[{"NetworkSettings":{"Networks":{"bridge":{"IPAddress":"172.17.0.2"}}}}]"#;
        assert_eq!(
            select_self_network(bridge),
            Some(SelfNetwork {
                network: "bridge".into(),
                ip: "172.17.0.2".into()
            })
        );
        let host = r#"[{"NetworkSettings":{"Networks":{"host":{"IPAddress":""}}}}]"#;
        assert_eq!(select_self_network(host), None);
        assert_eq!(select_self_network("not json"), None);

        let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let docker = format!(
            "1234 25 0:30 /docker/containers/{id}/hostname /etc/hostname rw - ext4 /dev/vda1 rw"
        );
        assert_eq!(container_id_in_mountinfo(&docker).as_deref(), Some(id));
        let podman = format!(
            "1 2 0:3 /containers/storage/overlay-containers/{id}/userdata/hosts /etc/hosts rw"
        );
        assert_eq!(container_id_in_mountinfo(&podman).as_deref(), Some(id));
        assert_eq!(container_id_in_mountinfo("1 2 0:3 / / rw"), None);
    }

    #[test]
    fn runtime_api_hosts_map_where_the_alias_does() {
        assert_eq!(
            known_runtime_api_target(Some("host.docker.internal:host-gateway"), None).as_deref(),
            Some("host-gateway")
        );
        assert_eq!(
            known_runtime_api_target(Some("host.docker.internal:172.17.0.1"), None).as_deref(),
            Some("172.17.0.1")
        );
        assert_eq!(
            known_runtime_api_target(None, Some("192.168.65.254".into())).as_deref(),
            Some("192.168.65.254")
        );
        assert_eq!(known_runtime_api_target(None, None), None);

        let getent = "192.168.127.254 STREAM host.containers.internal\n\
                      192.168.127.254 DGRAM  \n";
        assert_eq!(
            alias_address(getent, "host.containers.internal").as_deref(),
            Some("192.168.127.254")
        );
        let hosts = "127.0.0.1 localhost\n::1 localhost\n\
                     10.88.0.1 host.containers.internal host.docker.internal\n";
        assert_eq!(
            alias_address(hosts, "host.containers.internal").as_deref(),
            Some("10.88.0.1")
        );
        assert_eq!(alias_address(hosts, "nope"), None);
    }

    #[test]
    fn credentials_stay_off_the_command_line() {
        let env = vec![
            ("AWS_REGION".to_string(), "us-east-1".to_string()),
            ("AWS_ACCESS_KEY_ID".to_string(), "FSIAKEY".to_string()),
            ("AWS_SECRET_ACCESS_KEY".to_string(), "s3cr3t".to_string()),
            ("AWS_SESSION_TOKEN".to_string(), "tok".to_string()),
        ];
        let (args, child_env) = docker_env_args(env);
        assert_eq!(
            args,
            vec![
                "-e",
                "AWS_REGION=us-east-1",
                "-e",
                "AWS_ACCESS_KEY_ID",
                "-e",
                "AWS_SECRET_ACCESS_KEY",
                "-e",
                "AWS_SESSION_TOKEN"
            ]
        );
        assert!(args
            .iter()
            .all(|a| !a.contains("s3cr3t") && !a.contains("tok") && !a.contains("FSIAKEY")));
        assert_eq!(
            child_env,
            vec![
                ("AWS_ACCESS_KEY_ID".to_string(), "FSIAKEY".to_string()),
                ("AWS_SECRET_ACCESS_KEY".to_string(), "s3cr3t".to_string()),
                ("AWS_SESSION_TOKEN".to_string(), "tok".to_string()),
            ]
        );
    }

    /// A CLI that exits without reading its input (as `cp -` does when it
    /// refuses the target) breaks the pipe; the error still carries what it
    /// said on stderr.
    #[tokio::test]
    async fn a_cli_that_exits_early_reports_its_stderr() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let script = |name: &str, body: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, body).unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
            path.to_str().unwrap().to_string()
        };
        let refuses = script(
            "refuses",
            "#!/bin/sh\necho 'Error: No such container: abc' >&2\nexit 1\n",
        );
        let accepts = script("accepts", "#!/bin/sh\ncat > /dev/null\n");
        // Far more than a pipe buffer, so the write cannot complete.
        let input = vec![0u8; 8 << 20];
        let err = run_with_stdin(&refuses, &["cp", "-", "abc:/var"], &input)
            .await
            .unwrap_err();
        assert!(err.contains("No such container: abc"), "{err}");
        run_with_stdin(&accepts, &["cp", "-", "abc:/var"], &input)
            .await
            .unwrap();
    }
}
