//! Backing-container runtime for Amazon Managed Service for Apache Flink
//! (`kinesisanalyticsv2`).
//!
//! A **Flink-flavor** application (`RuntimeEnvironment` `FLINK-1_x`, with a JAR
//! in S3) is backed by a REAL Apache Flink session cluster running in a Docker
//! container: the application's code JAR is submitted to the cluster's
//! JobManager over its REST API and the application only becomes `RUNNING` once
//! Flink reports the job `RUNNING`. `StopApplication` cancels the real Flink
//! job. This is the same data-plane bar Amazon MQ / MSK / RDS / ElastiCache /
//! Lambda meet -- not a formatted-but-dead dashboard URL.
//!
//! Image: `flink:1.19` (the official Apache Flink image; ships the REST API and
//! bundled example JARs under `/opt/flink/examples/`), overridable via
//! `FAKECLOUD_KINESISANALYTICSV2_IMAGE`.
//!
//! Session mode, a SINGLE container running both the JobManager and a
//! TaskManager via `start-cluster.sh`. The Flink REST port `8081` is published
//! to a pre-allocated fixed free host port so external clients (the E2E suite,
//! the presigned dashboard URL) can reach it. `flink:1.19`'s default
//! `config.yaml` already binds the REST endpoint to `0.0.0.0`, so no config
//! rewrite is needed. `host` is the shared [`fakecloud_core::container_net`]
//! sibling host (`127.0.0.1` normally, `host.docker.internal` /
//! `host.containers.internal` when fakecloud is itself containerized), so the
//! reach address can't drift from the issue #1539 portability fixes.
//!
//! **SQL-flavor** applications (`RuntimeEnvironment` `SQL-1_0`) do NOT get a real
//! runtime: running arbitrary SQL as a live Flink job needs the SQL gateway,
//! which is out of scope. SQL apps keep the control-plane state machine and this
//! split is documented honestly in the service module + the website doc.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

/// A running application's backing Flink session cluster.
#[derive(Debug, Clone)]
pub struct RunningFlink {
    pub container_id: String,
    /// Address clients reach the published Flink REST port at (`127.0.0.1` or
    /// the sibling host alias when fakecloud is containerized).
    pub host: String,
    /// Published host port mapped to the container's Flink REST port (8081).
    pub rest_port: u16,
}

impl RunningFlink {
    /// The reachable Flink Web Dashboard / REST base URL.
    pub fn dashboard_url(&self) -> String {
        format!("http://{}:{}", self.host, self.rest_port)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("container runtime is unavailable")]
    Unavailable,
    #[error("Flink cluster container failed to start: {0}")]
    ContainerStartFailed(String),
}

/// A job-submission / job-control failure against the live Flink cluster.
#[derive(Debug, thiserror::Error)]
pub enum JobError {
    #[error("failed to reach the Flink REST API: {0}")]
    Http(String),
    #[error("Flink rejected the job: {0}")]
    Rejected(String),
    #[error("job {0} was not found on the cluster")]
    NotFound(String),
}

/// The Flink REST port inside the container.
const FLINK_REST_PORT: u16 = 8081;

/// Docker/Podman-backed single-container Flink session-cluster runtime.
#[derive(Debug, Clone)]
pub struct FlinkRuntime {
    cli: String,
    net: fakecloud_core::container_net::HostNetworking,
    instance_id: String,
    http: reqwest::Client,
    /// Application ARN -> running Flink cluster, for describe/stop/reattach.
    containers: Arc<RwLock<HashMap<String, RunningFlink>>>,
}

impl FlinkRuntime {
    /// Construct the Docker/Podman runtime. Returns `None` when no container CLI
    /// is available (fakecloud then degrades to the control-plane-only state
    /// machine), or when the real backend is explicitly disabled via
    /// `FAKECLOUD_KINESISANALYTICSV2_DISABLE_BACKEND` (the shared-partition
    /// control-plane E2E and the tfacc harness set this: they assert the AWS
    /// response *format* and must not spawn a heavy Flink container).
    pub fn new() -> Option<Self> {
        if std::env::var("FAKECLOUD_KINESISANALYTICSV2_DISABLE_BACKEND")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        {
            return None;
        }
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        let net = fakecloud_core::container_net::HostNetworking::detect(&cli);
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .ok()?;
        Some(Self {
            cli,
            net,
            instance_id: format!("fakecloud-{}", std::process::id()),
            http,
            containers: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Name of the active container CLI, for logging.
    pub fn cli_name(&self) -> &str {
        &self.cli
    }

    /// The Flink image, overridable via `FAKECLOUD_KINESISANALYTICSV2_IMAGE`.
    fn image(&self) -> String {
        std::env::var("FAKECLOUD_KINESISANALYTICSV2_IMAGE")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "flink:1.19".to_string())
    }

    /// Spawn (or return the already-tracked) backing Flink session cluster for
    /// an application and block until its REST API reports a free task slot.
    pub async fn ensure_cluster(&self, app_arn: &str) -> Result<RunningFlink, RuntimeError> {
        if let Some(existing) = self.containers.read().get(app_arn).cloned() {
            return Ok(existing);
        }
        let running = self.spawn_container(app_arn).await?;
        self.containers
            .write()
            .insert(app_arn.to_string(), running.clone());
        Ok(running)
    }

    /// Re-attach to an application's PERSISTED Flink cluster container after a
    /// fakecloud restart rather than spawning a fresh empty one. `Ok(None)` when
    /// the container is truly gone (the caller then marks the app `READY`),
    /// `Err` on a transient daemon failure, `Ok(Some(..))` once it is ready.
    pub async fn reattach_cluster(
        &self,
        app_arn: &str,
        container_id: &str,
    ) -> Result<Option<RunningFlink>, RuntimeError> {
        let inspect = tokio::process::Command::new(&self.cli)
            .args(["inspect", "--format", "{{.State.Status}}", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !inspect.status.success() {
            return Ok(None);
        }
        let start = tokio::process::Command::new(&self.cli)
            .args(["start", container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !start.status.success() {
            let stderr = String::from_utf8_lossy(&start.stderr);
            if stderr.contains("No such container") || stderr.contains("no such container") {
                return Ok(None);
            }
            return Err(RuntimeError::ContainerStartFailed(format!(
                "reattach start failed: {}",
                stderr.trim()
            )));
        }
        let rest_port = self.lookup_port(container_id, FLINK_REST_PORT).await?;
        let running = RunningFlink {
            container_id: container_id.to_string(),
            host: self.net.sibling_host.clone(),
            rest_port,
        };
        self.wait_for_ready(&running, container_id).await?;
        self.containers
            .write()
            .insert(app_arn.to_string(), running.clone());
        tracing::info!(
            app_arn = %app_arn,
            container_id = %container_id,
            "re-attached persisted Flink session-cluster container",
        );
        Ok(Some(running))
    }

    /// Stop + remove an application's backing container and drop its tracking
    /// entry. Called on the application's last stop and on delete.
    pub async fn stop_cluster(&self, app_arn: &str) {
        let running = self.containers.write().remove(app_arn);
        if let Some(running) = running {
            self.remove_container(&running.container_id).await;
        }
    }

    /// Remove a container by id without a tracking entry (delete-while-tracked).
    pub async fn remove_by_id(&self, container_id: &str) {
        self.remove_container(container_id).await;
    }

    /// Stop every tracked cluster container (graceful shutdown / reset).
    pub async fn stop_all(&self) {
        let containers: Vec<RunningFlink> = {
            let mut map = self.containers.write();
            map.drain().map(|(_, c)| c).collect()
        };
        for c in containers {
            self.remove_container(&c.container_id).await;
        }
    }

    async fn spawn_container(&self, app_arn: &str) -> Result<RunningFlink, RuntimeError> {
        // Reap a leaked container left by a PRIOR failed bring-up of THIS app,
        // scoped to this app AND this fakecloud instance.
        self.reap_stale_containers(app_arn).await;

        // Allocate a fixed free host port FIRST so the published REST port is
        // stable across a restart (re-read on reattach for good measure).
        let rest_port = alloc_free_port()?;

        let mut args: Vec<String> = vec!["create".to_string()];
        args.push("-p".to_string());
        args.push(format!("{rest_port}:{FLINK_REST_PORT}"));
        args.push("--label".to_string());
        args.push(format!("fakecloud-flink={app_arn}"));
        args.push("--label".to_string());
        args.push(format!("fakecloud-instance={}", self.instance_id));
        self.net.push_add_host_args(&mut args);
        // Single container: start the JobManager + a local TaskManager, then
        // hold the container open by tailing the cluster logs.
        args.push("--entrypoint".to_string());
        args.push("bash".to_string());
        args.push(self.image());
        args.push("-c".to_string());
        args.push("/opt/flink/bin/start-cluster.sh && tail -f /opt/flink/log/*.log".to_string());

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

        let start = tokio::process::Command::new(&self.cli)
            .args(["start", &container_id])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !start.status.success() {
            let diag = self.capture_container_diagnostics(&container_id).await;
            return Err(RuntimeError::ContainerStartFailed(format!(
                "container start failed: {}; {diag}",
                String::from_utf8_lossy(&start.stderr).trim()
            )));
        }

        let running = RunningFlink {
            container_id: container_id.clone(),
            host: self.net.sibling_host.clone(),
            rest_port,
        };
        self.wait_for_ready(&running, &container_id).await?;

        tracing::info!(
            app_arn = %app_arn,
            container_id = %container_id,
            host = %running.host,
            rest_port = rest_port,
            "Flink session-cluster container started",
        );
        Ok(running)
    }

    /// Read the host port the container's REST port (8081) is published on.
    async fn lookup_port(
        &self,
        container_id: &str,
        container_port: u16,
    ) -> Result<u16, RuntimeError> {
        let out = tokio::process::Command::new(&self.cli)
            .args(["port", container_id, &container_port.to_string()])
            .output()
            .await
            .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?;
        if !out.status.success() {
            return Err(RuntimeError::ContainerStartFailed(format!(
                "port lookup for {container_port} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            )));
        }
        let s = String::from_utf8_lossy(&out.stdout);
        s.lines()
            .next()
            .and_then(|l| l.trim().rsplit(':').next())
            .and_then(|p| p.parse::<u16>().ok())
            .ok_or_else(|| {
                RuntimeError::ContainerStartFailed(format!(
                    "could not determine host port for {container_port} from '{}'",
                    s.trim()
                ))
            })
    }

    /// Block until the Flink cluster answers its REST API AND a task slot is
    /// available (the JobManager REST endpoint binds before the TaskManager
    /// registers, so `/overview` answering is not enough to submit a job). On
    /// timeout the container's recent logs + terminal state are surfaced so a CI
    /// failure is diagnosable.
    async fn wait_for_ready(
        &self,
        running: &RunningFlink,
        container_id: &str,
    ) -> Result<(), RuntimeError> {
        // 240 * 500ms = ~120s (plus per-probe time): generous for a cold Flink
        // cluster boot + TaskManager registration on a constrained CI runner.
        const ATTEMPTS: u32 = 240;
        let start = std::time::Instant::now();
        let url = format!("{}/overview", running.dashboard_url());
        for _ in 0..ATTEMPTS {
            if let Ok(resp) = self.http.get(&url).send().await {
                if let Ok(v) = resp.json::<serde_json::Value>().await {
                    let slots = v
                        .get("slots-available")
                        .and_then(serde_json::Value::as_i64)
                        .unwrap_or(0);
                    if slots >= 1 {
                        return Ok(());
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        Err(RuntimeError::ContainerStartFailed(
            self.readiness_timeout_reason(container_id, start).await,
        ))
    }

    // ===================== job operations =====================

    /// Upload a job JAR to the cluster via `POST /jars/upload` (multipart) and
    /// return the Flink jar id (the basename of the stored filename).
    pub async fn upload_jar(
        &self,
        running: &RunningFlink,
        jar_bytes: Vec<u8>,
        file_name: &str,
    ) -> Result<String, JobError> {
        let part = reqwest::multipart::Part::bytes(jar_bytes)
            .file_name(file_name.to_string())
            .mime_str("application/x-java-archive")
            .map_err(|e| JobError::Http(e.to_string()))?;
        let form = reqwest::multipart::Form::new().part("jarfile", part);
        let url = format!("{}/jars/upload", running.dashboard_url());
        let resp = self
            .http
            .post(&url)
            .multipart(form)
            .send()
            .await
            .map_err(|e| JobError::Http(e.to_string()))?;
        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| JobError::Http(e.to_string()))?;
        if !status.is_success() || body.get("status").and_then(|s| s.as_str()) != Some("success") {
            return Err(JobError::Rejected(sanitize(&body.to_string())));
        }
        let filename = body
            .get("filename")
            .and_then(|s| s.as_str())
            .ok_or_else(|| JobError::Rejected("upload response missing filename".to_string()))?;
        Ok(filename.rsplit('/').next().unwrap_or(filename).to_string())
    }

    /// Run an uploaded jar via `POST /jars/{jar_id}/run` and return the Flink
    /// job id. `parallelism`, `program_args`, and `entry_class` are forwarded
    /// when present (from the application's Flink/Environment configuration).
    pub async fn run_job(
        &self,
        running: &RunningFlink,
        jar_id: &str,
        parallelism: Option<i64>,
        program_args: Option<&str>,
        entry_class: Option<&str>,
    ) -> Result<String, JobError> {
        let mut payload = serde_json::Map::new();
        if let Some(p) = parallelism {
            payload.insert("parallelism".into(), serde_json::json!(p.max(1)));
        }
        if let Some(a) = program_args.filter(|s| !s.trim().is_empty()) {
            payload.insert("programArgs".into(), serde_json::json!(a));
        }
        if let Some(c) = entry_class.filter(|s| !s.trim().is_empty()) {
            payload.insert("entryClass".into(), serde_json::json!(c));
        }
        let url = format!("{}/jars/{}/run", running.dashboard_url(), jar_id);
        let resp = self
            .http
            .post(&url)
            .json(&serde_json::Value::Object(payload))
            .send()
            .await
            .map_err(|e| JobError::Http(e.to_string()))?;
        let status = resp.status();
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| JobError::Http(e.to_string()))?;
        if !status.is_success() {
            return Err(JobError::Rejected(sanitize(&body.to_string())));
        }
        body.get("jobid")
            .and_then(|s| s.as_str())
            .map(str::to_string)
            .ok_or_else(|| JobError::Rejected(sanitize(&body.to_string())))
    }

    /// Query a Flink job's lifecycle state via `GET /jobs/{job_id}` -> the raw
    /// Flink `state` string (`RUNNING`, `FINISHED`, `FAILED`, `CANCELED`, ...).
    pub async fn job_state(
        &self,
        running: &RunningFlink,
        job_id: &str,
    ) -> Result<String, JobError> {
        let url = format!("{}/jobs/{}", running.dashboard_url(), job_id);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| JobError::Http(e.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(JobError::NotFound(job_id.to_string()));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| JobError::Http(e.to_string()))?;
        body.get("state")
            .and_then(|s| s.as_str())
            .map(str::to_string)
            .ok_or_else(|| JobError::NotFound(job_id.to_string()))
    }

    /// Cancel a running Flink job via `PATCH /jobs/{job_id}?mode=cancel`.
    pub async fn cancel_job(&self, running: &RunningFlink, job_id: &str) -> Result<(), JobError> {
        let url = format!("{}/jobs/{}?mode=cancel", running.dashboard_url(), job_id);
        let resp = self
            .http
            .patch(&url)
            .send()
            .await
            .map_err(|e| JobError::Http(e.to_string()))?;
        // 202 Accepted is the normal cancel response; a 404 means the job is
        // already gone, which is fine for a cancel.
        if resp.status().is_success() || resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(());
        }
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        Err(JobError::Rejected(format!(
            "cancel returned {status}: {}",
            sanitize(&text)
        )))
    }

    // ===================== exec / cleanup plumbing =====================

    async fn remove_container(&self, container_id: &str) {
        let _ = tokio::process::Command::new(&self.cli)
            .args(["rm", "-f", container_id])
            .output()
            .await;
    }

    /// Remove any container labeled for THIS app AND this fakecloud instance.
    async fn reap_stale_containers(&self, app_arn: &str) {
        let Ok(out) = tokio::process::Command::new(&self.cli)
            .args([
                "ps",
                "-aq",
                "--filter",
                &format!("label=fakecloud-flink={app_arn}"),
                "--filter",
                &format!("label=fakecloud-instance={}", self.instance_id),
            ])
            .output()
            .await
        else {
            return;
        };
        if !out.status.success() {
            return;
        }
        for id in String::from_utf8_lossy(&out.stdout).split_whitespace() {
            self.remove_container(id).await;
        }
    }

    async fn readiness_timeout_reason(
        &self,
        container_id: &str,
        started: std::time::Instant,
    ) -> String {
        let state = tokio::process::Command::new(&self.cli)
            .args([
                "inspect",
                "--format",
                "status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}}",
                container_id,
            ])
            .output()
            .await
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let exited = state.contains("status=exited");
        let logs = self.container_logs(container_id, 60).await;
        let headline = if exited {
            "Flink cluster exited during boot before a task slot became available".to_string()
        } else {
            "Flink cluster did not report an available task slot within 120 seconds; the container is still running".to_string()
        };
        let reason =
            format!("{headline}; container state [{state}]; recent container logs:\n{logs}");
        tracing::error!(
            container_id = %container_id,
            elapsed_secs = started.elapsed().as_secs(),
            container_state = %state,
            reason = %headline,
            "Flink cluster readiness timed out",
        );
        reason
    }

    async fn capture_container_diagnostics(&self, container_id: &str) -> String {
        let state = tokio::process::Command::new(&self.cli)
            .args([
                "inspect",
                "--format",
                "status={{.State.Status}} exit={{.State.ExitCode}} oom={{.State.OOMKilled}} error={{.State.Error}}",
                container_id,
            ])
            .output()
            .await
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .unwrap_or_default();
        let logs = self.container_logs(container_id, 80).await;
        format!("container state [{state}]; recent container logs:\n{logs}")
    }

    async fn container_logs(&self, container_id: &str, tail: u32) -> String {
        tokio::process::Command::new(&self.cli)
            .args(["logs", "--tail", &tail.to_string(), container_id])
            .output()
            .await
            .map(|o| {
                let mut s = String::from_utf8_lossy(&o.stdout).into_owned();
                s.push_str(&String::from_utf8_lossy(&o.stderr));
                s.trim().to_string()
            })
            .unwrap_or_else(|e| format!("<container logs failed: {e}>"))
    }
}

/// Map a raw Flink job `state` to the corresponding KDA/MSF application status.
/// A live `RUNNING` (or transiently `RESTARTING`/`CREATED`) job keeps the
/// application `RUNNING`; any terminal state (canceled, finished, failed) means
/// the job is no longer running, so the application is back to `READY` -- the
/// application-status enum has no `FAILED` member, so `READY` is the faithful
/// steady state a stopped/finished job settles to.
pub fn flink_state_to_app_status(state: &str) -> &'static str {
    match state {
        "RUNNING" | "RESTARTING" | "CREATED" | "RECONCILING" => "RUNNING",
        _ => "READY",
    }
}

/// Whether a raw Flink job `state` is terminal (no further transitions).
pub fn is_terminal_flink_state(state: &str) -> bool {
    matches!(state, "FINISHED" | "CANCELED" | "FAILED" | "SUSPENDED")
}

/// Bind a `127.0.0.1:0` listener, read the assigned port, and drop it -- the
/// standard free-port trick. The port is then published with `-p {P}:8081`.
fn alloc_free_port() -> Result<u16, RuntimeError> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).map_err(|e| {
        RuntimeError::ContainerStartFailed(format!("could not allocate a host port: {e}"))
    })?;
    let port = listener
        .local_addr()
        .map_err(|e| RuntimeError::ContainerStartFailed(e.to_string()))?
        .port();
    drop(listener);
    Ok(port)
}

/// Sanitize multi-line REST/daemon output for embedding in an error string or
/// header: take the most informative line, strip control chars, bound length.
/// NEVER put raw multi-line output into a response header (issue #1539 Bug 2).
fn sanitize(s: &str) -> String {
    let line = s
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("")
        .replace(|c: char| c.is_control(), " ");
    let line = line.trim();
    if line.len() > 300 {
        format!("{}...", &line[..300])
    } else {
        line.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_free_port_returns_a_usable_port() {
        let p = alloc_free_port().expect("port");
        assert!(p >= 1024, "expected an ephemeral port, got {p}");
        let _ = alloc_free_port().expect("second port");
    }

    #[test]
    fn dashboard_url_projects_host_and_port() {
        let r = RunningFlink {
            container_id: "abc".into(),
            host: "127.0.0.1".into(),
            rest_port: 58192,
        };
        assert_eq!(r.dashboard_url(), "http://127.0.0.1:58192");
    }

    #[test]
    fn dashboard_url_uses_sibling_host_when_containerized() {
        let r = RunningFlink {
            container_id: "abc".into(),
            host: "host.docker.internal".into(),
            rest_port: 5000,
        };
        assert_eq!(r.dashboard_url(), "http://host.docker.internal:5000");
    }

    #[test]
    fn running_flink_state_maps_to_running() {
        assert_eq!(flink_state_to_app_status("RUNNING"), "RUNNING");
        assert_eq!(flink_state_to_app_status("RESTARTING"), "RUNNING");
    }

    #[test]
    fn terminal_flink_states_map_to_ready() {
        for s in ["FINISHED", "CANCELED", "FAILED", "SUSPENDED"] {
            assert_eq!(flink_state_to_app_status(s), "READY", "{s}");
            assert!(is_terminal_flink_state(s), "{s} is terminal");
        }
        assert!(!is_terminal_flink_state("RUNNING"));
    }

    #[test]
    fn disable_backend_env_returns_no_runtime() {
        // Explicit disable -> None regardless of a container CLI being present.
        temp_env_set("FAKECLOUD_KINESISANALYTICSV2_DISABLE_BACKEND", "1");
        assert!(FlinkRuntime::new().is_none());
        temp_env_unset("FAKECLOUD_KINESISANALYTICSV2_DISABLE_BACKEND");
    }

    #[test]
    fn sanitize_strips_control_chars_and_bounds() {
        let raw = "line one\n\tERROR something bad\nmore";
        let s = sanitize(raw);
        assert!(!s.contains('\n'));
        assert!(!s.contains('\t'));
        assert_eq!(s, "line one");
    }

    fn temp_env_set(k: &str, v: &str) {
        // SAFETY: single-threaded unit test mutating a process env var.
        unsafe { std::env::set_var(k, v) };
    }
    fn temp_env_unset(k: &str) {
        // SAFETY: single-threaded unit test mutating a process env var.
        unsafe { std::env::remove_var(k) };
    }
}
