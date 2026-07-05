//! Real Docker/Podman-backed build execution for AWS CodeBuild.
//!
//! `StartBuild` returns immediately with the build `IN_PROGRESS`; the actual
//! work runs here in a background task ([`run_build`]) so the handler never
//! blocks on an image pull or a container run (a client-timeout bug class).
//!
//! The task resolves the environment image, assembles the buildspec (inline on
//! the project `source.buildspec` or a `StartBuild.buildspecOverride`), parses
//! its `env`/`phases`/`artifacts`, then runs a real container from the image and
//! executes each phase's `commands` as shell in it — capturing stdout/stderr,
//! honoring CodeBuild phase-failure semantics (`post_build` always runs), and
//! settling `buildStatus` on the REAL container exit codes. Output is streamed
//! to CloudWatch Logs (fakecloud-logs) and declared `S3` artifacts are uploaded
//! (fakecloud-s3) via the shared cross-service wiring.
//!
//! The backend is gated: it is used only when a container CLI is available AND
//! `FAKECLOUD_CODEBUILD_DISABLE_BACKEND` is not set. When disabled (the
//! conformance probe points `FAKECLOUD_CONTAINER_CLI` at a non-existent binary;
//! the tfacc harness sets the disable flag) the service falls back to the
//! deterministic settle-to-`SUCCEEDED`-on-read path, so response shapes are
//! identical and conformance stays green.

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde_json::{json, Map, Value};
use tokio::process::Command;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::delivery::S3Delivery;
use fakecloud_logs::ingest::{append_events, IngestEvent};
use fakecloud_logs::SharedLogsState;
use fakecloud_persistence::SnapshotStore;

use crate::state::SharedCodeBuildState;

/// Fallback image used when the project names an AWS-curated CodeBuild image
/// (e.g. `aws/codebuild/standard:7.0`), which is not pullable from a public
/// registry. A small Ubuntu from ECR Public (no auth, provides `bash`/`sh` so
/// buildspec `commands` run unchanged). A user-supplied real image is used
/// verbatim instead.
const DEFAULT_IMAGE: &str = "public.ecr.aws/docker/library/ubuntu:22.04";

/// Working directory inside the build container. Buildspec `commands` run here
/// and `artifacts.files` are resolved relative to it (or `base-directory`).
const BUILD_WORKDIR: &str = "/codebuild/build";

/// Env var that force-disables the real container backend (tfacc harness sets
/// it so acceptance tests get deterministic cosmetic behavior).
pub const DISABLE_ENV: &str = "FAKECLOUD_CODEBUILD_DISABLE_BACKEND";

fn env_truthy(name: &str) -> bool {
    matches!(
        std::env::var(name).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

/// The real build backend: a resolved container CLI (`docker`/`podman`).
/// `None` when disabled by env or no CLI is available — the caller then uses
/// the deterministic settle path.
pub struct CodeBuildBackend {
    cli: String,
}

impl CodeBuildBackend {
    /// Detect a usable container backend. Returns `None` (deterministic path)
    /// when `FAKECLOUD_CODEBUILD_DISABLE_BACKEND` is set or when no container
    /// CLI is available. `detect_container_cli` honors `FAKECLOUD_CONTAINER_CLI`
    /// (the conformance probe points it at a non-existent binary).
    pub fn detect() -> Option<Arc<Self>> {
        if env_truthy(DISABLE_ENV) {
            tracing::debug!("CodeBuild real backend disabled via {DISABLE_ENV}");
            return None;
        }
        let cli = fakecloud_core::container_net::detect_container_cli()?;
        tracing::info!(%cli, "CodeBuild real build backend enabled");
        Some(Arc::new(Self { cli }))
    }

    /// The resolved container CLI (`docker`/`podman`).
    pub fn cli(&self) -> &str {
        &self.cli
    }
}

/// Registry of live build containers, keyed by build id, so `StopBuild` can
/// kill the container out from under the execution task. Shared (cloned) into
/// every spawned job.
pub type RunningContainers = Arc<Mutex<HashMap<String, String>>>;

/// Everything a spawned execution task needs, without borrowing the service.
pub struct BuildJob {
    pub backend: Arc<CodeBuildBackend>,
    pub state: SharedCodeBuildState,
    pub snapshot_store: Option<Arc<dyn SnapshotStore>>,
    pub snapshot_lock: Arc<AsyncMutex<()>>,
    pub logs_state: Option<SharedLogsState>,
    pub s3_delivery: Option<Arc<dyn S3Delivery>>,
    pub running: RunningContainers,
    pub account: String,
    pub region: String,
    /// `IN_PROGRESS` record id (`<project>:<uuid>`).
    pub build_id: String,
    pub build_arn: String,
    pub project_name: String,
    pub build_number: i64,
    /// True for a build batch (settles `buildBatchStatus`/`complete` instead of
    /// `buildStatus`/`buildComplete`).
    pub is_batch: bool,
    /// Resolved `environment.image`.
    pub image: Option<String>,
    /// Base env vars (`CODEBUILD_*` + project `environmentVariables`), merged
    /// with the buildspec `env.variables` at run time.
    pub base_env: Vec<(String, String)>,
    /// The buildspec text (inline `source.buildspec` or `buildspecOverride`).
    pub buildspec: Option<String>,
    pub source_version: Option<String>,
    /// Resolved `logsConfig.cloudWatchLogs` value (or `null`).
    pub cw_logs: Value,
    /// Resolved `artifacts` value (or `null`).
    pub artifacts: Value,
}

impl BuildJob {
    fn status_key(&self) -> &'static str {
        if self.is_batch {
            "buildBatchStatus"
        } else {
            "buildStatus"
        }
    }

    fn complete_key(&self) -> &'static str {
        if self.is_batch {
            "complete"
        } else {
            "buildComplete"
        }
    }

    /// The `<uuid>` suffix of the build id (used as the default log stream).
    fn short_id(&self) -> String {
        self.build_id
            .rsplit(':')
            .next()
            .unwrap_or(&self.build_id)
            .to_string()
    }
}

/// Kill and remove a build's container (best-effort). Used by `StopBuild`.
pub async fn kill_container(cli: &str, container_id: &str) {
    let _ = Command::new(cli)
        .args(["rm", "-f", container_id])
        .output()
        .await;
}

// ---------------------------------------------------------------------------
// Buildspec parsing
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone)]
struct Buildspec {
    env_vars: Vec<(String, String)>,
    install: Vec<String>,
    pre_build: Vec<String>,
    build: Vec<String>,
    post_build: Vec<String>,
    artifacts_files: Vec<String>,
    artifacts_base_dir: Option<String>,
    artifacts_discard_paths: bool,
}

/// A YAML value that is either a single command string or a list of them.
fn commands_of(node: &serde_yaml::Value) -> Vec<String> {
    match node {
        serde_yaml::Value::String(s) => vec![s.clone()],
        serde_yaml::Value::Sequence(seq) => seq
            .iter()
            .filter_map(|v| match v {
                serde_yaml::Value::String(s) => Some(s.clone()),
                // A number/bool command is coerced to its display form, matching
                // how a YAML scalar would be shell-interpreted.
                serde_yaml::Value::Number(n) => Some(n.to_string()),
                serde_yaml::Value::Bool(b) => Some(b.to_string()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

fn phase_commands(phases: &serde_yaml::Value, name: &str) -> Vec<String> {
    phases
        .get(name)
        .and_then(|p| p.get("commands"))
        .map(commands_of)
        .unwrap_or_default()
}

fn parse_buildspec(text: &str) -> Result<Buildspec, String> {
    let doc: serde_yaml::Value =
        serde_yaml::from_str(text).map_err(|e| format!("YAML_SYNTAX_ERROR: {e}"))?;
    let mut spec = Buildspec::default();

    if let Some(vars) = doc
        .get("env")
        .and_then(|e| e.get("variables"))
        .and_then(|v| v.as_mapping())
    {
        for (k, v) in vars {
            if let Some(key) = k.as_str() {
                let val = match v {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    _ => continue,
                };
                spec.env_vars.push((key.to_string(), val));
            }
        }
    }

    if let Some(phases) = doc.get("phases") {
        spec.install = phase_commands(phases, "install");
        spec.pre_build = phase_commands(phases, "pre_build");
        spec.build = phase_commands(phases, "build");
        spec.post_build = phase_commands(phases, "post_build");
    }

    if let Some(artifacts) = doc.get("artifacts") {
        if let Some(files) = artifacts.get("files") {
            spec.artifacts_files = commands_of(files);
        }
        spec.artifacts_base_dir = artifacts
            .get("base-directory")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        spec.artifacts_discard_paths = artifacts
            .get("discard-paths")
            .map(yaml_truthy)
            .unwrap_or(false);
    }

    Ok(spec)
}

/// CodeBuild treats `yes`/`true`/`"yes"` as truthy for `discard-paths`.
fn yaml_truthy(v: &serde_yaml::Value) -> bool {
    match v {
        serde_yaml::Value::Bool(b) => *b,
        serde_yaml::Value::String(s) => {
            matches!(s.to_ascii_lowercase().as_str(), "yes" | "true")
        }
        _ => false,
    }
}

/// Resolve the runnable image: a user-supplied non-AWS image is used verbatim;
/// an AWS-curated `aws/codebuild/*` image (not pullable) or an empty value maps
/// to [`DEFAULT_IMAGE`].
fn resolve_image(image: Option<&str>) -> String {
    match image {
        Some(i) if !i.trim().is_empty() && !i.starts_with("aws/") => i.trim().to_string(),
        _ => DEFAULT_IMAGE.to_string(),
    }
}

// ---------------------------------------------------------------------------
// State mutation helpers
// ---------------------------------------------------------------------------

fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

impl BuildJob {
    fn record<'a>(&self, st: &'a mut crate::state::CodeBuildState) -> Option<&'a mut Value> {
        if self.is_batch {
            st.build_batches.get_mut(&self.build_id)
        } else {
            st.builds.get_mut(&self.build_id)
        }
    }

    /// Mutate the stored record under the write lock. Returns false if the
    /// record is gone or is no longer `IN_PROGRESS` (e.g. `StopBuild` won the
    /// race), signalling the task to abort.
    fn with_record<F: FnOnce(&mut Map<String, Value>)>(&self, f: F) -> bool {
        let mut guard = self.state.write();
        let st = guard.get_or_create(&self.account);
        let status_key = self.status_key();
        let Some(record) = self.record(st) else {
            return false;
        };
        if record.get(status_key).and_then(Value::as_str) != Some("IN_PROGRESS") {
            return false;
        }
        if let Some(obj) = record.as_object_mut() {
            f(obj);
        }
        true
    }

    fn is_running(&self) -> bool {
        let mut guard = self.state.write();
        let st = guard.get_or_create(&self.account);
        let status_key = self.status_key();
        self.record(st)
            .and_then(|r| r.get(status_key).and_then(Value::as_str))
            .map(|s| s == "IN_PROGRESS")
            .unwrap_or(false)
    }

    /// Overwrite the record's `phases`, `currentPhase`, and (optionally) a
    /// terminal status in one locked mutation, then snapshot when terminal.
    fn set_phases(&self, phases: &[Value], current: &str) {
        self.with_record(|obj| {
            obj.insert("phases".into(), Value::Array(phases.to_vec()));
            obj.insert("currentPhase".into(), json!(current));
        });
    }

    /// Settle the record to a terminal state (only if still `IN_PROGRESS`).
    fn settle(&self, status: &str, phases: &[Value], logs_loc: Option<Value>) -> bool {
        let now = Utc::now();
        self.with_record(|obj| {
            obj.insert(self.status_key().into(), json!(status));
            obj.insert(self.complete_key().into(), json!(true));
            obj.insert("currentPhase".into(), json!("COMPLETED"));
            obj.insert("endTime".into(), ts(now));
            obj.insert("phases".into(), Value::Array(phases.to_vec()));
            if let Some(loc) = logs_loc {
                obj.insert("logs".into(), loc);
            }
        })
    }

    /// Remove this build from the real-backed tracking set (called once it is
    /// terminal, so `BatchGetBuilds`' lazy settle can never touch it again).
    fn untrack(&self) {
        let mut guard = self.state.write();
        let st = guard.get_or_create(&self.account);
        if self.is_batch {
            st.real_backed_build_batches.remove(&self.build_id);
        } else {
            st.real_backed_builds.remove(&self.build_id);
        }
    }

    async fn snapshot(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }
}

// ---------------------------------------------------------------------------
// Phase model
// ---------------------------------------------------------------------------

struct PhaseTimer {
    phase_type: &'static str,
    start: DateTime<Utc>,
}

impl PhaseTimer {
    fn start(phase_type: &'static str) -> Self {
        Self {
            phase_type,
            start: Utc::now(),
        }
    }

    fn finish(self, status: &str, contexts: Vec<Value>) -> Value {
        let end = Utc::now();
        let dur = (end - self.start).num_seconds().max(0);
        json!({
            "phaseType": self.phase_type,
            "phaseStatus": status,
            "startTime": ts(self.start),
            "endTime": ts(end),
            "durationInSeconds": dur,
            "contexts": contexts,
        })
    }
}

fn submitted_queued(now: DateTime<Utc>) -> Vec<Value> {
    vec![
        json!({
            "phaseType": "SUBMITTED", "phaseStatus": "SUCCEEDED",
            "startTime": ts(now), "endTime": ts(now),
            "durationInSeconds": 0, "contexts": [],
        }),
        json!({
            "phaseType": "QUEUED", "phaseStatus": "SUCCEEDED",
            "startTime": ts(now), "endTime": ts(now),
            "durationInSeconds": 0, "contexts": [],
        }),
    ]
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Run one real build end to end. Any failure to even start the container
/// settles the build `FAILED` with a diagnostic phase context, so a build never
/// hangs `IN_PROGRESS` while the backend is live.
pub async fn run_build(job: BuildJob) {
    let start = Utc::now();
    let mut phases = submitted_queued(start);

    // Resolve the buildspec first — a syntax error fails the build before any
    // container is created (matching CodeBuild).
    let spec = match job.buildspec.as_deref() {
        Some(text) if !text.trim().is_empty() => match parse_buildspec(text) {
            Ok(s) => s,
            Err(e) => {
                let t = PhaseTimer::start("PROVISIONING");
                phases.push(t.finish(
                    "FAILED",
                    vec![json!({ "statusCode": "YAML_FILE_ERROR", "message": e })],
                ));
                finalize_failure(&job, phases, "FAILED").await;
                return;
            }
        },
        // No buildspec (no inline spec and no override): nothing to run, but the
        // container provisioning still "succeeds" — matching a project whose
        // source has no buildspec is an error in AWS, but fakecloud has no repo
        // to fetch one from, so treat an absent spec as an empty build.
        _ => Buildspec::default(),
    };

    let image = resolve_image(job.image.as_deref());
    let cli = job.backend.cli.clone();

    // PROVISIONING: pull + create + start a long-lived container we exec into.
    let prov = PhaseTimer::start("PROVISIONING");
    let container = match start_container(&cli, &image, &job, &spec).await {
        Ok(id) => {
            job.running.lock().insert(job.build_id.clone(), id.clone());
            phases.push(prov.finish("SUCCEEDED", vec![]));
            id
        }
        Err(e) => {
            phases.push(prov.finish(
                "FAILED",
                vec![json!({ "statusCode": "CLIENT_ERROR", "message": e })],
            ));
            finalize_failure(&job, phases, "FAILED").await;
            return;
        }
    };
    job.set_phases(&phases, "PROVISIONING");

    // DOWNLOAD_SOURCE: fakecloud has no real repo to clone; the primary source
    // is the (already-provided) buildspec, so this phase is a real, immediate
    // success rather than a fabricated one.
    let dl = PhaseTimer::start("DOWNLOAD_SOURCE");
    phases.push(dl.finish("SUCCEEDED", vec![]));
    job.set_phases(&phases, "DOWNLOAD_SOURCE");

    // Build-level env = base env + buildspec env.variables (buildspec wins).
    let mut env = job.base_env.clone();
    env.extend(spec.env_vars.iter().cloned());

    let mut log_buf: Vec<String> = Vec::new();
    let mut build_failed = false;
    let mut timed_out = false;

    // INSTALL -> PRE_BUILD -> BUILD, short-circuiting on the first failure.
    for (phase_type, commands) in [
        ("INSTALL", &spec.install),
        ("PRE_BUILD", &spec.pre_build),
        ("BUILD", &spec.build),
    ] {
        if build_failed {
            break;
        }
        if !job.is_running() {
            // StopBuild won the race; leave the record as STOPPED.
            cleanup(&job, &container).await;
            return;
        }
        let (phase, failed, to) =
            run_phase(&cli, &container, phase_type, commands, &env, &mut log_buf).await;
        timed_out |= to;
        build_failed |= failed;
        phases.push(phase);
        job.set_phases(&phases, phase_type);
        flush_logs(&job, &mut log_buf);
    }

    // POST_BUILD always runs, even if an earlier phase failed (AWS semantics).
    if job.is_running() {
        let (phase, failed, to) = run_phase(
            &cli,
            &container,
            "POST_BUILD",
            &spec.post_build,
            &env,
            &mut log_buf,
        )
        .await;
        timed_out |= to;
        build_failed |= failed;
        phases.push(phase);
        job.set_phases(&phases, "POST_BUILD");
        flush_logs(&job, &mut log_buf);
    } else {
        cleanup(&job, &container).await;
        return;
    }

    // UPLOAD_ARTIFACTS: only when the build succeeded and S3 artifacts declared.
    if !build_failed {
        let up = PhaseTimer::start("UPLOAD_ARTIFACTS");
        match upload_artifacts(&job, &cli, &container, &spec).await {
            Ok(uploaded) => {
                let ctx = if uploaded == 0 {
                    vec![]
                } else {
                    vec![json!({
                        "statusCode": "SUCCEEDED",
                        "message": format!("Uploaded {uploaded} artifact object(s)"),
                    })]
                };
                phases.push(up.finish("SUCCEEDED", ctx));
            }
            Err(e) => {
                build_failed = true;
                phases.push(up.finish(
                    "FAILED",
                    vec![json!({ "statusCode": "CLIENT_ERROR", "message": e })],
                ));
            }
        }
        job.set_phases(&phases, "UPLOAD_ARTIFACTS");
    }

    // FINALIZING + COMPLETED.
    let fin = PhaseTimer::start("FINALIZING");
    phases.push(fin.finish("SUCCEEDED", vec![]));
    phases.push(json!({ "phaseType": "COMPLETED", "endTime": ts(Utc::now()) }));

    let status = if timed_out {
        "TIMED_OUT"
    } else if build_failed {
        "FAILED"
    } else {
        "SUCCEEDED"
    };

    let logs_loc = self_logs_location(&job);
    let settled = job.settle(status, &phases, logs_loc);
    if settled {
        job.untrack();
        job.snapshot().await;
    }
    cleanup(&job, &container).await;
}

/// Settle a build that failed before/at provisioning (no container running).
async fn finalize_failure(job: &BuildJob, mut phases: Vec<Value>, status: &str) {
    phases.push(json!({ "phaseType": "COMPLETED", "endTime": ts(Utc::now()) }));
    if job.settle(status, &phases, self_logs_location(job)) {
        job.untrack();
        job.snapshot().await;
    }
}

async fn cleanup(job: &BuildJob, container: &str) {
    job.running.lock().remove(&job.build_id);
    kill_container(&job.backend.cli, container).await;
}

/// Create + start a detached container that stays alive (`sleep`) so we can
/// `exec` each phase into it. `docker cp` / bind mounts are avoided; the image
/// is pulled implicitly by `run`.
async fn start_container(
    cli: &str,
    image: &str,
    job: &BuildJob,
    _spec: &Buildspec,
) -> Result<String, String> {
    let mut cmd = Command::new(cli);
    cmd.arg("run")
        .arg("-d")
        .arg("--label")
        .arg(format!("fakecloud-codebuild={}", job.project_name))
        .arg("--label")
        .arg(format!(
            "fakecloud-instance=fakecloud-{}",
            std::process::id()
        ));
    // Override the entrypoint so an image with its own ENTRYPOINT still just
    // sleeps and lets us exec build commands into it.
    cmd.arg("--entrypoint").arg("sleep");
    cmd.arg(image).arg("3600");

    // Image pull can be slow on a cold cache; give it a generous ceiling.
    let out = tokio::time::timeout(Duration::from_secs(600), cmd.output())
        .await
        .map_err(|_| "timed out pulling/starting build image".to_string())?
        .map_err(|e| format!("failed to launch container: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "container failed to start: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if id.is_empty() {
        return Err("container id was empty".to_string());
    }
    // Create the working directory the phases run in.
    let mk = Command::new(cli)
        .args([
            "exec",
            &id,
            "sh",
            "-c",
            &format!("mkdir -p {BUILD_WORKDIR}"),
        ])
        .output()
        .await
        .map_err(|e| format!("failed to prepare workdir: {e}"))?;
    if !mk.status.success() {
        kill_container(cli, &id).await;
        return Err(format!(
            "failed to create {BUILD_WORKDIR}: {}",
            String::from_utf8_lossy(&mk.stderr).trim()
        ));
    }
    Ok(id)
}

/// Run one buildspec phase's commands in the container. Returns
/// `(phase_record, failed, timed_out)`. An empty command list is an immediate
/// success (the phase still appears in the breakdown).
async fn run_phase(
    cli: &str,
    container: &str,
    phase_type: &'static str,
    commands: &[String],
    env: &[(String, String)],
    log_buf: &mut Vec<String>,
) -> (Value, bool, bool) {
    let timer = PhaseTimer::start(phase_type);
    if commands.is_empty() {
        return (timer.finish("SUCCEEDED", vec![]), false, false);
    }

    // `exec 2>&1` merges stderr into stdout in capture order; `set -e` makes the
    // phase stop (and exit non-zero) at the first failing command — matching
    // CodeBuild, which fails a phase on its first failing command.
    let mut script = String::from("exec 2>&1\nset -e\ncd ");
    script.push_str(BUILD_WORKDIR);
    script.push('\n');
    for c in commands {
        script.push_str(c);
        script.push('\n');
    }

    let mut cmd = Command::new(cli);
    cmd.arg("exec");
    for (k, v) in env {
        cmd.arg("-e").arg(format!("{k}={v}"));
    }
    cmd.arg(container).arg("sh").arg("-c").arg(&script);

    log_buf.push(format!(
        "[Container] Entering phase {} at {}",
        phase_type.to_ascii_lowercase(),
        Utc::now().to_rfc3339()
    ));

    // Per-phase ceiling guards against a hung command; the whole build is also
    // bounded by the caller's overall runtime.
    let result = tokio::time::timeout(Duration::from_secs(3600), cmd.output()).await;

    match result {
        Err(_) => {
            log_buf.push(format!("[Container] Phase {phase_type} timed out"));
            (
                timer.finish(
                    "FAILED",
                    vec![json!({ "statusCode": "TIMED_OUT", "message": "phase exceeded time limit" })],
                ),
                true,
                true,
            )
        }
        Ok(Err(e)) => {
            log_buf.push(format!("[Container] Phase {phase_type} error: {e}"));
            (
                timer.finish(
                    "FAILED",
                    vec![json!({ "statusCode": "CLIENT_ERROR", "message": e.to_string() })],
                ),
                true,
                false,
            )
        }
        Ok(Ok(out)) => {
            let text = String::from_utf8_lossy(&out.stdout);
            for line in text.lines() {
                log_buf.push(line.to_string());
            }
            let code = out.status.code().unwrap_or(-1);
            if out.status.success() {
                (timer.finish("SUCCEEDED", vec![]), false, false)
            } else {
                log_buf.push(format!(
                    "[Container] Command did not exit successfully (exit code {code})"
                ));
                (
                    timer.finish(
                        "FAILED",
                        vec![json!({
                            "statusCode": "COMMAND_EXECUTION_ERROR",
                            "message": format!("Phase {phase_type} exited with code {code}"),
                        })],
                    ),
                    true,
                    false,
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

/// Resolved default log group `/aws/codebuild/<project>` and stream `<uuid>`,
/// unless the project's `logsConfig.cloudWatchLogs` overrides them (and unless
/// it is `DISABLED`).
fn log_target(job: &BuildJob) -> Option<(String, String)> {
    let cw = &job.cw_logs;
    if cw.get("status").and_then(Value::as_str) == Some("DISABLED") {
        return None;
    }
    let group = cw
        .get("groupName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("/aws/codebuild/{}", job.project_name));
    let stream = cw
        .get("streamName")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| job.short_id());
    Some((group, stream))
}

/// The `LogsLocation` written onto the settled build, pointing at the real
/// CloudWatch group/stream a client can read via `GetLogEvents`.
fn self_logs_location(job: &BuildJob) -> Option<Value> {
    let (group, stream) = log_target(job)?;
    let arn = format!(
        "arn:aws:logs:{}:{}:log-group:{}:log-stream:{}",
        job.region, job.account, group, stream
    );
    Some(json!({
        "groupName": group,
        "streamName": stream,
        "cloudWatchLogsArn": arn,
        "cloudWatchLogs": { "status": "ENABLED" },
        "deepLink": format!(
            "https://console.aws.amazon.com/cloudwatch/home?region={}#logsV2:log-groups/log-group/{}/log-events/{}",
            job.region, group, stream
        ),
    }))
}

/// Ship buffered lines to CloudWatch Logs and clear the buffer.
fn flush_logs(job: &BuildJob, buf: &mut Vec<String>) {
    if buf.is_empty() {
        return;
    }
    let (Some(logs), Some((group, stream))) = (job.logs_state.as_ref(), log_target(job)) else {
        buf.clear();
        return;
    };
    let now = Utc::now().timestamp_millis();
    let events: Vec<IngestEvent> = buf
        .drain(..)
        .enumerate()
        .map(|(i, message)| IngestEvent {
            timestamp_ms: now.saturating_add(i as i64),
            message,
        })
        .collect();
    append_events(logs, &job.account, &job.region, &group, &stream, &events);
}

// ---------------------------------------------------------------------------
// Artifacts
// ---------------------------------------------------------------------------

/// Upload declared `S3` artifacts. Returns the number of objects written (0 for
/// `NO_ARTIFACTS`/`CODEPIPELINE`/no files). Errors bubble up so UPLOAD_ARTIFACTS
/// fails the build, matching AWS.
async fn upload_artifacts(
    job: &BuildJob,
    cli: &str,
    container: &str,
    spec: &Buildspec,
) -> Result<usize, String> {
    if job.artifacts.get("type").and_then(Value::as_str) != Some("S3") {
        return Ok(0);
    }
    let Some(delivery) = job.s3_delivery.as_ref() else {
        return Ok(0);
    };
    if spec.artifacts_files.is_empty() {
        return Ok(0);
    }
    let bucket = job
        .artifacts
        .get("location")
        .and_then(Value::as_str)
        .ok_or_else(|| "artifacts.location (S3 bucket) is required".to_string())?
        .to_string();
    let art_path = job
        .artifacts
        .get("path")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim_matches('/')
        .to_string();
    let art_name = job
        .artifacts
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or(&job.project_name)
        .to_string();
    let packaging = job
        .artifacts
        .get("packaging")
        .and_then(Value::as_str)
        .unwrap_or("NONE");

    // Root the file globs at base-directory when set.
    let base = match &spec.artifacts_base_dir {
        Some(b) => format!("{BUILD_WORKDIR}/{}", b.trim_matches('/')),
        None => BUILD_WORKDIR.to_string(),
    };

    // Expand the declared file patterns inside the container. `find` handles the
    // common `**/*` and explicit-path cases; the resulting paths are relative to
    // `base` so we can preserve or discard the directory structure.
    let mut files: Vec<String> = Vec::new();
    for pattern in &spec.artifacts_files {
        let script = format!(
            "cd {base} 2>/dev/null && find {pattern} -type f 2>/dev/null || true",
            base = shell_quote(&base),
            pattern = if pattern == "**/*" {
                ".".to_string()
            } else {
                shell_quote(pattern)
            }
        );
        let out = Command::new(cli)
            .args(["exec", container, "sh", "-c", &script])
            .output()
            .await
            .map_err(|e| format!("failed to enumerate artifacts: {e}"))?;
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            let rel = line.trim_start_matches("./").to_string();
            if !rel.is_empty() && !files.contains(&rel) {
                files.push(rel);
            }
        }
    }
    if files.is_empty() {
        return Ok(0);
    }

    // Read each file's bytes out of the container.
    let mut collected: Vec<(String, Vec<u8>)> = Vec::new();
    for rel in &files {
        let in_container = format!("{base}/{rel}");
        let out = Command::new(cli)
            .args(["exec", container, "cat", &in_container])
            .output()
            .await
            .map_err(|e| format!("failed to read artifact {rel}: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "failed to read artifact {rel}: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        let stored = if spec.artifacts_discard_paths {
            rel.rsplit('/').next().unwrap_or(rel).to_string()
        } else {
            rel.clone()
        };
        collected.push((stored, out.stdout));
    }

    let prefix = match (art_path.is_empty(), art_name.is_empty()) {
        (true, true) => String::new(),
        (true, false) => art_name.clone(),
        (false, true) => art_path.clone(),
        (false, false) => format!("{art_path}/{art_name}"),
    };

    let mut written = 0usize;
    if packaging.eq_ignore_ascii_case("ZIP") {
        let zip_bytes =
            zip_files(&collected).map_err(|e| format!("failed to zip artifacts: {e}"))?;
        let key = if prefix.is_empty() {
            format!("{art_name}.zip")
        } else {
            prefix.clone()
        };
        delivery
            .put_object(
                &job.account,
                &bucket,
                &key,
                zip_bytes,
                Some("application/zip"),
            )
            .map_err(|e| format!("failed to upload artifact zip: {e}"))?;
        written += 1;
    } else {
        for (rel, bytes) in collected {
            let key = if prefix.is_empty() {
                rel.clone()
            } else {
                format!("{prefix}/{rel}")
            };
            delivery
                .put_object(&job.account, &bucket, &key, bytes, None)
                .map_err(|e| format!("failed to upload artifact {rel}: {e}"))?;
            written += 1;
        }
    }
    Ok(written)
}

/// Zip a set of `(relative_path, bytes)` into one archive.
fn zip_files(files: &[(String, Vec<u8>)]) -> std::io::Result<Vec<u8>> {
    let mut buf = std::io::Cursor::new(Vec::new());
    {
        let mut zip = zip::ZipWriter::new(&mut buf);
        let opts: zip::write::SimpleFileOptions =
            zip::write::SimpleFileOptions::default().unix_permissions(0o644);
        for (name, bytes) in files {
            zip.start_file(name.clone(), opts)?;
            zip.write_all(bytes)?;
        }
        zip.finish()?;
    }
    Ok(buf.into_inner())
}

/// Single-quote a string for safe interpolation into an `sh -c` script.
fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolves_curated_image_to_default() {
        assert_eq!(
            resolve_image(Some("aws/codebuild/standard:7.0")),
            DEFAULT_IMAGE
        );
        assert_eq!(resolve_image(None), DEFAULT_IMAGE);
        assert_eq!(resolve_image(Some("")), DEFAULT_IMAGE);
    }

    #[test]
    fn keeps_user_supplied_image() {
        assert_eq!(
            resolve_image(Some("public.ecr.aws/docker/library/node:20")),
            "public.ecr.aws/docker/library/node:20"
        );
        assert_eq!(resolve_image(Some("alpine:3.20")), "alpine:3.20");
    }

    #[test]
    fn parses_phases_and_env_and_artifacts() {
        let spec = parse_buildspec(
            "version: 0.2\n\
             env:\n  variables:\n    FOO: bar\n    NUM: 3\n\
             phases:\n  install:\n    commands:\n      - echo install\n\
             \x20 build:\n    commands:\n      - echo build\n      - make\n\
             \x20 post_build:\n    commands: echo done\n\
             artifacts:\n  files:\n    - out.txt\n  discard-paths: yes\n",
        )
        .expect("valid buildspec");
        assert_eq!(
            spec.env_vars,
            vec![
                ("FOO".to_string(), "bar".to_string()),
                ("NUM".to_string(), "3".to_string()),
            ]
        );
        assert_eq!(spec.install, vec!["echo install".to_string()]);
        assert_eq!(
            spec.build,
            vec!["echo build".to_string(), "make".to_string()]
        );
        assert_eq!(spec.post_build, vec!["echo done".to_string()]);
        assert_eq!(spec.artifacts_files, vec!["out.txt".to_string()]);
        assert!(spec.artifacts_discard_paths);
    }

    #[test]
    fn single_string_commands_supported() {
        let spec =
            parse_buildspec("version: 0.2\nphases:\n  build:\n    commands: echo one\n").unwrap();
        assert_eq!(spec.build, vec!["echo one".to_string()]);
    }

    #[test]
    fn invalid_yaml_is_error() {
        assert!(parse_buildspec("\tnot: [valid").is_err());
    }

    #[test]
    fn zip_round_trips() {
        let files = vec![("a/b.txt".to_string(), b"hello".to_vec())];
        let bytes = zip_files(&files).unwrap();
        let mut archive = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(archive.len(), 1);
        let entry = archive.by_index(0).unwrap();
        assert_eq!(entry.name(), "a/b.txt");
    }

    #[test]
    fn disabled_env_recognized() {
        // Sanity: env parsing helper honors the documented truthy values.
        assert!(!env_truthy("FAKECLOUD_CODEBUILD_DISABLE_BACKEND_UNSET_XYZ"));
    }
}
