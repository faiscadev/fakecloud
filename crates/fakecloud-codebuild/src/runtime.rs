//! Real Docker/Podman-backed build execution for AWS CodeBuild.
//!
//! `StartBuild` returns immediately with the build `IN_PROGRESS`; the actual
//! work runs here in a background task ([`run_build`]) so the handler never
//! blocks on an image pull or a container run (a client-timeout bug class).
//!
//! The task resolves the environment image, assembles the buildspec (inline on
//! the project `source.buildspec` or a `StartBuild.buildspecOverride`), parses
//! its `env`/`phases`/`artifacts`, then runs a real container from the image and
//! executes the phases in one `docker exec`, carrying cwd + exported vars across
//! phases (via a threaded state file) so `cd`/`export` persist exactly like AWS.
//! Each phase runs in a subshell so a failing command — including a user
//! `exit N` — fails only that phase (recorded FAILED) instead of aborting the
//! build. It honors CodeBuild phase-failure semantics
//! (`install`/`pre_build`/`build` short-circuit on the first failing phase,
//! `post_build` always runs), settles `buildStatus` on the REAL container exit
//! codes (or `TIMED_OUT` at the configured `timeoutInMinutes`), streams output
//! to CloudWatch Logs (fakecloud-logs), and uploads declared `S3` artifacts
//! (fakecloud-s3).
//!
//! The backend is gated: it is used only when a container CLI is available AND
//! `FAKECLOUD_CODEBUILD_DISABLE_BACKEND` is not set. When disabled (the
//! conformance probe points `FAKECLOUD_CONTAINER_CLI` at a non-existent binary;
//! the tfacc harness sets the disable flag) the service falls back to the
//! deterministic settle-to-`SUCCEEDED`-on-read path, so response shapes are
//! identical and conformance stays green.

use std::collections::HashMap;
use std::io::Write as _;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
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

use crate::state::{CodeBuildState, SharedCodeBuildState};

/// Fallback image used when the project names an AWS-curated CodeBuild image
/// (e.g. `aws/codebuild/standard:7.0`), which is not pullable from a public
/// registry. A small Ubuntu from ECR Public (no auth, provides `bash`/`sh` so
/// buildspec `commands` run unchanged). A user-supplied real image is used
/// verbatim instead.
const DEFAULT_IMAGE: &str = "public.ecr.aws/docker/library/ubuntu:22.04";

/// Working directory inside the build container. Buildspec `commands` run here
/// and `artifacts.files` are resolved relative to it (or `base-directory`).
const BUILD_WORKDIR: &str = "/codebuild/build";

/// Unique prefix for the phase-boundary marker lines the build script prints so
/// the parser can separate them from real build output.
const MARKER: &str = "@@FCB@@";

/// Bounds for the build timeout (minutes). AWS allows up to 8 hours; the
/// service defaults an unset timeout to 60 before it reaches [`BuildJob`].
const MAX_TIMEOUT_MIN: i64 = 480;
const MIN_TIMEOUT_MIN: i64 = 5;

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
    /// `buildStatus`/`buildComplete`, and emits BuildBatch-shaped phases).
    pub is_batch: bool,
    /// Resolved `environment.image`.
    pub image: Option<String>,
    /// Base env vars (`CODEBUILD_*` + project `environmentVariables`, with
    /// PARAMETER_STORE/SECRETS_MANAGER already resolved by the service), merged
    /// with the buildspec `env.variables` at run time.
    pub base_env: Vec<(String, String)>,
    /// The buildspec text (inline `source.buildspec` or `buildspecOverride`).
    pub buildspec: Option<String>,
    pub source_version: Option<String>,
    /// Resolved `logsConfig.cloudWatchLogs` value (or `null`).
    pub cw_logs: Value,
    /// Resolved `artifacts` value (or `null`).
    pub artifacts: Value,
    /// Resolved build timeout in minutes (default 60, clamped 5..=480).
    pub timeout_minutes: i64,
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

    fn timeout(&self) -> Duration {
        let mins = self.timeout_minutes.clamp(MIN_TIMEOUT_MIN, MAX_TIMEOUT_MIN);
        Duration::from_secs((mins * 60) as u64)
    }
}

/// Kill and remove a build's container (best-effort, async). Used by `StopBuild`.
pub async fn kill_container(cli: &str, container_id: &str) {
    let _ = Command::new(cli)
        .args(["rm", "-f", container_id])
        .output()
        .await;
}

/// Blocking container removal for the Drop-guard cleanup path (no async runtime
/// is guaranteed to be alive when a task is being torn down by a panic).
fn kill_container_blocking(cli: &str, container_id: &str) {
    let _ = std::process::Command::new(cli)
        .args(["rm", "-f", container_id])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
}

// ---------------------------------------------------------------------------
// Drop guard: guarantee cleanup + settle on ALL exits (incl. panic)
// ---------------------------------------------------------------------------

/// Ensures a build never lingers `IN_PROGRESS` with a leaked container if the
/// execution task ends abnormally (panic / unexpected early return). On normal
/// completion the task calls [`BuildGuard::disarm`]; otherwise `Drop` settles
/// the record `FAILED` (only if it is still `IN_PROGRESS`, so a `StopBuild` that
/// already set `STOPPED` is preserved) and force-removes the container.
struct BuildGuard {
    state: SharedCodeBuildState,
    account: String,
    build_id: String,
    is_batch: bool,
    cli: String,
    running: RunningContainers,
    container: Arc<Mutex<Option<String>>>,
    settled: Arc<AtomicBool>,
}

impl BuildGuard {
    fn new(job: &BuildJob) -> Self {
        Self {
            state: job.state.clone(),
            account: job.account.clone(),
            build_id: job.build_id.clone(),
            is_batch: job.is_batch,
            cli: job.backend.cli().to_string(),
            running: job.running.clone(),
            container: Arc::new(Mutex::new(None)),
            settled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set_container(&self, id: &str) {
        *self.container.lock() = Some(id.to_string());
    }

    /// Mark the build as properly settled so `Drop` is a no-op.
    fn disarm(&self) {
        self.settled.store(true, Ordering::Release);
        *self.container.lock() = None;
        self.running.lock().remove(&self.build_id);
    }
}

impl Drop for BuildGuard {
    fn drop(&mut self) {
        if self.settled.load(Ordering::Acquire) {
            return;
        }
        // Abnormal exit: fail the build if it is still in progress, drop it from
        // the real-backed set, and force-remove the container.
        {
            let mut guard = self.state.write();
            let st = guard.get_or_create(&self.account);
            let (key, complete) = if self.is_batch {
                ("buildBatchStatus", "complete")
            } else {
                ("buildStatus", "buildComplete")
            };
            let record = if self.is_batch {
                st.build_batches.get_mut(&self.build_id)
            } else {
                st.builds.get_mut(&self.build_id)
            };
            if let Some(record) = record {
                if record.get(key).and_then(Value::as_str) == Some("IN_PROGRESS") {
                    if let Some(obj) = record.as_object_mut() {
                        obj.insert(key.into(), json!("FAILED"));
                        obj.insert("currentPhase".into(), json!("COMPLETED"));
                        obj.insert(complete.into(), json!(true));
                        obj.insert("endTime".into(), ts(Utc::now()));
                    }
                }
            }
            if self.is_batch {
                st.real_backed_build_batches.remove(&self.build_id);
            } else {
                st.real_backed_builds.remove(&self.build_id);
            }
        }
        self.running.lock().remove(&self.build_id);
        if let Some(c) = self.container.lock().take() {
            kill_container_blocking(&self.cli, &c);
        }
    }
}

impl BuildJob {
    fn record<'a>(&self, st: &'a mut CodeBuildState) -> Option<&'a mut Value> {
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

    /// Overwrite the record's `phases` + `currentPhase` in one locked mutation.
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
// Build script (cwd + exported vars threaded across phases so state persists,
// while a failing command — including `exit N` — still records the phase FAILED)
// ---------------------------------------------------------------------------

/// Emit one phase's block. Each phase runs its commands in a SUBSHELL so a
/// user `exit N` (or any non-zero) fails only that phase instead of aborting the
/// whole build before the end-marker is printed. Cross-phase state is preserved
/// by sourcing the threaded env (`$FCB_S/env`) and cwd (`$FCB_S/cwd`) at the
/// start and, on success, writing them back — so `cd`/`export` in one phase are
/// visible to the next, matching AWS. Commands are `&&`-chained so the phase
/// stops at (and reports) the first failing command. `guarded` phases
/// (install/pre_build/build) only run while no earlier phase has failed;
/// post_build always runs.
fn phase_block(label: &str, cmds: &[String], guarded: bool) -> String {
    let ts = "$(date +%s%3N 2>/dev/null || echo 0)";
    let body = if cmds.is_empty() {
        ":".to_string()
    } else {
        cmds.join(" &&\n")
    };
    let inner = format!(
        "printf '{MARKER}S {label} %s\\n' \"{ts}\"\n\
         (\n\
         . \"$FCB_S/env\" 2>/dev/null || true\n\
         cd \"$(cat \"$FCB_S/cwd\")\" 2>/dev/null || cd '{BUILD_WORKDIR}'\n\
         {body}\n\
         __fc_rc=$?\n\
         if [ \"$__fc_rc\" = 0 ]; then pwd > \"$FCB_S/cwd\"; export -p > \"$FCB_S/env\"; fi\n\
         exit $__fc_rc\n\
         )\n\
         FC_RC=$?\n\
         printf '{MARKER}E {label} %s %s\\n' \"$FC_RC\" \"{ts}\"\n\
         if [ \"$FC_RC\" != 0 ]; then FC_FAILED=1; fi\n"
    );
    if guarded {
        format!("if [ \"$FC_FAILED\" = 0 ]; then\n{inner}fi\n")
    } else {
        inner
    }
}

/// Build the shell script that runs all phases in one `docker exec`. Phase
/// boundaries + exit codes are printed as [`MARKER`] lines the parser separates
/// from build output; `install`/`pre_build`/`build` short-circuit on the first
/// failing phase while `post_build` always runs. The script's own exit code is
/// non-zero iff any phase failed.
fn build_script(spec: &Buildspec) -> String {
    let mut s = String::new();
    s.push_str("exec 2>&1\n");
    // Thread cwd + exported vars across phases via a small state dir.
    s.push_str("FCB_S=/tmp/.fcb_state\n");
    s.push_str("rm -rf \"$FCB_S\"; mkdir -p \"$FCB_S\"\n");
    s.push_str(&format!("printf '%s' '{BUILD_WORKDIR}' > \"$FCB_S/cwd\"\n"));
    s.push_str(": > \"$FCB_S/env\"\n");
    s.push_str("FC_FAILED=0\n");
    s.push_str(&phase_block("INSTALL", &spec.install, true));
    s.push_str(&phase_block("PRE_BUILD", &spec.pre_build, true));
    s.push_str(&phase_block("BUILD", &spec.build, true));
    s.push_str(&phase_block("POST_BUILD", &spec.post_build, false));
    s.push_str("exit $FC_FAILED\n");
    s
}

/// A parsed phase result from the marker stream.
struct PhaseResult {
    name: String,
    rc: i64,
    start_ms: i64,
    end_ms: i64,
}

/// Split a build script's combined stdout into (phase results, build log
/// lines). Marker lines drive the phase breakdown; every other line is real
/// build output destined for CloudWatch Logs. A phase whose start-marker has NO
/// matching end-marker (the shell died mid-phase) is recorded FAILED rather than
/// dropped, so an unknown outcome never lets the build settle SUCCEEDED.
fn parse_exec_output(stdout: &str) -> (Vec<PhaseResult>, Vec<String>) {
    // An open phase (start seen); `end` is set when its end-marker arrives.
    struct Open {
        name: String,
        start_ms: i64,
        end: Option<(i64, i64)>, // (rc, end_ms)
    }
    let mut order: Vec<Open> = Vec::new();
    let mut logs: Vec<String> = Vec::new();
    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix(MARKER) {
            let mut it = rest.split_whitespace();
            match it.next() {
                Some("S") => {
                    if let (Some(name), Some(ms)) = (it.next(), it.next()) {
                        order.push(Open {
                            name: name.to_string(),
                            start_ms: ms.parse().unwrap_or(0),
                            end: None,
                        });
                    }
                }
                Some("E") => {
                    let name = it.next().unwrap_or("").to_string();
                    let rc: i64 = it.next().and_then(|v| v.parse().ok()).unwrap_or(1);
                    let end_ms: i64 = it.next().and_then(|v| v.parse().ok()).unwrap_or(0);
                    // Close the most-recent still-open phase of this name.
                    if let Some(e) = order
                        .iter_mut()
                        .rev()
                        .find(|o| o.name == name && o.end.is_none())
                    {
                        e.end = Some((rc, end_ms));
                    }
                }
                _ => {}
            }
        } else {
            logs.push(line.to_string());
        }
    }
    let phases = order
        .into_iter()
        .map(|o| match o.end {
            Some((rc, end_ms)) => PhaseResult {
                name: o.name,
                rc,
                start_ms: o.start_ms,
                end_ms,
            },
            // Start with no end: the shell died during this phase -> FAILED.
            None => PhaseResult {
                name: o.name,
                rc: 1,
                start_ms: o.start_ms,
                end_ms: 0,
            },
        })
        .collect();
    (phases, logs)
}

/// Convert a parsed buildspec-phase result into the wire `phases[]` entry.
/// Falls back to host wall-clock timing when the in-container `date` markers are
/// unusable (a non-GNU image without `%N`).
fn buildspec_phase_value(
    p: &PhaseResult,
    host_start: DateTime<Utc>,
    host_end: DateTime<Utc>,
) -> Value {
    let status = if p.rc == 0 { "SUCCEEDED" } else { "FAILED" };
    let (start, end) = if p.start_ms > 0 && p.end_ms >= p.start_ms {
        (
            DateTime::from_timestamp_millis(p.start_ms).unwrap_or(host_start),
            DateTime::from_timestamp_millis(p.end_ms).unwrap_or(host_end),
        )
    } else {
        (host_start, host_end)
    };
    let dur = (end - start).num_seconds().max(0);
    let mut contexts = vec![];
    if p.rc != 0 {
        contexts.push(json!({
            "statusCode": "COMMAND_EXECUTION_ERROR",
            "message": format!("Phase {} exited with code {}", p.name, p.rc),
        }));
    }
    json!({
        "phaseType": p.name,
        "phaseStatus": status,
        "startTime": ts(start),
        "endTime": ts(end),
        "durationInSeconds": dur,
        "contexts": contexts,
    })
}

// ---------------------------------------------------------------------------
// Timestamps + host-side phase timing
// ---------------------------------------------------------------------------

fn ts(dt: DateTime<Utc>) -> Value {
    json!(dt.timestamp_millis() as f64 / 1000.0)
}

struct PhaseTimer {
    phase_type: String,
    start: DateTime<Utc>,
}

impl PhaseTimer {
    fn start(phase_type: &str) -> Self {
        Self {
            phase_type: phase_type.to_string(),
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

fn submitted_queued(now: DateTime<Utc>, is_batch: bool) -> Vec<Value> {
    let mut phases = vec![json!({
        "phaseType": "SUBMITTED", "phaseStatus": "SUCCEEDED",
        "startTime": ts(now), "endTime": ts(now),
        "durationInSeconds": 0, "contexts": [],
    })];
    // Single builds pass through QUEUED; a build batch downloads the batch spec.
    if !is_batch {
        phases.push(json!({
            "phaseType": "QUEUED", "phaseStatus": "SUCCEEDED",
            "startTime": ts(now), "endTime": ts(now),
            "durationInSeconds": 0, "contexts": [],
        }));
    }
    phases
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

enum ExecOutcome {
    Ran {
        stdout: String,
        exit_code: Option<i32>,
    },
    TimedOut,
    LaunchError(String),
}

/// Run one real build end to end. A [`BuildGuard`] guarantees the build settles
/// and the container is removed even if this task panics.
pub async fn run_build(job: BuildJob) {
    let guard = BuildGuard::new(&job);
    let start = Utc::now();
    let mut phases = submitted_queued(start, job.is_batch);

    // Resolve the buildspec first — a syntax error fails the build before any
    // container is created (matching CodeBuild).
    let spec = match job.buildspec.as_deref() {
        Some(text) if !text.trim().is_empty() => match parse_buildspec(text) {
            Ok(s) => s,
            Err(e) => {
                let t = PhaseTimer::start(if job.is_batch {
                    "DOWNLOAD_BATCHSPEC"
                } else {
                    "PROVISIONING"
                });
                phases.push(t.finish(
                    "FAILED",
                    vec![json!({ "statusCode": "YAML_FILE_ERROR", "message": e })],
                ));
                finalize(&job, &guard, phases, "FAILED").await;
                return;
            }
        },
        _ => Buildspec::default(),
    };

    let image = resolve_image(job.image.as_deref());
    let cli = job.backend.cli.clone();

    // PROVISIONING (single) / DOWNLOAD_BATCHSPEC (batch): pull + create + start
    // a container kept alive long enough to run the whole build.
    let prov_type = if job.is_batch {
        "DOWNLOAD_BATCHSPEC"
    } else {
        "PROVISIONING"
    };
    let prov = PhaseTimer::start(prov_type);
    let keepalive_secs = job.timeout().as_secs() + 600;
    let container = match start_container(&cli, &image, &job, keepalive_secs).await {
        Ok(id) => {
            guard.set_container(&id);
            job.running.lock().insert(job.build_id.clone(), id.clone());
            phases.push(prov.finish("SUCCEEDED", vec![]));
            id
        }
        Err(e) => {
            phases.push(prov.finish(
                "FAILED",
                vec![json!({ "statusCode": "CLIENT_ERROR", "message": e })],
            ));
            finalize(&job, &guard, phases, "FAILED").await;
            return;
        }
    };
    job.set_phases(&phases, prov_type);

    if !job.is_batch {
        // DOWNLOAD_SOURCE: fakecloud has no real repo to clone; the primary
        // source is the (already-provided) buildspec, so this is a real,
        // immediate success rather than a fabricated one.
        let dl = PhaseTimer::start("DOWNLOAD_SOURCE");
        phases.push(dl.finish("SUCCEEDED", vec![]));
        job.set_phases(&phases, "DOWNLOAD_SOURCE");
    } else {
        // IN_PROGRESS is the batch's umbrella phase covering the build run.
        let ip = PhaseTimer::start("IN_PROGRESS");
        phases.push(ip.finish("SUCCEEDED", vec![]));
        job.set_phases(&phases, "IN_PROGRESS");
    }

    // Guard against a StopBuild that won the race before we exec.
    if !job.is_running() {
        finish_stopped(&job, &guard, &container).await;
        return;
    }

    // Build-level env = base env + buildspec env.variables (buildspec wins).
    let mut env = job.base_env.clone();
    env.extend(spec.env_vars.iter().cloned());

    // Run ALL phases in ONE continuous shell so cd/export persist across phases.
    let script = build_script(&spec);
    let host_start = Utc::now();
    let outcome = run_build_script(&cli, &container, &env, &script, job.timeout()).await;
    let host_end = Utc::now();

    let mut log_lines: Vec<String> = Vec::new();
    let mut build_failed;
    let mut timed_out = false;

    match outcome {
        ExecOutcome::TimedOut => {
            timed_out = true;
            build_failed = true;
            // Kill the container immediately so nothing runs past the deadline.
            kill_container(&cli, &container).await;
            job.running.lock().remove(&job.build_id);
            phases.push(
                PhaseTimer {
                    phase_type: "BUILD".to_string(),
                    start: host_start,
                }
                .finish(
                    "FAILED",
                    vec![json!({
                        "statusCode": "TIMED_OUT",
                        "message": format!(
                            "Build exceeded the configured timeout of {} minutes",
                            job.timeout_minutes
                        ),
                    })],
                ),
            );
        }
        ExecOutcome::LaunchError(e) => {
            build_failed = true;
            phases.push(
                PhaseTimer {
                    phase_type: "BUILD".to_string(),
                    start: host_start,
                }
                .finish(
                    "FAILED",
                    vec![json!({ "statusCode": "CLIENT_ERROR", "message": e })],
                ),
            );
        }
        ExecOutcome::Ran { stdout, exit_code } => {
            let (results, lines) = parse_exec_output(&stdout);
            log_lines = lines;
            // A phase reported non-zero, OR the build script itself exited
            // non-zero (defence against a lost marker) fails the build.
            build_failed = results.iter().any(|p| p.rc != 0) || !matches!(exit_code, Some(0));
            for p in &results {
                phases.push(buildspec_phase_value(p, host_start, host_end));
            }
        }
    }

    if !job.is_running() {
        // StopBuild set STOPPED while we ran.
        finish_stopped(&job, &guard, &container).await;
        return;
    }
    job.set_phases(&phases, "FINALIZING");
    flush_logs(&job, &mut log_lines);

    // UPLOAD_ARTIFACTS / COMBINE_ARTIFACTS: only when the build succeeded and S3
    // artifacts are declared.
    let art_type = if job.is_batch {
        "COMBINE_ARTIFACTS"
    } else {
        "UPLOAD_ARTIFACTS"
    };
    if !build_failed {
        let up = PhaseTimer::start(art_type);
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
        job.set_phases(&phases, art_type);
    }

    let status = if timed_out {
        "TIMED_OUT"
    } else if build_failed {
        "FAILED"
    } else {
        "SUCCEEDED"
    };

    // A single build ends FINALIZING -> COMPLETED; a batch ends on its terminal
    // status phase (BuildBatch has no COMPLETED phase type).
    if job.is_batch {
        phases.push(json!({
            "phaseType": status,
            "phaseStatus": status,
            "endTime": ts(Utc::now()),
            "durationInSeconds": 0,
            "contexts": [],
        }));
    } else {
        let fin = PhaseTimer::start("FINALIZING");
        phases.push(fin.finish("SUCCEEDED", vec![]));
        phases.push(json!({ "phaseType": "COMPLETED", "endTime": ts(Utc::now()) }));
    }

    // Only single builds carry a `logs` LogsLocation; a build batch keeps its
    // `logConfig` (set at creation) and must not grow a `logs` field.
    let logs_loc = if job.is_batch {
        None
    } else {
        self_logs_location(&job)
    };
    let settled = job.settle(status, &phases, logs_loc);
    if settled {
        job.untrack();
        job.snapshot().await;
    }
    guard.disarm();
    kill_container(&cli, &container).await;
    job.running.lock().remove(&job.build_id);
}

/// Settle a build that failed before/at provisioning.
async fn finalize(job: &BuildJob, guard: &BuildGuard, mut phases: Vec<Value>, status: &str) {
    if !job.is_batch {
        phases.push(json!({ "phaseType": "COMPLETED", "endTime": ts(Utc::now()) }));
    }
    let logs_loc = if job.is_batch {
        None
    } else {
        self_logs_location(job)
    };
    if job.settle(status, &phases, logs_loc) {
        job.untrack();
        job.snapshot().await;
    }
    guard.disarm();
    job.running.lock().remove(&job.build_id);
}

/// A `StopBuild` set the record `STOPPED`; leave it, just tear down.
async fn finish_stopped(job: &BuildJob, guard: &BuildGuard, container: &str) {
    guard.disarm();
    kill_container(&job.backend.cli, container).await;
    job.running.lock().remove(&job.build_id);
}

/// Create + start a detached container that stays alive (`sleep`) long enough to
/// run the whole build, so we can `exec` the build script into it. `docker cp`
/// / bind mounts are avoided; the image is pulled implicitly by `run`.
async fn start_container(
    cli: &str,
    image: &str,
    job: &BuildJob,
    keepalive_secs: u64,
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
    // sleeps; size the sleep to the build timeout (plus a teardown buffer) so a
    // long build's container never exits mid-run (AWS allows up to 8h).
    cmd.arg("--entrypoint").arg("sleep");
    cmd.arg(image).arg(keepalive_secs.to_string());

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

/// Run the whole build script in ONE `docker exec`. On the host-side timeout the
/// exec process is killed (`kill_on_drop`) and the caller force-removes the
/// container so nothing keeps running past the deadline.
async fn run_build_script(
    cli: &str,
    container: &str,
    env: &[(String, String)],
    script: &str,
    timeout: Duration,
) -> ExecOutcome {
    let mut cmd = Command::new(cli);
    cmd.kill_on_drop(true);
    cmd.arg("exec");
    for (k, v) in env {
        cmd.arg("-e").arg(format!("{k}={v}"));
    }
    cmd.arg(container).arg("sh").arg("-c").arg(script);

    match tokio::time::timeout(timeout, cmd.output()).await {
        Err(_) => ExecOutcome::TimedOut,
        Ok(Err(e)) => ExecOutcome::LaunchError(e.to_string()),
        Ok(Ok(out)) => ExecOutcome::Ran {
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            exit_code: out.status.code(),
        },
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
/// `NO_ARTIFACTS`/`CODEPIPELINE`/no files). Globs are resolved by copying the
/// artifact base directory out of the container and matching each pattern in
/// Rust. Errors (including "declared files matched nothing") bubble up so
/// UPLOAD_ARTIFACTS fails the build, matching AWS.
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
        .unwrap_or("NONE")
        .to_string();

    // Root the file globs at base-directory when set.
    let base_in_container = match &spec.artifacts_base_dir {
        Some(b) => format!("{BUILD_WORKDIR}/{}", b.trim_matches('/')),
        None => BUILD_WORKDIR.to_string(),
    };

    // Copy the artifact base directory out of the container so glob matching can
    // happen against a real tree (handles `**/*`, `target/*.jar`, `dir/**`, ...).
    let tmp = tempfile::TempDir::new().map_err(|e| format!("failed to stage artifacts: {e}"))?;
    let cp = Command::new(cli)
        .arg("cp")
        .arg(format!("{container}:{base_in_container}/."))
        .arg(tmp.path())
        .output()
        .await
        .map_err(|e| format!("failed to copy artifacts out: {e}"))?;
    if !cp.status.success() {
        return Err(format!(
            "failed to copy artifacts from {base_in_container}: {}",
            String::from_utf8_lossy(&cp.stderr).trim()
        ));
    }

    let matched = match_artifact_files(tmp.path(), &spec.artifacts_files)
        .map_err(|e| format!("failed to enumerate artifacts: {e}"))?;
    if matched.is_empty() {
        return Err(format!(
            "no matching artifact files for patterns {:?}",
            spec.artifacts_files
        ));
    }

    // Read each matched file's bytes.
    let mut collected: Vec<(String, Vec<u8>)> = Vec::new();
    for rel in &matched {
        let bytes = std::fs::read(tmp.path().join(rel))
            .map_err(|e| format!("failed to read artifact {rel}: {e}"))?;
        let stored = if spec.artifacts_discard_paths {
            rel.rsplit('/').next().unwrap_or(rel).to_string()
        } else {
            rel.clone()
        };
        collected.push((stored, bytes));
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

/// Recursively list files under `root` (relative, `/`-separated) and return
/// those matching any of the CodeBuild `artifacts.files` glob patterns.
fn match_artifact_files(root: &Path, patterns: &[String]) -> std::io::Result<Vec<String>> {
    let mut all: Vec<String> = Vec::new();
    collect_files(root, root, &mut all)?;
    let compiled: Vec<glob::Pattern> = patterns
        .iter()
        .filter_map(|p| glob::Pattern::new(p).ok())
        .collect();
    let mut out: Vec<String> = Vec::new();
    for rel in all.drain(..) {
        let is_match = compiled.iter().any(|pat| {
            pat.matches(&rel)
                // `dir/**` in CodeBuild includes files directly under `dir`;
                // glob requires `dir/**/*` for that, so also match the parent
                // directory form.
                || rel
                    .rsplit_once('/')
                    .map(|(d, _)| pat.matches(&format!("{d}/**")))
                    .unwrap_or(false)
        });
        if is_match && !out.contains(&rel) {
            out.push(rel);
        }
    }
    out.sort();
    Ok(out)
}

fn collect_files(root: &Path, dir: &Path, out: &mut Vec<String>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, out)?;
        } else if let Ok(rel) = path.strip_prefix(root) {
            out.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
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
    fn build_script_threads_state_and_isolates_exit() {
        // Cross-phase state is threaded (env + cwd files), phase bodies run in a
        // subshell (so a user `exit N` fails only that phase), and `&&`-chaining
        // stops a phase at its first failing command.
        let spec = parse_buildspec(
            "version: 0.2\nphases:\n  \
             pre_build:\n    commands:\n      - export TAG=1\n  \
             build:\n    commands:\n      - echo $TAG\n      - exit 1\n",
        )
        .unwrap();
        let script = build_script(&spec);
        // Each phase's commands run inside a subshell that restores + persists
        // state, so `exit 1` cannot abort the driver before the end-marker.
        assert!(script.contains(". \"$FCB_S/env\""));
        assert!(script.contains("cd \"$(cat \"$FCB_S/cwd\")\""));
        assert!(script.contains("export -p > \"$FCB_S/env\""));
        assert!(script.contains("echo $TAG &&\nexit 1"));
        // BUILD is guarded by FC_FAILED; POST_BUILD runs unconditionally.
        assert!(script.contains("if [ \"$FC_FAILED\" = 0 ]; then\nprintf '@@FCB@@S BUILD"));
        assert!(script.contains("printf '@@FCB@@S POST_BUILD"));
        assert!(script.trim_end().ends_with("exit $FC_FAILED"));
    }

    #[test]
    fn parses_markers_into_phase_results_and_logs() {
        let out = format!(
            "{MARKER}S INSTALL 1000\ninstalling deps\n{MARKER}E INSTALL 0 1500\n\
             {MARKER}S BUILD 1500\nboom\n{MARKER}E BUILD 2 1800\n"
        );
        let (phases, logs) = parse_exec_output(&out);
        assert_eq!(phases.len(), 2);
        assert_eq!(phases[0].name, "INSTALL");
        assert_eq!(phases[0].rc, 0);
        assert_eq!(phases[1].name, "BUILD");
        assert_eq!(phases[1].rc, 2);
        assert_eq!(
            logs,
            vec!["installing deps".to_string(), "boom".to_string()]
        );
        let v = buildspec_phase_value(&phases[0], Utc::now(), Utc::now());
        assert_eq!(v["phaseStatus"], "SUCCEEDED");
        assert_eq!(v["durationInSeconds"], 0); // 1000..1500ms -> 0 whole seconds
    }

    #[test]
    fn failing_phase_is_recorded_failed_and_fails_build() {
        // A BUILD phase reporting a non-zero exit must appear as a FAILED phase,
        // and any FAILED phase makes the overall build FAILED.
        let out = format!(
            "{MARKER}S INSTALL 1000\n{MARKER}E INSTALL 0 1010\n\
             {MARKER}S BUILD 1010\nrunning\n{MARKER}E BUILD 1 1020\n\
             {MARKER}S POST_BUILD 1020\n{MARKER}E POST_BUILD 0 1030\n"
        );
        let (phases, _) = parse_exec_output(&out);
        let build = phases
            .iter()
            .find(|p| p.name == "BUILD")
            .expect("BUILD present");
        assert_eq!(build.rc, 1, "BUILD must be recorded, not dropped");
        assert!(phases.iter().any(|p| p.name == "POST_BUILD" && p.rc == 0));
        assert!(phases.iter().any(|p| p.rc != 0));
        assert_eq!(
            buildspec_phase_value(build, Utc::now(), Utc::now())["phaseStatus"],
            "FAILED"
        );
    }

    #[test]
    fn missing_end_marker_is_failed_not_dropped() {
        // The shell died mid-BUILD (start, no end). The phase must be FAILED,
        // never absent (which previously let the build settle SUCCEEDED).
        let out = format!(
            "{MARKER}S INSTALL 1000\n{MARKER}E INSTALL 0 1010\n\
             {MARKER}S BUILD 1010\nboom\n"
        );
        let (phases, _) = parse_exec_output(&out);
        let build = phases
            .iter()
            .find(|p| p.name == "BUILD")
            .expect("BUILD present");
        assert_ne!(build.rc, 0, "missing end-marker must be FAILED");
        assert!(phases.iter().any(|p| p.rc != 0));
    }

    #[test]
    fn artifact_glob_matching() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("target")).unwrap();
        std::fs::write(dir.path().join("target/app.jar"), b"jar").unwrap();
        std::fs::write(dir.path().join("target/app.txt"), b"txt").unwrap();
        std::fs::write(dir.path().join("readme.md"), b"md").unwrap();

        let jars = match_artifact_files(dir.path(), &["target/*.jar".to_string()]).unwrap();
        assert_eq!(jars, vec!["target/app.jar".to_string()]);

        let all = match_artifact_files(dir.path(), &["**/*".to_string()]).unwrap();
        assert!(all.contains(&"target/app.jar".to_string()));
        assert!(all.contains(&"readme.md".to_string()));

        // A pattern that matches nothing yields an empty vec (caller errors).
        let none = match_artifact_files(dir.path(), &["nonexistent/*.zip".to_string()]).unwrap();
        assert!(none.is_empty());
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
        assert!(!env_truthy("FAKECLOUD_CODEBUILD_DISABLE_BACKEND_UNSET_XYZ"));
    }
}
