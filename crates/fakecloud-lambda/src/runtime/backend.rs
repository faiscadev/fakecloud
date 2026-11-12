//! Pluggable Lambda execution backend abstraction.
//!
//! The facade in [`super::facade::LambdaRuntime`] owns the warm-pool
//! bookkeeping, per-function startup serialization, and the HTTP
//! invocation logic that's identical across backends (through the Runtime
//! API fakecloud serves, see [`super::api`]). Backends only
//! implement how to bring up a fresh runtime instance for a function
//! (Docker container, Kubernetes pod, ...), how to tear it down, and
//! optionally how to sweep instances left behind by a previous
//! fakecloud process.

use async_trait::async_trait;
use fakecloud_core::auth::SessionCredentials;

use crate::state::LambdaFunction;

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
    /// The deployment package is not a readable ZIP archive.
    #[error("ZIP extraction failed: {0}")]
    ZipExtractionFailed(String),
    /// Unpacking the package failed on fakecloud's side (temporary
    /// directory, full disk, a worker task dying), not because of the
    /// package.
    #[error("could not unpack the deployment package locally: {0}")]
    LocalIo(String),
}

/// Opaque per-backend identifier for a launched runtime instance. The
/// facade hands this back to the backend on teardown so the backend can
/// find the right resource to delete.
#[derive(Debug, Clone)]
pub enum BackendHandle {
    Container { id: String },
    Pod { namespace: String, name: String },
}

/// How an instance's runtime process ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct InstanceExit {
    /// The exit status, for a process that exited.
    pub code: Option<i64>,
    /// The signal that killed it, for a process that was killed.
    pub signal: Option<i32>,
    /// Killed for exceeding its memory limit.
    pub oom: bool,
}

impl InstanceExit {
    /// From a container runtime's exit code, which reports a process killed
    /// by signal `n` as `128 + n`.
    pub fn from_container_code(code: i64, oom: bool) -> Self {
        if (129..=192).contains(&code) {
            Self {
                code: None,
                signal: Some((code - 128) as i32),
                oom,
            }
        } else {
            Self {
                code: Some(code),
                signal: None,
                oom,
            }
        }
    }

    /// The reason Lambda gives for the exit, as Go's `exec.ExitError` renders
    /// it: `exit status 3`, `signal: killed`; `None` for a clean exit.
    pub fn reason(&self) -> Option<String> {
        if let Some(signal) = self.signal {
            return Some(format!("signal: {}", signal_name(signal)));
        }
        match self.code {
            Some(0) | None => None,
            Some(code) => Some(format!("exit status {code}")),
        }
    }
}

/// Go's `syscall.Signal` names (Linux), which Lambda's exit messages use.
fn signal_name(signal: i32) -> String {
    match signal {
        1 => "hangup",
        2 => "interrupt",
        3 => "quit",
        4 => "illegal instruction",
        5 => "trace/breakpoint trap",
        6 => "aborted",
        7 => "bus error",
        8 => "floating point exception",
        9 => "killed",
        10 => "user defined signal 1",
        11 => "segmentation fault",
        12 => "user defined signal 2",
        13 => "broken pipe",
        14 => "alarm clock",
        15 => "terminated",
        _ => return format!("signal {signal}"),
    }
    .to_string()
}

/// Watches an instance's runtime process: `None` while it runs, the exit
/// once it has ended (or the instance disappeared).
pub type ExitWatch = tokio::sync::watch::Receiver<Option<InstanceExit>>;

/// Where an instance's runtime reaches the Runtime API: the per-instance
/// host name (see [`super::api::sandbox_host`]) the backend maps to the
/// fakecloud listener inside the instance, on the port it reaches fakecloud
/// at. Exported as `AWS_LAMBDA_RUNTIME_API=<host>:<port>`.
#[derive(Clone)]
pub struct RuntimeApiAddress {
    pub host: String,
    /// The instance's Runtime API state, for backends whose runtime runs
    /// in-process (test doubles, see
    /// [`super::api::spawn_in_process_runtime`]).
    pub sandbox: std::sync::Arc<super::api::Sandbox>,
}

/// What [`LambdaBackend::launch`] returns: the backend-specific handle
/// handed back on `terminate`, and a watch on the runtime process.
#[derive(Debug, Clone)]
pub struct WarmInstance {
    pub handle: BackendHandle,
    pub exit: ExitWatch,
    /// The instance runs the extension launcher (see
    /// [`super::extensions`]); its sandbox waits for the launcher's count.
    pub launcher: bool,
}

#[async_trait]
pub trait LambdaBackend: Send + Sync + 'static {
    /// How this backend's functions reach fakecloud, for a URL handed to one.
    fn callback_endpoint(&self) -> Option<String> {
        None
    }

    /// Short identifier surfaced via logs and introspection (e.g.
    /// `"docker"`, `"podman"`, `"kubernetes"`).
    fn name(&self) -> &str;

    /// Launch a fresh runtime instance for `func` using the supplied
    /// code (for zip-package functions) or `func.image_uri` (for image
    /// package functions). `layers` are the attached layer ZIPs in
    /// attach order. `deploy_id` is the facade-computed fingerprint
    /// used to label resources so reaper logic can correlate.
    /// `credentials` are the execution role's session credentials for this
    /// instance, exported into its environment (see
    /// [`super::environment::function_environment`]). `runtime_api` is the
    /// Runtime API host the instance's runtime must reach fakecloud by.
    ///
    /// Returns once the runtime process has started; its initialization runs
    /// on, and ends when it first asks the Runtime API for an event.
    async fn launch(
        &self,
        func: &LambdaFunction,
        code_zip: Option<&[u8]>,
        layers: &[Vec<u8>],
        deploy_id: &str,
        credentials: Option<&SessionCredentials>,
        runtime_api: &RuntimeApiAddress,
    ) -> Result<WarmInstance, RuntimeError>;

    /// Whether a function's tags shape the instances this backend launches
    /// (k8s scheduling overrides), so a tag change needs a fresh instance.
    fn launch_uses_tags(&self) -> bool {
        false
    }

    /// Tear down one instance. Must be idempotent — the facade may call
    /// this against an already-gone instance during cleanup races.
    async fn terminate(&self, handle: &BackendHandle);

    /// Sweep instances that belong to a previous fakecloud process so
    /// their function names don't leak across restarts. Default no-op;
    /// backends with out-of-process state should override.
    async fn reap_stale(&self) {}

    /// Optional hook: pre-warm the runtime image so the first `launch()`
    /// for a function doesn't pay the cold-pull cost. Called in the
    /// background after `CreateFunction` persists; backends that don't
    /// benefit from pre-pulling (e.g. Kubernetes, which pulls images on
    /// the scheduling node anyway) leave this as a no-op.
    ///
    /// `image` is the registry URI fetched from `runtime_to_image` for
    /// Zip-package functions, or the user-supplied `ImageUri` (already
    /// translated to the local registry if applicable) for Image-package
    /// functions.
    async fn prepull_image(&self, _image: &str) -> Result<(), RuntimeError> {
        Ok(())
    }

    /// Read a file from the instance's filesystem (the launcher's
    /// bookkeeping); `None` when it does not exist (yet).
    async fn read_instance_file(&self, _handle: &BackendHandle, _path: &str) -> Option<Vec<u8>> {
        None
    }

    /// Create (or replace) a file in the instance's filesystem. `false` on
    /// failure.
    async fn write_instance_file(
        &self,
        _handle: &BackendHandle,
        _path: &str,
        _contents: &[u8],
    ) -> bool {
        false
    }

    /// The instance's current position in its log (an opaque cursor), taken
    /// just before an event is dispatched so [`Self::instance_logs`] returns
    /// only what that invocation printed. `None` when the log is empty or
    /// the backend keeps no logs.
    async fn log_cursor(&self, _handle: &BackendHandle) -> Option<String> {
        None
    }

    /// The instance's stdout and stderr, interleaved in the order they were
    /// written, from after `since` (a [`Self::log_cursor`]; the whole log
    /// when `None`), used to build the `X-Amz-Log-Result` tail an `Invoke`
    /// with `LogType=Tail` returns. Default `None` (no logs available).
    async fn instance_logs(&self, _handle: &BackendHandle, _since: Option<&str>) -> Option<String> {
        None
    }
}
