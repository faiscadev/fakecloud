//! Backend-agnostic Lambda runtime facade.
//!
//! Owns the warm-pool bookkeeping, per-function startup serialization,
//! and the invocation path: events go to each instance's runtime through the
//! Runtime API fakecloud serves (see [`super::api`]), which reports exactly
//! how each one ended. Dispatches container lifecycle to whatever
//! [`LambdaBackend`] it was constructed with.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use fakecloud_core::auth::{SessionCredentialIssuer, SessionCredentials};
use fakecloud_core::delivery::LambdaInvocationOutput;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use super::api::{
    new_trace_id, Abandoned, Dispatched, InvocationEvent, InvocationOutcome, RuntimeApi,
    RuntimeSignal, Sandbox, MAX_SYNC_RESPONSE_BYTES,
};
use super::backend::{
    BackendHandle, InstanceExit, LambdaBackend, RuntimeApiAddress, RuntimeError, WarmInstance,
};
use super::docker::DockerBackend;
use crate::state::LambdaFunction;

/// A running runtime instance kept warm for reuse.
pub(crate) struct WarmEntry {
    instance: WarmInstance,
    /// The instance's Runtime API state: its runtime takes events and
    /// reports outcomes through it.
    sandbox: Arc<Sandbox>,
    /// When the instance was launched, for the first invocation's
    /// `Init Duration`.
    launched_at: Instant,
    last_used: RwLock<Instant>,
    /// Combined fingerprint of the function's code SHA-256, the launch
    /// configuration baked into the instance (see [`launch_config`]), and
    /// the SHA-256 of every attached layer's ZIP bytes, joined in attach
    /// order. Layers mutate `/opt`, so a layer change invalidates the
    /// warm instance even when the function code is unchanged.
    deploy_id: String,
    /// The execution role's session credentials exported into this
    /// instance's environment; revoked when the instance is torn down.
    credentials: Option<SessionCredentials>,
    /// Set when the instance must not take another invocation (its
    /// credentials were dropped by an IAM reset, or its version deleted). A
    /// busy instance finishes its current invocation and is retired once
    /// free.
    retiring: AtomicBool,
    /// Its invocation has answered and the instance is finishing the invoke
    /// phase (the runtime and extensions getting back to their next call).
    finishing: AtomicBool,
    /// Held for the duration of a single invocation against this
    /// instance. Lambda hands an execution environment exactly one event
    /// at a time; acquiring this lock before dispatching guarantees one
    /// in-flight event per instance, and lets the pool pick a *free*
    /// instance via `try_lock`.
    busy: Arc<tokio::sync::Mutex<()>>,
}

/// Default cap on warm instances per function. Real Lambda scales
/// execution environments with concurrent demand; we bound it so a
/// burst can't spawn unbounded containers/Pods. Override with
/// `FAKECLOUD_LAMBDA_MAX_CONCURRENCY`. Beyond the cap, invocations queue
/// on a busy instance rather than starting a new one.
const DEFAULT_MAX_CONCURRENCY: usize = 10;

/// Max attempts per invocation when a reserved warm instance turns out to be
/// gone. A failover only happens when the instance's runtime never took the
/// event, so retrying can't double-execute. Bounded so an all-dead pool can't
/// spin forever.
const MAX_INVOKE_ATTEMPTS: u32 = 5;

/// How long extensions get to handle `SHUTDOWN` before their environment is
/// torn down.
const EXTENSION_SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

/// Lifetime of the execution-role session minted for each instance: the
/// longest an IAM role session can last.
const EXECUTION_CREDENTIALS_LIFETIME: Duration = Duration::from_secs(12 * 60 * 60);

/// Instances stop taking new invocations this long before their session
/// expires: Lambda's maximum function timeout plus a minute, so an
/// invocation never runs past its credentials.
const INVOCATION_HEADROOM: Duration = Duration::from_secs(16 * 60);

/// Key of a function's warm pool: its qualified ARN (`<function-arn>:<version>`).
/// Accounts, regions and versions never share or evict each other's instances.
fn pool_key(func: &LambdaFunction) -> String {
    format!("{}:{}", func.function_arn, func.version)
}

/// Function name inside a pool key
/// (`arn:<p>:lambda:<region>:<account>:function:<name>:<version>`).
fn pool_key_function_name(key: &str) -> &str {
    key.split(':').nth(6).unwrap_or(key)
}

/// Whether `key` is a pool of the function whose unqualified ARN is
/// `function_arn` (any version).
fn pool_key_belongs_to(key: &str, function_arn: &str) -> bool {
    key.strip_prefix(function_arn)
        .is_some_and(|rest| rest.starts_with(':'))
}

/// The role the execution session is minted for: the function's role, in the
/// function's account (see [`crate::service::role_in_account`]).
fn session_role_arn(role_arn: &str, function_arn: &str) -> String {
    match fakecloud_aws::arn::account_of(function_arn) {
        Some(function_account) => crate::service::role_in_account(role_arn, function_account),
        None => role_arn.to_string(),
    }
}

/// Whether `entry` can take a new invocation of `deploy_id`: same code +
/// launch configuration, not being retired, and credentials that outlive a
/// full invocation (judged from their own expiration, not the launch time).
/// Cheap: no locks beyond the entry's own fields.
fn is_current(entry: &WarmEntry, deploy_id: &str) -> bool {
    let headroom = chrono::Duration::from_std(INVOCATION_HEADROOM).expect("headroom fits");
    entry.deploy_id == deploy_id
        && !entry.retiring.load(Ordering::Acquire)
        && entry
            .credentials
            .as_ref()
            .is_none_or(|c| c.expiration - chrono::Utc::now() > headroom)
}

/// Execution-role credentials minted for an instance that is still being
/// launched. Revoked on drop (a failed launch, or the launching future being
/// cancelled) unless [`disarm`](Self::disarm)ed once the instance is pooled.
struct PendingCredentials<'a> {
    issuer: Option<&'a Arc<dyn SessionCredentialIssuer>>,
    credentials: Option<SessionCredentials>,
}

impl PendingCredentials<'_> {
    fn disarm(mut self) {
        self.credentials = None;
    }
}

impl Drop for PendingCredentials<'_> {
    fn drop(&mut self) {
        if let (Some(issuer), Some(creds)) = (self.issuer, &self.credentials) {
            issuer.revoke(creds);
        }
    }
}

/// The Runtime API sandbox of an instance that is still being launched.
/// Unregistered on drop (a failed or cancelled launch) unless
/// [`disarm`](Self::disarm)ed once the instance is pooled.
struct PendingSandbox<'a> {
    api: &'a RuntimeApi,
    sandbox: Option<Arc<Sandbox>>,
}

impl PendingSandbox<'_> {
    fn get(&self) -> &Arc<Sandbox> {
        self.sandbox
            .as_ref()
            .expect("sandbox present until disarmed")
    }

    fn disarm(mut self) -> Arc<Sandbox> {
        self.sandbox.take().expect("sandbox present until disarmed")
    }
}

impl Drop for PendingSandbox<'_> {
    fn drop(&mut self) {
        if let Some(sandbox) = &self.sandbox {
            self.api.unregister(sandbox);
        }
    }
}

/// A reserved invocation slot: a warm instance plus the held busy guard
/// that grants exclusive use of it until the guard drops.
struct Slot {
    entry: Arc<WarmEntry>,
    /// Held only for its drop, which frees the instance.
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

/// Compute the warm-instance key for a function with its current layer
/// set. Stable across calls — layer ARNs are immutable in AWS, so the
/// hash of their bytes is the right cache key.
///
/// Encoded with `URL_SAFE_NO_PAD` so the result never contains `/`, `+`,
/// or `=`. The id is spliced raw into the init-container artifact URL
/// (`.../_internal/code/{account}/{region}/{function}/{deploy}.zip`) and into the
/// `fakecloud-deploy-id` Pod label; standard base64's `/` would grow an
/// extra URL path segment, break the axum route match, and wedge the Pod
/// in a cold-start loop for ~49% of deploys (issue #1643).
///
/// `with_tags` folds the function's tags in, for backends whose instances
/// are shaped by them (k8s scheduling); elsewhere TagResource must not
/// cold-start the function.
fn deploy_id_for(func: &LambdaFunction, layers: &[Vec<u8>], with_tags: bool) -> String {
    deploy_id_from(&func.code_sha256, &launch_config(func, with_tags), layers)
}

/// The function configuration an instance is started with: everything that
/// ends up in its environment, command, sandbox limits, or (with `with_tags`)
/// scheduling. A change to any of it must start a fresh instance rather than
/// reuse one configured for something else. Identity (account, version) is
/// the pool key, not part of this.
fn launch_config(func: &LambdaFunction, with_tags: bool) -> String {
    serde_json::json!([
        func.role,
        func.runtime,
        func.handler,
        func.timeout,
        func.memory_size,
        func.environment,
        func.package_type,
        func.image_uri,
        func.image_config,
        func.ephemeral_storage_size,
        func.logging_config,
        func.architectures,
        with_tags.then_some(&func.tags),
    ])
    .to_string()
}

/// Pure core of [`deploy_id_for`], split out so the URL-path-safety
/// invariant can be tested without constructing a full `LambdaFunction`.
fn deploy_id_from(code_sha256: &str, launch_config: &str, layers: &[Vec<u8>]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(code_sha256.as_bytes());
    hasher.update(b"\0");
    hasher.update(launch_config.as_bytes());
    for bytes in layers {
        let mut layer_hasher = Sha256::new();
        layer_hasher.update(bytes);
        hasher.update(b":");
        hasher.update(layer_hasher.finalize());
    }
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hasher.finalize())
}

/// Lambda's init limit: how long a runtime may take from process start to its
/// first `/invocation/next` before the init is redone within the invocation.
const INIT_LIMIT: Duration = Duration::from_secs(10);

/// How long a new invocation waits for an instance finishing its invoke
/// phase before starting a fresh one.
const FINISH_GRACE: Duration = Duration::from_millis(250);

/// `SHUTDOWN` reasons, as Lambda sends them to extensions.
pub(crate) const SHUTDOWN_SPINDOWN: &str = "spindown";
pub(crate) const SHUTDOWN_TIMEOUT: &str = "timeout";
pub(crate) const SHUTDOWN_FAILURE: &str = "failure";

/// When an event dispatched at `dispatched_at` must be answered by, given
/// when (if yet) the runtime took it. An environment that has served events
/// is idle and ready when it is handed one (its slot stays busy through the
/// whole invoke phase), so it has the function timeout. A first event waits
/// on Lambda's 10-second init limit; an init that overruns it is redone
/// within the invocation, so the invocation is then bounded by the function
/// timeout from the end of the init limit, init and handler together.
fn answer_deadline(
    first: bool,
    dispatched_at: tokio::time::Instant,
    delivered_at: Option<tokio::time::Instant>,
    timeout: Duration,
) -> tokio::time::Instant {
    let suppressed_init_deadline = dispatched_at + INIT_LIMIT + timeout;
    match delivered_at {
        None if !first => dispatched_at + timeout,
        None => suppressed_init_deadline,
        Some(at) if !first || at <= dispatched_at + INIT_LIMIT => at + timeout,
        Some(_) => suppressed_init_deadline,
    }
}

/// The error object of an invocation that ran past the function timeout.
fn timeout_payload(request_id: &str, timeout_secs: i64) -> Vec<u8> {
    serde_json::json!({
        "errorType": "Sandbox.Timedout",
        "errorMessage": format!(
            "RequestId: {request_id} Error: Task timed out after {timeout_secs}.00 seconds"
        ),
    })
    .to_string()
    .into_bytes()
}

/// The error object of an invocation whose runtime process ended before
/// answering it.
fn exit_payload(request_id: &str, exit: &InstanceExit) -> Vec<u8> {
    let message = match exit.reason() {
        Some(reason) => {
            format!("RequestId: {request_id} Error: Runtime exited with error: {reason}")
        }
        None => format!("RequestId: {request_id} Error: Runtime exited without providing a reason"),
    };
    let error_type = if exit.oom {
        "Runtime.OutOfMemory"
    } else {
        "Runtime.ExitError"
    };
    serde_json::json!({ "errorType": error_type, "errorMessage": message })
        .to_string()
        .into_bytes()
}

/// The error object of a response over its limit (6 MB buffered, 200 MB
/// streamed).
fn response_too_large_payload(limit: usize) -> Vec<u8> {
    serde_json::json!({
        "errorMessage": format!(
            "Response payload size exceeded maximum allowed payload size ({limit} bytes)."
        ),
        "errorType": "Function.ResponseSizeTooLarge",
    })
    .to_string()
    .into_bytes()
}

/// Per-invocation details the Runtime API hands the runtime with the event.
/// Every field is optional; unset ones get the values Lambda would use.
#[derive(Debug, Clone, Default)]
pub struct InvocationContext {
    /// The invocation's request ID (the Invoke response's
    /// `x-amzn-RequestId`, or an asynchronous invocation's); a fresh one
    /// when unset.
    pub request_id: Option<String>,
    /// The ARN the caller invoked (qualified when it named a version or
    /// alias); the function's own ARN when unset.
    pub invoked_function_arn: Option<String>,
    /// The caller's X-Ray trace header; a fresh trace when unset.
    pub trace_id: Option<String>,
    /// The client context JSON (`X-Amz-Client-Context`, decoded).
    pub client_context: Option<String>,
    /// The Cognito identity JSON.
    pub cognito_identity: Option<String>,
}

/// How an invocation the runtime answered ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunOutcome {
    /// The function returned; the response went to the chunk sink.
    Success,
    /// A function error, with its error object.
    Error(Vec<u8>),
}

/// What waiting on a dispatched event came to.
enum Awaited {
    /// The instance (one that had served events) went away before its
    /// runtime took the event, so the function never saw it and the
    /// invocation can run elsewhere.
    NotDelivered,
    /// The invocation ended. `reset` is set when the execution environment
    /// is no longer usable (timeout, exit, failed init), as Lambda resets
    /// it, with the `SHUTDOWN` reason its extensions get.
    Finished {
        outcome: RunOutcome,
        reset: Option<&'static str>,
        /// From the runtime taking the event to its answer.
        duration: Duration,
        /// For the event that ended the runtime's initialization: how long
        /// from launch until it took it.
        init_duration: Option<Duration>,
    },
}

/// The ARN an invocation of `func` was made through when the caller's is not
/// known: unqualified for `$LATEST`, version-qualified otherwise.
fn invoked_arn(func: &LambdaFunction) -> String {
    if func.version == "$LATEST" {
        func.function_arn.clone()
    } else {
        format!("{}:{}", func.function_arn, func.version)
    }
}

/// Follow a dispatched event to its end: the runtime's answer, its process
/// exiting, or the deadline passing (the init budget until the runtime takes
/// the event, then the function timeout from delivery).
async fn await_outcome(
    entry: &WarmEntry,
    dispatched: Dispatched,
    request_id: &str,
    timeout_secs: i64,
    on_chunk: &mut (dyn FnMut(bytes::Bytes) + Send),
) -> Awaited {
    let Dispatched { mut signals, first } = dispatched;
    let timeout = Duration::from_secs(timeout_secs.max(1) as u64);
    let dispatched_at = tokio::time::Instant::now();
    let mut delivered: Option<tokio::time::Instant> = None;
    let mut exit = entry.instance.exit.clone();
    let mut exited: Option<InstanceExit> = None;
    let mut exit_watched = true;
    let elapsed = |at: Option<tokio::time::Instant>| at.map(|t| t.elapsed()).unwrap_or_default();
    let launched_at = entry.launched_at;
    let init_duration = |at: Option<tokio::time::Instant>| {
        first.then(|| {
            at.map_or_else(
                || launched_at.elapsed(),
                |t| t.into_std().saturating_duration_since(launched_at),
            )
        })
    };
    loop {
        // Once the process is known to be gone, only drain what the runtime
        // reported before it went.
        let signal = if exited.is_some() {
            signals.try_recv().ok()
        } else {
            let deadline = answer_deadline(first, dispatched_at, delivered, timeout);
            let mut timed_out = false;
            let mut closed = false;
            let signal = tokio::select! {
                biased;
                signal = signals.recv() => {
                    // `None`: the sandbox was torn down under the invocation
                    // (the function was deleted, or the server is resetting).
                    closed = signal.is_none();
                    signal
                }
                changed = exit.wait_for(Option::is_some), if exit_watched => {
                    match changed {
                        Ok(e) => exited = Some(e.unwrap_or_default()),
                        // The backend stopped watching (it has no exit
                        // information): only the deadline is left.
                        Err(_) => exit_watched = false,
                    }
                    continue;
                }
                () = tokio::time::sleep_until(deadline) => {
                    timed_out = true;
                    None
                }
            };
            if closed {
                exited = Some(exit.borrow().unwrap_or_default());
            }
            if timed_out {
                // The runtime took the event at the deadline: its
                // `Delivered` is waiting in the channel.
                if entry.sandbox.abandon(request_id) == Abandoned::Nothing && delivered.is_none() {
                    continue;
                }
                if delivered.is_none() {
                    tracing::warn!(
                        sandbox = %entry.sandbox.host(),
                        "Lambda runtime never asked the Runtime API for an event; \
                         check that the instance can reach fakecloud"
                    );
                }
                return Awaited::Finished {
                    outcome: RunOutcome::Error(timeout_payload(request_id, timeout_secs)),
                    reset: Some(SHUTDOWN_TIMEOUT),
                    duration: elapsed(delivered),
                    init_duration: init_duration(delivered),
                };
            }
            signal
        };
        let Some(signal) = signal else {
            let exit = exited.unwrap_or_default();
            // An environment that served events and went away before taking
            // this one never ran it: it can run elsewhere. One that never
            // got as far as its first event failed its init.
            if delivered.is_none() && !first {
                return Awaited::NotDelivered;
            }
            entry.sandbox.abandon(request_id);
            return Awaited::Finished {
                outcome: RunOutcome::Error(exit_payload(request_id, &exit)),
                reset: Some(SHUTDOWN_FAILURE),
                duration: elapsed(delivered),
                init_duration: init_duration(delivered),
            };
        };
        match signal {
            RuntimeSignal::Delivered { .. } => {
                delivered = Some(tokio::time::Instant::now());
            }
            RuntimeSignal::Chunk(chunk) => on_chunk(chunk),
            RuntimeSignal::Done(outcome) => {
                let duration = elapsed(delivered);
                let (outcome, reset) = match outcome {
                    InvocationOutcome::Response => (RunOutcome::Success, None),
                    InvocationOutcome::Error(payload) => (RunOutcome::Error(payload), None),
                    InvocationOutcome::InitError(payload) => {
                        (RunOutcome::Error(payload), Some(SHUTDOWN_FAILURE))
                    }
                    InvocationOutcome::ResponseTooLarge(limit) => {
                        (RunOutcome::Error(response_too_large_payload(limit)), None)
                    }
                };
                return Awaited::Finished {
                    outcome,
                    reset,
                    duration,
                    init_duration: init_duration(delivered),
                };
            }
        }
    }
}

/// The log of one invocation as `LogType=Tail` returns it: the platform's
/// `START` / `END` / `REPORT` lines around what the function printed.
fn invocation_log(
    version: &str,
    memory_size: i64,
    request_id: &str,
    duration: Duration,
    init_duration: Option<Duration>,
    function_output: &str,
) -> String {
    let ms = duration.as_secs_f64() * 1000.0;
    let billed = (ms.ceil() as u64).max(1);
    let mut report = format!(
        "REPORT RequestId: {request_id}\tDuration: {ms:.2} ms\tBilled Duration: {billed} ms\tMemory Size: {memory_size} MB\t",
    );
    if let Some(init) = init_duration {
        let init_ms = init.as_secs_f64() * 1000.0;
        report.push_str(&format!("Init Duration: {init_ms:.2} ms\t"));
    }
    let mut out = format!("START RequestId: {request_id} Version: {version}\n");
    out.push_str(function_output);
    if !function_output.is_empty() && !function_output.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&format!("END RequestId: {request_id}\n{report}\n"));
    out
}

/// Every function's pool of warm instances, by [`pool_key`].
type Pools = RwLock<HashMap<String, Vec<Arc<WarmEntry>>>>;

/// The parts of the runtime that tear instances down, shared with detached
/// invocations so they can reset the instance they ran on.
#[derive(Clone)]
struct Teardown {
    backend: Arc<dyn LambdaBackend>,
    runtime_api: Arc<RuntimeApi>,
    instances: Arc<Pools>,
    credential_issuer: Arc<OnceLock<Arc<dyn SessionCredentialIssuer>>>,
}

impl Teardown {
    /// Tear down one instance and revoke the credentials it was given.
    /// Extensions registered for `SHUTDOWN` get it with `reason` and a
    /// moment to finish first (Lambda allows up to 2s).
    async fn retire(&self, entry: &WarmEntry, reason: &str) {
        entry
            .sandbox
            .shutdown_extensions(reason, EXTENSION_SHUTDOWN_GRACE)
            .await;
        self.runtime_api.unregister(&entry.sandbox);
        self.backend.terminate(&entry.instance.handle).await;
        if let (Some(issuer), Some(creds)) =
            (self.credential_issuer.get(), entry.credentials.as_ref())
        {
            issuer.revoke(creds);
        }
    }

    /// Remove one specific instance from a function's pool and tear it down
    /// (it is gone, stuck, or was reset).
    async fn evict(&self, key: &str, target: &Arc<WarmEntry>, reason: &str) {
        target.retiring.store(true, Ordering::Release);
        let removed = {
            let mut map = self.instances.write();
            match map.get_mut(key) {
                Some(pool) => {
                    let removed = pool
                        .iter()
                        .position(|e| Arc::ptr_eq(e, target))
                        .map(|pos| pool.remove(pos));
                    if pool.is_empty() {
                        map.remove(key);
                    }
                    removed
                }
                None => None,
            }
        };
        if let Some(entry) = removed {
            tracing::info!(
                pool = %key,
                handle = ?entry.instance.handle,
                reason,
                "resetting Lambda runtime instance"
            );
            self.retire(&entry, reason).await;
        }
    }
}

/// What a detached invocation reports to its caller.
enum Progress {
    Chunk(bytes::Bytes),
    /// The instance went away (or was stuck) before its runtime took the
    /// event; it has been evicted and the event can run elsewhere.
    NotDelivered,
    Finished {
        outcome: RunOutcome,
        logs: Option<String>,
    },
}

/// The log tail an invocation asked for (`LogType=Tail`).
struct LogRequest {
    /// The backend's log position before the event was dispatched.
    cursor: Option<String>,
    version: String,
    memory_size: i64,
}

/// One dispatched invocation, followed to its end in a task of its own.
struct FollowedInvocation {
    /// Holds the instance for the invocation's whole life.
    slot: Slot,
    dispatched: Dispatched,
    request_id: String,
    timeout_secs: i64,
    log: Option<LogRequest>,
    key: String,
    teardown: Teardown,
    tx: tokio::sync::mpsc::UnboundedSender<Progress>,
}

/// Follow an invocation whatever happens to its caller: forward the
/// response, enforce the deadline, and reset the instance when Lambda would.
/// The instance stays reserved until the runtime has answered or the
/// instance is reset, so no other invocation lands on a runtime still busy
/// with this event.
async fn follow_invocation(inv: FollowedInvocation) {
    let FollowedInvocation {
        slot,
        dispatched,
        request_id,
        timeout_secs,
        log,
        key,
        teardown,
        tx,
    } = inv;
    let entry = slot.entry.clone();
    let chunks = tx.clone();
    let awaited = await_outcome(
        &entry,
        dispatched,
        &request_id,
        timeout_secs,
        &mut |chunk| {
            let _ = chunks.send(Progress::Chunk(chunk));
        },
    )
    .await;
    match awaited {
        Awaited::NotDelivered => {
            drop(slot);
            teardown.evict(&key, &entry, SHUTDOWN_FAILURE).await;
            let _ = tx.send(Progress::NotDelivered);
        }
        Awaited::Finished {
            outcome,
            reset,
            duration,
            init_duration,
        } => {
            *entry.last_used.write() = Instant::now();
            // Read the log tail while the instance is still alive.
            let logs = match log {
                Some(log) => {
                    let output = teardown
                        .backend
                        .instance_logs(&entry.instance.handle, log.cursor.as_deref())
                        .await
                        .unwrap_or_default();
                    Some(invocation_log(
                        &log.version,
                        log.memory_size,
                        &request_id,
                        duration,
                        init_duration,
                        &output,
                    ))
                }
                None => None,
            };
            // The caller hears back as soon as the response is in; the
            // instance stays reserved through the rest of the invoke phase.
            if reset.is_some() {
                entry.retiring.store(true, Ordering::Release);
            } else {
                entry.finishing.store(true, Ordering::Release);
            }
            let _ = tx.send(Progress::Finished { outcome, logs });
            let reset = match reset {
                Some(reason) => Some(reason),
                // Lambda's invoke phase ends when the runtime and every
                // extension have asked for their next event; work they do
                // after the response still counts against the timeout.
                None => {
                    let remaining =
                        Duration::from_secs(timeout_secs.max(1) as u64).saturating_sub(duration);
                    let mut exit = entry.instance.exit.clone();
                    tokio::select! {
                        idle = entry.sandbox.wait_idle() => (!idle).then_some(SHUTDOWN_FAILURE),
                        Ok(_) = exit.wait_for(Option::is_some) => Some(SHUTDOWN_FAILURE),
                        () = tokio::time::sleep(remaining) => Some(SHUTDOWN_TIMEOUT),
                    }
                }
            };
            if let Some(reason) = reset {
                // Torn down while still reserved, so nothing lands on it.
                teardown.evict(&key, &entry, reason).await;
            }
            entry.finishing.store(false, Ordering::Release);
            drop(slot);
        }
    }
}

/// Follow an instance's extension launcher for the instance's whole life:
/// learn how many extensions it started (a launcher that never reports, an
/// unwritable bookkeeping directory say, fails the init loudly), let the
/// runtime start once they have all registered, and fail the environment
/// with Lambda's `Extension.*` error when one cannot be launched or exits,
/// during init or during an invocation.
///
/// Every read and write of the launcher's files goes through the backend's
/// container CLI (or Kubernetes exec), so each is bounded by
/// [`LAUNCHER_IO_TIMEOUT`]: one that times out is "not known yet" and is
/// retried. The launcher's 10-second report deadline ([`INIT_LIMIT`]) is
/// enforced against the clock: no read starts after it, and a read already
/// under way when it passes may still finish (within its own bound), so a
/// slow container CLI delays the verdict but does not fail a launch that
/// reported in time.
///
/// The exit record is polled for the instance's whole life, every
/// [`BUSY_EXTENSION_POLL`] while the environment works (an extension may be
/// busy with its `INVOKE` and hold no request open) and every
/// [`IDLE_EXTENSION_POLL`] between invocations (each read costs a CLI
/// process). Polling the record is the only exit signal: Lambda freezes an
/// idle environment, but fakecloud does not, so an extension's HTTP client
/// may time out its `/event/next` long poll and ask again, and a dropped
/// request says nothing about whether its process ended. A dropped request
/// does take the environment out of idle until the extension asks again,
/// which moves the record to the busy rate.
async fn monitor_launcher(teardown: Teardown, entry: std::sync::Weak<WarmEntry>, budget: Duration) {
    use super::extensions::{exited_extensions, EXITED_FILE, GO_FILE, LAUNCHED_FILE};
    let started = tokio::time::Instant::now();
    let poll = Duration::from_millis(50);
    // `None` when the file is missing, unreadable, or the read did not
    // finish within `limit`.
    let read = |path: &'static str, limit: Duration| {
        let teardown = teardown.clone();
        let entry = entry.clone();
        async move {
            let entry = entry.upgrade()?;
            let bytes = tokio::time::timeout(
                limit,
                teardown
                    .backend
                    .read_instance_file(&entry.instance.handle, path),
            )
            .await
            .ok()??;
            Some(String::from_utf8_lossy(&bytes).into_owned())
        }
    };
    let Some(sandbox) = entry.upgrade().map(|e| e.sandbox.clone()) else {
        return;
    };
    let launch_deadline = started + INIT_LIMIT;
    let launcher_failed = || {
        tracing::warn!(
            sandbox = %sandbox.host(),
            "the extension launcher never reported the extensions it started"
        );
        sandbox.launcher_failed("the extension launcher did not report the extensions it started");
    };
    let count = loop {
        // A read started before the deadline finishes, bounded by
        // `LAUNCHER_IO_TIMEOUT`, even if the deadline passes meanwhile.
        let count = read(LAUNCHED_FILE, LAUNCHER_IO_TIMEOUT)
            .await
            .and_then(|t| t.trim().parse::<usize>().ok());
        if let Some(n) = count {
            break n;
        }
        let gone = entry
            .upgrade()
            .is_none_or(|e| e.instance.exit.borrow().is_some());
        if gone || sandbox.is_closed_or_failed() {
            return;
        }
        let remaining = launch_deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            launcher_failed();
            return;
        }
        tokio::time::sleep(poll.min(remaining)).await;
    };
    sandbox.expect_extensions(count);
    if count == 0 {
        return;
    }
    let mut reported = 0;
    let mut released = false;
    loop {
        let Some(live) = entry.upgrade() else { return };
        // Once `SHUTDOWN` was sent, an extension ending is expected and
        // reports nothing, so there is nothing left to watch for.
        if sandbox.is_closed_or_failed()
            || sandbox.is_shutting_down()
            || live.instance.exit.borrow().is_some()
        {
            return;
        }
        if !released {
            match tokio::time::timeout(poll, sandbox.wait_registered()).await {
                Ok(true) => {
                    released = tokio::time::timeout(
                        LAUNCHER_IO_TIMEOUT,
                        teardown
                            .backend
                            .write_instance_file(&live.instance.handle, GO_FILE, b""),
                    )
                    .await
                    .unwrap_or(false);
                }
                Ok(false) => return,
                Err(_) => {}
            }
            if !released && started.elapsed() >= budget {
                // The first event times out on its own.
                return;
            }
        }
        // Extensions that ended, for the instance's whole life: during
        // init, while an invocation runs, and between invocations (the
        // next one then fails with the crash and the instance is reset).
        let idle = released && sandbox.is_idle();
        if let Some(exited) = read(EXITED_FILE, LAUNCHER_IO_TIMEOUT).await {
            let exited = exited_extensions(&exited);
            for (name, code) in exited.iter().skip(reported) {
                sandbox.extension_exited(name, *code);
            }
            reported = exited.len();
        }
        drop(live);
        if released && idle {
            // Cut short when the environment gets busy (an invocation
            // starts, or an extension stops waiting), so an exit then is
            // seen at the busy rate.
            let until_busy = async {
                while sandbox.is_idle() {
                    tokio::time::sleep(poll).await;
                }
            };
            let _ = tokio::time::timeout(IDLE_EXTENSION_POLL, until_busy).await;
        } else if released {
            tokio::time::sleep(BUSY_EXTENSION_POLL).await;
        }
    }
}

/// How often the launcher's exit record is read while an instance works,
/// and between invocations.
const BUSY_EXTENSION_POLL: Duration = Duration::from_millis(250);
const IDLE_EXTENSION_POLL: Duration = Duration::from_secs(5);
/// The longest one read or write of the launcher's files may take.
const LAUNCHER_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Retire an instance whose runtime exits while it is idle (killed, crashed,
/// Pod deleted), so no event is sent to it; one that is busy is handled by
/// its invocation. Stops watching once the instance is gone.
async fn retire_on_exit(teardown: Teardown, key: String, entry: std::sync::Weak<WarmEntry>) {
    let Some(mut exit) = entry.upgrade().map(|e| e.instance.exit.clone()) else {
        return;
    };
    loop {
        // Holding the watch must not keep a retired instance's watcher (and
        // the backend's poll behind it) alive: check in now and then.
        match tokio::time::timeout(Duration::from_secs(30), exit.wait_for(Option::is_some)).await {
            Ok(Ok(_)) => break,
            Ok(Err(_)) => return,
            Err(_) => {
                if entry
                    .upgrade()
                    .is_none_or(|e| e.retiring.load(Ordering::Acquire))
                {
                    return;
                }
            }
        }
    }
    drop(exit);
    let Some(entry) = entry.upgrade() else { return };
    let Ok(_free) = entry.busy.clone().try_lock_owned() else {
        return;
    };
    teardown.evict(&key, &entry, SHUTDOWN_FAILURE).await;
}

/// Warm instances taken out of the pool by
/// [`LambdaRuntime::take_account_instances`], awaiting termination.
pub struct TakenInstances(Vec<Arc<WarmEntry>>);

impl TakenInstances {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

pub struct LambdaRuntime {
    backend: Arc<dyn LambdaBackend>,
    /// The Runtime API every instance's runtime talks to.
    runtime_api: Arc<RuntimeApi>,
    /// Per-function pool of warm instances. Each instance serves one
    /// invocation at a time (its `busy` lock); the pool grows on demand
    /// up to `max_concurrency`, and the idle reaper trims it.
    instances: Arc<Pools>,
    /// Serializes runtime startup per function to prevent duplicate
    /// instances racing into the pool when several cold invokes arrive
    /// together.
    starting: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Cap on warm instances per function.
    max_concurrency: usize,
    /// Mints the execution-role credentials each instance is started with.
    /// Unset (no credentials exported) until the server wires IAM in.
    credential_issuer: Arc<OnceLock<Arc<dyn SessionCredentialIssuer>>>,
    /// Where asynchronous (`Event`) invocations record their outcome
    /// (`OnSuccess`/`OnFailure` destinations, dead-letter queues). Held
    /// weakly: the destinations bus reaches Lambda again through the
    /// invoker built on this runtime. Unset until the server wires it.
    async_destinations: OnceLock<std::sync::Weak<fakecloud_core::delivery::DeliveryBus>>,
}

impl LambdaRuntime {
    /// Construct a runtime over the supplied backend. Callers that want
    /// auto-detection should use [`Self::auto_detect_docker`] or
    /// [`Self::new`].
    pub fn from_backend(backend: Arc<dyn LambdaBackend>) -> Self {
        let max_concurrency = std::env::var("FAKECLOUD_LAMBDA_MAX_CONCURRENCY")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n >= 1)
            .unwrap_or(DEFAULT_MAX_CONCURRENCY);
        Self {
            backend,
            runtime_api: RuntimeApi::new(),
            instances: Arc::new(RwLock::new(HashMap::new())),
            starting: RwLock::new(HashMap::new()),
            max_concurrency,
            credential_issuer: Arc::new(OnceLock::new()),
            async_destinations: OnceLock::new(),
        }
    }

    /// Bind the bus asynchronous invocations route their outcome through.
    /// Set once at server startup; later calls are ignored.
    pub fn set_async_destinations(&self, bus: &Arc<fakecloud_core::delivery::DeliveryBus>) {
        let _ = self.async_destinations.set(Arc::downgrade(bus));
    }

    /// The bus bound by [`Self::set_async_destinations`], if still alive.
    pub fn async_destinations(&self) -> Option<Arc<fakecloud_core::delivery::DeliveryBus>> {
        self.async_destinations
            .get()
            .and_then(std::sync::Weak::upgrade)
    }

    /// Give instances execution-role credentials: each launch mints a
    /// session for the function's role (named after the function, as on
    /// AWS) and exports it into the instance environment. Set once at
    /// server startup; later calls are ignored.
    pub fn set_credential_issuer(&self, issuer: Arc<dyn SessionCredentialIssuer>) {
        let _ = self.credential_issuer.set(issuer);
    }

    fn issue_credentials(&self, func: &LambdaFunction) -> Option<SessionCredentials> {
        let issuer = self.credential_issuer.get()?;
        if func.role.is_empty() {
            return None;
        }
        let lifetime = chrono::Duration::from_std(EXECUTION_CREDENTIALS_LIFETIME)
            .expect("session lifetime fits chrono::Duration");
        let role = session_role_arn(&func.role, &func.function_arn);
        Some(issuer.issue(&role, &func.function_name, lifetime))
    }

    /// The Runtime API instances' runtimes reach fakecloud through; the
    /// server routes requests for its hosts to [`RuntimeApi::serve`].
    pub fn runtime_api(&self) -> Arc<RuntimeApi> {
        self.runtime_api.clone()
    }

    /// What a detached invocation needs to reset its instance.
    fn teardown(&self) -> Teardown {
        Teardown {
            backend: self.backend.clone(),
            runtime_api: self.runtime_api.clone(),
            instances: self.instances.clone(),
            credential_issuer: self.credential_issuer.clone(),
        }
    }

    /// Tear down one instance (it is being spun down) and revoke the
    /// credentials it was given.
    async fn retire(&self, entry: &WarmEntry) {
        self.teardown().retire(entry, SHUTDOWN_SPINDOWN).await;
    }

    /// Mark every pooled instance matching `pred` as retiring so it takes no
    /// further invocation. With `detach_free`, the free ones are also removed
    /// from the pool and returned for teardown; busy ones always stay until
    /// released (then the idle sweep or the next launch retires them).
    fn mark_retiring(
        &self,
        pred: impl Fn(&str, &WarmEntry) -> bool,
        detach_free: bool,
    ) -> Vec<Arc<WarmEntry>> {
        let mut map = self.instances.write();
        let mut detached = Vec::new();
        for (key, pool) in map.iter_mut() {
            pool.retain(|e| {
                if !pred(key, e) {
                    return true;
                }
                e.retiring.store(true, Ordering::Release);
                if detach_free && e.busy.try_lock().is_ok() {
                    detached.push(e.clone());
                    false
                } else {
                    true
                }
            });
        }
        map.retain(|_, pool| !pool.is_empty());
        detached
    }

    /// Synchronously stop handing invocations to every instance holding
    /// credentials registered in `account_id` (all accounts when `None`).
    /// Called in step with an IAM reset that drops those credentials, so no
    /// invocation can start with them afterwards; tear the instances down
    /// with [`Self::retire_released`].
    pub fn mark_credentials_revoked(&self, account_id: Option<&str>) {
        self.mark_retiring(
            |_, e| {
                e.credentials
                    .as_ref()
                    .is_some_and(|c| account_id.is_none_or(|a| c.account_id == a))
            },
            false,
        );
    }

    /// Retire every free instance already marked retiring.
    pub async fn retire_released(&self) {
        let free = self.mark_retiring(|_, e| e.retiring.load(Ordering::Acquire), true);
        self.terminate_instances(free).await;
    }

    /// DeleteFunction with a Qualifier: stop the version's instances without
    /// cutting off an in-flight invocation. Free instances are detached and
    /// returned for teardown; busy ones are marked retiring and go once free.
    pub(crate) fn retire_version(&self, function_arn: &str, version: &str) -> Vec<Arc<WarmEntry>> {
        let key = format!("{function_arn}:{version}");
        self.mark_retiring(|k, _| k == key, true)
    }

    /// Auto-detect Docker or Podman. Returns `None` if neither is available.
    /// Override with `FAKECLOUD_CONTAINER_CLI` env var.
    pub fn auto_detect_docker(server_port: u16) -> Option<Self> {
        DockerBackend::auto_detect(server_port)
            .map(|b| Self::from_backend(Arc::new(b) as Arc<dyn LambdaBackend>))
    }

    /// Backwards-compatible alias for [`Self::auto_detect_docker`].
    /// Callers across the workspace use `ContainerRuntime::new(port)`.
    pub fn new(server_port: u16) -> Option<Self> {
        Self::auto_detect_docker(server_port)
    }

    /// Construct a runtime backed by the Kubernetes backend. Reads
    /// configuration from env vars (`FAKECLOUD_K8S_SELF_URL`,
    /// `FAKECLOUD_K8S_NAMESPACE`, etc.) and connects to the cluster
    /// via in-cluster service account or kubeconfig. Hard-fails on
    /// any configuration or connectivity issue — we don't silently
    /// fall back to Docker because the operator explicitly opted in
    /// to K8s.
    ///
    /// `internal_token` is the bearer token the artifact endpoints on
    /// the fakecloud server expect from Pod init containers — caller
    /// must register the same token on those endpoints.
    pub async fn new_k8s(
        server_port: u16,
        internal_token: String,
    ) -> Result<Self, super::k8s::K8sBackendError> {
        let backend = super::k8s::K8sBackend::from_env(server_port, internal_token).await?;
        backend.reap_stale().await;
        Ok(Self::from_backend(Arc::new(backend)))
    }

    /// How functions reach fakecloud, for a URL handed to one.
    pub fn callback_endpoint(&self) -> Option<String> {
        self.backend.callback_endpoint()
    }

    pub fn cli_name(&self) -> &str {
        self.backend.name()
    }

    /// Background pre-warm hook: pull the image a Zip-package function
    /// will need at invoke time, or the `ImageUri` of an Image-package
    /// function. The first cold pull of an AWS base image (~700 MB)
    /// frequently exceeds the AWS CLI default 60s read timeout, surfacing
    /// to users as `Connection was closed` (issue #1539). Call after
    /// `CreateFunction` persists so the warm path is ready before the
    /// caller turns around and calls `Invoke`.
    ///
    /// Returns `None` if the function has no resolvable image (e.g. an
    /// unsupported runtime string we can't map to a base image).
    /// Otherwise returns the result of the backend's `prepull_image` —
    /// callers log failures and move on, since invoke time still
    /// re-attempts the pull as a fallback.
    pub async fn prepull_for_function(
        &self,
        func: &LambdaFunction,
    ) -> Option<Result<(), super::backend::RuntimeError>> {
        let image = if func.package_type == "Image" {
            func.image_uri.clone()?
        } else {
            super::docker::runtime_to_image(&func.runtime)?
        };
        Some(self.backend.prepull_image(&image).await)
    }

    /// Invoke a Lambda function, starting an instance if needed. Layer
    /// ZIPs are extracted into `/opt` of the runtime sandbox; AWS base
    /// images already include `/opt/python`, `/opt/nodejs/node_modules`,
    /// `/opt/lib`, and `/opt/bin` on the right import paths.
    ///
    /// `Ok` means the function ran; whether it failed is reported in the
    /// output's `function_error`, exactly as the runtime reported it through
    /// the Runtime API.
    pub async fn invoke(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
    ) -> Result<LambdaInvocationOutput, RuntimeError> {
        self.invoke_with(func, payload, layers, &InvocationContext::default())
            .await
            .map(|(output, _)| output)
    }

    /// [`Self::invoke`] with the caller's invocation details.
    pub async fn invoke_with(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
        ctx: &InvocationContext,
    ) -> Result<(LambdaInvocationOutput, Option<String>), RuntimeError> {
        self.invoke_buffered(func, payload, layers, ctx, false)
            .await
    }

    /// Like [`Self::invoke_with`] but also returns the invocation's log tail
    /// (for `Invoke` with `LogType=Tail` -> `X-Amz-Log-Result`). `None` when
    /// the backend can't supply logs.
    pub async fn invoke_with_log_tail(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
        ctx: &InvocationContext,
    ) -> Result<(LambdaInvocationOutput, Option<String>), RuntimeError> {
        self.invoke_buffered(func, payload, layers, ctx, true).await
    }

    async fn invoke_buffered(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
        ctx: &InvocationContext,
        capture_logs: bool,
    ) -> Result<(LambdaInvocationOutput, Option<String>), RuntimeError> {
        let mut body = Vec::new();
        let (outcome, logs) = self
            .run(
                func,
                payload,
                layers,
                ctx,
                Some(MAX_SYNC_RESPONSE_BYTES),
                capture_logs,
                &mut |chunk| body.extend_from_slice(&chunk),
            )
            .await?;
        let output = match outcome {
            RunOutcome::Success => LambdaInvocationOutput::success(body),
            RunOutcome::Error(payload) => LambdaInvocationOutput::unhandled(payload),
        };
        Ok((output, logs))
    }

    /// Invoke a Lambda function and hand each piece of the response body to
    /// `on_chunk` as the runtime sends it: a streaming handler's writes keep
    /// their boundaries, a buffered handler's result arrives whole. The
    /// outcome tells a completed response from a function error. A response
    /// past Lambda's 200 MB streaming limit is cut off there and ends with
    /// `Function.ResponseSizeTooLarge`.
    pub async fn invoke_streaming(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
        ctx: &InvocationContext,
        on_chunk: &mut (dyn FnMut(bytes::Bytes) + Send),
    ) -> Result<RunOutcome, RuntimeError> {
        self.run(
            func,
            payload,
            layers,
            ctx,
            Some(super::api::MAX_STREAM_RESPONSE_BYTES),
            false,
            on_chunk,
        )
        .await
        .map(|(outcome, _)| outcome)
    }

    /// Run one invocation on a warm (or freshly launched) instance.
    ///
    /// The event is queued on the instance's Runtime API sandbox; its runtime
    /// takes it with `/invocation/next` and answers through `/response` or
    /// `/error`. An instance found gone (or stuck) before its runtime took
    /// the event is evicted and the invocation retried on a fresh one (up to
    /// four times): the function provably never saw it. Timeouts, runtime
    /// exits and failed inits end the invocation with Lambda's error object
    /// and reset the execution environment.
    ///
    /// The invocation itself runs in a task of its own that holds the
    /// instance until the runtime answers or the function timeout resets it,
    /// so a caller that stops waiting (a dropped connection, a caller-side
    /// timeout) never frees an instance that is still running its event or
    /// lets the timeout go unenforced.
    #[allow(clippy::too_many_arguments)]
    async fn run(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
        ctx: &InvocationContext,
        max_response_bytes: Option<usize>,
        capture_logs: bool,
        on_chunk: &mut (dyn FnMut(bytes::Bytes) + Send),
    ) -> Result<(RunOutcome, Option<String>), RuntimeError> {
        let request_id = ctx
            .request_id
            .clone()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let timeout = Duration::from_secs(func.timeout.max(1) as u64);
        // An invocation without a payload hands the function `{}`, as Lambda
        // does.
        let payload = if payload.is_empty() {
            bytes::Bytes::from_static(b"{}")
        } else {
            bytes::Bytes::copy_from_slice(payload)
        };
        let key = pool_key(func);
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let slot = self.acquire_slot(func, layers).await?;
            let entry = slot.entry.clone();

            // Gone while idle (killed, crashed, Pod deleted) after serving
            // earlier events. A runtime that died during its init is not
            // skipped: that is this invocation's failure to report.
            let gone = entry.instance.exit.borrow().is_some();
            if gone && entry.sandbox.has_served() {
                drop(slot);
                self.evict_entry(&key, &entry, SHUTDOWN_FAILURE).await;
                if attempt < MAX_INVOKE_ATTEMPTS {
                    tracing::warn!(
                        function = %func.function_name,
                        "warm Lambda instance is gone; evicted, retrying with a cold start"
                    );
                    continue;
                }
                return Err(RuntimeError::InvocationFailed(format!(
                    "no live instance for {} after {attempt} attempts",
                    func.function_name
                )));
            }

            let log_cursor = if capture_logs {
                self.backend.log_cursor(&entry.instance.handle).await
            } else {
                None
            };
            let event = InvocationEvent {
                request_id: request_id.clone(),
                payload: payload.clone(),
                invoked_function_arn: ctx
                    .invoked_function_arn
                    .clone()
                    .unwrap_or_else(|| invoked_arn(func)),
                trace_id: ctx.trace_id.clone().unwrap_or_else(new_trace_id),
                client_context: ctx.client_context.clone(),
                cognito_identity: ctx.cognito_identity.clone(),
                timeout,
                max_response_bytes,
            };
            let dispatched = entry.sandbox.dispatch(event);
            let (tx, mut progress) = tokio::sync::mpsc::unbounded_channel();
            tokio::spawn(follow_invocation(FollowedInvocation {
                slot,
                dispatched,
                request_id: request_id.clone(),
                timeout_secs: func.timeout,
                log: capture_logs.then(|| LogRequest {
                    cursor: log_cursor,
                    version: func.version.clone(),
                    memory_size: func.memory_size,
                }),
                key: key.clone(),
                teardown: self.teardown(),
                tx,
            }));
            loop {
                match progress.recv().await {
                    Some(Progress::Chunk(chunk)) => on_chunk(chunk),
                    Some(Progress::Finished { outcome, logs }) => return Ok((outcome, logs)),
                    Some(Progress::NotDelivered) => break,
                    None => {
                        return Err(RuntimeError::InvocationFailed(
                            "the invocation ended without an outcome".into(),
                        ))
                    }
                }
            }
            if attempt >= MAX_INVOKE_ATTEMPTS {
                return Err(RuntimeError::InvocationFailed(format!(
                    "no live instance for {} after {attempt} attempts",
                    func.function_name
                )));
            }
            tracing::warn!(
                function = %func.function_name,
                "Lambda instance went away before taking the event; retrying with a cold start"
            );
        }
    }

    /// Reserve a warm instance to run exactly one invocation, returning a
    /// held busy guard that grants exclusive use until it drops. Shared
    /// by `invoke` and `invoke_streaming`.
    ///
    /// Order of preference: (1) a free, current-deploy instance already
    /// in the pool; (2) a freshly launched instance, if the pool is below
    /// `max_concurrency`; (3) queue on a busy current-deploy instance.
    /// Instances whose `deploy_id` no longer matches the function's
    /// current code+layers are torn down before sizing the pool.
    async fn acquire_slot(
        &self,
        func: &LambdaFunction,
        layers: &[Vec<u8>],
    ) -> Result<Slot, RuntimeError> {
        let is_image = func.package_type == "Image";
        if !is_image && func.code_zip.is_none() {
            return Err(RuntimeError::NoCodeZip(func.function_name.clone()));
        }

        let deploy_id = deploy_id_for(func, layers, self.backend.launch_uses_tags());
        let key = pool_key(func);

        loop {
            // (1) Fast path: a free instance already running the right deploy.
            if let Some(slot) = self.try_take_free(&key, &deploy_id) {
                return Ok(slot);
            }

            // Serialize launch decisions per pool so a burst of cold
            // invokes doesn't each push the pool past the cap.
            let startup_lock = {
                let mut starting = self.starting.write();
                starting
                    .entry(key.clone())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                    .clone()
            };
            let startup_guard = startup_lock.lock().await;

            // Re-check under the startup lock: another task may have freed or
            // launched an instance while we waited.
            if let Some(slot) = self.try_take_free(&key, &deploy_id) {
                return Ok(slot);
            }

            // Tear down free instances left over from a previous deploy.
            self.evict_stale_deploy(&key, &deploy_id).await;

            // Size against current instances only: stale ones still busy
            // finishing an invocation are on their way out.
            let current_len = self.instances.read().get(&key).map_or(0, |pool| {
                pool.iter().filter(|e| is_current(e, &deploy_id)).count()
            });

            // An instance that just answered is usually back in its next
            // call within moments (its runtime's round trip to the Runtime
            // API): give it that long before starting a cold one, so
            // back-to-back invocations reuse the warm environment.
            if let Some(slot) = self.wait_for_finishing(&key, &deploy_id).await {
                return Ok(slot);
            }

            // (2) Room to grow: launch a fresh instance and reserve it.
            if current_len < self.max_concurrency {
                return self.launch_slot(func, layers, &key, deploy_id).await;
            }

            // (3) At capacity: release the startup lock and wait for whichever
            // current instance frees up *first*. Racing every instance's lock
            // (rather than blocking on a fixed one) avoids convoying every
            // queued caller onto pool[0] while a different instance goes idle.
            drop(startup_guard);
            let candidates: Vec<Arc<WarmEntry>> = {
                let map = self.instances.read();
                map.get(&key)
                    .map(|pool| {
                        pool.iter()
                            .filter(|e| is_current(e, &deploy_id))
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default()
            };
            if candidates.is_empty() {
                // Every instance went away while we sized the pool; start over.
                continue;
            }
            let waiters = candidates.into_iter().map(|entry| {
                Box::pin(async move {
                    let guard = entry.busy.clone().lock_owned().await;
                    Slot {
                        entry,
                        _guard: guard,
                    }
                })
            });
            let (slot, _idx, _rest) = futures_util::future::select_all(waiters).await;
            // The instance may have been retired or evicted while we waited.
            if !is_current(&slot.entry, &deploy_id) || !self.in_pool(&key, &slot.entry) {
                continue;
            }
            *slot.entry.last_used.write() = Instant::now();
            return Ok(slot);
        }
    }

    /// Launch a fresh instance for `func`, with freshly minted execution-role
    /// credentials, and reserve it for the caller.
    async fn launch_slot(
        &self,
        func: &LambdaFunction,
        layers: &[Vec<u8>],
        key: &str,
        deploy_id: String,
    ) -> Result<Slot, RuntimeError> {
        let pending = PendingCredentials {
            issuer: self.credential_issuer.get(),
            credentials: self.issue_credentials(func),
        };
        let sandbox = PendingSandbox {
            api: &self.runtime_api,
            sandbox: Some(self.runtime_api.register_for(super::api::SandboxInfo {
                function_name: func.function_name.clone(),
                function_version: func.version.clone(),
                handler: func.handler.clone(),
            })),
        };
        let address = RuntimeApiAddress {
            host: sandbox.get().host(),
            sandbox: sandbox.get().clone(),
        };
        let launched_at = Instant::now();
        let instance = self
            .backend
            .launch(
                func,
                func.code_zip.as_deref(),
                layers,
                &deploy_id,
                pending.credentials.as_ref(),
                &address,
            )
            .await?;
        let entry = Arc::new(WarmEntry {
            instance,
            sandbox: sandbox.disarm(),
            launched_at,
            last_used: RwLock::new(Instant::now()),
            deploy_id,
            credentials: pending.credentials.clone(),
            retiring: AtomicBool::new(false),
            finishing: AtomicBool::new(false),
            busy: Arc::new(tokio::sync::Mutex::new(())),
        });
        let guard = entry
            .busy
            .clone()
            .try_lock_owned()
            .expect("freshly created busy lock is uncontended");
        self.instances
            .write()
            .entry(key.to_string())
            .or_default()
            .push(entry.clone());
        // Pooled: the entry owns the credentials now and revokes them on
        // retirement.
        pending.disarm();
        let teardown = self.teardown();
        if entry.instance.launcher {
            let budget = INIT_LIMIT + Duration::from_secs(func.timeout.max(1) as u64);
            tokio::spawn(monitor_launcher(
                teardown.clone(),
                Arc::downgrade(&entry),
                budget,
            ));
        }
        tokio::spawn(retire_on_exit(
            teardown,
            key.to_string(),
            Arc::downgrade(&entry),
        ));
        Ok(Slot {
            entry,
            _guard: guard,
        })
    }

    /// Wait briefly ([`FINISH_GRACE`]) for an instance of the pool that is
    /// finishing its invoke phase to become free, and reserve it.
    async fn wait_for_finishing(&self, key: &str, deploy_id: &str) -> Option<Slot> {
        let finishing: Vec<Arc<WarmEntry>> = self
            .instances
            .read()
            .get(key)?
            .iter()
            .filter(|e| is_current(e, deploy_id) && e.finishing.load(Ordering::Acquire))
            .cloned()
            .collect();
        if finishing.is_empty() {
            return None;
        }
        let waiters = finishing.into_iter().map(|entry| {
            Box::pin(async move {
                let guard = entry.busy.clone().lock_owned().await;
                Slot {
                    entry,
                    _guard: guard,
                }
            })
        });
        let slot = tokio::time::timeout(FINISH_GRACE, futures_util::future::select_all(waiters))
            .await
            .ok()?
            .0;
        (is_current(&slot.entry, deploy_id) && self.in_pool(key, &slot.entry)).then(|| {
            *slot.entry.last_used.write() = Instant::now();
            slot
        })
    }

    fn in_pool(&self, key: &str, entry: &Arc<WarmEntry>) -> bool {
        self.instances
            .read()
            .get(key)
            .is_some_and(|pool| pool.iter().any(|e| Arc::ptr_eq(e, entry)))
    }

    /// Try to reserve a free, current-deploy instance without launching.
    /// Returns `None` if every matching instance is busy (or there are
    /// none).
    fn try_take_free(&self, key: &str, deploy_id: &str) -> Option<Slot> {
        let map = self.instances.read();
        let pool = map.get(key)?;
        for entry in pool {
            if !is_current(entry, deploy_id) {
                continue;
            }
            if let Ok(guard) = entry.busy.clone().try_lock_owned() {
                *entry.last_used.write() = Instant::now();
                return Some(Slot {
                    entry: entry.clone(),
                    _guard: guard,
                });
            }
        }
        None
    }

    /// Remove one specific instance from a function's pool and terminate
    /// it. Used when an invocation finds the instance unreachable.
    async fn evict_entry(&self, key: &str, target: &Arc<WarmEntry>, reason: &'static str) {
        self.teardown().evict(key, target, reason).await;
    }

    /// Tear down every *free* instance in a pool that can no longer take an
    /// invocation of the current deploy (see [`is_current`]). A busy one is
    /// mid-invocation and is never cut off: it is left in place, takes no
    /// new work, and goes on a later sweep once free.
    async fn evict_stale_deploy(&self, key: &str, deploy_id: &str) {
        let stale: Vec<Arc<WarmEntry>> = {
            let mut map = self.instances.write();
            match map.get_mut(key) {
                Some(pool) => {
                    let mut stale = Vec::new();
                    pool.retain(|e| {
                        if is_current(e, deploy_id) || e.busy.try_lock().is_err() {
                            true
                        } else {
                            stale.push(e.clone());
                            false
                        }
                    });
                    if pool.is_empty() {
                        map.remove(key);
                    }
                    stale
                }
                None => Vec::new(),
            }
        };
        for entry in stale {
            tracing::info!(
                pool = %key,
                handle = ?entry.instance.handle,
                "stopping stale-deploy Lambda runtime instance"
            );
            self.retire(&entry).await;
        }
    }

    /// Remove and return the warm pool for a function **without**
    /// terminating it. Lets DeleteFunction snapshot exactly the instances
    /// that exist at delete time and terminate those, so a concurrent
    /// recreate of the same name (whose fresh warm instance is keyed
    /// identically) is not reaped by the deferred stop. Synchronous so the
    /// caller can take the snapshot while still ordered before any recreate
    /// (bug-hunt 2026-06-13, finding 4.2).
    ///
    /// `function_arn` is the unqualified function ARN; `version` limits the
    /// snapshot to one version's pool (DeleteFunction with a Qualifier),
    /// `None` takes every version's.
    pub(crate) fn take_warm_instances(
        &self,
        function_arn: &str,
        version: Option<&str>,
    ) -> Vec<Arc<WarmEntry>> {
        let mut map = self.instances.write();
        let keys: Vec<String> = map
            .keys()
            .filter(|k| match version {
                Some(v) => k.as_str() == format!("{function_arn}:{v}"),
                None => pool_key_belongs_to(k, function_arn),
            })
            .cloned()
            .collect();
        keys.into_iter()
            .flat_map(|k| map.remove(&k).unwrap_or_default())
            .collect()
    }

    /// Terminate a previously-snapshotted set of warm instances. Pairs with
    /// [`take_warm_instances`] for the delete path.
    pub(crate) async fn terminate_instances(&self, pool: Vec<Arc<WarmEntry>>) {
        for entry in pool {
            tracing::info!(
                handle = ?entry.instance.handle,
                "stopping Lambda runtime instance"
            );
            self.retire(&entry).await;
        }
    }

    /// Stop and remove every warm instance (all versions) of the function
    /// with unqualified ARN `function_arn`.
    pub async fn stop_container(&self, function_arn: &str) {
        let pool = self.take_warm_instances(function_arn, None);
        self.terminate_instances(pool).await;
    }

    /// Take the warm instances of every function in `account_id` (every
    /// account's for `None`) out of the pool, synchronously, for a later
    /// [`Self::terminate_taken`]. Called under the reset's state lock, so an
    /// instance a function created after the reset warms up is never taken.
    pub fn take_account_instances(&self, account_id: Option<&str>) -> TakenInstances {
        let mut map = self.instances.write();
        let keys: Vec<String> = map
            .keys()
            .filter(|k| account_id.is_none_or(|a| fakecloud_aws::arn::account_of(k) == Some(a)))
            .cloned()
            .collect();
        TakenInstances(
            keys.into_iter()
                .flat_map(|k| map.remove(&k).unwrap_or_default())
                .collect(),
        )
    }

    /// Stop and remove instances [`Self::take_account_instances`] took.
    pub async fn terminate_taken(&self, taken: TakenInstances) {
        self.terminate_instances(taken.0).await;
    }

    /// Stop and remove all warm instances (used on server shutdown or reset).
    pub async fn stop_all(&self) {
        let pools: Vec<(String, Vec<Arc<WarmEntry>>)> =
            { self.instances.write().drain().collect() };
        for (name, pool) in pools {
            for entry in pool {
                tracing::info!(
                    function = %name,
                    handle = ?entry.instance.handle,
                    "stopping Lambda runtime instance (cleanup)"
                );
                self.retire(&entry).await;
            }
        }
    }

    /// List all warm instances and their metadata for introspection.
    /// One row per running instance — a function scaled to several warm
    /// instances appears once per instance.
    pub fn list_warm_containers(
        &self,
        lambda_state: &crate::state::SharedLambdaState,
    ) -> Vec<serde_json::Value> {
        let entries = self.instances.read();
        let accounts = lambda_state.read();
        let mut rows = Vec::new();
        for (key, pool) in entries.iter() {
            let name = pool_key_function_name(key);
            // A pool key is a qualified function ARN, so it names the
            // function's account and region.
            let runtime = accounts
                .by_arn(key)
                .and_then(|state| state.functions.get(name))
                .map(|f| f.runtime.clone())
                .unwrap_or_default();
            for entry in pool {
                let idle_secs = entry.last_used.read().elapsed().as_secs();
                let mut row = serde_json::json!({
                    "functionName": name,
                    "runtime": runtime,
                    "backend": self.backend.name(),
                    "lastUsedSecsAgo": idle_secs,
                });
                let obj = row.as_object_mut().expect("json object");
                match &entry.instance.handle {
                    BackendHandle::Container { id } => {
                        obj.insert("containerId".into(), serde_json::Value::String(id.clone()));
                    }
                    BackendHandle::Pod { namespace, name } => {
                        obj.insert("podName".into(), serde_json::Value::String(name.clone()));
                        obj.insert(
                            "namespace".into(),
                            serde_json::Value::String(namespace.clone()),
                        );
                    }
                }
                rows.push(row);
            }
        }
        rows
    }

    /// Evict (stop and remove) every warm instance of every function named
    /// `function_name` (any account, any version). Returns true if at least
    /// one instance was evicted.
    pub async fn evict_container(&self, function_name: &str) -> bool {
        let pool: Vec<Arc<WarmEntry>> = {
            let mut map = self.instances.write();
            let keys: Vec<String> = map
                .keys()
                .filter(|k| pool_key_function_name(k) == function_name)
                .cloned()
                .collect();
            keys.into_iter()
                .flat_map(|k| map.remove(&k).unwrap_or_default())
                .collect()
        };
        let found = !pool.is_empty();
        for entry in pool {
            tracing::info!(
                function = %function_name,
                handle = ?entry.instance.handle,
                "evicting Lambda runtime instance via simulation API"
            );
            self.retire(&entry).await;
        }
        found
    }

    /// Background loop that stops instances idle longer than `ttl`.
    pub async fn run_cleanup_loop(self: Arc<Self>, ttl: Duration) {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        loop {
            interval.tick().await;
            self.cleanup_idle(ttl).await;
        }
    }

    async fn cleanup_idle(&self, ttl: Duration) {
        // Reap individual instances that are both idle past the TTL and
        // currently free (a busy instance is mid-invocation, so its
        // `last_used` is fresh anyway — the `try_lock` check just avoids
        // racing a slot that's about to be used).
        let expired: Vec<(String, Arc<WarmEntry>)> = {
            let mut map = self.instances.write();
            let mut out = Vec::new();
            for (name, pool) in map.iter_mut() {
                let mut i = 0;
                while i < pool.len() {
                    let idle = pool[i].last_used.read().elapsed() > ttl
                        || pool[i].retiring.load(Ordering::Acquire);
                    let free = pool[i].busy.try_lock().is_ok();
                    if idle && free {
                        out.push((name.clone(), pool.remove(i)));
                    } else {
                        i += 1;
                    }
                }
            }
            map.retain(|_, pool| !pool.is_empty());
            out
        };
        for (name, entry) in expired {
            tracing::info!(function = %name, "stopping idle Lambda runtime instance");
            self.retire(&entry).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::deploy_id_from;

    /// The deploy id is spliced raw into the init-container artifact URL
    /// path and into a Pod label, so it must never contain characters
    /// that standard base64 emits (`/`, `+`, `=`). Standard base64
    /// produced `/` for ~49% of code hashes (issue #1643); sweep a wide
    /// range of inputs to catch any regression back to a non-URL-safe
    /// alphabet.
    #[test]
    fn deploy_id_is_url_path_safe() {
        for i in 0..2_000u32 {
            // Vary both the code hash and the layer set.
            let code_sha256 = format!("sha256-seed-{i}-{}", i.wrapping_mul(2_654_435_761));
            let layers: Vec<Vec<u8>> = if i % 3 == 0 {
                vec![format!("layer-{i}").into_bytes()]
            } else {
                vec![]
            };
            let id = deploy_id_from(&code_sha256, "{}", &layers);
            assert!(
                !id.contains('/') && !id.contains('+') && !id.contains('='),
                "deploy id {id:?} (seed {i}) is not URL-path-safe"
            );
        }
    }

    /// Same inputs must always map to the same deploy id — the value is a
    /// warm-pool cache key, so instability would defeat reuse.
    #[test]
    fn deploy_id_is_stable() {
        let layers = vec![b"layer-a".to_vec(), b"layer-b".to_vec()];
        let a = deploy_id_from("abc123", "cfg", &layers);
        let b = deploy_id_from("abc123", "cfg", &layers);
        assert_eq!(a, b);
        assert_ne!(a, deploy_id_from("abc124", "cfg", &layers));
        assert_ne!(a, deploy_id_from("abc123", "cfg", &[]));
        assert_ne!(a, deploy_id_from("abc123", "cfg2", &layers));
    }

    // ---- warm pool + Runtime API invocation path ----

    use super::{InvocationContext, LambdaRuntime, RunOutcome};
    use crate::runtime::api::{spawn_in_process_runtime, InProcessReply, InvocationEvent};
    use crate::runtime::backend::{
        BackendHandle, InstanceExit, LambdaBackend, RuntimeApiAddress, RuntimeError, WarmInstance,
    };
    use crate::state::LambdaFunction;
    use fakecloud_core::auth::{SessionCredentialIssuer, SessionCredentials};
    use parking_lot::RwLock;
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;
    use tokio::sync::watch;

    type Reply = Arc<dyn Fn(&InvocationEvent) -> InProcessReply + Send + Sync>;

    /// The extension a fake instance runs.
    #[derive(Clone, Copy)]
    enum Ext {
        None,
        /// Registered for `SHUTDOWN`.
        Shutdown,
        /// Registered for `INVOKE`; works this long after each event
        /// before asking for the next.
        WorksAfterInvoke(Duration),
        /// Exits with this status during init.
        FailsInit(i32),
    }

    /// Backend double: every launched instance runs an in-process runtime
    /// against its Runtime API sandbox, holding each event for `delay` and
    /// answering with `reply`. Records launches/terminates, the peak number
    /// of events handled at once, every event seen, and keeps each
    /// instance's exit sender so a test can kill it.
    struct FakeBackend {
        delay: Duration,
        reply: Reply,
        /// Upcoming launches whose runtime exits during init with status 3.
        dying_launches: AtomicUsize,
        launches: AtomicUsize,
        terminates: AtomicUsize,
        launched_with: StdMutex<Vec<Option<String>>>,
        cur: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        events: Arc<StdMutex<Vec<InvocationEvent>>>,
        exits: StdMutex<Vec<watch::Sender<Option<InstanceExit>>>>,
        /// The extension each instance runs, if any; it records the events
        /// it receives.
        extension: Ext,
        extension_events: Arc<StdMutex<Vec<serde_json::Value>>>,
        /// Run "the launcher": the facade then reads its bookkeeping files
        /// from `files`.
        launcher: bool,
        files: Arc<StdMutex<std::collections::HashMap<String, Vec<u8>>>>,
        /// Reads of `files` never complete (a hung container CLI).
        hang_reads: bool,
        /// How long each read of `files` takes (a slow container CLI); it
        /// sees `files` as they are when it finishes.
        read_delay: Duration,
        /// How many times `files` was read.
        reads: AtomicUsize,
        /// Each instance's extension process; aborting one ends it (its
        /// `/event/next` request drops).
        extension_tasks: StdMutex<Vec<tokio::task::JoinHandle<()>>>,
    }

    impl FakeBackend {
        fn new(delay: Duration) -> Arc<Self> {
            Self::replying(
                delay,
                Arc::new(|_| InProcessReply::Response(b"ok".to_vec())),
            )
        }

        fn replying(delay: Duration, reply: Reply) -> Arc<Self> {
            Arc::new(Self {
                delay,
                reply,
                dying_launches: AtomicUsize::new(0),
                launches: AtomicUsize::new(0),
                terminates: AtomicUsize::new(0),
                launched_with: StdMutex::new(Vec::new()),
                cur: Arc::new(AtomicUsize::new(0)),
                peak: Arc::new(AtomicUsize::new(0)),
                events: Arc::new(StdMutex::new(Vec::new())),
                exits: StdMutex::new(Vec::new()),
                extension: Ext::None,
                extension_events: Arc::new(StdMutex::new(Vec::new())),
                launcher: false,
                files: Arc::new(StdMutex::new(std::collections::HashMap::new())),
                hang_reads: false,
                read_delay: Duration::ZERO,
                reads: AtomicUsize::new(0),
                extension_tasks: StdMutex::new(Vec::new()),
            })
        }

        /// Kill instance `n` (in launch order) as if its process exited.
        fn kill(&self, n: usize, code: i64) {
            let _ = self.exits.lock().unwrap()[n]
                .send(Some(InstanceExit::from_container_code(code, false)));
        }

        fn handled(&self) -> usize {
            self.events.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl LambdaBackend for FakeBackend {
        fn name(&self) -> &str {
            "test"
        }
        async fn launch(
            &self,
            _func: &LambdaFunction,
            _code_zip: Option<&[u8]>,
            _layers: &[Vec<u8>],
            _deploy_id: &str,
            credentials: Option<&SessionCredentials>,
            runtime_api: &RuntimeApiAddress,
        ) -> Result<WarmInstance, RuntimeError> {
            self.launched_with
                .lock()
                .unwrap()
                .push(credentials.map(|c| c.access_key_id.clone()));
            let n = self.launches.fetch_add(1, SeqCst);
            let (tx, exit) = watch::channel(None);
            let pending = self.dying_launches.load(SeqCst);
            let dying = pending > 0
                && self
                    .dying_launches
                    .compare_exchange(pending, pending - 1, SeqCst, SeqCst)
                    .is_ok();
            if self.launcher {
                runtime_api.sandbox.await_extension_count();
            }
            match self.extension {
                Ext::None => {}
                Ext::FailsInit(code) => {
                    runtime_api.sandbox.expect_extensions(1);
                    runtime_api.sandbox.extension_exited("bad-ext", code);
                }
                ext => {
                    let sandbox = runtime_api.sandbox.clone();
                    if !self.launcher {
                        sandbox.expect_extensions(1);
                    }
                    let events = match ext {
                        Ext::Shutdown => vec!["SHUTDOWN".to_string()],
                        _ => vec!["INVOKE".to_string()],
                    };
                    let id = sandbox.register_extension("ext", &events).unwrap();
                    let seen = self.extension_events.clone();
                    let task = tokio::spawn(async move {
                        while let Ok(event) = sandbox.extension_next(&id).await {
                            seen.lock().unwrap().push(event);
                            if let Ext::WorksAfterInvoke(d) = ext {
                                tokio::time::sleep(d).await;
                            }
                        }
                    });
                    self.extension_tasks.lock().unwrap().push(task);
                }
            }
            if dying {
                let _ = tx.send(Some(InstanceExit::from_container_code(3, false)));
            } else {
                let (delay, reply) = (self.delay, self.reply.clone());
                let (cur, peak, events) =
                    (self.cur.clone(), self.peak.clone(), self.events.clone());
                spawn_in_process_runtime(runtime_api.sandbox.clone(), move |event| {
                    let (reply, cur, peak, events) =
                        (reply.clone(), cur.clone(), peak.clone(), events.clone());
                    async move {
                        let now = cur.fetch_add(1, SeqCst) + 1;
                        peak.fetch_max(now, SeqCst);
                        tokio::time::sleep(delay).await;
                        cur.fetch_sub(1, SeqCst);
                        let answer = reply(&event);
                        events.lock().unwrap().push(event);
                        answer
                    }
                });
            }
            self.exits.lock().unwrap().push(tx);
            Ok(WarmInstance {
                handle: BackendHandle::Container {
                    id: format!("c{n}"),
                },
                exit,
                launcher: self.launcher,
            })
        }
        async fn terminate(&self, _handle: &BackendHandle) {
            self.terminates.fetch_add(1, SeqCst);
        }
        async fn read_instance_file(&self, _handle: &BackendHandle, path: &str) -> Option<Vec<u8>> {
            self.reads.fetch_add(1, SeqCst);
            if self.hang_reads {
                std::future::pending::<()>().await;
            }
            if !self.read_delay.is_zero() {
                tokio::time::sleep(self.read_delay).await;
            }
            self.files.lock().unwrap().get(path).cloned()
        }
        async fn write_instance_file(
            &self,
            _handle: &BackendHandle,
            path: &str,
            contents: &[u8],
        ) -> bool {
            self.files
                .lock()
                .unwrap()
                .insert(path.to_string(), contents.to_vec());
            true
        }
    }

    fn runtime_with(backend: Arc<FakeBackend>, max_concurrency: usize) -> Arc<LambdaRuntime> {
        let mut rt = LambdaRuntime::from_backend(backend);
        rt.max_concurrency = max_concurrency;
        Arc::new(rt)
    }

    fn arn(name: &str) -> String {
        format!("arn:aws:lambda:us-east-1:123456789012:function:{name}")
    }

    fn key(name: &str) -> String {
        format!("{}:$LATEST", arn(name))
    }

    /// Wait until an invocation holds an instance of `pool` busy. A fixed
    /// sleep races the spawned invoke under a loaded runner.
    async fn wait_until_busy(rt: &LambdaRuntime, pool: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            let busy = rt
                .instances
                .read()
                .get(pool)
                .is_some_and(|p| p.iter().any(|e| e.busy.try_lock().is_err()));
            if busy {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "no instance of {pool} became busy"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn test_func(name: &str, sha: &str) -> LambdaFunction {
        serde_json::from_value(serde_json::json!({
            "function_name": name,
            "function_arn": format!("arn:aws:lambda:us-east-1:123456789012:function:{name}"),
            "runtime": "python3.12",
            "role": "arn:aws:iam::123456789012:role/r",
            "handler": "index.handler",
            "description": "",
            "timeout": 5,
            "memory_size": 128,
            "code_sha256": sha,
            "code_size": 1,
            "version": "$LATEST",
            "last_modified": "2020-01-01T00:00:00Z",
            "tags": {},
            "environment": {},
            "architectures": ["x86_64"],
            "package_type": "Zip",
            "code_zip": [1, 2, 3],
            "policy": null
        }))
        .expect("build test LambdaFunction")
    }

    /// Wait (bounded) for `cond`: a reset environment is torn down after its
    /// caller has heard back.
    async fn wait_for(cond: impl Fn() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !cond() {
            assert!(std::time::Instant::now() < deadline, "condition never held");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn error_body(payload: &[u8]) -> serde_json::Value {
        serde_json::from_slice(payload).expect("error object is JSON")
    }

    /// With one warm instance, a burst of concurrent invocations is
    /// serialized onto it: an execution environment takes one event at a
    /// time. Peak overlap must be 1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_invokes_are_serialized_on_a_single_instance() {
        let backend = FakeBackend::new(Duration::from_millis(40));
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("conc", "sha-A");

        let mut handles = Vec::new();
        for _ in 0..8 {
            let rt = rt.clone();
            let func = func.clone();
            handles.push(tokio::spawn(
                async move { rt.invoke(&func, b"{}", &[]).await },
            ));
        }
        for h in handles {
            let out = h.await.unwrap().expect("invoke ok");
            assert_eq!(out.payload, b"ok");
        }

        assert_eq!(
            backend.peak.load(SeqCst),
            1,
            "events overlapped on one instance"
        );
        assert_eq!(
            backend.launches.load(SeqCst),
            1,
            "max_concurrency=1 must launch exactly one instance"
        );
        assert_eq!(backend.handled(), 8);
    }

    /// Under load the pool grows beyond one instance but never past the
    /// cap, and every event reaches exactly one runtime.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pool_scales_under_load_and_respects_cap() {
        let backend = FakeBackend::replying(
            Duration::from_millis(60),
            Arc::new(|e| InProcessReply::Response(e.payload.to_vec())),
        );
        let rt = runtime_with(backend.clone(), 4);
        let func = test_func("scale", "sha-A");

        let mut handles = Vec::new();
        for i in 0..8 {
            let rt = rt.clone();
            let func = func.clone();
            handles.push(tokio::spawn(async move {
                let payload = format!("{{\"n\":{i}}}");
                let out = rt.invoke(&func, payload.as_bytes(), &[]).await.unwrap();
                assert_eq!(out.payload, payload.as_bytes(), "answers never cross");
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let launched = backend.launches.load(SeqCst);
        assert!(
            (2..=4).contains(&launched),
            "expected the pool to scale within the cap, launched={launched}"
        );
        assert!(backend.peak.load(SeqCst) > 1);
        assert_eq!(backend.handled(), 8);
    }

    /// A warm instance whose process died while idle is evicted and the
    /// invocation runs on a fresh one, exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dead_warm_instance_is_evicted_and_retried() {
        let backend = FakeBackend::new(Duration::from_millis(5));
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("dead", "sha-A");

        rt.invoke(&func, b"{}", &[]).await.unwrap();
        backend.kill(0, 137);
        let out = rt
            .invoke(&func, b"{}", &[])
            .await
            .expect("recovers on a fresh instance");
        assert_eq!(out.payload, b"ok");
        assert_eq!(out.function_error, None);
        assert_eq!(backend.launches.load(SeqCst), 2);
        assert_eq!(backend.terminates.load(SeqCst), 1);
        assert_eq!(backend.handled(), 2, "the event ran once");
    }

    /// A runtime that exits during init fails the invocation with
    /// `Runtime.ExitError` (as Lambda reports an init crash), not a retry,
    /// and its instance is not reused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_exit_during_init_is_exit_error() {
        let backend = FakeBackend::new(Duration::from_millis(5));
        backend.dying_launches.store(1, SeqCst);
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("initcrash", "sha-A");
        let ctx = InvocationContext {
            request_id: Some("req-1".into()),
            ..Default::default()
        };

        let (out, _) = rt.invoke_with(&func, b"{}", &[], &ctx).await.unwrap();
        assert_eq!(out.function_error.as_deref(), Some("Unhandled"));
        let body = error_body(&out.payload);
        assert_eq!(body["errorType"], "Runtime.ExitError");
        assert_eq!(
            body["errorMessage"],
            "RequestId: req-1 Error: Runtime exited with error: exit status 3"
        );
        assert_eq!(backend.launches.load(SeqCst), 1);
        wait_for(|| backend.terminates.load(SeqCst) == 1).await;

        // The next invocation gets a fresh environment.
        let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(out.function_error, None);
        assert_eq!(backend.launches.load(SeqCst), 2);
    }

    /// A runtime killed while it holds the event reports the exit for that
    /// event and is never retried (the handler already ran).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_exit_mid_invocation_is_reported_once() {
        let backend =
            FakeBackend::replying(Duration::from_secs(30), Arc::new(|_| InProcessReply::Hang));
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("midexit", "sha-A");
        let call = {
            let rt = rt.clone();
            let func = func.clone();
            tokio::spawn(async move {
                let ctx = InvocationContext {
                    request_id: Some("req-2".into()),
                    ..Default::default()
                };
                rt.invoke_with(&func, b"{}", &[], &ctx).await
            })
        };
        // Wait for the runtime to have taken the event.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while backend.cur.load(SeqCst) == 0 {
            assert!(std::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        backend.kill(0, 137);
        let (out, _) = call.await.unwrap().unwrap();
        let body = error_body(&out.payload);
        assert_eq!(body["errorType"], "Runtime.ExitError");
        assert_eq!(
            body["errorMessage"],
            "RequestId: req-2 Error: Runtime exited with error: signal: killed"
        );
        assert_eq!(backend.launches.load(SeqCst), 1, "no retry once delivered");
    }

    /// What the function returned is its result, whatever its shape; only
    /// an error the runtime reports is a function error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn only_a_reported_error_is_a_function_error() {
        let error_shaped = br#"{"errorType":"ValueError","errorMessage":"returned, not raised"}"#;
        let backend = FakeBackend::replying(
            Duration::ZERO,
            Arc::new(|e| {
                if e.payload.as_ref() == b"raise" {
                    InProcessReply::Error {
                        error_type: Some("ValueError".into()),
                        body:
                            br#"{"errorMessage":"boom","errorType":"ValueError","stackTrace":[]}"#
                                .to_vec(),
                    }
                } else {
                    InProcessReply::Response(
                        br#"{"errorType":"ValueError","errorMessage":"returned, not raised"}"#
                            .to_vec(),
                    )
                }
            }),
        );
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("shapes", "sha-A");

        let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(out.function_error, None);
        assert_eq!(out.payload, error_shaped);

        let out = rt.invoke(&func, b"raise", &[]).await.unwrap();
        assert_eq!(out.function_error.as_deref(), Some("Unhandled"));
        assert_eq!(out.error_type().as_deref(), Some("ValueError"));
        assert_eq!(out.error_message().as_deref(), Some("boom"));
        // A raised error does not reset the environment.
        assert_eq!(backend.launches.load(SeqCst), 1);
        assert_eq!(backend.terminates.load(SeqCst), 0);
    }

    /// An invocation past the function timeout is `Sandbox.Timedout` under
    /// its own request ID, and the environment is reset.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn timeout_is_sandbox_timedout_and_resets_the_instance() {
        let backend =
            FakeBackend::replying(Duration::from_secs(30), Arc::new(|_| InProcessReply::Hang));
        let rt = runtime_with(backend.clone(), 1);
        let mut func = test_func("slow", "sha-A");
        func.timeout = 1;
        let ctx = InvocationContext {
            request_id: Some("req-t".into()),
            ..Default::default()
        };
        let started = std::time::Instant::now();
        let (out, _) = rt.invoke_with(&func, b"{}", &[], &ctx).await.unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        let body = error_body(&out.payload);
        assert_eq!(out.function_error.as_deref(), Some("Unhandled"));
        assert_eq!(body["errorType"], "Sandbox.Timedout");
        assert_eq!(
            body["errorMessage"],
            "RequestId: req-t Error: Task timed out after 1.00 seconds"
        );
        wait_for(|| backend.terminates.load(SeqCst) == 1).await;
        assert!(rt.instances.read().get(&key("slow")).is_none());
    }

    /// The event carries the request ID, invoked ARN, trace and client
    /// context the caller supplied, and a deadline one timeout away.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn event_carries_the_invocation_context() {
        let backend = FakeBackend::new(Duration::ZERO);
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("ctx", "sha-A");
        let ctx = InvocationContext {
            request_id: Some("req-c".into()),
            invoked_function_arn: Some(format!("{}:live", arn("ctx"))),
            trace_id: Some("Root=1-5e1b4151-5ac6c58f5a4b2e2f9c6b3e2d;Sampled=1".into()),
            client_context: Some(r#"{"custom":{"k":"v"}}"#.into()),
            cognito_identity: None,
        };
        rt.invoke_with(&func, b"{\"a\":1}", &[], &ctx)
            .await
            .unwrap();
        rt.invoke(&func, b"", &[]).await.unwrap();
        rt.invoke_streaming(
            &func,
            b"{}",
            &[],
            &InvocationContext::default(),
            &mut |_| {},
        )
        .await
        .unwrap();
        let events = backend.events.lock().unwrap();
        assert_eq!(events[1].payload.as_ref(), b"{}", "no payload is `{{}}`");
        assert_eq!(events[0].request_id, "req-c");
        assert_eq!(
            events[0].invoked_function_arn,
            format!("{}:live", arn("ctx"))
        );
        assert_eq!(
            events[0].trace_id,
            "Root=1-5e1b4151-5ac6c58f5a4b2e2f9c6b3e2d;Sampled=1"
        );
        assert_eq!(
            events[0].client_context.as_deref(),
            Some(r#"{"custom":{"k":"v"}}"#)
        );
        assert_eq!(events[0].payload.as_ref(), b"{\"a\":1}");
        assert_eq!(events[0].timeout, Duration::from_secs(5));
        assert_eq!(
            events[0].max_response_bytes,
            Some(super::MAX_SYNC_RESPONSE_BYTES)
        );
        assert_eq!(
            events[2].max_response_bytes,
            Some(crate::runtime::api::MAX_STREAM_RESPONSE_BYTES),
            "a streamed response is capped at 200 MB"
        );
        // Defaults: a fresh request ID and trace, the function's own ARN.
        assert_ne!(events[1].request_id, "req-c");
        assert_eq!(events[1].invoked_function_arn, arn("ctx"));
        assert!(events[1].trace_id.starts_with("Root=1-"));
    }

    /// A streamed response keeps its pieces; a buffered one over 6 MB is
    /// `Function.ResponseSizeTooLarge`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_keeps_chunks_and_buffered_responses_are_capped() {
        let backend = FakeBackend::replying(
            Duration::ZERO,
            Arc::new(|e| {
                if e.payload.as_ref() == b"big" {
                    InProcessReply::Response(vec![b'x'; super::MAX_SYNC_RESPONSE_BYTES + 1])
                } else {
                    InProcessReply::Stream(vec![b"one|".to_vec(), b"two".to_vec()])
                }
            }),
        );
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("stream", "sha-A");

        let mut chunks = Vec::new();
        let outcome = rt
            .invoke_streaming(&func, b"{}", &[], &InvocationContext::default(), &mut |c| {
                chunks.push(c)
            })
            .await
            .unwrap();
        assert_eq!(outcome, RunOutcome::Success);
        assert_eq!(chunks, vec![&b"one|"[..], &b"two"[..]]);

        let out = rt.invoke(&func, b"big", &[]).await.unwrap();
        assert_eq!(out.function_error.as_deref(), Some("Unhandled"));
        assert_eq!(
            out.error_type().as_deref(),
            Some("Function.ResponseSizeTooLarge")
        );
    }

    /// `LogType=Tail` output frames the function's output with the
    /// platform's START / END / REPORT lines; only the cold start reports an
    /// init duration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn log_tail_has_the_platform_lines() {
        let backend = FakeBackend::new(Duration::ZERO);
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("logs", "sha-A");
        let ctx = InvocationContext {
            request_id: Some("req-l".into()),
            ..Default::default()
        };
        let (_, logs) = rt
            .invoke_with_log_tail(&func, b"{}", &[], &ctx)
            .await
            .unwrap();
        let logs = logs.unwrap();
        assert!(
            logs.starts_with("START RequestId: req-l Version: $LATEST\n"),
            "{logs}"
        );
        assert!(logs.contains("END RequestId: req-l\n"), "{logs}");
        assert!(
            logs.contains("REPORT RequestId: req-l\tDuration: "),
            "{logs}"
        );
        assert!(logs.contains("Memory Size: 128 MB"), "{logs}");
        assert!(logs.contains("Init Duration: "), "{logs}");
        let (_, logs) = rt
            .invoke_with_log_tail(&func, b"{}", &[], &InvocationContext::default())
            .await
            .unwrap();
        assert!(!logs.unwrap().contains("Init Duration"));
    }

    /// Changing the function's code (a new deploy id) tears down the
    /// stale warm instance before serving against a fresh one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deploy_change_evicts_stale_instance() {
        let backend = FakeBackend::new(Duration::from_millis(5));
        let rt = runtime_with(backend.clone(), 2);

        rt.invoke(&test_func("upd", "sha-A"), b"{}", &[])
            .await
            .unwrap();
        rt.invoke(&test_func("upd", "sha-B"), b"{}", &[])
            .await
            .unwrap();

        assert_eq!(backend.launches.load(SeqCst), 2);
        assert!(backend.terminates.load(SeqCst) >= 1);
        let pool_len = rt.instances.read().get(&key("upd")).map_or(0, |v| v.len());
        assert_eq!(pool_len, 1);
        // The torn-down instance's sandbox is gone from the Runtime API.
        assert_eq!(rt.runtime_api.len(), 1);
    }

    fn idle_entry(rt: &LambdaRuntime, id: &str) -> Arc<super::WarmEntry> {
        let (_tx, exit) = watch::channel(None);
        Arc::new(super::WarmEntry {
            instance: WarmInstance {
                handle: BackendHandle::Container { id: id.to_string() },
                exit,
                launcher: false,
            },
            sandbox: rt.runtime_api.register(),
            launched_at: std::time::Instant::now(),
            last_used: RwLock::new(std::time::Instant::now()),
            deploy_id: "d".to_string(),
            credentials: None,
            retiring: std::sync::atomic::AtomicBool::new(false),
            finishing: std::sync::atomic::AtomicBool::new(false),
            busy: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// bug-hunt 2026-06-13, finding 4.2: DeleteFunction must snapshot the
    /// warm pool and terminate *that snapshot*, not whatever pool exists
    /// when the deferred stop runs.
    #[tokio::test]
    async fn take_warm_instances_snapshot_does_not_reap_recreated_pool() {
        let backend = FakeBackend::new(Duration::ZERO);
        let rt = runtime_with(backend.clone(), 10);

        rt.instances
            .write()
            .insert(key("f"), vec![idle_entry(&rt, "old")]);
        let snapshot = rt.take_warm_instances(&arn("f"), None);
        assert_eq!(snapshot.len(), 1);
        assert!(rt.instances.read().get(&key("f")).is_none());

        rt.instances
            .write()
            .insert(key("f"), vec![idle_entry(&rt, "new")]);
        rt.terminate_instances(snapshot).await;
        assert_eq!(backend.terminates.load(SeqCst), 1);
        assert_eq!(rt.instances.read().get(&key("f")).map(Vec::len), Some(1));
    }

    #[tokio::test]
    async fn take_account_instances_takes_only_that_account() {
        let backend = FakeBackend::new(Duration::ZERO);
        let rt = runtime_with(backend.clone(), 10);
        let other = "arn:aws:lambda:us-east-1:222222222222:function:g:$LATEST".to_string();
        rt.instances
            .write()
            .insert(key("f"), vec![idle_entry(&rt, "a")]);
        rt.instances.write().insert(
            other.clone(),
            vec![idle_entry(&rt, "b"), idle_entry(&rt, "c")],
        );

        let taken = rt.take_account_instances(Some("222222222222"));
        assert_eq!(taken.len(), 2);
        assert!(rt.instances.read().get(&other).is_none());
        rt.instances
            .write()
            .insert(other.clone(), vec![idle_entry(&rt, "new")]);
        rt.terminate_taken(taken).await;
        assert_eq!(backend.terminates.load(SeqCst), 2);
        assert_eq!(rt.instances.read().get(&other).map(Vec::len), Some(1));
        assert_eq!(rt.instances.read().get(&key("f")).map(Vec::len), Some(1));

        let all = rt.take_account_instances(None);
        assert_eq!(all.len(), 2);
        assert!(rt.instances.read().is_empty());
    }

    /// Issuer double: mints sequential keys with a configurable lifetime
    /// and records what was issued and revoked.
    struct RecordingIssuer {
        lifetime: chrono::Duration,
        issued: StdMutex<Vec<(String, String, String)>>,
        revoked: StdMutex<Vec<String>>,
    }

    impl RecordingIssuer {
        fn new(lifetime: chrono::Duration) -> Arc<Self> {
            Arc::new(Self {
                lifetime,
                issued: StdMutex::new(Vec::new()),
                revoked: StdMutex::new(Vec::new()),
            })
        }
    }

    impl SessionCredentialIssuer for RecordingIssuer {
        fn issue(
            &self,
            role_arn: &str,
            session_name: &str,
            _duration: chrono::Duration,
        ) -> SessionCredentials {
            let mut issued = self.issued.lock().unwrap();
            let key = format!("KEY{}", issued.len());
            issued.push((key.clone(), role_arn.to_string(), session_name.to_string()));
            SessionCredentials {
                access_key_id: key,
                secret_access_key: "s".into(),
                session_token: "t".into(),
                expiration: chrono::Utc::now() + self.lifetime,
                account_id: "123456789012".into(),
            }
        }
        fn revoke(&self, credentials: &SessionCredentials) {
            self.revoked
                .lock()
                .unwrap()
                .push(credentials.access_key_id.clone());
        }
    }

    /// Each instance is launched with a session for the function's execution
    /// role, named after the function, and the session is revoked when the
    /// instance goes away.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn instances_get_execution_role_credentials_revoked_on_teardown() {
        let backend = FakeBackend::new(Duration::from_millis(5));
        let rt = runtime_with(backend.clone(), 2);
        let issuer = RecordingIssuer::new(chrono::Duration::hours(1));
        rt.set_credential_issuer(issuer.clone());

        let func = test_func("creds", "sha-A");
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        rt.invoke(&func, b"{}", &[]).await.unwrap();

        assert_eq!(
            *issuer.issued.lock().unwrap(),
            vec![(
                "KEY0".to_string(),
                "arn:aws:iam::123456789012:role/r".to_string(),
                "creds".to_string()
            )],
            "a warm instance keeps its credentials across invocations"
        );
        assert_eq!(
            *backend.launched_with.lock().unwrap(),
            vec![Some("KEY0".to_string())]
        );

        rt.stop_container(&arn("creds")).await;
        assert_eq!(*issuer.revoked.lock().unwrap(), vec!["KEY0".to_string()]);
        assert!(rt.runtime_api.is_empty());
    }

    /// An IAM reset drops the credentials warm instances hold: free ones are
    /// retired (and replaced on the next invoke), busy ones finish their
    /// invocation untouched and take no new work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iam_reset_retires_free_instances_and_spares_busy_ones() {
        let backend = FakeBackend::new(Duration::from_secs(2));
        let rt = runtime_with(backend.clone(), 2);
        let issuer = RecordingIssuer::new(chrono::Duration::hours(12));
        rt.set_credential_issuer(issuer.clone());

        let func = test_func("reset", "sha-A");
        let (a, b) = tokio::join!(rt.invoke(&func, b"{}", &[]), rt.invoke(&func, b"{}", &[]));
        a.unwrap();
        b.unwrap();
        assert_eq!(backend.launches.load(SeqCst), 2);
        let busy = {
            let rt = rt.clone();
            let func = func.clone();
            tokio::spawn(async move { rt.invoke(&func, b"{}", &[]).await })
        };
        wait_until_busy(&rt, &key("reset")).await;

        rt.mark_credentials_revoked(Some("123456789012"));
        assert!(rt
            .instances
            .read()
            .get(&key("reset"))
            .unwrap()
            .iter()
            .all(|e| e.retiring.load(std::sync::atomic::Ordering::Acquire)));
        assert_eq!(backend.terminates.load(SeqCst), 0);
        rt.mark_credentials_revoked(Some("999999999999"));
        rt.retire_released().await;
        assert_eq!(
            backend.terminates.load(SeqCst),
            1,
            "only the free instance is retired"
        );
        busy.await
            .unwrap()
            .expect("the in-flight invocation completes");

        rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(backend.launches.load(SeqCst), 3);
        assert_eq!(
            backend
                .launched_with
                .lock()
                .unwrap()
                .last()
                .cloned()
                .flatten(),
            Some("KEY2".to_string())
        );
        assert_eq!(backend.terminates.load(SeqCst), 2);
        assert_eq!(issuer.revoked.lock().unwrap().len(), 2);
    }

    /// A deploy change never terminates an instance mid-invocation: the busy
    /// stale instance finishes and is only reaped once free.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deploy_change_spares_busy_instance() {
        let backend = FakeBackend::new(Duration::from_secs(2));
        let rt = runtime_with(backend.clone(), 2);

        let old = test_func("busy", "sha-A");
        let in_flight = {
            let rt = rt.clone();
            tokio::spawn(async move { rt.invoke(&old, b"{}", &[]).await })
        };
        wait_until_busy(&rt, &key("busy")).await;
        rt.invoke(&test_func("busy", "sha-B"), b"{}", &[])
            .await
            .unwrap();
        assert_eq!(backend.terminates.load(SeqCst), 0);
        in_flight
            .await
            .unwrap()
            .expect("in-flight invocation completes");

        rt.evict_stale_deploy(
            &key("busy"),
            &super::deploy_id_for(&test_func("busy", "sha-B"), &[], false),
        )
        .await;
        assert_eq!(backend.terminates.load(SeqCst), 1);
    }

    /// Versions (and accounts) have their own pools and never evict each
    /// other, even with identical code.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn versions_do_not_evict_each_other() {
        let backend = FakeBackend::new(Duration::from_millis(5));
        let rt = runtime_with(backend.clone(), 2);

        let latest = test_func("ver", "sha-A");
        let mut v1 = latest.clone();
        v1.version = "1".into();
        for _ in 0..2 {
            rt.invoke(&latest, b"{}", &[]).await.unwrap();
            rt.invoke(&v1, b"{}", &[]).await.unwrap();
        }
        assert_eq!(backend.launches.load(SeqCst), 2);
        assert_eq!(backend.terminates.load(SeqCst), 0);
        // A version's event names the version-qualified ARN.
        assert!(backend
            .events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.invoked_function_arn == format!("{}:1", arn("ver"))));

        let taken = rt.take_warm_instances(&arn("ver"), Some("1"));
        assert_eq!(taken.len(), 1);
        assert!(rt.instances.read().contains_key(&key("ver")));
        assert_eq!(rt.take_warm_instances(&arn("ver"), None).len(), 1);
    }

    /// A configuration change that reaches the instance environment (here
    /// the function's variables) starts a fresh instance even though the
    /// code is unchanged.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn configuration_change_replaces_the_warm_instance() {
        let backend = FakeBackend::new(Duration::from_millis(5));
        let rt = runtime_with(backend.clone(), 2);

        let mut func = test_func("cfg", "sha-A");
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        func.environment.insert("MODE".into(), "b".into());
        rt.invoke(&func, b"{}", &[]).await.unwrap();

        assert_eq!(backend.launches.load(SeqCst), 2);
        assert_eq!(backend.terminates.load(SeqCst), 1);
    }

    #[test]
    fn session_is_minted_in_the_function_account() {
        let f = "arn:aws:lambda:us-east-1:123456789012:function:f";
        assert_eq!(
            super::session_role_arn("arn:aws:iam::123456789012:role/r", f),
            "arn:aws:iam::123456789012:role/r"
        );
        assert_eq!(
            super::session_role_arn("arn:aws:iam::000000000000:role/path/r", f),
            "arn:aws:iam::123456789012:role/path/r"
        );
        // Regression: an account-less role ARN is validated as the
        // function account's role, so its session is minted there too rather
        // than in the server's default account.
        assert_eq!(
            super::session_role_arn("arn:aws:iam:::role/r", f),
            "arn:aws:iam::123456789012:role/r"
        );
        assert_eq!(super::session_role_arn("not-an-arn", f), "not-an-arn");
    }

    /// Backend double whose launches fail, or never finish.
    struct BrokenBackend {
        hang: bool,
    }

    #[async_trait::async_trait]
    impl LambdaBackend for BrokenBackend {
        fn name(&self) -> &str {
            "broken"
        }
        async fn launch(
            &self,
            _func: &LambdaFunction,
            _code_zip: Option<&[u8]>,
            _layers: &[Vec<u8>],
            _deploy_id: &str,
            _credentials: Option<&SessionCredentials>,
            _runtime_api: &RuntimeApiAddress,
        ) -> Result<WarmInstance, RuntimeError> {
            if self.hang {
                std::future::pending::<()>().await;
            }
            Err(RuntimeError::ContainerStartFailed("boom".into()))
        }
        async fn terminate(&self, _handle: &BackendHandle) {}
    }

    fn broken_runtime(hang: bool) -> Arc<LambdaRuntime> {
        let mut rt = LambdaRuntime::from_backend(Arc::new(BrokenBackend { hang }));
        rt.max_concurrency = 2;
        Arc::new(rt)
    }

    /// Credentials minted for a launch that fails, or whose launching future
    /// is dropped mid-launch, are revoked rather than left registered, and
    /// so is its Runtime API sandbox.
    #[tokio::test]
    async fn credentials_of_an_unfinished_launch_are_revoked() {
        let func = test_func("broken", "sha-A");

        let rt = broken_runtime(false);
        let issuer = RecordingIssuer::new(chrono::Duration::hours(12));
        rt.set_credential_issuer(issuer.clone());
        assert!(rt.invoke(&func, b"{}", &[]).await.is_err());
        assert_eq!(*issuer.revoked.lock().unwrap(), vec!["KEY0".to_string()]);
        assert!(rt.runtime_api.is_empty());

        let rt = broken_runtime(true);
        let issuer = RecordingIssuer::new(chrono::Duration::hours(12));
        rt.set_credential_issuer(issuer.clone());
        let cancelled =
            tokio::time::timeout(Duration::from_millis(100), rt.invoke(&func, b"{}", &[])).await;
        assert!(cancelled.is_err(), "the launch never finishes");
        assert_eq!(*issuer.revoked.lock().unwrap(), vec!["KEY0".to_string()]);
        assert!(rt.runtime_api.is_empty());
    }

    /// An instance stops taking invocations once its credentials could lapse
    /// during one, judged from the credentials' own expiration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn instance_whose_credentials_near_expiry_is_replaced() {
        let backend = FakeBackend::new(Duration::from_millis(5));
        let rt = runtime_with(backend.clone(), 2);
        let issuer = RecordingIssuer::new(chrono::Duration::minutes(10));
        rt.set_credential_issuer(issuer.clone());

        let func = test_func("aging", "sha-A");
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(backend.launches.load(SeqCst), 2);
        assert_eq!(*issuer.revoked.lock().unwrap(), vec!["KEY0".to_string()]);
    }

    /// Deleting a version stops its free instances now and leaves a busy one
    /// to finish, retiring it once released; other versions are untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deleting_a_version_spares_its_busy_instance() {
        let backend = FakeBackend::new(Duration::from_secs(2));
        let rt = runtime_with(backend.clone(), 2);

        let latest = test_func("delv", "sha-A");
        let mut v1 = latest.clone();
        v1.version = "1".into();
        rt.invoke(&latest, b"{}", &[]).await.unwrap();
        let in_flight = {
            let rt = rt.clone();
            let v1 = v1.clone();
            tokio::spawn(async move { rt.invoke(&v1, b"{}", &[]).await })
        };
        wait_until_busy(&rt, &format!("{}:1", arn("delv"))).await;

        let detached = rt.retire_version(&arn("delv"), "1");
        assert!(detached.is_empty(), "the busy instance is not detached");
        rt.terminate_instances(detached).await;
        assert_eq!(backend.terminates.load(SeqCst), 0);
        in_flight
            .await
            .unwrap()
            .expect("in-flight invocation completes");

        rt.retire_released().await;
        assert_eq!(backend.terminates.load(SeqCst), 1);
        assert!(rt
            .instances
            .read()
            .get(&format!("{}:1", arn("delv")))
            .is_none());
        assert!(rt.instances.read().contains_key(&key("delv")));
    }

    /// Tags only change the deploy fingerprint for backends that read them.
    #[test]
    fn tags_change_the_deploy_only_when_the_backend_uses_them() {
        let plain = test_func("tagged", "sha-A");
        let mut tagged = plain.clone();
        tagged.tags.insert("team".into(), "a".into());
        assert_eq!(
            super::deploy_id_for(&plain, &[], false),
            super::deploy_id_for(&tagged, &[], false)
        );
        assert_ne!(
            super::deploy_id_for(&plain, &[], true),
            super::deploy_id_for(&tagged, &[], true)
        );
    }

    /// A caller that stops waiting (dropped connection, caller-side timeout)
    /// does not free the instance while its runtime still runs the event,
    /// and the function timeout is still enforced: the instance is reset at
    /// the deadline and its extensions get `SHUTDOWN` with reason `timeout`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_invocation_keeps_its_instance_until_the_timeout() {
        let mut backend = FakeBackend::replying(
            Duration::ZERO,
            Arc::new(|e| {
                if e.payload.as_ref() == b"hang" {
                    InProcessReply::Hang
                } else {
                    InProcessReply::Response(b"ok".to_vec())
                }
            }),
        );
        Arc::get_mut(&mut backend).unwrap().extension = Ext::Shutdown;
        let rt = runtime_with(backend.clone(), 2);
        let mut func = test_func("cancel", "sha-A");
        func.timeout = 1;

        let cancelled =
            tokio::time::timeout(Duration::from_millis(300), rt.invoke(&func, b"hang", &[])).await;
        assert!(cancelled.is_err(), "the caller gave up");
        // The next invocation does not land on the instance still running
        // the abandoned event: it gets an instance of its own.
        let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(out.payload, b"ok");
        assert_eq!(backend.launches.load(SeqCst), 2);

        // The abandoned event's timeout resets its instance.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while backend.terminates.load(SeqCst) == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "timeout never enforced"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let events = backend.extension_events.lock().unwrap().clone();
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(events[0]["eventType"], "SHUTDOWN");
        assert_eq!(events[0]["shutdownReason"], "timeout");
    }

    /// Lambda's invoke phase ends when the runtime and every extension have
    /// asked for their next event: an extension still working after the
    /// response keeps the instance reserved (the next event waits for it,
    /// on the same instance) without the instance being reset.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn post_response_extension_work_holds_the_instance_without_resetting_it() {
        let mut backend = FakeBackend::new(Duration::ZERO);
        Arc::get_mut(&mut backend).unwrap().extension =
            Ext::WorksAfterInvoke(Duration::from_millis(400));
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("postwork", "sha-A");
        let started = std::time::Instant::now();
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(400),
            "the response does not wait for the extension"
        );
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(400),
            "the second event waited for the extension"
        );
        assert_eq!(backend.launches.load(SeqCst), 1);
        assert_eq!(backend.terminates.load(SeqCst), 0);
        assert_eq!(backend.extension_events.lock().unwrap().len(), 2);
    }

    /// A runtime that answers and never asks for another event holds its
    /// instance until the invocation's timeout resets it; the next event
    /// then runs on a fresh instance.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_runtime_that_stalls_after_answering_is_reset_at_the_timeout() {
        let backend = FakeBackend::replying(
            Duration::ZERO,
            Arc::new(|_| InProcessReply::ResponseThenStall(b"ok".to_vec())),
        );
        let rt = runtime_with(backend.clone(), 1);
        let mut func = test_func("stall", "sha-A");
        func.timeout = 1;
        let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(out.payload, b"ok");
        let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(out.payload, b"ok");
        assert_eq!(out.function_error, None);
        assert_eq!(backend.launches.load(SeqCst), 2);
        assert_eq!(backend.terminates.load(SeqCst), 1);
        assert_eq!(backend.handled(), 2, "each event ran once");
    }

    /// An extension that cannot be launched, or exits during init, fails
    /// the init with Lambda's `Extension.*` error instead of leaving the
    /// event waiting.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failing_extension_fails_the_init() {
        for (code, error_type) in [(1, "Extension.Crash"), (127, "Extension.LaunchError")] {
            let mut backend = FakeBackend::new(Duration::ZERO);
            Arc::get_mut(&mut backend).unwrap().extension = Ext::FailsInit(code);
            let rt = runtime_with(backend.clone(), 1);
            let func = test_func("badext", "sha-A");
            let started = std::time::Instant::now();
            let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
            assert!(started.elapsed() < Duration::from_secs(3));
            assert_eq!(out.function_error.as_deref(), Some("Unhandled"));
            assert_eq!(out.error_type().as_deref(), Some(error_type));
            assert!(out.error_message().unwrap().contains("bad-ext"));
            wait_for(|| backend.terminates.load(SeqCst) == 1).await;
        }
    }

    /// Lambda's init limit: a cold invocation whose runtime takes the event
    /// within 10s has the full function timeout from then; one whose init
    /// overruns it is bounded by the function timeout from the end of the
    /// init limit, init and handler together. A warm runtime has the
    /// pickup limit, then the timeout.
    #[test]
    fn deadlines_follow_the_init_limit() {
        let t0 = tokio::time::Instant::now();
        let timeout = Duration::from_secs(3);
        let d = |served: bool, delivered: Option<u64>| {
            super::answer_deadline(
                !served,
                t0,
                delivered.map(|s| t0 + Duration::from_secs(s)),
                timeout,
            ) - t0
        };
        assert_eq!(d(false, None), Duration::from_secs(13));
        assert_eq!(d(false, Some(2)), Duration::from_secs(5));
        assert_eq!(d(false, Some(10)), Duration::from_secs(13));
        assert_eq!(d(false, Some(12)), Duration::from_secs(13));
        // An environment that served events is idle and ready.
        assert_eq!(d(true, None), Duration::from_secs(3));
        assert_eq!(d(true, Some(1)), Duration::from_secs(4));
    }

    /// `SHUTDOWN` carries why the environment goes: `spindown` for a normal
    /// teardown, `failure` when the runtime exited.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shutdown_reasons_follow_why_the_environment_goes() {
        let mut backend = FakeBackend::new(Duration::ZERO);
        Arc::get_mut(&mut backend).unwrap().extension = Ext::Shutdown;
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("reasons", "sha-A");
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        rt.stop_container(&arn("reasons")).await;
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        backend.kill(1, 1);
        // The dead warm instance is reset before the event runs elsewhere.
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        let reasons: Vec<String> = backend
            .extension_events
            .lock()
            .unwrap()
            .iter()
            .map(|e| e["shutdownReason"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(reasons, vec!["spindown", "failure"]);
    }

    /// A launcher that never reports what it started (its bookkeeping
    /// directory unwritable, say) fails the init loudly with
    /// `Extension.LaunchError` instead of leaving the event queued.
    #[tokio::test(start_paused = true)]
    async fn a_launcher_that_never_reports_fails_the_init() {
        let mut backend = FakeBackend::new(Duration::ZERO);
        Arc::get_mut(&mut backend).unwrap().launcher = true;
        let rt = runtime_with(backend.clone(), 1);
        let mut func = test_func("silent-launcher", "sha-A");
        func.timeout = 60;
        let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(out.function_error.as_deref(), Some("Unhandled"));
        assert_eq!(out.error_type().as_deref(), Some("Extension.LaunchError"));
    }

    /// An extension that dies while an invocation runs fails it with
    /// `Extension.Crash` at once, not after the whole timeout.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_extension_dying_mid_invocation_fails_it() {
        let mut backend = FakeBackend::replying(Duration::ZERO, Arc::new(|_| InProcessReply::Hang));
        {
            let b = Arc::get_mut(&mut backend).unwrap();
            b.launcher = true;
            b.extension = Ext::WorksAfterInvoke(Duration::from_secs(60));
        }
        backend.files.lock().unwrap().insert(
            crate::runtime::extensions::LAUNCHED_FILE.to_string(),
            b"1\n".to_vec(),
        );
        let rt = runtime_with(backend.clone(), 1);
        let mut func = test_func("ext-dies", "sha-A");
        func.timeout = 60;
        let call = {
            let rt = rt.clone();
            let func = func.clone();
            tokio::spawn(async move { rt.invoke(&func, b"{}", &[]).await })
        };
        wait_for(|| backend.handled() == 1).await;
        backend.files.lock().unwrap().insert(
            crate::runtime::extensions::EXITED_FILE.to_string(),
            b"exit 2 ext\n".to_vec(),
        );
        let started = std::time::Instant::now();
        let out = call.await.unwrap().unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert_eq!(out.error_type().as_deref(), Some("Extension.Crash"));
        assert!(out.error_message().unwrap().contains("ext"));
        wait_for(|| backend.terminates.load(SeqCst) == 1).await;
    }

    /// An extension that exits after the instance has served (while it is
    /// idle between invocations) is still noticed: the next invocation fails
    /// with `Extension.Crash` and the instance is reset, so the one after
    /// runs on a fresh instance.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_extension_exiting_between_invocations_fails_the_next() {
        use crate::runtime::extensions::{EXITED_FILE, LAUNCHED_FILE};
        let mut backend = FakeBackend::new(Duration::ZERO);
        {
            let b = Arc::get_mut(&mut backend).unwrap();
            b.launcher = true;
            b.extension = Ext::WorksAfterInvoke(Duration::ZERO);
        }
        backend
            .files
            .lock()
            .unwrap()
            .insert(LAUNCHED_FILE.to_string(), b"1\n".to_vec());
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("ext-dies-idle", "sha-A");
        let first = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert!(first.function_error.is_none(), "{first:?}");

        // An idle environment's exit record is read at the idle rate
        // (`IDLE_EXTENSION_POLL`, 5s), not the busy one.
        let pool = key("ext-dies-idle");
        wait_for(|| {
            rt.instances
                .read()
                .get(&pool)
                .is_some_and(|p| p.iter().all(|e| e.sandbox.is_idle()))
        })
        .await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        let reads = backend.reads.load(SeqCst);
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(
            backend.reads.load(SeqCst) <= reads + 1,
            "{} reads in a second of idle",
            backend.reads.load(SeqCst) - reads
        );

        // The extension process ends: its status is recorded (and its
        // `/event/next` request drops, so the environment is no longer idle
        // and the record is read at the busy rate).
        backend
            .files
            .lock()
            .unwrap()
            .insert(EXITED_FILE.to_string(), b"exit 2 ext\n".to_vec());
        backend.extension_tasks.lock().unwrap()[0].abort();
        wait_for(|| {
            rt.instances
                .read()
                .get(&pool)
                .is_some_and(|p| p.iter().any(|e| e.sandbox.is_closed_or_failed()))
        })
        .await;
        let second = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(second.error_type().as_deref(), Some("Extension.Crash"));
        assert!(
            second
                .error_message()
                .unwrap()
                .contains("extension ext exited with error: exit status 2"),
            "{second:?}"
        );
        wait_for(|| backend.terminates.load(SeqCst) == 1).await;

        backend.files.lock().unwrap().remove(EXITED_FILE);
        let third = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert!(third.function_error.is_none(), "{third:?}");
        assert_eq!(backend.launches.load(SeqCst), 2);
    }

    /// A container CLI that hangs reading the launcher's report (a wedged
    /// daemon) does not hold the init forever: the launcher's 10-second
    /// deadline still fails it with `Extension.LaunchError`, at most one
    /// bounded read late.
    #[tokio::test(start_paused = true)]
    async fn a_hung_launcher_read_still_fails_the_init_on_time() {
        let mut backend = FakeBackend::new(Duration::ZERO);
        {
            let b = Arc::get_mut(&mut backend).unwrap();
            b.launcher = true;
            b.hang_reads = true;
        }
        let rt = runtime_with(backend.clone(), 1);
        let mut func = test_func("hung-launcher", "sha-A");
        func.timeout = 60;
        let started = tokio::time::Instant::now();
        let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(out.error_type().as_deref(), Some("Extension.LaunchError"));
        assert!(
            started.elapsed() <= super::INIT_LIMIT + super::LAUNCHER_IO_TIMEOUT,
            "{:?}",
            started.elapsed()
        );
    }

    /// A launcher read already under way when the 10-second deadline passes
    /// is allowed to finish: a slow container CLI that does bring back the
    /// launcher's report does not turn a good launch into a
    /// `Extension.LaunchError`.
    #[tokio::test(start_paused = true)]
    async fn a_slow_launcher_read_finishing_after_the_deadline_still_launches() {
        use crate::runtime::extensions::LAUNCHED_FILE;
        let mut backend = FakeBackend::new(Duration::ZERO);
        {
            let b = Arc::get_mut(&mut backend).unwrap();
            b.launcher = true;
            // Each read takes 4s: reads start at about 0s, 4s and 8s, and the
            // one started at 8s finishes past the deadline, at about 12s.
            b.read_delay = Duration::from_secs(4);
        }
        // The launcher reports at 9s, after the read started at 4s looked.
        let files = backend.files.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(9)).await;
            files
                .lock()
                .unwrap()
                .insert(LAUNCHED_FILE.to_string(), b"0\n".to_vec());
        });
        let rt = runtime_with(backend.clone(), 1);
        let mut func = test_func("slow-launcher", "sha-A");
        func.timeout = 60;
        let started = tokio::time::Instant::now();
        let out = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert!(out.function_error.is_none(), "{out:?}");
        assert_eq!(out.payload, b"ok");
        assert!(
            started.elapsed() > super::INIT_LIMIT,
            "{:?}",
            started.elapsed()
        );
    }

    /// An idle extension whose `/event/next` request drops while its
    /// process lives on (an HTTP client timing out its long poll) is not
    /// reported as a crash: with no exit in the launcher's record, the
    /// environment is left alone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_extension_dropping_its_request_without_an_exit_is_not_a_crash() {
        use crate::runtime::extensions::LAUNCHED_FILE;
        let mut backend = FakeBackend::new(Duration::ZERO);
        {
            let b = Arc::get_mut(&mut backend).unwrap();
            b.launcher = true;
            b.extension = Ext::WorksAfterInvoke(Duration::ZERO);
        }
        backend
            .files
            .lock()
            .unwrap()
            .insert(LAUNCHED_FILE.to_string(), b"1\n".to_vec());
        let rt = runtime_with(backend.clone(), 1);
        let func = test_func("ext-drops", "sha-A");
        let first = rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert!(first.function_error.is_none(), "{first:?}");
        let pool = key("ext-drops");
        wait_for(|| {
            rt.instances
                .read()
                .get(&pool)
                .is_some_and(|p| p.iter().all(|e| e.sandbox.is_idle()))
        })
        .await;
        backend.extension_tasks.lock().unwrap()[0].abort();
        // Well past several busy-rate reads of the record.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let instances = rt.instances.read();
        let pool = instances.get(&pool).expect("the instance is still pooled");
        assert_eq!(pool.len(), 1);
        assert!(!pool[0].sandbox.is_closed_or_failed());
        assert!(!pool[0].sandbox.is_idle(), "the extension is not waiting");
        drop(instances);
        assert_eq!(backend.terminates.load(SeqCst), 0);
    }

    #[test]
    fn exit_messages_follow_lambda() {
        let exit = |code| InstanceExit::from_container_code(code, false);
        let msg = |e: InstanceExit| error_body(&super::exit_payload("r", &e));
        assert_eq!(
            msg(exit(0))["errorMessage"],
            "RequestId: r Error: Runtime exited without providing a reason"
        );
        assert_eq!(
            msg(exit(1))["errorMessage"],
            "RequestId: r Error: Runtime exited with error: exit status 1"
        );
        assert_eq!(
            msg(exit(139))["errorMessage"],
            "RequestId: r Error: Runtime exited with error: signal: segmentation fault"
        );
        let oom = msg(InstanceExit::from_container_code(137, true));
        assert_eq!(oom["errorType"], "Runtime.OutOfMemory");
        assert_eq!(
            oom["errorMessage"],
            "RequestId: r Error: Runtime exited with error: signal: killed"
        );
    }
}
