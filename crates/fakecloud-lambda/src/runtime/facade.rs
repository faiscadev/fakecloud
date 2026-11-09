//! Backend-agnostic Lambda runtime facade.
//!
//! Owns the warm-pool bookkeeping, per-function startup serialization,
//! and the HTTP invocation path. Dispatches container lifecycle to
//! whatever [`LambdaBackend`] it was constructed with.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use base64::Engine;
use fakecloud_core::auth::{SessionCredentialIssuer, SessionCredentials};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use super::backend::{
    BackendHandle, LambdaBackend, RuntimeError, StreamingInvocation, WarmInstance,
};
use super::docker::DockerBackend;
use crate::state::LambdaFunction;

/// A running runtime instance kept warm for reuse.
pub(crate) struct WarmEntry {
    instance: WarmInstance,
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
    /// Held for the duration of a single invocation against this
    /// instance. The AWS Runtime Interface Emulator (and real Lambda)
    /// handles exactly one event per execution environment at a time;
    /// the RIE's `rapidcore` server nil-pointer-derefs and the process
    /// exits if two invokes overlap (issue #1644). Acquiring this lock
    /// before forwarding guarantees one in-flight event per instance,
    /// and lets the pool pick a *free* instance via `try_lock`.
    busy: Arc<tokio::sync::Mutex<()>>,
}

/// Default cap on warm instances per function. Real Lambda scales
/// execution environments with concurrent demand; we bound it so a
/// burst can't spawn unbounded containers/Pods. Override with
/// `FAKECLOUD_LAMBDA_MAX_CONCURRENCY`. Beyond the cap, invocations queue
/// on a busy instance rather than starting a new one.
const DEFAULT_MAX_CONCURRENCY: usize = 10;

/// Max attempts per invocation when a reserved warm instance turns out to be
/// unreachable. Each failover is a fast probe (or connect error) plus a cold
/// start; the connection provably never reached the handler, so retrying can't
/// double-execute. Bounded so an all-dead pool can't spin forever.
const MAX_INVOKE_ATTEMPTS: u32 = 5;

/// Timeout for the pre-invoke TCP reachability probe. A black-holed Pod IP (a
/// killed pod / drained node that drops packets with no RST) would otherwise
/// hang the full invoke timeout (~`func.timeout + 5s`); this detects it in ~1s
/// so the state-machine retry can succeed within its window.
const REACHABILITY_PROBE_TIMEOUT: Duration = Duration::from_millis(1500);

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

/// A reserved invocation slot: a warm instance plus the held busy guard
/// that grants exclusive use of it until the guard drops.
struct Slot {
    entry: Arc<WarmEntry>,
    guard: tokio::sync::OwnedMutexGuard<()>,
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

/// Quick liveness check: can a TCP connection to `endpoint` (`host:port`) be
/// opened within `timeout`? Used before forwarding a payload to a warm instance
/// so a dead/black-holed Pod is detected in ~1s instead of hanging the full
/// invoke timeout. A failed connect provably never reached the handler, so the
/// caller can safely evict and retry.
async fn endpoint_reachable(endpoint: &str, timeout: Duration) -> bool {
    matches!(
        tokio::time::timeout(timeout, tokio::net::TcpStream::connect(endpoint)).await,
        Ok(Ok(_))
    )
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
    /// Per-function pool of warm instances. Each instance serves one
    /// invocation at a time (its `busy` lock); the pool grows on demand
    /// up to `max_concurrency`, and the idle reaper trims it.
    instances: RwLock<HashMap<String, Vec<Arc<WarmEntry>>>>,
    /// Serializes runtime startup per function to prevent duplicate
    /// instances racing into the pool when several cold invokes arrive
    /// together.
    starting: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Cap on warm instances per function.
    max_concurrency: usize,
    /// Mints the execution-role credentials each instance is started with.
    /// Unset (no credentials exported) until the server wires IAM in.
    credential_issuer: OnceLock<Arc<dyn SessionCredentialIssuer>>,
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
            instances: RwLock::new(HashMap::new()),
            starting: RwLock::new(HashMap::new()),
            max_concurrency,
            credential_issuer: OnceLock::new(),
        }
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

    /// Tear down one instance and revoke the credentials it was given.
    async fn retire(&self, entry: &WarmEntry) {
        self.backend.terminate(&entry.instance.handle).await;
        self.revoke(entry.credentials.as_ref());
    }

    fn revoke(&self, credentials: Option<&SessionCredentials>) {
        if let (Some(issuer), Some(creds)) = (self.credential_issuer.get(), credentials) {
            issuer.revoke(creds);
        }
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
    /// Reserves a warm instance for the call (one in-flight invocation
    /// per instance — the RIE crashes on overlap, issue #1644). If the
    /// instance is unreachable (dead Pod/container from a node drain, OOM,
    /// or prior crash) it is evicted and the call retried (up to four
    /// times) against a freshly cold-started instance, so a dead instance can't
    /// wedge the function permanently.
    pub async fn invoke(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
    ) -> Result<Vec<u8>, RuntimeError> {
        self.invoke_inner(func, payload, layers, false)
            .await
            .map(|(bytes, _)| bytes)
    }

    /// Like [`Self::invoke`] but also returns the instance's recent log tail
    /// (for `Invoke` with `LogType=Tail` -> `X-Amz-Log-Result`). `None` when the
    /// backend can't supply logs.
    pub async fn invoke_with_log_tail(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
    ) -> Result<(Vec<u8>, Option<String>), RuntimeError> {
        self.invoke_inner(func, payload, layers, true).await
    }

    async fn invoke_inner(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
        capture_logs: bool,
    ) -> Result<(Vec<u8>, Option<String>), RuntimeError> {
        let client = reqwest::Client::builder()
            .connect_timeout(REACHABILITY_PROBE_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let slot = self.acquire_slot(func, layers).await?;

            // Fast reachability probe before forwarding the payload. A warm Pod
            // that was killed (FakeCloud recreating it, OOM, node reclaim) often
            // black-holes its old IP, so the POST would hang the full invoke
            // timeout. A short TCP probe detects the dead instance in ~1s; the
            // connection never reached the handler, so we can safely evict and
            // fail over to a cold start.
            if !endpoint_reachable(&slot.entry.instance.endpoint, REACHABILITY_PROBE_TIMEOUT).await
            {
                let entry = slot.entry.clone();
                drop(slot);
                self.evict_entry(&pool_key(func), &entry).await;
                if attempt < MAX_INVOKE_ATTEMPTS {
                    tracing::warn!(
                        function = %func.function_name,
                        endpoint = %entry.instance.endpoint,
                        "warm Lambda instance failed reachability probe; evicted, retrying with a cold start"
                    );
                    continue;
                }
                return Err(RuntimeError::InvocationFailed(format!(
                    "no reachable warm instance for {} after {attempt} attempts",
                    func.function_name
                )));
            }

            let url = format!(
                "http://{}/2015-03-31/functions/function/invocations",
                slot.entry.instance.endpoint
            );
            let send = client
                .post(&url)
                .body(payload.to_vec())
                .timeout(Duration::from_secs(func.timeout as u64 + 5))
                .send()
                .await;
            match send {
                Ok(resp) => {
                    let body = resp.bytes().await;
                    *slot.entry.last_used.write() = Instant::now();
                    return match body {
                        Ok(b) => {
                            // Capture the instance's log tail while the slot
                            // (and thus the container/Pod) is still alive.
                            let logs = if capture_logs {
                                self.backend
                                    .instance_logs(&slot.entry.instance.handle)
                                    .await
                            } else {
                                None
                            };
                            Ok((b.to_vec(), logs))
                        }
                        Err(e) => {
                            // Response failed mid-stream — the instance is
                            // suspect. Evict it but don't retry: the
                            // function already ran and may have side effects.
                            let entry = slot.entry.clone();
                            drop(slot);
                            self.evict_entry(&pool_key(func), &entry).await;
                            Err(RuntimeError::InvocationFailed(e.to_string()))
                        }
                    };
                }
                Err(e) => {
                    // Transport-level failure. Evict the suspect instance.
                    // Only retry when the connection was never
                    // established (`is_connect` — e.g. refused by a dead
                    // Pod): then the request provably never reached the
                    // function, so a cold-start retry can't double-execute
                    // it. A reset/timeout mid-flight may have already run
                    // the handler, so surface those instead of risking a
                    // duplicate invoke.
                    let entry = slot.entry.clone();
                    drop(slot);
                    self.evict_entry(&pool_key(func), &entry).await;
                    if attempt < MAX_INVOKE_ATTEMPTS && e.is_connect() {
                        tracing::warn!(
                            function = %func.function_name,
                            error = %e,
                            "warm Lambda instance unreachable; evicted, retrying with a cold start"
                        );
                        continue;
                    }
                    return Err(RuntimeError::InvocationFailed(e.to_string()));
                }
            }
        }
    }

    /// Invoke a Lambda function and yield the raw HTTP body as a stream
    /// of byte chunks. Each chunk corresponds to one HTTP frame the RIE
    /// flushed to the wire — for streaming-aware handlers this
    /// preserves the chunk boundaries the function emitted. Buffered
    /// handlers come back as a single chunk, which is still a valid
    /// streamed response.
    ///
    /// The reserved instance's busy guard travels with the returned
    /// [`StreamingInvocation`] so the slot stays held until the caller
    /// finishes draining the stream.
    pub async fn invoke_streaming(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
    ) -> Result<StreamingInvocation, RuntimeError> {
        let client = reqwest::Client::builder()
            .connect_timeout(REACHABILITY_PROBE_TIMEOUT)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let mut attempt: u32 = 0;
        loop {
            attempt += 1;
            let slot = self.acquire_slot(func, layers).await?;

            // Same fast reachability probe as `invoke`: detect a dead/black-holed
            // warm instance in ~1s and fail over instead of hanging.
            if !endpoint_reachable(&slot.entry.instance.endpoint, REACHABILITY_PROBE_TIMEOUT).await
            {
                let entry = slot.entry.clone();
                drop(slot);
                self.evict_entry(&pool_key(func), &entry).await;
                if attempt < MAX_INVOKE_ATTEMPTS {
                    continue;
                }
                return Err(RuntimeError::InvocationFailed(format!(
                    "no reachable warm instance for {} after {attempt} attempts",
                    func.function_name
                )));
            }

            let url = format!(
                "http://{}/2015-03-31/functions/function/invocations",
                slot.entry.instance.endpoint
            );
            let send = client
                .post(&url)
                .body(payload.to_vec())
                .timeout(Duration::from_secs(func.timeout as u64 + 5))
                .send()
                .await;
            match send {
                Ok(resp) => {
                    *slot.entry.last_used.write() = Instant::now();
                    let Slot {
                        entry: _entry,
                        guard,
                    } = slot;
                    return Ok(StreamingInvocation {
                        resp,
                        _slot_guard: Some(guard),
                    });
                }
                Err(e) => {
                    // Same connect-only retry policy as `invoke`: retry
                    // only when the connection never established, so a
                    // half-run handler isn't invoked twice.
                    let entry = slot.entry.clone();
                    drop(slot);
                    self.evict_entry(&pool_key(func), &entry).await;
                    if attempt < MAX_INVOKE_ATTEMPTS && e.is_connect() {
                        continue;
                    }
                    return Err(RuntimeError::InvocationFailed(e.to_string()));
                }
            }
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
                    Slot { entry, guard }
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
        let instance = self
            .backend
            .launch(
                func,
                func.code_zip.as_deref(),
                layers,
                &deploy_id,
                pending.credentials.as_ref(),
            )
            .await?;
        let entry = Arc::new(WarmEntry {
            instance,
            last_used: RwLock::new(Instant::now()),
            deploy_id,
            credentials: pending.credentials.clone(),
            retiring: AtomicBool::new(false),
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
        Ok(Slot { entry, guard })
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
                    guard,
                });
            }
        }
        None
    }

    /// Remove one specific instance from a function's pool and terminate
    /// it. Used when an invocation finds the instance unreachable.
    async fn evict_entry(&self, key: &str, target: &Arc<WarmEntry>) {
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
                "evicting unreachable Lambda runtime instance"
            );
            self.retire(&entry).await;
        }
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

    // ---- warm-pool concurrency + eviction (issue #1644) ----

    use super::LambdaRuntime;
    use crate::runtime::backend::{BackendHandle, LambdaBackend, RuntimeError, WarmInstance};
    use crate::state::LambdaFunction;
    use fakecloud_core::auth::{SessionCredentialIssuer, SessionCredentials};
    use parking_lot::RwLock;
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

    /// Backend double: counts launches/terminates and hands out endpoints
    /// from a queue (falling back to `default_endpoint`), so a test can
    /// inject a dead endpoint followed by a live one.
    struct CountingBackend {
        endpoints: StdMutex<VecDeque<String>>,
        default_endpoint: String,
        launches: AtomicUsize,
        terminates: AtomicUsize,
        /// Access key id of the credentials each launch was handed.
        launched_with: StdMutex<Vec<Option<String>>>,
    }

    impl CountingBackend {
        fn new(default_endpoint: impl Into<String>) -> Arc<Self> {
            Arc::new(Self {
                endpoints: StdMutex::new(VecDeque::new()),
                default_endpoint: default_endpoint.into(),
                launches: AtomicUsize::new(0),
                terminates: AtomicUsize::new(0),
                launched_with: StdMutex::new(Vec::new()),
            })
        }
        fn with_queue(default_endpoint: impl Into<String>, queue: Vec<String>) -> Arc<Self> {
            Arc::new(Self {
                endpoints: StdMutex::new(queue.into()),
                default_endpoint: default_endpoint.into(),
                launches: AtomicUsize::new(0),
                terminates: AtomicUsize::new(0),
                launched_with: StdMutex::new(Vec::new()),
            })
        }
    }

    #[async_trait::async_trait]
    impl LambdaBackend for CountingBackend {
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
        ) -> Result<WarmInstance, RuntimeError> {
            self.launched_with
                .lock()
                .unwrap()
                .push(credentials.map(|c| c.access_key_id.clone()));
            let n = self.launches.fetch_add(1, SeqCst);
            let endpoint = self
                .endpoints
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_else(|| self.default_endpoint.clone());
            Ok(WarmInstance {
                endpoint,
                handle: BackendHandle::Container {
                    id: format!("c{n}"),
                },
            })
        }
        async fn terminate(&self, _handle: &BackendHandle) {
            self.terminates.fetch_add(1, SeqCst);
        }
    }

    /// Spin a minimal in-process "RIE": one TCP accept loop that records
    /// the peak number of simultaneously-open connections, holds each
    /// request for `delay`, then returns a tiny 200. The peak directly
    /// observes how many invocations overlapped on the instance(s) it
    /// backs. Returns the `host:port` to use as a warm endpoint.
    async fn spawn_rie(delay: Duration, peak: Arc<AtomicUsize>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let cur = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                let cur = cur.clone();
                let peak = peak.clone();
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    // Only connections that actually send a request count as an
                    // invocation. A bare reachability probe (TCP connect, no
                    // bytes) reads EOF and is ignored — it neither overlaps a
                    // real invoke nor inflates the concurrency peak, mirroring
                    // the real RIE, which only starts an event on a request.
                    let mut buf = [0u8; 1024];
                    let n = sock.read(&mut buf).await.unwrap_or(0);
                    if n == 0 {
                        return;
                    }
                    let now = cur.fetch_add(1, SeqCst) + 1;
                    peak.fetch_max(now, SeqCst);
                    tokio::time::sleep(delay).await;
                    let _ = sock
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )
                        .await;
                    let _ = sock.flush().await;
                    cur.fetch_sub(1, SeqCst);
                });
            }
        });
        format!("{addr}")
    }

    fn runtime_with(backend: Arc<CountingBackend>, max_concurrency: usize) -> Arc<LambdaRuntime> {
        Arc::new(LambdaRuntime {
            backend,
            instances: RwLock::new(HashMap::new()),
            starting: RwLock::new(HashMap::new()),
            max_concurrency,
            credential_issuer: std::sync::OnceLock::new(),
        })
    }

    fn arn(name: &str) -> String {
        format!("arn:aws:lambda:us-east-1:123456789012:function:{name}")
    }

    fn key(name: &str) -> String {
        format!("{}:$LATEST", arn(name))
    }

    /// Wait until an invocation holds an instance of `pool` busy. A fixed
    /// sleep races the spawned invoke under a loaded runner: it may not have
    /// reserved its instance yet, or (with a short RIE delay) already be done.
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

    /// The core of #1644: with one warm instance, a burst of concurrent
    /// invocations must be serialized onto it — never delivered in
    /// parallel (which segfaults the real RIE). Peak overlap must be 1.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_invokes_are_serialized_on_a_single_instance() {
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_millis(40), peak.clone()).await;
        let backend = CountingBackend::new(endpoint);
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
            h.await.unwrap().expect("invoke ok");
        }

        assert_eq!(
            peak.load(SeqCst),
            1,
            "concurrent invokes overlapped on a single RIE instance"
        );
        assert_eq!(
            backend.launches.load(SeqCst),
            1,
            "max_concurrency=1 must launch exactly one instance"
        );
    }

    /// Under load the pool grows beyond one instance but never past the
    /// cap, so genuine concurrency is served (AWS semantics) without an
    /// unbounded container/Pod fan-out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn pool_scales_under_load_and_respects_cap() {
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_millis(60), peak.clone()).await;
        let backend = CountingBackend::new(endpoint);
        let rt = runtime_with(backend.clone(), 4);
        let func = test_func("scale", "sha-A");

        let mut handles = Vec::new();
        for _ in 0..8 {
            let rt = rt.clone();
            let func = func.clone();
            handles.push(tokio::spawn(
                async move { rt.invoke(&func, b"{}", &[]).await },
            ));
        }
        for h in handles {
            h.await.unwrap().expect("invoke ok");
        }

        let launched = backend.launches.load(SeqCst);
        assert!(
            (2..=4).contains(&launched),
            "expected the pool to scale within the cap, launched={launched}"
        );
        assert!(
            peak.load(SeqCst) > 1,
            "expected concurrent forwards across the scaled pool"
        );
    }

    /// An unreachable instance (dead Pod/container) must be evicted and
    /// the invocation retried against a fresh cold start, instead of
    /// wedging the function forever.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dead_instance_is_evicted_and_retried() {
        let peak = Arc::new(AtomicUsize::new(0));
        let live = spawn_rie(Duration::from_millis(5), peak.clone()).await;
        // Two launches in a row hand back refused ports; the third gets the
        // live one. Connect-refused provably never reached a handler, so the
        // extra retry budget can't double-execute.
        let backend = CountingBackend::with_queue(
            live,
            vec!["127.0.0.1:1".to_string(), "127.0.0.1:1".to_string()],
        );
        let rt = runtime_with(backend.clone(), 1);

        let out = rt
            .invoke(&test_func("dead", "sha-A"), b"{}", &[])
            .await
            .expect("should recover via cold-start retry");
        assert_eq!(out, b"ok");
        assert_eq!(
            backend.launches.load(SeqCst),
            3,
            "expected two dead instances plus one cold-start replacement"
        );
        assert!(
            backend.terminates.load(SeqCst) >= 2,
            "both dead instances should have been terminated on eviction"
        );
    }

    /// A black-holed warm instance (a killed Pod whose IP now drops packets,
    /// rather than refusing with an RST) must be detected by the fast
    /// reachability probe and failed over in seconds — not hang the full invoke
    /// timeout (~`func.timeout + 5s`). This is the failure mode that flaked the
    /// LOE workflows: a dead pod cost ~305s, blowing the test window.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn black_holed_instance_fails_over_fast() {
        let peak = Arc::new(AtomicUsize::new(0));
        let live = spawn_rie(Duration::from_millis(5), peak.clone()).await;
        // 192.0.2.1 is RFC 5737 TEST-NET-1: unrouted, so a connect black-holes
        // (caught by the probe's ~1.5s timeout) instead of getting a fast RST.
        let backend = CountingBackend::with_queue(live, vec!["192.0.2.1:9".to_string()]);
        let rt = runtime_with(backend.clone(), 1);

        // test_func timeout is 5 -> the pre-fix hang would be ~10s.
        let func = test_func("blackhole", "sha-A");
        let started = std::time::Instant::now();
        let out = rt
            .invoke(&func, b"{}", &[])
            .await
            .expect("should recover via cold-start retry");
        let elapsed = started.elapsed();

        assert_eq!(out, b"ok");
        assert_eq!(
            backend.launches.load(SeqCst),
            2,
            "expected one black-holed instance plus one cold-start replacement"
        );
        assert!(
            backend.terminates.load(SeqCst) >= 1,
            "the black-holed instance should have been evicted"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "failover took {elapsed:?}; must be far below the ~10s invoke timeout"
        );
    }

    /// Changing the function's code (a new deploy id) tears down the
    /// stale warm instance before serving against a fresh one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deploy_change_evicts_stale_instance() {
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_millis(5), peak.clone()).await;
        let backend = CountingBackend::new(endpoint);
        let rt = runtime_with(backend.clone(), 2);

        rt.invoke(&test_func("upd", "sha-A"), b"{}", &[])
            .await
            .unwrap();
        rt.invoke(&test_func("upd", "sha-B"), b"{}", &[])
            .await
            .unwrap();

        assert_eq!(
            backend.launches.load(SeqCst),
            2,
            "a new deploy id should launch a fresh instance"
        );
        assert!(
            backend.terminates.load(SeqCst) >= 1,
            "the stale-deploy instance should have been torn down"
        );
        // Exactly one current instance remains in the pool.
        let pool_len = rt.instances.read().get(&key("upd")).map_or(0, |v| v.len());
        assert_eq!(pool_len, 1);
    }

    /// bug-hunt 2026-06-13, finding 4.2: DeleteFunction must snapshot the
    /// warm pool and terminate *that snapshot*, not whatever pool exists
    /// when the deferred stop runs. Otherwise a CreateFunction + warm-up of
    /// the same name racing ahead of the stop has its fresh container
    /// reaped. This exercises the snapshot primitives directly.
    #[tokio::test]
    async fn take_warm_instances_snapshot_does_not_reap_recreated_pool() {
        let backend = CountingBackend::new("127.0.0.1:1");
        let rt = runtime_with(backend.clone(), 10);
        let mk = |id: &str| {
            Arc::new(super::WarmEntry {
                instance: WarmInstance {
                    endpoint: "127.0.0.1:1".to_string(),
                    handle: BackendHandle::Container { id: id.to_string() },
                },
                last_used: RwLock::new(std::time::Instant::now()),
                deploy_id: "d".to_string(),
                credentials: None,
                retiring: std::sync::atomic::AtomicBool::new(false),
                busy: Arc::new(tokio::sync::Mutex::new(())),
            })
        };

        // A function "f" with one warm instance.
        rt.instances.write().insert(key("f"), vec![mk("old")]);

        // Delete snapshots the pool synchronously and removes it from the map.
        let snapshot = rt.take_warm_instances(&arn("f"), None);
        assert_eq!(snapshot.len(), 1);
        assert!(rt.instances.read().get(&key("f")).is_none());

        // A recreate + warm-up of the same name wins the race ahead of the
        // deferred terminate.
        rt.instances.write().insert(key("f"), vec![mk("new")]);

        // Terminating the snapshot must touch only the old instance.
        rt.terminate_instances(snapshot).await;
        assert_eq!(
            backend.terminates.load(SeqCst),
            1,
            "only the snapshotted instance is terminated"
        );

        // The recreated function keeps its fresh warm instance.
        let pool = rt.instances.read();
        let f = pool
            .get(&key("f"))
            .expect("recreated function pool must survive");
        assert_eq!(f.len(), 1);
    }

    #[tokio::test]
    async fn take_account_instances_takes_only_that_account() {
        let backend = CountingBackend::new("127.0.0.1:1");
        let rt = runtime_with(backend.clone(), 10);
        let mk = |id: &str| {
            Arc::new(super::WarmEntry {
                instance: WarmInstance {
                    endpoint: "127.0.0.1:1".to_string(),
                    handle: BackendHandle::Container { id: id.to_string() },
                },
                last_used: RwLock::new(std::time::Instant::now()),
                deploy_id: "d".to_string(),
                credentials: None,
                retiring: std::sync::atomic::AtomicBool::new(false),
                busy: Arc::new(tokio::sync::Mutex::new(())),
            })
        };
        let other = "arn:aws:lambda:us-east-1:222222222222:function:g:$LATEST".to_string();
        rt.instances.write().insert(key("f"), vec![mk("a")]);
        rt.instances
            .write()
            .insert(other.clone(), vec![mk("b"), mk("c")]);

        let taken = rt.take_account_instances(Some("222222222222"));
        assert_eq!(taken.len(), 2);
        assert!(rt.instances.read().get(&other).is_none());
        // A pool warmed up after the take is not the taken set's business.
        rt.instances.write().insert(other.clone(), vec![mk("new")]);
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
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_millis(5), peak).await;
        let backend = CountingBackend::new(endpoint);
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
    }

    /// An IAM reset drops the credentials warm instances hold: free ones are
    /// retired (and replaced on the next invoke), busy ones finish their
    /// invocation untouched and take no new work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn iam_reset_retires_free_instances_and_spares_busy_ones() {
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_secs(2), peak).await;
        let backend = CountingBackend::new(endpoint);
        let rt = runtime_with(backend.clone(), 2);
        let issuer = RecordingIssuer::new(chrono::Duration::hours(12));
        rt.set_credential_issuer(issuer.clone());

        // Two instances: one left free, one kept busy across the reset.
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

        // Marking is synchronous: from here on no invocation can land on
        // either instance, before any teardown has run.
        rt.mark_credentials_revoked(Some("123456789012"));
        assert!(rt
            .instances
            .read()
            .get(&key("reset"))
            .unwrap()
            .iter()
            .all(|e| e.retiring.load(std::sync::atomic::Ordering::Acquire)));
        assert_eq!(backend.terminates.load(SeqCst), 0);
        // Another account's reset marks nothing further.
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

        // The next invoke never lands on the retiring instance: it gets a
        // fresh one with new credentials, and the now-free survivor goes.
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
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_secs(2), peak).await;
        let backend = CountingBackend::new(endpoint);
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
        assert_eq!(
            backend.terminates.load(SeqCst),
            0,
            "the busy stale instance must not be terminated"
        );
        in_flight
            .await
            .unwrap()
            .expect("in-flight invocation completes");

        // Next launch-path sweep reaps the now-free stale instance.
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
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_millis(5), peak).await;
        let backend = CountingBackend::new(endpoint);
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

        // Deleting one version's pool leaves the other's.
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
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_millis(5), peak).await;
        let backend = CountingBackend::new(endpoint);
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
        ) -> Result<WarmInstance, RuntimeError> {
            if self.hang {
                std::future::pending::<()>().await;
            }
            Err(RuntimeError::ContainerStartFailed("boom".into()))
        }
        async fn terminate(&self, _handle: &BackendHandle) {}
    }

    fn broken_runtime(hang: bool) -> Arc<LambdaRuntime> {
        Arc::new(LambdaRuntime {
            backend: Arc::new(BrokenBackend { hang }),
            instances: RwLock::new(HashMap::new()),
            starting: RwLock::new(HashMap::new()),
            max_concurrency: 2,
            credential_issuer: std::sync::OnceLock::new(),
        })
    }

    /// Credentials minted for a launch that fails, or whose launching future
    /// is dropped mid-launch, are revoked rather than left registered.
    #[tokio::test]
    async fn credentials_of_an_unfinished_launch_are_revoked() {
        let func = test_func("broken", "sha-A");

        let rt = broken_runtime(false);
        let issuer = RecordingIssuer::new(chrono::Duration::hours(12));
        rt.set_credential_issuer(issuer.clone());
        assert!(rt.invoke(&func, b"{}", &[]).await.is_err());
        assert_eq!(*issuer.revoked.lock().unwrap(), vec!["KEY0".to_string()]);

        let rt = broken_runtime(true);
        let issuer = RecordingIssuer::new(chrono::Duration::hours(12));
        rt.set_credential_issuer(issuer.clone());
        let cancelled =
            tokio::time::timeout(Duration::from_millis(100), rt.invoke(&func, b"{}", &[])).await;
        assert!(cancelled.is_err(), "the launch never finishes");
        assert_eq!(*issuer.revoked.lock().unwrap(), vec!["KEY0".to_string()]);
    }

    /// An instance stops taking invocations once its credentials could lapse
    /// during one, judged from the credentials' own expiration.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn instance_whose_credentials_near_expiry_is_replaced() {
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_millis(5), peak).await;
        let backend = CountingBackend::new(endpoint);
        let rt = runtime_with(backend.clone(), 2);
        // Minted with less than the invocation headroom left.
        let issuer = RecordingIssuer::new(chrono::Duration::minutes(10));
        rt.set_credential_issuer(issuer.clone());

        let func = test_func("aging", "sha-A");
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        rt.invoke(&func, b"{}", &[]).await.unwrap();
        assert_eq!(backend.launches.load(SeqCst), 2);
        // The first (free) one was retired on the second launch.
        assert_eq!(*issuer.revoked.lock().unwrap(), vec!["KEY0".to_string()]);
    }

    /// Deleting a version stops its free instances now and leaves a busy one
    /// to finish, retiring it once released; other versions are untouched.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deleting_a_version_spares_its_busy_instance() {
        let peak = Arc::new(AtomicUsize::new(0));
        let endpoint = spawn_rie(Duration::from_secs(2), peak).await;
        let backend = CountingBackend::new(endpoint);
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
}
