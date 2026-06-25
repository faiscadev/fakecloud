//! Backend-agnostic Lambda runtime facade.
//!
//! Owns the warm-pool bookkeeping, per-function startup serialization,
//! and the HTTP invocation path. Dispatches container lifecycle to
//! whatever [`LambdaBackend`] it was constructed with.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine;
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
    /// Combined fingerprint of the function's code SHA-256 plus the
    /// SHA-256 of every attached layer's ZIP bytes, joined in attach
    /// order. Layers mutate `/opt`, so a layer change invalidates the
    /// warm instance even when the function code is unchanged.
    deploy_id: String,
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
/// (`.../_internal/code/{account}/{function}/{deploy}.zip`) and into the
/// `fakecloud-deploy-id` Pod label; standard base64's `/` would grow an
/// extra URL path segment, break the axum route match, and wedge the Pod
/// in a cold-start loop for ~49% of deploys (issue #1643).
fn deploy_id_for(func: &LambdaFunction, layers: &[Vec<u8>]) -> String {
    deploy_id_from(&func.code_sha256, layers)
}

/// Pure core of [`deploy_id_for`], split out so the URL-path-safety
/// invariant can be tested without constructing a full `LambdaFunction`.
fn deploy_id_from(code_sha256: &str, layers: &[Vec<u8>]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(code_sha256.as_bytes());
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
        }
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
                self.evict_entry(&func.function_name, &entry).await;
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
                            self.evict_entry(&func.function_name, &entry).await;
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
                    self.evict_entry(&func.function_name, &entry).await;
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
                self.evict_entry(&func.function_name, &entry).await;
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
                    self.evict_entry(&func.function_name, &entry).await;
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

        let deploy_id = deploy_id_for(func, layers);

        // (1) Fast path: a free instance already running the right deploy.
        if let Some(slot) = self.try_take_free(&func.function_name, &deploy_id) {
            return Ok(slot);
        }

        // Serialize launch decisions per function so a burst of cold
        // invokes doesn't each push the pool past the cap.
        let startup_lock = {
            let mut starting = self.starting.write();
            starting
                .entry(func.function_name.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let startup_guard = startup_lock.lock().await;

        // Re-check under the startup lock — another task may have freed or
        // launched an instance while we waited.
        if let Some(slot) = self.try_take_free(&func.function_name, &deploy_id) {
            return Ok(slot);
        }

        // Tear down any instances left over from a previous deploy.
        self.evict_stale_deploy(&func.function_name, &deploy_id)
            .await;

        let pool_len = self
            .instances
            .read()
            .get(&func.function_name)
            .map_or(0, |v| v.len());

        // (2) Room to grow: launch a fresh instance and reserve it.
        if pool_len < self.max_concurrency {
            let instance = self
                .backend
                .launch(func, func.code_zip.as_deref(), layers, &deploy_id)
                .await?;
            let entry = Arc::new(WarmEntry {
                instance,
                last_used: RwLock::new(Instant::now()),
                deploy_id,
                busy: Arc::new(tokio::sync::Mutex::new(())),
            });
            let guard = entry
                .busy
                .clone()
                .try_lock_owned()
                .expect("freshly created busy lock is uncontended");
            self.instances
                .write()
                .entry(func.function_name.clone())
                .or_default()
                .push(entry.clone());
            return Ok(Slot { entry, guard });
        }

        // (3) At capacity: release the startup lock and wait for whichever
        // current instance frees up *first*. Racing every instance's lock
        // (rather than blocking on a fixed one) avoids convoying every
        // queued caller onto pool[0] while a different instance goes idle.
        drop(startup_guard);
        let candidates: Vec<Arc<WarmEntry>> = {
            let map = self.instances.read();
            map.get(&func.function_name)
                .map(|pool| {
                    pool.iter()
                        .filter(|e| e.deploy_id == deploy_id)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default()
        };
        if candidates.is_empty() {
            return Err(RuntimeError::InvocationFailed(format!(
                "no warm instance available for {}",
                func.function_name
            )));
        }
        let waiters = candidates.into_iter().map(|entry| {
            Box::pin(async move {
                let guard = entry.busy.clone().lock_owned().await;
                Slot { entry, guard }
            })
        });
        let (slot, _idx, _rest) = futures_util::future::select_all(waiters).await;
        *slot.entry.last_used.write() = Instant::now();
        Ok(slot)
    }

    /// Try to reserve a free, current-deploy instance without launching.
    /// Returns `None` if every matching instance is busy (or there are
    /// none).
    fn try_take_free(&self, function_name: &str, deploy_id: &str) -> Option<Slot> {
        let map = self.instances.read();
        let pool = map.get(function_name)?;
        for entry in pool {
            if entry.deploy_id != deploy_id {
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
    async fn evict_entry(&self, function_name: &str, target: &Arc<WarmEntry>) {
        let removed = {
            let mut map = self.instances.write();
            match map.get_mut(function_name) {
                Some(pool) => {
                    let removed = pool
                        .iter()
                        .position(|e| Arc::ptr_eq(e, target))
                        .map(|pos| pool.remove(pos));
                    if pool.is_empty() {
                        map.remove(function_name);
                    }
                    removed
                }
                None => None,
            }
        };
        if let Some(entry) = removed {
            tracing::info!(
                function = %function_name,
                handle = ?entry.instance.handle,
                "evicting unreachable Lambda runtime instance"
            );
            self.backend.terminate(&entry.instance.handle).await;
        }
    }

    /// Tear down every instance in a function's pool whose `deploy_id`
    /// no longer matches the current code+layers fingerprint.
    async fn evict_stale_deploy(&self, function_name: &str, deploy_id: &str) {
        let stale: Vec<Arc<WarmEntry>> = {
            let mut map = self.instances.write();
            match map.get_mut(function_name) {
                Some(pool) => {
                    let mut stale = Vec::new();
                    pool.retain(|e| {
                        if e.deploy_id == deploy_id {
                            true
                        } else {
                            stale.push(e.clone());
                            false
                        }
                    });
                    if pool.is_empty() {
                        map.remove(function_name);
                    }
                    stale
                }
                None => Vec::new(),
            }
        };
        for entry in stale {
            tracing::info!(
                function = %function_name,
                handle = ?entry.instance.handle,
                "stopping stale-deploy Lambda runtime instance"
            );
            self.backend.terminate(&entry.instance.handle).await;
        }
    }

    /// Remove and return the warm pool for a function **without**
    /// terminating it. Lets DeleteFunction snapshot exactly the instances
    /// that exist at delete time and terminate those, so a concurrent
    /// recreate of the same name (whose fresh warm instance is keyed
    /// identically) is not reaped by the deferred stop. Synchronous so the
    /// caller can take the snapshot while still ordered before any recreate
    /// (bug-hunt 2026-06-13, finding 4.2).
    pub(crate) fn take_warm_instances(&self, function_name: &str) -> Vec<Arc<WarmEntry>> {
        self.instances
            .write()
            .remove(function_name)
            .unwrap_or_default()
    }

    /// Terminate a previously-snapshotted set of warm instances. Pairs with
    /// [`take_warm_instances`] for the delete path.
    pub(crate) async fn terminate_instances(&self, pool: Vec<Arc<WarmEntry>>) {
        for entry in pool {
            tracing::info!(
                handle = ?entry.instance.handle,
                "stopping Lambda runtime instance"
            );
            self.backend.terminate(&entry.instance.handle).await;
        }
    }

    /// Stop and remove every warm instance for a specific function.
    pub async fn stop_container(&self, function_name: &str) {
        let pool = self.take_warm_instances(function_name);
        self.terminate_instances(pool).await;
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
                self.backend.terminate(&entry.instance.handle).await;
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
        for (name, pool) in entries.iter() {
            let runtime = accounts
                .iter()
                .find_map(|(_, state)| state.functions.get(name).map(|f| f.runtime.clone()))
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

    /// Evict (stop and remove) every warm instance for a specific
    /// function. Returns true if at least one instance was evicted.
    pub async fn evict_container(&self, function_name: &str) -> bool {
        let pool = self
            .instances
            .write()
            .remove(function_name)
            .unwrap_or_default();
        let found = !pool.is_empty();
        for entry in pool {
            tracing::info!(
                function = %function_name,
                handle = ?entry.instance.handle,
                "evicting Lambda runtime instance via simulation API"
            );
            self.backend.terminate(&entry.instance.handle).await;
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
                    let idle = pool[i].last_used.read().elapsed() > ttl;
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
            self.backend.terminate(&entry.instance.handle).await;
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
            let id = deploy_id_from(&code_sha256, &layers);
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
        let a = deploy_id_from("abc123", &layers);
        let b = deploy_id_from("abc123", &layers);
        assert_eq!(a, b);
        assert_ne!(a, deploy_id_from("abc124", &layers));
        assert_ne!(a, deploy_id_from("abc123", &[]));
    }

    // ---- warm-pool concurrency + eviction (issue #1644) ----

    use super::LambdaRuntime;
    use crate::runtime::backend::{BackendHandle, LambdaBackend, RuntimeError, WarmInstance};
    use crate::state::LambdaFunction;
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
    }

    impl CountingBackend {
        fn new(default_endpoint: impl Into<String>) -> Arc<Self> {
            Arc::new(Self {
                endpoints: StdMutex::new(VecDeque::new()),
                default_endpoint: default_endpoint.into(),
                launches: AtomicUsize::new(0),
                terminates: AtomicUsize::new(0),
            })
        }
        fn with_queue(default_endpoint: impl Into<String>, queue: Vec<String>) -> Arc<Self> {
            Arc::new(Self {
                endpoints: StdMutex::new(queue.into()),
                default_endpoint: default_endpoint.into(),
                launches: AtomicUsize::new(0),
                terminates: AtomicUsize::new(0),
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
        ) -> Result<WarmInstance, RuntimeError> {
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
        })
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
        let pool_len = rt.instances.read().get("upd").map_or(0, |v| v.len());
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
                busy: Arc::new(tokio::sync::Mutex::new(())),
            })
        };

        // A function "f" with one warm instance.
        rt.instances
            .write()
            .insert("f".to_string(), vec![mk("old")]);

        // Delete snapshots the pool synchronously and removes it from the map.
        let snapshot = rt.take_warm_instances("f");
        assert_eq!(snapshot.len(), 1);
        assert!(rt.instances.read().get("f").is_none());

        // A recreate + warm-up of the same name wins the race ahead of the
        // deferred terminate.
        rt.instances
            .write()
            .insert("f".to_string(), vec![mk("new")]);

        // Terminating the snapshot must touch only the old instance.
        rt.terminate_instances(snapshot).await;
        assert_eq!(
            backend.terminates.load(SeqCst),
            1,
            "only the snapshotted instance is terminated"
        );

        // The recreated function keeps its fresh warm instance.
        let pool = rt.instances.read();
        let f = pool.get("f").expect("recreated function pool must survive");
        assert_eq!(f.len(), 1);
    }
}
