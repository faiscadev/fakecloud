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
struct WarmEntry {
    instance: WarmInstance,
    last_used: RwLock<Instant>,
    /// Combined fingerprint of the function's code SHA-256 plus the
    /// SHA-256 of every attached layer's ZIP bytes, joined in attach
    /// order. Layers mutate `/opt`, so a layer change invalidates the
    /// warm instance even when the function code is unchanged.
    deploy_id: String,
}

/// Compute the warm-instance key for a function with its current layer
/// set. Stable across calls — layer ARNs are immutable in AWS, so the
/// hash of their bytes is the right cache key.
fn deploy_id_for(func: &LambdaFunction, layers: &[Vec<u8>]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(func.code_sha256.as_bytes());
    for bytes in layers {
        let mut layer_hasher = Sha256::new();
        layer_hasher.update(bytes);
        hasher.update(b":");
        hasher.update(layer_hasher.finalize());
    }
    base64::engine::general_purpose::STANDARD.encode(hasher.finalize())
}

pub struct LambdaRuntime {
    backend: Arc<dyn LambdaBackend>,
    instances: RwLock<HashMap<String, WarmEntry>>,
    /// Serializes runtime startup per function to prevent duplicate instances.
    starting: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl LambdaRuntime {
    /// Construct a runtime over the supplied backend. Callers that want
    /// auto-detection should use [`Self::auto_detect_docker`] or
    /// [`Self::new`].
    pub fn from_backend(backend: Arc<dyn LambdaBackend>) -> Self {
        Self {
            backend,
            instances: RwLock::new(HashMap::new()),
            starting: RwLock::new(HashMap::new()),
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

    pub fn cli_name(&self) -> &str {
        self.backend.name()
    }

    /// Invoke a Lambda function, starting an instance if needed. Layer
    /// ZIPs are extracted into `/opt` of the runtime sandbox; AWS base
    /// images already include `/opt/python`, `/opt/nodejs/node_modules`,
    /// `/opt/lib`, and `/opt/bin` on the right import paths.
    pub async fn invoke(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
    ) -> Result<Vec<u8>, RuntimeError> {
        let endpoint = self.ensure_warm_instance(func, layers).await?;

        let url = format!("http://{endpoint}/2015-03-31/functions/function/invocations");
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

    /// Invoke a Lambda function and yield the raw HTTP body as a stream
    /// of byte chunks. Each chunk corresponds to one HTTP frame the RIE
    /// flushed to the wire — for streaming-aware handlers this
    /// preserves the chunk boundaries the function emitted. Buffered
    /// handlers come back as a single chunk, which is still a valid
    /// streamed response.
    pub async fn invoke_streaming(
        &self,
        func: &LambdaFunction,
        payload: &[u8],
        layers: &[Vec<u8>],
    ) -> Result<StreamingInvocation, RuntimeError> {
        let endpoint = self.ensure_warm_instance(func, layers).await?;

        let url = format!("http://{endpoint}/2015-03-31/functions/function/invocations");
        let client = reqwest::Client::new();
        let resp = client
            .post(&url)
            .body(payload.to_vec())
            .timeout(Duration::from_secs(func.timeout as u64 + 5))
            .send()
            .await
            .map_err(|e| RuntimeError::InvocationFailed(e.to_string()))?;

        Ok(StreamingInvocation { resp })
    }

    /// Resolve a warm instance for `func`, launching one if its
    /// fingerprint doesn't match (or there isn't one yet). Returns the
    /// invocation endpoint. Shared by `invoke` and `invoke_streaming`
    /// so both paths use the same warm-pool logic.
    async fn ensure_warm_instance(
        &self,
        func: &LambdaFunction,
        layers: &[Vec<u8>],
    ) -> Result<String, RuntimeError> {
        let is_image = func.package_type == "Image";
        if !is_image && func.code_zip.is_none() {
            return Err(RuntimeError::NoCodeZip(func.function_name.clone()));
        }

        let deploy_id = deploy_id_for(func, layers);

        let endpoint = {
            let entries = self.instances.read();
            entries.get(&func.function_name).and_then(|e| {
                if e.deploy_id == deploy_id {
                    *e.last_used.write() = Instant::now();
                    Some(e.instance.endpoint.clone())
                } else {
                    None
                }
            })
        };
        if let Some(ep) = endpoint {
            return Ok(ep);
        }

        let startup_lock = {
            let mut starting = self.starting.write();
            starting
                .entry(func.function_name.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        };
        let _guard = startup_lock.lock().await;

        // Re-check after acquiring lock — another task may have launched it.
        let existing = {
            let entries = self.instances.read();
            entries
                .get(&func.function_name)
                .filter(|e| e.deploy_id == deploy_id)
                .map(|e| {
                    *e.last_used.write() = Instant::now();
                    e.instance.endpoint.clone()
                })
        };
        if let Some(ep) = existing {
            return Ok(ep);
        }

        self.stop_container(&func.function_name).await;

        let instance = self
            .backend
            .launch(func, func.code_zip.as_deref(), layers, &deploy_id)
            .await?;
        let endpoint = instance.endpoint.clone();
        let entry = WarmEntry {
            instance,
            last_used: RwLock::new(Instant::now()),
            deploy_id,
        };
        self.instances
            .write()
            .insert(func.function_name.clone(), entry);
        Ok(endpoint)
    }

    /// Stop and remove a warm instance for a specific function.
    pub async fn stop_container(&self, function_name: &str) {
        let entry = self.instances.write().remove(function_name);
        if let Some(entry) = entry {
            tracing::info!(
                function = %function_name,
                handle = ?entry.instance.handle,
                "stopping Lambda runtime instance"
            );
            self.backend.terminate(&entry.instance.handle).await;
        }
    }

    /// Stop and remove all warm instances (used on server shutdown or reset).
    pub async fn stop_all(&self) {
        let entries: Vec<(String, WarmInstance)> = {
            let mut map = self.instances.write();
            map.drain().map(|(name, e)| (name, e.instance)).collect()
        };
        for (name, instance) in entries {
            tracing::info!(
                function = %name,
                handle = ?instance.handle,
                "stopping Lambda runtime instance (cleanup)"
            );
            self.backend.terminate(&instance.handle).await;
        }
    }

    /// List all warm instances and their metadata for introspection.
    pub fn list_warm_containers(
        &self,
        lambda_state: &crate::state::SharedLambdaState,
    ) -> Vec<serde_json::Value> {
        let entries = self.instances.read();
        let accounts = lambda_state.read();
        entries
            .iter()
            .map(|(name, entry)| {
                let runtime = accounts
                    .iter()
                    .find_map(|(_, state)| state.functions.get(name).map(|f| f.runtime.clone()))
                    .unwrap_or_default();
                let last_used = entry.last_used.read();
                let idle_secs = last_used.elapsed().as_secs();
                let container_id = match &entry.instance.handle {
                    BackendHandle::Container { id } => id.clone(),
                };
                serde_json::json!({
                    "functionName": name,
                    "runtime": runtime,
                    "containerId": container_id,
                    "lastUsedSecsAgo": idle_secs,
                })
            })
            .collect()
    }

    /// Evict (stop and remove) the warm instance for a specific function.
    /// Returns true if an instance was found and evicted.
    pub async fn evict_container(&self, function_name: &str) -> bool {
        let entry = self.instances.write().remove(function_name);
        if let Some(entry) = entry {
            tracing::info!(
                function = %function_name,
                handle = ?entry.instance.handle,
                "evicting Lambda runtime instance via simulation API"
            );
            self.backend.terminate(&entry.instance.handle).await;
            true
        } else {
            false
        }
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
        let expired: Vec<String> = {
            let entries = self.instances.read();
            entries
                .iter()
                .filter(|(_, e)| e.last_used.read().elapsed() > ttl)
                .map(|(name, _)| name.clone())
                .collect()
        };
        for name in expired {
            tracing::info!(function = %name, "stopping idle Lambda runtime instance");
            self.stop_container(&name).await;
        }
    }
}
