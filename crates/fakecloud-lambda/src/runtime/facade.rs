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
                row
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
}
