//! Pluggable Lambda execution backend abstraction.
//!
//! The facade in [`super::facade::LambdaRuntime`] owns the warm-pool
//! bookkeeping, per-function startup serialization, and the HTTP
//! invocation logic that's identical across backends. Backends only
//! implement how to bring up a fresh runtime instance for a function
//! (Docker container, Kubernetes pod, ...), how to tear it down, and
//! optionally how to sweep instances left behind by a previous
//! fakecloud process.

use async_trait::async_trait;

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
    #[error("ZIP extraction failed: {0}")]
    ZipExtractionFailed(String),
}

/// Opaque per-backend identifier for a launched runtime instance. The
/// facade hands this back to the backend on teardown so the backend can
/// find the right resource to delete.
#[derive(Debug, Clone)]
pub enum BackendHandle {
    Container { id: String },
}

/// What [`LambdaBackend::launch`] returns. `endpoint` is the `host:port`
/// the facade POSTs invocation payloads to; `handle` is the
/// backend-specific identifier handed back on `terminate`.
#[derive(Debug, Clone)]
pub struct WarmInstance {
    pub endpoint: String,
    pub handle: BackendHandle,
}

/// Wrapper around an in-flight streaming invocation. Yields raw body
/// chunks via [`Self::next_chunk`] until the RIE closes the response,
/// at which point the final `Ok(None)` signals the caller to emit the
/// terminal `InvokeComplete` frame.
pub struct StreamingInvocation {
    pub(crate) resp: reqwest::Response,
}

impl StreamingInvocation {
    /// Read the next chunk of the function's response body. Returns
    /// `Ok(None)` once the RIE has finished streaming. Buffered
    /// handlers tend to deliver a single chunk; streaming handlers
    /// deliver one chunk per `responseStream.write(...)` call.
    pub async fn next_chunk(&mut self) -> Result<Option<bytes::Bytes>, RuntimeError> {
        match self.resp.chunk().await {
            Ok(Some(b)) => Ok(Some(b)),
            Ok(None) => Ok(None),
            Err(e) => Err(RuntimeError::InvocationFailed(e.to_string())),
        }
    }
}

#[async_trait]
pub trait LambdaBackend: Send + Sync + 'static {
    /// Short identifier surfaced via logs and introspection (e.g.
    /// `"docker"`, `"podman"`, `"kubernetes"`).
    fn name(&self) -> &str;

    /// Launch a fresh runtime instance for `func` using the supplied
    /// code (for zip-package functions) or `func.image_uri` (for image
    /// package functions). `layers` are the attached layer ZIPs in
    /// attach order. `deploy_id` is the facade-computed fingerprint
    /// used to label resources so reaper logic can correlate.
    async fn launch(
        &self,
        func: &LambdaFunction,
        code_zip: Option<&[u8]>,
        layers: &[Vec<u8>],
        deploy_id: &str,
    ) -> Result<WarmInstance, RuntimeError>;

    /// Tear down one instance. Must be idempotent — the facade may call
    /// this against an already-gone instance during cleanup races.
    async fn terminate(&self, handle: &BackendHandle);

    /// Sweep instances that belong to a previous fakecloud process so
    /// their function names don't leak across restarts. Default no-op;
    /// backends with out-of-process state should override.
    async fn reap_stale(&self) {}
}
