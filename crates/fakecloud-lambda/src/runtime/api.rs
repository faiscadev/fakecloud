//! The Lambda Runtime API (`2018-06-01`), served by fakecloud itself.
//!
//! On AWS a function's runtime client never sees the Invoke API: it long-polls
//! `GET /2018-06-01/runtime/invocation/next` on the address in
//! `AWS_LAMBDA_RUNTIME_API` for its next event and reports the outcome
//! explicitly, `POST .../invocation/<id>/response` for a returned value,
//! `POST .../invocation/<id>/error` for an error the handler raised and
//! `POST /2018-06-01/runtime/init/error` for a failed initialization. Serving
//! that API is what lets fakecloud tell those apart exactly: a handler that
//! *returns* an object shaped like an error is a success, as on AWS.
//!
//! Every execution environment (warm instance) gets its own [`Sandbox`],
//! addressed by an unguessable per-instance host name
//! (`fakecloud-runtime-<token>`) that the backend maps to the fakecloud
//! listener inside the instance. Requests arrive on the main listener and are
//! routed by their `Host` header ([`RuntimeApi::serve`]), so routing never
//! depends on which port or address a container reaches fakecloud through,
//! and one instance can only ever see and answer its own invocations.
//!
//! The same sandbox serves the Extensions API (`2020-01-01`) to extensions
//! started beside the runtime (see [`super::extensions`]): `register`,
//! `event/next` (`INVOKE` / `SHUTDOWN`), `init/error` and `exit/error`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::response::Response;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::BodyExt;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, Notify};

/// Host names of execution environments start with this; the rest is the
/// sandbox token.
pub const RUNTIME_HOST_PREFIX: &str = "fakecloud-runtime-";

/// The largest buffered response a synchronous invocation may return
/// (6 MB + 100 bytes of envelope, as Lambda reports it).
pub const MAX_SYNC_RESPONSE_BYTES: usize = 6_291_556;

const API_PREFIX: &str = "/2018-06-01/runtime";
const EXTENSION_PREFIX: &str = "/2020-01-01/extension";
const EXTENSION_NAME_HEADER: &str = "lambda-extension-name";
const EXTENSION_ID_HEADER: &str = "lambda-extension-identifier";
const EXTENSION_ERROR_TYPE_HEADER: &str = "lambda-extension-function-error-type";
const ERROR_TYPE_HEADER: &str = "lambda-runtime-function-error-type";
const ERROR_BODY_TRAILER: &str = "lambda-runtime-function-error-body";
const RESPONSE_MODE_HEADER: &str = "lambda-runtime-function-response-mode";

/// The host name the runtime of the execution environment with `token`
/// reaches the Runtime API at.
pub fn sandbox_host(token: &str) -> String {
    format!("{RUNTIME_HOST_PREFIX}{token}")
}

/// The sandbox token a `Host` header names, if it is a Runtime API host.
pub fn token_from_host(host: &str) -> Option<&str> {
    let name = match host.rsplit_once(':') {
        Some((name, port)) if port.bytes().all(|b| b.is_ascii_digit()) => name,
        _ => host,
    };
    let token = name.strip_prefix(RUNTIME_HOST_PREFIX)?;
    (!token.is_empty() && token.bytes().all(|b| b.is_ascii_alphanumeric())).then_some(token)
}

/// One event handed to a runtime through `/invocation/next`.
#[derive(Debug, Clone)]
pub struct InvocationEvent {
    pub request_id: String,
    pub payload: Bytes,
    /// `Lambda-Runtime-Invoked-Function-Arn`: the ARN the caller invoked,
    /// qualified when the caller named a version or alias.
    pub invoked_function_arn: String,
    /// `Lambda-Runtime-Trace-Id`.
    pub trace_id: String,
    /// `Lambda-Runtime-Client-Context`: the client context JSON.
    pub client_context: Option<String>,
    /// `Lambda-Runtime-Cognito-Identity`: the Cognito identity JSON.
    pub cognito_identity: Option<String>,
    /// The function's timeout; the deadline runs from delivery.
    pub timeout: Duration,
    /// Largest buffered response accepted, `None` for no limit.
    pub max_response_bytes: Option<usize>,
}

/// What a runtime reports about an invocation it was dispatched, in order:
/// `Delivered` once it takes the event, then zero or more `Chunk`s of the
/// response body, then exactly one `Done`.
#[derive(Debug)]
pub enum RuntimeSignal {
    /// The runtime took the event; it must answer by `deadline`.
    Delivered {
        deadline: SystemTime,
    },
    /// A piece of the response body, in arrival order.
    Chunk(Bytes),
    Done(InvocationOutcome),
}

/// How an invocation ended, as the runtime reported it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvocationOutcome {
    /// `POST /invocation/<id>/response` completed: the chunks are the result.
    Response,
    /// The handler failed: `.../error`, or a streamed response that ended
    /// with an error trailer. The payload is the runtime's error object.
    Error(Vec<u8>),
    /// The runtime failed to initialize (`/init/error`).
    InitError(Vec<u8>),
    /// The buffered response exceeded [`InvocationEvent::max_response_bytes`].
    ResponseTooLarge,
}

/// A Runtime API request the runtime got wrong, with Lambda's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApiError {
    /// The request ID names no invocation in flight on this sandbox.
    InvalidRequestId,
    /// The call is not valid in the sandbox's current state.
    InvalidStateTransition(&'static str),
    /// The response body is larger than the invocation allows.
    PayloadTooLarge(usize),
    /// The sandbox was torn down.
    Gone,
    /// An Extensions API call without (or with an unknown)
    /// `Lambda-Extension-Identifier`.
    UnknownExtension,
    /// `/extension/register` without `Lambda-Extension-Name`.
    MissingExtensionName,
}

impl ApiError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match self {
            Self::InvalidRequestId => (
                StatusCode::BAD_REQUEST,
                "InvalidRequestID",
                "Invalid request ID".to_string(),
            ),
            Self::InvalidStateTransition(what) => (
                StatusCode::FORBIDDEN,
                "InvalidStateTransition",
                format!("State transition from {what} failed for runtime. Error: State transition is not allowed"),
            ),
            Self::PayloadTooLarge(limit) => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "RequestEntityTooLarge",
                format!("Exceeded maximum allowed payload size ({limit} bytes)."),
            ),
            Self::Gone => (
                StatusCode::GONE,
                "Runtime.Unavailable",
                "The execution environment was shut down".to_string(),
            ),
            Self::UnknownExtension => (
                StatusCode::FORBIDDEN,
                "Extension.UnknownExtensionIdentifier",
                "Unknown or missing Lambda-Extension-Identifier".to_string(),
            ),
            Self::MissingExtensionName => (
                StatusCode::FORBIDDEN,
                "Extension.MissingExtensionName",
                "Missing Lambda-Extension-Name header".to_string(),
            ),
        };
        json_response(
            status,
            serde_json::json!({ "errorMessage": message, "errorType": error_type }).to_string(),
        )
    }
}

struct Queued {
    event: InvocationEvent,
    tx: mpsc::UnboundedSender<RuntimeSignal>,
}

struct InFlight {
    request_id: String,
    tx: mpsc::UnboundedSender<RuntimeSignal>,
    max_response_bytes: Option<usize>,
    /// A `/response` body is being received.
    responding: bool,
}

/// What an extension's `/register` answers with: the function it runs
/// beside.
#[derive(Debug, Clone, Default)]
pub struct SandboxInfo {
    pub function_name: String,
    pub function_version: String,
    pub handler: String,
}

/// An extension registered through the Extensions API (`2020-01-01`).
struct Extension {
    invoke: bool,
    shutdown: bool,
    /// Events waiting for its next `/event/next`.
    queue: VecDeque<serde_json::Value>,
    /// It is in `/event/next`, done with everything it was sent.
    waiting: bool,
}

#[derive(Default)]
struct State {
    /// Extension processes started with the runtime; events wait until
    /// that many have registered.
    expected_extensions: usize,
    extensions: HashMap<String, Extension>,
    /// An event has been handed to the runtime (initialization is over).
    delivered_any: bool,
    /// The runtime has asked for its first event: initialization is done.
    initialized: bool,
    /// The error the runtime reported from `/init/error`.
    init_error: Option<Vec<u8>>,
    /// Dispatched, not yet taken by `/next`.
    queued: Option<Queued>,
    /// Taken by `/next`, not yet answered.
    current: Option<InFlight>,
    /// The sandbox was unregistered.
    closed: bool,
}

/// The Runtime API state of one execution environment.
pub struct Sandbox {
    token: String,
    info: SandboxInfo,
    state: Mutex<State>,
    /// Woken (all waiters) on every state change a waiting `/next` cares
    /// about.
    event_ready: Notify,
}

/// A dispatched invocation: the runtime's reports, plus whether the sandbox
/// had already initialized when the event was queued (an event queued on an
/// initialized sandbox that dies before taking it never ran).
pub struct Dispatched {
    pub signals: mpsc::UnboundedReceiver<RuntimeSignal>,
    pub warm: bool,
}

impl Sandbox {
    fn new(token: String, info: SandboxInfo) -> Self {
        Self {
            token,
            info,
            state: Mutex::new(State::default()),
            event_ready: Notify::new(),
        }
    }

    /// Declare how many extension processes start with the runtime: the
    /// first event waits until they have all registered and finished their
    /// init (asked for their first event), as on Lambda.
    pub fn expect_extensions(&self, count: usize) {
        self.state.lock().expected_extensions = count;
    }

    pub fn token(&self) -> &str {
        &self.token
    }

    /// The host name this sandbox's runtime addresses the API by.
    pub fn host(&self) -> String {
        sandbox_host(&self.token)
    }

    pub fn is_initialized(&self) -> bool {
        self.state.lock().initialized
    }

    /// Queue `event` for the runtime's next `/invocation/next`.
    pub fn dispatch(&self, event: InvocationEvent) -> Dispatched {
        let (tx, signals) = mpsc::unbounded_channel();
        let mut st = self.state.lock();
        let warm = st.initialized;
        if st.closed {
            // Nothing will ever take it; the caller sees the channel close.
            return Dispatched { signals, warm };
        }
        if let Some(payload) = st.init_error.clone() {
            let _ = tx.send(RuntimeSignal::Done(InvocationOutcome::InitError(payload)));
            return Dispatched { signals, warm };
        }
        st.queued = Some(Queued { event, tx });
        drop(st);
        self.event_ready.notify_waiters();
        Dispatched { signals, warm }
    }

    /// Forget the invocation `request_id` (it timed out, or its caller went
    /// away): a late answer from the runtime is refused as an unknown ID.
    pub fn abandon(&self, request_id: &str) {
        let mut st = self.state.lock();
        if st
            .queued
            .as_ref()
            .is_some_and(|q| q.event.request_id == request_id)
        {
            st.queued = None;
        }
        if st
            .current
            .as_ref()
            .is_some_and(|c| c.request_id == request_id)
        {
            st.current = None;
        }
    }

    fn close(&self) {
        let mut st = self.state.lock();
        st.closed = true;
        st.queued = None;
        st.current = None;
        drop(st);
        self.event_ready.notify_waiters();
    }

    /// `GET /invocation/next`: wait for the next event. The first call ends
    /// the runtime's initialization.
    pub async fn next(&self) -> Result<NextEvent, ApiError> {
        {
            let mut st = self.state.lock();
            if st.init_error.is_some() {
                return Err(ApiError::InvalidStateTransition("InitError to Ready"));
            }
            st.initialized = true;
        }
        loop {
            let notified = self.event_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut st = self.state.lock();
                if st.closed {
                    return Err(ApiError::Gone);
                }
                let ready = extensions_ready(&st);
                if let Some(Queued { event, tx }) = st.queued.take_if(|_| ready) {
                    if tx.is_closed() {
                        // The caller gave up before the runtime asked.
                        continue;
                    }
                    let deadline = SystemTime::now() + event.timeout;
                    let _ = tx.send(RuntimeSignal::Delivered { deadline });
                    st.delivered_any = true;
                    let invoke = invoke_event(&event, deadline);
                    let mut woke = false;
                    for ext in st.extensions.values_mut().filter(|e| e.invoke) {
                        ext.queue.push_back(invoke.clone());
                        woke = true;
                    }
                    if woke {
                        self.event_ready.notify_waiters();
                    }
                    st.current = Some(InFlight {
                        request_id: event.request_id.clone(),
                        tx,
                        max_response_bytes: event.max_response_bytes,
                        responding: false,
                    });
                    return Ok(NextEvent { event, deadline });
                }
            }
            notified.await;
        }
    }

    /// `POST /extension/register`: a new extension, for the given events.
    /// Returns its identifier.
    pub fn register_extension(&self, events: &[String]) -> Result<String, ApiError> {
        let mut st = self.state.lock();
        if st.closed {
            return Err(ApiError::Gone);
        }
        if st.delivered_any {
            return Err(ApiError::InvalidStateTransition(
                "Ready to ExtensionRegister",
            ));
        }
        let id = uuid::Uuid::new_v4().to_string();
        st.extensions.insert(
            id.clone(),
            Extension {
                invoke: events.iter().any(|e| e == "INVOKE"),
                shutdown: events.iter().any(|e| e == "SHUTDOWN"),
                queue: VecDeque::new(),
                waiting: false,
            },
        );
        Ok(id)
    }

    /// `GET /extension/event/next`: the extension is done with what it was
    /// sent; wait for its next event.
    pub async fn extension_next(&self, id: &str) -> Result<serde_json::Value, ApiError> {
        loop {
            let notified = self.event_ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut st = self.state.lock();
                if st.closed {
                    return Err(ApiError::Gone);
                }
                let ext = st
                    .extensions
                    .get_mut(id)
                    .ok_or(ApiError::UnknownExtension)?;
                if let Some(event) = ext.queue.pop_front() {
                    ext.waiting = false;
                    return Ok(event);
                }
                if !ext.waiting {
                    ext.waiting = true;
                    drop(st);
                    // A runtime waiting on extensions may proceed.
                    self.event_ready.notify_waiters();
                }
            }
            notified.await;
        }
    }

    /// `POST /extension/init/error` or `/extension/exit/error`: the
    /// extension failed, which fails the environment like a runtime init
    /// error (the invocation in flight, or the one waiting for init, gets
    /// the error, and the environment is reset).
    pub fn extension_error(
        &self,
        id: &str,
        error_type: Option<&str>,
        body: &[u8],
    ) -> Result<(), ApiError> {
        let mut st = self.state.lock();
        if st.closed {
            return Err(ApiError::Gone);
        }
        if !st.extensions.contains_key(id) {
            return Err(ApiError::UnknownExtension);
        }
        let payload = error_payload(error_type, body);
        if let Some(q) = st.queued.take() {
            let _ = q.tx.send(RuntimeSignal::Done(InvocationOutcome::InitError(
                payload.clone(),
            )));
        }
        if let Some(c) = st.current.take() {
            let _ = c.tx.send(RuntimeSignal::Done(InvocationOutcome::InitError(
                payload.clone(),
            )));
        }
        st.init_error.get_or_insert(payload);
        Ok(())
    }

    /// Send `SHUTDOWN` to the extensions registered for it and give them up
    /// to `grace` to finish (call `/event/next` again), before the
    /// environment is torn down.
    pub async fn shutdown_extensions(&self, reason: &str, grace: Duration) {
        let deadline = SystemTime::now() + grace;
        let deadline_ms = deadline
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let event = serde_json::json!({
            "eventType": "SHUTDOWN",
            "shutdownReason": reason,
            "deadlineMs": deadline_ms,
        });
        let targets: Vec<String> = {
            let mut st = self.state.lock();
            st.extensions
                .iter_mut()
                .filter(|(_, e)| e.shutdown)
                .map(|(id, e)| {
                    e.queue.push_back(event.clone());
                    id.clone()
                })
                .collect()
        };
        if targets.is_empty() {
            return;
        }
        self.event_ready.notify_waiters();
        let done = async {
            loop {
                let notified = self.event_ready.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let st = self.state.lock();
                    let finished = st.closed
                        || targets.iter().all(|id| {
                            st.extensions
                                .get(id)
                                .is_none_or(|e| e.waiting && e.queue.is_empty())
                        });
                    if finished {
                        return;
                    }
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(grace, done).await;
    }

    /// Start receiving the response to `request_id`.
    pub fn begin_response(&self, request_id: &str) -> Result<ResponseWriter<'_>, ApiError> {
        let mut st = self.state.lock();
        if st.closed {
            return Err(ApiError::Gone);
        }
        let current = st
            .current
            .as_mut()
            .filter(|c| c.request_id == request_id)
            .ok_or(ApiError::InvalidRequestId)?;
        if current.responding {
            return Err(ApiError::InvalidStateTransition(
                "InvocationResponse to InvocationResponse",
            ));
        }
        current.responding = true;
        Ok(ResponseWriter {
            sandbox: self,
            request_id: request_id.to_string(),
            tx: current.tx.clone(),
            limit: current.max_response_bytes,
            written: 0,
            finished: false,
        })
    }

    /// End the invocation `request_id` with `outcome`, if it is still the
    /// one in flight.
    fn complete(&self, request_id: &str, outcome: InvocationOutcome) -> Result<(), ApiError> {
        let mut st = self.state.lock();
        if st.closed {
            return Err(ApiError::Gone);
        }
        match st.current.take() {
            Some(c) if c.request_id == request_id => {
                let _ = c.tx.send(RuntimeSignal::Done(outcome));
                Ok(())
            }
            other => {
                st.current = other;
                Err(ApiError::InvalidRequestId)
            }
        }
    }

    /// `POST /invocation/<id>/error`.
    pub fn post_error(
        &self,
        request_id: &str,
        error_type: Option<&str>,
        body: &[u8],
    ) -> Result<(), ApiError> {
        {
            let st = self.state.lock();
            if st
                .current
                .as_ref()
                .is_some_and(|c| c.request_id == request_id && c.responding)
            {
                return Err(ApiError::InvalidStateTransition(
                    "InvocationResponse to InvocationError",
                ));
            }
        }
        self.complete(
            request_id,
            InvocationOutcome::Error(error_payload(error_type, body)),
        )
    }

    /// `POST /init/error`.
    pub fn post_init_error(&self, error_type: Option<&str>, body: &[u8]) -> Result<(), ApiError> {
        let mut st = self.state.lock();
        if st.closed {
            return Err(ApiError::Gone);
        }
        if st.initialized || st.init_error.is_some() {
            return Err(ApiError::InvalidStateTransition("Ready to InitError"));
        }
        let payload = error_payload(error_type, body);
        if let Some(q) = st.queued.take() {
            let _ = q.tx.send(RuntimeSignal::Done(InvocationOutcome::InitError(
                payload.clone(),
            )));
        }
        st.init_error = Some(payload);
        Ok(())
    }
}

/// An event the runtime took, with the deadline it must answer by.
#[derive(Debug, Clone)]
pub struct NextEvent {
    pub event: InvocationEvent,
    pub deadline: SystemTime,
}

/// Every expected extension has registered, and every registered one is
/// waiting in `/event/next` with nothing left to do.
fn extensions_ready(st: &State) -> bool {
    st.extensions.len() >= st.expected_extensions
        && st
            .extensions
            .values()
            .all(|e| e.waiting && e.queue.is_empty())
}

/// The `INVOKE` event extensions registered for it receive.
fn invoke_event(event: &InvocationEvent, deadline: SystemTime) -> serde_json::Value {
    serde_json::json!({
        "eventType": "INVOKE",
        "deadlineMs": deadline.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis(),
        "requestId": event.request_id,
        "invokedFunctionArn": event.invoked_function_arn,
        "tracing": { "type": "X-Amzn-Trace-Id", "value": event.trace_id },
    })
}

/// Receives one `/response` body, chunk by chunk.
pub struct ResponseWriter<'a> {
    sandbox: &'a Sandbox,
    request_id: String,
    tx: mpsc::UnboundedSender<RuntimeSignal>,
    limit: Option<usize>,
    written: usize,
    finished: bool,
}

impl ResponseWriter<'_> {
    /// Forward a piece of the body. Fails once the body outgrows the
    /// invocation's limit, which ends the invocation.
    pub fn chunk(&mut self, data: Bytes) -> Result<(), ApiError> {
        if data.is_empty() {
            return Ok(());
        }
        self.written += data.len();
        if let Some(limit) = self.limit.filter(|l| self.written > *l) {
            self.finished = true;
            let _ = self
                .sandbox
                .complete(&self.request_id, InvocationOutcome::ResponseTooLarge);
            return Err(ApiError::PayloadTooLarge(limit));
        }
        let _ = self.tx.send(RuntimeSignal::Chunk(data));
        Ok(())
    }

    /// The body ended: a success, or the error a streamed response's
    /// trailers reported.
    pub fn finish(mut self, error: Option<Vec<u8>>) -> Result<(), ApiError> {
        self.finished = true;
        let outcome = match error {
            Some(payload) => InvocationOutcome::Error(payload),
            None => InvocationOutcome::Response,
        };
        self.sandbox.complete(&self.request_id, outcome)
    }
}

impl Drop for ResponseWriter<'_> {
    fn drop(&mut self) {
        if !self.finished {
            // The runtime's connection dropped mid-body: the invocation got
            // no complete answer, which Lambda reports as a failed response.
            let payload = serde_json::json!({
                "errorMessage": "The runtime closed the connection before sending the full response",
                "errorType": "Runtime.TruncatedResponse",
            })
            .to_string()
            .into_bytes();
            let _ = self
                .sandbox
                .complete(&self.request_id, InvocationOutcome::Error(payload));
        }
    }
}

/// The function error object for a runtime-reported error: the body the
/// runtime posted, as Lambda passes it through, or (for an empty body) one
/// built from the `Lambda-Runtime-Function-Error-Type` header.
fn error_payload(error_type: Option<&str>, body: &[u8]) -> Vec<u8> {
    if !body.iter().all(u8::is_ascii_whitespace) {
        return body.to_vec();
    }
    serde_json::json!({
        "errorMessage": "",
        "errorType": error_type.filter(|t| !t.is_empty()).unwrap_or("Runtime.Unknown"),
    })
    .to_string()
    .into_bytes()
}

/// Every live sandbox, by token.
#[derive(Default)]
pub struct RuntimeApi {
    sandboxes: RwLock<HashMap<String, Arc<Sandbox>>>,
}

impl RuntimeApi {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Create the Runtime API state for a new execution environment.
    pub fn register(&self) -> Arc<Sandbox> {
        self.register_for(SandboxInfo::default())
    }

    /// Create the Runtime API state for a new execution environment of the
    /// function `info` describes.
    pub fn register_for(&self, info: SandboxInfo) -> Arc<Sandbox> {
        // 122 random bits, and short enough that the host name stays one
        // DNS label (at most 63 characters): runtime clients IDNA-encode the
        // host and refuse a longer label (Python's `http.client` does).
        let token = uuid::Uuid::new_v4().simple().to_string();
        let sandbox = Arc::new(Sandbox::new(token.clone(), info));
        self.sandboxes.write().insert(token, sandbox.clone());
        sandbox
    }

    /// Drop a torn-down environment: its runtime's requests are refused and
    /// a pending `/next` returns.
    pub fn unregister(&self, sandbox: &Sandbox) {
        self.sandboxes.write().remove(&sandbox.token);
        sandbox.close();
    }

    /// Release every runtime waiting on `/next` (server shutdown): idle
    /// runtimes' long polls would otherwise hold graceful shutdown open.
    pub fn shutdown(&self) {
        let sandboxes: Vec<Arc<Sandbox>> = self.sandboxes.write().drain().map(|(_, s)| s).collect();
        for sandbox in sandboxes {
            sandbox.close();
        }
    }

    pub fn sandbox(&self, token: &str) -> Option<Arc<Sandbox>> {
        self.sandboxes.read().get(token).cloned()
    }

    pub fn len(&self) -> usize {
        self.sandboxes.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.sandboxes.read().is_empty()
    }

    /// Whether `headers` address a Runtime API host (see [`sandbox_host`]).
    pub fn is_runtime_request(headers: &HeaderMap) -> bool {
        host_token(headers).is_some()
    }

    /// Answer one Runtime API request.
    pub async fn serve(&self, req: axum::extract::Request) -> Response {
        let path = req.uri().path().to_string();
        let Some(sandbox) = host_token(req.headers()).and_then(|t| self.sandbox(t)) else {
            tracing::debug!(%path, "Runtime API request for an unknown execution environment");
            return not_found();
        };
        tracing::debug!(sandbox = %sandbox.token, method = %req.method(), %path, "Runtime API request");
        if let Some(rest) = path.strip_prefix(EXTENSION_PREFIX) {
            return serve_extension(&sandbox, rest, req).await;
        }
        let Some(rest) = path.strip_prefix(API_PREFIX) else {
            return not_found();
        };
        let segments: Vec<&str> = rest.trim_start_matches('/').split('/').collect();
        let method = req.method().clone();
        match segments.as_slice() {
            ["invocation", "next"] => {
                if method != Method::GET {
                    return method_not_allowed();
                }
                match sandbox.next().await {
                    Ok(event) => next_response(event),
                    Err(e) => e.into_response(),
                }
            }
            ["invocation", id, "response"] => {
                if method != Method::POST {
                    return method_not_allowed();
                }
                let id = (*id).to_string();
                serve_response(&sandbox, &id, req).await
            }
            ["invocation", id, "error"] => {
                if method != Method::POST {
                    return method_not_allowed();
                }
                let id = (*id).to_string();
                let error_type = header_str(req.headers(), ERROR_TYPE_HEADER);
                let Ok(body) = req.into_body().collect().await else {
                    return ApiError::InvalidRequestId.into_response();
                };
                match sandbox.post_error(&id, error_type.as_deref(), &body.to_bytes()) {
                    Ok(()) => accepted(),
                    Err(e) => e.into_response(),
                }
            }
            ["init", "error"] => {
                if method != Method::POST {
                    return method_not_allowed();
                }
                let error_type = header_str(req.headers(), ERROR_TYPE_HEADER);
                let Ok(body) = req.into_body().collect().await else {
                    return accepted();
                };
                match sandbox.post_init_error(error_type.as_deref(), &body.to_bytes()) {
                    Ok(()) => accepted(),
                    Err(e) => e.into_response(),
                }
            }
            _ => not_found(),
        }
    }
}

/// The Extensions API (`/2020-01-01/extension/*`).
async fn serve_extension(sandbox: &Sandbox, rest: &str, req: axum::extract::Request) -> Response {
    let method = req.method().clone();
    let id = header_str(req.headers(), EXTENSION_ID_HEADER);
    match rest.trim_start_matches('/') {
        "register" => {
            if method != Method::POST {
                return method_not_allowed();
            }
            if header_str(req.headers(), EXTENSION_NAME_HEADER).is_none() {
                return ApiError::MissingExtensionName.into_response();
            }
            let body = req.into_body().collect().await.map(|b| b.to_bytes());
            let events: Vec<String> = body
                .ok()
                .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
                .and_then(|v| {
                    v.get("events").and_then(|e| e.as_array()).map(|a| {
                        a.iter()
                            .filter_map(|e| e.as_str().map(str::to_string))
                            .collect()
                    })
                })
                .unwrap_or_default();
            match sandbox.register_extension(&events) {
                Ok(id) => {
                    let body = serde_json::json!({
                        "functionName": sandbox.info.function_name,
                        "functionVersion": sandbox.info.function_version,
                        "handler": sandbox.info.handler,
                    });
                    let mut resp = json_response(StatusCode::OK, body.to_string());
                    if let Ok(v) = HeaderValue::from_str(&id) {
                        resp.headers_mut().insert("Lambda-Extension-Identifier", v);
                    }
                    resp
                }
                Err(e) => e.into_response(),
            }
        }
        "event/next" => {
            if method != Method::GET {
                return method_not_allowed();
            }
            let Some(id) = id else {
                return ApiError::UnknownExtension.into_response();
            };
            match sandbox.extension_next(&id).await {
                Ok(event) => {
                    let mut resp = json_response(StatusCode::OK, event.to_string());
                    if let Ok(v) = HeaderValue::from_str(&uuid::Uuid::new_v4().to_string()) {
                        resp.headers_mut()
                            .insert("Lambda-Extension-Event-Identifier", v);
                    }
                    resp
                }
                Err(e) => e.into_response(),
            }
        }
        "init/error" | "exit/error" => {
            if method != Method::POST {
                return method_not_allowed();
            }
            let Some(id) = id else {
                return ApiError::UnknownExtension.into_response();
            };
            let error_type = header_str(req.headers(), EXTENSION_ERROR_TYPE_HEADER);
            let body = req
                .into_body()
                .collect()
                .await
                .map(|b| b.to_bytes())
                .unwrap_or_default();
            match sandbox.extension_error(&id, error_type.as_deref(), &body) {
                Ok(()) => accepted(),
                Err(e) => e.into_response(),
            }
        }
        _ => not_found(),
    }
}

fn host_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .and_then(token_from_host)
}

fn header_str(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// `/response`: relay the body as it arrives. A streamed response
/// (`Lambda-Runtime-Function-Response-Mode: streaming`) reports a mid-stream
/// failure in the `Lambda-Runtime-Function-Error-Type` /
/// `Lambda-Runtime-Function-Error-Body` (base64) trailers.
async fn serve_response(
    sandbox: &Sandbox,
    request_id: &str,
    req: axum::extract::Request,
) -> Response {
    let streaming = header_str(req.headers(), RESPONSE_MODE_HEADER)
        .is_some_and(|m| m.eq_ignore_ascii_case("streaming"));
    let mut writer = match sandbox.begin_response(request_id) {
        Ok(w) => w,
        Err(e) => return e.into_response(),
    };
    let mut body = req.into_body();
    let mut trailers: Option<HeaderMap> = None;
    while let Some(frame) = body.frame().await {
        let frame = match frame {
            Ok(f) => f,
            // The connection dropped mid-body; the writer's drop reports it.
            Err(_) => return ApiError::InvalidRequestId.into_response(),
        };
        match frame.into_data() {
            Ok(data) => {
                if let Err(e) = writer.chunk(data) {
                    return e.into_response();
                }
            }
            Err(frame) => {
                if let Ok(t) = frame.into_trailers() {
                    trailers = Some(t);
                }
            }
        }
    }
    let error = trailers
        .as_ref()
        .filter(|_| streaming)
        .and_then(stream_error_from_trailers);
    match writer.finish(error) {
        Ok(()) => accepted(),
        Err(e) => e.into_response(),
    }
}

/// The error a streamed response's trailers report, if any.
fn stream_error_from_trailers(trailers: &HeaderMap) -> Option<Vec<u8>> {
    let error_type = header_str(trailers, ERROR_TYPE_HEADER);
    let body = header_str(trailers, ERROR_BODY_TRAILER).and_then(|b| {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode(b.trim())
            .ok()
    });
    if error_type.is_none() && body.is_none() {
        return None;
    }
    Some(error_payload(
        error_type.as_deref(),
        body.as_deref().unwrap_or_default(),
    ))
}

fn next_response(NextEvent { event, deadline }: NextEvent) -> Response {
    let deadline_ms = deadline
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis()
        .to_string();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .header("Lambda-Runtime-Aws-Request-Id", &event.request_id)
        .header("Lambda-Runtime-Deadline-Ms", deadline_ms)
        .header(
            "Lambda-Runtime-Invoked-Function-Arn",
            &event.invoked_function_arn,
        )
        .header("Lambda-Runtime-Trace-Id", &event.trace_id);
    for (name, value) in [
        ("Lambda-Runtime-Client-Context", &event.client_context),
        ("Lambda-Runtime-Cognito-Identity", &event.cognito_identity),
    ] {
        if let Some(v) = value.as_deref().and_then(|v| HeaderValue::from_str(v).ok()) {
            builder = builder.header(name, v);
        }
    }
    builder
        .body(Body::from(event.payload))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response_body())
}

fn accepted() -> Response {
    json_response(StatusCode::ACCEPTED, r#"{"status":"OK"}"#.to_string())
}

fn json_response(status: StatusCode, body: String) -> Response {
    Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(Body::from(body))
        .unwrap_or_else(|_| status.into_response_body())
}

/// What the Runtime API answers an unknown path with.
fn not_found() -> Response {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(http::header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .body(Body::from("404 page not found\n"))
        .unwrap_or_else(|_| StatusCode::NOT_FOUND.into_response_body())
}

fn method_not_allowed() -> Response {
    StatusCode::METHOD_NOT_ALLOWED.into_response_body()
}

trait IntoResponseBody {
    fn into_response_body(self) -> Response;
}

impl IntoResponseBody for StatusCode {
    fn into_response_body(self) -> Response {
        let mut resp = Response::new(Body::empty());
        *resp.status_mut() = self;
        resp
    }
}

/// A fresh X-Ray trace header for an invocation the caller sent none for.
pub fn new_trace_id() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let id = uuid::Uuid::new_v4().simple().to_string();
    let parent = uuid::Uuid::new_v4().simple().to_string();
    format!(
        "Root=1-{secs:08x}-{};Parent={};Sampled=0",
        &id[..24],
        &parent[..16]
    )
}

/// How an in-process runtime (see [`spawn_in_process_runtime`]) answers an
/// event.
#[derive(Debug, Clone)]
pub enum InProcessReply {
    /// `POST /response` with this body.
    Response(Vec<u8>),
    /// A streamed `/response` sent in these pieces.
    Stream(Vec<Vec<u8>>),
    /// `POST /error` with this error type header and body.
    Error {
        error_type: Option<String>,
        body: Vec<u8>,
    },
    /// Never answer (a hung handler).
    Hang,
}

/// Run a runtime loop in this process against `sandbox`, answering each
/// event with `handler`, the way a runtime client in a container would over
/// HTTP. For backend doubles in tests across the workspace.
#[doc(hidden)]
pub fn spawn_in_process_runtime<F, Fut>(
    sandbox: Arc<Sandbox>,
    mut handler: F,
) -> tokio::task::JoinHandle<()>
where
    F: FnMut(InvocationEvent) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = InProcessReply> + Send,
{
    tokio::spawn(async move {
        loop {
            let Ok(next) = sandbox.next().await else {
                return;
            };
            let id = next.event.request_id.clone();
            match handler(next.event).await {
                InProcessReply::Response(body) => {
                    if let Ok(mut w) = sandbox.begin_response(&id) {
                        if w.chunk(Bytes::from(body)).is_ok() {
                            let _ = w.finish(None);
                        }
                    }
                }
                InProcessReply::Stream(chunks) => {
                    if let Ok(mut w) = sandbox.begin_response(&id) {
                        if chunks.into_iter().all(|c| w.chunk(Bytes::from(c)).is_ok()) {
                            let _ = w.finish(None);
                        }
                    }
                }
                InProcessReply::Error { error_type, body } => {
                    let _ = sandbox.post_error(&id, error_type.as_deref(), &body);
                }
                InProcessReply::Hang => std::future::pending::<()>().await,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: &str) -> InvocationEvent {
        InvocationEvent {
            request_id: id.to_string(),
            payload: Bytes::from_static(b"{\"k\":1}"),
            invoked_function_arn: "arn:aws:lambda:us-east-1:123456789012:function:f:live".into(),
            trace_id: "Root=1-5e1b4151-5ac6c58f5a4b2e2f9c6b3e2d;Sampled=0".into(),
            client_context: Some(r#"{"custom":{"a":"b"}}"#.into()),
            cognito_identity: None,
            timeout: Duration::from_secs(3),
            max_response_bytes: Some(16),
        }
    }

    fn request(host: &str, method: Method, path: &str, body: Body) -> axum::extract::Request {
        http::Request::builder()
            .method(method)
            .uri(path)
            .header(http::header::HOST, host)
            .body(body)
            .unwrap()
    }

    async fn body_of(resp: Response) -> (StatusCode, HeaderMap, Vec<u8>) {
        let status = resp.status();
        let headers = resp.headers().clone();
        let body = resp
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec();
        (status, headers, body)
    }

    fn signals(rx: &mut mpsc::UnboundedReceiver<RuntimeSignal>) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(s) = rx.try_recv() {
            out.push(match s {
                RuntimeSignal::Delivered { .. } => "delivered".to_string(),
                RuntimeSignal::Chunk(c) => format!("chunk:{}", String::from_utf8_lossy(&c)),
                RuntimeSignal::Done(InvocationOutcome::Error(p)) => {
                    format!("done:Error({})", String::from_utf8_lossy(&p))
                }
                RuntimeSignal::Done(InvocationOutcome::InitError(p)) => {
                    format!("done:InitError({})", String::from_utf8_lossy(&p))
                }
                RuntimeSignal::Done(o) => format!("done:{o:?}"),
            });
        }
        out
    }

    #[test]
    fn runtime_hosts_are_a_single_dns_label() {
        let api = RuntimeApi::new();
        let host = api.register().host();
        assert!(host.len() <= 63, "{host}");
        assert!(host
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'));
    }

    #[test]
    fn runtime_hosts_are_recognised_with_or_without_a_port() {
        assert_eq!(token_from_host("fakecloud-runtime-ab12:4566"), Some("ab12"));
        assert_eq!(token_from_host("fakecloud-runtime-ab12"), Some("ab12"));
        assert_eq!(token_from_host("fakecloud-runtime-:4566"), None);
        assert_eq!(token_from_host("fakecloud-runtime-a.b:1"), None);
        assert_eq!(token_from_host("localhost:4566"), None);
        assert_eq!(token_from_host("host.docker.internal:4566"), None);
    }

    /// `/next` hands over the event with Lambda's headers; `/response`
    /// relays the body and completes the invocation with 202.
    #[tokio::test]
    async fn next_then_response_over_http() {
        let api = RuntimeApi::new();
        let sandbox = api.register();
        let host = format!("{}:4566", sandbox.host());
        let mut dispatched = sandbox.dispatch(event("req-1"));
        assert!(!dispatched.warm);

        let (status, headers, body) = body_of(
            api.serve(request(
                &host,
                Method::GET,
                "/2018-06-01/runtime/invocation/next",
                Body::empty(),
            ))
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, b"{\"k\":1}");
        let h = |n: &str| headers.get(n).unwrap().to_str().unwrap().to_string();
        assert_eq!(h("lambda-runtime-aws-request-id"), "req-1");
        assert_eq!(
            h("lambda-runtime-invoked-function-arn"),
            "arn:aws:lambda:us-east-1:123456789012:function:f:live"
        );
        assert_eq!(
            h("lambda-runtime-trace-id"),
            "Root=1-5e1b4151-5ac6c58f5a4b2e2f9c6b3e2d;Sampled=0"
        );
        assert_eq!(
            h("lambda-runtime-client-context"),
            r#"{"custom":{"a":"b"}}"#
        );
        assert!(headers.get("lambda-runtime-cognito-identity").is_none());
        let deadline: u128 = h("lambda-runtime-deadline-ms").parse().unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        assert!(
            deadline > now + 2_000 && deadline <= now + 3_000,
            "{deadline} vs {now}"
        );
        assert!(sandbox.is_initialized());

        let (status, _, body) = body_of(
            api.serve(request(
                &host,
                Method::POST,
                "/2018-06-01/runtime/invocation/req-1/response",
                Body::from(r#"{"errorType":"X","errorMessage":"y"}"#),
            ))
            .await,
        )
        .await;
        // Larger than this event's 16-byte cap: refused, invocation over.
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap()["errorType"],
            "RequestEntityTooLarge"
        );
        assert_eq!(
            signals(&mut dispatched.signals),
            vec!["delivered".to_string(), "done:ResponseTooLarge".to_string()]
        );

        // The next event, answered within the cap.
        let mut dispatched = sandbox.dispatch(event("req-2"));
        assert!(dispatched.warm);
        let next = sandbox.next().await.unwrap();
        assert_eq!(next.event.request_id, "req-2");
        let (status, _, body) = body_of(
            api.serve(request(
                &host,
                Method::POST,
                "/2018-06-01/runtime/invocation/req-2/response",
                Body::from(r#"{"ok":1}"#),
            ))
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert_eq!(body, br#"{"status":"OK"}"#);
        assert_eq!(
            signals(&mut dispatched.signals),
            vec![
                "delivered".to_string(),
                r#"chunk:{"ok":1}"#.to_string(),
                "done:Response".to_string()
            ]
        );
    }

    /// `/error` ends the invocation with the runtime's error object; a
    /// request ID that is not in flight is refused.
    #[tokio::test]
    async fn error_and_unknown_request_ids() {
        let api = RuntimeApi::new();
        let sandbox = api.register();
        let host = sandbox.host();
        let mut dispatched = sandbox.dispatch(event("req-e"));
        sandbox.next().await.unwrap();

        let unknown = body_of(
            api.serve(request(
                &host,
                Method::POST,
                "/2018-06-01/runtime/invocation/other/response",
                Body::from("{}"),
            ))
            .await,
        )
        .await;
        assert_eq!(unknown.0, StatusCode::BAD_REQUEST);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&unknown.2).unwrap(),
            serde_json::json!({"errorMessage": "Invalid request ID", "errorType": "InvalidRequestID"})
        );

        let err = br#"{"errorMessage":"boom","errorType":"ValueError","stackTrace":[]}"#;
        let resp = http::Request::builder()
            .method(Method::POST)
            .uri("/2018-06-01/runtime/invocation/req-e/error")
            .header(http::header::HOST, &host)
            .header("Lambda-Runtime-Function-Error-Type", "ValueError")
            .body(Body::from(&err[..]))
            .unwrap();
        assert_eq!(api.serve(resp).await.status(), StatusCode::ACCEPTED);
        assert_eq!(
            signals(&mut dispatched.signals),
            vec![
                "delivered".to_string(),
                format!("done:Error({})", String::from_utf8_lossy(err))
            ]
        );
        // Answered: a second answer for it is an unknown ID.
        assert_eq!(
            sandbox.post_error("req-e", None, b"{}"),
            Err(ApiError::InvalidRequestId)
        );
        // An empty error body is built from the type header.
        let mut dispatched = sandbox.dispatch(event("req-f"));
        sandbox.next().await.unwrap();
        sandbox
            .post_error("req-f", Some("Custom.Error"), b"")
            .unwrap();
        let last = signals(&mut dispatched.signals).pop().unwrap();
        assert!(last.contains("Custom.Error"), "{last}");
    }

    /// `/init/error` fails the queued event and every later one with the
    /// runtime's error; it is refused once init is over.
    #[tokio::test]
    async fn init_error_fails_queued_and_later_events() {
        let api = RuntimeApi::new();
        let sandbox = api.register();
        let mut queued = sandbox.dispatch(event("req-i"));
        let body = br#"{"errorMessage":"Unable to import module 'index'","errorType":"Runtime.ImportModuleError"}"#;
        let resp = http::Request::builder()
            .method(Method::POST)
            .uri("/2018-06-01/runtime/init/error")
            .header(http::header::HOST, sandbox.host())
            .header(
                "Lambda-Runtime-Function-Error-Type",
                "Runtime.ImportModuleError",
            )
            .body(Body::from(&body[..]))
            .unwrap();
        assert_eq!(api.serve(resp).await.status(), StatusCode::ACCEPTED);
        let expected = format!("done:InitError({})", String::from_utf8_lossy(body));
        assert_eq!(signals(&mut queued.signals), vec![expected.clone()]);
        let mut later = sandbox.dispatch(event("req-j"));
        assert_eq!(signals(&mut later.signals), vec![expected]);
        assert!(matches!(
            sandbox.next().await,
            Err(ApiError::InvalidStateTransition(_))
        ));

        let ready = api.register();
        let _d = ready.dispatch(event("req-k"));
        ready.next().await.unwrap();
        assert!(matches!(
            ready.post_init_error(None, b"{}"),
            Err(ApiError::InvalidStateTransition(_))
        ));
    }

    /// A streamed response keeps its pieces, and its error trailers turn it
    /// into a function error.
    #[tokio::test]
    async fn streamed_response_with_error_trailers() {
        let api = RuntimeApi::new();
        let sandbox = api.register();
        let mut ev = event("req-s");
        ev.max_response_bytes = None;
        let mut dispatched = sandbox.dispatch(ev);
        sandbox.next().await.unwrap();

        let (tx, body) = {
            let (tx, rx) = mpsc::channel::<Result<http_body::Frame<Bytes>, std::io::Error>>(4);
            (
                tx,
                Body::new(http_body_util::StreamBody::new(tokio_stream_from(rx))),
            )
        };
        let req = http::Request::builder()
            .method(Method::POST)
            .uri("/2018-06-01/runtime/invocation/req-s/response")
            .header(http::header::HOST, sandbox.host())
            .header("Lambda-Runtime-Function-Response-Mode", "streaming")
            .body(body)
            .unwrap();
        let serve = tokio::spawn({
            let api = api.clone();
            async move { api.serve(req).await.status() }
        });
        tx.send(Ok(http_body::Frame::data(Bytes::from_static(b"one|"))))
            .await
            .unwrap();
        tx.send(Ok(http_body::Frame::data(Bytes::from_static(b"two"))))
            .await
            .unwrap();
        let mut trailers = HeaderMap::new();
        trailers.insert(
            "lambda-runtime-function-error-type",
            HeaderValue::from_static("Error"),
        );
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(br#"{"errorType":"Error","errorMessage":"mid-stream"}"#);
        trailers.insert(
            "lambda-runtime-function-error-body",
            HeaderValue::from_str(&encoded).unwrap(),
        );
        tx.send(Ok(http_body::Frame::trailers(trailers)))
            .await
            .unwrap();
        drop(tx);
        assert_eq!(serve.await.unwrap(), StatusCode::ACCEPTED);
        assert_eq!(
            signals(&mut dispatched.signals),
            vec![
                "delivered".to_string(),
                "chunk:one|".to_string(),
                "chunk:two".to_string(),
                r#"done:Error({"errorType":"Error","errorMessage":"mid-stream"})"#.to_string(),
            ]
        );
    }

    fn tokio_stream_from<T: Send + 'static>(
        mut rx: mpsc::Receiver<T>,
    ) -> impl futures_util::Stream<Item = T> + Send {
        futures_util::stream::poll_fn(move |cx| rx.poll_recv(cx))
    }

    /// Requests for a torn-down or unknown sandbox, or an unknown path, get
    /// nothing; a pending `/next` returns when its sandbox goes away.
    #[tokio::test]
    async fn unknown_hosts_paths_and_teardown() {
        let api = RuntimeApi::new();
        let sandbox = api.register();
        let next = tokio::spawn({
            let sandbox = sandbox.clone();
            async move { sandbox.next().await.map(|_| ()) }
        });
        tokio::task::yield_now().await;
        api.unregister(&sandbox);
        assert_eq!(next.await.unwrap(), Err(ApiError::Gone));
        assert!(api.is_empty());

        let gone = api
            .serve(request(
                &sandbox.host(),
                Method::GET,
                "/2018-06-01/runtime/invocation/next",
                Body::empty(),
            ))
            .await;
        assert_eq!(gone.status(), StatusCode::NOT_FOUND);

        let live = api.register();
        let path = api
            .serve(request(
                &live.host(),
                Method::GET,
                "/2018-06-01/runtime/nope",
                Body::empty(),
            ))
            .await;
        assert_eq!(path.status(), StatusCode::NOT_FOUND);
        let method = api
            .serve(request(
                &live.host(),
                Method::POST,
                "/2018-06-01/runtime/invocation/next",
                Body::empty(),
            ))
            .await;
        assert_eq!(method.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    /// Another sandbox's request ID is unknown here: instances only ever see
    /// and answer their own invocations.
    #[tokio::test]
    async fn sandboxes_never_cross_wires() {
        let api = RuntimeApi::new();
        let a = api.register();
        let b = api.register();
        let mut da = a.dispatch(event("for-a"));
        let _db = b.dispatch(event("for-b"));
        assert_eq!(b.next().await.unwrap().event.request_id, "for-b");
        assert_eq!(a.next().await.unwrap().event.request_id, "for-a");
        assert!(matches!(
            b.begin_response("for-a"),
            Err(ApiError::InvalidRequestId)
        ));
        assert_eq!(signals(&mut da.signals), vec!["delivered".to_string()]);
    }

    /// Extensions register over HTTP, hold the first event until they have
    /// all finished init, receive `INVOKE` with each event and hold the next
    /// until they are done with it, and get `SHUTDOWN` before teardown.
    #[tokio::test]
    async fn extensions_gate_events_and_receive_invoke_and_shutdown() {
        let api = RuntimeApi::new();
        let sandbox = api.register_for(SandboxInfo {
            function_name: "f".into(),
            function_version: "$LATEST".into(),
            handler: "index.handler".into(),
        });
        sandbox.expect_extensions(1);
        let host = sandbox.host();

        let register = http::Request::builder()
            .method(Method::POST)
            .uri("/2020-01-01/extension/register")
            .header(http::header::HOST, &host)
            .header("Lambda-Extension-Name", "ext")
            .body(Body::from(r#"{"events":["INVOKE","SHUTDOWN"]}"#))
            .unwrap();
        let (status, headers, body) = body_of(api.serve(register).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            serde_json::json!({"functionName":"f","functionVersion":"$LATEST","handler":"index.handler"})
        );
        let ext_id = headers["lambda-extension-identifier"]
            .to_str()
            .unwrap()
            .to_string();

        let mut d = sandbox.dispatch(event("req-x"));
        let runtime_next = tokio::spawn({
            let sandbox = sandbox.clone();
            async move { sandbox.next().await.map(|n| n.event.request_id) }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            signals(&mut d.signals).is_empty(),
            "the event waits for the extension's init"
        );

        let ext_next = |id: String| {
            let api = api.clone();
            let host = host.clone();
            tokio::spawn(async move {
                let req = http::Request::builder()
                    .uri("/2020-01-01/extension/event/next")
                    .header(http::header::HOST, host)
                    .header("Lambda-Extension-Identifier", id)
                    .body(Body::empty())
                    .unwrap();
                let (_, _, body) = body_of(api.serve(req).await).await;
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()
            })
        };
        let invoke = ext_next(ext_id.clone()).await.unwrap();
        assert_eq!(runtime_next.await.unwrap().unwrap(), "req-x");
        assert_eq!(invoke["eventType"], "INVOKE");
        assert_eq!(invoke["requestId"], "req-x");
        assert_eq!(
            invoke["invokedFunctionArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:f:live"
        );
        assert!(invoke["deadlineMs"].as_u64().is_some());

        // The runtime answers; the next event waits until the extension is
        // done with this one.
        sandbox
            .begin_response("req-x")
            .unwrap()
            .finish(None)
            .unwrap();
        let _d2 = sandbox.dispatch(event("req-y"));
        let second = tokio::spawn({
            let sandbox = sandbox.clone();
            async move { sandbox.next().await.map(|n| n.event.request_id) }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!second.is_finished());
        let pending = ext_next(ext_id.clone());
        assert_eq!(second.await.unwrap().unwrap(), "req-y");
        assert_eq!(pending.await.unwrap()["requestId"], "req-y");

        let shutdown = ext_next(ext_id.clone());
        tokio::time::sleep(Duration::from_millis(20)).await;
        let started = std::time::Instant::now();
        let grace = sandbox.shutdown_extensions("spindown", Duration::from_millis(300));
        let (_, event) = tokio::join!(grace, shutdown);
        let event = event.unwrap();
        assert_eq!(event["eventType"], "SHUTDOWN");
        assert_eq!(event["shutdownReason"], "spindown");
        // It never came back for more, so the grace period ran out.
        assert!(started.elapsed() >= Duration::from_millis(250));

        // Unknown identifiers and registration after init are refused.
        assert_eq!(
            sandbox.extension_next("nope").await,
            Err(ApiError::UnknownExtension)
        );
        assert!(matches!(
            sandbox.register_extension(&["INVOKE".into()]),
            Err(ApiError::InvalidStateTransition(_))
        ));
    }

    /// An extension's init error fails the invocation waiting on init.
    #[tokio::test]
    async fn extension_init_error_fails_the_environment() {
        let api = RuntimeApi::new();
        let sandbox = api.register();
        sandbox.expect_extensions(1);
        let id = sandbox.register_extension(&[]).unwrap();
        let mut d = sandbox.dispatch(event("req-z"));
        sandbox
            .extension_error(&id, Some("Extension.ConfigInvalid"), b"")
            .unwrap();
        let last = signals(&mut d.signals).pop().unwrap();
        assert!(last.starts_with("done:InitError("), "{last}");
        assert!(last.contains("Extension.ConfigInvalid"), "{last}");
    }

    /// A response whose connection drops mid-body still ends the
    /// invocation, as a failed one.
    #[test]
    fn a_dropped_response_writer_reports_truncation() {
        let api = RuntimeApi::new();
        let sandbox = api.register();
        let mut d = sandbox.dispatch(event("req-d"));
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(sandbox.next()).unwrap();
        {
            let mut w = sandbox.begin_response("req-d").unwrap();
            w.chunk(Bytes::from_static(b"par")).unwrap();
        }
        let last = signals(&mut d.signals).pop().unwrap();
        assert!(last.contains("Runtime.TruncatedResponse"), "{last}");
    }
}
