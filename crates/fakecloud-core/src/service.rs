use async_trait::async_trait;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use md5::{Digest, Md5};
use parking_lot::Mutex;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use crate::auth::Principal;

/// Streaming request body kept alongside the buffered `body: Bytes`. Set
/// by dispatch only for routes that opt into streaming (S3 PutObject /
/// UploadPart, ECR OCI blob upload PATCH/PUT). Service handlers call
/// [`AwsRequest::take_body_stream`] to consume the raw stream without
/// buffering the entire payload into memory; non-streaming services
/// keep using `req.body` (which is empty `Bytes` for streaming routes).
pub type RequestBodyStream = axum::body::Body;

/// A parsed AWS request.
pub struct AwsRequest {
    pub service: String,
    pub action: String,
    pub region: String,
    pub account_id: String,
    pub request_id: String,
    pub headers: HeaderMap,
    pub query_params: HashMap<String, String>,
    /// Buffered request body. For streaming routes this is `Bytes::new()`
    /// and the raw body is available via [`AwsRequest::take_body_stream`].
    pub body: Bytes,
    /// Raw streaming body, populated only for streaming routes. Wrapped
    /// in a Mutex so the per-service handler can `.take()` ownership
    /// behind the shared `&AwsRequest` reference threaded through the
    /// call chain.
    pub body_stream: Mutex<Option<RequestBodyStream>>,
    pub path_segments: Vec<String>,
    /// The raw URI path, before splitting into segments.
    pub raw_path: String,
    /// The raw URI query string (everything after `?`), preserving repeated keys.
    pub raw_query: String,
    pub method: Method,
    /// Whether this request came via Query (form-encoded) or JSON protocol.
    pub is_query_protocol: bool,
    /// The access key ID from the SigV4 Authorization header, if present.
    pub access_key_id: Option<String>,
    /// The resolved caller identity. `None` when the credential is unknown
    /// or the caller used the reserved root-bypass credentials. Populated
    /// by dispatch via the configured [`crate::auth::CredentialResolver`]
    /// so service handlers can make identity-based decisions (e.g.
    /// `GetCallerIdentity`, IAM enforcement) without re-parsing the
    /// Authorization header.
    pub principal: Option<Principal>,
}

impl std::fmt::Debug for AwsRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AwsRequest")
            .field("service", &self.service)
            .field("action", &self.action)
            .field("region", &self.region)
            .field("account_id", &self.account_id)
            .field("request_id", &self.request_id)
            .field("headers", &self.headers)
            .field("query_params", &self.query_params)
            .field("body_len", &self.body.len())
            .field(
                "body_stream",
                &self.body_stream.lock().as_ref().map(|_| "<stream>"),
            )
            .field("path_segments", &self.path_segments)
            .field("raw_path", &self.raw_path)
            .field("raw_query", &self.raw_query)
            .field("method", &self.method)
            .field("is_query_protocol", &self.is_query_protocol)
            .field("access_key_id", &self.access_key_id)
            .field("principal", &self.principal)
            .finish()
    }
}

impl AwsRequest {
    /// Parse the request body as JSON, returning `Value::Null` on failure.
    pub fn json_body(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or(serde_json::Value::Null)
    }

    /// Consume the streaming body if this request was dispatched as
    /// streaming. Returns `None` for buffered requests; the buffered
    /// body is available via [`AwsRequest::body`]. Calling this twice
    /// returns `None` on the second call.
    pub fn take_body_stream(&self) -> Option<RequestBodyStream> {
        self.body_stream.lock().take()
    }

    /// All values supplied for a repeated `@httpQuery` parameter, in wire
    /// order. [`AwsRequest::query_params`] is a `HashMap`, so `?a=1&a=2` keeps
    /// only `2` there; this re-parses [`AwsRequest::raw_query`] to recover every
    /// occurrence. Use it for list-style query params (filters, ids) where a
    /// client legitimately repeats the key. Returns an empty vec when the key
    /// is absent.
    pub fn query_param_all(&self, key: &str) -> Vec<String> {
        crate::protocol::form_urlencoded_pairs(&self.raw_query)
            .into_iter()
            .filter_map(|(k, v)| (k == key).then_some(v))
            .collect()
    }
}

/// Drain a streaming request body into a single [`Bytes`] buffer with no
/// upper bound. Used by handlers that legitimately need the whole payload
/// in memory (small JSON-shaped requests that happened to land on a
/// streaming route, e.g. ECR `mount` PUT with no body). Heavy uploads
/// (S3 PutObject / UploadPart, ECR blob PATCH/PUT) take the streaming
/// spool path via [`spool_request_stream`] instead. The dispatch-level
/// cap (`FAKECLOUD_MAX_REQUEST_BODY_BYTES`) does not apply to streaming
/// routes; this helper exists so a service handler that knows the
/// payload is small can buffer without dragging in `axum` itself.
pub async fn drain_request_stream(stream: RequestBodyStream) -> Result<Bytes, AwsServiceError> {
    use http_body_util::BodyExt;
    match stream.collect().await {
        Ok(c) => Ok(c.to_bytes()),
        Err(e) => Err(stream_error_to_aws(&e.to_string())),
    }
}

fn stream_error_to_aws(msg: &str) -> AwsServiceError {
    // Hyper / axum surface `body limit exceeded` with a
    // payload-too-large variant. Everything else (connection
    // reset, malformed chunked encoding, premature EOF) maps
    // to a 400 BadRequest so callers can distinguish.
    let too_large = msg.to_ascii_lowercase().contains("limit");
    let (status, code, message) = if too_large {
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            "RequestEntityTooLarge",
            "Streaming request body exceeded the configured limit",
        )
    } else {
        (
            StatusCode::BAD_REQUEST,
            "MalformedRequestBody",
            "Failed to read streaming request body",
        )
    };
    AwsServiceError::aws_error(status, code, message)
}

/// Outcome of spooling a streaming request body to disk: the path of the
/// freshly created tempfile, the total byte count, and the MD5 hash of
/// the bytes (lowercase hex, the form S3 uses for `ETag`).
///
/// The caller owns the file and is responsible for either consuming it
/// (passing the [`PathBuf`] into a `BodySource::File` handed to a store)
/// or unlinking it. Returning the file path instead of a handle lets the
/// downstream store rename the file directly, which is the whole point —
/// in disk-mode S3 a 1 GiB upload performs zero in-RAM copies of the
/// payload.
#[derive(Debug)]
pub struct SpooledBody {
    pub path: PathBuf,
    pub size: u64,
    pub md5_hex: String,
    /// Lowercase-hex SHA-256 of the decoded payload, computed in the same
    /// single streaming pass as the MD5. Lets the S3 layer verify a client's
    /// `x-amz-content-sha256` header against the bytes actually received
    /// (returning `XAmzContentSHA256Mismatch` on divergence) for plain,
    /// non-`aws-chunked` uploads where the header carries the real payload
    /// hash rather than a `STREAMING-…`/`UNSIGNED-PAYLOAD` marker.
    pub sha256_hex: String,
}

/// Incremental decoder for the `aws-chunked` content-encoding that modern AWS
/// S3 clients (aws-cli, boto3 >= 1.36, aws-crt) apply by default to PutObject /
/// UploadPart bodies when they send `x-amz-content-sha256:
/// STREAMING-AWS4-HMAC-SHA256-PAYLOAD` (or `STREAMING-UNSIGNED-PAYLOAD-TRAILER`).
///
/// The wire format wraps the real payload in application-layer frames:
/// `<hex-size>[;chunk-signature=<hex>]\r\n<data>\r\n` repeated, terminated by a
/// `0`-size chunk, then optional `x-amz-trailer` lines, then a final `\r\n`.
/// hyper only strips HTTP `Transfer-Encoding: chunked`, NOT this
/// `Content-Encoding: aws-chunked` framing — so without decoding, the size /
/// signature lines and trailers get stored as the object's bytes (silent
/// corruption + a wrong ETag). This fed-incrementally because network frames do
/// not align to chunk boundaries; trailer checksums are consumed but not
/// re-validated (fakecloud computes its own checksums over the decoded bytes).
#[derive(Default)]
pub struct AwsChunkedDecoder {
    state: ChunkState,
    line: Vec<u8>,
    remaining: usize,
    done: bool,
}

#[derive(Default, PartialEq)]
enum ChunkState {
    #[default]
    Header,
    Data,
    AfterData,
    Trailer,
}

/// A malformed `aws-chunked` chunk-size line (non-hex length).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MalformedChunk;

impl AwsChunkedDecoder {
    /// Feed a network frame; returns the decoded payload bytes it yielded.
    /// Errors only on a malformed chunk-size line.
    pub fn feed(&mut self, input: &[u8]) -> Result<Vec<u8>, MalformedChunk> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < input.len() && !self.done {
            match self.state {
                ChunkState::Data => {
                    let take = self.remaining.min(input.len() - i);
                    out.extend_from_slice(&input[i..i + take]);
                    i += take;
                    self.remaining -= take;
                    if self.remaining == 0 {
                        self.state = ChunkState::AfterData;
                    }
                }
                ChunkState::AfterData => {
                    // Consume the inter-chunk CRLF; next byte after \n is a header.
                    while i < input.len() {
                        let b = input[i];
                        i += 1;
                        if b == b'\n' {
                            self.state = ChunkState::Header;
                            break;
                        }
                    }
                }
                ChunkState::Header | ChunkState::Trailer => {
                    let is_header = self.state == ChunkState::Header;
                    while i < input.len() {
                        let b = input[i];
                        i += 1;
                        if b == b'\n' {
                            let line = std::mem::take(&mut self.line);
                            if is_header {
                                // size is the hex up to a `;` extension or EOL.
                                let hex_part: &[u8] =
                                    line.split(|&c| c == b';').next().unwrap_or(&[]);
                                let hex = std::str::from_utf8(hex_part)
                                    .map_err(|_| MalformedChunk)?
                                    .trim();
                                let size =
                                    usize::from_str_radix(hex, 16).map_err(|_| MalformedChunk)?;
                                if size == 0 {
                                    self.state = ChunkState::Trailer;
                                } else {
                                    self.remaining = size;
                                    self.state = ChunkState::Data;
                                }
                            } else if line.is_empty() {
                                // Blank line ends the trailer section.
                                self.done = true;
                            }
                            // (a non-empty trailer line is consumed and ignored)
                            break;
                        } else if b != b'\r' {
                            self.line.push(b);
                        }
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Whether a request body carries the `aws-chunked` content-encoding (so the
/// spool path must decode the framing). True when `Content-Encoding` lists
/// `aws-chunked`, or `x-amz-content-sha256` is a `STREAMING-…` marker — both of
/// which default modern S3 clients (aws-cli, boto3 >= 1.36, aws-crt) set.
pub fn is_aws_chunked(headers: &http::HeaderMap) -> bool {
    headers
        .get("content-encoding")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|t| t.trim().eq_ignore_ascii_case("aws-chunked"))
        })
        || headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("STREAMING-"))
}

/// Strip the `aws-chunked` token from a client `Content-Encoding` so the stored
/// object metadata reflects what AWS keeps (it consumes `aws-chunked` as a
/// transfer detail; any remaining real encoding such as `gzip` is preserved).
/// Returns `None` when nothing meaningful remains.
pub fn strip_aws_chunked_encoding(content_encoding: Option<&str>) -> Option<String> {
    let ce = content_encoding?;
    let kept: Vec<&str> = ce
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty() && !t.eq_ignore_ascii_case("aws-chunked"))
        .collect();
    if kept.is_empty() {
        None
    } else {
        Some(kept.join(", "))
    }
}

/// Stream a request body to a tempfile on disk while computing its MD5
/// and length on the fly. The body is **never** materialized into a
/// single `Bytes` buffer; chunks flow from hyper -> Tokio file in
/// constant memory. A 1 GiB PutObject moves through this function with
/// peak resident memory bounded by hyper's per-frame buffer.
///
/// `dir` controls where the tempfile lands. S3 callers point this at
/// the S3 object root so the eventual rename into the final storage
/// path stays on the same filesystem and is a metadata-only move.
/// Memory-mode callers can pass `None` for the system temp dir; the
/// memory store reads the file back into bytes and unlinks it.
///
/// `aws_chunked` decodes the `Content-Encoding: aws-chunked` application-layer
/// framing that default modern S3 clients apply (see [`AwsChunkedDecoder`]), so
/// the spooled bytes, MD5/ETag, and size reflect the real payload — not the
/// chunk-size/signature framing. Non-S3 callers (and raw `UNSIGNED-PAYLOAD`
/// uploads) pass `false` and stream verbatim.
pub async fn spool_request_stream(
    stream: RequestBodyStream,
    dir: Option<&std::path::Path>,
    aws_chunked: bool,
) -> Result<SpooledBody, AwsServiceError> {
    use http_body_util::BodyExt;
    use tokio::io::AsyncWriteExt;

    let dir = dir.map(|d| d.to_path_buf());
    if let Some(d) = dir.as_ref() {
        // Best-effort create; an existing dir is fine.
        let _ = tokio::fs::create_dir_all(d).await;
    }

    let mut builder = tempfile::Builder::new();
    builder.prefix("fc-spool-");
    let named = match dir.as_ref() {
        Some(d) => builder.tempfile_in(d),
        None => builder.tempfile(),
    }
    .map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("failed to create spool tempfile: {e}"),
        )
    })?;

    // `into_temp_path` would auto-delete on drop. We keep the path and
    // assume responsibility for either persisting or unlinking it.
    let (std_file, temp_path) = named.into_parts();
    // Persist to a stable PathBuf — `keep()` releases the
    // delete-on-drop guard so the file outlives this function.
    let path: PathBuf = temp_path.keep().map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("failed to persist spool tempfile: {e}"),
        )
    })?;

    let mut file = tokio::fs::File::from_std(std_file);
    let mut hasher = Md5::new();
    let mut sha = sha2::Sha256::new();
    let mut size: u64 = 0;
    let mut body = stream;
    let mut decoder = aws_chunked.then(AwsChunkedDecoder::default);

    // Cleanup helper: drop the file handle before unlinking so
    // platforms that disallow removing an open file (Windows) still
    // collect the partial spool. `drop(file)` closes the underlying
    // OS handle synchronously.
    async fn cleanup(file: tokio::fs::File, path: &std::path::Path) {
        drop(file);
        let _ = tokio::fs::remove_file(path).await;
    }

    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(raw) = frame.into_data() {
                    if !raw.is_empty() {
                        // Decode aws-chunked framing into the real payload when
                        // the client used it; otherwise the frame IS the payload.
                        let payload = match decoder.as_mut() {
                            Some(d) => match d.feed(&raw) {
                                Ok(decoded) => decoded,
                                Err(_) => {
                                    cleanup(file, &path).await;
                                    return Err(AwsServiceError::aws_error(
                                        StatusCode::BAD_REQUEST,
                                        "InvalidChunkSizeError",
                                        "Malformed aws-chunked request body",
                                    ));
                                }
                            },
                            None => raw.to_vec(),
                        };
                        if !payload.is_empty() {
                            hasher.update(&payload);
                            sha.update(&payload);
                            size += payload.len() as u64;
                            if let Err(e) = file.write_all(&payload).await {
                                cleanup(file, &path).await;
                                return Err(AwsServiceError::aws_error(
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "InternalError",
                                    format!("failed to spool request body: {e}"),
                                ));
                            }
                        }
                    }
                }
                // HTTP trailers are ignored; aws-chunked trailers are consumed
                // inside the decoder.
            }
            Some(Err(e)) => {
                cleanup(file, &path).await;
                return Err(stream_error_to_aws(&e.to_string()));
            }
            None => break,
        }
    }

    if let Err(e) = file.flush().await {
        cleanup(file, &path).await;
        return Err(AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("failed to flush spool tempfile: {e}"),
        ));
    }
    drop(file);

    let md5_hex = hex_lower(&hasher.finalize());
    let sha256_hex = hex_lower(&sha.finalize());
    Ok(SpooledBody {
        path,
        size,
        md5_hex,
        sha256_hex,
    })
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// A response body. Most handlers return [`ResponseBody::Bytes`] built from
/// an in-memory [`Bytes`] buffer; the [`File`](ResponseBody::File) variant
/// exists so large disk-backed objects can be streamed straight from the
/// filesystem to the HTTP body without being materialized into RAM. The file
/// handle is opened by the service handler while it still holds the
/// per-bucket read guard, so the reader sees a consistent inode even if a
/// concurrent PUT/DELETE renames or unlinks the path before dispatch streams
/// the body.
#[derive(Debug)]
pub enum ResponseBody {
    Bytes(Bytes),
    File { file: tokio::fs::File, size: u64 },
}

impl ResponseBody {
    pub fn len(&self) -> u64 {
        match self {
            ResponseBody::Bytes(b) => b.len() as u64,
            ResponseBody::File { size, .. } => *size,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Accessor that returns the bytes of a `Bytes` variant and panics for
    /// `File`. Used by tests and by callers that know the response was built
    /// from an in-memory buffer (JSON handlers, cross-service glue).
    pub fn expect_bytes(&self) -> &[u8] {
        match self {
            ResponseBody::Bytes(b) => b,
            ResponseBody::File { .. } => {
                panic!("expect_bytes called on ResponseBody::File")
            }
        }
    }
}

impl Default for ResponseBody {
    fn default() -> Self {
        ResponseBody::Bytes(Bytes::new())
    }
}

impl From<Bytes> for ResponseBody {
    fn from(b: Bytes) -> Self {
        ResponseBody::Bytes(b)
    }
}

impl From<Vec<u8>> for ResponseBody {
    fn from(v: Vec<u8>) -> Self {
        ResponseBody::Bytes(Bytes::from(v))
    }
}

impl From<&'static [u8]> for ResponseBody {
    fn from(s: &'static [u8]) -> Self {
        ResponseBody::Bytes(Bytes::from_static(s))
    }
}

impl From<String> for ResponseBody {
    fn from(s: String) -> Self {
        ResponseBody::Bytes(Bytes::from(s))
    }
}

impl From<&'static str> for ResponseBody {
    fn from(s: &'static str) -> Self {
        ResponseBody::Bytes(Bytes::from_static(s.as_bytes()))
    }
}

impl PartialEq<Bytes> for ResponseBody {
    fn eq(&self, other: &Bytes) -> bool {
        match self {
            ResponseBody::Bytes(b) => b == other,
            ResponseBody::File { .. } => false,
        }
    }
}

/// A response from a service handler.
pub struct AwsResponse {
    pub status: StatusCode,
    pub content_type: String,
    pub body: ResponseBody,
    pub headers: HeaderMap,
}

impl AwsResponse {
    pub fn xml(status: StatusCode, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            content_type: "text/xml".to_string(),
            body: ResponseBody::Bytes(body.into()),
            headers: HeaderMap::new(),
        }
    }

    pub fn json(status: StatusCode, body: impl Into<Bytes>) -> Self {
        Self {
            status,
            content_type: "application/x-amz-json-1.1".to_string(),
            body: ResponseBody::Bytes(body.into()),
            headers: HeaderMap::new(),
        }
    }

    /// Build a JSON response from a `serde_json::Value` with an explicit status.
    ///
    /// Serialization of an in-memory `Value` cannot fail — it has no cycles and
    /// no custom serializers — so the inner `to_vec` is documented as infallible
    /// rather than left as a bare `unwrap()`.
    pub fn json_value(status: StatusCode, value: serde_json::Value) -> Self {
        Self::json(
            status,
            serde_json::to_vec(&value).expect("serde_json::Value serialization is infallible"),
        )
    }

    /// Convenience constructor for a 200 OK JSON response from a `serde_json::Value`.
    pub fn ok_json(value: serde_json::Value) -> Self {
        Self::json_value(StatusCode::OK, value)
    }
}

/// Error returned by service handlers.
#[derive(Debug, thiserror::Error)]
pub enum AwsServiceError {
    #[error("service not found: {service}")]
    ServiceNotFound { service: String },

    #[error("action {action} not implemented for service {service}")]
    ActionNotImplemented { service: String, action: String },

    #[error("{code}: {message}")]
    AwsError {
        status: StatusCode,
        code: String,
        message: String,
        /// Additional key-value pairs to include in the error XML (e.g., BucketName, Key, Condition).
        extra_fields: Vec<(String, String)>,
        /// Additional HTTP headers to include in the error response.
        headers: Vec<(String, String)>,
    },
}

impl AwsServiceError {
    pub fn action_not_implemented(service: &str, action: &str) -> Self {
        Self::ActionNotImplemented {
            service: service.to_string(),
            action: action.to_string(),
        }
    }

    pub fn aws_error(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self::AwsError {
            status,
            code: code.into(),
            message: message.into(),
            extra_fields: Vec::new(),
            headers: Vec::new(),
        }
    }

    pub fn aws_error_with_fields(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
        extra_fields: Vec<(String, String)>,
    ) -> Self {
        Self::AwsError {
            status,
            code: code.into(),
            message: message.into(),
            extra_fields,
            headers: Vec::new(),
        }
    }

    pub fn aws_error_with_headers(
        status: StatusCode,
        code: impl Into<String>,
        message: impl Into<String>,
        headers: Vec<(String, String)>,
    ) -> Self {
        Self::AwsError {
            status,
            code: code.into(),
            message: message.into(),
            extra_fields: Vec::new(),
            headers,
        }
    }

    pub fn extra_fields(&self) -> &[(String, String)] {
        match self {
            Self::AwsError { extra_fields, .. } => extra_fields,
            _ => &[],
        }
    }

    pub fn status(&self) -> StatusCode {
        match self {
            Self::ServiceNotFound { .. } => StatusCode::BAD_REQUEST,
            Self::ActionNotImplemented { .. } => StatusCode::NOT_IMPLEMENTED,
            Self::AwsError { status, .. } => *status,
        }
    }

    pub fn code(&self) -> &str {
        match self {
            Self::ServiceNotFound { .. } => "UnknownService",
            Self::ActionNotImplemented { .. } => "InvalidAction",
            Self::AwsError { code, .. } => code,
        }
    }

    pub fn message(&self) -> String {
        match self {
            Self::ServiceNotFound { service } => format!("service not found: {service}"),
            Self::ActionNotImplemented { service, action } => {
                format!("action {action} not implemented for service {service}")
            }
            Self::AwsError { message, .. } => message.clone(),
        }
    }

    pub fn response_headers(&self) -> &[(String, String)] {
        match self {
            Self::AwsError { headers, .. } => headers,
            _ => &[],
        }
    }
}

/// Trait that every AWS service implements.
#[async_trait]
pub trait AwsService: Send + Sync {
    /// The AWS service identifier (e.g., "sqs", "sns", "sts", "events", "ssm").
    fn service_name(&self) -> &str;

    /// Handle an incoming request.
    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError>;

    /// List of actions this service supports (for introspection).
    fn supported_actions(&self) -> &[&str];

    /// Whether this service participates in opt-in IAM enforcement
    /// (`FAKECLOUD_IAM=soft|strict`).
    ///
    /// Defaults to `false`: unless a service has a full
    /// `iam_action_for` implementation covering every operation it
    /// supports plus resource-ARN extractors, it's silently skipped when
    /// IAM enforcement is on. The startup log enumerates which services
    /// are enforced and which are not so users always know the current
    /// enforcement surface.
    ///
    /// Phase 1 contract: a service that returns `true` here MUST also
    /// provide a fully populated [`AwsService::iam_action_for`]
    /// implementation covering every action it advertises. Returning
    /// `true` without the action mapping is a programming bug.
    fn iam_enforceable(&self) -> bool {
        false
    }

    /// Derive the IAM action + resource ARN for an incoming request.
    ///
    /// Only called when [`AwsService::iam_enforceable`] returns `true`
    /// and IAM enforcement is enabled. Services must map every action
    /// they implement; returning `None` for a covered action causes the
    /// evaluator to skip the request and flag it via the
    /// `fakecloud::iam::audit` tracing target so gaps are visible in
    /// soft mode.
    ///
    /// The `IamAction.resource` is built from `request.principal`'s
    /// account id (not global config) so multi-account isolation
    /// (#381) works once per-account state partitioning lands.
    fn iam_action_for(&self, _request: &AwsRequest) -> Option<crate::auth::IamAction> {
        None
    }

    /// Derive service-specific IAM condition keys for an incoming request.
    ///
    /// Called right after [`AwsService::iam_action_for`] when IAM
    /// enforcement is enabled. The returned map is merged into the
    /// [`crate::auth::ConditionContext::service_keys`] before the
    /// evaluator runs, so policies can reference keys like `s3:prefix`
    /// or `sns:Protocol` the same way they reference global keys.
    ///
    /// Keys MUST be in the full `"service:key"` form, lowercased
    /// (e.g. `"s3:prefix"`), matching the case-insensitive lookup in
    /// [`crate::auth::ConditionContext::lookup`]. Extractors should
    /// only emit keys they can populate with confidence; anything
    /// ambiguous or unimplemented should be skipped with a
    /// `tracing::debug!(target: "fakecloud::iam::audit", ...)` so
    /// condition evaluation safe-fails to "doesn't apply" rather than
    /// "matches".
    ///
    /// Default impl returns an empty map: services that haven't been
    /// plumbed yet behave exactly as before.
    fn iam_condition_keys_for(
        &self,
        _request: &AwsRequest,
        _action: &crate::auth::IamAction,
    ) -> BTreeMap<String, Vec<String>> {
        BTreeMap::new()
    }

    /// Return the tags on the resource identified by `resource_arn`.
    ///
    /// Called at dispatch time when IAM enforcement is enabled, right
    /// after [`AwsService::iam_action_for`]. The returned map populates
    /// `aws:ResourceTag/<key>` condition keys so policies can gate
    /// access based on the target resource's tags.
    ///
    /// Return `None` to signal that this service does not (yet) support
    /// resource-tag ABAC — dispatch will emit a debug audit log and
    /// skip `aws:ResourceTag/*` evaluation. Return `Some(empty map)`
    /// when the resource exists but has no tags.
    fn resource_tags_for(
        &self,
        _resource_arn: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        None
    }

    /// Extract tags being sent in the request (e.g. on CreateQueue,
    /// PutObject with `x-amz-tagging`, TagResource).
    ///
    /// The returned map populates `aws:RequestTag/<key>` and
    /// `aws:TagKeys` condition keys. Return `None` when the service
    /// does not (yet) support request-tag extraction — dispatch skips
    /// `aws:RequestTag/*` / `aws:TagKeys` evaluation with a debug log.
    /// Return `Some(empty map)` when the request legitimately carries
    /// no tags.
    fn request_tags_from(
        &self,
        _request: &AwsRequest,
        _action: &str,
    ) -> Option<std::collections::HashMap<String, String>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::IamAction;
    use async_trait::async_trait;

    /// Build a signed aws-chunked body for `payload`, split into chunks of
    /// `chunk_size`, terminated by a 0-chunk + a trailer + final CRLF.
    fn aws_chunked_body(payload: &[u8], chunk_size: usize, with_trailer: bool) -> Vec<u8> {
        let sig = "0".repeat(64);
        let mut out = Vec::new();
        for c in payload.chunks(chunk_size.max(1)) {
            out.extend_from_slice(format!("{:x};chunk-signature={sig}\r\n", c.len()).as_bytes());
            out.extend_from_slice(c);
            out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(format!("0;chunk-signature={sig}\r\n").as_bytes());
        if with_trailer {
            out.extend_from_slice(b"x-amz-checksum-crc32:AAAAAA==\r\n");
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    fn decode_all(body: &[u8], feed_size: usize) -> Vec<u8> {
        let mut d = AwsChunkedDecoder::default();
        let mut out = Vec::new();
        for frame in body.chunks(feed_size.max(1)) {
            out.extend(d.feed(frame).expect("valid chunked body"));
        }
        out
    }

    #[test]
    fn aws_chunked_decoder_roundtrips_across_frame_boundaries() {
        let payload: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        // Chunked with 1 KiB data chunks; with and without a trailer.
        for with_trailer in [false, true] {
            let body = aws_chunked_body(&payload, 1024, with_trailer);
            // Network frames don't align to chunk boundaries: try several sizes,
            // including 1 byte at a time and a single whole-body frame.
            for feed in [1usize, 7, 64, 1000, body.len()] {
                let decoded = decode_all(&body, feed);
                assert_eq!(decoded, payload, "feed={feed} trailer={with_trailer}");
            }
        }
    }

    #[test]
    fn aws_chunked_decoder_handles_empty_payload() {
        let body = aws_chunked_body(b"", 1024, false);
        assert_eq!(decode_all(&body, 3), Vec::<u8>::new());
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut h = sha2::Sha256::new();
        h.update(bytes);
        hex_lower(&h.finalize())
    }

    #[tokio::test]
    async fn spool_computes_sha256_over_plain_payload() {
        let payload = b"hello world".to_vec();
        let spooled = spool_request_stream(axum::body::Body::from(payload.clone()), None, false)
            .await
            .expect("spool ok");
        assert_eq!(spooled.size, payload.len() as u64);
        assert_eq!(spooled.sha256_hex, sha256_hex(&payload));
        let _ = std::fs::remove_file(&spooled.path);
    }

    #[tokio::test]
    async fn spool_sha256_is_over_decoded_aws_chunked_payload() {
        // The header the client sends over aws-chunked framing is a STREAMING
        // marker, but the spool's sha256 must still describe the DECODED bytes
        // (so the S3 layer never compares against the framed wire form).
        let payload: Vec<u8> = (0..9000u32).map(|i| (i % 251) as u8).collect();
        let body = aws_chunked_body(&payload, 1024, true);
        let spooled = spool_request_stream(axum::body::Body::from(body), None, true)
            .await
            .expect("spool ok");
        assert_eq!(spooled.size, payload.len() as u64);
        assert_eq!(spooled.sha256_hex, sha256_hex(&payload));
        let _ = std::fs::remove_file(&spooled.path);
    }

    #[test]
    fn aws_chunked_decoder_rejects_bad_size_line() {
        let mut d = AwsChunkedDecoder::default();
        assert!(d.feed(b"zz;chunk-signature=x\r\n").is_err());
    }

    #[test]
    fn is_aws_chunked_detects_streaming_markers() {
        let mut h = http::HeaderMap::new();
        assert!(!is_aws_chunked(&h));
        h.insert("content-encoding", "aws-chunked".parse().unwrap());
        assert!(is_aws_chunked(&h));
        let mut h2 = http::HeaderMap::new();
        h2.insert(
            "x-amz-content-sha256",
            "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".parse().unwrap(),
        );
        assert!(is_aws_chunked(&h2));
        // gzip without aws-chunked must NOT trigger decoding.
        let mut h3 = http::HeaderMap::new();
        h3.insert("content-encoding", "gzip".parse().unwrap());
        assert!(!is_aws_chunked(&h3));
    }

    #[test]
    fn strip_aws_chunked_keeps_real_encoding() {
        assert_eq!(strip_aws_chunked_encoding(Some("aws-chunked")), None);
        assert_eq!(
            strip_aws_chunked_encoding(Some("aws-chunked, gzip")).as_deref(),
            Some("gzip")
        );
        assert_eq!(
            strip_aws_chunked_encoding(Some("gzip")).as_deref(),
            Some("gzip")
        );
        assert_eq!(strip_aws_chunked_encoding(None), None);
    }

    struct DefaultService;

    #[async_trait]
    impl AwsService for DefaultService {
        fn service_name(&self) -> &str {
            "default"
        }
        async fn handle(&self, _request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
            unreachable!()
        }
        fn supported_actions(&self) -> &[&str] {
            &[]
        }
    }

    struct PopulatedService;

    #[async_trait]
    impl AwsService for PopulatedService {
        fn service_name(&self) -> &str {
            "populated"
        }
        async fn handle(&self, _request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
            unreachable!()
        }
        fn supported_actions(&self) -> &[&str] {
            &[]
        }
        fn iam_condition_keys_for(
            &self,
            _request: &AwsRequest,
            _action: &IamAction,
        ) -> BTreeMap<String, Vec<String>> {
            let mut m = BTreeMap::new();
            m.insert("s3:prefix".to_string(), vec!["logs/".to_string()]);
            m
        }
    }

    fn sample_request() -> AwsRequest {
        AwsRequest {
            service: "default".into(),
            action: "Noop".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "req-1".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::GET,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn sample_action() -> IamAction {
        IamAction {
            service: "s3",
            action: "ListBucket",
            resource: "arn:aws:s3:::my-bucket".to_string(),
        }
    }

    #[test]
    fn iam_condition_keys_for_default_is_empty() {
        let svc = DefaultService;
        let keys = svc.iam_condition_keys_for(&sample_request(), &sample_action());
        assert!(keys.is_empty());
    }

    #[test]
    fn iam_condition_keys_for_override_returns_map() {
        let svc = PopulatedService;
        let keys = svc.iam_condition_keys_for(&sample_request(), &sample_action());
        assert_eq!(keys.get("s3:prefix"), Some(&vec!["logs/".to_string()]));
    }

    #[test]
    fn response_body_len_and_is_empty_for_bytes() {
        let body: ResponseBody = Bytes::from_static(b"hello").into();
        assert_eq!(body.len(), 5);
        assert!(!body.is_empty());
        let empty: ResponseBody = ResponseBody::default();
        assert!(empty.is_empty());
    }

    #[test]
    fn response_body_from_vec_and_string_and_str() {
        let from_vec: ResponseBody = vec![1u8, 2, 3].into();
        assert_eq!(from_vec.expect_bytes(), &[1, 2, 3][..]);
        let from_string: ResponseBody = String::from("hi").into();
        assert_eq!(from_string.expect_bytes(), b"hi");
        let from_str: ResponseBody = "hey".into();
        assert_eq!(from_str.expect_bytes(), b"hey");
        let from_static: ResponseBody = (b"123" as &'static [u8]).into();
        assert_eq!(from_static.expect_bytes(), b"123");
    }

    #[test]
    fn response_body_partial_eq_bytes() {
        let body: ResponseBody = Bytes::from_static(b"x").into();
        assert!(body == Bytes::from_static(b"x"));
        assert!(!(body == Bytes::from_static(b"y")));
    }

    #[test]
    fn aws_request_json_body_empty_returns_null() {
        let req = sample_request();
        assert_eq!(req.json_body(), serde_json::Value::Null);
    }

    #[test]
    fn aws_request_json_body_parses_valid() {
        let mut req = sample_request();
        req.body = Bytes::from_static(br#"{"a":1}"#);
        assert_eq!(req.json_body(), serde_json::json!({"a": 1}));
    }

    #[test]
    fn aws_response_xml_constructor() {
        let resp = AwsResponse::xml(StatusCode::OK, Bytes::from_static(b"<ok/>"));
        assert_eq!(resp.status, StatusCode::OK);
        assert_eq!(resp.content_type, "text/xml");
    }

    #[test]
    fn aws_response_json_constructor() {
        let resp = AwsResponse::json(StatusCode::CREATED, "{}");
        assert_eq!(resp.status, StatusCode::CREATED);
        assert_eq!(resp.content_type, "application/x-amz-json-1.1");
    }

    #[test]
    fn aws_response_ok_json_helper() {
        let resp = AwsResponse::ok_json(serde_json::json!({"ok": true}));
        assert_eq!(resp.status, StatusCode::OK);
        assert!(resp.body.expect_bytes().starts_with(b"{"));
    }

    #[test]
    fn aws_error_service_not_found_fields() {
        let err = AwsServiceError::ServiceNotFound {
            service: "sqs".to_string(),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.code(), "UnknownService");
        assert!(err.message().contains("sqs"));
        assert!(err.extra_fields().is_empty());
        assert!(err.response_headers().is_empty());
    }

    #[test]
    fn aws_error_action_not_implemented_fields() {
        let err = AwsServiceError::action_not_implemented("sns", "FutureAction");
        assert_eq!(err.status(), StatusCode::NOT_IMPLEMENTED);
        assert_eq!(err.code(), "InvalidAction");
        assert!(err.message().contains("FutureAction"));
        assert!(err.message().contains("sns"));
    }

    #[test]
    fn aws_error_aws_error_helpers() {
        let e = AwsServiceError::aws_error(StatusCode::FORBIDDEN, "Denied", "no");
        assert_eq!(e.status(), StatusCode::FORBIDDEN);
        assert_eq!(e.code(), "Denied");
        assert_eq!(e.message(), "no");

        let fields = vec![("Bucket".to_string(), "b".to_string())];
        let ef = AwsServiceError::aws_error_with_fields(
            StatusCode::NOT_FOUND,
            "Missing",
            "gone",
            fields.clone(),
        );
        assert_eq!(ef.extra_fields(), fields.as_slice());

        let hdrs = vec![("X-Retry".to_string(), "1".to_string())];
        let eh = AwsServiceError::aws_error_with_headers(
            StatusCode::TOO_MANY_REQUESTS,
            "Throttled",
            "slow",
            hdrs.clone(),
        );
        assert_eq!(eh.response_headers(), hdrs.as_slice());
    }

    #[test]
    #[should_panic(expected = "expect_bytes called on ResponseBody::File")]
    fn response_body_expect_bytes_panics_on_file() {
        let f = std::fs::File::create(std::env::temp_dir().join("fc-test-expect-file")).unwrap();
        let async_f = tokio::fs::File::from_std(f);
        let body = ResponseBody::File {
            file: async_f,
            size: 0,
        };
        let _ = body.expect_bytes();
    }
}
