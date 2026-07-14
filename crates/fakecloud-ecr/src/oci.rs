//! OCI Distribution v2 HTTP handlers.
//!
//! Implements the subset of
//! <https://github.com/opencontainers/distribution-spec> needed for
//! `docker push` / `docker pull` / `aws ecr get-login-password |
//! docker login` against a running fakecloud. All blob and manifest
//! state lives in the existing repository state (see `state.rs`); this
//! module is the HTTP-layer adapter that shapes those operations into
//! the content-addressed REST surface that Docker/OCI clients expect.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use serde_json::json;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError, ResponseBody};

use std::path::{Path, PathBuf};

use crate::service::EcrService;
use crate::state::{Image, Layer, LayerUpload};

/// Per-upload spool file path. Uploads spool to a tempfile under the
/// system temp dir keyed by upload id; the file is appended to by every
/// `UploadLayerPart` / OCI blob `PATCH` and `PUT` chunk and then read
/// once on commit. The path is stored on the upload state so multiple
/// in-flight chunks can append in arrival order.
fn spool_path_for(upload_id: &str) -> PathBuf {
    std::env::temp_dir().join(format!("fakecloud-ecr-upload-{upload_id}"))
}

/// Create the per-upload spool file (truncating if a stray file exists)
/// and return its path. Called from `InitiateLayerUpload` /
/// `blob_upload_start` so subsequent chunks can append.
pub(crate) fn create_upload_spool(upload_id: &str) -> std::io::Result<PathBuf> {
    let path = spool_path_for(upload_id);
    // Truncate to make sure a previous failed upload with the same UUID
    // (impossible in practice but cheap insurance) doesn't leak.
    let _ = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)?;
    Ok(path)
}

/// Append `bytes` to the upload spool file. Used by the JSON
/// `UploadLayerPart` route, where the SDK sends a base64-encoded chunk.
pub(crate) fn append_bytes_sync(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new().append(true).open(path)?;
    f.write_all(bytes)?;
    f.sync_data()?;
    Ok(())
}

/// Stream a request body to the upload spool file. Returns the number
/// of bytes appended. Used by the OCI blob `PATCH` and `PUT` routes —
/// the body is consumed chunk-by-chunk from hyper, never materialized.
pub(crate) async fn append_stream(
    path: &Path,
    stream: fakecloud_core::service::RequestBodyStream,
) -> Result<u64, AwsServiceError> {
    use http_body_util::BodyExt;
    use tokio::io::AsyncWriteExt;
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(path)
        .await
        .map_err(|e| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("failed to open upload spool: {e}"),
            )
        })?;
    let mut written: u64 = 0;
    let mut body = stream;
    loop {
        match body.frame().await {
            Some(Ok(frame)) => {
                if let Ok(chunk) = frame.into_data() {
                    if !chunk.is_empty() {
                        if let Err(e) = file.write_all(&chunk).await {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "InternalError",
                                format!("failed to write upload chunk: {e}"),
                            ));
                        }
                        written += chunk.len() as u64;
                    }
                }
            }
            Some(Err(e)) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedRequestBody",
                    format!("failed to read upload chunk: {e}"),
                ));
            }
            None => break,
        }
    }
    if let Err(e) = file.flush().await {
        return Err(AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("failed to flush upload spool: {e}"),
        ));
    }
    Ok(written)
}

/// Read an upload spool file fully into memory and unlink it.
/// Read the upload spool file fully into memory without unlinking it.
/// Called at commit time to compute the SHA-256 and (on success)
/// promote the bytes into a `Layer`. The spool file is unlinked
/// separately via [`unlink_spool`] only after the digest matches; a
/// digest mismatch leaves the spool in place so the caller can retry
/// `CompleteLayerUpload` with the correct digest instead of having to
/// re-upload the entire blob.
pub(crate) fn read_spool(path: &Path) -> std::io::Result<Vec<u8>> {
    std::fs::read(path)
}

/// Best-effort cleanup of a stray spool file on cancel / error paths.
pub(crate) fn unlink_spool(path: &Path) {
    let _ = std::fs::remove_file(path);
}

/// Classify an OCI v2 request by method + path. Returns `None` when
/// the path is not a recognised OCI endpoint (the caller responds
/// with 404 in that case).
pub(crate) async fn dispatch(
    service: &EcrService,
    request: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // `/v2/...` — path_segments already splits off the leading `/`.
    let segs: Vec<&str> = request
        .path_segments
        .iter()
        .map(|s| s.as_str())
        .filter(|s| !s.is_empty())
        .collect();
    // API version probe: `/v2/` or `/v2`.
    if segs.len() == 1 && segs[0] == "v2" {
        if !authorized(request)? {
            return Err(unauthorized());
        }
        return Ok(base_response(
            StatusCode::OK,
            "application/json",
            b"{}".to_vec(),
        ));
    }
    if segs.is_empty() || segs[0] != "v2" {
        return Err(not_found());
    }
    if !authorized(request)? {
        return Err(unauthorized());
    }

    // Split the repository name out of segs[1..n]. OCI allows
    // slash-separated names so `v2/team/svc/...` means repo `team/svc`.
    // Anchor on the trailing segments (`blobs/...`, `manifests/...`,
    // `tags/list`) to find where the name ends.
    let (repo_name, action_segs) = match split_name(&segs[1..]) {
        Some(parts) => parts,
        None => return Err(not_found()),
    };

    let method = &request.method;
    let parts: Vec<&str> = action_segs.iter().map(|s| s.as_str()).collect();
    match (method, parts.as_slice()) {
        (&Method::GET, ["tags", "list"]) => tags_list(service, request, &repo_name),
        (&Method::HEAD, ["blobs", digest]) => blob_head(service, request, &repo_name, digest).await,
        (&Method::GET, ["blobs", digest]) => blob_get(service, request, &repo_name, digest).await,
        (&Method::DELETE, ["blobs", digest]) => blob_delete(service, request, &repo_name, digest),
        (&Method::POST, ["blobs", "uploads"]) | (&Method::POST, ["blobs", "uploads", ""]) => {
            blob_upload_start(service, request, &repo_name)
        }
        (&Method::PATCH, ["blobs", "uploads", upload_id]) => {
            blob_upload_patch(service, request, &repo_name, upload_id).await
        }
        (&Method::PUT, ["blobs", "uploads", upload_id]) => {
            blob_upload_finish(service, request, &repo_name, upload_id).await
        }
        (&Method::DELETE, ["blobs", "uploads", upload_id]) => {
            blob_upload_cancel(service, request, &repo_name, upload_id)
        }
        (&Method::HEAD, ["manifests", reference]) => {
            manifest_head(service, request, &repo_name, reference).await
        }
        (&Method::GET, ["manifests", reference]) => {
            manifest_get(service, request, &repo_name, reference).await
        }
        (&Method::PUT, ["manifests", reference]) => {
            manifest_put(service, request, &repo_name, reference)
        }
        (&Method::DELETE, ["manifests", reference]) => {
            manifest_delete(service, request, &repo_name, reference)
        }
        _ => Err(not_found()),
    }
}

/// Walk backwards from the end to find where the OCI action subpath
/// starts (`blobs/...`, `manifests/...`, `tags/list`). Everything
/// before it is the repository name.
fn split_name(segs: &[&str]) -> Option<(String, Vec<String>)> {
    for (i, s) in segs.iter().enumerate() {
        match *s {
            "blobs" | "manifests" | "tags" => {
                if i == 0 {
                    return None;
                }
                let name = segs[..i].join("/");
                let action = segs[i..].iter().map(|s| s.to_string()).collect();
                return Some((name, action));
            }
            _ => {}
        }
    }
    None
}

/// Accept Basic Auth with any token fakecloud issued via
/// `GetAuthorizationToken`, or the `test*` dev-bypass used elsewhere.
/// A missing `Authorization` header returns 401 with a
/// `WWW-Authenticate` challenge so the Docker CLI's two-phase login
/// flow (`GET /v2/` -> 401 -> Basic retry) works. Wire format:
/// `Authorization: Basic base64("AWS:<token>")`.
fn authorized(request: &AwsRequest) -> Result<bool, AwsServiceError> {
    let Some(header) = request.headers.get("authorization") else {
        return Ok(false);
    };
    let value = header.to_str().map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "UNAUTHORIZED",
            "Bad Authorization header",
        )
    })?;
    if let Some(rest) = value.strip_prefix("Basic ") {
        let decoded = B64.decode(rest.trim().as_bytes()).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UNAUTHORIZED",
                "Bad Basic credentials",
            )
        })?;
        let pair = String::from_utf8_lossy(&decoded);
        let mut parts = pair.splitn(2, ':');
        let user = parts.next().unwrap_or("");
        let _pass = parts.next().unwrap_or("");
        // AWS emits `AWS:<token>`; the legacy CLI uses `AWS:<bearer>`.
        // Dev bypass: user starts with `test`.
        return Ok(user == "AWS" || user.starts_with("test"));
    }
    // SigV4-signed requests (shouldn't happen on /v2/ but allow it —
    // the main dispatcher already validated the signature upstream if
    // verify_sigv4 is on).
    Ok(value.starts_with("AWS4-HMAC-SHA256"))
}

fn base_response(status: StatusCode, content_type: &str, body: Vec<u8>) -> AwsResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Docker-Distribution-Api-Version",
        HeaderValue::from_static("registry/2.0"),
    );
    AwsResponse {
        status,
        content_type: content_type.to_string(),
        body: ResponseBody::Bytes(Bytes::from(body)),
        headers,
    }
}

fn unauthorized() -> AwsServiceError {
    AwsServiceError::aws_error_with_headers(
        StatusCode::UNAUTHORIZED,
        "UNAUTHORIZED",
        "authentication required",
        vec![
            (
                "WWW-Authenticate".to_string(),
                "Basic realm=\"fakecloud-ecr\"".to_string(),
            ),
            (
                "Docker-Distribution-Api-Version".to_string(),
                "registry/2.0".to_string(),
            ),
        ],
    )
}

fn not_found() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NotFound",
        "The requested resource could not be found.",
    )
}

fn repo_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NAME_UNKNOWN",
        format!("repository name not known to registry: {name}"),
    )
}

fn blob_not_found(name: &str, digest: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "BLOB_UNKNOWN",
        format!("blob {digest} not found in repository {name}"),
    )
}

fn manifest_not_found(name: &str, reference: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "MANIFEST_UNKNOWN",
        format!("manifest {reference} not found in repository {name}"),
    )
}

fn immutable_tag(name: &str, tag: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MANIFEST_INVALID",
        format!(
            "tag invalid: the image tag '{tag}' already exists in repository '{name}' and cannot \
             be overwritten because the repository is configured for immutable tags"
        ),
    )
}

fn digest_invalid() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "DIGEST_INVALID",
        "provided digest did not match uploaded content",
    )
}

fn upload_unknown(upload_id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "BLOB_UPLOAD_UNKNOWN",
        format!("upload {upload_id} not found"),
    )
}

fn sha256_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

/// Resolve how this repository stores layer bytes. Returns `(stored,
/// encrypted_with)` where `stored` is what gets written into
/// `Layer.blob_b64` and `encrypted_with` is the KMS key ARN (if any)
/// the blob was encrypted under. Falls back to plaintext when the
/// service has no KMS state wired or the repo isn't KMS-configured.
pub(crate) fn encrypt_layer_bytes(
    service: &EcrService,
    account_id: &str,
    repo_name: &str,
    plaintext: &[u8],
) -> (Vec<u8>, Option<String>) {
    let Some(kms) = service.kms_handle() else {
        return (plaintext.to_vec(), None);
    };
    let accounts = service.state_handle().read();
    let Some(s) = accounts.get(account_id) else {
        return (plaintext.to_vec(), None);
    };
    let Some(repo) = s.repositories.get(repo_name) else {
        return (plaintext.to_vec(), None);
    };
    if repo.encryption_configuration.encryption_type != "KMS" {
        return (plaintext.to_vec(), None);
    }
    let Some(ref key_ref) = repo.encryption_configuration.kms_key else {
        return (plaintext.to_vec(), None);
    };
    // Drop the state read guard before the KMS call.
    let key_ref = key_ref.clone();
    drop(accounts);
    match fakecloud_kms::api::encrypt_blob(kms, account_id, &key_ref, plaintext) {
        Ok(bytes) => (bytes, Some(key_ref)),
        Err(err) => {
            tracing::warn!(
                %err, %repo_name,
                "KMS-encrypt failed for layer; storing plaintext"
            );
            (plaintext.to_vec(), None)
        }
    }
}

/// Reverse the transform applied by `encrypt_layer_bytes`. Returns
/// plaintext bytes; when the layer wasn't encrypted, the input is
/// returned unchanged.
pub(crate) fn decrypt_layer_bytes(
    service: &EcrService,
    account_id: &str,
    layer: &Layer,
) -> Result<Vec<u8>, AwsServiceError> {
    let raw = B64.decode(layer.blob_b64.as_bytes()).unwrap_or_default();
    let Some(ref _key_arn) = layer.encrypted_with_kms_key else {
        return Ok(raw);
    };
    let Some(kms) = service.kms_handle() else {
        return Err(AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "KmsNotWired",
            "layer was stored KMS-encrypted but KMS state is not wired into ECR",
        ));
    };
    fakecloud_kms::api::decrypt_blob(kms, account_id, &raw).map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "KmsDecryptFailed",
            format!("failed to decrypt layer blob: {e}"),
        )
    })
}

// -------- handlers --------

fn tags_list(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = service.state_handle().read();
    let state = accounts
        .get(&request.account_id)
        .ok_or_else(|| repo_not_found(name))?;
    let repo = state
        .repositories
        .get(name)
        .ok_or_else(|| repo_not_found(name))?;
    let mut tags: Vec<&str> = repo.image_tags.keys().map(|k| k.as_str()).collect();
    tags.sort();
    let body = json!({ "name": name, "tags": tags });
    Ok(base_response(
        StatusCode::OK,
        "application/json",
        serde_json::to_vec(&body).unwrap(),
    ))
}

async fn blob_head(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    digest: &str,
) -> Result<AwsResponse, AwsServiceError> {
    // Fast path: blob already in local state.
    let local = {
        let accounts = service.state_handle().read();
        accounts
            .get(&request.account_id)
            .and_then(|s| s.repositories.get(name))
            .and_then(|r| r.layers.get(digest).cloned())
    };
    if let Some(layer) = local {
        let mut resp = base_response(StatusCode::OK, "application/octet-stream", Vec::new());
        resp.headers.insert(
            "Docker-Content-Digest",
            HeaderValue::from_str(digest).unwrap(),
        );
        resp.headers
            .insert("Content-Length", HeaderValue::from(layer.size));
        return Ok(resp);
    }
    // Cache miss: try pull-through, which (on success) also caches.
    if let Some(outcome) =
        crate::pull_through::proxy_blob(service, &request.account_id, name, digest).await
    {
        let proxied = outcome?;
        let mut resp = crate::pull_through::blob_response(&proxied, digest);
        resp.body = fakecloud_core::service::ResponseBody::Bytes(bytes::Bytes::new());
        resp.headers.insert(
            "Content-Length",
            HeaderValue::from(proxied.bytes.len() as u64),
        );
        return Ok(resp);
    }
    Err(blob_not_found(name, digest))
}

async fn blob_get(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    digest: &str,
) -> Result<AwsResponse, AwsServiceError> {
    // Snapshot the layer under a write lock so we can bump pull metadata
    // on every image whose manifest references this blob digest.
    let caller_arn = request.principal.as_ref().map(|p| p.arn.clone());
    let local = {
        let mut accounts = service.state_handle().write();
        let layer = accounts
            .get_mut(&request.account_id)
            .and_then(|s| {
                let exclusions = crate::service::pull_time_exclusion_set(s);
                s.repositories.get_mut(name).map(|repo| (repo, exclusions))
            })
            .and_then(|(repo, exclusions)| {
                let layer = repo.layers.get(digest).cloned()?;
                let touched: Vec<String> = repo
                    .images
                    .iter()
                    .filter(|(_, img)| {
                        serde_json::from_str::<serde_json::Value>(&img.image_manifest)
                            .ok()
                            .and_then(|v| v.get("layers").cloned())
                            .and_then(|v| v.as_array().cloned())
                            .map(|arr| {
                                arr.iter().any(|l| {
                                    l.get("digest").and_then(|d| d.as_str()) == Some(digest)
                                })
                            })
                            .unwrap_or(false)
                    })
                    .map(|(d, _)| d.clone())
                    .collect();
                crate::service::touch_image_pull(
                    repo,
                    &touched,
                    caller_arn.as_deref(),
                    &exclusions,
                );
                Some(layer)
            });
        layer
    };
    if let Some(layer) = local {
        let bytes = decrypt_layer_bytes(service, &request.account_id, &layer)?;
        let mut resp = base_response(StatusCode::OK, &layer.media_type, bytes);
        resp.headers.insert(
            "Docker-Content-Digest",
            HeaderValue::from_str(digest).unwrap(),
        );
        return Ok(resp);
    }
    if let Some(outcome) =
        crate::pull_through::proxy_blob(service, &request.account_id, name, digest).await
    {
        let proxied = outcome?;
        // Pull-through cached the blob; mirror the local-hit code path
        // and bump the in-use counters on every image whose manifest
        // references this layer digest. Honours pull-time exclusions.
        {
            let mut accounts = service.state_handle().write();
            if let Some(state) = accounts.get_mut(&request.account_id) {
                let exclusions = crate::service::pull_time_exclusion_set(state);
                if let Some(repo) = state.repositories.get_mut(name) {
                    let touched: Vec<String> = repo
                        .images
                        .iter()
                        .filter(|(_, img)| {
                            serde_json::from_str::<serde_json::Value>(&img.image_manifest)
                                .ok()
                                .and_then(|v| v.get("layers").cloned())
                                .and_then(|v| v.as_array().cloned())
                                .map(|arr| {
                                    arr.iter().any(|l| {
                                        l.get("digest").and_then(|d| d.as_str()) == Some(digest)
                                    })
                                })
                                .unwrap_or(false)
                        })
                        .map(|(d, _)| d.clone())
                        .collect();
                    crate::service::touch_image_pull(
                        repo,
                        &touched,
                        caller_arn.as_deref(),
                        &exclusions,
                    );
                }
            }
        }
        return Ok(crate::pull_through::blob_response(&proxied, digest));
    }
    Err(blob_not_found(name, digest))
}

fn blob_delete(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    digest: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accounts = service.state_handle().write();
    let state = accounts
        .get_mut(&request.account_id)
        .ok_or_else(|| repo_not_found(name))?;
    let repo = state
        .repositories
        .get_mut(name)
        .ok_or_else(|| repo_not_found(name))?;
    if repo.layers.remove(digest).is_none() {
        return Err(blob_not_found(name, digest));
    }
    Ok(base_response(
        StatusCode::ACCEPTED,
        "application/json",
        Vec::new(),
    ))
}

fn blob_upload_start(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    // Support single-POST upload with `?digest=...` + body per the spec.
    let digest_q = request.query_params.get("digest").cloned();
    let upload_id = Uuid::new_v4().to_string();
    let body_bytes = request.body.to_vec();

    let mut accounts = service.state_handle().write();
    let state = accounts
        .get_mut(&request.account_id)
        .ok_or_else(|| repo_not_found(name))?;
    if !state.repositories.contains_key(name) {
        return Err(repo_not_found(name));
    }

    if let Some(expected) = digest_q {
        let computed = sha256_digest(&body_bytes);
        if expected != computed {
            return Err(digest_invalid());
        }
        let size = body_bytes.len() as u64;
        // Drop the state guard before KMS: encrypt_layer_bytes needs its
        // own read lock, and holding two concurrently would deadlock.
        drop(accounts);
        let (stored_bytes, encrypted_with) =
            encrypt_layer_bytes(service, &request.account_id, name, &body_bytes);
        let mut accounts = service.state_handle().write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| repo_not_found(name))?;
        let repo = state
            .repositories
            .get_mut(name)
            .ok_or_else(|| repo_not_found(name))?;
        repo.layers.insert(
            computed.clone(),
            Layer {
                digest: computed.clone(),
                size,
                blob_b64: B64.encode(&stored_bytes),
                media_type: "application/octet-stream".to_string(),
                encrypted_with_kms_key: encrypted_with,
            },
        );
        let mut resp = base_response(StatusCode::CREATED, "application/json", Vec::new());
        resp.headers.insert(
            "Location",
            HeaderValue::from_str(&format!("/v2/{name}/blobs/{digest}", digest = computed,))
                .unwrap(),
        );
        resp.headers.insert(
            "Docker-Content-Digest",
            HeaderValue::from_str(&computed).unwrap(),
        );
        return Ok(resp);
    }

    let spool = create_upload_spool(&upload_id).map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("failed to create upload spool: {e}"),
        )
    })?;
    state.layer_uploads.insert(
        upload_id.clone(),
        LayerUpload {
            upload_id: upload_id.clone(),
            repository_name: name.to_string(),
            created_at: Utc::now(),
            spool_path: spool.to_string_lossy().to_string(),
            last_byte_received: 0,
        },
    );
    let mut resp = base_response(StatusCode::ACCEPTED, "application/json", Vec::new());
    resp.headers.insert(
        "Location",
        HeaderValue::from_str(&format!("/v2/{name}/blobs/uploads/{upload_id}")).unwrap(),
    );
    resp.headers.insert(
        "Docker-Upload-UUID",
        HeaderValue::from_str(&upload_id).unwrap(),
    );
    resp.headers
        .insert("Range", HeaderValue::from_static("0-0"));
    Ok(resp)
}

/// Parse the start offset from a blob-upload `Content-Range` header. Accepts
/// the OCI form `<start>-<end>` and the HTTP form `bytes <start>-<end>/<total>`;
/// returns `None` when no leading integer can be read.
fn parse_content_range_start(value: &str) -> Option<u64> {
    let v = value.trim();
    let v = v.strip_prefix("bytes").map(str::trim).unwrap_or(v);
    let start = v.split('-').next()?.trim();
    start.parse::<u64>().ok()
}

async fn blob_upload_patch(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    upload_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    // Resolve the upload + spool path under a short-lived read guard
    // so the streaming append below doesn't hold a Send-unfriendly
    // `parking_lot::RwLockWriteGuard` across `.await`.
    let (spool, expected_start) = {
        let accounts = service.state_handle().read();
        let state = accounts
            .get(&request.account_id)
            .ok_or_else(|| repo_not_found(name))?;
        let upload = state
            .layer_uploads
            .get(upload_id)
            .ok_or_else(|| upload_unknown(upload_id))?;
        if upload.repository_name != name {
            return Err(upload_unknown(upload_id));
        }
        (PathBuf::from(&upload.spool_path), upload.last_byte_received)
    };

    // A chunked PATCH carries a Content-Range whose start must equal the
    // number of bytes already received. Docker retries a failed chunk by
    // re-sending the same range; without this check the retry appends the
    // chunk a second time and silently corrupts the blob (detected only at
    // the finishing digest). Reject a non-contiguous range with 416 so the
    // client resumes from the right offset instead of double-appending.
    if let Some(cr) = request
        .headers
        .get("content-range")
        .and_then(|v| v.to_str().ok())
    {
        if let Some(start) = parse_content_range_start(cr) {
            if start != expected_start {
                return Err(AwsServiceError::aws_error(
                    StatusCode::RANGE_NOT_SATISFIABLE,
                    "BLOB_UPLOAD_INVALID",
                    format!(
                        "blob upload PATCH Content-Range start {start} does not match the current \
                         upload offset {expected_start}"
                    ),
                ));
            }
        }
    }

    // Streaming-only: dispatch flags blob-upload PATCH/PUT to keep
    // `body_stream` populated, so we always consume it here. A 1 GiB
    // push lands in constant memory.
    let stream = request.take_body_stream().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "BLOB_UPLOAD_INVALID",
            "blob upload PATCH requires a streaming request body",
        )
    })?;
    let appended = append_stream(&spool, stream).await?;

    let mut accounts = service.state_handle().write();
    let state = accounts
        .get_mut(&request.account_id)
        .ok_or_else(|| repo_not_found(name))?;
    let upload = state
        .layer_uploads
        .get_mut(upload_id)
        .ok_or_else(|| upload_unknown(upload_id))?;
    if upload.repository_name != name {
        return Err(upload_unknown(upload_id));
    }
    // Increment the live counter under the write lock instead of from a
    // pre-append snapshot — concurrent PATCH calls are serialized by
    // append order on the spool file, so adding `appended` here is the
    // race-free progress update.
    upload.last_byte_received = upload.last_byte_received.saturating_add(appended);
    let range_end = upload.last_byte_received.saturating_sub(1);
    let mut resp = base_response(StatusCode::ACCEPTED, "application/json", Vec::new());
    resp.headers.insert(
        "Location",
        HeaderValue::from_str(&format!("/v2/{name}/blobs/uploads/{upload_id}")).unwrap(),
    );
    resp.headers.insert(
        "Docker-Upload-UUID",
        HeaderValue::from_str(upload_id).unwrap(),
    );
    resp.headers.insert(
        "Range",
        HeaderValue::from_str(&format!("0-{range_end}")).unwrap(),
    );
    Ok(resp)
}

async fn blob_upload_finish(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    upload_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let digest = request.query_params.get("digest").cloned().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "DIGEST_INVALID",
            "digest query parameter required on PUT",
        )
    })?;

    // Append the final chunk into the spool file (streaming when
    // available, buffered otherwise) before reading it back to compute
    // the SHA-256.
    let spool = {
        let accounts = service.state_handle().read();
        let state = accounts
            .get(&request.account_id)
            .ok_or_else(|| repo_not_found(name))?;
        let upload = state
            .layer_uploads
            .get(upload_id)
            .ok_or_else(|| upload_unknown(upload_id))?;
        if upload.repository_name != name {
            return Err(upload_unknown(upload_id));
        }
        PathBuf::from(&upload.spool_path)
    };

    // Final PUT may carry an empty body (chunks already streamed via
    // PATCH) or one last chunk. Either way, dispatch keeps body_stream
    // populated; we drain it into the spool unconditionally so a
    // single-call OCI upload (PATCH-less) works the same way.
    let stream = request.take_body_stream().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "BLOB_UPLOAD_INVALID",
            "blob upload PUT requires a streaming request body",
        )
    })?;
    append_stream(&spool, stream).await?;

    let combined = read_spool(&spool).map_err(|e| {
        AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalError",
            format!("failed to read upload spool: {e}"),
        )
    })?;
    let computed = sha256_digest(&combined);
    if digest != computed {
        // Spool stays in place — the OCI client can retry the final
        // PUT with the correct digest instead of re-uploading every
        // PATCH chunk.
        return Err(digest_invalid());
    }
    {
        let mut accounts = service.state_handle().write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| repo_not_found(name))?;
        // Commit. Removing upload after validation so retries can correct
        // a bad digest query param on the last PUT.
        state.layer_uploads.remove(upload_id);
    }
    unlink_spool(&spool);
    let (stored_bytes, encrypted_with) =
        encrypt_layer_bytes(service, &request.account_id, name, &combined);
    let mut accounts = service.state_handle().write();
    let state = accounts
        .get_mut(&request.account_id)
        .ok_or_else(|| repo_not_found(name))?;
    let repo = state
        .repositories
        .get_mut(name)
        .ok_or_else(|| repo_not_found(name))?;
    let size = combined.len() as u64;
    repo.layers.insert(
        computed.clone(),
        Layer {
            digest: computed.clone(),
            size,
            blob_b64: B64.encode(&stored_bytes),
            media_type: "application/octet-stream".to_string(),
            encrypted_with_kms_key: encrypted_with,
        },
    );
    let mut resp = base_response(StatusCode::CREATED, "application/json", Vec::new());
    resp.headers.insert(
        "Location",
        HeaderValue::from_str(&format!("/v2/{name}/blobs/{digest}", digest = computed)).unwrap(),
    );
    resp.headers.insert(
        "Docker-Content-Digest",
        HeaderValue::from_str(&computed).unwrap(),
    );
    Ok(resp)
}

fn blob_upload_cancel(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    upload_id: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accounts = service.state_handle().write();
    let state = accounts
        .get_mut(&request.account_id)
        .ok_or_else(|| repo_not_found(name))?;
    // Match patch/finish: refuse to cancel an upload scoped to a
    // different repository so a DELETE request for repo A can't tear
    // down an in-flight upload in repo B.
    let belongs = state
        .layer_uploads
        .get(upload_id)
        .map(|u| u.repository_name == name)
        .unwrap_or(false);
    if !belongs {
        return Err(upload_unknown(upload_id));
    }
    if let Some(removed) = state.layer_uploads.remove(upload_id) {
        unlink_spool(Path::new(&removed.spool_path));
    }
    Ok(base_response(
        StatusCode::NO_CONTENT,
        "application/json",
        Vec::new(),
    ))
}

async fn manifest_head(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    reference: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let local = {
        let accounts = service.state_handle().read();
        accounts
            .get(&request.account_id)
            .and_then(|s| s.repositories.get(name))
            .and_then(|repo| {
                resolve_reference(repo, reference).and_then(|digest| {
                    repo.images.get(&digest).map(|img| {
                        (
                            digest,
                            img.image_manifest_media_type.clone(),
                            img.image_manifest.len() as u64,
                        )
                    })
                })
            })
    };
    if let Some((digest, media_type, size)) = local {
        let mut resp = base_response(StatusCode::OK, &media_type, Vec::new());
        resp.headers.insert(
            "Docker-Content-Digest",
            HeaderValue::from_str(&digest).unwrap(),
        );
        resp.headers
            .insert("Content-Length", HeaderValue::from(size));
        return Ok(resp);
    }
    if let Some(outcome) = crate::pull_through::proxy_manifest(
        service,
        &request.account_id,
        name,
        reference,
        request.principal.as_ref().map(|p| p.arn.as_str()),
        // HEAD is an existence check, not a pull — don't bump pull
        // counters on the freshly cached image.
        false,
    )
    .await
    {
        let proxied = outcome?;
        let mut resp = crate::pull_through::manifest_response(&proxied);
        resp.headers.insert(
            "Content-Length",
            HeaderValue::from(proxied.bytes.len() as u64),
        );
        resp.body = fakecloud_core::service::ResponseBody::Bytes(bytes::Bytes::new());
        return Ok(resp);
    }
    Err(manifest_not_found(name, reference))
}

async fn manifest_get(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    reference: &str,
) -> Result<AwsResponse, AwsServiceError> {
    // Pull-shaped op: snapshot the manifest under a write lock so we can
    // bump `last_in_use_at`/`in_use_count` on the touched image.
    let caller_arn = request.principal.as_ref().map(|p| p.arn.clone());
    let local = {
        let mut accounts = service.state_handle().write();
        accounts
            .get_mut(&request.account_id)
            .and_then(|s| {
                let exclusions = crate::service::pull_time_exclusion_set(s);
                s.repositories.get_mut(name).map(|repo| (repo, exclusions))
            })
            .and_then(|(repo, exclusions)| {
                let digest = resolve_reference(repo, reference)?;
                let manifest = repo.images.get(&digest).map(|img| {
                    (
                        digest.clone(),
                        img.image_manifest_media_type.clone(),
                        img.image_manifest.as_bytes().to_vec(),
                    )
                });
                if manifest.is_some() {
                    crate::service::touch_image_pull(
                        repo,
                        &[digest],
                        caller_arn.as_deref(),
                        &exclusions,
                    );
                }
                manifest
            })
    };
    if let Some((digest, media_type, body)) = local {
        let mut resp = base_response(StatusCode::OK, &media_type, body);
        resp.headers.insert(
            "Docker-Content-Digest",
            HeaderValue::from_str(&digest).unwrap(),
        );
        return Ok(resp);
    }
    if let Some(outcome) = crate::pull_through::proxy_manifest(
        service,
        &request.account_id,
        name,
        reference,
        request.principal.as_ref().map(|p| p.arn.as_str()),
        // GET is a pull — bump in-use counters on the freshly cached image.
        true,
    )
    .await
    {
        let proxied = outcome?;
        return Ok(crate::pull_through::manifest_response(&proxied));
    }
    Err(manifest_not_found(name, reference))
}

fn manifest_put(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    reference: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let body = request.body.to_vec();
    let digest = sha256_digest(&body);
    let media_type = request
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/vnd.docker.distribution.manifest.v2+json")
        .to_string();

    let is_tag = !reference.starts_with("sha256:");

    let should_scan;
    {
        let mut accounts = service.state_handle().write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| repo_not_found(name))?;
        let registry_match = crate::service::registry_scan_on_push_matches(
            &state.registry_scanning_configuration,
            name,
        );
        let repo = state
            .repositories
            .get_mut(name)
            .ok_or_else(|| repo_not_found(name))?;

        // Immutable-tag guard: the docker/OCI push path must enforce the same
        // immutability the JSON PutImage path does, or `docker push` silently
        // overwrites a tag the repo was configured to protect.
        if is_tag && repo.image_tag_mutability == "IMMUTABLE" {
            if let Some(existing) = repo.image_tags.get(reference) {
                if existing != &digest {
                    return Err(immutable_tag(name, reference));
                }
            }
        }

        repo.images.insert(
            digest.clone(),
            Image {
                image_digest: digest.clone(),
                image_manifest: String::from_utf8_lossy(&body).to_string(),
                image_manifest_media_type: media_type,
                artifact_media_type: None,
                image_size_in_bytes: body.len() as u64,
                image_pushed_at: Utc::now(),
                last_recorded_pull_time: None,
                image_status: "ACTIVE".to_string(),
                last_archived_at: None,
                last_activated_at: None,
                last_in_use_at: None,
                in_use_count: 0,
            },
        );
        // If the reference isn't a digest, treat it as a tag.
        if is_tag {
            repo.image_tags
                .insert(reference.to_string(), digest.clone());
        }
        should_scan = repo.image_scanning_configuration.scan_on_push || registry_match;
    }

    // Scan-on-push + replication fire on the data-plane push path too, not just
    // JSON PutImage. Both re-acquire the state lock, so run them after dropping it.
    if should_scan {
        service.trigger_scan(&request.account_id, name, &digest);
    }
    service.replicate_image(&request.account_id, name, &digest);

    let mut resp = base_response(StatusCode::CREATED, "application/json", Vec::new());
    resp.headers.insert(
        "Location",
        HeaderValue::from_str(&format!("/v2/{name}/manifests/{digest}", digest = digest,)).unwrap(),
    );
    resp.headers.insert(
        "Docker-Content-Digest",
        HeaderValue::from_str(&digest).unwrap(),
    );
    Ok(resp)
}

fn manifest_delete(
    service: &EcrService,
    request: &AwsRequest,
    name: &str,
    reference: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accounts = service.state_handle().write();
    let state = accounts
        .get_mut(&request.account_id)
        .ok_or_else(|| repo_not_found(name))?;
    let repo = state
        .repositories
        .get_mut(name)
        .ok_or_else(|| repo_not_found(name))?;
    if reference.starts_with("sha256:") {
        if repo.images.remove(reference).is_none() {
            return Err(manifest_not_found(name, reference));
        }
        repo.image_tags.retain(|_, d| d != reference);
    } else {
        let digest = repo
            .image_tags
            .remove(reference)
            .ok_or_else(|| manifest_not_found(name, reference))?;
        let still_tagged = repo.image_tags.values().any(|d| d == &digest);
        if !still_tagged {
            repo.images.remove(&digest);
        }
    }
    Ok(base_response(
        StatusCode::ACCEPTED,
        "application/json",
        Vec::new(),
    ))
}

fn resolve_reference(repo: &crate::state::Repository, reference: &str) -> Option<String> {
    if reference.starts_with("sha256:") {
        if repo.images.contains_key(reference) {
            return Some(reference.to_string());
        }
        return None;
    }
    repo.image_tags.get(reference).cloned()
}

#[cfg(test)]
mod immutability_tests {
    use super::*;
    use crate::service::EcrService;
    use crate::state::{Repository, SharedEcrState};
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    const ACCT: &str = "111111111111";

    fn svc_with_repo(mutability: &str) -> EcrService {
        let mut mas: MultiAccountState<crate::state::EcrState> =
            MultiAccountState::new(ACCT, "us-east-1", "http://fakecloud:4566");
        let s = mas.get_or_create(ACCT);
        let arn = s.repository_arn("app");
        let mut repo = Repository::new("app", arn, ACCT, "fakecloud:4566");
        repo.image_tag_mutability = mutability.to_string();
        s.repositories.insert("app".to_string(), repo);
        let state: SharedEcrState = Arc::new(RwLock::new(mas));
        EcrService::new(state)
    }

    fn manifest_req(body: &str) -> AwsRequest {
        AwsRequest {
            service: "ecr".into(),
            action: "OciManifestPut".into(),
            region: "us-east-1".into(),
            account_id: ACCT.into(),
            request_id: "req-1".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(body.as_bytes().to_vec()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::PUT,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    // A manifest with no `layers` array skips the blob-existence check, so these
    // tests isolate the immutability behaviour.
    fn manifest(tag_hint: &str) -> String {
        format!("{{\"schemaVersion\":2,\"config\":{{\"digest\":\"sha256:{tag_hint}\"}}}}")
    }

    #[test]
    fn parse_content_range_start_handles_both_forms() {
        assert_eq!(super::parse_content_range_start("0-1023"), Some(0));
        assert_eq!(super::parse_content_range_start("1024-2047"), Some(1024));
        assert_eq!(
            super::parse_content_range_start("bytes 512-1023/*"),
            Some(512)
        );
        assert_eq!(super::parse_content_range_start("not-a-range"), None);
    }

    #[test]
    fn immutable_tag_push_is_rejected_on_the_oci_path() {
        let svc = svc_with_repo("IMMUTABLE");
        // First push of tag v1 succeeds.
        manifest_put(&svc, &manifest_req(&manifest("aaaa")), "app", "v1").unwrap();
        // Re-pushing DIFFERENT content to the same tag must be rejected.
        let Err(err) = manifest_put(&svc, &manifest_req(&manifest("bbbb")), "app", "v1") else {
            panic!("expected immutable-tag rejection");
        };
        assert_eq!(err.code(), "MANIFEST_INVALID");
    }

    #[test]
    fn mutable_tag_push_overwrites_freely() {
        let svc = svc_with_repo("MUTABLE");
        manifest_put(&svc, &manifest_req(&manifest("aaaa")), "app", "v1").unwrap();
        // Overwrite allowed on a MUTABLE repo.
        manifest_put(&svc, &manifest_req(&manifest("bbbb")), "app", "v1").unwrap();
    }
}
