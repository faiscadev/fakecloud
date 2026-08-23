//! ECR pull-through cache: on OCI v2 cache miss, proxy manifest / blob
//! GETs to the upstream registry configured via `CreatePullThroughCacheRule`,
//! stream the response through, and cache the result into fakecloud's
//! content-addressed blob store so subsequent pulls hit local.
//!
//! AWS supports a fixed set of upstreams (public.ecr.aws, registry.k8s.io,
//! docker.io, quay.io, ghcr.io); fakecloud is upstream-agnostic and
//! accepts any `upstreamRegistryUrl` the rule stores. Bearer-token
//! handshake is implemented per the OCI Distribution spec so Docker Hub
//! / Quay / ghcr work out of the box, not just `public.ecr.aws`.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bytes::Bytes;
use chrono::Utc;
use http::{HeaderMap, HeaderValue, StatusCode};
use sha2::{Digest, Sha256};

use fakecloud_core::service::{AwsResponse, AwsServiceError, ResponseBody};

use crate::service::EcrService;
use crate::state::{Image, Layer, PullThroughCacheRule};

/// Result of a successful pull-through proxy call.
pub(crate) struct ProxiedManifest {
    pub bytes: Vec<u8>,
    pub media_type: String,
    pub digest: String,
}

pub(crate) struct ProxiedBlob {
    pub bytes: Vec<u8>,
    pub media_type: String,
}

/// Find the pull-through rule whose `ecr_repository_prefix` matches
/// the start of `repo_name`. Returns the matching rule plus the repo
/// path upstream (`repo_name` with the prefix stripped).
fn match_rule<'a>(
    rules: &'a [PullThroughCacheRule],
    repo_name: &str,
) -> Option<(&'a PullThroughCacheRule, String)> {
    rules.iter().find_map(|r| {
        let prefix = format!("{}/", r.ecr_repository_prefix);
        repo_name
            .strip_prefix(&prefix)
            .map(|tail| (r, tail.to_string()))
    })
}

/// Collect the configured pull-through rules for this account. Returns
/// an owned Vec so the caller doesn't hold the state read guard across
/// the network call.
fn rules_for_account(service: &EcrService, account_id: &str) -> Vec<PullThroughCacheRule> {
    let accounts = service.state_handle().read();
    accounts
        .get(account_id)
        .map(|s| s.pull_through_cache_rules.values().cloned().collect())
        .unwrap_or_default()
}

/// Issue an upstream GET for `<registry>/v2/<path>`. Handles the
/// two-phase bearer-token flow: on 401 with `WWW-Authenticate: Bearer
/// realm=...,service=...[,scope=...]`, fetch a token from the realm
/// and retry with `Authorization: Bearer <token>`. No fallback for
/// Basic challenges since none of the supported upstreams use them.
async fn fetch_upstream(
    registry_url: &str,
    path: &str,
    accept: &[&str],
) -> Result<reqwest::Response, String> {
    let url = format!(
        "{}/v2/{}",
        registry_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let client = reqwest::Client::builder()
        .user_agent("fakecloud-ecr/pull-through")
        .build()
        .map_err(|e| format!("reqwest build: {e}"))?;

    let mut req = client.get(&url);
    for a in accept {
        req = req.header("Accept", *a);
    }
    let resp = req
        .send()
        .await
        .map_err(|e| format!("upstream GET {url}: {e}"))?;

    if resp.status() != StatusCode::UNAUTHORIZED {
        return Ok(resp);
    }

    let Some(token) = exchange_bearer_token(&client, resp.headers()).await? else {
        return Err(format!(
            "upstream {url} returned 401 with no recognised WWW-Authenticate challenge"
        ));
    };

    let mut req = client
        .get(&url)
        .header("Authorization", format!("Bearer {token}"));
    for a in accept {
        req = req.header("Accept", *a);
    }
    req.send()
        .await
        .map_err(|e| format!("upstream retry GET {url}: {e}"))
}

/// Parse `WWW-Authenticate: Bearer realm=...,service=...,scope=...`
/// and hit the realm to get an anonymous token. Returns None when the
/// challenge isn't Bearer.
async fn exchange_bearer_token(
    client: &reqwest::Client,
    headers: &reqwest::header::HeaderMap,
) -> Result<Option<String>, String> {
    let Some(challenge) = headers
        .get("www-authenticate")
        .and_then(|v| v.to_str().ok())
    else {
        return Ok(None);
    };
    let Some(params) = challenge.strip_prefix("Bearer ") else {
        return Ok(None);
    };
    let mut realm = None;
    let mut service = None;
    let mut scope = None;
    for part in params.split(',') {
        let part = part.trim();
        let (key, value) = match part.split_once('=') {
            Some((k, v)) => (k.trim(), v.trim().trim_matches('"')),
            None => continue,
        };
        match key {
            "realm" => realm = Some(value.to_string()),
            "service" => service = Some(value.to_string()),
            "scope" => scope = Some(value.to_string()),
            _ => {}
        }
    }
    let Some(realm) = realm else {
        return Ok(None);
    };

    let mut url = realm;
    let mut sep = if url.contains('?') { '&' } else { '?' };
    if let Some(s) = service.as_deref() {
        url.push(sep);
        url.push_str("service=");
        url.push_str(&url_encode(s));
        sep = '&';
    }
    if let Some(s) = scope.as_deref() {
        url.push(sep);
        url.push_str("scope=");
        url.push_str(&url_encode(s));
    }
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("bearer token realm GET: {e}"))?
        .error_for_status()
        .map_err(|e| format!("bearer token realm non-2xx: {e}"))?;
    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("bearer token JSON parse: {e}"))?;
    // Registries emit the token under either `token` or `access_token`.
    let token = json
        .get("token")
        .or_else(|| json.get("access_token"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| "bearer token response missing `token` field".to_string())?
        .to_string();
    Ok(Some(token))
}

/// Proxy a manifest GET. On success, persist the manifest + auto-create
/// the local `Repository` so subsequent requests hit local.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn proxy_manifest(
    service: &EcrService,
    account_id: &str,
    region: &str,
    repo_name: &str,
    reference: &str,
    caller_arn: Option<&str>,
    count_as_pull: bool,
) -> Option<Result<ProxiedManifest, AwsServiceError>> {
    let rules = rules_for_account(service, account_id);
    let (rule, upstream_path) = match_rule(&rules, repo_name)?;
    let accept = &[
        "application/vnd.docker.distribution.manifest.v2+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.oci.image.index.v1+json",
    ];
    let resp = match fetch_upstream(
        &rule.upstream_registry_url,
        &format!("{upstream_path}/manifests/{reference}"),
        accept,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return Some(Err(proxy_error(repo_name, &e))),
    };
    if resp.status() == StatusCode::NOT_FOUND {
        return Some(Err(upstream_not_found(repo_name, reference)));
    }
    if !resp.status().is_success() {
        return Some(Err(proxy_error(
            repo_name,
            &format!("upstream manifest status {}", resp.status()),
        )));
    }
    let media_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/vnd.docker.distribution.manifest.v2+json")
        .to_string();
    let bytes = match resp.bytes().await {
        Ok(b) => b.to_vec(),
        Err(e) => return Some(Err(proxy_error(repo_name, &e.to_string()))),
    };
    let digest = sha256_digest(&bytes);
    cache_manifest(
        service,
        account_id,
        region,
        repo_name,
        reference,
        &bytes,
        &media_type,
        &digest,
        caller_arn,
        count_as_pull,
    );
    Some(Ok(ProxiedManifest {
        bytes,
        media_type,
        digest,
    }))
}

/// Proxy a blob GET. Caches the returned bytes keyed by digest so
/// future requests skip the upstream entirely.
pub(crate) async fn proxy_blob(
    service: &EcrService,
    account_id: &str,
    region: &str,
    repo_name: &str,
    digest: &str,
) -> Option<Result<ProxiedBlob, AwsServiceError>> {
    let rules = rules_for_account(service, account_id);
    let (rule, upstream_path) = match_rule(&rules, repo_name)?;
    let accept = &["application/octet-stream"];
    let resp = match fetch_upstream(
        &rule.upstream_registry_url,
        &format!("{upstream_path}/blobs/{digest}"),
        accept,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return Some(Err(proxy_error(repo_name, &e))),
    };
    if resp.status() == StatusCode::NOT_FOUND {
        return Some(Err(blob_not_found(repo_name, digest)));
    }
    if !resp.status().is_success() {
        return Some(Err(proxy_error(
            repo_name,
            &format!("upstream blob status {}", resp.status()),
        )));
    }
    let media_type = resp
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/octet-stream")
        .to_string();
    let bytes = match resp.bytes().await {
        Ok(b) => b.to_vec(),
        Err(e) => return Some(Err(proxy_error(repo_name, &e.to_string()))),
    };
    cache_blob(
        service,
        account_id,
        region,
        repo_name,
        digest,
        &bytes,
        &media_type,
    );
    Some(Ok(ProxiedBlob { bytes, media_type }))
}

#[allow(clippy::too_many_arguments)]
fn cache_manifest(
    service: &EcrService,
    account_id: &str,
    region: &str,
    repo_name: &str,
    reference: &str,
    bytes: &[u8],
    media_type: &str,
    digest: &str,
    caller_arn: Option<&str>,
    count_as_pull: bool,
) {
    let mut accounts = service.state_handle().write();
    let state = accounts.get_or_create(account_id);
    let account_id = state.account_id.clone();
    // Scope the auto-created repository ARN to the request region, not the
    // frozen account region, so a pull-through from a non-default region
    // yields a correctly-scoped repositoryArn.
    let region = region.to_string();
    // Honour pull-time exclusions: an excluded principal that triggers
    // a proxy-cache should not bump the in-use counter on the freshly
    // cached image. HEAD requests also opt out via `count_as_pull=false`
    // — they are existence checks, not pulls.
    let excluded = caller_arn
        .map(|a| state.pull_time_exclusions.contains_key(a))
        .unwrap_or(false);
    let now = Utc::now();
    let (last_pull, last_in_use, in_use_count) = if !count_as_pull || excluded {
        (None, None, 0)
    } else {
        (Some(now), Some(now), 1)
    };
    let repo = state
        .repositories
        .entry(repo_name.to_string())
        .or_insert_with(|| {
            let arn = format!(
                "arn:aws:ecr:{region}:{account_id}:repository/{repo_name}",
                region = region,
                account_id = account_id,
                repo_name = repo_name,
            );
            crate::state::Repository::new(
                repo_name,
                arn,
                &account_id,
                "http://pull-through.fakecloud.internal",
            )
        });
    repo.images.insert(
        digest.to_string(),
        Image {
            image_digest: digest.to_string(),
            image_manifest: String::from_utf8_lossy(bytes).to_string(),
            image_manifest_media_type: media_type.to_string(),
            artifact_media_type: None,
            image_size_in_bytes: bytes.len() as u64,
            image_pushed_at: now,
            last_recorded_pull_time: last_pull,
            image_status: "ACTIVE".to_string(),
            last_archived_at: None,
            last_activated_at: None,
            last_in_use_at: last_in_use,
            in_use_count,
        },
    );
    // A reference can be a tag (alphanumeric) or a digest. Only store
    // the tag mapping; digest refs don't need one since images are
    // keyed by digest already.
    if !reference.starts_with("sha256:") {
        repo.image_tags
            .insert(reference.to_string(), digest.to_string());
    }
}

fn cache_blob(
    service: &EcrService,
    account_id: &str,
    region: &str,
    repo_name: &str,
    digest: &str,
    bytes: &[u8],
    media_type: &str,
) {
    let mut accounts = service.state_handle().write();
    let state = accounts.get_or_create(account_id);
    let account_id = state.account_id.clone();
    // Scope the auto-created repository ARN to the request region.
    let region = region.to_string();
    let repo = state
        .repositories
        .entry(repo_name.to_string())
        .or_insert_with(|| {
            let arn = format!(
                "arn:aws:ecr:{region}:{account_id}:repository/{repo_name}",
                region = region,
                account_id = account_id,
                repo_name = repo_name,
            );
            crate::state::Repository::new(
                repo_name,
                arn,
                &account_id,
                "http://pull-through.fakecloud.internal",
            )
        });
    repo.layers.insert(
        digest.to_string(),
        Layer {
            digest: digest.to_string(),
            size: bytes.len() as u64,
            blob_b64: B64.encode(bytes),
            media_type: media_type.to_string(),
            // Pull-through proxy caches blobs as fetched upstream; the
            // local repo for the pull-through prefix never has
            // encryption_configuration.kms, so plaintext is correct.
            encrypted_with_kms_key: None,
        },
    );
}

fn sha256_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("sha256:{:x}", hasher.finalize())
}

/// Minimal percent-encoder for bearer-token query params. Encodes
/// anything outside `[A-Za-z0-9._~-]` (the RFC 3986 unreserved set).
fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        match *b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'~' | b'-' => {
                out.push(*b as char);
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

fn upstream_not_found(repo: &str, reference: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "MANIFEST_UNKNOWN",
        format!("manifest {reference} not found upstream of pull-through repo {repo}"),
    )
}

fn blob_not_found(repo: &str, digest: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "BLOB_UNKNOWN",
        format!("blob {digest} not found upstream of pull-through repo {repo}"),
    )
}

fn proxy_error(repo: &str, detail: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_GATEWAY,
        "PROXY_ERROR",
        format!("pull-through proxy failed for repo {repo}: {detail}"),
    )
}

/// Build an AwsResponse that mirrors the local OCI handlers'
/// `base_response` shape, with `Docker-Content-Digest` set.
pub(crate) fn manifest_response(proxied: &ProxiedManifest) -> AwsResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Docker-Distribution-Api-Version",
        HeaderValue::from_static("registry/2.0"),
    );
    headers.insert(
        "Docker-Content-Digest",
        HeaderValue::from_str(&proxied.digest).unwrap(),
    );
    AwsResponse {
        status: StatusCode::OK,
        content_type: proxied.media_type.clone(),
        body: ResponseBody::Bytes(Bytes::from(proxied.bytes.clone())),
        headers,
    }
}

pub(crate) fn blob_response(proxied: &ProxiedBlob, digest: &str) -> AwsResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        "Docker-Distribution-Api-Version",
        HeaderValue::from_static("registry/2.0"),
    );
    headers.insert(
        "Docker-Content-Digest",
        HeaderValue::from_str(digest).unwrap(),
    );
    AwsResponse {
        status: StatusCode::OK,
        content_type: proxied.media_type.clone(),
        body: ResponseBody::Bytes(Bytes::from(proxied.bytes.clone())),
        headers,
    }
}
