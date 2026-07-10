// crates/fakecloud-cloudfront/src/dataplane.rs
//! In-process CloudFront data plane.
//!
//! Mirrors the ELBv2 data plane (`fakecloud-elbv2/src/dataplane.rs`): a
//! supervisor loop binds one `TcpListener` per *enabled* distribution, records
//! the bound port back into distribution state as `bound_port`, and serves
//! viewer requests via `hyper`. The bind defaults to `127.0.0.1:0` (loopback,
//! ephemeral) but the host and an optional deterministic port window are
//! configurable via `FAKECLOUD_CLOUDFRONT_DATAPLANE_{HOST,BASE_PORT,PORT_SPAN}`
//! so the listeners can be published from a container (see `BindConfig`). The AWS-shaped
//! `*.cloudfront.net` domain stays cosmetic; clients discover the real address
//! via `/_fakecloud/cloudfront/distributions` and connect to
//! `http://127.0.0.1:{bound_port}/...`.
//!
//! `handle_request` selects a cache behavior by path pattern, resolves its
//! origin, reverse-proxies to it, and applies CustomErrorResponses (e.g. the
//! SPA `404 -> /index.html` served as `200`). There is no global edge network —
//! this is a single local origin-serving node, matching the ALB/API Gateway
//! precedent. Deferred (not implemented): in-path CloudFront Functions /
//! Lambda@Edge, TTL caching / invalidation, and OAC/SigV4 to private S3.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http::{HeaderMap, Method};
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tracing::{debug, trace, warn};

use crate::model::DistributionConfig;
use crate::state::SharedCloudFrontState;

const ENV_DISABLE: &str = "FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE";
const SUPERVISOR_TICK_SECS: u64 = 1;

/// Bind host for the per-distribution data-plane listeners. Defaults to
/// `127.0.0.1` (loopback, unchanged). Set to e.g. `0.0.0.0` so the listeners
/// are reachable from outside a container.
const ENV_BIND_HOST: &str = "FAKECLOUD_CLOUDFRONT_DATAPLANE_HOST";
/// Optional base port. When set, each distribution binds a deterministic port
/// in `[base, base + span)` instead of an OS-allocated ephemeral one, so a
/// known port range can be published ahead of time (e.g. `docker run -p`).
/// Unset (the default) preserves the ephemeral `:0` behavior.
const ENV_BASE_PORT: &str = "FAKECLOUD_CLOUDFRONT_DATAPLANE_BASE_PORT";
/// Width of the deterministic port window opened above `ENV_BASE_PORT`. Only
/// consulted when a base port is set; defaults to `DEFAULT_PORT_SPAN`.
const ENV_PORT_SPAN: &str = "FAKECLOUD_CLOUDFRONT_DATAPLANE_PORT_SPAN";
const DEFAULT_BIND_HOST: &str = "127.0.0.1";
const DEFAULT_PORT_SPAN: u16 = 50;

/// Where the data-plane supervisor binds per-distribution listeners. Resolved
/// once from the environment at spawn time; the defaults reproduce the historic
/// `127.0.0.1:0` (loopback, ephemeral) behavior exactly.
#[derive(Clone, Debug, PartialEq, Eq)]
struct BindConfig {
    host: String,
    /// `None` => OS-allocated ephemeral port (`:0`). `Some(base)` => deterministic
    /// lowest-free port in `[base, base + span)`.
    base_port: Option<u16>,
    span: u16,
}

impl Default for BindConfig {
    fn default() -> Self {
        BindConfig {
            host: DEFAULT_BIND_HOST.to_string(),
            base_port: None,
            span: DEFAULT_PORT_SPAN,
        }
    }
}

impl BindConfig {
    fn from_env() -> Self {
        Self::resolve(
            std::env::var(ENV_BIND_HOST).ok(),
            std::env::var(ENV_BASE_PORT).ok(),
            std::env::var(ENV_PORT_SPAN).ok(),
        )
    }

    /// Pure resolver (testable without touching process env). An empty or
    /// unparseable value falls back to the default for that field, with a warning
    /// for the numeric ones so a typo is not silently ignored.
    fn resolve(host: Option<String>, base: Option<String>, span: Option<String>) -> Self {
        let host = host
            .map(|h| h.trim().to_string())
            .filter(|h| !h.is_empty())
            .unwrap_or_else(|| DEFAULT_BIND_HOST.to_string());
        let base_port = base
            .map(|b| b.trim().to_string())
            .filter(|b| !b.is_empty())
            .and_then(|b| match b.parse::<u16>() {
                Ok(0) | Err(_) => {
                    warn!("{ENV_BASE_PORT}={b:?} is not a valid port (1..=65535); ignoring");
                    None
                }
                Ok(p) => Some(p),
            });
        let span = span
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .and_then(|s| match s.parse::<u16>() {
                Ok(0) | Err(_) => {
                    warn!("{ENV_PORT_SPAN}={s:?} is not a valid span (>= 1); using {DEFAULT_PORT_SPAN}");
                    None
                }
                Ok(v) => Some(v),
            })
            .unwrap_or(DEFAULT_PORT_SPAN);
        BindConfig {
            host,
            base_port,
            span,
        }
    }
}

/// Lowest free port in `[base, base + span)` not already held by a listener,
/// saturating at the u16 ceiling. `None` when the window is fully occupied.
fn next_deterministic_port(base: u16, span: u16, in_use: &BTreeSet<u16>) -> Option<u16> {
    let start = base as u32;
    let end = (start + span as u32).min(u16::MAX as u32 + 1);
    (start..end).map(|p| p as u16).find(|p| !in_use.contains(p))
}

/// Whether the data plane should run. Disabled by setting
/// `FAKECLOUD_CLOUDFRONT_DISABLE_DATAPLANE` to a truthy value (mirrors the
/// ELBv2 flag), for environments that only exercise the control plane.
pub fn dataplane_enabled() -> bool {
    !matches!(
        std::env::var(ENV_DISABLE).as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

/// Per-distribution listener handle. Dropping it aborts the accept loop and
/// frees the OS port, so a disabled/deleted distribution stops serving. `port`
/// is the actually-bound port, used to compute the in-use set when assigning
/// deterministic ports to newly-enabled distributions.
struct BoundListener {
    handle: JoinHandle<()>,
    port: u16,
}

impl Drop for BoundListener {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// State shared across the supervisor and per-connection handlers.
#[derive(Clone)]
struct DataPlane {
    state: SharedCloudFrontState,
    /// HTTP client used to fetch from origins (reverse-proxy).
    upstream: reqwest::Client,
    /// `host:port` of fakecloud's own server. An S3-website origin is served by
    /// this same process on the main port, so those origins are reached here
    /// with the website domain preserved in the `Host` header (real CloudFront
    /// likewise treats an S3-website endpoint as an HTTP custom origin).
    s3_endpoint: String,
    /// Where per-distribution listeners bind (host + ephemeral-vs-deterministic
    /// port policy). Resolved once from the environment at spawn time.
    cfg: BindConfig,
}

/// Spawn the CloudFront data-plane supervisor. No-op (returns without spawning)
/// when disabled via the env flag. `server_port` is fakecloud's own listen port,
/// used to reach S3-website origins served by this process.
pub fn spawn_dataplane(state: SharedCloudFrontState, server_port: u16) {
    if !dataplane_enabled() {
        debug!("CloudFront data plane disabled via {ENV_DISABLE}");
        return;
    }
    let upstream = match reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            warn!("CloudFront data plane: failed to build reqwest client: {e}");
            return;
        }
    };
    let cfg = BindConfig::from_env();
    if cfg.host != DEFAULT_BIND_HOST || cfg.base_port.is_some() {
        debug!(
            host = %cfg.host,
            base_port = ?cfg.base_port,
            span = cfg.span,
            "CloudFront data plane: custom bind config"
        );
    }
    let dp = DataPlane {
        state,
        upstream,
        s3_endpoint: format!("127.0.0.1:{server_port}"),
        cfg,
    };
    tokio::spawn(supervisor_loop(dp));
}

async fn supervisor_loop(dp: DataPlane) {
    let mut bindings: BTreeMap<String, BoundListener> = BTreeMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(SUPERVISOR_TICK_SECS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tick.tick().await;
        reconcile(&dp, &mut bindings).await;
    }
}

/// Reconcile bound listeners against the set of enabled distributions. Binds
/// newly-enabled distributions, tears down listeners for ones that were
/// disabled or deleted, and keeps each distribution's `bound_port` in sync with
/// whether the supervisor currently holds its listener. Because the want-set is
/// derived purely from persisted state, enabled distributions loaded from a
/// snapshot on startup are re-bound on the first tick (startup rebind).
async fn reconcile(dp: &DataPlane, bindings: &mut BTreeMap<String, BoundListener>) {
    // 1. Snapshot the (distribution id, owning account) pairs that want a listener.
    let want: Vec<(String, String)> = {
        let accs = dp.state.read();
        accs.all_distributions()
            .filter(|(_acct, d)| d.config.enabled)
            .map(|(acct, d)| (d.id.clone(), acct.clone()))
            .collect()
    };
    let want_set: HashSet<&String> = want.iter().map(|(id, _)| id).collect();

    // 2. Drop bindings for distributions no longer wanted (disabled/deleted).
    bindings.retain(|id, _| want_set.contains(id));

    // 3. Bind any newly-enabled distribution.
    for (dist_id, account_id) in want.iter() {
        if bindings.contains_key(dist_id) {
            continue;
        }
        let in_use: BTreeSet<u16> = bindings.values().map(|b| b.port).collect();
        match bind_listener(&dp.cfg, &in_use, dist_id).await {
            Some(listener) => {
                let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
                if port == 0 {
                    warn!("CloudFront data plane: bind returned port 0 for {dist_id}; skipping");
                    continue;
                }
                {
                    let mut accs = dp.state.write();
                    if let Some(st) = accs.accounts.get_mut(account_id) {
                        if let Some(d) = st.distributions.get_mut(dist_id) {
                            d.bound_port = Some(port);
                        }
                    }
                }
                let dp2 = dp.clone();
                let id2 = dist_id.clone();
                let handle = tokio::spawn(async move {
                    accept_loop(dp2, id2, listener).await;
                });
                bindings.insert(dist_id.clone(), BoundListener { handle, port });
                trace!(dist = %dist_id, port, "CloudFront data plane: bound listener");
            }
            None => {
                warn!("CloudFront data plane: failed to bind for {dist_id}");
            }
        }
    }

    // 4. Clear bound_port for any distribution the supervisor no longer holds.
    let mut accs = dp.state.write();
    let account_ids: Vec<String> = accs.accounts.keys().cloned().collect();
    for acct in account_ids {
        if let Some(st) = accs.accounts.get_mut(&acct) {
            for d in st.distributions.values_mut() {
                if !bindings.contains_key(&d.id) {
                    d.bound_port = None;
                }
            }
        }
    }
}

/// Bind one distribution's listener according to `cfg`. With a base port set,
/// try the lowest free deterministic port in the window on the configured host;
/// if that specific port can't be bound (already taken by something outside our
/// bookkeeping) or the window is exhausted, warn and fall back to an ephemeral
/// port on the same host so the distribution still serves. With no base port
/// (the default) it binds `host:0` directly — historic behavior when the host
/// is also the default `127.0.0.1`.
async fn bind_listener(
    cfg: &BindConfig,
    in_use: &BTreeSet<u16>,
    dist_id: &str,
) -> Option<TcpListener> {
    if let Some(base) = cfg.base_port {
        match next_deterministic_port(base, cfg.span, in_use) {
            Some(port) => match TcpListener::bind((cfg.host.as_str(), port)).await {
                Ok(l) => return Some(l),
                Err(e) => warn!(
                    dist = %dist_id,
                    "CloudFront data plane: deterministic bind {}:{port} failed ({e}); \
                     falling back to an ephemeral port",
                    cfg.host
                ),
            },
            None => warn!(
                dist = %dist_id,
                "CloudFront data plane: deterministic port window [{base}, {}) is full; \
                 falling back to an ephemeral port",
                base as u32 + cfg.span as u32
            ),
        }
    }
    match TcpListener::bind((cfg.host.as_str(), 0)).await {
        Ok(l) => Some(l),
        Err(e) => {
            warn!(dist = %dist_id, "CloudFront data plane: bind {}:0 failed: {e}", cfg.host);
            None
        }
    }
}

async fn accept_loop(dp: DataPlane, dist_id: String, listener: TcpListener) {
    loop {
        let (sock, _peer) = match listener.accept().await {
            Ok(p) => p,
            Err(e) => {
                debug!(dist = %dist_id, "accept error: {e}");
                continue;
            }
        };
        let dp2 = dp.clone();
        let id2 = dist_id.clone();
        tokio::spawn(async move {
            let io = TokioIo::new(sock);
            let svc = service_fn(move |req| {
                let dp3 = dp2.clone();
                let id3 = id2.clone();
                async move { Ok::<_, Infallible>(handle_request(&dp3, &id3, req).await) }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, svc)
                .await
            {
                debug!("CloudFront data plane: connection error: {e}");
            }
        });
    }
}

/// Serve one viewer request: select a cache behavior by path pattern, resolve
/// its origin, and reverse-proxy to it. Task 4 interposes CustomErrorResponses
/// on the origin status before returning.
async fn handle_request(
    dp: &DataPlane,
    dist_id: &str,
    req: Request<hyper::body::Incoming>,
) -> Response<Full<Bytes>> {
    let (parts, body) = req.into_parts();
    let method = parts.method;
    let path = parts.uri.path().to_string();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str())
        .unwrap_or("/")
        .to_string();
    let req_headers = parts.headers;
    let body_bytes = body
        .collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default();

    // Resolve the route under the read lock (owned snapshot so the guard drops
    // at the end of the block).
    let route: Option<RouteResolution> = {
        let accs = dp.state.read();
        let resolved = accs
            .all_distributions()
            .find(|(_, d)| d.id == dist_id)
            .and_then(|(_, d)| resolve_route(&d.config, &path, &dp.s3_endpoint));
        resolved
    };
    let Some(route) = route else {
        return canned(502, "distribution or matching origin not found");
    };

    let url = format!("{}{path_and_query}", route.upstream.url_base);
    trace!(dist = %dist_id, %path, origin = %route.upstream.host_header, "CloudFront data plane: proxying");
    let resp = fetch_origin(
        dp,
        &method,
        &url,
        &route.upstream.host_header,
        &req_headers,
        &body_bytes,
    )
    .await;

    // CustomErrorResponses: if the origin status matches a configured rule with
    // a response page path, serve that page from the DEFAULT origin and return
    // it with the rule's response code (the SPA deep-link fallback, e.g.
    // 404 -> /index.html returned as 200).
    if let Some(rule) = match_error_rule(&route.error_rules, resp.status().as_u16()) {
        let origin_status = resp.status();
        let url = format!("{}{}", route.default_upstream.url_base, rule.page_path);
        let err_resp = fetch_origin(
            dp,
            &Method::GET,
            &url,
            &route.default_upstream.host_header,
            &HeaderMap::new(),
            &Bytes::new(),
        )
        .await;
        // Only interpose the custom error page when the fallback fetch itself
        // succeeded. If fetching the page failed (e.g. the default origin is
        // down, or the page path 404s), returning it under the rule's success
        // ResponseCode would mask an error body with a 200; keep the ORIGINAL
        // origin response instead.
        if err_resp.status().is_success() {
            let mut err_resp = err_resp;
            // Status = the rule's ResponseCode if set, else the ORIGINAL origin
            // error status (AWS: an omitted ResponseCode keeps the origin's code).
            let final_status = rule
                .response_code
                .and_then(|c| http::StatusCode::from_u16(c).ok())
                .unwrap_or(origin_status);
            *err_resp.status_mut() = final_status;
            return err_resp;
        }
        return resp;
    }
    resp
}

/// Owned per-request routing snapshot (taken under the state read lock).
struct RouteResolution {
    /// Resolved upstream for the matched cache behavior.
    upstream: UpstreamTarget,
    /// Resolved upstream for the default cache behavior (where
    /// CustomErrorResponse pages are fetched from).
    default_upstream: UpstreamTarget,
    /// CustomErrorResponses that have a response page path.
    error_rules: Vec<ErrorRule>,
}

/// A resolved origin address: the scheme+authority to connect to and the `Host`
/// header to send.
#[derive(Clone)]
struct UpstreamTarget {
    /// `scheme://authority` (no trailing slash); the request path is appended.
    url_base: String,
    /// `Host` header sent upstream (the origin domain name).
    host_header: String,
}

#[derive(Clone)]
struct ErrorRule {
    error_code: u16,
    page_path: String,
    response_code: Option<u16>,
}

/// Resolve the matched origin, the default origin, and the custom-error rules
/// for a request path.
fn resolve_route(
    cfg: &DistributionConfig,
    path: &str,
    s3_endpoint: &str,
) -> Option<RouteResolution> {
    let items = cfg.origins.items.as_ref()?;
    let target = select_target_origin(cfg, path);
    let upstream = items
        .origin
        .iter()
        .find(|o| o.id == target)
        .map(|o| upstream_for(o, s3_endpoint))?;
    let default_target = cfg.default_cache_behavior.target_origin_id.as_str();
    let default_upstream = items
        .origin
        .iter()
        .find(|o| o.id == default_target)
        .map(|o| upstream_for(o, s3_endpoint))
        .unwrap_or_else(|| upstream.clone());
    let error_rules = cfg
        .custom_error_responses
        .as_ref()
        .and_then(|c| c.items.as_ref())
        .map(|it| {
            it.custom_error_response
                .iter()
                .filter_map(|r| {
                    r.response_page_path.as_ref().map(|p| ErrorRule {
                        error_code: r.error_code as u16,
                        page_path: p.clone(),
                        response_code: r.response_code.as_ref().and_then(|s| s.parse().ok()),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Some(RouteResolution {
        upstream,
        default_upstream,
        error_rules,
    })
}

/// First custom-error rule whose error code matches the origin status.
fn match_error_rule(rules: &[ErrorRule], status: u16) -> Option<ErrorRule> {
    rules.iter().find(|r| r.error_code == status).cloned()
}

fn select_target_origin<'a>(cfg: &'a DistributionConfig, path: &str) -> &'a str {
    if let Some(cbs) = &cfg.cache_behaviors {
        if let Some(items) = &cbs.items {
            for cb in &items.cache_behavior {
                if path_pattern_matches(&cb.path_pattern, path) {
                    return &cb.target_origin_id;
                }
            }
        }
    }
    &cfg.default_cache_behavior.target_origin_id
}

/// An S3 static-website endpoint (`bucket.s3-website-<region>.amazonaws.com` or
/// `bucket.s3-website.<region>.amazonaws.com`). Matched precisely (`.s3-website`
/// label plus the `.amazonaws.com` suffix) so a custom origin that merely
/// contains the substring — e.g. `my.s3-website.example.com` — is NOT rerouted
/// to the local fakecloud port.
fn is_s3_website(domain: &str) -> bool {
    domain.contains(".s3-website") && domain.ends_with(".amazonaws.com")
}

/// Resolve an [`crate::model::Origin`] to the upstream to connect to.
///
/// - S3-website origins are served by this same fakecloud process, so connect to
///   its own port while preserving the website domain in `Host`.
/// - Custom origins honor `CustomOriginConfig`: an `https-only` protocol policy
///   is fetched over HTTPS (else HTTP), and the configured `HTTPPort`/`HTTPSPort`
///   is appended UNLESS the `domain_name` already carries an explicit `:port`
///   (as local test origins do) or the port is the scheme default.
/// - Bare origins (no config) are reached over HTTP at their domain verbatim.
fn upstream_for(origin: &crate::model::Origin, s3_endpoint: &str) -> UpstreamTarget {
    let domain = &origin.domain_name;
    if is_s3_website(domain) {
        return UpstreamTarget {
            url_base: format!("http://{s3_endpoint}"),
            host_header: domain.clone(),
        };
    }
    if let Some(cfg) = &origin.custom_origin_config {
        let https = cfg
            .origin_protocol_policy
            .eq_ignore_ascii_case("https-only");
        let (scheme, port) = if https {
            ("https", cfg.https_port)
        } else {
            ("http", cfg.http_port)
        };
        // A domain that already encodes a port (host:port, as local origins do)
        // wins over the config port; otherwise append a non-default port.
        let has_explicit_port = domain.rsplit(':').next().is_some_and(|s| {
            !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) && domain.contains(':')
        });
        let default_port = (scheme == "http" && port == 80) || (scheme == "https" && port == 443);
        let authority = if has_explicit_port || port <= 0 || default_port {
            domain.clone()
        } else {
            format!("{domain}:{port}")
        };
        return UpstreamTarget {
            url_base: format!("{scheme}://{authority}"),
            host_header: domain.clone(),
        };
    }
    UpstreamTarget {
        url_base: format!("http://{domain}"),
        host_header: domain.clone(),
    }
}

/// Reverse-proxy the request to the resolved origin and copy the response back.
async fn fetch_origin(
    dp: &DataPlane,
    method: &Method,
    url: &str,
    host_header: &str,
    req_headers: &HeaderMap,
    body: &Bytes,
) -> Response<Full<Bytes>> {
    let mut rb = dp.upstream.request(reqwest_method(method), url);
    for (k, v) in req_headers.iter() {
        let n = k.as_str();
        if is_hop_by_hop(n) || n.eq_ignore_ascii_case("host") {
            continue;
        }
        rb = rb.header(k.as_str(), v.as_bytes());
    }
    rb = rb.header("host", host_header);
    if !body.is_empty() {
        rb = rb.body(body.to_vec());
    }
    match rb.send().await {
        Ok(up) => {
            let status = up.status();
            let headers = up.headers().clone();
            let bytes = up.bytes().await.unwrap_or_default();
            let mut resp = Response::new(Full::new(bytes));
            *resp.status_mut() = status;
            for (k, v) in headers.iter() {
                if !is_hop_by_hop(k.as_str()) {
                    resp.headers_mut().append(k.clone(), v.clone());
                }
            }
            resp
        }
        Err(e) => canned(502, &format!("origin error: {e}")),
    }
}

/// Match a CloudFront cache-behavior path pattern (`*` = any sequence, `?` = one
/// character) against a request path. AWS path patterns are relative (no leading
/// slash, e.g. `api/*`); normalize both sides so a canonical `api/*` and a
/// slash-prefixed `/api/*` both match a request path like `/api/orders`.
fn path_pattern_matches(pattern: &str, path: &str) -> bool {
    let pat = pattern.trim_start_matches('/');
    let p = path.trim_start_matches('/');
    glob_match(pat.as_bytes(), p.as_bytes())
}

fn glob_match(pat: &[u8], text: &[u8]) -> bool {
    // Iterative glob with backtracking on `*`.
    let (mut p, mut t) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while t < text.len() {
        if p < pat.len() && (pat[p] == b'?' || pat[p] == text[t]) {
            p += 1;
            t += 1;
        } else if p < pat.len() && pat[p] == b'*' {
            star = Some(p);
            mark = t;
            p += 1;
        } else if let Some(sp) = star {
            p = sp + 1;
            mark += 1;
            t = mark;
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == b'*' {
        p += 1;
    }
    p == pat.len()
}

fn canned(status: u16, msg: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(msg.to_string())))
        .expect("canned response builds")
}

fn reqwest_method(m: &Method) -> reqwest::Method {
    reqwest::Method::from_bytes(m.as_str().as_bytes()).unwrap_or(reqwest::Method::GET)
}

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP.iter().any(|&h| h.eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{CustomOriginConfig, Origin};

    fn origin(domain: &str, custom: Option<CustomOriginConfig>) -> Origin {
        Origin {
            id: "o".into(),
            domain_name: domain.into(),
            custom_origin_config: custom,
            ..Default::default()
        }
    }

    fn custom(policy: &str, http_port: i32, https_port: i32) -> CustomOriginConfig {
        CustomOriginConfig {
            http_port,
            https_port,
            origin_protocol_policy: policy.into(),
            ..Default::default()
        }
    }

    #[test]
    fn s3_website_detection_is_precise() {
        assert!(is_s3_website("b.s3-website-us-east-1.amazonaws.com"));
        assert!(is_s3_website("b.s3-website.us-east-1.amazonaws.com"));
        // A custom origin that merely contains the substring must NOT match.
        assert!(!is_s3_website("my.s3-website.example.com"));
        assert!(!is_s3_website("api.example.com"));
        assert!(!is_s3_website("127.0.0.1:8080"));
    }

    #[test]
    fn s3_website_origin_routes_to_local_port() {
        let up = upstream_for(
            &origin("b.s3-website-us-east-1.amazonaws.com", None),
            "127.0.0.1:4566",
        );
        assert_eq!(up.url_base, "http://127.0.0.1:4566");
        assert_eq!(up.host_header, "b.s3-website-us-east-1.amazonaws.com");
    }

    #[test]
    fn https_only_custom_origin_uses_https_and_port() {
        let up = upstream_for(
            &origin("api.example.com", Some(custom("https-only", 80, 8443))),
            "127.0.0.1:4566",
        );
        assert_eq!(up.url_base, "https://api.example.com:8443");
    }

    #[test]
    fn http_custom_origin_default_port_omits_port() {
        let up = upstream_for(
            &origin("api.example.com", Some(custom("http-only", 80, 443))),
            "127.0.0.1:4566",
        );
        assert_eq!(up.url_base, "http://api.example.com");
    }

    #[test]
    fn explicit_port_in_domain_wins_over_config_port() {
        // Local origins encode the port in the domain; the config port (80) must
        // not be appended on top of it.
        let up = upstream_for(
            &origin("127.0.0.1:52111", Some(custom("http-only", 80, 443))),
            "127.0.0.1:4566",
        );
        assert_eq!(up.url_base, "http://127.0.0.1:52111");
    }

    #[test]
    fn bare_origin_defaults_to_http() {
        let up = upstream_for(&origin("origin.internal", None), "127.0.0.1:4566");
        assert_eq!(up.url_base, "http://origin.internal");
    }

    #[test]
    fn bind_config_defaults_reproduce_loopback_ephemeral() {
        let cfg = BindConfig::resolve(None, None, None);
        assert_eq!(cfg, BindConfig::default());
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.base_port, None);
        assert_eq!(cfg.span, DEFAULT_PORT_SPAN);
    }

    #[test]
    fn bind_config_reads_host_base_and_span() {
        let cfg = BindConfig::resolve(
            Some("0.0.0.0".into()),
            Some("8100".into()),
            Some("10".into()),
        );
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.base_port, Some(8100));
        assert_eq!(cfg.span, 10);
    }

    #[test]
    fn bind_config_rejects_bad_values_and_falls_back() {
        // Blank host -> default; port 0 / non-numeric -> ephemeral; bad span -> default.
        let cfg = BindConfig::resolve(
            Some("   ".into()),
            Some("0".into()),
            Some("nope".into()),
        );
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.base_port, None);
        assert_eq!(cfg.span, DEFAULT_PORT_SPAN);

        assert_eq!(
            BindConfig::resolve(None, Some("notaport".into()), None).base_port,
            None
        );
    }

    #[test]
    fn deterministic_port_picks_lowest_free() {
        let mut in_use = BTreeSet::new();
        assert_eq!(next_deterministic_port(8100, 50, &in_use), Some(8100));
        in_use.insert(8100);
        in_use.insert(8101);
        assert_eq!(next_deterministic_port(8100, 50, &in_use), Some(8102));
        // A hole below the high-water mark is reused.
        in_use.remove(&8101);
        assert_eq!(next_deterministic_port(8100, 50, &in_use), Some(8101));
    }

    #[test]
    fn deterministic_port_exhausted_window_returns_none() {
        let in_use: BTreeSet<u16> = (8100..8103).collect();
        assert_eq!(next_deterministic_port(8100, 3, &in_use), None);
    }

    #[test]
    fn deterministic_port_saturates_at_u16_ceiling() {
        // base + span would overflow u16; must not panic, just stop at 65535.
        let in_use = BTreeSet::new();
        assert_eq!(next_deterministic_port(65534, 50, &in_use), Some(65534));
        let in_use: BTreeSet<u16> = [65534, 65535].into_iter().collect();
        assert_eq!(next_deterministic_port(65534, 50, &in_use), None);
    }
}
