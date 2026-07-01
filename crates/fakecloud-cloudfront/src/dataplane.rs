// crates/fakecloud-cloudfront/src/dataplane.rs
//! In-process CloudFront data plane.
//!
//! Mirrors the ELBv2 data plane (`fakecloud-elbv2/src/dataplane.rs`): a
//! supervisor loop binds one `TcpListener` on `127.0.0.1:0` per *enabled*
//! distribution, records the OS-allocated port back into distribution state as
//! `bound_port`, and serves viewer requests via `hyper`. The AWS-shaped
//! `*.cloudfront.net` domain stays cosmetic; clients discover the real address
//! via `/_fakecloud/cloudfront/distributions` and connect to
//! `http://127.0.0.1:{bound_port}/...`.
//!
//! This module lands in stages: Task 2 wires the listener lifecycle and serves
//! a fixed `200`. Path-pattern origin routing (Task 3) and CustomErrorResponses
//! (Task 4) extend `handle_request`. There is no global edge network — this is a
//! single local origin-serving node, matching the ALB/API Gateway precedent.

use std::collections::{BTreeMap, HashSet};
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
/// frees the OS port, so a disabled/deleted distribution stops serving.
struct BoundListener {
    handle: JoinHandle<()>,
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
    let dp = DataPlane {
        state,
        upstream,
        s3_endpoint: format!("127.0.0.1:{server_port}"),
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
        match TcpListener::bind(("127.0.0.1", 0)).await {
            Ok(listener) => {
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
                bindings.insert(dist_id.clone(), BoundListener { handle });
                trace!(dist = %dist_id, port, "CloudFront data plane: bound listener");
            }
            Err(e) => {
                warn!("CloudFront data plane: failed to bind for {dist_id}: {e}");
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

    // Resolve the origin domain for this path under the read lock (return an
    // owned value so the guard drops at the end of the block).
    let origin_domain: Option<String> = {
        let accs = dp.state.read();
        let resolved = accs
            .all_distributions()
            .find(|(_, d)| d.id == dist_id)
            .and_then(|(_, d)| resolve_origin_for_path(&d.config, &path));
        resolved
    };
    let Some(origin_domain) = origin_domain else {
        return canned(502, "distribution or matching origin not found");
    };

    let (authority, host_header) = resolve_upstream(&origin_domain, &dp.s3_endpoint);
    let url = format!("http://{authority}{path_and_query}");
    trace!(dist = %dist_id, %path, origin = %origin_domain, "CloudFront data plane: proxying");
    fetch_origin(dp, &method, &url, &host_header, &req_headers, &body_bytes).await
}

/// Pick the cache behavior matching `path` (ordered `CacheBehaviors`, else the
/// default) and return its origin's `domain_name`.
fn resolve_origin_for_path(cfg: &DistributionConfig, path: &str) -> Option<String> {
    let target = select_target_origin(cfg, path);
    let items = cfg.origins.items.as_ref()?;
    items
        .origin
        .iter()
        .find(|o| o.id == target)
        .map(|o| o.domain_name.clone())
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

/// Resolve an origin `domain_name` to the `(authority, host_header)` to use.
/// S3-website origins are served by this same fakecloud process, so connect to
/// its own port while preserving the website domain in `Host`; other (custom)
/// origins are reached at their domain verbatim.
fn resolve_upstream(origin_domain: &str, s3_endpoint: &str) -> (String, String) {
    if origin_domain.contains(".s3-website") {
        (s3_endpoint.to_string(), origin_domain.to_string())
    } else {
        (origin_domain.to_string(), origin_domain.to_string())
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
/// character) against a request path.
fn path_pattern_matches(pattern: &str, path: &str) -> bool {
    glob_match(pattern.as_bytes(), path.as_bytes())
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
